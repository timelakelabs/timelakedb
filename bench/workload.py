#!/usr/bin/env python3
"""Workload generation shared by every backend under test.

Every candidate database accepts InfluxDB line protocol for writes
(InfluxDB 1.x/3 natively, QuestDB via ILP-over-HTTP, VictoriaMetrics via
/write), so line protocol is the universal write format. Only query
translation differs per engine, and that lives in backends/*.

The shapes mirror influxdb3-poc/loadgen/loadgen.py exactly — same
measurements, tags, fields, RNG seeding, and chunking — so a dataset
generated at a given scale is IDENTICAL across backends and across runs:

  pipeline_events,product_id=<id>,step=<01-download..10-upload>,
                  event=<start|stop>,route=<r>,worker_ip=<172.16.step.n>
      value=1i | duration_s=<float>                       <ns timestamp>
  host_metrics,host=<host0000..>
      cpu_pct=,mem_pct=,disk_pct=,net_rx_bps=i,net_tx_bps=i  <ns timestamp>
  disk_metrics,host=<host0000..>,device=<nvme0n1|nvme1n1|sda..>
      capacity_gb=i,used_gb=,used_pct=,read_bps=i,write_bps=i  <ns timestamp>
"""

import random

NS = 1_000_000_000
ROUTES = ["alpha", "bravo", "charlie", "delta"]
# Step values are "<nn>-<verb>" so lexical order == pipeline order.
STEP_NAMES = ["download", "extract", "validate", "translate", "transform",
              "enrich", "aggregate", "index", "package", "upload"]
PRODUCTS_PER_CHUNK = 500  # same chunking as loadgen.py -> same seeded pids


def step_label(s):
    return (f"{s:02d}-{STEP_NAMES[s - 1]}" if s <= len(STEP_NAMES)
            else f"{s:02d}")


def product_lines(pid, t0_ns, rng, steps, dropout_pct, max_stop_ns=None):
    """Start/stop line-protocol events for one product's lifecycle.

    Each step runs on a worker from that step's own subnet
    (172.16.<step>.0/24), so a journey shows a distinct worker_ip per
    step; start and stop of a step share the same worker."""
    route = rng.choice(ROUTES)
    t = t0_ns
    out = []
    for s in range(1, steps + 1):
        gap = int(rng.uniform(0, 90) * NS)          # queue time before step
        dur = int(rng.uniform(20, 600) * NS)        # 20s..10m per step
        ip = f"172.16.{s}.{rng.randint(1, 50)}"     # worker that ran the step
        start = t + gap
        stop = start + dur
        if max_stop_ns and stop > max_stop_ns:
            stop = max_stop_ns
            start = min(start, stop - 1)
        base = (f"pipeline_events,product_id={pid},step={step_label(s)},"
                f"route={route},worker_ip={ip}")
        out.append(f"{base},event=start value=1i {start}")
        out.append(f"{base},event=stop duration_s={dur / NS:.1f} {stop}")
        t = stop
        if s < steps and rng.random() < (dropout_pct / 100.0):
            break  # product drops out of the pipeline
    return out


def backfill_window(now_ns, backfill_days):
    """(win_start_ns, win_len_ns) — leaves ~2h headroom so lifecycles
    finish before 'now', matching loadgen.py."""
    win_end = now_ns - 2 * 3600 * NS
    win_start = now_ns - backfill_days * 86400 * NS
    return win_start, max(win_end - win_start, NS)


def backfill_plan(total_products):
    """[(chunk_idx, product_count), ...] — deterministic chunking."""
    n = (total_products + PRODUCTS_PER_CHUNK - 1) // PRODUCTS_PER_CHUNK
    return [(c, min(PRODUCTS_PER_CHUNK, total_products - c * PRODUCTS_PER_CHUNK))
            for c in range(n)]


def chunk_lines(chunk_idx, count, win_start_ns, win_len_ns, steps, dropout_pct):
    """All lines for one backfill chunk (seeded like loadgen._backfill_chunk)."""
    rng = random.Random(1000 + chunk_idx)
    for i in range(count):
        pid = f"p{chunk_idx:05d}-{i:05d}"
        t0 = win_start_ns + int(rng.random() * win_len_ns)
        yield from product_lines(pid, t0, rng, steps, dropout_pct)


def burst_lines(burst_size, steps, now_ns):
    """BURST_SIZE events timestamped within the last 5 minutes (seed 42)."""
    rng = random.Random(42)
    events_per_product = steps * 2
    n_products = max(1, burst_size // events_per_product)
    lines = []
    for i in range(n_products):
        pid = f"burst-{int(now_ns / NS)}-{i:06d}"
        t0 = now_ns - int(rng.uniform(0, 300) * NS)
        lines.extend(product_lines(pid, t0, rng, steps, dropout_pct=0.0))
    return lines[:burst_size]


def device_names(n):
    """nvme0n1, nvme1n1, then sda, sdb, ... — stable order per host."""
    return [f"nvme{i}n1" if i < 2 else f"sd{chr(ord('a') + i - 2)}"
            for i in range(n)]


class DeviceState:
    CAPACITIES_GB = [240, 480, 960, 1920, 3840]

    def __init__(self, rng):
        self.capacity_gb = rng.choice(self.CAPACITIES_GB)
        self.used_gb = self.capacity_gb * rng.uniform(0.2, 0.8)

    def step(self, rng):
        self.used_gb = min(self.capacity_gb * 0.98,
                           self.used_gb + rng.uniform(0, 0.01))


def _io_bps(rng):
    """Mostly-quiet device I/O with occasional heavy bursts (bytes/s)."""
    if rng.random() < 0.05:
        return rng.randint(50_000_000, 500_000_000)
    return rng.randint(50_000, 5_000_000)


class HostState:
    def __init__(self, rng, disk_devices=7):
        self.cpu = rng.uniform(5, 60)
        self.mem = rng.uniform(20, 70)
        self.disk = rng.uniform(30, 80)
        self.devices = {d: DeviceState(rng) for d in device_names(disk_devices)}

    def step(self, rng):
        self.cpu = min(99.0, max(1.0, self.cpu + rng.uniform(-6, 6)))
        self.mem = min(99.0, max(5.0, self.mem + rng.uniform(-2, 2)))
        self.disk = min(99.0, self.disk + rng.uniform(0, 0.002))
        for d in self.devices.values():
            d.step(rng)


def host_lines(states, rng, ts_ns):
    out = []
    for h, st in states.items():
        st.step(rng)
        out.append(
            f"host_metrics,host={h} "
            f"cpu_pct={st.cpu:.1f},mem_pct={st.mem:.1f},"
            f"disk_pct={st.disk:.2f},"
            f"net_rx_bps={rng.randint(10_000, 50_000_000)}i,"
            f"net_tx_bps={rng.randint(10_000, 20_000_000)}i "
            f"{ts_ns}"
        )
        for d, dev in st.devices.items():
            out.append(
                f"disk_metrics,host={h},device={d} "
                f"capacity_gb={dev.capacity_gb}i,"
                f"used_gb={dev.used_gb:.2f},"
                f"used_pct={100 * dev.used_gb / dev.capacity_gb:.2f},"
                f"read_bps={_io_bps(rng)}i,"
                f"write_bps={_io_bps(rng)}i "
                f"{ts_ns}"
            )
    return out


def iter_host_history(hosts, hours, now_ns, disk_devices=7):
    """Yield one list of lines per 1-minute tick, oldest first (seed 7)."""
    rng = random.Random(7)
    states = {f"host{h:04d}": HostState(rng, disk_devices)
              for h in range(hosts)}
    for m in range(int(hours * 60), 0, -1):
        yield host_lines(states, rng, now_ns - m * 60 * NS)
