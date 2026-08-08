#!/usr/bin/env python3
"""The timed test scenarios. Each maps to the evaluation plan:

  ingest   -> T1 bulk backfill        hosts   -> host-fleet history
  query_a  -> T3 journey lookups      query_b -> T4 cross-product aggs
  burst    -> T2 burst + concurrent   storage -> T7 footprint
  context  -> dataset sanity counts

Every scenario only talks to the Backend interface, so all backends get
identical treatment; results go into the Reporter's normalized sections.
"""

import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import workload
from backends.base import WriteError

NS = workload.NS


def _log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def _pctl(sorted_vals, p):
    if not sorted_vals:
        return None
    return sorted_vals[max(0, int(len(sorted_vals) * p) - 1)]


def _set_phase(mon, phase):
    if mon is not None:
        mon.set_phase(phase)


class _WriteStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.lines = 0
        self.bytes = 0
        self.errors = 0
        self.error_msgs = []
        self.latencies = []

    def ok(self, n_lines, dt, n_bytes):
        with self.lock:
            self.lines += n_lines
            self.bytes += n_bytes
            self.latencies.append(dt)

    def fail(self, msg):
        with self.lock:
            self.errors += 1
            if len(self.error_msgs) < 10:
                self.error_msgs.append(msg)

    def batch_ms(self):
        lat = sorted(self.latencies)
        return {
            "batch_p50_ms": round(_pctl(lat, 0.50) * 1000) if lat else None,
            "batch_p95_ms": round(_pctl(lat, 0.95) * 1000) if lat else None,
            "batch_max_ms": round(lat[-1] * 1000) if lat else None,
        }


def _write_batch(be, stats, batch):
    try:
        dt, nb = be.write_lines(batch)
        stats.ok(len(batch), dt, nb)
    except WriteError as e:
        stats.fail(str(e))


# ── ingest (T1) ──────────────────────────────────────────────────────────────

def run_ingest(be, cfg, rep, mon):
    _set_phase(mon, "ingest")
    total = cfg.products_per_day * cfg.backfill_days
    now_ns = time.time_ns()
    win_start, win_len = workload.backfill_window(now_ns, cfg.backfill_days)
    plan = workload.backfill_plan(total)
    est = total * cfg.steps * 2
    _log(f"INGEST: {total:,} products over {cfg.backfill_days}d "
         f"(~{est:,} events), {len(plan):,} chunks, {cfg.workers} workers")

    stats = _WriteStats()

    def do_chunk(chunk_idx, count):
        buf = []
        for line in workload.chunk_lines(chunk_idx, count, win_start, win_len,
                                         cfg.steps, cfg.dropout_pct):
            buf.append(line)
            if len(buf) >= cfg.batch_size:
                _write_batch(be, stats, buf)
                buf = []
        if buf:
            _write_batch(be, stats, buf)

    t0 = time.perf_counter()
    done = 0
    report_every = max(1, len(plan) // 10)
    with ThreadPoolExecutor(max_workers=cfg.workers) as ex:
        futs = [ex.submit(do_chunk, c, n) for c, n in plan]
        for f in as_completed(futs):
            f.result()
            done += 1
            if done % report_every == 0 or done == len(plan):
                el = time.perf_counter() - t0
                _log(f"  {done}/{len(plan)} chunks | {stats.lines:,} lines | "
                     f"{stats.lines / max(el, 1e-9):,.0f} lines/s | "
                     f"{stats.errors} errors")
    wall = time.perf_counter() - t0
    be.post_ingest()

    m = {"lines": stats.lines, "wall_s": round(wall, 1),
         "lines_per_s": round(stats.lines / max(wall, 1e-9)),
         "mb_sent": round(stats.bytes / 1e6, 1),
         "errors": stats.errors, **stats.batch_ms()}
    if stats.error_msgs:
        m["first_errors"] = stats.error_msgs
    rep.add_metric("ingest", m)
    _log(f"INGEST done: {stats.lines:,} lines in {wall:,.1f}s "
         f"({m['lines_per_s']:,} lines/s, {stats.errors} errors)")


# ── host-fleet history ───────────────────────────────────────────────────────

def run_hosts(be, cfg, rep, mon):
    if cfg.hosts <= 0 or cfg.host_hours <= 0:
        return
    _set_phase(mon, "hosts")
    _log(f"HOSTS: {cfg.hosts} hosts x {cfg.host_hours}h history @ 1-min "
         f"({cfg.disk_devices} disk devices each)")
    stats = _WriteStats()
    t0 = time.perf_counter()
    buf = []
    with ThreadPoolExecutor(max_workers=cfg.workers) as ex:
        futs = []
        for tick in workload.iter_host_history(cfg.hosts, cfg.host_hours,
                                               time.time_ns(),
                                               cfg.disk_devices):
            buf.extend(tick)
            while len(buf) >= cfg.batch_size:
                futs.append(ex.submit(_write_batch, be, stats,
                                      buf[:cfg.batch_size]))
                buf = buf[cfg.batch_size:]
        if buf:
            futs.append(ex.submit(_write_batch, be, stats, buf))
        for f in as_completed(futs):
            f.result()
    wall = time.perf_counter() - t0
    be.post_ingest()
    rep.add_metric("host_ingest", {
        "lines": stats.lines, "wall_s": round(wall, 1),
        "lines_per_s": round(stats.lines / max(wall, 1e-9)),
        "errors": stats.errors})
    _log(f"HOSTS done: {stats.lines:,} lines in {wall:,.1f}s "
         f"({stats.errors} errors)")


# ── Shape A (T3) ─────────────────────────────────────────────────────────────

def run_query_a(be, cfg, rep, mon):
    _set_phase(mon, "query_a")
    _log(f"SHAPE A: sampling {cfg.shape_a_samples} product ids")
    pids = be.sample_product_ids(cfg.shape_a_samples)
    if not pids:
        rep.note("Shape A skipped: no product ids found (run ingest first)")
        return
    times, errors = [], 0
    for i, pid in enumerate(pids):
        dt, rows, err = be.query(be.shape_a_query(pid))
        rep.add_query_row(f"A_journey_{i + 1:02d}", 1, dt * 1000, rows,
                          "ok" if err is None else "ERROR", err)
        if err:
            errors += 1
            _log(f"  A_journey_{i + 1:02d}: ERROR after {dt:.2f}s -> {err}")
        else:
            times.append(dt)
    p = sorted(times)
    m = {"n": len(pids), "errors": errors,
         "median_ms": round(statistics.median(p) * 1000) if p else None,
         "p95_ms": round(_pctl(p, 0.95) * 1000) if p else None,
         "max_ms": round(p[-1] * 1000) if p else None}
    rep.add_metric("query_shape_a", m)
    _log(f"SHAPE A done: median={m['median_ms']}ms p95={m['p95_ms']}ms "
         f"max={m['max_ms']}ms errors={errors}")


# ── Shape B (T4 — make-or-break) ─────────────────────────────────────────────

def run_query_b(be, cfg, rep, mon):
    _set_phase(mon, "query_b")
    _log(f"SHAPE B: {cfg.shape_b_repeats} runs each (cold -> warm)")
    per, total_errors = {}, 0
    for name, q in be.shape_b_queries().items():
        times, rows, errs = [None] * cfg.shape_b_repeats, None, 0
        for r in range(cfg.shape_b_repeats):
            dt, rc, err = be.query(q)
            rep.add_query_row(name, r + 1, dt * 1000, rc,
                              "ok" if err is None else "ERROR", err)
            if err:
                errs += 1
                _log(f"  {name} run {r + 1}: ERROR after {dt:.2f}s -> {err}")
            else:
                times[r] = dt
                rows = rc
        warm = [t for t in times[1:] if t is not None]
        per[name] = {
            "cold_ms": round(times[0] * 1000) if times[0] is not None else None,
            "warm_ms": round(min(warm) * 1000) if warm else None,
            "rows": rows, "errors": errs}
        total_errors += errs
        _log(f"  {name}: cold={per[name]['cold_ms']}ms "
             f"warm={per[name]['warm_ms']}ms rows={rows}"
             + (f"  [{errs} ERRORS]" if errs else ""))
    rep.add_metric("query_shape_b", {
        "queries": per, "total_errors": total_errors,
        "all_completed": total_errors == 0})
    _log(f"SHAPE B done: all_completed={total_errors == 0}")


# ── burst (T2) ───────────────────────────────────────────────────────────────

def run_burst(be, cfg, rep, mon):
    _set_phase(mon, "burst")
    lines = workload.burst_lines(cfg.burst_size, cfg.steps, time.time_ns())
    batches = [lines[i:i + cfg.batch_size]
               for i in range(0, len(lines), cfg.batch_size)]
    _log(f"BURST: {len(lines):,} events in {len(batches)} batches, "
         f"{cfg.workers} parallel writers, one Shape B query mid-burst")

    stats = _WriteStats()
    concurrent = {}

    def probe():
        time.sleep(0.3)  # let the burst begin first
        name, q = next(iter(be.shape_b_queries().items()))
        dt, rows, err = be.query(q)
        concurrent.update(ms=round(dt * 1000), ok=err is None, query=name,
                          error=err)

    probe_t = threading.Thread(target=probe, daemon=True)
    t0 = time.perf_counter()
    probe_t.start()
    with ThreadPoolExecutor(max_workers=cfg.workers) as ex:
        list(ex.map(lambda b: _write_batch(be, stats, b), batches))
    wall = time.perf_counter() - t0
    probe_t.join(timeout=300)
    be.post_ingest()

    m = {"events": stats.lines, "wall_s": round(wall, 2),
         "events_per_s": round(stats.lines / max(wall, 1e-9)),
         "errors": stats.errors, **stats.batch_ms(),
         "concurrent_query_ms": concurrent.get("ms"),
         "concurrent_query_ok": concurrent.get("ok"),
         "concurrent_query": concurrent.get("query")}
    if concurrent.get("error"):
        m["concurrent_query_error"] = concurrent["error"]
    if stats.error_msgs:
        m["first_errors"] = stats.error_msgs
    rep.add_metric("burst", m)
    _log(f"BURST done: {stats.lines:,} events in {wall:.2f}s "
         f"({m['events_per_s']:,}/s), errors={stats.errors}, "
         f"concurrent query {concurrent.get('ms')}ms "
         f"ok={concurrent.get('ok')}")


# ── storage (T7) ─────────────────────────────────────────────────────────────

def run_storage(be, cfg, rep, mon):
    _set_phase(mon, "storage")
    b = be.storage_bytes(cfg.container)
    if b is None:
        rep.note("storage: could not measure (no container/du unavailable)")
        return
    days = max(cfg.backfill_days, 1)
    gb = b / 1e9
    per_day = gb / days
    rep.add_metric("storage", {
        "bytes": b, "gb_total": round(gb, 3), "days_of_data": days,
        "gb_per_day": round(per_day, 3),
        "projected_gb_90d": round(per_day * 90, 1),
        "projected_gb_365d": round(per_day * 365, 1)})
    _log(f"STORAGE: {gb:.2f} GB total -> {per_day:.2f} GB/day "
         f"(90d ~{per_day * 90:.0f} GB, 365d ~{per_day * 365:.0f} GB)")
    if cfg.backend == "influxdb3":
        rep.note("storage: measured immediately after ingest; WAL may not be "
                 "fully compacted to Parquet yet. Re-run later with "
                 "--scenarios storage for a settled number.")


# ── context counts ───────────────────────────────────────────────────────────

def run_context(be, cfg, rep, mon):
    """Dataset sanity counts. Identical config + seed => identical dataset,
    so these values should MATCH across backends; a mismatch means an
    adapter's queries aren't semantically equivalent."""
    ctx = {}
    for name, q in be.context_queries().items():
        dt, rows, err = be.query(q)
        val = be.scalar(q) if err is None else None
        ctx[name] = {"ms": round(dt * 1000), "value": val, "error": err}
        _log(f"  context {name}: value={val} ({dt * 1000:.0f}ms)"
             + (f" ERROR {err}" if err else ""))
    if ctx:
        rep.add_metric("context", ctx)


SCENARIOS = {
    "ingest": run_ingest,
    "hosts": run_hosts,
    "query_a": run_query_a,
    "query_b": run_query_b,
    "burst": run_burst,
    "storage": run_storage,
    "context": run_context,
}
# execution order for --scenarios all
DEFAULT_ORDER = ["ingest", "hosts", "query_a", "query_b", "burst",
                 "storage", "context"]
