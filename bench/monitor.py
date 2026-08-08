#!/usr/bin/env python3
"""Server resource monitor: samples `docker stats` for the target container
in a background thread and tags each sample with the current test phase.

This is the memory evidence for the OOM question — peak memory per phase
(ingest / query_a / query_b / burst) and whether it returns to baseline.
"""

import csv
import re
import subprocess
import threading
import time

_SIZE = re.compile(r"([\d.]+)\s*([A-Za-z]+)")
_UNITS = {
    "B": 1, "KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12,
    "KIB": 2 ** 10, "MIB": 2 ** 20, "GIB": 2 ** 30, "TIB": 2 ** 40,
}


def parse_size_mb(s):
    """'512.3MiB' or '1.5GiB / 31GiB' -> MB (float), None if unparseable."""
    m = _SIZE.match(s.strip().split("/")[0].strip())
    if not m:
        return None
    val, unit = float(m.group(1)), m.group(2).upper()
    return val * _UNITS.get(unit, 1) / 1e6


def container_exists(name):
    try:
        r = subprocess.run(["docker", "inspect", "--format", "{{.State.Running}}", name],
                           capture_output=True, text=True, timeout=15)
        return r.returncode == 0 and "true" in r.stdout.lower()
    except Exception:
        return False


class ResourceMonitor(threading.Thread):
    """Samples cpu%/mem of one container every ~2s until stop()."""

    def __init__(self, container, interval=1.0):
        super().__init__(daemon=True)
        self.container = container
        self.interval = interval
        self.samples = []           # (unix_ts, phase, cpu_pct, mem_mb)
        self.phase = "baseline"
        self._stop = threading.Event()
        self._lock = threading.Lock()

    def set_phase(self, phase):
        with self._lock:
            self.phase = phase

    def run(self):
        while not self._stop.is_set():
            t0 = time.time()
            try:
                r = subprocess.run(
                    ["docker", "stats", "--no-stream", "--format",
                     "{{.CPUPerc}}|{{.MemUsage}}", self.container],
                    capture_output=True, text=True, timeout=30)
                if r.returncode == 0 and "|" in r.stdout:
                    cpu_s, mem_s = r.stdout.strip().split("|", 1)
                    cpu = float(cpu_s.replace("%", "").strip() or 0)
                    mem = parse_size_mb(mem_s)
                    with self._lock:
                        self.samples.append((time.time(), self.phase, cpu, mem))
            except Exception:
                pass  # keep sampling; gaps are fine
            self._stop.wait(max(0.1, self.interval - (time.time() - t0)))

    def stop(self):
        self._stop.set()

    # ── analysis ─────────────────────────────────────────────────────────────

    def summary(self):
        """Per-phase peaks + baseline + returned-to-baseline verdict."""
        with self._lock:
            samples = list(self.samples)
        if not samples:
            return None
        phases = {}
        for _, phase, cpu, mem in samples:
            p = phases.setdefault(phase, {"peak_mem_mb": 0.0, "peak_cpu_pct": 0.0, "n": 0})
            p["n"] += 1
            if mem is not None:
                p["peak_mem_mb"] = max(p["peak_mem_mb"], mem)
            p["peak_cpu_pct"] = max(p["peak_cpu_pct"], cpu)
        base_samples = [m for _, ph, _, m in samples if ph == "baseline" and m is not None]
        baseline = min(base_samples) if base_samples else None
        settle = [m for _, ph, _, m in samples if ph == "settle" and m is not None]
        settled = settle[-1] if settle else None
        returned = None
        if baseline is not None and settled is not None:
            # generous: within 1.5x baseline + 200MB slack counts as "returned"
            returned = settled <= baseline * 1.5 + 200
        out = {
            "baseline_mem_mb": round(baseline, 1) if baseline is not None else None,
            "settled_mem_mb": round(settled, 1) if settled is not None else None,
            "returned_to_baseline": returned,
            "phases": {k: {"peak_mem_mb": round(v["peak_mem_mb"], 1),
                           "peak_cpu_pct": round(v["peak_cpu_pct"], 1),
                           "samples": v["n"]}
                       for k, v in phases.items()},
        }
        return out

    def write_csv(self, path):
        with self._lock:
            samples = list(self.samples)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["unix_ts", "phase", "cpu_pct", "mem_mb"])
            for ts, phase, cpu, mem in samples:
                w.writerow([f"{ts:.1f}", phase, f"{cpu:.1f}",
                            f"{mem:.1f}" if mem is not None else ""])
