#!/usr/bin/env python3
"""Hammer one query N times and report its latency distribution.

Paired with `probe-alloc.sh`, which samples the server's /proc counters on
either side of this run, so the hammer's cost can be attributed: how many
minor page faults and how much *kernel* time one query costs.

Why that matters here: the 2026-08-10 11:40 cycle measured the scan-load at
40-75% of a warm Shape B query and proved it is NOT thread-bound — widening
the decode from 8 to 24 workers burnt 2.4x the CPU for identical wall time.
Contention, memory bandwidth and page-fault serialisation all look like that
from the outside; minor faults and system time tell them apart.

Run INSIDE the target's network namespace (see run-innet.sh for why):

    sh bench/probe-innet.sh tldb-perf probe-alloc.py [n] [case]
"""
import json
import statistics
import sys
import time
import urllib.request

URL = "http://localhost:1963/api/sql"

# The load proxy from probe-load.py: the identical scan (same columns, same
# filters, so the same pruning, row filter and Utf8View conversion) under a
# plan that does nothing but a single-group COUNT.
CASES = {
    "b2_load": "SELECT COUNT(step) a, COUNT(event) b, COUNT(product_id) c "
               "FROM pipeline_events "
               "WHERE event = 'stop' AND time >= now() - INTERVAL '48 hours'",
    "b2": "SELECT step, COUNT(DISTINCT product_id) AS products "
          "FROM pipeline_events "
          "WHERE event = 'stop' AND time >= now() - INTERVAL '48 hours' "
          "GROUP BY step ORDER BY step",
    "idle": "SELECT 1",
}


def sql(q, timeout=120):
    body = json.dumps({"db": "poc", "sql": q}).encode()
    req = urllib.request.Request(
        URL, data=body, headers={"Content-Type": "application/json"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        r.read()
    return (time.time() - t0) * 1000


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    case = sys.argv[2] if len(sys.argv) > 2 else "b2_load"
    q = CASES[case]
    for _ in range(3):
        sql(q)  # warm the metadata cache; faults there are one-off
    print(f"HAMMER-START {case} n={n}", flush=True)
    t0 = time.time()
    ms = [sql(q) for _ in range(n)]
    wall = time.time() - t0
    ms.sort()
    print(f"n={n} wall={wall:.1f}s median={statistics.median(ms):.1f}ms "
          f"min={ms[0]:.1f} p95={ms[int(0.95 * n)]:.1f}")


if __name__ == "__main__":
    sys.exit(main())
