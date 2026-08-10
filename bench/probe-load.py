#!/usr/bin/env python3
"""Split a Shape B query into SCAN-LOAD time and PLAN time.

Why this probe exists: `LazyTable::scan` decodes every candidate file and
hands the batches to a `MemorySourceConfig`, so the decode happens while the
plan is being BUILT. `EXPLAIN ANALYZE` therefore reports `DataSourceExec`
with almost no elapsed_compute — the load is invisible to it, and every
per-operator table in docs/evidence/PERFORMANCE_LOG.md is blind to the
largest part of a warm query.

The decomposition here is by construction: run a "proxy" query that forces
exactly the same scan (same columns, same filters, so the same pruning, the
same row filter, the same Utf8View conversion) under a plan that does almost
nothing — a single-group COUNT. Its wall time is load + HTTP. The real
query's wall minus that is what the plan above the scan costs.

Run INSIDE the target's network namespace (see run-innet.sh for why):

    sh bench/probe-innet.sh tldb-perf probe-load.py
"""
import json
import statistics
import sys
import time
import urllib.request

URL = "http://localhost:1963/api/sql"

# (name, real query, proxy that provokes the identical scan)
CASES = [
    ("B1_funnel_24h",
     "SELECT step, COUNT(DISTINCT product_id) AS products FROM pipeline_events "
     "WHERE event = 'stop' AND time >= now() - INTERVAL '24 hours' "
     "GROUP BY step ORDER BY step",
     "SELECT COUNT(step) a, COUNT(event) b, COUNT(product_id) c "
     "FROM pipeline_events "
     "WHERE event = 'stop' AND time >= now() - INTERVAL '24 hours'"),
    ("B2_funnel_48h",
     "SELECT step, COUNT(DISTINCT product_id) AS products FROM pipeline_events "
     "WHERE event = 'stop' AND time >= now() - INTERVAL '48 hours' "
     "GROUP BY step ORDER BY step",
     "SELECT COUNT(step) a, COUNT(event) b, COUNT(product_id) c "
     "FROM pipeline_events "
     "WHERE event = 'stop' AND time >= now() - INTERVAL '48 hours'"),
    ("B3_inflight_24h",
     "SELECT step, SUM(CASE WHEN event = 'start' THEN 1 ELSE 0 END) "
     " - SUM(CASE WHEN event = 'stop' THEN 1 ELSE 0 END) AS in_flight "
     "FROM pipeline_events WHERE time >= now() - INTERVAL '24 hours' "
     "GROUP BY step ORDER BY step",
     "SELECT COUNT(step) a, COUNT(event) b FROM pipeline_events "
     "WHERE time >= now() - INTERVAL '24 hours'"),
    ("B4_hourly_throughput_48h",
     "SELECT date_bin(INTERVAL '1 hour', time) AS hour, step, COUNT(*) AS events "
     "FROM pipeline_events WHERE time >= now() - INTERVAL '48 hours' "
     "GROUP BY 1, step ORDER BY 1, step",
     "SELECT COUNT(step) a FROM pipeline_events "
     "WHERE time >= now() - INTERVAL '48 hours'"),
    ("B5_route_rollup_24h",
     "SELECT route, step, COUNT(DISTINCT product_id) AS products, "
     "AVG(duration_s) AS avg_step_s FROM pipeline_events "
     "WHERE event = 'stop' AND time >= now() - INTERVAL '24 hours' "
     "GROUP BY route, step ORDER BY route, step",
     "SELECT COUNT(route) a, COUNT(step) b, COUNT(product_id) c, "
     "COUNT(duration_s) d FROM pipeline_events "
     "WHERE event = 'stop' AND time >= now() - INTERVAL '24 hours'"),
]


def sql(q, timeout=120):
    body = json.dumps({"db": "poc", "sql": q}).encode()
    req = urllib.request.Request(
        URL, data=body, headers={"Content-Type": "application/json"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        rows = json.loads(r.read())
    return (time.time() - t0) * 1000, rows


def med(q, n=5):
    sql(q)  # warm the metadata cache
    return statistics.median(sql(q)[0] for _ in range(n))


def main():
    floor = med("SELECT 1")
    print(f"HTTP+planning floor (SELECT 1): {floor:.1f} ms\n")
    print(f"{'query':26} {'whole':>8} {'scan-load':>10} {'plan':>8} {'load %':>7}")
    for name, real, proxy in CASES:
        whole = med(real)
        load = med(proxy)
        plan = whole - load
        pct = 100.0 * (load - floor) / whole if whole else 0.0
        print(f"{name:26} {whole:8.1f} {load:10.1f} {plan:8.1f} {pct:6.0f}%")


if __name__ == "__main__":
    sys.exit(main())
