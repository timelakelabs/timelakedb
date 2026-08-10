#!/usr/bin/env python3
"""Each Shape B query hammered ALONE, against a settled instance.

The harness runs its five Shape B queries in a fixed order and they share
one metadata cache, so a regression on the fourth can be an artefact of
what the first three left warm — the bloom cycle nearly banked a 2x
"regression" that was exactly this (docs/evidence/PERFORMANCE_LOG.md,
2026-08-09 01:55, and the standing measurement rule that came out of it).
The harness also reports one cold and one warm sample per query, which is
too few to separate a 3 ms move from noise.

This runs one query at a time, warms it, then takes n samples of that
query and nothing else. Cross-query cache effects are gone by
construction, and the spread is visible instead of inferred.

    sh bench/probe-innet.sh tldb-perf probe-shapeb.py [n]
"""
import json
import statistics
import sys
import time
import urllib.request

URL = "http://localhost:1963/api/sql"

QUERIES = {
    "B1_funnel_24h":
        "SELECT step, COUNT(DISTINCT product_id) AS products "
        "FROM pipeline_events "
        "WHERE event = 'stop' AND time >= now() - INTERVAL '24 hours' "
        "GROUP BY step ORDER BY step",
    "B2_funnel_48h":
        "SELECT step, COUNT(DISTINCT product_id) AS products "
        "FROM pipeline_events "
        "WHERE event = 'stop' AND time >= now() - INTERVAL '48 hours' "
        "GROUP BY step ORDER BY step",
    "B3_inflight_24h":
        "SELECT step, "
        "SUM(CASE WHEN event = 'start' THEN 1 ELSE 0 END) "
        " - SUM(CASE WHEN event = 'stop' THEN 1 ELSE 0 END) AS in_flight "
        "FROM pipeline_events "
        "WHERE time >= now() - INTERVAL '24 hours' "
        "GROUP BY step ORDER BY step",
    "B4_hourly_throughput_48h":
        "SELECT date_bin(INTERVAL '1 hour', time) AS hour, step, "
        "COUNT(*) AS events "
        "FROM pipeline_events "
        "WHERE time >= now() - INTERVAL '48 hours' "
        "GROUP BY 1, step ORDER BY 1, step",
    "B5_route_rollup_24h":
        "SELECT route, step, COUNT(DISTINCT product_id) AS products, "
        "AVG(duration_s) AS avg_step_s "
        "FROM pipeline_events "
        "WHERE event = 'stop' AND time >= now() - INTERVAL '24 hours' "
        "GROUP BY route, step ORDER BY route, step",
    # B5 minus its two halves, to say WHICH half moved: the group keys or
    # the distinct-count over the high-cardinality column.
    "B5a_route_keys_only":
        "SELECT route, step, COUNT(*) AS n FROM pipeline_events "
        "WHERE event = 'stop' AND time >= now() - INTERVAL '24 hours' "
        "GROUP BY route, step ORDER BY route, step",
    "B5b_route_avg_only":
        "SELECT route, step, AVG(duration_s) AS avg_step_s "
        "FROM pipeline_events "
        "WHERE event = 'stop' AND time >= now() - INTERVAL '24 hours' "
        "GROUP BY route, step ORDER BY route, step",
    # Same scan, same rows, ONE string group key against TWO. B1 and B4
    # group by one and B5 by two, which is the only structural difference
    # between the queries that won and the query that lost.
    "G1_step_only":
        "SELECT step, COUNT(*) AS n FROM pipeline_events "
        "WHERE event = 'stop' AND time >= now() - INTERVAL '24 hours' "
        "GROUP BY step",
    "G1_route_only":
        "SELECT route, COUNT(*) AS n FROM pipeline_events "
        "WHERE event = 'stop' AND time >= now() - INTERVAL '24 hours' "
        "GROUP BY route",
    "G2_route_step":
        "SELECT route, step, COUNT(*) AS n FROM pipeline_events "
        "WHERE event = 'stop' AND time >= now() - INTERVAL '24 hours' "
        "GROUP BY route, step",
}


def sql(q, timeout=120):
    body = json.dumps({"db": "poc", "sql": q}).encode()
    req = urllib.request.Request(
        URL, data=body, headers={"Content-Type": "application/json"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        rows = json.loads(r.read())
    return (time.time() - t0) * 1000, len(rows)


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    print(f"{'query':<26} {'median':>8} {'min':>7} {'p95':>7} {'rows':>6}")
    for name, q in QUERIES.items():
        for _ in range(3):          # warm this query's own footers
            sql(q)
        samples = []
        rows = 0
        for _ in range(n):
            ms, rows = sql(q)
            samples.append(ms)
        samples.sort()
        p95 = samples[min(len(samples) - 1, int(len(samples) * 0.95))]
        print(f"{name:<26} {statistics.median(samples):8.1f} "
              f"{samples[0]:7.1f} {p95:7.1f} {rows:6d}")


if __name__ == "__main__":
    main()
