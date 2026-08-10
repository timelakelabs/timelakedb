#!/usr/bin/env python3
"""EXPLAIN ANALYZE the funnel queries and report the aggregate's spill metrics.

Run INSIDE the target's network namespace (see run-innet.sh for why):

    docker run --rm --network container:tldb-perf -v <bench>:/bench -w /bench \
      tldb-bench:perf python probe-spill.py

The question it answers: is the final hash aggregate spilling, and how much of
the query is that? `FairSpillPool` divides its budget evenly across every
consumer that CAN spill, so a plan with one aggregate per partition gets
pool_size/N each — the more cores, the sooner it spills.
"""
import json
import re
import sys
import time
import urllib.request

URL = "http://localhost:1963/api/sql"

QUERIES = {
    "B1": "SELECT step, COUNT(DISTINCT product_id) AS products "
          "FROM pipeline_events "
          "WHERE event = 'stop' AND time >= now() - INTERVAL '24 hours' "
          "GROUP BY step ORDER BY step",
    "B2": "SELECT step, COUNT(DISTINCT product_id) AS products "
          "FROM pipeline_events "
          "WHERE event = 'stop' AND time >= now() - INTERVAL '48 hours' "
          "GROUP BY step ORDER BY step",
    "B3": "SELECT step, "
          "SUM(CASE WHEN event = 'start' THEN 1 ELSE 0 END) "
          " - SUM(CASE WHEN event = 'stop' THEN 1 ELSE 0 END) AS in_flight "
          "FROM pipeline_events WHERE time >= now() - INTERVAL '24 hours' "
          "GROUP BY step ORDER BY step",
}


def sql(q):
    body = json.dumps({"db": "poc", "sql": q}).encode()
    req = urllib.request.Request(
        URL, data=body, headers={"Content-Type": "application/json"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=120) as r:
        rows = json.loads(r.read())
    return (time.time() - t0) * 1000, rows


def main():
    for name, q in QUERIES.items():
        # warm the metadata cache, then time it three times
        sql(q)
        times = [sql(q)[0] for _ in range(3)]
        _, rows = sql("EXPLAIN ANALYZE " + q)
        plan = "\n".join(
            str(v) for row in rows for v in row.values())
        print(f"=== {name}: {', '.join(f'{t:.0f}ms' for t in times)} ===")
        for line in plan.splitlines():
            s = line.strip()
            if not s.startswith(("AggregateExec", "SortExec", "RepartitionExec",
                                 "DataSourceExec", "FilterExec", "ProjectionExec",
                                 "CoalesceBatchesExec", "SortPreservingMergeExec")):
                continue
            keep = re.findall(
                r"(mode=\w+|gby=\[[^\]]*\]|spill_count=[\d.KM ]+|"
                r"spilled_bytes=[\d.KMGB ]+|spilled_rows=[\d.KM ]+|"
                r"elapsed_compute=[\d.a-zµ]+|peak_mem_used=[\d.KMGB ]+|"
                r"reduction_factor=[\d.%()/KM ]+|partitions=\d+)", s)
            head = s.split(":")[0]
            print(f"  {head:26} {' '.join(keep)}")
        print()


if __name__ == "__main__":
    sys.exit(main())
