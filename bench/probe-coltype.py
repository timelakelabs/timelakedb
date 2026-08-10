#!/usr/bin/env python3
"""What does ONE decoded column cost, by column TYPE?

`probe-scan-floor.py` measured the marginal cost of a decoded column at
~4.7 ms and found it linear. It did not ask what the column is made of.
This does: same scan, same files, same row count, varying only which
column the projection pulls in.

The comparison that matters is a plain primitive (`duration_s`, f64) against
a dictionary-encoded string (`step`, ~10 distinct; `product_id`, ~200 K).
Both move bytes; only the string pays for dictionary reconstruction and for
the `Dictionary -> Utf8View` cast that `LazyTable::load_one_file` applies on
the decode worker. The gap between them is the whole prize available to a
change that decodes straight to `Utf8View`.

No tag filter, on purpose: with `tag_equals` empty the reader builds no row
filter, so every row of every kept group materialises and the per-column
cost is not confounded by selectivity.

    sh bench/probe-innet.sh tldb-perf probe-coltype.py
"""
import json
import statistics
import sys
import time
import urllib.request

URL = "http://localhost:1963/api/sql"
WHERE = "WHERE time >= now() - INTERVAL '48 hours'"

CASES = [
    ("floor: COUNT(*)", "COUNT(*)"),
    ("+1 f64  (duration_s)", "COUNT(duration_s)"),
    ("+2 f64  (duration_s, dur2)", "COUNT(duration_s), SUM(duration_s)"),
    ("+1 dict (step, ~10 distinct)", "COUNT(step)"),
    ("+1 dict (route)", "COUNT(route)"),
    ("+1 dict (product_id, ~200K)", "COUNT(product_id)"),
    ("+2 dict (step, product_id)", "COUNT(step), COUNT(product_id)"),
    ("+3 dict (step, event, product_id)",
     "COUNT(step), COUNT(event), COUNT(product_id)"),
]


def sql(q, timeout=120):
    body = json.dumps({"db": "poc", "sql": q}).encode()
    req = urllib.request.Request(
        URL, data=body, headers={"Content-Type": "application/json"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        r.read()
    return (time.time() - t0) * 1000


def med(q, n=9):
    for _ in range(3):
        sql(q)
    return statistics.median(sql(q) for _ in range(n))


def main():
    base = None
    print(f"{'projection':38} {'median':>9} {'vs floor':>9}")
    for name, proj in CASES:
        m = med(f"SELECT {proj} FROM pipeline_events {WHERE}")
        if base is None:
            base = m
        print(f"{name:38} {m:8.1f}ms {m - base:+8.1f}")


if __name__ == "__main__":
    sys.exit(main())
