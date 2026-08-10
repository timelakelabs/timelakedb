#!/usr/bin/env python3
"""What does a scan cost when it decodes NOTHING, and how does it grow per
column? Companion to probe-load.py, which says the load is 40-75% of a warm
Shape B query; this one asks what inside the load is actually paying.

The first line visits every candidate file and decodes none of them (the
bloom filter excludes an entity that is not there), so it is the fixed
per-file floor: footer lookup, bloom probe, thread spawn, the serial tail.
The rest add one decoded column at a time over the same 48 h window.

    sh bench/probe-innet.sh tldb-perf probe-scan-floor.py
"""
import json
import statistics
import sys
import time
import urllib.request

URL = "http://localhost:1963/api/sql"
W = "time >= now() - INTERVAL '48 hours'"

CASES = [
    ("floor: all files, zero decoded",
     f"SELECT COUNT(*) n FROM pipeline_events "
     f"WHERE product_id = 'nope-not-here' AND {W}"),
    ("1 column", f"SELECT COUNT(step) a FROM pipeline_events WHERE {W}"),
    ("2 columns",
     f"SELECT COUNT(step) a, COUNT(event) b FROM pipeline_events WHERE {W}"),
    ("3 columns",
     f"SELECT COUNT(step) a, COUNT(event) b, COUNT(product_id) c "
     f"FROM pipeline_events WHERE {W}"),
    ("4 columns",
     f"SELECT COUNT(step) a, COUNT(event) b, COUNT(product_id) c, "
     f"COUNT(route) d FROM pipeline_events WHERE {W}"),
]


def med(q, n=7):
    def one():
        body = json.dumps({"db": "poc", "sql": q}).encode()
        req = urllib.request.Request(
            URL, data=body, headers={"Content-Type": "application/json"})
        t0 = time.time()
        with urllib.request.urlopen(req, timeout=120) as r:
            r.read()
        return (time.time() - t0) * 1000
    one()  # warm the metadata cache
    return statistics.median(one() for _ in range(n))


def main():
    for name, q in CASES:
        print(f"{name:32} {med(q):6.1f} ms")


if __name__ == "__main__":
    sys.exit(main())
