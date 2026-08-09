#!/usr/bin/env python3
"""Ad-hoc probe: hammer one SQL statement and report the latency spread.

Not a referee — the harness is. This exists to answer "is the cost I want to
attack actually on the critical path", which the log demands before any
optimisation. Run it inside the target's network namespace.
"""
import json
import statistics
import sys
import time
import urllib.request

URL = "http://localhost:1963/api/sql"
SQL = sys.argv[1]
N = int(sys.argv[2]) if len(sys.argv) > 2 else 20

times = []
for i in range(N):
    body = json.dumps({"db": "poc", "sql": SQL}).encode()
    req = urllib.request.Request(URL, data=body,
                                 headers={"Content-Type": "application/json"})
    t0 = time.perf_counter()
    rows = json.loads(urllib.request.urlopen(req).read())
    times.append((time.perf_counter() - t0) * 1000)
print(f"n={N} rows={len(rows)} min={min(times):.0f} median={statistics.median(times):.0f} "
      f"p95={sorted(times)[int(N * 0.95) - 1]:.0f} max={max(times):.0f} ms")
