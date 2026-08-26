#!/usr/bin/env python3
"""InfluxDB v1 -> TimeLakeDB migration drill (#78, acceptance criterion 3).

`ops/influxdb1-import.py` is the tool. This is the proof it works: export a
known measurement to line protocol (what `influx_inspect export -lponly`
emits), migrate it over the ordinary write path, and show `COUNT(*)` plus a
per-entity (Shape-A) spot check agree with the source EXACTLY. Then the trap
the tool exists to survive: a field whose type drifted int -> float is not
silently retyped or dropped — the offending line lands in the rejects file and
the good data still imports.

Runs from the host against a running node (default http://localhost:1963); it
is Python end to end so the corpus it writes and the file it hands the importer
are the same path (a bash drill trips over Git-Bash `/tmp` vs Windows `/tmp`).
Each run uses a fresh database so a re-run cannot read back yesterday's rows as
duplicates before compaction collapses them.

    python3 deploy/compose/migration-drill/migrate_drill.py
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request

HOSTS = 10
PER_HOST = 500
TOTAL = HOSTS * PER_HOST


def sql(url, db, q):
    body = json.dumps({"db": db, "sql": q}).encode()
    req = urllib.request.Request(
        f"{url}/api/sql", data=body, headers={"content-type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())


def run_importer(importer, url, db, path, rejects, state):
    """Run ops/influxdb1-import.py; return (exit_code, stderr_tail)."""
    cmd = [
        sys.executable, importer,
        "--url", url, "--db", db, "--file", path,
        "--precision", "ns", "--rejects", rejects, "--state", state,
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    return p.returncode, (p.stderr or "").strip().splitlines()[-1:]


def make_clean_corpus(path, base_ns):
    """A well-typed export: cpu with a float and an int field, unique
    timestamps so every point is its own row (no LWW collapse)."""
    with open(path, "w", encoding="utf-8") as f:
        n = 0
        for h in range(HOSTS):
            for i in range(PER_HOST):
                ts = base_ns + n * 1000  # unique, monotonic
                usage = round((h * 7 + i) % 100 + i / 1000.0, 3)
                f.write(f"cpu,host=h{h} usage={usage},cores={4 + (h % 4)}i {ts}\n")
                n += 1
    return n


def check(label, ok, detail=""):
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}{('  ' + detail) if detail else ''}")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:1963")
    ap.add_argument("--importer", default="ops/influxdb1-import.py")
    args = ap.parse_args()

    suffix = int(time.time())
    tmp = tempfile.mkdtemp(prefix="migrate-drill-")
    print(f"=== InfluxDB v1 -> TimeLakeDB migration drill ({time.strftime('%Y-%m-%d %H:%M:%S')}) ===")
    print(f"url={args.url}  workdir={tmp}")
    ok = True

    # --- 1. Clean corpus: exact count-match + Shape-A spot check ---
    db = f"migr_clean_{suffix}"
    corpus = os.path.join(tmp, "cpu.lp")
    n = make_clean_corpus(corpus, base_ns=1_700_000_000_000_000_000)
    print(f"\n-- clean corpus: {n} points ({HOSTS} hosts x {PER_HOST}) into db '{db}' --")
    code, tail = run_importer(
        args.importer, args.url, db, corpus,
        os.path.join(tmp, "clean-rejects.lp"), os.path.join(tmp, "clean.state"),
    )
    ok &= check("importer exits 0 (nothing rejected)", code == 0, f"exit={code} {tail}")

    total = sql(args.url, db, "SELECT COUNT(*) AS n FROM cpu")[0]["n"]
    ok &= check("COUNT(*) matches the source exactly", total == TOTAL, f"got {total}, want {TOTAL}")

    per = sql(args.url, db, "SELECT COUNT(*) AS n FROM cpu WHERE host='h5'")[0]["n"]
    ok &= check("per-entity COUNT (host=h5) matches", per == PER_HOST, f"got {per}, want {PER_HOST}")

    # Shape-A spot check: a present-entity lookup returns the field, typed.
    rows = sql(args.url, db, "SELECT usage, cores FROM cpu WHERE host='h5' ORDER BY time LIMIT 1")
    spot_ok = len(rows) == 1 and isinstance(rows[0].get("usage"), (int, float)) and rows[0].get("cores") == 4 + (5 % 4)
    ok &= check("Shape-A lookup returns the row, correctly typed", spot_ok, str(rows[:1]))

    # --- 2. The int->float drift trap: quarantined, not dropped or retyped ---
    db2 = f"migr_trap_{suffix}"
    print(f"\n-- type-drift trap: establish int, then a float point, into db '{db2}' --")
    # old data: `used` is an integer -> creates an Int64 column
    intfile = os.path.join(tmp, "mem_int.lp")
    with open(intfile, "w", encoding="utf-8") as f:
        f.write("mem,host=a used=100i 1700000000000000000\n")
    code, _ = run_importer(args.importer, args.url, db2, intfile,
                           os.path.join(tmp, "t1-rej.lp"), os.path.join(tmp, "t1.state"))
    ok &= check("integer point imports (establishes the column type)", code == 0, f"exit={code}")

    # new data: the SAME field as a float -> must be refused, not coerced
    floatfile = os.path.join(tmp, "mem_float.lp")
    with open(floatfile, "w", encoding="utf-8") as f:
        f.write("mem,host=a used=100.5 1700000000000000001\n")
    rejects = os.path.join(tmp, "t2-rej.lp")
    code, _ = run_importer(args.importer, args.url, db2, floatfile, rejects,
                           os.path.join(tmp, "t2.state"))
    ok &= check("importer exits non-zero when a line is rejected", code != 0, f"exit={code}")

    rej_txt = open(rejects, encoding="utf-8").read() if os.path.exists(rejects) else ""
    ok &= check("the drifted line is in the rejects file, not lost", "used=100.5" in rej_txt,
                repr(rej_txt[:120]))

    cnt = sql(args.url, db2, "SELECT COUNT(*) AS n FROM mem")[0]["n"]
    ok &= check("the table still holds only the good (integer) point", cnt == 1, f"got {cnt}, want 1")

    print(f"\n=== {'PASS' if ok else 'FAIL'}: migration is exact and the type-drift trap is honoured ===")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
