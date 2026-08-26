#!/usr/bin/env python3
"""Import an InfluxDB v1 database into TimeLakeDB.

InfluxDB v1's data exports to line protocol, and TimeLakeDB's `/write` endpoint
IS the v1 write endpoint — the same line protocol, the same `precision` param.
So a migration is not a special back door: it is export v1 -> line protocol,
then stream that back in over the ordinary write path, so WAL durability, LWW
dedup, encryption (SEC-1) and row visibility (SEC-2) all apply exactly as they
do for Telegraf. Nothing here reaches under the engine.

STEP 1 — export the source (offline, from the data files; the node can be
stopped):

    influx_inspect export -database mydb \
        -datadir /var/lib/influxdb/data -waldir /var/lib/influxdb/wal \
        -lponly -out export.lp

  (or, against a running v1, per measurement:
    influx -database mydb -execute 'SELECT * FROM ...' -format csv  ... )

STEP 1b — InfluxDB v2 is the SAME shape: `influx export` (or the v2 read
API) emits line protocol, so point this tool at that file exactly as for
v1. The only difference is naming — a v2 bucket becomes the `--db` here;
match `--precision` to what the export wrote (`influx_inspect` emits ns).

STEP 2 — import, from a file:

    ops/influxdb1-import.py --url http://localhost:1963 --db mydb --file export.lp

  or stream it without a temp file:

    influx_inspect export -database mydb -datadir ... -waldir ... -lponly \
        -out /dev/stdout | ops/influxdb1-import.py --url http://localhost:1963 --db mydb

RESUMABLE, and that is the idempotency to rely on: pass `--state FILE` and a
killed run picks up exactly where it left off — it records how many input lines
committed and skips them on restart, so nothing is re-sent. LWW dedup is a
backstop, not the plan: it collapses exact duplicates, but at COMPACTION, not on
write, so a full re-run WITHOUT `--state` reads doubled until the next compaction
pass catches up. Import into a fresh target database and use `--state` to
resume; do not lean on LWW to undo a re-run.

THE ONE TRAP — integer->float drift. InfluxDB v1 tolerates a field whose type
changed over its life. TimeLakeDB coerces the widening cases silently (an
integer point written to a float column becomes a float), but NOT the narrowing
one: a field that was an integer in the OLD data and a float in the NEW makes
the oldest point create an INTEGER column, and every later float point is then
refused — `field 'x' type conflict: column was created with a different type
than Float(...)`. This tool does not paper over that: it BISECTS the offending
batch, writes every refused line to the rejects file with the server's reason,
and keeps going. Read that file — it is the list of points to decide a coercion
for (usually: rewrite the old integers as floats, e.g. `Ni` -> `N`, and
re-import) before you call the migration complete. Silence there would be points
quietly dropped.

Drilled end to end — a known corpus migrated, COUNT(*) and a Shape-A
lookup matching the source exactly, and the int->float drift trap landing
in the rejects file rather than being dropped: `deploy/compose/migration-drill/migrate_drill.py`
(evidence `docs/evidence/influxdb-migration-drill.log`).

Stdlib only — no pip install, runs anywhere with python3.
"""
import argparse
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def post_batch(url, db, precision, token, lines):
    """POST one batch of line-protocol lines. Returns (http_status, message)."""
    body = ("\n".join(lines) + "\n").encode("utf-8")
    q = f"{url.rstrip('/')}/write?db={urllib.parse.quote(db)}&precision={precision}"
    req = urllib.request.Request(q, data=body, method="POST")
    req.add_header("Content-Type", "text/plain; charset=utf-8")
    if token:
        # TimeLakeDB accepts the token three ways; Token is the InfluxDB v1 spelling.
        req.add_header("Authorization", f"Token {token}")
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.status, ""
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")[:600]
    except urllib.error.URLError as e:
        return -1, str(e.reason)


def send(url, db, precision, token, lines, rejects):
    """Send a batch, honouring backpressure and isolating a poison line.

    Returns the count actually committed (rejected lines are logged, not lost)."""
    delay = 0.5
    for _ in range(9):
        code, msg = post_batch(url, db, precision, token, lines)
        if code == 204:
            return len(lines)
        if code in (429, 500, 502, 503):        # backpressure / transient — never drop
            time.sleep(delay)
            delay = min(delay * 2, 30)
            continue
        if code == 400:                          # a bad/conflicting line is in here
            if len(lines) == 1:
                rejects.write(lines[0] + "\n")
                rejects.write(f"#   rejected: {msg}\n")
                rejects.flush()
                return 0
            mid = len(lines) // 2                 # bisect to find it, keep the rest
            return (send(url, db, precision, token, lines[:mid], rejects)
                    + send(url, db, precision, token, lines[mid:], rejects))
        if code in (401, 403):
            sys.exit(f"\nauth failed ({code}): {msg}\n  the server wants a token — pass --token")
        time.sleep(delay)                         # unexpected: back off and retry a few times
        delay = min(delay * 2, 30)
    sys.exit(f"\nbatch failed after retries (last {code}): {msg}")


def main():
    ap = argparse.ArgumentParser(
        description="Import InfluxDB v1 line protocol into TimeLakeDB over the write path.")
    ap.add_argument("--url", required=True, help="TimeLakeDB base URL, e.g. http://localhost:1963")
    ap.add_argument("--db", required=True, help="target database")
    ap.add_argument("--file", help="line-protocol input (default: stdin)")
    ap.add_argument("--token", help="data-plane token, if TIMELAKE_DATA_AUTH is on")
    ap.add_argument("--precision", default="ns", choices=["ns", "us", "ms", "s"],
                    help="timestamp precision of the export (influx_inspect emits ns)")
    ap.add_argument("--batch-lines", type=int, default=5000, help="lines per write request")
    ap.add_argument("--state", help="checkpoint file, for --resume across restarts")
    ap.add_argument("--rejects", default="influxdb1-import-rejects.lp",
                    help="where refused lines are written with the server's reason")
    args = ap.parse_args()

    skip = 0
    if args.state and os.path.exists(args.state):
        skip = int((open(args.state).read().strip() or "0"))
        print(f"resuming: skipping {skip:,} already-committed input lines", file=sys.stderr)

    src = open(args.file, encoding="utf-8") if args.file else sys.stdin
    rej = open(args.rejects, "a", encoding="utf-8")

    committed = rejected = read = 0
    batch = []
    t0 = time.time()

    def flush():
        nonlocal committed, rejected
        if not batch:
            return
        ok = send(args.url, args.db, args.precision, args.token, batch, rej)
        committed += ok
        rejected += len(batch) - ok
        batch.clear()
        if args.state:                            # checkpoint AFTER a durable write
            with open(args.state, "w") as s:
                s.write(str(read))

    for line in src:
        line = line.rstrip("\n")
        if not line or line.startswith("#"):      # skip blanks and any export headers/comments
            continue
        read += 1
        if read <= skip:
            continue
        batch.append(line)
        if len(batch) >= args.batch_lines:
            flush()
            el = time.time() - t0
            print(f"\r  {committed:,} committed  {committed / el:,.0f} lines/s"
                  f"  {rejected} rejected", end="", file=sys.stderr)
    flush()

    el = max(time.time() - t0, 1e-6)
    print(f"\ndone: {committed:,} lines in {el:.1f}s ({committed / el:,.0f}/s), "
          f"{rejected} rejected" + (f" — see {args.rejects}" if rejected else ""),
          file=sys.stderr)
    sys.exit(1 if rejected else 0)


if __name__ == "__main__":
    main()
