#!/bin/sh
# One paired-run step of a performance cycle, on an ISOLATED instance.
#
#   perf-cycle.sh <image-tag> <label> <scale> [extra bench.py args...]
#
# Two things this rig fixes, both learned the hard way:
#
# 1. The harness runs INSIDE the target's network namespace. Measured from
#    Windows through a published port, every request carries ~45 ms of
#    Docker Desktop overhead — ~94% of the reported Shape A figure
#    (PERFORMANCE_LOG 2026-08-09 07:15).
# 2. Ingest and the query shapes run as SEPARATE harness invocations with a
#    settle in between. In one in-network pass the whole laptop run finishes
#    in 8 s, inside the 10 s flush tick, so every scan logs files_total=0 and
#    the read path is never exercised at all — the queries measure the write
#    buffer. Settling first is what makes a read-path change measurable.
set -e
IMAGE="$1"; LABEL="$2"; SCALE="$3"; shift 3
export MSYS_NO_PATHCONV=1
HERE="$(dirname "$0")"

docker rm -f tldb-perf >/dev/null 2>&1 || true
docker volume rm tldb-perf-data >/dev/null 2>&1 || true
docker volume create tldb-perf-data >/dev/null
docker run -d --name tldb-perf --memory 8g -p 2965:1963 \
  -v tldb-perf-data:/var/lib/timelord/data "$IMAGE" >/dev/null

until curl -sf http://localhost:2965/health >/dev/null 2>&1; do sleep 1; done

# phase 1: load
sh "$HERE/run-innet.sh" tldb-perf "$LABEL-load" "$SCALE" --scenarios ingest,hosts "$@"

# phase 2: settle — flush ticks at 10 s, compaction at 30 s. Wait for the
# on-disk file count to stop moving so both arms of a pair query the same
# shape of data.
count() { docker exec tldb-perf sh -c 'find /var/lib/timelord/data/objects -name "*.parquet" | wc -l'; }
sleep 35
prev=$(count)
i=0
while [ "$i" -lt 5 ]; do
  sleep 12
  cur=$(count)
  [ "$cur" = "$prev" ] && break
  prev="$cur"
  i=$((i + 1))
done
echo "--- settled: $prev parquet files ---"
docker exec tldb-perf du -sh /var/lib/timelord/data/objects 2>/dev/null || true
curl -s http://localhost:2965/metrics 2>/dev/null | grep -E "^timelord_(compactions|flushes)_total" || true

# phase 3: read path
sh "$HERE/run-innet.sh" tldb-perf "$LABEL-read" "$SCALE" --scenarios query_a,query_b "$@"
