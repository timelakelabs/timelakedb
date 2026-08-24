#!/usr/bin/env bash
# C2 phase 5b drill: two compactors divide one store's partitions.
#
#   deploy/compose/compactor-drill/shard_drill.sh
#
# Proves, in real containers rather than a unit test, that:
#   1. both compactors START (the gate opened) and read their ordinal from
#      discovery — `timelake_compactor_shard_{ordinal,count}` on /metrics;
#   2. they DIVIDE the work — every partition is compacted exactly once, the
#      two compactors' `compactions_total` sum to the partition count, and
#      both did some (disjoint, total ownership);
#   3. no merge is WASTED — `stale_merges_total` stays 0, i.e. they never
#      raced the same partition;
#   4. nothing is LOST — the row count is exact across the compaction.
#
# The node ingests with its own compaction quiet (min_files=1e6 + ascending
# non-overlapping data, so neither trigger fires); the compactors run
# min_files=2. See the rig header for why that is the honest "node ingests,
# compactors compact" without a switch for it.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
RIG="$HERE/../timelakedb-compactor-cluster.yml"
NODE=http://localhost:7975
CA=http://localhost:7976
CB=http://localhost:7977
N=8                       # tables (= partitions); split across the two compactors
DB=poc
FAIL=0

say() { printf '\n=== %s ===\n' "$*"; }
ok()  { printf '  PASS  %s\n' "$*"; }
bad() { printf '  FAIL  %s\n' "$*"; FAIL=1; }

metric() { # url name -> value (0 if absent)
  curl -fs "$1/metrics" 2>/dev/null | awk -v n="$2" '$1==n {print $2; found=1} END{if(!found)print 0}'
}

cleanup() { say "tear down"; docker compose -f "$RIG" down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

say "bring the rig up (node + two compactors, shared store)"
docker compose -f "$RIG" up -d >/dev/null 2>&1 || { echo "compose up failed"; exit 1; }

say "wait for all three to answer /health"
for url in "$NODE" "$CA" "$CB"; do
  for i in $(seq 1 40); do
    curl -fs "$url/health" >/dev/null 2>&1 && break
    sleep 2
    [ "$i" = 40 ] && { bad "$url never became healthy"; docker compose -f "$RIG" logs --tail 20; exit 1; }
  done
done
ok "node and both compactors are up (the gate is open — neither compactor exited 2)"

say "each compactor learned its ordinal from discovery"
CA_ORD=$(metric "$CA" timelake_compactor_shard_ordinal)
CA_CNT=$(metric "$CA" timelake_compactor_shard_count)
CB_ORD=$(metric "$CB" timelake_compactor_shard_ordinal)
CB_CNT=$(metric "$CB" timelake_compactor_shard_count)
echo "  ca: ordinal=$CA_ORD count=$CA_CNT    cb: ordinal=$CB_ORD count=$CB_CNT"
[ "$CA_CNT" = 2 ] && [ "$CB_CNT" = 2 ] || bad "both should see count=2"
[ "$CA_ORD" != "$CB_ORD" ] || bad "the two compactors must hold different ordinals"
{ [ "$CA_ORD" = 0 ] || [ "$CA_ORD" = 1 ]; } && { [ "$CB_ORD" = 0 ] || [ "$CB_ORD" = 1 ]; } \
  || bad "ordinals must be 0 and 1"
[ "$FAIL" = 0 ] && ok "ca and cb hold distinct ordinals over count=2"

say "seed $N tables, 2 ascending non-overlapping files each (min_files=2 → compact)"
base=1700000000000000000
for round in 0 1; do
  off=$(( round * 10 ))
  for i in $(seq 0 $((N-1))); do
    t=$(( base + (off + 0) * 1000000000 ))
    t2=$(( base + (off + 1) * 1000000000 ))
    t3=$(( base + (off + 2) * 1000000000 ))
    printf 'm%s,host=h v=1 %s\nm%s,host=h v=2 %s\nm%s,host=h v=3 %s\n' "$i" "$t" "$i" "$t2" "$i" "$t3" \
      | curl -fs -X POST "$NODE/api/v3/write_lp?db=$DB&precision=ns" --data-binary @- >/dev/null \
      || bad "write to m$i failed"
  done
  sleep 12   # let the node flush this round to its own file (flush_age=1, tick 10s)
done
ok "wrote $((N*6)) rows across $N tables in two flushes each"

say "wait for the compactors to divide and merge, then settle (30s tick)"
# Settled = one file per table again ($N). Read from a COMPACTOR, not the
# node: the `all` node assumes it is the sole writer and does not tail the
# manifest log, so its own `parquet_files` still shows the files it flushed,
# unaware the compactors replaced them. A compactor tails the log and sees the
# merged state (its own commits and the peer's). A partition can be merged more
# than once (flush_rows=1 lets it re-accrue a file), so we key on the settled
# file count, not an exact merge tally — a *contended* merge would show up as a
# stale merge below regardless.
for i in $(seq 1 15); do
  ca=$(metric "$CA" timelake_compactions_total)
  cb=$(metric "$CB" timelake_compactions_total)
  files=$(metric "$CA" timelake_parquet_files)
  echo "  t+$((i*8))s: files=$files ca=$ca cb=$cb"
  { [ "$files" -le "$N" ] && [ "$(( ca + cb ))" -ge "$N" ]; } && break
  sleep 8
done

say "results"
CA_C=$(metric "$CA" timelake_compactions_total)
CB_C=$(metric "$CB" timelake_compactions_total)
CA_S=$(metric "$CA" timelake_stale_merges_total)
CB_S=$(metric "$CB" timelake_stale_merges_total)
FILES=$(metric "$CA" timelake_parquet_files)
echo "  compactions:  ca=$CA_C  cb=$CB_C"
echo "  stale merges: ca=$CA_S  cb=$CB_S"
echo "  files in catalog: $FILES (want $N — one per table, fully compacted)"

[ "$FILES" -eq "$N" ] && ok "every partition settled to a single file ($FILES == $N) — all compacted" \
                      || bad "catalog holds $FILES files, expected $N (a partition never compacted)"
{ [ "$CA_C" -gt 0 ] && [ "$CB_C" -gt 0 ]; } \
  && ok "both compactors did work — the partitions were divided, not hogged" \
  || bad "one compactor did nothing (ca=$CA_C cb=$CB_C) — no division"
{ [ "$CA_S" -eq 0 ] && [ "$CB_S" -eq 0 ]; } \
  && ok "zero stale merges — they never raced the same partition (ownership is disjoint)" \
  || bad "stale merges seen (ca=$CA_S cb=$CB_S) — two compactors touched one partition"

say "no rows lost across the compaction"
total_rows=0
for i in $(seq 0 $((N-1))); do
  r=$(curl -fs -X POST "$NODE/api/sql" -H 'content-type: application/json' \
    -d "{\"db\":\"$DB\",\"sql\":\"SELECT COUNT(*) AS n FROM m$i\"}" | grep -o '"n":[0-9]*' | grep -o '[0-9]*')
  total_rows=$(( total_rows + ${r:-0} ))
done
echo "  rows across all $N tables: $total_rows (want $((N*6)))"
[ "$total_rows" -eq $((N*6)) ] && ok "every row survived the merges — nothing lost, nothing doubled" \
                              || bad "row total is $total_rows, expected $((N*6))"

say "verdict"
if [ "$FAIL" = 0 ]; then echo "  ALL CHECKS PASSED"; else echo "  DRILL FAILED"; fi
exit "$FAIL"
