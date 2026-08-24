#!/usr/bin/env bash
# #64 drill: rollup materialisation in a role-split cluster (§18.6 cluster half).
#
#   deploy/compose/cluster-drill/rollup_cluster_drill.sh
#
# The claim: a role-split cluster (router + ingester pair + querier pair +
# compactor, one S3 bucket, NO `all` node) downsamples correctly. The
# COMPACTOR — not an all-node — materialises the rollup, reading the source
# through the same shard union a querier does, and the target read back
# through a querier is exact and idempotent.
#
# Source is written through the router (sharded to an ingester) with timestamps
# already older than the rollup's lookback, so the buckets are sealable at once;
# the compactor reads the ingesters' flushed data over the shared store, seals
# each bucket, and writes the target back to the store for the queriers.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
RIG="$HERE/../timelakedb-cluster-s3.yml"
ROUTER=http://localhost:5970     # writes + queries (the one public endpoint)
QUERIER=http://localhost:5973    # query a querier directly for the target
COMPACTOR=http://localhost:5975  # /health + /metrics only
DB=poc
FAIL=0

# The rollup, seeded into the compactor via env (a real one would arrive via
# /admin/rollups and propagate through the store — reload_rollups). avg + count
# of value by host, 60s buckets, 60s lookback.
export TLDB_ROLLUPS='[{"db":"poc","name":"sensor_1m","source":"sensor_reading","target":"sensor_reading_1m","interval_secs":60,"lookback_secs":60,"group_by":["host"],"aggregations":[{"function":"avg","source_column":"value","target_column":"v_avg"},{"function":"count","source_column":"value","target_column":"v_count"}]}]'

say() { printf '\n=== %s ===\n' "$*"; }
ok()  { printf '  PASS  %s\n' "$*"; }
bad() { printf '  FAIL  %s\n' "$*"; FAIL=1; }
metric() { curl -fs "$1/metrics" 2>/dev/null | awk -v n="$2" '$1==n {print $2; f=1} END{if(!f)print 0}'; }
q() { # query text -> JSON (through the given base url)
  curl -fs -X POST "$1/api/sql" -H 'content-type: application/json' \
    -d "{\"db\":\"$DB\",\"sql\":\"$2\"}" 2>/dev/null
}

cleanup() { say "tear down"; docker compose -f "$RIG" down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

say "bring the cluster up (router + ingesters + queriers + compactor + S3)"
docker compose -f "$RIG" up -d >/dev/null 2>&1 || { echo "compose up failed"; exit 1; }

say "wait for the public endpoint, a querier, and the compactor"
for url in "$ROUTER" "$QUERIER" "$COMPACTOR"; do
  for i in $(seq 1 60); do
    curl -fs "$url/health" >/dev/null 2>&1 && break
    sleep 2
    [ "$i" = 60 ] && { bad "$url never became healthy"; docker compose -f "$RIG" ps; exit 1; }
  done
done
comp_cnt=$(metric "$COMPACTOR" timelake_compactor_shard_count)
ok "cluster up; compactor sees shard_count=$comp_cnt (1 = lone compactor, owns every rollup)"

say "write 6 source rows (3 hosts x 2), pre-aged past the 60s lookback, via the router"
base=$(( ( ($(date +%s) - 300) / 60 ) * 60 ))   # a 60s bucket, 5 min ago
lp=""
h=0
for host in h1 h2 h3; do
  v1=$(( 10 + h*20 )); v2=$(( 20 + h*20 ))       # h1:10,20  h2:30,40  h3:50,60
  lp="${lp}sensor_reading,host=${host} value=${v1} $(( (base+1) * 1000000000 ))
sensor_reading,host=${host} value=${v2} $(( (base+2) * 1000000000 ))
"
  h=$((h+1))
done
code=$(curl -fs -o /dev/null -w '%{http_code}' -X POST "$ROUTER/api/v3/write_lp?db=$DB&precision=ns" --data-binary "$lp")
[ "$code" = 204 ] && ok "router accepted the write (204), sharded to an ingester" \
                  || bad "router write returned $code, expected 204"

say "wait for the compactor to materialise the sealed buckets (flush 15s, tick 30s)"
rows="null"
for i in $(seq 1 14); do
  written=$(metric "$COMPACTOR" timelake_rollup_rows_written_total)
  rows=$(q "$QUERIER" "SELECT COUNT(*) AS n FROM sensor_reading_1m" | grep -o '"n":[0-9]*' | grep -o '[0-9]*')
  echo "  t+$((i*8))s: compactor rows_written=$written; target rows via querier=${rows:-0}"
  { [ "${written:-0}" -gt 0 ] && [ "${rows:-0}" -ge 3 ]; } && break
  sleep 8
done

say "the compactor materialised it (not an all-node — there is none)"
written=$(metric "$COMPACTOR" timelake_rollup_rows_written_total)
mats=$(metric "$COMPACTOR" timelake_rollup_materializations_total)
echo "  compactor: rollup_rows_written_total=$written  materializations_total=$mats"
[ "${written:-0}" -gt 0 ] && ok "the compactor wrote rollup rows — a cluster with no all-node downsampled" \
                          || bad "compactor wrote no rollup rows"

say "the target is exact, read back through a querier and the shard union"
rows=$(q "$QUERIER" "SELECT host, v_avg, v_count FROM sensor_reading_1m ORDER BY host")
echo "  $rows"
n=$(echo "$rows" | grep -o '"host"' | wc -l | tr -d ' ')
[ "$n" = 3 ] && ok "one target row per host (3) — every shard's rows reached the compactor" \
             || bad "expected 3 target rows, got $n"
echo "$rows" | grep -q '"host":"h1".*"v_avg":15.0.*"v_count":2' && ok "h1 avg=15 count=2 (mean of 10,20)" || bad "h1 aggregate wrong"
echo "$rows" | grep -q '"host":"h3".*"v_avg":55.0.*"v_count":2' && ok "h3 avg=55 count=2 (mean of 50,60)" || bad "h3 aggregate wrong"
total=$(q "$QUERIER" "SELECT SUM(v_count) AS n FROM sensor_reading_1m" | grep -o '"n":[0-9]*' | grep -o '[0-9]*')
[ "${total:-0}" = 6 ] && ok "counts sum to 6 — all source rows accounted for, none dropped or doubled" \
                      || bad "v_count sums to ${total:-?}, expected 6"

say "idempotent: another pass changes nothing"
before=$(metric "$COMPACTOR" timelake_rollup_rows_written_total)
sleep 35   # at least one more compactor tick
after=$(metric "$COMPACTOR" timelake_rollup_rows_written_total)
cnt=$(q "$QUERIER" "SELECT COUNT(*) AS n FROM sensor_reading_1m" | grep -o '"n":[0-9]*' | grep -o '[0-9]*')
echo "  rows_written before=$before after=$after; target rows=$cnt"
[ "$before" = "$after" ] && [ "${cnt:-0}" = 3 ] \
  && ok "no new rows written and still 3 target rows — sealed buckets are not re-materialised" \
  || bad "a second pass changed the target (before=$before after=$after rows=$cnt)"

say "verdict"
if [ "$FAIL" = 0 ]; then echo "  ALL CHECKS PASSED"; else echo "  DRILL FAILED"; fi
exit "$FAIL"
