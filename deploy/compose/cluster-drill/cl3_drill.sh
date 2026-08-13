#!/bin/sh
# C2 phase 4 drill: the stateless querier (CL-3).
#
# The claim under test is the one the whole role split stands on: splitting
# reads off the write path does NOT cost freshness or exactness. So:
#   - write through the router (sharded across the ingester pair) and query
#     a querier IMMEDIATELY — every row must be there while it still exists
#     only in an ingester's memory;
#   - both queriers agree, and the router's forwarded answer matches;
#   - wait for the flush and ask again — rows crossed from memory to the
#     bucket with no loss and no double-count (the vanish window is what the
#     head watermark closes);
#   - kill a querier: reads continue (the router falls through);
#   - recreate a querier with an EMPTY disk: it rebuilds from the bucket
#     alone and answers exactly (CL-4);
#   - kill an ingester: the querier REFUSES rather than answering short —
#     the deliberate opposite of the write path's degraded mode;
#   - a write sent to a querier is refused (501), not accepted nowhere.
#
# Run from the HOST, from deploy/compose:
#   docker compose -f timelakedb-cluster-s3.yml up -d --build
#   sh cluster-drill/cl3_drill.sh
set -e
R=${R:-http://localhost:5970}          # router
QA=${QA:-http://localhost:5973}        # querier-a
QB=${QB:-http://localhost:5974}        # querier-b
IA=${IA:-http://localhost:5981}        # ingester-a internal
IB=${IB:-http://localhost:5982}        # ingester-b internal
COMPOSE=${COMPOSE:-timelakedb-cluster-s3.yml}
TABLES=${TABLES:-8}
PER=${PER:-400}
TOTAL=$((TABLES*PER))
RUN=$(date +%s)

pass=0; fail=0
chk() { if [ "$1" = "$2" ]; then echo "  PASS  $3"; pass=$((pass+1));
        else echo "  FAIL  $3 (got '$1' want '$2')"; fail=$((fail+1)); fi; }

# COUNT(*) across every table this run wrote, from one endpoint.
total_rows() {
  _u=$1; _sum=0; _i=0
  while [ "$_i" -lt "$TABLES" ]; do
    _n=$(curl -s -X POST "$_u/api/sql" -H 'content-type: application/json' \
      -d "{\"db\":\"poc\",\"sql\":\"SELECT COUNT(*) AS n FROM t${RUN}_${_i}\"}" 2>/dev/null \
      | python -c "import sys,json
try:
    d=json.load(sys.stdin); print(d[0]['n'] if isinstance(d,list) and d else 0)
except Exception: print(0)")
    _sum=$((_sum + _n)); _i=$((_i+1))
  done
  echo "$_sum"
}
metric() { curl -s "$1/metrics" | grep "^$2 " | awk '{print $2}' | head -1; }
live_tables() { curl -s "$1/internal/v1/live" | python -c "import sys,json; print(len(json.load(sys.stdin)['tables']))"; }
sql_code() {
  curl -s -o /dev/null -w "%{http_code}" -X POST "$1/api/sql" \
    -H 'content-type: application/json' \
    -d "{\"db\":\"poc\",\"sql\":\"SELECT COUNT(*) AS n FROM t${RUN}_0\"}"
}

echo "== C2 phase 4: the stateless querier =="

# Preflight. This drill deliberately stops and recreates containers, so a
# back-to-back second run can otherwise start against a node that is still
# coming up — and measure the drill's own wake rather than the database.
echo "-- waiting for every node --"
for u in "$R/health" "$QA/health" "$QB/health" \
         "http://localhost:5971/health" "http://localhost:5972/health"; do
  i=0
  while [ "$i" -lt 60 ]; do curl -fs "$u" >/dev/null 2>&1 && break; sleep 1; i=$((i+1)); done
  [ "$i" -lt 60 ] || { echo "  ABORT  $u never came up"; exit 1; }
done

REFUSALS_BEFORE=$(metric "$QA" timelake_querier_refusals_total)
REFUSALS_BEFORE=${REFUSALS_BEFORE:-0}

echo "-- writing $TOTAL lines across $TABLES tables through the router --"
python - "$R" "$RUN" "$TABLES" "$PER" <<'PY'
import sys, time, urllib.request
r, run, tables, per = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
t0 = int(time.time()*1e9) - tables*per*1_000_000
lines, k = [], 0
for tbl in range(tables):
    for i in range(per):
        lines.append(f"t{run}_{tbl},host=h{i%10} v={i}i {t0 + k*1_000_000}")
        k += 1
body = "\n".join(lines) + "\n"
req = urllib.request.Request(f"{r}/api/v3/write_lp?db=poc&precision=ns",
                             data=body.encode(), method="POST")
with urllib.request.urlopen(req, timeout=120) as resp:
    assert resp.status == 204, resp.status
print(f"acked {tables*per} lines across {tables} tables through the router")
PY

echo "-- freshness: the rows exist ONLY in ingester memory right now --"
chk "$(total_rows "$QA")" "$TOTAL" "querier-a answers exactly, with nothing flushed yet"
chk "$(total_rows "$QB")" "$TOTAL" "querier-b agrees (stateless nodes are interchangeable)"
chk "$(total_rows "$R")"  "$TOTAL" "the router's forwarded query gives the same answer"

# Sharding evidence that a shared catalog cannot blur: each ingester's own
# live buffer. Every table is on exactly one of them.
# Prove the rows travelled as live rows, not as files: the counter only
# moves when batches actually cross the wire from an ingester's buffer.
ROWS_OVER_WIRE=$(metric "$QA" timelake_querier_snapshot_rows_total)
chk "$([ "${ROWS_OVER_WIRE:-0}" -ge "$TOTAL" ] && echo yes || echo no)" "yes" \
    "the answer came from live buffers over the wire (rows fetched=$ROWS_OVER_WIRE)"

LA=$(live_tables "$IA"); LB=$(live_tables "$IB")
chk "$((LA + LB))" "$TABLES" "each table is live on exactly ONE ingester (A=$LA B=$LB)"
chk "$([ "$LA" -ge 1 ] && [ "$LB" -ge 1 ] && echo yes || echo no)" "yes" "tables DISTRIBUTED across the pair"
chk "$(metric "$QA" timelake_querier_ingesters)" "2" "querier-a reads both ingesters"
# Counters are cumulative and this drill deliberately causes a refusal later
# on, so a repeat run against the same container starts above zero: compare
# against the baseline taken before the writes, never against 0.
chk "$(metric "$QA" timelake_querier_refusals_total)" "$REFUSALS_BEFORE" \
    "no NEW refusals while the cluster is whole (baseline $REFUSALS_BEFORE)"

echo "-- the flush: rows cross from memory to the bucket --"
HEAD_BEFORE=$(metric "$QA" timelake_catalog_head)
i=0; while [ "$i" -lt 40 ]; do
  sleep 2
  [ "$(metric "$QA" timelake_catalog_head)" != "$HEAD_BEFORE" ] && break
  i=$((i+1))
done
HEAD_AFTER=$(metric "$QA" timelake_catalog_head)
chk "$([ "${HEAD_AFTER:-0}" -gt "${HEAD_BEFORE:-0}" ] && echo yes || echo no)" "yes" \
    "the querier followed the ingesters' commits (head $HEAD_BEFORE -> $HEAD_AFTER)"
chk "$(total_rows "$QA")" "$TOTAL" "still EXACT after the flush — no vanish, no double-count"
chk "$(total_rows "$QB")" "$TOTAL" "querier-b too"

echo "-- a querier dies: reads continue --"
docker stop cl3-querier-a >/dev/null
chk "$(total_rows "$QB")" "$TOTAL" "querier-b keeps answering"
chk "$(total_rows "$R")"  "$TOTAL" "the router falls through to the live querier"
docker start cl3-querier-a >/dev/null
i=0; while [ "$i" -lt 20 ]; do curl -fs "$QA/health" >/dev/null 2>&1 && break; sleep 1; i=$((i+1)); done
chk "$(total_rows "$QA")" "$TOTAL" "the restarted querier answers exactly again"

echo "-- CL-4: a querier rebuilt from an EMPTY disk --"
docker compose -f "$COMPOSE" up -d --force-recreate --no-deps querier-b >/dev/null 2>&1
i=0; while [ "$i" -lt 30 ]; do curl -fs "$QB/health" >/dev/null 2>&1 && break; sleep 1; i=$((i+1)); done
chk "$(total_rows "$QB")" "$TOTAL" "a fresh container rebuilt the whole view from the bucket"

echo "-- an ingester dies: the querier refuses rather than under-counting --"
# Fresh baseline: querier-a was restarted above, which zeroed its counters.
REFUSALS_BEFORE=$(metric "$QA" timelake_querier_refusals_total)
REFUSALS_BEFORE=${REFUSALS_BEFORE:-0}
docker stop cl3-ingester-b >/dev/null
CODE=$(sql_code "$QA")
chk "$([ "$CODE" != "200" ] && echo yes || echo no)" "yes" "the query is refused, not answered short (HTTP $CODE)"
REFUSALS_NOW=$(metric "$QA" timelake_querier_refusals_total)
chk "$([ "${REFUSALS_NOW:-0}" -gt "$REFUSALS_BEFORE" ] && echo yes || echo no)" "yes" \
    "the refusal is counted (alertable): $REFUSALS_BEFORE -> $REFUSALS_NOW"
docker start cl3-ingester-b >/dev/null
i=0; while [ "$i" -lt 30 ]; do curl -fs "http://localhost:5972/health" >/dev/null 2>&1 && break; sleep 1; i=$((i+1)); done
sleep 3
chk "$(total_rows "$QA")" "$TOTAL" "counts return, exact, once the ingester is back"

echo "-- a querier takes no writes --"
WCODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$QA/api/v3/write_lp?db=poc" \
  --data-binary "nope,host=a v=1i")
chk "$WCODE" "501" "a write to a querier is refused (501), not accepted nowhere"

echo "== phase 4 verdict =="
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ] && echo "PHASE 4: PASS" || echo "PHASE 4: FAIL"
