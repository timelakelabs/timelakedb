#!/bin/sh
# C2 phase 3 drill: the router shards writes across the ingester pair.
#
# The router is the single public write endpoint (localhost:5962). It hashes
# each measurement to one ingester, which becomes that table's primary and
# replicates to its CL-2 peer. We write many tables through the router, then
# confirm:
#   - every table's rows landed, exactly (no loss, no double-count);
#   - tables are DISTRIBUTED — some primary on A, some on B (real sharding);
#   - each table lives on exactly one primary (its replica is dormant);
#   - a poison line is rejected atomically (nothing written);
#   - the router routed queries nowhere (501) — that is phase 4.
#
# Ingesters use local stores here, so we verify by querying each ingester
# directly (a cluster-wide query needs a querier, phase 4).
# Run from the HOST. localhost:5962=router, 5963=A, 5964=B.
#   sh router_drill.sh
set -e
R=${R:-http://localhost:5962}
A=${A:-http://localhost:5963}
B=${B:-http://localhost:5964}
TABLES=${TABLES:-12}
PER=${PER:-500}
RUN=$(date +%s)

pass=0; fail=0
chk() { if [ "$1" = "$2" ]; then echo "  PASS  $3"; pass=$((pass+1));
        else echo "  FAIL  $3 (got '$1' want '$2')"; fail=$((fail+1)); fi; }

rows() {  # $1=url $2=table
  curl -s -X POST "$1/api/sql" -H 'content-type: application/json' \
    -d "{\"db\":\"poc\",\"sql\":\"SELECT COUNT(*) AS n FROM $2\"}" 2>/dev/null \
    | python -c "import sys,json
try:
    d=json.load(sys.stdin); print(d[0]['n'] if isinstance(d,list) and d else 0)
except Exception: print(0)"
}
metric() { curl -s "$1/metrics" | sed 's/\x1b\[[0-9;]*m//g' | grep "^$2 " | awk '{print $2}' | head -1; }

echo "== C2 phase 3: router write sharding =="

# One mixed body: TABLES measurements, PER lines each, all sent to the ROUTER.
python - "$R" "$RUN" "$TABLES" "$PER" <<'PY'
import sys, time, urllib.request
r, run, tables, per = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
t0 = int(time.time()*1e9) - tables*per*1_000_000
lines = []
k = 0
for tbl in range(tables):
    for i in range(per):
        lines.append(f"t{run}_{tbl},host=h{i%10} v={i}i {t0 + k*1_000_000}")
        k += 1
body = "\n".join(lines) + "\n"
req = urllib.request.Request(f"{r}/api/v3/write_lp?db=poc&precision=ns",
                             data=body.encode(), method="POST")
with urllib.request.urlopen(req, timeout=60) as resp:
    assert resp.status == 204, resp.status
print(f"acked {tables*per} lines across {tables} tables through the router")
PY

# For each table: exactly one of A/B is its primary (has the rows), the other
# holds a dormant replica (0 queryable). Count where each table landed.
onA=0; onB=0; split_ok=1; total=0
i=0
while [ "$i" -lt "$TABLES" ]; do
  T="t${RUN}_${i}"
  ra=$(rows "$A" "$T"); rb=$(rows "$B" "$T")
  total=$((total + ra + rb))
  if [ "$ra" = "$PER" ] && [ "$rb" = "0" ]; then onA=$((onA+1));
  elif [ "$rb" = "$PER" ] && [ "$ra" = "0" ]; then onB=$((onB+1));
  else split_ok=0; echo "    table $T mis-sharded: A=$ra B=$rb"; fi
  i=$((i+1))
done

chk "$split_ok" "1" "every table lives on exactly ONE primary (the other holds a dormant replica)"
chk "$total" "$((TABLES*PER))" "exact accounting: every acked line present once, no loss/dup"
chk "$([ "$onA" -ge 1 ] && [ "$onB" -ge 1 ] && echo yes || echo no)" "yes" "tables DISTRIBUTED across both ingesters (A=$onA, B=$onB)"

FWD=$(metric "$R" timelake_router_forwarded_total)
chk "$([ "${FWD:-0}" -ge 2 ] && echo yes || echo no)" "yes" "router forwarded to >1 shard (forwarded_total=$FWD)"
chk "$(metric "$R" timelake_router_ingesters)" "2" "router sees 2 ingesters"

echo "-- atomicity: a poison line rejects the whole batch --"
PT="poison_${RUN}"
CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$R/api/v3/write_lp?db=poc" \
  --data-binary "$PT,host=a v=1i
$PT this-is-not-valid-lp
$PT,host=b v=2i")
chk "$CODE" "400" "poison line -> 400 at the router"
chk "$(( $(rows "$A" "$PT") + $(rows "$B" "$PT") ))" "0" "nothing from the poison batch was written (atomic)"

echo "-- queries are not routed (phase 4) --"
QCODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$R/api/sql" -H 'content-type: application/json' -d '{"db":"poc","sql":"SELECT 1"}')
chk "$QCODE" "501" "router returns 501 for /api/sql (query routing is phase 4)"

echo "== phase 3 verdict =="
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ] && echo "PHASE 3: PASS" || echo "PHASE 3: FAIL"
