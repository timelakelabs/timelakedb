#!/bin/sh
# CL-2 drill: the ingester pair's two contract properties.
#
#   Leg 1 (degraded): the peer down must NOT fail writes — the node keeps
#     accepting on local durability, raises the CL2_REPLICATION_DEGRADED
#     alarm, and clears it when the peer returns (PR-7: availability outranks
#     the second replica when the pair is half up).
#   Leg 2 (zero loss): with both healthy, every acked write is durable on
#     both nodes, so SIGKILL-ing one and recovering on the other loses
#     nothing — exact count.
#
# Topology: ingester-a and ingester-b, each the other's replication peer.
# We write to A directly (the router that would front them is C2 phase 3).
# Run from the HOST (needs docker start/stop/kill); the ingesters publish
# their DATA ports (localhost:5963=A, 5964=B). The internal listener
# (:1965) is not published — it is reached via `docker exec` (exposure 10).
#   sh cl2_drill.sh
set -e
A=${A:-http://localhost:5963}
B=${B:-http://localhost:5964}
# The internal listener (:1965) is NOT published to the host (SECURITY.md
# exposure 10). Reach it from INSIDE the container instead — this is the
# only path a drill should use, and it matches the private-network posture
# the guidance requires.
recover_on_b() { docker exec ingester-b curl -s -X POST http://localhost:1965/internal/v1/recover; }
N=${N:-20000}
RUN=$(date +%s)

pass=0; fail=0
chk() { if [ "$1" = "$2" ]; then echo "  PASS  $3"; pass=$((pass+1));
        else echo "  FAIL  $3 (got '$1' want '$2')"; fail=$((fail+1)); fi; }

rows() {  # $1 = base url, $2 = table
  curl -s -X POST "$1/api/sql" -H 'content-type: application/json' \
    -d "{\"db\":\"poc\",\"sql\":\"SELECT COUNT(*) AS n FROM $2\"}" 2>/dev/null \
    | python -c "import sys,json
try:
    d=json.load(sys.stdin); print(d[0]['n'] if isinstance(d,list) and d else 0)
except Exception: print(0)"
}
metric() { curl -s "$1/metrics" | sed 's/\x1b\[[0-9;]*m//g' | grep "^$2 " | awk '{print $2}' | head -1; }

# write `count` lines of `table` to `url`; every 204 = acked
write() {  # $1=url $2=table $3=count
  python - "$1" "$2" "$3" <<'PY'
import sys, time, urllib.request
url, table, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
t0 = int(time.time()*1e9) - n*1_000_000
B = 5000
for start in range(0, n, B):
    body = "".join(f"{table},host=h{i%50} v={i}i {t0+i*1_000_000}\n"
                   for i in range(start, min(start+B, n)))
    req = urllib.request.Request(f"{url}/api/v3/write_lp?db=poc&precision=ns",
                                 data=body.encode(), method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        assert r.status == 204, r.status
PY
}

echo "== CL-2 drill =="

# ---- Leg 1: degraded mode (peer down, keep serving) ----
echo "-- leg 1: peer down -> degraded, not failed --"
DT="cl2_degraded_$RUN"
docker stop ingester-b >/dev/null 2>&1
sleep 2
write "$A" "$DT" 4000   # succeeds (204) despite B being down, or the assert trips
chk "$(rows "$A" "$DT")" "4000" "writes kept flowing on local durability while the peer was down"
chk "$(metric "$A" timelake_cl2_degraded)" "1" "A is degraded (gauge=1)"
DEGE=$(metric "$A" timelake_cl2_degraded_events_total)
chk "$([ "${DEGE:-0}" -ge 1 ] && echo yes || echo no)" "yes" "A recorded entering degraded ($DEGE)"
chk "$(docker logs ingester-a 2>&1 | grep -c 'CL2_REPLICATION_DEGRADED')" "1" "named alarm CL2_REPLICATION_DEGRADED logged once"

echo "-- peer back -> degraded clears --"
docker start ingester-b >/dev/null 2>&1
until curl -fs "$B/health" >/dev/null 2>&1; do sleep 1; done
write "$A" "$DT" 100    # a write after recovery flips the gauge back
sleep 1
chk "$(metric "$A" timelake_cl2_degraded)" "0" "A left degraded once the peer returned"

# ---- Leg 2: zero acknowledged loss on a node death ----
echo "-- leg 2: both healthy, SIGKILL A, recover on B --"
T="cl2_$RUN"
write "$A" "$T" "$N"
chk "$(rows "$A" "$T")" "$N" "A has every acked line"
REPL=$(metric "$A" timelake_cl2_replicated_total)
chk "$([ "${REPL:-0}" -ge 1 ] && echo yes || echo no)" "yes" "A replicated to B (replicated_total=$REPL)"
chk "$(metric "$A" timelake_cl2_degraded)" "0" "A is not degraded (peer healthy)"
BFRAMES=$(metric "$B" timelake_cl2_replica_frames_total)
chk "$([ "${BFRAMES:-0}" -ge 1 ] && echo yes || echo no)" "yes" "B received frames into its replica WAL ($BFRAMES)"
chk "$(rows "$B" "$T")" "0" "B has NOT applied the replica frames (dormant, no double-flush)"

echo "-- SIGKILL ingester-a --"
docker kill ingester-a >/dev/null 2>&1 || true
sleep 2
REC=$(recover_on_b); echo "  recover: $REC"
sleep 1
chk "$(rows "$B" "$T")" "$N" "ZERO ACKED LOSS: every line A acked is queryable on B after recovery"
RECN=$(metric "$B" timelake_cl2_recovered_total)
chk "$([ "${RECN:-0}" -ge 1 ] && echo yes || echo no)" "yes" "B recovered the peer's frames (recovered_total=$RECN)"

echo "== CL-2 verdict =="
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ] && echo "CL-2: PASS" || echo "CL-2: FAIL"
