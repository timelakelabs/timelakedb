#!/bin/sh
# CL-2 half-open partition chaos drill (chaos step 2).
#
# The existing CL-2 drill kills a node with `docker stop`/SIGKILL — the peer's
# port refuses instantly, so the ingester learns it is gone the fast way. This
# drill injects the OTHER failure: a half-open link, where packets are
# black-holed and the connection HANGS. That path is the one a clean stop never
# reaches, and it is the one that decides whether a slow/partitioned peer can
# stall the write path. A toxiproxy sits on ingester-a's replication link
# (peer cluster_addr, :1965) and an upstream `timeout` toxic holds the frames
# so they never reach ingester-b.
#
# What it proves — the survivor-side contract under a partition (PR-7):
#   - the survivor stays UP (RR-1) and keeps acking, on local durability;
#   - it rides its per-write replication timeout (repl_timeout_ms, 250 ms) into
#     degraded mode and raises timelake_cl2_degraded — it does NOT hang;
#   - it loses nothing it acked — the exact count is on the survivor;
#   - the alarm clears on the next successful replication once the link heals,
#     one episode, no flapping.
#
# What it does NOT prove, on purpose: two-copy durability under a partition.
# The toxic is UPSTREAM, so ingester-b never receives the degraded-era frames —
# they are single-copy on the survivor until an explicit /recover. That is the
# stated PR-7 cost (availability over the second replica while the pair is half
# up), and the honest thing this drill exists to make visible, not to hide.
#
# Bring the rig up WITH the chaos overlay first (reuses the node image — the
# overlay only reroutes an env var, no rebuild):
#
#   docker compose -f deploy/compose/timelakedb-cluster-s3.yml \
#                  -f deploy/compose/timelakedb-cluster-s3.chaos.yml \
#                  up -d localstack ingester-a ingester-b toxiproxy
#   # wait for both ingesters healthy, then:
#   sh deploy/compose/cluster-drill/cl2_partition_chaos.sh \
#       | tee docs/evidence/cl2-partition-chaos-drill.log
#   docker compose -f deploy/compose/timelakedb-cluster-s3.yml \
#                  -f deploy/compose/timelakedb-cluster-s3.chaos.yml down -v
#
# Needs docker on the host; parsing is pure sh. Re-runnable without down -v:
# the table is suffixed per run and the degraded-episode count is asserted
# relative to a baseline.
set -eu

A=cl3-ingester-a
TOX=cl3-toxiproxy
PROXY=repl-a-to-b
TOXIC=partition
DATA=http://localhost:1963      # each node's own data port, reached in-container
RUN=$(date +%s)
TBL="chaos_$RUN"

pass=0; fail=0
ck() { if [ "$1" = "$2" ]; then echo "  PASS  $3"; pass=$((pass+1));
       else echo "  FAIL  $3 (got '$1' want '$2')"; fail=$((fail+1)); fi; }

# --- read the survivor (ingester-a) directly, pure sh -------------------
mA()   { docker exec "$A" curl -s "$DATA/metrics" | grep "^$1 " | awk '{print $2}' | head -1; }
upA()  { docker exec "$A" curl -s -o /dev/null -w '%{http_code}' "$DATA/health"; }
cntA() { docker exec "$A" curl -s -X POST "$DATA/api/sql" -H 'content-type: application/json' \
           -d "{\"db\":\"poc\",\"sql\":\"SELECT COUNT(*) AS n FROM $TBL\"}" \
           | grep -o '"n":[0-9]*' | head -1 | sed 's/.*://'; }
# write <count> lines from index <start>, distinct host per line, direct to A
wrA()  { s=$1; n=$2; i=0
  lp=$(while [ "$i" -lt "$n" ]; do printf '%s,host=h%s v=%si\n' "$TBL" "$((s+i))" "$((s+i))"; i=$((i+1)); done)
  printf '%s\n' "$lp" | docker exec -i "$A" curl -s -o /dev/null -w '%{http_code}' \
    -X POST "$DATA/api/v3/write_lp?db=poc&precision=ns" --data-binary @-; }
tox()  { docker exec "$TOX" /toxiproxy-cli "$@"; }
# poll metric <name> until it equals <want>, up to <secs>; echoes the last value
wait_metric() { s=$(date +%s); v=""
  while [ "$(( $(date +%s) - s ))" -lt "$3" ]; do v=$(mA "$1"); [ "$v" = "$2" ] && break; sleep 1; done
  printf '%s' "$v"; }

echo "== CL-2 half-open partition chaos drill =="
tox toxic remove -n "$TOXIC" "$PROXY" >/dev/null 2>&1 || true   # clear a prior aborted run

echo "-- baseline: replication flows through toxiproxy --"
ck "$(wrA 0 200)" 204 "pre-partition write of 200 acks"
sleep 1
ck "$(mA timelake_cl2_degraded)" 0 "cl2_degraded=0 (peer reachable through the proxy)"
ck "$(upA)" 200 "ingester-a healthy"
EV0=$(mA timelake_cl2_degraded_events_total)   # episode baseline (re-run safe)

echo "-- inject: black-hole the A->B replication link (upstream, half-open) --"
tox toxic add -t timeout -a timeout=0 -n "$TOXIC" -u "$PROXY" >/dev/null
# A's replicate now hangs -> 250 ms repl timeout -> degraded, but the write still acks
ck "$(wrA 200 50)" 204 "write under partition still acks (survivor local durability, PR-7)"
ck "$(upA)" 200 "ingester-a stays UP under partition (RR-1: the dead peer does not stall it)"
ck "$(wait_metric timelake_cl2_degraded 1 6)" 1 "cl2_degraded raised — peer unreachable via the dead link"

echo "-- heal: remove the toxic; the next write clears the alarm --"
tox toxic remove -n "$TOXIC" "$PROXY" >/dev/null
ck "$(wrA 250 200)" 204 "post-heal write acks"
ck "$(wait_metric timelake_cl2_degraded 0 6)" 0 "cl2_degraded cleared on the next successful replication"

echo "-- survivor-side zero loss --"
sleep 1
ck "$(cntA)" 450 "exact count on the survivor: 200 + 50 + 200, nothing acked was lost"
ck "$(mA timelake_cl2_degraded_events_total)" "$((EV0 + 1))" "exactly one degraded episode — no flapping"

echo "== CL-2 partition chaos: $pass passed, $fail failed =="
test "$fail" -eq 0
