#!/bin/sh
# U3 cluster-view drill (timelakedb#111 §13 gate).
#
# The console's Cluster screen answers from GET /admin/cluster, which fans
# a node's own health out to every peer it was told about (a 3 s per-peer
# timeout) and folds the answers into one view. This drill pins the four
# properties that view owes an operator — the ones a unit test can't reach
# because they need two real nodes and a kill:
#
#   Gate 1 (membership): a node lists itself and its peer, each with the
#     right role, and reports config_converged while both sit at the same
#     revision.
#   Gate 2 (divergence flagged): advance one node's applied config; the
#     other does NOT follow (config is per-node — a node reloads its own
#     layers, not its peers'), so the view must show the revisions apart
#     and config_converged=false. "A node held at an old revision is
#     flagged" is the whole point of the column.
#   Gate 3 (a dead node shows, fast): SIGSTOP the peer, and within the 10 s
#     the ticket names the view must read it unreachable — still carrying
#     its role, not dropped from the list.
#   Gate 4 (CL-5 guard): the membership view is advisory. With the peer
#     down and the view degraded, a write to the survivor still acks and
#     reads back exact — the view changed nothing about write or catalog
#     correctness. This is the invariant the whole cluster rests on; a
#     drill that showed the view working but not this would be reassuring
#     about the wrong thing.
#
# Topology: the CL-3 rig's ingester pair (each the other's only peer). The
# admin listener is loopback by default (SECURITY.md — it is never public),
# so every /admin/* call goes in through `docker exec` on the node, exactly
# as an operator on the box would reach it; the wedge (gate 3) and the kill
# (gate 4) are `docker pause`/`docker stop` from the host. Run from the repo
# root — build BOTH nodes, or a stale per-service image serves an older
# /health and the peer shows up roleless (see the evidence log's detour):
#
#   docker compose -f deploy/compose/timelakedb-cluster-s3.yml \
#       up -d --build localstack ingester-a ingester-b
#   # wait for both /health, then:
#   sh deploy/compose/cluster-drill/u3_cluster_drill.sh | tee docs/evidence/u3-cluster-drill.log
#   docker compose -f deploy/compose/timelakedb-cluster-s3.yml down -v
#
# Needs docker on the host; the JSON parsing is pure sh, no python.
set -eu

A=cl3-ingester-a                 # container names (the rig sets container_name)
B=cl3-ingester-b
ADMIN=http://localhost:1966      # admin listener, loopback INSIDE the node
DATA=http://localhost:1963       # the node's own data port (reached in-container)
RUN=$(date +%s)

pass=0; fail=0
ck() { if [ "$1" = "$2" ]; then echo "  PASS  $3"; pass=$((pass+1));
       else echo "  FAIL  $3 (got '$1' want '$2')"; fail=$((fail+1)); fi; }

# --- JSON helpers (read the response on stdin) -------------------------
# The payloads are flat, compact JSON — a node object holds only scalars, so
# the only "},{" is the separator between nodes. Pure sh, no python: the real
# drill host and this one agree, and there is one less thing to install.
jconv()  { grep -o '"config_converged":[^,}]*' | sed 's/.*://'; }
jcount() { grep -o '"id":"[^"]*"' | wc -l | tr -d '[:space:]'; }
jnode()  {  # <node_id> <field>: that node's field, "" if absent
  sed 's/},{/}\n{/g' | grep "\"id\":\"$1\"" \
    | grep -o "\"$2\":[^,}]*" | head -1 | sed 's/^"[^"]*"://; s/^"//; s/"$//'
}
jn()     { grep -o '"n":[0-9]*' | head -1 | sed 's/.*://'; }

# --- admin plane: reached through the node, cookie jar lives there -----
adm() { docker exec "$A" curl -s "$@"; }
CSRF=""
login() {  # $1 = password; sets CSRF, drops the session cookie at /tmp/cj
  body=$(adm -c /tmp/cj -X POST "$ADMIN/admin/session" -H 'content-type: application/json' \
              -d "{\"username\":\"admin\",\"password\":\"$1\"}")
  CSRF=$(printf '%s' "$body" | sed -n 's/.*"csrf":"\([^"]*\)".*/\1/p')
}
cluster() { adm -b /tmp/cj "$ADMIN/admin/cluster"; }

echo "== U3 cluster-view drill =="

# SEC-4: first login is quarantined to the password change. Rotate off the
# seed, then sign in for real — rotation kills the seeding session too.
login admin
adm -b /tmp/cj -X POST "$ADMIN/admin/password" -H 'content-type: application/json' \
    -H "x-timelake-csrf: $CSRF" \
    -d '{"current_password":"admin","new_password":"drill console password"}' >/dev/null
login "drill console password"
[ -n "$CSRF" ] || { echo "  FAIL  could not establish an admin session"; exit 1; }

echo "-- gate 1: membership, both nodes at rev 0 --"
CV=$(cluster)
ck "$(printf '%s' "$CV" | jcount)"                     2         "view lists both nodes"
ck "$(printf '%s' "$CV" | jnode ingester-a reachable)" true      "ingester-a (self) reachable"
ck "$(printf '%s' "$CV" | jnode ingester-a role)"      ingester  "ingester-a role is ingester"
ck "$(printf '%s' "$CV" | jnode ingester-b reachable)" true      "ingester-b (peer) reachable"
ck "$(printf '%s' "$CV" | jnode ingester-b role)"      ingester  "ingester-b role is ingester"
ck "$(printf '%s' "$CV" | jconv)"                      true      "config_converged while both at rev 0"

echo "-- gate 2: advance A's config; B does not follow --"
adm -b /tmp/cj -X PUT "$ADMIN/admin/config/gc_grace_secs" -H 'content-type: application/json' \
    -H "x-timelake-csrf: $CSRF" -d '{"value":1500}' >/dev/null
CV=$(cluster)
ck "$(printf '%s' "$CV" | jnode ingester-a config_revision)" 1     "ingester-a advanced to rev 1"
ck "$(printf '%s' "$CV" | jnode ingester-b config_revision)" 0     "ingester-b held at rev 0"
ck "$(printf '%s' "$CV" | jconv)"                            false "divergence flagged (config_converged=false)"

echo "-- gate 3: wedge B, it must read unreachable within 10 s --"
# `pause`, not `stop`: a frozen node black-holes the SYN, so detection has to
# ride the 3 s per-peer timeout — the path the 10 s bound actually governs. A
# cleanly stopped node refuses instantly and would pass no matter how long the
# timeout were set, which is the regression this gate is here to catch.
docker pause "$B" >/dev/null
# Wall-clock, not a poll count: the detection latency is the per-peer timeout
# that elapses INSIDE the first cluster() call (a wedged peer black-holes the
# SYN), so counting loop iterations would report ~0 and hide the very number
# the 10 s bound is about.
start=$(date +%s); r=unknown
while [ "$(( $(date +%s) - start ))" -lt 10 ]; do
  r=$(cluster | jnode ingester-b reachable)
  [ "$r" = "false" ] && break
done
elapsed=$(( $(date +%s) - start ))
ck "$r" false "wedged ingester-b reads unreachable in ${elapsed}s (<=10)"
CV=$(cluster)
ck "$(printf '%s' "$CV" | jnode ingester-b status)" unreachable "ingester-b status=unreachable"
ck "$(printf '%s' "$CV" | jnode ingester-b role)"   ingester    "unreachable node keeps its role"
docker unpause "$B" >/dev/null

echo "-- gate 4: CL-5 — degraded view, writes still exact on the survivor --"
docker stop "$B" >/dev/null   # peer truly down now: replication refuses fast,
                              # A stays up on local durability (PR-7, degraded)
TBL="u3guard_$RUN"   # fresh table; a re-run into the same one reads back as dups
N=100                # distinct host tags → distinct series, no LWW collapse
lp=$(i=0; while [ "$i" -lt "$N" ]; do printf '%s,host=h%s v=%si\n' "$TBL" "$i" "$i"; i=$((i+1)); done)
code=$(printf '%s\n' "$lp" | docker exec -i "$A" curl -s -o /dev/null -w '%{http_code}' \
       -X POST "$DATA/api/v3/write_lp?db=poc&precision=ns" --data-binary @-)
ck "$code" 204 "write to the survivor acks (204) with its peer down"
n=$(docker exec "$A" curl -s -X POST "$DATA/api/sql" -H 'content-type: application/json' \
      -d "{\"db\":\"poc\",\"sql\":\"SELECT COUNT(*) AS n FROM $TBL\"}" | jn)
ck "$n" "$N" "the $N acked rows read back exact — the stale view changed nothing"

echo "== U3 drill: $pass passed, $fail failed =="
test "$fail" -eq 0
