#!/bin/bash
# C3 Consul discovery (#71) — the drill.
#
# Proves the four properties #71 has to have, against a REAL Consul agent (not a
# fake), with nodes that register themselves and discover their peers live:
#
#   Leg 1 (discover at boot): a router and an ingester pair, all with
#     TIMELAKE_DISCOVERY=consul and NO TIMELAKE_PEERS, come up and the router
#     shards writes across the two ingesters it found in Consul — no
#     hand-maintained peer list.
#   Leg 2 (live join): a third ingester registers; within the refresh window the
#     router's ingester count rises to three, with no restart.
#   Leg 3 (live leave): an ingester is killed; its Consul TTL check stops
#     passing, Consul drops it from the healthy set, and the router's count falls
#     — again with no restart.
#   Leg 4 (CL-5 under a Consul flap): with writes flowing, Consul is stopped and
#     restarted. Writes never fail — the router serves on its last-known-good
#     membership and raises CONSUL_DISCOVERY_DEGRADED — and no acked row is lost.
#
# Self-contained: a `consul agent -dev` plus `timelake-server` processes in one
# container, so it needs the binary + consul + python3 + curl and no image
# build. The router's `timelake_router_ingesters` gauge (now a live read of the
# discovered set) is the observable. Everything is plaintext (no TLS configured).
# The querier and compactor use the SAME spawn_refresh mechanism this drills on
# the router, covered by the #139 unit tests.
#
#   docker run --rm --dns 8.8.8.8 --dns 1.1.1.1 -v "$PWD:/w" -w /w \
#     -v rk-cargo-registry:/usr/local/cargo/registry -v rk-rustup:/usr/local/rustup \
#     rust:1-slim bash -c 'apt-get update -qq && apt-get install -y -qq python3 curl unzip >/dev/null; \
#       cargo build -p timelake-server --bin timelake-server && \
#       BIN=target/debug/timelake-server deploy/compose/cluster-drill/c3_consul_discovery_drill.sh'
set -u
export NO_COLOR=1
BIN=${BIN:-target/debug/timelake-server}
CONSUL_VERSION=${CONSUL_VERSION:-1.20.1}
WORK=$(mktemp -d)
pass=0; fail=0
chk() { if [ "$1" = "$2" ]; then echo "  [PASS] $3 ($1)"; pass=$((pass+1));
        else echo "  [FAIL] $3 (got '$1' want '$2')"; fail=$((fail+1)); fi; }

echo "=== C3 Consul discovery drill ($(date -u +%Y-%m-%dT%H:%M:%SZ)) ==="
echo "workdir=$WORK  bin=$BIN  consul=$CONSUL_VERSION"

# ---- consul --------------------------------------------------------------
if ! command -v consul >/dev/null 2>&1; then
  echo "-- fetching consul $CONSUL_VERSION --"
  curl -fsSL "https://releases.hashicorp.com/consul/${CONSUL_VERSION}/consul_${CONSUL_VERSION}_linux_amd64.zip" -o "$WORK/consul.zip"
  unzip -q -o "$WORK/consul.zip" -d "$WORK"
  CONSUL="$WORK/consul"
else
  CONSUL=consul
fi
CONSUL_HTTP=127.0.0.1:8500
start_consul() {
  "$CONSUL" agent -dev -client 127.0.0.1 -bind 127.0.0.1 \
    -data-dir "$WORK/consul-data" >"$WORK/consul.log" 2>&1 &
  CONSUL_PID=$!
}
start_consul
echo "-- waiting for consul --"
for _ in $(seq 1 40); do curl -fs "http://$CONSUL_HTTP/v1/status/leader" 2>/dev/null | grep -q '.' && break; sleep 0.5; done
curl -fs "http://$CONSUL_HTTP/v1/status/leader" >/dev/null 2>&1 && echo "consul up" || { echo "consul FAILED"; cat "$WORK/consul.log"; exit 1; }

DISCOVERY="consul://$CONSUL_HTTP"
declare -A PIDS

# name data flight admin cluster
start_ingester() {
  local id=$1 data=$2 flight=$3 admin=$4 cluster=$5
  mkdir -p "$WORK/$id"
  TIMELAKE_ROLE=ingester TIMELAKE_NODE_ID="$id" TIMELAKE_DISCOVERY="$DISCOVERY" \
  TIMELAKE_ADDR="127.0.0.1:$data" TIMELAKE_DATA_ADDR="127.0.0.1:$data" \
  TIMELAKE_FLIGHT_ADDR="127.0.0.1:$flight" TIMELAKE_ADMIN_ADDR="127.0.0.1:$admin" \
  TIMELAKE_CLUSTER_ADDR="127.0.0.1:$cluster" TIMELAKE_DATA_DIR="$WORK/$id" \
  NO_COLOR=1 "$BIN" >"$WORK/$id.log" 2>&1 &
  PIDS[$id]=$!
}

# healthy Consul-registered ingesters (role=ingester, passing)
consul_ingesters() {
  curl -fs "http://$CONSUL_HTTP/v1/health/service/timelakedb?passing=true" 2>/dev/null \
    | python3 -c "import sys,json
try:
 d=json.load(sys.stdin); print(sum(1 for e in d if e.get('Service',{}).get('Meta',{}).get('role')=='ingester'))
except Exception: print(0)"
}
router_ingesters() { curl -fs "http://127.0.0.1:5962/metrics" 2>/dev/null | grep '^timelake_router_ingesters ' | awk '{print $2}' | head -1; }
rows() { curl -fs -X POST "http://127.0.0.1:$1/api/sql" -H 'content-type: application/json' \
  -d "{\"db\":\"poc\",\"sql\":\"SELECT COUNT(*) AS n FROM $2\"}" 2>/dev/null \
  | python3 -c "import sys,json
try:
 d=json.load(sys.stdin); print(d[0]['n'] if isinstance(d,list) and d else 0)
except Exception: print(0)"; }
wait_health() { for _ in $(seq 1 60); do curl -fs "http://127.0.0.1:$1/health" >/dev/null 2>&1 && return 0; sleep 0.5; done; return 1; }

# write n lines of `table` to the router; every 204 = accepted
write() { python3 - "$1" "$2" "$3" <<'PY'
import sys, time, urllib.request
url, table, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
t0 = int(time.time()*1e9) - n*1_000_000
B = 2000
for start in range(0, n, B):
    body = "".join(f"{table},host=h{i%20} v={i}i {t0+i*1_000_000}\n"
                   for i in range(start, min(start+B, n)))
    req = urllib.request.Request(f"{url}/api/v3/write_lp?db=poc&precision=ns",
                                 data=body.encode(), method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        assert r.status == 204, r.status
PY
}

# ---- bring up ing-a, ing-b, then the router -----------------------------
echo
echo "### LEG 1 — discover the ingester pair from Consul (no TIMELAKE_PEERS) ###"
start_ingester ing-a 15963 15964 15965 16963
start_ingester ing-b 25963 25964 25965 26963
wait_health 15963 || { echo "ing-a down"; cat "$WORK/ing-a.log"; exit 1; }
wait_health 25963 || { echo "ing-b down"; cat "$WORK/ing-b.log"; exit 1; }
for _ in $(seq 1 30); do [ "$(consul_ingesters)" -ge 2 ] && break; sleep 1; done
chk "$(consul_ingesters)" "2" "both ingesters registered themselves in Consul"

# router starts AFTER the ingesters are in Consul, so its first discovery finds them
TIMELAKE_ROLE=router TIMELAKE_NODE_ID=router TIMELAKE_DISCOVERY="$DISCOVERY" \
  TIMELAKE_ADDR=127.0.0.1:5962 NO_COLOR=1 "$BIN" >"$WORK/router.log" 2>&1 &
PIDS[router]=$!
wait_health 5962 || { echo "router down"; cat "$WORK/router.log"; exit 1; }
chk "$(router_ingesters)" "2" "the router discovered both ingesters from Consul"

T="t_$$"
write "http://127.0.0.1:5962" "$T" 6000 && echo "  wrote 6000 via the router"
sleep 1
total=$(( $(rows 15963 "$T") + $(rows 25963 "$T") ))
chk "$total" "6000" "every routed line landed on a discovered ingester (sum across the pair)"

# ---- live join ----------------------------------------------------------
echo
echo "### LEG 2 — a third ingester joins, seen live ###"
start_ingester ing-c 35963 35964 35965 36963
wait_health 35963 || { echo "ing-c down"; cat "$WORK/ing-c.log"; exit 1; }
echo "  ing-c up; waiting for the router to pick it up (refresh is 5s)…"
for _ in $(seq 1 20); do [ "$(router_ingesters)" = "3" ] && break; sleep 1; done
chk "$(router_ingesters)" "3" "the router routes to the joined ingester with no restart"

# ---- live leave ---------------------------------------------------------
echo
echo "### LEG 3 — an ingester leaves, dropped live ###"
echo "  killing ing-b; Consul drops it after its TTL check lapses…"
kill -9 "${PIDS[ing-b]}" 2>/dev/null; wait "${PIDS[ing-b]}" 2>/dev/null
for _ in $(seq 1 60); do [ "$(router_ingesters)" = "2" ] && break; sleep 1; done
chk "$(router_ingesters)" "2" "the router stops routing to the departed ingester with no restart"

# ---- CL-5 under a Consul flap -------------------------------------------
echo
echo "### LEG 4 — CL-5: a Consul flap degrades, never fails ###"
FT="flap_$$"
# ~12s of writes so they span the whole outage; 20k total (still one shard).
( for k in $(seq 1 20); do write "http://127.0.0.1:5962" "$FT" 1000 || echo "WRITE-FAIL-$k" >> "$WORK/flap.err"; sleep 0.5; done ) &
WPID=$!
sleep 1
echo "  stopping Consul under load…"
kill -9 "$CONSUL_PID" 2>/dev/null; wait "$CONSUL_PID" 2>/dev/null
# Down longer than the 5s discovery refresh, so a tick is guaranteed to fail
# and the degraded alarm fires.
sleep 7
echo "  restarting Consul…"
start_consul
for _ in $(seq 1 40); do curl -fs "http://$CONSUL_HTTP/v1/status/leader" 2>/dev/null | grep -q '.' && break; sleep 0.5; done
wait "$WPID" 2>/dev/null
WERR=$( [ -f "$WORK/flap.err" ] && cat "$WORK/flap.err" || echo "" )
chk "${WERR:-none}" "none" "writes never failed while Consul was down (served on last-known-good)"
chk "$(grep -c 'CONSUL_DISCOVERY_DEGRADED' "$WORK/router.log")" "1" "the router raised the degraded alarm once"
sleep 1
flap_total=$(( $(rows 15963 "$FT") + $(rows 35963 "$FT") ))
chk "$flap_total" "20000" "no acked row lost across the flap (20000 written, all present)"

# ---- teardown -----------------------------------------------------------
for p in "${PIDS[@]}"; do kill -9 "$p" 2>/dev/null; done
kill -9 "$CONSUL_PID" 2>/dev/null
echo
echo "=== verdict: $pass passed, $fail failed ==="
if [ "$fail" -eq 0 ]; then
  echo "=== PASS: nodes register with Consul and discover peers live — boot"
  echo "          discovery, a live join and leave with no restart, and a"
  echo "          Consul flap that degrades rather than failing writes. ==="
fi
rm -rf "$WORK"
[ "$fail" -eq 0 ]
