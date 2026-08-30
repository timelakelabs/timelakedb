#!/bin/bash
# C3 required intra-cluster mTLS (#72) — the drill.
#
# Runs an ingester PAIR with required mTLS on the internal link and proves the
# three properties C3 has to have:
#
#   Leg 1 (mTLS replication, zero loss): both nodes up behind required mTLS, a
#     write to A replicates to B over the authenticated link; SIGKILL A and
#     recover on B loses nothing — exact count. Replication and /recover go
#     through the require-mode listener, so they are only reachable carded.
#   Leg 2 (hot rotation under load): overwrite BOTH nodes' short-TTL cert/key
#     with a fresh pair from the same cluster CA WHILE writes flow. The
#     established A->B replication connection must ride the swap out — zero write
#     errors, zero acked loss, replication still landing after the rotation.
#   Leg 3 (the wall): a peer with no client cert, and one signed by an outside
#     CA, are refused at the handshake; only a cluster-signed cert gets in.
#
# Self-contained: it runs two `timelake-server` processes directly rather than a
# compose rig, so it needs only the binary + openssl + python3 + curl and no
# image build. It exercises the same code — the require-mode listener
# (crates/server/src/internal_listener.rs), the peer client identity
# (peer_tls.rs), and the rotation watcher. The internal ports are bound on
# loopback and never published, matching the private-network posture (exposure
# 10). Setting the serving cert/key turns on data-plane TLS too (want mode), so
# the writers speak https and trust the cluster CA.
#
#   docker run --rm -v "$PWD:/w" -w /w \
#     -v rk-cargo-registry:/usr/local/cargo/registry -v rk-rustup:/usr/local/rustup \
#     rust:1-slim bash -c 'apt-get update -qq && apt-get install -y -qq openssl python3 curl >/dev/null; \
#       cargo build -p timelake-server --bin timelake-server && \
#       BIN=target/debug/timelake-server deploy/compose/cluster-drill/c3_mtls_rotation_drill.sh'
set -u
export MSYS_NO_PATHCONV=1
BIN=${BIN:-target/debug/timelake-server}
WORK=$(mktemp -d)
CERTS="$WORK/certs"
mkdir -p "$CERTS"
pass=0; fail=0
chk() { if [ "$1" = "$2" ]; then echo "  [PASS] $3 ($1)"; pass=$((pass+1));
        else echo "  [FAIL] $3 (got '$1' want '$2')"; fail=$((fail+1)); fi; }

echo "=== C3 required intra-cluster mTLS drill ($(date -u +%Y-%m-%dT%H:%M:%SZ)) ==="
echo "workdir=$WORK  bin=$BIN"

# ---- certificates -----------------------------------------------------------
# One cluster CA; a node cert per ingester, each usable as BOTH the server
# identity (SAN 127.0.0.1, serverAuth) and the client identity it presents to
# its peer (clientAuth). A renewal pair per node from the same CA is the hot-
# rotation input. An outsider CA + client cert is the negative case.
SAN="subjectAltName=IP:127.0.0.1,DNS:localhost"
NODE_EXT="$CERTS/node.ext"
printf '%s\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth,clientAuth\n' "$SAN" > "$NODE_EXT"

mkca() { # dir-prefix  cn
  openssl ecparam -name prime256v1 -genkey -noout -out "$CERTS/$1-ca.key" 2>/dev/null
  openssl req -new -x509 -key "$CERTS/$1-ca.key" -out "$CERTS/$1-ca.crt" -days 2 \
    -subj "/CN=$2" -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
}
mkleaf() { # ca-prefix  out-prefix  cn
  openssl ecparam -name prime256v1 -genkey -noout -out "$CERTS/$2.key" 2>/dev/null
  openssl req -new -key "$CERTS/$2.key" -out "$CERTS/$2.csr" -subj "/CN=$3" 2>/dev/null
  openssl x509 -req -in "$CERTS/$2.csr" -CA "$CERTS/$1-ca.crt" -CAkey "$CERTS/$1-ca.key" \
    -CAcreateserial -out "$CERTS/$2.crt" -days 1 -extfile "$NODE_EXT" 2>/dev/null
  rm -f "$CERTS/$2.csr"
}
mkca cluster timelake-cluster-ca
mkca outsider  outsider-ca
mkleaf cluster a       ing-a
mkleaf cluster b       ing-b
mkleaf cluster a-renew ing-a       # same CA, fresh key+cert: the rotation input
mkleaf cluster b-renew ing-b
mkleaf cluster drill   drill-admin # the client the drill itself presents to /recover
mkleaf outsider intruder intruder  # a valid cert from the WRONG ca
chmod a+r "$CERTS"/*.crt "$CERTS"/*.key
CA="$CERTS/cluster-ca.crt"

# ---- launch the pair --------------------------------------------------------
DA="$WORK/data-a"; DB="$WORK/data-b"; mkdir -p "$DA" "$DB"
start_node() { # name data-dir data-port flight admin cluster-port peer-id peer-cluster peer-data cert key
  TIMELAKE_ROLE=ingester TIMELAKE_NODE_ID="$1" \
  TIMELAKE_DATA_DIR="$2" \
  TIMELAKE_ADDR="127.0.0.1:$3" TIMELAKE_FLIGHT_ADDR="127.0.0.1:$4" \
  TIMELAKE_ADMIN_ADDR="127.0.0.1:$5" TIMELAKE_CLUSTER_ADDR="127.0.0.1:$6" \
  TIMELAKE_PEERS="$7=ingester@127.0.0.1:$8|127.0.0.1:$9" \
  TIMELAKE_TLS_CERT="${10}" TIMELAKE_TLS_KEY="${11}" TIMELAKE_CLUSTER_CA="$CA" \
  NO_COLOR=1 "$BIN" >"$WORK/$1.log" 2>&1 &
  echo $!
}
APID=$(start_node ing-a "$DA" 19631 19641 19661 19651 ing-b 19652 19632 "$CERTS/a.crt" "$CERTS/a.key")
BPID=$(start_node ing-b "$DB" 19632 19642 19662 19652 ing-a 19651 19631 "$CERTS/b.crt" "$CERTS/b.key")
echo "started ing-a (pid $APID) ing-b (pid $BPID)"

A="https://127.0.0.1:19631"; B="https://127.0.0.1:19632"   # public data ports
# The INTERNAL (mTLS) listeners — /internal/v1/* lives here, never on the data
# port. Reaching them requires a cluster client cert; the data ports do not.
AINT="https://127.0.0.1:19651"; BINT="https://127.0.0.1:19652"
# curl that trusts the cluster CA for the data plane (want mode: no client cert)
dcurl() { curl -s --cacert "$CA" "$@"; }
# curl that ALSO presents a cluster client cert (for the mTLS internal listener)
icurl() { curl -s --cacert "$CA" --cert "$CERTS/drill.crt" --key "$CERTS/drill.key" "$@"; }

wait_up() { for _ in $(seq 1 60); do dcurl -fs "$1/health" >/dev/null 2>&1 && return 0; sleep 0.5; done; return 1; }
wait_up "$A" || { echo "ing-a never came up"; cat "$WORK/ing-a.log"; exit 1; }
wait_up "$B" || { echo "ing-b never came up"; cat "$WORK/ing-b.log"; exit 1; }
echo "both nodes healthy behind TLS"

rows() { dcurl -X POST "$1/api/sql" -H 'content-type: application/json' \
  -d "{\"db\":\"poc\",\"sql\":\"SELECT COUNT(*) AS n FROM $2\"}" 2>/dev/null \
  | python3 -c "import sys,json
try:
 d=json.load(sys.stdin); print(d[0]['n'] if isinstance(d,list) and d else 0)
except Exception: print(0)"; }
metric() { dcurl "$1/metrics" | grep "^$2 " | awk '{print $2}' | head -1; }

# write `count` lines of `table` to `url` over https; returns non-zero on any
# non-204 (so the caller can assert writes never failed during a rotation).
write() { python3 - "$1" "$2" "$3" "$CA" <<'PY'
import sys, ssl, time, urllib.request
url, table, n, ca = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
ctx = ssl.create_default_context(cafile=ca)
t0 = int(time.time()*1e9) - n*1_000_000
B = 2000
for start in range(0, n, B):
    body = "".join(f"{table},host=h{i%50} v={i}i {t0+i*1_000_000}\n"
                   for i in range(start, min(start+B, n)))
    req = urllib.request.Request(f"{url}/api/v3/write_lp?db=poc&precision=ns",
                                 data=body.encode(), method="POST")
    with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
        assert r.status == 204, r.status
PY
}

# =============================================================================
echo
echo "### LEG 1 — mTLS replication, zero acked loss ###"
echo "  internal mTLS active: $(metric "$A" timelake_cluster_mtls_required) / $(metric "$B" timelake_cluster_mtls_required)"
chk "$(metric "$A" timelake_cluster_mtls_required)" "1" "ing-a reports required mTLS on its internal link"
chk "$(metric "$B" timelake_cluster_mtls_required)" "1" "ing-b reports required mTLS on its internal link"
T="repl_$$"
write "$A" "$T" 20000 && echo "  wrote 20000 to A"
chk "$(rows "$A" "$T")" "20000" "A has every acked line"
REPL=$(metric "$A" timelake_cl2_replicated_total)
chk "$([ "${REPL:-0}" -ge 1 ] && echo yes || echo no)" "yes" "A replicated to B over mTLS (replicated_total=$REPL)"
chk "$(rows "$B" "$T")" "0" "B holds the frames dormant (no double-flush)"
echo "  SIGKILL ing-a, recover on ing-b over the mTLS listener"
kill -9 "$APID" 2>/dev/null; wait "$APID" 2>/dev/null
REC=$(icurl -X POST "$BINT/internal/v1/recover" || echo "ERR")
echo "    recover response: ${REC:-<empty>}"
sleep 1
chk "$(rows "$B" "$T")" "20000" "ZERO ACKED LOSS: every line A acked is on B after recovery"

# =============================================================================
echo
echo "### LEG 2 — hot cert rotation under load ###"
# fresh A (the one we killed), fresh table
APID=$(start_node ing-a "$DA" 19631 19641 19661 19651 ing-b 19652 19632 "$CERTS/a.crt" "$CERTS/a.key")
wait_up "$A" || { echo "ing-a restart failed"; cat "$WORK/ing-a.log"; exit 1; }
RT="rot_$$"
BEFORE_FRAMES=$(metric "$B" timelake_cl2_replica_frames_total)
# a background writer that keeps posting to A for the duration
( for k in $(seq 1 12); do write "$A" "$RT" 2500 || echo "WRITE-FAIL-$k" >> "$WORK/rot-writes.err"; done ) &
WPID=$!
sleep 1
echo "  rotating BOTH nodes' serving cert/key to a fresh pair from the same CA, mid-write"
cp "$CERTS/a-renew.crt" "$CERTS/a.crt"; cp "$CERTS/a-renew.key" "$CERTS/a.key"
cp "$CERTS/b-renew.crt" "$CERTS/b.crt"; cp "$CERTS/b-renew.key" "$CERTS/b.key"
chmod a+r "$CERTS"/*.crt "$CERTS"/*.key
# the watcher polls at 2s + 300ms debounce; give it room while writes continue
sleep 4
wait "$WPID" 2>/dev/null
WERR=$( [ -f "$WORK/rot-writes.err" ] && cat "$WORK/rot-writes.err" || echo "" )
chk "${WERR:-none}" "none" "writes never failed across the rotation"
chk "$(rows "$A" "$RT")" "30000" "zero acked loss across the rotation (30000 written, all present)"
AFTER_FRAMES=$(metric "$B" timelake_cl2_replica_frames_total)
chk "$([ "${AFTER_FRAMES:-0}" -gt "${BEFORE_FRAMES:-0}" ] && echo yes || echo no)" "yes" \
  "replication kept landing on B across the swap (frames $BEFORE_FRAMES -> $AFTER_FRAMES)"
RLOK=$(metric "$A" timelake_tls_last_reload_ok)
chk "${RLOK:-0}" "1" "A's cert reload stayed healthy (last_reload_ok=1)"

# =============================================================================
echo
echo "### LEG 3 — the wall: only a cluster-signed peer gets in ###"
H="$BINT/internal/v1/health"
# no client cert at all
code=$(curl -s -o /dev/null -w '%{http_code}' --cacert "$CA" "$H" 2>/dev/null; echo "|$?")
echo "  no-cert:      http='${code%|*}' curl_exit='${code#*|}'"
chk "$([ "${code#*|}" != "0" ] && echo refused || echo allowed)" "refused" "a peer with NO client cert is refused at the handshake"
# a cert from the wrong CA
code=$(curl -s -o /dev/null -w '%{http_code}' --cacert "$CA" --cert "$CERTS/intruder.crt" --key "$CERTS/intruder.key" "$H" 2>/dev/null; echo "|$?")
echo "  wrong-ca:     http='${code%|*}' curl_exit='${code#*|}'"
chk "$([ "${code#*|}" != "0" ] && echo refused || echo allowed)" "refused" "a cert signed by an outside CA is refused"
# the cluster-signed drill cert
body=$(icurl "$H" || echo "ERR")
echo "  cluster-cert: body='$body'"
chk "$body" "ok" "a cluster-signed cert is served"

# ---- teardown ---------------------------------------------------------------
kill -9 "$APID" "$BPID" 2>/dev/null
echo
echo "=== verdict: $pass passed, $fail failed ==="
if [ "$fail" -eq 0 ]; then
  echo "=== PASS: required intra-cluster mTLS — replication zero-loss, hot rotation"
  echo "          under load, and a certless/wrong-CA peer refused. ==="
fi
rm -rf "$WORK"
[ "$fail" -eq 0 ]
