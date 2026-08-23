#!/bin/sh
# #46 drill: a token issued or revoked on one node takes effect on every node
# that shares the store — the other ingester and the queriers — within one
# maintenance tick, without a restart.
#
# Rig: compose/timelakedb-cluster-s3.yml brought up with TLDB_DATA_AUTH=required
# (router 5970, ingester-a 5971, ingester-b 5972, querier-a 5973, one bucket
# on LocalStack). The drill issues a token on ingester-a's console and then:
#   - writes DIRECTLY to ingester-b with it  -> 204 on first use (the on-miss
#     re-read; before #46 this was 401 until ingester-b restarted);
#   - writes THROUGH THE ROUTER a body that shards to both ingesters -> 204;
#   - reads on querier-a with it             -> 200 (queriers authenticate reads
#     and run no maintenance; they reload on the tail loop);
#   - revokes on ingester-a, waits one tick  -> ingester-b 401, querier-a 401,
#     and timelake_auth_token_reloads_total moved on both.
# Run from the HOST.
#   TLDB_DATA_AUTH=required docker compose -f compose/timelakedb-cluster-s3.yml up -d
#   sh cluster-drill/token_reload_drill.sh
set -e
R=${R:-http://localhost:5970}
A=${A:-http://localhost:5971}
B=${B:-http://localhost:5972}
Q=${Q:-http://localhost:5973}
TICK=${TICK:-15}            # the maintenance tick is 10 s (the querier reloads every 10th 1 s tail tick); one full tick plus slack
RUN=$(date +%s)
JAR=$(mktemp)
trap 'rm -f "$JAR"' EXIT

pass=0; fail=0
chk() { if [ "$1" = "$2" ]; then echo "  PASS  $3"; pass=$((pass+1));
        else echo "  FAIL  $3 (got '$1' want '$2')"; fail=$((fail+1)); fi; }
code() { curl -s -o /dev/null -w "%{http_code}" "$@"; }
metric() { curl -s "$1/metrics" | grep "^$2 " | awk '{print $2}' | head -1; }
count_on() {  # $1=url $2=table -> COUNT(*) with the token, or the HTTP code on failure
  out=$(curl -s -w '\n%{http_code}' -X POST "$1/api/sql" -H "authorization: Bearer $TOKEN" \
    -H 'content-type: application/json' \
    -d "{\"db\":\"poc\",\"sql\":\"SELECT COUNT(*) AS n FROM $2\"}")
  http=$(printf '%s' "$out" | tail -1); body=$(printf '%s' "$out" | sed '$d')
  if [ "$http" = "200" ]; then printf '%s' "$body" | python -c "import sys,json
d=json.load(sys.stdin); print(d[0]['n'] if isinstance(d,list) and d else 0)"; else echo "$http"; fi
}
lp() { # lp <table> -> 40 lines of line protocol
python - "$1" <<'PY'
import sys, time
t = sys.argv[1]; n = 40
t0 = int(time.time()*1e9) - n*1_000_000
print("\n".join(f"{t},host=h{i%4} v={i}i {t0 + i*1_000_000}" for i in range(n)))
PY
}

echo "== #46: tokens propagate across the cluster through the shared store =="

echo "-- the rig is up, in required mode --"
for i in $(seq 1 60); do
  curl -fs "$A/health" >/dev/null 2>&1 && curl -fs "$B/health" >/dev/null 2>&1 && \
  curl -fs "$Q/health" >/dev/null 2>&1 && curl -fs "$R/health" >/dev/null 2>&1 && break
  sleep 1
done
chk "$(code "$A/health")$(code "$B/health")$(code "$Q/health")$(code "$R/health")" "200200200200" "ingester-a, ingester-b, querier-a, router healthy"
chk "$(metric "$A" timelake_data_auth_mode)$(metric "$B" timelake_data_auth_mode)$(metric "$Q" timelake_data_auth_mode)" "222" "required mode on both ingesters and the querier"

echo "-- issue a token on ingester-a's console (admin/admin, rotated once as the quarantine requires) --"
login() { # login <password> -> csrf (empty on failure)
  rm -f "$JAR"
  curl -sS -c "$JAR" -H 'content-type: application/json' \
    -d "{\"username\":\"admin\",\"password\":\"$1\"}" "$A/admin/session" \
    | sed -n 's/.*"csrf":"\([^"]*\)".*/\1/p'
}
ROTATED="token-reload-drill-2026"
CSRF=$(login "admin")
if [ -n "$CSRF" ]; then
  curl -sS -b "$JAR" -H "x-timelake-csrf: $CSRF" -H 'content-type: application/json' \
    -d "{\"current_password\":\"admin\",\"new_password\":\"$ROTATED\"}" "$A/admin/password" >/dev/null 2>&1 || true
fi
CSRF=$(login "$ROTATED")
chk "$([ -n "$CSRF" ] && echo yes || echo no)" "yes" "admin session on ingester-a"
TOKEN=$(curl -sS -b "$JAR" -H "x-timelake-csrf: $CSRF" -H 'content-type: application/json' \
  -d '{"description":"token-reload drill","scope":"read_write"}' "$A/admin/tokens" \
  | sed -n 's/.*"secret":"\([^"]*\)".*/\1/p')
TOKEN_ID=$(curl -sS -b "$JAR" -H "x-timelake-csrf: $CSRF" "$A/admin/tokens" \
  | python -c "import sys,json; d=json.load(sys.stdin); print([t['id'] for t in d['tokens'] if t['description']=='token-reload drill' and not t['revoked']][-1])")
chk "$(printf '%s' "$TOKEN" | cut -c1-5)" "tldb_" "token issued on ingester-a (id $TOKEN_ID)"

B_RELOADS_0=$(metric "$B" timelake_auth_token_reloads_total)
Q_RELOADS_0=$(metric "$Q" timelake_auth_token_reloads_total)

echo "-- first use on the OTHER ingester, immediately: the on-miss re-read --"
T="tr_${RUN}"
chk "$(code -X POST "$B/api/v3/write_lp?db=poc" -H "authorization: Bearer $TOKEN" --data-binary "$(lp "${T}_b")")" "204" \
    "ingester-b accepts a token it never saw issued, on first presentation (was 401 until restart)"
chk "$([ "$(metric "$B" timelake_auth_token_reloads_total)" -gt "$B_RELOADS_0" ] && echo yes || echo no)" "yes" \
    "ingester-b's timelake_auth_token_reloads_total moved"

echo "-- through the router, a body that shards to both ingesters --"
BODY="$(lp "${T}_r1")
$(lp "${T}_r2")
$(lp "${T}_r3")
$(lp "${T}_r4")"
chk "$(code -X POST "$R/api/v3/write_lp?db=poc" -H "authorization: Bearer $TOKEN" --data-binary "$BODY")" "204" \
    "router write with the token -> 204 on every shard (#37 + #46 together)"

echo "-- reads on a querier with the same token --"
sleep 2
chk "$(count_on "$Q" "${T}_b")" "40" "querier-a answers with the token (it reloads on its tail loop / on miss)"
chk "$([ "$(metric "$Q" timelake_auth_token_reloads_total)" -gt "$Q_RELOADS_0" ] && echo yes || echo no)" "yes" \
    "querier-a's timelake_auth_token_reloads_total moved"

echo "-- revoke on ingester-a; every other node must refuse within one tick --"
chk "$(code -b "$JAR" -H "x-timelake-csrf: $CSRF" -X DELETE "$A/admin/tokens/$TOKEN_ID")" "200" "revoked on ingester-a"
chk "$(code -X POST "$A/api/v3/write_lp?db=poc" -H "authorization: Bearer $TOKEN" --data-binary "$(lp "${T}_x")")" "401" \
    "ingester-a refuses at once (its own store write)"
echo "    waiting ${TICK}s for one maintenance tick on the others..."
sleep "$TICK"
chk "$(code -X POST "$B/api/v3/write_lp?db=poc" -H "authorization: Bearer $TOKEN" --data-binary "$(lp "${T}_y")")" "401" \
    "ingester-b refuses the revoked token after one tick, no restart (the half that makes revocation real)"
chk "$(count_on "$Q" "${T}_b")" "401" "querier-a refuses the revoked token for reads too"
chk "$(code -X POST "$R/api/v3/write_lp?db=poc" -H "authorization: Bearer $TOKEN" --data-binary "$(lp "${T}_z")")" "401" \
    "through the router: refused on whichever shard it lands"

echo "== #46 verdict =="
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ] && echo "TOKEN-RELOAD: PASS" || { echo "TOKEN-RELOAD: FAIL"; exit 1; }
