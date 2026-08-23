#!/bin/sh
# #37 drill: the router carries the client's Authorization header to the
# ingester, so a cluster behind a router can run TIMELAKE_DATA_AUTH=required.
#
# Rig: compose/timelakedb-router-auth.yml — one router (6962) in front of one
# ingester (6963) that REQUIRES data-plane auth. The drill issues a token on
# the ingester's console, then writes THROUGH THE ROUTER three ways and reads
# back on the ingester:
#   - with the token       -> 204, and the rows are there;
#   - with no token        -> 401 (the router invents nothing);
#   - with a wrong token   -> 401 (the router forwards what it was given).
# Then the ingester's own counters say who it saw: authenticated >= 1,
# anonymous == 0, rejected >= 2. Before the fix the first write was a 401
# and `anonymous` was where every router write landed, which is the
# measurement an operator is told to flip to `required` on.
#
# Run from the HOST. R=router, I=ingester.
#   sh router_auth_drill.sh
set -e
R=${R:-http://localhost:6962}
I=${I:-http://localhost:6963}
ADMIN_PASS=${ADMIN_PASS:-router-auth-drill-2026}
RUN=$(date +%s)
JAR=$(mktemp)
trap 'rm -f "$JAR"' EXIT

pass=0; fail=0
chk() { if [ "$1" = "$2" ]; then echo "  PASS  $3"; pass=$((pass+1));
        else echo "  FAIL  $3 (got '$1' want '$2')"; fail=$((fail+1)); fi; }
code() { curl -s -o /dev/null -w "%{http_code}" "$@"; }
metric() { curl -s "$I/metrics" | grep "^$1 " | awk '{print $2}' | head -1; }
rows() {  # $1=table, reads on the INGESTER with the token
  curl -s -X POST "$I/api/sql" -H "authorization: Bearer $TOKEN" \
    -H 'content-type: application/json' \
    -d "{\"db\":\"poc\",\"sql\":\"SELECT COUNT(*) AS n FROM $1\"}" 2>/dev/null \
    | python -c "import sys,json
try:
    d=json.load(sys.stdin); print(d[0]['n'] if isinstance(d,list) and d else 0)
except Exception: print(0)"
}

echo "== #37: the router forwards the client's Authorization on writes =="

echo "-- both nodes up --"
for i in $(seq 1 40); do
  curl -fs "$I/health" >/dev/null 2>&1 && curl -fs "$R/health" >/dev/null 2>&1 && break
  sleep 1
done
chk "$(code "$I/health")" "200" "ingester healthy"
chk "$(code "$R/health")" "200" "router healthy"
chk "$(metric timelake_data_auth_mode)" "2" "ingester is in required mode (timelake_data_auth_mode=2)"

echo "-- issue a token on the ingester's console --"
# TIMELAKE_ADMIN_BOOTSTRAP_PASSWORD provisions the password but the seeded
# principal is still quarantined until it is rotated once (the password came
# from the environment, which is a compose file, which is not a secret). So:
# sign in, rotate, sign in again. On a re-run against the same volume the
# bootstrap password is already gone and the rotated one is what works.
ROTATED="${ADMIN_PASS}-rotated"
login() { # login <password> -> csrf on stdout (empty on failure)
  rm -f "$JAR"
  curl -sS -c "$JAR" -H 'content-type: application/json' \
    -d "{\"username\":\"admin\",\"password\":\"$1\"}" \
    "$I/admin/session" | sed -n 's/.*"csrf":"\([^"]*\)".*/\1/p'
}
CSRF=$(login "$ADMIN_PASS")
if [ -n "$CSRF" ]; then
  curl -sS -b "$JAR" -H "x-timelake-csrf: $CSRF" -H 'content-type: application/json' \
    -d "{\"current_password\":\"$ADMIN_PASS\",\"new_password\":\"$ROTATED\"}" \
    "$I/admin/password" >/dev/null 2>&1 || true
fi
CSRF=$(login "$ROTATED")
chk "$([ -n "$CSRF" ] && echo yes || echo no)" "yes" "admin session (bootstrap password rotated once, as the quarantine requires)"
TOKEN=$(curl -sS -b "$JAR" -H "x-timelake-csrf: $CSRF" -H 'content-type: application/json' \
  -d '{"description":"router-auth drill","scope":"read_write"}' \
  "$I/admin/tokens" | sed -n 's/.*"secret":"\([^"]*\)".*/\1/p')
chk "$(printf '%s' "$TOKEN" | cut -c1-5)" "tldb_" "read_write token issued (prefix tldb_, shown once)"

BEFORE_AUTH=$(metric timelake_data_requests_authenticated_total)
BEFORE_ANON=$(metric timelake_data_requests_anonymous_total)
BEFORE_REJ=$(metric timelake_data_requests_rejected_total)

echo "-- write THROUGH THE ROUTER --"
# Two tables, one per accepted write. The first version of this drill sent
# the same body twice (Bearer, then Token) and expected 50 rows; it got 100,
# which is correct — LWW dedup is a FLUSH-time property, and the live buffer
# shows both copies until then. An exact count needs distinct rows.
T="ra_${RUN}"
N=50
lp() { # lp <table> -> N lines of line protocol
python - "$1" "$N" <<'PY'
import sys, time
t, n = sys.argv[1], int(sys.argv[2])
t0 = int(time.time()*1e9) - n*1_000_000
print("\n".join(f"{t},host=h{i%5} v={i}i {t0 + i*1_000_000}" for i in range(n)))
PY
}
BODY_B=$(lp "${T}_bearer"); BODY_T=$(lp "${T}_token")
chk "$(code -X POST "$R/api/v3/write_lp?db=poc" -H "authorization: Bearer $TOKEN" --data-binary "$BODY_B")" "204" \
    "valid token through the router -> 204 (was 401 before #37: the header was dropped)"
chk "$(code -X POST "$R/api/v3/write_lp?db=poc" --data-binary "$BODY_B")" "401" \
    "no token through the router -> 401 (the router invents no credential)"
chk "$(code -X POST "$R/api/v3/write_lp?db=poc" -H "authorization: Bearer tldb_definitely_wrong" --data-binary "$BODY_B")" "401" \
    "wrong token through the router -> 401 (forwarded as given, refused by the ingester)"
chk "$(code -X POST "$R/api/v3/write_lp?db=poc" -H "authorization: Token $TOKEN" --data-binary "$BODY_T")" "204" \
    "Telegraf's 'Token' spelling survives the hop too"

echo "-- the rows are on the ingester, exactly --"
sleep 1
chk "$(rows "${T}_bearer")" "$N" "COUNT(*) of the Bearer write on the ingester = $N (the two 401s landed nothing)"
chk "$(rows "${T}_token")" "$N" "COUNT(*) of the Token write on the ingester = $N"

echo "-- the ingester's split saw the router's clients, not the router --"
AUTH=$(metric timelake_data_requests_authenticated_total)
ANON=$(metric timelake_data_requests_anonymous_total)
REJ=$(metric timelake_data_requests_rejected_total)
chk "$([ "$((AUTH - BEFORE_AUTH))" -ge 2 ] && echo yes || echo no)" "yes" "authenticated_total rose by >=2 (got +$((AUTH - BEFORE_AUTH)))"
chk "$((ANON - BEFORE_ANON))" "0" "anonymous_total unchanged: in required mode it must stay at its floor"
chk "$([ "$((REJ - BEFORE_REJ))" -ge 2 ] && echo yes || echo no)" "yes" "rejected_total rose by >=2 (got +$((REJ - BEFORE_REJ)))"
FWD=$(curl -s "$R/metrics" | grep "^timelake_router_forwarded_total " | awk '{print $2}')
chk "$([ "${FWD:-0}" -ge 2 ] && echo yes || echo no)" "yes" "router forwarded the accepted writes (forwarded_total=$FWD)"

echo "== #37 verdict =="
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ] && echo "ROUTER-AUTH: PASS" || { echo "ROUTER-AUTH: FAIL"; exit 1; }
