#!/usr/bin/env bash
# Authorized DDL end-to-end drill (#80 / #153). Proves CREATE TABLE is an
# authorized ADMIN operation over the real HTTP surface, and that it does NOT
# hand the data plane a way to run DDL:
#
#   1. Admin CREATEs poc.sensors via POST /admin/tables (201), and it is
#      queryable with ZERO rows immediately — SELECT answers [], not
#      "table not found".
#   2. The write path is HELD to the declaration: a conforming line is 204,
#      an undeclared column or a wrong-typed value is 400, and neither 400
#      lands a row (the count stays put — the reject is before the WAL).
#   3. The data plane still refuses DDL: `CREATE TABLE ...` over /api/sql is
#      refused by the read-only guard, and the table it named never appears.
#   4. Schema-on-write is untouched: an undeclared table still auto-creates
#      on first write, so CREATE is additive, not a new gate on everything.
#
# Self-contained: one `timelake-server` on local disk. Needs the binary
# (BIN=, default target/debug/timelake-server), curl and python3.
#
#   docker run --rm -v "$PWD:/w" -w /w \
#     -v rk-cargo-registry:/usr/local/cargo/registry rust:1-slim bash -c '
#       apt-get update -qq && apt-get install -y -qq python3 curl >/dev/null;
#       cargo build -p timelake-server 2>&1 | tail -1;
#       deploy/compose/ddl_drill.sh'
set -u
export NO_COLOR=1
FAIL=0
chk() { if [ "$1" = "$2" ]; then echo "  [PASS] $3 ($1)"; else echo "  [FAIL] $3 (got $1, want $2)"; FAIL=1; fi; }

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT"
BIN=${BIN:-target/debug/timelake-server}
WORK=$(mktemp -d)
DATA=127.0.0.1:1963
ADMIN=127.0.0.1:1966
JAR="$WORK/cookies"

echo "=== authorized DDL drill ($(date -u +%Y-%m-%dT%H:%M:%SZ)) ==="
echo "workdir=$WORK  bin=$BIN"

TIMELAKE_DATA_DIR="$WORK/data" TIMELAKE_ADDR="$DATA" TIMELAKE_ADMIN_ADDR="$ADMIN" \
  NO_COLOR=1 "$BIN" >"$WORK/server.log" 2>&1 &
SRV=$!
trap 'kill "$SRV" 2>/dev/null; rm -rf "$WORK"' EXIT
for _ in $(seq 1 40); do curl -fs "http://$DATA/health" >/dev/null 2>&1 && break; sleep 0.25; done
curl -fs "http://$DATA/health" >/dev/null 2>&1 || { echo "server FAILED"; cat "$WORK/server.log"; exit 1; }

write() { curl -sS -o /dev/null -w '%{http_code}' -XPOST "http://$DATA/api/v3/write_lp?db=poc&precision=ns" --data-binary "$1"; }
sql() { curl -sS -XPOST "http://$DATA/api/sql" -H 'content-type: application/json' -d "{\"db\":\"poc\",\"sql\":\"$1\"}"; }
sql_code() { curl -sS -o /dev/null -w '%{http_code}' -XPOST "http://$DATA/api/sql" -H 'content-type: application/json' -d "{\"db\":\"poc\",\"sql\":\"$1\"}"; }
n_rows() { sql "$1" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d[0]["n"] if isinstance(d,list) and d else "ERR")'; }

# --- admin session: first login is admin/admin, quarantined until rotated -----
login() { curl -sS -c "$JAR" -b "$JAR" -H 'content-type: application/json' \
  -d "{\"username\":\"admin\",\"password\":\"$1\"}" "http://$ADMIN/admin/session" \
  | sed -n 's/.*"csrf":"\([^"]*\)".*/\1/p'; }
CSRF=$(login admin)
curl -sS -b "$JAR" -H "x-timelake-csrf: $CSRF" -H 'content-type: application/json' \
  -d '{"current_password":"admin","new_password":"ddl drill pw"}' \
  "http://$ADMIN/admin/password" >/dev/null 2>&1 || true
CSRF=$(login "ddl drill pw")

echo
echo "### 1. admin CREATE, then zero-row visibility ###"
CREATE='{"db":"poc","table":"sensors","columns":[{"name":"site","type":"string","tag":true},{"name":"celsius","type":"float"}]}'
CODE=$(curl -sS -o /dev/null -w '%{http_code}' -b "$JAR" -H "x-timelake-csrf: $CSRF" \
  -H 'content-type: application/json' -d "$CREATE" "http://$ADMIN/admin/tables")
chk "$CODE" "201" "POST /admin/tables created poc.sensors"
chk "$(sql_code 'SELECT * FROM sensors')" "200" "SELECT on the empty declared table is 200, not an error"
chk "$(n_rows 'SELECT COUNT(*) AS n FROM sensors')" "0" "declared table answers COUNT(*) = 0 (not \"table not found\")"

echo
echo "### 2. the write path is held to the declaration ###"
chk "$(write $'sensors,site=a celsius=20.5 10')" "204" "conforming write accepted"
chk "$(write $'sensors,site=a celsius=20.5,humidity=0.6 20')" "400" "undeclared field 'humidity' rejected"
chk "$(write $'sensors,site=a celsius=\"hot\" 20')" "400" "wrong type (string for a float) rejected"
chk "$(write $'sensors,site=a,rack=r1 celsius=20.5 20')" "400" "undeclared tag 'rack' rejected"
chk "$(n_rows 'SELECT COUNT(*) AS n FROM sensors')" "1" "only the conforming write landed a row"

echo
echo "### 3. the data plane still refuses DDL ###"
# The read-only guard classifies the built plan; CREATE never executes.
chk "$(sql_code 'CREATE TABLE forbidden (t TIMESTAMP)')" "400" "CREATE TABLE over /api/sql refused (read-only guard)"
chk "$(sql_code 'SELECT COUNT(*) AS n FROM forbidden')" "400" "the table it named was never created (query errors)"

echo
echo "### 4. schema-on-write is untouched ###"
chk "$(write $'weather,city=paris tempc=18.0 10')" "204" "an undeclared table still auto-creates on first write"
chk "$(n_rows 'SELECT COUNT(*) AS n FROM weather')" "1" "and reads back"

echo
if [ "$FAIL" = 0 ]; then echo "=== ALL PASS ==="; else echo "=== FAILURES ==="; fi
exit "$FAIL"
