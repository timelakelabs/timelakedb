#!/usr/bin/env bash
# Authorized DROP end-to-end drill (#80 / #154). Proves DROP TABLE is an
# authorized ADMIN operation over the real HTTP surface, that it actually
# removes the table, and that it does NOT hand the data plane a way to run DDL:
#
#   1. Admin DROPs poc.sensors via DELETE /admin/tables/{db}/{table} (200),
#      and the table is then GONE — a SELECT errors (table not found), it does
#      not answer with zero rows (which would mean "still here, empty").
#   2. The data plane still refuses DDL: `DROP TABLE ...` over /api/sql is
#      refused by the read-only guard.
#   3. DROP is not permanent: a write to the dropped name re-creates the table
#      by schema-on-write, now with no declaration.
#   4. Dropping a table that does not exist is a 400, not a silent success.
#
# Self-contained: one `timelake-server` on local disk. Needs the binary
# (BIN=, default target/debug/timelake-server), curl and python3.
#
#   docker run --rm -v "$PWD:/w" -w /w \
#     -v rk-cargo-registry:/usr/local/cargo/registry rust:1-slim bash -c '
#       apt-get update -qq && apt-get install -y -qq python3 curl >/dev/null;
#       cargo build -p timelake-server 2>&1 | tail -1;
#       deploy/compose/drop_drill.sh'
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

echo "=== authorized DROP drill ($(date -u +%Y-%m-%dT%H:%M:%SZ)) ==="
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
  -d '{"current_password":"admin","new_password":"drop drill pw"}' \
  "http://$ADMIN/admin/password" >/dev/null 2>&1 || true
CSRF=$(login "drop drill pw")
create() { curl -sS -o /dev/null -w '%{http_code}' -b "$JAR" -H "x-timelake-csrf: $CSRF" \
  -H 'content-type: application/json' -d "$1" "http://$ADMIN/admin/tables"; }
drop() { curl -sS -o /dev/null -w '%{http_code}' -X DELETE -b "$JAR" -H "x-timelake-csrf: $CSRF" \
  "http://$ADMIN/admin/tables/$1/$2"; }

echo
echo "### setup: a declared table with data, plus a second table so the db persists ###"
chk "$(create '{"db":"poc","table":"sensors","columns":[{"name":"site","type":"string","tag":true},{"name":"celsius","type":"float"}]}')" "201" "CREATE poc.sensors"
chk "$(write $'sensors,site=a celsius=20.5 10\nsensors,site=b celsius=21.0 10')" "204" "wrote 2 rows"
chk "$(write $'keep,city=x tempc=1.0 10')" "204" "wrote poc.keep (keeps the db alive)"
chk "$(n_rows 'SELECT COUNT(*) AS n FROM sensors')" "2" "sensors has 2 rows before the drop"

echo
echo "### 1. admin DROP removes the table ###"
chk "$(drop poc sensors)" "200" "DELETE /admin/tables/poc/sensors"
chk "$(sql_code 'SELECT COUNT(*) FROM sensors')" "400" "SELECT on the dropped table errors (gone, not empty)"
chk "$(n_rows 'SELECT COUNT(*) AS n FROM keep')" "1" "the other table is untouched"

echo
echo "### 2. the data plane still refuses DDL ###"
chk "$(sql_code 'DROP TABLE keep')" "400" "DROP TABLE over /api/sql refused (read-only guard)"
chk "$(n_rows 'SELECT COUNT(*) AS n FROM keep')" "1" "so keep is still there"

echo
echo "### 3. DROP is not permanent (schema-on-write recreates) ###"
chk "$(write $'sensors,site=c celsius=19.0 20')" "204" "a write to the dropped name is accepted"
chk "$(n_rows 'SELECT COUNT(*) AS n FROM sensors')" "1" "the table is back, with just the new row"

echo
echo "### 4. dropping a table that does not exist ###"
chk "$(drop poc ghost)" "400" "DELETE of a nonexistent table is a 400, not a silent success"

echo
if [ "$FAIL" = 0 ]; then echo "=== ALL PASS ==="; else echo "=== FAILURES ==="; fi
exit "$FAIL"
