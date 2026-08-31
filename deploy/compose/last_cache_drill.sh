#!/usr/bin/env bash
# Last-value cache end-to-end drill (#57 / #150). Proves the three things the
# feature promises, over the real HTTP surface, not a unit test:
#
#   1. last_cache('cpu') answers with NO file scan — timelake_scan_files_
#      considered_total does not move across the call, AFTER the data flushed to
#      files (so a plain scan WOULD read them). Not wall-clock: a warm cache
#      can't pass for a working last-value cache.
#   2. Its answer is EXACT — the same latest-per-series a scan computes, and an
#      out-of-order older write never becomes the "current" value.
#   3. The cap holds — writing more series than the cap keeps the count at it,
#      and the function returns only the hot ones.
#
# Self-contained: one `timelake-server` process on local disk. Needs the binary
# (BIN=, default target/debug/timelake-server), curl and python3.
#
#   docker run --rm -v "$PWD:/w" -w /w \
#     -v rk-cargo-registry:/usr/local/cargo/registry rust:1-slim bash -c '
#       apt-get update -qq && apt-get install -y -qq python3 curl >/dev/null;
#       cargo build -p timelake-server 2>&1 | tail -1;
#       deploy/compose/last_cache_drill.sh'
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

echo "=== last-value cache drill ($(date -u +%Y-%m-%dT%H:%M:%SZ)) ==="
echo "workdir=$WORK  bin=$BIN"

# A short flush age so the write reaches files within the drill; production
# leaves this at the 60 s default.
TIMELAKE_DATA_DIR="$WORK/data" TIMELAKE_ADDR="$DATA" TIMELAKE_ADMIN_ADDR="$ADMIN" \
  TIMELAKE_FLUSH_AGE_SECS=2 TIMELAKE_LAST_CACHE_MAX=50 NO_COLOR=1 \
  "$BIN" >"$WORK/server.log" 2>&1 &
SRV=$!
trap 'kill "$SRV" 2>/dev/null; rm -rf "$WORK"' EXIT
for _ in $(seq 1 40); do curl -fs "http://$DATA/health" >/dev/null 2>&1 && break; sleep 0.25; done
curl -fs "http://$DATA/health" >/dev/null 2>&1 || { echo "server FAILED"; cat "$WORK/server.log"; exit 1; }

write() { curl -sS -o /dev/null -w '%{http_code}' -XPOST "http://$DATA/api/v3/write_lp?db=poc&precision=ns" --data-binary "$1"; }
sql() { curl -sS -XPOST "http://$DATA/api/sql" -H 'content-type: application/json' -d "{\"db\":\"poc\",\"sql\":\"$1\"}"; }
metric() { curl -sS "http://$DATA/metrics" | grep "^$1 " | awk '{print $2}' | head -1; }

# --- admin session: first login is admin/admin, quarantined until rotated -----
login() { curl -sS -c "$JAR" -b "$JAR" -H 'content-type: application/json' \
  -d "{\"username\":\"admin\",\"password\":\"$1\"}" "http://$ADMIN/admin/session" \
  | sed -n 's/.*"csrf":"\([^"]*\)".*/\1/p'; }
CSRF=$(login admin)
curl -sS -b "$JAR" -H "x-timelake-csrf: $CSRF" -H 'content-type: application/json' \
  -d '{"current_password":"admin","new_password":"last cache drill pw"}' \
  "http://$ADMIN/admin/password" >/dev/null 2>&1 || true
CSRF=$(login "last cache drill pw")

echo "-- enable last_cache for poc.cpu via /admin/last_cache --"
CODE=$(curl -sS -o /dev/null -w '%{http_code}' -X PUT -b "$JAR" -H "x-timelake-csrf: $CSRF" \
  -H 'content-type: application/json' -d '{"db":"poc","table":"cpu"}' \
  "http://$ADMIN/admin/last_cache")
chk "$CODE" "200" "PUT /admin/last_cache enabled poc.cpu"

echo "-- write series with distinct timestamps, plus an out-of-order older one --"
chk "$(write $'cpu,host=a usage=0.1 10\ncpu,host=b usage=0.2 10')" "204" "write t=10"
chk "$(write $'cpu,host=a usage=0.5 20\ncpu,host=b usage=0.6 20')" "204" "write t=20 (the latest)"
chk "$(write 'cpu,host=a usage=99 15')" "204" "write out-of-order t=15"
echo "-- wait for the flush so a scan reads files (maintenance tick is ~10 s) --"
sleep 13

echo
echo "### 1. no file scan ###"
S_BEFORE=$(metric timelake_scan_files_considered_total)
SCAN=$(sql "SELECT host, usage FROM (SELECT host, usage, row_number() OVER (PARTITION BY host ORDER BY time DESC) rn FROM cpu) WHERE rn = 1 ORDER BY host")
S_MID=$(metric timelake_scan_files_considered_total)
chk "$([ "$S_MID" -gt "$S_BEFORE" ] && echo yes || echo no)" "yes" "a real scan considered files ($S_BEFORE -> $S_MID)"
CACHED=$(sql "SELECT host, usage FROM last_cache('cpu') ORDER BY host")
S_AFTER=$(metric timelake_scan_files_considered_total)
chk "$S_AFTER" "$S_MID" "last_cache('cpu') scanned NO files (stayed $S_MID)"

echo
echo "### 2. exact, and out-of-order-safe ###"
echo "  scan   : $SCAN"
echo "  cached : $CACHED"
chk "$CACHED" "$SCAN" "last_cache equals the scan's latest-per-series"
chk "$(echo "$CACHED" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d[0]["usage"])')" "0.5" "host a is 0.5 (the t=20 value), not 99 (the out-of-order t=15)"

echo
echo "### 3. the cap holds (TIMELAKE_LAST_CACHE_MAX=50) ###"
python3 - <<PY > "$WORK/many.lp"
print("\n".join(f"cpu,host=x{i} usage={i} {i+100}" for i in range(500)))
PY
chk "$(curl -sS -o /dev/null -w '%{http_code}' -XPOST "http://$DATA/api/v3/write_lp?db=poc&precision=ns" --data-binary @"$WORK/many.lp")" "204" "wrote 500 more series"
N=$(sql "SELECT COUNT(*) AS n FROM last_cache('cpu')" | python3 -c 'import sys,json; print(json.load(sys.stdin)[0]["n"])')
chk "$N" "50" "entry count pinned at the cap (50 hot series, not 500+)"
chk "$(metric timelake_last_value_entries)" "50" "timelake_last_value_entries agrees"

echo
if [ "$FAIL" = 0 ]; then echo "=== ALL PASS ==="; else echo "=== FAILURES ==="; fi
exit "$FAIL"
