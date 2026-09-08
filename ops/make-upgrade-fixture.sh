#!/bin/sh
# Cut an upgrade fixture: a data directory this release wrote, kept so a
# LATER release can prove it still reads it (catchment#11).
#
# Why a fixture and not an old container. Standing release N up to write the
# data and N+1 to read it needs every old image kept and pulled, costs a
# container per version per nightly, and grows as N-1 upgrade paths. This
# pays the old-binary cost ONCE, here, at release. Restoring it later is a
# tar extract.
#
# Run it from the release commit, with that release's binary:
#
#   docker run --rm -v "$PWD:/w" -w /w \
#     -v rk-cargo-registry:/usr/local/cargo/registry \
#     -v rk-rustup:/usr/local/rustup rust:1-slim bash -c '
#       apt-get update -qq && apt-get install -y -qq python3 curl >/dev/null;
#       cargo build -p timelake-server 2>&1 | tail -1;
#       ops/make-upgrade-fixture.sh 0.4.0'
#
# Output, into fixtures/upgrade/:
#   <version>.tgz    the data directory, tarred exactly as ops/tldb-backup.sh
#                    writes one (`-C /data .`), so `tldb-backup.sh restore`
#                    consumes it unchanged
#   <version>.json   what a later release must still find in it
#
# NEVER regenerate an old fixture with a newer binary. The whole value of
# the 0.4 fixture is that 0.4 wrote it; rewriting it with 0.5 turns the test
# into 0.5 reading its own output, which is what the suite already does.
set -u
export NO_COLOR=1

VERSION=${1:-}
[ -n "$VERSION" ] || { echo "usage: $0 <version>   e.g. $0 0.4.0" >&2; exit 2; }

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
BIN=${BIN:-target/debug/timelake-server}
OUT="$ROOT/fixtures/upgrade"
WORK=$(mktemp -d)
DATA=127.0.0.1:1963
ADMIN=127.0.0.1:1966
JAR="$WORK/cookies"
DB=poc
ADMIN_PW="fixture rotated pw"

[ -x "$BIN" ] || { echo "no binary at $BIN — cargo build -p timelake-server" >&2; exit 2; }
mkdir -p "$OUT"

say() { echo "  $*"; }
die() { echo "FAILED: $*" >&2; cat "$WORK/server.log" >&2; exit 1; }

echo "=== upgrade fixture for $VERSION ($(date -u +%Y-%m-%dT%H:%M:%SZ)) ==="
say "workdir  $WORK"
say "binary   $BIN"

# flush_rows low so the first half of the corpus SETTLES into Parquet and the
# manifest gains real add_files; flush_age high so the second half stays in
# the WAL and nothing quietly flushes it behind us. Both halves matter: a
# fixture that is all Parquet never exercises WAL replay, and one that is all
# WAL never exercises the manifest.
TIMELAKE_DATA_DIR="$WORK/data" TIMELAKE_ADDR="$DATA" TIMELAKE_ADMIN_ADDR="$ADMIN" \
  TIMELAKE_FLUSH_ROWS=200 TIMELAKE_FLUSH_AGE_SECS=86400 \
  "$BIN" >"$WORK/server.log" 2>&1 &
SRV=$!
trap 'kill -9 "$SRV" 2>/dev/null; rm -rf "$WORK"' EXIT
for _ in $(seq 1 60); do curl -fs "http://$DATA/health" >/dev/null 2>&1 && break; sleep 0.25; done
curl -fs "http://$DATA/health" >/dev/null 2>&1 || die "server never became healthy"

write() { curl -sS -o /dev/null -w '%{http_code}' \
  -XPOST "http://$DATA/api/v3/write_lp?db=$DB&precision=ns" --data-binary "$1"; }
sql() { curl -sS -XPOST "http://$DATA/api/sql" -H 'content-type: application/json' \
  -d "{\"db\":\"$DB\",\"sql\":\"$1\"}"; }
n() { sql "$1" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d[0]["n"] if isinstance(d,list) and d else "ERR")'; }
adm() { # adm METHOD PATH [BODY]
  if [ $# -ge 3 ]; then
    curl -sS -o /dev/null -w '%{http_code}' -X"$1" -b "$JAR" -H "x-timelake-csrf: $CSRF" \
      -H 'content-type: application/json' -d "$3" "http://$ADMIN$2"
  else
    curl -sS -o /dev/null -w '%{http_code}' -X"$1" -b "$JAR" -H "x-timelake-csrf: $CSRF" \
      "http://$ADMIN$2"
  fi
}
login() { curl -sS -c "$JAR" -b "$JAR" -H 'content-type: application/json' \
  -d "{\"username\":\"admin\",\"password\":\"$1\"}" "http://$ADMIN/admin/session" \
  | sed -n 's/.*"csrf":"\([^"]*\)".*/\1/p'; }

# --- principals: rotate out of the first-run quarantine ----------------------
CSRF=$(login admin)
adm POST /admin/password "{\"current_password\":\"admin\",\"new_password\":\"$ADMIN_PW\"}" >/dev/null
CSRF=$(login "$ADMIN_PW")
[ -n "$CSRF" ] || die "could not log in after rotating"
say "principals: admin rotated (quarantine closed)"

# --- a DECLARED table (#80): the schema must survive the upgrade -------------
CREATE='{"db":"poc","table":"sensors","columns":[{"name":"site","type":"string","tag":true},{"name":"celsius","type":"float"}]}'
[ "$(adm POST /admin/tables "$CREATE")" = "201" ] || die "CREATE sensors"
say "ddl: poc.sensors declared"

# --- settled rows, across two hour partitions -------------------------------
# Two partitions so compaction and the per-partition file layout are real.
H1=1757000000000000000   # a fixed instant, so the fixture is reproducible
H2=1757003600000000000   # +1h
i=0; LP=""
while [ $i -lt 150 ]; do
  LP="${LP}sensors,site=north celsius=$((i % 40)).5 $((H1 + i * 1000000))
"
  i=$((i + 1))
done
i=0
while [ $i -lt 150 ]; do
  LP="${LP}sensors,site=south celsius=$((i % 40)).5 $((H2 + i * 1000000))
"
  i=$((i + 1))
done
[ "$(write "$LP")" = "204" ] || die "write sensors"

# --- a table with rows a targeted delete will remove ------------------------
i=0; LP=""
while [ $i -lt 60 ]; do
  LP="${LP}metrics,host=web-1 v=$i $((H1 + i * 1000000))
"
  i=$((i + 1))
done
i=0
while [ $i -lt 90 ]; do
  LP="${LP}metrics,host=web-2 v=$i $((H1 + i * 1000000))
"
  i=$((i + 1))
done
[ "$(write "$LP")" = "204" ] || die "write metrics"

# --- a table created, written to, then DROPPED ------------------------------
[ "$(write "scratch,k=1 v=1 $H1")" = "204" ] || die "write scratch"
[ "$(adm DELETE /admin/tables/$DB/scratch)" = "200" ] || die "DROP scratch"
say "ddl: poc.scratch written then dropped"

# --- a targeted delete that must STILL be applied after the upgrade ---------
DEL="{\"db\":\"$DB\",\"table\":\"metrics\",\"tags\":{\"host\":\"web-1\"}}"
[ "$(adm POST /admin/delete "$DEL")" = "200" ] || die "targeted delete"
say "r-1: metrics host=web-1 deleted (60 rows)"

# --- retention, rollup, tokens: the config documents ------------------------
[ "$(adm PUT /admin/retention "{\"db\":\"$DB\",\"table\":\"metrics\",\"duration\":\"365d\"}")" = "200" ] \
  || die "retention"
ROLL="{\"db\":\"$DB\",\"name\":\"metrics_1h\",\"source\":\"metrics\",\"interval\":\"1h\",\"aggregations\":[{\"function\":\"avg\",\"source_column\":\"v\",\"target_column\":\"v_avg\"}]}"
RC=$(adm PUT /admin/rollups "$ROLL")
[ "$RC" = "200" ] || die "rollup PUT -> $RC"
LC=$(adm PUT /admin/last_cache "{\"db\":\"$DB\",\"table\":\"sensors\"}")
[ "$LC" = "200" ] || say "last_cache: PUT -> $LC (not on this release; skipped)"
adm POST /admin/tokens '{"description":"fixture read token","scope":"read","databases":["poc"]}' >/dev/null
adm POST /admin/tokens '{"description":"fixture write token","scope":"write","databases":["poc"]}' >/dev/null
say "config: retention + rollup + last_cache + 2 tokens"

SETTLED=$(n 'SELECT COUNT(*) AS n FROM sensors')
METRICS=$(n 'SELECT COUNT(*) AS n FROM metrics')

# --- and finally rows that are STILL IN THE WAL -----------------------------
# The point of the whole fixture. A quiesced or gracefully-stopped node has
# an empty WAL, which silently drops WAL_VERSION out of the four formats
# under test — and replay across versions is exactly the thing that breaks.
i=0; LP=""
while [ $i -lt 25 ]; do
  LP="${LP}sensors,site=unflushed celsius=1.0 $((H2 + 900000000 + i * 1000000))
"
  i=$((i + 1))
done
[ "$(write "$LP")" = "204" ] || die "write unflushed"
TOTAL_SENSORS=$(n 'SELECT COUNT(*) AS n FROM sensors')
say "wal: 25 rows written and deliberately left unflushed"

# SIGKILL, not a graceful stop: a clean shutdown flushes, and a flushed WAL
# is the one thing this fixture must not have.
kill -9 "$SRV" 2>/dev/null
wait "$SRV" 2>/dev/null
sleep 1
[ -d "$WORK/data/wal" ] && say "wal dir present: $(ls "$WORK/data/wal" | wc -l) file(s)"

# --- the artifact, in the layout ops/tldb-backup.sh restores -----------------
tar czf "$OUT/$VERSION.tgz" -C "$WORK/data" .
python3 - "$OUT/$VERSION.json" "$VERSION" "$SETTLED" "$METRICS" "$TOTAL_SENSORS" <<'PY'
import json, sys, time
path, version, settled, metrics, total = sys.argv[1:6]
json.dump({
    "version": version,
    "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "db": "poc",
    "note": "Written by the release named above. Never regenerate with a "
            "newer binary — that turns the test into a version reading its "
            "own output.",
    "expect": {
        "sensors_rows": int(total),
        "sensors_settled_rows": int(settled),
        "metrics_rows": int(metrics),
        "declared_table": "sensors",
        "declared_columns": ["site", "celsius"],
        "dropped_table": "scratch",
        "deleted_tag": {"table": "metrics", "host": "web-1"},
        "retention": {"db": "poc", "table": "metrics", "duration": "365d"},
        "admin_password": "fixture rotated pw",
        "tokens": 2,
    },
}, open(path, "w", encoding="utf-8"), indent=2, sort_keys=True)
print("  wrote", path)
PY

echo
say "fixture  $OUT/$VERSION.tgz  ($(wc -c < "$OUT/$VERSION.tgz") bytes)"
say "expects  $OUT/$VERSION.json"
say "sensors=$TOTAL_SENSORS (settled $SETTLED, +25 in the WAL)  metrics=$METRICS"
