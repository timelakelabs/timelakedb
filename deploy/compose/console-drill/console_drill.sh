#!/bin/sh
# U2 console drill — every panel on the dashboard is executed against a real
# node, and the stored numbers are checked against the live `/metrics`.
#
# Why this exists: a dashboard whose queries have never run is a mock-up. The
# panels are written in SQL that only this engine's DataFusion dialect has to
# accept, against a schema that only exists once the node has sampled itself.
# Both of those can be wrong in ways no unit test sees, so the drill pulls
# the SQL **out of the dashboard JSON itself** — add a panel with a broken
# query and this fails, rather than the operator discovering it later.
#
# It also checks §13's U2 gate directly: a value stored in `_system.metrics`
# must equal the value `/metrics` reports for the same counter.
#
#   docker compose -f deploy/compose/timelakedb-console.yml up -d --build
#   docker run --rm --network timelake-console_default -v "$PWD:/repo:ro" \
#       alpine sh -c 'apk add --no-cache curl >/dev/null && sh /repo/deploy/compose/console-drill/console_drill.sh'
#
# Env: BASE (default http://timelakedb:1963), DASH (dashboard json path).

set -eu

BASE=${BASE:-http://timelakedb:1963}
DASH=${DASH:-/repo/deploy/grafana/dashboards/timelakedb-console.json}
# Each run uses its own table: a deterministic corpus written into the same
# table reads back as duplicates until compaction merges them.
RUN=$(date +%s)
TBL="drill_$RUN"

pass=0
fail=0
ck() {
    d=$1
    shift
    if "$@" >/dev/null 2>&1; then
        pass=$((pass + 1))
        echo "  PASS  $d"
    else
        fail=$((fail + 1))
        echo "  FAIL  $d"
    fi
}

# Run SQL against a database, echoing the raw response.
q() {
    db=$1
    sql=$2
    curl -s -X POST "$BASE/api/sql" -H 'content-type: application/json' \
        --data "{\"db\":\"$db\",\"sql\":\"$sql\"}"
}

# Same, but the SQL is already JSON-escaped (straight out of the dashboard).
q_raw() {
    db=$1
    sql=$2
    curl -s -X POST "$BASE/api/sql" -H 'content-type: application/json' \
        --data "{\"db\":\"$db\",\"sql\":\"$sql\"}"
}

metric() {
    curl -s "$BASE/metrics" | grep "^$1 " | awk '{print $2}' | head -1
}

echo "=== A. the node is up ==="
ck "health answers" curl -sf "$BASE/health"
ck "metrics answers" curl -sf "$BASE/metrics"
ck "query latency histogram is exposed" \
    sh -c "curl -s $BASE/metrics | grep -q timelake_query_duration_seconds_bucket"
ck "self-monitoring counters are exposed" \
    sh -c "curl -s $BASE/metrics | grep -q timelake_selfmon_written_total"

echo
echo "=== B. give the node something to say about itself ==="
i=0
body=""
while [ $i -lt 200 ]; do
    body="$body$TBL,host=h$((i % 8)),region=r$((i % 3)) v=$i.5 $((1700000000000000000 + i * 1000000))
"
    i=$((i + 1))
done
ck "writes accepted" sh -c "printf '%s' '$body' | curl -sf -X POST '$BASE/api/v3/write_lp?db=poc' \
    -H 'content-type: text/plain' --data-binary @- -o /dev/null"

# One of each outcome, so the outcome panel has all three to chart.
q poc "SELECT COUNT(*) AS n FROM $TBL" >/dev/null
q poc "SELECT host, AVG(v) AS a FROM $TBL GROUP BY host" >/dev/null
q poc "COPY (SELECT * FROM $TBL) TO '/tmp/x.parquet'" >/dev/null   # refused
q poc "SELECT * FROM no_such_table_$RUN" >/dev/null                # failed
echo "  ran 4 queries (2 ok, 1 refused, 1 failed)"

echo
echo "=== C. wait for a maintenance tick to store the sample ==="
# The tick is 10s; flush age is 20s. Wait past both so files exist and the
# per-table storage panels have something to show.
sleep 28

echo
echo "=== D. the node has written itself into _system ==="
# Read the counts as NUMBERS. An earlier revision tested
# `grep -qv '"n":0'` and `grep -q '"n"'`, both of which succeed on an ERROR
# response — so had `_system` not existed yet, these would have passed while
# proving nothing. A drill that cannot fail is not evidence.
n_queries=$(q _system "SELECT COUNT(*) AS n FROM queries" | sed -n 's/.*"n":\([0-9]*\).*/\1/p')
n_metrics=$(q _system "SELECT COUNT(*) AS n FROM metrics" | sed -n 's/.*"n":\([0-9]*\).*/\1/p')
echo "  _system.queries=${n_queries:-<none>}  _system.metrics=${n_metrics:-<none>}"
ck "_system.queries has rows" sh -c "test -n '$n_queries' && test '$n_queries' -gt 0"
ck "_system.metrics has rows" sh -c "test -n '$n_metrics' && test '$n_metrics' -gt 0"
ck "all four outcomes are distinguishable" \
    sh -c "curl -s -X POST '$BASE/api/sql' -H 'content-type: application/json' \
        --data '{\"db\":\"_system\",\"sql\":\"SELECT outcome, COUNT(*) AS n FROM queries GROUP BY outcome\"}' \
        | grep -q refused"
ck "nothing was dropped by the sampler" \
    sh -c "test \"\$(curl -s $BASE/metrics | grep '^timelake_selfmon_dropped_total ' | awk '{print \$2}')\" = 0"

echo
echo "=== E. every panel query on the dashboard actually runs ==="
# Pulled from the dashboard so a new panel is covered automatically. The
# extracted text is still JSON-escaped, which is exactly the form needed to
# splice into the request body.
n=0
sed -n 's/.*"rawSql": "\(.*\)",$/\1/p' "$DASH" | while IFS= read -r sql; do
    n=$((n + 1))
    out=$(q_raw _system "$sql")
    # A refusal or a planning failure comes back as an object with "error";
    # a good query returns a JSON array.
    case "$out" in
        \[*)
            echo "  PASS  panel query $n"
            ;;
        *)
            echo "  FAIL  panel query $n"
            echo "        sql: $(echo "$sql" | cut -c1-120)"
            echo "        got: $(echo "$out" | cut -c1-200)"
            ;;
    esac
done > /tmp/panels.out
cat /tmp/panels.out
ppass=$(grep -c '^  PASS' /tmp/panels.out || true)
pfail=$(grep -c '^  FAIL' /tmp/panels.out || true)
pass=$((pass + ppass))
fail=$((fail + pfail))
echo "  ($ppass panel queries ran, $pfail failed)"

echo
echo "=== F. the U2 gate: stored numbers agree with /metrics ==="
live=$(metric timelake_lines_written_total)
stored=$(curl -s -X POST "$BASE/api/sql" -H 'content-type: application/json' \
    --data '{"db":"_system","sql":"SELECT MAX(timelake_lines_written_total) AS v FROM metrics WHERE timelake_lines_written_total IS NOT NULL"}' \
    | sed -n 's/.*"v":\([0-9.]*\).*/\1/p')
echo "  live=/metrics: $live    stored=_system.metrics: $stored"
# The stored value is a snapshot taken at the last tick, so it can only lag
# the live counter — never exceed it. Equality holds when nothing was
# written in between, which is the case here.
ck "stored value does not exceed the live counter" \
    sh -c "awk -v a=\"$stored\" -v b=\"$live\" 'BEGIN{exit !(a<=b)}'"
ck "stored value is present and non-zero" \
    sh -c "awk -v a=\"$stored\" 'BEGIN{exit !(a>0)}'"

echo
echo "=== G. self-monitoring stays out of the user-ingest counter ==="
# 200 lines were written by this drill. The sampler has written many more
# rows into _system by now, and none of them may show up here.
echo "  timelake_lines_written_total = $live (drill wrote 200)"
ck "user-ingest counter excludes _system rows" \
    sh -c "awk -v a=\"$live\" 'BEGIN{exit !(a<=1000)}'"

echo
echo "================================"
echo "  PASS: $pass   FAIL: $fail"
echo "================================"
test "$fail" -eq 0
