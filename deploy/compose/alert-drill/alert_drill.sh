#!/bin/sh
# Grafana alerting drill — proves TimeLakeDB can drive an alert from data to
# a delivered notification, and proves the drill itself can tell the
# difference between a rule that fired and one that did not.
#
# Why this exists: "Grafana supports TimeLakeDB" is usually demonstrated by
# a dashboard rendering, and a dashboard renders whether or not alerting
# works. Alerting adds a stage a dashboard never exercises — the frame from
# the datasource has to survive a `reduce`, and reduce is where the
# TimeLakeDB/Grafana combination has a silent failure mode (see rules.yml,
# and docs/ALERTING.md for the full account). A mis-ordered rule evaluates
# clean, reports `health: ok` indefinitely, and never fires. Nothing in
# Grafana flags it. Only data that *should* trip a threshold, and does not,
# reveals it.
#
# So the drill drives real state transitions rather than inspecting config:
#   Normal -> Alerting -> notification delivered -> Normal
# and carries a rule deliberately written the wrong way that must stay
# Normal throughout. If that guard ever fires, this file's premise is stale
# and docs/ALERTING.md is wrong. If nothing fires, the drill is broken —
# a check that cannot fail is not a check.
#
#   docker compose -f deploy/compose/timelakedb-alerting.yml up -d --build
#   docker run --rm --network timelake-alerting_default -v "$PWD:/repo:ro" \
#       alpine sh -c 'apk add --no-cache curl python3 >/dev/null &&
#                     sh /repo/deploy/compose/alert-drill/alert_drill.sh'
#
# Env: DB (default http://timelakedb:1963), GRAF (http://grafana:3000),
#      SINK (http://alert-sink:9099), GUSER/GPASS (admin/admin).

set -eu

DB=${DB:-http://timelakedb:1963}
GRAF=${GRAF:-http://grafana:3000}
SINK=${SINK:-http://alert-sink:9099}
GUSER=${GUSER:-admin}
GPASS=${GPASS:-admin}

RULE_FIRES="alertprobe value high"
RULE_GUARD="ordering guard (must not fire)"

pass=0
fail=0
# A check that cannot mean anything on this run is reported skipped, not
# passed. Phases F and G both presuppose the alert fired; scoring them green
# when it did not would turn one real failure into a mostly-green run and
# attach a confident "the alert cleared back to Normal" to a rule that never
# left Normal.
skipped=0

# Set once phase E sees the rule fire. Nothing downstream is meaningful
# without it.
fired=0

ok() {
    pass=$((pass + 1))
    echo "  PASS  $1"
}

no() {
    fail=$((fail + 1))
    echo "  FAIL  $1"
    [ -n "${2:-}" ] && echo "        $2"
    return 0
}

skip() {
    skipped=$((skipped + 1))
    echo "  SKIP  $1"
    [ -n "${2:-}" ] && echo "        $2"
    return 0
}

ck() {
    d=$1
    shift
    if "$@" >/dev/null 2>&1; then ok "$d"; else no "$d"; fi
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Grafana's Prometheus-compatible rule API, reduced to "<title>\t<state>\t<health>".
# `state` is inactive | pending | firing; `health` is ok | error | nodata.
rule_table() {
    curl -s -u "$GUSER:$GPASS" \
        "$GRAF/api/prometheus/grafana/api/v1/rules" 2>/dev/null |
        python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for g in (d.get("data") or {}).get("groups") or []:
    for r in g.get("rules") or []:
        print("\t".join([
            r.get("name") or "?",
            r.get("state") or "?",
            r.get("health") or "?",
            (r.get("lastError") or "").replace("\t", " ")[:160],
        ]))
'
}

state_of() {
    rule_table | awk -F'\t' -v n="$1" '$1 == n {print $2; exit}'
}

health_of() {
    rule_table | awk -F'\t' -v n="$1" '$1 == n {print $3; exit}'
}

error_of() {
    rule_table | awk -F'\t' -v n="$1" '$1 == n {print $4; exit}'
}

# Poll until a rule reaches a state, or give up. Polling rather than
# sleeping a fixed span because the total latency here is a sum of three
# independent clocks — the node's flush age, Grafana's 10s group interval,
# and the query's own time — and a fixed sleep either wastes the drill's
# runtime or fails on a slow host for no real reason.
wait_state() {
    want_name=$1
    want=$2
    deadline=${3:-90}
    waited=0
    while [ "$waited" -lt "$deadline" ]; do
        got=$(state_of "$want_name" || true)
        [ "$got" = "$want" ] && return 0
        sleep 3
        waited=$((waited + 3))
    done
    return 1
}

# Seed `alertprobe` with `value`, on rows stamped to land inside the rules'
# 15-minute window. `age_from`/`age_to` are seconds before now.
#
# The relative ages matter as much as the values. The whole discriminator
# rests on the window holding LOW rows that are older and HIGH rows that
# are newer: the correct rule reduces to the newest (high, fires) and the
# mis-ordered one to the oldest (low, does not). Seed only high values and
# both rules fire, the guard proves nothing, and the drill passes while
# testing nothing.
seed() {
    value=$1
    age_from=$2
    age_to=$3
    SEED_VALUE=$value SEED_FROM=$age_from SEED_TO=$age_to python3 -c '
import os, time
v = os.environ["SEED_VALUE"]
a, b = int(os.environ["SEED_FROM"]), int(os.environ["SEED_TO"])
now = time.time()
step = max(1, (a - b) // 8)
lines = [
    "alertprobe,host=a value=%s %d" % (v, int((now - s) * 1e9))
    for s in range(a, b - 1, -step)
]
print("\n".join(lines))
' >/tmp/seed.lp
    code=$(curl -s -o /tmp/seed.out -w "%{http_code}" \
        -XPOST "$DB/api/v3/write_lp?db=alerts" --data-binary @/tmp/seed.lp)
    # Status code, not `curl -sf` — the -f form has bitten this repo before
    # by returning nonzero on a request that plainly succeeded.
    case "$code" in
        2*) return 0 ;;
        *) echo "        write returned $code: $(cat /tmp/seed.out)" ; return 1 ;;
    esac
}

sink_count() {
    curl -s "$SINK/alerts" 2>/dev/null |
        python3 -c 'import json,sys; print(json.load(sys.stdin).get("count", 0))' 2>/dev/null ||
        echo 0
}

echo "=============================================================================="
echo "  Grafana alerting drill against TimeLakeDB"
echo "=============================================================================="
echo
echo "=== A. the stack is up ==="

ready=0
i=0
while [ "$i" -lt 60 ]; do
    c=$(curl -s -o /dev/null -w "%{http_code}" "$DB/health" 2>/dev/null || echo 000)
    case "$c" in 2*) ready=1; break ;; esac
    sleep 2
    i=$((i + 1))
done
[ "$ready" = 1 ] && ok "TimeLakeDB answers /health" || no "TimeLakeDB never became ready"

ready=0
i=0
while [ "$i" -lt 60 ]; do
    c=$(curl -s -o /dev/null -w "%{http_code}" "$GRAF/api/health" 2>/dev/null || echo 000)
    case "$c" in 2*) ready=1; break ;; esac
    sleep 2
    i=$((i + 1))
done
[ "$ready" = 1 ] && ok "Grafana answers /api/health" || no "Grafana never became ready"

ck "the notification sink is listening" curl -s "$SINK/health"

# The drill needs an empty table and cannot produce one. TimeLakeDB's
# /api/sql is read-only — no DROP TABLE — and there is no delete-database
# route, so rows from an earlier run stay inside the 15-minute window the
# rules read.
#
# That is not cosmetic. Phase E's discriminator rests on the window holding
# older LOW rows and newer HIGH rows: the correct rule reduces to the newest
# and fires, the mis-ordered guard reduces to the oldest and does not. Let a
# previous run's HIGH rows age past this run's low seed and they become the
# oldest row in the window — so the guard fires, and the drill announces
# that the ordering trap no longer exists. That is both wrong and the most
# misleading sentence this script is capable of printing.
#
# So the precondition is asserted, not assumed, and dirty state stops the
# run with the command that fixes it.
rows=$(curl -s -XPOST "$DB/api/sql" -H 'content-type: application/json' \
    --data '{"db":"alerts","sql":"SELECT count(*) AS n FROM alertprobe"}' 2>/dev/null |
    python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    print("unreadable"); raise SystemExit
if isinstance(d, dict):
    print("absent")      # {"error": ...} — the table has not been created yet
elif d:
    print(d[0].get("n", "unreadable"))
else:
    print(0)
' 2>/dev/null || echo unreadable)

case "$rows" in
    absent | 0)
        ok "starting from an empty alertprobe table"
        ;;
    *)
        no "alertprobe already holds $rows rows from an earlier run"
        cat <<'DIRTY'
        This drill cannot clear it: /api/sql is read-only and the node
        exposes no drop route. Re-create the stack and run again:

          docker compose -f deploy/compose/timelakedb-alerting.yml down -v
          docker compose -f deploy/compose/timelakedb-alerting.yml up -d

  refusing to continue: phase E's discriminator is not valid on dirty data
  DRILL FAILED
DIRTY
        exit 1
        ;;
esac

echo
echo "=== B. Grafana can read TimeLakeDB over Flight SQL ==="

# Seed first: a datasource check against an empty database cannot tell a
# broken connection from a table that does not exist yet.
if seed 10 600 300; then ok "seeded low values (10) at t-600s..t-300s"; else no "seeding low values failed"; fi

# Give the node time to make them queryable before asking Grafana for them.
sleep 8

DS_OUT=/tmp/ds.json
FROM=$(python3 -c 'import time; print(int((time.time()-900)*1000))')
TO=$(python3 -c 'import time; print(int(time.time()*1000))')
cat >/tmp/ds_query.json <<JSON
{"queries":[{"refId":"A","datasource":{"type":"influxdb","uid":"tldb-alerts"},
"rawSql":"SELECT time, value FROM alertprobe ORDER BY time ASC",
"intervalMs":1000,"maxDataPoints":100}],"from":"$FROM","to":"$TO"}
JSON
curl -s -u "$GUSER:$GPASS" -XPOST "$GRAF/api/ds/query" \
    -H "content-type: application/json" -d @/tmp/ds_query.json >"$DS_OUT" 2>/dev/null || true

# These report their own diagnosis and exit nonzero rather than raising:
# a Python traceback in drill output buries the one line that says what
# went wrong under five that say where.
if python3 -c '
import json, sys
d = json.load(open("'"$DS_OUT"'"))
r = (d.get("results") or {}).get("A") or {}
frames = r.get("frames") or []
if not frames:
    print("no frames returned: %s" % str(r)[:200]); sys.exit(1)
vals = frames[0]["data"]["values"]
if len(vals) < 2 or not vals[1]:
    print("the frame carries no value column"); sys.exit(1)
' >/tmp/ds_err 2>&1; then
    ok "the provisioned datasource returns rows"
else
    no "the datasource returned no usable frame" "$(cat /tmp/ds_err)"
fi

# Alerting needs a frame it can reduce: a time column and a numeric column.
# A frame that renders on a graph panel can still be unreducible, so this is
# checked as its own claim rather than inferred from the query succeeding.
if python3 -c '
import json, sys
d = json.load(open("'"$DS_OUT"'"))
fields = d["results"]["A"]["frames"][0]["schema"]["fields"]
kinds = [f["type"] for f in fields]
if "time" not in kinds:
    print("no time field; got %s" % kinds); sys.exit(1)
if "number" not in kinds:
    print("no numeric field; got %s" % kinds); sys.exit(1)
' >/tmp/sh_err 2>&1; then
    ok "the frame has a time field and a numeric field (reducible)"
else
    no "the frame is not shaped for a reduce stage" "$(cat /tmp/sh_err)"
fi

echo
echo "=== C. both rules are provisioned and evaluating ==="

# Provisioning is read at startup; on a cold stack the rules may not be
# registered for a few seconds after the API answers healthy.
i=0
while [ "$i" -lt 20 ]; do
    [ -n "$(state_of "$RULE_FIRES" || true)" ] && break
    sleep 3
    i=$((i + 1))
done

if [ -n "$(state_of "$RULE_FIRES" || true)" ]; then
    ok "rule '$RULE_FIRES' is loaded"
else
    no "rule '$RULE_FIRES' never appeared" "provisioning did not load; check grafana logs"
fi

if [ -n "$(state_of "$RULE_GUARD" || true)" ]; then
    ok "negative control '$RULE_GUARD' is loaded"
else
    no "negative control '$RULE_GUARD' never appeared"
fi

# `health: ok` means the query and expressions ran. It says nothing about
# whether the rule can fire — which is exactly the trap this drill exists
# for — so it is checked, and then not trusted.
h=$(health_of "$RULE_FIRES" || true)
if [ "$h" = "ok" ]; then
    ok "rule evaluates without error (health=ok)"
else
    no "rule health is '$h', expected ok" "$(error_of "$RULE_FIRES" || true)"
fi

echo
echo "=== D. below the threshold, nothing fires ==="

if wait_state "$RULE_FIRES" inactive 90; then
    ok "value 10 vs threshold 50 -> Normal"
else
    no "rule did not settle to Normal on low data" "state=$(state_of "$RULE_FIRES" || true) health=$(health_of "$RULE_FIRES" || true)"
fi

echo
echo "=== E. above the threshold, the correct rule fires ==="

curl -s -XPOST "$SINK/reset" >/dev/null 2>&1 || true

# Newer than the low rows seeded in B, and inside the same 15-minute window.
if seed 100 120 0; then ok "seeded high values (100) at t-120s..now"; else no "seeding high values failed"; fi

if wait_state "$RULE_FIRES" firing 120; then
    fired=1
    ok "value 100 vs threshold 50 -> Alerting"
else
    no "the rule did NOT fire on data above its threshold" \
       "state=$(state_of "$RULE_FIRES" || true) health=$(health_of "$RULE_FIRES" || true) -- note health is 'ok': that is the ORDER BY trap, see docs/ALERTING.md"
fi

# Read the guard at this moment specifically. Both rules sit in one group
# and evaluate on the same tick, so a firing `value high` proves the guard
# has already been evaluated against the same rows — which is what makes
# "the guard is not firing" evidence rather than a race.
#
# If the correct rule never fired, that reasoning is gone: neither rule
# firing is the "harness is broken" case in rules.yml, and a guard that sat
# still because nothing could fire is not evidence of anything.
g=$(state_of "$RULE_GUARD" || true)
if [ "$fired" = 0 ]; then
    skip "whether the mis-ordered guard held (it is $g)" \
         "the correct rule never fired, so nothing distinguishes the guard from it"
elif [ "$g" = "firing" ]; then
    no "the mis-ordered guard fired" \
       "Grafana's 'last' reducer no longer takes the positionally last row. That is an improvement, but docs/ALERTING.md and rules.yml now describe a trap that no longer exists and must be corrected."
else
    ok "the mis-ordered guard stayed $g while the correct rule fired"
fi

echo
echo "=== F. the notification was actually delivered ==="

if [ "$fired" = 0 ]; then
    skip "notification delivery" \
         "the rule never fired, so Grafana had nothing to send and a silent sink proves nothing"
else
    i=0
    n=0
    while [ "$i" -lt 20 ]; do
        n=$(sink_count)
        [ "$n" -gt 0 ] 2>/dev/null && break
        sleep 3
        i=$((i + 1))
    done

    if [ "${n:-0}" -gt 0 ] 2>/dev/null; then
        ok "the sink received $n notification group(s)"
    else
        no "the rule fired but nothing reached the sink" \
           "rule state is not delivery: check the contact point and notification policy in rules.yml"
    fi

    if curl -s "$SINK/alerts" | python3 -c '
import json, sys
d = json.load(sys.stdin)
names = [
    a.get("alertname")
    for g in d.get("groups") or []
    for a in g.get("alerts") or []
    if a.get("status") == "firing"
]
if "'"$RULE_FIRES"'" not in names:
    print("firing alerts delivered: %s" % (names or "none")); sys.exit(1)
' >/tmp/nf_err 2>&1; then
        ok "the delivered payload names the firing rule"
    else
        no "the delivered payload did not name the firing rule" "$(cat /tmp/nf_err)"
    fi
fi

echo
echo "=== G. it resolves when the data comes back down ==="

# A rule that fires and never clears is a stuck alarm, which operationally
# is close to no alarm at all — so the return trip is checked too.
#
# Only meaningful if it fired. A rule sitting in Normal because it cannot
# fire will "recover" instantly and hand back a green tick for the one
# behaviour this phase exists to test.
if [ "$fired" = 0 ]; then
    skip "recovery to Normal" \
         "the rule never left Normal, so returning to it demonstrates nothing"
else
    if seed 5 30 0; then ok "seeded low values (5) as the newest rows"; else no "seeding recovery values failed"; fi

    if wait_state "$RULE_FIRES" inactive 120; then
        ok "the alert cleared back to Normal"
    else
        no "the alert did not clear" "state=$(state_of "$RULE_FIRES" || true)"
    fi
fi

echo
echo "------------------------------------------------------------------------------"
echo "  rule states at exit:"
rule_table | awk -F'\t' '{printf "    %-34s %-10s %s\n", $1, $2, $3}'
echo "------------------------------------------------------------------------------"
if [ "$skipped" -gt 0 ]; then
    echo "  $pass passed, $fail failed, $skipped skipped"
else
    echo "  $pass passed, $fail failed"
fi
if [ "$fail" -gt 0 ]; then
    echo "  DRILL FAILED"
    exit 1
fi
# Today a skip only ever follows a failure, so this is unreachable on a
# clean run. It is here so that if that ever stops being true, an
# unevaluated check cannot quietly read as a passing one.
if [ "$skipped" -gt 0 ]; then
    echo "  DRILL INCOMPLETE — $skipped check(s) could not be evaluated"
    exit 1
fi
echo "  DRILL PASSED"
