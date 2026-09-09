#!/bin/sh
# #162 drill: TIMELAKE_DATA_AUTH defaults to `optional`, and the UNCHANGED
# AT-6 fixtures — a stock Telegraf with no token, a stock Grafana with no
# token, over Flight SQL — keep working against a node that sets nothing.
#
# Rig: deploy/compose/timelakedb.yml with both profiles, which sets no
# TIMELAKE_DATA_AUTH at all, so what the node runs is the compiled-in default:
#   docker compose -f deploy/compose/timelakedb.yml \
#     --profile telegraf --profile grafana up -d --build
#   sh deploy/compose/data_auth_default_drill.sh
#
# What it proves, in order:
#   A. a node given no TIMELAKE_DATA_AUTH reports mode 1 (optional);
#   B. what a tokenless stock Telegraf actually SENDS (a recorder on the
#      compose network, the same trick as docs/evidence/data-auth-client-
#      probe.log) — this is the fact the blank-credential rule rests on;
#   C. the fixture Telegraf's rows arrive, nothing rejected, and they are
#      counted as anonymous;
#   D. the fixture Grafana, holding no token, reads them over Flight SQL;
#   E. a WRONG token is 401 on both surfaces and counted as rejected;
#   F. a BLANK credential in each spelling is served as anonymous.
#
# Run from the HOST. T=node HTTP, G=Grafana.
set -e
# Git Bash rewrites `/probe.py` in a docker argument into
# `C:/Program Files/Git/probe.py` and the recorder dies on the spot (see
# packaging/README.md, same trap). Harmless everywhere else.
export MSYS_NO_PATHCONV=1
T=${T:-http://localhost:1963}
G=${G:-http://localhost:3003}
GUSER=${GUSER:-admin}; GPASS=${GPASS:-admin}
NET=${NET:-bench-timelakedb_default}
HERE=$(cd "$(dirname "$0")" && pwd)
RUN=$(date +%s)

pass=0; fail=0
chk() { if [ "$1" = "$2" ]; then echo "  PASS  $3"; pass=$((pass+1));
        else echo "  FAIL  $3 (got '$1' want '$2')"; fail=$((fail+1)); fi; }
chk_ge() { if [ "$1" -ge "$2" ] 2>/dev/null; then echo "  PASS  $3 ($1 >= $2)"; pass=$((pass+1));
           else echo "  FAIL  $3 (got '$1' want >= $2)"; fail=$((fail+1)); fi; }
code() { curl -s -o /dev/null -w "%{http_code}" "$@"; }
metric() { curl -s "$T/metrics" | grep "^$1 " | awk '{print $2}' | head -1; }
rows() {  # $1=table; anonymous read on /api/sql
  curl -s -X POST "$T/api/sql" -H 'content-type: application/json' \
    -d "{\"db\":\"poc\",\"sql\":\"SELECT COUNT(*) AS n FROM $1\"}" 2>/dev/null \
    | python -c "import sys,json
try:
    d=json.load(sys.stdin); print(d[0]['n'] if isinstance(d,list) and d else 0)
except Exception: print(0)"
}
cleanup() { docker rm -f tldb-162-recorder tldb-162-probe-telegraf >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "== #162: TIMELAKE_DATA_AUTH defaults to optional; tokenless stock clients still work =="

echo "-- A. the node sets nothing and runs optional --"
for i in $(seq 1 60); do curl -fs "$T/health" >/dev/null 2>&1 && break; sleep 1; done
chk "$(code "$T/health")" "200" "node healthy"
chk "$(docker inspect timelakedb --format '{{.Config.Env}}' | grep -c TIMELAKE_DATA_AUTH)" "0" \
    "the container's environment does not mention TIMELAKE_DATA_AUTH"
chk "$(metric timelake_data_auth_mode)" "1" "timelake_data_auth_mode reads 1 (optional) from the compiled-in default"

echo "-- B. what a tokenless stock Telegraf sends (recorder on $NET) --"
cleanup
# Files go in over stdin, never as a docker argument: a host path in a
# docker argument is exactly what Git Bash mangles, and `docker cp` needs one.
# The recorder is the container's MAIN process (it waits for its script to
# land), because `docker logs` shows only that process — a `docker exec -d`
# python prints into the void, and that is how the first cut of this step
# recorded nothing while everything else passed.
docker run -d --name tldb-162-recorder --network "$NET" python:3-slim \
  sh -c 'while [ ! -f /probe.py ]; do sleep 0.2; done; exec python /probe.py' >/dev/null
docker exec -i tldb-162-recorder sh -c 'cat > /probe.py.part && mv /probe.py.part /probe.py' \
  < "$HERE/tls-drill/http_probe.py"
docker run -d --name tldb-162-probe-telegraf --network "$NET" --entrypoint sh telegraf:latest \
  -c 'while [ ! -f /tmp/probe.ready ]; do sleep 0.2; done; exec telegraf --config /tmp/probe.conf' >/dev/null
docker exec -i tldb-162-probe-telegraf sh -c 'cat > /tmp/probe.conf && touch /tmp/probe.ready' <<'EOF'
[agent]
  interval = "2s"
  flush_interval = "2s"
[[inputs.mem]]
[[outputs.influxdb_v2]]
  urls = ["http://tldb-162-recorder:8086"]
  token = ""
  organization = "poc"
  bucket = "poc"
[[outputs.influxdb]]
  urls = ["http://tldb-162-recorder:8086"]
  database = "poc"
  username = "telegraf"
  password = ""
EOF
for i in $(seq 1 30); do
  [ "$(docker logs tldb-162-recorder 2>&1 | grep -c PROBE)" -ge 2 ] && break; sleep 1
done
# The recorded lines are printed with any `Basic` value DECODED. The raw
# base64 of "telegraf:" is not a secret (the password is empty), but
# GitHub's secret scanning flags `Basic <base64>` wherever it appears, and
# an alert per drill run is noise nobody will keep reading (alert #3).
# Same reason the expected value below is computed rather than written.
decode_basic() { python -c "import sys,re,base64
for line in sys.stdin:
    print(re.sub(r\"Basic ([A-Za-z0-9+/=]+)\", lambda m: 'Basic base64(%r)' % base64.b64decode(m.group(1)).decode(), line), end='')"; }
docker logs tldb-162-recorder 2>&1 | grep PROBE | sort -u | decode_basic | sed 's/^/  recorded: /'
if [ "$(docker logs tldb-162-recorder 2>&1 | grep -c PROBE)" -lt 2 ]; then
  echo "  (recorder saw fewer than two probes; the probe Telegraf said:)"
  docker logs tldb-162-probe-telegraf 2>&1 | tail -6 | sed 's/^/    /'
fi
V2=$(docker logs tldb-162-recorder 2>&1 | grep "path=/api/v2/write" | head -1 | sed -n "s/.*authorization=\(.*\)$/\1/p")
V1=$(docker logs tldb-162-recorder 2>&1 | grep "path=/write" | head -1 | sed -n "s/.*authorization=\(.*\)$/\1/p")
BASIC_TELEGRAF=$(python -c "import base64; print(base64.b64encode(b'telegraf:').decode())")
# Go's HTTP client trims the trailing space, so the wire value is `Token`,
# scheme only. The node treats scheme-only and scheme-plus-space the same.
chk "$V2" "'Token'" "influxdb_v2 output with token=\"\" sends 'Token' with nothing after it"
chk "$V1" "'Basic $BASIC_TELEGRAF'" "influxdb (v1) output with a username and no password sends Basic base64('telegraf:')"
cleanup

echo "-- C. the fixture Telegraf (token = \"\") writes, and is counted as anonymous --"
N=0
for i in $(seq 1 60); do N=$(rows cpu); [ "$N" -gt 0 ] 2>/dev/null && break; sleep 2; done
chk_ge "$N" 1 "cpu rows from the tokenless fixture Telegraf are readable"
chk "$(metric timelake_data_requests_rejected_total)" "0" "nothing has been rejected"
chk_ge "$(metric timelake_data_requests_anonymous_total)" 1 "the writes were counted as anonymous"
chk "$(metric timelake_data_requests_authenticated_total)" "0" "and none as authenticated (no token exists yet)"

echo "-- D. the fixture Grafana holds no token and reads over Flight SQL --"
for i in $(seq 1 60); do curl -fs "$G/api/health" >/dev/null 2>&1 && break; sleep 1; done
chk "$(code "$G/api/health")" "200" "grafana healthy"
DS=$(curl -s -u "$GUSER:$GPASS" "$G/api/datasources/uid/influxdb3")
chk "$(printf '%s' "$DS" | python -c "import sys,json; d=json.load(sys.stdin); print('token' in (d.get('secureJsonFields') or {}))")" \
    "False" "the provisioned datasource carries no token"
FROM=$(python -c 'import time; print(int((time.time()-3600)*1000))')
TO=$(python -c 'import time; print(int(time.time()*1000))')
Q="{\"queries\":[{\"refId\":\"A\",\"datasource\":{\"type\":\"influxdb\",\"uid\":\"influxdb3\"},\"rawSql\":\"SELECT COUNT(*) AS n FROM cpu\",\"format\":\"table\",\"intervalMs\":60000,\"maxDataPoints\":100}],\"from\":\"$FROM\",\"to\":\"$TO\"}"
GN=$(curl -s -u "$GUSER:$GPASS" -X POST "$G/api/ds/query" -H 'content-type: application/json' -d "$Q" \
  | python -c "import sys,json
d=json.load(sys.stdin); r=(d.get('results') or {}).get('A') or {}
if r.get('error'): print('error: '+str(r['error'])[:120]); sys.exit(0)
f=r.get('frames') or []
print(f[0]['data']['values'][0][0] if f and f[0]['data']['values'] else 0)")
chk_ge "$GN" 1 "Grafana's own Flight SQL client, tokenless, gets a row count"
REJ_AFTER_G=$(metric timelake_data_requests_rejected_total)
chk "$REJ_AFTER_G" "0" "the Grafana read was not rejected"

echo "-- E. a WRONG token is refused on both surfaces --"
chk "$(code -X POST "$T/api/v2/write?org=poc&bucket=poc&precision=ns" -H 'authorization: Token tldb_wrong' --data-binary "drill,run=$RUN v=1i")" \
    "401" "write with a wrong token -> 401"
chk "$(code -X POST "$T/api/sql" -H 'authorization: Bearer tldb_wrong' -H 'content-type: application/json' -d '{"db":"poc","sql":"SELECT 1"}')" \
    "401" "/api/sql with a wrong token -> 401"
chk "$(metric timelake_data_requests_rejected_total)" "2" "both counted as rejected"

echo "-- F. a BLANK credential in each spelling is served as anonymous --"
ANON0=$(metric timelake_data_requests_anonymous_total)
chk "$(code -X POST "$T/api/v2/write?org=poc&bucket=poc&precision=ns" -H 'authorization: Token' --data-binary "drill,run=$RUN v=2i")" \
    "204" "'Token' (what a tokenless influxdb_v2 output sends, as recorded above) -> 204"
chk "$(code -X POST "$T/write?db=poc&precision=ns" -H "authorization: Basic $BASIC_TELEGRAF" --data-binary "drill,run=$RUN v=3i")" \
    "204" "'Basic base64(telegraf:)' (what a passwordless v1 output sends) -> 204"
chk "$(code -X POST "$T/api/v2/write?org=poc&bucket=poc&precision=ns" -H 'authorization: Bearer' --data-binary "drill,run=$RUN v=4i")" \
    "204" "'Bearer' with no value -> 204"
chk "$(code -X POST "$T/api/v2/write?org=poc&bucket=poc&precision=ns" -H 'authorization: Digest abc' --data-binary "drill,run=$RUN v=5i")" \
    "401" "a scheme the node does not speak is still presented-and-unusable -> 401"
ANON1=$(metric timelake_data_requests_anonymous_total)
chk_ge "$((ANON1 - ANON0))" 3 "the three blank writes were counted as anonymous"
chk "$(metric timelake_data_requests_rejected_total)" "3" "only the Digest one joined the rejected count"

echo
echo "== $pass passed, $fail failed =="
[ "$fail" -eq 0 ]
