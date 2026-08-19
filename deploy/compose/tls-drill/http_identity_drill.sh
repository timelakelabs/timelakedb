#!/bin/sh
# SEC-3 v2 on the HTTP listener: a verified client certificate NARROWS what
# `/api/sql` will show, exactly as it already did on Flight SQL.
#
# Why this drill exists. Flight SQL owns its own accept loop and could read
# the peer certificate directly, so it has been narrowing since SEC-3 v2.
# `axum-server` owns the HTTP accept loop, so `/api/sql` requested a client
# certificate, verified it, and then authorized NOTHING with it — the
# property this project documents held on one query surface and not the
# other. `crates/server/src/tls_identity.rs` closed that. Unit tests cover
# the plumbing; this proves it against a real TLS handshake, which is the
# only place a certificate actually exists.
#
# Runs INSIDE a container on the compose network: Windows curl is schannel
# and cannot present a PEM client certificate, so a host-side run would
# silently test the anonymous path three times and pass.
#
#   cd deploy/compose/tls-drill && sh gen-certs.sh initial
#   sh gen-certs.sh client narrowed-agent && sh gen-certs.sh client ungranted-agent
#   docker compose -f deploy/compose/timelakedb-tls.yml up -d --build
#   docker run --rm --network bench-timelakedb-tls_default \
#     -v "$PWD:/repo:ro" alpine sh -c \
#     'apk add --no-cache curl >/dev/null && sh /repo/deploy/compose/tls-drill/http_identity_drill.sh'

set -eu

BASE=${BASE:-https://timelakedb-tls:1963}
CERTS=${CERTS:-/repo/deploy/compose/tls-drill/certs}
DB=poc
# Deterministic rows written into the same table read back as duplicates
# until compaction merges them, so every run gets its own table.
RUN=$(date +%s)
TBL="ident_$RUN"
JAR=/tmp/drill-cookies
PASS_NEW="http identity drill password"

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

eq() { # eq <description> <actual> <expected>
    if [ "$2" = "$3" ]; then
        pass=$((pass + 1))
        echo "  PASS  $1 (got $2)"
    else
        fail=$((fail + 1))
        echo "  FAIL  $1 (got $2, expected $3)"
    fi
}

CA="$CERTS/ca.crt"

# --- admin session (seeded admin/admin is quarantined until rotated) -------
login() {
    rm -f "$JAR"
    curl -sS --cacert "$CA" -c "$JAR" -H 'content-type: application/json' \
        -d "{\"username\":\"admin\",\"password\":\"$1\"}" \
        "$BASE/admin/session" |
        sed -n 's/.*"csrf":"\([^"]*\)".*/\1/p'
}

CSRF=$(login admin)
if [ -n "$CSRF" ]; then
    curl -sS --cacert "$CA" -b "$JAR" -H "x-timelake-csrf: $CSRF" \
        -H 'content-type: application/json' \
        -d "{\"current_password\":\"admin\",\"new_password\":\"$PASS_NEW\"}" \
        "$BASE/admin/password" >/dev/null 2>&1 || true
fi
CSRF=$(login "$PASS_NEW")
[ -n "$CSRF" ] || CSRF=$(login admin)

admin() { # admin <method> <path> [body]
    if [ -n "${3:-}" ]; then
        curl -sS --cacert "$CA" -b "$JAR" -H "x-timelake-csrf: $CSRF" \
            -H 'content-type: application/json' -X "$1" -d "$3" "$BASE$2"
    else
        curl -sS --cacert "$CA" -b "$JAR" -H "x-timelake-csrf: $CSRF" -X "$1" "$BASE$2"
    fi
}

# Query /api/sql claiming ops+audit. `who` selects the client identity.
# The claim is IDENTICAL in all three cases — only the certificate differs,
# which is what makes the row counts attributable to identity and nothing else.
count_as() { # count_as anonymous|<cn>
    who=$1
    ident=""
    if [ "$who" != "anonymous" ]; then
        ident="--cert $CERTS/client-$who.crt --key $CERTS/client-$who.key"
    fi
    # shellcheck disable=SC2086
    curl -sS --cacert "$CA" $ident \
        -H 'content-type: application/json' \
        -H 'X-TimeLake-Authorizations: ops,audit' \
        -d "{\"db\":\"$DB\",\"sql\":\"SELECT COUNT(*) AS n FROM $TBL\"}" \
        "$BASE/api/sql" |
        sed -n 's/.*"n":\([0-9]*\).*/\1/p'
}

echo "=== SEC-3 v2 · client-certificate identity on /api/sql (HTTP) ==="
echo "    base=$BASE table=$TBL"

echo
echo "--- A. the stack is up and mutually authenticating ---"
ck "health answers over TLS" curl -sSf --cacert "$CA" "$BASE/health"
ck "an admin session was established" test -n "$CSRF"
ck "the server requests client certificates (want mode)" \
    sh -c "curl -sS --cacert '$CA' '$BASE/metrics' | grep -q '^timelake_tls_client_auth_mode 1'"
ck "a client certificate is accepted on the HTTP listener" \
    curl -sSf --cacert "$CA" --cert "$CERTS/client-narrowed-agent.crt" \
        --key "$CERTS/client-narrowed-agent.key" "$BASE/health"

echo
echo "--- B. grants recorded for ONE of the two identities ---"
admin PUT "/admin/cert-grants/narrowed-agent" '{"authorizations":["ops"]}' >/dev/null
# Captured in THIS shell, not inside `sh -c`: `admin` is a shell function
# and does not survive into a child shell. An earlier revision tested it
# with `ck ... sh -c "admin GET ..."`, which made the positive check fail
# spuriously and — worse — made the negative check `! admin GET ...` pass
# VACUOUSLY, because "command not found" is a non-zero exit that the `!`
# turned into success. Both were measuring the absence of a function.
GRANTS=$(admin GET /admin/cert-grants)
echo "    /admin/cert-grants -> $GRANTS"
case "$GRANTS" in
    *narrowed-agent*) pass=$((pass + 1)); echo "  PASS  narrowed-agent has grants recorded" ;;
    *) fail=$((fail + 1)); echo "  FAIL  narrowed-agent has grants recorded" ;;
esac
case "$GRANTS" in
    *'"ops"'*) pass=$((pass + 1)); echo "  PASS  the grant is exactly [ops]" ;;
    *) fail=$((fail + 1)); echo "  FAIL  the grant is exactly [ops]" ;;
esac
# ungranted-agent is deliberately left with NO entry. `None` grants mean
# "no policy recorded", not "deny everything" — the latter would break a
# working client the moment it presented a certificate.
case "$GRANTS" in
    *ungranted-agent*) fail=$((fail + 1)); echo "  FAIL  ungranted-agent has no grants recorded" ;;
    *) pass=$((pass + 1)); echo "  PASS  ungranted-agent has no grants recorded" ;;
esac

echo
echo "--- C. rows behind SEC-2 visibility labels ---"
# public (no label), ops-only, audit-only. A caller claiming ops+audit and
# holding no grant policy sees all three.
LP="$TBL,host=a v=1.0 1700000000000000000
$TBL,host=b,_visibility=ops v=2.0 1700000001000000000
$TBL,host=c,_visibility=audit v=3.0 1700000002000000000"
# Not `set -e`-fatal: a failed write here should be REPORTED, not abort the
# run with no diagnostic.
w=$(curl -sS -o /dev/null -w '%{http_code}' --cacert "$CA" \
    -H 'content-type: text/plain' --data-binary "$LP" \
    "$BASE/api/v3/write_lp?db=$DB" || echo 000)
eq "the three labelled rows were accepted" "$w" 204

echo
echo "--- D. the property: identity NARROWS, and only where granted ---"
anon=$(count_as anonymous)
narrowed=$(count_as narrowed-agent)
ungranted=$(count_as ungranted-agent)
echo "    claims ops+audit in all three cases; only the certificate differs"
echo "    anonymous       -> $anon"
echo "    narrowed-agent  -> $narrowed   (granted [ops])"
echo "    ungranted-agent -> $ungranted   (no grant policy)"

# Want mode: an anonymous caller behaves exactly as before. If this ever
# fails, want mode has regressed into required mode and Grafana/Telegraf
# are broken.
eq "an anonymous caller is unaffected (want mode)" "$anon" 3
# THE claim. Granted [ops], claiming ops+audit, the intersection is {ops}:
# the public row and the ops row, never the audit row. Before
# tls_identity.rs this read 3 — the certificate was verified and ignored.
eq "a granted identity is NARROWED to claims-intersect-grants" "$narrowed" 2
# `None` grants = no policy, so claims pass through unchanged. Deny-all
# here would make presenting a certificate a downgrade.
eq "an identity with no grants recorded keeps its claims" "$ungranted" 3

echo
echo "--- E. the narrowing is enforced in the scan, not in the projection ---"
# SEC-2's hard part: an aggregate must not count a row the caller cannot
# see. COUNT(*) above already proves it for this table, so here we check the
# rows themselves agree with the count — a filter applied above the
# aggregate would show the discrepancy.
rows_narrowed=$(curl -sS --cacert "$CA" \
    --cert "$CERTS/client-narrowed-agent.crt" --key "$CERTS/client-narrowed-agent.key" \
    -H 'content-type: application/json' -H 'X-TimeLake-Authorizations: ops,audit' \
    -d "{\"db\":\"$DB\",\"sql\":\"SELECT host FROM $TBL ORDER BY host\"}" \
    "$BASE/api/sql" | grep -o '"host":"[abc]"' | wc -l | tr -d ' ')
eq "SELECT returns the same number of rows COUNT(*) claimed" "$rows_narrowed" "$narrowed"
ck "the audit-labelled row is the one withheld" \
    sh -c "! curl -sS --cacert '$CA' \
        --cert '$CERTS/client-narrowed-agent.crt' --key '$CERTS/client-narrowed-agent.key' \
        -H 'content-type: application/json' -H 'X-TimeLake-Authorizations: ops,audit' \
        -d '{\"db\":\"$DB\",\"sql\":\"SELECT host FROM $TBL\"}' \
        '$BASE/api/sql' | grep -q '\"host\":\"c\"'"

echo
echo "--- F. the identity is attributed in the query log (U2) ---"
# One row per query in _system.queries carries the verified CN, which is
# what makes "which client is doing this to us" answerable in SQL.
# Wait for a maintenance tick to store the sample, then read the count as a
# NUMBER. An earlier revision tested `grep -qv '"n":0'`, which succeeds on an
# error response too — so if `_system` did not exist yet the assertion would
# have passed while proving nothing.
sleep 12
attributed=$(curl -sS --cacert "$CA" -H 'content-type: application/json' \
    -d "{\"db\":\"_system\",\"sql\":\"SELECT COUNT(*) AS n FROM queries WHERE identity = 'narrowed-agent'\"}" \
    "$BASE/api/sql" | sed -n 's/.*"n":\([0-9]*\).*/\1/p')
anon_attributed=$(curl -sS --cacert "$CA" -H 'content-type: application/json' \
    -d "{\"db\":\"_system\",\"sql\":\"SELECT COUNT(*) AS n FROM queries WHERE identity IS NULL\"}" \
    "$BASE/api/sql" | sed -n 's/.*"n":\([0-9]*\).*/\1/p')
echo "    queries attributed to narrowed-agent : ${attributed:-<none>}"
echo "    queries with no identity (anonymous) : ${anon_attributed:-<none>}"
ck "the certificate CN is recorded on the query rows it ran" \
    sh -c "test -n '$attributed' && test '$attributed' -gt 0"
# Both must be non-empty: an anonymous caller gets a NULL identity rather
# than a placeholder, and if EVERY row were NULL the CN never arrived.
ck "anonymous queries are recorded with a NULL identity, not a placeholder" \
    sh -c "test -n '$anon_attributed' && test '$anon_attributed' -gt 0"

echo
echo "================================"
echo "  PASS: $pass   FAIL: $fail"
echo "================================"
test "$fail" -eq 0
