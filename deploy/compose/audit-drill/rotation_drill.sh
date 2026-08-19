#!/bin/sh
# Audit-rotation drill (P1-2): the chain survives rotation, and deleting a
# segment does not go unnoticed.
#
# The crate has unit tests for both properties. This runs them against a
# REAL node, over HTTP, through the endpoint an auditor would actually use —
# because the unit tests exercise `AuditSink` directly, and the thing being
# claimed is that `GET /admin/audit?verify=1` tells the truth about a trail
# that has rotated on a running server.
#
# Runs INSIDE the timelakedb container: it already has curl (its healthcheck
# uses it) and it owns the data directory, so no extra container, no network
# plumbing, and no host path translation.
#
#   docker compose -f compose/timelakedb.yml -f compose/audit-rotate.yml up -d
#   docker exec -i timelakedb sh -s < compose/audit-drill/rotation_drill.sh

set -eu

BASE=http://localhost:1963
AUDIT_DIR=${AUDIT_DIR:-/var/lib/timelake/data/audit}
MUTATIONS=${MUTATIONS:-40}
JAR=/tmp/drill-cookies
PASS_NEW="audit drill password"

pass=0
fail=0
ck() {
    d=$1
    shift
    if "$@" >/dev/null 2>&1; then
        echo "  ok    $d"
        pass=$((pass + 1))
    else
        echo "  FAIL  $d"
        fail=$((fail + 1))
    fi
}

# --- admin session ---------------------------------------------------------
# A fresh node seeds admin/admin and quarantines it: the only thing that
# credential may do is change its own password. So provisioning rotates,
# which also invalidates the session that did it, hence the second login.
login() { # login <password> -> csrf on stdout
    rm -f "$JAR"
    curl -sS -c "$JAR" -H 'content-type: application/json' \
        -d "{\"username\":\"admin\",\"password\":\"$1\"}" \
        "$BASE/admin/session" |
        sed -n 's/.*"csrf":"\([^"]*\)".*/\1/p'
}

CSRF=$(login admin)
if [ -n "$CSRF" ]; then
    curl -sS -b "$JAR" -H "x-timelake-csrf: $CSRF" -H 'content-type: application/json' \
        -d "{\"current_password\":\"admin\",\"new_password\":\"$PASS_NEW\"}" \
        "$BASE/admin/password" >/dev/null 2>&1 || true
fi
CSRF=$(login "$PASS_NEW")
[ -n "$CSRF" ] || CSRF=$(login admin)

admin() { # admin <method> <path> [body]
    if [ -n "${3:-}" ]; then
        curl -sS -b "$JAR" -H "x-timelake-csrf: $CSRF" -H 'content-type: application/json' \
            -X "$1" -d "$3" "$BASE$2"
    else
        curl -sS -b "$JAR" -H "x-timelake-csrf: $CSRF" -X "$1" "$BASE$2"
    fi
}

segments() { ls -1 "$AUDIT_DIR" 2>/dev/null | grep -c '\.jsonl$' || echo 0; }
records()  { admin GET '/admin/audit?limit=10000' | grep -o '"seq":' | grep -c seq || echo 0; }
verify_ok() { admin GET '/admin/audit?verify=1&limit=1' | grep -q '"ok":true'; }

echo "=== audit rotation drill ==="
echo "  audit dir: $AUDIT_DIR"
ck "an admin session was established" test -n "$CSRF"

echo "--- baseline ---"
before_segments=$(segments)
before_records=$(records)
echo "    segments=$before_segments records=$before_records"
ck "the trail verifies before we start" verify_ok

echo "--- driving $MUTATIONS admin mutations ---"
i=0
while [ $i -lt "$MUTATIONS" ]; do
    # Each retention change is one audit record with a real before/after.
    admin PUT /admin/retention "{\"table\":\"drill_t$i\",\"duration\":\"$((i + 1))d\"}" >/dev/null
    i=$((i + 1))
done

after_segments=$(segments)
after_records=$(records)
echo "    segments=$after_segments records=$after_records"

ck "the trail ROTATED (more than one segment on disk)" test "$after_segments" -gt 1
ck "every mutation is still readable across the segments" \
    test "$after_records" -ge "$((before_records + MUTATIONS))"

echo "--- the property that matters: the chain spans the boundaries ---"
ck "verify=1 reports an intact chain AFTER rotation" verify_ok

echo "--- retention deleted nothing (it is unset) ---"
ck "no segment disappeared on its own" test "$after_segments" -ge 2

echo "--- deleting a whole segment must be DETECTED ---"
victim=$(ls -1 "$AUDIT_DIR"/audit.*.jsonl 2>/dev/null | head -1)
if [ -z "$victim" ]; then
    echo "  FAIL  no rotated segment to remove"
    fail=$((fail + 1))
else
    echo "    removing $victim"
    rm -f "$victim"
    # Through a file, not a shell variable: the body is JSON full of quotes,
    # and re-quoting it into `sh -c` is how a passing assertion turns into a
    # failing one that has nothing to do with the product.
    admin GET '/admin/audit?verify=1&limit=1' > /tmp/verify.json
    echo "    verify says: $(sed -n 's/.*\("verify":{[^}]*}[^}]*}\).*/\1/p' /tmp/verify.json)"
    ck "verify=1 now reports a BREAK"          grep -q '"ok":false' /tmp/verify.json
    ck "the break names the seq it happened at" grep -q '"seq"'      /tmp/verify.json
    ck "the break explains itself"              grep -q '"reason"'   /tmp/verify.json
fi

echo
echo "passed $pass, failed $fail"
[ "$fail" -eq 0 ] || exit 1
echo "AUDIT ROTATION DRILL PASSED"
