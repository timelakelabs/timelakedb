#!/bin/sh
# Install the built packages on the distros a release actually targets, and
# prove the result works: files land, the service account is created, the
# operator's config survives an upgrade, removal keeps the database, and the
# binary starts and answers /health on that distro's glibc.
#
# Run after packaging/build.sh:
#
#   packaging/verify.sh                     # every target
#   packaging/verify.sh rockylinux:9        # just one
#
# This is not ceremony. On its first run it caught two defects that no amount
# of reading the spec would have shown:
#   * `apt remove` deleted /var/lib/timelake, because a package-owned empty
#     directory is removed with the package;
#   * every install failed on Amazon Linux 2023 with "Error in PREIN
#     scriptlet", because AL2023 ships without shadow-utils and the scriptlet
#     called useradd.
#
# The same script runs the host loop and the in-container checks, so CI and a
# laptop cannot drift.

set -eu

# ---------------------------------------------------------------- in-container
if [ "${1:-}" = "--in-container" ]; then
    fails=0
    ck() { # ck "<description>" <command...>
        d=$1
        shift
        if "$@" >/dev/null 2>&1; then
            echo "  ok    $d"
        else
            echo "  FAIL  $d"
            fails=$((fails + 1))
        fi
    }

    . /etc/os-release 2>/dev/null || true
    echo "  ${PRETTY_NAME:-unknown} / glibc $(ldd --version 2>&1 | head -1 | sed 's/.* //')"

    if command -v apt-get >/dev/null 2>&1; then
        FMT=deb
        export DEBIAN_FRONTEND=noninteractive
        apt-get update -qq >/dev/null 2>&1
        command -v curl >/dev/null 2>&1 || apt-get install -y -qq curl >/dev/null 2>&1
        apt-get install -y -qq /dist/*.deb >/dev/null 2>&1
    else
        FMT=rpm
        # AL2023 ships curl-minimal; asking for `curl` there starts a package
        # conflict, so only install it when the command is genuinely absent.
        command -v curl >/dev/null 2>&1 || dnf install -y -q curl >/dev/null 2>&1
        dnf install -y -q /dist/*.rpm >/dev/null 2>&1
    fi

    ck "binary installed"  test -x /usr/bin/timelake-server
    ck "unit installed"    test -f /usr/lib/systemd/system/timelakedb.service
    ck "config installed"  test -f /etc/timelakedb/timelakedb.env
    ck "data dir created"  test -d /var/lib/timelake/data
    ck "service account"   id timelake

    # Docs are asserted in the PAYLOAD, not on disk: the Ubuntu and AL2023
    # images configure their package manager to discard /usr/share/doc/*, so
    # a filesystem check there measures the image, not the package.
    if [ "$FMT" = deb ]; then
        ck "docs in package" sh -c 'dpkg -c /dist/*.deb | grep -q usr/share/doc/timelakedb/SECURITY.md'
    else
        ck "docs in package" sh -c 'rpm -qlp /dist/*.rpm | grep -q /usr/share/doc/timelakedb/SECURITY.md'
    fi

    # The packaged default must be loopback: a package that starts listening
    # on every interface with an unauthenticated data plane is the one
    # mistake this whole layout exists to avoid (SECURITY.md exposure 1).
    ck "default bind is loopback" \
        grep -q '^TIMELAKE_ADDR=127.0.0.1:1963$' /etc/timelakedb/timelakedb.env
    ck "default flight bind is loopback" \
        grep -q '^TIMELAKE_FLIGHT_ADDR=127.0.0.1:1964$' /etc/timelakedb/timelakedb.env

    # The real question: does this binary run on THIS distro's glibc?
    mkdir -p /tmp/d
    TIMELAKE_DATA_DIR=/tmp/d \
    TIMELAKE_ADDR=127.0.0.1:1963 \
    TIMELAKE_FLIGHT_ADDR=127.0.0.1:1964 \
        /usr/bin/timelake-server >/tmp/server.log 2>&1 &
    srv=$!
    served=1
    i=0
    while [ $i -lt 30 ]; do
        if curl -sf http://127.0.0.1:1963/health >/tmp/health.json 2>/dev/null; then
            served=0
            break
        fi
        i=$((i + 1))
        sleep 1
    done
    if [ $served -eq 0 ]; then
        echo "  ok    serves /health: $(cat /tmp/health.json)"
    else
        echo "  FAIL  never served /health; server log:"
        sed 's/^/        /' /tmp/server.log
        fails=$((fails + 1))
    fi
    kill $srv 2>/dev/null || true

    # An upgrade must not clobber the operator's configuration.
    echo "# operator edit" >> /etc/timelakedb/timelakedb.env
    if [ "$FMT" = deb ]; then
        apt-get install -y -qq --reinstall /dist/*.deb >/dev/null 2>&1
    else
        dnf reinstall -y -q /dist/*.rpm >/dev/null 2>&1
    fi
    ck "config survives upgrade" grep -q "operator edit" /etc/timelakedb/timelakedb.env

    # Uninstalling a database must never destroy the database.
    echo "pretend parquet" > /var/lib/timelake/data/canary
    if [ "$FMT" = deb ]; then
        apt-get remove -y -qq timelakedb >/dev/null 2>&1
    else
        dnf remove -y -q timelakedb >/dev/null 2>&1
    fi
    ck "binary removed"       test ! -f /usr/bin/timelake-server
    ck "data directory kept"  test -d /var/lib/timelake/data
    ck "data itself kept"     test -f /var/lib/timelake/data/canary
    ck "service account kept" id timelake

    if [ "$fails" -ne 0 ]; then
        echo "  $fails CHECK(S) FAILED ($FMT)"
        exit 1
    fi
    echo "  all checks passed ($FMT)"
    exit 0
fi

# ------------------------------------------------------------------- host loop
REPO_ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$REPO_ROOT"

[ -n "$(ls dist/*.deb dist/*.rpm 2>/dev/null)" ] || {
    echo "no packages in dist/ — run packaging/build.sh first" >&2
    exit 1
}

# Two package managers, and the oldest and newest glibc we claim to support.
# AL2023 is not decoration: it is the EC2 default and the only one of these
# that ships without shadow-utils.
TARGETS=${*:-"debian:12 ubuntu:22.04 rockylinux:9 amazonlinux:2023"}

rc=0
for image in $TARGETS; do
    echo "===== $image ====="
    if docker run --rm \
        -v "$REPO_ROOT/dist":/dist:ro \
        -v "$REPO_ROOT/packaging/verify.sh":/verify.sh:ro \
        "$image" sh /verify.sh --in-container
    then :; else
        echo "  ^ FAILED on $image"
        rc=1
    fi
    echo
done

if [ $rc -ne 0 ]; then
    echo "package verification FAILED"
    exit 1
fi
echo "package verification passed on every target"
