#!/bin/sh
# Runs after files land, on both a fresh install and an upgrade.
#
# Deliberately does NOT enable or start the service. A database whose data
# plane is unauthenticated by default (SECURITY.md exposure 1) should not
# begin listening because someone ran `apt install`; starting it is an
# operator's decision, taken after reading the config. The message below says
# so in the place they will actually see it.
set -e

DATA_DIR=/var/lib/timelake/data
CONF_DIR=/etc/timelakedb

# The data directory is the only path the service may write; systemd's
# StateDirectory= also enforces this, but a correct owner before first start
# means `timelake-server` run by hand behaves the same as the unit.
mkdir -p "$DATA_DIR"
chown -R timelake:timelake /var/lib/timelake
chmod 0750 /var/lib/timelake "$DATA_DIR"

# The config may carry credentials (bootstrap password, encryption key path),
# so it is readable by the service account and root, and nobody else.
if [ -d "$CONF_DIR" ]; then
    chown -R root:timelake "$CONF_DIR"
    chmod 0750 "$CONF_DIR"
    [ -f "$CONF_DIR/timelakedb.env" ] && chmod 0640 "$CONF_DIR/timelakedb.env"
fi

if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
    # On an upgrade, restart only if the operator had it running.
    if systemctl is-active --quiet timelakedb 2>/dev/null; then
        systemctl restart timelakedb >/dev/null 2>&1 || true
    else
        cat <<'EOF'

TimeLakeDB is installed but NOT started.

  1. Review  /etc/timelakedb/timelakedb.env
     It binds 127.0.0.1 only. The data plane is unauthenticated by default —
     set TIMELAKE_DATA_AUTH=required (and issue tokens) or front it with an
     authenticating proxy before exposing it beyond loopback.

  2. Start it:
       systemctl enable --now timelakedb

  3. Check it:
       curl -s http://127.0.0.1:1963/health

The admin console is at http://127.0.0.1:1963/admin/ui — it seeds
admin/admin on first start and forces a password change before anything
else works. Do that immediately.

EOF
    fi
fi

exit 0
