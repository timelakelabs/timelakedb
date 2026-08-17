#!/bin/sh
# Runs after files are removed.
#
# What this deliberately does NOT do: delete /var/lib/timelake, or remove the
# timelake user. Uninstalling a database package must never destroy the
# database — an operator who wants the data gone removes it explicitly, and
# leaving the account means the files keep a valid owner until they do.
set -e

if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
fi

# dpkg passes "purge" when the admin asked for the config to go too. Even
# then the data stays; only the config directory we created is cleaned up.
if [ "$1" = "purge" ]; then
    rm -f /etc/timelakedb/timelakedb.env
    rmdir /etc/timelakedb 2>/dev/null || true
    cat <<'EOF'
TimeLakeDB configuration removed. The data directory /var/lib/timelake was
kept, along with the `timelake` user that owns it. Remove them by hand if you
mean to discard the database:

    rm -rf /var/lib/timelake && userdel timelake

EOF
fi

exit 0
