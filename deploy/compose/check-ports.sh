#!/bin/sh
# Every host port published by every rig in this directory, and a non-zero
# exit if two rigs publish the same one (#42).
#
# Rigs were added on different days and each picked a free-looking number;
# three pairs collided (alerting/cluster on 5963-4, console/data-auth on
# 4963-4, tls/console Grafana on 3004), and the cost was a drill running
# against the other rig's node. Run this before choosing a port for a new
# rig, and from CI if anyone ever wires it:
#
#   sh deploy/compose/check-ports.sh          # table + verdict
#
# Overlays (audit-rotate.yml) publish nothing and are skipped by the grep.
#
# What this does NOT check: the sibling harnesses' tables. Riverkeeper's and
# Catchment's stack.py reach some of these rigs by host port (data-auth on
# 4963, the cluster rigs on 5962-5983); moving one of those is a change in
# three repositories, and this script cannot see the other two. Grep them
# before moving a number a harness might hold.
set -e
cd "$(dirname "$0")"
tmp=$(mktemp); trap 'rm -f "$tmp"' EXIT
for f in timelakedb*.yml; do
  # `- "HOST:CONTAINER"` lines, any indentation, comment after allowed.
  sed -n 's/^[[:space:]]*- "\([0-9][0-9]*\):[0-9][0-9]*".*/\1/p' "$f" | while read -r p; do
    printf '%s\t%s\n' "$p" "$f"
  done
done | sort -n > "$tmp"
cat "$tmp"
dups=$(cut -f1 "$tmp" | uniq -d)
if [ -n "$dups" ]; then
  echo "DUPLICATE host ports:" >&2
  for d in $dups; do grep "^$d	" "$tmp" >&2; done
  exit 1
fi
echo "ok: $(wc -l < "$tmp" | tr -d ' ') host ports, no duplicates"
