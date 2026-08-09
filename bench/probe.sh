#!/bin/sh
# probe.sh "<sql>" [n] — run probe.py in the target's network namespace.
set -e
export MSYS_NO_PATHCONV=1
BENCH=$(cd "$(dirname "$0")" && pwd)
exec docker run --rm --network container:tldb-perf \
  -v "$BENCH":/bench -w /bench \
  tldb-bench:perf python probe.py "$1" "${2:-20}"
