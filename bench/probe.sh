#!/bin/sh
# probe.sh "<sql>" [n] — run probe.py in the target's network namespace.
set -e
export MSYS_NO_PATHCONV=1
exec docker run --rm --network container:tldb-perf \
  -v /c/project-time-lord-db/TimeLakeDB/bench:/bench -w /bench \
  tldb-bench:perf python probe.py "$1" "${2:-20}"
