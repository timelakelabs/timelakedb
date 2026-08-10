#!/bin/sh
# Run a probe script from bench/ INSIDE the target container's network
# namespace — same reason as run-innet.sh: measured through a published port
# on Windows every request carries ~45 ms of Docker Desktop overhead.
#
#   probe-innet.sh <container> <script.py> [args...]
set -e
CONTAINER="$1"; SCRIPT="$2"; shift 2
export MSYS_NO_PATHCONV=1
BENCH=$(cd "$(dirname "$0")" && pwd)
exec docker run --rm --network "container:${CONTAINER}" \
  -v "$BENCH":/bench -w /bench tldb-bench:perf python "$SCRIPT" "$@"
