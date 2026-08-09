#!/bin/sh
# Run the harness INSIDE the target container's network namespace, so the
# client and server share a loopback. Measuring from Windows through a
# published port adds ~45 ms per request of Docker Desktop overhead, which
# is ~94% of the reported Shape A figure (docs/evidence/PERFORMANCE_LOG.md,
# 2026-08-09 07:15). Anything measured through the port is unusable.
#
#   run-innet.sh <container> <label> <scale> [extra bench.py args...]
set -e
CONTAINER="$1"; LABEL="$2"; SCALE="$3"; shift 3
export MSYS_NO_PATHCONV=1
exec docker run --rm --network "container:${CONTAINER}" \
  -v /c/project-time-lord-db/TimeLakeDB/bench:/bench -w /bench \
  tldb-bench:perf \
  python bench.py run --backend timelakedb --url http://localhost:1963 \
    --container none --scale "$SCALE" --label "$LABEL" "$@"
