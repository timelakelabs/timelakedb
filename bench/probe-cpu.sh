#!/bin/sh
# Hammer one query in-network while sampling the server's CPU. If a scan is
# serial, a 24-core container sits near 100% of ONE core while the query runs.
#   probe-cpu.sh "<sql>" [n]
set -e
SQL="$1"; N="${2:-60}"
export MSYS_NO_PATHCONV=1
docker rm -f tldb-probe >/dev/null 2>&1 || true
docker run -d --name tldb-probe --network container:tldb-perf \
  -v /c/project-time-lord-db/TimeLakeDB/bench:/bench -w /bench \
  tldb-bench:perf python probe.py "$SQL" "$N" >/dev/null
sleep 3
for _ in 1 2 3 4 5 6; do
  docker stats --no-stream --format "cpu={{.CPUPerc}} mem={{.MemUsage}}" tldb-perf
done
docker wait tldb-probe >/dev/null
docker logs tldb-probe
docker rm -f tldb-probe >/dev/null 2>&1 || true
