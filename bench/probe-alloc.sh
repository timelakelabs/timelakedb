#!/bin/sh
# What does one query cost the KERNEL, and how much fresh memory does it
# fault in? Samples the server's /proc/1/stat either side of a hammered
# query, so the per-query figures are deltas, not absolutes.
#
#   probe-alloc.sh <container> [n] [case]
#
# Reads, per query:
#   minflt  minor page faults — freshly mapped pages touched for the first
#           time. A decoder that mmaps its buffers and returns them pays
#           this on every batch; one that reuses a thread-local heap does
#           not. The kernel zeroes each such page, which is write bandwidth
#           the query never asked for.
#   stime   kernel CPU. Fault handling and mmap/munmap land here, so a high
#           system share is the signature of allocation churn rather than
#           of decode work.
#   utime   user CPU. Together with wall time this gives the parallel width
#           actually achieved.
set -e
C="${1:-tldb-perf}"; N="${2:-200}"; CASE="${3:-b2_load}"
export MSYS_NO_PATHCONV=1
HERE="$(dirname "$0")"

# /proc/pid/stat: "pid (comm) state ...", and comm may contain spaces, so
# cut at the LAST ')' and index from `state`. minflt/majflt/utime/stime are
# global fields 10/12/14/15, i.e. 8/10/12/13 of the remainder.
sample() {
  docker exec "$C" sh -c 'cat /proc/1/stat' | sed 's/.*) //' |
    awk '{print $8, $10, $12, $13}'
}

before=$(sample)
sh "$HERE/probe-innet.sh" "$C" probe-alloc.py "$N" "$CASE"
after=$(sample)

echo "$before" "$after" "$N" | awk '{
  hz = 100
  minflt = $5 - $1; majflt = $6 - $2
  ut = ($7 - $3) * 1000 / hz; st = ($8 - $4) * 1000 / hz
  n = $9
  printf "per query: minflt=%.0f majflt=%.0f utime=%.1fms stime=%.1fms sys_share=%.0f%%\n", \
    minflt / n, majflt / n, ut / n, st / n, 100 * st / (ut + st + 0.0001)
}'
