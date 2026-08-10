#!/bin/sh
# Correctness gate for a performance cycle, through Docker (no local Rust).
#
#   ops/perf-check.sh            # test + fmt + clippy
#   ops/perf-check.sh test       # just the tests
#
# Separate target volume from ops/perf-build.sh's: debug and release
# artifacts do not share, and mixing them just evicts each other.
set -e
WHAT="${1:-all}"
export MSYS_NO_PATHCONV=1
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

run() {
  docker run --rm -v "$ROOT":/src -w /src \
    -v timelake-cargo-cache:/usr/local/cargo/registry \
    -v tldb-test-target:/src/target \
    rust:1-slim "$@"
}

[ "$WHAT" = "fmtclippy" ] || run cargo test --workspace
[ "$WHAT" = "test" ] || {
  run cargo fmt --all -- --check || echo "!! fmt drift (judge only YOUR lines)"
  run cargo clippy --workspace --all-targets 2>&1 | tail -40
}
