#!/bin/sh
# Run the workspace test suite in Docker against a named target volume, so a
# performance cycle's second `cargo test` is incremental rather than cold.
# (This machine has no Rust toolchain — CLAUDE.md "Build & verify".)
set -e
export MSYS_NO_PATHCONV=1
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
exec docker run --rm -v "$ROOT":/src -w /src \
  -v timelake-cargo-cache:/usr/local/cargo/registry \
  -v tldb-test-target:/src/target \
  rust:1-slim cargo "$@"
