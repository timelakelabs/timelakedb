#!/bin/sh
# Run a cargo command in the toolchain image (this machine has no local
# Rust). Shares one registry cache volume across invocations.
#   rust.sh "cargo test --workspace"
set -e
export MSYS_NO_PATHCONV=1
exec docker run --rm \
  -v /c/project-time-lord-db/TimeLakeDB:/src -w /src \
  -v timelake-cargo-cache:/usr/local/cargo/registry \
  rust:1-slim sh -c "$1"
