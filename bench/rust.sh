#!/bin/sh
# Run a cargo command in the toolchain image (this machine has no local
# Rust). Shares one registry cache volume across invocations.
#   rust.sh "cargo test --workspace"
set -e
export MSYS_NO_PATHCONV=1
# Resolve the repo from this script's own location, so the checkout can
# live anywhere. Under Git Bash `pwd` yields the /c/... form docker wants
# with MSYS_NO_PATHCONV set.
REPO=$(cd "$(dirname "$0")/.." && pwd)
exec docker run --rm \
  -v "$REPO":/src -w /src \
  -v timelake-cargo-cache:/usr/local/cargo/registry \
  rust:1-slim sh -c "$1"
