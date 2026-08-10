#!/bin/sh
# Build the perf-cycle server image WITHOUT a cold release build each time.
#
# The repo Dockerfile compiles inside the image, so a one-line source change
# invalidates `COPY crates` and pays a full release build again (~15 min).
# A performance cycle builds twice — baseline and candidate — so instead:
# compile into a named target volume (incremental across cycles, and off the
# Windows filesystem), then bake only the binary into the runtime stage.
#
#   ops/perf-build.sh [image-tag]
set -e
TAG="${1:-timelakedb:perf}"
export MSYS_NO_PATHCONV=1
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

docker volume create tldb-perf-target >/dev/null
docker run --rm -v "$ROOT":/src -w /src \
  -v timelake-cargo-cache:/usr/local/cargo/registry \
  -v tldb-perf-target:/src/target \
  rust:1-slim cargo build --release -p timelake-server

# A relative build context on purpose: MSYS_NO_PATHCONV=1 is needed for the
# container-side paths above, and it also stops Git Bash rewriting an absolute
# host path, which `docker build` then cannot find.
cd "$ROOT"
CTX="target-perf-ctx"
rm -rf "$CTX"; mkdir -p "$CTX"
docker run --rm -v tldb-perf-target:/t rust:1-slim \
  cat /t/release/timelake-server >"$CTX/timelake-server"

cat >"$CTX/Dockerfile" <<'EOF'
FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY timelake-server /usr/local/bin/timelake-server
ENV TIMELAKE_ADDR=0.0.0.0:1963
ENV TIMELAKE_DATA_DIR=/var/lib/timelake/data
EXPOSE 1963 1964
ENTRYPOINT ["timelake-server"]
EOF

docker build -q -t "$TAG" "./$CTX"
rm -rf "$CTX"
echo "built $TAG"
