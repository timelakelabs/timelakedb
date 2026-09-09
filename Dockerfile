# TimeLakeDB server image (M0 stub).
FROM rust:1-slim AS build
WORKDIR /src
# Cargo.lock is copied and the build is --locked: a RELEASE image has to be
# the tree that CI proved, and without the lock file cargo re-resolves at
# build time, so an image tagged 0.5.0 could contain dependency versions
# nothing ever tested. .dockerignore has always un-ignored the lock; the
# COPY simply never listed it.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
RUN cargo build --release --locked -p timelake-server

# trixie matches the glibc of the rust:1-slim builder (a bookworm runtime
# broke with `GLIBC_2.38 not found` when the builder image moved forward)
FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/timelake-server /usr/local/bin/timelake-server

# Run as a non-root user (SECURITY.md exposure 4). Defence in depth behind
# the read-only SQL surface: if some future write primitive slips the plan
# guard, it writes as an unprivileged uid to a filesystem it mostly cannot
# touch, instead of as root anywhere. The data dir is the one place this
# user owns — everything else stays root-owned and, under a read-only root
# filesystem (see the compose `read_only: true` + tmpfs), unwritable.
RUN groupadd --system --gid 1000 timelake \
    && useradd --system --uid 1000 --gid 1000 --home-dir /var/lib/timelake timelake \
    && mkdir -p /var/lib/timelake/data \
    && chown -R timelake:timelake /var/lib/timelake
USER timelake:timelake

ENV TIMELAKE_ADDR=0.0.0.0:1963
ENV TIMELAKE_DATA_DIR=/var/lib/timelake/data
EXPOSE 1963 1964
HEALTHCHECK --interval=10s --timeout=5s --retries=12 \
    CMD curl -sf http://localhost:1963/health || exit 1
ENTRYPOINT ["timelake-server"]
