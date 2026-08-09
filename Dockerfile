# TimeLakeDB server image (M0 stub).
FROM rust:1-slim AS build
WORKDIR /src
COPY Cargo.toml rust-toolchain.toml ./
COPY crates ./crates
RUN cargo build --release -p timelake-server

# trixie matches the glibc of the rust:1-slim builder (a bookworm runtime
# broke with `GLIBC_2.38 not found` when the builder image moved forward)
FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/timelake-server /usr/local/bin/timelake-server
ENV TIMELAKE_ADDR=0.0.0.0:1963
ENV TIMELAKE_DATA_DIR=/var/lib/timelake/data
EXPOSE 1963 1964
HEALTHCHECK --interval=10s --timeout=5s --retries=12 \
    CMD curl -sf http://localhost:1963/health || exit 1
ENTRYPOINT ["timelake-server"]
