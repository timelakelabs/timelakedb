# Contributing to TimeLakeDB

TimeLakeDB is specified from evidence: five engines ran an identical
high-cardinality workload, and their measured failures became the
requirements. That history sets the one rule that matters here — **claims
are measured, not asserted.** If a change is supposed to make something
faster, smaller or more robust, a benchmark run says so.

## Before you start

- `REQUIREMENTS.md` — every FR/PR/RR/SR/CL/SEC requirement and the
  acceptance tests (AT-1…AT-7) that close them.
- `ARCHITECTURE.md` — components, seams, and how requirements became
  structure.
- `../Gauge/bench/README.md` — the harness. It is the executable
  specification, and it lives in Gauge since the G0 split.
- `../Gauge/docs/BENCHMARK_RESULTS.md` — the measurements the project exists
  to answer.

Changes that contradict `REQUIREMENTS.md` §9 (anti-requirements) will be
declined regardless of how well they perform — those are the four designs the
evidence forbids.

## Development environment

Docker is the only hard prerequisite. A local Rust toolchain is optional and
only speeds up the edit loop.

### Build and test with Docker (no toolchain needed)

A named cache volume keeps `cargo` from recompiling the world each time:

```bash
docker volume create timelake-cargo-cache

docker run --rm -v "$PWD":/src -w /src \
  -v timelake-cargo-cache:/usr/local/cargo/registry \
  rust:1-slim cargo test --workspace
```

Run the server the way the benchmarks do:

```bash
cd deploy
docker compose -f compose/timelakedb.yml up -d --build
curl http://localhost:1963/health
```

### With a local toolchain

```bash
cargo test --workspace
cargo run -p timelake-server        # listens on TIMELAKE_ADDR, default 0.0.0.0:1963
```

The toolchain is pinned by `rust-toolchain.toml`. On Windows this also needs
the MSVC build tools.

### Container gotcha

The runtime stage of `Dockerfile` must track the builder's glibc. When
`rust:1-slim` moved to trixie, a bookworm runtime stage started failing with
`GLIBC_2.38 not found` at startup. If you bump the builder, bump the runtime.

## The workspace

Seventeen crates, each with a single job. Boundaries are load-bearing —
particularly `store` (the one object-I/O chokepoint, where SEC-1
encryption lives as the `EncryptingStore` decorator) and `query` (the one
mandatory-predicate injection point, where SEC-2 visibility labels are
enforced inside the scan). Do not route I/O around them, and do not add
a second enforcement point.

| Crate | Job |
|---|---|
| `ingest` | Line-protocol parser (FR-1). No heavy dependencies. |
| `wal` | Write-ahead log with generations; fsync before the 204 (RR-3). |
| `buffer` | Mutable per-table buffer with immutable Arrow snapshots (PR-9). |
| `store` | **The single chokepoint for all object I/O**, including SEC-1 envelope encryption and the `put_if_absent` CAS primitive. |
| `store-s3` | `S3Store` + `AwsKms` behind those same traits (CL-1). |
| `catalog` | The manifest log that makes the object store the source of truth (CL-1). |
| `compact` | Merges a partition's L0 files into one settled file (PR-6). |
| `retention` | Per-table retention as whole-file drops (FR-7). |
| `query` | DataFusion integration under a shared memory budget (FR-3, RR-1). |
| `api` | HTTP surface: writes, `/api/sql`, `/metrics`, health (FR-1, FR-9). |
| `flight` | Flight SQL server — the Grafana read path (FR-8). |
| `tls` | TLS 1.3 with hot certificate rotation, want-mode client certificates (SEC-3). |
| `auth` | Principals, roles and sessions for the admin surface; data-plane tokens and the one `guard::decide` both HTTP and Flight route through (SEC-4). |
| `audit` | The hash-chained, fsync'd, fail-closed audit trail behind every admin mutation (P1-2 / SR-6). |
| `cluster` | `Role` (`TIMELAKE_ROLE`), the `Discovery` trait and its static backend (`TIMELAKE_PEERS`). |
| `discovery` | Pluggable cluster membership (CL-5 seam). |
| `server` | The engine that composes the above, plus the binary — including the router, replication, selfmon and rate-limit modules. |

## What CI enforces

`.github/workflows/ci.yml` runs, and a pull request must pass:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo llvm-cov --workspace \
  --ignore-filename-regex 'main\.rs|store-s3/src/lib\.rs' --fail-under-lines 80
```

Clippy warnings are errors, and line coverage may not drop below 80%.
Two files are excluded from the coverage gate, both because they are
exercised somewhere that job is not: `main.rs` (socket bind and serve —
covered by the container healthcheck) and `store-s3` (S3 and KMS need a
live endpoint).

A second job, **`store-s3`**, runs that crate's `#[ignore]`d tests against
a LocalStack service container on every push — the `Store` contract over
S3 including range reads, the KMS envelope round-trip, and the two-writer
catalog CAS over `If-None-Match`. So the exclusion above means "covered by
another job", not "not covered". To run them yourself, bring up the C0 rig
(`deploy/compose/timelakedb-s3.yml`) and:

```bash
AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
TLDB_S3_TEST_BUCKET=timelake-it TLDB_KMS_TEST_KEY=alias/timelake \
cargo test -p timelake-store-s3 -- --ignored
```

Coverage headroom is thin (82.74% against the 80% gate at the last
recorded runner result, `docs/evidence/P0-1-ci.md`; it had dipped to
80.84% after the CL-3 work before the router integration tests), so a
change that adds much untested code will fail the gate rather than
quietly erode it. That is the intent.

One known wrinkle: `crates/server/tests/health.rs`'s
`rows_stay_visible_while_a_slow_flush_uploads` passes when the test binary
runs whole — which is how `cargo test` and CI run it — but can fail when
run alone through a filter, where the timing lets it catch the documented
sub-millisecond window in `flush_all` between the buffer swap and the
holding area. If you are bisecting with `--exact`, that is why.

## Measuring a change

For anything touching the write path, the read path, compaction, retention or
memory, run the harness and attach the result:

```bash
cd ../Gauge          # the harness lives in Gauge, not here (G0 split)
python bench/bench.py run --backend timelakedb --scale smoke --label my-change   # ~30 s sanity
python bench/bench.py run --backend timelakedb --scale laptop --label my-change  # real signal
python bench/bench.py compare <baseline-run> <my-change-run>
```

Recorded benchmark runs live in `../Gauge/bench/results/`; the InfluxDB 3
runs there are the baselines to beat. This repository's `docs/evidence/`
holds the correctness drill transcripts, which are a different thing. Do not invent a new measurement method for a change — if
the harness cannot express what you are claiming, extend the harness (see
"Adding a backend" and the scenario definitions in `../Gauge/bench/scenarios.py`) so
the next person can reproduce it.

Two hard-won measurement rules:

- **Set a container memory limit** on anything you run. An unbounded engine
  once wedged an entire Docker VM and every container on it.
- **Use a named volume, not a host bind mount**, for the data directory. On
  Windows and macOS a bind mount cost roughly three seconds per query in file
  read overhead alone, which will silently ruin your numbers.

## Pull requests

- One concern per PR, with a subject line that says what changed and a body
  that says *why* — the constraint or measurement behind it. The existing
  history is written that way on purpose; it is the project's design record.
- Reference the requirement or acceptance test involved (`FR-7`, `RR-1`,
  `AT-5`) when there is one.
- Update the documentation that the change invalidates: `README.md` status,
  `site/docs/index.html` (API reference, configuration table, behaviour),
  `CHANGELOG.md`, and `REQUIREMENTS.md`/`ARCHITECTURE.md` if a decision
  changed. A new `TIMELAKE_*` variable is not finished until it is in the
  configuration table with its real default.
- Include the benchmark evidence for performance or robustness claims.

## Reporting bugs and vulnerabilities

Functional bugs: open an issue with the version or commit, the configuration,
what you observed, and — ideally — a harness scenario that reproduces it.

Security issues: **do not open a public issue.** Follow `SECURITY.md`. Note
that the data plane ships with `TIMELAKE_DATA_AUTH=off` by design — the
mechanism exists and is drilled, the default is the compatibility
contract — so "the data plane is open on a default install" is the
documented posture, not a vulnerability report.

## Code of conduct

Participation is governed by `CODE_OF_CONDUCT.md`.
