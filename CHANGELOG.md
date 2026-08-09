# Changelog

All notable changes to TimelordDB are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
will follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html) from
its first release.

Nothing has been released yet. Everything below is pre-v1 work on `main`,
organised by the milestone that gated it. Every performance or robustness
entry traces to a recorded run in `bench/results/` — the harness is the
specification, so an entry without a measurement behind it does not belong
here.

## [Unreleased]

### Added — schema discovery (2026-08-08)

- `information_schema` is enabled on the query session, so `SHOW TABLES`,
  `DESCRIBE` and `information_schema.tables` work over `/api/sql`.
- The default catalog is named after the database being queried, so the
  three-part names BI tools generate — `poc.public.events` — resolve, and
  agree with what Flight SQL reports as a catalog. Planner errors now read
  `table 'poc.public.nope' not found`.
- Flight SQL answers `CommandGetCatalogs`, `CommandGetDbSchemas`,
  `CommandGetTables` (including `include_schema`), `CommandGetTableTypes` and
  `CommandGetSqlInfo`, all of which previously returned `Unimplemented` —
  which locked out every ADBC or JDBC client that enumerates a schema before
  it will show anything. A database is a catalog, every table sits in
  `public`, and `GetSqlInfo` reports the server as read-only because writes
  arrive as line protocol and there is no DDL.
- New `SqlBackend::databases`/`tables`/`table_schema`, backed by
  `Engine::databases`/`table_names`/`table_schema` extracted from
  `sql_batches` so the query and metadata paths cannot drift.

### Fixed — non-ASCII text was mangled (2026-08-08)

- The line-protocol parser decoded each byte as a character — a Latin-1
  decode — so every multi-byte character became mojibake: a tag value of
  `München` was stored and returned as `MÃ¼nchen`. It affected measurement
  names, tag keys and values, field keys and string field values. Bytes are
  now collected and decoded once as UTF-8.
- Unchanged, and now covered by a test: a body that is not valid UTF-8 is
  refused whole with `{"error":"body is not utf-8"}` before the parser and
  before the WAL. Line protocol has no byte escape, so such data has to be
  transcoded or base64-encoded by the client.

### Fixed — a rejected write could wedge the whole engine (2026-08-08)

- `TableBuffer::append` is now atomic. It pushed a row's tag values before
  validating its field types, so a type conflict returned an error with the
  tag columns already one longer than `time`. Every later snapshot of that
  table then failed with *"all columns in a record batch must have the same
  length"* — which killed reads of the table, the flush that would have
  drained it, and, because the maintenance tick ran the stages with `?`,
  compaction and retention for **every table on the node**. The WAL replayed
  the poisoned line at boot, so a restart did not clear it.
- A **duplicate tag key in one line** (`m,h=a,h=b v=1`) caused the same
  corruption from a *successful* write: the column was pushed twice for one
  row. A repeated key now takes its last value, for tags and fields alike.
- Field types are validated against the existing column — and against a
  column the same line is about to create — before anything is mutated.
- `flush_all` encodes each table through `flush_one`, so one bad buffer no
  longer discards the other tables' rows; WAL generations are retained when
  any table fails to flush, and the maintenance tick runs flush, compaction
  and retention independently rather than aborting on the first error.
- Regression tests: three in `timelord-buffer`, plus
  `a_rejected_write_cannot_poison_the_table_or_the_engine` covering the whole
  cascade end to end — reject, read, second table, duplicate key, flush,
  restart.

### Added — documentation and project files (2026-08-08)

- `site/docs/reference.html`: line protocol grammar, escapes and field types;
  the SQL dialect with what is and is not supported; the HTTP and Flight SQL
  surfaces; an InfluxDB compatibility matrix; every metric with its type and
  suggested alerts; and a glossary. Written from the code and verified against
  a running server.

- `LICENSE` (Apache-2.0), `SECURITY.md`, `CONTRIBUTING.md`,
  `CODE_OF_CONDUCT.md` and this changelog.
- `SECURITY.md` states the real pre-v1 posture: no authentication and no
  authorization on either listener, and the exposures that follow from it.
- `docs/BACKUP_RESTORE.md` and `ops/tldb-backup.sh` — the AT-5 procedure as a
  runnable script rather than a measured result with no method.
- Project website in `site/` (landing + documentation), published by
  `.github/workflows/pages.yml`.

### Added — SEC-3: TLS 1.3 with hot certificate rotation (2026-08-08)

- New `timelord-tls` crate: validate-before-swap certificate loading (PEM
  structure, leaf expiry, key↔certificate match) behind an `ArcSwap`
  resolver that is consulted only during a handshake.
- TLS on both listeners when `TIMELORD_TLS_CERT` and `TIMELORD_TLS_KEY` are
  set — HTTP via `axum-server`, Flight SQL via a `tokio-rustls` accept loop.
  Plaintext remains the default. `TIMELORD_TLS_MIN=1.2` lowers the floor.
- Rotation triggers: a 2 s file-modification watcher and
  `POST /admin/tls/reload`.
- A rejected renewal keeps the last-good pair serving and raises the named
  alarm `SEC3_CERT_RENEWAL_FAILED`.
- Metrics: `timelord_tls_cert_expiry_seconds`, `timelord_tls_last_reload_ok`.
- **AT-7 drill: 19/19** (`bench/results/at7-drill.log`). Under stock
  Telegraf-over-HTTPS plus sustained writes, a rotation landed mid-flight in a
  20 s Flight SQL query with an exact result, zero write errors and zero
  dropped connections.

### Fixed

- Runtime image moved to `debian:trixie-slim` to match the builder's glibc;
  a bookworm runtime failed at startup with `GLIBC_2.38 not found` after
  `rust:1-slim` moved forward.

### Added — M5: acceptance drills and the metadata cache (2026-08-08)

- Metadata cache over immutable Parquet footers: warm point lookups
  **0–6 ms** against roughly 300 ms cold, closing the M4 p95 carve-out.
- **AT-6:** stock Telegraf writes with only a URL configured; the unchanged
  fixture Grafana dashboards render over Flight SQL.
- **AT-5:** backup 34 s, restore from a destroyed volume 13 s, all 36.68M
  rows exact; SIGKILL mid-ingest recovered healthy in 4.7 s with zero
  acknowledged-write loss (40,340,794 rows exact); ten consecutive 100K
  bursts absorbed in ≤0.13 s each.
- **AT-4:** a repeat full-scale run within tolerance — ingest ±3.5%, funnels
  ±6%, storage ±9%, zero errors in both runs.

### Added — M4: the full-scale gate (2026-08-08)

- Shared `FairSpillPool` with an admission semaphore and a server-side query
  deadline (RR-1); scans moved to the blocking pool so a slow scan can always
  be preempted.
- Pruning table provider: time-bound file skipping, row-group statistics
  pruning, projection pushdown, decode-time row filters.
- Entity-clustered compaction, grace-period GC (`TIMELORD_GC_GRACE_SECS`),
  and a schema registry.
- **AT-3 gate green with two carve-outs** at 36.6M events against the
  InfluxDB 3 baseline: ingest 365–671K lines/s with zero errors, Shape A
  median **211 ms** (vs 520), all Shape B complete — funnel 1.7 s (vs 5.7),
  B4 0.68 s (vs 30.3) — storage **0.50 GB/day** (vs 1.15), and zero
  acknowledged-row loss proven by fixed-bound equality on identical data.
- Carve-outs carried forward: Shape A p95 608 ms against a 250 ms target, and
  intra-run ingest decline under maintenance contention (stable across runs,
  so not cardinality decay).

### Added — M3: compaction, retention, Flight SQL (2026-08-08)

- Compaction merges L0 files per `(table, hour)` with cross-file
  last-write-wins de-duplication (FR-5).
- Per-table retention drops whole partitions as a catalog operation (FR-7).
- Flight SQL on port 1964 serving Grafana's stock datasource (FR-8).

### Added — M2: the storage engine (2026-08-08)

- Write path: parser → WAL (fsynced before the 204, generation-rotated) →
  in-memory buffer.
- Flush: primary-key sort and last-write-wins de-duplication into
  `(table, UTC hour)` Parquet partitions through the single store chokepoint,
  committed to a manifest-log catalog, then WAL reclaim.
- Reads union live buffer snapshots with catalogued Parquet under the RR-1
  memory pool. A WAL cap answers 429 with `Retry-After` (RR-5).
- **SIGKILL → healthy in 0.8 s** with zero acknowledged-write loss (RR-3).

### Added — M0/M1: workspace and ingest path (2026-08-08)

- Cargo workspace, server binary, Docker image, compose target, CI gate
  (fmt, clippy `-D warnings`, tests, 80% line coverage).
- Line-protocol parser with the full escape set, Arrow buffer with
  `Dictionary<Int32, Utf8>` tag columns, DataFusion SQL over the buffer, and
  the InfluxDB-compatible write endpoints (`/write`, `/api/v2/write`,
  `/api/v3/write_lp`).
- `timelorddb` backend adapter for the tsdb-bench harness.

### Added — the evidence base (2026-08-08)

- `REQUIREMENTS.md` and `ARCHITECTURE.md` derived from the five-engine
  evaluation, with the tsdb-bench harness, benchmark record and Grafana
  fixtures vendored so the repository is self-contained.
