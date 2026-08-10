# Changelog

All notable changes to TimeLakeDB are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
will follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html) from
its first release.

Nothing has been released yet. Everything below is pre-v1 work on `main`,
organised by the milestone that gated it. Every performance or robustness
entry traces to a recorded run in `bench/results/` — the harness is the
specification, so an entry without a measurement behind it does not belong
here.

## [Unreleased]

### Fixed — P0-4: catalog commits are atomic against a second writer (2026-08-10)

- **Two writers on one object store can no longer lose each other's
  commits.** `Catalog::commit` picked the next manifest sequence from an
  in-process counter and wrote `catalog/manifest/{seq}.json` with a plain
  `put`; two writers replaying the same log computed the same `seq`, and
  the second `put` silently overwrote the first — its data files left
  orphaned in the store and invisible to every query. Latent on one node,
  active the moment a second writer appears (a botched restore, a stray
  container, day one of clustering).
- Commit is now **compare-and-swap on the next sequence key**
  (`put_if_absent`: S3 `If-None-Match`, local `File::create_new`). The
  loser of a race replays the winner's entries, folds them into memory,
  and retries at the new head — the manifest log becomes a true total
  order, every slot with exactly one writer. Bounded at 100 attempts
  (→ `ResourceBusy`); each retry catches up first, so honest contention
  converges in a few rounds.
- Metric `timelake_catalog_commit_conflicts_total` — 0 on a single writer,
  climbing means real contention, so it is visible rather than inferred.
- Drilled on **both** CAS mechanisms, which are different code: local
  hard-link (3 catalog tests, including the two-writer loss scenario and
  removals-survive-retry) and **real S3 `If-None-Match` via LocalStack**
  (a two-`Catalog`-on-one-bucket drill, `--ignored`).
  `bench/results/catalog-cas-drill.log`.
- Deferred to C2 (safe because maintenance is single-node until the role
  split): re-validating a commit against the new state on conflict, so a
  compaction whose inputs were concurrently retention-dropped aborts
  rather than resurrects dropped data.

### Fixed — P0-2: the data-plane SQL surface is read-only, container non-root (2026-08-10)

- **`POST /api/sql` and Flight SQL can no longer write files.** DataFusion's
  `COPY … TO '<path>'` wrote a Parquet file as the server process — verified
  against the pre-fix image, a root-owned file outside the data directory
  from one unauthenticated request. The surface is now read-only, enforced
  on the **logical plan** (not the query text, which a comment or
  `EXPLAIN ANALYZE` would defeat): `SELECT`/`SHOW`/`DESCRIBE`/`EXPLAIN` run,
  `COPY`/DDL/DML/session statements are refused, and the walk reaches inside
  `EXPLAIN ANALYZE` so a nested `COPY` cannot hide. Deny-by-default — the
  classifier matches every `LogicalPlan` variant with no wildcard, so a
  future DataFusion node fails the build rather than slipping through. HTTP
  and Flight SQL share the one enforcement point.
- **The container runs non-root under a read-only root filesystem.**
  `USER timelake` (uid 1000) in the image; the shipped compose sets
  `read_only: true` with a `tmpfs /tmp` and the data volume as the only
  writable mount. Defence in depth behind the SQL guard.
- Incidental fix: `CREATE`/`DROP TABLE` used to parse and return `[]` while
  doing nothing; they are now refused explicitly, which the roadmap called
  the correct outcome over silently succeeding at nothing.
- Drill: `bench/results/sql-sandbox-drill.log` (the exposure reproduced,
  then closed on both surfaces, nothing written, process non-root, rootfs
  read-only, reads untouched). +4 tests.
- **Upgrade note:** switching to uid 1000 means a data volume created under
  the old root image is root-owned and unwritable; the node exits with
  `open engine (recovery): Permission denied`. Chown the volume to
  `1000:1000` once, or start on a fresh volume.

### Added — SEC-4 phased: data-plane token authentication (2026-08-10)

- `TIMELAKE_DATA_AUTH=off|optional|required` turns on token
  authentication on the data plane. **Default `off`** does not examine
  `Authorization` at all — today's compatibility contract, so a Telegraf
  migrated from InfluxDB with a leftover token keeps writing unchanged.
  `optional` serves anonymous callers but refuses a presented-but-invalid
  token (fail loud on day one); `required` refuses any request without a
  valid one, on both `:1963` and `:1964`.
- **One token, three spellings, because that is what stock clients send**
  (`bench/results/data-auth-client-probe.log`): `Bearer` (Grafana's
  Flight SQL token field, and Tributary), `Token` (Telegraf
  `influxdb_v2`), `Basic` with the token as the password (Telegraf v1).
  Grafana's basic-auth toggle and custom headers are HTTP-only and never
  reach gRPC, which is *why* the token field is the mechanism rather than
  a preference.
- New `crates/auth/src/token.rs` and `guard.rs`: 256-bit OS-CSPRNG
  secrets stored only as SHA-256 digests (not Argon2id — a token has no
  brute-force surface, and a memory-hard KDF on the write path would be a
  self-inflicted RR-1 violation), scopes (`read`/`write`/`read_write`,
  not a total order — a shipper writes without reading back), database
  scoping, SEC-2 grants that *intersect* claimed authorizations, expiry
  and revocation. HTTP and Flight SQL enforce through **one** decision
  function; Flight re-authenticates at DoGet as well as planning, because
  a ticket is client-crafted.
- Console `/admin/tokens` (issue/list/revoke, admin-only, secret shown
  once and never re-listed) plus a page section.
- Metrics: `timelake_data_auth_mode` and the split
  `timelake_data_requests_{authenticated,anonymous,rejected}_total` — an
  operator flips `optional` → `required` on that measurement, not a
  guess, exactly as want-mode mTLS did.
- Drilled live end to end (`bench/results/data-auth-drill.log`) in a
  container, because in-process router tests cannot reach the Flight
  accept loop where the gRPC guard runs.
- Not done: Tributary presenting the token (P0-5).

### Added — SEC-3 (v2): optional client certificates in want mode (2026-08-10)

- `TIMELAKE_TLS_CLIENT_CA` turns on client-certificate verification in
  **want mode**: both listeners request a certificate, verify one if
  offered, and serve the connection either way. Grafana, Telegraf and
  the bench harness connect unchanged with no configuration.
- Trust anchors sit behind the same `ArcSwap` as the serving
  certificate and hot-rotate on the same trigger, with **dual-CA
  overlap** — a bundle carrying both outgoing and incoming anchors, so
  a CA roll does not require every client to change at one instant. A
  bundle that will not parse leaves the last-good anchors serving and
  raises the named alarm, exactly as a bad server renewal does.
- **The identity is the point, not the encryption.** A verified client
  certificate's common name reaches the query session over Flight SQL,
  where `QuerySession::resolve` intersects the caller's claimed SEC-2
  authorizations with what that identity is granted. Anonymous callers
  keep today's documented behaviour, so this narrows without breaking:
  the fix for SECURITY.md exposure 7 is additive rather than a flag day.
- Metrics: `timelake_tls_client_ca_anchors`,
  `timelake_tls_client_ca_last_reload_ok`,
  `timelake_tls_client_auth_mode`, and the split that makes want mode
  observable at all — `timelake_flight_connections_authenticated_total`
  against `timelake_flight_connections_anonymous_total`. Without those
  two, both paths return 200 and nothing tells an operator whether any
  client presents a certificate yet, so the decision to *require* one
  would have to be a guess.
- **AT-6 re-drilled under want mode** and extended into a real gate
  (`bench/results/at6-grafana-want-mode.log`, 11/11): the TLS compose
  stack gained a `grafana` profile whose datasource deliberately
  configures no client certificate, and all **58 panel queries** from
  the four fixture dashboards execute and return data through Grafana's
  own Flight SQL plugin while the server is asking every client for a
  certificate. A certificate-bearing client connects in the same run and
  is counted separately (1 authenticated vs 58 anonymous).
- **AT-7 remains 19/19** with client auth enabled
  (`bench/results/sec3-mtls-want-mode.log`). The drill itself needed a
  fix — it called `/admin/tls/reload` anonymously and SEC-4 had put that
  behind a session — so it now logs in and rotates the seeded credential,
  exercising the authenticated path instead of routing around it.
- Not done: `/api/sql` does not yet carry the identity; axum-server owns
  that accept loop and needs a custom `Accept` to surface peer
  certificates.

### Changed — renamed TimelordDB → TimeLakeDB (2026-08-09)

The project is now **TimeLakeDB**, and the name says what the architecture
is: immutable Parquet on object storage behind an Iceberg-style manifest
log, with compute replaceable (CL-1). Nothing has been released, so this
is a rename rather than a migration — but several of the strings are
user-facing contracts and they all moved together:

- **Crates:** `timelord-*` → `timelake-*` (all 15).
- **Environment:** `TIMELORD_*` → `TIMELAKE_*` (~20 variables, including
  `TIMELAKE_ADDR`, `TIMELAKE_DATA_DIR`, `TIMELAKE_OBJECT_STORE`,
  `TIMELAKE_KMS_KEY_ID`, `TIMELAKE_ENCRYPTION_KEY`).
- **Metrics:** `timelord_*` → `timelake_*` (~30 series). Renaming now
  costs nothing; after a release it would have broken every dashboard.
- **HTTP headers:** `X-Timelord-Authorizations` → `X-TimeLake-Authorizations`
  (SEC-2), `x-timelord-csrf` → `x-timelake-csrf` (SEC-4).
- **Paths:** `/var/lib/timelord/data` → `/var/lib/timelake/data`.
- **Harness:** `bench/backends/timelakedb.py`, backend key `timelakedb`,
  and the three compose targets (`timelakedb.yml`, `-tls`, `-s3`).
- **Brand:** logo, wordmark, site and all documentation.

Two things deliberately did **not** change:

- **The `TLDE1` encryption magic bytes.** It is a format version marker,
  not a brand string, and changing it would make every previously written
  object fail the magic check — routing it down the plaintext-passthrough
  path and returning ciphertext as data. A cosmetic rename is not worth a
  silent-corruption mode.
- **Historical evidence** under `bench/results/` and `ops/logs/`. Those
  record runs that actually happened under the old name; rewriting them
  would falsify the record.

`TLDB_*` / `tldb-*` identifiers (the backup script, drill variables, the
session cookie) were left alone — the abbreviation is true of both names.

### Added — SEC-4: authentication on the admin surface (2026-08-09)

- New `timelake-auth` crate: principals with `viewer`/`operator`/`admin`
  roles, Argon2id credentials, server-side sessions (cookie or bearer)
  with 30-minute idle and 12-hour absolute expiry, per-principal
  exponential backoff on failed logins. Principals persist through the
  `Store`, so they are envelope-encrypted with everything else (SEC-1).
- **Every `/admin/*` route now authenticates**, which closes SECURITY.md
  exposure 3a — the unauthenticated deletion control. Mutations from a
  cookie session additionally require a double-submit CSRF token and an
  Origin check. The `/admin/tls/reload` endpoint moved behind the same
  guard and now requires `admin`.
- **First run seeds `admin`/`admin`, quarantined**: it authenticates, and
  then the only route that answers is `POST /admin/password` — everything
  else returns `403 password_change_required`. Rotation invalidates every
  session for that principal, including the one that performed it. The
  replacement cannot be shorter than 8 characters, the username, or
  `admin`. `TIMELAKE_ADMIN_BOOTSTRAP_PASSWORD` provisions a real password
  instead so no well-known default ever exists. This reverses the
  bootstrap-token design; the cost is recorded in docs/CONSOLE.md §4.2.
- Retention authorization follows the data, not the verb: **growing** a
  window needs `operator`; **shrinking, introducing, or removing** one
  needs `admin`.
- The console at `/admin/ui` became a three-state page — sign in, forced
  password change, then management — still one self-contained file with
  no build step. It ships no data; every value it shows is fetched
  through an authenticated call.
- Metrics: `timelake_admin_default_credential_active` (alert on this),
  `timelake_admin_logins_total`, `timelake_admin_login_failures_total`.
- **The data plane is deliberately untouched**: `/write`, `/api/sql` and
  Flight SQL still require no credentials, so Telegraf, Grafana and the
  harness keep working. That migration is its own milestone (SEC-4
  "phased").

### Added — runtime retention management + GUI (2026-08-09)

- Retention (FR-7) is now a runtime control, not a boot-time setting:
  `GET/PUT /admin/retention` and `DELETE /admin/retention/{table}` manage
  per-table windows (`365d`/`72h`/`90m`/seconds); changes persist to
  `catalog/config/retention.json` through the store — envelope-encrypted
  like every object, S3-shared in the cluster era — and outlive a restart
  with a stale environment. `TIMELAKE_RETENTION` remains the seed when no
  stored config exists; bench fixtures are untouched.
- `GET /admin/ui`: a self-contained management page (no build step, no
  external assets, site palette) listing active policies, table-name
  autocomplete from `SHOW TABLES`, set/remove with an explicit
  "shrinking a window deletes data" warning, and the live
  `timelake_retention_drops_total` counter.
- SECURITY.md exposure 3a: `/admin/retention` is an **unauthenticated
  deletion control** — the strongest reason yet to keep 1963 private
  until token auth lands.

### Added — C0: S3 object store with KMS envelope + SSE-KMS, key-cached (2026-08-09)

- New `timelake-store-s3` crate: `S3Store` implements the `Store` trait
  over aws-sdk-s3 (owned-runtime sync bridge, safe from blocking and
  async contexts alike), and `AwsKms` implements the `Kms` trait over
  aws-sdk-kms (`generate` ↔ GenerateDataKey, `unwrap` ↔ Decrypt). The
  engine cannot tell S3 from a local directory (CL-1).
- The `Store` trait gains ONE method — `put_if_absent` — the multi-writer
  CAS primitive (S3 `If-None-Match: *`; local hard-link publish;
  encrypted passthrough). The sequence-keyed manifest log makes racing
  catalog commits collide on the same key, so exactly one wins.
- The `Kms` trait gains `generate()` (default: local random + wrap), and
  `CachingKms` decorates any Kms with the caching-CMM pattern: one data
  key reused per bounded window (default 300 s / 1,000 uses, hard cap
  2¹⁶) on encrypt, a bounded wrapped-blob→key LRU on decrypt. Thousands
  of KMS calls become a handful; `TIMELAKE_KMS_CACHE=off` restores
  strict per-object keys and is the drill's measured baseline.
- Server-side encryption rides every PUT: SSE-KMS headers with
  **S3 Bucket Keys enabled**, plus bucket-default SSE in the rig's init.
- Config: `TIMELAKE_OBJECT_STORE=s3://bucket[/prefix]`,
  `TIMELAKE_KMS_KEY_ID` (alias ok), `TIMELAKE_S3_SSE_KEY_ID`,
  `TIMELAKE_KMS_CACHE[_MAX_AGE_SECS|_MAX_USES]`; LocalStack via
  `AWS_ENDPOINT_URL` (path-style auto-forced). Setting both KMS and
  local-KEK key sources refuses to start.
- Metrics: `timelake_kms_{generate,decrypt}_total`,
  `timelake_kms_{generate,decrypt}_cache_hits_total`,
  `timelake_s3_{get,put,head,list,delete}_total`,
  `timelake_s3_{read,write}_bytes_total`.
- LocalStack rig: `bench/compose/timelakedb-s3.yml` (S3+KMS, init
  creates `alias/timelake` and buckets with default SSE + Bucket Keys).
  The rig proves correctness, call counts, and recovery — never latency.

### Added — SEC-1: encryption at rest, at the store chokepoint (2026-08-09)

- `EncryptingStore(inner, kms)` in `timelake-store`: every object —
  Parquet, manifests, checkpoints — is envelope-encrypted with a fresh
  per-object AES-256-GCM data key, wrapped by the configured key. The
  engine is unchanged; the decorator slots in at `Engine::open`.
- Encrypted objects are chunked (64 KiB, one auth tag per chunk, header +
  object path as AAD), so the range-read path keeps working: a bloom probe
  decrypts a few KB, a footer read takes the tail, and chunks cannot be
  reordered, cross-spliced, or truncated undetected. A tampered object or
  a wrong key is a clean named error.
- Opt-in: `TIMELAKE_ENCRYPTION_KEY` (64 hex chars) or
  `TIMELAKE_ENCRYPTION_KEY_FILE`. A malformed key refuses to start rather
  than silently serving plaintext. Objects written before the key existed
  remain readable. `timelake_encryption_enabled` gauge.
- Decision recorded in ARCHITECTURE §11: whole-object envelope over
  Parquet Modular Encryption — covers non-Parquet objects, no dependency
  on arrow-rs PME maturity (retires §16 risk 2); PME per-column keys stay
  open at the same seam.

### Added — SEC-2: Accumulo-style row visibility labels (2026-08-09)

- A `_visibility` tag holding a label expression — `admin`, `ops&audit`,
  `(ops&audit)|admin`, quoted tokens — restricts each row to sessions
  whose authorizations satisfy it. Labels are ordinary dictionary-encoded
  tags: no write-path changes, FR-2 economics.
- The SEC-2 hook is real: `mandatory_predicate(session, table, schema) →
  Option<Restriction>`, called unconditionally inside `LazyTable::scan`
  and applied to every batch (buffer and file) below user predicates and
  before aggregation — `COUNT(*)` reads the label column even when the
  query doesn't, so an aggregate cannot count a hidden row.
- Semantics: unlabeled/NULL rows are public; malformed expressions are
  visible to no one (fail closed); `&`/`|` may not mix without
  parentheses (Accumulo's rule); expressions are evaluated once per
  distinct dictionary value, not per row.
- Authorizations arrive via `X-TimeLake-Authorizations` (HTTP header,
  comma-separated, or the `/api/sql` body field) and
  `x-timelake-authorizations` gRPC metadata on Flight SQL, captured into
  the flight ticket at planning time. They are **claims, not
  credentials**, until token auth lands — SECURITY.md exposure 7.
- `timelake_visibility_rows_filtered_total` counter: enforcement is
  visible, not silent.

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
- Regression tests: three in `timelake-buffer`, plus
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

- New `timelake-tls` crate: validate-before-swap certificate loading (PEM
  structure, leaf expiry, key↔certificate match) behind an `ArcSwap`
  resolver that is consulted only during a handshake.
- TLS on both listeners when `TIMELAKE_TLS_CERT` and `TIMELAKE_TLS_KEY` are
  set — HTTP via `axum-server`, Flight SQL via a `tokio-rustls` accept loop.
  Plaintext remains the default. `TIMELAKE_TLS_MIN=1.2` lowers the floor.
- Rotation triggers: a 2 s file-modification watcher and
  `POST /admin/tls/reload`.
- A rejected renewal keeps the last-good pair serving and raises the named
  alarm `SEC3_CERT_RENEWAL_FAILED`.
- Metrics: `timelake_tls_cert_expiry_seconds`, `timelake_tls_last_reload_ok`.
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
- Entity-clustered compaction, grace-period GC (`TIMELAKE_GC_GRACE_SECS`),
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
- `timelakedb` backend adapter for the tsdb-bench harness.

### Added — the evidence base (2026-08-08)

- `REQUIREMENTS.md` and `ARCHITECTURE.md` derived from the five-engine
  evaluation, with the tsdb-bench harness, benchmark record and Grafana
  fixtures vendored so the repository is self-contained.
