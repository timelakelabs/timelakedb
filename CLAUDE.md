# TimeLakeDB

A new time-series database, specified from evidence: five engines ran the
identical high-cardinality workload under `bench/` (tsdb-bench), and
their measured successes and failures define what this one must do.

## Inspired
This project is inspired by the following projects.

| Project | Branch | Query Languages | Documentation |
|---------|--------|-----------------|---------------|
| InfluxDB v3 | [`main`](https://github.com/influxdata/influxdb/tree/main) | SQL, InfluxQL | [docs.influxdata.com/influxdb3/core/](https://docs.influxdata.com/influxdb3/core/) |
| InfluxDB v2 | [`main-2.x`](https://github.com/influxdata/influxdb/tree/main-2.x) | Flux, InfluxQL | [docs.influxdata.com/influxdb/v2/](https://docs.influxdata.com/influxdb/v2/) |
| InfluxDB v1 | [`master-1.x`](https://github.com/influxdata/influxdb/tree/master-1.x) | InfluxQL, Flux | [docs.influxdata.com/influxdb/v1/](https://docs.influxdata.com/influxdb/v1/) |
| InfluxDB Cluster | [master](https://github.com/chengshiwen/influxdb-cluster/tree/master) | Flux, InfluxQL | [docs.influxdata.com/influxdb/v2/](https://docs.influxdata.com/influxdb/v2/) |
| Questdb | [master](https://github.com/questdb/questdb/tree/master) | SQL, SIMD (AVX2) | [questdb.com/docs/](https://questdb.com/docs/) |
| VictoriaMetrics | [master](https://github.com/VictoriaMetrics/VictoriaMetrics/tree/master) | MetricsQL, a PromQL‑compatible | [docs.victoriametrics.com/victoriametrics/](https://docs.victoriametrics.com/victoriametrics/) |

## Read first

- `REQUIREMENTS.md` — the requirements document. Every FR/PR/RR/SR traces
  to a measured benchmark result; the anti-requirements list is the four
  ways real engines failed.
- `ARCHITECTURE.md` — how the requirements become components: crate
  workspace, write/read paths, manifest-log catalog, compaction levels,
  memory budgets, the SEC seams, clustering evolution, and the M0–M5
  milestones (each gated by a tsdb-bench run).
- `docs/CONSOLE.md` — the operator plane design (SR-5/SR-6/SEC-4,
  ARCHITECTURE §17): layered configuration with provenance, admin auth
  and roles, the hash-chained audit trail, logs, metrics views, cluster
  view. Phased U0–U3 in §14.
- `docs/evidence/BENCHMARK_RESULTS.md` — the evidence: InfluxDB 1.8 (OOM-killed by a
  query), 2.7 (funnel never completed, 12× ingest decay), 3 Core (passed
  everything — the bar to beat), plus prior QuestDB/VictoriaMetrics OOMs.
- `docs/evidence/EVALUATION_PLAN.md` — the original workload definition and pass
  criteria the benchmarks implement.

## Decided

- **Technology: Rust + Apache DataFusion / Arrow / Parquet / `object_store`**
  (REQUIREMENTS.md §11). Libraries, not a fork of InfluxDB 3.
- Clustering is phased-in, not out-of-scope: v1 single-node but
  cluster-ready (CL-1); replication and query HA are v2 MUSTs (§7).
- Retention is per-table (FR-7). Encryption (SEC-1) and Accumulo-style
  row visibility labels (SEC-2) are v1 *design constraints* — one narrow
  object-I/O layer, one mandatory-predicate injection point.
- **Configuration is layered, not owned by one place** (§17, designed
  2026-08-09): `EngineConfig::default()` < `TIMELAKE_*` system property <
  stored override, with every layer visible, a revert to the layer
  beneath, and divergence from the property reported loudly (RR-5).
  Overrides are three-state — absent (inherit), a value, or explicit-none
  (off regardless of the property) — because "revert to the property" and
  "keep everything anyway" are different intents. `TIMELAKE_CONFIG_PINNED`
  locks named keys to the property layer. The console gets its own
  listener (1965, private by default) with SEC-4 auth; the admin
  endpoints move off 1963.
- TLS 1.3 (rustls) on every listener in v1, mTLS intra-cluster in v2
  (SEC-3 — "TLS 3.0" in conversation means TLS 1.3). Certs are short-TTL
  (~24 h) and hot-rotated: file-watch + ArcSwap resolver, validate-before-
  swap, last-good on bad renewal, established connections never dropped
  (AT-7 drills this under load). Discovery is a trait: static backend v1,
  Consul v2 (CL-5); discovery may never carry correctness — that stays in
  catalog CAS.

## Build & verify

- **This machine has Docker but no Rust toolchain** (no MSVC either) —
  build and test via Docker:
  `docker compose -f bench/compose/timelakedb.yml up -d --build`
  then `curl http://localhost:1963/health`.
- CI (`.github/workflows/ci.yml`) runs fmt + clippy -D warnings + tests.
- The website is `site/` — hand-written HTML/CSS/SVG, no build step, all
  paths relative so it works at a project-page subpath or a domain root.
  `.github/workflows/pages.yml` publishes it (needs Settings → Pages →
  Source: GitHub Actions, once). Brand palette: navy 0B1320, blue
  2563EB, gold D4AF37, mist E6E8EC, gray 6B7280 — the canonical copy of
  each value lives in `site/assets/style.css` as CSS custom properties,
  and `site/assets/logo.svg` is the mark. (The original brand sheet is a
  local design asset, deliberately untracked.) Site claims must trace to
  `bench/results/` — no marketing numbers.
- With a local toolchain: `cargo test --workspace`,
  `cargo run -p timelake-server` (listens on TIMELAKE_ADDR,
  default 0.0.0.0:1963).
- Reference docs: `site/docs/reference.html` (line protocol, SQL dialect,
  HTTP + Flight SQL surface, InfluxDB compatibility matrix, metrics,
  glossary) — verified against a running server, keep it true.
  Known gaps it records: no prepared statements or DoPut over Flight.
  (`CREATE`/`DROP TABLE` no longer silently return `[]` — the read-only
  SQL guard (P0-2) refuses all DDL/DML/COPY explicitly.) Schema
  discovery works both ways now —
  `with_information_schema(true)` plus a default catalog named after the
  db (so `poc.public.t` resolves and planner errors say `poc.public.…`),
  and Flight SQL answers GetCatalogs/GetDbSchemas/GetTables/
  GetTableTypes/GetSqlInfo (catalog = database, schema = `public`,
  type = `BASE TABLE`). Text is UTF-8 end to end; a non-UTF-8 body is
  400 `body is not utf-8` before the WAL — there is no byte escape in
  line protocol. Also fixed: a type-conflicting or duplicate-tag-key
  line used to leave `TableBuffer` columns ragged, breaking reads AND
  the node-wide maintenance tick, durably (WAL replayed it).
- Repo files: `LICENSE` (Apache-2.0), `SECURITY.md`, `CONTRIBUTING.md`,
  `CODE_OF_CONDUCT.md`, `CHANGELOG.md`. `ops/tldb-backup.sh` +
  `docs/BACKUP_RESTORE.md` make AT-5 runnable (helper-container tar, no
  host bind mount; `--recreate` for the destroyed-volume case; drops
  `*.tmp-write` on restore). **Auth: `/admin/*` sessions (SEC-4) + data
  plane by token (SEC-4 phased, default off) — see the status entry
  below.** SECURITY.md states the full posture. **P0-2 CLOSED**:
  `/api/sql` + Flight are read-only (plan-level guard
  `crates/query/src/sql_guard.rs`, drill
  `bench/results/sql-sandbox-drill.log`) and the container is non-root
  under a read-only rootfs — the COPY-writes-a-file-as-root exposure is
  shut twice over. Known engine hardening: manifest replay should skip
  non-`.json` files.
- **RENAMED TimelordDB → TimeLakeDB** (2026-08-09). Crates `timelake-*`,
  env `TIMELAKE_*`, metrics `timelake_*`, headers
  `X-TimeLake-Authorizations` / `x-timelake-csrf`, data dir
  `/var/lib/timelake/data`, adapter `bench/backends/timelakedb.py`
  (backend key `timelakedb`), compose `bench/compose/timelakedb*.yml`.
  Target GitHub org: `timelakedb` (free; `timelakelabs` parked).
  **Deliberately unchanged:** the `TLDE1` encryption magic (format
  marker, not brand — renaming it makes old objects fail the magic check
  and fall into plaintext passthrough = silent corruption), historical
  `bench/results/**` and `ops/logs/**` (records of runs that really
  happened), and `TLDB_*`/`tldb-*` identifiers (true of both names).
  Local repo directory is still `TimelordDB/` — rename it whenever.
- Status: **P0-4 SHIPPED — catalog commits are CAS (no multi-writer loss)**
  (2026-08-10, drill `bench/results/catalog-cas-drill.log`). Was: `commit`
  picked next seq from an in-process atomic + plain `put` → two writers on
  one bucket compute same seq, 2nd `put` clobbers 1st, its data files
  orphaned+invisible. Now `crates/catalog/src/lib.rs` `commit` is a CAS
  loop: propose seq=head+1 → `put_if_absent(catalog/manifest/{seq}.json)`
  → on loss, `catch_up()` (list+replay entries past head, fold into
  memory) → retry at new head. `commit_lock: Mutex<()>` serializes
  intra-process; CAS handles inter-process. Bounded 100 attempts →
  ResourceBusy. Metric `timelake_catalog_commit_conflicts_total` (0 single
  writer). Factored `read_entry`/`apply_entry` helpers. DRILLED on BOTH
  mechanisms (different code): local hard-link (3 catalog unit tests incl.
  two-writer-loses-nothing, loser-learns-winner, removals-survive-retry)
  AND real S3 If-None-Match via LocalStack (store-s3 `#[ignore]`
  `two_catalogs_on_one_bucket_lose_no_commits_p04`, dev-dep on
  timelake-catalog). DEFERRED to C2 (safe: maintenance single-node till
  role-split): re-validate commit on conflict so a compaction whose inputs
  were retention-dropped aborts vs resurrects (ARCHITECTURE §12.3). 124
  tests (+3) + 1 S3 drill, clippy/fmt clean. NEXT P0: **P0-5 Tributary
  presents token** (last P0). Roadmap `docs/ROADMAP.md`.
- Previous: **P0-2 SHIPPED — /api/sql read-only + non-root container**
  (2026-08-10, drill `bench/results/sql-sandbox-drill.log`). The
  COPY-writes-a-file-as-root exposure (SECURITY.md ex. 2+4, verified: one
  request wrote /tmp/pwned.parquet owned by root) is shut TWICE:
  (1) `crates/query/src/sql_guard.rs` `ensure_read_only(&LogicalPlan)`
  classifies the built plan before execution — SELECT/SHOW/DESCRIBE/
  EXPLAIN pass, COPY/DDL/DML/Statement refused, walks INTO Explain/Analyze
  (inputs() treats them as leaves) so COPY can't hide in EXPLAIN ANALYZE.
  Deny-by-default: exhaustive match on every LogicalPlan variant, NO
  wildcard, so a new DataFusion node breaks the build. Both surfaces route
  through `run_sql_env` which builds plan → guard → `execute_logical_plan`
  (one parse). (2) Dockerfile `USER timelake` uid 1000 + compose
  `read_only: true` + `tmpfs /tmp` + data volume the only writable mount.
  Incidental: CREATE/DROP TABLE now REFUSED explicitly (were silent `[]`).
  **UPGRADE GOTCHA**: non-root uid can't open a root-owned data volume from
  the old image → panic "open engine (recovery): Permission denied"; chown
  volume to 1000:1000 or use fresh. 121 tests (+4 sql_guard), clippy/fmt
  clean. NEXT P0: P0-4 catalog CAS (put_if_absent exists, catalog still
  plain put), P0-5 Tributary presents token. Roadmap `docs/ROADMAP.md`.
- Previous: **SEC-4 phased data-plane auth SHIPPED** (2026-08-10, drill
  `bench/results/data-auth-drill.log`). `TIMELAKE_DATA_AUTH=off|optional|
  required` (default off = header not read, today's compat contract).
  Token = 256-bit OS-CSPRNG secret, prefix `tldb_`, stored only as
  SHA-256 digest (NOT Argon2id — no brute-force surface, and a ~50ms KDF
  on a 100k-line/s write path breaks RR-1; reasoning in
  `crates/auth/src/token.rs` header). `crates/auth/src/guard.rs` =
  the ONE `decide()`; both HTTP and Flight route through
  `Engine::authenticate_data_impl` → `Auth::decide_data` → `guard::decide`
  so policy can't fork. **Mechanism fixed by measurement**
  (`bench/results/data-auth-client-probe.log`): Grafana Flight SQL
  forwards only the token field as `Bearer`; its basic-auth toggle +
  custom headers are HTTP-only, never reach gRPC. So: one token, three
  spellings — `Bearer` (Grafana/Tributary), `Token` (Telegraf v2),
  `Basic` w/ token as password (Telegraf v1). Scope read|write|read_write
  is NOT a total order (shipper writes without reading back); +database
  allowlist +SEC-2 grants that INTERSECT claimed auths (narrow only;
  `None` grants = no policy ≠ deny-all). Flight re-auths at DoGet AND
  planning (ticket is client-crafted). Console `/admin/tokens`
  issue/list/revoke (admin-only, secret shown ONCE, digest never listed),
  page section added. Metrics `timelake_data_auth_mode` +
  `timelake_data_requests_{authenticated,anonymous,rejected}_total` (flip
  optional→required on the split, not a guess; anon must be 0 in
  required). Tokens persist `catalog/config/tokens.json` via Store (so
  SEC-1 encrypted). DRILLED LIVE in container (in-proc tests can't reach
  Flight accept loop): required locks both doors, token works Bearer+
  Token on HTTP + Flight, write-only token refused Flight read (403),
  revoke immediate, metrics column-0 valid. 116 tests (auth 6→20, +7
  server data_auth), clippy/fmt clean. **NOT DONE: Tributary presenting
  the token (P0-5)**; console page not browser-drilled (API is).
  Roadmap: `docs/ROADMAP.md` (competitive) + `docs/PRODUCTION_READINESS.md`.
- Previous: **SEC-3 v2 client certificates SHIPPED — WANT mode** (2026-08-10,
  drill `bench/results/sec3-mtls-want-mode.log`). `TIMELAKE_TLS_CLIENT_CA`
  = PEM bundle → both listeners request a client cert, verify one if
  presented, serve either way. `crates/tls/src/client_auth.rs`:
  `RotatingClientCa` (ArcSwap<RootCertStore>, dual-CA overlap,
  validate-before-swap, last-good + alarm on a bad bundle, empty bundle
  REFUSED) + `WantClientAuth` (`client_auth_mandatory()=false`,
  `allow_unauthenticated()`). Identity = subject CN, extracted in the
  Flight accept loop (`IdentifiedStream` implements tonic `Connected`,
  `PeerIdentity` extension). **The identity earns something:**
  `QuerySession::resolve(granted)` INTERSECTS claimed SEC-2 auths with
  grants → authenticating can only NARROW, never widen; anonymous path
  behaves exactly as before, so it's additive, not a flag day. `None`
  grants = "no policy" = keep claims (NOT deny-all — else presenting a
  cert breaks a working client). WANT not REQUIRED is the explicit
  requirement: stock Grafana/Telegraf hold no cert (AT-6). Metrics:
  `timelake_tls_client_auth_mode`, `_client_ca_anchors` (reads 2 during
  overlap), `_client_ca_last_reload_ok`. `gen-certs.sh client [CN]`.
  **AT-7 still 19/19 with client auth on** — its first run was 18/19 and
  the DRILL was stale, not the server: it called /admin/tls/reload
  anonymously after SEC-4 guarded it; it now logs in + rotates
  admin/admin. NOT DONE: `/api/sql` carries no identity (axum-server owns
  that accept loop → needs a custom `Accept`); requiring mTLS is a C3
  decision for the intra-cluster listener. SECURITY.md exposure 3 now
  CLOSED, new exposure 9 states plainly that want mode grants nothing on
  its own. Windows curl (schannel) can't drive this — drill from a Linux
  container on the compose network.
- Previous: **SEC-4 admin auth SHIPPED** (2026-08-09, drill
  `bench/results/sec4-auth-drill.log`). New `timelake-auth` crate:
  roles viewer<operator<admin, Argon2id, sessions (cookie or bearer,
  30 min idle / 12 h absolute), CSRF double-submit + Origin on cookie
  mutations, per-principal login backoff; principals persist at
  `catalog/config/principals.json` via the Store (so encrypted). Every
  `/admin/*` route is guarded — closes SECURITY exposure 3a.
  **First run seeds admin/admin, QUARANTINED**: the only route that
  answers is POST /admin/password; everything else 403
  `password_change_required`. Rotation kills all that principal's
  sessions (including the one that rotated). Policy: ≥8 chars, not the
  username, not "admin". `TIMELAKE_ADMIN_BOOTSTRAP_PASSWORD` avoids the
  well-known default. This REVERSES the bootstrap-token design — cost
  and mitigations recorded in docs/CONSOLE.md §4.2, REQUIREMENTS SEC-4
  amended. Retention authz follows the data: grow=operator,
  shrink/introduce/remove=admin. Console `/admin/ui` is now
  sign-in → forced rotation → manage. Metrics:
  timelake_admin_default_credential_active (ALERT ON THIS),
  _logins_total, _login_failures_total. **Data plane deliberately still
  open** so Telegraf/Grafana/bench work — that migration is its own
  milestone. Tests: 6 auth unit + 19 server integration.
- Previous: **Retention GUI SHIPPED** (2026-08-09): FR-7 is runtime-managed —
  `GET/PUT /admin/retention`, `DELETE /admin/retention/{table}`, GUI at
  `/admin/ui` (self-contained HTML in `crates/api/src/admin_ui.html`,
  site palette). Policies persist at `catalog/config/retention.json`
  via the Store (encrypted; store copy outranks the `TIMELAKE_RETENTION`
  env seed at boot; plain put — CAS if concurrent admins ever matter).
  Engine: `retention: RwLock<…>` replaces cfg reads in
  `enforce_retention`. SECURITY exposure 3a: it's an unauthenticated
  deletion control. Test: retention_is_manageable_at_runtime_and
  _persists_fr7.
- Previous: **C0 SHIPPED — S3 + KMS, key-cached** (2026-08-09, ARCH §12
  design phased C0-C3; drill log `bench/results/c0-s3-drill.log`).
  New `timelake-store-s3` crate: `S3Store` (aws-sdk-s3 behind the Store
  trait; owned-runtime sync bridge — never `block_on` in callers;
  path-style auto under `AWS_ENDPOINT_URL`) and `AwsKms`
  (GenerateDataKey/Decrypt behind the Kms trait). `Store` gained
  `put_if_absent` (CAS primitive: S3 If-None-Match, local hard-link
  publish) — C1 switches catalog commits to it. `Kms` gained
  `generate()`; `CachingKms` decorator = caching-CMM (300s/1000-use
  encrypt window, decrypt LRU, hard cap 2^16; `TIMELAKE_KMS_CACHE=off`
  is the measured baseline). SSE-KMS + Bucket Keys per PUT and as
  bucket default. Env: `TIMELAKE_OBJECT_STORE=s3://…`,
  `TIMELAKE_KMS_KEY_ID` (alias ok; mutually exclusive with
  `TIMELAKE_ENCRYPTION_KEY`). Rig: `bench/compose/timelakedb-s3.yml`
  (LocalStack s3+kms, init hook creates alias/timelake + buckets;
  ports 3966/3967) — proves correctness/call-counts/recovery, NEVER
  latency. Ignored integration tests run in-network:
  `cargo test -p timelake-store-s3 -- --ignored`. Next: C1 catalog CAS
  (two-writer drill), C2 role split, C3 Consul+mTLS+real-AWS sizing.
- Previous: **SEC-1 + SEC-2 SHIPPED** (2026-08-09). SEC-1: `EncryptingStore`
  decorator in `timelake-store` — per-object AES-256-GCM envelope
  encryption in 64 KiB authenticated chunks (range-read compatible; AAD =
  header + path), `Kms` trait with `LocalKek` from
  `TIMELAKE_ENCRYPTION_KEY`/`_KEY_FILE` (64 hex chars; malformed key
  refuses to start). Chose envelope-at-chokepoint over Parquet Modular
  Encryption (covers manifests, no arrow-rs PME dependency — ARCH §16
  risk 2 retired); engine holds `Arc<dyn Store>`, wrap decided in
  `Engine::open` only. Plaintext objects remain readable (migration);
  local WAL not covered. SEC-2: `_visibility` tag with Accumulo-style
  expressions (`(ops&audit)|admin`; no `&`/`|` mixing without parens;
  malformed = visible to no one; unlabeled = public), enforced in
  `LazyTable::scan` via `mandatory_predicate(session, table, schema) →
  Option<Restriction>` — applied to every batch below user predicates,
  COUNT(*) reads the label column so aggregates can't leak. Auths:
  `X-TimeLake-Authorizations` header / body field / Flight gRPC metadata
  (captured into the ticket) — CLAIMS until token auth exists.  Metrics:
  `timelake_encryption_enabled`, `timelake_visibility_rows_filtered_total`.
  Clippy/rustfmt 1.97 drift fixed workspace-wide (byte_char_slices,
  collapsible_if, is_multiple_of). Next: token auth (turns SEC-2 claims
  into authorization), in-network bench re-baseline, CI on a remote.
- Previous: **SEC-3 SHIPPED — AT-7 drill 19/19** (see
  `bench/results/at7-drill.log`). New `timelake-tls` crate:
  validate-before-swap cert loading (PEM, expiry via x509-parser,
  key↔cert match via `CertifiedKey::from_der`), `ArcSwap` resolver
  consulted only at handshake, last-good + named
  `SEC3_CERT_RENEWAL_FAILED` alarm on a bad renewal. Both listeners TLS
  when `TIMELAKE_TLS_CERT`/`_KEY` set (HTTP via axum-server, Flight via
  tokio-rustls accept loop into `serve_with_incoming`); plaintext stays
  the default (bench/fixtures unchanged). Triggers: 2 s mtime watcher
  (works through a Windows bind mount) + POST /admin/tls/reload.
  Gauges: timelake_tls_cert_expiry_seconds, timelake_tls_last_reload_ok.
  Drill stack: `compose/timelakedb-tls.yml` (ports 2963/2964) +
  `tls-drill/` (gen-certs.sh, at7_drill.py). Gotcha: rust:1-slim moved
  to trixie — runtime stage must be trixie-slim or the binary dies with
  `GLIBC_2.38 not found`. Post-TLS smoke gate green (0 errors, Shape A
  49 ms median). Next: streaming/range reads for the ingest-contention
  carve-out; CI on a remote.
- Previous: **M5 — AT-4/5/6 complete.** Metadata cache (immutable
  footers) → warm Shape A 0–6 ms, cold ~300 ms. Backup 34 s / restore
  13 s exact; SIGKILL mid-ingest recovery 4.7 s, zero loss (40,340,794
  exact); 10× 100K bursts clean; repeat run within tolerance; stock
  Telegraf writing (compose profile "telegraf"). Backup note: live
  snapshots are safe by objects-before-manifest ordering; quiesce only
  eliminates a tiny manifest-tear window.
- Previous: **M4 — AT-3 green with two carve-outs** (run
  tldb-m4-final + tldb-m4-settled2 + idb3-exactness in bench/results/).
  Fresh full-scale: Shape A median 211 ms, all Shape B complete (B1
  1.7 s, B4 0.68 s), burst 0.12 s, storage 0.50 GB/day, zero row loss
  (fixed-bound equality vs influxdb3 on identical data — the old "8-row
  deficit" was now()-window aging, not dedup). Carve-outs for M5:
  (a) Shape A p95 608 ms vs 250 target, (b) intra-run ingest decline
  under maintenance contention (no cross-run decay) — both point at
  streaming exec + range reads + maintenance/query isolation. Hard-won
  operational lessons in the M4 commit messages: async scans must not
  block, bind mounts cost ~3 s/query, batch-size vs shared dictionaries
  distorts memory accounting, mem_limit is non-negotiable.
- Previous: **M4 first pass (superseded, kept for the record):** Landed & unit-green (28 tests):
  shared FairSpillPool + admission semaphore + server-side timeout
  (QueryEnv), pruning LazyTable provider (time-bound file skip, bloom
  row-group skip, projection pushdown, COUNT(*) empty projection),
  bloom write props on dict columns, GC grace (fixes a real
  compaction-vs-query race AT-3 exposed), schema registry. Full-scale
  ingest reproducible: 517-660K lines/s, 0 errors (3 runs).
  **Gate blockers, in order:**
  1. `LazyTable::scan` does BLOCKING store.get + parquet decode on the
     async runtime → tokio timeout cannot preempt it → a slow scan hangs
     forever (journey_14 hung >300 s, wedged the whole Docker VM). Fix:
     spawn_blocking or yielding chunked loads. THIS FIRST.
  2. Compose service has NO memory limit — a runaway container took down
     the Docker VM (all host containers!). Add mem_limit ~6g to
     bench/compose/timelakedb.yml before any further full-scale run.
  3. Shape A ~4 s/lookup at full scale: bloom row-group pruning
     apparently ineffective — VERIFY blooms are actually written for
     Dictionary columns (SerializedFileReader on a real file); arrow
     writer may skip blooms for dict columns → may need write-path
     change (e.g., bloom on values) or a per-file pid min/max index.
  4. Earlier fix, needs re-verification at full scale: dictionary
     double-count removed (batch_size 1M, DataSourceExec accounting).
  5. Still pending: Shape B full-scale pass, exactness cross-check vs
     influxdb3 (8-row/2 ppm LWW delta), then AT-3 scorecard + commit.
  Run logs: bench/results/tldb-m4-full{,2,3}.log. Gate via /tldb-gate.
  Also: (3 ppm) `docker kill` suppresses restart policies — drills use
  `docker restart -t 0`.

## Ground rules for work in this directory

- The acceptance test is `bench/` — do not invent a new harness.
  A `timelakedb` backend adapter + compose target makes any prototype
  measurable with `python bench.py run --backend timelakedb` and
  comparable via `bench.py compare` against the recorded baselines in
  `bench/results/`.
- The hard invariant is RR-1: no query may kill the server. Designs that
  can't uphold it are out, regardless of speed.
- High-cardinality tags must cost what a compressed column costs (FR-2).
  Anything whose memory or write cost grows with distinct-tag-combination
  count repeats the failure this project exists to avoid.
- Keep query semantics identical to the canonical five Shape B queries and
  Shape A in `bench/backends/influxdb3.py` — those are the
  reference meanings, validated by matching row counts across engines.
- Telegraf (unmodified `influxdb`/`influxdb_v2` output plugins) and Grafana
  (stock datasource over Flight SQL) are first-class integrations — FR-8 /
  FR-9 / AT-6. The provisioned dashboards in `fixtures/grafana/`
  are the Grafana compatibility fixture; don't fork them.
