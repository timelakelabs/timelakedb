# TimeLakeDB

A new time-series database, specified from evidence: five engines ran the
identical high-cardinality workload under tsdb-bench, and their measured
successes and failures define what this one must do. That harness now
lives in its own repository, `../Gauge/` (moved 2026-08-11).

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
- `../Gauge/docs/BENCHMARK_RESULTS.md` — the evidence: InfluxDB 1.8 (OOM-killed by a
  query), 2.7 (funnel never completed, 12× ingest decay), 3 Core (passed
  everything — the bar to beat), plus prior QuestDB/VictoriaMetrics OOMs.
- `../Gauge/docs/EVALUATION_PLAN.md` — the original workload definition and pass
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
  `docker compose -f deploy/compose/timelakedb.yml up -d --build`
  then `curl http://localhost:1963/health`.
- CI (`.github/workflows/ci.yml`) runs fmt + clippy -D warnings + tests.
- The website is `site/` — hand-written HTML/CSS/SVG, no build step, all
  paths relative so it works at a project-page subpath or a domain root.
  `.github/workflows/pages.yml` publishes it (needs Settings → Pages →
  Source: GitHub Actions, once). **Brand palette (mountain-lake identity,
  corrected 2026-08-20): navy `0B1220`, navy-soft `0F1E33`, navy-line
  `1C2E45`, blue `3B82F6`, sky `7DD3FC`, TEAL `14B8A6` (the accent), mist
  `E6EDF5`, gray `6B7280`, ink `16202F`.** The canonical copy of each
  value lives in `site/assets/style.css` as CSS custom properties;
  `site/assets/logo.svg` is the circular mark, `avatar.svg` the square one
  (opaque navy corners, for platforms that composite onto their own
  ground). ~~gold D4AF37~~ — **teal replaced gold in the rebrand**, and
  this note recorded the pre-rebrand values until 2026-08-20: four of its
  five were stale, so anything built from it came out gold-accented. The
  admin console was one of those things and shipped gold until 2026-08-21;
  `crates/api/tests/console_palette.rs` pins it now. Note what gold was
  doing there, because it is the general case: it was the brand accent in
  one rule and a warning stripe in another, so it had no single
  replacement. Brand colours and semantic ones (`--red`, `--amber`,
  `--green`) are separate, and only the first kind moves in a rebrand.
  Wordmark is two parts: `TimeLake` white/ink + `DB` in teal. (The
  original brand sheet is a local design asset, deliberately untracked.)
  Site claims must trace to `docs/evidence/` — no marketing numbers.
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
  `docs/evidence/sql-sandbox-drill.log`) and the container is non-root
  under a read-only rootfs — the COPY-writes-a-file-as-root exposure is
  shut twice over. Known engine hardening: manifest replay should skip
  non-`.json` files.
- **RENAMED TimelordDB → TimeLakeDB** (2026-08-09). Crates `timelake-*`,
  env `TIMELAKE_*`, metrics `timelake_*`, headers
  `X-TimeLake-Authorizations` / `x-timelake-csrf`, data dir
  `/var/lib/timelake/data`, adapter `../Gauge/bench/backends/timelakedb.py`
  (backend key `timelakedb`), compose `deploy/compose/timelakedb*.yml`.
  GitHub org is **`timelakelabs`**, repos LOWERCASE under it:
  `timelakelabs/timelakedb`, `timelakelabs/tributary` (settled 2026-08-11 —
  the `timelakedb` org also exists but is not the one in use; SSH alias
  `github.com-timelakelabs`, key `id_ed25519_timelakelabs`).
  **Deliberately unchanged:** the `TLDE1` encryption magic (format
  marker, not brand — renaming it makes old objects fail the magic check
  and fall into plaintext passthrough = silent corruption), historical
  `docs/evidence/**` and `ops/logs/**` (records of runs that really
  happened), and `TLDB_*`/`tldb-*` identifiers (true of both names).
  Local repo directory renamed `TimelordDB/` → `TimeLakeDB/` 2026-08-22
  (sibling repos' path defaults updated the same day; `gauge/bench/
  results/timelorddb-*` and `docs/evidence/**` keep the old name, being
  records of runs that really happened).
- Status: **Pushed & private; P0-1 CLOSED; CI green-then-billing-blocked
  (2026-08-16).** All five repos pushed, CI recorded green on real runners
  (`docs/evidence/P0-1-ci.md`). Since then: **SEC-5** query-error
  sanitization (= P1-4), **SEC-6** per-client rate cap (= P1-3), **SEC-8**
  WAL encryption (= P1-5) all merged, each with a Riverkeeper control
  (R6, coverage 10/11); **R-1 targeted delete** shipped — `POST /admin/delete`
  records a manifest-log tombstone hidden in-scan (R-1a: buffer + files,
  aggregates, cluster-wide) and physically reclaimed by a maintenance pass
  (R-1b), with a Riverkeeper control (R7, `targeted-delete-hides-rows`);
  **P1-2 audit trail** shipped — new `timelake-audit` crate (fsync'd,
  SHA-256-chained, tamper-evident); every admin mutation writes one
  attributable record with resolved before/after; **fail-closed** (503 while
  the sink is broken, `TIMELAKE_AUDIT_FAIL_OPEN` escape); `GET /admin/audit`
  (viewer) with `?verify=1`; `timelake_audit_*` metrics (SR-6; data-plane +
  login/logout auditing deferred);
  intra-cluster port de-published (exposure 10);
  **release packaging** shipped — `packaging/` builds a `.deb` + `.rpm` from
  one nfpm spec, attached to `v*` tags by `.github/workflows/release.yml`;
  built against glibc 2.31 (Debian 11 container) because a current-image
  build needs GLIBC_2.39 and would not start on RHEL 9 / Debian 12 / Ubuntu
  22.04; `packaging/verify.sh` installs + runs them on Debian 12, Ubuntu
  22.04, Rocky 9 and AL2023; packages bind loopback and do NOT auto-start;
  deps refreshed within-semver (PR #4 MERGED; `thrift` still blocked upstream
  until datafusion 55 — a known, unreachable advisory, and the one open
  Dependabot alert). Site adopted the new mountain-lake brand (teal accent;
  `site/assets/logo.svg` + `avatar.svg` + repointed tokens), and the site's
  Security/Config/Endpoint pages were reconciled 2026-08-17 — they had still
  been claiming audit logging, mTLS, rate limiting and WAL encryption were
  unimplemented, and both quickstarts began with a `cd` into the deleted
  `bench/`. **Two live blockers:** GitHub Actions hit the **spending limit**
  (jobs won't start — a PR's checks show red with `steps=0`, which is the
  block, not a code failure); mitigation is 2 self-hosted WSL runners
  (`../ops/WSL_RUNNERS.md`, scoped not applied); and the **Phase 2 public
  flip** (public repos, Pages, `v0.1.0-alpha`) is paywalled. Still open: C2
  phase 5 compactor — **P1-6** (Tributary L4 client certs), **P1-7** (queue
  RPO, measured) and **T-1** (Tributary `/metrics` + `/healthz`, 2026-08-18)
  have all shipped, which **closes the v0.2-pilot gate**. What is left is
  the Phase 2 public flip and the CI billing block, neither of which is
  engineering. **U2 metrics + self-monitoring console shipped 2026-08-18**
  (docs/CONSOLE.md §7.4/§7.6): the exposition had NO histograms and timed
  NO queries, so the Query view could not be drawn and "Shape A got slow"
  was answerable only by running Gauge. Added
  `timelake_query_duration_seconds` + `_admission_wait_seconds`
  (histograms), `_in_flight`/`_queued` (RAII-guarded gauges),
  `timelake_queries_total`/`_timeouts_`/`_refused_`/`_failed_` (refused is
  counted APART from failed — refusing a COPY is P0-2 working), uptime,
  build_info, flush/compaction lag, gc_pending, `timelake_files{level}`,
  `timelake_storage_{bytes,rows}{db,table}`,
  `timelake_write_rejected_total{reason}`. Instrumented in `run_sql_env` —
  the ONE production call site, so HTTP and Flight cannot drift.
  **Self-monitoring** (`crates/server/src/selfmon.rs`): the node samples
  its own `/metrics` text into `_system.metrics` every maintenance tick
  and writes one row per query into `_system.queries`, read back by
  Grafana over Flight SQL (`deploy/grafana/`,
  `deploy/compose/timelakedb-console.yml`, port 3004). It CONVERTS the
  exposition rather than keeping a second metric list, so the U2 gate
  (stored numbers agree with `/metrics`) holds by construction and new
  metrics are self-monitored automatically. Bounded queue, drops counted,
  never blocks a query; `_system` rows excluded from
  `timelake_lines_written_total` so Gauge baselines stay comparable.
  **`/metrics` deliberately unchanged and is still the alerting surface** —
  it answers from atomics with no query path, i.e. it works when the
  stored copy cannot be read. KNOWN LIMITS, documented not hidden: a CL-3
  querier stores nothing (no write path, no maintenance — `/metrics` only;
  shipping its samples to an ingester is C2 work); `_system` gets NO
  default retention — ~~because `enforce_retention` matches table name
  ignoring the database~~, **that db-scoping bug was fixed 2026-08-19**
  (see the retention entry above), so bounding `_system` is now safe and
  `docs/CONSOLE.md` §7.6 gives the two calls. It is still not done for
  you: a deletion policy should be an operator's decision, not a side
  effect of enabling telemetry. Full
  milestone reconciliation: `../PROJECT_PLAN.md` §0.
- **C4 re-run 2026-08-22: rebalance-duplicates finding CLOSED, and a new
  agent bug found+fixed in the process.** Seven runs of Catchment's
  `router-tributary-exactness` against the overlap-compaction fix;
  closing run `...-20260822-160108` PASS 17/17. The duplicates still
  happen and now collapse at the next compaction pass — observed
  202,000 → 200,000 exact (`transient_rows_collapsed: 2000`), cross-node
  via the shared catalog (`compact_once` groups have NO node dimension;
  the CAS loop teaches nodes about peers' files). **New finding, fixed
  same day: `FINDING_agent_pools_a_reused_ip.md`** — the reshape hands
  the router's old IP to a recreated querier, the agent's keep-alive pool
  follows it (DNS is only consulted on dial), and 501 was filed under
  retryable transport → four agents retried the wrong node at ~5 req/s,
  silently, forever; caught by strace after /proc lied (WSL2 hides
  wchan). Tributary now rebuilds its client on 501 (WrongNode) + every
  3rd consecutive transport failure, `tributary_transport_rebuilds_total`
  makes it visible; unit test red-proofed (neutered fix → conns:1). Also
  four C4 harness defects fixed, each of which had read as a plausible
  product failure: thread-death KeyError eating the hang diagnosis,
  exactness asserted before the compaction cadence (the fix's transient
  read as a FAIL), the fault gate firing on the rig's own probe row
  (phase A ran against a healthy cluster), and the drain barrier
  demanding simultaneous zeros from three selfmon-oscillating buffer
  gauges (healthy flushing read as a flush freeze — U2 selfmon shipped
  AFTER the barrier was written and silently turned it into a coin
  flip). REBALANCE.md's "twins only meet on the same node" line was
  wrong and is corrected.
- **Grafana ALERTING verified end to end, 2026-08-21** — `docs/ALERTING.md`,
  rig `deploy/compose/timelakedb-alerting.yml` (port 3005), drill
  `deploy/compose/alert-drill/alert_drill.sh`, evidence
  `docs/evidence/grafana-alerting-drill.log`. FR-9 covered dashboards;
  nothing had ever exercised alerting, which adds a stage a dashboard never
  touches — the datasource frame has to survive a `reduce`. **Finding, a
  usage trap rather than an engine bug: `reduce: last` takes the
  positionally last row of the frame. It does not sort and does not know
  which column is time**, so a rule ending `ORDER BY time DESC` — the
  natural phrasing — thresholds the OLDEST row in the window. Measured:
  window holding 10s then 100s
  against `gt 50`, rule sat in Normal reporting **`health: ok`**
  indefinitely; flipping that one word to `ASC` made it fire on the next
  tick. Nothing surfaces it — a panel on the same SQL renders fine because
  a panel never reduces, and rule history is an unbroken green line either
  way. Also: the query model field is **`rawSql`** (`query`/`expr` reach the
  server as an empty statement → "No SQL statements were provided", which
  reads as an engine fault). The provisioned group carries a
  deliberately-DESC rule asserted NOT to fire while the real one does, so a
  green drill separates "alerting works" from "nothing can fire"; the drill
  was mutation-verified (ASC→DESC ⇒ fails at phase E, exit 1, and F/G report
  SKIP not PASS). It checks DELIVERY to a webhook recorder, not just rule
  state, and REFUSES a dirty table — `/api/sql` is read-only and there is no
  delete-database route, so it cannot clean up after itself and stale rows
  silently invert the discriminator. `/metrics` + Alertmanager stays the
  surface for NODE health: it answers from atomics and works when the query
  path is what is broken.
- Previous: **P0-1 PREPARED — CI verified on a cold runner, push is yours**
  (2026-08-10). NOT closed: the push needs GitHub credentials this box does
  not hold (no `gh`, no remotes). What IS done: every CI step run against a
  CLEAN target dir in stock `rust:1-slim` (fmt/clippy/173 tests/coverage all
  pass), plus the LocalStack-gated `store-s3` tests (3 pass) with the exact
  env the new job sets. **Org decided: `timelakedb`** (was `TimeLakeLabs` in
  Cargo.toml/README/SECURITY/4 site pages; CLAUDE.md was the correct side) —
  URLs rewritten in BOTH repos. New `store-s3` CI job (LocalStack service
  container + bucket/KMS alias creation) so the coverage exclusion means
  "counted in another job". **Tributary had NO CI at all** — now has
  fmt/clippy/test (51 tests pass) + a LICENSE file (it declared Apache-2.0
  with no text) + a README that no longer says "nothing is built yet".
  `pages.yml` now skips unless the repo is public (Pages is unavailable on
  a private Free repo → would have been a permanently red badge).
  Coverage was 80.84% against an 80% gate after CL-3 (was 82.44%) — router
  forwarding integration tests (`crates/server/tests/router.rs`, 9) brought
  it to **82.74%** rather than lowering the gate. TimeLakeDB README status
  section rewritten (it still claimed "data plane has no authentication",
  false since SEC-4 phased). **NOTE: someone else added `conformance` +
  `publish` jobs to both workflows** referencing a `Catchment` repo, a
  GitHub App (`vars.CI_APP_ID` / `secrets.CI_APP_KEY`) and a ghcr image —
  those are NOT verified by me and have blockers: Catchment has ZERO
  commits locally, its `CI.md` (referenced in a workflow comment) does not
  exist, and Tributary's conformance pulls `ghcr.io/<owner>/timelakedb:main`
  which only exists after TimeLakeDB's `publish` job runs — so push order
  is Catchment → App+secrets → TimeLakeDB → grant package read → Tributary.
- Previous: **flush handover made atomic** (2026-08-10, drill
  `docs/evidence/flush-handover-atomicity.log`). A reader could see rows in
  NEITHER the buffer nor the holding area mid-flush — a vanish, reachable by
  a plain COUNT(*), microseconds on disk but the length of a `put` on S3.
  Swap + holding-insert are now ONE critical section (`ingest_gate` → `dbs`
  → `flushing`) and all three read sites (`sql_batches`, `live_tables`,
  `snapshot_ipc`) hold both locks together in that order. Recorded wrong
  turn: locking only `flushing` across the swap converts the vanish into a
  DUPLICATE (4 failures in 6). COST, measured: snapshot now runs under the
  gate → laptop ingest ~600k → ~571k lines/s (**≈5%**). Cheaper design
  deliberately deferred: move the `TableBuffer` into holding instead of a
  snapshot (pointer move; readers snapshot as they already do) — changes the
  type the read path and CL-3 wire consume.
- Previous: **C2 phase 4 SHIPPED — the CL-3 stateless querier** (2026-08-10,
  drill `docs/evidence/cl3-querier-drill.log` 19/19, NEW rig
  `deploy/compose/timelakedb-cluster-s3.yml` = router + ingester×2 +
  querier×2 + localstack on ONE bucket; drill
  `deploy/compose/cluster-drill/cl3_drill.sh`). `TIMELAKE_ROLE=querier`:
  owns no data, refuses writes (501), runs NO maintenance, replays the
  catalog from the shared store and tails the manifest log (1 s).
  **Freshness**: ingesters serve live buffers as Arrow IPC on the internal
  listener — `GET /internal/v1/{live,snapshot}` (`crates/query/src/ipc.rs`;
  IPC not JSON/LP so dict tag columns stay dict-encoded, FR-2) — and
  `sql_batches` unions remote snapshots + files. **The watermark is the
  load-bearing bit**: every internal response carries
  `x-timelake-catalog-head` read AFTER the buffers, and the querier folds
  the manifest log to the max head seen BEFORE reading any file list, so a
  batch that left a buffer is guaranteed visible → duplicate possible,
  vanish impossible. sql_batches is now TWO passes (all live rows, then
  watermark, then all file lists) for that reason. **Refuses rather than
  under-counts**: an unreachable ingester → named error +
  `timelake_querier_refusals_total` (ALERT), the deliberate opposite of
  PR-7 on the write path. Router now forwards `/api/sql` round-robin with
  FALL-THROUGH on transport failure (no queriers → still 501; Flight not
  forwarded). role=all/ingester/router unchanged (remote=None → no CL-3
  metrics, pinned). 164 tests (+23), clippy/fmt clean.
  **TWO BUGS THE DRILL CAUGHT, unit tests could not**: (1) a brand-new
  table was "not found" for up to 1 s — the live view was tick-refreshed,
  so it is now refreshed ON THE QUERY PATH before listing; (2) a restarted
  peer left dead pooled sockets → spurious refusal, so snapshot reads
  retry once. **Also fixed (pre-existing)**: the schema registry never
  refreshed after boot, so a column added later read as absent on any
  non-writing node — now rebuilt on every catalog advance, via a
  footer-only read (`provider::file_schema`) instead of GETting the whole
  object. CLUSTER SMOKE: the UNMODIFIED bench drives the whole cluster
  through the router (`--url http://localhost:5970`) — 323K lines/s, Shape
  A 58 ms, all Shape B complete, **rows_48h 77,806 EXACT = the single-node
  value**, so FR-8/FR-9 survive the role split (run
  `timelakedb-cl3-cluster-through-router-20260810-203730`). Regression:
  CL-2 drill 12/12, router drill 8/8, role=all smoke 296,885 lines/s /
  Shape A 49 ms / rows_48h 77,806 exact.
  KNOWN COST (C3): a query registers providers for EVERY table in the db,
  so it fans out snapshots for tables it will not read.
  **C2 phasing:** (1) roles+discovery ✓ → (2) CL-2 ✓ → (3) router ✓ →
  (4) CL-3 querier ✓ → (5a) compactor role ✓ BUILT, GATE SHUT →
  (5b) work-avoidance on top of the commit fence (LAST). The role has a
  branch, a maintenance loop, an HTTP surface and a topology; what it does
  not have is permission — `Role::implemented` still refuses `compactor`.
  That is deliberate and the reason CHANGED rather than expired: the
  commit fence (`Catalog::commit_replace`) already makes concurrent
  compaction CORRECT, so the gate is now about waste, not safety. Two
  compactors racing every partition do double the IO to land half the
  merges. Flipping it is a decision with its own issue.
  Design: ARCHITECTURE §12.4.
  PRE-EXISTING FLAKE, not from this work: `health.rs`
  `rows_stay_visible_while_a_slow_flush_uploads` fails when run ALONE via
  a test filter (passes in the full binary and the full workspace, and
  fails the same way on the previous commit) — it can catch the
  documented sub-millisecond swap→`flushing` gap in `flush_all`.
- Previous: **C2 phase 3 SHIPPED — the router (write sharding)** (2026-08-10,
  drill `docs/evidence/router-sharding-drill.log`). `TIMELAKE_ROLE=router`:
  stateless, holds NO data, opens NO engine (main.rs branches before engine
  creation, `return`s). Hashes each line's `(db,measurement)` →
  `crates/server/src/router.rs` `shard_of` (FNV-1a over `db\0measurement`
  mod N, deterministic; ingesters sorted by id) → forwards that shard to the
  ingester's public write port; that ingester becomes the table's primary +
  replicates to its CL-2 peer (durability unchanged). ATOMICITY held: whole
  body validated before ANY forward (poison line → 400, writes zero); infra
  failure of a shard → returned for idempotent retry (LWW). QUERIES NOT
  routed → `/api/sql` returns 501 (needs querier to union shards, phase 4);
  query an ingester directly for now. `NodeInfo.data_address` added (public
  write port), carried in `TIMELAKE_PEERS` as `id=role@cluster_addr|data_addr`.
  Metrics `timelake_router_{forwarded,forward_errors,rejected,ingesters}`.
  role=all/ingester UNCHANGED (separate main branch). 141 tests (+5 router
  +1 cluster), clippy/fmt clean; live 8/8 (12 tables sharded 6/6 across the
  pair, exact accounting, distribution, atomic poison reject, 501 queries).
  **C2 phasing:** (1) roles+discovery ✓ → (2) CL-2 ingester replication ✓ →
  (3) router ✓ DONE → (4) CL-3 querier (stateless, unions live-buffer
  snapshots from ingesters + shared S3; enables query routing on the router)
  → (5) compactor+lease. Design: ARCHITECTURE §12.4.
- Previous: **C2 phase 2 SHIPPED — CL-2 ingester WAL replication** (2026-08-10,
  drill `docs/evidence/cl2-replication-drill.log`). ZERO ACKED LOSS on a
  node death: `TIMELAKE_ROLE=ingester` + peer from `TIMELAKE_PEERS` →
  each write replicated to peer's durable **replica WAL** BEFORE the 204.
  `crates/server/src/replication.rs` `Replicator` (blocking reqwest, one
  peer RTT/batch); engine `replica_wal` + `enable_replica_wal` +
  `replicate_receive` (dormant, NOT applied → no double-flush) +
  `recover_from_replica` (replay+flush, LWW-idempotent). `internal_router`
  on `TIMELAKE_CLUSTER_ADDR`: `/internal/v1/{replicate,recover,health}`
  (plaintext HTTP now, required-mTLS at C3). Degraded mode (PR-7): peer
  down → keep serving on local durability, `CL2_REPLICATION_DEGRADED`
  alarm once + `timelake_cl2_degraded` gauge, clears on peer return.
  Metrics `timelake_cl2_{replicated,degraded,degraded_events,replica_frames,
  recovered}`. **role=all UNCHANGED** (no peer→no replicator→no cl2
  metrics; unit + live pinned). 135 tests (+4 replication), clippy/fmt
  clean; live drill 12/12 (degraded + SIGKILL-A-recover-on-B-zero-loss).
  DEFERRED: auto health-triggered failover (recovery is explicit — drill/
  operator/router calls `/recover`); reqwest added to server deps
  (rustls, blocking). **C2 phasing:** (1) roles+discovery ✓ → (2) CL-2
  ingester replication ✓ DONE → (3) router (hash→ingester pair, forwards
  SQL/Flight to queriers) → (4) CL-3 querier + live-buffer snapshot → (5)
  compactor+lease. Design: ARCHITECTURE §12.4.
- Previous: **C2 phase 1 SHIPPED — cluster roles + Discovery seam**
  (2026-08-10). Working P1-1 (replication/HA) ONE PHASE AT A TIME (user
  preference [[phase-by-phase-workflow]]): each phase ends with regression
  + unit tests + doc updates. New `timelake-cluster` crate: `Role` enum
  (`TIMELAKE_ROLE`, default `all`), `Discovery` trait + `StaticDiscovery`
  (`TIMELAKE_NODE_ID`, `TIMELAKE_CLUSTER_ADDR`, `TIMELAKE_PEERS` =
  `id=role@host:port`). `all` = unchanged whole-stack default (bench
  untouched). Non-`all` roles REFUSE at startup (`exit 2`, "not yet
  implemented") — no half-built nodes. CL-5 guard: discovery carries NO
  correctness (commits go through catalog CAS, C1); nothing on write/commit
  path consults it. Server main.rs reads role at the edge, logs
  role/node/peers. 7 cluster unit tests; 131 total, clippy/fmt clean;
  role=all live-drilled (write/read unchanged), ingester+typo refuse.
  **C2 phasing:** (1) roles+discovery ✓ DONE → (2) CL-2 ingester WAL
  replication (next: gRPC frame ship before 204, degraded mode, SIGKILL
  zero-loss drill) → (3) router → (4) CL-3 querier + live-buffer snapshot
  → (5) compactor+lease. Design: ARCHITECTURE §12.4/12.5.
- Previous: **P0-5 SHIPPED — Tributary presents the data-plane token**
  (2026-08-10, in the TRIBUTARY repo: `docs/evidence/p05-data-auth.log`,
  drill `bench/drill-p05.sh`). Tributary `crates/tributary/src/auth.rs`
  `Secret` (redacting Debug/Display, `.expose()` the only door) +
  `resolve_token(TRIBUTARY_TOKEN env | token_file, env wins, trimmed)`;
  ship.rs sends `Authorization: Bearer` (built once, `set_sensitive(true)`),
  new `ShipError::Unauthorized` (401/403 — NOT bisected, NOT transport;
  data spools to durable queue, never dropped) + `unauthorized` counter.
  NO inline token in config (secret-in-committed-TOML = leak). Drilled
  10/10 vs required-mode node: correct token exact count, wrong token 0
  rows + spooled + auth-reported, no token authenticated=false, token
  never in logs. Verify-reads use a separate read token (also proves
  scope separation). **This closes the whole P0 set except P0-1 (push +
  CI, the user's action).** Next: P1 (replication/HA longest pole, audit
  trail, targeted delete R-1, Tributary self-telemetry T-1) — see
  `docs/ROADMAP.md`.
- Previous: **P0-4 SHIPPED — catalog commits are CAS (no multi-writer loss)**
  (2026-08-10, drill `docs/evidence/catalog-cas-drill.log`). Was: `commit`
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
  (2026-08-10, drill `docs/evidence/sql-sandbox-drill.log`). The
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
  `docs/evidence/data-auth-drill.log`). `TIMELAKE_DATA_AUTH=off|optional|
  required` (default off = header not read, today's compat contract).
  Token = 256-bit OS-CSPRNG secret, prefix `tldb_`, stored only as
  SHA-256 digest (NOT Argon2id — no brute-force surface, and a ~50ms KDF
  on a 100k-line/s write path breaks RR-1; reasoning in
  `crates/auth/src/token.rs` header). `crates/auth/src/guard.rs` =
  the ONE `decide()`; both HTTP and Flight route through
  `Engine::authenticate_data_impl` → `Auth::decide_data` → `guard::decide`
  so policy can't fork. **Mechanism fixed by measurement**
  (`docs/evidence/data-auth-client-probe.log`): Grafana Flight SQL
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
  drill `docs/evidence/sec3-mtls-want-mode.log`). `TIMELAKE_TLS_CLIENT_CA`
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
  admin/admin. ~~NOT DONE: `/api/sql` carries no identity~~ — **CLOSED
  2026-08-18**: `crates/server/src/tls_identity.rs` wraps `RustlsAcceptor`
  with an `Accept` that reads the subject CN off the completed handshake
  (`tls.get_ref().1.peer_certificates()`, borrowed not consumed) and
  layers `Extension(PeerIdentity)` onto the service, so the identity is
  extracted ONCE per connection rather than per request and the grant
  intersection now applies identically on HTTP and Flight. **DRILLED 15/15**
  (`docs/evidence/http-peer-identity-drill.log`,
  `deploy/compose/tls-drill/http_identity_drill.sh`): three clients make the
  IDENTICAL claim `ops,audit` and only the certificate differs — anonymous
  sees 3 rows (want mode unaffected), `narrowed-agent` granted `[ops]` sees
  **2** (was 3 before this change), a cert with no grants recorded sees 3
  (`None` = no policy, not deny-all). Also pinned: the restriction is
  enforced in the scan (SELECT agrees with COUNT(*)), and the CN lands on
  the `_system.queries` rows. Still open: requiring mTLS is a C3 decision
  for the intra-cluster listener. SECURITY.md exposure 3 now
  CLOSED, new exposure 9 states plainly that want mode grants nothing on
  its own. Windows curl (schannel) can't drive this — drill from a Linux
  container on the compose network.
- Previous: **SEC-4 admin auth SHIPPED** (2026-08-09, drill
  `docs/evidence/sec4-auth-drill.log`). New `timelake-auth` crate:
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
- **Retention is scoped by DATABASE as of 2026-08-19 — this was a data-loss
  bug.** `enforce_retention` matched on table name and ignored
  `FileMeta::db`, so a window set on one database's table expired every
  same-named table on the node (setting `metrics` in `poc` would have taken
  `_system.metrics` with it — which is why U2 self-monitoring shipped with
  no retention at all). Policies are now `(db, table)` with an explicit
  `"*"` wildcard; most specific wins. Stored v1 configs migrate to the
  wildcard, which preserves their behaviour EXACTLY — narrowing them on
  upgrade would silently stop expiring data an operator asked to delete.
  `db` is now REQUIRED on `PUT /admin/retention`; `DELETE` takes
  `/{db}/{table}`; the audit target names the scope; widening to `"*"`
  counts as destructive. Bounding `_system` is now safe (`docs/CONSOLE.md`
  §7.6 has the two calls) but is deliberately not done for you.
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
  design phased C0-C3; drill log `docs/evidence/c0-s3-drill.log`).
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
  `TIMELAKE_ENCRYPTION_KEY`). Rig: `deploy/compose/timelakedb-s3.yml`
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
  `docs/evidence/at7-drill.log`). New `timelake-tls` crate:
  validate-before-swap cert loading (PEM, expiry via x509-parser,
  key↔cert match via `CertifiedKey::from_der`), `ArcSwap` resolver
  consulted only at handshake, last-good + named
  `SEC3_CERT_RENEWAL_FAILED` alarm on a bad renewal. Both listeners TLS
  when `TIMELAKE_TLS_CERT`/`_KEY` set (HTTP via axum-server, Flight via
  tokio-rustls accept loop into `serve_with_incoming`); plaintext stays
  the default (`fixtures/` unchanged). Triggers: 2 s mtime watcher
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
  tldb-m4-final + tldb-m4-settled2 + idb3-exactness in
  ../Gauge/bench/results/).
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
     deploy/compose/timelakedb.yml before any further full-scale run.
  3. Shape A ~4 s/lookup at full scale: bloom row-group pruning
     apparently ineffective — VERIFY blooms are actually written for
     Dictionary columns (SerializedFileReader on a real file); arrow
     writer may skip blooms for dict columns → may need write-path
     change (e.g., bloom on values) or a per-file pid min/max index.
  4. Earlier fix, needs re-verification at full scale: dictionary
     double-count removed (batch_size 1M, DataSourceExec accounting).
  5. Still pending: Shape B full-scale pass, exactness cross-check vs
     influxdb3 (8-row/2 ppm LWW delta), then AT-3 scorecard + commit.
  Run logs: docs/evidence/tldb-m4-full{,2,3}.log. Gate via /tldb-gate.
  Also: (3 ppm) `docker kill` suppresses restart policies — drills use
  `docker restart -t 0`.

## Ground rules for work in this directory

- The acceptance test is tsdb-bench in `../Gauge/` — do not invent a new
  harness. A `timelakedb` backend adapter + compose target makes any
  prototype measurable with `python bench/bench.py run --backend
  timelakedb` and comparable via `bench/bench.py compare` against the
  recorded baselines in `../Gauge/bench/results/`. Run both from
  `../Gauge/` — the harness has not lived in this repository since G0.
- **Performance numbers come from Gauge; correctness verdicts come from
  Catchment** (`../Catchment/`, conformance and fault injection). Neither
  produces the other's output, deliberately — two sources of truth for
  one measure means the wrong one gets quoted.
- There is no `bench/` here any more. G0 gave the harness to Gauge, and D2
  split what was left by what it actually is: `deploy/compose/` holds the
  `timelakedb*.yml` topologies (each `build: ../..`) and the drill scripts
  that launch them, and `docs/evidence/` holds the drill transcripts that
  source comments and `docs/PRODUCTION_READINESS.md` cite by name.
  Catchment and Riverkeeper borrow those topologies; do not fork them.
- The hard invariant is RR-1: no query may kill the server. Designs that
  can't uphold it are out, regardless of speed.
- High-cardinality tags must cost what a compressed column costs (FR-2).
  Anything whose memory or write cost grows with distinct-tag-combination
  count repeats the failure this project exists to avoid.
- Keep query semantics identical to the canonical five Shape B queries and
  Shape A in `../Gauge/bench/backends/influxdb3.py` — those are the
  reference meanings, validated by matching row counts across engines.
- Telegraf (unmodified `influxdb`/`influxdb_v2` output plugins) and Grafana
  (stock datasource over Flight SQL) are first-class integrations — FR-8 /
  FR-9 / AT-6. The provisioned dashboards in `fixtures/grafana/`
  are the Grafana compatibility fixture; don't fork them.
