# TimelordDB

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
  `docker compose -f bench/compose/timelorddb.yml up -d --build`
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
  `cargo run -p timelord-server` (listens on TIMELORD_ADDR,
  default 0.0.0.0:1963).
- Repo files: `LICENSE` (Apache-2.0), `SECURITY.md`, `CONTRIBUTING.md`,
  `CODE_OF_CONDUCT.md`, `CHANGELOG.md`. `ops/tldb-backup.sh` +
  `docs/BACKUP_RESTORE.md` make AT-5 runnable (helper-container tar, no
  host bind mount; `--recreate` for the destroyed-volume case; drops
  `*.tmp-write` on restore). **No authn/authz exists** — SECURITY.md
  states the posture, including that `/api/sql` can `COPY … TO` files as
  the (root) server process. Known engine hardening: manifest replay
  should skip non-`.json` files.
- Status: **SEC-3 SHIPPED — AT-7 drill 19/19** (see
  `bench/results/at7-drill.log`). New `timelord-tls` crate:
  validate-before-swap cert loading (PEM, expiry via x509-parser,
  key↔cert match via `CertifiedKey::from_der`), `ArcSwap` resolver
  consulted only at handshake, last-good + named
  `SEC3_CERT_RENEWAL_FAILED` alarm on a bad renewal. Both listeners TLS
  when `TIMELORD_TLS_CERT`/`_KEY` set (HTTP via axum-server, Flight via
  tokio-rustls accept loop into `serve_with_incoming`); plaintext stays
  the default (bench/fixtures unchanged). Triggers: 2 s mtime watcher
  (works through a Windows bind mount) + POST /admin/tls/reload.
  Gauges: timelord_tls_cert_expiry_seconds, timelord_tls_last_reload_ok.
  Drill stack: `compose/timelorddb-tls.yml` (ports 2963/2964) +
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
     bench/compose/timelorddb.yml before any further full-scale run.
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
  A `timelorddb` backend adapter + compose target makes any prototype
  measurable with `python bench.py run --backend timelorddb` and
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
