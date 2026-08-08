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
- With a local toolchain: `cargo test --workspace`,
  `cargo run -p timelord-server` (listens on TIMELORD_ADDR,
  default 0.0.0.0:1963).
- Status: **M3 shipped** — compaction (cross-file LWW dedup, FR-5
  complete), per-table retention (FR-7), Flight SQL on :1964 serving the
  stock Grafana datasource (FR-8; fixture dashboards render unchanged;
  grafana profile in the compose file, port 3003). Laptop gate: 616K
  lines/s ingest, 0 errors, Shape B ≤1.6 s, burst 100K/0.17 s. Open
  items: (1) 8-row (2 ppm) dedup delta vs accepted lines — compare with
  influxdb3 on identical fresh data at the M4 gate; (2) no file pruning
  yet (Shape A 517 ms at laptop — M4's bloom/pruning work); (3) `docker
  kill` suppresses restart policies — drills use `docker restart -t 0`.
  Use the `/tldb-gate` skill for build/test/gate commands.

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
