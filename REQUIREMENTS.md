# TimelordDB — Requirements

**Status:** Draft v1 · 2026-08-08
**Evidence base:** Every requirement below traces to a measured result from
the tsdb-bench evaluation (`docs/evidence/BENCHMARK_RESULTS.md`, raw records in
`bench/results/`), in which five engines ran the identical workload:
InfluxDB 1.8 (OOM-killed by a query), InfluxDB 2.7 (funnel never completed),
QuestDB and VictoriaMetrics (prior trials, OOM on the same query shape), and
InfluxDB 3 Core (passed everything). TimelordDB must beat the survivor and
must be structurally incapable of the four failures.


---

## 0. Prior art and inspirations

TimelordDB stands on the projects below — four of which we benchmarked
directly on the reference workload. Each contributes something specific;
none satisfies the full requirement set, which is why this project exists.

| Project | Branch | Query languages | Docs | What TimelordDB takes from it |
|---------|--------|-----------------|------|-------------------------------|
| InfluxDB v3 | [`main`](https://github.com/influxdata/influxdb/tree/main) | SQL, InfluxQL | [docs.influxdata.com/influxdb3/core/](https://docs.influxdata.com/influxdb3/core/) | **The architecture to beat** — the only engine that passed Shape B. Validates FR-2 (dictionary-encoded tag columns), SR-1 (Parquet on object store), RR-1 (query memory pool), FR-8 (Flight SQL). Its weak spots define our targets: PR-3 (slow point lookups vs 1.x) and PR-6 (26× fresh-data penalty). |
| InfluxDB v2 | [`main-2.x`](https://github.com/influxdata/influxdb/tree/main-2.x) | Flux, InfluxQL | [docs.influxdata.com/influxdb/v2/](https://docs.influxdata.com/influxdb/v2/) | Cautionary evidence for FR-3 and FR-4: Flux's expressiveness cost (schema collisions, silent `stop: now()`) and the TSI decay curve (123K → 10K lines/s) behind FR-2/AR-1. |
| InfluxDB v1 | [`master-1.x`](https://github.com/influxdata/influxdb/tree/master-1.x) | InfluxQL, Flux | [docs.influxdata.com/influxdb/v1/](https://docs.influxdata.com/influxdb/v1/) | Proof that 18 ms point lookups exist (PR-3's stretch goal) — and proof of what the unbounded series index that delivers them costs (the OOM kill behind RR-1). Its TSM/WAL write path opened at 308K lines/s: raw write-path speed worth studying. |
| InfluxDB Cluster | [`master`](https://github.com/chengshiwen/influxdb-cluster/tree/master) | Flux, InfluxQL | [docs.influxdata.com/influxdb/v2/](https://docs.influxdata.com/influxdb/v2/) | Community clustering of the 1.x-era meta/data-node design — reference reading for §7: what a replication topology looks like when bolted on rather than designed in. TimelordDB inverts it: state in the object store, compute replaceable. |
| QuestDB | [`master`](https://github.com/questdb/questdb/tree/master) | SQL (SIMD/AVX2 execution) | [questdb.com/docs/](https://questdb.com/docs/) | Vectorized (SIMD) columnar execution and designated-timestamp ordered storage — the techniques for PR-5/PR-8 latency targets. Benchmarked history: OOM'd on Shape B in the prior trial, so its execution ideas must sit *behind* an RR-1 memory pool. |
| VictoriaMetrics | [`master`](https://github.com/VictoriaMetrics/VictoriaMetrics/tree/master) | MetricsQL (PromQL-compatible) | [docs.victoriametrics.com/victoriametrics/](https://docs.victoriametrics.com/victoriametrics/) | Resource-frugality engineering worth studying (compression, mmap discipline) — and two cautions it embodies: a metrics data model taxes event analytics (FR-6), and silent search guardrails mask capability (RR-5). Prior trial OOM'd on Shape B. |
| Apache Accumulo | [`main`](https://github.com/apache/accumulo) | scan API (server-side iterators) | [accumulo.apache.org](https://accumulo.apache.org/) | The cell-visibility security model (SEC-2): per-entry label expressions like `(ops&audit)\|admin` evaluated against session authorizations at scan time. TimelordDB adopts the *label model* at row granularity plus per-column keys — not the per-cell KV economics, which fight columnar storage. |

## 1. Mission

A time-series database for **high-cardinality event analytics plus fleet
metrics**: it must treat "millions of unique entity IDs per day" as the
normal case — ingesting at full speed, aggregating across all of them with
bounded memory, and storing them at columnar cost — while remaining
excellent at the classic TSDB jobs (point lookups, windowed rollups,
dashboard reads, cheap backups).

The one-sentence test: *count distinct entities per pipeline step over the
last 24 hours, across ~2M entities, without dying, without a warmup, in
seconds — on the same node that is ingesting.*

## 2. Reference workload (normative)

These numbers define "full scale" everywhere below. They are the real
workload profile the evaluation simulated, and the acceptance harness
generates them deterministically.

| Dimension | Value |
|---|---|
| Entities (`product_id`) | 1,000,000 new per day, never repeating |
| Pipeline events | 10 steps × start/stop ≈ 20M events/day (~232/s avg) |
| Event tags | entity id (~2M distinct per 2-day window), step (10), event (2), route (4), worker_ip (~500) |
| Burst pattern | 100,000 events delivered at once |
| Fleet metrics | 2,500 hosts × (1 rollup + 7 disk devices) every 10 s ≈ 2,000 points/s |
| Timestamps | nanosecond precision; slightly-future timestamps are legitimate |
| Query windows | 1–2 days operational; 1 year retention target (90 d hot acceptable) |
| Reference hardware | 8+ cores, 16 GB available to the engine (the evaluation box) |

## 3. Functional requirements

**FR-1 · Line-protocol push ingestion (MUST).** Accept InfluxDB line
protocol over HTTP POST with ns timestamps, batched (≥10K lines/request,
≥10 MB bodies). *Evidence: every candidate interoperated through this one
format; it is the migration path and the harness's write interface.*

**FR-2 · Tags are dictionary-encoded columns, never series keys (MUST).**
A tag with 2M+ distinct values must cost what a compressed column costs —
not an index entry per combination. There must be no data structure whose
size or maintenance cost is proportional to the number of distinct
tag-combinations. *Evidence: 40M series turned InfluxDB 2.x ingest from
123K to 10K lines/s and put 10.9 GB of idle index in RAM; the identical
data as dictionary columns ingested flat at 73K lines/s.*

**FR-3 · Standard SQL with real `COUNT(DISTINCT col)` (MUST).** The primary
query language is SQL; distinct-counting a tag column, CASE aggregation,
`date_bin`-style windowing, and subqueries must be first-class.
*Evidence: expressing the funnel needed gymnastics in InfluxQL (subquery
idiom), and Flux (group/distinct/count chains, int/float schema collisions,
two-yield workarounds). Adapter translation errors were a real defect
source; SQL was the only dialect where the canonical queries were trivial.*

**FR-4 · Unbounded-future time semantics by default (MUST).** A query with
only a lower time bound includes future-timestamped points. Any implicit
upper bound is a defect. *Evidence: Flux's silent `stop: now()` dropped
legitimately future-stamped events and produced wrong counts until patched.*

**FR-5 · Idempotent writes by primary key (MUST).** Same (timestamp, tag
set, field) ⇒ upsert, not duplicate. *Evidence: deterministic re-runs and
retry-after-timeout semantics in the harness depend on this; it is also
what made write-retry safe in every engine tested.*

**FR-6 · Both query shapes are first-class (MUST).**
- *Shape A — needle-in-haystack:* full ordered event history of one entity
  over 2 days. Wants pruning, not scanning.
- *Shape B — cross-entity aggregation:* distinct-count funnels, grouped
  counts, hourly throughput, rollups across the full entity population.
Neither may be an afterthought; see PR-3/PR-4 for targets.

**FR-7 · Retention per table/metric, file-granular (MUST).** Retention is
configurable per table (measurement) with a per-database default — e.g.,
`pipeline_events` 365 d, `host_metrics` 90 d, `disk_metrics` 30 d.
Enforcement drops whole immutable files, which constrains layout: a file
never mixes tables, and partition time-spans align to retention
granularity. Supports the 90-day-hot / archive-to-cold pattern without
rewrite; crypto-shredding (SEC-1) is an acceptable enforcement fast path.
*Upgraded from per-database SHOULD on 2026-08-08: the reference workload's
tables genuinely need different lifetimes.*

**FR-8 · Grafana works out of the box (MUST).** A stock Grafana
datasource — no custom plugin — connects, passes its health check, and
queries. Primary path: Flight SQL (the InfluxDB datasource's SQL mode);
PostgreSQL wire is an acceptable alternative. Concrete bar: the four
provisioned dashboards in `fixtures/grafana/` (pipeline funnel,
host fleet, host detail, product journey) render against TimelordDB with
no change beyond the datasource URL. *Evidence: those dashboards are plain
SQL over Flight SQL and served as the evaluation's entire read path.*

**FR-9 · Telegraf is a supported writer, unmodified (MUST).** A stock
Telegraf agent using the `influxdb` (v1) or `influxdb_v2` output plugin
works with only a URL (and token, when auth is on) change:
- `POST /write?db=&precision=` (v1) and
  `POST /api/v2/write?org=&bucket=&precision=` (v2)
- gzip `Content-Encoding` (Telegraf's v2-output default), precisions
  `s|ms|us|ns`, HTTP 204 on success
- parse errors identify the offending line; `/ping` and `/health` answer
  the plugins' startup checks
*Evidence: the "push ingestion barely changes" property (plan T8) is what
makes migration cheap — the real fleet's collectors are Telegraf-class
agents emitting exactly these calls. Write-API compatibility is in scope;
InfluxQL/Flux query compatibility stays out (§12).*

## 4. Performance requirements (at reference workload & hardware)

| ID | Requirement | Target | Evidence / bar to beat |
|---|---|---|---|
| PR-1 | Sustained bulk ingest | ≥ 75K lines/s | InfluxDB 3: 73,170 |
| PR-2 | **Ingest flatness under cardinality growth** | last-decile rate ≥ 80% of first-decile across 40M distinct keys | idb3 flat; idb2 decayed 12×; idb1 batch p95 4.2 s jitter |
| PR-3 | Shape A latency | p95 ≤ 250 ms warm; stretch ≤ 50 ms | idb1 proved 18 ms is possible (via the index we must not build); idb3 did 607 ms with pruning only |
| PR-4 | Shape B completion | **100%, always, bounded memory** — this is the make-or-break | 3 of 5 engines failed exactly here |
| PR-5 | Shape B latency | 24 h funnel ≤ 10 s on fresh data, ≤ 3 s settled; every canonical shape ≤ 30 s fresh / ≤ 5 s settled | idb3: 5.7 s fresh / 1.1 s settled (funnel), worst shape 30 s fresh |
| PR-6 | Fresh-vs-settled penalty | ≤ 10× on any canonical query | idb3 showed up to 26× (B4); halve it |
| PR-7 | Burst absorption | 100K events ≤ 5 s, zero loss, while serving queries | idb3: 1.14 s with a concurrent funnel answering |
| PR-8 | Windowed `COUNT(*)` over ~37M rows | ≤ 5 s | idb3: 1.0 s settled / 30 s fresh; idb1: timed out |
| PR-9 | Concurrent read during write load | Shape B during burst degrades ≤ 2× vs idle | idb3: 5.9 s during burst vs 5.7 s idle |

## 5. Robustness & resource governance

**RR-1 · No query may kill the server (MUST — hard invariant).** Query
execution runs under an admission-controlled memory pool (configurable,
default ~20% of RAM) with spill-to-disk where possible and a clean,
client-visible error where not. The *process* never dies from a query.
*Evidence: InfluxDB 1.8 was OOM-killed by a grouped count (14.4 GB of
15.2 GB) — the single event that disqualifies an engine; InfluxDB 3's
DataFusion pool failed the same class of query gracefully at ~2 GB while
concurrent queries kept answering.*

**RR-2 · Abandoned queries are reaped (MUST).** Client disconnect/timeout
cancels server-side execution promptly (≤ 5 s), releasing memory.
*Evidence: timed-out Flux queries kept grinding server-side, compounding
memory pressure toward the 1.x-style kill.*

**RR-3 · Crash recovery ≤ 30 s to writable (MUST).** After hard kill,
the node accepts writes again within 30 s; no data loss beyond the
acknowledged-write contract. *Evidence: 1.8 needed minutes of TSM reopening
and dropped 60% of a burst; the columnar engine restarted healthy in
seconds during the restore drill.*

**RR-4 · Bounded idle footprint (MUST).** Steady-state resident memory on
the reference workload ≤ 6 GB, independent of total distinct-key count.
*Evidence: idb3 ~5 GB working set vs idb2's 10.9 GB idle index.*

**RR-5 · Guardrails are visible, tunable, and never silent (MUST).** Any
limit that can reject work (query memory, files-per-query, series/values
caps) must be configurable, discoverable at runtime, and produce an error
naming the limit. Defaults must not silently disqualify the reference
workload. *Evidence: three engines shipped defaults that masked capability
(1.x 1M-series cap, VictoriaMetrics search limits, Core's 72 h
query-file-limit) — each cost evaluation time to distinguish "engine
can't" from "config won't."*

## 6. Storage & operability

**SR-1 · Immutable-file storage on an object-store abstraction (MUST).**
Data lives as immutable columnar files (Parquet or equivalent) behind a
file/S3-style store. Backup is file copy/replication — no proprietary
snapshot path. *Evidence: this single property turned the incumbent's
10–15 min backup pain into 47 s backup / 6 s restore.*

**SR-2 · Storage efficiency ≤ 1.5 GB/day at reference workload (MUST).**
*Evidence: columnar hit 1.15 GB/day; TSM engines needed 8.1 GB/day (~3 TB
per retained year — untenable vs 420 GB).*

**SR-3 · Background compaction with observable state (MUST).** Small fresh
files merge into settled generations without blocking reads/writes; the
fresh↔settled boundary is queryable (ties to PR-6). *Evidence: the 5–26×
fresh-vs-settled gap was the largest single performance lever observed.*

**SR-4 · Cheap operational verification (SHOULD).** Health endpoint,
version endpoint, per-query memory/telemetry, and a way to measure on-disk
size scoped per database — everything the harness had to shell into
containers to learn.

## 7. Clustering & availability

Clustering is a **requirement, not an aspiration** — the evaluation plan's
production constraint stands: a single instance is a single point of
failure, unacceptable for production. v1 ships single-node (the benchmark
target), but the v1 architecture must be cluster-ready, because the
disaggregated pattern is cheap to design in and brutal to bolt on
(see the InfluxDB Cluster fork, §0).

**CL-1 · Cluster-ready architecture from v1 (MUST, v1).** All durable
state lives in the object store plus a catalog with a pluggable backend.
A node's local disk holds only cache and WAL, both reconstructable or
uploadable. No component may assume it is the sole reader/writer except
through catalog coordination. *Evidence: state-as-object-store-files is
the property that made backup 47 s and restore 6 s; it is also what makes
"replication" mean object-store replication plus catalog failover instead
of a bespoke data protocol.*

**CL-2 · Ingest replication (MUST, v2).** Writes acknowledged only after
durability on N ingesters (default 2) or confirmed WAL upload to the
object store; zero acknowledged-write loss on any single node failure.

**CL-3 · Query high availability and scale-out (MUST, v2).** Multiple
stateless queriers share the store; a querier failure aborts only its own
in-flight queries (RR-1 holds per node). Target: Shape B throughput scales
near-linearly to 4 read nodes.

**CL-4 · Rolling upgrade and node replacement (SHOULD, v2).** Any node
replaceable without write downtime; a replacement reaches serving state in
≤ 5 min, bounded by cache warmup — never by state copy.

**CL-5 · Pluggable membership & discovery, Consul supported (MUST, v2).**
Cluster topology (which nodes exist, their roles, their health) comes from
a `Discovery` trait with named backends: **static config** (v1/dev) and
**HashiCorp Consul** (service registration, health checks, and sessions/
leases for role election such as the per-shard compactor singleton).
Design guard: no *correctness* property may depend on the discovery
backend — writes and catalog commits stay safe under a wrong or stale
membership view (correctness lives in catalog CAS, CL-1); discovery
affects availability and routing only. Backend choice is config, not code.

## 8. Security & data governance (design constraints now, implementation phased)

v1 ships token auth and optional TLS only (§12) — but the following are
**design constraints on v1 code**, because they are nearly free to
accommodate early and prohibitively expensive to retrofit.

**SEC-1 · Object-level encryption (design constraint v1 · implement SHOULD v2).**
Every stored object must be encryptable at rest with envelope encryption:
per-object data keys wrapped by KMS-managed master keys, key scope
configurable per database / table / column. Direction: **Parquet Modular
Encryption** — encrypted footer plus per-column keys — which makes column
access a key-distribution decision and composes with SEC-2. Key rotation
rides compaction rewrites; FR-7 retention gains crypto-shredding (destroy
the key, the data is gone) as a fast path. The v1 constraint: all object
I/O flows through one narrow read/write layer where encryption slots in
without touching the engine. Open question: arrow-rs Parquet encryption
maturity — verify before committing to PME versus whole-object envelope
encryption.

**SEC-2 · Cell-level access control, Accumulo-style (feasibility verdict + design constraint).**
The honest assessment requested:
- **Row-level visibility labels map well to this architecture.** An
  optional dictionary-encoded `_visibility` column holding Accumulo-style
  label expressions (`(ops&audit)|admin`) — label sets are low-cardinality,
  so FR-2's column economics apply — evaluated against session
  authorizations as a **mandatory predicate injected before any scan,
  filter, or aggregation**. Enforcement lives inside the query engine, not
  the API layer: an aggregate must never count a row the caller cannot see.
- **Column-level control falls out of SEC-1**: no column key, no column.
- **True per-cell labels (independent label on each value within a row)
  are NOT planned.** Accumulo prices that model for a sorted key-value LSM
  where every cell is already an independent entry; in columnar batches it
  destroys compression and vectorization. Row labels + column keys
  approximate realistic policies without breaking the engine's economics.
- The v1 constraint: the query path exposes exactly one mandatory-predicate
  injection point (also the future hook for multi-tenancy and
  retention-boundary filtering).

**SEC-3 · TLS 1.3 on every wire (MUST, v1 external · v2 intra-cluster).**
(Specified as TLS 1.3 — there is no "TLS 3.0"; a configurable TLS 1.2
floor is allowed for legacy clients, nothing older.)
- **v1:** every listener — the HTTP write API (FR-1/FR-9) and Flight SQL
  (FR-8) — serves TLS 1.3 via rustls; certificates hot-reload without
  restart. Verified with real clients: Telegraf's `tls_ca`/`tls_cert`
  output options and Grafana's secure-connection datasource settings
  (the evaluation ran Grafana with "insecure gRPC" only because the PoC
  had no TLS — the fixture must pass both ways).
- **v2:** intra-cluster traffic (WAL replication CL-2, router↔node,
  catalog) uses **mutual TLS** with the same rustls stack.
- **Short-TTL certificates with hot rotation (MUST, v1).** The system is
  designed for certificates with a validity of ~24 h, refreshed daily
  (Vault agent / step-ca / cert-manager / ACME-style issuers that
  materialize files):
  - Reload is triggered by watching the cert/key files (the universal
    integration) and by an explicit admin endpoint; **never by process
    restart**.
  - Rotation affects **new handshakes only** — established connections
    and in-flight streams (long-lived Flight SQL queries, Telegraf keep-
    alive sessions) continue uninterrupted; no draining, no forced close.
  - Cert+key pairs load **atomically and validate before adoption**
    (expiry, key match); a bad renewal keeps the last-good pair serving
    and raises a loud, named alarm (RR-5) — degraded-but-serving beats
    correct-but-down.
  - Days-to-expiry is an exported metric with an alert threshold below
    the renewal interval, so a stalled rotation is caught while the old
    cert still works.
  - mTLS trust anchors (v2) rotate the same way, with **dual-CA overlap**
    supported so client and server bundles can roll independently.
- Operational note carried from the evaluation: Flight SQL is gRPC over
  HTTP/2 — any proxy in front must speak HTTP/2.

## 9. Anti-requirements — designs the evidence forbids

1. **No inverted series index over the full tag space** (2.x decay curve,
   1.x death, 11 GB idle RAM). An *optional bounded* index for hot entities
   is acceptable if capped and evictable (PR-3 stretch).
2. **No query language that cannot distinct-count a tag without
   workarounds** (InfluxQL, MetricsQL, Flux all taxed the canonical query).
3. **No implicit query-time upper bound at `now()`** (silent wrong answers).
4. **No monolithic mutable storage requiring proprietary backup** (the
   original 10–15 min pain).
5. **No default configuration that rejects the reference workload** —
   ship defaults that attempt it; protect with the RR-1 pool, not caps.

## 10. Acceptance testing — the benchmark is the specification

The existing harness (`bench/`) is the executable acceptance test.

- **AT-1 (MUST):** Implement a `timelorddb` tsdb-bench adapter (~100 lines:
  write endpoint, five canonical Shape B queries in SQL, Shape A, health,
  storage path) and a compose target.
- **AT-2 (MUST):** At `--scale smoke`, all context sanity counts and
  per-query row counts match the influxdb3 reference exactly (the
  deterministic generator makes datasets byte-identical).
- **AT-3 (MUST):** At `--scale full` on reference hardware, meet every PR
  and RR row above; `run.json` is the artifact of record, compared against
  `influxdb3-idb3-full-*` baselines with `bench.py compare`.
- **AT-4 (MUST):** Repeat AT-3 twice; results within 10% run-to-run
  (the idb3 baseline pair demonstrated this reproducibility).
- **AT-5 (SHOULD):** Kill-during-load, backup/restore drill (≤ 2 min /
  ≤ 1 min), and the sustained-repeated-burst variant.
- **AT-6 (MUST):** Ecosystem end-to-end: a stock Telegraf agent
  (`influxdb_v2` output, gzip enabled) writes host metrics to TimelordDB
  while the evaluation's Grafana provisioning (`fixtures/grafana/`)
  points at it — datasource health check passes and all four dashboards
  render and refresh. This reruns the plan's T5 + T8 against TimelordDB.
  **Runs twice: plaintext and TLS 1.3 end-to-end** (Telegraf `tls_*`
  options, Grafana secure connection — SEC-3).
- **AT-7 (MUST):** Certificate rotation drill: under sustained Telegraf
  writes and a long-running Flight SQL query, rotate to a fresh 24 h cert.
  Pass: zero dropped connections, zero write errors, the in-flight query
  completes, and the next new connection presents the new certificate.
  Repeat with a deliberately corrupt renewal: server keeps serving on the
  last-good cert and raises the SEC-3 alarm.

## 11. Technology direction (decided 2026-08-08)

**Rust, with Apache DataFusion / Arrow / Parquet / `object_store` as the
foundation** — libraries, not a fork.

- RR-1 is the deciding argument: bounded query memory demands deterministic
  allocation accounting. The benchmark's GC-runtime engines (Go: InfluxDB
  1/2, VictoriaMetrics; Java: QuestDB) all failed under query memory
  pressure; the sole survivor runs DataFusion's memory pool, which we
  watched reject an unbounded aggregate cleanly at its budget.
- The stack directly supplies FR-2 (Arrow dictionary encoding), FR-3
  (SQL with `COUNT(DISTINCT)`), FR-8 (`arrow-flight` → Flight SQL →
  stock Grafana), SR-1 (`object_store` — the crate InfluxData donated),
  and SEC-1's direction (Parquet Modular Encryption).
- TimelordDB builds what differentiates it: line-protocol ingest + WAL,
  catalog/partitioning (CL-1), compaction (PR-6), per-table retention
  (FR-7), the bounded hot-entity index experiment (PR-3), and the
  mandatory-predicate injection point (SEC-2).
- Not forking InfluxDB 3 Core (we would inherit its gaps as upstream
  drift); GreptimeDB is the study reference for a small-team
  Rust/DataFusion TSDB.

## 12. Out of scope for v1

Multi-node operation (phased per §7 — v1 is single-node but cluster-ready
by CL-1; Consul discovery arrives with it, CL-5), authn/z hardening beyond
token auth (SEC constrains design only — note TLS is **in** scope for v1
per SEC-3), encryption *implementation* (SEC-1 is a v1 design constraint,
v2 feature), InfluxQL/Flux query compatibility layers (write-API compat is
in scope via FR-9), cross-datacenter replication, and a bespoke dashboard
UI (Grafana is the read path).

## 13. Open questions

- Ingest format v2: keep line protocol only, or add Arrow Flight ingest for
  higher-throughput native clients?
- PR-3 stretch (≤ 50 ms lookups): bounded hot-entity index vs. smarter
  file pruning (bloom filters per column chunk)? Prototype both against
  Shape A before committing.
- Fresh-data latency (PR-6): eager mini-compaction vs. query-side merge
  optimization — measure with the harness's `--scenarios query_b` on
  just-ingested data.
- Retention tiering (FR-7): is 90-day-hot + cold-recall a v1.x feature or
  a v2 feature?
- Catalog backend for CL-1: object-store-native manifest (Iceberg-style),
  embedded raft, or external Postgres? Decides how hard CL-2/CL-3 are.
- SEC-1: does arrow-rs Parquet Modular Encryption cover our needs today,
  or does v1's I/O layer plan for whole-object envelope encryption first?
- SEC-2: adopt Accumulo's visibility-expression syntax verbatim (operator
  familiarity, existing tooling) or a simplified label algebra?
