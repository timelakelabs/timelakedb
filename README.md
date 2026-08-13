# TimeLakeDB

[![ci](https://github.com/timelakelabs/timelakedb/actions/workflows/ci.yml/badge.svg)](https://github.com/timelakelabs/timelakedb/actions/workflows/ci.yml)

A time-series database for high-cardinality event analytics, specified
from evidence: five engines ran an identical 36M-event workload and their
measured failures define this one. The survivor (InfluxDB 3) sets the
baselines this project must beat; the failures (a query OOM-killing
InfluxDB 1.8, InfluxDB 2.x's funnel never completing, QuestDB and
VictoriaMetrics OOMs) define what it must be structurally incapable of.

**Stack:** Rust · Apache DataFusion · Arrow · Parquet · `object_store`.

| Read | Purpose |
|---|---|
| `REQUIREMENTS.md` | Evidence-traced requirements (FR/PR/RR/SR/CL/SEC + acceptance tests) |
| `ARCHITECTURE.md` | Components, seams, milestones M0–M5 |
| `SECURITY.md` | Security posture, exposures, and how to report a vulnerability |
| `CONTRIBUTING.md` | Development environment, the crate map, what CI enforces |
| `CHANGELOG.md` | What landed, milestone by milestone |
| `docs/BACKUP_RESTORE.md` | The AT-5 procedure, runnable (`ops/tldb-backup.sh`) |
| `deploy/compose/` | Compose topologies for every deployment shape, and the correctness drills that launch them |
| `docs/evidence/` | Drill transcripts — the recorded runs that source comments and `docs/PRODUCTION_READINESS.md` cite by name |
| `../Gauge/` | tsdb-bench — the executable acceptance spec, recorded baselines, and `PERFORMANCE_LOG.md` |
| `../Catchment/` | Conformance and fault-injection testing across this and Tributary |
| `site/` | Project website: landing, docs, and `docs/reference.html` — line protocol, SQL dialect, API surface, InfluxDB compatibility, metrics, glossary |

> **Security:** the data plane **can** authenticate but ships `off`.
> `TIMELAKE_DATA_AUTH=optional|required` turns on token authentication for
> writes, `/api/sql` and Flight SQL; until you set it, any client that can
> reach port 1963 or 1964 reads and writes everything, and network
> isolation is your only access control. The *administrative* surface
> always authenticates (SEC-4), and TLS 1.3 with hot rotation, client
> certificates in want mode, encryption at rest and row visibility labels
> are implemented and drilled. Two things to know before deploying: a
> fresh node seeds a quarantined `admin`/`admin` console credential, and
> SEC-2 authorizations are **self-asserted claims** on the anonymous path
> — a token or client certificate can only ever narrow them. Read
> [`SECURITY.md`](SECURITY.md) before deploying anything.

## Website

`site/` is plain HTML, CSS and SVG — no build step, no dependencies. Open
`site/index.html` in a browser to preview it. `.github/workflows/pages.yml`
publishes the directory on every push that touches it; enable it once per
repository under **Settings → Pages → Source: GitHub Actions**. Every figure
on the site traces to a run under `docs/evidence/`.

## Status: C2 phase 4 — the cluster reads without losing freshness

Clustering is arriving one role at a time, each ending in a recorded
drill. Four of the five phases are in: roles and discovery, ingester WAL
replication, the router, and now the stateless querier.

- **One binary, five roles** (`TIMELAKE_ROLE`): `all` is the default and
  unchanged — the whole stack in one process. A role whose phase has not
  landed refuses to start rather than running half-built.
- **Writes**: the router is the single endpoint clients keep seeing. It
  shards each line-protocol body by `(db, measurement)` across the
  ingesters, whole-body-validated first so a poison line writes nothing;
  the chosen ingester replicates every frame to its pair **before the
  204**, so an acknowledged write is durable on two nodes. A peer that
  is down degrades loudly rather than failing writes.
- **Reads**: a querier owns no data — it replays the catalog from the
  shared object store and unions the ingesters' *live buffers*, served as
  Arrow IPC, so counts are exact seconds after ingest rather than after
  the next flush. Kill one and reads continue; a fresh container with an
  empty disk rebuilds its whole view from the bucket. If an ingester is
  unreachable, a querier **refuses the query** instead of returning a
  quietly short count.
- **It stays the same database from outside.** The unmodified benchmark
  drove the whole cluster through the router's single address: 323K
  lines/s, all five Shape B queries complete, and `rows_48h` = 77,806 —
  exactly what one node returns.
- Drills: `docs/evidence/cl2-replication-drill.log` (12/12),
  `router-sharding-drill.log` (8/8), `cl3-querier-drill.log` (19/19).

Still to come in C2: the compactor role and its singleton lease.

**Next:** finish C2 with the compactor role, then C3 (Consul discovery and
required intra-cluster mTLS, `ARCHITECTURE.md` §12); re-baseline the
benchmark inside the container network, because ~94% of the reported
Shape A latency is Docker Desktop port forwarding
(`../Gauge/PERFORMANCE_LOG.md`).

### Previous: SEC-4 — authentication, admin surface and data plane

- **Every `/admin/*` route requires a session.** Argon2id credentials,
  cookie or bearer sessions (30 min idle / 12 h absolute), CSRF and
  Origin checks on mutations, per-principal backoff on failed logins.
  Roles are ordered `viewer` < `operator` < `admin`, and retention
  authorization follows the data rather than the verb: *growing* a
  window needs `operator`, while *shrinking, introducing or removing*
  one needs `admin`.
- **First run seeds `admin`/`admin`, quarantined.** It authenticates,
  and then the only route that answers is `POST /admin/password` —
  everything else returns `403 password_change_required`, so the
  well-known credential cannot destroy data. Rotation invalidates every
  session for that principal, including the one that performed it.
  `TIMELAKE_ADMIN_BOOTSTRAP_PASSWORD` provisions a real password so no
  well-known default ever exists; alert on
  `timelake_admin_default_credential_active`.
- **The data plane authenticates by token, in three stages.**
  `TIMELAKE_DATA_AUTH=off|optional|required` — `off` is the default and
  the compatibility contract, `optional` is the migration state (anonymous
  still served, bad tokens refused, and the
  `timelake_data_requests_*` split measures what a flip would break), and
  `required` closes both ports. One token, three spellings, because the
  clients differ: `Bearer` for Grafana's Flight SQL and Tributary, `Token`
  for Telegraf v2, `Basic` for Telegraf v1 — a mechanism chosen from a
  recorded client probe rather than from the specifications.
- Drills: `docs/evidence/sec4-auth-drill.log`,
  `docs/evidence/data-auth-drill.log`.

### Previous: C0 — S3 object store with KMS envelope encryption

- `S3Store` behind the same `Store` trait (aws-sdk-s3, owned-runtime sync
  bridge), and `AwsKms` behind the same `Kms` trait. The engine cannot
  tell S3 from a local directory (CL-1).
- Client-side envelope encryption **and** SSE-KMS with S3 Bucket Keys,
  each with its own key cache. Measured on the LocalStack rig: an
  identical workload writing 56 objects cost **1 KMS call cached against
  56 uncached**, confirmed by LocalStack's own API log.
- `Store` gained `put_if_absent` — the compare-and-swap primitive
  (S3 `If-None-Match`) that C1's multi-writer catalog needs.
- Drill: `docs/evidence/c0-s3-drill.log`.

### Previous: runtime retention management + console

- `GET`/`PUT` `/admin/retention` and `DELETE /admin/retention/{table}`
  manage per-table windows at runtime; policies persist through the
  `Store` (so they are encrypted with everything else) and outlive a
  restart with a stale environment.
- A self-contained management page at `/admin/ui` — no build step, no
  external assets.

### Previous: SEC-1 + SEC-2 — encryption at rest and row visibility labels

- **SEC-1, encryption at the store chokepoint:** set
  `TIMELAKE_ENCRYPTION_KEY` (64 hex chars, or `_KEY_FILE`) and every
  object — Parquet data, catalog manifests, checkpoints — is
  envelope-encrypted: a fresh AES-256-GCM data key per object, wrapped by
  your key, in 64 KiB authenticated chunks so the range-read path
  (bloom probes, footer loads) keeps working. The engine can't tell: the
  decorator wraps the one `Store` trait all object I/O flows through.
  A wrong key is a named startup refusal, never silent plaintext.
  Chose envelope over Parquet Modular Encryption (covers manifests, no
  arrow-rs dependency); PME per-column keys remain open at the same seam.
- **SEC-2, Accumulo-style row visibility:** a `_visibility` tag holds a
  label expression per row — `(ops&audit)|admin` — evaluated against the
  session's authorizations (`X-TimeLake-Authorizations` header, or Flight
  SQL metadata) *inside the scan*, below user predicates and before
  aggregation, so a `COUNT(*)` cannot count a hidden row. Unlabeled rows
  are public; malformed labels are visible to no one. Labels are ordinary
  dictionary tags: no write-path ceremony, FR-2 economics.
- **Drill** (`docs/evidence/sec12-drill.log`): HTTP and Flight SQL
  enforce identically on buffer and Parquet paths; everything at rest is
  ciphertext (data *and* manifests); restart recovers through encrypted
  manifests; the smoke suite on the same image is unchanged (0 errors,
  exact counts).

### Previous: SEC-3 — TLS 1.3 with hot cert rotation, AT-7 passed

- **TLS 1.3 (rustls) on both listeners** — HTTPS (writes, /api/sql) and
  Flight SQL — from one shared `ArcSwap<CertifiedKey>` resolver
  consulted only at handshake, so rotation never touches established
  connections. Configurable 1.2 floor (`TIMELAKE_TLS_MIN=1.2`);
  plaintext remains the default when `TIMELAKE_TLS_CERT`/`_KEY` are
  unset. Renewals validate (PEM, expiry, key↔cert match) *before* an
  atomic swap; triggers are a 2 s file watcher and POST
  `/admin/tls/reload`; `/metrics` exports
  `timelake_tls_cert_expiry_seconds` and `timelake_tls_last_reload_ok`.
- **AT-7 drill 19/19** (`docs/evidence/at7-drill.log`): under stock
  Telegraf-over-HTTPS plus sustained writes, rotating to a fresh 24 h
  cert landed mid-flight in a 20 s Flight SQL query (219B-combo SUM,
  result exact) — zero write errors, zero dropped connections, both
  listeners presenting the new cert to the next connection via the file
  watcher alone. A deliberately corrupt renewal was rejected 422 with
  the named `SEC3_CERT_RENEWAL_FAILED` alarm while the last-good pair
  kept serving; the next good renewal restored health.

### Previous: M5 — acceptance drills complete

- **AT-6:** stock Telegraf (`influxdb_v2` output, gzip default) writes
  with only a URL; the fixture Grafana dashboards render over Flight SQL.
- **AT-5:** backup 34 s / restore-from-destroyed-volume 13 s with all
  36.68M rows exact (vs 10–15 min on the 1.x incumbent); SIGKILL
  mid-ingest → healthy in 4.7 s, zero acknowledged-write loss (count
  exact at 40.34M); ten consecutive 100K bursts absorbed ≤0.13 s each,
  0 errors, concurrent queries stable.
- **AT-4:** repeat full-scale run within tolerance (ingest ±3.5%,
  funnels ±6%, storage ±9%, 0 errors both runs).
- **Metadata cache:** warm journey lookups **0–6 ms** (immutable footers
  prune without fetching; only surviving files are read) — the M4 p95
  carve-out closed; cold ≈300 ms.

### Previous: M4 — full-scale gate passed (two carve-outs)

The read path earned its full-scale numbers the hard way: five gate
attempts, each failure measured and fixed — shared memory pool with
admission control, decode-time row filters over entity-clustered
row-group statistics, blocking-pool scans with deadlines, grace-period
GC, a container memory cap, and native-volume I/O.

Final gate (36.6M events, fresh after ingest, vs the InfluxDB 3
baseline): ingest 365-671K lines/s 0 errors (5-9×); Shape A median
**211 ms** (vs 520); Shape B **all complete** — funnel 1.7 s (vs 5.7),
B4 0.68 s (vs 30.3); burst 100K in 0.12 s with concurrent query; COUNT(*)
over 36.7M in 2.2 s; storage **0.50 GB/day** (vs 1.15); zero
acknowledged-row loss proven by fixed-bound equality on identical data.
Carve-outs → M5: Shape A p95 608 ms vs the 250 ms target, and intra-run
ingest decline under maintenance contention (cross-run stable, so not
cardinality decay) — both addressed by streaming/range-read execution.

### Previous: M3 — compaction, retention, Flight SQL, Grafana

Compaction merges L0 files per (table, hour) with cross-file
last-write-wins dedup (FR-5 complete); per-table retention drops whole
partitions (FR-7); Flight SQL serves Grafana's stock datasource on :1964
(FR-8) — the unchanged fixture dashboards render against TimeLakeDB.

M3 gate: laptop scale (3.66M events) — ingest 616K lines/s, 0 errors,
all Shape B ≤1.6 s, burst 100K in 0.17 s with concurrent query, Grafana
datasource health OK and the funnel panel returning all ten steps
through Flight SQL. Open item: 8-row (2 ppm) LWW dedup delta vs accepted
lines — verify against an influxdb3 run on identical fresh data at M4.

### Previous: M2 — a real storage engine

Ingest: parser → WAL (durable before the 204, generation-rotated) →
buffer. Flush (L0): PK-sort + last-write-wins dedup → (table, UTC hour)
Parquet partitions through the Store chokepoint → manifest-log catalog →
WAL reclaim. Reads union buffer snapshots with cataloged Parquet under
the RR-1 memory pool; WAL cap answers 429 (RR-5).

M2 gate: smoke suite green with counts exact to the row (77,806) before
*and after* the full flush cycle (buffer 0, WAL 0 bytes, 52 Parquet
files); **SIGKILL → healthy in 0.8 s** with zero acknowledged-write loss
(RR-3). Known limits: cross-file dedup completes with compaction (M3);
no file pruning yet; fresh-vs-settled work is M3/M4.

## Quickstart (Docker — no local Rust needed)

```bash
git clone https://github.com/timelakelabs/timelakedb.git
cd TimeLakeDB/bench
docker compose -f compose/timelakedb.yml up -d --build
curl http://localhost:1963/health
(cd ../Gauge && python bench/bench.py backends)   # timelakedb is registered
```

The admin console is at <http://localhost:1963/admin/ui>. A fresh node
seeds `admin`/`admin`, which can do nothing until you change it — see
[`SECURITY.md`](SECURITY.md).

Local development additionally wants a Rust toolchain
(`rustup` + MSVC build tools on Windows), then:

```bash
cargo test --workspace
cargo run -p timelake-server
```

## Backup

```bash
./ops/tldb-backup.sh backup                 # live, no downtime
./ops/tldb-backup.sh verify  -f <archive>
./ops/tldb-backup.sh restore -f <archive> --recreate
```

Measured on a 1.19 GB volume while ingesting: backup 25 s, restore 66 s,
healthy in under a second, and a fixed-bound `COUNT(*)` identical on the
source and the restored copy (40,327,616). Runbook: `docs/BACKUP_RESTORE.md`.

## The rule

The benchmark harness is the specification. Every milestone gates on a
`bench/bench.py run --backend timelakedb` result, run from `../Gauge/` and
compared against the recorded InfluxDB 3 baselines in
`../Gauge/bench/results/`.

## License

Apache-2.0 — see `LICENSE`. Contributions are governed by `CONTRIBUTING.md`
and `CODE_OF_CONDUCT.md`.
