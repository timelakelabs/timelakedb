# TimelordDB

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
| `docs/evidence/` | The benchmark record this project is built on |
| `bench/` | tsdb-bench — the executable acceptance spec + recorded baselines |

## Status: M5 — acceptance drills complete

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

Remaining before v1: SEC-3 TLS + cert rotation (AT-7), streaming/range-
read execution (ingest-contention carve-out), CI on a remote.

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
(FR-8) — the unchanged fixture dashboards render against TimelordDB.

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
cd bench
docker compose -f compose/timelorddb.yml up -d --build
curl http://localhost:1963/health
python bench.py backends       # timelorddb is registered
```

Local development additionally wants a Rust toolchain
(`rustup` + MSVC build tools on Windows), then:

```bash
cargo test --workspace
cargo run -p timelord-server
```

## The rule

The benchmark harness is the specification. Every milestone gates on a
`bench.py run --backend timelorddb` result, compared against the recorded
InfluxDB 3 baselines in `bench/results/`.
