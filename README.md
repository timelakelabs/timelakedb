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

## Status: M3 — compaction, retention, Flight SQL, Grafana

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
