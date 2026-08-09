# tsdb-bench — cross-solution performance testing framework

Runs the product-pipeline + host-metrics workload (the one defined in
`../EVALUATION_PLAN.md`) against a time-series database and records
**normalized performance metrics**, so InfluxDB 3 can be compared
apples-to-apples against other solutions (QuestDB, VictoriaMetrics,
InfluxDB 1.x, or anything you add an adapter for).

Design in one paragraph: workload generation is deterministic and emitted
as InfluxDB line protocol, which every candidate accepts as a write format
— so **every backend ingests the exact same bytes**. Only query translation
differs, and that lives in one adapter file per backend. Every run produces
the same `run.json` metric schema regardless of backend or scale, and
`bench.py compare` renders any set of runs side by side.

## Quickstart

```powershell
# prerequisites: Python 3 + requests, Docker
pip install -r requirements.txt

# 1. start a target, e.g. the InfluxDB 3 baseline
docker compose -f compose/influxdb3.yml up -d

# 2. shake out the harness (~30s, any machine)
python bench.py run --backend influxdb3 --scale smoke --label idb3-smoke

# 3. real run (laptop dry run, or --scale full on the evaluation box)
python bench.py run --backend influxdb3 --scale laptop --label idb3-laptop

# 4. compare any runs, any backends
python bench.py compare "results/*/run.json" --out results/compare.md --csv results/compare.csv
```

## Scenarios (mapping to the evaluation plan)

| Scenario  | Plan | What is measured |
|-----------|------|------------------|
| `ingest`  | T1   | 2-day backfill: sustained lines/s, wall time, batch p50/p95/max, errors |
| `hosts`   | –    | host-fleet history: lines/s, errors |
| `query_a` | T3   | 20 single-product journey lookups: median/p95/max ms |
| `query_b` | T4   | the five cross-product aggregations, 3 runs each: cold/warm ms, rows, **completes-or-not** (the QuestDB/VictoriaMetrics OOM question) |
| `burst`   | T2   | 100K-events-at-once: wall s, events/s, errors, plus one Shape B query fired mid-burst |
| `storage` | T7   | data-dir bytes via `docker exec du`, GB/day, 90d/365d projections |
| `context` | –    | dataset sanity counts — **must match across backends** (same seed => same dataset); a mismatch means an adapter's queries aren't equivalent |

Throughout the run, a background thread samples `docker stats` for the
target container and tags every sample with the current phase — that's the
per-phase peak-memory evidence (`resources` section + `resources.csv`).

Run a subset with `--scenarios query_a,query_b` (e.g. re-query an already
loaded dataset), or `--scenarios storage` later for a settled storage
number after the WAL has been compacted to Parquet.

## Scales

| Preset  | Products/day | Days | Hosts | Burst | Purpose |
|---------|-------------:|-----:|------:|------:|---------|
| smoke   | 2,000        | 2    | 50    | 5K    | harness shakeout, ~30s |
| laptop  | 100,000      | 2    | 300   | 100K  | dry run (plan's guidance) |
| full    | 1,000,000    | 2    | 2,500 | 100K  | the real numbers, on the evaluation box |

Any preset value can be overridden (`--products-per-day`, `--burst-size`,
`--workers`, ...). **Compare runs only at the same scale.**

## Backends

```
python bench.py backends
```

| `--backend`       | Target | Launch |
|-------------------|--------|--------|
| `influxdb3`       | InfluxDB 3 Core/Enterprise | `docker compose -f compose/influxdb3.yml up -d` |
| `influxdb2`       | InfluxDB 2.x (v2 write API + Flux) | `docker compose -f compose/influxdb2.yml up -d` |
| `questdb`         | QuestDB (ILP/HTTP + REST SQL) | `docker compose -f compose/questdb.yml up -d` |
| `victoriametrics` | VictoriaMetrics (influx `/write` + MetricsQL) | `docker compose -f compose/victoriametrics.yml up -d` |
| `influxdb1`       | InfluxDB 1.x (the incumbent; InfluxQL) | `docker compose -f compose/influxdb1.yml up -d` |

**The `influxdb3` adapter is the validated reference** (its SQL is copied
verbatim from `influxdb3-poc/queries/run_query_tests.py`). The other three
are best-effort query translations and are flagged as such at runtime:
before trusting a comparison, run each at `--scale smoke` and check the
`context` values and per-query `rows` against the influxdb3 run — equal
counts is the cheap proof the queries are semantically equivalent.

## Output

Each run writes `results/<run-id>/`:

- **`run.json`** — the normalized record: backend + version + image,
  full config, environment, and a `metrics` object with fixed keys
  (`ingest`, `host_ingest`, `query_shape_a`, `query_shape_b.queries.B1..B5`,
  `burst`, `resources`, `storage`, `context`). This file is the
  comparison contract.
- `query_details.csv` — every individual timed query (test, run, ms, rows,
  status, error verbatim).
- `resources.csv` — timestamped cpu%/mem samples tagged by phase.

`compare` accepts run.json paths, run dirs, or globs, sorts by start time,
and emits a markdown (and optionally CSV) table of every canonical metric —
including per-query cold/warm latencies, peak memory per phase, and the
"Shape B all completed" verdict that decides the evaluation.

## Fairness and caveats

- **Same data everywhere.** Generation is seeded; a given scale produces an
  identical dataset on every backend and every run. Timestamps are relative
  to "now", and all query windows are relative (last 24/48h), so runs are
  comparable across days.
- **Errors are results.** Query failures (OOM, timeout, guardrail) are
  recorded verbatim, never retried silently — "completes at all" is the
  headline metric for Shape B. Writes retry 6x with backoff (matching the
  PoC loadgen) before counting as an error.
- **VictoriaMetrics** is configured (compose file) with raised
  `-search.max*` limits on purpose, so the high-cardinality funnel is
  genuinely attempted rather than failing fast on a guardrail. Its
  `-search.latencyOffset` (default 30s) hides the newest ~30s of data —
  irrelevant at 24h windows, visible in smoke-scale sanity counts.
- **InfluxDB 1.x** funnel queries use the InfluxQL subquery idiom (inner
  `GROUP BY product_id`); expect pain at full cardinality — that finding is
  the baseline, not a harness bug. The compose file uses the `tsi1` index.
- **Resource sampling** via `docker stats` yields a sample every ~2s;
  phases that finish faster than that (small scales) may have no samples.
  At laptop/full scale every phase gets solid coverage.
- **Storage right after ingest** may not reflect final on-disk layout
  (e.g. InfluxDB 3 WAL not yet compacted to Parquet) — re-run
  `--scenarios storage` a few minutes later for the settled number.
- The Python generator can be the bottleneck at full scale (same as the
  PoC loadgen) — if reported lines/s looks low while the server idles,
  raise `--workers`.

## Adding a backend

Subclass `backends/base.py::Backend` (~100 lines: a write endpoint, the
five Shape B queries in the engine's language, Shape A, health/version,
data dir for storage measurement), register it in `backends/__init__.py`,
add a compose file under `compose/`, then validate at smoke scale against
the influxdb3 reference as described above.

## Provenance

Vendored into TimeLakeDB from the `project-time-lord-db` evaluation repo
(tsdb-bench), where it benchmarked five engines on this exact workload.
The recorded InfluxDB 3 baselines in `results/` are the numbers TimeLakeDB
must beat (`../REQUIREMENTS.md` §10). The workload generator is
deterministic — datasets are identical across engines and runs — so
comparisons against the recorded baselines stay valid.
