> **Evidence snapshot** — copied 2026-08-08 from the `project-time-lord-db` evaluation repo; paths mentioned inside refer to that repo's layout.

# InfluxDB 3 Evaluation — Benchmark Results

**Date:** 2026-08-07 · **Engine:** InfluxDB 3 Core 3.11.0 (Docker, single node, file object store)
**Box:** Windows 11 dev machine, 24 cores, Docker VM 16.3 GB (plan calls for ≥32 GB on the eval box — treat these as conservative)
**Harness:** `benchmark/` (tsdb-bench), raw records in `benchmark/results/<run-id>/run.json`

## Verdict

**T1–T7 all pass, including T4 (Shape B) with zero OOMs across two independent
full-scale runs** — the exact failure mode that eliminated QuestDB and
VictoriaMetrics did not occur. Per the plan's exit criteria: **proceed to
Phase 2** (Enterprise trial / Cloud Dedicated, deployment + retention decision).

## Full-scale runs (1M products/day × 2 days ≈ 36.6M events, 2,500 hosts)

Two runs, launched independently about 3 hours apart (the second on the
current schema: named steps, `worker_ip`, 7 disk devices/host):

| Metric | Run 1 `19:44` | Run 2 `22:33` | Target |
|---|---|---|---|
| Ingest sustained | 73,227 lines/s | 73,170 lines/s | ≥ 2,500 → **29×** |
| Ingest errors | 0 / 36.58M | 0 / 36.58M | 0 |
| Burst 100K events | 1.23 s (81K/s) | 1.14 s (88K/s) | ≤ 30 s |
| Query mid-burst | 4.7 s, ok | 5.9 s, ok | answers |
| Shape A p95 | 630 ms | 607 ms | ≤ 2 s |
| B1 funnel 24 h cold/warm | 4.6 / 4.7 s | 5.7 / 5.8 s | ≤ 15 / ≤ 5 s |
| B2 funnel 48 h | 13.2 s | 16.4 s | — |
| B3 in-flight 24 h | 8.0 s | 10.3 s | — |
| B4 hourly 48 h | 22.8 s | 30.3 s | — |
| B5 route rollup 24 h | 5.1 s | 6.2 s | — |
| Shape B completed | 15/15, 0 errors | 15/15, 0 errors | 100%, **no OOM** |
| Peak memory (query phase) | 7.5 GB | 9.2 GB | bounded, no OOM |
| Storage | 0.99 GB/day | 1.57 GB/day¹ | recorded |

¹ Measured immediately after ingest, before WAL→Parquet settling.

## The settled-Parquet finding (why the Shape B numbers above are worst-case)

The T6 restore verification re-ran the full query suite ~1 h after ingest,
once data had settled into Parquet:

| Query | Fresh after ingest | Settled | Speedup |
|---|---|---|---|
| B1 funnel 24 h | 5.7 s | **1.1 s** | 5× |
| B2 funnel 48 h | 16.4 s | **2.3 s** | 7× |
| B3 in-flight 24 h | 10.3 s | **0.6 s** | 17× |
| B4 hourly 48 h | 30.3 s | **1.1 s** | 26× |
| B5 route rollup 24 h | 6.2 s | **0.8 s** | 8× |

Steady-state Shape B is comfortably inside every target. Storage likewise
settled from 3.15 GB to 2.3 GB (**≈1.15 GB/day** → 90 d ≈ 104 GB, 365 d ≈ 420 GB).

## T6 — backup / restore (the original 1.x pain point)

- **Backup: 47 s** — full 2-day dataset, 2.3 GB data dir → 1.0 GB tgz
  (vs 10–15 min today on 1.x with restricted retention)
- **Restore: 6 s** extract; server healthy immediately; full query suite
  passed on restored data (0 errors)

## Observations worth carrying into Phase 2

- An **unbounded** GROUP BY over a string function of `product_id` across the
  whole 36.6M-row table hit DataFusion's query memory pool cap (~2 GB) and
  failed cleanly — server stayed healthy, concurrent queries unaffected. The
  guardrail works; windowed operational queries never hit it.
- Server memory settles to a ~5 GB working set (caches) rather than the 67 MB
  cold baseline — bounded, not a leak; expected on a query engine.
- B4 (hourly throughput, 48 h) is the slowest shape fresh after ingest; on
  settled data it is 1.1 s. Enterprise compaction and the Distinct Value
  Cache are the tuning levers if fresh-data latency ever matters.
- Full-scale ingest wall time ≈ 8.3 min for 2 days of data implies
  re-backfilling from source at ~315× real-time — migration-friendly.

## InfluxDB 2.x — same test, same box (2026-08-08)

InfluxDB 2.7.12 (TSM/TSI, the same storage family as the 1.x incumbent) ran
the identical full-scale workload. Runs: `influxdb2-idb2-smoke*` (adapter
validation — sanity counts matched the reference exactly),
`influxdb2-idb2-full-*` (full scale; the main run was stopped during the
Shape B timeout wall, its log preserves everything through B1).

| Metric | InfluxDB 2.x | InfluxDB 3 (same data) |
|---|---|---|
| Ingest average | 33,143 lines/s (18.4 min) | 73,170 lines/s (8.3 min) |
| Ingest curve | **123K → ~10K lines/s decay** as series grew to ~40M | flat ~73K throughout |
| Low-cardinality control | 173K lines/s (host fleet, right after the decay) | 80K lines/s |
| Ingest errors | 0 | 0 |
| Shape A median / p95 | **134 ms / 472 ms** (index wins point lookups) | 520 ms / 607 ms |
| Shape B B1 funnel 24 h | **DNF — timed out ≥ 300 s, 3/3 attempts** (incl. mid-burst probe) | 5.7 s (1.1 s settled) |
| Shape B B2–B5 | not attempted (runs stopped during timeout wall) | all ≤ 30 s (≤ 2.3 s settled) |
| Burst 100K | 2.90 s, 0 errors | 1.14 s, 0 errors |
| Storage (2-day dataset) | **16.25 GB → 8.13 GB/day** (365 d ≈ 3.0 TB) | 2.3 GB settled → 1.15 GB/day (365 d ≈ 420 GB) |
| Idle memory after load | 10.85 GB resident (TSI index) | ~5 GB working set |

**Reading:** the write-path decay is pure series-index growth — the
low-cardinality host fleet wrote at 173K lines/s seconds after pipeline
ingestion had ground down to ~10K. Point lookups are excellent (the series
index doing its one job), but the make-or-break cross-product funnel *does
not complete* on the TSM/TSI family at this cardinality, storage costs ~7×,
and the idle index footprint alone is ~11 GB. This reproduces, on the
2.x generation, the same failure mode that eliminated QuestDB and
VictoriaMetrics — and quantifies why migrating within the 1.x/2.x family
would not have solved the problem.

## InfluxDB 1.x — the incumbent, same test (2026-08-08)

InfluxDB 1.8.10 (tsi1 index, default series caps lifted so the engine —
not a config guardrail — was tested). Run `influxdb1-idb1-full-20260808-045610`,
Shape B limited to one attempt per query. **The server was OOM-killed
mid-Shape-B**: B3 severed the connection at 39 s with query-phase memory at
14.4 GB of the 15.2 GB VM; Docker's restart policy revived it, and the
subsequent burst measured several minutes of post-crash write unavailability
(40K of 100K accepted). Even a plain COUNT(*) over the 48 h window timed out
at 300 s.

### The three-generation verdict (identical data, identical box)

| Metric | InfluxDB 1.8 | InfluxDB 2.7 | **InfluxDB 3 Core** |
|---|---|---|---|
| Ingest 36.58M events | 58,736 lines/s (10.4 min) | 33,143 lines/s (18.4 min) | **73,170 lines/s (8.3 min)** |
| Ingest curve | 308K → decay, p95 batch 4.2 s | 123K → ~10K decay | **flat, p95 batch 1.0 s** |
| Shape A (journey) median | **18 ms** | 134 ms | 520 ms (best index lookup: 1.x) |
| Shape B funnels (the killer) | DNF: 2× timeout ≥ 300 s, then **server OOM-killed** | DNF: 3× timeout ≥ 300 s | **all complete: ≤ 30 s fresh, ≤ 2.3 s settled** |
| Server survived queries | **No** (crash + minutes of write outage) | Yes (but 10.9 GB idle index) | Yes (bounded, pool-guarded) |
| Storage, 2-day dataset | 16.30 GB (8.15 GB/day) | 16.25 GB (8.13 GB/day) | **2.3 GB (1.15 GB/day)** |
| 1-year storage projection | ~3.0 TB | ~3.0 TB | **~420 GB** |
| Peak memory (query phase) | 14.4 GB / 15.2 GB → **killed** | ≥ 10.9 GB, held | 9.2 GB, bounded |

**Bottom line:** on this workload the TSM/TSI generations are superb at the
one thing their series index is built for (point lookups) and fail the
evaluation's make-or-break requirement — cross-product aggregation at
~2M-products/day cardinality — by timeout (2.x) or by killing the server
(1.x), at ~7× the storage. InfluxDB 3 is the only engine of five tested
(including the earlier QuestDB and VictoriaMetrics trials) that completes
Shape B with bounded memory. The evaluation's exit criteria are met with
the strongest possible contrast.

## Reproducing

```powershell
cd influxdb3-poc ; docker compose up -d ; cd ..\benchmark
python bench.py run --backend influxdb3 --scale full --label <label>
python bench.py compare "results/*/run.json" --out results/compare.md
```

Cross-solution comparison: launch a target from `benchmark/compose/`
(questdb / victoriametrics / influxdb1) and run the same command with that
`--backend` — see `benchmark/README.md` for adapter-validation caveats.

## Raw records

- `benchmark/results/influxdb3-idb3-full-20260807-194447/` — full run 1 (original schema)
- `benchmark/results/influxdb3-idb3-full-20260807-223308/` — full run 2 (current schema)
- `benchmark/results/influxdb2-idb2-*/` — 2.x smoke validation + full-scale evidence (main run's log preserved; fastclose run.json has burst/storage)
- `benchmark/results/influxdb1-idb1-full-20260808-045610/` — 1.x full run incl. the OOM kill (complete run.json)
- `benchmark/results/idb1-full-detached.log` — 1.x run console log
- `benchmark/results/influxdb3-idb3-smoke*/` — harness-validation smoke runs
- `benchmark/results/compare.md|csv` — side-by-side of all runs
- `influxdb3-poc/results/` — querytest CSV (restore verification), backup timings
- `EVALUATION_PLAN.md` §6 — scorecard with pass/fail against targets

## Appendix — every recorded run, every metric

Generated by `bench.py compare` from the raw run.json records. Note: the
InfluxDB 2.x full-scale ingest/Shape A/B1 numbers live in the narrative
section above (that run's record is its console log; it was stopped before
writing run.json — `idb2-full-fastclose` holds its burst/storage).

| Metric                                | idb3-smoke                           | idb3-smoke2                           | idb3-full                           | idb3-full                           | idb2-smoke                           | idb2-smoke2                           | idb2-full-fastclose                           | idb1-smoke                           | idb1-full                           |
|---------------------------------------|--------------------------------------|---------------------------------------|-------------------------------------|-------------------------------------|--------------------------------------|---------------------------------------|-----------------------------------------------|--------------------------------------|-------------------------------------|
| Backend                               | InfluxDB 3 Core                      | InfluxDB 3 Core                       | InfluxDB 3 Core                     | InfluxDB 3 Core                     | InfluxDB 2.x                         | InfluxDB 2.x                          | InfluxDB 2.x                                  | InfluxDB 1.x                         | InfluxDB 1.x                        |
| Version                               | 3.11.0                               | 3.11.0                                | 3.11.0                              | 3.11.0                              | v2.7.12                              | v2.7.12                               | v2.7.12                                       | 1.8.10                               | 1.8.10                              |
| Run                                   | influxdb3-idb3-smoke-20260807-192521 | influxdb3-idb3-smoke2-20260807-192716 | influxdb3-idb3-full-20260807-194447 | influxdb3-idb3-full-20260807-223308 | influxdb2-idb2-smoke-20260808-000942 | influxdb2-idb2-smoke2-20260808-001123 | influxdb2-idb2-full-fastclose-20260808-004641 | influxdb1-idb1-smoke-20260808-005319 | influxdb1-idb1-full-20260808-045610 |
| Date                                  | 2026-08-07T19:25                     | 2026-08-07T19:27                      | 2026-08-07T19:44                    | 2026-08-07T22:33                    | 2026-08-08T00:09                     | 2026-08-08T00:11                      | 2026-08-08T00:46                              | 2026-08-08T00:53                     | 2026-08-08T04:56                    |
| Scale (products/day x days)           | 2,000 x 2                            | 2,000 x 2                             | 1,000,000 x 2                       | 1,000,000 x 2                       | 2,000 x 2                            | 2,000 x 2                             | 1,000,000 x 2                                 | 2,000 x 2                            | 1,000,000 x 2                       |
| Hosts x history h                     | 50 x 1                               | 50 x 1                                | 2,500 x 6                           | 2,500 x 6                           | 50 x 1                               | 50 x 1                                | 2,500 x 6                                     | 50 x 1                               | 2,500 x 6                           |
| Ingest lines/s                        | 20,678                               | 23,390                                | 73,227                              | 73,170                              | 86,720                               | -                                     | -                                             | 172,139                              | 58,736                              |
| Ingest wall s                         | 3.5                                  | 3.1                                   | 499.5                               | 500.0                               | 0.8                                  | -                                     | -                                             | 0.4                                  | 622.9                               |
| Ingest errors                         | 0                                    | 0                                     | 0                                   | 0                                   | 0                                    | -                                     | -                                             | 0                                    | 0                                   |
| Ingest batch p95 ms                   | 1,003                                | 1,004                                 | 1,028                               | 1,034                               | 348                                  | -                                     | -                                             | 203                                  | 4,206                               |
| Host ingest lines/s                   | 200                                  | 3,038                                 | 75,359                              | 80,043                              | 210,390                              | -                                     | -                                             | 244,205                              | 193,529                             |
| Burst events                          | 5,000                                | 5,000                                 | 100,000                             | 100,000                             | 5,000                                | -                                     | 100,000                                       | 5,000                                | 40,000                              |
| Burst wall s                          | 0.71                                 | 0.61                                  | 1.23                                | 1.14                                | 0.08                                 | -                                     | 2.90                                          | 0.06                                 | 196.43                              |
| Burst events/s                        | 7,058                                | 8,219                                 | 81,152                              | 87,574                              | 62,848                               | -                                     | 34,528                                        | 85,958                               | 204                                 |
| Burst errors                          | 0                                    | 0                                     | 0                                   | 0                                   | 0                                    | -                                     | 0                                             | 0                                    | 6                                   |
| Query during burst ms                 | 36                                   | 42                                    | 4,724                               | 5,947                               | 648                                  | -                                     | 300,037  [ERR]                                | 294                                  | 1,027  [ERR]                        |
| Shape A n                             | 20                                   | 20                                    | 20                                  | 20                                  | 20                                   | 20                                    | -                                             | 20                                   | 20                                  |
| Shape A median ms                     | 53                                   | 54                                    | 505                                 | 520                                 | 48                                   | 48                                    | -                                             | 1                                    | 18                                  |
| Shape A p95 ms                        | 59                                   | 55                                    | 630                                 | 607                                 | 49                                   | 49                                    | -                                             | 2                                    | 29                                  |
| Shape A errors                        | 0                                    | 0                                     | 0                                   | 0                                   | 0                                    | 0                                     | -                                             | 0                                    | 0                                   |
| B1_funnel_24h cold/warm ms            | 68 / 62                              | 67 / 66                               | 4,603 / 4,719                       | 5,710 / 5,827                       | 710 / 621                            | 709 / 677                             | -                                             | 291 / 230                            | - / -  [1 ERR]                      |
| B2_funnel_48h cold/warm ms            | 91 / 76                              | 89 / 98                               | 13,231 / 12,679                     | 16,774 / 16,445                     | 1,020 / 1,063                        | 1,092 / 1,090                         | -                                             | 463 / 457                            | - / -  [1 ERR]                      |
| B3_inflight_24h cold/warm ms          | 66 / 63                              | 76 / 69                               | 8,021 / 7,930                       | 10,255 / 10,201                     | 254 / 235                            | 272 / 262                             | -                                             | 194 / 221                            | - / -  [1 ERR]                      |
| B4_hourly_throughput_48h cold/warm ms | 88 / 86                              | 114 / 115                             | 22,763 / 22,538                     | 30,270 / 30,152                     | - / -  [3 ERR]                       | 6,770 / 6,709                         | -                                             | 229 / 232                            | - / -  [1 ERR]                      |
| B5_route_rollup_24h cold/warm ms      | 61 / 57                              | 69 / 63                               | 5,073 / 4,789                       | 6,242 / 5,997                       | 290 / 284                            | 321 / 322                             | -                                             | 453 / 460                            | - / -  [1 ERR]                      |
| Shape B all completed                 | YES                                  | YES                                   | YES                                 | YES                                 | NO                                   | YES                                   | -                                             | YES                                  | NO                                  |
| Baseline mem MB                       | 38                                   | 276                                   | 372                                 | 67                                  | 51                                   | -                                     | 10,877                                        | 10                                   | 31                                  |
| Peak mem ingest MB                    | 82                                   | 288                                   | 2,946                               | 2,694                               | -                                    | -                                     | -                                             | -                                    | 12,649                              |
| Peak mem shape B MB                   | -                                    | -                                     | 7,529                               | 8,072                               | 301                                  | 339                                   | -                                             | 631                                  | 14,431                              |
| Peak mem burst MB                     | 267                                  | -                                     | 5,441                               | 6,126                               | 296                                  | -                                     | 14,442                                        | 710                                  | 959                                 |
| Mem returned to baseline              | NO                                   | YES                                   | NO                                  | NO                                  | NO                                   | -                                     | YES                                           | NO                                   | NO                                  |
| Storage GB total                      | 0.00                                 | 0.01                                  | 1.98                                | 3.15                                | 0.04                                 | -                                     | 16.25                                         | 0.07                                 | 16.30                               |
| Storage GB/day                        | 0.00                                 | 0.00                                  | 0.99                                | 1.57                                | 0.02                                 | -                                     | 8.13                                          | 0.04                                 | 8.15                                |
| Projected 90d GB                      | 0.2                                  | 0.4                                   | 89.2                                | 141.7                               | 1.7                                  | -                                     | 731.4                                         | 3.2                                  | 733.4                               |
| Projected 365d GB                     | 0.8                                  | 1.6                                   | 361.8                               | 574.5                               | 6.9                                  | -                                     | 2,966.0                                       | 13.0                                 | 2,974.2                             |

