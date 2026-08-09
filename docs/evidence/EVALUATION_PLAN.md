> **Evidence snapshot** — copied 2026-08-08 from the original evaluation repository; paths mentioned inside refer to that repo's layout.

# InfluxDB 3 Test & Evaluation Plan

**Purpose:** Prove, with our own data shapes and load patterns, that InfluxDB 3 can replace our end-of-life InfluxDB 1.x deployment before we commit to a migration and deployment decision.

---

## 1. Background and decision context

We run InfluxDB 1.x today. It handles our write load, but backups take 10–15 minutes, which forced us to restrict how much data we store. InfluxDB 1.x is no longer supported, so we need a forward strategy.

We already trialed QuestDB and VictoriaMetrics. Both failed the same way: out-of-memory errors on queries that aggregate across our high-cardinality `product_id` dimension (~1M unique IDs per day). That failure mode — not raw ingestion — is the thing this evaluation must specifically disprove for InfluxDB 3.

InfluxDB 3's architecture is different in the ways that matter to us: a columnar engine (Apache DataFusion) over Parquet files in object storage, native SQL, high-cardinality tags as dictionary-encoded columns rather than an exploding series index, and full compatibility with the v1/v2 line-protocol write APIs, so our push-based ingestion barely changes.

**Decision this evaluation feeds:** adopt InfluxDB 3 (and if so, which edition/deployment: self-hosted Enterprise vs. Cloud Dedicated) — or eliminate it and widen the search.

## 2. What the evaluation must prove

| # | Objective | Why it matters |
|---|-----------|----------------|
| O1 | Sustained ingestion at our real rate with headroom | ~1M products/day × ~20 events + 2,500 hosts of system metrics |
| O2 | Absorb 100K-event bursts without error or instability | Our real ingest pattern is bursty, not steady |
| O3 | Fast single-product journey lookups (Shape A) | The "where is product X in the pipeline" interface |
| O4 | Cross-product aggregations complete with bounded memory (Shape B) | The exact query shape that OOM-killed QuestDB and VictoriaMetrics |
| O5 | Grafana connects and renders the funnel + host dashboards | Validates the real read path end to end |
| O6 | Backup of the full dataset is fast and restorable | The original 10–15 minute pain point |
| O7 | Storage footprint supports a viable retention strategy | 1 year target, 90-day-hot fallback acceptable |

## 3. Workload profile being simulated

| Dimension | Value |
|---|---|
| Products per day | 1,000,000 (unique `product_id` values) |
| Lifecycle | 10 sequential steps, start + stop event each ≈ 20 events/product |
| Pipeline event volume | ~20M events/day ≈ 232 events/s average |
| Burst pattern | Up to 100,000 events dropped at once |
| Host fleet | 2,500 hosts × (cpu/mem/disk/net rollup + 7 disk devices) every 10 s ≈ 2,000 points/s |
| Query windows | 1–2 days for both query shapes |
| Retention target | 1 year preferred; 90 days hot acceptable if the design calls for it |
| Ingestion model | Push, line protocol (unchanged from 1.x) |

The two query shapes, precisely:

- **Shape A — single-product journey.** Given one `product_id`, return its full ordered event history over the last 1–2 days. Needle-in-haystack; wants indexing/pruning, not scanning.
- **Shape B — cross-product aggregation.** Funnel counts per step (distinct products), in-flight (started minus stopped) per step, hourly throughput, and route rollups — across the full 1–2M distinct product IDs in the window. Columnar scan + streaming aggregation; this is the QuestDB/VictoriaMetrics killer.

## 4. Test environment

Everything runs from the Docker Compose stack in this folder (see `README.md` for commands):

| Service | Role |
|---|---|
| `influxdb3` | InfluxDB 3 **Core** (free), single node, file object store, auth disabled for the PoC |
| `backfill` / `steady` / `burst` | Python load generator producing the product lifecycle in three modes |
| `hosts` | Host-fleet metrics simulator (random-walk cpu/mem/disk/net) |
| `querytest` | Timed runner for Shape A and Shape B; writes `results/query_results.csv` |
| `grafana` | Pre-provisioned InfluxDB 3 SQL datasource + funnel and host dashboards |

Core is the right engine for this phase because our operational query windows (1–2 days) sit inside its per-query Parquet-file budget (`query-file-limit`, default 432 files ≈ 72 h of data at the default 10-minute file granularity). Section 7 covers why long retention pushes us to Enterprise or Cloud Dedicated — that is a *deployment* decision, and the engine behavior we are validating here is the same family.

**Data model** (deliberate tag/field choices — this answers the schema question):

```
pipeline_events,product_id=<id>,step=<01-download..10-upload>,event=<start|stop>,route=<name>,worker_ip=<172.16.step.n>
    value=1i | duration_s=<float>            <ns timestamp>

host_metrics,host=<host0000..>
    cpu_pct=,mem_pct=,disk_pct=,net_rx_bps=i,net_tx_bps=i   <ns timestamp>

disk_metrics,host=<host0000..>,device=<nvme0n1|nvme1n1|sda..sde>
    capacity_gb=i,used_gb=,used_pct=,read_bps=i,write_bps=i <ns timestamp>
```

`product_id` is a **tag** even at 1M/day cardinality — in InfluxDB 3 tags are dictionary-encoded columns, so this enables pruning for Shape A without the series-explosion penalty of 1.x. `step`, `event`, and `route` are low-cardinality tags used for grouping; `worker_ip` (~500 values, one /24 per step in the 172.16 private space) records which worker processed the step. Durations and gauge values are fields. Part of the evaluation is confirming this modeling holds up under both query shapes.

**Test box guidance:** run the full-scale test on hardware representative of production (≥8 cores, ≥32 GB RAM, SSD/NVMe). The generators are configurable in `.env`; a laptop dry run at `PRODUCTS_PER_DAY=100000, HOSTS=300` validates the harness before the real run.

## 5. Test scenarios

Run in order. Keep `scripts/monitor.sh` sampling server CPU/memory in a second terminal for T2–T4; its CSV is your memory evidence.

### T1 — Bulk ingestion (backfill)
**Method:** `backfill` writes 2 days × 1M products (~40M events) at maximum speed and reports sustained lines/s.
**Pass:** zero failed writes; sustained rate ≥ 10× the 232 events/s live average (i.e., ample headroom); server stays healthy throughout.
**Record:** lines/s, wall time, error count.

### T2 — Burst absorption
**Method:** with `steady` + `hosts` running, fire `burst` (100,000 events at once). Watch `monitor.sh`; run one Shape B query mid-burst to check read responsiveness.
**Pass:** all 100K events accepted with zero errors; end-to-end ≤ 30 s; no restart/OOM; concurrent query still answers.
**Record:** wall time, events/s, batch latency p95, peak memory during burst.

### T3 — Shape A: single-product journey
**Method:** `querytest` samples 20 real product IDs and times the journey lookup for each.
**Pass:** p95 ≤ 2 s (target: sub-second warm); correct, complete, time-ordered event history.
**Record:** median/p95/max latency.

### T4 — Shape B: cross-product aggregation ← **the make-or-break test**
**Method:** `querytest` runs five aggregations (24 h and 48 h funnels with `COUNT(DISTINCT product_id)`, in-flight per step, hourly throughput, route rollup), 3 runs each (cold → warm).
**Pass:** **every query completes — no OOM, no error** (this is exactly where Quest/Victoria failed); 24 h funnel ≤ 15 s cold / ≤ 5 s warm; server memory stays bounded (peaks within the configured query memory pool, default 20% of RAM, and returns to baseline).
**Record:** per-query latencies, peak memory, any errors verbatim.

### T5 — Grafana end-to-end
**Method:** open http://localhost:3000 (admin/admin). Confirm the datasource "Save & test" passes, and both dashboards render: the **funnel** (products per step, in-flight vs processed — the 10-step dashboard we run today) and **host metrics** (fleet averages + per-host drilldown).
**Pass:** connection healthy over Flight SQL; panels populate; refresh doesn't destabilize the server.
**Record:** any panel-level latency issues at realistic refresh intervals.

### T6 — Backup and restore
**Method:** with the 2-day dataset loaded, run `scripts/backup_test.sh` (timed snapshot of the object store), then perform the restore drill it prints and re-run `querytest` against the restored data.
**Pass:** backup of the full 2-day dataset in ≤ 2 minutes (versus 10–15 minutes today on a restricted dataset); restore verified by passing T3/T4 again.
**Record:** data size, archive size, backup duration, restore duration.

### T7 — Storage footprint and retention projection
**Method:** after T1, read `du -sh data/influxdb3` and the backup CSV; compute GB/day.
**Pass:** informational — feeds Section 7's retention decision.
**Record:** GB/day → ×365 (full-year) and ×90 (hot-tier) projections.

### T8 (optional) — v1/v2 write-API compatibility
**Method:** POST a line-protocol sample to the v1-style `/write?db=poc` and v2-style `/api/v2/write` endpoints using our existing collector's request format.
**Pass:** 2xx accepted; data queryable. Confirms the "ingestion barely changes" migration story.

## 6. Success metrics summary (fill in during the run)

| Metric | Target | Actual | Pass? |
|---|---|---|---|
| Backfill sustained ingest | ≥ 2,500 lines/s (10× headroom) | 73,170 lines/s (36.58M events in 500 s, 0 errors) | ✅ 29× |
| Burst: 100K events accepted | ≤ 30 s, 0 errors | 1.14 s, 0 errors (87,574 events/s) | ✅ |
| Peak memory during burst | Bounded, returns to baseline | 6.1 GB peak of 16.3 GB VM; no OOM; settles at ~5 GB working set (caches), not the 67 MB cold baseline | ✅* |
| Shape A journey lookup p95 | ≤ 2 s | 607 ms (median 520 ms, max 1.14 s, n=20) | ✅ |
| Shape B 24 h funnel (cold / warm) | ≤ 15 s / ≤ 5 s, **no OOM** | 5.7 s / 5.8 s, no OOM (48 h funnel: 16.4 s; B4 hourly 48 h: 30 s) | ✅* |
| Shape B all queries complete | 100% (0 errors) | 100% — 15/15 runs, 0 errors; concurrent funnel during burst answered in 5.9 s | ✅ |
| Grafana datasource + dashboards | Healthy / rendering | Flight SQL healthy; 4 dashboards rendering (funnel, host fleet, host detail, product journey) | ✅ |
| Backup of 2-day dataset | ≤ 2 min | **47 s** (2.3 GB data dir → 1.0 GB tgz) vs 10–15 min on 1.x today | ✅ |
| Restore verified | querytest passes post-restore | 6 s extract, server healthy, full suite passed: Shape A ~1 s, every Shape B ≤ 2.3 s, 0 errors | ✅ |
| Storage per day | (record) GB/day | 1.57 GB/day right after ingest; **1.15 GB/day settled** (2.3 GB by backup time) → 90 d ≈ 104 GB, 365 d ≈ 420 GB | n/a |

Latency targets are starting points, not vendor promises — record actuals and judge them against the interface's real needs.

*Full-scale run 2026-08-07, dev box (24 cores, Docker VM 16.3 GB), run id `influxdb3-idb3-full-20260807-223308` (details in `benchmark/results/`). Warm 24 h funnel (5.8 s) narrowly misses the 5 s starting-point target and B4 (hourly throughput, 48 h) is the slowest shape at 30 s — both are Phase 2 tuning candidates (Enterprise compaction, Distinct Value Cache). Memory peaked at 9.2 GB during query phases with zero query failures.*

*Important follow-up finding from the T6 restore verification (~1 h after ingest, once the WAL had settled into Parquet): the same Shape B queries ran 5–26× faster — 24 h funnel 1.1 s, 48 h funnel 2.3 s, B4 hourly 1.1 s, all ≤ 2.3 s. The benchmark's Shape B numbers are the worst case immediately after a max-speed backfill; steady-state performance is comfortably within every target, and the storage footprint likewise settled from 3.15 GB to 2.3 GB.*

## 7. Retention strategy and edition decision

The engine result from T1–T5 transfers across editions; retention and HA are where the editions diverge.

**Core (free, this PoC):** stores unlimited history, but each *query* is capped by `query-file-limit` (default ≈ 72 h of Parquet files). Our operational 1–2 day queries fit comfortably. The limit can be raised, at the cost of query performance and memory — not a year-long-retention answer.

**Enterprise (paid; self-hosted):** adds the compactor that rewrites small Parquet files into large, indexed generations — this is what makes fast queries over long historical ranges viable. Recent releases also add high availability (multi-node), in-place and incremental backup/restore, and read replicas. A time-limited trial license makes this testable as Phase 2 (a commented-out service is in `docker-compose.yml`).

**Cloud Dedicated (managed):** same engine family, InfluxData operates it; retention, backup, and HA become their problem.

Retention options to decide between after T7's numbers:

1. **Full year in the database** (Enterprise or Cloud Dedicated). Old data is cold Parquet in object storage — it doesn't bloat a live engine the way 1.x TSM did, and "backup" becomes object-store replication/versioning plus (on Enterprise) built-in incremental backup. Likely feasible; cost is mostly storage.
2. **90 days hot + Parquet archive.** Set a 90-day retention period on the database and archive aged Parquet from object storage to cheap cold storage. Cleanest operational profile; historical reads beyond 90 days go through a slower recall path.

Either way, the 10–15 minute monolithic-backup problem disappears structurally: the data *is* files in an object store.

**Production deployment note:** a single instance means a single point of failure, which we've said is unacceptable for production. The PoC deliberately defers that; the Phase 2 decision is Enterprise-with-HA (self-hosted) vs. Cloud Dedicated, informed by these results plus cost.

## 8. Out of scope for this phase

High availability and failover testing, security hardening (the PoC runs with auth disabled), TLS, real client cutover, InfluxQL parity checks for existing dashboards, and network-attached object stores (S3/MinIO — the file store is fine for engine behavior; an optional MinIO variant is a small compose change if we want backup-via-object-store-replication demonstrated literally).

## 9. Risks and open questions

- `COUNT(DISTINCT product_id)` over ~2M IDs is the heaviest memory shape — if warm latency disappoints on Core, retest on the Enterprise trial (compaction materially changes large-scan behavior) before drawing conclusions.
- If the funnel dashboard needs sub-second refresh at production cardinality, evaluate InfluxDB 3's Distinct Value Cache / Last Value Cache as a tuning lever in Phase 2.
- Burst behavior under *sustained* repeated bursts (e.g., one per minute for an hour) is worth a follow-up run if the single-burst test passes easily.
- Confirm current Enterprise pricing/licensing and Cloud Dedicated sizing before the Phase 2 decision.

## 10. Exit criteria

**Proceed to Phase 2 (Enterprise trial / deployment design)** if T1–T6 pass, in particular T4 with zero OOMs. **Stop and reassess** if Shape B cannot complete within memory on evaluation hardware — that would mean the third engine has failed on the same workload and the schema/serving-layer design (e.g., pre-aggregation) needs rethinking regardless of database choice.
