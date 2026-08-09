# Performance enhancement log

A scheduled agent proposes one performance idea per cycle, implements it,
measures it against a paired baseline, and records the outcome here —
**whether it worked or not**. Failures are the more valuable half: they mark
the paths already explored so the next cycle does not walk them again.

The harness is the referee. Every number below comes from
`bench.py run --backend timelorddb`, never from an ad-hoc measurement, and
every entry names the run labels so the raw records in `bench/results/` can be
re-read.

## Protocol

1. **Read this log first.** Do not repeat an idea already listed as tried,
   unless the entry says why a retry is worthwhile.
2. **One idea per cycle**, stated as a hypothesis with a predicted direction
   and magnitude before any measurement.
3. **Paired runs in the same cycle** on an isolated instance: baseline from
   `master`, then the candidate, both at `--scale laptop` after a `--scale
   smoke` shakeout. Comparing against an older recorded run is not evidence —
   machine conditions drift.
4. **A win must clear the noise floor** and keep the workspace green:
   `cargo test --workspace`, plus `fmt`/`clippy` on the touched files.
5. **Outcome lands on `master` either way** — the change itself when it wins,
   the lessons when it does not.

## Entry format

```
### YYYY-MM-DD HH:MM — <short title>            [ADOPTED | REJECTED | ABORTED]
**Hypothesis.**  what should get faster, why, and by roughly how much
**Change.**      what was actually modified (files, mechanism)
**Measurement.** baseline vs candidate, run labels, the metrics that moved
**Verdict.**     the decision and the reasoning behind it
**Lesson.**      what the next cycle should take from this
```

`ADOPTED` — measured a real win, committed. `REJECTED` — no win, or a
regression, or it broke a test; code reverted, lesson kept. `ABORTED` — the
cycle could not complete (build failure, environment problem); says what
blocked it.

## Standing leads

Carried from `CLAUDE.md` and the M4/M5 carve-outs, as starting material:

- **Streaming / range reads.** `LazyTable` loads whole objects; range reads
  over the store would cut both latency and peak memory. The largest known
  lead.
- **Ingest decline under maintenance contention** (intra-run, no cross-run
  decay) — isolation between the maintenance tick and the write path.
- **Shape A p95 608 ms against a 250 ms target**, even with the metadata
  cache making the median 0–6 ms warm.
- **Compaction clustering choice** — currently the highest-cardinality
  dictionary column; is that optimal for the funnel queries too?
- **Flush sizing and Parquet row-group geometry** against the pruning that
  reads them back.

---

## Entries

### 2026-08-08 22:10 — Cluster L0 by entity and bound its row groups   [REJECTED]

**Hypothesis.** L0 files are written time-first in one default-sized row
group (the arrow writer's million-row default, against files of ~32K rows), so
`stats_keep_row_groups` can never prune *inside* a fresh file — only whole
files fall to time bounds. Compaction already clusters by the
highest-cardinality tag and writes 64K groups for exactly this reason. Give L0
the same treatment and a single-entity lookup should skip most of each file it
opens: predicted Shape A median well below the ~53 ms baseline, at a small cost
in ingest and storage.

**Change.** Extracted compaction's self-tuning cluster-column pick into
`buffer::flush::cluster_column`, used it from `merge_files` and from
`Engine::flush_one`, and wrote L0 through `to_parquet_bytes_rg(.., Some(8192))`
instead of the unbounded `to_parquet_bytes`. Three files, 49 tests still green,
no new fmt/clippy findings.

**Measurement.** Paired runs, `--scale laptop --shape-a-samples 100`, each on a
fresh volume and container, isolated instance on :2965. Baselines
`perf-base3`/`perf-base4`, candidates `perf-cand1`/`2`/`3`.

| Metric | Baseline | Candidate |
|---|---|---|
| Shape A median | 55, 52 ms | 53, 57, 55 ms |
| Shape A p95 | 111, 107 ms | 131, 72, 69 ms |
| B1 funnel cold | 57, 64 ms | 94, 105, 112 ms |
| Ingest | 575K, 591K lines/s | 545K, 549K, 580K lines/s |

**Verdict.** Rejected. Shape A did not move at all — every candidate median sits
inside the baseline's own spread. Against that nothing, the funnel regressed
about 70% in three runs out of three, with no overlap between the two sets, and
ingest drifted down. One candidate run also reported 4 Shape A errors that did
not recur in two further runs and left no trace in the container log; unexplained,
and not the reason for rejection, but recorded.

**Lesson.** *Row-group pruning cannot pay while the reader still fetches whole
objects.* `LazyTable` calls `store.get(&path)` (provider.rs:276 and :304) to pull
the entire file into memory and only then applies `with_row_groups(keep)`, which
limits **decoding**, not I/O. So finer groups buy back CPU that was never the
bottleneck, while every scan pays more footer metadata and every write pays an
extra sort key — which is precisely the shape of the numbers above. **Range reads
in the store are a prerequisite for this idea, not a parallel lead.** Do not retry
entity-clustered L0 until `Store` can fetch a byte range; then re-run this exact
experiment, because the mechanism is sound and only the I/O layer defeats it.

Three things about the measurement rig, for whoever runs the next cycle:

- **Laptop scale never compacts.** `timelord_compactions_total` was 0 with 114
  L0 files — the whole run finishes in ~20 s and the compaction tick is 30 s. It
  is therefore a clean measurement of the fresh-data path and completely blind to
  anything about compaction. Use it deliberately, or drive compaction explicitly.
- **The storage metric is unusable at this scale.** Identical baseline runs
  reported 0.09, 0.20, 0.36 and 0.40 GB, because it is sampled right after ingest
  with a variable amount still unflushed. Ignore it below full scale.
- **Shape A needs `--shape-a-samples 100`.** At the default 20 the median swung
  54→82 ms between identical runs; at 100 the same configuration repeats within
  3 ms. Every number above uses 100.
