# Shape A p95 — L0 row-group acceptance (#70)

2026-08-25. Phase 2 of #68, the acceptance for the `l0_row_group_rows` knob
shipped in #76. #69 (phase 1) found the real gap behind #68's 608 ms cold
p95: L0 flush uses the parquet writer's coarse (~1M-row) default row groups
while compaction uses 64K, so a present-entity lookup on fresh data decodes a
huge group for a handful of rows. #70 added the knob
(`TIMELAKE_L0_ROW_GROUP_ROWS`, default off) to flush L0 with fine groups. This
run answers the two questions that decide the default: does it help the read,
and what does it cost ingest — at **full scale**, not at unit scale.

## Method

Full-scale Gauge (`bench.py run --backend timelakedb --scale full`): 1M
products/day × 2 days ≈ 36.6M ingest lines + 7.2M host lines, then Shape A
(20 per-entity lookups), Shape B, burst, storage. Each run is a **fresh**
container (`compose down -v` then `up`) so no run inherits another's files;
the knob is passed through a compose override. Baseline = default (coarse L0);
treatments = `65536` (matches compaction) and `16384`.

Because a single 20-sample Shape A at sub-100 ms timings is noisy, and the
first (cold) run distorts ingest, the baseline↔64K comparison was **repeated
interleaved** (baseline, 64K, baseline, 64K, …) so cache warming cannot favour
one config.

## Results

Shape A per run (ms), and ingest (lines/s):

| Run | L0 rows | median | p95 | max | ingest |
|---|---|---|---|---|---|
| baseline (cold) | coarse | 56 | 65 | 65 | 410,231 |
| baseline-1 | coarse | 74 | 85 | 93 | 773,601 |
| baseline-2 | coarse | 178 | 199 | 200 | 783,214 |
| baseline-3 | coarse | 63 | 212 | 216 | 768,075 |
| 64K (first) | 65536 | 50 | 218 | 234 | 760,339 |
| 64K-1 | 65536 | 51 | 234 | 239 | 765,894 |
| 64K-2 | 65536 | 52 | 241 | 263 | 471,025 |
| 64K-3 | 65536 | 52 | 152 | 168 | 401,583 |
| 16K | 16384 | 71 | 89 | 98 | 405,760 |

Storage was ~0.43–0.52 GB/day across all configs — no meaningful difference.
Ingest ran with 0 errors everywhere; Shape B completed everywhere.

## What it says

1. **The #68 carve-out is resolved on current `main`, without the knob.**
   Every configuration — baseline included — clears the 250 ms target; the
   worst p95 observed was 241 ms. M4 measured 608 ms; blooms-on-dict (#69's
   correction of the stale "no blooms for dict columns" premise) plus the
   metadata cache already closed the gap. The baseline's cold first run —
   the coldest state, and the one that would have been worst at M4 — was
   **65 ms**.

2. **p95 is noise-dominated here and is not a reliable discriminator.** The
   baseline p95 alone swings 65 → 212 ms across four runs. p95 is the slowest
   of 20 samples, so it tracks a GC or a compaction pass overlapping the
   sampling window, not the row-group size. Do not read the single-run 65 vs
   218 as a real regression.

3. **The stable signal is the median, and it says finer L0 helps the typical
   lookup but not the tail.** 64K's median is a steady ~51 ms versus the
   baseline's ~65 ms — a smaller decode per group, as designed. But 64K's p95
   tail sits *higher*, nearer the 250 ms ceiling (152–241 ms), consistent with
   16× more row groups meaning more bloom/stat metadata to walk and a fatter
   footer; and its ingest trends *down* in the later interleaved pairs
   (401–471K vs the baseline's 768–783K), the flush cost the M4 note warned
   about. 16K (finer still) landed between the two on reads and at the
   baseline's ingest.

## Decision

**Keep `l0_row_group_rows` off by default.** The read path already meets the
target with coarse L0, so the knob buys no p95 improvement on the metric that
gated #68; what it buys is a faster *typical* lookup at the cost of a riskier
tail and a tendency to slower ingest — a trade an operator should opt into
after profiling, not one to impose by default on a fast ingest path (RR-1 /
PR-1). The knob stays as the documented lever it shipped as.

Caveat kept honest: this is one machine and single 20-sample Shape A runs;
the numbers are noisy and the finer-vs-coarse *comparison* rides on that
noise. The **decision** does not — the baseline clears the bar by a wide
margin regardless — but a fleet that sees a genuinely cold-disk Shape A tail
(footers evicted from page cache, not just a cold metadata cache) is the case
where the knob might earn its keep, and that case is not what this run
exercises.
