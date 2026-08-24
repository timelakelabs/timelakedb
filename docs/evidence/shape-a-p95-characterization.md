# Shape A p95 — characterisation (#69)

2026-08-24. Phase 1 of #68: find where a cold Shape A lookup spends its time
*before* building a fix, and add the observability to see it. The headline is
that #70's premise, inherited from M4, is stale.

## The M4 premise is wrong: L0 files DO have entity blooms

M4's carve-out reasoning was "the arrow writer emits no bloom filters for
dictionary columns, so Shape A can't prune fresh (L0) data by `product_id`; we
cluster settled files instead." The code contradicts it:

- `buffer::flush::to_parquet_bytes_rg` **explicitly enables bloom filters** on
  every dictionary column with ≥ `BLOOM_MIN_DISTINCT` (1024) distinct values
  (`set_column_bloom_filter_enabled`). `product_id` (2M/day) far exceeds it.
- This runs on **both** paths — L0 flush (`to_parquet_bytes`) and compaction —
  so L0 files carry `product_id` blooms too.
- The buffer's own test `dict_columns_do_get_blooms` proves every row group
  gets one. The comments claiming otherwise (buffer, compact) were stale and
  are corrected in this change.

The arrow writer emits no blooms for dict columns *by default* — but the
writer is asked, so it does. So `provider::bloom_keep_row_groups` **can** and
**does** exclude L0 row groups that don't hold a `product_id`. The 608 ms was
measured before blooms-on-dict landed.

## The real gap: L0 uses COARSE row groups

Blooms and stats prune at **row-group granularity**. The two paths size groups
very differently:

- **Compaction** writes 64K-row groups (`to_parquet_bytes_rg(_, Some(65536))`)
  — fine grain.
- **L0 flush** leaves `rg_rows = None` → the writer's **default (coarse)** size
  (~1M rows).

So on L0 a bloom correctly excludes a group that lacks the pid, but a group
that *holds* it is huge — a lookup decodes ~1M rows to return the handful that
match. That read amplification on the fresh path is the live hypothesis for
the cold p95, and it is a row-group-sizing gap, **not** a missing bloom.

## Observability added (this change)

Eight counters on `/metrics`, from a new `provider::ScanStats` threaded like
`filtered_rows`, so the breakdown is a metric, not a profiler session:

```
timelake_scan_files_considered_total
timelake_scan_files_time_pruned_total
timelake_scan_row_groups_considered_total
timelake_scan_row_groups_stats_pruned_total   # by min/max — ~0 on unclustered L0
timelake_scan_row_groups_bloom_pruned_total   # by bloom   — the L0 mechanism
timelake_scan_row_groups_scanned_total        # decoded    — the number to shrink
timelake_scan_meta_cache_hits_total           # warm footer
timelake_scan_meta_cache_misses_total         # cold footer
```

`considered = stats_pruned + bloom_pruned + scanned` per file, so a single
lookup on an idle node reads as the delta and names its own dominant cost.

## Demonstrated (representative scale)

`provider::tests::scan_stats_attribute_pruning_and_prove_l0_blooms_work`: an
unclustered, L0-shaped file (pids scattered across time) written with SMALL
row groups and blooms. A present pid scans **1** of ~78 groups (blooms prune
the other 77; stats prune 0 — ranges are wide); an absent pid scans 0; the
footer is a cold miss then a warm hit; the counter arithmetic ties out. This
proves L0 blooms exclude by entity at fine grain — i.e. **the fix is to make
L0's groups fine**, the lever `Some(65536)` already pulls for settled files.

## Outstanding (scopes #70)

The one thing this note does NOT establish is the full-scale **p95 magnitude**
— that needs a cold, full-scale Gauge run (`../Gauge/`), which is the
confirmation step, not the diagnosis. The diagnosis stands on the code and the
representative test:

> **#70 is re-scoped:** not "add blooms to L0" (they are already written) but
> **"flush L0 with finer row groups"** so a present-entity lookup reads a small
> group instead of a coarse one. Confirm with a full-scale Gauge run, watching
> `timelake_scan_row_groups_scanned_total` per lookup fall and the p95 with it,
> and measure the flush cost (finer groups = more metadata) against RR-1/PR-1.
