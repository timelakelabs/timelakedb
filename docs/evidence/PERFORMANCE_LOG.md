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

### 2026-08-08 23:50 — Range reads in the Store   [ADOPTED, on bytes read — latency was a wash]

**Hypothesis.** The previous cycle established that pruning cannot pay while
`LazyTable` calls `store.get()` for the whole object and `with_row_groups`
bounds only decoding. Give `Store` a range API, read the footer and then just
the surviving row groups, and both the bytes moved and the latency should fall.

**Change.** `Store::size` and `Store::get_range` (+ `LocalStore`), and a
`StoreFile` in the provider implementing Parquet's `ChunkReader` over them. The
scan now loads metadata via `ArrowReaderMetadata::load` (footer only), prunes,
prefetches exactly the byte spans the kept row groups occupy — coalescing
neighbours within 64 KB into one request — and decodes from those. The file
length comes from `FileMeta::size_bytes`, so no stat either. Warm-footer scans
reuse the cached metadata through `new_with_metadata`.

**Measurement.** Bytes: a unit test on a 228 KB clustered file reads **18,493
bytes — 8% of the file** — for a single-entity lookup, and asserts it stays
under half. That is the whole point, it is deterministic, and it is now
guarded. Latency, paired laptop runs (baseline n=4, candidate n=2 clean):

| Metric | Baseline | Candidate |
|---|---|---|
| Shape A median | 53–57 ms | 57, 57 ms |
| Shape A p95 | 72–78 ms | 69, 72 ms |
| B1 funnel cold | 54–67 ms | 52, 93 ms |
| Ingest | 596–605K lines/s | 594K, 578K lines/s |

**Verdict.** Adopted, with the reason stated plainly: **latency is a wash on
this hardware**, and the win is the 92% reduction in bytes read plus the seam
itself. On a local volume a whole-file read comes out of the page cache, so
moving less data buys nothing measurable — the syscalls saved and the syscalls
added roughly cancel. The value is that a scan no longer *needs* the whole
object, which is the difference between a viable and a ruinous S3 backend
(CL-1), and it is a precondition for anything that prunes harder. Re-measure
against real object storage before claiming a latency win.

**Also fixed here, and worth more than the performance work:** a scan that
pruned everything away built a source with **zero partitions**, so any plan
with an `ORDER BY` above it failed its sanity check —
`DataSourceExec: partitions=0 … does not satisfy distribution requirements:
SinglePartition` — and returned 400 instead of an empty result. It surfaced as
2–4 intermittent Shape A errors per 100 lookups in the benchmark. Latent in
master, and made common by sharper pruning. Now an empty partition, with a test
that fails without the fix.

**Lesson.** Two things for the next cycle:

- **Bytes moved and time taken are different metrics, and this rig only
  resolves one of them.** A local named volume makes I/O nearly free, so any
  change that trades syscalls for bytes will read as noise here. Measure I/O
  work as I/O work (the counting-store test) and do not expect the laptop
  harness to show it.
- **An unexplained ingest anomaly recurred**: four candidate runs out of ~11
  ingested at ~235K lines/s instead of ~590K — a clean 2.5x — while zero of six
  baseline runs did. Every occurrence followed a Docker image build, which makes
  host contention the likely cause, but it is not proven, and a read-path change
  has no mechanism to slow the write path. Anyone measuring here should discard
  runs whose ingest falls below ~500K rather than average them in.

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

**Retried on 2026-08-08 23:50 over range reads, and rejected again.** With the
reader fetching only the surviving groups, Shape A did improve — median 47–50 ms
against a 53–57 ms baseline, p95 59–64 against 72–78, max 64–71 against 88–98,
consistent across three runs — but B1 funnel went from 54–67 ms to **153–172 ms**
and B4 from 29–38 to 47–55, equally consistently. Coalescing the range requests
did not help, which located the cost: it is the *row-group count*, not the I/O.
A full scan keeps every group, and at 8K rows there are four times as many, so
the reader emits four times as many batches — each re-carrying the shared
dictionary, the exact effect the M4 notes warn about. Trading a 2.6x funnel
regression for a point lookup already ten times inside its 250 ms target (PR-3)
is a bad bargain. If this is tried a third time, vary the row-group size against
*both* query shapes rather than optimising Shape A alone.

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
