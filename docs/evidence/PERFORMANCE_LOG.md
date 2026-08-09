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

### 2026-08-09 04:15 — Cache the bloom filters next to the footers   [REJECTED]

**Hypothesis.** Footers are cached for the engine's lifetime; the bloom filters
they point at are not. So every entity lookup re-reads a filter per candidate
file — roughly 80 small range reads per warm query after time pruning. Cache
the parsed `Sbbf` alongside the footer, keyed by (path, row group, column), and
those reads disappear. Expect the Shape A median to fall from ~51 ms, and the
p95 tail (93–114 ms) to shorten more sharply, since that is where the reads
pile up.

**Change.** `MetaCache` became a struct holding two maps — footers as before,
plus blooms — with the same crude 4096-entry bound on each. `None` is cached
as well as `Some`, so a column without a filter is not re-probed. A unit test
pins the mechanism: two identical lookups for an absent entity, and the second
touches the store zero times.

**Measurement.** Paired laptop runs, 100 Shape A samples, all six clean
(ingest 544–591K):

| Metric | Baseline | Candidate |
|---|---|---|
| Shape A median | 50, 50, 53 ms | 50, 55, 51 ms |
| Shape A p95 | 93, 106, 114 ms | 100, 103, 75 ms |
| B1 funnel warm | 49, 49, 47 ms | 49, 55, 57 ms |

**Verdict.** Rejected. The median does not move and the p95 ranges overlap;
p95 leans the right way but 75–103 against 93–114 is not a result. The
mechanism demonstrably works — the test proves the reads go to zero — it simply
does not buy time on this hardware, because a 4 KB re-read from a local volume
comes out of the page cache. This is the same wall the range-read cycle hit,
and the second time a change that removes I/O has measured as nothing. Adopting
it would have meant carrying a cache, a lock per probe and a few hundred KB of
memory for no measured return.

**Lesson.** The bloom probe was never the bottleneck, so removing it could not
help. What Shape A costs tracks the number of candidate files it must consider:

| Scale | Files | Shape A median | Per file |
|---|---|---|---|
| smoke | 54 | 4 ms | 0.07 ms |
| laptop | 167 | 50 ms | 0.30 ms |

Not linear — laptop files hold ~22K rows against smoke's ~1.4K — so the cost is
per-file overhead *and* work proportional to what survives, not I/O. The next
cycle should attack the candidate set itself rather than the cost of examining
each candidate:

- **Entity summaries in the catalog.** `FileMeta` already carries min/max time
  and prunes on it before any file is opened. The same trick for the clustering
  column — a min/max, or a small digest — would let a lookup discard most files
  without opening them at all. This is ARCHITECTURE §13's "bounded hot-entity
  index" question, and it is now the obvious next move.
- **Fewer, larger files.** 167 L0 files exist because compaction never runs at
  laptop scale (a 20 s run against a 30 s tick). Whatever compaction would do
  to Shape A is unmeasured at this scale — driving compaction explicitly before
  the query phase would measure it.

And a rule that would have saved this cycle: **before optimising a cost, prove
it is on the critical path.** A probe of ~80 page-cached reads was always going
to be ~1 ms against a 51 ms query; the arithmetic was available before the
build.

### 2026-08-09 01:55 — Bloom filters on high-cardinality tags   [ADOPTED]

**Hypothesis.** M4 recorded a constraint — "the arrow writer emits no bloom
filters for dictionary columns" — and built entity clustering plus row-group
statistics around it. The pinning test never enabled blooms, and the Parquet
writer defaults them off, so the constraint may be an artifact. If it is, a
bloom on the entity column lets a point lookup skip whole files it currently
reads in full, which is exactly what fresh L0 data needs: unclustered, its
statistics ranges span every entity and prune nothing.

**Change.** `to_parquet_bytes_rg` enables a bloom on dictionary columns above
`BLOOM_MIN_DISTINCT` (1024) distinct values, sized with the column's exact
distinct count. The provider gains `bloom_keep_row_groups`, consulted after
statistics: any literal a bloom positively excludes drops that row group.
`Sbbf::read_from_column_chunk` reads through the range-read `StoreFile` from
the last cycle, so a probe costs a few KB rather than a file.

**Measurement.** The constraint is false: with blooms requested, a dictionary
column gets one — `bloom_filter_offset = Some(2756)`. The old test passed only
because its *reader* was never configured to load blooms. Paired laptop runs,
100 Shape A samples, discarding runs whose ingest fell below 500K:

| Metric | Baseline (n=4) | Candidate (n=6) |
|---|---|---|
| Shape A median | 56, 57, 57, 57 ms | 51, 47, 48, 49, 47, 48 ms |
| Shape A p95 | 69, 71, 93, 169 ms | 63, 62, 62, 64, 59, 59 ms |
| Ingest | 573–589K lines/s | 570–606K lines/s |
| Objects on disk | 71 MB / 114 files | 72 MB / 113 files |

A unit test pins the mechanism directly: a lookup for an entity that is not in
the file reads under a quarter of it, and one that is still returns its row.

**Verdict.** Adopted. Shape A median drops ~15% with no overlap between the two
sets — the baseline is unusually tight at 56–57 ms — and p95 both drops and
stops producing outliers. Storage costs 1.4%. Ingest is unchanged.

**Two things that nearly produced a wrong answer, both worth remembering:**

- **A 4x storage scare that was not real.** The first candidate reported 0.38 GB
  against a 0.09 GB baseline, consistently, 3 runs against 3. That looks like
  signal and is not: the harness samples storage right after ingest, and the
  figure was the WAL mid-drain. Measured properly — `du` of `objects/` with the
  WAL empty — it is 71 MB against 72 MB. **Never judge storage from the harness
  metric at laptop scale; look at the data directory.**
- **A 2x funnel regression that was an ordering artifact.** In full runs B1 went
  from 57–91 ms to 115–135 ms, three runs against four, no overlap — damning
  until Shape B was run *without* Shape A in front of it, where the two builds
  are identical (warm 113, 108 baseline against 111, 107 candidate). The cause
  is that Shape A now skips most files, so it no longer warms their footers in
  the metadata cache, and the next query pays for them. Total work across the
  suite is roughly conserved; the win is real for point lookups and neutral for
  scans. **When a scenario regresses, re-run it in isolation before believing
  it** — this harness runs its shapes in a fixed order and they share a cache.

**Lesson.** A documented constraint is only as good as the test under it. This
one had been load-bearing since M4 — entity clustering exists because of it —
and it was never true; the test asserted the absence of something nobody had
asked the writer to produce. Worth auditing the other pinned assumptions the
same way: check that the test actually exercises the thing it claims to pin.

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
