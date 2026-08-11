# Performance enhancement log

A scheduled agent proposes one performance idea per cycle, implements it,
measures it against a paired baseline, and records the outcome here —
**whether it worked or not**. Failures are the more valuable half: they mark
the paths already explored so the next cycle does not walk them again.

The harness is the referee. Every number below comes from
`bench.py run --backend timelakedb`, never from an ad-hoc measurement, and
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
  lead. (Qualified 2026-08-10 11:40: the load really is 40–75% of a warm
  Shape B query, but it is not thread-bound — widening the decode changed
  nothing. Whatever streaming wins, it will not be by parallelism.)
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

### 2026-08-11 01:50 — Promise tag equality `Exact` so the tag leaves the projection   [REJECTED]

**Hypothesis.** Every cycle so far has tried to make the scan's work *cheaper*.
This one tried to delete a column from it outright. `supports_filters_pushdown`
reported every filter `Inexact`, so DataFusion kept a `FilterExec` above the
scan — and to feed it, the scan had to project the tag column. `EXPLAIN` on
baseline B2 says it in one line:

```
FilterExec: event@3 = stop AND time@0 >= 1786238853061441895, projection=[product_id@1, step@2]
  DataSourceExec: partitions=24, partition_sizes=[2, 2, …]
```

`event` is decoded, compared once, and projected straight back out. But
`load_one_file` **already** installs a Parquet row filter for tag equality, so
non-matching rows never reach a batch: the re-check is redundant. Report such a
filter `Exact` and `event` should leave the scan's projection entirely.

**Proved on the critical path first, and this is where the cycle went wrong —
see the lesson.** `bench/probe-coltype.py` on the settled baseline, today's
host: the marginal cost of one decoded column, same scan, same rows.

| Projection | Median | vs floor |
|---|---|---|
| floor: `COUNT(*)` | 8.2 ms | — |
| +1 f64 (`duration_s`) | 10.2 ms | +2.0 |
| **+1 dict (`step`, ~10 distinct)** | 13.9 ms | **+5.7** |
| +1 dict (`route`) | 11.8 ms | +3.6 |
| +1 dict (`product_id`, ~200 K) | 12.1 ms | +3.9 |
| +3 dict (`step`, `event`, `product_id`) | 21.5 ms | +13.3 |

`event` is a low-cardinality dictionary column exactly like `step`, so removing
it from B1/B2/B5 should be worth 4–6 ms each — plus a conjunct off `FilterExec`.
Predicted ~10% off a 125 ms suite; B3/B4 untouched (no tag equality).

**A different idea was killed before this one, for the cost of one probe.** Per
query, `run_sql_env` builds a whole `SessionContext`. Timed directly (200
iterations, release, in Docker): `SessionContext::new_with_config_rt` = **35 µs**,
`create_logical_plan("SELECT 1")` = **20 µs**. Against a 3 ms Shape A that is
1%, so pooling or caching sessions is not worth a cycle. Recorded so nobody
re-derives it.

**Change.** `crates/query/src/provider.rs`, one file. `enforced_equality(expr,
schema)` recognises *exactly* `column = <string literal>` against a string
column of the presented schema and nothing else — no `Cast` (its semantics are
the cast's), no `OR` branch, no negation, no non-string column, pinned by a
table-driven test. `supports_filters_pushdown` returns `Exact` for those and
`Inexact` for everything else; `scan` computes the same set with the same
function, so the promise and the enforcement cannot drift. Time bounds stay
`Inexact` forever — they prune whole files by catalog bounds and never filter a
row. Two holes had to be closed before the promise was true: the **write-buffer
snapshot** had no row filter behind it and was being handed up whole (now
filtered in `load_pruned`, *before* projection, since the projection no longer
carries the column), and a **file that lacks the column** would have contributed
rows that are all NULL for it (now skipped outright — `NULL = 'stop'` is not
true). `eq_mask` centralises the comparison and folds nulls to false with
`prep_null_mask_filter`, which only mattered once the `FilterExec` safety net
was gone.

**Measurement.** Two paired runs on the isolated instance, harness in-network,
100 Shape A samples. All four clean (ingest 752–769 K lines/s) and all four
settled to **62–63 parquet files / 69 MB** (`du`, not the harness metric). A
third baseline was **discarded unread**: 267,800 lines/s *and* 111 files — both
tripwires at once, exactly as the 19:45 entry predicted they would fail
together. Labels `perf-base1/2`, `perf-cand1/2`, warm ms — **but see the note
below: this cycle's `run.json` records did not survive, so unlike every other
entry these numbers cannot be re-read from `bench/results/`.**

| Metric | Baseline | Candidate |
|---|---|---|
| B1_funnel_24h | 23, 24 | *26, 26* |
| B2_funnel_48h | 41, 46 | 40, 45 |
| B3_inflight_24h | 14, 15 | 14, 15 |
| B4_hourly_throughput_48h | 19, 20 | 19, 20 |
| B5_route_rollup_24h | 28, 28 | 27, 27 |
| **Shape B suite, warm total** | **125, 133 ms** | **126, 133 ms** |
| Shape A median / p95 | 3/4, 3/5 ms | 3/5, 3/4 ms |
| Ingest | 752K, 769K | 755K, 761K |

Row counts match in both arms (10/10/10/475–476/40) and Shape A reports zero
errors. 167 workspace tests pass, 0 failures (164 + 3 new); `cargo fmt --all
--check` clean; `clippy -p timelake-query --all-targets` clean.

**Verdict.** Rejected — a wash, with a small consistent regression. The suite is
125/133 against 126/133. B1 goes 23,24 → 26,26 with no overlap, B5 goes 28,28 →
27,27 with no overlap the other way, and B2/B3/B4 are identical to the
millisecond. Nothing here clears noise.

**And the mechanism worked perfectly, which is the whole finding.** `EXPLAIN` on
the candidate, same query:

```
TableScan: pipeline_events projection=[time, product_id, step],
  full_filters=[pipeline_events.event = Utf8View("stop")],
  partial_filters=[pipeline_events.time >= TimestampNanosecond(…)]
...
  FilterExec: time@0 >= 1786239670060616194, projection=[product_id@1, step@2]
```

`event` is gone from the projection and gone from the filter. The column this
cycle set out to delete is deleted, and the suite did not move by a millisecond.

**Lesson.**

- **The probe measured the wrong column, and the reason generalises.**
  `probe-coltype.py` varies the projection *with no tag filter, on purpose* —
  its docstring says so — so every column it prices is decoded **once**. A
  column named in the `WHERE` clause is not in that regime: the Parquet row
  filter must decode it **in full** to evaluate the predicate, whatever the
  projection says. So `event` was being decoded twice, and removing it from the
  projection removed the *second, cheaper* decode — arrow reuses the
  predicate-decoded column for output where it can — not the +5.7 ms the table
  advertised. **Never price a column with `probe-coltype.py` if the query
  filters on it; that number is only valid for a column the query merely
  reads.** This is the single most reusable thing in this entry.
- **Which also says where the remaining prize is, and that it is not here.**
  The scan cannot stop decoding a filtered column, because it is the thing
  being filtered on. The only way `event` gets cheaper is if the predicate is
  answered without decoding it at all — a dictionary-domain check (is `'stop'`
  in this row group's dictionary page?) or column-chunk statistics on a
  low-cardinality column, both of which answer per row group rather than per
  row. `event` has ~10 distinct values so `BLOOM_MIN_DISTINCT` (1024) correctly
  writes no bloom for it, and `stats_keep_row_groups` already consults
  min/max — which admits `'stop'` in every group, because every group contains
  every event type. **That is a dead end at this cardinality, and it is worth
  writing down so nobody tries it: a value present in every row group cannot be
  pruned by any per-group index.**
- **Five cycles have now assumed a scan-side saving converts into query time,
  and five have measured zero** (11:40, 13:45, 16:45, 19:45, this one). The
  earlier four moved work around inside the scan; this one *removed* a column
  from it outright and still bought nothing. The pattern is strong enough to
  invert the default: **on this workload, treat "the scan does less work" as
  predicting no change until a paired run says otherwise.**
- **B1's +2.5 ms is real but unexplained, and I am not going to pretend
  otherwise.** Four samples, no overlap, on the one query that ought to have
  gained most. It is not the file-skip guard (62 files × one name lookup) and
  not `eq_mask` (a match and a null-count check). The plausible candidate is
  that dropping the tag from the output projection costs arrow the reuse of its
  predicate-decoded column, making the two decodes *more* expensive than one
  decode plus one reuse. `sh bench/probe-innet.sh tldb-perf probe-coltype.py`
  with a `WHERE event = 'stop'` variant added — pricing a column that is
  filtered on, which no probe here currently does — would settle it in a minute
  and is the right first move for anyone who picks this up.
- **I destroyed this cycle's raw records, and the next cycle should not repeat
  it.** Reverting a rejected change, I ran `git clean -f bench/` to delete one
  scratch probe script and it also removed the four untracked
  `bench/results/timelakedb-perf-*/` directories this cycle had just produced —
  the run records every other entry in this log can still be re-read from.
  Every number in the table above is from the run transcript and is reported as
  measured; none of it is reconstructed. But it is unverifiable, which is
  exactly what this log's opening promise says it should never be. **Revert
  with `git restore <paths>` and delete scratch files by name — `git clean` in
  a directory the harness writes into is not a cleanup command, it is a
  data-loss command.**
- **Cost of the cycle, honestly.** ~200 lines against ~90, and the promise it
  makes is the expensive part: `Exact` means a bug in `enforced_equality`
  returns *wrong rows* rather than slow ones. Two of the three new tests exist
  only to hold that promise (buffer filtering, missing column). Reverted whole.
  Carrying that risk surface for a measured zero is not a trade worth making —
  but note the mechanism is sound and fully tested, so if a future change ever
  makes the redundant `FilterExec` conjunct expensive, this is off-the-shelf.

### 2026-08-10 19:45 — Stream the scan into the plan instead of materialising it   [REJECTED]

**Hypothesis.** Every previous cycle attacked what the scan *costs*. This one
attacked when it is *paid*. `LazyTable::scan` awaits the whole load and hands
the finished batches to a `MemorySourceConfig`, so a warm query is load **plus**
plan, strictly serial — and the 11:40 entry's own probe table proves it is
exactly additive, five queries out of five:

| Query (baseline, settled) | Load | Plan | Load+Plan | Whole, measured |
|---|---|---|---|---|
| B1_funnel_24h | 12.2 | 11.5 | 23.7 | **23.8** |
| B2_funnel_48h | 18.5 | 25.1 | 43.6 | **43.6** |
| B3_inflight_24h | 12.6 | 3.1 | 15.7 | **15.7** |
| B4_hourly_throughput_48h | 17.2 | 5.3 | 22.5 | **22.5** |
| B5_route_rollup_24h | 13.7 | 15.8 | 29.5 | **29.5** |

Overlap them and the floor becomes `max(load, plan)` instead of the sum: best
case B1 −49%, B2 −42%, B5 −46%, B3 −20%, B4 −24%. Realistically about half of
that, because the final aggregate still cannot emit before the last batch
lands. Predicted 15–25 ms off a ~137 ms suite; Shape A untouched, since it
prunes to almost nothing. This is precisely the part of the standing
streaming-exec lead that the 11:40 cycle left open when it closed scan
parallelism: *"Overlapping decode with execution may still win by starting the
plan earlier."* No probe was needed to put the cost on the critical path — the
table above already does, and it is stronger than a percentage, because
additivity means **none** of the load is hidden.

**Change.** `crates/query/src/provider.rs`, one file. `scan` no longer awaits
the load: it creates `target_partitions` `RecordBatchReceiverStreamBuilder`
channels (depth 2), returns them as a `StreamingTableExec` over one
`ScanPartition` each, and spawns the decode on its own thread. `load_pruned`
became `stream_pruned` — same worker pool, same file cursor, same RR-2 deadline
between files — but each decoded batch goes to an `emit` closure instead of a
slot vector, and `load_one_file` yields per row group rather than returning a
`Vec`. Everything that used to run *after* the load moved into `Prepare`,
applied on the decode worker: SEC-2 `apply_restriction`, the `COUNT(*)`
empty-batch rewrite, `align_to`. RR-1's reservation now measures what the scan
actually holds (grow before handoff, shrink after) because the candidate set is
never resident at once — the accounting bound became a structural one, queue
depth × partitions. A failure is delivered as an `Err` down partition 0 once the
workers stop, so a short stream can never be mistaken for a complete one.

**Measurement.** Paired runs on the isolated instance, harness in-network, 100
Shape A samples. Two baselines and **three** candidates, all five clean (ingest
733–780 K lines/s) and all five settled to **62 parquet files / 69 MB** (`du`,
not the harness metric). A fourth candidate was **discarded unread** — see the
lesson, it matters. Labels `perf-base1/2`, `perf-cand1/3/4`, warm ms:

| Metric | Baseline | Candidate |
|---|---|---|
| B1_funnel_24h | 24, 25 | 26, 26, 25 |
| B2_funnel_48h | 46, 43 | *50, 51, 52* |
| B3_inflight_24h | 16, 15 | 14, 15, 15 |
| B4_hourly_throughput_48h | 22, 20 | *26, 29, 27* |
| B5_route_rollup_24h | 32, 31 | **27, 27, 27** |
| **Shape B suite, warm total** | **140, 134 ms** | **143, 148, 146 ms** |
| Shape A median | 3, 3 ms | 3, 3, 3 ms |
| Ingest | 734K, 758K | 780K, 749K, 751K |

Row counts match in both arms (10/10/10/475–476/40) and Shape A reports zero
errors. 141 workspace tests pass, 0 failures — including
`memory_pool_rejects_cleanly_never_kills_rr1`, which still returns a clean 400;
`cargo fmt --all --check` clean; `clippy -p timelake-query --all-targets` clean.

**Verdict.** Rejected — a 7% regression, and not a marginal one. The suite goes
137 ms to 146 ms, and the per-query sets separate cleanly rather than smearing:
B2 43–46 → 50–52 and B4 20–22 → 26–29, **no overlap**, three candidate repeats
each; B5 31–32 → 27,27,27, no overlap the other way; B1 and B3 move about a
millisecond. **The split is not the one the hypothesis predicted, and it is not
about load/plan ratio at all.** B2 had the most to gain (18.5 ms of load hidden
behind a 25.1 ms plan) and lost the most. What actually separates the two groups
is the time window: **B2 and B4 are the 48-hour queries, and they are the only
two that regress. Every 24-hour query is neutral or better.** Twice the hours is
twice the files and twice the batches, so what this change added scales with
**batch count**, not with query cost.

**Lesson.**

- **Overlap is real but small, and a per-batch handoff cost eats it.** B5 −4.5
  ms and B3 −1 ms are the overlap showing up, on the two 24 h queries with the
  least to hand over. Against that, each batch now crosses a bounded channel and
  a task wakeup on its way to the plan, where before it was already sitting in a
  `Vec` when the plan was built. At 24 h the two roughly cancel; at 48 h the
  handoff wins outright. **The unit of the cost is the batch, and this engine's
  batches are enormous** — `with_batch_size(1_048_576)` means one batch per row
  group, so 24 partitions × depth 2 is a queue measured in hundreds of MB and a
  worker blocks on a *specific* partition even when 23 others have room.
- **Which names the retry, if there is one.** Do not retry this as written. The
  two things to vary are the handoff granularity and the blocking discipline: a
  single shared queue that any consumer can pull from (rather than round-robin
  into 24 private ones, where one slow consumer stalls all eight decoders), and
  a queue depth chosen against batch *bytes* rather than batch count. Both are
  changes to the channel, not to the streaming idea — the streaming idea
  measured as worth roughly 4 ms on the queries it can help.
- **`max(load, plan)` is the wrong model for a plan that must see every row.**
  The prediction assumed hiding the load behind the plan converts directly into
  wall time. It does not: B1/B2/B5 end in a final aggregate that cannot emit its
  first row until the last batch has arrived, so the load is not hidden *behind*
  the plan, it is interleaved *with* it and both get slower per batch. The
  additivity in the probe table is real, but additive does not imply separable.
  **Three cycles have now assumed a scan-side saving converts into query time
  (11:40, 13:45, 16:45, and now this one — four); none has.**
- **The discard rule earned its keep again, and for a second reason.** `cand2`
  ingested at **271 K lines/s** and was thrown away unread. It reported the
  **fastest suite of the entire cycle — 127 ms**, better than either baseline;
  read as data it would have turned this rejection into an adoption. It also
  settled to **110 parquet files instead of 62**, because the slow ingest
  changed what the flush tick produced. So there were two independent reasons to
  refuse it, and the ingest number caught it first. **Check the settled file
  count as well as the ingest rate before believing a pair** — `perf-cycle.sh`
  prints both, and they failed together here.
- **Cost of the cycle, honestly.** The change is ~340 lines against ~220, and it
  moves SEC-2 enforcement and RR-1 accounting onto the decode workers. It was
  reverted whole. Anything reaching for `StreamingTableExec` again should note
  that the plumbing itself was not the problem: it built, all 141 tests passed
  first time including the RR-1 hog, and the row counts never moved. The
  measurement is the only thing that rejected it.

### 2026-08-10 16:45 — Override the decode for LOW-cardinality tags only   [REJECTED]

**Hypothesis.** The previous entry closed by naming this cycle and calling it
"the highest-value thing on this list". Its blanket reader-override made the
scan 41% cheaper per string column and the suite total moved 130 → 131 ms,
because the split was exactly `product_id`: B3/B4 never read it and kept a
3.5 ms win, while B1/B2/B5 read it and gave the win back with interest. The
stated fix: **force the reader override for low-cardinality columns and keep
the post-decode cast for high-cardinality ones.** The discriminator needs no
new `FileMeta` field — the write path emits a bloom filter only above
`BLOOM_MIN_DISTINCT` (1024) distinct values, so "has a bloom" already means
"high cardinality" and it sits in the footer this code has open. Predicted:
B3/B4 hold their 3.5 ms, B1/B2/B5 return to baseline, ~7 ms off a 130 ms suite.

**Change.** `crates/query/src/provider.rs`, one file.
`view_override_schema(schema, md)` rewrites a `Dictionary<_, Utf8>` field to
`Utf8View` **only when no row group carries a bloom filter for that column**,
and returns `None` when the file has nothing to gain. `load_one_file` applies
it through `ArrowReaderOptions::with_schema`, falling back to the file's own
schema if the reader refuses the override — a rejected override costs speed,
never correctness, because `to_view_batch` still converts whatever is left.
`MetaCache` now holds `ArrowReaderMetadata` instead of the raw footer, so the
decision (a property of the file, not of the query) is made once per file and
the per-file-per-query `try_new` disappears. The row filter gained a
`Utf8View` arm: `event` is low-cardinality, so a predicate on it now meets a
view column and arrow's comparison kernels reject a mismatched scalar.

**Measurement.** Three paired runs on the isolated instance, harness
in-network, 100 Shape A samples. All six clean (ingest 756–776 K lines/s) and
all six settled to **62 parquet files / 69 MB** (`du`, not the harness
metric). Labels `perf-base1/2/3`, `perf-cand1/2/3`, cold/warm ms:

| Metric | Baseline | Candidate |
|---|---|---|
| B1_funnel_24h | 26/23, 25/24, 45/25 | 31/23, 28/24, 26/25 |
| B2_funnel_48h | 42/41, 48/47, 42/41 | 43/41, 52/49, 47/45 |
| B3_inflight_24h | 14/15, 15/14, 22/14 | **15/12, 12/11, 12/11** |
| B4_hourly_throughput_48h | 20/19, 19/19, 20/19 | **18/16, 16/17, 15/15** |
| B5_route_rollup_24h | 28/29, 28/29, 33/29 | *33/32, 34/33, 31/31* |
| **Shape B suite, warm total** | **127, 133, 128 ms** | **124, 134, 127 ms** |
| Shape A median | 3, 4, 3 ms | 3, 3, 3 ms |
| Ingest | 756K, 774K, 763K | 776K, 757K, 770K |

Row counts match in both arms (10/10/10/476/40) and Shape A reports zero
errors. 137 workspace tests pass, 0 failures (135 + two new); `cargo fmt --all
--check` clean; `clippy -p timelake-query --all-targets` clean.

**Verdict.** Rejected. **Half the prediction came true exactly** — B1 goes
23,24,25 → 23,24,25, the +4 ms the blanket override cost it is completely
gone, and B3 (15,14,14 → 12,11,11) and B4 (19,19,19 → 16,17,15) keep the full
win with no overlap. But B5 goes 29,29,29 → 32,33,31, also with no overlap,
and it is **the same regression, the same size, as when `product_id` WAS
being overridden**. Two queries win ~3 ms, one loses ~3 ms, B2 leans ~2 ms
worse inside its own spread: 129.3 ms of suite against 128.3. A second
mechanism that does what it says and arrives at the client as zero.

**Where B5's cost actually is, measured** (`bench/probe-shapeb.py`, new — each
query hammered alone, n=21, median ms, the same settled data in both arms):

| Isolated query | Baseline | Candidate |
|---|---|---|
| `GROUP BY step` — one string key | 10.9 | 11.4 |
| `GROUP BY route` — one string key | 11.1 | 11.6 |
| **`GROUP BY route, step` — two string keys** | **13.4** | **14.8** |
| B5a: two keys + `ORDER BY` | 13.1 | 15.4 |
| B5b: two keys + `AVG(duration_s)` | 14.1 | 15.9 |
| B3 | 14.7 | **11.5** |
| B4 | 20.7 | **15.7** |

**Lesson.**

- **The bloom filter works as a cardinality marker, and that part is
  keepable.** No new catalog field, no write-path change, decided once per
  file and cached with the footer; B1's regression disappeared exactly as
  predicted. The 13:45 diagnosis of *B1* was right to the millisecond.
- **It was also incomplete, and the missing half is not about cardinality at
  all.** B5's regression survives with `product_id` back on the cast path,
  unchanged in size. The table above locates it: one reader-built view group
  key costs +0.5 ms (noise), two cost +1.4–2.3 ms, consistently, whichever two.
  **The cost tracks the NUMBER of view group keys built by the parquet
  reader, not the cardinality of any one column.** B1 and B4 group by one key
  and win; B5 groups by two and loses. That is the entire split, and it was
  invisible while `product_id` was confounding it.
- **This lead is now closed, both halves tried, same ledger each time.**
  Blanket override: net zero. Cardinality-selective override: net zero, with
  the winners and losers reshuffled. The only discriminator left is how many
  string group keys the *query* uses, which the provider cannot see — a
  planner-level decision, not a file-level one. **Do not spend a fourth cycle
  on how a `Utf8View` tag column is constructed.**
- **Isolating a regressed scenario is necessary and was not sufficient.** The
  standing rule says re-run a regression alone before believing it. Done here,
  B5 alone reads 31.1–31.3 → 31.7–31.8 — a wash — because full B5 is dominated
  by `COUNT(DISTINCT product_id)` whose variance (p95 36–41 ms) swallows a
  2 ms move. The harness, running B5 in suite order, saw the regression
  cleanly; the isolated median hid it. What exposed the mechanism was
  *decomposing* B5 into its halves. **Isolate AND decompose — an isolated
  aggregate can be the less sensitive instrument, not the more.**
- **`MetaCache` holding `ArrowReaderMetadata` is still worth keeping**, and
  this is the second cycle to offer it up with a rejected change around it. It
  is ~6 lines, removes a `try_new` per file per query, and is orthogonal to
  every decision above.
- **Rig.** `sh bench/probe-innet.sh tldb-perf probe-shapeb.py 21` — about
  90 s — prints every Shape B query hammered alone with median/min/p95, plus
  the B5 half-queries and the one-key/two-key group probes above. It is the
  tool the "re-run a regressed scenario in isolation" rule has always required
  and never had; the harness's single cold/warm pair cannot separate 3 ms from
  noise.

### 2026-08-10 13:45 — Decode tags straight to `Utf8View`, no cast   [REJECTED]

**Hypothesis.** The 07:45 entry adopted `Utf8View` tags but delivered them by
*casting* a decoded `DictionaryArray` on the worker thread, and explicitly
recorded the other candidate as untried: hand the parquet reader an overridden
arrow schema (`ArrowReaderOptions::with_schema`) so a BYTE_ARRAY column decodes
straight to `Utf8View` and the keys array, the intermediate dictionary array and
the second pass over every value never exist. Predicted 30–50% off the
per-column decode cost, ~4 ms a query, and — unlike every previous `Utf8View`
attempt — a win on *all five* Shape B queries, because it removes work rather
than moving it.

**Two probes ran before any code was written, and the first killed a different
idea outright.** The 11:40 cycle left "memory bandwidth or allocator contention"
as the explanation for a load that burns 2.4× the CPU for the same wall time.
`bench/probe-alloc.sh` (new) samples the server's `/proc/1/stat` either side of
a hammered load proxy:

| B2 load proxy, n=200, settled `perf-base1` | |
|---|---|
| minor page faults **per query** | **25** |
| system CPU per query | 7.8 ms (**7%** of the query's CPU) |
| user CPU per query | 99.5 ms, against an 18.2 ms wall (≈550%) |

25 faults a query means the decode reuses its heap and the kernel is not
zeroing pages for it. **Swapping the global allocator — the obvious reading of
"not thread-bound" — was refuted for the cost of four minutes and no build.**

`bench/probe-coltype.py` (new) then located the cost by column *type*: same
scan, same rows, varying only the projection. No tag filter, so no row filter
and every row materialises.

| Projection | Baseline | Candidate |
|---|---|---|
| floor, `COUNT(*)` | 8.1 ms | 8.0 ms |
| +1 f64 (`duration_s`) | +1.3 | +2.6 |
| +1 dict (`step`, ~10 distinct) | +5.8 | +2.6 |
| +1 dict (`route`) | +3.8 | +2.3 |
| +1 dict (`product_id`, ~200 K) | +3.1 | +2.7 |
| +2 dict (`step`, `product_id`) | +9.0 | +4.9 |
| **+3 dict (`step`, `event`, `product_id`)** | **+13.7** | **+8.1** |

A dictionary string column cost ~3.5× what an f64 column cost for identical
rows. That gap — not "reading bytes" — was the target.

**Change.** `crates/query/src/provider.rs`, one file.
`view_reader_metadata` rewrites the file's own arrow schema through the
existing `view_schema` and rebuilds `ArrowReaderMetadata` with
`ArrowReaderOptions::with_schema`, which switches arrow's column dispatch from
the dictionary reader to the byte-view reader; it falls back to the file's
schema if the reader refuses the override, so `to_view_batch` still converts
and a coercion failure costs speed, never correctness. `MetaCache` now holds
`ArrowReaderMetadata` instead of the raw footer, so the arrow schema resolves
once per file rather than once per file per query (this also deletes a
`try_new` from the per-file path). The row filter gained a `Utf8View` arm —
it decodes through the same overridden schema, and arrow's comparison kernels
want both sides one type.

**Measurement.** Paired runs on the isolated instance, harness in-network, 100
Shape A samples. All four clean (ingest 737–770 K lines/s) and all four settled
to **62 parquet files / 69 MB** (`du`, not the harness metric). Labels
`perf-base1`/`perf-base2`, `perf-cand1`/`perf-cand2`, cold/warm ms:

| Metric | Baseline | Candidate |
|---|---|---|
| B1_funnel_24h | 33/23, 45/23 | *27/27, 27/27* |
| B2_funnel_48h | 45/45, 44/43 | 44/44, 51/45 |
| B3_inflight_24h | 19/15, 21/15 | **12/11, 12/12** |
| B4_hourly_throughput_48h | 19/19, 25/19 | **16/16, 16/15** |
| B5_route_rollup_24h | 28/29, 30/29 | *34/33, 33/32* |
| **Shape B suite, warm total** | **131, 129 ms** | **131, 131 ms** |
| Shape A median | 3, 3 ms | 3, 3 ms |
| Ingest | 748K, 763K | 737K, 770K |

Row counts match in both arms (10/10/10/475–476/40) and Shape A reports zero
errors. 136 workspace tests pass, 0 failures (135 + one new); `cargo fmt --all
--check` clean; `clippy -p timelake-query --all-targets` clean.

**Verdict.** Rejected — and this is the most interesting rejection in the log,
because the change did more than it promised and bought nothing.
**The scan got 41% cheaper per string column** (+13.7 → +8.1 ms for B2's three
columns) and a dictionary column now costs about what an f64 column costs,
which is the floor. **The suite total moved from 130 ms to 131 ms.** B3 warm
goes 15,15 → 11,12 and B4 19,19 → 16,15, no overlap; B1 goes 23,23 → 27,27 and
B5 29,29 → 33,32, equally consistent, equally no overlap. Two queries win ~3.5
ms, two lose ~3.5 ms, B2 is a wash. A 5.6 ms saving inside the scan arrives at
the client as zero.

**Lesson.**

- **The split is exactly `product_id`.** B3 and B4 never read it and keep the
  win; B1, B2 and B5 do and give it back with interest. Same presented schema
  in both arms — the planner sees `Utf8View` either way, so this is not the
  group-values fallback re-appearing. What differs is *how the view array is
  built*: the cast preserves the dictionary's single shared value buffer and
  emits views into it, while the reader's byte-view path builds its own buffers
  as it decodes. For 10 distinct values that is free; for 200 K it hands the
  aggregate above a far more scattered array to walk. **The construction of a
  `Utf8View` column matters as much as its type.**
- **This re-opens the cardinality-selective idea the 22:35 entry declared
  blocked, on new ground.** That cycle could not convert `product_id` alone
  because `GroupValuesColumn` needs *every* key column supported, so a mixed
  Dictionary/View key set falls back to the row format. That objection does not
  apply here: both paths present `Utf8View`, so the fast path is kept either
  way, and the only choice is per-column construction. **Force the reader
  override for LOW-cardinality columns and keep the cast for high-cardinality
  ones** — predicted to hold B3/B4's 3.5 ms while leaving B1/B5 alone. The
  discriminator needs no new `FileMeta` field either, which is what the 07:45
  entry assumed: the write path already writes a bloom filter only above
  `BLOOM_MIN_DISTINCT` (1024) distinct values, so "has a bloom" is a
  high-cardinality marker already sitting in the footer this code has open.
  **That is the next cycle, and it is the highest-value thing on this list.**
- **Scan-load percentages do not convert into query time.** `probe-load.py`
  called the load 40–75% of a warm Shape B query, and this cycle removed 41% of
  its per-column cost for a net zero. The load and the plan are not additive:
  changing what the scan hands upward changes what the plan costs. **Judge a
  scan change end-to-end, never by the scan probe alone** — and treat the
  standing "streaming / range reads is the largest known lead" with the
  scepticism this and the 11:40 entry have now earned it twice over.
- **`MetaCache` holding `ArrowReaderMetadata` is worth keeping** whenever this
  is retried — it removes a per-file-per-query `try_new` and is independent of
  the override decision. It is reverted here only because it arrived with the
  rest of the change.
- **Rig.** Two new probes, both about a minute in-network and both kept:
  `sh bench/probe-alloc.sh tldb-perf 200 b2_load` prints per-query minor faults
  and the user/system CPU split — the cheap test for "is this allocation or is
  it work"; `sh bench/probe-innet.sh tldb-perf probe-coltype.py` prints the
  marginal cost of a decoded column *by type*, which is what turned "the load
  is expensive" into "the string columns are 3.5× the primitives".

### 2026-08-10 11:40 — Decode the scan as wide as the plan runs   [REJECTED]

**Hypothesis.** Every per-operator table in this log is blind to the biggest
part of a warm query, and this cycle started by proving it. `LazyTable::scan`
decodes its candidate files while the plan is being *built* and hands the
finished batches to a `MemorySourceConfig`, so `EXPLAIN ANALYZE` charges the
load nothing — the previous entry's B2 table sums 347 ms of plan compute across
24 partitions (≈14 ms of wall) against a 42 ms query and never asks where the
other 28 ms went. New probe `bench/probe-load.py` answers that by construction:
run a proxy query that provokes the *identical* scan (same columns, same
filters, so the same pruning, row filter and `Utf8View` conversion) under a
plan that does nothing but a single-group COUNT.

| Query (baseline, `perf-base1` settled) | Whole | Scan-load | Plan | Load share |
|---|---|---|---|---|
| B1_funnel_24h | 23.8 ms | **12.2 ms** | 11.5 ms | 48% |
| B2_funnel_48h | 43.6 ms | **18.5 ms** | 25.1 ms | 40% |
| B3_inflight_24h | 15.7 ms | **12.6 ms** | 3.1 ms | 75% |
| B4_hourly_throughput_48h | 22.5 ms | **17.2 ms** | 5.3 ms | 72% |
| B5_route_rollup_24h | 29.5 ms | **13.7 ms** | 15.8 ms | 43% |

So the load is 40–75% of every warm Shape B query. And `scan_threads` capped it
at a constant 8 while every operator above it fans out to `target_partitions` —
24 on this host. `bench/probe-cpu.sh` confirmed the machine was idle underneath
it: **500–650% CPU** on 24 cores while a load-dominated query ran. Predicted:
raise the decode width to `target_partitions` and the load falls ~2–2.5×, taking
25–45% off each Shape B query; Shape A is untouched because it prunes to few
files and `files.min(..)` still bounds the pool.

**Change.** One file, `crates/query/src/provider.rs`. `scan_threads(files)`
became `scan_threads(files, target_partitions)` —
`files.min(cores).min(target_partitions).max(1)` instead of
`files.min(cores).clamp(1, 8)` — with `target_partitions` read once in `scan`
and threaded through `load_pruned`, and reused for the partition packing below
it. Plus a unit test pinning that the width follows the plan, not a constant.

**Measurement.** Paired runs on the isolated instance, harness in-network, 100
Shape A samples. All four clean (ingest 716–776 K lines/s) and all four settled
to **62 parquet files / 69 MB** (`du`, not the harness metric). Labels
`perf-base1`/`perf-base2`, `perf-cand1`/`perf-cand2`, cold/warm ms:

| Metric | Baseline | Candidate |
|---|---|---|
| B1_funnel_24h | 33/27, 27/24 | 43/29, 32/25 |
| B2_funnel_48h | 60/50, 45/44 | 61/53, 50/47 |
| B3_inflight_24h | 15/15, 15/15 | 16/15, 14/14 |
| B4_hourly_throughput_48h | 22/22, 19/19 | 21/21, 20/19 |
| B5_route_rollup_24h | 31/30, 29/29 | 32/34, 29/30 |
| Shape A median | 4, 3 ms | 5, 4 ms |
| Ingest | 745K, 776K | 759K, 716K |

125 workspace tests pass, 0 failures; `cargo fmt --all --check` clean;
`clippy -p timelake-query --all-targets` clean.

**Verdict.** Rejected, and not because the change failed to do what it said —
because doing it bought nothing. The candidate leans slower on four of five
queries and no query improves. The mechanism check is what makes this
unambiguous rather than a shrug at noise: hammering the B2 load proxy 400×
in-network,

| | Baseline | Candidate |
|---|---|---|
| Server CPU during the load | 500–650% | **1318–1518%** |
| Proxy query, n=400 | median 19 ms (min 18, p95 23) | median 20 ms (min 18, p95 23) |

**2.4× the CPU for the same wall time.** The threads were spawned, they were
busy, and they finished no sooner. (Checked that the change was real and not a
no-op: `pipeline_events` holds 48 of the 62 settled files, so this genuinely
moved 8 workers to 24, not 8 to 8.) Burning three times the cores to stand still
is a regression even where the clock is a wash — those cores are what the
aggregation above the scan, the write path and the maintenance tick run on.

**Lesson.**

- **The scan-load is 40–75% of a warm Shape B query, and it is NOT
  thread-bound.** That is the finding, and it retires the most obvious reading
  of the streaming-exec lead. Overlapping decode with execution may still win by
  starting the plan earlier, but "the load is serialised at 8 threads and the
  box has 24" was the intuitive explanation for its cost and it is now measured
  false. Do not spend another cycle on scan parallelism.
- **What the load is actually bound by, measured** (`bench/probe-scan-floor.py`,
  new): a scan that visits all 48 files and decodes **none** of them — the bloom
  excludes an absent entity — costs **3.6 ms**. Footers, bloom probes, thread
  spawn and the serial tail are therefore ~3.6 ms of an 18 ms load. Each decoded
  column then adds ~4.7 ms, linearly: 1 column 14.0 ms, 2 columns 18.2, 3
  columns 24.2, 4 columns 27.8. **The cost is per-column decode work that more
  threads do not accelerate** — which points at memory bandwidth or allocator
  contention inside the parquet decode plus `to_view_batch`, not at scheduling.
  The next cycle should attack the *work*, not its width: decode fewer bytes
  (the projection is already minimal, so this means encoding), or skip the
  `Utf8View` conversion for columns that gain nothing from it (the open
  selective-view lead — and this probe now gives it a per-column number to beat
  instead of a whole-query one).
- **`EXPLAIN ANALYZE` is systematically blind here, and every operator table in
  this log inherits that.** `DataSourceExec` reports near-zero elapsed_compute
  because its batches were decoded before the plan existed. Two entries above
  reasoned about "the whole query" from plan sums that could not see 40–75% of
  it. `bench/probe-load.py` is the correction and costs about a minute; run it
  before trusting a plan-derived percentage again.
- **A CPU-utilisation gap is evidence of idleness, not of a bottleneck.** 650%
  on a 24-core box looked like a starved parallel section and was really a
  section that had run out of parallelisable work. The cheap test that would
  have settled it before the build — and did settle it after — is to compare
  wall time *and* CPU across the two widths: if CPU rises and wall does not
  move, the width was never the constraint.
- **Rig.** Both probes are committed and take about a minute each in-network:
  `sh bench/probe-innet.sh tldb-perf probe-load.py` splits any Shape B query
  into scan-load and plan, `probe-scan-floor.py` gives the fixed per-scan floor
  and the marginal cost of a decoded column. `tldb-bench:perf` did not exist at
  the start of this cycle — `docker build -t tldb-bench:perf -f
  bench/perf-runner.Dockerfile bench/` rebuilds it in seconds, and every
  in-network script needs it.

### 2026-08-10 07:45 — Present tags as `Utf8View`, converted in the DECODE   [ADOPTED]

**Hypothesis.** The last cycle closed with an explicit retry instruction: the
`Utf8View` win is real, the delivery was wrong, and "the conversion must happen
inside the decode, not after it." So this cycle changes only *where* the
conversion happens, and predicts B1/B2 fall 30–45% while B3/B4/B5 — which the
first attempt made 2–5× slower — return to baseline.

**Proved on the critical path first** (`bench/probe-spill.py`, `EXPLAIN ANALYZE`
against the settled `perf-base1` instance, compute summed over 24 partitions).
This re-check mattered, because the previous cycle removed the spilling that had
been confounding the number:

| Operator | B1 | B2 | B3 |
|---|---|---|---|
| `AggregateExec: FinalPartitioned, gby=[step, alias1]` | **167.5 ms** | **483.9 ms** | *absent* |
| ... its peak memory | 221.6 M | 403.3 M | — |
| ... its spills | 0 | 0 | — |
| everything else in the plan | 76 ms | 88 ms | 46 ms |
| whole query, wall | 49–55 ms | 86–93 ms | 11 ms |

One operator is **85% of B2's plan compute and 69% of B1's**, no longer spilling,
so what is left is purely the group-key encoding: 1.83 M groups keyed by two
`Dictionary<Int32, Utf8>` columns, which DataFusion's column-wise
`GroupValuesColumn` does not accept, so it falls to the row-format fallback that
materialises a string per row per key. B3 has no such operator and never reads
`product_id` at all — it is the control.

**Change.** `crates/query`, two files, no format or write-path change:

- `provider.rs::view_schema` rewrites every `Dictionary<_, Utf8>` field to
  `Utf8View` in the schema `LazyTable` *presents*, applied in `LazyTable::new`.
  Storage, the WAL and the write buffer are untouched — FR-2 is a property of
  the file, not of the type the planner is shown.
- `provider.rs::to_view_batch` does the conversion **inside `load_one_file`, on
  the worker thread that decoded the batch.** That is the whole change relative
  to the rejected attempt.
- `lib.rs::align_to` casts on type mismatch, which now covers only the *write
  buffer* snapshot — file batches arrive already converted.
- `str_literal` learned `ScalarValue::Utf8View`; without it a predicate against
  a view column silently stops yielding tag equality and Shape A loses file
  pruning and its bloom filters.
- `apply_restriction` gained a `Utf8View` arm (SEC-2 fails closed, so a labelled
  table would have errored, not leaked — pinned by the existing COUNT(\*) test).

**Measurement.** Paired runs on the isolated instance, harness in-network, 100
Shape A samples. All four kept runs settled to **62 parquet files / 69 MB**
(`du`, not the harness metric). A `perf-base2` run ingested at **246 K lines/s**
and was **discarded unread** under the ≥500 K rule; the four kept runs ingested
760–783 K. Labels `perf-base1`/`perf-base3`, `perf-cand1`/`perf-cand2`,
cold/warm ms:

| Metric | Baseline | Candidate |
|---|---|---|
| B1_funnel_24h | 58/51, 65/48 | **28/24, 27/23** |
| B2_funnel_48h | 87/91, 103/92 | **44/42, 43/41** |
| B3_inflight_24h | 11/11, 12/11 | *15/15, 16/15* |
| B4_hourly_throughput_48h | 16/16, 16/17 | *20/19, 19/19* |
| B5_route_rollup_24h | 25/24, 25/25 | *30/28, 31/29* |
| Shape A median | 3, 4 ms | 3, 3 ms |
| Ingest | 783K, 760K | 776K, 761K |
| Settled objects | 62 files / 69 MB | 62 files / 69 MB |

Row counts match the baseline in both arms (10/10/10/475–477/40) and Shape A
reports zero errors. 117 workspace tests pass, 0 failures; `cargo fmt --all
--check` clean; `clippy -p timelake-query --all-targets` clean.

**Verdict.** Adopted, and the regression is real and stated rather than buried.
B1 warm goes 48–51 → 23–24 and B2 warm 91–92 → 41–42 — **2.1× and 2.2×**, no
overlap, repeats within 1 ms. B3, B4 and B5 each lose 3–4 ms (**+18% to +36%**),
equally consistently. The arithmetic is what decides it: across one warm pass of
the suite the candidate spends **11 ms more on three queries to save 76 ms on
two**, 190–193 ms → 128–129 ms, about **a third off the whole Shape B suite**.
The rejected attempt had exactly the opposite balance — +100 ms against −35 ms —
which is why it was rejected and this is not.

`EXPLAIN ANALYZE` on the candidate confirms the mechanism instead of inferring
it: B2's final aggregate goes **483.9 → 259.5 ms** of compute and **403.3 →
294.1 MB** of peak memory, B1's **167.5 → 103.4 ms** and 221.6 → 149.4 MB.

**Lesson.**

- **The group-key change makes the aggregate cheaper *and* lighter.** Every
  prediction in this log assumed views trade memory for speed — 16 bytes a row
  against a 4-byte dictionary key. At the operator that matters it is not a
  trade at all: peak fell 27% alongside the compute, because `GroupValuesRows`
  was storing whole key strings per group where the column path stores views
  into one buffer. The row format was costing memory, not saving it.
- **The 3–4 ms that B3/B4/B5 now lose is located, and it is not in the plan.**
  B3's own aggregate got *faster* (43.0 → 37.2 ms of compute, peak 10.95 →
  3.20 MB) while its wall time went 11 → 15 ms. So the residue is scan-side:
  `to_view_batch` building ~16 bytes a row of views for columns nobody groups
  by. `step` and `event` have ten distinct values and gain nothing from a view.
  **That is the remaining cycle, and it is now worth much less than it looked —
  4 ms a query, not 50.** The enabling ingredient is unchanged: a per-column
  distinct count in `FileMeta`, written from the count `to_parquet_bytes_rg`
  already computes for bloom sizing, so only entity-like columns convert.
  Note the catch that makes it fiddly rather than obvious: `GroupValuesColumn`
  needs **every** key column supported, so converting `product_id` alone would
  put B1/B2 straight back on the row format — `step` has to come with it.
- **The rejected cycle's diagnosis was right to the millisecond.** It attributed
  ~50 ms of B3's 53 ms to a serial `align_to` pass and predicted the plan itself
  was fine. Moving the identical conversion onto the decode workers took B3 from
  53 ms to 15 ms. A rejection that names *where* the cost lives is worth more
  than the change it rejects, and this is the second time this log has cashed
  one in.
- **Discard rules only work if you check the number.** `perf-base2`'s query
  figures were indistinguishable from `perf-base1`'s and would have passed
  unnoticed; its ingest was 246 K. It was thrown away on the rule, not on the
  numbers, which is the only way the rule means anything.
- **Open risk, and it is a compatibility one, not a performance one.** Query
  results now carry `Utf8View` columns, and Flight SQL hands those to Grafana
  (FR-9/AT-6) as Arrow StringView, which older Arrow clients cannot decode. B1
  was run against this build over Flight SQL from an ADBC client in-network and
  came back correct — `rows=10, schema=[string_view, int64]`, so the server
  encodes it and a current client reads it. **Stock Grafana was not drilled,
  because the running Grafana is the user's and off limits to this cycle.**
  Drill AT-6 against `fixtures/grafana/` before this reaches a release. If it
  breaks, the fix is a cast at the result boundary, not a revert — the win is
  below that seam.
- **Rig.** `ops/perf-test.sh` runs `cargo` against a named target volume, so the
  second `cargo test`/`clippy` of a cycle is incremental (seconds, not ~10 min)
  the way `ops/perf-build.sh` already does for the release build.
  `ops/wait-image.sh` blocks on a background build finishing.

### 2026-08-10 01:40 — Stop `FairSpillPool` dividing the pool 145 ways   [ADOPTED]

**Hypothesis.** The last cycle left an unclaimed lead: "Baseline B2's inner
aggregate spills 95 times for 39.6 MB … no cycle has yet checked whether Shape B
is spilling because the pool is too small or because the row format is too fat."
Reading `datafusion-execution-54.1.0`'s `memory_pool/pool.rs` first gave the
mechanism before any measurement: `FairSpillPool::try_grow` caps every
*spillable consumer* at `(pool_size - unspillable) / num_spill`, and its own
doc-comment warns it "will cause spills even when there was sufficient memory".
A DataFusion plan registers one such consumer per partition per aggregate
(`aggregates/row_hash.rs:598`), per partition per repartition
(`repartition/mod.rs:450`), and one per sort — so on a 24-core host a funnel
query registers ~145 of them and a 1 GiB pool becomes a ~7 MB budget each.
Predicted: the spills vanish and B1/B2 drop 25–30%; B3/B4/B5 and Shape A are
untouched, because nothing else in the suite comes near a spill.

**Proved on the critical path before writing code** (`bench/probe-spill.py`,
`EXPLAIN ANALYZE` on the settled baseline, sums over 24 partitions):

| B2 operator | Baseline |
|---|---|
| `AggregateExec: FinalPartitioned, gby=[step, alias1]` | **1.70 s** compute |
| ... its spills | **85 spills, 39.5 MB, 1.83 M rows** |
| ... its peak memory | 174.5 M — i.e. **7.3 MB per partition** |
| whole-plan residency | ~220 MB, of a **1024 MB** pool |
| B3's deepest aggregate | 0 spills, 11.3 M peak, 12 ms total |

7.3 MB per partition against a predicted `1 GiB / ~145` = 7.4 MB is the
confirmation: the operator spilled at its *fair share*, with 80% of the pool
free, and B1/B2 are the only queries in the suite that reach it.

**Change.** One file, `crates/query/src/lib.rs`: `QueryEnv::new` builds a
`GreedyMemoryPool` instead of a `FairSpillPool`, same `total_mem_bytes`. RR-1 is
untouched — the cap that matters is the total, and a greedy pool enforces
exactly the same one, spilling or erroring cleanly when the *whole* pool is gone
instead of when 1/145th of it is. Concurrency stays bounded where it was
designed to be bounded, the admission semaphore.

**Measurement.** Three paired runs on the isolated instance, harness in-network,
100 Shape A samples. All six clean (ingest 733–771 K lines/s) and all six
settled to **62 parquet files / 69 MB** (`du`, not the harness metric). Labels
`perf-base1/2/3`, `perf-cand1/2/3`, cold/warm ms:

| Metric | Baseline | Candidate |
|---|---|---|
| B1_funnel_24h | 80/71, 79/72, 75/72 | **55/49, 50/46, 58/59** |
| B2_funnel_48h | 177/142, 165/154, 174/143 | **121/105, 102/92, 111/114** |
| B3_inflight_24h | 12/10, 14/13, 11/11 | 13/12, 11/11, 16/15 |
| B4_hourly_throughput_48h | 18/19, 23/23, 18/18 | 20/17, 17/18, 21/23 |
| B5_route_rollup_24h | 27/28, 31/30, 31/28 | 29/27, 29/27, 34/33 |
| Shape A median | 3, 3, 3 ms | 3, 3, 4 ms |
| Ingest | 733K, 771K, 760K | 770K, 757K, 735K |

80 workspace tests pass, 0 failures — including
`memory_pool_rejects_cleanly_never_kills_rr1`, the 2 MB-pool hog test that pins
RR-1, which still returns a clean 400. `cargo fmt --all --check` clean; no
clippy findings.

**Verdict.** Adopted. B1 warm goes 71–80 → 46–59 and B2 warm 142–154 → 92–114,
**no overlap between the two sets** on either query across three repeats —
about 28% and 30%. B3/B4/B5 sit inside the baseline spread with no consistent
direction (the third candidate run is uniformly ~15% slow across all five
queries, including the two that win, which is host noise, not a mechanism), and
Shape A, ingest and storage are unmoved. `EXPLAIN ANALYZE` on the candidate
confirms the mechanism rather than inferring it: B2's final aggregate goes
1.70 s → **564 ms** of compute and **85 spills → 0**; B1's 716 ms → 187 ms,
36 spills → 0.

**Lesson.**

- **The bug got worse the bigger the machine.** The divisor is the number of
  registered spillable consumers, which scales with `target_partitions`, which
  is core count. The same pool on a 4-core box would have given each operator
  ~44 MB and never spilled; the 24-core CI/bench host spilled 39.5 MB to disk
  with 800 MB free. **Any future memory tuning here must be stated per-core, not
  per-pool** — and a pool figure that looks generous can be starvation.
- **Peak residency moved from 174 MB to 405 MB for one B2**, because the
  aggregate now keeps what it used to spill. That is the honest cost, and it
  sets a real bound: at the default 1 GiB pool two concurrent funnels fit and
  the third starts spilling again — correctly. The shipped compose already
  sizes 3.5 GB against `max_concurrent_queries=2`, so production has room, but
  **`bench/perf-cycle.sh` runs with the 1 GiB default and every number in this
  log was measured there.** Worth one cycle to measure a production-sized pool.
- **The dependency's own doc-comment named the defect.** "This pool works best
  when you know beforehand the query has multiple spillable operators that will
  likely all need to spill." This engine has one spillable operator family
  fanned across partitions of the *same* query — fairness between clones of one
  operator buys nothing and costs a 145× smaller budget. Read the pool choice,
  not just the pool size.
- **The Utf8View lead is still open and is now cheaper to judge.** The rejected
  cycle attributed B2's cost to the row-format group keys; ~1.1 s of that 1.70 s
  was spilling, not encoding. The remaining 564 ms is the real group-values
  cost, so a retry (decode-time `Utf8View` on high-cardinality columns only) now
  has a clean number to beat and no spill confounding it.
- **Rig, for the next cycle.** `ops/perf-build.sh` compiles into a named target
  volume and bakes only the binary, so the candidate image is an incremental
  build (~2 min) instead of a second cold release build (~15 min) — the repo
  `Dockerfile` compiles inside the image and a one-line source edit invalidates
  it entirely. `bench/probe-spill.py` + `bench/probe-innet.sh` print the
  per-operator spill/compute/peak table above in about a minute. Third cycle
  running where the probe, not the idea, was the valuable part.

### 2026-08-09 22:35 — Present tags as `Utf8View`, not `Dictionary`   [REJECTED]

**Hypothesis.** The last cycle named the target: B2's remaining cost is the
final aggregate over 1.83 M `(step, product_id)` groups, keyed by two
dictionary columns, and DataFusion's fast column-wise group-values path does
not cover `Dictionary`. Confirmed in the dependency source before writing any
code — `datafusion-physical-plan-54.1.0`'s `supported_type` lists `Utf8`,
`Utf8View`, `BinaryView` and the primitives, and `Dictionary` is absent, so
every tag group-by falls into the row-format `GroupValuesRows`. Then confirmed
on the critical path, `EXPLAIN ANALYZE` on a settled master instance:

| Operator (B2, summed over 24 partitions) | Baseline |
|---|---|
| `AggregateExec: FinalPartitioned, gby=[step, product_id]` | **1.71 s** compute |
| ... its `time_calculating_group_ids` | 378 ms |
| ... its peak memory | 179.7 MB |
| ... **its spills** | **95 spills, 39.6 MB, 1.83 M rows** |
| `AggregateExec: Partial` below it | 47 ms compute |
| `FilterExec` + scan | 12 ms compute |

So one operator is ~95% of B2's compute, and it is spilling. Present the tag
columns as `Utf8View` and that operator gets `GroupValuesColumn` with inline
views — predicted 30–40% off B1/B2, spills gone, B3/B4 untouched, Shape A
untouched. The recorded risk before measuring: views cost 16 bytes a row where
a dictionary key costs 4, so the scan-heavy queries pay for what the
group-heavy ones save.

**Change.** `group_friendly_schema` in `provider.rs` rewrites every
`Dictionary<_, Utf8>` field to `Utf8View` in the schema `LazyTable` presents,
so storage, the WAL and the write buffer are untouched (FR-2 intact) and only
the planner's view of the table changes. `align_to` gained a cast when a
batch's column type differs from the target field, which is where the
conversion actually happens — arrow's `view_from_dict_values` fast path, no
string copied. `str_literal` learned `ScalarValue::Utf8View`, without which
coercion against a view column silently turns off tag pruning and the blooms.

**Measurement.** Paired runs on the isolated instance, harness in-network, 100
Shape A samples. All four runs clean (ingest 734–774 K lines/s) and all four
settled to **62 parquet files / 69 MB**. Labels `perf-base1`/`perf-base2`,
`perf-cand1`/`perf-cand2`, cold/warm ms:

| Metric | Baseline | Candidate |
|---|---|---|
| B1_funnel_24h | 78/68, 76/81 | **42/41, 42/41** |
| B2_funnel_48h | 161/140, 191/136 | **127/126, 133/129** |
| B3_inflight_24h | 12/11, 12/12 | *53/67, 53/66* |
| B4_hourly_throughput_48h | 18/18, 19/18 | *71/51, 76/57* |
| B5_route_rollup_24h | 26/27, 26/26 | *48/46, 45/67* |
| Shape A median | 3, 4 ms | 3, 3 ms |
| Ingest | 742K, 774K | 734K, 764K |

Row counts are identical in both arms (10/10/10/477/40) and Shape A reports
zero errors, so the candidate answered correctly — it was simply slower
overall.

**Verdict.** Rejected. The mechanism works and is large: B1 goes 76–81 → 41–42
(**~45% off**, no overlap, both repeats identical) and B2 136–191 → 126–133.
But B3 pays **4–5×**, B4 **3–4×** and B5 **~2×**, equally consistently, and
those three are more of the suite than the two that win. Trading 35 ms of
funnel for 100 ms spread across the other three queries is a net loss, and no
amount of framing makes it one.

**Lesson — the win is real, the delivery is wrong, and the next cycle can have both.**

- **Where the regression lives is measured, not guessed.** `EXPLAIN ANALYZE`
  on the *candidate* B3 sums its whole plan to ~35 ms of compute across 24
  partitions — about 1.5 ms of wall — against a 53 ms query. The missing ~50 ms
  is outside the plan entirely: it is `align_to` converting every dictionary
  column of every batch **on one thread, after the parallel load has already
  finished**. The scan decodes on up to 8 workers and then funnels through a
  serial pass that touches every row. Same plan's `FilterExec` reports
  **530 MB** of output bytes where the dictionary version moved a few MB.
- **So the conversion must happen inside the decode, not after it.** Two
  candidates, in order of preference: hand the parquet reader an overridden
  arrow schema (`ArrowReaderOptions::with_schema`) so a BYTE_ARRAY column
  decodes straight to `Utf8View` and never builds a dictionary at all — that is
  what DataFusion's own `schema_force_view_types` does — or, failing that, cast
  per file inside `load_one_file`, on the worker threads. Note that
  `apply_restriction` matches `Dictionary` and `Utf8` explicitly and would need
  a `Utf8View` arm if the conversion moves above it (SEC-2 fails closed, so
  this would be a hard error, not a leak).
- **A blanket schema rewrite is the wrong shape regardless.** `step` and
  `event` have ten distinct values; a dictionary is exactly right for them and
  a view is 4× the bytes for nothing. Only `product_id` — 200 K distinct, the
  key that explodes to one group per row — is worth converting. The provider
  has no cardinality signal today, which is the actual missing ingredient:
  compaction already computes a highest-cardinality cluster column
  (`buffer::flush::cluster_column`) and `FileMeta` could carry a per-column
  distinct count as cheaply as it carries min/max time. **That is the enabling
  cycle, and it is worth running before retrying this one.**
- **The spill is a separate, unclaimed win.** Baseline B2's inner aggregate
  spills 95 times for 39.6 MB. Whatever fixes the group-key encoding also stops
  the spilling, and the spilling is worth measuring on its own — the query
  memory pool is sized by config, and no cycle has yet checked whether Shape B
  is spilling because the pool is too small or because the row format is too
  fat.

### 2026-08-09 19:35 — Stop starving the partial-aggregation skip   [ADOPTED]

**Hypothesis.** The last cycle handed this one its target: "what is left on
Shape B is the aggregation, not the I/O." So this cycle localised the
aggregation before touching it. Probing a settled instance decomposes B2
cleanly:

| Probe | Median |
|---|---|
| `COUNT(*)` over 48 h | 7.9 ms |
| `COUNT(*)` with the `event='stop'` filter (1.83 M rows) | 11.2 ms |
| `step, COUNT(*) GROUP BY step` | 18.1 ms |
| `COUNT(DISTINCT product_id)`, no GROUP BY (200 K groups) | 37.1 ms |
| `GROUP BY step, product_id` counted (1.83 M groups) | 114.2 ms |
| B2 in full | 113.5 ms |

So B2 is 18 ms of scan-and-filter and ~95 ms of one grouping, and that
grouping produces **one group per row**: every (step, product_id) pair is
unique, 1.83 M of them. `EXPLAIN ANALYZE` then named the waste exactly —
`AggregateExec: mode=Partial ... reduction_factor=100% (1.83 M/1.83 M)`,
365 ms of compute and 218 MB of peak memory spent building a hash table that
deduplicates *nothing* before handing every row on to the final aggregate
anyway. DataFusion already knows how to bail out of that (it measures its own
reduction and switches to pass-through), and the same plan says why it never
did: `skipped_aggregation_rows=0`, under `DataSourceExec: partitions=96,
partition_sizes=[1, 1, 1, …]`. **One partition per batch means each partial
aggregate makes its first measurement on its last input** — there is no next
batch to skip. Fixing that should take ~30% off B1/B2/B5 and leave B3/B4
alone, since their ten-group aggregates reduce enormously and must keep
aggregating early.

**Change.** Two coupled lines, because either alone is inert.
`provider.rs::scan` packs the loaded batches round-robin into at most
`state.config().target_partitions()` partitions instead of one partition per
batch (the empty-partition guard from the range-read cycle is untouched).
`lib.rs::run_sql_env` lowers
`execution.skip_partial_aggregation_probe_rows_threshold` from 100_000 to
8192, because a partition here holds 19 K–76 K rows and so never reached the
default check either. The ratio threshold (0.8) is left alone, so the
decision of *whether* to skip is still DataFusion's and still evidence-based.

**Measurement.** Paired runs on the isolated instance, harness in-network,
100 Shape A samples. All four runs clean (ingest 753–774 K lines/s) and all
four settled to **62 parquet files / 69 MB**, so the two arms queried the same
shape of data. Labels `perf-base2`/`perf-base3`, `perf-cand1`/`perf-cand2`,
cold/warm ms:

| Metric | Baseline | Candidate |
|---|---|---|
| B1_funnel_24h | 65/62, 81/66 | **52/48, 54/47** |
| B2_funnel_48h | 117/132, 140/142 | **103/104, 110/95** |
| B3_inflight_24h | 13/12, 15/13 | 13/11, 11/11 |
| B4_hourly_throughput_48h | 18/18, 20/20 | 19/18, 16/16 |
| B5_route_rollup_24h | 26/28, 29/28 | 28/28, 25/24 |
| Shape A median | 3 ms, 4 ms | 3 ms, 3 ms |
| Ingest | 753K, 774K | 757K, 770K |

86 workspace tests pass, 0 failures; `cargo fmt --all --check` clean;
no clippy findings in `timelake-query`.

**Verdict.** Adopted. B1 goes from 62–81 ms to 47–54 ms and B2 from
117–142 ms to 95–110 ms, with **no overlap between the two sets** on either
query — about 24% off both, close to the predicted 30%. B3/B4/B5 sit inside
the baseline spread and Shape A is untouched, which is exactly the predicted
selectivity: only the two queries whose group-by explodes to one group per row
were paying for the useless partial pass. `EXPLAIN ANALYZE` on the candidate
confirms the mechanism rather than inferring it — `DataSourceExec:
partitions=24, partition_sizes=[2, 2, …]`, and the inner partial aggregate
drops from 365 ms to **53 ms** of compute and from 218 MB to **28.7 MB** of
peak memory.

**Lesson.**

- **The confirming number is 196,608.** On the candidate the partial
  aggregate reports `reduction_factor=100% (196.6 K/196.6 K)` — it hashes
  196,608 rows and passes the other 1.63 M straight through. That is exactly
  24 partitions × 8192 probe rows, which proves both halves of the change are
  load-bearing: the threshold decides *when* the measurement happens, and the
  packing is what gives the operator a batch left to act on afterwards. A
  future cycle that reverts either half reverts the whole win.
- **Partition count is a tuning knob this engine was setting by accident.**
  `parts = batches.map(|b| vec![b])` reads like free parallelism and is
  really an assertion that every operator above the scan should see exactly
  one batch per partition. Adaptive operators — this one, and anything else
  in DataFusion that measures then adapts — are silently disabled by it. The
  scan's partitioning is now `target_partitions`; whether *that* is the right
  number is unmeasured and worth its own cycle.
- **The remaining B2 cost is now the final aggregate**, 573 ms of compute and
  **410 MB** peak building 1.83 M groups keyed by two dictionary columns.
  That is where the container-memory walk the last cycle recorded comes from,
  and it is the next target. DataFusion's fast column-wise group-values path
  does not cover `Dictionary` keys, so this grouping goes through the
  row-format fallback — worth checking whether handing the aggregate a
  `Utf8View` product_id beats the memory it costs, though FR-2 makes that a
  measurement, not an assumption.
- **The decomposition probe is reusable and cheap.** Six SQL statements
  against a settled instance located 84% of B2's cost in about a minute, and
  `EXPLAIN ANALYZE` named the operator. Do this before writing code, not
  after; it is the second cycle running where the probe, not the idea, was
  the valuable part.

### 2026-08-09 16:45 — Decode candidate files concurrently   [ADOPTED]

**Hypothesis.** The last cycle's rule is to prove a cost is on the critical
path first, so this one measured before it changed anything. On a 24-core
container, `docker stats` during a funnel query shows **~1250% CPU** — the
aggregation above the scan is already running ~12 wide. But `load_pruned`
walks its candidate files in one `for` loop on a single blocking task, so the
load is the one part of a Shape B query pinned to a single core. Decomposing
B2 against a settled 62-file instance:

| Probe | Median |
|---|---|
| `COUNT(step)` over 48 h — scan, no real aggregation | 37 ms |
| `COUNT(product_id), COUNT(step)` with the `event='stop'` filter | 64 ms |
| B2 in full (`COUNT(DISTINCT product_id)`) | 738 ms hammered |

Against harness figures of B3 39 ms, B4 69 ms, B5 72 ms, the serial load is
most of three of the five Shape B queries. Fan it out and those should fall
roughly 2x; B1/B2 should barely move, because they are aggregation-bound.

**Change.** `crates/query/src/provider.rs`. The per-file body of
`load_pruned` moved out to `load_one_file` — files were already independent,
each pruning and decoding through its own `StoreFile`, so nothing needed
untangling. `load_pruned` now runs a `std::thread::scope` with
`scan_threads()` workers (`min(files, cores, 8)`) pulling from an
`AtomicUsize` cursor. Results land in per-file slots and are concatenated in
file order, so a plan sees the same batch sequence as before. The RR-1
reservation became a `Mutex<MemoryReservation>` and `try_grow` stays *inside*
the decode loop — applying it after the join would have meant the memory was
already allocated, which is the guarantee this whole thing exists to keep.
RR-2's deadline is still checked between files, now by every worker; the
first error wins and the rest stop.

**Measurement.** Paired runs on the isolated instance, harness in-network,
100 Shape A samples, ingest 738–768K lines/s throughout (all runs clean).
Labels `perf-base1`/`perf-base2` and `perf-cand1`/`perf-cand2`, 62–63 settled
parquet files each, cold/warm ms:

| Metric | Baseline | Candidate |
|---|---|---|
| B3_inflight_24h | 39/48, 37/29 | **16/11, 11/11** |
| B4_hourly_throughput_48h | 69/65, 52/43 | **20/18, 17/18** |
| B5_route_rollup_24h | 72/72, 62/56 | **26/25, 25/25** |
| B1_funnel_24h | 109/98, 120/99 | 122/88, 81/89 |
| B2_funnel_48h | 252/243, 257/214 | 205/183, 704/472 |
| Shape A median | 4 ms, 4 ms | 3 ms, 4 ms |
| Ingest | 778K, 738K | 738K, 768K |

85 workspace tests pass, 0 failures; `cargo fmt` clean on the touched lines.

**Verdict.** Adopted. B3, B4 and B5 drop **2.3–2.5x** with no overlap between
the two sets and near-identical repeats, which is exactly the predicted size
and exactly the three queries the probe said were load-bound. B1 and B2 are
unchanged-to-noisy, also as predicted — they are dominated by
`COUNT(DISTINCT product_id)` over 200K distinct products, not by the scan.
B2's 704 ms candidate outlier is that aggregation's variance, not a
regression: it sits alongside a 205 ms candidate run and the same query
hammered 15x on *baseline* ranged 379–1079 ms. Shape A is untouched at 3–4 ms
because it already prunes to almost nothing.

**Lesson — and the measurement rig changed under this cycle, twice.**

- **The laptop harness was not measuring the read path at all.** Run
  in-network the whole laptop pass finishes in ~8 s, inside the 10 s flush
  tick, so every scan logged `files_total=0` and all five Shape B queries read
  the *write buffer*. Earlier cycles only saw files because the ~45 ms/request
  port overhead made the run slow enough to cross a tick. **Load and query
  must be separate harness invocations with a settle between** —
  `bench/perf-cycle.sh` now does this, and prints the settled file count so a
  pair can be checked for like-for-like.
- **True in-network numbers are far off the recorded ones.** Ingest is
  **740–780K lines/s**, not the ~590K every earlier entry records; Shape A is
  **4 ms**, confirming the 07:15 finding on the harness's own terms. Any
  comparison against a pre-07:15 figure in this log is invalid.
- **Per-file cost analysis in the bloom entry was contaminated too.** Its
  "0.30 ms per file at laptop scale" divided a 50 ms figure that was ~45 ms
  transport by 167 files. The real per-file number was ~0.03 ms, which is why
  that cycle's candidate-set reasoning pointed at a cost that was never large.
- **What is left on Shape B is the aggregation, not the I/O.** B1/B2 are
  `COUNT(DISTINCT product_id)` over 200K products; the scan under them is now
  ~15 ms of a ~100–250 ms query. The next cycle should look there — and
  should note that repeated B2 runs walked container memory from 2.4 to
  3.6 GiB, because `load_pruned` still materialises every batch before the
  plan starts. That is the streaming-exec lead, and it now has a number.

### 2026-08-09 07:15 — TCP_NODELAY on the listeners   [REJECTED — but read the lesson, it invalidates a metric]

**Hypothesis.** Following the last cycle's rule, measure before optimising. A
single lookup was timed directly with `curl` at **6 ms** while the harness
reported a 46 ms median for the same query shape against the same instance.
Something outside the engine was eating ~40 ms. `axum::serve` on a bare
`TcpListener` never sets `TCP_NODELAY` — and the TLS path gets it free from
axum-server, which the plaintext path does not — so Nagle holding the tail of
each response until the peer's delayed ACK fired looked like the answer.
Setting it should have collapsed the harness median from ~46 ms to ~6 ms.

**Change.** `ListenerExt::tap_io` to set nodelay on every accepted HTTP
connection, `tcp_nodelay(true)` on the tonic builder, and `set_nodelay` on the
TCP socket before the TLS wrap in the Flight accept loop.

**Measurement.** It is not the server. With the change in, the same Python
client still measured 47.9 ms median and **100 of 100 requests over 35 ms**.
Isolating the variables against one instance, one query, one product id:

| Client | Result |
|---|---|
| `curl`, new connection each time | 5.7–6.6 ms |
| `curl`, one process, connection reused | 2.4, 0.5, 0.4, 0.4 ms |
| Python `requests`, `Connection: close` | 4.5–22 ms |
| Python `requests`, keep-alive | 9 ms once, then **47, 47, 48, 48 ms** |
| Python `requests`, keep-alive, forced client `TCP_NODELAY` | still 45–53 ms |
| **Python `requests`, keep-alive, run INSIDE the docker network** | **2.7 ms median, 0/30 over 35 ms** |

**Verdict.** Rejected — the change fixes nothing, because nothing in the server
was broken. Harness runs bore it out: 4 ms, 52 ms, 45 ms across three
candidates, which is the artifact toggling rather than an effect.

**Lesson — this is the deliverable, and it is bigger than the change.**
**Shape A latency as this harness reports it is roughly 94% Docker Desktop
port-forwarding overhead on Windows.** The same client, the same server and the
same query cost 2.7 ms inside the docker network and ~47 ms through the
published port, and only on a reused connection. Consequences:

- **True server-side Shape A at laptop scale is ~3 ms, not ~46 ms.** Every
  Shape A figure in this log, and in the M4/M5 record if it was measured the
  same way, carries ~45 ms of overhead that has nothing to do with the engine.
- **Any Shape A improvement smaller than the artifact is invisible**, which is
  the most likely reason two cycles measured real mechanisms as "no change".
  The bloom cycle's 57 → 48 ms was, in server terms, more like 12 → 3 ms: a
  much larger win than it was credited with.
- **Shape B is far less affected** — its true cost is 50–150 ms, so a fixed
  ~45 ms distorts but does not dominate it.

**What the next cycle must do about it.** Run the harness where the client and
server share a network — `docker run --network container:tldb-perf` with the
harness inside, or a compose service — and re-baseline. Until then, do not
trust a Shape A delta under ~45 ms, and do not spend a cycle optimising
milliseconds that the transport is hiding. It is also worth re-measuring the
adopted bloom change this way to record what it is actually worth.

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

- **Laptop scale never compacts.** `timelake_compactions_total` was 0 with 114
  L0 files — the whole run finishes in ~20 s and the compaction tick is 30 s. It
  is therefore a clean measurement of the fresh-data path and completely blind to
  anything about compaction. Use it deliberately, or drive compaction explicitly.
- **The storage metric is unusable at this scale.** Identical baseline runs
  reported 0.09, 0.20, 0.36 and 0.40 GB, because it is sampled right after ingest
  with a variable amount still unflushed. Ignore it below full scale.
- **Shape A needs `--shape-a-samples 100`.** At the default 20 the median swung
  54→82 ms between identical runs; at 100 the same configuration repeats within
  3 ms. Every number above uses 100.
