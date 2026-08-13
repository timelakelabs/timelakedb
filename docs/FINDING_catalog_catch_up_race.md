# A querier returns every row N times under concurrent read load

**Found 2026-08-13** by Catchment's `read-gate` scenario, on its first real
execution. Not a hypothesis: reproduced twice with different multipliers, and
traced to a specific non-atomic sequence in `crates/catalog/src/lib.rs`.

---

## The symptom

Same table, same cluster, same instant, two read paths:

| Path | rows | distinct | rows per `idx` |
|---|---|---|---|
| `cl3-ingester-a` (holds the data) | 10,000 | 10,000 | **1** |
| `cl3-querier-a` | 80,000 | 10,000 | **8** |
| `cl3-querier-b` | 60,000 | 10,000 | 6 |

`COUNT(DISTINCT idx)` is correct everywhere and `no_loss_after_ack` passes:
**nothing is missing.** Rows are returned many times over, the two queriers
disagree with each other, and the count is stable across repeated queries
once load stops.

At the time of measurement `timelake_flushes_total` was **1** and
`timelake_catalog_head` was **1** — one flushed file, one manifest entry. So
this is not duplicate files on the object store, and not a repeated flush.

Evidence: `catchment/results/read-gate-20260813-145357/run.json`.

---

## The cause

`Catalog::catch_up()` is not atomic between reading the head and publishing
it:

```rust
let head = self.seq.load(SeqCst);            // 1. read head
for path in self.store.list("catalog/manifest")? {
    if entry.seq > head { newer.push(...) }  // 2. select entries above it
}
let mut files = self.files.lock()...;        // 3. lock taken only here
for (seq, entry) in newer {
    apply_entry(&mut files, entry);          // 4. apply
}
drop(files);
self.seq.store(new_head, SeqCst);            // 5. publish head
```

The `files` mutex covers step 4 alone. Steps 1 through 5 are not one
critical section, so N concurrent callers all read the same stale `head` at
step 1, all select the same manifest entries at step 2, and all apply them
at step 4. `apply_entry` appends unconditionally:

```rust
for f in entry.add_files {
    files.entry((f.db, f.table)).or_default().push(f);   // no dedup by path
}
```

So one manifest entry becomes N copies of the same `FileMeta`, and every
subsequent query scans that file N times.

**A querier calls this on every query.** `Engine::sql_batches` folds the
manifest forward to the ingesters' reported watermark before reading, so
concurrent queries mean concurrent `catch_up()`. The read gate (D2) makes
this *more* visible rather than less: refused snapshots leave the surviving
queries racing on the catalog.

The struct already documents the exact reasoning this path is missing:

```rust
/// Serializes this process's own commits so seq allocation, CAS, and the
/// in-memory apply are one critical section.
commit_lock: Mutex<()>,
```

The **commit** path takes that lock and is safe. `catch_up()` does not take
it, and is not.

Every observation follows:

- **Only queriers.** An ingester commits (locked) and rarely catch_ups
  concurrently; a querier catch_ups on every query.
- **Different factors per querier.** Each lost a different number of races.
- **Stable afterwards.** Once `seq` is current, `catch_up()` returns early.
- **`distinct` correct, `rows` multiplied.** The same file scanned N times.
- **One flush, one manifest entry, eight copies.** Duplication is in memory,
  not on the store.

---

## Why this direction is the dangerous one

A short count is visibly wrong and gets investigated. `COUNT(*)` returning
eight times the truth reads as a healthy system with more data than
expected — and it is the exact inverse of the InfluxDB failure this database
was specified to prevent. Every aggregate over the affected table is wrong
in a way that looks like success: sums, averages, rates, and any dashboard
built on them.

It also breaks the CL-3 claim in `README.md` — "counts are exact seconds
after ingest rather than after the next flush" — under precisely the load a
querier exists to serve.

---

## The fix

Make `catch_up()` one critical section, the way `commit()` already is: take
`commit_lock` across read-head → select → apply → store-head. It is the same
lock and the same argument, and it serialises a path that is already doing
object-store I/O, so contention is not the concern here.

Belt and braces, and worth having independently: make `apply_entry`
idempotent by file path, so a re-applied entry cannot duplicate a file even
if some future caller reaches it another way. The catalog is a log folded
into a set; folding the same record twice should be a no-op.

A regression test wants to be concurrent — N threads calling `catch_up()`
against one manifest entry, asserting the file appears exactly once.
`read-gate` covers it end to end but takes six minutes and a five-node
cluster.

---

## Status

Not fixed. Recorded so the finding is not lost, and so the fix can be judged
against a written account of the race rather than against a memory of it.
`read-gate` is committed in a failing state on purpose: it found this, and
it should keep failing until the catalog stops duplicating.
