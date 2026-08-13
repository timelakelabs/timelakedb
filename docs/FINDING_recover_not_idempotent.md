# A second recover permanently doubles every recovered count

**Found 2026-08-13** by Catchment's `ingester-kill` scenario, on its
second execution. Reproduced identically in the kill and stop phases,
with arithmetic that names the mechanism exactly.

---

## The symptom

The scenario runs the CL-2 failover runbook (`cl2_drill.sh`): kill the
owner mid-stream, `POST /internal/v1/recover` on the survivor, restart
the owner, then — because the survivor's queryable copy freezes at the
recovery point while the returned owner takes new writes — run the same
recover once more to bring the survivor current. The doc comment on
`recover_from_replica` said that was safe: *"Idempotent: recovering
twice re-applies the same rows to the same primary keys."*

After the second recover, same table, same instant:

| Node | rows | distinct |
|---|---|---|
| `ingester-a` (owner, restarted) | 4,500 | 4,500 |
| `ingester-b` (survivor, recovered twice) | **7,000** | 4,500 |

Stop phase, same shape: 76,500 rows, 39,500 distinct. In each case the
excess equals the first recovery's row count exactly: the second recover
re-applied everything the first had already applied.

Evidence: `catchment/results/ingester-kill-20260813-190733/run.json`.

---

## The cause

Three facts compose:

1. `recover_from_replica()` replayed the **entire** replica WAL every
   time — nothing marked frames as applied.
2. Each recover ends in `flush_all()`, so the re-applied rows land in a
   **new Parquet file**. The duplicates are not in one buffer where
   flush-time LWW dedup would collapse them; they are in separate files.
3. Cross-file dedup completes **at compaction** (FR-5), and a partition
   only compacts at `compact_min_files` files — default 4. Two files
   never reach four, so the "collapses at compaction" the comment leaned
   on never runs, and every query over the partition serves the
   duplicates indefinitely.

The idempotency claim was true at the primary-key level and false at the
only level a reader sees. It is the same failure direction as the
catalog catch_up race (`FINDING_catalog_catch_up_race.md`): a count
inflated with full confidence — the exact inverse of the failure this
database was specified to prevent.

Related, out of scope here: on a shared object store, even a *single*
recover overlaps rows the dead peer had already flushed, with the same
"safe at compaction" reasoning and the same may-never-compact caveat.
That window closes when compaction runs; the double-recover window never
closed, which is why it is the one fixed here.

---

## The fix

Recovery now **consumes** what it replays: seal the WAL with
`rotate()`, replay the sealed generations, `flush_all()`, then
`delete_generations_upto(sealed)` — all under the `replica_wal` mutex,
which the old code did not hold while reading (a frame arriving between
the read and the delete would have been consumed unapplied: acked loss).
Frames arriving mid-recovery land in the fresh generation and belong to
the next recover — which is also what makes the second recover mean
what an operator means by it: *apply what arrived since the first.*

Order matters at the tail: flush before delete, so a crash between them
re-applies one recovery's frames (the narrow remnant of the old bug,
closed by the next compaction) rather than losing them.

## Status

**Fixed 2026-08-13.** Regression test
`recovering_twice_serves_each_row_once` covers the cautious
double-recover (0 frames, count unchanged) and the catch-up flow (only
the new frames), and was verified to fail against the unfixed code.
Catchment's `ingester-kill` asserts the end state on a live cluster:
after the second recover, one answer everywhere.
