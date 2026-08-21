//! When a partition is worth compacting.
//!
//! Two independent triggers, for two unrelated reasons.
//!
//! **Count** — `compact_min_files` or more files in one partition. This is
//! about read amplification: every extra file is another thing a scan has
//! to open. It is a performance trigger and it was the only one.
//!
//! **Overlap** — two files whose time ranges intersect, however few files
//! there are. This is about *correctness*, and it exists because of
//! `docs/FINDING_rebalance_duplicates_replayed_writes.md` (C4).
//!
//! ## Why overlap means duplicates
//!
//! Duplicate primary keys written to different nodes collapse in exactly
//! one place: the cross-file last-write-wins pass inside a compaction. So
//! twins that never share a compaction are served twice, by a querier that
//! unions both files and is entirely confident about it. C4 measured
//! 202,000 rows where 200,000 were written, 200,000 distinct — a COUNT
//! inflated with full confidence, the precise inverse of what this
//! database is specified to prevent.
//!
//! With a count trigger alone, a partition holding two or three
//! duplicate-bearing files never reaches four and never compacts. The
//! finding's phrase is *"potentially forever"*.
//!
//! Normal ingest does not produce overlapping files: timestamps advance,
//! so each flush covers a later range than the last. It takes a replay, a
//! late arrival, or a crash-window recovery to make two files in one
//! partition cover the same instants — which is precisely the set of
//! events that can produce twins.
//!
//! ## Why the overlap test is strict
//!
//! `prev.max_ts_ns > next.min_ts_ns`, not `>=`. Touching at a single point
//! is not overlap, and the distinction is the whole difference between a
//! correctness fix and a write-amplification regression.
//!
//! Real fleets report on a shared tick: every host emits at the same
//! instant, every ten seconds. A flush boundary landing mid-tick puts that
//! one timestamp at the end of one file and the start of the next. Under a
//! `>=` test that is an "overlap", and since it happens at nearly every
//! flush boundary, nearly every adjacent pair of files would compact
//! forever. The engine would spend its life rewriting files that contain
//! no duplicates at all.
//!
//! Under `>`, a shared boundary instant is ignored and a genuine twin —
//! two files covering the same *span* — is caught.
//!
//! ## What this deliberately does not catch
//!
//! Two files that each contain rows at exactly one timestamp, the same
//! timestamp, and nothing else. Their ranges touch at a point and are
//! caught by neither branch.
//!
//! That is a real gap and it is accepted rather than papered over. Closing
//! it means treating point-files specially, and the cure costs more than
//! the disease: a file holds up to `flush_rows` rows, so for one to be a
//! point-file every row in it must share a nanosecond. The C4 case is
//! 500-line batches spanning real time, and those are caught. If a
//! workload ever produces point-files in volume, revisit this with a
//! measurement rather than a guess.

use timelake_catalog::FileMeta;

/// Does this partition need compacting, and why.
///
/// The reason is returned rather than a bare bool so the caller can log
/// it. "Compaction ran" and "compaction ran because two files overlapped"
/// are very different lines to find in a log at 2am when a count looks
/// wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    /// `compact_min_files` or more files: read amplification.
    FileCount,
    /// Time ranges intersect: possible duplicate primary keys (C4).
    Overlap,
}

impl Trigger {
    pub fn as_str(self) -> &'static str {
        match self {
            Trigger::FileCount => "file_count",
            Trigger::Overlap => "overlap",
        }
    }
}

/// Why this partition should be compacted, or `None` to leave it alone.
///
/// Count is checked first because it is cheaper and because a partition
/// over the file threshold would be compacted regardless of overlap — so
/// the reported reason stays the one an operator tuning
/// `compact_min_files` would expect.
pub fn trigger_for(files: &[FileMeta], min_files: usize) -> Option<Trigger> {
    if files.len() >= min_files.max(2) {
        return Some(Trigger::FileCount);
    }
    if has_overlap(files) {
        return Some(Trigger::Overlap);
    }
    None
}

/// Whether any two of these files cover a common span of time.
///
/// Sorts by start and compares each file against the furthest end seen so
/// far, which is O(n log n) and correct for the case a naive
/// adjacent-pairs scan gets wrong: one wide file followed by several
/// narrow ones inside it, where no *adjacent* pair overlaps but the wide
/// one overlaps them all.
pub fn has_overlap(files: &[FileMeta]) -> bool {
    if files.len() < 2 {
        return false;
    }
    let mut ranges: Vec<(i64, i64)> = files.iter().map(|f| (f.min_ts_ns, f.max_ts_ns)).collect();
    ranges.sort_unstable();

    let mut furthest_end = ranges[0].1;
    for &(start, end) in &ranges[1..] {
        // Strict: a shared boundary instant is not an overlap. See the
        // module docs -- this is what keeps a tick-aligned fleet from
        // compacting on every flush boundary.
        if start < furthest_end {
            return true;
        }
        furthest_end = furthest_end.max(end);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(min: i64, max: i64) -> FileMeta {
        FileMeta {
            db: "poc".into(),
            table: "t".into(),
            partition: "2026082112".into(),
            path: format!("p-{min}-{max}"),
            rows: 1,
            size_bytes: 1,
            min_ts_ns: min,
            max_ts_ns: max,
        }
    }

    #[test]
    fn ascending_flushes_do_not_overlap() {
        // The common case, and the one that must stay cheap: each flush
        // covers a later window than the last.
        let files = vec![file(0, 99), file(100, 199), file(200, 299)];
        assert!(!has_overlap(&files));
        assert_eq!(trigger_for(&files, 4), None);
    }

    #[test]
    fn a_shared_boundary_instant_is_not_an_overlap() {
        // A tick-aligned fleet splits one timestamp across a flush
        // boundary constantly. Treating that as overlap would compact
        // nearly every adjacent pair of files, forever.
        let files = vec![file(0, 100), file(100, 200)];
        assert!(!has_overlap(&files));
        assert_eq!(trigger_for(&files, 4), None);
    }

    #[test]
    fn twins_overlap_and_trigger_below_the_file_threshold() {
        // The C4 case: the same batch landing twice, in two files, in a
        // partition that will never reach compact_min_files.
        let files = vec![file(1000, 2000), file(1000, 2000)];
        assert!(has_overlap(&files));
        assert_eq!(trigger_for(&files, 4), Some(Trigger::Overlap));
    }

    #[test]
    fn a_partial_replay_overlaps() {
        // Only some of a batch re-shipped: ranges intersect without being
        // equal.
        let files = vec![file(1000, 2000), file(1500, 2500)];
        assert!(has_overlap(&files));
    }

    #[test]
    fn a_late_arrival_overlaps() {
        // Out-of-order: a file covering a span already written. This is
        // also the recover-crash-window tail from
        // FINDING_recover_not_idempotent.md.
        let files = vec![file(1000, 2000), file(1200, 1300)];
        assert!(has_overlap(&files));
    }

    #[test]
    fn a_wide_file_containing_narrow_ones_is_caught() {
        // The case an adjacent-pairs scan misses: sorted by start, no two
        // NEIGHBOURS overlap, but the first contains the third.
        let files = vec![file(0, 10_000), file(20_000, 30_000), file(5_000, 6_000)];
        assert!(has_overlap(&files));
    }

    #[test]
    fn the_count_trigger_still_works_and_is_reported_as_itself() {
        let files = vec![file(0, 9), file(10, 19), file(20, 29), file(30, 39)];
        assert_eq!(trigger_for(&files, 4), Some(Trigger::FileCount));
    }

    #[test]
    fn one_file_is_never_a_candidate() {
        assert!(!has_overlap(&[file(0, 100)]));
        assert_eq!(trigger_for(&[file(0, 100)], 4), None);
        assert_eq!(trigger_for(&[], 4), None);
    }

    #[test]
    fn min_files_below_two_cannot_make_a_single_file_compact() {
        // A misconfigured compact_min_files of 0 or 1 would otherwise mean
        // "compact every partition on every pass", rewriting a single
        // settled file into a copy of itself forever.
        assert_eq!(trigger_for(&[file(0, 100)], 0), None);
        assert_eq!(trigger_for(&[file(0, 100)], 1), None);
        assert_eq!(
            trigger_for(&[file(0, 100), file(200, 300)], 0),
            Some(Trigger::FileCount)
        );
    }

    #[test]
    fn point_files_at_the_same_instant_are_the_known_gap() {
        // Documented in the module docs. Asserted so the limitation is
        // visible and a future change that closes it fails this test
        // loudly rather than silently altering the trigger's shape.
        let files = vec![file(500, 500), file(500, 500)];
        assert!(
            !has_overlap(&files),
            "if this now passes, the point-file gap has been closed -- \
             update the module docs and this test together"
        );
    }
}
