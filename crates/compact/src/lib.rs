//! Compaction (PR-6): merge a partition's L0 files into one settled file.
//!
//! This is also where FR-5 *completes*: flush dedups within a batch, and
//! the cross-file duplicates that survive (retries across flush windows,
//! crash-window WAL replays) collapse here, because merge = align →
//! concat → the same PK-sort + last-write-wins pass flush uses. Order
//! matters: batch sets must be provided oldest-file-first so "last write"
//! is the newest file's row.
//!
//! Key rotation (SEC-1) and re-clustering ride these rewrites later.

use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::record_batch::RecordBatch;
use timelord_buffer::flush;
use timelord_query::align;

pub struct MergeResult {
    pub bytes: Vec<u8>,
    pub rows: u64,
    pub min_ts_ns: i64,
    pub max_ts_ns: i64,
}

/// Merge the batches of several files (oldest first) into one Parquet
/// blob. All inputs must belong to one (table, hour partition).
pub fn merge_files(batch_sets: Vec<Vec<RecordBatch>>) -> Result<MergeResult, String> {
    let batches: Vec<RecordBatch> = batch_sets.into_iter().flatten().collect();
    if batches.is_empty() {
        return Err("nothing to merge".into());
    }
    let (schema, aligned) = align(batches)?;
    let combined = concat_batches(&schema, &aligned).map_err(|e| e.to_string())?;

    // Cluster settled files by the highest-cardinality tag column so
    // row-group statistics on it become tight, prunable ranges (Shape A
    // without bloom filters — the arrow writer emits none for dictionary
    // columns). Self-tuning: no per-workload config.
    let cluster: Option<String> = {
        use datafusion::arrow::array::DictionaryArray;
        use datafusion::arrow::datatypes::{DataType, Int32Type};
        schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| matches!(f.data_type(), DataType::Dictionary(_, _)))
            .filter_map(|(i, f)| {
                combined
                    .column(i)
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .map(|d| (d.values().len(), f.name().clone()))
            })
            .max()
            .map(|(_, name)| name)
    };

    let mut parts = flush::prepare_ordered(&combined, cluster.as_deref())?;
    if parts.len() != 1 {
        return Err(format!(
            "merge crossed hour partitions ({} produced) — inputs must share one partition",
            parts.len()
        ));
    }
    let (_, merged) = parts.pop().unwrap();
    let (min_ts_ns, max_ts_ns) = flush::time_bounds(&merged);
    Ok(MergeResult {
        rows: merged.num_rows() as u64,
        // 64K-row groups: ~12 tight entity ranges per hour partition
        bytes: flush::to_parquet_bytes_rg(&merged, Some(65_536))?,
        min_ts_ns,
        max_ts_ns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use timelord_buffer::TableBuffer;
    use timelord_ingest::parse_lines;

    fn batch(lp: &str) -> RecordBatch {
        let mut buf = TableBuffer::default();
        for line in parse_lines(lp, 1, 0).unwrap() {
            buf.append(&line).unwrap();
        }
        buf.snapshot().unwrap()
    }

    #[test]
    fn cross_file_lww_dedup_completes_fr5() {
        let t = 1_786_179_600_000_000_000i64; // one fixed hour
        // file 1: original write; file 2 (newer): retry with a new value
        // and one genuinely new row
        let f1 = batch(&format!("m,tag=a v=1.0 {t}\nm,tag=b v=5.0 {}", t + 1));
        let f2 = batch(&format!("m,tag=a v=2.0 {t}\nm,tag=c v=7.0 {}", t + 2));
        let merged = merge_files(vec![vec![f1], vec![f2]]).unwrap();
        assert_eq!(merged.rows, 3, "duplicate PK across files collapsed");
        assert_eq!((merged.min_ts_ns, merged.max_ts_ns), (t, t + 2));

        let back = flush::read_parquet_bytes(merged.bytes).unwrap();
        let all = back.iter().map(|b| b.num_rows()).sum::<usize>();
        assert_eq!(all, 3);
        // the survivor for (t, tag=a) is the NEWER file's value
        let b0 = &back[0];
        let tags = b0.column_by_name("tag").unwrap();
        let vals = b0
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();
        for i in 0..b0.num_rows() {
            let tag =
                datafusion::arrow::util::display::array_value_to_string(tags, i).unwrap();
            if tag == "a" {
                assert_eq!(vals.value(i), 2.0, "newest file wins");
            }
        }
    }

    #[test]
    fn schema_evolution_across_files_merges() {
        let t = 1_786_179_600_000_000_000i64;
        let f1 = batch(&format!("m,tag=a v=1.0 {t}"));
        let f2 = batch(&format!("m,tag=b,route=x v=2.0,extra=9i {}", t + 1));
        let merged = merge_files(vec![vec![f1], vec![f2]]).unwrap();
        assert_eq!(merged.rows, 2);
        let back = flush::read_parquet_bytes(merged.bytes).unwrap();
        assert!(back[0].column_by_name("route").is_some());
        assert!(back[0].column_by_name("extra").is_some());
    }
}
