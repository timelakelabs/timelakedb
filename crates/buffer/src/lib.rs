//! Mutable per-table buffer with immutable Arrow snapshots (PR-9 readers
//! never block writers — a snapshot is a fresh RecordBatch).
//!
//! FR-2 in code: tags are interned per column (HashMap<value, key> +
//! value list) and snapshot as `Dictionary<Int32, Utf8>` — cost is a
//! compressed column, never a series index. Field types are
//! first-writer-wins (ints promote into float columns; anything else is
//! a 400 upstream, FR-9's "errors identify the line" contract).
//!
//! M1 scope: everything lives in memory; Parquet flush arrives at M2 —
//! the snapshot boundary is exactly where flush will slot in.

use std::collections::HashMap;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int64Array, StringArray,
    TimestampNanosecondArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

use timelake_ingest::{FieldValue, ParsedLine};

const TZ: &str = "+00:00";

#[derive(Default)]
struct TagCol {
    intern: HashMap<String, i32>,
    values: Vec<String>,
    keys: Vec<Option<i32>>, // one entry per row; None = tag absent
}

impl TagCol {
    fn pad_to(&mut self, n: usize) {
        self.keys.resize(n, None);
    }

    fn push(&mut self, v: &str) {
        let next = self.values.len() as i32;
        let key = *self.intern.entry(v.to_string()).or_insert_with(|| {
            self.values.push(v.to_string());
            next
        });
        self.keys.push(Some(key));
    }
}

enum FieldCol {
    F64(Vec<Option<f64>>),
    I64(Vec<Option<i64>>),
    U64(Vec<Option<u64>>),
    Bool(Vec<Option<bool>>),
    Str(Vec<Option<String>>),
}

impl FieldCol {
    fn new(v: &FieldValue) -> Self {
        match v {
            FieldValue::Float(_) => FieldCol::F64(Vec::new()),
            FieldValue::Int(_) => FieldCol::I64(Vec::new()),
            FieldValue::UInt(_) => FieldCol::U64(Vec::new()),
            FieldValue::Bool(_) => FieldCol::Bool(Vec::new()),
            FieldValue::Str(_) => FieldCol::Str(Vec::new()),
        }
    }

    fn pad_to(&mut self, n: usize) {
        match self {
            FieldCol::F64(v) => v.resize(n, None),
            FieldCol::I64(v) => v.resize(n, None),
            FieldCol::U64(v) => v.resize(n, None),
            FieldCol::Bool(v) => v.resize(n, None),
            FieldCol::Str(v) => v.resize(n, None),
        }
    }

    /// Would `push` accept this value? Lets a whole line be validated
    /// before any column is touched — see [`TableBuffer::append`].
    fn accepts(&self, v: &FieldValue) -> bool {
        matches!(
            (self, v),
            (
                FieldCol::F64(_),
                FieldValue::Float(_) | FieldValue::Int(_) | FieldValue::UInt(_)
            ) | (FieldCol::I64(_), FieldValue::Int(_))
                | (FieldCol::U64(_), FieldValue::UInt(_))
                | (FieldCol::Bool(_), FieldValue::Bool(_))
                | (FieldCol::Str(_), FieldValue::Str(_))
        )
    }

    fn push(&mut self, v: &FieldValue, col: &str) -> Result<(), String> {
        match (self, v) {
            (FieldCol::F64(c), FieldValue::Float(x)) => c.push(Some(*x)),
            // lenient promotion: ints land in float columns
            (FieldCol::F64(c), FieldValue::Int(x)) => c.push(Some(*x as f64)),
            (FieldCol::F64(c), FieldValue::UInt(x)) => c.push(Some(*x as f64)),
            (FieldCol::I64(c), FieldValue::Int(x)) => c.push(Some(*x)),
            (FieldCol::U64(c), FieldValue::UInt(x)) => c.push(Some(*x)),
            (FieldCol::Bool(c), FieldValue::Bool(x)) => c.push(Some(*x)),
            (FieldCol::Str(c), FieldValue::Str(x)) => c.push(Some(x.clone())),
            (_, other) => return Err(type_conflict(col, other)),
        }
        Ok(())
    }
}

fn type_conflict(col: &str, v: &FieldValue) -> String {
    format!(
        "field '{col}' type conflict: column was created with a \
         different type than {v:?}"
    )
}

/// One table's mutable buffer.
#[derive(Default)]
pub struct TableBuffer {
    times: Vec<i64>,
    tag_names: Vec<String>,
    tags: HashMap<String, TagCol>,
    field_names: Vec<String>,
    fields: HashMap<String, FieldCol>,
    first_write: Option<std::time::Instant>,
}

impl TableBuffer {
    /// Seconds since the first row landed (flush-age trigger).
    pub fn age_secs(&self) -> u64 {
        self.first_write.map(|t| t.elapsed().as_secs()).unwrap_or(0)
    }
}

impl TableBuffer {
    pub fn row_count(&self) -> usize {
        self.times.len()
    }

    /// Append one parsed line. On error the row is NOT applied (the whole
    /// request was already validated upstream; errors here are type
    /// conflicts, reported with the field name).
    /// Append one row.
    ///
    /// **Atomic**: a rejected line leaves the buffer exactly as it was. It
    /// has to be. Pushing tag values first and discovering a field type
    /// conflict afterwards left the tag columns one longer than `times`,
    /// and from then on every `snapshot()` failed with "all columns in a
    /// record batch must have the same length" — which took out reads on
    /// the table, the flush that would have drained it, and (because
    /// maintenance was one sequential task) compaction and retention for
    /// every other table on the node. One 400 poisoned the whole engine,
    /// and the WAL replayed it at boot, so a restart did not clear it.
    pub fn append(&mut self, line: &ParsedLine) -> Result<(), String> {
        // Validate every field — against the column it would land in, and
        // against any column this same line is about to create — BEFORE
        // touching anything.
        let mut pending: Vec<(&String, FieldCol)> = Vec::new();
        for (k, v) in &line.fields {
            if let Some(col) = self.fields.get(k) {
                if !col.accepts(v) {
                    return Err(type_conflict(k, v));
                }
            } else if let Some((_, col)) = pending.iter().find(|(name, _)| *name == k) {
                if !col.accepts(v) {
                    return Err(type_conflict(k, v));
                }
            } else {
                pending.push((k, FieldCol::new(v)));
            }
        }

        self.first_write.get_or_insert_with(std::time::Instant::now);
        let n = self.times.len();

        for (k, v) in &line.tags {
            let col = self.tags.entry(k.clone()).or_insert_with(|| {
                self.tag_names.push(k.clone());
                let mut c = TagCol::default();
                c.keys.resize(n, None); // backfill rows written before this tag existed
                c
            });
            // A key repeated inside one line overwrites rather than appends:
            // two pushes would leave this column longer than `times`.
            col.pad_to(n);
            col.push(v);
        }
        for (k, v) in &line.fields {
            let col = self.fields.entry(k.clone()).or_insert_with(|| {
                self.field_names.push(k.clone());
                let mut c = FieldCol::new(v);
                c.pad_to(n);
                c
            });
            col.pad_to(n); // same rule for a repeated field key: last wins
            col.push(v, k)?; // cannot fail — validated above
        }

        // pad columns this row didn't mention
        self.times.push(line.timestamp_ns);
        let n = self.times.len();
        for name in &self.tag_names {
            let c = self.tags.get_mut(name).unwrap();
            if c.keys.len() < n {
                c.keys.push(None);
            }
        }
        for name in &self.field_names {
            let c = self.fields.get_mut(name).unwrap();
            c.pad_to(n);
        }
        Ok(())
    }

    /// The buffer's schema without materializing any arrays (cheap —
    /// used to keep the engine's schema registry current).
    pub fn schema_only(&self) -> Arc<Schema> {
        let mut fields: Vec<Field> =
            Vec::with_capacity(1 + self.tag_names.len() + self.field_names.len());
        fields.push(Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(TZ.into())),
            false,
        ));
        for name in &self.tag_names {
            fields.push(Field::new(
                name,
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ));
        }
        for name in &self.field_names {
            let dt = match &self.fields[name] {
                FieldCol::F64(_) => DataType::Float64,
                FieldCol::I64(_) => DataType::Int64,
                FieldCol::U64(_) => DataType::UInt64,
                FieldCol::Bool(_) => DataType::Boolean,
                FieldCol::Str(_) => DataType::Utf8,
            };
            fields.push(Field::new(name, dt, true));
        }
        Arc::new(Schema::new(fields))
    }

    /// Immutable snapshot as one RecordBatch (time, tags..., fields...).
    pub fn snapshot(&self) -> Result<RecordBatch, String> {
        let mut fields: Vec<Field> = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();

        fields.push(Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(TZ.into())),
            false,
        ));
        arrays.push(Arc::new(
            TimestampNanosecondArray::from(self.times.clone()).with_timezone(TZ),
        ));

        for name in &self.tag_names {
            let col = &self.tags[name];
            let keys = datafusion::arrow::array::Int32Array::from(col.keys.clone());
            let values = Arc::new(StringArray::from(col.values.clone()));
            let dict =
                DictionaryArray::<Int32Type>::try_new(keys, values).map_err(|e| e.to_string())?;
            fields.push(Field::new(
                name,
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ));
            arrays.push(Arc::new(dict));
        }

        for name in &self.field_names {
            let (dt, arr): (DataType, ArrayRef) = match &self.fields[name] {
                FieldCol::F64(v) => (DataType::Float64, Arc::new(Float64Array::from(v.clone()))),
                FieldCol::I64(v) => (DataType::Int64, Arc::new(Int64Array::from(v.clone()))),
                FieldCol::U64(v) => (DataType::UInt64, Arc::new(UInt64Array::from(v.clone()))),
                FieldCol::Bool(v) => (DataType::Boolean, Arc::new(BooleanArray::from(v.clone()))),
                FieldCol::Str(v) => (DataType::Utf8, Arc::new(StringArray::from(v.clone()))),
            };
            fields.push(Field::new(name, dt, true));
            arrays.push(arr);
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|e| e.to_string())
    }
}

/// Flush preparation: PK sort + last-write-wins dedup (FR-5 within a
/// flush), UTC-hour partition split (ARCHITECTURE §6), Parquet encoding.
/// Compaction (M3) reuses these pieces for cross-file merges.
pub mod flush {
    use std::collections::BTreeMap;

    /// A tag column gets a bloom filter only above this many distinct
    /// values. Below it the value is in almost every file, so the filter
    /// answers "maybe" every time and is pure cost on the read path.
    const BLOOM_MIN_DISTINCT: usize = 1024;

    use datafusion::arrow::array::{Array, TimestampNanosecondArray, UInt32Array};
    use datafusion::arrow::compute::take;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::row::{RowConverter, SortField};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use datafusion::parquet::basic::Compression;
    use datafusion::parquet::file::properties::WriterProperties;

    /// Sort by primary key (time, tags...), keep the LAST write per PK,
    /// and split into (hour_partition, batch) pieces. L0 flush order:
    /// time-first (file min/max time bounds do the pruning for fresh data).
    pub fn prepare(batch: &RecordBatch) -> Result<Vec<(String, RecordBatch)>, String> {
        prepare_ordered(batch, None)
    }

    /// Like [`prepare`] but with `leading` (a tag column) as the primary
    /// sort key — compaction uses this to CLUSTER settled files by their
    /// hottest entity column, so row-group statistics on that column
    /// become tight ranges and Shape-A pruning works without bloom
    /// filters (which the arrow writer does not emit for dictionary
    /// columns — proven by test, ARCHITECTURE §9 rung 3).
    /// Dedup semantics are unchanged: the sort key SET is the same PK,
    /// only the order differs, so equal PKs remain adjacent.
    pub fn prepare_ordered(
        batch: &RecordBatch,
        leading: Option<&str>,
    ) -> Result<Vec<(String, RecordBatch)>, String> {
        let n = batch.num_rows();
        if n == 0 {
            return Ok(Vec::new());
        }
        let mut pk_cols = Vec::new();
        if let Some(name) = leading
            && let Some(c) = batch.column_by_name(name)
        {
            pk_cols.push(c.clone());
        }
        // time (index 0 by construction) + every dictionary (tag) column
        pk_cols.push(batch.column(0).clone());
        for (i, f) in batch.schema().fields().iter().enumerate().skip(1) {
            if matches!(f.data_type(), DataType::Dictionary(_, _))
                && leading != Some(f.name().as_str())
            {
                pk_cols.push(batch.column(i).clone());
            }
        }
        let converter = RowConverter::new(
            pk_cols
                .iter()
                .map(|c| SortField::new(c.data_type().clone()))
                .collect(),
        )
        .map_err(|e| e.to_string())?;
        let rows = converter
            .convert_columns(&pk_cols)
            .map_err(|e| e.to_string())?;

        let mut idx: Vec<usize> = (0..n).collect();
        // stable: equal PKs stay in arrival order, so .last() is LWW
        idx.sort_by(|&a, &b| rows.row(a).cmp(&rows.row(b)).then(a.cmp(&b)));

        let mut kept: Vec<usize> = Vec::with_capacity(n);
        for &i in &idx {
            if let Some(&prev) = kept.last()
                && rows.row(prev) == rows.row(i)
            {
                *kept.last_mut().unwrap() = i; // last write wins (FR-5)
                continue;
            }
            kept.push(i);
        }

        // split by UTC hour (kept is time-ordered already)
        let times = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or("time column is not ns timestamps")?;
        let mut parts: BTreeMap<String, Vec<u32>> = BTreeMap::new();
        for &i in &kept {
            parts
                .entry(hour_partition(times.value(i)))
                .or_default()
                .push(i as u32);
        }

        let mut out = Vec::with_capacity(parts.len());
        for (hour, indices) in parts {
            let indices = UInt32Array::from(indices);
            let cols = batch
                .columns()
                .iter()
                .map(|c| take(c.as_ref(), &indices, None).map_err(|e| e.to_string()))
                .collect::<Result<Vec<_>, _>>()?;
            out.push((
                hour,
                RecordBatch::try_new(batch.schema(), cols).map_err(|e| e.to_string())?,
            ));
        }
        Ok(out)
    }

    /// (min_ts_ns, max_ts_ns) of a prepared batch (any sort order).
    pub fn time_bounds(batch: &RecordBatch) -> (i64, i64) {
        let times = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("time column");
        let mut min = i64::MAX;
        let mut max = i64::MIN;
        for i in 0..times.len() {
            let v = times.value(i);
            min = min.min(v);
            max = max.max(v);
        }
        (min, max)
    }

    pub fn to_parquet_bytes(batch: &RecordBatch) -> Result<Vec<u8>, String> {
        to_parquet_bytes_rg(batch, None)
    }

    /// `rg_rows` bounds row-group size: compaction writes SMALL groups
    /// (64K) over entity-clustered data so per-group statistics become
    /// tight, prunable ranges. (Bloom filters would be preferable but the
    /// arrow writer does not emit them for dictionary columns.)
    pub fn to_parquet_bytes_rg(
        batch: &RecordBatch,
        rg_rows: Option<usize>,
    ) -> Result<Vec<u8>, String> {
        let mut props = WriterProperties::builder().set_compression(Compression::SNAPPY);
        if let Some(rows) = rg_rows {
            props = props.set_max_row_group_row_count(Some(rows));
        }
        // Bloom filters on the tag columns: an equality lookup can then be
        // told "not in this file" without reading any of it. Only the
        // dictionary columns get one — a bloom over a float measurement
        // would be paid for and never consulted.
        //
        // Sizing matters more than it looks. The writer's default assumes a
        // million distinct values per column chunk and reserves accordingly,
        // which quadrupled total storage when this was first measured. A
        // dictionary column already knows exactly how many distinct values
        // it holds, so say so.
        for (i, f) in batch.schema().fields().iter().enumerate() {
            if !matches!(f.data_type(), DataType::Dictionary(_, _)) {
                continue;
            }
            let Some(d) = batch
                .column(i)
                .as_any()
                .downcast_ref::<super::DictionaryArray<super::Int32Type>>()
            else {
                continue;
            };
            let ndv = d.values().len();
            // Only entity-like columns. A bloom earns its keep by saying
            // "not in this file", which needs the value to be ABSENT from
            // most files — true of a product id, never true of `event`,
            // which has two values and appears in every file. Writing one
            // there costs a probe per file per query and prunes nothing:
            // measured as a ~50% funnel regression before this guard.
            if ndv < BLOOM_MIN_DISTINCT {
                continue;
            }
            let path = datafusion::parquet::schema::types::ColumnPath::from(f.name().as_str());
            props = props.set_column_bloom_filter_enabled(path.clone(), true);
            props = props.set_column_bloom_filter_ndv(path, ndv as u64);
        }
        let props = props.build();
        let mut out = Vec::new();
        let mut w = ArrowWriter::try_new(&mut out, batch.schema(), Some(props))
            .map_err(|e| e.to_string())?;
        w.write(batch).map_err(|e| e.to_string())?;
        w.close().map_err(|e| e.to_string())?;
        Ok(out)
    }

    pub fn read_parquet_bytes(bytes: Vec<u8>) -> Result<Vec<RecordBatch>, String> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .map_err(|e| e.to_string())?
            .build()
            .map_err(|e| e.to_string())?;
        reader
            .into_iter()
            .map(|r| r.map_err(|e| e.to_string()))
            .collect()
    }

    /// "YYYYMMDDHH" in UTC, no chrono dependency (Hinnant's civil algo).
    pub fn hour_partition(ts_ns: i64) -> String {
        let secs = ts_ns.div_euclid(1_000_000_000);
        let days = secs.div_euclid(86_400);
        let sod = secs.rem_euclid(86_400);
        let (y, m, d) = civil_from_days(days);
        format!("{y:04}{m:02}{d:02}{:02}", sod / 3600)
    }

    fn civil_from_days(z: i64) -> (i64, u32, u32) {
        let z = z + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097);
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
        let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
        (if m <= 2 { y + 1 } else { y }, m, d)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;
    use timelake_ingest::parse_lines;

    #[test]
    fn append_snapshot_counts_and_schema_evolution() {
        let lp = "pipeline_events,product_id=p1,step=01-download,event=start value=1i 100\npipeline_events,product_id=p2,step=01-download,event=stop duration_s=9.5 200\npipeline_events,product_id=p1,step=02-extract,route=alpha,event=start value=1i 300";
        let mut buf = TableBuffer::default();
        for line in parse_lines(lp, 1, 0).unwrap() {
            buf.append(&line).unwrap();
        }
        assert_eq!(buf.row_count(), 3);

        let batch = buf.snapshot().unwrap();
        assert_eq!(batch.num_rows(), 3);
        // time + 4 tags (product_id, step, event, route) + 2 fields
        assert_eq!(batch.num_columns(), 7);
        // route arrived at row 3: rows 1-2 are null
        let route = batch
            .column_by_name("route")
            .unwrap()
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert!(route.is_null(0) && route.is_null(1) && !route.is_null(2));
        // dictionary interning: step has 2 distinct values across 3 rows
        let step = batch
            .column_by_name("step")
            .unwrap()
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(step.values().len(), 2);
    }

    #[test]
    fn rejected_line_leaves_the_buffer_usable() {
        // The regression: the conflicting line had already pushed its tag
        // value when the field push failed, leaving `h` one longer than
        // `time`. Every later snapshot then failed with "all columns in a
        // record batch must have the same length" — killing reads of the
        // table, the flush that would have drained it, and the whole
        // maintenance tick behind it.
        let mut buf = TableBuffer::default();
        for line in parse_lines("tt,h=a v=1 100", 1, 0).unwrap() {
            buf.append(&line).unwrap();
        }

        let bad = parse_lines("tt,h=a v=\"oops\" 200", 1, 0).unwrap();
        let err = buf.append(&bad[0]).unwrap_err();
        assert!(err.contains("type conflict"), "unexpected error: {err}");

        assert_eq!(buf.row_count(), 1, "rejected row must not be counted");
        assert_eq!(buf.snapshot().unwrap().num_rows(), 1);

        // and the buffer still accepts good writes afterwards
        for line in parse_lines("tt,h=a v=2 300", 1, 0).unwrap() {
            buf.append(&line).unwrap();
        }
        assert_eq!(buf.snapshot().unwrap().num_rows(), 2);
    }

    #[test]
    fn repeated_key_in_one_line_stays_aligned_and_last_wins() {
        // A duplicate tag key was accepted (204) and pushed twice into the
        // same column — same ragged-column corruption, from a *successful*
        // write.
        let mut buf = TableBuffer::default();
        for line in parse_lines("dt,h=a,h=b v=1,v=2 100", 1, 0).unwrap() {
            buf.append(&line).unwrap();
        }
        let batch = buf.snapshot().expect("snapshot must survive repeated keys");
        assert_eq!(batch.num_rows(), 1);

        let h = batch
            .column_by_name("h")
            .unwrap()
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let vals = h.values().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(vals.value(h.keys().value(0) as usize), "b");

        let v = batch
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(v.value(0), 2.0);
    }

    #[test]
    fn conflicting_repeat_within_one_line_is_rejected_cleanly() {
        let mut buf = TableBuffer::default();
        for line in parse_lines("ct,h=a v=1 100", 1, 0).unwrap() {
            buf.append(&line).unwrap();
        }
        // second `w` conflicts with the column the first `w` would create
        let bad = parse_lines("ct,h=a w=1,w=\"two\" 200", 1, 0).unwrap();
        assert!(buf.append(&bad[0]).is_err());
        assert_eq!(buf.row_count(), 1);
        assert_eq!(buf.snapshot().unwrap().num_rows(), 1);
    }

    #[test]
    fn flush_prepare_dedups_lww_and_splits_hours() {
        // same PK twice (later write wins), plus a second hour partition
        let h0 = 1_754_600_000_000_000_000i64; // some fixed instant
        let h1 = h0 + 3_600_000_000_000; // exactly one hour later
        let lp = format!(
            "m,tag=a v=1.0 {h0}\nm,tag=a v=2.0 {h0}\nm,tag=b v=3.0 {h0}\nm,tag=a v=4.0 {h1}"
        );
        let mut buf = TableBuffer::default();
        for line in parse_lines(&lp, 1, 0).unwrap() {
            buf.append(&line).unwrap();
        }
        let parts = flush::prepare(&buf.snapshot().unwrap()).unwrap();
        assert_eq!(parts.len(), 2, "two hour partitions");
        let (p0, b0) = &parts[0];
        let (p1, b1) = &parts[1];
        assert!(p0 < p1);
        assert_eq!(b0.num_rows(), 2, "duplicate PK collapsed");
        assert_eq!(b1.num_rows(), 1);
        // LWW: the surviving (h0, tag=a) row carries v=4? no — v=2.0
        let v = b0
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let tags = b0.column_by_name("tag").unwrap();
        let tag0 = datafusion::arrow::util::display::array_value_to_string(tags, 0).unwrap();
        let (a_idx, b_idx) = if tag0 == "a" { (0, 1) } else { (1, 0) };
        assert_eq!(v.value(a_idx), 2.0, "last write wins");
        assert_eq!(v.value(b_idx), 3.0);
        let (min, max) = flush::time_bounds(b0);
        assert_eq!((min, max), (h0, h0));

        // parquet roundtrip preserves rows and schema
        let bytes = flush::to_parquet_bytes(b0).unwrap();
        let back = flush::read_parquet_bytes(bytes).unwrap();
        assert_eq!(back.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        assert_eq!(back[0].schema().field(0).name(), "time");
    }

    #[test]
    fn dict_columns_do_get_blooms() {
        // This test used to pin the opposite: "the arrow writer emits no
        // bloom filters for dictionary columns", which sent M4 to entity
        // clustering and row-group statistics instead. The claim was an
        // artifact of the test — the writer defaults blooms OFF and nobody
        // had asked for one. Ask, and a dictionary column gets a bloom.
        use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
        let t = 1_786_179_600_000_000_000i64;
        let lp: String = (0..2000)
            .map(|i| format!("m,pid=p{:05} v=1.0 {}\n", (i * 7919) % 2000, t + i))
            .collect();
        let mut buf = TableBuffer::default();
        for line in parse_lines(&lp, 1, 0).unwrap() {
            buf.append(&line).unwrap();
        }
        let snap = buf.snapshot().unwrap();

        // clustered prepare: pid becomes the leading sort key
        let parts = flush::prepare_ordered(&snap, Some("pid")).unwrap();
        let bytes = flush::to_parquet_bytes_rg(&parts[0].1, Some(256)).unwrap();

        let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
        let md = reader.metadata();
        assert!(md.num_row_groups() > 3, "small row groups requested");
        let descr = md.file_metadata().schema_descr_ptr();
        let pid_idx = (0..descr.num_columns())
            .find(|i| descr.column(*i).name() == "pid")
            .expect("pid column");
        // every row group carries a bloom over the entity column
        for rg in 0..md.num_row_groups() {
            assert!(
                md.row_group(rg)
                    .column(pid_idx)
                    .bloom_filter_offset()
                    .is_some(),
                "row group {rg} has no bloom for pid"
            );
        }
        // and a float measurement does not pay for one it would never use
        let v_idx = (0..descr.num_columns())
            .find(|i| descr.column(*i).name() == "v")
            .expect("v column");
        assert!(
            md.row_group(0)
                .column(v_idx)
                .bloom_filter_offset()
                .is_none()
        );
        // clustering: row-group pid ranges are disjoint & ordered
        use datafusion::parquet::file::statistics::Statistics;
        let mut prev_max: Option<Vec<u8>> = None;
        for rg in 0..md.num_row_groups() {
            if let Some(Statistics::ByteArray(s)) = md.row_group(rg).column(pid_idx).statistics() {
                let (min, max) = (s.min_opt().unwrap(), s.max_opt().unwrap());
                if let Some(prev) = &prev_max {
                    assert!(
                        min.data() >= prev.as_slice(),
                        "row-group entity ranges must be ordered"
                    );
                }
                prev_max = Some(max.data().to_vec());
            }
        }
    }

    #[test]
    fn hour_partition_format() {
        // 2026-08-08T09:00:00Z == 1786179600
        assert_eq!(
            flush::hour_partition(1_786_179_600_000_000_000),
            "2026080809"
        );
        assert_eq!(flush::hour_partition(0), "1970010100");
    }

    #[test]
    fn type_conflicts_name_the_field() {
        let mut buf = TableBuffer::default();
        for line in parse_lines("m x=1i 1", 1, 0).unwrap() {
            buf.append(&line).unwrap();
        }
        let bad = parse_lines("m x=\"oops\" 2", 1, 0).unwrap();
        let err = buf.append(&bad[0]).unwrap_err();
        assert!(err.contains("'x'"));
        // ints promote into float columns
        let mut buf = TableBuffer::default();
        for line in parse_lines("m y=1.5 1\nm y=2i 2", 1, 0).unwrap() {
            buf.append(&line).unwrap();
        }
        assert_eq!(buf.row_count(), 2);
    }
}
