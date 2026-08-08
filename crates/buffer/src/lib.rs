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

use timelord_ingest::{FieldValue, ParsedLine};

const TZ: &str = "+00:00";

#[derive(Default)]
struct TagCol {
    intern: HashMap<String, i32>,
    values: Vec<String>,
    keys: Vec<Option<i32>>, // one entry per row; None = tag absent
}

impl TagCol {
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
            (_, other) => {
                return Err(format!(
                    "field '{col}' type conflict: column was created with a \
                     different type than {other:?}"
                ));
            }
        }
        Ok(())
    }
}

/// One table's mutable buffer.
#[derive(Default)]
pub struct TableBuffer {
    times: Vec<i64>,
    tag_names: Vec<String>,
    tags: HashMap<String, TagCol>,
    field_names: Vec<String>,
    fields: HashMap<String, FieldCol>,
}

impl TableBuffer {
    pub fn row_count(&self) -> usize {
        self.times.len()
    }

    /// Append one parsed line. On error the row is NOT applied (the whole
    /// request was already validated upstream; errors here are type
    /// conflicts, reported with the field name).
    pub fn append(&mut self, line: &ParsedLine) -> Result<(), String> {
        let n = self.times.len();

        for (k, v) in &line.tags {
            let col = self.tags.entry(k.clone()).or_insert_with(|| {
                self.tag_names.push(k.clone());
                let mut c = TagCol::default();
                c.keys.resize(n, None); // backfill rows written before this tag existed
                c
            });
            col.push(v);
        }
        for (k, v) in &line.fields {
            let col = self.fields.entry(k.clone()).or_insert_with(|| {
                self.field_names.push(k.clone());
                let mut c = FieldCol::new(v);
                c.pad_to(n);
                c
            });
            col.push(v, k)?;
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
            let dict = DictionaryArray::<Int32Type>::try_new(keys, values)
                .map_err(|e| e.to_string())?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;
    use timelord_ingest::parse_lines;

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
