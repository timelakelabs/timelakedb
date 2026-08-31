//! The `last_cache('table')` table function (#57 phase 2).
//!
//! It answers "current value per entity" from the in-memory last-value cache
//! (`timelake-lastvalue`) instead of planning and scanning files: a snapshot of
//! the cached latest `(time, tags, fields)` per series, handed back as a
//! `MemTable`. Because it is a table in memory, a scan of it touches NO data
//! files — the pruning counters stay flat, which is exactly what proves the
//! cache did the work rather than a warm file cache (#150).
//!
//! It only ever returns cached rows, so there is no wrong-answer fallback: a
//! windowed aggregate or a historical point queries the ordinary table and
//! scans, as it always did. A table that was never enabled (or whose series
//! aged out of the cap) simply yields fewer rows — the "hot series, not all
//! series" promise, visible rather than hidden.

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
    UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::common::{Result as DfResult, plan_err};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::Expr;
use datafusion::scalar::ScalarValue;

use timelake_ingest::FieldValue;
use timelake_lastvalue::{LastValue, LastValueCache};

/// The `last_cache(<table>)` function bound to one database's cache. Registered
/// per query (so it always reads the current snapshot), scoped to the session's
/// database.
pub struct LastCacheFunc {
    cache: Arc<LastValueCache>,
    db: String,
}

impl std::fmt::Debug for LastCacheFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LastCacheFunc(db={:?})", self.db)
    }
}

impl LastCacheFunc {
    pub fn new(cache: Arc<LastValueCache>, db: String) -> LastCacheFunc {
        LastCacheFunc { cache, db }
    }
}

impl TableFunctionImpl for LastCacheFunc {
    fn call(&self, args: &[Expr]) -> DfResult<Arc<dyn TableProvider>> {
        let table = match args {
            [Expr::Literal(ScalarValue::Utf8(Some(s)), _)] => s.clone(),
            _ => {
                return plan_err!(
                    "last_cache(table) takes one string argument — the table name, e.g. last_cache('cpu')"
                );
            }
        };
        let (schema, batch) = build_batch(self.cache.snapshot(&self.db, &table))?;
        let mem = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Arc::new(mem))
    }
}

fn field_datatype(v: &FieldValue) -> DataType {
    match v {
        FieldValue::Float(_) => DataType::Float64,
        FieldValue::Int(_) => DataType::Int64,
        FieldValue::UInt(_) => DataType::UInt64,
        FieldValue::Bool(_) => DataType::Boolean,
        FieldValue::Str(_) => DataType::Utf8,
    }
}

/// Turn a cache snapshot into one record batch. Columns are `time`, then every
/// tag key seen (Utf8, null where a series lacks it), then every field key seen
/// (typed from its first occurrence). An empty snapshot is a valid zero-row
/// batch with just `time`.
fn build_batch(rows: Vec<LastValue>) -> DfResult<(SchemaRef, RecordBatch)> {
    use std::collections::{BTreeMap, BTreeSet};

    let mut tag_keys: BTreeSet<String> = BTreeSet::new();
    let mut field_types: BTreeMap<String, DataType> = BTreeMap::new();
    for r in &rows {
        for (k, _) in &r.tags {
            tag_keys.insert(k.clone());
        }
        for (k, v) in &r.fields {
            field_types
                .entry(k.clone())
                .or_insert_with(|| field_datatype(v));
        }
    }
    let tag_keys: Vec<String> = tag_keys.into_iter().collect();
    let field_keys: Vec<String> = field_types.keys().cloned().collect();

    let mut fields = vec![Field::new(
        "time",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    )];
    for tk in &tag_keys {
        fields.push(Field::new(tk, DataType::Utf8, true));
    }
    for fk in &field_keys {
        fields.push(Field::new(fk, field_types[fk].clone(), true));
    }
    let schema = Arc::new(Schema::new(fields));

    let mut cols: Vec<ArrayRef> = Vec::with_capacity(1 + tag_keys.len() + field_keys.len());
    cols.push(Arc::new(TimestampNanosecondArray::from(
        rows.iter().map(|r| r.timestamp_ns).collect::<Vec<_>>(),
    )));
    for tk in &tag_keys {
        let vals: Vec<Option<String>> = rows
            .iter()
            .map(|r| r.tags.iter().find(|(k, _)| k == tk).map(|(_, v)| v.clone()))
            .collect();
        cols.push(Arc::new(StringArray::from(vals)));
    }
    for fk in &field_keys {
        cols.push(field_column(&rows, fk, &field_types[fk]));
    }

    let batch = RecordBatch::try_new(schema.clone(), cols)?;
    Ok((schema, batch))
}

/// One typed field column. A value of a different type than the column adopted
/// (which a fixed-schema table never produces) is a null rather than a panic.
fn field_column(rows: &[LastValue], name: &str, dt: &DataType) -> ArrayRef {
    macro_rules! col {
        ($variant:path, $arr:ty) => {{
            let vals: Vec<Option<_>> = rows
                .iter()
                .map(|r| {
                    r.fields
                        .iter()
                        .find(|(k, _)| k == name)
                        .and_then(|(_, v)| match v {
                            $variant(x) => Some(x.clone()),
                            _ => None,
                        })
                })
                .collect();
            Arc::new(<$arr>::from(vals)) as ArrayRef
        }};
    }
    match dt {
        DataType::Float64 => col!(FieldValue::Float, Float64Array),
        DataType::Int64 => col!(FieldValue::Int, Int64Array),
        DataType::UInt64 => col!(FieldValue::UInt, UInt64Array),
        DataType::Boolean => col!(FieldValue::Bool, BooleanArray),
        // Utf8 and anything else that reached here: strings.
        _ => col!(FieldValue::Str, StringArray),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_snapshot_is_a_valid_zero_row_batch() {
        let (schema, batch) = build_batch(vec![]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(schema.fields().len(), 1, "just time");
    }

    #[test]
    fn a_snapshot_becomes_time_tag_and_field_columns() {
        let rows = vec![
            LastValue {
                tags: vec![("host".into(), "a".into())],
                timestamp_ns: 20,
                fields: vec![("usage".into(), FieldValue::Float(0.7))],
            },
            LastValue {
                tags: vec![("host".into(), "b".into())],
                timestamp_ns: 10,
                fields: vec![("usage".into(), FieldValue::Float(0.5))],
            },
        ];
        let (schema, batch) = build_batch(rows).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["time", "host", "usage"]);
        let usage = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(usage.value(0), 0.7);
        assert_eq!(usage.value(1), 0.5);
    }
}
