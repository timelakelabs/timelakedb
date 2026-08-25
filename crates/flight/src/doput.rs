//! DoPut ingest: an Arrow `RecordBatch` becomes rows for the engine write
//! path (timelakedb#79).
//!
//! Column roles, so a batch fetched with DoGet writes straight back:
//!
//! - a column named `time` (Timestamp(ns) or Int64 ns) is the row timestamp;
//! - a **string** column (Utf8/Dictionary-of-utf8) is a **tag** by default;
//! - a numeric or boolean column (Float64/Int64/UInt64/Boolean) is a **field**.
//!
//! Flight hydrates dictionary columns to plain Utf8 on the wire, so tag columns
//! arrive (and come back from DoGet) as Utf8 — hence strings default to tags,
//! not fields. A column overrides the default with Arrow field metadata
//! `timelake:role` = `tag` | `field`; that is how a genuine **string field**
//! (a log line) travels, since by value type it is indistinguishable from a
//! tag. A null cell simply omits that tag or field on that row, exactly as a
//! sparse line-protocol write does.
//!
//! The rows are serialized to line protocol and handed to the SAME `write_lp`
//! seam every write uses, so schema union and type-conflict handling (#98), WAL
//! durability, replication, LWW and SEC-2 are inherited — a DoPut of a column
//! whose type disagrees with the table conflicts exactly as a bad line-protocol
//! field does, never silently forking the column.

use arrow::array::{
    Array, BooleanArray, DictionaryArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Int32Type, TimeUnit};
use timelake_ingest::{FieldValue, ParsedLine};

/// The metadata key a producer sets to force a column's role.
const ROLE_KEY: &str = "timelake:role";

enum TimeCol<'a> {
    Ns(&'a TimestampNanosecondArray),
    Int(&'a Int64Array),
}

impl TimeCol<'_> {
    fn is_valid(&self, r: usize) -> bool {
        match self {
            TimeCol::Ns(a) => a.is_valid(r),
            TimeCol::Int(a) => a.is_valid(r),
        }
    }
    fn value(&self, r: usize) -> i64 {
        match self {
            TimeCol::Ns(a) => a.value(r),
            TimeCol::Int(a) => a.value(r),
        }
    }
}

/// A string column read either as plain Utf8 or through a dictionary — the two
/// shapes a tag (or a string field) arrives in.
enum StrCol<'a> {
    Utf8(&'a StringArray),
    Dict(&'a DictionaryArray<Int32Type>, &'a StringArray),
}

impl StrCol<'_> {
    fn value(&self, r: usize) -> Option<&str> {
        match self {
            StrCol::Utf8(a) => a.is_valid(r).then(|| a.value(r)),
            StrCol::Dict(d, v) => d.is_valid(r).then(|| v.value(d.keys().value(r) as usize)),
        }
    }
}

enum FieldCol<'a> {
    F64(&'a Float64Array),
    I64(&'a Int64Array),
    U64(&'a UInt64Array),
    Bool(&'a BooleanArray),
    Str(StrCol<'a>),
}

/// Build a [`StrCol`] over `col`, or `None` if it is not a string column.
fn as_str_col(col: &dyn Array) -> Option<StrCol<'_>> {
    match col.data_type() {
        DataType::Utf8 => col.as_any().downcast_ref::<StringArray>().map(StrCol::Utf8),
        DataType::Dictionary(k, v)
            if matches!(k.as_ref(), DataType::Int32) && matches!(v.as_ref(), DataType::Utf8) =>
        {
            let dict = col.as_any().downcast_ref::<DictionaryArray<Int32Type>>()?;
            let values = dict.values().as_any().downcast_ref::<StringArray>()?;
            Some(StrCol::Dict(dict, values))
        }
        _ => None,
    }
}

/// Convert one DoPut `RecordBatch` into rows destined for `table`.
pub fn batch_to_rows(table: &str, batch: &RecordBatch) -> Result<Vec<ParsedLine>, String> {
    let schema = batch.schema();
    let nrows = batch.num_rows();

    let mut time: Option<TimeCol<'_>> = None;
    let mut tags: Vec<(String, StrCol<'_>)> = Vec::new();
    let mut fields: Vec<(String, FieldCol<'_>)> = Vec::new();

    for (i, f) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        let name = f.name();

        if name == "time" {
            time = Some(match f.data_type() {
                DataType::Timestamp(TimeUnit::Nanosecond, _) => TimeCol::Ns(
                    col.as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or("DoPut `time` column is not the TimestampNanosecond it claims")?,
                ),
                DataType::Int64 => TimeCol::Int(
                    col.as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or("DoPut `time` column is not the Int64 it claims")?,
                ),
                other => {
                    return Err(format!(
                        "DoPut `time` column must be Timestamp(ns) or Int64, not {other:?}"
                    ));
                }
            });
            continue;
        }

        let role = f.metadata().get(ROLE_KEY).map(String::as_str);
        let is_tag = match role {
            Some("tag") => true,
            Some("field") => false,
            Some(other) => {
                return Err(format!(
                    "column {name:?} has unknown {ROLE_KEY} {other:?} (want \"tag\" or \"field\")"
                ));
            }
            // Default: a string is a tag, everything else a field.
            None => matches!(f.data_type(), DataType::Utf8 | DataType::Dictionary(_, _)),
        };

        if is_tag {
            let sc = as_str_col(col.as_ref()).ok_or_else(|| {
                format!("column {name:?} is marked a tag but is not a string column")
            })?;
            tags.push((name.clone(), sc));
            continue;
        }

        let fc = match f.data_type() {
            DataType::Float64 => FieldCol::F64(col.as_any().downcast_ref().ok_or("f64 cast")?),
            DataType::Int64 => FieldCol::I64(col.as_any().downcast_ref().ok_or("i64 cast")?),
            DataType::UInt64 => FieldCol::U64(col.as_any().downcast_ref().ok_or("u64 cast")?),
            DataType::Boolean => FieldCol::Bool(col.as_any().downcast_ref().ok_or("bool cast")?),
            DataType::Utf8 | DataType::Dictionary(_, _) => FieldCol::Str(
                as_str_col(col.as_ref())
                    .ok_or_else(|| format!("string field {name:?} could not be read"))?,
            ),
            other => {
                return Err(format!(
                    "DoPut column {name:?} has type {other:?}, which is not a field \
                     (f64/i64/u64/bool/utf8), a tag (string), or `time`"
                ));
            }
        };
        fields.push((name.clone(), fc));
    }

    let Some(time) = time else {
        return Err("DoPut batch needs a `time` column".into());
    };

    let mut rows = Vec::with_capacity(nrows);
    for r in 0..nrows {
        if !time.is_valid(r) {
            return Err(format!("DoPut row {r} has a null `time`"));
        }
        let mut row_tags = Vec::new();
        for (name, t) in &tags {
            if let Some(v) = t.value(r) {
                row_tags.push((name.clone(), v.to_string()));
            }
        }
        let mut row_fields = Vec::new();
        for (name, fc) in &fields {
            let fv = match fc {
                FieldCol::F64(a) if a.is_valid(r) => {
                    let v = a.value(r);
                    if !v.is_finite() {
                        return Err(format!(
                            "DoPut field {name:?} row {r} is NaN/Inf, which line protocol \
                             cannot represent"
                        ));
                    }
                    Some(FieldValue::Float(v))
                }
                FieldCol::I64(a) if a.is_valid(r) => Some(FieldValue::Int(a.value(r))),
                FieldCol::U64(a) if a.is_valid(r) => Some(FieldValue::UInt(a.value(r))),
                FieldCol::Bool(a) if a.is_valid(r) => Some(FieldValue::Bool(a.value(r))),
                FieldCol::Str(sc) => sc.value(r).map(|s| FieldValue::Str(s.to_string())),
                _ => None,
            };
            if let Some(fv) = fv {
                row_fields.push((name.clone(), fv));
            }
        }
        if row_fields.is_empty() {
            return Err(format!(
                "DoPut row {r} has no non-null field; line protocol needs at least one"
            ));
        }
        rows.push(ParsedLine {
            table: table.to_string(),
            tags: row_tags,
            fields: row_fields,
            timestamp_ns: time.value(r),
        });
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, StringDictionaryBuilder,
        TimestampNanosecondArray,
    };
    use arrow::datatypes::{Field, Int32Type};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn dict(values: &[Option<&str>]) -> ArrayRef {
        let mut b = StringDictionaryBuilder::<Int32Type>::new();
        for v in values {
            match v {
                Some(s) => b.append_value(s),
                None => b.append_null(),
            }
        }
        Arc::new(b.finish())
    }

    #[test]
    fn strings_are_tags_numbers_are_fields_time_is_the_timestamp() {
        // A plain Utf8 column (how a tag arrives after Flight hydrates the
        // dictionary) is a tag; a dictionary column is too.
        let time: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![10_i64, 20]));
        let host: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let region = dict(&[Some("west"), Some("east")]);
        let temp: ArrayRef = Arc::new(Float64Array::from(vec![1.5, 2.5]));
        let ok: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
        let batch = RecordBatch::try_from_iter(vec![
            ("time", time),
            ("host", host),
            ("region", region),
            ("temp", temp),
            ("ok", ok),
        ])
        .unwrap();

        let rows = batch_to_rows("weather", &batch).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].table, "weather");
        assert_eq!(
            rows[0].tags,
            vec![
                ("host".to_string(), "a".to_string()),
                ("region".to_string(), "west".to_string())
            ]
        );
        assert_eq!(
            rows[0].fields,
            vec![
                ("temp".to_string(), FieldValue::Float(1.5)),
                ("ok".to_string(), FieldValue::Bool(true)),
            ]
        );
        assert_eq!(rows[0].timestamp_ns, 10);
    }

    #[test]
    fn a_string_field_needs_the_field_role_metadata() {
        // Without metadata a string is a tag; `timelake:role=field` makes it a
        // string FIELD (a log line), the only way to send one over Flight.
        let time: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![1_i64]));
        let msg: ArrayRef = Arc::new(StringArray::from(vec!["boom"]));
        let field = Field::new("msg", DataType::Utf8, true)
            .with_metadata(HashMap::from([(ROLE_KEY.to_string(), "field".to_string())]));
        let time_field = Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        );
        let batch = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![time_field, field])),
            vec![time, msg],
        )
        .unwrap();
        let rows = batch_to_rows("logs", &batch).unwrap();
        assert!(rows[0].tags.is_empty());
        assert_eq!(
            rows[0].fields,
            vec![("msg".to_string(), FieldValue::Str("boom".to_string()))]
        );
    }

    #[test]
    fn a_null_cell_omits_that_tag_or_field_on_that_row() {
        let time: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![1_i64, 2]));
        let host: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
        let v: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None]));
        let w: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(9)]));
        let batch =
            RecordBatch::try_from_iter(vec![("time", time), ("host", host), ("v", v), ("w", w)])
                .unwrap();

        let rows = batch_to_rows("m", &batch).unwrap();
        assert_eq!(rows[0].tags, vec![("host".to_string(), "a".to_string())]);
        assert_eq!(rows[0].fields, vec![("v".to_string(), FieldValue::Int(1))]);
        assert!(rows[1].tags.is_empty());
        assert_eq!(rows[1].fields, vec![("w".to_string(), FieldValue::Int(9))]);
    }

    #[test]
    fn a_batch_with_no_time_column_is_rejected() {
        let host: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
        let v: ArrayRef = Arc::new(Int64Array::from(vec![1_i64]));
        let batch = RecordBatch::try_from_iter(vec![("host", host), ("v", v)]).unwrap();
        let err = batch_to_rows("m", &batch).unwrap_err();
        assert!(err.contains("time"), "unexpected error: {err}");
    }
}
