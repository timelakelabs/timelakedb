//! Arrow IPC as the intra-cluster wire for live rows (CL-3).
//!
//! A querier holds no buffer of its own, so the rows an ingester has
//! accepted but not yet flushed reach it over the network. Arrow IPC is the
//! natural encoding: it is the batch's own memory layout, so a
//! `TableBuffer` snapshot (PR-9: already an immutable `RecordBatch`) crosses
//! the wire without a row-by-row re-encode, and — the part that matters for
//! FR-2 — **dictionary-encoded tag columns stay dictionary-encoded**.
//! Serialising to JSON or line protocol would explode a
//! `Dictionary<Int32,Utf8>` column back into repeated strings and hand the
//! querier the exact memory shape this project exists to avoid.
//!
//! The stream format (not the file format) is used deliberately: it is
//! append-only and needs no seekable footer, so the response can be written
//! straight into an HTTP body.
//!
//! Schema note: an empty batch list encodes to zero bytes and decodes back
//! to an empty list. That is the common case (a table with nothing live on
//! this ingester) and it must not be an error.

use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;

use crate::QueryBatch;

/// Encode batches as one Arrow IPC stream. All batches must share a schema
/// (they do: they come from one table's snapshot).
pub fn to_ipc(batches: &[QueryBatch]) -> Result<Vec<u8>, String> {
    let Some(first) = batches.first() else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut out, &first.schema())
            .map_err(|e| format!("arrow ipc writer: {e}"))?;
        for b in batches {
            w.write(b).map_err(|e| format!("arrow ipc write: {e}"))?;
        }
        w.finish().map_err(|e| format!("arrow ipc finish: {e}"))?;
    }
    Ok(out)
}

/// Decode an Arrow IPC stream back into batches. Zero bytes = no batches.
pub fn from_ipc(bytes: &[u8]) -> Result<Vec<QueryBatch>, String> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| format!("arrow ipc reader: {e}"))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("arrow ipc read: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, DictionaryArray, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use std::sync::Arc;

    fn sample() -> QueryBatch {
        let tags: DictionaryArray<Int32Type> =
            vec!["host-a", "host-b", "host-a"].into_iter().collect();
        let schema = Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new(
                "host",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ]);
        QueryBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])), Arc::new(tags)],
        )
        .unwrap()
    }

    #[test]
    fn round_trip_preserves_rows_and_schema() {
        let batch = sample();
        let bytes = to_ipc(std::slice::from_ref(&batch)).unwrap();
        let back = from_ipc(&bytes).unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].num_rows(), 3);
        assert_eq!(back[0].schema(), batch.schema());
    }

    #[test]
    fn tag_columns_stay_dictionary_encoded_across_the_wire() {
        // FR-2: if the wire widened dictionaries back to strings, a querier
        // would hold the memory shape this database exists to avoid.
        let batch = sample();
        let back = from_ipc(&to_ipc(std::slice::from_ref(&batch)).unwrap()).unwrap();
        let col = back[0].column(1);
        assert!(
            matches!(col.data_type(), DataType::Dictionary(_, _)),
            "tag column arrived as {:?}, not a dictionary",
            col.data_type()
        );
        let dict = col
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("dictionary array");
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.len(), 2, "two distinct hosts, three rows");
    }

    #[test]
    fn nothing_live_encodes_to_nothing_and_is_not_an_error() {
        let bytes = to_ipc(&[]).unwrap();
        assert!(bytes.is_empty());
        assert!(from_ipc(&bytes).unwrap().is_empty());
    }

    #[test]
    fn several_batches_survive_as_several_batches() {
        let batch = sample();
        let bytes = to_ipc(&[batch.clone(), batch.clone()]).unwrap();
        let back = from_ipc(&bytes).unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back.iter().map(|b| b.num_rows()).sum::<usize>(), 6);
    }

    #[test]
    fn a_truncated_stream_is_an_error_not_a_short_read() {
        // Half a response must never look like "that table has fewer rows".
        let batch = sample();
        let bytes = to_ipc(std::slice::from_ref(&batch)).unwrap();
        let truncated = &bytes[..bytes.len() / 2];
        let decoded = from_ipc(truncated);
        assert!(decoded.is_err(), "truncated stream decoded as {decoded:?}");
    }
}
