//! Prometheus `remote_write` decode (R-3, timelakedb#56).
//!
//! `remote_write` is snappy-compressed protobuf, NOT the gzip+text line
//! protocol the HTTP write path speaks — so `maybe_gunzip`/`parse_lines`
//! (crates/ingest) do not apply. This crate is that separate decode: snappy
//! block-decompress, `prost`-decode a `WriteRequest`, and build [`ParsedLine`]s
//! that the shared engine write path ingests exactly as a line-protocol write —
//! WAL fsync, replication, LWW dedup, SEC-2 all inherited below that seam.
//!
//! The four protobuf messages are hand-written (a stable, tiny `.proto`, so no
//! `protoc`/build-step); OTLP metrics is the same shape one message later.
//!
//! **The mapping is the whole point, and it is deliberately NOT VictoriaMetrics'.**
//! One Prometheus series — `(__name__, labels)` with a single float sample — maps
//! to ONE row: `__name__` is the measurement, every other label is a tag, the
//! sample value is a `value` field. It never fans one series out into a table per
//! field (freshet#4 caught VM doubling series that way). A tag is a compressed
//! dictionary column here (FR-2), so the whole label set on one row is what buys
//! TimeLakeDB's economics.

use timelake_ingest::{FieldValue, ParsedLine};

/// Prometheus `prompb.WriteRequest` — the top-level remote_write frame.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
    // field 2 is `metadata`, ignored on ingest.
}

/// One `(labels, samples)` series.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}

/// A single label — `__name__` is the metric name, the rest are dimensions.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

/// A value at a millisecond timestamp.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    /// Milliseconds since the Unix epoch (Prometheus' native resolution).
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

/// Why a `remote_write` frame could not be turned into rows. Every variant is
/// a client error (a bad or unsupported frame): the caller maps it to 400,
/// exactly as line protocol maps a parse error, and — like a poison line — the
/// whole request is rejected, nothing half-written.
#[derive(Debug, PartialEq)]
pub enum RemoteWriteError {
    /// The body did not snappy-decompress.
    Snappy(String),
    /// The decompressed bytes were not a valid `WriteRequest`.
    Protobuf(String),
    /// A series carried no `__name__` label, so it has no measurement to land in.
    MissingName,
}

impl std::fmt::Display for RemoteWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RemoteWriteError::Snappy(e) => write!(f, "remote_write body is not snappy: {e}"),
            RemoteWriteError::Protobuf(e) => write!(f, "remote_write protobuf decode failed: {e}"),
            RemoteWriteError::MissingName => {
                write!(f, "remote_write series has no __name__ label")
            }
        }
    }
}

impl std::error::Error for RemoteWriteError {}

/// The metric name label Prometheus reserves for the measurement.
const NAME_LABEL: &str = "__name__";
/// The single field every sample lands in.
const VALUE_FIELD: &str = "value";

/// Decode a snappy+protobuf `remote_write` body into rows for the engine.
///
/// `up{job="node",instance="h1"} 1 <ms>` becomes
/// `up,job=node,instance=h1 value=1 <ms*1e6>`.
pub fn decode_remote_write(body: &[u8]) -> Result<Vec<ParsedLine>, RemoteWriteError> {
    let raw = snap::raw::Decoder::new()
        .decompress_vec(body)
        .map_err(|e| RemoteWriteError::Snappy(e.to_string()))?;
    let req = <WriteRequest as ::prost::Message>::decode(raw.as_slice())
        .map_err(|e| RemoteWriteError::Protobuf(e.to_string()))?;
    map_write_request(req)
}

/// Map an already-decoded `WriteRequest` to rows. Split out from the wire step
/// so the mapping — the part that actually matters — is tested without a codec
/// in the way.
pub fn map_write_request(req: WriteRequest) -> Result<Vec<ParsedLine>, RemoteWriteError> {
    // One sample = one row, so pre-size to the sample count, not the series
    // count: a series carries many samples in a single frame.
    let mut lines = Vec::with_capacity(req.timeseries.iter().map(|t| t.samples.len()).sum());
    for ts in req.timeseries {
        let mut measurement: Option<String> = None;
        let mut tags: Vec<(String, String)> = Vec::with_capacity(ts.labels.len());
        for l in ts.labels {
            if l.name == NAME_LABEL {
                measurement = Some(l.value);
            } else {
                tags.push((l.name, l.value));
            }
        }
        let Some(measurement) = measurement else {
            return Err(RemoteWriteError::MissingName);
        };
        for s in ts.samples {
            // Prometheus marks a series stale by sending NaN (and can send
            // ±Inf); neither is a data point and neither is representable in
            // line protocol. A remote_write receiver drops them rather than
            // 400-ing the whole batch every scrape a target disappears.
            if !s.value.is_finite() {
                continue;
            }
            lines.push(ParsedLine {
                table: measurement.clone(),
                tags: tags.clone(),
                fields: vec![(VALUE_FIELD.to_string(), FieldValue::Float(s.value))],
                // ms -> ns; saturating so a hostile timestamp can't wrap.
                timestamp_ns: s.timestamp.saturating_mul(1_000_000),
            });
        }
    }
    Ok(lines)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lbl(n: &str, v: &str) -> Label {
        Label {
            name: n.to_string(),
            value: v.to_string(),
        }
    }

    #[test]
    fn a_series_maps_to_one_row_name_measurement_labels_tags_value_field() {
        // up{job="node",instance="h1"} 1 @ 1700000000000 ms
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    lbl("__name__", "up"),
                    lbl("job", "node"),
                    lbl("instance", "h1"),
                ],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp: 1_700_000_000_000,
                }],
            }],
        };
        let rows = map_write_request(req).unwrap();
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.table, "up");
        assert_eq!(
            r.tags,
            vec![
                ("job".to_string(), "node".to_string()),
                ("instance".to_string(), "h1".to_string())
            ]
        );
        assert_eq!(
            r.fields,
            vec![("value".to_string(), FieldValue::Float(1.0))]
        );
        assert_eq!(r.timestamp_ns, 1_700_000_000_000 * 1_000_000);
    }

    #[test]
    fn the_whole_label_set_is_one_row_not_a_table_per_field() {
        // The freshet#4 trap: two dimensions must not become two tables. One
        // series -> one measurement, both labels as tags on the same row.
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    lbl("__name__", "sensor_reading"),
                    lbl("device_id", "d1"),
                    lbl("room", "kitchen"),
                ],
                samples: vec![Sample {
                    value: 21.5,
                    timestamp: 1_000,
                }],
            }],
        };
        let rows = map_write_request(req).unwrap();
        assert_eq!(
            rows.len(),
            1,
            "one series is one row, never one row per label"
        );
        assert_eq!(rows[0].table, "sensor_reading");
        assert_eq!(rows[0].tags.len(), 2);
        assert_eq!(rows[0].fields.len(), 1);
    }

    #[test]
    fn many_samples_in_one_series_become_many_rows() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![lbl("__name__", "reqs")],
                samples: vec![
                    Sample {
                        value: 1.0,
                        timestamp: 1,
                    },
                    Sample {
                        value: 2.0,
                        timestamp: 2,
                    },
                    Sample {
                        value: 3.0,
                        timestamp: 3,
                    },
                ],
            }],
        };
        let rows = map_write_request(req).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[2].timestamp_ns, 3 * 1_000_000);
        assert!(rows.iter().all(|r| r.table == "reqs" && r.tags.is_empty()));
    }

    #[test]
    fn a_series_without_a_name_is_rejected_whole() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![lbl("job", "node")],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp: 1,
                }],
            }],
        };
        assert_eq!(map_write_request(req), Err(RemoteWriteError::MissingName));
    }

    #[test]
    fn stale_and_infinite_samples_are_dropped_not_stored() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![lbl("__name__", "up")],
                samples: vec![
                    Sample {
                        value: f64::NAN,
                        timestamp: 1,
                    },
                    Sample {
                        value: 1.0,
                        timestamp: 2,
                    },
                    Sample {
                        value: f64::INFINITY,
                        timestamp: 3,
                    },
                ],
            }],
        };
        let rows = map_write_request(req).unwrap();
        assert_eq!(rows.len(), 1, "only the finite sample survives");
        assert_eq!(rows[0].timestamp_ns, 2 * 1_000_000);
    }

    #[test]
    fn full_wire_roundtrip_snappy_then_protobuf() {
        // Prove the codec path, not just the mapping: encode as Prometheus
        // would (protobuf -> snappy block) and decode it back to rows.
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![lbl("__name__", "temp"), lbl("host", "a")],
                samples: vec![Sample {
                    value: 42.0,
                    timestamp: 5,
                }],
            }],
        };
        let proto = ::prost::Message::encode_to_vec(&req);
        let compressed = snap::raw::Encoder::new().compress_vec(&proto).unwrap();

        let rows = decode_remote_write(&compressed).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].table, "temp");
        assert_eq!(rows[0].tags, vec![("host".to_string(), "a".to_string())]);
        assert_eq!(
            rows[0].fields,
            vec![("value".to_string(), FieldValue::Float(42.0))]
        );
        assert_eq!(rows[0].timestamp_ns, 5 * 1_000_000);
    }

    #[test]
    fn a_body_that_is_not_snappy_is_a_client_error() {
        let err = decode_remote_write(b"not snappy at all").unwrap_err();
        assert!(matches!(err, RemoteWriteError::Snappy(_)));
    }
}
