//! Self-monitoring — the database stores its own telemetry (phase U2,
//! `docs/CONSOLE.md` §7).
//!
//! Two streams land in the `_system` database:
//!
//! * **`_system.metrics`** — a periodic snapshot of the whole `/metrics`
//!   exposition, one sample per maintenance tick (10 s, the interval §7.2
//!   specifies). Deliberately no knob of its own: this comment used to name
//!   a `TIMELAKE_SELFMON_SECS` that nothing read (#39), and a real one would
//!   break the property that makes the stored numbers trustworthy — one
//!   sample per tick, taken before the tick's other stages run, is what
//!   keeps `_system.metrics` and `/metrics` describing the same moment.
//! * **`_system.queries`** — one row per finished query, from the
//!   [`timelake_query::QueryObserver`] hook. Exact durations rather than
//!   bucket bounds, so a p99 is computed from the real distribution and can
//!   be sliced by database, outcome and client identity.
//!
//! ## Why it converts the exposition instead of listing metrics again
//!
//! The sample is produced by *parsing the Prometheus text the node already
//! renders* and re-emitting it as line protocol. That looks roundabout, and
//! it buys two things worth more than the parse:
//!
//! 1. §13's U2 gate is that the stored numbers agree with `/metrics`. Doing
//!    it this way makes them the same numbers, so they cannot disagree.
//! 2. A metric added later is self-monitored the day it is added. A second
//!    hand-maintained list would drift, and drift silently — the dashboard
//!    would simply never show the new series and nothing would fail.
//!
//! ## What it must never do
//!
//! Monitoring that adds load during an overload makes the outage worse. So
//! the observer never writes, allocates unboundedly, or blocks: it formats
//! one line and pushes it onto a bounded queue, and **drops** when that
//! queue is full. Drops are counted and exposed
//! (`timelake_selfmon_dropped_total`) — silent loss would make the
//! dashboard lie by omission at exactly the busiest moment.
//!
//! ## The querier gap, stated plainly
//!
//! A CL-3 querier owns no data, refuses writes and runs no maintenance, so
//! it cannot store samples locally: a buffer there would grow with nothing
//! to flush it. On a querier the sampler stays off and `/metrics` remains
//! the only surface. Shipping querier samples to an ingester is the real
//! fix and belongs with the C2 role work, not here.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use timelake_query::{QueryObserver, QueryStats};

/// The database self-monitoring writes into. Leading underscore marks it as
/// engine-owned; it is an ordinary database in every other respect, so it
/// gets the same encryption, retention, compaction and query path as user
/// data — which is the point of storing it here at all.
pub const SELFMON_DB: &str = "_system";

/// Default cap on queued query records. At ~120 bytes a line this is a few
/// hundred KiB — bounded, and far more than one maintenance interval's
/// worth of queries on any workload this engine is built for.
const DEFAULT_QUEUE: usize = 4096;

/// Buffers self-monitoring rows between maintenance ticks.
///
/// Cheap to hold: one mutex around a `Vec<String>` that is swapped out
/// wholesale on drain, so the query path's critical section is a push.
pub struct SelfMonitor {
    pending: Mutex<Vec<String>>,
    capacity: usize,
    dropped: AtomicU64,
    written: AtomicU64,
    node: String,
}

impl SelfMonitor {
    pub fn new(node: impl Into<String>) -> SelfMonitor {
        SelfMonitor {
            pending: Mutex::new(Vec::new()),
            capacity: std::env::var("TIMELAKE_SELFMON_QUEUE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_QUEUE),
            dropped: AtomicU64::new(0),
            written: AtomicU64::new(0),
            node: node.into(),
        }
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub fn written(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }

    pub fn pending_len(&self) -> usize {
        self.pending.lock().expect("selfmon lock").len()
    }

    /// Queue one line, dropping it if the buffer is full.
    ///
    /// Dropping is the correct failure here: the alternative is blocking a
    /// query on a monitoring buffer, which would let telemetry take the
    /// server down. The drop is counted so it is visible rather than
    /// silent.
    fn enqueue(&self, line: String) {
        let mut pending = self.pending.lock().expect("selfmon lock");
        if pending.len() >= self.capacity {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        pending.push(line);
    }

    /// Take everything queued, leaving an empty buffer behind.
    pub fn drain(&self) -> Vec<String> {
        std::mem::take(&mut *self.pending.lock().expect("selfmon lock"))
    }

    pub fn note_written(&self, n: u64) {
        self.written.fetch_add(n, Ordering::Relaxed);
    }

    /// Queue a `metrics` sample built from the node's own exposition.
    pub fn sample_exposition(&self, exposition: &str, now_ns: i64) {
        for line in exposition_to_line_protocol(exposition, &self.node, now_ns) {
            self.enqueue(line);
        }
    }

    /// The self-monitoring counters, as Prometheus text.
    ///
    /// Deliberately part of `/metrics`: if the sampler is dropping rows,
    /// the stored history is incomplete, and the place that has to say so
    /// is the surface that still works when the stored history does not.
    pub fn render(&self) -> String {
        format!(
            "# HELP timelake_selfmon_dropped_total Self-monitoring rows dropped, queue full.\n\
             # TYPE timelake_selfmon_dropped_total counter\n\
             timelake_selfmon_dropped_total {}\n\
             # HELP timelake_selfmon_written_total Self-monitoring rows written to _system.\n\
             # TYPE timelake_selfmon_written_total counter\n\
             timelake_selfmon_written_total {}\n\
             # HELP timelake_selfmon_pending Self-monitoring rows awaiting the next tick.\n\
             # TYPE timelake_selfmon_pending gauge\n\
             timelake_selfmon_pending {}\n",
            self.dropped(),
            self.written(),
            self.pending_len(),
        )
    }
}

impl QueryObserver for SelfMonitor {
    fn on_query(&self, stats: &QueryStats) {
        let mut line = String::with_capacity(160);
        line.push_str("queries,node=");
        line.push_str(&escape_tag(&self.node));
        line.push_str(",db=");
        line.push_str(&escape_tag(&stats.db));
        line.push_str(",outcome=");
        line.push_str(stats.outcome.as_str());
        // An anonymous caller gets no tag at all rather than a placeholder:
        // NULL is the truthful answer to "which identity", and a literal
        // like "anonymous" would collide with a client actually named that.
        if let Some(identity) = &stats.identity {
            line.push_str(",identity=");
            line.push_str(&escape_tag(identity));
        }
        // `ref` is a FIELD, not a tag. It is unique per query, so as a tag
        // it would create one series per query — precisely the
        // high-cardinality failure this engine exists to avoid (FR-2).
        line.push_str(&format!(
            " duration_ms={:.3},wait_ms={:.3},rows={}i,ref=\"{}\" {}",
            stats.duration.as_secs_f64() * 1000.0,
            stats.admission_wait.as_secs_f64() * 1000.0,
            stats.rows,
            escape_field_string(&stats.ref_id),
            now_ns_for_stats(),
        ));
        self.enqueue(line);
    }
}

/// Query rows are stamped at observation time; the engine's clock is the
/// same one the write path uses.
fn now_ns_for_stats() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Convert a Prometheus exposition into line-protocol rows.
///
/// Metrics sharing a label set become one row (labels → tags, metric names
/// → fields), which keeps the row count proportional to the number of
/// distinct label sets rather than to the number of series.
pub fn exposition_to_line_protocol(exposition: &str, node: &str, now_ns: i64) -> Vec<String> {
    // BTreeMap on both levels so a sample is byte-identical run to run for
    // the same input — a diff of two samples shows real change only.
    let mut grouped: BTreeMap<BTreeMap<String, String>, BTreeMap<String, String>> = BTreeMap::new();

    for line in exposition.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((series, value)) = line.rsplit_once(' ') else {
            continue;
        };
        // Only FINITE numeric samples.
        //
        // `"NaN".parse::<f64>()` succeeds in Rust, so an `is_err` check
        // lets NaN and inf through — and because fields sharing a label set
        // are grouped onto one line, a single non-finite value makes that
        // whole line fail to parse and takes every other metric on the row
        // down with it. One bad gauge would cost the entire sample.
        match value.parse::<f64>() {
            Ok(v) if v.is_finite() => {}
            _ => continue,
        }
        let (name, labels) = match series.split_once('{') {
            Some((name, rest)) => (name, parse_labels(rest.trim_end_matches('}'))),
            None => (series, BTreeMap::new()),
        };
        if name.is_empty() {
            continue;
        }
        grouped
            .entry(labels)
            .or_default()
            .insert(name.to_string(), value.to_string());
    }

    grouped
        .into_iter()
        .map(|(labels, fields)| {
            let mut line = String::from("metrics,node=");
            line.push_str(&escape_tag(node));
            for (key, value) in &labels {
                line.push(',');
                line.push_str(&escape_tag(key));
                line.push('=');
                line.push_str(&escape_tag(value));
            }
            line.push(' ');
            let mut first = true;
            for (name, value) in &fields {
                if !first {
                    line.push(',');
                }
                first = false;
                line.push_str(&escape_tag(name));
                line.push('=');
                line.push_str(value);
            }
            line.push(' ');
            line.push_str(&now_ns.to_string());
            line
        })
        .collect()
}

/// Parse `a="b",c="d"` from an exposition series.
fn parse_labels(input: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let Some(eq) = input[i..].find('=') else {
            break;
        };
        let key = input[i..i + eq]
            .trim_matches(|c| c == ',' || c == ' ')
            .to_string();
        i += eq + 1;
        if bytes.get(i) != Some(&b'"') {
            break;
        }
        i += 1;
        // Walk to the closing quote, honouring backslash escapes — the
        // exposition escapes quotes inside label values, and a naive
        // `find('"')` would cut a value like `ev\"il` in half.
        let mut value = String::new();
        while i < bytes.len() {
            match bytes[i] {
                b'\\' if i + 1 < bytes.len() => {
                    value.push(match bytes[i + 1] {
                        b'n' => '\n',
                        other => other as char,
                    });
                    i += 2;
                }
                b'"' => {
                    i += 1;
                    break;
                }
                _ => {
                    // Multi-byte UTF-8 has to survive intact.
                    let ch = input[i..].chars().next().unwrap_or('?');
                    value.push(ch);
                    i += ch.len_utf8();
                }
            }
        }
        if !key.is_empty() {
            out.insert(key, value);
        }
    }
    out
}

/// Escape a measurement name, tag key, tag value or field key.
///
/// Comma, space and equals are the separators of the format, so a value
/// carrying one would otherwise be read as structure. Cert CNs routinely
/// contain spaces ("CN=Tributary Agent 4"), and a table name is whatever a
/// client wrote — this is untrusted text, and an unescaped separator here
/// is a line-protocol injection, not a cosmetic bug.
pub fn escape_tag(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            ',' => out.push_str("\\,"),
            ' ' => out.push_str("\\ "),
            '=' => out.push_str("\\="),
            '\\' => out.push_str("\\\\"),
            // A newline would end the line and turn the remainder into a
            // second, attacker-shaped record.
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

/// Escape the inside of a quoted string field value.
pub fn escape_field_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use timelake_query::QueryOutcome;

    fn stats(outcome: QueryOutcome, identity: Option<&str>) -> QueryStats {
        QueryStats {
            ref_id: "q-0000002a".into(),
            db: "poc".into(),
            identity: identity.map(|s| s.to_string()),
            admission_wait: Duration::from_micros(1500),
            duration: Duration::from_millis(42),
            rows: 7,
            outcome,
        }
    }

    #[test]
    fn a_query_becomes_one_line_with_ref_as_a_field() {
        let mon = SelfMonitor::new("tldb");
        mon.on_query(&stats(QueryOutcome::Ok, Some("tributary-l4")));
        let lines = mon.drain();
        assert_eq!(lines.len(), 1);
        let line = &lines[0];

        assert!(
            line.starts_with("queries,node=tldb,db=poc,outcome=ok"),
            "{line}"
        );
        assert!(line.contains("identity=tributary-l4"), "{line}");
        assert!(line.contains("duration_ms=42.000"), "{line}");
        assert!(line.contains("wait_ms=1.500"), "{line}");
        assert!(line.contains("rows=7i"), "{line}");
        // The ref must be a field. As a tag it would be one series per
        // query — the cardinality explosion this engine exists to avoid.
        assert!(line.contains("ref=\"q-0000002a\""), "{line}");
        assert!(!line.contains(",ref=q-"), "ref must not be a tag: {line}");
    }

    #[test]
    fn an_anonymous_query_carries_no_identity_tag() {
        let mon = SelfMonitor::new("tldb");
        mon.on_query(&stats(QueryOutcome::Ok, None));
        let line = mon.drain().pop().unwrap();
        assert!(
            !line.contains("identity="),
            "absent identity must be absent, not a placeholder: {line}"
        );
    }

    #[test]
    fn an_identity_with_spaces_and_commas_cannot_break_the_line() {
        // A cert CN is untrusted text and routinely contains spaces. An
        // unescaped one would split the tag set and let the remainder be
        // parsed as further tags or fields.
        let mon = SelfMonitor::new("tldb");
        mon.on_query(&stats(QueryOutcome::Ok, Some("Tributary Agent,4=x")));
        let line = mon.drain().pop().unwrap();
        assert!(
            line.contains("identity=Tributary\\ Agent\\,4\\=x"),
            "{line}"
        );
        // Exactly one unescaped space separates tags from fields.
        let unescaped_spaces = line
            .char_indices()
            .filter(|(i, c)| *c == ' ' && (*i == 0 || line.as_bytes()[i - 1] != b'\\'))
            .count();
        assert_eq!(
            unescaped_spaces, 2,
            "tags|fields|timestamp needs exactly two separators: {line}"
        );
    }

    #[test]
    fn the_queue_is_bounded_and_drops_are_counted() {
        // Monitoring must never become the backpressure it is measuring.
        let mon = SelfMonitor {
            pending: Mutex::new(Vec::new()),
            capacity: 2,
            dropped: AtomicU64::new(0),
            written: AtomicU64::new(0),
            node: "tldb".into(),
        };
        for _ in 0..5 {
            mon.on_query(&stats(QueryOutcome::Ok, None));
        }
        assert_eq!(mon.pending_len(), 2);
        assert_eq!(mon.dropped(), 3, "drops must be counted, never silent");
        assert_eq!(mon.drain().len(), 2);
        assert_eq!(mon.pending_len(), 0, "drain empties the buffer");
    }

    #[test]
    fn exposition_converts_unlabelled_metrics_into_one_row() {
        let text = "# HELP a thing\n\
                    # TYPE a gauge\n\
                    timelake_buffer_rows 12\n\
                    timelake_wal_bytes 3400\n";
        let rows = exposition_to_line_protocol(text, "tldb", 1700000000000000000);
        assert_eq!(rows.len(), 1, "same label set = same row: {rows:?}");
        let row = &rows[0];
        assert!(row.starts_with("metrics,node=tldb "), "{row}");
        assert!(row.contains("timelake_buffer_rows=12"), "{row}");
        assert!(row.contains("timelake_wal_bytes=3400"), "{row}");
        assert!(row.ends_with(" 1700000000000000000"), "{row}");
    }

    #[test]
    fn labels_become_tags_and_split_rows() {
        let text = "timelake_storage_bytes{db=\"poc\",table=\"cpu\"} 100\n\
                    timelake_storage_rows{db=\"poc\",table=\"cpu\"} 5\n\
                    timelake_storage_bytes{db=\"poc\",table=\"mem\"} 200\n";
        let rows = exposition_to_line_protocol(text, "tldb", 1);
        assert_eq!(rows.len(), 2, "one row per distinct label set: {rows:?}");

        let cpu = rows.iter().find(|r| r.contains("table=cpu")).unwrap();
        // Both cpu metrics share a label set, so they share a row.
        assert!(cpu.contains("timelake_storage_bytes=100"), "{cpu}");
        assert!(cpu.contains("timelake_storage_rows=5"), "{cpu}");
        assert!(cpu.contains("db=poc"), "{cpu}");

        let mem = rows.iter().find(|r| r.contains("table=mem")).unwrap();
        assert!(mem.contains("timelake_storage_bytes=200"), "{mem}");
        assert!(!mem.contains("timelake_storage_rows"), "{mem}");
    }

    #[test]
    fn histogram_buckets_survive_as_le_tagged_rows() {
        let text = "timelake_query_duration_seconds_bucket{le=\"0.25\"} 4\n\
                    timelake_query_duration_seconds_bucket{le=\"+Inf\"} 9\n\
                    timelake_query_duration_seconds_count 9\n";
        let rows = exposition_to_line_protocol(text, "tldb", 1);
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().any(
            |r| r.contains("le=0.25") && r.contains("timelake_query_duration_seconds_bucket=4")
        ));
        assert!(rows.iter().any(|r| r.contains("le=+Inf")));
    }

    #[test]
    fn an_escaped_quote_in_a_label_round_trips() {
        // The exposition escapes a quote inside a label value; a naive
        // parser would cut the value in half and mis-tag the row.
        let text = "timelake_storage_bytes{table=\"ev\\\"il\"} 42\n";
        let rows = exposition_to_line_protocol(text, "tldb", 1);
        assert_eq!(rows.len(), 1);
        assert!(
            rows[0].contains("timelake_storage_bytes=42"),
            "value lost: {}",
            rows[0]
        );
        assert!(
            rows[0].contains("ev\"il"),
            "label value mangled: {}",
            rows[0]
        );
    }

    #[test]
    fn non_finite_and_comment_lines_are_skipped() {
        // NaN and inf both PARSE as f64, so a naive `is_err` filter admits
        // them. They are not valid line-protocol floats, and since fields
        // sharing a label set are grouped onto one line, admitting one
        // would lose every other metric on that row too.
        let text = "# HELP x y\n\
                    # TYPE x gauge\n\
                    \n\
                    timelake_nan NaN\n\
                    timelake_inf inf\n\
                    timelake_words hello\n\
                    timelake_good 5\n";
        let rows = exposition_to_line_protocol(text, "tldb", 1);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].contains("timelake_good=5"), "{}", rows[0]);
        assert!(!rows[0].contains("timelake_nan"), "{}", rows[0]);
        assert!(!rows[0].contains("timelake_inf"), "{}", rows[0]);
        assert!(!rows[0].contains("timelake_words"), "{}", rows[0]);
    }

    #[test]
    fn a_sample_is_byte_identical_for_identical_input() {
        // Ordering comes from BTreeMaps, not HashMap iteration order, so a
        // diff of two samples shows real change rather than reshuffling.
        let text = "b_metric{z=\"1\",a=\"2\"} 1\na_metric 2\n";
        let first = exposition_to_line_protocol(text, "tldb", 7);
        let second = exposition_to_line_protocol(text, "tldb", 7);
        assert_eq!(first, second);
    }
}
