//! U1 (§6): a bounded in-memory ring of the node's application log, so the
//! console can show recent logs. It is fed by a tracing `Layer` that runs
//! beside the fmt layer — stdout and the rotating file are unaffected — and is
//! read back through the Engine for `GET /admin/logs`.
//!
//! Deliberately NOT ingested into TimeLakeDB (§14): self-ingesting logs
//! amplifies writes exactly when the node is unhealthy, which is when the log
//! matters most. The ring is bounded (RR-4) and counts what it drops, so a
//! busy node cannot grow it without limit and a tailing console cannot pretend
//! it saw everything.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use serde::Serialize;
use tracing::field::{Field, Visit};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

/// Ring capacity: enough to triage a recent incident, bounded so a busy node
/// cannot grow it without limit.
const CAPACITY: usize = 2000;

#[derive(Clone, Serialize)]
pub struct LogEntry {
    pub ts: String,
    pub level: String,
    pub target: String,
    pub message: String,
}

pub struct AppLog {
    ring: Mutex<VecDeque<LogEntry>>,
    dropped: AtomicU64,
    cap: usize,
}

impl AppLog {
    fn new(cap: usize) -> Self {
        AppLog {
            ring: Mutex::new(VecDeque::with_capacity(cap)),
            dropped: AtomicU64::new(0),
            cap,
        }
    }

    fn push(&self, e: LogEntry) {
        let mut ring = self.ring.lock().expect("applog lock");
        if ring.len() >= self.cap {
            ring.pop_front();
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        ring.push_back(e);
    }

    /// The ring (oldest first) and how many entries have been dropped to
    /// overflow since start.
    pub fn snapshot(&self) -> (Vec<LogEntry>, u64) {
        let ring = self.ring.lock().expect("applog lock");
        (
            ring.iter().cloned().collect(),
            self.dropped.load(Ordering::Relaxed),
        )
    }
}

static GLOBAL: OnceLock<Arc<AppLog>> = OnceLock::new();

/// The process-global application-log ring (the subscriber is process-global,
/// so this is too), created on first use.
pub fn global() -> Arc<AppLog> {
    GLOBAL
        .get_or_init(|| Arc::new(AppLog::new(CAPACITY)))
        .clone()
}

/// The tracing layer that feeds the ring. Add it to the subscriber registry
/// beside the fmt layer.
pub fn layer() -> AppLogLayer {
    AppLogLayer { log: global() }
}

pub struct AppLogLayer {
    log: Arc<AppLog>,
}

impl<S: tracing::Subscriber> Layer<S> for AppLogLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        let mut v = MessageVisitor::default();
        event.record(&mut v);
        let mut message = v.message;
        message.push_str(&v.fields);
        self.log.push(LogEntry {
            ts: timelake_audit::rfc3339_utc(std::time::SystemTime::now()),
            level: meta.level().to_string(),
            target: meta.target().to_string(),
            message,
        });
    }
}

/// Pulls the `message` field out as the line and appends the rest as
/// `key=value`, so a structured event reads like its log line.
#[derive(Default)]
struct MessageVisitor {
    message: String,
    fields: String,
}

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.fields.push_str(&format!(" {}={value}", field.name()));
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        } else {
            self.fields
                .push_str(&format!(" {}={value:?}", field.name()));
        }
    }
}

/// Severity rank, lower = more severe, so a `level >= WARN` filter keeps
/// entries whose rank is `<=` WARN's.
pub fn level_rank(level: &str) -> u8 {
    match level.to_ascii_uppercase().as_str() {
        "ERROR" => 0,
        "WARN" => 1,
        "INFO" => 2,
        "DEBUG" => 3,
        _ => 4, // TRACE and anything unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ring_is_bounded_and_counts_drops() {
        let log = AppLog::new(3);
        for i in 0..5 {
            log.push(LogEntry {
                ts: "t".into(),
                level: "INFO".into(),
                target: "x".into(),
                message: format!("m{i}"),
            });
        }
        let (entries, dropped) = log.snapshot();
        assert_eq!(dropped, 2, "two oldest dropped");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries.first().unwrap().message, "m2"); // oldest kept
        assert_eq!(entries.last().unwrap().message, "m4"); // newest
    }

    #[test]
    fn level_rank_orders_by_severity() {
        assert!(level_rank("ERROR") < level_rank("WARN"));
        assert!(level_rank("WARN") < level_rank("INFO"));
        assert!(level_rank("INFO") < level_rank("DEBUG"));
        assert_eq!(level_rank("info"), level_rank("INFO"));
    }
}
