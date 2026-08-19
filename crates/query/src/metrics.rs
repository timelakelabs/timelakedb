//! Query instrumentation — the numbers that make "is it fast?" answerable
//! without running the harness (`docs/CONSOLE.md` §7.4, phase U2).
//!
//! Until this module existed the exposition was entirely counters and
//! gauges: how many lines arrived, how many files exist, how many
//! compactions ran. Nothing recorded how *long* anything took, so the
//! Query view of §7.3 — the one PR-3 and PR-6 are argued over — could not
//! be drawn at all, and "Shape A got slow" was a question you answered by
//! running Gauge.
//!
//! Two shapes, deliberately, because they answer different questions:
//!
//! * a **histogram** per measure, aggregated in memory and rendered into
//!   `/metrics`. Cheap, fixed-size, and available when the engine is too
//!   sick to serve SQL — which is exactly when a dashboard that reads the
//!   database through the database has stopped working.
//! * a **[`QueryStats`] record per query**, handed to a [`QueryObserver`].
//!   The self-monitoring sampler turns those into rows, so percentiles are
//!   computed from the real distribution rather than estimated from bucket
//!   bounds, and they can be sliced by database and outcome.
//!
//! Neither allocates on the query path (the record is built once per
//! query, off the hot loop) and neither takes a lock: every counter is an
//! atomic, so instrumentation cannot become the contention it measures.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Bucket upper bounds in seconds, chosen against the measured workload
/// rather than a library default: PR-3 puts Shape A at a 250 ms target,
/// M4 measured a 608 ms p95, and RR-2's server-side cap is tens of
/// seconds. Buckets are dense either side of 250 ms — where the argument
/// actually is — and coarse in the tail, where all anyone needs to know
/// is "this one was terrible".
pub const DURATION_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Admission wait is a different distribution: it is zero on an idle
/// server and only interesting once queueing starts, so the low end is
/// coarser and the bounds reach further.
pub const WAIT_BUCKETS: &[f64] = &[0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 15.0, 60.0];

/// A fixed-bucket cumulative histogram over atomics.
///
/// The sum is kept in **microseconds as an integer** rather than seconds
/// as a float: there is no `AtomicF64` in std, and durations are exactly
/// what this measures, so micros lose nothing and stay addable without a
/// compare-and-swap loop.
#[derive(Debug)]
pub struct Histogram {
    bounds: &'static [f64],
    /// `bounds.len() + 1` counts — the extra one is `+Inf`.
    buckets: Vec<AtomicU64>,
    sum_micros: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    pub fn new(bounds: &'static [f64]) -> Histogram {
        Histogram {
            bounds,
            buckets: (0..bounds.len() + 1).map(|_| AtomicU64::new(0)).collect(),
            sum_micros: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Record one observation. Values above the last bound land in `+Inf`,
    /// so nothing is ever dropped or clamped — a 10-minute query still
    /// shows up in the count and the sum, just without a bucket of its own.
    pub fn observe(&self, seconds: f64) {
        let idx = self
            .bounds
            .iter()
            .position(|b| seconds <= *b)
            .unwrap_or(self.bounds.len());
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.sum_micros
            .fetch_add((seconds * 1e6) as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_duration(&self, d: Duration) {
        self.observe(d.as_secs_f64());
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    pub fn sum_seconds(&self) -> f64 {
        self.sum_micros.load(Ordering::Relaxed) as f64 / 1e6
    }

    /// Estimate a quantile by linear interpolation inside the bucket it
    /// falls in — the same method `histogram_quantile` uses, with the same
    /// caveat: resolution is the bucket width, so this is a triage number,
    /// not a measurement. The exact value comes from the per-query rows.
    ///
    /// Returns `None` when nothing has been observed yet, so a caller
    /// renders "—" rather than a confident zero.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        let total = self.count();
        if total == 0 {
            return None;
        }
        let target = q * total as f64;
        let mut cumulative = 0u64;
        for (i, bucket) in self.buckets.iter().enumerate() {
            let prev = cumulative;
            cumulative += bucket.load(Ordering::Relaxed);
            if (cumulative as f64) < target {
                continue;
            }
            // Everything past the last bound is unbounded above; the best
            // honest answer is the bound itself, not an invented number.
            let Some(upper) = self.bounds.get(i) else {
                return self.bounds.last().copied();
            };
            let lower = if i == 0 { 0.0 } else { self.bounds[i - 1] };
            let in_bucket = (cumulative - prev) as f64;
            if in_bucket == 0.0 {
                return Some(*upper);
            }
            let frac = (target - prev as f64) / in_bucket;
            return Some(lower + (upper - lower) * frac.clamp(0.0, 1.0));
        }
        self.bounds.last().copied()
    }

    /// Render in Prometheus text format. Buckets are **cumulative** — a
    /// `le="0.25"` count includes everything below it — which is what the
    /// format requires and what every consumer assumes.
    pub fn render(&self, name: &str, help: &str) -> String {
        let mut out = format!("# HELP {name} {help}\n# TYPE {name} histogram\n");
        let mut cumulative = 0u64;
        for (i, bucket) in self.buckets.iter().enumerate() {
            cumulative += bucket.load(Ordering::Relaxed);
            match self.bounds.get(i) {
                Some(b) => out.push_str(&format!("{name}_bucket{{le=\"{b}\"}} {cumulative}\n")),
                None => out.push_str(&format!("{name}_bucket{{le=\"+Inf\"}} {cumulative}\n")),
            }
        }
        out.push_str(&format!("{name}_sum {}\n", self.sum_seconds()));
        out.push_str(&format!("{name}_count {}\n", self.count()));
        out
    }
}

/// How a query ended. Kept separate from the error string because the
/// string is deliberately opaque (SEC-5) — an operator still needs to know
/// whether the engine refused the statement, ran out of time, or broke.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryOutcome {
    Ok,
    /// RR-2 server-side cap fired.
    Timeout,
    /// The read-only guard refused it (a COPY, a DDL). Not a fault.
    Refused,
    /// Planning or execution failed.
    Failed,
}

impl QueryOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            QueryOutcome::Ok => "ok",
            QueryOutcome::Timeout => "timeout",
            QueryOutcome::Refused => "refused",
            QueryOutcome::Failed => "failed",
        }
    }
}

/// One finished query. This is the row the self-monitoring sampler writes.
#[derive(Debug, Clone)]
pub struct QueryStats {
    /// Matches the `ref:` in a sanitized error, so a user complaint maps
    /// to a row here and to the full error in the server log.
    pub ref_id: String,
    pub db: String,
    /// Verified client-certificate identity, when there was one (SEC-3).
    pub identity: Option<String>,
    /// Time spent queued on the admission semaphore before starting.
    pub admission_wait: Duration,
    /// Wall clock from arrival to result, admission wait included —
    /// what the caller actually experienced.
    pub duration: Duration,
    pub rows: u64,
    pub outcome: QueryOutcome,
}

/// Receives every finished query.
///
/// A trait rather than a direct call so the layering holds: `timelake-query`
/// knows nothing about buffers, ingest or databases, and the server side
/// implements this to turn records into rows. Implementations run **on the
/// query path** and must not block — the sampler's does an unbounded-fast
/// push onto a bounded queue and drops when full.
pub trait QueryObserver: Send + Sync {
    fn on_query(&self, stats: &QueryStats);
}

/// Aggregate query metrics for `/metrics`.
#[derive(Debug)]
pub struct QueryMetrics {
    pub duration: Histogram,
    pub admission_wait: Histogram,
    in_flight: AtomicU64,
    queued: AtomicU64,
    completed: AtomicU64,
    timeouts: AtomicU64,
    refused: AtomicU64,
    failed: AtomicU64,
}

impl Default for QueryMetrics {
    fn default() -> Self {
        QueryMetrics::new()
    }
}

impl QueryMetrics {
    pub fn new() -> QueryMetrics {
        QueryMetrics {
            duration: Histogram::new(DURATION_BUCKETS),
            admission_wait: Histogram::new(WAIT_BUCKETS),
            in_flight: AtomicU64::new(0),
            queued: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            failed: AtomicU64::new(0),
        }
    }

    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    pub fn queued(&self) -> u64 {
        self.queued.load(Ordering::Relaxed)
    }

    /// Enter the admission queue. The returned guard decrements on drop,
    /// so a caller cancelled *while waiting* — a client that hung up, which
    /// is normal — cannot leak the gauge upward forever.
    pub fn enter_queue(&self) -> CounterGuard<'_> {
        self.queued.fetch_add(1, Ordering::Relaxed);
        CounterGuard(&self.queued)
    }

    /// Admitted and running. Same reasoning: the query path has several
    /// early returns and a timeout, and a gauge that only decrements on the
    /// happy path reads as a permanently busy server.
    pub fn enter_flight(&self) -> CounterGuard<'_> {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        CounterGuard(&self.in_flight)
    }

    pub fn record(&self, stats: &QueryStats) {
        self.duration.observe_duration(stats.duration);
        self.admission_wait.observe_duration(stats.admission_wait);
        self.completed.fetch_add(1, Ordering::Relaxed);
        match stats.outcome {
            QueryOutcome::Ok => {}
            QueryOutcome::Timeout => {
                self.timeouts.fetch_add(1, Ordering::Relaxed);
            }
            QueryOutcome::Refused => {
                self.refused.fetch_add(1, Ordering::Relaxed);
            }
            QueryOutcome::Failed => {
                self.failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn render(&self) -> String {
        let mut out = self.duration.render(
            "timelake_query_duration_seconds",
            "End-to-end query latency, admission wait included.",
        );
        out.push_str(&self.admission_wait.render(
            "timelake_query_admission_wait_seconds",
            "Time queued on the admission semaphore before execution (RR-1).",
        ));
        out.push_str(&format!(
            "# HELP timelake_query_in_flight Queries currently executing.\n\
             # TYPE timelake_query_in_flight gauge\n\
             timelake_query_in_flight {}\n\
             # HELP timelake_query_queued Queries waiting for admission.\n\
             # TYPE timelake_query_queued gauge\n\
             timelake_query_queued {}\n\
             # HELP timelake_queries_total Queries finished, by outcome.\n\
             # TYPE timelake_queries_total counter\n\
             timelake_queries_total {}\n\
             # HELP timelake_query_timeouts_total Queries stopped by the RR-2 cap.\n\
             # TYPE timelake_query_timeouts_total counter\n\
             timelake_query_timeouts_total {}\n\
             # HELP timelake_query_refused_total Statements refused by the read-only guard.\n\
             # TYPE timelake_query_refused_total counter\n\
             timelake_query_refused_total {}\n\
             # HELP timelake_query_failed_total Queries that failed to plan or execute.\n\
             # TYPE timelake_query_failed_total counter\n\
             timelake_query_failed_total {}\n",
            self.in_flight(),
            self.queued(),
            self.completed.load(Ordering::Relaxed),
            self.timeouts.load(Ordering::Relaxed),
            self.refused.load(Ordering::Relaxed),
            self.failed.load(Ordering::Relaxed),
        ));
        out
    }
}

/// Decrements its counter on drop. See [`QueryMetrics::enter_flight`].
pub struct CounterGuard<'a>(&'a AtomicU64);

impl Drop for CounterGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_cumulative_and_include_inf() {
        let h = Histogram::new(DURATION_BUCKETS);
        h.observe(0.0005); // first bucket
        h.observe(0.2); // <= 0.25
        h.observe(120.0); // past every bound -> +Inf only

        let text = h.render("q", "help");
        // Cumulative: the 0.25 line has seen both of the small ones.
        assert!(text.contains("q_bucket{le=\"0.001\"} 1"), "{text}");
        assert!(text.contains("q_bucket{le=\"0.25\"} 2"), "{text}");
        assert!(text.contains("q_bucket{le=\"30\"} 2"), "{text}");
        assert!(text.contains("q_bucket{le=\"+Inf\"} 3"), "{text}");
        assert!(text.contains("q_count 3"), "{text}");
    }

    #[test]
    fn a_value_past_the_last_bound_still_counts_in_sum_and_count() {
        // The tail is where the interesting failures live; clamping it
        // would hide exactly the query that wedged the node.
        let h = Histogram::new(DURATION_BUCKETS);
        h.observe(300.0);
        assert_eq!(h.count(), 1);
        assert!(
            (h.sum_seconds() - 300.0).abs() < 0.01,
            "{}",
            h.sum_seconds()
        );
    }

    #[test]
    fn quantile_is_none_before_any_observation() {
        // A confident 0 would read as "everything is instant" on an idle
        // server, which is the opposite of the truth (nothing is known).
        let h = Histogram::new(DURATION_BUCKETS);
        assert_eq!(h.quantile(0.95), None);
    }

    #[test]
    fn quantile_lands_in_the_right_bucket() {
        let h = Histogram::new(DURATION_BUCKETS);
        for _ in 0..99 {
            h.observe(0.02);
        }
        h.observe(5.0);
        let p50 = h.quantile(0.5).unwrap();
        assert!((0.01..=0.025).contains(&p50), "p50 {p50}");
        let p99 = h.quantile(0.99).unwrap();
        assert!(p99 <= 0.025, "p99 {p99} should still be in the dense band");
        let p999 = h.quantile(0.999).unwrap();
        assert!(p999 > 1.0, "p999 {p999} should reach the slow bucket");
    }

    #[test]
    fn sum_survives_sub_millisecond_observations() {
        // Micros, not millis: a 200 us query is realistic on a warm buffer
        // and must not round to zero.
        let h = Histogram::new(DURATION_BUCKETS);
        for _ in 0..1000 {
            h.observe(0.0002);
        }
        assert!((h.sum_seconds() - 0.2).abs() < 0.01, "{}", h.sum_seconds());
    }

    #[test]
    fn guards_decrement_on_drop_including_the_error_path() {
        let m = QueryMetrics::new();
        {
            let _q = m.enter_queue();
            assert_eq!(m.queued(), 1);
            let _f = m.enter_flight();
            assert_eq!(m.in_flight(), 1);
            // Simulate an early return: both guards drop here.
        }
        assert_eq!(m.queued(), 0);
        assert_eq!(m.in_flight(), 0);
    }

    #[test]
    fn outcomes_are_counted_separately() {
        let m = QueryMetrics::new();
        let stat = |outcome| QueryStats {
            ref_id: "q-0".into(),
            db: "poc".into(),
            identity: None,
            admission_wait: Duration::from_millis(1),
            duration: Duration::from_millis(10),
            rows: 0,
            outcome,
        };
        m.record(&stat(QueryOutcome::Ok));
        m.record(&stat(QueryOutcome::Timeout));
        m.record(&stat(QueryOutcome::Refused));
        m.record(&stat(QueryOutcome::Refused));
        m.record(&stat(QueryOutcome::Failed));

        let text = m.render();
        assert!(text.contains("timelake_queries_total 5"), "{text}");
        assert!(text.contains("timelake_query_timeouts_total 1"), "{text}");
        assert!(text.contains("timelake_query_refused_total 2"), "{text}");
        assert!(text.contains("timelake_query_failed_total 1"), "{text}");
        // Every finished query is timed, whatever the outcome.
        assert_eq!(m.duration.count(), 5);
    }
}
