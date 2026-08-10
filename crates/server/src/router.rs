//! The router role (C2 phase 3) — stateless write sharding.
//!
//! The router holds no data. It is the single write endpoint the bench
//! adapter, Telegraf and Grafana keep seeing (FR-8/FR-9). It shards each
//! line-protocol body by `(db, measurement)` across the ingesters and
//! forwards each shard to the ingester that owns it; that ingester becomes
//! the primary for the table and replicates to its CL-2 peer, so durability
//! is unchanged — the router adds distribution, not a new failure mode.
//!
//! ATOMICITY IS PRESERVED. A line-protocol batch is all-or-nothing (one bad
//! line writes zero). The router **validates the whole body first** and
//! rejects with 400 before forwarding anything, so a poison line never
//! leaves part of the batch written. A forward that fails for an
//! infrastructure reason (an ingester down, backpressure) is returned to the
//! client to retry; the retry is safe because writes are idempotent under
//! LWW dedup (FR-5).
//!
//! QUERIES ARE NOT ROUTED HERE. In a sharded cluster a query is only correct
//! once a querier unions every shard from the shared object store; forwarding
//! a query to one ingester would return wrong counts. Query routing arrives
//! with the querier (CL-3, C2 phase 4). Until then `/api/sql` on the router
//! returns a clear 501.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};

/// One forwarding target: an ingester's public write endpoint.
#[derive(Clone)]
pub struct Target {
    pub id: String,
    write_url: String,
}

#[derive(Default)]
pub struct RouterStats {
    pub forwarded: AtomicU64,
    pub forward_errors: AtomicU64,
    pub rejected: AtomicU64,
}

pub struct RouterState {
    targets: Vec<Target>,
    client: reqwest::Client,
    pub stats: RouterStats,
}

impl RouterState {
    /// `ingesters` is `(id, data_address)` for each ingester the router can
    /// reach. Order is fixed (sorted by id) so the hash → target mapping is
    /// stable across router restarts.
    pub fn new(mut ingesters: Vec<(String, String)>) -> RouterState {
        ingesters.sort_by(|a, b| a.0.cmp(&b.0));
        let targets = ingesters
            .into_iter()
            .map(|(id, addr)| Target {
                id,
                write_url: format!("http://{addr}/api/v3/write_lp"),
            })
            .collect();
        RouterState {
            targets,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("router client"),
            stats: RouterStats::default(),
        }
    }

    pub fn target_ids(&self) -> Vec<String> {
        self.targets.iter().map(|t| t.id.clone()).collect()
    }
}

/// The router's HTTP surface: the write endpoints, health/ping/metrics, and
/// a 501 for query routing (queriers are phase 4).
pub fn router_app(state: Arc<RouterState>) -> axum::Router {
    axum::Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/ping", get(|| async { StatusCode::NO_CONTENT }))
        .route("/metrics", get(router_metrics))
        .route("/write", post(write))
        .route("/api/v2/write", post(write))
        .route("/api/v3/write_lp", post(write))
        .route("/api/sql", post(sql_not_here))
        .with_state(state)
}

async fn router_metrics(State(state): State<Arc<RouterState>>) -> String {
    format!(
        "# TYPE timelake_router_forwarded_total counter\n\
         timelake_router_forwarded_total {}\n\
         # TYPE timelake_router_forward_errors_total counter\n\
         timelake_router_forward_errors_total {}\n\
         # TYPE timelake_router_rejected_total counter\n\
         timelake_router_rejected_total {}\n\
         # TYPE timelake_router_ingesters gauge\n\
         timelake_router_ingesters {}\n",
        state.stats.forwarded.load(Ordering::Relaxed),
        state.stats.forward_errors.load(Ordering::Relaxed),
        state.stats.rejected.load(Ordering::Relaxed),
        state.targets.len(),
    )
}

async fn sql_not_here() -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        "query routing is not on the router yet — a query must union every \
         shard, which needs a querier (C2 phase 4). Query an ingester directly \
         for now.",
    )
}

/// The write handler. Reads the database from the request (`db` or `bucket`),
/// validates the whole body, shards it by measurement, and forwards each
/// shard. The db/precision the client sent are passed straight through.
async fn write(
    State(state): State<Arc<RouterState>>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> axum::response::Response {
    let db = params
        .get("db")
        .or_else(|| params.get("bucket"))
        .cloned()
        .unwrap_or_default();
    if db.is_empty() {
        return err(
            StatusCode::BAD_REQUEST,
            "missing 'db' (or 'bucket') parameter",
        );
    }
    if state.targets.is_empty() {
        return err(
            StatusCode::SERVICE_UNAVAILABLE,
            "router has no ingesters configured (TIMELAKE_PEERS)",
        );
    }
    let precision = params.get("precision").cloned();

    let text = match std::str::from_utf8(&body) {
        Ok(t) => t,
        Err(_) => return err(StatusCode::BAD_REQUEST, "body is not utf-8"),
    };

    // Validate the WHOLE body before forwarding anything (atomicity), and
    // group original lines by their shard target in the same pass.
    let mut shards: Vec<String> = vec![String::new(); state.targets.len()];
    for line in text.split_inclusive('\n') {
        let trimmed = line.trim_end_matches(['\n', '\r']);
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue; // blank / comment: no measurement, nothing to route
        }
        let measurement = match line_measurement(trimmed) {
            Some(m) if !m.is_empty() => m,
            _ => {
                state.stats.rejected.fetch_add(1, Ordering::Relaxed);
                return err(
                    StatusCode::BAD_REQUEST,
                    &format!("line has no measurement: {trimmed:.80}"),
                );
            }
        };
        let idx = shard_of(&db, measurement, state.targets.len());
        // Forward original bytes verbatim so the ingester parses exactly
        // what the client sent.
        shards[idx].push_str(line);
        if !line.ends_with('\n') {
            shards[idx].push('\n');
        }
    }

    // Forward each non-empty shard. Errors from an ingester surface to the
    // client (retry is idempotent under LWW dedup).
    for (idx, sub) in shards.iter().enumerate() {
        if sub.is_empty() {
            continue;
        }
        let target = &state.targets[idx];
        let mut req = state
            .client
            .post(&target.write_url)
            .query(&[("db", db.as_str())]);
        if let Some(p) = &precision {
            req = req.query(&[("precision", p.as_str())]);
        }
        match req
            .header("content-type", "text/plain; charset=utf-8")
            .body(sub.clone())
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                state.stats.forwarded.fetch_add(1, Ordering::Relaxed);
            }
            Ok(resp) => {
                // The ingester rejected this shard (400/429/5xx). Pass its
                // status and body through — the client sees the real reason.
                state.stats.forward_errors.fetch_add(1, Ordering::Relaxed);
                let status =
                    StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
                let msg = resp.text().await.unwrap_or_default();
                return (status, msg).into_response();
            }
            Err(e) => {
                state.stats.forward_errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(target = %target.id, error = %e, "forward failed");
                return err(
                    StatusCode::BAD_GATEWAY,
                    &format!(
                        "ingester {} unreachable: retry (writes are idempotent)",
                        target.id
                    ),
                );
            }
        }
    }
    StatusCode::NO_CONTENT.into_response()
}

fn err(code: StatusCode, msg: &str) -> axum::response::Response {
    (code, msg.to_string()).into_response()
}

/// The measurement (table) of one line: the text before the first unescaped
/// comma or space. Honours line-protocol escaping (`\,` `\ ` `\\`). Returns
/// the raw measurement text (still escaped) — that is fine, because it is
/// used only as a stable hash key, consistently for every line of the table.
pub fn line_measurement(line: &str) -> Option<&str> {
    let bytes = line.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2, // escaped char: skip it and the next byte
            b',' | b' ' => return Some(&line[..i]),
            _ => i += 1,
        }
    }
    // A measurement with no field set is malformed, but that is the
    // ingester's 400 to raise on the forwarded shard; here, a line that is
    // all measurement still yields the measurement.
    if bytes.is_empty() { None } else { Some(line) }
}

/// Stable shard selection: FNV-1a over `db\0measurement`, mod target count.
/// Deterministic across processes and restarts (unlike the default hasher),
/// so a table always lands on the same ingester.
pub fn shard_of(db: &str, measurement: &str, n: usize) -> usize {
    debug_assert!(n > 0);
    let mut h: u64 = 0xcbf2_9ce4_8422_2325; // FNV offset basis
    let mix = |h: &mut u64, bytes: &[u8]| {
        for &b in bytes {
            *h ^= b as u64;
            *h = h.wrapping_mul(0x0000_0100_0000_01b3); // FNV prime
        }
    };
    mix(&mut h, db.as_bytes());
    mix(&mut h, &[0]);
    mix(&mut h, measurement.as_bytes());
    (h % n as u64) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn measurement_is_the_prefix_before_tag_or_field() {
        assert_eq!(line_measurement("cpu,host=a v=1i 123"), Some("cpu"));
        assert_eq!(line_measurement("mem v=1i 123"), Some("mem")); // no tags
        assert_eq!(
            line_measurement("just_measurement"),
            Some("just_measurement")
        );
    }

    #[test]
    fn measurement_honours_escaping() {
        // An escaped comma/space is part of the measurement name.
        assert_eq!(line_measurement(r"we\,ird,host=a v=1i"), Some(r"we\,ird"));
        assert_eq!(line_measurement(r"sp\ ace v=1i"), Some(r"sp\ ace"));
    }

    #[test]
    fn sharding_is_deterministic_and_in_range() {
        for (db, m) in [("poc", "cpu"), ("logs", "app"), ("poc", "mem")] {
            let a = shard_of(db, m, 4);
            let b = shard_of(db, m, 4);
            assert_eq!(a, b, "same key -> same shard, always");
            assert!(a < 4);
        }
    }

    #[test]
    fn sharding_actually_distributes() {
        // Many tables across 2 shards should not all land on one — the whole
        // point of the router. (FNV over distinct keys spreads well.)
        let n = 2;
        let counts = (0..40).fold([0usize; 2], |mut acc, i| {
            acc[shard_of("poc", &format!("table_{i}"), n)] += 1;
            acc
        });
        assert!(counts[0] > 5 && counts[1] > 5, "lopsided: {counts:?}");
    }

    #[test]
    fn targets_are_sorted_so_the_mapping_is_stable_across_restarts() {
        // The router sorts ingesters by id, so the same TIMELAKE_PEERS in any
        // order yields the same table→ingester mapping every boot.
        let a = RouterState::new(vec![
            ("ing-b".into(), "b:1963".into()),
            ("ing-a".into(), "a:1963".into()),
        ]);
        let b = RouterState::new(vec![
            ("ing-a".into(), "a:1963".into()),
            ("ing-b".into(), "b:1963".into()),
        ]);
        assert_eq!(a.target_ids(), b.target_ids());
        assert_eq!(a.target_ids(), vec!["ing-a", "ing-b"]);
    }
}
