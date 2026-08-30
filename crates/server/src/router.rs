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
//! QUERIES GO TO A QUERIER, NEVER TO A SHARD. A query is only correct once
//! every shard is unioned from the shared object store plus the ingesters'
//! live buffers — which is exactly what a querier does (CL-3, C2 phase 4).
//! So `/api/sql` is forwarded to a querier, round-robin, and a router
//! configured with no queriers says so with a 501 rather than guessing at an
//! ingester and returning a confidently short count.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;

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

/// One read target: a querier's public SQL endpoint.
#[derive(Clone)]
pub struct QueryTarget {
    pub id: String,
    sql_url: String,
}

#[derive(Default)]
pub struct RouterStats {
    pub forwarded: AtomicU64,
    pub forward_errors: AtomicU64,
    pub rejected: AtomicU64,
    pub queries_forwarded: AtomicU64,
    pub query_errors: AtomicU64,
}

pub struct RouterState {
    /// Ingester shard targets and querier read targets, held live: an
    /// `ArcSwap` each so a discovery refresh (#71) swaps the set atomically. A
    /// write loads one consistent snapshot, so the shard count that sizes the
    /// grouping and the index that selects a target cannot disagree across a
    /// mid-request swap. The round-robin cursor and the stats stay put, which
    /// is why only these two fields are swapped, not the whole state.
    targets: ArcSwap<Vec<Target>>,
    queriers: ArcSwap<Vec<QueryTarget>>,
    /// Round-robin cursor over the queriers. Reads are stateless, so any
    /// querier can answer any query; spreading them is the whole benefit of
    /// running more than one.
    next_querier: AtomicU64,
    client: reqwest::Client,
    /// Largest request body the router accepts — the same
    /// `TIMELAKE_MAX_BODY_BYTES` the ingesters apply, so the front door
    /// cannot refuse what the nodes behind it would take. Defaults to the
    /// engine's default (32 MiB); `main.rs` sets it from the environment.
    ///
    /// This lives on the state rather than as a `router_app` parameter so
    /// the default is the engine's and not axum's: the 2026-08-13 fix that
    /// raised the limit on the data plane and the internal listener missed
    /// this router entirely, and for nine days a router-role node refused
    /// anything over axum's 2 MiB while its ingesters took 32 (#36).
    max_body_bytes: usize,
    pub stats: RouterStats,
}

/// Build the shard targets, sorted by id so the hash → target mapping is stable
/// across restarts and across a live refresh. One place, so construction and a
/// membership refresh produce identical `Target`s.
fn build_targets(mut ingesters: Vec<(String, String)>) -> Vec<Target> {
    ingesters.sort_by(|a, b| a.0.cmp(&b.0));
    ingesters
        .into_iter()
        .map(|(id, addr)| Target {
            id,
            write_url: format!("http://{addr}/api/v3/write_lp"),
        })
        .collect()
}

fn build_queriers(mut queriers: Vec<(String, String)>) -> Vec<QueryTarget> {
    queriers.sort_by(|a, b| a.0.cmp(&b.0));
    queriers
        .into_iter()
        .map(|(id, addr)| QueryTarget {
            id,
            sql_url: format!("http://{addr}/api/sql"),
        })
        .collect()
}

impl RouterState {
    /// `ingesters` is `(id, data_address)` for each ingester the router can
    /// reach. Order is fixed (sorted by id) so the hash → target mapping is
    /// stable across router restarts.
    pub fn new(ingesters: Vec<(String, String)>) -> RouterState {
        RouterState::with_queriers(ingesters, Vec::new())
    }

    /// As [`RouterState::new`], plus the queriers `/api/sql` is forwarded to.
    pub fn with_queriers(
        ingesters: Vec<(String, String)>,
        queriers: Vec<(String, String)>,
    ) -> RouterState {
        RouterState {
            targets: ArcSwap::from_pointee(build_targets(ingesters)),
            queriers: ArcSwap::from_pointee(build_queriers(queriers)),
            next_querier: AtomicU64::new(0),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("router client"),
            max_body_bytes: crate::EngineConfig::default().max_body_bytes,
            stats: RouterStats::default(),
        }
    }

    /// Accept request bodies up to `n` bytes. Set from
    /// `TIMELAKE_MAX_BODY_BYTES` in `main.rs`, so the router and the
    /// ingesters it fronts cannot disagree about what a client may send.
    pub fn with_max_body_bytes(mut self, n: usize) -> RouterState {
        self.max_body_bytes = n;
        self
    }

    pub fn max_body_bytes(&self) -> usize {
        self.max_body_bytes
    }

    /// Replace the ingester and querier sets from a live discovery refresh
    /// (#71). Atomic per set; a request loads a whole snapshot, so it never
    /// sees half of a swap.
    pub fn update(&self, ingesters: Vec<(String, String)>, queriers: Vec<(String, String)>) {
        self.targets.store(Arc::new(build_targets(ingesters)));
        self.queriers.store(Arc::new(build_queriers(queriers)));
    }

    pub fn target_ids(&self) -> Vec<String> {
        self.targets.load().iter().map(|t| t.id.clone()).collect()
    }

    pub fn querier_ids(&self) -> Vec<String> {
        self.queriers.load().iter().map(|q| q.id.clone()).collect()
    }

    /// Every querier, starting at the next one in the rotation.
    ///
    /// A dead querier must cost a retry, not half the queries: the router
    /// health-checks nothing (discovery carries no correctness, CL-5), so
    /// "is it up" is answered by asking. Queriers are stateless and
    /// interchangeable, which is exactly what makes falling through to the
    /// next one safe — it is the same answer from a different process.
    fn querier_rotation(&self) -> Vec<QueryTarget> {
        let queriers = self.queriers.load();
        if queriers.is_empty() {
            return Vec::new();
        }
        let n = self.next_querier.fetch_add(1, Ordering::Relaxed) as usize;
        (0..queriers.len())
            .map(|i| queriers[(n + i) % queriers.len()].clone())
            .collect()
    }
}

/// The router's HTTP surface: the write endpoints, query forwarding, and
/// health/ping/metrics.
///
/// The body limit is applied here, as it is on the data plane and the
/// internal listener (`lib.rs`), because the `Bytes` extractor in `write`
/// carries axum's 2 MiB default otherwise — and FR-1 wants batches of
/// 10 MB and more through exactly this endpoint. It caps the bytes on the
/// wire: gzip is decompressed inside the handler, same as everywhere else.
pub fn router_app(state: Arc<RouterState>) -> axum::Router {
    let limit = state.max_body_bytes;
    axum::Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/ping", get(|| async { StatusCode::NO_CONTENT }))
        .route("/metrics", get(router_metrics))
        .route("/write", post(write))
        .route("/api/v2/write", post(write))
        .route("/api/v3/write_lp", post(write))
        .route("/api/sql", post(sql))
        .layer(axum::extract::DefaultBodyLimit::max(limit))
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
         timelake_router_ingesters {}\n\
         # TYPE timelake_router_queries_forwarded_total counter\n\
         timelake_router_queries_forwarded_total {}\n\
         # TYPE timelake_router_query_errors_total counter\n\
         timelake_router_query_errors_total {}\n\
         # TYPE timelake_router_queriers gauge\n\
         timelake_router_queriers {}\n",
        state.stats.forwarded.load(Ordering::Relaxed),
        state.stats.forward_errors.load(Ordering::Relaxed),
        state.stats.rejected.load(Ordering::Relaxed),
        state.targets.load().len(),
        state.stats.queries_forwarded.load(Ordering::Relaxed),
        state.stats.query_errors.load(Ordering::Relaxed),
        state.queriers.load().len(),
    )
}

/// Forward one SQL request to a querier and hand back its answer verbatim.
///
/// The body is passed through untouched — the router does not parse SQL and
/// has no opinion about it. The credential headers travel with it, because
/// the querier is where SEC-2 visibility and SEC-4 data auth are decided;
/// dropping them here would silently widen or narrow what a caller sees.
async fn sql(
    State(state): State<Arc<RouterState>>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let rotation = state.querier_rotation();
    if rotation.is_empty() {
        return (
            StatusCode::NOT_IMPLEMENTED,
            "this router has no queriers configured (TIMELAKE_PEERS needs \
             id=querier@cluster_addr|data_addr). A query must union every shard, \
             so it cannot be answered by an ingester.",
        )
            .into_response();
    }
    let mut last_error = String::new();
    for target in rotation {
        let mut req = state
            .client
            .post(&target.sql_url)
            .header("content-type", "application/json")
            .body(body.clone());
        for name in [
            "authorization",
            "x-timelake-authorizations",
            "accept-encoding",
        ] {
            if let Some(v) = headers.get(name) {
                req = req.header(name, v);
            }
        }
        match req.send().await {
            Ok(resp) => {
                state
                    .stats
                    .queries_forwarded
                    .fetch_add(1, Ordering::Relaxed);
                let status =
                    StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
                if !status.is_success() {
                    // A status IS an answer — including a querier's refusal
                    // to answer from an incomplete cluster. Asking the next
                    // querier would put the same question to a node with
                    // the same view, and retrying a real error is how a
                    // clear failure turns into a mystery.
                    state.stats.query_errors.fetch_add(1, Ordering::Relaxed);
                }
                let ct = resp
                    .headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("application/json")
                    .to_string();
                return match resp.bytes().await {
                    Ok(bytes) => (status, [("content-type", ct.as_str())], bytes).into_response(),
                    Err(e) => {
                        state.stats.query_errors.fetch_add(1, Ordering::Relaxed);
                        err(
                            StatusCode::BAD_GATEWAY,
                            &format!("querier {} response failed: {e}", target.id),
                        )
                    }
                };
            }
            Err(e) => {
                // Transport failure: that querier is not there. Queriers are
                // stateless and interchangeable, so the next one answers the
                // same question identically — a dead one costs a retry, not
                // half the queries.
                state.stats.query_errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    querier = %target.id, error = %e,
                    "query forward failed; falling through to the next querier"
                );
                last_error = format!("{}: {e}", target.id);
            }
        }
    }
    err(
        StatusCode::BAD_GATEWAY,
        &format!("no querier could be reached (last {last_error})"),
    )
}

/// The write handler. Reads the database from the request (`db` or `bucket`),
/// validates the whole body, shards it by measurement, and forwards each
/// shard. The db/precision the client sent are passed straight through.
async fn write(
    State(state): State<Arc<RouterState>>,
    Query(params): Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
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
    // One consistent snapshot of the shard targets for the whole request: the
    // shard count that sizes the grouping and the index that selects a target
    // must agree, even if a live membership refresh swaps the set mid-request
    // (#71). load_full is an Arc clone, so the snapshot is owned across the
    // forward loop's awaits.
    let targets = state.targets.load_full();
    if targets.is_empty() {
        return err(
            StatusCode::SERVICE_UNAVAILABLE,
            "router has no ingesters configured (TIMELAKE_PEERS)",
        );
    }
    let precision = params.get("precision").cloned();

    // Decompress before validating, exactly as the single-node endpoint
    // does (api::maybe_gunzip). The router sells itself as the drop-in
    // single write endpoint, and both Tributary (gzip on by default) and
    // Telegraf's influxdb_v2 output send gzip bodies — the first version
    // of this handler read the raw gzip bytes as line protocol and 400'd
    // every batch, so a fleet of correct agents pointed at the correct
    // endpoint quarantined its entire output as poison (Catchment
    // router-tributary-exactness, 2026-08-13). Shards are forwarded PLAIN:
    // the router re-chunks bodies, so what it forwards must stand alone,
    // and the intra-cluster hop is a LAN where correctness outranks the
    // recompression it would cost.
    let gzipped = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("gzip"));
    let decompressed;
    let raw: &[u8] = if gzipped {
        use std::io::Read as _;
        let mut out = Vec::with_capacity(body.len() * 4);
        if let Err(e) = flate2::read::GzDecoder::new(&body[..]).read_to_end(&mut out) {
            return err(StatusCode::BAD_REQUEST, &format!("bad gzip body: {e}"));
        }
        decompressed = out;
        &decompressed
    } else {
        &body
    };
    let text = match std::str::from_utf8(raw) {
        Ok(t) => t,
        Err(_) => return err(StatusCode::BAD_REQUEST, "body is not utf-8"),
    };

    // Parse the WHOLE body exactly as an ingester would, before forwarding
    // anything (#38). The loop below only checked that each line had a
    // measurement, so "poison line writes zero" was true of a line with no
    // measurement and false of one with a bad field value: that line
    // reached one shard and was refused there, after every other shard
    // had already been acknowledged — a partial landing the client saw as
    // a 400, which an agent then quarantined as poison while the rest of
    // its batch sat durable in the database. Same parser, same precision
    // the client asked for (a seconds-precision body must not be refused
    // for "bad timestamp" under a default of ns), same error text, so the
    // router's 400 reads like an ingester's. Cost: one extra parse per
    // body, measured with Gauge through the router before this landed.
    let mult = match precision.as_deref() {
        None => 1,
        Some(p) => match timelake_ingest::precision_multiplier(p) {
            Some(m) => m,
            None => {
                state.stats.rejected.fetch_add(1, Ordering::Relaxed);
                return err(StatusCode::BAD_REQUEST, &format!("bad precision {p:?}"));
            }
        },
    };
    if let Err(e) = timelake_ingest::parse_lines(text, mult, 0) {
        state.stats.rejected.fetch_add(1, Ordering::Relaxed);
        return err(StatusCode::BAD_REQUEST, &e.to_string());
    }

    // Group original lines by their shard target. (Every line has a
    // measurement now — the parse above refused any that did not — but the
    // check stays as the loop's own guard rather than an assumption about
    // the code twenty lines up.)
    let mut shards: Vec<String> = vec![String::new(); targets.len()];
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
        let idx = shard_of(&db, measurement, targets.len());
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
        let target = &targets[idx];
        let mut req = state
            .client
            .post(&target.write_url)
            .query(&[("db", db.as_str())]);
        if let Some(p) = &precision {
            req = req.query(&[("precision", p.as_str())]);
        }
        // The client's credential travels with every shard. The ingester
        // is where SEC-4 data auth is decided for a write — the router has
        // no token store and must not grow one — so dropping the header
        // here made a cluster behind a router unable to run
        // TIMELAKE_DATA_AUTH=required at all: every forwarded shard arrived
        // anonymous and was refused 401, which came back to a client
        // holding a perfectly good token. In `optional` mode it was quieter
        // and worse: the ingesters' authenticated/anonymous split, the
        // measurement an operator flips to `required` on, counted every
        // router write as anonymous (#37). Only `Authorization` — the write
        // path reads nothing else; SEC-2 on a write is the `_visibility`
        // tag in the body.
        if let Some(v) = headers.get("authorization") {
            req = req.header("authorization", v);
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
    fn queries_rotate_across_queriers_and_the_rotation_is_stable() {
        let s = RouterState::with_queriers(
            vec![("ing-a".into(), "a:1963".into())],
            vec![
                ("qry-b".into(), "qb:1963".into()),
                ("qry-a".into(), "qa:1963".into()),
            ],
        );
        assert_eq!(s.querier_ids(), vec!["qry-a", "qry-b"], "sorted by id");
        let first: Vec<String> = (0..4).map(|_| s.querier_rotation()[0].id.clone()).collect();
        assert_eq!(first, vec!["qry-a", "qry-b", "qry-a", "qry-b"]);
    }

    #[test]
    fn the_rotation_offers_every_querier_so_a_dead_one_costs_a_retry() {
        // Without the fall-through, a dead querier fails every other query
        // forever — round-robin turns one outage into a 50% error rate.
        let s = RouterState::with_queriers(
            vec![("ing-a".into(), "a:1963".into())],
            vec![
                ("qry-a".into(), "qa:1963".into()),
                ("qry-b".into(), "qb:1963".into()),
                ("qry-c".into(), "qc:1963".into()),
            ],
        );
        let ids = |r: Vec<QueryTarget>| r.into_iter().map(|q| q.id.clone()).collect::<Vec<_>>();
        assert_eq!(ids(s.querier_rotation()), vec!["qry-a", "qry-b", "qry-c"]);
        assert_eq!(
            ids(s.querier_rotation()),
            vec!["qry-b", "qry-c", "qry-a"],
            "the next query starts at the next querier, but can still reach all"
        );
    }

    #[test]
    fn with_no_queriers_a_query_has_nowhere_correct_to_go() {
        // Deliberately NOT "fall back to an ingester": that answers with one
        // shard's rows and looks like a successful query.
        let s = RouterState::new(vec![("ing-a".into(), "a:1963".into())]);
        assert!(s.querier_rotation().is_empty());
        assert!(s.querier_ids().is_empty());
    }

    #[test]
    fn a_live_update_swaps_the_targets_and_queriers() {
        // #71 phase 3: a discovery refresh replaces the sets in place, so a
        // joined node is routed to and a departed one is dropped without a
        // restart. The round-robin cursor is untouched (it is not swapped).
        let s = RouterState::new(vec![("ing-a".into(), "a:1963".into())]);
        assert_eq!(s.target_ids(), vec!["ing-a"]);
        s.update(
            vec![
                ("ing-a".into(), "a:1963".into()),
                ("ing-b".into(), "b:1963".into()),
            ],
            vec![("qry-a".into(), "qa:1963".into())],
        );
        assert_eq!(
            s.target_ids(),
            vec!["ing-a", "ing-b"],
            "a joined ingester is a shard target"
        );
        assert_eq!(
            s.querier_ids(),
            vec!["qry-a"],
            "a joined querier is offered"
        );
        s.update(vec![("ing-b".into(), "b:1963".into())], vec![]);
        assert_eq!(
            s.target_ids(),
            vec!["ing-b"],
            "a departed ingester is dropped"
        );
        assert!(
            s.querier_rotation().is_empty(),
            "the departed querier is gone"
        );
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
