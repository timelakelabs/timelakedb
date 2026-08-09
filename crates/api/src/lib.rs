//! HTTP surface (FR-1, FR-9): /write (v1), /api/v2/write (v2),
//! /api/v3/write_lp (v3-style), /api/sql (harness/debug), /health, /ping,
//! /metrics. Speaks gzip Content-Encoding (Telegraf's influxdb_v2 output
//! default) and the s|ms|us|ns precision parameters; success is 204,
//! parse errors are 400 with the offending line identified.
//!
//! The router is generic over the [`Engine`] trait so this crate owns the
//! wire contract while timelord-server owns the machinery.

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

/// Errors the write path can surface over the wire.
pub enum WriteError {
    /// 400 — the request is at fault (parse error, type conflict, bad
    /// precision); the message identifies the line/field (FR-9).
    BadRequest(String),
    /// 429 + Retry-After — a named, visible limit (RR-5): the WAL is at
    /// its cap and flush needs to catch up before more writes land.
    Backpressure(String),
    /// 500 — the engine failed to make the write durable.
    Internal(String),
}

/// The seam between HTTP and the engine. Implemented by
/// `timelord_server::Engine`; mockable for endpoint tests.
pub trait Engine: Send + Sync + 'static {
    /// Parse, make durable (WAL), and apply one line-protocol body.
    /// Returns the number of lines written. Blocking (fsync) — the
    /// router calls it via `spawn_blocking`.
    fn write_lp(&self, db: &str, body: &[u8], precision: Option<&str>)
    -> Result<usize, WriteError>;

    /// Execute SQL against one database; returns a JSON array of row
    /// objects (the /api/sql wire contract). `authorizations` are the
    /// session's visibility authorizations (SEC-2), from the
    /// `X-Timelord-Authorizations` header and/or the request body.
    fn sql(
        &self,
        db: String,
        query: String,
        authorizations: Vec<String>,
    ) -> impl std::future::Future<Output = Result<Value, String>> + Send;

    /// Prometheus exposition text (SR-4).
    fn metrics_text(&self) -> String;

    /// Current per-table retention policies (FR-7), table → seconds.
    fn retention_policies(&self) -> Vec<(String, u64)>;

    /// Set (upsert) one table's retention window, durably.
    fn set_retention(&self, table: &str, seconds: u64) -> Result<(), String>;

    /// Remove one table's policy (the table keeps everything again).
    fn remove_retention(&self, table: &str) -> Result<(), String>;
}

/// Parse "365d", "72h", "90m", or bare seconds — the write half of the
/// same grammar `TIMELORD_RETENTION` seeds with.
pub fn parse_duration_secs(s: &str) -> Option<u64> {
    let s = s.trim();
    let (num, unit) = match s.chars().last()? {
        'd' => (&s[..s.len() - 1], 86_400),
        'h' => (&s[..s.len() - 1], 3_600),
        'm' => (&s[..s.len() - 1], 60),
        's' => (&s[..s.len() - 1], 1),
        _ => (s, 1),
    };
    let n: u64 = num.trim().parse().ok()?;
    n.checked_mul(unit).filter(|v| *v > 0)
}

/// Render seconds in the largest exact unit — the inverse of
/// [`parse_duration_secs`], for display.
pub fn humanize_secs(secs: u64) -> String {
    if secs.is_multiple_of(86_400) {
        format!("{}d", secs / 86_400)
    } else if secs.is_multiple_of(3_600) {
        format!("{}h", secs / 3_600)
    } else if secs.is_multiple_of(60) {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

pub fn app<E: Engine>(engine: Arc<E>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ping", get(ping::<E>).head(ping::<E>))
        .route("/metrics", get(metrics::<E>))
        .route("/write", post(write_v1::<E>))
        .route("/api/v2/write", post(write_v2::<E>))
        .route("/api/v3/write_lp", post(write_v3::<E>))
        .route("/api/sql", post(sql::<E>))
        .route(
            "/admin/retention",
            get(retention_list::<E>).put(retention_set::<E>),
        )
        .route(
            "/admin/retention/{table}",
            axum::routing::delete(retention_delete::<E>),
        )
        .route("/admin/ui", get(admin_ui))
        .with_state(engine)
}

/// The management page (FR-7 GUI): a single self-contained file — no
/// build step, no external assets, same rules as `site/`.
async fn admin_ui() -> axum::response::Html<&'static str> {
    axum::response::Html(include_str!("admin_ui.html"))
}

async fn retention_list<E: Engine>(State(engine): State<Arc<E>>) -> Json<Value> {
    let mut policies: Vec<Value> = engine
        .retention_policies()
        .into_iter()
        .map(|(table, seconds)| {
            json!({
                "table": table,
                "seconds": seconds,
                "duration": humanize_secs(seconds),
            })
        })
        .collect();
    policies.sort_by_key(|p| p["table"].as_str().unwrap_or("").to_string());
    Json(json!({ "policies": policies }))
}

#[derive(Deserialize)]
struct RetentionSet {
    table: String,
    /// "365d", "72h", "90m", or seconds.
    duration: String,
}

async fn retention_set<E: Engine>(
    State(engine): State<Arc<E>>,
    Json(req): Json<RetentionSet>,
) -> axum::response::Response {
    let table = req.table.trim();
    if table.is_empty() {
        return err_response(StatusCode::BAD_REQUEST, "table must not be empty");
    }
    let Some(seconds) = parse_duration_secs(&req.duration) else {
        return err_response(
            StatusCode::BAD_REQUEST,
            &format!(
                "duration {:?} is not <n>d, <n>h, <n>m, or seconds (and must be > 0)",
                req.duration
            ),
        );
    };
    match engine.set_retention(table, seconds) {
        Ok(()) => Json(json!({
            "table": table,
            "seconds": seconds,
            "duration": humanize_secs(seconds),
            "status": "set",
        }))
        .into_response(),
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, &e),
    }
}

async fn retention_delete<E: Engine>(
    State(engine): State<Arc<E>>,
    axum::extract::Path(table): axum::extract::Path<String>,
) -> axum::response::Response {
    match engine.remove_retention(&table) {
        Ok(()) => Json(json!({ "table": table, "status": "removed" })).into_response(),
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, &e),
    }
}

async fn health() -> Json<Value> {
    Json(json!({
        "status": "pass",
        "name": "timelorddb",
        "version": env!("CARGO_PKG_VERSION"),
        "milestone": "M3",
    }))
}

async fn ping<E: Engine>(
    State(_): State<Arc<E>>,
) -> (StatusCode, [(&'static str, &'static str); 1]) {
    (
        StatusCode::NO_CONTENT,
        [("x-timelorddb-version", env!("CARGO_PKG_VERSION"))],
    )
}

async fn metrics<E: Engine>(State(engine): State<Arc<E>>) -> String {
    engine.metrics_text()
}

type Params = Query<HashMap<String, String>>;

async fn write_v1<E: Engine>(
    state: State<Arc<E>>,
    Query(params): Params,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let Some(db) = params.get("db").cloned() else {
        return err_response(StatusCode::BAD_REQUEST, "missing 'db' parameter");
    };
    write_common(state.0, db, params.get("precision").cloned(), headers, body).await
}

async fn write_v2<E: Engine>(
    state: State<Arc<E>>,
    Query(params): Params,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    // v2 addresses a bucket; org is accepted and ignored (single-tenant M1)
    let Some(bucket) = params.get("bucket").cloned() else {
        return err_response(StatusCode::BAD_REQUEST, "missing 'bucket' parameter");
    };
    write_common(
        state.0,
        bucket,
        params.get("precision").cloned(),
        headers,
        body,
    )
    .await
}

async fn write_v3<E: Engine>(
    state: State<Arc<E>>,
    Query(params): Params,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let Some(db) = params.get("db").cloned() else {
        return err_response(StatusCode::BAD_REQUEST, "missing 'db' parameter");
    };
    write_common(state.0, db, params.get("precision").cloned(), headers, body).await
}

async fn write_common<E: Engine>(
    engine: Arc<E>,
    db: String,
    precision: Option<String>,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let body = match maybe_gunzip(&headers, body) {
        Ok(b) => b,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &format!("bad gzip body: {e}")),
    };
    let res =
        tokio::task::spawn_blocking(move || engine.write_lp(&db, &body, precision.as_deref()))
            .await;
    match res {
        Ok(Ok(_lines)) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(WriteError::BadRequest(msg))) => err_response(StatusCode::BAD_REQUEST, &msg),
        Ok(Err(WriteError::Backpressure(msg))) => (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", "1")],
            Json(json!({ "error": msg })),
        )
            .into_response(),
        Ok(Err(WriteError::Internal(msg))) => err_response(StatusCode::INTERNAL_SERVER_ERROR, &msg),
        Err(join) => err_response(StatusCode::INTERNAL_SERVER_ERROR, &join.to_string()),
    }
}

fn maybe_gunzip(headers: &HeaderMap, body: Bytes) -> std::io::Result<Vec<u8>> {
    let gzipped = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("gzip"));
    if gzipped {
        let mut out = Vec::with_capacity(body.len() * 4);
        flate2::read::GzDecoder::new(&body[..]).read_to_end(&mut out)?;
        Ok(out)
    } else {
        Ok(body.to_vec())
    }
}

#[derive(Deserialize)]
struct SqlRequest {
    #[serde(default)]
    db: Option<String>,
    sql: String,
    /// Visibility authorizations (SEC-2); unioned with the
    /// `X-Timelord-Authorizations` header.
    #[serde(default)]
    authorizations: Vec<String>,
}

/// Parse a comma-separated authorizations header value. Claims, not
/// credentials, while the surface has no authn (SECURITY.md posture);
/// the seam is what SEC-2 mandates, and a token layer slots in front.
fn auths_from_headers(headers: &HeaderMap) -> Vec<String> {
    headers
        .get("x-timelord-authorizations")
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

async fn sql<E: Engine>(
    State(engine): State<Arc<E>>,
    headers: HeaderMap,
    Json(req): Json<SqlRequest>,
) -> axum::response::Response {
    let db = req.db.unwrap_or_else(|| "poc".to_string());
    let mut auths = auths_from_headers(&headers);
    for a in req.authorizations {
        if !auths.contains(&a) {
            auths.push(a);
        }
    }
    match engine.sql(db, req.sql, auths).await {
        Ok(rows) => Json(rows).into_response(),
        Err(msg) => err_response(StatusCode::BAD_REQUEST, &msg),
    }
}

use axum::response::IntoResponse;

fn err_response(code: StatusCode, msg: &str) -> axum::response::Response {
    (code, Json(json!({ "error": msg }))).into_response()
}
