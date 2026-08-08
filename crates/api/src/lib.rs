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
    /// objects (the /api/sql wire contract).
    fn sql(
        &self,
        db: String,
        query: String,
    ) -> impl std::future::Future<Output = Result<Value, String>> + Send;

    /// Prometheus exposition text (SR-4).
    fn metrics_text(&self) -> String;
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
        .with_state(engine)
}

async fn health() -> Json<Value> {
    Json(json!({
        "status": "pass",
        "name": "timelorddb",
        "version": env!("CARGO_PKG_VERSION"),
        "milestone": "M1",
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
    write_common(state.0, bucket, params.get("precision").cloned(), headers, body).await
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
    let res = tokio::task::spawn_blocking(move || {
        engine.write_lp(&db, &body, precision.as_deref())
    })
    .await;
    match res {
        Ok(Ok(_lines)) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(WriteError::BadRequest(msg))) => err_response(StatusCode::BAD_REQUEST, &msg),
        Ok(Err(WriteError::Internal(msg))) => {
            err_response(StatusCode::INTERNAL_SERVER_ERROR, &msg)
        }
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
}

async fn sql<E: Engine>(
    State(engine): State<Arc<E>>,
    Json(req): Json<SqlRequest>,
) -> axum::response::Response {
    let db = req.db.unwrap_or_else(|| "poc".to_string());
    match engine.sql(db, req.sql).await {
        Ok(rows) => Json(rows).into_response(),
        Err(msg) => err_response(StatusCode::BAD_REQUEST, &msg),
    }
}

use axum::response::IntoResponse;

fn err_response(code: StatusCode, msg: &str) -> axum::response::Response {
    (code, Json(json!({ "error": msg }))).into_response()
}
