//! TimelordDB server — M0 stub.
//!
//! Serves `/health` and `/ping` (the check endpoints Telegraf's output
//! plugins and the tsdb-bench adapter rely on, FR-9) so AT-1 can gate:
//! *adapter health-checks against a stub*. The write and SQL endpoints
//! are declared but answer 501 until M1 — honest about what exists.

use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};

/// Build the M0 router. Kept in the lib so integration tests can drive
/// it without binding a socket.
pub fn app() -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ping", get(ping).head(ping))
        .route("/metrics", get(metrics))
        // FR-9 v1 + v2 and FR-1 v3-style write surfaces (M1):
        .route("/write", post(not_implemented))
        .route("/api/v2/write", post(not_implemented))
        .route("/api/v3/write_lp", post(not_implemented))
        // harness/debug SQL over HTTP (M1); Flight SQL arrives at M3:
        .route("/api/sql", post(not_implemented))
}

async fn health() -> Json<Value> {
    Json(json!({
        "status": "pass",
        "name": "timelorddb",
        "version": env!("CARGO_PKG_VERSION"),
        "milestone": "M0",
    }))
}

async fn ping() -> (StatusCode, [(&'static str, &'static str); 1]) {
    (
        StatusCode::NO_CONTENT,
        [("x-timelorddb-version", env!("CARGO_PKG_VERSION"))],
    )
}

async fn metrics() -> String {
    // Prometheus surface (SR-4); real gauges arrive with the components.
    "# TimelordDB M0 stub — metrics arrive with M1 ingest\n".to_string()
}

async fn not_implemented() -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": "not implemented at M0",
            "see": "ARCHITECTURE.md SS14 milestones",
        })),
    )
}
