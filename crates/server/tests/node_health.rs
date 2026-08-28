//! U3 (timelakedb#111): `/health` self-reports the fields the cluster view
//! aggregates — role, applied config revision, catalog head, uptime, id — on
//! top of the status/name/version contract Gauge already reads.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

fn engine(dir: &std::path::Path) -> Arc<timelake_server::Engine> {
    timelake_server::Engine::open(dir, timelake_server::EngineConfig::default()).unwrap()
}

#[tokio::test]
async fn health_reports_the_cluster_view_fields() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));
    let res = app
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    // The unchanged contract Gauge reads.
    assert_eq!(v["status"], "pass");
    assert_eq!(v["name"], "timelakedb");
    assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));

    // The U3 additions.
    assert_eq!(v["role"], "all", "default role with no TIMELAKE_ROLE");
    assert!(v["id"].is_string(), "a node id: {v}");
    assert!(v["config_revision"].is_u64(), "convergence field: {v}");
    assert!(v["catalog_head"].is_u64(), "convergence field: {v}");
    assert!(v["uptime_secs"].is_u64(), "liveness field: {v}");
}
