//! U1 (timelakedb#110) phase B: the application-log ring. A tracing event is
//! captured into the process-global ring and read back — filtered by level and
//! substring — through the engine's `app_logs`, the same call `/admin/logs`
//! serves. The endpoint's session guard is checked separately.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use tracing_subscriber::prelude::*;

fn engine(dir: &std::path::Path) -> Arc<timelake_server::Engine> {
    timelake_server::Engine::open(dir, timelake_server::EngineConfig::default()).unwrap()
}

#[test]
fn tracing_events_are_captured_and_filtered() {
    // A local subscriber whose only layer is the applog capture layer, which
    // feeds the same process-global ring the engine reads. No env filter, so
    // every level is captured; the engine's `app_logs` does the level filter.
    let _guard = tracing_subscriber::registry()
        .with(timelake_server::applog::layer())
        .set_default();

    // Unique marker so this test sees only its own lines, whatever else the
    // suite logs into the shared ring.
    let marker = "u1-marker-zzq-42";
    tracing::warn!(target: "test_ingest", "boom {marker}");
    tracing::info!(target: "test_query", "quiet {marker}");

    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());

    // level >= warn + the marker: only the warn line, with its level/target.
    let v = e.app_logs("warn", None, Some(marker), 100);
    let warns = v["entries"].as_array().unwrap();
    assert!(
        warns.iter().any(|x| x["level"] == "WARN"
            && x["target"] == "test_ingest"
            && x["message"].as_str().unwrap().contains("boom")),
        "warn line captured with level+target: {v}"
    );
    assert!(
        !warns
            .iter()
            .any(|x| x["message"].as_str().unwrap().contains("quiet")),
        "the info line is below the warn floor: {v}"
    );

    // level >= info + the marker: both lines.
    let v2 = e.app_logs("info", None, Some(marker), 100);
    assert_eq!(
        v2["entries"].as_array().unwrap().len(),
        2,
        "info floor keeps both: {v2}"
    );

    // target filter narrows to one.
    let v3 = e.app_logs("info", Some("test_query"), Some(marker), 100);
    assert_eq!(v3["entries"].as_array().unwrap().len(), 1, "{v3}");
    assert_eq!(v3["entries"][0]["target"], "test_query");
}

#[tokio::test]
async fn logs_endpoint_requires_a_session() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));
    let res = app
        .oneshot(Request::get("/admin/logs").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}
