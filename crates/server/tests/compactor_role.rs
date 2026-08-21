//! The compactor role's surface (C2 phase 5a).
//!
//! The role is built and cannot be selected: `Role::implemented` still
//! refuses `compactor`, deliberately, because a second compactor is only
//! *efficient* once work-avoidance sits on top of the commit fence. The
//! fence already makes it *correct*.
//!
//! Which leaves a code path nobody can reach, and unreachable code that
//! nobody has ever run is code that does not work. These tests exercise
//! what can be exercised without lifting the gate: the HTTP surface a
//! compactor serves, and — more importantly — everything it must refuse.
//!
//! The refusals are the point. A compactor with a data plane bolted on
//! would advertise four write endpoints that nothing should ever call and
//! a query endpoint answering from a catalog view kept fresh only by
//! accident.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

fn engine(dir: &std::path::Path) -> Arc<timelake_server::Engine> {
    timelake_server::Engine::open(
        dir,
        timelake_server::EngineConfig {
            query_mem_bytes: 64 * 1024 * 1024,
            max_concurrent_queries: 2,
            query_timeout_secs: 30,
            ..Default::default()
        },
    )
    .unwrap()
}

async fn status(app: &axum::Router, req: Request<Body>) -> StatusCode {
    app.clone().oneshot(req).await.unwrap().status()
}

#[tokio::test]
async fn a_compactor_serves_health_ping_and_metrics() {
    // It has to be scrapeable. A compactor that has stopped is invisible
    // by construction — its work shows up only as the ABSENCE of file
    // growth, which nothing alerts on.
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::compactor_app(engine(dir.path()));

    // Success, not a specific code: /ping answers 204 by the InfluxDB
    // convention the write endpoints follow, and pinning 200 here would be
    // asserting a detail this test does not care about.
    for path in ["/health", "/ping", "/metrics"] {
        let code = status(&app, Request::get(path).body(Body::empty()).unwrap()).await;
        assert!(code.is_success(), "{path} should be served, got {code}");
    }
}

#[tokio::test]
async fn compaction_counters_are_visible_on_the_compactor_surface() {
    // Not just "metrics returns 200" — the specific counters that say
    // whether this node is doing its job, and whether it is wasting the
    // work when there is more than one of it.
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::compactor_app(engine(dir.path()));
    let res = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body =
        String::from_utf8(res.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();

    for metric in [
        "timelake_compactions_total",
        "timelake_stale_merges_total",
        "timelake_files",
    ] {
        assert!(body.contains(metric), "{metric} missing from /metrics");
    }
}

#[tokio::test]
async fn a_compactor_serves_no_data_plane() {
    // The whole reason `maintenance_app` is built additively rather than
    // by subtracting routes from `app`: a route removed by subtraction
    // comes back the moment somebody adds one to `app` without thinking
    // about this caller.
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::compactor_app(engine(dir.path()));

    let writes = [
        Request::post("/write?db=poc").body(Body::from("cpu v=1 1")),
        Request::post("/api/v2/write?bucket=poc").body(Body::from("cpu v=1 1")),
        Request::post("/api/v3/write_lp?db=poc").body(Body::from("cpu v=1 1")),
    ];
    for req in writes {
        assert_eq!(
            status(&app, req.unwrap()).await,
            StatusCode::NOT_FOUND,
            "a compactor must not accept writes"
        );
    }

    let query = Request::post("/api/sql")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"db":"poc","sql":"SELECT 1"}"#))
        .unwrap();
    assert_eq!(
        status(&app, query).await,
        StatusCode::NOT_FOUND,
        "a compactor answers no queries; that is a querier's job"
    );
}

#[tokio::test]
async fn a_compactor_exposes_no_admin_surface() {
    // The admin routes carry retention and token management. A maintenance
    // node has no business holding either, and mounting them here would
    // widen the attack surface of the one node in a cluster that needs no
    // human touching it at all.
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::compactor_app(engine(dir.path()));

    for path in ["/admin/ui", "/admin/retention", "/admin/tokens"] {
        assert_eq!(
            status(&app, Request::get(path).body(Body::empty()).unwrap()).await,
            StatusCode::NOT_FOUND,
            "{path} must not be reachable on a compactor"
        );
    }
}

#[tokio::test]
async fn read_only_is_what_makes_a_stray_write_loud() {
    // main.rs calls set_read_only() on the compactor. Routing already
    // makes the write endpoints 404, so this is defence in depth for the
    // case that matters: someone mounts the full app on a compactor by
    // mistake. The write should then be refused rather than half-accepted
    // into a buffer nothing on this node will ever flush.
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.set_read_only();
    assert!(eng.is_read_only());

    let app = timelake_server::app(Arc::clone(&eng));
    let code = status(
        &app,
        Request::post("/api/v3/write_lp?db=poc")
            .header("content-type", "text/plain")
            .body(Body::from("cpu,host=a v=1.0 1700000000000000000"))
            .unwrap(),
    )
    .await;
    assert_ne!(
        code,
        StatusCode::NO_CONTENT,
        "a read-only node accepted a write"
    );
}
