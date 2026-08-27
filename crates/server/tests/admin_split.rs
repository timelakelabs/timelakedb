//! U0 (timelakedb#108): the admin plane is split off the data port.
//!
//! What is pinned here: on the data plane (`data_app`, port 1963) every
//! `/admin/*` path is `410 Gone` and names where it went, while `/metrics` and
//! `/health` stay; on the admin plane (`admin_app`, port 1966) the console
//! shell answers, guarded routes demand a session, and the data plane is
//! absent. The listeners are wired in `main.rs`; this exercises the routers the
//! two listeners serve, which is where a route could leak across the boundary.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

fn engine(dir: &std::path::Path) -> Arc<timelake_server::Engine> {
    timelake_server::Engine::open(
        dir,
        timelake_server::EngineConfig {
            flush_rows: 1_000_000,
            flush_age_secs: u64::MAX,
            wal_max_bytes: u64::MAX,
            ..Default::default()
        },
    )
    .unwrap()
}

async fn status(app: &axum::Router, method: &str, path: &str) -> StatusCode {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

/// Every `/admin/*` verb+path is 410 on the data port — one wildcard stub, so a
/// route that is added to the admin plane later cannot reappear on 1963 by
/// omission.
#[tokio::test]
async fn admin_star_is_gone_from_the_data_port() {
    let dir = tempfile::tempdir().unwrap();
    let data = timelake_server::data_app(engine(dir.path()));

    for (method, path) in [
        ("GET", "/admin/retention"),
        ("PUT", "/admin/retention"),
        ("GET", "/admin/ui"),
        ("POST", "/admin/session"),
        ("GET", "/admin/audit"),
        ("GET", "/admin/tokens"),
        ("POST", "/admin/delete"),
        ("POST", "/admin/tls/reload"),
        ("GET", "/admin/cert-grants"),
    ] {
        assert_eq!(
            status(&data, method, path).await,
            StatusCode::GONE,
            "{method} {path} must be 410 on the data port"
        );
    }

    // The 410 body names the new home so a stuck client is not left guessing.
    let res = data
        .oneshot(
            Request::get("/admin/retention")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        v["listener"].as_str().unwrap_or("").contains("1966"),
        "the 410 body should point at the admin listener, got {v}"
    );
}

/// `/metrics` and `/health` stay on the data port — `/metrics` is
/// unauthenticated by design and Prometheus scrapes it, so it must NOT move
/// behind the admin listener.
#[tokio::test]
async fn metrics_and_health_stay_on_the_data_port() {
    let dir = tempfile::tempdir().unwrap();
    let data = timelake_server::data_app(engine(dir.path()));
    assert_eq!(status(&data, "GET", "/metrics").await, StatusCode::OK);
    assert_eq!(status(&data, "GET", "/health").await, StatusCode::OK);
}

/// The admin plane serves the console and guards the rest; the data plane is
/// not reachable through it.
#[tokio::test]
async fn admin_plane_serves_admin_and_guards_it() {
    let dir = tempfile::tempdir().unwrap();
    let admin = timelake_server::admin_app(engine(dir.path()));

    // The console shell is public (it ships no data — it asks for it).
    assert_eq!(status(&admin, "GET", "/admin/ui").await, StatusCode::OK);

    // A guarded route with no session is refused — present, not 410, not open.
    assert_eq!(
        status(&admin, "GET", "/admin/audit").await,
        StatusCode::UNAUTHORIZED
    );

    // The data plane does not ride the admin listener.
    assert_eq!(
        status(&admin, "GET", "/metrics").await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        status(&admin, "POST", "/write").await,
        StatusCode::NOT_FOUND
    );
}
