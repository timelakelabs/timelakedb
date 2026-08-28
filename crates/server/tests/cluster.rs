//! U3 (timelakedb#111): `GET /admin/cluster` aggregates the cluster from the
//! peer list — this node (reachable, self-reported) plus each peer, fetched
//! from its public `/health`. A peer that does not answer is shown
//! **unreachable**, not omitted, and config-revision convergence is reported.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

fn engine(dir: &std::path::Path) -> Arc<timelake_server::Engine> {
    timelake_server::Engine::open(dir, timelake_server::EngineConfig::default()).unwrap()
}

#[derive(Clone, Default)]
struct AdminSession {
    cookie: String,
    csrf: String,
}

async fn login(app: &axum::Router, user: &str, pass: &str) -> (StatusCode, AdminSession) {
    let res = app
        .clone()
        .oneshot(
            Request::post("/admin/session")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"username": user, "password": pass}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let cookie = res
        .headers()
        .get("set-cookie")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .unwrap_or("")
        .to_string();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    let csrf = v["csrf"].as_str().unwrap_or("").to_string();
    (status, AdminSession { cookie, csrf })
}

async fn admin_json(
    app: &axum::Router,
    method: &str,
    path: &str,
    body: Option<serde_json::Value>,
    session: &AdminSession,
) -> (StatusCode, serde_json::Value) {
    let mut req = Request::builder().method(method).uri(path);
    if !session.cookie.is_empty() {
        req = req.header("cookie", &session.cookie);
        req = req.header("x-timelake-csrf", &session.csrf);
    }
    let req = match body {
        Some(b) => req
            .header("content-type", "application/json")
            .body(Body::from(b.to_string()))
            .unwrap(),
        None => req.body(Body::empty()).unwrap(),
    };
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, v)
}

async fn admin_ready(app: &axum::Router) -> AdminSession {
    let (code, seeded) = login(app, "admin", "admin").await;
    assert_eq!(code, StatusCode::OK);
    let (code, _) = admin_json(
        app,
        "POST",
        "/admin/password",
        Some(serde_json::json!({
            "current_password": "admin",
            "new_password": "test console password"
        })),
        &seeded,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    let (code, session) = login(app, "admin", "test console password").await;
    assert_eq!(code, StatusCode::OK);
    session
}

#[tokio::test]
async fn cluster_view_shows_self_and_an_unreachable_peer() {
    let dir = tempfile::tempdir().unwrap();
    // A peer nothing listens for: the connection is refused, so it must show
    // up as unreachable rather than vanish.
    let peers = vec![timelake_server::ClusterPeer {
        id: "ghost".into(),
        role: "ingester".into(),
        data_address: "127.0.0.1:1".into(),
    }];
    let app = timelake_server::admin_app(engine(dir.path()), peers);
    let session = admin_ready(&app).await;

    let (code, v) = admin_json(&app, "GET", "/admin/cluster", None, &session).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    let nodes = v["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 2, "self + the peer: {v}");

    let me = nodes.iter().find(|n| n["self"] == true).unwrap();
    assert_eq!(me["reachable"], true);
    assert_eq!(me["role"], "all");
    assert!(
        me["config_revision"].is_u64(),
        "self reports a revision: {me}"
    );

    let ghost = nodes.iter().find(|n| n["id"] == "ghost").unwrap();
    assert_eq!(
        ghost["reachable"], false,
        "the dead peer is shown, not omitted"
    );
    assert_eq!(ghost["role"], "ingester");

    // Only this node reports a revision, so the cluster is trivially converged.
    assert_eq!(v["config_converged"], true);
}

#[tokio::test]
async fn cluster_requires_a_session() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::admin_app(engine(dir.path()), Vec::new());
    let res = app
        .oneshot(Request::get("/admin/cluster").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}
