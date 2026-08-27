//! U0b (timelakedb#109) phase C: the `/admin/config` HTTP surface — provenance,
//! set/revert, `?dry_run`, the 409 for a rejected change, and the session
//! guard — over the admin plane with a real SEC-4 session.

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
async fn config_provenance_set_and_revert_over_http() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));
    let session = admin_ready(&app).await;

    // The list carries the revision and every setting with provenance.
    let (code, list) = admin_json(&app, "GET", "/admin/config", None, &session).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(list["revision"], 0);
    assert!(
        list["settings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["key"] == "gc_grace_secs"),
        "{list}"
    );

    // Set an override; it applies and the provenance flips to override.
    let (code, r) = admin_json(
        &app,
        "PUT",
        "/admin/config/gc_grace_secs",
        Some(serde_json::json!({"value": 1500})),
        &session,
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{r}");
    assert_eq!(r["revision"], 1);
    let (code, g) = admin_json(&app, "GET", "/admin/config/gc_grace_secs", None, &session).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(g["effective"]["value"], "1500");
    assert_eq!(g["effective"]["source"], "override");

    // Revert; back to the default.
    let (code, _) = admin_json(
        &app,
        "DELETE",
        "/admin/config/gc_grace_secs",
        None,
        &session,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    let (_, g) = admin_json(&app, "GET", "/admin/config/gc_grace_secs", None, &session).await;
    // The override is gone; the setting falls back to the property layer (the
    // config this node was given — here the default value 900). Source flips
    // from override back to property.
    assert_eq!(g["effective"]["source"], "property");
    assert_eq!(g["effective"]["value"], "900");
}

#[tokio::test]
async fn a_rejected_change_is_409_and_dry_run_previews_without_applying() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));
    let session = admin_ready(&app).await;

    // gc_grace 300 < query_timeout 600 → 409, invariant named.
    let (code, r) = admin_json(
        &app,
        "PUT",
        "/admin/config/gc_grace_secs",
        Some(serde_json::json!({"value": 300})),
        &session,
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT, "{r}");
    assert!(
        r["error"]
            .as_str()
            .unwrap_or("")
            .contains("query_timeout_secs"),
        "{r}"
    );

    // dry_run of a valid change previews but does NOT apply.
    let (code, r) = admin_json(
        &app,
        "PUT",
        "/admin/config/gc_grace_secs?dry_run=1",
        Some(serde_json::json!({"value": 1500})),
        &session,
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{r}");
    assert_eq!(r["dry_run"], true);
    assert_eq!(r["would_apply"]["effective"]["value"], "1500");
    let (_, g) = admin_json(&app, "GET", "/admin/config/gc_grace_secs", None, &session).await;
    // dry_run created no override: the setting still resolves to the property.
    assert_ne!(
        g["effective"]["source"], "override",
        "dry_run must not apply"
    );
    assert_eq!(g["effective"]["value"], "900");

    // an unknown key is 404.
    let (code, _) = admin_json(
        &app,
        "PUT",
        "/admin/config/nope",
        Some(serde_json::json!({"value": 1})),
        &session,
    )
    .await;
    assert_eq!(code, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn config_requires_a_session() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));
    let res = app
        .clone()
        .oneshot(Request::get("/admin/config").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

/// The console (phase D) ships the Configuration screen and wires it to the
/// API. This is a static-content smoke test — it cannot drive the JS in a
/// browser, but it catches the screen being dropped or the endpoint renamed.
#[tokio::test]
async fn the_console_includes_the_configuration_screen() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));
    let res = app
        .oneshot(Request::get("/admin/ui").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(
        html.contains("<h2>Configuration</h2>"),
        "config card present"
    );
    assert!(html.contains("/admin/config"), "config API wired");
    assert!(html.contains("loadSettings("), "config loader present");
}
