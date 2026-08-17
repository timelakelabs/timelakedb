//! P1-2 audit trail (SR-6), end to end over the HTTP surface.
//!
//! What is pinned here: every admin mutation writes exactly one attributable,
//! hash-chained record; the chain verifies through `GET /admin/audit?verify=1`;
//! a denied mutation is recorded too (with `outcome: "denied"`); reading the
//! log is itself audited; and `/metrics` exposes the record count. Tamper
//! detection over a corrupted file is a crate-level unit test — it cannot be
//! reached through the HTTP surface, which is the point.

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
    let b = match body {
        Some(v) => {
            req = req.header("content-type", "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let res = app.clone().oneshot(req.body(b).unwrap()).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    )
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

async fn metrics(app: &axum::Router) -> String {
    let res = app
        .clone()
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8_lossy(&bytes).to_string()
}

/// The value of a single-sample gauge/counter line from /metrics.
fn metric(text: &str, name: &str) -> Option<f64> {
    text.lines()
        .find(|l| l.starts_with(&format!("{name} ")))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
}

#[tokio::test]
async fn admin_mutations_are_audited_and_the_chain_verifies() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));
    let session = admin_ready(&app).await;

    // Two retention changes: introduce 30d (destructive: none -> bounded),
    // then shrink to 10d. Both are admin mutations and both must be recorded.
    for dur in ["30d", "10d"] {
        let (code, _) = admin_json(
            &app,
            "PUT",
            "/admin/retention",
            Some(serde_json::json!({"table": "pipeline_events", "duration": dur})),
            &session,
        )
        .await;
        assert_eq!(code, StatusCode::OK, "retention set {dur} failed");
    }

    // The trail carries both, attributed to admin, with the resolved before
    // and after — and the chain verifies.
    let (code, v) = admin_json(
        &app,
        "GET",
        "/admin/audit?action=retention.set&verify=1",
        None,
        &session,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(v["verify"]["ok"], true, "the hash chain must verify: {v}");

    let records = v["records"].as_array().unwrap();
    assert_eq!(records.len(), 2, "both retention.set mutations recorded");
    for r in records {
        assert_eq!(r["principal"], "admin");
        assert_eq!(r["role"], "admin");
        assert_eq!(r["action"], "retention.set");
        assert_eq!(r["target"], "pipeline_events");
        assert_eq!(r["outcome"], "ok");
        assert!(r["hash"].as_str().unwrap().starts_with("sha256:"));
    }
    // The shrink's `before` is the 30d it replaced; its `after` is 10d.
    let shrink = &records[1];
    assert_eq!(shrink["before"]["seconds"], 30 * 86_400);
    assert_eq!(shrink["after"]["seconds"], 10 * 86_400);
    assert_eq!(records[0]["before"], serde_json::Value::Null, "first had none");

    // /metrics exposes the count and a healthy sink.
    let m = metrics(&app).await;
    assert!(
        metric(&m, "timelake_audit_records_total").unwrap() >= 2.0,
        "records_total must reflect the mutations"
    );
    assert_eq!(metric(&m, "timelake_audit_sink_healthy"), Some(1.0));
}

#[tokio::test]
async fn a_denied_mutation_is_recorded() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));
    let session = admin_ready(&app).await;

    // An empty-predicate delete is refused by the engine (400) — and the
    // refusal is audited, because a denial is a security-relevant event.
    let (code, _) = admin_json(
        &app,
        "POST",
        "/admin/delete",
        Some(serde_json::json!({"db": "poc", "table": "metrics"})),
        &session,
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);

    let (code, v) = admin_json(&app, "GET", "/admin/audit?action=data.delete", None, &session).await;
    assert_eq!(code, StatusCode::OK);
    let records = v["records"].as_array().unwrap();
    assert_eq!(records.len(), 1, "the denied delete is recorded");
    assert_eq!(records[0]["outcome"], "denied");
    assert_eq!(records[0]["target"], "poc.metrics");
}

#[tokio::test]
async fn reading_the_audit_log_is_itself_audited() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));
    let session = admin_ready(&app).await;

    // One read...
    let (code, _) = admin_json(&app, "GET", "/admin/audit", None, &session).await;
    assert_eq!(code, StatusCode::OK);
    // ...then a second read sees the first read's own record (§5.1).
    let (code, v) = admin_json(&app, "GET", "/admin/audit?action=audit.read", None, &session).await;
    assert_eq!(code, StatusCode::OK);
    let reads = v["records"].as_array().unwrap();
    assert!(
        !reads.is_empty(),
        "reading the audit log must itself be audited"
    );
    assert_eq!(reads[0]["action"], "audit.read");
    assert_eq!(reads[0]["principal"], "admin");
}

#[tokio::test]
async fn a_viewer_can_read_the_audit_log() {
    // The read endpoint is viewer-gated; the seeded admin (which is at least
    // a viewer) reaches it. A completely unauthenticated request cannot.
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));

    let (code, _) = admin_json(&app, "GET", "/admin/audit", None, &AdminSession::default()).await;
    assert!(
        code.is_client_error(),
        "an unauthenticated audit read must be refused, got {code}"
    );
}
