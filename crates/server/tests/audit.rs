//! P1-2 audit trail (SR-6), end to end over the HTTP surface.
//!
//! What is pinned here: every admin mutation writes exactly one attributable,
//! hash-chained record; the chain verifies through `GET /admin/audit?verify=1`;
//! a denied mutation is recorded too (with `outcome: "denied"`); reading the
//! log is itself audited; and `/metrics` exposes the record count. Tamper
//! detection over a corrupted file is a crate-level unit test — it cannot be
//! reached through the HTTP surface, which is the point.
//!
//! Since #163 it also pins the session half: login, logout and refused login
//! are recorded, every record a session causes carries that session's id, and
//! a bearer logout really ends the session it says it ended.

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

/// The raw session token out of the Set-Cookie, for the bearer path.
fn bearer_of(session: &AdminSession) -> String {
    session
        .cookie
        .split_once('=')
        .map(|(_, v)| v.to_string())
        .unwrap_or_default()
}

/// Every audit record, read with a live session.
async fn audit_records(app: &axum::Router, session: &AdminSession) -> Vec<serde_json::Value> {
    let (code, v) = admin_json(app, "GET", "/admin/audit?limit=1000", None, session).await;
    assert_eq!(code, StatusCode::OK, "reading the audit log failed: {v}");
    v["records"].as_array().cloned().unwrap_or_default()
}

fn only<'a>(
    records: &'a [serde_json::Value],
    action: &str,
    principal: &str,
) -> Vec<&'a serde_json::Value> {
    records
        .iter()
        .filter(|r| r["action"] == action && r["principal"] == principal)
        .collect()
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
            Some(serde_json::json!({"db": "poc", "table": "pipeline_events", "duration": dur})),
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
        // The target names the SCOPE, not just the table. A trail that
        // recorded only "pipeline_events" could not distinguish expiring
        // one database's table from expiring that table everywhere — and
        // those are very different acts to have to account for later.
        assert_eq!(r["target"], "poc.pipeline_events");
        assert_eq!(r["outcome"], "ok");
        assert!(r["hash"].as_str().unwrap().starts_with("sha256:"));
    }
    // The shrink's `before` is the 30d it replaced; its `after` is 10d.
    let shrink = &records[1];
    assert_eq!(shrink["before"]["seconds"], 30 * 86_400);
    assert_eq!(shrink["after"]["seconds"], 10 * 86_400);
    assert_eq!(
        records[0]["before"],
        serde_json::Value::Null,
        "first had none"
    );

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

    let (code, v) = admin_json(
        &app,
        "GET",
        "/admin/audit?action=data.delete",
        None,
        &session,
    )
    .await;
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
    let (code, v) = admin_json(
        &app,
        "GET",
        "/admin/audit?action=audit.read",
        None,
        &session,
    )
    .await;
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
/// #163: the whole point of a session id. Log in, change something, log out —
/// and the three records say so as one story rather than as three unrelated
/// lines that happen to name the same username.
#[tokio::test]
async fn a_session_brackets_the_mutations_it_made() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));
    let working = admin_ready(&app).await;

    let (code, _) = admin_json(
        &app,
        "PUT",
        "/admin/retention",
        Some(serde_json::json!({"db": "poc", "table": "pipeline_events", "duration": "30d"})),
        &working,
    )
    .await;
    assert_eq!(code, StatusCode::OK);

    let (code, _) = admin_json(&app, "DELETE", "/admin/session", None, &working).await;
    assert_eq!(code, StatusCode::OK);

    // That session is gone, so read the trail from a fresh one. Its records
    // get their own id, which is also what makes the assertion below mean
    // something: "same id" has to be able to be false.
    let reader = login(&app, "admin", "test console password").await.1;
    let records = audit_records(&app, &reader).await;

    let logouts = only(&records, "session.logout", "admin");
    assert_eq!(logouts.len(), 1, "exactly one logout: {records:#?}");
    let sid = logouts[0]["session"].as_str().unwrap().to_string();
    assert!(!sid.is_empty(), "the logout record must carry a session id");

    let with_sid: Vec<&str> = records
        .iter()
        .filter(|r| r["session"] == serde_json::json!(sid))
        .map(|r| r["action"].as_str().unwrap())
        .collect();
    assert!(
        with_sid.contains(&"session.login")
            && with_sid.contains(&"retention.set")
            && with_sid.contains(&"session.logout"),
        "login, the mutation and logout must share one session id; got {with_sid:?}"
    );

    // And the filter that makes the id worth carrying: one request, one
    // session, the whole story in order.
    let (code, v) = admin_json(
        &app,
        "GET",
        &format!("/admin/audit?session={sid}"),
        None,
        &reader,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    let story: Vec<&str> = v["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["action"].as_str().unwrap())
        .collect();
    assert_eq!(
        story,
        vec!["session.login", "retention.set", "session.logout"],
        "?session= must return that session and nothing else, in order"
    );

    // And the reader is a different session, so the id actually discriminates.
    let reads = only(&records, "session.login", "admin");
    let ids: std::collections::HashSet<&str> =
        reads.iter().filter_map(|r| r["session"].as_str()).collect();
    assert!(
        ids.len() >= 2,
        "separate logins must get separate ids, got {ids:?}"
    );

    // The session id is NOT the session token. A trail any viewer can read
    // must not hand out a live credential.
    let token = bearer_of(&reader);
    assert!(!token.is_empty());
    let dump = serde_json::to_string(&records).unwrap();
    assert!(
        !dump.contains(&token),
        "a session token leaked into the audit trail"
    );
}

/// #163: a password-guessing run used to leave one counter and no record. The
/// record has to say what was tried and from where, or the chain is an
/// activity counter rather than an audit trail.
#[tokio::test]
async fn refused_logins_are_recorded_with_what_was_attempted() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));
    let reader = admin_ready(&app).await;

    // Four wrong passwords are refused one at a time; the fifth trips the
    // backoff. Both refusals are audited, and they are audited differently:
    // "still guessing" and "guessed wrong once" are not the same fact.
    for _ in 0..4 {
        let (code, _) = login(&app, "mallory", "hunter2").await;
        assert_eq!(code, StatusCode::UNAUTHORIZED);
    }
    let (code, _) = login(&app, "mallory", "hunter2").await;
    assert_eq!(code, StatusCode::TOO_MANY_REQUESTS, "backoff must engage");

    let (code, v) = admin_json(
        &app,
        "GET",
        "/admin/audit?action=session.login&principal=mallory",
        None,
        &reader,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    let records = v["records"].as_array().unwrap();
    assert_eq!(records.len(), 5, "every attempt recorded: {records:#?}");
    for r in records {
        assert_eq!(r["outcome"], "denied");
        assert_eq!(r["role"], "-", "a refused login never got a role");
        assert_eq!(
            r["session"],
            serde_json::Value::Null,
            "a refused login opened no session"
        );
    }
    assert_eq!(records[0]["after"]["reason"], "invalid_credentials");
    assert_eq!(records[4]["after"]["reason"], "rate_limited");

    // A successful login is in there too, and it is distinguishable.
    let (code, v) = admin_json(
        &app,
        "GET",
        "/admin/audit?action=session.login&principal=admin",
        None,
        &reader,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    let ok: Vec<&serde_json::Value> = v["records"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["outcome"] == "ok")
        .collect();
    assert!(!ok.is_empty(), "the admin logins must be recorded as ok");
    assert_eq!(ok[0]["role"], "admin");

    // The sink is healthy, so nothing was dropped on the best-effort path.
    let m = metrics(&app).await;
    assert_eq!(
        metric(&m, "timelake_audit_unrecorded_total"),
        Some(0.0),
        "no holes in the trail on a healthy node"
    );
}

/// A username is whatever an unauthenticated caller typed, and it lands in the
/// trail verbatim. Cap it, or a guessing run gets to choose how big the audit
/// log grows.
#[tokio::test]
async fn an_enormous_username_is_truncated_in_the_record() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));
    let reader = admin_ready(&app).await;

    let huge = "z".repeat(50_000);
    let (code, _) = login(&app, &huge, "nope").await;
    assert_eq!(code, StatusCode::UNAUTHORIZED);

    let records = audit_records(&app, &reader).await;
    let denied: Vec<&serde_json::Value> = records
        .iter()
        .filter(|r| r["action"] == "session.login" && r["outcome"] == "denied")
        .collect();
    assert_eq!(denied.len(), 1);
    // 128 kept, then an ellipsis saying it was cut. Exact, not "roughly":
    // a cap that drifts is a cap nobody can reason about from the record.
    let p = denied[0]["principal"].as_str().unwrap();
    assert_eq!(
        p,
        format!("{}...", "z".repeat(128)),
        "principal must be capped"
    );
}

/// Found while auditing logout: `DELETE /admin/session` read the cookie only,
/// so a bearer client — which is the automation path `admin_guard` documents —
/// got `200 logged out` and kept a live session until it expired on its own.
#[tokio::test]
async fn a_bearer_logout_actually_ends_the_session() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));
    let session = admin_ready(&app).await;
    let token = bearer_of(&session);

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/admin/session")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // The claim the 200 made, checked.
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/session")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        StatusCode::UNAUTHORIZED,
        "the token still works after logging out"
    );
}
