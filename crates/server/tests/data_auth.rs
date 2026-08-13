//! SEC-4 phased: data-plane tokens, end to end over the HTTP surface.
//!
//! What is pinned here, in order: the console can issue/list/revoke
//! without ever re-showing a secret; `off` keeps the compatibility
//! contract (credentials are not even read); `optional` refuses bad
//! tokens while serving anonymous callers; `required` locks both doors;
//! scopes separate shipping from reading; grants narrow SEC-2 claims
//! without a no-policy token going blind; revocation is immediate.
//!
//! The three header spellings are exercised against the real router —
//! `Bearer` (Grafana/Tributary), `Token` (Telegraf v2), `Basic`
//! (Telegraf v1, token as password) — because the probe that fixed this
//! design (`docs/evidence/data-auth-client-probe.log`) says these are
//! what stock clients actually send.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

fn engine_with_mode(
    dir: &std::path::Path,
    mode: timelake_auth::DataAuthMode,
) -> Arc<timelake_server::Engine> {
    timelake_server::Engine::open(
        dir,
        timelake_server::EngineConfig {
            flush_rows: 1_000_000,
            flush_age_secs: u64::MAX,
            wal_max_bytes: u64::MAX,
            data_auth: mode,
            ..Default::default()
        },
    )
    .unwrap()
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

/// Minimal standard base64 for the `Basic` spelling — a client would
/// use a library; the test writes it by hand to avoid a dependency.
fn b64(s: &str) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let b = s.as_bytes();
    let mut out = String::new();
    for c in b.chunks(3) {
        let t = [c[0], *c.get(1).unwrap_or(&0), *c.get(2).unwrap_or(&0)];
        let n = ((t[0] as u32) << 16) | ((t[1] as u32) << 8) | t[2] as u32;
        for i in 0..c.len() + 1 {
            out.push(A[((n >> (18 - 6 * i)) & 0x3F) as usize] as char);
        }
        for _ in 0..3 - c.len() {
            out.push('=');
        }
    }
    out
}

/// One line-protocol write, optionally carrying an Authorization value.
async fn write_auth(app: &axum::Router, auth: Option<&str>, lp: &str) -> StatusCode {
    let mut req = Request::post("/api/v3/write_lp?db=poc").header("content-type", "text/plain");
    if let Some(a) = auth {
        req = req.header("authorization", a);
    }
    app.clone()
        .oneshot(req.body(Body::from(lp.to_string())).unwrap())
        .await
        .unwrap()
        .status()
}

/// One /api/sql call: optional Authorization, optional claimed SEC-2
/// authorizations — the two are different headers with different jobs.
async fn sql_auth(
    app: &axum::Router,
    auth: Option<&str>,
    claims: Option<&str>,
    q: &str,
) -> (StatusCode, serde_json::Value) {
    let mut req = Request::post("/api/sql").header("content-type", "application/json");
    if let Some(a) = auth {
        req = req.header("authorization", a);
    }
    if let Some(c) = claims {
        req = req.header("x-timelake-authorizations", c);
    }
    let res = app
        .clone()
        .oneshot(
            req.body(Body::from(
                serde_json::json!({"db": "poc", "sql": q}).to_string(),
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    )
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

/// Issue a token through the console API and return its (secret, id).
async fn issue(
    app: &axum::Router,
    session: &AdminSession,
    body: serde_json::Value,
) -> (String, String) {
    let (code, v) = admin_json(app, "POST", "/admin/tokens", Some(body), session).await;
    assert_eq!(code, StatusCode::OK, "issue failed: {v}");
    (
        v["secret"].as_str().expect("secret shown once").to_string(),
        v["token"]["id"].as_str().expect("token id").to_string(),
    )
}

#[tokio::test]
async fn tokens_issue_list_revoke_and_the_secret_appears_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine_with_mode(
        dir.path(),
        timelake_auth::DataAuthMode::Off,
    ));
    let session = admin_ready(&app).await;

    let (secret, id) = issue(
        &app,
        &session,
        serde_json::json!({"description": "grafana-prod", "scope": "read"}),
    )
    .await;
    assert!(secret.starts_with("tldb_"), "prefixed for secret scanners");

    // The list shows the record and never anything derived from the
    // secret — the console cannot leak a credential by construction.
    let (code, v) = admin_json(&app, "GET", "/admin/tokens", None, &session).await;
    assert_eq!(code, StatusCode::OK);
    let listed = &v["tokens"][0];
    assert_eq!(listed["id"], serde_json::json!(id));
    assert_eq!(listed["description"], serde_json::json!("grafana-prod"));
    assert!(listed.get("hash").is_none(), "digest is not shown");
    assert!(!v.to_string().contains(&secret), "secret is not re-shown");

    // A garbage scope is a refusal, not a default.
    let (code, _) = admin_json(
        &app,
        "POST",
        "/admin/tokens",
        Some(serde_json::json!({"description": "x", "scope": "reed"})),
        &session,
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);

    let (code, _) = admin_json(
        &app,
        "DELETE",
        &format!("/admin/tokens/{id}"),
        None,
        &session,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    let (_, v) = admin_json(&app, "GET", "/admin/tokens", None, &session).await;
    assert_eq!(
        v["tokens"][0]["revoked"],
        serde_json::json!(true),
        "revocation is a tombstone, not a delete — the audit trail keeps the record"
    );
    // Revoking twice is NOT idempotent-OK: the second call says "no".
    let (code, _) = admin_json(
        &app,
        "DELETE",
        &format!("/admin/tokens/{id}"),
        None,
        &session,
    )
    .await;
    assert_eq!(code, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn off_mode_does_not_read_credentials_the_compat_contract() {
    // A Telegraf migrated from InfluxDB still carries its old token.
    // "Existing agents write to TimeLakeDB unmodified" must hold for
    // exactly that config, so off means the header is not examined.
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine_with_mode(
        dir.path(),
        timelake_auth::DataAuthMode::Off,
    ));
    let t = now_ns();
    assert_eq!(
        write_auth(
            &app,
            Some("Token leftover-influx-cloud-token"),
            &format!("m v=1i {t}")
        )
        .await,
        StatusCode::NO_CONTENT
    );
    let (code, rows) = sql_auth(
        &app,
        Some("Bearer garbage"),
        None,
        "SELECT COUNT(*) AS n FROM m",
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(rows[0]["n"], 1);
}

#[tokio::test]
async fn optional_mode_serves_anonymous_refuses_bad_and_accepts_all_three_spellings() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine_with_mode(dir.path(), timelake_auth::DataAuthMode::Optional);
    let app = timelake_server::app(eng);
    let session = admin_ready(&app).await;
    let (secret, _) = issue(
        &app,
        &session,
        serde_json::json!({"description": "shipper", "scope": "write"}),
    )
    .await;
    let t = now_ns();

    // The migration window: anonymous still writes.
    assert_eq!(
        write_auth(&app, None, &format!("m v=1i {t}")).await,
        StatusCode::NO_CONTENT
    );
    // Opted in, a bad token fails loudly on day one.
    assert_eq!(
        write_auth(&app, Some("Bearer tldb_wrong"), &format!("m v=2i {t}")).await,
        StatusCode::UNAUTHORIZED
    );

    // The three spellings stock clients use, against the real router.
    for (i, header) in [
        format!("Bearer {secret}"),                       // Grafana / Tributary
        format!("Token {secret}"),                        // Telegraf influxdb_v2
        format!("Basic {}", b64(&format!("u:{secret}"))), // Telegraf influxdb v1
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(
            write_auth(
                &app,
                Some(header),
                &format!("m v={}i {}", i + 10, t + i as i64 + 1)
            )
            .await,
            StatusCode::NO_CONTENT,
            "spelling {header:?} must authenticate"
        );
    }

    // Scope: the shipper's token must not read the database back.
    let (code, _) = sql_auth(
        &app,
        Some(&format!("Bearer {secret}")),
        None,
        "SELECT COUNT(*) AS n FROM m",
    )
    .await;
    assert_eq!(code, StatusCode::FORBIDDEN);
    // ...while anonymous reads stay open in optional mode.
    let (code, rows) = sql_auth(&app, None, None, "SELECT COUNT(*) AS n FROM m").await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(rows[0]["n"], 4);
}

#[tokio::test]
async fn required_mode_locks_both_doors_and_revocation_is_immediate() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine_with_mode(
        dir.path(),
        timelake_auth::DataAuthMode::Required,
    ));
    let session = admin_ready(&app).await;
    let (secret, id) = issue(
        &app,
        &session,
        serde_json::json!({"description": "agent", "scope": "read_write"}),
    )
    .await;
    let t = now_ns();

    assert_eq!(
        write_auth(&app, None, &format!("m v=1i {t}")).await,
        StatusCode::UNAUTHORIZED,
        "no credential, no write"
    );
    let (code, _) = sql_auth(&app, None, None, "SELECT 1 AS one").await;
    assert_eq!(code, StatusCode::UNAUTHORIZED, "no credential, no read");

    assert_eq!(
        write_auth(
            &app,
            Some(&format!("Bearer {secret}")),
            &format!("m v=1i {t}")
        )
        .await,
        StatusCode::NO_CONTENT
    );
    let (code, rows) = sql_auth(
        &app,
        Some(&format!("Bearer {secret}")),
        None,
        "SELECT COUNT(*) AS n FROM m",
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(rows[0]["n"], 1);

    // Revocation takes effect on the next request, not the next restart.
    let (code, _) = admin_json(
        &app,
        "DELETE",
        &format!("/admin/tokens/{id}"),
        None,
        &session,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(
        write_auth(
            &app,
            Some(&format!("Bearer {secret}")),
            &format!("m v=2i {t}")
        )
        .await,
        StatusCode::UNAUTHORIZED,
        "a revoked token is refused immediately"
    );
}

#[tokio::test]
async fn grants_narrow_sec2_claims_and_no_policy_leaves_them_alone() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine_with_mode(
        dir.path(),
        timelake_auth::DataAuthMode::Optional,
    ));
    let session = admin_ready(&app).await;
    let t = now_ns();

    // The SEC-2 fixture: one row per audience.
    let lp = format!(
        "audit,who=a,_visibility=admin v=1i {t0}\n\
         audit,who=b,_visibility=ops v=1i {t1}\n\
         audit,who=c v=1i {t2}",
        t0 = t - 1000,
        t1 = t - 2000,
        t2 = t - 3000,
    );
    assert_eq!(write_auth(&app, None, &lp).await, StatusCode::NO_CONTENT);

    // Granted only "ops": claiming admin,ops yields ops — the claim the
    // grant does not cover is dropped, so the admin-labeled row stays
    // hidden. Authenticating narrowed; it can never widen.
    let (granted_secret, _) = issue(
        &app,
        &session,
        serde_json::json!({"description": "scoped-reader", "scope": "read",
                           "authorizations": ["ops"]}),
    )
    .await;
    let (code, rows) = sql_auth(
        &app,
        Some(&format!("Bearer {granted_secret}")),
        Some("admin,ops"),
        "SELECT COUNT(*) AS n FROM audit",
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(rows[0]["n"], 2, "granted ops: public + ops, not admin");

    // No grant policy recorded: claims pass through untouched. If this
    // read 1, presenting a token would have made a working client go
    // blind — the opposite of an additive migration.
    let (open_secret, _) = issue(
        &app,
        &session,
        serde_json::json!({"description": "unscoped-reader", "scope": "read"}),
    )
    .await;
    let (_, rows) = sql_auth(
        &app,
        Some(&format!("Bearer {open_secret}")),
        Some("admin,ops"),
        "SELECT COUNT(*) AS n FROM audit",
    )
    .await;
    assert_eq!(rows[0]["n"], 3, "no policy: claims trusted as asserted");

    // And the anonymous caller keeps today's documented behaviour.
    let (_, rows) = sql_auth(
        &app,
        None,
        Some("admin,ops"),
        "SELECT COUNT(*) AS n FROM audit",
    )
    .await;
    assert_eq!(rows[0]["n"], 3, "anonymous claims unchanged — no flag day");
}

#[tokio::test]
async fn database_scoping_confines_a_token_to_its_databases() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine_with_mode(
        dir.path(),
        timelake_auth::DataAuthMode::Required,
    ));
    let session = admin_ready(&app).await;
    let (secret, _) = issue(
        &app,
        &session,
        serde_json::json!({"description": "poc-only", "scope": "read_write",
                           "databases": ["poc"]}),
    )
    .await;
    let t = now_ns();
    let auth = format!("Bearer {secret}");

    assert_eq!(
        write_auth(&app, Some(&auth), &format!("m v=1i {t}")).await,
        StatusCode::NO_CONTENT,
        "its own database"
    );
    let mut req = Request::post("/api/v3/write_lp?db=other")
        .header("content-type", "text/plain")
        .header("authorization", &auth);
    let _ = &mut req;
    let res = app
        .clone()
        .oneshot(req.body(Body::from(format!("m v=1i {t}"))).unwrap())
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        StatusCode::FORBIDDEN,
        "someone else's database — valid credential, wrong scope, 403 not 401"
    );
}

#[tokio::test]
async fn the_split_is_measured_because_the_migration_decision_rests_on_it() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine_with_mode(
        dir.path(),
        timelake_auth::DataAuthMode::Optional,
    ));
    let session = admin_ready(&app).await;
    let (secret, _) = issue(
        &app,
        &session,
        serde_json::json!({"description": "counted", "scope": "write"}),
    )
    .await;
    let t = now_ns();

    write_auth(&app, None, &format!("m v=1i {t}")).await;
    write_auth(
        &app,
        Some(&format!("Bearer {secret}")),
        &format!("m v=2i {}", t + 1),
    )
    .await;
    write_auth(&app, Some("Bearer tldb_bad"), &format!("m v=3i {}", t + 2)).await;

    let res = app
        .clone()
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body =
        String::from_utf8(res.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
    // Each sample line must start at column 0 — a leading space makes it
    // invalid Prometheus exposition, and a format-string continuation
    // that swallows its indentation is exactly how that regresses.
    for needle in [
        "\ntimelake_data_auth_mode 1\n",
        "\ntimelake_data_requests_authenticated_total 1\n",
        "\ntimelake_data_requests_anonymous_total 1\n",
        "\ntimelake_data_requests_rejected_total 1\n",
    ] {
        assert!(
            body.contains(needle),
            "missing or mis-indented {needle:?} in:\n{body}"
        );
    }
}
