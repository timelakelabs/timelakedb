//! R-1 targeted delete, end to end over the HTTP surface.
//!
//! What is pinned here: a tombstone recorded through POST /admin/delete
//! hides matching rows from every query at once — in the live buffer AND in
//! already-flushed files, and from COUNT(*) as much as from SELECT (the
//! aggregate-leak surface). The window is honoured (rows outside it stay),
//! one table's delete never touches another's rows, the endpoint is `admin`
//! only, an empty predicate is refused, and re-issuing a delete is a no-op.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

fn engine(dir: &std::path::Path) -> Arc<timelake_server::Engine> {
    timelake_server::Engine::open(
        dir,
        timelake_server::EngineConfig {
            // Keep everything in the buffer unless a test flushes explicitly,
            // so each test controls which storage layer a row lives in.
            flush_rows: 1_000_000,
            flush_age_secs: u64::MAX,
            wal_max_bytes: u64::MAX,
            // No GC grace: the physical-delete drill reclaims a superseded
            // file within the test rather than after a 15-minute window.
            gc_grace_secs: 0,
            ..Default::default()
        },
    )
    .unwrap()
}

async fn write(app: &axum::Router, lp: &str) -> StatusCode {
    app.clone()
        .oneshot(
            Request::post("/api/v3/write_lp?db=poc")
                .header("content-type", "text/plain")
                .body(Body::from(lp.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

/// COUNT(*) over metrics — the aggregate-leak check: a hidden row must not
/// be counted.
async fn count(app: &axum::Router) -> i64 {
    let (_, v) = sql(app, "SELECT COUNT(*) AS n FROM metrics").await;
    v[0]["n"].as_i64().unwrap()
}

async fn sql(app: &axum::Router, q: &str) -> (StatusCode, serde_json::Value) {
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/sql")
                .header("content-type", "application/json")
                .body(Body::from(
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

/// Log in as the seeded admin and clear the forced first-login rotation.
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

async fn delete(
    app: &axum::Router,
    session: &AdminSession,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    admin_json(app, "POST", "/admin/delete", Some(body), session).await
}

#[tokio::test]
async fn delete_hides_rows_across_buffer_and_files_and_aggregates() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(eng.clone());

    // Two rows that will be FLUSHED to a Parquet file (tags arrive as views
    // on the file path)...
    assert_eq!(
        write(
            &app,
            "metrics,host=web-1 v=1 1000000000\nmetrics,host=web-2 v=1 1000000000",
        )
        .await,
        StatusCode::NO_CONTENT
    );
    eng.flush_all().unwrap();
    // ...and three that stay in the live buffer (dictionary-encoded tags).
    assert_eq!(
        write(
            &app,
            "metrics,host=web-1 v=1 2000000000\n\
             metrics,host=web-1 v=1 5000000000\n\
             metrics,host=web-2 v=1 5000000000",
        )
        .await,
        StatusCode::NO_CONTENT
    );

    // Five rows total: web-1 × 3, web-2 × 2, split across file and buffer.
    assert_eq!(count(&app).await, 5);

    let session = admin_ready(&app).await;
    let (code, v) = delete(
        &app,
        &session,
        serde_json::json!({"db": "poc", "table": "metrics", "tags": {"host": "web-1"}}),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "delete rejected: {v}");
    assert_eq!(v["status"], "recorded");
    assert!(v["tombstone_id"].as_str().unwrap().starts_with("del-"));

    // Every web-1 row is gone — from the file, from the buffer, and from the
    // aggregate. Only the two web-2 rows survive.
    assert_eq!(count(&app).await, 2, "COUNT(*) must not see deleted rows");
    let (_, rows) = sql(&app, "SELECT DISTINCT host FROM metrics").await;
    let hosts: Vec<&str> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["host"].as_str().unwrap())
        .collect();
    assert_eq!(
        hosts,
        vec!["web-2"],
        "only web-2 should remain, got {hosts:?}"
    );
}

#[tokio::test]
async fn delete_honours_the_time_window() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(eng.clone());

    assert_eq!(
        write(
            &app,
            "metrics,host=web-1 v=1 1000000000\n\
             metrics,host=web-1 v=1 2000000000\n\
             metrics,host=web-1 v=1 5000000000\n\
             metrics,host=web-2 v=1 1000000000",
        )
        .await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(count(&app).await, 4);

    let session = admin_ready(&app).await;
    // Delete web-1 only within [0, 3e9]: drops the 1e9 and 2e9 rows, keeps
    // web-1@5e9 (outside the window) and web-2@1e9 (wrong host).
    let (code, v) = delete(
        &app,
        &session,
        serde_json::json!({
            "db": "poc", "table": "metrics",
            "tags": {"host": "web-1"},
            "start_ns": 0, "end_ns": 3000000000i64,
        }),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "delete rejected: {v}");
    assert_eq!(count(&app).await, 2);

    // Confirm the exact survivors.
    let (_, rows) = sql(&app, "SELECT host, \"time\" FROM metrics ORDER BY \"time\"").await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["host"], "web-2"); // @1e9 (wrong host)
    assert_eq!(arr[1]["host"], "web-1"); // @5e9 (outside window)
}

#[tokio::test]
async fn a_delete_on_one_table_never_touches_another() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(eng.clone());

    // Same tag value (host=web-1) present in two different tables.
    assert_eq!(
        write(
            &app,
            "metrics,host=web-1 v=1 1000000000\nevents,host=web-1 v=1 1000000000",
        )
        .await,
        StatusCode::NO_CONTENT
    );

    let session = admin_ready(&app).await;
    let (code, _) = delete(
        &app,
        &session,
        serde_json::json!({"db": "poc", "table": "metrics", "tags": {"host": "web-1"}}),
    )
    .await;
    assert_eq!(code, StatusCode::OK);

    // metrics.web-1 is hidden; events.web-1 is untouched — a tombstone is
    // scoped to its table, never matched by tag value alone.
    assert_eq!(count(&app).await, 0);
    let (_, v) = sql(&app, "SELECT COUNT(*) AS n FROM events").await;
    assert_eq!(v[0]["n"].as_i64().unwrap(), 1, "events must be untouched");
}

#[tokio::test]
async fn the_delete_endpoint_is_guarded_and_validated() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(eng.clone());
    assert_eq!(
        write(&app, "metrics,host=web-1 v=1 1000000000").await,
        StatusCode::NO_CONTENT
    );

    // No session: the admin guard refuses before any delete can be recorded.
    let (code, _) = delete(
        &app,
        &AdminSession::default(),
        serde_json::json!({"db": "poc", "table": "metrics", "tags": {"host": "web-1"}}),
    )
    .await;
    assert!(
        code.is_client_error() && code != StatusCode::OK,
        "unauthenticated delete must be refused, got {code}"
    );
    assert_eq!(count(&app).await, 1, "the refused delete changed nothing");

    let session = admin_ready(&app).await;

    // Empty predicate would erase the whole table — refused.
    let (code, _) = delete(
        &app,
        &session,
        serde_json::json!({"db": "poc", "table": "metrics"}),
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);

    // A table that was never written — refused, not silently recorded.
    let (code, _) = delete(
        &app,
        &session,
        serde_json::json!({"db": "poc", "table": "ghost", "tags": {"host": "web-1"}}),
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);

    assert_eq!(count(&app).await, 1, "no rejected request deleted anything");
}

#[tokio::test]
async fn re_issuing_an_identical_delete_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(eng.clone());
    assert_eq!(
        write(
            &app,
            "metrics,host=web-1 v=1 1000000000\nmetrics,host=web-2 v=1 1000000000",
        )
        .await,
        StatusCode::NO_CONTENT
    );

    let session = admin_ready(&app).await;
    let body = serde_json::json!({"db": "poc", "table": "metrics", "tags": {"host": "web-1"}});

    let (c1, v1) = delete(&app, &session, body.clone()).await;
    assert_eq!(c1, StatusCode::OK);
    assert_eq!(count(&app).await, 1);

    // The same request again: same content-addressed id, and the row count
    // does not move — no second tombstone stacks up.
    let (c2, v2) = delete(&app, &session, body).await;
    assert_eq!(c2, StatusCode::OK);
    assert_eq!(v1["tombstone_id"], v2["tombstone_id"]);
    assert_eq!(count(&app).await, 1);
}

/// Recursively true if any `.parquet` file under `root` contains `needle`.
/// A blunt byte-substring search — enough to prove a distinctive tag value
/// is, or is not, physically present in the settled store.
fn parquet_bytes_contain(root: &std::path::Path, needle: &[u8]) -> bool {
    let Ok(entries) = std::fs::read_dir(root) else {
        return false;
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            if parquet_bytes_contain(&p, needle) {
                return true;
            }
        } else if p.extension().and_then(|s| s.to_str()) == Some("parquet")
            && let Ok(bytes) = std::fs::read(&p)
            && bytes.windows(needle.len()).any(|w| w == needle)
        {
            return true;
        }
    }
    false
}

#[tokio::test]
async fn physical_gc_removes_deleted_rows_from_the_parquet_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(eng.clone());

    // A survivor and a distinctive victim, then settle both into a file.
    assert_eq!(
        write(
            &app,
            "metrics,host=keepme v=1 1000000000\n\
             metrics,host=SECRETDELETEME v=1 2000000000",
        )
        .await,
        StatusCode::NO_CONTENT
    );
    eng.flush_all().unwrap();

    // Premise of the drill: the victim's bytes really are on disk to begin
    // with, so a later absence means the GC removed them.
    assert!(
        parquet_bytes_contain(dir.path(), b"SECRETDELETEME"),
        "precondition: the victim must be in a Parquet file before the delete"
    );

    let session = admin_ready(&app).await;
    let (code, _) = delete(
        &app,
        &session,
        serde_json::json!({"db": "poc", "table": "metrics", "tags": {"host": "SECRETDELETEME"}}),
    )
    .await;
    assert_eq!(code, StatusCode::OK);

    // R-1a: hidden immediately, before any physical pass runs.
    assert_eq!(count(&app).await, 1);

    // R-1b: rewrite the overlapping file, then reclaim the superseded one
    // (grace is 0 in this test, so run_gc deletes it at once).
    assert_eq!(
        eng.apply_tombstones_once().unwrap(),
        1,
        "the one overlapping file must be rewritten"
    );
    eng.run_gc();

    // The victim is gone from the bytes on disk...
    assert!(
        !parquet_bytes_contain(dir.path(), b"SECRETDELETEME"),
        "deleted rows must be physically absent from the settled store"
    );
    // ...the survivor is still physically present and still queryable...
    assert!(parquet_bytes_contain(dir.path(), b"keepme"));
    let (_, rows) = sql(&app, "SELECT host FROM metrics").await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["host"], "keepme");

    // ...and a second pass has nothing left to do (idempotent).
    assert_eq!(eng.apply_tombstones_once().unwrap(), 0);
}
