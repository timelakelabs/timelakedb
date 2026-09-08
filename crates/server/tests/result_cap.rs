//! #161: the answer has a size limit, and it refuses rather than truncates.
//!
//! The memory pool bounds what a PLAN costs. Nothing bounded the answer, so
//! `SELECT *` over a large table was assembled in full before anything could
//! look at how big it was — and the RR-2 deadline never fired, because
//! `collect()` was succeeding the whole time. Promise 2 in the README ("no
//! query can kill the server") was true of the plan and false of the result.
//!
//! The cap is deliberately not a `LIMIT`. Truncating would answer a
//! different question than the one asked and return it with a 200, which a
//! Flight client paginating on its own could not detect.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

const CAP: u64 = 50;

fn engine(dir: &std::path::Path, max_result_rows: u64) -> Arc<timelake_server::Engine> {
    timelake_server::Engine::open(
        dir,
        timelake_server::EngineConfig {
            flush_rows: 1_000_000, // stay in the buffer; the cap is not about storage
            flush_age_secs: u64::MAX,
            wal_max_bytes: u64::MAX,
            max_result_rows,
            ..Default::default()
        },
    )
    .unwrap()
}

async fn write_rows(app: &axum::Router, n: u64) {
    let mut lp = String::new();
    for i in 0..n {
        lp.push_str(&format!(
            "cap,host=h{i} v={i}i {}\n",
            1_757_000_000_000_000_000u64 + i
        ));
    }
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v3/write_lp?db=poc&precision=ns")
                .header("content-type", "text/plain")
                .body(Body::from(lp))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT, "seed write");
}

async fn metrics(app: &axum::Router) -> String {
    let res = app
        .clone()
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8_lossy(&bytes).into_owned()
}

async fn sql(app: &axum::Router, q: &str) -> (StatusCode, String) {
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
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// Exactly at the cap is fine. An off-by-one here would make the limit
/// arbitrary in a way nobody could reason about from the config value.
#[tokio::test]
async fn a_result_at_the_cap_is_served() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path(), CAP));
    write_rows(&app, CAP).await;

    let (status, body) = sql(&app, "SELECT * FROM cap").await;
    assert_eq!(status, StatusCode::OK, "at the cap must be served: {body}");
    let rows: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(rows.as_array().map(|a| a.len()), Some(CAP as usize));
}

/// One row over, and it is an error rather than a short answer.
#[tokio::test]
async fn one_row_over_the_cap_is_refused_not_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path(), CAP));
    write_rows(&app, CAP + 1).await;

    let (status, body) = sql(&app, "SELECT * FROM cap").await;
    assert!(
        status.is_client_error(),
        "over the cap must be a client error, got {status}: {body}"
    );
    // The message has to carry the cap. An operator seeing this needs to know
    // which knob to turn without reading the source.
    assert!(
        body.contains("max_result_rows") && body.contains(&CAP.to_string()),
        "the refusal must name the cap and its value: {body}"
    );
    // And it must NOT look like a successful short answer.
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
    assert!(
        parsed.as_array().is_none(),
        "a refusal must not deserialize as a row array — that is a truncated \
         answer wearing a success: {body}"
    );
}

/// An aggregate over more rows than the cap is fine: the cap is about the
/// ANSWER, not the scan. This is the case `LIMIT` injection would have got
/// wrong — a GROUP BY producing one row still reads everything.
#[tokio::test]
async fn an_aggregate_over_more_rows_than_the_cap_still_works() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path(), CAP));
    write_rows(&app, CAP * 4).await;

    let (status, body) = sql(&app, "SELECT COUNT(*) AS n FROM cap").await;
    assert_eq!(status, StatusCode::OK, "aggregate must be served: {body}");
    assert!(
        body.contains(&(CAP * 4).to_string()),
        "the aggregate must see every row, not the first {CAP}: {body}"
    );
}

/// The refusal is counted, and counted apart from the read-only guard's.
/// Both are refusals; one is a client asking for something never permitted
/// and the other a legitimate query whose answer is too big, and an operator
/// needs to tell them apart.
#[tokio::test]
async fn an_oversized_result_moves_its_own_counter() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path(), CAP));
    write_rows(&app, CAP + 1).await;

    let before = metrics(&app).await;
    assert!(
        before.contains("timelake_query_result_rows_refused_total 0"),
        "counter should start at zero: {before}"
    );
    let _ = sql(&app, "SELECT * FROM cap").await;
    let after = metrics(&app).await;
    assert!(
        after.contains("timelake_query_result_rows_refused_total 1"),
        "the refusal must be counted: {after}"
    );
}
