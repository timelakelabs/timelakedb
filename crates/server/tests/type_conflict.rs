//! timelakedb#98: a flush must not be able to change a column's type.
//!
//! A field's established type outlives the buffer that a flush drains. Before
//! the fix the write path only consulted the live buffer, so once a flush had
//! reset it a value that conflicts with the column's real type was accepted
//! (204) and corrupted the table at read: a string retyped the whole column
//! (the float `1.5` came back as the string `"1.5"`), an int truncated it
//! (`1.5` came back as `1`). The identical write with no flush in between was
//! correctly rejected. All three faces — reject, coerce, and survive a
//! restart — are pinned here.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

fn engine(dir: &std::path::Path) -> Arc<timelake_server::Engine> {
    timelake_server::Engine::open(
        dir,
        timelake_server::EngineConfig {
            // Flush only when a test asks, so each test decides which storage
            // layer (buffer vs file) a row lives in.
            flush_rows: 1_000_000,
            flush_age_secs: u64::MAX,
            wal_max_bytes: u64::MAX,
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

/// The float column of `v`, in row order, as f64 — tolerant of whether the
/// wire spells `2.0` as `2` or `2.0`, and it would catch a `1.5` truncated to
/// `1.0`.
fn floats(v: &serde_json::Value) -> Vec<f64> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|r| r["v"].as_f64().unwrap())
        .collect()
}

#[tokio::test]
async fn a_string_into_a_flushed_float_column_is_refused_not_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(eng.clone());

    // Establish `v` as a float, then flush it OUT of the buffer so only the
    // registry — not the live buffer — remembers the type.
    assert_eq!(
        write(&app, "m,k=a v=1.5 1000000000").await,
        StatusCode::NO_CONTENT
    );
    eng.flush_all().unwrap();

    let (st, v) = sql(&app, "SELECT v FROM m").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(floats(&v), vec![1.5]);

    // A string cannot live in a float column: 400, and it must not reach the
    // WAL. Before the fix this was a 204.
    assert_eq!(
        write(&app, "m,k=a v=\"hot\" 2000000000").await,
        StatusCode::BAD_REQUEST,
        "a string into a since-flushed float column must be rejected"
    );

    // The table is untouched: still readable, still the one float.
    let (st, v) = sql(&app, "SELECT v FROM m").await;
    assert_eq!(
        st,
        StatusCode::OK,
        "the rejected write must not break reads"
    );
    assert_eq!(floats(&v), vec![1.5]);
}

#[tokio::test]
async fn an_int_into_a_flushed_float_column_coerces_and_does_not_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(eng.clone());

    assert_eq!(
        write(&app, "m,k=a v=1.5 1000000000").await,
        StatusCode::NO_CONTENT
    );
    eng.flush_all().unwrap();

    // An int IS accepted into a float column — the widening the InfluxDB
    // importer (#78) relies on. But it has to coerce into the established
    // float column, not create a fresh int column that unions the file's
    // `1.5` down to `1`.
    assert_eq!(
        write(&app, "m,k=a v=2i 2000000000").await,
        StatusCode::NO_CONTENT
    );

    let (st, v) = sql(&app, "SELECT v FROM m ORDER BY time").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        floats(&v),
        vec![1.5, 2.0],
        "1.5 must keep its fraction and 2 must widen to 2.0"
    );
}

#[tokio::test]
async fn a_flush_type_conflict_cannot_be_resurrected_by_wal_replay() {
    let dir = tempfile::tempdir().unwrap();
    {
        let eng = engine(dir.path());
        let app = timelake_server::app(eng.clone());
        // Float on disk (in a file, so the type is in the catalog).
        assert_eq!(
            write(&app, "m,k=a v=1.5 1000000000").await,
            StatusCode::NO_CONTENT
        );
        eng.flush_all().unwrap();
        // The conflicting string is refused at the door — it never reaches
        // the WAL, so there is nothing to resurrect.
        assert_eq!(
            write(&app, "m,k=a v=\"hot\" 2000000000").await,
            StatusCode::BAD_REQUEST
        );
        // A widening int IS accepted and stays only in the WAL (no flush), so
        // replay has to re-apply it with the established type, not the value's.
        assert_eq!(
            write(&app, "m,k=a v=2i 3000000000").await,
            StatusCode::NO_CONTENT
        );
    }

    // Reopen: catalog load + WAL replay. The registry is established from the
    // file BEFORE replay, so the replayed `v=2i` re-creates the column as a
    // float and unions cleanly with the file's `1.5`.
    let eng = engine(dir.path());
    let app = timelake_server::app(eng.clone());
    let (st, v) = sql(&app, "SELECT v FROM m ORDER BY time").await;
    assert_eq!(
        st,
        StatusCode::OK,
        "the table must be readable after replay"
    );
    let vals = floats(&v);
    // A replayed already-flushed row can read doubled until compaction, so
    // don't pin the count — pin that nothing is corrupted or truncated.
    assert!(
        vals.contains(&1.5),
        "1.5 must survive replay uncorrupted, got {vals:?}"
    );
    assert!(
        vals.contains(&2.0),
        "the replayed int must read back as the float 2.0, got {vals:?}"
    );
    assert!(
        vals.iter().all(|x| *x == 1.5 || *x == 2.0),
        "no garbled or truncated values after replay: {vals:?}"
    );
}
