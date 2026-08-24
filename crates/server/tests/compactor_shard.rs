//! C2 phase 5b: partition ownership in `compact_once`. A compactor merges
//! only the partitions it owns, so N compactors divide the work instead of
//! racing every one of it. Correctness is the commit fence's job (tested
//! elsewhere); this pins the *efficiency* layer — that the ownership filter
//! actually restricts which partitions a sharded compactor touches.

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
            gc_grace_secs: 0,
            ..Default::default()
        },
    )
    .unwrap()
}

async fn write(app: &axum::Router, lp: &str) -> StatusCode {
    app.clone()
        .oneshot(
            Request::post("/api/v3/write_lp?db=poc&precision=ns")
                .header("content-type", "text/plain")
                .body(Body::from(lp.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

async fn row_count(app: &axum::Router, table: &str) -> i64 {
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/sql")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"db": "poc", "sql": format!("SELECT COUNT(*) AS n FROM {table}")})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    v[0]["n"].as_i64().unwrap()
}

/// Seed `table` with two overlapping files carrying the same three rows, so
/// the partition has duplicate primary keys and trips the overlap trigger:
/// `COUNT(*)` reads 6 until a compaction collapses it to 3.
async fn seed_duplicates(eng: &timelake_server::Engine, app: &axum::Router, table: &str) {
    let base = 1_700_000_000_000_000_000i64; // one partition (same hour)
    let lp = format!(
        "{table},host=h value=1 {}\n{table},host=h value=2 {}\n{table},host=h value=3 {}",
        base,
        base + 1_000_000_000,
        base + 2_000_000_000
    );
    assert_eq!(write(app, &lp).await, StatusCode::NO_CONTENT);
    eng.flush_all().unwrap();
    assert_eq!(write(app, &lp).await, StatusCode::NO_CONTENT); // the duplicate file
    eng.flush_all().unwrap();
}

/// Seed one engine, shard it, compact once, and report whether the table's
/// duplicates collapsed (i.e. this compactor owned and merged the partition).
async fn collapsed_under_shard(ordinal: usize, count: usize) -> bool {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(Arc::clone(&eng));
    seed_duplicates(&eng, &app, "t").await;
    assert_eq!(
        row_count(&app, "t").await,
        6,
        "seed should read as 6 before compaction"
    );
    eng.set_compactor_shard(ordinal, count);
    eng.compact_once().unwrap();
    row_count(&app, "t").await == 3
}

#[tokio::test]
async fn a_sharded_compactor_only_merges_partitions_it_owns() {
    // With two compactors, the partition is owned by exactly one of them:
    // one ordinal collapses the duplicates, the other leaves them. That is
    // the whole point — disjoint ownership, so no two compactors race the
    // same merge.
    let by_0 = collapsed_under_shard(0, 2).await;
    let by_1 = collapsed_under_shard(1, 2).await;
    assert!(
        by_0 ^ by_1,
        "exactly one of two compactors must own the partition (0:{by_0}, 1:{by_1})"
    );
}

#[tokio::test]
async fn an_unsharded_compactor_owns_everything() {
    // The `all` node and a lone compactor set count<=1 (or never set a shard
    // at all): they own every partition, so the merge happens as it always
    // did. This is what keeps single-node behaviour unchanged.
    assert!(
        collapsed_under_shard(0, 1).await,
        "count=1 must own everything"
    );

    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(Arc::clone(&eng));
    seed_duplicates(&eng, &app, "t").await;
    // No set_compactor_shard call at all — the default is unsharded.
    eng.compact_once().unwrap();
    assert_eq!(
        row_count(&app, "t").await,
        3,
        "an un-set shard must own everything"
    );
}
