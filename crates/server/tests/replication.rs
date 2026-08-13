//! CL-2 ingester replication — the in-process, deterministic half.
//!
//! What is pinned here: a received replica frame is durable but DORMANT
//! (not applied to the buffer, so this node does not double-flush its
//! peer's live rows), recovery replays exactly what was received and makes
//! it queryable, and receiving before the replica WAL is enabled is a clean
//! error rather than a panic. The over-the-network replication, degraded
//! mode, and the SIGKILL-zero-loss property are drilled live
//! (`docs/evidence/cl2-replication-drill.log`).

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

fn engine(dir: &std::path::Path) -> Arc<timelake_server::Engine> {
    timelake_server::Engine::open(
        dir,
        timelake_server::EngineConfig {
            flush_rows: 1_000_000, // tests flush explicitly
            flush_age_secs: u64::MAX,
            wal_max_bytes: u64::MAX,
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

/// COUNT(*) via the public app; a table that was never applied does not
/// exist, which is 0 rows for our purposes.
async fn count(app: &axum::Router, table: &str) -> i64 {
    let req = Request::post("/api/sql")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({"db": "poc", "sql": format!("SELECT COUNT(*) AS n FROM {table}")})
                .to_string(),
        ))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|v| v.get(0).and_then(|r| r.get("n").and_then(|n| n.as_i64())))
        .unwrap_or(0)
}

#[tokio::test]
async fn replica_frames_are_dormant_until_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.enable_replica_wal(&dir.path().join("replica-wal"))
        .unwrap();
    let t = now_ns();

    // Receive 100 frames from the "peer" — durable, but not applied.
    for i in 0..100 {
        let frame = format!("cpu,host=h{i} v={i}i {}\n", t + i);
        eng.replicate_receive("poc", 1, frame.as_bytes()).unwrap();
    }

    let app = timelake_server::app(eng.clone());
    assert_eq!(
        count(&app, "cpu").await,
        0,
        "replica frames must not be queryable before recovery — else the node \
         double-flushes its peer's live rows"
    );

    // Recover: the peer's writes replay and become queryable.
    let n = eng.recover_from_replica().unwrap();
    assert_eq!(n, 100, "recovery replays exactly what was received");
    assert_eq!(
        count(&app, "cpu").await,
        100,
        "every recovered row is queryable — zero acknowledged loss"
    );
}

#[tokio::test]
async fn recovery_survives_a_restart_from_the_durable_replica_wal() {
    // The replica WAL is on disk, so a node that received frames and then
    // restarted can still recover them — the durability that makes the
    // property hold across the recovering node's own bounce.
    let dir = tempfile::tempdir().unwrap();
    let t = now_ns();
    {
        let eng = engine(dir.path());
        eng.enable_replica_wal(&dir.path().join("replica-wal"))
            .unwrap();
        for i in 0..50 {
            let frame = format!("mem,host=h{i} used={i}i {}\n", t + i);
            eng.replicate_receive("poc", 1, frame.as_bytes()).unwrap();
        }
        // Drop without recovering — simulate the node restarting.
    }
    let eng = engine(dir.path());
    eng.enable_replica_wal(&dir.path().join("replica-wal"))
        .unwrap();
    let n = eng.recover_from_replica().unwrap();
    assert_eq!(n, 50, "the replica WAL survived the restart");
    let app = timelake_server::app(eng.clone());
    assert_eq!(count(&app, "mem").await, 50);
}

#[tokio::test]
async fn receiving_before_enable_is_a_clean_error_not_a_panic() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    // No enable_replica_wal — a stray replicate must be refused, not crash.
    let err = eng.replicate_receive("poc", 1, b"cpu v=1i 1").unwrap_err();
    assert!(err.to_string().contains("replica WAL not enabled"), "{err}");
    assert!(eng.recover_from_replica().is_err(), "recovery too");
}

#[tokio::test]
async fn a_lone_node_has_no_replicator_and_writes_normally() {
    // role=all never sets a replicator; the write path is unchanged. Prove
    // it emits no CL-2 metrics and still writes.
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    assert!(
        eng.replication_stats().is_none(),
        "no replicator on a lone node"
    );
    let app = timelake_server::app(eng.clone());
    let t = now_ns();
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v3/write_lp?db=poc")
                .header("content-type", "text/plain")
                .body(Body::from(format!("cpu,host=a v=1i {t}")))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert_eq!(count(&app, "cpu").await, 1);

    let m = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body =
        String::from_utf8(m.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
    assert!(
        !body.contains("timelake_cl2_"),
        "a lone node must not emit CL-2 metrics"
    );
}
