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

// ---- P1-1 D1: a slow peer must not become an ingest outage -------------

/// The failure this bounds is a *slow* peer, not a dead one.
///
/// A dead peer trips to degraded on the first refused connection and
/// availability holds — that path was already covered. A slow peer trips
/// nothing: it accepts the connection and simply never answers, so before
/// D1 every write paid the full timeout before its ack. At the reference
/// workload's ~232 events/s that is an ingest outage rather than a hiccup,
/// and it is exactly the shape of the InfluxDB 1.x gaps this design came
/// from (`docs/P1-1_DESIGN.md` §2).
///
/// So this pins the bound itself: a peer that accepts and then stalls costs
/// the configured timeout and no more, and the write still succeeds because
/// availability outranks the second replica.
#[test]
fn a_stalled_peer_costs_the_timeout_and_no_more() {
    use std::io::Read;

    // A listener that accepts and then reads forever without replying.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_thread = stop.clone();
    let handle = std::thread::spawn(move || {
        listener.set_nonblocking(false).expect("blocking listener");
        while !stop_thread.load(std::sync::atomic::Ordering::Relaxed) {
            match listener.accept() {
                Ok((mut sock, _)) => {
                    // Consume the request and never respond.
                    let mut buf = [0u8; 1024];
                    let _ = sock.read(&mut buf);
                    std::thread::sleep(std::time::Duration::from_millis(2_000));
                }
                Err(_) => break,
            }
        }
    });

    let timeout_ms = 250;
    let r = timelake_server::replication::Replicator::new(
        "stalled-peer",
        &addr.to_string(),
        timeout_ms,
    );

    let started = std::time::Instant::now();
    let acked = r.replicate("db", 1, b"cpu,host=a v=1i 1");
    let elapsed = started.elapsed();

    assert!(
        !acked,
        "a peer that never answers must not be reported as durable"
    );
    // The ceiling is the point. Generous upper bound so the assertion is
    // about the bound existing, not about scheduler jitter; it would have
    // been ~5 s before D1, which is what would fail here.
    assert!(
        elapsed < std::time::Duration::from_millis(2_000),
        "a stalled peer cost {elapsed:?}, which is not bounded by the {timeout_ms} ms timeout"
    );
    assert!(
        r.stats()
            .degraded
            .load(std::sync::atomic::Ordering::Relaxed),
        "a stalled peer must raise the degraded gauge, exactly as a dead one does"
    );

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = std::net::TcpStream::connect(addr); // unblock accept()
    let _ = handle.join();
}

/// The default is a deliberate value, not an accident: it must stay far
/// below any plausible healthy round-trip so that slow and dead collapse
/// into one case.
#[test]
fn the_default_replication_timeout_is_sub_second() {
    let d = timelake_server::EngineConfig::default();
    assert_eq!(d.repl_timeout_ms, 250);
    assert!(
        d.repl_timeout_ms < 1_000,
        "a second or more here reintroduces the stall D1 exists to bound"
    );
}

/// FR-1 requires bodies of 10 MB and up. axum's `Bytes` extractor defaults
/// to 2 MB, which broke that contract in two places at once — and the
/// second one was silent.
///
/// A large batch was refused with 413 on `/write`, which is at least loud.
/// The same 413 from a peer's `/internal/v1/replicate` is not: the
/// replicator reads any non-2xx as "peer not durable", so the node dropped
/// to degraded and stopped replicating, while the write itself succeeded
/// locally. "Durable on two nodes" quietly stopped holding for exactly the
/// batches most worth replicating, and the alarm looked unexplained.
///
/// Both routers now take the ceiling from one config value, so they cannot
/// disagree: a frame the internal listener refuses would be a write its
/// peer had already accepted.
#[tokio::test]
async fn a_body_over_two_megabytes_is_accepted_on_both_planes() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());

    let t = now_ns();
    let mut lp = String::with_capacity(3 << 20);
    let mut i = 0i64;
    while lp.len() < (3 << 20) {
        lp.push_str(&format!("cpu,host=h{i} v={i}i {}\n", t + i));
        i += 1;
    }
    assert!(
        lp.len() > (2 << 20),
        "the probe must exceed axum's 2 MiB default"
    );

    // Public data plane.
    let app = timelake_server::app(Arc::clone(&e));
    let res = app
        .oneshot(
            Request::post("/write?db=d")
                .header("content-type", "text/plain")
                .body(Body::from(lp.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        StatusCode::NO_CONTENT,
        "FR-1 requires >=10 MB bodies; a {} byte write was refused",
        lp.len()
    );

    // Intra-cluster plane: the same frame a peer would ship.
    let dir2 = tempfile::tempdir().unwrap();
    let peer = engine(dir2.path());
    peer.enable_replica_wal(&dir2.path().join("replica-wal"))
        .expect("replica wal");
    let internal = timelake_server::internal_router(Arc::clone(&peer));
    let res = internal
        .oneshot(
            Request::post("/internal/v1/replicate")
                .header("x-repl-db", "d")
                .header("x-repl-mult", "1")
                .header("content-type", "application/octet-stream")
                .body(Body::from(lp.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        StatusCode::OK,
        "a frame the peer refuses is a write the primary already acknowledged"
    );
}

/// D2: the read gate protects ingest, and must not strangle it instead.
///
/// A querier unions every ingester's live buffer on each query, so read load
/// on the query tier arrives as work on the ingest tier. The gate bounds
/// that. What it must *not* bound is the peer's write path — throttling
/// `replicate` would be the stall D1 exists to prevent, arrived at from the
/// other side — nor `health`, which has to answer precisely when the node is
/// saturated.
///
/// A ceiling of zero makes the routing deterministic without racing the
/// semaphore: every gated route refuses, every ungated one still answers.
#[tokio::test]
async fn the_read_gate_bounds_queriers_and_never_the_write_path() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = timelake_server::EngineConfig {
        internal_max_concurrent: 0,
        ..Default::default()
    };
    let e = timelake_server::Engine::open(dir.path(), cfg).expect("open");
    e.enable_replica_wal(&dir.path().join("replica-wal"))
        .expect("replica wal");

    let status = |app: axum::Router, req: Request<Body>| async move {
        app.oneshot(req).await.unwrap().status()
    };

    // Gated: the querier's fan-out.
    for path in ["/internal/v1/live", "/internal/v1/snapshot?table=cpu"] {
        let s = status(
            timelake_server::internal_router(Arc::clone(&e)),
            Request::get(path).body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(
            s,
            StatusCode::SERVICE_UNAVAILABLE,
            "{path} must refuse rather than consume ingest capacity"
        );
    }

    // Ungated: the peer's writes and the liveness signal.
    let s = status(
        timelake_server::internal_router(Arc::clone(&e)),
        Request::post("/internal/v1/replicate")
            .header("x-repl-db", "d")
            .header("x-repl-mult", "1")
            .body(Body::from("cpu,host=a v=1i 1"))
            .unwrap(),
    )
    .await;
    assert_eq!(
        s,
        StatusCode::OK,
        "throttling a peer's writes is the stall D1 exists to prevent"
    );

    let s = status(
        timelake_server::internal_router(Arc::clone(&e)),
        Request::get("/internal/v1/health")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        s,
        StatusCode::OK,
        "health must answer while saturated — that is when it carries information"
    );

    // Refusals are visible, not merely logged.
    assert!(
        e.cl3_reads_refused() >= 2,
        "a refusal the operator cannot see is indistinguishable from a broken peer"
    );
}
