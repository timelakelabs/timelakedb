//! #165: a full data volume is a named condition, not a generic 500.
//!
//! Before this, ENOSPC on the WAL became `WriteError::Internal`, which is a
//! 500 counted as `reason="internal"`. Nothing on `/metrics` named the disk,
//! so `docs/ALERTING.md` had no rule to write and the operator found out
//! from the client's errors. On a small PVC it is also the *only* signal,
//! because the 2 GiB WAL cap never trips on a 1 GiB volume and the
//! backpressure path designed to say "slow down" stays silent.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

const LP: &str = "disk,host=a v=1i 1757000000000000000";

fn engine(dir: &std::path::Path) -> Arc<timelake_server::Engine> {
    timelake_server::Engine::open(dir, timelake_server::EngineConfig::default()).unwrap()
}

async fn write(app: &axum::Router, lp: &str) -> (StatusCode, String, Option<String>) {
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v3/write_lp?db=poc&precision=ns")
                .header("content-type", "text/plain")
                .body(Body::from(lp.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let retry = res
        .headers()
        .get("retry-after")
        .map(|v| v.to_str().unwrap().to_string());
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned(), retry)
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

/// 507, with a Retry-After, and a message that says what to do about it.
///
/// Not a 500: nothing about the request is wrong and the engine is not
/// broken. Not a 400 either, because retrying the identical bytes is the
/// correct client behaviour once space exists.
#[tokio::test]
async fn a_full_volume_is_a_507_with_a_retry_after() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let app = timelake_server::app(e.clone());

    e.fail_next_wal_append(std::io::Error::new(
        std::io::ErrorKind::StorageFull,
        "No space left on device (os error 28)",
    ));
    let (status, body, retry) = write(&app, LP).await;

    assert_eq!(
        status,
        StatusCode::INSUFFICIENT_STORAGE,
        "ENOSPC must be 507, got {status}: {body}"
    );
    assert_eq!(retry.as_deref(), Some("5"), "507 must carry a Retry-After");
    assert!(
        body.contains("no space left"),
        "the refusal must say what is wrong: {body}"
    );
    assert!(
        body.contains("NOT applied"),
        "the refusal must say whether the rows landed — that is the only \
         thing the client actually needs from it: {body}"
    );
}

/// The reason label is the point of the ticket. `internal` means "this node
/// is broken, page someone"; `disk_full` means "the volume is full, extend
/// it". Same 5xx, completely different response, so they cannot share a
/// series.
#[tokio::test]
async fn the_refusal_is_counted_as_disk_full_and_not_as_internal() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let app = timelake_server::app(e.clone());

    let before = metrics(&app).await;
    assert!(
        before.contains(r#"timelake_write_rejected_total{reason="disk_full"} 0"#),
        "the series must exist before it moves, or an alert on it never \
         fires: {before}"
    );

    e.fail_next_wal_append(std::io::Error::new(
        std::io::ErrorKind::StorageFull,
        "No space left on device (os error 28)",
    ));
    let _ = write(&app, LP).await;

    let after = metrics(&app).await;
    assert!(
        after.contains(r#"timelake_write_rejected_total{reason="disk_full"} 1"#),
        "disk_full must move: {after}"
    );
    assert!(
        after.contains(r#"timelake_write_rejected_total{reason="internal"} 0"#),
        "internal must NOT move — that is the misdiagnosis this fixes: {after}"
    );
}

/// A refused write must leave nothing behind. The WAL append is what makes a
/// write durable, so failing it has to mean the rows were never applied —
/// otherwise a client that retries on the 507 double-writes, and a client
/// that gives up leaves rows that survive in the buffer but not a restart.
#[tokio::test]
async fn a_refused_write_is_not_applied() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let app = timelake_server::app(e.clone());

    e.fail_next_wal_append(std::io::Error::new(
        std::io::ErrorKind::StorageFull,
        "No space left on device (os error 28)",
    ));
    let (status, _, _) = write(&app, LP).await;
    assert_eq!(status, StatusCode::INSUFFICIENT_STORAGE);

    let (status, body) = sql(&app, "SELECT COUNT(*) AS n FROM disk").await;
    assert!(
        status.is_client_error() || body.contains("0"),
        "the table must not exist, or must be empty; got {status}: {body}"
    );
}

/// The fault is one-shot, and so is the condition: freeing space brings the
/// node straight back. Nothing latches, and there is no restart in the
/// recovery path.
#[tokio::test]
async fn the_node_writes_again_once_the_condition_clears() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let app = timelake_server::app(e.clone());

    e.fail_next_wal_append(std::io::Error::new(
        std::io::ErrorKind::StorageFull,
        "No space left on device (os error 28)",
    ));
    let (status, _, _) = write(&app, LP).await;
    assert_eq!(status, StatusCode::INSUFFICIENT_STORAGE);

    let (status, body, _) = write(&app, LP).await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "the very next write must succeed: {body}"
    );

    let (_, body) = sql(&app, "SELECT COUNT(*) AS n FROM disk").await;
    assert!(
        body.contains("\"n\":1") || body.contains("\"n\": 1"),
        "exactly the second write should be there: {body}"
    );
}

/// A real errno, not just the portable spelling. `StorageFull` is what the
/// injection raises, but a full filesystem arrives as ENOSPC from the
/// kernel, and which `ErrorKind` that maps to has moved between Rust
/// releases. This injects the error the kernel actually produces, errno and
/// all, so both halves of the classifier see what they would see in
/// production.
#[cfg(unix)]
#[tokio::test]
async fn a_raw_enospc_errno_is_recognised_too() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let app = timelake_server::app(e.clone());

    let err = std::io::Error::from_raw_os_error(28);
    let kind = err.kind();
    e.fail_next_wal_append(err);
    let (status, body, _) = write(&app, LP).await;
    assert_eq!(
        status,
        StatusCode::INSUFFICIENT_STORAGE,
        "errno 28 is ENOSPC and must classify as disk-full whatever \
         ErrorKind this Rust maps it to ({kind:?}): {body}"
    );
}

/// The gauge exists, and reports a number an alert can threshold. Asserting
/// it is non-zero is the whole assertion: a zero would mean the probe
/// reported "full" for a tempdir that plainly is not, and that number goes
/// straight to a pager.
#[tokio::test]
async fn free_space_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));

    let text = metrics(&app).await;
    let line = text
        .lines()
        .find(|l| l.starts_with("timelake_data_dir_free_bytes "))
        .unwrap_or_else(|| panic!("no free-space gauge on /metrics:\n{text}"));
    let bytes: u64 = line.rsplit(' ').next().unwrap().parse().unwrap();
    assert!(
        bytes > 0,
        "a tempdir with no free space would be a broken probe, not a full \
         disk: {line}"
    );
}

/// `flushes_total` counts successes only, so a node that can no longer write
/// Parquet looked exactly like an idle one. The counter has to be present at
/// zero for a rule to be written against it at all.
#[tokio::test]
async fn flush_failures_are_exposed() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));

    let text = metrics(&app).await;
    assert!(
        text.contains("timelake_flush_failures_total 0"),
        "the flush-failure counter must exist before it moves: {text}"
    );
}
