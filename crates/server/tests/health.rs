//! M1 integration: the ingest path end to end, in process — the same
//! contracts the bench adapter (AT-2) and Telegraf (FR-9) rely on.

use std::io::Write as _;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

fn engine_cfg(retention: Vec<(String, u64)>) -> timelord_server::EngineConfig {
    timelord_server::EngineConfig {
        query_mem_bytes: 256 * 1024 * 1024,
        flush_rows: 1_000_000, // no auto-trigger; tests flush explicitly
        flush_age_secs: u64::MAX,
        wal_max_bytes: u64::MAX,
        compact_min_files: 2,
        retention,
        max_concurrent_queries: 4,
        query_timeout_secs: 120,
        gc_grace_secs: 0, // tests exercise immediate GC via run_gc()
    }
}

fn engine(dir: &std::path::Path) -> Arc<timelord_server::Engine> {
    timelord_server::Engine::open(dir, engine_cfg(Vec::new())).unwrap()
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

async fn write_lp(
    app: &axum::Router,
    path: &str,
    body: impl Into<Vec<u8>>,
    gzip: bool,
) -> StatusCode {
    let mut req = Request::post(path).header("content-type", "text/plain");
    let bytes = if gzip {
        req = req.header("content-encoding", "gzip");
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        enc.write_all(&body.into()).unwrap();
        enc.finish().unwrap()
    } else {
        body.into()
    };
    app.clone()
        .oneshot(req.body(Body::from(bytes)).unwrap())
        .await
        .unwrap()
        .status()
}

async fn sql(app: &axum::Router, db: &str, q: &str) -> (StatusCode, serde_json::Value) {
    let req = Request::post("/api/sql")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({"db": db, "sql": q}).to_string(),
        ))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    if !status.is_success() {
        eprintln!("sql error [{status}] for {q}: {v}");
    }
    (status, v)
}

#[tokio::test]
async fn health_payload_is_the_adapter_contract() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelord_server::app(engine(dir.path()));
    let res = app
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    // bench/backends/timelorddb.py healthy() needs 2xx; version() reads
    // .version — wire contract, do not rename.
    assert_eq!(v["status"], "pass");
    assert_eq!(v["name"], "timelorddb");
    assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
}

#[tokio::test]
async fn ping_and_metrics_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelord_server::app(engine(dir.path()));

    let res = app
        .clone()
        .oneshot(Request::get("/ping").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        res.headers()["x-timelorddb-version"],
        env!("CARGO_PKG_VERSION")
    );

    let res = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let text = String::from_utf8(
        res.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(text.starts_with('#'));
    assert!(text.contains("timelord_lines_written_total"));
}

#[tokio::test]
async fn write_then_query_exact_counts_mini_at2() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelord_server::app(engine(dir.path()));
    let t = now_ns();

    let lp = format!(
        "pipeline_events,product_id=p1,step=01-download,event=start value=1i {a}\n\
         pipeline_events,product_id=p1,step=01-download,event=stop duration_s=2.5 {b}\n\
         pipeline_events,product_id=p2,step=01-download,event=stop duration_s=1.5 {c}",
        a = t - 3000,
        b = t - 2000,
        c = t - 1000
    );
    assert_eq!(
        write_lp(&app, "/api/v3/write_lp?db=poc", lp.as_bytes().to_vec(), false).await,
        StatusCode::NO_CONTENT
    );

    let (code, rows) = sql(
        &app,
        "poc",
        "SELECT COUNT(*) AS n FROM pipeline_events \
         WHERE time >= now() - INTERVAL '48 hours'",
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(rows[0]["n"], 3, "exact count is the AT-2 contract");

    let (_, rows) = sql(
        &app,
        "poc",
        "SELECT step, COUNT(DISTINCT product_id) AS products FROM pipeline_events \
         WHERE event = 'stop' AND time >= now() - INTERVAL '24 hours' \
         GROUP BY step ORDER BY step",
    )
    .await;
    assert_eq!(rows[0]["products"], 2);
}

#[tokio::test]
async fn v1_precision_and_v2_gzip_telegraf_contract() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelord_server::app(engine(dir.path()));
    let secs = now_ns() / 1_000_000_000;

    // v1 endpoint with precision=s
    assert_eq!(
        write_lp(
            &app,
            "/write?db=poc&precision=s",
            format!("host_metrics,host=h1 cpu_pct=12.5 {secs}").into_bytes(),
            false
        )
        .await,
        StatusCode::NO_CONTENT
    );
    // v2 endpoint, gzip body (Telegraf influxdb_v2 default)
    assert_eq!(
        write_lp(
            &app,
            "/api/v2/write?org=poc&bucket=poc&precision=s",
            format!("host_metrics,host=h2 cpu_pct=50.0 {secs}").into_bytes(),
            true
        )
        .await,
        StatusCode::NO_CONTENT
    );

    let (_, rows) = sql(
        &app,
        "poc",
        "SELECT COUNT(*) AS n FROM host_metrics \
         WHERE time >= now() - INTERVAL '6 hours'",
    )
    .await;
    assert_eq!(rows[0]["n"], 2, "precision=s timestamps must land in-window");
}

#[tokio::test]
async fn errors_are_400_with_line_context_and_never_wal_d() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelord_server::app(engine(dir.path()));

    let code = write_lp(&app, "/api/v3/write_lp?db=poc", b"good f=1i\nbroken".to_vec(), false).await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    // missing db
    let code = write_lp(&app, "/api/v3/write_lp", b"m f=1i".to_vec(), false).await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    // unknown database on query
    let (code, _) = sql(&app, "nope", "SELECT 1").await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    // the rejected write must not have been made durable: nothing replays
    drop(app);
    let (_, rows_after) = {
        let app = timelord_server::app(engine(dir.path()));
        sql(&app, "poc", "SELECT 1 AS one").await
    };
    // db 'poc' never got a successful write, so it should not exist
    assert!(rows_after.is_null() || rows_after.get("error").is_some());
}

#[tokio::test]
async fn flush_parquet_union_restart_and_dedup_m2() {
    let dir = tempfile::tempdir().unwrap();
    let t = now_ns();
    let eng = engine(dir.path());
    let app = timelord_server::app(eng.clone());

    // rows across two different hours -> two partitions; one duplicate PK
    let h = 3_600_000_000_000i64;
    let lp = format!(
        "pipeline_events,product_id=p1,step=01-download,event=start value=1i {a}\n\
         pipeline_events,product_id=p1,step=01-download,event=start value=9i {a}\n\
         pipeline_events,product_id=p2,step=01-download,event=start value=1i {b}\n\
         pipeline_events,product_id=p3,step=02-extract,event=start value=1i {c}",
        a = t - 2 * h,
        b = t - 2 * h + 1,
        c = t - 1000,
    );
    assert_eq!(
        write_lp(&app, "/api/v3/write_lp?db=poc", lp.into_bytes(), false).await,
        StatusCode::NO_CONTENT
    );

    // flush everything to Parquet
    let files = eng.flush_all().unwrap();
    assert!(files >= 2, "expected >=2 partition files, got {files}");

    // buffer is empty now; reads come from Parquet: dup PK collapsed (FR-5)
    let (code, rows) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM pipeline_events").await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(rows[0]["n"], 3, "LWW dedup at flush");
    let (_, rows) = sql(
        &app,
        "poc",
        "SELECT value FROM pipeline_events WHERE product_id = 'p1'",
    )
    .await;
    assert_eq!(rows[0]["value"], 9, "last write wins");

    // new writes land in the buffer; queries union buffer + files
    let lp2 = format!(
        "pipeline_events,product_id=p4,step=03-validate,event=start value=1i {}",
        t - 500
    );
    write_lp(&app, "/api/v3/write_lp?db=poc", lp2.into_bytes(), false).await;
    let (_, rows) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM pipeline_events").await;
    assert_eq!(rows[0]["n"], 4, "union of parquet + buffer");

    // restart: parquet via catalog + WAL replay of the unflushed row only
    drop(app);
    drop(eng);
    let eng = engine(dir.path());
    let app = timelord_server::app(eng.clone());
    let (_, rows) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM pipeline_events").await;
    assert_eq!(rows[0]["n"], 4, "acknowledged rows survive restart, no dups");
    // schema alignment across files with different column sets works:
    let (code, _) = sql(
        &app,
        "poc",
        "SELECT step, COUNT(DISTINCT product_id) FROM pipeline_events GROUP BY step",
    )
    .await;
    assert_eq!(code, StatusCode::OK);
}

#[tokio::test]
async fn compaction_merges_files_and_completes_fr5_m3() {
    let dir = tempfile::tempdir().unwrap();
    let t = now_ns();
    let eng = engine(dir.path());
    let app = timelord_server::app(eng.clone());

    // write + flush, then RETRY the same PK with a new value + flush again:
    // the duplicate now lives across two files
    let lp1 = format!(
        "pipeline_events,product_id=p1,step=01-download,event=start value=1i {t}"
    );
    write_lp(&app, "/api/v3/write_lp?db=poc", lp1.into_bytes(), false).await;
    eng.flush_all().unwrap();
    let lp2 = format!(
        "pipeline_events,product_id=p1,step=01-download,event=start value=42i {t}\n\
         pipeline_events,product_id=p2,step=01-download,event=start value=1i {}",
        t + 1
    );
    write_lp(&app, "/api/v3/write_lp?db=poc", lp2.into_bytes(), false).await;
    eng.flush_all().unwrap();

    // before compaction: cross-file duplicate is visible (M2 limit)
    let (_, rows) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM pipeline_events").await;
    assert_eq!(rows[0]["n"], 3, "cross-file dup exists pre-compaction");

    let compacted = eng.compact_once().unwrap();
    assert!(compacted >= 1, "partition with 2 files must compact");

    let (_, rows) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM pipeline_events").await;
    assert_eq!(rows[0]["n"], 2, "compaction collapsed the cross-file dup");
    let (_, rows) = sql(
        &app,
        "poc",
        "SELECT value FROM pipeline_events WHERE product_id = 'p1'",
    )
    .await;
    assert_eq!(rows[0]["value"], 42, "newest file won (FR-5 complete)");

    // survives restart via the manifest log (removes + adds replayed)
    drop(app);
    drop(eng);
    let eng = engine(dir.path());
    let app = timelord_server::app(eng.clone());
    let (_, rows) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM pipeline_events").await;
    assert_eq!(rows[0]["n"], 2);
}

#[tokio::test]
async fn retention_drops_expired_partitions_fr7() {
    let dir = tempfile::tempdir().unwrap();
    let t = now_ns();
    let day = 86_400_000_000_000i64;
    let eng = timelord_server::Engine::open(
        dir.path(),
        engine_cfg(vec![("short_lived".into(), 86_400)]), // keep 1 day
    )
    .unwrap();
    let app = timelord_server::app(eng.clone());

    let lp = format!(
        "short_lived,k=a v=1.0 {old}\nshort_lived,k=b v=2.0 {new}\nkept,k=a v=3.0 {old}",
        old = t - 3 * day,
        new = t - 1000,
    );
    write_lp(&app, "/api/v3/write_lp?db=poc", lp.into_bytes(), false).await;
    eng.flush_all().unwrap();

    let dropped = eng.enforce_retention().unwrap();
    assert!(dropped >= 1, "expired partition must drop");

    let (_, rows) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM short_lived").await;
    assert_eq!(rows[0]["n"], 1, "only the in-window row survives");
    // tables WITHOUT a policy keep everything (FR-7 is per-table)
    let (_, rows) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM kept").await;
    assert_eq!(rows[0]["n"], 1);
}

#[tokio::test]
async fn memory_pool_rejects_cleanly_never_kills_rr1() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = engine_cfg(Vec::new());
    cfg.query_mem_bytes = 2 * 1024 * 1024; // 2 MB pool: tiny on purpose
    let eng = timelord_server::Engine::open(dir.path(), cfg).unwrap();
    let app = timelord_server::app(eng.clone());

    let t = now_ns();
    let mut lp = String::new();
    for i in 0..3000 {
        lp.push_str(&format!(
            "m,tag=t{i} v={}.5 {}\n",
            i % 7,
            t - 1000 - i as i64
        ));
    }
    write_lp(&app, "/api/v3/write_lp?db=poc", lp.into_bytes(), false).await;

    // a deliberately memory-hungry shape: wide cross-join aggregation
    let (code, body) = sql(
        &app,
        "poc",
        "SELECT COUNT(DISTINCT a.tag || b.tag || c.tag) AS n \
         FROM m a CROSS JOIN m b CROSS JOIN m c",
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST, "hog must be rejected: {body}");

    // ...and the server is entirely fine afterwards (the RR-1 contract)
    let (code, rows) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM m").await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(rows[0]["n"], 3000);
}

#[tokio::test]
async fn wal_replay_survives_restart_rr3() {
    let dir = tempfile::tempdir().unwrap();
    let t = now_ns();
    {
        let app = timelord_server::app(engine(dir.path()));
        let lp = format!("pipeline_events,product_id=p1,step=01-download,event=start value=1i {t}");
        assert_eq!(
            write_lp(&app, "/api/v3/write_lp?db=poc", lp.into_bytes(), false).await,
            StatusCode::NO_CONTENT
        );
    } // engine dropped — "crash"

    let app = timelord_server::app(engine(dir.path()));
    let (code, rows) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM pipeline_events").await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(rows[0]["n"], 1, "a 204'd write must survive restart");
}
