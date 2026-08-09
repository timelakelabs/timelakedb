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
    let text =
        String::from_utf8(res.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
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
        write_lp(
            &app,
            "/api/v3/write_lp?db=poc",
            lp.as_bytes().to_vec(),
            false
        )
        .await,
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
    assert_eq!(
        rows[0]["n"], 2,
        "precision=s timestamps must land in-window"
    );
}

#[tokio::test]
async fn errors_are_400_with_line_context_and_never_wal_d() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelord_server::app(engine(dir.path()));

    let code = write_lp(
        &app,
        "/api/v3/write_lp?db=poc",
        b"good f=1i\nbroken".to_vec(),
        false,
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    // Line protocol has no byte escape, so a non-UTF-8 body cannot be
    // expressed at all: it is refused whole, before the parser and before
    // the WAL. Clients holding Latin-1 or binary must transcode.
    let code = write_lp(
        &app,
        "/api/v3/write_lp?db=poc",
        b"m,host=\xffZ v=1".to_vec(),
        false,
    )
    .await;
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
    assert_eq!(
        rows[0]["n"], 4,
        "acknowledged rows survive restart, no dups"
    );
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
    let lp1 = format!("pipeline_events,product_id=p1,step=01-download,event=start value=1i {t}");
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
    assert_eq!(
        code,
        StatusCode::BAD_REQUEST,
        "hog must be rejected: {body}"
    );

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

#[tokio::test]
async fn a_query_pruned_to_nothing_returns_empty_not_an_error() {
    // Pruning every row group away left the plan with a source that had no
    // partitions at all, so anything with an ORDER BY above it failed its
    // sanity check and came back 400. Sharper pruning makes this the common
    // case, not a corner.
    let dir = tempfile::tempdir().unwrap();
    let engine_ref = engine(dir.path());
    let app = timelord_server::app(Arc::clone(&engine_ref));
    let t = now_ns();
    for i in 0..20 {
        let lp = format!("ev,pid=p{i} v=1i {}", t + i);
        assert_eq!(
            write_lp(&app, "/api/v3/write_lp?db=poc", lp.into_bytes(), false).await,
            StatusCode::NO_CONTENT
        );
    }
    // drain the buffer so the query has only files to prune
    engine_ref.flush_all().expect("flush");

    let (code, rows) = sql(
        &app,
        "poc",
        "SELECT time, pid FROM ev WHERE pid = 'no-such-entity' ORDER BY time",
    )
    .await;
    assert_eq!(
        code,
        StatusCode::OK,
        "pruned-to-nothing must not be an error"
    );
    assert_eq!(rows.as_array().map(|a| a.len()), Some(0));

    // and the same query without ORDER BY, for good measure
    let (code, rows) = sql(
        &app,
        "poc",
        "SELECT COUNT(*) AS n FROM ev WHERE pid = 'no-such-entity'",
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(rows[0]["n"], 0);
}

#[tokio::test]
async fn schema_is_discoverable_over_sql() {
    // Without information_schema, SHOW TABLES failed outright and every
    // BI tool that starts by enumerating a schema was locked out.
    let dir = tempfile::tempdir().unwrap();
    let app = timelord_server::app(engine(dir.path()));
    let t = now_ns();
    for lp in [
        format!("pipeline_events,product_id=p1,step=01 value=1i {t}"),
        format!("disk_metrics,host=h1 used_gb=1.5 {t}"),
    ] {
        assert_eq!(
            write_lp(&app, "/api/v3/write_lp?db=poc", lp.into_bytes(), false).await,
            StatusCode::NO_CONTENT
        );
    }

    let (code, rows) = sql(&app, "poc", "SHOW TABLES").await;
    assert_eq!(code, StatusCode::OK);
    let listed: Vec<&str> = rows
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|r| r["table_name"].as_str())
        .collect();
    assert!(listed.contains(&"pipeline_events"), "got {listed:?}");
    assert!(listed.contains(&"disk_metrics"), "got {listed:?}");

    // the same answer through information_schema, and the catalog is named
    // after the database so Flight SQL's catalog list agrees with SQL
    let (code, rows) = sql(
        &app,
        "poc",
        "SELECT table_catalog, table_schema FROM information_schema.tables \
         WHERE table_name = 'disk_metrics'",
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(rows[0]["table_catalog"], "poc");
    assert_eq!(rows[0]["table_schema"], "public");

    // which is what makes the three-part names BI tools generate resolve
    let (code, rows) = sql(
        &app,
        "poc",
        "SELECT COUNT(*) AS n FROM poc.public.pipeline_events",
    )
    .await;
    assert_eq!(code, StatusCode::OK, "three-part name must resolve");
    assert_eq!(rows[0]["n"], 1);
}

#[tokio::test]
async fn a_rejected_write_cannot_poison_the_table_or_the_engine() {
    // One type-conflicting line used to leave the buffer with ragged
    // columns: reads of the table failed from then on, the flush that
    // would have drained it failed, and the maintenance tick took
    // compaction and retention for every other table down with it. The
    // WAL replayed the same line at boot, so a restart did not clear it.
    let dir = tempfile::tempdir().unwrap();
    let t = now_ns();
    let engine_ref = engine(dir.path());
    let app = timelord_server::app(Arc::clone(&engine_ref));

    assert_eq!(
        write_lp(
            &app,
            "/api/v3/write_lp?db=poc",
            format!("tt,h=a v=1 {t}").into_bytes(),
            false
        )
        .await,
        StatusCode::NO_CONTENT
    );
    // a second table, to prove the blast radius stays at zero tables
    assert_eq!(
        write_lp(
            &app,
            "/api/v3/write_lp?db=poc",
            format!("other,h=z v=1 {t}").into_bytes(),
            false
        )
        .await,
        StatusCode::NO_CONTENT
    );

    let code = write_lp(
        &app,
        "/api/v3/write_lp?db=poc",
        format!("tt,h=a v=\"oops\" {}", t + 1).into_bytes(),
        false,
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST, "type conflict is a 400");

    // reads still work, on this table and the other one
    let (code, rows) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM tt").await;
    assert_eq!(
        code,
        StatusCode::OK,
        "the rejected line must not break reads"
    );
    assert_eq!(rows[0]["n"], 1);
    let (code, _) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM other").await;
    assert_eq!(code, StatusCode::OK);

    // a duplicate tag key is accepted, and must not corrupt anything either
    assert_eq!(
        write_lp(
            &app,
            "/api/v3/write_lp?db=poc",
            format!("tt,h=a,h=b v=2 {}", t + 2).into_bytes(),
            false
        )
        .await,
        StatusCode::NO_CONTENT
    );

    // maintenance still works: the buffers flush and the WAL is reclaimed
    engine_ref.flush_all().expect("flush must succeed");
    let (code, rows) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM tt").await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(rows[0]["n"], 2);

    // and the poison does not come back from the WAL after a restart
    drop(app);
    drop(engine_ref);
    let app = timelord_server::app(engine(dir.path()));
    let (code, rows) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM tt").await;
    assert_eq!(code, StatusCode::OK, "restart must not replay the poison");
    assert_eq!(rows[0]["n"], 2);
}

/// /api/sql with an authorizations header — the SEC-2 wire contract.
async fn sql_as(
    app: &axum::Router,
    db: &str,
    q: &str,
    auths: &str,
) -> (StatusCode, serde_json::Value) {
    let req = Request::post("/api/sql")
        .header("content-type", "application/json")
        .header("x-timelord-authorizations", auths)
        .body(Body::from(
            serde_json::json!({"db": db, "sql": q}).to_string(),
        ))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    )
}

#[tokio::test]
async fn visibility_labels_gate_the_http_surface_sec2() {
    let dir = tempfile::tempdir().unwrap();
    let t = now_ns();
    let eng = engine(dir.path());
    let app = timelord_server::app(eng.clone());

    // labels are ordinary tags: no write-path ceremony (FR-2 economics)
    let lp = format!(
        "audit_log,actor=amy,_visibility=admin action=\"drop\" {t0}\n\
         audit_log,actor=bob,_visibility=(ops&audit)|admin action=\"read\" {t1}\n\
         audit_log,actor=cat action=\"login\" {t2}",
        t0 = t - 1000,
        t1 = t - 2000,
        t2 = t - 3000,
    );
    assert_eq!(
        write_lp(&app, "/api/v3/write_lp?db=poc", lp.into_bytes(), false).await,
        StatusCode::NO_CONTENT
    );

    // buffer path: no header → only the unlabeled row; an aggregate must
    // not leak hidden rows either (the SEC-2 acceptance criterion)
    let (code, rows) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM audit_log").await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(rows[0]["n"], 1, "no auths sees only public rows");

    let (_, rows) = sql_as(
        &app,
        "poc",
        "SELECT COUNT(*) AS n FROM audit_log",
        "ops,audit",
    )
    .await;
    assert_eq!(rows[0]["n"], 2, "ops+audit satisfies (ops&audit)|admin");

    let (_, rows) = sql_as(&app, "poc", "SELECT COUNT(*) AS n FROM audit_log", "admin").await;
    assert_eq!(rows[0]["n"], 3, "admin sees everything");

    // parquet path: identical answers once the buffer drains to files
    eng.flush_all().unwrap();
    let (_, rows) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM audit_log").await;
    assert_eq!(rows[0]["n"], 1, "flushed: public only");
    let (_, rows) = sql_as(
        &app,
        "poc",
        "SELECT actor FROM audit_log ORDER BY actor",
        "ops , audit",
    )
    .await;
    let actors: Vec<&str> = rows
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|r| r["actor"].as_str())
        .collect();
    assert_eq!(
        actors,
        vec!["bob", "cat"],
        "flushed: filtered rows, header whitespace tolerated"
    );

    // enforcement is visible, not silent (RR-5 spirit)
    assert!(
        eng.metrics_text_impl()
            .contains("timelord_visibility_rows_filtered_total"),
        "filtered-rows counter must be exported"
    );
}

#[tokio::test]
async fn encrypted_store_serves_and_survives_restart_sec1() {
    use timelord_store::{EncryptingStore, LocalKek, LocalStore, Store};
    let dir = tempfile::tempdir().unwrap();
    let kek = timelord_store::key_from_hex(&"5a".repeat(32)).unwrap();
    let open_encrypted = || {
        let store: Arc<dyn Store> = Arc::new(EncryptingStore::new(
            LocalStore::new(&dir.path().join("objects")).unwrap(),
            Arc::new(LocalKek::new(kek)),
        ));
        timelord_server::Engine::open_with_store(dir.path(), engine_cfg(Vec::new()), store, true)
            .unwrap()
    };

    let t = now_ns();
    let eng = open_encrypted();
    let app = timelord_server::app(eng.clone());
    let lp: String = (0..500)
        .map(|i| format!("sec,pid=p{i:03} v={i}i {}\n", t - 1000 - i as i64))
        .collect();
    assert_eq!(
        write_lp(&app, "/api/v3/write_lp?db=poc", lp.into_bytes(), false).await,
        StatusCode::NO_CONTENT
    );
    eng.flush_all().unwrap();

    // reads work through the decorator, pruning and all
    let (code, rows) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM sec").await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(rows[0]["n"], 500);
    let (_, rows) = sql(&app, "poc", "SELECT v FROM sec WHERE pid = 'p042'").await;
    assert_eq!(rows[0]["v"], 42);
    assert!(
        eng.metrics_text_impl()
            .contains("timelord_encryption_enabled 1")
    );

    // at rest, NOTHING under objects/ is readable: not the parquet (no
    // PAR1 magic), not the manifest JSON
    let mut checked = 0;
    for entry in walkdir(&dir.path().join("objects")) {
        let bytes = std::fs::read(&entry).unwrap();
        assert!(
            bytes.starts_with(b"TLDE1"),
            "object {entry:?} must be encrypted at rest"
        );
        assert!(
            !bytes.windows(4).any(|w| w == b"PAR1"),
            "no parquet magic in {entry:?}"
        );
        checked += 1;
    }
    assert!(
        checked >= 2,
        "expected parquet + manifest under objects/, saw {checked}"
    );

    // restart: catalog manifests decrypt on load, data still answers
    drop(app);
    drop(eng);
    let eng = open_encrypted();
    let app = timelord_server::app(eng.clone());
    let (code, rows) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM sec").await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(rows[0]["n"], 500);
}

/// An authenticated admin session: the cookie plus its CSRF token.
#[derive(Clone, Default)]
struct AdminSession {
    cookie: String,
    csrf: String,
}

/// Log in and capture the session cookie + CSRF token.
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

/// Log in as the seeded admin and complete the forced rotation — what
/// every test that just wants a working console needs.
async fn admin_ready(app: &axum::Router) -> AdminSession {
    let (code, seeded) = login(app, "admin", "admin").await;
    assert_eq!(code, StatusCode::OK, "seeded credential must sign in");
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
    assert_eq!(code, StatusCode::OK, "forced rotation must succeed");
    let (code, session) = login(app, "admin", "test console password").await;
    assert_eq!(code, StatusCode::OK);
    session
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
        req = req.header("x-timelord-csrf", &session.csrf);
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

/// FR-7 as a runtime control: policies set over /admin/retention take
/// effect on the next enforcement pass, persist through the store, and
/// outlive a restart with a stale environment.
#[tokio::test]
async fn retention_is_manageable_at_runtime_and_persists_fr7() {
    let dir = tempfile::tempdir().unwrap();
    let t = now_ns();
    let day = 86_400_000_000_000i64;
    let eng = engine(dir.path()); // NO env seed — everything via the API
    let app = timelord_server::app(eng.clone());
    let session = admin_ready(&app).await;

    let (code, v) = admin_json(&app, "GET", "/admin/retention", None, &session).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(v["policies"].as_array().map(Vec::len), Some(0));

    let lp = format!(
        "gui_ret,k=a v=1i {old}\ngui_ret,k=b v=2i {new}",
        old = t - 3 * day,
        new = t - 1000,
    );
    write_lp(&app, "/api/v3/write_lp?db=poc", lp.into_bytes(), false).await;
    eng.flush_all().unwrap();

    // a bad duration is refused before anything changes
    let (code, _) = admin_json(
        &app,
        "PUT",
        "/admin/retention",
        Some(serde_json::json!({"table": "gui_ret", "duration": "soon"})),
        &session,
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);

    // keep 1 day
    let (code, v) = admin_json(
        &app,
        "PUT",
        "/admin/retention",
        Some(serde_json::json!({"table": "gui_ret", "duration": "1d"})),
        &session,
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{v}");
    assert_eq!(v["duration"], "1d");

    // the policy bites on the next pass: the 3-day-old partition drops
    assert!(eng.enforce_retention().unwrap() >= 1);
    let (_, rows) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM gui_ret").await;
    assert_eq!(rows[0]["n"], 1, "only the in-window row survives");

    // restart: the policy came from the STORE, not the environment —
    // and so did the (already rotated) credential
    drop(app);
    drop(eng);
    let eng = engine(dir.path());
    let app = timelord_server::app(eng.clone());
    let (code, session) = login(&app, "admin", "test console password").await;
    assert_eq!(
        code,
        StatusCode::OK,
        "the rotated password survives restart"
    );
    let (_, v) = admin_json(&app, "GET", "/admin/retention", None, &session).await;
    assert_eq!(v["policies"][0]["table"], "gui_ret");
    assert_eq!(v["policies"][0]["seconds"], 86_400);

    // remove it; the removal persists too
    let (code, _) = admin_json(&app, "DELETE", "/admin/retention/gui_ret", None, &session).await;
    assert_eq!(code, StatusCode::OK);
    drop(app);
    drop(eng);
    let eng = engine(dir.path());
    let app = timelord_server::app(eng);
    let (_, session) = login(&app, "admin", "test console password").await;
    let (_, v) = admin_json(&app, "GET", "/admin/retention", None, &session).await;
    assert_eq!(v["policies"].as_array().map(Vec::len), Some(0));
}

/// The full SEC-4 first-run story: admin/admin gets in, can do NOTHING
/// but change its password, and the console opens up once it has.
#[tokio::test]
async fn admin_surface_requires_auth_and_forces_the_first_password_change_sec4() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelord_server::app(eng.clone());
    let anon = AdminSession::default();

    // 1. unauthenticated: the deletion control is closed (exposure 3a)
    for (method, path, body) in [
        ("GET", "/admin/retention", None),
        (
            "PUT",
            "/admin/retention",
            Some(serde_json::json!({"table":"t","duration":"1s"})),
        ),
        ("DELETE", "/admin/retention/t", None),
    ] {
        let (code, v) = admin_json(&app, method, path, body, &anon).await;
        assert_eq!(code, StatusCode::UNAUTHORIZED, "{method} {path} -> {v}");
        assert_eq!(v["code"], "unauthenticated");
    }

    // 2. a wrong password, and a missing user, are both refused
    assert_eq!(
        login(&app, "admin", "nope").await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        login(&app, "ghost", "admin").await.0,
        StatusCode::UNAUTHORIZED
    );

    // 3. the seeded credential works but is quarantined to ONE action
    let (code, seeded) = login(&app, "admin", "admin").await;
    assert_eq!(code, StatusCode::OK);
    let (code, v) = admin_json(&app, "GET", "/admin/retention", None, &seeded).await;
    assert_eq!(code, StatusCode::FORBIDDEN);
    assert_eq!(v["code"], "password_change_required");
    let (code, _) = admin_json(
        &app,
        "PUT",
        "/admin/retention",
        Some(serde_json::json!({"table":"t","duration":"1s"})),
        &seeded,
    )
    .await;
    assert_eq!(
        code,
        StatusCode::FORBIDDEN,
        "the default credential must not be able to destroy data"
    );

    // 4. the password policy is enforced
    for bad in ["short", "admin", "ADMIN"] {
        let (code, _) = admin_json(
            &app,
            "POST",
            "/admin/password",
            Some(serde_json::json!({"current_password":"admin","new_password": bad})),
            &seeded,
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST, "{bad} must be refused");
    }

    // 5. a valid rotation opens the console — and kills the session that
    // performed it, so a stolen default-era cookie cannot outlive it
    let (code, _) = admin_json(
        &app,
        "POST",
        "/admin/password",
        Some(serde_json::json!({
            "current_password": "admin",
            "new_password": "a much better password"
        })),
        &seeded,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    let (code, _) = admin_json(&app, "GET", "/admin/retention", None, &seeded).await;
    assert_eq!(code, StatusCode::UNAUTHORIZED);

    // 6. the new credential is a working admin; the old one is dead
    assert_eq!(
        login(&app, "admin", "admin").await.0,
        StatusCode::UNAUTHORIZED
    );
    let (code, session) = login(&app, "admin", "a much better password").await;
    assert_eq!(code, StatusCode::OK);
    let (code, v) = admin_json(&app, "GET", "/admin/retention", None, &session).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(v["role"], "admin");

    // 7. CSRF: a valid cookie without the matching token cannot mutate
    let no_csrf = AdminSession {
        cookie: session.cookie.clone(),
        csrf: "wrong".into(),
    };
    let (code, v) = admin_json(
        &app,
        "PUT",
        "/admin/retention",
        Some(serde_json::json!({"table":"t","duration":"7d"})),
        &no_csrf,
    )
    .await;
    assert_eq!(code, StatusCode::FORBIDDEN);
    assert_eq!(v["code"], "csrf");

    // 8. it survives a restart and does NOT re-seed the default
    drop(app);
    drop(eng);
    let eng = engine(dir.path());
    let app = timelord_server::app(eng.clone());
    assert_eq!(
        login(&app, "admin", "admin").await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        login(&app, "admin", "a much better password").await.0,
        StatusCode::OK
    );
    assert!(
        eng.metrics_text_impl()
            .contains("timelord_admin_default_credential_active 0"),
        "the default-credential alarm must clear once rotated"
    );

    // 9. the DATA plane stayed open — Telegraf, Grafana and the harness
    // must not need credentials (SEC-4 is phased)
    assert_eq!(
        write_lp(
            &app,
            "/api/v3/write_lp?db=poc",
            format!("open,k=a v=1i {}", now_ns()).into_bytes(),
            false
        )
        .await,
        StatusCode::NO_CONTENT
    );
    let (code, _) = sql(&app, "poc", "SELECT COUNT(*) AS n FROM open").await;
    assert_eq!(code, StatusCode::OK);
}

/// The console page is public — it carries no data, it asks for it — and
/// a node still holding the seeded password says so in /metrics.
#[tokio::test]
async fn console_page_is_public_and_the_default_credential_is_alarmable() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelord_server::app(eng.clone());

    let res = app
        .oneshot(Request::get("/admin/ui").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let html =
        String::from_utf8(res.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
    assert!(html.contains("Sign in"));
    assert!(html.contains("Retention"));

    assert!(
        eng.metrics_text_impl()
            .contains("timelord_admin_default_credential_active 1"),
        "a fresh node must raise the default-credential alarm"
    );
}

/// The C0 S3 drill caught acked rows going UNQUERYABLE during a table's
/// first flush: buffer swapped out, catalog commit still seconds away
/// behind slow object writes → "table not found" mid-benchmark. Rows
/// must stay visible across the whole upload window.
#[tokio::test]
async fn rows_stay_visible_while_a_slow_flush_uploads() {
    use timelord_store::{LocalStore, Store};

    struct SlowStore {
        inner: LocalStore,
        delay: std::time::Duration,
    }
    impl Store for SlowStore {
        fn put(&self, path: &str, bytes: &[u8]) -> std::io::Result<()> {
            std::thread::sleep(self.delay); // an object-store-shaped put
            self.inner.put(path, bytes)
        }
        fn put_if_absent(&self, path: &str, bytes: &[u8]) -> std::io::Result<bool> {
            std::thread::sleep(self.delay);
            self.inner.put_if_absent(path, bytes)
        }
        fn get(&self, path: &str) -> std::io::Result<Vec<u8>> {
            self.inner.get(path)
        }
        fn get_range(&self, path: &str, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
            self.inner.get_range(path, offset, len)
        }
        fn size(&self, path: &str) -> std::io::Result<u64> {
            self.inner.size(path)
        }
        fn delete(&self, path: &str) -> std::io::Result<()> {
            self.inner.delete(path)
        }
        fn list(&self, prefix: &str) -> std::io::Result<Vec<String>> {
            self.inner.list(prefix)
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn timelord_store::Store> = Arc::new(SlowStore {
        inner: LocalStore::new(&dir.path().join("objects")).unwrap(),
        delay: std::time::Duration::from_millis(150),
    });
    let eng =
        timelord_server::Engine::open_with_store(dir.path(), engine_cfg(Vec::new()), store, false)
            .unwrap();

    let t = now_ns();
    let lp = format!(
        "slowflush,k=a v=1i {}\nslowflush,k=b v=2i {}\nslowflush,k=c v=3i {}",
        t - 1000,
        t - 2000,
        t - 3000
    );
    assert!(
        timelord_api::Engine::write_lp(&*eng, "poc", lp.as_bytes(), None).is_ok(),
        "write must land"
    );

    // first flush of this table: file put + manifest put ≈ 300 ms of
    // window that used to serve "table not found"
    let flusher = {
        let eng = Arc::clone(&eng);
        std::thread::spawn(move || eng.flush_all())
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    let mut probes = 0;
    while !flusher.is_finished() && std::time::Instant::now() < deadline {
        let batches = eng
            .sql_batches("poc", "SELECT COUNT(*) AS n FROM slowflush", Vec::new())
            .await
            .expect("mid-flush query must not fail");
        let n = timelord_query::batches_to_json(&batches)[0]["n"].as_i64();
        assert_eq!(n, Some(3), "acked rows vanished mid-flush");
        probes += 1;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(probes >= 3, "the flush window was never actually probed");
    flusher.join().unwrap().unwrap();

    // and after the flush the answer is identical, from files
    let batches = eng
        .sql_batches("poc", "SELECT COUNT(*) AS n FROM slowflush", Vec::new())
        .await
        .unwrap();
    assert_eq!(timelord_query::batches_to_json(&batches)[0]["n"], 3);
}

fn walkdir(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else {
                out.push(p);
            }
        }
    }
    out
}
