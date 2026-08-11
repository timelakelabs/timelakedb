//! CL-3 stateless querier — the in-process, deterministic half.
//!
//! What is pinned here: an ingester serves its live buffer as Arrow IPC; a
//! querier sharing the object store answers with *files plus those live
//! rows*, exactly; a flush that happens behind the querier's back does not
//! lose a row (the watermark); a querier refuses writes; and a lone `all`
//! node is untouched by any of it. The multi-container behaviour — killing a
//! querier, killing an ingester, routing through the router — is drilled
//! live (`bench/results/cl3-querier-drill.log`).

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use timelake_server::querier::RemoteBuffers;
use timelake_store::{LocalStore, Store};
use tower::ServiceExt;

fn cfg() -> timelake_server::EngineConfig {
    timelake_server::EngineConfig {
        flush_rows: 1_000_000, // tests flush explicitly
        flush_age_secs: u64::MAX,
        wal_max_bytes: u64::MAX,
        ..Default::default()
    }
}

/// Two engines, one object store — the cluster shape in a single process.
fn node(dir: &std::path::Path, store: Arc<dyn Store>) -> Arc<timelake_server::Engine> {
    timelake_server::Engine::open_with_store(dir, cfg(), store, false).unwrap()
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

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
        .unwrap_or(-1)
}

async fn write(engine: &Arc<timelake_server::Engine>, lp: &str) {
    let app = timelake_server::app(engine.clone());
    let res = app
        .oneshot(
            Request::post("/api/v3/write_lp?db=poc")
                .header("content-type", "text/plain")
                .body(Body::from(lp.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT, "write must succeed");
}

/// Serve one engine's intra-cluster listener on an ephemeral port and give
/// back its address — the real wire, not a mock.
async fn serve_internal(engine: Arc<timelake_server::Engine>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let app = timelake_server::internal_router(engine);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

#[tokio::test]
async fn a_querier_sees_rows_that_are_still_only_in_an_ingesters_memory() {
    let objects = tempfile::tempdir().unwrap();
    let store: Arc<dyn Store> = Arc::new(LocalStore::new(objects.path()).unwrap());
    let ing_dir = tempfile::tempdir().unwrap();
    let qry_dir = tempfile::tempdir().unwrap();

    let ing = node(ing_dir.path(), store.clone());
    let t = now_ns();
    let lp: String = (0..250)
        .map(|i| format!("cpu,host=h{} v={i}i {}\n", i % 7, t + i))
        .collect();
    write(&ing, &lp).await;

    let addr = serve_internal(ing.clone()).await;
    let qry = node(qry_dir.path(), store.clone());
    qry.set_read_only();
    let remote = Arc::new(RemoteBuffers::new(vec![("ing-a".into(), addr)]));
    qry.set_remote_buffers(remote.clone());

    // Nothing has been flushed: every one of these rows exists ONLY in the
    // ingester's buffer. A querier that reads the store alone answers 0.
    assert_eq!(qry.catalog_head(), 0, "no commits yet");
    let qapp = timelake_server::app(qry.clone());
    assert_eq!(
        count(&qapp, "cpu").await,
        250,
        "the querier must union the ingester's live buffer, not just the store"
    );
}

#[tokio::test]
async fn a_flush_behind_the_queriers_back_moves_rows_without_losing_them() {
    // The failure this guards: rows leave the ingester's buffer (flushed and
    // committed) while the querier's catalog view is still older than that
    // commit. Reading the buffer and then a stale file list would show
    // neither copy. The head watermark is what closes it.
    let objects = tempfile::tempdir().unwrap();
    let store: Arc<dyn Store> = Arc::new(LocalStore::new(objects.path()).unwrap());
    let ing_dir = tempfile::tempdir().unwrap();
    let qry_dir = tempfile::tempdir().unwrap();

    let ing = node(ing_dir.path(), store.clone());
    let t = now_ns();
    let lp: String = (0..300)
        .map(|i| format!("cpu,host=h{} v={i}i {}\n", i % 5, t + i))
        .collect();
    write(&ing, &lp).await;

    let addr = serve_internal(ing.clone()).await;
    let qry = node(qry_dir.path(), store.clone());
    qry.set_read_only();
    qry.set_remote_buffers(Arc::new(RemoteBuffers::new(vec![("ing-a".into(), addr)])));
    let qapp = timelake_server::app(qry.clone());
    assert_eq!(count(&qapp, "cpu").await, 300, "live rows");

    // The ingester flushes. The querier is told nothing and its tail loop
    // is not running — its catalog view is now definitively stale.
    let flushed = ing.flush_all().unwrap();
    assert!(flushed > 0, "the flush must have produced files");
    let stale = qry.catalog_head();

    assert_eq!(
        count(&qapp, "cpu").await,
        300,
        "rows moved from memory to the store and must still all be there"
    );
    assert!(
        qry.catalog_head() > stale,
        "answering it should have folded the manifest log forward"
    );

    // And new live rows on top of the now-flushed ones still add up.
    write(&ing, &format!("cpu,host=h9 v=1i {}\n", t + 100_000)).await;
    assert_eq!(count(&qapp, "cpu").await, 301, "files + fresh live rows");
}

#[tokio::test]
async fn a_table_written_after_the_querier_booted_becomes_queryable() {
    // A querier boots against an empty bucket, then a brand-new table is
    // written to an ingester and never flushed. It exists in no catalog and
    // no local buffer — only the live view can reveal it.
    let objects = tempfile::tempdir().unwrap();
    let store: Arc<dyn Store> = Arc::new(LocalStore::new(objects.path()).unwrap());
    let ing_dir = tempfile::tempdir().unwrap();
    let qry_dir = tempfile::tempdir().unwrap();

    let ing = node(ing_dir.path(), store.clone());
    let addr = serve_internal(ing.clone()).await;
    let qry = node(qry_dir.path(), store.clone());
    qry.set_read_only();
    let remote = Arc::new(RemoteBuffers::new(vec![("ing-a".into(), addr)]));
    qry.set_remote_buffers(remote.clone());

    let t = now_ns();
    write(&ing, &format!("brand_new,host=a v=1i {t}\n")).await;

    let qapp = timelake_server::app(qry.clone());
    assert_eq!(
        count(&qapp, "brand_new").await,
        1,
        "the cold-start refresh must find a database that exists only in memory"
    );
    // The tail loop's steady-state path reaches the same answer.
    remote.refresh_live().await;
    assert_eq!(
        remote.live_tables(),
        vec![("poc".to_string(), "brand_new".to_string())]
    );
    assert!(qry.table_names("poc").contains(&"brand_new".to_string()));
}

#[tokio::test]
async fn the_live_endpoints_report_what_the_read_path_would_union() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn Store> = Arc::new(LocalStore::new(dir.path()).unwrap());
    let eng = node(dir.path(), store);
    let t = now_ns();
    write(
        &eng,
        &format!("cpu,host=a v=1i {t}\ncpu,host=b v=2i {}\n", t + 1),
    )
    .await;
    write(&eng, &format!("mem,host=a used=3i {}\n", t + 2)).await;

    let internal = timelake_server::internal_router(eng.clone());
    let res = internal
        .clone()
        .oneshot(
            Request::get("/internal/v1/live")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let report = timelake_server::querier::parse_live(&body).unwrap();
    assert_eq!(report.tables.len(), 2, "cpu and mem");
    let cpu = report.tables.iter().find(|t| t.table == "cpu").unwrap();
    assert_eq!(cpu.rows, 2);
    assert_eq!(cpu.db, "poc");

    // The snapshot is Arrow IPC, and it decodes to exactly those rows.
    let res = internal
        .oneshot(
            Request::get("/internal/v1/snapshot?db=poc&table=cpu")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(
        res.headers()
            .get(timelake_server::querier::CATALOG_HEAD_HEADER)
            .is_some(),
        "the freshness watermark must travel with every snapshot"
    );
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let batches = timelake_query::ipc::from_ipc(&bytes).unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

#[tokio::test]
async fn asking_an_ingester_for_a_table_it_does_not_hold_is_empty_not_an_error() {
    // Under sharding most tables live on some *other* ingester, so this is
    // the common case on every fan-out — it must be cheap and quiet.
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn Store> = Arc::new(LocalStore::new(dir.path()).unwrap());
    let eng = node(dir.path(), store);
    let bytes = eng.snapshot_ipc("poc", "never_written").unwrap();
    assert!(bytes.is_empty());
    assert!(timelake_query::ipc::from_ipc(&bytes).unwrap().is_empty());
}

#[tokio::test]
async fn a_querier_refuses_writes_rather_than_accepting_them_nowhere() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn Store> = Arc::new(LocalStore::new(dir.path()).unwrap());
    let eng = node(dir.path(), store);
    eng.set_read_only();
    assert!(eng.is_read_only());

    let app = timelake_server::app(eng.clone());
    let res = app
        .oneshot(
            Request::post("/api/v3/write_lp?db=poc")
                .header("content-type", "text/plain")
                .body(Body::from(format!("cpu,host=a v=1i {}", now_ns())))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        StatusCode::NOT_IMPLEMENTED,
        "not 400 (the request is fine) and not 500 (nothing is broken)"
    );
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let msg = String::from_utf8_lossy(&body);
    assert!(
        msg.contains("querier"),
        "the error must name the cause: {msg}"
    );
}

#[tokio::test]
async fn a_lone_node_emits_no_querier_metrics_and_is_otherwise_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn Store> = Arc::new(LocalStore::new(dir.path()).unwrap());
    let eng = node(dir.path(), store);
    assert!(eng.remote_buffers().is_none());
    assert!(!eng.is_read_only(), "role=all still writes");

    let app = timelake_server::app(eng.clone());
    write(&eng, &format!("cpu,host=a v=1i {}\n", now_ns())).await;
    assert_eq!(count(&app, "cpu").await, 1);

    let m = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body =
        String::from_utf8(m.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
    assert!(
        !body.contains("timelake_querier_"),
        "a lone node must not emit CL-3 metrics"
    );
}

#[tokio::test]
async fn a_querier_reports_its_ingesters_and_its_catalog_head() {
    let objects = tempfile::tempdir().unwrap();
    let store: Arc<dyn Store> = Arc::new(LocalStore::new(objects.path()).unwrap());
    let ing_dir = tempfile::tempdir().unwrap();
    let qry_dir = tempfile::tempdir().unwrap();

    let ing = node(ing_dir.path(), store.clone());
    write(&ing, &format!("cpu,host=a v=1i {}\n", now_ns())).await;
    ing.flush_all().unwrap();
    let addr = serve_internal(ing.clone()).await;

    let qry = node(qry_dir.path(), store.clone());
    qry.set_read_only();
    qry.set_remote_buffers(Arc::new(RemoteBuffers::new(vec![("ing-a".into(), addr)])));

    let app = timelake_server::app(qry.clone());
    let m = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body =
        String::from_utf8(m.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
    assert!(body.contains("timelake_querier_ingesters 1"));
    assert!(body.contains("timelake_querier_refusals_total 0"));
    assert!(
        body.contains("timelake_catalog_head 1"),
        "the querier replayed the ingester's commit: {body}"
    );
}

#[tokio::test]
async fn a_column_added_after_the_querier_booted_is_not_read_short() {
    // The querier's schema registry is built from file footers at boot. If
    // it never refreshed, a column that appeared later would read as absent
    // — silently, on every row.
    let objects = tempfile::tempdir().unwrap();
    let store: Arc<dyn Store> = Arc::new(LocalStore::new(objects.path()).unwrap());
    let ing_dir = tempfile::tempdir().unwrap();
    let qry_dir = tempfile::tempdir().unwrap();

    let ing = node(ing_dir.path(), store.clone());
    let t = now_ns();
    write(&ing, &format!("cpu,host=a v=1i {t}\n")).await;
    ing.flush_all().unwrap();

    let addr = serve_internal(ing.clone()).await;
    let qry = node(qry_dir.path(), store.clone());
    qry.set_read_only();
    qry.set_remote_buffers(Arc::new(RemoteBuffers::new(vec![("ing-a".into(), addr)])));
    assert!(
        qry.table_schema("poc", "cpu")
            .unwrap()
            .field_with_name("temp")
            .is_err(),
        "the column does not exist yet"
    );

    // A later write adds a field, and it is flushed to a new file.
    write(&ing, &format!("cpu,host=a v=2i,temp=41.5 {}\n", t + 1)).await;
    ing.flush_all().unwrap();

    let app = timelake_server::app(qry.clone());
    let req = Request::post("/api/sql")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({"db":"poc","sql":"SELECT SUM(temp) AS s FROM cpu"}).to_string(),
        ))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let s = v[0]["s"].as_f64().unwrap_or(0.0);
    assert!(
        (s - 41.5).abs() < 1e-9,
        "the new column must be visible without a restart: {v}"
    );
}
