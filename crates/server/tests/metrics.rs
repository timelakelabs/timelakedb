//! The U2 exposition (`docs/CONSOLE.md` §7.4), over the real HTTP surface.
//!
//! What is pinned here: `/metrics` answers with the query-latency,
//! lifecycle and per-table storage series the Query and Storage views need;
//! the numbers move for the right reasons; a refused write is counted apart
//! from a broken one; and a table name containing a quote cannot break the
//! exposition for every other series in the scrape.
//!
//! The gate this serves is §13's U2 gate — console numbers agreeing with
//! `/metrics` — so the assertions are on values, not merely on presence.

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
            Request::post("/api/v3/write_lp?db=poc")
                .header("content-type", "text/plain")
                .body(Body::from(lp.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

async fn sql(app: &axum::Router, q: &str) -> StatusCode {
    sql_in(app, "poc", q).await.0
}

async fn sql_in(app: &axum::Router, db: &str, q: &str) -> (StatusCode, serde_json::Value) {
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/sql")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"db": db, "sql": q}).to_string(),
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

async fn metrics(app: &axum::Router) -> String {
    let res = app
        .clone()
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// Pull a single unlabelled gauge/counter value out of the exposition.
fn value_of(text: &str, name: &str) -> Option<f64> {
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .find_map(|l| l.strip_prefix(name)?.trim().parse::<f64>().ok())
}

/// Pull a labelled series value, matching the full `name{labels}` prefix.
fn labelled(text: &str, prefix: &str) -> Option<f64> {
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .find_map(|l| l.strip_prefix(prefix)?.trim().parse::<f64>().ok())
}

#[tokio::test]
async fn a_query_moves_the_latency_and_outcome_series() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));

    // Nothing has run yet: the histogram exists but is empty, which is what
    // lets a dashboard show "no data" instead of a confident zero.
    let before = metrics(&app).await;
    assert_eq!(value_of(&before, "timelake_queries_total").unwrap(), 0.0);
    assert_eq!(
        value_of(&before, "timelake_query_duration_seconds_count").unwrap(),
        0.0
    );

    assert_eq!(
        write(&app, "cpu,host=a v=1.0 1").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(sql(&app, "SELECT * FROM cpu").await, StatusCode::OK);

    let after = metrics(&app).await;
    assert_eq!(value_of(&after, "timelake_queries_total").unwrap(), 1.0);
    assert_eq!(
        value_of(&after, "timelake_query_duration_seconds_count").unwrap(),
        1.0
    );
    // Admission is observed exactly once per query, not twice.
    assert_eq!(
        value_of(&after, "timelake_query_admission_wait_seconds_count").unwrap(),
        1.0
    );
    // The query is over, so the gauges are back down.
    assert_eq!(value_of(&after, "timelake_query_in_flight").unwrap(), 0.0);
    assert_eq!(value_of(&after, "timelake_query_queued").unwrap(), 0.0);
    // A successful query is not an error.
    assert_eq!(
        value_of(&after, "timelake_query_failed_total").unwrap(),
        0.0
    );
    assert_eq!(
        value_of(&after, "timelake_query_refused_total").unwrap(),
        0.0
    );

    // The +Inf bucket must account for every observation, or the histogram
    // is unusable for a quantile.
    assert_eq!(
        labelled(
            &after,
            "timelake_query_duration_seconds_bucket{le=\"+Inf\"}"
        )
        .unwrap(),
        1.0
    );
}

#[tokio::test]
async fn a_refused_statement_is_counted_apart_from_a_failure() {
    // P0-2: refusing a COPY is the guard working. Folding it into the
    // failure counter would make a healthy node look broken whenever a
    // client probes it.
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));
    write(&app, "cpu,host=a v=1.0 1").await;

    sql(&app, "COPY (SELECT * FROM cpu) TO '/tmp/x.parquet'").await;
    let text = metrics(&app).await;
    assert_eq!(
        value_of(&text, "timelake_query_refused_total").unwrap(),
        1.0
    );
    assert_eq!(value_of(&text, "timelake_query_failed_total").unwrap(), 0.0);

    sql(&app, "SELECT * FROM no_such_table").await;
    let text = metrics(&app).await;
    assert_eq!(
        value_of(&text, "timelake_query_refused_total").unwrap(),
        1.0
    );
    assert_eq!(value_of(&text, "timelake_query_failed_total").unwrap(), 1.0);
}

#[tokio::test]
async fn a_bad_write_is_counted_by_cause() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));

    // Malformed line protocol — the client's fault, not backpressure.
    let status = write(&app, "this is not line protocol at all").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let text = metrics(&app).await;
    assert_eq!(
        labelled(
            &text,
            "timelake_write_rejected_total{reason=\"bad_request\"}"
        )
        .unwrap(),
        1.0
    );
    // The distinction that matters: nothing here was backpressure, so an
    // operator is not sent looking at the WAL cap.
    assert_eq!(
        labelled(
            &text,
            "timelake_write_rejected_total{reason=\"backpressure\"}"
        )
        .unwrap(),
        0.0
    );
    assert_eq!(
        labelled(&text, "timelake_write_rejected_total{reason=\"internal\"}").unwrap(),
        0.0
    );
}

#[tokio::test]
async fn storage_series_appear_per_table_after_a_flush() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(eng.clone());

    write(&app, "cpu,host=a v=1.0 1000000000").await;
    write(&app, "cpu,host=b v=2.0 2000000000").await;
    write(&app, "mem,host=a v=3.0 3000000000").await;

    // Buffered rows are not catalog files yet — storage is genuinely zero.
    let before = metrics(&app).await;
    assert_eq!(value_of(&before, "timelake_parquet_files").unwrap(), 0.0);

    eng.flush_all().unwrap();

    let after = metrics(&app).await;
    let cpu_bytes = labelled(&after, "timelake_storage_bytes{db=\"poc\",table=\"cpu\"}")
        .expect("cpu storage series");
    let cpu_rows =
        labelled(&after, "timelake_storage_rows{db=\"poc\",table=\"cpu\"}").expect("cpu rows");
    let mem_rows =
        labelled(&after, "timelake_storage_rows{db=\"poc\",table=\"mem\"}").expect("mem rows");

    assert!(cpu_bytes > 0.0, "a flushed table has bytes on disk");
    assert_eq!(cpu_rows, 2.0);
    assert_eq!(mem_rows, 1.0);

    // Every file is classified into exactly one level, and the total
    // agrees with the count the catalog reports independently.
    let flushed = labelled(&after, "timelake_files{level=\"flushed\"}").unwrap();
    let compacted = labelled(&after, "timelake_files{level=\"compacted\"}").unwrap();
    let rewritten = labelled(&after, "timelake_files{level=\"rewritten\"}").unwrap();
    let total = value_of(&after, "timelake_parquet_files").unwrap();
    assert_eq!(flushed + compacted + rewritten, total);
    assert_eq!(compacted, 0.0, "nothing has been compacted yet");
}

#[tokio::test]
async fn flush_lag_reports_uptime_until_something_flushes() {
    // The "never happened" case is the one worth getting right: a zero here
    // reads as "just flushed" — perfectly healthy — at the exact moment the
    // subsystem has never run.
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(eng.clone());

    let before = metrics(&app).await;
    let uptime = value_of(&before, "timelake_uptime_seconds").unwrap();
    let lag = value_of(&before, "timelake_flush_lag_seconds").unwrap();
    assert!(
        lag >= uptime,
        "with no flush yet, lag ({lag}) must be the whole uptime ({uptime}), not 0"
    );

    write(&app, "cpu,host=a v=1.0 1000000000").await;
    eng.flush_all().unwrap();

    let after = metrics(&app).await;
    let lag = value_of(&after, "timelake_flush_lag_seconds").unwrap();
    assert!(lag < 5.0, "a flush just happened, lag was {lag}");
}

#[tokio::test]
async fn build_info_names_the_running_version() {
    let dir = tempfile::tempdir().unwrap();
    let app = timelake_server::app(engine(dir.path()));
    let text = metrics(&app).await;
    let expected = format!(
        "timelake_build_info{{version=\"{}\"}} 1",
        env!("CARGO_PKG_VERSION")
    );
    assert!(text.contains(&expected), "missing {expected}");
}

#[tokio::test]
async fn a_table_name_with_a_quote_cannot_corrupt_the_exposition() {
    // Table names arrive from line protocol, so they are caller-controlled.
    // Unescaped, a quote closes the label early and everything after it is
    // reparsed — silently corrupting series that have nothing to do with
    // the offending table.
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(eng.clone());

    // A measurement whose name carries a double quote. The write MUST
    // succeed — if the engine rejected it there would be no series at all
    // and every assertion below would pass while testing nothing.
    let status = write(&app, "ev\\\"il,host=a v=1.0 1000000000").await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "the hostile name has to reach the catalog for this test to mean anything"
    );
    eng.flush_all().unwrap();

    let text = metrics(&app).await;
    let series: Vec<&str> = text
        .lines()
        .filter(|l| l.starts_with("timelake_storage_bytes{"))
        .collect();
    assert_eq!(
        series.len(),
        1,
        "expected exactly one table's series, got {series:?}"
    );
    assert!(
        series[0].contains("\\\""),
        "the quote in the table name was not escaped: {}",
        series[0]
    );
    // The series that follow must still parse — the reject counters come
    // after the storage block, so they are what a broken label would eat.
    assert!(
        labelled(
            &text,
            "timelake_write_rejected_total{reason=\"backpressure\"}"
        )
        .is_some(),
        "series after the storage block went missing:\n{text}"
    );
    // And no exposition line may contain a bare (unescaped) quote inside a
    // label value.
    for line in text.lines().filter(|l| l.starts_with("timelake_storage")) {
        let quotes = line.matches('"').count();
        let escaped = line.matches("\\\"").count();
        assert_eq!(
            (quotes - escaped) % 2,
            0,
            "unbalanced quotes in exposition line: {line}"
        );
    }
}

// ---- U2-C: self-monitoring round trip ------------------------------------

#[tokio::test]
async fn a_query_becomes_a_row_the_database_can_answer_about_itself() {
    // The whole premise of self-monitoring: run a query, and the database
    // can then be asked how long its own queries took — in SQL, through the
    // same path Grafana uses.
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(eng.clone());

    write(&app, "cpu,host=a v=1.0 1000000000").await;
    assert_eq!(sql(&app, "SELECT * FROM cpu").await, StatusCode::OK);

    // Nothing is stored until the maintenance tick runs — the query path
    // only buffers, so it can never block on the write path.
    let stored = eng.selfmon_tick();
    assert!(stored > 0, "the tick should have stored the queued rows");

    let (status, rows) = sql_in(
        &app,
        "_system",
        "SELECT outcome, rows, duration_ms FROM queries",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    let rows = rows.as_array().expect("rows");
    assert_eq!(rows.len(), 1, "exactly the one query we ran: {rows:?}");
    assert_eq!(rows[0]["outcome"], "ok");
    assert_eq!(rows[0]["rows"], 1);
    assert!(
        rows[0]["duration_ms"].as_f64().unwrap() >= 0.0,
        "a real duration was recorded"
    );
}

#[tokio::test]
async fn the_exposition_is_stored_as_queryable_metric_rows() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(eng.clone());

    write(&app, "cpu,host=a v=1.0 1000000000").await;
    eng.selfmon_tick();

    // The U2 gate: what is stored agrees with /metrics, because it IS
    // /metrics — the sample is a conversion of that exact text.
    let (status, rows) = sql_in(
        &app,
        "_system",
        "SELECT timelake_lines_written_total FROM metrics \
         WHERE timelake_lines_written_total IS NOT NULL",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    let rows = rows.as_array().expect("rows");
    assert!(!rows.is_empty(), "no metric rows stored");
    // Float, not integer: an exposition sample is a float by definition, and
    // the converter emits the value verbatim rather than guessing a type per
    // metric — guessing would give one column two types across samples.
    assert_eq!(
        rows[0]["timelake_lines_written_total"].as_f64().unwrap(),
        1.0,
        "the stored value must match the counter"
    );
}

#[tokio::test]
async fn self_monitoring_does_not_inflate_user_ingest_numbers() {
    // `timelake_lines_written_total` is what Gauge's throughput is compared
    // against. If the server's own telemetry counted toward it, the
    // baseline would drift upward for reasons that have nothing to do with
    // the workload being measured.
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(eng.clone());

    write(&app, "cpu,host=a v=1.0 1000000000").await;
    let before = value_of(&metrics(&app).await, "timelake_lines_written_total").unwrap();
    assert_eq!(before, 1.0);

    // A tick writes many rows into _system...
    let stored = eng.selfmon_tick();
    assert!(
        stored > 1,
        "the sample should be several rows, got {stored}"
    );

    // ...and the user-ingest counter has not moved.
    let after = value_of(&metrics(&app).await, "timelake_lines_written_total").unwrap();
    assert_eq!(
        after, before,
        "self-monitoring rows must not count as user ingest"
    );
    // But they are counted where they belong.
    let written = value_of(&metrics(&app).await, "timelake_selfmon_written_total").unwrap();
    assert!(written > 0.0, "selfmon rows are counted separately");
}

#[tokio::test]
async fn a_tick_with_nothing_queued_is_still_a_sample() {
    // The gauge stream must not go silent on an idle node — a flat line and
    // an absent line mean very different things on a dashboard.
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(eng.clone());

    let first = eng.selfmon_tick();
    assert!(first > 0, "an idle node still samples its gauges");

    let (status, rows) = sql_in(&app, "_system", "SELECT COUNT(*) AS n FROM metrics").await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    assert!(rows.as_array().unwrap()[0]["n"].as_i64().unwrap() > 0);
}

#[tokio::test]
async fn a_querier_stores_nothing_but_still_exposes_metrics() {
    // CL-3: a querier owns no data and refuses writes, so there is nowhere
    // local to put a sample and a buffer there would never be flushed.
    // /metrics has to keep working, since it becomes the only surface.
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(eng.clone());
    eng.set_read_only();

    assert_eq!(eng.selfmon_tick(), 0, "a querier must not write to itself");

    let text = metrics(&app).await;
    assert!(
        value_of(&text, "timelake_query_in_flight").is_some(),
        "/metrics must still answer on a querier"
    );
}
