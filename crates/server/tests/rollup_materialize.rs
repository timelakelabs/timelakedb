//! R-2 rollups, phase 2: materialisation end to end (ARCHITECTURE §18.3).
//! Writes source data and finalizes buckets into the target, checking the
//! target holds the correct downsampled rows — exactly, exactly-once (a
//! re-run writes nothing and never duplicates a bucket), and with the
//! late-data grace working both ways: a row that lands before its bucket is
//! sealed is counted, one that lands after is not.
//!
//! The wall clock is supplied explicitly via `materialize_rollups_at`, so
//! "this bucket has aged past lookback" is deterministic rather than a race.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

// One 60s bucket in nanoseconds, and a bucket-aligned base timestamp far
// enough in the past that any realistic lookback still seals it.
const IV: i64 = 60_000_000_000;
const LOOKBACK_S: u64 = 300; // 5 * 60s, so the horizon math stays clean
const B0: i64 = 28_000_000 * IV; // aligned to a bucket boundary by construction

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

async fn query(app: &axum::Router, sql: &str) -> serde_json::Value {
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/sql")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"db": "poc", "sql": sql}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
}

fn def() -> timelake_server::RollupDef {
    use timelake_server::{RollupAgg, RollupFn};
    let agg = |f, tgt: &str| RollupAgg {
        function: f,
        source_column: "value".into(),
        target_column: tgt.into(),
        quantile: None,
    };
    timelake_server::RollupDef {
        db: "poc".into(),
        name: "sensor_1m".into(),
        source: "sensor_reading".into(),
        target: "sensor_reading_1m".into(),
        interval_secs: 60,
        lookback_secs: LOOKBACK_S,
        group_by: vec!["host".into()],
        aggregations: vec![
            agg(RollupFn::Avg, "v_avg"),
            agg(RollupFn::Max, "v_max"),
            agg(RollupFn::Min, "v_min"),
            agg(RollupFn::Sum, "v_sum"),
            agg(RollupFn::Count, "v_count"),
            agg(RollupFn::First, "v_first"),
            agg(RollupFn::Last, "v_last"),
        ],
        filter: None,
    }
}

#[tokio::test]
async fn a_rollup_seals_buckets_exactly_once_with_a_working_late_data_grace() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(Arc::clone(&eng));

    // Two source rows in bucket B0, host=h1: 10 then 20.
    assert_eq!(
        write(
            &app,
            &format!(
                "sensor_reading,host=h1 value=10 {}\nsensor_reading,host=h1 value=20 {}",
                B0 + 1_000_000_000,
                B0 + 2_000_000_000
            )
        )
        .await,
        StatusCode::NO_CONTENT
    );
    eng.set_rollup(def()).unwrap();

    // (1) A pass whose clock leaves B0 still inside the lookback window seals
    // nothing — the bucket is held open for late data.
    let n_early = eng.materialize_rollups_at(B0 + IV + 1).await.unwrap();
    assert_eq!(
        n_early, 0,
        "a bucket inside lookback must not be sealed yet"
    );

    // (2) Late row into B0 while it is still open — must be counted.
    assert_eq!(
        write(
            &app,
            &format!("sensor_reading,host=h1 value=30 {}", B0 + 3_000_000_000)
        )
        .await,
        StatusCode::NO_CONTENT
    );

    // (3) Now seal, with a clock well past B0 + lookback. B0 finalizes once,
    // over all three rows present at seal time.
    let now = B0 + LOOKBACK_S as i64 * 1_000_000_000 + 5 * IV;
    let n = eng.materialize_rollups_at(now).await.unwrap();
    assert!(n >= 1, "the aged-out bucket wrote no rows");

    let rows = query(
        &app,
        "SELECT host, v_avg, v_max, v_min, v_sum, v_count, v_first, v_last, time \
         FROM sensor_reading_1m ORDER BY time",
    )
    .await;
    assert_eq!(
        rows.as_array().map(|a| a.len()),
        Some(1),
        "one bucket: {rows}"
    );
    let r = &rows[0];
    assert_eq!(r["host"], "h1");
    assert_eq!(r["v_avg"], 20.0, "late row counted: (10+20+30)/3");
    assert_eq!(r["v_max"], 30.0);
    assert_eq!(r["v_min"], 10.0);
    assert_eq!(r["v_sum"], 60.0);
    assert_eq!(r["v_count"], 3);
    assert_eq!(r["v_first"], 10.0, "first = earliest by time");
    assert_eq!(r["v_last"], 30.0, "last = latest by time");

    // (4) Exactly-once: a second pass at the same clock finalizes nothing —
    // the target's own max(time) is the watermark, so the sealed bucket is
    // never rewritten and never doubled. No compaction is involved.
    let n_again = eng.materialize_rollups_at(now).await.unwrap();
    assert_eq!(n_again, 0, "re-running sealed the same bucket again");
    let count = query(&app, "SELECT COUNT(*) AS n FROM sensor_reading_1m").await;
    assert_eq!(count[0]["n"], 1, "the bucket was duplicated");

    // (5) The honest limitation: a row arriving after the bucket is sealed is
    // not reflected. It changes nothing, rather than corrupting the sealed
    // aggregate or resurrecting the bucket.
    assert_eq!(
        write(
            &app,
            &format!("sensor_reading,host=h1 value=100 {}", B0 + 4_000_000_000)
        )
        .await,
        StatusCode::NO_CONTENT
    );
    let n_post = eng.materialize_rollups_at(now).await.unwrap();
    assert_eq!(n_post, 0, "a post-seal late row must not reopen the bucket");
    let after = query(&app, "SELECT v_avg, v_count FROM sensor_reading_1m").await;
    assert_eq!(
        after[0]["v_count"], 3,
        "post-seal row leaked into the bucket"
    );
    assert_eq!(after[0]["v_avg"], 20.0);
}

#[tokio::test]
async fn the_extended_grammar_filters_counts_distinct_and_takes_a_percentile() {
    use timelake_server::{RollupAgg, RollupDef, RollupFn};
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(Arc::clone(&eng));

    // host=h1 in bucket B0: four eu rows {10,10,20,30} and one us row that the
    // filter must drop. If the filter leaked, v_avg would be 213.8 and v_ndv 4.
    write(
        &app,
        &format!(
            "sensor_reading,host=h1,region=eu value=10 {}\n\
             sensor_reading,host=h1,region=eu value=10 {}\n\
             sensor_reading,host=h1,region=eu value=20 {}\n\
             sensor_reading,host=h1,region=eu value=30 {}\n\
             sensor_reading,host=h1,region=us value=999 {}",
            B0 + 1_000_000_000,
            B0 + 2_000_000_000,
            B0 + 3_000_000_000,
            B0 + 4_000_000_000,
            B0 + 5_000_000_000,
        ),
    )
    .await;

    let agg = |f, tgt: &str, q: Option<f64>| RollupAgg {
        function: f,
        source_column: "value".into(),
        target_column: tgt.into(),
        quantile: q,
    };
    eng.set_rollup(RollupDef {
        db: "poc".into(),
        name: "eu_1m".into(),
        source: "sensor_reading".into(),
        target: "sensor_reading_eu_1m".into(),
        interval_secs: 60,
        lookback_secs: LOOKBACK_S,
        group_by: vec!["host".into()],
        aggregations: vec![
            agg(RollupFn::Avg, "v_avg", None),
            agg(RollupFn::CountDistinct, "v_ndv", None),
            agg(RollupFn::Percentile, "v_p50", Some(0.5)),
            // p1.0 exercises the boundary quantile: it must render as the
            // float 1.0, not the integer 1, or approx_percentile_cont won't
            // plan. Its value is the eu max, 30.
            agg(RollupFn::Percentile, "v_p100", Some(1.0)),
        ],
        filter: Some("region = 'eu'".into()),
    })
    .unwrap();

    let now = B0 + LOOKBACK_S as i64 * 1_000_000_000 + 5 * IV;
    let n = eng.materialize_rollups_at(now).await.unwrap();
    assert!(n >= 1, "no rows sealed");

    let rows = query(
        &app,
        "SELECT host, v_avg, v_ndv, v_p50, v_p100 FROM sensor_reading_eu_1m",
    )
    .await;
    assert_eq!(
        rows.as_array().map(|a| a.len()),
        Some(1),
        "one bucket: {rows}"
    );
    let r = &rows[0];
    assert_eq!(r["host"], "h1");
    assert_eq!(
        r["v_avg"], 17.5,
        "us row not filtered out: mean of {{10,10,20,30}}"
    );
    assert_eq!(r["v_ndv"], 3, "count(distinct) over {{10,20,30}}");
    let p50 = r["v_p50"].as_f64().unwrap();
    assert!(
        (10.0..=20.0).contains(&p50),
        "median of {{10,10,20,30}} should sit between 10 and 20, got {p50}"
    );
    let p100 = r["v_p100"].as_f64().unwrap();
    assert!(
        (20.0..=30.0).contains(&p100),
        "p1.0 of the eu rows is the max, 30 — and it must have planned at all: {p100}"
    );

    // Same exactly-once guarantee holds for the extended grammar.
    let n_again = eng.materialize_rollups_at(now).await.unwrap();
    assert_eq!(n_again, 0, "re-running sealed the bucket again");
    let count = query(&app, "SELECT COUNT(*) AS n FROM sensor_reading_eu_1m").await;
    assert_eq!(count[0]["n"], 1);
}

#[tokio::test]
async fn an_empty_group_by_seals_every_source_tag() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(Arc::clone(&eng));

    // Two hosts, two regions — four series, all in bucket B0.
    write(
        &app,
        &format!(
            "sensor_reading,host=h1,region=r0 value=1 {t}\n\
             sensor_reading,host=h2,region=r0 value=2 {t}\n\
             sensor_reading,host=h1,region=r1 value=3 {t}\n\
             sensor_reading,host=h2,region=r1 value=4 {t}",
            t = B0 + 1_000_000_000
        ),
    )
    .await;

    let mut d = def();
    d.group_by = vec![]; // empty ⇒ resolve to every tag (host, region)
    eng.set_rollup(d).unwrap();

    let now = B0 + LOOKBACK_S as i64 * 1_000_000_000 + 5 * IV;
    eng.materialize_rollups_at(now).await.unwrap();

    let rows = query(
        &app,
        "SELECT host, region, v_avg FROM sensor_reading_1m ORDER BY host, region",
    )
    .await;
    assert_eq!(
        rows.as_array().map(|a| a.len()),
        Some(4),
        "one row per (host, region): {rows}"
    );
    // Both tags survived — a group_by that dropped one would collapse to 2.
    assert_eq!(rows[0]["host"], "h1");
    assert_eq!(rows[0]["region"], "r0");
    assert_eq!(rows[0]["v_avg"], 1.0);
    assert_eq!(rows[3]["v_avg"], 4.0);
}
