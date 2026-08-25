//! timelakedb#56: Prometheus `remote_write` lands on the same engine write
//! path line protocol uses, and reads back **identically** to the same data
//! sent as line protocol — the acceptance criterion for the on-ramp.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use timelake_prometheus::{Label, Sample, TimeSeries, WriteRequest};
use tower::ServiceExt;

fn engine(dir: &std::path::Path) -> Arc<timelake_server::Engine> {
    timelake_server::Engine::open(
        dir,
        timelake_server::EngineConfig {
            flush_rows: 1_000_000,
            flush_age_secs: u64::MAX,
            wal_max_bytes: u64::MAX,
            ..Default::default()
        },
    )
    .unwrap()
}

fn lbl(n: &str, v: &str) -> Label {
    Label {
        name: n.to_string(),
        value: v.to_string(),
    }
}

fn sample(value: f64, ts_ms: i64) -> Sample {
    Sample {
        value,
        timestamp: ts_ms,
    }
}

/// Encode + snappy-compress a WriteRequest the way a Prometheus server would.
fn frame(req: &WriteRequest) -> Vec<u8> {
    let proto = ::prost::Message::encode_to_vec(req);
    snap::raw::Encoder::new().compress_vec(&proto).unwrap()
}

async fn remote_write(app: &axum::Router, db: &str, body: Vec<u8>) -> StatusCode {
    app.clone()
        .oneshot(
            Request::post(format!("/api/v1/write?db={db}"))
                .header("content-encoding", "snappy")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

async fn write_lp(app: &axum::Router, db: &str, precision: &str, lp: &str) -> StatusCode {
    app.clone()
        .oneshot(
            Request::post(format!("/write?db={db}&precision={precision}"))
                .header("content-type", "text/plain")
                .body(Body::from(lp.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

async fn sql(app: &axum::Router, db: &str, q: &str) -> serde_json::Value {
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
    assert_eq!(res.status(), StatusCode::OK, "query failed: {q}");
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn remote_write_reads_back_identically_to_line_protocol() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(eng.clone());

    // Two series, one with two samples — the shape a real scrape sends.
    let req = WriteRequest {
        timeseries: vec![
            TimeSeries {
                labels: vec![
                    lbl("__name__", "up"),
                    lbl("job", "node"),
                    lbl("instance", "h1"),
                ],
                samples: vec![
                    sample(1.0, 1_700_000_000_000),
                    sample(0.0, 1_700_000_060_000),
                ],
            },
            TimeSeries {
                labels: vec![lbl("__name__", "node_cpu"), lbl("cpu", "0")],
                samples: vec![sample(0.5, 1_700_000_000_000)],
            },
        ],
    };
    assert_eq!(
        remote_write(&app, "prom", frame(&req)).await,
        StatusCode::NO_CONTENT
    );

    // The SAME data as line protocol into a second db. remote_write timestamps
    // are milliseconds, so the LP arm uses precision=ms — both land the same ns.
    let lp = "up,job=node,instance=h1 value=1 1700000000000\n\
              up,job=node,instance=h1 value=0 1700000060000\n\
              node_cpu,cpu=0 value=0.5 1700000000000";
    assert_eq!(write_lp(&app, "lp", "ms", lp).await, StatusCode::NO_CONTENT);

    // Row for row, the two arms agree — mapping (__name__→measurement,
    // label→tag, value→`value`) and the ms→ns timestamp are both right.
    for q in [
        "SELECT job, instance, value FROM up ORDER BY time",
        "SELECT cpu, value FROM node_cpu ORDER BY time",
        "SELECT COUNT(*) AS n FROM up",
    ] {
        assert_eq!(
            sql(&app, "prom", q).await,
            sql(&app, "lp", q).await,
            "remote_write and line-protocol arms disagree on: {q}"
        );
    }

    // And the concrete values, so the test says what "identical" means.
    assert_eq!(
        sql(&app, "prom", "SELECT value FROM up ORDER BY time").await,
        serde_json::json!([{"value": 1.0}, {"value": 0.0}])
    );
}

#[tokio::test]
async fn a_stale_only_scrape_is_a_204_noop_and_a_bad_body_is_400() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let app = timelake_server::app(eng.clone());

    // A scrape whose only sample is a staleness NaN decodes to zero rows.
    let stale = WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![lbl("__name__", "up")],
            samples: vec![sample(f64::NAN, 1)],
        }],
    };
    assert_eq!(
        remote_write(&app, "prom", frame(&stale)).await,
        StatusCode::NO_CONTENT,
        "a stale-only scrape is a successful no-op, not an error"
    );

    // Garbage that is not a snappy frame is a client error, not a 500.
    assert_eq!(
        remote_write(&app, "prom", b"definitely not snappy".to_vec()).await,
        StatusCode::BAD_REQUEST
    );
}
