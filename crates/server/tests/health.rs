//! M0 gate, in-process: the endpoints the bench adapter and Telegraf
//! probe must answer correctly before any socket exists — including the
//! payload fields the adapter's version() contractually reads.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

#[tokio::test]
async fn health_payload_is_the_adapter_contract() {
    let res = timelord_server::app()
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    // bench/backends/timelorddb.py healthy() needs 2xx; version() reads
    // .version; the fields below are the wire contract — do not rename.
    assert_eq!(v["status"], "pass");
    assert_eq!(v["name"], "timelorddb");
    assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
}

#[tokio::test]
async fn ping_is_204_with_version_header() {
    let res = timelord_server::app()
        .oneshot(Request::get("/ping").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        res.headers()["x-timelorddb-version"],
        env!("CARGO_PKG_VERSION")
    );
}

#[tokio::test]
async fn metrics_serves_prometheus_exposition() {
    let res = timelord_server::app()
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(
        text.starts_with('#'),
        "expected Prometheus exposition format, got: {text:?}"
    );
}

#[tokio::test]
async fn unimplemented_endpoints_answer_honest_501s() {
    for path in ["/write", "/api/v2/write", "/api/v3/write_lp", "/api/sql"] {
        let res = timelord_server::app()
            .oneshot(Request::post(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED, "{path}");
    }
}
