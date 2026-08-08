//! M0 gate, in-process: the endpoints the bench adapter and Telegraf
//! probe must answer correctly before any socket exists.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn health_ping_and_honest_501s() {
    let app = timelord_server::app();

    let res = app
        .clone()
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let res = app
        .clone()
        .oneshot(Request::get("/ping").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert!(res.headers().contains_key("x-timelorddb-version"));

    for path in ["/write", "/api/v2/write", "/api/v3/write_lp", "/api/sql"] {
        let res = app
            .clone()
            .oneshot(Request::post(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED, "{path}");
    }
}
