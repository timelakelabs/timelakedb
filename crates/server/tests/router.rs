//! The router's forwarding surface, against stub nodes.
//!
//! The sharding *arithmetic* is unit-tested inside `router.rs`; what this
//! file pins is the behaviour that only appears once something is on the
//! other end of a socket: that a write body reaches the right node
//! unaltered, that a poison line writes nothing anywhere, that a query is
//! handed to a querier with its credentials intact, and that a dead querier
//! costs one retry instead of half the queries.
//!
//! Stub nodes rather than real engines on purpose — the question here is
//! what the router sends and how it reacts, not what an ingester does with
//! it. The end-to-end version is drilled live
//! (`docs/evidence/router-sharding-drill.log`,
//! `docs/evidence/cl3-querier-drill.log`).

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use timelake_server::router::{RouterState, router_app};
use tower::ServiceExt;

/// What a stub node was asked to do.
#[derive(Default)]
struct Received {
    bodies: Vec<String>,
    authorization: Option<String>,
    authorizations: Option<String>,
}

type Log = Arc<Mutex<Received>>;

/// A node that records what it receives and answers `reply`.
async fn stub_node(reply: &'static str, status: StatusCode) -> (String, Log) {
    let log: Log = Arc::new(Mutex::new(Received::default()));
    let state = Arc::clone(&log);
    let app = axum::Router::new()
        .route(
            "/api/v3/write_lp",
            axum::routing::post(
                |axum::extract::State(log): axum::extract::State<Log>,
                 headers: axum::http::HeaderMap,
                 body: axum::body::Bytes| async move {
                    let mut r = log.lock().unwrap();
                    r.bodies.push(String::from_utf8_lossy(&body).into_owned());
                    // Recorded per write so a test can see whether the
                    // client's credential reached the node (#37).
                    r.authorization = headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string);
                    StatusCode::NO_CONTENT
                },
            ),
        )
        .route(
            "/api/sql",
            axum::routing::post(
                move |axum::extract::State(log): axum::extract::State<Log>,
                      headers: axum::http::HeaderMap,
                      body: axum::body::Bytes| async move {
                    {
                        let mut r = log.lock().unwrap();
                        r.bodies.push(String::from_utf8_lossy(&body).into_owned());
                        r.authorization = headers
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string);
                        r.authorizations = headers
                            .get("x-timelake-authorizations")
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string);
                    }
                    (status, reply)
                },
            ),
        )
        // The stub must take what the router forwards, or a large-body
        // test would measure the stub's limit rather than the router's.
        .layer(axum::extract::DefaultBodyLimit::max(64 << 20))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (addr, log)
}

/// An address with nothing listening on it.
fn dead_address() -> String {
    "127.0.0.1:1".to_string()
}

async fn post_sql(app: &axum::Router, with_credentials: bool) -> (StatusCode, String) {
    let mut req = Request::post("/api/sql").header("content-type", "application/json");
    if with_credentials {
        req = req
            .header("authorization", "Bearer tldb_secret")
            .header("x-timelake-authorizations", "ops,audit");
    }
    let res = app
        .clone()
        .oneshot(
            req.body(Body::from(
                r#"{"db":"poc","sql":"SELECT COUNT(*) AS n FROM cpu"}"#,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test]
async fn a_gzip_write_body_is_decompressed_validated_and_forwarded_plain() {
    // Tributary gzips by default, and stock Telegraf's influxdb_v2 output
    // speaks gzip too. The single-node endpoint decompresses; the router —
    // which sells itself as the drop-in single write endpoint those
    // clients keep pointing at — read the raw gzip bytes as line protocol
    // and rejected every batch with 400. The agent then did exactly what
    // it should with a rejected batch: bisected it hunting the poison
    // line, and quarantined all 40,000 lines of a correct corpus, per
    // stream, with `shipped: 0` (Catchment router-tributary-exactness,
    // 2026-08-13). A client that works against one node must work against
    // the router, or the router is not a drop-in anything.
    let (a, log_a) = stub_node("[]", StatusCode::OK).await;
    let (b, log_b) = stub_node("[]", StatusCode::OK).await;
    let state = Arc::new(RouterState::new(vec![
        ("ingester-a".into(), a),
        ("ingester-b".into(), b),
    ]));
    let app = router_app(state);

    let body = "cpu,host=h1 v=1i 1\nmem,host=h1 v=2i 2\n";
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    std::io::Write::write_all(&mut enc, body.as_bytes()).unwrap();
    let gz = enc.finish().unwrap();

    let res = app
        .oneshot(
            Request::post("/api/v3/write_lp?db=poc")
                .header("content-type", "text/plain; charset=utf-8")
                .header("content-encoding", "gzip")
                .body(Body::from(gz))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        StatusCode::NO_CONTENT,
        "a gzip write must pass through the router exactly as it passes \
         into a single node"
    );

    // Give the stub servers a beat to record the forwards.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Every line arrived somewhere, in PLAIN text: the router re-chunks
    // bodies per shard, so what it forwards must parse on its own, with
    // no memory of the original request's headers.
    let all: String = {
        let a = log_a.lock().unwrap().bodies.concat();
        let b = log_b.lock().unwrap().bodies.concat();
        a + &b
    };
    assert!(
        all.contains("cpu,host=h1 v=1i 1"),
        "cpu line missing: {all:?}"
    );
    assert!(
        all.contains("mem,host=h1 v=2i 2"),
        "mem line missing: {all:?}"
    );
}

#[tokio::test]
async fn a_corrupt_gzip_body_is_a_400_that_says_so() {
    // The failure mode must name itself: "bad gzip body", not "line has
    // no measurement" about bytes the client never wrote.
    let (a, _log) = stub_node("[]", StatusCode::OK).await;
    let state = Arc::new(RouterState::new(vec![("ingester-a".into(), a)]));
    let app = router_app(state);
    let res = app
        .oneshot(
            Request::post("/api/v3/write_lp?db=poc")
                .header("content-encoding", "gzip")
                .body(Body::from(&b"\x1f\x8b\x08\x00garbage"[..]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let msg = String::from_utf8_lossy(&bytes);
    assert!(
        msg.contains("gzip"),
        "the reason must name gzip, got: {msg}"
    );
}

#[tokio::test]
async fn a_query_reaches_a_querier_and_its_answer_comes_back_verbatim() {
    let (addr, log) = stub_node(r#"[{"n":42}]"#, StatusCode::OK).await;
    let app = router_app(Arc::new(RouterState::with_queriers(
        vec![("ing-a".into(), "127.0.0.1:9".into())],
        vec![("qry-a".into(), addr)],
    )));

    let (status, body) = post_sql(&app, true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, r#"[{"n":42}]"#, "the querier's answer, untouched");

    let r = log.lock().unwrap();
    assert!(
        r.bodies[0].contains("SELECT COUNT(*)"),
        "the SQL body passes through unparsed: {:?}",
        r.bodies[0]
    );
    // The querier is where SEC-2 and SEC-4 are decided, so dropping these
    // would silently widen or narrow what the caller is allowed to see.
    assert_eq!(r.authorization.as_deref(), Some("Bearer tldb_secret"));
    assert_eq!(r.authorizations.as_deref(), Some("ops,audit"));
}

#[tokio::test]
async fn a_dead_querier_costs_a_retry_not_the_query() {
    // Round-robin without fall-through turns one dead node into a 50%
    // error rate. The dead one is first in id order, so it is tried first.
    let (addr, _log) = stub_node(r#"[{"n":7}]"#, StatusCode::OK).await;
    let app = router_app(Arc::new(RouterState::with_queriers(
        vec![("ing-a".into(), "127.0.0.1:9".into())],
        vec![
            ("qry-a-dead".into(), dead_address()),
            ("qry-b".into(), addr),
        ],
    )));

    for attempt in 0..4 {
        let (status, body) = post_sql(&app, false).await;
        assert_eq!(status, StatusCode::OK, "attempt {attempt} failed: {body}");
        assert_eq!(body, r#"[{"n":7}]"#);
    }
}

#[tokio::test]
async fn every_querier_being_unreachable_is_a_gateway_error_not_a_wrong_answer() {
    let app = router_app(Arc::new(RouterState::with_queriers(
        vec![("ing-a".into(), "127.0.0.1:9".into())],
        vec![
            ("qry-a".into(), dead_address()),
            ("qry-b".into(), dead_address()),
        ],
    )));
    let (status, body) = post_sql(&app, false).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("no querier"), "unhelpful: {body}");
}

#[tokio::test]
async fn a_queriers_own_error_is_returned_rather_than_retried_elsewhere() {
    // A status is an ANSWER — including a querier refusing to answer from
    // an incomplete cluster. Retrying that against a node with the same
    // view would turn a clear failure into a mystery.
    let (addr_a, log_a) = stub_node(
        "refusing to answer from an incomplete cluster",
        StatusCode::BAD_REQUEST,
    )
    .await;
    let (addr_b, log_b) = stub_node(r#"[{"n":0}]"#, StatusCode::OK).await;
    let app = router_app(Arc::new(RouterState::with_queriers(
        vec![("ing-a".into(), "127.0.0.1:9".into())],
        vec![("qry-a".into(), addr_a), ("qry-b".into(), addr_b)],
    )));

    let (status, body) = post_sql(&app, false).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "the refusal is passed on");
    assert!(body.contains("incomplete cluster"));
    assert_eq!(log_a.lock().unwrap().bodies.len(), 1);
    assert_eq!(
        log_b.lock().unwrap().bodies.len(),
        0,
        "the second querier must not have been asked"
    );
}

#[tokio::test]
async fn a_router_with_no_queriers_says_so_instead_of_asking_an_ingester() {
    let app = router_app(Arc::new(RouterState::new(vec![(
        "ing-a".into(),
        "127.0.0.1:9".into(),
    )])));
    let (status, body) = post_sql(&app, false).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert!(body.contains("no queriers"), "{body}");
}

#[tokio::test]
async fn a_write_body_is_sharded_and_forwarded_unaltered() {
    let (addr_a, log_a) = stub_node("", StatusCode::NO_CONTENT).await;
    let (addr_b, log_b) = stub_node("", StatusCode::NO_CONTENT).await;
    let app = router_app(Arc::new(RouterState::new(vec![
        ("ing-a".into(), addr_a),
        ("ing-b".into(), addr_b),
    ])));

    // Enough distinct measurements that both shards get some.
    let body: String = (0..20)
        .map(|i| {
            format!(
                "table_{i},host=h{i} v={i}i {}\n",
                1_786_179_600_000_000_000i64 + i
            )
        })
        .collect();
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v3/write_lp?db=poc")
                .header("content-type", "text/plain")
                .body(Body::from(body.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    let (a, b) = (log_a.lock().unwrap(), log_b.lock().unwrap());
    let received: String = a.bodies.concat() + &b.bodies.concat();
    assert!(
        !a.bodies.is_empty() && !b.bodies.is_empty(),
        "both shards used"
    );
    for line in body.lines() {
        assert!(
            received.contains(line),
            "line went missing in sharding: {line}"
        );
    }
    assert_eq!(
        received.lines().count(),
        20,
        "every line exactly once, no duplication across shards"
    );
}

#[tokio::test]
async fn a_body_over_two_megabytes_passes_through_the_router() {
    // FR-1 wants batches of 10 MB and up, through the single write
    // endpoint clients keep seeing — which in a cluster is this router.
    // The 2026-08-13 fix raised the limit on the data plane and the
    // internal listener from axum's 2 MiB default to TIMELAKE_MAX_BODY_BYTES
    // and missed this router, so for nine days a router-role node refused
    // exactly the batches the ingesters behind it were built to take (#36).
    let (addr, log) = stub_node("", StatusCode::NO_CONTENT).await;
    let app = router_app(Arc::new(RouterState::new(vec![("ing-a".into(), addr)])));

    let mut body = String::with_capacity(3 << 20);
    let mut i = 0i64;
    while body.len() < (3 << 20) {
        body.push_str(&format!(
            "cpu,host=h{i} v={i}i {}\n",
            1_786_179_600_000_000_000i64 + i
        ));
        i += 1;
    }
    assert!(
        body.len() > (2 << 20),
        "the probe must exceed axum's 2 MiB default"
    );
    let sent = body.lines().count();

    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v3/write_lp?db=poc")
                .header("content-type", "text/plain")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        StatusCode::NO_CONTENT,
        "a {sent}-line body over 2 MiB must pass through the router, not 413"
    );
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let received = log.lock().unwrap().bodies.concat();
    assert_eq!(
        received.lines().count(),
        sent,
        "every line forwarded exactly once"
    );
}

#[tokio::test]
async fn the_router_body_limit_is_the_configured_one_not_axums() {
    // The default moving from 2 MiB to 32 MiB is the visible half. The
    // half that keeps it honest is that the limit is a setting: a router
    // told to take 1 KiB refuses 4 KiB with 413 and forwards nothing, so a
    // router and its ingesters can never disagree about the cap by
    // accident — they read the same variable.
    let (addr, log) = stub_node("", StatusCode::NO_CONTENT).await;
    let app = router_app(Arc::new(
        RouterState::new(vec![("ing-a".into(), addr)]).with_max_body_bytes(1024),
    ));
    let body: String = (0..200)
        .map(|i| {
            format!(
                "cpu,host=h{i} v={i}i {}\n",
                1_786_179_600_000_000_000i64 + i
            )
        })
        .collect();
    assert!(body.len() > 4096);
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v3/write_lp?db=poc")
                .header("content-type", "text/plain")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PAYLOAD_TOO_LARGE);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        log.lock().unwrap().bodies.is_empty(),
        "nothing may be forwarded from a refused body"
    );
}

#[tokio::test]
async fn a_write_carries_the_clients_authorization_to_every_shard() {
    // The ingester is where a write is authenticated (SEC-4); the router
    // holds no token store and must not grow one. It used to forward a
    // shard with only `db` and `precision`, so behind a router an ingester
    // in `required` mode refused every write 401 and in `optional` mode
    // counted every router write as anonymous — the split an operator
    // flips on measured the router, not the clients (#37). The /api/sql
    // forward had carried the header from day one; this pins that writes
    // do too, to *each* shard, and that a write without one forwards none.
    let (addr_a, log_a) = stub_node("", StatusCode::NO_CONTENT).await;
    let (addr_b, log_b) = stub_node("", StatusCode::NO_CONTENT).await;
    let app = router_app(Arc::new(RouterState::new(vec![
        ("ing-a".into(), addr_a),
        ("ing-b".into(), addr_b),
    ])));
    let body: String = (0..20)
        .map(|i| {
            format!(
                "table_{i},host=h{i} v={i}i {}\n",
                1_786_179_600_000_000_000i64 + i
            )
        })
        .collect();

    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v3/write_lp?db=poc")
                .header("content-type", "text/plain")
                .header("authorization", "Bearer tldb_not-a-real-secret")
                .body(Body::from(body.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    for (name, log) in [("ing-a", &log_a), ("ing-b", &log_b)] {
        let r = log.lock().unwrap();
        assert!(!r.bodies.is_empty(), "{name} received a shard");
        assert_eq!(
            r.authorization.as_deref(),
            Some("Bearer tldb_not-a-real-secret"),
            "{name} must see the client's credential on its shard"
        );
    }

    // And the router invents nothing: an anonymous write arrives anonymous.
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v3/write_lp?db=poc")
                .header("content-type", "text/plain")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    for (name, log) in [("ing-a", &log_a), ("ing-b", &log_b)] {
        assert_eq!(
            log.lock().unwrap().authorization,
            None,
            "{name}: an anonymous write must not acquire a credential in transit"
        );
    }
}

#[tokio::test]
async fn a_poison_line_writes_nothing_anywhere() {
    // Atomicity across shards: the whole body is validated before any of it
    // is forwarded, so a bad line cannot leave half a batch written.
    let (addr_a, log_a) = stub_node("", StatusCode::NO_CONTENT).await;
    let (addr_b, log_b) = stub_node("", StatusCode::NO_CONTENT).await;
    let app = router_app(Arc::new(RouterState::new(vec![
        ("ing-a".into(), addr_a),
        ("ing-b".into(), addr_b),
    ])));

    let res = app
        .oneshot(
            Request::post("/api/v3/write_lp?db=poc")
                .header("content-type", "text/plain")
                .body(Body::from(" ,no-measurement v=1i\ngood,host=a v=1i 1\n"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(log_a.lock().unwrap().bodies.is_empty());
    assert!(log_b.lock().unwrap().bodies.is_empty());
}

#[tokio::test]
async fn a_write_with_no_database_is_refused_before_anything_is_forwarded() {
    let (addr, log) = stub_node("", StatusCode::NO_CONTENT).await;
    let app = router_app(Arc::new(RouterState::new(vec![("ing-a".into(), addr)])));
    let res = app
        .oneshot(
            Request::post("/api/v3/write_lp")
                .header("content-type", "text/plain")
                .body(Body::from("cpu,host=a v=1i 1\n"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(log.lock().unwrap().bodies.is_empty());
}

#[tokio::test]
async fn an_unreachable_ingester_is_a_retryable_error_and_the_metrics_say_so() {
    let app = router_app(Arc::new(RouterState::new(vec![(
        "ing-gone".into(),
        dead_address(),
    )])));
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v3/write_lp?db=poc")
                .header("content-type", "text/plain")
                .body(Body::from("cpu,host=a v=1i 1\n"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_GATEWAY);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let msg = String::from_utf8_lossy(&bytes);
    assert!(
        msg.contains("idempotent"),
        "the client must be told the retry is safe: {msg}"
    );

    let m = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body =
        String::from_utf8(m.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
    assert!(body.contains("timelake_router_forward_errors_total 1"));
    assert!(body.contains("timelake_router_ingesters 1"));
    assert!(body.contains("timelake_router_queriers 0"));
}
