use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let addr = std::env::var("TIMELAKE_ADDR").unwrap_or_else(|_| "0.0.0.0:1963".to_string());
    let data_dir = timelake_server::data_dir_from_env();
    let cfg = timelake_server::config_from_env();

    tracing::info!(%addr, data_dir = %data_dir.display(), ?cfg, "timelakedb M3 starting");
    let engine = timelake_server::Engine::open(&data_dir, cfg).expect("open engine (recovery)");

    // Maintenance ticks (ARCHITECTURE §7): flush every 10 s, compaction
    // every 30 s, retention every 60 s — sequential on one blocking task
    // so background work never stacks up on itself.
    let maint = Arc::clone(&engine);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(10));
        let mut n: u64 = 0;
        loop {
            tick.tick().await;
            n += 1;
            let e = Arc::clone(&maint);
            let compact = n.is_multiple_of(3);
            let retention = n.is_multiple_of(6);
            // Each stage is independent. A failing flush used to abort the
            // rest of the tick, so one unflushable table stopped compaction
            // and retention for every table on the node.
            let res = tokio::task::spawn_blocking(move || {
                if let Err(err) = e.flush_if_needed() {
                    tracing::error!(%err, stage = "flush", "maintenance stage failed");
                }
                if compact && let Err(err) = e.compact_once() {
                    tracing::error!(%err, stage = "compact", "maintenance stage failed");
                }
                if retention && let Err(err) = e.enforce_retention() {
                    tracing::error!(%err, stage = "retention", "maintenance stage failed");
                }
                e.run_gc();
            })
            .await;
            if let Err(join) = res {
                tracing::error!(%join, "maintenance task panicked");
            }
        }
    });

    // Flight SQL (FR-8) on its own gRPC port
    let flight_addr: std::net::SocketAddr = std::env::var("TIMELAKE_FLIGHT_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:1964".to_string())
        .parse()
        .expect("TIMELAKE_FLIGHT_ADDR must be host:port");
    let flight_backend: Arc<dyn timelake_flight::SqlBackend> = engine.clone();

    // SEC-3: TLS on BOTH listeners when cert+key are configured; the
    // fixtures and bench stay plaintext by simply not setting these.
    let tls_cert = std::env::var("TIMELAKE_TLS_CERT").ok();
    let tls_key = std::env::var("TIMELAKE_TLS_KEY").ok();
    match (tls_cert, tls_key) {
        (Some(cert), Some(key)) => {
            let rot = timelake_tls::RotatingCert::load(cert.as_ref(), key.as_ref())
                .expect("initial TLS cert load must succeed (no last-good yet)");
            engine.set_tls(Arc::clone(&rot));
            // Floor is TLS 1.3; TIMELAKE_TLS_MIN=1.2 lowers it (SEC-3).
            let allow_tls12 = std::env::var("TIMELAKE_TLS_MIN").as_deref() == Ok("1.2");
            tracing::info!(
                expires_in_secs = rot.expires_in_secs(),
                min_version = if allow_tls12 { "1.2" } else { "1.3" },
                "TLS enabled on HTTP and Flight SQL listeners"
            );

            // File watcher: certbot-style renewals just overwrite the
            // files; poll mtimes (2 s), debounce, reload. A failed reload
            // alarms and keeps last-good — it must NOT stop the watcher.
            let watcher = Arc::clone(&rot);
            tokio::spawn(async move {
                let mut last = watcher.mtimes();
                let mut tick = tokio::time::interval(Duration::from_secs(2));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    let now = watcher.mtimes();
                    if now.is_some() && now != last {
                        // Debounce: cert and key are two files; let the
                        // writer finish both before validating the pair.
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        let settled = watcher.mtimes();
                        let w = Arc::clone(&watcher);
                        let _ = tokio::task::spawn_blocking(move || w.reload()).await;
                        last = settled;
                    } else {
                        last = now;
                    }
                }
            });

            // Flight SQL over TLS (gRPC wants ALPN h2).
            let flight_tls = rot.server_config(allow_tls12, &[b"h2".as_slice()]);
            tokio::spawn(async move {
                if let Err(e) =
                    timelake_flight::serve_tls(flight_backend, flight_addr, flight_tls).await
                {
                    tracing::error!(error = %e, "flight sql (TLS) server exited");
                }
            });

            // HTTP over TLS. axum-server drives hyper over our rustls
            // config; the resolver inside it is the rotation point.
            let http_tls = rot.server_config(allow_tls12, &[b"h2".as_slice(), b"http/1.1"]);
            let sock_addr: std::net::SocketAddr = addr
                .parse()
                .expect("TIMELAKE_ADDR must be host:port under TLS");
            let app = timelake_server::app_with_tls_admin(engine, rot);
            axum_server::bind_rustls(
                sock_addr,
                axum_server::tls_rustls::RustlsConfig::from_config(http_tls),
            )
            .serve(app.into_make_service())
            .await
            .expect("server error (TLS)");
        }
        (None, None) => {
            tokio::spawn(async move {
                if let Err(e) = timelake_flight::serve(flight_backend, flight_addr).await {
                    tracing::error!(error = %e, "flight sql server exited");
                }
            });
            let listener = TcpListener::bind(&addr).await.expect("bind listen address");
            axum::serve(listener, timelake_server::app(engine))
                .await
                .expect("server error");
        }
        _ => panic!("TIMELAKE_TLS_CERT and TIMELAKE_TLS_KEY must be set together"),
    }
}
