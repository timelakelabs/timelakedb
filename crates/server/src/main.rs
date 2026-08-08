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

    let addr = std::env::var("TIMELORD_ADDR").unwrap_or_else(|_| "0.0.0.0:1963".to_string());
    let data_dir = timelord_server::data_dir_from_env();
    let cfg = timelord_server::config_from_env();

    tracing::info!(%addr, data_dir = %data_dir.display(), ?cfg, "timelorddb M3 starting");
    let engine =
        timelord_server::Engine::open(&data_dir, cfg).expect("open engine (recovery)");

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
            let compact = n % 3 == 0;
            let retention = n % 6 == 0;
            let res = tokio::task::spawn_blocking(move || -> Result<(), String> {
                e.flush_if_needed()?;
                if compact {
                    e.compact_once()?;
                }
                if retention {
                    e.enforce_retention()?;
                }
                Ok(())
            })
            .await;
            match res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => tracing::error!(%err, "maintenance tick failed"),
                Err(join) => tracing::error!(%join, "maintenance task panicked"),
            }
        }
    });

    // Flight SQL (FR-8) on its own gRPC port
    let flight_addr: std::net::SocketAddr = std::env::var("TIMELORD_FLIGHT_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:1964".to_string())
        .parse()
        .expect("TIMELORD_FLIGHT_ADDR must be host:port");
    let flight_backend: Arc<dyn timelord_flight::SqlBackend> = engine.clone();
    tokio::spawn(async move {
        if let Err(e) = timelord_flight::serve(flight_backend, flight_addr).await {
            tracing::error!(error = %e, "flight sql server exited");
        }
    });

    let listener = TcpListener::bind(&addr).await.expect("bind listen address");
    axum::serve(listener, timelord_server::app(engine))
        .await
        .expect("server error");
}
