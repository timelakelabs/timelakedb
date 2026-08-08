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

    let engine =
        timelord_server::Engine::open(&data_dir, cfg).expect("open engine (recovery)");
    tracing::info!(%addr, data_dir = %data_dir.display(), ?cfg, "timelorddb M2 listening");

    // L0 flush tick (ARCHITECTURE §7)
    let flusher = Arc::clone(&engine);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(10));
        loop {
            tick.tick().await;
            let e = Arc::clone(&flusher);
            let res = tokio::task::spawn_blocking(move || e.flush_if_needed()).await;
            match res {
                Ok(Ok(0)) => {}
                Ok(Ok(files)) => tracing::info!(files, "flush tick"),
                Ok(Err(err)) => tracing::error!(%err, "flush failed"),
                Err(join) => tracing::error!(%join, "flush task panicked"),
            }
        }
    });

    let listener = TcpListener::bind(&addr).await.expect("bind listen address");
    axum::serve(listener, timelord_server::app(engine))
        .await
        .expect("server error");
}
