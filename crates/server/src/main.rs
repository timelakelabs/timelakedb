use std::path::PathBuf;

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
    let data_dir = PathBuf::from(
        std::env::var("TIMELORD_DATA_DIR").unwrap_or_else(|_| "./data".to_string()),
    );
    // RR-1 pool for queries; 20%-of-RAM autodetection arrives with the
    // memory-budget work (M4) — until then this is explicit config.
    let query_mem: usize = std::env::var("TIMELORD_QUERY_MEM_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_073_741_824);

    let engine =
        timelord_server::Engine::open(&data_dir, query_mem).expect("open engine (WAL replay)");
    let listener = TcpListener::bind(&addr).await.expect("bind listen address");
    tracing::info!(%addr, data_dir = %data_dir.display(), query_mem, "timelorddb M1 listening");
    axum::serve(listener, timelord_server::app(engine))
        .await
        .expect("server error");
}
