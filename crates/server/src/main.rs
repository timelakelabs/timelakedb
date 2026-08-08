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
    let listener = TcpListener::bind(&addr).await.expect("bind listen address");
    tracing::info!(%addr, "timelorddb M0 stub listening");
    axum::serve(listener, timelord_server::app())
        .await
        .expect("server error");
}
