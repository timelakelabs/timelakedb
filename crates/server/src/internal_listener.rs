//! Serving the intra-cluster listener (#72 phase 3).
//!
//! Plaintext where cluster TLS is not configured (drills, single-node dev);
//! **required** mTLS where it is — a peer with no client certificate, or one
//! signed outside the cluster CA, is refused at the handshake. The serving
//! cert and the cluster CA hot-rotate on file change (validate-before-swap,
//! last-good on a bad renewal), so a short-TTL renewal never restarts the node
//! (phase 4 drills the rotation under load).
//!
//! Deliberately independent of the data-plane listeners: they stay want mode
//! (stock Grafana/Telegraf hold no cert — AT-6), and the two build separate
//! client-auth configs from separate CAs — the data-plane client CA
//! (`TIMELAKE_TLS_CLIENT_CA`) and the cluster CA (`TIMELAKE_CLUSTER_CA`). The
//! gate is all three of `TIMELAKE_TLS_CERT` / `TIMELAKE_TLS_KEY` /
//! `TIMELAKE_CLUSTER_CA`; a CA set without a cert has already failed the node
//! at startup (see `peer_tls::PeerTls::from_env`), so reaching here with a CA
//! means the cert and key are present too.

use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;

use crate::Engine;

/// Spawn the intra-cluster listener bound to `addr`, serving `internal_router`
/// — over required mTLS when the cluster has TLS, plaintext otherwise.
pub fn spawn(engine: Arc<Engine>, addr: String) {
    let cert = std::env::var("TIMELAKE_TLS_CERT")
        .ok()
        .filter(|s| !s.is_empty());
    let key = std::env::var("TIMELAKE_TLS_KEY")
        .ok()
        .filter(|s| !s.is_empty());
    let cluster_ca = std::env::var("TIMELAKE_CLUSTER_CA")
        .ok()
        .filter(|s| !s.is_empty());
    match (cert, key, cluster_ca) {
        (Some(cert), Some(key), Some(ca)) => spawn_mtls(engine, addr, cert, key, ca),
        _ => spawn_plaintext(engine, addr),
    }
}

fn spawn_plaintext(engine: Arc<Engine>, addr: String) {
    let internal = crate::internal_router(engine);
    tokio::spawn(async move {
        let l = match TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(addr = %addr, error = %e, "internal listener bind failed");
                return;
            }
        };
        tracing::info!(addr = %addr, "CL2 internal replication listener up (plaintext)");
        if let Err(e) = axum::serve(l, internal).await {
            tracing::error!(error = %e, "internal replication listener exited");
        }
    });
}

/// Serve the intra-cluster listener over required mTLS with explicit cert,
/// key and cluster-CA paths. `spawn` calls this after reading the environment;
/// it is public so an integration test can drive it without touching env.
pub fn spawn_mtls(engine: Arc<Engine>, addr: String, cert: String, key: String, ca: String) {
    // The node's serving cert (same files as the data plane) and the cluster
    // CA that a peer's client cert must chain to. A bad load leaves the
    // listener down and loud rather than silently falling back to plaintext —
    // a plaintext intra-cluster port is exposure 10, which C3 exists to shut.
    let rot = match timelake_tls::RotatingCert::load(cert.as_ref(), key.as_ref()) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "internal listener: serving cert load failed — intra-cluster listener NOT started");
            return;
        }
    };
    let cluster_ca = match timelake_tls::RotatingClientCa::load(ca.as_ref()) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "internal listener: cluster CA load failed — intra-cluster listener NOT started");
            return;
        }
    };
    let sock: std::net::SocketAddr = match addr.parse() {
        Ok(s) => s,
        Err(_) => {
            tracing::error!(addr = %addr, "TIMELAKE_CLUSTER_ADDR must be host:port under mTLS");
            return;
        }
    };
    // Floor is TLS 1.3; TIMELAKE_TLS_MIN=1.2 lowers it, same knob as the data
    // plane (SEC-3).
    let allow_tls12 = std::env::var("TIMELAKE_TLS_MIN").as_deref() == Ok("1.2");

    // Rotation: poll mtimes (2 s, debounced) and reload in place. The built
    // config reaches the fresh cert through the RotatingCert resolver and the
    // fresh anchors through the client-CA ArcSwap, so nothing rebuilds and an
    // in-flight replication POST on an established connection is never dropped.
    // This is a second watcher over the same cert files as the data plane's,
    // deliberately: sharing one would mean loading the cert before the role
    // split up in main, and the redundant 2 s poll costs nothing.
    spawn_watcher(Arc::clone(&rot), Arc::clone(&cluster_ca));

    engine.mark_cluster_mtls_required();
    let config = rot.server_config_requiring_client_ca(
        allow_tls12,
        &[b"h2".as_slice(), b"http/1.1".as_slice()],
        cluster_ca,
    );
    // Capture the peer's identity the way the data plane does, so it is
    // available for future per-peer authz — the require handshake already
    // guarantees every peer here is carded.
    let acceptor =
        crate::tls_identity::IdentityAcceptor::new(axum_server::tls_rustls::RustlsAcceptor::new(
            axum_server::tls_rustls::RustlsConfig::from_config(config),
        ));
    let internal = crate::internal_router(engine);
    tokio::spawn(async move {
        tracing::info!(addr = %sock, "CL2 internal replication listener up (required mTLS)");
        if let Err(e) = axum_server::bind(sock)
            .acceptor(acceptor)
            .serve(internal.into_make_service_with_connect_info::<std::net::SocketAddr>())
            .await
        {
            tracing::error!(error = %e, "internal replication listener (mTLS) exited");
        }
    });
}

/// Poll the serving cert and the cluster CA for on-disk changes and reload
/// each in place, holding last-good on a bad renewal (the reloaders alarm).
fn spawn_watcher(rot: Arc<timelake_tls::RotatingCert>, ca: Arc<timelake_tls::RotatingClientCa>) {
    tokio::spawn(async move {
        let mut last_cert = rot.mtimes();
        let mut last_ca = ca.mtime();
        let mut tick = tokio::time::interval(Duration::from_secs(2));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let now_cert = rot.mtimes();
            if now_cert.is_some() && now_cert != last_cert {
                // Debounce: cert and key are two files; let the writer finish
                // both before validating the pair.
                tokio::time::sleep(Duration::from_millis(300)).await;
                let r = Arc::clone(&rot);
                let _ = tokio::task::spawn_blocking(move || r.reload()).await;
                last_cert = rot.mtimes();
            }
            let now_ca = ca.mtime();
            if now_ca.is_some() && now_ca != last_ca {
                let c = Arc::clone(&ca);
                let _ = tokio::task::spawn_blocking(move || c.reload()).await;
                last_ca = now_ca;
            }
        }
    });
}
