//! #72 phase 3: the intra-cluster listener requires mTLS.
//!
//! Stands up the real internal listener over required mTLS in-process and
//! proves a peer carrying a cluster-signed cert connects, while a peer with no
//! certificate is refused at the handshake — the property want mode could not
//! give. The metric that reports the mode is asserted too.

use std::sync::Arc;
use std::time::Duration;

fn engine(dir: &std::path::Path) -> Arc<timelake_server::Engine> {
    std::fs::create_dir_all(dir).unwrap();
    timelake_server::Engine::open(dir, timelake_server::EngineConfig::default()).unwrap()
}

fn ca(name: &str) -> (rcgen::Certificate, rcgen::KeyPair) {
    let key = rcgen::KeyPair::generate().unwrap();
    let mut p = rcgen::CertificateParams::new(vec![name.to_string()]).unwrap();
    p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    p.distinguished_name.push(rcgen::DnType::CommonName, name);
    (p.self_signed(&key).unwrap(), key)
}

/// (cert_pem, key_pem) for a leaf `cn` signed by the CA.
fn leaf(cn: &str, ca_cert: &rcgen::Certificate, ca_key: &rcgen::KeyPair) -> (String, String) {
    let key = rcgen::KeyPair::generate().unwrap();
    let mut p = rcgen::CertificateParams::new(vec![cn.to_string()]).unwrap();
    p.distinguished_name.push(rcgen::DnType::CommonName, cn);
    let cert = p.signed_by(&key, ca_cert, ca_key).unwrap();
    (cert.pem(), key.serialize_pem())
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test]
async fn the_internal_listener_requires_a_client_cert() {
    let dir = tempfile::tempdir().unwrap();

    // A cluster CA that signs both the server's cert and the good client's.
    let (ca_cert, ca_key) = ca("cluster-ca");
    // SAN "localhost" so a client dialing https://localhost passes hostname
    // verification while the socket itself is 127.0.0.1.
    let (srv_cert, srv_key) = leaf("localhost", &ca_cert, &ca_key);
    let cert_path = dir.path().join("srv.pem");
    let key_path = dir.path().join("srv.key");
    let ca_path = dir.path().join("cluster-ca.pem");
    std::fs::write(&cert_path, &srv_cert).unwrap();
    std::fs::write(&key_path, &srv_key).unwrap();
    std::fs::write(&ca_path, ca_cert.pem()).unwrap();

    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let eng = engine(&dir.path().join("data"));

    timelake_server::internal_listener::spawn_mtls(
        Arc::clone(&eng),
        addr,
        cert_path.to_string_lossy().into_owned(),
        key_path.to_string_lossy().into_owned(),
        ca_path.to_string_lossy().into_owned(),
    );

    let health = format!("https://localhost:{port}/internal/v1/health");
    let root = reqwest::Certificate::from_pem(ca_cert.pem().as_bytes()).unwrap();

    // The good peer: a cluster-signed identity, trusting only the cluster CA.
    let (cli_cert, cli_key) = leaf("node-b", &ca_cert, &ca_key);
    let mut id_pem = cli_key.into_bytes(); // key first, then cert (reqwest order)
    id_pem.extend_from_slice(cli_cert.as_bytes());
    let carded = reqwest::Client::builder()
        .identity(reqwest::Identity::from_pem(&id_pem).unwrap())
        .add_root_certificate(root.clone())
        .tls_built_in_root_certs(false)
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    // Poll until the listener is up (the carded client gets 200), up to ~5 s.
    let mut up = false;
    for _ in 0..50 {
        if let Ok(r) = carded.get(&health).send().await
            && r.status().is_success()
        {
            up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        up,
        "a peer with a cluster-signed cert must reach the internal listener"
    );

    // The intruder: trusts the server cert but presents NO client identity.
    let certless = reqwest::Client::builder()
        .add_root_certificate(root)
        .tls_built_in_root_certs(false)
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let refused = certless.get(&health).send().await;
    assert!(
        refused.is_err(),
        "require mode must refuse a peer with no client certificate, got {refused:?}"
    );

    // The mode is visible to an operator and the phase-4 drill.
    let metrics = eng.metrics_text_impl();
    assert!(
        metrics.contains("timelake_cluster_mtls_required 1"),
        "the internal listener's require mode must show on /metrics"
    );
}
