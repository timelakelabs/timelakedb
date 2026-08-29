//! SEC-3 (v2), **want mode**: verify a client certificate if one is
//! offered, and serve the client anyway if it is not.
//!
//! This is the property that makes it deployable today. Grafana,
//! Telegraf and the bench harness connect with no certificate and no
//! configuration change; Tributary presents one and is identified. The
//! server learns *who* without ever refusing *whether*.
//!
//! Stated plainly, because it would be dishonest to imply otherwise:
//! **want mode is not by itself a security control.** An attacker simply
//! declines to present a certificate and takes the anonymous path. Its
//! value is entirely in what the two paths are then allowed to do
//! differently — see `identity_of` and its use in the query session,
//! where a verified identity turns SEC-2's self-asserted authorization
//! claims into grants.
//!
//! Trust anchors sit behind the same `ArcSwap` as the serving
//! certificate, so a CA roll needs no restart. **Dual-CA overlap is the
//! reason this holds a bundle rather than one certificate:** during a
//! roll the file carries both outgoing and incoming anchors, so clients
//! holding either keep working. Any other approach requires every client
//! to change at the same instant.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::SystemTime;

use arc_swap::ArcSwap;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;

use crate::{RENEWAL_ALARM, TlsError, provider};

#[derive(Debug)]
pub struct RotatingClientCa {
    ca_path: PathBuf,
    roots: ArcSwap<RootCertStore>,
    anchors: AtomicI64,
    last_reload_ok: AtomicBool,
}

fn load_roots(path: &Path) -> Result<(RootCertStore, usize), TlsError> {
    let pem = std::fs::read(path)?;
    let mut rd = std::io::BufReader::new(&pem[..]);
    let certs: Vec<_> = rustls_pemfile::certs(&mut rd)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::Invalid(format!("client CA pem: {e}")))?;
    if certs.is_empty() {
        return Err(TlsError::Invalid(
            "client CA bundle contains no certificates".into(),
        ));
    }
    let mut store = RootCertStore::empty();
    let mut added = 0usize;
    for c in certs {
        store
            .add(c)
            .map_err(|e| TlsError::Invalid(format!("client CA: {e}")))?;
        added += 1;
    }
    Ok((store, added))
}

impl RotatingClientCa {
    pub fn load(ca_path: &Path) -> Result<Arc<Self>, TlsError> {
        let (roots, n) = load_roots(ca_path)?;
        tracing::info!(
            ca = %ca_path.display(),
            anchors = n,
            "SEC-3: client certificate verification enabled in WANT mode - a client \
             that presents a certificate is identified, one that does not is still served"
        );
        Ok(Arc::new(RotatingClientCa {
            ca_path: ca_path.to_path_buf(),
            roots: ArcSwap::from_pointee(roots),
            anchors: AtomicI64::new(n as i64),
            last_reload_ok: AtomicBool::new(true),
        }))
    }

    /// Validate-before-swap, exactly as the serving certificate does. A
    /// bundle that will not parse leaves the last-good anchors in place
    /// rather than locking out every client at once.
    pub fn reload(&self) -> Result<usize, TlsError> {
        match load_roots(&self.ca_path) {
            Ok((roots, n)) => {
                self.roots.store(Arc::new(roots));
                self.anchors.store(n as i64, Ordering::Relaxed);
                self.last_reload_ok.store(true, Ordering::Relaxed);
                tracing::info!(anchors = n, "client CA bundle rotated");
                Ok(n)
            }
            Err(e) => {
                self.last_reload_ok.store(false, Ordering::Relaxed);
                tracing::error!(
                    alarm = RENEWAL_ALARM,
                    ca = %self.ca_path.display(),
                    error = %e,
                    "client CA reload REJECTED; last-good anchors keep serving"
                );
                Err(e)
            }
        }
    }

    pub fn anchors(&self) -> i64 {
        self.anchors.load(Ordering::Relaxed)
    }
    pub fn last_reload_ok(&self) -> bool {
        self.last_reload_ok.load(Ordering::Relaxed)
    }
    pub fn mtime(&self) -> Option<SystemTime> {
        std::fs::metadata(&self.ca_path).ok()?.modified().ok()
    }
    pub(crate) fn current(&self) -> Arc<RootCertStore> {
        self.roots.load_full()
    }
}

/// Consults the rotating bundle at every handshake. In want mode
/// (`mandatory = false`) it never refuses a peer for merely being
/// anonymous; in require mode (`mandatory = true`, C3 / #72) an anonymous
/// peer is refused at the handshake. The trust decision is identical
/// either way — only whether "no certificate" is allowed differs.
#[derive(Debug)]
pub(crate) struct ClientAuth {
    pub(crate) ca: Arc<RotatingClientCa>,
    /// `false` = SEC-3 want mode (verify if offered, serve anyway).
    /// `true` = C3 require mode (no cert, or a cert outside the bundle,
    /// is refused). The intra-cluster listener sets this; the data plane
    /// never does.
    pub(crate) mandatory: bool,
}

impl ClientAuth {
    fn inner(&self) -> Result<Arc<dyn rustls::server::danger::ClientCertVerifier>, rustls::Error> {
        let builder = rustls::server::WebPkiClientVerifier::builder_with_provider(
            self.ca.current(),
            Arc::new(provider().clone()),
        );
        // The one call that separates want from require: want serves an
        // anonymous peer, require refuses it. Everything downstream — the
        // chain check against the bundle — is the same.
        let builder = if self.mandatory {
            builder
        } else {
            builder.allow_unauthenticated()
        };
        builder
            .build()
            .map_err(|e| rustls::Error::General(e.to_string()))
    }
}

impl rustls::server::danger::ClientCertVerifier for ClientAuth {
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        // Hints would have to be pinned at construction, and the bundle
        // rotates underneath us; offer none and let clients present what
        // they hold.
        &[]
    }

    fn client_auth_mandatory(&self) -> bool {
        self.mandatory
    }

    fn offer_client_auth(&self) -> bool {
        true
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        self.inner()?
            .verify_client_cert(end_entity, intermediates, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// The identity a verified client certificate carries: the subject
/// common name, which is what a principal is matched on.
///
/// `None` means the peer presented nothing — anonymous, and served.
/// Only ever called with certificates rustls has already verified
/// against the bundle, so this parses an identity rather than deciding
/// whether to trust one.
pub fn identity_of(peer: Option<&[CertificateDer<'_>]>) -> Option<String> {
    let leaf = peer?.first()?;
    let (_, parsed) = x509_parser::parse_x509_certificate(leaf).ok()?;
    parsed
        .subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    fn write_bundle(dir: &Path, pems: &[String]) -> PathBuf {
        let p = dir.join("client-ca.pem");
        let mut f = std::fs::File::create(&p).unwrap();
        for pem in pems {
            f.write_all(pem.as_bytes()).unwrap();
        }
        p
    }

    fn ca_pem(name: &str) -> String {
        let mut params = rcgen::CertificateParams::new(vec![name.to_string()]).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        let key = rcgen::KeyPair::generate().unwrap();
        params.self_signed(&key).unwrap().pem()
    }

    #[test]
    fn loads_a_bundle_and_counts_its_anchors() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_bundle(dir.path(), &[ca_pem("ca-one")]);
        let ca = RotatingClientCa::load(&p).unwrap();
        assert_eq!(ca.anchors(), 1);
        assert!(ca.last_reload_ok());
    }

    #[test]
    fn dual_ca_overlap_is_expressible() {
        // The roll: both the outgoing and incoming anchors present at
        // once, so clients holding either keep connecting.
        let dir = tempfile::tempdir().unwrap();
        let p = write_bundle(dir.path(), &[ca_pem("ca-old"), ca_pem("ca-new")]);
        let ca = RotatingClientCa::load(&p).unwrap();
        assert_eq!(ca.anchors(), 2, "both anchors must be trusted at once");
    }

    #[test]
    fn a_bad_bundle_keeps_the_last_good_anchors() {
        // Locking every client out because someone truncated a file mid
        // write is precisely the failure SEC-3 refuses.
        let dir = tempfile::tempdir().unwrap();
        let p = write_bundle(dir.path(), &[ca_pem("ca-one")]);
        let ca = RotatingClientCa::load(&p).unwrap();

        std::fs::write(&p, b"-----BEGIN CERTIFICATE-----\ngarbage\n").unwrap();
        assert!(ca.reload().is_err());
        assert!(!ca.last_reload_ok(), "the alarm must be raised");
        assert_eq!(ca.anchors(), 1, "last-good anchors still serving");

        // and a good bundle restores health
        std::fs::write(&p, ca_pem("ca-two")).unwrap();
        assert_eq!(ca.reload().unwrap(), 1);
        assert!(ca.last_reload_ok());
    }

    #[test]
    fn an_empty_bundle_is_refused_rather_than_trusting_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("empty.pem");
        std::fs::write(&p, b"").unwrap();
        assert!(RotatingClientCa::load(&p).is_err());
    }

    #[test]
    fn anonymous_peers_have_no_identity() {
        assert_eq!(identity_of(None), None);
        assert_eq!(identity_of(Some(&[])), None);
    }
}

/// Real in-memory TLS 1.3 handshakes through a require-mode server config,
/// because the one-line want/require difference (#72) is exactly the kind of
/// thing that reads correct and behaves backwards. These drive an actual
/// `ServerConnection`/`ClientConnection` pair rather than asserting a flag.
#[cfg(test)]
mod handshake_tests {
    use super::*;
    use crate::RotatingCert;
    use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::{ClientConfig, ClientConnection, ServerConfig, ServerConnection};

    fn ca(name: &str) -> (rcgen::Certificate, rcgen::KeyPair) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec![name.to_string()]).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        let cert = params.self_signed(&key).unwrap();
        (cert, key)
    }

    /// A leaf `cn` signed by `(issuer_cert, issuer_key)`, returned as the
    /// (chain, private key) an rustls identity wants.
    fn leaf(
        cn: &str,
        issuer_cert: &rcgen::Certificate,
        issuer_key: &rcgen::KeyPair,
    ) -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec![cn.to_string()]).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        let cert = params.signed_by(&key, issuer_cert, issuer_key).unwrap();
        let cert_der = cert.der().clone();
        let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der()));
        (cert_der, key_der)
    }

    /// Build a `RotatingClientCa` bundle file from a CA cert, and a
    /// `RotatingCert` server identity (`server_cn`) signed by that CA. The
    /// tempdir is returned so its files outlive the loaders.
    fn server_side(
        ca_cert: &rcgen::Certificate,
        ca_key: &rcgen::KeyPair,
        server_cn: &str,
    ) -> (Arc<RotatingCert>, Arc<RotatingClientCa>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let sk = rcgen::KeyPair::generate().unwrap();
        let mut sp = rcgen::CertificateParams::new(vec![server_cn.to_string()]).unwrap();
        sp.distinguished_name
            .push(rcgen::DnType::CommonName, server_cn);
        let scert = sp.signed_by(&sk, ca_cert, ca_key).unwrap();
        let cert_path = dir.path().join("server.pem");
        let key_path = dir.path().join("server.key");
        std::fs::write(&cert_path, scert.pem()).unwrap();
        std::fs::write(&key_path, sk.serialize_pem()).unwrap();
        let rot = RotatingCert::load(&cert_path, &key_path).unwrap();

        let bundle = dir.path().join("client-ca.pem");
        std::fs::write(&bundle, ca_cert.pem()).unwrap();
        let client_ca = RotatingClientCa::load(&bundle).unwrap();
        (rot, client_ca, dir)
    }

    fn client_config(
        server_root: &rcgen::Certificate,
        identity: Option<(CertificateDer<'static>, PrivateKeyDer<'static>)>,
    ) -> Arc<ClientConfig> {
        let mut roots = RootCertStore::empty();
        roots.add(server_root.der().clone()).unwrap();
        let b = ClientConfig::builder_with_provider(Arc::new(provider().clone()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots);
        let cfg = match identity {
            Some((chain, key)) => b.with_client_auth_cert(vec![chain], key).unwrap(),
            None => b.with_no_client_auth(),
        };
        Arc::new(cfg)
    }

    /// Drive a full handshake in memory. Returns the first error either side
    /// raises — for a require-mode refusal that is the server rejecting the
    /// empty/untrusted client certificate.
    fn handshake(
        server_cfg: Arc<ServerConfig>,
        client_cfg: Arc<ClientConfig>,
        server_name: &'static str,
    ) -> Result<(), rustls::Error> {
        let mut server = ServerConnection::new(server_cfg).unwrap();
        let name = rustls::pki_types::ServerName::try_from(server_name).unwrap();
        let mut client = ClientConnection::new(client_cfg, name).unwrap();
        for _ in 0..40 {
            let mut c2s = Vec::new();
            while client.wants_write() {
                client.write_tls(&mut c2s).unwrap();
            }
            let mut rd = &c2s[..];
            while !rd.is_empty() {
                if server.read_tls(&mut rd).unwrap() == 0 {
                    break;
                }
            }
            server.process_new_packets()?;

            let mut s2c = Vec::new();
            while server.wants_write() {
                server.write_tls(&mut s2c).unwrap();
            }
            let mut rd = &s2c[..];
            while !rd.is_empty() {
                if client.read_tls(&mut rd).unwrap() == 0 {
                    break;
                }
            }
            client.process_new_packets()?;

            if !client.is_handshaking() && !server.is_handshaking() {
                return Ok(());
            }
        }
        Ok(())
    }

    #[test]
    fn require_mode_accepts_a_carded_peer_and_refuses_the_rest() {
        let (ca_cert, ca_key) = ca("cluster-ca");
        let (rot, client_ca, _dir) = server_side(&ca_cert, &ca_key, "node-a");
        let server_cfg = rot.server_config_requiring_client_ca(false, &[], client_ca);

        // A peer carrying a cert signed by the cluster CA gets in.
        let carded = client_config(&ca_cert, Some(leaf("node-b", &ca_cert, &ca_key)));
        assert!(
            handshake(server_cfg.clone(), carded, "node-a").is_ok(),
            "require mode must accept a peer whose cert the bundle trusts"
        );

        // A peer with no certificate is refused — the whole point of require.
        let anon = client_config(&ca_cert, None);
        assert!(
            handshake(server_cfg.clone(), anon, "node-a").is_err(),
            "require mode must refuse an anonymous peer"
        );

        // A peer whose cert is signed by some other CA is refused too.
        let (other_cert, other_key) = ca("outsider-ca");
        let intruder = client_config(&ca_cert, Some(leaf("intruder", &other_cert, &other_key)));
        assert!(
            handshake(server_cfg, intruder, "node-a").is_err(),
            "a cert outside the bundle must be refused, not merely unidentified"
        );
    }

    #[test]
    fn want_mode_still_serves_the_anonymous_peer() {
        // The regression guard for the shared verifier: flipping require on
        // must not have turned the data plane's want mode into a wall.
        let (ca_cert, ca_key) = ca("cluster-ca");
        let (rot, client_ca, _dir) = server_side(&ca_cert, &ca_key, "node-a");
        let want_cfg = rot.server_config_with_client_ca(false, &[], Some(client_ca));

        let anon = client_config(&ca_cert, None);
        assert!(
            handshake(want_cfg, anon, "node-a").is_ok(),
            "want mode must keep serving a peer that presents no certificate"
        );
    }
}
