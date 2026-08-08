//! SEC-3: TLS 1.3 with hot certificate rotation for short-TTL (~24 h) certs.
//!
//! The design (ARCHITECTURE §11): both listeners share one
//! [`RotatingCert`] — an `ArcSwap<CertifiedKey>` behind a custom
//! `ResolvesServerCert`. rustls consults the resolver only at handshake
//! time, so swapping the pointer affects new connections exclusively;
//! established connections and in-flight streams are structurally
//! untouched by a rotation.
//!
//! A new cert+key pair is parsed and validated (PEM shape, key↔cert
//! consistency, not expired) *before* the atomic swap. A bad renewal
//! leaves the last-good pair serving and raises the named SEC-3 alarm
//! (RR-5: failures are loud and named, never silent).
//!
//! This crate is runtime-free: the file watcher and admin endpoint that
//! *trigger* [`RotatingCert::reload`] live in timelord-server.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::SystemTime;

use arc_swap::ArcSwap;
use rustls::ServerConfig;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;

/// The named alarm string (grep target for operators and for AT-7).
pub const RENEWAL_ALARM: &str = "SEC3_CERT_RENEWAL_FAILED";

#[derive(Debug)]
pub enum TlsError {
    Io(std::io::Error),
    /// PEM present but no certificate / no private key in it.
    Missing(&'static str),
    /// Certificate or key failed to parse, or the key doesn't match the cert.
    Invalid(String),
    /// The pair parsed but the leaf certificate is already expired.
    Expired { not_after_epoch: i64 },
}

impl std::fmt::Display for TlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsError::Io(e) => write!(f, "io: {e}"),
            TlsError::Missing(what) => write!(f, "no {what} found in PEM"),
            TlsError::Invalid(msg) => write!(f, "invalid cert/key pair: {msg}"),
            TlsError::Expired { not_after_epoch } => {
                write!(f, "certificate already expired (notAfter epoch {not_after_epoch})")
            }
        }
    }
}

impl std::error::Error for TlsError {}

impl From<std::io::Error> for TlsError {
    fn from(e: std::io::Error) -> Self {
        TlsError::Io(e)
    }
}

fn epoch_now() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Read + fully validate a PEM cert chain and key. Nothing is adopted
/// unless every check passes — this is the validate-before-swap gate.
fn load_pair(
    cert_path: &Path,
    key_path: &Path,
    provider: &CryptoProvider,
) -> Result<(CertifiedKey, i64), TlsError> {
    let cert_pem = std::fs::read(cert_path)?;
    let key_pem = std::fs::read(key_path)?;

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut &cert_pem[..])
        .collect::<Result<_, _>>()
        .map_err(|e| TlsError::Invalid(format!("cert PEM: {e}")))?;
    if certs.is_empty() {
        return Err(TlsError::Missing("certificate"));
    }
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut &key_pem[..])
        .map_err(|e| TlsError::Invalid(format!("key PEM: {e}")))?
        .ok_or(TlsError::Missing("private key"))?;

    // Leaf expiry: reject an already-expired renewal outright; the epoch
    // feeds the timelord_tls_cert_expiry_seconds gauge.
    let (_, parsed) = x509_parser::parse_x509_certificate(certs[0].as_ref())
        .map_err(|e| TlsError::Invalid(format!("x509 parse: {e}")))?;
    let not_after_epoch = parsed.validity().not_after.timestamp();
    if not_after_epoch <= epoch_now() {
        return Err(TlsError::Expired { not_after_epoch });
    }

    // from_der builds the signing key AND checks key↔cert consistency
    // where the provider can (InconsistentKeys::Unknown passes through).
    let ck = CertifiedKey::from_der(certs, key, provider)
        .map_err(|e| TlsError::Invalid(e.to_string()))?;
    Ok((ck, not_after_epoch))
}

/// The hot-rotation core: last-good cert pair, swapped atomically only
/// after a replacement validates. Shared by every listener.
pub struct RotatingCert {
    cert_path: PathBuf,
    key_path: PathBuf,
    current: ArcSwap<CertifiedKey>,
    /// Leaf notAfter of the *serving* pair, epoch seconds.
    expiry_epoch: AtomicI64,
    /// False after a failed renewal until a good reload lands.
    last_reload_ok: AtomicBool,
}

impl RotatingCert {
    /// Initial load. Startup fails hard on a bad pair — there is no
    /// last-good to fall back to yet.
    pub fn load(cert_path: &Path, key_path: &Path) -> Result<Arc<Self>, TlsError> {
        let (ck, not_after) = load_pair(cert_path, key_path, provider())?;
        Ok(Arc::new(RotatingCert {
            cert_path: cert_path.to_path_buf(),
            key_path: key_path.to_path_buf(),
            current: ArcSwap::from_pointee(ck),
            expiry_epoch: AtomicI64::new(not_after),
            last_reload_ok: AtomicBool::new(true),
        }))
    }

    /// Re-read the files; adopt only if the pair validates. On failure the
    /// last-good pair keeps serving and the named SEC-3 alarm is raised.
    /// Returns the new leaf expiry (epoch seconds) on success.
    pub fn reload(&self) -> Result<i64, TlsError> {
        match load_pair(&self.cert_path, &self.key_path, provider()) {
            Ok((ck, not_after)) => {
                self.current.store(Arc::new(ck));
                self.expiry_epoch.store(not_after, Ordering::Relaxed);
                self.last_reload_ok.store(true, Ordering::Relaxed);
                tracing::info!(
                    cert = %self.cert_path.display(),
                    expires_in_secs = not_after - epoch_now(),
                    "TLS certificate rotated (new handshakes only; established connections unaffected)"
                );
                Ok(not_after)
            }
            Err(e) => {
                self.last_reload_ok.store(false, Ordering::Relaxed);
                tracing::error!(
                    alarm = RENEWAL_ALARM,
                    cert = %self.cert_path.display(),
                    error = %e,
                    serving_expiry_epoch = self.expiry_epoch.load(Ordering::Relaxed),
                    "certificate renewal REJECTED — still serving last-good pair"
                );
                Err(e)
            }
        }
    }

    /// Leaf notAfter (epoch seconds) of the pair currently serving.
    pub fn expiry_epoch(&self) -> i64 {
        self.expiry_epoch.load(Ordering::Relaxed)
    }

    /// Seconds until the serving cert expires (negative = expired).
    pub fn expires_in_secs(&self) -> i64 {
        self.expiry_epoch() - epoch_now()
    }

    /// False while the most recent renewal attempt was rejected.
    pub fn last_reload_ok(&self) -> bool {
        self.last_reload_ok.load(Ordering::Relaxed)
    }

    /// The serving pair (for tests / introspection).
    pub fn current(&self) -> Arc<CertifiedKey> {
        self.current.load_full()
    }

    /// Watched-file mtimes, for the poll-based watcher in the server.
    pub fn mtimes(&self) -> Option<(SystemTime, SystemTime)> {
        let c = std::fs::metadata(&self.cert_path).and_then(|m| m.modified()).ok()?;
        let k = std::fs::metadata(&self.key_path).and_then(|m| m.modified()).ok()?;
        Some((c, k))
    }

    /// Build a listener's `ServerConfig` around this rotator. Each
    /// listener gets its own config (they need different ALPN) but all
    /// configs resolve certs through the same swap point.
    ///
    /// `allow_tls12` lowers the floor from the default TLS 1.3-only
    /// (SEC-3: 1.3 everywhere, configurable 1.2 floor for old clients).
    pub fn server_config(
        self: &Arc<Self>,
        allow_tls12: bool,
        alpn: &[&[u8]],
    ) -> Arc<ServerConfig> {
        let versions: &[&rustls::SupportedProtocolVersion] = if allow_tls12 {
            &[&rustls::version::TLS13, &rustls::version::TLS12]
        } else {
            &[&rustls::version::TLS13]
        };
        let mut cfg = ServerConfig::builder_with_provider(Arc::new(provider().clone()))
            .with_protocol_versions(versions)
            .expect("ring provider supports TLS 1.2/1.3")
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(Resolver(Arc::clone(self))));
        cfg.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
        Arc::new(cfg)
    }
}

fn provider() -> &'static CryptoProvider {
    // One process-wide provider; ring (workspace feature choice).
    static PROVIDER: std::sync::OnceLock<CryptoProvider> = std::sync::OnceLock::new();
    PROVIDER.get_or_init(rustls::crypto::ring::default_provider)
}

/// rustls calls this at handshake time only — the load is the entire
/// rotation mechanism.
struct Resolver(Arc<RotatingCert>);

impl std::fmt::Debug for Resolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Resolver")
            .field("cert", &self.0.cert_path)
            .field("expiry_epoch", &self.0.expiry_epoch())
            .finish()
    }
}

impl ResolvesServerCert for Resolver {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.0.current.load_full())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mint a self-signed localhost cert valid for `hours` from now
    /// (negative = already expired), returning (cert_pem, key_pem).
    fn mint(hours: i64) -> (String, String) {
        let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()])
            .expect("params");
        let now = time::OffsetDateTime::now_utc();
        params.not_before = now - time::Duration::hours(1);
        params.not_after = now + time::Duration::hours(hours);
        let key = rcgen::KeyPair::generate().expect("keypair");
        let cert = params.self_signed(&key).expect("self-sign");
        (cert.pem(), key.serialize_pem())
    }

    fn write_pair(dir: &Path, cert: &str, key: &str) -> (PathBuf, PathBuf) {
        let c = dir.join("server.crt");
        let k = dir.join("server.key");
        std::fs::write(&c, cert).unwrap();
        std::fs::write(&k, key).unwrap();
        (c, k)
    }

    #[test]
    fn valid_pair_loads_with_future_expiry() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = mint(24);
        let (c, k) = write_pair(dir.path(), &cert, &key);
        let rot = RotatingCert::load(&c, &k).expect("load");
        assert!(rot.last_reload_ok());
        // ~24 h out, generous slop for slow CI
        assert!(rot.expires_in_secs() > 23 * 3600 && rot.expires_in_secs() <= 24 * 3600);
    }

    #[test]
    fn corrupt_cert_rejected_at_startup() {
        let dir = tempfile::tempdir().unwrap();
        let (_, key) = mint(24);
        let (c, k) = write_pair(dir.path(), "not a pem at all", &key);
        assert!(RotatingCert::load(&c, &k).is_err());
    }

    #[test]
    fn expired_cert_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = mint(-2);
        let (c, k) = write_pair(dir.path(), &cert, &key);
        match RotatingCert::load(&c, &k) {
            Err(TlsError::Expired { .. }) => {}
            Err(other) => panic!("expected Expired, got {other:?}"),
            Ok(_) => panic!("expected Expired, got a loaded cert"),
        }
    }

    #[test]
    fn rotation_swaps_serving_cert() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_a, key_a) = mint(24);
        let (c, k) = write_pair(dir.path(), &cert_a, &key_a);
        let rot = RotatingCert::load(&c, &k).expect("load A");
        let der_a = rot.current().end_entity_cert().unwrap().to_vec();

        let (cert_b, key_b) = mint(24);
        write_pair(dir.path(), &cert_b, &key_b);
        rot.reload().expect("reload B");
        let der_b = rot.current().end_entity_cert().unwrap().to_vec();
        assert_ne!(der_a, der_b, "resolver must serve the rotated cert");
        assert!(rot.last_reload_ok());
    }

    #[test]
    fn corrupt_renewal_keeps_last_good_then_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_a, key_a) = mint(24);
        let (c, k) = write_pair(dir.path(), &cert_a, &key_a);
        let rot = RotatingCert::load(&c, &k).expect("load A");
        let der_a = rot.current().end_entity_cert().unwrap().to_vec();
        let expiry_a = rot.expiry_epoch();

        // Corrupt renewal: reload must fail, serving state untouched.
        std::fs::write(&c, "-----BEGIN GARBAGE-----\nzzzz\n-----END GARBAGE-----\n").unwrap();
        assert!(rot.reload().is_err());
        assert!(!rot.last_reload_ok(), "alarm state must be visible");
        assert_eq!(rot.current().end_entity_cert().unwrap().to_vec(), der_a);
        assert_eq!(rot.expiry_epoch(), expiry_a);

        // A subsequent good renewal recovers.
        let (cert_b, key_b) = mint(24);
        write_pair(dir.path(), &cert_b, &key_b);
        rot.reload().expect("recovery reload");
        assert!(rot.last_reload_ok());
        assert_ne!(rot.current().end_entity_cert().unwrap().to_vec(), der_a);
    }

    #[test]
    fn mismatched_key_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_a, _key_a) = mint(24);
        let (_cert_b, key_b) = mint(24);
        let (c, k) = write_pair(dir.path(), &cert_a, &key_b);
        // ring's provider exposes public keys for ECDSA, so from_der can
        // prove the mismatch. If a future provider can't, it must still
        // never adopt a provably-wrong pair.
        assert!(
            RotatingCert::load(&c, &k).is_err(),
            "cert A + key B must not be adopted"
        );
    }

    #[test]
    fn server_config_builds_tls13_only_and_with_floor() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = mint(24);
        let (c, k) = write_pair(dir.path(), &cert, &key);
        let rot = RotatingCert::load(&c, &k).expect("load");
        let strict = rot.server_config(false, &[b"h2".as_slice(), b"http/1.1"]);
        assert_eq!(strict.alpn_protocols, vec![b"h2".to_vec(), b"http/1.1".to_vec()]);
        let floored = rot.server_config(true, &[b"h2".as_slice()]);
        assert_eq!(floored.alpn_protocols, vec![b"h2".to_vec()]);
    }
}
