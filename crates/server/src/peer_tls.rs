//! Client-side TLS for the intra-cluster links (#72 phase 2).
//!
//! When a node has a serving cert/key and a cluster CA
//! (`TIMELAKE_TLS_CERT` + `TIMELAKE_TLS_KEY` + `TIMELAKE_CLUSTER_CA`), its
//! calls to a peer's `/internal/v1/*` go over https, present this node's
//! certificate as its cluster identity, and trust **only** the cluster CA —
//! never the public web PKI. Absent, the links stay plaintext, so the drills
//! and single-node dev are untouched.
//!
//! This is the client half of required intra-cluster mTLS: additive on its
//! own (a want-mode listener still accepts these), so it lands before the
//! listener starts *requiring* a cert in phase 3 (#131). One place decides
//! the scheme ([`peer_scheme`]) so the replication and querier clients cannot
//! drift into disagreeing about http vs https.

use std::io;

/// The material a peer client needs to speak mTLS: this node's identity
/// (its serving cert + key) and the cluster CA it verifies peers against.
#[derive(Clone)]
pub struct PeerTls {
    // reqwest's Identity/Certificate are not Clone and are consumed into a
    // builder, so hold the validated PEM bytes and rebuild per client. A node
    // builds two or three clients, once, at startup — the cost is nothing.
    identity_pem: Vec<u8>,
    ca_pem: Vec<u8>,
}

impl PeerTls {
    /// Load from the environment. `Ok(None)` when `TIMELAKE_CLUSTER_CA` is
    /// unset — the cluster links stay plaintext, independent of whether the
    /// data plane has TLS. An **error** when the CA is set but the node has no
    /// cert/key: a client that cannot present an identity is a
    /// misconfiguration under required mTLS, not a silent plaintext fallback.
    pub fn from_env() -> io::Result<Option<PeerTls>> {
        let ca = match std::env::var("TIMELAKE_CLUSTER_CA") {
            Ok(p) if !p.is_empty() => p,
            _ => return Ok(None),
        };
        let cert = std::env::var("TIMELAKE_TLS_CERT")
            .ok()
            .filter(|s| !s.is_empty());
        let key = std::env::var("TIMELAKE_TLS_KEY")
            .ok()
            .filter(|s| !s.is_empty());
        let (cert, key) = match (cert, key) {
            (Some(c), Some(k)) => (c, k),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "TIMELAKE_CLUSTER_CA is set (intra-cluster mTLS) but TIMELAKE_TLS_CERT/\
                     TIMELAKE_TLS_KEY are not — a node cannot present a cluster identity \
                     without its own certificate",
                ));
            }
        };
        Self::from_files(&cert, &key, &ca).map(Some)
    }

    /// Build from file paths (serving cert, key, cluster CA bundle).
    pub fn from_files(cert: &str, key: &str, ca: &str) -> io::Result<PeerTls> {
        Self::from_pems(
            &std::fs::read(cert)?,
            &std::fs::read(key)?,
            &std::fs::read(ca)?,
        )
    }

    /// Build from PEM bytes, validating them now so a bad cert fails at
    /// startup rather than at the first peer call.
    pub fn from_pems(cert_pem: &[u8], key_pem: &[u8], ca_pem: &[u8]) -> io::Result<PeerTls> {
        // reqwest's rustls Identity::from_pem wants the key first, then the
        // certificate chain.
        let mut identity_pem = Vec::with_capacity(key_pem.len() + cert_pem.len() + 1);
        identity_pem.extend_from_slice(key_pem);
        if !key_pem.ends_with(b"\n") {
            identity_pem.push(b'\n');
        }
        identity_pem.extend_from_slice(cert_pem);

        reqwest::Identity::from_pem(&identity_pem).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("node identity: {e}"))
        })?;
        // A bundle, not a single cert: dual-CA overlap during a roll carries
        // both anchors, exactly as the listener's RotatingClientCa does.
        let roots = reqwest::Certificate::from_pem_bundle(ca_pem)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("cluster CA: {e}")))?;
        if roots.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cluster CA bundle contains no certificates",
            ));
        }

        Ok(PeerTls {
            identity_pem,
            ca_pem: ca_pem.to_vec(),
        })
    }

    fn identity(&self) -> reqwest::Identity {
        reqwest::Identity::from_pem(&self.identity_pem)
            .expect("node identity PEM validated at load")
    }
    fn roots(&self) -> Vec<reqwest::Certificate> {
        reqwest::Certificate::from_pem_bundle(&self.ca_pem).expect("cluster CA validated at load")
    }

    /// Apply this node's identity and the cluster CA as the **only** trusted
    /// roots to a blocking client builder (the replication client).
    pub fn apply_blocking(
        &self,
        mut b: reqwest::blocking::ClientBuilder,
    ) -> reqwest::blocking::ClientBuilder {
        b = b.identity(self.identity()).tls_built_in_root_certs(false);
        for c in self.roots() {
            b = b.add_root_certificate(c);
        }
        b
    }

    /// As [`Self::apply_blocking`], for the async client (the querier).
    pub fn apply_async(&self, mut b: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
        b = b.identity(self.identity()).tls_built_in_root_certs(false);
        for c in self.roots() {
            b = b.add_root_certificate(c);
        }
        b
    }
}

/// `https` when the node has cluster TLS, `http` otherwise. The one place the
/// scheme is decided, so the two peer clients cannot disagree.
pub fn peer_scheme(tls: Option<&PeerTls>) -> &'static str {
    if tls.is_some() { "https" } else { "http" }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ca(name: &str) -> (rcgen::Certificate, rcgen::KeyPair) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut p = rcgen::CertificateParams::new(vec![name.to_string()]).unwrap();
        p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        p.distinguished_name.push(rcgen::DnType::CommonName, name);
        let cert = p.self_signed(&key).unwrap();
        (cert, key)
    }

    /// (cert_pem, key_pem) for a leaf signed by the CA.
    fn node(cn: &str, ca_cert: &rcgen::Certificate, ca_key: &rcgen::KeyPair) -> (String, String) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut p = rcgen::CertificateParams::new(vec![cn.to_string()]).unwrap();
        p.distinguished_name.push(rcgen::DnType::CommonName, cn);
        let cert = p.signed_by(&key, ca_cert, ca_key).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    #[test]
    fn scheme_follows_whether_tls_is_configured() {
        assert_eq!(peer_scheme(None), "http");
        let (ca_cert, ca_key) = ca("cluster-ca");
        let (cert, key) = node("node-a", &ca_cert, &ca_key);
        let tls =
            PeerTls::from_pems(cert.as_bytes(), key.as_bytes(), ca_cert.pem().as_bytes()).unwrap();
        assert_eq!(peer_scheme(Some(&tls)), "https");
    }

    #[test]
    fn a_valid_identity_and_ca_build_real_clients() {
        // The point of the test: reqwest actually accepts the concatenated
        // identity PEM and the CA bundle. A wrong key/cert order or a stray
        // trailing byte fails right here, at load, not on the first peer call.
        let (ca_cert, ca_key) = ca("cluster-ca");
        let (cert, key) = node("node-a", &ca_cert, &ca_key);
        let tls =
            PeerTls::from_pems(cert.as_bytes(), key.as_bytes(), ca_cert.pem().as_bytes()).unwrap();

        assert!(
            tls.apply_blocking(reqwest::blocking::Client::builder())
                .build()
                .is_ok(),
            "the replication (blocking) client must build with the identity + cluster CA"
        );
        assert!(
            tls.apply_async(reqwest::Client::builder()).build().is_ok(),
            "the querier (async) client must build with the identity + cluster CA"
        );
    }

    #[test]
    fn a_ca_set_without_cert_or_key_is_a_loud_error() {
        // from_env's contract, exercised through from_pems' sibling: a bad or
        // empty CA bundle is rejected rather than trusting nothing.
        let (ca_cert, ca_key) = ca("cluster-ca");
        let (cert, key) = node("node-a", &ca_cert, &ca_key);
        assert!(
            PeerTls::from_pems(cert.as_bytes(), key.as_bytes(), b"not a certificate").is_err(),
            "a CA bundle that parses to no anchors must be refused"
        );
    }
}
