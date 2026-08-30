//! CL-2 ingester WAL replication — the client half.
//!
//! An ingester ships every WAL frame to its paired ingester **before the
//! 204**, so an acknowledged write is durable in two places. The peer holds
//! the frame in a durable *replica WAL* (see `Engine::replicate_receive`);
//! if this node dies, the peer replays that copy and no acknowledged write
//! is lost — replay overlap with rows this node already flushed is safe
//! because last-write-wins dedup (FR-5) makes it idempotent.
//!
//! **Availability outranks the second replica when the pair is half up
//! (PR-7).** If the peer is unreachable, the write is NOT failed: the node
//! drops to *degraded mode*, raises the `CL2_REPLICATION_DEGRADED` alarm
//! once, sets a gauge, and keeps accepting on local durability alone. The
//! honestly-stated cost: while degraded, a second failure can lose the
//! un-replicated writes. That is the deliberate trade, made loud.
//!
//! Transport: a plain HTTP POST to the peer's internal cluster listener.
//! The design names gRPC; the wire is an internal detail behind this type,
//! and HTTP reuses the axum/reqwest stack already in the tree with no
//! protobuf toolchain. It moves to required-mTLS at C3 (the verifier is
//! shipped) and can become a streaming gRPC/Flight link if the per-batch
//! round-trip ever shows up as the ingest bottleneck.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// Replication counters, surfaced on `/metrics`.
#[derive(Default)]
pub struct ReplStats {
    /// Frames confirmed durable on the peer.
    pub replicated: AtomicU64,
    /// 1 while the peer is currently unreachable.
    pub degraded: AtomicBool,
    /// How many times the node has *entered* degraded mode (transitions,
    /// not per-write) — a flapping peer is visible without log-diving.
    pub degraded_events: AtomicU64,
}

/// The client to this ingester's replication peer.
pub struct Replicator {
    peer_id: String,
    endpoint: String,
    client: reqwest::blocking::Client,
    stats: Arc<ReplStats>,
}

impl Replicator {
    pub fn new(peer_id: &str, peer_addr: &str, timeout_ms: u64) -> Replicator {
        Self::new_with_tls(peer_id, peer_addr, timeout_ms, None)
    }

    /// As [`Self::new`], but presenting this node's certificate over https and
    /// trusting only the cluster CA when the cluster has TLS (#72 phase 2).
    /// `tls = None` is the plaintext path, unchanged.
    pub fn new_with_tls(
        peer_id: &str,
        peer_addr: &str,
        timeout_ms: u64,
        tls: Option<&crate::peer_tls::PeerTls>,
    ) -> Replicator {
        // A short timeout: a slow peer must not stall the write path
        // indefinitely — a timeout is treated exactly like a down peer
        // (degraded), which is the safe direction (availability holds).
        //
        // This sits before the ack, so the timeout *is* the per-write
        // latency ceiling a degrading peer can impose. Short by design, so
        // that "slow" and "dead" collapse into one case: a dead peer trips
        // to degraded immediately and availability holds, while a slow one
        // trips nothing and simply multiplies every write's latency.
        // See `docs/P1-1_DESIGN.md` D1.
        //
        // Build on a plain OS thread. reqwest::blocking's builder blocks on an
        // internal tokio runtime, and constructing it with a rustls identity
        // from inside #[tokio::main] panics dropping that runtime in an async
        // context ("Cannot drop a runtime ... from within an asynchronous
        // context"). #132's mTLS drill caught it — the unit and integration
        // tests never built the replicator inside a running runtime, so it only
        // showed at real startup. A fresh thread has no runtime, so the
        // constructor is safe to call from anywhere.
        let scheme = crate::peer_tls::peer_scheme(tls);
        let tls = tls.cloned();
        let client = std::thread::spawn(move || {
            let mut builder =
                reqwest::blocking::Client::builder().timeout(Duration::from_millis(timeout_ms));
            if let Some(t) = &tls {
                builder = t.apply_blocking(builder);
            }
            builder.build().expect("replication client")
        })
        .join()
        .expect("replication client build thread panicked");
        Replicator {
            peer_id: peer_id.to_string(),
            endpoint: format!("{scheme}://{peer_addr}/internal/v1/replicate"),
            client,
            stats: Arc::new(ReplStats::default()),
        }
    }

    pub fn stats(&self) -> Arc<ReplStats> {
        self.stats.clone()
    }

    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    /// Replicate one frame, blocking until the peer confirms durability or
    /// the attempt fails. Returns `true` when the peer acknowledged (the
    /// write is durable on two nodes), `false` when the peer is down and
    /// the node is now degraded. The caller proceeds either way — a `false`
    /// is availability, not an error, and the alarm carries the risk.
    pub fn replicate(&self, db: &str, mult: i64, body: &[u8]) -> bool {
        let sent = self
            .client
            .post(&self.endpoint)
            .header("x-repl-db", db)
            .header("x-repl-mult", mult.to_string())
            .header("content-type", "application/octet-stream")
            .body(body.to_vec())
            .send();
        match sent {
            Ok(resp) if resp.status().is_success() => {
                self.stats.replicated.fetch_add(1, Ordering::Relaxed);
                // Recovered: clear the gauge and say so, once.
                if self.stats.degraded.swap(false, Ordering::Relaxed) {
                    tracing::info!(
                        peer = %self.peer_id,
                        "CL2 replication recovered: peer reachable again, writes replicated"
                    );
                }
                true
            }
            outcome => {
                // Down, timed out, or a non-2xx — all "peer not durable".
                if !self.stats.degraded.swap(true, Ordering::Relaxed) {
                    self.stats.degraded_events.fetch_add(1, Ordering::Relaxed);
                    let detail = match &outcome {
                        Ok(resp) => format!("peer returned HTTP {}", resp.status().as_u16()),
                        Err(e) => e.to_string(),
                    };
                    tracing::error!(
                        peer = %self.peer_id,
                        alarm = "CL2_REPLICATION_DEGRADED",
                        detail,
                        "replication peer unreachable — accepting writes on LOCAL \
                         durability only; a second failure now can lose the \
                         un-replicated writes"
                    );
                }
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ca(name: &str) -> (rcgen::Certificate, rcgen::KeyPair) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut p = rcgen::CertificateParams::new(vec![name.to_string()]).unwrap();
        p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        p.distinguished_name.push(rcgen::DnType::CommonName, name);
        (p.self_signed(&key).unwrap(), key)
    }

    fn leaf(cn: &str, ca_cert: &rcgen::Certificate, ca_key: &rcgen::KeyPair) -> (String, String) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut p = rcgen::CertificateParams::new(vec![cn.to_string()]).unwrap();
        p.distinguished_name.push(rcgen::DnType::CommonName, cn);
        let cert = p.signed_by(&key, ca_cert, ca_key).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    // Regression for #132: building the TLS replicator inside a tokio runtime
    // must not panic. reqwest::blocking's builder blocks on an internal runtime,
    // and constructing it with a rustls identity from within #[tokio::main]
    // panicked ("Cannot drop a runtime ... from within an asynchronous
    // context") — new_with_tls now isolates the build on a plain thread. Before
    // that fix this test aborts with the panic; after it, it passes.
    #[tokio::test]
    async fn a_tls_replicator_builds_inside_a_runtime() {
        let (ca_cert, ca_key) = ca("cluster-ca");
        let (cert, key) = leaf("node-a", &ca_cert, &ca_key);
        let tls = crate::peer_tls::PeerTls::from_pems(
            cert.as_bytes(),
            key.as_bytes(),
            ca_cert.pem().as_bytes(),
        )
        .unwrap();
        let r = Replicator::new_with_tls("peer", "127.0.0.1:1965", 100, Some(&tls));
        assert_eq!(r.peer_id(), "peer");
    }

    // The plaintext path must stay safe inside a runtime too (it delegates to
    // the same thread-isolated build).
    #[tokio::test]
    async fn a_plaintext_replicator_builds_inside_a_runtime() {
        let r = Replicator::new("peer", "127.0.0.1:1965", 100);
        assert_eq!(r.peer_id(), "peer");
    }
}
