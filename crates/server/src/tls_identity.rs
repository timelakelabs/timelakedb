//! Client-certificate identity on the **HTTP** listener (SEC-3 v2).
//!
//! Flight SQL has carried a verified peer identity since SEC-3 v2, because
//! it owns its own accept loop and can read the certificate off the stream
//! directly. HTTP did not: `axum-server` owns that loop, so `/api/sql` saw
//! a connection that had been mutually authenticated and could not tell.
//!
//! The consequence was narrow but real, and was recorded as NOT DONE rather
//! than papered over: Tributary's L4 client certificate was **requested,
//! verified and accepted** at the TLS layer, and then authorized nothing on
//! the write path — no grant intersection, no per-identity attribution. The
//! feature promised more than it delivered.
//!
//! ## How it works
//!
//! `axum_server::accept::Accept` is given the stream *and the service*, and
//! may return a modified version of either. `RustlsAcceptor` yields a
//! `tokio_rustls::server::TlsStream`, whose `get_ref()` hands back the
//! `ServerConnection` **without consuming the stream** — the same call the
//! Flight listener makes. So this wraps the rustls acceptor, reads the
//! identity once the handshake completes, and layers an
//! `Extension(PeerIdentity)` onto the service so every request on that
//! connection carries it.
//!
//! Once per connection, not once per request: the peer certificate cannot
//! change mid-connection, and doing it per request would put an X.509 parse
//! on the query path.

use std::io;
use std::pin::Pin;

use axum::extract::Extension;
use axum_server::accept::Accept;
use axum_server::tls_rustls::RustlsAcceptor;
use timelake_api::PeerIdentity;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::server::TlsStream;
use tower_layer::Layer;

/// Wraps the rustls acceptor and attaches the verified identity.
#[derive(Clone)]
pub struct IdentityAcceptor {
    inner: RustlsAcceptor,
}

impl IdentityAcceptor {
    pub fn new(inner: RustlsAcceptor) -> Self {
        IdentityAcceptor { inner }
    }
}

impl<I, S> Accept<I, S> for IdentityAcceptor
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: Send + 'static,
{
    type Stream = TlsStream<I>;
    // Naming the layered service through the `Layer` associated type rather
    // than spelling axum's concrete wrapper, which is an implementation
    // detail that has changed between versions.
    type Service = <Extension<PeerIdentity> as Layer<S>>::Service;
    type Future = Pin<Box<dyn Future<Output = io::Result<(Self::Stream, Self::Service)>> + Send>>;

    fn accept(&self, stream: I, service: S) -> Self::Future {
        let inner = self.inner.clone();
        Box::pin(async move {
            let (tls, service) = inner.accept(stream, service).await?;

            // `get_ref().1` is the rustls ServerConnection. Borrowed, not
            // taken: `into_inner()` would consume the stream we still have
            // to serve on.
            let identity = timelake_tls::identity_of(tls.get_ref().1.peer_certificates());
            if let Some(cn) = &identity {
                tracing::debug!(identity = %cn, "http connection presented a client certificate");
            }

            Ok((tls, Extension(PeerIdentity(identity)).layer(service)))
        })
    }
}
