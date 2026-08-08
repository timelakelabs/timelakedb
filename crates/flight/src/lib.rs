//! Flight SQL server (FR-8) — the Grafana read path.
//!
//! Serves gRPC/HTTP-2 on its own port (1964 by default; one-port
//! multiplexing with the REST listener is a later nicety). Grafana's
//! stock InfluxDB datasource in SQL mode speaks exactly this: handshake,
//! GetFlightInfo(CommandStatementQuery), DoGet(ticket). The database is
//! taken from request metadata (`database` / `bucket-name` headers, the
//! keys that datasource sends), defaulting to "poc".
//!
//! TLS 1.3 with hot cert rotation (SEC-3) attaches to this same tonic
//! server at the security milestone.

use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket,
};
use futures::{Stream, TryStreamExt, stream};
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

/// Future alias so implementors don't need a futures dependency.
pub type SqlFuture<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<Vec<RecordBatch>, String>> + Send + 'a>>;

/// The seam to the engine (implemented by `timelord_server::Engine`).
pub trait SqlBackend: Send + Sync + 'static {
    fn query_batches<'a>(&'a self, db: String, sql: String) -> SqlFuture<'a>;
}

#[derive(Clone)]
pub struct TimelordFlight {
    backend: Arc<dyn SqlBackend>,
}

impl TimelordFlight {
    pub fn new(backend: Arc<dyn SqlBackend>) -> Self {
        TimelordFlight { backend }
    }
}

fn db_from_metadata(md: &tonic::metadata::MetadataMap) -> String {
    for key in ["database", "bucket-name", "bucket", "db"] {
        if let Some(v) = md.get(key).and_then(|v| v.to_str().ok()) {
            if !v.is_empty() {
                return v.to_string();
            }
        }
    }
    "poc".to_string()
}

#[tonic::async_trait]
impl FlightSqlService for TimelordFlight {
    type FlightService = TimelordFlight;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        // No auth at this milestone: accept anything, return an empty token.
        let resp = HandshakeResponse {
            protocol_version: 0,
            payload: Default::default(),
        };
        Ok(Response::new(Box::pin(stream::iter(vec![Ok(resp)]))))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let db = db_from_metadata(request.metadata());
        let handle = serde_json::json!({ "db": db, "sql": query.query }).to_string();
        let ticket = TicketStatementQuery {
            statement_handle: handle.into_bytes().into(),
        };
        let info = FlightInfo::new()
            .with_descriptor(request.into_inner())
            .with_endpoint(FlightEndpoint::new().with_ticket(Ticket {
                ticket: ticket.as_any().encode_to_vec().into(),
            }));
        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as FlightService>::DoGetStream>, Status> {
        let handle = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("ticket is not utf-8"))?;
        let parsed: serde_json::Value = serde_json::from_str(&handle)
            .map_err(|e| Status::invalid_argument(format!("bad ticket: {e}")))?;
        let db = parsed["db"].as_str().unwrap_or("poc").to_string();
        let sql = parsed["sql"]
            .as_str()
            .ok_or_else(|| Status::invalid_argument("ticket missing sql"))?
            .to_string();

        let batches = self
            .backend
            .query_batches(db, sql)
            .await
            .map_err(Status::internal)?;
        let schema = match batches.first() {
            Some(b) => b.schema(),
            None => Arc::new(arrow::datatypes::Schema::empty()),
        };
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::iter(batches.into_iter().map(Ok)))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Serve Flight SQL until the process exits.
pub async fn serve(
    backend: Arc<dyn SqlBackend>,
    addr: std::net::SocketAddr,
) -> Result<(), tonic::transport::Error> {
    tracing::info!(%addr, "flight sql listening");
    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(TimelordFlight::new(backend)))
        .serve(addr)
        .await
}

/// Serve Flight SQL over TLS (SEC-3). The `ServerConfig` comes from
/// `timelord_tls::RotatingCert::server_config` — its cert resolver is
/// consulted per handshake, so cert rotation needs no listener restart
/// and never touches established gRPC streams.
pub async fn serve_tls(
    backend: Arc<dyn SqlBackend>,
    addr: std::net::SocketAddr,
    tls: Arc<rustls::ServerConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let acceptor = tokio_rustls::TlsAcceptor::from(tls);
    tracing::info!(%addr, "flight sql listening (TLS)");

    // Handshakes run inline on the accept loop: rustls handshakes are
    // sub-millisecond of CPU and Grafana holds connections open, so
    // serialization here is not a bottleneck at this milestone. A failed
    // handshake (scanner, plaintext probe) is logged and skipped — it
    // must never take the listener down.
    let incoming = futures::stream::unfold((listener, acceptor), |(listener, acceptor)| async {
        loop {
            match listener.accept().await {
                Ok((tcp, peer)) => match acceptor.accept(tcp).await {
                    Ok(tls_stream) => {
                        return Some((Ok::<_, std::io::Error>(tls_stream), (listener, acceptor)));
                    }
                    Err(e) => {
                        tracing::warn!(%peer, error = %e, "flight TLS handshake failed");
                        continue;
                    }
                },
                Err(e) => return Some((Err(e), (listener, acceptor))),
            }
        }
    });

    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(TimelordFlight::new(backend)))
        .serve_with_incoming(incoming)
        .await?;
    Ok(())
}
