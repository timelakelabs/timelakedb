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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::server::PeekableFlightDataStream;
use arrow_flight::sql::{
    CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes,
    CommandGetTables, CommandStatementIngest, CommandStatementQuery, ProstMessageExt, SqlInfo,
    TicketStatementQuery,
};
use arrow_flight::utils::flight_data_to_batches;
use arrow_flight::{
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    Ticket,
};
use futures::{Stream, StreamExt, TryStreamExt, stream};
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

/// Future alias so implementors don't need a futures dependency.
pub type SqlFuture<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<Vec<RecordBatch>, String>> + Send + 'a>>;

mod doput;

/// What the blanket `FlightService` impl expects back from every DoGet.
type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

/// Flight SQL addresses tables as catalog.db_schema.table; TimeLakeDB has
/// database.table. A database is reported as the catalog, and every table
/// lives in one fixed schema — which is also what the SQL side registers,
/// so `poc.public.events` means the same thing through either interface.
const DB_SCHEMA: &str = "public";
const TABLE_TYPE: &str = "BASE TABLE";

/// The seam to the engine (implemented by `timelake_server::Engine`).
pub trait SqlBackend: Send + Sync + 'static {
    /// Authenticate one read call from the raw `Authorization` metadata
    /// value (SEC-4 phased). `Ok(None)` — proceed, claims untouched
    /// (anonymous, or a token with no grant policy). `Ok(Some(grants))`
    /// — proceed, and intersect the caller's claimed SEC-2
    /// authorizations with `grants`. The implementation is the same
    /// `decide` the HTTP router uses, so the two doors cannot drift
    /// into different policies.
    fn authenticate_read(
        &self,
        authorization: Option<&str>,
        db: &str,
    ) -> Result<Option<Vec<String>>, timelake_auth::TokenError>;

    /// `authorizations` are the session's visibility authorizations
    /// (SEC-2), from `x-timelake-authorizations` request metadata.
    /// `identity` is the verified client-certificate CN, when the caller
    /// presented one: the backend intersects its claims with what that
    /// identity is granted (exposures 7/9). `None` is anonymous.
    fn query_batches<'a>(
        &'a self,
        db: String,
        sql: String,
        authorizations: Vec<String>,
        identity: Option<String>,
    ) -> SqlFuture<'a>;

    /// SEC-6 (exposure 6): admit one query for a client, keyed by `key` (a
    /// hash of the authorization metadata when present, else the peer
    /// address, else `None` when neither is available). Returns an opaque
    /// guard to hold for the query's lifetime, or `None` when the client is
    /// at its per-client concurrency cap and the handler must refuse with
    /// `ResourceExhausted`. The default never limits — only the real engine,
    /// which owns the limiter, enforces the cap.
    fn admit_client(&self, _key: Option<String>) -> Option<Box<dyn Send>> {
        Some(Box::new(()))
    }

    /// Databases holding data — one catalog each, for `CommandGetCatalogs`.
    fn databases(&self) -> Vec<String>;

    /// Tables in one database, buffered or catalogued.
    fn tables(&self, db: &str) -> Vec<String>;

    /// Merged schema for one table, when it has any data behind it.
    fn table_schema(&self, db: &str, table: &str) -> Option<SchemaRef>;

    /// Authenticate one WRITE call (DoPut, #79). `Ok(())` — the caller may
    /// write. This is the mirror of [`Self::authenticate_read`]: a token
    /// scoped read-only is refused here, so DoPut is a separate write-scoped
    /// door, NOT an exception carved into the read-only SQL guard (P0-2).
    /// The default is open, matching the data plane's default-off posture.
    fn authenticate_write(
        &self,
        _authorization: Option<&str>,
        _db: &str,
    ) -> Result<(), timelake_auth::TokenError> {
        Ok(())
    }

    /// Write line protocol through the SAME durable path an HTTP write uses —
    /// WAL fsync before ack, replication, LWW, SEC-2, and the #98 schema-union
    /// conflict all below this seam. DoPut serializes its Arrow rows to line
    /// protocol and calls in here rather than reaching under the engine. The
    /// default refuses, so only the real engine ingests.
    fn write_lp(&self, _db: &str, _body: &[u8]) -> Result<usize, PutError> {
        Err(PutError::Internal(
            "DoPut is not supported by this backend".into(),
        ))
    }
}

/// A write failure on the DoPut path, mapped to the gRPC status the client
/// expects: a bad batch is `InvalidArgument`, the WAL cap is
/// `ResourceExhausted` (the gRPC 429), anything else `Internal`.
#[derive(Debug)]
pub enum PutError {
    BadRequest(String),
    Backpressure(String),
    Internal(String),
}

#[derive(Clone)]
pub struct TimeLakeFlight {
    backend: Arc<dyn SqlBackend>,
}

impl TimeLakeFlight {
    pub fn new(backend: Arc<dyn SqlBackend>) -> Self {
        TimeLakeFlight { backend }
    }
}

fn db_from_metadata(md: &tonic::metadata::MetadataMap) -> String {
    for key in ["database", "bucket-name", "bucket", "db"] {
        if let Some(v) = md.get(key).and_then(|v| v.to_str().ok())
            && !v.is_empty()
        {
            return v.to_string();
        }
    }
    "poc".to_string()
}

/// Visibility authorizations (SEC-2) ride gRPC metadata the same way the
/// database does, comma-separated. Like the db, they are captured at
/// GetFlightInfo time into the ticket, because some clients DoGet on a
/// fresh connection that no longer carries the metadata.
fn auths_from_metadata(md: &tonic::metadata::MetadataMap) -> Vec<String> {
    md.get("x-timelake-authorizations")
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

fn authorization_from_metadata(md: &tonic::metadata::MetadataMap) -> Option<String> {
    md.get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// A non-cryptographic digest so a bearer token becomes a stable per-client
/// key (SEC-6) without the raw secret ever being stored as a map key.
/// Collisions only conflate two clients' limits, never widen access.
fn short_hash(s: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

/// A refusal, in the grammar gRPC clients understand. Grafana surfaces
/// `Unauthenticated` as a datasource auth failure rather than a query
/// error, which is exactly the prompt an operator needs.
fn deny(e: timelake_auth::TokenError) -> Status {
    match e {
        timelake_auth::TokenError::Forbidden => Status::permission_denied(e.message()),
        _ => Status::unauthenticated(e.message()),
    }
}

/// Authenticate one call and fold the token's grants into the claimed
/// authorizations. Every handler passes through here — including DoGet,
/// because a ticket is an opaque handle a client can craft: planning-time
/// authentication alone would let a forged ticket skip the door.
fn put_error_to_status(e: PutError) -> Status {
    match e {
        PutError::BadRequest(m) => Status::invalid_argument(m),
        PutError::Backpressure(m) => Status::resource_exhausted(m),
        PutError::Internal(m) => Status::internal(m),
    }
}

fn resolve_auths<B: SqlBackend + ?Sized>(
    backend: &B,
    md: &tonic::metadata::MetadataMap,
    db: &str,
    mut claimed: Vec<String>,
) -> Result<Vec<String>, Status> {
    let granted = backend
        .authenticate_read(authorization_from_metadata(md).as_deref(), db)
        .map_err(deny)?;
    if let Some(granted) = granted {
        claimed.retain(|c| granted.iter().any(|g| g == c));
    }
    Ok(claimed)
}

/// Every metadata command answers GetFlightInfo the same way: a ticket that
/// is the command itself (the server dispatches on it in DoGet) plus the
/// schema of the batch that DoGet will return.
fn metadata_info(
    command: impl ProstMessageExt,
    schema: &Schema,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    let ticket = Ticket {
        ticket: command.as_any().encode_to_vec().into(),
    };
    let info = FlightInfo::new()
        .try_with_schema(schema)
        .map_err(|e| Status::internal(format!("encode schema: {e}")))?
        .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
        .with_descriptor(request.into_inner());
    Ok(Response::new(info))
}

/// Metadata responses are always one small batch.
fn one_batch(
    schema: SchemaRef,
    batch: Result<RecordBatch, arrow_flight::error::FlightError>,
) -> Response<DoGetStream> {
    let stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(stream::once(async move { batch }))
        .map_err(Status::from);
    Response::new(Box::pin(stream))
}

/// Constant server capabilities, built once. Deliberately honest: reads are
/// SQL, writes arrive as line protocol over HTTP, and there is no DDL — so
/// the server reports itself read-only rather than advertising an INSERT
/// path that does not exist.
fn sql_info() -> &'static SqlInfoData {
    static DATA: OnceLock<SqlInfoData> = OnceLock::new();
    DATA.get_or_init(|| {
        let mut b = SqlInfoDataBuilder::new();
        b.append(SqlInfo::FlightSqlServerName, "TimeLakeDB");
        b.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
        // The arrow crate's own version, not a literal: "58" sat here through
        // the DataFusion 55 bump that took arrow to 59 (#39).
        b.append(SqlInfo::FlightSqlServerArrowVersion, arrow::ARROW_VERSION);
        b.append(SqlInfo::FlightSqlServerReadOnly, true);
        b.append(SqlInfo::SqlDdlCatalog, false);
        b.append(SqlInfo::SqlDdlSchema, false);
        b.append(SqlInfo::SqlDdlTable, false);
        b.build().expect("static SqlInfo data")
    })
}

#[tonic::async_trait]
impl FlightSqlService for TimeLakeFlight {
    type FlightService = TimeLakeFlight;

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
        let auths = resolve_auths(
            self.backend.as_ref(),
            request.metadata(),
            &db,
            auths_from_metadata(request.metadata()),
        )?;
        let handle =
            serde_json::json!({ "db": db, "sql": query.query, "auths": auths }).to_string();
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
        request: Request<Ticket>,
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
        // ticket auths first (captured at planning), unioned with any on
        // this DoGet call — a client cannot LOSE authorizations between
        // the two, and gaining some only widens what was already theirs.
        let mut auths: Vec<String> = parsed["auths"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str())
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();
        for a in auths_from_metadata(request.metadata()) {
            if !auths.contains(&a) {
                auths.push(a);
            }
        }
        // Re-authenticate at execution: the ticket is client-crafted
        // bytes, so planning-time auth alone is a door with no lock on
        // the second entrance. Narrowing the union again is idempotent
        // for honest clients and decisive for forged tickets.
        let auths = resolve_auths(self.backend.as_ref(), request.metadata(), &db, auths)?;

        // The verified client-certificate identity for THIS connection, if
        // the caller presented one (SEC-3 want mode). tonic puts the
        // connection's `connect_info` in every request's extensions; the
        // backend intersects the caller's claims with what this identity
        // is granted (exposures 7/9). It is applied on DoGet — where the
        // rows actually flow — not at planning.
        // TLS connections carry FlightConn (identity + peer addr); the
        // plaintext listener carries tonic's own connect info, whose
        // remote_addr() the key falls back to.
        let conn = request.extensions().get::<FlightConn>();
        let identity = conn.and_then(|c| c.identity.0.clone());

        // SEC-6: cap concurrent queries per client — the token (its metadata
        // value, hashed) when the caller presents one, else the peer
        // address. The guard is held across the query and releases the slot
        // on drop; a client already at its cap is refused before it runs.
        let client_key = authorization_from_metadata(request.metadata())
            .map(|a| format!("tok:{:016x}", short_hash(&a)))
            .or_else(|| {
                conn.and_then(|c| c.addr)
                    .or_else(|| request.remote_addr())
                    .map(|a| format!("ip:{}", a.ip()))
            });
        let _slot = match self.backend.admit_client(client_key) {
            Some(g) => g,
            None => {
                return Err(Status::resource_exhausted(
                    "too many concurrent queries for this client (SEC-6)",
                ));
            }
        };

        let batches = self
            .backend
            .query_batches(db, sql, auths, identity)
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

    // ---- metadata: what a BI tool asks before it will show you anything ----

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.into_builder().schema(); // Copy, so `query` survives
        metadata_info(query, &schema, request)
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let mut builder = query.into_builder();
        for db in self.backend.databases() {
            builder.append(db);
        }
        let schema = builder.schema();
        Ok(one_batch(schema, builder.build()))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder().schema();
        metadata_info(query, &schema, request)
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let mut builder = query.into_builder();
        // one schema per database; the builder applies the client's filters
        for db in self.backend.databases() {
            builder.append(db, DB_SCHEMA);
        }
        let schema = builder.schema();
        Ok(one_batch(schema, builder.build()))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder().schema();
        metadata_info(query, &schema, request)
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        // Scope to the database the client asked for, falling back to the
        // one this connection is bound to. Listing every database's tables
        // would advertise tables that queries on this connection cannot
        // reach — the database comes from request metadata, not from SQL.
        let db = query
            .catalog
            .clone()
            .filter(|c| !c.is_empty())
            .unwrap_or_else(|| db_from_metadata(request.metadata()));
        resolve_auths(self.backend.as_ref(), request.metadata(), &db, vec![])?;

        let mut builder = query.into_builder();
        for table in self.backend.tables(&db) {
            let schema = self
                .backend
                .table_schema(&db, &table)
                .unwrap_or_else(|| Arc::new(Schema::empty()));
            builder
                .append(&db, DB_SCHEMA, &table, TABLE_TYPE, &schema)
                .map_err(|e| Status::internal(format!("encode table metadata: {e}")))?;
        }
        let schema = builder.schema();
        Ok(one_batch(schema, builder.build()))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.into_builder().schema(); // Copy, so `query` survives
        metadata_info(query, &schema, request)
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let mut builder = query.into_builder();
        builder.append(TABLE_TYPE); // there is only one kind of table here
        let schema = builder.schema();
        Ok(one_batch(schema, builder.build()))
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder(sql_info()).schema();
        metadata_info(query, &schema, request)
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        // an empty `info` list means "everything"
        let builder = query.into_builder(sql_info());
        let schema = builder.schema();
        Ok(one_batch(schema, builder.build()))
    }

    /// DoPut / bulk ingest (#79): an Arrow stream lands on the same write path
    /// line protocol uses. Write-scoped — a read-only token is refused, the
    /// mirror of the read guard; DoPut is NOT an exception carved into P0-2's
    /// read-only SQL guard. A column type that disagrees with the table
    /// conflicts exactly as a bad line-protocol field does (#98), because the
    /// rows go through the same write_lp seam.
    async fn do_put_statement_ingest(
        &self,
        ticket: CommandStatementIngest,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let table = ticket.table.clone();
        if table.is_empty() {
            return Err(Status::invalid_argument(
                "CommandStatementIngest.table is required",
            ));
        }
        // db: FlightSQL schema (db_schema), else catalog, else the `database`
        // metadata, else the default — the resolution DoGet already uses.
        let db = ticket
            .schema
            .clone()
            .or_else(|| ticket.catalog.clone())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| db_from_metadata(request.metadata()));
        self.backend
            .authenticate_write(
                authorization_from_metadata(request.metadata()).as_deref(),
                &db,
            )
            .map_err(deny)?;

        // The framework PEEKED the first message (schema + descriptor) rather
        // than consuming it, so iterating the peekable yields it first —
        // exactly what flight_data_to_batches expects as the schema message.
        let mut stream = request.into_inner();
        let mut datas: Vec<FlightData> = Vec::new();
        while let Some(fd) = stream.next().await {
            datas.push(fd?);
        }
        let batches = flight_data_to_batches(&datas).map_err(|e| {
            Status::invalid_argument(format!("DoPut stream is not decodable Arrow: {e}"))
        })?;

        // Every batch -> rows -> line protocol, accumulated into ONE write so
        // the whole DoPut is atomic: a conflict in any batch rejects all of it,
        // exactly as a poison line rejects a line-protocol request.
        let mut lp = String::new();
        let mut nrows = 0usize;
        for batch in &batches {
            let rows = doput::batch_to_rows(&table, batch).map_err(Status::invalid_argument)?;
            nrows += rows.len();
            lp.push_str(&timelake_ingest::to_line_protocol(&rows));
        }
        if nrows == 0 {
            return Ok(0);
        }
        let written = self
            .backend
            .write_lp(&db, lp.as_bytes())
            .map_err(put_error_to_status)?;
        Ok(written as i64)
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
        .add_service(FlightServiceServer::new(TimeLakeFlight::new(backend)))
        .serve(addr)
        .await
}

/// Serve Flight SQL over TLS (SEC-3). The `ServerConfig` comes from
/// `timelake_tls::RotatingCert::server_config` — its cert resolver is
/// consulted per handshake, so cert rotation needs no listener restart
/// and never touches established gRPC streams.
/// The identity of a peer that presented a verified client certificate.
/// `None` means anonymous — served exactly as before (SEC-3 want mode).
/// Reaches handlers through tonic's connection info.
#[derive(Debug, Clone, Default)]
pub struct PeerIdentity(pub Option<String>);

/// The connection info tonic hands every request on the TLS listener: the
/// verified certificate identity (SEC-3) and the peer address (SEC-6, so
/// the per-client limiter can key on origin when no token is presented).
/// The plaintext listener uses tonic's own `TcpConnectInfo` instead, whose
/// `remote_addr()` the handler falls back to.
#[derive(Debug, Clone, Default)]
pub struct FlightConn {
    pub identity: PeerIdentity,
    pub addr: Option<std::net::SocketAddr>,
}

/// How many connections took each path.
///
/// Want mode's whole problem is that it is invisible: the anonymous and
/// authenticated paths both return 200, so nothing tells an operator
/// whether anyone is actually presenting certificates yet. Flipping a
/// listener to *require* one without knowing that ratio is how you take
/// an outage. These two counters are the measurement that decision
/// should rest on.
#[derive(Debug, Default)]
pub struct ClientAuthCounts {
    pub authenticated: AtomicU64,
    pub anonymous: AtomicU64,
}

/// Wraps the TLS stream so the verified identity travels with the
/// connection: tonic reads `connect_info` once per connection and puts
/// it in every request's extensions.
pub struct IdentifiedStream {
    inner: tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
    identity: PeerIdentity,
    peer: std::net::SocketAddr,
}

impl tonic::transport::server::Connected for IdentifiedStream {
    type ConnectInfo = FlightConn;
    fn connect_info(&self) -> Self::ConnectInfo {
        FlightConn {
            identity: self.identity.clone(),
            addr: Some(self.peer),
        }
    }
}

impl tokio::io::AsyncRead for IdentifiedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for IdentifiedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

pub async fn serve_tls(
    backend: Arc<dyn SqlBackend>,
    addr: std::net::SocketAddr,
    tls: Arc<rustls::ServerConfig>,
    counts: Arc<ClientAuthCounts>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let acceptor = tokio_rustls::TlsAcceptor::from(tls);
    tracing::info!(%addr, "flight sql listening (TLS)");

    // Handshakes run inline on the accept loop: rustls handshakes are
    // sub-millisecond of CPU and Grafana holds connections open, so
    // serialization here is not a bottleneck at this milestone. A failed
    // handshake (scanner, plaintext probe) is logged and skipped — it
    // must never take the listener down.
    let incoming = futures::stream::unfold(
        (listener, acceptor, counts),
        |(listener, acceptor, counts)| async {
            loop {
                match listener.accept().await {
                    Ok((tcp, peer)) => match acceptor.accept(tcp).await {
                        Ok(tls_stream) => {
                            // Want mode: the peer may or may not have offered
                            // a certificate, and either way it is served. If
                            // it did, rustls has already verified it against
                            // the rotating bundle; this only reads out who.
                            let identity = PeerIdentity(timelake_tls::identity_of(
                                tls_stream.get_ref().1.peer_certificates(),
                            ));
                            match &identity.0 {
                                Some(who) => {
                                    counts.authenticated.fetch_add(1, Ordering::Relaxed);
                                    tracing::debug!(
                                        %peer, identity = %who, "flight client authenticated"
                                    );
                                }
                                None => {
                                    counts.anonymous.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            return Some((
                                Ok::<_, std::io::Error>(IdentifiedStream {
                                    inner: tls_stream,
                                    identity,
                                    peer,
                                }),
                                (listener, acceptor, counts),
                            ));
                        }
                        Err(e) => {
                            tracing::warn!(%peer, error = %e, "flight TLS handshake failed");
                            continue;
                        }
                    },
                    Err(e) => return Some((Err(e), (listener, acceptor, counts))),
                }
            }
        },
    );

    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(TimeLakeFlight::new(backend)))
        .serve_with_incoming(incoming)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use arrow_flight::sql::client::FlightSqlServiceClient;

    /// Two databases, one of them with tables — enough to tell "scoped to
    /// this connection" apart from "everything on the node".
    struct StubBackend;

    impl SqlBackend for StubBackend {
        fn authenticate_read(
            &self,
            _authorization: Option<&str>,
            _db: &str,
        ) -> Result<Option<Vec<String>>, timelake_auth::TokenError> {
            // The stub is a mode-off node: anonymous proceeds, claims
            // untouched. The mode matrix itself is pinned in the auth
            // crate and in the server's data_auth integration tests.
            Ok(None)
        }

        fn query_batches<'a>(
            &'a self,
            _db: String,
            _sql: String,
            _authorizations: Vec<String>,
            _identity: Option<String>,
        ) -> SqlFuture<'a> {
            Box::pin(async { Ok(Vec::new()) })
        }
        fn databases(&self) -> Vec<String> {
            vec!["metrics".to_string(), "poc".to_string()]
        }
        fn tables(&self, db: &str) -> Vec<String> {
            match db {
                "poc" => vec!["disk_metrics".to_string(), "pipeline_events".to_string()],
                "metrics" => vec!["host_metrics".to_string()],
                _ => Vec::new(),
            }
        }
        fn table_schema(&self, _db: &str, _table: &str) -> Option<SchemaRef> {
            Some(Arc::new(Schema::new(vec![Field::new(
                "time",
                DataType::Int64,
                false,
            )])))
        }
    }

    /// Start a server on an ephemeral port and connect a real Flight SQL
    /// client to it — the calls that used to answer Unimplemented.
    async fn client() -> FlightSqlServiceClient<tonic::transport::Channel> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            tonic::transport::Server::builder()
                .add_service(FlightServiceServer::new(TimeLakeFlight::new(Arc::new(
                    StubBackend,
                ))))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .expect("connect to the test server");
        FlightSqlServiceClient::new(channel)
    }

    async fn rows(
        client: &mut FlightSqlServiceClient<tonic::transport::Channel>,
        info: FlightInfo,
    ) -> RecordBatch {
        let ticket = info.endpoint[0].ticket.clone().unwrap();
        let batches: Vec<RecordBatch> = client
            .do_get(ticket)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
    }

    fn column(batch: &RecordBatch, name: &str) -> Vec<String> {
        let col = batch.column_by_name(name).unwrap();
        let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
        (0..arr.len()).map(|i| arr.value(i).to_string()).collect()
    }

    #[tokio::test]
    async fn catalogs_are_databases() {
        let mut c = client().await;
        let info = c.get_catalogs().await.unwrap();
        let batch = rows(&mut c, info).await;
        assert_eq!(column(&batch, "catalog_name"), vec!["metrics", "poc"]);
    }

    #[tokio::test]
    async fn schemas_are_public_per_database() {
        let mut c = client().await;
        let info = c.get_db_schemas(Default::default()).await.unwrap();
        let batch = rows(&mut c, info).await;
        assert_eq!(column(&batch, "catalog_name"), vec!["metrics", "poc"]);
        assert_eq!(column(&batch, "db_schema_name"), vec!["public", "public"]);
    }

    #[tokio::test]
    async fn tables_are_scoped_to_the_requested_catalog() {
        let mut c = client().await;
        let info = c
            .get_tables(arrow_flight::sql::CommandGetTables {
                catalog: Some("poc".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let batch = rows(&mut c, info).await;
        assert_eq!(
            column(&batch, "table_name"),
            vec!["disk_metrics", "pipeline_events"]
        );
        assert_eq!(column(&batch, "catalog_name"), vec!["poc", "poc"]);
        assert_eq!(
            column(&batch, "table_type"),
            vec!["BASE TABLE", "BASE TABLE"]
        );

        // a different catalog must not leak this one's tables
        let info = c
            .get_tables(arrow_flight::sql::CommandGetTables {
                catalog: Some("metrics".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let batch = rows(&mut c, info).await;
        assert_eq!(column(&batch, "table_name"), vec!["host_metrics"]);
    }

    #[tokio::test]
    async fn tables_can_carry_the_arrow_schema() {
        let mut c = client().await;
        let info = c
            .get_tables(arrow_flight::sql::CommandGetTables {
                catalog: Some("metrics".to_string()),
                include_schema: true,
                ..Default::default()
            })
            .await
            .unwrap();
        let batch = rows(&mut c, info).await;
        assert!(
            batch.column_by_name("table_schema").is_some(),
            "include_schema must add the IPC-encoded schema column"
        );
    }

    #[tokio::test]
    async fn table_types_and_sql_info_answer() {
        let mut c = client().await;
        let info = c.get_table_types().await.unwrap();
        let batch = rows(&mut c, info).await;
        assert_eq!(column(&batch, "table_type"), vec!["BASE TABLE"]);

        let info = c.get_sql_info(vec![]).await.unwrap();
        let batch = rows(&mut c, info).await;
        assert!(
            batch.num_rows() >= 4,
            "an empty info list means all of them, got {}",
            batch.num_rows()
        );
    }

    async fn client_with(
        backend: Arc<dyn SqlBackend>,
    ) -> FlightSqlServiceClient<tonic::transport::Channel> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            tonic::transport::Server::builder()
                .add_service(FlightServiceServer::new(TimeLakeFlight::new(backend)))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .expect("connect to the test server");
        FlightSqlServiceClient::new(channel)
    }

    /// Records the line protocol the DoPut path produces, so a test can assert
    /// the Arrow-batch -> rows -> LP conversion end to end over the wire.
    #[derive(Default)]
    struct CapturingBackend {
        writes: std::sync::Mutex<Vec<(String, String)>>,
        deny_write: bool,
    }

    impl SqlBackend for CapturingBackend {
        fn authenticate_read(
            &self,
            _a: Option<&str>,
            _db: &str,
        ) -> Result<Option<Vec<String>>, timelake_auth::TokenError> {
            Ok(None)
        }
        fn query_batches<'a>(
            &'a self,
            _db: String,
            _sql: String,
            _au: Vec<String>,
            _id: Option<String>,
        ) -> SqlFuture<'a> {
            Box::pin(async { Ok(Vec::new()) })
        }
        fn databases(&self) -> Vec<String> {
            Vec::new()
        }
        fn tables(&self, _db: &str) -> Vec<String> {
            Vec::new()
        }
        fn table_schema(&self, _db: &str, _t: &str) -> Option<SchemaRef> {
            None
        }
        fn authenticate_write(
            &self,
            _a: Option<&str>,
            _db: &str,
        ) -> Result<(), timelake_auth::TokenError> {
            if self.deny_write {
                Err(timelake_auth::TokenError::Forbidden)
            } else {
                Ok(())
            }
        }
        fn write_lp(&self, db: &str, body: &[u8]) -> Result<usize, PutError> {
            let lp = String::from_utf8(body.to_vec()).unwrap();
            let n = timelake_ingest::parse_lines(&lp, 1, 0)
                .map(|r| r.len())
                .unwrap_or(0);
            self.writes.lock().unwrap().push((db.to_string(), lp));
            Ok(n)
        }
    }

    fn ingest_batch() -> RecordBatch {
        use arrow::array::{
            ArrayRef, Float64Array, StringDictionaryBuilder, TimestampNanosecondArray,
        };
        use arrow::datatypes::Int32Type;
        let time: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            1_000_000_i64,
            2_000_000,
        ]));
        let mut hb = StringDictionaryBuilder::<Int32Type>::new();
        hb.append_value("h1");
        hb.append_value("h2");
        let host: ArrayRef = Arc::new(hb.finish());
        let temp: ArrayRef = Arc::new(Float64Array::from(vec![1.5, 2.5]));
        RecordBatch::try_from_iter(vec![("time", time), ("host", host), ("temp", temp)]).unwrap()
    }

    #[tokio::test]
    async fn do_put_ingests_an_arrow_batch_over_the_write_path() {
        let backend = Arc::new(CapturingBackend::default());
        let mut c = client_with(backend.clone()).await;
        let command = CommandStatementIngest {
            table: "weather".to_string(),
            schema: Some("flt".to_string()),
            ..Default::default()
        };
        let n = c
            .execute_ingest(command, futures::stream::iter(vec![Ok(ingest_batch())]))
            .await
            .unwrap();
        assert_eq!(n, 2, "two rows ingested");

        let writes = backend.writes.lock().unwrap();
        assert_eq!(writes.len(), 1);
        let (db, lp) = &writes[0];
        assert_eq!(db, "flt", "the FlightSQL schema field selects the database");
        // The batch survives as line protocol: dict column -> tag, f64 -> field,
        // time -> ns timestamp, table from the command.
        let rows = timelake_ingest::parse_lines(lp, 1, 0).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].table, "weather");
        assert_eq!(rows[0].tags, vec![("host".to_string(), "h1".to_string())]);
        assert_eq!(
            rows[0].fields,
            vec![("temp".to_string(), timelake_ingest::FieldValue::Float(1.5))]
        );
        assert_eq!(rows[0].timestamp_ns, 1_000_000);
    }

    #[tokio::test]
    async fn do_put_requires_write_scope() {
        let backend = Arc::new(CapturingBackend {
            deny_write: true,
            ..Default::default()
        });
        let mut c = client_with(backend.clone()).await;
        let command = CommandStatementIngest {
            table: "weather".to_string(),
            ..Default::default()
        };
        let err = c
            .execute_ingest(command, futures::stream::iter(vec![Ok(ingest_batch())]))
            .await
            .expect_err("a read-only caller must be refused");
        assert!(
            backend.writes.lock().unwrap().is_empty(),
            "a denied DoPut must not write anything"
        );
        let msg = format!("{err:?}").to_lowercase();
        assert!(
            msg.contains("permission") || msg.contains("denied") || msg.contains("forbidden"),
            "expected a permission error, got: {msg}"
        );
    }
}
