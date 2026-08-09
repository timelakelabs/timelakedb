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
use std::sync::{Arc, OnceLock};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes,
    CommandGetTables, CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    Ticket,
};
use futures::{Stream, TryStreamExt, stream};
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

/// Future alias so implementors don't need a futures dependency.
pub type SqlFuture<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<Vec<RecordBatch>, String>> + Send + 'a>>;

/// What the blanket `FlightService` impl expects back from every DoGet.
type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

/// Flight SQL addresses tables as catalog.db_schema.table; TimelordDB has
/// database.table. A database is reported as the catalog, and every table
/// lives in one fixed schema — which is also what the SQL side registers,
/// so `poc.public.events` means the same thing through either interface.
const DB_SCHEMA: &str = "public";
const TABLE_TYPE: &str = "BASE TABLE";

/// The seam to the engine (implemented by `timelord_server::Engine`).
pub trait SqlBackend: Send + Sync + 'static {
    /// `authorizations` are the session's visibility authorizations
    /// (SEC-2), from `x-timelord-authorizations` request metadata.
    fn query_batches<'a>(
        &'a self,
        db: String,
        sql: String,
        authorizations: Vec<String>,
    ) -> SqlFuture<'a>;

    /// Databases holding data — one catalog each, for `CommandGetCatalogs`.
    fn databases(&self) -> Vec<String>;

    /// Tables in one database, buffered or catalogued.
    fn tables(&self, db: &str) -> Vec<String>;

    /// Merged schema for one table, when it has any data behind it.
    fn table_schema(&self, db: &str, table: &str) -> Option<SchemaRef>;
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
    md.get("x-timelord-authorizations")
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
        b.append(SqlInfo::FlightSqlServerName, "TimelordDB");
        b.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
        b.append(SqlInfo::FlightSqlServerArrowVersion, "58");
        b.append(SqlInfo::FlightSqlServerReadOnly, true);
        b.append(SqlInfo::SqlDdlCatalog, false);
        b.append(SqlInfo::SqlDdlSchema, false);
        b.append(SqlInfo::SqlDdlTable, false);
        b.build().expect("static SqlInfo data")
    })
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
        let auths = auths_from_metadata(request.metadata());
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

        let batches = self
            .backend
            .query_batches(db, sql, auths)
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
        fn query_batches<'a>(
            &'a self,
            _db: String,
            _sql: String,
            _authorizations: Vec<String>,
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
                .add_service(FlightServiceServer::new(TimelordFlight::new(Arc::new(
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
}
