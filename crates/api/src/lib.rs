//! HTTP surface (FR-1, FR-9): /write (v1), /api/v2/write (v2),
//! /api/v3/write_lp (v3-style), /api/sql (harness/debug), /health, /ping,
//! /metrics. Speaks gzip Content-Encoding (Telegraf's influxdb_v2 output
//! default) and the s|ms|us|ns precision parameters; success is 204,
//! parse errors are 400 with the offending line identified.
//!
//! The router is generic over the [`Engine`] trait so this crate owns the
//! wire contract while timelake-server owns the machinery.

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use std::net::SocketAddr;

use axum::body::Bytes;
use axum::extract::{ConnectInfo, Extension, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use timelake_auth::{Action, Auth, Decision, Role, Scope, SessionInfo, TokenError};

/// The database wildcard in a retention policy: this table, in *every*
/// database.
///
/// It exists because until 2026-08-19 it was the only behaviour there was
/// — and it was implicit. `enforce_retention` matched on table name alone
/// and ignored `FileMeta::db`, so a policy an operator set on `metrics`
/// silently deleted `metrics` in every database on the node. That is a
/// deletion control doing more than it was told to, which is the one
/// direction a deletion control must never fail in.
///
/// The wildcard is kept, rather than removed, because existing stored
/// policies mean exactly this and migrating them to anything narrower
/// would silently stop deleting data an operator asked to have deleted.
/// What changed is that it is now **explicit and visible**: a policy says
/// which databases it covers, and a new one has to say so deliberately.
pub const RETENTION_ANY_DB: &str = "*";

/// One FR-7 retention policy: keep `db`.`table` for `seconds`, then drop
/// whole expired partitions.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RetentionPolicy {
    /// The database this applies to, or [`RETENTION_ANY_DB`] for all.
    pub db: String,
    pub table: String,
    pub seconds: u64,
}

impl RetentionPolicy {
    /// Does this policy govern `db`.`table`?
    pub fn covers(&self, db: &str, table: &str) -> bool {
        self.table == table && (self.db == RETENTION_ANY_DB || self.db == db)
    }

    /// True when this policy reaches into every database.
    pub fn is_wildcard(&self) -> bool {
        self.db == RETENTION_ANY_DB
    }
}

/// A downsampling aggregate: `function(source_column) AS target_column`, over
/// the rows in one time bucket. The set is deliberately the aggregates that
/// are **recomputable from source** — each can be recomputed exactly from a
/// bucket's raw rows, which is what makes re-materialisation idempotent
/// (ARCHITECTURE §18).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RollupFn {
    Avg,
    Min,
    Max,
    Sum,
    Count,
    First,
    Last,
    /// Distinct count of the source column in the bucket. Recomputable from
    /// source (one pass over the bucket's rows) but not algebraically
    /// combinable — which is fine here, because materialisation never
    /// combines partials, it computes each bucket once from raw rows (§18.6).
    CountDistinct,
    /// Approximate continuous percentile (`approx_percentile_cont`), quantile
    /// in `RollupAgg::quantile`. Approximate on purpose: an exact percentile
    /// over a wide `lookback` is expensive, and the recompute-from-source
    /// idempotency argument holds either way (§18.6).
    Percentile,
}

impl RollupFn {
    /// Whether this function takes a `quantile` argument. Only `Percentile`
    /// does; the check lives here so `validate` and the SQL builder agree.
    pub fn takes_quantile(self) -> bool {
        matches!(self, RollupFn::Percentile)
    }
}

// No `Eq`: `quantile` is an `f64`, which has no total equality. `PartialEq`
// is all the tests and upserts need.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RollupAgg {
    pub function: RollupFn,
    pub source_column: String,
    pub target_column: String,
    /// The quantile for `Percentile` (0.0–1.0); `None` for every other
    /// function. Enforced both ways in [`RollupDef::validate`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantile: Option<f64>,
}

/// One R-2 rollup definition: continuously downsample `db`.`source` into
/// `db`.`target` at `interval_secs` resolution, materialised on the
/// maintenance tick into an ordinary table that carries its own retention.
/// See ARCHITECTURE §18. This is configuration, not SQL DDL — the read-only
/// SQL guard (P0-2) refuses `CREATE MATERIALIZED VIEW`, and a standing
/// aggregate-and-delete control belongs behind the same admin auth as
/// retention.
// No `Eq`: a `RollupAgg` may carry an `f64` quantile. `PartialEq` suffices.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RollupDef {
    pub db: String,
    /// Unique per database; identifies the definition for update and remove.
    pub name: String,
    pub source: String,
    pub target: String,
    /// The time bucket, in seconds.
    pub interval_secs: u64,
    /// How far back each materialisation pass re-aggregates and overwrites,
    /// in seconds — the bound on how late a write can arrive and still be
    /// picked up. Must be ≥ `interval_secs`, and (checked against engine
    /// state) shorter than the source's retention (§18.4).
    pub lookback_secs: u64,
    /// Tag columns to keep in the target. Empty means every tag of the
    /// source, resolved from its schema at materialisation.
    pub group_by: Vec<String>,
    pub aggregations: Vec<RollupAgg>,
    /// Optional SQL boolean expression on the source rows, ANDed into the
    /// bucket scan before aggregation (`region = 'eu'`, `status_code >= 500`).
    /// It is recomputable like everything else here — the same predicate runs
    /// every pass — so it does not disturb idempotency (§18.6). Admin-authored
    /// and run under the read-only guard; a malformed expression fails the
    /// pass loudly rather than corrupting the target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
}

impl RollupDef {
    /// Structural validation only. The retention invariant (§18.4) needs the
    /// engine's policies and is checked in `set_rollup`.
    pub fn validate(&self) -> Result<(), String> {
        if self.db.trim().is_empty() {
            return Err("db must not be empty: name the database this rollup runs in".into());
        }
        if self.name.trim().is_empty() {
            return Err("rollup name must not be empty".into());
        }
        if self.source.trim().is_empty() {
            return Err("source table must not be empty".into());
        }
        if self.target.trim().is_empty() {
            return Err("target table must not be empty".into());
        }
        if self.source == self.target {
            return Err("source and target must differ — a rollup cannot feed itself".into());
        }
        if self.interval_secs == 0 {
            return Err("interval must be > 0".into());
        }
        if self.lookback_secs < self.interval_secs {
            return Err(
                "lookback must be ≥ interval: a pass has to cover at least one whole bucket".into(),
            );
        }
        if self.aggregations.is_empty() {
            return Err("a rollup needs at least one aggregation".into());
        }
        let mut seen = std::collections::HashSet::new();
        for a in &self.aggregations {
            if a.source_column.trim().is_empty() || a.target_column.trim().is_empty() {
                return Err("each aggregation needs a source_column and a target_column".into());
            }
            if a.target_column == "time" {
                return Err("a target_column may not be named 'time' (the bucket column)".into());
            }
            if !seen.insert(a.target_column.clone()) {
                return Err(format!(
                    "two aggregations write the same target_column {:?}",
                    a.target_column
                ));
            }
            // `quantile` belongs to `percentile` and nowhere else — enforced
            // both directions so a typo (quantile on `avg`, or a percentile
            // with no quantile) is a 400 at definition, not a rollup that
            // silently fails every pass.
            match (a.function.takes_quantile(), a.quantile) {
                (true, None) => {
                    return Err(format!(
                        "aggregation {:?} is a percentile and needs a quantile (0.0–1.0)",
                        a.target_column
                    ));
                }
                (true, Some(q)) if !(0.0..=1.0).contains(&q) => {
                    return Err(format!(
                        "percentile quantile for {:?} must be within 0.0–1.0, got {q}",
                        a.target_column
                    ));
                }
                (false, Some(_)) => {
                    return Err(format!(
                        "aggregation {:?} takes no quantile — only percentile does",
                        a.target_column
                    ));
                }
                _ => {}
            }
        }
        if let Some(f) = &self.filter
            && f.trim().is_empty()
        {
            return Err("filter, if given, must not be blank".into());
        }
        for g in &self.group_by {
            if seen.contains(g) {
                return Err(format!(
                    "group_by tag {g:?} collides with an aggregation's target_column"
                ));
            }
        }
        Ok(())
    }
}

/// Errors the write path can surface over the wire.
pub enum WriteError {
    /// 400 — the request is at fault (parse error, type conflict, bad
    /// precision); the message identifies the line/field (FR-9).
    BadRequest(String),
    /// 429 + Retry-After — a named, visible limit (RR-5): the WAL is at
    /// its cap and flush needs to catch up before more writes land.
    Backpressure(String),
    /// 500 — the engine failed to make the write durable.
    Internal(String),
    /// 501 — this node does not take writes at all (CL-3: a querier is a
    /// read replica). Distinct from 400/500 on purpose: nothing about the
    /// request is wrong and nothing here is broken, so a client must go to
    /// the router or an ingester rather than retry, and a monitor must not
    /// read it as an engine fault.
    NotHere(String),
}

/// The seam between HTTP and the engine. Implemented by
/// `timelake_server::Engine`; mockable for endpoint tests.
pub trait Engine: Send + Sync + 'static {
    /// Parse, make durable (WAL), and apply one line-protocol body.
    /// Returns the number of lines written. Blocking (fsync) — the
    /// router calls it via `spawn_blocking`.
    fn write_lp(&self, db: &str, body: &[u8], precision: Option<&str>)
    -> Result<usize, WriteError>;

    /// Execute SQL against one database; returns a JSON array of row
    /// objects (the /api/sql wire contract). `authorizations` are the
    /// session's visibility authorizations (SEC-2), from the
    /// `X-TimeLake-Authorizations` header and/or the request body.
    /// `identity` is the verified client-certificate CN when the caller
    /// presented one (SEC-3 v2 want mode), `None` otherwise. It can only
    /// narrow what the session sees — see [`PeerIdentity`].
    fn sql(
        &self,
        db: String,
        query: String,
        authorizations: Vec<String>,
        identity: Option<String>,
    ) -> impl std::future::Future<Output = Result<Value, String>> + Send;

    /// Authenticate a data-plane request (SEC-4 phased).
    ///
    /// The router does not decide policy — it hands over the raw
    /// `Authorization` header and is told yes or no. That keeps HTTP and
    /// Flight SQL on one implementation of the rule instead of two that
    /// drift.
    fn authenticate_data(
        &self,
        authorization: Option<&str>,
        action: Action,
        db: &str,
    ) -> Result<Decision, TokenError>;

    /// SEC-6 (exposure 6): admit one query for a client, keyed by
    /// `key` (the handler passes the data-plane token id when present, else
    /// the network origin, else `None` when the transport cannot attribute
    /// the caller). Returns an opaque guard to hold for the query's lifetime
    /// or `None` when the client is already at its per-client concurrency
    /// cap and the handler must refuse (429 / `ResourceExhausted`).
    ///
    /// The default never limits — a mock engine and the router inherit it —
    /// so only the real engine, which owns the limiter, enforces the cap.
    fn admit_client(&self, _key: Option<String>) -> Option<Box<dyn Send>> {
        Some(Box::new(()))
    }

    /// Prometheus exposition text (SR-4).
    fn metrics_text(&self) -> String;

    /// U0b (§3): the layered configuration surface backing `/admin/config`.
    /// The applied revision (§3.8), and the whole `default < property <
    /// override` stack with provenance per key.
    fn config_revision(&self) -> u64;
    fn config_provenance(&self, key: &str) -> Option<Value>;
    fn config_provenance_all(&self) -> Vec<Value>;
    /// Set an override (a value, or `None` for explicit-none) and apply it;
    /// returns the new revision. Validates the proposed whole config (§3.6).
    fn set_config(
        &self,
        key: &str,
        value: Option<String>,
        actor: &str,
        at: &str,
    ) -> Result<u64, timelake_config::ConfigSetError>;
    /// Remove an override, reverting to the property/default. `true` if one was
    /// present.
    fn revert_config(&self, key: &str) -> Result<bool, timelake_config::ConfigSetError>;
    /// Validate a proposed override and return the provenance it WOULD have,
    /// without applying it (`?dry_run=1`).
    fn config_dry_run(
        &self,
        key: &str,
        value: Option<String>,
        actor: &str,
        at: &str,
    ) -> Result<Value, timelake_config::ConfigSetError>;

    /// U1 (§6): a filtered page of the node's application log for
    /// `/admin/logs`. Newest first; `min_level` keeps entries at or above that
    /// severity.
    fn app_logs(
        &self,
        min_level: &str,
        target: Option<&str>,
        contains: Option<&str>,
        limit: usize,
    ) -> Value;

    /// U3 (§8): the node's self-reported health — status/name/version plus id,
    /// role, applied config revision, catalog head and uptime — the row the
    /// cluster view aggregates. Answered from atomics, not the query path.
    fn node_health(&self) -> Value;

    /// Current retention policies (FR-7).
    fn retention_policies(&self) -> Vec<RetentionPolicy>;

    /// Set (upsert) one policy, durably. `db` may be [`RETENTION_ANY_DB`].
    fn set_retention(&self, db: &str, table: &str, seconds: u64) -> Result<(), String>;

    /// Remove one policy (that table keeps everything again).
    fn remove_retention(&self, db: &str, table: &str) -> Result<(), String>;

    /// Last-value cache opt-in (#57), backing `/admin/last_cache`. A table is
    /// cached only after `enable_last_cache`; nothing is stamped by default,
    /// because the cache is bounded and accelerates hot series, not all series.
    fn last_cache_tables(&self) -> Vec<(String, String)>;
    fn enable_last_cache(&self, db: &str, table: &str) -> Result<(), String>;
    fn disable_last_cache(&self, db: &str, table: &str) -> Result<(), String>;

    /// R-2 rollups (ARCHITECTURE §18): the runtime downsampling surface,
    /// backing `/admin/rollups`. `set_rollup` upserts by `(db, name)` and
    /// enforces the retention invariant (§18.4); materialisation runs on the
    /// maintenance tick, not on this surface.
    fn rollups(&self) -> Vec<RollupDef>;
    fn set_rollup(&self, def: RollupDef) -> Result<(), String>;
    fn remove_rollup(&self, db: &str, name: &str) -> Result<(), String>;

    /// R-1 targeted delete: record a durable tombstone that hides every row
    /// matching (all `tag_equals` AND the `[min_ts_ns, max_ts_ns]` window)
    /// from every query at once. Returns (tombstone id, manifest seq).
    /// Rejects an empty predicate — that would erase the whole table.
    fn delete_where(
        &self,
        db: &str,
        table: &str,
        tag_equals: Vec<(String, String)>,
        min_ts_ns: Option<i64>,
        max_ts_ns: Option<i64>,
    ) -> Result<(String, u64), String>;
}

/// Parse "365d", "72h", "90m", or bare seconds — the write half of the
/// same grammar `TIMELAKE_RETENTION` seeds with.
pub fn parse_duration_secs(s: &str) -> Option<u64> {
    let s = s.trim();
    let (num, unit) = match s.chars().last()? {
        'd' => (&s[..s.len() - 1], 86_400),
        'h' => (&s[..s.len() - 1], 3_600),
        'm' => (&s[..s.len() - 1], 60),
        's' => (&s[..s.len() - 1], 1),
        _ => (s, 1),
    };
    let n: u64 = num.trim().parse().ok()?;
    n.checked_mul(unit).filter(|v| *v > 0)
}

/// Render seconds in the largest exact unit — the inverse of
/// [`parse_duration_secs`], for display.
pub fn humanize_secs(secs: u64) -> String {
    if secs.is_multiple_of(86_400) {
        format!("{}d", secs / 86_400)
    } else if secs.is_multiple_of(3_600) {
        format!("{}h", secs / 3_600)
    } else if secs.is_multiple_of(60) {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

/// The verified client-certificate identity for a connection, as a request
/// extension (SEC-3 v2 on the HTTP surface).
///
/// `None` means the caller presented no certificate, or the listener is
/// plaintext — both are ordinary, because client authentication is **want**
/// mode: a caller without a certificate is served exactly as before. So this
/// can only ever *narrow* what a session sees, never widen it, which is the
/// property exposures 7 and 9 rest on.
///
/// The value is the leaf's subject common name, extracted once per
/// connection at the TLS accept (`timelake_server::tls_identity`) rather
/// than per request — the certificate cannot change mid-connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerIdentity(pub Option<String>);

/// State for the authenticated admin surface (SEC-4).
pub struct AdminState<E: Engine> {
    engine: Arc<E>,
    auth: Arc<Auth>,
    /// P1-2 (SR-6): every mutating handler records through this chain.
    audit: Arc<timelake_audit::AuditLog>,
    /// Mark session cookies `Secure` — set when the listener is TLS.
    secure_cookies: bool,
}

impl<E: Engine> Clone for AdminState<E> {
    fn clone(&self) -> Self {
        AdminState {
            engine: self.engine.clone(),
            auth: self.auth.clone(),
            audit: self.audit.clone(),
            secure_cookies: self.secure_cookies,
        }
    }
}

/// Observability only: `/health`, `/ping`, `/metrics`. Nothing else.
///
/// For a node that does maintenance and serves no clients — the compactor
/// role (C2 phase 5). It holds no buffer, takes no writes and answers no
/// queries, so mounting the data plane on it would advertise four
/// endpoints that must never be used and one (`/api/sql`) that would
/// answer from a catalog view kept fresh only incidentally.
///
/// Built as its own router rather than by filtering `app`'s: a route
/// removed by subtraction comes back the moment someone adds one to the
/// list above and does not think about this caller. Additive is the only
/// direction that stays correct by default.
pub fn maintenance_app<E: Engine>(engine: Arc<E>) -> Router {
    Router::new()
        .route("/health", get(health::<E>))
        .route("/ping", get(ping::<E>).head(ping::<E>))
        .route("/metrics", get(metrics::<E>))
        .with_state(engine)
}

/// The combined router (data plane + admin plane on one `Router`). Used by the
/// in-process test harness and by callers that want a single service.
///
/// Production does NOT serve this: U0 split the admin plane onto its own
/// private listener, so the two are served separately — `data_app` on the
/// public data port (1963) and `admin_app` on the private admin port (1966).
/// Keeping the merged form is a test convenience; the split behaviour (410 on
/// the data port, auth on the admin port) is exercised against `data_app` /
/// `admin_app` directly.
pub fn app<E: Engine>(
    engine: Arc<E>,
    auth: Arc<Auth>,
    audit: Arc<timelake_audit::AuditLog>,
    secure_cookies: bool,
) -> Router {
    data_routes(engine.clone()).merge(admin_app(engine, auth, audit, secure_cookies))
}

/// The public data plane (port 1963): writes, `/api/sql`, `/health`, `/ping`,
/// `/metrics` — plus a `410 Gone` stub over the whole `/admin/*` space, which
/// moved to the private admin listener at U0. `/metrics` stays here on purpose:
/// it is unauthenticated by design and Prometheus scrapes it.
pub fn data_app<E: Engine>(engine: Arc<E>) -> Router {
    data_routes(engine).route("/admin/{*rest}", axum::routing::any(admin_moved))
}

/// The private admin plane (port 1966, loopback by default): the console shell,
/// the login endpoint, and every guarded `/admin/*` route. The most destructive
/// surface in the system, kept off the exposed data port.
pub fn admin_app<E: Engine>(
    engine: Arc<E>,
    auth: Arc<Auth>,
    audit: Arc<timelake_audit::AuditLog>,
    secure_cookies: bool,
) -> Router {
    let state = AdminState {
        engine,
        auth,
        audit,
        secure_cookies,
    };

    // Guarded: every one of these needs a session. The guard also
    // enforces the forced-rotation lockout and CSRF.
    let guarded = Router::new()
        .route(
            "/admin/retention",
            get(retention_list::<E>).put(retention_set::<E>),
        )
        .route(
            "/admin/retention/{db}/{table}",
            axum::routing::delete(retention_delete::<E>),
        )
        .route(
            "/admin/last_cache",
            get(last_cache_list::<E>).put(last_cache_set::<E>),
        )
        .route(
            "/admin/last_cache/{db}/{table}",
            axum::routing::delete(last_cache_delete::<E>),
        )
        .route("/admin/config", get(config_list::<E>))
        .route(
            "/admin/config/{key}",
            get(config_get::<E>)
                .put(config_set::<E>)
                .delete(config_delete::<E>),
        )
        .route("/admin/logs", get(logs_list::<E>))
        .route(
            "/admin/rollups",
            get(rollups_list::<E>).put(rollups_set::<E>),
        )
        .route(
            "/admin/rollups/{db}/{name}",
            axum::routing::delete(rollups_delete::<E>),
        )
        .route("/admin/delete", post(admin_delete::<E>))
        .route("/admin/audit", get(audit_list::<E>))
        .route(
            "/admin/session",
            get(session_show::<E>).delete(session_logout::<E>),
        )
        .route("/admin/password", post(change_password::<E>))
        .route(
            "/admin/tokens",
            get(tokens_list::<E>).post(tokens_issue::<E>),
        )
        .route(
            "/admin/tokens/{id}",
            axum::routing::delete(tokens_revoke::<E>),
        )
        .route("/admin/cert-grants", get(cert_grants_list::<E>))
        .route(
            "/admin/cert-grants/{cn}",
            axum::routing::put(cert_grants_set::<E>).delete(cert_grants_remove::<E>),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            admin_guard::<E>,
        ))
        .with_state(state.clone());

    // Public: the page shell (it contains no data — it asks for it) and
    // the login endpoint itself.
    let public = Router::new()
        .route("/admin/ui", get(admin_ui))
        .route("/admin/session", post(session_login::<E>))
        .with_state(state);

    guarded.merge(public)
}

/// The data-plane routes, shared by `app` (merged with the live admin plane)
/// and `data_app` (merged with the `/admin/*` gone-stub). No `/admin/*` here.
fn data_routes<E: Engine>(engine: Arc<E>) -> Router {
    Router::new()
        .route("/health", get(health::<E>))
        .route("/ping", get(ping::<E>).head(ping::<E>))
        .route("/metrics", get(metrics::<E>))
        .route("/write", post(write_v1::<E>))
        .route("/api/v2/write", post(write_v2::<E>))
        .route("/api/v3/write_lp", post(write_v3::<E>))
        .route("/api/v1/write", post(write_prometheus::<E>))
        .route(
            "/api/sql",
            post(sql::<E>).layer(axum::middleware::from_fn_with_state(
                engine.clone(),
                rate_limit_sql::<E>,
            )),
        )
        .with_state(engine)
}

/// `410 Gone` for the admin API on the data port: it moved to the private
/// admin listener (U0). Not a redirect — the target is loopback by default, so
/// a browser cannot follow it; the body names where it went.
async fn admin_moved() -> impl axum::response::IntoResponse {
    (
        axum::http::StatusCode::GONE,
        axum::Json(serde_json::json!({
            "error": "the admin API moved off the data port to the private admin listener",
            "listener": "TIMELAKE_ADMIN_ADDR (default 127.0.0.1:1966)",
        })),
    )
}

/// The management page (FR-7 GUI + SEC-4 login): a single self-contained
/// file — no build step, no external assets, same rules as `site/`. It
/// ships no data; it asks for it, and every one of those calls is
/// authenticated.
async fn admin_ui() -> axum::response::Html<&'static str> {
    axum::response::Html(include_str!("admin_ui.html"))
}

const SESSION_COOKIE: &str = "tldb_admin_session";
const CSRF_HEADER: &str = "x-timelake-csrf";

fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get("cookie")?
        .to_str()
        .ok()?
        .split(';')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| k.trim() == name)
        .map(|(_, v)| v.trim().to_string())
}

/// SEC-4 gate for every admin route. Order matters: authenticate, then
/// CSRF, then the forced-rotation lockout — a caller who has not proved
/// who they are gets no information about what they would be asked next.
async fn admin_guard<E: Engine>(
    State(state): State<AdminState<E>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let headers = req.headers().clone();
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    // Bearer for automation, cookie for browsers.
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_string);
    let cookie = cookie_value(&headers, SESSION_COOKIE);
    let token = bearer.clone().or(cookie.clone());

    let Some(session) = token.as_deref().and_then(|t| state.auth.session(t)) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "authentication required", "code": "unauthenticated" })),
        )
            .into_response();
    };

    let mutating = !matches!(method, axum::http::Method::GET | axum::http::Method::HEAD);
    if mutating && bearer.is_none() {
        // Cookie-authenticated mutation: a form on any page the operator
        // visits could otherwise drive this API. Double-submit token plus
        // an Origin sanity check.
        let presented = headers.get(CSRF_HEADER).and_then(|v| v.to_str().ok());
        if presented != Some(session.csrf.as_str()) {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "CSRF token missing or wrong", "code": "csrf" })),
            )
                .into_response();
        }
        if let Some(origin) = headers.get("origin").and_then(|v| v.to_str().ok()) {
            let host = headers
                .get("host")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let origin_host = origin.rsplit("//").next().unwrap_or("");
            if !host.is_empty() && origin_host != host {
                return (
                    StatusCode::FORBIDDEN,
                    Json(json!({ "error": "cross-origin request refused", "code": "origin" })),
                )
                    .into_response();
            }
        }
    }

    // Forced rotation: the seeded credential can do exactly one thing.
    if session.must_change_password && !(path == "/admin/password" || (path == "/admin/session")) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "the password must be changed before this credential can do anything else",
                "code": "password_change_required",
            })),
        )
            .into_response();
    }

    let mut req = req;
    req.extensions_mut().insert(session);
    next.run(req).await
}

/// Standalone SEC-4 guard requiring the `admin` role, for admin routes
/// that live outside the main router (the TLS reload endpoint, which
/// the server composes only when TLS is on).
pub async fn require_admin_session(
    State(auth): State<Arc<Auth>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let headers = req.headers().clone();
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_string)
        .or_else(|| cookie_value(&headers, SESSION_COOKIE));

    let Some(session) = token.as_deref().and_then(|t| auth.session(t)) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "authentication required", "code": "unauthenticated" })),
        )
            .into_response();
    };
    if session.must_change_password {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "the password must be changed first",
                "code": "password_change_required",
            })),
        )
            .into_response();
    }
    if let Some(deny) = require(&session, Role::Admin) {
        return deny;
    }
    next.run(req).await
}

fn session_of(req: &axum::http::Extensions) -> SessionInfo {
    req.get::<SessionInfo>()
        .cloned()
        .expect("admin_guard inserts the session before any handler runs")
}

fn require(session: &SessionInfo, needed: Role) -> Option<axum::response::Response> {
    (!session.role.allows(needed)).then(|| {
        (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": format!(
                    "role '{}' is not sufficient; '{}' required",
                    session.role.as_str(),
                    needed.as_str()
                ),
                "code": "forbidden",
            })),
        )
            .into_response()
    })
}

#[derive(Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

async fn session_login<E: Engine>(
    State(state): State<AdminState<E>>,
    Json(req): Json<LoginRequest>,
) -> axum::response::Response {
    match state.auth.login(&req.username, &req.password) {
        Ok((token, info)) => {
            let secure = if state.secure_cookies { "; Secure" } else { "" };
            let cookie =
                format!("{SESSION_COOKIE}={token}; HttpOnly; SameSite=Strict; Path=/admin{secure}");
            (
                StatusCode::OK,
                [(axum::http::header::SET_COOKIE, cookie)],
                Json(json!({
                    "username": info.username,
                    "role": info.role.as_str(),
                    "must_change_password": info.must_change_password,
                    "csrf": info.csrf,
                })),
            )
                .into_response()
        }
        Err(e @ timelake_auth::LoginError::RateLimited(_)) => (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({ "error": e.to_string(), "code": "rate_limited" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": e.to_string(), "code": "invalid_credentials" })),
        )
            .into_response(),
    }
}

async fn session_show<E: Engine>(
    State(state): State<AdminState<E>>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let s = session_of(req.extensions());
    Json(json!({
        "username": s.username,
        "role": s.role.as_str(),
        "must_change_password": s.must_change_password,
        "csrf": s.csrf,
        "default_credential_active": state.auth.default_credential_active(),
    }))
    .into_response()
}

async fn session_logout<E: Engine>(
    State(state): State<AdminState<E>>,
    headers: HeaderMap,
) -> axum::response::Response {
    if let Some(t) = cookie_value(&headers, SESSION_COOKIE) {
        state.auth.logout(&t);
    }
    let secure = if state.secure_cookies { "; Secure" } else { "" };
    (
        StatusCode::OK,
        [(
            axum::http::header::SET_COOKIE,
            format!("{SESSION_COOKIE}=; HttpOnly; SameSite=Strict; Path=/admin; Max-Age=0{secure}"),
        )],
        Json(json!({ "status": "logged out" })),
    )
        .into_response()
}

#[derive(Deserialize)]
struct PasswordChange {
    current_password: String,
    new_password: String,
}

async fn change_password<E: Engine>(
    State(state): State<AdminState<E>>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    let source = source_of(req.extensions());
    let body = match axum::body::to_bytes(req.into_body(), 64 * 1024).await {
        Ok(b) => b,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let change: PasswordChange = match serde_json::from_slice(&body) {
        Ok(c) => c,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    if let Some(resp) = audit_gate(&state.audit) {
        return resp;
    }
    // A password change is audited as an event, never with its values — the
    // record carries who rotated their own credential, not the secret.
    let target = Some(session.username.clone());
    match state.auth.change_password(
        &session.username,
        &change.current_password,
        &change.new_password,
    ) {
        // Every session for this principal — including this one — is
        // invalidated by the rotation, so the client must log in again.
        Ok(()) => {
            let ev = audit_event(
                &session,
                source,
                "password.change",
                target,
                None,
                None,
                "ok",
            );
            if let Some(resp) = audit_record(&state.audit, ev) {
                return resp;
            }
            Json(json!({
                "status": "password changed",
                "reauthenticate": true,
            }))
            .into_response()
        }
        Err(e) => {
            let ev = audit_event(
                &session,
                source,
                "password.change",
                target,
                None,
                None,
                "denied",
            );
            let _ = state.audit.record(ev);
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": e, "code": "password_rejected" })),
            )
                .into_response()
        }
    }
}

#[derive(serde::Deserialize)]
struct LogsQuery {
    level: Option<String>,
    target: Option<String>,
    contains: Option<String>,
    limit: Option<usize>,
}

/// `GET /admin/logs` — a filtered snapshot of the node's application-log ring
/// (§6). Not the audit trail, and not the stored data: the recent operational
/// log, in memory and bounded, so a console can triage without shipping logs
/// into the database.
async fn logs_list<E: Engine>(
    State(state): State<AdminState<E>>,
    Query(q): Query<LogsQuery>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    if let Some(deny) = require(&session, Role::Viewer) {
        return deny;
    }
    let level = q.level.as_deref().unwrap_or("info");
    let limit = q.limit.unwrap_or(500).min(5000);
    Json(
        state
            .engine
            .app_logs(level, q.target.as_deref(), q.contains.as_deref(), limit),
    )
    .into_response()
}

/// Map a config-crate role requirement to the auth role that gates it.
fn config_role(r: timelake_config::Role) -> Role {
    match r {
        timelake_config::Role::Viewer => Role::Viewer,
        timelake_config::Role::Operator => Role::Operator,
        timelake_config::Role::Admin => Role::Admin,
    }
}

/// `PUT /admin/config/{key}` body: `{"value": <string|number|bool|null>}`.
/// `null` is explicit-none (§3.3); a scalar is stringified for the resolver.
#[derive(serde::Deserialize)]
struct ConfigSetBody {
    value: Value,
}

fn config_value_text(v: &Value) -> Result<Option<String>, String> {
    match v {
        Value::Null => Ok(None),
        Value::String(s) => Ok(Some(s.clone())),
        Value::Number(n) => Ok(Some(n.to_string())),
        Value::Bool(b) => Ok(Some(b.to_string())),
        _ => Err("value must be a string, number, boolean, or null".to_string()),
    }
}

/// A config write error mapped to a status: a resolver rejection is `409` (the
/// request is well-formed but violates a constraint), a store failure `500`.
fn config_err_response(e: timelake_config::ConfigSetError) -> axum::response::Response {
    let code = match e {
        timelake_config::ConfigSetError::Rejected(_) => StatusCode::CONFLICT,
        timelake_config::ConfigSetError::Store(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    err_response(code, &e.to_string())
}

/// The override `at` timestamp, from the same source as the audit trail.
fn config_at() -> String {
    timelake_audit::rfc3339_utc(std::time::SystemTime::now())
}

/// `GET /admin/config` — every setting with full provenance, plus the revision.
async fn config_list<E: Engine>(State(state): State<AdminState<E>>) -> axum::response::Response {
    Json(json!({
        "revision": state.engine.config_revision(),
        "settings": state.engine.config_provenance_all(),
    }))
    .into_response()
}

/// `GET /admin/config/{key}` — one setting's provenance.
async fn config_get<E: Engine>(
    State(state): State<AdminState<E>>,
    axum::extract::Path(key): axum::extract::Path<String>,
) -> axum::response::Response {
    match state.engine.config_provenance(&key) {
        Some(p) => Json(p).into_response(),
        None => err_response(StatusCode::NOT_FOUND, &format!("unknown setting `{key}`")),
    }
}

/// `PUT /admin/config/{key}` — set an override (value or `null`). `?dry_run=1`
/// validates and previews without applying.
async fn config_set<E: Engine>(
    State(state): State<AdminState<E>>,
    axum::extract::Path(key): axum::extract::Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    let source = source_of(req.extensions());

    let Some(spec) = timelake_config::spec(&key) else {
        return err_response(StatusCode::NOT_FOUND, &format!("unknown setting `{key}`"));
    };
    if let Some(deny) = require(&session, config_role(spec.min_role)) {
        return deny;
    }

    let body = match axum::body::to_bytes(req.into_body(), 64 * 1024).await {
        Ok(b) => b,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let set: ConfigSetBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return err_response(
                StatusCode::BAD_REQUEST,
                &format!("body must be {{\"value\": <string|number|bool|null>}}: {e}"),
            );
        }
    };
    let value = match config_value_text(&set.value) {
        Ok(v) => v,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &e),
    };

    let dry_run = matches!(
        params.get("dry_run").map(String::as_str),
        Some("1") | Some("true")
    );
    let at = config_at();
    if dry_run {
        return match state
            .engine
            .config_dry_run(&key, value, &session.username, &at)
        {
            Ok(preview) => Json(json!({ "dry_run": true, "would_apply": preview })).into_response(),
            Err(e) => config_err_response(e),
        };
    }

    if let Some(resp) = audit_gate(&state.audit) {
        return resp;
    }
    let before = state
        .engine
        .config_provenance(&key)
        .map(|p| p["effective"].clone());
    match state.engine.set_config(&key, value, &session.username, &at) {
        Ok(revision) => {
            let after = state
                .engine
                .config_provenance(&key)
                .map(|p| p["effective"].clone());
            let ev = audit_event(
                &session,
                source,
                "config.set",
                Some(key.clone()),
                before,
                after,
                "ok",
            );
            if let Some(resp) = audit_record(&state.audit, ev) {
                return resp;
            }
            Json(json!({ "key": key, "revision": revision, "status": "applied" })).into_response()
        }
        Err(e) => {
            let ev = audit_event(
                &session,
                source,
                "config.set",
                Some(key.clone()),
                before,
                None,
                "rejected",
            );
            let _ = audit_record(&state.audit, ev);
            config_err_response(e)
        }
    }
}

/// `DELETE /admin/config/{key}` — revert an override to the property/default.
async fn config_delete<E: Engine>(
    State(state): State<AdminState<E>>,
    axum::extract::Path(key): axum::extract::Path<String>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    let source = source_of(req.extensions());
    let Some(spec) = timelake_config::spec(&key) else {
        return err_response(StatusCode::NOT_FOUND, &format!("unknown setting `{key}`"));
    };
    if let Some(deny) = require(&session, config_role(spec.min_role)) {
        return deny;
    }
    if let Some(resp) = audit_gate(&state.audit) {
        return resp;
    }
    let before = state
        .engine
        .config_provenance(&key)
        .map(|p| p["effective"].clone());
    match state.engine.revert_config(&key) {
        Ok(removed) => {
            let after = state
                .engine
                .config_provenance(&key)
                .map(|p| p["effective"].clone());
            let ev = audit_event(
                &session,
                source,
                "config.revert",
                Some(key.clone()),
                before,
                after,
                "ok",
            );
            if let Some(resp) = audit_record(&state.audit, ev) {
                return resp;
            }
            let status = if removed { "reverted" } else { "no override" };
            Json(json!({ "key": key, "status": status })).into_response()
        }
        Err(e) => config_err_response(e),
    }
}

async fn retention_list<E: Engine>(
    State(state): State<AdminState<E>>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    if let Some(deny) = require(&session, Role::Viewer) {
        return deny;
    }
    let engine = state.engine;
    let mut policies: Vec<Value> = engine
        .retention_policies()
        .into_iter()
        .map(|p| {
            json!({
                "db": p.db,
                "table": p.table,
                "seconds": p.seconds,
                "duration": humanize_secs(p.seconds),
                // Surfaced explicitly so an operator reading this list can
                // see which policies reach beyond one database. This was
                // the whole content of the hazard: every policy behaved
                // this way and nothing said so.
                "all_databases": p.is_wildcard(),
            })
        })
        .collect();
    policies.sort_by_key(|p| {
        (
            p["table"].as_str().unwrap_or("").to_string(),
            p["db"].as_str().unwrap_or("").to_string(),
        )
    });
    Json(json!({
        "policies": policies,
        "role": session.role.as_str(),
    }))
    .into_response()
}

#[derive(Deserialize)]
struct RetentionSet {
    /// Which database this governs, or `"*"` for every one of them.
    ///
    /// **Required, deliberately.** Omitting it used to be the only option
    /// and meant "all databases" silently; defaulting it to the wildcard
    /// now would keep that footgun loaded for every policy written from
    /// here on. An operator has to say which data they are scheduling for
    /// deletion, and `"*"` remains available for when they mean it.
    db: Option<String>,
    table: String,
    /// "365d", "72h", "90m", or seconds.
    duration: String,
}

async fn retention_set<E: Engine>(
    State(state): State<AdminState<E>>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    let source = source_of(req.extensions());
    let body = match axum::body::to_bytes(req.into_body(), 64 * 1024).await {
        Ok(b) => b,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let set: RetentionSet = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    let table = set.table.trim();
    if table.is_empty() {
        return err_response(StatusCode::BAD_REQUEST, "table must not be empty");
    }
    let Some(db) = set.db.as_deref().map(str::trim).filter(|d| !d.is_empty()) else {
        return err_response(
            StatusCode::BAD_REQUEST,
            "db is required: name the database this policy governs, or \"*\" for every \
             database. It was previously implicit and always meant \"*\", which deleted \
             same-named tables in databases the operator had not named",
        );
    };
    let Some(seconds) = parse_duration_secs(&set.duration) else {
        return err_response(
            StatusCode::BAD_REQUEST,
            &format!(
                "duration {:?} is not <n>d, <n>h, <n>m, or seconds (and must be > 0)",
                set.duration
            ),
        );
    };

    // The operator/admin split follows the data, not the verb: growing a
    // window keeps more, shrinking (or introducing one where none
    // existed) destroys. Only the destructive direction needs `admin`.
    let current = state
        .engine
        .retention_policies()
        .into_iter()
        .find(|p| p.db == db && p.table == table)
        .map(|p| p.seconds);
    // Widening the SCOPE is destructive too, even when the window is
    // unchanged: switching a policy to "*" starts expiring data in
    // databases that were never covered. Treated as introducing a policy,
    // because for those databases that is exactly what it is.
    let widening_scope = db == RETENTION_ANY_DB
        && current.is_none()
        && state
            .engine
            .retention_policies()
            .iter()
            .any(|p| p.table == table && !p.is_wildcard());
    let destructive = match current {
        Some(existing) => seconds < existing,
        None => true, // unbounded -> bounded: data starts expiring
    } || widening_scope;
    let needed = if destructive {
        Role::Admin
    } else {
        Role::Operator
    };
    if let Some(deny) = require(&session, needed) {
        return deny;
    }
    if let Some(resp) = audit_gate(&state.audit) {
        return resp;
    }

    // The audit target names the SCOPE, not just the table, so the trail
    // distinguishes "expire poc.metrics" from "expire metrics everywhere".
    let target = format!("{db}.{table}");
    let before = current.map(|s| json!({"seconds": s, "duration": humanize_secs(s)}));
    let after = json!({
        "db": db,
        "seconds": seconds,
        "duration": humanize_secs(seconds),
    });
    match state.engine.set_retention(db, table, seconds) {
        Ok(()) => {
            let ev = audit_event(
                &session,
                source,
                "retention.set",
                Some(target),
                before,
                Some(after),
                "ok",
            );
            if let Some(resp) = audit_record(&state.audit, ev) {
                return resp;
            }
            Json(json!({
                "db": db,
                "table": table,
                "seconds": seconds,
                "duration": humanize_secs(seconds),
                "status": "set",
                "destructive": destructive,
                "all_databases": db == RETENTION_ANY_DB,
            }))
            .into_response()
        }
        Err(e) => {
            let ev = audit_event(
                &session,
                source,
                "retention.set",
                Some(target),
                before,
                None,
                "error",
            );
            let _ = state.audit.record(ev);
            err_response(StatusCode::INTERNAL_SERVER_ERROR, &e)
        }
    }
}

async fn retention_delete<E: Engine>(
    State(state): State<AdminState<E>>,
    // Two segments: a policy is identified by (db, table), so removing one
    // has to name both. `*` is a legal database here and means the
    // all-databases policy — deleting `poc/metrics` must not silently
    // remove the wildcard that is still expiring every other database.
    axum::extract::Path((db, table)): axum::extract::Path<(String, String)>,
    req: axum::extract::Request,
) -> axum::response::Response {
    // Removing a policy makes the table grow without bound — governing
    // the node's storage, so it sits with `admin` alongside shrinking.
    let session = session_of(req.extensions());
    let source = source_of(req.extensions());
    if let Some(deny) = require(&session, Role::Admin) {
        return deny;
    }
    if let Some(resp) = audit_gate(&state.audit) {
        return resp;
    }
    let target = format!("{db}.{table}");
    let before = state
        .engine
        .retention_policies()
        .into_iter()
        .find(|p| p.db == db && p.table == table)
        .map(|p| json!({"db": p.db, "seconds": p.seconds, "duration": humanize_secs(p.seconds)}));
    match state.engine.remove_retention(&db, &table) {
        Ok(()) => {
            let ev = audit_event(
                &session,
                source,
                "retention.remove",
                Some(target),
                before,
                None,
                "ok",
            );
            if let Some(resp) = audit_record(&state.audit, ev) {
                return resp;
            }
            Json(json!({ "db": db, "table": table, "status": "removed" })).into_response()
        }
        Err(e) => {
            let ev = audit_event(
                &session,
                source,
                "retention.remove",
                Some(target),
                before,
                None,
                "error",
            );
            let _ = state.audit.record(ev);
            err_response(StatusCode::INTERNAL_SERVER_ERROR, &e)
        }
    }
}

/// `PUT /admin/last_cache` body: which table to cache (#57).
#[derive(serde::Deserialize)]
struct LastCacheSet {
    db: String,
    table: String,
}

async fn last_cache_list<E: Engine>(
    State(state): State<AdminState<E>>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    if let Some(deny) = require(&session, Role::Viewer) {
        return deny;
    }
    let tables: Vec<Value> = state
        .engine
        .last_cache_tables()
        .into_iter()
        .map(|(db, table)| json!({ "db": db, "table": table }))
        .collect();
    Json(json!({ "tables": tables })).into_response()
}

async fn last_cache_set<E: Engine>(
    State(state): State<AdminState<E>>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    let source = source_of(req.extensions());
    let body = match axum::body::to_bytes(req.into_body(), 64 * 1024).await {
        Ok(b) => b,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let set: LastCacheSet = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let (db, table) = (set.db.trim(), set.table.trim());
    if db.is_empty() || table.is_empty() {
        return err_response(StatusCode::BAD_REQUEST, "db and table must not be empty");
    }
    // A config change, not a data change — operator, like enabling a rollup.
    if let Some(deny) = require(&session, Role::Operator) {
        return deny;
    }
    if let Some(resp) = audit_gate(&state.audit) {
        return resp;
    }
    let target = format!("{db}.{table}");
    match state.engine.enable_last_cache(db, table) {
        Ok(()) => {
            let ev = audit_event(
                &session,
                source,
                "last_cache.enable",
                Some(target),
                None,
                None,
                "ok",
            );
            if let Some(resp) = audit_record(&state.audit, ev) {
                return resp;
            }
            Json(json!({ "db": db, "table": table, "status": "enabled" })).into_response()
        }
        Err(e) => {
            let ev = audit_event(
                &session,
                source,
                "last_cache.enable",
                Some(target),
                None,
                None,
                "error",
            );
            let _ = state.audit.record(ev);
            err_response(StatusCode::INTERNAL_SERVER_ERROR, &e)
        }
    }
}

async fn last_cache_delete<E: Engine>(
    State(state): State<AdminState<E>>,
    axum::extract::Path((db, table)): axum::extract::Path<(String, String)>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    let source = source_of(req.extensions());
    if let Some(deny) = require(&session, Role::Operator) {
        return deny;
    }
    if let Some(resp) = audit_gate(&state.audit) {
        return resp;
    }
    let target = format!("{db}.{table}");
    match state.engine.disable_last_cache(&db, &table) {
        Ok(()) => {
            let ev = audit_event(
                &session,
                source,
                "last_cache.disable",
                Some(target),
                None,
                None,
                "ok",
            );
            if let Some(resp) = audit_record(&state.audit, ev) {
                return resp;
            }
            Json(json!({ "db": db, "table": table, "status": "disabled" })).into_response()
        }
        Err(e) => {
            let ev = audit_event(
                &session,
                source,
                "last_cache.disable",
                Some(target),
                None,
                None,
                "error",
            );
            let _ = state.audit.record(ev);
            err_response(StatusCode::INTERNAL_SERVER_ERROR, &e)
        }
    }
}

/// A rollup definition as it arrives on the wire: durations are strings
/// (`"1m"`, `"7d"`), the stored form is seconds.
#[derive(Deserialize)]
struct RollupSet {
    db: String,
    name: String,
    source: String,
    /// Defaults to `{source}_{interval}`.
    target: Option<String>,
    /// "1m", "1h", "7d", or seconds — the time bucket.
    interval: String,
    /// How far back each pass re-aggregates. Defaults to `interval` (one
    /// bucket), which tolerates no out-of-order lag — set it to cover the
    /// real lateness of the source.
    lookback: Option<String>,
    /// Tag columns to keep; omitted or empty means every source tag.
    group_by: Option<Vec<String>>,
    aggregations: Vec<RollupAggSet>,
    /// SQL boolean expression ANDed into the source scan (`region = 'eu'`).
    #[serde(default)]
    filter: Option<String>,
}

#[derive(Deserialize)]
struct RollupAggSet {
    function: RollupFn,
    source_column: String,
    target_column: String,
    /// Quantile 0.0–1.0 for `percentile`; omitted for every other function.
    #[serde(default)]
    quantile: Option<f64>,
}

async fn rollups_list<E: Engine>(
    State(state): State<AdminState<E>>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    if let Some(deny) = require(&session, Role::Viewer) {
        return deny;
    }
    let mut rollups: Vec<Value> = state
        .engine
        .rollups()
        .into_iter()
        .map(|r| {
            json!({
                "db": r.db,
                "name": r.name,
                "source": r.source,
                "target": r.target,
                "interval": humanize_secs(r.interval_secs),
                "interval_secs": r.interval_secs,
                "lookback": humanize_secs(r.lookback_secs),
                "lookback_secs": r.lookback_secs,
                "group_by": r.group_by,
                "filter": r.filter,
                "aggregations": r.aggregations.iter().map(|a| json!({
                    "function": a.function,
                    "source_column": a.source_column,
                    "target_column": a.target_column,
                    "quantile": a.quantile,
                })).collect::<Vec<_>>(),
            })
        })
        .collect();
    rollups.sort_by_key(|r| {
        (
            r["db"].as_str().unwrap_or("").to_string(),
            r["name"].as_str().unwrap_or("").to_string(),
        )
    });
    Json(json!({ "rollups": rollups, "role": session.role.as_str() })).into_response()
}

async fn rollups_set<E: Engine>(
    State(state): State<AdminState<E>>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    let source_ip = source_of(req.extensions());
    // Introducing or replacing a rollup creates or abandons a table — a
    // storage-governing action, so `admin`, like the destructive half of
    // retention.
    if let Some(deny) = require(&session, Role::Admin) {
        return deny;
    }
    if let Some(resp) = audit_gate(&state.audit) {
        return resp;
    }
    let body = match axum::body::to_bytes(req.into_body(), 256 * 1024).await {
        Ok(b) => b,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let set: RollupSet = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    let Some(interval_secs) = parse_duration_secs(&set.interval) else {
        return err_response(
            StatusCode::BAD_REQUEST,
            &format!(
                "interval {:?} is not <n>d/<n>h/<n>m/seconds (> 0)",
                set.interval
            ),
        );
    };
    let lookback_secs = match set.lookback.as_deref() {
        None => interval_secs,
        Some(s) => match parse_duration_secs(s) {
            Some(v) => v,
            None => {
                return err_response(
                    StatusCode::BAD_REQUEST,
                    &format!("lookback {s:?} is not <n>d/<n>h/<n>m/seconds (> 0)"),
                );
            }
        },
    };
    let source = set.source.trim().to_string();
    let target = set
        .target
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("{source}_{}", set.interval.trim()));

    let def = RollupDef {
        db: set.db.trim().to_string(),
        name: set.name.trim().to_string(),
        source,
        target,
        interval_secs,
        lookback_secs,
        group_by: set.group_by.unwrap_or_default(),
        aggregations: set
            .aggregations
            .into_iter()
            .map(|a| RollupAgg {
                function: a.function,
                source_column: a.source_column.trim().to_string(),
                target_column: a.target_column.trim().to_string(),
                quantile: a.quantile,
            })
            .collect(),
        filter: set
            .filter
            .map(|f| f.trim().to_string())
            .filter(|f| !f.is_empty()),
    };

    let target_name = format!("{}.{}", def.db, def.name);
    let after = json!({
        "db": def.db, "name": def.name, "source": def.source, "target": def.target,
        "interval_secs": def.interval_secs, "lookback_secs": def.lookback_secs,
    });
    let before = state
        .engine
        .rollups()
        .into_iter()
        .find(|r| r.db == def.db && r.name == def.name)
        .map(|r| json!({"source": r.source, "target": r.target, "interval_secs": r.interval_secs}));

    match state.engine.set_rollup(def) {
        Ok(()) => {
            let ev = audit_event(
                &session,
                source_ip,
                "rollup.set",
                Some(target_name),
                before,
                Some(after.clone()),
                "ok",
            );
            if let Some(resp) = audit_record(&state.audit, ev) {
                return resp;
            }
            (
                StatusCode::OK,
                Json(json!({ "status": "set", "rollup": after })),
            )
                .into_response()
        }
        Err(e) => {
            let ev = audit_event(
                &session,
                source_ip,
                "rollup.set",
                Some(target_name),
                before,
                None,
                "denied",
            );
            let _ = state.audit.record(ev);
            // A rejected definition is the caller's — bad structure or the
            // retention invariant (§18.4). Store failures are rare and their
            // message says so.
            err_response(StatusCode::BAD_REQUEST, &e)
        }
    }
}

async fn rollups_delete<E: Engine>(
    State(state): State<AdminState<E>>,
    axum::extract::Path((db, name)): axum::extract::Path<(String, String)>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    let source_ip = source_of(req.extensions());
    // Removing the definition stops the target being fed; the target table
    // and its data stay until its own retention expires them. Admin.
    if let Some(deny) = require(&session, Role::Admin) {
        return deny;
    }
    if let Some(resp) = audit_gate(&state.audit) {
        return resp;
    }
    let target_name = format!("{db}.{name}");
    let before = state
        .engine
        .rollups()
        .into_iter()
        .find(|r| r.db == db && r.name == name)
        .map(|r| json!({"source": r.source, "target": r.target}));
    match state.engine.remove_rollup(&db, &name) {
        Ok(()) => {
            let ev = audit_event(
                &session,
                source_ip,
                "rollup.remove",
                Some(target_name),
                before,
                None,
                "ok",
            );
            if let Some(resp) = audit_record(&state.audit, ev) {
                return resp;
            }
            Json(json!({ "db": db, "name": name, "status": "removed" })).into_response()
        }
        Err(e) => {
            let ev = audit_event(
                &session,
                source_ip,
                "rollup.remove",
                Some(target_name),
                before,
                None,
                "error",
            );
            let _ = state.audit.record(ev);
            err_response(StatusCode::INTERNAL_SERVER_ERROR, &e)
        }
    }
}

/// A targeted-delete request (R-1). Timestamps are nanoseconds — the same
/// unit line protocol and the engine speak internally — so the window is
/// unambiguous and needs no timezone parsing on this admin surface.
#[derive(Deserialize)]
struct DeleteRequest {
    db: String,
    table: String,
    /// Tag equalities the row must ALL satisfy. Order-independent.
    #[serde(default)]
    tags: std::collections::BTreeMap<String, String>,
    /// Inclusive lower time bound, nanoseconds since the Unix epoch.
    #[serde(default)]
    start_ns: Option<i64>,
    /// Inclusive upper time bound, nanoseconds since the Unix epoch.
    #[serde(default)]
    end_ns: Option<i64>,
}

/// POST /admin/delete — record a targeted-delete tombstone (R-1). Deleting
/// data is irreversible and governs what the node stores, so it sits with
/// `admin`, the same bar as removing a retention policy.
async fn admin_delete<E: Engine>(
    State(state): State<AdminState<E>>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    let source = source_of(req.extensions());
    if let Some(deny) = require(&session, Role::Admin) {
        return deny;
    }
    let body = match axum::body::to_bytes(req.into_body(), 64 * 1024).await {
        Ok(b) => b,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let del: DeleteRequest = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    let db = del.db.trim();
    let table = del.table.trim();
    if db.is_empty() || table.is_empty() {
        return err_response(StatusCode::BAD_REQUEST, "db and table must not be empty");
    }
    let tag_equals: Vec<(String, String)> = del.tags.into_iter().collect();
    // The predicate as recorded in the audit trail — what was asked to be
    // deleted, independent of how many rows it matched.
    let predicate = json!({
        "tags": tag_equals
            .iter()
            .cloned()
            .collect::<std::collections::BTreeMap<String, String>>(),
        "start_ns": del.start_ns,
        "end_ns": del.end_ns,
    });
    let target = Some(format!("{db}.{table}"));

    if let Some(resp) = audit_gate(&state.audit) {
        return resp;
    }
    // The engine owns the policy (empty-predicate refusal, table existence,
    // window sanity) so HTTP and any future transport share one rule.
    match state
        .engine
        .delete_where(db, table, tag_equals, del.start_ns, del.end_ns)
    {
        Ok((id, seq)) => {
            let after = json!({"predicate": predicate, "tombstone_id": id, "seq": seq});
            let ev = audit_event(
                &session,
                source,
                "data.delete",
                target,
                None,
                Some(after),
                "ok",
            );
            if let Some(resp) = audit_record(&state.audit, ev) {
                return resp;
            }
            Json(json!({
                "db": db,
                "table": table,
                "tombstone_id": id,
                "seq": seq,
                "status": "recorded",
            }))
            .into_response()
        }
        // A rejected predicate or a missing table is the caller's error; a
        // failed commit is ours. Either way the attempt is audited (§5.1).
        Err(e) => {
            let is_client = e.contains("needs at least one predicate")
                || e.contains("does not exist")
                || e.contains("time window is empty")
                || e.contains("querier");
            let (code, outcome) = if is_client {
                (StatusCode::BAD_REQUEST, "denied")
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, "error")
            };
            let ev = audit_event(
                &session,
                source,
                "data.delete",
                target,
                None,
                Some(predicate),
                outcome,
            );
            let _ = state.audit.record(ev);
            err_response(code, &e)
        }
    }
}

/// `status`, `name`, `version` — the contract Gauge's adapter and the
/// compose healthchecks read. There used to be a fourth field, `milestone`,
/// typed as `"M3"` in August and never touched again while M4, M5 and five
/// cluster phases shipped: the first thing a new user curls told them
/// something false. Nothing parsed it (grep of every sibling repo), and
/// there is no milestone concept that survives the cluster phases, so it
/// is gone rather than bumped to a value that would rot the same way (#39).
async fn health<E: Engine>(State(engine): State<Arc<E>>) -> Json<Value> {
    Json(engine.node_health())
}

async fn ping<E: Engine>(
    State(_): State<Arc<E>>,
) -> (StatusCode, [(&'static str, &'static str); 1]) {
    (
        StatusCode::NO_CONTENT,
        [("x-timelakedb-version", env!("CARGO_PKG_VERSION"))],
    )
}

async fn metrics<E: Engine>(State(engine): State<Arc<E>>) -> String {
    engine.metrics_text()
}

type Params = Query<HashMap<String, String>>;

async fn write_v1<E: Engine>(
    state: State<Arc<E>>,
    Query(params): Params,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let Some(db) = params.get("db").cloned() else {
        return err_response(StatusCode::BAD_REQUEST, "missing 'db' parameter");
    };
    write_common(state.0, db, params.get("precision").cloned(), headers, body).await
}

async fn write_v2<E: Engine>(
    state: State<Arc<E>>,
    Query(params): Params,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    // v2 addresses a bucket; org is accepted and ignored (single-tenant M1)
    let Some(bucket) = params.get("bucket").cloned() else {
        return err_response(StatusCode::BAD_REQUEST, "missing 'bucket' parameter");
    };
    write_common(
        state.0,
        bucket,
        params.get("precision").cloned(),
        headers,
        body,
    )
    .await
}

async fn write_v3<E: Engine>(
    state: State<Arc<E>>,
    Query(params): Params,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let Some(db) = params.get("db").cloned() else {
        return err_response(StatusCode::BAD_REQUEST, "missing 'db' parameter");
    };
    write_common(state.0, db, params.get("precision").cloned(), headers, body).await
}

async fn write_common<E: Engine>(
    engine: Arc<E>,
    db: String,
    precision: Option<String>,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    if let Err(e) =
        engine.authenticate_data(authorization_of(&headers).as_deref(), Action::Write, &db)
    {
        return deny_response(e);
    }
    let body = match maybe_gunzip(&headers, body) {
        Ok(b) => b,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &format!("bad gzip body: {e}")),
    };
    let res =
        tokio::task::spawn_blocking(move || engine.write_lp(&db, &body, precision.as_deref()))
            .await;
    write_result_response(res)
}

/// The write-path response contract, shared by every write endpoint: 204 on
/// success, 400 for a bad body, 429 (with retry-after) at the WAL cap, 501 for
/// a wrong-node forward, 500 otherwise. A remote_write write is a write like
/// any other, so it answers on exactly these codes.
fn write_result_response(
    res: Result<Result<usize, WriteError>, tokio::task::JoinError>,
) -> axum::response::Response {
    match res {
        Ok(Ok(_lines)) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(WriteError::BadRequest(msg))) => err_response(StatusCode::BAD_REQUEST, &msg),
        Ok(Err(WriteError::Backpressure(msg))) => (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", "1")],
            Json(json!({ "error": msg })),
        )
            .into_response(),
        Ok(Err(WriteError::Internal(msg))) => err_response(StatusCode::INTERNAL_SERVER_ERROR, &msg),
        Ok(Err(WriteError::NotHere(msg))) => err_response(StatusCode::NOT_IMPLEMENTED, &msg),
        Err(join) => err_response(StatusCode::INTERNAL_SERVER_ERROR, &join.to_string()),
    }
}

/// Prometheus `remote_write` (R-3, timelakedb#56): `POST /api/v1/write?db=…`.
///
/// The body is snappy-compressed protobuf, not gzip+text, so it does NOT go
/// through `maybe_gunzip`/`parse_lines`. It is decoded to rows, serialized to
/// line protocol, and handed to the SAME `write_lp` seam every other write
/// uses — so WAL durability, CL-2 replication, LWW dedup and SEC-2 are
/// inherited, not reimplemented. `__name__` becomes the measurement, every
/// other label a tag, the sample value a `value` field (never a table per
/// field — freshet#4).
async fn write_prometheus<E: Engine>(
    state: State<Arc<E>>,
    Query(params): Params,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let Some(db) = params.get("db").cloned() else {
        return err_response(StatusCode::BAD_REQUEST, "missing 'db' parameter");
    };
    if let Err(e) =
        state
            .0
            .authenticate_data(authorization_of(&headers).as_deref(), Action::Write, &db)
    {
        return deny_response(e);
    }
    let rows = match timelake_prometheus::decode_remote_write(&body) {
        Ok(r) => r,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    // A scrape with only stale/empty series decodes to nothing — that is a
    // successful no-op, not a 400 (Prometheus keeps sending them).
    if rows.is_empty() {
        return StatusCode::NO_CONTENT.into_response();
    }
    // Rows already carry ns timestamps, so precision is ns (identity).
    let lp = timelake_ingest::to_line_protocol(&rows);
    let engine = state.0.clone();
    let res =
        tokio::task::spawn_blocking(move || engine.write_lp(&db, lp.as_bytes(), Some("ns"))).await;
    write_result_response(res)
}

fn maybe_gunzip(headers: &HeaderMap, body: Bytes) -> std::io::Result<Vec<u8>> {
    let gzipped = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("gzip"));
    if gzipped {
        let mut out = Vec::with_capacity(body.len() * 4);
        flate2::read::GzDecoder::new(&body[..]).read_to_end(&mut out)?;
        Ok(out)
    } else {
        Ok(body.to_vec())
    }
}

#[derive(Deserialize)]
struct SqlRequest {
    #[serde(default)]
    db: Option<String>,
    sql: String,
    /// Visibility authorizations (SEC-2); unioned with the
    /// `X-TimeLake-Authorizations` header.
    #[serde(default)]
    authorizations: Vec<String>,
}

/// Parse a comma-separated authorizations header value. Claims, not
/// credentials, while the surface has no authn (SECURITY.md posture);
/// the seam is what SEC-2 mandates, and a token layer slots in front.
/// Refuse a data-plane request the way each stock client expects.
///
/// 401 carries `WWW-Authenticate: Bearer`, which is what makes a bare
/// `curl -u` or a browser prompt behave sensibly, and what tells a
/// client library it should attach a credential rather than give up.
fn deny_response(e: TokenError) -> axum::response::Response {
    let code = match e {
        TokenError::Missing | TokenError::Invalid => StatusCode::UNAUTHORIZED,
        TokenError::Forbidden => StatusCode::FORBIDDEN,
    };
    let body = Json(json!({ "code": e.code(), "error": e.message() }));
    match code {
        StatusCode::UNAUTHORIZED => (
            code,
            [(
                axum::http::header::WWW_AUTHENTICATE,
                "Bearer realm=\"timelake\"",
            )],
            body,
        )
            .into_response(),
        _ => (code, body).into_response(),
    }
}

fn authorization_of(headers: &HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

fn auths_from_headers(headers: &HeaderMap) -> Vec<String> {
    headers
        .get("x-timelake-authorizations")
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

async fn sql<E: Engine>(
    State(engine): State<Arc<E>>,
    headers: HeaderMap,
    // Optional on purpose: the plaintext listener attaches no identity, and
    // a TLS caller that presents no certificate attaches `None`. Both are
    // ordinary under want mode, and neither may become a refusal.
    peer: Option<Extension<PeerIdentity>>,
    Json(req): Json<SqlRequest>,
) -> axum::response::Response {
    let db = req.db.unwrap_or_else(|| "poc".to_string());
    let decision =
        match engine.authenticate_data(authorization_of(&headers).as_deref(), Action::Read, &db) {
            Ok(d) => d,
            Err(e) => return deny_response(e),
        };
    let mut auths = auths_from_headers(&headers);
    for a in req.authorizations {
        if !auths.contains(&a) {
            auths.push(a);
        }
    }
    // SEC-2: a credential can only ever NARROW what its holder sees. An
    // anonymous caller keeps today's behaviour (claims as asserted),
    // which is what lets this ship without a flag day.
    if let Some(granted) = &decision.granted {
        auths.retain(|claimed| granted.iter().any(|g| g == claimed));
    }
    // SEC-3 v2: the verified certificate identity, when there is one. The
    // engine intersects that identity's recorded grants with the claims
    // above — a second narrowing, never a widening, and the reason a
    // certificate is worth presenting on this surface at all.
    let identity = peer.and_then(|Extension(PeerIdentity(id))| id);
    match engine.sql(db, req.sql, auths, identity).await {
        Ok(rows) => Json(rows).into_response(),
        Err(msg) => err_response(StatusCode::BAD_REQUEST, &msg),
    }
}

/// SEC-6 (exposure 6): per-client concurrency cap for `/api/sql`, as a
/// middleware so the handler stays a pure query path. Keyed by the
/// data-plane token when the caller presents one (its header, hashed —
/// the raw secret never becomes a map key) and by the network origin
/// otherwise. The admission guard is held across the downstream handler
/// and releases the slot on drop; a client already at its cap is refused
/// with 429 before the query runs. A caller the transport cannot attribute
/// (no token, no connect-info — e.g. an endpoint unit test) is not capped;
/// the global admission bound (RR-1) still applies.
async fn rate_limit_sql<E: Engine>(
    State(engine): State<Arc<E>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let key = client_key_of(&request);
    match engine.admit_client(key) {
        Some(_slot) => next.run(request).await,
        None => err_response(
            StatusCode::TOO_MANY_REQUESTS,
            "too many concurrent queries for this client (SEC-6)",
        ),
    }
}

/// The SEC-6 client key: `tok:<hash>` of the Authorization header when
/// present, else `ip:<addr>` from the connection info, else None (nothing
/// to key on — not capped).
fn client_key_of(request: &axum::extract::Request) -> Option<String> {
    if let Some(auth) = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        return Some(format!("tok:{:016x}", short_hash(auth)));
    }
    request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(a)| format!("ip:{}", a.ip()))
}

/// A non-cryptographic digest so a bearer token becomes a stable map key
/// without the raw secret ever being stored. Collisions only conflate two
/// clients' limits, never widen access.
fn short_hash(s: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

// ---- data-plane tokens (SEC-4 phased) ----------------------------------

/// The list view never includes the hash: it is not secret, but it is
/// also not useful to an operator, and excluding it makes "the console
/// cannot leak a credential" true by construction.
fn token_view(r: &timelake_auth::TokenRecord) -> serde_json::Value {
    json!({
        "id": r.id,
        "description": r.description,
        "scope": r.scope.as_str(),
        "databases": r.databases,
        "authorizations": r.authorizations,
        "created_by": r.created_by,
        "created_at_secs": r.created_at_secs,
        "expires_at_secs": r.expires_at_secs,
        "revoked": r.revoked,
        "last_used_secs": r.last_used_secs,
    })
}

async fn tokens_list<E: Engine>(
    State(state): State<AdminState<E>>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    if let Some(deny) = require(&session, Role::Operator) {
        return deny;
    }
    let tokens: Vec<_> = state.auth.tokens().iter().map(token_view).collect();
    Json(json!({ "tokens": tokens })).into_response()
}

#[derive(serde::Deserialize)]
struct TokenIssueRequest {
    description: String,
    /// "read" | "write" | "read_write"
    scope: String,
    #[serde(default)]
    databases: Vec<String>,
    /// SEC-2 authorizations GRANTED to this token; a caller's claims are
    /// intersected with these. Empty = no policy = claims pass through.
    #[serde(default)]
    authorizations: Vec<String>,
    #[serde(default)]
    expires_in_secs: Option<u64>,
}

async fn tokens_issue<E: Engine>(
    State(state): State<AdminState<E>>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    let source = source_of(req.extensions());
    // Issuing a credential is granting access: admin, no exceptions.
    if let Some(deny) = require(&session, Role::Admin) {
        return deny;
    }
    let body = match axum::body::to_bytes(req.into_body(), 64 * 1024).await {
        Ok(b) => b,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let issue: TokenIssueRequest = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    if issue.description.trim().is_empty() {
        return err_response(
            StatusCode::BAD_REQUEST,
            "description must not be empty — six months from now someone              has to know what this token is before revoking it",
        );
    }
    let Some(scope) = Scope::parse(&issue.scope) else {
        return err_response(
            StatusCode::BAD_REQUEST,
            &format!("scope {:?} is not read, write, or read_write", issue.scope),
        );
    };
    let expires_at_secs = issue.expires_in_secs.map(|n| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
            + n
    });
    if let Some(resp) = audit_gate(&state.audit) {
        return resp;
    }
    match state.auth.issue_token(
        &issue.description,
        scope,
        issue.databases,
        issue.authorizations,
        expires_at_secs,
        &session.username,
    ) {
        Ok((secret, record)) => {
            // The audit `after` is the safe token view — never the secret,
            // which exists in memory only long enough to be shown once.
            let view = token_view(&record);
            let target = view.get("id").and_then(|v| v.as_str()).map(String::from);
            let ev = audit_event(
                &session,
                source,
                "token.issue",
                target,
                None,
                Some(view.clone()),
                "ok",
            );
            if let Some(resp) = audit_record(&state.audit, ev) {
                return resp;
            }
            Json(json!({
                // Shown exactly once. Only the digest is stored, so there is
                // no "show me again" — losing it means issuing a new one.
                "secret": secret,
                "token": view,
            }))
            .into_response()
        }
        Err(e) => {
            let ev = audit_event(&session, source, "token.issue", None, None, None, "error");
            let _ = state.audit.record(ev);
            err_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
        }
    }
}

async fn tokens_revoke<E: Engine>(
    State(state): State<AdminState<E>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    let source = source_of(req.extensions());
    if let Some(deny) = require(&session, Role::Admin) {
        return deny;
    }
    if let Some(resp) = audit_gate(&state.audit) {
        return resp;
    }
    match state.auth.revoke_token(&id) {
        Ok(true) => {
            let ev = audit_event(
                &session,
                source,
                "token.revoke",
                Some(id.clone()),
                None,
                None,
                "ok",
            );
            if let Some(resp) = audit_record(&state.audit, ev) {
                return resp;
            }
            Json(json!({ "id": id, "status": "revoked" })).into_response()
        }
        // A no-op (already gone): a 404, no state change, nothing to record.
        Ok(false) => err_response(StatusCode::NOT_FOUND, "no such token, or already revoked"),
        Err(e) => {
            let ev = audit_event(
                &session,
                source,
                "token.revoke",
                Some(id.clone()),
                None,
                None,
                "error",
            );
            let _ = state.audit.record(ev);
            err_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
        }
    }
}

// ---- SEC-2 certificate grants (exposures 7/9) --------------------------
//
// A verified client-certificate identity (its CN) is held to a set of
// authorizations: the query path intersects the caller's self-asserted
// claims with these, so a certificate can only ever NARROW what its
// holder sees. No grants recorded = the want-mode passthrough (claims
// unchanged), which is why an unmapped certificate grants nothing.

async fn cert_grants_list<E: Engine>(
    State(state): State<AdminState<E>>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    if let Some(deny) = require(&session, Role::Operator) {
        return deny;
    }
    let grants: Vec<_> = state
        .auth
        .cert_grant_identities()
        .into_iter()
        .map(|(identity, authorizations)| {
            json!({ "identity": identity, "authorizations": authorizations })
        })
        .collect();
    Json(json!({ "cert_grants": grants })).into_response()
}

#[derive(serde::Deserialize)]
struct CertGrantsRequest {
    /// The authorizations this certificate identity is granted; a caller's
    /// claims are intersected with these. Empty means the identity sees
    /// only rows visible to no authorization (public), which is the
    /// tightest a grant can be.
    #[serde(default)]
    authorizations: Vec<String>,
}

async fn cert_grants_set<E: Engine>(
    State(state): State<AdminState<E>>,
    axum::extract::Path(cn): axum::extract::Path<String>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    let source = source_of(req.extensions());
    // Granting is deciding what an identity may see: admin, no exceptions.
    if let Some(deny) = require(&session, Role::Admin) {
        return deny;
    }
    let body = match axum::body::to_bytes(req.into_body(), 64 * 1024).await {
        Ok(b) => b,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let parsed: CertGrantsRequest = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    if cn.trim().is_empty() {
        return err_response(StatusCode::BAD_REQUEST, "certificate CN must not be empty");
    }
    if let Some(resp) = audit_gate(&state.audit) {
        return resp;
    }
    let before = state
        .auth
        .cert_grant_identities()
        .into_iter()
        .find(|(id, _)| *id == cn)
        .map(|(_, a)| json!({"authorizations": a}));
    match state
        .auth
        .set_cert_grants(&cn, parsed.authorizations.clone())
    {
        Ok(()) => {
            let ev = audit_event(
                &session,
                source,
                "cert_grants.set",
                Some(cn.clone()),
                before,
                Some(json!({"authorizations": parsed.authorizations.clone()})),
                "ok",
            );
            if let Some(resp) = audit_record(&state.audit, ev) {
                return resp;
            }
            Json(json!({
                "identity": cn,
                "authorizations": parsed.authorizations,
                "status": "set",
            }))
            .into_response()
        }
        Err(e) => {
            let ev = audit_event(
                &session,
                source,
                "cert_grants.set",
                Some(cn.clone()),
                before,
                None,
                "error",
            );
            let _ = state.audit.record(ev);
            err_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
        }
    }
}

async fn cert_grants_remove<E: Engine>(
    State(state): State<AdminState<E>>,
    axum::extract::Path(cn): axum::extract::Path<String>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    let source = source_of(req.extensions());
    if let Some(deny) = require(&session, Role::Admin) {
        return deny;
    }
    if let Some(resp) = audit_gate(&state.audit) {
        return resp;
    }
    let before = state
        .auth
        .cert_grant_identities()
        .into_iter()
        .find(|(id, _)| *id == cn)
        .map(|(_, a)| json!({"authorizations": a}));
    match state.auth.remove_cert_grants(&cn) {
        Ok(true) => {
            let ev = audit_event(
                &session,
                source,
                "cert_grants.remove",
                Some(cn.clone()),
                before,
                None,
                "ok",
            );
            if let Some(resp) = audit_record(&state.audit, ev) {
                return resp;
            }
            Json(json!({ "identity": cn, "status": "removed" })).into_response()
        }
        Ok(false) => err_response(
            StatusCode::NOT_FOUND,
            "no grants recorded for that identity",
        ),
        Err(e) => {
            let ev = audit_event(
                &session,
                source,
                "cert_grants.remove",
                Some(cn.clone()),
                before,
                None,
                "error",
            );
            let _ = state.audit.record(ev);
            err_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
        }
    }
}

/// Filters for `GET /admin/audit`. All optional; an empty query returns the
/// most recent page. `since` is an RFC 3339 string compared lexically, which
/// is a chronological compare for this format.
#[derive(Deserialize, Default)]
struct AuditQuery {
    action: Option<String>,
    principal: Option<String>,
    target: Option<String>,
    since: Option<String>,
    limit: Option<usize>,
    /// `?verify=1` appends a whole-chain verification result.
    verify: Option<u8>,
}

/// GET /admin/audit (viewer): the audit trail, filtered, most-recent last,
/// with an optional `?verify=1` chain check. Reading the log is itself
/// audited (§5.1) — best-effort, since a read is never blocked by the sink,
/// only mutations are.
async fn audit_list<E: Engine>(
    State(state): State<AdminState<E>>,
    Query(q): Query<AuditQuery>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    let source = source_of(req.extensions());
    if let Some(deny) = require(&session, Role::Viewer) {
        return deny;
    }

    let records = match state.audit.read_all() {
        Ok(r) => r,
        Err(e) => {
            return err_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("read audit log: {e}"),
            );
        }
    };

    let matches = |r: &AuditRecord| {
        q.action.as_ref().is_none_or(|a| &r.action == a)
            && q.principal.as_ref().is_none_or(|p| &r.principal == p)
            && q.target
                .as_ref()
                .is_none_or(|t| r.target.as_deref() == Some(t.as_str()))
            && q.since.as_ref().is_none_or(|s| r.ts.as_str() >= s.as_str())
    };
    let filtered: Vec<&AuditRecord> = records.iter().filter(|r| matches(r)).collect();

    // Most-recent last; a limit keeps the tail (the newest N), so a large log
    // does not have to be paged from the front to see what just happened.
    let limit = q.limit.unwrap_or(1000).min(10_000);
    let start = filtered.len().saturating_sub(limit);
    let page: Vec<&AuditRecord> = filtered[start..].to_vec();

    // `?verify=1`: verify the WHOLE chain, not the filtered page — a subset is
    // not itself a valid chain.
    let verify = (q.verify.unwrap_or(0) != 0).then(|| match verify_records(&records) {
        Ok(()) => json!({"ok": true}),
        Err(b) => json!({"ok": false, "break": {"seq": b.seq, "reason": b.reason}}),
    });

    // §5.1: reading the audit log is itself audited. Best-effort — the read is
    // served regardless, so no gate and no 503.
    let _ = state.audit.record(audit_event(
        &session,
        source,
        "audit.read",
        None,
        None,
        Some(json!({"returned": page.len(), "total": records.len()})),
        "ok",
    ));

    let mut body = json!({
        "records": page,
        "total": records.len(),
        "returned": page.len(),
        "role": session.role.as_str(),
    });
    if let Some(v) = verify {
        body["verify"] = v;
    }
    Json(body).into_response()
}

use axum::response::IntoResponse;

fn err_response(code: StatusCode, msg: &str) -> axum::response::Response {
    (code, Json(json!({ "error": msg }))).into_response()
}

// -- P1-2 audit emission (SR-6) --------------------------------------------

use timelake_audit::{AuditLog, AuditRecord, NewRecord, verify_records};

/// The request's source address, if the listener attached one. Stamped on
/// the audit record's `source`.
fn source_of(ext: &axum::http::Extensions) -> Option<String> {
    ext.get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(a)| a.ip().to_string())
}

/// A `503 audit sink unavailable`, the fail-closed refusal (§5.5).
fn audit_unavailable() -> axum::response::Response {
    err_response(StatusCode::SERVICE_UNAVAILABLE, "audit sink unavailable")
}

/// Fail-closed admission, called BEFORE a mutation runs. `Some(503)` means
/// the sink is known-broken and the caller must not mutate; `None` means
/// proceed.
fn audit_gate(audit: &AuditLog) -> Option<axum::response::Response> {
    audit.gate().err().map(|_| audit_unavailable())
}

/// Record the outcome of a mutation. `Some(503)` means the record could not
/// be written and fail-open is off, so the caller returns it instead of a
/// success response (the mutation happened, but leaving no record is refused
/// — the next `audit_gate` then keeps the door shut until the sink recovers).
/// `None` means the record is durable (or fail-open swallowed the failure)
/// and the caller may return success.
fn audit_record(audit: &AuditLog, nr: NewRecord) -> Option<axum::response::Response> {
    match audit.record(nr) {
        Ok(_) => None,
        Err(e) if audit.fail_open() => {
            tracing::error!(error = %e, "audit append failed but fail-open is set; proceeding");
            None
        }
        Err(e) => {
            tracing::error!(error = %e, "audit append failed; refusing (fail-closed)");
            Some(audit_unavailable())
        }
    }
}

/// Build a `NewRecord` for a mutation, filling the common fields. Session id
/// is not yet threaded from `SessionInfo` (it carries none), so it stays
/// `None`; request-id likewise until correlation middleware exists.
#[allow(clippy::too_many_arguments)]
fn audit_event(
    session: &SessionInfo,
    source: Option<String>,
    action: &str,
    target: Option<String>,
    before: Option<Value>,
    after: Option<Value>,
    outcome: &str,
) -> NewRecord {
    NewRecord {
        principal: session.username.clone(),
        role: session.role.as_str().to_string(),
        session: None,
        source,
        request_id: None,
        action: action.to_string(),
        target,
        before,
        after,
        outcome: outcome.to_string(),
    }
}
