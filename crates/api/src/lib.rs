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

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use timelake_auth::{Action, Auth, Decision, Role, Scope, SessionInfo, TokenError};

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
    fn sql(
        &self,
        db: String,
        query: String,
        authorizations: Vec<String>,
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

    /// Prometheus exposition text (SR-4).
    fn metrics_text(&self) -> String;

    /// Current per-table retention policies (FR-7), table → seconds.
    fn retention_policies(&self) -> Vec<(String, u64)>;

    /// Set (upsert) one table's retention window, durably.
    fn set_retention(&self, table: &str, seconds: u64) -> Result<(), String>;

    /// Remove one table's policy (the table keeps everything again).
    fn remove_retention(&self, table: &str) -> Result<(), String>;
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

/// State for the authenticated admin surface (SEC-4).
pub struct AdminState<E: Engine> {
    engine: Arc<E>,
    auth: Arc<Auth>,
    /// Mark session cookies `Secure` — set when the listener is TLS.
    secure_cookies: bool,
}

impl<E: Engine> Clone for AdminState<E> {
    fn clone(&self) -> Self {
        AdminState {
            engine: self.engine.clone(),
            auth: self.auth.clone(),
            secure_cookies: self.secure_cookies,
        }
    }
}

/// The data plane (unauthenticated — FR-1/FR-8/FR-9 clients) plus the
/// admin surface (authenticated — SEC-4). Two routers, two states,
/// merged: no admin route can accidentally inherit the open state.
pub fn app<E: Engine>(engine: Arc<E>, auth: Arc<Auth>, secure_cookies: bool) -> Router {
    let data = Router::new()
        .route("/health", get(health))
        .route("/ping", get(ping::<E>).head(ping::<E>))
        .route("/metrics", get(metrics::<E>))
        .route("/write", post(write_v1::<E>))
        .route("/api/v2/write", post(write_v2::<E>))
        .route("/api/v3/write_lp", post(write_v3::<E>))
        .route("/api/sql", post(sql::<E>))
        .with_state(engine.clone());

    let state = AdminState {
        engine,
        auth,
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
            "/admin/retention/{table}",
            axum::routing::delete(retention_delete::<E>),
        )
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

    data.merge(guarded).merge(public)
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
    let body = match axum::body::to_bytes(req.into_body(), 64 * 1024).await {
        Ok(b) => b,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let change: PasswordChange = match serde_json::from_slice(&body) {
        Ok(c) => c,
        Err(e) => return err_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    match state.auth.change_password(
        &session.username,
        &change.current_password,
        &change.new_password,
    ) {
        // Every session for this principal — including this one — is
        // invalidated by the rotation, so the client must log in again.
        Ok(()) => Json(json!({
            "status": "password changed",
            "reauthenticate": true,
        }))
        .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e, "code": "password_rejected" })),
        )
            .into_response(),
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
        .map(|(table, seconds)| {
            json!({
                "table": table,
                "seconds": seconds,
                "duration": humanize_secs(seconds),
            })
        })
        .collect();
    policies.sort_by_key(|p| p["table"].as_str().unwrap_or("").to_string());
    Json(json!({
        "policies": policies,
        "role": session.role.as_str(),
    }))
    .into_response()
}

#[derive(Deserialize)]
struct RetentionSet {
    table: String,
    /// "365d", "72h", "90m", or seconds.
    duration: String,
}

async fn retention_set<E: Engine>(
    State(state): State<AdminState<E>>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
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
        .find(|(t, _)| t == table)
        .map(|(_, s)| s);
    let destructive = match current {
        Some(existing) => seconds < existing,
        None => true, // unbounded -> bounded: data starts expiring
    };
    let needed = if destructive {
        Role::Admin
    } else {
        Role::Operator
    };
    if let Some(deny) = require(&session, needed) {
        return deny;
    }

    match state.engine.set_retention(table, seconds) {
        Ok(()) => Json(json!({
            "table": table,
            "seconds": seconds,
            "duration": humanize_secs(seconds),
            "status": "set",
            "destructive": destructive,
        }))
        .into_response(),
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, &e),
    }
}

async fn retention_delete<E: Engine>(
    State(state): State<AdminState<E>>,
    axum::extract::Path(table): axum::extract::Path<String>,
    req: axum::extract::Request,
) -> axum::response::Response {
    // Removing a policy makes the table grow without bound — governing
    // the node's storage, so it sits with `admin` alongside shrinking.
    let session = session_of(req.extensions());
    if let Some(deny) = require(&session, Role::Admin) {
        return deny;
    }
    match state.engine.remove_retention(&table) {
        Ok(()) => Json(json!({ "table": table, "status": "removed" })).into_response(),
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, &e),
    }
}

async fn health() -> Json<Value> {
    Json(json!({
        "status": "pass",
        "name": "timelakedb",
        "version": env!("CARGO_PKG_VERSION"),
        "milestone": "M3",
    }))
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
        Err(join) => err_response(StatusCode::INTERNAL_SERVER_ERROR, &join.to_string()),
    }
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
    match engine.sql(db, req.sql, auths).await {
        Ok(rows) => Json(rows).into_response(),
        Err(msg) => err_response(StatusCode::BAD_REQUEST, &msg),
    }
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
    match state.auth.issue_token(
        &issue.description,
        scope,
        issue.databases,
        issue.authorizations,
        expires_at_secs,
        &session.username,
    ) {
        Ok((secret, record)) => Json(json!({
            // Shown exactly once. Only the digest is stored, so there is
            // no "show me again" — losing it means issuing a new one.
            "secret": secret,
            "token": token_view(&record),
        }))
        .into_response(),
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

async fn tokens_revoke<E: Engine>(
    State(state): State<AdminState<E>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let session = session_of(req.extensions());
    if let Some(deny) = require(&session, Role::Admin) {
        return deny;
    }
    match state.auth.revoke_token(&id) {
        Ok(true) => Json(json!({ "id": id, "status": "revoked" })).into_response(),
        Ok(false) => err_response(StatusCode::NOT_FOUND, "no such token, or already revoked"),
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

use axum::response::IntoResponse;

fn err_response(code: StatusCode, msg: &str) -> axum::response::Response {
    (code, Json(json!({ "error": msg }))).into_response()
}
