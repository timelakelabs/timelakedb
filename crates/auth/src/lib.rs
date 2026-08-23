//! SEC-4: authentication and roles for the administrative surface.
//!
//! Scope, deliberately: this protects `/admin/*` — the config, retention
//! and TLS controls. The DATA plane (`/write`, `/api/sql`, Flight SQL)
//! stays open, because requiring credentials there breaks Telegraf,
//! Grafana and the harness in one step and belongs to its own migration
//! (REQUIREMENTS SEC-4 "phased"). What this closes today is SECURITY.md
//! exposure 3a: the unauthenticated deletion control.
//!
//! **Bootstrap (decided 2026-08-09, replacing the bootstrap-token
//! sketch):** the first start seeds `admin`/`admin`, flagged
//! `must_change_password`. That credential can do exactly ONE thing —
//! change its own password — and nothing else in the admin surface
//! answers until it does. The known-default window is real, so it is
//! made loud rather than quiet: a WARN at every start, a banner in the
//! console, and `timelake_admin_default_credential_active 1` in
//! `/metrics` so it can be alerted on. Operators who want no default at
//! all set `TIMELAKE_ADMIN_BOOTSTRAP_PASSWORD` and the seed uses that
//! instead, still flagged for rotation.
//!
//! Credentials are Argon2id PHC strings, never reversible. Principals
//! persist through the `Store`, so they are envelope-encrypted with
//! everything else (SEC-1) and shared via S3 in the cluster era.

use std::collections::{BTreeMap, HashMap};
use std::io::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// rand_core comes from password_hash so the RNG versions cannot drift
// apart from argon2's own (0.5 pins rand_core 0.6).
use argon2::Argon2;
use argon2::password_hash::rand_core::{OsRng, RngCore};
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use serde::{Deserialize, Serialize};
use timelake_store::Store;

pub mod guard;
pub mod token;
pub use guard::{Action, Decision, decide};
pub use token::{
    DataAuthCounts, DataAuthMode, Scope, TokenError, TokenIdentity, TokenIndex, TokenRecord,
    generate_secret, hash_token, token_from_authorization,
};

/// Where the principal store lives in the object store.
pub const PRINCIPALS_PATH: &str = "catalog/config/principals.json";
/// Data-plane tokens live beside the principals, so they inherit SEC-1
/// envelope encryption and the C0 S3 sharing story for free.
pub const TOKENS_PATH: &str = "catalog/config/tokens.json";
/// SEC-2 grants for verified client-certificate identities: a CN → the
/// authorizations that identity is held to. A caller presenting a cert
/// has its self-asserted claims intersected with these (exposures 7/9).
/// Beside the tokens, so it inherits the same envelope encryption and
/// object-store sharing.
pub const CERT_GRANTS_PATH: &str = "catalog/config/cert-grants.json";
/// The seeded username and its seeded password.
pub const DEFAULT_USER: &str = "admin";
pub const DEFAULT_PASSWORD: &str = "admin";
/// Minimum length for a replacement password. Short enough not to be
/// theatre, long enough that the default cannot be swapped for "admin1".
pub const MIN_PASSWORD_LEN: usize = 8;

const SESSION_IDLE: Duration = Duration::from_secs(30 * 60);
const SESSION_ABSOLUTE: Duration = Duration::from_secs(12 * 60 * 60);
/// Failed logins per principal before backoff starts biting.
const FREE_ATTEMPTS: u32 = 3;
const MAX_BACKOFF: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Read everything, change nothing.
    Viewer,
    /// Non-destructive tunables, GROWING a retention window, triggering
    /// maintenance.
    Operator,
    /// Destructive and resource-governing settings (shrinking or removing
    /// retention, memory limits) and principal management.
    Admin,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Viewer => "viewer",
            Role::Operator => "operator",
            Role::Admin => "admin",
        }
    }

    pub fn parse(s: &str) -> Option<Role> {
        match s {
            "viewer" => Some(Role::Viewer),
            "operator" => Some(Role::Operator),
            "admin" => Some(Role::Admin),
            _ => None,
        }
    }

    /// Does this role satisfy a requirement of `needed`? Roles are a
    /// total order, so this is a comparison rather than a matrix.
    pub fn allows(self, needed: Role) -> bool {
        self >= needed
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Principal {
    pub username: String,
    pub role: Role,
    /// Argon2id PHC string.
    password_hash: String,
    /// While true the principal may ONLY change its own password.
    pub must_change_password: bool,
    pub created_at_secs: u64,
    #[serde(default)]
    pub last_login_secs: Option<u64>,
    /// True while the seeded credential has never been rotated — what
    /// the warning banner and the metric key off.
    #[serde(default)]
    pub is_default_credential: bool,
}

/// What a caller is, once authenticated.
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub username: String,
    pub role: Role,
    pub must_change_password: bool,
    /// Double-submit CSRF token for cookie-authenticated mutations.
    pub csrf: String,
}

struct Session {
    username: String,
    role: Role,
    must_change_password: bool,
    csrf: String,
    created: Instant,
    last_seen: Instant,
}

#[derive(Debug)]
pub enum LoginError {
    /// Wrong user or password — deliberately indistinguishable.
    Invalid,
    /// Too many failures; retry after the given delay.
    RateLimited(Duration),
}

impl std::fmt::Display for LoginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoginError::Invalid => write!(f, "invalid username or password"),
            LoginError::RateLimited(d) => write!(
                f,
                "too many failed attempts; retry in {} seconds",
                d.as_secs().max(1)
            ),
        }
    }
}

struct Failures {
    count: u32,
    last: Instant,
}

pub struct Auth {
    store: Arc<dyn Store>,
    principals: RwLock<BTreeMap<String, Principal>>,
    tokens: RwLock<TokenIndex>,
    /// Hash of the `tokens.json` bytes this node last loaded or wrote, so a
    /// reload can tell "unchanged" from "changed" with one `get` and no
    /// parse. `None` until the first load.
    tokens_loaded_hash: Mutex<Option<u64>>,
    /// When the last on-miss reload was attempted (see [`Auth::verify_token`]).
    /// Bounded to one attempt per second so a flood of bad tokens cannot
    /// turn into a flood of store reads.
    token_miss_reload_at: Mutex<Option<Instant>>,
    /// Reloads that actually changed the in-memory token set, for /metrics —
    /// the number a drill watches to see propagation happen.
    pub token_reloads_total: AtomicU64,
    /// CN -> the authorizations a verified client certificate is granted.
    cert_grants: RwLock<BTreeMap<String, Vec<String>>>,
    sessions: RwLock<HashMap<String, Session>>,
    failures: Mutex<HashMap<String, Failures>>,
    /// Successful and failed logins, for /metrics.
    pub logins_total: AtomicU64,
    pub login_failures_total: AtomicU64,
}

/// Content hash of a token file, for change detection only — not a
/// security property, so the std hasher is the right tool.
fn bytes_hash(bytes: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    h.finish()
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn hash_password(password: &str) -> Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| std::io::Error::other(format!("password hash: {e}")))
}

fn random_token() -> String {
    let mut raw = [0u8; 32];
    OsRng.fill_bytes(&mut raw);
    raw.iter().map(|b| format!("{b:02x}")).collect()
}

impl Auth {
    /// Load principals from the store, seeding the first administrator
    /// when there are none. `bootstrap_password` (the server passes
    /// `TIMELAKE_ADMIN_BOOTSTRAP_PASSWORD`) replaces the well-known
    /// default for automated provisioning; either way the seeded
    /// credential is flagged for rotation. Env lookup stays at the edge
    /// so this is deterministic under test.
    pub fn open(store: Arc<dyn Store>, bootstrap_password: Option<&str>) -> Result<Arc<Auth>> {
        let principals: BTreeMap<String, Principal> = match store.get(PRINCIPALS_PATH) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{PRINCIPALS_PATH}: {e}"),
                )
            })?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(e) => return Err(e),
        };

        let (tokens, tokens_hash): (Vec<TokenRecord>, Option<u64>) = match store.get(TOKENS_PATH) {
            Ok(bytes) => (
                serde_json::from_slice(&bytes).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("{TOKENS_PATH}: {e}"),
                    )
                })?,
                Some(bytes_hash(&bytes)),
            ),
            // An absent file is the empty token set, and must hash as such:
            // `reload_tokens` reads "not found" as empty bytes, and if this
            // stayed `None` the first reload on a fresh store would count
            // nothing-to-nothing as a change.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                (Vec::new(), Some(bytes_hash(&[])))
            }
            Err(e) => return Err(e),
        };

        let cert_grants: BTreeMap<String, Vec<String>> = match store.get(CERT_GRANTS_PATH) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{CERT_GRANTS_PATH}: {e}"),
                )
            })?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(e) => return Err(e),
        };

        let auth = Auth {
            store,
            principals: RwLock::new(principals),
            tokens: RwLock::new(TokenIndex::from_records(tokens)),
            tokens_loaded_hash: Mutex::new(tokens_hash),
            token_miss_reload_at: Mutex::new(None),
            token_reloads_total: AtomicU64::new(0),
            cert_grants: RwLock::new(cert_grants),
            sessions: RwLock::new(HashMap::new()),
            failures: Mutex::new(HashMap::new()),
            logins_total: AtomicU64::new(0),
            login_failures_total: AtomicU64::new(0),
        };

        if auth.principals.read().expect("principals lock").is_empty() {
            // A provisioned password keeps the "no well-known default
            // anywhere" posture available; either way it must be rotated.
            let (password, from_env) = match bootstrap_password {
                Some(p) if !p.trim().is_empty() => (p.to_string(), true),
                _ => (DEFAULT_PASSWORD.to_string(), false),
            };
            let principal = Principal {
                username: DEFAULT_USER.to_string(),
                role: Role::Admin,
                password_hash: hash_password(&password)?,
                must_change_password: true,
                created_at_secs: now_secs(),
                last_login_secs: None,
                is_default_credential: !from_env,
            };
            auth.principals
                .write()
                .expect("principals lock")
                .insert(DEFAULT_USER.to_string(), principal);
            auth.persist()?;
            if from_env {
                tracing::info!(
                    user = DEFAULT_USER,
                    "SEC-4: seeded the first administrator from \
                     TIMELAKE_ADMIN_BOOTSTRAP_PASSWORD; it must be changed at first login"
                );
            } else {
                tracing::warn!(
                    "SEC-4: no principals existed, so the admin console was seeded with \
                     the DEFAULT CREDENTIAL admin/admin. It can do nothing but change its \
                     own password, and every other admin route refuses until it does — \
                     but until then anyone who can reach this port can take the console. \
                     Log in and change it now, or set TIMELAKE_ADMIN_BOOTSTRAP_PASSWORD. \
                     Alert on timelake_admin_default_credential_active."
                );
            }
        }
        Ok(Arc::new(auth))
    }

    fn persist_tokens(&self) -> Result<()> {
        let t = self.tokens.read().expect("tokens lock");
        let bytes = serde_json::to_vec_pretty(&t.records()).expect("tokens json");
        self.store.put(TOKENS_PATH, &bytes)?;
        // What we wrote is what we hold: the next reload sees the same
        // bytes and does nothing, rather than re-parsing our own write.
        *self.tokens_loaded_hash.lock().expect("hash lock") = Some(bytes_hash(&bytes));
        Ok(())
    }

    /// Re-read the token file from the store and swap it in if it changed.
    /// Returns `Ok(true)` when the in-memory set was replaced.
    ///
    /// Why this exists: every node in a cluster shares one bucket and
    /// therefore one `tokens.json`, but each node read it exactly once, at
    /// `open`. A token issued on one ingester's console was unknown to its
    /// peer until that peer restarted — and a token *revoked* on one node
    /// kept working on every other, which is not revocation. The
    /// maintenance tick (and the querier's tail loop, which has no
    /// maintenance) calls this every ~10 s; [`Auth::verify_token`] also
    /// calls it once, rate-limited, when a token is unknown, so a fresh
    /// token works on first use without waiting a tick.
    ///
    /// Cost: one `get` of a small file per tick per node; the bytes are
    /// hashed before they are parsed, so an unchanged file is a
    /// comparison and nothing else. A store error leaves the current set
    /// in place and is returned, never swallowed into "no tokens".
    pub fn reload_tokens(&self) -> Result<bool> {
        let bytes = match self.store.get(TOKENS_PATH) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e),
        };
        let hash = bytes_hash(&bytes);
        if *self.tokens_loaded_hash.lock().expect("hash lock") == Some(hash) {
            return Ok(false);
        }
        let records: Vec<TokenRecord> = if bytes.is_empty() {
            Vec::new()
        } else {
            serde_json::from_slice(&bytes).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{TOKENS_PATH}: {e}"),
                )
            })?
        };
        let n = records.len();
        *self.tokens.write().expect("tokens lock") = TokenIndex::from_records(records);
        *self.tokens_loaded_hash.lock().expect("hash lock") = Some(hash);
        self.token_reloads_total.fetch_add(1, Ordering::Relaxed);
        tracing::info!(tokens = n, "SEC-4: token set reloaded from the store");
        Ok(true)
    }

    /// One on-miss reload per second, node-wide. Enough that a token issued
    /// elsewhere a moment ago works on its first presentation; bounded so
    /// a client spraying bad tokens costs the store one read a second, not
    /// one per request.
    fn on_miss_reload_due(&self) -> bool {
        let mut at = self.token_miss_reload_at.lock().expect("miss lock");
        let due = at.is_none_or(|t| t.elapsed() >= Duration::from_secs(1));
        if due {
            *at = Some(Instant::now());
        }
        due
    }

    /// The authorizations a verified client-certificate identity is
    /// granted, or `None` if none are recorded for it. `None` means the
    /// caller's claims are kept as asserted (the documented want-mode
    /// passthrough); `Some` intersects them (exposures 7/9).
    pub fn cert_grants(&self, identity: &str) -> Option<Vec<String>> {
        self.cert_grants
            .read()
            .expect("cert grants lock")
            .get(identity)
            .cloned()
    }

    /// Grant a certificate identity a set of authorizations, replacing any
    /// prior set. This can only ever NARROW what that identity sees — the
    /// query path intersects claims with it and never unions.
    pub fn set_cert_grants(&self, identity: &str, authorizations: Vec<String>) -> Result<()> {
        self.cert_grants
            .write()
            .expect("cert grants lock")
            .insert(identity.to_string(), authorizations);
        self.persist_cert_grants()?;
        tracing::info!(identity, "SEC-2: certificate grants set");
        Ok(())
    }

    /// Remove a certificate identity's grants. Returns whether it existed.
    /// After this the identity is back to the want-mode passthrough.
    pub fn remove_cert_grants(&self, identity: &str) -> Result<bool> {
        let removed = self
            .cert_grants
            .write()
            .expect("cert grants lock")
            .remove(identity)
            .is_some();
        if removed {
            self.persist_cert_grants()?;
            tracing::info!(identity, "SEC-2: certificate grants removed");
        }
        Ok(removed)
    }

    /// Every recorded (identity, authorizations), for the admin list view.
    pub fn cert_grant_identities(&self) -> Vec<(String, Vec<String>)> {
        self.cert_grants
            .read()
            .expect("cert grants lock")
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn persist_cert_grants(&self) -> Result<()> {
        let g = self.cert_grants.read().expect("cert grants lock");
        let bytes = serde_json::to_vec_pretty(&*g).expect("cert grants json");
        self.store.put(CERT_GRANTS_PATH, &bytes)
    }

    /// Issue a data-plane token. The secret is returned exactly once and
    /// only its digest is kept — an operator who loses it issues another,
    /// which is what makes revocation mean something.
    pub fn issue_token(
        &self,
        description: &str,
        scope: Scope,
        databases: Vec<String>,
        authorizations: Vec<String>,
        expires_at_secs: Option<u64>,
        created_by: &str,
    ) -> Result<(String, TokenRecord)> {
        let secret = generate_secret();
        let now = now_secs();
        let record = TokenRecord {
            id: format!("tok-{now}-{}", &hash_token(&secret)[..8]),
            description: description.trim().to_string(),
            hash: hash_token(&secret),
            scope,
            databases,
            authorizations,
            created_by: created_by.to_string(),
            created_at_secs: now,
            expires_at_secs,
            revoked: false,
            last_used_secs: None,
        };
        self.tokens
            .write()
            .expect("tokens lock")
            .insert(record.clone());
        self.persist_tokens()?;
        tracing::info!(
            id = %record.id, description = %record.description, scope = record.scope.as_str(),
            by = created_by, "SEC-4: data-plane token issued"
        );
        Ok((secret, record))
    }

    pub fn tokens(&self) -> Vec<TokenRecord> {
        self.tokens.read().expect("tokens lock").records()
    }

    /// Revoking is a tombstone rather than a delete: the record stays so
    /// an operator can still see that the credential existed and when it
    /// was withdrawn.
    pub fn revoke_token(&self, id: &str) -> Result<bool> {
        {
            let mut t = self.tokens.write().expect("tokens lock");
            match t.by_id_mut(id) {
                Some(r) if !r.revoked => r.revoked = true,
                Some(_) => return Ok(false),
                None => return Ok(false),
            }
        }
        self.persist_tokens()?;
        tracing::info!(id = %id, "SEC-4: data-plane token revoked");
        Ok(true)
    }

    /// Verify a presented secret. Hot path: a digest and a map lookup,
    /// no KDF (see token.rs for why that is the right call rather than a
    /// shortcut).
    pub fn verify_token(&self, secret: &str) -> std::result::Result<TokenIdentity, TokenError> {
        let first = self
            .tokens
            .read()
            .expect("tokens lock")
            .verify(secret, now_secs());
        if first.is_ok() || !self.on_miss_reload_due() {
            return first;
        }
        // Unknown here may mean issued elsewhere a moment ago. One bounded
        // re-read, then the same answer the lookup gives; a store error is
        // logged and the token stays refused — a bucket blip must never
        // turn `required` into `open`.
        match self.reload_tokens() {
            Ok(true) => self
                .tokens
                .read()
                .expect("tokens lock")
                .verify(secret, now_secs()),
            Ok(false) => first,
            Err(e) => {
                tracing::warn!(error = %e, "SEC-4: on-miss token reload failed; refusing");
                first
            }
        }
    }

    /// The one entry point for data-plane authentication. HTTP and
    /// Flight SQL both route here, so the policy cannot fork: header →
    /// token extraction (three spellings) → verification → [`decide`].
    ///
    /// In `Off` the header is not examined at all — see the rules on
    /// [`guard::decide`] for why that is a compatibility promise rather
    /// than a shortcut.
    pub fn decide_data(
        &self,
        mode: DataAuthMode,
        authorization: Option<&str>,
        action: Action,
        db: &str,
    ) -> std::result::Result<Decision, TokenError> {
        let presented = if mode == DataAuthMode::Off {
            None
        } else {
            authorization.map(|h| match token_from_authorization(h) {
                Some(secret) => self.verify_token(&secret),
                // An Authorization header in a scheme we don't speak
                // (Digest, Negotiate…) is a presented-but-unusable
                // credential, not an anonymous request.
                None => Err(TokenError::Invalid),
            })
        };
        guard::decide(mode, presented, action, db)
    }

    fn persist(&self) -> Result<()> {
        let p = self.principals.read().expect("principals lock");
        let bytes = serde_json::to_vec_pretty(&*p).expect("principals json");
        self.store.put(PRINCIPALS_PATH, &bytes)
    }

    /// True while any principal still holds the seeded password — drives
    /// the console banner and `timelake_admin_default_credential_active`.
    pub fn default_credential_active(&self) -> bool {
        self.principals
            .read()
            .expect("principals lock")
            .values()
            .any(|p| p.is_default_credential)
    }

    pub fn principals(&self) -> Vec<Principal> {
        self.principals
            .read()
            .expect("principals lock")
            .values()
            .cloned()
            .collect()
    }

    /// How long this principal must wait before another attempt.
    fn backoff(&self, username: &str) -> Option<Duration> {
        let f = self.failures.lock().expect("failures lock");
        let entry = f.get(username)?;
        if entry.count <= FREE_ATTEMPTS {
            return None;
        }
        // 1s, 2s, 4s … capped. Slows a guessing run to a crawl without
        // ever locking an operator out permanently.
        let secs = 1u64 << (entry.count - FREE_ATTEMPTS - 1).min(8);
        let wait = Duration::from_secs(secs).min(MAX_BACKOFF);
        let since = entry.last.elapsed();
        (since < wait).then(|| wait - since)
    }

    /// Verify a credential and open a session. Returns the session token.
    pub fn login(
        &self,
        username: &str,
        password: &str,
    ) -> std::result::Result<(String, SessionInfo), LoginError> {
        if let Some(wait) = self.backoff(username) {
            self.login_failures_total.fetch_add(1, Ordering::Relaxed);
            return Err(LoginError::RateLimited(wait));
        }

        let principal = self
            .principals
            .read()
            .expect("principals lock")
            .get(username)
            .cloned();

        // Verify even when the user does not exist, against a throwaway
        // hash, so a missing user and a wrong password cost the same
        // time and cannot be told apart by a stopwatch.
        let ok = match &principal {
            Some(p) => PasswordHash::new(&p.password_hash)
                .map(|parsed| {
                    Argon2::default()
                        .verify_password(password.as_bytes(), &parsed)
                        .is_ok()
                })
                .unwrap_or(false),
            None => {
                let _ = hash_password(password);
                false
            }
        };

        let Some(principal) = principal.filter(|_| ok) else {
            let mut f = self.failures.lock().expect("failures lock");
            let e = f.entry(username.to_string()).or_insert(Failures {
                count: 0,
                last: Instant::now(),
            });
            e.count += 1;
            e.last = Instant::now();
            self.login_failures_total.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(user = username, "SEC-4: failed admin login");
            return Err(LoginError::Invalid);
        };

        self.failures
            .lock()
            .expect("failures lock")
            .remove(username);
        let token = random_token();
        let info = SessionInfo {
            username: principal.username.clone(),
            role: principal.role,
            must_change_password: principal.must_change_password,
            csrf: random_token(),
        };
        let now = Instant::now();
        self.sessions.write().expect("sessions lock").insert(
            token.clone(),
            Session {
                username: info.username.clone(),
                role: info.role,
                must_change_password: info.must_change_password,
                csrf: info.csrf.clone(),
                created: now,
                last_seen: now,
            },
        );
        if let Some(p) = self
            .principals
            .write()
            .expect("principals lock")
            .get_mut(username)
        {
            p.last_login_secs = Some(now_secs());
        }
        let _ = self.persist();
        self.logins_total.fetch_add(1, Ordering::Relaxed);
        tracing::info!(
            user = username,
            role = info.role.as_str(),
            must_change_password = info.must_change_password,
            "SEC-4: admin login"
        );
        Ok((token, info))
    }

    /// Resolve a session token, enforcing idle and absolute expiry.
    pub fn session(&self, token: &str) -> Option<SessionInfo> {
        let mut sessions = self.sessions.write().expect("sessions lock");
        let s = sessions.get_mut(token)?;
        if s.created.elapsed() > SESSION_ABSOLUTE || s.last_seen.elapsed() > SESSION_IDLE {
            sessions.remove(token);
            return None;
        }
        s.last_seen = Instant::now();
        Some(SessionInfo {
            username: s.username.clone(),
            role: s.role,
            must_change_password: s.must_change_password,
            csrf: s.csrf.clone(),
        })
    }

    pub fn logout(&self, token: &str) {
        self.sessions.write().expect("sessions lock").remove(token);
    }

    /// Change a principal's password, clearing the rotation flag. The
    /// old password is required even mid-forced-rotation: a session
    /// hijacked before the first change must not be able to lock the
    /// real operator out.
    pub fn change_password(
        &self,
        username: &str,
        old: &str,
        new: &str,
    ) -> std::result::Result<(), String> {
        let new = new.trim_end_matches(['\r', '\n']);
        if new.chars().count() < MIN_PASSWORD_LEN {
            return Err(format!(
                "password must be at least {MIN_PASSWORD_LEN} characters"
            ));
        }
        if new == DEFAULT_PASSWORD || new.eq_ignore_ascii_case(username) {
            return Err("password must not be the default or the username".to_string());
        }
        if new == old {
            return Err("new password must differ from the current one".to_string());
        }

        let mut principals = self.principals.write().expect("principals lock");
        let p = principals
            .get_mut(username)
            .ok_or_else(|| "no such principal".to_string())?;
        let parsed = PasswordHash::new(&p.password_hash).map_err(|e| e.to_string())?;
        Argon2::default()
            .verify_password(old.as_bytes(), &parsed)
            .map_err(|_| "current password is incorrect".to_string())?;

        p.password_hash = hash_password(new).map_err(|e| e.to_string())?;
        p.must_change_password = false;
        p.is_default_credential = false;
        drop(principals);
        self.persist().map_err(|e| format!("persist: {e}"))?;

        // Every existing session for this principal dies with the old
        // password — including the one that just changed it, so a stolen
        // cookie cannot outlive the rotation that was meant to stop it.
        let mut sessions = self.sessions.write().expect("sessions lock");
        sessions.retain(|_, s| s.username != username);
        tracing::info!(
            user = username,
            "SEC-4: password changed; sessions invalidated"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use timelake_store::LocalStore;

    fn auth_on(dir: &std::path::Path) -> Arc<Auth> {
        let store: Arc<dyn Store> = Arc::new(LocalStore::new(dir).unwrap());
        Auth::open(store, None).unwrap()
    }

    fn auth_on_with(dir: &std::path::Path, bootstrap: &str) -> Arc<Auth> {
        let store: Arc<dyn Store> = Arc::new(LocalStore::new(dir).unwrap());
        Auth::open(store, Some(bootstrap)).unwrap()
    }

    #[test]
    fn cert_grants_are_set_read_removed_and_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let auth = auth_on(dir.path());
            assert_eq!(
                auth.cert_grants("cn=probe"),
                None,
                "unmapped identity has none"
            );
            auth.set_cert_grants("cn=probe", vec!["alpha".into(), "beta".into()])
                .unwrap();
            assert_eq!(
                auth.cert_grants("cn=probe"),
                Some(vec!["alpha".into(), "beta".into()])
            );
            // Distinct identities do not bleed into each other.
            assert_eq!(auth.cert_grants("cn=other"), None);
        }
        {
            // Reopened from the same store: the grants persisted.
            let auth = auth_on(dir.path());
            assert_eq!(
                auth.cert_grants("cn=probe"),
                Some(vec!["alpha".into(), "beta".into()]),
                "cert grants must survive a restart, like tokens"
            );
            assert!(auth.remove_cert_grants("cn=probe").unwrap());
            assert!(
                !auth.remove_cert_grants("cn=probe").unwrap(),
                "idempotent remove"
            );
            assert_eq!(auth.cert_grants("cn=probe"), None);
        }
        {
            let auth = auth_on(dir.path());
            assert_eq!(auth.cert_grants("cn=probe"), None, "removal persisted too");
        }
    }

    #[test]
    fn seeds_admin_admin_flagged_for_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let auth = auth_on(dir.path());
        assert!(auth.default_credential_active());

        let (_token, info) = auth.login("admin", "admin").expect("seeded login works");
        assert_eq!(info.role, Role::Admin);
        assert!(
            info.must_change_password,
            "the seeded credential must be flagged for rotation"
        );

        assert!(matches!(
            auth.login("admin", "wrong"),
            Err(LoginError::Invalid)
        ));
        assert!(matches!(
            auth.login("nobody", "admin"),
            Err(LoginError::Invalid)
        ));
    }

    #[test]
    fn changing_the_password_clears_the_flag_and_persists() {
        let dir = tempfile::tempdir().unwrap();
        let auth = auth_on(dir.path());

        // policy: too short, the default itself, or the username
        assert!(auth.change_password("admin", "admin", "short").is_err());
        assert!(auth.change_password("admin", "admin", "admin").is_err());
        assert!(auth.change_password("admin", "admin", "ADMIN").is_err());
        // wrong current password
        assert!(
            auth.change_password("admin", "nope", "correct horse")
                .is_err()
        );

        auth.change_password("admin", "admin", "correct horse battery")
            .expect("valid rotation");
        assert!(!auth.default_credential_active());
        assert!(
            auth.login("admin", "admin").is_err(),
            "old password is dead"
        );
        let (_t, info) = auth.login("admin", "correct horse battery").unwrap();
        assert!(!info.must_change_password);

        // survives a restart, and does NOT re-seed
        let reopened = auth_on(dir.path());
        assert!(!reopened.default_credential_active());
        assert!(reopened.login("admin", "admin").is_err());
        let (_t, info) = reopened.login("admin", "correct horse battery").unwrap();
        assert!(!info.must_change_password);
    }

    #[test]
    fn rotation_invalidates_existing_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let auth = auth_on(dir.path());
        let (token, _) = auth.login("admin", "admin").unwrap();
        assert!(auth.session(&token).is_some());
        auth.change_password("admin", "admin", "a better password")
            .unwrap();
        assert!(
            auth.session(&token).is_none(),
            "a session predating the rotation must not survive it"
        );
    }

    #[test]
    fn repeated_failures_are_rate_limited() {
        let dir = tempfile::tempdir().unwrap();
        let auth = auth_on(dir.path());
        for _ in 0..=FREE_ATTEMPTS {
            let _ = auth.login("admin", "guess");
        }
        match auth.login("admin", "guess") {
            Err(LoginError::RateLimited(_)) => {}
            other => panic!("expected rate limiting, got {other:?}"),
        }
        // and the correct password is refused too while backed off —
        // the limiter is on the principal, not on the guess
        assert!(matches!(
            auth.login("admin", "admin"),
            Err(LoginError::RateLimited(_))
        ));
    }

    #[test]
    fn tokens_issued_and_revoked_on_one_node_reach_another_through_the_store() {
        // Two Auths over ONE store = two nodes sharing a bucket. Before
        // reload_tokens existed, B learned about A's tokens at boot and
        // never again, so a token revoked on A kept working on B.
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn Store> = Arc::new(LocalStore::new(dir.path()).unwrap());
        let a = Auth::open(Arc::clone(&store), None).unwrap();
        let b = Auth::open(Arc::clone(&store), None).unwrap();

        // Nothing to do: same bytes, no swap, no count.
        assert!(!b.reload_tokens().unwrap());
        assert_eq!(b.token_reloads_total.load(Ordering::Relaxed), 0);

        let (secret, rec) = a
            .issue_token("shipper", Scope::Write, vec![], vec![], None, "admin")
            .unwrap();
        // A's own write is not a change to A.
        assert!(!a.reload_tokens().unwrap());
        // B: a tick-style reload picks it up, once.
        assert!(b.reload_tokens().unwrap());
        assert!(
            !b.reload_tokens().unwrap(),
            "second reload sees the same bytes"
        );
        assert_eq!(b.token_reloads_total.load(Ordering::Relaxed), 1);
        assert_eq!(b.verify_token(&secret).unwrap().id, rec.id);

        // Revoke on A; B still holds the stale record until its next reload —
        // and then refuses. This is the half that makes revocation real.
        assert!(a.revoke_token(&rec.id).unwrap());
        assert!(b.reload_tokens().unwrap());
        assert!(b.verify_token(&secret).is_err());
        assert_eq!(b.token_reloads_total.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn an_unknown_token_triggers_one_bounded_reload_so_a_fresh_token_works_on_first_use() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn Store> = Arc::new(LocalStore::new(dir.path()).unwrap());
        let a = Auth::open(Arc::clone(&store), None).unwrap();
        let b = Auth::open(Arc::clone(&store), None).unwrap();

        let (secret, rec) = a
            .issue_token("grafana", Scope::Read, vec![], vec![], None, "admin")
            .unwrap();
        // No explicit reload on B: the miss itself re-reads the store.
        assert_eq!(b.verify_token(&secret).unwrap().id, rec.id);
        assert_eq!(b.token_reloads_total.load(Ordering::Relaxed), 1);

        // A second unknown token within the same second does NOT re-read —
        // the miss path is rate-limited — and is simply refused.
        let before = b.token_reloads_total.load(Ordering::Relaxed);
        assert!(b.verify_token("tldb_nope").is_err());
        assert_eq!(b.token_reloads_total.load(Ordering::Relaxed), before);
    }

    #[test]
    fn a_provisioned_bootstrap_password_is_not_the_default() {
        let dir = tempfile::tempdir().unwrap();
        let auth = auth_on_with(dir.path(), "provisioned secret");

        assert!(
            !auth.default_credential_active(),
            "an explicitly provisioned password is not the well-known default"
        );
        assert!(auth.login("admin", "admin").is_err());
        let (_t, info) = auth.login("admin", "provisioned secret").unwrap();
        assert!(
            info.must_change_password,
            "provisioned credentials still rotate at first login"
        );
    }

    #[test]
    fn roles_are_ordered() {
        assert!(Role::Admin.allows(Role::Operator));
        assert!(Role::Operator.allows(Role::Viewer));
        assert!(!Role::Viewer.allows(Role::Operator));
        assert!(!Role::Operator.allows(Role::Admin));
        assert_eq!(Role::parse("operator"), Some(Role::Operator));
        assert_eq!(Role::parse("root"), None);
    }
}
