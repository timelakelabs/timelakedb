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

/// Where the principal store lives in the object store.
pub const PRINCIPALS_PATH: &str = "catalog/config/principals.json";
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
    sessions: RwLock<HashMap<String, Session>>,
    failures: Mutex<HashMap<String, Failures>>,
    /// Successful and failed logins, for /metrics.
    pub logins_total: AtomicU64,
    pub login_failures_total: AtomicU64,
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

        let auth = Auth {
            store,
            principals: RwLock::new(principals),
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
