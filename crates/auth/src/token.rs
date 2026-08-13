//! Data-plane credentials (SEC-4 phased).
//!
//! WHY TOKENS AND NOT PASSWORDS OR HEADERS: the mechanism was chosen by
//! measurement, not preference (`docs/evidence/data-auth-client-probe.log`).
//! Grafana's InfluxDB datasource in Flight SQL mode forwards exactly one
//! credential onto the gRPC path — the `token` field, as
//! `authorization: Bearer <token>`. Its basic-auth toggle and its custom
//! header fields are HTTP-only and never reach Flight SQL, so a design
//! resting on either would work under curl and fail under the one client
//! FR-8 makes first-class. Telegraf then spells the same idea two other
//! ways (`Token …` on influxdb_v2, `Basic …` on v1). One token accepted
//! under three spellings is the only shape that fits all three.
//!
//! WHY SHA-256 AND NOT ARGON2ID: principals hash with Argon2id because a
//! human picks the password and an attacker who steals the file would
//! otherwise brute-force it. A token is 256 bits from the OS CSPRNG —
//! there is no brute-force surface to slow down, so the KDF would buy
//! nothing and cost everything: Argon2id is deliberately ~50 ms, and the
//! write path is specified at hundreds of thousands of lines per second.
//! Putting a memory-hard KDF in front of that is a self-inflicted denial
//! of service, which RR-1 forbids. Verification is a SHA-256 of the
//! presented secret looked up in a map, compared in constant time.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use argon2::password_hash::rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// Marks a TimeLakeDB token in logs, config files and secret scanners.
/// The value after it is base64url, so the whole thing is one word and
/// survives being pasted into a Grafana field or a shell variable.
pub const TOKEN_PREFIX: &str = "tldb_";

/// What a token may do. A total order would be wrong here: shipping
/// agents must write without being able to read the database back, which
/// is the entire point of giving Tributary its own credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    Read,
    Write,
    ReadWrite,
}

impl Scope {
    pub fn as_str(self) -> &'static str {
        match self {
            Scope::Read => "read",
            Scope::Write => "write",
            Scope::ReadWrite => "read_write",
        }
    }

    pub fn parse(s: &str) -> Option<Scope> {
        match s {
            "read" => Some(Scope::Read),
            "write" => Some(Scope::Write),
            "read_write" | "readwrite" | "rw" => Some(Scope::ReadWrite),
            _ => None,
        }
    }

    pub fn allows_read(self) -> bool {
        matches!(self, Scope::Read | Scope::ReadWrite)
    }

    pub fn allows_write(self) -> bool {
        matches!(self, Scope::Write | Scope::ReadWrite)
    }
}

/// How strictly the data plane treats credentials. The three states are
/// a migration, not a preference: `Off` is today's behaviour, `Optional`
/// is the window in which operators roll credentials out while watching
/// the authenticated/anonymous split, and `Required` is the end state.
/// Jumping straight to `Required` is what breaks a fleet, so the middle
/// state exists to make that avoidable — the same discipline want-mode
/// mTLS uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataAuthMode {
    Off,
    Optional,
    Required,
}

impl DataAuthMode {
    pub fn as_str(self) -> &'static str {
        match self {
            DataAuthMode::Off => "off",
            DataAuthMode::Optional => "optional",
            DataAuthMode::Required => "required",
        }
    }

    pub fn parse(s: &str) -> Option<DataAuthMode> {
        match s.trim().to_ascii_lowercase().as_str() {
            "off" | "" => Some(DataAuthMode::Off),
            "optional" | "want" => Some(DataAuthMode::Optional),
            "required" | "require" | "on" => Some(DataAuthMode::Required),
            _ => None,
        }
    }
}

/// A token as stored: never the secret, only its digest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenRecord {
    /// Stable id, safe to show and to name in an audit line.
    pub id: String,
    /// Operator-facing label ("grafana-prod", "tributary-edge-07").
    pub description: String,
    /// Hex SHA-256 of the secret. The secret itself is shown once, at
    /// creation, and is not recoverable — losing it means issuing a new
    /// one, which is the property that makes revocation meaningful.
    pub hash: String,
    pub scope: Scope,
    /// Databases this token may touch; empty means all of them.
    #[serde(default)]
    pub databases: Vec<String>,
    /// SEC-2 authorizations this token is *granted*. A caller's claimed
    /// authorizations are intersected with these, so presenting a token
    /// can only narrow what it sees.
    #[serde(default)]
    pub authorizations: Vec<String>,
    /// Which principal issued it, for the audit trail.
    pub created_by: String,
    pub created_at_secs: u64,
    #[serde(default)]
    pub expires_at_secs: Option<u64>,
    #[serde(default)]
    pub revoked: bool,
    /// Coarse last-use stamp. Deliberately coarse: a precise one would
    /// mean a store write per request.
    #[serde(default)]
    pub last_used_secs: Option<u64>,
}

impl TokenRecord {
    pub fn is_valid_at(&self, now: u64) -> bool {
        !self.revoked && self.expires_at_secs.is_none_or(|e| e > now)
    }

    /// Empty `databases` means every database — the common case for a
    /// single-tenant deployment, and the default a bare token gets.
    pub fn allows_database(&self, db: &str) -> bool {
        self.databases.is_empty() || self.databases.iter().any(|d| d == db)
    }
}

/// The credential a request presented, once verified.
#[derive(Debug, Clone)]
pub struct TokenIdentity {
    pub id: String,
    pub description: String,
    pub scope: Scope,
    pub databases: Vec<String>,
    pub authorizations: Vec<String>,
}

/// Why a request was refused. Kept coarse on purpose — a client learning
/// *which* of "unknown", "revoked" or "expired" applies learns whether a
/// token it holds was ever real.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenError {
    /// No credential presented at all, and the mode requires one.
    Missing,
    /// Presented something that is not a usable credential.
    Invalid,
    /// Valid credential, but not for this action or this database.
    Forbidden,
}

impl TokenError {
    pub fn code(self) -> &'static str {
        match self {
            TokenError::Missing => "unauthenticated",
            TokenError::Invalid => "unauthenticated",
            TokenError::Forbidden => "forbidden",
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            TokenError::Missing => "authentication required",
            TokenError::Invalid => "invalid token",
            TokenError::Forbidden => "token is not permitted to do that",
        }
    }
}

pub fn hash_token(secret: &str) -> String {
    let digest = Sha256::digest(secret.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// A fresh 256-bit secret, prefixed and base64url-encoded.
pub fn generate_secret() -> String {
    let mut raw = [0u8; 32];
    OsRng.fill_bytes(&mut raw);
    format!("{TOKEN_PREFIX}{}", base64url(&raw))
}

fn base64url(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        let take = chunk.len() + 1; // 3 bytes -> 4 chars, 2 -> 3, 1 -> 2
        for i in 0..take {
            out.push(A[((n >> (18 - 6 * i)) & 0x3F) as usize] as char);
        }
    }
    out // unpadded: '=' would need escaping in some config formats
}

/// Pull a token out of an `Authorization` header value.
///
/// Three spellings, because three stock clients spell it three ways and
/// all of them are first-class (FR-8/FR-9). `Basic` maps the *password*
/// to the token and ignores the username, which is how InfluxDB's own v1
/// compatibility works — Telegraf's v1 output has no token field, only
/// username and password.
pub fn token_from_authorization(header: &str) -> Option<String> {
    let (scheme, rest) = header.split_once(' ')?;
    let rest = rest.trim();
    if rest.is_empty() {
        return None;
    }
    if scheme.eq_ignore_ascii_case("bearer") || scheme.eq_ignore_ascii_case("token") {
        return Some(rest.to_string());
    }
    if scheme.eq_ignore_ascii_case("basic") {
        let decoded = base64_decode(rest)?;
        let text = String::from_utf8(decoded).ok()?;
        let (_user, password) = text.split_once(':')?;
        if password.is_empty() {
            return None;
        }
        return Some(password.to_string());
    }
    None
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some((c - b'A') as u32),
            b'a'..=b'z' => Some((c - b'a') as u32 + 26),
            b'0'..=b'9' => Some((c - b'0') as u32 + 52),
            b'+' | b'-' => Some(62),
            b'/' | b'_' => Some(63),
            _ => None,
        }
    }
    let cleaned: Vec<u8> = s
        .bytes()
        .filter(|&c| c != b'=' && !c.is_ascii_whitespace())
        .collect();
    let mut out = Vec::with_capacity(cleaned.len() * 3 / 4);
    for chunk in cleaned.chunks(4) {
        if chunk.len() < 2 {
            return None;
        }
        let mut n = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            n |= val(c)? << (18 - 6 * i);
        }
        for i in 0..chunk.len() - 1 {
            out.push(((n >> (16 - 8 * i)) & 0xFF) as u8);
        }
    }
    Some(out)
}

/// Constant-time digest comparison. The digests are of high-entropy
/// secrets so a timing oracle is a stretch, but the map lookup already
/// leaks nothing and this costs nothing.
pub fn digests_match(a: &str, b: &str) -> bool {
    a.as_bytes().ct_eq(b.as_bytes()).into()
}

/// Counters behind the metrics that make a migration observable. Exactly
/// the lesson from want-mode mTLS: without a measured split, the decision
/// to move from `optional` to `required` is a guess, and guessing wrong
/// takes a fleet down.
#[derive(Debug, Default)]
pub struct DataAuthCounts {
    pub authenticated: AtomicU64,
    pub anonymous: AtomicU64,
    pub rejected: AtomicU64,
}

impl DataAuthCounts {
    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.authenticated.load(Ordering::Relaxed),
            self.anonymous.load(Ordering::Relaxed),
            self.rejected.load(Ordering::Relaxed),
        )
    }
}

/// The token set, indexed by digest for O(1) verification.
#[derive(Debug, Default)]
pub struct TokenIndex {
    by_hash: BTreeMap<String, TokenRecord>,
}

impl TokenIndex {
    pub fn from_records(records: Vec<TokenRecord>) -> TokenIndex {
        TokenIndex {
            by_hash: records.into_iter().map(|r| (r.hash.clone(), r)).collect(),
        }
    }

    pub fn records(&self) -> Vec<TokenRecord> {
        self.by_hash.values().cloned().collect()
    }

    pub fn insert(&mut self, record: TokenRecord) {
        self.by_hash.insert(record.hash.clone(), record);
    }

    pub fn by_id_mut(&mut self, id: &str) -> Option<&mut TokenRecord> {
        self.by_hash.values_mut().find(|r| r.id == id)
    }

    pub fn verify(&self, secret: &str, now: u64) -> Result<TokenIdentity, TokenError> {
        let digest = hash_token(secret);
        let found = self
            .by_hash
            .get(&digest)
            .filter(|r| digests_match(&r.hash, &digest));
        match found {
            Some(r) if r.is_valid_at(now) => Ok(TokenIdentity {
                id: r.id.clone(),
                description: r.description.clone(),
                scope: r.scope,
                databases: r.databases.clone(),
                authorizations: r.authorizations.clone(),
            }),
            // Revoked and expired are reported as "invalid" like an
            // unknown token: distinguishing them tells a caller holding a
            // stale secret that it was once real.
            _ => Err(TokenError::Invalid),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_all_three_spellings_stock_clients_use() {
        // Grafana over Flight SQL, and Tributary.
        assert_eq!(
            token_from_authorization("Bearer tldb_abc").as_deref(),
            Some("tldb_abc")
        );
        // Telegraf's influxdb_v2 output.
        assert_eq!(
            token_from_authorization("Token tldb_abc").as_deref(),
            Some("tldb_abc")
        );
        // Telegraf's v1 output has no token field — password carries it.
        // base64("ignored:tldb_abc")
        assert_eq!(
            token_from_authorization("Basic aWdub3JlZDp0bGRiX2FiYw==").as_deref(),
            Some("tldb_abc")
        );
        // Case-insensitive: the header scheme is not case sensitive and
        // clients disagree about capitalisation.
        assert_eq!(
            token_from_authorization("bearer tldb_abc").as_deref(),
            Some("tldb_abc")
        );
        assert_eq!(token_from_authorization("Digest xyz"), None);
        assert_eq!(token_from_authorization("Bearer   ").as_deref(), None);
    }

    #[test]
    fn basic_auth_without_a_password_is_not_a_credential() {
        // base64("user:") — an empty password must not authenticate as
        // the empty token.
        assert_eq!(token_from_authorization("Basic dXNlcjo="), None);
    }

    #[test]
    fn secrets_are_prefixed_unique_and_not_recoverable_from_the_record() {
        let a = generate_secret();
        let b = generate_secret();
        assert!(a.starts_with(TOKEN_PREFIX));
        assert_ne!(a, b);
        assert!(a.len() > 40, "expected a 256-bit secret, got {}", a.len());
        let h = hash_token(&a);
        assert_eq!(h.len(), 64);
        assert!(!h.contains(&a[TOKEN_PREFIX.len()..]));
    }

    #[test]
    fn verify_accepts_a_live_token_and_refuses_revoked_expired_and_unknown() {
        let secret = generate_secret();
        let mut idx = TokenIndex::from_records(vec![TokenRecord {
            id: "t1".into(),
            description: "grafana".into(),
            hash: hash_token(&secret),
            scope: Scope::Read,
            databases: vec!["poc".into()],
            authorizations: vec!["ops".into()],
            created_by: "admin".into(),
            created_at_secs: 100,
            expires_at_secs: Some(200),
            revoked: false,
            last_used_secs: None,
        }]);

        let id = idx.verify(&secret, 150).expect("live token verifies");
        assert_eq!(id.id, "t1");
        assert_eq!(id.authorizations, vec!["ops".to_string()]);
        assert!(id.scope.allows_read() && !id.scope.allows_write());

        assert_eq!(
            idx.verify(&secret, 300).err(),
            Some(TokenError::Invalid),
            "expired"
        );
        assert_eq!(
            idx.verify("tldb_nope", 150).err(),
            Some(TokenError::Invalid)
        );

        idx.by_id_mut("t1").expect("present").revoked = true;
        assert_eq!(
            idx.verify(&secret, 150).err(),
            Some(TokenError::Invalid),
            "revoked"
        );
    }

    #[test]
    fn database_scoping_defaults_to_all_but_is_enforced_when_set() {
        let mut r = TokenRecord {
            id: "t".into(),
            description: String::new(),
            hash: String::new(),
            scope: Scope::ReadWrite,
            databases: vec![],
            authorizations: vec![],
            created_by: "admin".into(),
            created_at_secs: 0,
            expires_at_secs: None,
            revoked: false,
            last_used_secs: None,
        };
        assert!(r.allows_database("anything"), "empty list means all");
        r.databases = vec!["poc".into()];
        assert!(r.allows_database("poc"));
        assert!(!r.allows_database("other"));
    }

    #[test]
    fn modes_parse_the_spellings_an_operator_will_actually_type() {
        assert_eq!(DataAuthMode::parse("off"), Some(DataAuthMode::Off));
        assert_eq!(DataAuthMode::parse(""), Some(DataAuthMode::Off));
        assert_eq!(
            DataAuthMode::parse("Optional"),
            Some(DataAuthMode::Optional)
        );
        assert_eq!(
            DataAuthMode::parse(" required "),
            Some(DataAuthMode::Required)
        );
        // A typo must not silently disable authentication.
        assert_eq!(DataAuthMode::parse("requried"), None);
    }

    #[test]
    fn base64_round_trips_the_shapes_basic_auth_produces() {
        for raw in ["a:b", "user:tldb_xyz", "u:p:with:colons", "x:padded=="] {
            let enc = {
                // encode with the standard alphabet, padded, as a client would
                const A: &[u8; 64] =
                    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
                let b = raw.as_bytes();
                let mut out = String::new();
                for c in b.chunks(3) {
                    let t = [c[0], *c.get(1).unwrap_or(&0), *c.get(2).unwrap_or(&0)];
                    let n = ((t[0] as u32) << 16) | ((t[1] as u32) << 8) | t[2] as u32;
                    for i in 0..c.len() + 1 {
                        out.push(A[((n >> (18 - 6 * i)) & 0x3F) as usize] as char);
                    }
                    for _ in 0..(3 - c.len()) {
                        out.push('=');
                    }
                }
                out
            };
            let got = base64_decode(&enc).expect("decodes");
            assert_eq!(String::from_utf8(got).unwrap(), raw, "round trip {raw}");
        }
    }
}
