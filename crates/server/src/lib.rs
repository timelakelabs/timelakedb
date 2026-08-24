//! TimeLakeDB server — M2: a real storage engine.
//!
//! Write path (ARCHITECTURE §5): parse → WAL (durable before 204) →
//! mutable buffer. Flush (§6/§7 L0): buffers swap out under the ingest
//! gate, WAL rotates, rows are PK-sorted + LWW-deduped, split by
//! (table, UTC hour), encoded to Parquet through the Store chokepoint
//! (SEC-1), committed to the manifest-log catalog (CL-1), and the sealed
//! WAL generations are reclaimed. Reads union buffer snapshots with
//! cataloged Parquet under the RR-1 memory pool.
//!
//! M2 honesty notes:
//! - Crash between manifest commit and WAL reclaim replays flushed rows:
//!   duplicates possible in that window (cross-file dedup lands with
//!   compaction, M3). Acknowledged writes are never lost.
//! - Reads load every cataloged file per query (no pruning yet — M3/M4);
//!   fine at smoke scale, called out in ARCHITECTURE §16.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

use serde_json::Value;
use timelake_api::WriteError;
// Re-exported so callers and tests name the retention types through the
// engine they configure, without depending on the api crate directly.
pub use timelake_api::{RETENTION_ANY_DB, RetentionPolicy, RollupAgg, RollupDef, RollupFn};
use timelake_buffer::{TableBuffer, flush};
use timelake_catalog::{Catalog, FileMeta};
use timelake_ingest::{parse_lines, precision_multiplier};
use timelake_query::{QuerySession, batches_to_json};
use timelake_store::{CachingKms, EncryptingStore, Kms, KmsStats, LocalKek, LocalStore, Store};
use timelake_store_s3::{AwsContext, AwsKms, S3Stats, S3Store};
use timelake_wal::{Wal, WalCipher};

pub mod compaction;
pub mod logfile;
pub mod querier;
pub mod ratelimit;
pub mod replication;
pub mod router;
pub mod selfmon;
pub mod tls_identity;
use ratelimit::ClientLimiter;
use replication::Replicator;

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub query_mem_bytes: usize,
    pub flush_rows: usize,
    pub flush_age_secs: u64,
    pub wal_max_bytes: u64,
    /// Compact a (table, hour) partition once it accumulates this many
    /// L0 files (PR-6 lever).
    pub compact_min_files: usize,
    /// Retention policies (FR-7). Empty = keep everything.
    pub retention: Vec<RetentionPolicy>,
    /// Admission control: queries beyond this queue (RR-1).
    pub max_concurrent_queries: usize,
    /// SEC-6 (exposure 6): the most concurrent queries any ONE client may
    /// hold of the `max_concurrent_queries` budget, keyed by data-plane
    /// token when present and by network origin otherwise. Beyond it the
    /// client is refused (429 / Flight `ResourceExhausted`) so it cannot
    /// starve the rest. Kept below `max_concurrent_queries` so a permit is
    /// always reachable by another client; `0` disables the per-client cap.
    /// Raise this and `max_concurrent_queries` together for a single-tenant
    /// deployment whose one dashboard issues many concurrent panels.
    pub max_concurrent_queries_per_client: usize,
    /// Server-side query cap (RR-2): abandoned work stops burning pool.
    pub query_timeout_secs: u64,
    /// Files superseded by compaction/retention are physically deleted
    /// only after this grace (must exceed query_timeout so an in-flight
    /// query's catalog snapshot never dangles — the AT-3 race).
    pub gc_grace_secs: u64,
    /// SEC-4 phased: how the data plane treats credentials.
    /// `Off` (default) does not read `Authorization` at all — the
    /// documented compatibility contract. See `timelake_auth::guard`.
    pub data_auth: timelake_auth::DataAuthMode,
    /// CL-2: how long a write waits for the replication peer before the
    /// node declares itself degraded and acks on local durability alone.
    ///
    /// This bounds what a *slow* peer can do to ingest, which is the
    /// failure a *dead* peer does not have: dead trips to degraded at once
    /// and availability holds, while slow never trips anything and simply
    /// multiplies every write's latency. Replication is synchronous before
    /// the ack, so this value is a per-write latency ceiling — at the
    /// reference workload's ~232 events/s, seconds here are an ingest
    /// outage rather than a hiccup (`docs/P1-1_DESIGN.md` D1).
    ///
    /// The default is deliberately far below any plausible healthy
    /// round-trip on a cluster network, so that "slow" and "dead" become
    /// the same case — the safe direction, and the one the alarm already
    /// describes. Raise it only with a measurement showing healthy peers
    /// legitimately exceed it; `timelake_cl2_replication_degraded_events`
    /// makes a too-aggressive value visible as flapping rather than as
    /// silence.
    pub repl_timeout_ms: u64,
    /// Largest accepted HTTP body, on the public data plane and on the
    /// intra-cluster listener alike.
    ///
    /// FR-1 requires >=10 MB bodies. axum's `Bytes` extractor defaults to
    /// 2 MB, which silently broke that contract in two places at once: a
    /// large batch was refused with 413 on `/write`, and a large *frame*
    /// was refused the same way by the peer's `/internal/v1/replicate` —
    /// which the replicator reads as a failed peer, so the node dropped to
    /// degraded and "durable on two nodes" stopped holding, with an alarm
    /// that looked unexplained.
    ///
    /// Both routers take this one value on purpose. If the public limit
    /// ever exceeded the internal one, every write between the two sizes
    /// would be accepted locally and refused by its replica.
    pub max_body_bytes: usize,
    /// CL-3: how many live/snapshot reads the intra-cluster listener will
    /// serve at once before refusing.
    ///
    /// A querier unions the ingesters' live buffers on every query, so read
    /// load on a querier becomes work on every ingester — and an ingester's
    /// real job is taking writes. Without a ceiling an expensive query fans
    /// out unbounded, slows the ingester, and its peer then blocks on every
    /// write it takes (`docs/P1-1_DESIGN.md` D2).
    ///
    /// Refusing is the honest outcome and the querier already models it:
    /// a failed snapshot makes it refuse the query rather than answer from
    /// an incomplete cluster, because a partial answer is worse than no
    /// answer. Watch `timelake_cl3_reads_refused_total` — a rising count
    /// means this ceiling, not a broken peer.
    ///
    /// Deliberately NOT applied to `/internal/v1/replicate`: throttling a
    /// peer's writes is the stall D1 exists to prevent. Nor to `/health`,
    /// which must answer while the node is saturated — that is when the
    /// answer matters most.
    pub internal_max_concurrent: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            query_mem_bytes: 1 << 30, // 1 GiB RR-1 pool
            flush_rows: 50_000,       // L0 trigger
            flush_age_secs: 60,
            wal_max_bytes: 2 << 30, // RR-3 replay bound / RR-5 backpressure
            compact_min_files: 4,
            retention: Vec::new(),
            max_concurrent_queries: 6,
            // Below the global 6, so two permits are always reachable by
            // other clients — exposure 6's "no one client takes the whole
            // budget", without refusing a modest single-client burst.
            max_concurrent_queries_per_client: 4,
            query_timeout_secs: 600,
            gc_grace_secs: 900,
            data_auth: timelake_auth::DataAuthMode::Off,
            repl_timeout_ms: 250,
            max_body_bytes: 32 << 20, // 32 MiB: FR-1 asks for >=10 MB, with headroom
            // Above one querier's max_concurrent_queries (6), so a single
            // querier at full tilt is served rather than refused.
            internal_max_concurrent: 8,
        }
    }
}

/// The composed storage stack (ARCHITECTURE §12): base backend (local
/// directory or S3), optional envelope encryption (LocalKek or AWS KMS,
/// optionally key-cached), plus the stats handles /metrics exports.
pub struct StoreStack {
    pub store: Arc<dyn Store>,
    pub encrypted: bool,
    pub backend: &'static str,
    pub kms_stats: Option<Arc<KmsStats>>,
    pub s3_stats: Option<Arc<S3Stats>>,
    /// SEC-8: the envelope key handle, surfaced so the WAL can encrypt with
    /// the SAME key as the object store. `Some` exactly when `encrypted`.
    pub kms: Option<Arc<dyn Kms>>,
}

impl StoreStack {
    fn plain(store: Arc<dyn Store>, encrypted: bool) -> StoreStack {
        StoreStack {
            store,
            encrypted,
            backend: "custom",
            kms_stats: None,
            s3_stats: None,
            kms: None,
        }
    }
}

/// Build the storage stack from the environment. The decisions live here
/// and only here — nothing downstream can tell local from S3 or
/// plaintext from encrypted (SEC-1/CL-1, §12).
///
/// - `TIMELAKE_OBJECT_STORE` — unset = local `objects/` dir;
///   `s3://bucket[/prefix]` = S3 (LocalStack via `AWS_ENDPOINT_URL`).
/// - `TIMELAKE_KMS_KEY_ID` — envelope-encrypt with AWS KMS data keys,
///   behind the key cache unless `TIMELAKE_KMS_CACHE=off`
///   (`TIMELAKE_KMS_CACHE_MAX_AGE_SECS` / `_MAX_USES` bound the window).
/// - `TIMELAKE_ENCRYPTION_KEY[_FILE]` — envelope-encrypt with a local
///   KEK, as before. Setting BOTH key sources is a refused ambiguity.
/// - `TIMELAKE_S3_SSE_KEY_ID` — SSE-KMS key for server-side encryption
///   (defaults to `TIMELAKE_KMS_KEY_ID`; Bucket Keys requested per PUT).
pub fn store_stack_from_env(data_dir: &Path) -> std::io::Result<StoreStack> {
    let object_store = std::env::var("TIMELAKE_OBJECT_STORE").ok();
    let kms_key = std::env::var("TIMELAKE_KMS_KEY_ID").ok();
    let local_key = encryption_key_from_env()?;
    if kms_key.is_some() && local_key.is_some() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "TIMELAKE_KMS_KEY_ID and TIMELAKE_ENCRYPTION_KEY are both set — \
             pick one key source",
        ));
    }

    let is_s3 = object_store
        .as_deref()
        .is_some_and(|u| u.starts_with("s3://"));
    let ctx = if is_s3 || kms_key.is_some() {
        Some(AwsContext::new()?)
    } else {
        None
    };

    let (base, s3_stats, backend): (Arc<dyn Store>, Option<Arc<S3Stats>>, &'static str) =
        match object_store {
            None => (
                Arc::new(LocalStore::new(&data_dir.join("objects"))?),
                None,
                "local",
            ),
            Some(url) if url.starts_with("s3://") => {
                let sse = std::env::var("TIMELAKE_S3_SSE_KEY_ID")
                    .ok()
                    .or_else(|| kms_key.clone());
                let s3 = S3Store::new(ctx.clone().expect("ctx built for s3"), &url, sse)?;
                let stats = s3.stats();
                (Arc::new(s3), Some(stats), "s3")
            }
            Some(other) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("TIMELAKE_OBJECT_STORE {other:?} is not s3://bucket[/prefix]"),
                ));
            }
        };

    fn env_u64(k: &str, d: u64) -> u64 {
        std::env::var(k)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(d)
    }
    #[allow(clippy::type_complexity)]
    let (store, encrypted, kms_stats, kms): (
        Arc<dyn Store>,
        bool,
        Option<Arc<KmsStats>>,
        Option<Arc<dyn Kms>>,
    ) = match (kms_key, local_key) {
        (Some(key_id), None) => {
            let aws = AwsKms::new(ctx.expect("ctx built for kms"), key_id);
            if std::env::var("TIMELAKE_KMS_CACHE").as_deref() == Ok("off") {
                tracing::warn!(
                    "TIMELAKE_KMS_CACHE=off: strict per-object data keys, \
                     one KMS call per object (the drill's baseline mode)"
                );
                let kms: Arc<dyn Kms> = Arc::new(aws);
                (
                    Arc::new(EncryptingStore::new(base, kms.clone())),
                    true,
                    None,
                    Some(kms),
                )
            } else {
                let cached = CachingKms::new(
                    aws,
                    std::time::Duration::from_secs(env_u64("TIMELAKE_KMS_CACHE_MAX_AGE_SECS", 300)),
                    env_u64("TIMELAKE_KMS_CACHE_MAX_USES", 1000) as u32,
                );
                let stats = cached.stats();
                let kms: Arc<dyn Kms> = Arc::new(cached);
                (
                    Arc::new(EncryptingStore::new(base, kms.clone())),
                    true,
                    Some(stats),
                    Some(kms),
                )
            }
        }
        (None, Some(kek)) => {
            let kms: Arc<dyn Kms> = Arc::new(LocalKek::new(kek));
            (
                Arc::new(EncryptingStore::new(base, kms.clone())),
                true,
                None,
                Some(kms),
            )
        }
        (None, None) => (base, false, None, None),
        (Some(_), Some(_)) => unreachable!("refused above"),
    };

    Ok(StoreStack {
        store,
        encrypted,
        backend,
        kms_stats,
        s3_stats,
        kms,
    })
}

/// Adapts the object store's envelope `Kms` to the WAL's `WalCipher` (SEC-8),
/// so the WAL encrypts with the same key without the wal crate depending on
/// the store. `Kms::generate` returns `(dek, wrapped)`; the WAL wants
/// `(wrapped, dek)`.
struct WalKms(Arc<dyn Kms>);

impl WalCipher for WalKms {
    fn generate(&self) -> std::io::Result<(Vec<u8>, [u8; 32])> {
        self.0
            .generate()
            .map(|(dek, wrapped)| (wrapped, dek))
            .map_err(|e| std::io::Error::other(e.to_string()))
    }
    fn unwrap(&self, wrapped: &[u8]) -> std::io::Result<[u8; 32]> {
        // A wrong key fails here — fail closed, as InvalidData, so WAL replay
        // refuses rather than dropping acked frames.
        self.0
            .unwrap(wrapped)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
    }
}

/// Where runtime retention config lives in the object store (FR-7).
const RETENTION_CONFIG_PATH: &str = "catalog/config/retention.json";

/// v1 was a bare `{"table": seconds}` map with no database scope at all.
const RETENTION_FORMAT_VERSION: u32 = 2;

#[derive(serde::Serialize, serde::Deserialize)]
struct RetentionDoc {
    version: u32,
    policies: Vec<RetentionPolicy>,
}

/// R-2 rollup definitions (ARCHITECTURE §18), persisted beside retention.
const ROLLUPS_CONFIG_PATH: &str = "catalog/config/rollups.json";
const ROLLUPS_FORMAT_VERSION: u32 = 1;

#[derive(serde::Serialize, serde::Deserialize)]
struct RollupDoc {
    version: u32,
    rollups: Vec<RollupDef>,
}

fn load_rollups_doc(bytes: &[u8]) -> std::io::Result<Vec<RollupDef>> {
    let doc: RollupDoc = serde_json::from_slice(bytes).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{ROLLUPS_CONFIG_PATH}: {e}"),
        )
    })?;
    Ok(doc.rollups)
}

/// Seed rollups from `TIMELAKE_ROLLUPS` — a JSON array of definitions (the
/// same shape `/admin/rollups` and `rollups.json` use), because a rollup is
/// too structured for a compact env grammar. The stored copy outranks this
/// at boot, exactly like retention. Unset or unparseable ⇒ none.
fn parse_rollups_env() -> Vec<RollupDef> {
    match std::env::var("TIMELAKE_ROLLUPS") {
        Ok(s) if !s.trim().is_empty() => match serde_json::from_str::<Vec<RollupDef>>(&s) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "TIMELAKE_ROLLUPS is not a JSON array of rollup definitions; ignoring");
                Vec::new()
            }
        },
        _ => Vec::new(),
    }
}

/// Read the stored policies, migrating a v1 document if that is what is
/// there.
///
/// **A v1 entry becomes an explicit [`RETENTION_ANY_DB`] policy, and that
/// is the only safe reading.** v1 matched on table name and ignored the
/// database, so `{"metrics": 604800}` really did delete `metrics` in every
/// database — migrating it to a single database would silently stop
/// deleting data an operator asked to have deleted, which for a retention
/// control can be a compliance failure rather than a mere surprise. The
/// wildcard preserves the behaviour exactly; what changes is that it is
/// now visible, and narrowing it is a deliberate edit.
fn load_retention_doc(bytes: &[u8]) -> std::io::Result<(Vec<RetentionPolicy>, bool)> {
    if let Ok(doc) = serde_json::from_slice::<RetentionDoc>(bytes) {
        return Ok((doc.policies, false));
    }
    let legacy: std::collections::BTreeMap<String, u64> =
        serde_json::from_slice(bytes).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{RETENTION_CONFIG_PATH}: {e}"),
            )
        })?;
    let migrated = legacy
        .into_iter()
        .map(|(table, seconds)| RetentionPolicy {
            db: RETENTION_ANY_DB.to_string(),
            table,
            seconds,
        })
        .collect();
    Ok((migrated, true))
}

/// SEC-1 key config: `TIMELAKE_ENCRYPTION_KEY` (64 hex chars) or
/// `TIMELAKE_ENCRYPTION_KEY_FILE` (a file holding them). Key material
/// stays out of [`EngineConfig`] so a `?cfg` log line can never leak it.
fn encryption_key_from_env() -> std::io::Result<Option<[u8; 32]>> {
    let hex = match std::env::var("TIMELAKE_ENCRYPTION_KEY") {
        Ok(k) => k,
        Err(_) => match std::env::var("TIMELAKE_ENCRYPTION_KEY_FILE") {
            Ok(path) => std::fs::read_to_string(&path).map_err(|e| {
                std::io::Error::new(e.kind(), format!("encryption key file {path}: {e}"))
            })?,
            Err(_) => return Ok(None),
        },
    };
    timelake_store::key_from_hex(&hex).map(Some)
}

/// Parse "pipeline_events=365d,poc.host_metrics=90d,disk_metrics=72h" (FR-7).
///
/// A bare `table=` is scoped to [`RETENTION_ANY_DB`], which is what the
/// setting has always meant; `db.table=` scopes it to one database, split
/// on the FIRST dot. A table name containing a dot therefore cannot be
/// expressed here and must be set through `/admin/retention`, which takes
/// the two as separate fields.
pub fn parse_retention(spec: &str) -> Vec<RetentionPolicy> {
    spec.split(',')
        .filter_map(|part| {
            let (scope, dur) = part.trim().split_once('=')?;
            let dur = dur.trim();
            let (num, unit_secs) = if let Some(d) = dur.strip_suffix('d') {
                (d, 86_400)
            } else if let Some(h) = dur.strip_suffix('h') {
                (h, 3_600)
            } else {
                (dur, 1)
            };
            let scope = scope.trim();
            let (db, table) = match scope.split_once('.') {
                Some((db, table)) => (db.trim().to_string(), table.trim().to_string()),
                None => (RETENTION_ANY_DB.to_string(), scope.to_string()),
            };
            Some(RetentionPolicy {
                db,
                table,
                seconds: num.parse::<u64>().ok()? * unit_secs,
            })
        })
        .collect()
}

/// `"64MiB"`, `"512KB"`, or a bare byte count. `KiB` is 1024 and `KB` is
/// 1000; both spellings are accepted because both are written, and they are
/// deliberately not treated as the same number.
pub fn parse_size_bytes(s: &str) -> Option<u64> {
    let s = s.trim();
    let (num, mult) = if let Some(v) = s.strip_suffix("GiB") {
        (v, 1024u64.pow(3))
    } else if let Some(v) = s.strip_suffix("MiB") {
        (v, 1024u64.pow(2))
    } else if let Some(v) = s.strip_suffix("KiB") {
        (v, 1024)
    } else if let Some(v) = s.strip_suffix("GB") {
        (v, 1_000_000_000)
    } else if let Some(v) = s.strip_suffix("MB") {
        (v, 1_000_000)
    } else if let Some(v) = s.strip_suffix("KB") {
        (v, 1_000)
    } else if let Some(v) = s.strip_suffix('B') {
        (v, 1)
    } else {
        (s, 1)
    };
    num.trim()
        .parse::<u64>()
        .ok()
        .filter(|n| *n > 0)
        .map(|n| n * mult)
}

/// `"1d"`, `"12h"`, `"30m"`, `"90s"`, or bare seconds.
pub fn parse_secs(s: &str) -> Option<u64> {
    let s = s.trim();
    let (num, mult) = match s.chars().last()? {
        'd' => (&s[..s.len() - 1], 86_400),
        'h' => (&s[..s.len() - 1], 3_600),
        'm' => (&s[..s.len() - 1], 60),
        's' => (&s[..s.len() - 1], 1),
        _ => (s, 1),
    };
    num.trim()
        .parse::<u64>()
        .ok()
        .filter(|n| *n > 0)
        .map(|n| n * mult)
}

/// A stable, content-addressed id for a targeted-delete predicate (R-1).
/// Identical delete requests hash to the same id, so re-issuing one dedups
/// in the catalog instead of stacking a duplicate tombstone. Deterministic
/// across runs (`DefaultHasher` uses fixed keys), so replay agrees with the
/// original writer.
fn tombstone_id(
    db: &str,
    table: &str,
    tag_equals: &[(String, String)],
    min_ts_ns: Option<i64>,
    max_ts_ns: Option<i64>,
) -> String {
    use std::hash::{Hash, Hasher};
    // Canonicalize tag order so {a,b} and {b,a} address the same delete.
    let mut tags = tag_equals.to_vec();
    tags.sort();
    let mut h = std::collections::hash_map::DefaultHasher::new();
    db.hash(&mut h);
    table.hash(&mut h);
    tags.hash(&mut h);
    min_ts_ns.hash(&mut h);
    max_ts_ns.hash(&mut h);
    format!("del-{:016x}", h.finish())
}

/// Whether a tombstone's time window overlaps a file's span. A tag-only
/// tombstone (no bounds) overlaps everything; a bounded one uses
/// closed-interval overlap. Only a read-pruning heuristic for R-1b — a
/// false positive merely reads a file that then drops no rows.
fn tombstone_overlaps_file(t: &timelake_catalog::Tombstone, f: &FileMeta) -> bool {
    let lo = t.min_ts_ns.unwrap_or(i64::MIN);
    let hi = t.max_ts_ns.unwrap_or(i64::MAX);
    lo <= f.max_ts_ns && hi >= f.min_ts_ns
}

/// Escape a Prometheus label value: backslash, double quote and newline,
/// per the exposition format.
///
/// Table and database names reach `/metrics` straight from line protocol,
/// so they are caller-controlled text. Unescaped, a measurement containing
/// a quote would not merely look wrong — it would close the label early
/// and let the rest of the name be parsed as further labels or a value,
/// corrupting neighbouring series in whatever scrapes it.
fn escape_label(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

/// Writes refused, split by cause (U2, `docs/CONSOLE.md` §7.4).
///
/// The split is the point. A single `write_rejected_total` cannot tell an
/// operator whether the server is shedding load — the WAL cap of RR-5,
/// which is theirs to fix — or a client is sending malformed line
/// protocol, which is not. Those demand opposite responses, so they are
/// counted apart.
#[derive(Debug, Default)]
pub struct WriteRejects {
    /// RR-5: the WAL cap refused the write. This is backpressure working.
    pub backpressure: AtomicU64,
    /// Malformed body, bad precision, non-UTF-8 — the client's fault.
    pub bad_request: AtomicU64,
    /// CL-3: a querier holds no write path.
    pub not_here: AtomicU64,
    /// WAL append or apply failed — a real fault.
    pub internal: AtomicU64,
}

impl WriteRejects {
    fn render(&self) -> String {
        format!(
            "# HELP timelake_write_rejected_total Writes refused, by cause.\n\
             # TYPE timelake_write_rejected_total counter\n\
             timelake_write_rejected_total{{reason=\"backpressure\"}} {}\n\
             timelake_write_rejected_total{{reason=\"bad_request\"}} {}\n\
             timelake_write_rejected_total{{reason=\"not_here\"}} {}\n\
             timelake_write_rejected_total{{reason=\"internal\"}} {}\n",
            self.backpressure.load(Ordering::Relaxed),
            self.bad_request.load(Ordering::Relaxed),
            self.not_here.load(Ordering::Relaxed),
            self.internal.load(Ordering::Relaxed),
        )
    }
}

pub struct Engine {
    dbs: RwLock<HashMap<String, HashMap<String, TableBuffer>>>,
    /// Writers hold this shared for append+apply; flush holds it
    /// exclusively for the swap+rotate instant, so no write can land in a
    /// sealed WAL generation but miss the swapped buffers.
    ingest_gate: RwLock<()>,
    wal: Mutex<Wal>,
    store: Arc<dyn Store>,
    catalog: Catalog<Arc<dyn Store>>,
    /// SEC-1: true when the store is the encrypting decorator (drives the
    /// timelake_encryption_enabled gauge; the engine itself cannot tell).
    store_encrypted: bool,
    cfg: EngineConfig,
    /// Shared pool + admission + timeout (RR-1/RR-2) — ONE for all queries.
    query_env: timelake_query::QueryEnv,
    /// SEC-6: per-client concurrency cap in front of the admission
    /// semaphore, so one client cannot hold the whole query budget.
    client_limiter: Arc<ClientLimiter>,
    /// SEC-8: the WAL's encryption key (the object store's envelope key),
    /// used to open the local and replica WALs. `None` = plaintext WAL.
    wal_cipher: Option<Arc<dyn WalCipher>>,
    /// Full column set per (db, table): survives flushes and restarts so
    /// providers can present a stable merged schema without re-reading
    /// every footer per query.
    schemas: RwLock<HashMap<(String, String), timelake_query::QuerySchema>>,
    /// Which file each registry entry was last folded from, so a catalog
    /// tail re-reads a footer only when the newest file for a table has
    /// actually changed (CL-3 runs this on every manifest advance).
    schema_source: RwLock<HashMap<(String, String), String>>,
    /// SEC-4 principals and sessions for the admin surface.
    auth: Arc<timelake_auth::Auth>,
    /// P1-2 (SR-6): the hash-chained audit trail every admin mutation writes
    /// to. Shared with the admin router; fail-closed unless the operator
    /// sets `TIMELAKE_AUDIT_FAIL_OPEN`.
    audit: Arc<timelake_audit::AuditLog>,
    /// The data-plane split: how many requests authenticated, came
    /// anonymous, or were rejected. The measurement an operator flips
    /// `optional` → `required` on — same lesson as the mTLS counters.
    data_auth_counts: timelake_auth::DataAuthCounts,
    /// FR-7 policies, runtime-mutable (the /admin/retention surface).
    /// Seeded from `TIMELAKE_RETENTION`; changes persist to
    /// [`RETENTION_CONFIG_PATH`] through the store — encrypted like any
    /// object, shared via S3 in the cluster era, and the store copy wins
    /// over the env seed at boot. Plain put (last-writer-wins): admin
    /// config, not data — CAS arrives with the C1 catalog work if
    /// concurrent admins ever matter.
    retention: RwLock<Vec<RetentionPolicy>>,
    /// R-2 rollup definitions (ARCHITECTURE §18), runtime-mutable via
    /// `/admin/rollups`. Seeded from `TIMELAKE_ROLLUPS`; changes persist to
    /// [`ROLLUPS_CONFIG_PATH`] through the store, the stored copy winning at
    /// boot — the retention pattern exactly. Materialisation reads this on
    /// the maintenance tick.
    rollups: RwLock<Vec<RollupDef>>,
    /// Rows mid-flush: snapshotted at buffer swap-out, dropped after the
    /// catalog commit that makes their files visible. Queries union this
    /// with buffer + files, reading it BEFORE the catalog — acknowledged
    /// rows must never vanish for the duration of an object-store upload
    /// (the C0 S3 drill caught exactly that: first flush of a table took
    /// long enough on S3 that "table not found" hit the benchmark; on
    /// local disk the same window was microseconds wide). The residual
    /// commit→clear window errs toward a transient duplicate, which is
    /// the M2-documented tolerance, never a vanish.
    flushing: RwLock<HashMap<(String, String), timelake_query::QueryBatch>>,
    lines_total: AtomicU64,
    flushes_total: AtomicU64,
    compactions_total: AtomicU64,
    /// Merges discarded because another writer replaced the inputs first.
    /// Expected once more than one node compacts; a steadily climbing
    /// value means several compactors keep choosing the same partition and
    /// the work-avoidance layer above the fence is not doing its job.
    stale_merges: AtomicU64,
    /// U2: process start, for `timelake_uptime_seconds` and as the origin
    /// the two lag stamps below are measured against.
    started: std::time::Instant,
    /// Millis since `started` at the last flush / last compaction. Zero
    /// means "not since boot", and the lag gauge then reports the whole
    /// uptime — which is the honest answer to "how long since the last
    /// one" rather than a reassuring zero.
    last_flush_ms: AtomicU64,
    last_compaction_ms: AtomicU64,
    /// U2: writes refused, split by cause. Backpressure is the one an
    /// operator acts on (SR-3); a bad request is the client's problem.
    write_rejects: WriteRejects,
    /// U2: the self-monitoring buffer, unless `TIMELAKE_SELFMON=off`.
    /// Also registered as the query observer, so it is the same instance
    /// the query path pushes to.
    selfmon: Option<std::sync::Arc<selfmon::SelfMonitor>>,
    retention_drops_total: AtomicU64,
    /// R-1b: files rewritten or dropped by the physical tombstone GC pass.
    tombstone_rewrites_total: AtomicU64,
    file_seq: AtomicU64,
    /// Deferred deletions: (when superseded, path). Drained by run_gc
    /// after gc_grace_secs so in-flight catalog snapshots never dangle.
    pending_gc: Mutex<Vec<(std::time::Instant, String)>>,
    /// Immutable-footer cache: warm queries prune without fetching files.
    meta_cache: Arc<timelake_query::provider::MetaCache>,
    /// SEC-2: rows the mandatory predicate dropped, across all queries.
    visibility_filtered: Arc<AtomicU64>,
    /// §12.2: KMS call/cache counters, when the key cache is active.
    kms_stats: Option<Arc<KmsStats>>,
    /// §12.1: S3 request counters, when the backend is S3.
    s3_stats: Option<Arc<S3Stats>>,
    /// SEC-3: set when the listeners run TLS; feeds the expiry gauge and
    /// renewal-health metric so a failing rotation is visible (RR-5).
    tls: RwLock<Option<Arc<timelake_tls::RotatingCert>>>,
    /// SEC-3 want mode: the rotating client-CA bundle, when configured.
    client_ca: RwLock<Option<Arc<timelake_tls::RotatingClientCa>>>,
    client_auth_counts: RwLock<Option<Arc<timelake_flight::ClientAuthCounts>>>,
    /// CL-2 replication (ingester role only; `None` on a lone `all` node,
    /// which is why that path is byte-for-byte unchanged). The client to
    /// this ingester's peer, set by main from discovery.
    replicator: RwLock<Option<Replicator>>,
    /// The durable copy of the PEER's frames — what recovers the peer's
    /// acknowledged writes if it dies. Distinct from the local `wal`.
    replica_wal: Mutex<Option<Wal>>,
    replica_wal_dir: RwLock<Option<PathBuf>>,
    cl2_replica_frames: AtomicU64,
    cl3_reads_refused: AtomicU64,
    cl2_recovered: AtomicU64,
    /// CL-3: a querier is a read replica. It holds no WAL of its own and
    /// must not pretend to — a write accepted here would be durable
    /// nowhere the cluster reads from.
    read_only: std::sync::atomic::AtomicBool,
    /// CL-3: the ingesters whose live buffers this node unions into every
    /// query. `None` on `all`/`ingester`/`router`, which is why those
    /// paths are unchanged.
    remote: RwLock<Option<Arc<querier::RemoteBuffers>>>,
}

impl Engine {
    /// Open the engine: catalog load + WAL replay happen BEFORE serving
    /// (RR-3 — writes are accepted as soon as this returns).
    ///
    /// SEC-1/CL-1 are decided here and only here: the environment picks
    /// the backend (local or S3) and the key source (local KEK or AWS
    /// KMS, cached), and nothing downstream can tell. A malformed key or
    /// URL refuses to start — silently running plaintext (or the wrong
    /// bucket) would be the worst possible reading of "configured".
    pub fn open(data_dir: &Path, cfg: EngineConfig) -> std::io::Result<Arc<Engine>> {
        let stack = store_stack_from_env(data_dir)?;
        tracing::info!(
            backend = stack.backend,
            encrypted = stack.encrypted,
            kms_cached = stack.kms_stats.is_some(),
            "object store stack"
        );
        Self::open_with_stack(data_dir, cfg, stack)
    }

    /// Open over an explicit store (tests wire decorators directly;
    /// `open` is the env-driven front door).
    pub fn open_with_store(
        data_dir: &Path,
        cfg: EngineConfig,
        store: Arc<dyn Store>,
        store_encrypted: bool,
    ) -> std::io::Result<Arc<Engine>> {
        Self::open_with_stack(data_dir, cfg, StoreStack::plain(store, store_encrypted))
    }

    pub fn open_with_stack(
        data_dir: &Path,
        cfg: EngineConfig,
        stack: StoreStack,
    ) -> std::io::Result<Arc<Engine>> {
        let StoreStack {
            store,
            encrypted: store_encrypted,
            backend: _,
            kms_stats,
            s3_stats,
            kms,
        } = stack;
        let catalog = Catalog::load(store.clone())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        // SEC-8: the WAL encrypts with the SAME envelope key as the object
        // store, so turning on at-rest encryption covers the WAL too.
        let wal_cipher: Option<Arc<dyn WalCipher>> =
            kms.map(|k| Arc::new(WalKms(k)) as Arc<dyn WalCipher>);
        let (wal, frames) = Wal::open(&data_dir.join("wal"), wal_cipher.clone())?;

        // FR-7 runtime policies: the store copy (written by
        // /admin/retention) outranks the env seed — an operator's
        // durable change must survive a restart with a stale env.
        let retention: Vec<RetentionPolicy> = match store.get(RETENTION_CONFIG_PATH) {
            Ok(bytes) => {
                let (policies, migrated) = load_retention_doc(&bytes)?;
                if migrated {
                    // Loud on purpose. The policies still do exactly what
                    // they did, but an operator should learn today that
                    // they reach every database, rather than the next time
                    // a table name collides across two of them.
                    tracing::warn!(
                        policies = policies.len(),
                        "migrated retention config to v2: every existing policy now reads as \
                         db=\"*\" (all databases), which is what it already did. Narrow them \
                         with PUT /admin/retention if that is not what you meant"
                    );
                }
                policies
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => cfg.retention.clone(),
            Err(e) => return Err(e),
        };

        // R-2 rollups: same seed rule as retention — stored copy outranks
        // the env, absent file falls back to TIMELAKE_ROLLUPS (§18.2).
        let rollups: Vec<RollupDef> = match store.get(ROLLUPS_CONFIG_PATH) {
            Ok(bytes) => load_rollups_doc(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => parse_rollups_env(),
            Err(e) => return Err(e),
        };

        // Node identity, read once: it stamps the audit chain (§5.2) and
        // tags every self-monitoring row, so a cluster's samples aggregate.
        // Through the cluster crate's reader, so the name here and the name
        // discovery advertises to peers cannot differ — they did, when this
        // line carried its own fallback (#40).
        let node_id = timelake_cluster::node_id_from_env();
        // U2 self-monitoring. Built before the query environment because
        // the observer has to be attached at construction — the sampler is
        // a plain buffer with no engine reference, so there is no cycle.
        // `TIMELAKE_SELFMON=off` opts out entirely and leaves `/metrics`
        // as the only surface.
        let selfmon = match std::env::var("TIMELAKE_SELFMON").as_deref() {
            Ok("off") | Ok("false") | Ok("0") => None,
            _ => Some(std::sync::Arc::new(selfmon::SelfMonitor::new(
                node_id.clone(),
            ))),
        };
        let query_env = timelake_query::QueryEnv::new(
            cfg.query_mem_bytes,
            cfg.max_concurrent_queries,
            cfg.query_timeout_secs,
        );
        let query_env = match &selfmon {
            Some(m) => query_env.with_observer(m.clone()),
            None => query_env,
        };
        let client_limiter = ClientLimiter::new(cfg.max_concurrent_queries_per_client);
        // SEC-4 principals live in the store like everything else, so
        // they are encrypted with it and travel with a cluster's bucket.
        let auth = timelake_auth::Auth::open(
            store.clone(),
            std::env::var("TIMELAKE_ADMIN_BOOTSTRAP_PASSWORD")
                .ok()
                .as_deref(),
        )?;
        // P1-2 (SR-6): the audit trail is local, fsync-per-record, and lives
        // beside the WAL. A per-node chain, so the node id is stamped on
        // every record. Fail-closed by default (§5.5) — an operator who
        // wants mutations to proceed when the sink is broken sets fail-open.
        let audit_fail_open = std::env::var("TIMELAKE_AUDIT_FAIL_OPEN")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        // Rotation and retention for the trail. Splitting is on by default
        // because one unbounded audit file cannot be archived or read
        // selectively; DELETING is off by default and floored at 90 days
        // even when set, because these records are evidence.
        let audit_policy = timelake_audit::Policy {
            rotate_bytes: std::env::var("TIMELAKE_AUDIT_ROTATE_SIZE")
                .ok()
                .and_then(|v| parse_size_bytes(&v))
                .or(timelake_audit::Policy::default().rotate_bytes),
            rotate_after: std::env::var("TIMELAKE_AUDIT_ROTATE_EVERY")
                .ok()
                .and_then(|v| parse_secs(&v))
                .map(std::time::Duration::from_secs),
            retain_days: std::env::var("TIMELAKE_AUDIT_RETAIN_DAYS")
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok()),
        };
        let audit = Arc::new(timelake_audit::AuditLog::open_with(
            data_dir.join("audit"),
            node_id,
            audit_fail_open,
            audit_policy,
        )?);
        let engine = Engine {
            dbs: RwLock::new(HashMap::new()),
            ingest_gate: RwLock::new(()),
            data_auth_counts: timelake_auth::DataAuthCounts::default(),
            wal: Mutex::new(wal),
            store,
            catalog,
            store_encrypted,
            query_env,
            client_limiter,
            wal_cipher,
            schemas: RwLock::new(HashMap::new()),
            schema_source: RwLock::new(HashMap::new()),
            auth,
            audit,
            retention: RwLock::new(retention),
            rollups: RwLock::new(rollups),
            flushing: RwLock::new(HashMap::new()),
            cfg,
            lines_total: AtomicU64::new(0),
            flushes_total: AtomicU64::new(0),
            compactions_total: AtomicU64::new(0),
            stale_merges: AtomicU64::new(0),
            started: std::time::Instant::now(),
            last_flush_ms: AtomicU64::new(0),
            last_compaction_ms: AtomicU64::new(0),
            write_rejects: WriteRejects::default(),
            selfmon,
            retention_drops_total: AtomicU64::new(0),
            tombstone_rewrites_total: AtomicU64::new(0),
            file_seq: AtomicU64::new(0),
            pending_gc: Mutex::new(Vec::new()),
            meta_cache: Arc::new(Default::default()),
            visibility_filtered: Arc::new(AtomicU64::new(0)),
            kms_stats,
            s3_stats,
            tls: RwLock::new(None),
            client_ca: RwLock::new(None),
            client_auth_counts: RwLock::new(None),
            replicator: RwLock::new(None),
            replica_wal: Mutex::new(None),
            replica_wal_dir: RwLock::new(None),
            cl2_replica_frames: AtomicU64::new(0),
            cl3_reads_refused: AtomicU64::new(0),
            cl2_recovered: AtomicU64::new(0),
            read_only: std::sync::atomic::AtomicBool::new(false),
            remote: RwLock::new(None),
        };
        let n = frames.len();
        for (db, mult, body) in frames {
            let body = String::from_utf8_lossy(&body);
            if let Err(e) = engine.apply(&db, &body, mult, 0) {
                tracing::warn!(db, error = %e, "skipping unreplayable WAL frame");
            }
        }
        engine.refresh_schema_registry();
        tracing::info!(
            frames = n,
            files = engine.catalog.file_count(),
            "recovery complete (WAL replay + catalog load)"
        );
        Ok(Arc::new(engine))
    }

    fn apply(&self, db: &str, body: &str, mult: i64, default_ts_ns: i64) -> Result<usize, String> {
        let rows = parse_lines(body, mult, default_ts_ns).map_err(|e| e.to_string())?;
        let n = rows.len();
        let touched: Vec<(String, usize)> = {
            let mut dbs = self.dbs.write().expect("dbs lock");
            let tables = dbs.entry(db.to_string()).or_default();
            for row in &rows {
                tables.entry(row.table.clone()).or_default().append(row)?;
            }
            let mut touched: Vec<String> = rows.iter().map(|r| r.table.clone()).collect();
            touched.sort();
            touched.dedup();
            touched
                .into_iter()
                .map(|t| {
                    let cols = tables[&t].schema_only().fields().len();
                    (t, cols)
                })
                .collect()
        };
        // registry upkeep only when a table's column set could have grown
        for (table, cols) in touched {
            let key = (db.to_string(), table.clone());
            let known = self
                .schemas
                .read()
                .expect("schemas lock")
                .get(&key)
                .map(|s| s.fields().len())
                .unwrap_or(0);
            if cols > known {
                let schema = {
                    let dbs = self.dbs.read().expect("dbs lock");
                    dbs[db][&table].schema_only()
                };
                let mut reg = self.schemas.write().expect("schemas lock");
                let merged = timelake_query::schema_union(reg.get(&key).cloned(), schema)?;
                reg.insert(key, merged);
            }
        }
        // U2: self-monitoring rows are not user ingest. Counting them here
        // would inflate `timelake_lines_written_total` by a steady
        // background rate, and that counter is what Gauge's throughput
        // numbers are compared against — a benchmark whose baseline drifts
        // because the server is watching itself is worse than no metric.
        if db != selfmon::SELFMON_DB {
            self.lines_total.fetch_add(n as u64, Ordering::Relaxed);
        }
        Ok(n)
    }

    fn now_ns() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
    }

    /// L0 trigger check — called by the background tick and by tests.
    pub fn flush_if_needed(&self) -> Result<usize, String> {
        let need = {
            let dbs = self.dbs.read().expect("dbs lock");
            dbs.values().flat_map(|t| t.values()).any(|b| {
                b.row_count() >= self.cfg.flush_rows
                    || (b.row_count() > 0 && b.age_secs() >= self.cfg.flush_age_secs)
            })
        };
        if need { self.flush_all() } else { Ok(0) }
    }

    /// Flush every non-empty buffer to Parquet. Returns files written.
    pub fn flush_all(&self) -> Result<usize, String> {
        // 1. The handover — buffer out, holding area in — is ONE critical
        // section, and every reader takes the same two locks in the same
        // order (`dbs`, then `flushing`), so a query observes the rows in
        // exactly one of the two places. Never neither, which would report
        // an acknowledged row as missing; never both, which would count it
        // twice.
        //
        // Getting this wrong is not theoretical. Doing the swap and the
        // insert as two separately-locked steps leaves both defects open,
        // in opposite directions depending on where a reader lands, and the
        // slow-store test in `tests/health.rs` reproduces each of them.
        //
        // The cost is that `snapshot()` — building the Arrow batch — now
        // runs under the ingest gate, so writers pause for it rather than
        // only for the pointer swap. That is a few milliseconds once per
        // flush interval, and it buys a guarantee the whole read path is
        // built on. Moving it back out means finding another way to make
        // the two observations one, not simply reordering these locks.
        let (snapshots, failed, sealed_gen) = {
            let _gate = self.ingest_gate.write().expect("ingest gate");
            let mut dbs = self.dbs.write().expect("dbs lock");
            let mut hold = self.flushing.write().expect("flushing lock");

            let mut owned: Vec<(String, String, TableBuffer)> = Vec::new();
            for (db, tables) in dbs.iter_mut() {
                for (table, buf) in tables.iter_mut() {
                    if buf.row_count() > 0 {
                        owned.push((db.clone(), table.clone(), std::mem::take(buf)));
                    }
                }
            }
            if owned.is_empty() {
                return Ok(0);
            }
            let sealed = self
                .wal
                .lock()
                .expect("wal lock")
                .rotate()
                .map_err(|e| format!("wal rotate: {e}"))?;

            // The swapped-out rows land in the holding area before any
            // upload: from here on queries serve them from `flushing` while
            // the (possibly slow, S3-era) object writes proceed.
            let mut snapshots: Vec<(String, String, timelake_query::QueryBatch)> = Vec::new();
            let mut failed: Vec<String> = Vec::new();
            for (db, table, buf) in owned {
                match buf.snapshot() {
                    Ok(batch) => {
                        hold.insert((db.clone(), table.clone()), batch.clone());
                        snapshots.push((db, table, batch));
                    }
                    Err(err) => {
                        tracing::error!(%db, %table, %err, "flush snapshot failed; others continue");
                        failed.push(format!("{db}.{table}"));
                    }
                }
            }
            (snapshots, failed, sealed)
        };
        let mut failed = failed;

        // 2. encode + upload through the Store chokepoint. One table's
        // failure must not discard the others' rows, so each is encoded
        // independently and a failure is recorded rather than propagated.
        let mut metas = Vec::new();
        let mut done: Vec<(String, String)> = Vec::new();
        for (db, table, batch) in &snapshots {
            match self.flush_one(db, table, batch) {
                Ok(mut m) => {
                    metas.append(&mut m);
                    done.push((db.clone(), table.clone()));
                }
                Err(err) => {
                    tracing::error!(%db, %table, %err, "flush failed for this table; others continue");
                    failed.push(format!("{db}.{table}"));
                }
            }
        }

        // 3. durable manifest commit, then 4. reclaim sealed WAL gens
        let n = metas.len();
        self.catalog
            .commit_add(metas)
            .map_err(|e| format!("catalog commit: {e}"))?;
        // committed tables leave holding — their files are visible now.
        // FAILED tables stay: their rows keep serving from the snapshot
        // (strictly better than the old swap-and-hope; the retained WAL
        // still replays them at restart).
        {
            let mut hold = self.flushing.write().expect("flushing lock");
            for key in &done {
                hold.remove(key);
            }
        }
        if failed.is_empty() {
            self.wal
                .lock()
                .expect("wal lock")
                .delete_generations_upto(sealed_gen)
                .map_err(|e| format!("wal reclaim: {e}"))?;
        } else {
            // The failed tables' rows are still in the sealed generation.
            // Keeping it means a restart replays them — safe, because
            // replaying already-flushed rows dedups last-write-wins.
            tracing::warn!(
                tables = ?failed,
                "WAL generations retained: some tables did not flush"
            );
        }
        self.flushes_total.fetch_add(1, Ordering::Relaxed);
        self.stamp_flush();
        tracing::info!(files = n, "flush complete");
        if failed.is_empty() {
            Ok(n)
        } else {
            Err(format!("flush incomplete for {}", failed.join(", ")))
        }
    }

    /// Encode and upload one table's snapshot. Split out of
    /// [`Self::flush_all`] so a single bad batch is contained.
    fn flush_one(
        &self,
        db: &str,
        table: &str,
        batch: &timelake_query::QueryBatch,
    ) -> Result<Vec<FileMeta>, String> {
        // keep the registry current: flushed columns must remain
        // queryable after the buffer empties (and after restart)
        {
            let key = (db.to_string(), table.to_string());
            let mut reg = self.schemas.write().expect("schemas lock");
            let merged = timelake_query::schema_union(reg.get(&key).cloned(), batch.schema())?;
            reg.insert(key, merged);
        }
        let mut metas = Vec::new();
        for (partition, part) in flush::prepare(batch)? {
            let (min_ts, max_ts) = flush::time_bounds(&part);
            let bytes = flush::to_parquet_bytes(&part)?;
            let seq = self.file_seq.fetch_add(1, Ordering::Relaxed);
            let path = format!(
                "{db}/{table}/data/{partition}/{:020}-{seq:06}.parquet",
                max_ts
            );
            self.store
                .put(&path, &bytes)
                .map_err(|e| format!("store put {path}: {e}"))?;
            metas.push(FileMeta {
                db: db.to_string(),
                table: table.to_string(),
                partition,
                path,
                rows: part.num_rows() as u64,
                size_bytes: bytes.len() as u64,
                min_ts_ns: min_ts,
                max_ts_ns: max_ts,
            });
        }
        Ok(metas)
    }

    /// L0→L1: merge every (db, table, hour) group holding at least
    /// `compact_min_files` files into one (PR-6; completes FR-5 across
    /// files). Bounded work per call; returns partitions compacted.
    pub fn compact_once(&self) -> Result<usize, String> {
        const MAX_GROUPS_PER_TICK: usize = 8;

        let mut groups: HashMap<(String, String, String), Vec<FileMeta>> = HashMap::new();
        for f in self.catalog.all_files() {
            groups
                .entry((f.db.clone(), f.table.clone(), f.partition.clone()))
                .or_default()
                .push(f);
        }
        // Two triggers now, not one. Count is read amplification, as it
        // always was; overlap is CORRECTNESS -- duplicate primary keys
        // collapse only in a compaction's cross-file last-write-wins
        // pass, so twins in a partition that never reaches
        // `compact_min_files` are served twice, forever
        // (FINDING_rebalance_duplicates_replayed_writes.md). See
        // `compaction` for why the overlap test is strict.
        let mut todo: Vec<(compaction::Trigger, Vec<FileMeta>)> = groups
            .into_values()
            .filter_map(|v| compaction::trigger_for(&v, self.cfg.compact_min_files).map(|t| (t, v)))
            .collect();
        // Overlap first, then most files. A partition with duplicates is
        // serving wrong answers right now; one with four files is merely
        // slower, and MAX_GROUPS_PER_TICK means the tail of this list may
        // wait for the next pass.
        todo.sort_by_key(|(trigger, v)| {
            (
                *trigger != compaction::Trigger::Overlap,
                std::cmp::Reverse(v.len()),
            )
        });
        todo.truncate(MAX_GROUPS_PER_TICK);

        let mut done = 0;
        for (trigger, mut files) in todo {
            // oldest first so merge's last-write-wins favors newer files;
            // file names embed max_ts+seq, so path order is write order
            files.sort_by(|a, b| a.path.cmp(&b.path));
            let mut batch_sets = Vec::with_capacity(files.len());
            for f in &files {
                let bytes = self
                    .store
                    .get(&f.path)
                    .map_err(|e| format!("store get {}: {e}", f.path))?;
                batch_sets.push(flush::read_parquet_bytes(bytes)?);
            }
            let merged = timelake_compact::merge_files(batch_sets)?;
            let f0 = &files[0];
            let seq = self.file_seq.fetch_add(1, Ordering::Relaxed);
            let path = format!(
                "{}/{}/data/{}/c{:020}-{seq:06}.parquet",
                f0.db, f0.table, f0.partition, merged.max_ts_ns
            );
            let written_path = path.clone();
            self.store
                .put(&path, &merged.bytes)
                .map_err(|e| format!("store put {path}: {e}"))?;
            let remove: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
            // commit_replace, not commit: the merge is only valid if its
            // inputs are still there. Another compactor replacing them
            // first is a normal outcome once more than one node can
            // compact, not an error -- see the `stale` arm below.
            let committed = self.catalog.commit_replace(
                vec![FileMeta {
                    db: f0.db.clone(),
                    table: f0.table.clone(),
                    partition: f0.partition.clone(),
                    path,
                    rows: merged.rows,
                    size_bytes: merged.bytes.len() as u64,
                    min_ts_ns: merged.min_ts_ns,
                    max_ts_ns: merged.max_ts_ns,
                }],
                remove.clone(),
            );
            if let Err(e) = &committed
                && e.kind() == std::io::ErrorKind::AlreadyExists
            {
                // Someone else compacted this partition while we were
                // merging it. Our output is redundant, so drop it and move
                // on. Deliberately not an error and deliberately not
                // counted as a compaction: nothing is wrong, and a metric
                // that climbed here would make ordinary concurrency look
                // like a fault.
                tracing::debug!(
                    db = %f0.db, table = %f0.table, partition = %f0.partition,
                    "skipped a stale merge: inputs already replaced"
                );
                self.stale_merges.fetch_add(1, Ordering::Relaxed);
                // The merged blob we just wrote is unreferenced. Hand it to
                // the same deferred GC that cleans superseded files, or it
                // leaks in the store forever.
                self.defer_gc(vec![written_path.clone()]);
                continue;
            }
            committed.map_err(|e| format!("catalog commit: {e}"))?;
            // deletion is DEFERRED (gc_grace): in-flight queries hold
            // catalog snapshots referencing the old paths (AT-3 race)
            self.defer_gc(remove);
            // Per partition, at debug: which trigger fired is the first
            // thing anyone investigating a duplicate count will want, and
            // reconstructing it afterwards from file counts is guesswork.
            tracing::debug!(
                db = %f0.db, table = %f0.table, partition = %f0.partition,
                trigger = trigger.as_str(), files = files.len(),
                "compacted partition"
            );
            done += 1;
        }
        if done > 0 {
            self.compactions_total
                .fetch_add(done as u64, Ordering::Relaxed);
            self.stamp_compaction();
            tracing::info!(partitions = done, "compaction pass");
        }
        Ok(done)
    }

    /// FR-7: drop whole partitions past their table's retention window.
    pub fn enforce_retention(&self) -> Result<usize, String> {
        let policies = self.retention.read().expect("retention lock").clone();
        if policies.is_empty() {
            return Ok(0);
        }
        let now = Self::now_ns();
        let mut remove: Vec<String> = Vec::new();
        for f in self.catalog.all_files() {
            // Matched on (db, table), not on table alone. Before 2026-08-19
            // this ignored `f.db` entirely, so a policy set on one
            // database's table silently deleted every same-named table on
            // the node — including `_system`, which is why self-monitoring
            // shipped with no default retention at all.
            //
            // Most specific wins: an exact-database policy overrides a
            // wildcard for the same table, so an operator can carve one
            // database out of a broad rule instead of having to choose
            // between all and nothing.
            let policy = policies
                .iter()
                .find(|p| p.db == f.db && p.table == f.table)
                .or_else(|| policies.iter().find(|p| p.covers(&f.db, &f.table)));
            if let Some(p) = policy {
                let cutoff = flush::hour_partition(now - (p.seconds as i64) * 1_000_000_000);
                // partition strings sort chronologically; a partition
                // strictly before the cutoff hour is wholly expired
                if f.partition.as_str() < cutoff.as_str() {
                    remove.push(f.path.clone());
                }
            }
        }
        let n = remove.len();
        if n > 0 {
            self.catalog
                .commit(Vec::new(), remove.clone())
                .map_err(|e| format!("catalog commit: {e}"))?;
            self.defer_gc(remove);
            self.retention_drops_total
                .fetch_add(n as u64, Ordering::Relaxed);
            tracing::info!(files = n, "retention drop");
        }
        Ok(n)
    }

    /// R-1b physical delete: rewrite catalogued files so rows matching an
    /// active tombstone are gone from the bytes on disk, not merely hidden at
    /// read time (R-1a). Runs as maintenance because a tombstone is a
    /// standing filter: data written after a delete is reclaimed on a later
    /// pass, and until then R-1a keeps it invisible, so a query never depends
    /// on this having run. Deferred GC (grace window) covers in-flight
    /// queries still holding the old paths, exactly as compaction does.
    pub fn apply_tombstones_once(&self) -> Result<usize, String> {
        let tombstones = self.catalog.tombstones();
        if tombstones.is_empty() {
            return Ok(0);
        }
        // Bounded per tick so a large backlog cannot monopolize maintenance.
        const MAX_FILES_PER_TICK: usize = 16;
        let mut rewritten = 0usize;
        for f in self.catalog.all_files() {
            if rewritten >= MAX_FILES_PER_TICK {
                break;
            }
            // Only the tombstones that could touch THIS file: same table, and
            // a time window that overlaps the file's span.
            let deletes: Vec<timelake_catalog::Tombstone> = tombstones
                .iter()
                .filter(|t| t.db == f.db && t.table == f.table && tombstone_overlaps_file(t, &f))
                .cloned()
                .collect();
            if deletes.is_empty() {
                continue;
            }

            let bytes = self
                .store
                .get(&f.path)
                .map_err(|e| format!("store get {}: {e}", f.path))?;
            let batches = flush::read_parquet_bytes(bytes)?;
            // Reuse the exact read-time filter (SEC-2 hook): the bytes that
            // survive here are precisely the rows a query would still return.
            let restriction = timelake_query::Restriction::Tombstone { deletes };
            let mut kept = Vec::with_capacity(batches.len());
            let mut before = 0usize;
            let mut after = 0usize;
            for b in batches {
                before += b.num_rows();
                let fb = timelake_query::apply_restriction(&restriction, &b)?;
                after += fb.num_rows();
                if fb.num_rows() > 0 {
                    kept.push(fb);
                }
            }
            if after == before {
                // Overlapped the time span but matched no row here (e.g. the
                // tag value isn't in this file) — nothing to reclaim, and a
                // rewrite would only churn the store.
                continue;
            }

            if after == 0 {
                // Every row is gone: drop the file outright.
                self.catalog
                    .commit(Vec::new(), vec![f.path.clone()])
                    .map_err(|e| format!("catalog commit: {e}"))?;
            } else {
                // Some rows survive: write them to a fresh file and swap it in
                // atomically (add new + remove old in one manifest entry).
                let merged = timelake_compact::merge_files(vec![kept])?;
                let seq = self.file_seq.fetch_add(1, Ordering::Relaxed);
                let path = format!(
                    "{}/{}/data/{}/t{:020}-{seq:06}.parquet",
                    f.db, f.table, f.partition, merged.max_ts_ns
                );
                self.store
                    .put(&path, &merged.bytes)
                    .map_err(|e| format!("store put {path}: {e}"))?;
                self.catalog
                    .commit(
                        vec![FileMeta {
                            db: f.db.clone(),
                            table: f.table.clone(),
                            partition: f.partition.clone(),
                            path,
                            rows: merged.rows,
                            size_bytes: merged.bytes.len() as u64,
                            min_ts_ns: merged.min_ts_ns,
                            max_ts_ns: merged.max_ts_ns,
                        }],
                        vec![f.path.clone()],
                    )
                    .map_err(|e| format!("catalog commit: {e}"))?;
            }
            // Old bytes leave on the grace timer, never synchronously: an
            // in-flight query may still be reading the path we just removed.
            self.defer_gc(vec![f.path.clone()]);
            rewritten += 1;
        }
        if rewritten > 0 {
            self.tombstone_rewrites_total
                .fetch_add(rewritten as u64, Ordering::Relaxed);
            tracing::info!(files = rewritten, "tombstone physical GC pass");
        }
        Ok(rewritten)
    }

    /// SEC-4 principal/session store, shared with the admin router.
    pub fn auth(&self) -> Arc<timelake_auth::Auth> {
        self.auth.clone()
    }

    /// Pick up tokens issued or revoked on another node sharing this store
    /// (#46). Called from the maintenance tick on ingesters and `all`
    /// nodes, and from the querier's tail loop, which is the only periodic
    /// thing a querier runs. A store error is logged, not propagated: the
    /// token set in memory stays what it was, which is the safe direction.
    pub fn reload_tokens(&self) {
        if let Err(e) = self.auth.reload_tokens() {
            tracing::warn!(error = %e, stage = "tokens", "token reload failed; keeping the loaded set");
        }
    }

    /// P1-2 audit trail, shared with the admin router so every mutating
    /// handler records through the same chain.
    pub fn audit_log(&self) -> Arc<timelake_audit::AuditLog> {
        self.audit.clone()
    }

    /// The largest HTTP body this node accepts, on either listener.
    /// Read by both routers so they cannot disagree (FR-1).
    pub fn max_body_bytes(&self) -> usize {
        self.cfg.max_body_bytes
    }

    /// Concurrency ceiling for intra-cluster live/snapshot reads (CL-3).
    pub fn internal_max_concurrent(&self) -> usize {
        self.cfg.internal_max_concurrent
    }

    /// True where the intra-cluster listener runs: the replica WAL is
    /// enabled on exactly those nodes (ingesters). Keeps a lone `all`
    /// node's /metrics unchanged, as the CL-2 lines already do.
    pub fn serves_internal_listener(&self) -> bool {
        self.replica_wal.lock().expect("replica wal lock").is_some()
    }

    /// Live/snapshot reads refused because the ceiling was reached.
    pub fn cl3_reads_refused(&self) -> u64 {
        self.cl3_reads_refused.load(Ordering::Relaxed)
    }

    pub fn note_cl3_read_refused(&self) {
        self.cl3_reads_refused.fetch_add(1, Ordering::Relaxed);
    }

    /// Runtime FR-7 policy surface (backs /admin/retention).
    pub fn retention_policies(&self) -> Vec<RetentionPolicy> {
        self.retention.read().expect("retention lock").clone()
    }

    pub fn set_retention(&self, db: &str, table: &str, seconds: u64) -> Result<(), String> {
        let mut r = self.retention.write().expect("retention lock");
        // Keyed on (db, table): setting `poc.metrics` must not overwrite a
        // separate `_system.metrics` policy, which is the same conflation
        // that made this a hazard in the first place.
        match r.iter_mut().find(|p| p.db == db && p.table == table) {
            Some(entry) => entry.seconds = seconds,
            None => r.push(RetentionPolicy {
                db: db.to_string(),
                table: table.to_string(),
                seconds,
            }),
        }
        // persisted under the write lock so concurrent admin edits
        // cannot land in the store out of order
        Self::persist_retention(&self.store, &r)
    }

    pub fn remove_retention(&self, db: &str, table: &str) -> Result<(), String> {
        let mut r = self.retention.write().expect("retention lock");
        r.retain(|p| !(p.db == db && p.table == table));
        Self::persist_retention(&self.store, &r)
    }

    /// R-2 rollup surface (ARCHITECTURE §18), the retention pattern reused.
    pub fn rollups(&self) -> Vec<RollupDef> {
        self.rollups.read().expect("rollups lock").clone()
    }

    /// Upsert a rollup by `(db, name)`. Validates structurally and enforces
    /// the retention invariant (§18.4) — a rejected definition returns Err
    /// and the admin surface maps it to 400.
    pub fn set_rollup(&self, def: RollupDef) -> Result<(), String> {
        def.validate()?;
        // The source's retention must outlast the rollup's lookback, or the
        // oldest buckets under-count as retention drops the source out from
        // under a materialisation pass. Most-specific policy wins, matching
        // enforcement; a source with no policy (kept forever) always passes.
        let source_ret = {
            let policies = self.retention.read().expect("retention lock");
            policies
                .iter()
                .find(|p| p.db == def.db && p.table == def.source)
                .map(|p| p.seconds)
                .or_else(|| {
                    policies
                        .iter()
                        .find(|p| p.is_wildcard() && p.table == def.source)
                        .map(|p| p.seconds)
                })
        };
        if let Some(secs) = source_ret
            && secs <= def.lookback_secs
        {
            return Err(format!(
                "source {}.{} keeps {secs}s but the rollup lookback is {}s; the lookback must be \
                 shorter than the source's retention, or the oldest buckets under-count as \
                 retention drops the source (ARCHITECTURE §18.4)",
                def.db, def.source, def.lookback_secs
            ));
        }
        let mut r = self.rollups.write().expect("rollups lock");
        match r.iter_mut().find(|x| x.db == def.db && x.name == def.name) {
            Some(entry) => *entry = def,
            None => r.push(def),
        }
        Self::persist_rollups(&self.store, &r)
    }

    pub fn remove_rollup(&self, db: &str, name: &str) -> Result<(), String> {
        let mut r = self.rollups.write().expect("rollups lock");
        r.retain(|x| !(x.db == db && x.name == name));
        Self::persist_rollups(&self.store, &r)
    }

    fn persist_rollups(store: &Arc<dyn Store>, rollups: &[RollupDef]) -> Result<(), String> {
        let doc = RollupDoc {
            version: ROLLUPS_FORMAT_VERSION,
            rollups: rollups.to_vec(),
        };
        store
            .put(
                ROLLUPS_CONFIG_PATH,
                &serde_json::to_vec_pretty(&doc).expect("rollups json"),
            )
            .map_err(|e| format!("persist rollups config: {e}"))
    }

    /// R-1 targeted delete: record a durable tombstone predicate that hides
    /// every matching row from every query, immediately and cluster-wide,
    /// before the physical GC (R-1b) reclaims the bytes. Returns the
    /// tombstone id and the manifest seq it committed at.
    ///
    /// Refuses an empty predicate — a delete with neither a tag match nor a
    /// time bound would erase the whole table, which is what retention and a
    /// future DROP are for, not this surface.
    pub fn delete_where(
        &self,
        db: &str,
        table: &str,
        tag_equals: Vec<(String, String)>,
        min_ts_ns: Option<i64>,
        max_ts_ns: Option<i64>,
    ) -> Result<(String, u64), String> {
        // A querier owns no write path; deletes, like writes, go to an
        // ingester or the router that fronts them.
        if self.read_only.load(Ordering::Relaxed) {
            return Err(
                "this node is a querier (TIMELAKE_ROLE=querier) and holds no delete \
                        path — send the delete to the router or an ingester"
                    .into(),
            );
        }
        if tag_equals.is_empty() && min_ts_ns.is_none() && max_ts_ns.is_none() {
            return Err(
                "a targeted delete needs at least one predicate — a tag match or a \
                        time bound. Refusing to delete an entire table from this surface."
                    .into(),
            );
        }
        if let (Some(a), Some(b)) = (min_ts_ns, max_ts_ns)
            && a > b
        {
            return Err(format!("time window is empty: start {a} is after end {b}"));
        }
        // Deleting from a table that was never written is a client mistake
        // worth surfacing rather than silently recording a dead predicate.
        if !self.table_names(db).iter().any(|t| t == table) {
            return Err(format!("table '{table}' does not exist in database '{db}'"));
        }

        // Content-addressed id: the tombstone is a standing predicate, so
        // re-issuing the identical delete is idempotent. The catalog dedups
        // by id on replay regardless; short-circuiting here also spares a
        // redundant manifest entry when a caller retries.
        let id = tombstone_id(db, table, &tag_equals, min_ts_ns, max_ts_ns);
        if let Some(t) = self.catalog.tombstones().into_iter().find(|t| t.id == id) {
            return Ok((id, t.created_seq));
        }
        let tombstone = timelake_catalog::Tombstone {
            id: id.clone(),
            db: db.to_string(),
            table: table.to_string(),
            tag_equals,
            min_ts_ns,
            max_ts_ns,
            created_seq: 0, // stamped with the landing seq inside commit
        };
        let seq = self
            .catalog
            .commit_tombstone(tombstone)
            .map_err(|e| format!("commit tombstone: {e}"))?;
        Ok((id, seq))
    }

    fn persist_retention(
        store: &Arc<dyn Store>,
        policies: &[RetentionPolicy],
    ) -> Result<(), String> {
        let doc = RetentionDoc {
            version: RETENTION_FORMAT_VERSION,
            policies: policies.to_vec(),
        };
        store
            .put(
                RETENTION_CONFIG_PATH,
                &serde_json::to_vec_pretty(&doc).expect("retention json"),
            )
            .map_err(|e| format!("persist retention config: {e}"))
    }

    fn defer_gc(&self, paths: Vec<String>) {
        let now = std::time::Instant::now();
        let mut q = self.pending_gc.lock().expect("gc lock");
        q.extend(paths.into_iter().map(|p| (now, p)));
    }

    /// Physically delete superseded files older than the grace window.
    pub fn run_gc(&self) -> usize {
        let grace = std::time::Duration::from_secs(self.cfg.gc_grace_secs);
        let due: Vec<String> = {
            let mut q = self.pending_gc.lock().expect("gc lock");
            let (ready, keep): (Vec<_>, Vec<_>) =
                q.drain(..).partition(|(t, _)| t.elapsed() >= grace);
            *q = keep;
            ready.into_iter().map(|(_, p)| p).collect()
        };
        let n = due.len();
        for p in due {
            if let Err(e) = self.store.delete(&p) {
                tracing::warn!(path = p, error = %e, "gc delete failed");
            }
        }
        n
    }
}

impl timelake_api::Engine for Engine {
    fn authenticate_data(
        &self,
        authorization: Option<&str>,
        action: timelake_auth::Action,
        db: &str,
    ) -> Result<timelake_auth::Decision, timelake_auth::TokenError> {
        self.authenticate_data_impl(authorization, action, db)
    }

    fn admit_client(&self, key: Option<String>) -> Option<Box<dyn Send>> {
        self.client_limiter
            .admit(key)
            .map(|g| Box::new(g) as Box<dyn Send>)
    }

    fn write_lp(
        &self,
        db: &str,
        body: &[u8],
        precision: Option<&str>,
    ) -> Result<usize, WriteError> {
        // U2: one accounting point around the whole path, so a refusal is
        // counted wherever below it originates. Counting at each
        // `return Err` instead would mean a refusal added later silently
        // misses the metric — the same reason `finish` is the only exit in
        // the query path.
        let result = (|| -> Result<usize, WriteError> {
            // CL-3: a querier takes no writes. Checked before parsing so the
            // answer costs nothing and cannot depend on the body.
            if self.read_only.load(Ordering::Relaxed) {
                return Err(WriteError::NotHere(
                    "this node is a querier (TIMELAKE_ROLE=querier) and holds no write path — \
                 send writes to the router, or to an ingester directly"
                        .into(),
                ));
            }
            let mult = match precision {
                None => 1,
                Some(p) => precision_multiplier(p)
                    .ok_or_else(|| WriteError::BadRequest(format!("bad precision {p:?}")))?,
            };
            let text = std::str::from_utf8(body)
                .map_err(|_| WriteError::BadRequest("body is not utf-8".into()))?;

            // validate before durability: a 400 must not land in the WAL
            parse_lines(text, mult, 0).map_err(|e| WriteError::BadRequest(e.to_string()))?;

            // RR-5: the WAL cap is a named, visible limit
            if self.wal.lock().expect("wal lock").size() > self.cfg.wal_max_bytes {
                return Err(WriteError::Backpressure(format!(
                    "wal exceeds {} bytes; flush in progress — retry",
                    self.cfg.wal_max_bytes
                )));
            }

            let _gate = self.ingest_gate.read().expect("ingest gate");
            self.wal
                .lock()
                .expect("wal lock")
                .append(db, mult, body)
                .map_err(|e| WriteError::Internal(format!("wal append: {e}")))?;
            // CL-2: replicate the frame to the paired ingester BEFORE the ack, so
            // an acknowledged write is durable on two nodes. A lone `all` node
            // has no replicator, so this is a no-op and the path is unchanged.
            // A down peer degrades (availability holds); it never fails the write.
            if let Some(r) = self.replicator.read().expect("replicator lock").as_ref() {
                r.replicate(db, mult, body);
            }
            self.apply(db, text, mult, Self::now_ns())
                .map_err(WriteError::BadRequest)
        })();
        if let Err(e) = &result {
            let counter = match e {
                WriteError::Backpressure(_) => &self.write_rejects.backpressure,
                WriteError::BadRequest(_) => &self.write_rejects.bad_request,
                WriteError::NotHere(_) => &self.write_rejects.not_here,
                WriteError::Internal(_) => &self.write_rejects.internal,
            };
            counter.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn sql(
        &self,
        db: String,
        query: String,
        authorizations: Vec<String>,
        identity: Option<String>,
    ) -> Result<Value, String> {
        // HTTP now carries a verified client-certificate identity, the same
        // as Flight: the acceptor reads it off the TLS connection once and
        // attaches it to every request (`tls_identity`). `sql_batches`
        // intersects that identity's grants with the caller's claims, so a
        // certificate narrows what the session sees and can never widen it.
        // `None` — plaintext, or a caller that presented no certificate —
        // takes exactly the path it always did.
        let batches = self
            .sql_batches(&db, &query, authorizations, identity.as_deref())
            .await?;
        Ok(batches_to_json(&batches))
    }

    fn metrics_text(&self) -> String {
        self.metrics_text_impl()
    }

    fn retention_policies(&self) -> Vec<RetentionPolicy> {
        Engine::retention_policies(self)
    }

    fn set_retention(&self, db: &str, table: &str, seconds: u64) -> Result<(), String> {
        Engine::set_retention(self, db, table, seconds)
    }

    fn remove_retention(&self, db: &str, table: &str) -> Result<(), String> {
        Engine::remove_retention(self, db, table)
    }

    fn rollups(&self) -> Vec<RollupDef> {
        Engine::rollups(self)
    }

    fn set_rollup(&self, def: RollupDef) -> Result<(), String> {
        Engine::set_rollup(self, def)
    }

    fn remove_rollup(&self, db: &str, name: &str) -> Result<(), String> {
        Engine::remove_rollup(self, db, name)
    }

    fn delete_where(
        &self,
        db: &str,
        table: &str,
        tag_equals: Vec<(String, String)>,
        min_ts_ns: Option<i64>,
        max_ts_ns: Option<i64>,
    ) -> Result<(String, u64), String> {
        Engine::delete_where(self, db, table, tag_equals, min_ts_ns, max_ts_ns)
    }
}

impl Engine {
    /// Shared query path: /api/sql renders JSON from these batches and
    /// Flight SQL streams them natively (FR-8). `authorizations` are the
    /// session's visibility authorizations (SEC-2) — claims, not
    /// credentials, until authn exists (see SECURITY.md).
    pub async fn sql_batches(
        &self,
        db: &str,
        query: &str,
        authorizations: Vec<String>,
        identity: Option<&str>,
    ) -> Result<Vec<timelake_query::QueryBatch>, String> {
        let remote = self.remote_buffers();
        if let Some(r) = &remote {
            // CL-3: ask *before listing*, every query, not just on the tail
            // tick. A table written a moment ago exists only in an
            // ingester's memory: it is in no catalog and no local buffer, so
            // a querier working from a one-second-old view would answer
            // "table not found" for it — the freshness claim failing at
            // exactly the moment it matters most (write, then read).
            //
            // The head that comes back is read by the ingester AFTER its
            // buffers, so folding the catalog to it covers anything that has
            // just LEFT a buffer by being flushed. Together the two make the
            // table list complete in both directions before it is taken.
            // In the steady state neither costs a store call.
            let head = r.refresh_live().await;
            let before = self.catalog.head();
            if self.catch_up_catalog(head) > before {
                r.stats
                    .catchups
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let names = self.table_names(db);
        if names.is_empty() {
            return Err(format!(
                "database '{db}' does not exist (write to it first)"
            ));
        }

        // SEC-2 (exposures 7/9): a verified client-certificate identity
        // turns the caller's self-asserted claims into grants — its
        // authorizations are intersected with what that identity is
        // granted, and only ever narrowed. An anonymous caller, or a cert
        // with no grants recorded, passes through unchanged (want mode
        // grants nothing on its own). `resolve` is a no-op unless BOTH an
        // identity and a grant set are present, so this is safe for every
        // caller.
        let mut session = QuerySession::with_authorizations(authorizations);
        if let Some(id) = identity {
            session.identity = Some(id.to_string());
        }
        let granted = identity.and_then(|id| self.auth.cert_grants(id));
        let session = session.resolve(granted.as_deref());

        // PASS 1 — every live row, before any catalog read.
        //
        // Buffer-before-catalog is the rule that keeps an acknowledged row
        // from vanishing mid-flush (see the `flushing` field). It is done
        // for ALL tables up front, not per table, because a querier's live
        // rows arrive over the network: interleaving would let a flush
        // complete between one table's snapshot and another's file list.
        let mut live: Vec<(String, Vec<timelake_query::QueryBatch>)> =
            Vec::with_capacity(names.len());
        let mut watermark = 0u64;
        for name in names {
            // Buffer and holding area under ONE consistent view: both locks
            // held together, in the order `flush_all` takes them. Reading
            // them as two separate observations lets a flush land in
            // between and yield the same rows twice (buffer copy + holding
            // copy) — or, if it lands the other way, neither.
            //
            // Both guards are released before any `.await` below, which the
            // borrow checker also insists on.
            let mut buffer: Vec<timelake_query::QueryBatch> = {
                let dbs = self.dbs.read().expect("dbs lock");
                let hold = self.flushing.read().expect("flushing lock");
                let mut batches = match dbs.get(db).and_then(|t| t.get(&name)) {
                    Some(buf) if buf.row_count() > 0 => vec![buf.snapshot()?],
                    _ => Vec::new(),
                };
                if let Some(held) = hold.get(&(db.to_string(), name.clone())) {
                    batches.push(held.clone());
                }
                batches
            };
            // CL-3: the ingesters' live rows. All or nothing — an
            // unreachable ingester fails the query rather than shortening
            // it (see `querier`).
            if let Some(r) = &remote {
                let (mut remote_batches, head) = r.snapshot(db, &name).await?;
                watermark = watermark.max(head);
                buffer.append(&mut remote_batches);
            }
            live.push((name, buffer));
        }

        // The freshness watermark: fold the manifest log forward to at
        // least the highest head any ingester reported BEFORE the snapshots
        // above were taken. Any batch already gone from a buffer is
        // committed below that head, so it is now visible as a file. Costs
        // nothing when the tail loop has already kept up.
        if remote.is_some() && watermark > self.catalog.head() {
            let reached = self.catch_up_catalog(watermark);
            if let Some(r) = &remote {
                r.stats
                    .catchups
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if reached < watermark {
                    r.stats
                        .refusals
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Err(format!(
                        "refusing to answer from a stale catalog: an ingester has committed \
                         up to manifest {watermark} but this querier could only reach \
                         {reached}. Rows flushed in between would be missing."
                    ));
                }
            }
        }

        // PASS 2 — the cataloged files, and the providers.
        let mut tables: Vec<(String, Arc<dyn timelake_query::DfTableProvider>)> =
            Vec::with_capacity(live.len());
        for (name, buffer) in live {
            let files = self.catalog.files_for(db, &name);
            if buffer.is_empty() && files.is_empty() {
                continue;
            }
            // merged schema: registry (covers files + past flushes) ∪ every
            // live batch. Every one, not just the first: batches now come
            // from several nodes, and two of them can legitimately disagree
            // about which columns exist.
            let mut schema = self.table_schema(db, &name);
            for b in &buffer {
                // SEC-5 (exposure 5): a schema_union conflict names a column
                // and two Arrow types. This is the read path — the error
                // returns straight to /api/sql and Flight — and it happens
                // before run_sql_env, so it must be sanitized at the source
                // or it leaks (#47). The write-path union sites keep their
                // verbatim message on purpose; only this one is caller-facing
                // read output.
                schema = Some(
                    timelake_query::schema_union(schema, b.schema())
                        .map_err(|e| timelake_query::opaque_read_error("schema-union", e))?,
                );
            }
            let Some(schema) = schema else { continue };

            // R-1: the active targeted-delete predicates for THIS table are
            // scoped onto a per-table session, so the mandatory predicate
            // hides deleted rows in the same scan that enforces visibility —
            // never leaking one table's tombstone onto another's rows.
            let provider = timelake_query::provider::LazyTable::new(
                name.clone(),
                schema,
                buffer,
                files,
                self.store.clone(),
                std::time::Duration::from_secs(self.cfg.query_timeout_secs),
                self.query_env.runtime.memory_pool.clone(),
                self.meta_cache.clone(),
                session
                    .clone()
                    .with_tombstones(self.catalog.tombstones_for(db, &name)),
                self.visibility_filtered.clone(),
            );
            tables.push((name, Arc::new(provider)));
        }

        timelake_query::run_sql_env(&self.query_env, &session, db, tables, query).await
    }

    /// Databases that hold data — live buffers plus anything the catalog
    /// knows about. Flight SQL reports each one as a catalog.
    pub fn databases(&self) -> Vec<String> {
        let mut names: Vec<String> = {
            let dbs = self.dbs.read().expect("dbs lock");
            dbs.keys().cloned().collect()
        };
        for db in self.catalog.databases() {
            if !names.contains(&db) {
                names.push(db);
            }
        }
        // CL-3: a database whose first writes are still in an ingester's
        // memory exists nowhere local yet. Without this a querier answers
        // "no such database" for the first ten seconds of its life.
        if let Some(r) = self.remote_buffers() {
            for (db, _) in r.live_tables() {
                if !names.contains(&db) {
                    names.push(db);
                }
            }
        }
        names.sort();
        names
    }

    /// Tables in one database, from the buffer AND the catalog — a table
    /// that has been written but not yet flushed exists only in the former,
    /// one that has been flushed and drained only in the latter.
    pub fn table_names(&self, db: &str) -> Vec<String> {
        let mut names: Vec<String> = {
            let dbs = self.dbs.read().expect("dbs lock");
            dbs.get(db)
                .map(|t| t.keys().cloned().collect())
                .unwrap_or_default()
        };
        for t in self.catalog.tables_for(db) {
            if !names.contains(&t) {
                names.push(t);
            }
        }
        // CL-3: tables that exist only in an ingester's buffer. The list is
        // refreshed by the tail loop, so it can lag by a tick — which costs
        // a listing, never a count: a table that IS named in a query still
        // resolves through the catalog + snapshot path above.
        if let Some(r) = self.remote_buffers() {
            for (d, t) in r.live_tables() {
                if d == db && !names.contains(&t) {
                    names.push(t);
                }
            }
        }
        names.sort();
        names
    }

    /// Merged schema for one table: the registry (files and past flushes)
    /// unioned with whatever the live buffer has added since.
    pub fn table_schema(&self, db: &str, table: &str) -> Option<timelake_query::QuerySchema> {
        let key = (db.to_string(), table.to_string());
        let registered = self
            .schemas
            .read()
            .expect("schemas lock")
            .get(&key)
            .cloned();
        let buffered = {
            let dbs = self.dbs.read().expect("dbs lock");
            dbs.get(db)
                .and_then(|t| t.get(table))
                .filter(|b| b.row_count() > 0)
                .map(|b| b.schema_only())
        };
        match buffered {
            Some(b) => timelake_query::schema_union(registered, b).ok(),
            None => registered,
        }
    }

    /// Attach the TLS rotator so /metrics exports cert health (SEC-3).
    pub fn set_tls(&self, rot: Arc<timelake_tls::RotatingCert>) {
        *self.tls.write().expect("tls lock") = Some(rot);
    }

    /// Attach the client-CA bundle so /metrics can report anchor count
    /// and reload health (SEC-3 want mode).
    pub fn set_client_ca(&self, ca: Arc<timelake_tls::RotatingClientCa>) {
        *self.client_ca.write().expect("client ca lock") = Some(ca);
    }

    /// Attach the connection counters the Flight accept loop increments,
    /// so /metrics can report how many peers actually authenticate.
    pub fn set_client_auth_counts(&self, c: Arc<timelake_flight::ClientAuthCounts>) {
        *self.client_auth_counts.write().expect("auth counts lock") = Some(c);
    }

    // ---- CL-2 ingester replication --------------------------------------

    /// Attach the replication client to the peer ingester (main, from
    /// discovery). Once set, every write replicates before its ack.
    pub fn set_replicator(&self, r: Replicator) {
        *self.replicator.write().expect("replicator lock") = Some(r);
    }

    /// Replication stats for `/metrics`, if this node is an ingester.
    pub fn replication_stats(&self) -> Option<Arc<replication::ReplStats>> {
        self.replicator
            .read()
            .expect("replicator lock")
            .as_ref()
            .map(|r| r.stats())
    }

    /// Open the durable replica WAL — the copy of the peer's frames. Any
    /// frames already present (a restart with an un-recovered peer) are
    /// counted so `/metrics` reflects them; they are applied only on an
    /// explicit `recover_from_replica`, never during normal operation, so
    /// this node does not double-flush its peer's live rows.
    pub fn enable_replica_wal(&self, dir: &Path) -> std::io::Result<()> {
        // SEC-8: the replica WAL holds the peer's acked frames — encrypt it
        // with the same key as the local WAL and the object store.
        let (wal, frames) = Wal::open(dir, self.wal_cipher.clone())?;
        self.cl2_replica_frames
            .store(frames.len() as u64, Ordering::Relaxed);
        *self.replica_wal.lock().expect("replica wal lock") = Some(wal);
        *self.replica_wal_dir.write().expect("replica dir lock") = Some(dir.to_path_buf());
        Ok(())
    }

    /// Durably record one frame from the peer (called by the internal
    /// replication listener). fsync before returning, so the peer's 2xx
    /// means the frame is safe here.
    pub fn replicate_receive(&self, db: &str, mult: i64, body: &[u8]) -> std::io::Result<()> {
        let mut g = self.replica_wal.lock().expect("replica wal lock");
        let wal = g
            .as_mut()
            .ok_or_else(|| std::io::Error::other("replica WAL not enabled"))?;
        wal.append(db, mult, body)?;
        self.cl2_replica_frames.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Recover the peer's acknowledged writes: replay every frame in the
    /// replica WAL into the engine, flush, and CONSUME what was replayed.
    ///
    /// Consumption is what makes this idempotent in the only sense that
    /// matters. The first version re-applied the full WAL every time and
    /// called itself idempotent because LWW dedup "collapses duplicate
    /// keys at compaction" — but each recover ends in its own flush, so
    /// the re-applied rows land in a NEW file, and cross-file dedup waits
    /// for a compaction that needs `compact_min_files` files in one
    /// partition. Two files never reach a threshold of four, so a second
    /// recover doubled every recovered count on the serving read path,
    /// durably (Catchment `ingester-kill`, 2026-08-13). Consuming the
    /// frames turns the second recover into what an operator means by it:
    /// apply what arrived since the first, which is also exactly the
    /// catch-up flow after the peer returns and new frames stream in.
    ///
    /// The replica lock is held across seal → read → apply → flush →
    /// consume. A frame arriving mid-recovery (peer back up) must not
    /// slip between the read and the delete — it would be consumed
    /// unapplied, which is acked loss. The peer's replication call blocks
    /// or times out into degraded for the duration; a node mid-recovery
    /// is not a reliable replica anyway, and degraded-not-failed is the
    /// pair's documented answer to that (PR-7). Crash between the flush
    /// and the delete re-applies one recovery's frames — the narrow tail
    /// of the old bug, closed by the next compaction. The other order
    /// would turn that crash into data loss, which is never the trade.
    ///
    /// Phase-2 scope: recovery is an explicit operation (the drill and, in
    /// production, an operator or the router on a confirmed peer death).
    /// Automatic health-triggered failover is a later cluster phase.
    /// Returns the frame count replayed.
    pub fn recover_from_replica(&self) -> std::io::Result<usize> {
        let mut g = self.replica_wal.lock().expect("replica wal lock");
        let wal = g
            .as_mut()
            .ok_or_else(|| std::io::Error::other("replica WAL not enabled"))?;
        let dir = self
            .replica_wal_dir
            .read()
            .expect("replica dir lock")
            .clone()
            .ok_or_else(|| std::io::Error::other("replica WAL not enabled"))?;
        // Seal everything received so far; frames arriving after this
        // recover belong to the next one. The fresh generation rotate()
        // opens is where they will land.
        let sealed = wal.rotate()?;
        let (_reader, frames) = Wal::open(&dir, self.wal_cipher.clone())?;
        let n = frames.len();
        for (db, mult, body) in frames {
            let text = String::from_utf8_lossy(&body);
            if let Err(e) = self.apply(&db, &text, mult, 0) {
                tracing::warn!(db, error = %e, "skipping unrecoverable replica frame");
            }
        }
        self.flush_all().map_err(std::io::Error::other)?;
        wal.delete_generations_upto(sealed)?;
        self.cl2_recovered.fetch_add(n as u64, Ordering::Relaxed);
        tracing::info!(
            frames = n,
            "CL2 recovery complete: peer's writes replayed, flushed, and consumed"
        );
        Ok(n)
    }

    // ---- CL-3: the querier and the live rows it reads ------------------

    /// Refuse writes on this node (CL-3). A querier is a read replica: it
    /// has no peer to replicate to and its WAL is read by nobody, so a
    /// write accepted here would be acknowledged and then invisible to the
    /// cluster — the one outcome worse than refusing it.
    pub fn set_read_only(&self) {
        self.read_only.store(true, Ordering::Relaxed);
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only.load(Ordering::Relaxed)
    }

    /// Attach the ingesters whose live buffers this node unions into every
    /// query (CL-3). Only a querier sets this.
    pub fn set_remote_buffers(&self, remote: Arc<querier::RemoteBuffers>) {
        *self.remote.write().expect("remote lock") = Some(remote);
    }

    pub fn remote_buffers(&self) -> Option<Arc<querier::RemoteBuffers>> {
        self.remote.read().expect("remote lock").clone()
    }

    /// The highest manifest sequence this node has applied — the freshness
    /// watermark a querier catches up to (see `querier`).
    pub fn catalog_head(&self) -> u64 {
        self.catalog.head()
    }

    /// What this node is holding in memory right now: every table with live
    /// (unflushed) rows, including rows mid-flush. This is what a querier
    /// asks for, and the answer is deliberately the same set the local read
    /// path unions — one definition of "live", not two that drift.
    pub fn live_tables(&self) -> Vec<(String, String, u64)> {
        let mut out: HashMap<(String, String), u64> = HashMap::new();
        {
            // One view of both, as the read path takes them — otherwise a
            // flush landing mid-count reports a table's rows twice or not
            // at all, and a querier decides what to fetch from this.
            let dbs = self.dbs.read().expect("dbs lock");
            let hold = self.flushing.read().expect("flushing lock");
            for (db, tables) in dbs.iter() {
                for (table, buf) in tables.iter() {
                    let rows = buf.row_count() as u64;
                    if rows > 0 {
                        *out.entry((db.clone(), table.clone())).or_default() += rows;
                    }
                }
            }
            for ((db, table), batch) in hold.iter() {
                *out.entry((db.clone(), table.clone())).or_default() += batch.num_rows() as u64;
            }
        }
        let mut v: Vec<(String, String, u64)> = out
            .into_iter()
            .map(|((db, table), rows)| (db, table, rows))
            .collect();
        v.sort();
        v
    }

    /// One table's live rows as Arrow IPC — the querier's read of this
    /// node's buffer.
    ///
    /// The order is the same as the local read path for the same reason:
    /// buffer, then rows mid-flush. A batch that has left `flushing` is
    /// already committed to the catalog, and the caller's head watermark
    /// guarantees it sees that commit — so the worst case here is a
    /// transient duplicate, never a vanished row.
    pub fn snapshot_ipc(&self, db: &str, table: &str) -> Result<Vec<u8>, String> {
        let mut batches: Vec<timelake_query::QueryBatch> = {
            // Both locks together, as everywhere else: a querier must not
            // receive the same rows twice because a flush landed between
            // this node's two reads.
            let dbs = self.dbs.read().expect("dbs lock");
            let hold = self.flushing.read().expect("flushing lock");
            let mut batches = match dbs.get(db).and_then(|t| t.get(table)) {
                Some(buf) if buf.row_count() > 0 => vec![buf.snapshot()?],
                _ => Vec::new(),
            };
            if let Some(held) = hold.get(&(db.to_string(), table.to_string())) {
                batches.push(held.clone());
            }
            batches
        };
        // Two batches of the same table can carry different column sets (a
        // write added a column after the mid-flush batch was taken). One
        // IPC stream has one schema, so widen both to the union rather than
        // dropping the newer column.
        if batches.len() > 1 {
            let mut merged: Option<timelake_query::QuerySchema> = None;
            for b in &batches {
                merged = Some(timelake_query::schema_union(merged, b.schema())?);
            }
            if let Some(schema) = merged {
                batches = timelake_query::align_to(schema, batches)?;
            }
        }
        timelake_query::ipc::to_ipc(&batches)
    }

    /// Fold the shared manifest log forward until this node has applied at
    /// least `target` (0 = "just take whatever is there"). Read-only: a
    /// querier commits nothing, so this is the only way its view advances.
    ///
    /// Returns the head actually reached. Falling short is reported, not
    /// hidden — a querier that cannot reach the watermark is a querier
    /// whose answers could be short.
    pub fn catch_up_catalog(&self, target: u64) -> u64 {
        if self.catalog.head() >= target && target > 0 {
            return self.catalog.head();
        }
        let before = self.catalog.head();
        if let Err(e) = self.catalog.catch_up() {
            tracing::warn!(error = %e, "catalog tail failed");
            return before;
        }
        let head = self.catalog.head();
        if head != before {
            // New files can mean new tables or new columns; the registry is
            // what the query path builds its merged schema from.
            self.refresh_schema_registry();
        }
        head
    }

    /// (Re)build the schema registry from the newest cataloged file per
    /// table (fullest column set in practice; compaction folds old columns
    /// forward over time). Footer-only reads, cached — cheap enough to run
    /// on every catalog advance, which is what a querier needs: a table
    /// that gained a column after this node booted must not read short.
    ///
    /// The registry only ever widens: the union with what is already known
    /// means a compaction that drops back to an older column set cannot
    /// un-register a column that live rows may still carry.
    fn refresh_schema_registry(&self) {
        let mut newest: HashMap<(String, String), (String, u64)> = HashMap::new();
        for f in self.catalog.all_files() {
            let key = (f.db.clone(), f.table.clone());
            let e = newest.entry(key).or_default();
            if f.path > e.0 {
                *e = (f.path, f.size_bytes);
            }
        }
        for ((db, table), (path, size)) in newest {
            let key = (db, table);
            if self
                .schema_source
                .read()
                .expect("schema source lock")
                .get(&key)
                == Some(&path)
            {
                continue; // already folded this file in
            }
            match timelake_query::provider::file_schema(&self.store, &path, size, &self.meta_cache)
            {
                Ok(schema) => {
                    let mut reg = self.schemas.write().expect("schemas lock");
                    let known = reg.get(&key).cloned();
                    match timelake_query::schema_union(known, schema) {
                        Ok(merged) => {
                            reg.insert(key.clone(), merged);
                            drop(reg);
                            self.schema_source
                                .write()
                                .expect("schema source lock")
                                .insert(key, path);
                        }
                        Err(e) => tracing::warn!(path, error = %e, "schema merge failed"),
                    }
                }
                Err(e) => tracing::warn!(path, error = %e, "schema bootstrap failed"),
            }
        }
    }

    /// U2: take one self-monitoring sample and store everything queued.
    ///
    /// Called from the maintenance tick, never from the query or write
    /// path. Returns the number of rows written, or `Ok(0)` when
    /// self-monitoring is off or there is nothing to store.
    ///
    /// **A failure here is not a server failure.** If `_system` cannot be
    /// written — the WAL is at its cap, the node has gone read-only — the
    /// rows are discarded and counted, not retried and not propagated. The
    /// whole point of the bounded queue is that telemetry yields to the
    /// workload it is telemetry *for*.
    pub fn selfmon_tick(&self) -> usize {
        let Some(monitor) = &self.selfmon else {
            return 0;
        };
        // A querier holds no write path (CL-3), so there is nowhere local
        // to put a sample. `/metrics` still answers; see the module docs.
        if self.read_only.load(Ordering::Relaxed) {
            return 0;
        }

        monitor.sample_exposition(&self.metrics_text_impl(), Self::now_ns());
        let lines = monitor.drain();
        if lines.is_empty() {
            return 0;
        }

        let body = lines.join("\n");
        match timelake_api::Engine::write_lp(self, selfmon::SELFMON_DB, body.as_bytes(), None) {
            Ok(n) => {
                monitor.note_written(n as u64);
                n
            }
            Err(e) => {
                let reason = match &e {
                    WriteError::BadRequest(m)
                    | WriteError::Backpressure(m)
                    | WriteError::Internal(m)
                    | WriteError::NotHere(m) => m.as_str(),
                };
                // Warn, not error: the server is fine, the telemetry is
                // not. Logging the count makes the gap in the stored
                // history measurable after the fact.
                tracing::warn!(
                    rows = lines.len(),
                    reason,
                    "self-monitoring sample dropped; /metrics is unaffected"
                );
                0
            }
        }
    }

    /// U2 lifecycle stamps: millis since process start, recorded when a
    /// flush or a compaction completes.
    fn stamp_flush(&self) {
        self.last_flush_ms
            .store(self.started.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    fn stamp_compaction(&self) {
        self.last_compaction_ms
            .store(self.started.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    pub fn uptime_secs(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    /// Seconds since the event a stamp records.
    ///
    /// With no event yet the stamp is zero, so this returns the uptime —
    /// "nothing has flushed in the whole time this node has been up",
    /// which is true and is the thing worth alerting on. Returning zero
    /// instead would read as "just flushed", i.e. exactly healthy, at the
    /// moment the subsystem has never run at all.
    fn lag_secs(&self, stamp: &AtomicU64) -> f64 {
        let now_ms = self.started.elapsed().as_millis() as u64;
        now_ms.saturating_sub(stamp.load(Ordering::Relaxed)) as f64 / 1000.0
    }

    /// U2: per-table storage, file levels, lifecycle lag, uptime, build.
    fn lifecycle_lines(&self) -> String {
        let mut out = format!(
            "# HELP timelake_uptime_seconds Seconds since this process started.\n\
             # TYPE timelake_uptime_seconds gauge\n\
             timelake_uptime_seconds {}\n\
             # HELP timelake_build_info Build metadata; the value is always 1.\n\
             # TYPE timelake_build_info gauge\n\
             timelake_build_info{{version=\"{}\"}} 1\n\
             # HELP timelake_flush_lag_seconds Seconds since the last completed flush.\n\
             # TYPE timelake_flush_lag_seconds gauge\n\
             timelake_flush_lag_seconds {:.3}\n\
             # HELP timelake_compaction_lag_seconds Seconds since the last compaction pass.\n\
             # TYPE timelake_compaction_lag_seconds gauge\n\
             timelake_compaction_lag_seconds {:.3}\n\
             # HELP timelake_gc_pending_files Files awaiting deletion after the GC grace.\n\
             # TYPE timelake_gc_pending_files gauge\n\
             timelake_gc_pending_files {}\n",
            self.uptime_secs(),
            escape_label(env!("CARGO_PKG_VERSION")),
            self.lag_secs(&self.last_flush_ms),
            self.lag_secs(&self.last_compaction_ms),
            self.pending_gc.lock().expect("pending gc lock").len(),
        );

        let levels = self.catalog.level_counts();
        out.push_str(&format!(
            "# HELP timelake_files Data files by provenance (see catalog::level_of).\n\
             # TYPE timelake_files gauge\n\
             timelake_files{{level=\"flushed\"}} {}\n\
             timelake_files{{level=\"compacted\"}} {}\n\
             timelake_files{{level=\"rewritten\"}} {}\n",
            levels.flushed, levels.compacted, levels.rewritten,
        ));

        // Per-table series. Cardinality is the table count, which FR-2
        // bounds deliberately — tags are columns here, not series, so this
        // does not inherit the high-cardinality problem the engine exists
        // to avoid.
        let storage = self.catalog.storage_summary();
        out.push_str(
            "# HELP timelake_storage_bytes Parquet bytes in the catalog, per table.\n\
             # TYPE timelake_storage_bytes gauge\n",
        );
        for t in &storage {
            out.push_str(&format!(
                "timelake_storage_bytes{{db=\"{}\",table=\"{}\"}} {}\n",
                escape_label(&t.db),
                escape_label(&t.table),
                t.bytes,
            ));
        }
        out.push_str(
            "# HELP timelake_storage_rows Rows in catalog files, per table.\n\
             # TYPE timelake_storage_rows gauge\n",
        );
        for t in &storage {
            out.push_str(&format!(
                "timelake_storage_rows{{db=\"{}\",table=\"{}\"}} {}\n",
                escape_label(&t.db),
                escape_label(&t.table),
                t.rows,
            ));
        }
        out.push_str(&self.write_rejects.render());
        // The self-monitoring counters belong on `/metrics` specifically:
        // if the sampler is dropping rows then the STORED history is the
        // thing that is incomplete, so it cannot be the surface that
        // reports the gap. This one still works when that one does not.
        if let Some(monitor) = &self.selfmon {
            out.push_str(&monitor.render());
        }
        out
    }

    pub fn metrics_text_impl(&self) -> String {
        let (n_dbs, n_tables, n_rows) = {
            let dbs = self.dbs.read().expect("dbs lock");
            let tables: usize = dbs.values().map(|t| t.len()).sum();
            let rows: usize = dbs
                .values()
                .flat_map(|t| t.values())
                .map(|b| b.row_count())
                .sum();
            (dbs.len(), tables, rows)
        };
        let wal_bytes = self.wal.lock().expect("wal lock").size();
        let kms_lines = match &self.kms_stats {
            Some(k) => format!(
                "# TYPE timelake_kms_generate_total counter\n\
                 timelake_kms_generate_total {}\n\
                 # TYPE timelake_kms_decrypt_total counter\n\
                 timelake_kms_decrypt_total {}\n\
                 # TYPE timelake_kms_generate_cache_hits_total counter\n\
                 timelake_kms_generate_cache_hits_total {}\n\
                 # TYPE timelake_kms_decrypt_cache_hits_total counter\n\
                 timelake_kms_decrypt_cache_hits_total {}\n",
                k.generate_calls.load(Ordering::Relaxed),
                k.decrypt_calls.load(Ordering::Relaxed),
                k.generate_hits.load(Ordering::Relaxed),
                k.decrypt_hits.load(Ordering::Relaxed),
            ),
            None => String::new(),
        };
        let s3_lines = match &self.s3_stats {
            Some(s) => format!(
                "# TYPE timelake_s3_get_total counter\ntimelake_s3_get_total {}\n\
                 # TYPE timelake_s3_put_total counter\ntimelake_s3_put_total {}\n\
                 # TYPE timelake_s3_head_total counter\ntimelake_s3_head_total {}\n\
                 # TYPE timelake_s3_list_total counter\ntimelake_s3_list_total {}\n\
                 # TYPE timelake_s3_delete_total counter\ntimelake_s3_delete_total {}\n\
                 # TYPE timelake_s3_read_bytes_total counter\n\
                 timelake_s3_read_bytes_total {}\n\
                 # TYPE timelake_s3_write_bytes_total counter\n\
                 timelake_s3_write_bytes_total {}\n",
                s.get_total.load(Ordering::Relaxed),
                s.put_total.load(Ordering::Relaxed),
                s.head_total.load(Ordering::Relaxed),
                s.list_total.load(Ordering::Relaxed),
                s.delete_total.load(Ordering::Relaxed),
                s.read_bytes_total.load(Ordering::Relaxed),
                s.write_bytes_total.load(Ordering::Relaxed),
            ),
            None => String::new(),
        };
        // The ratio of these two is what tells an operator when a
        // deployment has finished migrating and it is safe to flip a
        // listener from want mode to require — a decision that should
        // come from a measurement, not a guess.
        let client_ca_lines = match self.client_ca.read().expect("client ca lock").as_ref() {
            Some(ca) => format!(
                "# TYPE timelake_tls_client_ca_anchors gauge\n\
                 timelake_tls_client_ca_anchors {}\n\
                 # TYPE timelake_tls_client_ca_last_reload_ok gauge\n\
                 timelake_tls_client_ca_last_reload_ok {}\n\
                 # TYPE timelake_tls_client_auth_mode gauge\n\
                 timelake_tls_client_auth_mode 1\n",
                ca.anchors(),
                if ca.last_reload_ok() { 1 } else { 0 },
            ),
            None => String::new(),
        };
        let (da_auth, da_anon, da_rej) = self.data_auth_counts.snapshot();
        let data_auth_lines = format!(
            "# TYPE timelake_data_auth_mode gauge\n\
             timelake_data_auth_mode {}\n\
             # TYPE timelake_data_requests_authenticated_total counter\n\
             timelake_data_requests_authenticated_total {}\n\
             # TYPE timelake_data_requests_anonymous_total counter\n\
             timelake_data_requests_anonymous_total {}\n\
             # TYPE timelake_data_requests_rejected_total counter\n\
             timelake_data_requests_rejected_total {}\n",
            match self.cfg.data_auth {
                timelake_auth::DataAuthMode::Off => 0,
                timelake_auth::DataAuthMode::Optional => 1,
                timelake_auth::DataAuthMode::Required => 2,
            },
            da_auth,
            da_anon,
            da_rej,
        );
        let auth_split_lines = match self
            .client_auth_counts
            .read()
            .expect("auth counts lock")
            .as_ref()
        {
            Some(c) => format!(
                "# TYPE timelake_flight_connections_authenticated_total counter\n\
                 timelake_flight_connections_authenticated_total {}\n\
                 # TYPE timelake_flight_connections_anonymous_total counter\n\
                 timelake_flight_connections_anonymous_total {}\n",
                c.authenticated.load(Ordering::Relaxed),
                c.anonymous.load(Ordering::Relaxed),
            ),
            None => String::new(),
        };
        let tls_lines = match self.tls.read().expect("tls lock").as_ref() {
            Some(t) => format!(
                "# TYPE timelake_tls_cert_expiry_seconds gauge\n\
                 timelake_tls_cert_expiry_seconds {}\n\
                 # TYPE timelake_tls_last_reload_ok gauge\n\
                 timelake_tls_last_reload_ok {}\n",
                t.expires_in_secs(),
                if t.last_reload_ok() { 1 } else { 0 },
            ),
            None => String::new(),
        };
        // CL-2 lines only on an ingester (replicator set) — a lone `all`
        // node's /metrics is unchanged.
        let cl2_lines = match self.replication_stats() {
            Some(s) => format!(
                "# TYPE timelake_cl2_replicated_total counter\n\
                 timelake_cl2_replicated_total {}\n\
                 # TYPE timelake_cl2_degraded gauge\n\
                 timelake_cl2_degraded {}\n\
                 # TYPE timelake_cl2_degraded_events_total counter\n\
                 timelake_cl2_degraded_events_total {}\n\
                 # TYPE timelake_cl2_replica_frames_total counter\n\
                 timelake_cl2_replica_frames_total {}\n\
                 # TYPE timelake_cl2_recovered_total counter\n\
                 timelake_cl2_recovered_total {}\n",
                s.replicated.load(Ordering::Relaxed),
                if s.degraded.load(Ordering::Relaxed) {
                    1
                } else {
                    0
                },
                s.degraded_events.load(Ordering::Relaxed),
                self.cl2_replica_frames.load(Ordering::Relaxed),
                self.cl2_recovered.load(Ordering::Relaxed),
            ),
            None => String::new(),
        };
        // The CL-3 read gate. Gated on the listener existing rather than on
        // a replicator: an ingester with no peer still serves queriers, and
        // its refusals are exactly as worth seeing. Named apart from the
        // querier-side cl3 lines below, which answer a different question.
        let read_gate_lines = if self.serves_internal_listener() {
            format!(
                "# TYPE timelake_cl3_reads_refused_total counter\n\
                 timelake_cl3_reads_refused_total {}\n",
                self.cl3_reads_refused.load(Ordering::Relaxed),
            )
        } else {
            String::new()
        };
        // CL-3 lines only on a querier (remote buffers set). `refusals` is
        // the one to alert on: it separates "the cluster is down" from the
        // failure this database exists to prevent — quiet under-counting.
        let cl3_lines = match self.remote_buffers() {
            Some(r) => format!(
                "# TYPE timelake_querier_ingesters gauge\n\
                 timelake_querier_ingesters {}\n\
                 # TYPE timelake_querier_snapshot_fetches_total counter\n\
                 timelake_querier_snapshot_fetches_total {}\n\
                 # TYPE timelake_querier_snapshot_rows_total counter\n\
                 timelake_querier_snapshot_rows_total {}\n\
                 # TYPE timelake_querier_snapshot_errors_total counter\n\
                 timelake_querier_snapshot_errors_total {}\n\
                 # TYPE timelake_querier_refusals_total counter\n\
                 timelake_querier_refusals_total {}\n\
                 # TYPE timelake_querier_catchups_total counter\n\
                 timelake_querier_catchups_total {}\n\
                 # TYPE timelake_catalog_head gauge\n\
                 timelake_catalog_head {}\n",
                r.peer_count(),
                r.stats.snapshot_fetches.load(Ordering::Relaxed),
                r.stats.snapshot_rows.load(Ordering::Relaxed),
                r.stats.snapshot_errors.load(Ordering::Relaxed),
                r.stats.refusals.load(Ordering::Relaxed),
                r.stats.catchups.load(Ordering::Relaxed),
                self.catalog.head(),
            ),
            None => String::new(),
        };
        format!(
            "# TYPE timelake_lines_written_total counter\n\
             timelake_lines_written_total {}\n\
             # TYPE timelake_flushes_total counter\ntimelake_flushes_total {}\n\
             # TYPE timelake_parquet_files gauge\ntimelake_parquet_files {}\n\
             # TYPE timelake_catalog_commit_conflicts_total counter\n\
             timelake_catalog_commit_conflicts_total {}\n\
             # TYPE timelake_databases gauge\ntimelake_databases {}\n\
             # TYPE timelake_tables gauge\ntimelake_tables {}\n\
             # TYPE timelake_buffer_rows gauge\ntimelake_buffer_rows {}\n\
             # TYPE timelake_wal_bytes gauge\ntimelake_wal_bytes {}\n\
             # TYPE timelake_compactions_total counter\ntimelake_compactions_total {}\n\
             # TYPE timelake_stale_merges_total counter\ntimelake_stale_merges_total {}\n\
             # TYPE timelake_retention_drops_total counter\ntimelake_retention_drops_total {}\n\
             # TYPE timelake_rollups gauge\ntimelake_rollups {}\n\
             # TYPE timelake_tombstone_rewrites_total counter\n\
             timelake_tombstone_rewrites_total {}\n\
             # TYPE timelake_encryption_enabled gauge\ntimelake_encryption_enabled {}\n\
             # TYPE timelake_visibility_rows_filtered_total counter\n\
             timelake_visibility_rows_filtered_total {}\n\
             # TYPE timelake_admin_default_credential_active gauge\n\
             timelake_admin_default_credential_active {}\n\
             # TYPE timelake_admin_logins_total counter\n\
             timelake_admin_logins_total {}\n\
             # TYPE timelake_admin_login_failures_total counter\n\
             timelake_admin_login_failures_total {}\n\
             # TYPE timelake_auth_token_reloads_total counter\n\
             timelake_auth_token_reloads_total {}\n\
             # TYPE timelake_query_rate_limited_total counter\n\
             timelake_query_rate_limited_total {}\n\
             # TYPE timelake_audit_records_total counter\n\
             timelake_audit_records_total {}\n\
             # TYPE timelake_audit_sink_healthy gauge\n\
             timelake_audit_sink_healthy {}\n{}{}{}{}{}{}{}{}{}{}{}",
            self.lines_total.load(Ordering::Relaxed),
            self.flushes_total.load(Ordering::Relaxed),
            self.catalog.file_count(),
            self.catalog.commit_conflicts(),
            n_dbs,
            n_tables,
            n_rows,
            wal_bytes,
            self.compactions_total.load(Ordering::Relaxed),
            self.stale_merges.load(Ordering::Relaxed),
            self.retention_drops_total.load(Ordering::Relaxed),
            self.rollups.read().expect("rollups lock").len(),
            self.tombstone_rewrites_total.load(Ordering::Relaxed),
            if self.store_encrypted { 1 } else { 0 },
            self.visibility_filtered.load(Ordering::Relaxed),
            if self.auth.default_credential_active() {
                1
            } else {
                0
            },
            self.auth.logins_total.load(Ordering::Relaxed),
            self.auth.login_failures_total.load(Ordering::Relaxed),
            self.auth.token_reloads_total.load(Ordering::Relaxed),
            self.client_limiter.rejected(),
            self.audit.records_total(),
            if self.audit.healthy() { 1 } else { 0 },
            client_ca_lines,
            data_auth_lines,
            auth_split_lines,
            kms_lines,
            s3_lines,
            tls_lines,
            cl2_lines,
            cl3_lines,
            read_gate_lines,
            // U2: latency, admission pressure and outcomes. Unconditional —
            // unlike the cluster blocks above, every node runs queries, and
            // this is the block that makes "the database got slow"
            // answerable without running the harness.
            self.query_env.metrics.render(),
            self.lifecycle_lines(),
        )
    }
}

impl Engine {
    pub fn config(&self) -> EngineConfig {
        self.cfg.clone()
    }

    /// The one data-plane authentication path. Both the HTTP router and
    /// Flight SQL land here, so HTTP and Flight cannot drift into two
    /// policies — and the counters see every request whichever door it
    /// came through.
    fn authenticate_data_impl(
        &self,
        authorization: Option<&str>,
        action: timelake_auth::Action,
        db: &str,
    ) -> Result<timelake_auth::Decision, timelake_auth::TokenError> {
        let result = self
            .auth
            .decide_data(self.cfg.data_auth, authorization, action, db);
        match &result {
            Ok(d) if d.is_authenticated() => {
                self.data_auth_counts
                    .authenticated
                    .fetch_add(1, Ordering::Relaxed);
            }
            Ok(_) => {
                self.data_auth_counts
                    .anonymous
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                self.data_auth_counts
                    .rejected
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    code = e.code(),
                    action = ?action,
                    db,
                    "data-plane request refused"
                );
            }
        }
        result
    }
}

impl timelake_flight::SqlBackend for Engine {
    fn authenticate_read(
        &self,
        authorization: Option<&str>,
        db: &str,
    ) -> Result<Option<Vec<String>>, timelake_auth::TokenError> {
        self.authenticate_data_impl(authorization, timelake_auth::Action::Read, db)
            .map(|d| d.granted)
    }

    fn admit_client(&self, key: Option<String>) -> Option<Box<dyn Send>> {
        self.client_limiter
            .admit(key)
            .map(|g| Box::new(g) as Box<dyn Send>)
    }

    fn query_batches<'a>(
        &'a self,
        db: String,
        sql: String,
        authorizations: Vec<String>,
        identity: Option<String>,
    ) -> timelake_flight::SqlFuture<'a> {
        Box::pin(async move {
            self.sql_batches(&db, &sql, authorizations, identity.as_deref())
                .await
        })
    }

    fn databases(&self) -> Vec<String> {
        Engine::databases(self)
    }

    fn tables(&self, db: &str) -> Vec<String> {
        self.table_names(db)
    }

    fn table_schema(&self, db: &str, table: &str) -> Option<timelake_query::QuerySchema> {
        Engine::table_schema(self, db, table)
    }
}

/// The plaintext router. Admin routes authenticate (SEC-4); the data
/// plane does not (that migration is its own milestone).
pub fn app(engine: Arc<Engine>) -> axum::Router {
    let auth = engine.auth();
    let audit = engine.audit_log();
    let limit = engine.max_body_bytes();
    timelake_api::app(engine, auth, audit, false).layer(axum::extract::DefaultBodyLimit::max(limit))
}

/// The compactor's HTTP surface: health, ping and metrics, nothing else.
///
/// A compactor takes no writes and answers no queries, so it gets no data
/// plane. It still needs to be reachable, because a maintenance node that
/// cannot be scraped is a node nobody notices has stopped — and a
/// compactor that has stopped is invisible by construction, since its work
/// only shows up as the absence of file growth.
pub fn compactor_app(engine: Arc<Engine>) -> axum::Router {
    let limit = engine.max_body_bytes();
    timelake_api::maintenance_app(engine).layer(axum::extract::DefaultBodyLimit::max(limit))
}

/// The intra-cluster listener for an ingester (CL-2 replication, CL-3 live
/// reads), bound to `TIMELAKE_CLUSTER_ADDR`. NOT the public data port: it
/// carries only peer traffic, and at C3 it moves behind required mTLS.
/// There is no data-plane auth here — trust is the private network now, the
/// peer certificate later.
///
/// SECURITY NOTE: `/internal/v1/snapshot` returns *rows*, not just metadata,
/// and applies no SEC-2 visibility filter — a querier re-applies the
/// caller's restriction when it scans them, exactly as it does for a file
/// it reads from the bucket. So this listener must never be exposed
/// publicly; it is the same trust boundary as read access to the object
/// store itself.
pub fn internal_router(engine: Arc<Engine>) -> axum::Router {
    let limit = engine.max_body_bytes();
    let gate = Arc::new(ReadGate {
        sem: Arc::new(tokio::sync::Semaphore::new(
            engine.internal_max_concurrent(),
        )),
        engine: Arc::clone(&engine),
    });

    // Reads a querier fans out. These are bounded: a querier unions every
    // ingester's live buffer on every query, so read load on the query tier
    // lands as work on the ingest tier, and an ingester's real job is taking
    // writes (D2). The permit is *tried*, never waited on — queueing here
    // would turn a refusal into latency, and the querier's own 30 s deadline
    // would hold the resources for the whole of it.
    let reads = axum::Router::new()
        .route("/internal/v1/live", axum::routing::get(internal_live))
        .route(
            "/internal/v1/snapshot",
            axum::routing::get(internal_snapshot),
        )
        .with_state(Arc::clone(&engine))
        .layer(axum::middleware::from_fn_with_state(
            gate,
            internal_read_gate,
        ));

    // Deliberately unbounded. Throttling `replicate` would stall a peer's
    // write path, which is the failure D1 exists to prevent — protecting
    // ingest by strangling ingest. `health` must answer while the node is
    // saturated, since that is exactly when the answer carries information.
    let unbounded = axum::Router::new()
        .route(
            "/internal/v1/replicate",
            axum::routing::post(internal_replicate),
        )
        .route(
            "/internal/v1/recover",
            axum::routing::post(internal_recover),
        )
        .route("/internal/v1/health", axum::routing::get(|| async { "ok" }))
        .with_state(engine);

    reads
        .merge(unbounded)
        // Same ceiling as the public plane, from the same config value: a
        // frame this listener refuses is a write its peer already took.
        .layer(axum::extract::DefaultBodyLimit::max(limit))
}

/// State for the CL-3 read gate: the permits, and the engine that counts
/// refusals so they show up on `/metrics` rather than only in a log.
#[derive(Clone)]
struct ReadGate {
    sem: Arc<tokio::sync::Semaphore>,
    engine: Arc<Engine>,
}

/// Refuse rather than queue when the ceiling is reached.
///
/// 503 is the honest answer and the querier already handles it: a failed
/// snapshot makes it refuse the query instead of answering from an
/// incomplete cluster. A queued request would instead be answered late,
/// which for a query that has already been abandoned is pure cost paid by
/// the ingest path.
async fn internal_read_gate(
    axum::extract::State(gate): axum::extract::State<Arc<ReadGate>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match Arc::clone(&gate.sem).try_acquire_owned() {
        Ok(_permit) => next.run(req).await,
        Err(_) => {
            gate.engine.note_cl3_read_refused();
            (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "intra-cluster read concurrency exhausted",
            )
                .into_response()
        }
    }
}

/// What this node holds live, plus its catalog head (CL-3).
async fn internal_live(
    axum::extract::State(engine): axum::extract::State<Arc<Engine>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let tables: Vec<Value> = engine
        .live_tables()
        .into_iter()
        .map(|(db, table, rows)| serde_json::json!({"db": db, "table": table, "rows": rows}))
        .collect();
    // The head is read AFTER the buffers, so it can only be newer than the
    // rows reported — never older. The other order would let a caller trust
    // a watermark that predates a flush it did not see.
    let head = engine.catalog_head();
    axum::Json(serde_json::json!({ "head": head, "tables": tables })).into_response()
}

/// One table's live rows as an Arrow IPC stream (CL-3).
async fn internal_snapshot(
    axum::extract::State(engine): axum::extract::State<Arc<Engine>>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    let (Some(db), Some(table)) = (params.get("db"), params.get("table")) else {
        return (StatusCode::BAD_REQUEST, "missing db/table").into_response();
    };
    let (db, table) = (db.clone(), table.clone());
    let e = Arc::clone(&engine);
    // Snapshot + IPC encode are CPU work over a lock: off the async runtime.
    let encoded = tokio::task::spawn_blocking(move || e.snapshot_ipc(&db, &table)).await;
    // Head AFTER the snapshot, and this order is load-bearing: a batch
    // missing from the snapshot was committed before the snapshot was
    // taken, so a head read now is guaranteed to include that commit. Read
    // first, it could name a sequence older than a flush the caller did not
    // see — which is exactly how a row goes missing.
    let head = engine.catalog_head().to_string();
    match encoded {
        Ok(Ok(bytes)) => (
            StatusCode::OK,
            [
                ("content-type", "application/vnd.apache.arrow.stream"),
                (querier::CATALOG_HEAD_HEADER, head.as_str()),
            ],
            bytes,
        )
            .into_response(),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(join) => (StatusCode::INTERNAL_SERVER_ERROR, format!("join: {join}")).into_response(),
    }
}

/// Receive one replicated frame from the peer and record it durably.
async fn internal_replicate(
    axum::extract::State(engine): axum::extract::State<Arc<Engine>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> (axum::http::StatusCode, String) {
    use axum::http::StatusCode;
    let db = headers
        .get("x-repl-db")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    if db.is_empty() {
        return (StatusCode::BAD_REQUEST, "missing x-repl-db".into());
    }
    let mult: i64 = headers
        .get("x-repl-mult")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    // The append fsyncs — do it off the async runtime.
    match tokio::task::spawn_blocking(move || engine.replicate_receive(&db, mult, &body)).await {
        Ok(Ok(())) => (StatusCode::OK, String::new()),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(join) => (StatusCode::INTERNAL_SERVER_ERROR, format!("join: {join}")),
    }
}

/// Replay the peer's replica WAL into this node — the recovery a dead
/// ingester's rows return through.
async fn internal_recover(
    axum::extract::State(engine): axum::extract::State<Arc<Engine>>,
) -> (axum::http::StatusCode, String) {
    use axum::http::StatusCode;
    match tokio::task::spawn_blocking(move || engine.recover_from_replica()).await {
        Ok(Ok(n)) => (StatusCode::OK, format!("{{\"recovered\":{n}}}")),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(join) => (StatusCode::INTERNAL_SERVER_ERROR, format!("join: {join}")),
    }
}

/// The TLS-enabled router: the api surface plus the explicit rotation
/// trigger (SEC-3 mandates BOTH a file watcher and an admin endpoint;
/// reload-by-restart is forbidden). Session cookies are `Secure` here,
/// and the reload endpoint sits behind the same SEC-4 guard as the rest
/// of /admin.
pub fn app_with_tls_admin(
    engine: Arc<Engine>,
    rot: Arc<timelake_tls::RotatingCert>,
) -> axum::Router {
    use axum::response::IntoResponse;
    let reload = move || {
        let rot = Arc::clone(&rot);
        async move {
            let res =
                tokio::task::spawn_blocking(move || rot.reload().map(|_| rot.expires_in_secs()))
                    .await;
            match res {
                Ok(Ok(expires_in)) => (
                    axum::http::StatusCode::OK,
                    axum::Json(serde_json::json!({
                        "status": "rotated",
                        "expires_in_seconds": expires_in,
                    })),
                )
                    .into_response(),
                Ok(Err(e)) => (
                    axum::http::StatusCode::UNPROCESSABLE_ENTITY,
                    axum::Json(serde_json::json!({
                        "error": e.to_string(),
                        "alarm": timelake_tls::RENEWAL_ALARM,
                        "serving": "last-good certificate",
                    })),
                )
                    .into_response(),
                Err(join) => (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(serde_json::json!({ "error": join.to_string() })),
                )
                    .into_response(),
            }
        }
    };
    let auth = engine.auth();
    let audit = engine.audit_log();
    timelake_api::app(engine, auth.clone(), audit, true).merge(
        axum::Router::new()
            .route("/admin/tls/reload", axum::routing::post(reload))
            .layer(axum::middleware::from_fn_with_state(
                auth,
                timelake_api::require_admin_session,
            )),
    )
}

/// Parse engine config from environment (main + integration use).
pub fn config_from_env() -> EngineConfig {
    fn env<T: std::str::FromStr>(k: &str, d: T) -> T {
        std::env::var(k)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(d)
    }
    let d = EngineConfig::default();
    EngineConfig {
        query_mem_bytes: env("TIMELAKE_QUERY_MEM_BYTES", d.query_mem_bytes),
        flush_rows: env("TIMELAKE_FLUSH_ROWS", d.flush_rows),
        flush_age_secs: env("TIMELAKE_FLUSH_AGE_SECS", d.flush_age_secs),
        wal_max_bytes: env("TIMELAKE_WAL_MAX_BYTES", d.wal_max_bytes),
        compact_min_files: env("TIMELAKE_COMPACT_MIN_FILES", d.compact_min_files),
        retention: std::env::var("TIMELAKE_RETENTION")
            .map(|s| parse_retention(&s))
            .unwrap_or_default(),
        max_concurrent_queries: env("TIMELAKE_MAX_CONCURRENT_QUERIES", d.max_concurrent_queries),
        max_concurrent_queries_per_client: env(
            "TIMELAKE_MAX_CONCURRENT_QUERIES_PER_CLIENT",
            d.max_concurrent_queries_per_client,
        ),
        query_timeout_secs: env("TIMELAKE_QUERY_TIMEOUT_SECS", d.query_timeout_secs),
        gc_grace_secs: env("TIMELAKE_GC_GRACE_SECS", d.gc_grace_secs),
        repl_timeout_ms: env("TIMELAKE_REPL_TIMEOUT_MS", d.repl_timeout_ms),
        max_body_bytes: env("TIMELAKE_MAX_BODY_BYTES", d.max_body_bytes),
        internal_max_concurrent: env(
            "TIMELAKE_INTERNAL_MAX_CONCURRENT",
            d.internal_max_concurrent,
        ),
        // A typo here must refuse to start, not silently disable
        // authentication — the same posture as a malformed encryption key.
        data_auth: match std::env::var("TIMELAKE_DATA_AUTH") {
            Ok(v) => timelake_auth::DataAuthMode::parse(&v)
                .unwrap_or_else(|| panic!("TIMELAKE_DATA_AUTH={v:?} is not off|optional|required")),
            Err(_) => d.data_auth,
        },
    }
}

/// Convenience used by main and tests.
pub fn data_dir_from_env() -> PathBuf {
    PathBuf::from(std::env::var("TIMELAKE_DATA_DIR").unwrap_or_else(|_| "./data".to_string()))
}
