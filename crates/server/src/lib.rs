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
use timelake_buffer::{TableBuffer, flush};
use timelake_catalog::{Catalog, FileMeta};
use timelake_ingest::{parse_lines, precision_multiplier};
use timelake_query::{QuerySession, batches_to_json};
use timelake_store::{CachingKms, EncryptingStore, Kms, KmsStats, LocalKek, LocalStore, Store};
use timelake_store_s3::{AwsContext, AwsKms, S3Stats, S3Store};
use timelake_wal::Wal;

pub mod replication;
pub mod router;
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
    /// Per-table retention (table name → seconds); FR-7. Empty = keep all.
    pub retention: Vec<(String, u64)>,
    /// Admission control: queries beyond this queue (RR-1).
    pub max_concurrent_queries: usize,
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
            query_timeout_secs: 600,
            gc_grace_secs: 900,
            data_auth: timelake_auth::DataAuthMode::Off,
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
}

impl StoreStack {
    fn plain(store: Arc<dyn Store>, encrypted: bool) -> StoreStack {
        StoreStack {
            store,
            encrypted,
            backend: "custom",
            kms_stats: None,
            s3_stats: None,
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
    let (store, encrypted, kms_stats): (Arc<dyn Store>, bool, Option<Arc<KmsStats>>) =
        match (kms_key, local_key) {
            (Some(key_id), None) => {
                let aws = AwsKms::new(ctx.expect("ctx built for kms"), key_id);
                if std::env::var("TIMELAKE_KMS_CACHE").as_deref() == Ok("off") {
                    tracing::warn!(
                        "TIMELAKE_KMS_CACHE=off: strict per-object data keys, \
                         one KMS call per object (the drill's baseline mode)"
                    );
                    let kms: Arc<dyn Kms> = Arc::new(aws);
                    (Arc::new(EncryptingStore::new(base, kms)), true, None)
                } else {
                    let cached = CachingKms::new(
                        aws,
                        std::time::Duration::from_secs(env_u64(
                            "TIMELAKE_KMS_CACHE_MAX_AGE_SECS",
                            300,
                        )),
                        env_u64("TIMELAKE_KMS_CACHE_MAX_USES", 1000) as u32,
                    );
                    let stats = cached.stats();
                    let kms: Arc<dyn Kms> = Arc::new(cached);
                    (Arc::new(EncryptingStore::new(base, kms)), true, Some(stats))
                }
            }
            (None, Some(kek)) => {
                let kms: Arc<dyn Kms> = Arc::new(LocalKek::new(kek));
                (Arc::new(EncryptingStore::new(base, kms)), true, None)
            }
            (None, None) => (base, false, None),
            (Some(_), Some(_)) => unreachable!("refused above"),
        };

    Ok(StoreStack {
        store,
        encrypted,
        backend,
        kms_stats,
        s3_stats,
    })
}

/// Where runtime retention config lives in the object store (FR-7).
const RETENTION_CONFIG_PATH: &str = "catalog/config/retention.json";

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

/// Parse "pipeline_events=365d,host_metrics=90d,disk_metrics=72h" (FR-7).
pub fn parse_retention(spec: &str) -> Vec<(String, u64)> {
    spec.split(',')
        .filter_map(|part| {
            let (table, dur) = part.trim().split_once('=')?;
            let dur = dur.trim();
            let (num, unit_secs) = if let Some(d) = dur.strip_suffix('d') {
                (d, 86_400)
            } else if let Some(h) = dur.strip_suffix('h') {
                (h, 3_600)
            } else {
                (dur, 1)
            };
            Some((
                table.trim().to_string(),
                num.parse::<u64>().ok()? * unit_secs,
            ))
        })
        .collect()
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
    /// Full column set per (db, table): survives flushes and restarts so
    /// providers can present a stable merged schema without re-reading
    /// every footer per query.
    schemas: RwLock<HashMap<(String, String), timelake_query::QuerySchema>>,
    /// SEC-4 principals and sessions for the admin surface.
    auth: Arc<timelake_auth::Auth>,
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
    retention: RwLock<Vec<(String, u64)>>,
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
    retention_drops_total: AtomicU64,
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
    cl2_recovered: AtomicU64,
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
        } = stack;
        let catalog = Catalog::load(store.clone())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let (wal, frames) = Wal::open(&data_dir.join("wal"))?;

        // FR-7 runtime policies: the store copy (written by
        // /admin/retention) outranks the env seed — an operator's
        // durable change must survive a restart with a stale env.
        let retention: Vec<(String, u64)> = match store.get(RETENTION_CONFIG_PATH) {
            Ok(bytes) => {
                let map: std::collections::BTreeMap<String, u64> = serde_json::from_slice(&bytes)
                    .map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("{RETENTION_CONFIG_PATH}: {e}"),
                    )
                })?;
                map.into_iter().collect()
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => cfg.retention.clone(),
            Err(e) => return Err(e),
        };

        let query_env = timelake_query::QueryEnv::new(
            cfg.query_mem_bytes,
            cfg.max_concurrent_queries,
            cfg.query_timeout_secs,
        );
        // SEC-4 principals live in the store like everything else, so
        // they are encrypted with it and travel with a cluster's bucket.
        let auth = timelake_auth::Auth::open(
            store.clone(),
            std::env::var("TIMELAKE_ADMIN_BOOTSTRAP_PASSWORD")
                .ok()
                .as_deref(),
        )?;
        let engine = Engine {
            dbs: RwLock::new(HashMap::new()),
            ingest_gate: RwLock::new(()),
            data_auth_counts: timelake_auth::DataAuthCounts::default(),
            wal: Mutex::new(wal),
            store,
            catalog,
            store_encrypted,
            query_env,
            schemas: RwLock::new(HashMap::new()),
            auth,
            retention: RwLock::new(retention),
            flushing: RwLock::new(HashMap::new()),
            cfg,
            lines_total: AtomicU64::new(0),
            flushes_total: AtomicU64::new(0),
            compactions_total: AtomicU64::new(0),
            retention_drops_total: AtomicU64::new(0),
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
            cl2_recovered: AtomicU64::new(0),
        };
        let n = frames.len();
        for (db, mult, body) in frames {
            let body = String::from_utf8_lossy(&body);
            if let Err(e) = engine.apply(&db, &body, mult, 0) {
                tracing::warn!(db, error = %e, "skipping unreplayable WAL frame");
            }
        }
        // Rebuild the schema registry from the NEWEST file per table
        // (fullest column set in practice; compaction folds old columns
        // forward over time). Full-footer sweep is an S3-era refinement.
        {
            let mut newest: HashMap<(String, String), String> = HashMap::new();
            for f in engine.catalog.all_files() {
                let key = (f.db.clone(), f.table.clone());
                let e = newest.entry(key).or_default();
                if f.path > *e {
                    *e = f.path;
                }
            }
            for ((db, table), path) in newest {
                match engine
                    .store
                    .get(&path)
                    .map_err(|e| e.to_string())
                    .and_then(|b| {
                        flush::read_parquet_bytes(b).map(|bs| bs.first().map(|b| b.schema()))
                    }) {
                    Ok(Some(schema)) => {
                        engine
                            .schemas
                            .write()
                            .expect("schemas lock")
                            .insert((db, table), schema);
                    }
                    Ok(None) => {}
                    Err(e) => tracing::warn!(path, error = %e, "schema bootstrap failed"),
                }
            }
        }
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
        self.lines_total.fetch_add(n as u64, Ordering::Relaxed);
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
        // 1. atomically: swap out all buffers + rotate the WAL
        let (owned, sealed_gen) = {
            let _gate = self.ingest_gate.write().expect("ingest gate");
            let mut dbs = self.dbs.write().expect("dbs lock");
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
            (owned, sealed)
        };

        // 1b. snapshot the swapped-out rows into the holding area BEFORE
        // any upload: from here on, queries serve them from `flushing`
        // while the (possibly slow, S3-era) object writes proceed. Pure
        // CPU — the unavoidable swap→holding gap stays sub-millisecond.
        let mut snapshots: Vec<(String, String, timelake_query::QueryBatch)> = Vec::new();
        let mut failed: Vec<String> = Vec::new();
        for (db, table, buf) in owned {
            match buf.snapshot() {
                Ok(batch) => {
                    self.flushing
                        .write()
                        .expect("flushing lock")
                        .insert((db.clone(), table.clone()), batch.clone());
                    snapshots.push((db, table, batch));
                }
                Err(err) => {
                    tracing::error!(%db, %table, %err, "flush snapshot failed; others continue");
                    failed.push(format!("{db}.{table}"));
                }
            }
        }

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
        let mut todo: Vec<_> = groups
            .into_values()
            .filter(|v| v.len() >= self.cfg.compact_min_files)
            .collect();
        // biggest wins first: most files = most read amplification saved
        todo.sort_by_key(|v| std::cmp::Reverse(v.len()));
        todo.truncate(MAX_GROUPS_PER_TICK);

        let mut done = 0;
        for mut files in todo {
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
            self.store
                .put(&path, &merged.bytes)
                .map_err(|e| format!("store put {path}: {e}"))?;
            let remove: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
            self.catalog
                .commit(
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
                )
                .map_err(|e| format!("catalog commit: {e}"))?;
            // deletion is DEFERRED (gc_grace): in-flight queries hold
            // catalog snapshots referencing the old paths (AT-3 race)
            self.defer_gc(remove);
            done += 1;
        }
        if done > 0 {
            self.compactions_total
                .fetch_add(done as u64, Ordering::Relaxed);
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
            if let Some((_, secs)) = policies.iter().find(|(table, _)| *table == f.table) {
                let cutoff = flush::hour_partition(now - (*secs as i64) * 1_000_000_000);
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

    /// SEC-4 principal/session store, shared with the admin router.
    pub fn auth(&self) -> Arc<timelake_auth::Auth> {
        self.auth.clone()
    }

    /// Runtime FR-7 policy surface (backs /admin/retention).
    pub fn retention_policies(&self) -> Vec<(String, u64)> {
        self.retention.read().expect("retention lock").clone()
    }

    pub fn set_retention(&self, table: &str, seconds: u64) -> Result<(), String> {
        let mut r = self.retention.write().expect("retention lock");
        match r.iter_mut().find(|(t, _)| t == table) {
            Some(entry) => entry.1 = seconds,
            None => r.push((table.to_string(), seconds)),
        }
        // persisted under the write lock so concurrent admin edits
        // cannot land in the store out of order
        Self::persist_retention(&self.store, &r)
    }

    pub fn remove_retention(&self, table: &str) -> Result<(), String> {
        let mut r = self.retention.write().expect("retention lock");
        r.retain(|(t, _)| t != table);
        Self::persist_retention(&self.store, &r)
    }

    fn persist_retention(store: &Arc<dyn Store>, policies: &[(String, u64)]) -> Result<(), String> {
        let map: std::collections::BTreeMap<&str, u64> =
            policies.iter().map(|(t, s)| (t.as_str(), *s)).collect();
        store
            .put(
                RETENTION_CONFIG_PATH,
                &serde_json::to_vec_pretty(&map).expect("retention json"),
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

    fn write_lp(
        &self,
        db: &str,
        body: &[u8],
        precision: Option<&str>,
    ) -> Result<usize, WriteError> {
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
    }

    async fn sql(
        &self,
        db: String,
        query: String,
        authorizations: Vec<String>,
    ) -> Result<Value, String> {
        let batches = self.sql_batches(&db, &query, authorizations).await?;
        Ok(batches_to_json(&batches))
    }

    fn metrics_text(&self) -> String {
        self.metrics_text_impl()
    }

    fn retention_policies(&self) -> Vec<(String, u64)> {
        Engine::retention_policies(self)
    }

    fn set_retention(&self, table: &str, seconds: u64) -> Result<(), String> {
        Engine::set_retention(self, table, seconds)
    }

    fn remove_retention(&self, table: &str) -> Result<(), String> {
        Engine::remove_retention(self, table)
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
    ) -> Result<Vec<timelake_query::QueryBatch>, String> {
        let names = self.table_names(db);
        if names.is_empty() {
            return Err(format!(
                "database '{db}' does not exist (write to it first)"
            ));
        }

        let session = QuerySession::with_authorizations(authorizations);
        let mut tables: Vec<(String, Arc<dyn timelake_query::DfTableProvider>)> =
            Vec::with_capacity(names.len());
        for name in names {
            let mut buffer: Vec<timelake_query::QueryBatch> = {
                let dbs = self.dbs.read().expect("dbs lock");
                match dbs.get(db).and_then(|t| t.get(&name)) {
                    Some(buf) if buf.row_count() > 0 => vec![buf.snapshot()?],
                    _ => Vec::new(),
                }
            };
            // rows mid-flush: MUST be read before the catalog, so the
            // race with a completing flush errs toward a transient
            // duplicate, never a vanish (see the `flushing` field)
            if let Some(held) = self
                .flushing
                .read()
                .expect("flushing lock")
                .get(&(db.to_string(), name.clone()))
            {
                buffer.push(held.clone());
            }
            let files = self.catalog.files_for(db, &name);
            if buffer.is_empty() && files.is_empty() {
                continue;
            }
            // merged schema: registry (covers files + past flushes) ∪ buffer
            let mut schema = self.table_schema(db, &name);
            if let Some(b) = buffer.first() {
                schema = Some(timelake_query::schema_union(schema, b.schema())?);
            }
            let Some(schema) = schema else { continue };

            let provider = timelake_query::provider::LazyTable::new(
                name.clone(),
                schema,
                buffer,
                files,
                self.store.clone(),
                std::time::Duration::from_secs(self.cfg.query_timeout_secs),
                self.query_env.runtime.memory_pool.clone(),
                self.meta_cache.clone(),
                session.clone(),
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
        let (wal, frames) = Wal::open(dir)?;
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
    /// replica WAL into the engine and flush. Overlap with rows the dead
    /// peer had already flushed to S3 is safe — LWW dedup (FR-5) collapses
    /// it at compaction. Idempotent: recovering twice re-applies the same
    /// rows to the same primary keys. Returns the frame count replayed.
    ///
    /// Phase-2 scope: recovery is an explicit operation (the drill and, in
    /// production, an operator or the router on a confirmed peer death).
    /// Automatic health-triggered failover is a later cluster phase.
    pub fn recover_from_replica(&self) -> std::io::Result<usize> {
        let dir = self
            .replica_wal_dir
            .read()
            .expect("replica dir lock")
            .clone()
            .ok_or_else(|| std::io::Error::other("replica WAL not enabled"))?;
        // Reopen to read the full durable set. The peer is dead in the
        // recovery scenario, so no new frames race this.
        let (_wal, frames) = Wal::open(&dir)?;
        let n = frames.len();
        for (db, mult, body) in frames {
            let text = String::from_utf8_lossy(&body);
            if let Err(e) = self.apply(&db, &text, mult, 0) {
                tracing::warn!(db, error = %e, "skipping unrecoverable replica frame");
            }
        }
        self.flush_all().map_err(std::io::Error::other)?;
        self.cl2_recovered.fetch_add(n as u64, Ordering::Relaxed);
        tracing::info!(
            frames = n,
            "CL2 recovery complete: peer's writes replayed and flushed"
        );
        Ok(n)
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
             # TYPE timelake_retention_drops_total counter\ntimelake_retention_drops_total {}\n\
             # TYPE timelake_encryption_enabled gauge\ntimelake_encryption_enabled {}\n\
             # TYPE timelake_visibility_rows_filtered_total counter\n\
             timelake_visibility_rows_filtered_total {}\n\
             # TYPE timelake_admin_default_credential_active gauge\n\
             timelake_admin_default_credential_active {}\n\
             # TYPE timelake_admin_logins_total counter\n\
             timelake_admin_logins_total {}\n\
             # TYPE timelake_admin_login_failures_total counter\n\
             timelake_admin_login_failures_total {}\n{}{}{}{}{}{}{}",
            self.lines_total.load(Ordering::Relaxed),
            self.flushes_total.load(Ordering::Relaxed),
            self.catalog.file_count(),
            self.catalog.commit_conflicts(),
            n_dbs,
            n_tables,
            n_rows,
            wal_bytes,
            self.compactions_total.load(Ordering::Relaxed),
            self.retention_drops_total.load(Ordering::Relaxed),
            if self.store_encrypted { 1 } else { 0 },
            self.visibility_filtered.load(Ordering::Relaxed),
            if self.auth.default_credential_active() {
                1
            } else {
                0
            },
            self.auth.logins_total.load(Ordering::Relaxed),
            self.auth.login_failures_total.load(Ordering::Relaxed),
            client_ca_lines,
            data_auth_lines,
            auth_split_lines,
            kms_lines,
            s3_lines,
            tls_lines,
            cl2_lines,
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

    fn query_batches<'a>(
        &'a self,
        db: String,
        sql: String,
        authorizations: Vec<String>,
    ) -> timelake_flight::SqlFuture<'a> {
        Box::pin(async move { self.sql_batches(&db, &sql, authorizations).await })
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
    timelake_api::app(engine, auth, false)
}

/// The intra-cluster listener for an ingester (CL-2), bound to
/// `TIMELAKE_CLUSTER_ADDR`. NOT the public data port: it carries only
/// replication and recovery, and at C3 it moves behind required mTLS. There
/// is no data-plane auth here — trust is the private network now, the peer
/// certificate later.
pub fn internal_router(engine: Arc<Engine>) -> axum::Router {
    axum::Router::new()
        .route(
            "/internal/v1/replicate",
            axum::routing::post(internal_replicate),
        )
        .route(
            "/internal/v1/recover",
            axum::routing::post(internal_recover),
        )
        .route("/internal/v1/health", axum::routing::get(|| async { "ok" }))
        .with_state(engine)
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
    timelake_api::app(engine, auth.clone(), true).merge(
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
        query_timeout_secs: env("TIMELAKE_QUERY_TIMEOUT_SECS", d.query_timeout_secs),
        gc_grace_secs: env("TIMELAKE_GC_GRACE_SECS", d.gc_grace_secs),
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
