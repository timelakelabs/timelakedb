//! TimelordDB server — M2: a real storage engine.
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
use timelord_api::WriteError;
use timelord_buffer::{TableBuffer, flush};
use timelord_catalog::{Catalog, FileMeta};
use timelord_ingest::{parse_lines, precision_multiplier};
use timelord_query::{QuerySession, batches_to_json, run_sql};
use timelord_store::{LocalStore, Store};
use timelord_wal::Wal;

#[derive(Clone, Copy, Debug)]
pub struct EngineConfig {
    pub query_mem_bytes: usize,
    pub flush_rows: usize,
    pub flush_age_secs: u64,
    pub wal_max_bytes: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            query_mem_bytes: 1 << 30,    // 1 GiB RR-1 pool
            flush_rows: 50_000,          // L0 trigger
            flush_age_secs: 60,
            wal_max_bytes: 2 << 30,      // RR-3 replay bound / RR-5 backpressure
        }
    }
}

pub struct Engine {
    dbs: RwLock<HashMap<String, HashMap<String, TableBuffer>>>,
    /// Writers hold this shared for append+apply; flush holds it
    /// exclusively for the swap+rotate instant, so no write can land in a
    /// sealed WAL generation but miss the swapped buffers.
    ingest_gate: RwLock<()>,
    wal: Mutex<Wal>,
    store: Arc<LocalStore>,
    catalog: Catalog<Arc<LocalStore>>,
    cfg: EngineConfig,
    lines_total: AtomicU64,
    flushes_total: AtomicU64,
    file_seq: AtomicU64,
}

impl Engine {
    /// Open the engine: catalog load + WAL replay happen BEFORE serving
    /// (RR-3 — writes are accepted as soon as this returns).
    pub fn open(data_dir: &Path, cfg: EngineConfig) -> std::io::Result<Arc<Engine>> {
        let store = Arc::new(LocalStore::new(&data_dir.join("objects"))?);
        let catalog = Catalog::load(store.clone())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let (wal, frames) = Wal::open(&data_dir.join("wal"))?;

        let engine = Engine {
            dbs: RwLock::new(HashMap::new()),
            ingest_gate: RwLock::new(()),
            wal: Mutex::new(wal),
            store,
            catalog,
            cfg,
            lines_total: AtomicU64::new(0),
            flushes_total: AtomicU64::new(0),
            file_seq: AtomicU64::new(0),
        };
        let n = frames.len();
        for (db, mult, body) in frames {
            let body = String::from_utf8_lossy(&body);
            if let Err(e) = engine.apply(&db, &body, mult, 0) {
                tracing::warn!(db, error = %e, "skipping unreplayable WAL frame");
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
        let mut dbs = self.dbs.write().expect("dbs lock");
        let tables = dbs.entry(db.to_string()).or_default();
        let n = rows.len();
        for row in &rows {
            tables.entry(row.table.clone()).or_default().append(row)?;
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

        // 2. encode + upload through the Store chokepoint
        let mut metas = Vec::new();
        for (db, table, buf) in owned {
            let batch = buf.snapshot()?;
            for (partition, part) in flush::prepare(&batch)? {
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
                    db: db.clone(),
                    table: table.clone(),
                    partition,
                    path,
                    rows: part.num_rows() as u64,
                    size_bytes: bytes.len() as u64,
                    min_ts_ns: min_ts,
                    max_ts_ns: max_ts,
                });
            }
        }

        // 3. durable manifest commit, then 4. reclaim sealed WAL gens
        let n = metas.len();
        self.catalog
            .commit_add(metas)
            .map_err(|e| format!("catalog commit: {e}"))?;
        self.wal
            .lock()
            .expect("wal lock")
            .delete_generations_upto(sealed_gen)
            .map_err(|e| format!("wal reclaim: {e}"))?;
        self.flushes_total.fetch_add(1, Ordering::Relaxed);
        tracing::info!(files = n, "flush complete");
        Ok(n)
    }
}

impl timelord_api::Engine for Engine {
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
        self.apply(db, text, mult, Self::now_ns())
            .map_err(WriteError::BadRequest)
    }

    async fn sql(&self, db: String, query: String) -> Result<Value, String> {
        // gather table names from buffer AND catalog
        let mut names: Vec<String> = {
            let dbs = self.dbs.read().expect("dbs lock");
            dbs.get(&db).map(|t| t.keys().cloned().collect()).unwrap_or_default()
        };
        for t in self.catalog.tables_for(&db) {
            if !names.contains(&t) {
                names.push(t);
            }
        }
        if names.is_empty() {
            return Err(format!("database '{db}' does not exist (write to it first)"));
        }

        let mut tables: Vec<(String, Vec<_>)> = Vec::with_capacity(names.len());
        for name in names {
            let mut batches = Vec::new();
            {
                let dbs = self.dbs.read().expect("dbs lock");
                if let Some(buf) = dbs.get(&db).and_then(|t| t.get(&name)) {
                    if buf.row_count() > 0 {
                        batches.push(buf.snapshot()?);
                    }
                }
            }
            for meta in self.catalog.files_for(&db, &name) {
                let bytes = self
                    .store
                    .get(&meta.path)
                    .map_err(|e| format!("store get {}: {e}", meta.path))?;
                batches.extend(flush::read_parquet_bytes(bytes)?);
            }
            if !batches.is_empty() {
                tables.push((name, batches));
            }
        }

        let session = QuerySession::default();
        let batches = run_sql(&session, tables, &query, self.cfg.query_mem_bytes).await?;
        Ok(batches_to_json(&batches))
    }

    fn metrics_text(&self) -> String {
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
        format!(
            "# TYPE timelord_lines_written_total counter\n\
             timelord_lines_written_total {}\n\
             # TYPE timelord_flushes_total counter\ntimelord_flushes_total {}\n\
             # TYPE timelord_parquet_files gauge\ntimelord_parquet_files {}\n\
             # TYPE timelord_databases gauge\ntimelord_databases {}\n\
             # TYPE timelord_tables gauge\ntimelord_tables {}\n\
             # TYPE timelord_buffer_rows gauge\ntimelord_buffer_rows {}\n\
             # TYPE timelord_wal_bytes gauge\ntimelord_wal_bytes {}\n",
            self.lines_total.load(Ordering::Relaxed),
            self.flushes_total.load(Ordering::Relaxed),
            self.catalog.file_count(),
            n_dbs,
            n_tables,
            n_rows,
            wal_bytes,
        )
    }
}

pub struct EnginePaths;

impl Engine {
    pub fn config(&self) -> EngineConfig {
        self.cfg
    }
}

pub fn app(engine: Arc<Engine>) -> axum::Router {
    timelord_api::app(engine)
}

/// Parse engine config from environment (main + integration use).
pub fn config_from_env() -> EngineConfig {
    fn env<T: std::str::FromStr>(k: &str, d: T) -> T {
        std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
    }
    let d = EngineConfig::default();
    EngineConfig {
        query_mem_bytes: env("TIMELORD_QUERY_MEM_BYTES", d.query_mem_bytes),
        flush_rows: env("TIMELORD_FLUSH_ROWS", d.flush_rows),
        flush_age_secs: env("TIMELORD_FLUSH_AGE_SECS", d.flush_age_secs),
        wal_max_bytes: env("TIMELORD_WAL_MAX_BYTES", d.wal_max_bytes),
    }
}

/// Convenience used by main and tests.
pub fn data_dir_from_env() -> PathBuf {
    PathBuf::from(std::env::var("TIMELORD_DATA_DIR").unwrap_or_else(|_| "./data".to_string()))
}
