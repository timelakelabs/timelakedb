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
use timelord_query::{QuerySession, batches_to_json};
use timelord_store::{LocalStore, Store};
use timelord_wal::Wal;

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
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            query_mem_bytes: 1 << 30,    // 1 GiB RR-1 pool
            flush_rows: 50_000,          // L0 trigger
            flush_age_secs: 60,
            wal_max_bytes: 2 << 30,      // RR-3 replay bound / RR-5 backpressure
            compact_min_files: 4,
            retention: Vec::new(),
            max_concurrent_queries: 6,
            query_timeout_secs: 600,
            gc_grace_secs: 900,
        }
    }
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
            Some((table.trim().to_string(), num.parse::<u64>().ok()? * unit_secs))
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
    store: Arc<LocalStore>,
    catalog: Catalog<Arc<LocalStore>>,
    cfg: EngineConfig,
    /// Shared pool + admission + timeout (RR-1/RR-2) — ONE for all queries.
    query_env: timelord_query::QueryEnv,
    /// Full column set per (db, table): survives flushes and restarts so
    /// providers can present a stable merged schema without re-reading
    /// every footer per query.
    schemas: RwLock<HashMap<(String, String), timelord_query::QuerySchema>>,
    lines_total: AtomicU64,
    flushes_total: AtomicU64,
    compactions_total: AtomicU64,
    retention_drops_total: AtomicU64,
    file_seq: AtomicU64,
    /// Deferred deletions: (when superseded, path). Drained by run_gc
    /// after gc_grace_secs so in-flight catalog snapshots never dangle.
    pending_gc: Mutex<Vec<(std::time::Instant, String)>>,
    /// Immutable-footer cache: warm queries prune without fetching files.
    meta_cache: Arc<timelord_query::provider::MetaCache>,
    /// SEC-3: set when the listeners run TLS; feeds the expiry gauge and
    /// renewal-health metric so a failing rotation is visible (RR-5).
    tls: RwLock<Option<Arc<timelord_tls::RotatingCert>>>,
}

impl Engine {
    /// Open the engine: catalog load + WAL replay happen BEFORE serving
    /// (RR-3 — writes are accepted as soon as this returns).
    pub fn open(data_dir: &Path, cfg: EngineConfig) -> std::io::Result<Arc<Engine>> {
        let store = Arc::new(LocalStore::new(&data_dir.join("objects"))?);
        let catalog = Catalog::load(store.clone())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let (wal, frames) = Wal::open(&data_dir.join("wal"))?;

        let query_env = timelord_query::QueryEnv::new(
            cfg.query_mem_bytes,
            cfg.max_concurrent_queries,
            cfg.query_timeout_secs,
        );
        let engine = Engine {
            dbs: RwLock::new(HashMap::new()),
            ingest_gate: RwLock::new(()),
            wal: Mutex::new(wal),
            store,
            catalog,
            query_env,
            schemas: RwLock::new(HashMap::new()),
            cfg,
            lines_total: AtomicU64::new(0),
            flushes_total: AtomicU64::new(0),
            compactions_total: AtomicU64::new(0),
            retention_drops_total: AtomicU64::new(0),
            file_seq: AtomicU64::new(0),
            pending_gc: Mutex::new(Vec::new()),
            meta_cache: Arc::new(Default::default()),
            tls: RwLock::new(None),
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
                match engine.store.get(&path).map_err(|e| e.to_string()).and_then(|b| {
                    flush::read_parquet_bytes(b)
                        .map(|bs| bs.first().map(|b| b.schema()))
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
                let merged = timelord_query::schema_union(reg.get(&key).cloned(), schema)?;
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

        // 2. encode + upload through the Store chokepoint. One table's
        // failure must not discard the others' rows, so each is encoded
        // independently and a failure is recorded rather than propagated.
        let mut metas = Vec::new();
        let mut failed: Vec<String> = Vec::new();
        for (db, table, buf) in owned {
            match self.flush_one(&db, &table, buf) {
                Ok(mut m) => metas.append(&mut m),
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

    /// Encode and upload one table's buffer. Split out of [`Self::flush_all`]
    /// so a single bad buffer is contained.
    fn flush_one(&self, db: &str, table: &str, buf: TableBuffer) -> Result<Vec<FileMeta>, String> {
        let batch = buf.snapshot()?;
        // keep the registry current: flushed columns must remain
        // queryable after the buffer empties (and after restart)
        {
            let key = (db.to_string(), table.to_string());
            let mut reg = self.schemas.write().expect("schemas lock");
            let merged = timelord_query::schema_union(reg.get(&key).cloned(), batch.schema())?;
            reg.insert(key, merged);
        }
        let mut metas = Vec::new();
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
            let merged = timelord_compact::merge_files(batch_sets)?;
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
            self.compactions_total.fetch_add(done as u64, Ordering::Relaxed);
            tracing::info!(partitions = done, "compaction pass");
        }
        Ok(done)
    }

    /// FR-7: drop whole partitions past their table's retention window.
    pub fn enforce_retention(&self) -> Result<usize, String> {
        if self.cfg.retention.is_empty() {
            return Ok(0);
        }
        let now = Self::now_ns();
        let mut remove: Vec<String> = Vec::new();
        for f in self.catalog.all_files() {
            if let Some((_, secs)) = self
                .cfg
                .retention
                .iter()
                .find(|(table, _)| *table == f.table)
            {
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
            self.retention_drops_total.fetch_add(n as u64, Ordering::Relaxed);
            tracing::info!(files = n, "retention drop");
        }
        Ok(n)
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
        let batches = self.sql_batches(&db, &query).await?;
        Ok(batches_to_json(&batches))
    }

    fn metrics_text(&self) -> String {
        self.metrics_text_impl()
    }
}

impl Engine {
    /// Shared query path: /api/sql renders JSON from these batches and
    /// Flight SQL streams them natively (FR-8).
    pub async fn sql_batches(
        &self,
        db: &str,
        query: &str,
    ) -> Result<Vec<timelord_query::QueryBatch>, String> {
        // gather table names from buffer AND catalog
        let mut names: Vec<String> = {
            let dbs = self.dbs.read().expect("dbs lock");
            dbs.get(db).map(|t| t.keys().cloned().collect()).unwrap_or_default()
        };
        for t in self.catalog.tables_for(db) {
            if !names.contains(&t) {
                names.push(t);
            }
        }
        if names.is_empty() {
            return Err(format!("database '{db}' does not exist (write to it first)"));
        }

        let mut tables: Vec<(String, Arc<dyn timelord_query::DfTableProvider>)> =
            Vec::with_capacity(names.len());
        for name in names {
            let buffer: Vec<timelord_query::QueryBatch> = {
                let dbs = self.dbs.read().expect("dbs lock");
                match dbs.get(db).and_then(|t| t.get(&name)) {
                    Some(buf) if buf.row_count() > 0 => vec![buf.snapshot()?],
                    _ => Vec::new(),
                }
            };
            let files = self.catalog.files_for(db, &name);
            if buffer.is_empty() && files.is_empty() {
                continue;
            }
            // merged schema: registry (covers files + past flushes) ∪ buffer
            let key = (db.to_string(), name.clone());
            let mut schema = self.schemas.read().expect("schemas lock").get(&key).cloned();
            if let Some(b) = buffer.first() {
                schema = Some(timelord_query::schema_union(schema, b.schema())?);
            }
            let Some(schema) = schema else { continue };

            let store_dyn: Arc<dyn Store> = self.store.clone();
            let provider = timelord_query::provider::LazyTable::new(
                name.clone(),
                schema,
                buffer,
                files,
                store_dyn,
                std::time::Duration::from_secs(self.cfg.query_timeout_secs),
                self.query_env.runtime.memory_pool.clone(),
                self.meta_cache.clone(),
            );
            tables.push((name, Arc::new(provider)));
        }

        let session = QuerySession::default();
        timelord_query::run_sql_env(&self.query_env, &session, tables, query).await
    }

    /// Attach the TLS rotator so /metrics exports cert health (SEC-3).
    pub fn set_tls(&self, rot: Arc<timelord_tls::RotatingCert>) {
        *self.tls.write().expect("tls lock") = Some(rot);
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
        let tls_lines = match self.tls.read().expect("tls lock").as_ref() {
            Some(t) => format!(
                "# TYPE timelord_tls_cert_expiry_seconds gauge\n\
                 timelord_tls_cert_expiry_seconds {}\n\
                 # TYPE timelord_tls_last_reload_ok gauge\n\
                 timelord_tls_last_reload_ok {}\n",
                t.expires_in_secs(),
                if t.last_reload_ok() { 1 } else { 0 },
            ),
            None => String::new(),
        };
        format!(
            "# TYPE timelord_lines_written_total counter\n\
             timelord_lines_written_total {}\n\
             # TYPE timelord_flushes_total counter\ntimelord_flushes_total {}\n\
             # TYPE timelord_parquet_files gauge\ntimelord_parquet_files {}\n\
             # TYPE timelord_databases gauge\ntimelord_databases {}\n\
             # TYPE timelord_tables gauge\ntimelord_tables {}\n\
             # TYPE timelord_buffer_rows gauge\ntimelord_buffer_rows {}\n\
             # TYPE timelord_wal_bytes gauge\ntimelord_wal_bytes {}\n\
             # TYPE timelord_compactions_total counter\ntimelord_compactions_total {}\n\
             # TYPE timelord_retention_drops_total counter\ntimelord_retention_drops_total {}\n{}",
            self.lines_total.load(Ordering::Relaxed),
            self.flushes_total.load(Ordering::Relaxed),
            self.catalog.file_count(),
            n_dbs,
            n_tables,
            n_rows,
            wal_bytes,
            self.compactions_total.load(Ordering::Relaxed),
            self.retention_drops_total.load(Ordering::Relaxed),
            tls_lines,
        )
    }
}

impl Engine {
    pub fn config(&self) -> EngineConfig {
        self.cfg.clone()
    }
}

impl timelord_flight::SqlBackend for Engine {
    fn query_batches<'a>(&'a self, db: String, sql: String) -> timelord_flight::SqlFuture<'a> {
        Box::pin(async move { self.sql_batches(&db, &sql).await })
    }
}

pub fn app(engine: Arc<Engine>) -> axum::Router {
    timelord_api::app(engine)
}

/// The TLS-enabled router: the api surface plus the explicit rotation
/// trigger (SEC-3 mandates BOTH a file watcher and an admin endpoint;
/// reload-by-restart is forbidden). No auth at this milestone,
/// consistent with the rest of the PoC surface.
pub fn app_with_tls_admin(
    engine: Arc<Engine>,
    rot: Arc<timelord_tls::RotatingCert>,
) -> axum::Router {
    use axum::response::IntoResponse;
    let reload = move || {
        let rot = Arc::clone(&rot);
        async move {
            let res = tokio::task::spawn_blocking(move || rot.reload().map(|_| rot.expires_in_secs()))
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
                        "alarm": timelord_tls::RENEWAL_ALARM,
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
    timelord_api::app(engine)
        .merge(axum::Router::new().route("/admin/tls/reload", axum::routing::post(reload)))
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
        compact_min_files: env("TIMELORD_COMPACT_MIN_FILES", d.compact_min_files),
        retention: std::env::var("TIMELORD_RETENTION")
            .map(|s| parse_retention(&s))
            .unwrap_or_default(),
        max_concurrent_queries: env("TIMELORD_MAX_CONCURRENT_QUERIES", d.max_concurrent_queries),
        query_timeout_secs: env("TIMELORD_QUERY_TIMEOUT_SECS", d.query_timeout_secs),
        gc_grace_secs: env("TIMELORD_GC_GRACE_SECS", d.gc_grace_secs),
    }
}

/// Convenience used by main and tests.
pub fn data_dir_from_env() -> PathBuf {
    PathBuf::from(std::env::var("TIMELORD_DATA_DIR").unwrap_or_else(|_| "./data".to_string()))
}
