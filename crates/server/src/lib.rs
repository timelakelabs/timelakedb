//! TimelordDB server — M1: the ingest path is real.
//!
//! Write path (ARCHITECTURE §5): parse → WAL (durable before 204, RR-3)
//! → mutable buffer. Read path: buffer snapshots → DataFusion under a
//! bounded memory pool (RR-1) → JSON rows on /api/sql.
//!
//! M1 honesty notes, in code where they belong:
//! - WAL is fsync-per-request; group-commit windows are M4 tuning.
//! - No Parquet yet (M2): everything lives in buffer memory and the WAL
//!   grows unbounded — fine for smoke scale, called out in /metrics.
//! - No PK dedup yet (FR-5, M2): a retried request double-applies.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

use serde_json::Value;
use timelord_api::WriteError;
use timelord_buffer::TableBuffer;
use timelord_ingest::{parse_lines, precision_multiplier};
use timelord_query::{QuerySession, batches_to_json, run_sql};
use timelord_wal::Wal;

pub struct Engine {
    dbs: RwLock<HashMap<String, HashMap<String, TableBuffer>>>,
    wal: Mutex<Wal>,
    lines_total: AtomicU64,
    query_mem_bytes: usize,
    data_dir: PathBuf,
}

impl Engine {
    /// Open the engine: WAL replay happens BEFORE serving (RR-3 — writes
    /// are accepted as soon as this returns).
    pub fn open(data_dir: &Path, query_mem_bytes: usize) -> std::io::Result<Arc<Engine>> {
        let (wal, frames) = Wal::open(&data_dir.join("wal"))?;
        let engine = Engine {
            dbs: RwLock::new(HashMap::new()),
            wal: Mutex::new(wal),
            lines_total: AtomicU64::new(0),
            query_mem_bytes,
            data_dir: data_dir.to_path_buf(),
        };
        let n = frames.len();
        for (db, mult, body) in frames {
            let body = String::from_utf8_lossy(&body);
            if let Err(e) = engine.apply(&db, &body, mult, 0) {
                // A frame that applied before a crash must not brick the
                // node; log and continue (the data made it to WAL, the
                // conflict is deterministic and was reported at write time).
                tracing::warn!(db, error = %e, "skipping unreplayable WAL frame");
            }
        }
        tracing::info!(frames = n, "WAL replay complete");
        Ok(Arc::new(engine))
    }

    /// Parse and apply to buffers. `default_ts_ns` of 0 is only used
    /// during replay, where lines carrying timestamps is the norm.
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

        // durable before 204 (ack contract, ARCHITECTURE §5)
        self.wal
            .lock()
            .expect("wal lock")
            .append(db, mult, body)
            .map_err(|e| WriteError::Internal(format!("wal append: {e}")))?;

        self.apply(db, text, mult, Self::now_ns())
            .map_err(WriteError::BadRequest)
    }

    async fn sql(&self, db: String, query: String) -> Result<Value, String> {
        // snapshot under the read lock, query outside it (PR-9)
        let tables: Vec<(String, _)> = {
            let dbs = self.dbs.read().expect("dbs lock");
            let Some(tables) = dbs.get(&db) else {
                return Err(format!("database '{db}' does not exist (write to it first)"));
            };
            tables
                .iter()
                .map(|(name, buf)| buf.snapshot().map(|b| (name.clone(), b)))
                .collect::<Result<_, _>>()?
        };
        let session = QuerySession::default();
        let batches = run_sql(&session, tables, &query, self.query_mem_bytes).await?;
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
             # TYPE timelord_databases gauge\ntimelord_databases {}\n\
             # TYPE timelord_tables gauge\ntimelord_tables {}\n\
             # TYPE timelord_buffer_rows gauge\ntimelord_buffer_rows {}\n\
             # TYPE timelord_wal_bytes gauge\ntimelord_wal_bytes {}\n",
            self.lines_total.load(Ordering::Relaxed),
            n_dbs,
            n_tables,
            n_rows,
            wal_bytes,
        )
    }
}

impl Engine {
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}

pub fn app(engine: Arc<Engine>) -> axum::Router {
    timelord_api::app(engine)
}
