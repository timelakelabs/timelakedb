//! Catalog — the manifest log that makes the object store the source of
//! truth (CL-1). Every state change is an append-only JSON manifest
//! entry under `catalog/manifest/`; boot replays the log in order. v1 is
//! single-writer (commits are sequential); CL-2/3 upgrade `commit` to
//! conditional-put CAS on the head without touching callers.
//!
//! M2 scope: add_files commits from flush. Checkpoints, retention drops,
//! and schema entries arrive with their milestones.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use timelord_store::Store;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileMeta {
    pub db: String,
    pub table: String,
    /// UTC hour partition, e.g. "2026080809"
    pub partition: String,
    /// object path in the Store
    pub path: String,
    pub rows: u64,
    pub size_bytes: u64,
    pub min_ts_ns: i64,
    pub max_ts_ns: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ManifestEntry {
    seq: u64,
    add_files: Vec<FileMeta>,
    /// Object paths superseded by this commit (compaction merges,
    /// retention drops). Removals apply before adds on replay.
    #[serde(default)]
    remove_paths: Vec<String>,
}

pub struct Catalog<S: Store> {
    store: S,
    seq: AtomicU64,
    files: Mutex<HashMap<(String, String), Vec<FileMeta>>>,
}

impl<S: Store> Catalog<S> {
    /// Load by replaying the manifest log (sorted list = seq order).
    pub fn load(store: S) -> std::io::Result<Catalog<S>> {
        let mut files: HashMap<(String, String), Vec<FileMeta>> = HashMap::new();
        let mut seq = 0u64;
        for path in store.list("catalog/manifest")? {
            let bytes = store.get(&path)?;
            let entry: ManifestEntry = serde_json::from_slice(&bytes).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("manifest {path}: {e}"),
                )
            })?;
            seq = seq.max(entry.seq);
            if !entry.remove_paths.is_empty() {
                for list in files.values_mut() {
                    list.retain(|f| !entry.remove_paths.contains(&f.path));
                }
            }
            for f in entry.add_files {
                files
                    .entry((f.db.clone(), f.table.clone()))
                    .or_default()
                    .push(f);
            }
        }
        Ok(Catalog {
            store,
            seq: AtomicU64::new(seq),
            files: Mutex::new(files),
        })
    }

    /// Durably commit newly flushed files, then apply in memory.
    pub fn commit_add(&self, add_files: Vec<FileMeta>) -> std::io::Result<u64> {
        self.commit(add_files, Vec::new())
    }

    /// Durably commit a replacement (compaction) or drop (retention):
    /// removals apply before adds, atomically from readers' perspective.
    pub fn commit(
        &self,
        add_files: Vec<FileMeta>,
        remove_paths: Vec<String>,
    ) -> std::io::Result<u64> {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let entry = ManifestEntry {
            seq,
            add_files: add_files.clone(),
            remove_paths: remove_paths.clone(),
        };
        let path = format!("catalog/manifest/{seq:012}.json");
        self.store
            .put(&path, &serde_json::to_vec(&entry).expect("manifest json"))?;
        let mut files = self.files.lock().expect("catalog lock");
        if !remove_paths.is_empty() {
            for list in files.values_mut() {
                list.retain(|f| !remove_paths.contains(&f.path));
            }
        }
        for f in add_files {
            files
                .entry((f.db.clone(), f.table.clone()))
                .or_default()
                .push(f);
        }
        Ok(seq)
    }

    /// All files, for compaction/retention planning.
    pub fn all_files(&self) -> Vec<FileMeta> {
        self.files
            .lock()
            .expect("catalog lock")
            .values()
            .flatten()
            .cloned()
            .collect()
    }

    pub fn files_for(&self, db: &str, table: &str) -> Vec<FileMeta> {
        self.files
            .lock()
            .expect("catalog lock")
            .get(&(db.to_string(), table.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    /// Tables known to the catalog for a database (buffer may know more).
    pub fn tables_for(&self, db: &str) -> Vec<String> {
        let files = self.files.lock().expect("catalog lock");
        let mut out: Vec<String> = files
            .keys()
            .filter(|(d, _)| d == db)
            .map(|(_, t)| t.clone())
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// Databases known to the catalog (buffer may know more).
    pub fn databases(&self) -> Vec<String> {
        let files = self.files.lock().expect("catalog lock");
        let mut out: Vec<String> = files.keys().map(|(d, _)| d.clone()).collect();
        out.sort();
        out.dedup();
        out
    }

    pub fn file_count(&self) -> usize {
        self.files
            .lock()
            .expect("catalog lock")
            .values()
            .map(Vec::len)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use timelord_store::LocalStore;

    fn meta(db: &str, table: &str, part: &str, path: &str) -> FileMeta {
        FileMeta {
            db: db.into(),
            table: table.into(),
            partition: part.into(),
            path: path.into(),
            rows: 10,
            size_bytes: 100,
            min_ts_ns: 1,
            max_ts_ns: 2,
        }
    }

    #[test]
    fn commit_survives_reload() {
        let dir = tempfile::tempdir().unwrap();
        {
            let cat = Catalog::load(LocalStore::new(dir.path()).unwrap()).unwrap();
            cat.commit_add(vec![meta(
                "poc",
                "pipeline_events",
                "2026080800",
                "a.parquet",
            )])
            .unwrap();
            cat.commit_add(vec![
                meta("poc", "pipeline_events", "2026080801", "b.parquet"),
                meta("poc", "host_metrics", "2026080801", "c.parquet"),
            ])
            .unwrap();
        }
        let cat = Catalog::load(LocalStore::new(dir.path()).unwrap()).unwrap();
        assert_eq!(cat.files_for("poc", "pipeline_events").len(), 2);
        assert_eq!(cat.files_for("poc", "host_metrics").len(), 1);
        assert_eq!(
            cat.tables_for("poc"),
            vec!["host_metrics", "pipeline_events"]
        );
        assert_eq!(cat.file_count(), 3);
        // seq continues after reload
        cat.commit_add(vec![meta("poc", "disk_metrics", "2026080802", "d.parquet")])
            .unwrap();
        assert_eq!(cat.file_count(), 4);
    }
}
