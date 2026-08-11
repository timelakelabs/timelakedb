//! Catalog — the manifest log that makes the object store the source of
//! truth (CL-1). Every state change is an append-only JSON manifest
//! entry under `catalog/manifest/`; boot replays the log in order.
//!
//! Commits are **conditional-put CAS on the next sequence key** (C1,
//! ARCHITECTURE §12.3), so two writers pointed at one object store cannot
//! lose each other's work. Each commit claims `catalog/manifest/{seq}.json`
//! with `put_if_absent`; the loser of a race replays the winner's entry and
//! retries at the new head. Before this, `commit` used a plain `put` keyed
//! by an in-process counter: two writers replaying the same log computed the
//! same next `seq`, both wrote the same key, and the second silently
//! overwrote the first — its data files left orphaned in the store and
//! invisible to every query. That is P0-4.
//!
//! Scope note: this makes concurrent commits SAFE (no loss), not concurrent
//! reads FRESH. A node folds in another writer's entries when it collides on
//! a commit; polling for external commits between commits is a later,
//! separate concern (C2/C3).

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use timelake_store::Store;

/// A commit that keeps losing CAS races is a sign of pathological contention
/// or a bug, not normal operation — each retry first catches up to the true
/// head, so honest contention converges in a handful of rounds. This cap
/// turns an infinite loop into a loud error instead.
const MAX_COMMIT_ATTEMPTS: u32 = 100;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestEntry {
    seq: u64,
    add_files: Vec<FileMeta>,
    /// Object paths superseded by this commit (compaction merges,
    /// retention drops). Removals apply before adds on replay.
    #[serde(default)]
    remove_paths: Vec<String>,
}

/// Read and parse one manifest object, tagging the path on failure so a
/// corrupt entry is diagnosable.
fn read_entry<S: Store>(store: &S, path: &str) -> std::io::Result<ManifestEntry> {
    let bytes = store.get(path)?;
    serde_json::from_slice(&bytes).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("manifest {path}: {e}"),
        )
    })
}

/// Fold one entry into the in-memory file index: removals first (so a
/// compaction's replacement is atomic to a reader), then adds.
fn apply_entry(files: &mut HashMap<(String, String), Vec<FileMeta>>, entry: ManifestEntry) {
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

pub struct Catalog<S: Store> {
    store: S,
    seq: AtomicU64,
    files: Mutex<HashMap<(String, String), Vec<FileMeta>>>,
    /// Serializes this process's own commits so seq allocation, CAS, and the
    /// in-memory apply are one critical section. The CAS handles the
    /// *inter*-process race; this handles the intra-process one, and keeps
    /// the retry loop from fighting itself.
    commit_lock: Mutex<()>,
    /// How many times a commit lost a CAS race and had to catch up + retry.
    /// Zero on a single-writer deployment; climbing means real contention —
    /// worth a metric so it is visible rather than inferred.
    commit_conflicts: AtomicU64,
}

fn manifest_path(seq: u64) -> String {
    format!("catalog/manifest/{seq:012}.json")
}

impl<S: Store> Catalog<S> {
    /// Load by replaying the manifest log (sorted list = seq order).
    pub fn load(store: S) -> std::io::Result<Catalog<S>> {
        let mut files: HashMap<(String, String), Vec<FileMeta>> = HashMap::new();
        let mut seq = 0u64;
        for path in store.list("catalog/manifest")? {
            let entry = read_entry(&store, &path)?;
            seq = seq.max(entry.seq);
            apply_entry(&mut files, entry);
        }
        Ok(Catalog {
            store,
            seq: AtomicU64::new(seq),
            files: Mutex::new(files),
            commit_lock: Mutex::new(()),
            commit_conflicts: AtomicU64::new(0),
        })
    }

    /// Durably commit newly flushed files, then apply in memory.
    pub fn commit_add(&self, add_files: Vec<FileMeta>) -> std::io::Result<u64> {
        self.commit(add_files, Vec::new())
    }

    /// Durably commit a replacement (compaction) or drop (retention):
    /// removals apply before adds, atomically from readers' perspective.
    ///
    /// The commit is a CAS on the next sequence key. If another writer holds
    /// it, we replay whatever they (and anyone else) appended past our head,
    /// fold it into our in-memory state, and retry at the new head — so no
    /// commit can overwrite another, and the loser ends up aware of the
    /// winner's files rather than blind to them.
    pub fn commit(
        &self,
        add_files: Vec<FileMeta>,
        remove_paths: Vec<String>,
    ) -> std::io::Result<u64> {
        let _commit = self.commit_lock.lock().expect("commit lock");
        for _ in 0..MAX_COMMIT_ATTEMPTS {
            let seq = self.seq.load(Ordering::SeqCst) + 1;
            let entry = ManifestEntry {
                seq,
                add_files: add_files.clone(),
                remove_paths: remove_paths.clone(),
            };
            let bytes = serde_json::to_vec(&entry).expect("manifest json");
            if self.store.put_if_absent(&manifest_path(seq), &bytes)? {
                // We own this slot. Advance the head and apply our own entry.
                self.seq.store(seq, Ordering::SeqCst);
                let mut files = self.files.lock().expect("catalog lock");
                apply_entry(&mut files, entry);
                return Ok(seq);
            }
            // Lost the race: someone else holds `seq`. Learn what they wrote,
            // advance past it, and try again at the new head.
            self.commit_conflicts.fetch_add(1, Ordering::Relaxed);
            self.catch_up()?;
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::ResourceBusy,
            format!(
                "catalog commit lost {MAX_COMMIT_ATTEMPTS} CAS races in a row — \
                 sustained contention or a stuck writer on the same manifest prefix"
            ),
        ))
    }

    /// The highest manifest sequence this replica has applied. A querier
    /// (CL-3) compares it against an ingester's head to decide whether its
    /// view is already fresh enough to answer, so it is part of the
    /// read-freshness contract, not just diagnostics.
    pub fn head(&self) -> u64 {
        self.seq.load(Ordering::SeqCst)
    }

    /// Fold every manifest entry newer than our known head into memory and
    /// advance `seq`. Called after a CAS collision; safe to call when there
    /// is nothing new (it just re-confirms the head).
    ///
    /// Public because a read-only replica tails the log with it: a querier
    /// holds no WAL and commits nothing, so this is the *only* way its view
    /// of the shared store advances.
    pub fn catch_up(&self) -> std::io::Result<()> {
        let head = self.seq.load(Ordering::SeqCst);
        let mut newer: Vec<(u64, ManifestEntry)> = Vec::new();
        for path in self.store.list("catalog/manifest")? {
            let entry = read_entry(&self.store, &path)?;
            if entry.seq > head {
                newer.push((entry.seq, entry));
            }
        }
        // Apply in seq order — removals-before-adds within an entry is
        // handled by apply_entry; across entries, order is the log order.
        newer.sort_by_key(|(seq, _)| *seq);
        let mut files = self.files.lock().expect("catalog lock");
        let mut new_head = head;
        for (seq, entry) in newer {
            apply_entry(&mut files, entry);
            new_head = new_head.max(seq);
        }
        drop(files);
        self.seq.store(new_head, Ordering::SeqCst);
        Ok(())
    }

    /// CAS collisions since load — 0 without a second writer (SR-4 metric).
    pub fn commit_conflicts(&self) -> u64 {
        self.commit_conflicts.load(Ordering::Relaxed)
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
    use timelake_store::LocalStore;

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

    /// Two catalogs over ONE store, as two processes pointed at one bucket.
    /// Before the CAS commit, both computed the same next seq and the second
    /// `put` clobbered the first — the clobbered writer's file was orphaned.
    /// Now every committed file must survive a fresh replay.
    #[test]
    fn two_writers_on_one_store_lose_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = || LocalStore::new(dir.path()).unwrap();

        // Both replay the same (empty) log, so both believe head == 0 and
        // will target seq 1 first — the exact collision that used to lose.
        let a = Catalog::load(store()).unwrap();
        let b = Catalog::load(store()).unwrap();

        let sa = a
            .commit_add(vec![meta("poc", "t", "2026080800", "a.parquet")])
            .unwrap();
        let sb = b
            .commit_add(vec![meta("poc", "t", "2026080800", "b.parquet")])
            .unwrap();
        // Distinct sequence numbers: the CAS forced B off seq 1 onto 2.
        assert_ne!(sa, sb, "two writers must not share a manifest slot");
        assert!(
            b.commit_conflicts() >= 1,
            "B should have lost the seq-1 race"
        );

        // Interleave a few more, each writer unaware of the other until it
        // collides. No commit may vanish.
        a.commit_add(vec![meta("poc", "t", "2026080801", "a2.parquet")])
            .unwrap();
        b.commit_add(vec![meta("poc", "t", "2026080801", "b2.parquet")])
            .unwrap();

        // The store is the source of truth: a cold replay sees all four.
        let fresh = Catalog::load(store()).unwrap();
        let paths: std::collections::BTreeSet<String> = fresh
            .files_for("poc", "t")
            .into_iter()
            .map(|f| f.path)
            .collect();
        assert_eq!(
            paths,
            ["a.parquet", "a2.parquet", "b.parquet", "b2.parquet"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        );
        assert_eq!(fresh.file_count(), 4, "no committed file was orphaned");
    }

    /// A writer that collides catches up: after the race, the loser's own
    /// in-memory view includes the winner's file, not just the store's.
    #[test]
    fn the_loser_learns_the_winners_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = || LocalStore::new(dir.path()).unwrap();
        let a = Catalog::load(store()).unwrap();
        let b = Catalog::load(store()).unwrap();

        a.commit_add(vec![meta("poc", "t", "2026080800", "a.parquet")])
            .unwrap();
        // B still thinks head == 0; committing forces a catch-up over A's entry.
        b.commit_add(vec![meta("poc", "t", "2026080800", "b.parquet")])
            .unwrap();

        let bpaths: std::collections::BTreeSet<String> = b
            .files_for("poc", "t")
            .into_iter()
            .map(|f| f.path)
            .collect();
        assert!(
            bpaths.contains("a.parquet") && bpaths.contains("b.parquet"),
            "after colliding, B must see both files, got {bpaths:?}"
        );
    }

    /// Removals (compaction/retention) must survive the CAS path too: a
    /// replacement commit that loses a race still applies its remove on retry.
    #[test]
    fn removals_survive_a_cas_retry() {
        let dir = tempfile::tempdir().unwrap();
        let store = || LocalStore::new(dir.path()).unwrap();

        // Seed one file via a first writer.
        let seed = Catalog::load(store()).unwrap();
        seed.commit_add(vec![meta("poc", "t", "2026080800", "old.parquet")])
            .unwrap();

        // Two fresh writers both at head 1. A adds; B compacts old->new.
        let a = Catalog::load(store()).unwrap();
        let b = Catalog::load(store()).unwrap();
        a.commit_add(vec![meta("poc", "t", "2026080801", "concurrent.parquet")])
            .unwrap();
        b.commit(
            vec![meta("poc", "t", "2026080800", "new.parquet")],
            vec!["old.parquet".to_string()],
        )
        .unwrap();

        let fresh = Catalog::load(store()).unwrap();
        let paths: std::collections::BTreeSet<String> = fresh
            .files_for("poc", "t")
            .into_iter()
            .map(|f| f.path)
            .collect();
        assert_eq!(
            paths,
            ["concurrent.parquet", "new.parquet"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            "old.parquet must be gone; both the add and the concurrent add kept"
        );
    }
}
