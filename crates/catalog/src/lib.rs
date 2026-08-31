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

/// A targeted-delete predicate (R-1). Rows of `table` in `db` that match ALL
/// of `tag_equals` AND fall in the half-open time range `[min_ts_ns,
/// max_ts_ns)` are deleted. An unset bound is open on that side; an empty
/// `tag_equals` matches every row in range (a pure time delete). Recorded in
/// the manifest log so it replays on restart and reaches every node exactly
/// as a file add does. `id` makes replay and CAS-retry idempotent;
/// `created_seq` is the manifest seq it committed at (R-1b uses it to know
/// which files predate the delete and must be rewritten).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Tombstone {
    pub id: String,
    pub db: String,
    pub table: String,
    #[serde(default)]
    pub tag_equals: Vec<(String, String)>,
    #[serde(default)]
    pub min_ts_ns: Option<i64>,
    #[serde(default)]
    pub max_ts_ns: Option<i64>,
    #[serde(default)]
    pub created_seq: u64,
}

/// A declared column of a table (#80). `tag` distinguishes a primary-key tag
/// (always a string) from a field. Types are the line-protocol field types.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    Float,
    Integer,
    Unsigned,
    Boolean,
    String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: ColumnType,
    #[serde(default)]
    pub tag: bool,
}

/// An explicitly declared table schema (#80), recorded in the manifest log by an
/// authorized CREATE so it is CAS-committed, replays on restart, and reaches
/// every node exactly as a file add does. A write is then validated against it
/// rather than the first writer winning the schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableSchema {
    pub db: String,
    pub table: String,
    pub columns: Vec<TableColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestEntry {
    seq: u64,
    add_files: Vec<FileMeta>,
    /// Object paths superseded by this commit (compaction merges,
    /// retention drops). Removals apply before adds on replay.
    #[serde(default)]
    remove_paths: Vec<String>,
    /// Targeted-delete predicates (R-1) recorded by this commit. `default`
    /// so every manifest written before R-1 still deserializes.
    #[serde(default)]
    tombstones: Vec<Tombstone>,
    /// Declared table schemas (#80) recorded by an authorized CREATE. `default`
    /// so every manifest written before this feature still deserializes.
    #[serde(default)]
    schema_decls: Vec<TableSchema>,
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
/// The first path that is no longer in the catalog, if any.
///
/// Returns the path rather than a bool so the error can name it. "Some
/// input vanished" sends someone reading the log hunting; naming the file
/// tells them which compaction lost, which is usually the whole question.
fn first_absent(
    files: &HashMap<(String, String), Vec<FileMeta>>,
    remove_paths: &[String],
) -> Option<String> {
    for path in remove_paths {
        if !files
            .values()
            .any(|list| list.iter().any(|f| &f.path == path))
        {
            return Some(path.clone());
        }
    }
    None
}

fn apply_entry(
    files: &mut HashMap<(String, String), Vec<FileMeta>>,
    tombstones: &mut Vec<Tombstone>,
    declared: &mut HashMap<(String, String), TableSchema>,
    entry: ManifestEntry,
) {
    // A re-declared schema replaces the prior one (last-wins), so a CREATE that
    // corrects a typo'd type is not stuck behind the first. Idempotent on replay
    // because the same entry yields the same map.
    for s in entry.schema_decls {
        declared.insert((s.db.clone(), s.table.clone()), s);
    }
    if !entry.remove_paths.is_empty() {
        for list in files.values_mut() {
            list.retain(|f| !entry.remove_paths.contains(&f.path));
        }
    }
    for f in entry.add_files {
        let list = files.entry((f.db.clone(), f.table.clone())).or_default();
        // Folding a log into a set: applying the same record twice must be
        // a no-op. The locking above is what makes double-application not
        // happen; this is what makes it harmless if some future caller
        // reaches here another way. A path is unique per commit, so it is
        // the identity to compare on.
        list.push(f);
    }
    // Same idempotency contract for tombstones — a delete predicate replayed
    // by `load`/`catch_up`, or re-seen after a CAS retry, must not accumulate
    // duplicates. `id` is the identity.
    for t in entry.tombstones {
        if !tombstones.iter().any(|x| x.id == t.id) {
            tombstones.push(t);
        }
    }
}

pub struct Catalog<S: Store> {
    store: S,
    seq: AtomicU64,
    files: Mutex<HashMap<(String, String), Vec<FileMeta>>>,
    /// Active targeted-delete predicates (R-1), folded from the manifest log.
    /// Locked AFTER `files` wherever both are held, so the two never deadlock.
    tombstones: Mutex<Vec<Tombstone>>,
    /// Explicitly declared table schemas (#80), folded from the manifest log.
    /// Locked AFTER `tombstones` wherever more than one is held.
    declared_schemas: Mutex<HashMap<(String, String), TableSchema>>,
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
        let mut tombstones: Vec<Tombstone> = Vec::new();
        let mut declared: HashMap<(String, String), TableSchema> = HashMap::new();
        let mut seq = 0u64;
        for path in store.list("catalog/manifest")? {
            let entry = read_entry(&store, &path)?;
            seq = seq.max(entry.seq);
            apply_entry(&mut files, &mut tombstones, &mut declared, entry);
        }
        Ok(Catalog {
            store,
            seq: AtomicU64::new(seq),
            files: Mutex::new(files),
            tombstones: Mutex::new(tombstones),
            declared_schemas: Mutex::new(declared),
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
        self.commit_guarded(add_files, remove_paths, Vec::new(), Vec::new(), false)
    }

    /// Commit a replacement whose inputs must still exist (C2 phase 5).
    ///
    /// `commit` is unconditional: it removes what it names, adds what it
    /// carries, and a removal of something already gone is a silent no-op.
    /// That is right for a flush, and wrong for a compaction.
    ///
    /// Two compactors merging one partition each read files F1..F4, each
    /// produce their own merged file, and each commit. The CAS serialises
    /// them, so no manifest entry is lost — but the loser retries with its
    /// ORIGINAL entry, its removals find nothing left to remove, and its
    /// merged file is added anyway. The partition ends up holding both
    /// merges: every row twice, committed by a mechanism designed to make
    /// concurrent writers safe. The CAS was never the gap; it guarantees
    /// no commit is *overwritten*, not that a commit is still *valid*.
    ///
    /// So a compaction commits through here instead. Inside the same
    /// critical section, after catching up to the true head, every path in
    /// `remove_paths` must still be present. If any has gone, another
    /// writer has already replaced these inputs and this merge is stale
    /// work: `AlreadyExists` is returned and nothing is written.
    ///
    /// This is the fence, and it is deliberately not a lease. A lease is a
    /// promise about wall-clock time across machines, and under skew or a
    /// long GC pause two nodes can both believe they hold one. This checks
    /// the thing that actually has to be true at the instant it has to be
    /// true, using state the commit already holds a lock on. A lease on top
    /// avoids the wasted merge; it is an optimisation, and its failure is
    /// safe because of this.
    pub fn commit_replace(
        &self,
        add_files: Vec<FileMeta>,
        remove_paths: Vec<String>,
    ) -> std::io::Result<u64> {
        self.commit_guarded(add_files, remove_paths, Vec::new(), Vec::new(), true)
    }

    /// Durably record a targeted-delete predicate (R-1) in the manifest log.
    /// The same CAS loop as any commit, so it composes with file adds/removes
    /// and replays/propagates identically. Returns the seq it landed at.
    pub fn commit_tombstone(&self, tombstone: Tombstone) -> std::io::Result<u64> {
        self.commit_guarded(Vec::new(), Vec::new(), vec![tombstone], Vec::new(), false)
    }

    fn commit_guarded(
        &self,
        add_files: Vec<FileMeta>,
        remove_paths: Vec<String>,
        tombstones: Vec<Tombstone>,
        schema_decls: Vec<TableSchema>,
        require_removals_present: bool,
    ) -> std::io::Result<u64> {
        let _commit = self.commit_lock.lock().expect("commit lock");
        for _ in 0..MAX_COMMIT_ATTEMPTS {
            // Checked on every attempt, not once before the loop: a lost
            // CAS race means someone else committed, and they are exactly
            // the writer who may have taken these inputs. Checking only
            // up front would validate against a view already stale by the
            // time it mattered.
            if require_removals_present {
                let files = self.files.lock().expect("catalog lock");
                if let Some(missing) = first_absent(&files, &remove_paths) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        format!(
                            "compaction input {missing} is gone: another writer already replaced these files, so this merge is stale and was not committed"
                        ),
                    ));
                }
            }
            let seq = self.seq.load(Ordering::SeqCst) + 1;
            // Stamp each tombstone with the seq it is actually landing at, so
            // `created_seq` stays correct even when a CAS retry bumps the head.
            let stamped: Vec<Tombstone> = tombstones
                .iter()
                .cloned()
                .map(|mut t| {
                    t.created_seq = seq;
                    t
                })
                .collect();
            let entry = ManifestEntry {
                seq,
                add_files: add_files.clone(),
                remove_paths: remove_paths.clone(),
                tombstones: stamped,
                schema_decls: schema_decls.clone(),
            };
            let bytes = serde_json::to_vec(&entry).expect("manifest json");
            if self.store.put_if_absent(&manifest_path(seq), &bytes)? {
                // We own this slot. Advance the head and apply our own entry.
                self.seq.store(seq, Ordering::SeqCst);
                let mut files = self.files.lock().expect("catalog lock");
                let mut tombs = self.tombstones.lock().expect("tombstone lock");
                let mut declared = self.declared_schemas.lock().expect("schema lock");
                apply_entry(&mut files, &mut tombs, &mut declared, entry);
                return Ok(seq);
            }
            // Lost the race: someone else holds `seq`. Learn what they wrote,
            // advance past it, and try again at the new head.
            self.commit_conflicts.fetch_add(1, Ordering::Relaxed);
            // Already inside the critical section; taking it again would
            // self-deadlock on a non-reentrant Mutex.
            self.catch_up_locked()?;
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
        // One critical section from reading the head to publishing it.
        //
        // Without this, N concurrent callers all read the same stale head,
        // all select the same manifest entries, and all apply them — the
        // `files` mutex covers only the apply, so it does not make the
        // sequence atomic. A querier folds the log forward on *every*
        // query, so concurrent queries meant concurrent catch_up, and one
        // manifest entry became N copies of the same file: 8x row counts
        // on one querier and 6x on its neighbour, from a single flush
        // (`docs/FINDING_catalog_catch_up_race.md`).
        //
        // The same lock and the same argument as `commit`, which has always
        // held that seq allocation and the in-memory apply belong together.
        // This path is I/O-bound on the object store either way, so
        // serialising it costs little that the listing had not already.
        let _guard = self.commit_lock.lock().expect("commit lock");
        self.catch_up_locked()
    }

    /// The body of `catch_up`, for callers already holding `commit_lock`.
    fn catch_up_locked(&self) -> std::io::Result<()> {
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
        let mut tombs = self.tombstones.lock().expect("tombstone lock");
        let mut declared = self.declared_schemas.lock().expect("schema lock");
        let mut new_head = head;
        for (seq, entry) in newer {
            apply_entry(&mut files, &mut tombs, &mut declared, entry);
            new_head = new_head.max(seq);
        }
        drop(files);
        drop(tombs);
        drop(declared);
        self.seq.store(new_head, Ordering::SeqCst);
        Ok(())
    }

    /// Durably record a declared table schema (#80) in the manifest log. Same
    /// CAS loop as any commit, so it composes and replays/propagates like a file
    /// add. Returns the seq it landed at.
    pub fn commit_schema(&self, schema: TableSchema) -> std::io::Result<u64> {
        self.commit_guarded(Vec::new(), Vec::new(), Vec::new(), vec![schema], false)
    }

    /// The declared schema for a table, if an authorized CREATE declared one.
    pub fn declared_schema(&self, db: &str, table: &str) -> Option<TableSchema> {
        self.declared_schemas
            .lock()
            .expect("schema lock")
            .get(&(db.to_string(), table.to_string()))
            .cloned()
    }

    /// Every declared schema — the engine seeds its schema registry from these
    /// on boot so a declared table is queryable before any write lands.
    pub fn declared_schemas(&self) -> Vec<TableSchema> {
        self.declared_schemas
            .lock()
            .expect("schema lock")
            .values()
            .cloned()
            .collect()
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

    /// All active targeted-delete predicates (R-1), for the query filter and
    /// the compaction/apply pass.
    pub fn tombstones(&self) -> Vec<Tombstone> {
        self.tombstones.lock().expect("tombstone lock").clone()
    }

    /// Tombstones scoped to one table — what the query path folds into its
    /// mandatory predicate.
    pub fn tombstones_for(&self, db: &str, table: &str) -> Vec<Tombstone> {
        self.tombstones
            .lock()
            .expect("tombstone lock")
            .iter()
            .filter(|t| t.db == db && t.table == table)
            .cloned()
            .collect()
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

    /// Per-table storage totals for the Storage view (`docs/CONSOLE.md`
    /// §7.3, phase U2).
    ///
    /// Folded under the catalog lock rather than built from
    /// [`Catalog::all_files`], which clones every [`FileMeta`] — an
    /// allocation proportional to the whole catalog, made on every
    /// `/metrics` scrape and every self-monitoring sample. The aggregate is
    /// bounded by the table count instead.
    pub fn storage_summary(&self) -> Vec<TableStorage> {
        let files = self.files.lock().expect("catalog lock");
        let mut out: Vec<TableStorage> = files
            .iter()
            .map(|((db, table), metas)| TableStorage {
                db: db.clone(),
                table: table.clone(),
                files: metas.len() as u64,
                bytes: metas.iter().map(|m| m.size_bytes).sum(),
                rows: metas.iter().map(|m| m.rows).sum(),
            })
            .collect();
        // Stable order so a diff of two scrapes is readable.
        out.sort_by(|a, b| (&a.db, &a.table).cmp(&(&b.db, &b.table)));
        out
    }

    /// File counts by compaction level, for the Storage view's L0/L1 split
    /// and the compaction-lag question ("is L0 growing faster than the
    /// compactor drains it?").
    ///
    /// The level is derived from the filename prefix the write path
    /// stamps — see [`level_of`]. There is no `level` field on
    /// [`FileMeta`]; adding one is the better long-term answer, and would
    /// let historic files report their true level instead of being
    /// classified by a naming convention.
    pub fn level_counts(&self) -> LevelCounts {
        let files = self.files.lock().expect("catalog lock");
        let mut counts = LevelCounts::default();
        for meta in files.values().flatten() {
            match level_of(&meta.path) {
                Level::Flushed => counts.flushed += 1,
                Level::Compacted => counts.compacted += 1,
                Level::Rewritten => counts.rewritten += 1,
            }
        }
        counts
    }
}

/// Storage totals for one table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableStorage {
    pub db: String,
    pub table: String,
    pub files: u64,
    pub bytes: u64,
    pub rows: u64,
}

/// File counts by provenance.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LevelCounts {
    /// L0: written straight out of a buffer flush, never merged.
    pub flushed: u64,
    /// L1: produced by a compaction merge.
    pub compacted: u64,
    /// Rewritten by the R-1b tombstone GC pass.
    pub rewritten: u64,
}

/// How a data file came to exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Flushed,
    Compacted,
    Rewritten,
}

/// Classify a data file by the prefix its writer stamped on the basename.
///
/// This is a naming convention, not a recorded fact, so it is deliberately
/// in one place with a test that builds paths the same way the three write
/// sites do — change a path format and that test fails rather than this
/// metric quietly misreporting.
pub fn level_of(path: &str) -> Level {
    let name = path.rsplit('/').next().unwrap_or(path);
    match name.as_bytes().first() {
        Some(b'c') => Level::Compacted,
        Some(b't') => Level::Rewritten,
        // The flush path writes a zero-padded sequence number with no
        // prefix, so "starts with a digit" is the L0 case; anything
        // unrecognized is counted as L0 rather than dropped, because a
        // total that silently excludes files is worse than a coarse one.
        _ => Level::Flushed,
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

    /// The three paths the engine actually writes, built with the same
    /// `format!` strings as `crates/server/src/lib.rs` — the flush site,
    /// the compaction site, and the R-1b tombstone-rewrite site.
    ///
    /// `level_of` reads a naming convention rather than a recorded field,
    /// so this test is the thing that keeps the convention honest: change
    /// a path format at a write site without updating the classifier and
    /// the level metric would otherwise start lying silently.
    #[test]
    fn level_is_derived_from_the_paths_the_engine_really_writes() {
        let flushed = format!(
            "poc/events/data/{}/{:020}-{:06}.parquet",
            "2026080800", 7, 3
        );
        let compacted = format!(
            "poc/events/data/{}/c{:020}-{:06}.parquet",
            "2026080800", 7, 3
        );
        let rewritten = format!(
            "poc/events/data/{}/t{:020}-{:06}.parquet",
            "2026080800", 7, 3
        );

        assert_eq!(level_of(&flushed), Level::Flushed);
        assert_eq!(level_of(&compacted), Level::Compacted);
        assert_eq!(level_of(&rewritten), Level::Rewritten);

        // The prefix is on the BASENAME. A database or table beginning with
        // 'c' must not make every one of its files read as compacted.
        let tricky = format!(
            "customers/cpu/data/{}/{:020}-{:06}.parquet",
            "2026080800", 1, 1
        );
        assert_eq!(level_of(&tricky), Level::Flushed);
    }

    #[test]
    fn storage_summary_totals_bytes_and_rows_per_table() {
        let dir = tempfile::tempdir().unwrap();
        let cat = Catalog::load(LocalStore::new(dir.path()).unwrap()).unwrap();
        cat.commit_add(vec![
            meta("poc", "events", "2026080800", "poc/events/data/p/1.parquet"),
            meta("poc", "events", "2026080801", "poc/events/data/p/2.parquet"),
            meta("poc", "cpu", "2026080801", "poc/cpu/data/p/3.parquet"),
        ])
        .unwrap();

        let summary = cat.storage_summary();
        assert_eq!(summary.len(), 2, "one row per table, not per file");
        // Sorted, so this is stable.
        assert_eq!(summary[0].table, "cpu");
        assert_eq!(summary[0].files, 1);
        assert_eq!(summary[0].bytes, 100);
        assert_eq!(summary[1].table, "events");
        assert_eq!(summary[1].files, 2);
        assert_eq!(summary[1].bytes, 200, "two files of 100 bytes");
        assert_eq!(summary[1].rows, 20);
    }

    #[test]
    fn level_counts_split_flushed_from_compacted() {
        let dir = tempfile::tempdir().unwrap();
        let cat = Catalog::load(LocalStore::new(dir.path()).unwrap()).unwrap();
        cat.commit_add(vec![
            meta("poc", "events", "p", "poc/events/data/p/00001.parquet"),
            meta("poc", "events", "p", "poc/events/data/p/00002.parquet"),
            meta("poc", "events", "p", "poc/events/data/p/c00003.parquet"),
            meta("poc", "events", "p", "poc/events/data/p/t00004.parquet"),
        ])
        .unwrap();

        let counts = cat.level_counts();
        assert_eq!(counts.flushed, 2);
        assert_eq!(counts.compacted, 1);
        assert_eq!(counts.rewritten, 1);
        assert_eq!(
            counts.flushed + counts.compacted + counts.rewritten,
            cat.file_count() as u64,
            "every file must land in exactly one level"
        );
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

    /// A querier folds the manifest forward on every query, so concurrent
    /// queries mean concurrent `catch_up`. Before this was one critical
    /// section, N callers each read the same stale head, each selected the
    /// same entries, and each applied them — one flushed file became N
    /// entries in memory, and every later query scanned it N times. Measured
    /// on a live cluster: 10,000 rows on the ingester, 80,000 on one querier
    /// and 60,000 on another, from a single flush
    /// (`docs/FINDING_catalog_catch_up_race.md`).
    ///
    /// The replica is loaded BEFORE the writer commits, which is the whole
    /// setup: `load` folds the log, so a replica loaded afterwards already
    /// has a current head and every `catch_up` returns having done nothing.
    /// A first version of this test did exactly that and passed against the
    /// unfixed code — a regression test that cannot reproduce its own
    /// regression.
    ///
    /// The count is the entire assertion. `distinct` stayed correct
    /// throughout the real incident, so anything checking only "the data is
    /// there" passes while the bug is live.
    #[test]
    fn concurrent_catch_up_applies_an_entry_once() {
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();

        // Loaded first, against an empty store: head 0, knows nothing.
        let replica = Arc::new(Catalog::load(LocalStore::new(dir.path()).unwrap()).unwrap());
        assert_eq!(replica.head(), 0, "the replica must start behind");

        // A separate writer commits one file. The replica does not know yet.
        let writer = Catalog::load(LocalStore::new(dir.path()).unwrap()).unwrap();
        writer
            .commit_add(vec![meta("poc", "t", "2026081300", "one.parquet")])
            .unwrap();
        assert_eq!(writer.head(), 1);
        assert_eq!(replica.head(), 0, "still behind: this is the race window");

        let threads: Vec<_> = (0..16)
            .map(|_| {
                let c = Arc::clone(&replica);
                std::thread::spawn(move || c.catch_up().unwrap())
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }

        let files = replica.files_for("poc", "t");
        assert_eq!(
            files.len(),
            1,
            "one manifest entry must fold to one file however many callers              raced; got {} copies, which is the over-count that reads as a              healthy system with more data than expected",
            files.len()
        );
        assert_eq!(replica.head(), 1);
    }

    fn tomb(id: &str, db: &str, table: &str) -> Tombstone {
        Tombstone {
            id: id.into(),
            db: db.into(),
            table: table.into(),
            tag_equals: vec![("host".into(), "web-1".into())],
            min_ts_ns: Some(1),
            max_ts_ns: Some(100),
            created_seq: 0,
        }
    }

    /// A targeted-delete predicate is durable: it rides the same manifest log
    /// as file adds, so a cold replay must reconstruct it. Without this, a
    /// delete would silently un-apply after a restart and the "deleted" rows
    /// would reappear — a data-exposure regression, not just a lost feature.
    #[test]
    fn tombstone_survives_reload() {
        let dir = tempfile::tempdir().unwrap();
        {
            let cat = Catalog::load(LocalStore::new(dir.path()).unwrap()).unwrap();
            cat.commit_add(vec![meta("poc", "t", "2026081600", "a.parquet")])
                .unwrap();
            cat.commit_tombstone(tomb("d1", "poc", "t")).unwrap();
        }
        let cat = Catalog::load(LocalStore::new(dir.path()).unwrap()).unwrap();
        let ts = cat.tombstones_for("poc", "t");
        assert_eq!(ts.len(), 1, "the tombstone must replay from the log");
        assert_eq!(ts[0].id, "d1");
        assert_eq!(ts[0].tag_equals, vec![("host".into(), "web-1".into())]);
        // Scoping works: a different table sees nothing.
        assert!(cat.tombstones_for("poc", "other").is_empty());
        // And the file it hides is still catalogued (logical delete only).
        assert_eq!(cat.files_for("poc", "t").len(), 1);
    }

    /// `created_seq` must equal the seq the tombstone actually landed at, even
    /// when a CAS race bumps the head between building the entry and winning
    /// the slot. Physical GC (R-1b) only reclaims files whose max_ts predates
    /// the delete; a stale `created_seq` would misorder that comparison.
    #[test]
    fn tombstone_created_seq_matches_landing_slot() {
        let dir = tempfile::tempdir().unwrap();
        let store = || LocalStore::new(dir.path()).unwrap();

        // Two writers both at head 0. A takes seq 1 with a file; B's tombstone
        // loses the seq-1 race and must retry onto seq 2.
        let a = Catalog::load(store()).unwrap();
        let b = Catalog::load(store()).unwrap();
        let sa = a
            .commit_add(vec![meta("poc", "t", "2026081600", "a.parquet")])
            .unwrap();
        let sb = b.commit_tombstone(tomb("d1", "poc", "t")).unwrap();
        assert_eq!(sa, 1);
        assert_eq!(sb, 2, "the tombstone must land past the file commit");
        assert!(b.commit_conflicts() >= 1, "B should have lost seq 1");

        let fresh = Catalog::load(store()).unwrap();
        let ts = fresh.tombstones_for("poc", "t");
        assert_eq!(ts.len(), 1);
        assert_eq!(
            ts[0].created_seq, 2,
            "created_seq must be the slot it won, not the slot first attempted"
        );
    }

    /// Tombstones fold by id, so replaying or catching up over the same entry
    /// twice keeps exactly one copy — the mirror of the file over-count race.
    #[test]
    fn tombstone_applied_once_by_id() {
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let replica = Arc::new(Catalog::load(LocalStore::new(dir.path()).unwrap()).unwrap());
        assert_eq!(replica.head(), 0, "the replica must start behind");

        let writer = Catalog::load(LocalStore::new(dir.path()).unwrap()).unwrap();
        writer.commit_tombstone(tomb("d1", "poc", "t")).unwrap();

        let threads: Vec<_> = (0..16)
            .map(|_| {
                let c = Arc::clone(&replica);
                std::thread::spawn(move || c.catch_up().unwrap())
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(
            replica.tombstones_for("poc", "t").len(),
            1,
            "one tombstone entry must fold to one tombstone however many callers raced"
        );
    }
    /// Two writers, one object store — a compactor on each of two nodes.
    fn pair(dir: &std::path::Path) -> (Catalog<LocalStore>, Catalog<LocalStore>) {
        (
            Catalog::load(LocalStore::new(dir).unwrap()).unwrap(),
            Catalog::load(LocalStore::new(dir).unwrap()).unwrap(),
        )
    }

    fn paths(cat: &Catalog<LocalStore>) -> Vec<String> {
        let mut got: Vec<String> = cat
            .files_for("poc", "cpu")
            .into_iter()
            .map(|f| f.path)
            .collect();
        got.sort();
        got
    }

    #[test]
    fn plain_commit_lets_two_compactors_both_land_their_merge() {
        // Not a bug being introduced -- a bug being DEMONSTRATED, so the
        // guard below has something to be measured against. This is the
        // lost update the CAS does not catch: it guarantees no commit is
        // overwritten, not that a commit is still valid when it lands.
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = pair(dir.path());
        a.commit_add(vec![
            meta("poc", "cpu", "p", "f1"),
            meta("poc", "cpu", "p", "f2"),
        ])
        .unwrap();
        b.catch_up().unwrap();

        a.commit(
            vec![meta("poc", "cpu", "p", "merged-a")],
            vec!["f1".into(), "f2".into()],
        )
        .unwrap();
        b.commit(
            vec![meta("poc", "cpu", "p", "merged-b")],
            vec!["f1".into(), "f2".into()],
        )
        .unwrap();

        let fresh = Catalog::load(LocalStore::new(dir.path()).unwrap()).unwrap();
        assert_eq!(
            paths(&fresh),
            vec!["merged-a".to_string(), "merged-b".to_string()],
            "both merges landed: every row of this partition is now stored twice"
        );
    }

    #[test]
    fn commit_replace_refuses_a_merge_whose_inputs_are_gone() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = pair(dir.path());
        a.commit_add(vec![
            meta("poc", "cpu", "p", "f1"),
            meta("poc", "cpu", "p", "f2"),
        ])
        .unwrap();
        b.catch_up().unwrap();

        a.commit_replace(
            vec![meta("poc", "cpu", "p", "merged-a")],
            vec!["f1".into(), "f2".into()],
        )
        .unwrap();

        // b still believes f1/f2 exist. Its CAS loses, it catches up, and
        // the guard sees the inputs are gone.
        let err = b
            .commit_replace(
                vec![meta("poc", "cpu", "p", "merged-b")],
                vec!["f1".into(), "f2".into()],
            )
            .expect_err("the second merge must be refused");
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        assert!(
            err.to_string().contains("f1"),
            "the error should name the missing input: {err}"
        );

        let fresh = Catalog::load(LocalStore::new(dir.path()).unwrap()).unwrap();
        assert_eq!(paths(&fresh), vec!["merged-a".to_string()]);
    }

    #[test]
    fn commit_replace_is_an_ordinary_commit_when_the_inputs_are_there() {
        let dir = tempfile::tempdir().unwrap();
        let cat = Catalog::load(LocalStore::new(dir.path()).unwrap()).unwrap();
        cat.commit_add(vec![
            meta("poc", "cpu", "p", "f1"),
            meta("poc", "cpu", "p", "f2"),
        ])
        .unwrap();
        let seq = cat
            .commit_replace(
                vec![meta("poc", "cpu", "p", "merged")],
                vec!["f1".into(), "f2".into()],
            )
            .unwrap();
        assert_eq!(seq, 2);
        assert_eq!(paths(&cat), vec!["merged".to_string()]);
    }

    #[test]
    fn a_partial_disappearance_is_refused_too() {
        // The dangerous shape: one input survived, so a naive "did anything
        // change" check could conclude the merge is still valid. It is not
        // -- the surviving file would be removed and the vanished one's rows
        // would come back from the merge, which is worse than either.
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = pair(dir.path());
        a.commit_add(vec![
            meta("poc", "cpu", "p", "f1"),
            meta("poc", "cpu", "p", "f2"),
            meta("poc", "cpu", "p", "f3"),
        ])
        .unwrap();
        b.catch_up().unwrap();

        a.commit_replace(
            vec![meta("poc", "cpu", "p", "m-a")],
            vec!["f1".into(), "f2".into()],
        )
        .unwrap();
        let err = b
            .commit_replace(
                vec![meta("poc", "cpu", "p", "m-b")],
                vec!["f2".into(), "f3".into()],
            )
            .expect_err("overlapping input sets must not both commit");
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn a_refused_merge_writes_no_manifest_entry() {
        // It must not burn a sequence number. A refusal that still advanced
        // the head would make every other writer catch up over an entry
        // that records nothing.
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = pair(dir.path());
        a.commit_add(vec![meta("poc", "cpu", "p", "f1")]).unwrap();
        b.catch_up().unwrap();
        a.commit_replace(vec![meta("poc", "cpu", "p", "m")], vec!["f1".into()])
            .unwrap();
        let head_before = a.head();
        let _ = b.commit_replace(vec![meta("poc", "cpu", "p", "m2")], vec!["f1".into()]);
        a.catch_up().unwrap();
        assert_eq!(a.head(), head_before, "a refusal must not advance the log");
    }

    #[test]
    fn declared_schema_survives_reload_and_last_wins() {
        // #80: a CREATE TABLE is a manifest-log entry like a tombstone, so the
        // two things that matter are that a cold reload replays it and that a
        // re-declaration of the same (db, table) is the one that survives.
        let dir = tempfile::tempdir().unwrap();
        let store = || LocalStore::new(dir.path()).unwrap();

        let a = Catalog::load(store()).unwrap();
        a.commit_schema(TableSchema {
            db: "poc".into(),
            table: "cpu".into(),
            columns: vec![
                TableColumn { name: "host".into(), ty: ColumnType::String, tag: true },
                TableColumn { name: "usage".into(), ty: ColumnType::Float, tag: false },
            ],
        })
        .unwrap();
        a.commit_schema(TableSchema {
            db: "poc".into(),
            table: "cpu".into(),
            columns: vec![
                TableColumn { name: "host".into(), ty: ColumnType::String, tag: true },
                TableColumn { name: "usage".into(), ty: ColumnType::Float, tag: false },
                TableColumn { name: "core".into(), ty: ColumnType::Integer, tag: true },
            ],
        })
        .unwrap();

        let fresh = Catalog::load(store()).unwrap();
        let s = fresh.declared_schema("poc", "cpu").expect("declaration replayed");
        assert_eq!(s.columns.len(), 3, "the later declaration wins on replay");
        assert!(s.columns.iter().any(|c| c.name == "core"));
        assert!(fresh.declared_schema("poc", "absent").is_none());
        assert_eq!(fresh.declared_schemas().len(), 1, "one table declared");
    }
}
