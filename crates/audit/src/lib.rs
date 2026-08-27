//! Audit trail (P1-2 / SR-6): a hash-chained, append-only record of every
//! administrative mutation.
//!
//! Each record chains to the previous by SHA-256, so a deletion or an edit
//! anywhere in the log is *detectable* — tamper evidence, not tamper
//! proofing (`docs/CONSOLE.md` §5.3): someone with write access to the file
//! can still rewrite the whole chain, and making *that* detectable needs an
//! external anchor, a documented v1 limitation.
//!
//! The sink fsyncs per record. The volume is human-scale — administrative
//! actions, not data-plane writes — and an administrative change that leaves
//! no durable record is worse than one that did not happen. That is why the
//! policy layer here is fail-closed (§5.5): once an append fails, further
//! mutations are refused until the sink recovers, unless the operator sets
//! fail-open.
//!
//! Two layers: [`AuditSink`] is the pure chain (append/replay/verify), and
//! [`AuditLog`] wraps it with the node identity, a health flag, the
//! fail-open policy, and a record counter — the object the server shares
//! with every admin handler.

use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// `prev_hash` of the very first record: a fixed genesis, so even record 1's
/// hash is a function of a known constant rather than of nothing.
pub const GENESIS: &str = "sha256:genesis";

/// The live segment. Rotation renames it aside as
/// `audit.<zero-padded-last-seq>.jsonl` and opens a fresh one.
const SEGMENT: &str = "audit.jsonl";

/// Default size trigger. An audit trail that is never rotated becomes one
/// enormous file that nothing can ship, archive or read selectively; a
/// default of "split it" is safe because splitting deletes nothing.
const DEFAULT_ROTATE_BYTES: u64 = 64 * 1024 * 1024;

/// The floor from `docs/CONSOLE.md` §5.4. Pruning may never delete a
/// segment younger than this, whatever else is configured — the retention
/// controls must not be able to erase the record of their own use.
pub const MIN_RETENTION_DAYS: u64 = 90;

/// Rotation and retention policy for the trail.
#[derive(Debug, Clone)]
pub struct Policy {
    /// Rotate once the live segment passes this many bytes.
    pub rotate_bytes: Option<u64>,
    /// Rotate this long after the live segment was opened.
    pub rotate_after: Option<std::time::Duration>,
    /// Delete segments older than this many days. `None` — the default —
    /// **never deletes anything.**
    ///
    /// Audit records are evidence. Losing them to a default nobody chose is
    /// the failure this is shaped to avoid, so retention is opt-in and is
    /// clamped to [`MIN_RETENTION_DAYS`] even when set lower.
    pub retain_days: Option<u64>,
}

impl Default for Policy {
    fn default() -> Self {
        Policy {
            rotate_bytes: Some(DEFAULT_ROTATE_BYTES),
            rotate_after: None,
            retain_days: None,
        }
    }
}

impl Policy {
    /// The effective retention, floored. Returns `None` when nothing is to
    /// be deleted.
    pub fn effective_retain_days(&self) -> Option<u64> {
        self.retain_days.map(|d| d.max(MIN_RETENTION_DAYS))
    }
}

/// The caller-supplied half of a record — everything the sink does not
/// assign itself (it stamps `node`, `seq`, `ts`, `prev_hash`, `hash`).
#[derive(Debug, Clone)]
pub struct NewRecord {
    /// Who did it — the authenticated principal.
    pub principal: String,
    /// The principal's role at the time.
    pub role: String,
    /// The admin session id, when one is available.
    pub session: Option<String>,
    /// The request's network origin, when known.
    pub source: Option<String>,
    /// Correlation id shared with the application log line, when one exists.
    pub request_id: Option<String>,
    /// What was attempted, dotted: `retention.set`, `token.issue`, …
    pub action: String,
    /// What it acted on (a table, a token id, a principal), when scoped.
    pub target: Option<String>,
    /// The resolved state before and after, so the record answers "what
    /// actually changed for the server" (§5.2). `None` when not applicable.
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
    /// `ok`, `denied`, or `error` — a denial is audited too (§5.1).
    pub outcome: String,
}

/// One committed audit record, serialized as a single JSON line.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditRecord {
    pub seq: u64,
    pub ts: String,
    pub node: String,
    pub principal: String,
    pub role: String,
    pub session: Option<String>,
    pub source: Option<String>,
    pub request_id: Option<String>,
    pub action: String,
    pub target: Option<String>,
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
    pub outcome: String,
    pub prev_hash: String,
    pub hash: String,
}

/// The chain has a break at `seq`, for the reason given. Returned by the
/// verifier and surfaced through `/admin/audit?verify=1`.
#[derive(Debug, Clone, PartialEq)]
pub struct AuditBreak {
    pub seq: u64,
    pub reason: String,
}

impl std::fmt::Display for AuditBreak {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "audit chain break at seq {}: {}", self.seq, self.reason)
    }
}

/// The sink could not durably record a mutation. Under fail-closed the
/// server turns this into `503 audit sink unavailable`.
#[derive(Debug)]
pub struct AuditUnavailable(pub String);

impl std::fmt::Display for AuditUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "audit sink unavailable: {}", self.0)
    }
}

impl std::error::Error for AuditUnavailable {}

/// The fields a record's hash is computed over: every field except `hash`
/// itself, in a fixed order. A struct (not a map) so the serialization is
/// deterministic without a canonicalization pass — and `serde_json::Value`
/// objects sort their keys (no `preserve_order` feature), so `before`/`after`
/// are deterministic too.
#[derive(Serialize)]
struct HashInput<'a> {
    seq: u64,
    ts: &'a str,
    node: &'a str,
    principal: &'a str,
    role: &'a str,
    session: &'a Option<String>,
    source: &'a Option<String>,
    request_id: &'a Option<String>,
    action: &'a str,
    target: &'a Option<String>,
    before: &'a Option<serde_json::Value>,
    after: &'a Option<serde_json::Value>,
    outcome: &'a str,
    prev_hash: &'a str,
}

/// `hash = SHA-256(canonical_json(record without hash) || prev_hash)` (§5.3).
fn compute_hash(r: &AuditRecord) -> String {
    let input = HashInput {
        seq: r.seq,
        ts: &r.ts,
        node: &r.node,
        principal: &r.principal,
        role: &r.role,
        session: &r.session,
        source: &r.source,
        request_id: &r.request_id,
        action: &r.action,
        target: &r.target,
        before: &r.before,
        after: &r.after,
        outcome: &r.outcome,
        prev_hash: &r.prev_hash,
    };
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(&input).expect("audit record serializes"));
    // `|| prev_hash` explicitly, matching the documented formula.
    hasher.update(r.prev_hash.as_bytes());
    let digest = hasher.finalize();
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    format!("sha256:{hex}")
}

/// Verify a sequence of records forms an unbroken chain: each `prev_hash`
/// matches the previous record's `hash`, each `hash` recomputes, and `seq`
/// increases by one. Returns the first break, or `Ok(())`.
pub fn verify_records(records: &[AuditRecord]) -> Result<(), AuditBreak> {
    let mut prev = GENESIS.to_string();
    let mut expected_seq: Option<u64> = None;
    for r in records {
        if let Some(want) = expected_seq
            && r.seq != want
        {
            return Err(AuditBreak {
                seq: r.seq,
                reason: format!("expected seq {want}, found {}", r.seq),
            });
        }
        if r.prev_hash != prev {
            return Err(AuditBreak {
                seq: r.seq,
                reason: "prev_hash does not match the previous record's hash".into(),
            });
        }
        if compute_hash(r) != r.hash {
            return Err(AuditBreak {
                seq: r.seq,
                reason: "record hash does not match its contents (edited)".into(),
            });
        }
        prev = r.hash.clone();
        expected_seq = Some(r.seq + 1);
    }
    Ok(())
}

/// The append-only, hash-chained sink. One per node, node-agnostic itself:
/// whoever appends supplies the node id. Not `Sync` — [`AuditLog`] holds it
/// behind a mutex, exactly as the engine holds the WAL.
pub struct AuditSink {
    dir: PathBuf,
    path: PathBuf,
    writer: File,
    /// The seq the next appended record will get (1-based).
    next_seq: u64,
    /// The hash of the last record — the next record's `prev_hash`.
    head: String,
    policy: Policy,
    /// Bytes in the live segment, and when it was opened — the two
    /// rotation triggers.
    written: u64,
    opened: std::time::SystemTime,
}

impl AuditSink {
    pub fn open(dir: impl AsRef<Path>) -> io::Result<AuditSink> {
        AuditSink::open_with(dir, Policy::default())
    }

    /// Open (creating if absent) the live audit segment under `dir`,
    /// recovering `next_seq` and `head` from the **whole trail** — every
    /// rotated segment plus the live one. Recovering from the live file
    /// alone would restart the chain at genesis immediately after a
    /// rotation, which is precisely the discontinuity the chain exists to
    /// make impossible.
    ///
    /// Replay does not verify the chain: a running node keeps auditing
    /// regardless, and the break is surfaced on demand through
    /// [`AuditSink::verify`].
    pub fn open_with(dir: impl AsRef<Path>, policy: Policy) -> io::Result<AuditSink> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(SEGMENT);

        let mut next_seq = 1u64;
        let mut head = GENESIS.to_string();
        for seg in segments_in_order(&dir)? {
            for rec in read_segment(&seg)? {
                next_seq = rec.seq + 1;
                head = rec.hash;
            }
        }

        let writer = OpenOptions::new().create(true).append(true).open(&path)?;
        let written = writer.metadata().map(|m| m.len()).unwrap_or(0);
        Ok(AuditSink {
            dir,
            path,
            writer,
            next_seq,
            head,
            policy,
            written,
            opened: std::time::SystemTime::now(),
        })
    }

    /// Would either rotation trigger fire?
    fn should_rotate(&self, next: u64) -> bool {
        if self.written == 0 {
            return false; // never rotate an empty segment
        }
        if let Some(limit) = self.policy.rotate_bytes
            && self.written + next > limit
        {
            return true;
        }
        if let Some(after) = self.policy.rotate_after
            && self.opened.elapsed().unwrap_or_default() >= after
        {
            return true;
        }
        false
    }

    /// Close the live segment and start a new one.
    ///
    /// Named by the last seq it contains, zero-padded, so the segments sort
    /// into chain order by filename and a missing one is obvious both to a
    /// human and to `read_all`. The chain is NOT restarted: the next record
    /// still carries the previous record's hash as its `prev_hash`, so
    /// verification runs straight through the boundary.
    fn rotate(&mut self) -> io::Result<()> {
        let last = self.next_seq.saturating_sub(1);
        let target = self.dir.join(format!("audit.{last:012}.jsonl"));
        if target.exists() {
            // Already rotated at this seq; nothing sensible to do but keep
            // writing rather than clobber evidence.
            return Ok(());
        }
        self.writer.flush()?;
        self.writer.sync_all()?;
        std::fs::rename(&self.path, &target)?;
        self.writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.written = 0;
        self.opened = std::time::SystemTime::now();
        self.prune();
        Ok(())
    }

    /// Delete whole segments older than the configured retention.
    ///
    /// Three guards, because this is the one operation here that destroys
    /// evidence: it does nothing unless retention was explicitly configured,
    /// it is clamped to [`MIN_RETENTION_DAYS`], and it never touches the
    /// live segment.
    fn prune(&self) {
        let Some(days) = self.policy.effective_retain_days() else {
            return;
        };
        let cutoff = match std::time::SystemTime::now()
            .checked_sub(std::time::Duration::from_secs(days * 86_400))
        {
            Some(t) => t,
            None => return,
        };
        let Ok(segs) = rotated_segments(&self.dir) else {
            return;
        };
        for seg in segs {
            let Ok(md) = std::fs::metadata(&seg) else {
                continue;
            };
            let Ok(modified) = md.modified() else {
                continue;
            };
            if modified < cutoff {
                let _ = std::fs::remove_file(&seg);
            }
        }
    }

    /// Every segment file, rotated and live, in chain order.
    pub fn segment_paths(&self) -> io::Result<Vec<PathBuf>> {
        segments_in_order(&self.dir)
    }

    /// Append one record durably (fsync). On any I/O error the in-memory
    /// `next_seq`/`head` are left untouched, so a retry reuses the same slot
    /// — no gap in the chain, no double-advance. Returns the committed record.
    pub fn append(&mut self, node: &str, nr: NewRecord) -> io::Result<AuditRecord> {
        let mut rec = AuditRecord {
            seq: self.next_seq,
            ts: rfc3339_utc(SystemTime::now()),
            node: node.to_string(),
            principal: nr.principal,
            role: nr.role,
            session: nr.session,
            source: nr.source,
            request_id: nr.request_id,
            action: nr.action,
            target: nr.target,
            before: nr.before,
            after: nr.after,
            outcome: nr.outcome,
            prev_hash: self.head.clone(),
            hash: String::new(),
        };
        rec.hash = compute_hash(&rec);

        let mut line = serde_json::to_vec(&rec).expect("audit record serializes");
        line.push(b'\n');
        // Durability before acknowledgement: write, flush, fsync. Only then
        // is the chain advanced — a failure here leaves the sink exactly
        // where it was, which is what lets the policy layer fail a mutation
        // closed without desyncing the log.
        // Rotate BEFORE writing, so a record is never split across the
        // boundary and the segment named `audit.<N>.jsonl` really does end
        // at seq N.
        if self.should_rotate(line.len() as u64) {
            self.rotate()?;
        }

        self.writer.write_all(&line)?;
        self.writer.flush()?;
        self.writer.sync_all()?;

        self.written += line.len() as u64;
        self.next_seq = rec.seq + 1;
        self.head = rec.hash.clone();
        Ok(rec)
    }

    /// The next record's `prev_hash` — the current chain head.
    pub fn head(&self) -> &str {
        &self.head
    }

    /// How many records have been written (the last assigned seq).
    pub fn count(&self) -> u64 {
        self.next_seq - 1
    }

    /// Every record on disk, in order. Backs the read endpoint.
    /// Every record in the trail, across every segment, in chain order.
    ///
    /// Concatenating in segment order is what lets [`verify_records`] work
    /// unchanged after rotation: the chain does not restart at a boundary,
    /// so a whole missing segment surfaces as a seq gap and a `prev_hash`
    /// mismatch exactly like a missing record would. Deleting a file is not
    /// a way to hide anything.
    pub fn read_all(&self) -> io::Result<Vec<AuditRecord>> {
        let mut out = Vec::new();
        for seg in segments_in_order(&self.dir)? {
            out.extend(read_segment(&seg)?);
        }
        Ok(out)
    }

    /// Re-read the segment and verify the whole chain. `Ok(())` if intact.
    pub fn verify(&self) -> io::Result<Result<(), AuditBreak>> {
        Ok(verify_records(&self.read_all()?))
    }
}

/// The shared audit surface the server hands to every admin handler: the
/// sink, the node identity it stamps, a health flag, the fail-open policy,
/// and a record counter for `/metrics`.
pub struct AuditLog {
    node: String,
    fail_open: bool,
    healthy: AtomicBool,
    records_total: AtomicU64,
    sink: Mutex<AuditSink>,
}

impl AuditLog {
    /// Open the log under `dir`, stamping `node` on every record. `fail_open`
    /// comes from `TIMELAKE_AUDIT_FAIL_OPEN` — default false (fail-closed).
    pub fn open(
        dir: impl AsRef<Path>,
        node: impl Into<String>,
        fail_open: bool,
    ) -> io::Result<AuditLog> {
        AuditLog::open_with(dir, node, fail_open, Policy::default())
    }

    /// As [`AuditLog::open`], with an explicit rotation and retention
    /// policy. The server builds one from `TIMELAKE_AUDIT_*`.
    pub fn open_with(
        dir: impl AsRef<Path>,
        node: impl Into<String>,
        fail_open: bool,
        policy: Policy,
    ) -> io::Result<AuditLog> {
        let sink = AuditSink::open_with(dir, policy)?;
        let count = sink.count();
        Ok(AuditLog {
            node: node.into(),
            fail_open,
            healthy: AtomicBool::new(true),
            records_total: AtomicU64::new(count),
            sink: Mutex::new(sink),
        })
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    pub fn fail_open(&self) -> bool {
        self.fail_open
    }

    pub fn healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    pub fn records_total(&self) -> u64 {
        self.records_total.load(Ordering::Relaxed)
    }

    /// Fail-closed admission, called BEFORE a mutation runs: `Ok` if it may
    /// proceed, `Err` when the sink is known-broken and fail-open is off — in
    /// which case the caller returns 503 and does not mutate. This is what
    /// keeps the door shut after the sink breaks, until it recovers.
    pub fn gate(&self) -> Result<(), AuditUnavailable> {
        if self.healthy() || self.fail_open {
            Ok(())
        } else {
            Err(AuditUnavailable(
                "audit sink is unhealthy; refusing the mutation".into(),
            ))
        }
    }

    /// Append a record. On success returns it and (re)marks the sink healthy;
    /// on I/O failure marks it unhealthy so the next `gate` refuses, and
    /// returns `Err`. Under fail-closed the caller turns that into a 503;
    /// under fail-open it proceeds and logs loudly.
    pub fn record(&self, nr: NewRecord) -> Result<AuditRecord, AuditUnavailable> {
        let mut sink = self.sink.lock().expect("audit lock");
        match sink.append(&self.node, nr) {
            Ok(rec) => {
                self.records_total.fetch_add(1, Ordering::Relaxed);
                self.healthy.store(true, Ordering::Relaxed);
                Ok(rec)
            }
            Err(e) => {
                self.healthy.store(false, Ordering::Relaxed);
                Err(AuditUnavailable(e.to_string()))
            }
        }
    }

    pub fn read_all(&self) -> io::Result<Vec<AuditRecord>> {
        self.sink.lock().expect("audit lock").read_all()
    }

    pub fn verify(&self) -> io::Result<Result<(), AuditBreak>> {
        self.sink.lock().expect("audit lock").verify()
    }
}

/// Rotated segments only, sorted. `audit.<012 seq>.jsonl` sorts
/// lexicographically into chain order because the seq is zero-padded.
fn rotated_segments(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir)? {
        let p = e?.path();
        let Some(name) = p.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if name.starts_with("audit.") && name.ends_with(".jsonl") && name != SEGMENT {
            out.push(p);
        }
    }
    out.sort();
    Ok(out)
}

/// Every segment in chain order: the rotated ones, then the live one.
fn segments_in_order(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = rotated_segments(dir)?;
    let live = dir.join(SEGMENT);
    if live.exists() {
        out.push(live);
    }
    Ok(out)
}

fn read_segment(path: &Path) -> io::Result<Vec<AuditRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for line in BufReader::new(File::open(path)?).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let rec: AuditRecord = serde_json::from_str(&line).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("corrupt audit record: {e}"),
            )
        })?;
        out.push(rec);
    }
    Ok(out)
}

/// RFC 3339 UTC with microsecond precision, no chrono — the same civil-date
/// algorithm the buffer crate uses for hour partitions. Public so the config
/// surface stamps override `at` timestamps from the same source as the audit
/// trail, rather than growing a second date formatter.
pub fn rfc3339_utc(sys: SystemTime) -> String {
    let dur = sys.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = dur.as_secs() as i64;
    let micros = dur.subsec_micros();
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let (h, mi, s) = (sod / 3600, (sod % 3600) / 60, sod % 60);
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{micros:06}Z")
}

/// Howard Hinnant's `civil_from_days`: days since the Unix epoch → (y, m, d).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(action: &str) -> NewRecord {
        NewRecord {
            principal: "rcowell".into(),
            role: "admin".into(),
            session: None,
            source: Some("10.0.3.7".into()),
            request_id: None,
            action: action.into(),
            target: Some("pipeline_events".into()),
            before: Some(serde_json::json!({"value": "90d"})),
            after: Some(serde_json::json!({"value": "30d"})),
            outcome: "ok".into(),
        }
    }

    #[test]
    fn append_assigns_seq_and_links_the_chain() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = AuditSink::open(dir.path()).unwrap();

        let a = sink.append("tldb-1", ev("retention.set")).unwrap();
        let b = sink.append("tldb-1", ev("token.issue")).unwrap();

        assert_eq!(a.seq, 1);
        assert_eq!(b.seq, 2);
        assert_eq!(a.node, "tldb-1");
        assert_eq!(a.prev_hash, GENESIS, "first record chains to genesis");
        assert_eq!(b.prev_hash, a.hash, "each record links to the previous");
        assert_eq!(sink.count(), 2);
        assert_eq!(sink.head(), b.hash);
        assert!(sink.verify().unwrap().is_ok());
    }

    #[test]
    fn reopen_recovers_seq_and_head() {
        let dir = tempfile::tempdir().unwrap();
        let head;
        {
            let mut sink = AuditSink::open(dir.path()).unwrap();
            sink.append("tldb-1", ev("retention.set")).unwrap();
            head = sink.append("tldb-1", ev("token.issue")).unwrap().hash;
        }
        // A fresh sink over the same dir continues the chain, it does not
        // restart it — a silent seq reset would let a deletion hide.
        let mut sink = AuditSink::open(dir.path()).unwrap();
        assert_eq!(sink.count(), 2);
        assert_eq!(sink.head(), head);
        let c = sink.append("tldb-1", ev("token.revoke")).unwrap();
        assert_eq!(c.seq, 3);
        assert_eq!(c.prev_hash, head);
        assert!(sink.verify().unwrap().is_ok());
    }

    #[test]
    fn an_edited_record_breaks_the_chain_at_its_seq() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = AuditSink::open(dir.path()).unwrap();
        sink.append("tldb-1", ev("retention.set")).unwrap();
        sink.append("tldb-1", ev("retention.set")).unwrap();
        sink.append("tldb-1", ev("retention.set")).unwrap();

        let mut records = sink.read_all().unwrap();
        assert!(verify_records(&records).is_ok(), "unedited chain is intact");

        // Tamper with the middle record's content but keep its stored hash —
        // the classic "edit the log" attack.
        records[1].after = Some(serde_json::json!({"value": "9999d"}));
        let brk = verify_records(&records).expect_err("edit must be caught");
        assert_eq!(brk.seq, 2, "the break is reported at the edited record");
    }

    #[test]
    fn a_deleted_record_breaks_the_chain() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = AuditSink::open(dir.path()).unwrap();
        sink.append("tldb-1", ev("a")).unwrap();
        sink.append("tldb-1", ev("b")).unwrap();
        sink.append("tldb-1", ev("c")).unwrap();

        let mut records = sink.read_all().unwrap();
        // Excise the middle record: seq now jumps 1 -> 3 and c's prev_hash no
        // longer matches its predecessor.
        records.remove(1);
        let brk = verify_records(&records).expect_err("deletion must be caught");
        assert_eq!(brk.seq, 3);
    }

    #[test]
    fn each_record_is_one_json_line() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = AuditSink::open(dir.path()).unwrap();
        sink.append("tldb-1", ev("a")).unwrap();
        sink.append("tldb-1", ev("b")).unwrap();
        let raw = std::fs::read_to_string(dir.path().join(SEGMENT)).unwrap();
        let lines: Vec<&str> = raw.lines().filter(|l| !l.trim().is_empty()).collect();
        assert_eq!(lines.len(), 2, "one line per record");
        for l in lines {
            serde_json::from_str::<AuditRecord>(l).expect("each line is a full record");
        }
    }

    #[test]
    fn timestamps_are_rfc3339_utc() {
        // 2025-01-01T00:00:00Z: 55y*365 + 14 leap days = 20089 days * 86400.
        let t = UNIX_EPOCH + std::time::Duration::from_secs(1_735_689_600);
        assert_eq!(rfc3339_utc(t), "2025-01-01T00:00:00.000000Z");
        // A sub-second component renders to microseconds.
        let t2 = UNIX_EPOCH + std::time::Duration::from_micros(1_735_689_600_000_123);
        assert_eq!(rfc3339_utc(t2), "2025-01-01T00:00:00.000123Z");
    }

    #[test]
    fn auditlog_records_and_counts_and_recovers() {
        let dir = tempfile::tempdir().unwrap();
        {
            let log = AuditLog::open(dir.path(), "tldb-1", false).unwrap();
            assert!(log.healthy());
            assert!(!log.fail_open());
            assert_eq!(log.records_total(), 0);
            log.record(ev("retention.set")).unwrap();
            log.record(ev("token.issue")).unwrap();
            assert_eq!(log.records_total(), 2);
            assert!(log.verify().unwrap().is_ok());
        }
        // The counter is recovered from disk on reopen, so /metrics is
        // continuous across a restart.
        let log = AuditLog::open(dir.path(), "tldb-1", false).unwrap();
        assert_eq!(log.records_total(), 2);
    }

    /// A tiny rotation limit, so a handful of records spans several files.
    fn rotating(dir: &Path) -> AuditSink {
        AuditSink::open_with(
            dir,
            Policy {
                rotate_bytes: Some(512),
                rotate_after: None,
                retain_days: None,
            },
        )
        .unwrap()
    }

    #[test]
    fn rotation_splits_the_trail_and_the_chain_still_verifies() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = rotating(dir.path());
        for _ in 0..40 {
            sink.append("tldb-1", ev("retention.set")).unwrap();
        }
        assert!(
            sink.segment_paths().unwrap().len() > 1,
            "40 records at a 512-byte limit must have rotated"
        );
        // The whole point: the chain runs straight through the boundaries.
        assert!(
            sink.verify().unwrap().is_ok(),
            "a rotated trail must still verify end to end"
        );
        assert_eq!(
            sink.read_all().unwrap().len(),
            40,
            "no record lost to rotation"
        );
    }

    #[test]
    fn seqs_are_continuous_across_a_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = rotating(dir.path());
        for _ in 0..30 {
            sink.append("tldb-1", ev("token.issue")).unwrap();
        }
        let all = sink.read_all().unwrap();
        for (i, r) in all.iter().enumerate() {
            assert_eq!(r.seq, i as u64 + 1, "seq must not restart at a boundary");
        }
    }

    /// Reopening after a rotation must continue the chain, not restart it.
    /// Recovering `head` from the live segment alone would hand the next
    /// record a genesis prev_hash and split the trail in two.
    #[test]
    fn reopening_after_rotation_continues_the_chain() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut sink = rotating(dir.path());
            for _ in 0..40 {
                sink.append("tldb-1", ev("a")).unwrap();
            }
        }
        let mut sink = rotating(dir.path());
        let next = sink.append("tldb-1", ev("after-restart")).unwrap();
        assert_eq!(next.seq, 41, "seq continues across a restart");
        assert_ne!(next.prev_hash, GENESIS, "the chain must not restart");
        assert!(sink.verify().unwrap().is_ok());
    }

    /// Deleting a whole segment is the obvious way to try to hide a record.
    /// It must be caught, exactly as editing one is.
    #[test]
    fn deleting_a_rotated_segment_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = rotating(dir.path());
        for _ in 0..40 {
            sink.append("tldb-1", ev("a")).unwrap();
        }
        assert!(sink.verify().unwrap().is_ok());

        let rotated = rotated_segments(dir.path()).unwrap();
        assert!(!rotated.is_empty());
        std::fs::remove_file(&rotated[0]).unwrap();

        let brk = sink
            .verify()
            .unwrap()
            .expect_err("a removed segment must break the chain");
        assert!(brk.seq > 0, "the break names where the trail jumps");
    }

    #[test]
    fn retention_is_off_by_default_and_floored_when_set() {
        // Off by default: audit records are evidence, and nothing should
        // delete them because a default said so.
        assert_eq!(Policy::default().effective_retain_days(), None);
        // A too-low setting is clamped rather than honoured, so the
        // retention control cannot erase the record of its own use.
        let p = Policy {
            retain_days: Some(1),
            ..Policy::default()
        };
        assert_eq!(p.effective_retain_days(), Some(MIN_RETENTION_DAYS));
        let p = Policy {
            retain_days: Some(365),
            ..Policy::default()
        };
        assert_eq!(p.effective_retain_days(), Some(365));
    }

    #[test]
    fn pruning_never_deletes_recent_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = AuditSink::open_with(
            dir.path(),
            Policy {
                rotate_bytes: Some(512),
                rotate_after: None,
                // Even asking for 1 day is clamped to 90, and these
                // segments are seconds old.
                retain_days: Some(1),
            },
        )
        .unwrap();
        for _ in 0..40 {
            sink.append("tldb-1", ev("a")).unwrap();
        }
        assert_eq!(
            sink.read_all().unwrap().len(),
            40,
            "freshly written segments must survive any retention setting"
        );
        assert!(sink.verify().unwrap().is_ok());
    }

    #[test]
    fn a_healthy_log_gate_admits() {
        let dir = tempfile::tempdir().unwrap();
        let log = AuditLog::open(dir.path(), "tldb-1", false).unwrap();
        assert!(log.gate().is_ok(), "a healthy sink admits mutations");
    }

    #[test]
    fn fail_closed_gate_refuses_when_unhealthy_then_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let log = AuditLog::open(dir.path(), "tldb-1", false).unwrap();
        // Simulate a broken sink (the in-crate test can touch the flag a real
        // I/O failure would set inside `record`).
        log.healthy
            .store(false, std::sync::atomic::Ordering::Relaxed);
        assert!(
            log.gate().is_err(),
            "fail-closed: a known-broken sink refuses further mutations"
        );
        // A subsequent successful append re-marks it healthy and reopens the
        // gate — the door stays shut only while the sink is actually broken.
        log.record(ev("retention.set")).unwrap();
        assert!(log.gate().is_ok(), "a recovered sink admits again");
    }

    #[test]
    fn fail_open_gate_admits_even_when_unhealthy() {
        let dir = tempfile::tempdir().unwrap();
        let log = AuditLog::open(dir.path(), "tldb-1", true).unwrap();
        log.healthy
            .store(false, std::sync::atomic::Ordering::Relaxed);
        assert!(
            log.gate().is_ok(),
            "fail-open: the operator chose to proceed despite a broken sink"
        );
    }
}
