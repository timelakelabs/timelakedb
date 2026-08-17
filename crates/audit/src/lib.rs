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

/// The single append-only segment. One file for now; rotation and
/// object-store upload (§5.4) are a later enhancement on top of this.
const SEGMENT: &str = "audit.jsonl";

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
        if let Some(want) = expected_seq {
            if r.seq != want {
                return Err(AuditBreak {
                    seq: r.seq,
                    reason: format!("expected seq {want}, found {}", r.seq),
                });
            }
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
    path: PathBuf,
    writer: File,
    /// The seq the next appended record will get (1-based).
    next_seq: u64,
    /// The hash of the last record — the next record's `prev_hash`.
    head: String,
}

impl AuditSink {
    /// Open (creating if absent) the audit segment under `dir`, recovering
    /// `next_seq` and `head` by replaying what is already there. Replay does
    /// not verify the chain — a running node keeps auditing regardless; the
    /// break is surfaced on demand through [`AuditSink::verify`].
    pub fn open(dir: impl AsRef<Path>) -> io::Result<AuditSink> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let path = dir.join(SEGMENT);

        let mut next_seq = 1u64;
        let mut head = GENESIS.to_string();
        if path.exists() {
            for line in BufReader::new(File::open(&path)?).lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let rec: AuditRecord = serde_json::from_str(&line).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("corrupt audit record: {e}"))
                })?;
                next_seq = rec.seq + 1;
                head = rec.hash;
            }
        }

        let writer = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(AuditSink { path, writer, next_seq, head })
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
        self.writer.write_all(&line)?;
        self.writer.flush()?;
        self.writer.sync_all()?;

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
    pub fn read_all(&self) -> io::Result<Vec<AuditRecord>> {
        read_segment(&self.path)
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
    pub fn open(dir: impl AsRef<Path>, node: impl Into<String>, fail_open: bool) -> io::Result<AuditLog> {
        let sink = AuditSink::open(dir)?;
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
            Err(AuditUnavailable("audit sink is unhealthy; refusing the mutation".into()))
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
            io::Error::new(io::ErrorKind::InvalidData, format!("corrupt audit record: {e}"))
        })?;
        out.push(rec);
    }
    Ok(out)
}

/// RFC 3339 UTC with microsecond precision, no chrono — the same civil-date
/// algorithm the buffer crate uses for hour partitions.
fn rfc3339_utc(sys: SystemTime) -> String {
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
        log.healthy.store(false, std::sync::atomic::Ordering::Relaxed);
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
        log.healthy.store(false, std::sync::atomic::Ordering::Relaxed);
        assert!(
            log.gate().is_ok(),
            "fail-open: the operator chose to proceed despite a broken sink"
        );
    }
}
