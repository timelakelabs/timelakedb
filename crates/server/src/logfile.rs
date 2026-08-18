//! Rotating file sink for the server's **system** log — its `tracing`
//! output. Not the audit trail: that is `crates/audit`, which has its own
//! rotation because its records are evidence and are hash-chained, and the
//! two must not share a policy.
//!
//! Under systemd or Docker, stdout is captured and rotated for you and none
//! of this is needed; `TIMELAKE_LOG_FILE` unset leaves that path exactly as
//! it was. It exists for the bare-process deployment, where stdout
//! redirected to a file grows until the disk fills and takes the node with
//! it.
//!
//! Rotation fires on **either** trigger, whichever comes first:
//! `TIMELAKE_LOG_ROTATE_SIZE` (bytes written) or `TIMELAKE_LOG_ROTATE_EVERY`
//! (time since the file was opened). `TIMELAKE_LOG_KEEP` bounds how many
//! rotated files survive; unset keeps them all.
//!
//! This mirrors Tributary's `logfile.rs` deliberately — same triggers, same
//! naming, same retention rule — because an operator running both should
//! not have to learn two log-rotation models. The two repositories cannot
//! share the code (nothing is published to a registry), so they share the
//! shape, exactly as `credential.rs` mirrors `timelake-tls`.
//!
//! **This sink owns the file.** Do not also point logrotate at it: two
//! rotators on one path is how a log ends up in a deleted inode.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct RotatingLog {
    path: PathBuf,
    size_limit: Option<u64>,
    interval: Option<Duration>,
    keep: Option<usize>,
    inner: Mutex<Inner>,
}

struct Inner {
    file: File,
    written: u64,
    opened: SystemTime,
}

impl RotatingLog {
    pub fn open(
        path: impl Into<PathBuf>,
        size_limit: Option<u64>,
        interval: Option<Duration>,
        keep: Option<usize>,
    ) -> io::Result<Arc<RotatingLog>> {
        let path = path.into();
        if let Some(dir) = path.parent()
            && !dir.as_os_str().is_empty()
        {
            std::fs::create_dir_all(dir)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        // Append rather than truncate: a restart must not discard the log
        // that explains why the process restarted.
        let written = file.metadata().map(|m| m.len()).unwrap_or(0);
        Ok(Arc::new(RotatingLog {
            path,
            size_limit,
            interval,
            keep,
            inner: Mutex::new(Inner {
                file,
                written,
                opened: SystemTime::now(),
            }),
        }))
    }

    fn should_rotate(&self, inner: &Inner, next: u64) -> bool {
        // Never rotate an empty file: one line longer than the limit would
        // otherwise rotate on every write and produce endless empty files.
        if inner.written == 0 {
            return false;
        }
        if let Some(limit) = self.size_limit
            && inner.written + next > limit
        {
            return true;
        }
        if let Some(every) = self.interval
            && inner.opened.elapsed().unwrap_or_default() >= every
        {
            return true;
        }
        false
    }

    fn rotate(&self, inner: &mut Inner) -> io::Result<()> {
        let stamp = stamp_utc(SystemTime::now());
        // Append the stamp rather than `with_extension`, which REPLACES the
        // extension and would turn `timelakedb.log` into
        // `timelakedb.20250101-000000`, dropping the suffix the retention
        // scan matches on.
        let dir = self
            .path
            .parent()
            .filter(|d| !d.as_os_str().is_empty())
            .map(|d| d.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let name = self
            .path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("log")
            .to_string();

        let mut n = 0;
        let target = loop {
            let candidate = if n == 0 {
                dir.join(format!("{name}.{stamp}"))
            } else {
                dir.join(format!("{name}.{stamp}.{n}"))
            };
            if !candidate.exists() {
                break candidate;
            }
            n += 1;
            if n > 1000 {
                return Ok(());
            }
        };

        inner.file.flush()?;
        std::fs::rename(&self.path, &target)?;
        inner.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        inner.written = 0;
        inner.opened = SystemTime::now();
        self.prune();
        Ok(())
    }

    fn prune(&self) {
        let Some(keep) = self.keep else { return };
        let mut rotated = self.rotated_files();
        if rotated.len() <= keep {
            return;
        }
        rotated.sort();
        let excess = rotated.len() - keep;
        for p in rotated.into_iter().take(excess) {
            let _ = std::fs::remove_file(p);
        }
    }

    pub fn rotated_files(&self) -> Vec<PathBuf> {
        let dir = self
            .path
            .parent()
            .filter(|d| !d.as_os_str().is_empty())
            .map(|d| d.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let Some(stem) = self.path.file_name().and_then(|s| s.to_str()) else {
            return Vec::new();
        };
        let prefix = format!("{stem}.");
        let Ok(entries) = std::fs::read_dir(dir) else {
            return Vec::new();
        };
        entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|s| s.to_str())
                    .is_some_and(|n| n.starts_with(&prefix))
            })
            .collect()
    }
}

#[derive(Clone)]
pub struct LogSink(pub Arc<RotatingLog>);

impl Write for LogSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let log = &self.0;
        let mut inner = log.inner.lock().expect("log lock");
        if log.should_rotate(&inner, buf.len() as u64) {
            // A failed rotation must not lose the line or kill the node.
            let _ = log.rotate(&mut inner);
        }
        let n = inner.file.write(buf)?;
        inner.written += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.inner.lock().expect("log lock").file.flush()
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogSink {
    type Writer = LogSink;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Build the sink from `TIMELAKE_LOG_*`, or `None` for stdout.
pub fn from_env() -> Option<Arc<RotatingLog>> {
    let path = std::env::var("TIMELAKE_LOG_FILE").ok()?;
    let size = std::env::var("TIMELAKE_LOG_ROTATE_SIZE")
        .ok()
        .and_then(|v| crate::parse_size_bytes(&v));
    let every = std::env::var("TIMELAKE_LOG_ROTATE_EVERY")
        .ok()
        .and_then(|v| crate::parse_secs(&v))
        .map(Duration::from_secs);
    let keep = std::env::var("TIMELAKE_LOG_KEEP")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok());
    // Neither trigger set means the file would grow forever, which is what
    // this exists to prevent — so fall back to a size trigger rather than
    // quietly behaving like plain redirection.
    let size = if size.is_none() && every.is_none() {
        Some(100 * 1024 * 1024)
    } else {
        size
    };
    match RotatingLog::open(path, size, every, keep) {
        Ok(l) => Some(l),
        Err(e) => {
            eprintln!("TIMELAKE_LOG_FILE could not be opened ({e}); logging to stdout");
            None
        }
    }
}

fn stamp_utc(t: SystemTime) -> String {
    let secs = t
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}{m:02}{d:02}-{:02}{:02}{:02}",
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60
    )
}

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

    fn write(log: &Arc<RotatingLog>, s: &str) {
        LogSink(Arc::clone(log)).write_all(s.as_bytes()).unwrap();
    }

    #[test]
    fn rotates_on_size_and_loses_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("timelakedb.log");
        let log = RotatingLog::open(&p, Some(64), None, None).unwrap();
        for i in 0..20 {
            write(&log, &format!("line-{i:03}\n"));
        }
        assert!(!log.rotated_files().is_empty(), "should have rotated");

        let mut all = std::fs::read_to_string(&p).unwrap();
        for r in log.rotated_files() {
            all.push_str(&std::fs::read_to_string(r).unwrap());
        }
        for i in 0..20 {
            assert!(all.contains(&format!("line-{i:03}")), "lost line {i}");
        }
    }

    #[test]
    fn rotates_on_time() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("timelakedb.log");
        let log = RotatingLog::open(&p, None, Some(Duration::from_millis(50)), None).unwrap();
        write(&log, "before\n");
        std::thread::sleep(Duration::from_millis(80));
        write(&log, "after\n");
        assert_eq!(log.rotated_files().len(), 1);
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "after\n");
    }

    #[test]
    fn retention_bounds_the_rotated_files() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("timelakedb.log");
        let log = RotatingLog::open(&p, Some(20), None, Some(2)).unwrap();
        for i in 0..15 {
            write(&log, &format!("aaaaaaaaaaaaaaa-{i}\n"));
        }
        assert!(log.rotated_files().len() <= 2);
    }

    #[test]
    fn a_restart_appends_rather_than_truncating() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("timelakedb.log");
        {
            let log = RotatingLog::open(&p, None, None, None).unwrap();
            write(&log, "before restart\n");
        }
        {
            let log = RotatingLog::open(&p, None, None, None).unwrap();
            write(&log, "after restart\n");
        }
        let body = std::fs::read_to_string(&p).unwrap();
        assert!(body.contains("before restart") && body.contains("after restart"));
    }

    #[test]
    fn sizes_and_durations_parse() {
        assert_eq!(crate::parse_size_bytes("100MiB"), Some(100 * 1024 * 1024));
        assert_ne!(
            crate::parse_size_bytes("1KiB"),
            crate::parse_size_bytes("1KB"),
            "KiB is 1024, KB is 1000"
        );
        assert_eq!(crate::parse_secs("1d"), Some(86_400));
        assert_eq!(crate::parse_secs("0d"), None);
    }
}
