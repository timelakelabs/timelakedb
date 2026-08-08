//! Write-ahead log (RR-3). M1 scope: one local file of length-prefixed
//! frames storing the RAW line-protocol body (plus db and precision
//! multiplier), fsynced per append. Replay re-parses through the same
//! ingest path, so WAL and live writes cannot diverge. A truncated tail
//! (crash mid-write) is tolerated: replay stops at the last complete
//! frame and the file is truncated back to it.
//!
//! Later milestones: segment rotation + upload via Store (CL-1), and
//! group-commit fsync windows for PR-1 (M4 tuning) — the `append`
//! signature already takes a batch, so callers won't change.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, Write};
use std::path::{Path, PathBuf};

const MAGIC: u32 = 0x544C_4442; // "TLDB"

/// One replayed write: (database, precision multiplier, raw LP body).
pub type Frame = (String, i64, Vec<u8>);

pub struct Wal {
    file: File,
    path: PathBuf,
}

impl Wal {
    /// Open (creating if absent) and replay all complete frames.
    pub fn open(dir: &Path) -> std::io::Result<(Wal, Vec<Frame>)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("wal.log");
        let mut frames = Vec::new();
        let mut good_end = 0u64;

        if path.exists() {
            let mut r = BufReader::new(File::open(&path)?);
            loop {
                match read_frame(&mut r) {
                    Ok(Some(f)) => {
                        frames.push(f);
                        good_end = r.stream_position()?;
                    }
                    Ok(None) => break,
                    Err(_) => break, // truncated / corrupt tail: keep the good prefix
                }
            }
        }

        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let len = file.metadata()?.len();
        if len > good_end {
            // crash mid-frame — drop the partial tail
            let f = OpenOptions::new().write(true).open(&path)?;
            f.set_len(good_end)?;
            f.sync_data()?;
        }

        Ok((Wal { file, path }, frames))
    }

    /// Durably append one write. 204 must not be sent before this returns.
    pub fn append(&mut self, db: &str, mult: i64, body: &[u8]) -> std::io::Result<()> {
        let db_b = db.as_bytes();
        let mut buf = Vec::with_capacity(20 + db_b.len() + body.len());
        buf.extend_from_slice(&MAGIC.to_le_bytes());
        buf.extend_from_slice(&(db_b.len() as u32).to_le_bytes());
        buf.extend_from_slice(db_b);
        buf.extend_from_slice(&mult.to_le_bytes());
        buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
        buf.extend_from_slice(body);
        self.file.write_all(&buf)?;
        self.file.sync_data()
    }

    /// Bytes currently in the log (bounds RR-3 replay; flush truncation at M2).
    pub fn size(&self) -> u64 {
        self.file.metadata().map(|m| m.len()).unwrap_or(0)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn read_frame<R: Read + Seek>(r: &mut R) -> std::io::Result<Option<Frame>> {
    let mut hdr = [0u8; 4];
    match r.read_exact(&mut hdr) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    if u32::from_le_bytes(hdr) != MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bad frame magic",
        ));
    }
    let mut b4 = [0u8; 4];
    r.read_exact(&mut b4)?;
    let db_len = u32::from_le_bytes(b4) as usize;
    if db_len > 4096 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "db name too long",
        ));
    }
    let mut db = vec![0u8; db_len];
    r.read_exact(&mut db)?;
    let mut b8 = [0u8; 8];
    r.read_exact(&mut b8)?;
    let mult = i64::from_le_bytes(b8);
    r.read_exact(&mut b4)?;
    let body_len = u32::from_le_bytes(b4) as usize;
    let mut body = vec![0u8; body_len];
    r.read_exact(&mut body)?;
    let db = String::from_utf8(db)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "db not utf8"))?;
    Ok(Some((db, mult, body)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_reopen_replay_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut wal, replay) = Wal::open(dir.path()).unwrap();
            assert!(replay.is_empty());
            wal.append("poc", 1, b"m f=1i 1").unwrap();
            wal.append("poc", 1_000_000_000, b"m f=2i 2\nm f=3i 3").unwrap();
            wal.append("other", 1, b"h c=0.5 4").unwrap();
        }
        let (_, replay) = Wal::open(dir.path()).unwrap();
        assert_eq!(replay.len(), 3);
        assert_eq!(replay[0], ("poc".into(), 1, b"m f=1i 1".to_vec()));
        assert_eq!(replay[1].1, 1_000_000_000);
        assert_eq!(replay[2].0, "other");
    }

    #[test]
    fn truncated_tail_is_dropped_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut wal, _) = Wal::open(dir.path()).unwrap();
            wal.append("poc", 1, b"m f=1i 1").unwrap();
        }
        // simulate a crash mid-frame: append garbage half-frame
        {
            let mut f = OpenOptions::new()
                .append(true)
                .open(dir.path().join("wal.log"))
                .unwrap();
            f.write_all(&MAGIC.to_le_bytes()).unwrap();
            f.write_all(&3u32.to_le_bytes()).unwrap();
            f.write_all(b"po").unwrap(); // incomplete
        }
        let (mut wal, replay) = Wal::open(dir.path()).unwrap();
        assert_eq!(replay.len(), 1);
        // and the log is writable again after truncation
        wal.append("poc", 1, b"m f=2i 2").unwrap();
        let (_, replay) = Wal::open(dir.path()).unwrap();
        assert_eq!(replay.len(), 2);
    }
}
