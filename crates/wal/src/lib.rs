//! Write-ahead log (RR-3) with generations.
//!
//! Frames store the RAW line-protocol body (plus db and precision
//! multiplier); replay re-parses through the same ingest path, so WAL and
//! live writes cannot diverge. fsync before the 204 (ack contract).
//! A truncated tail (crash mid-write) is tolerated per generation.
//!
//! Generations exist for the flush checkpoint (ARCHITECTURE §5/§6):
//! `rotate()` seals the current file and starts gen+1 for new writes;
//! after the sealed generations' rows are flushed to Parquet and the
//! manifest commit is durable, `delete_generations_before(gen)` reclaims
//! them. Crash between flush and delete replays already-flushed rows —
//! duplicates across parquet+buffer in that window are a known M2 limit
//! (cross-source dedup completes with compaction at M3); acknowledged
//! writes are never lost, which is the contract that matters.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, Write};
use std::path::{Path, PathBuf};

const MAGIC: u32 = 0x544C_4442; // "TLDB"

/// One replayed write: (database, precision multiplier, raw LP body).
pub type Frame = (String, i64, Vec<u8>);

pub struct Wal {
    dir: PathBuf,
    file: File,
    generation: u64,
}

fn gen_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("wal.{generation:08}.log"))
}

fn list_generations(dir: &Path) -> std::io::Result<Vec<u64>> {
    let mut gens = Vec::new();
    if dir.is_dir() {
        for entry in std::fs::read_dir(dir)? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            if let Some(g) = name
                .strip_prefix("wal.")
                .and_then(|s| s.strip_suffix(".log"))
                .and_then(|s| s.parse::<u64>().ok())
            {
                gens.push(g);
            }
        }
    }
    gens.sort_unstable();
    Ok(gens)
}

impl Wal {
    /// Open (creating if absent) and replay all complete frames of all
    /// generations, oldest first.
    pub fn open(dir: &Path) -> std::io::Result<(Wal, Vec<Frame>)> {
        std::fs::create_dir_all(dir)?;
        // migrate the M1 single-file layout
        let legacy = dir.join("wal.log");
        if legacy.exists() {
            std::fs::rename(&legacy, gen_path(dir, 0))?;
        }

        let gens = list_generations(dir)?;
        let mut frames = Vec::new();
        for &g in &gens {
            replay_file(&gen_path(dir, g), &mut frames)?;
        }
        let generation = gens.last().copied().unwrap_or(0);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(gen_path(dir, generation))?;
        Ok((
            Wal {
                dir: dir.to_path_buf(),
                file,
                generation,
            },
            frames,
        ))
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

    /// Seal the current generation and start a new one for subsequent
    /// appends. Returns the sealed generation number.
    pub fn rotate(&mut self) -> std::io::Result<u64> {
        let sealed = self.generation;
        self.file.sync_data()?;
        self.generation += 1;
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(gen_path(&self.dir, self.generation))?;
        Ok(sealed)
    }

    /// Reclaim generations `<= upto` (call only after their rows are in
    /// a durably committed manifest).
    pub fn delete_generations_upto(&self, upto: u64) -> std::io::Result<()> {
        for g in list_generations(&self.dir)? {
            if g <= upto && g != self.generation {
                std::fs::remove_file(gen_path(&self.dir, g))?;
            }
        }
        Ok(())
    }

    /// Total bytes across live generations (bounds RR-3 replay; feeds
    /// the RR-5 backpressure check).
    pub fn size(&self) -> u64 {
        list_generations(&self.dir)
            .map(|gens| {
                gens.iter()
                    .filter_map(|&g| std::fs::metadata(gen_path(&self.dir, g)).ok())
                    .map(|m| m.len())
                    .sum()
            })
            .unwrap_or(0)
    }
}

fn replay_file(path: &Path, frames: &mut Vec<Frame>) -> std::io::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let mut good_end = 0u64;
    {
        let mut r = BufReader::new(File::open(path)?);
        loop {
            match read_frame(&mut r) {
                Ok(Some(f)) => {
                    frames.push(f);
                    good_end = r.stream_position()?;
                }
                Ok(None) => return Ok(()),
                Err(_) => break, // truncated / corrupt tail
            }
        }
    }
    // crash mid-frame — drop the partial tail so appends resume cleanly
    let f = OpenOptions::new().write(true).open(path)?;
    if f.metadata()?.len() > good_end {
        f.set_len(good_end)?;
        f.sync_data()?;
    }
    Ok(())
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
        assert_eq!(replay[2].0, "other");
    }

    #[test]
    fn rotation_checkpoint_and_reclaim() {
        let dir = tempfile::tempdir().unwrap();
        let (mut wal, _) = Wal::open(dir.path()).unwrap();
        wal.append("poc", 1, b"m f=1i 1").unwrap();
        let sealed = wal.rotate().unwrap();
        wal.append("poc", 1, b"m f=2i 2").unwrap();

        // both generations replay, oldest first
        let (_, replay) = Wal::open(dir.path()).unwrap();
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].2, b"m f=1i 1".to_vec());

        // after "flush + manifest commit", the sealed gen is reclaimed
        let (wal, _) = Wal::open(dir.path()).unwrap();
        wal.delete_generations_upto(sealed).unwrap();
        let (_, replay) = Wal::open(dir.path()).unwrap();
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].2, b"m f=2i 2".to_vec());
    }

    #[test]
    fn truncated_tail_is_dropped_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut wal, _) = Wal::open(dir.path()).unwrap();
            wal.append("poc", 1, b"m f=1i 1").unwrap();
        }
        {
            let mut f = OpenOptions::new()
                .append(true)
                .open(gen_path(dir.path(), 0))
                .unwrap();
            f.write_all(&MAGIC.to_le_bytes()).unwrap();
            f.write_all(&3u32.to_le_bytes()).unwrap();
            f.write_all(b"po").unwrap(); // incomplete
        }
        let (mut wal, replay) = Wal::open(dir.path()).unwrap();
        assert_eq!(replay.len(), 1);
        wal.append("poc", 1, b"m f=2i 2").unwrap();
        let (_, replay) = Wal::open(dir.path()).unwrap();
        assert_eq!(replay.len(), 2);
    }

    #[test]
    fn m1_single_file_layout_migrates() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut wal, _) = Wal::open(dir.path()).unwrap();
            wal.append("poc", 1, b"m f=1i 1").unwrap();
        }
        // simulate an M1 layout
        std::fs::rename(gen_path(dir.path(), 0), dir.path().join("wal.log")).unwrap();
        let (_, replay) = Wal::open(dir.path()).unwrap();
        assert_eq!(replay.len(), 1);
        assert!(gen_path(dir.path(), 0).exists());
    }
}
