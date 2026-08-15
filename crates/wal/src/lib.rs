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
//!
//! Encryption at rest (SEC-8, exposure 8). When a [`WalCipher`] is supplied
//! (the engine passes one whenever the object store is encrypted, using the
//! same envelope key), each generation file is encrypted: a one-time header
//! carries a per-file data key wrapped by the KEK, and every frame is sealed
//! with AES-256-GCM under it. A file is entirely plaintext or entirely
//! encrypted, decided at creation and recorded in a file-level magic —
//! `"TLDB"` frames for a plaintext file, a `"TLDW"` header for an encrypted
//! one — so a node that gains a key still replays the plaintext segments it
//! wrote before (passthrough, exactly as the object store does). Replay
//! fails CLOSED: an encrypted segment with no key, or a whole frame that
//! fails authentication, is a hard error, never a skipped acked write. Only
//! an INCOMPLETE trailing frame (a crash mid-append) is the tolerated torn
//! tail.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Key, Nonce};

const MAGIC: u32 = 0x544C_4442; // "TLDB" — a plaintext file / frame
const WMAGIC: u32 = 0x544C_4457; // "TLDW" — an encrypted file header
const WAL_VERSION: u8 = 1;
/// Sanity bound on a ciphertext length read from disk, so a corrupt length
/// prefix cannot drive a huge allocation. Comfortably above any real frame.
const MAX_CT: usize = 128 << 20;

/// One replayed write: (database, precision multiplier, raw LP body).
pub type Frame = (String, i64, Vec<u8>);

/// Supplies per-file data keys for WAL encryption (SEC-8). Implemented by
/// the engine over the object store's envelope KEK/KMS, so the WAL and the
/// store share one key without the WAL crate depending on the store.
pub trait WalCipher: Send + Sync {
    /// A fresh 32-byte data key and its wrapped form. The wrapped bytes go
    /// in the file header; the raw key seals that file's frames.
    fn generate(&self) -> std::io::Result<(Vec<u8>, [u8; 32])>;
    /// Recover a file's data key from the wrapped bytes in its header.
    fn unwrap(&self, wrapped: &[u8]) -> std::io::Result<[u8; 32]>;
}

/// The active encrypted file's key and next frame sequence number (the AEAD
/// nonce). Present iff the WAL was opened with a cipher.
struct FileEnc {
    dek: [u8; 32],
    seq: u64,
}

pub struct Wal {
    dir: PathBuf,
    file: File,
    generation: u64,
    cipher: Option<Arc<dyn WalCipher>>,
    enc: Option<FileEnc>,
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

fn invalid(msg: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg)
}

// ---- frame body codec (shared by the plaintext and encrypted paths) ------

/// The bytes AFTER the frame magic: `db_len|db|mult|body_len|body`. A
/// plaintext frame is `MAGIC ++ this`; an encrypted frame seals `this`.
fn encode_frame_body(db: &str, mult: i64, body: &[u8]) -> Vec<u8> {
    let db_b = db.as_bytes();
    let mut buf = Vec::with_capacity(16 + db_b.len() + body.len());
    buf.extend_from_slice(&(db_b.len() as u32).to_le_bytes());
    buf.extend_from_slice(db_b);
    buf.extend_from_slice(&mult.to_le_bytes());
    buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
    buf.extend_from_slice(body);
    buf
}

/// Parse a decrypted frame body. It is already authenticated, so anything
/// malformed here is corruption, not a torn write.
fn parse_frame_body(buf: &[u8]) -> std::io::Result<Frame> {
    let mut o = 0usize;
    let mut take = |n: usize| -> std::io::Result<&[u8]> {
        let end = o
            .checked_add(n)
            .ok_or_else(|| invalid("wal frame overflow"))?;
        if end > buf.len() {
            return Err(invalid("wal frame body truncated"));
        }
        let s = &buf[o..end];
        o = end;
        Ok(s)
    };
    let db_len = u32::from_le_bytes(take(4)?.try_into().unwrap()) as usize;
    if db_len > 4096 {
        return Err(invalid("db name too long"));
    }
    let db = take(db_len)?.to_vec();
    let mult = i64::from_le_bytes(take(8)?.try_into().unwrap());
    let body_len = u32::from_le_bytes(take(4)?.try_into().unwrap()) as usize;
    let body = take(body_len)?.to_vec();
    let db = String::from_utf8(db).map_err(|_| invalid("db not utf8"))?;
    Ok((db, mult, body))
}

// ---- AES-256-GCM sealing --------------------------------------------------

/// 12-byte nonce from the frame sequence number. Unique per frame within a
/// file, and each file has its own data key, so a (key, nonce) pair never
/// repeats.
fn nonce_bytes(seq: u64) -> [u8; 12] {
    let mut n = [0u8; 12];
    n[..8].copy_from_slice(&seq.to_le_bytes());
    n
}

fn seal(dek: &[u8; 32], seq: u64, plaintext: &[u8]) -> std::io::Result<Vec<u8>> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(dek));
    let nonce = nonce_bytes(seq);
    cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad: &seq.to_le_bytes(),
            },
        )
        .map_err(|_| invalid("wal frame seal failed"))
}

fn open_sealed(dek: &[u8; 32], seq: u64, ct: &[u8]) -> std::io::Result<Vec<u8>> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(dek));
    let nonce = nonce_bytes(seq);
    cipher
        .decrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: ct,
                aad: &seq.to_le_bytes(),
            },
        )
        // A COMPLETE frame that will not authenticate is corruption, not a
        // torn tail — InvalidData is fatal in replay (fail closed).
        .map_err(|_| invalid("wal frame authentication failed"))
}

fn write_enc_header(file: &mut File, wrapped: &[u8]) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(7 + wrapped.len());
    buf.extend_from_slice(&WMAGIC.to_le_bytes());
    buf.push(WAL_VERSION);
    buf.extend_from_slice(&(wrapped.len() as u16).to_le_bytes());
    buf.extend_from_slice(wrapped);
    file.write_all(&buf)
}

impl Wal {
    /// Open (creating if absent) and replay all complete frames of all
    /// generations, oldest first. Plaintext when `cipher` is `None`.
    pub fn open(
        dir: &Path,
        cipher: Option<Arc<dyn WalCipher>>,
    ) -> std::io::Result<(Wal, Vec<Frame>)> {
        std::fs::create_dir_all(dir)?;
        // migrate the M1 single-file layout
        let legacy = dir.join("wal.log");
        if legacy.exists() {
            std::fs::rename(&legacy, gen_path(dir, 0))?;
        }

        let gens = list_generations(dir)?;
        let mut frames = Vec::new();
        for &g in &gens {
            replay_file(&gen_path(dir, g), &mut frames, cipher.as_deref())?;
        }

        // The active file for new appends. Plaintext continues the last
        // generation as before. Encrypted always starts a FRESH generation,
        // so its frame-sequence nonces begin at zero under a fresh data key —
        // no need to recover a key or a counter from a partly-written file.
        if let Some(c) = &cipher {
            let generation = gens.last().map(|g| g + 1).unwrap_or(0);
            let (wrapped, dek) = c.generate()?;
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(gen_path(dir, generation))?;
            write_enc_header(&mut file, &wrapped)?;
            file.sync_data()?;
            Ok((
                Wal {
                    dir: dir.to_path_buf(),
                    file,
                    generation,
                    cipher: cipher.clone(),
                    enc: Some(FileEnc { dek, seq: 0 }),
                },
                frames,
            ))
        } else {
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
                    cipher: None,
                    enc: None,
                },
                frames,
            ))
        }
    }

    /// Durably append one write. 204 must not be sent before this returns.
    pub fn append(&mut self, db: &str, mult: i64, body: &[u8]) -> std::io::Result<()> {
        if let Some(enc) = &mut self.enc {
            let ct = seal(&enc.dek, enc.seq, &encode_frame_body(db, mult, body))?;
            let mut buf = Vec::with_capacity(4 + ct.len());
            buf.extend_from_slice(&(ct.len() as u32).to_le_bytes());
            buf.extend_from_slice(&ct);
            self.file.write_all(&buf)?;
            self.file.sync_data()?;
            enc.seq += 1;
            Ok(())
        } else {
            let mut buf = Vec::with_capacity(4);
            buf.extend_from_slice(&MAGIC.to_le_bytes());
            buf.extend_from_slice(&encode_frame_body(db, mult, body));
            self.file.write_all(&buf)?;
            self.file.sync_data()
        }
    }

    /// Seal the current generation and start a new one for subsequent
    /// appends. Returns the sealed generation number.
    pub fn rotate(&mut self) -> std::io::Result<u64> {
        let sealed = self.generation;
        self.file.sync_data()?;
        self.generation += 1;
        let path = gen_path(&self.dir, self.generation);
        if let Some(c) = &self.cipher {
            let (wrapped, dek) = c.generate()?;
            let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
            write_enc_header(&mut file, &wrapped)?;
            file.sync_data()?;
            self.file = file;
            self.enc = Some(FileEnc { dek, seq: 0 });
        } else {
            self.file = OpenOptions::new().create(true).append(true).open(&path)?;
        }
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

/// Replay one generation file, dispatching on its file-level magic so a
/// plaintext segment written before a key was configured still replays.
fn replay_file(
    path: &Path,
    frames: &mut Vec<Frame>,
    cipher: Option<&dyn WalCipher>,
) -> std::io::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let magic = match peek_magic(path)? {
        None => return Ok(()), // empty file
        Some(m) => m,
    };
    if magic == WMAGIC {
        replay_encrypted(path, frames, cipher)
    } else if magic == MAGIC {
        replay_plaintext(path, frames)
    } else {
        Err(invalid("unrecognised wal file magic"))
    }
}

/// The first four bytes of a file, or None if it is empty.
fn peek_magic(path: &Path) -> std::io::Result<Option<u32>> {
    let mut f = File::open(path)?;
    let mut m = [0u8; 4];
    match f.read_exact(&mut m) {
        Ok(()) => Ok(Some(u32::from_le_bytes(m))),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

fn replay_plaintext(path: &Path, frames: &mut Vec<Frame>) -> std::io::Result<()> {
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

fn replay_encrypted(
    path: &Path,
    frames: &mut Vec<Frame>,
    cipher: Option<&dyn WalCipher>,
) -> std::io::Result<()> {
    // Fail closed: an encrypted segment holds acked writes; no key means we
    // cannot recover them and must not pretend the log is empty.
    let cipher = cipher.ok_or_else(|| {
        invalid("wal segment is encrypted but no key is configured — refusing to start (SEC-8)")
    })?;
    let mut good_end;
    {
        let mut r = BufReader::new(File::open(path)?);
        let dek = read_enc_header(&mut r, cipher)?;
        good_end = r.stream_position()?; // never truncate into the header
        let mut seq = 0u64;
        loop {
            match read_frame_encrypted(&mut r, &dek, seq) {
                Ok(Some(f)) => {
                    frames.push(f);
                    good_end = r.stream_position()?;
                    seq += 1;
                }
                Ok(None) => return Ok(()), // clean end
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break, // torn tail
                Err(e) => return Err(e),   // auth failure / corruption — FATAL
            }
        }
    }
    let f = OpenOptions::new().write(true).open(path)?;
    if f.metadata()?.len() > good_end {
        f.set_len(good_end)?;
        f.sync_data()?;
    }
    Ok(())
}

/// Read the encrypted-file header and recover its data key. Leaves the
/// reader positioned at the first frame.
fn read_enc_header<R: Read>(r: &mut R, cipher: &dyn WalCipher) -> std::io::Result<[u8; 32]> {
    let mut m = [0u8; 4];
    r.read_exact(&mut m)?;
    if u32::from_le_bytes(m) != WMAGIC {
        return Err(invalid("not a wal encrypted header"));
    }
    let mut v = [0u8; 1];
    r.read_exact(&mut v)?;
    if v[0] != WAL_VERSION {
        return Err(invalid("unsupported wal encryption version"));
    }
    let mut wl = [0u8; 2];
    r.read_exact(&mut wl)?;
    let wrapped_len = u16::from_le_bytes(wl) as usize;
    if wrapped_len > 4096 {
        return Err(invalid("wal wrapped key too long"));
    }
    let mut wrapped = vec![0u8; wrapped_len];
    r.read_exact(&mut wrapped)?;
    cipher.unwrap(&wrapped)
}

/// Read up to 4 bytes as a little-endian length. `None` = a clean end (no
/// bytes remained); a partial read is a torn tail (`UnexpectedEof`).
fn read_len<R: Read>(r: &mut R) -> std::io::Result<Option<u32>> {
    let mut b = [0u8; 4];
    let mut got = 0;
    while got < 4 {
        match r.read(&mut b[got..]) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    if got == 0 {
        Ok(None)
    } else if got < 4 {
        Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "torn wal length prefix",
        ))
    } else {
        Ok(Some(u32::from_le_bytes(b)))
    }
}

fn read_frame_encrypted<R: Read>(
    r: &mut R,
    dek: &[u8; 32],
    seq: u64,
) -> std::io::Result<Option<Frame>> {
    let ct_len = match read_len(r)? {
        None => return Ok(None), // clean end
        Some(n) => n as usize,
    };
    if ct_len > MAX_CT {
        return Err(invalid("wal ciphertext length implausible"));
    }
    let mut ct = vec![0u8; ct_len];
    r.read_exact(&mut ct)?; // partial -> UnexpectedEof -> torn tail
    let pt = open_sealed(dek, seq, &ct)?; // auth fail -> InvalidData -> fatal
    Ok(Some(parse_frame_body(&pt)?))
}

fn read_frame<R: Read + Seek>(r: &mut R) -> std::io::Result<Option<Frame>> {
    let mut hdr = [0u8; 4];
    match r.read_exact(&mut hdr) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    if u32::from_le_bytes(hdr) != MAGIC {
        return Err(invalid("bad frame magic"));
    }
    let mut b4 = [0u8; 4];
    r.read_exact(&mut b4)?;
    let db_len = u32::from_le_bytes(b4) as usize;
    if db_len > 4096 {
        return Err(invalid("db name too long"));
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
    let db = String::from_utf8(db).map_err(|_| invalid("db not utf8"))?;
    Ok(Some((db, mult, body)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_plain(dir: &Path) -> (Wal, Vec<Frame>) {
        Wal::open(dir, None).unwrap()
    }

    #[test]
    fn append_reopen_replay_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut wal, replay) = open_plain(dir.path());
            assert!(replay.is_empty());
            wal.append("poc", 1, b"m f=1i 1").unwrap();
            wal.append("poc", 1_000_000_000, b"m f=2i 2\nm f=3i 3")
                .unwrap();
            wal.append("other", 1, b"h c=0.5 4").unwrap();
        }
        let (_, replay) = open_plain(dir.path());
        assert_eq!(replay.len(), 3);
        assert_eq!(replay[0], ("poc".into(), 1, b"m f=1i 1".to_vec()));
        assert_eq!(replay[2].0, "other");
    }

    #[test]
    fn rotation_checkpoint_and_reclaim() {
        let dir = tempfile::tempdir().unwrap();
        let (mut wal, _) = open_plain(dir.path());
        wal.append("poc", 1, b"m f=1i 1").unwrap();
        let sealed = wal.rotate().unwrap();
        wal.append("poc", 1, b"m f=2i 2").unwrap();

        // both generations replay, oldest first
        let (_, replay) = open_plain(dir.path());
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].2, b"m f=1i 1".to_vec());

        // after "flush + manifest commit", the sealed gen is reclaimed
        let (wal, _) = open_plain(dir.path());
        wal.delete_generations_upto(sealed).unwrap();
        let (_, replay) = open_plain(dir.path());
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].2, b"m f=2i 2".to_vec());
    }

    #[test]
    fn truncated_tail_is_dropped_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut wal, _) = open_plain(dir.path());
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
        let (mut wal, replay) = open_plain(dir.path());
        assert_eq!(replay.len(), 1);
        wal.append("poc", 1, b"m f=2i 2").unwrap();
        let (_, replay) = open_plain(dir.path());
        assert_eq!(replay.len(), 2);
    }

    #[test]
    fn m1_single_file_layout_migrates() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut wal, _) = open_plain(dir.path());
            wal.append("poc", 1, b"m f=1i 1").unwrap();
        }
        // simulate an M1 layout
        std::fs::rename(gen_path(dir.path(), 0), dir.path().join("wal.log")).unwrap();
        let (_, replay) = open_plain(dir.path());
        assert_eq!(replay.len(), 1);
        assert!(gen_path(dir.path(), 0).exists());
    }

    // ---- SEC-8: encryption ------------------------------------------------

    /// A trivial in-test cipher: XOR-wrap a per-file key with a fixed KEK.
    /// Enough to exercise wrap/unwrap and the wrong-key path deterministically.
    struct TestCipher {
        kek: u8,
        next: std::sync::atomic::AtomicU8,
    }
    impl TestCipher {
        fn new(kek: u8) -> Arc<Self> {
            Arc::new(TestCipher {
                kek,
                next: std::sync::atomic::AtomicU8::new(1),
            })
        }
    }
    impl WalCipher for TestCipher {
        fn generate(&self) -> std::io::Result<(Vec<u8>, [u8; 32])> {
            let seed = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let dek = [seed; 32];
            let wrapped = dek.iter().map(|b| b ^ self.kek).collect();
            Ok((wrapped, dek))
        }
        fn unwrap(&self, wrapped: &[u8]) -> std::io::Result<[u8; 32]> {
            if wrapped.len() != 32 {
                return Err(invalid("bad wrapped len"));
            }
            let mut dek = [0u8; 32];
            for (o, w) in dek.iter_mut().zip(wrapped) {
                *o = w ^ self.kek;
            }
            Ok(dek)
        }
    }

    #[test]
    fn encrypted_roundtrip_and_bytes_are_ciphertext() {
        let dir = tempfile::tempdir().unwrap();
        let c = TestCipher::new(0xAB);
        {
            let (mut wal, replay) = Wal::open(dir.path(), Some(c.clone())).unwrap();
            assert!(replay.is_empty());
            wal.append("poc", 1, b"secret_measurement value=42i 7")
                .unwrap();
            wal.append("poc", 1, b"another x=1i 8").unwrap();
        }
        // On disk: the file starts with the TLDW header, and the plaintext
        // measurement name never appears in the raw bytes.
        let gen0 = std::fs::read(gen_path(dir.path(), 0)).unwrap();
        assert_eq!(&gen0[..4], &WMAGIC.to_le_bytes());
        let needle = b"secret_measurement";
        assert!(
            !gen0.windows(needle.len()).any(|w| w == needle),
            "plaintext leaked into the encrypted WAL"
        );
        // Replays cleanly with the key. Note: open() started a fresh gen 1
        // for appends, so gen 0 holds the two frames.
        let (_, replay) = Wal::open(dir.path(), Some(c.clone())).unwrap();
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].2, b"secret_measurement value=42i 7".to_vec());
        assert_eq!(replay[1].2, b"another x=1i 8".to_vec());
    }

    #[test]
    fn plaintext_segments_still_replay_after_a_key_is_added() {
        let dir = tempfile::tempdir().unwrap();
        // Write plaintext first (no key), as a pre-upgrade node would.
        {
            let (mut wal, _) = open_plain(dir.path());
            wal.append("poc", 1, b"old_plain a=1i 1").unwrap();
        }
        // Now open WITH a key: the old plaintext gen must still replay, and
        // new writes land encrypted.
        let c = TestCipher::new(0x11);
        {
            let (mut wal, replay) = Wal::open(dir.path(), Some(c.clone())).unwrap();
            assert_eq!(replay.len(), 1, "plaintext gen replayed under a key");
            assert_eq!(replay[0].2, b"old_plain a=1i 1".to_vec());
            wal.append("poc", 1, b"new_enc b=2i 2").unwrap();
        }
        let (_, replay) = Wal::open(dir.path(), Some(c.clone())).unwrap();
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[1].2, b"new_enc b=2i 2".to_vec());
    }

    #[test]
    fn an_encrypted_segment_without_a_key_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let c = TestCipher::new(0x22);
        {
            let (mut wal, _) = Wal::open(dir.path(), Some(c)).unwrap();
            wal.append("poc", 1, b"acked a=1i 1").unwrap();
        }
        // Reopening with NO key must refuse, not silently drop the acked row.
        let err = Wal::open(dir.path(), None).err().unwrap();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_wrong_key_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut wal, _) = Wal::open(dir.path(), Some(TestCipher::new(0x33))).unwrap();
            wal.append("poc", 1, b"acked a=1i 1").unwrap();
        }
        // A different KEK unwraps to the wrong DEK -> frame auth fails -> fatal.
        let err = Wal::open(dir.path(), Some(TestCipher::new(0x99)))
            .err()
            .unwrap();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn encrypted_torn_tail_is_dropped_but_a_mid_file_corruption_is_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let c = TestCipher::new(0x44);
        // Two encrypted frames in gen 0.
        {
            let (mut wal, _) = Wal::open(dir.path(), Some(c.clone())).unwrap();
            wal.append("poc", 1, b"one a=1i 1").unwrap();
            wal.append("poc", 1, b"two b=2i 2").unwrap();
        }
        // Append an incomplete trailing frame (a crash mid-append): a length
        // prefix promising more ciphertext than is present.
        {
            let mut f = OpenOptions::new()
                .append(true)
                .open(gen_path(dir.path(), 0))
                .unwrap();
            f.write_all(&64u32.to_le_bytes()).unwrap();
            f.write_all(b"not enough").unwrap();
        }
        let (_, replay) = Wal::open(dir.path(), Some(c.clone())).unwrap();
        assert_eq!(
            replay.len(),
            2,
            "torn trailing frame dropped, acked frames kept"
        );

        // Now corrupt a COMPLETE frame in the middle: flip a byte just after
        // the header. That frame authenticates no longer -> fatal, never a
        // silent truncation of everything after it.
        let path = gen_path(dir.path(), 0);
        let mut bytes = std::fs::read(&path).unwrap();
        let flip = 11; // past the 7-byte header + into the first frame
        bytes[flip] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        let err = Wal::open(dir.path(), Some(c)).err().unwrap();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }
}
