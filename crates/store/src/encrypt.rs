//! SEC-1: envelope encryption at the Store chokepoint.
//!
//! [`EncryptingStore`] wraps any [`Store`] and encrypts every object with
//! a fresh per-object data key (DEK), wrapped by a key-encryption key the
//! [`Kms`] holds. The engine never knows — callers see plaintext offsets,
//! plaintext lengths, and plaintext bytes on every method, so `FileMeta`
//! sizes, Parquet footers, and bloom probes all keep working unchanged.
//!
//! Objects are encrypted in fixed-size chunks (AES-256-GCM, one auth tag
//! per chunk) rather than as one stream, because the read path is built
//! on `get_range`: a bloom probe reads a few KB of a file and a footer
//! read takes the tail. A range read decrypts only the chunks it covers.
//! Whole-object Parquet Modular Encryption remains the evolution path for
//! per-column keys — it slots in at this same seam.
//!
//! Layout of an encrypted object:
//!
//! ```text
//! magic "TLDE1\0\0\0" (8) | chunk_size u32 LE | plaintext_len u64 LE |
//! nonce_prefix (8) | wrap_len u16 LE | wrapped_dek (wrap_len) |
//! chunk 0 ciphertext+tag | chunk 1 ciphertext+tag | ...
//! ```
//!
//! Chunk `i` uses nonce `nonce_prefix || i as u32 LE` and carries the
//! fixed header fields plus the object path as AAD — so chunks cannot be
//! reordered, spliced between objects, or re-homed under a renamed path,
//! and a tampered header (e.g. a shortened plaintext_len) fails every
//! chunk's tag instead of silently truncating the object.
//!
//! Objects WITHOUT the magic are passed through as plaintext: enabling
//! encryption on an existing data directory encrypts new writes while old
//! objects stay readable. The threat model is media at rest (a stolen
//! volume, a decommissioned disk), not an active writer inside the store.

use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use std::sync::{Arc, Mutex};

use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Aead, OsRng, Payload};
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};

use crate::Store;

const MAGIC: &[u8; 8] = b"TLDE1\0\0\0";
/// 64 KiB chunks: a bloom probe decrypts one chunk, a footer read one or
/// two, and the tag overhead is 16 bytes per 65536 (0.02%).
const CHUNK_SIZE: u32 = 64 * 1024;
const TAG_LEN: usize = 16;
/// magic + chunk_size + plaintext_len + nonce_prefix — the fixed fields,
/// which double as the AAD prefix.
const FIXED_LEN: usize = 8 + 4 + 8 + 8;
const WRAP_LEN_LEN: usize = 2;
/// Sanity cap on the wrapped-DEK field; LocalKek wraps to 60 bytes, a
/// remote KMS ciphertext is a few hundred.
const MAX_WRAP: usize = 4096;

/// Wraps and unwraps per-object data keys. v1 backend: [`LocalKek`].
/// A remote KMS (per-database or per-table key scoping, SEC-1's
/// "key scope configurable") implements this same trait.
pub trait Kms: Send + Sync + 'static {
    fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>>;
    fn unwrap(&self, wrapped: &[u8]) -> Result<[u8; 32]>;
}

/// A single key-encryption key held in memory, loaded from 64 hex chars
/// (`TIMELORD_ENCRYPTION_KEY`). Wrapping is AES-256-GCM with a random
/// nonce: `nonce (12) | ciphertext (32) | tag (16)`.
pub struct LocalKek {
    kek: [u8; 32],
}

impl LocalKek {
    pub fn new(kek: [u8; 32]) -> LocalKek {
        LocalKek { kek }
    }

    pub fn from_hex(s: &str) -> Result<LocalKek> {
        Ok(LocalKek {
            kek: key_from_hex(s)?,
        })
    }
}

/// Parse a 64-hex-char key, tolerating surrounding whitespace (key files
/// end in a newline).
pub fn key_from_hex(s: &str) -> Result<[u8; 32]> {
    let s = s.trim();
    if s.len() != 64 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "encryption key must be exactly 64 hex characters (32 bytes)",
        ));
    }
    let mut out = [0u8; 32];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hi = (chunk[0] as char).to_digit(16).unwrap() as u8;
        let lo = (chunk[1] as char).to_digit(16).unwrap() as u8;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

impl Kms for LocalKek {
    fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>> {
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.kek));
        let mut nonce = [0u8; 12];
        OsRng.fill_bytes(&mut nonce);
        let ct = cipher
            .encrypt(Nonce::from_slice(&nonce), dek.as_slice())
            .map_err(|_| Error::other("kek wrap failed"))?;
        let mut out = Vec::with_capacity(12 + ct.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ct);
        Ok(out)
    }

    fn unwrap(&self, wrapped: &[u8]) -> Result<[u8; 32]> {
        if wrapped.len() != 12 + 32 + TAG_LEN {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "wrapped DEK has wrong length",
            ));
        }
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.kek));
        let pt = cipher
            .decrypt(Nonce::from_slice(&wrapped[..12]), &wrapped[12..])
            .map_err(|_| {
                Error::new(
                    ErrorKind::InvalidData,
                    "DEK unwrap failed — wrong encryption key for this store?",
                )
            })?;
        let mut dek = [0u8; 32];
        dek.copy_from_slice(&pt);
        Ok(dek)
    }
}

/// Parsed header of one encrypted object, cached so a query's many range
/// reads against the same file parse and unwrap once.
enum ObjHeader {
    /// Object predates encryption (no magic): every call passes through.
    Plain,
    Encrypted {
        chunk_size: u32,
        plen: u64,
        header_len: usize,
        /// AAD fixed part (the header's fixed fields, verbatim).
        aad_fixed: [u8; FIXED_LEN],
        nonce_prefix: [u8; 8],
        dek: [u8; 32],
    },
}

pub struct EncryptingStore<S: Store> {
    inner: S,
    kms: Arc<dyn Kms>,
    /// path → parsed header. Bounded crudely, like the query meta cache:
    /// entries are ~100 bytes and files are few post-compaction.
    headers: Mutex<HashMap<String, Arc<ObjHeader>>>,
}

impl<S: Store> EncryptingStore<S> {
    pub fn new(inner: S, kms: Arc<dyn Kms>) -> EncryptingStore<S> {
        EncryptingStore {
            inner,
            kms,
            headers: Mutex::new(HashMap::new()),
        }
    }

    fn cache_put(&self, path: &str, h: Arc<ObjHeader>) {
        let mut c = self.headers.lock().expect("header cache lock");
        if c.len() > 4096 {
            c.clear();
        }
        c.insert(path.to_string(), h);
    }

    /// Parse an object's header from its leading bytes. `None` means the
    /// bytes are too short to hold the declared wrapped DEK (caller reads
    /// more); Plain means no magic.
    fn parse_header(&self, path: &str, head: &[u8]) -> Result<Option<ObjHeader>> {
        if head.len() < FIXED_LEN + WRAP_LEN_LEN || &head[..8] != MAGIC {
            return Ok(Some(ObjHeader::Plain));
        }
        let chunk_size = u32::from_le_bytes(head[8..12].try_into().unwrap());
        let plen = u64::from_le_bytes(head[12..20].try_into().unwrap());
        let mut nonce_prefix = [0u8; 8];
        nonce_prefix.copy_from_slice(&head[20..28]);
        let wrap_len = u16::from_le_bytes(
            head[FIXED_LEN..FIXED_LEN + WRAP_LEN_LEN]
                .try_into()
                .unwrap(),
        ) as usize;
        if chunk_size == 0 || wrap_len == 0 || wrap_len > MAX_WRAP {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("corrupt encryption header on {path}"),
            ));
        }
        let header_len = FIXED_LEN + WRAP_LEN_LEN + wrap_len;
        if head.len() < header_len {
            return Ok(None); // caller must fetch a longer prefix
        }
        let dek = self
            .kms
            .unwrap(&head[FIXED_LEN + WRAP_LEN_LEN..header_len])?;
        let mut aad_fixed = [0u8; FIXED_LEN];
        aad_fixed.copy_from_slice(&head[..FIXED_LEN]);
        Ok(Some(ObjHeader::Encrypted {
            chunk_size,
            plen,
            header_len,
            aad_fixed,
            nonce_prefix,
            dek,
        }))
    }

    /// The header for `path`, from cache or by probing the object's head.
    fn header(&self, path: &str) -> Result<Arc<ObjHeader>> {
        if let Some(h) = self.headers.lock().expect("header cache lock").get(path) {
            return Ok(h.clone());
        }
        // One probe covers any realistic wrapped-DEK size; re-read only
        // if a KMS produced something bigger.
        let head = self
            .inner
            .get_range(path, 0, FIXED_LEN + WRAP_LEN_LEN + 512)?;
        let parsed = match self.parse_header(path, &head)? {
            Some(h) => h,
            None => {
                let head = self
                    .inner
                    .get_range(path, 0, FIXED_LEN + WRAP_LEN_LEN + MAX_WRAP)?;
                self.parse_header(path, &head)?.ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidData,
                        format!("truncated header on {path}"),
                    )
                })?
            }
        };
        let h = Arc::new(parsed);
        self.cache_put(path, h.clone());
        Ok(h)
    }
}

fn chunk_nonce(prefix: &[u8; 8], idx: u32) -> [u8; 12] {
    let mut n = [0u8; 12];
    n[..8].copy_from_slice(prefix);
    n[8..].copy_from_slice(&idx.to_le_bytes());
    n
}

fn aad(fixed: &[u8; FIXED_LEN], path: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(FIXED_LEN + path.len());
    v.extend_from_slice(fixed);
    v.extend_from_slice(path.as_bytes());
    v
}

fn crypt_err(path: &str) -> Error {
    Error::new(
        ErrorKind::InvalidData,
        format!("decryption failed on {path} — wrong key or tampered object"),
    )
}

impl<S: Store> EncryptingStore<S> {
    fn decrypt_chunk(
        cipher: &Aes256Gcm,
        prefix: &[u8; 8],
        aad: &[u8],
        idx: u32,
        ct: &[u8],
        path: &str,
    ) -> Result<Vec<u8>> {
        cipher
            .decrypt(
                Nonce::from_slice(&chunk_nonce(prefix, idx)),
                Payload { msg: ct, aad },
            )
            .map_err(|_| crypt_err(path))
    }
}

impl<S: Store> Store for EncryptingStore<S> {
    fn put(&self, path: &str, bytes: &[u8]) -> Result<()> {
        let mut dek = [0u8; 32];
        OsRng.fill_bytes(&mut dek);
        let mut nonce_prefix = [0u8; 8];
        OsRng.fill_bytes(&mut nonce_prefix);
        let wrapped = self.kms.wrap(&dek)?;
        if wrapped.len() > MAX_WRAP {
            return Err(Error::new(ErrorKind::InvalidInput, "wrapped DEK too large"));
        }

        let mut fixed = [0u8; FIXED_LEN];
        fixed[..8].copy_from_slice(MAGIC);
        fixed[8..12].copy_from_slice(&CHUNK_SIZE.to_le_bytes());
        fixed[12..20].copy_from_slice(&(bytes.len() as u64).to_le_bytes());
        fixed[20..28].copy_from_slice(&nonce_prefix);
        let aad = aad(&fixed, path);

        let n_chunks = bytes.len().div_ceil(CHUNK_SIZE as usize);
        let mut out = Vec::with_capacity(
            FIXED_LEN + WRAP_LEN_LEN + wrapped.len() + bytes.len() + n_chunks * TAG_LEN,
        );
        out.extend_from_slice(&fixed);
        out.extend_from_slice(&(wrapped.len() as u16).to_le_bytes());
        out.extend_from_slice(&wrapped);

        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&dek));
        for (i, chunk) in bytes.chunks(CHUNK_SIZE as usize).enumerate() {
            let ct = cipher
                .encrypt(
                    Nonce::from_slice(&chunk_nonce(&nonce_prefix, i as u32)),
                    Payload {
                        msg: chunk,
                        aad: &aad,
                    },
                )
                .map_err(|_| Error::other("chunk encryption failed"))?;
            out.extend_from_slice(&ct);
        }
        let header_len = FIXED_LEN + WRAP_LEN_LEN + wrapped.len();
        self.inner.put(path, &out)?;
        self.cache_put(
            path,
            Arc::new(ObjHeader::Encrypted {
                chunk_size: CHUNK_SIZE,
                plen: bytes.len() as u64,
                header_len,
                aad_fixed: fixed,
                nonce_prefix,
                dek,
            }),
        );
        Ok(())
    }

    fn get(&self, path: &str) -> Result<Vec<u8>> {
        let raw = self.inner.get(path)?;
        let parsed = self.parse_header(path, &raw)?.ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                format!("truncated header on {path}"),
            )
        })?;
        let ObjHeader::Encrypted {
            chunk_size,
            plen,
            header_len,
            aad_fixed,
            nonce_prefix,
            dek,
        } = &parsed
        else {
            return Ok(raw); // plaintext passthrough
        };
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(dek));
        let aad = aad(aad_fixed, path);
        let cs = *chunk_size as usize;
        let mut out = Vec::with_capacity(*plen as usize);
        for (i, ct) in raw[*header_len..].chunks(cs + TAG_LEN).enumerate() {
            out.extend_from_slice(&Self::decrypt_chunk(
                &cipher,
                nonce_prefix,
                &aad,
                i as u32,
                ct,
                path,
            )?);
        }
        if out.len() as u64 != *plen {
            return Err(crypt_err(path));
        }
        self.cache_put(path, Arc::new(parsed));
        Ok(out)
    }

    fn get_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>> {
        let header = self.header(path)?;
        let ObjHeader::Encrypted {
            chunk_size,
            plen,
            header_len,
            aad_fixed,
            nonce_prefix,
            dek,
        } = header.as_ref()
        else {
            return self.inner.get_range(path, offset, len);
        };
        let end = (offset + len as u64).min(*plen);
        if offset >= end {
            return Ok(Vec::new()); // past EOF: short read, like the inner store
        }
        let cs = *chunk_size as u64;
        let ct_chunk = cs as usize + TAG_LEN;
        let first = (offset / cs) as usize;
        let last = ((end - 1) / cs) as usize;
        let n_chunks = plen.div_ceil(cs) as usize;

        // one contiguous ciphertext read covering the touched chunks
        let ct_start = *header_len as u64 + (first * ct_chunk) as u64;
        let last_pt = if last == n_chunks - 1 {
            (*plen - last as u64 * cs) as usize
        } else {
            cs as usize
        };
        let ct_len = (last - first) * ct_chunk + last_pt + TAG_LEN;
        let raw = self.inner.get_range(path, ct_start, ct_len)?;
        if raw.len() != ct_len {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("encrypted object {path} truncated (wanted {ct_len} ciphertext bytes)"),
            ));
        }

        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(dek));
        let aad = aad(aad_fixed, path);
        let mut plain = Vec::with_capacity((end - offset) as usize + cs as usize);
        for (i, ct) in raw.chunks(ct_chunk).enumerate() {
            plain.extend_from_slice(&Self::decrypt_chunk(
                &cipher,
                nonce_prefix,
                &aad,
                (first + i) as u32,
                ct,
                path,
            )?);
        }
        let skip = (offset - first as u64 * cs) as usize;
        Ok(plain[skip..skip + (end - offset) as usize].to_vec())
    }

    fn size(&self, path: &str) -> Result<u64> {
        match self.header(path)?.as_ref() {
            ObjHeader::Encrypted { plen, .. } => Ok(*plen),
            ObjHeader::Plain => self.inner.size(path),
        }
    }

    fn delete(&self, path: &str) -> Result<()> {
        self.headers.lock().expect("header cache lock").remove(path);
        self.inner.delete(path)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LocalStore;

    fn enc_store(dir: &std::path::Path) -> EncryptingStore<LocalStore> {
        let kek = key_from_hex(&"ab".repeat(32)).unwrap();
        EncryptingStore::new(LocalStore::new(dir).unwrap(), Arc::new(LocalKek::new(kek)))
    }

    #[test]
    fn roundtrip_and_at_rest_bytes_are_ciphertext() {
        let dir = tempfile::tempdir().unwrap();
        let s = enc_store(dir.path());
        let body: Vec<u8> = (0..200_000u32).flat_map(|i| i.to_le_bytes()).collect();
        s.put("db/t/data/2026080800/a.parquet", &body).unwrap();

        assert_eq!(s.get("db/t/data/2026080800/a.parquet").unwrap(), body);
        assert_eq!(
            s.size("db/t/data/2026080800/a.parquet").unwrap(),
            body.len() as u64
        );

        // what actually hit the disk is headered ciphertext, not the body
        let raw = std::fs::read(dir.path().join("db/t/data/2026080800/a.parquet")).unwrap();
        assert_eq!(&raw[..8], MAGIC);
        assert!(
            raw.len() > body.len(),
            "tags + header make it strictly larger"
        );
        let window: &[u8] = &body[1000..1064];
        assert!(
            !raw.windows(window.len()).any(|w| w == window),
            "plaintext must not appear in the stored object"
        );
    }

    #[test]
    fn range_reads_cross_chunk_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let s = enc_store(dir.path());
        let body: Vec<u8> = (0..300_000usize).map(|i| (i % 251) as u8).collect();
        s.put("o", &body).unwrap();

        let cs = CHUNK_SIZE as usize;
        for (off, len) in [
            (0usize, 10usize),
            (cs - 5, 10),          // straddles chunk 0/1
            (cs * 2 + 7, cs * 2),  // interior, multi-chunk
            (body.len() - 9, 100), // runs past EOF: short read
            (body.len() + 5, 4),   // wholly past EOF: empty
        ] {
            let got = s.get_range("o", off as u64, len).unwrap();
            let want = &body[off.min(body.len())..(off + len).min(body.len())];
            assert_eq!(got, want, "range {off}+{len}");
        }
    }

    #[test]
    fn tampering_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let s = enc_store(dir.path());
        s.put("o", b"the truth, durably").unwrap();

        let p = dir.path().join("o");
        let mut raw = std::fs::read(&p).unwrap();
        let last = raw.len() - 1;
        raw[last] ^= 1; // flip one ciphertext bit
        std::fs::write(&p, &raw).unwrap();

        let fresh = enc_store(dir.path()); // no warm cache
        assert!(fresh.get("o").is_err());
        assert!(fresh.get_range("o", 0, 10).is_err());

        // header tamper (shrink the declared length): AAD breaks every tag
        let mut raw = std::fs::read(&p).unwrap();
        raw[12] = 1; // plaintext_len low byte
        std::fs::write(&p, &raw).unwrap();
        let fresh = enc_store(dir.path());
        assert!(fresh.get("o").is_err());
    }

    #[test]
    fn wrong_key_is_a_clean_error() {
        let dir = tempfile::tempdir().unwrap();
        enc_store(dir.path()).put("o", b"secret").unwrap();

        let other = EncryptingStore::new(
            LocalStore::new(dir.path()).unwrap(),
            Arc::new(LocalKek::new(key_from_hex(&"cd".repeat(32)).unwrap())),
        );
        let err = other.get("o").unwrap_err().to_string();
        assert!(err.contains("wrong encryption key"), "got: {err}");
    }

    #[test]
    fn plaintext_objects_pass_through() {
        let dir = tempfile::tempdir().unwrap();
        // written before encryption was enabled
        LocalStore::new(dir.path())
            .unwrap()
            .put("old.json", b"{\"v\":1}")
            .unwrap();

        let s = enc_store(dir.path());
        assert_eq!(s.get("old.json").unwrap(), b"{\"v\":1}");
        assert_eq!(s.get_range("old.json", 1, 3).unwrap(), b"\"v\"");
        assert_eq!(s.size("old.json").unwrap(), 7);
    }

    #[test]
    fn empty_object_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let s = enc_store(dir.path());
        s.put("empty", b"").unwrap();
        assert_eq!(s.get("empty").unwrap(), b"");
        assert_eq!(s.size("empty").unwrap(), 0);
        assert_eq!(s.get_range("empty", 0, 8).unwrap(), b"");
    }

    #[test]
    fn key_parsing_rejects_garbage() {
        assert!(key_from_hex(&"ab".repeat(32)).is_ok());
        assert!(key_from_hex(&format!("  {}\n", "ab".repeat(32))).is_ok());
        assert!(key_from_hex("deadbeef").is_err());
        assert!(key_from_hex(&"zz".repeat(32)).is_err());
    }
}
