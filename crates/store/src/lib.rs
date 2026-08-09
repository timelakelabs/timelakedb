//! Store — the single chokepoint for ALL object I/O (SEC-1 seam).
//!
//! Every byte that reaches durable object storage passes through the
//! [`Store`] trait. Encryption ships as a decorator —
//! [`EncryptingStore`]`(inner, kms)` implementing this same trait — and
//! the engine never knows (SEC-1). An S3 backend adopts the
//! `object_store` crate behind this same seam (CL-1) without touching
//! callers.
//!
//! M2 backend: the local filesystem. Writes are atomic (temp file +
//! rename) and fsynced — a manifest committed through this layer is
//! durable or absent, never torn.

use std::io::Write;
use std::path::{Path, PathBuf};

pub mod encrypt;
pub use encrypt::{EncryptingStore, Kms, LocalKek, key_from_hex};

pub trait Store: Send + Sync + 'static {
    fn put(&self, path: &str, bytes: &[u8]) -> std::io::Result<()>;
    fn get(&self, path: &str) -> std::io::Result<Vec<u8>>;
    fn delete(&self, path: &str) -> std::io::Result<()>;
    /// Paths under `prefix`, sorted lexicographically (manifest replay
    /// relies on the ordering).
    fn list(&self, prefix: &str) -> std::io::Result<Vec<String>>;

    /// Size in bytes, without reading the object.
    fn size(&self, path: &str) -> std::io::Result<u64>;

    /// Read `len` bytes starting at `offset`. A short object yields what
    /// is there rather than an error, matching an HTTP range read.
    ///
    /// This is the seam that lets a reader take a Parquet footer and a
    /// couple of column chunks instead of the whole file — the measured
    /// reason row-group pruning could not pay (see
    /// docs/evidence/PERFORMANCE_LOG.md, 2026-08-08).
    fn get_range(&self, path: &str, offset: u64, len: usize) -> std::io::Result<Vec<u8>>;
}

impl<S: Store> Store for std::sync::Arc<S> {
    fn put(&self, path: &str, bytes: &[u8]) -> std::io::Result<()> {
        (**self).put(path, bytes)
    }
    fn get(&self, path: &str) -> std::io::Result<Vec<u8>> {
        (**self).get(path)
    }
    fn delete(&self, path: &str) -> std::io::Result<()> {
        (**self).delete(path)
    }
    fn list(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        (**self).list(prefix)
    }
    fn size(&self, path: &str) -> std::io::Result<u64> {
        (**self).size(path)
    }
    fn get_range(&self, path: &str, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        (**self).get_range(path, offset, len)
    }
}

/// The engine holds its store as `Arc<dyn Store>` so the encrypting
/// decorator (SEC-1) slots in by configuration, invisible to every caller.
impl Store for std::sync::Arc<dyn Store> {
    fn put(&self, path: &str, bytes: &[u8]) -> std::io::Result<()> {
        (**self).put(path, bytes)
    }
    fn get(&self, path: &str) -> std::io::Result<Vec<u8>> {
        (**self).get(path)
    }
    fn delete(&self, path: &str) -> std::io::Result<()> {
        (**self).delete(path)
    }
    fn list(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        (**self).list(prefix)
    }
    fn size(&self, path: &str) -> std::io::Result<u64> {
        (**self).size(path)
    }
    fn get_range(&self, path: &str, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        (**self).get_range(path, offset, len)
    }
}

pub struct LocalStore {
    root: PathBuf,
}

impl LocalStore {
    pub fn new(root: &Path) -> std::io::Result<LocalStore> {
        std::fs::create_dir_all(root)?;
        Ok(LocalStore {
            root: root.to_path_buf(),
        })
    }

    fn abs(&self, path: &str) -> PathBuf {
        // object paths use '/'; keep them relative and traversal-free
        let clean: Vec<&str> = path
            .split('/')
            .filter(|s| !s.is_empty() && *s != "." && *s != "..")
            .collect();
        let mut p = self.root.clone();
        for seg in clean {
            p.push(seg);
        }
        p
    }
}

impl Store for LocalStore {
    fn put(&self, path: &str, bytes: &[u8]) -> std::io::Result<()> {
        let dest = self.abs(path);
        if let Some(dir) = dest.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp = dest.with_extension("tmp-write");
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(bytes)?;
            f.sync_data()?;
        }
        std::fs::rename(&tmp, &dest)?;
        Ok(())
    }

    fn get(&self, path: &str) -> std::io::Result<Vec<u8>> {
        std::fs::read(self.abs(path))
    }

    fn size(&self, path: &str) -> std::io::Result<u64> {
        Ok(std::fs::metadata(self.abs(path))?.len())
    }

    fn get_range(&self, path: &str, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = std::fs::File::open(self.abs(path))?;
        f.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; len];
        // read_exact would fail at EOF; a range that runs past the end
        // should return the tail, as an HTTP range read does
        let mut filled = 0usize;
        while filled < len {
            match f.read(&mut buf[filled..])? {
                0 => break,
                n => filled += n,
            }
        }
        buf.truncate(filled);
        Ok(buf)
    }

    fn delete(&self, path: &str) -> std::io::Result<()> {
        match std::fs::remove_file(self.abs(path)) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            other => other,
        }
    }

    fn list(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        let dir = self.abs(prefix);
        let mut out = Vec::new();
        if dir.is_dir() {
            walk(&dir, &mut |p| {
                if let Ok(rel) = p.strip_prefix(&self.root) {
                    out.push(
                        rel.components()
                            .map(|c| c.as_os_str().to_string_lossy())
                            .collect::<Vec<_>>()
                            .join("/"),
                    );
                }
            })?;
        }
        out.sort();
        Ok(out)
    }
}

fn walk(dir: &Path, f: &mut impl FnMut(&Path)) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let p = entry?.path();
        if p.is_dir() {
            walk(&p, f)?;
        } else {
            f(&p);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_list_delete() {
        let dir = tempfile::tempdir().unwrap();
        let s = LocalStore::new(dir.path()).unwrap();
        s.put("poc/pipeline_events/data/2026080800/a.parquet", b"AAA")
            .unwrap();
        s.put("poc/pipeline_events/data/2026080801/b.parquet", b"BBB")
            .unwrap();
        s.put("catalog/manifest/000001.json", b"{}").unwrap();

        assert_eq!(
            s.get("poc/pipeline_events/data/2026080800/a.parquet")
                .unwrap(),
            b"AAA"
        );
        let files = s.list("poc/pipeline_events").unwrap();
        assert_eq!(files.len(), 2);
        assert!(files[0] < files[1], "list must be sorted");

        s.delete("catalog/manifest/000001.json").unwrap();
        s.delete("catalog/manifest/000001.json").unwrap(); // idempotent
        assert!(s.list("catalog").unwrap().is_empty());
        // traversal is neutralized
        s.put("../escape.txt", b"x").unwrap();
        assert!(dir.path().join("escape.txt").exists());
    }
}
