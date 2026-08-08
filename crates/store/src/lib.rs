//! Store — the single chokepoint for ALL object I/O (SEC-1 seam).
//!
//! Every byte that reaches the object store passes through this trait.
//! Encryption ships later as a decorator (`EncryptingStore(inner, kms)`)
//! implementing this same trait — the engine never knows (SEC-1).
//!
//! M0 placeholder: signatures use owned bytes and string paths; they
//! become `object_store` types (paths, streams, multipart) at M2.

/// The object-I/O seam. Implementations at M2: `ObjectStoreImpl`
/// (wrapping the `object_store` crate) and, later, `EncryptingStore`.
pub trait Store: Send + Sync {
    fn put(&self, path: &str, bytes: Vec<u8>) -> std::io::Result<()>;
    fn get(&self, path: &str) -> std::io::Result<Vec<u8>>;
    fn delete(&self, path: &str) -> std::io::Result<()>;
    fn list(&self, prefix: &str) -> std::io::Result<Vec<String>>;
}
