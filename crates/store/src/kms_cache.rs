//! Key caching for the [`Kms`] seam (ARCHITECTURE §12.2) — the
//! caching-CMM pattern: a remote KMS charges per call and adds a network
//! round trip to every flush and every cold read, so thousands of
//! objects must not mean thousands of calls.
//!
//! Two directions, two caches:
//!
//! - **generate** hands out ONE (dek, wrapped) pair for up to `max_age`
//!   or `max_uses` objects, then rotates. Objects still draw their own
//!   random nonce prefix, which is what makes bounded key reuse
//!   nonce-safe (~2⁻⁴⁵ collision odds at the default 1,000-use cap; the
//!   hard cap is 2¹⁶, far below any birthday concern).
//! - **unwrap** memoizes wrapped-blob → key, so re-opening files costs
//!   zero KMS calls, and every object sealed under one reused key hits
//!   one entry.
//!
//! Keys live only in process memory, bounded by the caps. Turning the
//! cache off (`TIMELORD_KMS_CACHE=off`) restores strict per-object keys
//! — which is also how the drill measures what the cache is worth.

use std::collections::HashMap;
use std::io::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::Kms;

/// Hard ceiling on `max_uses` — keeps any configuration comfortably
/// inside the nonce-collision analysis above.
pub const MAX_USES_CEILING: u32 = 1 << 16;

/// Call counters, shared with /metrics. `*_calls` count what reached the
/// inner (real) KMS; `*_hits` count what the cache absorbed.
#[derive(Default)]
pub struct KmsStats {
    pub generate_calls: AtomicU64,
    pub generate_hits: AtomicU64,
    pub decrypt_calls: AtomicU64,
    pub decrypt_hits: AtomicU64,
}

struct EncryptSlot {
    dek: [u8; 32],
    wrapped: Vec<u8>,
    born: Instant,
    uses: u32,
}

pub struct CachingKms<K: Kms> {
    inner: K,
    max_age: Duration,
    max_uses: u32,
    slot: Mutex<Option<EncryptSlot>>,
    /// wrapped blob → key. Bounded crudely, like the other caches here:
    /// entries are ~250 bytes and distinct wrapped blobs are few while
    /// generate() reuses keys.
    unwrapped: Mutex<HashMap<Vec<u8>, [u8; 32]>>,
    stats: Arc<KmsStats>,
}

impl<K: Kms> CachingKms<K> {
    pub fn new(inner: K, max_age: Duration, max_uses: u32) -> CachingKms<K> {
        CachingKms {
            inner,
            max_age,
            max_uses: max_uses.clamp(1, MAX_USES_CEILING),
            slot: Mutex::new(None),
            unwrapped: Mutex::new(HashMap::new()),
            stats: Arc::new(KmsStats::default()),
        }
    }

    /// Shared handle for /metrics.
    pub fn stats(&self) -> Arc<KmsStats> {
        self.stats.clone()
    }

    fn remember(&self, wrapped: &[u8], dek: [u8; 32]) {
        let mut c = self.unwrapped.lock().expect("kms cache lock");
        if c.len() > 4096 {
            c.clear();
        }
        c.insert(wrapped.to_vec(), dek);
    }
}

impl<K: Kms> Kms for CachingKms<K> {
    fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>> {
        // rare path (generate() is what encryption uses); no caching —
        // wrapping the same key twice legitimately differs per call
        self.inner.wrap(dek)
    }

    fn unwrap(&self, wrapped: &[u8]) -> Result<[u8; 32]> {
        if let Some(dek) = self.unwrapped.lock().expect("kms cache lock").get(wrapped) {
            self.stats.decrypt_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(*dek);
        }
        let dek = self.inner.unwrap(wrapped)?;
        self.stats.decrypt_calls.fetch_add(1, Ordering::Relaxed);
        self.remember(wrapped, dek);
        Ok(dek)
    }

    fn generate(&self) -> Result<([u8; 32], Vec<u8>)> {
        let mut slot = self.slot.lock().expect("kms slot lock");
        if let Some(s) = slot.as_mut()
            && s.uses < self.max_uses
            && s.born.elapsed() < self.max_age
        {
            s.uses += 1;
            self.stats.generate_hits.fetch_add(1, Ordering::Relaxed);
            return Ok((s.dek, s.wrapped.clone()));
        }
        let (dek, wrapped) = self.inner.generate()?;
        self.stats.generate_calls.fetch_add(1, Ordering::Relaxed);
        // our own objects must decrypt without a KMS round trip
        self.remember(&wrapped, dek);
        *slot = Some(EncryptSlot {
            dek,
            wrapped: wrapped.clone(),
            born: Instant::now(),
            uses: 1,
        });
        Ok((dek, wrapped))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encrypt::LocalKek;
    use crate::key_from_hex;

    /// A Kms that counts what actually reaches it.
    struct CountingKms {
        inner: LocalKek,
        generates: AtomicU64,
        unwraps: AtomicU64,
    }

    impl CountingKms {
        fn new() -> CountingKms {
            CountingKms {
                inner: LocalKek::new(key_from_hex(&"ee".repeat(32)).unwrap()),
                generates: AtomicU64::new(0),
                unwraps: AtomicU64::new(0),
            }
        }
    }

    impl Kms for CountingKms {
        fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>> {
            self.inner.wrap(dek)
        }
        fn unwrap(&self, wrapped: &[u8]) -> Result<[u8; 32]> {
            self.unwraps.fetch_add(1, Ordering::Relaxed);
            self.inner.unwrap(wrapped)
        }
        fn generate(&self) -> Result<([u8; 32], Vec<u8>)> {
            self.generates.fetch_add(1, Ordering::Relaxed);
            self.inner.generate()
        }
    }

    #[test]
    fn generate_reuses_within_the_window() {
        let k = CachingKms::new(CountingKms::new(), Duration::from_secs(300), 10);
        let (first, w1) = k.generate().unwrap();
        for _ in 0..8 {
            let (dek, w) = k.generate().unwrap();
            assert_eq!(dek, first);
            assert_eq!(w, w1);
        }
        // 9 uses so far; the 10th consumes the slot, the 11th rotates
        k.generate().unwrap();
        let (rotated, _) = k.generate().unwrap();
        assert_ne!(rotated, first, "max_uses must rotate the key");

        let inner_calls = k.inner.generates.load(Ordering::Relaxed);
        assert_eq!(inner_calls, 2, "11 objects, 2 KMS calls");
        assert_eq!(k.stats().generate_hits.load(Ordering::Relaxed), 9);
    }

    #[test]
    fn max_age_zero_rotates_every_time() {
        let k = CachingKms::new(CountingKms::new(), Duration::ZERO, 1000);
        let (a, _) = k.generate().unwrap();
        let (b, _) = k.generate().unwrap();
        assert_ne!(a, b, "an expired slot must not be reused");
        assert_eq!(k.inner.generates.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn unwrap_is_cached_and_seeded_by_generate() {
        let k = CachingKms::new(CountingKms::new(), Duration::from_secs(300), 100);
        let (dek, wrapped) = k.generate().unwrap();
        // our own key round-trips without touching the inner KMS
        assert_eq!(k.unwrap(&wrapped).unwrap(), dek);
        assert_eq!(k.unwrap(&wrapped).unwrap(), dek);
        assert_eq!(k.inner.unwraps.load(Ordering::Relaxed), 0);

        // a foreign blob costs one call, then is cached
        let other = CountingKms::new();
        let (odek, owrapped) = other.generate().unwrap();
        assert_eq!(k.unwrap(&owrapped).unwrap(), odek);
        assert_eq!(k.unwrap(&owrapped).unwrap(), odek);
        assert_eq!(k.inner.unwraps.load(Ordering::Relaxed), 1);
        assert_eq!(k.stats().decrypt_hits.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn uses_cap_is_clamped_to_the_ceiling() {
        let k = CachingKms::new(CountingKms::new(), Duration::from_secs(1), u32::MAX);
        assert_eq!(k.max_uses, MAX_USES_CEILING);
    }
}
