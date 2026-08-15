//! Per-client admission cap (SEC-6, exposure 6).
//!
//! The global admission semaphore (RR-1, `timelake_query::QueryEnv`) bounds
//! the TOTAL number of concurrent queries so the memory pool is never
//! oversubscribed. It does nothing to stop ONE client from taking every
//! permit and starving the rest — the DoS this closes.
//!
//! This caps how many queries a single client may hold at once, keyed by
//! the data-plane token when the caller presents one and by network origin
//! otherwise (the handlers compute the key; see `Engine::admit_client`).
//! It is a CONCURRENCY cap, not a rate/QPS limit — it sits in front of the
//! same semaphore and speaks the same language.
//!
//! Reject, not queue: a client already at its cap is refused (HTTP 429 /
//! Flight `ResourceExhausted`) rather than queued. A refusal is a signal an
//! operator and a blue-team probe can both see; a silent delay is neither.
//! The default cap is deliberately below the global budget so at least one
//! permit is always reachable by another client; raise both together for a
//! single-tenant deployment whose one dashboard issues many concurrent
//! panels (`timelake_query_rate_limited_total` makes a too-low cap visible).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// Tracks in-flight query counts per client key and refuses a client that
/// is already at `cap`. `cap == 0` disables the cap entirely.
pub struct ClientLimiter {
    cap: usize,
    active: Mutex<HashMap<String, usize>>,
    rejected: AtomicU64,
}

impl ClientLimiter {
    pub fn new(cap: usize) -> Arc<Self> {
        Arc::new(ClientLimiter {
            cap,
            active: Mutex::new(HashMap::new()),
            rejected: AtomicU64::new(0),
        })
    }

    /// Admit one query for `key`, returning a guard to hold for the query's
    /// lifetime, or `None` if this key is already at `cap` (the caller then
    /// refuses the request). A `None` key — an unidentifiable client, which
    /// under the open default is one the transport could not attribute — is
    /// always admitted: there is nothing to key on fairly, so the global
    /// admission semaphore remains the only bound.
    pub fn admit(self: &Arc<Self>, key: Option<String>) -> Option<ClientGuard> {
        let key = match key {
            _ if self.cap == 0 => return Some(ClientGuard::noop()),
            None => return Some(ClientGuard::noop()),
            Some(k) => k,
        };
        let mut active = self.active.lock().unwrap();
        let n = active.entry(key.clone()).or_insert(0);
        if *n >= self.cap {
            // A fresh entry can only be 0, which is < cap for cap >= 1, so
            // this branch never leaves a spurious zero entry behind.
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        *n += 1;
        Some(ClientGuard {
            limiter: Some(self.clone()),
            key,
        })
    }

    pub fn rejected(&self) -> u64 {
        self.rejected.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn tracked_keys(&self) -> usize {
        self.active.lock().unwrap().len()
    }
}

/// Releases the client's slot on drop. The map entry is removed when a
/// client's last in-flight query ends, so distinct keys (client IPs) never
/// accumulate beyond the set currently running something.
pub struct ClientGuard {
    limiter: Option<Arc<ClientLimiter>>,
    key: String,
}

impl ClientGuard {
    fn noop() -> Self {
        ClientGuard {
            limiter: None,
            key: String::new(),
        }
    }
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        if let Some(l) = &self.limiter {
            let mut active = l.active.lock().unwrap();
            if let Some(n) = active.get_mut(&self.key) {
                *n -= 1;
                if *n == 0 {
                    active.remove(&self.key);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caps_a_single_client_and_releases_on_drop() {
        let l = ClientLimiter::new(2);
        let a = l.admit(Some("ip:1".into())).expect("1st admitted");
        let b = l.admit(Some("ip:1".into())).expect("2nd admitted");
        assert!(
            l.admit(Some("ip:1".into())).is_none(),
            "3rd over cap refused"
        );
        assert_eq!(l.rejected(), 1);
        drop(a);
        // a slot freed -> the client can run one more
        let c = l
            .admit(Some("ip:1".into()))
            .expect("admitted after a freed");
        drop((b, c));
        // all released -> the key is forgotten, no unbounded growth
        assert_eq!(l.tracked_keys(), 0);
    }

    #[test]
    fn clients_are_independent() {
        let l = ClientLimiter::new(1);
        let _a = l.admit(Some("tok:x".into())).expect("x admitted");
        assert!(l.admit(Some("tok:x".into())).is_none(), "x at cap");
        let _b = l
            .admit(Some("tok:y".into()))
            .expect("y is a different client");
    }

    #[test]
    fn a_zero_cap_disables_and_an_unkeyed_client_is_never_capped() {
        let disabled = ClientLimiter::new(0);
        for _ in 0..100 {
            assert!(disabled.admit(Some("ip:1".into())).is_some());
        }
        let l = ClientLimiter::new(1);
        // None key: nothing to key on, so never refused (global cap still applies).
        let _a = l.admit(None).expect("unkeyed admitted");
        let _b = l.admit(None).expect("unkeyed never capped");
        assert_eq!(l.rejected(), 0);
    }
}
