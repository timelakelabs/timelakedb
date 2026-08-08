//! Discovery — pluggable cluster membership (CL-5 seam).
//!
//! Backends: static config (v1, dev, bench) and Consul (v2: service
//! registration, health checks, sessions for role election such as the
//! per-shard compactor singleton).
//!
//! Two rules keep this honest (CL-5):
//! - Discovery informs routing and availability ONLY. A stale or lying
//!   membership view may waste work but can never corrupt state —
//!   correctness lives in catalog CAS.
//! - Leases are advisory. A double-fired compactor is safe: both outputs
//!   are valid file sets; catalog CAS accepts one, GC collects the loser.
//!
//! M0 placeholder: async signatures + watch streams arrive at M2/v2.

/// A cluster node's role, as reported by discovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Ingester,
    Querier,
    Compactor,
}

/// The membership seam (CL-5). Implementations: `StaticDiscovery` (v1),
/// `ConsulDiscovery` (v2).
pub trait Discovery: Send + Sync {
    fn members(&self, role: Role) -> Vec<String>;
    fn register(&self, role: Role, addr: &str);
    /// Advisory lease for role election; never a correctness primitive.
    fn lease(&self, name: &str) -> bool;
}
