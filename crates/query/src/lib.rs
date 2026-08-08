//! Query — DataFusion integration: session factory, TableProvider over
//! buffer + Parquet, memory pool + admission (RR-1), cancellation (RR-2),
//! and the mandatory-predicate injection point (SEC-2 seam).
//!
//! M0 placeholder: DataFusion arrives at M1/M2. What is fixed NOW is the
//! shape of the one security hook, called unconditionally for every table
//! scan and composed with AND *below* any user predicate — which is why
//! aggregate leakage is impossible by construction.

/// Per-session context the mandatory predicate sees (SEC-2).
/// Grows Accumulo-style authorizations, tenant, and retention context.
#[derive(Debug, Default, Clone)]
pub struct SessionContext {
    pub authorizations: Vec<String>,
}

/// THE injection point (SEC-2). v1 returns `None` (no restriction).
/// At M2 the return type becomes `Option<datafusion::logical_expr::Expr>`;
/// visibility labels, retention boundaries, and tenant scoping all arrive
/// as implementations of this one function.
pub fn mandatory_predicate(_session: &SessionContext, _table: &str) -> Option<String> {
    None
}
