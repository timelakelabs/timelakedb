//! L0 -> L1 (table, hour) -> L2 (table, day) planner + executor.
//! PR-6 (<=10x fresh penalty) is won or lost here. Runs under the same
//! memory pool as queries (RR-1 applies to internal work). Arrives at M3.
//!
//! See ARCHITECTURE.md SS3 for the crate map.
