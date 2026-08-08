//! Mutable Arrow builders per (table, partition); immutable snapshots
//! for readers (PR-9); flush to Parquet at 128 MB / 5 min. Backpressure
//! at budget (429 + Retry-After, RR-5). Arrives at M1.
//!
//! See ARCHITECTURE.md SS3 for the crate map.
