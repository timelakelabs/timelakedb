//! Segmented WAL: group-commit fsync (<=10 ms window), replay on
//! boot (RR-3: writable <=30 s), segment upload via Store (CL-1).
//! Arrives at M1.
//!
//! See ARCHITECTURE.md SS3 for the crate map.
