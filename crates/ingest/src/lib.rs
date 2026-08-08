//! Line-protocol parser (zero-copy), schema normalization, type
//! checks (FR-1). Nothing here may scale with historical distinct-key
//! count (PR-2). Arrives at M1.
//!
//! See ARCHITECTURE.md SS3 for the crate map.
