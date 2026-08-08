//! Catalog — the manifest log that makes the object store the source of
//! truth (CL-1 seam).
//!
//! Every state change (flush, compaction, retention drop, schema add) is
//! a commit appending a manifest entry under `/catalog/manifest/`;
//! checkpoints bound replay. v1 is single-writer; v2 upgrades commits to
//! conditional-put CAS on the manifest head (CL-2/CL-3) without touching
//! callers. A local embedded cache accelerates reads and is disposable.
//!
//! M0 placeholder: real commit/snapshot types arrive at M2.

/// Marker for the catalog seam. At M2 this becomes
/// `trait Catalog { fn commit(&self, delta: ManifestDelta) -> Result<Seq>;
/// fn snapshot(&self) -> Result<CatalogSnapshot>; ... }`.
pub trait Catalog: Send + Sync {}
