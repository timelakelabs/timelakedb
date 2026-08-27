//! Layered configuration for TimeLakeDB (CONSOLE.md §3, timelakedb#109).
//!
//! A setting resolves `default < system property < stored override`: the
//! effective value is the highest layer that is set, every layer stays
//! visible, reverting an override falls back to the **property** (not the
//! default), and a property an operator changed while an override shadows it is
//! *reported*, never silently applied.
//!
//! This crate is the pure model — resolver, provenance, validation, and the
//! persisted document shape. It deliberately does NOT read the environment,
//! touch the object store, or know about `EngineConfig`. The server seeds the
//! property layer from `TIMELAKE_*`, loads/persists the override document
//! through its `Store`, and materialises the resolved values into
//! `EngineConfig` behind an `ArcSwap` (that wiring is phase B).

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// Where a setting lives: `Node` = local to one process, `Cluster` = shared in
/// the object store and polled by every node (§3.5/§3.8).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    Node,
    Cluster,
}

/// When a change takes effect: `Hot` on next use, `Staged` for work admitted
/// after the change, `Boot` only on restart (§3.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Apply {
    Hot,
    Staged,
    Boot,
}

/// The minimum role a change needs. Mirrors `timelake_auth::Role`, kept local
/// so this crate carries no auth dependency; the API layer maps one to the
/// other. Ordered `Viewer < Operator < Admin`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Viewer,
    Operator,
    Admin,
}

/// A setting's value type — enough to parse and bound-check its text form. The
/// bound is what differs between a count, a duration and a byte size, not the
/// parse, so one variant covers all three.
#[derive(Clone, Copy, Debug)]
pub enum Kind {
    /// An unsigned integer within an inclusive range.
    Uint { min: u64, max: u64 },
    /// Optional unsigned integer: an empty string or `off` is "unset" — the
    /// only kind for which the `explicit-none` override state (§3.3) is valid.
    OptUint { min: u64, max: u64 },
    /// One of a fixed set of string values (e.g. `off`/`optional`/`required`).
    Enum(&'static [&'static str]),
}

impl Kind {
    /// Bound-check a candidate value in its text form.
    fn check(&self, value: &str) -> Result<(), String> {
        match self {
            Kind::Uint { min, max } => {
                let n: u64 = value
                    .parse()
                    .map_err(|_| format!("`{value}` is not a non-negative integer"))?;
                if n < *min || n > *max {
                    return Err(format!("{n} is out of range [{min}, {max}]"));
                }
                Ok(())
            }
            Kind::OptUint { min, max } => {
                if value.is_empty() || value == "off" {
                    return Ok(());
                }
                let n: u64 = value
                    .parse()
                    .map_err(|_| format!("`{value}` is not `off` or a non-negative integer"))?;
                if n < *min || n > *max {
                    return Err(format!("{n} is out of range [{min}, {max}]"));
                }
                Ok(())
            }
            Kind::Enum(allowed) => {
                if allowed.contains(&value) {
                    Ok(())
                } else {
                    Err(format!("`{value}` is not one of {allowed:?}"))
                }
            }
        }
    }

    fn allows_explicit_none(&self) -> bool {
        matches!(self, Kind::OptUint { .. })
    }
}

/// One setting's metadata. `default` is the compiled-in value as text; a test
/// in the server pins these against `EngineConfig::default()` so the two
/// cannot drift.
#[derive(Clone, Copy, Debug)]
pub struct Spec {
    pub key: &'static str,
    pub env: &'static str,
    pub scope: Scope,
    pub apply: Apply,
    pub min_role: Role,
    pub kind: Kind,
    pub default: &'static str,
}

/// The settings inventory (§3.5), scalar tunables only. `retention` is a list
/// managed by its own store and folds in under the `retention.*` prefix at a
/// later phase; the boot-only display settings (`data_dir`, listener addrs,
/// key material) are added when the console renders them read-only.
pub static INVENTORY: &[Spec] = &[
    Spec {
        key: "flush_rows",
        env: "TIMELAKE_FLUSH_ROWS",
        scope: Scope::Node,
        apply: Apply::Hot,
        min_role: Role::Operator,
        kind: Kind::Uint {
            min: 1_000,
            max: 10_000_000,
        },
        default: "50000",
    },
    Spec {
        key: "flush_age_secs",
        env: "TIMELAKE_FLUSH_AGE_SECS",
        scope: Scope::Node,
        apply: Apply::Hot,
        min_role: Role::Operator,
        kind: Kind::Uint { min: 1, max: 3_600 },
        default: "60",
    },
    Spec {
        key: "wal_max_bytes",
        env: "TIMELAKE_WAL_MAX_BYTES",
        scope: Scope::Node,
        apply: Apply::Hot,
        min_role: Role::Operator,
        kind: Kind::Uint {
            min: 1 << 20,
            max: u64::MAX,
        },
        default: "2147483648",
    },
    Spec {
        key: "compact_min_files",
        env: "TIMELAKE_COMPACT_MIN_FILES",
        scope: Scope::Cluster,
        apply: Apply::Hot,
        min_role: Role::Operator,
        kind: Kind::Uint { min: 2, max: 64 },
        default: "4",
    },
    Spec {
        key: "l0_row_group_rows",
        env: "TIMELAKE_L0_ROW_GROUP_ROWS",
        scope: Scope::Node,
        apply: Apply::Hot,
        min_role: Role::Operator,
        kind: Kind::OptUint {
            min: 1_024,
            max: 10_000_000,
        },
        default: "",
    },
    Spec {
        key: "max_concurrent_queries",
        env: "TIMELAKE_MAX_CONCURRENT_QUERIES",
        scope: Scope::Node,
        apply: Apply::Staged,
        min_role: Role::Admin,
        kind: Kind::Uint { min: 1, max: 64 },
        default: "6",
    },
    Spec {
        key: "max_concurrent_queries_per_client",
        env: "TIMELAKE_MAX_CONCURRENT_QUERIES_PER_CLIENT",
        scope: Scope::Node,
        apply: Apply::Staged,
        min_role: Role::Admin,
        kind: Kind::Uint { min: 0, max: 64 },
        default: "4",
    },
    Spec {
        key: "query_timeout_secs",
        env: "TIMELAKE_QUERY_TIMEOUT_SECS",
        scope: Scope::Cluster,
        apply: Apply::Staged,
        min_role: Role::Operator,
        kind: Kind::Uint {
            min: 1,
            max: 86_400,
        },
        default: "600",
    },
    Spec {
        key: "gc_grace_secs",
        env: "TIMELAKE_GC_GRACE_SECS",
        scope: Scope::Cluster,
        apply: Apply::Hot,
        min_role: Role::Admin,
        kind: Kind::Uint {
            min: 1,
            max: 86_400,
        },
        default: "900",
    },
    Spec {
        key: "query_mem_bytes",
        env: "TIMELAKE_QUERY_MEM_BYTES",
        scope: Scope::Node,
        apply: Apply::Staged,
        min_role: Role::Admin,
        kind: Kind::Uint {
            min: 1 << 20,
            max: u64::MAX,
        },
        default: "1073741824",
    },
    Spec {
        key: "repl_timeout_ms",
        env: "TIMELAKE_REPL_TIMEOUT_MS",
        scope: Scope::Node,
        apply: Apply::Staged,
        min_role: Role::Admin,
        kind: Kind::Uint {
            min: 1,
            max: 60_000,
        },
        default: "250",
    },
    Spec {
        key: "max_body_bytes",
        env: "TIMELAKE_MAX_BODY_BYTES",
        scope: Scope::Node,
        apply: Apply::Boot,
        min_role: Role::Admin,
        kind: Kind::Uint {
            min: 1 << 20,
            max: u64::MAX,
        },
        default: "33554432",
    },
    Spec {
        key: "internal_max_concurrent",
        env: "TIMELAKE_INTERNAL_MAX_CONCURRENT",
        scope: Scope::Node,
        apply: Apply::Boot,
        min_role: Role::Admin,
        kind: Kind::Uint { min: 1, max: 256 },
        default: "8",
    },
    Spec {
        key: "data_auth",
        env: "TIMELAKE_DATA_AUTH",
        scope: Scope::Node,
        apply: Apply::Staged,
        min_role: Role::Admin,
        kind: Kind::Enum(&["off", "optional", "required"]),
        default: "off",
    },
];

/// Look a setting's spec up by key.
pub fn spec(key: &str) -> Option<&'static Spec> {
    INVENTORY.iter().find(|s| s.key == key)
}

/// Which layer produced the effective value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Source {
    Default,
    Property,
    Override,
}

/// The resolved value and where it came from.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Effective {
    pub value: String,
    pub source: Source,
}

/// The stored override for one key. `value: None` is the **explicit-none**
/// state (§3.3) — off here regardless of the property — and is distinct from
/// the key being absent from the map (which means "inherit the property").
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredOverride {
    /// `None` = explicit-none; `Some` = this value.
    pub value: Option<String>,
    /// The document revision at which this override was written.
    pub revision: u64,
    pub actor: String,
    /// RFC-3339 timestamp, supplied by the caller (this crate stamps no time).
    pub at: String,
    /// The property value when this override was written, so a later
    /// divergence is detectable (§3.2). `None` if there was no property then.
    #[serde(default)]
    pub property_at_write: Option<String>,
}

/// The persisted settings document (§3.7): one per scope, revision-stamped.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SettingsDoc {
    pub schema_version: u32,
    pub revision: u64,
    pub settings: BTreeMap<String, StoredOverride>,
}

pub const SCHEMA_VERSION: u32 = 1;

impl SettingsDoc {
    /// An empty document at revision 0.
    pub fn empty() -> Self {
        SettingsDoc {
            schema_version: SCHEMA_VERSION,
            revision: 0,
            settings: BTreeMap::new(),
        }
    }
}

/// A rejected change: nothing is applied and the reason names the offending
/// key or invariant (§3.6).
#[derive(Clone, Debug, PartialEq)]
pub enum ConfigError {
    /// No such setting.
    Unknown(String),
    /// The key is pinned to the property layer by system policy (§3.4).
    Pinned(String),
    /// The value failed its kind's parse or bounds.
    Invalid { key: String, reason: String },
    /// `explicit-none` (`null`) was used on a setting that cannot be unset.
    NullNotAllowed(String),
    /// A cross-field invariant would be violated by the change (§3.6).
    CrossField(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Unknown(k) => write!(f, "unknown setting `{k}`"),
            ConfigError::Pinned(k) => write!(f, "`{k}` is pinned by a system property"),
            ConfigError::Invalid { key, reason } => write!(f, "`{key}`: {reason}"),
            ConfigError::NullNotAllowed(k) => {
                write!(f, "`{k}` cannot be set to null — it has no unset state")
            }
            ConfigError::CrossField(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for ConfigError {}

/// A rejected write on the config surface: either the resolver refused the
/// change (§3.6 — maps to `409`), or the override store could not be written
/// (maps to `500`). Shared by the engine and the `/admin/config` handlers.
#[derive(Debug)]
pub enum ConfigSetError {
    Rejected(ConfigError),
    Store(String),
}

impl std::fmt::Display for ConfigSetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigSetError::Rejected(e) => write!(f, "{e}"),
            ConfigSetError::Store(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ConfigSetError {}

/// The layered configuration at runtime: the property layer (from env), the
/// override layer (persisted), and the pinned set. The default layer is the
/// inventory's compiled-in values.
#[derive(Clone, Debug)]
pub struct Layered {
    /// Only keys the environment set. Value is the property's text form.
    property: BTreeMap<String, String>,
    overrides: BTreeMap<String, StoredOverride>,
    pinned: BTreeSet<String>,
    revision: u64,
}

impl Layered {
    /// Seed the property layer (from env) and the pinned set; no overrides yet.
    pub fn new(property: BTreeMap<String, String>, pinned: BTreeSet<String>) -> Self {
        Layered {
            property,
            overrides: BTreeMap::new(),
            pinned,
            revision: 0,
        }
    }

    /// Seed the property + pinned layers and load a persisted override doc.
    pub fn load(
        property: BTreeMap<String, String>,
        pinned: BTreeSet<String>,
        doc: SettingsDoc,
    ) -> Self {
        Layered {
            property,
            overrides: doc.settings,
            pinned,
            revision: doc.revision,
        }
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn is_pinned(&self, key: &str) -> bool {
        self.pinned.contains(key)
    }

    /// The effective value and its source. `None` only for an unknown key.
    pub fn effective(&self, key: &str) -> Option<Effective> {
        let spec = spec(key)?;
        if let Some(ov) = self.overrides.get(key) {
            let value = match &ov.value {
                Some(v) => v.clone(),
                // explicit-none: the OptUint "unset" form.
                None => String::new(),
            };
            return Some(Effective {
                value,
                source: Source::Override,
            });
        }
        if let Some(p) = self.property.get(key) {
            return Some(Effective {
                value: p.clone(),
                source: Source::Property,
            });
        }
        Some(Effective {
            value: spec.default.to_string(),
            source: Source::Default,
        })
    }

    /// The effective values for every known setting, keyed by name.
    pub fn effective_all(&self) -> BTreeMap<String, String> {
        INVENTORY
            .iter()
            .map(|s| (s.key.to_string(), self.effective(s.key).unwrap().value))
            .collect()
    }

    /// Keys whose stored override was written against a property value that no
    /// longer matches the current one — the change is being shadowed (§3.2).
    pub fn divergent(&self) -> Vec<String> {
        let mut out = Vec::new();
        for (key, ov) in &self.overrides {
            if let Some(paw) = &ov.property_at_write {
                let now = self.property.get(key);
                if now != Some(paw) {
                    out.push(key.clone());
                }
            }
        }
        out
    }

    /// Full provenance for one setting (the §3.2 shape), or `None` if unknown.
    pub fn provenance(&self, key: &str) -> Option<serde_json::Value> {
        let spec = spec(key)?;
        let effective = self.effective(key).unwrap();
        let property = self.property.get(key);
        let over = self.overrides.get(key);
        let diverged = over.as_ref().and_then(|ov| {
            ov.property_at_write.as_ref().and_then(|paw| {
                let now = self.property.get(key);
                if now != Some(paw) {
                    Some(serde_json::json!({
                        "property_now": now,
                        "property_at_write": paw,
                    }))
                } else {
                    None
                }
            })
        });
        Some(serde_json::json!({
            "key": key,
            "effective": effective,
            "layers": {
                "default": spec.default,
                "property": property.map(|v| serde_json::json!({ "value": v, "env": spec.env })),
                "override": over.map(|ov| serde_json::json!({
                    "value": ov.value,
                    "revision": ov.revision,
                    "actor": ov.actor,
                    "at": ov.at,
                    "property_at_write": ov.property_at_write,
                })),
            },
            "scope": spec.scope,
            "apply": spec.apply,
            "min_role": spec.min_role,
            "pinned": self.is_pinned(key),
            "diverged": diverged,
        }))
    }

    /// Provenance for every setting.
    pub fn provenance_all(&self) -> Vec<serde_json::Value> {
        INVENTORY
            .iter()
            .map(|s| self.provenance(s.key).unwrap())
            .collect()
    }

    /// Set an override. `value = None` is explicit-none (`PUT null`); `Some` is
    /// a value. Validates the *proposed whole config* and applies nothing if it
    /// fails (§3.6). On success the revision bumps and `property_at_write`
    /// records the current property so a later divergence is detectable.
    pub fn set(
        &mut self,
        key: &str,
        value: Option<String>,
        actor: &str,
        at: &str,
    ) -> Result<u64, ConfigError> {
        let spec = spec(key).ok_or_else(|| ConfigError::Unknown(key.to_string()))?;
        if self.is_pinned(key) {
            return Err(ConfigError::Pinned(key.to_string()));
        }
        match &value {
            None => {
                if !spec.kind.allows_explicit_none() {
                    return Err(ConfigError::NullNotAllowed(key.to_string()));
                }
            }
            Some(v) => spec.kind.check(v).map_err(|reason| ConfigError::Invalid {
                key: key.to_string(),
                reason,
            })?,
        }

        // Validate the whole config as it WOULD be after the change, so a
        // cross-field invariant is checked against the proposed state, not the
        // current one (§3.6). Apply to a trial copy first.
        let mut trial = self.overrides.clone();
        trial.insert(
            key.to_string(),
            StoredOverride {
                value: value.clone(),
                revision: self.revision + 1,
                actor: actor.to_string(),
                at: at.to_string(),
                property_at_write: self.property.get(key).cloned(),
            },
        );
        let probe = Layered {
            property: self.property.clone(),
            overrides: trial,
            pinned: self.pinned.clone(),
            revision: self.revision + 1,
        };
        validate_cross_field(&probe)?;

        // Commit.
        self.overrides = probe.overrides;
        self.revision += 1;
        Ok(self.revision)
    }

    /// Remove an override, reverting to the property (or the default). Returns
    /// whether an override was actually present. Bumps the revision if so.
    pub fn revert(&mut self, key: &str) -> Result<bool, ConfigError> {
        if spec(key).is_none() {
            return Err(ConfigError::Unknown(key.to_string()));
        }
        if self.is_pinned(key) {
            return Err(ConfigError::Pinned(key.to_string()));
        }
        let removed = self.overrides.remove(key).is_some();
        if removed {
            self.revision += 1;
        }
        Ok(removed)
    }

    /// The persisted document for the current override layer.
    pub fn to_doc(&self) -> SettingsDoc {
        SettingsDoc {
            schema_version: SCHEMA_VERSION,
            revision: self.revision,
            settings: self.overrides.clone(),
        }
    }
}

/// The cross-field invariants that would break a guarantee (§3.6). Advisory
/// bounds live on each `Kind`; this is the small set of relations between
/// settings. Runs over the *effective* values.
fn validate_cross_field(layered: &Layered) -> Result<(), ConfigError> {
    let eff = layered.effective_all();
    // gc_grace_secs must exceed query_timeout_secs, or a query's catalog
    // snapshot can outlive its files while it still runs (the AT-3 race).
    let gc: u64 = eff["gc_grace_secs"].parse().unwrap_or(0);
    let qt: u64 = eff["query_timeout_secs"].parse().unwrap_or(0);
    if gc <= qt {
        return Err(ConfigError::CrossField(format!(
            "gc_grace_secs ({gc}s) must exceed query_timeout_secs ({qt}s) — a query's \
             catalog snapshot would outlive its files (the AT-3 compaction-vs-query race)"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layered() -> Layered {
        // A property layer that sets a couple of keys; the rest fall to default.
        let mut prop = BTreeMap::new();
        prop.insert("gc_grace_secs".to_string(), "1200".to_string());
        prop.insert("flush_rows".to_string(), "80000".to_string());
        Layered::new(prop, BTreeSet::new())
    }

    #[test]
    fn resolution_order_is_default_then_property_then_override() {
        let mut l = layered();
        // default (no property, no override)
        assert_eq!(
            l.effective("flush_age_secs").unwrap().source,
            Source::Default
        );
        assert_eq!(l.effective("flush_age_secs").unwrap().value, "60");
        // property beats default
        let e = l.effective("gc_grace_secs").unwrap();
        assert_eq!((e.source, e.value.as_str()), (Source::Property, "1200"));
        // override beats property
        l.set("gc_grace_secs", Some("1500".into()), "rc", "t0")
            .unwrap();
        let e = l.effective("gc_grace_secs").unwrap();
        assert_eq!((e.source, e.value.as_str()), (Source::Override, "1500"));
    }

    #[test]
    fn revert_falls_back_to_the_property_not_the_default() {
        let mut l = layered();
        l.set("flush_rows", Some("120000".into()), "rc", "t0")
            .unwrap();
        assert_eq!(l.effective("flush_rows").unwrap().value, "120000");
        assert!(l.revert("flush_rows").unwrap());
        // back to the PROPERTY (80000), not the default (50000).
        let e = l.effective("flush_rows").unwrap();
        assert_eq!((e.source, e.value.as_str()), (Source::Property, "80000"));
    }

    #[test]
    fn explicit_none_is_distinct_from_absent_and_only_valid_for_optuint() {
        let mut l = layered();
        // l0_row_group_rows is OptUint — null is allowed and means "off".
        l.set("l0_row_group_rows", None, "rc", "t0").unwrap();
        let e = l.effective("l0_row_group_rows").unwrap();
        assert_eq!((e.source, e.value.as_str()), (Source::Override, ""));
        // a required scalar cannot be nulled.
        assert_eq!(
            l.set("gc_grace_secs", None, "rc", "t0"),
            Err(ConfigError::NullNotAllowed("gc_grace_secs".into()))
        );
    }

    #[test]
    fn bounds_are_enforced_and_reject_applies_nothing() {
        let mut l = layered();
        assert!(matches!(
            l.set("compact_min_files", Some("100".into()), "rc", "t0"),
            Err(ConfigError::Invalid { .. })
        ));
        // unchanged — still the default.
        assert_eq!(
            l.effective("compact_min_files").unwrap().source,
            Source::Default
        );
        // enum too.
        assert!(matches!(
            l.set("data_auth", Some("maybe".into()), "rc", "t0"),
            Err(ConfigError::Invalid { .. })
        ));
    }

    #[test]
    fn cross_field_gc_grace_must_exceed_query_timeout() {
        let mut l = layered();
        // default query_timeout is 600; gc_grace property is 1200. Lowering
        // gc_grace to 300 (< 600) must be refused, naming the invariant.
        let err = l
            .set("gc_grace_secs", Some("300".into()), "rc", "t0")
            .unwrap_err();
        match err {
            ConfigError::CrossField(m) => {
                assert!(
                    m.contains("gc_grace_secs") && m.contains("query_timeout_secs"),
                    "{m}"
                );
            }
            other => panic!("expected cross-field error, got {other:?}"),
        }
        // and the value did not change.
        assert_eq!(l.effective("gc_grace_secs").unwrap().value, "1200");
        // raising query_timeout above gc_grace is the same invariant, other side.
        assert!(matches!(
            l.set("query_timeout_secs", Some("2000".into()), "rc", "t0"),
            Err(ConfigError::CrossField(_))
        ));
    }

    #[test]
    fn pinned_keys_reject_writes_and_reverts() {
        let mut pinned = BTreeSet::new();
        pinned.insert("gc_grace_secs".to_string());
        let mut l = Layered::new(BTreeMap::new(), pinned);
        assert_eq!(
            l.set("gc_grace_secs", Some("1000".into()), "rc", "t0"),
            Err(ConfigError::Pinned("gc_grace_secs".into()))
        );
        assert_eq!(
            l.revert("gc_grace_secs"),
            Err(ConfigError::Pinned("gc_grace_secs".into()))
        );
    }

    #[test]
    fn divergence_is_detected_when_the_property_changes_under_an_override() {
        let mut prop = BTreeMap::new();
        prop.insert("gc_grace_secs".to_string(), "1200".to_string());
        let mut l = Layered::new(prop, BTreeSet::new());
        l.set("gc_grace_secs", Some("1500".into()), "rc", "t0")
            .unwrap();
        assert!(l.divergent().is_empty());
        // operator changes the deployment property; the override still wins but
        // is now shadowing a different property.
        l.property
            .insert("gc_grace_secs".to_string(), "1800".to_string());
        assert_eq!(l.divergent(), vec!["gc_grace_secs".to_string()]);
        let prov = l.provenance("gc_grace_secs").unwrap();
        assert_eq!(prov["diverged"]["property_at_write"], "1200");
        assert_eq!(prov["diverged"]["property_now"], "1800");
    }

    #[test]
    fn document_round_trips_through_serde() {
        let mut l = layered();
        l.set(
            "flush_rows",
            Some("120000".into()),
            "rc",
            "2026-08-27T00:00:00Z",
        )
        .unwrap();
        l.set("l0_row_group_rows", None, "rc", "2026-08-27T00:00:01Z")
            .unwrap();
        let doc = l.to_doc();
        let json = serde_json::to_vec(&doc).unwrap();
        let back: SettingsDoc = serde_json::from_slice(&json).unwrap();
        assert_eq!(doc, back);
        assert_eq!(back.revision, 2);
        // re-load reproduces the same effective values.
        let l2 = Layered::load(l.property.clone(), BTreeSet::new(), back);
        assert_eq!(l2.effective("flush_rows").unwrap().value, "120000");
        assert_eq!(
            l2.effective("l0_row_group_rows").unwrap().source,
            Source::Override
        );
    }

    #[test]
    fn provenance_shows_the_whole_stack() {
        let mut l = layered();
        l.set(
            "gc_grace_secs",
            Some("1500".into()),
            "rcowell",
            "2026-08-27T09:52:11Z",
        )
        .unwrap();
        let p = l.provenance("gc_grace_secs").unwrap();
        assert_eq!(p["effective"]["value"], "1500");
        assert_eq!(p["effective"]["source"], "override");
        assert_eq!(p["layers"]["default"], "900");
        assert_eq!(p["layers"]["property"]["value"], "1200");
        assert_eq!(p["layers"]["property"]["env"], "TIMELAKE_GC_GRACE_SECS");
        assert_eq!(p["layers"]["override"]["value"], "1500");
        assert_eq!(p["layers"]["override"]["actor"], "rcowell");
        assert_eq!(p["scope"], "cluster");
        assert_eq!(p["apply"], "hot");
        assert_eq!(p["min_role"], "admin");
    }
}
