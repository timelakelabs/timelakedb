//! In-memory last-value cache (#57): the latest `(timestamp, fields)` per
//! series, so a "current value per entity" query answers from one cached row
//! instead of planning and scanning files.
//!
//! Two invariants it must never break — both the kind that pass a demo and fail
//! on real data:
//!
//! - **Out-of-order writes.** A late write with an *older* timestamp must not
//!   overwrite the cached latest, or "current value" flickers backwards
//!   silently. Every update is guarded by `incoming_ts > cached_ts`.
//! - **Bounded memory.** One entry per series is *exactly* the per-series blowup
//!   FR-2 forbids — a million entities would be a million entries. The cache is
//!   capped with LRU eviction, so it accelerates HOT series (recently written),
//!   not all series. That is the promise, stated up front.
//!
//! It is opt-in per `(db, table)`: a table is cached only after `enable`, so the
//! write-path cost lands only where an operator asked for it. When nothing is
//! enabled, [`LastValueCache::is_active`] is a lock-free `false` and the write
//! path does no work at all.

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use lru::LruCache;
use timelake_ingest::FieldValue;

/// A cached latest reading for one series.
#[derive(Debug, Clone, PartialEq)]
pub struct LastValue {
    /// Sorted tag key/value pairs — the series identity minus time and fields.
    pub tags: Vec<(String, String)>,
    pub timestamp_ns: i64,
    pub fields: Vec<(String, FieldValue)>,
}

/// The default cap: 100k hot series. Chosen so the cache is a bounded accelerator
/// (a few MB) rather than a shadow copy of a high-cardinality table.
pub const DEFAULT_CAP: usize = 100_000;

struct Inner {
    /// `db\0table` for each opted-in table.
    enabled: HashSet<String>,
    /// series key -> latest reading, LRU-evicted at the cap.
    map: LruCache<String, LastValue>,
}

pub struct LastValueCache {
    /// Lock-free gate: true iff at least one table is enabled. The write path
    /// checks this per batch and, in the common case where no table opted in,
    /// never takes the lock.
    active: AtomicBool,
    inner: Mutex<Inner>,
}

impl LastValueCache {
    pub fn new(cap: usize) -> LastValueCache {
        LastValueCache {
            active: AtomicBool::new(false),
            inner: Mutex::new(Inner {
                enabled: HashSet::new(),
                map: LruCache::new(NonZeroUsize::new(cap.max(1)).unwrap()),
            }),
        }
    }

    pub fn with_default_cap() -> LastValueCache {
        LastValueCache::new(DEFAULT_CAP)
    }

    /// Whether any table is cached — a lock-free read the hot write path gates on.
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    /// Turn caching on for a table. Idempotent.
    pub fn enable(&self, db: &str, table: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.enabled.insert(tt(db, table));
        self.active
            .store(!inner.enabled.is_empty(), Ordering::Relaxed);
    }

    /// Turn caching off for a table and drop its cached rows — once it can't be
    /// queried, keeping the entries is just wasted memory.
    pub fn disable(&self, db: &str, table: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.enabled.remove(&tt(db, table));
        self.active
            .store(!inner.enabled.is_empty(), Ordering::Relaxed);
        let prefix = key_prefix(db, table);
        let stale: Vec<String> = inner
            .map
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(k, _)| k.clone())
            .collect();
        for k in stale {
            inner.map.pop(&k);
        }
    }

    pub fn is_enabled(&self, db: &str, table: &str) -> bool {
        self.inner.lock().unwrap().enabled.contains(&tt(db, table))
    }

    /// Every enabled `(db, table)`, sorted — for the admin list view.
    pub fn enabled_tables(&self) -> Vec<(String, String)> {
        let inner = self.inner.lock().unwrap();
        let mut out: Vec<(String, String)> = inner.map_enabled().collect();
        out.sort();
        out
    }

    /// Write-path update for one row. A no-op unless the table is enabled. Only a
    /// strictly-newer timestamp overwrites the cached latest; an equal or older
    /// one is ignored (out-of-order guard) but still marks the series recently
    /// used, since it is being written.
    pub fn observe(
        &self,
        db: &str,
        table: &str,
        tags: &[(String, String)],
        timestamp_ns: i64,
        fields: &[(String, FieldValue)],
    ) {
        let mut inner = self.inner.lock().unwrap();
        if !inner.enabled.contains(&tt(db, table)) {
            return;
        }
        let mut sorted = tags.to_vec();
        sorted.sort();
        let key = series_key(db, table, &sorted);
        if let Some(e) = inner.map.get_mut(&key) {
            if timestamp_ns > e.timestamp_ns {
                e.timestamp_ns = timestamp_ns;
                e.fields = fields.to_vec();
            }
        } else {
            inner.map.put(
                key,
                LastValue {
                    tags: sorted,
                    timestamp_ns,
                    fields: fields.to_vec(),
                },
            );
        }
    }

    /// The cached latest rows for one table (the hot series only). Cloned out, so
    /// the caller holds no lock. O(cache size) — a query-time cost, not a write
    /// one.
    pub fn snapshot(&self, db: &str, table: &str) -> Vec<LastValue> {
        let inner = self.inner.lock().unwrap();
        let prefix = key_prefix(db, table);
        inner
            .map
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// Live entry count, for the `timelake_last_value_entries` gauge.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Inner {
    /// Split the `db\0table` enabled keys back into pairs.
    fn map_enabled(&self) -> impl Iterator<Item = (String, String)> + '_ {
        self.enabled.iter().filter_map(|k| {
            k.split_once('\0')
                .map(|(d, t)| (d.to_string(), t.to_string()))
        })
    }
}

/// `db\0table` — the enabled-set key and the series-key prefix root.
fn tt(db: &str, table: &str) -> String {
    format!("{db}\0{table}")
}

/// `db\0table\0` — every series key of this table starts with it, and the
/// trailing `\0` stops `table` from prefix-matching `table2`.
fn key_prefix(db: &str, table: &str) -> String {
    format!("{db}\0{table}\0")
}

/// `db\0table\0k1=v1\0k2=v2…` over SORTED tags, so a series has exactly one key
/// regardless of the order tags arrived in.
fn series_key(db: &str, table: &str, sorted_tags: &[(String, String)]) -> String {
    let mut k = key_prefix(db, table);
    for (i, (tk, tv)) in sorted_tags.iter().enumerate() {
        if i > 0 {
            k.push('\0');
        }
        k.push_str(tk);
        k.push('=');
        k.push_str(tv);
    }
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }
    fn f(v: f64) -> Vec<(String, FieldValue)> {
        vec![("value".to_string(), FieldValue::Float(v))]
    }

    #[test]
    fn disabled_table_is_a_no_op() {
        let c = LastValueCache::with_default_cap();
        assert!(!c.is_active());
        c.observe("db", "cpu", &tags(&[("host", "a")]), 10, &f(1.0));
        assert!(
            c.snapshot("db", "cpu").is_empty(),
            "nothing cached until enabled"
        );
    }

    #[test]
    fn caches_latest_per_series_once_enabled() {
        let c = LastValueCache::with_default_cap();
        c.enable("db", "cpu");
        assert!(c.is_active());
        c.observe("db", "cpu", &tags(&[("host", "a")]), 10, &f(1.0));
        c.observe("db", "cpu", &tags(&[("host", "b")]), 10, &f(2.0));
        c.observe("db", "cpu", &tags(&[("host", "a")]), 20, &f(3.0)); // newer for a
        let snap = c.snapshot("db", "cpu");
        assert_eq!(snap.len(), 2, "one entry per series");
        let a = snap
            .iter()
            .find(|v| v.tags == tags(&[("host", "a")]))
            .unwrap();
        assert_eq!(a.timestamp_ns, 20);
        assert_eq!(a.fields, f(3.0));
    }

    #[test]
    fn an_out_of_order_older_write_does_not_move_current_value_backwards() {
        let c = LastValueCache::with_default_cap();
        c.enable("db", "cpu");
        c.observe("db", "cpu", &tags(&[("host", "a")]), 20, &f(3.0));
        // a delayed sample with an OLDER timestamp lands
        c.observe("db", "cpu", &tags(&[("host", "a")]), 10, &f(999.0));
        let a = &c.snapshot("db", "cpu")[0];
        assert_eq!(a.timestamp_ns, 20, "latest timestamp unchanged");
        assert_eq!(a.fields, f(3.0), "value did not flicker backwards");
    }

    #[test]
    fn tag_order_does_not_split_a_series() {
        let c = LastValueCache::with_default_cap();
        c.enable("db", "cpu");
        c.observe(
            "db",
            "cpu",
            &tags(&[("host", "a"), ("dc", "x")]),
            10,
            &f(1.0),
        );
        c.observe(
            "db",
            "cpu",
            &tags(&[("dc", "x"), ("host", "a")]),
            20,
            &f(2.0),
        );
        assert_eq!(
            c.snapshot("db", "cpu").len(),
            1,
            "same series regardless of tag order"
        );
    }

    #[test]
    fn the_cap_holds_under_more_series_than_it_can_hold() {
        let c = LastValueCache::new(100);
        c.enable("db", "cpu");
        for i in 0..1000 {
            c.observe(
                "db",
                "cpu",
                &tags(&[("host", &format!("h{i}"))]),
                i,
                &f(i as f64),
            );
        }
        assert_eq!(c.len(), 100, "entry count never exceeds the cap");
        // The most-recently-written series survive (LRU keeps hot ones).
        let snap = c.snapshot("db", "cpu");
        assert!(
            snap.iter().any(|v| v.tags == tags(&[("host", "h999")])),
            "newest series is present"
        );
        assert!(
            !snap.iter().any(|v| v.tags == tags(&[("host", "h0")])),
            "oldest series was evicted"
        );
    }

    #[test]
    fn disable_drops_the_tables_entries() {
        let c = LastValueCache::with_default_cap();
        c.enable("db", "cpu");
        c.observe("db", "cpu", &tags(&[("host", "a")]), 10, &f(1.0));
        assert_eq!(c.len(), 1);
        c.disable("db", "cpu");
        assert!(!c.is_active());
        assert_eq!(
            c.len(),
            0,
            "entries dropped once the table can't be queried"
        );
    }

    #[test]
    fn tables_do_not_bleed_into_each_other() {
        let c = LastValueCache::with_default_cap();
        c.enable("db", "cpu");
        c.enable("db", "cpu2");
        c.observe("db", "cpu", &tags(&[("host", "a")]), 10, &f(1.0));
        c.observe("db", "cpu2", &tags(&[("host", "a")]), 10, &f(2.0));
        assert_eq!(c.snapshot("db", "cpu").len(), 1);
        assert_eq!(c.snapshot("db", "cpu2").len(), 1);
        assert_ne!(
            c.snapshot("db", "cpu")[0].fields,
            c.snapshot("db", "cpu2")[0].fields
        );
    }
}
