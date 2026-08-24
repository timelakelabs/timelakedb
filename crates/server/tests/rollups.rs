//! R-2 rollups, phase 1: the definition surface and its persistence
//! (ARCHITECTURE §18.1/§18.2). Materialisation is phase 2 of #59 and is not
//! exercised here — these pin that a rollup can be defined, validated,
//! persisted and removed, and that the retention invariant (§18.4) is
//! enforced at definition time.

use std::sync::Arc;

use timelake_server::{Engine, EngineConfig, RollupAgg, RollupDef, RollupFn};

fn engine(dir: &std::path::Path) -> Arc<Engine> {
    Engine::open(
        dir,
        EngineConfig {
            flush_rows: 1_000_000,
            flush_age_secs: u64::MAX,
            wal_max_bytes: u64::MAX,
            gc_grace_secs: 0,
            ..Default::default()
        },
    )
    .unwrap()
}

/// A valid rollup: avg+max of `value` from `sensor_reading` into
/// `sensor_reading_1m`, 60 s buckets, 5 min lookback, grouped by host.
fn valid() -> RollupDef {
    RollupDef {
        db: "poc".into(),
        name: "sensor_1m".into(),
        source: "sensor_reading".into(),
        target: "sensor_reading_1m".into(),
        interval_secs: 60,
        lookback_secs: 300,
        group_by: vec!["host".into()],
        aggregations: vec![
            RollupAgg {
                function: RollupFn::Avg,
                source_column: "value".into(),
                target_column: "value_avg".into(),
                quantile: None,
            },
            RollupAgg {
                function: RollupFn::Max,
                source_column: "value".into(),
                target_column: "value_max".into(),
                quantile: None,
            },
        ],
        filter: None,
    }
}

#[test]
fn a_rollup_is_defined_listed_and_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    assert!(eng.rollups().is_empty());

    eng.set_rollup(valid()).unwrap();
    let listed = eng.rollups();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "sensor_1m");
    assert_eq!(listed[0].target, "sensor_reading_1m");
    assert_eq!(listed[0].aggregations.len(), 2);

    // Reopen the same store: the definition persisted through it.
    drop(eng);
    let eng = engine(dir.path());
    assert_eq!(eng.rollups(), vec![valid()], "rollup did not persist");

    // Remove, and it stays removed across a restart.
    eng.remove_rollup("poc", "sensor_1m").unwrap();
    assert!(eng.rollups().is_empty());
    drop(eng);
    let eng = engine(dir.path());
    assert!(eng.rollups().is_empty(), "removal did not persist");
}

#[test]
fn setting_the_same_name_upserts_rather_than_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.set_rollup(valid()).unwrap();
    let mut changed = valid();
    changed.interval_secs = 300;
    changed.target = "sensor_reading_5m".into();
    eng.set_rollup(changed).unwrap();
    let listed = eng.rollups();
    assert_eq!(
        listed.len(),
        1,
        "same (db,name) must replace, not duplicate"
    );
    assert_eq!(listed[0].interval_secs, 300);
    assert_eq!(listed[0].target, "sensor_reading_5m");
}

#[test]
fn structurally_invalid_definitions_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());

    type Mutate = fn(&mut RollupDef);
    let cases: &[(&str, Mutate)] = &[
        ("empty db", |d| d.db.clear()),
        ("empty name", |d| d.name.clear()),
        ("empty source", |d| d.source.clear()),
        ("source == target", |d| d.target = d.source.clone()),
        ("zero interval", |d| d.interval_secs = 0),
        ("lookback < interval", |d| {
            d.interval_secs = 300;
            d.lookback_secs = 60;
        }),
        ("no aggregations", |d| d.aggregations.clear()),
        ("duplicate target column", |d| {
            d.aggregations[1].target_column = d.aggregations[0].target_column.clone()
        }),
        ("target column named time", |d| {
            d.aggregations[0].target_column = "time".into()
        }),
        ("percentile without a quantile", |d| {
            d.aggregations[0].function = RollupFn::Percentile
        }),
        ("quantile out of range", |d| {
            d.aggregations[0].function = RollupFn::Percentile;
            d.aggregations[0].quantile = Some(1.5);
        }),
        ("quantile on a non-percentile", |d| {
            d.aggregations[0].quantile = Some(0.5)
        }),
        ("blank filter", |d| d.filter = Some("   ".into())),
    ];
    for (label, mutate) in cases {
        let mut def = valid();
        mutate(&mut def);
        assert!(eng.set_rollup(def).is_err(), "expected rejection: {label}");
    }
    assert!(
        eng.rollups().is_empty(),
        "a rejected rollup must not persist"
    );
}

#[test]
fn the_retention_invariant_is_enforced_at_definition_time() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());

    // No retention on the source: any lookback is fine (kept forever).
    eng.set_rollup(valid()).unwrap();
    eng.remove_rollup("poc", "sensor_1m").unwrap();

    // Source kept 1h; a rollup reaching back 2h would under-count its oldest
    // buckets as retention drops the source out from under it (§18.4).
    eng.set_retention("poc", "sensor_reading", 3600).unwrap();
    let mut too_long = valid();
    too_long.lookback_secs = 7200;
    let err = eng.set_rollup(too_long).unwrap_err();
    assert!(
        err.contains("lookback") && err.contains("retention"),
        "rejection should name the invariant: {err}"
    );

    // A lookback inside the source's retention is accepted.
    let mut ok = valid();
    ok.lookback_secs = 1800;
    eng.set_rollup(ok).unwrap();

    // A wildcard (all-databases) source policy binds it too.
    eng.remove_rollup("poc", "sensor_1m").unwrap();
    eng.remove_retention("poc", "sensor_reading").unwrap();
    eng.set_retention(timelake_server::RETENTION_ANY_DB, "sensor_reading", 3600)
        .unwrap();
    let mut too_long = valid();
    too_long.lookback_secs = 7200;
    assert!(
        eng.set_rollup(too_long).is_err(),
        "a wildcard retention policy on the source binds the invariant"
    );
}
