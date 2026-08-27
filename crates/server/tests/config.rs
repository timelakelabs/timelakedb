//! U0b (timelakedb#109) engine integration: a config override applies live to
//! the hot fields, persists to settings.json, survives a restart over a stale
//! system property (with the divergence visible), and is validated as a whole.

use std::sync::Arc;

fn engine(dir: &std::path::Path) -> Arc<timelake_server::Engine> {
    timelake_server::Engine::open(dir, timelake_server::EngineConfig::default()).unwrap()
}

#[test]
fn an_override_applies_live_and_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    assert_eq!(e.config().gc_grace_secs, 900);
    assert_eq!(e.config_revision(), 0);

    let rev = e
        .set_config(
            "gc_grace_secs",
            Some("1500".into()),
            "rc",
            "2026-08-27T00:00:00Z",
        )
        .unwrap();
    assert_eq!(rev, 1);
    // Hot: the ArcSwap already carries it, no restart.
    assert_eq!(e.config().gc_grace_secs, 1500);
    assert_eq!(e.config_revision(), 1);
    drop(e);

    // Restart on the same dir: the override is loaded from settings.json.
    let e2 = engine(dir.path());
    assert_eq!(e2.config().gc_grace_secs, 1500);
    assert_eq!(e2.config_revision(), 1);
    let p = e2.config_provenance("gc_grace_secs").unwrap();
    assert_eq!(p["effective"]["value"], "1500");
    assert_eq!(p["effective"]["source"], "override");
}

#[test]
fn a_change_that_breaks_a_cross_field_invariant_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    // query_timeout default is 600, gc_grace 900. Lowering gc_grace to 300
    // (< 600) must be refused with the invariant named, and nothing changes.
    let err = e
        .set_config("gc_grace_secs", Some("300".into()), "rc", "t0")
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("gc_grace_secs") && msg.contains("query_timeout_secs"),
        "{msg}"
    );
    assert_eq!(e.config().gc_grace_secs, 900);
    assert_eq!(e.config_revision(), 0);
}

#[test]
fn a_stale_property_does_not_override_a_stored_setting_and_divergence_is_visible() {
    let dir = tempfile::tempdir().unwrap();
    // First boot with the default property; store an override.
    {
        let e = engine(dir.path());
        e.set_config("flush_rows", Some("120000".into()), "rc", "t0")
            .unwrap();
        assert_eq!(e.config().flush_rows, 120_000);
    }
    // Restart as if TIMELAKE_FLUSH_ROWS had been changed to a new value.
    let cfg = timelake_server::EngineConfig {
        flush_rows: 90_000,
        ..Default::default()
    };
    let e2 = timelake_server::Engine::open(dir.path(), cfg).unwrap();
    // The stored override still wins over the changed property.
    assert_eq!(e2.config().flush_rows, 120_000);
    // And the change is reported, not silently shadowed.
    let p = e2.config_provenance("flush_rows").unwrap();
    assert_eq!(p["diverged"]["property_at_write"], "50000");
    assert_eq!(p["diverged"]["property_now"], "90000");
}

#[test]
fn revert_removes_the_override_and_falls_back_to_the_property() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    e.set_config("compact_min_files", Some("16".into()), "rc", "t0")
        .unwrap();
    assert_eq!(e.config().compact_min_files, 16);
    assert!(e.revert_config("compact_min_files").unwrap());
    assert_eq!(e.config().compact_min_files, 4); // back to the default
}
