//! Last-value cache write-path integration (#57 phase 1): the cache reflects
//! writes for opted-in tables only, keeps the latest per series, and does not
//! move backwards on an out-of-order write — exercised through the real write
//! path, not the cache in isolation.

use timelake_ingest::FieldValue;

fn engine(dir: &std::path::Path) -> std::sync::Arc<timelake_server::Engine> {
    timelake_server::Engine::open(dir, timelake_server::EngineConfig::default()).unwrap()
}

fn wr(e: &timelake_server::Engine, db: &str, body: &[u8]) {
    e.write_lp_internal(db, body, Some("ns"))
        .unwrap_or_else(|_| panic!("write failed: {:?}", std::str::from_utf8(body)));
}

fn field(v: &[timelake_lastvalue::LastValue], tag: (&str, &str), name: &str) -> FieldValue {
    let want = vec![(tag.0.to_string(), tag.1.to_string())];
    v.iter()
        .find(|e| e.tags == want)
        .unwrap_or_else(|| panic!("no series {tag:?}"))
        .fields
        .iter()
        .find(|(k, _)| k == name)
        .unwrap_or_else(|| panic!("no field {name}"))
        .1
        .clone()
}

#[test]
fn write_path_updates_the_cache_only_for_enabled_tables() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());

    // Not enabled yet: a write caches nothing.
    wr(&e, "poc", b"cpu,host=a usage=0.1 1");
    assert!(
        e.last_value_snapshot("poc", "cpu").is_empty(),
        "no cache until enabled"
    );

    e.enable_last_cache("poc", "cpu").unwrap();
    wr(
        &e,
        "poc",
        b"cpu,host=a usage=0.5 10\ncpu,host=b usage=0.9 10",
    );
    wr(&e, "poc", b"cpu,host=a usage=0.7 20"); // newer for a
    // A different, NON-enabled table is not cached.
    wr(&e, "poc", b"mem,host=a used=1 30");

    let snap = e.last_value_snapshot("poc", "cpu");
    assert_eq!(snap.len(), 2, "one entry per series for the enabled table");
    assert_eq!(
        field(&snap, ("host", "a"), "usage"),
        FieldValue::Float(0.7),
        "latest for a"
    );
    assert_eq!(
        field(&snap, ("host", "b"), "usage"),
        FieldValue::Float(0.9),
        "latest for b"
    );
    assert!(
        e.last_value_snapshot("poc", "mem").is_empty(),
        "mem was never enabled"
    );
}

#[test]
fn an_out_of_order_write_does_not_move_current_value_backwards() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    e.enable_last_cache("poc", "cpu").unwrap();
    wr(&e, "poc", b"cpu,host=a usage=0.7 20");
    // A delayed sample with an OLDER timestamp lands.
    wr(&e, "poc", b"cpu,host=a usage=99 10");
    let snap = e.last_value_snapshot("poc", "cpu");
    assert_eq!(snap[0].timestamp_ns, 20, "latest timestamp unchanged");
    assert_eq!(
        field(&snap, ("host", "a"), "usage"),
        FieldValue::Float(0.7),
        "value did not flicker back"
    );
}

#[test]
fn the_opt_in_and_its_entries_are_dropped_on_disable() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    e.enable_last_cache("poc", "cpu").unwrap();
    wr(&e, "poc", b"cpu,host=a usage=0.5 10");
    assert_eq!(e.last_value_snapshot("poc", "cpu").len(), 1);
    assert_eq!(
        e.last_cache_tables(),
        vec![("poc".to_string(), "cpu".to_string())]
    );

    e.disable_last_cache("poc", "cpu").unwrap();
    assert!(e.last_cache_tables().is_empty());
    assert!(
        e.last_value_snapshot("poc", "cpu").is_empty(),
        "entries dropped with the opt-in"
    );
}
