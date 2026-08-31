//! Authorized DDL (#80 / #153): CREATE TABLE declares a schema in the catalog
//! manifest log, the write path is held to that declaration, and the table is
//! queryable with zero rows the moment it commits. Driven through the real
//! engine — the admin HTTP layer + SQL-DDL-refused half is the drill's job
//! (deploy/compose/ddl_drill.sh), not a unit test's.

use std::sync::Arc;

use timelake_catalog::{ColumnType, TableColumn};

fn engine(dir: &std::path::Path) -> Arc<timelake_server::Engine> {
    timelake_server::Engine::open(
        dir,
        timelake_server::EngineConfig {
            // Never flush unasked: a row a test wrote stays in the buffer + WAL,
            // so a restart replays it and the "rejected write never hit the WAL"
            // assertion means what it says.
            flush_rows: 1_000_000,
            flush_age_secs: u64::MAX,
            wal_max_bytes: u64::MAX,
            ..Default::default()
        },
    )
    .unwrap()
}

/// host tag + usage float — the schema every case here starts from.
fn cpu_cols() -> Vec<TableColumn> {
    vec![
        TableColumn {
            name: "host".into(),
            ty: ColumnType::String,
            tag: true,
        },
        TableColumn {
            name: "usage".into(),
            ty: ColumnType::Float,
            tag: false,
        },
    ]
}

/// A write that must succeed. `write_lp_internal`'s error is not `Debug`, so a
/// bare `.unwrap()` won't compile — spell the panic out.
fn wr_ok(e: &timelake_server::Engine, db: &str, body: &[u8]) {
    e.write_lp_internal(db, body, Some("ns"))
        .unwrap_or_else(|_| panic!("write must land: {:?}", std::str::from_utf8(body)));
}

/// A write that must be refused. Returns whether it was, so the caller can also
/// assert the row count didn't move.
fn wr_refused(e: &timelake_server::Engine, db: &str, body: &[u8]) -> bool {
    e.write_lp_internal(db, body, Some("ns")).is_err()
}

async fn count(e: &timelake_server::Engine, db: &str, table: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) AS n FROM {table}");
    let batches = e
        .sql_batches(db, &sql, Vec::new(), None)
        .await
        .expect("count query must not fail");
    timelake_query::batches_to_json(&batches)[0]["n"]
        .as_i64()
        .expect("n is an integer")
}

/// True when a table no longer resolves — the query errors ("table not found",
/// or "database does not exist" if it was the last table). A dropped table must
/// answer this way, not with zero rows (which would mean it still exists, empty).
async fn gone(e: &timelake_server::Engine, db: &str, table: &str) -> bool {
    let sql = format!("SELECT COUNT(*) AS n FROM {table}");
    e.sql_batches(db, &sql, Vec::new(), None).await.is_err()
}

#[tokio::test]
async fn create_makes_an_empty_table_queryable_not_missing() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());

    e.create_table("poc", "cpu", cpu_cols()).unwrap();

    // The declaration is in the catalog immediately, no write required.
    assert!(e.declared_schema("poc", "cpu").is_some(), "schema declared");

    // A brand-new db that holds only a declared table still resolves — a query
    // against it answers with rows, not "database does not exist".
    assert_eq!(
        count(&e, "poc", "cpu").await,
        0,
        "declared, empty, queryable"
    );

    // And SELECT * resolves the columns rather than erroring "table not found".
    let batches = e
        .sql_batches("poc", "SELECT * FROM cpu", Vec::new(), None)
        .await
        .expect("select on a zero-row declared table must succeed");
    assert!(
        timelake_query::batches_to_json(&batches)
            .as_array()
            .unwrap()
            .is_empty(),
        "no rows yet"
    );
}

#[tokio::test]
async fn write_is_held_to_the_declaration() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    e.create_table("poc", "cpu", cpu_cols()).unwrap();

    // A conforming write lands.
    wr_ok(&e, "poc", b"cpu,host=a usage=0.5 10");
    assert_eq!(count(&e, "poc", "cpu").await, 1);

    // Every shape of "not the declared schema" is refused, and none of them
    // moves the count — the rejection is before durability.
    let bad: [(&str, &[u8]); 5] = [
        (
            "an undeclared field",
            &b"cpu,host=a usage=0.5,extra=1 20"[..],
        ),
        ("an undeclared tag", &b"cpu,host=a,rack=r1 usage=0.5 20"[..]),
        (
            "a type mismatch (string for a float)",
            &b"cpu,host=a usage=\"hot\" 20"[..],
        ),
        (
            "a declared tag written as a field",
            &b"cpu usage=0.5,host=\"x\" 20"[..],
        ),
        (
            "a declared field written as a tag",
            &b"cpu,usage=0.5,host=a other=1 20"[..],
        ),
    ];
    for (what, body) in bad {
        assert!(wr_refused(&e, "poc", body), "must refuse {what}");
    }
    assert_eq!(
        count(&e, "poc", "cpu").await,
        1,
        "not one refused write landed a row"
    );

    // A second conforming write still works — a rejection isn't a wedge.
    wr_ok(&e, "poc", b"cpu,host=b usage=0.9 30");
    assert_eq!(count(&e, "poc", "cpu").await, 2);
}

#[test]
fn create_refuses_bad_declarations() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());

    e.create_table("poc", "cpu", cpu_cols()).unwrap();
    assert!(
        e.create_table("poc", "cpu", cpu_cols()).is_err(),
        "re-declaring the same table is an error, not a no-op"
    );

    // A table schema-on-write already created cannot be CREATEd over.
    wr_ok(&e, "poc", b"mem,host=a used=1i 10");
    assert!(
        e.create_table("poc", "mem", cpu_cols()).is_err(),
        "a table a write created already exists"
    );

    // time is implicit; a duplicate column is a typo; an empty name is nonsense.
    assert!(
        e.create_table(
            "poc",
            "t1",
            vec![TableColumn {
                name: "time".into(),
                ty: ColumnType::Integer,
                tag: false
            }]
        )
        .is_err()
    );
    assert!(
        e.create_table(
            "poc",
            "t2",
            vec![
                TableColumn {
                    name: "x".into(),
                    ty: ColumnType::Float,
                    tag: false
                },
                TableColumn {
                    name: "x".into(),
                    ty: ColumnType::Integer,
                    tag: true
                },
            ]
        )
        .is_err()
    );
    assert!(e.create_table("poc", "", cpu_cols()).is_err());
}

#[tokio::test]
async fn declaration_and_refusal_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let e = engine(dir.path());
        e.create_table("poc", "cpu", cpu_cols()).unwrap();
        e.create_table("poc", "empty", cpu_cols()).unwrap();
        wr_ok(&e, "poc", b"cpu,host=a usage=0.5 10");
        // Refused: it must not reach the WAL, so the restart below must not see
        // it. This is the load-bearing assertion — a validation that ran AFTER
        // the WAL append would durably keep this row.
        assert!(wr_refused(&e, "poc", b"cpu,host=a usage=\"hot\" 20"));
        assert_eq!(count(&e, "poc", "cpu").await, 1);
    }

    // Fresh engine, same dir: catalog + WAL replay from disk.
    let e = engine(dir.path());
    assert!(
        e.declared_schema("poc", "cpu").is_some(),
        "declaration replayed"
    );
    assert!(
        e.declared_schema("poc", "empty").is_some(),
        "the unwritten one too"
    );
    assert_eq!(
        count(&e, "poc", "cpu").await,
        1,
        "the good row replayed; the refused one never entered the WAL"
    );
    assert_eq!(
        count(&e, "poc", "empty").await,
        0,
        "a declared-but-unwritten table is still queryable after a restart"
    );
}

#[tokio::test]
async fn drop_removes_the_table_including_unflushed_rows_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let e = engine(dir.path());
        e.create_table("poc", "cpu", cpu_cols()).unwrap();
        // Unflushed: the engine config never flushes on its own, so these rows
        // live in the buffer + WAL. If drop_table did NOT flush first, the WAL
        // would replay them after the restart below and resurrect the table.
        wr_ok(&e, "poc", b"cpu,host=a usage=0.5 10");
        wr_ok(&e, "poc", b"weather,city=x tempc=1.0 10"); // keeps the db alive
        assert_eq!(count(&e, "poc", "cpu").await, 1);

        e.drop_table("poc", "cpu").unwrap();
        assert!(gone(&e, "poc", "cpu").await, "the table no longer resolves");
        assert!(
            e.declared_schema("poc", "cpu").is_none(),
            "its declaration went too"
        );
        assert_eq!(
            count(&e, "poc", "weather").await,
            1,
            "other tables untouched"
        );
    }

    // Fresh engine, same dir: the WAL had unflushed cpu rows when the drop ran.
    let e = engine(dir.path());
    assert!(
        gone(&e, "poc", "cpu").await,
        "a dropped table must not come back from the WAL on restart"
    );
    assert_eq!(count(&e, "poc", "weather").await, 1);
}

#[tokio::test]
async fn a_write_after_drop_recreates_the_table() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    e.create_table("poc", "cpu", cpu_cols()).unwrap();
    wr_ok(&e, "poc", b"cpu,host=a usage=0.5 10");
    e.drop_table("poc", "cpu").unwrap();
    assert!(gone(&e, "poc", "cpu").await);

    // DROP is not permanent: schema-on-write brings the table back, now with no
    // declaration (the drop removed it), so any columns are accepted again.
    wr_ok(&e, "poc", b"cpu,host=b usage=0.9,extra=1 20");
    assert_eq!(count(&e, "poc", "cpu").await, 1, "the table is back");
    assert!(
        e.declared_schema("poc", "cpu").is_none(),
        "but undeclared now"
    );
}

#[tokio::test]
async fn drop_a_declared_but_unwritten_table() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    e.create_table("poc", "empty", cpu_cols()).unwrap();
    wr_ok(&e, "poc", b"keep,city=x tempc=1.0 10"); // keeps the db alive

    e.drop_table("poc", "empty").unwrap();
    assert!(e.declared_schema("poc", "empty").is_none());
    assert!(gone(&e, "poc", "empty").await, "no longer resolves");
}

#[test]
fn drop_a_table_that_does_not_exist_errors() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    wr_ok(&e, "poc", b"keep,city=x tempc=1.0 10");
    assert!(
        e.drop_table("poc", "ghost").is_err(),
        "dropping a table that was never created or written is an error"
    );
}
