//! The data-plane SQL surface is read-only, enforced at the plan.
//!
//! `POST /api/sql` and Flight SQL run arbitrary DataFusion SQL. DataFusion
//! can `COPY … TO '<path>'`, which writes a file as the server process —
//! and the container runs as root (SECURITY.md exposures 2 and 4). A single
//! unauthenticated request wrote a Parquet file outside the data directory;
//! this is the drill in `bench/results/sql-sandbox-drill.log`.
//!
//! Data-plane authentication does NOT close this. It narrows *who* can do it
//! to any holder of a read-capable token — which is every Grafana in the
//! deployment. A read token must never be a filesystem-write primitive.
//!
//! WHY AT THE PLAN, NOT THE TEXT. A regex over the SQL string is not a
//! boundary: comments, casing, whitespace, and string literals all defeat
//! it, and `COPY` can hide inside `EXPLAIN ANALYZE`. This classifies the
//! *logical plan* DataFusion actually built, walking every node including
//! the ones an `EXPLAIN`/`ANALYZE` wraps — so what is judged is exactly what
//! would execute.
//!
//! WHY DENY BY DEFAULT. `effect_of` matches every `LogicalPlan` variant with
//! no wildcard arm. A future DataFusion that adds a new plan node breaks this
//! build rather than silently letting it through — the safe direction for a
//! security check is to fail closed on the unknown.

use datafusion::logical_expr::LogicalPlan;

/// What a plan node *does* to the world, if anything. `None` is a pure read.
/// The `&'static str` is the operator name for the refusal message.
fn effect_of(plan: &LogicalPlan) -> Option<&'static str> {
    match plan {
        // The effectful nodes — the whole reason this module exists.
        LogicalPlan::Copy(_) => Some("COPY … TO"),
        LogicalPlan::Ddl(_) => Some("CREATE/DROP (DDL)"),
        LogicalPlan::Dml(_) => Some("INSERT/UPDATE/DELETE"),
        // SET, START/COMMIT TRANSACTION, PREPARE, DEALLOCATE — session and
        // transaction control. Harmless individually, but not reads, and
        // allowing them widens the surface for no BI-tool benefit.
        LogicalPlan::Statement(_) => Some("a session/transaction statement"),

        // Pure reads — everything a SELECT, SHOW, DESCRIBE or EXPLAIN builds.
        // Listed exhaustively (no `_`) so a new DataFusion variant is a
        // compile error here, not a silent pass.
        LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Join(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::TableScan(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::Explain(_)
        | LogicalPlan::Analyze(_)
        | LogicalPlan::Extension(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::DescribeTable(_)
        | LogicalPlan::Unnest(_)
        | LogicalPlan::RecursiveQuery(_) => None,
    }
}

/// Reject anything that is not a read, checking the whole plan tree.
///
/// The walk is manual rather than `TreeNode::apply`, because the nodes that
/// matter most — `Explain` and `Analyze` — do not expose their wrapped plan
/// through `inputs()`, so a visitor would classify the wrapper and miss the
/// `COPY` inside `EXPLAIN ANALYZE COPY … TO`. This reaches into them
/// explicitly.
pub fn ensure_read_only(plan: &LogicalPlan) -> Result<(), String> {
    if let Some(op) = effect_of(plan) {
        return Err(format!(
            "{op} is not permitted here: this endpoint executes read-only SQL \
             (SELECT, SHOW, DESCRIBE, EXPLAIN). See SECURITY.md."
        ));
    }
    for child in plan.inputs() {
        ensure_read_only(child)?;
    }
    // inputs() treats these as leaves, so their wrapped plan is reached here
    // or an EXPLAIN ANALYZE could smuggle an effectful statement past the walk.
    match plan {
        LogicalPlan::Explain(e) => ensure_read_only(&e.plan)?,
        LogicalPlan::Analyze(a) => ensure_read_only(&a.input)?,
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{SessionConfig, SessionContext};

    async fn plan(sql: &str) -> LogicalPlan {
        // information_schema on, matching the production context — DESCRIBE
        // and information_schema queries need it.
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
        ctx.sql("CREATE TABLE t (x INT) AS VALUES (1)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        // create_logical_plan is exactly what ctx.sql() runs before execution
        ctx.state().create_logical_plan(sql).await.unwrap()
    }

    async fn allowed(sql: &str) -> bool {
        ensure_read_only(&plan(sql).await).is_ok()
    }

    #[tokio::test]
    async fn reads_pass() {
        assert!(allowed("SELECT * FROM t").await);
        assert!(allowed("SELECT count(*) FROM t WHERE x > 0 GROUP BY x").await);
        assert!(allowed("SELECT * FROM t ORDER BY x LIMIT 5").await);
        assert!(allowed("WITH c AS (SELECT x FROM t) SELECT * FROM c").await);
        assert!(allowed("SELECT * FROM t UNION ALL SELECT * FROM t").await);
        assert!(allowed("EXPLAIN SELECT * FROM t").await);
        assert!(allowed("DESCRIBE t").await);
        assert!(allowed("SELECT * FROM information_schema.tables").await);
    }

    #[tokio::test]
    async fn the_copy_exposure_is_refused() {
        // The exact shape that wrote a root-owned file before this existed.
        let p = plan("COPY (SELECT 1 AS x) TO '/tmp/pwned.parquet' STORED AS PARQUET").await;
        let err = ensure_read_only(&p).unwrap_err();
        assert!(err.contains("COPY"), "{err}");
    }

    #[tokio::test]
    async fn copy_hidden_inside_explain_analyze_is_still_refused() {
        // EXPLAIN ANALYZE executes its inner plan, so a COPY nested in it
        // runs. The walk must reach through the wrapper.
        let p =
            plan("EXPLAIN ANALYZE COPY (SELECT 1 AS x) TO '/tmp/pwned2.parquet' STORED AS PARQUET")
                .await;
        assert!(ensure_read_only(&p).is_err(), "nested COPY leaked through");
    }

    #[tokio::test]
    async fn ddl_and_dml_are_refused() {
        assert!(ensure_read_only(&plan("CREATE TABLE u (y INT) AS VALUES (1)").await).is_err());
        assert!(ensure_read_only(&plan("DROP TABLE t").await).is_err());
        assert!(ensure_read_only(&plan("INSERT INTO t VALUES (2)").await).is_err());
    }
}
