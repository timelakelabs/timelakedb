//! Query — DataFusion integration (FR-3): session per query under a
//! bounded memory pool (RR-1 from the first line of query code), tables
//! registered from buffer snapshots, JSON row output for /api/sql.
//!
//! The SEC-2 hook [`mandatory_predicate`] is called unconditionally for
//! every table scan, inside the provider — below any user predicate and
//! before any aggregation, so an aggregate can never count a row the
//! caller cannot see. v1 restriction: Accumulo-style visibility labels
//! (see [`visibility`]); retention boundaries and tenant scoping arrive
//! as new [`Restriction`] variants through this same hook.

use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, Float64Array, Int64Array, UInt64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use serde_json::{Map, Number, Value, json};

pub use datafusion::arrow::datatypes::SchemaRef as QuerySchema;
/// Re-export so downstream crates name batches without a direct
/// datafusion dependency (version unification lives here).
pub use datafusion::arrow::record_batch::RecordBatch as QueryBatch;
pub use datafusion::datasource::TableProvider as DfTableProvider;

pub mod provider;
pub mod sql_guard;
pub mod visibility;

use datafusion::execution::runtime_env::RuntimeEnv;

/// Shared query environment (RR-1/RR-2 hardening, M4):
/// ONE memory pool for every concurrent query (previously each query got
/// its own full-size pool — concurrency multiplied memory, the exact
/// disease this project exists to cure), a semaphore for admission
/// control, and a server-side timeout so abandoned queries stop burning
/// the pool.
pub struct QueryEnv {
    pub runtime: Arc<RuntimeEnv>,
    admission: tokio::sync::Semaphore,
    pub timeout: std::time::Duration,
}

impl QueryEnv {
    pub fn new(total_mem_bytes: usize, max_concurrent: usize, timeout_secs: u64) -> Self {
        // The pool is GREEDY, not fair, and the difference is measured.
        // `FairSpillPool` hands each *spillable consumer* an even slice —
        // `(pool_size - unspillable) / num_spill` — and a DataFusion plan
        // registers one such consumer per partition per aggregate, per
        // partition per repartition, and one per sort. A funnel query on a
        // 24-core host registers ~145 of them, so a 1 GiB pool becomes a
        // ~7 MB budget per operator: the final aggregate spilled 39.5 MB to
        // disk with 800 MB of the pool still free. That divisor grows with
        // core count, so the engine spilled *more* the bigger the machine.
        //
        // RR-1 is unaffected — the cap that matters is the total, and a
        // greedy pool enforces exactly the same one, returning a clean
        // error (or a spill) when the whole pool is gone rather than when
        // 1/145th of it is. Concurrency is bounded where it was designed to
        // be bounded: the admission semaphore below.
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(total_mem_bytes)))
            .build_arc()
            .expect("runtime env");
        QueryEnv {
            runtime,
            admission: tokio::sync::Semaphore::new(max_concurrent.max(1)),
            timeout: std::time::Duration::from_secs(timeout_secs.max(1)),
        }
    }
}

/// Execute SQL over registered providers under the SHARED environment.
pub async fn run_sql_env(
    env: &QueryEnv,
    _session: &QuerySession,
    db: &str,
    tables: Vec<(String, Arc<dyn datafusion::datasource::TableProvider>)>,
    sql: &str,
) -> Result<Vec<RecordBatch>, String> {
    // admission control: excess queries queue here, bounded by RR-1
    let _permit = env
        .admission
        .acquire()
        .await
        .map_err(|_| "query admission closed".to_string())?;

    // information_schema powers SHOW TABLES / DESCRIBE and is what SQL and BI
    // tools use to discover a schema. Naming the default catalog after the
    // database makes the three-part names those tools generate resolve —
    // Flight SQL reports each database as a catalog, so `poc.public.events`
    // has to mean something here.
    let mut config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema(db, "public");
    // A partial hash aggregate only pays for itself when it DEDUPLICATES.
    // The funnel queries group by (step, product_id) — one group per row at
    // 200K products — so the partial pass builds a 1.8 M-entry hash table
    // that reduces nothing and then hands every row on anyway. DataFusion
    // detects that and switches to pass-through, but only after
    // `probe_rows_threshold` rows have gone through ONE partition; at the
    // default 100_000 a scan whose partitions hold ~19K-76K rows never
    // reaches the check. Probing a batch in makes the heuristic reachable at
    // the sizes this engine actually produces. The ratio threshold (0.8) is
    // untouched, so a genuinely reducing group-by — B3/B4's ten steps — still
    // aggregates early exactly as before.
    config
        .options_mut()
        .execution
        .skip_partial_aggregation_probe_rows_threshold = 8192;
    let ctx = SessionContext::new_with_config_rt(config, env.runtime.clone());
    // SEC-2 note: enforcement is NOT here. The providers carry the session
    // and call mandatory_predicate inside scan() — the filter is part of
    // the scan itself, so no plan shape can aggregate around it.
    for (name, provider) in tables {
        ctx.register_table(&name, provider)
            .map_err(|e| e.to_string())?;
    }

    let work = async {
        // Read-only enforcement: classify the built plan before executing
        // it, so COPY/DDL/DML never run on the data-plane surface. The plan
        // is reused for execution, so this is one parse, not two.
        let plan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(|e| e.to_string())?;
        sql_guard::ensure_read_only(&plan)?;
        let df = ctx
            .execute_logical_plan(plan)
            .await
            .map_err(|e| e.to_string())?;
        df.collect().await.map_err(|e| e.to_string())
    };
    match tokio::time::timeout(env.timeout, work).await {
        Ok(res) => res,
        Err(_) => Err(format!(
            "query timed out after {}s (server-side cap, RR-2)",
            env.timeout.as_secs()
        )),
    }
}

/// Union of two optional schemas (registry merge helper).
pub fn schema_union(
    a: Option<Arc<datafusion::arrow::datatypes::Schema>>,
    b: Arc<datafusion::arrow::datatypes::Schema>,
) -> Result<Arc<datafusion::arrow::datatypes::Schema>, String> {
    use datafusion::arrow::datatypes::{Field, Schema};
    let Some(a) = a else { return Ok(b) };
    let mut names: Vec<String> = a.fields().iter().map(|f| f.name().clone()).collect();
    let mut types: std::collections::HashMap<String, DataType> = a
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect();
    for f in b.fields() {
        match types.get(f.name()) {
            None => {
                names.push(f.name().clone());
                types.insert(f.name().clone(), f.data_type().clone());
            }
            Some(t) if t == f.data_type() => {}
            Some(t) => {
                return Err(format!(
                    "column '{}' has conflicting types {t:?} vs {:?}",
                    f.name(),
                    f.data_type()
                ));
            }
        }
    }
    Ok(Arc::new(Schema::new(
        names
            .iter()
            .map(|n| Field::new(n, types[n].clone(), true))
            .collect::<Vec<_>>(),
    )))
}

/// Align batches to a FIXED target schema (missing columns become null).
///
/// A column whose type differs from the target is cast. That is the WRITE
/// BUFFER's path: file batches are converted to the presented types on the
/// scan's worker threads, but a buffer snapshot is still dictionary-encoded
/// when it gets here. Keep it that way round — a conversion done here is
/// serial, after the parallel load, and a previous cycle measured exactly
/// that as ~50 ms of a 53 ms query.
pub fn align_to(
    schema: Arc<datafusion::arrow::datatypes::Schema>,
    batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>, String> {
    use datafusion::arrow::array::new_null_array;
    use datafusion::arrow::compute::cast;
    let mut aligned = Vec::with_capacity(batches.len());
    for b in batches {
        let cols = schema
            .fields()
            .iter()
            .map(|f| match b.column_by_name(f.name()) {
                Some(c) if c.data_type() == f.data_type() => Ok(c.clone()),
                Some(c) => cast(c, f.data_type()).map_err(|e| e.to_string()),
                None => Ok(new_null_array(f.data_type(), b.num_rows())),
            })
            .collect::<Result<Vec<_>, String>>()?;
        aligned.push(RecordBatch::try_new(schema.clone(), cols).map_err(|e| e.to_string())?);
    }
    Ok(aligned)
}

/// Per-session context the mandatory predicate sees (SEC-2).
/// Grows tenant and retention context alongside the authorizations.
#[derive(Debug, Default, Clone)]
pub struct QuerySession {
    pub authorizations: Vec<String>,
    /// The verified client-certificate identity, when the caller
    /// presented one (SEC-3 want mode). `None` is an anonymous caller —
    /// served, because Grafana and Telegraf have no certificate.
    pub identity: Option<String>,
}

impl QuerySession {
    pub fn with_authorizations(authorizations: Vec<String>) -> QuerySession {
        QuerySession {
            authorizations,
            identity: None,
        }
    }

    /// Resolve what this session may actually see.
    ///
    /// SECURITY.md records that `X-TimeLake-Authorizations` is a
    /// self-asserted claim: whatever the caller says. A verified client
    /// certificate is the credential that turns a claim into a grant, and
    /// want mode is what lets that arrive without a flag day:
    ///
    /// | caller | result |
    /// |---|---|
    /// | no certificate | claims trusted as asserted — **unchanged**, so Grafana and Telegraf keep working |
    /// | verified certificate | claims **intersected** with what that identity is granted |
    ///
    /// So authenticating can only ever *narrow* what a caller sees, never
    /// widen it, and an anonymous caller is exactly as (un)restricted as
    /// it was before this existed. Restricting the anonymous path is a
    /// separate, deliberate decision — see SECURITY.md exposure 7.
    pub fn resolve(mut self, granted: Option<&[String]>) -> QuerySession {
        if let (Some(_), Some(granted)) = (&self.identity, granted) {
            self.authorizations
                .retain(|claimed| granted.iter().any(|g| g == claimed));
        }
        self
    }
}

/// The label column: a dictionary-encoded tag holding an Accumulo-style
/// expression per row. Low-cardinality in practice, so FR-2's column
/// economics apply — a label costs what a tag costs.
pub const VISIBILITY_COLUMN: &str = "_visibility";

/// What [`mandatory_predicate`] can require of a scan. One variant today;
/// tenant scoping and retention boundaries arrive as more variants, each
/// enforced at the same place in the provider.
#[derive(Debug, Clone)]
pub enum Restriction {
    /// Drop rows whose visibility expression the session's authorizations
    /// do not satisfy.
    Visibility {
        column: String,
        authorizations: Vec<String>,
    },
}

/// THE injection point (SEC-2). Called unconditionally for every table
/// scan by the provider; the returned restriction composes below any
/// user predicate and before aggregation. Tables without a label column
/// carry no restriction — the bench workload is untouched.
pub fn mandatory_predicate(
    session: &QuerySession,
    _table: &str,
    schema: &datafusion::arrow::datatypes::Schema,
) -> Option<Restriction> {
    if schema.field_with_name(VISIBILITY_COLUMN).is_err() {
        return None;
    }
    Some(Restriction::Visibility {
        column: VISIBILITY_COLUMN.to_string(),
        authorizations: session.authorizations.clone(),
    })
}

/// Enforce one restriction on one batch. Rows fail closed: a label the
/// session's authorizations do not satisfy — or one that does not parse —
/// drops its row here, before DataFusion ever sees it.
pub fn apply_restriction(r: &Restriction, batch: &RecordBatch) -> Result<RecordBatch, String> {
    use datafusion::arrow::array::{BooleanArray, DictionaryArray, StringArray, StringViewArray};
    use datafusion::arrow::compute::filter_record_batch;
    use datafusion::arrow::datatypes::Int32Type;

    let Restriction::Visibility {
        column,
        authorizations,
    } = r;
    // A batch without the column (a file written before labels existed)
    // holds unlabeled rows, and unlabeled rows are visible to everyone.
    let Some(col) = batch.column_by_name(column) else {
        return Ok(batch.clone());
    };
    let auths: std::collections::HashSet<&str> =
        authorizations.iter().map(|s| s.as_str()).collect();

    let mask: BooleanArray = match col.data_type() {
        // the expected shape: tags are dictionary columns (FR-2), so each
        // distinct label is evaluated once per batch, not once per row
        DataType::Dictionary(k, v) if **k == DataType::Int32 && **v == DataType::Utf8 => {
            let dict = col
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("checked dictionary type");
            let values = dict
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("checked utf8 values");
            let visible: Vec<bool> = (0..values.len())
                .map(|i| values.is_null(i) || visibility::is_visible(values.value(i), &auths))
                .collect();
            let keys = dict.keys();
            (0..dict.len())
                .map(|i| {
                    if dict.is_null(i) {
                        true // NULL label = unlabeled = public
                    } else {
                        visible[keys.value(i) as usize]
                    }
                })
                .collect()
        }
        DataType::Utf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("checked utf8");
            let mut memo: std::collections::HashMap<&str, bool> = std::collections::HashMap::new();
            (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        true
                    } else {
                        let s = arr.value(i);
                        *memo
                            .entry(s)
                            .or_insert_with(|| visibility::is_visible(s, &auths))
                    }
                })
                .collect()
        }
        // The provider presents tag columns as views, so this is the shape
        // a label arrives in from a scan. SEC-2 fails CLOSED: without this
        // arm a labelled table would error rather than leak, but it would
        // still be broken, so it is pinned by a test.
        DataType::Utf8View => {
            let arr = col
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("checked utf8view");
            let mut memo: std::collections::HashMap<&str, bool> = std::collections::HashMap::new();
            (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        true
                    } else {
                        let s = arr.value(i);
                        *memo
                            .entry(s)
                            .or_insert_with(|| visibility::is_visible(s, &auths))
                    }
                })
                .collect()
        }
        other => {
            // a _visibility FIELD (e.g. a float) cannot be evaluated;
            // failing the query loudly beats silently hiding every row
            return Err(format!(
                "{column} must be a string column to carry visibility labels, got {other:?}"
            ));
        }
    };
    filter_record_batch(batch, &mask).map_err(|e| e.to_string())
}

/// Merge batches with heterogeneous-but-compatible schemas (files written
/// before a column existed union'd with newer ones): the merged schema is
/// the first-seen-ordered union; missing columns become nulls. A name
/// carried with two different types is an error (first-writer-wins is
/// enforced at write time; a conflict here means corrupted state).
pub fn align(
    batches: Vec<RecordBatch>,
) -> Result<(Arc<datafusion::arrow::datatypes::Schema>, Vec<RecordBatch>), String> {
    use datafusion::arrow::array::new_null_array;
    use datafusion::arrow::datatypes::{Field, Schema};

    let mut names: Vec<String> = Vec::new();
    let mut types: std::collections::HashMap<String, DataType> = std::collections::HashMap::new();
    for b in &batches {
        for f in b.schema().fields() {
            match types.get(f.name()) {
                None => {
                    names.push(f.name().clone());
                    types.insert(f.name().clone(), f.data_type().clone());
                }
                Some(t) if t == f.data_type() => {}
                Some(t) => {
                    return Err(format!(
                        "column '{}' has conflicting types {t:?} vs {:?}",
                        f.name(),
                        f.data_type()
                    ));
                }
            }
        }
    }
    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|n| Field::new(n, types[n].clone(), true))
            .collect::<Vec<_>>(),
    ));

    let mut aligned = Vec::with_capacity(batches.len());
    for b in batches {
        let cols = names
            .iter()
            .map(|n| match b.column_by_name(n) {
                Some(c) => c.clone(),
                None => new_null_array(&types[n], b.num_rows()),
            })
            .collect::<Vec<_>>();
        aligned.push(RecordBatch::try_new(schema.clone(), cols).map_err(|e| e.to_string())?);
    }
    Ok((schema, aligned))
}

/// Execute `sql` over the given (table_name, batches) pairs — each table
/// is the union of its buffer snapshot and its Parquet batches — bounded
/// by `mem_limit_bytes` (RR-1: a query that exceeds the pool gets a clean
/// error, never a dead process).
pub async fn run_sql(
    session: &QuerySession,
    tables: Vec<(String, Vec<RecordBatch>)>,
    sql: &str,
    mem_limit_bytes: usize,
) -> Result<Vec<RecordBatch>, String> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(mem_limit_bytes)))
        .build_arc()
        .map_err(|e| e.to_string())?;
    let ctx = SessionContext::new_with_config_rt(
        SessionConfig::new().with_information_schema(true),
        runtime,
    );

    for (name, batches) in tables {
        let (schema, aligned) = align(batches)?;
        // SEC-2: the hook is on the path for every table, every query —
        // this in-memory path enforces at registration, before DataFusion.
        let aligned = match mandatory_predicate(session, &name, &schema) {
            None => aligned,
            Some(r) => aligned
                .iter()
                .map(|b| apply_restriction(&r, b))
                .collect::<Result<Vec<_>, _>>()?,
        };
        // one partition per batch -> DataFusion scans them in parallel
        let parts = aligned.into_iter().map(|b| vec![b]).collect();
        let table = MemTable::try_new(schema, parts).map_err(|e| e.to_string())?;
        ctx.register_table(&name, Arc::new(table))
            .map_err(|e| e.to_string())?;
    }

    let plan = ctx
        .state()
        .create_logical_plan(sql)
        .await
        .map_err(|e| e.to_string())?;
    sql_guard::ensure_read_only(&plan)?;
    let df = ctx
        .execute_logical_plan(plan)
        .await
        .map_err(|e| e.to_string())?;
    df.collect().await.map_err(|e| e.to_string())
}

/// Render result batches as a JSON array of row objects — the /api/sql
/// wire contract the bench adapter reads (numbers as numbers; timestamps,
/// strings, and dictionary values as strings; NULL as null).
pub fn batches_to_json(batches: &[RecordBatch]) -> Value {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            let mut obj = Map::with_capacity(batch.num_columns());
            for (ci, field) in schema.fields().iter().enumerate() {
                let col = batch.column(ci);
                obj.insert(field.name().clone(), cell_to_json(col.as_ref(), row));
            }
            rows.push(Value::Object(obj));
        }
    }
    json!(rows)
}

fn cell_to_json(col: &dyn Array, row: usize) -> Value {
    if col.is_null(row) {
        return Value::Null;
    }
    match col.data_type() {
        DataType::Int64 => {
            let a = col.as_any().downcast_ref::<Int64Array>().unwrap();
            Value::Number(a.value(row).into())
        }
        DataType::UInt64 => {
            let a = col.as_any().downcast_ref::<UInt64Array>().unwrap();
            Value::Number(a.value(row).into())
        }
        DataType::Float64 => {
            let a = col.as_any().downcast_ref::<Float64Array>().unwrap();
            Number::from_f64(a.value(row)).map_or(Value::Null, Value::Number)
        }
        DataType::Boolean => {
            let a = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Bool(a.value(row))
        }
        // timestamps, Utf8, dictionaries, intervals, everything else:
        // arrow's display formatting (RFC3339 for timestamps)
        _ => array_value_to_string(col, row)
            .map(Value::String)
            .unwrap_or(Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use timelake_buffer::TableBuffer;
    use timelake_ingest::parse_lines;

    fn buffer_with(lp: &str) -> RecordBatch {
        let mut buf = TableBuffer::default();
        for line in parse_lines(lp, 1, 0).unwrap() {
            buf.append(&line).unwrap();
        }
        buf.snapshot().unwrap()
    }

    #[tokio::test]
    async fn count_distinct_group_by_over_dictionary_tags() {
        // now()-relative timestamps so the canonical WHERE clauses match
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let lp = format!(
            "pipeline_events,product_id=p1,step=01-download,event=stop duration_s=1.5 {t1}\n\
             pipeline_events,product_id=p2,step=01-download,event=stop duration_s=2.5 {t2}\n\
             pipeline_events,product_id=p1,step=02-extract,event=stop duration_s=3.5 {t3}",
            t1 = now - 1_000,
            t2 = now - 2_000,
            t3 = now - 3_000,
        );
        let batch = buffer_with(&lp);
        let session = QuerySession::default();
        let batches = run_sql(
            &session,
            vec![("pipeline_events".into(), vec![batch])],
            "SELECT step, COUNT(DISTINCT product_id) AS products \
             FROM pipeline_events \
             WHERE event = 'stop' AND time >= now() - INTERVAL '24 hours' \
             GROUP BY step ORDER BY step",
            64 * 1024 * 1024,
        )
        .await
        .unwrap();

        let rows = batches_to_json(&batches);
        let rows = rows.as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["step"], "01-download");
        assert_eq!(rows[0]["products"], 2);
        assert_eq!(rows[1]["step"], "02-extract");
        assert_eq!(rows[1]["products"], 1);
    }

    #[tokio::test]
    async fn visibility_labels_gate_rows_and_aggregates() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        // one row visible to admin, one to ops&audit holders, one public
        let lp = format!(
            "pipeline_events,product_id=p1,_visibility=admin duration_s=1.0 {t1}\n\
             pipeline_events,product_id=p2,_visibility=ops&audit duration_s=2.0 {t2}\n\
             pipeline_events,product_id=p3 duration_s=3.0 {t3}",
            t1 = now - 1_000,
            t2 = now - 2_000,
            t3 = now - 3_000,
        );
        let batch = buffer_with(&lp);

        for (auths, expect) in [
            (vec![], 1i64), // no auths: public rows only
            (vec!["admin"], 2),
            (vec!["ops"], 1), // ops alone does not satisfy ops&audit
            (vec!["ops", "audit"], 2),
            (vec!["admin", "ops", "audit"], 3),
        ] {
            let session =
                QuerySession::with_authorizations(auths.iter().map(|s| s.to_string()).collect());
            // the aggregate is the leak test: a hidden row must not COUNT
            let rows = run_sql(
                &session,
                vec![("pipeline_events".into(), vec![batch.clone()])],
                "SELECT COUNT(*) AS n FROM pipeline_events",
                64 * 1024 * 1024,
            )
            .await
            .unwrap();
            let rows = batches_to_json(&rows);
            assert_eq!(rows[0]["n"], expect, "auths {auths:?}");
        }
    }

    #[test]
    fn an_anonymous_caller_is_unchanged_by_grants() {
        // The compatibility guarantee: Grafana, Telegraf and the bench
        // harness present no certificate and must behave exactly as they
        // did before client auth existed.
        let s = QuerySession::with_authorizations(vec!["admin".into(), "ops".into()]);
        assert_eq!(s.identity, None);
        let resolved = s.resolve(Some(&["ops".to_string()]));
        assert_eq!(
            resolved.authorizations,
            vec!["admin".to_string(), "ops".to_string()],
            "an anonymous caller's claims are still taken as asserted"
        );
    }

    #[test]
    fn a_verified_identity_narrows_its_claims_to_its_grants() {
        let mut s =
            QuerySession::with_authorizations(vec!["admin".into(), "ops".into(), "audit".into()]);
        s.identity = Some("tributary-node-1".into());
        let resolved = s.resolve(Some(&["ops".to_string(), "audit".to_string()]));
        assert_eq!(
            resolved.authorizations,
            vec!["ops".to_string(), "audit".to_string()],
            "claiming admin does not grant admin"
        );
    }

    #[test]
    fn authenticating_can_only_narrow_never_widen() {
        // A principal granted more than it claimed still only gets what
        // it asked for — the intersection is not a union.
        let mut s = QuerySession::with_authorizations(vec!["ops".into()]);
        s.identity = Some("agent".into());
        let resolved = s.resolve(Some(&["ops".to_string(), "admin".to_string()]));
        assert_eq!(resolved.authorizations, vec!["ops".to_string()]);
    }

    #[test]
    fn an_identity_with_no_grants_recorded_keeps_its_claims() {
        // Grants are not yet administered anywhere, so `None` must mean
        // "no grant policy exists" rather than "deny everything" — the
        // latter would break an identified client the moment it presented
        // a certificate, which is the opposite of an additive migration.
        let mut s = QuerySession::with_authorizations(vec!["ops".into()]);
        s.identity = Some("agent".into());
        assert_eq!(s.resolve(None).authorizations, vec!["ops".to_string()]);
    }

    #[tokio::test]
    async fn bad_sql_is_an_error_not_a_panic() {
        let batch = buffer_with("m f=1i 1");
        let err = run_sql(
            &QuerySession::default(),
            vec![("m".into(), vec![batch])],
            "SELECT nope FROM missing",
            1024 * 1024,
        )
        .await
        .unwrap_err();
        assert!(!err.is_empty());
    }
}
