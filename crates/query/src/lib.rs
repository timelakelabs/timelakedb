//! Query — DataFusion integration (FR-3): session per query under a
//! bounded memory pool (RR-1 from the first line of query code), tables
//! registered from buffer snapshots, JSON row output for /api/sql.
//!
//! The SEC-2 hook is called unconditionally for every registered table —
//! at M1 it returns None (no restriction) and its call is the seam where
//! visibility labels, retention boundaries, and tenant scoping arrive
//! (M2 turns the return into a DataFusion Expr composed under the scan).

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

/// Re-export so downstream crates name batches without a direct
/// datafusion dependency (version unification lives here).
pub use datafusion::arrow::record_batch::RecordBatch as QueryBatch;
pub use datafusion::arrow::datatypes::SchemaRef as QuerySchema;
pub use datafusion::datasource::TableProvider as DfTableProvider;

pub mod provider;

use datafusion::execution::memory_pool::FairSpillPool;
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
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(total_mem_bytes)))
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
    session: &QuerySession,
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
    let config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema(db, "public");
    let ctx = SessionContext::new_with_config_rt(config, env.runtime.clone());
    for (name, provider) in tables {
        // SEC-2: the hook is on the path for every table, every query.
        let _restriction = mandatory_predicate(session, &name);
        ctx.register_table(&name, provider).map_err(|e| e.to_string())?;
    }

    let work = async {
        let df = ctx.sql(sql).await.map_err(|e| e.to_string())?;
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
pub fn align_to(
    schema: Arc<datafusion::arrow::datatypes::Schema>,
    batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>, String> {
    use datafusion::arrow::array::new_null_array;
    let mut aligned = Vec::with_capacity(batches.len());
    for b in batches {
        let cols = schema
            .fields()
            .iter()
            .map(|f| match b.column_by_name(f.name()) {
                Some(c) => Ok(c.clone()),
                None => Ok(new_null_array(f.data_type(), b.num_rows())),
            })
            .collect::<Result<Vec<_>, String>>()?;
        aligned.push(RecordBatch::try_new(schema.clone(), cols).map_err(|e| e.to_string())?);
    }
    Ok(aligned)
}

/// Per-session context the mandatory predicate sees (SEC-2).
/// Grows Accumulo-style authorizations, tenant, and retention context.
#[derive(Debug, Default, Clone)]
pub struct QuerySession {
    pub authorizations: Vec<String>,
}

/// THE injection point (SEC-2). v1 returns `None` (no restriction).
/// At M2 the return type becomes a DataFusion `Expr` composed with AND
/// below any user predicate — inside the scan, not the API layer.
pub fn mandatory_predicate(_session: &QuerySession, _table: &str) -> Option<String> {
    None
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
        // SEC-2: the hook is on the path for every table, every query.
        let _restriction = mandatory_predicate(session, &name);
        debug_assert!(_restriction.is_none(), "M2 has no predicate sources");
        let (schema, aligned) = align(batches)?;
        // one partition per batch -> DataFusion scans them in parallel
        let parts = aligned.into_iter().map(|b| vec![b]).collect();
        let table = MemTable::try_new(schema, parts).map_err(|e| e.to_string())?;
        ctx.register_table(&name, Arc::new(table))
            .map_err(|e| e.to_string())?;
    }

    let df = ctx.sql(sql).await.map_err(|e| e.to_string())?;
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
    use timelord_buffer::TableBuffer;
    use timelord_ingest::parse_lines;

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
