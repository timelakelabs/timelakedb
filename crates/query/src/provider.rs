//! Pruning TableProvider (PR-3/PR-8): the read path stops loading the
//! world. Filters DataFusion pushes down are used to skip whole files
//! (catalog min/max time bounds) and row groups (Parquet bloom filters
//! on tag columns); everything reported `Inexact` so DataFusion still
//! applies the predicates after the scan — pruning can only skip data
//! that provably cannot match, never change results.
//!
//! File loads register with the shared memory pool (RR-1): a query whose
//! candidate set is too large for the pool budget fails cleanly at load
//! time instead of OOMing the process.
//!
//! This provider is also SEC-2's future push-down home: the mandatory
//! predicate composes here, below any user filter.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use timelord_catalog::FileMeta;
use timelord_store::Store;

use datafusion::arrow::record_batch::RecordBatch;

/// Time bounds and tag-equality literals extracted from pushed filters.
#[derive(Debug, Default, Clone)]
pub struct Pruning {
    pub min_ts_ns: Option<i64>,
    pub max_ts_ns: Option<i64>,
    /// (column, value) equality literals, e.g. product_id = 'p1'
    pub tag_equals: Vec<(String, String)>,
}

pub fn extract_pruning(filters: &[Expr]) -> Pruning {
    let mut p = Pruning::default();
    for f in filters {
        walk(f, &mut p);
    }
    p
}

fn walk(e: &Expr, p: &mut Pruning) {
    if let Expr::BinaryExpr(b) = e {
        match b.op {
            Operator::And => {
                walk(&b.left, p);
                walk(&b.right, p);
            }
            Operator::GtEq | Operator::Gt => {
                if let (Some(col), Some(ts)) = (col_name(&b.left), ts_literal(&b.right)) {
                    if col == "time" {
                        p.min_ts_ns = Some(p.min_ts_ns.map_or(ts, |c| c.max(ts)));
                    }
                }
            }
            Operator::LtEq | Operator::Lt => {
                if let (Some(col), Some(ts)) = (col_name(&b.left), ts_literal(&b.right)) {
                    if col == "time" {
                        p.max_ts_ns = Some(p.max_ts_ns.map_or(ts, |c| c.min(ts)));
                    }
                }
            }
            Operator::Eq => {
                if let (Some(col), Some(v)) = (col_name(&b.left), str_literal(&b.right)) {
                    p.tag_equals.push((col, v));
                }
            }
            _ => {}
        }
    }
}

fn col_name(e: &Expr) -> Option<String> {
    match e {
        Expr::Column(c) => Some(c.name.clone()),
        Expr::Cast(c) => col_name(&c.expr),
        _ => None,
    }
}

fn ts_literal(e: &Expr) -> Option<i64> {
    match e {
        Expr::Literal(ScalarValue::TimestampNanosecond(Some(v), _), _) => Some(*v),
        Expr::Literal(ScalarValue::TimestampMicrosecond(Some(v), _), _) => {
            Some(v.checked_mul(1_000)?)
        }
        Expr::Cast(c) => ts_literal(&c.expr),
        _ => None,
    }
}

fn str_literal(e: &Expr) -> Option<String> {
    match e {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Some(s.clone()),
        Expr::Literal(ScalarValue::Dictionary(_, inner), _) => match inner.as_ref() {
            ScalarValue::Utf8(Some(s)) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    }
}

pub struct LazyTable {
    schema: SchemaRef,
    buffer: Vec<RecordBatch>,
    files: Vec<FileMeta>,
    store: Arc<dyn Store>,
    runtime: Arc<RuntimeEnv>,
}

impl std::fmt::Debug for LazyTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyTable")
            .field("buffer_batches", &self.buffer.len())
            .field("files", &self.files.len())
            .finish()
    }
}

impl LazyTable {
    /// `schema` must already be the merged schema of buffer + files
    /// (cheap: footer-only reads happen at registration in the engine).
    pub fn new(
        schema: SchemaRef,
        buffer: Vec<RecordBatch>,
        files: Vec<FileMeta>,
        store: Arc<dyn Store>,
        runtime: Arc<RuntimeEnv>,
    ) -> Self {
        LazyTable {
            schema,
            buffer,
            files,
            store,
            runtime,
        }
    }

    fn load_pruned(
        &self,
        pruning: &Pruning,
        needed: Option<&[String]>,
    ) -> Result<Vec<RecordBatch>, String> {
        // Memory accounting note (RR-1): loaded batches are handed to
        // DataFusion's memory-tracked DataSourceExec — a candidate set
        // beyond the pool budget fails there, cleanly. A separate load
        // reservation here double-counted shared dictionary buffers
        // (once per emitted batch) and rejected queries that actually
        // fit — the full-scale AT-3 run caught it.
        // buffer snapshots respect the projection too
        let mut batches = Vec::with_capacity(self.buffer.len());
        for b in &self.buffer {
            batches.push(match needed {
                None => b.clone(),
                Some(names) => {
                    let idx: Vec<usize> = names
                        .iter()
                        .filter_map(|n| b.schema().index_of(n).ok())
                        .collect();
                    b.project(&idx).map_err(|e| e.to_string())?
                }
            });
        }

        for meta in &self.files {
            // file-level time pruning (catalog bounds)
            if let Some(min) = pruning.min_ts_ns {
                if meta.max_ts_ns < min {
                    continue;
                }
            }
            if let Some(max) = pruning.max_ts_ns {
                if meta.min_ts_ns > max {
                    continue;
                }
            }
            let bytes = bytes::Bytes::from(
                self.store
                    .get(&meta.path)
                    .map_err(|e| format!("store get {}: {e}", meta.path))?,
            );
            let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
                .map_err(|e| e.to_string())?;

            // row-group bloom pruning for tag equality literals (PR-3)
            let n_rg = builder.metadata().num_row_groups();
            let keep: Vec<usize> = if pruning.tag_equals.is_empty() {
                (0..n_rg).collect()
            } else {
                bloom_keep_row_groups(&bytes, n_rg, &pruning.tag_equals)
            };
            if keep.is_empty() {
                continue;
            }

            // projection pushdown: decode only the columns the plan needs
            let builder = match needed {
                None => builder,
                Some(names) => {
                    let descr = builder.parquet_schema().clone();
                    let idx: Vec<usize> = (0..descr.num_columns())
                        .filter(|i| names.iter().any(|n| n == descr.column(*i).name()))
                        .collect();
                    let mask = datafusion::parquet::arrow::ProjectionMask::roots(&descr, idx);
                    builder.with_projection(mask)
                }
            };

            // one batch per row group: small default batches would each
            // carry (and re-count) the whole shared dictionary buffer
            let reader = builder
                .with_row_groups(keep)
                .with_batch_size(1_048_576)
                .build()
                .map_err(|e| e.to_string())?;
            for b in reader {
                batches.push(b.map_err(|e| e.to_string())?);
            }
        }
        Ok(batches)
    }
}

/// Row groups that MAY contain every tag literal, per the file's bloom
/// filters (written by flush for all dictionary columns). A group is
/// skipped only on a definite bloom miss; missing blooms keep the group.
fn bloom_keep_row_groups(
    bytes: &bytes::Bytes,
    n_rg: usize,
    tag_equals: &[(String, String)],
) -> Vec<usize> {
    use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};

    let Ok(reader) = SerializedFileReader::new(bytes.clone()) else {
        return (0..n_rg).collect();
    };
    let schema_descr = reader.metadata().file_metadata().schema_descr_ptr();
    let mut keep = Vec::with_capacity(n_rg);
    'rg: for rg in 0..n_rg {
        let Ok(rg_reader) = reader.get_row_group(rg) else {
            keep.push(rg);
            continue;
        };
        for (col, val) in tag_equals {
            let Some(idx) =
                (0..schema_descr.num_columns()).find(|i| schema_descr.column(*i).name() == col)
            else {
                continue;
            };
            if let Some(sbbf) = rg_reader.get_column_bloom_filter(idx) {
                if !sbbf.check(&val.as_str()) {
                    continue 'rg; // definite miss: skip the group
                }
            }
        }
        keep.push(rg);
    }
    keep
}

#[async_trait]
impl TableProvider for LazyTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // Inexact: we use filters to SKIP data, DataFusion re-applies them
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let pruning = extract_pruning(filters);

        // projection pushdown: read only needed columns. An EMPTY
        // projection (COUNT(*)) wants zero-column batches that still
        // carry row counts — we read the cheapest column to count rows.
        let count_only = projection.is_some_and(|p| p.is_empty());
        let (target_schema, needed): (SchemaRef, Option<Vec<String>>) = match projection {
            None => (self.schema.clone(), None),
            Some(_) if count_only => (
                Arc::new(datafusion::arrow::datatypes::Schema::empty()),
                Some(vec!["time".to_string()]),
            ),
            Some(idx) => {
                let names: Vec<String> = idx
                    .iter()
                    .map(|i| self.schema.field(*i).name().clone())
                    .collect();
                let fields: Vec<_> = names
                    .iter()
                    .map(|n| self.schema.field_with_name(n).unwrap().clone())
                    .collect();
                (
                    Arc::new(datafusion::arrow::datatypes::Schema::new(fields)),
                    Some(names),
                )
            }
        };

        let batches = self
            .load_pruned(&pruning, needed.as_deref())
            .map_err(DataFusionError::Execution)?;
        let aligned = if count_only {
            use datafusion::arrow::record_batch::RecordBatchOptions;
            batches
                .into_iter()
                .map(|b| {
                    RecordBatch::try_new_with_options(
                        target_schema.clone(),
                        vec![],
                        &RecordBatchOptions::new().with_row_count(Some(b.num_rows())),
                    )
                    .map_err(|e| e.to_string())
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(DataFusionError::Execution)?
        } else {
            // align to the (projected) schema: files may lack newer columns
            crate::align_to(target_schema.clone(), batches)
                .map_err(DataFusionError::Execution)?
        };
        let parts: Vec<Vec<RecordBatch>> = aligned.into_iter().map(|b| vec![b]).collect();
        // projection already applied via the target schema
        MemorySourceConfig::try_new_exec(&parts, target_schema, None)
            .map(|e| e as Arc<dyn ExecutionPlan>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};

    #[test]
    fn extracts_time_bounds_and_tag_literals() {
        let filters = vec![
            col("time").gt_eq(lit(ScalarValue::TimestampNanosecond(Some(100), None))),
            col("time").lt(lit(ScalarValue::TimestampNanosecond(Some(900), None))),
            col("product_id").eq(lit("p1")),
        ];
        let p = extract_pruning(&filters);
        assert_eq!(p.min_ts_ns, Some(100));
        assert_eq!(p.max_ts_ns, Some(900));
        assert_eq!(p.tag_equals, vec![("product_id".to_string(), "p1".to_string())]);
    }
}
