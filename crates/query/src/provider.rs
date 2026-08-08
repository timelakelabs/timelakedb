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

/// Engine-lifetime cache of parquet footers, keyed by object path.
/// Sound because data files are IMMUTABLE (CL-1): a path's metadata
/// never changes; superseded paths simply stop being referenced.
/// Lets warm queries prune row groups WITHOUT fetching file bytes —
/// only files that survive pruning get read at all.
pub type MetaCache = std::sync::Mutex<
    std::collections::HashMap<String, Arc<datafusion::parquet::file::metadata::ParquetMetaData>>,
>;

pub struct LazyTable {
    name: String,
    schema: SchemaRef,
    buffer: Vec<RecordBatch>,
    files: Vec<FileMeta>,
    store: Arc<dyn Store>,
    meta_cache: Arc<MetaCache>,
    /// Loads run on the blocking pool with this deadline: a slow scan is
    /// abandoned between files instead of pinning the async runtime
    /// forever (the M4 hang that wedged a whole Docker VM).
    load_timeout: std::time::Duration,
    /// The SHARED pool (RR-1): loads try_grow here and fail cleanly.
    pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        schema: SchemaRef,
        buffer: Vec<RecordBatch>,
        files: Vec<FileMeta>,
        store: Arc<dyn Store>,
        load_timeout: std::time::Duration,
        pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
        meta_cache: Arc<MetaCache>,
    ) -> Self {
        LazyTable {
            name,
            schema,
            buffer,
            files,
            store,
            load_timeout,
            pool,
            meta_cache,
        }
    }
}

/// Row groups whose column-chunk statistics ADMIT every tag literal.
/// Settled files are entity-clustered by compaction, so these ranges are
/// tight; a group is skipped only when a literal falls outside its
/// min/max (bloom filters would be sharper, but the arrow writer emits
/// none for dictionary columns — proven by test).
pub fn stats_keep_row_groups(
    metadata: &datafusion::parquet::file::metadata::ParquetMetaData,
    tag_equals: &[(String, String)],
) -> Vec<usize> {
    use datafusion::parquet::file::statistics::Statistics;

    let descr = metadata.file_metadata().schema_descr();
    let n_rg = metadata.num_row_groups();
    let mut keep = Vec::with_capacity(n_rg);
    'rg: for rg in 0..n_rg {
        for (col, val) in tag_equals {
            let Some(idx) =
                (0..descr.num_columns()).find(|i| descr.column(*i).name() == col)
            else {
                continue;
            };
            let col_meta = metadata.row_group(rg).column(idx);
            if let Some(Statistics::ByteArray(s)) = col_meta.statistics() {
                if let (Some(min), Some(max)) = (s.min_opt(), s.max_opt()) {
                    let v = val.as_bytes();
                    if v < min.data() || v > max.data() {
                        continue 'rg; // literal outside this group's range
                    }
                }
            }
        }
        keep.push(rg);
    }
    keep
}

/// The blocking half of scan: runs on the blocking pool, checks the
/// deadline between files (RR-2 — abandonable, never pins the runtime).
#[allow(clippy::too_many_arguments)]
fn load_pruned(
    buffer: &[RecordBatch],
    files: &[FileMeta],
    store: &Arc<dyn Store>,
    pruning: &Pruning,
    needed: Option<&[String]>,
    deadline: std::time::Instant,
    pool: &Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
    table: &str,
    meta_cache: &Arc<MetaCache>,
) -> Result<(Vec<RecordBatch>, datafusion::execution::memory_pool::MemoryReservation), String> {
    use datafusion::execution::memory_pool::MemoryConsumer;
    // RR-1: loads are pool-visible at ACTUAL batch size. Accurate now:
    // with batch_size >= row-group size each batch owns its dictionary
    // (the earlier double-count came from 1024-row batches sharing one
    // RG dictionary). The process must never be OOM-killable by a load.
    let mut reservation = MemoryConsumer::new(format!("scan:{table}")).register(pool);
    let mut batches = Vec::with_capacity(buffer.len());
    for b in buffer {
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

    for meta in files {
        if std::time::Instant::now() >= deadline {
            return Err("scan load deadline exceeded — query abandoned (RR-2)".to_string());
        }
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

        // metadata-cache fast path: on a warm footer, decide row-group
        // pruning WITHOUT fetching the file — only survivors get read
        let cached_md = meta_cache
            .lock()
            .expect("meta cache lock")
            .get(&meta.path)
            .cloned();
        let (keep, mut bytes_opt): (Vec<usize>, Option<bytes::Bytes>) = match cached_md {
            Some(md) => {
                let keep = if pruning.tag_equals.is_empty() {
                    (0..md.num_row_groups()).collect()
                } else {
                    stats_keep_row_groups(&md, &pruning.tag_equals)
                };
                (keep, None)
            }
            None => {
                let bytes = bytes::Bytes::from(
                    store
                        .get(&meta.path)
                        .map_err(|e| format!("store get {}: {e}", meta.path))?,
                );
                let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
                    .map_err(|e| e.to_string())?;
                let md: Arc<_> = Arc::new(builder.metadata().as_ref().clone());
                {
                    let mut cache = meta_cache.lock().expect("meta cache lock");
                    if cache.len() > 4096 {
                        cache.clear(); // crude bound; files are few post-compaction
                    }
                    cache.insert(meta.path.clone(), md.clone());
                }
                let keep = if pruning.tag_equals.is_empty() {
                    (0..md.num_row_groups()).collect()
                } else {
                    stats_keep_row_groups(&md, &pruning.tag_equals)
                };
                (keep, Some(bytes))
            }
        };
        if keep.is_empty() {
            continue;
        }
        let bytes = match bytes_opt.take() {
            Some(b) => b,
            None => bytes::Bytes::from(
                store
                    .get(&meta.path)
                    .map_err(|e| format!("store get {}: {e}", meta.path))?,
            ),
        };
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(|e| e.to_string())?;

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

        // decode-time row filtering (PR-3's last mile): for tag equality
        // literals, only MATCHING rows materialize — a journey pulls its
        // ~20 rows out of each kept row group instead of all 64K
        let builder = if pruning.tag_equals.is_empty() {
            builder
        } else {
            use datafusion::parquet::arrow::ProjectionMask;
            use datafusion::parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
            let descr = builder.parquet_schema().clone();
            let mut predicates: Vec<Box<dyn datafusion::parquet::arrow::arrow_reader::ArrowPredicate>> =
                Vec::new();
            for (col, val) in pruning.tag_equals.clone() {
                let Some(idx) =
                    (0..descr.num_columns()).find(|i| descr.column(*i).name() == col)
                else {
                    continue;
                };
                let mask = ProjectionMask::roots(&descr, [idx]);
                predicates.push(Box::new(ArrowPredicateFn::new(mask, move |batch| {
                    use datafusion::arrow::array::StringArray;
                    use datafusion::arrow::compute::kernels::cmp::eq;
                    let scalar = StringArray::new_scalar(val.clone());
                    eq(batch.column(0), &scalar)
                })));
            }
            if predicates.is_empty() {
                builder
            } else {
                builder.with_row_filter(RowFilter::new(predicates))
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
            let b = b.map_err(|e| e.to_string())?;
            reservation
                .try_grow(b.get_array_memory_size())
                .map_err(|e| format!("query memory budget exceeded at {}: {e}", meta.path))?;
            batches.push(b);
        }
    }
    tracing::info!(
        table,
        files_total = files.len(),
        batches = batches.len(),
        reserved_mb = reservation.size() / (1024 * 1024),
        pruning = ?pruning,
        "scan load complete"
    );
    Ok((batches, reservation))
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

        // blocking half runs on the blocking pool, abandonable (RR-2)
        let buffer = self.buffer.clone();
        let files = self.files.clone();
        let store = self.store.clone();
        let needed_owned = needed.clone();
        let pruning_owned = pruning.clone();
        let deadline = std::time::Instant::now() + self.load_timeout;
        let pool = self.pool.clone();
        let table_name = self.name.clone();
        let meta_cache = self.meta_cache.clone();
        let (batches, reservation) = tokio::task::spawn_blocking(move || {
            load_pruned(
                &buffer,
                &files,
                &store,
                &pruning_owned,
                needed_owned.as_deref(),
                deadline,
                &pool,
                &table_name,
                &meta_cache,
            )
        })
        .await
        .map_err(|e| DataFusionError::Execution(format!("scan load task: {e}")))?
        .map_err(DataFusionError::Execution)?;
        // The reservation's job is done: try_grow during the load is what
        // rejects an oversized candidate set BEFORE memory blows up (the
        // crash cause). Accounting is released here because the plan API
        // gives the batches, not us, to the executor; execution-time
        // residency is bounded instead by admission control
        // (max_concurrent_queries × pool). Tying reservations to plan
        // lifetime is the M5 streaming-exec work.
        drop(reservation);
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
    fn stats_prune_clustered_row_groups() {
        use timelord_buffer::{TableBuffer, flush};
        use timelord_ingest::parse_lines;

        let t = 1_786_179_600_000_000_000i64;
        let lp: String = (0..2000)
            .map(|i| format!("m,pid=p{:05} v=1.0 {}\n", (i * 7919) % 2000, t + i))
            .collect();
        let mut buf = TableBuffer::default();
        for line in parse_lines(&lp, 1, 0).unwrap() {
            buf.append(&line).unwrap();
        }
        let parts =
            flush::prepare_ordered(&buf.snapshot().unwrap(), Some("pid")).unwrap();
        let bytes = flush::to_parquet_bytes_rg(&parts[0].1, Some(256)).unwrap();
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
        let md = builder.metadata();
        let total = md.num_row_groups();
        assert!(total > 3);

        // a specific entity: only its slice of the clustered file survives
        let keep = stats_keep_row_groups(md, &[("pid".into(), "p00042".into())]);
        assert!(
            keep.len() <= 2,
            "expected <=2 of {total} row groups, kept {}",
            keep.len()
        );
        // beyond every range: nothing survives
        let keep = stats_keep_row_groups(md, &[("pid".into(), "zzzz".into())]);
        assert!(keep.is_empty());
        // no literals: everything survives
        let keep = stats_keep_row_groups(md, &[]);
        assert_eq!(keep.len(), total);
    }

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
