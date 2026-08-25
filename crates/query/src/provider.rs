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
//! This provider is also SEC-2's enforcement point: scan() calls the
//! mandatory-predicate hook unconditionally and applies the returned
//! restriction to every batch — buffer and file alike — before the
//! execution plan is built, below any user filter and any aggregation.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use timelake_catalog::FileMeta;
use timelake_store::Store;

use datafusion::arrow::record_batch::RecordBatch;

/// A Parquet file read through the [`Store`] in ranges rather than whole.
///
/// The reader asks for the footer, then for the column chunks of the row
/// groups that survived pruning; nothing else leaves the store. Before
/// this, every scan pulled the entire object into memory and `with_row_groups`
/// bounded only *decoding* — which is why finer row groups measured as a
/// regression rather than a win (../Gauge/PERFORMANCE_LOG.md).
pub struct StoreFile {
    store: Arc<dyn Store>,
    path: String,
    len: u64,
    /// Ranges already fetched, sorted by start and non-overlapping. The
    /// reader is served from these; anything outside falls back to a
    /// fresh range request.
    blocks: Vec<(u64, bytes::Bytes)>,
}

impl StoreFile {
    pub fn open(store: Arc<dyn Store>, path: &str) -> Result<Self, String> {
        let len = store
            .size(path)
            .map_err(|e| format!("store size {path}: {e}"))?;
        Ok(Self::with_len(store, path, len))
    }

    /// The catalog already records every file's size, so a scan does not
    /// need to stat it — one syscall per candidate file, on a path that
    /// touches every file in the partition.
    pub fn with_len(store: Arc<dyn Store>, path: &str, len: u64) -> Self {
        StoreFile {
            store,
            path: path.to_string(),
            len,
            blocks: Vec::new(),
        }
    }

    /// Fetch exactly the bytes the kept row groups occupy — one request per
    /// group, because a group's column chunks are contiguous on disk.
    ///
    /// Doing this up front is what makes pruning worth anything: without it
    /// the reader pulls whatever it happens to need through small reads and
    /// (with read-ahead) can fetch several times the file.
    pub fn prefetch_row_groups(
        &mut self,
        md: &datafusion::parquet::file::metadata::ParquetMetaData,
        keep: &[usize],
    ) -> Result<(), String> {
        let mut spans: Vec<(u64, u64)> = Vec::with_capacity(keep.len());
        for &rg in keep {
            let (mut lo, mut hi) = (u64::MAX, 0u64);
            for c in md.row_group(rg).columns() {
                let (start, len) = c.byte_range();
                lo = lo.min(start);
                hi = hi.max(start + len);
            }
            if lo < hi {
                spans.push((lo, hi));
            }
        }
        spans.sort_unstable();

        // Coalesce neighbours before fetching. A point lookup keeps one or
        // two groups and reads a sliver; a full scan keeps them all, and
        // without this it paid a separate request per group — which is what
        // turned the funnel queries 2.6x slower when this was first measured.
        // Bridging a small gap is cheaper than a second round trip.
        const GAP: u64 = 64 << 10;
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(spans.len());
        for (lo, hi) in spans {
            match merged.last_mut() {
                Some((_, prev_hi)) if lo <= *prev_hi + GAP => *prev_hi = (*prev_hi).max(hi),
                _ => merged.push((lo, hi)),
            }
        }

        for (lo, hi) in merged {
            let bytes = self
                .store
                .get_range(&self.path, lo, (hi - lo) as usize)
                .map_err(|e| format!("store range {}: {e}", self.path))?;
            self.blocks.push((lo, bytes::Bytes::from(bytes)));
        }
        self.blocks.sort_by_key(|(start, _)| *start);
        Ok(())
    }

    /// The prefetched block containing `start`, and the offset into it.
    fn covering(&self, start: u64) -> Option<(&bytes::Bytes, usize)> {
        self.blocks.iter().find_map(|(s, b)| {
            (start >= *s && start < *s + b.len() as u64).then(|| (b, (start - *s) as usize))
        })
    }
}

impl datafusion::parquet::file::reader::Length for StoreFile {
    fn len(&self) -> u64 {
        self.len
    }
}

impl datafusion::parquet::file::reader::ChunkReader for StoreFile {
    type T = Box<dyn std::io::Read + Send>;

    fn get_read(&self, start: u64) -> datafusion::parquet::errors::Result<Self::T> {
        if let Some((block, off)) = self.covering(start) {
            return Ok(Box::new(std::io::Cursor::new(block.slice(off..))));
        }
        Ok(Box::new(StoreRangeReader {
            store: self.store.clone(),
            path: self.path.clone(),
            pos: start,
            end: self.len,
            buf: Vec::new(),
            buf_pos: 0,
        }))
    }

    fn get_bytes(
        &self,
        start: u64,
        length: usize,
    ) -> datafusion::parquet::errors::Result<bytes::Bytes> {
        if let Some((block, off)) = self.covering(start)
            && off + length <= block.len()
        {
            return Ok(block.slice(off..off + length));
        }
        let v = self
            .store
            .get_range(&self.path, start, length)
            .map_err(|e| {
                datafusion::parquet::errors::ParquetError::General(format!(
                    "store range {}[{start}+{length}]: {e}",
                    self.path
                ))
            })?;
        Ok(bytes::Bytes::from(v))
    }
}

/// Sequential reader over a store object, fetched in chunks on demand.
/// The Parquet reader takes a column chunk as `get_read(start).take(len)`,
/// so this never needs to know the chunk length up front.
pub struct StoreRangeReader {
    store: Arc<dyn Store>,
    path: String,
    pos: u64,
    end: u64,
    buf: Vec<u8>,
    buf_pos: usize,
}

impl StoreRangeReader {
    /// Fallback granularity only — the row groups a scan actually decodes
    /// are prefetched whole by [`StoreFile::prefetch_row_groups`]. Kept
    /// small because read-ahead here is speculative: at 1 MiB a scan of a
    /// 228 KB file read 1.79 MB, which is how the first attempt at this
    /// was caught.
    const CHUNK: usize = 64 << 10;
}

impl std::io::Read for StoreRangeReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.buf_pos >= self.buf.len() {
            if self.pos >= self.end {
                return Ok(0);
            }
            let want = Self::CHUNK.min((self.end - self.pos) as usize);
            self.buf = self.store.get_range(&self.path, self.pos, want)?;
            self.buf_pos = 0;
            if self.buf.is_empty() {
                return Ok(0);
            }
            self.pos += self.buf.len() as u64;
        }
        let n = out.len().min(self.buf.len() - self.buf_pos);
        out[..n].copy_from_slice(&self.buf[self.buf_pos..self.buf_pos + n]);
        self.buf_pos += n;
        Ok(n)
    }
}

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
                if let (Some(col), Some(ts)) = (col_name(&b.left), ts_literal(&b.right))
                    && col == "time"
                {
                    p.min_ts_ns = Some(p.min_ts_ns.map_or(ts, |c| c.max(ts)));
                }
            }
            Operator::LtEq | Operator::Lt => {
                if let (Some(col), Some(ts)) = (col_name(&b.left), ts_literal(&b.right))
                    && col == "time"
                {
                    p.max_ts_ns = Some(p.max_ts_ns.map_or(ts, |c| c.min(ts)));
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
        // A predicate against a view column coerces its literal to
        // Utf8View. Missing this arm does not fail — it silently stops
        // extracting tag equality, which turns off file pruning and the
        // bloom filters, i.e. it costs Shape A everything it has.
        Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => Some(s.clone()),
        Expr::Literal(ScalarValue::Dictionary(_, inner), _) => match inner.as_ref() {
            ScalarValue::Utf8(Some(s)) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// The schema this provider PRESENTS: dictionary-encoded string columns
/// become `Utf8View`. Storage, the WAL and the write buffer are untouched,
/// so FR-2's column economics are unchanged — this is the planner's view
/// of the table, not the file's.
///
/// Why: DataFusion's column-wise group-values path covers `Utf8`,
/// `Utf8View`, `BinaryView` and the primitives, and NOT `Dictionary`, so
/// every tag GROUP BY falls into the row-format fallback that materialises
/// a string per row per key. On this workload that single operator is ~85%
/// of B2's plan compute (../Gauge/PERFORMANCE_LOG.md).
pub fn view_schema(schema: &datafusion::arrow::datatypes::Schema) -> SchemaRef {
    use datafusion::arrow::datatypes::{DataType, Schema};
    if !schema.fields().iter().any(|f| is_dict_utf8(f.data_type())) {
        return Arc::new(schema.clone());
    }
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| {
            if is_dict_utf8(f.data_type()) {
                Arc::new(f.as_ref().clone().with_data_type(DataType::Utf8View))
            } else {
                f.clone()
            }
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn is_dict_utf8(t: &datafusion::arrow::datatypes::DataType) -> bool {
    use datafusion::arrow::datatypes::DataType;
    matches!(t, DataType::Dictionary(_, v) if **v == DataType::Utf8)
}

/// Convert one decoded batch's dictionary columns to `Utf8View`.
///
/// Called on whichever worker thread decoded the batch, which is the whole
/// point: arrow builds the views over the dictionary's existing value
/// buffer and copies no string, so the conversion is cheap — but doing it
/// on ONE thread after the parallel load has finished is what sank the
/// first attempt at this idea. It measured as ~50 ms of a 53 ms query,
/// outside the execution plan entirely.
fn to_view_batch(b: RecordBatch) -> Result<RecordBatch, String> {
    use datafusion::arrow::compute::cast;
    if !b
        .schema()
        .fields()
        .iter()
        .any(|f| is_dict_utf8(f.data_type()))
    {
        return Ok(b);
    }
    let schema = view_schema(b.schema_ref());
    let cols = b
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(c, f)| {
            if c.data_type() == f.data_type() {
                Ok(c.clone())
            } else {
                cast(c, f.data_type()).map_err(|e| e.to_string())
            }
        })
        .collect::<Result<Vec<_>, String>>()?;
    RecordBatch::try_new(schema, cols).map_err(|e| e.to_string())
}

/// Engine-lifetime cache of parquet footers, keyed by object path.
/// Sound because data files are IMMUTABLE (CL-1): a path's metadata
/// never changes; superseded paths simply stop being referenced.
/// Lets warm queries prune row groups WITHOUT fetching file bytes —
/// only files that survive pruning get read at all.
pub type MetaCache = std::sync::Mutex<
    std::collections::HashMap<String, Arc<datafusion::parquet::file::metadata::ParquetMetaData>>,
>;

/// One file's Parquet footer, from the cache or by a range read (never by
/// fetching the file). The cache is the M5 metadata cache: footers are
/// immutable, so a hit is always valid.
/// Returns the metadata and whether it came from the cache (`true`) or a cold
/// footer read (`false`) — the caller on the scan path counts that (#69), a
/// schema read at registration ignores it.
fn cached_metadata(
    file: &StoreFile,
    path: &str,
    meta_cache: &Arc<MetaCache>,
) -> Result<
    (
        Arc<datafusion::parquet::file::metadata::ParquetMetaData>,
        bool,
    ),
    String,
> {
    use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    if let Some(md) = meta_cache
        .lock()
        .expect("meta cache lock")
        .get(path)
        .cloned()
    {
        return Ok((md, true));
    }
    // footer only — a range read, not the file
    let loaded = ArrowReaderMetadata::load(file, ArrowReaderOptions::default())
        .map_err(|e| e.to_string())?;
    let md = loaded.metadata().clone();
    let mut cache = meta_cache.lock().expect("meta cache lock");
    if cache.len() > 4096 {
        cache.clear(); // crude bound; files are few post-compaction
    }
    cache.insert(path.to_string(), md.clone());
    Ok((md, false))
}

/// The Arrow schema of one cataloged file, read from its footer alone.
///
/// The engine's schema registry is built from this. It used to `get` the
/// whole object and decode it just to reach `batch.schema()` — tolerable on
/// local disk at boot, wrong the moment the store is S3 and a *querier*
/// (CL-3) re-reads it every time a new file appears for a table.
pub fn file_schema(
    store: &Arc<dyn Store>,
    path: &str,
    size_bytes: u64,
    meta_cache: &Arc<MetaCache>,
) -> Result<SchemaRef, String> {
    use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    let file = StoreFile::with_len(store.clone(), path, size_bytes);
    let (md, _) = cached_metadata(&file, path, meta_cache)?; // schema read, not a scan
    let arrow_md = ArrowReaderMetadata::try_new(md, ArrowReaderOptions::default())
        .map_err(|e| e.to_string())?;
    Ok(arrow_md.schema().clone())
}

/// Per-scan pruning telemetry (#69): where a lookup's cost actually goes —
/// how many files and row groups a scan considered versus skipped, by which
/// mechanism, and whether the footer was already cached. Engine-wide running
/// totals; a single lookup on an idle node reads as the delta. The point is
/// to make "Shape A scanned every row group" show up in a counter rather than
/// only under a profiler, and to tell time-pruning from stats-pruning from
/// bloom-pruning apart — which is the whole question #68 turns on.
#[derive(Default)]
pub struct ScanStats {
    /// Files a scan opened (passed the query's file list).
    pub files_considered: std::sync::atomic::AtomicU64,
    /// Of those, skipped whole by the catalog time bounds before any read.
    pub files_time_pruned: std::sync::atomic::AtomicU64,
    /// Row groups in the files that survived file-level time pruning.
    pub row_groups_considered: std::sync::atomic::AtomicU64,
    /// Excluded because a tag literal fell outside the group's min/max stats
    /// (tight only on entity-clustered/settled files).
    pub row_groups_stats_pruned: std::sync::atomic::AtomicU64,
    /// Excluded because a bloom said the tag is definitely absent (works on
    /// unclustered L0 too — the mechanism #68's premise assumed was missing).
    pub row_groups_bloom_pruned: std::sync::atomic::AtomicU64,
    /// Actually fetched and decoded. This is the number Shape A must shrink.
    pub row_groups_scanned: std::sync::atomic::AtomicU64,
    /// Footer served from the metadata cache (warm) vs read cold.
    pub meta_cache_hits: std::sync::atomic::AtomicU64,
    pub meta_cache_misses: std::sync::atomic::AtomicU64,
}

pub struct LazyTable {
    name: String,
    schema: SchemaRef,
    buffer: Vec<RecordBatch>,
    files: Vec<FileMeta>,
    store: Arc<dyn Store>,
    meta_cache: Arc<MetaCache>,
    /// #69 scan telemetry, engine-wide, threaded like `filtered_rows`.
    scan_stats: Arc<ScanStats>,
    /// Loads run on the blocking pool with this deadline: a slow scan is
    /// abandoned between files instead of pinning the async runtime
    /// forever (the M4 hang that wedged a whole Docker VM).
    load_timeout: std::time::Duration,
    /// The SHARED pool (RR-1): loads try_grow here and fail cleanly.
    pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
    /// Who is asking (SEC-2): scan() hands this to the mandatory
    /// predicate; providers are built per query, so this is per session.
    session: crate::QuerySession,
    /// Rows the mandatory predicate dropped, engine-wide (observability:
    /// enforcement that leaves no trace is indistinguishable from a bug).
    filtered_rows: Arc<std::sync::atomic::AtomicU64>,
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
        session: crate::QuerySession,
        filtered_rows: Arc<std::sync::atomic::AtomicU64>,
        scan_stats: Arc<ScanStats>,
    ) -> Self {
        LazyTable {
            name,
            schema: view_schema(&schema),
            buffer,
            files,
            store,
            load_timeout,
            pool,
            meta_cache,
            session,
            filtered_rows,
            scan_stats,
        }
    }
}

/// Does any row group's bloom filter positively exclude a tag literal?
///
/// A bloom answers "definitely not here" exactly, and "maybe" otherwise, so
/// one negative for any literal means the group cannot match. Statistics
/// only bound a range; on unclustered L0 data every range spans everything
/// and prunes nothing, which is precisely where this earns its keep.
///
/// Returns the groups that survive. Reads one small range per group per
/// literal — cheap against the alternative of decoding the group.
fn bloom_keep_row_groups(
    file: &StoreFile,
    md: &datafusion::parquet::file::metadata::ParquetMetaData,
    tag_equals: &[(String, String)],
    candidates: Vec<usize>,
) -> Vec<usize> {
    use datafusion::parquet::bloom_filter::Sbbf;
    let descr = md.file_metadata().schema_descr_ptr();
    let cols: Vec<(usize, &str)> = tag_equals
        .iter()
        .filter_map(|(col, val)| {
            (0..descr.num_columns())
                .find(|i| descr.column(*i).name() == col)
                .map(|i| (i, val.as_str()))
        })
        .collect();
    if cols.is_empty() {
        return candidates;
    }

    candidates
        .into_iter()
        .filter(|&rg| {
            let group = md.row_group(rg);
            // keep unless a bloom says the value is definitely absent
            !cols.iter().any(|(idx, val)| {
                let chunk = group.column(*idx);
                if chunk.bloom_filter_offset().is_none() {
                    return false; // no bloom written: cannot exclude
                }
                match Sbbf::read_from_column_chunk(chunk, file) {
                    Ok(Some(sbbf)) => !sbbf.check(*val),
                    _ => false,
                }
            })
        })
        .collect()
}

/// Row groups whose column-chunk statistics ADMIT every tag literal.
/// Settled files are entity-clustered by compaction, so these ranges are
/// tight; a group is skipped only when a literal falls outside its
/// min/max. Bloom filters (see [`bloom_keep_row_groups`]) are sharper and
/// work on unclustered data too.
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
            let Some(idx) = (0..descr.num_columns()).find(|i| descr.column(*i).name() == col)
            else {
                continue;
            };
            let col_meta = metadata.row_group(rg).column(idx);
            if let Some(Statistics::ByteArray(s)) = col_meta.statistics()
                && let (Some(min), Some(max)) = (s.min_opt(), s.max_opt())
            {
                let v = val.as_bytes();
                if v < min.data() || v > max.data() {
                    continue 'rg; // literal outside this group's range
                }
            }
        }
        keep.push(rg);
    }
    keep
}

/// Decode ONE candidate file: prune its row groups, fetch the survivors,
/// decode them. Split out of [`load_pruned`] because files are independent
/// — nothing here reads another file's state, which is what lets the loads
/// run concurrently.
///
/// `Ok(None)` means the file was pruned away entirely and never read.
fn load_one_file(
    meta: &FileMeta,
    store: &Arc<dyn Store>,
    pruning: &Pruning,
    needed: Option<&[String]>,
    meta_cache: &Arc<MetaCache>,
    scan_stats: &Arc<ScanStats>,
    reservation: &std::sync::Mutex<datafusion::execution::memory_pool::MemoryReservation>,
) -> Result<Option<Vec<RecordBatch>>, String> {
    use std::sync::atomic::Ordering::Relaxed;
    scan_stats.files_considered.fetch_add(1, Relaxed);
    // file-level time pruning (catalog bounds)
    if let Some(min) = pruning.min_ts_ns
        && meta.max_ts_ns < min
    {
        scan_stats.files_time_pruned.fetch_add(1, Relaxed);
        return Ok(None);
    }
    if let Some(max) = pruning.max_ts_ns
        && meta.min_ts_ns > max
    {
        scan_stats.files_time_pruned.fetch_add(1, Relaxed);
        return Ok(None);
    }

    // metadata-cache fast path: on a warm footer, decide row-group
    // pruning WITHOUT fetching the file — only survivors get read.
    // Range-read handle: only the footer and the surviving row groups
    // ever leave the store.
    use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    let mut file = StoreFile::with_len(store.clone(), &meta.path, meta.size_bytes);
    let (md, cache_hit): (Arc<_>, bool) = cached_metadata(&file, &meta.path, meta_cache)?;
    if cache_hit {
        scan_stats.meta_cache_hits.fetch_add(1, Relaxed);
    } else {
        scan_stats.meta_cache_misses.fetch_add(1, Relaxed);
    }

    let n_rg = md.num_row_groups();
    scan_stats
        .row_groups_considered
        .fetch_add(n_rg as u64, Relaxed);
    let keep: Vec<usize> = if pruning.tag_equals.is_empty() {
        (0..n_rg).collect()
    } else {
        // statistics first (free — already in the footer), then blooms
        // for whatever survives. On fresh L0 data statistics prune
        // nothing, because an unclustered group's min/max spans the
        // whole entity space; the bloom is what actually excludes (#68).
        let by_stats = stats_keep_row_groups(&md, &pruning.tag_equals);
        let n_stats = by_stats.len();
        scan_stats
            .row_groups_stats_pruned
            .fetch_add((n_rg - n_stats) as u64, Relaxed);
        let after_bloom = bloom_keep_row_groups(&file, &md, &pruning.tag_equals, by_stats);
        scan_stats
            .row_groups_bloom_pruned
            .fetch_add((n_stats - after_bloom.len()) as u64, Relaxed);
        after_bloom
    };
    scan_stats
        .row_groups_scanned
        .fetch_add(keep.len() as u64, Relaxed);
    if keep.is_empty() {
        return Ok(None);
    }

    // fetch just those groups, then decode from memory
    file.prefetch_row_groups(&md, &keep)?;
    let arrow_md = ArrowReaderMetadata::try_new(md, ArrowReaderOptions::default())
        .map_err(|e| e.to_string())?;
    let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, arrow_md);

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
            let Some(idx) = (0..descr.num_columns()).find(|i| descr.column(*i).name() == col)
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
    let mut out = Vec::new();
    for b in reader {
        // Dictionary -> Utf8View HERE, on the decoding worker, so the
        // conversion is as wide as the load is (see `to_view_batch`).
        let b = to_view_batch(b.map_err(|e| e.to_string())?)?;
        // RR-1 still guards every batch, on whichever thread produced it:
        // the reservation is what rejects an oversized candidate set
        // BEFORE memory blows up, so it must stay inside the decode loop
        // rather than being applied after the join.
        reservation
            .lock()
            .expect("scan reservation lock")
            .try_grow(b.get_array_memory_size())
            .map_err(|e| format!("query memory budget exceeded at {}: {e}", meta.path))?;
        out.push(b);
    }
    Ok(Some(out))
}

/// How many threads a scan may use to decode its candidate files.
///
/// Bounded well below the core count on purpose: the write path and the
/// maintenance tick share this machine, and the ingest-contention carve-out
/// says a query that grabs every core is a regression somewhere else.
fn scan_threads(files: usize) -> usize {
    const MAX: usize = 8;
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    files.min(cores).clamp(1, MAX)
}

/// The blocking half of scan: runs on the blocking pool, checks the
/// deadline between files (RR-2 — abandonable, never pins the runtime).
///
/// Candidate files are decoded CONCURRENTLY. They are independent — each
/// one prunes and decodes from its own `StoreFile` — while the aggregation
/// above the scan was already running ~12-wide on this host, so a serial
/// load was the one part of a Shape B query pinned to a single core.
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
    scan_stats: &Arc<ScanStats>,
) -> Result<
    (
        Vec<RecordBatch>,
        datafusion::execution::memory_pool::MemoryReservation,
    ),
    String,
> {
    use datafusion::execution::memory_pool::MemoryConsumer;
    use std::sync::atomic::{AtomicUsize, Ordering};
    // RR-1: loads are pool-visible at ACTUAL batch size. Accurate now:
    // with batch_size >= row-group size each batch owns its dictionary
    // (the earlier double-count came from 1024-row batches sharing one
    // RG dictionary). The process must never be OOM-killable by a load.
    let reservation =
        std::sync::Mutex::new(MemoryConsumer::new(format!("scan:{table}")).register(pool));
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

    // Results land in file order regardless of which thread produced them,
    // so a plan sees exactly the batch sequence it saw when this loop was
    // serial. Cheap insurance against a "why did the row order change"
    // hunt later.
    let slots: Vec<std::sync::Mutex<Option<Vec<RecordBatch>>>> = (0..files.len())
        .map(|_| std::sync::Mutex::new(None))
        .collect();
    let next = AtomicUsize::new(0);
    let failure: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

    std::thread::scope(|scope| {
        for _ in 0..scan_threads(files.len()) {
            scope.spawn(|| {
                loop {
                    if failure.lock().expect("scan failure lock").is_some() {
                        return;
                    }
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let Some(meta) = files.get(i) else { return };
                    // RR-2: the deadline is still checked between files, now
                    // by every worker, so a slow scan is abandoned rather
                    // than pinning threads until it finishes.
                    if std::time::Instant::now() >= deadline {
                        *failure.lock().expect("scan failure lock") = Some(
                            "scan load deadline exceeded — query abandoned (RR-2)".to_string(),
                        );
                        return;
                    }
                    match load_one_file(
                        meta,
                        store,
                        pruning,
                        needed,
                        meta_cache,
                        scan_stats,
                        &reservation,
                    ) {
                        Ok(None) => {}
                        Ok(Some(b)) => {
                            *slots[i].lock().expect("scan slot lock") = Some(b);
                        }
                        Err(e) => {
                            let mut f = failure.lock().expect("scan failure lock");
                            if f.is_none() {
                                *f = Some(e);
                            }
                            return;
                        }
                    }
                }
            });
        }
    });

    if let Some(err) = failure.into_inner().expect("scan failure lock") {
        return Err(err);
    }
    for slot in slots {
        if let Some(b) = slot.into_inner().expect("scan slot lock") {
            batches.extend(b);
        }
    }

    let reservation = reservation.into_inner().expect("scan reservation lock");
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let pruning = extract_pruning(filters);

        // SEC-2: THE mandatory-predicate call — unconditional, per scan.
        // Whatever it returns is applied to every batch below, before the
        // plan exists, so no query shape can aggregate around it. It may now
        // return more than one restriction (visibility, then R-1 tombstones).
        let restrictions = crate::mandatory_predicate(&self.session, &self.name, &self.schema);
        // Columns every restriction needs materialized even when the query
        // projects none of them (the visibility label; the time and tag
        // columns a tombstone tests). Without these, a narrow projection
        // would strip the mask's inputs and a hidden row would leak.
        let mut extra_cols: Vec<String> = Vec::new();
        for r in &restrictions {
            for c in r.required_columns() {
                if !extra_cols.contains(&c) {
                    extra_cols.push(c);
                }
            }
        }

        // projection pushdown: read only needed columns. An EMPTY
        // projection (COUNT(*)) wants zero-column batches that still
        // carry row counts — we read the cheapest column to count rows.
        // Under a restriction the mask columns ride along even when the
        // query never mentions them (COUNT(*) must not count hidden rows);
        // align_to / the count path drop them again before results form.
        let count_only = projection.is_some_and(|p| p.is_empty());
        let (target_schema, needed): (SchemaRef, Option<Vec<String>>) = match projection {
            None => (self.schema.clone(), None),
            Some(_) if count_only => {
                let mut cols = vec!["time".to_string()];
                for c in &extra_cols {
                    if !cols.contains(c) {
                        cols.push(c.clone());
                    }
                }
                (
                    Arc::new(datafusion::arrow::datatypes::Schema::empty()),
                    Some(cols),
                )
            }
            Some(idx) => {
                let names: Vec<String> = idx
                    .iter()
                    .map(|i| self.schema.field(*i).name().clone())
                    .collect();
                let fields: Vec<_> = names
                    .iter()
                    .map(|n| self.schema.field_with_name(n).unwrap().clone())
                    .collect();
                let mut read_names = names.clone();
                for c in &extra_cols {
                    if !read_names.iter().any(|n| n == c) {
                        read_names.push(c.clone());
                    }
                }
                (
                    Arc::new(datafusion::arrow::datatypes::Schema::new(fields)),
                    Some(read_names),
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
        let scan_stats = self.scan_stats.clone();
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
                &scan_stats,
            )
        })
        .await
        .map_err(|e| DataFusionError::Execution(format!("scan load task: {e}")))?
        .map_err(DataFusionError::Execution)?;
        // SEC-2 enforcement: every batch — buffer snapshot and file alike —
        // passes each restriction before the plan is built.
        let batches = if restrictions.is_empty() {
            batches
        } else {
            let mut kept = Vec::with_capacity(batches.len());
            let mut dropped: u64 = 0;
            for b in batches {
                let before = b.num_rows();
                let mut cur = b;
                for r in &restrictions {
                    cur = crate::apply_restriction(r, &cur).map_err(DataFusionError::Execution)?;
                }
                dropped += (before - cur.num_rows()) as u64;
                kept.push(cur);
            }
            self.filtered_rows
                .fetch_add(dropped, std::sync::atomic::Ordering::Relaxed);
            kept
        };
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
            crate::align_to(target_schema.clone(), batches).map_err(DataFusionError::Execution)?
        };
        // At most `target_partitions` partitions — but never ZERO. A scan
        // that prunes everything away used to build a source with no
        // partitions at all, and any plan with a SortExec above it (every
        // ORDER BY) then failed its sanity check with a 400 instead of
        // returning an empty result. Sharper pruning makes that case common,
        // so it has to be an empty partition rather than no partition.
        //
        // It used to be one partition PER BATCH, which looks like free
        // parallelism and is not: a partition then holds exactly one batch,
        // so every operator above the scan makes its first decision on its
        // last input. That is precisely what defeats DataFusion's
        // partial-aggregation skip (see run_sql_env) — it can only act on a
        // batch that comes after the one it measured. Packing round-robin
        // keeps the partitions the same size within one batch while giving
        // each one a sequence to work along.
        let parts: Vec<Vec<RecordBatch>> = if aligned.is_empty() {
            vec![Vec::new()]
        } else {
            let n = state.config().target_partitions().max(1).min(aligned.len());
            let mut parts: Vec<Vec<RecordBatch>> = vec![Vec::new(); n];
            for (i, b) in aligned.into_iter().enumerate() {
                parts[i % n].push(b);
            }
            parts
        };
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
        use timelake_buffer::{TableBuffer, flush};
        use timelake_ingest::parse_lines;

        let t = 1_786_179_600_000_000_000i64;
        let lp: String = (0..2000)
            .map(|i| format!("m,pid=p{:05} v=1.0 {}\n", (i * 7919) % 2000, t + i))
            .collect();
        let mut buf = TableBuffer::default();
        for line in parse_lines(&lp, 1, 0).unwrap() {
            buf.append(&line, None).unwrap();
        }
        let parts = flush::prepare_ordered(&buf.snapshot().unwrap(), Some("pid")).unwrap();
        let bytes = flush::to_parquet_bytes_rg(&parts[0].1, Some(256)).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
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

    /// An in-memory store that remembers how many bytes it handed out.
    struct CountingStore {
        objects: std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>,
        bytes_read: std::sync::atomic::AtomicU64,
    }

    impl CountingStore {
        fn with(path: &str, bytes: Vec<u8>) -> Arc<CountingStore> {
            let mut m = std::collections::HashMap::new();
            m.insert(path.to_string(), bytes);
            Arc::new(CountingStore {
                objects: std::sync::Mutex::new(m),
                bytes_read: std::sync::atomic::AtomicU64::new(0),
            })
        }
        fn read(&self) -> u64 {
            self.bytes_read.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    impl Store for CountingStore {
        fn put(&self, path: &str, bytes: &[u8]) -> std::io::Result<()> {
            self.objects
                .lock()
                .unwrap()
                .insert(path.to_string(), bytes.to_vec());
            Ok(())
        }
        fn put_if_absent(&self, path: &str, bytes: &[u8]) -> std::io::Result<bool> {
            let mut m = self.objects.lock().unwrap();
            if m.contains_key(path) {
                return Ok(false);
            }
            m.insert(path.to_string(), bytes.to_vec());
            Ok(true)
        }
        fn get(&self, path: &str) -> std::io::Result<Vec<u8>> {
            let m = self.objects.lock().unwrap();
            let v = m
                .get(path)
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, path))?;
            self.bytes_read
                .fetch_add(v.len() as u64, std::sync::atomic::Ordering::Relaxed);
            Ok(v.clone())
        }
        fn delete(&self, path: &str) -> std::io::Result<()> {
            self.objects.lock().unwrap().remove(path);
            Ok(())
        }
        fn list(&self, prefix: &str) -> std::io::Result<Vec<String>> {
            let m = self.objects.lock().unwrap();
            let mut out: Vec<String> = m
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect();
            out.sort();
            Ok(out)
        }
        fn size(&self, path: &str) -> std::io::Result<u64> {
            let m = self.objects.lock().unwrap();
            m.get(path)
                .map(|v| v.len() as u64)
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, path))
        }
        fn get_range(&self, path: &str, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
            let m = self.objects.lock().unwrap();
            let v = m
                .get(path)
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, path))?;
            let start = (offset as usize).min(v.len());
            let end = start.saturating_add(len).min(v.len());
            self.bytes_read
                .fetch_add((end - start) as u64, std::sync::atomic::Ordering::Relaxed);
            Ok(v[start..end].to_vec())
        }
    }

    #[test]
    fn a_pruned_scan_reads_ranges_not_the_whole_file() {
        use timelake_buffer::{TableBuffer, flush};
        use timelake_ingest::parse_lines;

        // one entity-clustered file, small row groups
        let t = 1_786_179_600_000_000_000i64;
        let lp: String = (0..20_000)
            .map(|i| format!("m,pid=p{:05} v=1.0 {}\n", (i * 7919) % 20_000, t + i))
            .collect();
        let mut buf = TableBuffer::default();
        for line in parse_lines(&lp, 1, 0).unwrap() {
            buf.append(&line, None).unwrap();
        }
        let parts = flush::prepare_ordered(&buf.snapshot().unwrap(), Some("pid")).unwrap();
        let bytes = flush::to_parquet_bytes_rg(&parts[0].1, Some(1024)).unwrap();
        let file_len = bytes.len() as u64;

        let path = "poc/m/data/2026080809/f.parquet";
        let store = CountingStore::with(path, bytes);
        let store_dyn: Arc<dyn Store> = store.clone();
        let meta = FileMeta {
            db: "poc".into(),
            table: "m".into(),
            partition: "2026080809".into(),
            path: path.into(),
            rows: 20_000,
            size_bytes: file_len,
            min_ts_ns: t,
            max_ts_ns: t + 20_000,
        };

        let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> =
            Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        let pruning = Pruning {
            tag_equals: vec![("pid".into(), "p00042".into())],
            ..Default::default()
        };
        let (batches, _res) = load_pruned(
            &[],
            std::slice::from_ref(&meta),
            &store_dyn,
            &pruning,
            None,
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            &pool,
            "m",
            &Arc::new(MetaCache::default()),
            &Arc::new(ScanStats::default()),
        )
        .expect("scan");

        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "the row filter still returns exactly the entity");

        // The point of the exercise: a single-entity lookup must not pull
        // the whole object. It used to read 100% of every candidate file,
        // which is why row-group pruning bought nothing.
        let read = store.read();
        assert!(
            read < file_len / 2,
            "read {read} of {file_len} bytes — pruning must not fetch the whole file"
        );
        // measured at ~8% of the file when this landed
    }

    #[test]
    fn a_bloom_miss_skips_the_file_without_reading_it() {
        use timelake_buffer::{TableBuffer, flush};
        use timelake_ingest::parse_lines;

        // unclustered, one row group — the shape of a fresh L0 file, where
        // row-group statistics span every entity and prune nothing
        let t = 1_786_179_600_000_000_000i64;
        let lp: String = (0..20_000)
            .map(|i| format!("m,pid=p{:05} v=1.0 {}\n", i, t + i))
            .collect();
        let mut buf = TableBuffer::default();
        for line in parse_lines(&lp, 1, 0).unwrap() {
            buf.append(&line, None).unwrap();
        }
        let parts = flush::prepare(&buf.snapshot().unwrap()).unwrap();
        let bytes = flush::to_parquet_bytes(&parts[0].1).unwrap();
        let file_len = bytes.len() as u64;

        let path = "poc/m/data/2026080809/f.parquet";
        let store = CountingStore::with(path, bytes);
        let store_dyn: Arc<dyn Store> = store.clone();
        let meta = FileMeta {
            db: "poc".into(),
            table: "m".into(),
            partition: "2026080809".into(),
            path: path.into(),
            rows: 20_000,
            size_bytes: file_len,
            min_ts_ns: t,
            max_ts_ns: t + 20_000,
        };
        let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> =
            Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        let scan = |pid: &str| {
            let pruning = Pruning {
                tag_equals: vec![("pid".into(), pid.into())],
                ..Default::default()
            };
            load_pruned(
                &[],
                std::slice::from_ref(&meta),
                &store_dyn,
                &pruning,
                None,
                std::time::Instant::now() + std::time::Duration::from_secs(60),
                &pool,
                "m",
                &Arc::new(MetaCache::default()),
                &Arc::new(ScanStats::default()),
            )
            .expect("scan")
        };

        // an entity that is not in the file: the bloom says so, and the
        // data is never touched
        let (batches, _r) = scan("definitely-absent");
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
        let missed = store.read();
        assert!(
            missed < file_len / 4,
            "bloom miss read {missed} of {file_len} bytes — it should skip the data entirely"
        );

        // and one that IS present still returns its row
        let (batches, _r) = scan("p00042");
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    #[test]
    fn scan_stats_attribute_pruning_and_prove_l0_blooms_work() {
        // #69 characterisation: an UNCLUSTERED, L0-shaped file (pids
        // scattered across time, not entity-sorted) with SMALL row groups
        // and blooms. The M4 premise was that L0 cannot prune by entity; this
        // shows it can — the bloom excludes the groups that don't hold the
        // pid — and that the ScanStats counters attribute it correctly. The
        // real L0 flush uses COARSE row groups, which is the actual gap #70
        // is about, not a missing bloom.
        use std::sync::atomic::Ordering::Relaxed;
        use timelake_buffer::{TableBuffer, flush};
        use timelake_ingest::parse_lines;

        let t = 1_786_179_600_000_000_000i64;
        // i -> (i*7919)%20000 is a bijection (7919 prime, coprime to 20000),
        // so each pid appears exactly once, scattered across time.
        let lp: String = (0..20_000)
            .map(|i| format!("m,pid=p{:05} v=1.0 {}\n", (i * 7919) % 20_000, t + i))
            .collect();
        let mut buf = TableBuffer::default();
        for line in parse_lines(&lp, 1, 0).unwrap() {
            buf.append(&line, None).unwrap();
        }
        let parts = flush::prepare(&buf.snapshot().unwrap()).unwrap();
        // small row groups (256) so pruning is at a fine grain and visible in
        // the counters; this is exactly the lever #70 would pull for L0.
        let bytes = flush::to_parquet_bytes_rg(&parts[0].1, Some(256)).unwrap();
        let file_len = bytes.len() as u64;

        let path = "poc/m/data/2026080809/f.parquet";
        let store: Arc<dyn Store> = CountingStore::with(path, bytes);
        let meta = FileMeta {
            db: "poc".into(),
            table: "m".into(),
            partition: "2026080809".into(),
            path: path.into(),
            rows: 20_000,
            size_bytes: file_len,
            min_ts_ns: t,
            max_ts_ns: t + 20_000,
        };
        let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> =
            Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        let cache = Arc::new(MetaCache::default());
        let stats = Arc::new(ScanStats::default());
        let scan = |pid: &str| {
            load_pruned(
                &[],
                std::slice::from_ref(&meta),
                &store,
                &Pruning {
                    tag_equals: vec![("pid".into(), pid.into())],
                    ..Default::default()
                },
                None,
                std::time::Instant::now() + std::time::Duration::from_secs(60),
                &pool,
                "m",
                &cache,
                &stats,
            )
            .expect("scan");
        };

        // A pid that IS present sits in exactly one row group; the bloom
        // prunes all the others.
        scan("p00042");
        let considered = stats.row_groups_considered.load(Relaxed);
        assert!(
            considered > 10,
            "small row groups: expected many, got {considered}"
        );
        assert_eq!(stats.files_considered.load(Relaxed), 1);
        assert_eq!(stats.files_time_pruned.load(Relaxed), 0);
        assert_eq!(
            stats.meta_cache_misses.load(Relaxed),
            1,
            "first read is cold"
        );
        assert_eq!(stats.meta_cache_hits.load(Relaxed), 0);
        let scanned = stats.row_groups_scanned.load(Relaxed);
        let bloom = stats.row_groups_bloom_pruned.load(Relaxed);
        let by_stats = stats.row_groups_stats_pruned.load(Relaxed);
        // The point: pruning collapses ~78 groups to a handful, and the BLOOM
        // does real work on unclustered data — the mechanism M4 thought was
        // missing. (Stats prune some too: a group whose scattered pids all
        // fall on one side of the target excludes it by min/max — correct, not
        // a bug, so we do not require it to be zero.)
        assert!(
            scanned >= 1 && scanned < considered,
            "pruning should scan few of {considered}, scanned {scanned}"
        );
        assert!(
            bloom > 0,
            "blooms must exclude some L0 groups (the #69 point)"
        );
        // the arithmetic ties out: considered = stats + bloom + scanned
        assert_eq!(considered, by_stats + bloom + scanned);

        // A second scan warms the footer cache: a hit, no new miss.
        scan("p00007");
        assert_eq!(
            stats.meta_cache_hits.load(Relaxed),
            1,
            "second read is a cache hit"
        );
        assert_eq!(stats.meta_cache_misses.load(Relaxed), 1, "no new cold read");

        // An ABSENT pid: every row group bloom-pruned, nothing scanned.
        let before = stats.row_groups_scanned.load(Relaxed);
        scan("p99999");
        assert_eq!(
            stats.row_groups_scanned.load(Relaxed),
            before,
            "an absent pid must scan no row groups"
        );
    }

    #[test]
    fn finer_l0_row_groups_read_far_less_for_a_present_entity() {
        // #70: the fix. A PRESENT entity is the case fine groups help — the
        // bloom can't skip a group that holds it, so with coarse groups the
        // lookup decodes a huge group for a few rows; with fine groups it
        // decodes a small one. Same unclustered L0 data, written both ways;
        // measure the bytes each lookup pulls from the store.
        use timelake_buffer::{TableBuffer, flush};
        use timelake_ingest::parse_lines;

        let t = 1_786_179_600_000_000_000i64;
        let lp: String = (0..20_000)
            .map(|i| format!("m,pid=p{:05} v=1.0 {}\n", (i * 7919) % 20_000, t + i))
            .collect();
        let mut buf = TableBuffer::default();
        for line in parse_lines(&lp, 1, 0).unwrap() {
            buf.append(&line, None).unwrap();
        }
        let snap = buf.snapshot().unwrap();
        let part = &flush::prepare(&snap).unwrap()[0].1;

        let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> =
            Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        // bytes one present-entity lookup pulls from a file written with the
        // given row-group size.
        let bytes_read = |rg: Option<usize>| -> u64 {
            let bytes = flush::to_parquet_bytes_rg(part, rg).unwrap();
            let path = "poc/m/data/2026080809/f.parquet";
            let store = CountingStore::with(path, bytes.clone());
            let store_dyn: Arc<dyn Store> = store.clone();
            let meta = FileMeta {
                db: "poc".into(),
                table: "m".into(),
                partition: "2026080809".into(),
                path: path.into(),
                rows: 20_000,
                size_bytes: bytes.len() as u64,
                min_ts_ns: t,
                max_ts_ns: t + 20_000,
            };
            let (batches, _r) = load_pruned(
                &[],
                std::slice::from_ref(&meta),
                &store_dyn,
                &Pruning {
                    tag_equals: vec![("pid".into(), "p10000".into())],
                    ..Default::default()
                },
                None,
                std::time::Instant::now() + std::time::Duration::from_secs(60),
                &pool,
                "m",
                &Arc::new(MetaCache::default()),
                &Arc::new(ScanStats::default()),
            )
            .expect("scan");
            // correctness holds either way: the entity's one row comes back.
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
            store.read()
        };

        let coarse = bytes_read(None); // writer default: one big group
        let fine = bytes_read(Some(256)); // fine groups, like settled files
        // >2x less even here, where the 20K-row file is small enough that
        // fine's per-group metadata is a big fraction; at full scale a coarse
        // group is megabytes and the gap widens sharply.
        assert!(
            fine * 2 < coarse,
            "fine L0 groups should read far less for a present entity: fine {fine} vs coarse {coarse}"
        );
    }

    /// The SEC-2 acceptance shape: labels written through the normal
    /// ingest path, flushed to Parquet, scanned by a real DataFusion
    /// session — and COUNT(*) (the empty-projection fast path that never
    /// asks for the label column) still cannot count a hidden row.
    #[tokio::test]
    async fn scan_enforces_visibility_even_for_count_star() {
        use datafusion::prelude::SessionContext;
        use timelake_buffer::{TableBuffer, flush};
        use timelake_ingest::parse_lines;

        let t = 1_786_179_600_000_000_000i64;
        let lp: String = (0..100)
            .map(|i| {
                if i % 2 == 0 {
                    format!("m,pid=p{i:03},_visibility=secret v=1.0 {}\n", t + i)
                } else {
                    format!("m,pid=p{i:03} v=1.0 {}\n", t + i)
                }
            })
            .collect();
        let mut buf = TableBuffer::default();
        for line in parse_lines(&lp, 1, 0).unwrap() {
            buf.append(&line, None).unwrap();
        }
        let snapshot = buf.snapshot().unwrap();
        let parts = flush::prepare(&snapshot).unwrap();
        let bytes = flush::to_parquet_bytes(&parts[0].1).unwrap();
        let file_len = bytes.len() as u64;
        let path = "poc/m/data/2026080809/f.parquet";
        let store = CountingStore::with(path, bytes);
        let meta = FileMeta {
            db: "poc".into(),
            table: "m".into(),
            partition: "2026080809".into(),
            path: path.into(),
            rows: 100,
            size_bytes: file_len,
            min_ts_ns: t,
            max_ts_ns: t + 100,
        };

        let run = |auths: Vec<&str>, sql: &'static str| {
            let store_dyn: Arc<dyn Store> = store.clone();
            let meta = meta.clone();
            let schema = snapshot.schema();
            let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let session = crate::QuerySession::with_authorizations(
                auths.iter().map(|s| s.to_string()).collect(),
            );
            let c = counter.clone();
            async move {
                let table = LazyTable::new(
                    "m".into(),
                    schema,
                    Vec::new(),
                    vec![meta],
                    store_dyn,
                    std::time::Duration::from_secs(60),
                    Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default()),
                    Arc::new(MetaCache::default()),
                    session,
                    c,
                    Arc::new(ScanStats::default()),
                );
                let ctx = SessionContext::new();
                ctx.register_table("m", Arc::new(table)).unwrap();
                let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
                (batches, counter.load(std::sync::atomic::Ordering::Relaxed))
            }
        };

        // COUNT(*): the empty projection must still see (and obey) labels
        let (batches, dropped) = run(vec![], "SELECT COUNT(*) AS n FROM m").await;
        let n = batches[0]
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 50, "unauthorized COUNT(*) must not count hidden rows");
        assert_eq!(dropped, 50, "the filtered-rows counter records enforcement");

        let (batches, _) = run(vec!["secret"], "SELECT COUNT(*) AS n FROM m").await;
        let n = batches[0]
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 100, "authorized session sees everything");

        // a projection that never mentions the label column: rows are
        // filtered, and the rode-along column does not leak into results
        let (batches, _) = run(vec![], "SELECT pid FROM m ORDER BY pid").await;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 50);
        for b in &batches {
            assert!(b.column_by_name("_visibility").is_none());
        }
    }

    /// One partition per batch gave every operator above the scan exactly
    /// one batch to work with, which is what stopped DataFusion ever acting
    /// on its own partial-aggregation measurement. Pin the packing: at most
    /// `target_partitions` partitions, every batch still present.
    #[tokio::test]
    async fn scan_packs_batches_into_target_partitions() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_plan::ExecutionPlanProperties;
        use datafusion::prelude::{SessionConfig, SessionContext};

        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let buffer: Vec<RecordBatch> = (0..20)
            .map(|i| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int64Array::from(vec![i as i64; 8]))],
                )
                .unwrap()
            })
            .collect();
        let store: Arc<dyn Store> = CountingStore::with("unused", Vec::new());
        let table = LazyTable::new(
            "m".into(),
            schema,
            buffer,
            Vec::new(),
            store,
            std::time::Duration::from_secs(60),
            Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default()),
            Arc::new(MetaCache::default()),
            crate::QuerySession::default(),
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
            Arc::new(ScanStats::default()),
        );

        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        let plan = table.scan(&ctx.state(), None, &[], None).await.unwrap();
        assert_eq!(
            plan.output_partitioning().partition_count(),
            4,
            "20 batches must pack into target_partitions, not 20 partitions"
        );

        // and nothing is dropped on the way in
        ctx.register_table("m", Arc::new(table)).unwrap();
        let batches = ctx
            .sql("SELECT COUNT(*) AS n FROM m")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let n = batches[0]
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 160, "every batch survives the packing");
    }

    /// The presented schema is what decides which group-values path
    /// DataFusion picks, so pin it: tags arrive as views, measurements and
    /// timestamps are untouched, and a scan's batches actually match.
    #[tokio::test]
    async fn tags_are_presented_as_views_for_the_group_by_fast_path() {
        use datafusion::arrow::datatypes::DataType;
        use datafusion::prelude::SessionContext;
        use timelake_buffer::{TableBuffer, flush};
        use timelake_ingest::parse_lines;

        let t = 1_786_179_600_000_000_000i64;
        let lp: String = (0..500)
            .map(|i| format!("m,pid=p{:03},step=s{} v=1.0 {}\n", i, i % 3, t + i as i64))
            .collect();
        let mut buf = TableBuffer::default();
        for line in parse_lines(&lp, 1, 0).unwrap() {
            buf.append(&line, None).unwrap();
        }
        let snapshot = buf.snapshot().unwrap();
        // the stored schema is still dictionary-encoded — FR-2 is a
        // property of the file, not of what the planner is shown
        assert!(
            snapshot
                .schema()
                .fields()
                .iter()
                .any(|f| matches!(f.data_type(), DataType::Dictionary(_, _))),
        );
        let parts = flush::prepare(&snapshot).unwrap();
        let bytes = flush::to_parquet_bytes(&parts[0].1).unwrap();
        let file_len = bytes.len() as u64;
        let path = "poc/m/data/2026080809/f.parquet";
        let store: Arc<dyn Store> = CountingStore::with(path, bytes);
        let meta = FileMeta {
            db: "poc".into(),
            table: "m".into(),
            partition: "2026080809".into(),
            path: path.into(),
            rows: 500,
            size_bytes: file_len,
            min_ts_ns: t,
            max_ts_ns: t + 500,
        };

        // a BUFFER batch as well as a file: both must reach the plan as
        // views, by different routes (worker-thread conversion vs align_to)
        let table = LazyTable::new(
            "m".into(),
            snapshot.schema(),
            vec![snapshot.clone()],
            vec![meta],
            store,
            std::time::Duration::from_secs(60),
            Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default()),
            Arc::new(MetaCache::default()),
            crate::QuerySession::default(),
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
            Arc::new(ScanStats::default()),
        );
        for f in table.schema().fields() {
            let expected = match f.name().as_str() {
                "pid" | "step" => DataType::Utf8View,
                "v" => DataType::Float64,
                _ => f.data_type().clone(),
            };
            assert_eq!(f.data_type(), &expected, "field {}", f.name());
        }

        let ctx = SessionContext::new();
        ctx.register_table("m", Arc::new(table)).unwrap();
        // the shape that matters: GROUP BY over two tag columns, with the
        // tag-equality pruning path exercised by the WHERE clause
        let batches = ctx
            .sql("SELECT step, COUNT(DISTINCT pid) AS n FROM m WHERE pid = 'p001' GROUP BY step")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "pruning must not lose the row it is looking for");
        let n = batches[0]
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 1);
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
        assert_eq!(
            p.tag_equals,
            vec![("product_id".to_string(), "p1".to_string())]
        );
    }
}
