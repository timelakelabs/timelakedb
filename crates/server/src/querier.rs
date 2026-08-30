//! The querier role (C2 phase 4, CL-3) — the read half of the cluster.
//!
//! A querier owns no data. It replays the catalog from the shared object
//! store, tails the manifest log, and answers SQL and Flight SQL. Killing
//! one loses nothing; adding one adds read capacity. That is the whole
//! point of the role: reads scale and fail independently of writes.
//!
//! **Freshness is not optional.** AT-2 demands exact counts seconds after
//! ingest, and seconds after ingest the rows are still in an ingester's
//! memory buffer — not in any Parquet file, not in the catalog. So a
//! querier's table is the union of *files from the shared store* and *live
//! buffer snapshots pulled from every ingester*, exactly as a single node
//! unions its own buffer with its own files. This is the IOx-proven shape,
//! forced here by the harness rather than chosen by taste.
//!
//! **A partial answer is worse than no answer.** If an ingester cannot be
//! reached, its live rows are missing and every COUNT is silently short.
//! The querier therefore *refuses the query* with a named error rather than
//! returning a plausible wrong number. That is the opposite of the write
//! path's PR-7 trade (where availability outranks the second replica) and
//! deliberately so: a write that degrades is still honest about what it
//! stored, whereas a query that degrades lies.
//!
//! **How the read stays consistent with a flush.** The querier's catalog
//! view lags the ingesters' by up to one poll interval, which would open a
//! window where rows have left an ingester's buffer (flushed) but have not
//! yet appeared in the querier's catalog — a *vanish*, the one failure a
//! count-exactness harness cannot tolerate. It is closed with a watermark:
//! every internal response carries the ingester's own catalog head
//! (`x-timelake-catalog-head`), and the querier folds the manifest log
//! forward until its head reaches the highest one it saw *before* reading
//! any file list. Because an ingester clears a batch from `flushing` only
//! after its commit, any batch missing from a snapshot is guaranteed to be
//! below that watermark, hence visible. The remaining race is the same one
//! the single-node path already accepts: a transient *duplicate*, never a
//! vanish. In the steady state the watermark costs zero extra store calls —
//! the head arrives free on a request the querier was making anyway.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use arc_swap::ArcSwap;
use timelake_query::QueryBatch;

/// The header every internal read response carries: the serving node's
/// applied manifest sequence. See the module note — this is the freshness
/// watermark, not a diagnostic.
pub const CATALOG_HEAD_HEADER: &str = "x-timelake-catalog-head";

/// One ingester this querier reads live rows from.
struct IngesterPeer {
    id: String,
    live_url: String,
    snapshot_url: String,
}

/// Counters surfaced on `/metrics`.
#[derive(Default)]
pub struct QuerierStats {
    /// Successful snapshot fetches (one per table per ingester per query).
    pub snapshot_fetches: AtomicU64,
    /// Live rows pulled over the wire.
    pub snapshot_rows: AtomicU64,
    /// Failed snapshot/live fetches.
    pub snapshot_errors: AtomicU64,
    /// Queries refused rather than answered from an incomplete cluster.
    /// **Alert on this**: it is the metric that distinguishes "the cluster
    /// is down" from "the cluster is quietly under-counting".
    pub refusals: AtomicU64,
    /// Times the catalog was folded forward to reach an ingester's head.
    pub catchups: AtomicU64,
}

/// One table an ingester currently holds live rows for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveTable {
    pub db: String,
    pub table: String,
    pub rows: u64,
}

/// What one ingester reports it is holding, plus its catalog watermark.
#[derive(Debug, Clone)]
pub struct LiveReport {
    pub head: u64,
    pub tables: Vec<LiveTable>,
}

/// Parse the `/internal/v1/live` body. Kept separate from the HTTP call so
/// the contract is unit-testable without a server.
pub fn parse_live(body: &[u8]) -> Result<LiveReport, String> {
    let v: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| format!("live report json: {e}"))?;
    let head = v.get("head").and_then(|h| h.as_u64()).unwrap_or(0);
    let mut tables = Vec::new();
    for t in v
        .get("tables")
        .and_then(|t| t.as_array())
        .into_iter()
        .flatten()
    {
        let (Some(db), Some(table)) = (
            t.get("db").and_then(|x| x.as_str()),
            t.get("table").and_then(|x| x.as_str()),
        ) else {
            return Err(format!("live report entry missing db/table: {t}"));
        };
        tables.push(LiveTable {
            db: db.to_string(),
            table: table.to_string(),
            rows: t.get("rows").and_then(|x| x.as_u64()).unwrap_or(0),
        });
    }
    Ok(LiveReport { head, tables })
}

/// The querier's view of the ingesters: who they are, what they are
/// currently holding, and the client that fetches it.
pub struct RemoteBuffers {
    /// The ingesters this querier reads. An `ArcSwap` so a live discovery
    /// refresh (#71) swaps the set atomically — a query never sees a torn
    /// list, and a joined ingester is unioned or a departed one dropped
    /// without a restart.
    peers: ArcSwap<Vec<IngesterPeer>>,
    /// http|https for the internal links, fixed at construction so a refresh
    /// rebuilds peer URLs with the same scheme.
    scheme: &'static str,
    client: reqwest::Client,
    /// `(db, table)` pairs some ingester holds live rows for. Refreshed by
    /// the tail loop so that a table written but never yet flushed is
    /// listable — without it, `SELECT` against a brand-new table would say
    /// "no such table" until the first flush.
    live: RwLock<BTreeSet<(String, String)>>,
    pub stats: QuerierStats,
}

/// Build the sorted peer list with its per-scheme internal URLs. One place, so
/// construction and a live refresh produce byte-identical `IngesterPeer`s.
fn build_peers(mut ingesters: Vec<(String, String)>, scheme: &str) -> Vec<IngesterPeer> {
    // Sorted so the batch order a query sees is the same on every querier and
    // every run — one less reason for two nodes to disagree about anything.
    ingesters.sort_by(|a, b| a.0.cmp(&b.0));
    ingesters
        .into_iter()
        .map(|(id, addr)| IngesterPeer {
            id,
            live_url: format!("{scheme}://{addr}/internal/v1/live"),
            snapshot_url: format!("{scheme}://{addr}/internal/v1/snapshot"),
        })
        .collect()
}

impl RemoteBuffers {
    /// `ingesters` is `(id, cluster_address)` — the *internal* listener,
    /// not the public data port: live snapshots travel the same private
    /// link as CL-2 replication and move behind required mTLS at C3.
    pub fn new(ingesters: Vec<(String, String)>) -> RemoteBuffers {
        Self::new_with_tls(ingesters, None)
    }

    /// As [`Self::new`], but presenting this node's certificate over https and
    /// trusting only the cluster CA when the cluster has TLS (#72 phase 2).
    /// `tls = None` is the plaintext path, unchanged.
    pub fn new_with_tls(
        ingesters: Vec<(String, String)>,
        tls: Option<&crate::peer_tls::PeerTls>,
    ) -> RemoteBuffers {
        let scheme = crate::peer_tls::peer_scheme(tls);
        let mut builder = reqwest::Client::builder()
            // A hung ingester must not hang the query: the deadline
            // turns it into a refusal, which is the honest outcome.
            .timeout(Duration::from_secs(30));
        if let Some(t) = tls {
            builder = t.apply_async(builder);
        }
        RemoteBuffers {
            peers: ArcSwap::from_pointee(build_peers(ingesters, scheme)),
            scheme,
            client: builder.build().expect("querier client"),
            live: RwLock::new(BTreeSet::new()),
            stats: QuerierStats::default(),
        }
    }

    /// Replace the ingester set from a live discovery refresh (#71). Sorted and
    /// URL-built exactly as construction does, so nothing downstream can tell a
    /// refreshed set from a booted one.
    pub fn update_peers(&self, ingesters: Vec<(String, String)>) {
        self.peers
            .store(Arc::new(build_peers(ingesters, self.scheme)));
    }

    pub fn peer_ids(&self) -> Vec<String> {
        self.peers.load().iter().map(|p| p.id.clone()).collect()
    }

    pub fn peer_count(&self) -> usize {
        self.peers.load().len()
    }

    /// Tables some ingester is currently holding live rows for.
    pub fn live_tables(&self) -> Vec<(String, String)> {
        self.live
            .read()
            .expect("live lock")
            .iter()
            .cloned()
            .collect()
    }

    /// Poll every ingester for what it is holding. Returns the highest
    /// catalog head seen, so the caller can fold the manifest log forward.
    ///
    /// On the **query path**, not just the tail loop: a table written a
    /// moment ago lives only in an ingester's memory, and a querier that
    /// listed tables from a one-second-old view would answer "table not
    /// found" for it. One round trip per ingester, in parallel, is the
    /// price of the freshness claim being true at the moment of the write
    /// rather than a tick later.
    ///
    /// A peer that fails to answer does **not** clear the table list: the
    /// list only shrinks when every peer has been heard from. Listing a
    /// table that no longer has live rows is harmless (it resolves to its
    /// files); dropping one that does have them would make it vanish from
    /// `SHOW TABLES` mid-outage.
    pub async fn refresh_live(&self) -> u64 {
        let mut set = tokio::task::JoinSet::new();
        let peers = self.peers.load_full();
        for p in peers.iter() {
            let client = self.client.clone();
            let url = p.live_url.clone();
            let id = p.id.clone();
            set.spawn(async move {
                let out = async {
                    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
                    if !resp.status().is_success() {
                        return Err(format!("HTTP {}", resp.status().as_u16()));
                    }
                    let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
                    parse_live(&bytes)
                }
                .await;
                (id, out)
            });
        }

        let mut head = 0u64;
        let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
        let mut all_answered = true;
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((_, Ok(report))) => {
                    head = head.max(report.head);
                    for t in report.tables {
                        seen.insert((t.db, t.table));
                    }
                }
                Ok((id, Err(e))) => {
                    all_answered = false;
                    self.stats.snapshot_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(ingester = %id, error = %e, "live-table poll failed");
                }
                Err(join) => {
                    all_answered = false;
                    tracing::warn!(error = %join, "live-table poll task failed");
                }
            }
        }

        let mut live = self.live.write().expect("live lock");
        if all_answered {
            *live = seen;
        } else {
            live.extend(seen);
        }
        head
    }

    /// Fetch one table's live rows from every ingester.
    ///
    /// All or nothing: one unreachable ingester fails the whole call, and
    /// the caller turns that into a refused query. Under sharding a table
    /// lives on exactly one ingester, so most of these return empty — the
    /// fan-out is what keeps that fact out of the read path, where a stale
    /// idea of who owns a table would silently drop rows.
    pub async fn snapshot(&self, db: &str, table: &str) -> Result<(Vec<QueryBatch>, u64), String> {
        let mut set = tokio::task::JoinSet::new();
        let peers = self.peers.load_full();
        for p in peers.iter() {
            let client = self.client.clone();
            let url = p.snapshot_url.clone();
            let id = p.id.clone();
            let db = db.to_string();
            let table = table.to_string();
            set.spawn(async move {
                let attempt = async |client: &reqwest::Client| {
                    let resp = client
                        .get(&url)
                        .query(&[("db", db.as_str()), ("table", table.as_str())])
                        .send()
                        .await
                        .map_err(|e| e.to_string())?;
                    if !resp.status().is_success() {
                        return Err(format!("HTTP {}", resp.status().as_u16()));
                    }
                    let head: u64 = resp
                        .headers()
                        .get(CATALOG_HEAD_HEADER)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);
                    let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
                    let batches = timelake_query::ipc::from_ipc(&bytes)?;
                    Ok((batches, head))
                };
                // One retry. An ingester that has just restarted leaves
                // dead sockets in this client's connection pool, and the
                // first use of one fails instantly — a self-healing blip
                // that must not surface as "the cluster is incomplete".
                // Reading a snapshot is idempotent, so a retry is free of
                // consequence; a genuinely absent peer refuses the
                // connection immediately and still fails fast.
                let out = match attempt(&client).await {
                    Ok(v) => Ok(v),
                    Err(first) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        attempt(&client).await.map_err(|second| {
                            if first == second {
                                first
                            } else {
                                format!("{first}; on retry: {second}")
                            }
                        })
                    }
                };
                (id, out)
            });
        }

        // Collected then sorted by ingester id: batch order must not depend
        // on which node happened to answer first, or two runs of the same
        // query could order rows differently.
        let mut collected: Vec<(String, Vec<QueryBatch>)> =
            Vec::with_capacity(self.peers.load().len());
        let mut head = 0u64;
        let mut failure: Option<String> = None;
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((id, Ok((batches, h)))) => {
                    head = head.max(h);
                    self.stats.snapshot_fetches.fetch_add(1, Ordering::Relaxed);
                    self.stats.snapshot_rows.fetch_add(
                        batches.iter().map(|b| b.num_rows() as u64).sum(),
                        Ordering::Relaxed,
                    );
                    collected.push((id, batches));
                }
                Ok((id, Err(e))) => {
                    self.stats.snapshot_errors.fetch_add(1, Ordering::Relaxed);
                    failure.get_or_insert(format!("ingester {id}: {e}"));
                }
                Err(join) => {
                    self.stats.snapshot_errors.fetch_add(1, Ordering::Relaxed);
                    failure.get_or_insert(format!("snapshot task: {join}"));
                }
            }
        }
        if let Some(why) = failure {
            self.stats.refusals.fetch_add(1, Ordering::Relaxed);
            return Err(format!(
                "refusing to answer from an incomplete cluster: could not read live rows \
                 for '{table}' ({why}). A missing ingester means missing rows, and a \
                 short count is worse than an error."
            ));
        }

        collected.sort_by(|a, b| a.0.cmp(&b.0));
        Ok((collected.into_iter().flat_map(|(_, b)| b).collect(), head))
    }
}

/// Tail the cluster: poll the ingesters for what they hold live and fold
/// the shared manifest log forward. This keeps table *listing* and the
/// schema registry current between queries; per-query freshness is
/// guaranteed separately by the head watermark (see the module note), so a
/// slow or skipped tick can never make a query wrong — only a listing
/// briefly stale.
pub async fn tail(engine: Arc<crate::Engine>, remote: Arc<RemoteBuffers>, period: Duration) {
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut n: u64 = 0;
    loop {
        tick.tick().await;
        n += 1;
        let head = remote.refresh_live().await;
        let e = Arc::clone(&engine);
        // A querier runs no maintenance, so this loop is the only periodic
        // thing it has — and it authenticates reads, so a token revoked on
        // an ingester must stop working here too (#46). Every tenth tail
        // tick, so the cadence matches the ingesters' 10 s maintenance tick
        // rather than re-reading the token file once a second for nothing.
        let reload = n.is_multiple_of(10);
        if let Err(join) = tokio::task::spawn_blocking(move || {
            if reload {
                e.reload_tokens();
            }
            e.catch_up_catalog(head)
        })
        .await
        {
            tracing::error!(%join, "catalog tail task panicked");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_report_parses_head_and_tables() {
        let body = br#"{"head":42,"tables":[{"db":"poc","table":"cpu","rows":7},
                        {"db":"poc","table":"mem","rows":0}]}"#;
        let r = parse_live(body).unwrap();
        assert_eq!(r.head, 42);
        assert_eq!(r.tables.len(), 2);
        assert_eq!(r.tables[0].db, "poc");
        assert_eq!(r.tables[0].table, "cpu");
        assert_eq!(r.tables[0].rows, 7);
    }

    #[test]
    fn an_ingester_holding_nothing_is_a_valid_report() {
        let r = parse_live(br#"{"head":0,"tables":[]}"#).unwrap();
        assert_eq!(r.head, 0);
        assert!(r.tables.is_empty());
    }

    #[test]
    fn a_malformed_entry_is_an_error_not_a_silently_dropped_table() {
        // Dropping it would make the table unlistable with no signal at all.
        assert!(parse_live(br#"{"head":1,"tables":[{"db":"poc"}]}"#).is_err());
        assert!(parse_live(b"not json").is_err());
    }

    #[test]
    fn peers_are_sorted_so_batch_order_is_stable_everywhere() {
        let a = RemoteBuffers::new(vec![
            ("ing-b".into(), "b:1965".into()),
            ("ing-a".into(), "a:1965".into()),
        ]);
        let b = RemoteBuffers::new(vec![
            ("ing-a".into(), "a:1965".into()),
            ("ing-b".into(), "b:1965".into()),
        ]);
        assert_eq!(a.peer_ids(), b.peer_ids());
        assert_eq!(a.peer_ids(), vec!["ing-a", "ing-b"]);
        assert_eq!(a.peer_count(), 2);
    }

    #[test]
    fn update_peers_swaps_the_live_ingester_set() {
        // #71 phase 3: a discovery refresh replaces the ingester set the
        // querier fans out to, sorted the same way, without a restart.
        let r = RemoteBuffers::new(vec![("ing-a".into(), "a:1965".into())]);
        assert_eq!(r.peer_ids(), vec!["ing-a"]);
        r.update_peers(vec![
            ("ing-b".into(), "b:1965".into()),
            ("ing-a".into(), "a:1965".into()),
        ]);
        assert_eq!(
            r.peer_ids(),
            vec!["ing-a", "ing-b"],
            "a joined ingester is picked up, still sorted"
        );
        r.update_peers(vec![]);
        assert!(r.peer_ids().is_empty(), "all ingesters departed");
    }

    #[tokio::test]
    async fn an_unreachable_ingester_refuses_the_query_instead_of_under_counting() {
        // Port 1 on localhost: nothing listens, so the fetch fails fast.
        let r = RemoteBuffers::new(vec![("ing-dead".into(), "127.0.0.1:1".into())]);
        let err = r.snapshot("poc", "cpu").await.unwrap_err();
        assert!(
            err.contains("incomplete cluster") && err.contains("ing-dead"),
            "unhelpful refusal: {err}"
        );
        assert_eq!(r.stats.refusals.load(Ordering::Relaxed), 1);
        assert!(r.stats.snapshot_errors.load(Ordering::Relaxed) >= 1);
    }

    #[tokio::test]
    async fn a_failed_poll_keeps_the_known_tables_rather_than_forgetting_them() {
        let r = RemoteBuffers::new(vec![("ing-dead".into(), "127.0.0.1:1".into())]);
        r.live
            .write()
            .unwrap()
            .insert(("poc".to_string(), "cpu".to_string()));
        let head = r.refresh_live().await;
        assert_eq!(head, 0, "nothing answered, so no watermark");
        assert_eq!(
            r.live_tables(),
            vec![("poc".to_string(), "cpu".to_string())],
            "an outage must not un-list a table"
        );
    }
}
