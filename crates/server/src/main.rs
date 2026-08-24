use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    // System log. Unset TIMELAKE_LOG_FILE keeps the unchanged path: stdout,
    // which systemd and Docker capture and rotate for you. Set it and the
    // node writes to a file it rotates itself, by size and/or by time.
    // This is NOT the audit trail — that is hash-chained evidence with its
    // own rotation and its own retention floor.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    match timelake_server::logfile::from_env() {
        None => tracing_subscriber::fmt().with_env_filter(filter).init(),
        Some(log) => tracing_subscriber::fmt()
            .with_env_filter(filter)
            // No ANSI escapes in a file — they make a log grep-hostile.
            .with_ansi(false)
            .with_writer(timelake_server::logfile::LogSink(log))
            .init(),
    }

    // Cluster role + topology (CL-1/CL-5). `all` is the default and does
    // everything, as it always has. A role whose C2 phase has not landed is
    // refused here rather than started half-built.
    let role = match std::env::var("TIMELAKE_ROLE") {
        Ok(v) => timelake_cluster::Role::parse(&v).unwrap_or_else(|e| {
            eprintln!("{e}");
            std::process::exit(2);
        }),
        Err(_) => timelake_cluster::Role::All,
    };
    if !role.implemented() {
        eprintln!(
            "TIMELAKE_ROLE={} is not yet implemented — the cluster roles land \
             one C2 phase at a time (ARCHITECTURE §12.4). Use `all` (the \
             default) for a single-node deployment.",
            role.as_str()
        );
        std::process::exit(2);
    }
    let discovery = timelake_cluster::StaticDiscovery::from_env(role).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(2);
    });
    {
        use timelake_cluster::Discovery;
        let peers: Vec<String> = discovery
            .peers()
            .iter()
            .map(|n| format!("{}={}@{}", n.id, n.role.as_str(), n.address))
            .collect();
        tracing::info!(
            role = role.as_str(),
            node = %discovery.this_node().id,
            cluster_addr = %discovery.this_node().address,
            peers = %peers.join(","),
            "cluster role selected"
        );
    }

    let addr = std::env::var("TIMELAKE_ADDR").unwrap_or_else(|_| "0.0.0.0:1963".to_string());

    // The router (C2 phase 3) holds NO data — it is a stateless write
    // forwarder. It never opens an engine; it shards line protocol across the
    // ingesters from discovery and forwards. Runs here and returns.
    if role == timelake_cluster::Role::Router {
        use timelake_cluster::Discovery;
        let ingesters: Vec<(String, String)> = discovery
            .peers_with_role(timelake_cluster::Role::Ingester)
            .into_iter()
            .filter(|n| !n.data_address.is_empty())
            .map(|n| (n.id, n.data_address))
            .collect();
        if ingesters.is_empty() {
            eprintln!(
                "TIMELAKE_ROLE=router needs ingesters with public data addresses in \
                 TIMELAKE_PEERS (id=ingester@cluster_addr|data_addr)"
            );
            std::process::exit(2);
        }
        // Queriers are optional: a router with none still shards writes, and
        // says plainly that it cannot answer a query (CL-3).
        let queriers: Vec<(String, String)> = discovery
            .peers_with_role(timelake_cluster::Role::Querier)
            .into_iter()
            .filter(|n| !n.data_address.is_empty())
            .map(|n| (n.id, n.data_address))
            .collect();
        if queriers.is_empty() {
            tracing::warn!(
                "router has no queriers in TIMELAKE_PEERS — /api/sql will return 501 \
                 (a query must union every shard, so no ingester can answer it)"
            );
        }
        // The router opens no engine, so it reads the one engine setting it
        // shares with the ingesters — TIMELAKE_MAX_BODY_BYTES — from the
        // same parser the engine uses, rather than re-deriving a default
        // here that could drift (#36).
        let max_body_bytes = timelake_server::config_from_env().max_body_bytes;
        let state = Arc::new(
            timelake_server::router::RouterState::with_queriers(ingesters, queriers)
                .with_max_body_bytes(max_body_bytes),
        );
        tracing::info!(
            ingesters = ?state.target_ids(),
            queriers = ?state.querier_ids(),
            %addr,
            "router up (write sharding + query forwarding)"
        );
        let app = timelake_server::router::router_app(state);
        let listener = TcpListener::bind(&addr).await.expect("router bind");
        axum::serve(listener, app).await.expect("router serve");
        return;
    }

    let data_dir = timelake_server::data_dir_from_env();
    let cfg = timelake_server::config_from_env();

    tracing::info!(%addr, data_dir = %data_dir.display(), ?cfg, "timelakedb M3 starting");
    // Read before the config moves into the engine; the replicator is built
    // further down, once discovery has produced a peer.
    let repl_timeout_ms = cfg.repl_timeout_ms;
    let engine = timelake_server::Engine::open(&data_dir, cfg).expect("open engine (recovery)");

    // CL-2: an ingester replicates every write to its paired ingester before
    // the ack, and holds the peer's frames in a durable replica WAL. The
    // pairing comes from discovery (the other `ingester` node). `all` never
    // reaches here, so its write path stays unchanged.
    if role == timelake_cluster::Role::Ingester {
        use timelake_cluster::Discovery;
        engine
            .enable_replica_wal(&data_dir.join("replica-wal"))
            .expect("open replica WAL");
        match discovery
            .peers_with_role(timelake_cluster::Role::Ingester)
            .first()
        {
            Some(peer) => {
                tracing::info!(peer = %peer.id, addr = %peer.address, "CL2 replication peer");
                engine.set_replicator(timelake_server::replication::Replicator::new(
                    &peer.id,
                    &peer.address,
                    repl_timeout_ms,
                ));
            }
            None => tracing::warn!(
                "ingester has no peer in discovery (TIMELAKE_PEERS) — running WITHOUT \
                 replication; a single failure can lose acknowledged writes"
            ),
        }
        let cluster_addr = discovery.this_node().address.clone();
        if cluster_addr.is_empty() {
            eprintln!(
                "TIMELAKE_ROLE=ingester requires TIMELAKE_CLUSTER_ADDR (this node's \
                 internal replication listener, e.g. 0.0.0.0:1965)"
            );
            std::process::exit(2);
        }
        let internal = timelake_server::internal_router(Arc::clone(&engine));
        let listen = cluster_addr.clone();
        tokio::spawn(async move {
            let l = match TcpListener::bind(&listen).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!(addr = %listen, error = %e, "internal listener bind failed");
                    return;
                }
            };
            tracing::info!(addr = %listen, "CL2 internal replication listener up");
            if let Err(e) = axum::serve(l, internal).await {
                tracing::error!(error = %e, "internal replication listener exited");
            }
        });
    }

    // CL-3: the querier is a read replica. It takes no writes, runs no
    // maintenance (compaction and retention are the compactor's job and a
    // second one would be a second writer), and unions the ingesters' live
    // buffers into every query so freshness survives the role split.
    if role == timelake_cluster::Role::Querier {
        use timelake_cluster::Discovery;
        engine.set_read_only();
        let ingesters: Vec<(String, String)> = discovery
            .peers_with_role(timelake_cluster::Role::Ingester)
            .into_iter()
            .filter(|n| !n.address.is_empty())
            .map(|n| (n.id, n.address))
            .collect();
        if ingesters.is_empty() {
            eprintln!(
                "TIMELAKE_ROLE=querier needs the ingesters' internal addresses in \
                 TIMELAKE_PEERS (id=ingester@cluster_addr). Without them a query \
                 cannot see rows that have not been flushed yet, and would answer \
                 short."
            );
            std::process::exit(2);
        }
        let remote = Arc::new(timelake_server::querier::RemoteBuffers::new(ingesters));
        engine.set_remote_buffers(Arc::clone(&remote));
        tracing::info!(
            ingesters = ?remote.peer_ids(),
            catalog_head = engine.catalog_head(),
            "querier up (stateless reads: shared store + live buffers)"
        );
        let e = Arc::clone(&engine);
        tokio::spawn(timelake_server::querier::tail(
            e,
            remote,
            Duration::from_secs(1),
        ));
    }

    // The compactor (C2 phase 5a) does rewrite work and nothing else: no
    // writes, no queries, no buffer of its own. It runs here and returns,
    // like the router does.
    //
    // Two things it must do that `all` does not have to.
    //
    // It TAILS THE CATALOG. `compact_once` reads the in-memory file list,
    // which advances only on this node's own commits. A compactor commits
    // nothing until it compacts, so without tailing it would work forever
    // from the file list it booted with — choosing partitions that no
    // longer exist and merging files another node already replaced. The
    // commit fence catches that (it refuses a merge whose inputs are
    // gone), so the result would be safe; it would also be a node that
    // burns CPU and never lands anything.
    //
    // It is READ-ONLY for writes. It has no WAL of its own and no client
    // should be pointed at it, so a write arriving here is a
    // misconfiguration and should be refused loudly rather than half-
    // accepted into a buffer nothing will ever flush.
    if role == timelake_cluster::Role::Compactor {
        engine.set_read_only();
        // C2 phase 5b: find this compactor's place among its peers so it owns
        // a disjoint slice of partitions. Every compactor sorts the same id
        // list (itself plus the discovered compactor peers) and reads its own
        // index, so the assignment agrees across nodes with no coordination.
        let (ordinal, count) = {
            use timelake_cluster::Discovery;
            let self_id = discovery.this_node().id.clone();
            let mut ids: Vec<String> = discovery
                .peers_with_role(timelake_cluster::Role::Compactor)
                .into_iter()
                .map(|n| n.id)
                .collect();
            ids.push(self_id.clone());
            ids.sort();
            ids.dedup();
            let ordinal = ids.iter().position(|id| *id == self_id).unwrap_or(0);
            (ordinal, ids.len())
        };
        engine.set_compactor_shard(ordinal, count);
        tracing::info!(
            %addr,
            catalog_head = engine.catalog_head(),
            ordinal,
            compactors = count,
            "compactor up (maintenance only: no writes, no queries; owns 1/{count} of partitions)"
        );
        // Rollup materialisation (§18.6, the cluster half) reads the SAME
        // shard union a querier does, so a rollup over a source sharded across
        // ingesters aggregates every shard, not just the files one node can
        // see. Set up the live-buffer union from the ingesters in discovery
        // and keep it fresh the querier's way. With no ingesters (a lone
        // compactor beside an `all` node) there is no union to build — it
        // reads files only, which is complete for buckets old enough to have
        // flushed, and rollups only seal buckets that have aged past lookback.
        {
            use timelake_cluster::Discovery;
            let ingesters: Vec<(String, String)> = discovery
                .peers_with_role(timelake_cluster::Role::Ingester)
                .into_iter()
                .filter(|n| !n.address.is_empty())
                .map(|n| (n.id, n.address))
                .collect();
            if !ingesters.is_empty() {
                let remote = Arc::new(timelake_server::querier::RemoteBuffers::new(ingesters));
                engine.set_remote_buffers(Arc::clone(&remote));
                tracing::info!(
                    ingesters = ?remote.peer_ids(),
                    "compactor reads the shard union for rollup materialisation"
                );
                tokio::spawn(timelake_server::querier::tail(
                    Arc::clone(&engine),
                    remote,
                    Duration::from_secs(1),
                ));
            }
        }
        let worker = Arc::clone(&engine);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(30));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                // Pick up rollup definitions made on any node's console — the
                // compactor serves no admin surface and would otherwise seal
                // only what it seeded at boot (§18.6). A small store get, off
                // the async runtime.
                {
                    let e = Arc::clone(&worker);
                    let _ = tokio::task::spawn_blocking(move || e.reload_rollups()).await;
                }
                // Materialise the rollups this compactor owns first (§18.6).
                // It is a QUERY over the source union, so it is async and
                // lives outside the blocking closure; its fresh target rows
                // flush in the same tick's flush stage below.
                if let Err(err) = Arc::clone(&worker).materialize_rollups_once().await {
                    tracing::error!(%err, stage = "rollup", "compactor stage failed");
                }
                let e = Arc::clone(&worker);
                let res = tokio::task::spawn_blocking(move || {
                    // Tail first. Compacting a stale view is wasted work at
                    // best; the fence makes it harmless, not useful. (When an
                    // ingester union is up, the querier tail above already
                    // keeps the catalog fresh; this covers the no-union case.)
                    e.catch_up_catalog(0);
                    // Flush what materialisation just wrote, so queriers and
                    // the next pass's watermark see the sealed buckets — the
                    // compactor has no other writes, so this is cheap.
                    if let Err(err) = e.flush_all() {
                        tracing::error!(%err, stage = "flush", "compactor stage failed");
                    }
                    // Each stage independent, as in the `all` loop: one
                    // failing stage must not stop the others.
                    if let Err(err) = e.compact_once() {
                        tracing::error!(%err, stage = "compact", "compactor stage failed");
                    }
                    if let Err(err) = e.apply_tombstones_once() {
                        tracing::error!(%err, stage = "tombstones", "compactor stage failed");
                    }
                    if let Err(err) = e.enforce_retention() {
                        tracing::error!(%err, stage = "retention", "compactor stage failed");
                    }
                })
                .await;
                if let Err(join) = res {
                    tracing::error!(%join, "compactor tick panicked");
                }
            }
        });
        let listener = TcpListener::bind(&addr).await.expect("compactor bind");
        axum::serve(listener, timelake_server::compactor_app(engine))
            .await
            .expect("compactor serve");
        return;
    }

    // Maintenance ticks (ARCHITECTURE §7): flush every 10 s, compaction
    // every 30 s, retention every 60 s — sequential on one blocking task
    // so background work never stacks up on itself. A querier owns no
    // data and commits nothing, so it runs none of this.
    let maint = Arc::clone(&engine);
    // Rollups materialise HERE only on an `all` node. In a role-split cluster
    // the ingesters also run this tick, but materialisation belongs to the
    // compactor (§18.6): if an ingester materialised too, it and the compactor
    // would both seal the same bucket and — the watermark model not deduping —
    // double it. A cluster with no compactor simply does not downsample.
    let materialise_here = role == timelake_cluster::Role::All;
    if role != timelake_cluster::Role::Querier {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            let mut n: u64 = 0;
            loop {
                tick.tick().await;
                n += 1;
                let compact = n.is_multiple_of(3);
                let retention = n.is_multiple_of(6);
                // R-1b runs on the compaction cadence: physical delete is the
                // same kind of rewrite work and never on a query's critical
                // path, since R-1a already hides the rows at read time.
                let tombstones = n.is_multiple_of(3);
                // R-2 (§18.3): materialise BEFORE the flush+compact below, so
                // this pass's rollup rows flush in the same tick. It reads
                // through SQL (async), so it can't live in the blocking closure;
                // per-rollup failures are logged inside, never fatal. Gated to
                // the `all` node — the compactor owns this in a cluster.
                if materialise_here
                    && compact
                    && let Err(err) = Arc::clone(&maint).materialize_rollups_once().await
                {
                    tracing::error!(%err, stage = "rollup", "maintenance stage failed");
                }
                let e = Arc::clone(&maint);
                // Each stage is independent. A failing flush used to abort
                // the rest of the tick, so one unflushable table stopped
                // compaction and retention for every table on the node.
                let res = tokio::task::spawn_blocking(move || {
                    // U2: sample first, so the numbers stored describe the
                    // state the tick's other stages are about to change
                    // rather than the state they left behind. It never
                    // returns an error — a sample that cannot be stored is
                    // logged and dropped, because telemetry must not be
                    // able to fail maintenance.
                    e.selfmon_tick();
                    // #46: tokens issued or revoked on a peer sharing this
                    // store take effect here within one tick. Cheap — one
                    // small get, hashed before it is parsed.
                    e.reload_tokens();
                    // R-2: a rollup defined on a peer's console propagates the
                    // same way, so every node's /admin/rollups agrees and the
                    // compactor materialising it stays current (§18.6).
                    e.reload_rollups();
                    if let Err(err) = e.flush_if_needed() {
                        tracing::error!(%err, stage = "flush", "maintenance stage failed");
                    }
                    if compact && let Err(err) = e.compact_once() {
                        tracing::error!(%err, stage = "compact", "maintenance stage failed");
                    }
                    if tombstones && let Err(err) = e.apply_tombstones_once() {
                        tracing::error!(%err, stage = "tombstones", "maintenance stage failed");
                    }
                    if retention && let Err(err) = e.enforce_retention() {
                        tracing::error!(%err, stage = "retention", "maintenance stage failed");
                    }
                    e.run_gc();
                })
                .await;
                if let Err(join) = res {
                    tracing::error!(%join, "maintenance task panicked");
                }
            }
        });
    }

    // Flight SQL (FR-8) on its own gRPC port
    let flight_addr: std::net::SocketAddr = std::env::var("TIMELAKE_FLIGHT_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:1964".to_string())
        .parse()
        .expect("TIMELAKE_FLIGHT_ADDR must be host:port");
    let flight_backend: Arc<dyn timelake_flight::SqlBackend> = engine.clone();

    // SEC-3: TLS on BOTH listeners when cert+key are configured; the
    // fixtures and bench stay plaintext by simply not setting these.
    let tls_cert = std::env::var("TIMELAKE_TLS_CERT").ok();
    let tls_key = std::env::var("TIMELAKE_TLS_KEY").ok();
    match (tls_cert, tls_key) {
        (Some(cert), Some(key)) => {
            let rot = timelake_tls::RotatingCert::load(cert.as_ref(), key.as_ref())
                .expect("initial TLS cert load must succeed (no last-good yet)");
            engine.set_tls(Arc::clone(&rot));

            // SEC-3 want mode: with a client CA bundle configured the
            // listeners REQUEST a client certificate and identify anyone
            // who presents one — while still serving anyone who does not,
            // so Grafana, Telegraf and the bench harness are unaffected.
            let client_ca = std::env::var("TIMELAKE_TLS_CLIENT_CA").ok().map(|p| {
                timelake_tls::RotatingClientCa::load(p.as_ref())
                    .expect("initial client CA bundle must load")
            });
            // The anonymous/authenticated split (see Engine::metrics):
            // one counter, shared by the accept loop that increments it
            // and the metrics handler that reports it.
            let auth_counts = Arc::new(timelake_flight::ClientAuthCounts::default());
            engine.set_client_auth_counts(Arc::clone(&auth_counts));
            if let Some(ca) = &client_ca {
                engine.set_client_ca(Arc::clone(ca));
            }
            // Floor is TLS 1.3; TIMELAKE_TLS_MIN=1.2 lowers it (SEC-3).
            let allow_tls12 = std::env::var("TIMELAKE_TLS_MIN").as_deref() == Ok("1.2");
            tracing::info!(
                expires_in_secs = rot.expires_in_secs(),
                min_version = if allow_tls12 { "1.2" } else { "1.3" },
                "TLS enabled on HTTP and Flight SQL listeners"
            );

            // File watcher: certbot-style renewals just overwrite the
            // files; poll mtimes (2 s), debounce, reload. A failed reload
            // alarms and keeps last-good — it must NOT stop the watcher.
            let ca_watcher = client_ca.clone();
            let watcher = Arc::clone(&rot);
            tokio::spawn(async move {
                let mut last = watcher.mtimes();
                let mut tick = tokio::time::interval(Duration::from_secs(2));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    let now = watcher.mtimes();
                    if now.is_some() && now != last {
                        // Debounce: cert and key are two files; let the
                        // writer finish both before validating the pair.
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        let settled = watcher.mtimes();
                        let w = Arc::clone(&watcher);
                        let _ = tokio::task::spawn_blocking(move || w.reload()).await;
                        // Roll the trust anchors on the same trigger:
                        // dual-CA overlap means the bundle changes during
                        // a CA roll while clients keep connecting.
                        if let Some(ca) = ca_watcher.clone() {
                            let _ = tokio::task::spawn_blocking(move || ca.reload()).await;
                        }
                        last = settled;
                    } else {
                        last = now;
                    }
                }
            });

            // Flight SQL over TLS (gRPC wants ALPN h2).
            let flight_tls = rot.server_config_with_client_ca(
                allow_tls12,
                &[b"h2".as_slice()],
                client_ca.clone(),
            );
            tokio::spawn(async move {
                if let Err(e) =
                    timelake_flight::serve_tls(flight_backend, flight_addr, flight_tls, auth_counts)
                        .await
                {
                    tracing::error!(error = %e, "flight sql (TLS) server exited");
                }
            });

            // HTTP over TLS. axum-server drives hyper over our rustls
            // config; the resolver inside it is the rotation point.
            let http_tls = rot.server_config_with_client_ca(
                allow_tls12,
                &[b"h2".as_slice(), b"http/1.1"],
                client_ca.clone(),
            );
            let sock_addr: std::net::SocketAddr = addr
                .parse()
                .expect("TIMELAKE_ADDR must be host:port under TLS");
            let app = timelake_server::app_with_tls_admin(engine, rot);
            // SEC-3 v2 on HTTP: wrap the rustls acceptor so a verified
            // client certificate's CN reaches the handlers as a request
            // extension. Without this the connection is mutually
            // authenticated and `/api/sql` cannot tell, so the certificate
            // authorizes nothing — which is what it did until now.
            let acceptor = timelake_server::tls_identity::IdentityAcceptor::new(
                axum_server::tls_rustls::RustlsAcceptor::new(
                    axum_server::tls_rustls::RustlsConfig::from_config(http_tls),
                ),
            );
            axum_server::bind(sock_addr)
                .acceptor(acceptor)
                // with_connect_info so the SEC-6 per-client limiter can key on
                // the peer address when a caller presents no data-plane token.
                .serve(app.into_make_service_with_connect_info::<std::net::SocketAddr>())
                .await
                .expect("server error (TLS)");
        }
        (None, None) => {
            tokio::spawn(async move {
                if let Err(e) = timelake_flight::serve(flight_backend, flight_addr).await {
                    tracing::error!(error = %e, "flight sql server exited");
                }
            });
            let listener = TcpListener::bind(&addr).await.expect("bind listen address");
            axum::serve(
                listener,
                // with_connect_info: the SEC-6 limiter keys on the peer
                // address for callers without a data-plane token.
                timelake_server::app(engine)
                    .into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .expect("server error");
        }
        _ => panic!("TIMELAKE_TLS_CERT and TIMELAKE_TLS_KEY must be set together"),
    }
}
