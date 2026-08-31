# Changelog

All notable changes to TimeLakeDB are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
will follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html) from
its first release.

Nothing has been released yet. Everything below is pre-v1 work on `main`,
organised by the milestone that gated it. Every performance or robustness
entry traces to a recorded run in `docs/evidence/` — the harness is the
specification, so an entry without a measurement behind it does not belong
here.

## [Unreleased]

### Added — Last-value cache, the query (#57 phase 2, #150) (2026-08-31)

`last_cache('<table>')` is now a SQL table function that answers "current value
per entity" from the in-memory cache (#149) with NO file scan — the latest
`(time, tags, fields)` per series handed back as an in-memory table. It only
ever returns cached rows, so there is no wrong-answer fallback: a historical
point or a windowed aggregate queries the ordinary table and scans, as before.
Registered per query in `run_sql_env` (the one production call site) and scoped
to the session's database; it passes the read-only guard because it is a
`SELECT`. Verified end to end (`deploy/compose/last_cache_drill.sh`,
`docs/evidence/last-cache-drill.log`) against the SCAN COUNTERS, not wall-clock:
a real latest-per-series scan moves `timelake_scan_files_considered_total`,
`last_cache('cpu')` leaves it flat; its answer equals the scan's exactly (and an
out-of-order older write never wins); and writing 500 series into a 50-entry cap
keeps the count at 50 with only the hot series returned. Reference docs updated;
this completes #57.

### Added — Last-value cache, the mechanism (#57 phase 1, #149) (2026-08-31)

A new `timelake-lastvalue` crate holds an in-memory cache of the latest
`(timestamp, fields)` per series, updated on the write path — the groundwork for
"current value per entity" lookups that today plan and scan files. It is opt-in
per `(db, table)` via `PUT /admin/last_cache` (persisted like retention/rollups),
so the write-path cost lands only where asked; when nothing is enabled,
`is_active()` is a lock-free `false` and the write path does no extra work. Two
traps handled up front and covered by tests: an out-of-order write with an older
timestamp never moves "current value" backwards, and the cache is capped with
LRU eviction — one entry per series is exactly the per-series blowup FR-2 forbids,
so it accelerates HOT series, not all series, with `timelake_last_value_entries`
exposing the live count. Querying it (a `last_cache('table')` function, verified
against the scan counters) is phase 2 (#150); this phase ships the cache and the
write-path wiring only, with unit + write-path integration tests.

### Added — Helm chart, cluster mode (#81 phase 2, #146) (2026-08-30)

`helm install --set mode=cluster` deploys the C2 split: ingesters as a
**StatefulSet** (durable WAL + PVC, CL-2 paired), and the router, queriers and
compactor as **Deployments** (they own no durable data). Clients talk only to
the router's Service; the intra-cluster listener (1965) is confined to the
headless ingester Service — never external, never a LoadBalancer.

Discovery is **Consul, not static `TIMELAKE_PEERS`** — and the reason is worth
recording, because the ticket assumed static peers via stable DNS. It can't
work: an ingester replicates to its first ingester peer and does not filter
itself out, so within a shared StatefulSet pod template (one env for every pod)
there is no static list that excludes self. Consul has each node self-register
(node id = pod name, advertised address = pod IP, exactly as the discovery drill
wires it) and returns peers with self excluded, so `cluster.ingester.replicas` is
genuinely a value you turn. A bundled dev Consul + MinIO make a cluster
self-contained for a smoke; point `objectStore` at real S3 for production. A
wait-consul init container removes a router CrashLoop race (it refuses to start
with no ingesters, so it needs Consul up first). Verified end to end on a real
(kind) cluster — 2 ingesters + 2 queriers + compactor + router self-register,
a write through the router shards to the ingesters and reads back through a
querier (`docs/evidence/helm-cluster-smoke.log`). Completes #81.

### Added — Helm chart, single-node (#81 phase 1, #145) (2026-08-30)

`deploy/helm/timelakedb/` — the first supported way to deploy TimeLakeDB on
Kubernetes. `helm install` brings up a single `role=all` node as a **StatefulSet**
(not a Deployment: the node owns a durable WAL and data dir and must reschedule
onto the same volume) with a PVC for `/var/lib/timelake/data`, a client ClusterIP
Service plus a headless Service for stable identity, config via ConfigMap, and
object-store creds / encryption key / TLS via a chart Secret or an
`existingSecret`. Safe by default: uid 1000 (P0-2), read-only rootfs with a
tmpfs `/tmp`, dropped capabilities, `fsGroup` so the PVC is writable, and a
template-time refusal to render `dataAuth: off` behind an externally-exposed
Service. Installed on a real (kind) cluster and smoked green — `/health` reports
`role=all`, a line-protocol write returns 204 and `/api/sql` reads the rows back,
with the data on the PVC and the rootfs read-only
(`docs/evidence/helm-single-node-smoke.log`). The cluster topology (router /
ingester / querier split) is phase 2 (#146).

### Added — Live Consul discovery (C3, #71) (2026-08-30)

Discovery gained a second backend. Set `TIMELAKE_DISCOVERY=consul://host:port`
and a node **registers itself** with Consul and discovers its peers from the
live catalog, instead of a `TIMELAKE_PEERS` list hand-maintained on every node
and frozen until a restart; `static` (the default, unchanged) still reads
`TIMELAKE_PEERS`. The router, the CL-3 querier, the compactor and the U3 cluster
view **re-read membership live**, so a node joining or leaving takes effect
without a restart — a joined ingester receives writes / is unioned into queries /
appears in the view, a departed one is dropped.

CL-5 is preserved: discovery carries no correctness. A stale or lying membership
view only misroutes or wastes work — a misrouted write is idempotent under LWW,
an unreachable querier falls through, and two compactors briefly owning a
partition are caught by the commit fence — and a Consul outage **degrades to the
last-known-good set** (a `CONSUL_DISCOVERY_DEGRADED` alarm) rather than failing
writes. `peers()` is a lock-free snapshot read, never a Consul round-trip on the
hot path; membership refreshes out of band.

Landed over #137 (the ConsulDiscovery backend, repurposing the dead
`crates/discovery` placeholder so `timelake-cluster` stays dependency-free),
#138 (`TIMELAKE_DISCOVERY` selection, `main` holds `Arc<dyn Discovery>`), #139
(consumers re-read live) and #140 (the drill). Drilled end to end against a real
Consul agent in `docs/evidence/c3-consul-discovery-drill.log`
(`deploy/compose/cluster-drill/c3_consul_discovery_drill.sh`): a router + ingester
pair discovered from Consul with no `TIMELAKE_PEERS`, a live join and leave with
no restart, and a Consul flap that degrades rather than failing writes (zero
acked loss). Consul-side leave detection uses a self-passed TTL check, so Consul
never has to reach a node behind required mTLS.

### Added — Required intra-cluster mTLS (C3, #72) (2026-08-30)

The intra-cluster listener (`/internal/v1/*` on `TIMELAKE_CLUSTER_ADDR` — CL-2
replication, CL-3 live/snapshot reads) now **requires** a cluster-signed client
certificate when the cluster has TLS: set `TIMELAKE_TLS_CERT`/`_KEY` plus a new
`TIMELAKE_CLUSTER_CA`, and a peer with no certificate — or one signed outside
the cluster CA — is refused at the handshake. Unset keeps the link plaintext, so
the drills and single-node dev are untouched. The **data-plane listeners stay
want mode** (stock Grafana/Telegraf hold no cert, AT-6): the two listeners build
independent client-auth configs from independent CAs (`TIMELAKE_TLS_CLIENT_CA`
vs `TIMELAKE_CLUSTER_CA`). This closes SECURITY.md exposure 10 — de-published was
not authenticated.

The node presents its serving cert as its cluster identity: the CL-2 replication
client and the CL-3 querier's remote-buffer client dial peers over https,
present that identity, and trust **only** the cluster CA (not the public web
PKI). The serving cert and the cluster CA hot-rotate on file change
(validate-before-swap, last-good on a bad renewal), so a short-TTL renewal never
restarts the node. A new gauge `timelake_cluster_mtls_required` reports the
listener's mode, separate from the data plane's `timelake_tls_client_auth_mode`.

Landed over #129 (a require-mode verifier beside want, in `timelake-tls`), #130
(peer clients present an identity over TLS), #131 (require it on the listener)
and #132 (the drill + docs). Drilled end to end in
`docs/evidence/c3-mtls-rotation-drill.log`
(`deploy/compose/cluster-drill/c3_mtls_rotation_drill.sh`): a two-ingester pair
behind required mTLS — replication with zero acked loss across a node death, a
hot cert rotation under continuous writes that the established replication link
rides out (zero write errors, zero loss), and a certless / wrong-CA peer refused
at the handshake.

## [0.3.0] - 2026-08-26

### Added — InfluxDB migration, proven end to end (#78) (2026-08-25)

`ops/influxdb1-import.py` (the InfluxDB v1/v2 line-protocol importer, #97) now
has the drill that closes #78's acceptance:
`deploy/compose/migration-drill/migrate_drill.py` migrates a known corpus over
the ordinary write path and shows `COUNT(*)` plus a per-entity Shape-A lookup
agree with the source **exactly**, then that the int->float type-drift trap
quarantines the offending line to the rejects file rather than dropping it or
silently retyping the column. Evidence:
`docs/evidence/influxdb-migration-drill.log`. InfluxDB v2 is the same path (its
export is line protocol too), noted in the tool.

### Added — Flight DoPut: an Arrow-native write path (#79) (2026-08-25)

`DoPut(CommandStatementIngest)` over Flight SQL lands an Arrow stream on the
same write path line protocol uses — WAL fsync before ack, replication, LWW,
SEC-2, and the #98 schema-union conflict all inherited below `write_lp`. A
producer that already holds Arrow (Tributary L6, a Spark job, a DataFusion
pipeline) writes columnar batches without encoding to line-protocol text on the
wire; this unblocks Tributary's L6 Flight-shipping path.

Write-scoped and explicit: a read-only token is refused, the mirror of the read
guard — DoPut is a separate ingestion door, not an exception carved into P0-2's
read-only SQL guard. Column roles follow the engine's storage so a DoGet batch
writes straight back: `time` is the timestamp, a string column is a tag, a
numeric/boolean column is a field, with an Arrow field-metadata override
(`timelake:role`) for a genuine string field (Flight hydrates dictionaries to
Utf8 on the wire, so tag and string-field columns are otherwise
indistinguishable). A conflicting column type is refused like a bad
line-protocol field (#98), never silently forked.

### Added — Prometheus `remote_write` ingest (#56, R-3) (2026-08-25)

`POST /api/v1/write` accepts a snappy-compressed protobuf `WriteRequest`, so a
Prometheus server or Grafana Agent points at TimeLakeDB with one config line —
no Telegraf or Tributary in between. The decode is separate from line protocol
(new `timelake-prometheus` crate — the four `prompb` messages are hand-written,
so no `protoc` build step), but the rows land on the **same** engine write path:
WAL fsync before the 204, CL-2 replication, LWW dedup and SEC-2 all inherited,
not reimplemented.

The mapping is deliberately not VictoriaMetrics': one Prometheus series
`(__name__, labels)` becomes one row — `__name__` the measurement, every other
label a tag, the sample value a `value` field — never a table per field, so a
tag stays a compressed dictionary column (FR-2). Millisecond timestamps become
nanoseconds; stale/±Inf samples are dropped, so a stale-only scrape is a 204
no-op rather than a 400. A remote_write arm reads back row-for-row identical to
the same data sent as line protocol
(`crates/server/tests/prometheus_remote_write.rs`).

### Fixed — a flush could let a later write change a column's type (#98) (2026-08-25)

A field's established type has to outlive the buffer a flush drains, and it
didn't. The write path only checked the *live* buffer, so once a flush had
reset it, a value conflicting with the column's real type was accepted with a
204 and corrupted the table at read: a string retyped the whole column (the
float `1.5` came back as `"1.5"`), an int truncated it (`1.5` came back as
`1`). The identical write with no flush in between was correctly refused — the
bug only surfaced across a flush, which is exactly when nobody is watching for
it.

Fixed to consult the column's committed type, not just whatever the buffer
currently holds. All three faces are pinned in
`crates/server/tests/type_conflict.rs`: the string is refused, the int coerces
without truncating, and neither can be resurrected by WAL replay after a
restart.

### Fixed — one schema-conflicting table no longer fails reads of every other table (#99) (2026-08-25)

The query path registers a DataFusion provider for every table up front, so a
single table whose files and buffer disagree on a column's type — it cannot
present one schema — used to abort the *whole* request, taking reads of every
other table in the database down with it. A table corrupted before the #98 fix
could make the entire database look dead.

Isolate it instead: the conflicting table gets an `ErrorTable` provider that
errors only when that table is actually scanned, so a query that never names it
runs untouched. A new `unbuildable_tables_total` counter makes the condition
visible — a rising count means pre-#98 corrupt tables are being read around, not
that reads are failing. The conflict message names the columns and Arrow types
in the server log; SEC-5 keeps it off the wire, sanitized like any other read
error (#47).

### Changed — the L0 row-group knob, measured at full scale: leave it off (#70) (2026-08-25)

`TIMELAKE_L0_ROW_GROUP_ROWS` (shipped default-off in 0.2.0, #76) flushes L0 with
fine row groups so a present-entity Shape A lookup on fresh data need not decode
a ~1M-row group for a handful of rows. Phase 1 (#69) traced #68's 608 ms cold
p95 to that gap; this is the full-scale acceptance that decides the default. Run
interleaved (baseline, 64K, baseline, …) on a fresh container each time so cache
warming can't favour one config, over the 36.6M-line workload — not unit scale.
The knob doesn't earn its ingest cost, so the default stays off. Evidence:
`docs/evidence/shape-a-p95-l0-rowgroups.md`.

## [0.2.0] - 2026-08-25

### Added — a finer-L0-row-group knob for the Shape A path (#70, mechanism) (2026-08-24)

The lever #69 pointed at. `EngineConfig.l0_row_group_rows` /
`TIMELAKE_L0_ROW_GROUP_ROWS` sets the row-group size for **L0 flush**, which
until now used the parquet writer's coarse default (~1M rows) while compaction
used 64K. A small value makes fresh data's blooms and stats prune at a fine
grain, so a point lookup decodes a small group instead of a huge one — the
Shape A p95 fix (#68).

Proven at the mechanism level (`provider::tests::finer_l0_row_groups_read_far_
less_for_a_present_entity`): the same unclustered L0 data written with fine
(256-row) vs coarse groups, a **present** entity's lookup — the case blooms
can't skip — reads **~3× fewer bytes** fine vs coarse at 20K rows, and the gap
widens sharply at scale where a coarse group is megabytes. Correctness is
unchanged (the entity's row comes back either way).

**Off by default, deliberately.** M4 once measured finer row groups as a
regression — before range reads existed, when a scan pulled the whole object.
That regime is gone (the scan fetches only surviving groups), but the write
cost of finer groups (more per-group metadata) is real, so flipping the
default waits on a **full-scale Gauge run** confirming Shape A p95 < 250 ms AND
no ingest/storage regression (RR-1/PR-1). That run — and the default flip it
justifies — is the remaining step to close #68; the knob and the
`timelake_scan_*` counters (#69) are what make it measurable.

### Added — scan-pruning telemetry, and the Shape A p95 diagnosis (#69) (2026-08-24)

Phase 1 of the Shape A p95 carve-out (#68): instrument the read path so a
lookup's cost is a metric, and find out where it actually goes before building
a fix. It went somewhere other than the ticket assumed.

Eight `/metrics` counters from a new `ScanStats`, threaded through the scan
like `filtered_rows`: `timelake_scan_files_{considered,time_pruned}_total`,
`timelake_scan_row_groups_{considered,stats_pruned,bloom_pruned,scanned}_total`,
and `timelake_scan_meta_cache_{hits,misses}_total`. `considered = stats_pruned
+ bloom_pruned + scanned` per file, so a single lookup on an idle node reads as
the delta and names its own dominant cost — pruning that leaves no trace was
indistinguishable from pruning that doesn't happen.

**The finding, which re-scopes #70.** M4's carve-out reasoning was "the arrow
writer emits no blooms for dictionary columns, so L0 can't prune by
`product_id`." The code contradicts it: `to_parquet_bytes_rg` explicitly
enables blooms on entity columns (NDV ≥ 1024), on both the L0 and compaction
paths, and the buffer's own `dict_columns_do_get_blooms` test proves every row
group gets one. So L0 files ARE bloom-prunable by entity; the 608 ms predates
blooms-on-dict. The real gap is that **L0 flush uses the writer's coarse
default row-group size** while compaction uses 64K — so L0 blooms exclude at a
coarse grain, and a present entity's lookup decodes a ~1M-row group to return a
handful of rows. Pinned by
`provider::tests::scan_stats_attribute_pruning_and_prove_l0_blooms_work` (an
unclustered, small-group L0 file: a present pid scans 1 group of ~78, an absent
pid scans 0, the counters attribute it). The stale "arrow emits no blooms"
comments in `buffer` and `compact` are corrected. Full write-up:
`docs/evidence/shape-a-p95-characterization.md`. #70 is now "flush L0 with
finer row groups," not "add blooms."

### Added — downsampling in a cluster: rollups materialise on the compactor (R-2, #64) (2026-08-24)

The cluster half of downsampling. A role-split cluster (router + ingesters +
queriers, no `all` node) had nowhere to run rollups; now the **compactor**
does it, next to compaction and retention.

Four things it needed beyond the single-node path:

- **The shard union.** The compactor reads the source the way a querier does
  — it holds the ingesters as peers and reuses `RemoteBuffers` + the querier
  tail — so a rollup over a source sharded across ingesters aggregates every
  shard before it seals. The querier's refuse-rather-than-undercount rule
  applies for free: an unreachable ingester fails the pass rather than sealing
  an incomplete bucket, and the next tick retries.
- **An internal write.** The compactor is read-only to the data plane, but a
  rollup target is server-generated, not a client write. The write body split
  into `write_lp_internal` (WAL, buffer, replication, apply) which
  materialisation calls directly; the trait `write_lp` keeps the read-only
  refusal and the reject accounting for actual clients. A read-only node now
  materialises but still refuses client writes — pinned by a test.
- **Rollup ownership.** With more than one compactor each rollup is owned by
  exactly one (`owns_rollup`, the partition-ownership hash keyed on the
  rollup), or two would seal the same bucket and — the watermark model not
  deduping — double it. The FNV hash factored into one helper shared with
  partition ownership.
- **Runtime propagation.** Rollups loaded only at boot; a definition made on a
  console never reached the compactor (which serves no admin surface) until a
  restart. `reload_rollups` reads the shared store each tick, like token
  reload (#46).

Also: the `all`-node tick still materialises for a single node, but an
ingester's tick now does not — in a cluster the compactor owns it, and an
ingester materialising too would double it.

Drilled on `deploy/compose/timelakedb-cluster-s3.yml` with a compactor and no
`all` node (`cluster-drill/rollup_cluster_drill.sh`,
`docs/evidence/rollup-cluster-drill.log`): source written through the router
and sharded to an ingester, the compactor seals it, and the target read back
**through a querier** is exact (one row per host, counts summing to every
source row) and idempotent. This closes downsampling (R-2) end to end.

### Added — the compactor gate opens: partition ownership (C2 phase 5b) (2026-08-24)

`TIMELAKE_ROLE=compactor` starts now. Phase 5a built the role and left the
gate shut on purpose: the commit fence already makes two compactors *correct*
(a merge whose inputs were replaced is refused), but not *efficient* — both
would race every partition and do double the IO to land half the merges. 5b is
the work-avoidance layer the gate was waiting on.

Each compactor owns a disjoint slice of partitions — FNV-1a over
`db\0table\0partition` mod N, the same hash the router shards writes with,
keyed on the partition (`compaction::owns_partition`) — so `compact_once`
skips what it does not own and N compactors never race the same merge. Every
node computes the same ordinal by sorting its id with the discovered compactor
peers; no coordination. The fence stays the floor: if ownership ever overlaps
(a membership change mid-flight, a mismatched N) the loser's merge is still
refused, so ownership only has to be good, not perfect.
`timelake_compactor_shard_{ordinal,count}` on `/metrics` shows the split.

Scoped to compaction merges, the IO-heavy stage the gate names; retention and
tombstone GC still run on every compactor and stay fence-safe (cheaper work,
sharding them is a later refinement, not correctness).

Pinned by a unit test (ownership is a total, deterministic partition), an
integration test (`compact_once` merges only owned partitions; an unsharded
node owns everything), and a two-compactor drill
(`deploy/compose/compactor-drill/shard_drill.sh`,
`docs/evidence/c2-phase5b-shard-drill.log`): 8 tables split 4/4, both
compactors busy, `stale_merges` 0, the store settling to one file per table,
48 rows in and out. `ARCHITECTURE.md` §12.4 updated; the single-compactor
rig's stale "does not start yet" comment corrected. This unblocks the cluster
half of downsampling (#64), which moves rollup materialisation onto the
compactor.

### Added — downsampling: a filter and two more aggregates (R-2, #60) (2026-08-24)

The grammar half of #60, on top of the part-2 mechanism. Three additions to
a rollup definition, all still recomputable from a bucket's own rows so the
exactly-once argument is untouched:

- **`filter`** — a SQL boolean expression on the source (`region = 'eu'`,
  `status_code >= 500`), ANDed into the bucket scan before aggregation. Same
  predicate every pass, so it does not disturb idempotency; parenthesised so
  its own `OR` can't rebind against the time bounds; run under the read-only
  guard, so it selects but can't write. A malformed filter fails that one
  rollup's pass loudly and the others carry on, rather than writing a wrong
  number — the same rule the tick already uses for a bad rollup.
- **`count_distinct`** — distinct count over the bucket. It is *not*
  algebraically combinable, which is exactly why it is safe here and would
  not be under an accumulating scheme: materialisation never combines
  partials, it computes each sealed bucket once from raw rows.
- **`percentile`** — `approx_percentile_cont`, with a `quantile` (0.0–1.0) on
  the aggregation. Approximate on purpose: an exact percentile over a wide
  `lookback` is too expensive, and the recompute-each-pass property holds
  either way. `quantile` is enforced both directions in `validate` — a
  percentile without one, or a quantile on any other function, is a 400 at
  definition rather than a rollup that fails silently forever.

`RollupAgg`/`RollupDef` lost their `Eq` derive because `quantile` is an
`f64` (no total equality); `PartialEq` is all the upserts and tests use.
Pinned by `crates/server/tests/rollup_materialize.rs` (a combined
filter + `count_distinct` + `percentile` rollup, exact and idempotent) and
four new rejection cases in `rollups.rs`. The cluster half of #60 —
materialising from the shard union in the compactor role — is blocked on the
compactor role becoming startable (C2 phase 5b) and is not in this change.

Also reconciles ARCHITECTURE §18, which merged (timelakedb#58) still
describing the recompute-and-overwrite design that part 2 replaced: §18.3
now documents watermark-finalization and why recompute-plus-LWW was unsound
here, §18.5 marks which metrics actually shipped, and §18.6 marks phase 1 and
this grammar half done.
### Added — downsampling, part 2: rollup materialisation (R-2, #59) (2026-08-23)

The second half of #59: a defined rollup now **runs**. On the maintenance
tick a pass re-aggregates the source into the target table, so part 1's
`/admin/rollups` surface stops being inert. It is on the public reference
page now, because it works.

The design sketch in §18.3 said "recompute the trailing window every pass
and let last-write-wins collapse the re-emitted buckets." That is wrong
here, and the reason is worth writing down so nobody restores it: LWW
dedup in this engine happens at compaction, and compaction's overlap
trigger is *strict on a shared boundary* (`compaction::has_overlap`). A
rollup row sits exactly on its bucket start, so a rollup whose data lands
in one bucket writes single-instant files — `min_ts == max_ts` — and two
of those never register as overlapping. The re-emitted bucket would sit
there duplicated forever, and a `sum`/`count` over the target would read
double. Widening the source data hides it; a single-bucket rollup shows
it every time.

So materialisation is **exactly-once by construction** instead. A bucket
is written once, only after its whole span has aged past `lookback`, and
never rewritten. The watermark is the target's own `max(time)` + one
interval — no side table, correct across a restart for free — so a
re-run finalizes nothing already present and the target never carries a
duplicate primary key. No compaction is in the correctness path at all.

`lookback` now has a precise meaning: the grace an open bucket is held for
late data before it seals. A row landing within `lookback` of its bucket
is counted; one landing after the seal is not — the honest limitation,
and exactly what the retention invariant from part 1 protects (the source
outlives the grace, so a sealing bucket still has its rows). Same shape as
a TimescaleDB continuous aggregate with a refresh lag.

Order matters in the tick: materialise runs **before** flush and compaction,
not after, so a pass's target rows are flushed and settled in the same
tick rather than lingering a cycle. Materialisation is a query, so it runs
in the async loop, not the blocking maintenance closure. One rollup's
failure is logged and skipped, never fatal to the others or the tick.

The window bounds are pinned into the SQL as `arrow_cast(<ns>, 'Timestamp
(Nanosecond, None)')` literals rather than left as `now()`: `now()` is
timezone-aware and `time` is not, and a coercion error would be swallowed
as a non-fatal rollup failure, silently leaving the target empty. The pass
reads with empty authorizations — public rows only — so a SEC-2-labelled
source contributes only its unlabelled rows to a public target, the safe
direction, and a documented v1 limit.

`timelake_rollup_materializations_total` and `timelake_rollup_rows_written
_total` on `/metrics`. Pinned by `crates/server/tests/rollup_materialize.rs`:
exact seven-aggregate downsampling, exactly-once (a second pass at the same
clock writes zero and the target stays one row), and the grace both ways —
a late row before the seal is counted, one after it is not. The clock is
injected (`materialize_rollups_at`) so "aged past lookback" is deterministic
rather than a race against `SystemTime::now`.

### Added — downsampling, part 1: the rollup definition surface (R-2, #59) (2026-08-23)

The first half of single-node downsampling (ARCHITECTURE §18, design in
timelakedb#58): a rollup can be **defined, validated, persisted and
removed**, and the retention invariant is enforced at definition time.
Materialisation — the stage that actually writes the target table on the
maintenance tick — is the second half of #59 and is **not** in this
change, so a defined rollup persists but does not yet run. That is why it
is not on the public reference page: the site documents what works, and a
rollup that materialises nothing would be a footgun there. The
`/admin/rollups` routes exist so the surface can be reviewed and driven,
and the CHANGELOG says plainly that they are inert until part 2.

A `RollupDef` is configuration, not SQL DDL — the read-only guard refuses
`CREATE MATERIALIZED VIEW` (P0-2), and a standing aggregate-and-delete
control belongs behind the same admin auth as retention. So the whole
thing is the retention pattern reused: the type lives in `crates/api`
beside `RetentionPolicy`; it persists to `catalog/config/rollups.json`
through the `Store` (encrypted, SEC-1), seeded from `TIMELAKE_ROLLUPS`
(a JSON array), the stored copy winning at boot; `GET/PUT /admin/rollups`
and `DELETE /admin/rollups/{db}/{name}` mirror `/admin/retention`,
`admin`-role and audited (`rollup.set` / `rollup.remove`); and
`timelake_rollups` counts the definitions on `/metrics`.

The v1 aggregate set is the recomputable-from-source ones only — `avg`,
`min`, `max`, `sum`, `count`, `first`, `last` — because recompute-from-
source is what makes re-materialisation idempotent (§18.3), and an
aggregate that cannot be recomputed exactly from a bucket's rows would
break that before it is even written.

The retention invariant (§18.4) is checked in `set_rollup`, not
discovered wrong later: a rollup whose lookback reaches past its source's
retention is refused, because the oldest buckets would under-count as
retention drops the source out from under a materialisation pass. Most-
specific source policy wins, matching enforcement; a wildcard (`"*"`)
policy binds it too; a source kept forever passes any lookback.

Pinned by `crates/server/tests/rollups.rs`: the define/list/remove +
restart round-trip through the store, upsert-by-`(db,name)`, the eight
structural rejections, and the retention invariant (specific and wildcard
source policies). No materialisation is exercised — that test lands with
part 2.

### Fixed — SEC-5 had a hole: a schema-union conflict leaked a column and its types (2026-08-23)

SEC-5 sanitizes query failures at `run_sql_env`, the one execution point,
so a bad table or column name comes back as `query could not be executed
(ref: q-…)`. One failure never reached it. Before a query runs, the read
path unions the schemas of a table's live batches (`Engine::sql_batches`),
and a conflict there returns `column 'reading' has conflicting types Utf8
vs Float64` — a column name and both Arrow types — straight to `/api/sql`
and Flight, because the `?` propagates out of `sql_batches` before
`run_sql_env` is called. SECURITY.md said exposure 5 was CLOSED; it had a
hole (#47).

The one read-path union now routes through
`timelake_query::opaque_read_error` — the same policy `opaque` applies,
its own ref, the full error logged server-side. The write-path union
sites keep their verbatim message on purpose: a write that conflicts with
a stored type is told which field, and that is on the reference page. The
deliberately-safe assembly messages — `database … does not exist`, the
stale-catalog refusal — stay verbatim too; they name the caller's own
input or cluster state, not schema.

Narrow to reach, which is why it survived: two live batches of one table
have to disagree on a column's type, and first-writer-wins typing
prevents that within a node. It is reachable on real data — a table that
took a pre-#43 `time`-field line still carries the conflict until
compaction, and a CL-3 querier unioning several ingesters' snapshots can
produce it — which is exactly the case a "closed" label stops anyone
watching for.

Pinned by `crates/query`
`a_schema_union_conflict_is_named_in_full_then_sanitized_for_the_wire`:
`schema_union`'s own message names the column and both types (the write
path wants that), and `opaque_read_error` over it is the ref with none of
it. The existing `query_errors_are_opaque_sec5` covers the plan/execute
paths. What no in-process test covers is a live 1850 conflict end to end,
because the write path cannot construct one on a single node; Riverkeeper's
`query-errors-are-sanitized` control runs the same single topology and so
has the same limit, documented there rather than faked.

### Fixed — no two compose rigs publish the same host port any more (2026-08-23)

Three pairs did: the alerting rig and the CL-2 cluster rig (and the
compactor rig's node) on 5963/5964, the console and data-auth rigs on
4963/4964, the TLS and console rigs' Grafanas on 3004. Each rig was added
on a different day and picked a free-looking number; nobody grepped. The
cost is not the "port is already allocated" — that one at least fails —
it is the half-down case, where the second rig's drill writes its probe
rows into the first rig's node and waits for something on a different
stack to react (#42).

Moved: alerting → 7963/7964, compactor → 7965/7966, the console rig's
data ports → 4973/4974, TLS Grafana → 3006 (`at6_grafana_mtls.py`
follows). Left alone, deliberately: `timelakedb.yml` (1963/1964/3003 —
every external document points at it), the cluster rigs (every cluster
drill, and Catchment's `ingester_kill`, reach them by number), the
data-auth rig's 4963/4964 — the first cut moved *that* one, and a grep of
the sibling repos found Riverkeeper's and Catchment's stack tables both
pointing at it, so the console moved instead — the console's Grafana on
3004 (cited in CLAUDE.md and on the site), and `docs/evidence/**`, which
are transcripts of runs on the old numbers and keep saying so.

`deploy/compose/check-ports.sh` lists every host port in the directory
and exits non-zero on a duplicate — the ticket's done-when, now a
command rather than a sentence. Run it before choosing a number.

### Fixed — the router parses the whole body before it forwards, so a poison line writes zero on every shard (2026-08-23)

The router's pre-forward check was "does each line have a measurement",
plus gzip and UTF-8 for the body. `ARCHITECTURE.md` §12.4 and the README
said "the whole body is validated before any shard is forwarded, so a
poison line writes zero", and that was true of the poison line the drill
used (no measurement) and false of the ordinary kind: a line with a bad
field value passed the router, reached exactly one shard and was refused
there — after every other shard had already been acknowledged. The
client saw a 400; the rest of its batch was durable. An agent treating
a 400 as poison then quarantines rows that are already in the database
(#38).

The router now runs `timelake_ingest::parse_lines` over the decompressed
body, under the precision the client asked for, before it sends
anything; a parse error is a 400 in the ingester's own words (`line 21:
bad float "notanumber" in …`) and nothing leaves the router. An unknown
`precision` is a 400 at the router too. The measurement-presence loop
stays as the sharding pass.

**Cost, measured with Gauge through the router before landing** (the
router + 2 ingesters + 2 queriers rig, `--scale laptop`, fresh rig per
run, two before and two after on an idle box): ingest 530,176 / 522,969
→ 500,610 / 502,434 lines/s, **−4.8%** on the pair medians, batch p95
unchanged (102–106 ms), `rows_48h` exact on both after runs. Host ingest
and burst within noise. Runs `timelakedb-router-validate-{before-1,
before-2,after-3,after-4}` in Gauge's results; the reasoning and the
lesson are in `gauge/PERFORMANCE_LOG.md`. The lesson is worth repeating
here: the first two "after" runs read 110K and 251K lines/s — a 2–5×
collapse — because the test suite was compiling on the same box. Not
recorded. A delta that looks like a story usually is one.

Pinned: `tests/router.rs` —
`a_field_level_poison_line_writes_nothing_anywhere_either` (20 good
lines across both shards plus one bad float → 400 naming line 21 and the
fault, and neither stub received a byte) and
`the_router_validates_under_the_clients_precision_not_a_default`
(`precision=s` body accepted and forwarded; `precision=bogus` a 400 with
nothing forwarded). The parse runs on the request task, as the
measurement-presence loop always did; if a future body size makes that
visible, `spawn_blocking` is the next move and the number to beat is
written down.

### Fixed — three literals that were true when typed and never again (2026-08-23)

`/health` reported `"milestone":"M3"` — typed on 2026-08-08 and untouched
while M4, M5 and five cluster phases shipped, so the first thing a new
user curls told them something false. Gone, not bumped: nothing parsed
it (grep of every sibling repo), and there is no milestone concept that
survives the cluster phases, so any value would rot the same way. The
`/health` test now pins its absence. Flight `SqlInfo` reported Arrow
`"58"` against arrow 59.2 — a literal that sat through the DataFusion 55
bump; it is `arrow::ARROW_VERSION` now, and cannot drift again. And the
self-monitoring module's doc comment named a `TIMELAKE_SELFMON_SECS`
that no code read — an operator who set it because the source told them
to got a knob that silently did nothing, which RR-5 names as a failure.
The sentence says why there is deliberately no such knob instead (#39).

### Fixed — tokens issued or revoked on one node take effect on every node within a tick (2026-08-23)

Every node in a cluster shares one bucket and so one
`catalog/config/tokens.json` — and each node read it exactly once, at
`Auth::open`. The file was shared; the in-memory copy was not. A token
issued on ingester-a's console was 401 on ingester-b until ingester-b
restarted, which made `TIMELAKE_DATA_AUTH=required` a one-ingester
feature, and — the half that actually matters — a token *revoked* on A
kept working on B, and on every querier Grafana reads through. The
reference page said revocation was "effective immediately"; it was true
of one process. #45's drill rig has one ingester precisely to keep this
out of that test, which was the polite way of saying the two-ingester
version would fail (#46).

`Auth::reload_tokens` re-reads the file and swaps the set in if it
changed: one small `get`, hashed before it is parsed, so an unchanged
file costs a comparison. The maintenance tick calls it on ingesters and
`all` nodes; the querier's catalog-tail loop calls it every tenth
iteration (the tail runs at 1 s, so the cadence matches), because a
querier runs no maintenance and is the node that authenticates reads.
`verify_token` also calls it once on an *unknown* token, rate-limited to
one attempt a second node-wide, so a token issued elsewhere a moment ago
works on its first presentation and a client spraying bad tokens costs
the store one read a second rather than one per request. A store error
leaves the loaded set in place and is logged — a bucket blip must never
turn `required` into `open`. `persist_tokens` records the hash of what
it wrote, so a node's own issue or revoke is not a "change" on its next
tick. `timelake_auth_token_reloads_total` counts reloads that changed
something.

Red first, for a real reason: on a fresh store `open` recorded no hash
for the absent file, so the first reload counted "no file → empty file"
as a change. An absent file now hashes as the empty file.

Drilled on `timelakedb-cluster-s3.yml`, which gained
`TIMELAKE_DATA_AUTH: ${TLDB_DATA_AUTH:-off}` on every data node (off by
default, so nothing existing changes): `cluster-drill/token_reload_drill.sh`,
14/14 in `docs/evidence/token-reload-drill.log` — issue on A; first use
on B 204 with `token_reloads_total` moved (was 401 until restart); a
router write sharded across both 204; a read on querier-a 200 and its
counter moved; revoke on A → A 401 at once, B 401 after one tick,
querier-a 401, router 401 on whichever shard. No restarts anywhere.

Known, not done here: `persist_tokens` is still a plain `put`. Two
consoles issuing at the same instant on one bucket is a last-writer-wins
on the file, as it was before — reload makes the loss visible sooner
rather than causing it. CAS on the token file is the fix, and its own
issue.

### Fixed — `TIMELAKE_NODE_ID` has one default, read in one place (2026-08-23)

Two reads of one variable, two fallbacks, written two phases apart:
discovery (C2 phase 1) fell back to `node-local`, the engine (U2
self-monitoring, then the audit chain) to `tldb`. An unset node had two
names — the boot log and its peers said one, every audit record and
`_system` row said the other — and an operator correlating a log line
with the audit trail had to read the source to learn they were one
process. The cluster rigs all set the variable, which is why nobody saw
it; every package install and the stock compose file do not (#40).

`timelake_cluster::node_id_from_env()` is the one read, with
`DEFAULT_NODE_ID = "tldb"` — the name already in the evidence logs, in
`_system.queries` on every drilled node, and in the site's configuration
table, so the one that keeps the most things true. `StaticDiscovery::from_env`
and `Engine::open` both call it; a cluster test pins the constant and
the discovery default. The audit chain is per node and stamps `node_id`
on every record: a fresh unset node stamps `tldb` from its first record,
which the engine side already did, and nothing touches existing audit
segments — a rewritten record is indistinguishable from tampering, which
is what `?verify=1` exists to catch.

### Fixed — the router forwards the client's `Authorization` on writes, so `required` auth works behind it (2026-08-22)

The router's `/api/sql` forward had carried `authorization` and
`x-timelake-authorizations` from day one, because the querier is where
SEC-2 and SEC-4 are decided. Its *write* forward sent each shard with
`db`, `precision` and the bytes — nothing else — so every shard arrived
at the ingester anonymous. Behind a router, `TIMELAKE_DATA_AUTH=required`
was therefore impossible: a client with a good token got its 401 back
through the router. `optional` was quieter and worse: the ingesters'
`timelake_data_requests_{authenticated,anonymous}_total` split — the
number SECURITY.md tells an operator to flip to `required` on — counted
every router write as anonymous, so it measured the router, not the
clients (#37). Nobody had hit it because every cluster rig runs with
auth off, which is the default.

One header, `Authorization`, copied onto each shard's forward. Not
`X-TimeLake-Authorizations`: the write path does not read it — a
write's visibility label is the `_visibility` tag in the body. And the
router still authenticates nothing itself; it has no token store and
must not grow one.

Unit: `tests/router.rs::a_write_carries_the_clients_authorization_to_every_shard`
— red with the fix stashed (`ing-a must see the client's credential on
its shard`), green with it; an anonymous write is pinned to arrive
anonymous, so the router invents nothing either. Live: new rig
`deploy/compose/timelakedb-router-auth.yml` (router + one ingester in
`required` mode, ports 6962/6963 — checked against every other rig,
#42) and `cluster-drill/router_auth_drill.sh`, 15/15 in
`docs/evidence/router-auth-drill.log`: token issued on the ingester's
console, Bearer through the router 204, none 401, wrong 401, Telegraf's
`Token` spelling 204, both accepted writes exactly 50 rows on the
ingester, `authenticated_total` +4, `anonymous_total` +0,
`rejected_total` +2, `router_forwarded_total` 4.

Two things the drill taught, recorded in its transcript: a
`TIMELAKE_ADMIN_BOOTSTRAP_PASSWORD` principal is still quarantined until
it rotates once (by design — `crates/auth` pins it; the docs now say
so), and LWW dedup is flush-time, so sending one body twice reads back
as 100 rows from the live buffer, not 50. Both were the drill being
wrong, and both are the kind of wrong the next person would repeat.

One ingester in the rig on purpose. Each node loads
`catalog/config/tokens.json` at boot and nothing reloads it, so a token
issued on one ingester is unknown to its peer until that peer restarts
— with a shared bucket the file is shared and the in-memory copy is
not. That is its own issue, and a pair would have confounded it with
this one.

### Fixed — a router-role node accepts `TIMELAKE_MAX_BODY_BYTES`, not axum's 2 MiB (2026-08-22)

The 2026-08-13 entry below says bodies over 2 MiB were refused and that
the fix applied the limit "to **both** routers from one config value".
It did — to both routers that live in `lib.rs`. The router *role* is
built in `router.rs`, and `router_app` never got the layer, so for nine
days the single write endpoint a cluster's clients are told to use
refused anything over axum's default while the ingesters behind it took
32 MiB. FR-1's ≥10 MB batches, 413'd at the front door (#36). Nobody
hit it because the drills use small bodies and the bench goes through
the router with batches under the default.

The limit now lives on `RouterState` (default: the engine's
`EngineConfig::default().max_body_bytes`, set from
`TIMELAKE_MAX_BODY_BYTES` in `main.rs` through the same
`config_from_env` the engine uses) and `router_app` applies it. On the
state rather than as a parameter on purpose: the failure was a router
built with *no* limit argument silently inheriting axum's, and a field
with the engine's default cannot do that. It still caps bytes on the
wire — gzip is decompressed inside the handler, as everywhere else.

The router opens no engine (that is the point of the role), so it reads
the one setting it shares with the ingesters from the environment
parser rather than re-deriving a default here that could drift.

Pinned in `crates/server/tests/router.rs`: a 3 MiB body is forwarded
whole and every line arrives once, and a router told to take 1 KiB
refuses 4 KiB with 413 and forwards nothing — the second test is what
proves the limit is the configured one and not merely a bigger
constant. The stub node in that file gained its own 64 MiB limit, or it
would have been the thing under test.

### Fixed — a tag or field named `time` is refused at parse; it used to wedge the table (2026-08-22)

`time` is the timestamp column every table gets as field 0, and nothing
reserved the name. Found by reading the buffer (#41); measured before the
fix landed, because the issue said to write the test first and watch it:

```
write "tt,h=a time=1,v=1 ..."   -> 204 No Content
SELECT * FROM tt                -> 400 query could not be executed (ref: q-00000000)
DESCRIBE tt                     -> time Timestamp(ns) | h Utf8View | time Float64 | v Float64
flush_all()                     -> ERR flush incomplete for poc.tt, poc.tg
COUNT(*) after the flush        -> 400 column 'time' has conflicting types Dictionary(Int32, Utf8) vs Timestamp(...)
```

So: accepted, unreadable, **unflushable** — which means the WAL holds the
frame forever, the frame replays on every restart, and the table is wedged
durably by one line. The same shape as the ragged-column bug of
2026-08-08, and it has the same fix: a parse error in `crates/ingest`,
which is a 400 *before* the WAL append. A check in the buffer would have
been too late — the frame is fsynced by then, and the fix would have had
to be another "skip unreplayable frame" path.

`tag 'time' is reserved: it is the timestamp column of every table` /
`field 'time' is reserved: …`, with the line number like every other
parse error. Only `time` is reserved — InfluxDB also refuses it; it
additionally reserves Flux's `_measurement`/`_field`/`_time`, which are
not columns here and a migrated dataset may legitimately carry. A
measurement called `time`, a tag *value* of `time`, and keys such as
`time_ms` or `uptime` are untouched (pinned).

One consequence, deliberate: a WAL written before this change that holds
such a line will log `skipping unreplayable WAL frame` on the next start
instead of replaying the poison. That is the existing path for a frame
the parser refuses, and the right one — the frame was never readable.

Pinned by `crates/ingest` (both key positions, line numbers, the
not-reserved neighbours) and `crates/server/tests/health.rs`
`a_tag_or_field_named_time_is_refused_before_the_wal` (400 on both
surfaces, nothing durable, a restart finds no table).

### Fixed — the rebalance-duplicates finding is closed, and a second one found on the way (2026-08-22)

Catchment's `router-tributary-exactness` composition was re-run seven
times against overlap-aware compaction (below, 2026-08-21). The closing
run, `...-20260822-160108`, is PASS 17/17. An undrained 2→3 ingester
reshape still twins the batches caught mid-window — 202,000 rows observed
— and the next overlap-triggered compaction pass collapses them to an
exact 200,000, **cross-node**: `compact_once` groups files out of the
shared catalog by `(db, table, partition)` with no node dimension, and
the CAS loop teaches every node about its peers' files on the first
commit collision. That corrects a sentence written into `REBALANCE.md`
two days earlier, that twins only meet if they land on the same node.
Wrong in the safe direction, still wrong; fixed.
`docs/FINDING_rebalance_duplicates_replayed_writes.md` is closed; the
drain rule in `docs/REBALANCE.md` stays, because what it buys now is
avoiding the transient inflation between the reshape and that compaction
pass, which is a different sentence than "preventing a permanent one".

The better story is the bug the re-runs found. Three of the first five
wedged in a way the rebalance finding does not describe: the reshape
frees the router's IP, Docker hands it to a recreated *querier*, the
agents' keep-alive connection pool follows the address (DNS is consulted
on dial only), and the querier's 501 — which literally says "this node
holds no write path" — was filed under retryable transport and retried on
the same connection, about five times a second, forever. `/proc` lied
about it (WSL2 hides `wchan`); strace did not. Kubernetes recycles pod
IPs on every rollout, so this is not a Docker party trick.
`docs/FINDING_agent_pools_a_reused_ip.md` has the strace, the two
hypotheses that died first (an uncapped retry-after sleep; DNS thread
pile-up), and the fix — shipped in Tributary: rebuild the client on a
501 and on every third consecutive transport failure,
`tributary_transport_rebuilds_total` makes it visible, and the unit test
was red-proofed by neutering the fix (`conns:1`). On this side, four C4
harness defects that had each read as a plausible product failure were
fixed in the same campaign; the run notes have them.

### Added — Grafana alerting, verified end to end, and the ORDER BY that silently kills a rule (2026-08-21/22)

FR-9 covered dashboards; nothing had ever exercised alerting, which adds
a stage a dashboard never touches — the datasource frame has to survive
a `reduce`. It does: data written → rule evaluates → threshold breached
→ alert fires → notification *delivered* to a webhook recorder → data
returns to normal → alert clears, against Grafana 13.1.3 and a stock
node. `docs/ALERTING.md`; rig `deploy/compose/timelakedb-alerting.yml`
(Grafana on 3005, a `python:3.12-alpine` webhook sink on 9099); drill
`deploy/compose/alert-drill/alert_drill.sh`; transcript
`docs/evidence/grafana-alerting-drill.log`.

The finding is a usage trap, not an engine bug, and nothing surfaces it.
**`reduce: last` is positional** — it takes the last row of the frame, it
does not sort, and it does not know which column is time — so a rule
whose SQL ends `ORDER BY time DESC`, the natural phrasing, thresholds the
*oldest* row in the window. Measured: a window holding 10 s then 100 s
against `gt 50` sat in Normal reporting `health: ok` indefinitely;
flipping that one word to `ASC` fired on the next tick. A panel on the
same SQL renders fine because a panel never reduces, and rule history is
an unbroken green line either way. The provisioned group therefore
carries a deliberately-`DESC` rule asserted *not* to fire beside the real
one, so a green drill separates "alerting works" from "nothing can
fire"; mutation-verified (ASC→DESC ⇒ phase E fails, exit 1, and F/G
report SKIP not PASS). Second trap: the query model field is `rawSql` —
`query`/`expr` reach the server as an empty statement and the error
reads like an engine fault. The drill refuses a dirty table, because
`/api/sql` is read-only and there is no delete-database route, so it
cannot clean up after itself and stale rows would silently invert the
discriminator. `/metrics` + Alertmanager remains the surface for *node*
health — it answers from atomics when the query path is what broke. The
short version is on `site/docs/reference.html` under Client
compatibility; keep the two in step.

### Changed — the checkout directory finally matches the product name (2026-08-22)

`TimelordDB/` → `TimeLakeDB/` on disk, thirteen days after the rename
in the code. Sibling repos' path defaults (Catchment, Gauge, Riverkeeper,
including Riverkeeper's CI checkout path) moved the same day. Historical
records keep the old spelling where they earned it: Gauge's
`timelorddb-*` results, `docs/evidence/**`, `ops/logs/**`. For the
record: something on the machine held a handle on the directory object
itself — every child including `.git` renamed freely — so it was a move
of contents into a fresh directory. Probe the children first; it
localises that kind of lock in one pass.

### Changed — DataFusion 55, which removes the thrift dependency entirely (2026-08-21)

Dependabot had an open medium against `thrift` 0.17.0 (excessive-size
memory allocation, fixed in 0.23.0). We do not depend on it directly:
DataFusion 54 pulls parquet 58.4.0, which pulls thrift.

`cargo update -p thrift --precise 0.24.0` cannot work, and says so —
parquet 58.4.0 requires `thrift = "^0.17"`, which for a 0.x crate means
`>=0.17.0, <0.18.0`. Nothing in the 0.2x range satisfies it.

parquet 59.2.0 does not depend on thrift **at all** — not a newer thrift,
none — and DataFusion 55 uses parquet 59.2.0. So the advisory closes by
removing the dependency rather than patching it, which also means it
cannot come back with the next thrift CVE.

The upgrade is three version strings:

- `datafusion` 54 → 55 (workspace)
- `arrow`, `arrow-flight` 58 → 59 in `crates/flight`, which was the only
  real work: DataFusion 55 brings arrow 59, and the flight crate pinning
  58 put two incompatible `arrow_schema::Schema` types in one tree. Two
  compile errors, both the same cause.
- one renamed method: `set_column_bloom_filter_ndv` →
  `set_column_bloom_filter_max_ndv`

No API churn in the query path — `SessionContext`, `TableProvider`, the
plan split, the custom `LazyTable` and the SEC-2 mandatory-predicate hook
in `scan()` all compiled unchanged, which was not what timelakedb#26
predicted.

Query latency is unchanged. `crates/query/tests/floor_breakdown.rs`,
same machine, before → after: session build 0.162 → 0.174 ms, planning
0.335 → 0.367 ms, execute+collect 1.766 → 1.620 ms, whole path 2.703 →
2.442 ms. Single runs on a box that also runs four production containers,
so read that as flat, not faster.

`cargo tree -i thrift` now reports no such package. That is the acceptance
test.

### Added — the compactor role, built and deliberately not startable (2026-08-21)

C2 phase 5a. `TIMELAKE_ROLE=compactor` runs rewrite work and nothing else:
no writes, no queries, no buffer of its own. It tails the catalog, then
compacts, applies tombstone rewrites and enforces retention on a 30 s
cadence, and serves `/health`, `/ping` and `/metrics` — nothing more.

Two things it must do that a single `all` node does not.

It **tails the catalog**. `compact_once` reads the in-memory file list,
which advances only on a node's own commits, and a compactor commits
nothing until it compacts. Without tailing it would work forever from the
list it booted with, choosing partitions that no longer exist. The commit
fence makes that safe; it does not make it useful.

It is **read-only for writes**. It has no WAL and no client should be
pointed at it, so a stray write is a misconfiguration and is refused
rather than half-accepted into a buffer nothing will flush.

The HTTP surface is built additively (`timelake_api::maintenance_app`)
rather than by removing routes from the data plane. A route removed by
subtraction returns the moment somebody adds one to the main router
without thinking about this caller.

**`Role::implemented` still refuses `compactor`, on purpose.** The reason
changed rather than expired: it is no longer "not built yet", it is that a
second compactor is only *efficient* once work-avoidance exists. The
commit fence already makes it *correct* — a merge whose inputs were
replaced is refused. Two compactors racing every partition would do double
the IO to land half the merges.

`deploy/compose/timelakedb-compactor.yml` is committed and documented as
not starting today, because a role nobody has tried to launch is a role
nobody knows is wired correctly. Verified both ways: with the gate shut the
container exits 2 naming the reason; with the gate temporarily open (local,
uncommitted) the compactor came up, tailed to catalog head 5, logged
`compaction pass partitions=1`, reported `timelake_compactions_total 1`,
and returned 404 for `/api/v3/write_lp` and `/api/sql`.

### Fixed — two compactors could both commit a merge of one partition (2026-08-21)

Groundwork for the compactor role split (C2 phase 5). Not reachable today,
because `Role::implemented` refuses to start a second compactor — which is
exactly why it is worth fixing before that refusal is lifted.

Catalog commits are a CAS on the next manifest sequence, so no commit can
overwrite another. That guarantees no commit is *lost*; it does not
guarantee a commit is still *valid* when it lands. Two compactors merging
one partition each read files F1..F4, each produce their own merged file,
and each commit. The CAS serialises them, the loser replays the winner's
entry and retries with its own original entry, its removals find nothing
left to remove, and its merged file is added anyway. The partition ends up
holding both merges: every row twice, from the mechanism designed to make
concurrent writers safe.

`Catalog::commit_replace` is the fence. Inside the same critical section,
after catching up to the true head, every path being removed must still be
present; if any has gone, another writer already replaced these inputs and
`AlreadyExists` is returned with the missing path named. No manifest entry
is written, so a refusal does not burn a sequence number. Compaction now
commits through it, and a refusal is treated as an ordinary outcome — the
redundant merge is handed to the deferred GC, `timelake_stale_merges_total`
is incremented, and it is deliberately not counted as a compaction.

**This is not the singleton lease the roadmap called for, and that is on
purpose.** A lease is a promise about wall-clock time across machines:
under skew or a long GC pause two nodes can both believe they hold one,
and then nothing catches the commit. It also has a failure mode this does
not — a holder that dies mid-compaction leaves a lease nobody can break,
turning a crash into a partition that silently stops compacting. The fence
checks the thing that actually has to be true, at the instant it has to be
true, using state the commit already holds a lock on. A lease on top would
avoid the wasted merge; it is an optimisation, and its failure is safe
because of this. `timelake_stale_merges_total` is what will say whether it
is worth adding.

### Fixed — duplicate rows could be served forever, because three files never reach four (2026-08-21)

A partition was compacted only when it held `compact_min_files` files
(default 4). Duplicate primary keys from different nodes collapse in
exactly one place — the cross-file last-write-wins pass inside a
compaction — so twins sitting in a partition of two or three files never
met, and the querier unioned both copies indefinitely. C4 measured 202,000
rows where 200,000 were written and 200,000 were distinct: a COUNT
inflated with full confidence, the inverse of what this database is for.

Compaction now has a second, independent trigger: any two files in a
partition whose time ranges intersect, regardless of count. Normal ingest
never produces those — timestamps advance, so each flush covers a later
range than the last. It takes a replay, a late arrival, or a crash-window
recovery, which is precisely the set of events that produces twins.

The overlap test is **strict** (`prev.max > next.min`), and that detail is
the difference between a fix and a regression. A tick-aligned fleet has
every host reporting at the same instant, so a flush boundary landing
mid-tick puts one timestamp in both files. Under a non-strict test that
is an "overlap" at nearly every boundary, and the engine would spend its
life rewriting files containing no duplicates at all.

Known gap, accepted deliberately: two files that each hold rows at exactly
one identical timestamp and nothing else touch at a point and are caught by
neither branch. Closing it costs more than the disease — a file holds up to
`flush_rows` rows, so every row in it would have to share a nanosecond.
Asserted in the tests so it stays visible.

Overlapping partitions are compacted ahead of merely-numerous ones: one is
serving wrong answers now, the other is only slower.

This fixes the mechanism. `FINDING_rebalance_duplicates_replayed_writes.md`
stays open until Catchment's C4 scenario — which needs a real cluster and an
undrained rebalance — has been re-run.

### Fixed — the admin console was still wearing the old brand (2026-08-21)

`crates/api/src/admin_ui.html` shipped the pre-rebrand palette: gold
accent `#D4AF37`, navy `#0B1320` (one digit off the real `#0B1220`), plus
an old blue and mist. The site, the docs and the Grafana dashboard moved
months earlier. The console is the only one of those a customer opens.

`--gold` had no direct replacement, which is why it outlasted the rebrand.
It was doing two jobs: brand accent on the heading, and a caution stripe on
`.warn`. The first became `--teal`, matching the site. The second became a
new semantic `--amber` alongside the existing `--red` and `--green` —
painting a warning with the brand accent makes every caution box read as a
heading, and semantic colours should not move in a rebrand at all.

Hand-mixed `rgba()` values moved with their tokens. Those drift silently:
`rgba(230,232,236,…)` was mist, `rgba(212,175,55,…)` was gold, and the role
pill's `#93b4fd` was a light blue mixed by eye, now `var(--sky)`.

`crates/api/tests/console_palette.rs` pins all of it — brand tokens match
`CLAUDE.md`, retired values cannot reappear (including as `rgb()` triples),
`--gold` is gone by name, and `.warn` may not use a brand colour. Verified
red against the pre-change file and green after, because a test only ever
seen passing says nothing about what it catches.

No behaviour, API or data-path change.

### Fixed — a retention policy no longer deletes tables it was not pointed at (2026-08-19)

**This is a data-loss fix.** `enforce_retention` matched a policy on table
name and ignored `FileMeta::db`, so a window an operator set on one
database's table quietly expired **every same-named table on the node**.
Set `metrics` to 7 days in `poc` and `_system.metrics` — or a tenant's —
went with it. A deletion control doing more than it was told is the one
direction a deletion control must never fail in.

- **Policies are now scoped `(db, table)`**, with `"*"` as an explicit
  all-databases wildcard. Most specific wins: an exact-database policy
  overrides a wildcard for the same table, so one database can be carved
  out of a broad rule instead of forcing a choice between all and nothing.
- **Existing stored policies migrate to the wildcard**, which preserves
  their behaviour *exactly*. That is the only safe reading: they really
  did apply everywhere, and narrowing them on upgrade would silently stop
  expiring data an operator had asked to have deleted. The migration logs
  a warning naming what happened. `retention.json` gains a version field;
  a v1 document still loads.
- **`db` is required** on `PUT /admin/retention`, and `DELETE` takes
  `/admin/retention/{db}/{table}`. Omitting the scope used to be the only
  option and silently meant "everywhere"; defaulting it now would keep
  that footgun loaded for every policy written from here on.
- **The audit target names the scope** (`poc.pipeline_events`), so the
  trail distinguishes expiring one database's table from expiring that
  table everywhere.
- **Widening a policy to `"*"` counts as destructive** and needs `admin`,
  even when the window is unchanged — for the databases it newly covers,
  introducing a window is exactly what it is.
- The console lists the database per policy, marks wildcards in red, and
  confirms before applying one. `TIMELAKE_RETENTION` accepts
  `db.table=90d` as well as the existing `table=90d`, which still means
  every database.
- This also unblocks bounding `_system` (`docs/CONSOLE.md` §7.6): setting
  retention there was previously unsafe because the policy would have
  reached user tables of the same name.

### Added — the database can answer how fast it is (U2, 2026-08-18)

Drill: `docs/evidence/u2-console-drill.log` (37/37, every dashboard panel
executed against a live node).

- **Queries are timed.** The exposition previously had no histograms and
  timed nothing, so the Query view of `docs/CONSOLE.md` §7.3 could not be
  drawn and "Shape A got slow" was answerable only by running Gauge. Added
  `timelake_query_duration_seconds` and
  `timelake_query_admission_wait_seconds` (histograms, buckets dense
  either side of the 250 ms PR-3 target and coarse in the tail),
  `timelake_query_in_flight` / `_queued`, and
  `timelake_queries_total` split into `_timeouts_`, `_refused_` and
  `_failed_`. Instrumented in `run_sql_env` — the single production call
  site — so HTTP and Flight SQL cannot drift into different accounting.
- **A refused statement is not a failure.** Refusing a `COPY` is the P0-2
  read-only guard working; counting it as an error would make a healthy
  node look broken whenever a client probes it, so the two are separate
  counters.
- **Storage and lifecycle**: `timelake_storage_bytes{db,table}`,
  `_storage_rows{db,table}`, `timelake_files{level}`,
  `timelake_flush_lag_seconds`, `_compaction_lag_seconds`,
  `timelake_gc_pending_files`, `timelake_uptime_seconds`,
  `timelake_build_info`, and `timelake_write_rejected_total{reason}` —
  split so WAL backpressure (yours to fix) is distinguishable from a
  malformed request (the client's). Lag gauges report the **whole uptime**
  before their subsystem has ever run, because a zero would read as "just
  ran, healthy" at exactly the wrong moment.
- **Self-monitoring**: the node samples its own `/metrics` into
  `_system.metrics` each maintenance tick and writes one row per query
  into `_system.queries`, read back by Grafana over Flight SQL
  (`deploy/grafana/`, `deploy/compose/timelakedb-console.yml`). Storing
  exact per-query durations means p50/p95/p99 are measured rather than
  estimated from bucket bounds, and are sliceable by database, outcome and
  client identity. The sampler **converts the exposition** instead of
  keeping a second metric list, so §13's U2 gate holds by construction and
  new metrics are self-monitored the day they are added.
- **Monitoring yields to the workload.** The query-path observer only
  formats a line onto a bounded queue — never writes, never blocks — and
  drops when full, with drops counted (`timelake_selfmon_dropped_total`)
  because silent loss would make the console lie by omission at the
  busiest moment. `_system` rows are excluded from
  `timelake_lines_written_total` so Gauge baselines stay comparable.
- `/metrics` is **unchanged and remains the alerting surface**: it answers
  from in-memory atomics with no query path, so it still works when the
  stored copy cannot be read. Known limits are documented rather than
  discovered: a metric never emitted has no column in `_system` (so panels
  for TLS/S3/KMS/cluster metrics must be added per deployment), a CL-3
  querier stores nothing, and `_system` gets no default retention because
  `enforce_retention` matches table name **ignoring the database**.
  *(That last limit was itself a data-loss bug and was fixed the next day —
  see the 2026-08-19 entry above. `_system` can now be bounded safely.)*

### Added — a client certificate now authorizes on `/api/sql` too (2026-08-18)

- **HTTP carries a verified peer identity.** `axum-server` owns the HTTP
  accept loop, so `/api/sql` previously requested and verified a client
  certificate and then authorized nothing with it — the SEC-3 narrowing
  property held on Flight SQL and not on HTTP.
  `crates/server/src/tls_identity.rs` wraps `RustlsAcceptor` with an
  `Accept` that reads the subject CN off the completed handshake and
  layers `Extension(PeerIdentity)` onto the service, **once per
  connection** rather than per request. Tributary's L4 client certificate
  now authorizes something on the write path instead of only proving a
  handshake. Want mode still grants nothing on its own (SECURITY.md
  exposure 9); the anonymous path is unchanged, so this is additive.
- **Drilled 15/15 against a real handshake** (2026-08-19,
  `docs/evidence/http-peer-identity-drill.log`). Three clients make the
  identical claim `ops,audit` against rows labelled public / `ops` /
  `audit`; only the certificate differs. Anonymous sees 3 rows (want mode
  unaffected), an identity granted `[ops]` sees **2** — it saw 3 before
  this change — and an identity with no grants recorded sees 3, because
  `None` means "no policy", not deny-all. The drill also pins that the
  restriction is enforced in the scan rather than the projection, so no
  aggregate can count a withheld row. It runs from a Linux container on
  the compose network: Windows curl is schannel and cannot present a PEM
  client certificate, so a host-side run would exercise the anonymous
  path three times and pass.

### Added — log rotation, and audit segments that stay verifiable (2026-08-18)

- **The audit trail rotates**, into ordered segments named by the last seq
  they contain (`TIMELAKE_AUDIT_ROTATE_SIZE`, default 64 MiB, and
  `TIMELAKE_AUDIT_ROTATE_EVERY`). The hash chain runs **through** the
  boundaries: `read_all` concatenates segments in order, so `?verify=1`
  still walks the whole trail and **removing a segment file breaks
  verification exactly as editing a record does**. Reopening after a
  rotation recovers the head from the entire trail, never from the live
  segment alone — the latter would hand the next record a genesis
  `prev_hash` and silently split the chain in two.
- **Audit retention deletes nothing by default.**
  `TIMELAKE_AUDIT_RETAIN_DAYS` is opt-in, and clamped to the documented
  90-day floor even when set lower (`docs/CONSOLE.md` §5.4), so the
  retention control cannot erase the record of its own use.
- **The system log can rotate itself** — `TIMELAKE_LOG_FILE` plus
  `_ROTATE_SIZE` / `_ROTATE_EVERY` / `_KEEP`. Unset keeps the previous
  behaviour: stdout, captured and rotated by systemd or Docker. Deliberately
  separate from the audit trail, which is evidence and has its own policy.
- No new dependencies. `KiB` (1024) and `KB` (1000) are both accepted and
  are not treated as the same number.
- **Drilled against a live node** (`docs/evidence/audit-rotation-drill.log`,
  9/9): 40 admin mutations spanning 5 segments, `?verify=1` still intact
  after rotation, and a removed segment file reported as a break naming its
  seq and reason. The unit tests exercise `AuditSink` directly; this proves
  the endpoint an auditor actually uses tells the truth about a rotated
  trail.

### Fixed

- Four `clippy -D warnings` failures in the R-1 and P1-2 code that CI would
  have caught had it been running: two collapsible `if`s in the server and
  audit crates, a redundant closure in the query crate, and two more in
  `tests/delete.rs`. Found by finally running the workspace-wide command CI
  uses rather than targeted per-crate checks.

### Added — Linux packages for releases (2026-08-17)

- **`.deb` and `.rpm`, attached to every tagged release.** A `v*` tag runs
  `.github/workflows/release.yml`, which builds both packages and uploads
  them with `SHA256SUMS`. One `packaging/nfpm.yaml` produces both formats, so
  the two cannot drift; the whole build runs in containers, so a laptop and
  the runner produce the same artifact.
- **The packages are inert on install, deliberately.** The shipped
  `/etc/timelakedb/timelakedb.env` binds `127.0.0.1` only and the systemd
  unit is installed but not started — with an unauthenticated data plane
  (SECURITY.md exposure 1), `apt install` must not put a database on the
  network before anyone has read the config. The unit runs as a shell-less
  `timelake` account under `ProtectSystem=strict` with `/var/lib/timelake` as
  its only writable path, mirroring the container's posture (exposure 4).
- **Built against glibc 2.31 on purpose.** Linked on a current image the
  server requires `GLIBC_2.39`, which RHEL 9, Debian 12 and Ubuntu 22.04 do
  not have — it would install cleanly and then fail to start. Building on
  Debian 11 covers RHEL/Rocky 9+, Debian 11+, Ubuntu 20.04+ and Amazon Linux
  2023; `build.sh` fails if the binary ever needs more than the metadata
  promises.
- **`packaging/verify.sh` installs and runs the packages** on Debian 12,
  Ubuntu 22.04, Rocky 9 and Amazon Linux 2023, and the release workflow runs
  it. It found two defects on its first execution: `apt remove` deleted
  `/var/lib/timelake` (a package-owned empty directory is removed with the
  package), and every AL2023 install failed in the `PREIN` scriptlet because
  AL2023 ships without `shadow-utils`.

### Security — three open exposures closed (2026-08-15)

Driven by Riverkeeper R6; each shipped with a control in that repo that goes
red if the fix regresses. These are the P1-4/P1-3/P1-5 items, taken early.

- **SEC-5 (exposure 5) — query errors are sanitized.** A query that failed
  to plan or execute returned the DataFusion error verbatim, disclosing
  table and column names (a bad column enumerated the whole schema). It now
  returns one opaque `query could not be executed (ref: q-XXXXXXXX)` on both
  `/api/sql` and Flight SQL, with the full error logged server-side against
  that ref. Sanitized at the one shared execution point
  (`crates/query` `run_sql_env`). = **P1-4 error redaction**.
- **SEC-6 (exposure 6) — per-client query concurrency cap.** The admission
  semaphore bounded total concurrency but let one client take every permit.
  A per-client cap now sits in front of it: past its cap (default 4 of the
  global 6) a client is refused — HTTP 429 / Flight `ResourceExhausted` —
  keyed by data-plane token when present and by network origin otherwise, on
  both surfaces (`crates/server/src/ratelimit.rs`). Metric
  `timelake_query_rate_limited_total`. = **P1-3 per-client rate limits**.
- **SEC-8 (exposure 8) — the WAL is encrypted at rest.** At-rest encryption
  covered the object store but not the local WAL, so a stolen volume gave up
  the unflushed writes in cleartext. The WAL now encrypts with the SAME
  envelope key: a per-file data key wrapped by the KEK in a `TLDW` header,
  AES-256-GCM frames, plaintext passthrough on upgrade, and replay that fails
  CLOSED on a missing/wrong key or a frame that fails authentication
  (`crates/wal`). Covers the durable replica WAL. = **P1-5 WAL encryption**.

### Changed — the intra-cluster port is never published (exposure 10)

The cluster listener (`TIMELAKE_CLUSTER_ADDR`, `:1965`) serves live rows with
no data-plane token check and no SEC-2 visibility filter, so reaching it is
read access to the bucket — it belongs on the private network only. The
shipped cluster compose files no longer publish it to a routable interface;
the cluster drills reach it via `docker exec`. Surfaced by Riverkeeper R4.

### Changed — dependencies refreshed to latest within-semver (2026-08-15)

`cargo update` across the workspace (~36 crates). The `thrift` advisory
(GHSA-2f9f-gq7v-9h6m, medium/availability, deferred and unreachable — we
parse only Parquet we wrote) stays open, **blocked upstream**: arrow-rs
dropped the external thrift crate in `parquet` 59, but `datafusion` 54
(latest) pins `parquet ^58.3.0` and rejects 59. Clears with `datafusion` 55.

### Fixed — a querier returned every row N times under concurrent reads (2026-08-13)

Found by Catchment's `read-gate` scenario on its first real execution.
Detail: `docs/FINDING_catalog_catch_up_race.md`.

- **`Catalog::catch_up()` was not atomic between reading the head and
  publishing it.** The `files` mutex covered only the apply, so N concurrent
  callers all read the same stale head, all selected the same manifest
  entries, and all applied them — and `apply_entry` pushed unconditionally.
  One manifest entry became N copies of the same file, and every later query
  scanned it N times.
- A querier folds the log forward on **every** query, so concurrent queries
  meant concurrent `catch_up`. Measured on a live cluster from a single
  flush: 10,000 rows on the ingester, 80,000 on one querier, 60,000 on
  another. `COUNT(DISTINCT)` was correct throughout — nothing was lost,
  everything was counted repeatedly, which falsifies the CL-3 claim that
  counts are exact seconds after ingest.
- `catch_up` now takes `commit_lock` for the whole sequence, as `commit`
  always has; the body moved to `catch_up_locked` so the commit retry path,
  which already holds that lock, does not deadlock on a non-reentrant mutex.
  `apply_entry` additionally dedups by file path — folding a log into a set
  should be idempotent whoever calls it.
- The over-count was the dangerous direction: a short count is visibly
  wrong, while `COUNT(*)` returning eight times the truth reads as a healthy
  system with more data than expected.

### Added — the intra-cluster listener bounds what a querier can cost an ingester (2026-08-13)

Design: `docs/P1-1_DESIGN.md` D2.

- **`/internal/v1/live` and `/internal/v1/snapshot` are now bounded by
  `TIMELAKE_INTERNAL_MAX_CONCURRENT` (default 8), refusing with 503 rather
  than queueing.** A querier unions every ingester’s live buffer on each
  query, so read load on the query tier arrives as work on the ingest tier,
  and an ingester’s real job is taking writes. The permit is *tried*, never
  waited on: queueing would turn a refusal into latency, and the querier’s
  own 30 s deadline would hold an ingester for the whole of it.
- Refusing is the honest outcome and the querier already models it — a failed
  snapshot makes it refuse the query rather than answer from an incomplete
  cluster. Refusals are counted in `timelake_cl3_reads_refused_total`, so a
  ceiling that is set too low reads as a ceiling rather than as a broken peer.
- **`replicate` and `health` are deliberately left unbounded.** Throttling a
  peer’s write path is the stall D1 exists to prevent, reached from the other
  side; and health has to answer precisely when the node is saturated.

### Fixed — bodies over 2 MiB were refused, and replication went quiet (2026-08-13)

- **FR-1 requires batches of 10 MB and up; anything over 2 MiB got 413.**
  axum’s `Bytes` extractor carries a 2 MiB default limit and neither
  listener overrode it. On `/write` that is at least loud. On a peer’s
  `/internal/v1/replicate` it was not: the replicator reads any non-2xx as
  “peer not durable”, so a large frame dropped the node into degraded mode
  while the write succeeded locally — “durable on two nodes” stopped holding
  for exactly the batches most worth replicating, under an alarm that looked
  unexplained.
- Now `TIMELAKE_MAX_BODY_BYTES`, default 32 MiB, applied to **both** routers
  from one config value. A public limit above the internal one would accept
  writes their own replica refuses, so they are deliberately not separate
  knobs.
- Pinned by `a_body_over_two_megabytes_is_accepted_on_both_planes`, which
  exercises the public write and the replication frame in the same test.

### Changed — a slow replication peer can no longer stall ingest (2026-08-13)

Design: `docs/P1-1_DESIGN.md` D1.

- **The CL-2 replication timeout is now `TIMELAKE_REPL_TIMEOUT_MS`, default
  250 ms, down from a hardcoded 5 s.** Replication is synchronous before the
  ack, so that timeout is a per-write latency ceiling. A *dead* peer was
  always handled — it trips to degraded at once and availability holds — but
  a *slow* peer tripped nothing and simply multiplied every write's latency.
  At the reference workload's ~232 events/s a five-second stall is an ingest
  outage rather than a hiccup, and it is reachable from ordinary read load:
  a querier unions the ingesters' live buffers, so an expensive query slows
  an ingester, whose peer then blocks on every write. Sub-second by design,
  so slow and dead collapse into the same case.
- Pinned by `a_stalled_peer_costs_the_timeout_and_no_more`, which stalls a
  real socket rather than closing it — the case a dead-peer test misses.

### Fixed — the flush handover could lose a row to a reader (2026-08-10)

Evidence: `docs/evidence/flush-handover-atomicity.log`.

- **An acknowledged row could read as missing during a flush.** Rows moved
  from the live buffer to the mid-flush holding area in two separately
  locked steps, so a query landing between them found them in neither —
  and not yet in the catalog either, for as long as the object write took.
  On a local disk that window was microseconds; against an object store it
  is the length of a `put`. The handover is now one critical section, and
  every reader takes the same locks in the same order, so a query sees the
  rows in exactly one place: never neither (a vanish), never both (a
  double count). Found while making CI trustworthy for P0-1 — the test
  that catches it passed in the full binary and failed when run alone,
  which is a race, not a flaky test.
- **Cost, measured rather than asserted:** building the snapshot now
  happens under the ingest gate, so laptop-scale ingest goes from ~600k to
  ~571k lines/s — **≈5%**. A cheaper design (move the buffer rather than a
  snapshot of it) is recorded in the evidence log as the follow-up.

### Added — C2 phase 4: the stateless querier (CL-3) (2026-08-10)

Drill: `docs/evidence/cl3-querier-drill.log` (19/19), rig
`deploy/compose/timelakedb-cluster-s3.yml`.

- **`TIMELAKE_ROLE=querier` — reads scale and fail independently of
  writes.** A querier owns no data: it replays the catalog from the shared
  object store, tails the manifest log, and answers SQL and Flight SQL.
  Killing one loses nothing; a fresh container with an empty disk rebuilds
  its whole view from the bucket (CL-4, drilled).
- **Freshness is not optional.** Seconds after ingest the rows are still in
  an ingester's memory — in no file and no catalog — and AT-2 demands exact
  counts there. Ingesters therefore serve their live buffers over the
  intra-cluster listener as **Arrow IPC** (`/internal/v1/snapshot`, plus
  `/internal/v1/live` for what a node is holding), and a querier's table is
  the union of those snapshots and the store's files, exactly as a single
  node unions its own buffer with its own files. Arrow IPC keeps
  dictionary-encoded tag columns encoded across the wire (FR-2); line
  protocol or JSON would hand the querier the memory shape this database
  exists to avoid.
- **No vanished rows across a flush.** Every internal response carries the
  serving node's catalog head, read *after* its buffers; the querier folds
  the manifest log forward to that watermark before reading any file list.
  A batch that has left an ingester's buffer is therefore guaranteed
  visible as a file. The residual race is the one the single-node path
  already accepts: a transient duplicate, never a vanish. Steady state
  costs no extra store calls.
- **A partial answer is refused, not returned.** If an ingester is
  unreachable its live rows are missing and every COUNT is silently short,
  so the querier fails the query with a named error and counts
  `timelake_querier_refusals_total` — **alert on it**. This is deliberately
  the opposite of the write path's PR-7 trade: a degraded write is still
  honest about what it stored; a degraded query lies.
- **Queries now route.** `/api/sql` on the router is forwarded to a
  querier, round-robin, falling through to the next on a transport failure
  so one dead querier costs a retry rather than half the queries. A router
  with no queriers still answers 501 rather than guessing at an ingester.
  Credential headers travel with the request — the querier is where SEC-2
  visibility and SEC-4 data auth are decided.
- **A querier takes no writes** — 501 with a named reason, not 400 (the
  request is fine) or 500 (nothing is broken).
- Metrics: `timelake_querier_{ingesters,snapshot_fetches_total,
  snapshot_rows_total,snapshot_errors_total,refusals_total,catchups_total}`,
  `timelake_catalog_head`, and `timelake_router_{queries_forwarded_total,
  query_errors_total,queriers}`.

### Fixed

- **A table written moments ago could read as "not found" on a querier**
  for up to a second — it exists in no catalog and no local buffer, and the
  live view was refreshed only by a background tick. The live view is now
  refreshed on the query path, before the table list is taken. Caught by
  the drill, not by a unit test.
- **A restarted peer left dead sockets in a querier's connection pool**, so
  the first snapshot read after a peer bounce failed and surfaced as "the
  cluster is incomplete". Snapshot reads are idempotent and now retry once.
- **The schema registry never refreshed after boot.** It was built from
  file footers at startup only, so a column added to a table afterwards
  read as absent — silently, on every row — on any node that does not write
  (i.e. every querier). It is now rebuilt whenever the catalog advances,
  and only for tables whose newest file changed.
- **Reading a file's schema no longer fetches the file.** The registry used
  `get` + full decode to reach `batch.schema()`; it now reads the footer
  through the metadata cache. Tolerable on local disk at boot, wrong on S3
  in a loop.

### Added — C2 phase 3: the router (stateless write sharding) (2026-08-10)

- **The router is the single public write endpoint** the bench adapter,
  Telegraf and Grafana keep seeing (FR-8/FR-9). `TIMELAKE_ROLE=router`, it
  holds no data and opens no engine — it hashes each line's
  `(db, measurement)` to one ingester and forwards that shard. The chosen
  ingester becomes the table's primary and replicates to its CL-2 peer, so
  durability is unchanged; the router adds distribution, not a new failure
  mode.
- **Atomicity preserved across shards.** The whole line-protocol body is
  validated before any shard is forwarded, so a poison line writes zero of
  the batch. A shard forward that fails for an infrastructure reason (an
  ingester down, backpressure) is returned to the client for an idempotent
  retry (LWW dedup).
- **Queries are not routed yet** — a query is only correct once a querier
  unions every shard from the shared store, so `/api/sql` on the router
  returns 501 until CL-3 (phase 4). Queries go direct to an ingester for
  now.
- Sharding is FNV-1a over `db\0measurement` mod N (deterministic across
  restarts, unlike the default hasher), with ingesters sorted by id so a
  table always lands on the same node. Discovery's `NodeInfo` gained a
  `data_address` (the public write port the router forwards to), carried in
  `TIMELAKE_PEERS` as `id=role@cluster_addr|data_addr`. Metrics
  `timelake_router_{forwarded,forward_errors,rejected,ingesters}`.
- **`role=all` and `role=ingester` are unchanged** — the router is a
  separate `main` branch that never touches the engine path. 141 tests
  (+5 router, +1 cluster); drilled live 8/8 (`docs/evidence/router-sharding-drill.log`):
  12 tables sharded across the pair, exact accounting, real distribution,
  atomic poison rejection, 501 for queries.

### Added — C2 phase 2: CL-2 ingester WAL replication (2026-08-10)

- **Zero acknowledged-write loss on a single node failure** — the first
  step of P1-1 that actually delivers "survives node loss." Two ingesters
  pair up (`TIMELAKE_ROLE=ingester`, peer from `TIMELAKE_PEERS`): each
  write is replicated to the peer's durable **replica WAL** before the
  204, so an acknowledged write is durable on two nodes.
- **Degraded mode, loudly (PR-7).** A down peer does not fail writes — the
  node keeps accepting on local durability, raises `CL2_REPLICATION_DEGRADED`
  once, sets `timelake_cl2_degraded`, and clears it when the peer returns.
  The alarm states the cost honestly: while degraded, a second failure can
  lose the un-replicated writes.
- **Recovery** replays the peer's replica WAL into the engine and flushes;
  overlap with rows the dead peer already flushed is safe because LWW
  dedup (FR-5) makes it idempotent. The replica frames are dormant (not
  applied) in steady state, so a node never double-flushes its peer's live
  rows, and the replica WAL survives the recovering node's own restart.
- New: the `internal_router` (an ingester's private listener on
  `TIMELAKE_CLUSTER_ADDR`: `/internal/v1/replicate`, `/recover`, `/health`)
  and a `Replicator` seam. Transport is plaintext HTTP at C2, moving to
  required-mTLS at C3 (the verifier is shipped). Metrics
  `timelake_cl2_{replicated,degraded,degraded_events,replica_frames,recovered}_*`.
- **`role=all` is byte-for-byte unchanged** — a lone node has no peer, so
  no replicator, no replica WAL, no internal listener, no CL-2 metrics.
  Pinned by a unit test and a live smoke.
- 135 tests (+4 in-process replication); drilled live 12/12 — degraded
  mode and SIGKILL-an-ingester-zero-loss (`docs/evidence/cl2-replication-drill.log`).
- Deferred: automatic health-triggered failover (recovery is explicit
  here); the router (C2 phase 3).

### Added — C2 phase 1: cluster roles + discovery seam (2026-08-10)

- New `timelake-cluster` crate: the `Role` enum (`TIMELAKE_ROLE`, default
  `all`) and the `Discovery` seam with a static backend
  (`TIMELAKE_NODE_ID`, `TIMELAKE_CLUSTER_ADDR`, `TIMELAKE_PEERS` as
  `id=role@host:port`). This is the foundation the C2 replication phases
  build on — the first step of P1-1 (replication/HA).
- **`all` is unchanged**: the whole stack in one process, the default,
  bench and fixtures untouched. The specialised roles
  (`router`/`ingester`/`querier`/`compactor`) land one phase at a time,
  and **a role whose phase has not landed is refused at startup** with a
  named message rather than run half-built — no one deploys an ingester
  that does not replicate. A typo'd role is refused too.
- Design guard (CL-5) baked into the crate docs and its placement:
  discovery carries **no correctness** — a wrong or stale membership view
  can misroute or waste work but never corrupt state, because every
  durable commit goes through catalog CAS (C1). Nothing on the write or
  commit path consults it.
- 7 unit tests (role parsing + refusal, peer parsing incl. malformed/
  duplicate/blank, role-filtered peer selection, lone-node). `role=all`
  drilled live: writes/reads unchanged, correct boot log; `ingester` and
  a typo both refuse with `exit 2`.

### Fixed — P0-4: catalog commits are atomic against a second writer (2026-08-10)

- **Two writers on one object store can no longer lose each other's
  commits.** `Catalog::commit` picked the next manifest sequence from an
  in-process counter and wrote `catalog/manifest/{seq}.json` with a plain
  `put`; two writers replaying the same log computed the same `seq`, and
  the second `put` silently overwrote the first — its data files left
  orphaned in the store and invisible to every query. Latent on one node,
  active the moment a second writer appears (a botched restore, a stray
  container, day one of clustering).
- Commit is now **compare-and-swap on the next sequence key**
  (`put_if_absent`: S3 `If-None-Match`, local `File::create_new`). The
  loser of a race replays the winner's entries, folds them into memory,
  and retries at the new head — the manifest log becomes a true total
  order, every slot with exactly one writer. Bounded at 100 attempts
  (→ `ResourceBusy`); each retry catches up first, so honest contention
  converges in a few rounds.
- Metric `timelake_catalog_commit_conflicts_total` — 0 on a single writer,
  climbing means real contention, so it is visible rather than inferred.
- Drilled on **both** CAS mechanisms, which are different code: local
  hard-link (3 catalog tests, including the two-writer loss scenario and
  removals-survive-retry) and **real S3 `If-None-Match` via LocalStack**
  (a two-`Catalog`-on-one-bucket drill, `--ignored`).
  `docs/evidence/catalog-cas-drill.log`.
- Deferred to C2 (safe because maintenance is single-node until the role
  split): re-validating a commit against the new state on conflict, so a
  compaction whose inputs were concurrently retention-dropped aborts
  rather than resurrects dropped data.

### Fixed — P0-2: the data-plane SQL surface is read-only, container non-root (2026-08-10)

- **`POST /api/sql` and Flight SQL can no longer write files.** DataFusion's
  `COPY … TO '<path>'` wrote a Parquet file as the server process — verified
  against the pre-fix image, a root-owned file outside the data directory
  from one unauthenticated request. The surface is now read-only, enforced
  on the **logical plan** (not the query text, which a comment or
  `EXPLAIN ANALYZE` would defeat): `SELECT`/`SHOW`/`DESCRIBE`/`EXPLAIN` run,
  `COPY`/DDL/DML/session statements are refused, and the walk reaches inside
  `EXPLAIN ANALYZE` so a nested `COPY` cannot hide. Deny-by-default — the
  classifier matches every `LogicalPlan` variant with no wildcard, so a
  future DataFusion node fails the build rather than slipping through. HTTP
  and Flight SQL share the one enforcement point.
- **The container runs non-root under a read-only root filesystem.**
  `USER timelake` (uid 1000) in the image; the shipped compose sets
  `read_only: true` with a `tmpfs /tmp` and the data volume as the only
  writable mount. Defence in depth behind the SQL guard.
- Incidental fix: `CREATE`/`DROP TABLE` used to parse and return `[]` while
  doing nothing; they are now refused explicitly, which the roadmap called
  the correct outcome over silently succeeding at nothing.
- Drill: `docs/evidence/sql-sandbox-drill.log` (the exposure reproduced,
  then closed on both surfaces, nothing written, process non-root, rootfs
  read-only, reads untouched). +4 tests.
- **Upgrade note:** switching to uid 1000 means a data volume created under
  the old root image is root-owned and unwritable; the node exits with
  `open engine (recovery): Permission denied`. Chown the volume to
  `1000:1000` once, or start on a fresh volume.

### Added — SEC-4 phased: data-plane token authentication (2026-08-10)

- `TIMELAKE_DATA_AUTH=off|optional|required` turns on token
  authentication on the data plane. **Default `off`** does not examine
  `Authorization` at all — today's compatibility contract, so a Telegraf
  migrated from InfluxDB with a leftover token keeps writing unchanged.
  `optional` serves anonymous callers but refuses a presented-but-invalid
  token (fail loud on day one); `required` refuses any request without a
  valid one, on both `:1963` and `:1964`.
- **One token, three spellings, because that is what stock clients send**
  (`docs/evidence/data-auth-client-probe.log`): `Bearer` (Grafana's
  Flight SQL token field, and Tributary), `Token` (Telegraf
  `influxdb_v2`), `Basic` with the token as the password (Telegraf v1).
  Grafana's basic-auth toggle and custom headers are HTTP-only and never
  reach gRPC, which is *why* the token field is the mechanism rather than
  a preference.
- New `crates/auth/src/token.rs` and `guard.rs`: 256-bit OS-CSPRNG
  secrets stored only as SHA-256 digests (not Argon2id — a token has no
  brute-force surface, and a memory-hard KDF on the write path would be a
  self-inflicted RR-1 violation), scopes (`read`/`write`/`read_write`,
  not a total order — a shipper writes without reading back), database
  scoping, SEC-2 grants that *intersect* claimed authorizations, expiry
  and revocation. HTTP and Flight SQL enforce through **one** decision
  function; Flight re-authenticates at DoGet as well as planning, because
  a ticket is client-crafted.
- Console `/admin/tokens` (issue/list/revoke, admin-only, secret shown
  once and never re-listed) plus a page section.
- Metrics: `timelake_data_auth_mode` and the split
  `timelake_data_requests_{authenticated,anonymous,rejected}_total` — an
  operator flips `optional` → `required` on that measurement, not a
  guess, exactly as want-mode mTLS did.
- Drilled live end to end (`docs/evidence/data-auth-drill.log`) in a
  container, because in-process router tests cannot reach the Flight
  accept loop where the gRPC guard runs.
- Not done: Tributary presenting the token (P0-5).

### Added — SEC-3 (v2): optional client certificates in want mode (2026-08-10)

- `TIMELAKE_TLS_CLIENT_CA` turns on client-certificate verification in
  **want mode**: both listeners request a certificate, verify one if
  offered, and serve the connection either way. Grafana, Telegraf and
  the bench harness connect unchanged with no configuration.
- Trust anchors sit behind the same `ArcSwap` as the serving
  certificate and hot-rotate on the same trigger, with **dual-CA
  overlap** — a bundle carrying both outgoing and incoming anchors, so
  a CA roll does not require every client to change at one instant. A
  bundle that will not parse leaves the last-good anchors serving and
  raises the named alarm, exactly as a bad server renewal does.
- **The identity is the point, not the encryption.** A verified client
  certificate's common name reaches the query session over Flight SQL,
  where `QuerySession::resolve` intersects the caller's claimed SEC-2
  authorizations with what that identity is granted. Anonymous callers
  keep today's documented behaviour, so this narrows without breaking:
  the fix for SECURITY.md exposure 7 is additive rather than a flag day.
- Metrics: `timelake_tls_client_ca_anchors`,
  `timelake_tls_client_ca_last_reload_ok`,
  `timelake_tls_client_auth_mode`, and the split that makes want mode
  observable at all — `timelake_flight_connections_authenticated_total`
  against `timelake_flight_connections_anonymous_total`. Without those
  two, both paths return 200 and nothing tells an operator whether any
  client presents a certificate yet, so the decision to *require* one
  would have to be a guess.
- **AT-6 re-drilled under want mode** and extended into a real gate
  (`docs/evidence/at6-grafana-want-mode.log`, 11/11): the TLS compose
  stack gained a `grafana` profile whose datasource deliberately
  configures no client certificate, and all **58 panel queries** from
  the four fixture dashboards execute and return data through Grafana's
  own Flight SQL plugin while the server is asking every client for a
  certificate. A certificate-bearing client connects in the same run and
  is counted separately (1 authenticated vs 58 anonymous).
- **AT-7 remains 19/19** with client auth enabled
  (`docs/evidence/sec3-mtls-want-mode.log`). The drill itself needed a
  fix — it called `/admin/tls/reload` anonymously and SEC-4 had put that
  behind a session — so it now logs in and rotates the seeded credential,
  exercising the authenticated path instead of routing around it.
- Not done: `/api/sql` does not yet carry the identity; axum-server owns
  that accept loop and needs a custom `Accept` to surface peer
  certificates.

### Changed — renamed TimelordDB → TimeLakeDB (2026-08-09)

The project is now **TimeLakeDB**, and the name says what the architecture
is: immutable Parquet on object storage behind an Iceberg-style manifest
log, with compute replaceable (CL-1). Nothing has been released, so this
is a rename rather than a migration — but several of the strings are
user-facing contracts and they all moved together:

- **Crates:** `timelord-*` → `timelake-*` (all 15).
- **Environment:** `TIMELORD_*` → `TIMELAKE_*` (~20 variables, including
  `TIMELAKE_ADDR`, `TIMELAKE_DATA_DIR`, `TIMELAKE_OBJECT_STORE`,
  `TIMELAKE_KMS_KEY_ID`, `TIMELAKE_ENCRYPTION_KEY`).
- **Metrics:** `timelord_*` → `timelake_*` (~30 series). Renaming now
  costs nothing; after a release it would have broken every dashboard.
- **HTTP headers:** `X-Timelord-Authorizations` → `X-TimeLake-Authorizations`
  (SEC-2), `x-timelord-csrf` → `x-timelake-csrf` (SEC-4).
- **Paths:** `/var/lib/timelord/data` → `/var/lib/timelake/data`.
- **Harness:** `bench/backends/timelakedb.py`, backend key `timelakedb`,
  and the three compose targets (`timelakedb.yml`, `-tls`, `-s3`).
- **Brand:** logo, wordmark, site and all documentation.

Two things deliberately did **not** change:

- **The `TLDE1` encryption magic bytes.** It is a format version marker,
  not a brand string, and changing it would make every previously written
  object fail the magic check — routing it down the plaintext-passthrough
  path and returning ciphertext as data. A cosmetic rename is not worth a
  silent-corruption mode.
- **Historical evidence** under `docs/evidence/` and `ops/logs/`. Those
  record runs that actually happened under the old name; rewriting them
  would falsify the record.

`TLDB_*` / `tldb-*` identifiers (the backup script, drill variables, the
session cookie) were left alone — the abbreviation is true of both names.

### Added — SEC-4: authentication on the admin surface (2026-08-09)

- New `timelake-auth` crate: principals with `viewer`/`operator`/`admin`
  roles, Argon2id credentials, server-side sessions (cookie or bearer)
  with 30-minute idle and 12-hour absolute expiry, per-principal
  exponential backoff on failed logins. Principals persist through the
  `Store`, so they are envelope-encrypted with everything else (SEC-1).
- **Every `/admin/*` route now authenticates**, which closes SECURITY.md
  exposure 3a — the unauthenticated deletion control. Mutations from a
  cookie session additionally require a double-submit CSRF token and an
  Origin check. The `/admin/tls/reload` endpoint moved behind the same
  guard and now requires `admin`.
- **First run seeds `admin`/`admin`, quarantined**: it authenticates, and
  then the only route that answers is `POST /admin/password` — everything
  else returns `403 password_change_required`. Rotation invalidates every
  session for that principal, including the one that performed it. The
  replacement cannot be shorter than 8 characters, the username, or
  `admin`. `TIMELAKE_ADMIN_BOOTSTRAP_PASSWORD` provisions a real password
  instead so no well-known default ever exists. This reverses the
  bootstrap-token design; the cost is recorded in docs/CONSOLE.md §4.2.
- Retention authorization follows the data, not the verb: **growing** a
  window needs `operator`; **shrinking, introducing, or removing** one
  needs `admin`.
- The console at `/admin/ui` became a three-state page — sign in, forced
  password change, then management — still one self-contained file with
  no build step. It ships no data; every value it shows is fetched
  through an authenticated call.
- Metrics: `timelake_admin_default_credential_active` (alert on this),
  `timelake_admin_logins_total`, `timelake_admin_login_failures_total`.
- **The data plane is deliberately untouched**: `/write`, `/api/sql` and
  Flight SQL still require no credentials, so Telegraf, Grafana and the
  harness keep working. That migration is its own milestone (SEC-4
  "phased").

### Added — runtime retention management + GUI (2026-08-09)

- Retention (FR-7) is now a runtime control, not a boot-time setting:
  `GET/PUT /admin/retention` and `DELETE /admin/retention/{table}` manage
  per-table windows (`365d`/`72h`/`90m`/seconds); changes persist to
  `catalog/config/retention.json` through the store — envelope-encrypted
  like every object, S3-shared in the cluster era — and outlive a restart
  with a stale environment. `TIMELAKE_RETENTION` remains the seed when no
  stored config exists; bench fixtures are untouched.
- `GET /admin/ui`: a self-contained management page (no build step, no
  external assets, site palette) listing active policies, table-name
  autocomplete from `SHOW TABLES`, set/remove with an explicit
  "shrinking a window deletes data" warning, and the live
  `timelake_retention_drops_total` counter.
- SECURITY.md exposure 3a: `/admin/retention` is an **unauthenticated
  deletion control** — the strongest reason yet to keep 1963 private
  until token auth lands.

### Added — C0: S3 object store with KMS envelope + SSE-KMS, key-cached (2026-08-09)

- New `timelake-store-s3` crate: `S3Store` implements the `Store` trait
  over aws-sdk-s3 (owned-runtime sync bridge, safe from blocking and
  async contexts alike), and `AwsKms` implements the `Kms` trait over
  aws-sdk-kms (`generate` ↔ GenerateDataKey, `unwrap` ↔ Decrypt). The
  engine cannot tell S3 from a local directory (CL-1).
- The `Store` trait gains ONE method — `put_if_absent` — the multi-writer
  CAS primitive (S3 `If-None-Match: *`; local hard-link publish;
  encrypted passthrough). The sequence-keyed manifest log makes racing
  catalog commits collide on the same key, so exactly one wins.
- The `Kms` trait gains `generate()` (default: local random + wrap), and
  `CachingKms` decorates any Kms with the caching-CMM pattern: one data
  key reused per bounded window (default 300 s / 1,000 uses, hard cap
  2¹⁶) on encrypt, a bounded wrapped-blob→key LRU on decrypt. Thousands
  of KMS calls become a handful; `TIMELAKE_KMS_CACHE=off` restores
  strict per-object keys and is the drill's measured baseline.
- Server-side encryption rides every PUT: SSE-KMS headers with
  **S3 Bucket Keys enabled**, plus bucket-default SSE in the rig's init.
- Config: `TIMELAKE_OBJECT_STORE=s3://bucket[/prefix]`,
  `TIMELAKE_KMS_KEY_ID` (alias ok), `TIMELAKE_S3_SSE_KEY_ID`,
  `TIMELAKE_KMS_CACHE[_MAX_AGE_SECS|_MAX_USES]`; LocalStack via
  `AWS_ENDPOINT_URL` (path-style auto-forced). Setting both KMS and
  local-KEK key sources refuses to start.
- Metrics: `timelake_kms_{generate,decrypt}_total`,
  `timelake_kms_{generate,decrypt}_cache_hits_total`,
  `timelake_s3_{get,put,head,list,delete}_total`,
  `timelake_s3_{read,write}_bytes_total`.
- LocalStack rig: `deploy/compose/timelakedb-s3.yml` (S3+KMS, init
  creates `alias/timelake` and buckets with default SSE + Bucket Keys).
  The rig proves correctness, call counts, and recovery — never latency.

### Added — SEC-1: encryption at rest, at the store chokepoint (2026-08-09)

- `EncryptingStore(inner, kms)` in `timelake-store`: every object —
  Parquet, manifests, checkpoints — is envelope-encrypted with a fresh
  per-object AES-256-GCM data key, wrapped by the configured key. The
  engine is unchanged; the decorator slots in at `Engine::open`.
- Encrypted objects are chunked (64 KiB, one auth tag per chunk, header +
  object path as AAD), so the range-read path keeps working: a bloom probe
  decrypts a few KB, a footer read takes the tail, and chunks cannot be
  reordered, cross-spliced, or truncated undetected. A tampered object or
  a wrong key is a clean named error.
- Opt-in: `TIMELAKE_ENCRYPTION_KEY` (64 hex chars) or
  `TIMELAKE_ENCRYPTION_KEY_FILE`. A malformed key refuses to start rather
  than silently serving plaintext. Objects written before the key existed
  remain readable. `timelake_encryption_enabled` gauge.
- Decision recorded in ARCHITECTURE §11: whole-object envelope over
  Parquet Modular Encryption — covers non-Parquet objects, no dependency
  on arrow-rs PME maturity (retires §16 risk 2); PME per-column keys stay
  open at the same seam.

### Added — SEC-2: Accumulo-style row visibility labels (2026-08-09)

- A `_visibility` tag holding a label expression — `admin`, `ops&audit`,
  `(ops&audit)|admin`, quoted tokens — restricts each row to sessions
  whose authorizations satisfy it. Labels are ordinary dictionary-encoded
  tags: no write-path changes, FR-2 economics.
- The SEC-2 hook is real: `mandatory_predicate(session, table, schema) →
  Option<Restriction>`, called unconditionally inside `LazyTable::scan`
  and applied to every batch (buffer and file) below user predicates and
  before aggregation — `COUNT(*)` reads the label column even when the
  query doesn't, so an aggregate cannot count a hidden row.
- Semantics: unlabeled/NULL rows are public; malformed expressions are
  visible to no one (fail closed); `&`/`|` may not mix without
  parentheses (Accumulo's rule); expressions are evaluated once per
  distinct dictionary value, not per row.
- Authorizations arrive via `X-TimeLake-Authorizations` (HTTP header,
  comma-separated, or the `/api/sql` body field) and
  `x-timelake-authorizations` gRPC metadata on Flight SQL, captured into
  the flight ticket at planning time. They are **claims, not
  credentials**, until token auth lands — SECURITY.md exposure 7.
- `timelake_visibility_rows_filtered_total` counter: enforcement is
  visible, not silent.

### Added — schema discovery (2026-08-08)

- `information_schema` is enabled on the query session, so `SHOW TABLES`,
  `DESCRIBE` and `information_schema.tables` work over `/api/sql`.
- The default catalog is named after the database being queried, so the
  three-part names BI tools generate — `poc.public.events` — resolve, and
  agree with what Flight SQL reports as a catalog. Planner errors now read
  `table 'poc.public.nope' not found`.
- Flight SQL answers `CommandGetCatalogs`, `CommandGetDbSchemas`,
  `CommandGetTables` (including `include_schema`), `CommandGetTableTypes` and
  `CommandGetSqlInfo`, all of which previously returned `Unimplemented` —
  which locked out every ADBC or JDBC client that enumerates a schema before
  it will show anything. A database is a catalog, every table sits in
  `public`, and `GetSqlInfo` reports the server as read-only because writes
  arrive as line protocol and there is no DDL.
- New `SqlBackend::databases`/`tables`/`table_schema`, backed by
  `Engine::databases`/`table_names`/`table_schema` extracted from
  `sql_batches` so the query and metadata paths cannot drift.

### Fixed — non-ASCII text was mangled (2026-08-08)

- The line-protocol parser decoded each byte as a character — a Latin-1
  decode — so every multi-byte character became mojibake: a tag value of
  `München` was stored and returned as `MÃ¼nchen`. It affected measurement
  names, tag keys and values, field keys and string field values. Bytes are
  now collected and decoded once as UTF-8.
- Unchanged, and now covered by a test: a body that is not valid UTF-8 is
  refused whole with `{"error":"body is not utf-8"}` before the parser and
  before the WAL. Line protocol has no byte escape, so such data has to be
  transcoded or base64-encoded by the client.

### Fixed — a rejected write could wedge the whole engine (2026-08-08)

- `TableBuffer::append` is now atomic. It pushed a row's tag values before
  validating its field types, so a type conflict returned an error with the
  tag columns already one longer than `time`. Every later snapshot of that
  table then failed with *"all columns in a record batch must have the same
  length"* — which killed reads of the table, the flush that would have
  drained it, and, because the maintenance tick ran the stages with `?`,
  compaction and retention for **every table on the node**. The WAL replayed
  the poisoned line at boot, so a restart did not clear it.
- A **duplicate tag key in one line** (`m,h=a,h=b v=1`) caused the same
  corruption from a *successful* write: the column was pushed twice for one
  row. A repeated key now takes its last value, for tags and fields alike.
- Field types are validated against the existing column — and against a
  column the same line is about to create — before anything is mutated.
- `flush_all` encodes each table through `flush_one`, so one bad buffer no
  longer discards the other tables' rows; WAL generations are retained when
  any table fails to flush, and the maintenance tick runs flush, compaction
  and retention independently rather than aborting on the first error.
- Regression tests: three in `timelake-buffer`, plus
  `a_rejected_write_cannot_poison_the_table_or_the_engine` covering the whole
  cascade end to end — reject, read, second table, duplicate key, flush,
  restart.

### Added — documentation and project files (2026-08-08)

- `site/docs/reference.html`: line protocol grammar, escapes and field types;
  the SQL dialect with what is and is not supported; the HTTP and Flight SQL
  surfaces; an InfluxDB compatibility matrix; every metric with its type and
  suggested alerts; and a glossary. Written from the code and verified against
  a running server.

- `LICENSE` (Apache-2.0), `SECURITY.md`, `CONTRIBUTING.md`,
  `CODE_OF_CONDUCT.md` and this changelog.
- `SECURITY.md` states the real pre-v1 posture: no authentication and no
  authorization on either listener, and the exposures that follow from it.
- `docs/BACKUP_RESTORE.md` and `ops/tldb-backup.sh` — the AT-5 procedure as a
  runnable script rather than a measured result with no method.
- Project website in `site/` (landing + documentation), published by
  `.github/workflows/pages.yml`.

### Added — SEC-3: TLS 1.3 with hot certificate rotation (2026-08-08)

- New `timelake-tls` crate: validate-before-swap certificate loading (PEM
  structure, leaf expiry, key↔certificate match) behind an `ArcSwap`
  resolver that is consulted only during a handshake.
- TLS on both listeners when `TIMELAKE_TLS_CERT` and `TIMELAKE_TLS_KEY` are
  set — HTTP via `axum-server`, Flight SQL via a `tokio-rustls` accept loop.
  Plaintext remains the default. `TIMELAKE_TLS_MIN=1.2` lowers the floor.
- Rotation triggers: a 2 s file-modification watcher and
  `POST /admin/tls/reload`.
- A rejected renewal keeps the last-good pair serving and raises the named
  alarm `SEC3_CERT_RENEWAL_FAILED`.
- Metrics: `timelake_tls_cert_expiry_seconds`, `timelake_tls_last_reload_ok`.
- **AT-7 drill: 19/19** (`docs/evidence/at7-drill.log`). Under stock
  Telegraf-over-HTTPS plus sustained writes, a rotation landed mid-flight in a
  20 s Flight SQL query with an exact result, zero write errors and zero
  dropped connections.

### Fixed

- Runtime image moved to `debian:trixie-slim` to match the builder's glibc;
  a bookworm runtime failed at startup with `GLIBC_2.38 not found` after
  `rust:1-slim` moved forward.

### Added — M5: acceptance drills and the metadata cache (2026-08-08)

- Metadata cache over immutable Parquet footers: warm point lookups
  **0–6 ms** against roughly 300 ms cold, closing the M4 p95 carve-out.
- **AT-6:** stock Telegraf writes with only a URL configured; the unchanged
  fixture Grafana dashboards render over Flight SQL.
- **AT-5:** backup 34 s, restore from a destroyed volume 13 s, all 36.68M
  rows exact; SIGKILL mid-ingest recovered healthy in 4.7 s with zero
  acknowledged-write loss (40,340,794 rows exact); ten consecutive 100K
  bursts absorbed in ≤0.13 s each.
- **AT-4:** a repeat full-scale run within tolerance — ingest ±3.5%, funnels
  ±6%, storage ±9%, zero errors in both runs.

### Added — M4: the full-scale gate (2026-08-08)

- Shared `FairSpillPool` with an admission semaphore and a server-side query
  deadline (RR-1); scans moved to the blocking pool so a slow scan can always
  be preempted.
- Pruning table provider: time-bound file skipping, row-group statistics
  pruning, projection pushdown, decode-time row filters.
- Entity-clustered compaction, grace-period GC (`TIMELAKE_GC_GRACE_SECS`),
  and a schema registry.
- **AT-3 gate green with two carve-outs** at 36.6M events against the
  InfluxDB 3 baseline: ingest 365–671K lines/s with zero errors, Shape A
  median **211 ms** (vs 520), all Shape B complete — funnel 1.7 s (vs 5.7),
  B4 0.68 s (vs 30.3) — storage **0.50 GB/day** (vs 1.15), and zero
  acknowledged-row loss proven by fixed-bound equality on identical data.
- Carve-outs carried forward: Shape A p95 608 ms against a 250 ms target, and
  intra-run ingest decline under maintenance contention (stable across runs,
  so not cardinality decay).

### Added — M3: compaction, retention, Flight SQL (2026-08-08)

- Compaction merges L0 files per `(table, hour)` with cross-file
  last-write-wins de-duplication (FR-5).
- Per-table retention drops whole partitions as a catalog operation (FR-7).
- Flight SQL on port 1964 serving Grafana's stock datasource (FR-8).

### Added — M2: the storage engine (2026-08-08)

- Write path: parser → WAL (fsynced before the 204, generation-rotated) →
  in-memory buffer.
- Flush: primary-key sort and last-write-wins de-duplication into
  `(table, UTC hour)` Parquet partitions through the single store chokepoint,
  committed to a manifest-log catalog, then WAL reclaim.
- Reads union live buffer snapshots with catalogued Parquet under the RR-1
  memory pool. A WAL cap answers 429 with `Retry-After` (RR-5).
- **SIGKILL → healthy in 0.8 s** with zero acknowledged-write loss (RR-3).

### Added — M0/M1: workspace and ingest path (2026-08-08)

- Cargo workspace, server binary, Docker image, compose target, CI gate
  (fmt, clippy `-D warnings`, tests, 80% line coverage).
- Line-protocol parser with the full escape set, Arrow buffer with
  `Dictionary<Int32, Utf8>` tag columns, DataFusion SQL over the buffer, and
  the InfluxDB-compatible write endpoints (`/write`, `/api/v2/write`,
  `/api/v3/write_lp`).
- `timelakedb` backend adapter for the tsdb-bench harness.

### Added — the evidence base (2026-08-08)

- `REQUIREMENTS.md` and `ARCHITECTURE.md` derived from the five-engine
  evaluation, with the tsdb-bench harness, benchmark record and Grafana
  fixtures vendored so the repository is self-contained.
