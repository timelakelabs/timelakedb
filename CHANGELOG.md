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
