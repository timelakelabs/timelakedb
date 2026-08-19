# TimeLakeDB Console — Design

**Status: designed 2026-08-09; build phased U0–U3 (ARCHITECTURE §14).**
The console is the operator plane: one authenticated surface for changing
what the server does, seeing what it did, and watching what it is doing —
on a node, and later across a cluster.

Everything here composes from seams that already exist: the `Store`
chokepoint (SEC-1), the sequence-keyed manifest catalog, `timelake-tls`'s
validate-before-swap rotation (SEC-3), the `Discovery` trait (CL-5), and
the Prometheus exposition in `timelake-server`. The console adds four
crates and one listener; it does not add a second way to store state.

---

## 0. What exists today, and what it taught us

The retention slice shipped on 2026-08-09: `GET/PUT /admin/retention`,
`DELETE /admin/retention/{table}`, a self-contained page at `/admin/ui`,
and policies persisted to `catalog/config/retention.json` through the
`Store`. It works, and it is the right shape — but it surfaced three
problems that this design exists to answer:

1. **It is an unauthenticated deletion control.** SECURITY.md exposure 3a
   says it plainly: anyone who can reach port 1963 can set a one-second
   window on any table and the enforcement pass drops its history within
   60 s, permanently once the GC grace elapses. A management GUI without
   authentication is a remote data-destruction button.
2. **Stored config silently outranks the environment.** Today the store
   copy wins at boot and nothing says so. An operator who edits
   `TIMELAKE_RETENTION` in compose, restarts, and sees no change has hit
   a silent guardrail — exactly what RR-5 forbids ("guardrails are
   visible, tunable, and never silent").
3. **`DELETE` is ambiguous.** Removing a policy means "keep everything"
   today. Under a layered model it must mean "revert to the system
   property" — which may legitimately reinstate a 90-day window. "Keep
   everything, regardless of the property" is a *different* intent and
   needs its own representation.

Retention is also one setting of nine in `EngineConfig`, none of the
others reachable at runtime at all.

## 1. Requirements trace

| Capability | Traces to |
|---|---|
| Runtime-editable tunables with visible provenance | RR-5 (visible, tunable, never silent), SR-4 |
| Per-table retention management | FR-7 |
| App log viewing | SR-4, RR-5 |
| Audit trail of every mutation | **SR-6** (new), SEC-2 governance posture |
| Metrics, ingest/query/storage performance views | SR-3 (observable compaction), SR-4, PR-* verification |
| Cluster/node view | CL-3, CL-4, CL-5 |
| Authentication + roles on the admin surface | **SEC-4** (new), SECURITY.md exposures 1–4 |
| The console itself | **SR-5** (new) |

SR-5, SR-6 and SEC-4 are added to `REQUIREMENTS.md` by this design.

## 2. Principles

1. **The console may never be why the data plane fails.** It is a
   corollary of RR-1: its own listener, its own bounded buffers, no
   unbounded query behind a page, and no page that can pin the query
   memory pool. If the console is overloaded, the console degrades.
2. **Every mutation is authenticated, authorized, and audited.** No
   exceptions, no "internal" endpoints outside the rule.
3. **Nothing is silent** (RR-5). Provenance on every setting, a loud
   banner on divergence, an impact preview before anything destructive.
4. **No build step, no external assets.** Same rule as `site/`:
   hand-written HTML/CSS/JS, embedded in the binary, works offline and
   behind a strict CSP.
5. **Grafana is not replaced** (FR-8). The console explains *the node*;
   Grafana explores *the data*. The provisioned dashboards in
   `fixtures/grafana/` stay the compatibility fixture and are not forked.
6. **Read-only is the default posture.** The `viewer` role sees
   everything and changes nothing; that is what most operators need most
   of the time.

---

## 3. The configuration model

This is the heart of the design: a setting may be declared by a system
property *and* edited in the GUI, and both facts have to remain true and
visible afterwards.

### 3.1 Layers

Three layers resolve, lowest to highest:

```
built-in default   EngineConfig::default()          — compiled in
system property    TIMELAKE_*  (env / compose / unit file)
stored override    catalog/config/settings.json     — written by the console
```

The **effective value** is the highest layer that is set. A stored
override wins at runtime; the property remains visible underneath it, and
removing the override falls back to the property, not to the default.

### 3.2 Provenance

Every setting the API returns carries its whole stack, not just the
answer:

```json
{
  "key": "retention.pipeline_events",
  "effective": {"value": "30d", "source": "override"},
  "layers": {
    "default":  null,
    "property": {"value": "90d", "env": "TIMELAKE_RETENTION"},
    "override": {"value": "30d", "revision": 41,
                 "actor": "rcowell", "at": "2026-08-09T09:52:11Z",
                 "property_at_write": "90d"}
  },
  "scope": "cluster", "apply": "hot", "danger": "destructive",
  "pinned": false
}
```

`property_at_write` is what makes divergence detectable. When the current
property differs from the one recorded at override time, the operator
changed the deployment and the change is being shadowed. That is
reported three ways: a banner on the setting, a `WARN` log line at boot
and at detection, and the gauge
`timelake_config_divergent_settings`.

### 3.3 Override states — three, not two

| State | Meaning | API |
|---|---|---|
| absent | inherit the property, or the default | `DELETE /admin/config/{key}` |
| value | this value, regardless of the property | `PUT` with a value |
| explicit-none | the feature is *off* here, even if the property sets it | `PUT` with `null` |

This resolves problem 3 from §0. For retention: `DELETE` reverts a table
to whatever `TIMELAKE_RETENTION` declares; `PUT null` means "this table
keeps everything, and I mean it".

### 3.4 Pinning: the property layer can lock a setting

`TIMELAKE_CONFIG_PINNED=gc_grace_secs,query_mem_bytes` marks named
settings read-only in the console. The UI shows them with the property
value and a lock, and the API rejects writes with `409 pinned by system
property`. Deployments that require configuration-as-code keep it for the
keys that matter without losing the GUI for the rest.

The audit-retention floor (§5.4) is pinned implicitly and always.

### 3.5 The settings inventory

`scope`: **cluster** = stored in the object store, shared by every node;
**node** = local to one process. `apply`: **hot** = takes effect on the
next use, **staged** = applies to work admitted after the change,
**boot** = requires a restart (shown read-only, with the exact line to
change and a "restart required" marker).

| Key | Property | Scope | Apply | Min role | Validation / danger |
|---|---|---|---|---|---|
| `retention.<table>` | `TIMELAKE_RETENTION` | cluster | hot | operator to grow, **admin to shrink** | Shrink deletes data — impact preview required (§11) |
| `flush_rows` | `TIMELAKE_FLUSH_ROWS` | node | hot | operator | 1e3..1e7; high values grow the buffer (RR-4) |
| `flush_age_secs` | `TIMELAKE_FLUSH_AGE_SECS` | node | hot | operator | 1..3600 |
| `wal_max_bytes` | `TIMELAKE_WAL_MAX_BYTES` | node | hot | operator | Lowering below current depth causes immediate 429s — warn with the current depth |
| `compact_min_files` | `TIMELAKE_COMPACT_MIN_FILES` | cluster | hot | operator | 2..64; drives the PR-6 fresh penalty |
| `max_concurrent_queries` | `TIMELAKE_MAX_CONCURRENT_QUERIES` | node | staged | admin | 1..64; raising it divides the same pool further (RR-1) |
| `query_timeout_secs` | `TIMELAKE_QUERY_TIMEOUT_SECS` | cluster | staged | operator | Must stay **below** `gc_grace_secs` |
| `gc_grace_secs` | `TIMELAKE_GC_GRACE_SECS` | cluster | hot | admin | **Must exceed `query_timeout_secs`** — the AT-3 compaction-vs-query race |
| `query_mem_bytes` | `TIMELAKE_QUERY_MEM_BYTES` | node | staged | admin | Warn when > 60 % of the cgroup limit (RR-1) |
| `kms_cache*` | `TIMELAKE_KMS_CACHE*` | node | boot | admin | v1 boot-only; hot candidate later |
| `tls.cert/key` | `TIMELAKE_TLS_CERT/_KEY` | node | boot (paths) | admin | Contents already hot-rotate (SEC-3); path changes need a restart |
| `object_store`, `data_dir`, `role`, `addr`, `flight_addr`, `admin_addr` | `TIMELAKE_*` | node | boot | admin (read-only) | Displayed with provenance; never editable at runtime |
| `encryption_key`, `kms_key_id` | `TIMELAKE_ENCRYPTION_KEY*`, `_KMS_KEY_ID` | node | boot | admin | **Never rendered.** Presence, source and key id fingerprint only |

Secrets follow the existing discipline — key material stays out of
`EngineConfig` precisely so a `?cfg` log line cannot leak it, and the
console inherits that rule: it reports *that* a key is configured and
from where, never the bytes.

### 3.6 Validation

Validation is a pure function over the *proposed whole config*, not over
one field, because the interesting rules are cross-field. A rejected
change applies nothing and returns the reason and the offending
invariant:

```
409 gc_grace_secs (300s) must exceed query_timeout_secs (600s) —
    a query's catalog snapshot would outlive its files (AT-3 race).
```

The three invariants that matter today: `gc_grace_secs >
query_timeout_secs`; `query_mem_bytes` under the process memory limit;
retention windows strictly positive. Bounds are advisory-with-warning
where the right value is workload-dependent (`flush_rows`,
`compact_min_files`) and hard where violation breaks a guarantee.

### 3.7 Persistence

One document per scope, versioned and revision-stamped:

```
catalog/config/settings.json     cluster scope, via Store (encrypted by SEC-1)
<data_dir>/config/node.json      node scope, local
```

```json
{"schema_version": 1, "revision": 42,
 "settings": {"gc_grace_secs": {"value": 1200, "actor": "rcowell",
                                "at": "...", "property_at_write": "900"}}}
```

Read-modify-write under a lock, `put` today (last-writer-wins, matching
the retention slice's honest note that this is admin config, not data).
**C1 upgrades this to `put_if_absent` CAS** on a revision-keyed path —
the same primitive the catalog gets — and a losing writer returns `409`
with a diff of what changed underneath it rather than clobbering.
`retention.json` folds into `settings.json` under the `retention.*` key
prefix at U0, with a one-time migration read.

### 3.8 Propagation in a cluster

Cluster-scope config is polled from the store on the existing maintenance
tick (10 s) and applied through the same hot-swap holder used locally.
Each node exposes its applied `revision` on `/health` and as
`timelake_config_revision`, so the console can show convergence and flag
a node that is behind or diverged (U3). Nothing about correctness depends
on propagation speed — config is policy, and CL-5's guard stands: the
discovery/console path may affect availability and routing, never
correctness.

### 3.9 Hot application

Config is held as an immutable snapshot behind an `ArcSwap` — the same
pattern `timelake-tls` uses for certificates, for the same reason: readers
on the hot path take a pointer, writers publish a whole validated
snapshot, and in-flight work keeps the version it started with. Concretely:

- `flush_*`, `wal_max_bytes`, `compact_min_files`, `retention.*` are read
  from the snapshot at each maintenance tick or write admission — hot with
  no machinery.
- `query_timeout_secs` and `query_mem_bytes` are captured per query at
  admission; running queries keep their pool and deadline. A pool change
  builds a new `QueryEnv` for subsequent admissions and drops the old one
  when its last query finishes.
- `max_concurrent_queries` resizes the admission semaphore by adding
  permits to grow, and by acquiring-and-forgetting permits to shrink, so
  a shrink never cancels a running query — it just stops admitting.

---

## 4. Authentication and authorization (SEC-4)

No authentication exists anywhere in the codebase today. U0 introduces it
for the admin surface only; the data plane follows later, and the design
is deliberately shaped so that following is cheap.

### 4.1 Roles

| Role | Can |
|---|---|
| `viewer` | Read config, metrics, logs, audit, cluster state. Change nothing. |
| `operator` | Everything viewer does, plus non-destructive tunables, growing a retention window, triggering flush/compaction/TLS reload. |
| `admin` | Everything operator does, plus destructive and resource-governing settings (retention shrink/remove, `gc_grace_secs`, `query_mem_bytes`, `max_concurrent_queries`), and principal management. |

The operator/admin split on retention follows the data: growing a window
keeps more, shrinking destroys. That asymmetry is the single most
consequential thing the console can do, so it gets the higher bar.

### 4.2 Bootstrap — **decision changed 2026-08-09, and why**

This section originally read "No default password, ever": a one-time
bootstrap token printed to stdout, single-use, 30-minute expiry. **The
shipped behaviour is Grafana's instead: a fresh node seeds
`admin`/`admin`, flagged for mandatory rotation.** The reversal was a
deliberate product call — the familiar first-run experience beats the
token dance for a system operators will meet through a browser — so the
honest thing is to record what it costs rather than quietly restate the
design.

**What it costs.** `admin/admin` is the most-guessed credential pair in
existence. Between a node's first start and its first password change,
anyone who can reach the admin port owns the console. A bootstrap token
has no such window. Nothing below removes that window; it is narrowed
and made noisy.

**What narrows it:**

- The seeded credential is **quarantined**: it authenticates, and then
  the only route that answers is `POST /admin/password`. Everything
  else — every read, every retention change — returns `403
  password_change_required`. A stolen default cannot destroy data; it
  can only lock the real operator out of a node that has nothing in it
  yet.
- Rotation **invalidates every session** for that principal, including
  the one that performed it, so a cookie captured during the window dies
  with the password.
- The replacement is refused if it is shorter than 8 characters, equal
  to the username, or `admin`.
- `timelake_admin_default_credential_active` is `1` until rotation, a
  `WARN` names the risk at every start, and the console shows a
  persistent red banner. This is the difference between a known default
  and a *silent* one.
- `TIMELAKE_ADMIN_BOOTSTRAP_PASSWORD` keeps the original posture
  available: provision a real password, no well-known default ever
  exists, and the rotation flag still applies.

**Deployment consequence:** with the admin surface still riding on 1963
(the listener split is U0 work), a fresh node exposed to an untrusted
network is exposed *with* a known credential. Bind privately, or set
`TIMELAKE_ADMIN_BOOTSTRAP_PASSWORD`, before that node is reachable.

### 4.3 Sessions and tokens

- **Browser**: credential → server-side session; cookie is `HttpOnly`,
  `Secure`, `SameSite=Strict`, with idle (30 min) and absolute (12 h)
  expiry. State-changing requests additionally require an `Origin` check
  and a double-submit CSRF token — a config API reachable by a browser
  form is a config API reachable by any page the operator visits.
- **Automation**: role-scoped bearer tokens, revocable, listed with last
  use, stored as Argon2id hashes. Credentials are never stored reversibly.
- Failed logins are rate-limited per source and per principal with
  exponential backoff, and every attempt is audited.

### 4.4 Transport

The admin listener refuses to serve plaintext unless
`TIMELAKE_ADMIN_INSECURE=1` is set, which logs a `WARN` on every start and
shows a persistent banner in the UI. It reuses `timelake-tls` wholesale —
the same rotating resolver, the same last-good behaviour, no second
certificate story.

### 4.5 The bridge to SEC-2

SEC-2 visibility authorizations are, in SECURITY.md's words,
"unauthenticated claims" — `X-TimeLake-Authorizations` is whatever the
caller says it is. SEC-4 gives them an owner: a principal has a set of
grantable authorizations, and once the data plane requires authentication
the scan-time predicate can trust the intersection of *claimed* and
*granted* instead of the claim alone. The console is where that grant is
administered and displayed. U0 ships the principal store and the grants;
enforcing them on `/api/sql` and Flight SQL is its own milestone, because
it breaks every existing client and belongs with a deliberate migration.

---

## 5. The audit log (SR-6)

### 5.1 What is audited

Every mutation, every authentication event, and every denial: config and
retention changes, principal/token lifecycle, login success/failure/
lockout, TLS reload, maintenance triggers, and console-initiated backup
or GC actions. Reads are not audited by default (volume), with one
exception: reading the audit log is itself audited.

### 5.2 Record

```json
{"seq": 1041, "ts": "2026-08-09T09:52:11.418739Z", "node": "tldb-1",
 "principal": "rcowell", "role": "admin", "session": "s_7f…",
 "source": "10.0.3.7", "request_id": "r_2c…",
 "action": "retention.set", "target": "pipeline_events",
 "before": {"value": "90d", "source": "property"},
 "after": {"value": "30d", "source": "override"},
 "revision": 42, "outcome": "ok",
 "prev_hash": "sha256:9a3…", "hash": "sha256:04b…"}
```

`before`/`after` carry the resolved layer, not just the value, so the
record answers "what actually changed for the server", which is the
question an incident asks.

### 5.3 Tamper evidence

`hash = SHA-256(canonical_json(record without hash) || prev_hash)` — a
per-node chain. Segment files carry the chain head of the previous
segment; `ops/tldb-audit-verify.sh` walks segments and reports the first
break with its sequence number. This is tamper *evidence*, not tamper
*proofing*: someone with write access to the store can rewrite the whole
chain. Making that detectable too means anchoring heads somewhere the
node cannot rewrite (external WORM bucket, or a signed head) — designed
for, not built in v1, and stated as a limitation in SECURITY.md.

### 5.4 Storage and retention

Audit records go to append-only local segments (fsync per record —
volume is human-scale) and upload to the object store on rotation, where
SEC-1 encrypts them like every other object. They are **not** an ordinary
table: the retention UI must not be able to delete the record of its own
use. They are exposed read-only as `system.audit` so the console, SQL and
Flight SQL all read them the same way.

Audit retention has a floor: `TIMELAKE_AUDIT_MIN_RETENTION_DAYS`
(default 90) is pinned to the property layer, so lowering it requires
deployment access, not a console session — and the attempt is audited.

**Shipped 2026-08-18**, with one deviation from the paragraph above worth
stating plainly. Rotation is in: the live segment becomes
`audit.<last-seq>.jsonl` at `TIMELAKE_AUDIT_ROTATE_SIZE` (default 64 MiB)
or `TIMELAKE_AUDIT_ROTATE_EVERY`, and the hash chain runs *through* the
boundary — `read_all` concatenates segments in order, so a removed segment
breaks verification exactly as a removed record does. Reopening after a
rotation recovers the head from the whole trail, never from the live
segment alone; the latter would hand the next record a genesis `prev_hash`
and silently split the trail in two.

The floor is enforced as a **clamp rather than a refusal**:
`TIMELAKE_AUDIT_RETAIN_DAYS` below 90 is raised to 90 rather than rejected,
and unset means **nothing is ever deleted**. Upload-on-rotation and the
`system.audit` exposure are still to come, so segments currently stay on
the node.

### 5.5 Failure policy: fail closed

If the audit sink cannot append, mutations are **refused** with `503
audit sink unavailable`; reads and the data plane are unaffected. An
administrative change that leaves no record is worse than a change that
did not happen. `TIMELAKE_AUDIT_FAIL_OPEN=1` exists for the operator who
disagrees, logs loudly, and is itself audited when the sink recovers.

---

## 6. Application logs

`tracing` already emits structured events to stdout; that stays exactly
as it is, because container log collectors depend on it. The console adds
a second, bounded subscriber layer:

- A **ring buffer** in memory — default 10,000 records or 8 MiB,
  whichever binds first, configurable, counted against RR-4's idle
  footprint budget. Oldest records drop; the drop count is visible so a
  gap is never silent.
- `GET /admin/logs?level=&since=&contains=&target=&request_id=&limit=` —
  a snapshot query over the ring.
- `GET /admin/logs/stream` — Server-Sent Events tail with the same
  filters. SSE over EventSource, not WebSockets: one direction, trivially
  proxied, no framing library.
- **Correlation**: every HTTP request gets a `request_id`, every query a
  `query_id`, both in the log record and in the audit record, so a slow
  query in the performance view links to its own log lines and, if it
  changed something, its audit entry.
- **Redaction** is structural: key material never enters a log field in
  the first place (the existing `EngineConfig` discipline), and the
  console refuses to render any field named like a secret.

App logs are explicitly *not* written into TimeLakeDB. A database that
ingests its own logs feeds a write amplification loop precisely when it is
unhealthy, which is when the logs matter most.

---

## 7. Metrics and performance views

### 7.1 Sources

Three, no new pipeline: the Prometheus text from `metrics_text_impl`, the
catalog (file counts, partitions, bytes, levels), and the query log
(latency, memory, pruning stats).

### 7.2 A small sample ring for history

Grafana is the answer for real history; the console needs enough to
answer "what happened in the last few hours" on a node with nothing else
installed. A server-side ring samples the counters every 10 s and keeps
6 hours (2,160 points per series, a few hundred KiB for the series
below). Bounded, in memory, lost on restart, and never a storage
dependency.

### 7.3 Views

| View | Answers |
|---|---|
| **Overview** | Health, version, uptime, applied config revision, ingest rate, buffer rows, WAL depth, file count, storage bytes, encryption/TLS state, alarms |
| **Ingest** | Lines/s over time, 429 backpressure events, WAL depth vs cap, flush cadence and lag, write errors by class |
| **Storage** | Bytes per table, files by level (L0/L1/L2), compaction rate and lag, retention drops, GC pending files and grace |
| **Query** | In-flight and queued, admission wait, latency p50/p95/p99, peak memory per query, pruning effectiveness (files/row-groups skipped), timeouts and rejections, the slowest recent queries with their plans |
| **Security** | TLS certificate expiry and last reload, KMS calls and cache hit rate, visibility rows filtered, recent auth failures |

The Query view is the one that pays for itself: PR-3 and PR-6 are the
project's contested numbers, and "Shape A got slow" is currently a
question you answer by running the harness.

### 7.4 Metrics that must be added — **BUILT 2026-08-18**

> **Status.** Everything in the table below is now exposed on `/metrics`,
> with the exceptions noted after it. Before this the exposition was
> entirely counters and gauges — nothing recorded how *long* anything
> took, so the Query view could not be drawn at all and "Shape A got slow"
> was a question you answered by running Gauge.
>
> The surfacing decision went to **self-monitoring** rather than
> Prometheus: the node writes its own telemetry into its own `_system`
> database (§7.6) and Grafana reads it back over Flight SQL. `/metrics`
> is unchanged and remains the escape hatch — it answers from in-memory
> atomics with no query path involved, which is what still works when the
> engine is too sick to serve SQL.

ARCHITECTURE §13 promises several of these; the exposition previously had
counters and gauges for lines, buffer rows, WAL bytes, files, flushes,
compactions, retention drops, databases/tables, encryption, visibility
filtering, KMS, S3 and TLS. Missing, and needed for the views above:

| Metric | Type | For |
|---|---|---|
| `timelake_uptime_seconds`, `timelake_build_info` | gauge | Overview |
| `timelake_query_duration_seconds` | histogram | Query latency, PR-3 |
| `timelake_query_peak_memory_bytes` | histogram | RR-1 headroom |
| `timelake_query_admission_queued`, `_wait_seconds` | gauge/histogram | Admission pressure |
| `timelake_query_rejected_total{reason}`, `_timeout_total` | counter | RR-2/RR-5 |
| `timelake_query_files_pruned_total`, `_row_groups_pruned_total` | counter | Pruning effectiveness |
| `timelake_write_rejected_total{reason}` | counter | Backpressure (429s) |
| `timelake_storage_bytes{table}`, `timelake_files{level}` | gauge | Storage view, SR-2 |
| `timelake_compaction_lag_seconds`, `timelake_flush_lag_seconds` | gauge | SR-3 |
| `timelake_gc_pending_files` | gauge | GC grace behaviour |
| `timelake_config_revision`, `timelake_config_divergent_settings` | gauge | §3.2, §3.8 |
| `timelake_audit_records_total`, `timelake_audit_sink_healthy` | counter/gauge | SR-6 |

These are useful to Grafana and the harness independently of the console,
which is the argument for adding them at U2 regardless.

**What was built, with the names as shipped.** The query metrics are
instrumented in `run_sql_env` — the single production call site for both
HTTP and Flight SQL, the same chokepoint the read-only guard uses, so the
two surfaces cannot drift into different accounting.

| Shipped | Notes |
|---|---|
| `timelake_query_duration_seconds` | Histogram. Buckets are dense either side of 250 ms because that is where PR-3's argument is, coarse in the tail |
| `timelake_query_admission_wait_seconds` | Histogram. Separates *slow* from *merely queued* |
| `timelake_query_in_flight`, `_queued` | Gauges, RAII-guarded so a cancelled query cannot leak them upward |
| `timelake_queries_total`, `_timeouts_total`, `_refused_total`, `_failed_total` | `refused` is counted apart from `failed`: refusing a COPY is the P0-2 guard working, and folding it into an error rate makes a healthy node look broken whenever a client probes it |
| `timelake_uptime_seconds`, `timelake_build_info` | |
| `timelake_flush_lag_seconds`, `timelake_compaction_lag_seconds` | With nothing having run yet these report the **whole uptime**, not zero — a zero reads as "just flushed, healthy" at the exact moment the subsystem has never run |
| `timelake_gc_pending_files` | |
| `timelake_files{level}` | `flushed` / `compacted` / `rewritten`. See the caveat below |
| `timelake_storage_bytes{db,table}`, `timelake_storage_rows{db,table}` | Folded under the catalog lock, not via `all_files()`, which clones every `FileMeta` on every scrape |
| `timelake_write_rejected_total{reason}` | `backpressure` / `bad_request` / `not_here` / `internal`. The split is the point: the first is yours to act on, the second is the client's |
| `timelake_selfmon_dropped_total`, `_written_total`, `_pending` | §7.6 |

**Deferred, with reasons rather than silence:**

- **`timelake_query_peak_memory_bytes`** — DataFusion's pool reports a
  process-wide reservation, not a per-query peak; attributing it per query
  needs a per-query pool, which is exactly the design M4 removed for
  measured reasons (see `QueryEnv`). Not worth reintroducing for a metric.
- **`timelake_query_files_pruned_total` / `_row_groups_pruned_total`** —
  the pruning decisions happen inside `provider::scan`, below the seam
  that currently carries any counters. Reachable, but it is a change to
  the hot path and belongs with its own measurement.
- **`timelake_config_revision` / `_divergent_settings`** — these describe
  the layered configuration of §3, which is U0 work and does not exist yet.
- **A real `level` field on `FileMeta`.** `timelake_files{level}` derives
  the level from the filename prefix the write path stamps (`c` =
  compacted, `t` = tombstone-rewritten, otherwise L0). That is a naming
  convention, not a recorded fact. It is isolated in `catalog::level_of`
  with a test that builds paths using the same `format!` strings as the
  three write sites, so changing a path format fails the test rather than
  making the metric quietly lie — but recording the level explicitly is
  the better fix.

### 7.5 Relationship to the harness

The console reports; Gauge's `bench/` remains the specification. The U2 gate is
that the console's numbers agree with `/metrics` and with the `run.json`
of a bench run — a console that disagrees with the harness is a bug in
the console.

Since §7.6 samples by converting the `/metrics` text itself, the first
half of that gate holds by construction; the drill
(`deploy/compose/console-drill/console_drill.sh`) checks it anyway,
because "by construction" is a claim and a drill is evidence.

### 7.6 Self-monitoring: the database stores its own telemetry

*(Built 2026-08-18. `crates/server/src/selfmon.rs`.)*

The node writes two streams into a `_system` database, and Grafana reads
them back over Flight SQL — the same surface user data is read through.

| Table | Contents |
|---|---|
| `_system.metrics` | The whole `/metrics` exposition, sampled every maintenance tick (10 s) |
| `_system.queries` | One row per finished query: `db`, `outcome`, `identity`, `duration_ms`, `wait_ms`, `rows`, `ref` |

**Percentiles are measured, not estimated.** Storing one row per query
means p50/p95/p99 come from the real distribution and can be sliced by
database, outcome and client identity — which a pre-bucketed histogram
cannot do. The histogram on `/metrics` remains, for the case below.

**The sample is a conversion of `/metrics`, not a second list.** The
sampler parses the exposition the node already renders and re-emits it as
line protocol. That looks roundabout and buys two things: §13's U2 gate
(stored numbers agree with `/metrics`) becomes true by construction
because they are the same numbers, and a metric added later is
self-monitored the day it is added rather than silently missing from the
dashboard forever.

**It yields to the workload.** Monitoring that adds load during an
overload makes the outage worse. The query-path observer only formats a
line and pushes it onto a bounded queue; it never writes, never blocks,
and **drops** when full. Drops are counted and exposed, because silent
loss would make the dashboard lie by omission at the busiest moment. A
failed `_system` write is logged and discarded, never retried and never
propagated — telemetry cannot fail maintenance.

**`_system` rows are excluded from `timelake_lines_written_total`.** That
counter is what Gauge's throughput is compared against; a baseline that
drifts upward because the server is watching itself is worse than no
metric.

#### A metric never emitted has no column

Found by the drill, and worth understanding before writing panels: a
metric becomes a **column** in `_system.metrics`, and columns are created
on write. A subsystem that is not configured emits nothing, so its column
never exists — and SQL cannot reference a column that does not exist. The
panel fails with a query error rather than showing an empty graph.

Prometheus degrades gracefully here (a missing series is simply no data);
SQL does not. This is the sharpest edge of storing metrics as rows.

Affected are every conditionally-emitted metric:

| Metric group | Emitted only when |
|---|---|
| `timelake_tls_*` | `TIMELAKE_TLS_CERT` is set |
| `timelake_tls_client_ca_*`, `timelake_flight_connections_*` | a client CA is configured (SEC-3 want mode) |
| `timelake_s3_*` | the object store is S3 |
| `timelake_kms_*` | the KMS key cache is active |
| `timelake_cl2_*` | the node is an ingester with a peer |
| `timelake_querier_*`, `timelake_catalog_head` | the node is a querier |
| `timelake_router_*` | the node is a router |

**The shipped dashboard therefore uses only always-emitted metrics**, so
it works on a stock node out of the box. Add the rest per deployment —
for a TLS node, for instance:

```sql
SELECT MAX(timelake_tls_cert_expiry_seconds) AS expires_in
FROM metrics
WHERE timelake_tls_cert_expiry_seconds IS NOT NULL
  AND time >= now() - INTERVAL '15 minutes'
```

Note that `/metrics` was deliberately **not** changed to emit placeholder
zeros for absent subsystems. A `timelake_tls_cert_expiry_seconds 0` on a
plaintext node would read as "certificate expired" and fire the very
alert §7.4 recommends, on every node that has no certificate at all.
Absent is the truthful encoding; the cost is paid here instead.

#### Three limits, stated rather than discovered later

1. **It dies when you need it.** Reading the database through the
   database means these panels go blank exactly when the engine is too
   unhealthy to serve SQL. `/metrics` on 1963 is unchanged and answers
   from in-memory atomics with no query path involved — that is the
   surface to scrape for alerting, and the reason it was not replaced.
2. **A CL-3 querier stores nothing.** A querier owns no data, refuses
   writes and runs no maintenance, so a local buffer would grow with
   nothing to flush it. `selfmon_tick` returns 0 there and `/metrics`
   is the only surface. In a cluster that is where the queries run, so
   shipping querier samples to an ingester is real work — it belongs
   with the C2 role split, not with metrics.
3. **`_system` has no retention by default — but it is now safe to set
   one.** ~~`enforce_retention` matches on table name alone, ignoring the
   database~~ — **fixed 2026-08-19**. Policies are scoped `(db, table)`,
   so a window on `_system.metrics` no longer touches a user table that
   happens to share the name. Until that fix, seeding one here would have
   been a data-loss hazard rather than a tidy default, which is why this
   shipped unbounded.

   Nothing is created for you, because a deletion policy should be an
   operator's decision and not a side effect of enabling telemetry. Two
   calls bound it:

   ```
   PUT /admin/retention {"db": "_system", "table": "metrics", "duration": "7d"}
   PUT /admin/retention {"db": "_system", "table": "queries", "duration": "30d"}
   ```

   At a 10-second sample interval `_system.metrics` is the one that grows
   steadily; `_system.queries` grows with query volume.

`TIMELAKE_SELFMON=off` disables the whole thing;
`TIMELAKE_SELFMON_QUEUE` sets the queue bound (default 4096).

---

## 8. Cluster view (U3, after C2)

Once roles split (§12.4) the console aggregates rather than introspects:

- **Node table** from `Discovery`: id, role (router/ingester/querier/
  compactor), address, health, version, uptime, applied config revision,
  and role-specific lag (ingester WAL replication lag, compactor queue
  depth, querier in-flight).
- **Drill-in** to any node's Overview/Ingest/Query views by proxying that
  node's admin API with the caller's identity — one login, not one per
  node.
- **Convergence**: which nodes have applied which config revision, and a
  loud banner when they disagree for longer than two maintenance ticks.
- **Degraded modes stay loud** (RR-5): ingest running unreplicated,
  compactor singleton unelected, catalog CAS contention — each with the
  named alarm the engine already raises.

CL-5's guard is restated as a console rule: the view is derived from
discovery and is therefore *advisory*. No console action may depend on the
membership view being correct, and nothing in the write or catalog path
may consult the console.

---

## 9. Surface and topology

### 9.1 Listeners

| Port | Listener | Default bind | Content |
|---|---|---|---|
| 1963 | HTTP data | `0.0.0.0` | writes, `/api/sql`, `/health`, `/ping`, `/metrics` |
| 1964 | Flight SQL | `0.0.0.0` | FR-8 |
| **1965** | **Admin** | **`127.0.0.1`** | console UI + `/admin/*` API |

The admin listener is private by default and must be opened deliberately
(`TIMELAKE_ADMIN_ADDR=0.0.0.0:1965`), which is the opposite of the
current situation where the most destructive endpoint in the system rides
on the most exposed port.

**Breaking change**: `/admin/retention` and `/admin/ui` move off 1963,
and `/admin/tls/reload` moves with them. Since nothing has been released
and the only caller is the page itself, they move rather than being
deprecated in place — but 1963 keeps stub routes returning `410 Gone`
with the new location for one milestone, because the uncommitted retention
slice is already in operators' hands. `/metrics` stays on 1963: it is
unauthenticated by design and Prometheus scrapes it.

### 9.2 Crates

```
crates/
  config/   layered resolver, provenance, validation, persistence, hot-swap holder
  auth/     principals, credentials, sessions, tokens, roles   ← SEC-4, later data plane
  audit/    chained append-only sink, segments, system.audit, verifier
  admin/    the admin listener: REST API + embedded console assets
```

Four crates rather than one because three of them outlive the console:
`config` is where the engine reads its tunables from once they are hot,
`auth` is what SEC-4 needs for `/api/sql`, and `audit` is a governance
artefact that a headless deployment still wants. `admin` is the only one
that owns HTML.

### 9.3 API

| Method | Path | Role | Notes |
|---|---|---|---|
| `GET` | `/admin/config` | viewer | All settings with full provenance |
| `GET` | `/admin/config/{key}` | viewer | One setting |
| `PUT` | `/admin/config/{key}` | per §3.5 | Value or `null` (explicit-none); `?dry_run=1` returns the impact preview |
| `DELETE` | `/admin/config/{key}` | per §3.5 | Revert to property/default |
| `GET` | `/admin/retention` | viewer | Sugar over `config?prefix=retention.` — the shipped shape, kept |
| `PUT`/`DELETE` | `/admin/retention[/{table}]` | operator/admin | Same semantics as config, with the impact preview |
| `GET` | `/admin/logs`, `/admin/logs/stream` | viewer | Snapshot, SSE tail |
| `GET` | `/admin/audit` | viewer | Filter by actor/action/target/time; `?verify=1` checks the chain |
| `GET` | `/admin/metrics/series` | viewer | The sample ring, JSON |
| `GET` | `/admin/queries` | viewer | Recent + in-flight, with plans |
| `POST` | `/admin/maintenance/{flush,compact,gc}` | operator | Trigger a tick now |
| `POST` | `/admin/tls/reload` | admin | Moved from 1963 |
| `GET`/`POST`/`DELETE` | `/admin/principals`, `/admin/tokens` | admin | SEC-4 |
| `POST` | `/admin/session` | — | Login; `DELETE` to log out |
| `GET` | `/admin/cluster` | viewer | U3 |

Every mutating route above emits exactly one audit record, including
denials. That is the U1 gate.

### 9.4 Assets

One HTML file per page plus a shared `console.css` and `console.js`,
`include_str!`-embedded exactly as `admin_ui.html` is today, served with
a strict CSP (`default-src 'none'; style-src 'self'; script-src 'self'`),
no CDN, no fonts, no build step. The palette is the site's: navy `0B1320`,
blue `2563EB`, gold `D4AF37`, mist `E6E8EC`, gray `6B7280`. Charts are
hand-drawn SVG sparklines over the sample ring — a few hundred lines, no
charting library, and no dependency that a CSP has to make an exception
for.

---

## 10. Screens

**Overview** — the page an operator opens at 3 a.m.

```
TimeLake DB · tldb-1                    [healthy]  v0.5.0  up 6d 04:11
─────────────────────────────────────────────────────────────────────
 INGEST            73.2K lines/s  ▁▂▅▇▇▆▇█    WAL      412 MiB / 2 GiB
 QUERIES        2 in flight, 0 queued        POOL     318 MiB / 1 GiB
 STORAGE            0.50 GB/day  ▁▁▂▂▃▃▄▄    FILES    L0 12 · L1 40 · L2 8
 TLS       cert expires in 21h 04m           KMS      cache hit 99.2%
─────────────────────────────────────────────────────────────────────
 ⚠ config revision 42 applied · TIMELAKE_RETENTION differs from 1
   override (pipeline_events)                              [review]
```

**Configuration** — provenance is the layout, not a tooltip.

```
 Setting              Effective      Source            
 ──────────────────────────────────────────────────────────────────
 gc_grace_secs        1200           override      [edit] [revert]
   ├ override         1200   rcowell · 2026-08-09 09:52
   ├ property          900   TIMELAKE_GC_GRACE_SECS   ⚠ changed since
   └ default           900
 query_timeout_secs    600           property      [edit]
 query_mem_bytes      1 GiB          default       [edit]
 data_dir             /data          property      🔒 restart required
 encryption_key       configured     property      🔒 (key id …a41f)
```

**Retention** — the shipped page, plus provenance and a real preview.

```
 Table              Window   Source     
 ────────────────────────────────────────────────────
 pipeline_events    30d      override   [edit] [revert to 90d]
 host_metrics       90d      property   [edit]
 disk_metrics       —        none       [set]

 ⚠ Shrinking pipeline_events 90d → 30d will drop
   1,440 partitions · 47.2 GiB · 2026-05-11 → 2026-07-11
   Permanent after the 15-minute GC grace.
   Type the table name to confirm:  [____________]  [Apply]
```

**Logs** — filter, tail, correlate.

```
 [ level ≥ WARN ▾ ] [ target… ] [ contains… ]   ● live  ⟨247 dropped⟩
 09:52:11.418 WARN  timelake_server::flush  table=pipeline_events
              partition=2026-08-09T09  flush took 4.2s (lag 12s)
              request_id=r_2c… → [audit] [query]
```

**Audit** — the record, and whether to believe it.

```
 [ actor ▾ ] [ action ▾ ] [ 24h ▾ ]              chain: ✓ verified (1041)
 09:52:11  rcowell  admin  retention.set  pipeline_events  90d → 30d   ok
 09:51:02  rcowell  admin  session.login  —                            ok
 09:50:44  —        —      session.login  rcowell           denied (bad password)
```

**Cluster** (U3) — roles, health, convergence.

```
 Node      Role        Health   Ver     Cfg  Lag        
 ───────────────────────────────────────────────────────
 tldb-i1   ingester    ● up     0.5.0   42   repl 0.4s
 tldb-i2   ingester    ● up     0.5.0   42   repl 0.4s
 tldb-q1   querier     ● up     0.5.0   42   3 in flight
 tldb-c1   compactor   ◐ elect  0.5.0   41   ⚠ behind 1 revision
```

---

## 11. Guardrails on destructive actions (RR-5)

1. **Impact preview before the fact.** Any retention change that would
   drop data returns, from `?dry_run=1`, the exact partition count, byte
   count and time range that will go. The number comes from the catalog,
   not an estimate.
2. **Typed confirmation** above a threshold (default: any drop over
   1 GiB or 24 h of data) — the operator types the table name.
3. **Delay and undo where it is free.** Dropped files survive the GC
   grace; within that window the console offers "restore the window",
   which re-widens the policy before physical deletion. After the grace,
   the UI says so plainly instead of implying recoverability.
4. **Global read-only switch** (`TIMELAKE_ADMIN_READ_ONLY=1`) for
   change-freeze periods: the console renders, nothing mutates.
5. **Rate limits** on mutating routes, per principal, audited on trip.
6. **Every guardrail is visible**: each shows its own configured value and
   who can change it — a limit you cannot see is the failure mode RR-5
   was written against.

---

## 12. Non-goals for v1

- Not a query IDE. `/api/sql`, Flight SQL and Grafana cover that; the
  console shows *queries the server ran*, not a workbench for new ones.
- No dashboard builder and no alerting engine — `/metrics` plus
  Alertmanager, and the named alarms the engine already raises.
- No data browsing or editing. The console administers the server; SEC-2
  labels govern the data.
- No cross-node log aggregation before U3, and even then by proxying, not
  by shipping logs into a store.
- No multi-tenant administration. One deployment, one principal store.

---

## 13. Phases and gates

Each phase is gated by something the harness or a drill can check, per
CLAUDE.md's rule that no milestone is done on unit tests alone.

**U0 — Admin plane: layered config, auth, retention rebuilt.**
The admin listener on 1965 with TLS and SEC-4 (bootstrap, roles,
sessions, tokens); `timelake-config` with the three layers, provenance,
pinning, validation and hot-swap; the settings inventory of §3.5;
retention migrated onto it (three-state override, impact preview);
`/admin/*` moved off 1963 with `410` stubs.
*Gate*: a full-scale bench run green with every tunable set through the
console rather than the environment; restart with a stale property keeps
overrides **and** logs the divergence; unauthenticated admin request
returns 401 and is audited; `gc_grace_secs ≤ query_timeout_secs` is
rejected with its invariant named.

**U1 — Logs and audit.**
Ring-buffer app log with snapshot and SSE tail; `timelake-audit` with the
hash chain, segments, upload, `system.audit`, and the verifier script.
*Gate*: an audit drill in which every mutating route produces exactly one
record (including denials), the chain verifies, a hand-edited record is
detected at the right sequence number, the sink survives SIGKILL
mid-write, and mutations fail closed with the sink unavailable.

**U2 — Metrics and performance views.**
The metrics of §7.4, the sample ring, and the Overview/Ingest/Storage/
Query/Security views.
*Gate*: console numbers agree with `/metrics` and with a bench run's
`run.json` within tolerance; RR-4 idle footprint unchanged with the ring
and log buffer at full size; the Query view reproduces the known
fresh-vs-settled effect (Shape B on just-ingested vs settled data) without
the operator running the harness.

**U3 — Cluster view.** *(after C2)*
Discovery-backed node table, drill-in proxying, config convergence,
degraded-mode banners.
*Gate*: killing a node shows it within 10 s with the right role and
health; a node held at an old revision is flagged; a wrong or stale
membership view changes nothing about write or catalog correctness
(CL-5 guard, drilled).

U0–U2 are independent of the C track and can proceed in parallel with
C1; U3 depends on the C2 role split.

---

## 14. Decisions and alternatives considered

| Decision | Chose | Over | Because |
|---|---|---|---|
| Property vs GUI | Layered with provenance and revert | GUI-wins-after-boot (today's behaviour); property-always-locks | Keeps both facts true and visible; pinning (§3.4) recovers the locking model per key for deployments that need it |
| Override absence | Three states, `null` ≠ absent | Two states | "Revert to the property" and "off regardless of the property" are different intents; conflating them was the shipped bug |
| Console transport | Separate listener, private by default | Path on 1963 | The most destructive surface must not inherit the data plane's exposure |
| Auth scope at U0 | Admin plane only | Whole-server auth at once | Data-plane auth breaks every client and Telegraf/Grafana fixtures; it deserves its own migration, not a side effect |
| Audit storage | Append-only chained segments outside the tables | A regular table in the DB | The retention UI must not be able to delete the record of its own use; the sink must work when the engine is unhealthy |
| Audit failure | Fail closed | Fail open | An unrecorded administrative change is worse than a refused one; opt-out exists and is loud |
| App logs | Bounded ring + stdout | Ingest logs into TimeLakeDB | Self-ingestion amplifies writes exactly when the server is unhealthy (RR-4) |
| History | 6 h in-memory sample ring | Persisted metrics store | Grafana is the real answer (FR-8); the console needs enough to triage, not a second TSDB inside the TSDB |
| Charts | Hand-drawn SVG | A charting library | Same rule as `site/`: no build step, no external assets, CSP stays strict |
| Hot apply | `ArcSwap` snapshot, per-query capture | Mutex-guarded live config | Proven in `timelake-tls`; in-flight work keeps a consistent view and readers stay lock-free |

## 15. Risks, each with its falsification test

1. **The console destabilises the data plane** — SSE tails, metric
   scraping and audit fsyncs compete with ingest. *Test at U2: a bench
   full-scale run with ten console sessions attached, tailing logs and
   polling every view; ingest and Shape A/B must stay within tolerance of
   the unattached baseline.*
2. **Hot config application races the maintenance tick** — a snapshot
   swapped mid-flush could apply half a policy. *Test at U0: a loop that
   flips `flush_rows`, `compact_min_files` and retention every 200 ms
   during a full-scale ingest; exactness check must still be row-exact.*
3. **Audit fsync per record throttles bursty administration** — plausible
   only under automation. *Test at U1: 1,000 sequential config writes;
   record the per-write latency and confirm the data plane is unaffected.*
4. **Session auth on the admin listener becomes the outage** — a bug that
   locks every operator out during an incident. *Mitigation and test at
   U0: the bootstrap token path works on a running server with an intact
   principal store (documented break-glass), drilled in the U0 gate.*
5. **The three-state override confuses operators more than it helps.**
   *Falsified or confirmed by the U0 UI: if the retention page needs more
   than one sentence to explain "revert" vs "off", collapse to two states
   and make "off" a distinguished value instead.*
6. **`put` last-writer-wins loses a concurrent admin's change** —
   two admins, two tabs. *Accepted at U0 (admin config, not data),
   closed at C1 by revision-preconditioned CAS with a 409 diff.*

## 16. Open questions

- Should `viewer` be able to read the audit log, or is that an
  `operator`+ capability? Operators' logins and lockouts are in there.
- Does the console proxy drill-in (one login, U3) or redirect to each
  node's own admin listener (simpler, but N logins and N exposed ports)?
- Is a per-principal authorization grant (§4.5) the right model for
  SEC-2, or should grants attach to roles so that labels and roles share
  one administration surface?
- Should `system.audit` be readable through Flight SQL, given that
  Flight's authorizations are currently claims? Probably not before
  data-plane auth.
