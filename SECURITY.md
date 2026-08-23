# Security Policy

## Supported versions

TimeLakeDB is **pre-v1**. Only `main` is supported; there are no released
versions and no backports. The workspace version is `0.1.0`.

| Version | Supported |
|---|---|
| `main` | Yes — fixes land here |
| anything else | No |

## Reporting a vulnerability

Report privately through GitHub's **Security → Report a vulnerability**
(private vulnerability reporting) at
<https://github.com/timelakelabs/timelakedb/security/advisories/new>.
Please do not open a public issue for a suspected vulnerability, and do
not attack a deployment you do not own.

A useful report includes the version or commit, the configuration
(`TIMELAKE_*` variables, whether TLS is on), the exposure model (what a
reachable attacker can do), and a reproduction. Expect an acknowledgement
within a week; this is a single-maintainer project with no on-call rotation,
so please size your disclosure timeline accordingly.

## Current security posture — read this before deploying

**The data plane can now authenticate, but ships `off` by default.** As
of SEC-4 (phased), `TIMELAKE_DATA_AUTH=optional|required` turns on token
authentication on the data plane — issue tokens from the console, and
Grafana, Telegraf and Tributary present them on the `Authorization`
header (`Bearer`/`Token`/`Basic`, whichever the client speaks). Until an
operator sets that, **the default is `off`: any client reaching port
1963 or 1964 has full read and write access to every database**, exactly
as before, because turning it on is a breaking change for any client not
yet configured with a token. The three-mode migration (`off` →
`optional` → `required`) exists so that flip can be staged on a measured
split rather than taken blind. On a default (`off`) deployment, network
reachability is still the only access control over your data.

Until you set `TIMELAKE_DATA_AUTH`, treat a TimeLakeDB port as equivalent
to an unauthenticated shell into the data: bind it to localhost or a
private segment, and front it with an authenticating proxy if anything
but your own agents needs access. Setting `required` (with tokens issued
and clients configured) removes that constraint — it is the intended way
to expose a port beyond a trusted segment.

**What the `.deb` / `.rpm` do about that.** A distro package is a sharper
version of this problem than a container is: `apt install` that started a
service listening on every interface would hand an unauthenticated database
to whatever network the machine is attached to, before the operator had read
anything. So the packages are deliberately inert on install:

- the shipped `/etc/timelakedb/timelakedb.env` binds **`127.0.0.1` only**, on
  both the HTTP and Flight SQL ports;
- the systemd unit is installed but **not enabled or started** — that is an
  explicit `systemctl enable --now timelakedb` after you have configured it,
  and the postinstall says so;
- the unit runs as a shell-less `timelake` account under
  `ProtectSystem=strict`, with `/var/lib/timelake` as the only writable path
  — the same shape as the container (exposure 4);
- `packaging/verify.sh` asserts the loopback default on every supported
  distro, so a future edit that widens it fails the release build.

Changing `TIMELAKE_ADDR` to a routable address is the moment the guidance
above starts applying to you. Uninstalling never deletes `/var/lib/timelake`.

| Control | Status |
|---|---|
| Transport encryption | **Implemented, opt-in.** TLS 1.3 on both listeners when `TIMELAKE_TLS_CERT`/`_KEY` are set, with hot rotation (SEC-3). Plaintext is the default. |
| Client certificate / mTLS | **Implemented, opt-in, WANT mode.** Set `TIMELAKE_TLS_CLIENT_CA` and both listeners request a client certificate, verify one if presented, and serve the connection either way — so Grafana, Telegraf and the harness need no change. A verified identity narrows that session's SEC-2 authorizations to what it is granted, on **both** Flight SQL and `/api/sql`. Trust anchors hot-rotate with dual-CA overlap. Want mode is not itself a control — see exposure 9. |
| Authentication | **Admin surface (SEC-4) + data plane (SEC-4 phased).** `/admin/*` requires a session (Argon2id, cookie/bearer, CSRF + Origin, backoff). The **data plane** authenticates by token when `TIMELAKE_DATA_AUTH` is `optional` or `required`: one token on the `Authorization` header, accepted as `Bearer` (Grafana Flight SQL / Tributary), `Token` (Telegraf v2) or `Basic` (Telegraf v1, token as password). **Default is `off`** — the header is not examined and the data plane is open, as it always was. HTTP and Flight SQL enforce through one decision function. In a cluster the token file lives in the shared store and every node re-reads it on its maintenance tick (a querier, on its catalog tail) and once on an unknown token, so issue and revoke take effect cluster-wide within about ten seconds (#46). |
| Authorization | **Roles on the admin surface; scopes + grants on data tokens.** Admin roles: `viewer`/`operator`/`admin`. Data tokens carry a scope (`read`, `write`, `read_write` — deliberately not a total order, so a shipper can write without being able to read back), an optional database allowlist, and optional SEC-2 grants that *intersect* a caller's claimed authorizations. No per-column permissions. |
| First-run credential | **`admin`/`admin`, quarantined.** Seeded only when no principal exists; it may do nothing but change its own password, and every other admin route answers `403 password_change_required` until it does. Rotating it invalidates all its sessions. `TIMELAKE_ADMIN_BOOTSTRAP_PASSWORD` replaces it for provisioning. Alert on `timelake_admin_default_credential_active`. |
| Tenancy isolation | **Not a boundary.** `org` is accepted and ignored; databases are namespaces only. |
| Encryption at rest | **Implemented, opt-in.** Set `TIMELAKE_ENCRYPTION_KEY` (64 hex chars) or `TIMELAKE_ENCRYPTION_KEY_FILE` and every object written to the store — Parquet, manifests, checkpoints — is envelope-encrypted (per-object AES-256-GCM data key, wrapped by the configured key). Objects written before the key was set stay readable (plaintext passthrough). Since **SEC-8** the local WAL (and the replica WAL) is encrypted with the same envelope key — see exposure 8. |
| Row visibility labels | **Implemented.** A `_visibility` tag holding an Accumulo-style expression (`(ops&audit)\|admin`) restricts rows to sessions presenting satisfying authorizations (`X-TimeLake-Authorizations` header / Flight SQL metadata). Enforced inside the scan, so aggregates cannot leak. **Authorizations are unauthenticated claims** — see exposure 7. |
| Targeted delete / erasure | **Implemented (R-1), `admin` only.** `POST /admin/delete` records a durable tombstone — a `(tag equalities AND time window)` predicate — that hides every matching row from every query at once, in the live buffer and in settled files, and from `COUNT(*)` as much as from `SELECT`, because it is enforced at the same in-scan point as visibility. A background pass then physically rewrites the files so the bytes are gone from the settled store (deferred GC covers in-flight readers). An empty predicate is refused; deletes are irreversible and go to the write path, not a querier. See [Targeted delete (R-1)](#targeted-delete-r-1). |
| Audit logging | **Admin mutations (P1-2 / SR-6).** Every administrative mutation — retention set/remove, targeted delete, token and cert-grant lifecycle, password change — writes one fsync'd, hash-chained record attributing it to its authenticated principal, with the resolved before/after state. **Fail-closed**: while the sink cannot append, mutations are refused with `503 audit sink unavailable` (`TIMELAKE_AUDIT_FAIL_OPEN=1` overrides). Read via `GET /admin/audit` (viewer role) with filters and `?verify=1` for a whole-chain check; `timelake_audit_records_total` / `timelake_audit_sink_healthy` on `/metrics`. Tamper-*evident* (a per-node SHA-256 chain), not tamper-proof — an external anchor is future work. **Still not covered:** data-plane reads/writes are unattributed (no data-plane principal by default, exposure 1/7), and session login/logout auditing is a documented follow-on. See [Audit trail (P1-2)](#audit-trail-p1-2). |
| Availability guardrails | **Implemented.** Shared query memory pool, admission semaphore, server-side query deadline (RR-1), and WAL backpressure as an explicit 429 (RR-5). These bound resource exhaustion; they are not access control. |

## Known exposures

These are verified properties of the current build, not hypotheticals. They
follow from "no authentication" and are listed so you can design around them.

1. **Ingest and query are unauthenticated by default** on `:1963` (line
   protocol, `/api/sql`) and `:1964` (Flight SQL): anyone reachable can
   write arbitrary data, read all data, and enumerate the schema. This is
   the `off` default. Setting `TIMELAKE_DATA_AUTH=required` closes it —
   both ports then refuse any request without a valid token — but that is
   a deliberate opt-in, because it breaks every client not yet holding
   one. `optional` is the migration state between the two: anonymous
   still served, invalid tokens refused, and the
   `timelake_data_requests_*` split shows how much traffic would break at
   the flip.

2. **~~`POST /api/sql` can `COPY … TO` files as the server process.~~
   CLOSED (P0-2).** The data-plane SQL surface is now read-only, enforced
   on the *logical plan* DataFusion built rather than the query text:
   `SELECT`, `SHOW`, `DESCRIBE` and `EXPLAIN` run; `COPY`, DDL, DML and
   session statements are refused — including a `COPY` hidden inside
   `EXPLAIN ANALYZE`. HTTP and Flight SQL share the one enforcement point
   (`docs/evidence/sql-sandbox-drill.log`; the same request that wrote a
   root-owned Parquet file now returns a refusal and writes nothing).
   Arbitrary file *reads* remain unreachable (no `read_parquet`/`read_csv`
   table functions registered, and `CREATE EXTERNAL TABLE` is refused by
   the same guard). The check is deny-by-default: a future DataFusion plan
   node fails the build rather than slipping through.

3. **~~`POST /admin/tls/reload` is unauthenticated.~~ CLOSED (SEC-4).**
   It now sits behind the same session guard as the rest of `/admin/*`
   and requires the `admin` role. The behaviour that limited its impact
   is unchanged — it only re-reads the already-configured cert and key
   paths, validates before swapping, and keeps the last-good pair on
   failure.

3a. **~~`/admin/retention` is an unauthenticated deletion control.~~
   CLOSED (SEC-4).** `/admin/*` now authenticates every request. What
   replaces it is smaller but real: **a fresh node seeds `admin`/`admin`**
   (see below), so the window between first start and the first password
   change is a window in which a reachable attacker can take the console.
   The seeded credential can do nothing but change its own password, and
   `timelake_admin_default_credential_active` is 1 until it does — but the
   mitigation is procedural, not structural. Set
   `TIMELAKE_ADMIN_BOOTSTRAP_PASSWORD` to skip the well-known default
   entirely, or change the password immediately after first start.

4. **~~The container runs as root.~~ CLOSED (P0-2).** The image now runs
   as an unprivileged user (`USER timelake`, uid 1000), and the shipped
   compose mounts the root filesystem read-only with the data volume as
   the only writable path (`read_only: true`, `tmpfs /tmp`). Defence in
   depth behind (2): a write primitive that ever slipped the SQL guard
   would run as a non-root uid against a filesystem it cannot write.
   **Upgrade note:** a data volume created under the old root image is
   root-owned; uid 1000 cannot open it and the node exits with
   `open engine (recovery): Permission denied`. Chown the volume to
   `1000:1000` once, or start on a fresh volume.

5. **~~Query errors are returned verbatim~~, including DataFusion planning
   errors that disclose table and column names. CLOSED (SEC-5).** A query
   that fails to plan or execute now returns one opaque message —
   `query could not be executed (ref: q-XXXXXXXX)` — on both `/api/sql` and
   Flight SQL. The full DataFusion error is logged server-side against that
   `ref`, so an operator diagnoses from the reference the caller quotes
   while the caller learns only that the query failed. This closes the
   column-enumeration variant too: a bad column name previously made
   DataFusion return `No field named X. Valid fields are ...` and list every
   real column of the table. Sanitized at the one shared execution point
   (`crates/query` `run_sql_env`), so the HTTP and Flight surfaces cannot
   diverge. Deliberately-safe messages are unchanged and still returned in
   full: the read-only-guard refusal (exposure 2) names a statement class,
   the timeout and the "database does not exist" message name the caller's
   own input — none discloses schema.

   *Amended 2026-08-23 (#47):* one failure escaped `run_sql_env` and so its
   sanitizer. Before a query runs, the read path unions the schemas of a
   table's live batches, and a schema conflict there — `column 'x' has
   conflicting types Utf8 vs Float64`, naming a column and both Arrow types
   — was raised in `Engine::sql_batches` and returned verbatim. Narrow to
   reach (two live batches of one table must disagree on a type, which
   first-writer-wins typing prevents within a node, but a pre-#43 leftover
   table or a cross-node querier union can produce), but a real schema
   disclosure on the read surface. That one call site now routes through
   `timelake_query::opaque_read_error`, the same opaque policy with its own
   ref. The *write*-path union keeps its verbatim message on purpose — a
   write that conflicts with a stored type is told which field, by design.

6. **~~No rate limiting per client.~~ CLOSED (SEC-6).** The memory pool and
   admission semaphore (RR-1) bound the TOTAL number of concurrent queries;
   they did nothing to stop one client from taking every permit and starving
   the rest. A per-client concurrency cap now sits in front of the admission
   semaphore: a client already holding
   `TIMELAKE_MAX_CONCURRENT_QUERIES_PER_CLIENT` queries (default 4, of the
   global 6) is refused — HTTP 429, Flight `ResourceExhausted` — rather than
   queued, so the refusal is visible to an operator and a probe alike. The
   client is keyed by its data-plane token when it presents one and by
   network origin otherwise, on both `/api/sql` and Flight SQL. The default
   keeps two permits always reachable by another client; raise it and
   `TIMELAKE_MAX_CONCURRENT_QUERIES` together for a single-tenant deployment
   whose one dashboard issues many concurrent panels —
   `timelake_query_rate_limited_total` makes a too-low cap visible. Only
   queries are capped (writes have their own backpressure, RR-5); `0`
   disables the cap. This bounds concurrency, not request rate: a flood of
   cheap, fast queries is still served, which is the object-store and
   admission budget's job, not this cap's.

7. **Visibility authorizations are self-asserted on the anonymous path.**
   `X-TimeLake-Authorizations: admin` is a claim any client can make.
   Two credentials now change this *for callers that present one*: a
   verified client certificate (exposure 9), and — new — a data-plane
   token whose grants intersect the caller's claims. Both only *narrow*
   what a caller sees, and both are optional under the `off`/`optional`
   defaults, so an attacker declines and keeps the honor-system front
   door. Under `TIMELAKE_DATA_AUTH=required` the door is shut: every
   caller holds a token, and a token with recorded grants cannot claim
   beyond them. Short of that, real isolation still needs an
   authenticating proxy that *sets* (and strips inbound) the header, or a
   deployment migrated far enough to run `required`.

8. **~~Encryption at rest does not cover the local WAL~~, which holds recent
   line-protocol bytes until flush. CLOSED (SEC-8).** When at-rest encryption
   is configured (`TIMELAKE_ENCRYPTION_KEY[_FILE]` or `TIMELAKE_KMS_KEY_ID`)
   the WAL is now encrypted with the SAME envelope key as the object store:
   each generation file carries a per-file data key wrapped by the KEK, and
   every frame is sealed with AES-256-GCM. Turning on store encryption covers
   the WAL — and the durable replica WAL — with no extra flag. Plaintext
   segments written before a key was configured still replay (passthrough,
   keyed on a file-level magic, exactly as the object store passes through
   pre-encryption objects), so enabling it needs no migration. Replay fails
   **closed**: an encrypted segment with no key, or a whole frame that fails
   authentication, refuses to start rather than silently dropping an acked
   write; only an incomplete *trailing* frame — a crash mid-append — is the
   tolerated torn tail. Key material is held in process memory and sourced
   from the environment. What this still does NOT do, by design, is protect
   data from anyone who can reach the query API — queries decrypt
   transparently; that is authentication's job (SEC-4), not encryption's.
   Encryption at rest protects the media: a stolen volume, a decommissioned
   disk, an S3 bucket leak, and now the WAL among them.

9. **Want-mode client authentication is optional by design, so it
   grants nothing on its own.** An attacker simply declines to present a
   certificate and takes the anonymous path, which still behaves exactly
   as it did before. Its value is that an *authenticated* caller can be
   held to less: a verified identity's SEC-2 claims are intersected with
   its grants. Making the anonymous path more restricted is a separate,
   deliberate decision, and it should rest on a measurement rather than a
   guess: `timelake_flight_connections_authenticated_total` against
   `timelake_flight_connections_anonymous_total` says how much of your
   traffic would break if you required a certificate today.

   *Updated 2026-08-18:* this now applies to **both** query surfaces. It
   previously applied only to Flight SQL — `/api/sql` requested and
   verified a client certificate and then authorized nothing with it,
   because `axum-server` owns that accept loop and the handler could not
   see the peer. A custom `Accept` (`crates/server/src/tls_identity.rs`)
   reads the subject CN once per connection and layers it onto the
   service, so the grant intersection applies identically on HTTP. The
   practical consequence is that Tributary's L4 client certificate now
   authorizes something on the write path instead of only proving a
   handshake. The identity is also recorded per query in
   `_system.queries`, so "which client is doing this to us" is an
   answerable question rather than an inference from logs.

10. **The intra-cluster listener is unauthenticated and now serves
    rows.** `TIMELAKE_CLUSTER_ADDR` carries CL-2 replication and, since
    CL-3, `GET /internal/v1/snapshot` — a table's live buffer as Arrow
    IPC — and `/internal/v1/live`. Two consequences, both deliberate and
    both requiring that this port stay on a private network:
    it applies **no data-plane token check** (trust is the network now,
    the peer certificate at C3), and it applies **no SEC-2 visibility
    filter**, because the querier re-applies the caller's restriction
    when it scans those batches, exactly as it does for a file it reads
    from the object store. That makes reaching this port equivalent to
    read access to the bucket itself. A querier is only as private as its
    ingesters' cluster addresses.

## Targeted delete (R-1)

A recorded delete is a **standing predicate**, not a one-shot scrub. `POST
/admin/delete` (admin role) takes `{db, table, tags{}, start_ns?, end_ns?}`
and commits a *tombstone* to the manifest log — the same append-only,
CAS-guarded log that carries file additions — so it is durable, replays on
restart, and propagates to every querier the moment it is committed. A row
matches a tombstone when it satisfies **all** of the tag equalities **and**
falls inside the `[start_ns, end_ns]` window (either bound may be omitted).

Two layers, and the security property rests entirely on the first:

- **Logical (immediate, everywhere).** The tombstone is enforced inside the
  scan through the same mandatory-predicate hook as row visibility — below
  every user predicate and before any aggregation. So a deleted row is
  invisible to `SELECT`, to `COUNT(*)`/`SUM(...)`, and across every storage
  layer (live buffer, just-flushed holding area, settled Parquet) the
  instant the delete is committed, with no rebuild and no window. A
  tombstone is scoped to its `(db, table)`: the same tag value in another
  table is untouched.
- **Physical (background reclamation).** A maintenance pass
  (`apply_tombstones_once`, on the compaction cadence) rewrites any file
  that still holds matching rows and drops files emptied entirely, so the
  bytes leave the settled store — the "actually gone", not merely "not
  returned", half. Superseded files leave on the normal GC grace timer, so
  an in-flight query holding an old catalog snapshot never dangles.
  `timelake_tombstone_rewrites_total` counts the work. Because the tombstone
  stays a standing filter, data written to a matched predicate *after* the
  delete is hidden immediately (logical) and reclaimed on a later pass
  (physical).

Residual properties to design around, all deliberate:

- **The predicate must be non-empty.** A delete with neither a tag match nor
  a time bound is refused — erasing a whole table is retention's job (and a
  future explicit `DROP`), not this surface, so a malformed request cannot
  wipe everything.
- **Deletes are `admin`, and unauthenticated by default like every surface.**
  `/admin/*` authenticates (SEC-4), but the seeded `admin`/`admin` window
  (exposure 3a) applies here too: close it before exposing the console.
- **The WAL is not scrubbed by a delete.** A tombstone governs the settled
  store and the query path; recent line-protocol bytes for a since-deleted
  row can persist in the WAL until it rotates. Where a delete must guarantee
  the bytes are gone from *all* media, pair it with at-rest encryption
  (exposure 8) so the WAL residue is ciphertext.

Pinned by `crates/catalog` (tombstone durability + CAS replay),
`crates/query` (`Restriction::Tombstone`, the in-scan filter and its
aggregate-leak test) and `crates/server/tests/delete.rs` (end-to-end across
buffer and files, the time window, cross-table isolation, the admin guard,
idempotency, and a drill that reads the raw Parquet bytes back and asserts a
deleted value is physically absent).

## Audit trail (P1-2)

Every administrative mutation writes one record to a per-node,
hash-chained, append-only log (`<data_dir>/audit/`, fsync per record). The
record names **who** (the authenticated principal and role), **from where**
(the request source), **what** (a dotted action such as `retention.set`,
`token.issue`, `data.delete`, `cert_grants.remove`, `password.change`),
**on what** (the target), and the **resolved before/after** — so it answers
"what actually changed for the server", which is the question an incident
asks. A denial is recorded too (`outcome: "denied"`), and reading the log
is itself audited (`audit.read`).

- **Tamper evidence, not tamper proofing.** `hash =
  SHA-256(record-without-hash || prev_hash)`, a per-node chain from a fixed
  genesis. `GET /admin/audit?verify=1` walks the chain and reports the first
  break by seq: an edited field, a deleted record (seq gap), or a broken
  link all surface. What it does **not** stop is someone with write access
  to the file rewriting the *whole* chain — detecting that needs an external
  anchor (a WORM bucket or a signed head), designed-for but not built.
- **Fail-closed (§5.5).** If the sink cannot append, a mutation is refused
  with `503 audit sink unavailable` and the door stays shut until the sink
  recovers — an administrative change that leaves no record is worse than
  one that did not happen. `TIMELAKE_AUDIT_FAIL_OPEN=1` inverts this for an
  operator who would rather keep mutating and be alerted; the choice is
  itself a deployment-time decision, not a console one.
- **Read surface.** `GET /admin/audit` (viewer) filters by
  `action`/`principal`/`target`/`since` and returns the most-recent page
  (`limit`, default 1000). `timelake_audit_records_total` and
  `timelake_audit_sink_healthy` are on `/metrics`.

Scope and residuals, all deliberate for this slice:

- **Admin mutations only.** The data plane is unauthenticated by default
  (exposure 1), so a write or query has no principal to attribute — data-plane
  auditing arrives with `TIMELAKE_DATA_AUTH=required` and a token identity.
- **Session login/logout are not yet chained.** Login already emits metrics
  and structured logs (`timelake_admin_logins_total` /
  `_login_failures_total`); folding those events into the audit chain is a
  follow-on, as is threading a session id (the admin `SessionInfo` carries
  none today) and a request-correlation id.
- **The recorded denials are engine/policy denials** (`outcome: "denied"` —
  an empty-predicate delete, a rejected password). A role-based `403` refused
  by the admin guard *before* the handler runs is not yet chained; folding
  guard denials into the trail is part of the same login/logout follow-on.
- **Local segments, rotated but not uploaded.** The trail rotates into
  ordered segments (`TIMELAKE_AUDIT_ROTATE_SIZE`, default 64 MiB, and
  `TIMELAKE_AUDIT_ROTATE_EVERY`), and **the chain verifies straight through
  the boundaries** — `?verify=1` reads every segment in order, so removing a
  whole segment file surfaces as a break exactly like editing a record does.
  Deleting a file is not a way to hide anything.

  **Retention deletes nothing by default.** `TIMELAKE_AUDIT_RETAIN_DAYS` is
  opt-in and is clamped to a 90-day floor even when set lower
  (`docs/CONSOLE.md` §5.4), so the retention control cannot erase the record
  of its own use. Object-store upload on rotation (SEC-1 encrypted) and the
  read-only `system.audit` SQL exposure remain the next enhancements.

  Drilled against a live node, not only unit-tested:
  `docs/evidence/audit-rotation-drill.log` — 40 mutations across 5 segments,
  `?verify=1` intact after rotation, and a removed segment reported as
  `{"ok":false,"break":{"seq":10,"reason":"prev_hash does not match …"}}`.

Pinned by `crates/audit` (the chain: link, replay, tamper detection on edit
and deletion, fail-closed gate) and `crates/server/tests/audit.rs`
(end-to-end attribution, `?verify=1`, a recorded denial, self-audited reads,
and the viewer gate).

## Dependency advisories

Advisories against crates in the dependency tree, and what was done about
each. Recorded here rather than left as open alerts in a dashboard, because
"we looked and it does not reach us" is a claim like any other and should be
written down where it can be argued with.

**Closed 2026-08-13 — three advisories against `rustls-webpki` 0.101.7**
(GHSA-82j2-j2ch-gfr8, high, denial of service via panic on a malformed CRL;
GHSA-xgp8-3hg3-c2mh and GHSA-965h-392x-2mh5, low, name-constraint parsing).
Neither was reachable: rustls only parses a CRL if the client is configured
with one, and the AWS SDK does not configure any. They are closed anyway,
and at the root rather than by pinning. `aws-sdk-s3` and `aws-sdk-kms` carry
`rustls` in their *default* features, which resolves through
`aws-smithy-runtime/tls-rustls` to `legacy-rustls-ring` and so to rustls
0.21. Their `default-https-client` default already supplies the current
stack (`rustls-aws-lc`, rustls 0.23), so the obsolete TLS implementation was
being compiled into the binary beside the one actually in use. Turning that
one feature off removes it entirely.

**Closed 2026-08-21 — `thrift` 0.17.0, GHSA-2f9f-gq7v-9h6m** (medium,
CVSS 5.3, availability only: memory allocation with an excessive size
value). It arrived as `thrift` → `parquet` (the non-optional `arrow`
feature) → `datafusion` → this workspace, and for a week there was
nothing to pin: arrow-rs had dropped the external `thrift` crate in
**`parquet` 59**, but the then-latest `datafusion` 54 pinned `parquet
^58.3.0` and rejected 59 (verified 2026-08-15: `cargo update -p parquet
--precise 59.0.0` → "candidate versions found which didn't match"). It
closed the way it was always going to: **DataFusion 55** (PR #27) brings
`parquet` 59.2.0, and `cargo tree -i thrift` now reports no such package.
The dependency is gone rather than patched, so the next `thrift` CVE
cannot arrive down this chain either. `paste` and `lru`, flagged on the
same chain, cleared with it.

While it was open the assessment was: the reachable path is parsing
Parquet metadata from an untrusted source, and TimeLakeDB reads Parquet
that TimeLakeDB wrote, from its own object store, so reaching it meant
already holding write access to the bucket. That reasoning is kept here
because it is the reasoning that would apply again if this database were
ever pointed at files it did not produce.

## Deploying it safely today

- **Do not expose 1963 or 1964 to an untrusted network.** Bind to `127.0.0.1`
  (`TIMELAKE_ADDR=127.0.0.1:1963`) or to a private Docker/Kubernetes network,
  and publish nothing.
- **Never publish `TIMELAKE_CLUSTER_ADDR`.** In a cluster it is the peer
  link: replication frames in, live rows out, no authentication, no
  visibility filter (exposure 10). It belongs on the private network the
  nodes share and nowhere else.
- **Turn on data-plane auth** (`TIMELAKE_DATA_AUTH=required`) once tokens are
  issued and clients hold them — this is the native way to make a port safe to
  expose. Stage it through `optional`, watching `timelake_data_requests_*`, so
  you flip to `required` only when the anonymous count has reached zero. Or
  **front it with a proxy that authenticates** — note Flight SQL is gRPC over
  HTTP/2, so a proxy before `:1964` must speak HTTP/2.
- **Enable TLS** (`TIMELAKE_TLS_CERT`/`_KEY`) — and if you rely on token auth
  over a network, TLS is not optional, because a `Bearer` token on a plaintext
  connection is a password in the clear.
- **Set a container memory limit.** `mem_limit` is not optional in practice —
  an unbounded engine took down an entire Docker VM during development.
- **Keep the shipped container hardening.** The image runs non-root and the
  compose mounts the root filesystem read-only with the data volume as the
  only writable path (P0-2). If you write your own deployment manifest,
  carry `read_only: true`, a `tmpfs` for `/tmp`, and a data volume owned by
  uid 1000 — do not run it back as root.
- **Alert on certificate health**: `timelake_tls_last_reload_ok == 0` and
  `timelake_tls_cert_expiry_seconds` below two renewal periods. A failed
  renewal keeps serving on the last-good pair, which is exactly why it can go
  unnoticed until expiry.
- **Alert on `timelake_data_requests_anonymous_total` in `required` mode.**
  It must stay flat at zero; any increase means something is still reaching
  the data plane without a token — a misrouted client, or a gap you have not
  closed.

## Roadmap

| Item | Requirement | State |
|---|---|---|
| TLS 1.3 both listeners, hot rotation | SEC-3 (v1 MUST) | **Shipped** — AT-7 drill 19/19 |
| Admin authentication + roles | SEC-4 (v1 MUST) | **Shipped** — sessions, Argon2id, CSRF, forced first-run rotation |
| Data-plane authentication | SEC-4 (phased) | **Shipped** — token auth on both listeners via `TIMELAKE_DATA_AUTH=off\|optional\|required`, scopes + database scoping + SEC-2 grants, one decision function for HTTP and Flight, drilled live (`docs/evidence/data-auth-drill.log`). Turns SEC-2 claims into authorization. Tributary presents the token (P0-5, done — Tributary repo `docs/evidence/p05-data-auth.log`). |
| Client certificates, want mode | SEC-3 (v2) | **Shipped** — opt-in via `TIMELAKE_TLS_CLIENT_CA`, hot-rotating anchors with dual-CA overlap, identity plumbed into the query session on **both** Flight SQL and `/api/sql` (the latter via a custom `axum-server` `Accept`, 2026-08-18). AT-7 still 19/19 with it enabled. |
| Mutual TLS *required*, intra-cluster | SEC-3 (v2) | Not started — want mode is the client-compatible half; requiring it is a C2/C3 decision for the intra-cluster listener, where there is no Grafana to keep working |
| Encryption at rest | SEC-1 (design constraint v1, implement SHOULD v2) | **Shipped early** — envelope encryption at the store chokepoint, opt-in by key config; AWS KMS as a key source (`TIMELAKE_KMS_KEY_ID`, C0) and the WAL (SEC-8) are covered. Per-column keys (Parquet Modular Encryption) remain open at the same seam. |
| Row visibility labels | SEC-2 (design constraint v1) | **Shipped** — `_visibility` labels enforced in-scan via the mandatory-predicate hook. |

`REQUIREMENTS.md` §8 is the full security specification and explains why
SEC-1 and SEC-2 are constrained now and implemented later.
