# Production readiness — TimeLakeDB + Tributary

A prioritised path to running both products in production, for someone
who is not their author. Written 2026-08-10. The umbrella roadmap —
which merges these priorities with a competitive analysis of the
inspired projects (InfluxDB v1/v2/v3, QuestDB, VictoriaMetrics) and the
shipper field (Telegraf, Vector, Fluent Bit) — is
[`ROADMAP.md`](ROADMAP.md); this document is its operational half.

Everything here is scored against what is **actually in the repos**, not
against a wish list. Where an item is already proven, the drill that
proves it is named; where it is not, the gap is stated plainly.

---

## 0. The honest summary

**TimeLakeDB is engine-complete and drill-proven; the operational shell
is most of the way in.** The hard parts — the storage engine, exactness
under crash and rotation, the RR-1 "no query may kill the server"
invariant, TLS rotation under load — are done and measured.

Most of the gap this document opened with has since closed: the data
plane can authenticate (token, opt-in, still `off` by default), the
container runs as an unprivileged user on a read-only root filesystem,
`/api/sql` is read-only on the logical plan, every repository is pushed,
`.deb`/`.rpm` packages ship with each tagged release, administrative
mutations are recorded in a hash-chained audit trail, and data can be
deleted on a predicate rather than only by retention.

What remains is genuinely operational. The cluster role split is four
phases in (ingester pairs replicate before the ack, a router shards
writes, stateless queriers read with exact freshness — each drilled), so
"one node, one copy" is now a *deployment choice* (`TIMELAKE_ROLE=all`)
rather than the only shape; the compactor role is built but gated behind
its work-avoidance layer (C2 phase 5b), so a cluster today still runs
compaction on an `all`-role node; C3 (Consul discovery, required
intra-cluster mTLS) is unstarted; and the data plane's default is open
until an operator sets `TIMELAKE_DATA_AUTH`. Read the per-item sections
below rather than this paragraph — each says what shipped and what is
still owed.

**Tributary is earlier but structurally sound.** Its three hardest
correctness properties — exact counts through rotation, crash resume
mid-millisecond, and absorbing a 60-second database outage without loss
— are drilled and exact. Its gap is not correctness, it is that it
cannot yet authenticate, cannot be discovered, and has no deployment
story.

Neither is production ready today. The distance is roughly **one quarter
of focused work for a pilot**, and the critical path runs through
security and durability, not features.

### What "production ready" means here

Five axes. An item earns priority by which axis it unblocks.

| # | Axis | Today |
|---|---|---|
| 1 | **Deployable by someone else** | Partly — pushed, CI recorded green, and `.deb`/`.rpm` now ship with each tagged release (verified installing and serving on Debian 12, Ubuntu 22.04, Rocky 9, AL2023). No Helm chart; no public release cut yet |
| 2 | **Access controlled and attributable** | Partly — the mechanisms exist but the defaults do not enforce them. Admin routes require a session (SEC-4); the data plane takes tokens but `TIMELAKE_DATA_AUTH` **defaults to `off`**, so a stock node still serves anyone who can reach it. Attribution is real: a hash-chained audit trail for every admin mutation (P1-2), client-certificate identity on both query surfaces (SEC-3 v2), and one row per query in `_system.queries` (U2). Not yet: data-plane auth on by default, and auditing of the data plane itself |
| 3 | **Survives node loss** | With the role split, yes for acknowledged writes — an ingester replicates every frame to its pair before the 204 and a SIGKILL'd ingester recovers on the peer with zero acked loss (`docs/evidence/cl2-replication-drill.log`, 12/12); a querier is stateless and rebuilds from the bucket. Not yet: automatic failover (recovery is an explicit `/recover`), and a compactor that can run on its own node. A default `TIMELAKE_ROLE=all` deployment is still single node, single volume, RPO = last backup |
| 4 | **Failures visible before outages** | Yes — query latency/admission/outcome histograms, per-table storage, lifecycle lag and write-refusal causes on `/metrics` (U2, 2026-08-18); the hash-chained audit trail (P1-2); a documented alert list in `site/docs/reference.html`; and a self-monitoring Grafana console reading the node's own `_system` database. No alert *rules* are shipped — the list is prose, not a rules file |
| 5 | **Safe to upgrade** | Partly — no released version, no compat policy |

---

## 1. P0 — blocks *any* production deployment

These are not improvements. Each one is a reason to say "do not deploy
this yet."

### P0-1 · Push the repos; make CI actually run  ⟂ blocks everything

**Effort: S. Prepared 2026-08-10 — the push itself is the remaining step,
and it needs credentials this project's tooling does not hold.**

The original problem: `.github/workflows/ci.yml` — fmt, `clippy -D
warnings`, tests, an 80% coverage gate — **had never executed on a
runner.** Every quality claim in this repo rested on runs on one Windows
laptop through a Docker container. That is still true until the push
happens, so this item is not closed. What has changed is that the
workflows are no longer *unverified*: each step was run against a clean
target directory in a stock `rust:1-slim` container, which is the nearest
local equivalent of a cold runner.

Measured that way (2026-08-10):

| Step | Result |
|---|---|
| `cargo fmt --all --check` | pass |
| `cargo clippy --workspace --all-targets -D warnings` | pass |
| `cargo test --workspace` | 173 passed, 0 failed |
| `cargo llvm-cov … --fail-under-lines 80` | pass |
| `cargo test -p timelake-store-s3 -- --ignored` (LocalStack) | 3 passed |
| Tributary: fmt · clippy · `cargo test --workspace` | pass, 51 tests |

Decisions and changes made while preparing it:

- **The org is `timelakelabs`, and the repositories are lowercase under
  it** — `timelakelabs/timelakedb` and `timelakelabs/tributary`. Three
  spellings had accumulated (`TimeLakeLabs` in `Cargo.toml`, the README
  clone URL, the SECURITY.md advisory link and four site pages;
  `timelakedb` in `CLAUDE.md` as the "target org"), and the org that
  actually exists settles it. Every URL in both repositories now matches
  what is on GitHub, and the SSH alias and key are named
  `github.com-timelakelabs` / `id_ed25519_timelakelabs` to match.
- **The `store-s3` coverage hole is closed by a job, not by an
  exclusion.** A `store-s3` job runs the three `#[ignore]`d tests against
  a LocalStack service container — the Store contract over S3, the KMS
  envelope round-trip, and the P0-4 two-writer catalog CAS over
  `If-None-Match`. The coverage job still excludes the file, but the
  exclusion now means "counted in another job" rather than "not counted".
- **Tributary had no CI at all.** It has the same fmt/clippy/test gate now.
- **`pages.yml` would have failed on a private repository** (Pages is not
  available there on the Free plan), producing a red badge caused by
  visibility rather than by the site. It now skips unless the repository
  is public, and starts publishing by itself when that changes.
- **Coverage margin is thin.** The CL-3 work took workspace line coverage
  from 82.44% to 80.84% against an 80% gate. Rather than lower the gate,
  the router's forwarding surface — previously proven only by a live
  drill — gained integration tests against stub nodes.

Remaining, and it needs a human with GitHub credentials:

- ~~Push both repos to the `timelakelabs` org **private**, and confirm both
  workflows are green on a real runner.~~ **DONE (2026-08-13).** All five
  repositories are green on a runner at their current heads: 173 tests,
  82.74% line coverage, the `store-s3` contract against LocalStack, and
  tier-1 conformance against an image built from this tree. Run URLs and
  shas are in `docs/evidence/P0-1-ci.md`, which also records the three
  infrastructure failures found on the way — runner disk, a conformance job
  that executed no scenarios, and a harness that could not test itself
  alone. None was a defect in the software.
- Flip to public once they are, and enable Settings → Pages → Source:
  GitHub Actions so the site publishes.
- Tag `v0.1.0-alpha` so there is a fixed point to talk about.

**P0-1 is not closed.** The first item was the one blocking everything —
until it was done, no claim in this document traced to anything but one
laptop — but two of the three remain, and they are Phase 2 in
`PROJECT_PLAN.md`. Marking the item DONE on the strength of the first would
overstate it in precisely the way this project's standing rule forbids.

Riverkeeper's R0 — the one unsigned commit anywhere in the program — was
signed on 2026-08-13 by rewriting and force-pushing its history, so
`git verify-commit HEAD` now returns Good in all six repositories. The
rewrite is recorded in `docs/evidence/P0-1-ci.md`.

### P0-2 · `/api/sql` writes files as root  ⟂ **DONE (2026-08-10)**

**Effort: M. Shipped** — read-only SQL guard at the plan + non-root, read-only-rootfs container; drill `docs/evidence/sql-sandbox-drill.log`. The original text is kept below for the record.

**Effort: M.** SECURITY.md exposures 2 and 4 **compound into remote code
execution territory**, and this is verified, not theoretical: a single
unauthenticated request wrote a Parquet file outside the data directory,
and the image has no `USER` directive so it did so as root.

Data-plane auth (P0-3) does **not** fix this. It narrows *who* can do it
to *whoever holds any read-capable token* — which will include every
Grafana instance in the deployment. A read token must not be a filesystem
write primitive.

Three parts, all needed:

1. **Non-root `USER` in the Dockerfile** (S) — and a read-only root
   filesystem with the data dir as the only writable mount.
2. **Statement allowlist at the planner** (M) — accept `SELECT`,
   `SHOW`, `EXPLAIN`, `DESCRIBE`; refuse `COPY`, DDL, and anything else
   by *statement type*, not by pattern-matching the SQL text. A regex
   over query strings is not a security boundary.
3. **Re-drill it** — the exposure was found by trying it; the fix is
   only real when the same attempt returns a refusal.

Note the existing honesty in SECURITY.md: arbitrary file *reads* are not
reachable today only because no `read_parquet`-style table functions are
registered and `CREATE EXTERNAL TABLE` does not survive the per-request
session. Both are accidents of current configuration, not boundaries.
The allowlist makes it a boundary.

### P0-3 · Data-plane authentication  ⟂ **BUILT, NOT DEFAULTED ON (2026-08-13)**

**Every item this entry listed as remaining has shipped.** The Flight SQL
side, the engine implementation, the `/admin/tokens` management surface and
its console page, the authenticated/anonymous metrics split
(`timelake_data_requests_authenticated_total`) and the drills are all in:
routes at `crates/api/src/lib.rs:171`, integration tests in
`crates/server/tests/data_auth.rs`, transcripts in
`docs/evidence/data-auth-drill.log` and `sec4-auth-drill.log`. Riverkeeper
R0 independently verified the data-auth truth table on 23 assertions.

**It is still not closed, and the reason is the last line of this entry.**
The mechanism ships `off`: `crates/server/src/lib.rs:76` compiles in
`DataAuthMode::Off`, and only an explicit `TIMELAKE_DATA_AUTH` changes it.
So the sentence this entry opens with — anyone who can reach `:1963` or
`:1964` has full read and write access to every database — remains true of
a default install, which is the condition P0-3 exists to describe. Building
the lock is not the same as fitting it.

Closing this means defaulting to `optional`, taking the measured
authenticated/anonymous split from a real deployment, and only then
considering `required`. Until that first flip, treat the data plane as open
and rely on network isolation, exactly as `SECURITY.md` and the README say.

The original text follows unchanged, for the record.

**Status when written: in progress, design settled.**

**Effort: M (partially built).** Today anyone who can reach `:1963` or
`:1964` has full read and write access to every database. SEC-2's
visibility labels are enforced correctly but sit behind an honor-system
front door (exposure 7): `X-TimeLake-Authorizations: admin` is a claim
anyone can make.

The mechanism was chosen by measurement, not preference — see
`docs/evidence/data-auth-client-probe.log`. Grafana's Flight SQL path
forwards **only** the InfluxDB `token` field, as
`authorization: Bearer <token>`; its basic-auth toggle and custom
headers are HTTP-only and never reach gRPC. So the design is forced:
one token, accepted under three spellings (`Bearer`, `Token`, `Basic`
with the token as password), because Grafana, Telegraf v2 and Telegraf
v1 each spell it differently.

Already built (uncommitted): the token model (`crates/auth/src/token.rs`
— 256-bit secrets, SHA-256 digests rather than Argon2id with the
reasoning recorded, scopes, database scoping, grants, expiry,
revocation) and the single decision point
(`crates/auth/src/guard.rs`), 19 tests.

Remaining: the Flight SQL side, the engine implementation, the
`/admin/tokens` management surface and console page, the
authenticated/anonymous metrics split, and the drills.

**Ship it in `optional` mode by default.** The three-state
`off | optional | required` migration is the same discipline want-mode
mTLS used, and for the same reason: flipping straight to `required`
without a measured split of who is actually presenting credentials is
how a fleet goes down at once.

### P0-4 · Catalog commits are not atomic against a second writer  ⟂ **DONE (2026-08-10)**

**Effort: M. Shipped** — CAS on the next manifest sequence (`put_if_absent`), catch-up-and-retry on conflict, `timelake_catalog_commit_conflicts_total`; drilled on local hard-link and real S3 If-None-Match (`docs/evidence/catalog-cas-drill.log`). Original text below.

**Effort: M.** The CAS primitive exists — `Store::put_if_absent`, S3
`If-None-Match`, local hard-link publish, built during C0 — but **catalog
commits still use a plain `put`.** Single-node deployment makes this
latent rather than active, which is precisely why it is dangerous: it is
a silent-corruption class of bug that stays invisible until the first
moment two writers touch one bucket. A botched restore, an accidental
second container pointed at the same prefix, or the first day of
clustering all qualify.

This is C1 in `ARCHITECTURE.md` §12 and it gates everything in P2.

### P0-5 · Tributary cannot authenticate  ⟂ **DONE (2026-08-10)**

**Shipped** (Tributary repo) — `TRIBUTARY_TOKEN`/`token_file`, redacted two ways, a 401 spools rather than drops, drilled 10/10 against a required-mode node (`docs/evidence/p05-data-auth.log`). Original text below.

**Effort: S once P0-3 lands.** The moment TimeLakeDB can require a
credential, a shipper that cannot present one is not deployable.
`ship.rs` needs the `Authorization: Bearer` header, a credential source
(file, env, later Vault), and — easy to forget, hard to undo —
**redaction so the token never reaches a log line or an error message.**

---

## 2. P1 — blocks *unattended* operation

You can pilot without these. You cannot run them unwatched.

### P1-1 · Node loss loses data  ⟂ MOSTLY DONE — C2 phases 1–4 shipped 2026-08-10, phase 5a 2026-08-21

**Was Effort: L.** The original text below described a single node with
one volume. Since then the role split has landed one phase at a time,
each with a recorded drill:

- **CL-2 ingester replication** — every write is shipped to the paired
  ingester's durable replica WAL *before* the 204; a peer that is down
  degrades loudly (`timelake_cl2_degraded`) rather than failing writes;
  SIGKILL an ingester, recover on the peer, zero acknowledged loss
  (`docs/evidence/cl2-replication-drill.log`, 12/12).
- **Router** — stateless write sharding by `(db, measurement)`, whole
  body validated before any forward, queries forwarded round-robin to
  queriers (`router-sharding-drill.log`, 8/8).
- **CL-3 querier** — stateless, replays the catalog from the shared
  bucket, unions the ingesters' live buffers as Arrow IPC under a
  freshness watermark, refuses rather than under-counts when an ingester
  is unreachable (`cl3-querier-drill.log`, 19/19; the unmodified bench
  through the router: `rows_48h` 77,806 = the single-node value).
- **Compactor role** — built 2026-08-21 with a catalog tailer and a
  `/health` + `/metrics`-only surface; `Role::implemented` still refuses
  it. The reason is efficiency, not safety: the compaction commit fence
  (`Catalog::commit_replace`) already refuses a merge whose inputs were
  replaced, so two compactors are correct, but they would do double the
  IO to land half the merges. Phase 5b (work-avoidance) opens the gate.

**Still owed:** automatic failover (recovery is an explicit
`POST /internal/v1/recover` by an operator or the router today), a
startable compactor, and C3 — Consul discovery and *required* mTLS on
the intra-cluster listener (§3). A `TIMELAKE_ROLE=all` node remains what
the original text describes.

The original text, for the record:

**Effort: L.** One node, one volume. Backup and restore are drilled and
exact (34 s backup, 13 s restore, `docs/BACKUP_RESTORE.md`), so the
recovery story is real — but RPO is "since the last backup" and RTO
includes a human. `REQUIREMENTS.md` §7 makes replication and query HA
**v2 MUSTs**; they are not started.

Sequence: P0-4 (CAS) → C2 role split → WAL replication → query HA.
This is the longest pole on the list and the one most likely to be
underestimated. Start it early even though it finishes late.

### P1-2 · Audit logging — DONE for admin mutations (SR-6, 2026-08-16)

**Was Effort: M.** Every administrative mutation is now recorded to a
per-node, SHA-256-chained, fsync'd append-only log (`crates/audit`,
`<data_dir>/audit/`): who (principal + role), from where (source), what
(dotted action), on what (target), and the resolved before/after. Denials
are recorded too, and reading the log is itself audited. **Fail-closed** —
a mutation is refused with `503 audit sink unavailable` while the sink
cannot append (`TIMELAKE_AUDIT_FAIL_OPEN=1` overrides). Read via
`GET /admin/audit` (viewer) with filters and `?verify=1` (chain check);
`timelake_audit_records_total` / `timelake_audit_sink_healthy` on
`/metrics`. Tamper-*evident*, not tamper-proof (external anchoring is
future work — see SECURITY.md "Audit trail (P1-2)").

**Still open (the follow-on):** the data plane is unauthenticated by
default, so its reads/writes have no principal to attribute — data-plane
auditing arrives with `TIMELAKE_DATA_AUTH=required`. Session login/logout
chaining, a `system.audit` SQL exposure, object-store upload on rotation,
and the retention floor are the remaining `docs/CONSOLE.md` §5 items.

### P1-3 · Per-client rate limiting — DONE (SEC-6, 2026-08-15)

**Was Effort: M. Exposure 6, now CLOSED.** The shared memory pool and
admission semaphore uphold RR-1, but nothing stopped one client from
taking the whole admission budget. A per-client concurrency cap now sits
in front of the semaphore: past its cap (default 4 of the global 6) a
client is refused — HTTP 429 / Flight `ResourceExhausted` — keyed by
data-plane token else network origin, on both surfaces
(`crates/server/src/ratelimit.rs`). Verified by Riverkeeper's
`query-rate-limited-per-client` control.

### P1-4 · Query error sanitization — DONE (SEC-5, 2026-08-15)

**Was Effort: S. Exposure 5, now CLOSED.** DataFusion errors were
returned verbatim, naming tables and columns. Every failure now returns
one opaque `query could not be executed (ref: q-XXXXXXXX)` on both
surfaces, with the full error logged server-side against that ref
(`crates/query` `run_sql_env`). Verified by Riverkeeper's
`query-errors-are-sanitized` control.

### P1-5 · WAL encryption at rest — DONE (SEC-8, 2026-08-15)

**Was Effort: M. Exposure 8, now CLOSED.** SEC-1 covered every object
through the store chokepoint, but recent line-protocol bytes sat in the
WAL in plaintext until flush. The WAL now encrypts with the SAME envelope
key: per-file wrapped DEK, AES-256-GCM frames, plaintext passthrough on
upgrade, replay fails closed on a missing/wrong key (`crates/wal`). Covers
the replica WAL. Verified by Riverkeeper's `wal-encrypted-at-rest` control.

### P1-6 · Tributary L4 — identity and mTLS under rotation — DONE (2026-08-17)

**Was Effort: M.** The server half had already shipped (want-mode client
certificates, dual-CA overlap, AT-6 11/11, AT-7 19/19). Tributary now
presents a certificate and rotates it with the same validate-before-swap
and last-good discipline, and the gate written into its `ROADMAP.md` is
met: 10/10 in `Tributary/bench/results/l4-mtls-rotation.log` — both
certificates rotated under sustained shipping, 15,000 written read back
exactly, a rejected renewal kept the last-good pair shipping, and an
anonymous caller was served throughout (AT-6 not regressed).

**The gap it exposed, on this side — CLOSED 2026-08-19.** The certificate
was verified at the TLS layer but its CN reached no HTTP handler:
`identity_of` was wired only into the Flight listener, so `/api/sql` and
the write endpoints carried no peer identity. `axum-server` owns that
accept loop, which is why it needed a custom `Accept`. Tributary writes
over HTTP, so its certificate bought handshake-level verification and
nothing more: no SEC-2 grant intersection, no per-identity attribution.

`crates/server/src/tls_identity.rs` wraps `RustlsAcceptor` with an `Accept`
that reads the subject CN off the completed handshake
(`tls.get_ref().1.peer_certificates()` — borrowed, not consumed, since the
stream still has to be served) and layers `Extension(PeerIdentity)` onto
the service. **Once per connection, not per request**: a peer certificate
cannot change mid-connection, and doing it per request would put an X.509
parse on the query path. From there the identity travels the path Flight
already used — `Engine::sql` → `sql_batches` → `QuerySession.identity` →
`.resolve(granted)`.

**Drilled 15/15** against a real TLS handshake
(`docs/evidence/http-peer-identity-drill.log`). Three clients issue the
*identical* claim `ops,audit` against rows labelled public / `ops` /
`audit`; only the certificate differs, which is what makes the row counts
attributable to identity and nothing else:

| caller | grants | rows seen |
|---|---|---|
| anonymous (no certificate) | — | **3** — want mode unaffected, so Grafana and Telegraf are not broken |
| `narrowed-agent` | `[ops]` | **2** — claims ∩ grants, the audit row withheld. Before this change it was 3 |
| `ungranted-agent` | none recorded | **3** — `None` means "no policy", not deny-all |

The drill also confirms the restriction is enforced *in the scan*
(`SELECT` returns the same count `COUNT(*)` claimed, so no aggregate leaks
a hidden row) and that the CN lands on the query rows in `_system.queries`,
which is what makes "which client is doing this to us" answerable in SQL.
The drill runs from a Linux container on the compose network because
Windows curl is schannel and cannot present a PEM client certificate — a
host-side run would silently exercise the anonymous path three times and
pass.

### P1-7 · Tributary's queue is node-local durability, not replication — DONE (2026-08-17)

**Was Effort: S.** The trade is now explicit, and the RPO is **measured**
rather than asserted (`Tributary/bench/results/p17-queue-rpo.log`):

| the node… | RPO |
|---|---|
| comes back (process restart, same disk) | **0** — the checkpoint resumes exactly (L1, re-verified) |
| is gone (spot eviction, evicted pod) | `batch_lines × (1 + max_inflight)` + queue contents |

At Tributary's shipped defaults that ceiling is 25,000 lines — 25 s at
1,000 lines/s. The agent now prints its live exposure every
`rpo_report_secs`, so an operator watches the number instead of deriving
it; `Tributary/DESIGN.md` §3.3 states the trade.

**Two corrections the measurement forced**, both worth more than the
number itself. The mitigation as originally written — "a shorter
checkpoint interval and a smaller queue" — was subtly wrong: the
checkpoint interval governs *duplicates on restart*, not loss on node
death. And the first drill reported that smaller batches were *worse*,
because a single `kill -9` samples the flush sawtooth; instrumenting the
agent's own peak exposure showed smaller batches are in fact 10× better by
peak and 50× better by bound.

---

## 3. P2 — scale and multi-node

- **C2 phase 5b, then C3 Consul discovery + intra-cluster mTLS.** C2
  phases 1–4 shipped (P1-1 above); 5a built the compactor role behind the
  commit fence; 5b is the work-avoidance that lets it start. The
  client-certificate verifier is built; C3 flips it from *want* to
  *required* on the intra-cluster listener, which is safe there
  precisely because no stock client dials it.
- **Rebalancing needs a drained fleet, and there is no code that enforces
  it.** Change the ingester count while any agent still holds an
  undelivered batch and row counts can come back *too high*: `shard_of` is
  FNV-1a mod N, so a retry issued after the change lands on a different
  node than the original write, and the two copies never meet anywhere
  flush-time LWW would collapse them. Measured at 2,000 excess rows per
  affected table over eight streams
  (`FINDING_rebalance_duplicates_replayed_writes.md` — CLOSED 2026-08-22:
  the C4 composition was re-run against overlap-aware compaction and the
  duplicates now collapse at the next compaction pass, observed at
  202,000 → 200,000 exact). What the drain buys today is avoiding the
  transient: between an undrained rebalance and that compaction pass,
  counts read high. The same re-run campaign found and fixed a second,
  unrelated agent bug the reshape triggers —
  `FINDING_agent_pools_a_reused_ip.md`, an agent wedged forever on a
  recreated neighbor's reused IP — which is worth knowing about because
  its trigger is any orchestrator that recycles addresses, i.e.
  Kubernetes on an ordinary rollout.
  **`docs/REBALANCE.md` is the procedure.** Note what it does *not* cover:
  a node dying is a membership change nobody scheduled, and that is the
  one you would most want guarded.
- **Real-AWS sizing.** Every S3/KMS number so far is LocalStack, which
  proves correctness, call counts and recovery, and **deliberately
  proves nothing about latency.** Node-type sizing needs real AWS.
- **The two M4 carve-outs**, both pointing at the same fix: Shape A p95
  608 ms against a 250 ms target, and intra-run ingest decline under
  maintenance contention. Streaming execution, range reads, and
  maintenance/query isolation.
- **Console U0, U1 (part) and U3** — the admin listener on 1965 bound to
  loopback (moving `/admin/*` off the data port), layered configuration
  with provenance, cluster view. **U2 is done** (metrics + performance
  views, 2026-08-18): see `docs/CONSOLE.md` §7.4/§7.6 and the drill
  `docs/evidence/u2-console-drill.log`. Its own follow-ups are the
  per-query pruning counters, a real `level` field on `FileMeta` instead
  of the filename-prefix convention, and shipping a querier's samples to
  an ingester so a CL-3 node can be charted at all.
- **Tributary L5** — Consul/Kubernetes discovery, DaemonSet deployment,
  container-log metadata with the tag allowlist earning its keep, and
  workload identity (SPIFFE / projected tokens) as a third credential
  source.

---

## 4. P3 — product surface

Genuinely optional for production; they make it a *product*.

- Flight SQL `DoPut` and prepared statements.
- ~~`CREATE`/`DROP TABLE` currently return `[]` and do nothing~~ — **done
  with P0-2**: the read-only guard refuses DDL explicitly, so a
  `DROP TABLE` no longer appears to succeed while doing nothing.
- Manifest replay should skip non-`.json` files (known, small).
- ~~A tag or field named `time` is not refused on the write path~~ —
  **done 2026-08-22 (#41).** Measured first: the line was accepted, every
  `SELECT` on the table failed, and the table could no longer *flush*, so
  the WAL held it forever. Now a 400 at parse, before the WAL.
- A router-role node carries no `DefaultBodyLimit`, so it falls back to
  axum's 2 MiB default while the ingesters it fronts accept
  `TIMELAKE_MAX_BODY_BYTES` (32 MiB) — the 2026-08-13 fix covered the
  data-plane and internal routers and missed this one
  (`crates/server/src/router.rs` `router_app`). One `.layer(...)`, but it
  needs the config value threaded into `main.rs`'s router branch.
- The router forwards writes without the client's `Authorization`
  header, so a cluster behind a router cannot run
  `TIMELAKE_DATA_AUTH=required` on its ingesters today (every forwarded
  shard would be refused 401). Queries *do* pass the header through.
- Helm chart, packaging, versioned upgrade/rollback policy.
- **Tributary L6 (Arrow wire protocol) — explicitly do not start.** Its
  own roadmap gates this on L3 proving line protocol is the bottleneck.
  L3 measured 492k lines/s and did not prove that. Building it now would
  be optimising a bottleneck that has not been demonstrated to exist.

---

## 5. Critical path

The dependency chain that actually determines the date:

```
P0-1 push + CI ──┐
                 ├─→ everything else is trustworthy
P0-2 sandbox SQL ┤   (independent, do immediately)
                 │
P0-3 data auth ──┼─→ P0-5 Tributary auth
                 └─→ P1-2 audit trail  (needs a principal to attribute to)

P0-4 catalog CAS ──→ C2 role split ──→ P1-1 replication ──→ query HA
                                        (longest pole — start early)
```

Three things can run in parallel from day one: **CI**, **SQL
sandboxing**, and **catalog CAS**. Data-plane auth is already underway.
Replication should start before it is needed, because it will take
longer than it looks.

## 6. Proposed release train

| Release | Contains | Means |
|---|---|---|
| **v0.1 alpha** | P0-1, P0-2 | Pushed, CI green, no longer trivially exploitable. Internal use on a trusted network. |
| **v0.2 pilot** | + P0-3, P0-4, P0-5, P1-4 | Authenticated, attributable, safe against a second writer. Pilot with real data and a real Tributary fleet, still single-node. |
| **v1.0 production** | + P1-1, P1-2, P1-3, P1-5, P1-6, P1-7 | Survives node loss, audited, fair under multi-tenant load, encrypted end to end. |
| **v1.1+** | P2 | Multi-node at scale, cloud-native deployment, the performance carve-outs. |

## 7. What is deliberately *not* on this list

Things that look like gaps and are not, recorded so they do not get
re-litigated:

- **Want-mode mTLS not being mandatory.** That is the design, and
  SECURITY.md exposure 9 states plainly that it grants nothing on its
  own. Requiring certificates belongs on the intra-cluster listener
  (C3), not the client-facing one.
- **The `TLDE1` encryption magic not matching the new brand.** It is a
  format marker; renaming it makes every previously written object fail
  the magic check and fall into plaintext passthrough — silent
  corruption for a cosmetic gain.
- **LocalStack numbers not being performance numbers.** By design. The
  rig proves correctness, call counts and recovery, and is explicitly
  documented as proving nothing about latency.
- **The first-run `admin`/`admin` credential.** Quarantined — it may do
  nothing but change its own password — and
  `timelake_admin_default_credential_active` alerts while it is live.
  The cost is recorded in `docs/CONSOLE.md` §4.2.
