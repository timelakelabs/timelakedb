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

**TimeLakeDB is engine-complete and drill-proven, but operationally
unshipped.** The hard parts — the storage engine, exactness under crash
and rotation, the RR-1 "no query may kill the server" invariant, TLS
rotation under load — are done and measured. What is missing is almost
entirely the boring, unavoidable operational shell: the data plane has
no authentication, the container runs as root, `/api/sql` can write
files, nothing has ever been pushed to a remote, and there is exactly
one node holding exactly one copy of the data.

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
| 1 | **Deployable by someone else** | No — never pushed, CI never ran, no packaging |
| 2 | **Access controlled and attributable** | No — open data plane, no audit trail |
| 3 | **Survives node loss** | No — single node, single volume, RPO = last backup |
| 4 | **Failures visible before outages** | Mostly — good metrics, no alerting story, no audit |
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

**Effort: M. Shipped** — read-only SQL guard at the plan + non-root, read-only-rootfs container; drill `bench/results/sql-sandbox-drill.log`. The original text is kept below for the record.

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

### P0-3 · Data-plane authentication  ⟂ in progress, design settled

**Effort: M (partially built).** Today anyone who can reach `:1963` or
`:1964` has full read and write access to every database. SEC-2's
visibility labels are enforced correctly but sit behind an honor-system
front door (exposure 7): `X-TimeLake-Authorizations: admin` is a claim
anyone can make.

The mechanism was chosen by measurement, not preference — see
`bench/results/data-auth-client-probe.log`. Grafana's Flight SQL path
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

**Effort: M. Shipped** — CAS on the next manifest sequence (`put_if_absent`), catch-up-and-retry on conflict, `timelake_catalog_commit_conflicts_total`; drilled on local hard-link and real S3 If-None-Match (`bench/results/catalog-cas-drill.log`). Original text below.

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

**Shipped** (Tributary repo) — `TRIBUTARY_TOKEN`/`token_file`, redacted two ways, a 401 spools rather than drops, drilled 10/10 against a required-mode node (`bench/results/p05-data-auth.log`). Original text below.

**Effort: S once P0-3 lands.** The moment TimeLakeDB can require a
credential, a shipper that cannot present one is not deployable.
`ship.rs` needs the `Authorization: Bearer` header, a credential source
(file, env, later Vault), and — easy to forget, hard to undo —
**redaction so the token never reaches a log line or an error message.**

---

## 2. P1 — blocks *unattended* operation

You can pilot without these. You cannot run them unwatched.

### P1-1 · Node loss loses data  ⟂ the biggest architectural gap

**Effort: L.** One node, one volume. Backup and restore are drilled and
exact (34 s backup, 13 s restore, `docs/BACKUP_RESTORE.md`), so the
recovery story is real — but RPO is "since the last backup" and RTO
includes a human. `REQUIREMENTS.md` §7 makes replication and query HA
**v2 MUSTs**; they are not started.

Sequence: P0-4 (CAS) → C2 role split → WAL replication → query HA.
This is the longest pole on the list and the one most likely to be
underestimated. Start it early even though it finishes late.

### P1-2 · No audit logging

**Effort: M.** Writes and queries are attributed to nobody, because
until P0-3 there *is* nobody. Once tokens land there is finally a
principal, and `docs/CONSOLE.md` already designs the hash-chained trail.
For most regulated deployments this is a hard gate, not a nicety —
and "who read this data" is the question SEC-2 exists to answer.

### P1-3 · No per-client rate limiting

**Effort: M.** Exposure 6. The shared memory pool and admission
semaphore uphold RR-1 — no single query kills the server — but nothing
stops one client from consuming the entire admission budget and starving
every other tenant. Availability guardrails are not access control.

### P1-4 · Query errors disclose schema

**Effort: S.** Exposure 5: DataFusion planning errors are returned
verbatim, naming tables and columns. Trivial to fix, and it stops
mattering the moment there is more than one tenant.

### P1-5 · The WAL is not encrypted

**Effort: M.** Exposure 8. SEC-1 covers every object through the store
chokepoint — Parquet, manifests, checkpoints — but recent line-protocol
bytes sit in the WAL in plaintext until flush. A stolen volume is
exactly the threat SEC-1 was built for, and this is the hole in it.

### P1-6 · Tributary L4 — identity and mTLS under rotation

**Effort: M.** The server half shipped (want-mode client certificates,
dual-CA overlap, AT-6 11/11, AT-7 19/19). Tributary now needs to present
a certificate, rotate it on the client side with the same
validate-before-swap and last-good discipline, and pass the gate already
written into its `ROADMAP.md`: *rotate both server and client
certificates under sustained shipping, exact count, zero dropped
connections, and Grafana's dashboards keep rendering throughout without
a client certificate.*

### P1-7 · Tributary's queue is node-local durability, not replication

**Effort: S — documentation and knobs, not architecture.** Its own
roadmap already states this honestly. On an ephemeral or spot node, a
non-empty queue buys minutes, not guarantees. What is needed is not a
fix but an explicit, documented trade: a shorter checkpoint interval, a
bounded queue, and a stated RPO under node loss — so an operator chooses
it rather than discovers it.

---

## 3. P2 — scale and multi-node

- **C2 role split, C3 Consul discovery + intra-cluster mTLS.** The
  client-certificate verifier is built; C3 flips it from *want* to
  *required* on the intra-cluster listener, which is safe there
  precisely because no stock client dials it.
- **Real-AWS sizing.** Every S3/KMS number so far is LocalStack, which
  proves correctness, call counts and recovery, and **deliberately
  proves nothing about latency.** Node-type sizing needs real AWS.
- **The two M4 carve-outs**, both pointing at the same fix: Shape A p95
  608 ms against a 250 ms target, and intra-run ingest decline under
  maintenance contention. Streaming execution, range reads, and
  maintenance/query isolation.
- **Console U0–U3** — the admin listener on 1965 bound to loopback
  (moving `/admin/*` off the data port), layered configuration with
  provenance, cluster view.
- **Tributary L5** — Consul/Kubernetes discovery, DaemonSet deployment,
  container-log metadata with the tag allowlist earning its keep, and
  workload identity (SPIFFE / projected tokens) as a third credential
  source.

---

## 4. P3 — product surface

Genuinely optional for production; they make it a *product*.

- Flight SQL `DoPut` and prepared statements.
- `CREATE`/`DROP TABLE` currently return `[]` and do nothing — either
  implement or refuse them explicitly. Silently succeeding at nothing is
  the worst of the three options.
- Manifest replay should skip non-`.json` files (known, small).
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
