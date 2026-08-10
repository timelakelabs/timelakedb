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
<https://github.com/TimeLakeLabs/TimeLakeDB/security/advisories/new>.
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

| Control | Status |
|---|---|
| Transport encryption | **Implemented, opt-in.** TLS 1.3 on both listeners when `TIMELAKE_TLS_CERT`/`_KEY` are set, with hot rotation (SEC-3). Plaintext is the default. |
| Client certificate / mTLS | **Implemented, opt-in, WANT mode.** Set `TIMELAKE_TLS_CLIENT_CA` and both listeners request a client certificate, verify one if presented, and serve the connection either way — so Grafana, Telegraf and the harness need no change. A verified identity narrows that session's SEC-2 authorizations to what it is granted (Flight SQL; `/api/sql` identity is still to come). Trust anchors hot-rotate with dual-CA overlap. Want mode is not itself a control — see exposure 9. |
| Authentication | **Admin surface (SEC-4) + data plane (SEC-4 phased).** `/admin/*` requires a session (Argon2id, cookie/bearer, CSRF + Origin, backoff). The **data plane** authenticates by token when `TIMELAKE_DATA_AUTH` is `optional` or `required`: one token on the `Authorization` header, accepted as `Bearer` (Grafana Flight SQL / Tributary), `Token` (Telegraf v2) or `Basic` (Telegraf v1, token as password). **Default is `off`** — the header is not examined and the data plane is open, as it always was. HTTP and Flight SQL enforce through one decision function. |
| Authorization | **Roles on the admin surface; scopes + grants on data tokens.** Admin roles: `viewer`/`operator`/`admin`. Data tokens carry a scope (`read`, `write`, `read_write` — deliberately not a total order, so a shipper can write without being able to read back), an optional database allowlist, and optional SEC-2 grants that *intersect* a caller's claimed authorizations. No per-column permissions. |
| First-run credential | **`admin`/`admin`, quarantined.** Seeded only when no principal exists; it may do nothing but change its own password, and every other admin route answers `403 password_change_required` until it does. Rotating it invalidates all its sessions. `TIMELAKE_ADMIN_BOOTSTRAP_PASSWORD` replaces it for provisioning. Alert on `timelake_admin_default_credential_active`. |
| Tenancy isolation | **Not a boundary.** `org` is accepted and ignored; databases are namespaces only. |
| Encryption at rest | **Implemented, opt-in.** Set `TIMELAKE_ENCRYPTION_KEY` (64 hex chars) or `TIMELAKE_ENCRYPTION_KEY_FILE` and every object written to the store — Parquet, manifests, checkpoints — is envelope-encrypted (per-object AES-256-GCM data key, wrapped by the configured key). Objects written before the key was set stay readable (plaintext passthrough); the local WAL is **not** encrypted. |
| Row visibility labels | **Implemented.** A `_visibility` tag holding an Accumulo-style expression (`(ops&audit)\|admin`) restricts rows to sessions presenting satisfying authorizations (`X-TimeLake-Authorizations` header / Flight SQL metadata). Enforced inside the scan, so aggregates cannot leak. **Authorizations are unauthenticated claims** — see exposure 7. |
| Audit logging | Not implemented. Writes and queries are not attributed to a principal, because there is no principal. |
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
   (`bench/results/sql-sandbox-drill.log`; the same request that wrote a
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

5. **Query errors are returned verbatim**, including DataFusion planning errors
   that disclose table and column names.

6. **No rate limiting per client.** The memory pool and admission semaphore
   keep a query from killing the server, but nothing stops one client from
   consuming the whole admission budget.

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

8. **Encryption at rest does not cover the local WAL**, which holds recent
   line-protocol bytes until flush, nor does it protect data from anyone who
   can reach the query API (queries decrypt transparently). It protects the
   object store's media: a stolen volume, a decommissioned disk, an S3
   bucket leak. Key material is held in process memory and sourced from the
   environment.

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

## Deploying it safely today

- **Do not expose 1963 or 1964 to an untrusted network.** Bind to `127.0.0.1`
  (`TIMELAKE_ADDR=127.0.0.1:1963`) or to a private Docker/Kubernetes network,
  and publish nothing.
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
| Data-plane authentication | SEC-4 (phased) | **Shipped** — token auth on both listeners via `TIMELAKE_DATA_AUTH=off\|optional\|required`, scopes + database scoping + SEC-2 grants, one decision function for HTTP and Flight, drilled live (`bench/results/data-auth-drill.log`). Turns SEC-2 claims into authorization. Tributary presenting the token (P0-5) is the remaining half. |
| Client certificates, want mode | SEC-3 (v2) | **Shipped** — opt-in via `TIMELAKE_TLS_CLIENT_CA`, hot-rotating anchors with dual-CA overlap, identity plumbed into the query session over Flight SQL. AT-7 still 19/19 with it enabled. `/api/sql` identity outstanding. |
| Mutual TLS *required*, intra-cluster | SEC-3 (v2) | Not started — want mode is the client-compatible half; requiring it is a C2/C3 decision for the intra-cluster listener, where there is no Grafana to keep working |
| Encryption at rest | SEC-1 (design constraint v1, implement SHOULD v2) | **Shipped early** — envelope encryption at the store chokepoint, opt-in by key config. Per-column keys (Parquet Modular Encryption) and KMS backends remain open at the same seam. |
| Row visibility labels | SEC-2 (design constraint v1) | **Shipped** — `_visibility` labels enforced in-scan via the mandatory-predicate hook. |

`REQUIREMENTS.md` §8 is the full security specification and explains why
SEC-1 and SEC-2 are constrained now and implemented later.
