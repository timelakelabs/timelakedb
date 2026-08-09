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

**The data plane has no authentication.** Any client that can open a TCP
connection to port 1963 or 1964 has full read and write access to every
database on the node. The *administrative* surface (`/admin/*`) does
authenticate as of SEC-4, which closes the remote-deletion exposure, but
it does not protect the data: reads, writes and `/api/sql` remain open by
design until the data-plane migration (a deliberate breaking change for
Telegraf, Grafana and every existing client). Network reachability is
still the only access control over your data.

Treat a TimeLakeDB port as equivalent to an unauthenticated shell into the
data. Bind it to localhost or a private network segment, and put an
authenticating proxy in front of it if anything other than your own agents
needs access.

| Control | Status |
|---|---|
| Transport encryption | **Implemented, opt-in.** TLS 1.3 on both listeners when `TIMELAKE_TLS_CERT`/`_KEY` are set, with hot rotation (SEC-3). Plaintext is the default. |
| Client certificate / mTLS | Not implemented — server-side TLS only. Mutual TLS is v2 (SEC-3 intra-cluster). |
| Authentication | **Admin surface only (SEC-4).** `/admin/*` requires a session: Argon2id credentials, cookie sessions (HttpOnly, SameSite=Strict, idle 30 min / absolute 12 h) or bearer tokens, CSRF + Origin checks on mutations, per-principal backoff on failed logins. **The data plane is still open** — write endpoints accept any `Authorization` token and ignore it; Flight SQL's handshake accepts anything. |
| Authorization | **Roles on the admin surface.** `viewer` (read), `operator` (non-destructive changes, *growing* a retention window), `admin` (shrinking/removing retention, principal management). No per-database/table permissions on the data plane. |
| First-run credential | **`admin`/`admin`, quarantined.** Seeded only when no principal exists; it may do nothing but change its own password, and every other admin route answers `403 password_change_required` until it does. Rotating it invalidates all its sessions. `TIMELAKE_ADMIN_BOOTSTRAP_PASSWORD` replaces it for provisioning. Alert on `timelake_admin_default_credential_active`. |
| Tenancy isolation | **Not a boundary.** `org` is accepted and ignored; databases are namespaces only. |
| Encryption at rest | **Implemented, opt-in.** Set `TIMELAKE_ENCRYPTION_KEY` (64 hex chars) or `TIMELAKE_ENCRYPTION_KEY_FILE` and every object written to the store — Parquet, manifests, checkpoints — is envelope-encrypted (per-object AES-256-GCM data key, wrapped by the configured key). Objects written before the key was set stay readable (plaintext passthrough); the local WAL is **not** encrypted. |
| Row visibility labels | **Implemented.** A `_visibility` tag holding an Accumulo-style expression (`(ops&audit)\|admin`) restricts rows to sessions presenting satisfying authorizations (`X-TimeLake-Authorizations` header / Flight SQL metadata). Enforced inside the scan, so aggregates cannot leak. **Authorizations are unauthenticated claims** — see exposure 7. |
| Audit logging | Not implemented. Writes and queries are not attributed to a principal, because there is no principal. |
| Availability guardrails | **Implemented.** Shared query memory pool, admission semaphore, server-side query deadline (RR-1), and WAL backpressure as an explicit 429 (RR-5). These bound resource exhaustion; they are not access control. |

## Known exposures

These are verified properties of the current build, not hypotheticals. They
follow from "no authentication" and are listed so you can design around them.

1. **Unauthenticated ingest and query** on `:1963` (line protocol, `/api/sql`)
   and `:1964` (Flight SQL). Anyone reachable can write arbitrary data, read
   all data, and enumerate the schema.

2. **`POST /api/sql` executes arbitrary DataFusion SQL, including `COPY … TO`,
   which writes files as the server process.** Verified: a single unauthenticated
   request wrote a Parquet file outside the data directory. Arbitrary file
   *reads* are not reachable today — no `read_parquet`/`read_csv`-style table
   functions are registered, and `CREATE EXTERNAL TABLE` does not survive the
   request because each query gets a fresh session — but do not rely on that as
   a boundary. **Treat SQL access as filesystem-write access to the container.**

3. **`POST /admin/tls/reload` is unauthenticated** when TLS is enabled. Impact
   is limited by design — it only re-reads the already-configured cert and key
   paths, validates before swapping, and keeps the last-good pair on failure —
   so the realistic worst case is forced log/alarm noise rather than a
   downgrade. It is still an unauthenticated administrative endpoint.

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

4. **The container runs as root.** The image has no `USER` directive, so the
   server process and every file it writes are root-owned. Combined with (2),
   a reachable attacker can write root-owned files anywhere the container's
   filesystem permits.

5. **Query errors are returned verbatim**, including DataFusion planning errors
   that disclose table and column names.

6. **No rate limiting per client.** The memory pool and admission semaphore
   keep a query from killing the server, but nothing stops one client from
   consuming the whole admission budget.

7. **Visibility authorizations are self-asserted.** There is no
   authentication, so `X-TimeLake-Authorizations: admin` is a claim any
   client can make. Until token auth lands, SEC-2 is a correct enforcement
   mechanism behind an honor-system front door: real isolation requires an
   authenticating proxy that *sets* (and strips inbound) that header.

8. **Encryption at rest does not cover the local WAL**, which holds recent
   line-protocol bytes until flush, nor does it protect data from anyone who
   can reach the query API (queries decrypt transparently). It protects the
   object store's media: a stolen volume, a decommissioned disk, an S3
   bucket leak. Key material is held in process memory and sourced from the
   environment.

## Deploying it safely today

- **Do not expose 1963 or 1964 to an untrusted network.** Bind to `127.0.0.1`
  (`TIMELAKE_ADDR=127.0.0.1:1963`) or to a private Docker/Kubernetes network,
  and publish nothing.
- **Front it with something that authenticates** if remote access is needed — a
  reverse proxy doing mTLS or token checks, or a VPN/overlay network. Note that
  Flight SQL is gRPC over HTTP/2, so any proxy in front of `:1964` must speak
  HTTP/2.
- **Enable TLS** (`TIMELAKE_TLS_CERT`/`_KEY`) even on a private network; it is
  the one security control that is finished and drilled.
- **Set a container memory limit.** `mem_limit` is not optional in practice —
  an unbounded engine took down an entire Docker VM during development.
- **Run it on a dedicated volume** with nothing else of value on the filesystem,
  given exposure (2) and (4).
- **Alert on certificate health**: `timelake_tls_last_reload_ok == 0` and
  `timelake_tls_cert_expiry_seconds` below two renewal periods. A failed
  renewal keeps serving on the last-good pair, which is exactly why it can go
  unnoticed until expiry.

## Roadmap

| Item | Requirement | State |
|---|---|---|
| TLS 1.3 both listeners, hot rotation | SEC-3 (v1 MUST) | **Shipped** — AT-7 drill 19/19 |
| Admin authentication + roles | SEC-4 (v1 MUST) | **Shipped** — sessions, Argon2id, CSRF, forced first-run rotation |
| Data-plane authentication | SEC-4 (phased) | Not started — the piece that turns SEC-2 claims into authorization; breaks every existing client, so it is its own migration |
| Mutual TLS, intra-cluster | SEC-3 (v2) | Not started |
| Encryption at rest | SEC-1 (design constraint v1, implement SHOULD v2) | **Shipped early** — envelope encryption at the store chokepoint, opt-in by key config. Per-column keys (Parquet Modular Encryption) and KMS backends remain open at the same seam. |
| Row visibility labels | SEC-2 (design constraint v1) | **Shipped** — `_visibility` labels enforced in-scan via the mandatory-predicate hook. |

`REQUIREMENTS.md` §8 is the full security specification and explains why
SEC-1 and SEC-2 are constrained now and implemented later.
