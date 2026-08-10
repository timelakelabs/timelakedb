# Roadmap — TimeLakeDB + Tributary

Written 2026-08-10. Two inputs, merged into one prioritised plan:

1. **Production readiness** — what blocks running either product
   unattended with real data. The deep dive is
   [`PRODUCTION_READINESS.md`](PRODUCTION_READINESS.md); its P0 items are
   reproduced here unchanged because nothing competitive outranks them.
2. **The competitive landscape** — the feature surface of the projects
   this one was specified against (InfluxDB v1/v2/v3, influxdb-cluster,
   QuestDB, VictoriaMetrics) and, for Tributary, the shippers it will be
   measured against (Telegraf, Vector, Fluent Bit, Grafana Alloy).

Competitor feature claims are as of mid-2026, from their public docs and
this project's own recorded evaluations (`docs/evidence/`). Verify
before repeating any of them in public material — the evidence rule
(site claims trace to `bench/results/`) applies to competitor claims
doubly.

---

## 1. Where TimeLakeDB actually stands against the field

The comparison that matters is not a feature checklist — it is *what
each engine does under the workload this project was born from*. That
part is measured, ours, and worth restating because it is the moat:

- **InfluxDB 1.8**: ingest decayed 308K → collapse, and the server was
  **OOM-killed by a Shape B query** (`docs/evidence/BENCHMARK_RESULTS.md`).
- **InfluxDB 2.7**: 123K → ~10K lines/s decay as series grew to ~40M;
  the funnel never completed.
- **InfluxDB 3 Core**: passed everything — the bar to beat. TimeLakeDB
  matches it on exactness (fixed-bound equality on identical data) and
  beats the failure modes by construction (RR-1: bounded memory,
  admission control, server-side deadlines — a "killer" query fails
  cleanly at 9.2 GB bounded).
- **QuestDB / VictoriaMetrics**: OOMs under the same workload in the
  prior evaluation round.

### Feature position, axis by axis

**T** = TimeLakeDB today. ✓ shipped · ◐ partial · ✗ absent.

#### Ingest

| | v1 | v2 | v3 Core | QuestDB | VM | T |
|---|---|---|---|---|---|---|
| Line protocol | ✓ | ✓ | ✓ | ✓ ILP | ✓ | ✓ drilled |
| Prometheus `remote_write` | ✓ | ✓ | ✗ | ✗ | ✓ native | ✗ |
| OTLP | ✗ | ✗ | ✗ | ✗ | ✓ | ✗ |
| Out-of-order / backfill | ◐ | ◐ | ✓ | ✓ | ✓ | ✓ |
| Bulk import (CSV/Parquet) | CLI | CLI | ◐ | ✓ COPY | ✓ vmctl | ✗ |

The `remote_write`/OTLP row is the **adoption funnel**: VictoriaMetrics
wins deployments because a Prometheus fleet can point at it with one
config line. TimeLakeDB currently requires everything to speak line
protocol (i.e. Telegraf or Tributary in front).

#### Query

| | v1 | v2 | v3 Core | QuestDB | VM | T |
|---|---|---|---|---|---|---|
| SQL | ✗ | ✗ | ✓ | ✓ | ✗ | ✓ |
| InfluxQL | ✓ | ✓ | ✓ | ✗ | ✗ | ✗ (deliberate, REQUIREMENTS §12) |
| PromQL/MetricsQL | ✗ | ◐ | ✗ | ✗ | ✓ | ✗ |
| Flight SQL (stock Grafana) | ✗ | ✗ | ✓ | ✗ PGWire | ✗ | ✓ AT-6 drilled |
| ASOF / time-series joins | ✗ | ✗ | ✗ | ✓ strong | ✗ | ✗ |
| **Full history queryable** | ✓ | ✓ | **✗ ~72 h** | ✓ | ✓ | **✓** |

That last row is a real differentiator against the direct competitor:
InfluxDB 3 **Core** is positioned for recent data — long-range queries
are an Enterprise feature. TimeLakeDB queries its full history in OSS,
with the metadata cache making warm Shape A 0–6 ms.

#### Storage & lifecycle

| | v1 | v2 | v3 Core | QuestDB | VM | T |
|---|---|---|---|---|---|---|
| Object-store native | ✗ | ✗ | ✓ | Ent. | ✗ | ✓ C0, KMS-encrypted |
| Compaction in OSS | ✓ | ✓ | **✗ Ent.** | ✓ | ✓ | **✓** |
| Retention | ✓ RP | ✓ | ◐ | ✓ TTL | ✓ global | ✓ per-table, runtime GUI |
| **Targeted delete** (GDPR) | ✓ | ✓ | ◐ | ◐ | ✓ API | **✗** |
| Downsampling / rollups | ✓ CQ | ✓ tasks | ✗ plugins | ✓ mat. views | Ent. | **✗** |
| Dedup | LWW | LWW | LWW | ✓ keys | ✓ | ✓ LWW (FR-5) |

Two genuine gaps against nearly everyone: **no way to delete specific
data** (retention is the only eraser — a GDPR request today means
manual surgery), and **no downsampling** (the "keep 1s data for a week,
1m forever" cost story every mature TSDB tells).

Also a genuine differentiator: compaction, object storage, and full
retention management are OSS here, where InfluxDB 3 gates compaction
and history behind Enterprise, and QuestDB gates replication and
cold storage. **Do not give this up casually** — it is the positioning.

#### Security

| | v1 | v2 | v3 Core | QuestDB | VM | T |
|---|---|---|---|---|---|---|
| Data-plane authn | basic | tokens | tokens | ✓ | ✗ (vmauth) | **✓ tokens (SEC-4 phased)** |
| RBAC / scopes | ◐ | ✓ | Ent. | Ent. | Ent. | ✓ scopes + db scoping |
| **Row-level visibility** | ✗ | ✗ | ✗ | ✗ | ✗ | **✓ SEC-2** |
| **Encryption at rest, OSS** | ✗ | ✗ | ✗ | ✗ | ✗ | **✓ SEC-1** |
| TLS hot rotation | ✗ | ✗ | ◐ | ✗ | ✗ | ✓ AT-7 drilled |
| Client certs (want mode) | ✗ | ✗ | ✗ | ✗ | ✗ | ✓ |
| Audit trail | ◐ | ◐ | Ent. | Ent. | Ent. | ✗ planned |

Nobody in this field has Accumulo-style row visibility or OSS envelope
encryption. Those two, plus the drill culture (measured exactness, not
claimed), are what a security-sensitive buyer cannot get elsewhere
without an Enterprise contract.

#### HA & scale

| | v1 | v2 | v3 Core | influxdb-cluster | QuestDB | VM | T |
|---|---|---|---|---|---|---|---|
| Replication / HA | Ent. | ✗ | Ent. | ✓ | Ent. | **✓ OSS cluster** | ✗ (C1–C3 designed) |
| Multi-tenancy | ◐ | orgs | ◐ | ✗ | ✗ | ✓ tenants | ✗ (`org` ignored) |

VictoriaMetrics' OSS clustering is why it eats Prometheus deployments.
The C-phases close this; they are already the longest pole in
`PRODUCTION_READINESS.md` (P1-1).

### Non-goals, decided now so they stay decided

- **Flux.** Deprecated by its own vendor. Never.
- **A PromQL engine.** VM owns this; competing there is a second
  product. Revisit only if `remote_write` ingest (R-3) creates real pull.
- **An alerting engine.** Grafana is the alerting surface (FR-9);
  v2-style tasks/checks are not worth their maintenance weight.
- **A plugin/processing engine** (InfluxDB 3's Python triggers).
  Tributary is where transformation lives.
- **InfluxQL.** The read side is deliberately not InfluxDB-shaped
  (REQUIREMENTS §12). Cost of a compat layer is huge, benefit is
  migration of dashboards that Grafana's SQL mode already replaces.
  Revisit only on hard evidence of a blocked migration.

---

## 2. Where Tributary stands against the shippers

| | Telegraf | Vector | Fluent Bit | Alloy | Tributary |
|---|---|---|---|---|---|
| File tail + rotation | ✓ | ✓ | ✓ | ✓ | ✓ **exact-count drilled** |
| Crash resume, no dupes | ◐ | ✓ acks | ◐ | ◐ | ✓ drilled (mid-ms checkpoint) |
| Disk buffering | ◐ | ✓ | ◐ | ✓ | ✓ 60 s outage drill, exact |
| **Watermarks** (completeness claims) | ✗ | ✗ | ✗ | ✗ | **✓** |
| Multiline | ✓ | ✓ | ✓ | ✓ | ✓ |
| Sources beyond files (journald, syslog, docker, winlog) | ✓ 300+ | ✓ many | ✓ | ✓ | **✗ files only** |
| Transforms (filter/sample/redact) | ✓ | ✓ VRL | ✓ | ✓ | ◐ mapping+allowlist only |
| K8s metadata enrichment | ◐ | ✓ | ✓ strong | ✓ | ✗ (L5) |
| Self-telemetry (/metrics) | ✓ | ✓ | ✓ | ✓ | **◐ counters, no endpoint** |
| Config reload | SIGHUP | ✓ watch | ✓ | ✓ | ✗ |
| mTLS client | ✓ | ✓ | ✓ | ✓ | ✗ (L4, server half done) |
| Secrets (Vault etc.) | ✓ stores | ✓ | ◐ | ✓ | ✗ (seam designed) |

Tributary's differentiators are **watermarks** (no mainstream shipper
makes a completeness claim) and the **measured exactness culture** — the
30.4% silent-loss discovery, the outage-absorption drill, crash-exact
resume. Its gap is breadth: it is a file shipper, and the field ships
everything. The strategy is *not* to chase 300 plugins — it is to be the
best possible TimeLakeDB shipper (auth, identity, watermarks, k8s), and
let Telegraf remain the breadth answer (it already writes to TimeLakeDB
unmodified, FR-8).

---

## 3. The merged roadmap

P0 is unchanged from `PRODUCTION_READINESS.md` — nothing competitive
outranks "do not deploy this yet":

> **P0-1** push + CI on a real runner · **P0-2** `/api/sql` sandbox +
> non-root container ✓ (done 2026-08-10) · **P0-3** data-plane tokens (in progress,
> mechanism fixed by the client probe) · **P0-4** catalog CAS ·
> **P0-5** Tributary presents the token

The competitive analysis adds and re-ranks the rest:

### P1 — production + the two parity gaps that bite first

| Item | Why | Effort |
|---|---|---|
| P1-1 Replication/HA (C1→C2→WAL repl) | Only OSS-cluster competitor is VM; longest pole, start now | L |
| P1-2 Audit trail | Enterprise-gated everywhere else; needs P0-3's principal | M |
| **R-1 Targeted delete** (`DELETE WHERE` on tag/time predicates, tombstone + compaction-applied) | The GDPR answer every competitor has in some form and TimeLakeDB has none of. Fits the existing manifest/compaction machinery | M |
| **T-1 Tributary self-telemetry** (`/metrics` + `/healthz`) | Unwatchable shippers don't survive ops review; every competitor has it; prerequisite for the L5 DaemonSet | S |
| P1-3 per-client rate limits · P1-4 error redaction · P1-5 WAL encryption · P1-6 Tributary mTLS (L4) · P1-7 queue RPO documented | as in PRODUCTION_READINESS | S–M |

### P2 — adoption levers and scale

| Item | Why | Effort |
|---|---|---|
| **R-2 Downsampling / rollups** (continuous aggregates into ordinary tables, compaction-driven) | The storage-cost story v1 CQs, v2 tasks and QuestDB materialized views all tell; ours can be simpler because rollups are just another table behind the same store | L |
| **R-3 Prometheus `remote_write` ingest** | One config line to capture a Prometheus fleet — VM's entire growth engine. OTLP metrics second, same seam | M |
| **R-4 Last-value cache** | InfluxDB 3's answer to the exact workload behind the Shape A p95 carve-out (608 ms vs 250 target); attack both with one design | M |
| C2/C3 cluster phases, real-AWS sizing, console U0–U3 | as designed | L |
| **T-2 Transform stage** (filter/sample/redact on the mapped record) | The minimum Vector-shaped competence; redaction doubles as a security feature | M |
| **T-3 K8s: DaemonSet + metadata enrichment** (L5) | Where log shippers live now; tag allowlist already designed for exactly the label-explosion trap | L |
| T-4 journald + docker-json sources | The two sources that block bare-VM and container adoption most often | M |
| T-5 config reload | table stakes in the field | S |

### P3 — surface, deliberately later

Bulk import/export (authorized `COPY` — note the interplay with P0-2:
the allowlist refuses `COPY` for data-plane callers; an *authorized
admin export* is the correct replacement, not a regression) · real
multi-tenancy (`org` stops being ignored) · migration tooling from
InfluxDB (`vmctl` equivalent) · Windows event log source · OTLP logs
into Tributary · Flight `DoPut` · `CREATE`/`DROP TABLE` either work or
refuse loudly · packaging (Helm, deb/rpm) · **Tributary L6 stays
gated** — L3 measured 492k lines/s without proving line protocol is the
bottleneck; its own roadmap forbids starting it until that is proven.

### Release train (updated)

| Release | Adds | Competitive meaning |
|---|---|---|
| v0.1 alpha | P0-1, P0-2 | Exists in public; not trivially exploitable |
| v0.2 pilot | P0-3/4/5, P1-4, T-1 | Authenticated end-to-end — the thing VM OSS *doesn't* have natively; observable shipper |
| v1.0 | P1-*, R-1 | Survives node loss, audited, can delete data — matches Enterprise-tier security posture in OSS |
| v1.1 | R-2, R-3, T-2, T-3 | Cost story + Prometheus funnel + k8s-native shipping |
| v2.0 | C2/C3, R-4, multi-tenant | OSS clustering — the VM position, with the security stack VM gates |

---

## 4. The one-paragraph strategy

TimeLakeDB's defensible position is **"the security-serious,
evidence-cultured, object-store-native TSDB whose OSS tier is not
crippled"** — row visibility, envelope encryption, drilled TLS rotation,
compaction and full history in the open, against competitors who gate
exactly those behind Enterprise. Tributary's is **"the shipper that can
prove what arrived"** — watermarks and measured exactness rather than
plugin count. Every P1/P2 item above either closes a gap that
contradicts that story (deletes, audit, HA, shipper telemetry) or
widens the funnel into it (`remote_write`, k8s). Everything that merely
imitates a competitor's breadth is P3 or a non-goal.
