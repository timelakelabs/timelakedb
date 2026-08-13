# P1-1 — surviving node loss without reintroducing ingest gaps

**Written 2026-08-13.** A design for `PRODUCTION_READINESS.md` P1-1, which is
older than the code it describes and needs restating before it can be
finished.

The requirement that shaped this document did not come from the benchmark.
It came from an operator watching InfluxDB 1.x drop events whenever an
expensive query ran — the ordinary, non-fatal version of the failure that
OOM-killed InfluxDB 1.8 in the evaluation. That symptom is `RR-1` and `PR-9`
territory, not node loss. It belongs here because **the HA design can
reintroduce it**, and on the current code path it does.

---

## 1. What P1-1 still needs

P1-1 says one node, one volume, and lists its sequence as
`P0-4 (CAS) → C2 role split → WAL replication → query HA`, "not started".
Three of the four have shipped since it was written:

| Step | State |
|---|---|
| P0-4 catalog CAS | DONE 2026-08-10 |
| C2 role split | shipped, C2 phase 1 |
| WAL replication | shipped, CL-2 — `server/src/lib.rs:912`, replicates before the 204 |
| Query HA | shipped, C2 phase 4 — stateless queriers, kill one and reads continue |

What is genuinely left:

1. **The compactor role and its singleton lease** (C2 phase 5). The last
   phase of the role split.
2. **Automatic failover.** `lib.rs:1240` states the current scope plainly:
   recovery is an explicit operation, and "automatic health-triggered
   failover is a later cluster phase."
3. **Proof under real node death** — Catchment C3.
4. **Ingest isolation from query load**, which is the subject of the rest of
   this document and was not previously on the list.

---

## 2. The gap path that exists today

Three measured facts about the current code:

- **Replication is synchronous, in the write path, before the ack**, with a
  **5-second** timeout (`replication.rs:54`). A dead peer trips to degraded
  quickly and availability holds — that part is deliberate and loud. A
  *slow* peer never trips; it simply multiplies every write's latency.
- **A querier holds an ingester for up to 30 seconds** — `RemoteBuffers`
  sets a 30 s client timeout (`querier.rs:153`) against
  `/internal/v1/live` and `/internal/v1/snapshot`.
- **The internal listener has no concurrency bound and no body limit.**
  `internal_router` (`lib.rs:1764`) is a plain axum router: no semaphore, no
  `DefaultBodyLimit`, no admission control. `/internal/v1/snapshot` returns
  *rows*, so the work is real.

Which composes into:

```
expensive query on a querier
  → unions the ingesters' live buffers (up to 30 s each, unbounded fan-out)
  → ingester B slows
  → ingester A blocks in replicate() up to 5 s PER FRAME
  → acks stall → the shipper's buffer fills → GAPS
```

**A query on one node stalls ingest on another, through a link added for
durability.** The asymmetry is the tell: a querier may occupy an ingester
six times longer than the replication path is willing to wait for it.

The dangerous state is not a dead peer. It is a *slow* one. Dead is handled;
slow is invisible, and at the reference workload's ~232 events/s a 5-second
stall is an outage rather than a hiccup.

**`PR-9` does not cover this.** Its "≤ 2× degradation under burst" was
measured on a single node, before the role split. The querier↔ingester
coupling is new and has never been measured.

---

## 3. The design

Four changes, smallest first. Each is independently useful; the first is
most of the value.

**D1 · Cut the replication timeout to ~250 ms. — LANDED 2026-08-13.** The contract is unchanged —
durable on two nodes, or loudly degraded — but the damage a slow peer can do
becomes bounded. Slow becomes indistinguishable from dead, which is the safe
direction and already the module's stated philosophy. One constant, and it
is the single highest-value line in this document.

  Shipped as `TIMELAKE_REPL_TIMEOUT_MS`, default 250 ms, rather than as a
  constant: the right value is a property of a deployment's network, and
  `timelake_cl2_replication_degraded_events` already makes a
  too-aggressive setting visible as flapping rather than as silence.
  Pinned by `a_stalled_peer_costs_the_timeout_and_no_more`, which stalls a
  real socket instead of closing it — a dead-peer test would pass either
  side of this change and prove nothing.

**D2 · Give the internal listener its own admission control.** *Not yet
landed — but investigating it found a live defect first, fixed 2026-08-13:
the listener had no body limit, so axum’s 2 MiB default silently refused
any replication frame above it and dropped the node to degraded. Both
listeners now share `TIMELAKE_MAX_BODY_BYTES` (32 MiB). The semaphore
below is still to do.*

The remaining work: A semaphore
sized independently of the ingest path, plus a body limit, so a querier's
fan-out cannot consume an ingester's capacity. Refusing a snapshot is an
honest outcome that the querier already handles — it refuses the query
rather than returning a short count (`querier.rs:16-18`). Exhausting the ingester
is not.

**D3 · Write down the RPO/latency trade rather than inherit it.**
Sync-before-ack buys RPO ≈ 0 and pays per-write latency. The alternative — a
bounded disk-backed queue, ack on local WAL durability, ship behind it —
trades a small RPO window for immunity to peer latency. Recommendation:
**keep sync**, with D1. The current choice is defensible and the module
already anticipates the escape hatch ("can become a streaming gRPC/Flight
link if the per-batch round-trip ever shows up as the ingest bottleneck").
It should be a recorded decision, not a default nobody revisited.

**D4 · Do not co-locate querier and ingester roles in production.** Free
today, and it makes D2 defence in depth rather than the only barrier.

Sequencing: D1 and D2 are independent and land first; D3 is a documentation
decision; D4 is a deployment note. None blocks the compactor lease.

---

## 4. The drill

The gate is a scenario watched to go red, not a latency table. Ingest
isolation is only proven by a run that would have failed before the change.

- Sustained ingest at reference rate through the router.
- Expensive Shape B queries against a querier, concurrently.
- One ingester artificially slowed — not killed. **Slow is the case that
  matters**; dead is already handled.
- **Assert zero gaps**, by exact count, not acceptable latency. Exactness is
  the property; latency is the symptom.
- Repeat with the ingester killed outright, to confirm degraded mode still
  holds availability and the alarm fires once.

This belongs in Catchment, beside C3 (node death), and it is a **negative
control**: run it against the current 5-second timeout and it must fail. A
scenario that passes before and after the fix is measuring nothing — the
same discipline C1 exists to enforce.

---

## 5. Gate

- The drill above passes, and is recorded in `docs/evidence/`.
- The same drill, run against the pre-D1 build, **fails** — recorded, so the
  scenario is proven able to detect the fault.
- `Role::Compactor.implemented()` is true and the pin at
  `crates/cluster/src/lib.rs:244` is inverted.
- A killed ingester loses no acknowledged write, with RPO and RTO measured
  rather than asserted.
- `PR-9` restated for the cluster shape, or a new PR row added: the existing
  number describes a single node and does not cover this.

## 6. Doc updates that travel with it

- `PRODUCTION_READINESS.md` P1-1 — restated; its sequence is three-quarters
  done and its "not started" is false.
- `REQUIREMENTS.md` — the `PR-9` successor covering the clustered shape.
- `ARCHITECTURE.md` §12.4 (roles) and §12 — the admission boundary on the
  internal listener, and D4's co-location note.
- `SECURITY.md` — the internal listener gains a bound; its existing note
  already says this surface must never be public.
- `CHANGELOG.md` `[Unreleased]`.

---

## 7. What this document does not settle

- Whether automatic failover is health-triggered by the router or by
  discovery (C3). Named here only so it is not mistaken for done.
- The compactor lease's own design, which is C2 phase 5 and builds on
  catalog CAS rather than beside it.
- Whether `/internal/v1/snapshot` should stream rather than return whole
  batches. That is the same round-trip question D3 defers, and the answer
  should come from a measurement, not from this document.
