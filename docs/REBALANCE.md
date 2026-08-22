# Rebalancing the cluster (changing the ingester count)

**Drain the agents before you change N.** Changing the number of
ingesters while any agent still holds an undelivered batch can inflate row
counts — silently, with the query path perfectly confident about the wrong
number.

This is a procedure, not a fix. It guards the rebalance you *planned*.
The one you didn't is covered under [What this does not
guard](#what-this-does-not-guard), and you should read that section before
relying on this one.

---

## Why

`shard_of` is FNV-1a **mod N** (`crates/server/src/router.rs:428`). Change
N and roughly **(N−1)/N** of all table homes move — going from two
ingesters to three relocates about two thirds of them.

That matters because agent idempotency is *implicit*. A batch can land on
a node without the agent hearing the acknowledgement: the kernel buffered
the bytes, the client timed out, the node stored the rows and replied to a
dead socket. The agent retries, and normally that is harmless — the retry
lands on the same node, and flush-time last-write-wins collapses the
overlap because the primary keys are byte-identical.

Change N between the original write and the retry, and the retry lands
somewhere else. The twins are now on two different nodes and never meet,
so nothing collapses them, and the querier's union serves both copies.

Measured, on eight streams of 200,000 lines: **2,000 excess rows per
affected table**, nothing missing. Full account, including why every
component is individually correct, in
[`FINDING_rebalance_duplicates_replayed_writes.md`](FINDING_rebalance_duplicates_replayed_writes.md).

---

## The rule

Before changing N, these must read **zero on every agent**:

| metric | why it blocks |
|---|---|
| `tributary_queue_bytes` | batches spooled on disk that will be re-shipped |
| `tributary_inflight_batches` | in flight right now; may land without an ack |
| `tributary_at_risk_lines` | read but unacknowledged; these become in-flight |

### `tributary_unread_bytes` does not have to be zero

This is the trap, and it will cost you an afternoon if nobody says so.

`tributary_unread_bytes` counts bytes written to the source files that the
agent has not read yet. Those lines have **never been sent**, so after the
rebalance they ship fresh to whatever home is correct by then. There is no
original for them to duplicate. They are irrelevant to this procedure.

Wait for that gauge to reach zero and you are waiting for the application
to stop producing logs, which on a live system is never. The three metrics
in the table are the ones that describe work already in flight; that one
describes work not yet started.

---

## Procedure

### 1. Quiesce the agents

Stop each Tributary with **SIGTERM**, not SIGKILL. A clean shutdown drains
the spool and reports what was left.

```sh
docker stop -t 120 tributary-a
```

The `-t 120` is deliberate. `docker stop` sends SIGTERM, waits, then
SIGKILL — and the default wait is 10 seconds.

Two things make ten seconds optimistic. The agent only checks for the
shutdown signal when its pipeline goes idle (`main.rs:452`), so a busy
agent finishes what it is doing first; and only *then* does it run the
drain — `flush`, `checkpoint_now`, `drain_queue`, in that order
(`main.rs:474-476`). A backed-up spool has to be re-shipped over the
network inside whatever budget you allowed. SIGKILL landing mid-drain
leaves batches on disk, which is precisely the state this procedure exists
to avoid.

Each agent prints a JSON summary at exit:

```json
{"read":412000,"shipped":412000,"quarantined":0,"rotations":3,"files_lost":0,
 "spilled":12,"drained":12,"bisects":0,"queued":0,"queue_bytes":0, ...}
```

**`queued` and `queue_bytes` must both be zero.** `spilled` and `drained`
being equal and non-zero is fine and expected — it means batches were
spooled during the run and all of them were re-shipped.

A non-zero `queue_bytes` here is exactly what a node loss would have cost,
and exactly what will duplicate if you change N now.

### 2. Confirm across every agent

Do not trust one exit summary and assume the fleet. Check each:

```sh
for a in tributary-a tributary-b tributary-c; do
  echo "== $a"
  curl -s "http://$a:9109/metrics" \
    | grep -E '^tributary_(queue_bytes|inflight_batches|at_risk_lines) '
done
```

Every value must be `0`. If an agent is already stopped it will not answer
— that is fine, its exit summary is the record.

The port is whatever that agent's `[telemetry] addr` is set to; `9109` is
the suggested starting point, not a default. **If an agent has no
`[telemetry]` section it has no listener at all** — that is the shipped
posture, so no port is opened that nobody asked for. For those agents the
exit summary in step 1 is the only signal you get, which is a reason to
configure telemetry on anything you intend to rebalance around.

### 3. Change N

Update `TIMELAKE_PEERS` and recreate the router and queriers with the new
list. The format is comma-separated `id=role@cluster_addr`, with the
public data address after a pipe — `id=role@cluster_addr|data_addr` — and
the router needs that second half to forward writes at all.

### 4. Confirm the router picked it up

```sh
curl -s http://router:1963/metrics | grep '^timelake_router_ingesters '
```

It must report the new N before you restart anything that writes. A router
still on the old N will keep routing on the old modulus, which is the same
split-brain by another route.

### 5. Restart the agents

They resume from their checkpoints and ship to the new homes.

---

## Verifying afterwards

Compare what the agents shipped against what the cluster holds. The two
numbers come from different places — `tributary_lines_shipped_total` is an
*agent* metric, so it is summed across the fleet, not read off the router:

```sh
# what the agents believe they delivered, summed across the fleet
for a in tributary-a tributary-b tributary-c; do
  curl -s "http://$a:9109/metrics" | awk '/^tributary_lines_shipped_total /{print $2}'
done | paste -sd+ | bc

# what the cluster will serve
curl -s -XPOST http://querier:1963/api/sql \
  -H 'content-type: application/json' \
  -d '{"db":"poc","sql":"SELECT COUNT(*) AS n FROM your_table"}'
```

`tributary_lines_shipped_total` is "lines acknowledged as durable by
TimeLakeDB (HTTP 204)" — it increments on the ack, not on the send. That
is what makes it the honest denominator: the un-acked landing this whole
procedure is about never incremented it, and the retry that followed
incremented it once. One in the agent's count, two on disk.

Two things invalidate the comparison, so check them before believing a
mismatch: a retention policy on the table (which removes rows the agents
did ship), and any other writer. Both make `COUNT(*)` legitimately differ
from the fleet's total.

Equal is the pass. **Greater than** is the failure this procedure prevents
— and note the direction, because it is the inverse of the failure most
people are watching for. Nothing goes missing here. The count comes back
too high, with total confidence.

---

## What this does not guard

**The unplanned rebalance.** A node dying is a membership change nobody
scheduled, and it is the one you would most want guarded. If you widen or
narrow the peer list in response to a node loss while agents are holding
spooled batches — which, during an outage, they certainly are — you are in
the finding's scenario exactly.

There is no procedure for that here. It is a known limitation, written
down so it stays known, in the same spirit as `recover` being an explicit
operator step rather than something automatic.

---

## If you could not drain

Since overlap-aware compaction landed (`crates/server/src/compaction.rs`,
issue #20), a partition is compacted when its files' time ranges intersect
**regardless of file count** — `trigger_for()` returns `Trigger::Overlap`
without waiting for `compact_min_files`. Twins in one partition are
overlapping by definition, so an undrained rebalance's duplicates now
resolve at the next compaction pass of the affected partitions, rather
than waiting for a fourth file that may never arrive.

That bounds the window, and as of 2026-08-22 the bound is **measured, not
promised**: Catchment's `router-tributary-exactness` (C4) re-ran the full
composition — eight streams, a frozen ingester, an undrained 2→3
rebalance — against the fix, caught the twinned tables at 202,000 rows
about 25 s after the thaw, and watched them collapse to an exact 200,000
at the next compaction pass
(`catchment/results/router-tributary-exactness-20260822-160108`, PASS).
`FINDING_rebalance_duplicates_replayed_writes.md` is closed on that run.

An earlier revision of this section claimed twins only meet "in the same
partition on the same node". Wrong, and in the misleading direction for
the shared-object-store topology: `compact_once` groups files by
`(db, table, partition)` out of the **shared catalog**, which has no node
dimension — the CAS commit loop teaches each node about its peers' files,
so twins merge wherever they were written. On the local-store topology
(no shared bucket) there is no unioning querier either, so the
cross-node-duplicate case does not arise there in the first place.

Two cautions survive the re-run:

1. Between the rebalance and that compaction pass, queries return
   inflated counts — the transient is the contract, not a bug, and it is
   an operator-visible one.
2. The drain remains the only thing that prevents the inflation rather
   than bounding it. "It will compact within a pass" is a recovery
   property, not a reason to skip the procedure.

---

## Related

- [`FINDING_rebalance_duplicates_replayed_writes.md`](FINDING_rebalance_duplicates_replayed_writes.md)
  — the measurement, the four mechanisms that compose into it, and the
  options considered.
- [`PRODUCTION_READINESS.md`](PRODUCTION_READINESS.md) §P1-1 — where this
  sits against the rest of the multi-node work.
- `ARCHITECTURE.md` §12.4 — the C2 role split and how routing works.
