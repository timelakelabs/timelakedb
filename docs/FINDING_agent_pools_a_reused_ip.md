# A recreated neighbor's IP wedges the agent, and the agent never says so

**Found 2026-08-22** by re-running Catchment's `router-tributary-exactness`
(C4) against the overlap-compaction fix. The re-run was supposed to close
`FINDING_rebalance_duplicates_replayed_writes.md` — and did, on the
merits — but three of five runs wedged in a way the original finding does
not describe. This is that wedge.

---

## The symptom

Phase B recreates the router and both queriers with a widened peer list
(the rebalance under test). In three runs of five, every Tributary agent
that was shipping when the reshape happened **never delivered another
byte**. Not slowly — never. All four agents sat with 221–390 spooled
segments each, frozen byte-for-byte for over half an hour, while the
recreated cluster answered a manual write probe in 5 ms:

```
$ docker top bench-timelakedb-cluster-s3-tributary-1
tributary --config /etc/tributary/b0.toml ... --once     TIME 00:00:02
tributary --config /etc/tributary/b1.toml ... --once     TIME 00:00:02
...          (four agents, ~2s of CPU each, 30+ minutes after the reshape)

$ curl -XPOST http://localhost:5970/write?db=catchment --data-binary "..."
204 in 0.010s
```

No error in any log. No counter moving. `tributary_queue_bytes` frozen,
checkpoint mtime frozen, and the router's `timelake_router_forwarded_total`
showing only the probes sent by hand. From every dashboard this deployment
would have had, the agents were healthy and the data was simply not
arriving.

---

## The cause

Three mechanisms, each defensible alone:

1. **Docker reuses freed IPs.** The reshape removes the router and both
   queriers, then recreates them. The router was `192.168.0.7`; after the
   reshape, **querier-b** is:

   ```
   192.168.0.6/20  cl3-querier-a
   192.168.0.7/20  cl3-querier-b     <-- the router's old address
   192.168.0.9/20  cl3-router
   ```

2. **A keep-alive pool consults DNS only on dial.** During the reshape
   window the agents' ships fail and spool (correct). When an agent
   redials `router`, the connection it gets lands on `.7` — where
   querier-b now accepts. From then on hyper reuses that healthy
   keep-alive connection for every request. A pooled connection that
   keeps answering HTTP is never redialed, so the stale resolution is
   never corrected. This is not a Docker quirk to wave off: **Kubernetes
   reuses pod IPs on every rollout.** Any agent shipping through a
   keep-alive pool while its neighbors are rescheduled can land here.

3. **The shipper files 501 under retryable transport.** A querier answers
   writes with:

   ```
   HTTP/1.1 501 Not Implemented
   {"error":"this node is a querier (TIMELAKE_ROLE=querier) and holds no
   write path — send writes to the router, or to an ingester directly"}
   ```

   `ship.rs` maps every status outside its named set to
   `ShipError::Transport` — retry the wire. So the agent retries. The
   same connection. Forever.

The loop, caught live by strace after `/proc` had nothing to say
(`wchan` masked on this WSL2 kernel; the agents *looked* asleep):

```
19:16:17.256123 recvfrom(12, "HTTP/1.1 501 Not Implemented\r\nco"..., 8192, ...) = 262
19:16:17.464383 recvfrom(12, "HTTP/1.1 501 Not Implemented\r\nco"..., 8192, ...) = 262
19:16:17.669624 recvfrom(12, "HTTP/1.1 501 Not Implemented\r\nco"..., 8192, ...) = 262
   ...  (~5/second, the same fd, indefinitely)
```

`drain_queue` retries the front segment each pass, gets 501 through the
pooled connection, returns "still down; try again next tick", and the
tick tries again. ~5 requests a second, none of them reaching the actual
router, none of them counted anywhere that distinguishes this from an
ordinary outage.

The bitter detail: **the server names the problem in every single
response** — "this node holds no write path, send writes to the router" —
three hundred times a minute, and the agent reads none of it.

## Why nothing surfaced it

- The response is fast and well-formed, so nothing times out.
- 501 is not 4xx-rejected, not 401/403, not 429 — it lands in the
  catch-all `Transport` arm, whose whole design premise is "retrying the
  wire can fix this". Here the wire is the problem.
- The retry goes to the pooled connection, so no dial, no DNS, no chance
  for the network to self-correct.
- `--once` mode exits only when the queue is empty. The queue can never
  empty. The agent cannot finish, cannot progress, and has no
  N-failures-then-give-up escape.

Diagnosis was itself misled twice, which is worth recording: the frozen
counters and ~zero CPU read as *parked in a long sleep*, and two
hypotheses (an uncapped `retry-after` sleep; DNS threads piling up) were
eliminated by measurement before strace showed the loop was hot. A
five-per-second failure loop with no logging is indistinguishable from a
hang from outside the process.

---

## What this is not

Not the duplicate-rows finding. The same C4 re-runs that exposed this
also confirmed the original finding closed: an undrained rebalance's
twinned batches now collapse at the next overlap-triggered compaction
pass (202,000 → 200,000 observed, `transient_rows_collapsed: 2000` in
`catchment/results/router-tributary-exactness-20260822-144638`).

Also not explained here: run 4 of 5
(`router-tributary-exactness-20260822-145358`) wedged differently — rows
acked but ingester buffers frozen at 48/48/96 on all three nodes,
including one no fault ever touched. That did not reproduce and is
recorded as open, not folded into this finding.

---

## The fix

In the shipper (`Tributary crates/tributary/src/ship.rs`):

1. **501 is not transport.** It is the server stating the client dialed
   the wrong node. On 501 the shipper rebuilds its client — dropping the
   connection pool, so the next attempt dials fresh and re-resolves —
   and returns a retryable error so the existing spool machinery keeps
   custody of the data.
2. **A consecutive-failure backstop.** Any N consecutive transport
   failures also rebuild the client, bounded by a cooldown. This covers
   the inheritor that answers 404, or hangs, or speaks something other
   than HTTP — any flavor of "the pool is pinned to the wrong peer",
   not just the one flavor that was observed.

The rebuild reuses the `ArcSwap` machinery L4 credential rotation
already established: swapping a whole client is already how this shipper
changes transport state, and in-flight batches finish on the client they
started with.

## Status

**FIXED and verified in composition, 2026-08-22.**

The shipper now treats 501 as what it is — the peer saying "wrong node" —
and rebuilds its client on the spot, dropping the pool so the next
attempt dials and resolves fresh (`ShipError::WrongNode`). A backstop
rebuilds after every third consecutive transport-class failure, covering
inheritors that answer 404, hang, or speak something other than HTTP.
Both are visible: `tributary_transport_rebuilds_total` on `/metrics`,
`transport_rebuilds` in the exit summary — the counter exists because
this failure was otherwise indistinguishable from an ordinary outage.

Verified at three levels, harshest first:

- **Composition**: the C4 scenario that found the wedge, re-run against
  the patched agent
  (`catchment/results/router-tributary-exactness-20260822-160108`, PASS
  17/17). All four agents crossed the reshape, drained 44–45 spooled
  segments each, and exited zero — where the unpatched agent held its
  221–390 segments forever.
- **Unit, red-proofed**: a scripted TCP peer answering 501 must see a
  new connection per attempt. With the pool-drop deliberately neutered
  the test fails with `conns: 1` — one keep-alive connection absorbing
  every retry, the wedge in miniature — so the assertion is known to
  detect the behavior it guards.
- **Streak semantics**: rebuild on the 3rd and 6th consecutive
  unrecognized answer; an acknowledged write in between resets the
  streak, so intermittent flakes do not churn healthy connections.

Left open, deliberately: the agent still spools and retries forever
rather than exiting — an agent's data outliving its patience is the
design (`queue.rs` header). What changed is that its retries can now
escape a poisoned pool.
