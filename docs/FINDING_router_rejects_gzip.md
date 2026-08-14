# The router rejects every gzip write body as malformed line protocol

**Found 2026-08-13** by Catchment's `router-tributary-exactness` (C4), on
its first execution — before the scenario ever reached the failure it was
designed to probe.

---

## The symptom

Four Tributary streams, 40,000 lines each, shipped through the router.
Every agent run ended:

```
read: 40000   shipped: 0   spilled: 0   requests: 79921   exit: 0
```

79,921 requests for 80 batches is the bisect signature: the router
rejected each batch with 400, the agent did exactly what it should with
a rejected batch — halve it hunting the poison line — and at width one it
quarantined every line of a correct corpus. Eight streams, 320,000 lines,
all dead-lettered, all agents exiting 0. The tables held only the
harness's probe rows.

Evidence: `catchment/results/router-tributary-exactness-20260813-225440/`.

---

## The cause

Three facts compose:

1. Tributary gzips write bodies **by default** (`[output] gzip`,
   `default_true`). Stock Telegraf's `influxdb_v2` output also speaks
   gzip — `crates/api`'s own doc comment says so.
2. The single-node endpoint decompresses: `api::maybe_gunzip` checks
   `Content-Encoding: gzip` before parsing.
3. The router's write handler did not. It read the **raw request bytes**
   and validated them as line protocol, so a gzip body was "not utf-8"
   or "line has no measurement" — a 400 whose text blames data the
   client never wrote.

So the router — whose module doc calls it "the single write endpoint the
bench adapter, Telegraf and Grafana keep seeing" — was not a drop-in for
the endpoint it fronts. Any client with compression on worked against
one node and lost 100% against the cluster, and the failure dressed
itself as the client's poison data, sending the operator to debug the
agent that was behaving correctly.

The C1 calibration pair could not have caught this: it ships to the
single topology, whose endpoint decompresses. The seam scenario exists
precisely because composed paths get different bugs than their parts.

---

## The fix

The router now decompresses exactly as the single-node endpoint does,
before validating, and forwards each shard **plain**: the router
re-chunks bodies, so what it forwards must stand alone, and the
intra-cluster hop is a LAN where correctness outranks recompression. A
corrupt gzip body is a 400 that says "bad gzip body" — the failure names
itself instead of blaming the payload.

## Status

**Fixed 2026-08-13.** Regression tests
`a_gzip_write_body_is_decompressed_validated_and_forwarded_plain` and
`a_corrupt_gzip_body_is_a_400_that_says_so` (crates/server/tests/
router.rs), both verified failing against the unfixed handler. The live
confirmation is the C4 scenario's rerun.
