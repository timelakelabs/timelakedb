# TimelordDB

A time-series database for high-cardinality event analytics, specified
from evidence: five engines ran an identical 36M-event workload and their
measured failures define this one. The survivor (InfluxDB 3) sets the
baselines this project must beat; the failures (a query OOM-killing
InfluxDB 1.8, InfluxDB 2.x's funnel never completing, QuestDB and
VictoriaMetrics OOMs) define what it must be structurally incapable of.

**Stack:** Rust · Apache DataFusion · Arrow · Parquet · `object_store`.

| Read | Purpose |
|---|---|
| `REQUIREMENTS.md` | Evidence-traced requirements (FR/PR/RR/SR/CL/SEC + acceptance tests) |
| `ARCHITECTURE.md` | Components, seams, milestones M0–M5 |
| `docs/evidence/` | The benchmark record this project is built on |
| `bench/` | tsdb-bench — the executable acceptance spec + recorded baselines |

## Status: M0

Cargo workspace + server stub (`/health`, `/ping`; write/SQL endpoints
answer 501 until M1), bench adapter, compose target, CI.

## Quickstart (Docker — no local Rust needed)

```bash
cd bench
docker compose -f compose/timelorddb.yml up -d --build
curl http://localhost:1963/health
python bench.py backends       # timelorddb is registered
```

Local development additionally wants a Rust toolchain
(`rustup` + MSVC build tools on Windows), then:

```bash
cargo test --workspace
cargo run -p timelord-server
```

## The rule

The benchmark harness is the specification. Every milestone gates on a
`bench.py run --backend timelorddb` result, compared against the recorded
InfluxDB 3 baselines in `bench/results/`.
