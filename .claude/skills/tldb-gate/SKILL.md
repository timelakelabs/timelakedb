---
name: tldb-gate
description: Build, test, and run TimeLakeDB's benchmark gate cheaply — the exact commands and pass criteria, so a fresh session needs no re-derivation. Use for "run the gate", "build and test", "verify milestone".
---

# TimeLakeDB gate runner

This machine has NO Rust toolchain — everything runs via Docker. Filter
all command output (grep/Select-String) — never dump full build logs.

All commands below assume the repository root as the working directory,
and mount it via `${PWD}` rather than any absolute path.

## Test (fast, cached)

```powershell
docker run --rm -v "${PWD}:/src" -v timelake-cargo-cache:/usr/local/cargo/registry -w /src rust:1-slim sh -c "cargo test --workspace 2>&1 | grep -E 'test result|error|FAILED' | grep -v 'ok. 0 passed'"
```

## Rebuild + start (with Grafana fixture)

```powershell
cd deploy
docker compose -f compose/timelakedb.yml --profile grafana up -d --build
# fresh data: down first, then Remove-Item -Recurse compose\data\timelake
```

## Bench gate

The harness lives in the Gauge repository, not here:

```powershell
cd ..\Gauge   # sibling of this repository
python bench\bench.py run --backend timelakedb --scale smoke  --label <X>   # shakeout, ~15 s
python bench\bench.py run --backend timelakedb --scale laptop --label <X>   # milestone gate, ~1 min
```

Pass criteria:
- 0 errors in every scenario; all five Shape B queries complete.
- context counts vs accepted lines: rows_48h == pipeline lines + burst
  (small deficit = LWW dedup of source PK collisions — compare against an
  influxdb3 run on the same fresh dataset before calling it a bug; the
  influxdb3 baselines live in `../Gauge/bench/results/influxdb3-idb3-full-*`).
- Grafana (port 3003, admin/admin): datasource health OK; funnel panel
  query returns 10 steps. Crash drill: `docker restart -t 0 timelakedb`
  (NOT `docker kill` — kill suppresses the restart policy), healthy ≤30 s,
  counts unchanged.

## Commit

Conventional message + evidence in body; end with the session's
Co-Authored-By/Claude-Session trailers. Update README "Status" + CLAUDE.md
status bullet with the gate numbers. run.json lands in
`docs/evidence/<run-id>/` — cite it.
