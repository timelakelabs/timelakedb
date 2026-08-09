Run one TimeLakeDB performance-improvement cycle in this repository — the runner (`ops/run-perf-cycle.ps1`) has already set the working directory to the repository root, so use relative paths throughout and never hardcode a checkout location. Work autonomously and finish inside ~90 minutes; if you cannot, log it as ABORTED and clean up.

ISOLATION — non-negotiable, this Docker host runs production services:
- NEVER touch the running containers `timelakedb`, `timelake-telegraf`, `timelake-grafana`, the volume `bench-timelakedb_timelake-data`, or anything else already running (Paperless, Tailscale, Caddy, etc).
- NEVER restart Docker Desktop, and never use `docker compose` — it would recreate the user's stack. Plain `docker run` only.
- Use your own instance only: build an image tag like `timelakedb:perf`, run a container named `tldb-perf` with `--memory 8g`, port `2965:1963`, and a dedicated volume `tldb-perf-data`. Point the harness at it with `--url http://localhost:2965 --container tldb-perf`.
- Remove your container, volume and image at the end of the cycle, win or lose.

NO local Rust toolchain — build and test through Docker:
docker run --rm -v "${PWD}":/src -w /src -v timelake-cargo-cache:/usr/local/cargo/registry rust:1-slim cargo test --workspace

THE CYCLE:
1. Read docs/evidence/PERFORMANCE_LOG.md first, all of it. Do not repeat an idea already logged unless the entry says a retry is worthwhile. The "Standing leads" section is starting material, not a mandate, and later entries carry measurement rules that override it.
2. Confirm the git tree is clean and you are on master. If it is dirty, log ABORTED and stop — the user may be mid-edit.
3. Pick ONE idea. Write the hypothesis down before measuring: what gets faster, by roughly how much, and why. Prove the cost you are attacking is on the critical path before optimising it.
4. Measure a baseline from unmodified master on your isolated instance: `--scale smoke` to shake out, then `--scale laptop --shape-a-samples 100`.
5. Implement the change. Keep it focused and in the project's style.
6. Verify correctness before speed: `cargo test --workspace` must pass (52 as of 2026-08-09), and the files you touched must be `cargo fmt --all --check` clean and free of new clippy warnings. The repo has PRE-EXISTING fmt/clippy drift against stable 1.97.1 — judge only the lines you wrote.
7. Rebuild the image and measure the candidate the same way.
8. Decide honestly. A win must clear run-to-run noise, not just move a number. A regression, a broken test, or an ambiguous result is a REJECTED outcome — say so plainly rather than rationalising it.

MEASUREMENT RULES learned the hard way — the log explains each:
- Discard any run whose ingest falls below ~500K lines/s; the host is noisy.
- The harness's storage metric is unusable at laptop scale (it samples mid-flush). Measure `du` of the data directory instead.
- Scenarios share a metadata cache and run in a fixed order. If one regresses, re-run it in isolation before believing it.
- Query latency measured from Windows through the published port carries ~45 ms of Docker Desktop overhead — about 94% of the Shape A figure. Run the harness inside the docker network (`docker run --network container:tldb-perf`) or treat any Shape A delta under ~45 ms as unmeasurable.

RECORD THE OUTCOME ON master EITHER WAY, appending an entry to docs/evidence/PERFORMANCE_LOG.md in the documented format (### date — title, then Hypothesis / Change / Measurement / Verdict / Lesson):
- ADOPTED: commit the code change together with the log entry.
- REJECTED: revert the code (`git restore` the touched source paths), then commit the log entry alone — the lesson is the deliverable. Name what you learned specifically enough that the next cycle can act on it.
- ABORTED: commit a log entry saying what blocked the cycle.
Never push (there is no remote). Leave the tree clean and on master.

Finish with a few sentences: the idea, the numbers, the verdict, and what you left behind.
