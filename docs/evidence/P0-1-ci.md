# P0-1 — CI green on a real runner

**Recorded 2026-08-13.** Every claim below is a run on GitHub-hosted
infrastructure, cited by URL and commit sha. Nothing here was measured on the
author's laptop; that is the entire point of the item.

Until this file existed, every quality claim in this program rested on runs on
one Windows machine through a Docker container. `ci.yml` had never executed on
a runner at all.

## The five runs

| Repo | sha | Run | Result |
|---|---|---|---|
| TimeLakeDB | `fe2b45b` | [31667503653](https://github.com/timelakelabs/timelakedb/actions/runs/31667503653) | success |
| Tributary | `5eee47d` | [31561969531](https://github.com/timelakelabs/tributary/actions/runs/31561969531) | success |
| Catchment | `17276bf` | [31650406399](https://github.com/timelakelabs/catchment/actions/runs/31650406399) | success |
| Gauge | `1be9c8e` | [31591472854](https://github.com/timelakelabs/gauge/actions/runs/31591472854) | success |
| Riverkeeper | `a5552dc` | [31591721097](https://github.com/timelakelabs/riverkeeper/actions/runs/31591721097) | success |

Each sha is the current head of its default branch, so no row is a green run
on a superseded tree.

## What TimeLakeDB's run establishes

Run 31667503653, four jobs, all green:

| Gate | Result |
|---|---|
| `cargo fmt --all --check` | pass |
| `cargo clippy --workspace --all-targets -- -D warnings` | pass |
| `cargo test --workspace` | **173 passed, 0 failed** |
| `cargo llvm-cov --fail-under-lines 80` | **pass, 82.74% lines** |
| `store-s3` — Store contract, KMS envelope, catalog CAS vs LocalStack | pass |
| `catchment · conformance (tier 1)` — image built from this tree | pass |
| `publish` — `ghcr.io/timelakelabs/timelakedb:main` | pass |

The 173 figure is the one this document has claimed since 2026-08-10. It was
correct; it had simply never been established anywhere but one machine.
Coverage at 82.74% matches the pre-Gauge-split measurement exactly, so the
split moved no tested code.

`store-s3` passing on a runner is what makes the P0-4 CAS claim a statement
about S3 semantics rather than about a local hard-link emulation.

## What the conformance runs establish

`catchment ci --tier 1` executed and returned PASS in two places: Catchment's
own run against both products at main, and TimeLakeDB's run against an image
built from its own tree. This is the first execution of the conformance gate
anywhere other than a laptop.

## Three failures worth keeping

None was a defect in the software. All three were infrastructure, and each
would have recurred silently.

**Runner disk.** The `rust` job died with
`ld terminated with signal 7 [Bus error]`, which reads as a broken build. The
cause was four lines earlier: 87 MB of disk left. A 16-crate workspace built
`--all-targets` and rebuilt instrumented for coverage does not fit beside a
hosted runner's preinstalled images. Fixed by reclaiming ~25 GB the job never
uses. Run 31561992129 is the failure; `fe2b45b` carries the fix.

**Conformance ran no scenarios and still failed late.** TimeLakeDB's
conformance job checked out Catchment but not Tributary, so `catchment doctor`
reported the corpus generator missing and exited 2. Because the step runs under
`bash -e`, the `ci --tier 1` line never executed: the job failed having tested
nothing. Catchment's own job had always checked out both products, which is why
it went green on the same tier while this went red. This is the "green run that
executed nothing" failure mode the harness exists to prevent, arriving through
the workflow rather than the suite — caught only because `doctor` runs first.

**Riverkeeper could not test itself alone.** Its suite loaded 25 of 47 tests
and died on `SECURITY.md not found`. `SECURITY.md` is the specification it
executes, loaded at import time, so a checkout without the sibling product
fails to *collect* tests rather than fail them. It had passed locally only
because TimeLakeDB happens to sit beside it on the development machine — the
sibling a runner does not have.

## What this file does not establish

- **The repos are still private.** Flipping to public, enabling Pages, and
  tagging `v0.1.0-alpha` are the other three items under P0-1's "Remaining",
  and none is done. `pages.yml` self-skips on a private repo, so the site is
  not published.
- **Riverkeeper's R0 (`9296c05`) is unsigned and pushed.** `git verify-commit
  HEAD` returns Good in five of six repositories, not six. Signing it means
  rewriting a published commit and force-pushing.
- **Tier 2 and tier 3 have not run.** Only tier 1 gates a push. The clustered,
  S3 and TLS scenarios are nightly in Catchment's own repository.
- **Coverage margin is thin.** 82.74% against an 80% gate. The first moderate
  feature landing without tests turns this red.

## Setup this depended on

Recorded because it is invisible from the workflows and cost several red runs
to discover:

- A GitHub App (`timelake-ci`, id `4576887`) installed on the `timelakelabs`
  org, with `CI_APP_ID` and `CI_APP_KEY` set in **four** repositories — the
  three product repos plus Riverkeeper, which mints a token but is never
  itself the repository being fetched.
- `ghcr.io/timelakelabs/timelakedb` granted Read to the Tributary repository
  under Manage Actions access. Without it Tributary fails with `denied`; before
  the image existed at all it failed with `manifest unknown`.
