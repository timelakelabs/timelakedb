# Releasing TimeLakeDB

Every cut so far has dropped a different step, and never the same one, so
none of them got learned. 0.2.0 and 0.3.0 went in through a pull request,
0.4.0 was pushed straight to `main`; 0.4.0 shipped no container image at
all and its Helm chart advertised `appVersion: "main"` (#166). This is the
list. It is short because most of it is automated now — the point of the
document is to say which parts are, so nobody "helpfully" does them by
hand.

## What a tag does on its own

Pushing `vX.Y.Z` starts `.github/workflows/release.yml`, which:

1. **Refuses** unless `ci.yml` has already finished green on that exact
   commit (#168). Not "was expected to be green" — it looks.
2. Builds the `.deb` and `.rpm` in containers, installs them on Debian 12,
   Ubuntu 22.04, Rocky 9 and AL2023, and attaches them with `SHA256SUMS`.
3. Publishes `ghcr.io/timelakelabs/timelakedb:X.Y.Z` for `linux/amd64` and
   `linux/arm64`, plus `:latest` for a non-prerelease tag, then inspects
   the manifest and fails if either platform is missing (#167).
4. Packages the Helm chart **with `version` and `appVersion` stamped at the
   tag**, pushes it to `oci://ghcr.io/timelakelabs/charts`, and attaches the
   `.tgz` to the Release.

So: no image step, no chart edit, no artifact upload is yours to do.

## Before you tag

**Do not bump `deploy/helm/timelakedb/Chart.yaml`.** It reads `0.1.0` /
`appVersion: "main"` in the tree on purpose: that is what a developer who
runs `helm install` from a checkout should get. The *artifact* is what has
to name a real release, and the workflow stamps it. A version somebody
remembers to bump by hand is a version that eventually does not get
bumped, which is how 0.4.0 shipped a chart pointing at `main`.

The release commit touches exactly four things:

| File | Change |
|---|---|
| `CHANGELOG.md` | promote `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD` and open a fresh `[Unreleased]` above it |
| `Cargo.toml` | the workspace `version` |
| `Cargo.lock` | follows the workspace version — regenerate, do not hand-edit |
| `README.md` | the `VER=` line in the install snippet |

No local Rust toolchain on the usual machine, so the lock file comes from
a container:

```sh
docker run --rm -v "$PWD:/w" -w /w rust:1-slim cargo update -w
```

Read the promoted CHANGELOG section once as a stranger. Every performance
or robustness entry must trace to a run in `docs/evidence/`; an entry with
no measurement behind it does not belong in a release.

## The order, and why it is this order

**`main` refuses a direct push** (#169: required checks, no bypass), and
the release workflow **refuses a tag whose CI is not green** (#168). Those
two together fix the order:

1. Open a pull request with the release commit. `changes` will say
   `code=true` because `Cargo.toml` moved, so the full suite runs.
2. Merge it.
3. **Wait for `ci.yml` to finish green on the merge commit on `main`.**
   Roughly 25 minutes. Tag before that and the gate refuses the tag — that
   is the gate working, not a fault.
4. Tag the merge commit and push:

```sh
git checkout main && git pull --ff-only
git tag -a vX.Y.Z -m "TimeLakeDB X.Y.Z — <what a reader would want to know>. .deb + .rpm attached."
../ops/git-push-ssh.sh --tags
```

Tags are signed (`tag.gpgsign` is on) and annotated. The message is prose
that names what shipped, not a version number repeated three times — see
`git tag -n99 v0.4.0`.

If the gate refuses, nothing is lost and the tag does not move: fix
whatever is red on `main`, then **re-run the release workflow from the
Actions page**. A re-run keeps the original event, so it still publishes.

## After the tag

Watch the run to the end. Then:

- `gh release view vX.Y.Z --json assets` — the `.deb`, the `.rpm`,
  `SHA256SUMS`, and the chart `.tgz`.
- `docker buildx imagetools inspect ghcr.io/timelakelabs/timelakedb:X.Y.Z`
  — both platforms.
- `helm show chart oci://ghcr.io/timelakelabs/charts/timelakedb --version X.Y.Z`
  — `appVersion` is `X.Y.Z`. Checking the chart **in the tree** instead is
  the mistake #166 was filed for; that copy still says `main`, correctly.

## The cross-repo half, which is where steps go missing

None of this is in a workflow, and all of it has been forgotten at least
once:

- **Close the milestone, open the next.** `gh api
  repos/timelakelabs/timelakedb/milestones` — a release with no milestone
  is a release nobody can enumerate afterwards.
- **Add the row to the umbrella's `ROADMAP.md` §4.** This one is now
  enforced from the other side: the umbrella's `ops/status.py` fails if a
  released version has no row, so skipping it turns that repository's CI
  red rather than going unnoticed for two releases (umbrella#9).
- **Move the board.** Org Project #2: the shipped items to Done, and set
  `Release` on anything that landed in this version.
- **Re-read `docs/PRODUCTION_READINESS.md` §0.** It describes the current
  posture and it is the document most likely to be quietly wrong after a
  release; four of its lines were, for a fortnight (#184).

## What not to do

- **Do not make `release.yml` re-run the test suite.** Its header explains
  the ~30 minutes per tag, and that cost is why cutting a release stops
  being something people avoid. The gate reads `ci.yml`'s verdict instead.
- **Do not hand-edit the chart, the image tags, or the release assets.**
  See the first section.
- **Do not tag a commit that is not on `main`.** The gate looks up the
  commit's own ci run; a branch commit may have one, which makes this
  quietly work and produces a release nobody can find in the history.
