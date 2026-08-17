# Packaging

The `.deb` and `.rpm` attached to each GitHub Release, and how to build them.

| file | what it is |
|---|---|
| `nfpm.yaml` | the package spec — **one** file, both formats |
| `build.sh` | builds the binary and both packages, entirely in containers |
| `verify.sh` | installs and runs them on every target distro |
| `timelakedb.service` | the systemd unit |
| `timelakedb.env` | the config file, installed to `/etc/timelakedb/` |
| `scripts/` | pre/post install and remove scriptlets |

## Verified on

Every one of these installs the package, starts the server and gets a healthy
`/health` — checked by `packaging/verify.sh`, which the release workflow runs.

| distro | glibc | format |
|---|---|---|
| Debian 12 (bookworm) | 2.36 | deb |
| Ubuntu 22.04 LTS | 2.35 | deb |
| Rocky Linux 9 | 2.34 | rpm |
| Amazon Linux 2023 | 2.34 | rpm |

Newer releases of each (Debian 13, Ubuntu 24.04, RHEL 10) are covered by the
same floor. Amazon Linux **2** is not: its glibc is 2.26, below the floor, and
it reached end of life in June 2026.

## Build

```sh
packaging/build.sh                 # version from `git describe`, else Cargo.toml
packaging/build.sh 0.1.0-alpha     # explicit
packaging/build.sh --skip-build    # repackage an existing dist/timelake-server
```

Output lands in `dist/`: the two packages plus `SHA256SUMS`.

Then check them:

```sh
packaging/verify.sh                 # all four target distros
packaging/verify.sh rockylinux:9    # just one
```

Docker is the only prerequisite — no Rust toolchain, no `nfpm`, no `rpmbuild`,
no `dpkg-dev`. That is the same constraint the rest of this program builds
under, and it means a laptop and the CI runner produce the artifact the same
way.

On Windows, run these from WSL, or prefix with `MSYS_NO_PATHCONV=1` under Git
Bash — MSYS rewrites the container-side paths handed to `docker -v` and `sh
/verify.sh`, so without it the mounts silently point at the wrong place.

## Why verify.sh exists

Because reading the spec does not tell you what a package manager will do. On
its first run it caught two defects:

- **`apt remove` deleted the database.** `/var/lib/timelake` was shipped as a
  package-owned directory, and dpkg removes those when the package goes and
  the directory is empty. Fixed by creating it in `postinstall.sh` instead, so
  the package manager never owns it.
- **Every install failed on Amazon Linux 2023** with `Error in PREIN
  scriptlet` and no further explanation. AL2023 ships without `shadow-utils`,
  so the `useradd` in `preinstall.sh` did not exist. Fixed with a
  `shadow-utils` / `passwd` dependency, plus a scriptlet that says which
  package is missing instead of failing the transaction silently.

Neither is exotic, and neither would have been found before a user found it.

## One spec, two formats

`nfpm.yaml` is read twice, once per format. The alternative — `cargo-deb`
metadata plus a `cargo-generate-rpm` block — means two descriptions of the
same package that drift apart, and the drift is always found by whichever
user got the less-tested one. The places the formats genuinely differ live in
one `overrides:` block: `libc6 (>= 2.31)` against `glibc >= 2.31`.

## The glibc floor

**Packages require glibc 2.31 or newer**: RHEL/Rocky/Alma 9+, Debian 11+,
Ubuntu 20.04+. RHEL 8 (glibc 2.28) is not covered.

This is a build-time property, not a runtime one. A dynamically linked binary
inherits the glibc of the machine that linked it and refuses to start on
anything older, so the floor is decided by `BUILD_IMAGE` — currently
`debian:11`. Linked on a current image instead, the server demands
`GLIBC_2.39`, which RHEL 9 (2.34), Debian 12 (2.36) and Ubuntu 22.04 (2.35)
do not have: the package would install cleanly and then fail at startup with
a symbol-lookup error, which is the worst possible way to learn about it.

`build.sh` reads the built binary's highest required `GLIBC_*` symbol and
fails the build if it exceeds what the metadata promises. Without that check
the floor is a comment; with it, raising `BUILD_IMAGE` by accident breaks the
build rather than somebody's server.

A fully static musl build would remove the floor entirely and is the obvious
next step, but `aws-lc-sys` and `zstd-sys` are in the tree and both compile C,
so it is its own piece of work rather than a flag.

## What the package does, and does not, do

It installs:

```
/usr/bin/timelake-server                     the server
/usr/lib/systemd/system/timelakedb.service   the unit
/etc/timelakedb/timelakedb.env               config; your edits survive upgrades
/var/lib/timelake/data                       data, owned by the timelake user
/usr/share/doc/timelakedb/                   README, SECURITY, CHANGELOG, LICENSE
```

It creates a `timelake` system account with no shell, and points the unit at
it. The unit is hardened the way the container is (SECURITY.md exposure 4):
`ProtectSystem=strict` with `/var/lib/timelake` as the only writable path, no
new privileges, a `@system-service` syscall filter.

**It does not start the service, and that is deliberate.** With
`TIMELAKE_DATA_AUTH` unset, any client that can reach the port has full read
and write access to every database (SECURITY.md exposure 1). A package that
begins listening because somebody ran `apt install` would hand that to
whatever the machine is attached to. So the shipped config binds `127.0.0.1`
only, the unit is installed but not enabled, and the postinstall prints the
configure-then-`systemctl enable --now` steps.

Uninstalling does not delete `/var/lib/timelake` or the `timelake` user.
`apt purge` additionally removes `/etc/timelakedb/timelakedb.env`, and says
plainly that the data is still there.

## Releasing

`.github/workflows/release.yml` runs on a `v*` tag: it calls `build.sh`,
installs the `.deb` on Debian 12 and the `.rpm` on Rocky 9 as a smoke test,
and attaches both plus `SHA256SUMS` to the Release. A tag containing `-`
(`v0.1.0-alpha`) is marked as a pre-release.

It deliberately does not re-run the test suite: `ci.yml` proves the tree, and
duplicating ~30 minutes of billed private-repo minutes per tag makes cutting
a release something people avoid doing. Tag a commit whose CI was green.
