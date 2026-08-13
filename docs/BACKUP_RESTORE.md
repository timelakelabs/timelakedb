# Backup and restore (AT-5)

The AT-5 acceptance test measures backup and restore; this is the procedure
it measures, as something you can run. `ops/tldb-backup.sh` does the work —
everything happens inside a throwaway helper container, so Docker is the only
prerequisite and no host bind mount is involved (bind mounts are what break
this on Windows and macOS).

```sh
./ops/tldb-backup.sh backup                          # live, no downtime
./ops/tldb-backup.sh verify  -f timelake-backup-*.tgz
./ops/tldb-backup.sh restore -f timelake-backup-*.tgz --recreate
```

## What a backup contains

Everything is under `TIMELAKE_DATA_DIR` (`/var/lib/timelake/data` in the
image, the `bench-timelakedb_timelake-data` volume under the bench compose
file). The whole directory is the backup — there is nothing else to capture:

```
$TIMELAKE_DATA_DIR/
├── wal/
│   └── wal.00000111.log                     generation-rotated write-ahead log
└── objects/
    ├── catalog/manifest/
    │   ├── 000000000306.json                append-only manifest log —
    │   └── 000000000307.json                replayed in order at boot
    └── <db>/<table>/data/<YYYYMMDDHH>/
        └── 01786183199803519018-000090.parquet
```

**Not included, because it does not live there:** the server configuration
(`TIMELAKE_*` environment variables, your compose file) and the TLS
certificate and key, which are mounted from outside the data directory. Back
those up with your configuration management. A restore into a node configured
differently — a different retention specification in particular — will apply
the new configuration to the old data.

**If encryption at rest is enabled (SEC-1):** everything under `objects/`
in the archive is ciphertext — the backup is exactly as safe to store as
the volume was, and `verify`'s entry counts still work because file names
and layout are unchanged. Two consequences to plan for:

- A restore only serves if the restored node is configured with the
  **same `TIMELAKE_ENCRYPTION_KEY`** the archive was written under. The
  wrong key is a named refusal at startup (`DEK unwrap failed`), not
  garbage data. The key is part of your configuration backup — kept
  *separately* from the archives, or the encryption was theater.
- **Losing every copy of the key is losing every backup at once.** That
  is crypto-shredding when you mean it and catastrophe when you don't.

The WAL in the archive is plaintext regardless (SEC-1 covers the object
store only), so archives of an encrypted node still contain up to a few
minutes of readable recent writes — store them accordingly.

## Why a live backup is safe

A backup taken under sustained ingest restores exactly. Three engine
properties make that true, and they bound when it stops being true:

1. **Objects are written before the manifest that references them.** The
   catalog is an append-only log committed after the Parquet files it names,
   so a snapshot can contain an object no manifest mentions (harmless — it is
   invisible until a manifest names it) but not the reverse.
2. **Every object is written temp-then-fsync-then-rename**, so a manifest
   entry is durable or absent, never torn.
3. **A truncated WAL tail is tolerated**, per generation, by design and by
   unit test. Capturing a WAL segment mid-append costs at most the frame being
   written — which by definition was not acknowledged to a client.

The one window worth naming: a file removed from the catalog is not deleted
from disk until `TIMELAKE_GC_GRACE_SECS` (default 900 s) has passed. So an
archive can only contain a dangling reference if more than the grace period
elapses between the archiver reading the catalog and reading that object.
**Keep the backup faster than the GC grace and the archive is internally
consistent** — a 25 s backup against a 900 s grace has 36× of headroom.
Quiescing writes (`--stop <container>`) removes even that; it is not required.

One artefact of a live copy: the archiver can catch a `*.tmp-write` file
mid-rename. Those are never referenced by the catalog, but one landing inside
`catalog/manifest/` would abort manifest replay at boot, so `verify` reports
them and `restore` deletes them. (Engine-side hardening worth doing: skip
non-`.json` files during manifest replay.)

## Procedure

### 1. Back up

```sh
./ops/tldb-backup.sh backup                                  # default volume, gzip
./ops/tldb-backup.sh backup -v my_volume -o /srv/backups/tldb-$(date -u +%F).tgz
./ops/tldb-backup.sh backup --no-compress                    # usually the better trade
./ops/tldb-backup.sh backup --stop timelakedb                # quiesced, with downtime
```

The source volume is mounted read-only, so a backup cannot alter the running
node. The archive streams to stdout and is redirected on your host.

**Compression rarely pays.** Parquet is already compressed. Measured on 1.19 GB
of real data: gzip saved 15% of the size and cost 2.5× the wall clock.

### 2. Verify — always, before you need it

```sh
./ops/tldb-backup.sh verify -f timelake-backup-20260808-214500.tgz
```

It streams the archive and reports entry count, manifest count, Parquet count
and WAL segments, failing if there is no manifest log to replay. An archive
with zero manifests is not a TimeLakeDB data directory, whatever its name.

A verify reads the whole archive, so it takes about as long as the backup did.

### 3. Restore

Stop anything using the target volume first — the script refuses otherwise,
because restoring underneath a live server corrupts the catalog.

```sh
docker compose -f deploy/compose/timelakedb.yml down
./ops/tldb-backup.sh restore -f timelake-backup-20260808-214500.tgz --recreate
docker compose -f deploy/compose/timelakedb.yml up -d
```

`--recreate` deletes and recreates the volume so the restore lands on empty
storage — this is the "destroyed volume" case AT-5 measures. Without it the
archive is unpacked over whatever is already there, which is only what you
want when you are repairing a partial loss. Docker refuses to delete a volume
that a stopped container still references, so `down` (not just `stop`) is the
reliable sequence.

### 4. Validate the restore

Health first, then row counts. Use a **fixed upper time bound** on both sides
rather than `COUNT(*)`, or a still-ingesting source will always look different
from the restored copy:

```sh
curl -s http://localhost:1963/health

curl -s -X POST http://localhost:1963/api/sql \
  -H 'content-type: application/json' \
  -d '{"db":"poc","sql":"SELECT COUNT(*) AS n FROM pipeline_events WHERE time < '\''2026-08-08T20:00:00Z'\''"}'
```

Recovery is a WAL replay plus a manifest replay, not an index rebuild, so a
restored node is healthy within seconds regardless of dataset size.

## Non-Docker installations

The same rules apply to a data directory on a filesystem; only the mechanics
change:

```sh
tar cf /srv/backups/tldb-$(date -u +%F).tar -C /var/lib/timelake/data .   # server may stay up
systemctl stop timelakedb
rm -rf /var/lib/timelake/data && mkdir -p /var/lib/timelake/data
tar xf /srv/backups/tldb-2026-08-08.tar -C /var/lib/timelake/data
find /var/lib/timelake/data -name '*.tmp-write' -delete
systemctl start timelakedb
```

## Scheduling

Backups are whole-directory copies — there is no incremental mode, no
point-in-time recovery, and no built-in scheduler. Run the script from cron or
a systemd timer, keep the archives on different storage from the volume, and
verify at least the newest one:

```cron
17 3 * * *  cd /opt/TimeLakeDB && ./ops/tldb-backup.sh backup --no-compress \
              -o /srv/backups/tldb-$(date -u +\%F).tar >> /var/log/tldb-backup.log 2>&1
```

Retention of the archives themselves is your policy. Sizing input: the
reference workload writes **0.50 GB/day** on disk, so a full copy costs
roughly (days retained × 0.50 GB) per archive.

## Recorded results

AT-5, full scale, on the evaluation box (`docs/evidence/`, M5 milestone):

| Step | Result |
|---|---|
| Backup | 34 s |
| Restore from a destroyed volume | 13 s |
| Rows after restore | 36,680,000 — exact |
| Comparison | the 1.x incumbent took 10–15 minutes |

Re-run of this script on a live node, 2026-08-08 (1.19 GB volume, Windows
Docker Desktop, server ingesting throughout):

| Step | Result |
|---|---|
| Backup, uncompressed | 1,191,501,824 bytes in **25 s**, zero downtime |
| Backup, gzip | 1,007,495,013 bytes in **63 s** (15% smaller, 2.5× slower) |
| Verify | ~60 s (streams the whole archive) |
| Restore into a recreated volume | **66 s** |
| Server healthy after start | **< 1 s** |
| Fixed-bound `COUNT(*)`, source vs restored | **40,327,616 = 40,327,616** |

The last row is the one that matters: an identical count under an identical
time bound, taken from a node that never stopped accepting writes and from the
copy restored beside it.
