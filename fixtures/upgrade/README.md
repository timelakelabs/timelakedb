# Upgrade fixtures

A data directory each release wrote, kept so a **later** release can prove
it still reads one.

```
0.4.0.tgz    the data directory, tarred as ops/tldb-backup.sh writes one
0.4.0.json   what a later release must still find in it
```

Cut by `ops/make-upgrade-fixture.sh <version>` at release time, consumed by
Catchment's `upgrade-from-released-fixture` scenario.

## Two rules

**Never regenerate an old fixture with a newer binary.** The entire value of
`0.4.0.tgz` is that 0.4.0 wrote it. Rewriting it with 0.5 turns the test into
0.5 reading its own output, which is what the rest of the suite already does.
If a fixture looks wrong, that is a finding, not a maintenance task.

**Never take the backup quiesced.** The generator SIGKILLs the node with rows
still unflushed, so the archive carries a dirty WAL. A clean shutdown flushes,
and a flushed WAL silently drops `WAL_VERSION` out of the formats under test —
replay across a version boundary being exactly the thing that breaks.

## What one contains, and why each part is there

Four persisted formats live in a data directory, added at four different
times by four different pieces of work. A fixture that exercises one of them
tests one of them.

| in the fixture | the format it exercises |
|---|---|
| Parquet across two hour partitions, and the manifest listing them | `MANIFEST_FORMAT_VERSION` |
| 25 rows still in the WAL at SIGKILL | `WAL_VERSION` |
| a retention policy, a rollup, a last-value cache | `RETENTION_FORMAT_VERSION`, `ROLLUPS_FORMAT_VERSION` |
| two issued tokens, a rotated admin principal | the token and principal stores |
| a declared table (`sensors`) | schema declarations in the manifest |
| a dropped table (`scratch`) | drop markers in the manifest |
| a targeted delete (`metrics`, `host=web-1`) | tombstones in the manifest |

The last two are the load-bearing ones. A row count alone passes while a
tombstone silently reverts, and that is timelakedb#160 exactly: entries an
older reader drops without a word, so a `DROP TABLE` reads as an empty
commit and the files it retired stay listed. The dropped table's Parquet is
still in the archive — it was dropped inside the GC grace — so a build that
mis-reads the drop marker will cheerfully serve it, and the scenario will
see that.

## Size

Small on purpose: these are committed and kept forever. `0.4.0.tgz` is
about 12 KB. If a fixture ever needs to be large to prove something, the
thing it proves probably belongs in a different test.
