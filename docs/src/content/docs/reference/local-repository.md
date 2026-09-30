---
title: Local Repository
description: Standard persistent profile layout, process coordination, collection behavior, and operational boundaries.
---

`Repository::local(path).await` opens the standard persistent profile. CLI
commands that open a local repository use the same profile. `sync` selects
its source and destination explicitly; archive inspection needs no
repository.

Opening a new profile creates its payload directory, lock files, and logical
database. Opening an already-current profile performs no logical state write.
Schema initialization or upgrade may write and is coordinated like any other
state mutation.

## Layout

The main paths are:

```text
<repository>/
  blobs/
    blobs/b3/<xx>/<hex>
    chunks/b3/<xx>/<hex>
    bao/b3/<xx>/<hex>
  casita.sqlite
  casita.sqlite.online-pins
  gc.lock
  spill/
  runs/
```

| Path | Purpose |
|---|---|
| `casita.sqlite` | Revisioned object records, ordered links, named roots, and current repository revision, plus two local accelerators: verified closures and the ingest cache |
| `blobs/blobs/b3/...` | Multi-chunk and empty-payload manifests |
| `blobs/chunks/b3/...` | Zstd-compressed FastCDC chunks |
| `blobs/bao/b3/...` | Optional derived Bao outboards for verified range reads |
| `casita.sqlite.online-pins` | Durable scoped pins and deletion claims |
| `gc.lock` | Cross-process single-collector mutex |
| `spill/` | Temporary traversal state, present only while a traversal exceeds its memory budget |
| `runs/` | Temporary checkouts for `casita run`; an interrupted run may leave files here |

Spill files are not repository state. A traversal that outgrows `SpillLimits`
moves its visited set and work queue into a temporary database here, deletes it
when the traversal ends however it ends, and holds a lock file meanwhile so a
repository open sweeps abandoned files without disturbing a live traversal in
another process.

## Spill telemetry

`fsck()` reports traversal temporary-state use in `FsckReport::spill`, and
`collect()` reports the same information in `CollectionOutcome::spill`. The
`SpillMetrics` summary has two fields:

- `files_opened`: temporary spill databases opened during the operation.
- `peak_bytes`: the greatest aggregate footprint observed for active spill
  databases, including their SQLite WAL files.

Metrics are captured before cleanup. It is therefore normal for a completed
operation to report nonzero values while `spill/` is empty afterwards. They are
performance diagnostics, not persistent repository state or a quota guarantee.

The sharding key is lowercase BLAKE3 hex; `<xx>` is its leading shard.
Single-chunk payloads omit a separate manifest because their chunk and payload
digests are identical.

:::caution
This physical layout is an implementation detail of the standard local
profile, not a logical format contract. Do not construct object keys from
paths, edit the database, replace manifests, or depend on compression and
chunking choices in an application protocol.
:::

## Logical state

The SQLite-format database stores four generic concerns:

- one opaque current repository revision;
- immutable object keys, physical payload IDs, and verified sizes;
- canonical ordered forward links; and
- validated named roots selecting exact object keys.

Logical state is authoritative. Casita does not reconstruct records by scanning
the payload directory. Physical data without a record is collectible residue;
a record with no physical payload is an integrity failure.

## Local accelerators

The same database holds two tables that are not logical state. Neither is
consulted by a reader, neither participates in the repository revision, and
either may be discarded: losing one costs work, never correctness.

- **Verified closures** record which object graphs have already been checked
  whole. Records are immutable and only collection removes them, so a closure
  that verified once stays verified; publication, checkout, and synchronization
  stop their walk at a remembered one instead of re-reading everything beneath
  it. `fsck` ignores the table, because reading the bytes back is exactly what
  an audit is for.
- **The ingest cache** records which content each imported file held, keyed by
  the device, inode, size, and both timestamps the walk observed. See
  [Imports](../../concepts/imports/).

Both are pruned by collection alongside the objects they refer to, and every
digest either one produces is confirmed against committed state before it is
used, so an entry that outlives its object costs a lookup rather than
returning a stale answer.

The local state engine uses a multi-process WAL. Casita serializes logical
writes while independent readers may proceed concurrently. The current
pre-release database layout is schema version 6, including object creation
generations for online snapshot retention. Existing
databases with any other `PRAGMA user_version` are rejected without mutation;
development-only schemas from before the first release must be recreated or
re-imported. A future schema change will require an explicit offline migration
rather than modifying a repository during ordinary open.

Every commit syncs the WAL before it is acknowledged (`synchronous = FULL`).
On Linux and Windows that sync reaches stable storage. On macOS it reaches the
drive's volatile cache, so a power loss may discard the most recent
acknowledged commits. Deletions that a commit allows (collection sweeps and
catalog reclamation) first flush the drive cache (`F_FULLFSYNC`), so a power
loss cannot keep those deletions while losing the commit behind them. The
repository reopens consistent, at an earlier revision. Casita flushes before
every deletion batch, rather than on every commit. WAL size and modification
time cannot identify commits that finished syncing after an earlier flush.

## Physical payload behavior

Payload identity is always BLAKE3 over complete plaintext bytes. The default
chunk store uses content-defined FastCDC boundaries targeting 256 KiB, bounded
at half and twice that average, then compresses chunks with Zstd.

These choices do not affect logical identity:

- equal plaintext payloads share the same `BlobId`;
- equal regions may share physical chunks across different payloads;
- changing chunking, compression, or backend does not change object keys; and
- every chunk is verified after decompression, while a complete sequential
  read verifies the whole payload at EOF.

Bao outboards are derived physical state. Their presence enables independent
verified range reads but is not required to retain or transfer a logical
object.

The default store admits at most 64 MiB of plaintext chunk work concurrently
across its clones. Transfer independently defaults to a 64 MiB in-flight byte
budget. Both are deployment controls rather than durable format parameters.

## Process coordination

Mutation sessions, retention holds, and transfers register durable scoped pins.
Readers and writers can continue during collection; their pins retain the
logical objects and physical data they use. Collectors take `gc.lock` so only
one collection or initialization operation runs across processes.

`collect()` waits for a competing collector, while `try_collect()` returns
`Busy`. Operating-system lock release handles collector crashes. Data pins,
prune fences, and deletion claims remain durable until their exact ownership is
resolved; elapsed time never makes an operation safe to forget.

Deleting or replacing `gc.lock` while Casita processes are running breaks
collector coordination.

Clauses C5 and C6 of the [reliability contract](../reliability/) state these
guarantees, where they apply, and which tests check them.

## Collection order

Collection marks named-root closures and active pin scopes in one immutable
snapshot. It then:

1. atomically commits a logical state containing only the marked records;
2. deletes unreferenced payload manifests; and
3. deletes unreferenced chunks.

Logical prune precedes physical deletion. A physical deletion failure may leak
space for a later run but does not invalidate reachable logical state.

The standard local profile knows that state and payloads share one filesystem.
If the logical prune fails specifically because storage is full, it may delete
only the already-marked stale physical set to create emergency headroom, reopen
the writer, and retry the prune. The pin ledger separately reserves bounded
bookkeeping space for ownership transitions while capacity is available.

Before a local `MutationSession` registers its staging pin, Casita samples disk
usage. At the default 80% threshold it attempts nonblocking collection. If
usage remains at or above 75%, it releases least recently used evictable roots
and vacuums after each release. Permanent roots remain. Completed pressure
passes have a 60-second cooldown shared through a repository stamp file.
`Busy` admits the mutation and leaves the attempt pending for a later session.
Callers may still run `gc`, invoke `collect()` directly, or schedule
`DiskPressurePolicy::probe_and_collect`. On a filesystem with nonzero reported
capacity, zero free bytes always triggers an attempt when the cooldown permits.

The [reliability contract](../reliability/) states what an interrupted
collection may leave behind (C9) and how collection behaves when storage is
full (C15).

## Deterministic publication crash tests

The library test suite kills a child process at recorded publication steps,
then reopens the repository and checks committed roots, payload bytes, and
subsequent collection. Run the matrix with:

```sh
devenv shell cargo test --all-features --lib blob::crash_tests -- --nocapture
```

The matrix covers immutable object writes, catalog publication and rebase,
SQL root changes, and process death before or after commit. It verifies that
visible roots have complete graphs, acknowledged data survives, and a
reopened repository can accept new work. It runs in the normal Linux, macOS,
and Windows library suites without an external server.

These tests model process death while the operating system remains alive.
They do not model power loss, torn sectors, or remote object-store
durability. Unix also tests directory flush boundaries.

## Backup and restore

There is currently no separate repository snapshot command. For a simple
filesystem backup:

1. stop or quiesce every process that can write, publish, retain, or collect;
2. copy the complete repository root, including the database and payload tree;
3. preserve filesystem metadata needed by the database and regular files; and
4. open the restored copy with Casita and run `fsck` before relying on it.

Copying only `casita.sqlite` loses payloads. Copying only `blobs/` loses the
logical keys, links, roots, and revision that make those bytes meaningful.
Ordinary copy tools do not participate in Casita's advisory locks, so do not
assume a live file-by-file copy is an atomic snapshot.

Treat an independently restored copy as a separate repository. Revisions are
opaque local state tokens and must not be used to order or equate later states
across the original and restored repositories.

## Operational commands

| Goal | Command |
|---|---|
| Inspect roots | `casita --repository PATH root ls` |
| Preview reclaimable data | `casita --repository PATH gc --dry-run` |
| Collect unreachable data | `casita --repository PATH gc` |
| Verify logical and physical integrity, and safely repair when possible | `casita --repository PATH fsck [--source REPLICA]` |

Do not use `gc` as a repair tool for reachable corruption. Preserve the
affected repository, inspect the `fsck` findings, and restore or re-import the
authoritative data as appropriate.
