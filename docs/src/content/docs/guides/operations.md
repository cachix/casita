---
title: Operate a Local Repository
description: Check roots and integrity, collect unused data, and back up a coherent repository.
---

Roots tell Casita which graphs to keep. Routine operation is to inspect those
roots, preview collection, and audit integrity before making changes.

## Routine checks

```console
$ casita --repository /var/lib/casita root ls
$ casita --repository /var/lib/casita gc --dry-run
$ casita --repository /var/lib/casita fsck --audit-only
```

`gc --dry-run` computes the collection plan without deleting data.
`fsck --audit-only` checks physical payloads, logical records, and reachable
closures without running the repair pass. It can report collectible residue
or an unavailable format verifier without classifying the repository as
corrupt. Read [Errors and Integrity](../../reference/errors/) for the report
categories.

## Collect unreachable data

```console
$ casita --repository /var/lib/casita gc
```

Collection removes unreachable records, then unreferenced payloads and chunks.
Active mutations and retained readers pin the data they need while unrelated
garbage remains collectible. The local profile can also try collection when a
mutation starts on a filesystem at least 80% full. It may then release roots
explicitly marked evictable, preserving permanent roots and data held by active
operations. Explicit capacity monitoring and collection remain useful because
busy work can defer that attempt.

## Back up and restore

Back up the repository directory as one unit:

1. Stop or quiesce every process that can write, retain, or collect.
2. Copy the complete directory, including `casita.sqlite` and `blobs`.
3. Restore that complete copy, then open it and run `fsck` before relying on it.

Ordinary file copy tools do not participate in Casita's locks. A live
file-by-file copy may combine incompatible state and payload revisions. Treat
the restored copy as a separate repository; its revision tokens cannot order
changes against the original.

## Inspect the database

Do not point SQLite tools such as `sqlite3` or DB Browser for SQLite at the
`casita.sqlite` of a running repository. Such a tool does not see Casita's
multi-process coordination, and when it closes it would fold what it read into
the database file and delete the write-ahead log Casita is still writing,
silently undoing later commits. On Linux and macOS Casita holds SQLite's own
locks against this, so the tool fails with `database is locked` or `locking
protocol`, and a tool that was already holding the database exclusively makes
Casita report the repository busy until it closes. Windows has no such
protection: commits made while the tool was open can be lost when it closes.
Casita detects the deleted log the next time it commits or deletes a payload,
and from then on refuses commits, deletions and reads with a
`restart_required` error until every process using the repository restarts.

To inspect the state, copy `casita.sqlite` together with `casita.sqlite-wal`
to another directory and open the copy. Take the copy with every Casita
process using the repository stopped, or from a consistent filesystem
snapshot: files copied one after the other while Casita commits may not form
a database SQLite can read.

## Respond to a failed integrity check

Preserve the affected repository before attempting recovery. A normal `fsck`
may rebuild a corrupt Bao outboard from verified local bytes. With an
independently verified local replica, `--source` can replace a missing or
corrupt physical representation:

```console
$ casita --repository /var/lib/casita fsck --dry-run
$ casita --repository /var/lib/casita fsck --source /srv/casita-replica
```

`--dry-run` previews physical repair actions without writing. Repair never
invents object records or changes roots. For reachable missing or invalid
content that cannot be repaired from a trusted replica, restore a coherent
backup or re-import and re-sync. For collectible residue, run `gc` and audit
again. For an `Unchecked` namespace, use a build with its format verifier.

Read [Local Repository](../../reference/local-repository/) for layout and
[Garbage Collection](../../concepts/garbage-collection/) for collection order.
