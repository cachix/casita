---
title: Maintain an S3 Repository
description: Inspect holds, shut down cleanly, and recover interrupted collection.
---

Every runner sharing an S3 repository must use the same bucket and prefix and
support its WAL3 admission protocol. Stop older binaries before upgrading
because they do not observe the current holds. The S3 profile requires the
`s3` feature. Recovery APIs below are in `casita::experimental`.

## Routine checks

```sh
casita holds s3://my-bucket/repository
casita holds s3://my-bucket/repository --json
```

`casita holds` reads collector ownership and the state and coordination pin
ledgers without waiting for repository admission. It uses standard AWS
credentials. Each ledger is a separate snapshot, so use exact tokens rather
than matching entries by writer name or time. Inspection never releases a
hold. A listed token does not prove its owner has stopped.

For shared root ownership, see [Share an S3 Repository Across Owners](../s3-multi-owner/).

## Collection and reads

Mutation sessions and retained reads create durable pins. They protect
staging data, selected payloads, snapshots, and metadata files while collection
runs. Keep a retention hold until its payload stream closes. A WAL3 cursor
alone does not protect physical payloads.

Only one collector runs at a time. `collect` waits for ownership;
`try_collect`, `try_collect_logical`, and `try_vacuum` return `Busy`
when another collector or a conflicting pin update prevents a pass. Holds do
not expire. An abandoned collector token blocks later `collect` calls until
it is recovered.

Collection first prunes unreachable logical records, then updates the payload
catalog and deletes retired packs and manifests. An interrupted pass may leave
physical garbage; `vacuum` reclaims deferred packs and obsolete catalog
files. Rooted data and active pins remain protected.

## Clean shutdown

For a library application, stop new work, await running operations, drop
sessions and readers, then await
`casita::experimental::flush_repository_leases()` before stopping Tokio.
Dropping a session schedules its durable release; the final flush waits for
those releases and background catalog work. A failed release reports an error
and leaves the durable pin in place. The CLI drains releases on exit.

## When a command is blocked

Run `casita holds s3://BUCKET/PREFIX --json` to see every token, pin scope,
released history, prune fence, and deletion claim. Admission warnings show a
short list of blocking tokens by default. For acquisition and release events,
use `--log-filter casita=info`; `casita=debug` adds pin events.

Writer names are diagnostic. The CLI uses `CASITA_WRITER` when set, otherwise
a process ID and random suffix. Even an explicit name can be reused. Correlate
the exact token with process logs before deciding it is abandoned.

## Recover an abandoned hold

Holds have no TTL. A paused process can still resume and read or publish data,
so elapsed time is never sufficient evidence that its hold is abandoned. The
[reliability contract](../../reference/reliability/) states this as clause C6,
with the tests that check it and its current limits.

1. Terminate the owning runner and prevent it from resuming. For an interrupted
   exclusive collector, stop every runner and allow outstanding backend requests
   to settle before recovery.
2. Run `casita holds s3://BUCKET/PREFIX --json` to inspect operational tokens and
   both pin ledgers. For interrupted payload collection, record the exact
   `state.collector` token as well as the operational collector token. The two
   tokens identify different ownership records.
3. Call `release_abandoned_repository_hold(&hold.token)` for the exact abandoned
   token. Reusing a diagnostic writer name does not grant ownership; replaying
   recovery for an old token cannot remove a newer token.
4. For interrupted payload collection, call
   `Repository::recover_s3_collection(bucket, prefix, writer, &collector_token)`.
   It obtains collector ownership before opening payload discovery and can
   therefore reopen through an abandoned logical-prune fence. It preserves live
   pins and inherited deletion claims throughout marking and metadata commit,
   then finishes the sweep and catalog publication. For an already-open generic
   repository, use `recover_collection(&collector_token)`.
5. Drain releases, check the remaining inventories, and run an integrity audit
   before resuming traffic. If a recovery attempt fails, inspect again: its new
   collector token may have replaced the abandoned token while preserving the
   original fence and claims. Never substitute an older token for the current one.

The operational collector token and `state.collector` are different records.
Releasing the operational hold does not clear a prune fence or deletion claim.

For other abandoned records, first establish that the owner and its outstanding
requests have stopped:

| Record | Recovery API |
| --- | --- |
| Reader or writer pin | `release(&exact_pin_token)` on `state.pin_store()` or `state.repository_coordination_pin_store()`, according to its ledger |
| State WAL shard claims | `state.recover_wal_deletions(exact_claim_tokens)` |
| Coordination WAL claims | `state.recover_repository_coordination_deletions(exact_claim_tokens)` |

WAL claim recovery needs the complete current token set for that ledger. It
verifies that claimed paths are unreferenced and retains claims through
retries. Pin release clears only the named pin. Reinspect after a failure
before retrying because the current tokens may have changed.

## Maintain the WALs

`Wal3MetadataStore::collect_wal(reader_grace_period)` collects obsolete
state-log shards. `collect_repository_coordination(reader_grace_period)`
compacts the separate admission log without expiring holds. Both acquire
collector ownership and can run alongside ordinary readers and writers.
Choose a grace period longer than the longest manifest-to-fragment metadata
read. Pins protect files in use; unsettled deletion claims prevent path reuse
until recovery. A failed collection retains its token for recovery.

Unlike payload collection, WAL collection relies on the grace period for
safety: nothing verifies that readers are gone before it deletes. The
[reliability contract](../../reference/reliability/) tracks this as a known
limit of clause C5.
