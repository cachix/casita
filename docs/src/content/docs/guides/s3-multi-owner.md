---
title: Share an S3 Repository Across Owners
description: Use separate root names and one collector for several applications.
---

Several applications can share one S3 repository and deduplicate identical
content. Give each owner a separate root prefix, such as `owner-a/current`
and `owner-b/releases/42`. Casita retains the union of their named graphs
and active reads; one collector removes data nobody needs.

Root ownership, permissions, scheduling, and peer discovery are application
decisions. This guide uses the supported `casita::Repository` API except
where it explicitly names an experimental type. The
[`s3_multi_owner` integration tests](/library/#test-remote-application-workflows)
exercise separate writer, reader, and collector processes.

## The composition

Every process opens `Repository::s3(bucket, prefix, writer)` with the same
bucket and prefix. The writer name is diagnostic; random durable tokens own
pins and collector passes. Casita treats root prefixes as opaque, so the
application must control every writer for each owner's prefix.

Owners may point to the same object key or share chunks through different
graphs. Removing one root leaves content retained by another root or active
pin. Any process may collect, but only one collector runs at a time.

## What retention is and is not

Named roots retain their complete graphs. Online pins protect active work:

| Read API | Retention scope |
| --- | --- |
| `retained_reader` | The whole snapshot at its generation, including data from other owners. It can still resolve names removed later. |
| `RetainedReader::retain_objects` | The same objects as its reader, without the reader's metadata snapshot. |
| `open`, `open_verified` | Only the selected object's closure. Unrelated data stays collectible. |

Pins survive root removal and dropping the repository handle. Process death
does not expire them. A long lived retained reader may delay reclamation of
older garbage from other owners; use closure-scoped opens for long reads when
possible.

Retention does not grant access control. Any client with credentials for the
shared prefix can read its objects. Use separate repositories or bucket policy
when owners need isolation.

## Conditional roots compare current values

`compare_and_set_root`, `remove_root`, and `commit` checks compare
current targets, not a name's history. After removing a name, a delayed
create that expects absence could set its old target again.

To reject that replay, advance an owner-scoped fence root in the same commit
as each shared-name change. Check the fence value observed before the change:

```rust
# async fn example(repository: &casita::Repository, fence: casita::RootName, shared: casita::RootName, epoch_0: casita::ObjectKey, epoch_1: casita::ObjectKey, key: casita::ObjectKey) -> Result<(), casita::Error> {
use casita::{MetadataChange, MetadataCheck, MetadataCommitResult};

let result = repository
    .commit(
        vec![
            MetadataCheck::Root { name: fence.clone(), expected: Some(epoch_0.clone()) },
            MetadataCheck::Root { name: shared.clone(), expected: None },
        ],
        vec![
            MetadataChange::SetRoot { name: shared.clone(), target: key.clone() },
            MetadataChange::SetRoot { name: fence.clone(), target: epoch_1.clone() },
        ],
    )
    .await?;
assert!(matches!(result, MetadataCommitResult::Committed { .. }));
# Ok(())
# }
```

A later removal checks `epoch_1` and advances to `epoch_2`. A delayed
duplicate create still expects `epoch_0`, so it conflicts even if the
shared name is absent again. Put the fence check first: conflicts report the
first failing check in caller order. Fence values are ordinary immutable
objects. The application decides its claim IDs and replay policy.

## Publication, collection and retries

Collection marks one repository revision. Overlapping publication can return
`StaleRevision`; a racing pin update can return `Busy`. Both are retryable
without a logical prune. Schedule collection when publications leave a
quiet window. `collect` waits for collector ownership; schedulers can use
`try_collect` to receive `Busy` instead.

An integrity audit may report collectible unrooted objects. Use
`IntegrityReport::is_healthy` to distinguish those findings from reachable
corruption; require `is_clean` after a completed collection.

## Process failures and recovery

An interrupted reader can leave a durable pin, and an interrupted collector
can retain its ownership token and deletion claims. They do not expire. Once
the owner and its outstanding requests have stopped, recover by exact token.
A release for one owner's pin cannot remove another owner's protection.

Follow [S3 maintenance](../s3-maintenance/#recover-an-abandoned-hold) for
the complete recovery procedure. These inspection and recovery APIs use
`casita::experimental`.

## Selected root readers

A `retained_reader` protects the entire snapshot, so keep it short in a
shared repository. Resolving a root and then calling a closure-scoped
`open` takes two steps. If the root is removed and collected between them,
`open` returns `None`; retry the lookup. The experimental
`TransferSelection::Selected` can resolve and pin selected root closures
for a transfer session.
