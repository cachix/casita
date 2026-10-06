---
title: Errors and Integrity
description: Stable repository error categories, retry guidance, closure states, and fsck findings.
---

Casita keeps machine-readable classification separate from display text. Rust
callers should match typed variants or use category helpers rather than parse
`Display` messages. The [Library guide](/library/) covers built-in workflows.

## Repository error categories

`RepositoryError::category()` returns a non-exhaustive
`RepositoryErrorCategory`. `as_str()` provides the stable spelling used by the
CLI.

| Category | Stable string | Meaning |
|---|---|---|
| `Absent` | `absent` | A requested object, root, record, or payload is not present |
| `InvalidInput` | `invalid_input` | Caller input is malformed, foreign to this repository, or over a configured limit |
| `InvalidData` | `invalid_data` | Supplied or stored object bytes fail identity, canonical encoding, link, or relation verification |
| `ImmutableConflict` | `immutable_conflict` | One exact immutable key is already associated with a different record |
| `StaleRevision` | `stale_revision` | A compare-and-swap expected an obsolete repository revision |
| `DestinationConflict` | `destination_conflict` | A filesystem checkout destination is occupied or otherwise conflicts |
| `Busy` | `busy` | A nonblocking operation cannot acquire required ownership now |
| `Unsupported` | `unsupported` | A namespace, format, backend capability, or build feature is unavailable |
| `Corrupt` | `corrupt` | Committed state violates a repository invariant |
| `CollectedDuringRead` | `collected_during_read` | An unheld best-effort read raced collection of unrooted data |
| `Backend` | `backend` | I/O, storage, state-engine, or other operational infrastructure failed |
| `Cancelled` | `cancelled` | The caller cancelled the operation before it completed |

The enum is non-exhaustive. Include a fallback arm when matching it.

## Retry guidance

`RepositoryError::retry_disposition()` and `casita::experimental::Error::retry_disposition()`
return a non-exhaustive `RetryDisposition`:

| Disposition | Caller interpretation |
|---|---|
| `Never` | Repeating the unchanged request cannot fix the reported condition |
| `Retry` | Retry may succeed, normally with bounded exponential backoff and jitter |
| `RetryAfter(duration)` | Wait at least the supplied duration, then retry with normal bounds |
| `Unknown` | The backend did not provide enough typed information to decide |

`Busy`, stale revisions, typed payload or state-backend transient failures,
throttling, selected network I/O errors, and storage-full state may be
retryable. Invalid identities, immutable conflicts, malformed input, and
missing data normally are not. Cancellation is the caller's own decision and
is never retried automatically.

A retry disposition does not make a non-idempotent application operation safe
to repeat blindly. Observe the operation's commit result, root expectation, or
destination state before retrying work with external side effects.

## Closure status

`verify_closure()` returns one `ClosureStatus` for an exact snapshot:

| Status | Meaning |
|---|---|
| `Complete { objects }` | Every reachable record and payload exists and all intrinsic format relations pass |
| `Missing { from, missing }` | The requested object itself or the first canonical reachable boundary has no record |
| `Invalid { object, reason }` | An object's payload, identity, encoding, links, or direct relation failed verification |
| `Unsupported { object }` | The object's namespace has no registered verifier |

`Complete` reports how many distinct objects the traversal visited rather than
the set itself: a complete closure may be larger than the process verifying it,
so the traversal spills to local storage instead of keeping the set in memory.
Any count is meaningful only with the repository revision that produced it. A
later state may add a previously missing object or use a different format
registry.

Roots may be set only over `Complete` closures. Existing records can be
unrooted or temporarily incomplete while an import or transfer is staging, but
no successful root publication exposes such a graph.

## Integrity reports

`Repository::fsck()` inspects one logical snapshot protected by an online pin.
Collection can reclaim unrelated data during the scan. Admission returns
`Busy` if it conflicts with collection; retry after completion or recover an
interrupted collector first. `FsckReport` records the inspected revision and counts of
roots, objects, and unique payloads, followed by deterministic findings.

### Dispositions

| Disposition | Meaning | `is_healthy()` |
|---|---|---|
| `Corrupt` | Reachable state violates an invariant | `false` |
| `Collectible` | Valid unrooted logical or unreferenced physical residue may be collected | unchanged |
| `Unchecked` | Exact validation could not run because a verifier is unavailable | unchanged |

`FsckReport::is_healthy()` means no reachable corruption was found.
`FsckReport::is_clean()` is stricter: it requires no findings of any kind. A
repository containing only collectible residue is healthy but not clean. A
repository with an unchecked namespace can be reported healthy, but that does
not prove the unchecked object's format validity.

### Issue kinds

| Kind | Typical interpretation |
|---|---|
| `StateEncoding` | Primary state could not be decoded or enumerated |
| `MissingRecord` | A root target or stored forward link has no logical record |
| `MissingPayload` | A logical record names physically absent payload bytes |
| `InvalidObject` | Payload identity, canonical encoding, recorded links, or a direct relation failed |
| `UnsupportedNamespace` | No verifier is registered for the namespace |
| `UnrootedObject` | A valid logical record is unreachable from every named root |
| `UnreferencedPayload` | A physical payload is referenced by no logical record |
| `UnreferencedChunk` | A physical chunk is referenced by no present payload |

The last three unrooted/unreferenced conditions are normally collectible, not
reachable corruption.

## CLI behavior

The CLI prints runtime failures to stderr with the stable category supplied by
the repository, Casitar, or frontend error:

```text
error[invalid_data]: <human-readable detail>
```

Usage failures use `error: <detail>` and exit `2`. Runtime failures exit `1`;
the stable category string is not a distinct numeric exit code. Success exits
`0`.

`casita fsck` exits successfully when the report is healthy, even if it also
reports `Collectible` or `Unchecked` findings. It fails when at least one
`Corrupt` finding exists.

## Response guide

| Finding | First response |
|---|---|
| `busy` | Let the active mutation/read/collector finish, then retry with bounds |
| `cancelled` | Nothing to repair; repeat the operation without cancelling when its result is still wanted |
| `stale_revision` | Read a fresh snapshot and recompute the conditional mutation |
| `collected_during_read` | Repeat under a `RetentionHold`, or root the data before relying on it |
| `unsupported` / `Unchecked` | Open with a registry or build that contains the required verifier |
| `Collectible` findings | Preview and run collection if the residue is no longer needed |
| `InvalidObject`, `MissingPayload`, or another `Corrupt` finding | Preserve the repository, stop treating it as authoritative, and restore or re-import from a trusted source |
| `backend` | Inspect the underlying I/O/storage error and available capacity before applying typed retry guidance |

`fsck` does not rewrite reachable records or synthesize missing payloads. It
can rebuild derived physical state or replace a bad physical representation only
from an independently verified replica; it never changes logical records.
Collection removes unreachable residue and is not a substitute for restoring
corrupted reachable data.
