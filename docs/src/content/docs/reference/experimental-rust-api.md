---
title: Experimental Rust API
description: Custom backends, formats, sessions, transfer sources, and limits.
---

Enable `experimental` to compose Casita's repository engine from your own
payload store (`PS`), metadata store (`SS`), and immutable
`FormatRegistry`. Import these types from `casita::experimental`. This API
may change between revisions. For built-in workflows, use the
[supported Rust API](../rust-api/) and [library guide](../../library/).

| Need | Section |
| --- | --- |
| Open or compose a repository | [Constructors](#constructors) |
| Stage verified objects and publish roots | [Publication](#publication) |
| Retain data while reading | [Stable reads and retention](#stable-reads-and-retention) |
| Implement storage or format behavior | [Backend traits](#backend-traits) |
| Set bounds and classify failures | [Limits](#limits), [Errors](#errors) |

Import requests and `Importer` live in `casita::import` for both APIs.
`MultiRootFilesystemImport` and `UnrootedFilesystemImport` are available
there with `experimental`.

Native operations need a Tokio runtime with timers; custom runtime builders
should call `enable_all()`. Drop sessions and readers, then await
`flush_repository_leases()` before runtime shutdown to finish durable pin
releases.

Build item-level Rustdoc with:

```console
$ cargo doc --all-features --no-deps --open
```

## Constructors

| Constructor | Result |
|---|---|
| `Repository::memory()` | Ephemeral `MemoryBlobStore` plus `MemoryMetadataStore` with built-in formats |
| `Repository::local(path).await` | Standard persistent `ChunkedBlobStore` plus `TursoMetadataStore`, including cross-process coordination |
| `Repository::new(payloads, state)` | Caller-supplied backends, built-in formats, and default deployment limits |
| `Repository::with_formats(payloads, state, formats, limits)` | Caller-supplied backends, exact immutable registry, and explicit `FormatLimits` |

`Repository::with_fs_coordination(path)` adds cross-process mutation/read
versus collection ownership to a custom composition sharing one local root.
Only use it when every process opening those backends agrees on that root.

### Deployment profiles

`RepositoryProfile` holds the deployment policy a repository applies on top of
its backends. `Repository::with_profile(profile)` replaces a handle's profile;
existing clones keep theirs.

| Profile | Behaviour |
|---|---|
| `RepositoryProfile::generic()` | What `Repository::new` starts from: no cross-process coordination, spills in the platform temporary directory, no import cache, no implicit maintenance |
| `RepositoryProfile::local(root).await` | What `Repository::local` applies: collector locks and spill files under `root`, emergency collection when storage is full, and a disk-pressure collection before a handle's first mutation. Opening reserves the lock file and removes stale spill files |

`with_fs_coordination(root)`, `with_spill_limits(limits)` and
`with_ingest_cache(&turso_metadata)` adjust a profile before it is applied. The
ingest cache lets a re-import skip files whose identity is unchanged; it must
belong to the same `TursoMetadataStore` the repository uses. The local profile
assumes payloads and state share the filesystem holding `root`:

```rust
# async fn example(root: std::path::PathBuf) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
# use casita::experimental::{ChunkedBlobStore, Repository, RepositoryProfile, TursoMetadataStore};
std::fs::create_dir_all(root.join("blobs"))?;
let payloads = ChunkedBlobStore::local_packed(root.join("blobs")).await?;
let metadata = TursoMetadataStore::open(root.join("casita.sqlite")).await?;
let profile = RepositoryProfile::local(&root).await?.with_ingest_cache(&metadata);
let repository = Repository::new(payloads, metadata).with_profile(profile);
# let _ = repository;
# Ok(())
# }
```

`Repository::local` additionally takes the collector lock while it opens and
migrates the payload catalog, and reclaims abandoned metadata before
returning; prefer it when its fixed layout suits the deployment.

## Core logical types

Catalog construction and binary encoding are free functions in this namespace:

| Value | Construct | Encode / decode |
|---|---|---|
| `ObjectKey` | Public identity constructors | `encode_object_key` / `decode_object_key` |
| `RootRecord` | `new_root_record` | `encode_root_record` / `decode_root_record` |
| `ObjectRecord` | `new_object_record` | `encode_object_record` / `decode_object_record` |

`new_object_record` validates link ordering but does not verify the payload or
graph. `LogicalEncodingError` and `ObjectRecordError` also live here. The frozen
binary layouts are unchanged; enabling `experimental` does not add construction
or encoding methods to the supported record types.

| Type | Meaning |
|---|---|
| `ObjectKey` | Versioned namespace plus canonical format-native identifier |
| `ObjectRecord` | Immutable key, BLAKE3 payload ID, payload length, and canonical forward links |
| `RootName` / `RootRecord` | Durable name selecting one exact object and retaining its complete closure |
| `RepositoryRevision` | Opaque token for one logical state; compare for equality only |
| `RepositoryGeneration` | Position of a logical state in its repository's commit order; ordered within one repository |
| `ClosureStatus` | `Complete`, `Missing`, `Invalid`, or `Unsupported` result for one snapshot traversal |
| `Digest`, `BlobId`, `DirectoryId`, `ChunkId` | Raw and capability-typed BLAKE3 identities |
| `Directory`, `Node`, `PathComponent`, `SymlinkTarget` | Canonical filesystem data model |

See [Identifiers](../identifiers/) for their exact textual and validation
rules.

## Publication

Call `Repository::mutation_session().await` to acquire a `MutationSession`. The
standard local profile first attempts pressure-driven collection at 80% used
capacity, then the session registers a staging pin before payload writes begin. Generic repository compositions skip the admission policy.

The main staging methods are:

- `stage_blob` and `stage_blob_reader` for raw payloads;
- `stage_object` for a caller-selected generic key and payload;
- `stage_directory` for canonical directory data; and
- `stage_existing` when the payload already exists in the same store.

Staging returns `StagedObject`, a sealed value tied to that exact repository
instance. Namespace verification has already reproduced its identity and
links. An arbitrary `ObjectRecord` is not accepted as a publication substitute.

Publication choices include:

- `publish_unrooted` for verified records only;
- `publish_closures` for records plus bounded, checked closure targets without named roots;
- `publish_rooted` for records plus one root;
- `publish` for records plus a batch of `RootChange` values;
- `publish_at_revision` for an exact compare-and-swap; and
- `publish_if_roots_match` for linearizable compare-and-publish against exact
  current root values.

Records and root changes commit atomically. A root is published only after its
resulting closure is complete and valid. Unrelated revision races can be
retried; an observed root mismatch is returned as
`ConditionalPublishResult::RootMismatch` without overwriting the changed name.

`publish_closures` verifies staged or existing targets with normal format and
link checks before atomically publishing records and requested witnesses.
Staged-object and target counts are each limited by `max_batch_objects`.
Existing witnesses may be reused; this is a completeness check, not a fresh
corruption audit. Only the targets gain witnesses, and a built-in raw blob
needs none because its record proves its own closure. The mutation retains
the checked graphs for its lifetime, but their witnesses do not become
permanent roots.

## Stable reads and retention

`Repository::retention_hold().await` returns a borrowed `RetentionHold` over
one immutable `MetadataSnapshot`. `owned_retention_hold()` provides the owned
variant for services that need a `'static` lifetime.

A hold provides snapshot object lookups, payload opens, and closure
verification while preventing collection from removing physical data visible
to that snapshot. Collection can reclaim unrelated data while the hold remains
live. Dropping the hold makes its otherwise unreferenced data collectible.

Repository helpers such as filesystem checkout and transfer source sessions
take the required hold internally.

## Repository workflows

| Workflow | Primary API | Extra capability |
|---|---|---|
| Raw blob import | `Repository::import(BlobImport::new(reader, root))` or `MutationSession::import(BlobImport::new(reader, root))` | `PS: BlobStore`, `SS: MetadataStore`, `native` |
| Filesystem import and checkout | `Repository::import(FilesystemImport::new(...))`, `FilesystemImport::new(...).reread(true)`, `Repository::checkout` | `PS: BlobStore`, `SS: MetadataStore`, `native` |
| Tar stream import | `Repository::import(TarImport::new(...))` | `PS: BlobStore`, `SS: MetadataStore`, `native` |
| Closure validation | `Repository::verify_closure` or a retention hold | `PS: BlobStore`, `SS: MetadataStore` |
| Logical collection | `preview_logical_collection`, `collect_logical`, `try_collect_logical` | `SS: MetadataStore` |
| Collection preview and execution | `preview_collection`, `collect`, `try_collect` | `PS: BlobGc` |
| Integrity inspection | `Repository::fsck` | `PS: BlobGc` |
| Physical fsck repair preflight | `Repository::fsck_repair`, `Repository::preview_fsck_repair` | `ChunkedBlobStore` |
| Generic transfer | `transfer` | Destination repository plus a `TransferSource` |
| Named graph import | `Repository::import(CopyImport::from_source(source, source_name, destination_name))` | Custom `TransferSource`; built-in sources use `CopyImport::new` |
| Path-selected transfer | `transfer_path` | Named filesystem root plus a `TransferSource` |
| Git view publish/read/checkout | `publish_git_view`, `read_git_view`, `checkout_git_tree` | `native` |
| Native local Git import | `Repository::import(GitImport::new(...))` | `git` |
| Git fetch service | `GitFetchService` | `git-fetch`; HTTP adapter requires `git-http` |
| SSH source | `SshTransferSource` | `ssh` |
| Casitar stream I/O, export, and import | `CasitarReader`, `CasitarWriter`, `Repository::export_casitar`, `Repository::import(CasitarImport::new(...))` | `native` |

## Backend traits

### Payload storage

`BlobStore` is the minimum physical interface: presence checks, seekable reads,
streaming writes, and optional chunk metadata. Convenience methods provide
whole-slice writes and reads.

Every implementation states its publication contract through the required
`publication()` method. `PayloadPublication::Immediate` means each closed
writer is durable. `PayloadPublication::Cataloged` points at a
`CatalogPublication` implementation, which seals staged writes and coordinates
the physical catalog with logical commits. An adapter forwards the capability
of the store that receives its writes; there is no default to inherit.

`BlobGc: BlobStore` adds enumeration and deletion of payloads and chunks. It is
required for physical collection and `fsck` because those operations compare
logical reachability with physical coverage. Logical-only collection requires
only `MetadataStore`; it prunes records without making a payload-liveness claim.
Use it when a service shares physical blobs across repositories or tenants.
Its `*_pinned` deletion methods have no defaults. Each store states how it
honours online pins, and a wrapper forwards them together with any behavior it
adds, so collection cannot silently bypass the pin ledger.

`BlobSync` is an optional destination capability exposed through
`BlobStore::as_blob_sync`. A source needs only `BlobChunkSource` and its chunk
map for transfer to negotiate missing chunks. Otherwise the same logical
operation streams and re-verifies complete plaintext payloads.

Built-in implementations include `MemoryBlobStore`, `ChunkedBlobStore`,
`CombinedBlobStore`, and `RepairingBlobStore`.

#### Near/far payload composition

`CombinedBlobStore::new(near, far)` is a public `BlobStore` implementation for
tiered deployments. Reads and `has` checks prefer near and fall back to far;
writes are opened only in near. A far read is not copied into near.

```rust
use casita::experimental::{BlobStore, CombinedBlobStore, MemoryBlobStore};

async fn read_from_far() -> Result<(), casita::experimental::Error> {
    let near = MemoryBlobStore::new();
    let far = MemoryBlobStore::new();
    let existing = far.put_slice(b"upstream").await?;
    let payloads = CombinedBlobStore::new(near.clone(), far);

    assert_eq!(
        payloads.read_to_vec(&existing).await?,
        Some(b"upstream".to_vec()),
    );
    assert!(!near.has(&existing).await?); // no implicit cache warming
    Ok(())
}
```

The far type still satisfies `BlobStore`, but the adapter never opens a writer
on it. `CombinedBlobStore` intentionally implements neither `BlobGc` nor a
combined `BlobSync`: it cannot establish global liveness for a shared far tier,
and it falls back to verified whole-blob transfer. Applications that own those
cross-tier policies can provide a custom backend implementing the additional
capabilities.

`RepairingBlobStore::new(near, far)` is the opt-in corruption-aware composition
for two `ChunkedBlobStore` values. It performs a complete validation before a
reader can expose bytes. Typed near corruption or a missing referenced chunk
causes one per-blob repair flight: the far payload is verified in full, its
compressed chunks are independently checked while copied, and the near
manifest is atomically replaced only after complete identity verification.
Other backend failures are returned unchanged. `BlobRepairError` retains both
near and repair-source diagnostics when recovery fails.

`RepairingBlobStore::verified_read` applies the same policy to Bao ranges and
rebuilds derived outboards from verified near bytes. Its `BlobGc` capability
owns only the near tier and excludes deletion while a local reader, writer, or
repair is active; the far tier requires independent lifetime coordination.

### Repository metadata

`MetadataStore` has two foundational operations:

- `snapshot()` returns an immutable, internally consistent `MetadataSnapshot`;
- `commit(expected_revision, mutation)` atomically compares and applies one
  `MetadataMutation`.

`MetadataSnapshot` exposes its revision plus lazy object, root, and complete
enumeration methods. A metadata backend is trusted infrastructure. Applications
should normally mutate it through `Repository`, which verifies objects and root
closures before constructing metadata mutations. `repository.metadata()`
accesses the backend; `repository.payloads()` accesses the separate blob store.

Built-in implementations are `MemoryMetadataStore`, `TursoMetadataStore`, and,
with `s3`, `Wal3MetadataStore`. Custom backends must provide a durable `PinStore`,
`MetadataSnapshot::generation()` and `objects_created_through(generation)` for
online retention. A successful commit advances the generation atomically;
idempotent object inserts preserve their creation generation. Unsupported
generation methods fail closed instead of retaining future garbage indefinitely.
Lazy snapshots also report immutable files through `retention_resources()`.

`DataPinLease` protects scoped reads and staged writes while collection proceeds.
`RepositoryLease` is collector-only ownership returned by `try_collection_lease()`.
Both `try_collection_lease()` and `coordinates_payload_catalog()` are required:
a store shared between processes without a common lock returns a durable
lease, while local and in-memory stores return `RepositoryLease::process_local()`.
Wrappers forward both, so they cannot silently drop a shared store's collector
exclusion or its payload catalog.

For an already-open repository, `recover_collection(&collector_token)` retries
an interrupted physical collection using the exact token in its pin inventory.
The caller must first establish that the collector and all requests covered by
its claims have stopped. Recovery preserves live pins and keeps interrupted
claims and prune fences through marking and commit. S3 operational ownership
must be recovered separately before this call. A stale token cannot take over
a newer collector. This API does not expire or release abandoned reader pins.
For the standard S3 profile, `Repository::recover_s3_collection(bucket, prefix,
writer, &collector_token)` also performs the reopen under collector ownership,
including when an abandoned prune fence blocks ordinary read admission.

### Read-only chunk sources

`BlobChunkSource::get_chunk` reads one compressed chunk. It has no write,
metadata, or collection operations. `BlobSync` extends it with destination
presence checks and verified chunk/manifest writes.

`TransferReadSession::as_chunk_source()` exposes source reads; a destination
continues to expose `BlobStore::as_blob_sync()`. A writable blob backend also
gets `BlobStore::as_chunk_source()` by default. Transfer still copies only
missing chunks and verifies their contents and complete payload identity before
publishing records or moving roots. The session must protect the actual blob
storage for the full operation; a read interface alone does not establish that
retention agreement.

This prepares the API for independent blob transports. Session-bound S3
descriptors and direct-export retention are still proposed work.

Custom chunk backends implement `BlobChunkSource` separately from `BlobSync`.
Import `BlobChunkSource` for chunk read method calls on concrete chunk stores.

### Object formats

Implement `ObjectFormat` to add one deterministic namespace. `verify` consumes
a `VerificationContext`, checks the format's native identity and canonical
payload, and returns a sealed `VerifiedObject` with exact forward links.

Override `verify_links` when validity depends on an intrinsic relation to
direct children, such as checksum evidence comparing a linked archive. The
`DirectLinkView` exposes only the declared direct targets. Verification must be
deterministic and must not depend on network or mutable policy.

Register `Arc<dyn ObjectFormat>` values with `FormatRegistry::new`; duplicate
namespace ownership is rejected.

### Transfer sources

`TransferSource::begin_transfer` opens a stable `TransferReadSession` bound to
one revision and physical retention lifetime. The session exposes transfer-
shaped record, root, payload, and optional chunk reads rather than leaking the
source's backend types. `path_proof` is an optional optimization: capable
sessions return a retained-revision proof in one operation, while the default
reports `PathProofResponse::Unsupported` and preserves the ordinary spine walk.

Both a local `Repository` and `SshTransferSource` implement this boundary. The
destination never trusts sender-supplied links or identities: it reruns its own
format verifier before publication.

`transfer_path` resolves one path beneath a named filesystem root. It verifies
the directory spine without publishing those ancestors, transfers only the
selected file or directory closure, and optionally installs that closure under
an explicit destination-owned root. Both functions take `TransferOptions`, which
selects the discovery policy. To transfer through an already-open stable
session, for example after resolving roots on it, pass it as
`&HeldSession(session)`: the held session keeps its revision and retention.

## Limits

`FormatLimits` configures hostile-input and deployment bounds. Defaults are:

| Field | Default |
|---|---:|
| `max_payload_bytes` | `u64::MAX` |
| `max_metadata_bytes` | 256 MiB |
| `max_links_per_object` | 1,000,000 |
| `max_directory_entries` | 1,000,000 |
| `max_batch_objects` | 4,096 |
| `max_root_changes` | 1,024 |
| `max_traversal_objects` | 10,000,000 |
| `read_buffer_bytes` | 64 KiB |
| `max_transfer_in_flight_bytes` | 64 MiB |

These are active repository limits, not durable identity parameters. Lowering
a limit can make an otherwise valid large object unavailable to a deployment
without changing that object's key.

## Errors

Repository workflows return `RepositoryError`; physical and filesystem
operations use `casita::experimental::Error`; transfer has `TransferError`. Do not classify
failures from display strings. Use `RepositoryError::category()` and
`retry_disposition()` where orchestration needs stable behavior. See
[Errors and Integrity](../errors/).
