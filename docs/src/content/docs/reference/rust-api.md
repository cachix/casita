---
title: Rust API Reference
description: The supported Rust application API for built-in repository workflows.
---

Import request types and the `Importer` trait from `casita::import`; repository
handles and shared data types are exported directly from `casita`. `Repository` is a non-generic
handle: backend traits and protocol details do not appear in its signatures.
The `native` feature is enabled by default.

The supported surface contains repository handles, importer requests and reports,
and portable identity and filesystem types. Implementation modules remain private.

## Repository API

With `native`, the crate exports `Repository`, `Reader`, `VerifiedReader`,
`MetadataReader`, `RetainedReader`, `Error`, `ErrorKind`,
`CollectionReport`, `IntegrityReport`, `IntegrityIssue`, `IntegrityIssueKind`,
and `IntegrityDisposition`.

| Area | `Repository` methods |
|---|---|
| Open | `local`, `memory`; `s3` with the experimental S3 storage profile |
| Import | `import` with `BlobImport`, `CopyImport`, `FilesystemImport`, `TarImport`, `CasitarImport`, or `GitImport` / `GitClosureImport` (`git`) |
| Filesystem | `checkout` |
| Objects | `object`, `open`, `open_verified` |
| Consistent reads | `metadata_reader`, `retained_reader` |
| Application metadata | `get`, `scan`, `commit` |
| Names | `root`, `roots`, `set_root`, `compare_and_set_root`, `remove_root`, `root_retention`, `set_root_retention`, `set_root_with_retention`, `touch_root` |
| Archives | `export_casitar` |
| Maintenance | `preview_collection`, `collect`, `try_collect`, `vacuum`, `fsck`, `flush` |

`Importer<R>` is the sole public import contract for both repository handles.
It consumes an input request and returns that importer's associated `Report` and
`Error`. The repository's `import(input)` method delegates to this trait; there
are no public format-specific import methods. Built-in requests use application
`Error` with the standard handle and typed engine errors with experimental
compositions. Custom importers can implement the same trait.

`GitClosureImport::new(objects_dir, roots)` imports type-qualified native Git
roots directly from an object directory. It follows packs and alternates and
reuses complete stored subtrees across unrelated revisions and repositories.
It creates no named roots or serving-view inventory. The returned
`GitClosureImportOutcome` contains a `report` and a retained `reader`; keep the
reader alive until application roots have been published. A fully stored
selection does not access the source directory. A present blob is complete
on its own, so imports store a completeness record for a blob only when it is
a selected root. Completeness records are committed in bounded batches, and
only once the whole selection is proven complete; repeating an interrupted
import completes them. Report counters describe work performed and reuse
boundaries, not the size of the complete reachable graph.

`Reader` implements Tokio `AsyncRead` and `AsyncSeek`. It keeps the selected
object's content protected from collection until dropped. `VerifiedReader`
provides sequential reads that authenticate bytes before returning them.
Both expose their `ObjectRecord` through `record()`.

`metadata_reader` keeps one metadata revision stable but does not protect
payloads from collection. Use `retained_reader` when a root or metadata lookup
and the content read must share a protected snapshot. `get`, `scan`, and
`commit` operate on namespaced application metadata where the backend supports
it. `Error` exposes `kind()` and `retry_disposition()` and preserves the
standard error source chain.

`RetainedReader::object_batch` looks up several object records in the protected
snapshot in one call, preserving input order and duplicate keys.

`set_root` unconditionally creates or replaces a name after verifying the
complete target graph. `compare_and_set_root(name, expected, target)` publishes
only if the name is absent (`None`) or still points to the expected key
(`Some(&key)`). It returns `false` on mismatch and retries unrelated revision
conflicts internally. It compares current values, not root-change history, so a
delayed duplicate create can succeed after a removal; the
[multi-owner guide](/guides/s3-multi-owner/) shows a fenced transition built
from `commit`. `remove_root` likewise requires an exact expected target.
Local roots are permanent by default. `set_root_retention` marks an existing
root `RootRetention::Evictable` or `RootRetention::Permanent`;
`set_root_with_retention` sets a root and its policy atomically. Applications
using an evictable root can call `touch_root` after a successful read to update
its eviction order. The CLI's artifact checkout, restore, and run paths do this
automatically. The S3 profile does not support evictable root policy.
Filesystem and tar import requests also accept `with_retention`, which publishes
the imported root and its policy atomically.

`import(CopyImport::new(source, source_name, destination_name))` copies a complete graph
from one retained source snapshot and atomically creates or replaces the
destination root, including an occupied destination name.

Cloning a repository shares coordination and caches. Drop active readers before
collection or the final `flush()`, and await that flush before runtime shutdown.
It finishes payload writes, dropped-lease cleanup, and transient metadata
compaction. Local SQLite checkpoints report `ErrorKind::Busy` when a live
snapshot prevents truncation; release the snapshot and retry. Checkpointing
does not delete payload packs or catalog roots.

`CollectionReport` reports logical-object, physical-payload, and chunk counts.
`IntegrityReport` exposes inspected counts, a revision, and `IntegrityIssue`
findings categorized by `IntegrityDisposition` and `IntegrityIssueKind`.
`is_healthy()` checks for reachable corruption; `is_clean()` requires no
findings of any kind.

## Portable data model

These types remain public with `default-features = false`:

- `Digest`, `BlobId`, `DirectoryId`, `ObjectId`, and `DigestError`;
- `Directory`, `Node`, `DirectoryError`, and `DirectoryDecodeError`;
- `PathComponent`, `PathComponentError`, `SymlinkTarget`, and `SymlinkTargetError`;
- `NamespaceId`, `ObjectKey`, `ObjectRecord`, `RootName`, `RootRecord`,
  `RepositoryRevision`, and `RepositoryGeneration`;
- `NamespaceIdError`, `ObjectKeyError`, `RootNameError`, and
  `RepositoryRevisionError`; and
- `RetryDisposition`.

Physical chunk identities, backend implementations, format verifiers, and
wire framing are not application exports.

`ObjectRecord` and `RootRecord` expose read-only getters. Record construction
and binary encoding of keys and records use experimental free functions.
`ObjectKey` construction and text parsing/display remain supported, as do
`Directory` construction and encoding. The binary formats remain unchanged.

## Experimental boundary

Enable `experimental` and import from `casita::experimental` for the generic
`Repository<PS, SS>`, backend and format traits, sessions, detailed options,
archive framing, transfer protocols, Git services, Gix adapters, verified
range primitives, and backend conformance helpers. Dependency re-exports also
live there. These APIs may change between releases.

An existing custom composition migrates to:

```rust
# fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
use casita::experimental::{MemoryBlobStore, MemoryMetadataStore, Repository};

let repository = Repository::new(MemoryBlobStore::new(), MemoryMetadataStore::new()?);
# let _ = repository;
# Ok(())
# }
```

Both handles use the same storage engine and publication rules. The CLI
enables `experimental` for advanced commands. `fuzzing` adds parser adapters
solely for test harnesses.

See the [Library guide](../../library/) for supported workflows or the
[Experimental Rust API](../experimental-rust-api/) for advanced contracts.
`cargo doc --no-deps` shows the default public API; `--all-features` includes
the experimental namespace and optional integrations.
