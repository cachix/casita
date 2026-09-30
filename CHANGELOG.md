# Changelog

All notable user-visible changes to Casita are recorded here.

Casita is currently pre-release. Until the first tagged release, changes are
collected under **Unreleased**. This changelog describes product and API
changes.

The project follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and will use [Semantic Versioning](https://semver.org/) for tagged releases.

## [Unreleased]

### Added

- `RepositoryGeneration` orders the logical states of one repository.
  `MetadataReader::generation` and `RetainedReader::generation` report a
  reader's position in the commit order, so an application holding several
  readers can tell which one observes the newest state without comparing
  their unordered `RepositoryRevision`s.
- `experimental::RepositoryProfile` groups a repository's deployment policy
  (cross-process coordination, spill placement and limits, emergency
  collection, the import cache and disk-pressure maintenance) in one value.
  `RepositoryProfile::local(root)` is the policy `Repository::local` applies,
  so `Repository::new(payloads, metadata).with_profile(profile)` gives a
  composition of caller-owned backends the full local behaviour, which the
  individual `with_*` builders could not.
- Native Rust FSKit extension and rootless repository-backed macOS mounts,
  with packaging, activation, and permanent launch/read/cache-pressure benchmarks.
- Adaptive verified decoded-chunk reuse bounded to 2 MiB per reader and 32 MiB
  across the process, preserving byte verification and repository release gates.

- Multi-owner S3 conformance tests in which independent owners share one
  repository through opaque named roots, with writers, readers and collection
  in separate processes, and a guide documenting the supported composition,
  the current-value semantics of conditional roots with a fenced transition
  pattern, and recovery from killed readers and abandoned collectors.
- `Repository::try_collect` on the supported API collects only when a pass can
  start immediately and otherwise returns `Busy` with the retry disposition,
  so external schedulers never block behind another collector.
- `NarImport` and `FilesystemNarImport` bring Nix archives and native trees
  into a repository with locally measured NAR, flat, text, and Git hashes,
  reusable through `lookup_nar`, `ensure_nar`, and `scrub_nar`. Reference
  needles are 32-byte Nix base32 store-path hash parts, scanned across the
  whole archive exactly as Nix does; Git object hashing follows
  `nix hash path --mode git`, both through nix-archive.
- Sliced payloads over the SSH transfer: the receiver names the current and
  next closure of each root a transfer will replace, the serving process pairs
  the trees by path and indexes each counterpart when it is needed, and every
  payload arrives as copies of byte ranges the receiver already has plus
  compressed literal segments. A rebuilt blob that
  differs only in embedded store hashes costs a few kilobytes on the wire.
  Transfer progress reports the copied and literal bytes.
- SSH transfers pipeline requests on one connection and answer a discovery
  request with the records of the whole reachable closure plus the small
  payloads the receiver lacks, so a tree costs one round trip per answer
  bound instead of one per level.
- Local IPC importer selection for filesystem, tar, Casitar, and optional Git
  imports, with importer-specific options and results. Initialization advertises
  available importers; existing path/root filesystem requests remain supported.
- `casita holds PATH|s3://BUCKET/PREFIX [--json]` inspects durable holds without
  waiting for repository admission. Admission reports blockers periodically;
  informational traces record token ownership and release. Default CLI writer
  names include a random instance suffix, and CLI warnings are visible by default.

- Durable online pins for reads, staged writes, payload catalogs, and lazy
  metadata files on local and S3 repositories. Collectors serialize with each
  other while data pins allow ordinary work to continue. Cancellation retains
  pin and deletion ownership until storage I/O settles.
  Packed S3 collection publishes its catalog before deleting old representations;
  vacuum retries retired pack deletion after interruption.
  Integrity scans use snapshot pins and return `Busy` when collection prevents
  safe admission. Snapshot holds exclude unrelated objects published later.
- A generic verified object repository with immutable records, revisioned named
  roots, receiver-side verification, retention holds, and reachability-based
  collection.
- Canonical filesystem trees, raw and linked IPLD objects, native Git SHA-1 and
  SHA-256 objects, and immutable Git ref views.
- Local repository storage with chunking, compression, bounded traversal spill,
  integrity inspection, repair of rebuildable physical state, and disk-pressure
  collection.
- An opt-in corruption-aware near/far payload adapter with complete pre-read
  verification, single-flight replica repair, Bao outboard rebuilding, and
  near-tier collection coordination.
- Local, authenticated SSH, and experimental S3-compatible synchronization,
  including path-selected transfer from one stable source revision.
- Casitar v1 deterministic offline export, inspection, verification, and import.
- Native Git import, safe checkout, and experimental read-only fetch and smart
  HTTP features.
- A bounded `CasitaGixOdb` compatibility layer implementing Gix object reads,
  writes, headers, existence checks, explicit flushing, and atomic Git-view
  compare-and-swap publication over Casita storage.
- Reproducible benchmark suites for repository workflows, graph traversal,
  packed storage, S3-compatible transfer, native Git scale, and Gix object
  database compatibility, plus verified-read and replica-repair primitives.
- Structured runtime tracing across repository, state, payload, transfer,
  collection, repair, archive, Git, SSH, IPC, filesystem, contention, and spill
  paths. The CLI adds scoped `--log-filter`/`RUST_LOG` filtering and compact or
  JSON stderr output, while library builds install no subscriber.

### Changed

- On macOS, a durable local publication waits for the drive cache once instead
  of once per synced file and directory. Every sync but the last issues an I/O
  barrier (`F_BARRIERFSYNC`), so nothing can reach storage out of order, and
  the last flushes the drive (`F_FULLFSYNC`), persisting them all. Durable
  catalog objects publish about 2.5× faster, 64-object catalog batches about
  4×, collections that delete payloads about 1.5×, and pin-ledger slot
  exchanges 2×. Filesystems without barriers keep the full flush on every
  sync; Linux and Windows are unchanged.
- `casita archive create`, `experimental::Repository::export_casitar_file` and
  `export_casitar_file_with_policy`, and `casita init` sync the directory that
  receives their file before reporting success. They already synced the file,
  but not its new directory entry, so a power loss could lose an exported
  archive or a workspace marker (and with it access to the workspace's roots)
  after the command had succeeded. Checkout and IPC restores do not flush what
  they materialize, since the repository still holds it; the CLI and IPC
  references now say so.
- On macOS, repositories whose state is a `TursoMetadataStore`, including
  `Repository::local` and custom compositions, flush the drive cache
  (`F_FULLFSYNC`) before each deletion batch. Commits sync only to the drive's
  volatile cache there, so a power loss could previously keep a collection's
  deletions while losing the commit that allowed them, leaving roots that
  referenced deleted payloads.
  Commits themselves cost nothing more; collection pays per deletion batch.
- Publishing a packed catalog to an object store that lacks conditional
  updates now fails with `NotSupported` instead of silently overwriting the
  catalog pointer, which let concurrent writers lose each other's updates.
- SSH transfers pass `ConnectTimeout=30`, `ServerAliveInterval=15`, and
  `ServerAliveCountMax=3` to OpenSSH, so an unreachable or silent host fails
  the sync instead of blocking it indefinitely.
- Transfer has two entry points, `transfer` and `transfer_path`, each taking
  `TransferOptions`. `transfer_session`, `transfer_session_with_discovery`,
  `transfer_path_session` and `transfer_path_session_with_discovery` were
  removed; pass an open session as `&HeldSession(session)` and select the
  discovery policy with `TransferOptions::with_discovery`.
- The SSH transfer protocol has one version, `casita-ssh-source-v3`. Client
  and server no longer negotiate capabilities or fall back for v1 and v2
  peers, so both ends must speak the same protocol version.
- The local IPC service moved from the library into the `casita` CLI. The `ipc`
  Cargo feature and `casita::experimental::{serve_ipc, serve_ipc_with_options,
  IpcOptions}` were removed; run `casita ipc` from a `cli` build instead. The
  endpoint path and wire protocol are unchanged.
- The `casita::nar` module is private. Its application items, such as
  `NarRequirements`, `NarError`, `ensure_nar`, `lookup_nar`, `scrub_nar` and
  `prune_nar_associations`, are re-exported at the crate root.
- `casita-fs` now uses native FSKit by default on macOS 26+. `PersistentMount::new`
  takes repository and mount-parent paths; publication takes a `casita::Node`.
  Independent processes mount different repositories using one explicitly
  installed extension, with protocol checks and shared installation locks.
  The fuser/FUSE-T dependency, installer, features, and comparison
  backend are removed. Historical reports remain; current benchmarks use host
  filesystem controls. Distribution signing and fresh-host activation remain
  release gates.

- Remote catalog rebases now retain repository admission through publication or
  discard and finish without another user write. Shutdown draining waits for
  maintenance; S3 vacuum reclaims obsolete immutable catalog objects.

- Publication finishes catalog preparation, metadata commit, and finalization
  after caller cancellation; shutdown draining waits for that work. Cancelled
  catalog builds restore pending changes for retry. Custom repository backends
  must now own their dependencies (`'static`); `Clone` is not required.
- Online GC releases its durable exclusive hold after a rejected logical prune.
  It retains the recovery hold once physical collection begins.

- Repository construction now selects the publication strategy once and shares
  it across cloned handles. Generic constructors require `BlobStore` and
  `MetadataStore`; built-in repository APIs are unchanged.
- All public imports now use `Repository::import` and `Importer<R>` with
  `FilesystemImport`, `TarImport`, `CasitarImport`, or `GitImport`. The same
  requests support built-in and custom repository storage; filesystem imports
  also support existing mutation sessions. Format-specific repository methods
  are no longer public. Archive and Git application imports return their typed
  reports, and importer futures are now `Send` for service callers.

- Local and S3 repositories now publish the payload catalog in the same atomic
  metadata commit as records and roots. Local repositories reopen directly from
  SQLite without a separate catalog pointer. The local database layout is now
  schema version 3; older pre-release repositories must be recreated or
  re-imported, following the existing policy of no automatic schema migrations.
- Git fetch planning and pack generation now require the explicit experimental
  `git-fetch` Cargo feature. The default `native` feature no longer includes
  their pack-serving dependencies.
- Git fetch request parsing and validation are asynchronous so future
  disk-backed planning does not require a public API break.
- One payload batch now carries 16 MiB and 256 objects rather than 1 MiB and
  64, and a transfer round is sized to a batch, so a closure costs far fewer
  round trips: a rebuilt 349-path closure fell from 926 to 179 requests cold
  and from 433 to 133 on the rebuild, for the same bytes. A receiver also
  holds four discovery answers' worth of volunteered payloads instead of one,
  so payloads an answer already sent are no longer dropped and fetched again.
  What a discovery answer may volunteer keeps its own narrower bound, because
  those payloads are a guess the receiver has not asked for.
- A discovery answer volunteers payloads only when it can tell they are wanted:
  to a receiver that holds nothing, or to one syncing a tree rather than
  walking a closure. A receiver walking a closure runs a payload batch pipeline
  that asks its own store what it already holds, which the serving side cannot
  know, and it threw away 9 percent of what a rebuilt 349-path closure sent it.
  That closure's rebuild now costs 65.5 MB rather than 72.2 MB; a cold sync is
  unchanged.
- Packed reads no longer wait on the buffer budget they share with every other
  reader of a store. Read-ahead runs only while that budget has room, and a
  read a caller is waiting on takes whatever room is left, down to one chunk
  fetched uncharged. Several readers open at once over one store, which is how
  sliced payloads are served, could previously wedge each other: a reader that
  sits idle mid-blob holds the windows its read-ahead already fetched, and
  releases them only when it is polled again. Two new pack read counters,
  `readahead_deferrals` and `buffer_bypasses`, report how often the budget runs
  out.

### Security

- Filesystem import and checkout resolve descendants beneath already-open root
  handles and reject unsafe object and path shapes.
- Native Git SHA-1 verification uses a frozen collision-detection policy.
- Hostile-input fuzz targets cover logical records, frozen formats, Casitar,
  native Git parsers, and command input boundaries.

[Unreleased]: https://github.com/cachix/casita/commits/main
