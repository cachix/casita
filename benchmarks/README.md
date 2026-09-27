# Benchmark suite

## Filesystem reuse

`benchmark run filesystem-reuse` separates cached tree import from forced rereads
and profiles directory staging and pin-journal work. See the
[commands, correctness gates, and timing interpretation](filesystem-reuse.md).

## Remote pins and maintenance publication

The [pin protocol investigation](pin-ledger-protocol.md) traces shared-ledger
costs, existing coalescing, a writer-owned collection barrier, RustFS lock
timeouts and checkpoint retry classification. Four permanent entrypoints run
in `benchmark all`:

* `remote-pin-cost`: actual optimistic pin edits and wire-codec request/byte
  accounting at 1/10/32/64/100 active pins; no network or process scaling claim.
* `pin-protocol`: independent 1/10/32/64/100-process **protocol model**, with
  concurrent GC, killed writers, cancellation, lost write responses and fresh
  process readback. JSON/file-CAS results are not Casita/S3 throughput.
* `pin-protocol-s3`: the same three protocol prototypes on real RustFS GET,
  conditional PUT, LIST and DELETE, with fresh process readback after GC.
  These use JSON records rather than Casita's production WAL/catalog format.
* `pin-http`: real raw S3 disjoint/hot-key/CAS controls, per-request HTTP status
  and latency, plus a 2-second RustFS lock-timeout control in `benchmark all`.
  Defaults use isolated RustFS; production requires an explicit test location.

`pin-protocol-s3` retains every attempted case and audits durable roots after
failed cases. The aggregate completion ledger records expected, recorded,
failed and unaudited case counts. Known liveness failures still make
`benchmark all` exit nonzero; a passing readback audit does not turn them into
passing performance samples.

```sh
benchmark run remote-pin-cost
benchmark run pin-protocol --profile smoke --output /tmp/pin-protocol.json
benchmark run pin-protocol-s3 --profile smoke --output /tmp/pin-protocol-s3.json
benchmark run pin-http --profile smoke --output /tmp/pin-http.json
```

The [retained evidence](reports/2026-09-20-pin-protocol/README.md) distinguishes
production-code counts, protocol-model results and backend HTTP controls.
The [implementation follow-up](reports/2026-09-20-pin-batching-retries/README.md)
adds bounded typed checkpoint retries and combines known loose-chunk protection.
`small-blob-pins` permanently compares loose and packed staging around the
chunker minimum and maximum, with fresh-store readback and duplicate/release gates.

## Transfer hold scope

`benchmark run transfer-holds` compares whole-snapshot and selected named-root
acquisition in the same binary, using persistent local repositories and either
direct calls or the real SSH stdio transfer protocol over a bounded in-process
duplex connection. It measures session acquisition and end-to-end copy time
separately for 4 KiB and 4 MiB incompressible payloads. Each fixture also contains
16 unrelated, unrooted 256 KiB blobs. Repository construction and staging are
outside timing; copies use fresh destinations, with no deduplication shortcut.

GC cases pause a payload stream after its first byte, collect while the session
and reader remain live, verify the rest, then time a copy. Non-GC cases avoid that
extra source-cache warmup. Compare hold policies within the same GC setting.
Correctness gates check exact payloads, the original named-root revision after
root removal, logical reclamation during the hold, and complete pack reclamation
after release. Physical measurements sum file lengths under `blobs/packs`;
metadata/catalog size and filesystem allocated blocks are excluded.
The 4 MiB selected case retains historical packs while the read remains open;
the 4 KiB case exercises mixed-pack retention below the local 4 MiB pack target.
Both GC during an active read and GC after release retain wall-clock and phase
timings plus pin-ledger operation counts. GC during selected reads also caps
journal syncs at 16 to catch per-file deletion-claim regressions.

```console
benchmark run transfer-holds
benchmark all --suites transfer-holds --repetitions 6 --output /tmp/transfer-holds
```

The all-suite runner retains every JSON sample and alternates paired execution
order each repetition. Both profiles run the same bounded 16-case matrix.
For an existing build, supply `--bin-dir` containing a `transfer_holds` executable.
This isolates retention policy in the current implementation; it is not a
historical binary comparison. The SSH case excludes process startup, encryption,
and network RTT, so it cannot predict WAN throughput or startup latency.
The [six-repetition report](reports/2026-09-13-transfer-holds.md) includes raw
measurements, timing ranges, and logical versus physical reclamation results.
The historical [scoped-catalog](reports/2026-09-13-scoped-catalog-gc.md) and
[batched-cleanup](reports/2026-09-13-batched-cleanup.md) experiments record physical
narrowing and its cost; that narrowing was removed. The
[batching-only comparison](reports/2026-09-13-batching-only.md) validates cleanup
without changing historical pack retention.

`benchmark cleanup-batches --output results.json` measures real local cleanup
and file-ledger claims at 999 and 1,001 paths while preserving a disjoint held
file. It also measures a queue-only workload at 999, 1,001 and 100,000 paths,
recording RSS before cleanup and while its first deletion is paused. That probe
uses absent files and an in-memory object store to isolate queue iteration;
it is not storage throughput. RSS is optional off Linux. `--queue-counts` changes
its sizes; `--counts` changes the filesystem workload. `benchmark all` includes
both workloads. The [cleanup audit](reports/2026-09-13-cleanup-audit.md) covers
recovery and durability; the [retirement-queue comparison](reports/2026-09-13-retirement-queue.md)
measures the removal of the full-queue temporary copy.

`benchmark catalog-marking --output results.json` isolates historical payload
marking with a memory ledger and in-memory immutable catalog objects. Standard
cases use 1,024 and 65,536 entries per base, one or eight holds, and identical,
overlapping, or disjoint sharded roots. Each entry names one pack, one chunk and
one blob (four retained paths). Distinct overlapping roots differ in generation
but share the complete base; disjoint roots share no payload paths. Every sample
checks the exact union of all expected paths. Setup and validation are outside
timing; inventory acquisition, root decoding, shard loading and path marking are
inside. Each sample starts in a fresh process with an empty shard cache.

```console
benchmark run catalog-marking --profile standard --repetitions 3 --output /tmp/catalog-marking.json
benchmark all --suites catalog-marking --repetitions 3 --output /tmp/catalog-marking-smoke
```

Supply `--probe-binary` for a prebuilt library test executable, or `--bin-dir`
containing `casita-lib-test` for `benchmark all`. `--counts` and `--holds` override
the matrix. `--baseline-binary` adds paired baseline/candidate execution and
alternates their order each repetition, retaining both executable hashes and all
raw samples. The [shared-shard comparison](reports/2026-09-13-catalog-marking-dedup.md)
uses this mode. RSS before/after marking includes allocator reuse and catalog caches;
it is neither peak heap nor an exact retained-set allocation measurement. The
fixture also keeps its expected-path set alive. Path-string bytes give a separate
lower bound on retained data. This probe does not measure durable-ledger cost,
payload I/O, deletion, run overlays, or full GC latency. See the
[scaling investigation](reports/2026-09-13-catalog-marking.md) for results and limitations.

`benchmark held-catalog-gc --output results.json` measures whole local repository
GC with one, eight or ten selected transfer sessions holding distinct catalogs that
share a sharded base. Standard cases contain 65, 66, 128 and 1,024 deterministic
4 KiB blobs; smoke includes 65, 66 and 128. With one hold, 65 and 66 blobs
exercise 64 and 65 logical deletions, covering the proposed SQL batch boundary.
The [SQL batching experiment](reports/2026-09-14-delete-batch.md) was rejected;
these cases remain in the permanent corpus.
The subsequent [individual row-ID deletion comparison](reports/2026-09-14-rowid-delete.md)
keeps one prepared DELETE statement and was retained after paired and cutoff checks.
Later holds add one unrooted blob each. Setup uses the existing test-only
rebase threshold to create shards at bounded sizes, then reopens with normal
settings. It does not benchmark production rebase thresholds. All storage,
pin-ledger updates, logical pruning, compaction and cleanup during GC are real.

Each sample verifies distinct held roots and their common base, exact logical
removals, every original historical pack file, complete paused and freshly opened
reads through all sessions after GC, and complete pack reclamation after release.
It retains whole-GC and post-release timings, GC phase events and live-GC ledger
counters. Setup and correctness checks are outside timing. Compaction may create
new packs while the originals remain held. Compaction diagnostics split loading,
classification, copying/hash verification, sealing, retirement, replacement writes
and marker writes. The `compaction` and `release_compaction` summaries report
per-phase call counts, summed durations and the union of wall-clock intervals
(`busy_seconds`). Up to two packs run concurrently: sums can exceed elapsed time,
and busy times for different phases can overlap. Older binaries without these
events still work and produce empty compaction summaries. The retained
[marker-batching experiment](reports/2026-09-13-gc-marker-batch.md) adds a later
`compact_pack_marker_commit` phase outside the per-pack interval; use
`finish_deletions` and whole-GC time when comparing that prototype. The 128-blob
cases with eight and ten holds contain seven and nine wholly dead packs, covering
both sides of its eight-marker batch limit. The runtime prototype was not adopted;
its patch and correctness test are retained with the report.

```console
benchmark run held-catalog-gc --profile standard --repetitions 3 --output results.json
benchmark run held-catalog-gc --baseline-binary BEFORE --probe-binary AFTER --no-build --repetitions 3 --output paired.json
benchmark all --suites held-catalog-gc --repetitions 1 --output /tmp/held-catalog-gc-smoke
```

Logical-pruning diagnostics also split metadata work into writer-lock/blocking-pool
wait, transaction begin, revision preparation, retained-set SQL, state update,
transaction commit and WAL checkpoint attempt (`prune_db_*`). These are nested
inside `prune_commit`; fence admission, validation and release remain separate.
Checkpoint timing measures the existing best-effort attempt, not proof that all
WAL pages were checkpointed. No extra command or benchmark entry is needed.
See the [logical-pruning profile](reports/2026-09-14-logical-pruning-profile.md)
for the measured breakdown and reproduction commands.

On Linux, add `--strace-dir FRESH_DIRECTORY` to retain per-thread file-sync and
rename traces. Traced runs retain all correctness gates but include tracing
overhead and must be kept separate from performance comparisons. Sync summaries
cover the entire probe (setup, both collections and cleanup); concurrent syscall
durations are summed, not elapsed wall time. Marker-directory totals omit shared
ancestors outside `pack-replacements`. Raw traces, hashes and unparsed-line counts
are retained. Existing trace prefixes are rejected to avoid mixing runs.

```console
benchmark run held-catalog-gc --counts 128 --holds 8 --repetitions 1 --probe-binary LIB_TEST --no-build --strace-dir /tmp/gc-sync-traces --output traced.json
```

The runner accepts `--counts`, `--holds` and prebuilt library test executables.
Paired runs alternate binary order each repetition and retain raw samples and
executable hashes. `benchmark all` includes the bounded smoke profile. See the
[whole-GC comparison](reports/2026-09-13-held-catalog-gc.md) and
[compaction profile](reports/2026-09-13-gc-compaction-profile.md), and
[individual marker write comparison](reports/2026-09-13-gc-local-marker-put.md)
for results and limits.

`benchmark mutation-catalog --output results.json` exercises 63 and 65 successive
1 MiB catalog witnesses through one mutation and the real bounded file ledger.
It gates active catalog bytes and complete release, covering both sides of the
former cumulative 64 MiB history limit. It is included in `benchmark all`.

`benchmark run output-import --profile smoke --output results.json` measures the
same raw output staging path used by Obrador. It compares a mutation session and
metadata publication for every output with one session and one atomic publication
for all outputs. Smoke crosses the 128 KiB chunker threshold at 131,071, 131,072
and 131,073 bytes, and checks exact roots, byte-for-byte payload reads and a clean
`fsck` outside timing. The result retains session, staging and publication phase
times so batching tradeoffs are visible. It is included in `benchmark all`.

## Verified file I/O

`verified_io` is a permanent Criterion target, registered under `core-primitives`
in `manifest.json` and included in `benchmark all`. Run its correctness gates
or collect timings with:

```console
cargo bench --features experimental --bench verified_io -- --test
cargo bench --features experimental --bench verified_io -- --warm-up-time 1 --measurement-time 3
```

Cases cover BLAKE3's 1 KiB boundary, Bao's 16 KiB boundary, the default CDC
128 KiB minimum, the 64 KiB outboard spill boundary (16 MiB + 16 KiB of input),
and the leaf-value spill boundary (32 MiB of input), each on both sides.
They measure ingestion with retained proofs, time to the first 1,024 verified
bytes, complete verified streaming, and 300-byte overwrites both within and
across proof groups. Input comes from the deterministic seed-91 random corpus.
Each sample checks its digest or authenticated bytes; setup additionally checks
complete reconstructed old and edited contents against independently computed
BLAKE3 hashes. The backend is an in-memory object store with ordinary
chunked/zstd storage; overwrite timings reuse previously uploaded changed chunks.
They do not represent cold disk, network, or unique-edit upload latency.

Companion correctness tests assert bounded source reads: a 20 MiB input needs
less than 1 MiB of payload reads for either its first verified prefix or a
300-byte middle overwrite; prefix startup reads less than 32 KiB of metadata.
Paged overwrites also keep metadata reads and writes below 64 KiB in that
20 MiB regression case.

## Fresh edits with shared metadata pages

`overwrite_pages` is registered in `manifest.json` and `benchmark all` under
core primitives. It uses deterministic seed-91 input, explicit 16 KiB storage
chunks, and fresh 300-byte replacements within/across a chunk boundary. Sizes
are 63, 64, 65, 66, 4095, 4096, 4097, 4098, 4099, and 16384 chunks. These cover
both sides of flat-to-paged conversion and fanout-64 tree-height thresholds
for both chunk maps and Bao metadata, plus a 256 MiB scaling point.

```console
cargo bench --features experimental --bench overwrite_pages -- --test
cargo bench --features experimental --bench overwrite_pages -- --quick
cargo bench --features experimental --bench overwrite_pages
```

Every iteration computes an independent full-file BLAKE3 digest, times only the
edit, checks its returned identity, then checks full readback. It removes the
fresh result and reclaims its orphan pages outside timing, so repeated edits do
not accumulate storage. Setup, independent hashing, readback, and cleanup are
excluded from reported latency. This measures fresh uploads into an in-memory
object store, not disk or network latency, nor the repository's root commit.
The existing `verified_io` benchmark separately covers repeated deduplicated edits.

Object-store counters record actual range bytes read and bytes submitted to
PUT for payload and metadata separately. A benchmark-only global allocator
records peak extra Rust heap above the quiescent fixture baseline during the
edit. Native codec workspace and allocator internals are outside that counter;
latencies include its atomic-accounting overhead. Per-case maxima are printed
beside Criterion output. Gates require less than 256 KiB payload reads, 192 KiB
metadata reads, 128 KiB metadata writes, and 1 MiB extra Rust heap per edit, in
addition to content correctness. The thresholds allow runtime variation while
rejecting whole-file metadata copying and hashing at the larger sizes.

The [shared metadata pages report](reports/2026-09-11-shared-metadata-pages.md)
retains the first complete scaling run and its machine-readable measurements.

## Existing reports

The [chunk slicing investigation](reports/2026-09-16-chunk-slicing/README.md)
retains the rebuilt store path, whole closure, and shaped link measurements
behind the sliced SSH transfer. Its raw results are saved alongside the report.
The [pack buffer admission follow-up](reports/2026-09-20-pack-buffer-admission.md)
records the shaped S3 read check for the shared buffer budget change.

The [local metadata primitives report](reports/2026-09-08-metadata-kv.md)
measures batched reads, ordered scans, and atomic record/root commits, including
an 8× improvement in registration of already rooted, verified content.
It retains correctness gates and scaling cases around the collection regression
found at 8,192 records.

The [2026-09-08 statement-cache comparison](reports/2026-09-08-statement-cache.md)
measures collection, imports, retained histories, and publication contention.
It records faster warm metadata collection below the inline cutoff, noisy
end-to-end results, and a pre-existing collection slowdown above 65,536 objects.
The [collection paging follow-up](reports/2026-09-08-collection-paging.md)
isolates repeated SQL sorting and measures a 96.8% reduction in warm metadata
collection time by paging physical row IDs.

The [2026-09-06 performance report](reports/2026-09-06-performance.md) compares
catalog refresh, cache pressure, and RPC changes, including measured tradeoffs
and the discarded OID lookup experiment.
The [cache/network follow-up](reports/2026-09-06-cache-network.md) measures the
latency cost of extra cache misses, including concurrent-read regressions.
The [discarded adaptive promotion experiment](reports/2026-09-06-cache-adaptive.md)
records the timing gains, increased traffic and limited workload coverage that
led to reverting the policy.
The [publication snapshot profile](reports/2026-09-06-publication-snapshot.md)
isolates the primary index clone; it accounts for only a small share of the
measured retained-history update time.
The [publication phase investigation](reports/2026-09-06-publication-phases.md)
identifies the state-commit cost and measures releasing the validation snapshot
before the write transaction.

This directory contains the reproducible benchmark suite behind Casita
performance claims. The existing Criterion benches isolate the payload write
path and deduplication primitives; this harness measures complete user-facing
operations in fresh repositories.

The September 2026 optimization work is recorded in [EXPERIMENTS.md](EXPERIMENTS.md).
`--casita-incremental-sync` explicitly selects destination-closure reuse for
Casita sync measurements. The default retains exhaustive source discovery.
`experiments/compare_local.py` alternates two retained executables through the
validated standard suite and records their hashes, keeping build time outside
measurement.

## Layout and discovery

`manifest.json` is the canonical registry. `benchmark list` shows every
runnable entrypoint and `benchmark run NAME --help` shows suite-specific
options. Python implementations live under `suites/`, shared helpers under
`lib/`, and harness tests under `tests/`. Cargo Criterion microbenchmarks remain
in the `crates/casita/benches/` directory. Exploratory output belongs in
the ignored `results/` directory; only reviewed release evidence belongs in
`baselines/`.

```console
$ benchmark list
$ benchmark run catalog-index --help
```

### Run every registered suite

Memory index tradeoffs include full-key reads, ordered pagination, collection
removing 0/1/50/99/100 percent, and releasing all snapshot/store owners. Each
sample audits exact inventory, validation marks, roots and birth history:

```console
$ benchmark run memory-index-lifecycle --profile standard --output benchmarks/results/memory-index-lifecycle.json
$ benchmark all --suites memory-index-lifecycle --profile smoke --output benchmarks/results/memory-index-lifecycle-all
```


Memory publication distinguishes updates that change roots, application
records, the payload catalog or objects, plus idempotent updates and collection.
Both held-reader and no-reader cases are included at each repository size:

```console
$ benchmark run memory-publication --profile standard --output benchmarks/results/memory-publication.json
$ benchmark all --suites memory-publication --profile smoke --output benchmarks/results/memory-publication-all
```

Timing includes the commit and release of its reader; fixture construction,
snapshot acquisition and correctness audits are outside timing. Held-reader
cases also retain the original generation throughout the run. Every sample
checks exact inventory, roots, application records, catalog and birth generations.

Memory metadata snapshot scaling, including acquisition/drop and publication
while a reader retains an older revision:

```console
$ benchmark run memory-snapshots --profile standard --output benchmarks/results/memory-snapshots.json
$ benchmark all --suites memory-snapshots --profile smoke --output benchmarks/results/memory-snapshots-all
```

The standard corpus covers 256, 4,096 and 16,384 objects with one root and
validation mark per object. The `deep-copy` control models the old whole-index BTreeMap clones from a
template prepared before timing; `shared` uses the public snapshot API.
Both use the current publication path, which shares unchanged tree nodes.
Every sample checks the held revision's object, root, validation and catalog
views, plus the current root and inventory. Fixture creation is outside timing.

```console
$ benchmark all --profile smoke --nixpkgs /path/to/nixpkgs \
    --output benchmarks/results/all-smoke
```

This builds release artifacts once, copies them into the result directory,
records SHA-256 identities, and runs suites sequentially. Every suite has a
pending/running/passed/failed/timeout/skipped entry in `execution.json`.
Failures do not suppress subsequent suites. A skipped external nixpkgs corpus
makes the run incomplete and returns a nonzero exit status. The rebuilt store
path corpus is also opt-in: pass `--cdcs-store-names NAME,NAME` to include
`cdcs-corpus`, otherwise it is recorded as skipped. Use `--suites` for
a subset, `--bin-dir` for prebuilt artifacts, and `--build-dir` to isolate Cargo
intermediates from shared-cache cleanup. The output directory must be new.
The standard profile uses suite defaults; online holds covers both 60 and 300
imports with all four reader/GC combinations and application readers. Smoke uses
3 imports and 2 files per import. Physical frontier runs remain opt-in.
Core primitives include `write_path`, `dedup`, `repairing`, `optimization`, `verified_io`, and `cdcs`;
the codec and metadata experiments therefore run and retain Criterion results
through both `benchmark run core-primitives` and `benchmark all`.

Additional coverage is available through:

- `fsck`: full integrity checks just below, at, and above the metadata-cache
  limit, with process RSS, spill metrics, and exact checkout audits.
- `state-publication`: immutable snapshots and concurrent exact-revision CAS,
  including losing writers and independent commit retries on memory and Turso.
- `process-contention`: independent CLI writers beside root-list snapshot readers and fsck scans;
  every writer's root and restored bytes must survive.
- `casitar`: export, import, duplicate-payload import, tar import and malformed rejection.
- `fault-and-recovery`: a receiver killed before EOF, restart/reimport, and
  truncated-input rejection. The exact staging position at interruption is
  unspecified; this does not claim deterministic coverage of every crash point.
- `generations --generations N`: tiny updates and unchanged imports against
  retained histories, with root, fsck and restore gates for every generation.
- `catalog-maintenance`, `catalog-durability`, `logical-state`: the previously
  unregistered native maintenance and logical-state probes.

`edited-import` retains the copied-tree dedup workload. Use
`edited-import-in-place` for ordinary edits: its sample-local source preserves
all stat identities of unchanged regular files. `graph-traversal` accepts
`--spill-thresholds 4,1024,250000` and `--spill-expectation spill|memory|either`
so forced spilling and an in-memory control have separate result identities.
Gix's `below-cache` and `cache-pressure` profiles hold body size constant while
crossing the default metadata cache capacity. Path transfer accepts
`--bandwidth-kib` (per TCP connection, in each direction) and reports RPC
command counts separately from source S3 requests. `--max-rpc-requests N`
enforces a per-phase command budget; the all-suite smoke matrix allows five.

Local catalog writers now compare and durably replace their pointer under an
OS file lock, retrying and merging competing publications. Turso writers take
the write transaction before reading the expected revision. The multi-process
test verifies every published root after simultaneous writers exit.

The catalog point-read budget now includes bytes as well as requests. Newly
written large chunk shards use an authenticated routing footer and 1,024-entry
blocks; readers fetch a footer plus candidate blocks, while small shards use
one complete read. V2 maps and V1 chunk shards remain readable. New writes use
V3 maps and V2 chunk shards and require this reader version. Streaming rebase
still authenticates and processes complete shards.

### Adding a suite

Always promote benchmarks from performance investigations into this corpus,
including threshold cases on both sides of any discovered cliff. Temporary
probes and reports alone are insufficient. Wire new suites into `benchmark all`
and retain their correctness gates.

Add a module below `benchmarks/suites/` with a
`main(argv: Sequence[str] | None) -> int` entrypoint, register it once in the
`entrypoints` array in `manifest.json`, and add its harness tests below
`benchmarks/tests/`. The module should write versioned raw JSON plus a report to
`benchmarks/results/` by default. Add a dashboard normalizer only when the new
result schema is ready to be compared or published.

### Mutable metadata and Obrador indexes

```console
$ benchmark run metadata-kv --profile smoke --repetitions 1 --output benchmarks/results/metadata-kv.json
$ benchmark all --suites metadata-kv --profile smoke --output benchmarks/results/metadata-kv-all
$ benchmark run metadata-kv --profile standard --repetitions 3 --output benchmarks/results/metadata-kv-standard.json
$ benchmark run metadata-kv --counts 256,257,65536,65537 --batches 1,16,256,257 --value-bytes 4096 --page-size 256 --output benchmarks/results/metadata-kv-large-values.json
$ benchmark run metadata-kv --counts 256,257 --batches 256,257 --page-size 257 --output benchmarks/results/metadata-kv-page-boundary.json
```

One-shot `get-current-N` reads compare the short transaction path with
`get-current-reference-N`, which opens a metadata snapshot before reading the
same key. Both paths run the same revision, catalog-reference, and generation
validation inside their read transaction. Paired execution order alternates per
iteration. Widths 1/7/8/9 bracket
the shared pool's eight-idle-connection limit; the pool does not limit active
readers. Widths 15/16/17 retain the historical investigation's sixteen-idle
boundary cases without changing the current pool limit. Both paths check exact
returned bytes. These cases run in every
`metadata-kv` profile and through `benchmark all`. Their timings include worker
dispatch and transaction setup, and exclude fixture construction. They do not
measure whole derivations or cold OS-cache reads.

`metadata-kv` measures the implemented public local `get`, `scan`, and `commit`
APIs. Single hit/miss and mixed batch reads preserve input order, duplicates,
and absent values. An identical scalar-read control measures batching's
benefit on the same snapshot and keys. First-page, deep-page, and complete
prefix scans read a fixed 257 reverse references amid a growing unrelated
`paths/` inventory, excluding neighboring prefixes and another namespace.

Commit cases cover expected-absent insert, expected-value replace/delete,
first/last-check conflicts, atomic path descriptor plus reverse references and
GC root registration, disjoint writers, and same-key contention with exactly
one winner. Each process audits persisted row counts after reopen and confirms
that roots protect payloads while index records survive collection without
retaining their targets. Dedicated tests additionally cover binary/empty/0xff
prefixes, snapshot and cursor consistency, result-byte limits, independent
repository handles, additive schema migration, and process death partway
through record/root transactions.

Smoke crosses 256/257 and 8,191/8,192 initial path records with batches of 1/16
and two iterations. The larger pair retains the collection reader-lifetime
regression found by this investigation. Standard uses 256, 257, 8,191, 8,192,
65,536 and 65,537 path records with
batches of 1, 16, 256 and 257 and ten iterations. `--counts`, `--batches`,
`--iterations`, `--repetitions`, `--value-bytes` (1..4096), and `--page-size`
(1..1024) select explicit sweeps. The 257 matching referrers remain fixed, so
unrelated-inventory growth cannot masquerade as increased requested work.

Timing excludes fixture construction, format verification and assertions.
Reads reuse a reopened snapshot; OS caches are not flushed. Registration adds
one descriptor and a batch of edges per iteration. Contention timings include
task scheduling for two writers. Raw timings, medians, sample p50/p95/p99,
process RSS including setup/audits, binary identity, and failed-process output
are retained. Sparse smoke samples validate execution, not tail-latency claims.
The harness is registered in `benchmark all`, revision comparisons, and the
dashboard; batch, value size and page size remain separate dimensions.

`metadata-primitives` remains the earlier **object metadata baseline**: object
point/batch reads, full ordered object inventories and exact-revision commits.
It is deliberately separate from mutable application records. Existing
`state-publication` and `process-contention` suites cover revision-level writers.
These new mutable-record APIs support local Turso and an in-memory reference;
S3 returns an explicit unsupported error rather than accepting unpersisted data.

### Metadata collection cutoff

```console
$ benchmark run metadata-collection --profile smoke --repetitions 1 --output benchmarks/results/metadata-collection.json
```

Both smoke and standard cover 65,536 and 65,537 objects. Standard also covers
256 and 8,192 objects and measures three warm commits per fresh process;
smoke measures one. `--counts`, `--iterations`, and `--repetitions` override
these dimensions. The suite is included in `benchmark all` and supports
revision comparisons and dashboard normalization.

Every case retains all verified blobs and audits the exact object inventory and
revision after reopening. Commit timing excludes setup and audit; first and warm
collections are separate observations, with raw iterations and process RSS
retained. This isolates metadata collection, not graph traversal or payload
reclamation. The first collection follows seeding and does not imply a cold OS
cache. The paging investigation's paired results remain in the report above.

### Ordered metadata inventory

The [2026-09-08 ordered-scan investigation](reports/2026-09-08-ordered-scan.md)
records the scaling curve, repeated-sort query plan, and a diagnostic reader
teardown failure excluded from the successful measurements.
The [ordered-pagination fix and paired results](reports/2026-09-08-ordered-paging.md)
record the implemented range searches and a 65,537-object warm median reduction
from 14.028 seconds to 220.104 milliseconds across three matched pairs.

```console
$ benchmark run metadata-scan --profile smoke --repetitions 1 --output benchmarks/results/metadata-scan.json
```

This measures the public ordered `MetadataSnapshot::objects()` stream in a
reopened snapshot. Every scan checks exact records in canonical key order and
the snapshot revision. Smoke covers 256, 257, and 8,192 objects; standard adds
65,536 and 65,537. Setup, reopen, and correctness comparisons are outside scan
timing. First and warm scans remain separate, with raw iterations and diagnostic
SQL query plans retained. `benchmark all` and revision comparisons include this
suite. Diagnostic alternative query plans do not represent implemented scans.

### Metadata batch reads

The [2026-09-09 batched SQL investigation](reports/2026-09-09-metadata-bulk.md)
records direct read improvements, boundary costs, and inconclusive full-fsck results.

```console
$ benchmark run metadata-batch --profile smoke --repetitions 1 --output benchmarks/results/metadata-batch.json
$ benchmark run metadata-batch --profile standard --repetitions 3 --output benchmarks/results/metadata-batch-standard.json
```

This compares `MetadataSnapshot::object_batch` with the frozen a0a8b89 per-key
implementation (one task and lock per batch, cached point queries inside).
Both paths share a reopened snapshot and alternate order on each iteration.
Smoke uses 512 records and 1,024 requests; standard uses 8,192 and 65,536 records
with 4,096 requests. Both cover widths 1, 127, 128, 129, 255, 256, 257, and 1,024 with random
hits and mixed hits, duplicates, and misses across three namespaces.
`--counts`, `--widths`, and `--requests` support explicit sweeps.

Every iteration checks exact results in caller order; the probe also checks
reopened inventory and snapshot isolation across a commit. Setup and correctness
comparisons are outside timing. First and warm results, raw iterations, process
RSS, and the executable hash are retained. These are same-process comparisons
against a reference algorithm, not separate revision builds. `benchmark all`,
revision comparisons, and the dashboard include this suite.

### Full repository integrity checks

The [2026-09-09 sustained-spill investigation](reports/2026-09-09-spill-insertion.md)
records batched insertion results, individual pairs, and cache-boundary controls.
The [indexed spill membership investigation](reports/2026-09-09-spill-membership.md)
records a rejected multi-key SQL experiment: sustained-spill medians were
essentially unchanged against cached point lookups. It retains the candidate
patch, all paired results, and the same five memory limits and correctness gates.

`fsck` measures the complete `fsck --audit-only` command in a separate process,
including startup, repository open, and teardown. It also retains the CLI's
logical fsck timer and process peak RSS; import and checkout are outside timing
and outside the measured process. Setup audits warm the repository's OS cache.

```console
$ benchmark run fsck --profile smoke --casita target/release/casita --repetitions 1 --output benchmarks/results/fsck-smoke.json
$ benchmark run fsck --profile standard --casita target/release/casita --seed-probe /path/to/casita-lib-test --output benchmarks/results/fsck-standard.json
```

The deterministic wide-tree fixture contains one unique blob per file, branch
directories with at most 1,024 files each, and one top-level directory. Smoke uses 256 files (321
records); standard uses 8,192 and 65,536 files (8,257 and 65,601 records).
Frontier uses 249,755 files and 244 branches, giving exactly 250,000 records, the default
in-memory object limit. `--files` accepts an explicit comma-separated sweep.

Each fixture runs with the memory limit set to N−1, N, and N+1, where N is its
exact record count. N−1 exercises streamed metadata and spilled traversal sets;
N keeps the metadata cache but spills traversal sets; N+1 keeps both in memory.
Standard and frontier also use limits of 1,024 and 4,096 objects, exercising
sustained spilling after the first buffer fills. Smoke includes 32 and 128.
`--memory-objects` replaces the default limits for explicit sweeps, for example:

```console
$ benchmark run fsck --files 65536 --memory-objects 1024,4096 --casita target/release/casita --repetitions 3 --output benchmarks/results/fsck-sustained.json
```
The limit applies per structure and is not a bound on total process RSS.

Every sample requires a healthy report with exactly one root, N objects and N
payloads, an unchanged revision, the expected spill behavior, and no leftover
spill files. Before and after the matrix, checkout must match the source tree's
paths, types, content hashes, and executable bits; the retained root must remain
unchanged. Failed samples and partial matrices are saved and excluded from
dashboard comparisons. The suite is registered in `benchmark all` and revision
comparisons, and dashboard groups keep each memory limit separate.
Pass `--baseline-casita /path/to/baseline` to alternate two binaries on the same
fixture, reversing their order each repetition. Binary hashes are checked
before and after the matrix. `--work-dir /new/directory` retains the source and
repository for follow-up profiling. `--reuse-work-dir /existing/directory`
reuses those fixtures and repeats all correctness audits.

`--seed-probe /path/to/casita-lib-test` uses the registered ignored library test
`repository::fsck_benchmark::benchmark_seed_fsck_fixture` for untimed setup.
It constructs a new, exclusively owned repository with packed payloads,
format-verified records, and an atomic metadata/catalog commit. This avoids
charging fsck investigations for online import pin-ledger staging. It refuses
to write an existing repository. The normal CLI performs all timed fsck scans
and checkout audits; the seeder's binary identity and output are retained.
`benchmark all` supplies this probe automatically. Without it, direct runs use
the filesystem importer, whose setup can take substantially longer at scale.

The [2026-09-08 fsck investigation](reports/2026-09-08-fsck.md) records the
three-pair comparison, process memory, and CPU profile of the remaining spill
cost. At 65,601 records, the scan fix reduced streamed fsck from 23.047 seconds
to 9.529 seconds.

The [spill statement reuse investigation](reports/2026-09-08-spill-statements.md)
records the next optimization using the same permanent suite. Against a freshly
built baseline, streamed fsck at 65,601 records improved from 11.619 seconds to
8.100 seconds (30% faster); sampled SQL compilation fell from 22.29% to 0.27%
of user CPU cycles.

The [batched membership investigation](reports/2026-09-08-spill-membership.md)
uses the same suite to measure fewer blocking jobs for spill lookups. At 65,601
records, fresh paired medians improved from 7.904 seconds to 6.549 seconds for
streamed fsck (17% faster), and from 5.229 seconds to 4.051 seconds when metadata
remains cached but traversal sets spill (23% faster).

The [streaming inventory investigation](reports/2026-09-08-fsck-inventory.md)
replaces the final per-key inventory lookups with ordered stream comparisons.
At 65,601 records, fresh paired medians improved from 5.804 seconds to 4.535
seconds for streamed fsck (22% faster), and from 3.472 seconds to 2.115 seconds
with cached metadata and spilled sets (39% faster).

The [ordered metadata batch experiment](reports/2026-09-08-metadata-batches.md)
found no convincing fsck improvement from sorting and deduplicating batch keys.
The large streamed case changed by +1.71% initially and −2.03% in a five-pair
follow-up, with comparable control variation. The optimization was reverted;
both runs and the regression tests are retained.

## Quick smoke run

From the pinned development environment:

```console
$ devenv shell
$ benchmark --profile smoke \
    --implementations casita,git,tar-zstd \
    --cache-policies warm \
    --repetitions 1
```

Unavailable optional tools are reported as skips. Pass `--require-all` for a
publication run so a missing comparator is an error.

## Publication run

The root development environment supplies Casita's Rust toolchain, Python,
Git, GNU time, GNU tar, zstd, restic, and Borg from the single `devenv.lock`:

```console
$ devenv shell
$ benchmark \
    --profile standard \
    --implementations casita,git,tar-zstd,restic,borg \
    --operations cold-import,unchanged-import,edited-import,checkout,sync-cold,sync-warm,verify,collect \
    --cache-policies warm,cold \
    --repetitions 10 \
    --require-all \
    --require-clean \
    --output benchmarks/baselines/REVISION-MACHINE.json \
    --report benchmarks/baselines/REVISION-MACHINE.md

$ benchmark-dashboard \
    --output docs/public/benchmarks/index.html \
    --catalog-output docs/public/benchmarks/catalog.json
```

`devenv.lock` pins the complete build and comparison environment. The JSON also
retains the resolved tool versions. Borg 2 uses a different command surface and
is deliberately rejected until it has a separately reviewed adapter.

Use comma-separated selectors to make iteration cheaper. `--keep-work PATH`
preserves every sample workspace for inspection; otherwise workspaces are
created in a private temporary directory and removed after the result files are
written.

Regenerate and validate a suite-specific report from its authoritative raw
result without rerunning timed work:

```console
$ benchmark --render-existing benchmarks/baselines/REVISION-MACHINE.json
```

The public page is the unified dashboard, not a suite-specific report.
`benchmark-dashboard` reads the suite registry in `manifest.json`, normalizes
every selected result, and exposes both the interactive page and its catalog.
By default it includes every checked-in JSON result below
`benchmarks/baselines/`; repeat `--result PATH` to render an explicit set.

## Commit-to-commit comparisons

The harness keeps workload execution and correctness validation local, then
exports a small registered metric set for historical tracking. Metric IDs,
units, direction, and comparison policy live in `manifest.json`; unregistered
diagnostic counters remain in the raw result without creating a new time
series.

The revision runner accepts two or more Git revisions. It resolves every input
to an exact commit, checks each out sequentially in one isolated worktree, and
copies its release artifact aside. It then uses the current stable harness to
rotate revisions between benchmark rounds:

```console
$ benchmark revisions \
    baseline=v0.1.0 candidate=HEAD~1 current=HEAD \
    --suite repository \
    --repetitions 10 \
    --baseline baseline \
    --output-dir benchmarks/results/revision-series \
    -- \
    --profile standard \
    --implementations casita,git \
    --operations cold-import,unchanged-import,checkout,verify \
    --cache-policies warm,cold \
    --require-all
```

Options after `--` belong to the selected suite. The runner supports
`repository`, `nixpkgs`, `pack-limits`, `pack-index`, `catalog-index`,
`pack-gc`, `s3-pack`, `s3-pack-index`, `s3-pack-gc`, `s3-path-transfer`,
`graph-traversal`, `git-scale`, and `gix-odb`. It builds the exact Casita CLI,
Rust example, library test probe, or hashed Cargo bench executable required by
the selected suite. The native Criterion `core-primitives` entrypoint keeps
Criterion's own result format and is run once per checked-out revision instead
of through this JSON-series runner.
The runner controls repetitions, builds, revision metadata, workspaces, and
result paths so those suite options cannot accidentally invalidate the
comparison. Round one runs baseline/candidate/current, round two rotates to
candidate/current/baseline, and so on. Use `--keep-worktrees PATH` only when
the isolated source and target trees are needed for debugging.

When an exact artifact was already built and retained, pass one
`--artifact LABEL=PATH` for every revision label. The runner records each
provided artifact's size and SHA-256 digest in `execution.json` and skips only
the build step; suite execution and interleaving are unchanged. This is useful
when several suites consume the same CLI binary.

Large repository corpora can use `--skip-post-fsck` after the `--` separator
to avoid repeating a full integrity scan after every timed command. Checkout
and manifest validation still run for every sample, and timed `verify` samples
still execute the selected `--casita-fsck-mode`.

The output directory contains every raw per-round JSON and Markdown artifact,
`execution.json` with the exact schedule, resolved commits, and built-binary
SHA-256 digests, `series.json`
and `series.md` with values against both the chosen baseline and the preceding
revision, and one Bencher Metric Format document per revision. A failed sample
remains visible and prevents that observation from being exported as a valid
performance point. Builds use one worktree and Cargo target cache so Cargo can
reuse unchanged dependencies across revisions; a target directory shared
between distinct worktree paths does not reliably provide that reuse. Each
completed revision artifact is copied aside before the next commit is checked
out, so timed runs still use exact immutable per-revision executables.

For example, compare exact persistent-open request counts across three
revisions with a small RustFS matrix:

```console
$ benchmark revisions before=REV1 candidate=REV2 current=HEAD \
    --suite s3-pack-index --repetitions 5 -- \
    --targets-mib 16 --files 8192 --file-kib 8
```

The same pattern works for `s3-pack-gc`, `s3-path-transfer`, and `gix-odb`;
their normal suite options remain available after `--`.

Run the same benchmark matrix for the base and head commits on the same pinned
testbed, preferably in interleaved order, and retain both raw JSON artifacts.
For already existing artifacts, compare a pair without rerunning either side:

```console
$ benchmark compare \
    --base-result results/base-repository.json \
    --head-result results/head-repository.json \
    --output results/comparison.json \
    --report results/comparison.md \
    --bencher-output results/head.bmf.json
```

The comparison matches observations by suite, workload, profile, cache policy,
operation, and implementation. Commit IDs and timestamps are deliberately not
part of the benchmark identity. Missing observations remain visible rather
than being compared as zero, and failed observations are not exported to the
performance history.

For Bencher, the exporter writes the documented Bencher Metric Format. Keep
export and upload as separate steps so the exact payload is retained with the
raw benchmark artifacts:

```console
$ benchmark export-bencher \
    --result results/head-repository.json \
    --output results/head.bmf.json
$ bencher run \
    --host http://127.0.0.1:36610 \
    --project PROJECT \
    --branch BRANCH \
    --hash COMMIT \
    --testbed PINNED-TESTBED \
    --adapter json \
    --file results/head.bmf.json
```

For the free self-hosted tier, bind the API and Console to loopback or place
them behind the repository's private network boundary. Projects are public to
anyone who can reach that instance; Bencher-level private-project access
control is a paid feature. Do not use the default `bencher up` port mapping on
an externally reachable host because it binds container ports to all host
interfaces.

The API-only local prototype was validated with the pinned v0.6.11 images. Its
database and configuration survive container replacement in named volumes:

```console
$ docker volume create casita_bencher_etc
$ docker volume create casita_bencher_data
$ docker volume create casita_bencher_logs
$ docker run -d --name casita_bencher_api --restart unless-stopped \
    -p 127.0.0.1:36610:6610 \
    -v casita_bencher_etc:/etc/bencher \
    -v casita_bencher_data:/var/lib/bencher \
    -v casita_bencher_logs:/var/log/bencher \
    ghcr.io/bencherdev/bencher-api:v0.6.11
```

On NixOS, where the generic Linux CLI binary does not use the system dynamic
linker, run the pinned CLI image and mount only the exported payload:

```console
$ docker run --rm --network host \
    -v "$PWD/results/head.bmf.json:/data/results.json:ro" \
    ghcr.io/bencherdev/bencher:v0.6.11 run \
    --host http://127.0.0.1:36610 \
    --project PROJECT --branch BRANCH --hash COMMIT \
    --testbed PINNED-TESTBED --adapter json \
    --file /data/results.json
```

The open-source server supports the history, branch, testbed, threshold, alert,
and GitHub-reporting workflow needed here. Bencher Plus is only needed if the
instance itself must enforce private-project access or another explicitly
Plus-licensed capability. For a private repository, keep the free instance
unreachable outside the trusted CI network and do not expose its public project
URLs.

Bencher owns history, branches, thresholds, alerts, and PR reporting; the raw
harness JSON remains the authoritative evidence. Exact request budgets can gate
immediately. Timing and RSS thresholds should be enabled only after enough
clean runs exist to model the testbed's noise. The integration targets Bencher
v0.6.11 until the self-hosted server and CLI are upgraded together.

The release-facing controlled-RustFS path matrix is published separately from
the comparator run because it measures protocol request shape rather than tool
semantics. Run it from a clean worktree after building the release helper:

```console
$ benchmark run s3-path-transfer \
    --transports direct-s3 --rtt-ms 0,30,80 \
    --depths 0,4,16 --subtree-files 1,16,64 \
    --cache-mib 0,64 --repetitions 10 \
    --require-clean \
    --output benchmarks/baselines/REVISION-MACHINE-s3-path-transfer.json \
    --report benchmarks/baselines/REVISION-MACHINE-s3-path-transfer.md
```

## Gix object-database benchmark

The native-Git history benchmark exercises import and fetch. The separate Gix
ODB benchmark isolates the compatibility traits used inside a Git operation:
bounded writes plus flush, cold and warm body lookup, header lookup, missing
OID lookup, and lookup after reopening persistent storage.

```console
$ benchmark run gix-odb --profile smoke --repetitions 3
$ benchmark run gix-odb --profile standard --repetitions 10
$ benchmark run gix-odb --profile smoke --pack-cache-mib 0 --repetitions 3
```

Every timed row has a Gix in-memory or loose-object baseline where the
operation has a direct counterpart. The report retains Casita pack range,
whole-pack, and cache counters so a latency improvement cannot hide backend
request amplification. Full OID and payload validation runs outside the timed
regions.

## Verified-repair benchmark

The Criterion repair target measures healthy full and Bao range reads, repair
of a corrupt compressed chunk, rebuilding corrupt Bao state from verified local
bytes, and eight readers sharing one repair flight. Destructive scenarios build
and damage a fresh in-memory near tier before the timer starts; every timed
repair sample validates the returned payload after the timer stops.

```console
$ cargo bench --features experimental --bench repairing
```

## Workloads

Each sample gets a fresh repository. Setup, cache conditioning, and validation
are outside the timed region.

| Operation | Timed work |
|---|---|
| `cold-import` | Add the base tree to an initialized, empty repository |
| `unchanged-import` | Add the byte-identical tree after one base snapshot |
| `edited-import` | Add a deterministically edited tree after one base snapshot |
| `checkout` | Materialize the base snapshot into an empty directory |
| `sync-cold` | Copy the selected snapshot into an initialized, empty repository |
| `sync-warm` | Copy an edited snapshot after the destination received the base |
| `verify` | Run the implementation's full available data-integrity check |
| `collect` | Remove an unreachable base snapshot and reclaim its physical data |

Not every tool supports every operation. Unsupported pairs are omitted, while
missing executables are explicit skips in the result. In particular, tar+zstd
has no retention or synchronization model and Borg 1.x has no direct local
repository-copy operation.

The standard profile has three deterministic corpus shapes:

- `small-files`: 4,096 small structured and pseudo-random files;
- `mixed`: 512 small files plus eight 4 MiB structured/binary files;
- `large-files`: four 32 MiB structured/binary files.

The edited tree inserts deterministic bytes into one file and adds one file.
Every file is generated from a named SHA-256 counter stream or a fixed text
pattern. Paths, executable bits, symlinks, and timestamps are fixed. The JSON
records SHA-256 hashes of the complete base and edited manifests.

## Cache policies and ordering

`warm` reads every regular source and repository file immediately before the
timed operation. `cold` calls `POSIX_FADV_DONTNEED` for those files after a
filesystem sync. This needs no root privilege, but it cannot evict directory
metadata or storage-device caches. Results must retain the policy label and
must not describe this as a whole-machine cold boot.

For each corpus, cache policy, and operation, implementation order is shuffled
from the recorded seed and rotated between repetitions. This balanced,
deterministic ordering reduces first-tool and thermal bias without making the
run order irreproducible.

## Measurement and validation

The timer uses GNU time to measure child CPU and peak resident memory, with
wall time measured by Python's monotonic clock. GNU time starts the measured
process from a native launcher, so Python's heap does not contaminate child RSS.
CPU counters have GNU time's 10 ms reporting resolution. RSS is the maximum
individual process peak in the command sequence, not aggregate concurrent RSS. Repository apparent bytes,
allocated bytes, and entry count are measured after the operation. Throughput
uses logical source-file bytes; it is not compressed or transferred bytes.

After timing, the harness runs the tool's integrity check. It restores the
selected result and compares a deterministic manifest containing every path,
file SHA-256, size, executable bit, and symlink target. A command that exits
successfully but restores different data is a failed sample.

The JSON contains raw samples, bounded stdout/stderr captures,
operation-specific Casita transfer counters, exact command lines,
pack list/footer/range/whole-object request and byte counters, cache activity,
configuration, environment, tool versions, corpus identities, failures, and
derived aggregates. The Markdown report is always rendered from that
in-memory result. Median and p95 use the nearest-rank definition.

## Nixpkgs acceptance workload

`nixpkgs` is an opt-in real-corpus entrypoint in the same repository benchmark
schema. It exports an exact committed tree from a local checkout, excluding
dirty and untracked files, and records the full commit, tree, manifest, path
count, and logical byte identities without publishing the local checkout path.

```console
$ benchmark run nixpkgs \
    --source /path/to/nixpkgs \
    --nixpkgs-revision HEAD
```

The release defaults run Casita and Git ten times under both warm and cold
policies for cold import, unchanged import, checkout, and full verification.
Every timed sample reports wall/user/system time, peak RSS, logical-byte
throughput, apparent and allocated repository bytes, entry count, and
backend-specific pack metrics. Checkout output is compared byte-for-byte and
by mode/symlink metadata with the exported source manifest; every other
operation is followed by integrity verification and an independently validated
checkout. Git and Casita both ingest the same exported single tree, so unrelated
nixpkgs history does not inflate Git's storage result. The nixpkgs entrypoint
forces Git to include paths matched by the source tree's `.gitignore`, then
runs `git gc --prune=now` after ingest so its storage and checkout measurements
use a pack rather than tens of thousands of loose comparator objects.

For a quick development run, override the matrix explicitly:

```console
$ benchmark run nixpkgs \
    --source /path/to/nixpkgs \
    --operations cold-import,checkout \
    --cache-policies warm \
    --repetitions 1
```

The workload accepts only operations with well-defined semantics for one
committed tree. Edited import, warm incremental sync, and collection require a
separately pinned second tree and are deliberately rejected instead of silently
benchmarking a zero-change substitute.

## Interpretation

These tools overlap; they are not interchangeable:

- Casita verifies native object identity and graph closure before publication.
- Git is a source-tree object database and version-control system.
- restic and Borg are backup systems with snapshot policies. Restic's mandatory
  repository encryption remains enabled; the Borg adapter uses
  `--encryption=none` to match Casita's local plaintext-at-rest profile.
- tar+zstd is a deterministic one-shot archive baseline without deduplication,
  named retention, repository sync, or object-graph verification.

Reports must keep these semantic differences beside the tables. A result from
one filesystem and machine is evidence about that environment, not a universal
ranking or a performance regression threshold.

## Pack-limit sweep

`pack-limits` runs the validated Casita adapter at several compressed pack
targets and produces both the ordinary raw results and a cross-target report:

```console
$ benchmark run pack-limits \
    --targets-mib 1,4,16,64,256 \
    --profile standard \
    --operations cold-import,checkout \
    --cache-policies warm,cold \
    --repetitions 3
```

Every sample records pack count, pack/body/footer bytes, entry count, largest
pack, loose-chunk count (required to stay zero), and the exact index-rebuild cost: one 16-byte
trailer range plus one encoded-footer range per pack. The local sweep finds
filesystem and index-rebuild knees; it does not replace remote validation.

## Persistent pack-index benchmark

`pack-index` isolates repository reopen cost. It imports one deterministic
repository, measures the one-time footer rebuild after the inventory changes,
then opens the unchanged repository in fresh processes:

```console
$ benchmark run pack-index \
    --files 1024 --file-kib 32 --pack-target-mib 1 --warm-opens 5
```

The harness requires the cold open to read exactly two footer ranges per pack
and publish one checksummed v1 root with a compare-and-swap update. Its small
fixture keeps the base inline, so every warm open must load the authoritative
catalog in one GET with zero LIST, separate checkpoint, or footer reads.
Missing or corrupt catalogs fall back to four inventory LISTs over the packs,
replacement records, GC deltas, and blob manifests.

For cliff finding without materializing millions of payload files, the catalog
suite runs the production-format encoder, a fresh-process decoder/RSS probe,
and lookup/list/GC operation probes at every requested cardinality:

```console
$ benchmark run catalog-index \
    --entries 65536,262144,1000000 \
    --manifest-percents 0,1,10,100 \
    --shard-bits 0 \
    --projection-storage-tb 500 \
    --projection-average-chunk-kib 256 \
    --projection-pack-mib 16 \
    --projection-run-mib 1 \
    --repetitions 3 --lookups 1000000 --threads 8 --gc-percent 10
```

It records exact catalog bytes, encoding, hashing, decoding, Linux peak RSS,
chunk and manifest membership lookup, concurrent chunk lookup, full ID
enumeration, and in-memory GC marking. Manifest density is expressed relative
to the chunk-entry count; zero-density samples retain the established
acceptance budgets while nonzero samples expose the manifest catalog's
incremental cost.

The same run also exercises the v1 exact-mutation publication codec. It adds one
pack and 1,024 manifest IDs, then compares a full inline catalog rewrite with a
content-addressed base checkpoint and one bounded inline delta. The report
includes publication bytes and CPU plus exact reopen reconstruction. It forces
the external-base mode to expose its cold-open tradeoff: smaller ordinary PUTs
require two GETs (root and base), while small live repositories retain an
inline base and one-GET open. When inline deltas reach their encoded-byte
bound, production v1 seals them into one immutable run; binary level carries
merge older runs without rewriting the base checkpoint. Large sharded bases
are opened through a small map object without fetching their referenced runs.
The authenticated sharded map includes run block routing while the WAL3 root
stays small, so point lookup fetches one candidate chunk block without
materializing the run; clean-base point reads fetch only
the addressed shard, while enumeration and GC visit one shard at a time.

The immutable-shard table reports the map size, nonempty object count, largest
object, total bytes, and encode time for separate chunk-location,
manifest-membership, and pack-state tables. `--shard-bits 0` selects a width
from a 32 MiB maximum chunk-shard target; explicit widths 1–24 are available
for sweeps. At 65,536 chunks, the automatic one-bit layout produced six
objects and a 377-byte map, compared with 5,041 objects and a 256 KiB map for a
fixed 12-bit layout. This request-count difference is why a fixed global width
is not acceptable for S3.

At the 500 TB frontier and a 256 KiB average stored chunk, the same policy
projects about 1.907 billion chunks and selects 13 bits (up to 8,192 prefixes).
The current exact dual access orders project roughly 290 GB of immutable index
objects: about 183 GB for 96-byte chunk locations and about 107 GB for pack
state. This is a format/capacity projection, not yet a passing 500 TB runtime
result.

The request-amplification section deliberately tests and rejects eager
affected-shard rewriting. With the default 500 TB projection, one 1 MiB inline
delta batch holds about 278 changes for 16 MiB packs; its queryable immutable
run object is larger because it also contains range-readable location blocks.
The batch's 17,792 chunks are expected to touch 7,259 of 8,192 chunk prefixes
plus 274 pack prefixes. Fetching and replacing those objects would require
about 7,533 GETs and 7,534 PUTs per seal. Across an initial 500 TB ingest that
is roughly 1.62 billion incremental index requests, versus about 322,000 for
immutable run PUTs, binary merge GETs, and authenticated routing-map PUTs. The other
29.8 million catalog publications ride inside the wal3 state fragments already
required by logical commits, so they add no standalone S3 request. Immutable
byte-bounded delta runs, binary leveled merging, and wal3 root co-publication
are now active in the v1 production path; the benchmark reports their seal and
reopen costs. A fresh-process sharded-base probe also gates cold open, point
lookup, cache reuse, shard-by-shard enumeration and GC by exact object-store
request counts, with a separate peak-RSS limit at the canonical scale. A
streaming rebase folds long-lived immutable run levels into a new sharded base;
the benchmark gates one GET per old data shard and one PUT per new data shard
plus its map, and applies a 256 MiB peak-RSS ceiling at one million entries.
Normal publications append exact deltas and immutable binary-leveled runs.
Their exact block routing is stored in the authenticated sharded map already
fetched at open, keeping WAL3 small without adding a GET. At 4 GiB of aggregate
encoded run data or 12 run references, the rebase folds them into a fresh
immutable base one shard at a time. The 500 TB threshold sweep selects 4 GiB
under a 32 MiB open-map routing budget, cutting projected complete-base rebase
requests from 149 million to 2.4 million, including routing-map PUTs. RustFS
integration tests use a per-instance,
test-only threshold to exercise the same WAL3 publication path with racing
writers without weakening the production threshold.
The deferred-run probe separately gates sharded open at one map GET, first run
point lookup at one range GET, and subsequent lookups at zero GETs.
The 500 TB routing probe additionally materializes the selected rebase interval's
full changed-pack and chunk-block routing map, then measures authenticated map
encode/decode time and peak RSS rather than extrapolating from the small run.
It complements the RustFS benchmark; it does not measure network transfer or
wal3 recovery.

The canonical 1,000,000-entry run is also an acceptance gate. Reader peak RSS
must stay at or below 256 MiB, hit and miss lookups must each stay at or below
1 us/op, and removing 10% (100,000 entries) must finish within 100 ms. Smaller
cardinalities remain useful for finding scaling cliffs but do not decide the
gate. The limits live in `benchmarks/manifest.json`, so reports and exit status
cannot silently disagree with the documented targets.

`s3-pack-index` repeats the same contract through temporary RustFS buckets
and Casita's real S3 payload plus wal3 state profile:

```console
$ benchmark run s3-pack-index \
    --targets-mib 16 --files 512,2048,8192 --file-kib 8 --repetitions 3
```

The helper times repository open and an open-plus-first-snapshot operation,
including wal3 state recovery. It separately reports the state-first WAL3 and
payload-index stages, first-snapshot time, unchanged cached-snapshot time, and
payload-catalog hashing and decoding CPU. The wal3 phase table further separates
writer/manifest recovery, fragment GET transfer, Parquet parsing, Casita state
decoding, and conditional manifest refresh latency. It also reports record count and
logical record bytes beside physical fragment bytes so format amplification is
visible. Sweeping `--files` shows whether state recovery scales with repository
cardinality independently of pack size. This distinguishes S3 request fan-out
from CPU-bound parsing and verifies that the payload catalog is seeded from the
opened WAL3 checkpoint without an advisory-pointer request, while WAL3 reuses a
checkpoint only while its manifest witness remains current. Treat mmap or a
zero-copy table as justified only when decode is a material share of the target
open latency; it cannot reduce wal3 freshness checks.

Every RustFS sample enforces the S3 request-cost gate from the same manifest.
A complete unchanged inline-catalog warm open must use two requests: WAL3's
initial manifest GET and one tail-fragment GET. It must use zero payload-pointer,
LIST, or footer-range requests. Each subsequent unchanged snapshot must use
exactly one conditional wal3 manifest GET and no fragment request. Any extra
request fails the run. The report's Base column distinguishes inline,
content-addressed checkpoint, and sharded bases and includes the number of
immutable run objects. A clean sharded base is allowed exactly one additional
GET for its map object, so its warm-open total is three requests.

## S3 path-selected transfer benchmark

`s3-path-transfer` isolates the path-selection behavior added by
`transfer_path()`. It creates deterministic nested trees, imports each through
the real S3/wal3 profile, and transfers the selected directory closure into
fresh verified destinations. The default uses temporary RustFS buckets:

```console
$ benchmark run s3-path-transfer \
    --depths 0,4,16 --subtree-files 1,64 \
    --cache-mib 0,64 --repetitions 3
```

The matrix reports cold and same-handle warm latency, pack range and whole GET
counts, bytes, cache hits, and wal3 snapshot requests. Repository construction,
import, and correctness validation are outside the timed interval. Loopback
RustFS establishes request amplification; it does not simulate production RTT,
so an RPC or auxiliary path index should be proposed only after the same shape
is material on a deployed endpoint.

`--subtree-files` also exposes selected-closure width. The path spine remains
dependent, but range reads for independent selected payloads run under one
bounded physical-I/O gate and can overlap after resolution. Sweep at least
`1,16,64` with the pack cache disabled when validating that behavior; the
request ledger should remain unchanged while fitted client-visible turns stop
growing one-for-one with file count.

For a repeatable latency sensitivity experiment without root privileges, put a
user-space TCP delay relay in front of RustFS with `--rtt-ms`. The driver splits
each configured RTT evenly across both directions and routes all S3 traffic
through the delayed endpoint; import and source open remain outside the timer:

```console
$ benchmark run s3-path-transfer \
    --rtt-ms 0,10,30,80 \
    --depths 0,4,16 --subtree-files 1 \
    --cache-mib 0,64 --repetitions 10
```

RTT is part of the deterministically interleaved matrix. The zero-delay row
keeps the same proxy hop, making within-run depth and RTT comparisons fair.
This isolates dependent request latency, but does not model cloud throttling,
TLS termination, routing variance, or service-side queueing.

To compare that direct path with the atomic path-proof protocol, add both
transports to the same interleaved run:

```console
$ benchmark run s3-path-transfer \
    --transports direct-s3,atomic-rpc \
    --rtt-ms 0,10,30,80 \
    --depths 0,4,16 --subtree-files 1 \
    --cache-mib 0,64 --repetitions 10
```

For `direct-s3`, the controlled RTT sits between the client and RustFS. For
`atomic-rpc`, it sits between the client and a resolver that holds the same
S3/wal3 repository open beside RustFS; resolver-to-RustFS traffic remains on
loopback. The receiver verifies the bounded proof bindings exactly as it does
over the SSH transport, but the in-process benchmark channel deliberately
omits peer authentication and encryption so it measures protocol turns rather
than cryptography.

Pass a caller-owned bucket prefix to measure that endpoint using credentials
and region from the standard `AWS_*` environment:

```console
$ benchmark run s3-path-transfer \
    --s3-url s3://benchmark-bucket/casita-path \
    --latency-label us-east-1-from-runner-a \
    --depths 0,4,16 --subtree-files 1 \
    --cache-mib 0,64 --repetitions 10
```

The driver deterministically interleaves the complete configuration matrix in
each repetition. Remote sample prefixes are unique and intentionally retained;
the versioned JSON result lists every exact URL for cleanup under the bucket
owner's retention policy. Published deployed-endpoint results should use at
least 10 repetitions and report median and p95.

## S3 pack sweep

`s3-pack` uploads each pack target once below a unique caller-owned prefix,
then reads those same immutable packs into fresh validated local repositories
while sweeping cache capacity and promotion threshold:

```console
$ benchmark run s3-pack \
    --s3-url s3://benchmark-bucket/casita \
    --targets-mib 4,16,32 \
    --cache-mib 0,64,256 \
    --promotion-reads 1,2,3 \
    --profile standard --corpus mixed --repetitions 3
```

The report includes source-side range, whole-object, cache, footer-request, and
footer-byte counters. Run it once per representative network condition and set
`--latency-label` accordingly. The harness never deletes bucket data: its JSON
lists the unique prefixes that the operator may remove through normal bucket
retention tooling after recording the result.

The optional `pack-tuning` profile expands the deterministic corpora enough to
fill candidate packs: 32,768 small files, a roughly 224 MiB mixed tree, and a
512 MiB large-file tree. It is intentionally heavier than the publication
profile and should normally be used with only the corpus under investigation.

## Pack GC density sweep

`pack-gc` isolates immutable-pack reclamation at several requested deletion
densities. It snapshots pack footers, replacement records, and immutable GC
deltas containing durable bitmap tombstones, so the report uses the actual percentage of entries removed from
dirty packs and records read/write amplification rather than assuming file
deletion density maps directly to pack density:

```console
$ benchmark run pack-gc \
    --targets-mib 4,16 \
    --dead-percent 1,10,50,100 \
    --files 512 --file-kib 64 --repetitions 3
```

Every sample validates that fully dead packs use no whole-pack read, sparse
packs below the 50% compressed-byte threshold share one batched delta PUT,
denser partially live packs use exactly one whole-pack read and one replacement,
survivor range reads remain zero, the retained tree checks out, and the
repository passes `fsck`.

`s3-pack-gc` runs the same requested-density matrix through a temporary
RustFS endpoint using Casita's real S3 payload and wal3 state implementations:

```console
$ benchmark run s3-pack-gc \
    --targets-mib 4,16 \
    --dead-percent 1,10,50,100 \
    --files 512 --file-kib 64 --repetitions 3
```

The RustFS report measures GC latency, whole-object requests and bytes, and
removed chunks. Its complete request ledger includes payload inventory, reads,
writes and deletes, the catalog update, wal3's state refresh and durable
append, immutable logical-state shards, and the checkpoint publication
barrier. Manifest-backed gates require twelve total wal3/state requests: the
five core wal3 requests, at most two changed logical-shard PUTs, and the
first-use barrier's two GETs plus three conditional PUTs. They also require
zero logical-shard inventory LISTs during repository collection, zero payload
inventory LISTs, zero standalone catalog PUTs, no survivor ranges or catalog
recovery reads, and at most one batched tombstone PUT. Each sample gets a fresh
bucket; setup is outside the timer and the temporary server is removed after
the run. Use `s3-pack` separately for real-endpoint latency and adaptive
read-cache tuning.

## Native-Git scale suite

`git-scale` complements the filesystem corpus above with deterministic Git
histories generated directly through `git fast-import`. It measures the paths
that determine whether a multi-gigabyte repository remains usable:

- cold and unchanged native import;
- immutable-view service bind;
- full clone, including server peak memory;
- incremental import and rebind after a small history extension;
- incremental fetch, with native Git clone/fetch baselines.

The four shapes independently stress object count, delta-heavy history,
physical pack bytes, and very wide trees. Start with the smoke profile:

```console
$ benchmark run git-scale \
    --profile smoke \
    --shape many-objects \
    --keep-work /fast-disk/casita-git-scale
```

`standard` is intended for a dedicated benchmark machine. `huge` is an
explicit cliff-finding matrix and may take hours. Its `pack-heavy` corpus uses
deterministic incompressible payloads and represents more than 30 GiB of
physical pack input. Keep its work directory on the filesystem being evaluated
and retain the raw JSON report:

```console
$ benchmark run git-scale \
    --profile huge \
    --shape pack-heavy \
    --cache-policy cold \
    --max-pack-bytes 34359738368 \
    --repetitions 3 \
    --keep-work /benchmark-volume/casita-git-scale \
    --output benchmarks/results/git-scale-huge.json
```

The suite passes its recorded `--max-pack-bytes` value through to `git serve`;
its default is 8 GiB so the standard physical-pack case exercises a complete
stream instead of intentionally tripping the service's conservative 512 MiB
default. Increase it explicitly for huge corpora whose generated Git pack can
exceed that bound.

`--pack-compression-level` records and forwards the generated-entry zlib level
(`0..=9`, default `6`). Lower levels can expose CPU/transfer tradeoffs, but they
must be compared on both incompressible and delta-heavy shapes: level 1 can be
faster on physical-pack data while materially increasing bytes on compressible
histories.

The separate `delta-heavy` shape represents more than 30 GiB of logical blob
versions that Git can pack efficiently; it does not satisfy the physical-byte
frontier. `many-objects` and `wide-tree` isolate metadata and path-count costs.
The remaining canonical targets—500 TB of unique physical storage, one million
revisions, 10,000 generations, a 0.001% delta, a 1 GiB process budget, and cold
restoration—are defined in `manifest.json` and remain visible as partial
coverage until dedicated runners enforce them. The 500 TB frontier is a
catalog and object-store scale target (about two billion 256 KiB chunks), so it
may be generated without materializing 500 TB of payload bytes; it must still
measure exact shard sizes, request counts, and peak memory.

The huge profile is not part of CI. Failures at configured traversal, protocol,
or pack limits are retained in the report and make the runner exit non-zero.
Operations whose prerequisites fail are reported as failures rather than
silently omitted. A release claim requires both the synthetic axis runs and at
least one locally supplied real repository; reports record its content identity
without publishing sensitive names.

## Graph traversal and spill

`graph-traversal` measures closure verification and collection with a small
explicit spill frontier. It records peak RSS through the shared `wait4` timer,
the object count, actual spill databases opened, peak temporary bytes,
temporary-byte budget, and cleanup result. Start with:

```console
$ benchmark run graph-traversal --profile smoke \
    --repetitions 1 --output benchmarks/results/graph-traversal-smoke.json
```

Use `standard` for a scaling point and `frontier` for a manual cliff run.
The default 512 MiB spill budget is shared by every traversal structure in one
operation; lower it deliberately to record a limit cliff as a failed result.

## History, cache, and network scale matrix

```sh
benchmark run history-scale --profile standard --repetitions 3 \
  --output benchmarks/results/history-scale.json
benchmark run pack-cache-scale --profile standard --repetitions 3 \
  --output benchmarks/results/pack-cache-scale.json
benchmark run network-scale --profile standard --repetitions 3 \
  --output benchmarks/results/network-scale.json
benchmark run gix-odb --profile below-cache --repetitions 3 \
  --output benchmarks/results/oid-below-cache.json
benchmark run gix-odb --profile cache-pressure --repetitions 3 \
  --output benchmarks/results/oid-above-cache.json
```

`history-scale` retains **100, 1,000, and 10,000 distinct named snapshots**.
Each snapshot changes a 256-byte file beside a shared 64-KiB file. The default
seeds older snapshots in batches of 64, then measures the last 100 individual
publications at each checkpoint (`--window`). This isolates retained-root
cardinality without forcing one physical pack per historical generation.
All earlier roots remain named, and every root and its exact directory/file
bytes plus clean `fsck` are audited after reopening at each checkpoint.

Use **`--seed-batch-size 1`** for the separate fragmented sequential-history
stress case: every generation is then an individual measured publication. This
can be much slower and must not be compared as the same fixture as batched setup.
The JSON records the setup batch size and seeded generation count, which remain
separate dashboard identities. Each measured update retains its latency and
catalog PUT bytes/requests; the interval and last-window distributions preserve
compaction spikes. Reports also include storage growth and reopen latency.
Per-update diagnostics split mutation startup, blob staging, directory staging,
and publication, with catalog bytes read and hash/decode counters. The catalog
GET count counts external catalog objects; byte counts also include decoded
pointer bytes. Use `--idle-sessions 3` to measure repeated mutation
starts without staging or publishing after each integrity audit, outside
publication timings. The usual automatic maintenance policy still applies.
Setup, validation, and fixture generation are outside timing; no filesystem scan
runs on every update. `--generations` selects other increasing checkpoints.
Storage growth measures file lengths, including the database WAL, not allocated
blocks or writes below the filesystem. Catalog PUT counters exclude payload and
database writes. The sequential stress runner is deliberately available even
when a full 10,000-generation run is too expensive for routine smoke testing.

Each history update also reports `catalog_snapshot_calls`,
`catalog_snapshot_nanos`, `catalog_snapshot_payload_bytes_lower_bound`, and
`catalog_snapshot_accounting_nanos`. These test-only counters isolate the primary
index clone in state preparation and standalone publication. They exclude lock
acquisition, lazy-catalog/witness copies and additional background-rebase clones.
Payload bytes count live owned data; spare capacity, hash-table control data and
allocator overhead are excluded. Memory accounting is outside clone timing and
reported separately. Older probe binaries without these fields remain readable.
The `publication_phases` map further separates coordination wait, snapshot read,
validation/mutation assembly, payload preparation, state commit and catalog
finalization. `catalog_build_calls` and `catalog_build_nanos` are nested within
payload preparation, not additional time to sum. `coordinates_payload_catalog`
records which publication path the fixture used. Local Turso uses standalone
catalog publication during flush; coordinated backends prepare a catalog for
the state commit. The reader rejects missing/invalid phase counters and phase
totals exceeding their enclosing publish duration.

`pack-cache-scale` compares **4 MiB and 32 MiB** of incompressible data against an
**8-MiB pack cache**, with 256-KiB pack targets and the default two-read promotion
threshold. It verifies that actual packed bytes lie on the advertised side of
cache capacity. Sequential, seeded shuffled, and skewed (80% of reads choose
from 12.5% of objects; the remaining 20% choose from all objects) access patterns
each run cold and warm, recording individual read
latencies, p50/p95/p99/max, backend pack bytes, GETs, hits and evictions. Each pattern
reopens the store, clearing application caches; the warm pass follows an
additional untimed full sweep. Operating-system caches are not dropped.
Below-cache warm reads must avoid backend payload reads, and above-cache warm
sequential reads must actually evict and fetch. Every timed payload is compared
byte-for-byte after its timer stops. `--working-set-kib`, `--cache-mib`,
`--promotion-reads`, and `--reads` expose the fixture dimensions. Peak process
RSS includes the expected-byte fixture and all phases, so it is not a direct
measure of cache memory alone.

The existing Gix profiles separately straddle its **8,192-entry OID cache** with
4,096 and 16,384 objects of equal body size. These are metadata-cache workloads;
compressible Gix fixture bodies are not evidence of physical pack-cache pressure.

`pack-cache-network` crosses the physical cache boundary with real S3 reads
through the TCP delay/rate proxy. It supports sequential, shuffled, and skewed
access, cold and primed caches, and read concurrency. Its optional
`--baseline-helper` alternates before/after execution for every case and checks
that fixture bytes, pack layouts and access sequences match. Start with a
focused comparison; the complete default matrix can take hours:

```sh
benchmark run pack-cache-network --profile smoke --no-build \
  --helper /path/to/after/pack_cache_network \
  --baseline-helper /path/to/before/pack_cache_network \
  --patterns random --phases warm --concurrency 1 \
  --rtt-ms 100 --bandwidths-kib 8192 --repetitions 3 \
  --output benchmarks/results/cache-network.json
```

Build the identical `crates/casita/examples/pack_cache_network.rs` helper against each source
revision with `cargo build --release --features s3,ssh,experimental --example
pack_cache_network --locked`, then copy each executable before building the
other revision. Revisions lacking this helper need its source and example
declaration added; record that harness-only patch with the results.

The smoke fixture uses a **1 MiB cache, 512 KiB / 4 MiB working sets, and 128
reads**; standard uses **8 MiB, 4 MiB / 32 MiB, and 2,048 reads**. A cold phase
opens a fresh Casita cache. A warm phase opens another fresh handle and primes
it with one complete sequential scan. Every read is checked byte-for-byte.
Wall time includes verification, while p50/p95/p99 read latencies exclude
verification and time waiting for admission. The maximum in-flight count is
for logical reads, not individual S3 requests. OS caches are not flushed. Rates
apply per TCP connection in each direction, so increasing concurrency may also
increase aggregate bandwidth. Fixture upload and cache priming are untimed.
Helper binary hashes, raw output, process resources and server log tails are
retained, including failures. Dashboard grouping separates revision variants
and concurrency settings.

`network-scale` crosses **0/25/100 ms RTT** with **unlimited/1/8 MiB/s**, using
both direct S3 and atomic RPC, with cold and warm source caches. Each phase
uses a fresh in-memory destination; warm does not mean destination deduplication. Each standard
subtree contains 64 files of 16 KiB (1 MiB total) at depth four. Bandwidth blocks
are deterministically shuffled across repetitions; the underlying runner also
shuffles transport/RTT cases. Each phase retains the existing transfer harness's
verified selected-closure and installed-root checks. The shaping rate applies **per connection,
per direction**, not to an aggregate shared link. The RustFS endpoint is local:
these are controlled transport experiments, not production WAN measurements.
Three repetitions are exploratory; use more before making timing regression
claims. Select axes with `--rtt-ms` and `--bandwidths-kib`.
RustFS setup allows up to 60 seconds for readiness and includes recent server
logs in startup errors; this wait is outside transfer timings.

All four scale runners have `smoke` profiles and participate in
`benchmark all`, `benchmark revisions`, and dashboard normalization. Pass
`--probe-binary` (history/cache) or `--helper` (network) with `--no-build` to use
retained binaries. Reports record artifact hashes, process RSS, and environment
metadata. They save completed cases incrementally; failure leaves `complete:
false` and an error, and incomplete matrices cannot enter revision comparisons.
Different generation counts, working-set sizes, RTTs and bandwidths remain
separate dashboard observations.

## Online holds under contention

The [contention report](reports/2026-09-08-online-holds.md) records the initial
import failure, the local-ledger fix, and a successful full-workload rerun.
The [GC progress investigation](reports/2026-09-08-gc-progress.md) separates
snapshot-protected garbage from unnecessary conflicts caused by empty writers.
The [object-scoped read report](reports/2026-09-08-scoped-reads.md) records the
narrower reader protection, correctness checks, and benchmark rerun.

```sh
cargo bench --features experimental --bench online_holds
# Compare with the historical experimental object reader:
CASITA_BENCH_READER_SCOPE=object cargo bench --features experimental --bench online_holds
# Compare with full snapshot readers:
CASITA_BENCH_READER_SCOPE=snapshot cargo bench --features experimental --bench online_holds
# Short correctness/smoke run:
CASITA_BENCH_IMPORTS=3 CASITA_BENCH_FILES=2 cargo bench --features experimental --bench online_holds
# Also discoverable as: benchmark run online-holds
```

This standalone experiment emits four JSON lines for imports alone, imports with
one reader, imports with GC, and all three together. Each scenario uses a fresh
local repository, 30 sequential generic filesystem imports of 16 unique,
deterministic 4 KiB files, and a four-thread Tokio runtime. Corpus generation and
repository setup are outside timing. Imports replace the same named root so
previous versions become collectible. The reader repeatedly opens a rooted
sentinel payload through the application API's process-owned reader and verifies
its bytes. The default scope is `application`; `CASITA_BENCH_READER_SCOPE=object`
selects the historical experimental `open_payload` path, and `snapshot` selects
full snapshot holds. Each result records `reader_scope`. The application mode
opens a second local repository handle before timing; all modes share the same
on-disk repository. `benchmark all` therefore exercises application readers by
default too.
Set `CASITA_BENCH_SCENARIO` to `imports`, `imports_readers`, `imports_gc`,
or `imports_readers_gc` to run only that scenario; unset runs all four.
Readers pause 1 ms between operations; GC attempts pause 5 ms. Actual request
rates depend on operation latency and runtime scheduling.

`files_per_second` includes mutation admission and publication. Writer admission
percentiles time `mutation_session`; `reader_open` percentiles include pin and
snapshot admission plus opening the payload stream, excluding reading its bytes.
Historical `reader_admission` results timed only snapshot-hold acquisition and
are not directly comparable to this metric. `ledger_revision_changes`
is the durable pin inventory revision delta, measured through the final worker
and deferred-release drain. It includes reader and collector activity, and is
**not** a count of physical writes, bytes written, or failed CAS attempts. The
import timer ends when imports finish; background operations already in flight
may finish afterward and contribute to the ledger count.

GC reports successful passes, busy attempts, other typed retryable errors, and
logical objects removed by successful passes during the concurrent workload.
`gc_busy_reasons` and `gc_retryable_error_reasons` count the exact error messages
within each total. Messages are diagnostic text, not stable error identifiers;
revision conflicts include revision IDs and may occupy separate entries.
`gc_completed_passes` records each successful pass's elapsed completion time
and reported removals, so repeated runs can distinguish progress spread across
the import interval from progress concentrated near its end.
The `gc_passes_during_imports` and `gc_removed_objects_during_imports`
fields count successful passes whose completion timestamp is at or before the
writer's completion timestamp. The corresponding `_after_imports` fields count
in-flight passes completed afterward; the two groups sum to the existing totals.
These are pass-completion measurements: a pass completed after imports may have
performed some deletions earlier. They do not timestamp individual deletions.
Retryable failures can occur after partial progress, so these removal counts can
undercount actual concurrent reclamation. `cleanup_removed_objects` reports a separate final
collection, excluded from measurements. A successful run verifies sentinel reads,
recovers unfinished GC claims before checking for leaked pins/deletion claims,
runs fsck, and checks out and byte-compares
the final import after collection. Non-retryable GC errors and all import/read
errors fail the run; scenarios time out after
five minutes. Small smoke runs need not reclaim anything during concurrent GC.

Use larger `CASITA_BENCH_IMPORTS` values for meaningful tail percentiles, and
repeat runs on an idle machine before comparing changes. This measures one local
writer with optional reader/collector contention; it does not cover multiple
writers, separate processes, S3, or long-lived reader snapshots. Scenarios run in
a fixed order without statistical warmup. Save JSON output under `results/`
alongside the revision, command, hardware, filesystem, and machine-load details.
`benchmark all --suites online-holds` runs and retains this experiment too;
its smoke profile uses three imports of two files, and `--repetitions` repeats
all four scenarios. Standard uses the defaults above.

### Object reader protection

The [local reader report](reports/2026-09-08-object-reads.md) records the original
GC progress and admission costs. The [process-reader follow-up](reports/2026-09-09-process-readers.md)
measures ordinary local reads with process-owned protection and retains the final
correctness, crash recovery, and reservation-boundary results.

```console
$ benchmark run object-reads --profile smoke --repetitions 1 --output benchmarks/results/object-reads.json
$ benchmark all --suites object-reads --profile smoke --output benchmarks/results/object-reads-all
$ benchmark run object-reads --profile standard --repetitions 3 --output benchmarks/results/object-reads-standard.json
```

`snapshot-connections` measures snapshot acquisition, a checked generation read,
and release with fresh versus reused database connections. It covers 1, 7, 8, 9,
and 16 simultaneously live snapshots around the eight-connection idle bound.
Every burst checks the latest committed generation; the suite also rejects
writes and verifies that idle connections retain no transaction. Writer commits
are outside timing. The fresh control includes destruction of idle connections;
this is a connection-reuse experiment, not a historical binary comparison.

```sh
benchmark run snapshot-connections --profile standard --repetitions 3 --output benchmarks/results/snapshot-connections.json
benchmark all --suites snapshot-connections --profile smoke --output benchmarks/results/snapshot-connections-all
```

`object-reads` compares ordinary object protection with explicit snapshot protection
on local packed storage. It measures admission, physical resolution, total open,
temporary lease cleanup, payload consumption, release, and collection separately.
Total open includes admission and resolution, so those metrics overlap. Each case
checks exact bytes and seek after independent collection and vacuum, the expected
logical removals, physical GC progress for object readers, and complete pin release.
Pack cache is disabled; OS caches remain warm. RSS includes setup and audits.

Both profiles include empty and tiny objects, 65535/65536/65537 bytes around the
64 KiB inline verification threshold, and a guaranteed multi-chunk object.
Smoke uses 1 and 4 unrelated garbage objects; standard uses 1, 16, and 64.
The default modes are `object`, `durable-object`, and `snapshot`, each with cold
and warm reader admission. Cold means first process-owner setup, not a cold OS
cache. `durable-object` is a test-only control using the previous durable pin
handoff with the same narrow GC contract. Snapshot mode has broader retention
and provides context rather than an equivalent performance baseline. Warm object
opens and all object releases must leave the durable ledger unchanged in these
bounded fixtures; cold setup, durable-object, and snapshot admission must change
it. Raw results retain these gates and the combined ledger revision transitions.

```console
$ benchmark run object-reads --sizes 64,1048593 --garbage-counts 1,64 --modes object,durable-object --admissions warm --repetitions 3 --output benchmarks/results/reader-comparison.json
$ benchmark run reader-coordination --profile smoke --repetitions 3 --output benchmarks/results/reader-coordination.json
$ benchmark all --suites object-reads,reader-coordination --profile smoke --repetitions 1 --output benchmarks/results/readers-all
```

`reader-coordination` measures registration, pack protection, and release with 1
and 64 other active readers. Both profiles cover the last reserved revision,
the next transition that renews the durable reservation, and a transition after
renewal. Test setup positions the real revision counter just below the boundary
outside timing; the timed operation follows the production reserve path. Warm
operations must avoid durable changes, rollover must persist its reservation,
stale and protected deletion claims must fail, and durable staging must survive
with no leaked reader pins. Smoke uses 8 warm cycles; standard uses 128. Raw
iterations, binary hashes, commands, and environment metadata are retained.

### Durable local ledger

```console
$ benchmark run durable-ledger --profile smoke --repetitions 3 --output benchmarks/results/durable-ledger.json
$ benchmark run durable-ledger --counts 1,64,4096,16384 --writers 1,8,64 --contexts quiet,readers,claims --iterations 8 --repetitions 3 --output benchmarks/results/durable-ledger-scale.json
$ benchmark run ledger-boundaries --repetitions 3 --output benchmarks/results/ledger-boundaries.json
$ benchmark all --suites durable-ledger,ledger-boundaries --profile smoke --repetitions 1 --output benchmarks/results/ledger-all
```

These Linux/macOS suites retain optimized Rust test probes, binary hashes, raw process
logs, environment metadata, and correctness gates. `durable-ledger` compares the
journal and a preallocated full-inventory replacement control in the **same binary**.
On macOS this control is not the historical temporary-file backend; use the
pinned Obrador revision comparison to measure the actual before/after change.
Journal runs also cover active reader pins and outstanding deletion claims. Their
correctness gate requires `cached_edits == operations` and zero inventory copies
or diffs, preventing regressions to whole-inventory CPU work.
It measures registration, payload pin extension, catalog publication pin
extension, release, deletion claims, and deletion completion separately. The
publication phase measures ledger protection; actual payload sealing and metadata
publication are outside this probe. Counts select retained records; writers select
concurrent operations. Reported time is phase completion time divided by logical
operations, **not individual request latency**. Setup, rejection gates, and cold
replay audits are outside timing. Filesystem caches remain warm.

`ledger-boundaries` includes every boundary in both profiles: 255/256/257 journal
operations (grouped below the byte limit); the third/fourth/fifth 256 KiB catalog record (the fourth crosses the
1 MiB frame window); 1/2 and 63/64/65 operations through the production group executor;
and legacy migration with growth denied/allowed. The group probe runs the batch
executor on a blocking worker and excludes asynchronous queue arbitration. The
space probe injects allocation denial; it does not fill a physical device.

Counters record frames, bytes submitted, append/checkpoint syncs, checkpoint
count, cross-handle journal adoptions, group size, and replacement updates.
`max_group` is the process-local store’s cumulative high-water mark; other
reported counters are measured interval deltas.
`journal_syncs` counts the append file sync or the checkpoint file and directory
syncs; capacity growth and adoption syncs are not included. Every acknowledged
journal update crosses the required durability boundary. Gates verify exact cold
replay, protected resources, stale-revision rejection, deletion arbitration, no
leaked protection, and exact checkpoint/group limits. Both suites run through
`benchmark all`, revision comparisons, and dashboard normalization.

The [active-GC progress report](reports/2026-09-08-active-gc.md) separates
pass completions during and after imports and covers duplicate-reader mark validation.

The [GC conflict investigation](reports/2026-09-08-gc-conflicts.md) identifies
collector admission races as the dominant conflict in a follow-up run.

The [admission-retry experiment](reports/2026-09-08-admission-retries.md)
eliminated observed admission failures but still found no reclamation during
the combined reader/import workload.

The [logical-protection experiment](reports/2026-09-08-logical-protection.md)
separates logical prune validation from physical resource protection and
records the remaining logical-pin conflicts.

The [already-covered root experiment](reports/2026-09-08-covered-roots.md)
permits protection already covered by the retained mark and reports 289
objects reclaimed in passes completed during combined imports and reads.

The [three longer repetitions](reports/2026-09-08-repeated-online-gc.md)
passed integrity checks but did not reproduce useful GC pass completions during
combined imports and reads; the earlier progress observation is not consistent.

The [unpublished-root investigation](reports/2026-09-08-unpublished-roots.md)
classifies the remaining mark conflicts without relaxing GC protection rules.

The [absent-root validation follow-up](reports/2026-09-08-absent-root-validation.md)
implements that exception and cached retry checks, but six runs still found
no useful GC pass completions during imports because prune-fence admission raced.

The [fenced-validation experiment](reports/2026-09-08-fenced-validation.md)
moves final root checks inside the fence but still finds admission races.
The [atomic-admission follow-up](reports/2026-09-08-atomic-prune-admission.md)
combines inventory capture and fence installation in one ledger operation.
Its three runs eliminate prune-admission failures but still show inconsistent
GC progress and long reader waits; phase timing is the next investigation.

The [GC phase investigation](reports/2026-09-08-gc-phase-timing.md) adds
wall-clock timings to every benchmark GC attempt, including failures. Nested
phase timings distinguish collector admission, marking, prune validation/commit,
physical deletion claims, deletion calls, and catalog work.

The three profiled runs identify collector admission and repeated physical
chunk-claim admission as the dominant delays; metadata pruning stays short.

The [atomic collector and physical-claim follow-up](reports/2026-09-09-atomic-collector-and-claims.md)
checks ownership and current validated protection within ledger transactions,
with per-page caching of successful immutable manifest expansions.

The follow-up eliminates measured collector-admission failures and chunk-page
retries in all three runs. Catalog-reclamation conflicts still prevent sustained
successful collection during imports.

The [deferred payload-cleanup follow-up](reports/2026-09-09-deferred-payload-cleanup.md)
allows ordinary GC to finish when pins change before a cleanup batch starts
deletion. All three repetitions reclaimed 95–97% of obsolete logical objects
during imports, with zero catalog-cleanup errors; remaining objects were
reclaimed afterward. Tests cover late holds, historical catalogs, cancellation,
failed deletion recovery, and strict emergency fences.

The [pin-ledger latency investigation](reports/2026-09-09-ledger-latency.md)
separates lock waits, file syncs, and GC admission phases. Syncs dominate local
ledger writes. Removing a redundant spare-file allocation sync reduces the
steady-state sync count from four to three per update. Three follow-up runs
preserve active GC progress and integrity; reader p99 is 460–521 ms, while
shared-host throughput results remain inconclusive.

The local journal suites and their correctness gates run identically on Linux and
macOS. The 1/2 cases distinguish individual writes from the first shared barrier;
63/64/65 cross the maximum group size. Neither profile skips these boundaries.

The [shared Linux/macOS pin persistence report](reports/2026-09-09-shared-pin-persistence.md)
retains common correctness gates, threshold probes, raw Linux comparisons and
traces, macOS improvements, and the inconclusive Linux timing comparison.

The [merged-main GC report](reports/2026-09-09-merged-main-online-gc.md) compares
three application-reader runs with three historical experimental-reader runs
on the combined journal/GC implementation. Application reader p99 is 29–33 ms;
GC reclaims 78–85% of obsolete logical objects during imports. Temporary broad
reader holds introduce snapshot-generation conflicts, making their narrowing
the next investigation. The permanent benchmark now defaults to application
readers; `CASITA_BENCH_READER_SCOPE=object` retains the historical path.

The [narrow reader-admission follow-up](reports/2026-09-09-narrow-reader-admission.md)
protects the requested closure while preserving catalog and physical protection.
Three application-reader runs show no snapshot-generation conflicts and reclaim
97–98% of obsolete logical objects during imports. Reader p99 is 45–66 ms;
these shared-host runs demonstrate improved GC progress, not a latency gain.

The [alternating before/after comparison](reports/2026-09-09-paired-reader-admission.md)
adds six pairs each at 60 and 300 imports plus a no-GC control. Short-workload GC
progress improves at a reader-latency cost; the longer workload reproduces a
catalog-publication cleanup failure in both versions. This blocks a clean
performance claim and needs a deterministic regression and fix. Standard
`benchmark all` now retains both import counts.

The [publication-cleanup fix](reports/2026-09-09-deferred-publication-cleanup.md)
lets pinned online cleanup defer a writer's pending catalog publication while
keeping unpinned and emergency cleanup strict. Its deterministic regression
passes, and all three 300-import follow-up runs finish with integrity intact,
reclaiming 99.3–99.7% of obsolete logical objects during imports.

The [checkpoint investigation](reports/2026-09-10-checkpoints.md) separates
checkpoint and append-sync cost and adds permanent single-record byte boundaries.
The `checkpoint-record-bytes` cases make three tiny protection edits to catalogs
just below, at, and above 1 MiB, with exact replay and durability-counter gates.

### FSKit setup reuse

The [`filesystem-transports` baseline](reports/2026-09-10-filesystem-transports/README.md)
measures real mounted metadata, reads, concurrency, mmap, execution and lifecycle
with integrity and isolation gates. It is registered in `benchmark all` and runs
the native Linux FUSE or macOS native FSKit transport (run `casita-fs-setup` once with the packaged app before mounting). Its first/repeat passes do
not imply cold backing storage; native FSKit uses Apple-managed IPC.

## Whole-blob hash reuse at chunk boundaries

`hash_write_boundaries` in the permanent `write_path` Criterion target measures
cold writes to the in-memory object-store backend. It is registered under
`core-primitives` in `benchmarks/manifest.json` and runs through `benchmark all`.
The deterministic random and zero-filled corpora cover 0 B, 64 B, 1 KiB,
16 KiB, 64 KiB, 128 KiB minus/exact/plus one byte, 256 KiB, 512 KiB
minus/exact/plus one byte, 1 MiB, and 4 MiB. These are controlled fixtures,
not a claim about representative repository size distributions.

Every timed write checks its returned size. Outside the timed region, every
sample checks the whole-blob BLAKE3 digest, exact chunk identities and sizes
against the standalone FastCDC implementation, and complete readback. Store
creation and verification are excluded from the elapsed write duration.

```sh
devenv shell cargo bench --features experimental --bench write_path -- hash_write_boundaries --save-baseline before-hash-reuse
# Run the same command on the changed implementation:
devenv shell cargo bench --features experimental --bench write_path -- hash_write_boundaries --baseline before-hash-reuse
```

Use identical toolchains, host conditions, and Criterion options for both runs.
The matrix includes both sides of the small-file fast path and the maximum
chunk buffer boundary, where EOF may not yet have been observed when the first
chunk is emitted. Digest reuse must never infer EOF from a full buffer.

The writer shares one borrowed upload context across queued chunks. When the
streaming hasher has observed EOF and FastCDC yields a first chunk covering
exactly that byte count, the writer reuses its completed digest and skips the
second hash and blocking-pool dispatch. Multi-chunk uploads retain independent
worker hashes. Repository-level checks against dishonest backend completions
remain independent. No extra payload buffering or lookahead is introduced.

The [hash-reuse investigation](reports/2026-09-10-hash-reuse.md) retains the
correctness results and raw timing data. Its shared-host timings are
inconclusive; use an isolated paired run before making a speedup claim.

## Repeated durable NAR imports

The `nar_import` Criterion target includes `nar_import_sequence/{15,16,17,18,19,32}`,
registered through `core-primitives` in `manifest.json` and `benchmark all`.
Each iteration imports distinct one-KiB regular files into a fresh local
repository and retains every import report until the sequence ends. Historical
catalog pins therefore remain live while later mutations run maintenance.
These counts bracket the 16-start metadata reclamation interval, including
the initial import needed to create pending catalog work.

Only imports are timed. Every sample checks canonical SHA256 and NAR size,
rejects association hits, and independently scrubs each retained root before
releasing the reports and collecting. The corresponding library test reports
pin-ledger sync counts without imposing a scheduler-sensitive exact count.

```sh
devenv shell cargo bench --bench nar_import -- nar_import_sequence
devenv shell cargo test --bench nar_import
devenv shell cargo test --lib repeated_nar_intake_reports_maintenance_syncs -- --nocapture
```

The [maintenance investigation](reports/2026-09-20-metadata-maintenance/README.md)
retains the paired sync measurements, raw traces and cleanup tradeoffs.
## Content-defined chunk slicing

`chunk_slicing` in the permanent `cdcs` Criterion target compares the current
whole-chunk FastCDC deduplication with an encoder that stores a rebuilt blob
as slices of blobs the store already holds. Sampled small discovery chunks
find a candidate, a byte comparison confirms it, and the match extends in
both directions until the first mismatch, so one copy token covers the whole
run between two edits. All literals of one blob share one zstd frame. The
target is registered under `core-primitives` in `benchmarks/manifest.json`
and runs through `benchmark all`.

The strategy matrix holds exact chunking at 256 KiB (the production default),
64 KiB, 8 KiB and 1 KiB; slicing with 8 KiB, 2 KiB and 1 KiB discovery chunks
sampled 1 in 16; and 1 KiB discovery sampled 1 in 4 and unsampled. The exact
rows down to the discovery sizes separate the effect of slicing from the
effect of merely cutting smaller chunks. The sampling sweep brackets the cliff
where a gap between two edits holds no sampled chunk and is lost whole.

The synthetic fixture is four 1 MiB files of alternating aperiodic text and
random 64 KiB blocks with a fake Nix store path every 512 B, 4 KiB, 64 KiB and
1 MiB; the rebuilt tree maps each of eight dependency hashes to a new hash
consistently. The spacings bracket both cliffs: below the 1 KiB discovery
chunk no discovery chunk survives an edit, and below the 256 KiB production
chunk no whole chunk survives one. Files never share content with each other,
so every saving comes from the base tree.

Every strategy reconstructs each rebuilt blob through its compressed frames
before it is reported. Exact rows decompress every chunk frame; slicing rows
decompress the literal frame and resolve copies from base bytes or from the
already verified decode of an earlier rebuilt blob. The whole-blob BLAKE3
identity must match. JSON rows on stderr report physical bytes (new payload
plus 40-byte manifest entries or token metadata), literal and copy bytes,
token counts, index entries, the longest reference chain, and how many
distinct sources a verified 16 KiB group read must touch. Timed iterations
encode the rebuilt tree against a prebuilt base index.

```sh
cargo bench --features experimental --bench cdcs -- chunk_slicing
```

`cdcs-corpus` runs the same matrix on two real trees with identical layouts,
by default two rebuilds of one Nix store path discovered in the local store.
Among all `<hash>-NAME` paths, the first two with equal file layouts but
different contents form the pair; identical copies and layout changes are
skipped so a zero-change import or a version upgrade is never reported as a
rebuild. `benchmark all --cdcs-store-names icu4c-76.1,gmp-6.3.0` includes it;
without that option the suite is recorded as skipped.

The closure form pairs every store path that is new in the rebuilt closure
with its old counterpart: the same name for a rebuild, or the unique old path
with the same package key for an upgrade. Shared paths cost nothing under any
strategy and are only counted; derivations and paths without a counterpart
are listed as unpaired. Each pair is encoded against its own counterpart, not
the whole old closure, so cross-package matches are not counted. Process
output is discarded and the result keeps per-strategy totals beside the rows.

```console
$ benchmark run cdcs-corpus --store-name icu4c-76.1 --store-name gmp-6.3.0
$ benchmark run cdcs-corpus --base /path/to/tree --rebuilt /path/to/rebuilt-tree
$ benchmark run cdcs-corpus --closure-base /nix/var/nix/profiles/system-149-link \
    --closure-rebuilt /nix/var/nix/profiles/system-150-link
```

With `--wire`, every pair is also imported into a source and a destination
repository and its rebuilt root is synced through the stdio transfer protocol
by the `sliced_wire` example, behind an in-process relay that delays each
direction by half the round trip and paces it at the bandwidth of each
`--links` entry (`KIB:MS`, default unpaced, 100 Mbit at 5 ms, and 20 Mbit at
50 ms). Each link runs a cold sync into an empty destination and a sync into
the destination that holds the base, and the report gains an "Over the wire"
table with bytes per direction, transport requests and wall time. Every
destination closure must verify complete.

```console
$ benchmark run cdcs-corpus --wire --store-name gmp-6.3.0 --store-name icu4c-76.1
```

## Sliced payload transfer

`sliced_transfer` is the permanent Criterion target for the sliced payload
frame the SSH transfer uses (layout in `crates/casita/src/sync/sliced.rs`).
An 8 MiB blob is rebuilt with a different 32-byte hash every 512 B, 64 KiB
and 1 MiB; the first spacing sits below the 1 KiB discovery chunk, where
nothing can be sliced, the others above it. The codec cases time indexing the
base, encoding the rebuilt blob against it and decoding the frame back, and
every timed iteration is compared with the original bytes outside the timer.
The sync cases move the same blobs through the stdio transfer protocol
between two in-memory repositories with a byte-counting server writer, first
into an empty destination and then into one that already holds the base under
the root being replaced, so the server pairs the two roots and indexes the
base when the rebuilt payload is requested. The cost cases split one rebuild
between the sides against the chunked backend: indexing, server encode,
receiver decode to a sink and into the store, and a plain write baseline. JSON rows report frame and wire bytes, copy and
literal counts, transport requests and wall time; the rebuild rows must copy
at least 95 percent of the blob above the discovery chunk and use under 5
percent of the cold wire bytes, and every destination closure must verify
complete. It is registered under `core-primitives` and runs through
`benchmark all`.

`sliced_pipeline` compares the streaming encoder with encoding a whole
32 MiB literal-only frame before writing it, through a writer paced at 128
MiB/s to 1 GiB/s, which brackets the encoder's own throughput.

```sh
cargo bench --features ssh,experimental --bench sliced_transfer
```

The [chunk slicing investigation](reports/2026-09-16-chunk-slicing/README.md)
retains the synthetic, six-package and whole-closure results, and the wire
results of this target. On a full
NixOS system rebuild (1763 paths, 14.6 GiB) slicing at 1 KiB sampled 1 in 4
stores 1.5 times fewer bytes than the 256 KiB backend because most of the
remaining bytes are new content; on hash-only rebuild pairs it stores 2 to
78 times fewer than 8 KiB exact chunks. Slicing rows model an encoder
that does not exist in Casita; exact rows model the current backend without
pack, catalog, or transfer costs.

## Pipelined tar imports

The permanent `tar_import` Criterion target compares `max_in_flight_files=1`
and `16` on identical archives, using the real chunked payload backend over
an in-memory object store and in-memory metadata. It runs in `core-primitives`
and `benchmark all`. Cases contain 0, 1, 15, 16, 17, and 256 one-KiB files,
four 4-MiB files, or 32 one-KiB files plus four 4-MiB files. The 15/16/17 cases
cover both sides of the default pipeline admission window.

The timed region includes parsing, copying, finalization, verification,
metadata staging, and root publication. Archive and repository construction
are excluded. Every iteration checks the canonical root (including executable
bits), published root, file/entry counts, total logical bytes, and full payload
readback before its sample is accepted. File bytes come from the deterministic
SplitMix64 generator with seed `97 + file_index`.

```sh
devenv shell cargo bench --features experimental --bench tar_import
# Fast correctness-only execution of the same cases:
devenv shell cargo test --features experimental --bench tar_import
```

The importer keeps archive parsing sequential and co-polls body copying with
bounded file finalization. One admission permit follows each file through
copying, queueing, close, and verification. It does not introduce a whole-file
buffer or detached task. Backend buffers and the backend's shared chunk byte
budget still determine the payload working set. This suite measures pipeline
concurrency, not multithreading within one BLAKE3 hash.

The [tar pipeline investigation](reports/2026-09-10-tar-pipeline.md) retains
the concurrency/cancellation checks and exploratory timing data. Use isolated,
repeated measurements before making throughput claims.

## Ingest concurrency

The [filesystem scheduling investigation](reports/2026-09-11-ingest-scheduling/README.md)
records the release baseline, controlled-delay improvement, alternating mixed-size
repeats, correctness checks, and shared-host measurement limits.

`benchmark run ingest-concurrency` is included in `benchmark all`. It sweeps file
limits 1/16/32 and per-writer chunk limits 1/32/64 for tiny files, inputs immediately
below and above the 128 KiB chunker minimum, 20 MiB blobs, and mixed file sizes
(one 4 MiB file followed by fifteen 1 KiB files). Select a subset with `--corpora`.
Every fresh import
is gated by reopened root identity across configurations, exact checkout bytes
and metadata, and fsck. Source preparation, repository initialization, and audits
are outside timing; the suite records CLI wall time, CPU time, peak RSS, binary
identity, commands, and environment. These are warm OS cache measurements.

```sh
devenv shell benchmark run ingest-concurrency --profile smoke --repetitions 1 \
  --output /tmp/ingest-concurrency.json --report /tmp/ingest-concurrency.md
devenv shell benchmark run ingest-concurrency --profile standard --repetitions 3 \
  --file-concurrency 1,8,16,32 --chunk-concurrency 1,16,32,64 \
  --output /tmp/ingest-concurrency-standard.json
```

`benchmark run ingest-scheduling` isolates admission behavior using the real
handle-rooted walker, file reads and hashes. It compares the previous ordered
window with completion-order admission, restoring traversal order within each
page. The uniform control injects no delay. The skewed case waits 20 ms before
each sixteenth file and 1 ms before the others; these are controlled delays,
not a measurement of a particular remote filesystem. Repository publication is
excluded from this fixture and is covered by `ingest-concurrency`.

The default cases include 15/16/17 files around the default concurrency window,
and 1,023/1,024/1,025 files around the walk-page boundary (the root directory also
occupies an entry). Serial admission is a control. Every sample checks exact
post-order paths, bytes via their digests, sizes, active-read bounds, and page
bounds. Both schedulers live in the same release test binary; the ordered
reference is compiled only for tests. Raw fixture output and process RSS are
retained. Both suites are included in `benchmark all`.

```sh
devenv shell benchmark run ingest-scheduling --profile standard --repetitions 3 \
  --output /tmp/ingest-scheduling.json
```

## Native Git ingestion concurrency

The [bounded Git ingestion investigation](reports/2026-09-11-git-ingest/README.md)
retains the original baseline, candidate sweeps, paired comparisons, controlled
upload-latency results and resource-bound checks.

`git-ingest-concurrency` measures fresh durable CLI imports and fast-forward
incremental imports across loose objects, packed objects without deltas, and
delta-heavy packs. It retains the source object-count check, reopened view/ref
identity, exact checkout manifest and fsck gates. Pack caching is disabled so
source representation cannot change the selected view. Source creation, audits
and checkout are outside timing. Sources are warm in the OS cache.

`git-ingest-scheduling` isolates upload latency using the real Git importer and
an instrumented memory payload store, with either zero delay or a controlled
5 ms wait per upload. Both initial and incremental imports must match an
independent `git rev-list`/`git cat-file` inventory and have a complete verified
closure. Every upload checks active-object and byte bounds, including exclusive
oversized uploads. Every checkpoint requires zero active uploads before flushing.
The fixture uses seven-object publication batches to exercise checkpoints while
the configured staging window can be larger than a batch.
Process output, timing, RSS and binary hashes are retained.
The controlled delay does not represent any particular network backend.

Both suites are registered in `manifest.json`, `benchmark all` and revision
comparisons. Standard counts cross the 16-object admission window. Byte budgets
65,535/65,536/65,537 cross the 64 KiB fixture-object size, alongside the default
64 MiB budget. Concurrency 1 is the serial control. A byte budget governs decoded
staging bodies, not Gix caches, decoding workspace, payload-store memory or RSS.

```sh
benchmark run git-ingest-concurrency --profile standard --repetitions 3 \
  --output /tmp/git-ingest.json --report /tmp/git-ingest.md
benchmark run git-ingest-scheduling --profile standard --repetitions 3 \
  --output /tmp/git-scheduling.json --report /tmp/git-scheduling.md
```

`--counts`, `--concurrency`, and `--max-buffered-bytes` select explicit sweeps.
The CLI suite accepts `--layouts loose,packed,delta`; the scheduling fixture
accepts `--layout loose|packed|both` and `--delays-ms 0,5`. To benchmark a CLI
predating these options, use `--omit-limits --concurrency 1` with one byte budget.

## Hash input distributions from committed repositories

`hash_inputs` is a permanent Criterion target in `core-primitives` and
`benchmark all`. Its default deterministic random/zero corpus covers the
same 28 size/flavor combinations as `hash_write_boundaries`, including both
sides of the 128-KiB and 512-KiB writer boundaries. It measures whole-file
BLAKE3 hashing and cold payload writes. Each write checks the full digest,
exact standalone FastCDC chunk IDs/sizes, and complete readback; readback and
reference construction are outside the write timer. Each corpus iteration
starts with an empty chunked in-memory store; duplicate contents within that
iteration can deduplicate.

Set `CASITA_HASH_REPOSITORY` to load every committed regular-file occurrence
at that repository's resolved HEAD, including duplicate contents. Git object
IDs pin the content during the run; working-tree changes are excluded.
Symlinks and submodules are counted as skipped. There is no file sampling or
silent truncation; corpora over 1 GiB of logical file data are rejected. The
benchmark holds the corpus in memory, so allow additional space for the
backend and reference chunks.

```sh
CASITA_HASH_REPOSITORY=/path/to/repo CASITA_HASH_REPORT=/tmp/hash-inputs.json \
  devenv shell cargo bench --features experimental --bench hash_inputs -- --test
# Omit --test to measure the same correctness-gated cases.
```

The JSON records the source commit/tree-listing digest and cumulative count
and byte percentages through 0 B, 64 B, 1 KiB, 16 KiB, 64 KiB, 128 KiB,
256 KiB, 512 KiB, 1 MiB, 4 MiB, and an unbounded final bucket. Empty
populations have null percentages. Whole-file inputs, reference storage
chunks, and predicted independently hashed chunks are separate populations.
Reuse counts are derived from reference chunks and current writer EOF rules,
not runtime call instrumentation. A report requires the cold-write case to
execute successfully.

These distributions describe cold payload writes for the selected snapshots.
They exclude metadata hashes, repository-level verification, readback hashes,
and incremental imports that skip unchanged files. They are not a claim about
all repositories or production traffic.

For repeated tar comparisons, build first, then run the permanent 16-case
matrix using its binary without rebuilding between samples:

```sh
devenv shell cargo bench --features experimental --bench tar_import --no-run
python3 -m benchmarks.tar_compare --binary /path/from/cargo/tar_import-BUILD_ID \
  --output /tmp/tar-paired-new --repetitions 4
# Compare two immutable builds using the same benchmark source and fixtures:
python3 -m benchmarks.tar_compare --baseline-binary /path/to/baseline-tar-import \
  --binary /path/to/candidate-tar-import --output /tmp/tar-versions-new --repetitions 4
```

The runner alternates concurrency order 1/16 and 16/1, uses a fresh Criterion
directory per repetition, checks binary SHA-256 before/after, retains raw
samples and estimates, and requires all 16 cases. It waits for ten quiet
seconds and monitors external CPU/compiler activity throughout each matrix
with the existing Linux `/proc` guard. Use `--max-external-cpu-percent 30`
to permit up to 30% sampled background CPU across all logical CPUs instead
of the default 5%. The selected limit applies to admission and measurement
and is retained in the report. Add `--allow-competing-builds` to record
compiler/benchmark processes without vetoing them separately; the CPU limit
still applies to every measured interval. For shared-host timing with a 30%
budget, use both options. Otherwise active compiler/benchmark processes still
veto a run. A contended run or quiet-wait timeout
is retained and returns failure; only a complete set of accepted repetitions
supports comparison. Sampled quietness is not proof of physical host isolation.
With `--baseline-binary`, it also alternates baseline/candidate execution order,
uses a separate Criterion directory per variant, and requires both matrices
in every pair to pass. Only a complete accepted set produces per-case paired
candidate/baseline time ratios; values below one favor the candidate. The
report retains each pair and its median ratio, without asserting statistical
significance from a small number of repetitions. Scoped profiling environment
variables are cleared so they cannot alter a throughput comparison.
The matrix's archive-root, counts, bytes, and full-readback correctness gates
remain active. `CASITA_TAR_REVERSE=1` also reverses order when invoking the
benchmark directly.

The [2026-09-11 hash-input investigation](reports/2026-09-11-hash-inputs.md)
retains correctness-gated distributions for committed Casita, devenv, and
nixpkgs snapshots, including the different conclusions from count and byte
shares.
The [2026-09-11 tar quiet-host report](reports/2026-09-11-tar-quiet.md) retains
two rejected timing attempts and the retry's activity samples; no new speedup
claim was made on the busy workstation.
The [current-main tar retry](reports/2026-09-11-tar-current.md) passed all 16
release correctness cases but again failed its 180-second quiet-host wait;
it provides no accepted timing baseline.
The [September 12 retry](reports/2026-09-12-tar-current.md) completed all 16
cases, but a new Clippy build overlapped measurement and invalidated the timing.
The [post-publication baseline attempt](reports/2026-09-12-tar-baseline.md)
completed all 16 cases on `f0b369b`, but background CPU and compiler activity
again caused the timing guard to reject the matrix.
The [build-suspension attempt](reports/2026-09-12-tar-reserved/README.md)
retains explicit pause/resume evidence and the stopped-process guard fix.
All 16 cases passed, but desktop CPU bursts and new build launches invalidated
the timing. Its optional controller requires permission to pause other builds.
The [30% CPU-budget attempt](reports/2026-09-13-tar-cpu30/README.md)
records the relaxed shared-host policy and its admission result.
The [30% retry](reports/2026-09-13-tar-cpu30-retry/README.md) completed all
64 case executions: three matrices passed, while the fourth exceeded the CPU
limit. Its accepted measurements and rejected matrix are retained separately.
The [FastCDC 3 versus 5 comparison](reports/2026-09-11-fastcdc5-comparison.md)
retains matched release builds, their complete lockfiles and shared harness,
correctness results, and two rejected admission attempts. No version speedup
is established by these attempts.

## Blob verification scratch buffers

`optimization` includes `verification_buffers/{known,unknown}/{size}` for
0, 1, 1,024, 16,384, 65,535, 65,536, 65,537, and 262,144-byte payloads. These
cases cover both sides of the default 64 KiB scratch-buffer cap and are part
of `benchmark all`. Each iteration verifies the complete payload identity,
size, EOF, and configured scratch bound. Preflight JSON lines record the
largest buffer supplied to the reader; this is a scratch-size metric, not
total process memory.

```sh
devenv shell cargo bench --features experimental --bench optimization -- verification_buffers
# Correctness and deterministic scratch sizes, without collecting timings:
devenv shell cargo bench --features experimental --bench optimization -- --test verification_buffers
```

Known short payloads need only their own length in scratch space. Unknown
lengths retain the configured capacity. A length hint changes allocation only:
verification still consumes EOF, hashes every byte, and enforces payload limits.
The [verification buffer report](reports/2026-09-11-verification-buffers.md)
retains baseline/candidate scratch sizes and validation results.

## Native Git import phase profiling

`git-import-profile` runs the permanent `git-scale` many-object, delta-heavy,
and wide-tree workloads through the production local importer. Test-only clocks
measure source headers and decoding, staging, checkpoints, root publication,
compaction, and entry into the mutation session. Publication has separate
coordination, validation, payload and metadata counters. The defaults remain
16 active objects and 64 MiB of decoded staging bodies; source pack caching is
disabled. Explicit source repacking exercises delta chains.

Each initial, unchanged and incremental import runs in a fresh process against
the same destination. Import time excludes source generation, repository open,
and correctness auditing. Linux peak RSS is captured immediately after import,
before loading the independent expected inventory and auditing the closure;
it includes repository opening. Verification and upload durations are summed
across concurrent operations and overlap staging waits. They must not be added
to the other phase durations to reconstruct wall time.

Every sample requires an exact independent `git rev-list`/`git cat-file`
inventory, the expected main ref, the persisted view identity, and a complete
verified closure. Sources pass `git fsck --full --strict`. Raw process output,
phase counters, source statistics, environment and binary hashes are retained.
The suite is registered in `manifest.json`, `benchmark all` and revision builds.
Use a persistent fixture directory to reuse identical sources between binaries:

```sh
benchmark run git-import-profile --profile standard --repetitions 3 \
  --fixture-root /tmp/casita-git-profile-fixtures \
  --output /tmp/git-import-profile.json
```

`--shapes` selects a comma-separated subset. `--probe-binary PATH --no-build`
uses an existing release `cli,git` libtest binary containing the profiling probe.
Run builds and fixture generation before timing when comparing binaries, and
record shared-host or storage-pressure conditions with `--measurement-note`.

`pin-growth` isolates the durable staging-pin update underneath the importer.
It seeds one pin outside timing and measures sequential additions to that same
pin at 1, 8,192 and 16,384 retained resources. Fixed-width paths put the latter
two cases on opposite sides of the 1 MiB journal window. Both smoke and standard
profiles retain all three sizes; standard takes more additions per sample.
Standard measures 1,024 additions, crossing multiple periodic checkpoints; smoke
measures two. Resource-addition frames avoid re-encoding the retained pin.
The `ledger-boundaries` suite also retains catalog sizes just below, at and
above the former 1 MiB single-record cliff, with exact write-volume gates.
After timing, each case discards the in-memory cache, replays the exact resource
set from disk, checks that protected deletion is rejected, and confirms release
permits deletion without leaving pins or claims. Journal byte, checkpoint and
sync counters accompany the timings. It is also included in `benchmark all`.

```sh
benchmark run pin-growth --profile standard --repetitions 3 \
  --output /tmp/pin-growth.json
```

## Bounded chunk decompression

The `optimization` target includes 108 `chunk_decompression` cases comparing
the former streaming chunk decoder with the shared bounded reusable codec.
Random/text inputs cover both sides of 64 KiB and 128 KiB, plus 1 KiB,
256 KiB, and 512 KiB. Sized, unsized, and concatenated frames exercise the
bulk path and legacy streaming fallback. All cases run in `benchmark all`.
Each iteration checks complete decoded bytes outside its decode timer;
preflight also checks undersized output limits and truncated frames. JSON
records output Vec capacity, which is not total allocation or process memory.
`CASITA_CHUNK_DECODE_REVERSE=1` reverses decoder order for repeated comparisons;
`benchmark all` clears that ambient override.

The [September 13 decoder investigation](reports/2026-09-13-chunk-decode/README.md)
records passing correctness checks and reduced output Vec capacity. Timing
retries produced one accepted decoder matrix, but the repeat exceeded the
30% background CPU limit. Repeated and end-to-end speedups remain unverified.

```sh
cargo bench --features experimental --bench optimization -- chunk_decompression
cargo bench --features experimental --bench optimization -- --test chunk_decompression
```

## Compression scheduling

`compression_handoff` compares the current blocking-pool handoff with inline
compression and a variant that yields before inlining chunks up to 4 KiB.
It uses the production codec source and covers random/text chunks, sizes on
both sides of 4 KiB, concurrency 1/16, and current-thread/four-worker runtimes.
All 144 cases run through `benchmark all`, checking frame lengths and complete
roundtrips for every indexed chunk.

```sh
devenv shell cargo bench --features experimental --bench compression_handoff
```

Before Criterion measurements, each configuration emits four paired JSON
diagnostics with a competing ready task, reversing variant order between pairs.
The diagnostics measure batch completion, longest compression-task poll, and
longest ready-task scheduling gap. Criterion times batches without that task.
Even `--test` runs the bounded diagnostic matrix and all correctness gates.

The [compression scheduling investigation](reports/2026-09-11-compression-handoff/README.md)
retains the fairness tradeoff, a yielding experiment, identical-path controls,
host activity, and reproducible commands. Production scheduling remains unchanged.


## Scoped tar CPU profiling

The tar benchmark supports acknowledged perf FIFO control through
`CASITA_BENCH_PERF_CONTROL` and `CASITA_BENCH_PERF_ACK`. Without those variables
it follows the normal benchmark path. The permanent profile runner prepares
the pipes and runs the small-256, large, and mixed cases at concurrency 1/16:

```sh
python3 -m benchmarks.tar_profile --binary /path/to/tar_import-BUILD_ID \
  --perf /path/to/perf --output /tmp/tar-profile-new --seconds 10
```

Events are enabled only around `TarImport::import`, including importer
verification/publication, and disabled during fixture creation and benchmark
readback. The runner preserves correctness gates, executable fingerprints,
raw recordings, flat symbol reports, and host-activity samples. It rejects
empty profiles and reports with fewer than 100 approximate samples. The cases
remain in the permanent tar matrix and `benchmark all`; profiling is an
optional diagnostic invocation of those same cases.

The [September 13 profiles](reports/2026-09-13-tar-profile/README.md) retain
large/mixed hashing and memory costs, resolved copy caller edges, and the
bounded chunk-decoder reuse candidate. Background load exceeded 30% in five
cases, so the evidence is diagnostic CPU attribution rather than timing.

The [2026-09-11 scoped tar profiles](reports/2026-09-11-tar-profile.md) show
roughly 5–7% BLAKE3 self CPU for tiny files versus 31–35% for large/mixed
archives. They were collected under contention and do not establish a
throughput improvement. Assembly unwinding limits exact hash-call attribution.

## Filesystem read batching

`cargo bench --features experimental --bench filesystem_import` measures fresh
filesystem import into memory storage, including publication. It is registered
in `core-primitives` and runs in `benchmark all`. Cases use empty and 1 KiB
files plus 256 KiB − 1, 256 KiB, 256 KiB + 1 and 1 MiB files, at concurrency
1 and 16. Every iteration checks the independently constructed canonical root,
executable bits and complete payload readback. Fixture setup, validation and
repository destruction are outside the timed import. See the
[small-file investigation](reports/2026-09-13-small-file-import/README.md) for
results and the unpromoted spike; production import remains streamed.
## Catalog WAL investigation

`benchmark run catalog-wal --profile standard --repetitions 3 --output /tmp/catalog-wal.json`
measures SQLite WAL file length using production catalog encodings and the native
Turso metadata backend. The permanent suite is also included in `benchmark all`.
It covers both sides of the 4 MiB inline checkpoint and 1 MiB inline delta limits,
catalog resubmission and metadata-only commits, and retained/unretained readers.
A 48-byte reference is a SQL-only control, not an implemented storage protocol.
The `external` mode uses the production 56-byte reference and includes durable
root-object publication on each commit, plus authentication of the reopened root.
Every process verifies catalog membership, the original retained snapshot, the
reopened catalog/revision/generation, and successful WAL truncation after release.
The end-of-run passive checkpoint records busy/log/checkpointed frame counts.
WAL file length measures footprint, not cumulative writes or physical allocation;
timing includes commits and file stat calls but excludes fixture setup and audits.

## Native Rust FSKit feasibility

`benchmark run native-fskit` compares native Rust FSKit with host filesystem
reads on macOS 15.4+. It is included in `benchmark all`; Linux records a skip.
`native-fskit-portable` uses identical portable names; the raw-name suite keeps
the native byte-name gate and records APFS exclusions. Preparation and activation
are required on first use. See [current commands](../crates/casita-fskit/BENCHMARKS.md#memory-transport).

`benchmark run native-fskit-repository` compares a real Casita snapshot through
native FSKit with host reads (macOS 26+). It retains first-touch counters,
execution, publication, sandbox, and lifecycle gates. See
[reproduction](../crates/casita-fskit/BENCHMARKS.md#repository-workloads). Historical comparisons
remain in reports; the removed adapter and its installer are no longer runnable.

`benchmark run native-fskit-launch` isolates launch latency over the same real
repository. It includes a host control, direct/interpreted scripts, blocking/timed
waits, observed `posix_spawn`, spawn/wait phases and per-launch storage counters.
`native-fskit-launch-uncached` disables the native immutable-directory cache in
the same binary. Both are included in `benchmark all`.
See [launch findings and reproduction](reports/2026-09-15-native-fskit-launch/README.md).

The launch suite also includes `F_GETPATH`, open/close, libc `realpath`, name/full-path
attribute queries, code-signing object creation and extended-attribute controls.
It now includes timed directory listings with an independent name/count oracle
and public CoreFoundation bundle discovery. The native prototype defaults to
one-second-after-epoch immutable timestamps.
`native-fskit-launch-zero-times` retains the old zero-timestamp regression;
`--item-timestamps store|zero` is supported by launch and density runners.
Both are in `benchmark all`, and the harness checks VFS-visible timestamps.
See [the timestamp fix and directory controls](reports/2026-09-18-native-fskit-directory-controls/README.md).
`native-fskit-first-launch` measures setup, optional preparation, first execution
and immediate second execution on fresh mounts or fresh host file identities.
It retains all correctness/lifecycle gates and includes direct 15/16/17-file
reader-cache pressure cases. `native-fskit-first-launch-uncached` is its
same-binary baseline with native reader reuse disabled. Both are in
`benchmark all`; `--reader-cache enabled|disabled` is also supported by ordinary
launch and density runners. See [first-launch results](reports/2026-09-18-native-fskit-first-launch/README.md).
`native-fskit-workloads` runs relocated GNU awk, sort and gzip executables with
checked stdin-processing results, at 1/8/15/16/17/31/32/33 concurrent processes.
`native-fskit-workloads-readers-16` permanently runs the old-limit control.
`--reader-cache-capacity 16` compares the old reader limit with the default 32;
`--workload-workers 17` selects a focused matrix while preserving the full fixture. The suite measures
first and immediate repeat batches on fresh mounts, using shared and distinct
executable paths. `native-fskit-workloads-uncached` disables native reader reuse.
These cases are in `benchmark all`; smoke retains every concurrency boundary with one
repetition. Set `CASITA_WORKLOAD_AWK`, `CASITA_WORKLOAD_SORT` and
`CASITA_WORKLOAD_GZIP` to executable binaries, including the underlying binary
when a package supplies a wrapper script. The runner retains tool and payload
hashes and validates copied programs before importing the fixture.
See [real-tool and concurrency results](reports/2026-09-18-native-fskit-workloads/README.md).
`native-fskit-workloads-read-trace` adds opt-in native read-range diagnostics to
the same matrix and is included in `benchmark all`. It records calls, returned
bytes, service time and errors by inode/offset/request size, capped at 8,192
unique ranges per mount. Traced workload gates reject truncated traces or missing
callbacks. Completed FSKit read callbacks also record backend, buffer-copy and
reply-call durations. Diagnostic-file reads are excluded; final unmount gates
require exact callback coverage. These service-time sums can overlap across
threads and do not partition launch wall time. They exclude framework dispatch,
argument validation and callback accounting. Timing includes instrumentation; use the ordinary workload case as
the control. See [read-range analysis](reports/2026-09-18-native-fskit-read-ranges/README.md)
and [FSKit callback phase results](reports/2026-09-19-fskit-callbacks/README.md).
`native-fskit-launch-eager` retains unnecessary enumeration attribute
allocation; `native-fskit-launch-density` varies metadata sibling counts.
`native-fskit-launch-enumeration-uncached` and
`native-fskit-launch-density-enumeration-uncached` disable shared immutable entry
vectors while keeping the older repository metadata cache enabled. These are
same-binary controls included in `benchmark all`.
See [entry-vector cache results](reports/2026-09-16-native-fskit-entry-cache/README.md).
`native-fskit-launch-density-phases` enables per-entry filename construction,
packing and release timers across the same directory sizes. Ordinary launch
runs keep those timers disabled. Both modes retain correctness/lifecycle gates;
the phase suite is registered in `benchmark all`.
See [filename and packing phase results](reports/2026-09-18-native-fskit-phases/README.md).
`native-fskit-launch-profile` takes a rootless stack sample of the benchmark's
native extension during the permanent launch workload. It retains result and
lifecycle gates, writes adjacent `.stacks.txt` and `.sample.log` artifacts, and
is included in `benchmark all`. Its timings are diagnostic because sampling
perturbs execution. Use `--sample-extension-seconds 0` to disable sampling.
See [native packer stack findings](reports/2026-09-18-native-fskit-packer/README.md).
`native-fskit-launch-density-filename-bytes` selects direct byte-based
`FSFileName` construction during native enumeration. The normal density suite
retains the temporary-`NSData` path. Both constructors run byte-preservation and
copy-ownership checks before mounting, including invalid UTF-8 and a 255-byte
name. Both modes are in `benchmark all`; `--filename-construction data|bytes`
selects a same-binary control.
See [direct-byte constructor results](reports/2026-09-18-native-fskit-filenames/README.md).
`native-fskit-launch-capabilities` tests explicit volume capability declarations
at 512 metadata siblings and verifies their VFS-visible bits before timing.
The default remains minimal; `--volume-capabilities minimal|explicit` selects
the same-binary control in launch and density runners. The case is included in
`benchmark all`. See [capability results](reports/2026-09-18-native-fskit-capabilities/README.md).
`native-fskit-launch-explicit-xattrs` and
`native-fskit-launch-density-explicit-xattrs` select the opt-in empty-xattr
experiment. All are in `benchmark all`; see
[enumeration, xattr and directory-density results](reports/2026-09-16-native-fskit-enumeration/README.md).
`--host-only` is a harness control and never substitutes for native measurements.
`--baseline-server` optionally adds the production Casita transport, which is
excluded from controlled ratios because it uses repository storage. Reports
pair matching cases and record caching policies, runtime hashes, and run order.

### Decoded-chunk reuse under seeks

`decoded-seek-replay` replays the measured extra FSKit launch ranges through the
production packed reader, with decoded caching disabled and enabled in the same
binary. It separately measures compressed-frame fetch, decoding admission and
decode/verification time. It includes sequential/one-seek/two-seek controls,
2 MiB byte-capacity boundaries, 63/64/65-chunk entry-capacity boundaries,
and 15/16/17 simultaneously retained readers around the 32 MiB shared cap.
Admission keeps chunks until repeated missed seek targets account for at least
the retained set's decoded size without a cache hit. Hits reset this evidence,
and read-ahead never contributes. Reads never wait for cache space.
Phase-changing cases fill the cache with an initial chunk just below, at, or above
2 MiB, then measure repeated seeks through a disjoint working set. Cases also
cover the 64-target miss-history limit, the demand-byte threshold, and doubled
reads in a 65-chunk cyclic scan.
Every returned byte, resident-cache bound and reader release is checked.
The case is included in `benchmark all` and revision comparisons. Synthetic
inputs are portable; `--input` accepts the hash-matched measured GNU awk binary.
See [decoded seek replay](reports/2026-09-18-decoded-seek-replay/README.md) and
[stable admission and shared memory bounds](reports/2026-09-19-stable-decoded-cache/README.md),
[remaining launch costs and phase changes](reports/2026-09-19-launch-service-phases/README.md),
and [adaptive admission and reader capacity](reports/2026-09-19-adaptive-cache/README.md).

## Multiple filesystem outputs

`benchmark run filesystem-outputs --profile standard --output results.json`
compares separate filesystem imports inside one mutation session with one
multi-root import inside one mutation session. Each sample uses a fresh persistent
local repository. The default matrix covers 2, 8 and 32 outputs with distinct
4 KiB files, nested and empty directories, executable bits, and an escaping
symlink whose text must remain unchanged on Unix. Fixture preparation, independent
tree-digest checks, exact named-root checks, payload reads, and `fsck` are outside
timing. Both modes reread source files.

The result records discovery, traversal plus staging, publication, maintenance,
and total elapsed time. Discovery is a subset of traversal plus staging and
includes blocking-pool scheduling. Shared pages reduce dispatches and permit
file ingestion across output boundaries; every directory entry still must be
visited. `walks` counts traversal drivers, not directories read. Publication
counts include bounded checkpoints and the final root publication.

Smoke includes 1, 27, 28, 29 and 30 files per output, bracketing the 1,024-entry
page boundary for 32 outputs on Unix and Windows. Standard adds 123, 125 and 126
files, bracketing the 4,096-object publication batch boundary at 32 outputs.
The benchmark asserts exact page and publication counts as well as correctness.
It is registered in `benchmark all` and revision comparisons.

```console
benchmark run filesystem-outputs --profile standard --repetitions 5 --output results.json --report results.md
benchmark all --suites filesystem-outputs --repetitions 1 --output /tmp/filesystem-outputs
```

Use `--counts`, `--files`, and `--sizes` to select a matrix. `--probe-binary`
accepts a prebuilt release library test executable; add `--no-build` to require
it. These synthetic Casita timings do not measure Obrador end-to-end build time.

## Casitar record protection and destination reuse

The permanent Casitar scaling corpus measures archive creation, inspection, and
imports into empty, half-seeded, and fully seeded repositories. Every import
checks exact reuse counters, archive identity, root preservation, full checkout
contents and executable bits, and fsck. Corrupted incoming bytes must fail even
when the destination already has their payload.

All four runners participate in `benchmark all`:

- `casitar-scaling`: payload sizes around 64 KiB and object-count scaling.
- `casitar-import-profile`: opt-in import phase timings.
- `casitar-pin-profile`: pin-journal counts with a bounded append budget.
- `casitar-quiet-import`: paired latency measurements without phase timing.

The object-count cases include 254 and 256 files, giving 255 and 257 records
including the directory, on opposite sides of the 256-record publication batch.
The standard import matrix also includes 4,096 files.

```sh
benchmark run casitar-pin-profile --profile smoke --repetitions 1 \
  --casita-bin /absolute/candidate --baseline-bin /absolute/baseline \
  --output /absolute/boundary.json
benchmark run casitar-quiet-import --profile standard --file-counts 4096 \
  --repetitions 4 --max-external-cpu-percent 40 \
  --casita-bin /absolute/candidate --baseline-bin /absolute/baseline \
  --output /absolute/paired.json
```

The latency runner alternates binary and case order. It requires ten consecutive
seconds under the external CPU ceiling before each case and samples activity
through setup, timing, and verification. The default ceiling is 40% across all
logical CPUs. Compiler activity is recorded without a separate rejection rule.
This measures a shared host, with no disk-isolation claim. Incomplete or rejected
runs retain their evidence and cannot be normalized as completed comparisons.

See [the current-main comparison](reports/casitar-current-main-20260925/README.md)
for exact source, binary, dependency, and result provenance.

## Named-root prefix reads

The [root-prefix](root-prefix.md) suite checks indexed named-root ranges at
255 and 257 matches, plus sparse and dense prefixes in a 4,096-root store.
It is registered in `manifest.json` and included in `benchmark all`.
