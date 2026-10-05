# Chunk hash batching corpus

`chunk-hash-batch` measures `ChunkedBlobStore::put_slice` for an empty destination
and then the same content in the same destination. The duplicate measurement
follows a complete cold-write audit. Both memory and local object stores are
available. Fixture construction, ordinary BLAKE3 hashing, reference FastCDC,
and full verified readback are outside the timed interval. Fixtures occupy a
whole payload buffer, matching the existing decoded Git blob staging path;
process memory measurements do not isolate writer memory.

Every sample requires the ordinary BLAKE3 blob identity, exact reference chunk
boundaries and hashes, exhaustive verified readback, and a real object-store
counter proving zero chunk writes on the duplicate pass. Paired variants must
produce identical ordered chunk metadata. The reported `manifest_hash` hashes
that canonical metadata, rather than the on-disk manifest encoding.

The permanent standard cases include upload concurrency 1/3/4/5/16, memory for
one/three/four 64 KiB admission units, a 1 KiB many-chunk configuration, the
256 KiB default average, periodic duplicate-heavy input, sizes immediately
below/at/above the small-file cutoff, average chunk sizes below/at/above 512 KiB
(where maximum chunks cross the 1 MiB job cap), oversized chunks, and
single-upload writes whose chunks fall below, across and above the 4 KiB bound
under which a group holding one chunk is hashed without a blocking job.
Actual content-defined cuts need not hit the cap exactly; unit tests cover
exact group-byte and group-count boundaries independently.

Fresh measurements on October 4, 2026 compare the current hash-batching implementation
with its parent commit and with a grouping-only control. Both comparisons use
the memory backend and four pinned logical CPUs (16-19). Each table reports
the median paired time reduction; positive means faster, negative means slower.
Parentheses count faster pairs over total pairs. These are measurements on one
host, not a guarantee across workloads; full paired ranges are in the reports.

The [batching comparison](reports/2026-10-04-chunk-writer/hash-batch.json)
uses nine alternating pairs per case against per-chunk blocking hash jobs.
All 252 samples are retained, including regressions.

| Case | Cold reduction | Duplicate reduction |
|---|---:|---:|
| `many-1` | +24.3% (9/9) | +47.7% (9/9) |
| `many-4` | +3.2% (6/9) | +26.4% (9/9) |
| `many-16` | -0.5% (3/9) | +17.1% (9/9) |
| `one-permit` | +24.3% (9/9) | +47.8% (9/9) |
| `default-4` | -4.5% (3/9) | +1.0% (6/9) |
| `default-16` | -3.7% (1/9) | -1.1% (2/9) |
| `inline-2048` | +20.8% (9/9) | +38.7% (9/9) |

The gains are concentrated in small-chunk duplicate writes. Default-size cold
writes were 4.5% and 3.7% slower at the median for concurrency 4 and 16,
respectively; these measurements do not establish a universal speedup.

The [inline comparison](reports/2026-10-04-chunk-writer/hash-inline.json)
uses fifteen alternating pairs per case against the same current code with
only `INLINE_BYTES` changed from `4 * 1024` to `0`, keeping grouping and storage
operations intact. All 360 samples are retained.

| Case | Cold reduction | Duplicate reduction |
|---|---:|---:|
| `many-1` | +26.2% (15/15) | +47.8% (15/15) |
| `one-permit` | +25.7% (15/15) | +48.7% (15/15) |
| `inline-2048` | +21.0% (15/15) | +39.4% (15/15) |
| `inline-4096` | +3.7% (15/15) | +7.4% (15/15) |
| `inline-8196` | +0.5% (12/15) | +0.1% (8/15) |
| `many-16` | -1.8% (6/15) | +16.5% (15/15) |

`8196` intentionally sets the rounded minimum normal chunk size to 4098 bytes,
above the inclusive 4096-byte inline bound; an EOF tail can still be shorter.

Reports include executable, fixture, lockfile, and source fingerprints;
compiler versions and flags; the complete grouping-only patch; process
outcomes; and individual timings. The parent uses the current fixture and
lockfile. Builds used one source path and a shared Cargo target directory,
with `cargo clean -p casita --release` before each variant and each executable
frozen immediately afterward. All measurements ran after the builds finished.
The recorded source revision precedes the documentation/report amendment;
the candidate's recorded Casita source tree and fixture match the amended commit.
Repeated stdout and empty stderr were omitted after validating all samples
against the successful probe output. Temporary executable paths and the host
name were removed; all timings, RSS values, and paired summaries are retained.

```sh
python3 -m benchmarks.suites.chunk_hash_batch --profile smoke --output hash-smoke.json
python3 -m benchmarks.all --suites chunk-hash-batch --profile smoke
```

For comparisons, use identical fixtures and lockfiles, matching compilers,
features, and flags. To reproduce these controls, use the hash-batching commit
for the candidate and its parent for the per-chunk baseline. Copy the candidate's
`crates/casita/tests/chunk_hash_batch.rs` into the parent snapshot. For the
grouping-only control, use the candidate snapshot and change only
`pub(super) const INLINE_BYTES: usize = 4 * 1024;` to
`pub(super) const INLINE_BYTES: usize = 0;` in
`crates/casita/src/blob/chunked/hash_batch.rs`. Preserve that patch and restore
the unmodified candidate before its build.

With a shared target directory, replace the source snapshot at the same path
between variants and clean Casita's release artifacts before each build.
Freeze the executable identified by Cargo's `compiler-artifact` JSON before
any subsequent Cargo command. Alternatively, use separate source checkouts and
separate target directories. Retain executable hashes and `.build.json`
fingerprints alongside the frozen binaries, as recorded in the reports.

```sh
cargo clean -p casita --release
cargo test --offline --locked --release -p casita --no-default-features \
  --features native,experimental --test chunk_hash_batch --no-run --message-format=json
# Freeze the reported executable; repeat for the other source variants.
python3 -m benchmarks.suites.chunk_hash_batch --profile standard \
  --baseline-binary /path/to/baseline --probe-binary /path/to/candidate --no-build \
  --repetitions 5 --cpu-affinity 0,1,2,3 --output hash-paired.json
```

`--cases many-4,default-4` selects named cases from the suite's `CASES` map.
Each process emits cold and duplicate samples. Variant order alternates per
repetition. The runner reports paired reductions for each phase separately;
one smoke repetition is correctness evidence, not a performance conclusion.

The retained matrices can be repeated with the corresponding
per-chunk, grouping-only, and inline-enabled executables. CPU IDs are host
specific; these runs used 16-19. Each baseline and candidate must use the same
fixture and lockfile. The grouping-only control disables lone-chunk inlining
while retaining grouping and every storage operation.

```sh
python3 -m benchmarks.suites.chunk_hash_batch \
  --cases many-1,many-4,many-16,one-permit,default-4,default-16,inline-2048 \
  --backend memory --repetitions 9 --cpu-affinity 16,17,18,19 \
  --baseline-binary /path/to/per-chunk --probe-binary /path/to/inline-enabled \
  --no-build --output hash-batch-paired.json
python3 -m benchmarks.suites.chunk_hash_batch \
  --cases many-1,one-permit,inline-2048,inline-4096,inline-8196,many-16 \
  --backend memory --repetitions 15 --cpu-affinity 16,17,18,19 \
  --baseline-binary /path/to/grouping-only --probe-binary /path/to/inline-enabled \
  --no-build --output hash-inline-paired.json
```

## Production-default validation

The `default-4` and `default-16` cases above use the default average chunk size,
but override upload concurrency and memory admission. The permanent
`production-default` cases use all three production settings: a 256 KiB average,
32 concurrent uploads, and a 64 MiB shared chunk budget. They cover 16 MiB and
64 MiB random payloads, plus a 16 MiB periodic payload. The 16 MiB random case
also runs in the smoke profile, including `benchmark all`.

The [production-default report](reports/2026-10-04-chunk-writer/hash-production-defaults.json)
compares commit 6 against its parent with fifteen alternating pairs per case,
on CPUs 16-19. The local object store uses a ZFS-backed temporary directory,
not tmpfs. All 360 samples and the original build fingerprints are retained.
The fixture and production code are unchanged from the binaries used above;
the newly registered cases only change their runtime parameters.

| Case | Backend | Cold reduction | Duplicate reduction |
|---|---|---:|---:|
| `production-default` | memory | +1.6% (8/15) | +1.0% (8/15) |
| `production-default` | local | -0.0% (7/15) | +2.2% (9/15) |
| `production-default-large` | memory | +0.3% (8/15) | -0.3% (7/15) |
| `production-default-large` | local | +5.3% (9/15) | +2.4% (8/15) |
| `production-default-periodic` | memory | -0.4% (7/15) | +3.1% (12/15) |
| `production-default-periodic` | local | +2.2% (10/15) | +2.0% (12/15) |

Positive percentages mean less time, with faster-pair counts in parentheses.
These runs do not reproduce the 3-5% cold-write slowdown at the actual production
settings. Several cases have nearly even faster/slower pair counts and wide
ranges, so small median differences do not establish a stable default speedup
or prove the absence of a regression on another workload. The concurrency-4/16
costs above remain part of the measured tradeoff.

## Smaller hash-job experiment

To test whether serializing larger hashes explains those costs, an isolated
control changed only `MAX_BYTES` in `hash_batch.rs` from `1024 * 1024` to
`64 * 1024`. This makes chunks at least 64 KiB run alone, while preserving the
four-chunk limit and lone-chunk inlining through 4 KiB. The exact patch is in the
candidate build fingerprints. This control is not the production implementation.

The [default-setting comparison](reports/2026-10-04-chunk-writer/hash-small-jobs-defaults.json)
contains 360 samples; the [small-chunk and boundary comparison](reports/2026-10-04-chunk-writer/hash-small-jobs-boundaries.json)
contains 540. Both compare the 64 KiB control with the current 1 MiB policy,
using fifteen alternating pairs per case. Every sample passed the same identity,
reference chunking, verified readback, and duplicate-write correctness gates.

| Case | Backend | Cold reduction | Duplicate reduction |
|---|---|---:|---:|
| `production-default` | memory | +0.5% (8/15) | +0.4% (8/15) |
| `production-default` | local | -1.8% (7/15) | +4.0% (10/15) |
| `production-default-large` | memory | +10.9% (12/15) | -0.8% (7/15) |
| `production-default-large` | local | -1.0% (7/15) | +0.1% (9/15) |
| `production-default-periodic` | memory | -2.6% (6/15) | +0.9% (10/15) |
| `production-default-periodic` | local | +1.7% (9/15) | +1.2% (10/15) |

| Case | Backend | Cold reduction | Duplicate reduction |
|---|---|---:|---:|
| `many-1` | memory | +1.3% (8/15) | -0.6% (6/15) |
| `many-4` | memory | -8.0% (3/15) | +0.2% (8/15) |
| `many-16` | memory | -7.6% (6/15) | +0.4% (8/15) |
| `one-permit` | memory | +1.0% (11/15) | +0.4% (10/15) |
| `default-4` | memory | +4.3% (9/15) | +6.5% (9/15) |
| `default-16` | memory | -4.2% (6/15) | -2.3% (7/15) |
| `small-job-cap--2` | memory | -5.0% (7/15) | -2.3% (4/15) |
| `small-job-cap-+0` | memory | -3.1% (4/15) | -2.4% (7/15) |
| `small-job-cap-+2` | memory | -5.1% (3/15) | -4.7% (2/15) |

The `small-job-cap` cases use averages 32766, 32768, and 32770 bytes, so their
maximum chunk sizes cross the proposed 64 KiB cap. They remain permanent
standard-profile cases even though the experiment was not adopted.

**Decision: retain the 1 MiB production cap.** The smaller cap improved the
64 MiB in-memory cold-write case, but did not consistently improve local writes
or recover the concurrency-16 cost. Boundary cases regressed, and several
paired ranges were large. These observations do not justify replacing the
current policy or attributing the earlier slowdown solely to serialized hashes.
Keeping the existing policy preserves the demonstrated small-chunk gains while
making its measured tradeoffs explicit; it is not a universal-speedup claim.

To reproduce, freeze the parent, current, and 64 KiB-control executables using
the identical fixture, lockfile, compiler and release flags described above.
Apply the one-constant control patch only to its source snapshot. Use a
disk-backed directory for `TMPDIR` when measuring local storage, and record its
filesystem. No builds should overlap timed runs.

```sh
TMPDIR=/path/to/disk-backed-tmp python3 -m benchmarks.suites.chunk_hash_batch \
  --cases production-default,production-default-large,production-default-periodic \
  --backend both --repetitions 15 --cpu-affinity 16,17,18,19 \
  --baseline-binary /path/to/per-chunk --probe-binary /path/to/inline-enabled \
  --no-build --output production-defaults.json
TMPDIR=/path/to/disk-backed-tmp python3 -m benchmarks.suites.chunk_hash_batch \
  --cases production-default,production-default-large,production-default-periodic \
  --backend both --repetitions 15 --cpu-affinity 16,17,18,19 \
  --baseline-binary /path/to/inline-enabled --probe-binary /path/to/small-jobs \
  --no-build --output small-jobs-defaults.json
TMPDIR=/path/to/disk-backed-tmp python3 -m benchmarks.suites.chunk_hash_batch \
  --cases many-1,many-4,many-16,one-permit,default-4,default-16,small-job-cap--2,small-job-cap-+0,small-job-cap-+2 \
  --backend memory --repetitions 15 --cpu-affinity 16,17,18,19 \
  --baseline-binary /path/to/inline-enabled --probe-binary /path/to/small-jobs \
  --no-build --output small-jobs-boundaries.json
```
