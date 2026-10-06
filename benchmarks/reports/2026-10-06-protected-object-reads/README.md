# Protected object reads

`object_reads` reads a working set of locally stored blobs through a held
`RetainedReader` snapshot and through readers that do not keep a metadata
snapshot. Each pass opens a verified reader for every key and reads it to the
end, checking the exact bytes. All strategies run in the same process on the
same repository, so the snapshot strategy is a control for host drift.

## ObjectReader

`ObjectReader` looks up each object with its own short metadata read instead
of the held snapshot. Times are milliseconds per 128-object pass, the median of
six processes' Criterion medians; ratios are object reader over snapshot
within each process.

| Payload bytes | Snapshot | ObjectReader | Per-process ratio |
|---:|---:|---:|---|
| 128 | 14.97 | 17.94 | 1.19, 1.04, 1.20, 1.22, 1.18, 1.19 |
| 4,096 | 17.53 | 19.68 | 1.19, 1.06, 1.21, 1.31, 1.02, 1.02 |

The object reader is slower in every process, by up to 31%: each
lookup begins and ends its own read transaction. Its benefit is that it does
not hold a snapshot, so checkpoints proceed between reads
(`tests/application_api.rs`). `object-reader.json` holds the raw Criterion
medians and confidence intervals.

## Immutable lookups without explicit transactions

The previous object reader opened an explicit read transaction and revalidated
repository state for each lookup. The candidate runs each lookup's statements
in their implicit transactions on a pooled query connection. Previous and
candidate executables alternated in twelve processes (ABBAAB, twice). Absolute
times drift between processes on this shared host, so the table compares each
process's object-reader time with its own unchanged snapshot control: median
ratio and range over six processes per build.

| Payload bytes | Previous ratio | Candidate ratio |
|---:|---:|---:|
| 128 | 1.19 (1.04 to 1.22) | 1.05 (0.97 to 1.10) |
| 4,096 | 1.12 (1.02 to 1.31) | 1.05 (0.98 to 1.09) |

Lookups that fail during decoding still release their implicit transaction,
which `tests/application_api.rs` checks with WAL truncation after corrupted
records. `immutable-lookups.json` holds both builds and all twelve processes.

## Batched protected handles

`ObjectReader::objects` looks up the whole working set in one call and returns
handles that open verified payloads without a further lookup. Working sets
reach both sides of the 256-key query chunk. The sections above used the
128-object benchmark of their commits; this one reruns all three readers with
the extended benchmark. Milliseconds per pass, mean of two processes' Criterion
medians, with batched time relative to per-key object reads and to snapshot
reads.

| Payload bytes | Objects | Snapshot | ObjectReader | Batched | Batched / ObjectReader | Batched / snapshot |
|---:|---:|---:|---:|---:|---:|---:|
| 128 | 128 | 16.43 | 15.96 | 12.35 | 0.77 | 0.75 |
| 128 | 256 | 30.08 | 31.97 | 24.75 | 0.77 | 0.82 |
| 128 | 257 | 26.70 | 32.27 | 24.41 | 0.76 | 0.91 |
| 4,096 | 128 | 17.12 | 17.89 | 13.96 | 0.78 | 0.82 |
| 4,096 | 256 | 32.92 | 36.17 | 28.01 | 0.77 | 0.85 |
| 4,096 | 257 | 34.58 | 36.22 | 28.13 | 0.78 | 0.81 |

`batched-handles.json` holds the raw medians.

## Build and host

Each JSON file records the `crates/casita` tree of every build it measured,
its build command, compiler, lockfile and executable digests, the host, and
the load average before each run.

## Reproduce

```sh
taskset -c 32-47 cargo bench --no-default-features --features native,experimental --bench object_reads
```
