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

## Build and host

Each JSON file records the `crates/casita` tree of every build it measured,
its build command, compiler, lockfile and executable digests, the host, and
the load average before each run.

## Reproduce

```sh
taskset -c 32-47 cargo bench --no-default-features --features native,experimental --bench object_reads
```
