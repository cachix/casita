# WAL growth under retained object protection

A `RetainedReader` keeps its metadata snapshot, an open read transaction in a
local repository, so checkpoints cannot truncate the WAL while it lives. An
`ObjectRetention` guard keeps the same objects protected without the snapshot.

## Measurements

Each case imports a sentinel blob, takes a retained reader, removes the
sentinel's root and then performs unbatched 512-byte `BlobImport`s that each
replace one root. The snapshot case keeps the reader; the guard case keeps only
`retain_objects()` after dropping it. Two independent processes ran every case;
times are their mean, WAL sizes their maximum.

| Writes | Snapshot seconds | Guard seconds | Snapshot peak WAL MB | Guard peak WAL MB | Snapshot WAL after checkpoint MB | Guard WAL after checkpoint MB |
|---:|---:|---:|---:|---:|---:|---:|
| 32 | 0.18 | 0.27 | 0.82 | 0.82 | 0.82 | 0.00 |
| 256 | 2.35 | 2.04 | 6.58 | 4.13 | 6.58 | 0.00 |
| 1,024 | 10.46 | 10.31 | 27.15 | 4.14 | 27.15 | 0.00 |

With the snapshot held, the WAL grows with every write and `flush` reports
`Busy`. With only the guard, checkpoints keep the WAL bounded during the writes
and `flush` truncates it to zero. At 256 and 1,024 writes the two write times
are within 13% of each other; this measures WAL retention, not ingestion
throughput.

Every case checks the imported identities, reads the sentinel back after
collection, requires it to be collected once protection is released, and
requires a clean `fsck`. The `benchmark all` collector rejects missing or
duplicate cases and inconsistent checkpoint evidence.

## Build and host

`results.json` records the `crates/casita` tree it measured, the build
command, compiler, lockfile and executable digests, the host, and the load
average before each run. The executable was built with
`cargo bench --no-default-features --features native,experimental --bench retained_wal --no-run`.

## Reproduce

```sh
CASITA_BENCH_RETAINED_WRITES=32,256,1024 taskset -c 32-47 \
  cargo bench --no-default-features --features native,experimental --bench retained_wal
benchmark all --suites core-primitives --profile smoke --repetitions 1 --output /tmp/core-primitives
```
