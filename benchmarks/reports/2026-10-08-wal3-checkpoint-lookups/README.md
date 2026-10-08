# WAL3 publication after object-shard checkpoints

Publishing raw blobs to a WAL3 repository is much slower once earlier objects
have been checkpointed into object shards. The `wal3-publication-checkpoints`
probe commits empty metadata edits until the next object-shard write,
optionally reopens the store, and times one publication. Staging is untimed.
The batch cases checkpoint one batch of 511, 512 or 513 objects, both sides of
a 512-record block, and then publish a second batch of the same size. The
corpus cases checkpoint 8,192 or 131,072 objects, published 4,096 at a time,
and then publish 512 more, which spread over most of the checkpoint's blocks.
Every case checks the records it looks up, that raw blobs gain no closure
witnesses, and an fsck that reports only the deliberately unrooted objects.

## Reproduction

Milliseconds, median of five processes. Shards and blocks describe the
checkpoint; shard cache hits count logical shard reads served from the cache
during the timed publication. A corpus has no "before" publication of the same
shape, so its row has none.

| Case | Handle | Shards | Blocks | Before checkpoint | After checkpoint | Shard cache hits after |
|---|---|---:|---:|---:|---:|---:|
| 511 batch | warm | 1 | 1 | 5.1 | 109.2 | 1014 |
| 511 batch | reopened | 1 | 1 | 6.1 | 109.0 | 1013 |
| 512 batch | warm | 1 | 1 | 6.6 | 110.0 | 1016 |
| 512 batch | reopened | 1 | 1 | 5.8 | 110.7 | 1015 |
| 513 batch | warm | 1 | 2 | 6.4 | 131.7 | 1018 |
| 513 batch | reopened | 1 | 2 | 6.5 | 109.8 | 1017 |
| 8,192 corpus | warm | 1 | 16 | — | 117.0 | 1024 |
| 8,192 corpus | reopened | 1 | 16 | — | 116.8 | 1023 |
| 131,072 corpus | warm | 12 | 265 | — | 138.4 | 1024 |
| 131,072 corpus | reopened | 12 | 265 | — | 144.7 | 1017 |

After the checkpoint, every published object is checked against the shards
one key at a time, decoding a shard block of up to 512 records for each
lookup, so publication goes from a few milliseconds to over a hundred
whatever the size of the checkpoint. The storage is WAL3's local transport, so
this isolates checkpoint CPU and I/O costs rather than network latency.
`reproducer.json` holds every metric.

## Batched shard lookups

Publication now prefetches the shard records of a whole mutation or delta:
keys are grouped by shard, each shard is read once, and each selected block is
authenticated and decoded once. Snapshot `object_batch` and
`validated_closures` use the same path. Milliseconds after the checkpoint,
median of five processes.

| Case | Handle | Previous | Batched | Speedup | Shard cache hits |
|---|---|---:|---:|---:|---:|
| 511 batch | warm | 109.2 | 8.4 | 13.1 | 2 |
| 511 batch | reopened | 109.0 | 7.9 | 13.9 | 1 |
| 512 batch | warm | 110.0 | 12.3 | 8.9 | 2 |
| 512 batch | reopened | 110.7 | 7.5 | 14.8 | 1 |
| 513 batch | warm | 131.7 | 7.9 | 16.7 | 2 |
| 513 batch | reopened | 109.8 | 8.1 | 13.6 | 1 |
| 8,192 corpus | warm | 117.0 | 14.0 | 8.3 | 2 |
| 8,192 corpus | reopened | 116.8 | 11.0 | 10.6 | 1 |
| 131,072 corpus | warm | 138.4 | 53.5 | 2.6 | 14 |
| 131,072 corpus | reopened | 144.7 | 59.0 | 2.5 | 7 |

A single-batch checkpoint is one or two blocks, so batching decodes each block
once instead of once per key. The publication into a large corpus is new keys
spread over most blocks of the checkpoint, so it still decodes hundreds of
whole blocks. `batched-lookups.json` holds every metric.

## Build and host

Each JSON file records its build command, compiler, lockfile and executable
digests, the host, and the load average before each run. Processes were pinned
to 16 cores of a shared host and ran serially; control and batched processes
alternated CB BC CB BC CB.

## Reproduce

```sh
cargo test -p casita --release --all-features --lib --no-run --message-format=json
benchmark run wal3-publication-checkpoints --probe-binary <casita lib test executable> \
  --repetitions 5 --output /tmp/wal3-publication.json
benchmark all --suites wal3-publication-checkpoints --profile smoke --repetitions 1 \
  --output /tmp/wal3-publication-all
```
