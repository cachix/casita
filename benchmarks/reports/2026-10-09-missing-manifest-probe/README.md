# Missing-manifest probes

A verified read classifies a blob's manifest with one bounded read of its
prefix. When that read found no manifest, upstream still sent a HEAD for the
same path before falling back to the blob's single chunk. A blob that fits in
one chunk is stored without a manifest, so every verified read of such a blob,
and of a missing blob, paid for that HEAD. This change goes straight to the
chunk. Flat manifests keep their HEAD, which supplies their length.

## Requests

Three chunked-store tests count manifest requests: for a single-chunk blob, a
single-chunk blob with an outboard, and a missing blob. Upstream `1407672`,
built with this commit's tests, fails each of them with one extra HEAD and
passes the other 105. This commit passes all 108 chunked-store tests.

## Timing

`verified_manifest_reads` reads 64 prepared blobs through authenticated EOF per
iteration, one at a time or 64 at a time. Upstream and this commit ran the same
benchmark source in four rounds, alternating which went first. Ratios are this
commit's Criterion median over upstream's; the range and the count of faster
rounds come from the four rounds paired.

With `memory-get-1ms`, which delays every GET and HEAD by 1 ms, one request
costs about 2.1 ms in this harness, more than the configured delay because
Tokio's timer has millisecond granularity. Upstream's single-chunk reads amount
to 3 requests per blob, or 5 when the blob also has an outboard, and this commit
saves one of them:

| Blob | Readers | Upstream ms | Ratio | Range | Faster |
|---|---:|---:|---:|---|---|
| single 1 KiB chunk | 1 | 407.5 | 0.670 | 0.669–0.673 | 4 of 4 |
| single 1 KiB chunk | 64 | 7.5 | 0.709 | 0.703–0.716 | 4 of 4 |
| single 16,384-byte chunk | 1 | 408.6 | 0.669 | 0.664–0.673 | 4 of 4 |
| single 16,384-byte chunk | 64 | 8.3 | 0.748 | 0.743–0.750 | 4 of 4 |
| single 16,385-byte chunk, with outboard | 1 | 679.5 | 0.802 | 0.799–0.806 | 4 of 4 |
| single 16,385-byte chunk, with outboard | 64 | 12.7 | 0.835 | 0.833–0.836 | 4 of 4 |
| empty flat manifest | 1 | 405.8 | 1.002 | 0.999–1.003 | 1 of 4 |
| empty flat manifest | 64 | 6.8 | 1.006 | 1.003–1.012 | 0 of 4 |
| flat manifest, 2 chunks | 1 | 814.8 | 1.001 | 0.992–1.005 | 2 of 4 |
| flat manifest, 2 chunks | 64 | 14.1 | 1.000 | 0.998–1.001 | 2 of 4 |

The two flat-manifest cases make the same requests in both builds and serve as
controls.

Without request latency, the saved HEAD is an in-memory lookup or a `stat`:

- `memory-loose`: single-chunk reads of 1-byte and 1 KiB blobs take 0.93–0.97
  times as long, and of 16,383 to 32,768-byte blobs 0.98–0.99, where hashing and
  decompression dominate.
- `local-packed`: single-chunk reads take 0.59–0.95 times as long, with wide
  round-to-round ranges at one reader.
- Reads with a manifest, 0 to 65 chunks on all three backends, make the same
  requests in both builds and stay within -2.0% to +1.3% at the median. Two of
  them are slower in all four rounds: `local-packed` with 2 chunks and 64
  readers (+1.2%), and the throttled empty manifest with 64 readers (+0.6%).

`results.json` holds every run's per-case Criterion medians and confidence
intervals, the paired summaries and the test outcomes.

## Build and host

- Upstream: `crates/casita` tree `dbb21e5`, with this commit's benchmark, its
  `Cargo.toml` registration and the chunked-store tests copied in. This commit:
  tree `e9e9d4a`.
- Both built in Cargo's bench profile with rustc 1.96.0 (ac68faa20 2026-05-25)
  and one lockfile. `results.json` records the build commands and executable
  digests.
- AMD EPYC 9454P, shared with other users (one-minute load 1.8–3.1), pinned to
  `taskset -c 36-41`, cores sharing one L3 cache. Nothing else from this work
  ran during timing.

## Reproduce

```sh
taskset -c 36-41 cargo bench -p casita --features native,experimental --bench verified_manifest_reads
cargo test -p casita --features native,experimental --lib blob::chunked::
```
