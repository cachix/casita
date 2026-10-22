# Collection membership batches

Collection planning tests every logical object, manifest, pinned payload, stale
payload candidate and stored chunk against the live, pinned and manifest
inventories. Upstream asked one key at a time; once an inventory spills to disk,
each answer is its own blocking disk job. This change asks in batches of up to
256 keys, and a spilled inventory answers each batch in disk jobs of at most as
many keys as the spill memory limit.

## Method

`benchmark run collection-inventory` times the whole collection plan of one live
directory with shared edges and five orphans, using memory metadata and chunked
payloads, then audits the plan and a full sweep. Memory limits cover forced
spill (17 keys), the live-object set boundary (files, files plus one, files plus
two) and an in-memory control (16 × files + 128).

Upstream `9d7a2f4` was built with this commit's probe test copied in. Both
probes ran under this commit's suite, in six rounds of two repetitions per case,
alternating which build ran first. Each case ran in a fresh process, with spill
files on tmpfs. Both builds opened the same number of spill files in every case,
and every run passed the suite's correctness gates.

## 8,192 files

Times are the median of the round medians. Ratios are this commit's time over
upstream's, the median of six paired rounds:

| Memory limit | Spill files | Upstream ms | This change ms | Ratio | Range | Faster |
|---|---:|---:|---:|---:|---|---|
| 17 keys | 5 | 3,526.0 | 3,077.1 | 0.865 | 0.854–0.932 | 6 of 6 |
| files + 0 | 5 | 1,383.3 | 684.1 | 0.502 | 0.490–0.537 | 6 of 6 |
| files + 1 | 4 | 1,172.3 | 485.6 | 0.412 | 0.384–0.493 | 6 of 6 |
| files + 2 | 2 | 580.9 | 276.2 | 0.498 | 0.414–0.665 | 6 of 6 |
| in memory | 0 | 39.5 | 39.2 | 0.994 | 0.987–1.002 | 4 of 6 |

- At the live-object set boundary, planning takes 0.41–0.50 times as long,
  faster in every round.
- With 17 keys in memory it takes 0.87 times as long. Each 256-key batch still
  runs in jobs of at most 17 keys there.
- The in-memory control is unchanged. Median process RSS is 95.7 MiB upstream
  and 95.4 MiB here.

## 255 to 257 files

These plans take milliseconds, and some samples stall in both builds: 34 of 144
spilled upstream samples and 31 of 144 here took 88–190 ms longer than the rest
(median 129 ms). The stalls occur only in cases that spill, in either build, and
their source was not isolated. The table gives each build's median over its
twelve samples, excluding samples more than 75 ms above that build's fastest,
and the number excluded:

| Files | Memory limit | Spill files | Upstream ms | This change ms | Ratio | Stalls, upstream / here |
|---:|---|---:|---:|---:|---:|---|
| 255 | 17 keys | 5 | 35.70 | 30.00 | 0.840 | 2 / 1 |
| 255 | files + 0 | 5 | 24.02 | 15.10 | 0.629 | 3 / 4 |
| 255 | files + 1 | 4 | 21.15 | 12.44 | 0.588 | 1 / 2 |
| 255 | files + 2 | 2 | 12.57 | 7.62 | 0.606 | 2 / 0 |
| 255 | in memory | 0 | 0.94 | 0.93 | 0.990 | 0 / 0 |
| 256 | 17 keys | 5 | 36.30 | 29.11 | 0.802 | 4 / 5 |
| 256 | files + 0 | 5 | 26.75 | 15.90 | 0.595 | 7 / 5 |
| 256 | files + 1 | 4 | 21.52 | 13.21 | 0.614 | 4 / 3 |
| 256 | files + 2 | 2 | 14.13 | 8.29 | 0.587 | 0 / 0 |
| 256 | in memory | 0 | 0.94 | 0.94 | 0.993 | 0 / 0 |
| 257 | 17 keys | 5 | 43.50 | 35.69 | 0.820 | 4 / 4 |
| 257 | files + 0 | 5 | 25.72 | 15.67 | 0.609 | 3 / 2 |
| 257 | files + 1 | 4 | 21.12 | 11.80 | 0.559 | 1 / 5 |
| 257 | files + 2 | 2 | 11.75 | 7.23 | 0.615 | 3 / 0 |
| 257 | in memory | 0 | 0.95 | 0.94 | 0.994 | 0 / 0 |

- Spilled plans take 0.56–0.63 times as long at the live-object set boundary,
  and 0.80–0.84 with 17 keys in memory.
- In-memory controls take 0.99 times as long, with no stalls.
- Medians over all twelve samples, stalls included, give ratios within 0.04 of
  these, except for 256 files at 256 keys, where seven of upstream's twelve
  samples stalled.

`results.json` holds every sample, the paired summaries, run order and load, and
provenance.

## Build and host

- Upstream: `crates/casita` tree `6569de2`, with this commit's
  `collection_inventory_tests.rs` and its module declaration added. This commit:
  tree `7bd90ea`.
- Both built with `cargo test --release --features cli --lib --no-run`, rustc
  1.96.0 (ac68faa20 2026-05-25) and one lockfile. The suite records the probe's
  SHA-256; its `casita_revision` is the suite's checkout for both builds:
  `3aa1fdb`, this commit before the report was added, with the same
  `crates/casita` tree and suite.
- AMD EPYC 9454P, shared with other users (one-minute load 2.3–4.6), pinned to
  `taskset -c 36-41`, cores sharing one L3 cache. Nothing else from this work
  ran during timing.

## Reproduce

```sh
cargo test --release --features cli --lib --no-run
TMPDIR=/dev/shm/collection-inventory taskset -c 36-41 benchmark run collection-inventory --profile standard --repetitions 2 --no-build --probe-binary <probe> --output <run>.json
```
