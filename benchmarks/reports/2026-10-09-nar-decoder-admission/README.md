# NAR decoder admission

NAR intake decodes an archive with a synchronous decoder that hands its entries
to an async consumer over bounded pipes. The decoder ran under `spawn_blocking`
and waited on those pipes, while the consumer needed the same blocking pool to
store what it read. Once large imports were as many as the blocking workers,
every worker held a decoder waiting for its consumer and every consumer waited
for a worker. Decoders now run on their own threads, at most 16 per process;
further imports wait for a decoder.

## Liveness

`tests/nar_decoder_admission.rs` runs each case in a child process under a
60-second watchdog. Upstream `1407672` was built with the same test. Times are
the test's own, from a debug build:

| Case | Upstream | This commit |
|---|---|---|
| One import each of 8 and 32 MiB, on one and then two blocking workers | exceeds the 60 s watchdog | passes, 9.1 s |
| Two concurrent 32 MiB imports, one blocking worker | exceeds the 60 s watchdog | passes, 6.7 s |
| Three concurrent 32 MiB imports, four blocking workers | passes, 10.7 s | passes, 10.6 s |
| Four concurrent 32 MiB imports, four blocking workers | exceeds the 60 s watchdog | passes, 14.0 s |
| 17 concurrent 2 MiB imports, one blocking worker | passes, 4.8 s | passes, 4.3 s |

Smaller files fit in the pipeline's buffering, so their consumers finish without
a blocking worker; 8 MiB on one worker and 17 imports of 2 MiB pass upstream
too. All 54 NAR library tests pass, including the decoder's own tests of queued
cancellation, abandoned callers, capacity shared across runtimes, runtime
teardown and panics.

## Cost

The change adds a thread per import and a limit of 16 concurrent decoders. Both
builds ran every `nar_import` case in four rounds, alternating which went first,
except one 32 MiB import on one blocking worker, which stalls upstream. Cases
bearing on the cost then ran in eight more rounds. Ratios are this commit's
Criterion median over upstream's, paired by round.

Concurrent 2 MiB imports into one repository, each its own task on a
multi-threaded runtime, on both sides of the limit:

| Imports | Upstream ms | This commit ms | Ratio | Faster |
|---|---:|---:|---:|---|
| 16 | 31.0 | 28.2 | 0.91 | 4 of 4 |
| 17 | 32.7 | 29.4 | 0.90 | 4 of 4 |
| 32 | 55.3 | 52.2 | 0.94 | 4 of 4 |

Every round of this commit was faster than every round of upstream. Going from
16 to 17 imports, where the 17th decoder waits, adds 1.2 ms here and 1.7 ms
upstream.

With one blocking worker, this commit imports 32 MiB in 97.3 ms, as it does with
two (100.6 ms; upstream 100.6 ms). 8 MiB on one worker takes 0.97 times as long
as upstream.

Single imports, the other 41 `nar_import` cases, show per-case ratios from 0.49
to 1.10 (median 0.94), and none is slower in every round. The low ratios come
from 28 case runs, all this commit's and mostly in two of its four runs, in
which a case took under 0.6 times its usual time. Upstream had none, and the
eight-round series had none in either build, so they are not counted as a
speedup. In the eight-round series, one 1 KiB import takes 2.82 ms in both
builds, and 2,048 files of 64 KiB take 0.92 times as long.

Repeated 1 KiB imports into one repository, retaining every report, over eight
rounds:

| Imports | Upstream ms | This commit ms | Ratio | Faster |
|---|---:|---:|---:|---|
| 15 | 42.5 | 42.7 | 1.003 | 3 of 8 |
| 16 | 45.2 | 45.9 | 1.017 | 1 of 8 |
| 17 | 48.1 | 49.6 | 1.009 | 3 of 8 |
| 18 | 53.1 | 56.8 | 1.036 | 1 of 8 |
| 19 | 55.9 | 60.4 | 1.069 | 2 of 8 |
| 32 | 92.5 | 100.5 | 1.078 | 3 of 8 |

Up to 17 imports the builds are within 2%. Longer sequences, past the 16-import
metadata reclamation interval, took 3.6% to 7.8% longer. A separate six-round
series measured +1.0% at 32 imports. In that series, decoder threads reused from
a dedicated pool took 1.1% longer than a thread per import, so thread creation
does not explain the difference. Its cause was not isolated.

`results.json` holds every run's per-case Criterion medians and confidence
intervals, the paired summaries, the pooled variant's runs and the test
outcomes.

## Build and host

- Upstream: `crates/casita` tree `dbb21e5`, with this commit's integration test,
  fixture and benchmark copied in. This commit: tree `bd2635a`.
- Benchmarks built in Cargo's bench profile and tests in the test profile, with
  rustc 1.96.0 (ac68faa20 2026-05-25) and one lockfile. `results.json` records
  the build commands and executable digests.
- AMD EPYC 9454P, shared with other users (one-minute load 1.8–4.4), pinned to
  `taskset -c 36-41`, cores sharing one L3 cache. Nothing else from this work
  ran during timing.

## Reproduce

```sh
cargo test -p casita --test nar_decoder_admission
taskset -c 36-41 cargo bench -p casita --bench nar_import
```

Upstream stalls in `nar_decoder_pool/workers-1/33554432`; bound its runs with
`timeout`, or exclude that case.
