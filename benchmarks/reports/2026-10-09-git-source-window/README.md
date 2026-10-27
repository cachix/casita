# Git source reopening window

Both native Git import paths reopen their source repository after decoding a
window of object bytes, which drops its pack mappings and object caches. This
change lowers that window from 128 MiB to 32 MiB. These runs pair the upstream
base (`1407672`, `crates/casita` tree `dbb21e518940`) with this change (tree
`ef5bd251823c`) on the eight registered `git-*-source-window-*` cases, which
import uniform blobs on both sides of each trigger. `build.json` records each
executable's tree, command and sha256, the compiler and the lockfile.

All processes ran pinned to `taskset -c 36-41`, cores that share one L3 cache,
on a shared AMD EPYC 9454P host with a one-minute load average between 2.2 and
6.5. Sources, repositories and fixtures were on tmpfs (`TMPDIR=/dev/shm`).
Changes compare medians; the pair counts say how many of the eight paired
samples the current code won.

## Git views

`git-view-source-window-*` imports a deterministic repository, with deltas or,
for the oversized cases, a plain pack, into a Git view with the CLI, then
imports a second revision that changes every fourth file. Each label ran four
rounds in alternating order, each round one suite process with two repetitions,
so every configuration has eight samples per label, paired by round and
repetition. Peak RSS is the import process's alone; source creation and the
checkout, view and fsck gates are outside it. Decoded MiB is the revision's
reachable bytes for initial imports and its new bytes for incremental ones,
inventoried independently through Git.

### 2,046 to 2,049 64 KiB blobs (128 MiB trigger)

| Files | Concurrency | Decoded MiB | Previous MiB | Current MiB | RSS change | Lower pairs | Previous s | Current s | Wall change | Faster pairs |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **initial-import** | | | | | | | | | | |
| 2,046 | 1 | 127.9 | 302.6 | 193.9 | -35.9% | 8 of 8 | 0.972 | 0.972 | -0.0% | 3 of 8 |
| 2,046 | 16 | 127.9 | 327.7 | 211.7 | -35.4% | 8 of 8 | 0.730 | 0.719 | -1.6% | 6 of 8 |
| 2,047 | 1 | 128.0 | 293.5 | 183.3 | -37.6% | 8 of 8 | 0.972 | 0.962 | -1.0% | 7 of 8 |
| 2,047 | 16 | 128.0 | 321.1 | 201.8 | -37.2% | 8 of 8 | 0.729 | 0.711 | -2.5% | 7 of 8 |
| 2,048 | 1 | 128.1 | 293.0 | 183.0 | -37.5% | 8 of 8 | 0.967 | 0.965 | -0.1% | 6 of 8 |
| 2,048 | 16 | 128.1 | 320.2 | 201.5 | -37.1% | 8 of 8 | 0.733 | 0.714 | -2.7% | 6 of 8 |
| 2,049 | 1 | 128.1 | 295.6 | 182.0 | -38.4% | 8 of 8 | 0.979 | 0.969 | -1.0% | 6 of 8 |
| 2,049 | 16 | 128.1 | 321.3 | 201.9 | -37.2% | 8 of 8 | 0.718 | 0.707 | -1.6% | 7 of 8 |
| **incremental-import** | | | | | | | | | | |
| 2,046 | 1 | 32.1 | 164.8 | 150.4 | -8.7% | 8 of 8 | 0.309 | 0.309 | +0.0% | 5 of 8 |
| 2,046 | 16 | 32.1 | 182.4 | 167.5 | -8.2% | 8 of 8 | 0.259 | 0.254 | -1.8% | 7 of 8 |
| 2,047 | 1 | 32.1 | 165.5 | 154.4 | -6.7% | 8 of 8 | 0.308 | 0.303 | -1.6% | 8 of 8 |
| 2,047 | 16 | 32.1 | 181.2 | 166.7 | -8.0% | 8 of 8 | 0.256 | 0.255 | -0.6% | 6 of 8 |
| 2,048 | 1 | 32.1 | 165.5 | 151.4 | -8.5% | 8 of 8 | 0.310 | 0.309 | -0.2% | 5 of 8 |
| 2,048 | 16 | 32.1 | 182.3 | 167.1 | -8.3% | 8 of 8 | 0.258 | 0.254 | -1.4% | 6 of 8 |
| 2,049 | 1 | 32.1 | 163.5 | 152.0 | -7.1% | 8 of 8 | 0.311 | 0.309 | -0.7% | 5 of 8 |
| 2,049 | 16 | 32.1 | 183.3 | 166.8 | -9.0% | 8 of 8 | 0.261 | 0.253 | -3.4% | 6 of 8 |

The previous code reopened at most once, after the initial import's last
objects; the current code reopens after every 32 MiB. Initial imports peak 35
to 38 percent lower and incremental imports, which decode 32 MiB, 7 to 9
percent lower, in every pair. Wall time is unchanged or slightly lower. With
2,046 blobs the previous code never reached its trigger, so its peak is 6 to 10
MiB higher than with 2,047 or more.

### 511 to 513 64 KiB blobs (32 MiB trigger)

| Files | Concurrency | Decoded MiB | Previous MiB | Current MiB | RSS change | Lower pairs | Previous s | Current s | Wall change | Faster pairs |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **initial-import** | | | | | | | | | | |
| 511 | 1 | 32.0 | 127.6 | 127.0 | -0.5% | 4 of 8 | 0.254 | 0.256 | +0.9% | 4 of 8 |
| 511 | 16 | 32.0 | 143.0 | 142.4 | -0.4% | 5 of 8 | 0.221 | 0.217 | -1.7% | 4 of 8 |
| 512 | 1 | 32.0 | 127.4 | 117.2 | -8.0% | 8 of 8 | 0.260 | 0.258 | -0.6% | 6 of 8 |
| 512 | 16 | 32.0 | 142.8 | 132.3 | -7.3% | 8 of 8 | 0.217 | 0.216 | -0.3% | 4 of 8 |
| 513 | 1 | 32.1 | 126.9 | 116.7 | -8.0% | 8 of 8 | 0.259 | 0.262 | +1.2% | 3 of 8 |
| 513 | 16 | 32.1 | 142.3 | 131.1 | -7.8% | 8 of 8 | 0.219 | 0.217 | -1.1% | 5 of 8 |
| **incremental-import** | | | | | | | | | | |
| 511 | 1 | 8.0 | 63.3 | 63.1 | -0.2% | 4 of 8 | 0.093 | 0.089 | -4.2% | 5 of 8 |
| 511 | 16 | 8.0 | 69.4 | 70.6 | +1.7% | 2 of 8 | 0.073 | 0.075 | +2.4% | 2 of 8 |
| 512 | 1 | 8.0 | 62.6 | 62.9 | +0.6% | 1 of 8 | 0.092 | 0.091 | -1.2% | 6 of 8 |
| 512 | 16 | 8.0 | 69.3 | 69.6 | +0.4% | 4 of 8 | 0.075 | 0.073 | -1.4% | 5 of 8 |
| 513 | 1 | 8.1 | 63.3 | 63.4 | +0.2% | 5 of 8 | 0.089 | 0.088 | -1.5% | 5 of 8 |
| 513 | 16 | 8.1 | 69.3 | 69.7 | +0.6% | 4 of 8 | 0.075 | 0.074 | -1.5% | 3 of 8 |

From 512 blobs, whose trees take the decoded total past 32 MiB, the current
code reopens once and peaks 7 to 8 percent lower in every pair. With 511 blobs,
and for the 8 MiB incremental imports, neither code reopens and the results
match within noise.

### Two blobs just over each trigger

Oversized-32 imports two 33 MiB blobs:

| Files | Concurrency | Decoded MiB | Previous MiB | Current MiB | RSS change | Lower pairs | Previous s | Current s | Wall change | Faster pairs |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **initial-import** | | | | | | | | | | |
| 2 | 1 | 66.0 | 289.7 | 194.2 | -32.9% | 8 of 8 | 0.433 | 0.431 | -0.4% | 5 of 8 |
| 2 | 16 | 66.0 | 290.3 | 200.4 | -30.9% | 8 of 8 | 0.428 | 0.438 | +2.3% | 1 of 8 |
| **incremental-import** | | | | | | | | | | |
| 2 | 1 | 33.0 | 134.4 | 117.9 | -12.2% | 8 of 8 | 0.177 | 0.179 | +1.2% | 3 of 8 |
| 2 | 16 | 33.0 | 134.8 | 117.6 | -12.7% | 8 of 8 | 0.180 | 0.178 | -0.7% | 5 of 8 |

Oversized-128 imports two 129 MiB blobs:

| Files | Concurrency | Decoded MiB | Previous MiB | Current MiB | RSS change | Lower pairs | Previous s | Current s | Wall change | Faster pairs |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **initial-import** | | | | | | | | | | |
| 2 | 1 | 258.0 | 539.9 | 543.7 | +0.7% | 3 of 8 | 1.493 | 1.506 | +0.9% | 4 of 8 |
| 2 | 16 | 258.0 | 535.5 | 543.3 | +1.5% | 2 of 8 | 1.484 | 1.486 | +0.2% | 3 of 8 |
| **incremental-import** | | | | | | | | | | |
| 2 | 1 | 129.0 | 406.0 | 405.7 | -0.1% | 4 of 8 | 0.626 | 0.631 | +0.9% | 4 of 8 |
| 2 | 16 | 129.0 | 405.9 | 406.0 | +0.0% | 3 of 8 | 0.626 | 0.629 | +0.3% | 4 of 8 |

A single object larger than the window still passes it before the next
reopening: the window is a reopening trigger, not an RSS cap. Two 33 MiB blobs
peak 31 to 33 percent lower initially and 12 to 13 percent lower incrementally.
Their initial import at concurrency 16 is 2.3 percent slower, winning 1 of 8
pairs; the other three are within 1.2 percent. Two 129 MiB blobs pass both
triggers at the same point and are unchanged.

## Closure imports

`git-closure-source-window-*` runs the `git_closure_import` probe, built for
each tree with `--no-default-features --features native,git,experimental`. The
suite ran both builds in one paired run: eight repetitions, alternating which
probe runs first, one process per probe, file count and repetition. Sources are
packed random blobs imported into a local repository with 16 objects staged
concurrently and a 64 MiB byte budget. Cold import wall time covers the import
alone. Process RSS is the whole probe process, including fixture generation and
closure audits, so it bounds rather than isolates the import.

| Case | Files | Source MiB | Previous cold s | Current cold s | Cold change | Faster pairs | Previous process MiB | Current process MiB | Process RSS change | Lower pairs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 32 | 31 × 1 MiB | 31.0 | 0.212 | 0.208 | -1.8% | 7 of 8 | 205.1 | 196.8 | -4.0% | 5 of 8 |
| 32 | 32 × 1 MiB | 32.0 | 0.205 | 0.218 | +6.4% | 2 of 8 | 204.3 | 204.7 | +0.2% | 4 of 8 |
| 32 | 33 × 1 MiB | 33.0 | 0.222 | 0.219 | -1.2% | 5 of 8 | 208.2 | 203.3 | -2.4% | 6 of 8 |
| 128 | 127 × 1 MiB | 127.0 | 0.729 | 0.707 | -3.0% | 4 of 8 | 512.8 | 414.7 | -19.1% | 8 of 8 |
| 128 | 128 × 1 MiB | 128.0 | 0.704 | 0.722 | +2.6% | 2 of 8 | 517.4 | 418.4 | -19.1% | 8 of 8 |
| 128 | 129 × 1 MiB | 129.0 | 0.722 | 0.740 | +2.5% | 2 of 8 | 514.0 | 429.7 | -16.4% | 8 of 8 |
| oversized-32 | 2 × 33 MiB | 66.0 | 0.402 | 0.400 | -0.5% | 3 of 8 | 262.7 | 228.6 | -13.0% | 8 of 8 |
| oversized-128 | 2 × 129 MiB | 258.0 | 1.378 | 1.380 | +0.1% | 3 of 8 | 646.3 | 646.3 | +0.0% | 3 of 8 |

With 127 to 129 1 MiB blobs, process peaks fall 16 to 19 percent in every pair,
and two 33 MiB blobs 13 percent. Cold imports move between -3.0 and +6.4
percent. The 31-file case runs the same code path in both builds, since neither
reaches its trigger, yet its median moves by -1.8 percent. At 32 files the
current code is slower at the median and wins 2 of 8 pairs, but its range lies
inside the previous code's. Warm, subtree-delta and wide-delta imports read
less than 64 KiB of source and are retained in the results only.

## Reproduce

Build both trees with the commands in `build.json`, then for each Git view case
alternate the two CLIs across rounds:

```sh
taskset -c 36-41 benchmark run git-view-source-window-128 --profile standard \
  --repetitions 2 --casita-bin /path/to/casita --no-build --output view-128.json
```

The closure cases pair both probes in one run:

```sh
taskset -c 36-41 benchmark run git-closure-source-window-128 --repetitions 8 \
  --baseline-binary /path/to/base/git_closure_import \
  --probe-binary /path/to/candidate/git_closure_import --no-build \
  --output closure-128.json
```

`runs/` keeps every run's samples and gates. Git view results keep each source
revision's reachable and new byte totals; the per-object listings behind them
were dropped to keep each file small.
