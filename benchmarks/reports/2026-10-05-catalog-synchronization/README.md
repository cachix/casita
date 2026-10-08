# Catalog synchronization measurements (2026-10-05)

Compared upstream `84ec2920791276cd4ad8c029cd60529810e15705` with the
implementation in this branch. [Source hashes](provenance.json) identify the
measured production code and benchmark harness independently of commit-message
or report edits. The baseline received only the identical test-only benchmark
function and its module registration; no production fixes were backported.

Both binaries used Rust 1.96.0, the same Cargo.lock, and `--release
--no-default-features --features native --lib`. These are optimized library test
executables, including compiled test hooks, rather than production application
binaries. The host was Linux 7.2.6 on an AMD Ryzen AI 9 HX 370. Processes were
pinned to CPU 2, with the performance governor and boost enabled. The CPU was
not reserved exclusively; normal desktop activity remained possible.

Each batch warmed both binaries once, then measured ten fresh baseline/candidate
pairs, alternating AB and BA order. The first batch used 100 iterations per
case; a second used 500 to investigate the initial deferred-listing slowdown.
All 24 cases ran in every process, including all 12 deferred-run cases. No
builds or validation jobs ran during measurement. Both batches and all warmups
are retained in [samples.json.gz](samples.json.gz).

Times below are medians of per-process mean operation times, in microseconds,
shown as **baseline → branch**. Each iteration includes one changed and one
unchanged synchronization. Deferred listing also includes loading the run,
reading base manifest shards, and collecting the complete listing into a
`BTreeSet`; it does not isolate run decoding. Paired changes use the median of
within-pair percentage differences. The [100-iteration](summary-100.json) and
[500-iteration](summary-500.json) summaries include 95% percentile bootstrap
intervals (10,000 resamples, seed 1741), which describe variation in
these runs, not all machine/workload uncertainty.

The table uses the 500-iteration confirmation batch. A dash means the case
has no deferred run. Each row covers one catalog form, base size, and count
of pending local changes.

| Catalog | Entries | Pending | Changed root (µs) | Unchanged root (µs) | Deferred listing (µs) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Materialized | 16 | 0 | 4.155 → 4.195 | 1.093 → 1.085 | — |
| Materialized | 16 | 1 | 5.833 → 5.762 | 1.126 → 1.145 | — |
| Materialized | 16 | 64 | 15.416 → 15.387 | 1.137 → 1.155 | — |
| Materialized | 65,536 | 0 | 2251.842 → 2192.021 | 612.557 → 588.030 | — |
| Materialized | 65,536 | 1 | 3214.319 → 3239.054 | 843.866 → 845.723 | — |
| Materialized | 65,536 | 64 | 3285.083 → 3296.445 | 864.478 → 853.879 | — |
| Sharded | 16 | 0 | 5.371 → 5.379 | 0.430 → 0.416 | — |
| Sharded | 16 | 1 | 7.064 → 7.060 | 0.456 → 0.434 | — |
| Sharded | 16 | 64 | 15.443 → 15.274 | 0.449 → 0.454 | — |
| Sharded | 65,536 | 0 | 11.750 → 11.731 | 0.713 → 0.725 | — |
| Sharded | 65,536 | 1 | 14.477 → 14.223 | 0.759 → 0.734 | — |
| Sharded | 65,536 | 64 | 28.707 → 27.669 | 0.743 → 0.721 | — |
| Legacy run | 16 | 0 | 6.978 → 6.946 | 0.634 → 0.618 | 6.448 → 6.579 |
| Legacy run | 16 | 1 | 8.659 → 8.548 | 0.641 → 0.619 | 7.977 → 8.215 |
| Legacy run | 16 | 64 | 16.802 → 16.608 | 0.643 → 0.633 | 22.680 → 23.086 |
| Legacy run | 65,536 | 0 | 24.002 → 22.917 | 2.125 → 2.187 | 8908.857 → 8948.408 |
| Legacy run | 65,536 | 1 | 25.861 → 24.102 | 1.831 → 1.852 | 8238.923 → 8458.082 |
| Legacy run | 65,536 | 64 | 36.222 → 34.792 | 1.914 → 1.696 | 8321.334 → 8535.582 |
| Queryable run | 16 | 0 | 7.399 → 7.529 | 0.754 → 0.755 | 6.818 → 7.025 |
| Queryable run | 16 | 1 | 9.260 → 8.995 | 0.780 → 0.782 | 8.430 → 8.543 |
| Queryable run | 16 | 64 | 17.664 → 17.111 | 0.759 → 0.764 | 23.517 → 23.377 |
| Queryable run | 65,536 | 0 | 20.464 → 18.678 | 2.135 → 1.945 | 8220.706 → 8423.871 |
| Queryable run | 65,536 | 1 | 25.460 → 23.255 | 2.146 → 1.988 | 8166.227 → 8397.353 |
| Queryable run | 65,536 | 64 | 37.777 → 35.150 | 2.359 → 1.978 | 8343.764 → 8475.308 |

Whole-matrix process time was 33.547 → 34.182 seconds, a median paired change
of +2.2% (95% interval −0.5% to +3.7%). The first batch measured +3.7%. Peak
process RSS was 40.09 → 40.08 MiB; this is process-wide high-water memory, not
a bound on catalog memory. Catalog request and byte counters matched in every
case in both batches. All membership and publication/reopen gates passed.

The measurements do not establish a general speedup or regression-free latency.
The initial large legacy-run/no-pending case showed +6.3% for listing
(+3.4% to +7.7%) and +5.9% for changed-root synchronization. In the longer
batch those effects did not reproduce: listing was −0.3% (−4.2% to +5.2%),
and changed-root synchronization was −5.0% (−12.3% to +0.3%). Other large
listing cases still showed costs: legacy runs with 1/64 pending changes were
+2.1%/+3.4%, and queryable runs with 0/1 pending changes were +2.1%/+4.3%;
all four intervals were above zero. Absolute times and variability also changed
between batches, so they are reported separately rather than pooled.

Code inspection and identical I/O counters show that the branch adds coherent
capture, locking, and selection checks without extra catalog requests in this
workload. The timings do not isolate their individual cost from test hooks,
compiler layout, or machine variability. The benefit is the deterministic
correctness fixes; a modest listing cost remains visible in these test binaries.
This benchmark does not measure disk/network throughput, concurrent contention,
or end-to-end import performance.

## Retained evidence and reproduction

- [100-iteration summary](summary-100.json) and
  [500-iteration summary](summary-500.json): all 60 timing metrics, paired
  intervals, catalog counters, and process resources.
- [Samples](samples.json.gz): every measured sample and warmup, with batch
  settings, pair order, timing metrics, and resources. A null pair identifies a
  warmup; summaries exclude warmups. Runner logs and duplicate machine metadata
  are omitted from this compact extract.
- [Source and build identities](provenance.json): source hashes, features,
  release profile, binary hashes, and the dependency-lock hash.
- [Baseline adapter](baseline-adapter.patch): the identical benchmark function
  and its test-module registration, with no production changes.
- [Dependency lock](Cargo.lock.gz): the exact Cargo.lock shared by both builds.

Recompute every saved summary and bootstrap interval from the retained samples:

```sh
python3 benchmarks/reports/2026-10-05-catalog-synchronization/verify.py
```

To repeat both measurement batches, use the repository's configured development
shell with Rust 1.96.0 on Linux. Choose an available CPU on an otherwise idle
machine; CPU 2 was used here. From the completed branch checkout, run:

```sh
bash benchmarks/reports/2026-10-05-catalog-synchronization/reproduce.sh 2
```

The script creates separate temporary baseline and candidate worktrees, applies
only the adapter to the baseline, installs the saved lock in both, and builds
optimized library test executables in separate target directories. It then
warms each binary and runs ten alternating AB/BA pairs for each iteration count.
Both variants use the candidate's permanent benchmark runner. The worktrees,
build output, and all resulting JSON samples remain at the printed path.
The source hashes identify the measured implementation; future code or toolchain
changes produce a new comparison rather than reproducing this exact build.
