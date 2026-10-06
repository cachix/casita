# Artifact batch library benchmarks

Batching improved local imports and directory checkout in all three runs at
8 and 32 requests. At 32 requests, memory imports improved consistently by
about 1.6 times; memory checkout showed no consistent gain.

| Local operation | Requests | Observed speedup across three runs |
|---|---:|---:|
| Mixed import | 8 | 1.3 to 4.2 times |
| Mixed import | 32 | 9.5 to 16.6 times |
| Directory checkout | 8 | 1.3 to 1.7 times |
| Directory checkout | 32 | 1.8 to 2.3 times |

These are observed ranges of individual-call median divided by batch median,
not confidence intervals. Absolute timings varied substantially, including
when execution order was reversed. For example, the 32-request local import
control ranged from 700 to 3231 ms and the batch from 42 to 222 ms. This is
useful evidence for reducing call overhead, but does not establish precise
production speedups or an intrinsic performance cliff. Singleton results are
particularly sensitive to the observed variation.

The [full results](results.md) contain group medians for every case and run.
[summary.json](summary.json) retains median confidence intervals and comparisons;
[criterion-raw.tar.gz](criterion-raw.tar.gz) retains the original estimates,
samples, benchmark identities and baselines from the `criterion-run-*` directories. [environment.json](environment.json)
records the worktree, source fingerprint, compiler, CPU and filesystem.

## Scope and correctness

The permanent `artifact_batches` benchmark compares two paths in the same
implementation, rather than comparing separate Git revisions:

* Individual imports create a mutation session and publish each root.
  Batch imports stage into one caller-owned session and publish all roots once.
* Individual checkout uses `Repository::checkout` for each destination.
  Batch checkout uses one `RetainedReader` for the whole group.

Each sample creates a fresh memory or local repository. Payloads are the same
8 bytes, `contents`, so this is deliberately a tiny, highly deduplicated corpus.
Import requests cycle through blob, filesystem and tar; the singleton case
contains only a blob. Each filesystem/tar input has one regular file.
Checkout repeatedly materializes the same one-file directory into different
new destinations. The runtime is Tokio's current-thread runtime.

Timing excludes repository setup, fixture construction and correctness checks.
It excludes JSON-RPC framing, root lookup, access-time updates and Cargo itself.
Blob, Git and NAR restoration are not timed separately. Large artifact I/O and
production filesystem behavior therefore require separate measurements.
Caches were not flushed and other host activity was not controlled.

Every iteration checks exact import commit counts, every root target and every
restored payload. A batch must produce exactly one commit; individual imports
must produce one commit per request. All 24 cases passed in each of three
optimized runs, for 72 successful cases total.

## Reproduction

The cases are registered in `benchmarks/manifest.json` under `core-primitives`
and included in `benchmark all --suites core-primitives`. The focused commands
below reproduce this investigation without running the other core benches.
Run from the retained worktree. To regenerate summaries from the saved samples,
extract the archive in this report directory first:

```sh
cd benchmarks/reports/2026-10-05-artifact-batches
tar -xzf criterion-raw.tar.gz
python summarize.py
```

To repeat the measurements, run these commands from the repository root:

```sh
batch_report=benchmarks/reports/2026-10-05-artifact-batches
for batch_run in 1 2; do
  CRITERION_HOME="$PWD/$batch_report/criterion-run-$batch_run" \
    cargo bench -p casita --offline --target-dir /tmp/casita-batch-target \
    --bench artifact_batches -- --warm-up-time 1 --measurement-time 3 --noplot \
    > "$batch_report/run-$batch_run.log" 2>&1
done
python "$batch_report/run_reverse.py" \
  --binary /tmp/casita-batch-target/release/deps/artifact_batches-fc50b431afa7d881
python "$batch_report/summarize.py"
```

The benchmark binary hash suffix can change after rebuilding. Use the executable
reported by Cargo in `run-1.log`. Each case collects 10 Criterion samples with
one second of warmup and a three-second measurement target. Criterion extends
collection when ten samples cannot fit within that target; the raw logs record
these extensions. Runs 1 and 2 use the original case order in one process.
Run 3 reverses the exact same cases and starts one new benchmark process per
case; no concurrent benchmark workloads were started by this session.

For correctness-only validation:

```sh
cargo test -p casita --offline --target-dir /tmp/casita-batch-target \
  --bench artifact_batches -- --test
```

Next, prefer switching Cargo's callers to lists when multiple artifacts are
available. An alternative is extending the permanent corpus with representative
Cargo artifacts and measuring the complete JSON-RPC import/restore path.
