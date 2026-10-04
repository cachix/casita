# WAL3 commit preparation

Moving the owned cumulative tail avoids another clone on ordinary commits.
Tails with zero or more than eight deltas select a checkpoint before serialization.
The serializer copies each encoded entry into the record as it is produced,
removing the intermediate collection of encoded entries.

## Measurements

The eight-delta case improved in every measured process: the median paired
speedup was 1.78, with a range of 1.65 to 1.84. The ninth-delta case returns
before serializing its tail; its candidate timing approaches the empty-tail
measurement floor. The three byte-limit cases have no consistent speedup.
Collection timings are too small and variable to support a speedup claim.

Ten fresh processes each ran 1,000 paired iterations of every case, alternating
which implementation ran first within each iteration. The table shows the median
of each process's mean preparation time in microseconds. Paired speedups divide
the previous implementation's total time by the candidate's in the same process;
the reported speedup is the median of those ten ratios.

| Case | Previous microseconds | Candidate microseconds | Paired speedup | Paired range |
|---|---:|---:|---:|---|
| Collection (empty replacement tail) | 4.08 | 3.02 | 1.28 | 0.28 to 6.30 |
| 8 deltas | 842.77 | 469.97 | 1.78 | 1.65 to 1.84 |
| 9 deltas | 309.17 | 4.49 | 59.37 | 53.99 to 87.87 |
| 1 MiB minus 1 byte | 2600.43 | 2541.84 | 1.01 | 0.99 to 1.04 |
| Exactly 1 MiB | 2325.01 | 2331.97 | 1.00 | 0.98 to 1.03 |
| 1 MiB plus 1 byte | 185.44 | 184.85 | 1.00 | 0.87 to 1.03 |

Absolute timings varied substantially across processes on this shared host.
For example, the candidate eight-delta means ranged from 157 to 1,052
microseconds even though the paired ratios consistently favored the candidate.
The empty-tail and ninth-delta candidate measurements include timer overhead.
These numbers measure preparation wall time. WAL appends, shard compaction,
retry admission, and S3 I/O are outside timing, as are fixture construction,
input cloning for each measurement, decoding checks, and output destruction.
RSS is the whole process with fixtures and audits, not per-implementation memory.

## Build and fixtures

The measured source is commit `5cfb313de3a5c412b9a1af85e54ee5afadaa4fed`.
Casita was compiled at optimization level 3 with debug information disabled;
debug assertions and overflow checks remained enabled. Dependencies used the
cached development profile. Both implementations run in the same test executable.
The baseline retains the previous cloning and serializer in the benchmark source.
This build configuration targets the Casita codec and preparation methods measured
here. Full repository performance requires a separate workload and release build.

The host CPU was AMD Ryzen 7 7840S with Radeon 780M Graphics and the compiler was
`rustc 1.97.1 (8bab26f4f 2026-07-14)`; the raw JSON retains the environment and
executable digest. Each delta contains 16 object records, one root change,
one validation key, and a payload catalog byte field. The count cases use
64 KiB catalog fields per delta. The byte cases use one delta padded to produce
an encoded record one byte below, at, or one byte above the 1 MiB limit.
Collection has a prior eight-delta tail and produces an empty replacement tail.
These inputs exercise the codec rather than actual repository imports.

Each iteration asserts exact record bytes, exact tail data, and checkpoint
selection against the previous implementation. Every accepted record must decode
to its exact base position and tail. The normal regression test also checks that
ordinary commits move the owned prior tail and that collection discards it.

## Reproduce

From the repository root, build the optimized Casita test executable:

```sh
cargo test --offline \
  --config 'profile.dev.package.casita.opt-level=3' \
  --config 'profile.dev.package.casita.debug=0' \
  -p casita --features s3 --lib --no-run --message-format=json \
  > /tmp/wal3-build.jsonl
wal3_probe="$(python -c 'from pathlib import Path; from benchmarks.suites.pack.catalog import parse_probe_binary; print(parse_probe_binary(Path("/tmp/wal3-build.jsonl").read_text()))')"
python -m benchmarks.cli run wal3-commit-preparation \
  --probe-binary "$wal3_probe" --iterations 1000 --repetitions 10 \
  --output /tmp/wal3-preparation.json
```

The permanent suite is registered in `benchmarks/manifest.json` and is included
in `benchmark all`. To reuse this build with the all-suite runner:

```sh
mkdir -p /tmp/wal3-binaries
ln -sf "$wal3_probe" /tmp/wal3-binaries/casita-lib-test
python -m benchmarks.cli all --suites wal3-commit-preparation \
  --profile standard --repetitions 10 --bin-dir /tmp/wal3-binaries \
  --output /tmp/wal3-preparation-all
```

The all-suite standard profile runs 100 iterations per case. That shorter run
is retained in `standard.json`; the conclusions above use `extended.json`
with 1,000 iterations. `build.json` records the build flags and source revision,
`artifacts.json` identifies the executable, and `execution.json` retains the
successful all-suite completion ledger. Both runs passed all six correctness gates
in every process. The prior unoptimized smoke run also completed two processes.

Validation of the change included four focused Rust tests covering codec bounds,
cumulative tails, ninth-commit checkpointing, and lazy shard loading after reopen;
25 benchmark runner/CLI tests; formatting; and the repository's Clippy hook with
all features and all targets and warnings denied.
