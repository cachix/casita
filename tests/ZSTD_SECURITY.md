# zstd security and compatibility regressions

These checks cover RUSTSEC-2026-0391 (abandoned streaming contexts),
RUSTSEC-2026-0398 (seekable offsets beyond EOF), and RUSTSEC-2026-0403
(buffer positions after a failed legacy decode). Error and bounds probes use
initialized slices, so their negative cases do not expose uninitialized Vec
storage. The ordinary past-EOF assertion precedes extreme offsets.

From the repository root:

```sh
cargo test -p casita --no-default-features --features native --test zstd_security
cargo test --manifest-path tests/zstd_seekable/Cargo.toml
cargo test --manifest-path tests/zstd_parquet/Cargo.toml
python3 tests/check_zstd_dependencies.py
cargo deny --frozen check
```

The seekable fixture is a separate workspace: it exercises the dependency's
optional API without enabling seekable in Casita. The WAL fixture exercises
both Snappy and zstd, relative and absolute offsets, exact record bodies and
setsums, and malformed-fragment classification. It does not contact S3.

Casita's direct dependency floor does not repair Parquet 55.2's independent
`zstd ^0.13` requirement. The local candidate also uses a task-local Parquet
55.2 manifest overlay requiring zstd 0.14.3; all Parquet runtime source and
features remain unchanged. The same overlay must apply to every workspace
being tested. These commands do not establish a complete upstream or
independently resolved downstream repair until that dependency route is
available without the local override. Do not disable the zstd codec, remove
features, or add advisory ignores to make the checks pass.

Run the unchanged repository suite and integrity tests in addition to these
focused regressions. A clean dependency graph does not establish application
exploitability, paced-workload performance, production qualification, or
upstream adoption.
