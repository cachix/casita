# WAL dependency source mirrors

These local sources support a policy-preserving dependency experiment for Casita. They are not an upstream Arrow or Chroma release.

- `parquet/` was unpacked from a `cargo package --no-verify` archive of Apache Arrow-rs `60.1.0` with local repair commit `b3c59188fb06c00ff170e3d001cad8433111e6cb`. Cargo normalized its manifest. The packaged production source matches that commit byte for byte; trailing whitespace in `src/bin/parquet-fromcsv-help.txt` was removed for Git diff hygiene.
- `wal3/` is copied from Chroma commit `20edba455b4b093eb8271f36c49ba6e711c4d569`. It includes Chroma's Apache-2.0 license. Trailing whitespace in `README.md` and a source diagram was removed for Git diff hygiene. Its workspace-inherited manifest still needs to be made standalone before Casita can use the local copy.

Casita's frozen WAL allocation fixtures and tests remain in `crates/casita/tests/wal_allocation/`. The selected dependency graph must be checked with `cargo metadata`, `cargo-deny`, the 256 MiB child-process tests, historical WAL codec tests, and full Casita checks before any pin is adopted.
