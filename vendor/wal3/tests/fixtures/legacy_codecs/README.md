# Historical WAL codec fixtures

These files were generated with Arrow/Parquet **55.2.0**, before the 57.3.2
upgrade. They contain synthetic records only: a short string, an empty body,
three non-UTF-8 bytes, and a deterministic 256 KiB body. No operator payloads
or state are included. Each codec has both absolute and relative offsets.
The base is 987 and every record has timestamp 123456.

The standalone reader has **only WAL3 as a dependency**. Do not add direct
Arrow, Parquet or codec dependencies there: Cargo feature unification could
hide missing production codecs. The same assertions run as an ordinary WAL3
integration test and in the standalone workspace registered in Rust CI.

Run the regression from the repository root:

```sh
cargo test --locked --manifest-path rust/wal3/tests/standalone-codecs/Cargo.toml --test legacy_codecs
```

The separate legacy **writer-only** generator is kept solely to reproduce
these frozen bytes. It is not a runtime dependency or part of the reader's
feature graph. It verifies each file's four-row count and column codec.
Regenerate deliberately, then review any fixture hash changes:

```sh
cargo run --locked --manifest-path rust/wal3/tests/fixtures/legacy_codecs/generator/Cargo.toml -- rust/wal3/tests/fixtures/legacy_codecs
```

| File | Bytes | SHA256 |
| --- | ---: | --- |
| absolute-snappy.parquet | 14489 | `b0abb75586d9582e4ac5896a710f5bb0ca8312de7eade07ef7db7cde15f83a6d` |
| absolute-zstd.parquet | 1464 | `edbfa7e028d22679868569358e19e7768ec0d085c9de98145c477bc780e95f8e` |
| relative-snappy.parquet | 14517 | `4eb63e26d42d211a2a82ab0755c93a9c87cd5770a1d5c7b296bf56083fdc840f` |
| relative-zstd.parquet | 1492 | `5f4ddf2767370ab388a8794bdaf9a8059c3796fbdf412651b9a0907b9128a5ec` |
