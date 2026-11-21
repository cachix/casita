---
title: Reference
description: Exact command, API, identity, format, feature, storage, and error contracts for Casita.
---

Use this section when you need the exact spelling or behavior of a Casita
interface. For an introduction, start with the [Quick Start](../getting-started/),
then read the task-oriented [CLI](../cli/) or [Library](../library/) guide.

## Reference map

| Topic | Use it to answer |
|---|---|
| [CLI](./cli/) | Which commands and flags exist, what they print, and which features they require |
| [Rust API](./rust-api/) | Supported application workflows, identities, errors, and reports |
| [Experimental Rust API](./experimental-rust-api/) | Custom backend composition, formats, sessions, and protocols |
| [Identifiers](./identifiers/) | How object keys, digests, roots, revisions, paths, refs, and SSH endpoints are validated |
| [Object formats](./object-formats/) | Which namespaces are built in, how they derive identity, and which links they retain |
| [Cargo features](./cargo-features/) | Build requirements for the CLI, native storage, Git, SSH, S3, and experimental APIs |
| [Local repository](./local-repository/) | What the standard persistent profile stores and how processes coordinate |
| [Benchmarks](./benchmarks/) | How end-to-end performance results are generated, validated, and compared |
| [Errors and integrity](./errors/) | How to classify failures, decide about retries, and interpret `fsck` |
| [Reliability contract](./reliability/) | What survives process death, power loss, and corruption on each platform, and which tests check it |

## Contract levels

Casita documents three different kinds of fact:

- **Frozen encodings and semantics** are compatibility contracts. The
  [object formats reference](./object-formats/) summarizes them, and their
  golden vectors in the test suite pin the exact byte layouts.
- **Public Rust and CLI interfaces** describe the current pre-release
  revision. They may evolve even when the frozen data they read remains
  compatible.
- **Physical implementation details**, such as chunk sizes, compression, and
  local filenames, are operationally useful but are not logical object
  identity. They are labeled as private where they appear.

When a summary here and a normative specification disagree, use the
specification for durable encoding questions and report the documentation
discrepancy.

## Exact API documentation

The crate-level API map and item documentation live with the Rust source. Build
the complete local Rustdoc, including feature-gated items, with:

```console
$ cargo doc --all-features --no-deps --open
```

The [Rust API reference](./rust-api/) explains how those items compose; it does
not duplicate every method signature.
