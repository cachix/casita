---
title: Casitar Archives
description: Portable, closure-complete offline interchange for Casita object graphs.
---

Rust examples on this page use the `experimental` Cargo feature and
`casita::experimental`. For the supported built-in workflows, see the
[Library guide](/library/).

Casitar moves a complete Casita object graph through a file or stream without
a live source repository. The archive names exact root objects and carries
their reachable records and plaintext payloads. Import verifies the graph
before publishing destination root names.

## Goals and format boundary

Use Casitar when the exchange must preserve Casita's exact object identities,
links, and complete reachable graph. Tar or ZIP is simpler for ordinary files;
Git bundles, NAR, CAR, and OCI layouts serve their respective native formats.

Casitar carries a union of the selected roots' closures. Shared records and
payloads appear once. Canonical root and frame ordering, with no timestamps or
producer metadata, makes the same graph produce the same archive bytes. Stream
limits bound resource use, and the destination chooses its own root names.

CAR does not itself require complete root closures or Casita's separate
namespace-qualified records and shared payloads. Casitar encodes those
requirements directly.

## What the archive contains

- A canonical, non-empty set of root `ObjectKey`s.
- Deduplicated plaintext payload frames in strictly ascending `BlobId` order.
- Deduplicated canonical `ObjectRecord` frames in strictly ascending
  `ObjectKey` order.
- An explicit end marker followed by strict EOF.

It does not copy `casita.sqlite`, physical chunks, compressed representations,
bao outboards, repository revisions, or local root names. Those details are
local policy or backend layout, not portable graph meaning.

Native Rust callers can use `CasitarReader` and `CasitarWriter` with arbitrary
Tokio streams. The codecs check frame lengths, payload hashes, ordering,
duplicates, limits, and strict EOF. A successful structural read alone does
not prove namespace semantics or closure completeness.

`Repository::export_casitar` accepts root names, exact object keys, or both.
It verifies the selected closures under one retention hold, then writes their
distinct payloads and records in canonical order. File export stages and syncs
a sibling temporary file, publishes it atomically, then syncs its directory
before reporting success. The CLI creates a new file by default and requires
`--force` to replace one.

## Verification and retention

`Repository::import(CasitarImport::new(...))` hashes every payload, verifies
every record against its namespace, and rejects missing or unrelated archive
entries. Only a complete, exact closure can publish all destination root
mappings together in one revision.

An interrupted import may leave verified, unrooted data for collection, but
publishes no selected root. Names must be absent by default. Replacement is
conditional on their preflight values remaining unchanged.

## CLI import

```console
$ casita --repository ./source archive create \
    --root releases/current --output release.casitar
$ casita archive inspect release.casitar
$ casita archive verify release.casitar
$ casita --repository ./destination archive import release.casitar \
    --root releases/imported
```

Create also accepts repeatable exact `--object KEY` selectors and `-` for
stdout. Inspect and verify accept `-` for stdin. `inspect` proves only canonical
framing, hashes, ordering, strict EOF, counts, and the complete archive digest;
it labels that result `structural`. `verify` runs the full importer in an
isolated temporary repository and labels success `verified`, without opening
the configured durable repository.

Import never invents a destination name. Repeat `--root NAME` exactly once per
canonical header root, or select `--root-prefix PREFIX` to map the roots to
`PREFIX/0`, `PREFIX/1`, and so on. The default refuses existing names;
`--replace` performs compare-and-swap replacement. `--json` emits the stable
`casita.archive.v1` report schema. Common maximum-archive, payload,
total-payload, payload-count, and record-count flags keep untrusted inputs
finite.

## Rust import

Open the reader first so the caller can inspect the archive's roots and choose
destination-owned names before the repository receives any payload. The
destination vector must contain one name for every header root, in its
canonical order.

```rust
# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
use casita::experimental::{
    CasitarReader, CasitarStreamLimits, Repository,
    RootName,
};

let repository = Repository::local("./destination").await?;
let input = tokio::fs::File::open("release.casitar").await?;
let reader = CasitarReader::open(input, CasitarStreamLimits::default()).await?;

let destinations = reader
    .header()
    .roots()
    .iter()
    .enumerate()
    .map(|(index, _)| RootName::try_from(format!("releases/imported/{index}")))
    .collect::<Result<Vec<_>, _>>()?;
let report = repository
    .import(casita::import::CasitarImport::from_reader(reader, destinations))
    .await?;

println!("imported {} record(s)", report.records_inserted);
# Ok(())
# }
```
