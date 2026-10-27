---
title: Import a Tar Archive
description: Stream a tar archive directly into a verified canonical filesystem graph.
---

The tar importer creates the same canonical filesystem object model as a
filesystem import, without extracting untrusted paths to disk first. It accepts
regular files, directories, symlinks, and hard links in POSIX ustar/pax or GNU
archives. The `tar-codec` decoder requires UTF-8 paths and link targets and rejects
legacy v7 archives, streams mixing POSIX and GNU framing, and sparse files.
Other extensions follow `tar-codec`'s default decode policy, which rejects unknown
PAX vendor extensions. Any execute bit marks a regular file executable.

## CLI

`import` detects a tar file from its first valid header; `import -i tar` also
accepts `-` for standard input. The stream must
already be decompressed; for example, use a decompressor pipeline for a
`.tar.gz` input. The named root is installed only after the whole archive is
validated.

```console
$ casita --repository ./cache import release.tar --root releases/current
$ gzip -cd release.tar.gz | casita --repository ./cache import -i tar - \
    --root releases/current
```

The command defaults to the same finite archive, entry, pathname, file-byte,
and total-file-byte limits as `TarImportLimits`. Override
them with `--tar-max-archive-bytes`, `--tar-max-entries`,
`--tar-max-path-bytes`, `--tar-max-file-bytes`,
and `--tar-max-total-file-bytes` for a stricter deployment policy.
`--tar-max-sparse-expansion-bytes` remains available for compatibility; sparse
files are rejected regardless of its value.

File finalization is pipelined while the next tar body is read. At most 16
files are being copied, queued, finalized, or verified at once. Set
`--tar-max-in-flight-files 1` for serial processing, or choose another positive
limit to control the number of open writers. Archive parsing stays sequential;
the importer does not buffer whole files. Compression and chunk hashing use
the payload backend's existing workers.

## Rust

`TarImport` accepts an already-decompressed asynchronous tar stream, so a
`.tar.gz` or similar input must be decompressed before it reaches Casita. The
root is published only after the complete archive has been validated and
converted to a canonical directory graph.

```rust
# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
use casita::{Repository, RootName, import::TarImport, TarImportLimits};
use tokio::io::BufReader;

let repository = Repository::local("./cache").await?;
let archive = tokio::fs::File::open("release.tar").await?;
let report = repository
    .import(
        TarImport::new(BufReader::new(archive), RootName::try_from("releases/current")?)
            .with_limits(TarImportLimits::default()),
    )
    .await?;

println!("{} entries in {}", report.entries, report.root);
# Ok(())
# }
```

`TarImportLimits` bounds archive bytes, entries, one file, total file bytes,
and in-flight files (`max_in_flight_files`, default 16).
Set lower deployment-specific limits before accepting
untrusted archives.
