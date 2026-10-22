---
title: Capture and Restore a Filesystem Tree
description: Import, inspect, retain, refresh, and safely materialize a canonical filesystem graph.
---

This workflow turns one directory into a verified immutable graph, gives it a
durable name, and restores the exact graph later.

## Import and retain the tree

### CLI

```console
$ casita --repository ./cache import ./project --root projects/demo
```

Directories select the filesystem importer automatically. The command prints the resulting `casita.directory.v1` key. The named root
retains that directory and every object reachable through its verified links.

Imports normally reuse an earlier file result when device, inode, size, and
timestamps are unchanged. Force every file to be reread when that assumption
does not suit the source:

```console
$ casita --repository ./cache import ./project \
    --root projects/demo --filesystem-rehash
```

### Rust

`FilesystemImport` publishes the tree and its root in one operation. Use
`FilesystemImport::new(...).reread(true)` when every regular file must be read and hashed again.

```rust
# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
use casita::{Repository, RootName};

let repository = Repository::local("./cache").await?;
let root = RootName::try_from("projects/demo")?;
let tree = repository.import(casita::import::FilesystemImport::new("./project", root)).await?;

println!("{tree}");
# Ok(())
# }
```

## Inspect the graph

```console
$ casita --repository ./cache root ls projects
$ casita --repository ./cache object show casita.directory.v1:...
$ casita --repository ./cache tree list casita.directory.v1:...
$ casita --repository ./cache cat casita.blob.v1:...
```

`object show` exposes the logical key, physical payload, exact forward links,
and closure status. `tree list` interprets a canonical directory; `cat` writes
a blob's bytes to standard output.

## Restore safely

```console
$ casita --repository ./cache checkout casita.directory.v1:... ./restored
```

The destination must be absent or empty. Checkout first builds a sibling staging
directory on the destination filesystem, then renames it into place. It either
recreates the graph's exact names or fails without a partial checkout; this
catches case-folding, Unicode-normalization, and native-name conflicts on the
actual target filesystem. Checkout uses handle-relative writes so a path
component swapped for a symlink during materialization cannot redirect writes
outside the destination. Stored symlinks are recreated rather than followed.

Successful checkout creates an `auto/checkout/...` root by default. Use
`--no-root` only when another root already retains the graph or the restored
copy is intentionally disposable.

## Restore a runnable artifact

A verified tree can still depend on files outside the imported directory.
Casita stores a symlink's target without importing the file it points to, and
checkout recreates that target unchanged. Build tools may also embed absolute
workspace paths in generated files. An exact restore can therefore stop working
when the original workspace is moved or removed.

This POSIX example imports an output directory whose only file is a link to
an external dependency:

```sh
casita_demo=$(mktemp -d)
mkdir -p "$casita_demo/build/output" "$casita_demo/build/deps"
printf 'runtime data\n' > "$casita_demo/build/deps/data.txt"
ln -s "$casita_demo/build/deps/data.txt" "$casita_demo/build/output/data.txt"
casita --repository "$casita_demo/store" import "$casita_demo/build/output" \
  --root examples/external-link
```

Use the directory key printed by import in place of `casita.directory.v1:...`:

```sh
casita --repository "$casita_demo/store" checkout casita.directory.v1:... \
  "$casita_demo/restored" --no-root
mv "$casita_demo/build" "$casita_demo/retired-build"
test -L "$casita_demo/restored/data.txt"
test ! -e "$casita_demo/restored/data.txt"
```

Both tests succeed: the restored symlink exists, but its dependency is no longer
at the stored path. The named root retains the imported tree, not the external
file.

For reusable application artifacts, package the required files inside the tree
with paths that remain valid after relocation, or provision matching dependencies
at the destination. Check out into a fresh directory, move the original workspace
away, and exercise the restored application, including code that loads runtime
libraries or assets. A successful checkout alone does not establish that the
application will run. The [run guide](../run/) covers host compatibility and
runtime dependencies.

## Release data deliberately

```console
$ casita --repository ./cache root rm projects/demo
$ casita --repository ./cache gc --dry-run
$ casita --repository ./cache gc
$ casita --repository ./cache fsck
```

Removing a name only makes its unshared closure eligible for collection. The
dry run shows what a real collection would remove.

Read [Imports](../../concepts/imports/) for cache assumptions,
[Roots and Retention](../../concepts/roots-and-retention/) for liveness, and
the [CLI Reference](../../reference/cli/) for exact syntax.
