---
title: CLI Reference
description: Command syntax, important defaults, and repository effects.
---

Install from a source checkout with `cargo install --path crates/casita`.
Optional commands need the [corresponding Cargo feature](../cargo-features/).
For a first walkthrough, start with the [CLI guide](../../cli/).

| Task | Commands |
| --- | --- |
| Store and read | [`import`](#import), [`object show`](#object-show), [`tree list`](#tree-list), [`cat`](#cat), [`checkout`](#checkout) |
| Retain and maintain | [`root`](#named-roots), [`gc`](#gc), [`vacuum`](#vacuum), [`fsck`](#fsck), [`holds`](#holds) |
| Move data | [`sync`](#synchronization), [`archive`](#portable-casitar-archives) |
| Integrate | [`run`](#run), [`ipc`](#ipc), [`git`](#native-git) |

## Global syntax

```text
casita [GLOBAL OPTIONS] COMMAND [COMMAND OPTIONS]
```

Put global options before the command. `-h`/`--help` prints command-specific
help; `-V`/`--version` prints the binary version.

| Option | Effect |
| --- | --- |
| `--repository PATH` | Use this local repository instead of the per-user default. `sync` uses `--from` and `--to` instead. |
| `--log-filter DIRECTIVES` | Select trace levels; overrides `RUST_LOG`. Defaults to `RUST_LOG`, then `off`. |
| `--log-format compact\|json` | Write compact or newline-delimited JSON trace events to stderr. Default: `compact`. |
| `--spill-memory-objects OBJECTS`, `--spill-bytes BYTES` | Bound temporary traversal state. |
| `--pack-target-bytes BYTES` | Set the approximate compressed size of an immutable chunk pack. |
| `--pack-cache-bytes BYTES` | Set the S3 compressed-chunk cache size; `0` disables it. |

The pack cache option is intended for S3 endpoints used by `sync` or
remote verified `cat`.
Spill options affect temporary traversal state, not object identity.

For example, `casita=info` records operation outcomes and `casita=debug`
adds internal decisions and contention. Casita's own trace fields omit paths,
root names, credentials, and payload contents, but dependency traces may have
different rules. Use a scoped filter when sharing logs:

```console
$ casita --log-filter casita=info --repository ./cache fsck --dry-run
$ RUST_LOG=casita=debug casita --log-format json \
    --repository ./cache gc --dry-run
```

Without `--repository`, Casita uses the OS application data directory,
falling back to `$HOME/.casita` and then `.casita-data` when needed. It also
searches the current directory and its parents for a `.casita` workspace
marker. The marker scopes root names by a workspace UUID; bytes stay in the
per-user repository. An explicit repository skips workspace scoping. The
[`run` command](#run) uses the marker only for optional name shortcuts.

## Repository and filesystem commands

### `init`

```text
casita [--repository PATH] init
```

Without `--repository`, creates or reuses the current directory's `.casita`
workspace marker after opening the global profile. A new marker and its
directory are synced before `init` succeeds. With `--repository`, creates or
opens that local profile and prints its path and current repository revision.

### `import`

```text
casita [--repository PATH] import [-i IMPORTER] PATH [--root NAME] \
  [--retention permanent|evictable] [--filesystem-rehash]
```

Without `-i`, Casita recognizes Git repositories from their metadata,
probes regular-file headers for Casitar or tar, and otherwise imports a
directory as a filesystem tree. Use `-i filesystem|tar|git|casitar|oci` to select
an importer explicitly. Standard input (`-`) requires `-i`.

For a filesystem import, `--root` names the tree. When omitted, Casita derives
a name below `auto/` from the canonical source path. In a workspace, both
explicit and automatic names are scoped by its UUID. Importing the workspace
directory omits its `.casita` marker.
`--retention` publishes the root and policy together for filesystem and tar
imports. Roots are permanent by default; omitting the flag keeps an existing
root's policy. Git, Casitar, and OCI imports do not accept this flag.

The filesystem importer prints the directory key and does not follow
symlinks. It may reuse a file whose device, inode, size, and timestamps match
the previous import. `--filesystem-rehash` reads every file again. Use
`--filesystem-concurrency FILES` to change the number of files ingested at
once (default 16), and `--chunk-upload-concurrency CHUNKS` to bound uploads
per blob writer (default 32). See [Import semantics](../../concepts/imports/)
for the reuse assumption.

### `import -i oci`

```text
casita [--repository PATH] import -i oci IMAGE --root NAME \
  [--oci-platform OS/ARCH[/VARIANT]] [--oci-http] \
  [--oci-max-blob-bytes BYTES] [--oci-max-total-blob-bytes BYTES] \
  [--oci-rootfs-root NAME] [--oci-rootfs-max-bytes BYTES] \
  [--oci-rootfs-max-entries COUNT]
```

Requires the `oci` Cargo feature. The importer selects one platform's manifest
and downloads its config and compressed layer blobs into a standard OCI image
layout. Layer blobs stream into storage. It uses anonymous registry access
and HTTPS by default.

`--root` names the OCI image layout, including its original layer archives.
`--oci-rootfs-root` optionally names the merged container root filesystem,
which can be checked out or mounted. The names must differ. Casita applies
layer whiteouts and verifies uncompressed DiffIDs before publishing both
roots atomically. The filesystem bounds limit decoded tar bytes and entries;
the blob bounds limit the original compressed downloads.
See [Import an OCI Image](../../guides/oci/) for the output and limits.

### `import -i tar`

```text
casita [--repository PATH] import [-i tar] FILE|- --root NAME \
  [--retention permanent|evictable] \
  [--tar-max-archive-bytes BYTES] [--tar-max-entries COUNT] \
  [--tar-max-in-flight-files COUNT] \
  [--tar-max-path-bytes BYTES] [--tar-max-file-bytes BYTES] \
  [--tar-max-total-file-bytes BYTES] [--tar-max-sparse-expansion-bytes BYTES]
```

Streams one already-decompressed POSIX tar archive into the canonical
filesystem model without extracting it to disk. A tar file is detected from
its first valid header; use `-i tar` for standard input. The command accepts regular
files, directories, symlinks, hard links, and old-GNU sparse files; PAX GNU
sparse members are rejected. `FILE` may be `-` for standard input.

The root name is required and is committed only after complete validation.
Defaults bound raw archive bytes (1 TiB), entries (1,000,000), path bytes
(4096), individual files (256 GiB), total file bytes (1 TiB), and sparse-hole
expansion (256 GiB). The corresponding flags override those bounds.

### `object show`

```text
casita [--repository PATH] object show KEY
```

`KEY` must be a full generic object key. Output includes the logical key,
physical payload ID, payload size, ordered forward links, and current closure
status.

### `tree list`

```text
casita [--repository PATH] tree list KEY
```

Lists the direct entries of a canonical filesystem directory. `KEY` may be a
full `casita.directory.v1` key or a short `blake3-...` directory digest. The
command requires a complete valid closure.

Entry output identifies directories (`d`), regular files (`f`), executable
files (`x`), and symlinks (`l`).

### `run`

```text
casita [--repository PATH] run ROOT [--bin NAME_OR_PATH] [-- ARGS...]
```

Runs an executable from an existing named directory root. Casita searches the
tree recursively and starts its sole executable. With several candidates, use
`--bin` to choose a unique filename (`uv`) or relative path (`release/uv`);
`./uv` selects a root-level file. It fails when no executable is found.

On Unix, candidates need an executable mode bit. On Windows, they need a
`.exe` or `.com` extension. Internal executable symlinks can be selected,
but discovery skips broken links, links outside the tree, and directory
symlinks. Selection does not search `$PATH` or prefer a build profile.

```console
$ casita run cargo/builds/uv -- --version
$ casita run go/builds/server --bin server -- --port 8080
```

To shorten names, add run settings to the `.casita` marker created by
`casita init`, keeping its header and workspace UUID:

```text
casita-workspace-v1
workspace = "12345678-1234-4234-8234-123456789abc"

[run]
default-scope = "cargo"

[run.scopes]
cargo = "cargo/builds"
go = "go/builds"
```

| Input | Resolved repository root |
| --- | --- |
| `cargo:uv` | `cargo/builds/uv` |
| `go:server` | `go/builds/server` |
| `uv` | `cargo/builds/uv`, using the optional default scope |
| `cargo/builds/uv` | `cargo/builds/uv`, unaffected by the default |
| `/uv` | Literal root `uv`, bypassing all shortcuts |

Only bare names use the optional default scope. `SCOPE:NAME` selects a
configured prefix, and a leading `/` requests a literal name. The closest
marker wins; invalid scopes or configuration fail instead of falling back to
another name.

`run` resolves the root once and retains that exact graph while the application
runs. It materializes the tree under the repository's `runs/` directory and
normally removes it on exit. A crash may leave temporary files there. The
child inherits the caller's working directory, environment, and standard
streams. Arguments after `--` go to the child without a shell. Casita returns
its exit code and forwards common termination signals on Unix. Execution uses
the caller's permissions. See the [run guide](../../guides/run/) for a complete
workflow and binary-selection details.

### `ipc`

```text
casita [--repository PATH] ipc [--max-connections COUNT] \
  [--frame-timeout-secs SECONDS] [--response-timeout-secs SECONDS]
```

Starts the local JSON-RPC service for artifact import and restore. Defaults are
64 connections, 60 seconds to receive a request, and 30 seconds to write a
response. See [Local IPC](../../integrations/ipc/) for the endpoint and protocol.

### `cat`

```text
casita [--repository PATH] cat KEY [--verified [--from ENDPOINT]]
```

Writes a blob's exact plaintext payload to standard output. `KEY` may be a full
generic key or a short `blake3-...` blob digest. Diagnostics go to standard
error, so stdout may be redirected safely. `--verified` authenticates blocks
before writing them. With `--from`, it reads a verified raw blob from a
local, SSH, or S3 repository instead of the selected local repository;
`--from` requires `--verified` and a raw blob key.

### `checkout`

```text
casita [--repository PATH] checkout KEY DIR [--no-root]
```

Materializes a complete canonical directory into `DIR`. The target is created
if absent and must otherwise be empty. Checkout first writes a sibling staging
directory on `DIR`'s filesystem and renames it into place. A target that
cannot represent exact stored names, for example because of case folding or
Unicode normalization, fails without a partial checkout. `KEY` accepts a full
directory key or a short directory digest.

Every directory, file, and link is created relative to one open handle on the
staging root, so a component swapped for a symlink, junction, or other reparse
point while the checkout runs cannot redirect a write outside it. `DIR` itself
must be a real directory: a path that is already a link is refused rather than
written through. The path leading to `DIR` is the caller's own authority, and a
link stored inside the materialized tree is created as a link, not followed.
Windows refuses to materialize a stored link whose target is absolute.

By default, successful checkout also registers an `auto/checkout/...` root
derived from the canonical destination path. That root retains the materialized
closure until explicitly removed. `--no-root` skips this step.

Checkout does not flush the materialized tree to stable storage. A power loss
or operating-system crash soon after checkout can leave `DIR` absent,
incomplete, or holding truncated files, and can leave a `.casita-checkout-*`
staging directory beside it. The repository keeps the graph while a root
retains it: remove both directories and check out again.

## Named roots

Roots retain the complete forward closure of one exact object.

### `root set`

```text
casita [--repository PATH] root set NAME TARGET [--retention permanent|evictable]
```

Atomically sets or replaces `NAME`. `TARGET` may be a full object key or a
short filesystem digest. A short digest is rejected as ambiguous if matching
blob and directory records both exist. The target closure must be complete and
valid. Roots are permanent by default. `--retention` sets the policy in the
same commit as the root. Without it, replacing a root keeps its existing policy.
Evictable retention requires the local metadata backend.

### `root retention`

```text
casita [--repository PATH] root retention NAME permanent|evictable
```

Changes the policy of an existing local root. A permanent root remains until
explicitly removed. An evictable root may be released by local disk-pressure
collection, after which its name becomes a cache miss.

### `root rm`

```text
casita [--repository PATH] root rm NAME
casita [--repository PATH] root rm --prefix PREFIX
```

The first form removes one exact name. The second removes every name equal to
or below `PREFIX` on root-name segment boundaries and prints each removed name.
The two selectors are mutually exclusive. Removing a root makes data eligible
for collection; it does not immediately delete objects.

### `root ls`

```text
casita [--repository PATH] root ls [PREFIX] [--long]
```

Lists targets and names. An optional positional `PREFIX` restricts output to
equal or descendant root names. `--long` also displays `permanent` or
`evictable` for each root.

## Collection and integrity

### `holds`

```sh
casita holds PATH [--json]
casita holds s3://BUCKET/PREFIX [--json]
```

Lists online data pins, released pin history, collector ownership, prune fences,
and deletion claims without waiting for repository admission. Local inspection
requires an existing repository; S3 additionally requires the `s3` feature.
`--json` emits an object with `collectors`, `state`, and `coordination` fields.
Each ledger has its own revision and exact tokens. Pin scopes include snapshot
generations or closure roots; catalogs are identified by digest and encoded size.
S3 collector entries include diagnostic writer names and legacy exclusivity flags.
Listing never releases an existing hold or declares its owner dead. See
[S3 maintenance](/guides/s3-maintenance/) for diagnosis and explicit recovery.

### `gc`

```text
casita [--repository PATH] gc [--dry-run]
```

Collection starts from every named root and active data pin. `--dry-run` takes exclusive ownership,
runs the same mark plan, and reports removable logical records, payloads, and
chunks without changing state. Without it, Casita first commits the logical
prune and then removes unreferenced physical data.

Collection can proceed during mutations, transfers, and retained reads, preserving
the data protected by their pins. Collectors serialize with other collectors.
Starting a mutation on the standard local profile attempts nonblocking collection
when disk usage has reached 80%. After collecting existing garbage, it releases
least recently used evictable roots and vacuums until disk usage falls below
75% or no eligible roots remain. Explicit `gc` and `vacuum` preserve all named
roots regardless of their retention policy.

### `vacuum`

```text
casita [--repository PATH] vacuum
```

Runs collection and forces reclamation of garbage deferred inside sparse
packs. Use this when ordinary collection has left physical pack space to
reclaim.

### `fsck`

```text
casita [--repository PATH] fsck [--audit-only | --dry-run] [--source REPOSITORY]
```

`--audit-only` checks logical records, root closures, payload identity and
size, format relations, manifests, chunks, and unreferenced residue without
running the separate repair pass. It is the appropriate mode for routine
integrity measurement and is mutually exclusive with `--dry-run` and
`--source`.

First, `fsck` verifies every physical payload representation named by the
current logical snapshot. It rebuilds a corrupt existing Bao outboard and,
when `--source` names a local replica with the expected fully verified payload,
repairs a missing or corrupt local representation. It then checks logical
records, root closures, payload identity and size, format relations, manifests,
chunks, and unreferenced residue. `--dry-run` reports physical repairs without
writing.

The repair pass never changes object records or roots, never repairs an
unrelated I/O failure, and never invents logical state from a payload scan. It
pins source and destination data for the operation, preserving replacement inputs
while collection and ordinary readers continue. `fsck` prints a summary
and each deterministic finding. Reachable corruption makes the command fail.
Collectible residue and unavailable format verifiers are reported but do not,
by themselves, make the repository unhealthy. See [Errors and Integrity](../errors/).

## Portable Casitar archives

All archive commands enforce finite defaults: 1 TiB for the complete stream,
256 GiB for one plaintext payload, 1 TiB for total plaintext payloads, and
1,000,000 each for payload and record frames. Override them with
`--max-archive-bytes`, `--max-payload-bytes`,
`--max-total-payload-bytes`, `--max-payloads`, and `--max-records`. The frozen
4 MiB header and 256 MiB encoded-record ceilings still apply.

Successful commands print the complete-file BLAKE3 digest and structural
counts. `--json` selects the stable `casita.archive.v1` report schema.

### `archive create`

```text
casita [--repository PATH] archive create \
  [--root NAME]... [--object KEY]... \
  --output FILE|- [--force] [--json]
```

At least one selector is required; named roots and exact generic object keys
may be mixed and repeated. Casita resolves names and verifies every selected
closure through one source snapshot, deduplicates their union, then writes
payloads and records in canonical order. Equal root sets and logical closures
produce equal archive bytes regardless of selector order or traversal spill.

File output is staged, synced, and atomically published, and its directory is
synced before the command succeeds. It refuses an existing destination by
default, including one created concurrently; `--force` selects atomic
replacement. `--output -` writes only archive bytes to stdout and sends the
human report to stderr. `--json` and `--force` are rejected for stdout output.

### `archive inspect`

```text
casita archive inspect FILE|- [--json]
```

Consumes and hashes the complete stream, requiring canonical framing, payload
identities and lengths, section ordering, record encodings, the explicit end
marker, and strict EOF. A successful result is labeled `structural`: it does
not claim namespace reproduction, exact reachability, or complete root
closures. This command does not open the configured repository. `-` reads
archive bytes from stdin.

### `archive verify`

```text
casita archive verify FILE|- [--json]
```

Runs the full receiver import algorithm in an isolated temporary repository.
Success means every payload hash, namespace-produced record, direct-link
relation, exact record/payload membership rule, and declared closure verified.
The temporary repository is discarded, and the configured durable repository
is never opened or mutated. `-` reads from stdin.

### `archive import`

```text
casita [--repository PATH] archive import FILE|- \
  (--root NAME... | --root-prefix PREFIX) [--replace] [--json]
```

Destination naming is always explicit. Repeat `--root NAME` exactly once per
archive root in the canonical order printed by `inspect`, or use
`--root-prefix PREFIX` to map them to `PREFIX/0`, `PREFIX/1`, and so on. There
is no default prefix and no digest-derived or `auto/` naming.

The command streams every payload through BLAKE3, reuses exact physical
payloads when possible, stages records through the registered namespace
verifiers, checks the exact declared union closure, and publishes every mapped
root together in one repository revision. By default all names must be absent.
`--replace` captures their values before ingestion and replaces them only if
every value remains unchanged at publication. Failure publishes no mapped root
and may leave only receiver-verified unrooted residue for ordinary collection.

### `import -i casitar`

```text
casita [--repository PATH] import [-i casitar] FILE|- \
  (--casitar-root NAME... | --casitar-root-prefix PREFIX) [--casitar-replace]
```

This is the common importer-command form of Casitar restoration. A file with
the Casitar header is detected automatically; use `-i casitar` for standard
input. It has the
same all-or-nothing destination mapping and verification behavior as `archive
import`, but every Casitar-specific option is namespaced:
`--casitar-root`, `--casitar-root-prefix`, `--casitar-replace`,
`--casitar-max-archive-bytes`, `--casitar-max-payload-bytes`,
`--casitar-max-total-payload-bytes`, `--casitar-max-payloads`, and
`--casitar-max-records`. It does not accept `--root`; use the Casitar mapping
options instead. See [Import a Casitar Archive](../../guides/casitar/) for
examples and Rust usage.

## Synchronization

```text
casita sync --from ENDPOINT --to ENDPOINT \
  [--from-blobs ENDPOINT] [--writer NAME] \
  [--object KEY]... [--root NAME]... \
  [--path PATH [--destination-root NAME]] [--shallow] [--incremental]
```

`--from` supplies roots and object records. `--from-blobs`, when present,
supplies payloads from a second Casita repository; it must contain the exact
bytes required by those records. Sources accept local paths, S3 URLs
(`s3://BUCKET/PREFIX`), or SSH URLs
(`ssh://[user@]host[:port]/absolute/path`). Destinations accept local paths
or S3 URLs. S3 needs the `s3` feature; SSH sources need `ssh` on both machines
and a remote `casita` on the SSH user's `PATH`. `--writer` sets the
diagnostic WAL writer name for S3; otherwise `CASITA_WRITER` or a generated
name is used.

Select at least one `--object` or `--root`; both are repeatable. Objects use
full keys and copy their complete forward closure unless `--shallow` is set.
Roots always copy complete closures and move under the same names only after
receiver verification. `--incremental` reuses complete destination closures
without auditing their descendants at the source. Intermediate object batches
may remain after failure, but requested roots do not move. Sync does not
remove destination data.

`--path PATH` requires exactly one filesystem root and cannot be combined
with `--object` or `--shallow`. Only the selected file or directory closure
is copied; verified path ancestors stay at the source. Without
`--destination-root`, the copied closure is unrooted and collectible. Inline
symlinks have no standalone key and cannot be rooted.

See [Synchronize Repositories](../../guides/sync/) for examples and failure
behavior.

## Native Git

### `import -i git`

```text
casita [--repository PATH] import [-i git] SOURCE \
  [--git-view VIEW] \
  [--git-ref FULL_REF]... \
  [--git-max-cached-pack-bytes BYTES] \
  [--git-concurrency COUNT] [--git-max-buffered-bytes BYTES]
```

Requires `cli,git`. `SOURCE` is a local working tree or bare repository and is
detected from its metadata. When auto-detected, an omitted `--git-view` uses
the source directory basename. Each
`--git-ref` is a full canonical name such as `refs/heads/main`. With no explicit
refs, Casita selects local branches and tags. The command verifies native
objects, publishes one immutable view, and atomically sets `git/VIEW`.
When one verified source pack exactly matches that view, Casita retains packs
up to 8 GiB by default as a rebuildable, byte-for-byte full-clone cache. This
can substantially accelerate large clones but may nearly double stored bytes
for incompressible repositories. Set `--git-max-cached-pack-bytes 0` to disable
the cache or lower the limit to fit the storage budget.
`--git-concurrency` bounds staged objects (default 16);
`--git-max-buffered-bytes` bounds decoded bytes held by staging futures
(default 64 MiB). An object larger than the budget runs alone.

### `git show`

```text
casita [--repository PATH] git show VIEW
```

Prints the selected view key, object format, optional default ref, and every
direct or symbolic ref. It is available in the standard `cli` build.

### `git checkout`

```text
casita [--repository PATH] git checkout TREE DIR [--skip-gitlinks]
```

Safely materializes one exact type-qualified native Git tree into an empty
directory, under the same handle-rooted containment as `checkout` above.
Gitlinks fail by default; `--skip-gitlinks` materializes them as empty
directories. This command is available in the standard `cli` build.

### `git serve`

```text
casita [--repository PATH] git serve VIEW \
  [--listen ADDRESS] [--max-pack-bytes BYTES] \
  [--pack-compression-level 0..9]
```

Requires `cli,git-http`. The default listen address is `127.0.0.1:9418`. The
command binds one immutable view for read-only Git smart HTTP, prints its clone
URL as `http://<address>/<view>.git`, and serves until stopped. Generated
packs default to a 512 MiB limit and zlib compression level 6.

## Exit status and diagnostics

Successful commands return exit status `0`. Usage errors return `2`; runtime
failures return `1`. There are no per-category exit codes.

Runtime failures are written to stderr as:

```text
error[<category>]: <detail>
```

Usage errors use `error: <detail>`. Programs should prefer the Rust API's typed
errors when they need richer classification or retry guidance rather than
parsing CLI display text.
