---
title: Local IPC
description: Local JSON-RPC protocol for importing and restoring artifacts.
---

The `casita ipc` service lets a local client import or restore artifacts by
name. Requests contain paths on the daemon's machine. The daemon reads or
writes the files; artifact bytes do not pass through JSON.

| Method | Purpose |
| --- | --- |
| [`rpc.initialize`](#initialize) | Negotiate protocol version and frame size. Call first on each connection. |
| [`artifact.import`](#import) | Store an artifact under a durable root. |
| [`artifact.restore`](#restore) | Restore a root to a local path. |
| [`artifact.checkout`](#checkout) | Check out a directory root, including a cache-miss response. |
| [`rpc.shutdown`](#shutdown-and-cancellation) | Close this connection. |

## Run the service

```console
$ cargo install --path crates/casita
$ casita --repository ./cache ipc
```

Without `--repository`, the service uses the CLI's per-user repository.
Only one service can listen for a repository at a time. Build with
`--features git` to enable Git import and restore.

## Transport and framing

The endpoint is a Unix-domain socket or Windows named pipe. Unix socket and
parent-directory permissions restrict access to the current user.

The endpoint suffix is the first 16 hexadecimal characters of the BLAKE3 hash
of the repository path's lossy UTF-8 representation. Unix uses
`/tmp/casita/cargo-<suffix>.sock`; Windows uses
`\\.\pipe\casita-cargo-v<suffix>`. Use the same repository path spelling as the
daemon when computing the endpoint.

Send one JSON-RPC 2.0 object per UTF-8 line, ending with LF (`\n`).
Responses use the same framing. CRLF, blank lines, and multiline JSON are
invalid. The JSON examples below represent individual lines; send an LF after
each one. Use absolute artifact paths to avoid dependence on the daemon's
working directory.

Requests on one connection run in order. Include an `id` to receive a response
with the same `id`; failures use `error` instead of `result`. Separate
connections can operate concurrently.

## Connection limits and deadlines

| Limit | Default | When exceeded |
| --- | --- | --- |
| Simultaneous connections, idle ones included | 64 | The new client receives one `-32000` error frame with a `null` `id`, then the connection closes |
| Time to deliver one complete request line, counted from the previous response (or from connect) | 60 seconds | The connection closes without a JSON-RPC response |
| Time for the client to drain one response | 30 seconds | The connection closes; an import that already returned its result has still been committed |

The receive deadline includes idle time. Reconnect and call
`rpc.initialize` again after a connection closes. Artifact operations
themselves have no deadline; the receive timer restarts after a response.

`casita ipc --max-connections`, `--frame-timeout-secs`, and
`--response-timeout-secs` change these defaults; every value must be positive.
The service is part of the CLI.

## Initialize

Call `rpc.initialize` first on every connection. `versions` must contain
`1`. Optional `max_frame_bytes` defaults to 1,048,576 (1 MiB) and accepts
4,096 through 1,048,576 bytes, excluding LF. The server rejects longer
requests; this limit does not currently bound responses.

```json
{"jsonrpc":"2.0","id":1,"method":"rpc.initialize","params":{"versions":[1],"max_frame_bytes":1048576}}
{"jsonrpc":"2.0","id":1,"result":{"version":1,"max_frame_bytes":1048576,"capabilities":["artifact.checkout","artifact.import","artifact.restore"],"importers":["filesystem","blob","copy","nar","filesystem_nar","tar","casitar"]}}
```

`importers` also contains `"git"` when the daemon has the `git` feature.
Initialization succeeds only once per connection.

## Import

`artifact.import` takes named parameters. `importer` selects the input type
(default: `filesystem`); `options` holds importer-specific settings.
Required fields are listed below. Fields may be flat or grouped under
`parameters`:

```json
{"jsonrpc":"2.0","id":2,"method":"artifact.import","params":{"importer":"blob","parameters":{"path":"/build/result.bin","root":"build/blob"},"options":{}}}
```

Keep `importer` and `options` outside `parameters`, and do not repeat a field
in both places. Unknown importers, fields, and options fail before mutation.
Every successful import publishes a durable root (Git uses `git/<view>`),
so the result survives source deletion, restart, and collection. A failed
import does not replace a root, but may leave collectible staged objects.

### Filesystem

Required fields: `path` (directory) and `root` (destination root name).

```json
{"jsonrpc":"2.0","id":2,"method":"artifact.import","params":{"importer":"filesystem","path":"/build/output","root":"build/latest","options":{"reread":true,"exclude":".control"}}}
{"jsonrpc":"2.0","id":2,"result":{"object":"<object-key>"}}
```

- `reread`: boolean, default `true`, preserving the original IPC behavior.
  Set `false` to allow reuse of unchanged files from the local ingest cache.
- `exclude`: optional single exact relative path to omit, not a glob or list.

The import publishes the directory under `root`. Legacy parameters such as
`{"root":"build/latest","path":"/build/output"}` remain accepted, with the
same result shape.

### Tar

Required fields: `path` (a tar or gzip-compressed tar file) and `root`.
The daemon streams the archive from disk without extracting it first.
`options.compression` is `"none"` (default) or `"gzip"`; filenames do not select
compression. Gzip decompression streams directly into the tar importer.
All gzip members and their checksums must validate before publication.
Truncated members, corrupt trailers and trailing non-gzip bytes fail.

For gzip, `options.max_compressed_bytes` separately bounds compressed input
(default 1 TiB). It is rejected with compression `"none"`.
`options.limits.max_archive_bytes` always bounds decompressed tar bytes,
including padding through EOF. Neither limit silently truncates the stream.
For example:

```json
{"jsonrpc":"2.0","id":3,"method":"artifact.import","params":{"importer":"tar","path":"/archives/build.tar.gz","root":"build/from-gzip","options":{"compression":"gzip","max_compressed_bytes":104857600,"limits":{"max_archive_bytes":1073741824}}}}
```

```json
{"jsonrpc":"2.0","id":3,"method":"artifact.import","params":{"importer":"tar","path":"/archives/build.tar","root":"build/from-tar","options":{"limits":{"max_entries":10000}}}}
{"jsonrpc":"2.0","id":3,"result":{"object":"<object-key>","archive_bytes":2048,"entries":1,"files":1,"directories":0,"symlinks":0,"hardlinks":0,"file_bytes":14,"sparse_expansion_bytes":0}}
```

`options.limits` accepts any subset of these nonnegative integer fields:

| Field | Default | Meaning |
| --- | ---: | --- |
| `max_archive_bytes` | 1 TiB (1099511627776) | Complete raw tar bytes |
| `max_in_flight_files` | 16 | Concurrent file staging operations, positive |
| `max_entries` | 1,000,000 | Logical entries, excluding extension records |
| `max_path_bytes` | 4096 | Bytes in one archive pathname |
| `max_file_bytes` | 256 GiB (274877906944) | Logical bytes in one regular file |
| `max_total_file_bytes` | 1 TiB (1099511627776) | Total logical regular-file bytes |
| `max_sparse_expansion_bytes` | 256 GiB (274877906944) | Total materialized sparse holes |

The result includes the imported object and counts of consumed archive bytes,
accepted entries by type, logical file bytes, and materialized sparse holes.

### Casitar

Required fields: `path` (a Casitar archive) and a nonempty `destinations` array.
Supply one destination root name per archive root, in canonical archive-header
order. The importer verifies the archive and publishes all destinations in one
revision.

```json
{"jsonrpc":"2.0","id":4,"method":"artifact.import","params":{"importer":"casitar","path":"/archives/build.casitar","destinations":["build/restored"],"options":{"conflict_policy":"require_absent","limits":{"max_payload_bytes":1073741824}}}}
{"jsonrpc":"2.0","id":4,"result":{"mappings":[{"index":0,"root":"<object-key>","name":"build/restored"}],"destination_revision":"<revision>","records_inserted":3,"records_reused":0,"payloads_written":1,"payloads_reused":0}}
```

`options.conflict_policy` is one of:

- `require_absent` (default): every destination must be absent.
- `replace_if_unchanged`: replace each destination only if its value remains
  unchanged between import preflight and final publication.

`options.limits` accepts any subset of these nonnegative integer fields:

| Field | Default | Meaning |
| --- | ---: | --- |
| `max_header_bytes` | 4 MiB (4194304) | Encoded header body bytes |
| `max_record_bytes` | 256 MiB (268435456) | Bytes in one encoded logical record |
| `max_payload_bytes` | u64 maximum (18446744073709551615) | Plaintext bytes in one payload |
| `max_total_payload_bytes` | u64 maximum | Total plaintext payload bytes |
| `max_archive_bytes` | u64 maximum | Complete archive bytes |
| `max_payloads` | 1,000,000 | Distinct payload frames |
| `max_records` | 1,000,000 | Distinct logical record frames |
| `read_buffer_bytes` | 64 KiB (65536) | Copy/hash scratch buffer; must be positive |

Frozen format ceilings still apply. Omit unchanged limits, particularly the
64-bit maxima, if the client's JSON number implementation cannot represent
them exactly. The result reports root mappings, publication revision, and
counts of inserted/reused records and written/reused payloads.

### Git

Available with the `git` build feature. Required fields:
`path` (local working tree or bare repository) and `view` (view name below
`git/`). This imports native Git objects and publishes `git/<view>`.

```json
{"jsonrpc":"2.0","id":5,"method":"artifact.import","params":{"importer":"git","path":"/src/project","view":"upstream","options":{"refs":["refs/heads/main"],"max_cached_pack_bytes":0}}}
{"jsonrpc":"2.0","id":5,"result":{"view":"<object-key>","objects":42,"revision":"<revision>"}}
```

| Option | Default | Effect |
| --- | --- | --- |
| `refs` | Local branches and tags | Exact canonical refs to include, such as `refs/heads/main`. An empty list uses the default. |
| `revisions` | `[]` | Full hexadecimal object IDs to pin under `refs/casita/pins/<lowercase-oid>`. Abbreviations and revision expressions are invalid. |
| `max_cached_pack_bytes` | 8 GiB (8589934592) | Maximum exact source pack retained for full clones; `0` disables it. |
| `concurrency` | 16 | Positive number of concurrently staged objects. |
| `max_buffered_bytes` | 64 MiB (67108864) | Positive budget for decoded bytes held by staging futures. Larger objects run alone. |

Pinned revisions appear in restored Git repositories. The result reports the
view key, distinct native objects visited, and publication revision.

### Blob

Required fields: `path` (file) and `root`. Options are empty. The importer
streams and publishes the bytes. Result:
`{"object":"<object-key>"}`. Restore with `importer: "blob"` to an absent file.

### Repository copy

`importer: "copy"` requires `path` (a local Casita repository),
`source_root`, and `root` (the destination name). Options are empty. It
copies the complete source graph before publishing the destination root and
returns `{"object":"<object-key>"}`.

### NAR and filesystem-NAR

`importer: "nar"` requires `path` (one canonical NAR archive) and `root`.
Options are empty. Malformed archives and bytes after the archive fail.
`importer: "filesystem_nar"` requires `path` (directory, regular file, or
symlink) and `root`. Its only option is `reread`, default `false`, matching the
library importer. Both use the library NAR validation and measurement paths.

```json
{"jsonrpc":"2.0","id":5,"method":"artifact.import","params":{"importer":"nar","path":"/archives/result.nar","root":"nix/result"}}
{"jsonrpc":"2.0","id":5,"result":{"object":"<envelope-object-key>","nar_size":120}}
```

The stored object is a directory envelope with one entry named `root`.
This retains directories, files, and symlinks alike. `artifact.restore` with
`nar` or `filesystem_nar` unwraps the original node; ordinary checkout
exposes the envelope.

## Restore

`artifact.restore` takes `root`, destination `path`, and an optional
`importer` (default `filesystem`). It reads retained content without the
original source.

| Importer | Restored result |
| --- | --- |
| `filesystem`, `tar`, `casitar`, `copy` | Directory tree |
| `blob` | File |
| `nar`, `filesystem_nar` | Original directory, file, or symlink from the NAR envelope |
| `git` | Bare Git repository with selected refs and object format; requires `git` |

Select the result's content kind even when the root came from `copy` or
`casitar`. For Git, use root `git/<view>`.

```json
{"jsonrpc":"2.0","id":6,"method":"artifact.restore","params":{"importer":"git","root":"git/upstream","path":"/restore/upstream.git"}}
{"jsonrpc":"2.0","id":6,"result":{"present":true,"object":"<object-key>"}}
```

A missing root returns `{"present":false}` without creating a destination.
The parent directory must exist. Directory results accept an absent path or
an empty real directory. File and symlink results require an absent path.
Existing nonempty destinations are rejected. Restoration stages into a sibling
on the same filesystem and publishes only the completed result; failures clean
up staged output and leave existing destination contents untouched.

Restored results are not flushed to stable storage: a power loss or
operating-system crash soon after the response can leave the destination
absent or incomplete, beside a leftover `.casita-restore-*` staging directory.
The repository still holds the content, so remove both and restore again.

## Checkout

`artifact.checkout` takes a directory `root` and destination `path`. It
returns `present: true` after materialization. For a missing root, it creates
an empty destination directory and returns `present: false`.

```json
{"jsonrpc":"2.0","id":6,"method":"artifact.checkout","params":{"root":"build/latest","path":"/build/restored"}}
{"jsonrpc":"2.0","id":6,"result":{"present":true}}
```

The parent must exist. For a present root, the destination may be absent or
an empty real directory. For a missing root, it must be absent. As with
[CLI checkout](../../reference/cli/#checkout), the materialized tree is not
flushed to stable storage.

## Shutdown and cancellation

`rpc.shutdown` closes only this connection after returning `null`. Omit
`params`; the daemon continues serving other clients.

```json
{"jsonrpc":"2.0","id":7,"method":"rpc.shutdown"}
{"jsonrpc":"2.0","id":7,"result":null}
```

The `rpc.cancel` notification is accepted but currently does nothing. It does
not interrupt an import or checkout.

## Errors

```json
{"jsonrpc":"2.0","id":8,"error":{"code":-32602,"message":"unsupported compression","data":{"category":"unsupported_option","importer":"tar","option":"compression"}}}
```

| Code | Meaning |
| --- | --- |
| `-32700` | Invalid JSON |
| `-32600` | Invalid JSON-RPC request, missing initialization, or unsupported/duplicate initialization |
| `-32601` | Unknown method |
| `-32602` | Invalid parameters, including unknown importer/options or incorrect field types |
| `-32008` | Import, checkout, filesystem, or repository operation failed |
| `-32000` | The connection limit was reached; sent with a `null` `id` before the connection closes |

Import and restore errors include `data.category`, `data.importer` and
`data.option`. Importer is the requested name, or `null` if it could not be
parsed. Option is the offending option name (for example `limits.max_entry`),
or `null` when no specific option applies.

| Category | Code | Meaning |
| --- | --- | --- |
| `unsupported_importer` | `-32602` | Unknown importer or unavailable build feature |
| `unsupported_option` | `-32602` | Unknown import option or unsupported option value, such as compression |
| `invalid_parameters` | `-32602` | Missing fields, invalid names, wrong types or invalid parameter combinations |
| `execution_failure` | `-32008` | Source I/O, integrity, limits, destination conflict or repository failure |

Unsupported import requests are validated before source access or repository
mutation, so callers can safely select another implementation. There is no
required discovery handshake beyond normal `rpc.initialize`. Existing methods
retain their numeric codes. Treat messages as diagnostic text, not stable
machine-readable codes. Invalid framing (including oversized,
empty, CR-containing, or unterminated lines) and an expired receive or
response deadline close the connection without a JSON-RPC response.

The service does not expose listing/deleting roots, individual blob access,
binary streaming, or garbage collection. Clients must verify that paths and
root names are appropriate for their own trust boundary.

The historical [Cargo prototype](../cargo/) used this generic protocol. Cargo
chose its root names and artifact lifecycle; the IPC service
does not contain package resolution, build, registry, or checksum semantics.
