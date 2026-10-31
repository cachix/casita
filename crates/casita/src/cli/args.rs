//! CLI argument definitions and conversion to repository options.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

/// A generic content-addressed repository.
#[derive(Parser)]
#[command(name = "casita", version, about)]
pub(super) struct Cli {
    /// Runtime trace filter. Defaults to RUST_LOG, then `off`.
    /// Example: `casita=debug,object_store=info`.
    #[arg(long, global = true, value_name = "DIRECTIVES")]
    pub(super) log_filter: Option<String>,
    /// Runtime trace encoding written to stderr.
    #[arg(long, global = true, value_enum, default_value_t = LogFormat::Compact)]
    pub(super) log_format: LogFormat,
    /// Directory holding the repository's payloads and state database.
    /// Defaults to the OS data directory (e.g. ~/.local/share/casita on Linux).
    #[arg(long)]
    pub(super) repository: Option<PathBuf>,
    /// Entries each traversal structure keeps in memory before spilling.
    /// Intended for constrained deployments and reproducible spill benchmarks.
    #[arg(long, value_name = "OBJECTS")]
    pub(super) spill_memory_objects: Option<usize>,
    /// Total temporary bytes one command may use for spilled traversal state.
    /// Intended for constrained deployments and reproducible spill benchmarks.
    #[arg(long, value_name = "BYTES")]
    pub(super) spill_bytes: Option<u64>,
    /// Approximate compressed bytes per immutable chunk pack.
    /// Intended for deployment tuning and reproducible storage benchmarks.
    #[arg(long, value_name = "BYTES")]
    pub(super) pack_target_bytes: Option<u64>,
    /// Bytes retained by the compressed-chunk cache for S3 endpoints.
    /// Zero disables payload caching.
    #[arg(long, value_name = "BYTES")]
    pub(super) pack_cache_bytes: Option<u64>,

    #[command(subcommand)]
    pub(super) command: Command,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
pub(super) enum LogFormat {
    #[default]
    Compact,
    Json,
}

#[derive(Subcommand)]
pub(super) enum Command {
    /// Attach the current directory to the global repository, or initialize
    /// the explicit local repository selected by `--repository`.
    Init,
    /// Import an external input with one selected importer.
    Import(ImportArgs),
    /// Find and run an executable in a named directory root.
    Run(RunArgs),
    /// Create, inspect, verify, or import portable Casitar archives.
    Archive {
        #[command(subcommand)]
        command: ArchiveCommand,
    },
    /// Inspect generic immutable object records.
    Object {
        #[command(subcommand)]
        command: ObjectCommand,
    },
    /// Inspect canonical filesystem trees.
    Tree {
        #[command(subcommand)]
        command: TreeCommand,
    },
    /// Synchronize objects or named-root closures between repositories.
    Sync(SyncArgs),
    /// Inspect online pins and collectors without waiting for admission.
    Holds {
        /// Local repository directory or s3://BUCKET[/PREFIX].
        endpoint: String,
        /// Emit collector tokens and state/coordination pin-ledger inventories as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Import, inspect, and check out native Git objects and views.
    Git {
        #[command(subcommand)]
        command: NativeGitCommand,
    },
    /// Serve local JSON-RPC clients over the repository's local endpoint.
    Ipc(IpcArgs),
    /// Materialize a directory object into an empty target directory and name
    /// it under auto/checkout/<target> until that root is explicitly removed.
    Checkout {
        /// Generic directory key, or its short `blake3-...` digest.
        key: String,
        /// Target directory (created if missing; must be empty).
        dir: PathBuf,
        /// Skip registering the named checkout root.
        #[arg(long)]
        no_root: bool,
    },
    /// Write a blob object's payload to stdout.
    Cat {
        /// Generic blob key, or its short `blake3-...` digest.
        key: String,
        /// Authenticate each block before writing it to stdout.
        #[arg(long)]
        verified: bool,
        /// Read verified raw blob contents from a local, SSH, or S3 source.
        #[arg(long, requires = "verified")]
        from: Option<String>,
    },
    /// Manage named roots retained across collection.
    Root {
        #[command(subcommand)]
        command: RootCommand,
    },
    /// Collect everything not reachable from named roots.
    Gc {
        /// Mark and count candidates under exclusive ownership without
        /// changing repository state.
        #[arg(long)]
        dry_run: bool,
    },
    /// Force physical reclamation of garbage deferred inside sparse packs.
    Vacuum,
    /// Verify repository integrity and safely repair physical state when
    /// possible.
    Fsck(FsckArgs),
    /// Internal authenticated SSH transfer-source process.
    #[cfg(feature = "ssh")]
    #[command(name = "__ssh-source", hide = true)]
    SshSource(SshSourceArgs),
}

#[derive(Args)]
pub(super) struct RunArgs {
    /// Literal repository root, SCOPE:NAME, or a name in the default scope.
    pub(super) root: String,
    /// Select an executable by filename or relative path when there are several.
    #[arg(long, value_name = "NAME_OR_PATH")]
    pub(super) bin: Option<PathBuf>,
    /// Arguments passed unchanged to the application after `--`.
    #[arg(last = true)]
    pub(super) args: Vec<std::ffi::OsString>,
}

#[derive(Args)]
pub(super) struct FsckArgs {
    /// Audit logical state, payload identity, graph closure, and physical
    /// coverage without running the separate physical-repair pass.
    #[arg(long, conflicts_with_all = ["dry_run", "source"])]
    pub(super) audit_only: bool,
    /// Report safe repairs without changing physical storage.
    #[arg(long)]
    pub(super) dry_run: bool,
    /// Local Casita repository containing an independently verified replica.
    #[arg(long, value_name = "REPOSITORY")]
    pub(super) source: Option<PathBuf>,
}

pub(super) const CLI_MAX_CASITAR_ARCHIVE_BYTES: u64 = 1 << 40;
pub(super) const CLI_MAX_CASITAR_PAYLOAD_BYTES: u64 = 1 << 38;
pub(super) const CLI_MAX_CASITAR_TOTAL_PAYLOAD_BYTES: u64 = 1 << 40;

#[derive(Args, Clone, Copy)]
pub(super) struct ArchiveLimitsArgs {
    /// Maximum complete archive bytes accepted or produced.
    #[arg(long, value_name = "BYTES", default_value_t = CLI_MAX_CASITAR_ARCHIVE_BYTES)]
    pub(super) max_archive_bytes: u64,
    /// Maximum bytes in any one plaintext payload.
    #[arg(long, value_name = "BYTES", default_value_t = CLI_MAX_CASITAR_PAYLOAD_BYTES)]
    pub(super) max_payload_bytes: u64,
    /// Maximum sum of plaintext payload bytes.
    #[arg(
        long,
        value_name = "BYTES",
        default_value_t = CLI_MAX_CASITAR_TOTAL_PAYLOAD_BYTES
    )]
    pub(super) max_total_payload_bytes: u64,
    /// Maximum distinct payload frames.
    #[arg(
        long,
        value_name = "COUNT",
        default_value_t = casita::experimental::DEFAULT_MAX_CASITAR_STREAM_ITEMS
    )]
    pub(super) max_payloads: usize,
    /// Maximum distinct logical record frames.
    #[arg(
        long,
        value_name = "COUNT",
        default_value_t = casita::experimental::DEFAULT_MAX_CASITAR_STREAM_ITEMS
    )]
    pub(super) max_records: usize,
}

impl ArchiveLimitsArgs {
    pub(super) fn stream_limits(self) -> casita::experimental::CasitarStreamLimits {
        casita::experimental::CasitarStreamLimits {
            max_archive_bytes: self.max_archive_bytes,
            max_payload_bytes: self.max_payload_bytes,
            max_total_payload_bytes: self.max_total_payload_bytes,
            max_payloads: self.max_payloads,
            max_records: self.max_records,
            ..casita::experimental::CasitarStreamLimits::default()
        }
    }
}

#[derive(Subcommand)]
pub(super) enum ArchiveCommand {
    /// Create a deterministic archive from complete named-root or object closures.
    Create(ArchiveCreateArgs),
    /// Inspect framing, roots, counts, and digest without repository verification.
    Inspect(ArchiveInspectArgs),
    /// Fully verify an archive in an isolated temporary repository.
    Verify(ArchiveVerifyArgs),
    /// Fully verify an archive and atomically publish destination roots.
    Import(ArchiveImportArgs),
}

#[derive(Args)]
pub(super) struct ArchiveCreateArgs {
    /// Local named root to resolve in one source snapshot (repeatable).
    #[arg(long = "root", required_unless_present = "objects")]
    pub(super) roots: Vec<String>,
    /// Exact generic object key whose closure to include (repeatable).
    #[arg(long = "object", required_unless_present = "roots")]
    pub(super) objects: Vec<String>,
    /// Destination archive path, or `-` for stdout.
    #[arg(long)]
    pub(super) output: String,
    /// Replace an existing output file.
    #[arg(long)]
    pub(super) force: bool,
    /// Emit a stable JSON report (not available when archive bytes use stdout).
    #[arg(long)]
    pub(super) json: bool,
    #[command(flatten)]
    pub(super) limits: ArchiveLimitsArgs,
}

#[derive(Args)]
pub(super) struct ArchiveInspectArgs {
    /// Archive path, or `-` for stdin.
    pub(super) input: String,
    /// Emit a stable JSON report.
    #[arg(long)]
    pub(super) json: bool,
    #[command(flatten)]
    pub(super) limits: ArchiveLimitsArgs,
}

#[derive(Args)]
pub(super) struct ArchiveVerifyArgs {
    /// Archive path, or `-` for stdin.
    pub(super) input: String,
    /// Emit a stable JSON report.
    #[arg(long)]
    pub(super) json: bool,
    #[command(flatten)]
    pub(super) limits: ArchiveLimitsArgs,
}

#[derive(Args)]
pub(super) struct ArchiveImportArgs {
    /// Archive path, or `-` for stdin.
    pub(super) input: String,
    /// Exact destination root name in canonical header order (repeatable).
    #[arg(
        long = "root",
        required_unless_present = "root_prefix",
        conflicts_with = "root_prefix"
    )]
    pub(super) roots: Vec<String>,
    /// Map canonical roots to PREFIX/0, PREFIX/1, and so on.
    #[arg(long, required_unless_present = "roots", conflicts_with = "roots")]
    pub(super) root_prefix: Option<String>,
    /// Replace mapped roots only if their preflight values remain unchanged.
    #[arg(long)]
    pub(super) replace: bool,
    /// Emit a stable JSON report.
    #[arg(long)]
    pub(super) json: bool,
    #[command(flatten)]
    pub(super) limits: ArchiveLimitsArgs,
}

/// Selection for one generic repository synchronization.
#[derive(Args)]
pub(super) struct SyncArgs {
    /// Source local path, `s3://BUCKET[/PREFIX]`, or
    /// ssh://[user@]host[:port]/absolute/path endpoint.
    #[arg(long)]
    pub(super) from: String,
    /// Read payloads from this Casita repository instead of --from. Accepts
    /// the same endpoint types; roots and records still come from --from.
    #[arg(long, value_name = "ENDPOINT")]
    pub(super) from_blobs: Option<String>,
    /// Destination local path or `s3://BUCKET[/PREFIX]`.
    #[arg(long)]
    pub(super) to: String,
    /// Diagnostic WAL writer name for an S3 endpoint. Defaults to
    /// `CASITA_WRITER`, then a process ID with a random instance suffix.
    #[arg(long, value_name = "NAME")]
    pub(super) writer: Option<String>,
    /// Exact generic object key to transfer (repeatable).
    #[arg(long = "object")]
    pub(super) objects: Vec<String>,
    /// Source root name to transfer and atomically install under the same name (repeatable).
    #[arg(long = "root")]
    pub(super) roots: Vec<String>,
    /// Resolve this filesystem path beneath exactly one source root and copy
    /// only the selected file or directory closure.
    #[arg(long)]
    pub(super) path: Option<String>,
    /// Explicit destination root for a path-selected closure. Without this,
    /// the selected closure remains unrooted and collectible.
    #[arg(long, requires = "path")]
    pub(super) destination_root: Option<String>,
    /// Copy only selected object records, without following their links.
    #[arg(long)]
    pub(super) shallow: bool,
    /// Reuse verified destination closures without auditing their descendants
    /// at the source. Incomplete destination closures are still fetched.
    #[arg(long)]
    pub(super) incremental: bool,
}

/// Arguments encoded by [`casita::experimental::SshTransferSource`] for the remote process.
#[cfg(feature = "ssh")]
#[derive(Args)]
pub(super) struct SshSourceArgs {
    /// URL-safe base64 encoding of the remote UTF-8 repository path.
    #[arg(long)]
    pub(super) repository_base64: String,
}

/// Bounds for the local IPC listener. Exceeding one closes the connection
/// with a plain EOF; clients reconnect and initialize again.
#[derive(Args)]
pub(super) struct IpcArgs {
    /// Maximum simultaneous connections, counting idle clients.
    #[arg(long, default_value_t = super::ipc::IpcOptions::default().max_connections)]
    pub(super) max_connections: usize,
    /// Seconds to receive each complete request frame, including idle time
    /// since the previous response. Operations themselves are not timed out.
    #[arg(long, default_value_t = super::ipc::IpcOptions::default().frame_timeout.as_secs())]
    pub(super) frame_timeout_secs: u64,
    /// Seconds to write and flush each response.
    #[arg(long, default_value_t = super::ipc::IpcOptions::default().response_timeout.as_secs())]
    pub(super) response_timeout_secs: u64,
}

impl IpcArgs {
    pub(super) fn options(&self) -> super::ipc::IpcOptions {
        super::ipc::IpcOptions {
            max_connections: self.max_connections,
            frame_timeout: std::time::Duration::from_secs(self.frame_timeout_secs),
            response_timeout: std::time::Duration::from_secs(self.response_timeout_secs),
        }
    }
}

#[derive(Subcommand)]
pub(super) enum NativeGitCommand {
    /// Show one selected immutable Git view and its refs.
    Show {
        /// View-root segment below git/.
        view: String,
    },
    /// Safely check out one exact native Git tree object.
    Checkout {
        /// Exact type-qualified Git tree object key.
        tree: String,
        /// Empty destination directory.
        dir: PathBuf,
        /// Materialize gitlinks as empty directories instead of rejecting them.
        #[arg(long)]
        skip_gitlinks: bool,
    },
    /// Serve one exact immutable view over read-only Git smart HTTP.
    Serve {
        /// View-root segment below git/.
        view: String,
        /// TCP listen address.
        #[arg(long, default_value = "127.0.0.1:9418")]
        listen: String,
        /// Largest generated pack response in bytes.
        #[arg(long, default_value_t = 512 * 1024 * 1024)]
        max_pack_bytes: usize,
        /// Zlib compression level for generated pack entries.
        #[arg(long, default_value_t = 6, value_parser = clap::value_parser!(u32).range(0..=9))]
        pack_compression_level: u32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(super) enum ImporterKind {
    Filesystem,
    Tar,
    #[cfg(feature = "oci")]
    Oci,
    Git,
    Casitar,
}

/// Shared import selection and importer-specific options.
#[derive(Args)]
pub(super) struct ImportArgs {
    /// Input path. Tar accepts `-` for standard input.
    pub(super) path: PathBuf,
    /// Importer to run. When omitted, Casita detects the input type.
    #[arg(short = 'i', long, value_enum)]
    pub(super) importer: Option<ImporterKind>,
    /// Root name for filesystem, tar, or OCI imports. For OCI, names the image
    /// layout. Filesystem imports default to the canonicalized source path
    /// below `auto/`.
    #[arg(long = "root")]
    pub(super) name: Option<String>,
    /// Retention policy for the imported root. Existing policy is kept when omitted.
    #[arg(long, value_enum)]
    pub(super) retention: Option<RootRetentionArg>,
    /// Read and hash every file, ignoring what earlier imports recorded.
    ///
    /// An import normally recognizes a file whose device, inode, size and
    /// timestamps are exactly what they were when its content was last read,
    /// and does not read it again. Use this where that is not enough.
    #[arg(long = "filesystem-rehash")]
    pub(super) rehash: bool,
    /// Maximum files ingested at once (filesystem importer only; default 16).
    #[arg(long = "filesystem-concurrency", value_name = "FILES")]
    pub(super) file_concurrency: Option<std::num::NonZeroUsize>,
    /// Maximum chunk uploads per blob writer; shared memory may limit this further.
    #[arg(long, value_name = "CHUNKS", default_value = "32")]
    pub(super) chunk_upload_concurrency: std::num::NonZeroUsize,
    #[command(flatten)]
    pub(super) tar: TarImportArgs,
    #[cfg(feature = "oci")]
    #[command(flatten)]
    pub(super) oci: OciImportArgs,
    #[command(flatten)]
    pub(super) git: GitImportArgs,
    #[command(flatten)]
    pub(super) casitar: CasitarImportArgs,
}

#[cfg(feature = "oci")]
#[derive(Args)]
pub(super) struct OciImportArgs {
    /// Also publish the merged container root filesystem under this name.
    /// Must differ from --root, which names the OCI image layout.
    #[arg(long = "oci-rootfs-root", value_name = "NAME")]
    pub(super) rootfs_root: Option<String>,
    /// Maximum decoded tar bytes across all layers of a merged filesystem.
    #[arg(long = "oci-rootfs-max-bytes", default_value_t = casita::OciRootfsLimits::default().max_total_archive_bytes)]
    pub(super) rootfs_max_bytes: u64,
    /// Maximum layer entries and merged filesystem nodes.
    #[arg(long = "oci-rootfs-max-entries", default_value_t = casita::OciRootfsLimits::default().max_entries)]
    pub(super) rootfs_max_entries: usize,
    /// Select a platform from an image index, for example linux/amd64.
    #[arg(long = "oci-platform", value_name = "OS/ARCH[/VARIANT]")]
    pub(super) platform: Option<String>,
    /// Use plain HTTP for a registry. Intended for explicitly trusted local registries.
    #[arg(long = "oci-http")]
    pub(super) http: bool,
    /// Maximum compressed bytes in one config or layer blob.
    #[arg(long = "oci-max-blob-bytes", default_value_t = casita::OciImportLimits::default().max_blob_bytes)]
    pub(super) max_blob_bytes: u64,
    /// Maximum aggregate compressed config and layer bytes.
    #[arg(long = "oci-max-total-blob-bytes", default_value_t = casita::OciImportLimits::default().max_total_blob_bytes)]
    pub(super) max_total_blob_bytes: u64,
}

#[cfg(feature = "oci")]
impl Default for OciImportArgs {
    fn default() -> Self {
        let limits = casita::OciImportLimits::default();
        Self {
            rootfs_root: None,
            rootfs_max_bytes: casita::OciRootfsLimits::default().max_total_archive_bytes,
            rootfs_max_entries: casita::OciRootfsLimits::default().max_entries,
            platform: None,
            http: false,
            max_blob_bytes: limits.max_blob_bytes,
            max_total_blob_bytes: limits.max_total_blob_bytes,
        }
    }
}

#[derive(Args)]
pub(super) struct GitImportArgs {
    /// View-root segment installed below `git/`.
    #[arg(long = "git-view")]
    pub(super) view: Option<String>,
    /// Exact canonical ref to include (repeatable); empty selects branches and tags.
    #[arg(long = "git-ref")]
    pub(super) refs: Vec<String>,
    /// Largest exact source-native pack retained as a full-clone cache; zero disables it.
    #[arg(
        long = "git-max-cached-pack-bytes",
        default_value_t = casita::experimental::DEFAULT_MAX_CACHED_GIT_PACK_BYTES
    )]
    pub(super) max_cached_pack_bytes: u64,
    /// Maximum concurrently staged Git objects; one selects serial staging.
    #[arg(long = "git-concurrency", default_value_t = casita::experimental::DEFAULT_GIT_IMPORT_CONCURRENCY)]
    pub(super) concurrency: std::num::NonZeroUsize,
    /// Decoded source byte budget; an oversized object runs alone (not an RSS limit).
    #[arg(long = "git-max-buffered-bytes", default_value_t = casita::experimental::DEFAULT_GIT_IMPORT_BUFFERED_BYTES)]
    pub(super) max_buffered_bytes: std::num::NonZeroU64,
}

#[derive(Args)]
pub(super) struct CasitarImportArgs {
    /// Exact destination root name in canonical archive-header order (repeatable).
    #[arg(
        id = "casitar_roots",
        long = "casitar-root",
        conflicts_with = "casitar_root_prefix"
    )]
    pub(super) roots: Vec<String>,
    /// Map canonical archive roots to PREFIX/0, PREFIX/1, and so on.
    #[arg(
        id = "casitar_root_prefix",
        long = "casitar-root-prefix",
        conflicts_with = "casitar_roots"
    )]
    pub(super) root_prefix: Option<String>,
    /// Replace mapped roots only if their preflight values remain unchanged.
    #[arg(id = "casitar_replace", long = "casitar-replace")]
    pub(super) replace: bool,
    /// Maximum complete Casitar archive bytes.
    #[arg(id = "casitar_max_archive_bytes", long = "casitar-max-archive-bytes", default_value_t = CLI_MAX_CASITAR_ARCHIVE_BYTES)]
    pub(super) max_archive_bytes: u64,
    /// Maximum bytes in one plaintext payload.
    #[arg(id = "casitar_max_payload_bytes", long = "casitar-max-payload-bytes", default_value_t = CLI_MAX_CASITAR_PAYLOAD_BYTES)]
    pub(super) max_payload_bytes: u64,
    /// Maximum sum of plaintext payload bytes.
    #[arg(id = "casitar_max_total_payload_bytes", long = "casitar-max-total-payload-bytes", default_value_t = CLI_MAX_CASITAR_TOTAL_PAYLOAD_BYTES)]
    pub(super) max_total_payload_bytes: u64,
    /// Maximum distinct payload frames.
    #[arg(id = "casitar_max_payloads", long = "casitar-max-payloads", default_value_t = casita::experimental::DEFAULT_MAX_CASITAR_STREAM_ITEMS)]
    pub(super) max_payloads: usize,
    /// Maximum distinct logical record frames.
    #[arg(id = "casitar_max_records", long = "casitar-max-records", default_value_t = casita::experimental::DEFAULT_MAX_CASITAR_STREAM_ITEMS)]
    pub(super) max_records: usize,
}

impl CasitarImportArgs {
    pub(super) fn stream_limits(&self) -> casita::experimental::CasitarStreamLimits {
        casita::experimental::CasitarStreamLimits {
            max_archive_bytes: self.max_archive_bytes,
            max_payload_bytes: self.max_payload_bytes,
            max_total_payload_bytes: self.max_total_payload_bytes,
            max_payloads: self.max_payloads,
            max_records: self.max_records,
            ..casita::experimental::CasitarStreamLimits::default()
        }
    }
}

pub(super) const CLI_MAX_TAR_ARCHIVE_BYTES: u64 = 1 << 40;
pub(super) const CLI_MAX_TAR_ENTRIES: usize = 1_000_000;
pub(super) const CLI_MAX_TAR_PATH_BYTES: usize = 4096;
pub(super) const CLI_MAX_TAR_FILE_BYTES: u64 = 1 << 38;
pub(super) const CLI_MAX_TAR_TOTAL_FILE_BYTES: u64 = 1 << 40;
pub(super) const CLI_MAX_TAR_SPARSE_EXPANSION_BYTES: u64 = 1 << 38;

#[derive(Args)]
pub(super) struct TarImportArgs {
    /// Maximum files being copied, queued, finalized, or verified at once.
    #[arg(
        long = "tar-max-in-flight-files",
        value_name = "COUNT",
        default_value_t = casita::experimental::TarImportLimits::default().max_in_flight_files
    )]
    pub(super) max_in_flight_files: usize,
    /// Maximum raw tar bytes, including headers and padding.
    #[arg(long = "tar-max-archive-bytes", value_name = "BYTES", default_value_t = CLI_MAX_TAR_ARCHIVE_BYTES)]
    pub(super) max_archive_bytes: u64,
    /// Maximum logical archive entries.
    #[arg(long = "tar-max-entries", value_name = "COUNT", default_value_t = CLI_MAX_TAR_ENTRIES)]
    pub(super) max_entries: usize,
    /// Maximum byte length of one archive pathname.
    #[arg(long = "tar-max-path-bytes", value_name = "BYTES", default_value_t = CLI_MAX_TAR_PATH_BYTES)]
    pub(super) max_path_bytes: usize,
    /// Maximum bytes in one regular file.
    #[arg(long = "tar-max-file-bytes", value_name = "BYTES", default_value_t = CLI_MAX_TAR_FILE_BYTES)]
    pub(super) max_file_bytes: u64,
    /// Maximum aggregate logical regular-file bytes.
    #[arg(long = "tar-max-total-file-bytes", value_name = "BYTES", default_value_t = CLI_MAX_TAR_TOTAL_FILE_BYTES)]
    pub(super) max_total_file_bytes: u64,
    /// Reserved for compatibility. Sparse tar entries are rejected.
    #[arg(
        long = "tar-max-sparse-expansion-bytes",
        value_name = "BYTES",
        default_value_t = CLI_MAX_TAR_SPARSE_EXPANSION_BYTES
    )]
    pub(super) max_sparse_expansion_bytes: u64,
}

impl TarImportArgs {
    pub(super) fn limits(&self) -> casita::experimental::TarImportLimits {
        casita::experimental::TarImportLimits {
            max_in_flight_files: self.max_in_flight_files,
            max_archive_bytes: self.max_archive_bytes,
            max_entries: self.max_entries,
            max_path_bytes: self.max_path_bytes,
            max_file_bytes: self.max_file_bytes,
            max_total_file_bytes: self.max_total_file_bytes,
            max_sparse_expansion_bytes: self.max_sparse_expansion_bytes,
        }
    }
}

#[derive(Subcommand)]
pub(super) enum ObjectCommand {
    /// Show one immutable record and its closure status.
    Show {
        /// Generic object key (`namespace:base64url-native-id`).
        key: String,
    },
}

#[derive(Subcommand)]
pub(super) enum TreeCommand {
    /// List the direct entries of one canonical directory object.
    List {
        /// Generic directory key, or its short `blake3-...` digest.
        key: String,
    },
    /// Recursively compare two canonical directory objects without checkout.
    Diff {
        /// Old directory key, or its short `blake3-...` digest.
        old: String,
        /// New directory key, or its short `blake3-...` digest.
        new: String,
        /// Emit one JSON object per changed entry (NDJSON).
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
pub(super) enum RootCommand {
    /// Set NAME to an existing object, atomically replacing its prior target.
    Set {
        /// Validated root name; setting it again atomically repoints it.
        name: String,
        /// Generic object key, or a short filesystem blob/directory digest.
        target: String,
        /// Retention policy for this root. Existing policy is kept when omitted.
        #[arg(long, value_enum)]
        retention: Option<RootRetentionArg>,
    },
    /// Change an existing root's retention policy.
    Retention {
        name: String,
        #[arg(value_enum)]
        policy: RootRetentionArg,
    },
    /// Unregister a root name; its data becomes collectable by `gc`.
    Rm {
        /// The registered root name.
        #[arg(required_unless_present = "prefix", conflicts_with = "prefix")]
        name: Option<String>,
        /// Release every root at or under this prefix instead, printing each
        /// released name (e.g. `--prefix auto/` releases every automatic
        /// filesystem-import root).
        #[arg(long)]
        prefix: Option<String>,
    },
    /// List the registered roots.
    Ls {
        /// Only entries at or under this prefix (e.g. `auto/`).
        #[arg(default_value = "")]
        prefix: String,
        /// Include the retention policy.
        #[arg(long)]
        long: bool,
    },
}

#[derive(Clone, Copy, ValueEnum)]
pub(super) enum RootRetentionArg {
    Permanent,
    Evictable,
}

impl From<RootRetentionArg> for casita::RootRetention {
    fn from(value: RootRetentionArg) -> Self {
        match value {
            RootRetentionArg::Permanent => Self::Permanent,
            RootRetentionArg::Evictable => Self::Evictable,
        }
    }
}
