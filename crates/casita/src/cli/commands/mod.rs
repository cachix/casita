//! CLI command execution over the generic repository API.

use std::collections::BTreeMap;
use std::io::Read as _;
use std::path::{Path, PathBuf};

use casita::experimental::{
    BlobId, ClosureStatus, Digest, DirectoryId, Node, ObjectKey, RootChange, RootName,
};
use casita::{experimental::MetadataStore as _, import::Importer as _};
use futures::{StreamExt, TryStreamExt};
use tokio::io::{AsyncRead, AsyncWriteExt};

#[cfg(test)]
use super::LogFormat;
#[cfg(feature = "oci")]
use super::OciImportArgs;
#[cfg(feature = "ssh")]
use super::SshSourceArgs;
use super::{
    ArchiveCommand, ArchiveCreateArgs, ArchiveImportArgs, ArchiveInspectArgs, ArchiveVerifyArgs,
    CasitarImportArgs, Cli, Command, Error, FsckArgs, NativeGitCommand, ObjectCommand, RootCommand,
    SyncArgs, TarImportArgs, TreeCommand, usage_error,
};

mod naming;
mod run;
mod workspace;

use naming::{canonical_source, nested_name};
use workspace::Workspace;

/// The default repository location: the OS data directory joined with `casita`,
/// falling back to `~/.casita`, and then `.casita-data` in the current directory
/// if no home directory can be determined either.
fn default_repository_dir() -> PathBuf {
    dirs::data_dir()
        .map(|dir| dir.join("casita"))
        .or_else(|| dirs::home_dir().map(|home| home.join(".casita")))
        .unwrap_or_else(|| PathBuf::from(".casita-data"))
}

fn parse_directory_key(value: &str) -> Result<ObjectKey, Error> {
    if value.contains(':') {
        return Ok(value.parse()?);
    }
    let digest: DirectoryId = value.parse()?;
    Ok(ObjectKey::directory(digest))
}

fn parse_blob_or_exact_key(value: &str) -> Result<ObjectKey, Error> {
    if value.contains(':') {
        return Ok(value.parse()?);
    }
    let digest: BlobId = value.parse()?;
    Ok(ObjectKey::blob(digest))
}

fn require_complete(key: &ObjectKey, status: ClosureStatus) -> Result<(), Error> {
    if matches!(status, ClosureStatus::Complete { .. }) {
        Ok(())
    } else {
        Err(casita::experimental::RepositoryError::ObjectNotReadable {
            object: key.clone(),
            status,
        }
        .into())
    }
}

/// Keep every user-visible root inside the discovered workspace.  Repositories
/// selected explicitly remain raw repositories: they are useful for service
/// storage and for scripts that intentionally manage the global namespace.
fn scoped_root(workspace: Option<&Workspace>, name: impl AsRef<str>) -> Result<RootName, Error> {
    match workspace {
        Some(workspace) => workspace.root_name(name.as_ref()),
        None => Ok(RootName::try_from(name.as_ref())?),
    }
}

fn automatic_root(
    workspace: Option<&Workspace>,
    prefix: &str,
    source: &str,
) -> Result<RootName, Error> {
    scoped_root(workspace, nested_name(prefix, source))
}

/// A workspace marker is local control data, never project content.  Only an
/// import rooted at the workspace itself can contain it; importing a child or
/// a parent keeps its ordinary filesystem meaning.
fn imports_workspace_root(workspace: Option<&Workspace>, path: &Path) -> bool {
    let Some(workspace) = workspace else {
        return false;
    };
    std::fs::canonicalize(path).is_ok_and(|path| path == workspace.root())
}

async fn object_show<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    value: &str,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore,
    SS: casita::experimental::MetadataStore + 'static,
{
    let key: ObjectKey = value.parse()?;
    let hold = repository.retention_hold().await?;
    let record = hold
        .object(&key)
        .await?
        .ok_or_else(|| casita::experimental::RepositoryError::Absent(format!("object {key}")))?;
    println!("key {}", record.key());
    println!("payload {}", record.payload());
    println!("payload-size {}", record.payload_size());
    println!("links {}", record.links().len());
    for link in record.links() {
        println!("  {link}");
    }
    println!("closure {:?}", hold.verify_closure(&key).await?);
    Ok(())
}

async fn tree_list<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    value: &str,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore,
    SS: casita::experimental::MetadataStore + 'static,
{
    let key = parse_directory_key(value)?;
    if key.namespace().as_str() != casita::experimental::DIRECTORY_NAMESPACE {
        return Err(casita::experimental::RepositoryError::InvalidInput(format!(
            "tree listing requires `{}`, got `{}`",
            casita::experimental::DIRECTORY_NAMESPACE,
            key.namespace()
        ))
        .into());
    }
    let hold = repository.retention_hold().await?;
    require_complete(&key, hold.verify_closure(&key).await?)?;
    let (record, mut reader) = hold
        .open_payload(&key)
        .await?
        .ok_or_else(|| casita::experimental::RepositoryError::Absent(format!("object {key}")))?;
    let limits = repository.limits();
    let directory = casita::experimental::read_directory_payload(
        &key,
        &record,
        &mut *reader,
        limits.max_metadata_bytes.min(limits.max_payload_bytes),
    )
    .await?;
    for (name, node) in directory.nodes() {
        match node {
            Node::Directory { digest, size } => {
                println!("d {size:>12}  {name}  {}", ObjectKey::directory(*digest));
            }
            Node::File {
                digest,
                size,
                executable,
            } => {
                let kind = if *executable { "x" } else { "f" };
                println!("{kind} {size:>12}  {name}  {}", ObjectKey::blob(*digest));
            }
            Node::Symlink { target } => println!("l {:>12}  {name} -> {target}", "-"),
        }
    }
    Ok(())
}

async fn object_cat<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    value: &str,
    verified: bool,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore,
    SS: casita::experimental::MetadataStore + 'static,
{
    let key = parse_blob_or_exact_key(value)?;
    let hold = repository.retention_hold().await?;
    require_complete(&key, hold.verify_closure(&key).await?)?;
    if verified {
        let record = hold.object(&key).await?.ok_or_else(|| {
            casita::experimental::RepositoryError::Absent(format!("object {key}"))
        })?;
        let mut reader = repository
            .payloads()
            .open_verified(&record.payload(), record.payload_size())
            .await?
            .ok_or_else(|| {
                casita::experimental::RepositoryError::MissingPayload(record.payload())
            })?;
        let mut stdout = tokio::io::stdout();
        tokio::io::copy(&mut reader, &mut stdout).await?;
        stdout.flush().await?;
        return Ok(());
    }
    let (_, mut reader) = hold
        .open_payload(&key)
        .await?
        .ok_or_else(|| casita::experimental::RepositoryError::Absent(format!("object {key}")))?;
    let mut stdout = tokio::io::stdout();
    tokio::io::copy(&mut reader, &mut stdout).await?;
    stdout.flush().await?;
    Ok(())
}

async fn open_archive_input(input: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, Error> {
    if input == "-" {
        Ok(Box::new(tokio::io::stdin()))
    } else {
        Ok(Box::new(tokio::fs::File::open(Path::new(input)).await?))
    }
}

/// Choose a built-in importer without trusting a filename extension. Archive
/// inputs are reopened by their importer after this small, bounded probe.
pub(super) async fn detect_importer(path: &Path) -> Result<super::ImporterKind, Error> {
    if path == Path::new("-") {
        return Err(usage_error(
            "automatic importer detection cannot replay standard input; select an importer with -i",
        ));
    }

    let metadata = std::fs::metadata(path)?;
    if metadata.is_dir() {
        return Ok(if is_git_repository(path) {
            super::ImporterKind::Git
        } else {
            super::ImporterKind::Filesystem
        });
    }
    if !metadata.is_file() {
        return Err(usage_error(format!(
            "cannot detect an importer for {}; select one with -i",
            path.display()
        )));
    }

    let mut header = [0_u8; 1024];
    let mut input = std::fs::File::open(path)?;
    let bytes = input.read(&mut header)?;
    let header = &header[..bytes];
    if header.starts_with(casita::experimental::CASITAR_MAGIC) {
        Ok(super::ImporterKind::Casitar)
    } else if is_tar_header(header) {
        Ok(super::ImporterKind::Tar)
    } else {
        Err(usage_error(format!(
            "cannot detect an importer for {}; select one with -i",
            path.display()
        )))
    }
}

fn is_git_repository(path: &Path) -> bool {
    let dot_git = path.join(".git");
    dot_git.is_dir()
        || dot_git.is_file()
        || (path.join("HEAD").is_file() && path.join("objects").is_dir())
}

fn is_tar_header(bytes: &[u8]) -> bool {
    let Some(header) = bytes.get(..512) else {
        return false;
    };
    if header.iter().all(|byte| *byte == 0) {
        return bytes
            .get(512..1024)
            .is_some_and(|block| block.iter().all(|byte| *byte == 0));
    }
    let Some(expected) = parse_tar_checksum(&header[148..156]) else {
        return false;
    };
    let actual = header
        .iter()
        .enumerate()
        .map(|(index, byte)| {
            if (148..156).contains(&index) {
                u32::from(b' ')
            } else {
                u32::from(*byte)
            }
        })
        .sum::<u32>();
    actual == expected
}

fn parse_tar_checksum(field: &[u8]) -> Option<u32> {
    let field = field
        .iter()
        .copied()
        .skip_while(|byte| *byte == b' ' || *byte == 0)
        .take_while(|byte| *byte != b' ' && *byte != 0)
        .collect::<Vec<_>>();
    if field.is_empty() || field.iter().any(|byte| !(b'0'..=b'7').contains(byte)) {
        return None;
    }
    field.into_iter().try_fold(0_u32, |value, byte| {
        value.checked_mul(8)?.checked_add(u32::from(byte - b'0'))
    })
}

#[cfg(feature = "git")]
fn automatic_git_view(path: &Path) -> Result<String, Error> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| usage_error("cannot derive a Git view name; pass --git-view"))?;
    casita::experimental::git_view_root_name(name)?;
    Ok(name.into())
}

async fn import_tar<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    input: &Path,
    name: RootName,
    args: TarImportArgs,
    retention: Option<casita::RootRetention>,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore,
    SS: casita::experimental::MetadataStore + 'static,
{
    let input = open_archive_input(&input.to_string_lossy()).await?;
    let mut request =
        casita::import::TarImport::new(input, name.clone()).with_limits(args.limits());
    if let Some(retention) = retention {
        request = request.with_retention(retention);
    }
    let report = request.import(repository).await?;
    println!("root {}", report.root);
    println!("name {name}");
    println!("archive-bytes {}", report.archive_bytes);
    println!("entries {}", report.entries);
    println!("files {}; directories {}", report.files, report.directories);
    println!(
        "symlinks {}; hardlinks {}",
        report.symlinks, report.hardlinks
    );
    println!(
        "file-bytes {}; sparse-expansion-bytes {}",
        report.file_bytes, report.sparse_expansion_bytes
    );
    Ok(())
}

#[cfg(feature = "oci")]
async fn import_oci<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    reference: &Path,
    name: RootName,
    args: OciImportArgs,
    rootfs_name: Option<RootName>,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore,
    SS: casita::experimental::MetadataStore + 'static,
{
    use oci_client::client::{ClientConfig, ClientProtocol};
    use oci_client::{Client, Reference};

    let reference: Reference = reference
        .to_str()
        .ok_or_else(|| usage_error("OCI image reference must be UTF-8"))?
        .parse()?;
    let mut config = ClientConfig::default();
    if args.http {
        config.protocol = ClientProtocol::Http;
    }
    if let Some(platform) = args.platform.as_deref() {
        let parts: Vec<_> = platform.split('/').collect();
        if !(2..=3).contains(&parts.len()) || parts.iter().any(|part| part.is_empty()) {
            return Err(usage_error("--oci-platform requires OS/ARCH[/VARIANT]"));
        }
    }
    let limits = casita::OciImportLimits {
        max_blob_bytes: args.max_blob_bytes,
        max_total_blob_bytes: args.max_total_blob_bytes,
        ..Default::default()
    };
    let mut request = casita::import::OciImport::new(reference, name.clone())
        .with_client(Client::new(config))
        .with_limits(limits);
    if let Some(platform) = args.platform {
        request = request.with_platform(platform);
    }
    if let Some(root) = rootfs_name.as_ref() {
        request = request
            .with_rootfs(root.clone())
            .with_rootfs_limits(casita::OciRootfsLimits {
                max_layer_bytes: args.rootfs_max_bytes,
                max_total_archive_bytes: args.rootfs_max_bytes,
                max_entries: args.rootfs_max_entries,
                max_tree_entries: args.rootfs_max_entries,
                ..Default::default()
            });
    }
    let report = request.import(repository).await?;
    println!("root {}", report.root);
    println!("name {name}");
    println!("manifest {}", report.manifest_digest);
    println!("layers {}; blob-bytes {}", report.layers, report.blob_bytes);
    if let Some(key) = report.rootfs {
        println!("rootfs {key}");
        println!(
            "rootfs-name {}",
            rootfs_name.expect("requested filesystem root")
        );
    }
    Ok(())
}

fn archive_stats_json(stats: &casita::experimental::CasitarStats) -> serde_json::Value {
    serde_json::json!({
        "archive_bytes": stats.archive_bytes,
        "archive_digest": stats.archive_digest.map(|digest| digest.to_string()),
        "header_bytes": stats.header_bytes,
        "payload_bytes": stats.payload_bytes,
        "payloads": stats.payloads,
        "record_bytes": stats.record_bytes,
        "records": stats.records,
    })
}

fn emit_archive_summary(
    operation: &str,
    validity: &str,
    roots: &[ObjectKey],
    stats: &casita::experimental::CasitarStats,
    json: bool,
    stderr: bool,
) -> Result<(), Error> {
    if json {
        let value = serde_json::json!({
            "operation": operation,
            "roots": roots.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "schema": "casita.archive.v1",
            "stats": archive_stats_json(stats),
            "validity": validity,
        });
        println!("{}", serde_json::to_string(&value)?);
        return Ok(());
    }

    let digest = stats
        .archive_digest
        .map_or_else(|| "-".to_owned(), |digest| digest.to_string());
    let lines = [
        format!("validity {validity}"),
        format!("archive-digest {digest}"),
        format!("archive-bytes {}", stats.archive_bytes),
        format!("roots {}", roots.len()),
        format!(
            "payloads {}; payload-bytes {}",
            stats.payloads, stats.payload_bytes
        ),
        format!(
            "records {}; record-bytes {}",
            stats.records, stats.record_bytes
        ),
    ];
    for line in lines {
        if stderr {
            eprintln!("{line}");
        } else {
            println!("{line}");
        }
    }
    for (index, root) in roots.iter().enumerate() {
        if stderr {
            eprintln!("root[{index}] {root}");
        } else {
            println!("root[{index}] {root}");
        }
    }
    Ok(())
}

fn emit_archive_create_report(
    report: &casita::experimental::CasitarExportReport,
    json: bool,
    stderr: bool,
) -> Result<(), Error> {
    if json {
        let value = serde_json::json!({
            "named_roots": report.named_roots.iter().map(|(name, root)| serde_json::json!({
                "name": name.to_string(),
                "root": root.to_string(),
            })).collect::<Vec<_>>(),
            "operation": "create",
            "roots": report.roots.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "schema": "casita.archive.v1",
            "source_revision": report.source_revision.to_string(),
            "stats": archive_stats_json(&report.stats),
            "validity": "verified",
        });
        println!("{}", serde_json::to_string(&value)?);
        return Ok(());
    }

    emit_archive_summary(
        "create",
        "verified",
        &report.roots,
        &report.stats,
        false,
        stderr,
    )?;
    if stderr {
        eprintln!("source-revision {}", report.source_revision);
        for (name, root) in &report.named_roots {
            eprintln!("source-root {name} {root}");
        }
    } else {
        println!("source-revision {}", report.source_revision);
        for (name, root) in &report.named_roots {
            println!("source-root {name} {root}");
        }
    }
    Ok(())
}

async fn archive_create<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    args: ArchiveCreateArgs,
    workspace: Option<&Workspace>,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore,
    SS: casita::experimental::MetadataStore + 'static,
{
    if args.output == "-" && args.json {
        return Err(usage_error(
            "archive create --json cannot share stdout with archive bytes",
        ));
    }
    if args.output == "-" && args.force {
        return Err(usage_error(
            "archive create --force is only meaningful for a file output",
        ));
    }

    let mut targets = Vec::with_capacity(args.roots.len() + args.objects.len());
    for root in args.roots {
        targets.push(casita::experimental::CasitarExportTarget::NamedRoot(
            scoped_root(workspace, root)?,
        ));
    }
    for object in args.objects {
        targets.push(casita::experimental::CasitarExportTarget::ExactObject(
            object.parse()?,
        ));
    }

    let limits = args.limits.stream_limits();
    let report = if args.output == "-" {
        let (_, report) = repository
            .export_casitar(targets, tokio::io::stdout(), limits)
            .await?;
        report
    } else {
        let policy = if args.force {
            casita::experimental::CasitarExportFilePolicy::Replace
        } else {
            casita::experimental::CasitarExportFilePolicy::CreateNew
        };
        repository
            .export_casitar_file_with_policy(targets, Path::new(&args.output), limits, policy)
            .await?
    };
    emit_archive_create_report(&report, args.json, args.output == "-")
}

async fn archive_inspect(args: ArchiveInspectArgs) -> Result<(), Error> {
    let input = open_archive_input(&args.input).await?;
    let mut reader =
        casita::experimental::CasitarReader::open(input, args.limits.stream_limits()).await?;
    let roots = reader.header().roots().to_vec();
    while let Some(frame) = reader.next_frame().await? {
        if matches!(
            frame,
            casita::experimental::CasitarReadFrame::Payload { .. }
        ) {
            reader.read_payload_to(&mut tokio::io::sink()).await?;
        }
    }
    let (_, stats) = reader.into_inner()?;
    emit_archive_summary("inspect", "structural", &roots, &stats, args.json, false)
}

async fn archive_verify(
    args: ArchiveVerifyArgs,
    spill_limits: casita::experimental::SpillLimits,
) -> Result<(), Error> {
    let input = open_archive_input(&args.input).await?;
    let reader =
        casita::experimental::CasitarReader::open(input, args.limits.stream_limits()).await?;
    let roots = reader.header().roots().to_vec();
    let destinations = roots
        .iter()
        .enumerate()
        .map(|(index, _)| RootName::try_from(format!("verify/{index}")))
        .collect::<Result<Vec<_>, _>>()?;
    let temporary = tempfile::tempdir()?;
    let report: Result<_, Error> = async {
        let repository = casita::experimental::Repository::local(temporary.path())
            .await?
            .with_spill_limits(spill_limits);
        Ok(repository
            .import(casita::import::CasitarImport::from_reader(
                reader,
                destinations,
            ))
            .await?)
    }
    .await;
    // Session drops schedule asynchronous pin releases. Keep the temporary
    // repository alive until those complete, including when import fails.
    let cleanup = casita::experimental::flush_repository_leases().await;
    let report = report?;
    cleanup?;
    emit_archive_summary(
        "verify",
        "verified",
        &roots,
        &report.stats,
        args.json,
        false,
    )
}

async fn archive_import<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    args: ArchiveImportArgs,
    workspace: Option<&Workspace>,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore,
    SS: casita::experimental::MetadataStore + 'static,
{
    let input = open_archive_input(&args.input).await?;
    let reader =
        casita::experimental::CasitarReader::open(input, args.limits.stream_limits()).await?;
    let roots = reader.header().roots().to_vec();
    let destinations = if let Some(prefix) = args.root_prefix {
        let prefix = scoped_root(workspace, prefix)?;
        roots
            .iter()
            .enumerate()
            .map(|(index, _)| RootName::try_from(format!("{prefix}/{index}")))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        args.roots
            .into_iter()
            .map(|name| scoped_root(workspace, name))
            .collect::<Result<Vec<_>, _>>()?
    };
    let policy = if args.replace {
        casita::experimental::CasitarRootConflictPolicy::ReplaceIfUnchanged
    } else {
        casita::experimental::CasitarRootConflictPolicy::RequireAbsent
    };
    let report = casita::import::CasitarImport::from_reader(reader, destinations)
        .with_conflict_policy(policy)
        .import(repository)
        .await?;

    if args.json {
        let value = serde_json::json!({
            "destination_revision": report.destination_revision.to_string(),
            "mappings": report.mappings.iter().map(|mapping| serde_json::json!({
                "index": mapping.index,
                "name": mapping.name.to_string(),
                "root": mapping.root.to_string(),
            })).collect::<Vec<_>>(),
            "records_inserted": report.records_inserted,
            "records_reused": report.records_reused,
            "payloads_reused": report.payloads_reused,
            "payloads_written": report.payloads_written,
            "operation": "import",
            "schema": "casita.archive.v1",
            "spill": {
                "files_opened": report.spill.files_opened,
                "peak_bytes": report.spill.peak_bytes,
            },
            "stats": archive_stats_json(&report.stats),
            "validity": "imported",
        });
        println!("{}", serde_json::to_string(&value)?);
    } else {
        emit_archive_summary("import", "imported", &roots, &report.stats, false, false)?;
        println!("destination-revision {}", report.destination_revision);
        println!(
            "records-inserted {}; records-reused {}",
            report.records_inserted, report.records_reused
        );
        println!(
            "payloads-written {}; payloads-reused {}",
            report.payloads_written, report.payloads_reused
        );
        for mapping in &report.mappings {
            println!(
                "mapping[{}] {} {}",
                mapping.index, mapping.root, mapping.name
            );
        }
    }
    Ok(())
}

async fn import_casitar<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    input: &Path,
    args: CasitarImportArgs,
    workspace: Option<&Workspace>,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore,
    SS: casita::experimental::MetadataStore + 'static,
{
    let input = open_archive_input(&input.to_string_lossy()).await?;
    let reader = casita::experimental::CasitarReader::open(input, args.stream_limits()).await?;
    let roots = reader.header().roots().to_vec();
    let destinations = if let Some(prefix) = args.root_prefix {
        let prefix = scoped_root(workspace, prefix)?;
        roots
            .iter()
            .enumerate()
            .map(|(index, _)| RootName::try_from(format!("{prefix}/{index}")))
            .collect::<Result<Vec<_>, _>>()?
    } else if args.roots.is_empty() {
        return Err(usage_error(
            "casitar import requires --casitar-root or --casitar-root-prefix",
        ));
    } else {
        args.roots
            .into_iter()
            .map(|name| scoped_root(workspace, name))
            .collect::<Result<Vec<_>, _>>()?
    };
    let policy = if args.replace {
        casita::experimental::CasitarRootConflictPolicy::ReplaceIfUnchanged
    } else {
        casita::experimental::CasitarRootConflictPolicy::RequireAbsent
    };
    let report = casita::import::CasitarImport::from_reader(reader, destinations)
        .with_conflict_policy(policy)
        .import(repository)
        .await?;

    println!("destination-revision {}", report.destination_revision);
    println!(
        "records-inserted {}; records-reused {}",
        report.records_inserted, report.records_reused
    );
    println!(
        "payloads-written {}; payloads-reused {}",
        report.payloads_written, report.payloads_reused
    );
    for mapping in &report.mappings {
        println!(
            "mapping[{}] {} {}",
            mapping.index, mapping.root, mapping.name
        );
    }
    Ok(())
}

async fn resolve_root_target<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    value: &str,
) -> Result<ObjectKey, Error>
where
    PS: casita::experimental::BlobStore,
    SS: casita::experimental::MetadataStore + 'static,
{
    if value.contains(':') {
        return Ok(value.parse()?);
    }
    let digest: Digest = value.parse()?;
    let snapshot = repository.metadata().snapshot().await?;
    let directory = ObjectKey::directory(DirectoryId::new(digest));
    let blob = ObjectKey::blob(BlobId::new(digest));
    match (
        snapshot.object(&directory).await?.is_some(),
        snapshot.object(&blob).await?.is_some(),
    ) {
        (true, false) => Ok(directory),
        (false, true) => Ok(blob),
        (false, false) => Err(casita::experimental::RepositoryError::Absent(format!(
            "no blob or directory record has digest {digest}"
        ))
        .into()),
        (true, true) => Err(casita::experimental::RepositoryError::InvalidInput(format!(
            "digest {digest} is ambiguous; supply the full generic object key"
        ))
        .into()),
    }
}

async fn generic_root<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    command: RootCommand,
    workspace: Option<&Workspace>,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore,
    SS: casita::experimental::MetadataStore + 'static,
{
    match command {
        RootCommand::Set {
            name,
            target,
            retention,
        } => {
            let name = scoped_root(workspace, name)?;
            let target = resolve_root_target(repository, &target).await?;
            if let Some(retention) = retention {
                repository
                    .set_root_with_retention(name, target, retention.into())
                    .await?;
            } else {
                repository
                    .mutation_session()
                    .await?
                    .publish(Vec::new(), vec![RootChange::Set { name, target }])
                    .await?;
            }
        }
        RootCommand::Retention { name, policy } => {
            let name = scoped_root(workspace, name)?;
            repository.set_root_retention(&name, policy.into()).await?;
        }
        RootCommand::Rm { name, prefix } => {
            let snapshot = repository.metadata().snapshot().await?;
            let roots = snapshot.roots().try_collect::<Vec<_>>().await?;
            let selected = if let Some(prefix) = prefix {
                let prefix = match workspace {
                    Some(workspace) => workspace.scoped_prefix(&prefix)?,
                    None => RootName::try_from(prefix)?,
                };
                roots
                    .into_iter()
                    .filter(|root| root.name().is_under(&prefix))
                    .collect::<Vec<_>>()
            } else {
                let name = scoped_root(
                    workspace,
                    name.expect("clap requires a name when --prefix is absent"),
                )?;
                roots
                    .into_iter()
                    .filter(|root| root.name() == &name)
                    .collect::<Vec<_>>()
            };
            if selected.is_empty() {
                return Err(
                    casita::experimental::RepositoryError::Absent("root name".to_owned()).into(),
                );
            }
            let changes = selected
                .iter()
                .map(|root| RootChange::Remove {
                    name: root.name().clone(),
                })
                .collect();
            repository
                .mutation_session()
                .await?
                .publish(Vec::new(), changes)
                .await?;
            for root in selected {
                let name = workspace
                    .and_then(|workspace| workspace.display_name(root.name()))
                    .unwrap_or_else(|| root.name().as_str());
                println!("removed {name}");
            }
        }
        RootCommand::Ls { prefix, long } => {
            let prefix = match workspace {
                Some(workspace) => Some(workspace.scoped_prefix(&prefix)?),
                None if prefix.is_empty() => None,
                None => Some(RootName::try_from(prefix)?),
            };
            let snapshot = repository.metadata().snapshot().await?;
            let mut roots = snapshot.roots();
            while let Some(root) = roots.next().await {
                let root = root?;
                if prefix
                    .as_ref()
                    .is_none_or(|prefix| root.name().is_under(prefix))
                {
                    let name = workspace
                        .and_then(|workspace| workspace.display_name(root.name()))
                        .unwrap_or_else(|| root.name().as_str());
                    if long {
                        let retention = repository.root_retention(root.name()).await?;
                        let retention = match retention {
                            Some(casita::RootRetention::Evictable) => "evictable",
                            _ => "permanent",
                        };
                        println!("{}  {retention}  {name}", root.target());
                    } else {
                        println!("{}  {name}", root.target());
                    }
                }
            }
        }
    }
    Ok(())
}

async fn print_logical_fsck<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
) -> Result<casita::experimental::FsckReport, Error>
where
    PS: casita::experimental::BlobGc + 'static,
    SS: casita::experimental::MetadataStore + 'static,
{
    let report = repository.fsck().await?;
    println!(
        "revision {}; checked {} root(s), {} object(s), {} payload(s)",
        report.revision, report.roots_checked, report.objects_checked, report.payloads_checked
    );
    println!(
        "traversal-spill-files-opened {}; traversal-spill-peak-bytes {}",
        report.spill.files_opened, report.spill.peak_bytes
    );
    for issue in &report.issues {
        let object = issue
            .object
            .as_ref()
            .map_or_else(|| "-".to_owned(), ToString::to_string);
        println!(
            "{:?} {:?} {object} {}",
            issue.disposition, issue.kind, issue.message
        );
    }
    Ok(report)
}

async fn print_fsck(
    repository: &casita::experimental::Repository<
        casita::experimental::ChunkedBlobStore,
        casita::experimental::TursoMetadataStore,
    >,
    args: FsckArgs,
    spill_limits: casita::experimental::SpillLimits,
) -> Result<(), Error> {
    if args.audit_only {
        let started = std::time::Instant::now();
        let logical = print_logical_fsck(repository).await?;
        println!("logical-fsck-nanos {}", started.elapsed().as_nanos());
        if !logical.is_healthy() {
            return Err(casita::experimental::RepositoryError::Metadata(
                casita::experimental::MetadataError::Corruption(
                    "fsck found repository corruption".to_owned(),
                ),
            )
            .into());
        }
        return Ok(());
    }
    let replica = match args.source {
        Some(path) => Some(
            casita::experimental::Repository::local(path)
                .await?
                .with_spill_limits(spill_limits),
        ),
        None => None,
    };
    let physical_started = std::time::Instant::now();
    let report = match (args.dry_run, replica.as_ref()) {
        (true, replica) => repository.preview_fsck_repair(replica).await?,
        (false, replica) => repository.fsck_repair(replica).await?,
    };
    let physical_elapsed = physical_started.elapsed();
    println!(
        "physical repair: revision {}; checked {} payload(s), {} byte(s), {} outboard(s)",
        report.revision, report.payloads_checked, report.bytes_checked, report.outboards_checked
    );
    println!(
        "traversal-spill-files-opened {}; traversal-spill-peak-bytes {}",
        report.spill.files_opened, report.spill.peak_bytes
    );
    println!("physical-fsck-nanos {}", physical_elapsed.as_nanos());
    for action in &report.actions {
        println!(
            "{:?} {:?} {} {} byte(s)",
            action.status, action.kind, action.payload, action.bytes
        );
    }
    for finding in &report.findings {
        println!(
            "{:?} {} {} byte(s) {}",
            finding.kind, finding.payload, finding.bytes, finding.message
        );
    }
    let physical_healthy = report.is_healthy();
    let logical_started = std::time::Instant::now();
    let logical = print_logical_fsck(repository).await?;
    println!(
        "logical-fsck-nanos {}",
        logical_started.elapsed().as_nanos()
    );
    if !physical_healthy || !logical.is_healthy() {
        return Err(casita::experimental::RepositoryError::Metadata(
            casita::experimental::MetadataError::Corruption(
                "fsck found repository corruption".to_owned(),
            ),
        )
        .into());
    }
    Ok(())
}

enum SyncSelection {
    Request(casita::experimental::TransferRequest),
    Path {
        source_root: RootName,
        path: String,
        destination_root: Option<RootName>,
    },
}

struct SyncSource {
    source: Box<dyn casita::experimental::TransferSource>,
    payloads: Option<casita::experimental::ChunkedBlobStore>,
}

async fn open_sync_source(
    endpoint: &str,
    writer: &str,
    pack_target_bytes: Option<u64>,
    pack_cache_bytes: Option<u64>,
) -> Result<SyncSource, Error> {
    #[cfg(not(feature = "s3"))]
    let _ = (writer, pack_cache_bytes);
    let source_payloads;
    let source: Box<dyn casita::experimental::TransferSource> =
        if let Some(location) = s3_location(endpoint)? {
            #[cfg(feature = "s3")]
            {
                let repository = open_s3_repository(
                    location,
                    writer.to_owned(),
                    pack_target_bytes,
                    pack_cache_bytes,
                )
                .await?;
                source_payloads = Some(repository.payloads().clone());
                Box::new(repository)
            }
            #[cfg(not(feature = "s3"))]
            {
                let _ = location;
                return Err(usage_error("S3 sync requires the `s3` Cargo feature"));
            }
        } else if endpoint.starts_with("ssh://") {
            #[cfg(feature = "ssh")]
            {
                source_payloads = None;
                let endpoint = endpoint.parse::<casita::experimental::SshEndpoint>()?;
                Box::new(casita::experimental::SshTransferSource::new(endpoint))
            }
            #[cfg(not(feature = "ssh"))]
            {
                return Err("SSH sync requires the `ssh` Cargo feature".into());
            }
        } else {
            let path = PathBuf::from(endpoint);
            let repository = match pack_target_bytes {
                Some(target) => {
                    casita::experimental::Repository::local_with_pack_options(
                        path,
                        casita::experimental::PackOptions {
                            target_size: target,
                            ..Default::default()
                        },
                    )
                    .await?
                }
                None => casita::experimental::Repository::local(path).await?,
            };
            source_payloads = Some(repository.payloads().clone());
            Box::new(repository)
        };
    Ok(SyncSource {
        source,
        payloads: source_payloads,
    })
}

async fn generic_sync(
    args: SyncArgs,
    spill_limits: casita::experimental::SpillLimits,
    pack_target_bytes: Option<u64>,
    pack_cache_bytes: Option<u64>,
) -> Result<(), Error> {
    if args.path.is_some() && (!args.objects.is_empty() || args.roots.len() != 1 || args.shallow) {
        return Err(usage_error(
            "sync --path requires exactly one --root and cannot be combined with --object or --shallow",
        ));
    }
    if args.objects.is_empty() && args.roots.is_empty() {
        return Err(usage_error(
            "sync requires at least one --object or --root selector",
        ));
    }
    if (pack_cache_bytes.is_some())
        && !args.from.starts_with("s3://")
        && !args.to.starts_with("s3://")
        && !args
            .from_blobs
            .as_deref()
            .is_some_and(|endpoint| endpoint.starts_with("s3://"))
    {
        return Err(usage_error(
            "pack cache tuning requires an S3 sync endpoint",
        ));
    }
    let writer = sync_writer(args.writer.as_deref())?;
    let metadata_source =
        open_sync_source(&args.from, &writer, pack_target_bytes, pack_cache_bytes).await?;
    let blob_source = match args.from_blobs.as_deref() {
        Some(endpoint) => {
            Some(open_sync_source(endpoint, &writer, pack_target_bytes, pack_cache_bytes).await?)
        }
        None => None,
    };
    let transfer_selection = casita::experimental::TransferSelection::Selected {
        objects: args
            .objects
            .iter()
            .map(|value| value.parse())
            .collect::<Result<Vec<_>, _>>()?,
        roots: args
            .roots
            .iter()
            .map(|value| RootName::try_from(value.clone()))
            .collect::<Result<Vec<_>, _>>()?,
    };
    let source = metadata_source
        .source
        .begin_transfer(transfer_selection)
        .await?;
    let source: Box<dyn casita::experimental::TransferReadSession + '_> = match &blob_source {
        Some(blobs) => Box::new(casita::experimental::SplitTransferSession::new(
            source,
            blobs
                .source
                .begin_transfer(casita::experimental::TransferSelection::Snapshot)
                .await?,
        )),
        None => source,
    };

    let selection = if let Some(path) = args.path {
        SyncSelection::Path {
            source_root: RootName::try_from(args.roots[0].clone())?,
            path,
            destination_root: args.destination_root.map(RootName::try_from).transpose()?,
        }
    } else {
        let mut selected = BTreeMap::<ObjectKey, bool>::new();
        for value in args.objects {
            let key: ObjectKey = value.parse()?;
            selected
                .entry(key)
                .and_modify(|recursive| *recursive |= !args.shallow)
                .or_insert(!args.shallow);
        }
        let mut roots = BTreeMap::<RootName, ObjectKey>::new();
        for value in args.roots {
            let name = RootName::try_from(value)?;
            let target = source.root(&name).await?.ok_or_else(|| {
                casita::experimental::RepositoryError::Absent(format!("root `{name}`"))
            })?;
            selected
                .entry(target.clone())
                .and_modify(|recursive| *recursive = true)
                .or_insert(true);
            roots.insert(name, target);
        }
        SyncSelection::Request(casita::experimental::TransferRequest {
            objects: selected
                .into_iter()
                .map(|(key, recursive)| casita::experimental::ObjectRequest { key, recursive })
                .collect(),
            roots: roots
                .into_iter()
                .map(|(name, target)| casita::experimental::DestinationRoot { name, target })
                .collect(),
        })
    };
    if let Some(location) = s3_location(&args.to)? {
        #[cfg(feature = "s3")]
        {
            let destination =
                open_s3_repository(location, writer, pack_target_bytes, pack_cache_bytes)
                    .await?
                    .with_spill_limits(spill_limits);
            let result =
                finish_sync(source.as_ref(), &destination, selection, args.incremental).await;
            print_sync_source_stats(&metadata_source, blob_source.as_ref());
            print_pack_stats(destination.payloads());
            return result;
        }
        #[cfg(not(feature = "s3"))]
        {
            let _ = location;
            return Err(usage_error("S3 sync requires the `s3` Cargo feature"));
        }
    }
    let path = PathBuf::from(args.to);
    let destination = match pack_target_bytes {
        Some(target) => {
            casita::experimental::Repository::local_with_pack_options(
                path,
                casita::experimental::PackOptions {
                    target_size: target,
                    ..Default::default()
                },
            )
            .await?
        }
        None => casita::experimental::Repository::local(path).await?,
    }
    .with_spill_limits(spill_limits);
    let result = finish_sync(source.as_ref(), &destination, selection, args.incremental).await;
    print_sync_source_stats(&metadata_source, blob_source.as_ref());
    print_pack_stats(destination.payloads());
    result
}

fn print_sync_source_stats(metadata: &SyncSource, blobs: Option<&SyncSource>) {
    if let Some(payloads) = &metadata.payloads {
        print_pack_stats_with_prefix(
            payloads,
            if blobs.is_some() {
                "source-repo-"
            } else {
                "source-"
            },
        );
    }
    if let Some(payloads) = blobs.and_then(|source| source.payloads.as_ref()) {
        print_pack_stats_with_prefix(payloads, "source-blobs-");
    }
}

async fn finish_sync<PS, SS>(
    source: &dyn casita::experimental::TransferReadSession,
    destination: &casita::experimental::Repository<PS, SS>,
    selection: SyncSelection,
    incremental: bool,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore,
    SS: casita::experimental::MetadataStore + 'static,
{
    let discovery = if incremental {
        casita::experimental::TransferDiscovery::ReuseVerified
    } else {
        casita::experimental::TransferDiscovery::Exhaustive
    };
    match selection {
        SyncSelection::Request(request) => {
            let result = casita::experimental::transfer(
                &casita::experimental::HeldSession(source),
                destination,
                request,
                casita::experimental::TransferOptions::default().with_discovery(discovery),
            )
            .await?;
            print_transfer_progress(result.progress);
        }
        SyncSelection::Path {
            source_root,
            path,
            destination_root,
        } => {
            let outcome = casita::experimental::transfer_path(
                &casita::experimental::HeldSession(source),
                destination,
                &source_root,
                &path,
                destination_root.clone(),
                casita::experimental::TransferOptions::default().with_discovery(discovery),
            )
            .await?;
            let node = outcome.node.ok_or_else(|| {
                casita::experimental::RepositoryError::Absent(format!(
                    "path `{path}` beneath source root `{source_root}`"
                ))
            })?;
            match node {
                Node::Directory { digest, .. } => println!("selected directory {digest}"),
                Node::File { digest, .. } => println!("selected blob {digest}"),
                Node::Symlink { target } => println!("selected symlink -> {target}"),
            }
            if let Some(name) = destination_root {
                println!("root {name}");
            }
            if let Some(result) = outcome.transfer {
                print_transfer_progress(result.progress);
            }
        }
    }
    Ok(())
}

fn print_transfer_progress(progress: casita::experimental::TransferProgress) {
    println!("revision {}", progress.destination_revision);
    println!("published-objects {}", progress.published_objects);
    println!("payloads-sent {}", progress.payloads_sent);
    println!("payloads-reused {}", progress.payloads_reused);
    println!("chunks-sent {}", progress.chunks_sent);
    println!("chunks-reused {}", progress.chunks_reused);
    println!("slice-copy-bytes {}", progress.slice_copy_bytes);
    println!("slice-literal-bytes {}", progress.slice_literal_bytes);
    for status in progress.requested {
        println!("status {status:?}");
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct S3Location {
    bucket: String,
    prefix: String,
}

fn s3_location(value: &str) -> Result<Option<S3Location>, Error> {
    let Some(location) = value.strip_prefix("s3://") else {
        return Ok(None);
    };
    if location.is_empty() || location.contains(['?', '#']) {
        return Err(usage_error(
            "S3 repository URLs must be s3://BUCKET or s3://BUCKET/PREFIX",
        ));
    }
    let (bucket, prefix) = location.split_once('/').unwrap_or((location, ""));
    if bucket.is_empty() {
        return Err(usage_error(
            "S3 repository URLs must be s3://BUCKET or s3://BUCKET/PREFIX",
        ));
    }
    Ok(Some(S3Location {
        bucket: bucket.to_owned(),
        prefix: prefix.trim_matches('/').to_owned(),
    }))
}

#[cfg(feature = "s3")]
async fn open_s3_repository(
    location: S3Location,
    writer: String,
    pack_target_bytes: Option<u64>,
    pack_cache_bytes: Option<u64>,
) -> Result<
    casita::experimental::Repository<
        casita::experimental::ChunkedBlobStore,
        casita::experimental::Wal3MetadataStore,
    >,
    Error,
> {
    Ok(casita::experimental::Repository::s3_with_pack_options(
        location.bucket,
        location.prefix,
        writer,
        casita::experimental::PackOptions {
            target_size: pack_target_bytes
                .unwrap_or(casita::experimental::DEFAULT_PACK_TARGET_SIZE),
            cache_capacity: pack_cache_bytes
                .unwrap_or(casita::experimental::DEFAULT_PACK_CACHE_CAPACITY),
        },
    )
    .await?)
}

fn sync_writer(explicit: Option<&str>) -> Result<String, Error> {
    if let Some(writer) = explicit.map(ToOwned::to_owned).or_else(|| {
        std::env::var("CASITA_WRITER")
            .ok()
            .filter(|name| !name.is_empty())
    }) {
        return Ok(writer);
    }
    let mut instance = [0; 8];
    getrandom::fill(&mut instance).map_err(|error| format!("runner identity: {error}"))?;
    Ok(format!(
        "casita-{}-{}",
        std::process::id(),
        data_encoding::HEXLOWER.encode(&instance)
    ))
}

fn pin_inventory_json(inventory: &casita::experimental::PinInventory) -> serde_json::Value {
    use casita::experimental::{PinResource, PinScope};
    let catalog = |bytes: &[u8]| {
        serde_json::json!({
            "digest": Digest::hash(bytes).to_string(), "bytes": bytes.len(),
        })
    };
    let resource = |resource: &PinResource| match resource {
        PinResource::Blob(id) => serde_json::json!({"kind": "blob", "id": id.to_string()}),
        PinResource::Chunk(id) => serde_json::json!({"kind": "chunk", "id": id.to_string()}),
        PinResource::StorageObject(path) => {
            serde_json::json!({"kind": "storage_object", "path": path})
        }
        PinResource::Object(key) => serde_json::json!({"kind": "object", "key": key.to_string()}),
        PinResource::Catalog(bytes) => {
            serde_json::json!({"kind": "catalog", "catalog": catalog(bytes)})
        }
        PinResource::MetadataObject(path) => {
            serde_json::json!({"kind": "metadata_object", "path": path})
        }
    };
    let pins = inventory.pins.iter().map(|(token, pin)| {
        let scope = match &pin.scope {
            PinScope::Snapshot { generation } => serde_json::json!({"kind": "snapshot", "generation": generation}),
            PinScope::Closures(roots) => serde_json::json!({"kind": "closures", "roots": roots.iter().map(ToString::to_string).collect::<Vec<_>>()}),
            PinScope::Staging => serde_json::json!({"kind": "staging"}),
            PinScope::Metadata => serde_json::json!({"kind": "metadata"}),
        };
        serde_json::json!({
            "token": token.to_string(), "scope": scope,
            "released": inventory.retired.contains(token),
            "catalog": pin.catalog.as_deref().map(catalog),
            "resources": pin.resources.iter().map(resource).collect::<Vec<_>>(),
        })
    }).collect::<Vec<_>>();
    let deletions = inventory.deletions.iter().map(|(token, resources)| serde_json::json!({
        "token": token.to_string(), "resources": resources.iter().map(resource).collect::<Vec<_>>(),
    })).collect::<Vec<_>>();
    serde_json::json!({
        "revision": inventory.revision, "pins": pins, "deletions": deletions,
        "collector": inventory.collector.as_ref().map(ToString::to_string),
        "logical_prune": inventory.logical_prune.as_ref().map(ToString::to_string),
    })
}

async fn list_holds(endpoint: &str, json: bool) -> Result<(), Error> {
    let (collectors, state, coordination) = if let Some(location) = s3_location(endpoint)? {
        #[cfg(not(feature = "s3"))]
        {
            let _ = location;
            return Err(usage_error(
                "S3 hold inspection requires the `s3` Cargo feature",
            ));
        }
        #[cfg(feature = "s3")]
        {
            let prefix = if location.prefix.is_empty() {
                "state".to_owned()
            } else {
                format!("{}/state", location.prefix)
            };
            let state = casita::experimental::Wal3MetadataStore::open_s3(
                location.bucket,
                prefix,
                sync_writer(None)?,
            )
            .await?;
            let collectors = state.repository_holds().await?.iter().map(|hold| serde_json::json!({
                "token": hold.token.as_str(), "writer": hold.writer, "exclusive": hold.exclusive,
            })).collect::<Vec<_>>();
            let pins = state.pin_store().await?.inventory().await?;
            let coordination = state
                .repository_coordination_pin_store()
                .await?
                .inventory()
                .await?;
            (collectors, pins, Some(coordination))
        }
    } else {
        let database = Path::new(endpoint).join("casita.sqlite");
        if !database.is_file() {
            return Err(usage_error(format!(
                "repository database {} is absent",
                database.display()
            )));
        }
        let state = casita::experimental::TursoMetadataStore::open(database).await?;
        (
            Vec::<serde_json::Value>::new(),
            state.pin_store().await?.inventory().await?,
            None,
        )
    };
    let state = pin_inventory_json(&state);
    let coordination = coordination.as_ref().map(pin_inventory_json);
    if json {
        println!(
            "{}",
            serde_json::json!({"collectors": collectors, "state": state, "coordination": coordination})
        );
    } else {
        for collector in collectors {
            println!("collector {}", serde_json::to_string(&collector)?);
        }
        for (name, inventory) in std::iter::once(("state", &state)).chain(
            coordination
                .as_ref()
                .map(|inventory| ("coordination", inventory)),
        ) {
            println!(
                "{name} ledger revision={} collector={} logical_prune={}",
                inventory["revision"], inventory["collector"], inventory["logical_prune"]
            );
            for pin in inventory["pins"].as_array().expect("encoded pins array") {
                println!("{name} pin {}", serde_json::to_string(pin)?);
            }
            for deletion in inventory["deletions"]
                .as_array()
                .expect("encoded deletion array")
            {
                println!("{name} deletion {}", serde_json::to_string(deletion)?);
            }
        }
    }
    Ok(())
}

fn print_pack_stats(payloads: &casita::experimental::ChunkedBlobStore) {
    print_pack_stats_with_prefix(payloads, "");
}

fn print_pack_stats_with_prefix(payloads: &casita::experimental::ChunkedBlobStore, prefix: &str) {
    if std::env::var_os("CASITA_PACK_STATS").is_none() {
        return;
    }
    let Some(stats) = payloads.pack_read_stats() else {
        return;
    };
    println!("{prefix}pack-list-requests {}", stats.list_requests);
    println!(
        "{prefix}pack-gc-manifest-list-requests {}",
        stats.gc_manifest_list_requests
    );
    println!(
        "{prefix}pack-gc-loose-chunk-list-requests {}",
        stats.gc_loose_chunk_list_requests
    );
    println!(
        "{prefix}pack-footer-range-requests {}",
        stats.footer_range_requests
    );
    println!(
        "{prefix}pack-footer-range-bytes {}",
        stats.footer_range_bytes
    );
    println!(
        "{prefix}pack-chunk-range-requests {}",
        stats.chunk_range_requests
    );
    println!("{prefix}pack-chunk-range-bytes {}", stats.chunk_range_bytes);
    println!("{prefix}pack-whole-requests {}", stats.whole_pack_requests);
    println!("{prefix}pack-whole-bytes {}", stats.whole_pack_bytes);
    println!("{prefix}pack-cache-hits {}", stats.cache_hits);
    println!("{prefix}pack-cache-promotions {}", stats.cache_promotions);
    println!("{prefix}pack-cache-evictions {}", stats.cache_evictions);
    println!(
        "{prefix}pack-gc-replacement-put-requests {}",
        stats.gc_replacement_put_requests
    );
    println!(
        "{prefix}pack-gc-replacement-put-bytes {}",
        stats.gc_replacement_put_bytes
    );
    println!(
        "{prefix}pack-gc-marker-put-requests {}",
        stats.gc_marker_put_requests
    );
    println!(
        "{prefix}pack-gc-marker-put-bytes {}",
        stats.gc_marker_put_bytes
    );
    println!(
        "{prefix}pack-gc-delete-requests {}",
        stats.gc_pack_delete_requests
    );
    println!(
        "{prefix}pack-gc-manifest-delete-requests {}",
        stats.gc_manifest_delete_requests
    );
    println!(
        "{prefix}pack-gc-outboard-delete-requests {}",
        stats.gc_outboard_delete_requests
    );
    println!(
        "{prefix}pack-gc-loose-chunk-delete-requests {}",
        stats.gc_loose_chunk_delete_requests
    );
    println!(
        "{prefix}pack-gc-tombstone-put-requests {}",
        stats.gc_tombstone_put_requests
    );
    println!(
        "{prefix}pack-gc-tombstone-put-bytes {}",
        stats.gc_tombstone_put_bytes
    );
    println!(
        "{prefix}pack-gc-tombstone-delete-requests {}",
        stats.gc_tombstone_delete_requests
    );
    println!("{prefix}pack-gc-deferred-packs {}", stats.gc_deferred_packs);
    println!(
        "{prefix}pack-index-pointer-requests {}",
        stats.index_pointer_requests
    );
    println!("{prefix}pack-index-requests {}", stats.index_requests);
    println!("{prefix}pack-index-bytes {}", stats.index_bytes);
    println!("{prefix}pack-index-hash-nanos {}", stats.index_hash_nanos);
    println!(
        "{prefix}pack-index-decode-nanos {}",
        stats.index_decode_nanos
    );
    println!("{prefix}pack-index-hits {}", stats.index_hits);
    println!("{prefix}pack-index-fallbacks {}", stats.index_fallbacks);
    println!(
        "{prefix}pack-index-put-requests {}",
        stats.index_put_requests
    );
    println!("{prefix}pack-index-put-bytes {}", stats.index_put_bytes);
}

#[cfg(feature = "ssh")]
async fn serve_ssh_source(args: SshSourceArgs) -> Result<(), Error> {
    let encoded = data_encoding::BASE64URL_NOPAD
        .decode(args.repository_base64.as_bytes())
        .map_err(|error| format!("invalid encoded SSH repository path: {error}"))?;
    let path =
        String::from_utf8(encoded).map_err(|_| "encoded SSH repository path is not valid UTF-8")?;
    let repository = casita::experimental::Repository::local(PathBuf::from(path)).await?;
    casita::experimental::serve_transfer_stdio(
        &repository,
        tokio::io::stdin(),
        tokio::io::stdout(),
    )
    .await?;
    Ok(())
}

#[cfg(feature = "git-http")]
async fn serve_native_git<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    view: String,
    listen: String,
    max_pack_bytes: usize,
    pack_compression_level: u32,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore + Clone + Send + Sync + 'static,
    SS: casita::experimental::MetadataStore + Clone + Send + Sync + 'static,
{
    if max_pack_bytes == 0 {
        return Err(usage_error("--max-pack-bytes must be at least 1"));
    }
    let mut limits = casita::experimental::GitFetchLimits::default();
    limits.max_pack_bytes = max_pack_bytes;
    limits.compression_level = pack_compression_level;
    let service = casita::experimental::GitFetchService::bind(repository, &view, limits).await?;
    let listener = tokio::net::TcpListener::bind(&listen).await?;
    let address = listener.local_addr()?;
    let route = format!("/{view}.git");
    println!("http://{address}{route}");
    casita::experimental::serve_git_smart_http_with_shutdown(
        listener,
        route,
        service,
        casita::experimental::GitHttpOptions::default(),
        async {
            let _ = tokio::signal::ctrl_c().await;
        },
    )
    .await?;
    Ok(())
}

#[cfg(not(feature = "git-http"))]
async fn serve_native_git<PS, SS>(
    _repository: &casita::experimental::Repository<PS, SS>,
    _view: String,
    _listen: String,
    _max_pack_bytes: usize,
    _pack_compression_level: u32,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore + Clone + Send + Sync + 'static,
    SS: casita::experimental::MetadataStore + Clone + Send + Sync + 'static,
{
    Err("native Git smart-HTTP serving requires the 'git-http' cargo feature".into())
}

async fn run_native_git<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    command: NativeGitCommand,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore + Clone + Send + Sync + 'static,
    SS: casita::experimental::MetadataStore + Clone + Send + Sync + 'static,
{
    match command {
        NativeGitCommand::Show { view } => {
            let (key, body) = casita::experimental::read_git_view(repository, &view)
                .await?
                .ok_or_else(|| {
                    casita::experimental::RepositoryError::Absent(format!("Git view `{view}`"))
                })?;
            println!("view {key}");
            println!("object-format {:?}", body.object_format);
            if let Some(default_ref) = body.default_ref {
                println!("default-ref {default_ref}");
            }
            for (name, value) in body.refs {
                match value {
                    casita::experimental::GitRefValue::Direct(target) => {
                        println!("ref {name} {target}");
                    }
                    casita::experimental::GitRefValue::Symbolic(target) => {
                        println!("ref {name} -> {target}");
                    }
                }
            }
        }
        NativeGitCommand::Checkout {
            tree,
            dir,
            skip_gitlinks,
        } => {
            let tree: ObjectKey = tree.parse()?;
            let policy = if skip_gitlinks {
                casita::experimental::GitlinkCheckoutPolicy::Skip
            } else {
                casita::experimental::GitlinkCheckoutPolicy::Error
            };
            casita::experimental::checkout_git_tree(repository, &tree, &dir, policy).await?;
            println!("checked out {tree} to {}", dir.display());
        }
        NativeGitCommand::Serve {
            view,
            listen,
            max_pack_bytes,
            pack_compression_level,
        } => {
            serve_native_git(
                repository,
                view,
                listen,
                max_pack_bytes,
                pack_compression_level,
            )
            .await?;
        }
    }
    Ok(())
}

/// Dispatch one CLI invocation from a stack-bounded handle.
///
/// The unboxed future contains the state for every enabled command branch and
/// is large enough to exhaust libtest's default worker stack. A real CLI only
/// pays this allocation once, while boxing here also keeps embedders and tests
/// from having to know about the dispatch future's implementation size.
pub(super) fn run(cli: Cli) -> impl std::future::Future<Output = Result<(), Error>> {
    Box::pin(run_inner(cli))
}

/// Build a command's future in its own frame and poll it from the heap.
///
/// Unoptimized builds give every future awaited inline in [`run_inner`] its own
/// stack slots in the dispatcher's poll frame, so the frame grew with the sum
/// of all command futures (about 430 KiB) and overflowed Windows stacks.
fn on_heap<'a, F>(
    start: impl FnOnce() -> F,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = F::Output> + 'a>>
where
    F: std::future::Future + 'a,
{
    Box::pin(start())
}

fn command_trace_name(command: &Command) -> &'static str {
    match command {
        Command::Init => "init",
        Command::Import(_) => "import",
        Command::Run(_) => "run",
        Command::Archive { .. } => "archive",
        Command::Object { .. } => "object",
        Command::Tree { .. } => "tree",
        Command::Sync(_) => "sync",
        Command::Holds { .. } => "holds",
        Command::Git { .. } => "git",
        Command::Ipc(_) => "ipc",
        Command::Checkout { .. } => "checkout",
        Command::Cat { .. } => "cat",
        Command::Root { .. } => "root",
        Command::Gc { .. } => "gc",
        Command::Vacuum => "vacuum",
        Command::Fsck(_) => "fsck",
        #[cfg(feature = "ssh")]
        Command::SshSource(_) => "ssh-source",
    }
}

#[tracing::instrument(
    name = "cli.run",
    skip_all,
    fields(command = tracing::field::Empty)
)]
async fn run_inner(cli: Cli) -> Result<(), Error> {
    let Cli {
        log_filter: _,
        log_format: _,
        repository,
        spill_memory_objects,
        spill_bytes,
        pack_target_bytes,
        pack_cache_bytes,

        command,
    } = cli;
    tracing::Span::current().record("command", command_trace_name(&command));
    let uses_default_repository = repository.is_none();
    let repository_dir = repository.unwrap_or_else(default_repository_dir);
    let mut spill_limits = casita::experimental::SpillLimits::default();
    if let Some(limit) = spill_memory_objects {
        if limit == 0 {
            return Err(usage_error("--spill-memory-objects must be at least 1"));
        }
        spill_limits.max_memory_objects = limit;
    }
    if let Some(limit) = spill_bytes {
        spill_limits.max_spill_bytes = limit;
    }
    if pack_target_bytes == Some(0) {
        return Err(usage_error("--pack-target-bytes must be at least 1"));
    }

    let command = match command {
        Command::Cat {
            key,
            verified: true,
            from: Some(endpoint),
        } => {
            let key = parse_blob_or_exact_key(&key)?;
            let source = open_sync_source(
                &endpoint,
                "verified-cat",
                pack_target_bytes,
                pack_cache_bytes,
            )
            .await?;
            let session = source
                .source
                .begin_transfer(casita::experimental::TransferSelection::Selected {
                    objects: vec![key.clone()],
                    roots: Vec::new(),
                })
                .await?;
            let record = session
                .object(&key)
                .await?
                .ok_or_else(|| casita::experimental::RepositoryError::Absent(key.to_string()))?;
            if record.key() != &key
                || key != ObjectKey::blob(record.payload())
                || !record.links().is_empty()
            {
                return Err(usage_error("remote verified cat requires a raw blob ID"));
            }
            let mut reader = session.open_verified(&record).await?.ok_or_else(|| {
                casita::experimental::RepositoryError::MissingPayload(record.payload())
            })?;
            let mut stdout = tokio::io::stdout();
            tokio::io::copy(&mut reader, &mut stdout).await?;
            stdout.flush().await?;
            return Ok(());
        }
        Command::Holds { endpoint, json } => return on_heap(|| list_holds(&endpoint, json)).await,
        // Run names belong to producer namespaces, not the workspace root
        // namespace. Project configuration supplies only explicit shortcuts,
        // even when --repository selects a different local store.
        Command::Run(args) => {
            if pack_cache_bytes.is_some() {
                return Err(usage_error(
                    "pack cache tuning is only supported for S3 sync endpoints",
                ));
            }
            return on_heap(|| {
                run::execute(args, &repository_dir, spill_limits, pack_target_bytes)
            })
            .await;
        }
        Command::Ipc(args) => {
            return on_heap(|| {
                crate::cli::ipc::serve_with_options(&repository_dir, args.options())
            })
            .await;
        }
        Command::Sync(args) => {
            return on_heap(|| {
                generic_sync(args, spill_limits, pack_target_bytes, pack_cache_bytes)
            })
            .await;
        }
        Command::Archive {
            command: ArchiveCommand::Inspect(args),
        } => return on_heap(|| archive_inspect(args)).await,
        Command::Archive {
            command: ArchiveCommand::Verify(args),
        } => return on_heap(|| archive_verify(args, spill_limits)).await,
        #[cfg(feature = "ssh")]
        Command::SshSource(args) => return on_heap(|| serve_ssh_source(args)).await,
        command => command,
    };
    if pack_cache_bytes.is_some() {
        return Err(usage_error(
            "pack cache tuning is only supported for S3 sync endpoints",
        ));
    }

    // An explicit repository is an administrative/service operation.  The
    // portable workspace attachment is deliberately only for the default
    // user-global repository.
    let workspace = if uses_default_repository && !matches!(command, Command::Init) {
        workspace::discover()?
    } else {
        None
    };

    let repository = match pack_target_bytes {
        Some(target) => {
            casita::experimental::Repository::local_with_pack_options(
                &repository_dir,
                casita::experimental::PackOptions {
                    target_size: target,
                    ..Default::default()
                },
            )
            .await?
        }
        None => casita::experimental::Repository::local(&repository_dir).await?,
    }
    .with_spill_limits(spill_limits);
    match command {
        Command::Init => {
            let revision = repository.metadata().snapshot().await?.revision();
            if uses_default_repository {
                let workspace = Workspace::create_at(std::env::current_dir()?)?;
                println!(
                    "initialized workspace {} at {} (global repository {})",
                    workspace.id(),
                    workspace.root().display(),
                    repository_dir.display()
                );
            } else {
                println!("initialized {} at {revision}", repository_dir.display());
            }
        }
        Command::Import(args) => {
            let repository = repository
                .clone()
                .with_chunk_upload_concurrency(args.chunk_upload_concurrency);
            #[cfg(feature = "git")]
            let automatically_detected = args.importer.is_none();
            let importer = match args.importer {
                Some(importer) => importer,
                None => detect_importer(&args.path).await?,
            };
            if args.file_concurrency.is_some() && importer != super::ImporterKind::Filesystem {
                return Err(usage_error(
                    "--filesystem-concurrency requires the filesystem importer",
                ));
            }
            if importer == super::ImporterKind::Tar {
                let name = args
                    .name
                    .map(|name| scoped_root(workspace.as_ref(), name))
                    .transpose()?
                    .ok_or_else(|| usage_error("tar import requires --root"))?;
                return on_heap(|| {
                    import_tar(
                        &repository,
                        &args.path,
                        name,
                        args.tar,
                        args.retention.map(Into::into),
                    )
                })
                .await;
            }
            #[cfg(feature = "oci")]
            if importer == super::ImporterKind::Oci {
                if args.retention.is_some() {
                    return Err(usage_error("--retention does not support OCI imports"));
                }
                let name = args
                    .name
                    .map(|name| scoped_root(workspace.as_ref(), name))
                    .transpose()?
                    .ok_or_else(|| usage_error("OCI import requires --root"))?;
                let rootfs_name = args
                    .oci
                    .rootfs_root
                    .as_ref()
                    .map(|name| scoped_root(workspace.as_ref(), name))
                    .transpose()?;
                return import_oci(&repository, &args.path, name, args.oci, rootfs_name).await;
            }
            if importer == super::ImporterKind::Casitar {
                if args.retention.is_some() {
                    return Err(usage_error("--retention does not support Casitar imports"));
                }
                if args.name.is_some() {
                    return Err(usage_error(
                        "casitar import uses --casitar-root, not --root",
                    ));
                }
                return on_heap(|| {
                    import_casitar(&repository, &args.path, args.casitar, workspace.as_ref())
                })
                .await;
            }
            if importer == super::ImporterKind::Git {
                if args.retention.is_some() {
                    return Err(usage_error("--retention does not support Git imports"));
                }
                #[cfg(feature = "git")]
                {
                    if args.name.is_some() {
                        return Err(usage_error("git import uses --git-view, not --root"));
                    }
                    let view_name = match args.git.view {
                        Some(view) => view,
                        None if automatically_detected => automatic_git_view(&args.path)?,
                        None => return Err(usage_error("git import requires --git-view")),
                    };
                    let import = casita::import::GitImport::new(args.path, view_name)
                        .with_refs(args.git.refs)?
                        .with_max_cached_pack_bytes(args.git.max_cached_pack_bytes)
                        .with_concurrency(args.git.concurrency)
                        .with_max_buffered_bytes(args.git.max_buffered_bytes);
                    let report = on_heap(|| import.import(&repository)).await?;
                    println!("view {}", report.view);
                    println!("objects {}", report.objects);
                    return Ok(());
                }
                #[cfg(not(feature = "git"))]
                {
                    return Err("git import requires the 'git' cargo feature".into());
                }
            }
            let name = match args.name {
                Some(name) => scoped_root(workspace.as_ref(), name)?,
                None => automatic_root(workspace.as_ref(), "auto", &canonical_source(&args.path))?,
            };
            let excludes_marker = imports_workspace_root(workspace.as_ref(), &args.path);
            let mut input =
                casita::import::FilesystemImport::new(&args.path, name.clone()).reread(args.rehash);
            if let Some(concurrency) = args.file_concurrency {
                input = input.with_file_concurrency(concurrency);
            }
            if excludes_marker {
                input = input.exclude(workspace::MARKER);
            }
            if let Some(retention) = args.retention {
                input = input.with_retention(retention.into());
            }
            let key = on_heap(|| repository.import(input)).await?;
            println!("{key}");
            println!("root {name}");
        }
        Command::Archive { command } => match command {
            ArchiveCommand::Create(args) => {
                on_heap(|| archive_create(&repository, args, workspace.as_ref())).await?
            }
            ArchiveCommand::Import(args) => {
                on_heap(|| archive_import(&repository, args, workspace.as_ref())).await?
            }
            ArchiveCommand::Inspect(_) | ArchiveCommand::Verify(_) => {
                unreachable!("handled before local open")
            }
        },
        Command::Object { command } => match command {
            ObjectCommand::Show { key } => on_heap(|| object_show(&repository, &key)).await?,
        },
        Command::Git { command } => on_heap(|| run_native_git(&repository, command)).await?,
        Command::Tree { command } => match command {
            TreeCommand::List { key } => on_heap(|| tree_list(&repository, &key)).await?,
        },
        Command::Checkout { key, dir, no_root } => {
            let key = parse_directory_key(&key)?;
            on_heap(|| repository.checkout(&key, &dir)).await?;
            println!("checked out {key} to {}", dir.display());
            if !no_root {
                let name =
                    automatic_root(workspace.as_ref(), "auto/checkout", &canonical_source(&dir))?;
                repository
                    .mutation_session()
                    .await?
                    .publish(
                        Vec::new(),
                        vec![RootChange::Set {
                            name: name.clone(),
                            target: key,
                        }],
                    )
                    .await?;
                println!("root {name}");
            }
        }
        Command::Cat {
            key,
            verified,
            from: _,
        } => {
            on_heap(|| object_cat(&repository, &key, verified)).await?;
        }
        Command::Root { command } => {
            on_heap(|| generic_root(&repository, command, workspace.as_ref())).await?
        }
        Command::Gc { dry_run } => {
            if dry_run {
                let preview = on_heap(|| repository.preview_collection()).await?;
                println!(
                    "would remove {} object record(s), {} payload(s), {} chunk(s)",
                    preview.logical_objects, preview.payload_blobs, preview.chunks
                );
                return Ok(());
            }
            let outcome = on_heap(|| repository.collect()).await?;
            // A collection that pruned no logical record leaves the revision
            // alone, which is the ordinary outcome whenever the named roots
            // still cover everything stored. Physical payloads can still have
            // been reclaimed, so the counts below are reported either way.
            match outcome.revision {
                Some(revision) => print!("revision {revision}; "),
                None => print!("revision unchanged; "),
            }
            println!(
                "removed {} object record(s), {} payload(s), {} chunk(s)",
                outcome.removed.logical_objects,
                outcome.removed.payload_blobs,
                outcome.removed.chunks
            );
            println!(
                "traversal-spill-files-opened {}; traversal-spill-peak-bytes {}",
                outcome.spill.files_opened, outcome.spill.peak_bytes
            );
        }
        Command::Vacuum => {
            let outcome = on_heap(|| repository.vacuum()).await?;
            match outcome.revision {
                Some(revision) => print!("revision {revision}; "),
                None => print!("revision unchanged; "),
            }
            println!(
                "removed {} object record(s), {} payload(s), {} chunk(s); physically reclaimed deferred pack garbage",
                outcome.removed.logical_objects,
                outcome.removed.payload_blobs,
                outcome.removed.chunks
            );
            println!(
                "traversal-spill-files-opened {}; traversal-spill-peak-bytes {}",
                outcome.spill.files_opened, outcome.spill.peak_bytes
            );
        }
        Command::Fsck(args) => on_heap(|| print_fsck(&repository, args, spill_limits)).await?,
        Command::Sync(_) | Command::Holds { .. } => {
            unreachable!("handled before local open")
        }
        Command::Ipc(_) => {
            unreachable!("handled before local open")
        }
        Command::Run(_) => unreachable!("handled before workspace scoping"),
        #[cfg(feature = "ssh")]
        Command::SshSource(_) => {
            unreachable!("handled before local open")
        }
    }

    print_pack_stats(repository.payloads());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_dispatch_future_is_one_stack_pointer() {
        let future = run(Cli {
            log_filter: None,
            log_format: LogFormat::Compact,
            repository: None,
            spill_memory_objects: None,
            spill_bytes: None,
            pack_target_bytes: None,
            pack_cache_bytes: None,

            command: Command::Init,
        });

        assert_eq!(std::mem::size_of_val(&future), std::mem::size_of::<usize>());
    }

    #[test]
    fn s3_urls_preserve_the_bucket_and_normalize_edge_slashes() {
        assert_eq!(
            s3_location("s3://bucket/releases/current").unwrap(),
            Some(S3Location {
                bucket: "bucket".into(),
                prefix: "releases/current".into(),
            })
        );
        assert_eq!(
            s3_location("s3://bucket/").unwrap(),
            Some(S3Location {
                bucket: "bucket".into(),
                prefix: String::new(),
            })
        );
        assert!(s3_location("s3://bucket?prefix=one").is_err());
        assert!(s3_location("s3://").is_err());
    }
}
