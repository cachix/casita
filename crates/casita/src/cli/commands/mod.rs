//! CLI repository setup, workspace scoping, and command dispatch.

use std::path::{Path, PathBuf};

use casita::experimental::MetadataStore as _;
use casita::experimental::{BlobId, DirectoryId, ObjectKey, RootChange, RootName};
#[cfg(feature = "git")]
use casita::import::Importer as _;
use tokio::io::AsyncWriteExt;

#[cfg(test)]
use super::LogFormat;
use super::{ArchiveCommand, Cli, Command, Error, ObjectCommand, TreeCommand, usage_error};

mod archive;
mod git;
mod imports;
mod maintenance;
mod naming;
mod objects;
mod roots;
mod run;
mod stats;
mod transfer;
mod workspace;

use archive::{archive_create, archive_import, archive_inspect, archive_verify, import_casitar};
use git::run_native_git;
#[cfg(feature = "git")]
use imports::automatic_git_view;
pub(super) use imports::detect_importer;
#[cfg(feature = "oci")]
use imports::import_oci;
use imports::import_tar;
use maintenance::print_fsck;
use naming::{canonical_source, nested_name};
use objects::{object_cat, object_show, tree_list};
use roots::generic_root;
use stats::print_pack_stats;
#[cfg(feature = "ssh")]
use transfer::serve_ssh_source;
use transfer::{generic_sync, list_holds, open_sync_source};
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
}
