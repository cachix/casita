//! Casitar creation, inspection, verification, and import.

use std::path::Path;

use casita::experimental::{ObjectKey, RootName};
use casita::import::Importer as _;
use tokio::io::AsyncRead;

use super::{Error, scoped_root, usage_error, workspace::Workspace};
use crate::cli::{
    ArchiveCreateArgs, ArchiveImportArgs, ArchiveInspectArgs, ArchiveVerifyArgs, CasitarImportArgs,
};

pub(super) async fn open_archive_input(
    input: &str,
) -> Result<Box<dyn AsyncRead + Send + Unpin>, Error> {
    if input == "-" {
        Ok(Box::new(tokio::io::stdin()))
    } else {
        Ok(Box::new(tokio::fs::File::open(Path::new(input)).await?))
    }
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

pub(super) async fn archive_create<PS, SS>(
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

pub(super) async fn archive_inspect(args: ArchiveInspectArgs) -> Result<(), Error> {
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

pub(super) async fn archive_verify(
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

pub(super) async fn archive_import<PS, SS>(
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

pub(super) async fn import_casitar<PS, SS>(
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
