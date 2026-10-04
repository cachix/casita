//! Logical auditing and repository repair.

use super::Error;
use crate::cli::FsckArgs;

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

pub(super) async fn print_fsck(
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
