//! Stable-snapshot repository export into Casitar v1 streams.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::{
    CasitarError, CasitarFrameHeader, CasitarHeader, CasitarStats, CasitarStreamError,
    CasitarStreamLimits, CasitarWriter, PAYLOAD_FRAME_HEADER_BYTES, PROLOGUE_BYTES,
    RECORD_FRAME_PREFIX_BYTES,
};
use crate::blob::BlobStore;
use crate::metadata::MetadataStore;
use crate::repository::{ClosureStatus, Repository, RepositoryError, RetentionHold};
use crate::spill::FrozenSpillSet;
use crate::{BlobId, ObjectKey, RepositoryErrorCategory, RepositoryRevision, RootName};

/// One source graph selected for a Casitar export.
///
/// Named roots are resolved through the export's exact state snapshot. Exact
/// objects allow exporting a complete unrooted graph without changing source
/// retention policy. Duplicate selections do not duplicate archive frames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasitarExportTarget {
    /// Resolve this local mutable name to its exact snapshot value.
    NamedRoot(RootName),
    /// Export this exact object's complete forward closure.
    ExactObject(ObjectKey),
}

impl From<RootName> for CasitarExportTarget {
    fn from(value: RootName) -> Self {
        Self::NamedRoot(value)
    }
}

impl From<ObjectKey> for CasitarExportTarget {
    fn from(value: ObjectKey) -> Self {
        Self::ExactObject(value)
    }
}

/// Exact source facts and structural counts from a completed export.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CasitarExportReport {
    /// Logical revision held for root resolution, verification, and streaming.
    pub source_revision: RepositoryRevision,
    /// Canonical exact roots written into the archive header.
    pub roots: Vec<ObjectKey>,
    /// Requested local names and their values in `source_revision`.
    pub named_roots: BTreeMap<RootName, ObjectKey>,
    /// Exact completed stream statistics.
    pub stats: CasitarStats,
}

/// Final publication policy for a staged filesystem archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasitarExportFilePolicy {
    /// Atomically publish only if no filesystem entry already exists.
    CreateNew,
    /// Atomically replace an existing filesystem entry when the platform allows it.
    Replace,
}

/// Stable-snapshot traversal or output failure during repository export.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CasitarExportError {
    /// A requested local name was absent from the held source snapshot.
    #[error("Casitar export root `{0}` is absent")]
    RootAbsent(RootName),
    /// A requested exact graph was missing, invalid, or unsupported.
    #[error("cannot export Casitar root {root}: closure is {status:?}")]
    UnreadableRoot {
        /// Exact requested root.
        root: ObjectKey,
        /// Deterministic verification result from the held snapshot.
        status: ClosureStatus,
    },
    /// One immutable snapshot record disappeared or changed while held.
    #[error("Casitar export snapshot is inconsistent at {0}")]
    InconsistentSnapshot(ObjectKey),
    /// Two records made inconsistent size claims for one content identity.
    #[error(
        "Casitar export records disagree on payload {payload}: sizes {first_size} and {second_size}"
    )]
    InconsistentPayloadSize {
        /// Shared physical content identity.
        payload: BlobId,
        /// First size observed in canonical record order.
        first_size: u64,
        /// Conflicting size observed later.
        second_size: u64,
    },
    /// A filesystem output path has no usable final filename.
    #[error("invalid Casitar output path: {}", .0.display())]
    InvalidOutputPath(PathBuf),
    /// Repository state, format verification, or physical storage failed.
    #[error(transparent)]
    Repository(#[from] RepositoryError),
    /// Casitar framing, bounds, or stream I/O failed.
    #[error(transparent)]
    Stream(#[from] CasitarStreamError),
    /// Creating, syncing, or atomically publishing a filesystem output failed.
    #[error("Casitar filesystem output failed: {0}")]
    Io(#[from] io::Error),
}

impl CasitarExportError {
    /// Stable frontend category for this export failure.
    pub fn category(&self) -> RepositoryErrorCategory {
        use RepositoryErrorCategory as Category;

        match self {
            Self::RootAbsent(_) => Category::Absent,
            Self::UnreadableRoot { status, .. } => match status {
                ClosureStatus::Missing { .. } => Category::Absent,
                ClosureStatus::Invalid { .. } | ClosureStatus::Complete { .. } => {
                    Category::InvalidData
                }
                ClosureStatus::Unsupported { .. } => Category::Unsupported,
            },
            Self::InconsistentSnapshot(_) | Self::InconsistentPayloadSize { .. } => {
                Category::Corrupt
            }
            Self::InvalidOutputPath(_) => Category::InvalidInput,
            Self::Repository(error) => error.category(),
            Self::Stream(error) => error.category(),
            Self::Io(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                Category::DestinationConflict
            }
            Self::Io(_) => Category::Backend,
        }
    }
}

/// Keys read per page from a frozen plan set.
const PLAN_PAGE: usize = 256;

/// An export plan that keeps only counts in memory.
///
/// The selected closure and the payloads it needs live in spillable sets, so
/// planning an archive larger than memory is bounded by the traversal limits
/// rather than by the size of the graph being exported.
#[derive(Debug)]
struct ExportPlan {
    revision: RepositoryRevision,
    header: CasitarHeader,
    named_roots: BTreeMap<RootName, ObjectKey>,
    closure: FrozenSpillSet<ObjectKey>,
    payloads: FrozenSpillSet<PlannedPayload>,
    /// Distinct payloads, after collapsing the records that share one.
    distinct_payloads: usize,
}

/// One record's payload as the plan sees it.
///
/// Ordering is payload first, so every entry naming one payload is adjacent in
/// canonical order: the first is its representative, and a size disagreement is
/// visible by comparing neighbours rather than by keeping a map.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PlannedPayload {
    payload: BlobId,
    size: u64,
    representative: ObjectKey,
}

impl crate::spill::SpillKey for PlannedPayload {
    fn encode_spill(&self) -> Vec<u8> {
        // Fixed-width digest and size prefixes make the byte order identical to
        // the field order above.
        let mut encoded = Vec::new();
        encoded.extend_from_slice(self.payload.digest().as_bytes());
        encoded.extend_from_slice(&self.size.to_be_bytes());
        encoded.extend_from_slice(&self.representative.encode());
        encoded
    }

    fn decode_spill(bytes: &[u8]) -> Result<Self, crate::error::Error> {
        let (payload, rest) = bytes
            .split_at_checked(32)
            .ok_or_else(|| crate::error::Error::from("spilled payload plan is truncated"))?;
        let (size, representative) = rest
            .split_at_checked(8)
            .ok_or_else(|| crate::error::Error::from("spilled payload plan is truncated"))?;
        let payload: [u8; 32] = payload.try_into().expect("split at 32 bytes");
        let size: [u8; 8] = size.try_into().expect("split at 8 bytes");
        Ok(Self {
            payload: BlobId::new(crate::Digest::from(payload)),
            size: u64::from_be_bytes(size),
            representative: ObjectKey::decode(representative).map_err(|error| {
                crate::error::Error::from(format!("spilled payload plan: {error}"))
            })?,
        })
    }
}

impl<PS, SS> Repository<PS, SS>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    /// Export selected complete graphs to one caller-owned asynchronous stream.
    ///
    /// Root names are resolved and every requested closure is verified before
    /// `output` is touched. All observations and physical reads use one
    /// [`RetentionHold`], so concurrent root changes cannot mix revisions and
    /// collection cannot remove an unrooted selected object mid-stream.
    ///
    /// The returned writer has been flushed, but durability or mutation of
    /// a filesystem destination remains the caller's responsibility. Use
    /// [`export_casitar_file`](Self::export_casitar_file) for a staged file.
    #[tracing::instrument(name = "casitar.export", skip_all)]
    pub async fn export_casitar<W, I, T>(
        &self,
        targets: I,
        output: W,
        limits: CasitarStreamLimits,
    ) -> Result<(W, CasitarExportReport), CasitarExportError>
    where
        W: AsyncWrite + Unpin,
        I: IntoIterator<Item = T>,
        T: Into<CasitarExportTarget>,
    {
        let mut hold = self.retention_hold().await?;
        let plan = build_plan(self, &mut hold, targets, limits).await?;
        write_plan(&hold, plan, output, limits).await
    }

    /// Export selected complete graphs to a temporary sibling and atomically
    /// publish the completed file at `destination`. Success is reported only
    /// after the file and its directory have been flushed to storage.
    ///
    /// A failure or cancelled future removes the temporary file and never
    /// exposes a partial archive at `destination`. This compatibility method
    /// replaces an existing destination; use
    /// [`export_casitar_file_with_policy`](Self::export_casitar_file_with_policy)
    /// for atomic no-clobber publication.
    #[tracing::instrument(name = "casitar.export_file", skip_all)]
    pub async fn export_casitar_file<I, T>(
        &self,
        targets: I,
        destination: impl AsRef<Path>,
        limits: CasitarStreamLimits,
    ) -> Result<CasitarExportReport, CasitarExportError>
    where
        I: IntoIterator<Item = T>,
        T: Into<CasitarExportTarget>,
    {
        self.export_casitar_file_with_policy(
            targets,
            destination,
            limits,
            CasitarExportFilePolicy::Replace,
        )
        .await
    }

    /// Export to a staged sibling and publish it using an explicit final-file policy.
    ///
    /// [`CasitarExportFilePolicy::CreateNew`] performs an atomic no-clobber
    /// publication, including against a destination created concurrently while
    /// the archive is being written.
    #[tracing::instrument(name = "casitar.export_file_with_policy", skip_all, fields(?policy))]
    pub async fn export_casitar_file_with_policy<I, T>(
        &self,
        targets: I,
        destination: impl AsRef<Path>,
        limits: CasitarStreamLimits,
        policy: CasitarExportFilePolicy,
    ) -> Result<CasitarExportReport, CasitarExportError>
    where
        I: IntoIterator<Item = T>,
        T: Into<CasitarExportTarget>,
    {
        let staged = AtomicOutput::create(destination.as_ref(), policy).await?;
        let (staged, report) = self.export_casitar(targets, staged, limits).await?;
        staged.publish().await?;
        Ok(report)
    }
}

#[tracing::instrument(name = "casitar.build_export_plan", skip_all)]
async fn build_plan<PS, SS, I, T>(
    repository: &Repository<PS, SS>,
    hold: &mut RetentionHold<'_, PS, SS>,
    targets: I,
    limits: CasitarStreamLimits,
) -> Result<ExportPlan, CasitarExportError>
where
    PS: BlobStore,
    SS: MetadataStore,
    I: IntoIterator<Item = T>,
    T: Into<CasitarExportTarget>,
{
    limits.validate()?;
    let traversal_limit = repository.limits().max_traversal_objects;
    let revision = hold.snapshot().revision();
    let mut named_roots = BTreeMap::new();
    let mut roots = BTreeSet::new();
    let mut selections = 0usize;

    for target in targets {
        selections = selections.checked_add(1).ok_or_else(length_overflow)?;
        if selections > traversal_limit {
            return Err(RepositoryError::LimitExceeded(format!(
                "Casitar export selection exceeded {traversal_limit} targets"
            ))
            .into());
        }
        match target.into() {
            CasitarExportTarget::NamedRoot(name) => {
                let Some(root) = hold
                    .snapshot()
                    .root(&name)
                    .await
                    .map_err(RepositoryError::from)?
                else {
                    return Err(CasitarExportError::RootAbsent(name));
                };
                named_roots.insert(name, root.clone());
                roots.insert(root);
            }
            CasitarExportTarget::ExactObject(root) => {
                roots.insert(root);
            }
        }
    }

    hold.retain_only(roots.clone()).await?;
    let header =
        CasitarHeader::new(roots.iter().cloned().collect()).map_err(CasitarStreamError::from)?;
    // One spillable union across every selected root: each verification walks
    // its own closure and records what it verified, so planning an archive
    // larger than memory does not depend on holding the union in memory.
    let mut reachable = crate::spill::SpillSet::new(hold.spill_area(), "casitar-closure");
    for root in &roots {
        match hold.verify_closure_into(root, &mut reachable).await? {
            ClosureStatus::Complete { .. } => {
                if reachable.len() > traversal_limit {
                    return Err(RepositoryError::LimitExceeded(format!(
                        "Casitar export union closure exceeded {traversal_limit} objects"
                    ))
                    .into());
                }
            }
            status => {
                return Err(CasitarExportError::UnreadableRoot {
                    root: root.clone(),
                    status,
                });
            }
        }
    }

    // One entry per selected record, ordered by the payload it names, and
    // spilled like the closure itself.
    let mut payloads = crate::spill::SpillSet::new(hold.spill_area(), "casitar-payloads");
    let mut record_bytes = 0u64;
    let mut records = 0usize;
    let closure = reachable.freeze().await.map_err(RepositoryError::Payload)?;
    let mut after = None;
    loop {
        let page = closure
            .page(after, PLAN_PAGE)
            .await
            .map_err(RepositoryError::Payload)?;
        let Some(last) = page.last().cloned() else {
            break;
        };
        for key in page {
            let Some(record) = hold.object(&key).await? else {
                return Err(CasitarExportError::InconsistentSnapshot(key));
            };
            let frame = CasitarFrameHeader::Record(record.clone())
                .encode()
                .map_err(CasitarStreamError::from)?;
            let bytes = frame
                .len()
                .checked_sub(RECORD_FRAME_PREFIX_BYTES)
                .ok_or_else(length_overflow)?;
            check_limit(
                "record bytes",
                usize_u64(bytes)?,
                usize_u64(limits.max_record_bytes)?,
            )?;
            record_bytes = record_bytes
                .checked_add(usize_u64(frame.len())?)
                .ok_or_else(length_overflow)?;
            records = records.checked_add(1).ok_or_else(length_overflow)?;
            payloads
                .insert(PlannedPayload {
                    payload: record.payload(),
                    size: record.payload_size(),
                    representative: key.clone(),
                })
                .await
                .map_err(RepositoryError::Payload)?;
        }
        after = Some(last);
    }

    let payloads = payloads.freeze().await.map_err(RepositoryError::Payload)?;
    let payload_totals = collapse_payloads(&payloads, limits).await?;
    validate_plan(&header, records, record_bytes, &payload_totals, limits)?;
    tracing::debug!(
        source_revision = %revision,
        root_count = roots.len(),
        records,
        distinct_payloads = payload_totals.distinct,
        "Casitar export plan built"
    );
    Ok(ExportPlan {
        revision,
        header,
        named_roots,
        closure,
        payloads,
        distinct_payloads: payload_totals.distinct,
    })
}

/// What the payload entries add up to once records sharing a payload collapse.
struct PayloadTotals {
    distinct: usize,
    bytes: u64,
    frames: u64,
}

/// Walk the payload entries in canonical order, collapsing the records that
/// share one payload and rejecting a payload whose size two records disagree
/// about.
async fn collapse_payloads(
    payloads: &FrozenSpillSet<PlannedPayload>,
    limits: CasitarStreamLimits,
) -> Result<PayloadTotals, CasitarExportError> {
    let mut totals = PayloadTotals {
        distinct: 0,
        bytes: 0,
        frames: 0,
    };
    let mut previous: Option<PlannedPayload> = None;
    let mut after = None;
    loop {
        let page = payloads
            .page(after, PLAN_PAGE)
            .await
            .map_err(RepositoryError::Payload)?;
        let Some(last) = page.last().cloned() else {
            break;
        };
        for entry in page {
            if let Some(previous) = &previous
                && previous.payload == entry.payload
            {
                if previous.size != entry.size {
                    return Err(CasitarExportError::InconsistentPayloadSize {
                        payload: entry.payload,
                        first_size: previous.size,
                        second_size: entry.size,
                    });
                }
                continue;
            }
            check_limit("payload bytes", entry.size, limits.max_payload_bytes)?;
            totals.distinct = totals.distinct.checked_add(1).ok_or_else(length_overflow)?;
            totals.bytes = totals
                .bytes
                .checked_add(entry.size)
                .ok_or_else(length_overflow)?;
            totals.frames = totals
                .frames
                .checked_add(PAYLOAD_FRAME_HEADER_BYTES as u64)
                .and_then(|value| value.checked_add(entry.size))
                .ok_or_else(length_overflow)?;
            previous = Some(entry);
        }
        after = Some(last);
    }
    Ok(totals)
}

#[tracing::instrument(name = "casitar.write_export", skip_all)]
async fn write_plan<PS, SS, W>(
    hold: &RetentionHold<'_, PS, SS>,
    plan: ExportPlan,
    output: W,
    limits: CasitarStreamLimits,
) -> Result<(W, CasitarExportReport), CasitarExportError>
where
    PS: BlobStore,
    SS: MetadataStore,
    W: AsyncWrite + Unpin,
{
    let roots = plan.header.roots().to_vec();
    let mut writer = CasitarWriter::new(output, plan.header, limits).await?;

    // Payload frames first, in canonical payload order, one per distinct
    // payload: entries naming a payload already written are its duplicates.
    let mut written = 0usize;
    let mut previous: Option<BlobId> = None;
    let mut after = None;
    loop {
        let page = plan
            .payloads
            .page(after, PLAN_PAGE)
            .await
            .map_err(RepositoryError::Payload)?;
        let Some(last) = page.last().cloned() else {
            break;
        };
        for planned in page {
            if previous == Some(planned.payload) {
                continue;
            }
            previous = Some(planned.payload);
            let Some((record, mut reader)) = hold.open_payload(&planned.representative).await?
            else {
                return Err(CasitarExportError::InconsistentSnapshot(
                    planned.representative,
                ));
            };
            if record.payload() != planned.payload || record.payload_size() != planned.size {
                return Err(CasitarExportError::InconsistentSnapshot(
                    record.key().clone(),
                ));
            }
            writer
                .write_payload(planned.payload, planned.size, &mut reader)
                .await?;
            written += 1;
        }
        after = Some(last);
    }
    if written != plan.distinct_payloads {
        return Err(RepositoryError::LimitExceeded(format!(
            "Casitar export planned {} payloads but wrote {written}",
            plan.distinct_payloads
        ))
        .into());
    }

    // Record frames follow, in canonical key order, read back from the same
    // hold that planned them.
    let mut after = None;
    loop {
        let page = plan
            .closure
            .page(after, PLAN_PAGE)
            .await
            .map_err(RepositoryError::Payload)?;
        let Some(last) = page.last().cloned() else {
            break;
        };
        for key in page {
            let Some(record) = hold.object(&key).await? else {
                return Err(CasitarExportError::InconsistentSnapshot(key));
            };
            writer.write_record(&record).await?;
        }
        after = Some(last);
    }

    let (output, stats) = writer.finish().await?;
    tracing::info!(
        source_revision = %plan.revision,
        roots = roots.len(),
        payloads = stats.payloads,
        payload_bytes = stats.payload_bytes,
        records = stats.records,
        archive_bytes = stats.archive_bytes,
        "Casitar export completed"
    );
    Ok((
        output,
        CasitarExportReport {
            source_revision: plan.revision,
            roots,
            named_roots: plan.named_roots,
            stats,
        },
    ))
}

/// Check the whole archive against its limits before any byte is written.
///
/// Per-record and per-payload bounds are checked as the plan is built; this
/// checks what only the totals can answer.
fn validate_plan(
    header: &CasitarHeader,
    records: usize,
    record_bytes: u64,
    payloads: &PayloadTotals,
    limits: CasitarStreamLimits,
) -> Result<(), CasitarExportError> {
    let header_bytes = usize_u64(header.encode().len())?;
    let header_body = header_bytes
        .checked_sub(PROLOGUE_BYTES as u64)
        .ok_or_else(length_overflow)?;
    check_limit(
        "header bytes",
        header_body,
        usize_u64(limits.max_header_bytes)?,
    )?;
    check_limit(
        "payload count",
        usize_u64(payloads.distinct)?,
        usize_u64(limits.max_payloads)?,
    )?;
    check_limit(
        "record count",
        usize_u64(records)?,
        usize_u64(limits.max_records)?,
    )?;
    check_limit(
        "total payload bytes",
        payloads.bytes,
        limits.max_total_payload_bytes,
    )?;

    let archive_bytes = header_bytes
        .checked_add(payloads.frames)
        .and_then(|value| value.checked_add(record_bytes))
        // The terminating frame tag.
        .and_then(|value| value.checked_add(1))
        .ok_or_else(length_overflow)?;
    check_limit("archive bytes", archive_bytes, limits.max_archive_bytes)
}

fn check_limit(field: &'static str, actual: u64, limit: u64) -> Result<(), CasitarExportError> {
    if actual > limit {
        Err(CasitarStreamError::Limit {
            field,
            actual,
            limit,
        }
        .into())
    } else {
        Ok(())
    }
}

fn usize_u64(value: usize) -> Result<u64, CasitarExportError> {
    u64::try_from(value).map_err(|_| length_overflow())
}

fn length_overflow() -> CasitarExportError {
    CasitarStreamError::Format(CasitarError::LengthOverflow).into()
}

static NEXT_TEMP_FILE: AtomicU64 = AtomicU64::new(0);

struct AtomicOutput {
    file: Option<tokio::fs::File>,
    temporary: PathBuf,
    destination: PathBuf,
    policy: CasitarExportFilePolicy,
    published: bool,
}

impl AtomicOutput {
    async fn create(
        destination: &Path,
        policy: CasitarExportFilePolicy,
    ) -> Result<Self, CasitarExportError> {
        let Some(file_name) = destination.file_name() else {
            return Err(CasitarExportError::InvalidOutputPath(
                destination.to_path_buf(),
            ));
        };
        let parent = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));

        for _ in 0..128 {
            let sequence = NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed);
            let mut temporary_name = OsString::from(".");
            temporary_name.push(file_name);
            temporary_name.push(format!(".casitar-tmp-{}-{sequence}", std::process::id()));
            let temporary = parent.join(temporary_name);
            match tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .await
            {
                Ok(file) => {
                    return Ok(Self {
                        file: Some(file),
                        temporary,
                        destination: destination.to_path_buf(),
                        policy,
                        published: false,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate a unique sibling Casitar temporary file",
        )
        .into())
    }

    async fn publish(mut self) -> Result<(), CasitarExportError> {
        let mut file = self
            .file
            .take()
            .expect("an unpublished Casitar output owns its temporary file");
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        match self.policy {
            CasitarExportFilePolicy::CreateNew => {
                std::fs::hard_link(&self.temporary, &self.destination)?;
                // The final name is now complete and visible. Temporary-link
                // cleanup cannot safely turn that success back into a reported
                // failure, so let `Drop` retry if the first unlink fails.
                if std::fs::remove_file(&self.temporary).is_ok() {
                    self.published = true;
                }
            }
            CasitarExportFilePolicy::Replace => {
                std::fs::rename(&self.temporary, &self.destination)?;
                self.published = true;
            }
        }
        // Flush the new name, and the removed temporary name, before
        // reporting success. The temporary was created in that directory.
        let directory = self
            .temporary
            .parent()
            .expect("the Casitar temporary file has a parent directory");
        crate::blob::sync_directory(directory)?;
        Ok(())
    }
}

impl AsyncWrite for AtomicOutput {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(
            self.file
                .as_mut()
                .expect("an unpublished Casitar output owns its temporary file"),
        )
        .poll_write(context, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(
            self.file
                .as_mut()
                .expect("an unpublished Casitar output owns its temporary file"),
        )
        .poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(
            self.file
                .as_mut()
                .expect("an unpublished Casitar output owns its temporary file"),
        )
        .poll_shutdown(context)
    }
}

impl Drop for AtomicOutput {
    fn drop(&mut self) {
        if !self.published {
            drop(self.file.take());
            let _ = std::fs::remove_file(&self.temporary);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use tokio::sync::Semaphore;

    use super::*;
    use crate::metadata::{CommitResult, MetadataError, MetadataMutation, MetadataSnapshot};
    use crate::{
        CasitarReadFrame, CasitarReader, Digest, Directory, MemoryBlobStore, MemoryMetadataStore,
        Node, ObjectRecord, PathComponent, RootChange, RootRecord, SpillLimits,
    };

    #[derive(Clone, Debug, Default)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl SharedWriter {
        fn bytes(&self) -> Vec<u8> {
            self.0.lock().unwrap().clone()
        }
    }

    impl AsyncWrite for SharedWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.0.lock().unwrap().extend_from_slice(buffer);
            Poll::Ready(Ok(buffer.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    async fn graph_repository() -> (
        Repository<MemoryBlobStore, MemoryMetadataStore>,
        RootName,
        ObjectKey,
        RootName,
        ObjectKey,
    ) {
        let repository = Repository::memory().unwrap();
        let mutation = repository.mutation_session().await.unwrap();
        let child_bytes = b"shared child";
        let child = mutation.stage_blob(child_bytes).await.unwrap();
        let child_payload = child.record().payload();

        let first_directory = Directory::try_from_iter([(
            PathComponent::try_from("shared").unwrap(),
            Node::File {
                digest: child_payload,
                size: child_bytes.len() as u64,
                executable: false,
            },
        )])
        .unwrap();
        let second_directory = Directory::try_from_iter([(
            PathComponent::try_from("also-shared").unwrap(),
            Node::File {
                digest: child_payload,
                size: child_bytes.len() as u64,
                executable: false,
            },
        )])
        .unwrap();
        let first = mutation.stage_directory(&first_directory).await.unwrap();
        let second = mutation.stage_directory(&second_directory).await.unwrap();
        let first_key = first.record().key().clone();
        let second_key = second.record().key().clone();
        let first_name = RootName::try_from("exports/first").unwrap();
        let second_name = RootName::try_from("exports/second").unwrap();
        mutation
            .publish(
                vec![child, first, second],
                vec![
                    RootChange::Set {
                        name: first_name.clone(),
                        target: first_key.clone(),
                    },
                    RootChange::Set {
                        name: second_name.clone(),
                        target: second_key.clone(),
                    },
                ],
            )
            .await
            .unwrap();
        drop(mutation);
        (repository, first_name, first_key, second_name, second_key)
    }

    #[tokio::test]
    async fn exports_canonical_union_once_from_one_snapshot() {
        let (repository, first_name, first_key, second_name, second_key) = graph_repository().await;
        let output = SharedWriter::default();
        let observer = output.clone();
        let (_, report) = repository
            .export_casitar(
                vec![
                    CasitarExportTarget::NamedRoot(second_name.clone()),
                    CasitarExportTarget::ExactObject(first_key.clone()),
                    CasitarExportTarget::NamedRoot(first_name.clone()),
                ],
                output,
                CasitarStreamLimits::default(),
            )
            .await
            .unwrap();

        let mut expected_roots = vec![first_key.clone(), second_key.clone()];
        expected_roots.sort();
        assert_eq!(report.roots, expected_roots);
        assert_eq!(report.named_roots[&first_name], first_key);
        assert_eq!(report.named_roots[&second_name], second_key);
        assert_eq!(report.stats.payloads, 3);
        assert_eq!(report.stats.records, 3);
        assert_eq!(report.stats.archive_bytes as usize, observer.bytes().len());

        let mut reader = CasitarReader::open(
            std::io::Cursor::new(observer.bytes()),
            CasitarStreamLimits::default(),
        )
        .await
        .unwrap();
        assert_eq!(reader.header().roots(), report.roots);
        let mut saw_record = false;
        let mut payloads = BTreeSet::new();
        let mut records = Vec::new();
        while let Some(frame) = reader.next_frame().await.unwrap() {
            match frame {
                CasitarReadFrame::Payload { payload, .. } => {
                    assert!(!saw_record, "payload appeared after a logical record");
                    assert!(payloads.insert(payload));
                    reader
                        .read_payload_to(&mut tokio::io::sink())
                        .await
                        .unwrap();
                }
                CasitarReadFrame::Record(record) => {
                    saw_record = true;
                    records.push(record.key().clone());
                }
            }
        }
        assert_eq!(payloads.len(), 3);
        assert!(records.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[tokio::test]
    async fn export_bytes_are_stable_across_selection_order_and_spilling() {
        let (repository, first_name, first_key, second_name, second_key) = graph_repository().await;

        let baseline = SharedWriter::default();
        let baseline_bytes = baseline.clone();
        repository
            .export_casitar(
                vec![
                    CasitarExportTarget::NamedRoot(second_name.clone()),
                    CasitarExportTarget::ExactObject(first_key.clone()),
                    CasitarExportTarget::NamedRoot(first_name.clone()),
                ],
                baseline,
                CasitarStreamLimits::default(),
            )
            .await
            .unwrap();

        let forced_spill = repository.clone().with_spill_limits(SpillLimits {
            max_memory_objects: 1,
            max_spill_bytes: 64 * 1024 * 1024,
        });
        let reordered = SharedWriter::default();
        let reordered_bytes = reordered.clone();
        forced_spill
            .export_casitar(
                vec![
                    CasitarExportTarget::ExactObject(second_key),
                    CasitarExportTarget::NamedRoot(first_name),
                    CasitarExportTarget::ExactObject(first_key),
                    CasitarExportTarget::NamedRoot(second_name),
                ],
                reordered,
                CasitarStreamLimits::default(),
            )
            .await
            .unwrap();

        assert_eq!(baseline_bytes.bytes(), reordered_bytes.bytes());
    }

    #[tokio::test]
    async fn preflight_failures_do_not_touch_stream_output() {
        let (repository, _, first_key, _, _) = graph_repository().await;

        let absent_output = SharedWriter::default();
        let absent_observer = absent_output.clone();
        let error = repository
            .export_casitar(
                [CasitarExportTarget::NamedRoot(
                    RootName::try_from("exports/absent").unwrap(),
                )],
                absent_output,
                CasitarStreamLimits::default(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, CasitarExportError::RootAbsent(_)));
        assert!(absent_observer.bytes().is_empty());

        let limited_output = SharedWriter::default();
        let limited_observer = limited_output.clone();
        let error = repository
            .export_casitar(
                [CasitarExportTarget::ExactObject(first_key)],
                limited_output,
                CasitarStreamLimits {
                    max_records: 0,
                    ..CasitarStreamLimits::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            CasitarExportError::Stream(CasitarStreamError::Limit {
                field: "record count",
                ..
            })
        ));
        assert!(limited_observer.bytes().is_empty());

        let missing = ObjectKey::blob(BlobId::new(Digest::hash(b"not present")));
        let closure_output = SharedWriter::default();
        let closure_observer = closure_output.clone();
        let error = repository
            .export_casitar(
                [CasitarExportTarget::ExactObject(missing)],
                closure_output,
                CasitarStreamLimits::default(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            CasitarExportError::UnreadableRoot {
                status: ClosureStatus::Missing { .. },
                ..
            }
        ));
        assert!(closure_observer.bytes().is_empty());
    }

    #[tokio::test]
    async fn filesystem_export_publishes_only_a_complete_archive() {
        let (repository, first_name, _, _, _) = graph_repository().await;
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("release.casitar");
        let report = repository
            .export_casitar_file(
                [CasitarExportTarget::NamedRoot(first_name.clone())],
                &destination,
                CasitarStreamLimits::default(),
            )
            .await
            .unwrap();
        let bytes = std::fs::read(&destination).unwrap();
        assert_eq!(bytes.len(), report.stats.archive_bytes as usize);

        std::fs::write(&destination, b"previous complete result").unwrap();
        let error = repository
            .export_casitar_file(
                [CasitarExportTarget::NamedRoot(first_name)],
                &destination,
                CasitarStreamLimits {
                    max_records: 0,
                    ..CasitarStreamLimits::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(error, CasitarExportError::Stream(_)));
        assert_eq!(
            std::fs::read(&destination).unwrap(),
            b"previous complete result"
        );
        assert_no_temporary_files(directory.path());
    }

    #[tokio::test]
    async fn filesystem_export_create_new_never_clobbers() {
        let (repository, first_name, _, _, _) = graph_repository().await;
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("release.casitar");
        std::fs::write(&destination, b"existing archive").unwrap();

        let error = repository
            .export_casitar_file_with_policy(
                [CasitarExportTarget::NamedRoot(first_name)],
                &destination,
                CasitarStreamLimits::default(),
                CasitarExportFilePolicy::CreateNew,
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            CasitarExportError::Io(ref error)
                if error.kind() == std::io::ErrorKind::AlreadyExists
        ));
        assert_eq!(std::fs::read(&destination).unwrap(), b"existing archive");
        assert_no_temporary_files(directory.path());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn filesystem_export_flushes_the_published_directory_entry() {
        let (repository, first_name, _, _, _) = graph_repository().await;
        let temporary = tempfile::tempdir().unwrap();
        for policy in [
            CasitarExportFilePolicy::Replace,
            CasitarExportFilePolicy::CreateNew,
        ] {
            let directory = temporary.path().join(format!("{policy:?}"));
            std::fs::create_dir(&directory).unwrap();
            crate::blob::SYNCED_DIRECTORIES.with_borrow_mut(|record| *record = Some(Vec::new()));
            repository
                .export_casitar_file_with_policy(
                    [CasitarExportTarget::NamedRoot(first_name.clone())],
                    directory.join("release.casitar"),
                    CasitarStreamLimits::default(),
                    policy,
                )
                .await
                .unwrap();
            let synced =
                crate::blob::SYNCED_DIRECTORIES.with_borrow_mut(|record| record.take().unwrap());
            assert!(
                synced.contains(&directory),
                "{policy:?} export must flush {} before success: {synced:?}",
                directory.display()
            );
            assert_no_temporary_files(&directory);
        }
    }

    #[derive(Clone)]
    struct BlockingMetadataStore {
        inner: MemoryMetadataStore,
        control: Arc<BlockingControl>,
    }

    struct BlockingControl {
        block_objects: std::sync::atomic::AtomicBool,
        entered: Semaphore,
    }

    struct BlockingSnapshot {
        inner: Arc<dyn MetadataSnapshot>,
        control: Arc<BlockingControl>,
    }

    #[async_trait]
    impl MetadataSnapshot for BlockingSnapshot {
        fn generation(&self) -> Result<u64, MetadataError> {
            self.inner.generation()
        }
        fn objects_created_through(
            &self,
            generation: u64,
        ) -> futures::stream::BoxStream<'static, Result<ObjectRecord, MetadataError>> {
            self.inner.objects_created_through(generation)
        }

        fn revision(&self) -> RepositoryRevision {
            self.inner.revision()
        }

        fn retention_resources(&self) -> std::collections::BTreeSet<crate::metadata::PinResource> {
            self.inner.retention_resources()
        }

        async fn object(&self, key: &ObjectKey) -> Result<Option<ObjectRecord>, MetadataError> {
            if self
                .control
                .block_objects
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                self.control.entered.add_permits(1);
                std::future::pending::<()>().await;
            }
            self.inner.object(key).await
        }

        async fn root(&self, name: &RootName) -> Result<Option<ObjectKey>, MetadataError> {
            self.inner.root(name).await
        }

        fn objects(&self) -> BoxStream<'static, Result<ObjectRecord, MetadataError>> {
            self.inner.objects()
        }

        fn roots(&self) -> BoxStream<'static, Result<RootRecord, MetadataError>> {
            self.inner.roots()
        }
    }

    #[async_trait]
    impl MetadataStore for BlockingMetadataStore {
        async fn try_collection_lease(
            &self,
        ) -> Result<Option<crate::metadata::RepositoryLease>, crate::metadata::MetadataError>
        {
            self.inner.try_collection_lease().await
        }
        fn coordinates_payload_catalog(&self) -> bool {
            self.inner.coordinates_payload_catalog()
        }
        async fn pin_store(
            &self,
        ) -> Result<std::sync::Arc<dyn crate::metadata::PinStore>, crate::metadata::MetadataError>
        {
            self.inner.pin_store().await
        }

        async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
            Ok(Arc::new(BlockingSnapshot {
                inner: self.inner.snapshot().await?,
                control: self.control.clone(),
            }))
        }

        async fn commit(
            &self,
            expected: &RepositoryRevision,
            mutation: MetadataMutation,
        ) -> Result<CommitResult, MetadataError> {
            self.inner.commit(expected, mutation).await
        }
    }

    #[tokio::test]
    async fn cancelled_filesystem_export_removes_its_temporary_file() {
        let control = Arc::new(BlockingControl {
            block_objects: std::sync::atomic::AtomicBool::new(false),
            entered: Semaphore::new(0),
        });
        let repository = Arc::new(Repository::new(
            MemoryBlobStore::new(),
            BlockingMetadataStore {
                inner: MemoryMetadataStore::new().unwrap(),
                control: control.clone(),
            },
        ));
        let mutation = repository.mutation_session().await.unwrap();
        let object = mutation.stage_blob(b"interrupt me").await.unwrap();
        let key = object.record().key().clone();
        let garbage = mutation
            .stage_blob(b"unrelated export garbage")
            .await
            .unwrap();
        mutation
            .publish_unrooted(vec![object, garbage])
            .await
            .unwrap();
        drop(mutation);

        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("cancelled.casitar");
        control
            .block_objects
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let task_repository = repository.clone();
        let task_destination = destination.clone();
        let task = tokio::spawn(async move {
            task_repository
                .export_casitar_file(
                    [CasitarExportTarget::ExactObject(key)],
                    task_destination,
                    CasitarStreamLimits::default(),
                )
                .await
        });
        let permit = control.entered.acquire().await.unwrap();
        permit.forget();
        // The export is already suspended in its record lookup. Let the
        // collector read metadata while the export's retention pin stays live.
        control
            .block_objects
            .store(false, std::sync::atomic::Ordering::SeqCst);
        crate::flush_repository_leases().await.unwrap();
        assert_eq!(
            repository
                .try_collect()
                .await
                .unwrap()
                .removed
                .logical_objects,
            1
        );
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        crate::flush_repository_leases().await.unwrap();
        assert_eq!(
            repository
                .try_collect()
                .await
                .unwrap()
                .removed
                .logical_objects,
            1
        );
        assert!(!destination.exists());
        assert_no_temporary_files(directory.path());
    }

    fn assert_no_temporary_files(directory: &Path) {
        let entries: Vec<_> = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert!(
            entries
                .iter()
                .all(|name| !name.to_string_lossy().contains(".casitar-tmp-")),
            "temporary Casitar output remained: {entries:?}"
        );
    }
}
