//! Application workflows over built-in storage profiles.

use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::TryStreamExt;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};

use crate::blob::{BlobGc, BlobReader};
use crate::metadata::{MetadataStore, RootChange};
use crate::repository::{
    OwnedRetentionHold, Repository as CoreRepository, RepositoryError, RootRetention,
};
use crate::sync::TransferError;
use crate::{
    ErrorKind, IntegrityDisposition, IntegrityIssue, ObjectKey, ObjectRecord, RepositoryRevision,
    RetryDisposition, RootName, RootRecord,
};

use crate::metadata::{MetadataError, MetadataMutation, MetadataSnapshot};
use crate::{
    MetadataChange, MetadataCheck, MetadataCommitResult, MetadataCursor, MetadataKey, MetadataPage,
};

/// A consistent view of object records, roots, and application metadata. Holding this reader
/// keeps one metadata revision stable, but does not retain payloads.
/// Release readers when finished so old metadata files and WAL pages can be reclaimed.
/// Use [`Repository::retained_reader`] before looking up metadata or roots that
/// select content to open while collection may run.
#[derive(Clone)]
pub struct MetadataReader {
    snapshot: Arc<dyn MetadataSnapshot>,
    _pin: Option<crate::metadata::DataPinLease>,
}

impl MetadataReader {
    /// Look up an immutable object record at this reader's revision.
    /// The record remains readable even if GC retires its payload.
    pub async fn object(&self, key: &ObjectKey) -> Result<Option<ObjectRecord>, Error> {
        self.snapshot.object(key).await.app()
    }

    /// List named roots from this reader's revision, in name order.
    pub async fn roots(&self) -> Result<Vec<RootRecord>, Error> {
        self.snapshot.roots().try_collect().await.app()
    }

    /// List one named root and its descendants at this reader's revision.
    pub async fn roots_under(&self, prefix: &RootName) -> Result<Vec<RootRecord>, Error> {
        self.snapshot.roots_under(prefix).try_collect().await.app()
    }

    /// Read a GC root at the same revision as this reader's application records.
    pub async fn root(&self, name: &RootName) -> Result<Option<ObjectKey>, Error> {
        self.snapshot.root(name).await.app()
    }
    /// Revision observed by every operation on this reader.
    pub fn revision(&self) -> RepositoryRevision {
        self.snapshot.revision()
    }

    /// Get up to 4096 keys in input order. Duplicates are preserved and missing
    /// keys return `None`. Empty values remain distinct from missing values.
    /// Results above 16 MiB are rejected; split those batches into smaller reads.
    /// Backends without application records return [`ErrorKind::Unsupported`].
    pub async fn get(&self, keys: &[MetadataKey]) -> Result<Vec<Option<bytes::Bytes>>, Error> {
        self.snapshot.get(keys).await.app()
    }

    /// Scan a raw byte prefix in one namespace. Limit must be 1..=1024;
    /// pages also stop at 16 MiB of key/value bytes and return a continuation.
    /// A cursor belongs to this prefix and revision; use the same reader for
    /// stable pagination during concurrent commits. No startup index rebuild
    /// or scan of preceding/unrelated records is required.
    /// Backends without application records return [`ErrorKind::Unsupported`].
    pub async fn scan(
        &self,
        prefix: &MetadataKey,
        cursor: Option<&MetadataCursor>,
        limit: usize,
    ) -> Result<MetadataPage, Error> {
        scan_snapshot(self.snapshot.as_ref(), prefix, cursor, limit).await
    }
}

async fn scan_snapshot(
    snapshot: &dyn MetadataSnapshot,
    prefix: &MetadataKey,
    cursor: Option<&MetadataCursor>,
    limit: usize,
) -> Result<MetadataPage, Error> {
    if let Some(cursor) = cursor
        && (cursor.revision != snapshot.revision() || &cursor.prefix != prefix)
    {
        return Err(MetadataError::InvalidMetadata(
            "cursor belongs to a different prefix or revision".into(),
        )
        .into_application_error());
    }
    let mut records = snapshot
        .scan(prefix, cursor.map(|c| c.after.as_ref()), limit)
        .await
        .app()?;
    let length = crate::metadata::records::page_len(&records, limit);
    let more = records.len() > length;
    records.truncate(length);
    let cursor = if more {
        Some(MetadataCursor {
            revision: snapshot.revision(),
            prefix: prefix.clone(),
            after: records.last().expect("positive page limit").key.key.clone(),
        })
    } else {
        None
    };
    Ok(MetadataPage { records, cursor })
}

/// One stable metadata snapshot with collection protection acquired before
/// any roots or application records are read. Unlike [`MetadataReader`], this
/// session retains the snapshot's content until it and all payload readers
/// opened from it are dropped. Local sessions share one process-owned pin;
/// process death releases ownership safely. Remote backends use durable pins.
/// Release it promptly after the read operation.
#[derive(Clone)]
pub struct RetainedReader {
    pub(crate) hold: Arc<BuiltinRetentionHold>,
}

impl RetainedReader {
    /// Open a sequential payload reader that authenticates bytes before use.
    /// Writes generate the required proof metadata before publication. Missing
    /// or corrupt proofs fail verification; reads never fall back to EOF only.
    pub async fn open_verified(&self, key: &ObjectKey) -> Result<Option<VerifiedReader>, Error> {
        let Some(record) = self.hold.object(key).await.app()? else {
            return Ok(None);
        };
        let opened = self
            .hold
            .repository()
            .payloads()
            .open_verified(&record.payload(), record.payload_size())
            .await;
        let inner = match opened {
            Ok(Some(inner)) => inner,
            result => {
                // Only missing or unauthenticated bytes say anything about
                // stored content; a busy or throttled backend does not.
                let damaged = match &result {
                    Ok(_) => true,
                    Err(error) => crate::blob::is_damaged_payload_error(error),
                };
                if damaged && let Some(store) = &self.hold.repository().nar_store {
                    store.record_read_failure().await;
                }
                return Err(match result {
                    Err(error) => RepositoryError::Payload(error),
                    _ => RepositoryError::MissingPayload(record.payload()),
                }
                .into_application_error());
            }
        };
        Ok(Some(VerifiedReader {
            record,
            inner,
            _hold: Some(self.hold.clone()),
            nar_health: crate::nar::store::ReadHealth::new(
                self.hold.repository().nar_store.clone(),
            ),
        }))
    }
    pub(crate) fn new(hold: BuiltinRetentionHold) -> Self {
        Self {
            hold: Arc::new(hold),
        }
    }

    /// List roots from the same protected snapshot used for content opening.
    pub async fn roots(&self) -> Result<Vec<RootRecord>, Error> {
        self.hold.snapshot().roots().try_collect().await.app()
    }

    /// List one named root and its descendants from this protected snapshot.
    pub async fn roots_under(&self, prefix: &RootName) -> Result<Vec<RootRecord>, Error> {
        self.hold
            .snapshot()
            .roots_under(prefix)
            .try_collect()
            .await
            .app()
    }

    /// Revision shared by metadata, root, and content lookups.
    pub fn revision(&self) -> RepositoryRevision {
        self.hold.snapshot().revision()
    }

    /// Read a root whose snapshot content remains protected while this session
    /// is alive, even if another writer removes or replaces the root.
    pub async fn root(&self, name: &RootName) -> Result<Option<ObjectKey>, Error> {
        self.hold.snapshot().root(name).await.app()
    }

    /// Get application records in input order with the same bounds as
    /// [`MetadataReader::get`]. Supported on local and in-memory repositories.
    pub async fn get(&self, keys: &[MetadataKey]) -> Result<Vec<Option<bytes::Bytes>>, Error> {
        self.hold.snapshot().get(keys).await.app()
    }

    /// Scan application records with the same ordering, cursor semantics, and
    /// row/byte bounds as [`MetadataReader::scan`]. Local and in-memory only.
    pub async fn scan(
        &self,
        prefix: &MetadataKey,
        cursor: Option<&MetadataCursor>,
        limit: usize,
    ) -> Result<MetadataPage, Error> {
        scan_snapshot(self.hold.snapshot(), prefix, cursor, limit).await
    }

    /// Look up an immutable object record in this protected snapshot.
    pub async fn object(&self, key: &ObjectKey) -> Result<Option<ObjectRecord>, Error> {
        self.hold.object(key).await.app()
    }

    /// Open content from this exact snapshot. The returned reader keeps the
    /// protection alive even after the session and repository are dropped.
    /// Keys absent from the snapshot return `None`; opaque records do not
    /// guarantee that any application-referenced content exists.
    pub async fn open(&self, key: &ObjectKey) -> Result<Option<Reader>, Error> {
        let Some((record, inner)) = self.hold.open_payload(key).await.app()? else {
            return Ok(None);
        };
        Ok(Some(Reader {
            record,
            inner,
            _hold: Some(self.hold.clone()),
            nar_health: crate::nar::store::ReadHealth::new(
                self.hold.repository().nar_store.clone(),
            ),
        }))
    }
}

type BuiltinRepository = CoreRepository<Arc<dyn BlobGc>, Arc<dyn MetadataStore>>;
type BuiltinRetentionHold = OwnedRetentionHold<Arc<dyn BlobGc>, Arc<dyn MetadataStore>>;

/// A failure of an application repository operation.
///
/// Use [`Self::kind`] and [`Self::retry_disposition`] for programmatic handling.
/// The underlying diagnostic is preserved through [`std::error::Error::source`]
/// without making backend error types part of the application contract.
#[derive(Debug, thiserror::Error)]
#[error("{source}")]
pub struct Error {
    kind: ErrorKind,
    retry: RetryDisposition,
    #[source]
    source: Box<dyn std::error::Error + Send + Sync>,
}

impl Error {
    /// Stable failure category.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Whether and when repeating the operation may succeed.
    pub fn retry_disposition(&self) -> RetryDisposition {
        self.retry
    }

    pub(crate) fn classified(
        kind: ErrorKind,
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        let mut retry = match kind {
            ErrorKind::Absent
            | ErrorKind::InvalidInput
            | ErrorKind::InvalidData
            | ErrorKind::ImmutableConflict
            | ErrorKind::DestinationConflict
            | ErrorKind::Unsupported
            | ErrorKind::Corrupt => RetryDisposition::Never,
            ErrorKind::Busy | ErrorKind::StaleRevision => RetryDisposition::Retry,
            _ => RetryDisposition::Unknown,
        };
        let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        while let Some(current) = cause {
            if let Some(repository) = current.downcast_ref::<RepositoryError>() {
                retry = repository.retry_disposition();
                break;
            }
            if let Some(payload) = current.downcast_ref::<crate::error::Error>() {
                retry = payload.retry_disposition();
                break;
            }
            cause = current.source();
        }
        Self {
            kind,
            retry,
            source: Box::new(error),
        }
    }
}

// Keep conversions from engine errors private so backend types do not leak
// through public From implementations on the application error.
trait ApplicationError {
    fn into_application_error(self) -> Error;
}

impl ApplicationError for RepositoryError {
    fn into_application_error(self) -> Error {
        Error::classified(self.category(), self)
    }
}

impl ApplicationError for crate::metadata::MetadataError {
    fn into_application_error(self) -> Error {
        RepositoryError::Metadata(self).into_application_error()
    }
}

impl ApplicationError for TransferError {
    fn into_application_error(self) -> Error {
        Error::classified(self.category(), self)
    }
}

trait ApplicationResult<T> {
    fn app(self) -> Result<T, Error>;
}

impl<T, E: ApplicationError> ApplicationResult<T> for Result<T, E> {
    fn app(self) -> Result<T, Error> {
        self.map_err(ApplicationError::into_application_error)
    }
}

/// Counts selected by a collection preview or removed by collection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CollectionReport {
    /// Unreachable logical records.
    pub logical_objects: usize,
    /// Unreferenced physical payloads.
    pub payload_blobs: usize,
    /// Unreferenced physical chunks.
    pub chunks: usize,
}

impl CollectionReport {
    fn from_preview(value: crate::repository::CollectionPreview) -> Self {
        Self {
            logical_objects: value.logical_objects,
            payload_blobs: value.payload_blobs,
            chunks: value.chunks,
        }
    }
}

/// Integrity findings from one repository revision.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct IntegrityReport {
    /// Revision inspected.
    pub revision: RepositoryRevision,
    /// Logical records inspected.
    pub objects_checked: usize,
    /// Named roots inspected.
    pub roots_checked: usize,
    /// Distinct payloads inspected.
    pub payloads_checked: usize,
    /// Ordered diagnostic findings.
    pub issues: Vec<IntegrityIssue>,
}

impl IntegrityReport {
    /// Whether no reachable corruption was found. Unchecked objects can still
    /// produce findings; use [`Self::is_clean`] to require a complete clean audit.
    pub fn is_healthy(&self) -> bool {
        !self
            .issues
            .iter()
            .any(|issue| issue.disposition == IntegrityDisposition::Corrupt)
    }

    /// Whether inspection completed without any findings.
    pub fn is_clean(&self) -> bool {
        self.issues.is_empty()
    }
}

/// A seekable payload reader with collection protection.
///
/// Implements Tokio's [`AsyncRead`] and [`AsyncSeek`]. Ordinary opens retain the
/// selected object's closure and required physical data; retained-session opens
/// keep their snapshot-wide protection. Both survive repository handle drop.
pub struct Reader {
    record: ObjectRecord,
    inner: Box<dyn BlobReader>,
    // Drop the physical reader before releasing the collection hold.
    _hold: Option<Arc<BuiltinRetentionHold>>,
    nar_health: crate::nar::store::ReadHealth,
}

/// Sequential reader that authenticates every byte before returning it.
/// Successful EOF also authenticates the complete payload length. The reader
/// retains the selected graph until dropped. Retained-session opens keep the
/// session’s snapshot protection. Both survive repository handle drop.
pub struct VerifiedReader {
    record: ObjectRecord,
    inner: Box<dyn crate::blob::BlobStreamReader>,
    _hold: Option<Arc<BuiltinRetentionHold>>,
    nar_health: crate::nar::store::ReadHealth,
}

impl VerifiedReader {
    /// The immutable record selected when this reader was opened.
    pub fn record(&self) -> &ObjectRecord {
        &self.record
    }
}

impl AsyncRead for VerifiedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(result) = self.nar_health.poll(cx) {
            return result;
        }
        match Pin::new(&mut *self.inner).poll_read(cx, buffer) {
            Poll::Ready(Err(error)) => {
                self.nar_health.failed(error);
                self.nar_health.poll(cx).expect("queued invalidation")
            }
            other => other,
        }
    }
}

impl Reader {
    /// Release prefetch reservations before caching an idle reader. The current
    /// position, decoded cache and protection against collection are retained.
    pub async fn park(&mut self) {
        self.inner.park().await;
    }

    /// Verified logical metadata associated with this payload at open time.
    pub fn record(&self) -> &ObjectRecord {
        &self.record
    }
}

impl AsyncRead for Reader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(result) = self.nar_health.poll(cx) {
            return result;
        }
        match Pin::new(&mut *self.inner).poll_read(cx, buffer) {
            Poll::Ready(Err(error)) => {
                self.nar_health.failed(error);
                self.nar_health.poll(cx).expect("queued invalidation")
            }
            other => other,
        }
    }
}

impl AsyncSeek for Reader {
    fn start_seek(mut self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        // An error queued for the abandoned position must not surface at the
        // new one; its invalidation, if any, already runs on its own task.
        self.nar_health.reset();
        Pin::new(&mut *self.inner).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Pin::new(&mut *self.inner).poll_complete(cx)
    }
}

/// A verified repository using built-in storage and object formats.
///
/// Cloning shares the repository's coordination and caches. Operations publish
/// verified objects and named roots atomically; readers retain their payloads
/// against concurrent collection. Custom composition and detailed resource
/// tuning live in `casita::experimental` behind the `experimental` feature.
#[derive(Clone)]
pub struct Repository {
    pub(crate) inner: BuiltinRepository,
}

impl Repository {
    /// Create a new raw blob by replacing bytes at `offset` without changing
    /// its length, then atomically create or replace `name`. The original blob
    /// remains immutable. Chunked storage reuses untouched payload chunks.
    pub async fn overwrite_blob(
        &self,
        key: &ObjectKey,
        offset: u64,
        replacement: &[u8],
        name: RootName,
    ) -> Result<ObjectKey, Error> {
        let session = self.inner.mutation_session().await.app()?;
        let staged = session
            .stage_blob_overwrite(key, offset, replacement)
            .await
            .app()?;
        let updated = staged.record().key().clone();
        session
            .publish_rooted(vec![staged], name, updated.clone())
            .await
            .app()?;
        Ok(updated)
    }
    /// Open content with authentication before every byte is returned.
    /// New chunked writes retain the required Bao metadata automatically.
    pub async fn open_verified(&self, key: &ObjectKey) -> Result<Option<VerifiedReader>, Error> {
        let result = self.inner.open_object_verified(key).await;
        let damaged = match &result {
            Err(RepositoryError::MissingPayload(_)) => true,
            Err(RepositoryError::Payload(error)) => crate::blob::is_damaged_payload_error(error),
            _ => false,
        };
        if damaged && let Some(store) = &self.inner.nar_store {
            store.record_read_failure().await;
        }
        Ok(result.app()?.map(|(record, inner)| VerifiedReader {
            record,
            inner,
            _hold: None,
            nar_health: crate::nar::store::ReadHealth::new(self.inner.nar_store.clone()),
        }))
    }
    /// Open persistent local storage, including cross-process coordination.
    pub async fn local(path: impl AsRef<Path>) -> Result<Self, Error> {
        Ok(Self {
            inner: CoreRepository::local(path).await.app()?.into_builtin(),
        })
    }

    /// Create an ephemeral repository.
    pub fn memory() -> Result<Self, Error> {
        Ok(Self {
            inner: CoreRepository::memory().app()?.into_builtin(),
        })
    }

    /// Open shared S3 storage using the standard AWS credential chain.
    ///
    /// All participants use the same bucket and prefix and distinct diagnostic
    /// writer names. The S3 storage profile remains experimental. Call
    /// [`Self::flush`] after dropping active readers and before runtime shutdown.
    #[cfg(feature = "s3")]
    pub async fn s3(
        bucket: impl AsRef<str>,
        prefix: impl AsRef<str>,
        writer: impl Into<String>,
    ) -> Result<Self, Error> {
        Ok(Self {
            inner: CoreRepository::s3(bucket, prefix, writer)
                .await
                .app()?
                .into_builtin(),
        })
    }

    /// Flush payload writes, await dropped leases, and compact transient metadata.
    /// Drop readers before the final flush: a live SQLite snapshot can make
    /// compaction report [`ErrorKind::Busy`]. Retry after releasing it, and await
    /// the final flush before shutting down the async runtime.
    pub async fn flush(&self) -> Result<(), Error> {
        self.inner.flush().await.app()
    }

    /// Import an external input through its format-specific request.
    ///
    /// Format-specific repository entry points are intentionally private:
    ///
    /// ```compile_fail
    /// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let repository = casita::Repository::memory()?;
    /// repository.import_path("./project", "project".try_into()?).await?;
    /// # Ok(())
    /// # }
    /// ```
    /// Raw bytes use `BlobImport` through the same entry point:
    ///
    /// ```compile_fail
    /// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let repository = casita::Repository::memory()?;
    /// repository.put_blob(&b"bytes"[..], "blob".try_into()?).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn import<I: crate::import::Importer<Self>>(
        &self,
        input: I,
    ) -> Result<I::Report, I::Error> {
        input.import(self).await
    }

    /// Restore a filesystem graph into an empty or absent destination directory.
    pub async fn checkout(&self, root: &ObjectKey, target: impl AsRef<Path>) -> Result<(), Error> {
        self.inner.checkout(root, target).await.app()
    }

    /// Open a consistent metadata snapshot on any built-in backend.
    /// Use this reader when multiple lookups or scan pages must share one revision.
    /// For a single application-record batch, use [`Self::get`].
    /// Object records, roots, and revision are always available; application-record get/scan
    /// require backend support. This does not pin object payloads.
    pub async fn metadata_reader(&self) -> Result<MetadataReader, Error> {
        let (snapshot, pin) = crate::metadata::read_snapshot(self.inner.metadata())
            .await
            .app()?;
        Ok(MetadataReader {
            snapshot,
            _pin: pin,
        })
    }

    /// Acquire collection protection and one stable snapshot before reading
    /// metadata or roots. Use this session for both lookup and content opening
    /// to avoid a gap in which a concurrent root removal and GC could retire
    /// the selected content. Root/content reads work on every built-in backend;
    /// application record reads require local or in-memory storage.
    pub async fn retained_reader(&self) -> Result<RetainedReader, Error> {
        Ok(RetainedReader::new(
            self.inner.owned_read_hold().await.app()?,
        ))
    }

    /// Read namespaced application records from one consistent snapshot.
    /// The snapshot lasts only for this call; separate calls may see different
    /// revisions. Use [`Self::metadata_reader`] to share a snapshot across calls.
    pub async fn get(&self, keys: &[MetadataKey]) -> Result<Vec<Option<bytes::Bytes>>, Error> {
        if !self.inner.metadata().supports_metadata_records() {
            return Err(MetadataError::UnsupportedMetadata.into_application_error());
        }
        self.inner.metadata().get_records(keys).await.app()
    }

    /// Scan one ordered prefix page. If a commit occurs between calls, a cursor
    /// from an older revision is rejected. Use `metadata_reader` for stable
    /// pagination while writers are active.
    pub async fn scan(
        &self,
        prefix: &MetadataKey,
        cursor: Option<&MetadataCursor>,
        limit: usize,
    ) -> Result<MetadataPage, Error> {
        self.metadata_reader()
            .await?
            .scan(prefix, cursor, limit)
            .await
    }

    /// Atomically insert, replace or delete application records and GC roots,
    /// conditional on current expected values. Checks are evaluated in input
    /// order and a conflict changes nothing. Unrelated writes do not cause
    /// false value conflicts. Values are opaque and do not retain objects.
    ///
    /// Limits: 4096 checks/changes, 4 KiB keys, 1 MiB values, 16 MiB record input
    /// bytes. Root targets must already exist and have complete verified graphs.
    /// For new content, import under a staging root then atomically replace
    /// that root and register its indexes here. Root-only commits also work on
    /// S3; application records require a local or in-memory repository.
    pub async fn commit(
        &self,
        checks: Vec<MetadataCheck>,
        changes: Vec<MetadataChange>,
    ) -> Result<MetadataCommitResult, Error> {
        let mut mutation =
            MetadataMutation::with_metadata(checks.clone(), changes.clone()).app()?;
        if !self.inner.metadata().supports_metadata_records() {
            if checks
                .iter()
                .any(|check| matches!(check, MetadataCheck::Record { .. }))
                || changes.iter().any(|change| {
                    matches!(
                        change,
                        MetadataChange::Set { .. } | MetadataChange::Delete { .. }
                    )
                })
            {
                return Err(MetadataError::UnsupportedMetadata.into_application_error());
            }
            return self
                .commit_roots(&checks, mutation.take_root_changes())
                .await;
        }
        // Fast root changes require a persisted verification witness AND a
        // current root of the same target, checked under the state write lock.
        // That root (or the pin history of its publisher) protects the graph
        // from a concurrent collector's frozen victim set, even during ENOSPC
        // recovery, which can delete unrooted bytes before logical pruning.
        // A rootless target must instead pass ordinary pinned admission.
        mutation.require_validated_roots = true;
        let result = match self.inner.metadata().commit_checked(mutation).await {
            Err(MetadataError::RootVerificationRequired) => {
                self.inner
                    .mutation_session()
                    .await
                    .app()?
                    .publish_with_metadata(Vec::new(), checks, changes)
                    .await
            }
            other => other.map_err(RepositoryError::Metadata),
        };
        match result {
            Ok(result) => Ok(MetadataCommitResult::Committed {
                revision: result.revision,
            }),
            Err(RepositoryError::Metadata(MetadataError::CheckFailed { index })) => {
                Ok(MetadataCommitResult::Conflict { check_index: index })
            }
            Err(error) => Err(error.into_application_error()),
        }
    }

    // Backends without application records retain their existing durable root
    // publication protocol. Check in caller order, then publish at precisely
    // that revision; an unrelated writer retries the entire check/read pair.
    async fn commit_roots(
        &self,
        checks: &[MetadataCheck],
        changes: Vec<RootChange>,
    ) -> Result<MetadataCommitResult, Error> {
        let session = self.inner.mutation_session().await.app()?;
        loop {
            let snapshot = self.inner.metadata().snapshot().await.app()?;
            let revision = snapshot.revision();
            for (check_index, check) in checks.iter().enumerate() {
                let MetadataCheck::Root { name, expected } = check else {
                    return Err(MetadataError::UnsupportedMetadata.into_application_error());
                };
                if snapshot.root(name).await.app()?.as_ref() != expected.as_ref() {
                    return Ok(MetadataCommitResult::Conflict { check_index });
                }
            }
            drop(snapshot);
            match session
                .publish_at_revision(revision, Vec::new(), changes.clone())
                .await
            {
                Ok(result) => {
                    return Ok(MetadataCommitResult::Committed {
                        revision: result.revision,
                    });
                }
                Err(RepositoryError::Metadata(MetadataError::StaleRevision { .. })) => continue,
                Err(error) => return Err(error.into_application_error()),
            }
        }
    }

    /// Read one root name at a stable revision; absence returns `None`.
    /// For lookup followed by content opening, use [`Self::retained_reader`]
    /// and perform both operations on that session to keep the snapshot alive.
    pub async fn root(&self, name: &RootName) -> Result<Option<ObjectKey>, Error> {
        self.metadata_reader().await?.root(name).await
    }

    /// List named roots from one stable metadata revision without retaining content.
    /// Use [`Self::retained_reader`] to keep selected content alive for later reads.
    pub async fn roots(&self) -> Result<Vec<RootRecord>, Error> {
        self.metadata_reader().await?.roots().await
    }

    /// List one named root and its descendants at a stable revision.
    pub async fn roots_under(&self, prefix: &RootName) -> Result<Vec<RootRecord>, Error> {
        self.metadata_reader().await?.roots_under(prefix).await
    }

    /// Unconditionally create or replace a name after verifying the target graph.
    /// Use [`Self::compare_and_set_root`] to preserve concurrent replacements.
    pub async fn set_root(&self, name: RootName, target: ObjectKey) -> Result<(), Error> {
        self.commit(Vec::new(), vec![MetadataChange::SetRoot { name, target }])
            .await?;
        Ok(())
    }

    /// Read a root's retention policy. Existing roots are permanent by default.
    pub async fn root_retention(&self, name: &RootName) -> Result<Option<RootRetention>, Error> {
        self.inner.root_retention(name).await.app()
    }

    /// Set a local root and its retention policy in one publication.
    pub async fn set_root_with_retention(
        &self,
        name: RootName,
        target: ObjectKey,
        retention: RootRetention,
    ) -> Result<(), Error> {
        self.inner
            .set_root_with_retention(name, target, retention)
            .await
            .app()
    }

    /// Change an existing local root's retention policy.
    pub async fn set_root_retention(
        &self,
        name: &RootName,
        retention: RootRetention,
    ) -> Result<(), Error> {
        self.inner.set_root_retention(name, retention).await.app()
    }

    /// Record a successful use of an evictable root for eviction order.
    pub async fn touch_root(&self, name: &RootName, target: &ObjectKey) -> Result<(), Error> {
        self.inner.touch_root(name, target).await.app()
    }

    /// Create or replace a name only while its current target matches `expected`.
    ///
    /// `None` requires an absent name; `Some(key)` requires that exact target.
    /// Returns `false` on a mismatch without changing the repository. The complete
    /// target graph must already exist and is verified before publication.
    /// Unrelated revision conflicts are retried internally. This compares the
    /// current value, not the history of changes to the name.
    ///
    /// Import under a staging name to retain a new graph, then use this method
    /// to conditionally promote it to a shared name.
    pub async fn compare_and_set_root(
        &self,
        name: RootName,
        expected: Option<&ObjectKey>,
        target: ObjectKey,
    ) -> Result<bool, Error> {
        let result = self
            .commit(
                vec![MetadataCheck::Root {
                    name: name.clone(),
                    expected: expected.cloned(),
                }],
                vec![MetadataChange::SetRoot { name, target }],
            )
            .await?;
        Ok(matches!(result, MetadataCommitResult::Committed { .. }))
    }

    /// Remove a name only if it still points at the expected object.
    /// Returns `false` if it is absent or was concurrently repointed.
    pub async fn remove_root(&self, name: &RootName, expected: &ObjectKey) -> Result<bool, Error> {
        let result = self
            .commit(
                vec![MetadataCheck::Root {
                    name: name.clone(),
                    expected: Some(expected.clone()),
                }],
                vec![MetadataChange::RemoveRoot { name: name.clone() }],
            )
            .await?;
        Ok(matches!(result, MetadataCommitResult::Committed { .. }))
    }

    /// Look up an immutable object record at one stable revision.
    pub async fn object(&self, key: &ObjectKey) -> Result<Option<ObjectRecord>, Error> {
        self.metadata_reader().await?.object(key).await
    }

    /// Open one object and retain its closure and required physical data until
    /// the reader is dropped. Unrelated garbage remains collectible. Packed
    /// storage may retain neighboring bytes sharing a required immutable pack.
    /// Use `retained_reader` when lookup and open must share one snapshot.
    pub async fn open(&self, key: &ObjectKey) -> Result<Option<Reader>, Error> {
        Ok(self
            .inner
            .open_object(key)
            .await
            .app()?
            .map(|(record, inner)| Reader {
                record,
                inner,
                _hold: None,
                nar_health: crate::nar::store::ReadHealth::new(self.inner.nar_store.clone()),
            }))
    }

    /// Export one complete object graph as a Casitar archive.
    /// Returns the flushed writer; filesystem durability remains the caller's responsibility.
    pub async fn export_casitar<W: AsyncWrite + Unpin>(
        &self,
        root: &ObjectKey,
        output: W,
    ) -> Result<W, Error> {
        let (output, _) = self
            .inner
            .export_casitar(
                [root.clone()],
                output,
                crate::CasitarStreamLimits::default(),
            )
            .await
            .map_err(|error| Error::classified(error.category(), error))?;
        Ok(output)
    }

    /// Count unreachable data without deleting it.
    pub async fn preview_collection(&self) -> Result<CollectionReport, Error> {
        Ok(CollectionReport::from_preview(
            self.inner.preview_collection().await.app()?,
        ))
    }

    /// Remove unreachable objects and reclaim physical data.
    pub async fn collect(&self) -> Result<CollectionReport, Error> {
        Ok(CollectionReport::from_preview(
            self.inner.collect().await.app()?.removed,
        ))
    }

    /// Collect only if collection can start immediately, otherwise return
    /// [`ErrorKind::Busy`] with [`RetryDisposition::Retry`] instead of waiting
    /// for another collector or a conflicting pin update. Intended for
    /// opportunistic housekeeping driven by an external scheduler.
    pub async fn try_collect(&self) -> Result<CollectionReport, Error> {
        Ok(CollectionReport::from_preview(
            self.inner.try_collect().await.app()?.removed,
        ))
    }

    /// Collect unreachable data and force deferred pack reclamation.
    pub async fn vacuum(&self) -> Result<CollectionReport, Error> {
        Ok(CollectionReport::from_preview(
            self.inner.vacuum().await.app()?.removed,
        ))
    }

    /// Audit repository integrity at one stable revision.
    pub async fn fsck(&self) -> Result<IntegrityReport, Error> {
        let report = self.inner.fsck().await.app()?;
        Ok(IntegrityReport {
            revision: report.revision,
            objects_checked: report.objects_checked,
            roots_checked: report.roots_checked,
            payloads_checked: report.payloads_checked,
            issues: report.issues,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BlobId, Digest, Directory, Node, PathComponent};

    #[tokio::test]
    async fn verified_read_and_overwrite_survive_collection_and_reopen() {
        use tokio::io::AsyncReadExt;
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::local(directory.path()).await.unwrap();
        let original = vec![19; 300_000];
        let name = RootName::try_from("file").unwrap();
        let old = repository
            .import(crate::import::BlobImport::new(
                original.as_slice(),
                name.clone(),
            ))
            .await
            .unwrap();
        let mut reader = repository.open_verified(&old).await.unwrap().unwrap();
        let mut prefix = [0; 1024];
        reader.read_exact(&mut prefix).await.unwrap();
        let new = repository
            .overwrite_blob(&old, 16_380, &[42; 300], name.clone())
            .await
            .unwrap();
        let mut expected = original.clone();
        expected[16_380..16_680].fill(42);
        assert_eq!(
            new,
            ObjectKey::blob(BlobId::new(blake3::hash(&expected).into()))
        );
        assert!(
            repository
                .overwrite_blob(&new, 300_000, &[1], name.clone())
                .await
                .is_err()
        );
        assert_eq!(repository.root(&name).await.unwrap(), Some(new.clone()));
        repository.collect().await.unwrap();
        let mut rest = Vec::new();
        reader.read_to_end(&mut rest).await.unwrap();
        assert_eq!(rest, original[1024..]);
        drop(reader);
        repository.flush().await.unwrap();
        drop(repository);
        let repository = Repository::local(directory.path()).await.unwrap();
        let mut reader = repository.open_verified(&new).await.unwrap().unwrap();
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.unwrap();
        assert_eq!(actual, expected);
        drop(reader);
        repository.collect().await.unwrap();
        assert!(repository.object(&old).await.unwrap().is_none());
        repository.flush().await.unwrap();
    }

    #[tokio::test]
    async fn conditional_root_rejects_a_present_directory_with_a_missing_child() {
        let repository = Repository::memory().unwrap();
        let name = RootName::try_from("current").unwrap();
        let original = repository
            .import(crate::import::BlobImport::new(
                &b"original"[..],
                name.clone(),
            ))
            .await
            .unwrap();
        let child = b"arrives later";
        let directory = Directory::try_from_iter([(
            PathComponent::try_from("child").unwrap(),
            Node::File {
                digest: BlobId::new(Digest::hash(child)),
                size: child.len() as u64,
                executable: false,
            },
        )])
        .unwrap();
        let target = ObjectKey::directory(directory.digest());
        // Only the engine can store an incomplete graph. Keep the session's
        // retention hold while checking the application publication boundary.
        let session = repository.inner.mutation_session().await.unwrap();
        let staged = session.stage_directory(&directory).await.unwrap();
        session.publish_unrooted(vec![staged]).await.unwrap();
        assert!(repository.object(&target).await.unwrap().is_some());
        assert!(
            repository
                .compare_and_set_root(name.clone(), Some(&original), target.clone())
                .await
                .is_err()
        );
        assert_eq!(
            repository.root(&name).await.unwrap(),
            Some(original.clone())
        );

        let staged_child = session.stage_blob(child).await.unwrap();
        session.publish_unrooted(vec![staged_child]).await.unwrap();
        assert!(
            repository
                .compare_and_set_root(name.clone(), Some(&original), target.clone())
                .await
                .unwrap()
        );
        assert_eq!(repository.root(&name).await.unwrap(), Some(target));
    }
}
