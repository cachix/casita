//! Repository errors and stable error classification.

use super::*;

/// Errors from repository orchestration rather than object validity.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RepositoryError {
    /// A requested generic repository item is absent.
    #[error("repository item is absent: {0}")]
    Absent(String),
    /// Physical payload bytes expected by a staged or committed record are
    /// absent.
    #[error("payload {0} is missing from physical storage")]
    MissingPayload(BlobId),
    /// A physical store returned bytes under the wrong content identity.
    #[error("payload store returned {actual} while opening {expected}")]
    PayloadIdentityMismatch {
        /// Address requested from storage.
        expected: BlobId,
        /// Digest observed by the format verifier.
        actual: BlobId,
    },
    /// A physical writer reported a byte count different from the source it
    /// consumed.
    #[error("payload writer returned size {actual}, expected {expected}")]
    PayloadSizeMismatch {
        /// Number of source bytes written.
        expected: u64,
        /// Number of bytes reported by the completed writer.
        actual: u64,
    },
    /// A requested root is not complete and valid.
    #[error("cannot publish root {root}: closure is {status:?}")]
    RootNotPublishable {
        /// Requested root target.
        root: ObjectKey,
        /// Exact closure result.
        status: ClosureStatus,
    },
    /// A requested read requires a complete valid closure.
    #[error("cannot read object {object}: closure is {status:?}")]
    ObjectNotReadable {
        /// Requested object.
        object: ObjectKey,
        /// Exact closure result.
        status: ClosureStatus,
    },
    /// Two staged values claim different records under one immutable key.
    #[error("staged immutable object conflict at {0}")]
    StagedConflict(ObjectKey),
    /// A staged value was verified against a different repository instance.
    #[error("staged object {0} belongs to a different repository")]
    ForeignStagedObject(ObjectKey),
    /// A caller request cannot be represented by the selected repository
    /// operation.
    #[error("invalid repository input: {0}")]
    InvalidInput(String),
    /// A bounded operation exceeded its configured deployment limit.
    #[error("repository limit exceeded: {0}")]
    LimitExceeded(String),
    /// A caller requested a nonblocking operation while its required
    /// ownership was held elsewhere.
    #[error("repository is busy: {0}")]
    Busy(String),
    /// An unheld best-effort read lost unrooted data to collection.
    #[error("object {0} was collected during a best-effort read")]
    CollectedDuringRead(ObjectKey),
    /// Format verification failed while staging a payload.
    #[error(transparent)]
    Format(#[from] FormatError),
    /// Revisioned state operation failed.
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    /// Physical payload storage failed.
    #[error(transparent)]
    Payload(crate::error::Error),
    /// Streaming a staged payload failed.
    #[error(transparent)]
    Io(std::io::Error),
}

/// A state-engine failure stays [`RepositoryError::Metadata`] whichever
/// payload or I/O layer carried it, such as a refused payload deletion or
/// opening the state database.
impl From<crate::error::Error> for RepositoryError {
    fn from(error: crate::error::Error) -> Self {
        match error {
            crate::error::Error::Backend(inner) => match inner.downcast::<MetadataError>() {
                Ok(error) => Self::Metadata(*error),
                Err(inner) => Self::Payload(crate::error::Error::Backend(inner)),
            },
            crate::error::Error::Io(error) => match take_metadata(error) {
                Ok(error) => Self::Metadata(error),
                Err(error) => Self::Payload(crate::error::Error::Io(error)),
            },
            error => Self::Payload(error),
        }
    }
}

impl From<std::io::Error> for RepositoryError {
    fn from(error: std::io::Error) -> Self {
        match take_metadata(error) {
            Ok(error) => Self::Metadata(error),
            Err(error) => Self::Io(error),
        }
    }
}

/// The state-engine failure an I/O error carries, or the I/O error itself.
fn take_metadata(error: std::io::Error) -> Result<MetadataError, std::io::Error> {
    if !error
        .get_ref()
        .is_some_and(|inner| inner.is::<MetadataError>())
    {
        return Err(error);
    }
    let kind = error.kind();
    match error.into_inner() {
        Some(inner) => inner
            .downcast::<MetadataError>()
            .map(|error| *error)
            .map_err(|inner| std::io::Error::new(kind, inner)),
        None => Err(kind.into()),
    }
}

/// Stable error categories shared by repository operations and frontends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RepositoryErrorCategory {
    /// A requested object, root, or payload is absent.
    Absent,
    /// A caller supplied a malformed or over-limit request.
    InvalidInput,
    /// Stored or supplied object data failed verification.
    InvalidData,
    /// One immutable key already names a different record.
    ImmutableConflict,
    /// The expected state revision is obsolete.
    StaleRevision,
    /// A filesystem destination is already occupied.
    DestinationConflict,
    /// Required ownership is held elsewhere.
    Busy,
    /// A namespace, format, or backend capability is unavailable.
    Unsupported,
    /// Committed state violates a repository invariant.
    Corrupt,
    /// Unrooted data disappeared during a best-effort read.
    CollectedDuringRead,
    /// An I/O, backend, or other operational failure occurred.
    Backend,
    /// This process lost its hold on the repository's durable state, for
    /// example because its database files were replaced underneath it.
    /// Nothing it does can be durable until it restarts.
    RestartRequired,
}

impl RepositoryErrorCategory {
    /// Stable machine-readable spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::InvalidInput => "invalid_input",
            Self::InvalidData => "invalid_data",
            Self::ImmutableConflict => "immutable_conflict",
            Self::StaleRevision => "stale_revision",
            Self::DestinationConflict => "destination_conflict",
            Self::Busy => "busy",
            Self::Unsupported => "unsupported",
            Self::Corrupt => "corrupt",
            Self::CollectedDuringRead => "collected_during_read",
            Self::Backend => "backend",
            Self::RestartRequired => "restart_required",
        }
    }
}

/// `error` and the errors that caused it. Transparent wrappers may omit their
/// immediate inner error from `source()`, and an `io::Error` reports the
/// error it carries only through `get_ref()`, so both boundaries are crossed
/// explicitly.
fn causes(error: &RepositoryError) -> impl Iterator<Item = &(dyn std::error::Error + 'static)> {
    let first: &(dyn std::error::Error + 'static) = match error {
        RepositoryError::Metadata(error) => error,
        RepositoryError::Io(error) | RepositoryError::Payload(crate::error::Error::Io(error)) => {
            error
        }
        RepositoryError::Payload(crate::error::Error::Backend(error)) => error.as_ref(),
        RepositoryError::Payload(error) => error,
        _ => error,
    };
    std::iter::successors(Some(first), |error| {
        match error.downcast_ref::<std::io::Error>() {
            Some(io) => io
                .get_ref()
                .map(|inner| inner as &(dyn std::error::Error + 'static)),
            None => error.source(),
        }
    })
}

pub(super) fn is_storage_full(error: &RepositoryError) -> bool {
    causes(error).any(|error| {
        matches!(
            error.downcast_ref::<MetadataError>(),
            Some(MetadataError::StorageFull)
        ) || error
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::StorageFull)
    })
}

/// The first state-engine failure behind `error`, such as a deletion that the
/// metadata store refused while it ran inside a payload operation.
fn metadata_cause(error: &RepositoryError) -> Option<&MetadataError> {
    causes(error).find_map(|error| error.downcast_ref::<MetadataError>())
}

fn metadata_category(error: &MetadataError) -> RepositoryErrorCategory {
    use RepositoryErrorCategory as Category;

    match error {
        MetadataError::Busy(_)
        | MetadataError::MaintenanceFenced
        | MetadataError::ForeignSqliteLock { .. } => Category::Busy,
        MetadataError::StorageFull => Category::Backend,
        MetadataError::UnsupportedMetadata => Category::Unsupported,
        MetadataError::InvalidMetadata(_) => Category::InvalidInput,
        MetadataError::RootVerificationRequired => Category::InvalidInput,
        MetadataError::CheckFailed { .. } => Category::DestinationConflict,
        MetadataError::StaleRevision { .. } => Category::StaleRevision,
        MetadataError::ImmutableConflict(_) => Category::ImmutableConflict,
        MetadataError::MissingObject { .. } => Category::Absent,
        MetadataError::InvalidRetainedSet(_) | MetadataError::MixedCollectionMutation => {
            Category::InvalidInput
        }
        MetadataError::Corruption(_) => Category::Corrupt,
        MetadataError::DatabaseReplaced { .. } => Category::RestartRequired,
        MetadataError::Poisoned
        | MetadataError::RevisionEntropy(_)
        | MetadataError::Transient(_)
        | MetadataError::Backend(_) => Category::Backend,
    }
}

impl RepositoryError {
    /// Classify this failure without parsing its display text.
    pub fn category(&self) -> RepositoryErrorCategory {
        use RepositoryErrorCategory as Category;

        match self {
            Self::Absent(_) => Category::Absent,
            Self::MissingPayload(_) => Category::Absent,
            Self::PayloadIdentityMismatch { .. } | Self::PayloadSizeMismatch { .. } => {
                Category::InvalidData
            }
            Self::RootNotPublishable { status, .. } => match status {
                ClosureStatus::Missing { .. } => Category::Absent,
                ClosureStatus::Invalid { .. } => Category::InvalidData,
                ClosureStatus::Unsupported { .. } => Category::Unsupported,
                ClosureStatus::Complete { .. } => Category::InvalidData,
            },
            Self::ObjectNotReadable { status, .. } => match status {
                ClosureStatus::Missing { .. } => Category::Absent,
                ClosureStatus::Invalid { .. } => Category::InvalidData,
                ClosureStatus::Unsupported { .. } => Category::Unsupported,
                ClosureStatus::Complete { .. } => Category::InvalidData,
            },
            Self::StagedConflict(_) => Category::ImmutableConflict,
            Self::ForeignStagedObject(_) | Self::InvalidInput(_) | Self::LimitExceeded(_) => {
                Category::InvalidInput
            }
            Self::Busy(_) => Category::Busy,
            Self::CollectedDuringRead(_) => Category::CollectedDuringRead,
            Self::Format(error) => match error {
                FormatError::MissingDirectLink { .. } => Category::Absent,
                FormatError::UnsupportedNamespace(_) => Category::Unsupported,
                FormatError::DuplicateNamespace(_) => Category::InvalidInput,
                FormatError::Io(_) => Category::Backend,
                _ => Category::InvalidData,
            },
            Self::Metadata(error) => metadata_category(error),
            Self::Payload(error) => match error {
                crate::error::Error::NotFound { .. } => Category::Absent,
                crate::error::Error::TargetNotEmpty { .. }
                | crate::error::Error::TargetNameConflict { .. } => Category::DestinationConflict,
                crate::error::Error::LimitExceeded(_) => Category::InvalidInput,
                crate::error::Error::Digest(_)
                | crate::error::Error::Directory(_)
                | crate::error::Error::PathComponent(_)
                | crate::error::Error::SymlinkTarget(_) => Category::InvalidData,
                crate::error::Error::Io(_)
                | crate::error::Error::Msg(_)
                | crate::error::Error::Transient(_)
                | crate::error::Error::Throttled { .. }
                | crate::error::Error::Backend(_) => self.operational_category(),
            },
            Self::Io(_) => self.operational_category(),
        }
    }

    /// An operational failure is a backend failure unless the state engine
    /// caused it, as when the metadata store refuses a payload deletion.
    fn operational_category(&self) -> RepositoryErrorCategory {
        metadata_cause(self).map_or(RepositoryErrorCategory::Backend, metadata_category)
    }

    /// Typed retry guidance propagated from coordination, state
    /// compare-and-swap, I/O, and payload backends.
    pub fn retry_disposition(&self) -> crate::RetryDisposition {
        use crate::RetryDisposition;

        match self {
            Self::Busy(_) => RetryDisposition::Retry,
            Self::Metadata(error) => error.retry_disposition(),
            Self::Payload(error) => error.retry_disposition(),
            Self::Io(error) | Self::Format(FormatError::Io(error)) => {
                crate::error::wrapped_retry_disposition(error)
            }
            Self::Absent(_)
            | Self::MissingPayload(_)
            | Self::PayloadIdentityMismatch { .. }
            | Self::PayloadSizeMismatch { .. }
            | Self::RootNotPublishable { .. }
            | Self::ObjectNotReadable { .. }
            | Self::StagedConflict(_)
            | Self::ForeignStagedObject(_)
            | Self::InvalidInput(_)
            | Self::LimitExceeded(_)
            | Self::CollectedDuringRead(_)
            | Self::Format(_) => RetryDisposition::Never,
        }
    }
}
