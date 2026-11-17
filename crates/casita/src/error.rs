//! Errors for the data model and physical payload operations.

use crate::digest::{DigestError, ObjectId};
use crate::path::{PathComponent, PathComponentError, SymlinkTargetError};

/// Machine-readable guidance for retrying an operation without parsing error
/// text. A retry never implies that a non-idempotent caller may blindly repeat
/// work; callers must still honor the operation's mutation contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RetryDisposition {
    /// Repeating the same request cannot resolve this failure.
    Never,
    /// The operation may be retried, normally with bounded backoff.
    Retry,
    /// The operation may be retried after at least this backend-supplied delay.
    RetryAfter(std::time::Duration),
    /// The backend did not provide enough typed information to decide.
    Unknown,
}

/// The error type shared by physical payload and filesystem operations.
///
/// Data-model errors ([`DigestError`], [`DirectoryError`],
/// [`PathComponentError`], [`SymlinkTargetError`]) and I/O errors convert into
/// it via `From`; backend-specific errors (Turso, object store, ...) are carried
/// type-erased in [`Error::Backend`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An I/O error from the filesystem or an object store.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// An object looked up by digest is not in physical payload storage.
    #[error("object {digest} not found in payload storage")]
    NotFound {
        /// The typed identifier that was looked up.
        digest: ObjectId,
    },
    /// The checkout target directory already has content.
    #[error("checkout target {} is not empty", path.display())]
    TargetNotEmpty {
        /// The offending target path.
        path: std::path::PathBuf,
    },
    /// A fresh checkout staging directory could not represent one stored name
    /// because the target filesystem treats it as an existing sibling.
    #[error(
        "cannot materialize {}: the target filesystem treats this name as an existing sibling",
        path.display()
    )]
    TargetNameConflict {
        /// Relative path whose final component collided under the target
        /// filesystem's own naming rules.
        path: std::path::PathBuf,
    },
    /// An invalid digest (wrong length or malformed textual form).
    #[error(transparent)]
    Digest(#[from] DigestError),
    /// An invalid directory (see [`DirectoryError`]).
    #[error(transparent)]
    Directory(#[from] DirectoryError),
    /// An invalid path component.
    #[error(transparent)]
    PathComponent(#[from] PathComponentError),
    /// An invalid symlink target.
    #[error(transparent)]
    SymlinkTarget(#[from] SymlinkTargetError),
    /// A free-form operational error.
    #[error("{0}")]
    Msg(String),
    /// A bounded operation exceeded a configured resource limit.
    #[error("limit exceeded: {0}")]
    LimitExceeded(String),
    /// A typed temporary backend or transport failure.
    #[error("transient backend failure: {0}")]
    Transient(Box<dyn std::error::Error + Send + Sync + 'static>),
    /// A backend explicitly rejected work due to current load.
    #[error("backend throttled the operation: {source}")]
    Throttled {
        /// Minimum delay advertised by the backend, if one was supplied.
        retry_after: Option<std::time::Duration>,
        /// Original typed backend failure.
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// A type-erased error from a backend or third-party library.
    #[error(transparent)]
    Backend(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl Error {
    /// Typed retry guidance for this physical operation failure.
    pub fn retry_disposition(&self) -> RetryDisposition {
        match self {
            Self::Transient(_) => RetryDisposition::Retry,
            Self::Throttled {
                retry_after: Some(delay),
                ..
            } => RetryDisposition::RetryAfter(*delay),
            Self::Throttled {
                retry_after: None, ..
            } => RetryDisposition::Retry,
            Self::Io(error) => wrapped_retry_disposition(error),
            Self::Backend(error) => wrapped_retry_disposition(error.as_ref()),
            Self::NotFound { .. }
            | Self::TargetNotEmpty { .. }
            | Self::TargetNameConflict { .. }
            | Self::Digest(_)
            | Self::Directory(_)
            | Self::PathComponent(_)
            | Self::SymlinkTarget(_)
            | Self::Msg(_)
            | Self::LimitExceeded(_) => RetryDisposition::Never,
        }
    }
}

/// Recover typed guidance through adapters such as AsyncWrite's I/O error and
/// type-erased backend errors. Unknown wrappers do not erase a known cause.
pub(crate) fn wrapped_retry_disposition(
    error: &(dyn std::error::Error + 'static),
) -> RetryDisposition {
    if let Some(error) = error.downcast_ref::<Error>() {
        return error.retry_disposition();
    }
    #[cfg(feature = "native")]
    {
        if let Some(error) = error.downcast_ref::<crate::repository::RepositoryError>() {
            return error.retry_disposition();
        }
        if let Some(error) = error.downcast_ref::<crate::metadata::MetadataError>() {
            return error.retry_disposition();
        }
    }
    if let Some(error) = error.downcast_ref::<std::io::Error>() {
        let guidance = io_retry_disposition(error.kind());
        if guidance != RetryDisposition::Unknown {
            return guidance;
        }
        // io::Error::source can skip the contained wrapper itself.
        if let Some(inner) = error.get_ref() {
            return wrapped_retry_disposition(inner);
        }
    }
    error
        .source()
        .map_or(RetryDisposition::Unknown, wrapped_retry_disposition)
}

pub(crate) fn io_retry_disposition(kind: std::io::ErrorKind) -> RetryDisposition {
    use std::io::ErrorKind;

    match kind {
        ErrorKind::Interrupted
        | ErrorKind::WouldBlock
        | ErrorKind::TimedOut
        | ErrorKind::ConnectionAborted
        | ErrorKind::ConnectionReset
        | ErrorKind::ConnectionRefused
        | ErrorKind::NotConnected
        | ErrorKind::BrokenPipe => RetryDisposition::Retry,
        _ => RetryDisposition::Unknown,
    }
}

impl From<String> for Error {
    fn from(msg: String) -> Self {
        Error::Msg(msg)
    }
}

impl From<&str> for Error {
    fn from(msg: &str) -> Self {
        Error::Msg(msg.to_string())
    }
}

/// A boxed error unwraps back to [`Error`] when it holds one (internals that
/// work type-erased, like the git importer, lose nothing at the boundary);
/// anything else becomes [`Error::Backend`].
impl From<Box<dyn std::error::Error + Send + Sync + 'static>> for Error {
    fn from(err: Box<dyn std::error::Error + Send + Sync + 'static>) -> Self {
        match err.downcast::<Error>() {
            Ok(err) => *err,
            Err(err) => Error::Backend(err),
        }
    }
}

/// `From` impls wrapping the third-party error types the crate handles with
/// `?` into [`Error::Backend`].
macro_rules! backend_errors {
    ($($ty:ty),* $(,)?) => {$(
        impl From<$ty> for Error {
            fn from(err: $ty) -> Self {
                Error::Backend(Box::new(err))
            }
        }
    )*};
}

backend_errors!(std::path::StripPrefixError, crate::encode::DecodeError);

#[cfg(feature = "native")]
backend_errors!(
    turso::Error,
    tokio::task::JoinError,
    object_store::Error,
    object_store::path::Error,
);

#[cfg(feature = "git")]
backend_errors!(
    gix::config::file::init::Error,
    gix::objs::decode::Error,
    gix::objs::find::existing::Error,
    gix::refspec::parse::Error,
    gix::object::find::existing::with_conversion::Error,
    gix::object::peel::to_kind::Error,
    gix::open::Error,
    gix::remote::find::existing::Error,
    gix::revision::spec::parse::single::Error,
);

/// Errors constructing or mutating a [`crate::Directory`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DirectoryError {
    /// Two entries share the same name.
    #[error("duplicate name in directory: {0}")]
    DuplicateName(PathComponent),
    /// The directory's recursive size would overflow `u64`.
    #[error("directory size overflows u64")]
    SizeOverflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "native")]
    #[test]
    fn retry_guidance_survives_import_error_wrappers() {
        use crate::{metadata::MetadataError, repository::RepositoryError};
        for (cause, expected) in [
            (
                MetadataError::Transient("pin ledger remained contended".into()),
                RetryDisposition::Retry,
            ),
            (
                MetadataError::Corruption("bad inventory".into()),
                RetryDisposition::Never,
            ),
            (
                MetadataError::Backend("unknown".into()),
                RetryDisposition::Unknown,
            ),
        ] {
            let error = RepositoryError::Payload(Error::Backend(Box::new(
                RepositoryError::Payload(Error::Io(std::io::Error::other(cause))),
            )));
            assert_eq!(error.retry_disposition(), expected);
        }
        let error = Error::Backend(Box::new(std::io::Error::other(Error::Throttled {
            retry_after: Some(std::time::Duration::from_secs(3)),
            source: Box::new(std::io::Error::other("busy")),
        })));
        assert_eq!(
            error.retry_disposition(),
            RetryDisposition::RetryAfter(std::time::Duration::from_secs(3))
        );
    }

    #[test]
    fn retry_guidance_is_typed() {
        let throttled = Error::Throttled {
            retry_after: Some(std::time::Duration::from_secs(2)),
            source: Box::new(std::io::Error::other("slow down")),
        };
        assert_eq!(
            throttled.retry_disposition(),
            RetryDisposition::RetryAfter(std::time::Duration::from_secs(2))
        );
        assert_eq!(
            Error::Io(std::io::Error::from(std::io::ErrorKind::TimedOut)).retry_disposition(),
            RetryDisposition::Retry
        );
        assert_eq!(
            Error::from("invalid").retry_disposition(),
            RetryDisposition::Never
        );
    }
}
