//! Import selected immutable Git closures without creating a serving view.

use std::collections::BTreeSet;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;

use async_trait::async_trait;

use super::{BackendImporter, Importer};
use crate::blob::BlobStore;
use crate::git::repository::{DEFAULT_GIT_IMPORT_BUFFERED_BYTES, DEFAULT_GIT_IMPORT_CONCURRENCY};
use crate::metadata::MetadataStore;
use crate::repository::{OwnedRetentionHold, Repository};
use crate::{ObjectKey, RetainedReader};

/// Import exact native roots from a Git object directory, following alternates
/// and packs without creating refs, a worktree, or a Casita Git view.
///
/// Complete stored subtrees are reused across all imports. Source access is
/// lazy: an entirely stored selection needs no source directory at all.
/// Returned roots remain protected until the result's reader is dropped.
#[derive(Debug, Clone)]
pub struct GitClosureImport {
    pub(crate) objects_dir: PathBuf,
    pub(crate) roots: Vec<ObjectKey>,
    pub(crate) concurrency: NonZeroUsize,
    pub(crate) max_buffered_bytes: NonZeroU64,
}

impl GitClosureImport {
    /// Select type-qualified native Git keys in one hash format. `objects_dir`
    /// is the directory reported by `git rev-parse --git-path objects`.
    pub fn new(
        objects_dir: impl Into<PathBuf>,
        roots: impl IntoIterator<Item = ObjectKey>,
    ) -> Self {
        Self {
            objects_dir: objects_dir.into(),
            roots: roots
                .into_iter()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            concurrency: DEFAULT_GIT_IMPORT_CONCURRENCY,
            max_buffered_bytes: DEFAULT_GIT_IMPORT_BUFFERED_BYTES,
        }
    }

    /// Maximum simultaneously staged objects. Source decoding runs on a
    /// blocking worker; payload writes run concurrently on the async runtime.
    pub fn with_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Bound decoded source bodies awaiting staging. An oversized object runs
    /// alone. Pack delta workspace, caches, and payload backend buffers are
    /// additional; repository payload limits still apply to every object.
    pub fn with_max_buffered_bytes(mut self, bytes: NonZeroU64) -> Self {
        self.max_buffered_bytes = bytes;
        self
    }
}

/// Work actually performed by a closure import, excluding descendants behind
/// reused complete boundaries. No full reachable-object inventory is built.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GitClosureImportReport {
    /// Canonically ordered, distinct selected native roots.
    pub roots: Vec<ObjectKey>,
    /// Native objects decoded, verified, and staged from the source.
    pub imported_objects: usize,
    /// Existing native records encountered, including complete boundaries.
    pub reused_objects: usize,
    /// Uncompressed source body bytes decoded for newly imported objects.
    pub source_bytes: u64,
}

/// A completed import and the reader protecting its selected closures.
/// Publish application roots before releasing the reader to retain the result
/// beyond this operation. An import does not create permanent named roots.
pub struct GitClosureImportOutcome<R = RetainedReader> {
    /// Selected roots and work counters.
    pub report: GitClosureImportReport,
    /// Collection protection acquired before the importing session is released.
    pub reader: R,
}

impl<R> std::fmt::Debug for GitClosureImportOutcome<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitClosureImportOutcome")
            .field("report", &self.report)
            .finish_non_exhaustive()
    }
}

/// A failed native closure import. Partial objects may be stored, but are
/// never marked complete until every reachable dependency is durable.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum GitClosureImportError {
    /// The selection is not a set of native keys in one Git hash format.
    #[error("invalid Git closure selection: {0}")]
    InvalidSelection(String),
    /// A selected root names a different type than the source object has.
    #[error("selected Git root {root} is a {} in the source", actual.as_str())]
    RootKind {
        /// The selected type-qualified key.
        root: ObjectKey,
        /// The type the source object database stores for this OID.
        actual: crate::git::GitObjectKind,
    },
    /// Native object syntax, type or identity is invalid, including a linked
    /// object whose source type differs from its referrer's link.
    #[error(transparent)]
    Git(#[from] crate::git::GitError),
    /// A source object database could not be opened or decoded.
    #[error("Git closure source: {0}")]
    Source(String),
    /// Storage, validation, retention, or resource limit failure.
    #[error(transparent)]
    Repository(#[from] crate::repository::RepositoryError),
}

impl GitClosureImportError {
    pub(crate) fn category(&self) -> crate::RepositoryErrorCategory {
        use crate::RepositoryErrorCategory as Category;
        match self {
            Self::InvalidSelection(_) | Self::RootKind { .. } => Category::InvalidInput,
            Self::Git(_) => Category::InvalidData,
            Self::Source(_) => Category::Backend,
            Self::Repository(error) => error.category(),
        }
    }
}

impl<PS: BlobStore, SS: MetadataStore> BackendImporter<Repository<PS, SS>> for GitClosureImport {
    type Report = GitClosureImportOutcome<OwnedRetentionHold<PS, SS>>;
    type Error = GitClosureImportError;

    async fn import_into(
        self,
        repository: &Repository<PS, SS>,
    ) -> Result<Self::Report, Self::Error> {
        let session = repository.mutation_session().await?;
        let report = crate::git::repository::closure_import::import(&session, &self).await?;
        let reader = repository.owned_read_hold().await?;
        Ok(GitClosureImportOutcome { report, reader })
    }
}

#[async_trait]
impl Importer for GitClosureImport {
    type Report = GitClosureImportOutcome;
    type Error = crate::Error;

    async fn import(self, repository: &crate::Repository) -> Result<Self::Report, Self::Error> {
        let imported = self
            .import_into(&repository.inner)
            .await
            .map_err(|error| crate::api::Error::classified(error.category(), error))?;
        Ok(GitClosureImportOutcome {
            report: imported.report,
            reader: RetainedReader::new(imported.reader),
        })
    }
}

#[cfg(feature = "experimental")]
repository_importer!(
    GitClosureImport,
    [],
    GitClosureImportOutcome<OwnedRetentionHold<PS, SS>>,
    GitClosureImportError
);

#[cfg(feature = "experimental")]
#[async_trait]
impl<'session, PS: BlobStore, SS: MetadataStore> Importer<crate::MutationSession<'session, PS, SS>>
    for GitClosureImport
{
    type Report = GitClosureImportReport;
    type Error = GitClosureImportError;

    /// The receiving session retains the imported closures. Acquire a reader
    /// or publish roots before dropping it.
    async fn import(
        self,
        session: &crate::MutationSession<'session, PS, SS>,
    ) -> Result<Self::Report, Self::Error> {
        crate::git::repository::closure_import::import(session, &self).await
    }
}
