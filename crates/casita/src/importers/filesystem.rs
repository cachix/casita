//! Rooted and session-scoped filesystem import requests.

use async_trait::async_trait;

use super::{BackendImporter, Importer};
use crate::blob::BlobStore;
use crate::metadata::MetadataStore;
use crate::repository::Repository;
use crate::repository::RepositoryError;
use crate::{ObjectKey, RootName, RootRetention};
use std::path::PathBuf;

/// One filesystem-tree import request.
#[derive(Debug, Clone)]
pub struct FilesystemImport {
    /// Filesystem directory to capture.
    path: PathBuf,
    /// Destination-owned root name.
    root: RootName,
    /// Whether every regular file must be read again.
    reread: bool,
    /// Maximum regular files staged at once (default 16), bounded by the walk page.
    file_concurrency: std::num::NonZeroUsize,
    /// One exact relative path to omit from the imported directory.
    excluded: Option<PathBuf>,
    /// Retention to publish atomically with the root, when requested.
    retention: Option<RootRetention>,
}

/// Capture a directory without creating a named root, within an existing session.
///
/// The session protects the imported graph for its lifetime. Publish a root or
/// acquire another hold before dropping it to keep the graph live. Files are
/// always read again.
///
/// This request cannot be imported by a repository: a temporary session would
/// release the graph's protection before returning it to the caller.
///
/// ```compile_fail
/// # async fn example() {
/// use casita::import::UnrootedFilesystemImport;
/// let repository = casita::Repository::memory().unwrap();
/// repository.import(UnrootedFilesystemImport::new("./tree")).await.unwrap();
/// # }
/// ```
///
/// ```compile_fail
/// # async fn example() {
/// use casita::{experimental::Repository, import::UnrootedFilesystemImport};
/// let repository = Repository::memory().unwrap();
/// repository.import(UnrootedFilesystemImport::new("./tree")).await.unwrap();
/// # }
/// ```
#[cfg(feature = "experimental")]
#[derive(Debug, Clone)]
pub struct UnrootedFilesystemImport {
    /// Filesystem directory to capture.
    path: PathBuf,
}

#[cfg(feature = "experimental")]
impl UnrootedFilesystemImport {
    /// Capture a directory protected by the receiving mutation session.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[cfg(feature = "experimental")]
#[async_trait]
impl<'session, PS, SS> Importer<crate::MutationSession<'session, PS, SS>>
    for UnrootedFilesystemImport
where
    PS: BlobStore,
    SS: MetadataStore,
{
    type Report = ObjectKey;
    type Error = RepositoryError;

    async fn import(
        self,
        session: &crate::MutationSession<'session, PS, SS>,
    ) -> Result<Self::Report, Self::Error> {
        session
            .import_path_inner(
                self.path,
                None,
                false,
                None,
                crate::filesystem::DEFAULT_FILE_CONCURRENCY,
            )
            .await
    }
}

impl<PS, SS> BackendImporter<Repository<PS, SS>> for FilesystemImport
where
    PS: BlobStore,
    SS: MetadataStore,
{
    type Report = ObjectKey;
    type Error = RepositoryError;

    async fn import_into(
        self,
        repository: &Repository<PS, SS>,
    ) -> Result<Self::Report, Self::Error> {
        self.import_into(&repository.mutation_session().await?)
            .await
    }
}

impl FilesystemImport {
    /// Capture a directory under a named root, reusing unchanged local files.
    pub fn new(path: impl Into<PathBuf>, root: RootName) -> Self {
        Self {
            path: path.into(),
            root,
            reread: false,
            file_concurrency: crate::filesystem::DEFAULT_FILE_CONCURRENCY,
            excluded: None,
            retention: None,
        }
    }

    /// Read every file again instead of consulting the local ingest cache.
    pub fn reread(mut self, reread: bool) -> Self {
        self.reread = reread;
        self
    }

    /// Limit concurrent file ingestion for this request. One runs serially.
    /// Chunk uploads within each file have a separate storage-level limit.
    pub fn with_file_concurrency(mut self, concurrency: std::num::NonZeroUsize) -> Self {
        self.file_concurrency = concurrency;
        self
    }

    /// Omit one exact relative path, such as a workspace control file.
    pub fn exclude(mut self, name: impl Into<PathBuf>) -> Self {
        self.excluded = Some(name.into());
        self
    }

    /// Set the root's retention policy in the same publication as the import.
    pub fn with_retention(mut self, retention: RootRetention) -> Self {
        self.retention = Some(retention);
        self
    }
}

impl FilesystemImport {
    /// Stage a filesystem graph and root change without publishing any checkpoints.
    #[cfg(feature = "experimental")]
    pub async fn stage<'hold, PS: BlobStore, SS: MetadataStore>(
        self,
        session: &'hold crate::MutationSession<'_, PS, SS>,
    ) -> Result<super::StagedImport<'hold, ObjectKey>, RepositoryError> {
        let mut objects = Vec::new();
        let (mut keys, _) = session
            .import_paths_with_staging(
                vec![(self.path, Some(self.root.clone()), self.excluded)],
                !self.reread,
                self.file_concurrency,
                false,
                super::ImportPublication {
                    retention: self.retention,
                    staging: Some(&mut objects),
                },
            )
            .await?;
        let key = keys.remove(0);
        let metadata_changes = self
            .retention
            .map(|retention| crate::repository::root_policy::policy_change(&self.root, retention))
            .into_iter()
            .collect();
        Ok(super::StagedImport {
            report: key.clone(),
            objects,
            root_change: crate::RootChange::Set {
                name: self.root,
                target: key,
            },
            metadata_changes,
        })
    }
}

#[async_trait]
impl Importer for FilesystemImport {
    type Report = ObjectKey;
    type Error = crate::Error;

    async fn import(self, repository: &crate::Repository) -> Result<Self::Report, Self::Error> {
        self.import_into(&repository.inner)
            .await
            .map_err(|error| crate::api::Error::classified(error.category(), error))
    }
}

impl<'session, PS, SS> BackendImporter<crate::MutationSession<'session, PS, SS>>
    for FilesystemImport
where
    PS: BlobStore,
    SS: MetadataStore,
{
    type Report = ObjectKey;
    type Error = RepositoryError;

    async fn import_into(
        self,
        repository: &crate::MutationSession<'session, PS, SS>,
    ) -> Result<Self::Report, Self::Error> {
        repository
            .import_path_inner_with_retention(
                self.path,
                Some(self.root),
                !self.reread,
                self.excluded.as_deref(),
                self.file_concurrency,
                self.retention,
            )
            .await
    }
}

#[cfg(feature = "experimental")]
repository_importer!(FilesystemImport, [], ObjectKey, RepositoryError);

#[cfg(feature = "experimental")]
#[async_trait]
impl<'session, PS, SS> Importer<crate::MutationSession<'session, PS, SS>> for FilesystemImport
where
    PS: BlobStore,
    SS: MetadataStore,
{
    type Report = ObjectKey;
    type Error = RepositoryError;

    async fn import(
        self,
        repository: &crate::MutationSession<'session, PS, SS>,
    ) -> Result<Self::Report, Self::Error> {
        self.import_into(repository).await
    }
}

/// Import separate filesystem trees through shared bounded pages and checkpoints.
///
/// Root names must be unique. The traversal limit applies to the whole request.
/// All named roots are published together after every tree succeeds; failed
/// requests may leave reusable unrooted checkpoints. Input order is preserved in
/// the returned keys. An empty request does no work. Each path is opened as its
/// own directory handle, preserving ordinary filesystem import link semantics.
#[cfg(feature = "experimental")]
#[derive(Debug, Clone)]
pub struct MultiRootFilesystemImport {
    paths: Vec<(PathBuf, RootName)>,
    reread: bool,
    file_concurrency: std::num::NonZeroUsize,
}

#[cfg(feature = "experimental")]
impl MultiRootFilesystemImport {
    /// Capture each `(path, root name)` pair without a synthetic parent.
    pub fn new(paths: impl IntoIterator<Item = (PathBuf, RootName)>) -> Self {
        Self {
            paths: paths.into_iter().collect(),
            reread: false,
            file_concurrency: crate::filesystem::DEFAULT_FILE_CONCURRENCY,
        }
    }

    /// Read every file again instead of consulting the local ingest cache.
    pub fn reread(mut self, reread: bool) -> Self {
        self.reread = reread;
        self
    }

    /// Bound file ingestion across all roots, rather than per output.
    pub fn with_file_concurrency(mut self, concurrency: std::num::NonZeroUsize) -> Self {
        self.file_concurrency = concurrency;
        self
    }
}

#[cfg(feature = "experimental")]
#[async_trait]
impl<'session, PS: BlobStore, SS: MetadataStore> Importer<crate::MutationSession<'session, PS, SS>>
    for MultiRootFilesystemImport
{
    type Report = Vec<ObjectKey>;
    type Error = RepositoryError;

    async fn import(
        self,
        session: &crate::MutationSession<'session, PS, SS>,
    ) -> Result<Self::Report, Self::Error> {
        let (keys, _) = session
            .import_paths_inner(
                self.paths
                    .into_iter()
                    .map(|(path, name)| (path, Some(name), None))
                    .collect(),
                !self.reread,
                self.file_concurrency,
                true,
            )
            .await?;
        Ok(keys)
    }
}

#[cfg(feature = "experimental")]
#[async_trait]
impl<PS: BlobStore, SS: MetadataStore> Importer<Repository<PS, SS>> for MultiRootFilesystemImport {
    type Report = Vec<ObjectKey>;
    type Error = RepositoryError;

    async fn import(self, repository: &Repository<PS, SS>) -> Result<Self::Report, Self::Error> {
        self.import(&repository.mutation_session().await?).await
    }
}
