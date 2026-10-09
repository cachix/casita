//! NAR and native filesystem import requests.

use async_trait::async_trait;
use bytes::Bytes;
use std::path::PathBuf;
use tokio::io::AsyncRead;

use super::Importer;
use crate::api::Repository;
#[cfg(feature = "experimental")]
use crate::blob::BlobGc;
#[cfg(feature = "experimental")]
use crate::metadata::MetadataStore;
use crate::nar::{NarError, NarRequirements, VerifiedNarReport, ensure_nar, stream};
#[cfg(feature = "experimental")]
use crate::repository::Repository as CoreRepository;
use crate::{Node, SymlinkTarget};

/// A standalone canonical archive. EOF is required after exactly one NAR.
/// For framed protocols pass a length-limited reader; the frame length must
/// include only this archive. No byte outside that reader is consumed.
///
/// The archive is decoded on a dedicated thread, at most 16 per process
/// across all repositories; further imports wait for a decoder. An import
/// keeps its decoder until the archive is decoded or rejected, or the import
/// is dropped, so a stalled reader holds one: give slow or untrusted readers
/// a read timeout.
pub struct NarImport<R> {
    reader: R,
    requirements: NarRequirements,
}
impl<R> NarImport<R> {
    /// Import without publishing a named root. The report retains the tree.
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            requirements: NarRequirements::default(),
        }
    }
    /// Select additional measured facts and reference scanning.
    pub fn requirements(mut self, requirements: NarRequirements) -> Self {
        self.requirements = requirements;
        self
    }
}
/// Native filesystem intake. Directory ingestion shares the ordinary importer;
/// first intake may serialize the retained tree once. Warm intake reuses facts.
pub struct FilesystemNarImport {
    path: PathBuf,
    requirements: NarRequirements,
    reread: bool,
}
impl FilesystemNarImport {
    /// Capture a native filesystem tree without an intermediate archive.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            requirements: NarRequirements::default(),
            reread: false,
        }
    }
    /// Read every source file again instead of consulting the local ingest
    /// cache. This does not force a NAR audit when the resulting tree already
    /// has measured facts; use [`crate::scrub_nar`] to audit stored content.
    /// Standalone file roots are always read, regardless of this setting.
    pub fn reread(mut self, reread: bool) -> Self {
        self.reread = reread;
        self
    }
    /// Select additional measured facts and reference scanning.
    pub fn requirements(mut self, requirements: NarRequirements) -> Self {
        self.requirements = requirements;
        self
    }
}
#[async_trait]
impl<R: AsyncRead + Unpin + Send> Importer for NarImport<R> {
    type Report = VerifiedNarReport;
    type Error = NarError;
    async fn import(self, repository: &Repository) -> Result<Self::Report, Self::Error> {
        stream::import(repository, self.reader, self.requirements).await
    }
}
#[async_trait]
impl Importer for FilesystemNarImport {
    type Report = VerifiedNarReport;
    type Error = NarError;
    async fn import(self, repository: &Repository) -> Result<Self::Report, Self::Error> {
        self.requirements.validate(None)?;
        let session = repository
            .inner
            .mutation_session()
            .await
            .map_err(NarError::storage)?;
        let metadata = tokio::fs::symlink_metadata(&self.path).await?;
        let root = if metadata.is_dir() {
            let key = session
                .import_path_inner(
                    &self.path,
                    None,
                    !self.reread,
                    None,
                    crate::filesystem::DEFAULT_FILE_CONCURRENCY,
                )
                .await
                .map_err(NarError::storage)?;
            let reader = repository.retained_reader().await?;
            let directory = stream::read_directory(&reader, &key).await?;
            Node::Directory {
                digest: directory.digest(),
                size: directory.size(),
            }
        } else if metadata.file_type().is_symlink() {
            let target = tokio::fs::read_link(&self.path).await?;
            Node::Symlink {
                target: SymlinkTarget::try_from(Bytes::copy_from_slice(
                    crate::filesystem::names::os_str_bytes(target.as_os_str())
                        .map_err(NarError::storage)?,
                ))
                .map_err(NarError::storage)?,
            }
        } else if metadata.is_file() {
            let parent = self
                .path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| std::path::Path::new("."));
            let root = crate::filesystem::root::FsRoot::open_read(parent)
                .await
                .map_err(NarError::storage)?;
            let name = self
                .path
                .file_name()
                .ok_or_else(|| NarError::invalid("file has no name"))?;
            let (_, executable, object) = crate::repository::stage_filesystem_file(
                &session,
                &root,
                std::path::Path::new(name),
            )
            .await
            .map_err(NarError::storage)?;
            let root = Node::File {
                digest: crate::BlobId::new(object.record().key().native_digest().unwrap()),
                size: object.record().payload_size(),
                executable,
            };
            session
                .publish_unrooted(vec![object])
                .await
                .map_err(NarError::storage)?;
            root
        } else {
            return Err(NarError::invalid("unsupported filesystem root"));
        };
        let reader = repository.retained_reader().await?;
        let result = ensure_nar(&reader, &root, &self.requirements).await;
        drop(session);
        result
    }
}
#[cfg(feature = "experimental")]
#[async_trait]
impl<PS: BlobGc + 'static, SS: MetadataStore + 'static, R: AsyncRead + Unpin + Send>
    Importer<CoreRepository<PS, SS>> for NarImport<R>
{
    type Report = VerifiedNarReport;
    type Error = NarError;
    async fn import(
        self,
        repository: &CoreRepository<PS, SS>,
    ) -> Result<Self::Report, Self::Error> {
        self.import(&Repository {
            inner: repository.clone().into_builtin(),
        })
        .await
    }
}
#[cfg(feature = "experimental")]
#[async_trait]
impl<PS: BlobGc + 'static, SS: MetadataStore + 'static> Importer<CoreRepository<PS, SS>>
    for FilesystemNarImport
{
    type Report = VerifiedNarReport;
    type Error = NarError;
    async fn import(
        self,
        repository: &CoreRepository<PS, SS>,
    ) -> Result<Self::Report, Self::Error> {
        self.import(&Repository {
            inner: repository.clone().into_builtin(),
        })
        .await
    }
}
