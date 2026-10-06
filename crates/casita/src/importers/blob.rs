//! Raw blob import requests.

use async_trait::async_trait;

use super::{BackendImporter, Importer};
use crate::blob::BlobStore;
use crate::metadata::MetadataStore;
use crate::repository::Repository;
use crate::repository::RepositoryError;
use crate::{ObjectKey, RootName};
use tokio::io::AsyncRead;

/// Stream a raw blob and atomically create or replace its named root.
pub struct BlobImport<R> {
    reader: R,
    root: RootName,
}

impl<R> BlobImport<R> {
    /// Import bytes under a destination-owned root name.
    pub fn new(reader: R, root: RootName) -> Self {
        Self { reader, root }
    }
}

impl<R: AsyncRead + Unpin + Send> BlobImport<R> {
    /// Stage bytes and a root change without publishing into the repository.
    #[cfg(feature = "experimental")]
    pub async fn stage<'hold, PS: BlobStore, SS: MetadataStore>(
        mut self,
        session: &'hold crate::MutationSession<'_, PS, SS>,
    ) -> Result<super::StagedImport<'hold, ObjectKey>, RepositoryError> {
        let object = session.stage_blob_reader(&mut self.reader).await?;
        let key = object.record().key().clone();
        Ok(super::StagedImport {
            report: key.clone(),
            objects: vec![object],
            root_change: crate::RootChange::Set {
                name: self.root,
                target: key,
            },
            metadata_changes: Vec::new(),
        })
    }
}

impl<PS, SS, R> BackendImporter<Repository<PS, SS>> for BlobImport<R>
where
    PS: BlobStore,
    SS: MetadataStore,
    R: AsyncRead + Unpin + Send,
{
    type Report = ObjectKey;
    type Error = RepositoryError;

    async fn import_into(
        self,
        repository: &Repository<PS, SS>,
    ) -> Result<ObjectKey, RepositoryError> {
        let session = repository.mutation_session().await?;
        self.import_into(&session).await
    }
}

impl<'session, PS, SS, R> BackendImporter<crate::MutationSession<'session, PS, SS>>
    for BlobImport<R>
where
    PS: BlobStore,
    SS: MetadataStore,
    R: AsyncRead + Unpin + Send,
{
    type Report = ObjectKey;
    type Error = RepositoryError;

    async fn import_into(
        self,
        session: &crate::MutationSession<'session, PS, SS>,
    ) -> Result<ObjectKey, RepositoryError> {
        let mut reader = self.reader;
        let staged = session.stage_blob_reader(&mut reader).await?;
        let key = staged.record().key().clone();
        session
            .publish_rooted(vec![staged], self.root, key.clone())
            .await?;
        Ok(key)
    }
}

#[async_trait]
impl<R: AsyncRead + Unpin + Send> Importer for BlobImport<R> {
    type Report = ObjectKey;
    type Error = crate::Error;

    async fn import(self, repository: &crate::Repository) -> Result<Self::Report, Self::Error> {
        self.import_into(&repository.inner)
            .await
            .map_err(|error| crate::api::Error::classified(error.category(), error))
    }
}

#[cfg(feature = "experimental")]
repository_importer!(BlobImport<R>, [R], ObjectKey, RepositoryError);

#[cfg(feature = "experimental")]
#[async_trait]
impl<'session, PS, SS, R> Importer<crate::MutationSession<'session, PS, SS>> for BlobImport<R>
where
    PS: BlobStore,
    SS: MetadataStore,
    R: AsyncRead + Unpin + Send,
{
    type Report = ObjectKey;
    type Error = RepositoryError;

    async fn import(
        self,
        session: &crate::MutationSession<'session, PS, SS>,
    ) -> Result<ObjectKey, RepositoryError> {
        self.import_into(session).await
    }
}
