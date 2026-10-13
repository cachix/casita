//! Raw blob import requests.

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};

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

    /// Group blob requests into one atomic publication with bounded concurrent
    /// staging. Reports preserve input order; an input failure changes no roots.
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), casita::Error> {
    /// use casita::{import::BlobImport, Repository};
    /// let repository = Repository::memory()?;
    /// let inputs = ["first", "second"].map(|name| {
    ///     BlobImport::new(std::io::Cursor::new(name.as_bytes()), name.parse().unwrap())
    /// });
    /// let keys = repository.import(BlobImport::batch(inputs)).await?;
    /// assert_eq!(keys.len(), 2);
    /// # Ok(())
    /// # }
    /// ```
    pub fn batch(inputs: impl IntoIterator<Item = Self>) -> BlobBatchImport<R> {
        BlobBatchImport::new(inputs.into_iter().collect())
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
        let mut keys = Self::batch([self]).import_into(repository).await?;
        Ok(keys.pop().expect("one blob request returns one key"))
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
        let mut keys = Self::batch([self]).import_into(session).await?;
        Ok(keys.pop().expect("one blob request returns one key"))
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

session_importer!(BlobImport<R>, [R], ObjectKey, RepositoryError);

/// Stream a bounded group of blobs and publish all named roots atomically.
///
/// Stages up to 16 inputs concurrently so their durable resource protection
/// updates can share a ledger sync. Uses one mutation session and one metadata
/// publication. Reports preserve input order even when reads complete out of
/// order. On any input error,
/// every root keeps its previous value; bytes already staged are collectible.
/// Inputs must have distinct root names and fit the repository's mutation batch
/// limit. Empty batches acquire no staging pins and publish nothing.
/// Construct this request with [`BlobImport::batch`].
pub struct BlobBatchImport<R> {
    inputs: Vec<BlobImport<R>>,
}

impl<R> BlobBatchImport<R> {
    fn new(inputs: Vec<BlobImport<R>>) -> Self {
        Self { inputs }
    }

    fn validate(&self, limits: &crate::format::FormatLimits) -> Result<(), RepositoryError> {
        let limit = limits.max_batch_objects.min(limits.max_root_changes);
        if self.inputs.len() > limit {
            return Err(RepositoryError::LimitExceeded(format!(
                "blob import batch has {} inputs; limit is {limit}",
                self.inputs.len()
            )));
        }
        let mut roots = std::collections::BTreeSet::new();
        for input in &self.inputs {
            if !roots.insert(&input.root) {
                return Err(RepositoryError::InvalidInput(format!(
                    "duplicate blob import root {}",
                    input.root
                )));
            }
        }
        Ok(())
    }
}

impl<R: AsyncRead + Unpin + Send> BlobBatchImport<R> {
    async fn publish<PS: BlobStore, SS: MetadataStore>(
        self,
        session: &crate::MutationSession<'_, PS, SS>,
    ) -> Result<Vec<ObjectKey>, RepositoryError> {
        // The existing pin coalescer confirms each resource before any payload
        // write. Concurrent staging lets sibling requests join that durable
        // update without weakening the storage/collection ordering. Poll these
        // futures in place so cancelling the batch drops unfinished readers;
        // storage work retains its existing cancellation-safe protection.
        const CONCURRENT_BLOB_READERS: usize = 16;
        let staged_inputs =
            futures::stream::iter(self.inputs.into_iter().map(|mut input| async move {
                let object = session.stage_blob_reader(&mut input.reader).await?;
                Ok::<_, RepositoryError>((object, input.root))
            }))
            .buffered(CONCURRENT_BLOB_READERS)
            .try_collect::<Vec<_>>()
            .await?;
        let mut staged = Vec::with_capacity(staged_inputs.len());
        let mut roots = Vec::with_capacity(staged_inputs.len());
        let mut keys = Vec::with_capacity(staged_inputs.len());
        for (object, root) in staged_inputs {
            let key = object.record().key().clone();
            roots.push(crate::metadata::RootChange::Set {
                name: root,
                target: key.clone(),
            });
            keys.push(key);
            staged.push(object);
        }
        session.publish(staged, roots).await?;
        Ok(keys)
    }
}

impl<PS, SS, R> BackendImporter<Repository<PS, SS>> for BlobBatchImport<R>
where
    PS: BlobStore,
    SS: MetadataStore,
    R: AsyncRead + Unpin + Send,
{
    type Report = Vec<ObjectKey>;
    type Error = RepositoryError;

    async fn import_into(
        self,
        repository: &Repository<PS, SS>,
    ) -> Result<Self::Report, Self::Error> {
        self.validate(repository.limits())?;
        if self.inputs.is_empty() {
            return Ok(Vec::new());
        }
        let session = repository.mutation_session().await?;
        self.publish(&session).await
    }
}

impl<'session, PS, SS, R> BackendImporter<crate::MutationSession<'session, PS, SS>>
    for BlobBatchImport<R>
where
    PS: BlobStore,
    SS: MetadataStore,
    R: AsyncRead + Unpin + Send,
{
    type Report = Vec<ObjectKey>;
    type Error = RepositoryError;

    async fn import_into(
        self,
        session: &crate::MutationSession<'session, PS, SS>,
    ) -> Result<Self::Report, Self::Error> {
        self.validate(session.repository().limits())?;
        if self.inputs.is_empty() {
            return Ok(Vec::new());
        }
        self.publish(session).await
    }
}

#[async_trait]
impl<R: AsyncRead + Unpin + Send> Importer for BlobBatchImport<R> {
    type Report = Vec<ObjectKey>;
    type Error = crate::Error;

    async fn import(self, repository: &crate::Repository) -> Result<Self::Report, Self::Error> {
        self.import_into(&repository.inner)
            .await
            .map_err(|error| crate::api::Error::classified(error.category(), error))
    }
}

#[cfg(feature = "experimental")]
repository_importer!(BlobBatchImport<R>, [R], Vec<ObjectKey>, RepositoryError);

session_importer!(BlobBatchImport<R>, [R], Vec<ObjectKey>, RepositoryError);
