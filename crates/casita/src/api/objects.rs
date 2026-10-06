//! Immutable reads using existing collection protection and short metadata views.

use super::*;

/// Protected immutable objects without a persistent metadata snapshot.
/// Clones and opened payloads share the admitted generation's protection.
/// Roots and mutable application metadata require a [`RetainedReader`] instead.
/// Queries reject objects born after this reader's generation, even if another
/// writer has since published them. Local and memory stores support this API.
#[derive(Clone)]
pub struct ObjectReader {
    inner: Arc<Inner>,
}

struct Inner {
    repository: BuiltinRepository,
    generation: RepositoryGeneration,
    _protection: Arc<dyn Send + Sync>,
}

impl ObjectReader {
    pub(super) fn new(
        repository: BuiltinRepository,
        generation: RepositoryGeneration,
        protection: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                repository,
                generation,
                _protection: protection,
            }),
        }
    }

    /// Latest commit generation whose immutable objects are protected.
    pub fn generation(&self) -> RepositoryGeneration {
        self.inner.generation
    }

    /// Look up one object. Missing objects and objects newer than this reader
    /// return `None`.
    pub async fn object(&self, key: &ObjectKey) -> Result<Option<ObjectRecord>, Error> {
        Ok(self
            .object_batch(std::slice::from_ref(key))
            .await?
            .pop()
            .flatten())
    }

    /// Read records in input order, preserving duplicates and missing entries.
    /// Each call releases its metadata transaction before returning.
    pub async fn object_batch(
        &self,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<ObjectRecord>>, Error> {
        self.inner
            .repository
            .metadata()
            .object_batch_created_through(keys, self.inner.generation.get())
            .await
            .app()
    }

    /// Open a seekable payload while sharing this reader's collection protection.
    pub async fn open(&self, key: &ObjectKey) -> Result<Option<Reader>, Error> {
        let Some(record) = self.object(key).await? else {
            return Ok(None);
        };
        let inner = self
            .inner
            .repository
            .payloads()
            .open_read(&record.payload())
            .await
            .map_err(RepositoryError::Payload)
            .app()?
            .ok_or(RepositoryError::MissingPayload(record.payload()))
            .app()?;
        Ok(Some(Reader {
            record,
            inner,
            _hold: Some(self.inner.clone()),
            nar_health: crate::nar::store::ReadHealth::new(self.inner.repository.nar_store.clone()),
        }))
    }

    /// Open a sequential payload that authenticates bytes before returning them.
    /// Missing or corrupt proofs fail verification, as for a retained reader.
    pub async fn open_verified(&self, key: &ObjectKey) -> Result<Option<VerifiedReader>, Error> {
        let Some(record) = self.object(key).await? else {
            return Ok(None);
        };
        open_verified_record(&self.inner.repository, record, self.inner.clone())
            .await
            .map(Some)
    }
}
