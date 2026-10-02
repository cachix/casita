use casita::experimental::MemoryBlobStore;

/// An in-memory payload store that counts payload opens and writes.
#[derive(Clone)]
pub(crate) struct CountingBlobStore {
    pub(crate) inner: MemoryBlobStore,
    /// Payloads opened for reading.
    pub(crate) reads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Payload writers opened and slices stored.
    pub(crate) writes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl CountingBlobStore {
    /// An empty store with both counters at zero.
    pub(crate) fn new() -> Self {
        Self {
            inner: MemoryBlobStore::new(),
            reads: Default::default(),
            writes: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl casita::experimental::BlobStore for CountingBlobStore {
    fn write_scope(&self) -> casita::experimental::BackendWriteScope {
        self.inner.write_scope()
    }
    fn begin_pinned_batch(
        &self,
        pin: casita::experimental::DataPinLease,
    ) -> Result<casita::experimental::BlobBatchGuard, casita::experimental::Error> {
        self.inner.begin_pinned_batch(pin)
    }
    fn publication(&self) -> casita::experimental::PayloadPublication<'_> {
        self.inner.publication()
    }
    fn order_deletions_after(&self, commits: casita::experimental::CommitDurability) {
        self.inner.order_deletions_after(commits);
    }
    async fn has(&self, id: &casita::BlobId) -> Result<bool, casita::experimental::Error> {
        self.inner.has(id).await
    }
    async fn open_read(
        &self,
        id: &casita::BlobId,
    ) -> Result<Option<Box<dyn casita::experimental::BlobReader>>, casita::experimental::Error>
    {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.open_read(id).await
    }
    async fn open_write(&self) -> Box<dyn casita::experimental::BlobWriter> {
        self.writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.open_write().await
    }
    async fn put_slice(&self, data: &[u8]) -> Result<casita::BlobId, casita::experimental::Error> {
        self.writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.put_slice(data).await
    }
}
