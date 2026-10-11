//! Collection planning faults must never reach logical pruning or physical deletion.
use super::*;
use crate::{BlobWriter, ChunkMeta, ChunkedBlobStore, MemoryMetadataStore};
use futures::stream::BoxStream;
use std::sync::atomic::{AtomicUsize, Ordering};

struct InventoryFaultStore<BS> {
    inner: BS,
    blob_error_after: Arc<AtomicUsize>,
    chunk_error_after: Arc<AtomicUsize>,
    deletions: Arc<AtomicUsize>,
}

struct InventoryFaultMetadata {
    inner: MemoryMetadataStore,
    error_after: Arc<AtomicUsize>,
}

struct InventoryFaultSnapshot {
    inner: Arc<dyn MetadataSnapshot>,
    error_after: usize,
}

#[async_trait]
impl MetadataSnapshot for InventoryFaultSnapshot {
    async fn get(
        &self,
        keys: &[crate::metadata::MetadataKey],
    ) -> Result<Vec<Option<bytes::Bytes>>, MetadataError> {
        self.inner.get(keys).await
    }

    async fn scan(
        &self,
        prefix: &crate::metadata::MetadataKey,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<crate::metadata::MetadataRecord>, MetadataError> {
        self.inner.scan(prefix, after, limit).await
    }

    async fn object_payload_batch(
        &self,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<(BlobId, u64)>>, MetadataError> {
        self.inner.object_payload_batch(keys).await
    }

    async fn validated_payload_batch(
        &self,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<(BlobId, u64)>>, MetadataError> {
        self.inner.validated_payload_batch(keys).await
    }

    async fn validated_closures(&self, keys: &[ObjectKey]) -> Result<Vec<bool>, MetadataError> {
        self.inner.validated_closures(keys).await
    }

    fn roots_under(
        &self,
        prefix: &RootName,
    ) -> BoxStream<'static, Result<crate::RootRecord, MetadataError>> {
        self.inner.roots_under(prefix)
    }

    fn revision(&self) -> crate::RepositoryRevision {
        self.inner.revision()
    }

    fn generation(&self) -> Result<u64, MetadataError> {
        self.inner.generation()
    }

    fn objects_created_through(
        &self,
        generation: u64,
    ) -> BoxStream<'static, Result<ObjectRecord, MetadataError>> {
        self.inner.objects_created_through(generation)
    }

    fn payload_catalog(&self) -> Option<&[u8]> {
        self.inner.payload_catalog()
    }

    fn retention_resources(&self) -> BTreeSet<crate::metadata::PinResource> {
        self.inner.retention_resources()
    }

    async fn object(&self, key: &ObjectKey) -> Result<Option<ObjectRecord>, MetadataError> {
        self.inner.object(key).await
    }

    async fn object_batch(
        &self,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<ObjectRecord>>, MetadataError> {
        self.inner.object_batch(keys).await
    }

    async fn root(&self, name: &RootName) -> Result<Option<ObjectKey>, MetadataError> {
        self.inner.root(name).await
    }

    fn objects(&self) -> BoxStream<'static, Result<ObjectRecord, MetadataError>> {
        self.inner.objects()
    }

    fn objects_unordered(&self) -> BoxStream<'static, Result<ObjectRecord, MetadataError>> {
        let objects = self.inner.objects_unordered();
        if self.error_after == usize::MAX {
            return objects;
        }
        objects
            .take(self.error_after)
            .chain(futures::stream::once(async {
                Err(MetadataError::Transient(
                    "injected inventory failure".into(),
                ))
            }))
            .boxed()
    }

    fn roots(&self) -> BoxStream<'static, Result<crate::RootRecord, MetadataError>> {
        self.inner.roots()
    }
}

#[async_trait]
impl MetadataStore for InventoryFaultMetadata {
    fn supports_metadata_records(&self) -> bool {
        self.inner.supports_metadata_records()
    }

    fn supports_root_retention(&self) -> bool {
        self.inner.supports_root_retention()
    }

    fn verification_facts(&self) -> Option<Arc<dyn crate::metadata::VerificationFacts>> {
        self.inner.verification_facts()
    }

    fn commit_durability(&self) -> Option<crate::blob::CommitDurability> {
        self.inner.commit_durability()
    }

    async fn get_records(
        &self,
        keys: &[crate::metadata::MetadataKey],
    ) -> Result<Vec<Option<bytes::Bytes>>, MetadataError> {
        self.inner.get_records(keys).await
    }

    async fn commit_checked(
        &self,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        self.inner.commit_checked(mutation).await
    }

    async fn compact_transient_state(&self) -> Result<(), MetadataError> {
        self.inner.compact_transient_state().await
    }

    async fn try_collection_lease(
        &self,
    ) -> Result<Option<crate::metadata::RepositoryLease>, MetadataError> {
        self.inner.try_collection_lease().await
    }

    fn coordinates_payload_catalog(&self) -> bool {
        self.inner.coordinates_payload_catalog()
    }

    async fn pin_store(&self) -> Result<Arc<dyn crate::metadata::PinStore>, MetadataError> {
        self.inner.pin_store().await
    }

    async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
        Ok(Arc::new(InventoryFaultSnapshot {
            inner: self.inner.snapshot().await?,
            error_after: self.error_after.load(Ordering::SeqCst),
        }))
    }

    async fn commit(
        &self,
        expected: &crate::RepositoryRevision,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        self.inner.commit(expected, mutation).await
    }
}

fn fault_after<'a, T: Send + 'a>(
    inventory: BoxStream<'a, Result<T, crate::error::Error>>,
    count: usize,
) -> BoxStream<'a, Result<T, crate::error::Error>> {
    if count == usize::MAX {
        return inventory;
    }
    inventory
        .take(count)
        .chain(futures::stream::once(async {
            Err(crate::error::Error::Msg(
                "injected inventory failure".into(),
            ))
        }))
        .boxed()
}

#[tokio::test]
async fn inventory_failures_do_not_prune_or_sweep_and_allow_retry() {
    for inventory in ["logical", "blobs", "chunks"] {
        for fail_after in [0, 255, 256, 257] {
            let temporary = tempfile::tempdir().unwrap();
            let object_error_after = Arc::new(AtomicUsize::new(usize::MAX));
            let blob_error_after = Arc::new(AtomicUsize::new(usize::MAX));
            let chunk_error_after = Arc::new(AtomicUsize::new(usize::MAX));
            let deletions = Arc::new(AtomicUsize::new(0));
            let payloads = InventoryFaultStore {
                inner: ChunkedBlobStore::new(
                    Arc::new(object_store::memory::InMemory::new()),
                    object_store::path::Path::default(),
                    1024,
                ),
                blob_error_after: blob_error_after.clone(),
                chunk_error_after: chunk_error_after.clone(),
                deletions: deletions.clone(),
            };
            let repository = Repository::new(
                payloads,
                InventoryFaultMetadata {
                    inner: MemoryMetadataStore::new().unwrap(),
                    error_after: object_error_after.clone(),
                },
            );
            let mutation = repository.mutation_session().await.unwrap();
            let mut staged = Vec::new();
            let mut entries = Vec::new();
            let mut live = Vec::new();
            for index in 0..260u64 {
                let bytes =
                    super::collection_inventory_tests::inventory_bytes(index as usize, index < 259);
                let blob = mutation.stage_blob(&bytes).await.unwrap();
                let payload = blob.record().payload();
                live.push((payload, bytes.clone()));
                entries.push((
                    PathComponent::try_from(format!("file-{index:08}").as_str()).unwrap(),
                    Node::File {
                        digest: payload,
                        size: bytes.len() as u64,
                        executable: false,
                    },
                ));
                staged.push(blob);
            }
            let directory = mutation
                .stage_directory(&Directory::try_from_iter(entries).unwrap())
                .await
                .unwrap();
            let root = directory.record().key().clone();
            staged.push(directory);
            let orphan = mutation
                .stage_blob(&super::collection_inventory_tests::inventory_bytes(
                    261, true,
                ))
                .await
                .unwrap();
            let orphan_record = orphan.record().clone();
            staged.push(orphan);
            mutation
                .publish_rooted(staged, RootName::try_from("live").unwrap(), root)
                .await
                .unwrap();
            drop(mutation);
            crate::flush_repository_leases().await.unwrap();
            let before_revision = repository.metadata().snapshot().await.unwrap().revision();
            let before_blobs: BTreeSet<_> = repository
                .payloads()
                .list_blobs()
                .try_collect()
                .await
                .unwrap();
            let before_chunks: BTreeSet<_> = repository
                .payloads()
                .list_chunks()
                .try_collect()
                .await
                .unwrap();
            assert!(before_blobs.len() > fail_after && before_chunks.len() > fail_after);
            let repository = repository
                .with_fs_coordination(temporary.path())
                .with_spill_limits(SpillLimits {
                    max_memory_objects: 17,
                    ..SpillLimits::default()
                });
            let fault = match inventory {
                "logical" => &object_error_after,
                "blobs" => &blob_error_after,
                "chunks" => &chunk_error_after,
                _ => unreachable!(),
            };
            fault.store(fail_after, Ordering::SeqCst);
            let error = repository.collect().await.unwrap_err();
            assert!(
                error.to_string().contains("injected inventory failure"),
                "{error}"
            );
            assert_eq!(deletions.load(Ordering::SeqCst), 0);
            crate::flush_repository_leases().await.unwrap();
            let snapshot = repository.metadata().snapshot().await.unwrap();
            assert_eq!(snapshot.revision(), before_revision);
            assert_eq!(
                snapshot.object(orphan_record.key()).await.unwrap(),
                Some(orphan_record.clone())
            );
            drop(snapshot);
            let spill = temporary.path().join(crate::spill::SPILL_DIRECTORY);
            assert!(std::fs::read_dir(&spill).unwrap().next().is_none());
            fault.store(usize::MAX, Ordering::SeqCst);
            assert_eq!(
                repository
                    .payloads()
                    .list_blobs()
                    .try_collect::<BTreeSet<_>>()
                    .await
                    .unwrap(),
                before_blobs
            );
            assert_eq!(
                repository
                    .payloads()
                    .list_chunks()
                    .try_collect::<BTreeSet<_>>()
                    .await
                    .unwrap(),
                before_chunks
            );
            let result = repository.collect().await.unwrap();
            assert_eq!(result.removed.logical_objects, 1);
            assert_eq!(result.removed.payload_blobs, 1);
            assert!(
                repository
                    .metadata()
                    .snapshot()
                    .await
                    .unwrap()
                    .object(orphan_record.key())
                    .await
                    .unwrap()
                    .is_none()
            );
            for (payload, bytes) in live {
                assert_eq!(
                    repository
                        .payloads()
                        .read_to_vec(&payload)
                        .await
                        .unwrap()
                        .unwrap(),
                    bytes
                );
            }
            assert!(std::fs::read_dir(&spill).unwrap().next().is_none());
        }
    }
}

#[async_trait]
impl<BS: BlobStore> BlobStore for InventoryFaultStore<BS> {
    fn publication(&self) -> crate::blob::PayloadPublication<'_> {
        self.inner.publication()
    }
    fn write_scope(&self) -> crate::metadata::BackendWriteScope {
        self.inner.write_scope()
    }

    fn begin_pinned_batch(
        &self,
        pin: crate::metadata::DataPinLease,
    ) -> Result<crate::blob::BlobBatchGuard, crate::error::Error> {
        self.inner.begin_pinned_batch(pin)
    }

    async fn has(&self, digest: &BlobId) -> Result<bool, crate::error::Error> {
        self.inner.has(digest).await
    }

    async fn open_read(
        &self,
        digest: &BlobId,
    ) -> Result<Option<Box<dyn BlobReader>>, crate::error::Error> {
        self.inner.open_read(digest).await
    }

    async fn open_write(&self) -> Box<dyn BlobWriter> {
        self.inner.open_write().await
    }

    async fn chunks(&self, digest: &BlobId) -> Result<Option<Vec<ChunkMeta>>, crate::error::Error> {
        self.inner.chunks(digest).await
    }
}

#[async_trait]
impl<BS: BlobStore + BlobGc> BlobGc for InventoryFaultStore<BS> {
    async fn chunks_for_gc(
        &self,
        digest: &BlobId,
        manifest_present: bool,
    ) -> Result<Option<Vec<ChunkMeta>>, crate::error::Error> {
        self.inner.chunks_for_gc(digest, manifest_present).await
    }

    async fn reclaim_metadata_pinned(
        &self,
        pins: Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
    ) -> Result<(), crate::error::Error> {
        self.inner.reclaim_metadata_pinned(pins, owned_claims).await
    }

    fn list_blobs(&self) -> BoxStream<'_, Result<BlobId, crate::error::Error>> {
        fault_after(
            self.inner.list_blobs(),
            self.blob_error_after.load(Ordering::SeqCst),
        )
    }

    fn list_chunks(&self) -> BoxStream<'_, Result<ChunkId, crate::error::Error>> {
        fault_after(
            self.inner.list_chunks(),
            self.chunk_error_after.load(Ordering::SeqCst),
        )
    }

    async fn delete_blob(&self, digest: &BlobId) -> Result<(), crate::error::Error> {
        self.deletions.fetch_add(1, Ordering::SeqCst);
        self.inner.delete_blob(digest).await
    }

    async fn delete_chunk(&self, digest: &ChunkId) -> Result<(), crate::error::Error> {
        self.deletions.fetch_add(1, Ordering::SeqCst);
        self.inner.delete_chunk(digest).await
    }

    async fn delete_blobs_pinned(
        &self,
        digests: &[BlobId],
        pins: Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
        before_prune: bool,
    ) -> Result<usize, crate::error::Error> {
        self.deletions.fetch_add(1, Ordering::SeqCst);
        self.inner
            .delete_blobs_pinned(digests, pins, owned_claims, before_prune)
            .await
    }

    async fn delete_chunks_pinned(
        &self,
        digests: &[ChunkId],
        pins: Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
    ) -> Result<usize, crate::error::Error> {
        self.deletions.fetch_add(1, Ordering::SeqCst);
        self.inner
            .delete_chunks_pinned(digests, pins, owned_claims)
            .await
    }

    async fn finish_deletions_pinned(
        &self,
        force_reclaim: bool,
        pins: Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
        before_prune: bool,
    ) -> Result<(), crate::error::Error> {
        self.inner
            .finish_deletions_pinned(force_reclaim, pins, owned_claims, before_prune)
            .await
    }

    async fn finish_collection_pinned(
        &self,
        force_reclaim: bool,
        pins: Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
    ) -> Result<(), crate::error::Error> {
        self.inner
            .finish_collection_pinned(force_reclaim, pins, owned_claims)
            .await
    }
}
