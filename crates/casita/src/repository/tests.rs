use super::collection::{mark_named_roots, mark_pin_scopes};
use super::*;

#[tokio::test]
async fn pressure_eviction_selects_oldest_opted_in_root_and_preserves_shared_data() {
    let repository = Repository::new(
        crate::MemoryBlobStore::new(),
        crate::MemoryMetadataStore::new().unwrap(),
    );
    let session = repository.mutation_session().await.unwrap();
    let object = session
        .stage_blob(b"shared release and build")
        .await
        .unwrap();
    let shared = object.record().key().clone();
    let release: RootName = "releases/current".parse().unwrap();
    session
        .publish_rooted(vec![object], release.clone(), shared.clone())
        .await
        .unwrap();
    let old: RootName = "cargo/builds/old".parse().unwrap();
    repository
        .set_root_with_retention(old.clone(), shared.clone(), RootRetention::Evictable)
        .await
        .unwrap();
    let newer: RootName = "cargo/builds/new".parse().unwrap();
    repository
        .set_root_with_retention(newer.clone(), shared.clone(), RootRetention::Evictable)
        .await
        .unwrap();
    repository.touch_root(&old, &shared).await.unwrap();

    assert_eq!(
        repository.root_retention(&release).await.unwrap(),
        Some(RootRetention::Permanent)
    );
    assert_eq!(
        repository
            .evict_roots_until(|| std::future::ready(crate::DiskUsage::new(100, 30)))
            .await
            .unwrap(),
        0
    );
    let mut probes = 0;
    assert_eq!(
        repository
            .evict_roots_until(|| {
                probes += 1;
                std::future::ready(crate::DiskUsage::new(
                    100,
                    if probes == 1 { 20 } else { 26 },
                ))
            })
            .await
            .unwrap(),
        1
    );
    assert_eq!(repository.root_retention(&newer).await.unwrap(), None);
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .root(&release)
            .await
            .unwrap(),
        Some(shared.clone())
    );
    assert_eq!(repository.evict_next_root().await.unwrap(), Some(old));
    repository.vacuum().await.unwrap();
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .root(&release)
            .await
            .unwrap(),
        Some(shared)
    );
    assert_eq!(repository.evict_next_root().await.unwrap(), None);
}

#[tokio::test]
async fn removed_root_does_not_pass_eviction_policy_to_recreated_name() {
    let repository = Repository::new(
        crate::MemoryBlobStore::new(),
        crate::MemoryMetadataStore::new().unwrap(),
    );
    let session = repository.mutation_session().await.unwrap();
    let object = session.stage_blob(b"build").await.unwrap();
    let key = object.record().key().clone();
    let name: RootName = "cargo/builds/reused".parse().unwrap();
    session
        .publish_rooted(vec![object], name.clone(), key.clone())
        .await
        .unwrap();
    repository
        .set_root_retention(&name, RootRetention::Evictable)
        .await
        .unwrap();
    repository
        .mutation_session()
        .await
        .unwrap()
        .publish(Vec::new(), vec![RootChange::Remove { name: name.clone() }])
        .await
        .unwrap();
    repository
        .mutation_session()
        .await
        .unwrap()
        .publish(
            Vec::new(),
            vec![RootChange::Set {
                name: name.clone(),
                target: key,
            }],
        )
        .await
        .unwrap();
    assert_eq!(
        repository.root_retention(&name).await.unwrap(),
        Some(RootRetention::Permanent)
    );
    assert_eq!(repository.evict_next_root().await.unwrap(), None);
}

#[tokio::test]
async fn remote_vacuum_preserves_ready_unpublished_catalog_rebase() {
    use crate::object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};

    async fn open_payloads(objects: Arc<dyn ObjectStore>, catalog: &[u8]) -> ChunkedBlobStore {
        ChunkedBlobStore::packed_with_catalog(
            objects,
            Path::from("ready-rebase"),
            1024,
            crate::PackOptions {
                target_size: u64::MAX,
                cache_capacity: 0,
            },
            catalog,
        )
        .await
        .unwrap()
    }

    let directory = tempfile::tempdir().unwrap();
    let state = crate::TursoMetadataStore::open(directory.path().join("state.db"))
        .await
        .unwrap();
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let empty = ChunkedBlobStore::empty_state_catalog().unwrap();
    // Use the remote packed profile, with no filesystem coordination or
    // local durability. SQLite only supplies the atomic metadata commits.
    let writer = Repository::new(open_payloads(objects.clone(), &empty).await, state.clone());
    let pause = writer.pause_catalog_maintenance_for_test();
    let mut keys = Vec::new();
    for name in ["first", "second"] {
        let session = writer.mutation_session().await.unwrap();
        let object = session.stage_blob(name.as_bytes()).await.unwrap();
        let key = object.record().key().clone();
        session
            .publish_rooted(vec![object], RootName::try_from(name).unwrap(), key.clone())
            .await
            .unwrap();
        keys.push(key);
        drop(session);
        writer
            .payloads()
            .set_pack_catalog_rebase_run_bytes_for_test(1);
    }
    pause.reached.notified().await;
    let catalog = state
        .snapshot()
        .await
        .unwrap()
        .payload_catalog()
        .unwrap()
        .to_vec();
    let collector = Repository::new(
        open_payloads(objects.clone(), &catalog).await,
        state.clone(),
    );
    // The completed candidate is absent from this handle's committed root.
    // Its writer pin must preserve it while unrelated metadata is vacuumed.
    collector.try_vacuum().await.unwrap();
    let drain = crate::flush_repository_leases();
    tokio::pin!(drain);
    assert!(futures::poll!(&mut drain).is_pending());
    pause.resume.notify_one();
    drain.await.unwrap();
    assert_ne!(
        state.snapshot().await.unwrap().payload_catalog().unwrap(),
        catalog
    );
    let orphan = crate::object_store::path::Path::from(format!(
        "ready-rebase/pack-indexes/b3/{}",
        blake3::hash(b"orphan catalog").to_hex()
    ));
    objects
        .put(&orphan, Bytes::from_static(b"orphan catalog").into())
        .await
        .unwrap();
    collector.vacuum().await.unwrap();
    assert!(matches!(
        objects.head(&orphan).await,
        Err(crate::object_store::Error::NotFound { .. })
    ));

    let session = writer.mutation_session().await.unwrap();
    let object = session.stage_blob(b"third").await.unwrap();
    let key = object.record().key().clone();
    session
        .publish_rooted(
            vec![object],
            RootName::try_from("third").unwrap(),
            key.clone(),
        )
        .await
        .unwrap();
    keys.push(key);
    drop(session);
    let catalog = state
        .snapshot()
        .await
        .unwrap()
        .payload_catalog()
        .unwrap()
        .to_vec();
    let reopened = Repository::new(open_payloads(objects, &catalog).await, state);
    let hold = reopened.retention_hold().await.unwrap();
    for (key, expected) in keys.into_iter().zip(["first", "second", "third"]) {
        let (_, mut reader) = hold.open_payload(&key).await.unwrap().unwrap();
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, expected.as_bytes());
    }
}

#[test]
fn local_pressure_collection_stamp_survives_a_new_handle() {
    let directory = tempfile::tempdir().unwrap();
    let repository = Repository::new(
        crate::MemoryBlobStore::new(),
        crate::MemoryMetadataStore::new().unwrap(),
    );
    let first = LocalMutationStart::new(repository.clone(), directory.path().to_path_buf());
    assert!(!first.collection_is_recent());
    first.record_collection();
    let reopened = LocalMutationStart::new(repository, directory.path().to_path_buf());
    assert!(reopened.collection_is_recent());
}
use std::sync::RwLock as StdRwLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use futures::stream::BoxStream;
use proptest::prelude::*;

use crate::node::Node;
use crate::path::PathComponent;
use crate::{
    BlobReader, BlobWriter, ChunkMeta, ChunkedBlobStore, Digest, MemoryBlobStore,
    MemoryMetadataStore, ObjectFormat,
};

#[test]
fn transient_state_backend_failure_has_typed_retry_guidance() {
    let error = RepositoryError::Metadata(MetadataError::Transient("try later".to_owned()));
    assert_eq!(error.category(), RepositoryErrorCategory::Backend);
    assert_eq!(error.retry_disposition(), crate::RetryDisposition::Retry);
}

struct SyntheticFormat {
    namespace: crate::NamespaceId,
}

impl Default for SyntheticFormat {
    fn default() -> Self {
        Self {
            namespace: "test.graph.v1".parse().unwrap(),
        }
    }
}

#[async_trait]
impl ObjectFormat for SyntheticFormat {
    fn namespace(&self) -> &crate::NamespaceId {
        &self.namespace
    }

    async fn verify(
        &self,
        mut context: VerificationContext<'_>,
        limits: &FormatLimits,
    ) -> Result<VerifiedObject, FormatError> {
        if context.key().native_id().len() != 1 {
            return Err(FormatError::InvalidPayload {
                namespace: self.namespace.clone(),
                message: "synthetic keys are exactly one byte".to_owned(),
            });
        }
        let bytes = context
            .read_to_end_bounded(limits.max_metadata_bytes)
            .await?;
        if bytes.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(FormatError::InvalidPayload {
                namespace: self.namespace.clone(),
                message: "synthetic links are not strictly ordered".to_owned(),
            });
        }
        if bytes.len() > limits.max_links_per_object {
            return Err(FormatError::LinkLimit {
                actual: bytes.len(),
                limit: limits.max_links_per_object,
            });
        }
        let links = bytes
            .into_iter()
            .map(|native| {
                ObjectKey::new(self.namespace.clone(), Bytes::from(vec![native])).unwrap()
            })
            .collect();
        context.finish(links)
    }
}

fn pc(value: &str) -> PathComponent {
    value.try_into().unwrap()
}

fn repository() -> Repository<MemoryBlobStore, MemoryMetadataStore> {
    Repository::new(MemoryBlobStore::new(), MemoryMetadataStore::new().unwrap())
}

#[test]
fn storage_full_detection_preserves_typed_nested_errors() {
    assert!(is_storage_full(&RepositoryError::Metadata(
        MetadataError::StorageFull
    )));
    assert!(is_storage_full(&RepositoryError::Io(
        std::io::ErrorKind::StorageFull.into()
    )));
    let nested = object_store::Error::Generic {
        store: "test",
        source: Box::new(std::io::Error::from(std::io::ErrorKind::StorageFull)),
    };
    assert!(is_storage_full(&RepositoryError::Payload(
        crate::error::Error::Io(std::io::Error::other(nested))
    )));
    assert!(!is_storage_full(&RepositoryError::Io(
        std::io::Error::other("No space left on device")
    )));
}

#[tokio::test]
async fn object_reader_handoff_preserves_requested_roots_without_blocking_unrelated_gc() {
    for local in [false, true] {
        for marked in [false, true] {
            for cancel in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let reader = if local {
                    crate::Repository::local(directory.path()).await.unwrap()
                } else {
                    crate::Repository::memory().unwrap()
                };
                let collector = if local {
                    crate::Repository::local(directory.path()).await.unwrap()
                } else {
                    reader.clone()
                };
                let held_name = "held".parse().unwrap();
                let garbage_name = "garbage".parse().unwrap();
                let expected = b"read through the catalog held before collection";
                let held = reader
                    .import(crate::import::BlobImport::new(&expected[..], held_name))
                    .await
                    .unwrap();
                let garbage = reader
                    .import(crate::import::BlobImport::new(
                        &b"unrelated garbage"[..],
                        garbage_name,
                    ))
                    .await
                    .unwrap();
                reader
                    .remove_root(&"garbage".parse().unwrap(), &garbage)
                    .await
                    .unwrap();
                if !marked {
                    reader
                        .remove_root(&"held".parse().unwrap(), &held)
                        .await
                        .unwrap();
                }
                reader.flush().await.unwrap();
                let core = &collector.inner;
                let plan = core
                    .collection_plan(
                        core.coordination.clone().lock_owned().await,
                        core.exclusive_fs().await.unwrap(),
                        true,
                    )
                    .await
                    .unwrap();
                let (entered, admitted) = tokio::sync::oneshot::channel();
                let (resume, resumed) = tokio::sync::oneshot::channel();
                let opening = tokio::spawn({
                    let reader = reader.clone();
                    let held = held.clone();
                    async move {
                        let mut profile = ObjectReadProfile {
                            pause_after_pin: Some((entered, resumed)),
                            ..Default::default()
                        };
                        reader.inner.open_object_inner(&held, &mut profile).await
                    }
                });
                tokio::time::timeout(std::time::Duration::from_secs(5), admitted)
                    .await
                    .unwrap()
                    .unwrap();
                let ledger = core.metadata().pin_store().await.unwrap();
                let inventory = ledger.inventory().await.unwrap();
                assert!(
                    inventory.pins.values().all(|pin| !matches!(
                        pin.scope,
                        crate::metadata::PinScope::Snapshot { .. }
                    ))
                );
                let temporary = inventory
                    .pins
                    .values()
                    .find(|pin| {
                        pin.scope
                            == crate::metadata::PinScope::Closures(BTreeSet::from([held.clone()]))
                    })
                    .unwrap();
                if local {
                    assert!(
                        temporary.catalog.is_some(),
                        "catalog remains protected through handoff"
                    );
                }
                let outcome = tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    core.execute_collection(plan, false),
                )
                .await
                .unwrap();
                if marked {
                    assert_eq!(outcome.unwrap().removed.logical_objects, 1);
                    assert!(collector.object(&garbage).await.unwrap().is_none());
                } else {
                    assert!(
                        matches!(outcome, Err(RepositoryError::Busy(reason)) if reason == "logical pin protects existing unmarked object")
                    );
                    assert!(collector.object(&held).await.unwrap().is_some());
                }
                if cancel {
                    opening.abort();
                    assert!(matches!(opening.await, Err(error) if error.is_cancelled()));
                    drop(resume);
                } else {
                    resume.send(()).unwrap();
                    let (_, mut stream) = opening.await.unwrap().unwrap().unwrap();
                    let mut actual = Vec::new();
                    tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut actual)
                        .await
                        .unwrap();
                    assert_eq!(actual, expected);
                    drop(stream);
                }
                reader.flush().await.unwrap();
                if marked {
                    collector
                        .remove_root(&"held".parse().unwrap(), &held)
                        .await
                        .unwrap();
                }
                collector.flush().await.unwrap();
                let removed = collector.collect().await.unwrap().logical_objects;
                assert_eq!(removed, if marked { 1 } else { 2 });
                collector.flush().await.unwrap();
                let inventory = ledger.inventory().await.unwrap();
                assert!(inventory.pins.is_empty() && inventory.deletions.is_empty());
                assert!(inventory.collector.is_none() && inventory.logical_prune.is_none());
                assert!(collector.open(&held).await.unwrap().is_none());
            }
        }
    }
}

fn graph_repository() -> Repository<MemoryBlobStore, MemoryMetadataStore> {
    Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        FormatRegistry::new([Arc::new(SyntheticFormat::default()) as Arc<dyn ObjectFormat>])
            .unwrap(),
        FormatLimits::default(),
    )
}

struct CollectOnMutationStart {
    repository: Repository<MemoryBlobStore, MemoryMetadataStore>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl MutationStart for CollectOnMutationStart {
    async fn before_mutation(&self, _spill_limits: SpillLimits) -> Result<bool, RepositoryError> {
        self.repository.try_collect().await?;
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(false)
    }
}

#[derive(Clone)]
struct StaleOnceMetadataStore {
    inner: MemoryMetadataStore,
    stale_once: Arc<AtomicBool>,
}

#[derive(Clone)]
struct FailingCommitMetadataStore<SS> {
    inner: SS,
    fail_once: Arc<AtomicBool>,
}

#[derive(Clone)]
struct StorageFullOnceMetadataStore {
    inner: MemoryMetadataStore,
    fail_once: Arc<AtomicBool>,
}

#[derive(Clone)]
struct InterruptedEmergencyMetadataStore {
    inner: MemoryMetadataStore,
    failures: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct FailingDeleteStore<BS> {
    inner: BS,
}

/// A physically shared backend that deliberately does not implement
/// `BlobGc`, as a remote shared backend should not let one repository
/// choose physical deletion.
#[derive(Clone)]
struct SharedBlobStore {
    inner: MemoryBlobStore,
}

#[derive(Clone)]
struct HiddenObjectMetadataStore {
    inner: MemoryMetadataStore,
    hidden: Arc<StdRwLock<Option<ObjectKey>>>,
}

#[derive(Clone)]
struct CountingBlobStore {
    inner: MemoryBlobStore,
    reads: Arc<AtomicUsize>,
}

#[derive(Clone, Copy)]
enum WriterLie {
    Digest,
    Size,
}

#[derive(Clone)]
struct LyingWriterStore {
    inner: MemoryBlobStore,
    lie: WriterLie,
}

struct LyingWriter {
    inner: Box<dyn BlobWriter>,
    lie: WriterLie,
}

impl tokio::io::AsyncWrite for LyingWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut *self.inner).poll_write(cx, buffer)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut *self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut *self.inner).poll_shutdown(cx)
    }
}

#[async_trait]
impl BlobWriter for LyingWriter {
    async fn close(&mut self) -> Result<(BlobId, u64), crate::error::Error> {
        let (digest, size) = self.inner.close().await?;
        Ok(match self.lie {
            WriterLie::Digest => (BlobId::new(Digest::hash(b"writer-lie")), size),
            WriterLie::Size => (digest, size.saturating_add(1)),
        })
    }
}

struct HiddenObjectSnapshot {
    inner: Arc<dyn MetadataSnapshot>,
    hidden: Option<ObjectKey>,
}

#[async_trait]
impl MetadataSnapshot for HiddenObjectSnapshot {
    fn generation(&self) -> Result<u64, MetadataError> {
        self.inner.generation()
    }
    fn objects_created_through(
        &self,
        generation: u64,
    ) -> futures::stream::BoxStream<'static, Result<ObjectRecord, MetadataError>> {
        let hidden = self.hidden.clone();
        Box::pin(
            self.inner
                .objects_created_through(generation)
                .filter_map(move |record| {
                    let hidden = hidden.clone();
                    async move {
                        match record {
                            Ok(record) if Some(record.key()) == hidden.as_ref() => None,
                            other => Some(other),
                        }
                    }
                }),
        )
    }

    fn revision(&self) -> crate::RepositoryRevision {
        self.inner.revision()
    }

    fn retention_resources(&self) -> std::collections::BTreeSet<crate::metadata::PinResource> {
        self.inner.retention_resources()
    }

    async fn object(&self, key: &ObjectKey) -> Result<Option<ObjectRecord>, MetadataError> {
        if self.hidden.as_ref() == Some(key) {
            Ok(None)
        } else {
            self.inner.object(key).await
        }
    }

    async fn root(&self, name: &RootName) -> Result<Option<ObjectKey>, MetadataError> {
        self.inner.root(name).await
    }

    fn objects(&self) -> BoxStream<'static, Result<ObjectRecord, MetadataError>> {
        let hidden = self.hidden.clone();
        Box::pin(self.inner.objects().filter_map(move |record| {
            let hidden = hidden.clone();
            async move {
                match record {
                    Ok(record) if Some(record.key()) == hidden.as_ref() => None,
                    other => Some(other),
                }
            }
        }))
    }

    fn roots(&self) -> BoxStream<'static, Result<crate::RootRecord, MetadataError>> {
        self.inner.roots()
    }
}

#[async_trait]
impl MetadataStore for HiddenObjectMetadataStore {
    async fn try_collection_lease(
        &self,
    ) -> Result<Option<crate::metadata::RepositoryLease>, crate::metadata::MetadataError> {
        self.inner.try_collection_lease().await
    }
    fn coordinates_payload_catalog(&self) -> bool {
        self.inner.coordinates_payload_catalog()
    }
    async fn pin_store(
        &self,
    ) -> Result<std::sync::Arc<dyn crate::metadata::PinStore>, crate::metadata::MetadataError> {
        self.inner.pin_store().await
    }

    async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
        let hidden = self
            .hidden
            .read()
            .map_err(|_| MetadataError::Poisoned)?
            .clone();
        Ok(Arc::new(HiddenObjectSnapshot {
            inner: self.inner.snapshot().await?,
            hidden,
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

#[async_trait]
impl<BS: BlobStore> BlobStore for FailingDeleteStore<BS> {
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
impl BlobStore for SharedBlobStore {
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
impl BlobStore for CountingBlobStore {
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
        self.reads.fetch_add(1, Ordering::SeqCst);
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
impl BlobStore for LyingWriterStore {
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

    async fn overwrite(
        &self,
        digest: &BlobId,
        size: u64,
        offset: u64,
        replacement: &[u8],
    ) -> Result<(BlobId, bytes::Bytes), crate::error::Error> {
        let (actual, proof) = self
            .inner
            .overwrite(digest, size, offset, replacement)
            .await?;
        Ok(match self.lie {
            WriterLie::Digest => (BlobId::new(Digest::hash(b"lying overwrite")), proof),
            WriterLie::Size => (
                actual,
                bytes::Bytes::from_static(b"invalid old-content proof"),
            ),
        })
    }

    async fn open_write(&self) -> Box<dyn BlobWriter> {
        Box::new(LyingWriter {
            inner: self.inner.open_write().await,
            lie: self.lie,
        })
    }
}

#[async_trait]
impl<BS: BlobStore + BlobGc> BlobGc for FailingDeleteStore<BS> {
    async fn reclaim_metadata_pinned(
        &self,
        pins: Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
    ) -> Result<(), crate::error::Error> {
        self.inner.reclaim_metadata_pinned(pins, owned_claims).await
    }

    fn list_blobs(&self) -> BoxStream<'_, Result<BlobId, crate::error::Error>> {
        self.inner.list_blobs()
    }

    fn list_chunks(&self) -> BoxStream<'_, Result<ChunkId, crate::error::Error>> {
        self.inner.list_chunks()
    }

    async fn delete_blob(&self, _digest: &BlobId) -> Result<(), crate::error::Error> {
        Err(crate::error::Error::Msg(
            "injected physical deletion failure".to_owned(),
        ))
    }

    async fn delete_chunk(&self, _digest: &ChunkId) -> Result<(), crate::error::Error> {
        Err(crate::error::Error::Msg(
            "injected physical deletion failure".to_owned(),
        ))
    }

    async fn delete_blobs_pinned(
        &self,
        _digests: &[BlobId],
        _pins: Arc<dyn crate::metadata::PinStore>,
        _owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
        _before_prune: bool,
    ) -> Result<usize, crate::error::Error> {
        Err(crate::error::Error::Msg(
            "injected physical deletion failure".to_owned(),
        ))
    }

    async fn delete_chunks_pinned(
        &self,
        _digests: &[ChunkId],
        _pins: Arc<dyn crate::metadata::PinStore>,
        _owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
    ) -> Result<usize, crate::error::Error> {
        Err(crate::error::Error::Msg(
            "injected physical deletion failure".to_owned(),
        ))
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

#[async_trait]
impl<SS: MetadataStore> MetadataStore for FailingCommitMetadataStore<SS> {
    async fn try_collection_lease(
        &self,
    ) -> Result<Option<crate::metadata::RepositoryLease>, crate::metadata::MetadataError> {
        self.inner.try_collection_lease().await
    }
    async fn pin_store(
        &self,
    ) -> Result<std::sync::Arc<dyn crate::metadata::PinStore>, crate::metadata::MetadataError> {
        self.inner.pin_store().await
    }

    fn coordinates_payload_catalog(&self) -> bool {
        self.inner.coordinates_payload_catalog()
    }

    async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
        self.inner.snapshot().await
    }

    async fn commit(
        &self,
        expected: &crate::RepositoryRevision,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        if self.fail_once.swap(false, Ordering::SeqCst) {
            return Err(MetadataError::Backend(
                "injected failure before logical commit".to_owned(),
            ));
        }
        self.inner.commit(expected, mutation).await
    }
}

#[async_trait]
impl MetadataStore for StorageFullOnceMetadataStore {
    async fn try_collection_lease(
        &self,
    ) -> Result<Option<crate::metadata::RepositoryLease>, crate::metadata::MetadataError> {
        self.inner.try_collection_lease().await
    }
    async fn pin_store(
        &self,
    ) -> Result<std::sync::Arc<dyn crate::metadata::PinStore>, crate::metadata::MetadataError> {
        self.inner.pin_store().await
    }

    fn coordinates_payload_catalog(&self) -> bool {
        self.inner.coordinates_payload_catalog()
    }

    async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
        self.inner.snapshot().await
    }

    async fn commit(
        &self,
        expected: &crate::RepositoryRevision,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        if self.fail_once.swap(false, Ordering::SeqCst) {
            return Err(MetadataError::StorageFull);
        }
        self.inner.commit(expected, mutation).await
    }
}

#[async_trait]
impl MetadataStore for InterruptedEmergencyMetadataStore {
    async fn try_collection_lease(
        &self,
    ) -> Result<Option<crate::metadata::RepositoryLease>, crate::metadata::MetadataError> {
        self.inner.try_collection_lease().await
    }
    async fn pin_store(
        &self,
    ) -> Result<std::sync::Arc<dyn crate::metadata::PinStore>, crate::metadata::MetadataError> {
        self.inner.pin_store().await
    }

    fn coordinates_payload_catalog(&self) -> bool {
        self.inner.coordinates_payload_catalog()
    }

    async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
        self.inner.snapshot().await
    }

    async fn commit(
        &self,
        expected: &crate::RepositoryRevision,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        match self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_sub(1)
            }) {
            Ok(2) => Err(MetadataError::StorageFull),
            Ok(1) => Err(MetadataError::Backend(
                "injected failure after emergency sweep".to_owned(),
            )),
            _ => self.inner.commit(expected, mutation).await,
        }
    }
}

#[async_trait]
impl MetadataStore for StaleOnceMetadataStore {
    async fn try_collection_lease(
        &self,
    ) -> Result<Option<crate::metadata::RepositoryLease>, crate::metadata::MetadataError> {
        self.inner.try_collection_lease().await
    }
    fn coordinates_payload_catalog(&self) -> bool {
        self.inner.coordinates_payload_catalog()
    }
    async fn pin_store(
        &self,
    ) -> Result<std::sync::Arc<dyn crate::metadata::PinStore>, crate::metadata::MetadataError> {
        self.inner.pin_store().await
    }

    async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
        self.inner.snapshot().await
    }

    async fn commit(
        &self,
        expected: &crate::RepositoryRevision,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        if self.stale_once.swap(false, Ordering::SeqCst) {
            let competing = self.inner.commit(expected, MetadataMutation::new()).await?;
            return Err(MetadataError::StaleRevision {
                expected: *expected,
                actual: competing.revision,
            });
        }
        self.inner.commit(expected, mutation).await
    }
}

async fn stage_graph<'hold, SS: MetadataStore>(
    mutation: &'hold MutationSession<'_, MemoryBlobStore, SS>,
    native: u8,
    links: &[u8],
) -> StagedObject<'hold> {
    let payload = mutation.repository.payloads.put_slice(links).await.unwrap();
    let key = ObjectKey::new("test.graph.v1".parse().unwrap(), Bytes::from(vec![native])).unwrap();
    mutation.stage_existing(key, payload).await.unwrap()
}

fn chunked_repository(
    average_chunk_size: u32,
) -> Repository<ChunkedBlobStore, MemoryMetadataStore> {
    Repository::new(
        ChunkedBlobStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            object_store::path::Path::default(),
            average_chunk_size,
        ),
        MemoryMetadataStore::new().unwrap(),
    )
}

#[derive(Clone, Debug)]
enum GcModelOperation {
    Set { name: u8, graph: u8 },
    Remove { name: u8 },
    Collect,
}

fn gc_model_strategy() -> impl Strategy<Value = GcModelOperation> {
    prop_oneof![
        3 => (0u8..3, 0u8..4)
            .prop_map(|(name, graph)| GcModelOperation::Set { name, graph }),
        1 => (0u8..3).prop_map(|name| GcModelOperation::Remove { name }),
        2 => Just(GcModelOperation::Collect),
    ]
}

fn gc_model_nodes(graph: u8) -> Vec<(u8, Vec<u8>)> {
    match graph {
        0 => vec![(1, vec![3]), (2, vec![]), (3, vec![]), (10, vec![1, 2])],
        1 => vec![(1, vec![3]), (3, vec![]), (4, vec![]), (11, vec![1, 4])],
        2 => vec![(2, vec![]), (5, vec![]), (12, vec![2, 5])],
        3 => vec![(6, vec![]), (7, vec![]), (13, vec![6, 7])],
        _ => unreachable!("the strategy emits four graphs"),
    }
}

fn gc_model_key(native: u8) -> ObjectKey {
    ObjectKey::new("test.graph.v1".parse().unwrap(), Bytes::from(vec![native])).unwrap()
}

fn gc_model_root(graph: u8) -> ObjectKey {
    let native = match graph {
        0 => 10,
        1 => 11,
        2 => 12,
        3 => 13,
        _ => unreachable!("the strategy emits four graphs"),
    };
    gc_model_key(native)
}

fn gc_model_closure(graph: u8) -> BTreeSet<ObjectKey> {
    gc_model_nodes(graph)
        .into_iter()
        .map(|(native, _)| gc_model_key(native))
        .collect()
}

async fn run_gc_model(operations: &[GcModelOperation]) -> Result<(), String> {
    let repository = graph_repository();
    let mut roots = BTreeMap::<u8, u8>::new();

    for (index, operation) in operations.iter().enumerate() {
        match *operation {
            GcModelOperation::Set { name, graph } => {
                let mutation = repository
                    .mutation_session()
                    .await
                    .map_err(|error| format!("operation {index} mutation: {error}"))?;
                let mut staged = Vec::new();
                for (native, links) in gc_model_nodes(graph) {
                    staged.push(stage_graph(&mutation, native, &links).await);
                }
                mutation
                    .publish_rooted(
                        staged,
                        RootName::try_from(format!("model/{name}")).unwrap(),
                        gc_model_root(graph),
                    )
                    .await
                    .map_err(|error| format!("operation {index} set: {error}"))?;
                roots.insert(name, graph);
            }
            GcModelOperation::Remove { name } => {
                repository
                    .mutation_session()
                    .await
                    .map_err(|error| format!("operation {index} mutation: {error}"))?
                    .publish(
                        Vec::new(),
                        vec![RootChange::Remove {
                            name: RootName::try_from(format!("model/{name}")).unwrap(),
                        }],
                    )
                    .await
                    .map_err(|error| format!("operation {index} remove: {error}"))?;
                roots.remove(&name);
            }
            GcModelOperation::Collect => {
                repository
                    .collect()
                    .await
                    .map_err(|error| format!("operation {index} collection: {error}"))?;
                let expected_keys: BTreeSet<_> = roots
                    .values()
                    .flat_map(|graph| gc_model_closure(*graph))
                    .collect();
                let snapshot = repository
                    .metadata()
                    .snapshot()
                    .await
                    .map_err(|error| format!("operation {index} snapshot: {error}"))?;
                let records = snapshot
                    .objects()
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(|error| format!("operation {index} records: {error}"))?;
                let actual_keys: BTreeSet<_> =
                    records.iter().map(|record| record.key().clone()).collect();
                if actual_keys != expected_keys {
                    return Err(format!(
                        "operation {index} ({operation:?}) retained {actual_keys:?}, expected {expected_keys:?}"
                    ));
                }

                let expected_payloads: BTreeSet<_> =
                    records.iter().map(ObjectRecord::payload).collect();
                let actual_payloads: BTreeSet<_> = repository
                    .payloads()
                    .list_blobs()
                    .try_collect()
                    .await
                    .map_err(|error| format!("operation {index} payloads: {error}"))?;
                if actual_payloads != expected_payloads {
                    return Err(format!(
                        "operation {index} ({operation:?}) retained payloads {actual_payloads:?}, expected {expected_payloads:?}"
                    ));
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn logical_collection_retains_a_pin_and_collects_unrelated_records() {
    use crate::metadata::{DataPin, DataPinLease, PinScope};

    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let held = mutation.stage_blob(b"held").await.unwrap();
    let key = held.record().key().clone();
    let stale = mutation.stage_blob(b"stale").await.unwrap();
    let stale_key = stale.record().key().clone();
    mutation.publish_unrooted(vec![held, stale]).await.unwrap();
    drop(mutation);
    crate::flush_repository_leases().await.unwrap();
    let pin = DataPinLease::acquire(
        repository.metadata().pin_store().await.unwrap(),
        DataPin {
            scope: PinScope::Closures(BTreeSet::from([key.clone()])),
            catalog: None,
            resources: BTreeSet::new(),
        },
    )
    .await
    .unwrap();
    let collected = repository.collect_logical().await.unwrap();
    assert_eq!(collected.removed.logical_objects, 1);
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert!(snapshot.object(&key).await.unwrap().is_some());
    assert!(snapshot.object(&stale_key).await.unwrap().is_none());
    drop(snapshot);
    drop(pin);
    crate::flush_repository_leases().await.unwrap();
    assert_eq!(
        repository
            .collect_logical()
            .await
            .unwrap()
            .removed
            .logical_objects,
        1
    );
}

#[tokio::test]
async fn collection_progress_distinguishes_snapshot_protection_from_idle_writers() {
    let repository = repository();
    let seed = repository.mutation_session().await.unwrap();
    let sentinel = seed.stage_blob(b"sentinel").await.unwrap();
    let key = sentinel.record().key().clone();
    seed.publish_rooted(vec![sentinel], "sentinel".parse().unwrap(), key.clone())
        .await
        .unwrap();
    drop(seed);
    crate::flush_repository_leases().await.unwrap();
    // Keep a reader and writer alive for the entire sequence. Neither owns
    // the subsequently created garbage.
    let old = repository.retention_hold().await.unwrap();
    let idle = repository.mutation_session().await.unwrap();
    for iteration in 0..4 {
        let writer = repository.mutation_session().await.unwrap();
        let garbage = writer.stage_blob(&[iteration; 4096]).await.unwrap();
        let payload = garbage.record().payload();
        writer.publish_unrooted(vec![garbage]).await.unwrap();
        drop(writer);
        crate::flush_repository_leases().await.unwrap();
        let fresh = repository.retention_hold().await.unwrap();
        assert_eq!(
            repository.collect().await.unwrap().removed.logical_objects,
            0
        );
        assert!(repository.payloads().has(&payload).await.unwrap());
        drop(fresh);
        crate::flush_repository_leases().await.unwrap();
        let collected = repository.collect().await.unwrap();
        assert_eq!(collected.removed.logical_objects, 1);
        assert_eq!(collected.removed.payload_blobs, 1);
        assert!(!repository.payloads().has(&payload).await.unwrap());
        let (_, mut reader) = old.open_payload(&key).await.unwrap().unwrap();
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"sentinel");
    }
    drop((old, idle));
    crate::flush_repository_leases().await.unwrap();
}

#[tokio::test]
async fn physical_only_writer_growth_allows_collection_without_metadata_changes() {
    check_physical_growth(repository(), false, false).await;
    check_physical_growth(chunked_repository(1024), true, false).await;
    check_physical_growth(chunked_repository(1024), true, true).await;
}

async fn check_physical_growth<PS: BlobGc + 'static>(
    repository: Repository<PS, MemoryMetadataStore>,
    chunked: bool,
    manifest_only: bool,
) {
    // Memory metadata isolates this conflict from Turso and database locks.
    let bytes: Vec<u8> = (0..8192).map(|i| (i % 251) as u8).collect();
    let seed = repository.mutation_session().await.unwrap();
    let garbage = seed.stage_blob(&bytes).await.unwrap();
    let payload = garbage.record().payload();
    seed.publish_unrooted(vec![garbage]).await.unwrap();
    drop(seed);
    crate::flush_repository_leases().await.unwrap();
    let writer = repository.mutation_session().await.unwrap();
    let plan = repository
        .collection_plan(
            repository.coordination.clone().lock_owned().await,
            None,
            true,
        )
        .await
        .unwrap();
    assert_eq!(plan.preview.logical_objects, 1);
    if chunked {
        assert!(plan.preview.chunks > 1);
    }
    let revision = plan.logical.snapshot_revision;
    // No logical publication, new closure, or snapshot hold is involved.
    let staged = if manifest_only {
        // Protect only the manifest identity: sweep must discover its chunks
        // from this late pin instead of relying on explicit chunk pins.
        writer
            .pin
            .protect(BTreeSet::from([crate::metadata::PinResource::Blob(
                payload,
            )]))
            .await
            .unwrap();
        None
    } else {
        Some(writer.stage_blob(&bytes).await.unwrap())
    };
    let inventory = repository
        .metadata()
        .pin_store()
        .await
        .unwrap()
        .inventory()
        .await
        .unwrap();
    assert!(inventory.pins.values().all(|pin| {
        pin.scope == crate::metadata::PinScope::Staging
            && pin
                .resources
                .iter()
                .all(|resource| !matches!(resource, crate::metadata::PinResource::Object(_)))
    }));
    assert_eq!(
        repository.metadata().snapshot().await.unwrap().revision(),
        revision
    );
    // Reusing bytes already selected as garbage must retain them physically
    // while allowing the old unrooted logical record to be pruned.
    let result = repository.execute_collection(plan, false).await.unwrap();
    assert_eq!(result.removed.logical_objects, 1);
    assert_eq!(result.removed.payload_blobs, 0);
    assert!(repository.payloads().has(&payload).await.unwrap());
    let mut reader = repository
        .payloads()
        .open_read(&payload)
        .await
        .unwrap()
        .unwrap();
    let mut recovered = Vec::new();
    reader.read_to_end(&mut recovered).await.unwrap();
    assert_eq!(recovered, bytes);
    drop(reader);
    assert_eq!(result.removed.chunks, 0);
    let staged = match staged {
        Some(staged) => staged,
        None => writer.stage_blob(&bytes).await.unwrap(),
    };
    writer.publish_unrooted(vec![staged]).await.unwrap();
    drop(writer);
    crate::flush_repository_leases().await.unwrap();
    assert!(repository.fsck().await.unwrap().is_healthy());
}

#[tokio::test]
async fn logical_prune_fences_validation_and_cleans_up_after_error_or_cancellation() {
    #[derive(Debug)]
    struct Gated {
        inner: Arc<dyn crate::metadata::RetainedObjects>,
        reached: tokio::sync::Notify,
        resume: tokio::sync::Notify,
        first: std::sync::atomic::AtomicBool,
        fail: bool,
    }
    #[async_trait]
    impl crate::metadata::RetainedObjects for Gated {
        fn len(&self) -> usize {
            self.inner.len()
        }
        async fn page(
            &self,
            after: Option<ObjectKey>,
            limit: usize,
        ) -> Result<Vec<ObjectKey>, MetadataError> {
            self.inner.page(after, limit).await
        }
        async fn contains(&self, key: &ObjectKey) -> Result<bool, MetadataError> {
            if self.first.swap(false, Ordering::SeqCst) {
                self.reached.notify_one();
                self.resume.notified().await;
                if self.fail {
                    return Err(MetadataError::Backend("validation lookup failed".into()));
                }
            }
            self.inner.contains(key).await
        }
    }
    for fail in [false, true] {
        for cancel in [false, true] {
            let repository = repository();
            let seed = repository.mutation_session().await.unwrap();
            let garbage = seed
                .stage_blob(b"garbage before fenced validation")
                .await
                .unwrap();
            let key = garbage.record().key().clone();
            seed.publish_unrooted(vec![garbage]).await.unwrap();
            drop(seed);
            crate::flush_repository_leases().await.unwrap();
            let plan = repository
                .logical_collection_plan(
                    repository.coordination.clone().lock_owned().await,
                    None,
                    true,
                )
                .await
                .unwrap();
            let ledger = repository.metadata().pin_store().await.unwrap();
            let new_pin = crate::metadata::DataPin {
                scope: crate::metadata::PinScope::Closures(BTreeSet::from([ObjectKey::blob(
                    BlobId::new(Digest::hash(b"unpublished root")),
                )])),
                catalog: None,
                resources: BTreeSet::new(),
            };
            let pin = crate::metadata::DataPinLease::acquire(ledger.clone(), new_pin.clone())
                .await
                .unwrap();
            let gated = Arc::new(Gated {
                inner: plan.live_objects.clone(),
                reached: tokio::sync::Notify::new(),
                resume: tokio::sync::Notify::new(),
                first: std::sync::atomic::AtomicBool::new(true),
                fail,
            });
            let publication = repository.publication.clone();
            let protection = plan.protection.clone();
            let expected = plan.snapshot_revision;
            let marked = plan.pins.clone();
            let retained = MetadataMutation::install_retained_source(gated.clone());
            let waiter = tokio::spawn(async move {
                publication
                    .prune(
                        protection,
                        expected,
                        marked,
                        BTreeSet::new(),
                        None,
                        retained,
                    )
                    .await
            });
            tokio::time::timeout(Duration::from_secs(5), gated.reached.notified())
                .await
                .unwrap();
            assert!(ledger.inventory().await.unwrap().logical_prune.is_some());
            assert!(ledger.register(new_pin.clone()).await.unwrap().is_none());
            if cancel {
                waiter.abort();
                assert!(waiter.await.unwrap_err().is_cancelled());
                // Cancellation of the caller cannot open admission while
                // the tracked validation task still owns the fence.
                assert!(ledger.inventory().await.unwrap().logical_prune.is_some());
            } else {
                gated.resume.notify_one();
                let result = waiter.await.unwrap();
                if fail {
                    assert!(
                        matches!(result, Err(RepositoryError::Metadata(MetadataError::Backend(ref message))) if message == "validation lookup failed")
                    );
                } else {
                    result.unwrap();
                }
            }
            if cancel {
                gated.resume.notify_one();
            }
            crate::flush_repository_leases().await.unwrap();
            assert!(ledger.inventory().await.unwrap().logical_prune.is_none());
            let admitted = ledger.register(new_pin).await.unwrap().unwrap();
            ledger.release(&admitted).await.unwrap();
            assert_eq!(
                repository
                    .metadata()
                    .snapshot()
                    .await
                    .unwrap()
                    .object(&key)
                    .await
                    .unwrap()
                    .is_some(),
                fail
            );
            drop((pin, plan));
            crate::flush_repository_leases().await.unwrap();
            assert!(ledger.inventory().await.unwrap().collector.is_none());
        }
    }
}

#[tokio::test]
async fn collection_accepts_unpublished_roots_but_rejects_concurrent_publication() {
    for published in [false, true] {
        let repository = repository();
        let seed = repository.mutation_session().await.unwrap();
        let old = seed.stage_blob(b"old garbage").await.unwrap();
        seed.publish_unrooted(vec![old]).await.unwrap();
        drop(seed);
        crate::flush_repository_leases().await.unwrap();
        let plan = repository
            .collection_plan(
                repository.coordination.clone().lock_owned().await,
                None,
                true,
            )
            .await
            .unwrap();
        let writer = repository.mutation_session().await.unwrap();
        let staged = writer.stage_blob(b"new data").await.unwrap();
        let key = staged.record().key().clone();
        let payload = staged.record().payload();
        writer
            .pin
            .protect(BTreeSet::from([crate::metadata::PinResource::Object(
                key.clone(),
            )]))
            .await
            .unwrap();
        assert!(
            plan.logical
                .snapshot
                .as_ref()
                .expect("mark snapshot")
                .object(&key)
                .await
                .unwrap()
                .is_none()
        );
        let mut staged = Some(staged);
        if published {
            writer
                .publish_unrooted(vec![staged.take().unwrap()])
                .await
                .unwrap();
        }
        let result = repository.execute_collection(plan, false).await;
        if published {
            assert!(matches!(
                result,
                Err(RepositoryError::Metadata(
                    MetadataError::StaleRevision { .. }
                ))
            ));
            assert_eq!(
                repository.collect().await.unwrap().removed.logical_objects,
                1
            );
        } else {
            assert_eq!(result.unwrap().removed.logical_objects, 1);
        }
        assert!(repository.payloads().has(&payload).await.unwrap());
        if let Some(staged) = staged {
            writer.publish_unrooted(vec![staged]).await.unwrap();
        }
        drop(writer);
        crate::flush_repository_leases().await.unwrap();
        assert!(repository.fsck().await.unwrap().is_healthy());
    }
}

#[tokio::test]
async fn collection_accepts_new_pins_only_for_already_marked_objects() {
    use crate::metadata::{DataPin, DataPinLease, PinResource, PinScope};
    for covered in [true, false] {
        for explicit_object in [false, true] {
            let repository = repository();
            let seed = repository.mutation_session().await.unwrap();
            let kept = seed.stage_blob(b"rooted").await.unwrap();
            let key = kept.record().key().clone();
            seed.publish_rooted(vec![kept], "kept".parse().unwrap(), key.clone())
                .await
                .unwrap();
            let garbage = seed.stage_blob(b"garbage").await.unwrap();
            let garbage_key = garbage.record().key().clone();
            let garbage_payload = garbage.record().payload();
            seed.publish_unrooted(vec![garbage]).await.unwrap();
            drop(seed);
            crate::flush_repository_leases().await.unwrap();
            let plan = repository
                .collection_plan(
                    repository.coordination.clone().lock_owned().await,
                    None,
                    true,
                )
                .await
                .unwrap();
            assert_eq!(plan.preview.logical_objects, 1);
            let target = if covered { key.clone() } else { garbage_key };
            // Put an absent root first: accepting it must not skip checking
            // the existing root later in this same pin.
            let absent = (0u64..1024)
                .map(|i| ObjectKey::blob(BlobId::new(Digest::hash(&i.to_le_bytes()))))
                .find(|candidate| candidate < &target)
                .unwrap();
            assert!(
                repository
                    .metadata()
                    .snapshot()
                    .await
                    .unwrap()
                    .object(&absent)
                    .await
                    .unwrap()
                    .is_none()
            );
            let pin = DataPinLease::acquire(
                repository.metadata().pin_store().await.unwrap(),
                DataPin {
                    scope: if explicit_object {
                        PinScope::Staging
                    } else {
                        PinScope::Closures(BTreeSet::from([absent.clone(), target.clone()]))
                    },
                    catalog: None,
                    resources: if explicit_object {
                        BTreeSet::from([PinResource::Object(absent), PinResource::Object(target)])
                    } else {
                        BTreeSet::new()
                    },
                },
            )
            .await
            .unwrap();
            let result = repository.execute_collection(plan, false).await;
            if covered {
                assert_eq!(result.unwrap().removed.logical_objects, 1);
                assert!(!repository.payloads().has(&garbage_payload).await.unwrap());
            } else {
                assert!(matches!(result, Err(RepositoryError::Busy(_))));
                assert!(repository.payloads().has(&garbage_payload).await.unwrap());
            }
            let (_, mut reader) = repository.open_payload(&key).await.unwrap().unwrap();
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, b"rooted");
            drop((reader, pin));
            crate::flush_repository_leases().await.unwrap();
        }
    }
}

#[tokio::test]
async fn collection_progress_survives_duplicate_readers_after_marking() {
    for new_protection in [false, true] {
        let repository = repository();
        let writer = repository.mutation_session().await.unwrap();
        let kept = writer.stage_blob(b"kept").await.unwrap();
        let key = kept.record().key().clone();
        let garbage = writer.stage_blob(b"garbage").await.unwrap();
        let garbage_key = garbage.record().key().clone();
        let payload = garbage.record().payload();
        writer.publish_unrooted(vec![kept, garbage]).await.unwrap();
        drop(writer);
        crate::flush_repository_leases().await.unwrap();
        let original = repository.open_payload(&key).await.unwrap().unwrap();
        let plan = repository
            .collection_plan(
                repository.coordination.clone().lock_owned().await,
                None,
                true,
            )
            .await
            .unwrap();
        assert_eq!(plan.preview.logical_objects, 1);
        let mut readers = Vec::new();
        for _ in 0..32 {
            readers.push(repository.open_payload(&key).await.unwrap().unwrap());
        }
        // Retired identical pins must also be harmless.
        drop(original);
        crate::flush_repository_leases().await.unwrap();
        if new_protection {
            readers.push(
                repository
                    .open_payload(&garbage_key)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        let result = repository.execute_collection(plan, false).await;
        if new_protection {
            assert!(matches!(result, Err(RepositoryError::Busy(_))));
            assert!(repository.payloads().has(&payload).await.unwrap());
        } else {
            assert_eq!(result.unwrap().removed.logical_objects, 1);
            assert!(!repository.payloads().has(&payload).await.unwrap());
        }
        for (_, mut reader) in readers {
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            assert!(bytes == b"kept" || (new_protection && bytes == b"garbage"));
        }
        crate::flush_repository_leases().await.unwrap();
    }
}

#[tokio::test]
async fn collection_progress_survives_idle_writers_admitted_after_marking() {
    for acquire_resource in [false, true] {
        let repository = repository();
        let writer = repository.mutation_session().await.unwrap();
        let garbage = writer.stage_blob(b"unreachable").await.unwrap();
        let key = garbage.record().key().clone();
        let payload = garbage.record().payload();
        writer.publish_unrooted(vec![garbage]).await.unwrap();
        drop(writer);
        crate::flush_repository_leases().await.unwrap();
        let plan = repository
            .collection_plan(
                repository.coordination.clone().lock_owned().await,
                None,
                true,
            )
            .await
            .unwrap();
        assert_eq!(plan.preview.logical_objects, 1);
        // Deterministically place admission churn between mark and prune.
        let mut idle = Vec::new();
        for _ in 0..32 {
            idle.push(repository.mutation_session().await.unwrap());
        }
        if acquire_resource {
            idle[0]
                .pin
                .protect(BTreeSet::from([
                    crate::metadata::PinResource::Object(key.clone()),
                    crate::metadata::PinResource::Blob(payload),
                ]))
                .await
                .unwrap();
        }
        let result = repository.execute_collection(plan, false).await;
        if acquire_resource {
            assert!(matches!(result, Err(RepositoryError::Busy(_))));
            assert!(repository.payloads().has(&payload).await.unwrap());
        } else {
            let result = result.unwrap();
            assert_eq!(result.removed.logical_objects, 1);
            assert_eq!(result.removed.payload_blobs, 1);
            assert!(!repository.payloads().has(&payload).await.unwrap());
        }
        drop(idle);
        crate::flush_repository_leases().await.unwrap();
    }
}

#[tokio::test]
async fn logical_collection_admits_metadata_reads_registered_after_marking() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let object = mutation
        .stage_blob(b"unreachable logical object")
        .await
        .unwrap();
    mutation.publish_unrooted(vec![object]).await.unwrap();
    drop(mutation);
    crate::flush_repository_leases().await.unwrap();
    let plan = repository
        .logical_collection_plan(
            repository.coordination.clone().lock_owned().await,
            None,
            true,
        )
        .await
        .unwrap();
    let metadata = crate::metadata::DataPinLease::acquire(
        repository.metadata().pin_store().await.unwrap(),
        crate::metadata::DataPin {
            scope: crate::metadata::PinScope::Metadata,
            catalog: None,
            resources: BTreeSet::from([crate::metadata::PinResource::MetadataObject(
                "checkpoint".into(),
            )]),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        repository
            .execute_logical_collection(plan)
            .await
            .unwrap()
            .removed
            .logical_objects,
        1
    );
    assert!(
        repository
            .metadata()
            .pin_store()
            .await
            .unwrap()
            .inventory()
            .await
            .unwrap()
            .pins
            .contains_key(metadata.token())
    );
}

#[tokio::test]
async fn logical_collection_rejects_a_pin_registered_after_marking() {
    use crate::metadata::{DataPin, DataPinLease, PinScope};

    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let held = mutation.stage_blob(b"late pin").await.unwrap();
    let key = held.record().key().clone();
    mutation.publish_unrooted(vec![held]).await.unwrap();
    drop(mutation);
    crate::flush_repository_leases().await.unwrap();
    let plan = repository
        .logical_collection_plan(
            repository.coordination.clone().lock_owned().await,
            None,
            true,
        )
        .await
        .unwrap();
    assert_eq!(plan.preview.logical_objects, 1);
    let revision = plan.snapshot_revision;
    let pin = DataPinLease::acquire(
        repository.metadata().pin_store().await.unwrap(),
        DataPin {
            scope: PinScope::Closures(BTreeSet::from([key.clone()])),
            catalog: None,
            resources: BTreeSet::new(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        repository.execute_logical_collection(plan).await,
        Err(RepositoryError::Busy(_))
    ));
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert_eq!(snapshot.revision(), revision);
    assert!(snapshot.object(&key).await.unwrap().is_some());
    drop(snapshot);
    drop(pin);
    crate::flush_repository_leases().await.unwrap();
    assert_eq!(
        repository
            .collect_logical()
            .await
            .unwrap()
            .removed
            .logical_objects,
        1
    );
}

#[tokio::test]
async fn pin_marks_retain_selected_closures_and_tolerate_unpublished_inputs() {
    use crate::metadata::{DataPin, PinResource, PinScope};

    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let child = mutation.stage_blob(b"child").await.unwrap();
    let child_key = child.record().key().clone();
    let unrelated = mutation.stage_blob(b"unrelated").await.unwrap();
    let unrelated_key = unrelated.record().key().clone();
    let tree = mutation
        .stage_directory(
            &Directory::try_from_iter([(
                PathComponent::try_from("child").unwrap(),
                Node::File {
                    digest: BlobId::new(child_key.native_digest().unwrap()),
                    size: 5,
                    executable: false,
                },
            )])
            .unwrap(),
        )
        .await
        .unwrap();
    let tree_key = tree.record().key().clone();
    mutation
        .publish_unrooted(vec![child, tree, unrelated])
        .await
        .unwrap();
    let absent = mutation.stage_blob(b"not yet published").await.unwrap();
    let absent_key = absent.record().key().clone();
    drop(absent);
    drop(mutation);
    crate::flush_repository_leases().await.unwrap();
    let snapshot = repository.metadata().snapshot().await.unwrap();
    let ledger = repository.metadata().pin_store().await.unwrap();

    for pin in [
        DataPin {
            scope: PinScope::Closures(BTreeSet::from([tree_key.clone(), absent_key.clone()])),
            catalog: None,
            resources: BTreeSet::new(),
        },
        DataPin {
            scope: PinScope::Staging,
            catalog: None,
            resources: BTreeSet::from([
                PinResource::Object(tree_key.clone()),
                PinResource::Object(absent_key.clone()),
            ]),
        },
        DataPin {
            scope: PinScope::Snapshot {
                generation: u64::MAX,
            },
            catalog: None,
            resources: BTreeSet::new(),
        },
    ] {
        let all = matches!(pin.scope, PinScope::Snapshot { .. });
        let token = ledger.register(pin).await.unwrap().unwrap();
        let area = repository.spill_area();
        let mut marked = mark_named_roots(snapshot.as_ref(), 100, &area)
            .await
            .unwrap();
        mark_pin_scopes(
            snapshot.as_ref(),
            &ledger.inventory().await.unwrap(),
            &mut marked,
            100,
            &area,
        )
        .await
        .unwrap();
        assert!(marked.contains(&tree_key).await.unwrap());
        assert!(marked.contains(&child_key).await.unwrap());
        assert!(!marked.contains(&absent_key).await.unwrap());
        assert_eq!(marked.contains(&unrelated_key).await.unwrap(), all);
        assert_eq!(marked.len(), if all { 3 } else { 2 });
        ledger.release(&token).await.unwrap();
    }
}

#[tokio::test]
async fn selected_graph_hold_protects_all_roots_and_releases_unrelated_objects() {
    let repository = repository();
    let session = repository.mutation_session().await.unwrap();
    let first = session.stage_blob(b"first selected").await.unwrap();
    let second = session.stage_blob(b"second selected").await.unwrap();
    let garbage = session.stage_blob(b"unrelated").await.unwrap();
    let roots = BTreeSet::from([first.record().key().clone(), second.record().key().clone()]);
    session
        .publish_unrooted(vec![first, second, garbage])
        .await
        .unwrap();
    drop(session);
    let hold = repository.retention_hold_for(&roots).await.unwrap();
    crate::flush_repository_leases().await.unwrap();
    assert_eq!(
        repository
            .try_collect()
            .await
            .unwrap()
            .removed
            .logical_objects,
        1
    );
    for key in roots {
        assert!(hold.object(&key).await.unwrap().is_some());
        assert!(matches!(
            hold.verify_closure(&key).await.unwrap(),
            ClosureStatus::Complete { .. }
        ));
    }
    drop(hold);
    crate::flush_repository_leases().await.unwrap();
    assert_eq!(
        repository
            .try_collect()
            .await
            .unwrap()
            .removed
            .logical_objects,
        2
    );
}

#[tokio::test]
async fn snapshot_admission_retries_when_metadata_files_change_at_the_same_revision() {
    struct RewritingMetadata {
        inner: MemoryMetadataStore,
        snapshots: AtomicUsize,
    }
    struct RewrittenSnapshot {
        inner: Arc<dyn MetadataSnapshot>,
        path: &'static str,
    }
    #[async_trait]
    impl MetadataSnapshot for RewrittenSnapshot {
        fn generation(&self) -> Result<u64, MetadataError> {
            self.inner.generation()
        }
        fn objects_created_through(
            &self,
            generation: u64,
        ) -> futures::stream::BoxStream<'static, Result<ObjectRecord, MetadataError>> {
            self.inner.objects_created_through(generation)
        }

        fn revision(&self) -> crate::RepositoryRevision {
            self.inner.revision()
        }
        fn retention_resources(&self) -> BTreeSet<crate::metadata::PinResource> {
            BTreeSet::from([crate::metadata::PinResource::MetadataObject(
                self.path.into(),
            )])
        }
        async fn object(&self, key: &ObjectKey) -> Result<Option<ObjectRecord>, MetadataError> {
            self.inner.object(key).await
        }
        async fn root(&self, name: &RootName) -> Result<Option<ObjectKey>, MetadataError> {
            self.inner.root(name).await
        }
        fn objects(
            &self,
        ) -> futures::stream::BoxStream<'static, Result<ObjectRecord, MetadataError>> {
            self.inner.objects()
        }
        fn roots(
            &self,
        ) -> futures::stream::BoxStream<'static, Result<crate::RootRecord, MetadataError>> {
            self.inner.roots()
        }
    }
    #[async_trait]
    impl MetadataStore for RewritingMetadata {
        async fn try_collection_lease(
            &self,
        ) -> Result<Option<crate::metadata::RepositoryLease>, crate::metadata::MetadataError>
        {
            self.inner.try_collection_lease().await
        }
        fn coordinates_payload_catalog(&self) -> bool {
            self.inner.coordinates_payload_catalog()
        }
        async fn pin_store(&self) -> Result<Arc<dyn crate::metadata::PinStore>, MetadataError> {
            self.inner.pin_store().await
        }
        async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
            let call = self.snapshots.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(RewrittenSnapshot {
                inner: self.inner.snapshot().await?,
                path: if call == 0 {
                    "old-shard"
                } else {
                    "rewritten-shard"
                },
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
    for (claimed, metadata_only) in [(false, false), (true, false), (false, true), (true, true)] {
        let repository = Repository::new(
            MemoryBlobStore::new(),
            RewritingMetadata {
                inner: MemoryMetadataStore::new().unwrap(),
                snapshots: AtomicUsize::new(0),
            },
        );
        let ledger = repository.metadata().pin_store().await.unwrap();
        let claim = if claimed {
            Some(
                ledger
                    .claim_deletions(
                        ledger.inventory().await.unwrap().revision,
                        BTreeSet::from([crate::metadata::PinResource::MetadataObject(
                            "old-shard".into(),
                        )]),
                    )
                    .await
                    .unwrap()
                    .unwrap(),
            )
        } else {
            None
        };
        let (resources, _hold, _metadata_pin) =
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                if metadata_only {
                    let (snapshot, pin) =
                        crate::metadata::read_snapshot(repository.metadata()).await?;
                    Ok::<_, MetadataError>((snapshot.retention_resources(), None, pin))
                } else {
                    let hold = repository.retention_hold().await.unwrap();
                    Ok((hold.snapshot().retention_resources(), Some(hold), None))
                }
            })
            .await
            .expect("admission must reload an obsolete claimed candidate")
            .unwrap();
        assert_eq!(
            repository.metadata().snapshots.load(Ordering::SeqCst),
            if claimed { 3 } else { 4 }
        );
        crate::metadata::flush_pin_releases().await;
        let inventory = repository
            .metadata()
            .pin_store()
            .await
            .unwrap()
            .inventory()
            .await
            .unwrap();
        assert_eq!(inventory.pins.len(), 1);
        if metadata_only {
            assert_eq!(
                inventory.pins.values().next().unwrap().scope,
                crate::metadata::PinScope::Metadata
            );
        }
        assert_eq!(inventory.pins.values().next().unwrap().resources, resources);
        assert!(inventory.pins.values().next().unwrap().resources.contains(
            &crate::metadata::PinResource::MetadataObject("rewritten-shard".into())
        ));
        if let Some(claim) = claim {
            assert!(inventory.deletions.contains_key(&claim));
            ledger.finish_deletions(&claim).await.unwrap();
        }
    }
}

#[tokio::test]
async fn payload_readers_keep_snapshot_pins_after_both_hold_forms_are_dropped() {
    use tokio::io::AsyncSeekExt;

    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let object = mutation.stage_blob(b"retained stream").await.unwrap();
    let key = object.record().key().clone();
    mutation
        .publish_rooted(
            vec![object],
            RootName::try_from("reader").unwrap(),
            key.clone(),
        )
        .await
        .unwrap();
    drop(mutation);
    crate::flush_repository_leases().await.unwrap();
    let ledger = repository.metadata().pin_store().await.unwrap();
    let revision = repository.metadata().snapshot().await.unwrap().revision();

    let borrowed = repository.retention_hold().await.unwrap();
    let owned = repository.owned_retention_hold().await.unwrap();
    let (_, mut first) = borrowed.open_payload(&key).await.unwrap().unwrap();
    let (_, mut second) = owned.open_payload(&key).await.unwrap().unwrap();
    drop(borrowed);
    drop(owned);
    crate::flush_repository_leases().await.unwrap();
    let inventory = ledger.inventory().await.unwrap();
    assert_eq!(inventory.pins.len(), 2);
    assert!(
        inventory
            .pins
            .values()
            .all(|pin| matches!(pin.scope, crate::metadata::PinScope::Snapshot { .. }))
    );
    assert_eq!(
        repository.metadata().snapshot().await.unwrap().revision(),
        revision
    );
    let mut bytes = Vec::new();
    first.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"retained stream");
    second.seek(std::io::SeekFrom::Start(9)).await.unwrap();
    bytes.clear();
    second.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"stream");
    drop(first);
    crate::flush_repository_leases().await.unwrap();
    assert_eq!(ledger.inventory().await.unwrap().pins.len(), 1);
    drop(second);
    crate::flush_repository_leases().await.unwrap();
    assert!(ledger.inventory().await.unwrap().pins.is_empty());
}

#[tokio::test]
async fn mutation_session_registers_staged_bytes_in_the_shared_pin_ledger() {
    let repository = repository();
    let state = repository.metadata();
    let revision = state.snapshot().await.unwrap().revision();
    let ledger = state.pin_store().await.unwrap();
    let mutation = repository.mutation_session().await.unwrap();
    let object = mutation
        .stage_blob(b"pending importer bytes")
        .await
        .unwrap();
    let inventory = ledger.inventory().await.unwrap();
    assert_eq!(inventory.pins.len(), 1);
    assert!(inventory.pins.values().next().unwrap().resources.contains(
        &crate::metadata::PinResource::Blob(object.record().payload()),
    ));
    assert_eq!(state.snapshot().await.unwrap().revision(), revision);
    drop(object);
    drop(mutation);
    crate::flush_repository_leases().await.unwrap();
    assert!(ledger.inventory().await.unwrap().pins.is_empty());
}

#[tokio::test]
async fn publishes_payload_record_and_root_atomically() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let object = mutation.stage_blob(b"hello").await.unwrap();
    let key = object.record().key().clone();
    mutation
        .publish_rooted(
            vec![object],
            RootName::try_from("profiles/main").unwrap(),
            key.clone(),
        )
        .await
        .unwrap();
    drop(mutation);

    assert!(matches!(
        repository.verify_closure(&key).await.unwrap(),
        ClosureStatus::Complete { .. }
    ));
}

/// The point of the shortcut: naming a graph that overlaps one already
/// verified must only re-read the part that is new.
#[tokio::test]
async fn an_incremental_walk_stops_at_an_already_verified_closure() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let mut staged = Vec::new();
    let mut entries = Vec::new();
    for index in 0..4u8 {
        let blob = mutation.stage_blob(&[index; 32]).await.unwrap();
        entries.push((
            PathComponent::try_from(format!("file{index}").as_str()).unwrap(),
            Node::File {
                digest: BlobId::new(blob.record().key().native_digest().unwrap()),
                size: 32,
                executable: false,
            },
        ));
        staged.push(blob);
    }
    let directory = Directory::try_from_iter(entries).unwrap();
    let tree = mutation.stage_directory(&directory).await.unwrap();
    let tree_key = tree.record().key().clone();
    staged.push(tree);
    mutation
        .publish_rooted(
            staged,
            RootName::try_from("profiles/first").unwrap(),
            tree_key.clone(),
        )
        .await
        .unwrap();
    drop(mutation);

    let hold = repository.retention_hold().await.unwrap();
    // An audit reads the directory and all four blobs, every time.
    assert!(matches!(
        hold.verify_closure(&tree_key).await.unwrap(),
        ClosureStatus::Complete { objects: 5 }
    ));
    // Verified once, so an incremental walk stops at the root and never
    // descends into what it already vouched for.
    assert!(matches!(
        hold.verify_closure_incremental(&tree_key).await.unwrap(),
        ClosureStatus::Complete { objects: 1 }
    ));
    let owned = repository.owned_retention_hold().await.unwrap();
    assert!(matches!(
        owned.verify_closure(&tree_key).await.unwrap(),
        ClosureStatus::Complete { objects: 5 }
    ));
    assert!(matches!(
        owned.verify_closure_incremental(&tree_key).await.unwrap(),
        ClosureStatus::Complete { objects: 1 }
    ));
}

/// The shortcut must never let an incomplete graph acquire a name, and a
/// mark must never outlive the object it vouches for.
#[tokio::test]
async fn an_incremental_walk_still_reports_an_absent_object() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let object = mutation.stage_blob(b"collected later").await.unwrap();
    let key = object.record().key().clone();
    let name = RootName::try_from("profiles/temporary").unwrap();
    mutation
        .publish_rooted(vec![object], name.clone(), key.clone())
        .await
        .unwrap();
    drop(mutation);

    // Unnamed and collected, the object is gone and so is its mark.
    repository
        .mutation_session()
        .await
        .unwrap()
        .remove_root_if_matches(&name, &key)
        .await
        .unwrap()
        .unwrap();
    repository.collect().await.unwrap();

    assert!(matches!(
        repository.verify_closure_incremental(&key).await.unwrap(),
        ClosureStatus::Missing { .. }
    ));
}

#[tokio::test]
async fn publication_revalidates_and_retries_a_revision_race() {
    let state = StaleOnceMetadataStore {
        inner: MemoryMetadataStore::new().unwrap(),
        stale_once: Arc::new(AtomicBool::new(true)),
    };
    let repository = Repository::new(MemoryBlobStore::new(), state);
    let mutation = repository.mutation_session().await.unwrap();
    let object = mutation.stage_blob(b"race-safe").await.unwrap();
    let key = object.record().key().clone();
    mutation
        .publish_rooted(
            vec![object],
            RootName::try_from("roots/raced").unwrap(),
            key.clone(),
        )
        .await
        .unwrap();
    drop(mutation);

    assert!(matches!(
        repository.verify_closure(&key).await.unwrap(),
        ClosureStatus::Complete { .. }
    ));
}

#[tokio::test]
async fn conditional_publication_creates_and_repoints_a_matching_root() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let name = RootName::try_from("replicated/workspace").unwrap();

    let first = mutation.stage_blob(b"first frontier").await.unwrap();
    let first_key = first.record().key().clone();
    let first_result = mutation
        .publish_if_roots_match(
            vec![first],
            vec![RootExpectation {
                name: name.clone(),
                target: None,
            }],
            vec![RootChange::Set {
                name: name.clone(),
                target: first_key.clone(),
            }],
        )
        .await
        .unwrap();
    assert!(matches!(
        first_result,
        ConditionalPublishResult::Committed(CommitResult {
            objects_inserted: 1,
            roots_changed: 1,
            ..
        })
    ));

    let second = mutation.stage_blob(b"second frontier").await.unwrap();
    let second_key = second.record().key().clone();
    let second_result = mutation
        .publish_if_roots_match(
            vec![second],
            vec![RootExpectation {
                name: name.clone(),
                target: Some(first_key),
            }],
            vec![RootChange::Set {
                name: name.clone(),
                target: second_key.clone(),
            }],
        )
        .await
        .unwrap();
    assert!(matches!(
        second_result,
        ConditionalPublishResult::Committed(CommitResult {
            objects_inserted: 1,
            roots_changed: 1,
            ..
        })
    ));
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .root(&name)
            .await
            .unwrap(),
        Some(second_key)
    );
}

#[tokio::test]
async fn conditional_root_mismatch_publishes_nothing() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let name = RootName::try_from("replicated/workspace").unwrap();
    let current = mutation.stage_blob(b"current frontier").await.unwrap();
    let current_key = current.record().key().clone();
    mutation
        .publish_rooted(vec![current], name.clone(), current_key.clone())
        .await
        .unwrap();

    let candidate = mutation.stage_blob(b"candidate frontier").await.unwrap();
    let candidate_key = candidate.record().key().clone();
    let candidate_payload = candidate.record().payload();
    let before = repository.metadata().snapshot().await.unwrap().revision();
    let wrong = ObjectKey::blob(BlobId::new(Digest::hash(b"wrong frontier")));
    let result = mutation
        .publish_if_roots_match(
            vec![candidate],
            vec![RootExpectation {
                name: name.clone(),
                target: Some(wrong.clone()),
            }],
            vec![RootChange::Set {
                name: name.clone(),
                target: candidate_key.clone(),
            }],
        )
        .await
        .unwrap();
    assert_eq!(
        result,
        ConditionalPublishResult::RootMismatch {
            name: name.clone(),
            expected: Some(wrong),
            actual: Some(current_key.clone()),
        }
    );

    let after = repository.metadata().snapshot().await.unwrap();
    assert_eq!(after.revision(), before);
    assert_eq!(after.root(&name).await.unwrap(), Some(current_key));
    assert!(after.object(&candidate_key).await.unwrap().is_none());
    assert!(repository.payloads().has(&candidate_payload).await.unwrap());
    drop(after);
    drop(mutation);
    repository.collect().await.unwrap();
    assert!(!repository.payloads().has(&candidate_payload).await.unwrap());
}

#[tokio::test]
async fn conditional_publication_checks_multiple_roots_atomically() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let first_name = RootName::try_from("replicated/a").unwrap();
    let second_name = RootName::try_from("replicated/z").unwrap();
    let current = mutation.stage_blob(b"current frontier").await.unwrap();
    let current_key = current.record().key().clone();
    mutation
        .publish_rooted(vec![current], first_name.clone(), current_key.clone())
        .await
        .unwrap();

    let first_candidate = mutation.stage_blob(b"first candidate").await.unwrap();
    let first_candidate_key = first_candidate.record().key().clone();
    let second_candidate = mutation.stage_blob(b"second candidate").await.unwrap();
    let second_candidate_key = second_candidate.record().key().clone();
    let wrong = ObjectKey::blob(BlobId::new(Digest::hash(b"wrong frontier")));
    let before = repository.metadata().snapshot().await.unwrap().revision();
    let result = mutation
        .publish_if_roots_match(
            vec![first_candidate, second_candidate],
            vec![
                RootExpectation {
                    name: second_name.clone(),
                    target: None,
                },
                RootExpectation {
                    name: first_name.clone(),
                    target: Some(wrong.clone()),
                },
            ],
            vec![
                RootChange::Set {
                    name: first_name.clone(),
                    target: first_candidate_key.clone(),
                },
                RootChange::Set {
                    name: second_name.clone(),
                    target: second_candidate_key.clone(),
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        result,
        ConditionalPublishResult::RootMismatch {
            name: first_name.clone(),
            expected: Some(wrong),
            actual: Some(current_key.clone()),
        }
    );

    let after = repository.metadata().snapshot().await.unwrap();
    assert_eq!(after.revision(), before);
    assert_eq!(after.root(&first_name).await.unwrap(), Some(current_key));
    assert_eq!(after.root(&second_name).await.unwrap(), None);
    assert!(after.object(&first_candidate_key).await.unwrap().is_none());
    assert!(after.object(&second_candidate_key).await.unwrap().is_none());
}

#[tokio::test]
async fn conditional_publication_retries_an_unrelated_revision_race() {
    let state = StaleOnceMetadataStore {
        inner: MemoryMetadataStore::new().unwrap(),
        stale_once: Arc::new(AtomicBool::new(true)),
    };
    let repository = Repository::new(MemoryBlobStore::new(), state);
    let mutation = repository.mutation_session().await.unwrap();
    let name = RootName::try_from("replicated/workspace").unwrap();
    let object = mutation.stage_blob(b"race-safe frontier").await.unwrap();
    let key = object.record().key().clone();

    let result = mutation
        .publish_if_roots_match(
            vec![object],
            vec![RootExpectation {
                name: name.clone(),
                target: None,
            }],
            vec![RootChange::Set {
                name: name.clone(),
                target: key.clone(),
            }],
        )
        .await
        .unwrap();
    assert!(matches!(result, ConditionalPublishResult::Committed(_)));
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .root(&name)
            .await
            .unwrap(),
        Some(key)
    );
}

#[tokio::test]
async fn conditional_publication_reports_a_concurrent_root_change() {
    #[derive(Clone)]
    struct RepointOnceMetadataStore {
        inner: MemoryMetadataStore,
        competing: Arc<std::sync::RwLock<Option<(RootName, ObjectKey)>>>,
    }

    #[async_trait]
    impl MetadataStore for RepointOnceMetadataStore {
        async fn try_collection_lease(
            &self,
        ) -> Result<Option<crate::metadata::RepositoryLease>, crate::metadata::MetadataError>
        {
            self.inner.try_collection_lease().await
        }
        fn coordinates_payload_catalog(&self) -> bool {
            self.inner.coordinates_payload_catalog()
        }
        async fn pin_store(
            &self,
        ) -> Result<std::sync::Arc<dyn crate::metadata::PinStore>, crate::metadata::MetadataError>
        {
            self.inner.pin_store().await
        }

        async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
            self.inner.snapshot().await
        }

        async fn commit(
            &self,
            expected: &crate::RepositoryRevision,
            mutation: MetadataMutation,
        ) -> Result<CommitResult, MetadataError> {
            let competing = self
                .competing
                .write()
                .map_err(|_| MetadataError::Poisoned)?
                .take();
            if let Some((name, target)) = competing {
                let mut repoint = MetadataMutation::new();
                repoint.set_root(name, target);
                let competing = self.inner.commit(expected, repoint).await?;
                return Err(MetadataError::StaleRevision {
                    expected: *expected,
                    actual: competing.revision,
                });
            }
            self.inner.commit(expected, mutation).await
        }
    }

    let competing = Arc::new(std::sync::RwLock::new(None));
    let state = RepointOnceMetadataStore {
        inner: MemoryMetadataStore::new().unwrap(),
        competing: competing.clone(),
    };
    let repository = Repository::new(MemoryBlobStore::new(), state);
    let mutation = repository.mutation_session().await.unwrap();
    let name = RootName::try_from("replicated/workspace").unwrap();

    let current = mutation.stage_blob(b"current frontier").await.unwrap();
    let current_key = current.record().key().clone();
    let concurrent = mutation.stage_blob(b"concurrent frontier").await.unwrap();
    let concurrent_key = concurrent.record().key().clone();
    mutation
        .publish(
            vec![current, concurrent],
            vec![RootChange::Set {
                name: name.clone(),
                target: current_key.clone(),
            }],
        )
        .await
        .unwrap();

    let candidate = mutation
        .stage_blob(b"stale derived frontier")
        .await
        .unwrap();
    let candidate_key = candidate.record().key().clone();
    *competing.write().unwrap() = Some((name.clone(), concurrent_key.clone()));
    let result = mutation
        .publish_if_roots_match(
            vec![candidate],
            vec![RootExpectation {
                name: name.clone(),
                target: Some(current_key.clone()),
            }],
            vec![RootChange::Set {
                name: name.clone(),
                target: candidate_key.clone(),
            }],
        )
        .await
        .unwrap();
    assert_eq!(
        result,
        ConditionalPublishResult::RootMismatch {
            name: name.clone(),
            expected: Some(current_key),
            actual: Some(concurrent_key.clone()),
        }
    );
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert_eq!(snapshot.root(&name).await.unwrap(), Some(concurrent_key));
    assert!(snapshot.object(&candidate_key).await.unwrap().is_none());
}

#[tokio::test]
async fn conditional_publication_rejects_duplicate_expectations() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let name = RootName::try_from("replicated/workspace").unwrap();
    let error = mutation
        .publish_if_roots_match(
            Vec::new(),
            vec![
                RootExpectation {
                    name: name.clone(),
                    target: None,
                },
                RootExpectation { name, target: None },
            ],
            Vec::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RepositoryError::InvalidInput(_)));
}

#[tokio::test]
async fn root_release_is_an_exact_compare_and_remove() {
    let repository = repository();
    let name = RootName::try_from("services/cache").unwrap();
    let mutation = repository.mutation_session().await.unwrap();
    let object = mutation.stage_blob(b"owned").await.unwrap();
    let key = object.record().key().clone();
    mutation
        .publish_rooted(vec![object], name.clone(), key.clone())
        .await
        .unwrap();
    drop(mutation);

    let wrong = ObjectKey::blob(BlobId::new(Digest::hash(b"wrong")));
    assert!(
        repository
            .remove_root_if_matches(&name, &wrong)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .root(&name)
            .await
            .unwrap(),
        Some(key.clone())
    );
    assert!(
        repository
            .remove_root_if_matches(&name, &key)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .root(&name)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn nonblocking_collection_runs_under_a_live_mutation() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    repository.try_collect().await.unwrap();
    drop(mutation);
    repository.try_collect().await.unwrap();
}

#[tokio::test]
async fn mutation_start_runs_before_staging_pin_admission() {
    let mut repository = repository();
    let calls = Arc::new(AtomicUsize::new(0));
    Arc::make_mut(&mut repository.profile).mutation_start =
        Some(Arc::new(CollectOnMutationStart {
            repository: repository.clone(),
            calls: calls.clone(),
        }));

    let mutation = repository.mutation_session().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    repository.try_collect().await.unwrap();
    drop(mutation);
}

#[tokio::test]
async fn later_leaf_completes_graph_without_rewriting_parent() {
    let repository = repository();
    let child_bytes = b"arrives later";
    let child_id = BlobId::new(Digest::hash(child_bytes));
    let parent = Directory::try_from_iter([(
        pc("child"),
        Node::File {
            digest: child_id,
            size: child_bytes.len() as u64,
            executable: false,
        },
    )])
    .unwrap();
    let parent_key = ObjectKey::directory(parent.digest());

    let mutation = repository.mutation_session().await.unwrap();
    let staged_parent = mutation.stage_directory(&parent).await.unwrap();
    mutation
        .publish_unrooted(vec![staged_parent])
        .await
        .unwrap();
    let error = mutation
        .publish_rooted(
            Vec::new(),
            RootName::try_from("tree").unwrap(),
            parent_key.clone(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RepositoryError::RootNotPublishable {
            status: ClosureStatus::Missing { .. },
            ..
        }
    ));

    let child = mutation.stage_blob(child_bytes).await.unwrap();
    mutation
        .publish_rooted(
            vec![child],
            RootName::try_from("tree").unwrap(),
            parent_key.clone(),
        )
        .await
        .unwrap();
    drop(mutation);
    assert!(matches!(
        repository.verify_closure(&parent_key).await.unwrap(),
        ClosureStatus::Complete { .. }
    ));
}

#[tokio::test]
async fn generic_closure_handles_cycles_and_high_fanout() {
    let repository = graph_repository();
    let mutation = repository.mutation_session().await.unwrap();
    let a = stage_graph(&mutation, 1, &[2]).await;
    let a_key = a.record().key().clone();
    let b = stage_graph(&mutation, 2, &[1]).await;
    mutation
        .publish_rooted(
            vec![a, b],
            RootName::try_from("cycles/main").unwrap(),
            a_key.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(
        mutation
            .repository
            .verify_closure(&a_key)
            .await
            .unwrap(),
        ClosureStatus::Complete { objects } if objects == 2
    ));
    drop(mutation);

    let repository = graph_repository();
    let mutation = repository.mutation_session().await.unwrap();
    let mut staged = Vec::new();
    for native in 0..200u8 {
        staged.push(stage_graph(&mutation, native, &[]).await);
    }
    let root = stage_graph(&mutation, 250, &(0..200u8).collect::<Vec<_>>()).await;
    let root_key = root.record().key().clone();
    staged.push(root);
    mutation
        .publish_rooted(
            staged,
            RootName::try_from("fanout/main").unwrap(),
            root_key.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(
        mutation
            .repository
            .verify_closure(&root_key)
            .await
            .unwrap(),
        ClosureStatus::Complete { objects } if objects == 201
    ));
}

#[tokio::test]
async fn immutable_conflict_is_distinct_from_idempotent_reinsertion() {
    let repository = graph_repository();
    let mutation = repository.mutation_session().await.unwrap();
    let first = stage_graph(&mutation, 1, &[]).await;
    mutation.publish_unrooted(vec![first]).await.unwrap();
    let identical = stage_graph(&mutation, 1, &[]).await;
    let outcome = mutation.publish_unrooted(vec![identical]).await.unwrap();
    assert_eq!(outcome.objects_inserted, 0);

    let conflict = stage_graph(&mutation, 1, &[2]).await;
    assert!(matches!(
        mutation.publish_unrooted(vec![conflict]).await,
        Err(RepositoryError::Metadata(MetadataError::ImmutableConflict(
            _
        )))
    ));
}

#[tokio::test]
async fn unavailable_namespace_remains_conservatively_collectable() {
    let repository = graph_repository();
    let mutation = repository.mutation_session().await.unwrap();
    let child = stage_graph(&mutation, 1, &[]).await;
    let root = stage_graph(&mutation, 2, &[1]).await;
    let root_key = root.record().key().clone();
    mutation
        .publish_rooted(
            vec![child, root],
            RootName::try_from("graphs/main").unwrap(),
            root_key.clone(),
        )
        .await
        .unwrap();
    drop(mutation);

    let without_format = Repository::with_formats(
        repository.payloads().clone(),
        repository.metadata().clone(),
        FormatRegistry::new(Vec::<Arc<dyn ObjectFormat>>::new()).unwrap(),
        FormatLimits::default(),
    );
    assert!(matches!(
        without_format.verify_closure(&root_key).await.unwrap(),
        ClosureStatus::Unsupported { .. }
    ));
    let outcome = without_format.collect().await.unwrap();
    assert_eq!(outcome.removed.logical_objects, 0);
    let snapshot = without_format.metadata().snapshot().await.unwrap();
    assert_eq!(
        snapshot
            .objects()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn direct_relation_mismatch_prevents_root_publication() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let child = mutation.stage_blob(b"four").await.unwrap();
    let child_id = child.record().payload();
    let parent = Directory::try_from_iter([(
        pc("child"),
        Node::File {
            digest: child_id,
            size: 99,
            executable: false,
        },
    )])
    .unwrap();
    let parent_key = ObjectKey::directory(parent.digest());
    let parent = mutation.stage_directory(&parent).await.unwrap();
    let error = mutation
        .publish_rooted(
            vec![child, parent],
            RootName::try_from("tree").unwrap(),
            parent_key,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RepositoryError::RootNotPublishable {
            status: ClosureStatus::Invalid { .. },
            ..
        }
    ));
}

#[tokio::test]
async fn generic_collection_prunes_logical_and_physical_orphans() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let live = mutation.stage_blob(b"live").await.unwrap();
    let live_key = live.record().key().clone();
    let live_payload = live.record().payload();
    let orphan = mutation.stage_blob(b"orphan").await.unwrap();
    let orphan_key = orphan.record().key().clone();
    let orphan_payload = orphan.record().payload();
    mutation
        .publish_rooted(
            vec![live, orphan],
            RootName::try_from("live").unwrap(),
            live_key.clone(),
        )
        .await
        .unwrap();
    drop(mutation);

    let preview = repository.preview_collection().await.unwrap();
    assert_eq!(preview.logical_objects, 1);
    assert_eq!(preview.payload_blobs, 1);
    let outcome = repository.collect().await.unwrap();
    assert_eq!(outcome.removed, preview);

    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert!(snapshot.object(&live_key).await.unwrap().is_some());
    assert!(snapshot.object(&orphan_key).await.unwrap().is_none());
    assert!(repository.payloads().has(&live_payload).await.unwrap());
    assert!(!repository.payloads().has(&orphan_payload).await.unwrap());
}

#[tokio::test]
async fn logical_collection_prunes_one_tenant_without_deleting_a_shared_blob() {
    let payloads = SharedBlobStore {
        inner: MemoryBlobStore::new(),
    };
    let tenant_a = Repository::new(payloads.clone(), MemoryMetadataStore::new().unwrap());
    let tenant_b = Repository::new(payloads.clone(), MemoryMetadataStore::new().unwrap());

    let mutation_a = tenant_a.mutation_session().await.unwrap();
    let shared = mutation_a
        .stage_blob(b"shared across tenants")
        .await
        .unwrap();
    let shared_key = shared.record().key().clone();
    let shared_payload = shared.record().payload();
    mutation_a.publish_unrooted(vec![shared]).await.unwrap();
    drop(mutation_a);

    let mutation_b = tenant_b.mutation_session().await.unwrap();
    let shared_for_b = mutation_b
        .stage_existing(shared_key.clone(), shared_payload)
        .await
        .unwrap();
    mutation_b
        .publish_rooted(
            vec![shared_for_b],
            RootName::try_from("inputs/source").unwrap(),
            shared_key.clone(),
        )
        .await
        .unwrap();
    drop(mutation_b);

    assert_eq!(
        tenant_a.preview_logical_collection().await.unwrap(),
        LogicalCollectionPreview { logical_objects: 1 }
    );
    let outcome = tenant_a.collect_logical().await.unwrap();
    assert_eq!(outcome.removed.logical_objects, 1);
    assert!(outcome.revision.is_some());
    assert!(
        tenant_a
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .object(&shared_key)
            .await
            .unwrap()
            .is_none()
    );

    // The byte remains readable because physical lifetime is not owned by
    // tenant A's logical collection. A remote backend's aggregate ledger
    // may reclaim it only after tenant B releases it too.
    assert!(payloads.has(&shared_payload).await.unwrap());
    assert!(matches!(
        tenant_b.verify_closure(&shared_key).await.unwrap(),
        ClosureStatus::Complete { objects: 1 }
    ));
}

#[tokio::test]
async fn borrowed_and_owned_holds_independently_retain_their_snapshot() {
    for drop_owned_first in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::local(directory.path()).await.unwrap();
        let name = RootName::try_from("held/root").unwrap();
        let bytes = b"protected by either hold";
        let mutation = repository.mutation_session().await.unwrap();
        let staged = mutation.stage_blob(bytes).await.unwrap();
        let record = staged.record().clone();
        let key = record.key().clone();
        mutation
            .publish_rooted(vec![staged], name.clone(), key.clone())
            .await
            .unwrap();
        drop(mutation);

        // Opening a local handle takes exclusive initialization ownership.
        let collector = Repository::local(directory.path()).await.unwrap();
        let borrowed = repository.retention_hold().await.unwrap();
        let owned = repository.owned_retention_hold().await.unwrap();
        let revision = borrowed.snapshot().revision();
        assert_eq!(owned.snapshot().revision(), revision);
        for found in [
            borrowed.object(&key).await.unwrap(),
            owned.object(&key).await.unwrap(),
        ] {
            assert_eq!(found, Some(record.clone()));
        }

        collector
            .remove_root_if_matches(&name, &key)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(
            collector.metadata().snapshot().await.unwrap().revision(),
            revision
        );
        for snapshot in [borrowed.snapshot(), owned.snapshot()] {
            assert_eq!(snapshot.root(&name).await.unwrap(), Some(key.clone()));
        }
        assert_eq!(
            collector
                .try_collect()
                .await
                .unwrap()
                .removed
                .logical_objects,
            0
        );

        if drop_owned_first {
            drop(owned);
            assert_eq!(
                collector
                    .try_collect()
                    .await
                    .unwrap()
                    .removed
                    .logical_objects,
                0
            );
            let (_, mut reader) = borrowed.open_payload(&key).await.unwrap().unwrap();
            let mut actual = Vec::new();
            reader.read_to_end(&mut actual).await.unwrap();
            assert_eq!(actual, bytes);
            drop(reader);
            drop(borrowed);
        } else {
            drop(borrowed);
            drop(repository);
            assert_eq!(
                collector
                    .try_collect()
                    .await
                    .unwrap()
                    .removed
                    .logical_objects,
                0
            );
            let (_, mut reader) = owned.open_payload(&key).await.unwrap().unwrap();
            let mut actual = Vec::new();
            reader.read_to_end(&mut actual).await.unwrap();
            assert_eq!(actual, bytes);
            drop(reader);
            drop(owned);
        }
        // Both read pins are gone, so an independent handle can reclaim
        // the now-unrooted object and its physical payload.
        let outcome = collector.try_collect().await.unwrap();
        assert_eq!(outcome.removed.logical_objects, 1);
        assert!(
            collector
                .metadata()
                .snapshot()
                .await
                .unwrap()
                .object(&key)
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn nonblocking_logical_collection_respects_a_retention_hold() {
    let repository = Repository::new(
        SharedBlobStore {
            inner: MemoryBlobStore::new(),
        },
        MemoryMetadataStore::new().unwrap(),
    );
    let hold = repository.retention_hold().await.unwrap();
    assert_eq!(
        repository
            .try_collect_logical()
            .await
            .unwrap()
            .removed
            .logical_objects,
        0
    );
    drop(hold);
    assert!(repository.try_collect_logical().await.is_ok());
}

#[tokio::test]
async fn existing_payload_pin_preserves_chunks_during_an_older_sweep() {
    let repository = chunked_repository(1024);
    let mutation = repository.mutation_session().await.unwrap();
    let object = mutation.stage_blob(&[1; 4096]).await.unwrap();
    let key = object.record().key().clone();
    let payload = object.record().payload();
    let unrelated = mutation.stage_blob(&[2; 4096]).await.unwrap();
    let unrelated_payload = unrelated.record().payload();
    drop((object, unrelated));
    drop(mutation);
    crate::flush_repository_leases().await.unwrap();
    let plan = repository
        .collection_plan(
            repository.coordination.clone().lock_owned().await,
            None,
            true,
        )
        .await
        .unwrap();
    assert_eq!(plan.preview.payload_blobs, 2);
    assert_eq!(plan.preview.chunks, 2);
    let writer = Repository::new(repository.payloads().clone(), repository.metadata().clone());
    let mutation = writer.mutation_session().await.unwrap();
    let object = mutation.stage_existing(key.clone(), payload).await.unwrap();
    mutation
        .publish_rooted(
            vec![object],
            RootName::try_from("reused").unwrap(),
            key.clone(),
        )
        .await
        .unwrap();
    drop(mutation);
    crate::metadata::flush_pin_releases().await;
    let outcome = repository.execute_collection(plan, false).await.unwrap();
    assert_eq!(outcome.removed.payload_blobs, 1);
    assert_eq!(outcome.removed.chunks, 1);
    assert!(!repository.payloads().has(&unrelated_payload).await.unwrap());
    let mut reader = repository
        .payloads()
        .open_read(&payload)
        .await
        .unwrap()
        .unwrap();
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, &[1; 4096]);
    crate::flush_repository_leases().await.unwrap();
}

async fn assert_writer_pin_isolation<PS: BlobGc + Clone + 'static>(payloads: PS) {
    let repository = Repository::new(payloads, MemoryMetadataStore::new().unwrap());
    let first = repository.mutation_session().await.unwrap();
    let held = first.stage_blob(&[21; 4096]).await.unwrap();
    let held_payload = held.record().payload();
    let second = repository.mutation_session().await.unwrap();
    let garbage = second.stage_blob(&[22; 4096]).await.unwrap();
    let garbage_payload = garbage.record().payload();
    second.publish_unrooted(vec![garbage]).await.unwrap();
    drop(second);
    crate::metadata::flush_pin_releases().await;
    let inventory = repository
        .metadata()
        .pin_store()
        .await
        .unwrap()
        .inventory()
        .await
        .unwrap();
    let first_resources = &inventory.pins[first.pin.token()].resources;
    assert!(first_resources.contains(&crate::metadata::PinResource::Blob(held_payload)));
    assert!(
        !first_resources.contains(&crate::metadata::PinResource::Blob(garbage_payload)),
        "an idle session must not acquire another writer's payload"
    );
    // Independent facade shares storage and pins while avoiding the old
    // process-wide guard, which is still present during this transition.
    let collector = Repository::new(repository.payloads().clone(), repository.metadata().clone());
    let collected = collector.collect().await.unwrap();
    assert_eq!(collected.removed.logical_objects, 1);
    assert_eq!(collected.removed.payload_blobs, 1);
    assert!(!repository.payloads().has(&garbage_payload).await.unwrap());
    assert!(repository.payloads().has(&held_payload).await.unwrap());
    drop(held);
    drop(first);
    crate::flush_repository_leases().await.unwrap();
    collector.collect().await.unwrap();
    assert!(!repository.payloads().has(&held_payload).await.unwrap());
}

#[tokio::test]
async fn post_prune_snapshot_reader_can_enter_during_physical_sweep() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let live = mutation.stage_blob(b"live during sweep").await.unwrap();
    let live_key = live.record().key().clone();
    let garbage = mutation.stage_blob(b"old unrooted record").await.unwrap();
    let garbage_key = garbage.record().key().clone();
    let garbage_payload = garbage.record().payload();
    mutation
        .publish(
            vec![live, garbage],
            vec![RootChange::Set {
                name: "live".parse().unwrap(),
                target: live_key.clone(),
            }],
        )
        .await
        .unwrap();
    drop(mutation);
    crate::flush_repository_leases().await.unwrap();
    let plan = repository
        .collection_plan(
            repository.coordination.clone().lock_owned().await,
            None,
            true,
        )
        .await
        .unwrap();
    repository
        .publication
        .prune(
            plan.logical.protection.clone(),
            plan.logical.snapshot_revision,
            plan.logical.pins.clone(),
            BTreeSet::new(),
            None,
            MetadataMutation::install_retained_source(plan.logical.live_objects.clone()),
        )
        .await
        .unwrap();
    let ledger = repository.metadata().pin_store().await.unwrap();
    let claim = ledger
        .claim_deletions(
            ledger.inventory().await.unwrap().revision,
            BTreeSet::from([crate::metadata::PinResource::Blob(garbage_payload)]),
        )
        .await
        .unwrap()
        .unwrap();
    let reader = Repository::new(repository.payloads().clone(), repository.metadata().clone());
    let hold = tokio::time::timeout(Duration::from_secs(2), reader.retention_hold())
        .await
        .unwrap()
        .unwrap();
    assert!(
        hold.snapshot()
            .object(&garbage_key)
            .await
            .unwrap()
            .is_none()
    );
    assert!(hold.snapshot().object(&live_key).await.unwrap().is_some());
    assert!(matches!(
        hold.verify_closure(&live_key).await.unwrap(),
        ClosureStatus::Complete { .. }
    ));
    assert!(
        ledger
            .inventory()
            .await
            .unwrap()
            .deletions
            .contains_key(&claim)
    );
    drop(hold);
    crate::metadata::flush_pin_releases().await;
    ledger.finish_deletions(&claim).await.unwrap();
    plan.logical.protection.collector.finish().await.unwrap();
    drop(plan);
    crate::flush_repository_leases().await.unwrap();
    repository.collect().await.unwrap();
    assert!(!repository.payloads().has(&garbage_payload).await.unwrap());
}

#[tokio::test]
async fn mutation_sessions_only_pin_their_own_writes() {
    assert_writer_pin_isolation(MemoryBlobStore::new()).await;
    assert_writer_pin_isolation(chunked_repository(1024).payloads().clone()).await;
    let packed = ChunkedBlobStore::packed_with_options(
        Arc::new(object_store::memory::InMemory::new()),
        object_store::path::Path::default(),
        1024,
        crate::PackOptions {
            target_size: u64::MAX,
            cache_capacity: 0,
        },
    )
    .await
    .unwrap();
    assert_writer_pin_isolation(packed).await;
}

#[tokio::test]
async fn completed_writer_cannot_disappear_from_an_older_collection_mark() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let orphan = mutation
        .stage_blob(b"published during collection")
        .await
        .unwrap();
    let payload = orphan.record().payload();
    let unrelated = mutation.stage_blob(b"unrelated garbage").await.unwrap();
    let unrelated_payload = unrelated.record().payload();
    drop(unrelated);
    drop(orphan);
    drop(mutation);
    crate::flush_repository_leases().await.unwrap();
    let plan = repository
        .collection_plan(
            repository.coordination.clone().lock_owned().await,
            None,
            true,
        )
        .await
        .unwrap();
    assert_eq!(plan.preview.payload_blobs, 2);
    assert_eq!(plan.preview.logical_objects, 0);
    // Independent facade: its generic metadata/backend handles share the
    // durable ledger, not this collector's legacy in-process guard.
    let writer = Repository::new(repository.payloads().clone(), repository.metadata().clone());
    let mutation = writer.mutation_session().await.unwrap();
    let object = mutation
        .stage_blob(b"published during collection")
        .await
        .unwrap();
    let key = object.record().key().clone();
    mutation
        .publish_rooted(
            vec![object],
            RootName::try_from("new-root").unwrap(),
            key.clone(),
        )
        .await
        .unwrap();
    drop(mutation);
    crate::metadata::flush_pin_releases().await;
    let ledger = repository.metadata().pin_store().await.unwrap();
    let inventory = ledger.inventory().await.unwrap();
    assert!(!inventory.retired.is_empty());
    assert!(inventory.pins.values().any(|pin| {
        pin.resources
            .contains(&crate::metadata::PinResource::Blob(payload))
    }));
    let outcome = repository.execute_collection(plan, false).await.unwrap();
    assert_eq!(outcome.removed.payload_blobs, 1);
    assert_eq!(outcome.removed.logical_objects, 0);
    assert!(!repository.payloads().has(&unrelated_payload).await.unwrap());
    crate::flush_repository_leases().await.unwrap();
    assert!(repository.payloads().has(&payload).await.unwrap());
    assert!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .object(&key)
            .await
            .unwrap()
            .is_some()
    );
    repository.collect().await.unwrap();
    assert!(repository.payloads().has(&payload).await.unwrap());
}

#[tokio::test]
async fn collection_preserves_pinned_unpublished_payloads_and_individual_chunks() {
    use crate::metadata::{DataPin, DataPinLease, PinResource, PinScope};

    let repository = chunked_repository(1024);
    let mutation = repository.mutation_session().await.unwrap();
    let held = mutation.stage_blob(&[1; 4096]).await.unwrap();
    let held_payload = held.record().payload();
    let chunk_only = mutation.stage_blob(&[2; 4096]).await.unwrap();
    let chunk_payload = chunk_only.record().payload();
    let orphan = mutation.stage_blob(&[3; 4096]).await.unwrap();
    let orphan_payload = orphan.record().payload();
    // Leave all records unpublished: only the physical staging pins below
    // establish liveness, so this cannot pass through named-root marking.
    drop((held, chunk_only, orphan));
    drop(mutation);
    crate::flush_repository_leases().await.unwrap();
    let held_chunk = repository
        .payloads()
        .chunks(&chunk_payload)
        .await
        .unwrap()
        .unwrap()[0]
        .digest;
    let absent = BlobId::new(Digest::hash(b"absent physical pin"));
    let ledger = repository.metadata().pin_store().await.unwrap();
    let pin = DataPinLease::acquire(
        ledger.clone(),
        DataPin {
            scope: PinScope::Staging,
            catalog: None,
            resources: BTreeSet::from([
                PinResource::Blob(held_payload),
                PinResource::Chunk(held_chunk),
                PinResource::Blob(absent),
            ]),
        },
    )
    .await
    .unwrap();
    repository.collect().await.unwrap();
    assert_eq!(
        repository
            .payloads()
            .read_to_vec(&held_payload)
            .await
            .unwrap()
            .unwrap(),
        vec![1; 4096]
    );
    assert!(!repository.payloads().has(&orphan_payload).await.unwrap());
    let chunks: BTreeSet<_> = repository
        .payloads()
        .list_chunks()
        .try_collect()
        .await
        .unwrap();
    assert!(chunks.contains(&held_chunk));
    assert!(ledger.inventory().await.unwrap().deletions.is_empty());
    drop(pin);
    crate::flush_repository_leases().await.unwrap();
    repository.collect().await.unwrap();
    assert!(
        repository
            .payloads()
            .list_blobs()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        repository
            .payloads()
            .list_chunks()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn collection_keeps_chunks_shared_with_a_live_payload() {
    let repository = chunked_repository(1024);
    let common: Vec<u8> = (0..50_000).map(|index| (index % 251) as u8).collect();
    let mut live_bytes = common.clone();
    live_bytes.extend_from_slice(b"----live-suffix-AAAA");
    let mut orphan_bytes = common;
    orphan_bytes.extend_from_slice(b"----orphan-suffix-BBBB");

    let mutation = repository.mutation_session().await.unwrap();
    let live = mutation.stage_blob(&live_bytes).await.unwrap();
    let live_key = live.record().key().clone();
    let live_payload = live.record().payload();
    let orphan = mutation.stage_blob(&orphan_bytes).await.unwrap();
    let orphan_payload = orphan.record().payload();
    mutation
        .publish_rooted(
            vec![live, orphan],
            RootName::try_from("chunks/live").unwrap(),
            live_key,
        )
        .await
        .unwrap();
    drop(mutation);

    let live_chunks: BTreeSet<_> = repository
        .payloads()
        .chunks(&live_payload)
        .await
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|chunk| chunk.digest)
        .collect();
    let orphan_chunks: BTreeSet<_> = repository
        .payloads()
        .chunks(&orphan_payload)
        .await
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|chunk| chunk.digest)
        .collect();
    assert!(!live_chunks.is_disjoint(&orphan_chunks));
    let orphan_only: BTreeSet<_> = orphan_chunks.difference(&live_chunks).copied().collect();
    assert!(!orphan_only.is_empty());

    repository.collect().await.unwrap();
    assert_eq!(
        repository
            .payloads()
            .read_to_vec(&live_payload)
            .await
            .unwrap()
            .as_deref(),
        Some(live_bytes.as_slice())
    );
    assert!(!repository.payloads().has(&orphan_payload).await.unwrap());
    let remaining: BTreeSet<_> = repository
        .payloads()
        .list_chunks()
        .try_collect()
        .await
        .unwrap();
    assert!(live_chunks.is_subset(&remaining));
    assert!(orphan_only.is_disjoint(&remaining));
}

#[tokio::test]
async fn reimport_after_collection_reuploads_swept_chunks() {
    let repository = chunked_repository(1024);
    let bytes = b"content that must survive a stale deduplication index";

    let mutation = repository.mutation_session().await.unwrap();
    let first = mutation.stage_blob(bytes).await.unwrap();
    let key = first.record().key().clone();
    let payload = first.record().payload();
    mutation.publish_unrooted(vec![first]).await.unwrap();
    drop(mutation);

    repository.collect().await.unwrap();
    assert!(!repository.payloads().has(&payload).await.unwrap());

    let mutation = repository.mutation_session().await.unwrap();
    let second = mutation.stage_blob(bytes).await.unwrap();
    assert_eq!(second.record().key(), &key);
    mutation
        .publish_rooted(
            vec![second],
            RootName::try_from("reimport/current").unwrap(),
            key,
        )
        .await
        .unwrap();
    drop(mutation);
    assert_eq!(
        repository.payloads().read_to_vec(&payload).await.unwrap(),
        Some(bytes.to_vec())
    );
}

#[tokio::test]
async fn collection_with_no_roots_empties_storage_and_second_pass_is_a_noop() {
    let repository = chunked_repository(1024);
    let mutation = repository.mutation_session().await.unwrap();
    let first = mutation.stage_blob(b"first orphan").await.unwrap();
    let second = mutation.stage_blob(&vec![0xabu8; 16 * 1024]).await.unwrap();
    mutation
        .publish_unrooted(vec![first, second])
        .await
        .unwrap();
    drop(mutation);

    let first = repository.collect().await.unwrap();
    assert_eq!(first.removed.logical_objects, 2);
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .objects()
            .try_collect::<Vec<_>>()
            .await
            .unwrap(),
        Vec::new()
    );
    assert!(
        repository
            .payloads()
            .list_blobs()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        repository
            .payloads()
            .list_chunks()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );

    let second = repository.collect().await.unwrap();
    assert_eq!(second.removed, CollectionPreview::default());
    assert_eq!(second.revision, None);
    assert!(repository.fsck().await.unwrap().is_clean());
}

#[tokio::test]
async fn logical_prune_failure_never_starts_physical_deletion() {
    let fail_once = Arc::new(AtomicBool::new(false));
    let repository = Repository::new(
        MemoryBlobStore::new(),
        FailingCommitMetadataStore {
            inner: MemoryMetadataStore::new().unwrap(),
            fail_once: fail_once.clone(),
        },
    );
    let mutation = repository.mutation_session().await.unwrap();
    let orphan = mutation.stage_blob(b"survives failed prune").await.unwrap();
    let key = orphan.record().key().clone();
    let payload = orphan.record().payload();
    mutation.publish_unrooted(vec![orphan]).await.unwrap();
    drop(mutation);

    let before = repository.metadata().snapshot().await.unwrap().revision();
    fail_once.store(true, Ordering::SeqCst);
    assert!(matches!(
        repository.collect().await,
        Err(RepositoryError::Metadata(MetadataError::Backend(message)))
            if message.contains("injected failure")
    ));
    let after = repository.metadata().snapshot().await.unwrap();
    assert_eq!(after.revision(), before);
    assert!(after.object(&key).await.unwrap().is_some());
    assert!(repository.payloads().has(&payload).await.unwrap());

    repository.collect().await.unwrap();
    assert!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .object(&key)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!repository.payloads().has(&payload).await.unwrap());
}

#[tokio::test]
async fn local_emergency_collection_uses_stale_payload_as_commit_space() {
    let fail_once = Arc::new(AtomicBool::new(false));
    let mut repository = Repository::new(
        MemoryBlobStore::new(),
        StorageFullOnceMetadataStore {
            inner: MemoryMetadataStore::new().unwrap(),
            fail_once: fail_once.clone(),
        },
    );
    // The standard local profile sets this only because its state and
    // payload bytes are guaranteed to occupy the same filesystem.
    Arc::make_mut(&mut repository.profile).set_emergency_collection(true);
    let mutation = repository.mutation_session().await.unwrap();
    let orphan = mutation
        .stage_blob(b"garbage funds the state retry")
        .await
        .unwrap();
    let key = orphan.record().key().clone();
    let payload = orphan.record().payload();
    mutation.publish_unrooted(vec![orphan]).await.unwrap();
    drop(mutation);

    fail_once.store(true, Ordering::SeqCst);
    let outcome = repository.collect().await.unwrap();
    assert_eq!(outcome.removed.logical_objects, 1);
    assert!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .object(&key)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!repository.payloads().has(&payload).await.unwrap());
}

#[tokio::test]
async fn generic_storage_full_does_not_assume_colocated_payload_capacity() {
    let fail_once = Arc::new(AtomicBool::new(false));
    let repository = Repository::new(
        MemoryBlobStore::new(),
        StorageFullOnceMetadataStore {
            inner: MemoryMetadataStore::new().unwrap(),
            fail_once: fail_once.clone(),
        },
    );
    let mutation = repository.mutation_session().await.unwrap();
    let orphan = mutation
        .stage_blob(b"custom backends may live on different devices")
        .await
        .unwrap();
    let key = orphan.record().key().clone();
    let payload = orphan.record().payload();
    mutation.publish_unrooted(vec![orphan]).await.unwrap();
    drop(mutation);

    fail_once.store(true, Ordering::SeqCst);
    assert!(matches!(
        repository.collect().await,
        Err(RepositoryError::Metadata(MetadataError::StorageFull))
    ));
    assert!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .object(&key)
            .await
            .unwrap()
            .is_some()
    );
    assert!(repository.payloads().has(&payload).await.unwrap());
}

#[tokio::test]
async fn interrupted_emergency_sweep_is_collectible_and_retryable() {
    let failures = Arc::new(AtomicUsize::new(0));
    let mut repository = Repository::new(
        MemoryBlobStore::new(),
        InterruptedEmergencyMetadataStore {
            inner: MemoryMetadataStore::new().unwrap(),
            failures: failures.clone(),
        },
    );
    Arc::make_mut(&mut repository.profile).set_emergency_collection(true);
    let mutation = repository.mutation_session().await.unwrap();
    let orphan = mutation
        .stage_blob(b"unreachable payload removed before interrupted retry")
        .await
        .unwrap();
    let key = orphan.record().key().clone();
    let payload = orphan.record().payload();
    mutation.publish_unrooted(vec![orphan]).await.unwrap();
    drop(mutation);

    failures.store(2, Ordering::SeqCst);
    assert!(matches!(
        repository.collect().await,
        Err(RepositoryError::Metadata(MetadataError::Backend(message)))
            if message.contains("after emergency sweep")
    ));
    assert!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .object(&key)
            .await
            .unwrap()
            .is_some()
    );
    assert!(!repository.payloads().has(&payload).await.unwrap());
    let ledger = repository.metadata().pin_store().await.unwrap();
    let interrupted = ledger.inventory().await.unwrap();
    assert!(
        interrupted.logical_prune.is_some(),
        "the emergency fence must survive a failed metadata retry"
    );
    assert!(interrupted.collector.is_some());
    assert!(!interrupted.deletions.is_empty());
    assert!(
        ledger
            .register(crate::metadata::DataPin {
                scope: crate::metadata::PinScope::Staging,
                catalog: None,
                resources: BTreeSet::from([crate::metadata::PinResource::Object(key.clone())]),
            })
            .await
            .unwrap()
            .is_none(),
        "an old unrooted record cannot become a new root after its payload was swept"
    );
    assert!(matches!(
        repository.fsck().await,
        Err(RepositoryError::Busy(_))
    ));

    let plan = repository
        .collection_plan(
            repository.coordination.clone().lock_owned().await,
            None,
            true,
        )
        .await
        .unwrap();
    let remarking = ledger.inventory().await.unwrap();
    assert_eq!(
        remarking.logical_prune, interrupted.logical_prune,
        "recovery must not reopen admission while rebuilding its mark"
    );
    assert!(
        ledger
            .register(crate::metadata::DataPin {
                scope: crate::metadata::PinScope::Snapshot {
                    generation: u64::MAX
                },
                catalog: None,
                resources: BTreeSet::new(),
            })
            .await
            .unwrap()
            .is_none()
    );
    repository.execute_collection(plan, false).await.unwrap();
    assert!(repository.fsck().await.unwrap().is_clean());
    let recovered = ledger.inventory().await.unwrap();
    assert!(recovered.logical_prune.is_none());
    assert!(recovered.deletions.is_empty());
}

#[tokio::test]
async fn missing_rooted_descendant_aborts_before_logical_prune() {
    let hidden = Arc::new(StdRwLock::new(None));
    let state = HiddenObjectMetadataStore {
        inner: MemoryMetadataStore::new().unwrap(),
        hidden: hidden.clone(),
    };
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        state,
        FormatRegistry::new([Arc::new(SyntheticFormat::default()) as Arc<dyn ObjectFormat>])
            .unwrap(),
        FormatLimits::default(),
    );
    let mutation = repository.mutation_session().await.unwrap();
    let child = stage_graph(&mutation, 1, &[]).await;
    let child_key = child.record().key().clone();
    let root = stage_graph(&mutation, 2, &[1]).await;
    let root_key = root.record().key().clone();
    let orphan = stage_graph(&mutation, 3, &[]).await;
    let orphan_key = orphan.record().key().clone();
    mutation
        .publish_rooted(
            vec![child, root, orphan],
            RootName::try_from("corrupt/root").unwrap(),
            root_key.clone(),
        )
        .await
        .unwrap();
    drop(mutation);

    let before = repository.metadata().snapshot().await.unwrap().revision();
    *hidden.write().unwrap() = Some(child_key);
    assert!(matches!(
        repository.collect().await,
        Err(RepositoryError::Metadata(MetadataError::Corruption(message)))
            if message.contains("links to missing")
    ));
    let after = repository.metadata().snapshot().await.unwrap();
    assert_eq!(after.revision(), before);
    assert!(after.object(&root_key).await.unwrap().is_some());
    assert!(after.object(&orphan_key).await.unwrap().is_some());

    *hidden.write().unwrap() = None;
    repository.collect().await.unwrap();
    assert!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .object(&orphan_key)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn physical_deletion_failure_leaks_after_safe_logical_prune() {
    let payloads = FailingDeleteStore {
        inner: MemoryBlobStore::new(),
    };
    let repository = Repository::new(payloads, MemoryMetadataStore::new().unwrap());
    let mutation = repository.mutation_session().await.unwrap();
    let orphan = mutation.stage_blob(b"leaked safely").await.unwrap();
    let key = orphan.record().key().clone();
    let payload = orphan.record().payload();
    mutation.publish_unrooted(vec![orphan]).await.unwrap();
    drop(mutation);

    assert!(matches!(
        repository.collect().await,
        Err(RepositoryError::Payload(crate::error::Error::Msg(_)))
    ));
    assert!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .object(&key)
            .await
            .unwrap()
            .is_none()
    );
    assert!(repository.payloads().has(&payload).await.unwrap());
    let ledger = repository.metadata().pin_store().await.unwrap();
    let inventory = ledger.inventory().await.unwrap();
    assert_eq!(inventory.deletions.len(), 1);
    assert!(
        inventory
            .deletions
            .values()
            .next()
            .unwrap()
            .contains(&crate::metadata::PinResource::Blob(payload))
    );
    assert!(
        ledger
            .register(crate::metadata::DataPin {
                scope: crate::metadata::PinScope::Staging,
                catalog: None,
                resources: BTreeSet::from([crate::metadata::PinResource::Blob(payload)]),
            })
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        matches!(repository.collect().await, Err(RepositoryError::Busy(message)) if message.contains("exact-token recovery"))
    );
    assert_eq!(
        ledger.inventory().await.unwrap().deletions,
        inventory.deletions
    );
    let healthy = Repository::new(
        repository.payloads().inner.clone(),
        repository.metadata().clone(),
    );
    healthy
        .recover_collection(inventory.collector.as_ref().unwrap())
        .await
        .unwrap();
    assert!(!healthy.payloads().has(&payload).await.unwrap());
    let recovered = ledger.inventory().await.unwrap();
    assert!(recovered.collector.is_none());
    assert!(recovered.deletions.is_empty());
}

#[tokio::test]
async fn overlapping_roots_keep_shared_payload_until_the_last_release() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let shared = mutation.stage_blob(b"shared").await.unwrap();
    let key = shared.record().key().clone();
    let payload = shared.record().payload();
    mutation
        .publish(
            vec![shared],
            vec![
                RootChange::Set {
                    name: "roots/a".parse().unwrap(),
                    target: key.clone(),
                },
                RootChange::Set {
                    name: "roots/b".parse().unwrap(),
                    target: key.clone(),
                },
            ],
        )
        .await
        .unwrap();
    mutation
        .publish(
            Vec::new(),
            vec![RootChange::Remove {
                name: "roots/a".parse().unwrap(),
            }],
        )
        .await
        .unwrap();
    drop(mutation);
    repository.collect().await.unwrap();
    assert!(repository.payloads().has(&payload).await.unwrap());

    repository
        .mutation_session()
        .await
        .unwrap()
        .publish(
            Vec::new(),
            vec![RootChange::Remove {
                name: "roots/b".parse().unwrap(),
            }],
        )
        .await
        .unwrap();
    repository.collect().await.unwrap();
    assert!(!repository.payloads().has(&payload).await.unwrap());
}

#[tokio::test]
async fn missing_rooted_payload_aborts_before_logical_prune() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let live = mutation.stage_blob(b"live").await.unwrap();
    let live_key = live.record().key().clone();
    let live_payload = live.record().payload();
    mutation
        .publish_rooted(
            vec![live],
            RootName::try_from("live").unwrap(),
            live_key.clone(),
        )
        .await
        .unwrap();
    drop(mutation);
    let before = repository.metadata().snapshot().await.unwrap().revision();
    repository
        .payloads()
        .delete_blob(&live_payload)
        .await
        .unwrap();
    assert!(matches!(
        repository.collect().await,
        Err(RepositoryError::Metadata(MetadataError::Corruption(_)))
    ));
    let after = repository.metadata().snapshot().await.unwrap();
    assert_eq!(after.revision(), before);
    assert!(after.object(&live_key).await.unwrap().is_some());
}

#[tokio::test]
async fn fsck_distinguishes_corruption_from_collectible_staging() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let live = mutation.stage_blob(b"live").await.unwrap();
    let live_key = live.record().key().clone();
    let live_payload = live.record().payload();
    let orphan = mutation.stage_blob(b"orphan").await.unwrap();
    mutation
        .publish_rooted(
            vec![live, orphan],
            RootName::try_from("live").unwrap(),
            live_key,
        )
        .await
        .unwrap();
    drop(mutation);

    let report = repository.fsck().await.unwrap();
    assert!(report.is_healthy());
    assert!(!report.is_clean());
    assert!(report.issues.iter().any(|issue| {
        issue.kind == FsckIssueKind::UnrootedObject
            && issue.disposition == FsckDisposition::Collectible
    }));

    let spilled_repository = repository.clone().with_spill_limits(SpillLimits {
        max_memory_objects: 1,
        ..SpillLimits::default()
    });
    let mut spilled = spilled_repository.fsck().await.unwrap();
    assert!(spilled.spill.files_opened > 0);
    spilled.spill = report.spill;
    assert_eq!(spilled, report);

    repository
        .payloads()
        .delete_blob(&live_payload)
        .await
        .unwrap();
    let report = repository.fsck().await.unwrap();
    assert!(!report.is_healthy());
    assert!(report.issues.iter().any(|issue| {
        issue.kind == FsckIssueKind::MissingPayload && issue.disposition == FsckDisposition::Corrupt
    }));
    let mut spilled = spilled_repository.fsck().await.unwrap();
    assert!(spilled.spill.files_opened > 0);
    spilled.spill = report.spill;
    assert_eq!(spilled, report);
}

#[tokio::test]
async fn fsck_batched_sets_preserve_shared_graphs_and_traversal_limits() {
    let source = tempfile::tempdir().unwrap();
    for group in 0..2 {
        let directory = source.path().join(format!("group-{group}"));
        std::fs::create_dir(&directory).unwrap();
        for index in 0u32..257 {
            std::fs::write(
                directory.join(format!("{group}-{index}")),
                index.to_le_bytes(),
            )
            .unwrap();
        }
    }
    let repository = repository();
    repository
        .import(crate::import::FilesystemImport::new(
            source.path(),
            RootName::try_from("shared").unwrap(),
        ))
        .await
        .unwrap();
    let expected = repository.fsck().await.unwrap();
    assert!(expected.is_clean());
    assert_eq!(expected.objects_checked, 260);
    for limit in [1, 17, 259, 260, 261, 1024] {
        let mut bounded = repository.clone().with_spill_limits(SpillLimits {
            max_memory_objects: limit,
            ..SpillLimits::default()
        });
        bounded.limits.max_traversal_objects = 260;
        let mut actual = bounded.fsck().await.unwrap();
        // Shared children appear twice in the queue even though sets
        // count them once. Its 514 pending edges also spill at 261.
        assert_eq!(actual.spill.files_opened > 0, limit <= 514);
        actual.spill = expected.spill;
        assert_eq!(actual, expected);
        bounded.limits.max_traversal_objects = 259;
        assert!(matches!(
            bounded.fsck().await,
            Err(RepositoryError::LimitExceeded(_))
        ));
    }
}

#[tokio::test]
async fn fsck_inventory_difference_preserves_garbage_findings_across_spill_limits() {
    let repository = chunked_repository(1024);
    let mutation = repository.mutation_session().await.unwrap();
    let live = mutation.stage_blob(&vec![42; 4096]).await.unwrap();
    let live_key = live.record().key().clone();
    let live_payload = live.record().payload();
    let mut garbage = BTreeSet::new();
    for byte in 1..=4 {
        let staged = mutation.stage_blob(&vec![byte; 4096]).await.unwrap();
        garbage.insert(staged.record().payload());
    }
    mutation
        .publish_rooted(vec![live], RootName::try_from("live").unwrap(), live_key)
        .await
        .unwrap();
    drop(mutation);

    let live_chunks: BTreeSet<_> = repository
        .payloads()
        .chunks(&live_payload)
        .await
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|chunk| chunk.digest)
        .collect();
    let all_chunks: BTreeSet<_> = repository
        .payloads()
        .list_chunks()
        .try_collect()
        .await
        .unwrap();
    let unused: Vec<_> = all_chunks.difference(&live_chunks).copied().collect();
    assert!(!unused.is_empty());
    let expected: Vec<_> = garbage
        .iter()
        .map(|payload| FsckIssue {
            disposition: FsckDisposition::Collectible,
            kind: FsckIssueKind::UnreferencedPayload,
            object: None,
            message: format!("physical payload {payload} has no logical record"),
        })
        .chain(unused.iter().map(|chunk| FsckIssue {
            disposition: FsckDisposition::Collectible,
            kind: FsckIssueKind::UnreferencedChunk,
            object: None,
            message: format!("physical chunk {chunk} has no logical payload"),
        }))
        .collect();
    let report = repository.fsck().await.unwrap();
    assert!(report.is_healthy());
    assert_eq!(report.payloads_checked, 1);
    assert_eq!(report.issues, expected);
    for max_memory_objects in [1, 2, 3] {
        let mut spilled = repository
            .clone()
            .with_spill_limits(SpillLimits {
                max_memory_objects,
                ..SpillLimits::default()
            })
            .fsck()
            .await
            .unwrap();
        assert!(spilled.spill.files_opened > 0);
        spilled.spill = report.spill;
        assert_eq!(spilled, report);
    }
}

#[tokio::test]
async fn filesystem_import_checkout_and_reimport_preserve_identity() {
    let source = tempfile::tempdir().unwrap();
    std::fs::create_dir(source.path().join("sub")).unwrap();
    std::fs::create_dir(source.path().join("empty")).unwrap();
    std::fs::write(source.path().join("file.txt"), b"hello").unwrap();
    std::fs::write(source.path().join("sub/data.bin"), [7u8; 32]).unwrap();
    let executable = source.path().join("run.sh");
    std::fs::write(&executable, b"#!/bin/sh\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&executable, permissions).unwrap();
        std::os::unix::fs::symlink("file.txt", source.path().join("link")).unwrap();
    }

    let repository = repository();
    let first = repository
        .import(crate::import::FilesystemImport::new(
            source.path(),
            RootName::try_from("trees/first").unwrap(),
        ))
        .await
        .unwrap();
    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("tree");
    repository.checkout(&first, &target).await.unwrap();
    let second = repository
        .import(crate::import::FilesystemImport::new(
            &target,
            RootName::try_from("trees/second").unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(std::fs::read(target.join("file.txt")).unwrap(), b"hello");
    assert!(target.join("empty").is_dir());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_ne!(
            std::fs::metadata(target.join("run.sh"))
                .unwrap()
                .permissions()
                .mode()
                & 0o100,
            0
        );
        assert_eq!(
            std::fs::read_link(target.join("link")).unwrap(),
            PathBuf::from("file.txt")
        );
    }
}

#[tokio::test]
async fn slice_staging_verifies_native_identity_without_reopening_storage() {
    let reads = Arc::new(AtomicUsize::new(0));
    let repository = Repository::new(
        CountingBlobStore {
            inner: MemoryBlobStore::new(),
            reads: reads.clone(),
        },
        MemoryMetadataStore::new().unwrap(),
    );
    let bytes = b"native Git blob content";
    let key = crate::git::git_object_key_for_body(
        crate::GitObjectFormat::Sha256,
        crate::GitObjectKind::Blob,
        bytes,
    )
    .unwrap();
    let mutation = repository.mutation_session().await.unwrap();
    let object = mutation.stage_object(key.clone(), bytes).await.unwrap();
    assert_eq!(object.record().key(), &key);
    assert_eq!(object.record().payload_size(), bytes.len() as u64);
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert!(
        mutation
            .stage_object(key.clone(), b"wrong content")
            .await
            .is_err()
    );
    let reread = mutation
        .stage_existing(key, object.record().payload())
        .await
        .unwrap();
    assert_eq!(reread.record(), object.record());
    assert_eq!(reads.load(Ordering::SeqCst), 1);

    let lying = Repository::new(
        LyingWriterStore {
            inner: MemoryBlobStore::new(),
            lie: WriterLie::Digest,
        },
        MemoryMetadataStore::new().unwrap(),
    );
    let mutation = lying.mutation_session().await.unwrap();
    assert!(matches!(
        mutation
            .stage_object(ObjectKey::blob(BlobId::new(Digest::hash(bytes))), bytes)
            .await,
        Err(RepositoryError::PayloadIdentityMismatch { .. })
    ));
}

#[tokio::test]
async fn overwrite_publication_rejects_backend_digest_and_proof_lies() {
    for lie in [WriterLie::Digest, WriterLie::Size] {
        let inner = MemoryBlobStore::new();
        let original = vec![3; 40_000];
        let id = inner.put_slice(&original).await.unwrap();
        let repository = Repository::new(
            LyingWriterStore { inner, lie },
            MemoryMetadataStore::new().unwrap(),
        );
        let mutation = repository.mutation_session().await.unwrap();
        let staged = mutation
            .stage_existing(ObjectKey::blob(id), id)
            .await
            .unwrap();
        let name = RootName::try_from("file").unwrap();
        mutation
            .publish_rooted(vec![staged], name.clone(), ObjectKey::blob(id))
            .await
            .unwrap();
        assert!(
            mutation
                .stage_blob_overwrite(&ObjectKey::blob(id), 16_380, &[42; 300])
                .await
                .is_err()
        );
        assert_eq!(
            repository
                .metadata()
                .snapshot()
                .await
                .unwrap()
                .root(&name)
                .await
                .unwrap(),
            Some(ObjectKey::blob(id))
        );
    }
}

#[tokio::test]
async fn filesystem_construction_proof_avoids_a_second_payload_walk() {
    let source = tempfile::tempdir().unwrap();
    std::fs::create_dir(source.path().join("nested")).unwrap();
    std::fs::create_dir(source.path().join("empty")).unwrap();
    std::fs::write(source.path().join("one"), b"one").unwrap();
    std::fs::write(source.path().join("nested/two"), b"two").unwrap();

    let reads = Arc::new(AtomicUsize::new(0));
    let repository = Repository::new(
        CountingBlobStore {
            inner: MemoryBlobStore::new(),
            reads: reads.clone(),
        },
        MemoryMetadataStore::new().unwrap(),
    );
    let root = repository
        .import(crate::import::FilesystemImport::new(
            source.path(),
            RootName::try_from("trees/constructed").unwrap(),
        ))
        .await
        .unwrap();

    // The content-addressed writers and canonical directory encodings
    // prove the five staged payloads without reopening them. Publication
    // must not read them or reopen child directories either.
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .validated_closures(&[root])
            .await
            .unwrap(),
        vec![true]
    );
}

#[tokio::test]
async fn raw_blob_fast_seal_rejects_dishonest_writer_completion() {
    for lie in [WriterLie::Digest, WriterLie::Size] {
        let repository = Repository::new(
            LyingWriterStore {
                inner: MemoryBlobStore::new(),
                lie,
            },
            MemoryMetadataStore::new().unwrap(),
        );
        let mutation = repository.mutation_session().await.unwrap();
        let mut source = b"independently observed source bytes".as_slice();
        let error = match mutation.stage_blob_reader(&mut source).await {
            Ok(_) => panic!("a dishonest writer completion must not produce a seal"),
            Err(error) => error,
        };
        match lie {
            WriterLie::Digest => {
                assert!(matches!(
                    error,
                    RepositoryError::PayloadIdentityMismatch { .. }
                ));
            }
            WriterLie::Size => {
                assert!(matches!(error, RepositoryError::PayloadSizeMismatch { .. }));
            }
        }
        assert_eq!(error.category(), RepositoryErrorCategory::InvalidData);
        assert_eq!(error.retry_disposition(), crate::RetryDisposition::Never);
    }
}

#[tokio::test]
async fn a_small_packed_import_performs_no_post_staging_range_reads() {
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("source");
    std::fs::create_dir_all(source.join("nested")).unwrap();
    std::fs::write(source.join("one"), b"one").unwrap();
    std::fs::write(source.join("nested/two"), b"two").unwrap();
    let repository = Repository::local(temporary.path().join("repository"))
        .await
        .unwrap();
    repository.payloads().reset_pack_read_stats();

    repository
        .import(crate::import::FilesystemImport::new(
            &source,
            RootName::try_from("trees/packed").unwrap(),
        ))
        .await
        .unwrap();

    let stats = repository.payloads().pack_read_stats().unwrap();
    assert_eq!(stats.chunk_range_requests, 0);
    assert_eq!(stats.whole_pack_requests, 0);
    assert_eq!(
        stats.index_pointer_requests, 0,
        "the state catalog must cover every negative payload probe"
    );
}

#[tokio::test]
async fn packed_fsck_groups_reads_in_physical_order_under_cache_pressure() {
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("source");
    std::fs::create_dir(&source).unwrap();
    for index in 0..256_u32 {
        let mut bytes = Vec::with_capacity(1024);
        for block in 0..32_u32 {
            bytes.extend_from_slice(
                blake3::hash(format!("fsck-{index}-{block}").as_bytes()).as_bytes(),
            );
        }
        std::fs::write(source.join(format!("file-{index:04}")), bytes).unwrap();
    }
    let payloads = ChunkedBlobStore::packed_with_options(
        Arc::new(object_store::memory::InMemory::new()),
        object_store::path::Path::default(),
        crate::DEFAULT_AVG_CHUNK_SIZE,
        crate::PackOptions {
            target_size: 16 * 1024,
            cache_capacity: 32 * 1024,
        },
    )
    .await
    .unwrap();
    let repository = Repository::new(payloads, MemoryMetadataStore::new().unwrap());
    repository
        .import(crate::import::FilesystemImport::new(
            &source,
            RootName::try_from("trees/fsck-order").unwrap(),
        ))
        .await
        .unwrap();
    repository.payloads().reset_pack_read_stats();

    assert!(repository.fsck_repair(None).await.unwrap().is_healthy());
    assert!(repository.fsck().await.unwrap().is_clean());

    let stats = repository.payloads().pack_read_stats().unwrap();
    assert!(
        stats.cache_evictions > 0,
        "fixture must exceed the pack cache"
    );
    assert!(
        stats.whole_pack_requests < 100,
        "physical ordering should prevent repeated pack reloads: {stats:?}"
    );
}

#[tokio::test]
async fn a_failed_large_walk_leaves_only_bounded_durable_checkpoints() {
    let source = tempfile::tempdir().unwrap();
    for index in 0..1_100u32 {
        std::fs::write(
            source.path().join(format!("file-{index:05}")),
            format!("bounded-{index}"),
        )
        .unwrap();
    }
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        FormatRegistry::builtin(),
        FormatLimits {
            max_batch_objects: 256,
            // The first 1,024-entry page completes, then the second page
            // fails before it can be yielded to the importer.
            max_traversal_objects: 1_050,
            ..FormatLimits::default()
        },
    );

    assert!(
        repository
            .import(crate::import::FilesystemImport::new(
                source.path(),
                RootName::try_from("trees/too-wide").unwrap()
            ))
            .await
            .is_err()
    );
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert!(
        snapshot
            .root(&RootName::try_from("trees/too-wide").unwrap())
            .await
            .unwrap()
            .is_none(),
        "a partial import must never publish the final root"
    );
    assert_eq!(
        snapshot
            .objects()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        crate::filesystem::WALK_PAGE_ENTRIES,
        "the completed page should survive as reusable bounded progress"
    );
}

#[tokio::test]
async fn filesystem_construction_proof_survives_a_stale_revision_retry() {
    let source = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("file"), b"retry-safe").unwrap();
    let repository = Repository::new(
        MemoryBlobStore::new(),
        StaleOnceMetadataStore {
            inner: MemoryMetadataStore::new().unwrap(),
            stale_once: Arc::new(AtomicBool::new(true)),
        },
    );

    let root = repository
        .import(crate::import::FilesystemImport::new(
            source.path(),
            RootName::try_from("trees/retried").unwrap(),
        ))
        .await
        .unwrap();
    assert!(matches!(
        repository.verify_closure(&root).await.unwrap(),
        ClosureStatus::Complete { objects: 2 }
    ));
}

#[tokio::test]
async fn generic_formats_cannot_claim_a_filesystem_construction_proof() {
    let repository = graph_repository();
    let mutation = repository.mutation_session().await.unwrap();
    let object = stage_graph(&mutation, 1, &[]).await;
    assert!(matches!(
        mutation
            .publish_filesystem_constructed(vec![object], Vec::new())
            .await,
        Err(RepositoryError::InvalidInput(message))
            if message.contains("filesystem construction proof")
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn filesystem_nar_reread_bypasses_recognition() {
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("file"), b"source bytes").unwrap();
    let root = crate::filesystem::root::FsRoot::open_read(&source)
        .await
        .unwrap();
    let identity = root
        .walk_post_order(8)
        .await
        .unwrap()
        .into_iter()
        .find_map(|entry| match entry {
            crate::filesystem::root::RootedEntry::File { identity, .. } => identity,
            _ => None,
        })
        .unwrap();
    let repository = Repository::local(temporary.path().join("repository"))
        .await
        .unwrap();
    let mutation = repository.mutation_session().await.unwrap();
    let object = mutation.stage_blob(b"cached bytes").await.unwrap();
    let digest = BlobId::new(object.record().key().native_digest().unwrap());
    let key = object.record().key().clone();
    mutation
        .publish_rooted(vec![object], RootName::try_from("cached").unwrap(), key)
        .await
        .unwrap();
    drop(mutation);
    // Deliberately seed a stale recognition entry for an unchanged identity.
    repository
        .profile
        .ingest_cache()
        .unwrap()
        .remember(&[(identity, digest)])
        .await
        .unwrap();
    let repository = crate::api::Repository {
        inner: repository.into_builtin(),
    };
    let cached = repository
        .import(crate::import::FilesystemNarImport::new(&source))
        .await
        .unwrap();
    let fresh = repository
        .import(crate::import::FilesystemNarImport::new(&source).reread(true))
        .await
        .unwrap();
    assert_ne!(cached.nar_sha256(), fresh.nar_sha256());
    let file = repository
        .import(crate::import::FilesystemNarImport::new(source.join("file")))
        .await
        .unwrap();
    let directory = crate::Directory::decode(&{
        let key = match fresh.root() {
            crate::Node::Directory { digest, .. } => ObjectKey::directory(*digest),
            _ => unreachable!(),
        };
        let mut reader = fresh.reader().open_verified(&key).await.unwrap().unwrap();
        let mut bytes = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut bytes)
            .await
            .unwrap();
        bytes
    })
    .unwrap();
    assert_eq!(directory.get(b"file"), Some(file.root()));
}

#[cfg(unix)]
#[tokio::test]
async fn ingest_cache_requires_a_validated_closure_witness() {
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("file"), b"cached bytes").unwrap();
    let root = crate::filesystem::root::FsRoot::open_read(&source)
        .await
        .unwrap();
    let identity = root
        .walk_post_order(8)
        .await
        .unwrap()
        .into_iter()
        .find_map(|entry| match entry {
            crate::filesystem::root::RootedEntry::File { identity, .. } => identity,
            _ => None,
        })
        .unwrap();

    let repository = Repository::local(temporary.path().join("repository"))
        .await
        .unwrap();
    let mutation = repository.mutation_session().await.unwrap();
    let object = mutation.stage_blob(b"cached bytes").await.unwrap();
    let key = object.record().key().clone();
    let digest = BlobId::new(key.native_digest().unwrap());
    mutation.publish_unrooted(vec![object]).await.unwrap();
    drop(mutation);
    repository
        .profile
        .ingest_cache()
        .unwrap()
        .remember(&[(identity, digest)])
        .await
        .unwrap();

    let mutation = repository.mutation_session().await.unwrap();
    assert_eq!(
        mutation
            .recognize_files(vec![Some(identity)])
            .await
            .unwrap(),
        vec![None],
        "an unrooted object without a closure witness must be reread"
    );
    mutation
        .publish_rooted(
            Vec::new(),
            RootName::try_from("blobs/validated").unwrap(),
            key,
        )
        .await
        .unwrap();
    drop(mutation);

    let mutation = repository.mutation_session().await.unwrap();
    let mut changed = identity;
    changed.ctime_nsec ^= 1;
    assert_eq!(
        mutation
            .recognize_files(vec![Some(identity), None, Some(changed), Some(identity)])
            .await
            .unwrap(),
        vec![
            Some((b"cached bytes".len() as u64, digest)),
            None,
            None,
            Some((b"cached bytes".len() as u64, digest)),
        ]
    );
    assert!(
        mutation
            .recognize_files(Vec::new())
            .await
            .unwrap()
            .is_empty()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn composed_repository_takes_on_the_local_profile() {
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("file"), b"composed bytes").unwrap();
    let root = crate::filesystem::root::FsRoot::open_read(&source)
        .await
        .unwrap();
    let identity = root
        .walk_post_order(8)
        .await
        .unwrap()
        .into_iter()
        .find_map(|entry| match entry {
            crate::filesystem::root::RootedEntry::File { identity, .. } => identity,
            _ => None,
        })
        .unwrap();

    // Only public constructors: caller-owned backends below one directory,
    // plus the local profile rooted at that same directory.
    let store_root = temporary.path().join("store");
    std::fs::create_dir_all(store_root.join("blobs")).unwrap();
    let payloads = crate::ChunkedBlobStore::local_packed(store_root.join("blobs"))
        .await
        .unwrap();
    let state = crate::TursoMetadataStore::open(store_root.join("casita.sqlite"))
        .await
        .unwrap();
    let profile = RepositoryProfile::local(&store_root)
        .await
        .unwrap()
        .with_ingest_cache(&state);
    let composed = Repository::new(payloads, state).with_profile(profile);
    let generic = Repository::new(composed.payloads().clone(), composed.metadata().clone());

    assert!(composed.profile().coordinates_processes());
    assert!(composed.profile().collects_in_emergency());
    assert!(
        composed.profile().mutation_start().is_some(),
        "the local profile runs disk-pressure maintenance before mutations"
    );
    assert!(!generic.profile().coordinates_processes());
    assert!(!generic.profile().collects_in_emergency());
    assert!(generic.profile().mutation_start().is_none());
    // Builders that edit the profile keep the maintenance hook it installed.
    let bounded = composed.clone().with_spill_limits(SpillLimits {
        max_memory_objects: 1,
        ..SpillLimits::default()
    });
    assert!(bounded.profile().mutation_start().is_some());
    assert_eq!(composed.spill_limits(), SpillLimits::default());

    composed
        .import(crate::import::FilesystemImport::new(
            &source,
            RootName::try_from("trees/composed").unwrap(),
        ))
        .await
        .unwrap();

    // The import remembered the file, so re-importing it would not read it.
    let recognized = composed
        .mutation_session()
        .await
        .unwrap()
        .recognize_files(vec![Some(identity)])
        .await
        .unwrap();
    assert!(
        matches!(recognized.as_slice(), [Some((size, _))] if *size == b"composed bytes".len() as u64),
        "{recognized:?}"
    );
    // The same backends without the profile have no accelerator to consult.
    assert_eq!(
        generic
            .mutation_session()
            .await
            .unwrap()
            .recognize_files(vec![Some(identity)])
            .await
            .unwrap(),
        vec![None]
    );
}

#[tokio::test]
async fn reopen_and_fsck_recover_after_publication_commit_failure() {
    let temporary = tempfile::tempdir().unwrap();
    let store_root = temporary.path().join("store");
    let source = temporary.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("data"), b"staged before crash").unwrap();

    let base = Repository::local(&store_root).await.unwrap();
    let repository = Repository::with_formats(
        base.payloads().clone(),
        FailingCommitMetadataStore {
            inner: base.metadata().clone(),
            fail_once: Arc::new(AtomicBool::new(true)),
        },
        FormatRegistry::builtin(),
        FormatLimits::default(),
    )
    .with_fs_coordination(&store_root);
    drop(base);
    assert!(matches!(
        repository
            .import(crate::import::FilesystemImport::new(&source, RootName::try_from("trees/interrupted").unwrap()))
            .await,
        Err(RepositoryError::Metadata(MetadataError::Backend(message)))
            if message.contains("injected failure")
    ));
    drop(repository);

    let reopened = Repository::local(&store_root).await.unwrap();
    let snapshot = reopened.metadata().snapshot().await.unwrap();
    assert!(
        snapshot
            .root(&RootName::try_from("trees/interrupted").unwrap())
            .await
            .unwrap()
            .is_none()
    );
    drop(snapshot);
    let report = reopened.fsck().await.unwrap();
    assert!(
        report.is_clean(),
        "uncommitted payloads are outside the catalog"
    );
    let packs = store_root.join("blobs/packs/b3");
    assert!(std::fs::read_dir(&packs).unwrap().any(|entry| {
        std::fs::read_dir(entry.unwrap().path())
            .unwrap()
            .next()
            .is_some()
    }));
    reopened.collect().await.unwrap();
    assert!(
        std::fs::read_dir(&packs).unwrap().all(|entry| {
            std::fs::read_dir(entry.unwrap().path())
                .unwrap()
                .next()
                .is_none()
        }),
        "collection must remove abandoned uploads"
    );
    assert!(reopened.fsck().await.unwrap().is_clean());
}

#[tokio::test]
async fn local_catalog_reclamation_defers_a_prepared_publication() {
    let temporary = tempfile::tempdir().unwrap();
    let repository = Repository::local(temporary.path()).await.unwrap();
    let mutation = repository.mutation_session().await.unwrap();
    let object = mutation
        .stage_blob(b"publication survives cleanup")
        .await
        .unwrap();
    let key = object.record().key().clone();
    // Hold the real publication boundary while another writer is admitted.
    let guard = repository.publication.lock().await;
    let prepared = repository
        .payloads()
        .publication()
        .prepare_state_commit()
        .await
        .unwrap();
    assert!(prepared.catalog().is_some());
    let marker = temporary.path().join("blobs/pack-index-reclaim-needed");
    std::fs::write(&marker, b"catalog garbage may be present\n").unwrap();
    assert!(matches!(
        repository.try_reclaim_metadata().await,
        Err(RepositoryError::Busy(_))
    ));
    let concurrent = repository.mutation_session().await.unwrap();
    assert!(marker.exists(), "busy cleanup must remain pending");
    drop(concurrent);
    prepared.abort().unwrap();
    drop(guard);
    mutation
        .publish_rooted(vec![object], "kept".parse().unwrap(), key)
        .await
        .unwrap();
    drop(mutation);
    crate::flush_repository_leases().await.unwrap();
    let retry = repository.mutation_session().await.unwrap();
    assert!(
        !marker.exists(),
        "busy maintenance must retry on the next start"
    );
    drop(retry);
    crate::flush_repository_leases().await.unwrap();
    assert!(repository.fsck().await.unwrap().is_clean());
    drop(repository);
    let reopened = Repository::local(temporary.path()).await.unwrap();
    assert!(reopened.fsck().await.unwrap().is_clean());
}

fn install_catalog_garbage(root: &Path, seed: &[u8]) -> (PathBuf, PathBuf) {
    let digest = Digest::from(blake3::hash(seed)).to_hex();
    let object = root
        .join("blobs/pack-indexes/b3")
        .join(&digest[..2])
        .join(digest.as_str());
    std::fs::create_dir_all(object.parent().unwrap()).unwrap();
    std::fs::write(&object, b"unreachable catalog object").unwrap();
    let marker = root.join("blobs/pack-index-reclaim-needed");
    std::fs::write(&marker, b"catalog garbage may be present\n").unwrap();
    (object, marker)
}

#[tokio::test]
async fn local_catalog_reclamation_runs_before_mutation_and_during_reopen() {
    let temporary = tempfile::tempdir().unwrap();
    let store_root = temporary.path().join("store");
    let repository = Repository::local(&store_root).await.unwrap();

    let (object, marker) = install_catalog_garbage(&store_root, b"live handle garbage");
    let mutation = repository.mutation_session().await.unwrap();
    assert!(!object.exists());
    assert!(!marker.exists());
    drop(mutation);
    drop(repository);

    let (object, marker) = install_catalog_garbage(&store_root, b"restart garbage");
    let reopened = Repository::local(&store_root).await.unwrap();
    assert!(!object.exists());
    assert!(!marker.exists());
    drop(reopened);

    let reader = Repository::local(&store_root).await.unwrap();
    let writer = Repository::local(&store_root).await.unwrap();
    let (object, marker) = install_catalog_garbage(&store_root, b"garbage unrelated to reader");
    let hold = reader.retention_hold().await.unwrap();
    let mutation = writer.mutation_session().await.unwrap();
    assert!(!object.exists());
    // The marker can remain while a reader's historical catalog is pinned.
    assert!(marker.exists());
    drop(mutation);
    drop(hold);

    crate::flush_repository_leases().await.unwrap();
    for start in 1..=METADATA_RECLAIM_INTERVAL {
        let mutation = writer.mutation_session().await.unwrap();
        assert!(!object.exists());
        assert_eq!(marker.exists(), start < METADATA_RECLAIM_INTERVAL);
        drop(mutation);
        crate::flush_repository_leases().await.unwrap();
    }
}

#[tokio::test]
async fn local_catalog_reclamation_has_a_shared_bounded_deferral() {
    let temporary = tempfile::tempdir().unwrap();
    let repository = Repository::local(temporary.path()).await.unwrap();
    let clone = repository.clone();
    // A start without pending work must not defer the first actual sweep.
    drop(repository.mutation_session().await.unwrap());
    crate::flush_repository_leases().await.unwrap();
    let (first, _) = install_catalog_garbage(temporary.path(), b"first sweep");
    drop(repository.mutation_session().await.unwrap());
    assert!(!first.exists());
    crate::flush_repository_leases().await.unwrap();

    let (object, marker) = install_catalog_garbage(temporary.path(), b"deferred sweep");
    for start in 1..=METADATA_RECLAIM_INTERVAL {
        let handle = if start % 2 == 0 { &repository } else { &clone };
        drop(handle.mutation_session().await.unwrap());
        assert_eq!(object.exists(), start < METADATA_RECLAIM_INTERVAL);
        assert_eq!(marker.exists(), start < METADATA_RECLAIM_INTERVAL);
        crate::flush_repository_leases().await.unwrap();
    }

    let (object, _) = install_catalog_garbage(temporary.path(), b"explicit vacuum");
    drop(clone.mutation_session().await.unwrap());
    assert!(object.exists(), "the deferral must still be active");
    crate::flush_repository_leases().await.unwrap();
    repository.vacuum().await.unwrap();
    assert!(!object.exists(), "vacuum must bypass admission deferral");

    let (object, _) = install_catalog_garbage(temporary.path(), b"reopen during deferral");
    drop(repository.mutation_session().await.unwrap());
    assert!(object.exists());
    drop(clone);
    drop(repository);
    crate::flush_repository_leases().await.unwrap();
    let reopened = Repository::local(temporary.path()).await.unwrap();
    assert!(!object.exists(), "reopen must bypass admission deferral");
    assert!(reopened.fsck().await.unwrap().is_clean());
}

#[tokio::test]
async fn local_collection_recovers_an_interrupted_logical_prune_fence() {
    let directory = tempfile::tempdir().unwrap();
    let repository = Repository::local(directory.path()).await.unwrap();
    let mutation = repository.mutation_session().await.unwrap();
    let object = mutation.stage_blob(b"interrupted prune").await.unwrap();
    mutation.publish_unrooted(vec![object]).await.unwrap();
    drop(mutation);
    crate::flush_repository_leases().await.unwrap();
    let ledger = repository.metadata().pin_store().await.unwrap();
    ledger
        .begin_prune(ledger.inventory().await.unwrap().revision)
        .await
        .unwrap()
        .unwrap();
    let collected = repository.collect().await.unwrap();
    assert_eq!(collected.removed.logical_objects, 1);
    let inventory = ledger.inventory().await.unwrap();
    assert!(inventory.logical_prune.is_none());
    assert!(inventory.deletions.is_empty());
}

#[tokio::test]
async fn reopen_and_fsck_recover_after_post_prune_sweep_failure() {
    let temporary = tempfile::tempdir().unwrap();
    let store_root = temporary.path().join("store");
    let object = {
        let repository = Repository::local(&store_root).await.unwrap();
        let mutation = repository.mutation_session().await.unwrap();
        let object = mutation.stage_blob(b"orphan before prune").await.unwrap();
        let key = object.record().key().clone();
        mutation.publish_unrooted(vec![object]).await.unwrap();
        key
    };

    let base = Repository::local(&store_root).await.unwrap();
    let repository = Repository::with_formats(
        FailingDeleteStore {
            inner: base.payloads().clone(),
        },
        base.metadata().clone(),
        FormatRegistry::builtin(),
        FormatLimits::default(),
    )
    .with_fs_coordination(&store_root);
    drop(base);
    assert!(matches!(
        repository.collect().await,
        Err(RepositoryError::Payload(crate::error::Error::Msg(message)))
            if message.contains("injected physical deletion failure")
    ));
    assert!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .object(&object)
            .await
            .unwrap()
            .is_none()
    );
    drop(repository);

    let reopened = Repository::local(&store_root).await.unwrap();
    let report = reopened.fsck().await.unwrap();
    assert!(report.is_healthy());
    assert!(!report.is_clean());
    assert!(report.issues.iter().any(|issue| {
        matches!(
            issue.kind,
            FsckIssueKind::UnreferencedPayload | FsckIssueKind::UnreferencedChunk
        ) && issue.disposition == FsckDisposition::Collectible
    }));
    reopened.collect().await.unwrap();
    assert!(reopened.fsck().await.unwrap().is_clean());
}

#[tokio::test]
async fn local_repository_reopens_generic_state_and_payloads() {
    let temporary = tempfile::tempdir().unwrap();
    let store_root = temporary.path().join("store");
    let source = temporary.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("data"), b"persistent").unwrap();

    let root = {
        let repository = Repository::local(&store_root).await.unwrap();
        repository
            .import(crate::import::FilesystemImport::new(
                &source,
                RootName::try_from("trees/main").unwrap(),
            ))
            .await
            .unwrap()
    };

    let repository = Repository::local(&store_root).await.unwrap();
    assert!(matches!(
        repository.verify_closure(&root).await.unwrap(),
        ClosureStatus::Complete { .. }
    ));
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert_eq!(
        snapshot
            .root(&RootName::try_from("trees/main").unwrap())
            .await
            .unwrap(),
        Some(root)
    );
    drop(snapshot);
    assert!(repository.fsck().await.unwrap().is_clean());
}

#[cfg(target_os = "linux")]
mod full_disk_crash {
    use std::fs::{File, OpenOptions};
    use std::io::{self, Write as _};
    use std::os::unix::process::ExitStatusExt as _;
    use std::path::{Path, PathBuf};
    use std::process::Command;

    use super::*;

    const NAMESPACE_ENV: &str = "CASITA_CRASH_GC_NAMESPACE_CHILD";
    const WORKER_ENV: &str = "CASITA_CRASH_GC_WORKER";
    const ROOT_ENV: &str = "CASITA_CRASH_GC_ROOT";
    const UNMOUNTED_TEST_NAME: &str = concat!(
        "repository::tests::full_disk_crash::",
        "local_emergency_sweep_recovers_after_abort_without_a_mount"
    );
    const TEST_NAME: &str = concat!(
        "repository::tests::full_disk_crash::",
        "local_emergency_sweep_recovers_after_real_process_abort"
    );

    /// The same crash and recovery as the filesystem-level test below,
    /// with the exhausted state engine stood in for rather than mounted.
    ///
    /// Everything that matters here is still real: a real local
    /// repository, a real payload sweep, a real process death at the
    /// emergency boundary, and a real reopen afterwards. Only the reason
    /// the commit could not proceed is injected, which is what lets this
    /// run on a machine that forbids unprivileged namespaces.
    #[test]
    fn local_emergency_sweep_recovers_after_abort_without_a_mount() {
        if std::env::var_os(WORKER_ENV).is_some() {
            let root = PathBuf::from(std::env::var_os(ROOT_ENV).unwrap());
            let runtime = runtime();
            runtime.block_on(async move {
                let repository = Repository::local(root).await.unwrap();
                let result = repository.collect().await;
                panic!("emergency collection returned instead of aborting: {result:?}");
            });
            return;
        }

        for post_prune in [false, true] {
            let temporary = tempfile::tempdir().unwrap();
            let repository_root = temporary.path().join("repository");
            let runtime = runtime();
            let (rooted_key, rooted_payload, stale_key, stale_payload) =
                runtime.block_on(prepare_repository(&repository_root));

            let worker = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", UNMOUNTED_TEST_NAME, "--nocapture"])
                .env(WORKER_ENV, "1")
                .env(ROOT_ENV, &repository_root)
                .env(
                    if post_prune {
                        "CASITA_TEST_POST_PRUNE_STORAGE_FULL_ONCE"
                    } else {
                        "CASITA_TEST_STORAGE_FULL_ONCE"
                    },
                    "1",
                )
                .env("CASITA_TEST_ABORT_AFTER_EMERGENCY_SWEEP", "1")
                .env_remove(NAMESPACE_ENV)
                .output()
                .expect("launch emergency collection worker");
            assert_eq!(
                worker.status.signal(),
                Some(libc::SIGABRT),
                "worker did not abort at the emergency boundary: {worker:?}"
            );

            runtime.block_on(async move {
                let repository = Repository::local(&repository_root).await.unwrap();
                // The pre-prune failure leaves a stale logical record; the
                // post-prune failure has already committed its removal.
                let snapshot = repository.metadata().snapshot().await.unwrap();
                assert_eq!(
                    snapshot.object(&stale_key).await.unwrap().is_some(),
                    !post_prune
                );
                drop(snapshot);
                assert!(!repository.payloads().has(&stale_payload).await.unwrap());
                assert!(repository.payloads().has(&rooted_payload).await.unwrap());
                // The worker has stopped: verify the crash witness directly
                // before recovery. Online scans must respect its abandoned fence.
                let mut reader = repository
                    .payloads()
                    .open_read(&rooted_payload)
                    .await
                    .unwrap()
                    .unwrap();
                let mut bytes = Vec::new();
                reader.read_to_end(&mut bytes).await.unwrap();
                assert_eq!(bytes, b"rooted crash witness");
                drop(reader);
                assert!(matches!(
                    repository.fsck().await,
                    Err(RepositoryError::Busy(_))
                ));

                let outcome = repository.collect().await.unwrap();
                assert_eq!(outcome.removed.logical_objects, usize::from(!post_prune));
                assert!(matches!(
                    repository.verify_closure(&rooted_key).await.unwrap(),
                    ClosureStatus::Complete { .. }
                ));
                assert!(repository.fsck().await.unwrap().is_clean());
            });
        }
    }

    #[test]
    fn local_emergency_sweep_recovers_after_real_process_abort() {
        if std::env::var_os(WORKER_ENV).is_some() {
            let root = PathBuf::from(std::env::var_os(ROOT_ENV).unwrap());
            let runtime = runtime();
            runtime.block_on(async move {
                let repository = Repository::local(root).await.unwrap();
                // The cfg(test) hook aborts after the stale physical set is
                // gone and before the logical retry starts.
                let result = repository.collect().await;
                panic!("emergency collection returned instead of aborting: {result:?}");
            });
            return;
        }

        if std::env::var_os(NAMESPACE_ENV).is_some() {
            run_supervisor();
            return;
        }

        // A machine that refuses unprivileged namespaces is answering
        // about itself, not about casita. The recovery this proves is
        // covered without a mount by the test above; what only a real
        // filesystem can show is that a kernel ENOSPC arrives as
        // `StorageFull`, so this reports the gap instead of failing.
        if !mount_namespaces_available() {
            eprintln!(
                "SKIP {TEST_NAME}: this machine does not permit unprivileged user and \
                 mount namespaces, so a full filesystem cannot be created to test against"
            );
            return;
        }

        let output = Command::new("unshare")
            .args(["--user", "--map-root-user", "--mount"])
            .arg(std::env::current_exe().unwrap())
            .args(["--exact", TEST_NAME, "--nocapture"])
            .env(NAMESPACE_ENV, "1")
            .env_remove(WORKER_ENV)
            .output()
            .expect("launch crash recovery test in an unprivileged mount namespace");
        assert!(
            output.status.success(),
            "crash recovery supervisor failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    /// Whether this machine lets an unprivileged process create the user
    /// and mount namespaces a real full filesystem needs.
    fn mount_namespaces_available() -> bool {
        Command::new("unshare")
            .args(["--user", "--map-root-user", "--mount", "true"])
            .output()
            .map(|probe| probe.status.success())
            .unwrap_or(false)
    }

    fn run_supervisor() {
        let filesystem = MountedTmpfs::new(64 * 1024 * 1024);
        let repository_root = filesystem.path().join("repository");
        let runtime = runtime();
        let (rooted_key, rooted_payload, stale_key, stale_payload) =
            runtime.block_on(prepare_repository(&repository_root));

        let probe_path = filesystem.path().join("enospc-probe");
        File::create(&probe_path).unwrap();
        let mut filler = File::create(filesystem.path().join("filler")).unwrap();
        let block = vec![0xa5; 1024 * 1024];
        assert_enospc(&fill(&mut filler, &block));
        assert_eq!(free_space(filesystem.path()), 0);
        let mut probe = OpenOptions::new().write(true).open(&probe_path).unwrap();
        assert_enospc(&probe.write_all(&[0x5a; 4096]).unwrap_err());
        drop(probe);

        let worker = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST_NAME, "--nocapture"])
            .env(NAMESPACE_ENV, "1")
            .env(WORKER_ENV, "1")
            .env(ROOT_ENV, &repository_root)
            .env("CASITA_TEST_ABORT_AFTER_EMERGENCY_SWEEP", "1")
            .output()
            .expect("launch emergency collection worker");
        assert_eq!(
            worker.status.signal(),
            Some(libc::SIGABRT),
            "worker did not abort at the emergency boundary: {worker:?}"
        );

        runtime.block_on(async move {
            let repository = Repository::local(&repository_root).await.unwrap();
            let snapshot = repository.metadata().snapshot().await.unwrap();
            // Reused WAL space may let the logical prune commit before the
            // physical allocation fails. Both orders must recover safely.
            let remaining_logical =
                usize::from(snapshot.object(&stale_key).await.unwrap().is_some());
            drop(snapshot);
            assert!(!repository.payloads().has(&stale_payload).await.unwrap());
            assert!(repository.payloads().has(&rooted_payload).await.unwrap());
            // The worker has stopped: verify the crash witness directly
            // before recovery. Online scans must respect its abandoned fence.
            let mut reader = repository
                .payloads()
                .open_read(&rooted_payload)
                .await
                .unwrap()
                .unwrap();
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, b"rooted crash witness");
            drop(reader);
            assert!(matches!(
                repository.fsck().await,
                Err(RepositoryError::Busy(_))
            ));

            let outcome = repository.collect().await.unwrap();
            assert_eq!(outcome.removed.logical_objects, remaining_logical);
            assert!(repository.fsck().await.unwrap().is_clean());
            assert!(matches!(
                repository.verify_closure(&rooted_key).await.unwrap(),
                ClosureStatus::Complete { .. }
            ));
        });
        drop(filler);
    }

    async fn prepare_repository(root: &Path) -> (ObjectKey, BlobId, ObjectKey, BlobId) {
        let repository = Repository::local(root).await.unwrap();
        let mutation = repository.mutation_session().await.unwrap();
        let rooted = mutation.stage_blob(b"rooted crash witness").await.unwrap();
        let rooted_key = rooted.record().key().clone();
        let rooted_payload = rooted.record().payload();
        mutation
            .publish_rooted(
                vec![rooted],
                RootName::try_from("full-disk/crash-live").unwrap(),
                rooted_key.clone(),
            )
            .await
            .unwrap();

        let stale_bytes = deterministic_bytes(12 * 1024 * 1024);
        let stale = mutation.stage_blob(&stale_bytes).await.unwrap();
        let stale_key = stale.record().key().clone();
        let stale_payload = stale.record().payload();
        mutation.publish_unrooted(vec![stale]).await.unwrap();
        drop(mutation);
        drop(repository);
        // The supervisor stops driving this current-thread runtime while
        // its child collects. Finish the setup operation's durable release
        // before that handoff; a paused owner is still a live pin owner.
        crate::flush_repository_leases().await.unwrap();
        (rooted_key, rooted_payload, stale_key, stale_payload)
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn fill(file: &mut File, block: &[u8]) -> io::Error {
        loop {
            match file.write(block) {
                Ok(0) => panic!("filler made no progress before ENOSPC"),
                Ok(_) => {}
                Err(error) => return error,
            }
        }
    }

    fn assert_enospc(error: &io::Error) {
        assert_eq!(error.raw_os_error(), Some(libc::ENOSPC));
    }

    fn free_space(path: &Path) -> u64 {
        fs4::statvfs(path).unwrap().free_space()
    }

    fn deterministic_bytes(len: usize) -> Vec<u8> {
        let mut state = 0x4d59_5df4_d0f3_3173u64;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect()
    }

    struct MountedTmpfs {
        directory: tempfile::TempDir,
        path: PathBuf,
    }

    impl MountedTmpfs {
        fn new(bytes: u64) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().to_path_buf();
            let status = Command::new("mount")
                .args(["-t", "tmpfs", "-o"])
                .arg(format!("size={bytes},nr_inodes=16384"))
                .arg("tmpfs")
                .arg(&path)
                .status()
                .expect("execute mount");
            assert!(status.success(), "mount tmpfs failed: {status}");
            Self { directory, path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for MountedTmpfs {
        fn drop(&mut self) {
            let status = Command::new("umount")
                .args(["--lazy"])
                .arg(&self.path)
                .status();
            if !std::thread::panicking() && !matches!(status, Ok(status) if status.success()) {
                panic!("unmount tmpfs failed: {status:?}");
            }
            let _ = &self.directory;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_after_a_spill_opens_removes_its_temporary_files() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    for group in 0..8 {
        let directory = source.join(format!("group-{group}"));
        std::fs::create_dir_all(&directory).unwrap();
        for entry in 0..8 {
            std::fs::write(
                directory.join(format!("entry-{entry}")),
                format!("{group}/{entry}"),
            )
            .unwrap();
        }
    }
    let root = temp.path().join("repository");
    let repository =
        std::sync::Arc::new(Repository::local(&root).await.unwrap().with_spill_limits(
            SpillLimits {
                max_memory_objects: 4,
                max_spill_bytes: 64 * 1024 * 1024,
            },
        ));
    let imported = repository
        .import(crate::import::FilesystemImport::new(
            &source,
            RootName::try_from("test/cancelled").unwrap(),
        ))
        .await
        .unwrap();

    let spill_directory = root.join(crate::spill::SPILL_DIRECTORY);
    for check_fsck in [false, true] {
        let pause = crate::spill::SpillOpenPauseHandle::install(&spill_directory);
        let verification = tokio::spawn({
            let repository = repository.clone();
            let imported = imported.clone();
            async move {
                if check_fsck {
                    repository.fsck().await.map(|_| ())
                } else {
                    repository.verify_closure(&imported).await.map(|_| ())
                }
            }
        });
        let opened = pause.wait_until_open();
        assert!(
            opened.starts_with(&spill_directory) && opened.exists(),
            "the pause must happen after a spill database exists"
        );

        verification.abort();
        pause.resume();
        assert!(verification.await.unwrap_err().is_cancelled());
        assert!(
            std::fs::read_dir(root.join(crate::spill::SPILL_DIRECTORY))
                .unwrap()
                .next()
                .is_none(),
            "cancelling after spill open leaked temporary state"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    #[test]
    fn generic_collection_matches_an_independent_reachability_model(
        mut operations in prop::collection::vec(gc_model_strategy(), 1..32)
    ) {
        operations.push(GcModelOperation::Collect);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let outcome = runtime.block_on(run_gc_model(&operations));
        prop_assert!(outcome.is_ok(), "{}", outcome.unwrap_err());
    }
}

#[tokio::test]
async fn multi_root_failure_preserves_all_names_after_durable_checkpoints() {
    let temporary = tempfile::tempdir().unwrap();
    let mut paths = Vec::new();
    for index in 0..2 {
        let path = temporary.path().join(index.to_string());
        std::fs::create_dir(&path).unwrap();
        for file in 0..600 {
            std::fs::write(path.join(file.to_string()), format!("{index}/{file}")).unwrap();
        }
        paths.push((
            path,
            Some(RootName::try_from(format!("root/{index}").as_str()).unwrap()),
            None,
        ));
    }
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        FormatRegistry::builtin(),
        FormatLimits {
            max_batch_objects: 256,
            max_traversal_objects: 1050,
            ..FormatLimits::default()
        },
    );
    let old = repository
        .import(crate::import::BlobImport::new(
            &b"old"[..],
            paths[0].1.clone().unwrap(),
        ))
        .await
        .unwrap();
    let session = repository.mutation_session().await.unwrap();
    assert!(
        session
            .import_paths_inner(
                paths.clone(),
                false,
                crate::filesystem::DEFAULT_FILE_CONCURRENCY,
                true
            )
            .await
            .is_err()
    );
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert_eq!(
        snapshot.root(paths[0].1.as_ref().unwrap()).await.unwrap(),
        Some(old)
    );
    assert_eq!(
        snapshot.root(paths[1].1.as_ref().unwrap()).await.unwrap(),
        None
    );
    assert!(
        snapshot
            .objects()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len()
            >= 1024
    );
    drop(snapshot);
    drop(session);
    let report = repository.fsck().await.unwrap();
    assert!(!report.issues.is_empty());
    assert!(
        report
            .issues
            .iter()
            .all(|issue| issue.disposition == FsckDisposition::Collectible),
        "{report:?}"
    );
    repository.collect().await.unwrap();
    assert!(repository.fsck().await.unwrap().is_clean());
}

#[tokio::test]
async fn multi_root_pages_keep_equal_relative_names_separate_and_reuse_cache() {
    let temporary = tempfile::tempdir().unwrap();
    let mut paths = Vec::new();
    for index in 0..3 {
        let path = temporary.path().join(index.to_string());
        std::fs::create_dir(&path).unwrap();
        for file in 0..520 {
            std::fs::write(path.join(file.to_string()), format!("{index}/{file}")).unwrap();
        }
        paths.push((
            path,
            Some(RootName::try_from(format!("root/{index}").as_str()).unwrap()),
            None,
        ));
    }
    let repository = Repository::local(temporary.path().join("repository"))
        .await
        .unwrap();
    let session = repository.mutation_session().await.unwrap();
    let (keys, stats) = session
        .import_paths_inner(
            paths.clone(),
            true,
            crate::filesystem::DEFAULT_FILE_CONCURRENCY,
            true,
        )
        .await
        .unwrap();
    assert_eq!(stats.pages, 2);
    assert_eq!(stats.publications, 1);
    let (repeated, _) = session
        .import_paths_inner(
            paths.clone(),
            true,
            crate::filesystem::DEFAULT_FILE_CONCURRENCY,
            true,
        )
        .await
        .unwrap();
    assert_eq!(keys, repeated);
    for ((path, name, _), key) in paths.into_iter().zip(keys) {
        assert_eq!(
            session
                .import_path_inner(
                    path,
                    name,
                    false,
                    None,
                    crate::filesystem::DEFAULT_FILE_CONCURRENCY
                )
                .await
                .unwrap(),
            key
        );
    }
    drop(session);
    assert!(repository.fsck().await.unwrap().is_clean());
}

#[tokio::test]
async fn batched_existing_protection_survives_collection_and_releases_after_drop() {
    let repository = repository();
    let seed = repository.mutation_session().await.unwrap();
    let first = seed.stage_blob(b"first protected payload").await.unwrap();
    let second = seed.stage_blob(b"second protected payload").await.unwrap();
    let records = vec![first.record().clone(), second.record().clone()];
    seed.publish_unrooted(vec![first, second]).await.unwrap();
    drop(seed);
    crate::flush_repository_leases().await.unwrap();

    let mutation = repository.mutation_session().await.unwrap();
    mutation.protect_existing_records(&records).await.unwrap();
    // Force collection between durable batch protection and verification.
    assert_eq!(
        repository
            .try_collect()
            .await
            .unwrap()
            .removed
            .logical_objects,
        0
    );
    for record in &records {
        let staged = mutation
            .stage_existing(record.key().clone(), record.payload())
            .await
            .unwrap();
        assert_eq!(staged.record(), record);
    }
    drop(mutation);
    crate::flush_repository_leases().await.unwrap();
    assert_eq!(
        repository
            .try_collect()
            .await
            .unwrap()
            .removed
            .logical_objects,
        2
    );
    for record in records {
        assert!(!repository.payloads().has(&record.payload()).await.unwrap());
    }
}

#[tokio::test]
async fn batched_existing_protection_obeys_limits_and_does_not_verify_claims() {
    let mut repository = repository();
    repository.limits.max_batch_objects = 1;
    let mutation = repository.mutation_session().await.unwrap();
    let staged = mutation.stage_blob(b"actual bytes").await.unwrap();
    let record = staged.record().clone();
    assert!(matches!(
        mutation
            .protect_existing_records(&[record.clone(), record.clone()])
            .await,
        Err(RepositoryError::LimitExceeded(_))
    ));
    let wrong = ObjectKey::blob(BlobId::new(Digest::hash(b"different bytes")));
    let claimed = ObjectRecord::new(
        wrong.clone(),
        record.payload(),
        record.payload_size(),
        vec![],
    )
    .unwrap();
    mutation.protect_existing_records(&[claimed]).await.unwrap();
    assert!(
        mutation
            .stage_existing(wrong, record.payload())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn batched_existing_protection_invalidates_a_stale_collection_plan() {
    let repository = repository();
    let seed = repository.mutation_session().await.unwrap();
    let staged = seed.stage_blob(b"protected after marking").await.unwrap();
    let record = staged.record().clone();
    seed.publish_unrooted(vec![staged]).await.unwrap();
    drop(seed);
    crate::flush_repository_leases().await.unwrap();
    let plan = repository
        .collection_plan(
            repository.coordination.clone().lock_owned().await,
            None,
            true,
        )
        .await
        .unwrap();
    assert_eq!(plan.preview.logical_objects, 1);
    let mutation = repository.mutation_session().await.unwrap();
    mutation
        .protect_existing_records(std::slice::from_ref(&record))
        .await
        .unwrap();
    assert!(matches!(
        repository.execute_collection(plan, false).await,
        Err(RepositoryError::Busy(_))
    ));
    assert!(repository.payloads().has(&record.payload()).await.unwrap());
    assert_eq!(
        mutation
            .stage_existing(record.key().clone(), record.payload())
            .await
            .unwrap()
            .record(),
        &record
    );
}

#[tokio::test]
async fn metadata_reclaim_defers_while_a_catalog_publication_is_prepared() {
    let temporary = tempfile::tempdir().unwrap();
    let repository = Repository::local(temporary.path()).await.unwrap();
    let first = repository.mutation_session().await.unwrap();
    let initial = first.stage_blob(b"initial").await.unwrap();
    let initial_key = initial.record().key().clone();
    first
        .publish_rooted(
            vec![initial],
            RootName::try_from("initial").unwrap(),
            initial_key,
        )
        .await
        .unwrap();
    drop(first);

    let writer = repository.mutation_session().await.unwrap();
    let plan = repository
        .collection_plan(
            repository.coordination.clone().lock_owned().await,
            None,
            true,
        )
        .await
        .unwrap();
    let staged = writer.stage_blob(b"new publication").await.unwrap();
    let guard = repository.publication.lock().await;
    let prepared = repository
        .payloads()
        .publication()
        .prepare_state_commit()
        .await
        .unwrap();
    assert!(prepared.catalog().is_some());
    let marker = temporary.path().join("blobs/pack-index-reclaim-needed");
    std::fs::write(&marker, b"catalog garbage may be present\n").unwrap();

    let result = repository.reclaim_payload_metadata(&plan, true).await;
    let deferred = marker.exists();
    prepared.abort().unwrap();
    drop(guard);
    drop(staged);
    drop(writer);
    assert!(result.is_ok(), "metadata reclaim failed: {result:?}");
    assert!(deferred, "busy reclaim must leave cleanup pending");
}
