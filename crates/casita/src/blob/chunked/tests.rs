use super::*;
use crate::metadata::MetadataStore as _;
use crate::test_util::native::{read_blob, small_chunked_store, write_blob};
use crate::{
    BlobGc, BlobRepairError, MemoryMetadataStore, RepairingBlobStore, repository::Repository,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Notify;

#[test]
fn a_self_addressed_object_is_named_by_the_hash_of_its_bytes() {
    let digest = Digest::from(blake3::hash(b"catalog object"));
    let path = sharded_path(&Path::from("base"), "pack-indexes", &digest);
    assert_eq!(validate_content_address(&path, &digest), Ok(()));
    let other = Digest::from(blake3::hash(b"other bytes"));
    assert!(validate_content_address(&path, &other).is_err());
    let unnamed = Path::from("base/pack-index-current");
    assert!(validate_content_address(&unnamed, &digest).is_err());
}

#[tokio::test]
async fn scoped_verified_reader_rejects_corrupt_proof_before_returning_bytes() {
    let (store, _directory) = small_chunked_store();
    let repository = Repository::new(store.clone(), MemoryMetadataStore::new().unwrap());
    let mutation = repository.mutation_session().await.unwrap();
    let object = mutation.stage_blob(&vec![19; 65536]).await.unwrap();
    let key = object.record().key().clone();
    let digest = object.record().payload();
    mutation.publish_unrooted(vec![object]).await.unwrap();
    drop(mutation);
    let (_, mut reader) = repository
        .open_object_verified(&key)
        .await
        .unwrap()
        .unwrap();
    let mut proof = store.get_outboard(&digest).await.unwrap().unwrap().to_vec();
    proof[0] ^= 1;
    store.put_outboard(&digest, proof.into()).await.unwrap();
    let mut output = Vec::new();
    assert!(reader.read_to_end(&mut output).await.is_err());
    assert!(output.is_empty());
    drop(reader);
    crate::flush_repository_leases().await.unwrap();
}

#[tokio::test]
async fn ingested_proofs_and_partial_overwrites_roundtrip() {
    let (store, _directory) = small_chunked_store();
    for size in [0usize, 1, 1024, 16383, 16384, 16385, 32769, 262145] {
        let original = (0..size).map(|i| (i * 29) as u8).collect::<Vec<_>>();
        let old = store.put_slice(&original).await.unwrap();
        let mut read = store
            .open_verified(&old, size as u64)
            .await
            .unwrap()
            .unwrap();
        let mut output = Vec::new();
        read.read_to_end(&mut output).await.unwrap();
        assert_eq!(output, original);
        for offset in [0, size / 2, size.saturating_sub(1)] {
            let count = (size - offset).min(333);
            let replacement = vec![7; count];
            let (new, proof) = store
                .overwrite(&old, size as u64, offset as u64, &replacement)
                .await
                .unwrap();
            assert_eq!(
                crate::verified::patch::verify(
                    &proof,
                    old,
                    size as u64,
                    offset as u64,
                    &replacement
                )
                .await
                .unwrap()
                .digest,
                new
            );
            let mut expected = original.clone();
            expected[offset..offset + count].copy_from_slice(&replacement);
            assert_eq!(new, BlobId::new(blake3::hash(&expected).into()));
            assert_eq!(store.read_to_vec(&new).await.unwrap().unwrap(), expected);
            let mut read = store
                .open_verified(&new, size as u64)
                .await
                .unwrap()
                .unwrap();
            let mut output = Vec::new();
            read.read_to_end(&mut output).await.unwrap();
            assert_eq!(output, expected);
            assert_eq!(store.read_to_vec(&old).await.unwrap().unwrap(), original);
        }
        assert!(
            store
                .overwrite(&old, size as u64, size as u64, &[1])
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn first_verified_byte_and_overwrite_read_bounded_payload_ranges() {
    let objects = Arc::new(ChaosObjectStore::new(ChaosFault::ReadChunk));
    let store = ChunkedBlobStore::new(objects.clone(), Path::default(), 64 * 1024);
    let mut state = 71u64;
    let bytes: Vec<u8> = (0..(20 * 1024 * 1024))
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect();
    let old = store.put_slice(&bytes).await.unwrap();
    objects.chunk_read_bytes.store(0, Ordering::SeqCst);
    objects.metadata_read_bytes.store(0, Ordering::SeqCst);
    let mut reader = store
        .open_verified(&old, bytes.len() as u64)
        .await
        .unwrap()
        .unwrap();
    let mut prefix = [0; 1024];
    reader.read_exact(&mut prefix).await.unwrap();
    assert_eq!(&prefix, &bytes[..1024]);
    assert!(objects.chunk_read_bytes.load(Ordering::SeqCst) < 1024 * 1024);
    assert!(objects.metadata_read_bytes.load(Ordering::SeqCst) < 32 * 1024);
    drop(reader);
    objects.chunk_read_bytes.store(0, Ordering::SeqCst);
    objects.metadata_read_bytes.store(0, Ordering::SeqCst);
    objects.metadata_write_bytes.store(0, Ordering::SeqCst);
    let offset = bytes.len() / 2 - 100;
    let (updated, _) = store
        .overwrite(&old, bytes.len() as u64, offset as u64, &[42; 300])
        .await
        .unwrap();
    assert!(objects.chunk_read_bytes.load(Ordering::SeqCst) < 1024 * 1024);
    assert!(objects.metadata_read_bytes.load(Ordering::SeqCst) < 64 * 1024);
    assert!(objects.metadata_write_bytes.load(Ordering::SeqCst) < 64 * 1024);
    let mut expected = bytes;
    expected[offset..offset + 300].fill(42);
    assert_eq!(updated, BlobId::new(blake3::hash(&expected).into()));
    assert_eq!(
        store.read_to_vec(&updated).await.unwrap().unwrap(),
        expected
    );
}

#[tokio::test]
async fn verified_reads_reject_substitution_and_repair_missing_outboards() {
    let (store, _directory) = small_chunked_store();
    let original = vec![17; 100_000];
    let old = store.put_slice(&original).await.unwrap();
    store
        .object_store
        .delete(&store.outboard_path(&old))
        .await
        .unwrap();
    let mut reader = store
        .open_verified(&old, original.len() as u64)
        .await
        .unwrap()
        .unwrap();
    assert!(reader.read(&mut [0; 1]).await.is_err());
    drop(reader);
    store.build_outboard(&old).await.unwrap();
    let mut reader = store
        .open_verified(&old, original.len() as u64)
        .await
        .unwrap()
        .unwrap();
    let mut output = Vec::new();
    reader.read_to_end(&mut output).await.unwrap();
    assert_eq!(output, original);
    drop(reader);
    let other = store.put_slice(&vec![23; original.len()]).await.unwrap();
    let substitution = encode_manifest(&store.chunks(&other).await.unwrap().unwrap());
    put_object(
        &store.object_store,
        &store.blob_path(&old),
        substitution,
        false,
    )
    .await
    .unwrap();
    let mut reader = store
        .open_verified(&old, original.len() as u64)
        .await
        .unwrap()
        .unwrap();
    let mut output = Vec::new();
    assert!(reader.read_to_end(&mut output).await.is_err());
    assert!(output.is_empty());
}

/// A deliberately armed fault at one object-store operation. This makes each
/// repair failure mode reproducible instead of relying on random I/O errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChaosFault {
    ReadChunk,
    WriteManifest,
    WriteOutboard,
    PauseManifest,
    PauseChunk,
    PauseChunkUploads,
    PauseDelete,
    FailDelete,
}

#[tokio::test]
async fn loose_gc_preserves_physical_pins_and_counts_only_admitted_deletions() {
    use crate::metadata::{DataPin, MetadataStore, PinResource, PinScope};
    let (store, _directory) = small_chunked_store();
    let held = store.put_slice(&[1; 4096]).await.unwrap();
    let garbage = store.put_slice(&[2; 4096]).await.unwrap();
    let held_chunk = store.chunks(&held).await.unwrap().unwrap()[0].digest;
    let garbage_chunk = store.chunks(&garbage).await.unwrap().unwrap()[0].digest;
    let state = MemoryMetadataStore::new().unwrap();
    let ledger = state.pin_store().await.unwrap();
    let pin = ledger
        .register(DataPin {
            scope: PinScope::Staging,
            catalog: None,
            resources: [store.blob_path(&held), store.chunk_path(&held_chunk)]
                .into_iter()
                .map(|path| PinResource::StorageObject(path.to_string()))
                .collect(),
        })
        .await
        .unwrap()
        .unwrap();
    let repository = Repository::new(store.clone(), state);
    let outcome = repository.collect().await.unwrap();
    assert_eq!(outcome.removed.payload_blobs, 1);
    assert_eq!(outcome.removed.chunks, 1);
    assert_eq!(read_blob(&store, &held).await, Some(vec![1; 4096]));
    assert!(!store.has(&garbage).await.unwrap());
    assert!(store.chunk_index.contains(&held_chunk));
    assert!(!store.chunk_index.contains(&garbage_chunk));
    ledger.release(&pin).await.unwrap();
    let outcome = repository.collect().await.unwrap();
    assert_eq!(outcome.removed.payload_blobs, 1);
    assert_eq!(outcome.removed.chunks, 1);
    assert!(!store.has(&held).await.unwrap());
    crate::flush_repository_leases().await.unwrap();
}

#[tokio::test]
async fn collector_pack_writes_do_not_extend_a_concurrent_staging_pin() {
    use crate::metadata::{DataPin, DataPinLease, PinScope, PinStore};
    let store = ChunkedBlobStore::packed_with_options(
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
    let ledger = Arc::new(crate::metadata::MemoryPinStore::default());
    let pin = DataPinLease::acquire(
        ledger.clone(),
        DataPin {
            scope: PinScope::Staging,
            catalog: None,
            resources: Default::default(),
        },
    )
    .await
    .unwrap();
    let batch = store.begin_pinned_batch(pin.clone()).unwrap();
    let collector = ledger
        .begin_collection(ledger.inventory().await.unwrap().revision, None)
        .await
        .unwrap()
        .unwrap();
    let prune = ledger
        .begin_prune(ledger.inventory().await.unwrap().revision)
        .await
        .unwrap()
        .unwrap();
    let payload = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        store.write_scope().run(async {
            let payload = store.put_slice(&[7; 4096]).await.unwrap();
            store.flush().await.unwrap();
            payload
        }),
    )
    .await
    .unwrap();
    assert!(store.has(&payload).await.unwrap());
    assert!(
        ledger.inventory().await.unwrap().pins[pin.token()]
            .resources
            .is_empty()
    );
    ledger.finish_prune(&prune).await.unwrap();
    // Ending the scope restores ordinary writer protection for this backend.
    store.put_slice(&[8; 4096]).await.unwrap();
    store.flush().await.unwrap();
    assert!(
        !ledger.inventory().await.unwrap().pins[pin.token()]
            .resources
            .is_empty()
    );
    drop((batch, pin));
    crate::flush_repository_leases().await.unwrap();
    ledger.finish_collection(&collector).await.unwrap();
}

#[tokio::test]
async fn loose_gc_cancellation_keeps_path_claim_until_delete_settles() {
    use crate::metadata::{DataPin, PinResource, PinScope, PinStore};
    let (store, pause) = chaos_chunked_store(ChaosFault::PauseDelete);
    let blob = store.put_slice(&[3; 4096]).await.unwrap();
    let chunk = store.chunks(&blob).await.unwrap().unwrap()[0].digest;
    let resource = PinResource::StorageObject(store.chunk_path(&chunk).to_string());
    let ledger = Arc::new(crate::metadata::MemoryPinStore::default());
    let collector = ledger.begin_collection(0, None).await.unwrap().unwrap();
    pause.arm();
    let worker = tokio::spawn({
        let store = store.clone();
        let ledger = ledger.clone();
        async move {
            store
                .delete_chunks_pinned(&[chunk], ledger, Default::default())
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), pause.wait_until_paused())
        .await
        .unwrap();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    assert!(!store.chunk_index.contains(&chunk));
    assert!(
        ledger
            .inventory()
            .await
            .unwrap()
            .deletions
            .values()
            .any(|paths| paths.contains(&resource))
    );
    assert!(
        ledger
            .register(DataPin {
                scope: PinScope::Staging,
                catalog: None,
                resources: std::collections::BTreeSet::from([resource]),
            })
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            crate::flush_repository_leases()
        )
        .await
        .is_err()
    );
    pause.resume();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        crate::flush_repository_leases(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(ledger.inventory().await.unwrap().deletions.is_empty());
    assert!(pause.inner.head(&store.chunk_path(&chunk)).await.is_err());
    ledger.finish_collection(&collector).await.unwrap();
}

#[tokio::test]
async fn loose_gc_failed_delete_requires_exact_claim_recovery() {
    use crate::metadata::PinStore;
    let (store, fault) = chaos_chunked_store(ChaosFault::FailDelete);
    let first = store.put_slice(&[4; 4096]).await.unwrap();
    let second = store.put_slice(&[5; 4096]).await.unwrap();
    let first = store.chunks(&first).await.unwrap().unwrap()[0].digest;
    let second = store.chunks(&second).await.unwrap().unwrap()[0].digest;
    let ledger = Arc::new(crate::metadata::MemoryPinStore::default());
    let collector = ledger.begin_collection(0, None).await.unwrap().unwrap();
    fault.arm();
    assert!(
        store
            .delete_chunks_pinned(&[first], ledger.clone(), Default::default())
            .await
            .is_err()
    );
    let failed = ledger.inventory().await.unwrap().deletions;
    assert_eq!(failed.len(), 1);
    assert!(ledger.finish_collection(&collector).await.is_err());
    fault.disarm();
    assert!(
        store
            .delete_chunks_pinned(&[first], ledger.clone(), Default::default())
            .await
            .is_err()
    );
    assert_eq!(
        store
            .delete_chunks_pinned(&[second], ledger.clone(), Default::default())
            .await
            .unwrap(),
        1
    );
    assert_eq!(ledger.inventory().await.unwrap().deletions, failed);
    let owned = failed.keys().cloned().collect();
    // The injected failure returned before issuing I/O. Its exact owner can
    // now retry; the helper must not release adopted recovery claims itself.
    assert_eq!(
        store
            .delete_chunks_pinned(&[first], ledger.clone(), owned)
            .await
            .unwrap(),
        1
    );
    assert_eq!(ledger.inventory().await.unwrap().deletions, failed);
    for token in failed.keys() {
        ledger.finish_deletions(token).await.unwrap();
    }
    ledger.finish_collection(&collector).await.unwrap();
    crate::flush_repository_leases().await.unwrap();
}

#[derive(Debug)]
struct ChaosObjectStore {
    inner: Arc<object_store::memory::InMemory>,
    fault: ChaosFault,
    armed: AtomicBool,
    reached: Arc<Notify>,
    resume: Arc<Notify>,
    paused_once: AtomicBool,
    manifest_puts: AtomicUsize,
    paused_chunk_puts: AtomicUsize,
    chunk_read_bytes: AtomicUsize,
    metadata_read_bytes: AtomicUsize,
    metadata_write_bytes: AtomicUsize,
    bao_multipart_completed: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct StrictBaoMultipart {
    inner: Box<dyn MultipartUpload>,
    parts: Vec<usize>,
    completed: Arc<AtomicUsize>,
}

#[async_trait]
impl MultipartUpload for StrictBaoMultipart {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        self.parts.push(data.content_length());
        self.inner.put_part(data)
    }
    async fn complete(&mut self) -> object_store::Result<PutResult> {
        assert!(
            self.parts
                .iter()
                .take(self.parts.len().saturating_sub(1))
                .all(|size| *size >= 5 * 1024 * 1024),
            "S3 rejects undersized non-final parts"
        );
        let result = self.inner.complete().await?;
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(result)
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

#[tokio::test]
async fn spilled_outboard_upload_uses_bounded_shared_pages() {
    use std::io::{Seek, Write};
    let objects = Arc::new(ChaosObjectStore::new(ChaosFault::ReadChunk));
    let store: Arc<dyn ObjectStore> = objects.clone();
    for length in [
        4095,
        4096,
        4097,
        4096 * 64 - 1,
        4096 * 64,
        4096 * 64 + 1,
        4096 * 4096 - 1,
        4096 * 4096 + 1,
    ] {
        let bytes = vec![19; length];
        let digest = BlobId::new(blake3::hash(&bytes).into());
        let mut file = tempfile::spooled_tempfile(64 * 1024);
        file.write_all(&bytes).unwrap();
        file.rewind().unwrap();
        super::writer::store_outboard(
            &store,
            &Path::default(),
            digest,
            crate::verified::ingest::OutboardData {
                file,
                len: length as u64,
            },
            true,
            None,
        )
        .await
        .unwrap();
        let reader = ChunkedBlobStore::new(store.clone(), Path::default(), 64 * 1024);
        assert_eq!(reader.get_outboard(&digest).await.unwrap().unwrap(), bytes);
        let pages: Vec<_> = store
            .list(Some(&Path::from("pages/")))
            .try_collect()
            .await
            .unwrap();
        assert!(pages.iter().all(|page| page.size <= 4120));
    }
    assert_eq!(objects.bao_multipart_completed.load(Ordering::SeqCst), 0);
}

impl ChaosObjectStore {
    fn new(fault: ChaosFault) -> Self {
        Self {
            inner: Arc::new(object_store::memory::InMemory::new()),
            fault,
            armed: AtomicBool::new(false),
            reached: Arc::new(Notify::new()),
            resume: Arc::new(Notify::new()),
            paused_once: AtomicBool::new(false),
            manifest_puts: AtomicUsize::new(0),
            chunk_read_bytes: AtomicUsize::new(0),
            metadata_read_bytes: AtomicUsize::new(0),
            metadata_write_bytes: AtomicUsize::new(0),
            bao_multipart_completed: Arc::new(AtomicUsize::new(0)),
            paused_chunk_puts: AtomicUsize::new(0),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn disarm(&self) {
        self.armed.store(false, Ordering::SeqCst);
    }

    async fn wait_until_paused(&self) {
        self.reached.notified().await;
    }

    fn resume(&self) {
        self.resume.notify_one();
    }

    fn manifest_puts(&self) -> usize {
        self.manifest_puts.load(Ordering::SeqCst)
    }

    fn should_fail(&self, operation: &str, location: &object_store::path::Path) -> bool {
        if !self.armed.load(Ordering::SeqCst) {
            return false;
        }
        match self.fault {
            ChaosFault::ReadChunk => operation == "get" && location.as_ref().starts_with("chunks/"),
            ChaosFault::WriteManifest => {
                operation == "put" && location.as_ref().starts_with("blobs/")
            }
            ChaosFault::WriteOutboard => {
                operation == "put" && location.as_ref().starts_with("bao/")
            }
            ChaosFault::PauseManifest
            | ChaosFault::PauseChunk
            | ChaosFault::PauseChunkUploads
            | ChaosFault::PauseDelete
            | ChaosFault::FailDelete => false,
        }
    }

    async fn pause_before_manifest_put(&self, location: &object_store::path::Path) {
        if self.armed.load(Ordering::SeqCst)
            && self.fault == ChaosFault::PauseManifest
            && location.as_ref().starts_with("blobs/")
        {
            self.reached.notify_one();
            self.resume.notified().await;
        }
    }

    async fn pause_before_chunk_get(&self, location: &object_store::path::Path) {
        if self.armed.load(Ordering::SeqCst)
            && self.fault == ChaosFault::PauseChunk
            && location.as_ref().starts_with("chunks/")
            && !self.paused_once.swap(true, Ordering::SeqCst)
        {
            self.reached.notify_one();
            self.resume.notified().await;
        }
    }

    fn injected_error(&self, operation: &str) -> object_store::Error {
        object_store::Error::Generic {
            store: "fsck-repair-chaos",
            source: Box::new(std::io::Error::other(format!(
                "injected {operation} failure at {:?}",
                self.fault
            ))),
        }
    }
}

impl fmt::Display for ChaosObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ChaosObjectStore")
    }
}

#[async_trait]
impl ObjectStore for ChaosObjectStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if !location.as_ref().starts_with("chunks/") {
            self.metadata_write_bytes
                .fetch_add(payload.content_length(), Ordering::SeqCst);
        }
        if self.armed.load(Ordering::SeqCst)
            && self.fault == ChaosFault::PauseChunkUploads
            && location.as_ref().starts_with("chunks/")
        {
            self.paused_chunk_puts.fetch_add(1, Ordering::SeqCst);
            self.reached.notify_one();
            self.resume.notified().await;
        }
        if location.as_ref().starts_with("blobs/") {
            self.manifest_puts.fetch_add(1, Ordering::SeqCst);
        }
        self.pause_before_manifest_put(location).await;
        if self.should_fail("put", location) {
            return Err(self.injected_error("put"));
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let inner = self.inner.put_multipart_opts(location, options).await?;
        if location.as_ref().starts_with("bao/") {
            Ok(Box::new(StrictBaoMultipart {
                inner,
                parts: Vec::new(),
                completed: self.bao_multipart_completed.clone(),
            }))
        } else {
            Ok(inner)
        }
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.pause_before_chunk_get(location).await;
        if self.should_fail("get", location) {
            return Err(self.injected_error("get"));
        }
        let head = options.head;
        let result = self.inner.get_opts(location, options).await?;
        if !head {
            let counter = if location.as_ref().starts_with("chunks/") {
                &self.chunk_read_bytes
            } else {
                &self.metadata_read_bytes
            };
            counter.fetch_add(
                (result.range.end - result.range.start) as usize,
                Ordering::SeqCst,
            );
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<object_store::path::Path>>,
    ) -> BoxStream<'static, object_store::Result<object_store::path::Path>> {
        if self.armed.load(Ordering::SeqCst) && self.fault == ChaosFault::FailDelete {
            let error = self.injected_error("delete");
            return futures::stream::once(async move { Err(error) }).boxed();
        }
        if self.armed.load(Ordering::SeqCst)
            && self.fault == ChaosFault::PauseDelete
            && !self.paused_once.swap(true, Ordering::SeqCst)
        {
            let reached = self.reached.clone();
            let resume = self.resume.clone();
            let inner = self.inner.clone();
            return futures::stream::once(async move {
                reached.notify_one();
                resume.notified().await;
                inner.delete_stream(locations)
            })
            .flatten()
            .boxed();
        }
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn repairing_tier_recovers_manifests_chunks_and_elided_blobs() {
    #[derive(Clone, Copy)]
    enum Damage {
        Manifest,
        Chunk,
    }

    for (data, damage) in [
        (
            (0..60_000).map(|index| (index % 251) as u8).collect(),
            Damage::Manifest,
        ),
        (
            (0..60_000).map(|index| (index % 239) as u8).collect(),
            Damage::Chunk,
        ),
        (b"manifest-elided payload".to_vec(), Damage::Chunk),
    ] {
        let (near, _near_dir) = small_chunked_store();
        let (far, _far_dir) = small_chunked_store();
        let digest = write_blob(&near, &data).await;
        assert_eq!(write_blob(&far, &data).await, digest);
        let chunks = near.chunks(&digest).await.unwrap().unwrap();

        match damage {
            Damage::Manifest => {
                assert!(chunks.len() >= 2);
                near.object_store
                    .put(
                        &near.blob_path(&digest),
                        Bytes::from_static(b"not a manifest").into(),
                    )
                    .await
                    .unwrap();
            }
            Damage::Chunk => {
                near.object_store
                    .put(
                        &near.chunk_path(&chunks[0].digest),
                        Bytes::from_static(b"not zstd").into(),
                    )
                    .await
                    .unwrap();
            }
        }

        let repairing = RepairingBlobStore::new(near.clone(), far);
        assert_eq!(
            repairing.read_to_vec(&digest).await.unwrap(),
            Some(data.clone())
        );
        assert_eq!(read_blob(&near, &digest).await, Some(data));
    }
}

#[tokio::test]
async fn repairing_tier_rejects_an_unverified_source_with_both_diagnostics() {
    let (near, _near_dir) = small_chunked_store();
    let (far, _far_dir) = small_chunked_store();
    let data: Vec<u8> = (0..60_000).map(|index| (index % 251) as u8).collect();
    let digest = write_blob(&near, &data).await;
    assert_eq!(write_blob(&far, &data).await, digest);
    let chunk = near.chunks(&digest).await.unwrap().unwrap()[0].digest;
    for store in [&near, &far] {
        store
            .object_store
            .put(
                &store.chunk_path(&chunk),
                Bytes::from_static(b"not zstd").into(),
            )
            .await
            .unwrap();
    }

    let error = match RepairingBlobStore::new(near.clone(), far)
        .open_read(&digest)
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("corrupt source unexpectedly repaired the near tier"),
    };
    let Error::Backend(source) = error else {
        panic!("repair failure lost its typed diagnostic: {error}");
    };
    let repair = source.downcast_ref::<BlobRepairError>().unwrap();
    assert!(repair.near_error().contains("invalid"));
    assert!(repair.repair_error().contains("far tier did not verify"));
    assert!(is_unreadable(&near, &digest).await);
}

#[tokio::test]
async fn repairing_tier_does_not_mistake_backend_io_for_corruption() {
    let (near, fault) = chaos_chunked_store(ChaosFault::ReadChunk);
    let (far, _far_dir) = small_chunked_store();
    let data: Vec<u8> = (0..60_000).map(|index| (index % 251) as u8).collect();
    let digest = write_blob(&near, &data).await;
    assert_eq!(write_blob(&far, &data).await, digest);
    fault.arm();

    let error = match RepairingBlobStore::new(near.clone(), far)
        .open_read(&digest)
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("backend I/O failure unexpectedly returned a reader"),
    };
    assert!(error.to_string().contains("injected get failure"));
    assert!(!error.to_string().contains("could not repair"));

    fault.disarm();
    assert_eq!(read_blob(&near, &digest).await, Some(data));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repairing_tier_is_single_flight_and_excludes_gc() {
    let (near, publication) = chaos_chunked_store(ChaosFault::WriteManifest);
    let (far, source_pause) = chaos_chunked_store(ChaosFault::PauseChunk);
    let data: Vec<u8> = (0..60_000).map(|index| (index % 251) as u8).collect();
    let digest = write_blob(&near, &data).await;
    assert_eq!(write_blob(&far, &data).await, digest);
    let manifest_puts = publication.manifest_puts();
    let chunk = near.chunks(&digest).await.unwrap().unwrap()[0].digest;
    near.object_store
        .put(
            &near.chunk_path(&chunk),
            Bytes::from_static(b"not zstd").into(),
        )
        .await
        .unwrap();
    let repairing = RepairingBlobStore::new(near.clone(), far);
    source_pause.arm();

    let first_store = repairing.clone();
    let first = tokio::spawn(async move { first_store.read_to_vec(&digest).await });
    source_pause.wait_until_paused().await;
    let second_store = repairing.clone();
    let second = tokio::spawn(async move { second_store.read_to_vec(&digest).await });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while repairing.repair_waiters(&digest).await == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second reader did not join the repair flight");

    let deleting_store = repairing.clone();
    let mut deleting = tokio::spawn(async move { deleting_store.delete_chunk(&chunk).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut deleting)
            .await
            .is_err(),
        "near-tier GC raced an active repair"
    );

    source_pause.resume();
    assert_eq!(first.await.unwrap().unwrap(), Some(data.clone()));
    assert_eq!(second.await.unwrap().unwrap(), Some(data));
    assert_eq!(
        publication.manifest_puts(),
        manifest_puts + 1,
        "concurrent readers published more than one repair"
    );
    deleting.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repairing_tier_cancellation_releases_the_flight_for_retry() {
    let (near, _near_dir) = small_chunked_store();
    let (far, pause) = chaos_chunked_store(ChaosFault::PauseChunk);
    let data = b"cancel and retry repair".to_vec();
    let digest = write_blob(&near, &data).await;
    assert_eq!(write_blob(&far, &data).await, digest);
    let chunk = near.chunks(&digest).await.unwrap().unwrap()[0].digest;
    near.object_store
        .put(
            &near.chunk_path(&chunk),
            Bytes::from_static(b"not zstd").into(),
        )
        .await
        .unwrap();
    let repairing = RepairingBlobStore::new(near, far);
    pause.arm();

    let cancelled_store = repairing.clone();
    let cancelled = tokio::spawn(async move { cancelled_store.read_to_vec(&digest).await });
    pause.wait_until_paused().await;
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    pause.resume();

    for _ in 0..10 {
        match repairing.read_to_vec(&digest).await {
            Ok(bytes) => {
                assert_eq!(bytes, Some(data));
                return;
            }
            Err(error) if error.to_string().contains("repair was cancelled") => {
                tokio::task::yield_now().await;
            }
            Err(error) => panic!("retry failed unexpectedly: {error}"),
        }
    }
    panic!("cancelled repair flight was not released for retry");
}

#[tokio::test]
async fn repairing_tier_rebuilds_bao_state_from_verified_local_bytes() {
    let (near, _near_dir) = small_chunked_store();
    let (far, _far_dir) = small_chunked_store();
    let data: Vec<u8> = (0..60_000).map(|index| (index % 251) as u8).collect();
    let digest = write_blob(&near, &data).await;
    assert_eq!(write_blob(&far, &data).await, digest);
    near.build_outboard(&digest).await.unwrap();
    near.put_outboard(&digest, Bytes::from_static(b"not a bao outboard"))
        .await
        .unwrap();

    let repairing = RepairingBlobStore::new(near.clone(), far);
    assert_eq!(
        repairing
            .verified_read(&digest, 12_345, 4096)
            .await
            .unwrap(),
        Bytes::copy_from_slice(&data[12_345..12_345 + 4096])
    );
    assert_eq!(
        near.verified_read(&digest, 12_345, 4096).await.unwrap(),
        Bytes::copy_from_slice(&data[12_345..12_345 + 4096])
    );

    let chunk = near.chunks(&digest).await.unwrap().unwrap()[0].digest;
    near.object_store
        .put(
            &near.chunk_path(&chunk),
            Bytes::from_static(b"not zstd").into(),
        )
        .await
        .unwrap();
    assert_eq!(
        repairing
            .verified_read(&digest, 20_000, 2048)
            .await
            .unwrap(),
        Bytes::copy_from_slice(&data[20_000..20_000 + 2048])
    );
}

fn chaos_chunked_store(fault: ChaosFault) -> (ChunkedBlobStore, Arc<ChaosObjectStore>) {
    let objects = Arc::new(ChaosObjectStore::new(fault));
    let store = ChunkedBlobStore::new(objects.clone(), Path::default(), 1024);
    (store, objects)
}

async fn publish_repair_payload<SS>(
    target: &Repository<ChunkedBlobStore, SS>,
    replica: &ChunkedBlobStore,
    data: &[u8],
) -> BlobId
where
    SS: crate::MetadataStore,
{
    let mutation = target.mutation_session().await.unwrap();
    let staged = mutation.stage_blob(data).await.unwrap();
    let payload = staged.record().payload();
    mutation.publish_unrooted(vec![staged]).await.unwrap();
    assert_eq!(write_blob(replica, data).await, payload);
    payload
}

async fn is_unreadable(store: &ChunkedBlobStore, payload: &BlobId) -> bool {
    let mut reader = match store.open_read(payload).await {
        Ok(Some(reader)) => reader,
        Ok(None) | Err(_) => return true,
    };
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.is_err()
}

#[tokio::test]
async fn verified_ranges_and_windows_need_no_preparation_at_proof_boundary() {
    let (store, _dir) = small_chunked_store();
    for size in [0, 1, 16383, 16384, 16385, 70000] {
        let bytes: Vec<_> = (0..size).map(|i| (i * 29) as u8).collect();
        let digest = store.put_slice(&bytes).await.unwrap();
        assert_eq!(
            store.verified_read(&digest, 0, size as u64).await.unwrap(),
            bytes
        );
        let mut stream = store.verified_stream(&digest, 4096).await.unwrap();
        let mut actual = Vec::new();
        while let Some(window) = stream.try_next().await.unwrap() {
            actual.extend_from_slice(&window);
        }
        assert_eq!(actual, bytes);
    }
}

#[tokio::test]
async fn verified_read_roundtrip() {
    let (svc, _dir) = small_chunked_store();
    let data: Vec<u8> = (0..20_000u32).map(|i| (i % 256) as u8).collect();
    let digest = write_blob(&svc, &data).await;
    svc.build_outboard(&digest).await.unwrap();

    let got = svc.verified_read(&digest, 5000, 300).await.unwrap();
    assert_eq!(got, &data[5000..5300]);

    // Large blobs still require proof nodes; absence must not weaken verification.
    svc.object_store
        .delete(&svc.outboard_path(&digest))
        .await
        .unwrap();
    assert!(svc.verified_read(&digest, 0, 4).await.is_err());
    assert!(svc.verified_stream(&digest, 4096).await.is_err());
}

#[tokio::test]
async fn verified_stream_roundtrip() {
    let (svc, _dir) = small_chunked_store();
    let data: Vec<u8> = (0..70_000u32).map(|i| (i % 251) as u8).collect();
    let digest = write_blob(&svc, &data).await;
    svc.build_outboard(&digest).await.unwrap();

    let mut stream = svc.verified_stream(&digest, 4096).await.unwrap();
    let mut got = Vec::new();
    while let Some(window) = stream.try_next().await.unwrap() {
        assert!(window.len() <= 4096);
        got.extend_from_slice(&window);
    }
    assert_eq!(got, data);
    assert!(svc.verified_stream(&digest, 0).await.is_err());
}

#[tokio::test]
async fn inline_decode_uses_the_declared_size_as_its_work_cap() {
    let data = vec![7u8; 4096];
    let digest = ChunkId::new(blake3::hash(&data).into());
    let compressed = zstd::bulk::compress(&data, zstd::DEFAULT_COMPRESSION_LEVEL).unwrap();
    assert!(
        decompress_verified_chunk_adaptive(
            compressed.into(),
            digest,
            MAX_CHUNK_SIZE as usize,
            Some(1024),
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn sequential_stream_reports_large_chunk_corruption_at_eof() {
    let dir = tempfile::tempdir().unwrap();
    let svc = ChunkedBlobStore::local(dir.path())
        .unwrap()
        .with_chunk_memory_budget_bytes(1);
    let data = vec![7u8; 100_000];
    let digest = write_blob(&svc, &data).await;
    assert_eq!(svc.read_to_vec(&digest).await.unwrap().unwrap(), data);

    let replacement = vec![9u8; data.len()];
    let compressed = zstd::bulk::compress(&replacement, zstd::DEFAULT_COMPRESSION_LEVEL).unwrap();
    svc.object_store
        .put(
            &svc.chunk_path(&ChunkId::new(digest.digest())),
            compressed.into(),
        )
        .await
        .unwrap();

    let mut stream = svc.open_stream(&digest).await.unwrap().unwrap();
    let mut first = vec![0u8; 4096];
    assert!(stream.read(&mut first).await.unwrap() > 0);
    let mut rest = Vec::new();
    assert!(stream.read_to_end(&mut rest).await.is_err());
}

#[tokio::test]
async fn roundtrip_various_sizes() {
    let (svc, _dir) = small_chunked_store();
    for &n in &[0usize, 1, 100, 1024, 4096, 100_000] {
        let data: Vec<u8> = (0..n).map(|i| (i * 31 + 7) as u8).collect();
        let digest = write_blob(&svc, &data).await;
        assert_eq!(
            digest,
            BlobId::new(blake3::hash(&data).into()),
            "digest, size {n}"
        );
        assert!(svc.has(&digest).await.unwrap(), "has, size {n}");
        assert_eq!(
            read_blob(&svc, &digest).await.as_deref(),
            Some(data.as_slice()),
            "read, size {n}"
        );
    }
}

/// Content shorter than the chunker's minimum takes a path that never builds a
/// chunker, because FastCDC could not have cut it anyway. Both sides of that
/// boundary must store and read back the same bytes under the same identity.
#[tokio::test]
async fn the_chunker_minimum_boundary_stores_identical_content() {
    let (svc, _dir) = small_chunked_store();
    // `small_chunked_store` averages 1024 bytes, and the minimum is half of it.
    let minimum = 512usize;
    for &size in &[minimum - 1, minimum, minimum + 1] {
        let data: Vec<u8> = (0..size).map(|i| (i * 17 + 3) as u8).collect();
        let digest = write_blob(&svc, &data).await;
        assert_eq!(
            digest,
            BlobId::new(blake3::hash(&data).into()),
            "identity, size {size}"
        );
        assert_eq!(
            read_blob(&svc, &digest).await.as_deref(),
            Some(data.as_slice()),
            "content, size {size}"
        );
        let total: u64 = svc
            .chunks(&digest)
            .await
            .unwrap()
            .unwrap()
            .iter()
            .map(|chunk| chunk.size)
            .sum();
        assert_eq!(total, size as u64, "chunk coverage, size {size}");
    }
}

#[tokio::test]
async fn custom_chunk_sizes_match_fastcdc5_and_read_back() {
    // Include odd averages, even averages with odd halves, and the
    // non-power-of-two mask bucket that differed between upstream adapters.
    for (configured, min, avg, max) in [
        (0, 128, 256, 1024),
        (257, 128, 256, 1024),
        (1025, 512, 1024, 2048),
        (1026, 512, 1026, 2052),
        (1500, 750, 1500, 3000),
        (12288, 6144, 12288, 24576),
    ] {
        let store = ChunkedBlobStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            Path::default(),
            configured,
        );
        for size in [min - 1, min, min + 1, max - 1, max, max + 1, max * 4 + 1] {
            let bytes: Vec<u8> = (0..size)
                .map(|index| blake3::hash(&(index as u32).to_le_bytes()).as_bytes()[0])
                .collect();
            let expected: Vec<_> = fastcdc::v2020::FastCDC::new(&bytes, min, avg, max)
                .map(|chunk| ChunkMeta {
                    digest: ChunkId::new(
                        blake3::hash(&bytes[chunk.offset..chunk.offset + chunk.length]).into(),
                    ),
                    size: chunk.length as u64,
                })
                .collect();
            let digest = write_blob(&store, &bytes).await;
            assert_eq!(digest, BlobId::new(blake3::hash(&bytes).into()));
            assert_eq!(store.chunks(&digest).await.unwrap().unwrap(), expected);
            assert_eq!(read_blob(&store, &digest).await.unwrap(), bytes);
        }
    }
}

#[tokio::test]
async fn large_blob_is_chunked() {
    let (svc, _dir) = small_chunked_store();
    let data: Vec<u8> = (0..100_000).map(|i| (i % 251) as u8).collect();
    let digest = write_blob(&svc, &data).await;
    let chunks = svc.chunks(&digest).await.unwrap().unwrap();
    assert!(chunks.len() >= 2, "expected multiple chunks");
    let total: u64 = chunks.iter().map(|c| c.size).sum();
    assert_eq!(total, data.len() as u64);
}

#[tokio::test]
async fn chunk_upload_concurrency_bounds_pending_backend_writes() {
    use std::future::Future;
    use std::task::Poll;
    for limit in [1, 2, 8] {
        let backend = Arc::new(ChaosObjectStore::new(ChaosFault::PauseChunkUploads));
        backend.arm();
        let store = ChunkedBlobStore::new(backend.clone(), Path::default(), 1024)
            .with_chunk_upload_concurrency(std::num::NonZeroUsize::new(limit).unwrap());
        let mut data = vec![0; 100_000];
        blake3::Hasher::new().finalize_xof().fill(&mut data);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let writer = write_blob(&store, &data);
            tokio::pin!(writer);
            while backend.paused_chunk_puts.load(Ordering::SeqCst) < limit {
                tokio::select! {
                    _ = &mut writer => panic!("writer completed while uploads were paused"),
                    _ = backend.reached.notified() => {},
                }
            }
            // Give any admitted work another poll while all backend writes are
            // blocked. The upload window must not admit another chunk.
            futures::future::poll_fn(|cx| {
                assert!(writer.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            assert_eq!(backend.paused_chunk_puts.load(Ordering::SeqCst), limit);
            backend.disarm();
            backend.resume.notify_waiters();
            let digest = writer.await;
            assert_eq!(digest, BlobId::new(blake3::hash(&data).into()));
            assert_eq!(
                read_blob(&store, &digest).await.as_deref(),
                Some(data.as_slice())
            );
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn chunk_budget_smaller_than_the_upload_window_still_makes_progress() {
    for concurrency in [1, 2, 32, 64] {
        let backend = Arc::new(object_store::memory::InMemory::new());
        let store = ChunkedBlobStore::new(backend, Path::default(), 1024)
            .with_chunk_upload_concurrency(std::num::NonZeroUsize::new(concurrency).unwrap())
            .with_chunk_memory_budget_bytes(1);
        let data: Vec<_> = (0..100_000).map(|index| (index % 251) as u8).collect();
        let digest = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            write_blob(&store, &data),
        )
        .await
        .unwrap();
        assert_eq!(digest, BlobId::new(blake3::hash(&data).into()));
        assert_eq!(
            read_blob(&store, &digest).await.as_deref(),
            Some(data.as_slice())
        );
    }
}

#[tokio::test]
async fn shared_content_deduplicates() {
    let (svc, _dir) = small_chunked_store();
    let common: Vec<u8> = (0..50_000).map(|i| (i % 251) as u8).collect();
    let mut a = common.clone();
    a.extend_from_slice(b"----suffix-a");
    let mut b = common.clone();
    b.extend_from_slice(b"----suffix-b");

    let da = write_blob(&svc, &a).await;
    let db = write_blob(&svc, &b).await;

    let ca: std::collections::HashSet<_> = svc
        .chunks(&da)
        .await
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|c| c.digest)
        .collect();
    let cb: std::collections::HashSet<_> = svc
        .chunks(&db)
        .await
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|c| c.digest)
        .collect();

    assert!(
        ca.intersection(&cb).count() > 0,
        "similar blobs should share chunks"
    );
    assert_eq!(read_blob(&svc, &da).await.unwrap(), a);
    assert_eq!(read_blob(&svc, &db).await.unwrap(), b);
}

#[tokio::test]
async fn seek_reads_correct_bytes() {
    let (svc, _dir) = small_chunked_store();
    let data: Vec<u8> = (0..20_000).map(|i| (i % 256) as u8).collect();
    let digest = write_blob(&svc, &data).await;

    let mut r = svc.open_read(&digest).await.unwrap().unwrap();
    for &pos in &[0u64, 1, 1023, 1024, 5000, 19_999] {
        r.seek(io::SeekFrom::Start(pos)).await.unwrap();
        let mut buf = [0u8; 16];
        let n = r.read(&mut buf).await.unwrap();
        assert!(n > 0, "expected data at pos {pos}");
        let end = pos as usize + n;
        assert_eq!(&buf[..n], &data[pos as usize..end], "bytes at pos {pos}");
    }
}

#[tokio::test]
async fn seek_from_end_and_beyond() {
    let (svc, _dir) = small_chunked_store();
    let data: Vec<u8> = (0..5000).map(|i| (i % 256) as u8).collect();
    let digest = write_blob(&svc, &data).await;
    let mut r = svc.open_read(&digest).await.unwrap().unwrap();

    r.seek(io::SeekFrom::End(-100)).await.unwrap();
    let mut tail = Vec::new();
    r.read_to_end(&mut tail).await.unwrap();
    assert_eq!(tail, &data[data.len() - 100..]);

    assert!(
        r.seek(io::SeekFrom::Start(data.len() as u64 + 1))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn missing_blob_is_none() {
    let (svc, _dir) = small_chunked_store();
    let digest = BlobId::new(blake3::hash(b"never written").into());
    assert!(!svc.has(&digest).await.unwrap());
    assert!(svc.open_read(&digest).await.unwrap().is_none());
    assert_eq!(svc.chunks(&digest).await.unwrap(), None);
}

#[tokio::test]
async fn blob_sync_chunk_roundtrip_and_negotiation() {
    let (src, _d1) = small_chunked_store();
    let (dst, _d2) = small_chunked_store();
    let data: Vec<u8> = (0..50_000).map(|i| (i % 251) as u8).collect();
    let blob = write_blob(&src, &data).await;
    let manifest = src.chunks(&blob).await.unwrap().unwrap();
    assert!(manifest.len() >= 2, "want a multi-chunk blob");

    // everything is missing at dst; duplicates are folded (the repetitive
    // test data repeats a chunk within one manifest), order kept.
    let mut seen = std::collections::HashSet::new();
    let unique: Vec<ChunkMeta> = manifest
        .iter()
        .filter(|c| seen.insert(c.digest))
        .cloned()
        .collect();
    assert!(unique.len() < manifest.len(), "data should repeat chunks");
    let mut doubled = manifest.clone();
    doubled.extend(manifest.iter().cloned());
    assert_eq!(dst.missing_chunks(&doubled).await.unwrap(), unique);

    for chunk in &manifest {
        let compressed = src.get_chunk(&chunk.digest).await.unwrap().unwrap();
        dst.put_chunk(chunk, compressed).await.unwrap();
    }
    assert!(dst.missing_chunks(&manifest).await.unwrap().is_empty());

    dst.put_manifest(&blob, manifest).await.unwrap();
    assert_eq!(read_blob(&dst, &blob).await.unwrap(), data);
}

#[tokio::test]
async fn put_chunk_verifies_digest_and_size() {
    let (dst, _dir) = small_chunked_store();
    let data = b"chunk contents".to_vec();
    let compressed =
        Bytes::from(zstd::encode_all(&data[..], zstd::DEFAULT_COMPRESSION_LEVEL).unwrap());
    let digest = ChunkId::new(blake3::hash(&data).into());
    let len = data.len() as u64;

    let wrong_digest = ChunkMeta {
        digest: ChunkId::new(blake3::hash(b"other").into()),
        size: len,
    };
    assert!(
        dst.put_chunk(&wrong_digest, compressed.clone())
            .await
            .is_err()
    );

    // a smaller declared size trips the decompression cap, a larger one
    // trips the length check.
    let too_small = ChunkMeta {
        digest,
        size: len - 1,
    };
    assert!(dst.put_chunk(&too_small, compressed.clone()).await.is_err());
    let too_big = ChunkMeta {
        digest,
        size: len + 1,
    };
    assert!(dst.put_chunk(&too_big, compressed.clone()).await.is_err());

    let over_ceiling = ChunkMeta {
        digest,
        size: MAX_CHUNK_SIZE + 1,
    };
    assert!(
        dst.put_chunk(&over_ceiling, compressed.clone())
            .await
            .is_err()
    );

    assert!(
        !dst.has(&BlobId::new(digest.digest())).await.unwrap(),
        "nothing stored on failure"
    );

    let correct = ChunkMeta { digest, size: len };
    dst.put_chunk(&correct, compressed).await.unwrap();
}

#[tokio::test]
async fn put_manifest_verifies_binding_and_ceiling() {
    let (store, _dir) = small_chunked_store();
    let data: Vec<u8> = (0..50_000).map(|i| (i % 251) as u8).collect();
    let blob = write_blob(&store, &data).await;
    let manifest = store.chunks(&blob).await.unwrap().unwrap();
    assert!(manifest.len() >= 2, "want a multi-chunk blob");

    // reordered chunks assemble to different content.
    let mut reversed = manifest.clone();
    reversed.reverse();
    assert!(store.put_manifest(&blob, reversed).await.is_err());

    // the right chunks under the wrong blob digest.
    let wrong = BlobId::new(blake3::hash(b"nope").into());
    assert!(store.put_manifest(&wrong, manifest.clone()).await.is_err());
    assert!(!store.has(&wrong).await.unwrap());

    // a chunk that was never stored.
    let ghost = vec![ChunkMeta {
        digest: ChunkId::new(blake3::hash(b"ghost").into()),
        size: 5,
    }];
    let ghost_blob = BlobId::new(blake3::hash(b"ghost").into());
    assert!(store.put_manifest(&ghost_blob, ghost).await.is_err());

    // a declared size over the ceiling is rejected up front.
    let huge = vec![ChunkMeta {
        digest: manifest[0].digest,
        size: MAX_CHUNK_SIZE + 1,
    }];
    assert!(store.put_manifest(&blob, huge).await.is_err());

    // the empty blob: an empty manifest binding to BLAKE3 of nothing.
    let empty = BlobId::new(blake3::hash(b"").into());
    store.put_manifest(&empty, Vec::new()).await.unwrap();
    assert_eq!(read_blob(&store, &empty).await.unwrap(), b"");

    // and the true manifest commits.
    store.put_manifest(&blob, manifest).await.unwrap();
    assert_eq!(read_blob(&store, &blob).await.unwrap(), data);
}

#[tokio::test]
async fn single_chunk_blob_elides_its_manifest() {
    let (svc, _dir) = small_chunked_store();

    // single chunk: no manifest object, yet fully readable, and chunks()
    // synthesizes the one entry with the real uncompressed size.
    let small = write_blob(&svc, b"just one chunk").await;
    assert!(
        !head_exists(&svc.object_store, &svc.blob_path(&small))
            .await
            .unwrap(),
        "manifest must be elided"
    );
    assert!(svc.has(&small).await.unwrap());
    assert_eq!(
        read_blob(&svc, &small).await.as_deref(),
        Some(&b"just one chunk"[..])
    );
    assert_eq!(
        svc.chunks(&small).await.unwrap(),
        Some(vec![ChunkMeta {
            digest: ChunkId::new(small.digest()),
            size: 14
        }])
    );

    // a one-byte blob's zstd frame is shorter than the frame-header
    // ceiling `bare_chunk_meta` asks for, so the ranged read must come
    // back short rather than out of range.
    let tiny = write_blob(&svc, b"x").await;
    assert_eq!(
        svc.chunks(&tiny).await.unwrap(),
        Some(vec![ChunkMeta {
            digest: ChunkId::new(tiny.digest()),
            size: 1
        }])
    );

    // multi chunk: the manifest is stored as before.
    let data: Vec<u8> = (0..50_000).map(|i| (i % 251) as u8).collect();
    let big = write_blob(&svc, &data).await;
    assert!(
        head_exists(&svc.object_store, &svc.blob_path(&big))
            .await
            .unwrap()
    );

    // empty blob: keeps its empty manifest.
    let empty = write_blob(&svc, b"").await;
    assert!(
        head_exists(&svc.object_store, &svc.blob_path(&empty))
            .await
            .unwrap()
    );
    assert_eq!(svc.chunks(&empty).await.unwrap(), Some(vec![]));

    // an elided blob can still build an outboard and verified-read.
    svc.build_outboard(&small).await.unwrap();
    let got = svc.verified_read(&small, 5, 3).await.unwrap();
    assert_eq!(got, &b"one"[..]);
}

#[tokio::test]
async fn put_manifest_elides_single_self_chunk() {
    let (src, _d1) = small_chunked_store();
    let (dst, _d2) = small_chunked_store();
    let digest = write_blob(&src, b"tiny").await;
    let manifest = src.chunks(&digest).await.unwrap().unwrap();
    assert_eq!(manifest.len(), 1);

    // chunk not yet at dst: the elided manifest cannot commit.
    assert!(dst.put_manifest(&digest, manifest.clone()).await.is_err());

    let compressed = src
        .get_chunk(&ChunkId::new(digest.digest()))
        .await
        .unwrap()
        .unwrap();
    dst.put_chunk(&manifest[0], compressed).await.unwrap();
    dst.put_manifest(&digest, manifest).await.unwrap();

    assert!(
        !head_exists(&dst.object_store, &dst.blob_path(&digest))
            .await
            .unwrap(),
        "receiver stays elided"
    );
    assert_eq!(
        read_blob(&dst, &digest).await.as_deref(),
        Some(&b"tiny"[..])
    );
}

#[tokio::test]
async fn immutable_cache_control_on_content_objects() {
    let os: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let svc =
        ChunkedBlobStore::new(os.clone(), Path::default(), 1024).with_immutable_cache_control();

    // multi-chunk blob: chunk and manifest objects both carry the header.
    let data: Vec<u8> = (0..50_000).map(|i| (i % 251) as u8).collect();
    let blob = write_blob(&svc, &data).await;
    let manifest = os.get(&svc.blob_path(&blob)).await.unwrap();
    assert_eq!(
        manifest
            .attributes
            .get(&Attribute::CacheControl)
            .map(|v| v.as_ref()),
        Some(IMMUTABLE_CACHE_CONTROL)
    );
    let chunk = svc.chunks(&blob).await.unwrap().unwrap()[0].digest;
    let chunk_obj = os.get(&svc.chunk_path(&chunk)).await.unwrap();
    assert_eq!(
        chunk_obj
            .attributes
            .get(&Attribute::CacheControl)
            .map(|v| v.as_ref()),
        Some(IMMUTABLE_CACHE_CONTROL)
    );

    // without the flag, content carries no header either.
    let plain = ChunkedBlobStore::new(os.clone(), Path::from("plain"), 1024);
    let small = write_blob(&plain, b"uncached").await;
    let obj = os
        .get(&plain.chunk_path(&ChunkId::new(small.digest())))
        .await
        .unwrap();
    assert!(obj.attributes.get(&Attribute::CacheControl).is_none());
}

#[tokio::test]
async fn zero_length_read_is_not_eof() {
    // a read into an empty buffer must return Ok(0) and must not disable the
    // whole-blob verification: a subsequent full read still checks the digest.
    let (svc, _dir) = small_chunked_store();
    let data: Vec<u8> = (0..5000).map(|i| (i % 256) as u8).collect();
    let digest = write_blob(&svc, &data).await;

    let mut r = svc.open_read(&digest).await.unwrap().unwrap();
    // a zero-capacity read is not EOF.
    assert_eq!(r.read(&mut []).await.unwrap(), 0);
    // the full content still reads back and still verifies at EOF.
    let mut out = Vec::new();
    r.read_to_end(&mut out).await.unwrap();
    assert_eq!(out, data);
}

#[tokio::test]
async fn read_after_reopen_with_smaller_avg() {
    // a blob written with a large average chunk size stays readable when the
    // store is reopened with a smaller one: the decompression cap comes from
    // the manifest, not the reading store's configuration.
    let dir = tempfile::tempdir().unwrap();
    // uniform bytes give FastCDC no cut points, so it emits max-size chunks
    // (avg*2 = 2 MiB), larger than a default-avg reader's old 1 MiB cap.
    let data = vec![0xABu8; 3_000_000];

    let digest = {
        let fs = object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let store = ChunkedBlobStore::new(Arc::new(fs), Path::default(), 1 << 20);
        let d = write_blob(&store, &data).await;
        assert!(
            store.chunks(&d).await.unwrap().unwrap()[0].size > (1 << 20),
            "want a chunk larger than the default cap"
        );
        d
    };

    let fs = object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let reopened = ChunkedBlobStore::new(Arc::new(fs), Path::default(), DEFAULT_AVG_CHUNK_SIZE);
    assert_eq!(read_blob(&reopened, &digest).await, Some(data));
}

#[tokio::test]
async fn substituted_manifest_is_rejected() {
    // overwriting a multi-chunk blob's manifest with another blob's (valid)
    // manifest must not serve the other content: each chunk verifies against
    // its own digest, but the whole-blob check at EOF catches that the
    // reconstructed bytes are not this blob.
    let (svc, _dir) = small_chunked_store();
    let victim_data: Vec<u8> = (0..60_000).map(|i| (i % 253) as u8).collect();
    let victim = write_blob(&svc, &victim_data).await;
    assert!(
        svc.chunks(&victim).await.unwrap().unwrap().len() >= 2,
        "want a multi-chunk victim so the swapped manifest is actually read"
    );
    let other_data: Vec<u8> = (0..5000).map(|i| (i % 256) as u8).collect();
    let other = write_blob(&svc, &other_data).await;

    let other_manifest = svc
        .object_store
        .get(&svc.blob_path(&other))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    svc.object_store
        .put(&svc.blob_path(&victim), other_manifest.into())
        .await
        .unwrap();

    let mut r = svc.open_read(&victim).await.unwrap().unwrap();
    let mut out = Vec::new();
    assert!(
        r.read_to_end(&mut out).await.is_err(),
        "a swapped manifest must fail the whole-blob digest check"
    );
}

#[tokio::test]
async fn stale_negative_catalog_refreshes_to_a_new_manifest() {
    let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let base = Path::from("stale-negative-fallback");
    let reader = ChunkedBlobStore::packed_with_options(
        objects.clone(),
        base.clone(),
        1 << 20,
        crate::PackOptions {
            target_size: u64::MAX,
            cache_capacity: 0,
        },
    )
    .await
    .unwrap();
    let data = (0..400_000)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let digest = write_blob(&reader, &data).await;
    assert!(
        !head_exists(&objects, &reader.blob_path(&digest))
            .await
            .unwrap(),
        "the first representation must be manifest-elided"
    );

    let self_chunk = single_chunk_id(digest);
    let packed = reader.packed_chunks.as_ref().unwrap();
    packed.delete_many(&[self_chunk]).await.unwrap();
    packed.finish_deletions(true).await.unwrap();
    // A second process republishes the same content with smaller chunks and a
    // real manifest. The original reader still holds the earlier negative.
    let writer = ChunkedBlobStore::packed_with_options(
        objects,
        base,
        64 * 1024,
        crate::PackOptions {
            target_size: u64::MAX,
            cache_capacity: 0,
        },
    )
    .await
    .unwrap();
    assert_eq!(write_blob(&writer, &data).await, digest);
    assert_eq!(read_blob(&reader, &digest).await, Some(data));
}

#[tokio::test]
async fn fsck_uses_the_authoritative_manifest_catalog() {
    let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let base = Path::from("fsck-manifest-inventory");
    let store = ChunkedBlobStore::packed_with_options(
        objects.clone(),
        base,
        DEFAULT_AVG_CHUNK_SIZE,
        crate::PackOptions {
            target_size: u64::MAX,
            cache_capacity: 0,
        },
    )
    .await
    .unwrap();
    let data = b"valid self chunk";
    let digest = write_blob(&store, data).await;

    // An object injected outside the packed publication path is not part of
    // the repository: both ordinary reads and fsck use catalogued membership.
    objects
        .put(
            &store.blob_path(&digest),
            Bytes::from_static(b"not a manifest").into(),
        )
        .await
        .unwrap();
    assert_eq!(read_blob(&store, &digest).await.as_deref(), Some(&data[..]));
    assert!(
        crate::blob::BlobGc::list_blobs(&store)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn state_catalog_keeps_retired_manifests_until_catalog_publication() {
    use crate::blob::BlobGc;
    let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = ChunkedBlobStore::packed_with_options(
        objects.clone(),
        Path::from("manifest-retirement"),
        64 * 1024,
        crate::PackOptions {
            target_size: u64::MAX,
            cache_capacity: 0,
        },
    )
    .await
    .unwrap();
    store.publication().enable_state_catalog();
    let data = (0..400_000)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let digest = write_blob(&store, &data).await;
    let prepared = store.publication().prepare_state_commit().await.unwrap();
    assert!(prepared.catalog().is_some());
    prepared.commit().unwrap();
    let path = store.blob_path(&digest);
    assert!(head_exists(&objects, &path).await.unwrap());
    store.delete_blob(&digest).await.unwrap();
    store.finish_deletions().await.unwrap();
    assert!(head_exists(&objects, &path).await.unwrap());
    assert!(store.finish_collection(false).await.is_err());
    let prepared = store.publication().prepare_state_commit().await.unwrap();
    assert!(prepared.catalog().is_some());
    prepared.commit().unwrap();
    store.finish_collection(false).await.unwrap();
    assert!(!head_exists(&objects, &path).await.unwrap());
}

#[tokio::test]
async fn packed_manifest_deletion_is_published_without_a_pack_rewrite() {
    let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let base = Path::from("manifest-deletion-catalog");
    let store = ChunkedBlobStore::packed_with_options(
        objects.clone(),
        base.clone(),
        64 * 1024,
        crate::PackOptions {
            target_size: u64::MAX,
            cache_capacity: 0,
        },
    )
    .await
    .unwrap();
    let data = (0..400_000)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let digest = write_blob(&store, &data).await;
    assert!(
        head_exists(&objects, &store.blob_path(&digest))
            .await
            .unwrap()
    );

    crate::blob::BlobGc::delete_blob(&store, &digest)
        .await
        .unwrap();
    crate::blob::BlobGc::finish_deletions(&store).await.unwrap();
    assert!(
        crate::blob::BlobGc::list_blobs(&store)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );

    let reopened = ChunkedBlobStore::packed_with_options(
        objects,
        base,
        64 * 1024,
        crate::PackOptions {
            target_size: u64::MAX,
            cache_capacity: 0,
        },
    )
    .await
    .unwrap();
    assert!(
        crate::blob::BlobGc::list_blobs(&reopened)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn build_outboard_rejects_substituted_manifest() {
    // building an outboard for a blob whose manifest was swapped for another
    // blob's must fail (the content's bao root is not this blob's digest),
    // rather than persist an outboard describing the wrong content.
    let (svc, _dir) = small_chunked_store();
    let victim = write_blob(&svc, b"the real contents").await;
    let other_data: Vec<u8> = (0..5000).map(|i| (i % 256) as u8).collect();
    let other = write_blob(&svc, &other_data).await;

    let other_manifest = svc
        .object_store
        .get(&svc.blob_path(&other))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    svc.object_store
        .put(&svc.blob_path(&victim), other_manifest.into())
        .await
        .unwrap();

    assert!(svc.build_outboard(&victim).await.is_err());
    // and nothing was persisted for the victim.
    assert!(svc.get_outboard(&victim).await.unwrap().is_none());
}

#[tokio::test]
async fn write_after_close_errors() {
    use tokio::io::AsyncWriteExt;
    let (svc, _dir) = small_chunked_store();
    let mut w = svc.open_write().await;
    w.write_all(b"abc").await.unwrap();
    let (d1, _) = w.close().await.unwrap();
    // writing after close must fail, not silently vanish.
    assert!(w.write_all(b"def").await.is_err());
    // close stays idempotent and returns the original digest.
    let (d2, _) = w.close().await.unwrap();
    assert_eq!(d1, d2);
}

#[tokio::test]
async fn corrupted_chunk_fails_read() {
    let (svc, _dir) = small_chunked_store();

    // single-chunk fast path: overwrite the one chunk with valid zstd of
    // different content, so decode succeeds but the digest check rejects it.
    let digest = write_blob(&svc, b"hello").await;
    let bad = zstd::encode_all(&b"HELLO"[..], zstd::DEFAULT_COMPRESSION_LEVEL).unwrap();
    svc.object_store
        .put(&svc.chunk_path(&ChunkId::new(digest.digest())), bad.into())
        .await
        .unwrap();
    assert!(svc.open_read(&digest).await.is_err());

    // multi-chunk path: corrupt one referenced chunk and read through it.
    let data: Vec<u8> = (0..50_000).map(|i| (i % 251) as u8).collect();
    let blob = write_blob(&svc, &data).await;
    let chunks = svc.chunks(&blob).await.unwrap().unwrap();
    assert!(chunks.len() >= 2, "want a multi-chunk blob");
    let victim = chunks[0].digest;
    let bad = zstd::encode_all(
        &vec![0u8; chunks[0].size as usize][..],
        zstd::DEFAULT_COMPRESSION_LEVEL,
    )
    .unwrap();
    svc.object_store
        .put(&svc.chunk_path(&victim), bad.into())
        .await
        .unwrap();
    let mut r = svc.open_read(&blob).await.unwrap().unwrap();
    let mut out = Vec::new();
    assert!(r.read_to_end(&mut out).await.is_err());
}

#[tokio::test]
async fn fsck_repair_chaos_recovers_target_representation_failures() {
    #[derive(Debug, Clone, Copy)]
    enum TargetDamage {
        MissingManifest,
        InvalidManifest,
        InvalidCompressedChunk,
        SubstitutedChunk,
    }

    for damage in [
        TargetDamage::MissingManifest,
        TargetDamage::InvalidManifest,
        TargetDamage::InvalidCompressedChunk,
        TargetDamage::SubstitutedChunk,
    ] {
        let (target_store, _target_dir) = small_chunked_store();
        let (replica_store, _replica_dir) = small_chunked_store();
        let target = Repository::new(target_store.clone(), MemoryMetadataStore::new().unwrap());
        let replica = Repository::new(replica_store.clone(), MemoryMetadataStore::new().unwrap());
        let data: Vec<u8> = (0..60_000).map(|index| (index % 251) as u8).collect();
        let payload = publish_repair_payload(&target, &replica_store, &data).await;
        let chunks = target_store.chunks(&payload).await.unwrap().unwrap();
        assert!(chunks.len() >= 2, "{damage:?} needs a manifest-backed blob");

        match damage {
            TargetDamage::MissingManifest => {
                target_store
                    .object_store
                    .delete(&target_store.blob_path(&payload))
                    .await
                    .unwrap();
            }
            TargetDamage::InvalidManifest => {
                target_store
                    .object_store
                    .put(
                        &target_store.blob_path(&payload),
                        Bytes::from_static(b"not a Casita manifest").into(),
                    )
                    .await
                    .unwrap();
            }
            TargetDamage::InvalidCompressedChunk => {
                target_store
                    .object_store
                    .put(
                        &target_store.chunk_path(&chunks[0].digest),
                        Bytes::from_static(b"not zstd").into(),
                    )
                    .await
                    .unwrap();
            }
            TargetDamage::SubstitutedChunk => {
                let replacement = vec![0xA5; chunks[0].size as usize];
                let compressed =
                    zstd::encode_all(&replacement[..], zstd::DEFAULT_COMPRESSION_LEVEL).unwrap();
                target_store
                    .object_store
                    .put(
                        &target_store.chunk_path(&chunks[0].digest),
                        compressed.into(),
                    )
                    .await
                    .unwrap();
            }
        }

        assert!(is_unreadable(&target_store, &payload).await, "{damage:?}");
        let preview = target.preview_fsck_repair(Some(&replica)).await.unwrap();
        assert!(
            preview.actions.iter().any(|action| {
                action.kind == crate::FsckRepairActionKind::RepairPayloadFromReplica
                    && action.status == crate::FsckRepairActionStatus::Planned
            }),
            "{damage:?} should plan a replacement"
        );
        assert!(
            is_unreadable(&target_store, &payload).await,
            "dry-run changed {damage:?}"
        );

        let report = target.fsck_repair(Some(&replica)).await.unwrap();
        assert!(report.is_healthy(), "{damage:?}: {report:#?}");
        assert_eq!(read_blob(&target_store, &payload).await, Some(data));
    }
}

#[tokio::test]
async fn fsck_repair_chaos_refuses_an_unavailable_replica_then_retries() {
    let (target_store, _target_dir) = small_chunked_store();
    let (replica_store, replica_fault) = chaos_chunked_store(ChaosFault::ReadChunk);
    let target = Repository::new(target_store.clone(), MemoryMetadataStore::new().unwrap());
    let replica = Repository::new(replica_store.clone(), MemoryMetadataStore::new().unwrap());
    let data: Vec<u8> = (0..60_000).map(|index| (index % 251) as u8).collect();
    let payload = publish_repair_payload(&target, &replica_store, &data).await;
    let chunks = target_store.chunks(&payload).await.unwrap().unwrap();
    target_store
        .object_store
        .put(
            &target_store.chunk_path(&chunks[0].digest),
            Bytes::from_static(b"not zstd").into(),
        )
        .await
        .unwrap();

    replica_fault.arm();
    let report = target.fsck_repair(Some(&replica)).await.unwrap();
    assert!(!report.is_healthy());
    assert!(
        report.actions.is_empty(),
        "unavailable source must not repair"
    );
    assert!(report.findings.iter().any(|finding| {
        finding.kind == crate::FsckRepairFindingKind::CorruptPayload
            && finding.message.contains("replica is not verified")
    }));
    assert!(is_unreadable(&target_store, &payload).await);

    replica_fault.disarm();
    let retried = target.fsck_repair(Some(&replica)).await.unwrap();
    assert!(retried.is_healthy(), "{retried:#?}");
    assert_eq!(read_blob(&target_store, &payload).await, Some(data));
}

#[tokio::test]
async fn fsck_repair_chaos_refuses_a_missing_or_corrupt_replica() {
    #[derive(Debug, Clone, Copy)]
    enum ReplicaDamage {
        MissingChunk,
        InvalidCompressedChunk,
    }

    for damage in [
        ReplicaDamage::MissingChunk,
        ReplicaDamage::InvalidCompressedChunk,
    ] {
        let (target_store, _target_dir) = small_chunked_store();
        let (replica_store, _replica_dir) = small_chunked_store();
        let target = Repository::new(target_store.clone(), MemoryMetadataStore::new().unwrap());
        let replica = Repository::new(replica_store.clone(), MemoryMetadataStore::new().unwrap());
        let data: Vec<u8> = (0..60_000).map(|index| (index % 251) as u8).collect();
        let payload = publish_repair_payload(&target, &replica_store, &data).await;
        let chunks = target_store.chunks(&payload).await.unwrap().unwrap();
        target_store
            .object_store
            .put(
                &target_store.chunk_path(&chunks[0].digest),
                Bytes::from_static(b"not zstd").into(),
            )
            .await
            .unwrap();

        match damage {
            ReplicaDamage::MissingChunk => {
                replica_store
                    .object_store
                    .delete(&replica_store.chunk_path(&chunks[0].digest))
                    .await
                    .unwrap();
            }
            ReplicaDamage::InvalidCompressedChunk => {
                replica_store
                    .object_store
                    .put(
                        &replica_store.chunk_path(&chunks[0].digest),
                        Bytes::from_static(b"not zstd").into(),
                    )
                    .await
                    .unwrap();
            }
        }

        let report = target.fsck_repair(Some(&replica)).await.unwrap();
        assert!(!report.is_healthy(), "{damage:?}");
        assert!(report.actions.is_empty(), "{damage:?} must not be copied");
        assert!(report.findings.iter().any(|finding| {
            finding.kind == crate::FsckRepairFindingKind::CorruptPayload
                && finding.message.contains("replica is not verified")
        }));
        assert!(is_unreadable(&target_store, &payload).await);
    }
}

#[tokio::test]
async fn fsck_repair_chaos_manifest_publish_failure_stays_unreadable_then_retries() {
    let (target_store, target_fault) = chaos_chunked_store(ChaosFault::WriteManifest);
    let (replica_store, _replica_dir) = small_chunked_store();
    let target = Repository::new(target_store.clone(), MemoryMetadataStore::new().unwrap());
    let replica = Repository::new(replica_store.clone(), MemoryMetadataStore::new().unwrap());
    let data: Vec<u8> = (0..60_000).map(|index| (index % 251) as u8).collect();
    let payload = publish_repair_payload(&target, &replica_store, &data).await;
    target_store
        .object_store
        .delete(&target_store.blob_path(&payload))
        .await
        .unwrap();

    target_fault.arm();
    let failed = target.fsck_repair(Some(&replica)).await.unwrap();
    assert!(!failed.is_healthy());
    assert!(failed.actions.is_empty());
    assert!(failed.findings.iter().any(|finding| {
        finding.kind == crate::FsckRepairFindingKind::UnavailablePayload
            && finding.message.contains("could not be published")
    }));
    assert!(
        is_unreadable(&target_store, &payload).await,
        "a failed manifest publication exposed a partial payload"
    );

    target_fault.disarm();
    let retried = target.fsck_repair(Some(&replica)).await.unwrap();
    assert!(retried.is_healthy(), "{retried:#?}");
    assert_eq!(read_blob(&target_store, &payload).await, Some(data));
}

#[tokio::test]
async fn fsck_repair_chaos_cancellation_before_manifest_publish_is_retryable() {
    let (target_store, pause) = chaos_chunked_store(ChaosFault::PauseManifest);
    let (replica_store, _replica_dir) = small_chunked_store();
    let target = Repository::new(target_store.clone(), MemoryMetadataStore::new().unwrap());
    let replica = Repository::new(replica_store.clone(), MemoryMetadataStore::new().unwrap());
    let data: Vec<u8> = (0..60_000).map(|index| (index % 251) as u8).collect();
    let payload = publish_repair_payload(&target, &replica_store, &data).await;
    target_store
        .object_store
        .delete(&target_store.blob_path(&payload))
        .await
        .unwrap();

    pause.arm();
    let repair_target = target.clone();
    let repair_replica = replica.clone();
    let repair =
        tokio::spawn(async move { repair_target.fsck_repair(Some(&repair_replica)).await });
    pause.wait_until_paused().await;
    repair.abort();
    assert!(repair.await.unwrap_err().is_cancelled());
    pause.disarm();

    assert!(
        is_unreadable(&target_store, &payload).await,
        "cancellation before manifest publication exposed a partial payload"
    );
    let retried = target.fsck_repair(Some(&replica)).await.unwrap();
    assert!(retried.is_healthy(), "{retried:#?}");
    assert_eq!(read_blob(&target_store, &payload).await, Some(data));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn fsck_repair_chaos_process_abort_before_manifest_publish_is_retryable() {
    use std::os::unix::process::ExitStatusExt as _;
    use std::path::PathBuf;
    use std::process::Command;

    const WORKER_ENV: &str = "CASITA_FSCK_REPAIR_CRASH_WORKER";
    const TARGET_ENV: &str = "CASITA_FSCK_REPAIR_CRASH_TARGET";
    const REPLICA_ENV: &str = "CASITA_FSCK_REPAIR_CRASH_REPLICA";
    const TEST_NAME: &str = concat!(
        "blob::chunked::tests::",
        "fsck_repair_chaos_process_abort_before_manifest_publish_is_retryable"
    );

    if std::env::var_os(WORKER_ENV).is_some() {
        let target = Repository::local(PathBuf::from(std::env::var_os(TARGET_ENV).unwrap()))
            .await
            .unwrap();
        let replica = Repository::local(PathBuf::from(std::env::var_os(REPLICA_ENV).unwrap()))
            .await
            .unwrap();
        let result = target.fsck_repair(Some(&replica)).await;
        panic!("repair returned instead of aborting before manifest publication: {result:?}");
    }

    let temporary = tempfile::tempdir().unwrap();
    let target_root = temporary.path().join("target");
    let replica_root = temporary.path().join("replica");
    // The local profile's average chunk size is 256 KiB, so this must exceed
    // its 512 KiB maximum to exercise the manifest publication boundary.
    let data: Vec<u8> = (0..600_000).map(|index| (index % 251) as u8).collect();
    let payload = {
        let target = Repository::local(&target_root).await.unwrap();
        let replica = Repository::local(&replica_root).await.unwrap();
        let payload = publish_repair_payload(&target, replica.payloads(), &data).await;
        let mutation = replica.mutation_session().await.unwrap();
        let staged = mutation.stage_blob(&data).await.unwrap();
        mutation.publish_unrooted(vec![staged]).await.unwrap();
        drop(mutation);
        target
            .payloads()
            .object_store
            .delete(&target.payloads().blob_path(&payload))
            .await
            .unwrap();
        payload
    };

    let worker = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST_NAME, "--nocapture"])
        .env(WORKER_ENV, "1")
        .env(TARGET_ENV, &target_root)
        .env(REPLICA_ENV, &replica_root)
        .env("CASITA_TEST_ABORT_BEFORE_REPAIR_MANIFEST", "1")
        .output()
        .expect("launch fsck repair crash worker");
    assert_eq!(
        worker.status.signal(),
        Some(libc::SIGABRT),
        "worker did not abort at the manifest boundary: {worker:?}"
    );

    let target = Repository::local(&target_root).await.unwrap();
    let replica = Repository::local(&replica_root).await.unwrap();
    assert!(
        is_unreadable(target.payloads(), &payload).await,
        "a process abort exposed a partial payload"
    );
    let retried = target.fsck_repair(Some(&replica)).await.unwrap();
    assert!(retried.is_healthy(), "{retried:#?}");
    assert_eq!(read_blob(target.payloads(), &payload).await, Some(data));
}

#[tokio::test]
async fn fsck_repair_chaos_retains_its_objects_during_logical_collection() {
    let (target_store, pause) = chaos_chunked_store(ChaosFault::PauseManifest);
    let (replica_store, _replica_dir) = small_chunked_store();
    let coordination_root = tempfile::tempdir().unwrap();
    let state = MemoryMetadataStore::new().unwrap();
    // Separate coordination facades over one lock directory stand in for two
    // processes; the file-lock test suite establishes that this is the same
    // cross-process exclusion domain.
    let target = Repository::new(target_store.clone(), state.clone())
        .with_fs_coordination(coordination_root.path());
    let collector =
        Repository::new(target_store.clone(), state).with_fs_coordination(coordination_root.path());
    let replica = Repository::new(replica_store.clone(), MemoryMetadataStore::new().unwrap());
    let data: Vec<u8> = (0..60_000).map(|index| (index % 251) as u8).collect();
    let payload = publish_repair_payload(&target, &replica_store, &data).await;
    target_store
        .object_store
        .delete(&target_store.blob_path(&payload))
        .await
        .unwrap();

    pause.arm();
    let repair_target = target.clone();
    let repair_replica = replica.clone();
    let repair =
        tokio::spawn(async move { repair_target.fsck_repair(Some(&repair_replica)).await });
    pause.wait_until_paused().await;

    let inventory = target
        .metadata()
        .pin_store()
        .await
        .unwrap()
        .inventory()
        .await
        .unwrap();
    assert!(
        inventory.pins.values().any(|pin| pin.resources.contains(
            &crate::metadata::PinResource::StorageObject(
                target_store.blob_path(&payload).to_string()
            )
        )),
        "repair must pin its manifest before the paused upload"
    );

    // A missing manifest prevents a physical inventory until repaired, but
    // logical collection can run and must retain the object being repaired.
    assert_eq!(
        collector
            .try_collect_logical()
            .await
            .unwrap()
            .removed
            .logical_objects,
        0
    );

    pause.resume();
    let report = repair.await.unwrap().unwrap();
    assert!(report.is_healthy(), "{report:#?}");
    assert_eq!(read_blob(&target_store, &payload).await, Some(data));
    // The repair's retention hold must be released once publication finishes.
    collector.try_collect().await.unwrap();
}

#[tokio::test]
async fn fsck_repair_chaos_outboard_publish_failure_is_retryable() {
    let (store, fault) = chaos_chunked_store(ChaosFault::WriteOutboard);
    let repository = Repository::new(store.clone(), MemoryMetadataStore::new().unwrap());
    let mutation = repository.mutation_session().await.unwrap();
    let staged = mutation.stage_blob(b"rebuild this outboard").await.unwrap();
    let payload = staged.record().payload();
    mutation.publish_unrooted(vec![staged]).await.unwrap();
    store.build_outboard(&payload).await.unwrap();
    store
        .put_outboard(&payload, Bytes::from_static(b"not a bao outboard"))
        .await
        .unwrap();

    fault.arm();
    let failed = repository.fsck_repair(None).await.unwrap();
    assert!(!failed.is_healthy());
    assert!(failed.findings.iter().any(|finding| {
        finding.kind == crate::FsckRepairFindingKind::UnavailableOutboard
            && finding.message.contains("could not publish rebuilt")
    }));
    assert_eq!(
        store.get_outboard(&payload).await.unwrap(),
        Some(Bytes::from_static(b"not a bao outboard")),
        "a failed outboard replacement must leave the prior value alone"
    );

    fault.disarm();
    let retried = repository.fsck_repair(None).await.unwrap();
    assert!(retried.is_healthy(), "{retried:#?}");
    assert_eq!(
        store.verified_read(&payload, 0, 7).await.unwrap(),
        Bytes::from_static(b"rebuild")
    );
}

#[tokio::test]
async fn fsck_repair_repairs_a_corrupt_chunk_from_a_verified_replica() {
    let (target_store, _target_dir) = small_chunked_store();
    let (replica_store, _replica_dir) = small_chunked_store();
    let target = Repository::new(target_store.clone(), MemoryMetadataStore::new().unwrap());
    // The replica needs physical payload coverage, while its own state is held
    // during the repair to exclude concurrent collection.
    let replica = Repository::new(replica_store.clone(), MemoryMetadataStore::new().unwrap());
    let data: Vec<u8> = (0..60_000).map(|index| (index % 251) as u8).collect();

    let mutation = target.mutation_session().await.unwrap();
    let staged = mutation.stage_blob(&data).await.unwrap();
    let payload = staged.record().payload();
    mutation.publish_unrooted(vec![staged]).await.unwrap();
    drop(mutation);
    assert_eq!(write_blob(&replica_store, &data).await, payload);

    let chunks = target_store.chunks(&payload).await.unwrap().unwrap();
    assert!(chunks.len() >= 2, "test needs a manifest-backed blob");
    let victim = chunks[0].digest;
    let bad = zstd::encode_all(
        &vec![0u8; chunks[0].size as usize][..],
        zstd::DEFAULT_COMPRESSION_LEVEL,
    )
    .unwrap();
    target_store
        .object_store
        .put(&target_store.chunk_path(&victim), bad.into())
        .await
        .unwrap();

    let preview = target.preview_fsck_repair(Some(&replica)).await.unwrap();
    assert!(!preview.is_healthy());
    assert!(preview.actions.iter().any(|action| {
        action.kind == crate::FsckRepairActionKind::RepairPayloadFromReplica
            && action.status == crate::FsckRepairActionStatus::Planned
    }));

    let report = target.fsck_repair(Some(&replica)).await.unwrap();
    assert!(report.is_healthy(), "{report:#?}");
    assert!(report.actions.iter().any(|action| {
        action.kind == crate::FsckRepairActionKind::RepairPayloadFromReplica
            && action.status == crate::FsckRepairActionStatus::Repaired
    }));
    assert_eq!(
        read_blob(&target_store, &payload).await.as_deref(),
        Some(data.as_slice())
    );
}

#[tokio::test]
async fn fsck_repair_rebuilds_an_existing_corrupt_outboard() {
    let (store, _dir) = small_chunked_store();
    let repository = Repository::new(store.clone(), MemoryMetadataStore::new().unwrap());
    let mutation = repository.mutation_session().await.unwrap();
    let staged = mutation.stage_blob(b"rebuild this outboard").await.unwrap();
    let payload = staged.record().payload();
    mutation.publish_unrooted(vec![staged]).await.unwrap();
    drop(mutation);
    store.build_outboard(&payload).await.unwrap();
    store
        .put_outboard(&payload, Bytes::from_static(b"not a bao outboard"))
        .await
        .unwrap();

    let preview = repository.preview_fsck_repair(None).await.unwrap();
    assert!(!preview.is_healthy());
    assert!(preview.actions.iter().any(|action| {
        action.kind == crate::FsckRepairActionKind::RebuildOutboard
            && action.status == crate::FsckRepairActionStatus::Planned
    }));

    let report = repository.fsck_repair(None).await.unwrap();
    assert!(report.is_healthy(), "{report:#?}");
    assert!(report.actions.iter().any(|action| {
        action.kind == crate::FsckRepairActionKind::RebuildOutboard
            && action.status == crate::FsckRepairActionStatus::Repaired
    }));
    assert_eq!(
        store.verified_read(&payload, 0, 7).await.unwrap(),
        Bytes::from_static(b"rebuild")
    );
}

#[tokio::test]
async fn verified_read_range_edges() {
    let (svc, _dir) = small_chunked_store();
    let data: Vec<u8> = (0..20_000u32).map(|i| (i % 256) as u8).collect();
    let digest = write_blob(&svc, &data).await;
    svc.build_outboard(&digest).await.unwrap();
    let size = data.len() as u64;

    // out of range and overflowing requests error rather than panic.
    assert!(svc.verified_read(&digest, size, 1).await.is_err());
    assert!(svc.verified_read(&digest, size - 10, 100).await.is_err());
    assert!(svc.verified_read(&digest, u64::MAX, 1).await.is_err());

    // a zero-length read is valid and empty at any in-range offset, the end
    // of the blob included...
    assert!(svc.verified_read(&digest, 0, 0).await.unwrap().is_empty());
    assert!(
        svc.verified_read(&digest, 5000, 0)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        svc.verified_read(&digest, size, 0)
            .await
            .unwrap()
            .is_empty()
    );
    // ...but an offset past the end is out of range whatever the length.
    assert!(svc.verified_read(&digest, size + 1, 0).await.is_err());
}

#[tokio::test]
async fn fast_path_digest_matches_cdc_across_minimum_boundary() {
    let (store, _dir) = small_chunked_store();
    for length in [0, 1, 511, 512, 513] {
        let bytes: Vec<u8> = (0..length).map(|at| (at * 17 % 251) as u8).collect();
        let digest = write_blob(&store, &bytes).await;
        assert_eq!(digest.digest(), Digest::hash(&bytes));
        assert_eq!(read_blob(&store, &digest).await.unwrap(), bytes);
        assert_eq!(write_blob(&store, &bytes).await, digest);
    }
}

#[tokio::test]
async fn online_dedup_waits_for_deletion_then_revalidates_the_cached_pack() {
    use crate::metadata::{
        DataPin, DataPinLease, MemoryPinStore, PinResource, PinScope, PinStore,
        flush_repository_leases,
    };
    use std::collections::BTreeSet;

    let objects = Arc::new(object_store::memory::InMemory::new());
    let base = Path::from("online-dedup");
    let store = ChunkedBlobStore::packed_with_options(
        objects.clone(),
        base.clone(),
        1024,
        crate::PackOptions {
            target_size: 1,
            cache_capacity: 0,
        },
    )
    .await
    .unwrap();
    let bytes = b"identical content after collection";
    let blob = store.put_slice(bytes).await.unwrap();
    let paths = objects
        .list(Some(&base.clone().join("packs")))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(paths.len(), 1);
    let pack = paths[0].location.clone();
    let resource = PinResource::StorageObject(pack.to_string());
    let ledger = Arc::new(MemoryPinStore::default());
    let deletion = ledger
        .claim_deletions(0, BTreeSet::from([resource.clone()]))
        .await
        .unwrap()
        .unwrap();
    let pin = DataPinLease::acquire(
        ledger.clone(),
        DataPin {
            scope: PinScope::Staging,
            catalog: None,
            resources: BTreeSet::new(),
        },
    )
    .await
    .unwrap();
    let token = pin.token().clone();
    let batch = store.begin_pinned_batch(pin).unwrap();
    let rewriting = {
        let store = store.clone();
        tokio::spawn(async move { store.put_slice(bytes).await })
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let inventory = ledger.inventory().await.unwrap();
            if inventory.pins[&token]
                .resources
                .contains(&PinResource::Chunk(single_chunk_id(blob)))
            {
                assert!(!inventory.pins[&token].resources.contains(&resource));
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!rewriting.is_finished());
    // Settle the old deletion before allowing an identical path to be reused.
    objects.delete(&pack).await.unwrap();
    ledger.finish_deletions(&deletion).await.unwrap();
    assert_eq!(rewriting.await.unwrap().unwrap(), blob);
    store.flush().await.unwrap();
    assert_eq!(store.read_to_vec(&blob).await.unwrap().unwrap(), bytes);
    let inventory = ledger.inventory().await.unwrap();
    assert!(inventory.pins[&token].resources.contains(&resource));
    assert!(
        inventory.pins[&token]
            .resources
            .contains(&PinResource::Blob(blob))
    );
    drop(batch);
    flush_repository_leases().await.unwrap();
    assert!(ledger.inventory().await.unwrap().pins.is_empty());
}

#[tokio::test]
async fn repairing_repository_collects_unrelated_payloads_with_a_live_reader() {
    let (near, _near_dir) = small_chunked_store();
    let (far, _far_dir) = small_chunked_store();
    let repository = Repository::new(
        RepairingBlobStore::new(near, far),
        MemoryMetadataStore::new().unwrap(),
    );
    let mutation = repository.mutation_session().await.unwrap();
    let live = mutation
        .stage_blob(b"reader remains open throughout collection")
        .await
        .unwrap();
    let key = live.record().key().clone();
    let garbage = mutation
        .stage_blob(b"unpublished orphan beside a live reader")
        .await
        .unwrap();
    let garbage_payload = garbage.record().payload();
    drop(garbage);
    mutation
        .publish_rooted(vec![live], "repairing/live".parse().unwrap(), key.clone())
        .await
        .unwrap();
    drop(mutation);
    crate::flush_repository_leases().await.unwrap();
    let hold = repository.retention_hold().await.unwrap();
    let (_, mut reader) = hold.open_payload(&key).await.unwrap().unwrap();
    drop(hold);
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), repository.collect())
        .await
        .expect("scoped collection must not wait for the raw-reader gate")
        .unwrap();
    assert_eq!(outcome.removed.chunks, 1);
    assert!(
        !repository
            .payloads()
            .near()
            .has(&garbage_payload)
            .await
            .unwrap()
    );
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"reader remains open throughout collection");
}

#[tokio::test]
async fn repair_on_read_pins_its_uploaded_representation() {
    let (near, _near_dir) = small_chunked_store();
    let (far, _far_dir) = small_chunked_store();
    let state = MemoryMetadataStore::new().unwrap();
    let target = Repository::new(near.clone(), state.clone());
    let data = b"repair with an online reader";
    let payload = publish_repair_payload(&target, &far, data).await;
    crate::flush_repository_leases().await.unwrap();
    let chunk = near.chunks(&payload).await.unwrap().unwrap()[0].digest;
    near.object_store
        .put(
            &near.chunk_path(&chunk),
            Bytes::from_static(b"not zstd").into(),
        )
        .await
        .unwrap();
    let repository = Repository::new(RepairingBlobStore::new(near.clone(), far), state);
    let hold = repository.retention_hold().await.unwrap();
    let (_, mut reader) = hold
        .open_payload(&crate::ObjectKey::blob(payload))
        .await
        .unwrap()
        .unwrap();
    let inventory = repository
        .metadata()
        .pin_store()
        .await
        .unwrap()
        .inventory()
        .await
        .unwrap();
    assert!(
        inventory.pins.values().any(|pin| pin.resources.contains(
            &crate::metadata::PinResource::StorageObject(near.chunk_path(&chunk).to_string())
        )),
        "repair-on-read uploads must use the reader's online pin"
    );
    drop(hold);
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, data);
}

#[tokio::test]
async fn paged_versions_share_metadata_across_reopen_and_reader_retention() {
    use crate::metadata::{DataPin, DataPinLease, PinScope, PinStore};
    let objects = Arc::new(object_store::memory::InMemory::new());
    let store = ChunkedBlobStore::new(objects.clone(), Path::default(), 64 * 1024);
    let original: Vec<u8> = (0..8 * 1024 * 1024 + 1).map(|i| (i * 29) as u8).collect();
    let old = store.put_slice(&original).await.unwrap();
    let pages = super::pages::Pages::from(&store);
    let old_root = super::pages::descriptor(&store, &store.blob_path(&old), super::pages::CHUNKS)
        .await
        .unwrap()
        .unwrap();
    let old_bao =
        super::pages::descriptor(&store, &store.outboard_path(&old), super::pages::OUTBOARD)
            .await
            .unwrap()
            .unwrap();
    let mut old_pages = std::collections::BTreeSet::new();
    pages.mark(old_root, &mut old_pages).await.unwrap();
    pages.mark(old_bao, &mut old_pages).await.unwrap();
    let ledger = Arc::new(crate::metadata::MemoryPinStore::default());
    let pin = DataPinLease::acquire(
        ledger.clone(),
        DataPin {
            scope: PinScope::Staging,
            catalog: None,
            resources: Default::default(),
        },
    )
    .await
    .unwrap();
    let batch = store.begin_pinned_batch(pin.clone()).unwrap();
    let mut reader = store
        .open_verified(&old, original.len() as u64)
        .await
        .unwrap()
        .unwrap();
    let mut prefix = [0; 1024];
    reader.read_exact(&mut prefix).await.unwrap();
    drop((batch, pin));
    let offset = original.len() / 2 - 100;
    let (new, _) = store
        .write_scope()
        .run(store.overwrite(&old, original.len() as u64, offset as u64, &[42; 300]))
        .await
        .unwrap();
    let mut expected = original.clone();
    expected[offset..offset + 300].fill(42);
    assert_eq!(new, BlobId::new(blake3::hash(&expected).into()));
    let mut new_pages = std::collections::BTreeSet::new();
    pages
        .mark(
            super::pages::descriptor(&store, &store.blob_path(&new), super::pages::CHUNKS)
                .await
                .unwrap()
                .unwrap(),
            &mut new_pages,
        )
        .await
        .unwrap();
    pages
        .mark(
            super::pages::descriptor(&store, &store.outboard_path(&new), super::pages::OUTBOARD)
                .await
                .unwrap()
                .unwrap(),
            &mut new_pages,
        )
        .await
        .unwrap();
    assert!(old_pages.intersection(&new_pages).count() > 0);
    assert!(old_pages.difference(&new_pages).count() > 0);
    // Exercise retained page roots even when the physical descriptor changes
    // or disappears; payload chunks are deliberately still available here.
    store.delete_blob(&old).await.unwrap();
    let collector = ledger
        .begin_collection(ledger.inventory().await.unwrap().revision, None)
        .await
        .unwrap()
        .unwrap();
    store
        .reclaim_metadata_pinned(ledger.clone(), Default::default())
        .await
        .unwrap();
    let mut remaining = Vec::new();
    reader.read_to_end(&mut remaining).await.unwrap();
    assert_eq!(remaining, original[1024..]);
    drop(reader);
    crate::flush_repository_leases().await.unwrap();
    ledger.finish_collection(&collector).await.unwrap();
    let collector = ledger
        .begin_collection(ledger.inventory().await.unwrap().revision, None)
        .await
        .unwrap()
        .unwrap();
    store
        .reclaim_metadata_pinned(ledger.clone(), Default::default())
        .await
        .unwrap();
    ledger.finish_collection(&collector).await.unwrap();
    for hash in old_pages.difference(&new_pages) {
        assert!(objects.head(&pages.path(hash)).await.is_err());
    }
    for hash in &new_pages {
        assert!(objects.head(&pages.path(hash)).await.is_ok());
    }
    let reopened = ChunkedBlobStore::new(objects, Path::default(), 64 * 1024);
    assert_eq!(reopened.read_to_vec(&new).await.unwrap().unwrap(), expected);
    let mut reader = reopened
        .open_verified(&new, expected.len() as u64)
        .await
        .unwrap()
        .unwrap();
    let mut read = Vec::new();
    reader.read_to_end(&mut read).await.unwrap();
    assert_eq!(read, expected);
}

#[tokio::test]
async fn paged_overwrite_failed_publication_leaves_old_version_and_reclaims_orphans() {
    let (store, fault) = chaos_chunked_store(ChaosFault::WriteManifest);
    let original: Vec<_> = (0..2 * 1024 * 1024 + 1).map(|i| (i * 13) as u8).collect();
    let old = store.put_slice(&original).await.unwrap();
    let before: Vec<_> = fault
        .inner
        .list(Some(&Path::from("pages/")))
        .try_collect()
        .await
        .unwrap();
    fault.arm();
    assert!(
        store
            .overwrite(&old, original.len() as u64, 16380, &[7; 300])
            .await
            .is_err()
    );
    fault.disarm();
    assert_eq!(store.read_to_vec(&old).await.unwrap().unwrap(), original);
    store.reclaim_metadata().await.unwrap();
    let after: Vec<_> = fault
        .inner
        .list(Some(&Path::from("pages/")))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(after.len(), before.len());
    let (new, _) = store
        .overwrite(&old, original.len() as u64, 16380, &[7; 300])
        .await
        .unwrap();
    let mut expected = original;
    expected[16380..16680].fill(7);
    assert_eq!(new, BlobId::new(blake3::hash(&expected).into()));
    assert_eq!(store.read_to_vec(&new).await.unwrap().unwrap(), expected);
}

#[tokio::test]
async fn paged_corruption_never_releases_unauthenticated_data_or_publishes_an_edit() {
    let (store, _) = small_chunked_store();
    let original: Vec<_> = (0..2 * 1024 * 1024 + 1).map(|i| (i * 53) as u8).collect();
    let old = store.put_slice(&original).await.unwrap();
    let root = super::pages::descriptor(&store, &store.outboard_path(&old), super::pages::OUTBOARD)
        .await
        .unwrap()
        .unwrap();
    let pages = super::pages::Pages::from(&store);
    let path = pages.path(&root.hash);
    let bytes = store
        .object_store
        .get(&path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let mut corrupt = bytes.to_vec();
    *corrupt.last_mut().unwrap() ^= 1;
    put_object(&store.object_store, &path, corrupt, false)
        .await
        .unwrap();
    let mut reader = store
        .open_verified(&old, original.len() as u64)
        .await
        .unwrap()
        .unwrap();
    let mut output = Vec::new();
    assert!(reader.read_to_end(&mut output).await.is_err());
    assert!(output.is_empty());
    assert!(
        store
            .overwrite(&old, original.len() as u64, 16380, &[3; 300])
            .await
            .is_err()
    );
    put_object(&store.object_store, &path, bytes, false)
        .await
        .unwrap();
    assert_eq!(store.read_to_vec(&old).await.unwrap().unwrap(), original);
}

#[tokio::test]
async fn paged_gc_cancellation_keeps_claim_until_io_settles() {
    use crate::metadata::{PinResource, PinStore};
    let (store, pause) = chaos_chunked_store(ChaosFault::PauseDelete);
    let pages = super::pages::Pages::from(&store);
    let orphan = pages.bytes_leaf(&[42; 4096]).await.unwrap();
    let resource = PinResource::StorageObject(pages.path(&orphan.hash).to_string());
    let ledger = Arc::new(crate::metadata::MemoryPinStore::default());
    let collector = ledger.begin_collection(0, None).await.unwrap().unwrap();
    pause.arm();
    let worker = tokio::spawn({
        let store = store.clone();
        let ledger = ledger.clone();
        async move {
            store
                .reclaim_metadata_pinned(ledger, Default::default())
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), pause.wait_until_paused())
        .await
        .unwrap();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    assert!(
        ledger
            .inventory()
            .await
            .unwrap()
            .deletions
            .values()
            .any(|paths| paths.contains(&resource))
    );
    pause.resume();
    crate::flush_repository_leases().await.unwrap();
    assert!(ledger.inventory().await.unwrap().deletions.is_empty());
    assert!(pause.inner.head(&pages.path(&orphan.hash)).await.is_err());
    ledger.finish_collection(&collector).await.unwrap();
}

#[tokio::test]
async fn manifest_publication_rebuilds_flat_metadata_without_changing_identity() {
    let (store, _) = small_chunked_store();
    let original: Vec<_> = (0..2 * 1024 * 1024 + 1).map(|i| (i * 61) as u8).collect();
    let id = store.put_slice(&original).await.unwrap();
    let chunks = store.chunks(&id).await.unwrap().unwrap();
    let outboard = store.get_outboard(&id).await.unwrap().unwrap();
    assert!(chunks.len() > super::pages::FANOUT);
    put_object(
        &store.object_store,
        &store.blob_path(&id),
        encode_manifest(&chunks),
        false,
    )
    .await
    .unwrap();
    put_object(
        &store.object_store,
        &store.outboard_path(&id),
        outboard.clone(),
        false,
    )
    .await
    .unwrap();
    store.put_manifest(&id, chunks.clone()).await.unwrap();
    assert!(
        super::pages::descriptor(&store, &store.blob_path(&id), super::pages::CHUNKS)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        super::pages::descriptor(&store, &store.outboard_path(&id), super::pages::OUTBOARD)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(store.chunks(&id).await.unwrap().unwrap(), chunks);
    assert_eq!(store.get_outboard(&id).await.unwrap().unwrap(), outboard);
    assert_eq!(store.read_to_vec(&id).await.unwrap().unwrap(), original);
}

#[tokio::test]
async fn empty_chunk_entries_are_rejected_without_replacing_existing_metadata() {
    let (store, _) = small_chunked_store();
    let original = b"positive length chunks";
    let id = store.put_slice(original).await.unwrap();
    let before = store.chunks(&id).await.unwrap().unwrap();
    let empty = ChunkMeta {
        digest: ChunkId::new(blake3::hash(b"").into()),
        size: 0,
    };
    assert!(
        store
            .put_chunk(&empty, zstd::encode_all(b"".as_slice(), 0).unwrap().into())
            .await
            .is_err()
    );
    assert!(store.get_chunk(&empty.digest).await.unwrap().is_none());
    // Cover both compact and indexed manifest sizes.
    for count in [1, super::pages::FANOUT + 1] {
        let mut chunks = vec![empty.clone(); count];
        chunks.extend(before.clone());
        assert!(store.put_manifest(&id, chunks).await.is_err());
        assert_eq!(store.chunks(&id).await.unwrap().unwrap(), before);
        assert_eq!(store.read_to_vec(&id).await.unwrap().unwrap(), original);
    }
    // Empty content is still valid and has no chunk entries, including sync publication.
    let empty_id = BlobId::new(blake3::hash(b"").into());
    store.put_manifest(&empty_id, Vec::new()).await.unwrap();
    assert!(store.chunks(&empty_id).await.unwrap().unwrap().is_empty());
    assert!(
        store
            .read_to_vec(&empty_id)
            .await
            .unwrap()
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.put_slice(b"").await.unwrap(), empty_id);
}

#[tokio::test]
async fn paged_fsck_repairs_corrupt_and_missing_outboard_pages() {
    let (store, _) = small_chunked_store();
    let repository = Repository::new(store.clone(), MemoryMetadataStore::new().unwrap());
    let original: Vec<_> = (0..2 * 1024 * 1024 + 1).map(|i| (i * 47) as u8).collect();
    let mutation = repository.mutation_session().await.unwrap();
    let staged = mutation.stage_blob(&original).await.unwrap();
    let id = staged.record().payload();
    let key = staged.record().key().clone();
    mutation
        .publish_rooted(
            vec![staged],
            crate::RootName::try_from("file").unwrap(),
            key,
        )
        .await
        .unwrap();
    drop(mutation);
    let root = super::pages::descriptor(&store, &store.outboard_path(&id), super::pages::OUTBOARD)
        .await
        .unwrap()
        .unwrap();
    let path = super::pages::Pages::from(&store).path(&root.hash);
    for missing in [false, true] {
        if missing {
            store.object_store.delete(&path).await.unwrap();
        } else {
            put_object(&store.object_store, &path, b"corrupt page".to_vec(), false)
                .await
                .unwrap();
        }
        let error = store.get_outboard(&id).await.unwrap_err();
        assert!(crate::blob::is_integrity_error(&error), "{error}");
        let report = repository.fsck_repair(None).await.unwrap();
        assert!(report.is_healthy(), "{report:#?}");
        assert!(
            report
                .actions
                .iter()
                .any(|action| matches!(action.kind, crate::FsckRepairActionKind::RebuildOutboard))
        );
        assert_eq!(
            store.get_outboard(&id).await.unwrap().unwrap(),
            store.compute_outboard(&id).await.unwrap()
        );
    }
}

#[tokio::test]
async fn paged_fsck_repairs_a_corrupt_chunk_map_from_a_replica() {
    let (store, _) = small_chunked_store();
    let (replica_store, _) = small_chunked_store();
    let target = Repository::new(store.clone(), MemoryMetadataStore::new().unwrap());
    let replica = Repository::new(replica_store.clone(), MemoryMetadataStore::new().unwrap());
    let data: Vec<_> = (0..2 * 1024 * 1024 + 1).map(|i| (i * 43) as u8).collect();
    let id = publish_repair_payload(&target, &replica_store, &data).await;
    let root = super::pages::descriptor(&store, &store.blob_path(&id), super::pages::CHUNKS)
        .await
        .unwrap()
        .unwrap();
    let path = super::pages::Pages::from(&store).path(&root.hash);
    put_object(
        &store.object_store,
        &path,
        b"corrupt chunk map".to_vec(),
        false,
    )
    .await
    .unwrap();
    assert!(crate::blob::is_integrity_error(
        &store.chunks(&id).await.unwrap_err()
    ));
    let report = target.fsck_repair(Some(&replica)).await.unwrap();
    assert!(report.is_healthy(), "{report:#?}");
    assert_eq!(store.read_to_vec(&id).await.unwrap().unwrap(), data);
}

#[tokio::test]
async fn paged_gc_recovers_an_orphan_descriptor_after_its_last_page_was_deleted() {
    let (store, _) = small_chunked_store();
    let pages = super::pages::Pages::from(&store);
    let root = pages.bytes_leaf(&[42; 64]).await.unwrap();
    let orphan = BlobId::new(blake3::hash(b"never published").into());
    let path = store.outboard_path(&orphan);
    put_object(&store.object_store, &path, root.encode(), false)
        .await
        .unwrap();
    store
        .object_store
        .delete(&pages.path(&root.hash))
        .await
        .unwrap();
    store.reclaim_metadata().await.unwrap();
    assert!(store.object_store.head(&path).await.is_err());
}

#[tokio::test]
async fn cancelled_blocking_decode_is_drained_before_ownership_release() {
    let owner = Arc::new(());
    let worker_owner = owner.clone();
    let (started, ready) = tokio::sync::oneshot::channel();
    let (resume, wait) = std::sync::mpsc::channel();
    let task = super::DecodeTask(Some(tokio::task::spawn_blocking(move || {
        let _owner = worker_owner;
        started.send(()).unwrap();
        wait.recv().unwrap();
        Ok(Bytes::new())
    })));
    ready.await.unwrap();
    drop(task);
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(20),
            crate::metadata::flush_repository_leases()
        )
        .await
        .is_err()
    );
    assert_eq!(Arc::strong_count(&owner), 2);
    resume.send(()).unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        crate::metadata::flush_repository_leases(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(Arc::strong_count(&owner), 1);
}

#[tokio::test]
async fn blob_coadmits_known_identities_and_bao_path() {
    use crate::metadata::{DataPin, DataPinLease, FilePinStore, PinResource, PinScope, PinStore};
    use std::collections::BTreeSet;
    for (layout, average, size) in ["packed", "loose"].into_iter().flat_map(|layout| {
        [
            (1024, 1usize),
            (1024, 511),
            (1024, 512),
            (1024, 513),
            (1024, 2047),
            (1024, 2048),
            (65536, 16383),
            (65536, 16384),
            (65536, 16385),
            (65536, 32767),
            (65536, 32768),
            (65536, 32769),
        ]
        .map(move |(average, size)| (layout, average, size))
    }) {
        let directory = tempfile::tempdir().unwrap();
        let ledger = Arc::new(FilePinStore::new(directory.path().join("pins")));
        let objects = Arc::new(object_store::memory::InMemory::new());
        let store = small_blob_pin_store(objects, layout, average).await;
        let pin = DataPinLease::acquire(
            ledger.clone(),
            DataPin {
                scope: PinScope::Staging,
                catalog: None,
                resources: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
        let token = pin.token().clone();
        let batch = store.begin_pinned_batch(pin).unwrap();
        let before = ledger.inventory().await.unwrap().revision;
        let bytes = vec![size as u8; size];
        let blob = write_blob(&store, &bytes).await;
        let inventory = ledger.inventory().await.unwrap();
        // Packed storage can coadmit a sole chunk after EOF, except at the
        // maximum. Loose storage emits at the minimum before the blob identity
        // is known, so it needs one later protection edit for that identity.
        assert_eq!(
            inventory.revision - before,
            if size == average as usize * 2 || (layout == "loose" && size >= average as usize / 2) {
                2
            } else {
                1
            },
            "layout={layout} average={average} size={size}"
        );
        let resources = &inventory.pins[&token].resources;
        assert!(resources.contains(&PinResource::Blob(blob)));
        assert!(resources.contains(&PinResource::Chunk(single_chunk_id(blob))));
        let bao = PinResource::StorageObject(
            sharded_path(&Path::from("blobs"), "bao", blob.as_digest()).to_string(),
        );
        assert_eq!(
            resources.contains(&bao),
            size > crate::verified::ingest::BLOCK_BYTES
        );
        if layout == "loose" {
            assert!(resources.contains(&PinResource::StorageObject(
                chunk_path(&Path::from("blobs"), &single_chunk_id(blob)).to_string()
            )));
        }
        assert_eq!(write_blob(&store, &bytes).await, blob);
        assert_eq!(
            ledger.inventory().await.unwrap().revision,
            inventory.revision
        );
        store.flush().await.unwrap();
        assert_eq!(read_blob(&store, &blob).await.unwrap(), bytes);
        drop(batch);
        crate::flush_repository_leases().await.unwrap();
        assert!(ledger.inventory().await.unwrap().pins.is_empty());
    }
}

async fn small_blob_pin_store(
    objects: Arc<dyn ObjectStore>,
    layout: &str,
    average: u32,
) -> ChunkedBlobStore {
    match layout {
        "loose" => ChunkedBlobStore::new(objects, Path::from("blobs"), average),
        "packed" => ChunkedBlobStore::packed_with_options(
            objects,
            Path::from("blobs"),
            average,
            crate::PackOptions {
                target_size: 16 * 1024 * 1024,
                cache_capacity: 0,
            },
        )
        .await
        .unwrap(),
        _ => panic!("unknown pin benchmark layout: {layout}"),
    }
}

#[tokio::test]
#[ignore = "permanent durable small-blob staging benchmark; run benchmark small-blob-pins"]
async fn benchmark_small_blob_pins() {
    use crate::metadata::{DataPin, DataPinLease, FilePinStore, PinScope, PinStore};
    use std::collections::BTreeSet;
    let count: usize = std::env::var("CASITA_BENCH_BLOBS")
        .unwrap()
        .parse()
        .unwrap();
    let size: usize = std::env::var("CASITA_BENCH_BLOB_BYTES")
        .unwrap()
        .parse()
        .unwrap();
    assert!(count > 0 && size >= 8);
    let directory = tempfile::tempdir().unwrap();
    let ledger = Arc::new(FilePinStore::new(directory.path().join("pins")));
    let objects =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap());
    let layout = std::env::var("CASITA_BENCH_PIN_LAYOUT").unwrap_or_else(|_| "packed".into());
    let store = small_blob_pin_store(objects.clone(), &layout, 1024).await;
    let pin = DataPinLease::acquire(
        ledger.clone(),
        DataPin {
            scope: PinScope::Staging,
            catalog: None,
            resources: BTreeSet::new(),
        },
    )
    .await
    .unwrap();
    let batch = store.begin_pinned_batch(pin).unwrap();
    let payload = |index: usize| {
        let mut bytes = vec![17; size];
        bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
        bytes
    };
    let before = ledger.inventory().await.unwrap().revision;
    let start = std::time::Instant::now();
    let mut blobs = Vec::new();
    for index in 0..count {
        let bytes = payload(index);
        blobs.push(write_blob(&store, &bytes).await);
    }
    let nanos = start.elapsed().as_nanos();
    let edits = ledger.inventory().await.unwrap().revision - before;
    // Readback and duplicate-write validation are outside the measured phase.
    for (index, blob) in blobs.iter().enumerate() {
        assert_eq!(*blob, write_blob(&store, &payload(index)).await);
    }
    assert_eq!(ledger.inventory().await.unwrap().revision - before, edits);
    store.flush().await.unwrap();
    let reader = small_blob_pin_store(objects, &layout, 1024).await;
    for (index, blob) in blobs.iter().enumerate() {
        assert_eq!(read_blob(&reader, blob).await.unwrap(), payload(index));
    }
    drop(batch);
    crate::flush_repository_leases().await.unwrap();
    assert!(ledger.inventory().await.unwrap().pins.is_empty());
    println!(
        "small_blob_pins_sample {}",
        serde_json::json!({
            "layout": layout, "count": count, "bytes": size, "nanos": nanos, "ledger_edits": edits,
            "correctness": "exact readback, duplicate identity, no duplicate edits, no leaked pins",
        })
    );
}

/// A ledger that reports every refused protection, so a test can observe a
/// writer retrying against a deletion claim instead of guessing from time.
#[derive(Default)]
struct RefusalCountingLedger {
    inner: crate::metadata::MemoryPinStore,
    refused: AtomicUsize,
    changed: Notify,
}

impl RefusalCountingLedger {
    async fn refused_at_least(&self, count: usize) {
        while self.refused.load(Ordering::Acquire) < count {
            self.changed.notified().await;
        }
    }
}

#[async_trait]
impl crate::metadata::PinStore for RefusalCountingLedger {
    async fn inventory(&self) -> Result<crate::metadata::PinInventory, crate::MetadataError> {
        self.inner.inventory().await
    }
    async fn register(
        &self,
        pin: crate::metadata::DataPin,
    ) -> Result<Option<crate::metadata::PinToken>, crate::MetadataError> {
        self.inner.register(pin).await
    }
    async fn protect(
        &self,
        token: &crate::metadata::PinToken,
        resources: std::collections::BTreeSet<crate::metadata::PinResource>,
    ) -> Result<bool, crate::MetadataError> {
        let protected = self.inner.protect(token, resources).await?;
        if !protected {
            self.refused.fetch_add(1, Ordering::AcqRel);
            self.changed.notify_one();
        }
        Ok(protected)
    }
    async fn release(&self, token: &crate::metadata::PinToken) -> Result<(), crate::MetadataError> {
        self.inner.release(token).await
    }
    async fn begin_prune(
        &self,
        revision: u64,
    ) -> Result<Option<crate::metadata::PinToken>, crate::MetadataError> {
        self.inner.begin_prune(revision).await
    }
    async fn finish_prune(
        &self,
        token: &crate::metadata::PinToken,
    ) -> Result<(), crate::MetadataError> {
        self.inner.finish_prune(token).await
    }
    async fn claim_deletions(
        &self,
        revision: u64,
        resources: std::collections::BTreeSet<crate::metadata::PinResource>,
    ) -> Result<Option<crate::metadata::PinToken>, crate::MetadataError> {
        self.inner.claim_deletions(revision, resources).await
    }
    async fn finish_deletions(
        &self,
        token: &crate::metadata::PinToken,
    ) -> Result<(), crate::MetadataError> {
        self.inner.finish_deletions(token).await
    }
}

#[tokio::test]
async fn single_blob_waits_for_claim_without_partial_protection() {
    use crate::metadata::{DataPin, DataPinLease, PinResource, PinScope, PinStore};
    use std::collections::BTreeSet;
    let packed = [16385, 32768, 32769]
        .into_iter()
        .flat_map(|size| (0..3).map(move |kind| ("packed", 65536, size, kind)));
    let loose = [0, 2, 3].into_iter().map(|kind| ("loose", 1024, 19, kind));
    for (layout, average, size, kind, cancel) in
        packed
            .chain(loose)
            .flat_map(|(layout, average, size, kind)| {
                [false, true].map(move |cancel| (layout, average, size, kind, cancel))
            })
    {
        let ledger = Arc::new(RefusalCountingLedger::default());
        let objects = Arc::new(object_store::memory::InMemory::new());
        let store = small_blob_pin_store(objects.clone(), layout, average).await;
        let bytes = vec![42u8; size];
        let blob = BlobId::new(Digest::hash(&bytes));
        let chunk = PinResource::Chunk(single_chunk_id(blob));
        let path = chunk_path(&Path::from("blobs"), &single_chunk_id(blob));
        let resource = match kind {
            0 => chunk.clone(),
            1 => PinResource::StorageObject(
                sharded_path(&Path::from("blobs"), "bao", blob.as_digest()).to_string(),
            ),
            2 => PinResource::Blob(blob),
            3 => PinResource::StorageObject(path.to_string()),
            _ => unreachable!(),
        };
        let claim = ledger
            .claim_deletions(0, BTreeSet::from([resource.clone()]))
            .await
            .unwrap()
            .unwrap();
        let pin = DataPinLease::acquire(
            ledger.clone(),
            DataPin {
                scope: PinScope::Staging,
                catalog: None,
                resources: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
        let token = pin.token().clone();
        let batch = store.begin_pinned_batch(pin).unwrap();
        let writing = tokio::spawn({
            let store = store.clone();
            async move { write_blob(&store, &bytes).await }
        });
        // The writer has been refused twice, so it is retrying against the
        // claim rather than still on its way to the first attempt.
        ledger.refused_at_least(2).await;
        assert!(!writing.is_finished());
        assert!(matches!(
            objects.head(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert!(
            ledger.inventory().await.unwrap().pins[&token]
                .resources
                .is_empty()
        );
        if cancel {
            writing.abort();
            assert!(writing.await.unwrap_err().is_cancelled());
        } else {
            ledger.finish_deletions(&claim).await.unwrap();
            assert_eq!(writing.await.unwrap(), blob);
            let inventory = ledger.inventory().await.unwrap();
            assert!(inventory.pins[&token].resources.contains(&chunk));
            assert!(inventory.pins[&token].resources.contains(&resource));
            assert!(
                inventory.pins[&token]
                    .resources
                    .contains(&PinResource::Blob(blob))
            );
        }
        if cancel {
            ledger.finish_deletions(&claim).await.unwrap();
        }
        drop(batch);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                crate::flush_repository_leases().await.unwrap();
                if ledger.inventory().await.unwrap().pins.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn corrupt_packed_outboard_does_not_fall_back_and_can_be_repaired() {
    let directory = tempfile::tempdir().unwrap();
    let repository = Repository::local(directory.path()).await.unwrap();
    let store = repository.payloads();
    let mutation = repository.mutation_session().await.unwrap();
    let mut staged = Vec::new();
    let mut ids = Vec::new();
    for byte in [37, 73] {
        let object = mutation.stage_blob(&vec![byte; 16385]).await.unwrap();
        ids.push(object.record().payload());
        staged.push(object);
    }
    mutation.publish_unrooted(staged).await.unwrap();
    drop(mutation);
    let outboards: Vec<_> = store.list_outboards().try_collect().await.unwrap();
    assert_eq!(outboards.len(), 2);
    let id = ids[0];
    let proof = store.get_outboard(&id).await.unwrap().unwrap();
    put_object(
        &store.object_store,
        &store.outboard_path(&id),
        proof.clone(),
        false,
    )
    .await
    .unwrap();
    let packs: Vec<_> = store
        .object_store
        .list(Some(&kind_prefix(&store.base_path, "bao-packs")))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(packs.len(), 1);
    put_object(
        &store.object_store,
        &packs[0].location,
        Bytes::from_static(b"corrupt"),
        false,
    )
    .await
    .unwrap();
    let error = store.get_outboard(&id).await.unwrap_err();
    assert!(crate::blob::is_integrity_error(&error), "{error}");
    // Healthy near payloads can regenerate damaged proof storage without a replica.
    let far = ChunkedBlobStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        Path::default(),
        1024,
    );
    let repairing = RepairingBlobStore::new(store.clone(), far);
    assert_eq!(
        repairing.verified_read(&id, 0, 16385).await.unwrap(),
        vec![37; 16385]
    );
    let repaired = repository.fsck_repair(None).await.unwrap();
    assert!(repaired.is_healthy(), "{repaired:#?}");
    assert_eq!(store.get_outboard(&id).await.unwrap().unwrap(), proof);
    for (id, byte) in ids.into_iter().zip([37, 73]) {
        assert_eq!(
            store.verified_read(&id, 0, 16385).await.unwrap(),
            vec![byte; 16385]
        );
    }
}

/// Two readers on one store, the first parked between reads, must both make
/// progress. Slicing a payload reads its bytes and a copy source's bytes from
/// the same store at once, and a large blob made that pair deadlock.
#[tokio::test]
async fn a_parked_reader_does_not_block_a_second_reader() {
    use tokio::io::AsyncReadExt as _;
    let dir = tempfile::tempdir().unwrap();
    let fs = object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let store = ChunkedBlobStore::new(Arc::new(fs), Path::default(), DEFAULT_AVG_CHUNK_SIZE);
    // Many chunks, so both readers stream rather than serving one inline.
    let mut data = vec![0u8; 24 << 20];
    for (index, byte) in data.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    let digest = write_blob(&store, &data).await;

    let mut parked = store.open_read(&digest).await.unwrap().unwrap();
    let mut head = vec![0u8; 4096];
    parked.read_exact(&mut head).await.unwrap();
    assert_eq!(head, data[..4096]);

    let second = async {
        let mut reader = store.open_read(&digest).await.unwrap().unwrap();
        let mut bytes = Vec::new();
        (&mut reader)
            .take(8 << 20)
            .read_to_end(&mut bytes)
            .await
            .unwrap();
        bytes
    };
    let bytes = tokio::time::timeout(std::time::Duration::from_secs(30), second)
        .await
        .expect("a second reader must not wait on a parked one");
    assert_eq!(bytes, data[..8 << 20]);

    // The parked reader still finishes its own stream afterwards.
    let mut rest = Vec::new();
    parked.read_to_end(&mut rest).await.unwrap();
    assert_eq!(rest.len(), data.len() - 4096);
}

/// A reader parked mid-blob keeps the compressed windows its read-ahead
/// already fetched, and every reader of a store charges one shared budget for
/// those windows. A second reader still has to make progress: it takes the
/// room that is left, or fetches a single chunk uncharged, rather than waiting
/// for buffers that only a poll of the parked reader would release.
///
/// Incompressible content is what makes a window charge its full size, which
/// is why rebuilt store paths reach this and repeating test patterns do not.
/// A regression hangs here rather than failing.
#[tokio::test]
async fn a_parked_reader_does_not_starve_a_second_reader_of_buffers() {
    use tokio::io::AsyncReadExt as _;
    fn noise(seed: u64, len: usize) -> Vec<u8> {
        let mut state = seed;
        let mut out = Vec::with_capacity(len + 8);
        while out.len() < len {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut v = state;
            v = (v ^ (v >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            v = (v ^ (v >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            out.extend_from_slice(&(v ^ (v >> 31)).to_le_bytes());
        }
        out.truncate(len);
        out
    }
    let dir = tempfile::tempdir().unwrap();
    let store =
        ChunkedBlobStore::local_packed_with_options(dir.path(), crate::PackOptions::local())
            .await
            .unwrap();
    let first = noise(1, 96 << 20);
    let second = noise(2, 96 << 20);
    let one = write_blob(&store, &first).await;
    let two = write_blob(&store, &second).await;

    let mut parked = store.open_read(&one).await.unwrap().unwrap();
    let mut head = vec![0u8; 8 << 20];
    parked.read_exact(&mut head).await.unwrap();
    assert_eq!(head, first[..8 << 20]);

    let mut reader = store.open_read(&two).await.unwrap().unwrap();
    let mut bytes = vec![0u8; 8 << 20];
    reader.read_exact(&mut bytes).await.unwrap();
    assert_eq!(bytes, second[..8 << 20]);

    // The parked reader still finishes its own stream afterwards.
    parked.read_to_end(&mut head).await.unwrap();
    assert_eq!(head.len(), first.len());
}
