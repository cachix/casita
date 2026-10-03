#![cfg(all(feature = "native", feature = "experimental"))]

use async_trait::async_trait;
use casita::experimental::{BlobStore, ChunkedBlobStore};
use casita::{BlobId, Digest};
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

#[derive(Debug)]
struct DelayedStore {
    inner: object_store::memory::InMemory,
    started: AtomicUsize,
    completed: AtomicUsize,
    released: Notify,
    barrier: Option<(usize, String)>,
    delay_ms: u64,
}
impl std::fmt::Display for DelayedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("delayed upload test store")
    }
}
#[async_trait]
impl ObjectStore for DelayedStore {
    async fn put_opts(
        &self,
        path: &Path,
        bytes: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if path.as_ref().starts_with("chunks/") {
            let index = self.started.fetch_add(1, Ordering::SeqCst);
            if let Some((window, first_chunk)) = &self.barrier {
                if path.as_ref().ends_with(first_chunk) {
                    self.released.notified().await;
                } else if index >= *window {
                    self.released.notify_one();
                }
            }
            if self.delay_ms != 0 {
                // One straggler in each pair of four-upload windows.
                tokio::time::sleep(Duration::from_millis(if index.is_multiple_of(8) {
                    self.delay_ms
                } else {
                    1
                }))
                .await;
            }
            let result = self.inner.put_opts(path, bytes, options).await;
            self.completed.fetch_add(1, Ordering::SeqCst);
            return result;
        }
        self.inner.put_opts(path, bytes, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, options).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
fn data(size: usize) -> Vec<u8> {
    let mut state = 0x13eb73a922cb4907u64;
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}
fn store(
    barrier: Option<usize>,
    delay_ms: u64,
    budget: usize,
    concurrency: usize,
) -> (ChunkedBlobStore, Arc<DelayedStore>) {
    let backend = Arc::new(DelayedStore {
        inner: Default::default(),
        started: AtomicUsize::new(0),
        completed: AtomicUsize::new(0),
        released: Notify::new(),
        barrier: barrier.map(|window| {
            let bytes = data(64 * 1024);
            let first = fastcdc::v2020::FastCDC::new(&bytes, 512, 1024, 2048)
                .next()
                .unwrap();
            (window, Digest::hash(&bytes[..first.length]).to_hex())
        }),
        delay_ms,
    });
    (
        ChunkedBlobStore::new(backend.clone(), Path::default(), 1024)
            .with_chunk_upload_concurrency(concurrency.try_into().unwrap())
            .with_chunk_memory_budget_bytes(budget),
        backend,
    )
}
async fn audit(store: &ChunkedBlobStore, digest: BlobId, body: &[u8]) {
    assert_eq!(digest, BlobId::new(Digest::hash(body)));
    let expected: Vec<_> = fastcdc::v2020::FastCDC::new(body, 512, 1024, 2048)
        .map(|part| {
            (
                Digest::hash(&body[part.offset..part.offset + part.length]),
                part.length as u64,
            )
        })
        .collect();
    let actual = store.chunks(&digest).await.unwrap().unwrap();
    assert_eq!(
        actual
            .iter()
            .map(|part| (part.digest.digest(), part.size))
            .collect::<Vec<_>>(),
        expected
    );
    let mut reader = store
        .open_verified(&digest, body.len() as u64)
        .await
        .unwrap()
        .unwrap();
    let mut bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, body);
}

#[tokio::test]
async fn completed_uploads_allow_new_work_past_an_earlier_straggler() {
    let (store, backend) = store(Some(4), 0, 1024 * 1024, 4);
    let body = data(64 * 1024);
    let digest = tokio::time::timeout(Duration::from_secs(2), store.put_slice(&body))
        .await
        .expect("an earlier pending chunk blocked new work after other uploads completed")
        .unwrap();
    assert!(backend.started.load(Ordering::SeqCst) > 4);
    assert_eq!(
        backend.started.load(Ordering::SeqCst),
        backend.completed.load(Ordering::SeqCst)
    );
    audit(&store, digest, &body).await;
}

#[tokio::test]
async fn completion_order_preserves_fastcdc_manifests_and_verified_bytes() {
    for size in [0, 1, 511, 512, 513, 2047, 2048, 2049, 65536, 1048576] {
        for budget in [65535, 65536, 65537, 196607, 196608, 196609, 1048576] {
            let (store, _) = store(None, 0, budget, 4);
            let body = data(size);
            let digest = tokio::time::timeout(Duration::from_secs(5), store.put_slice(&body))
                .await
                .unwrap()
                .unwrap();
            audit(&store, digest, &body).await;
        }
    }
}

#[tokio::test]
#[ignore = "run through benchmark run chunk-upload-completion"]
async fn benchmark_chunk_upload_completion() {
    let env = |name: &str, default: usize| {
        std::env::var(name)
            .ok()
            .map(|value| value.parse().unwrap())
            .unwrap_or(default)
    };
    let size = env("CASITA_CHUNK_COMPLETION_BYTES", 65536);
    let budget = env("CASITA_CHUNK_COMPLETION_BUDGET", 1048576);
    let delay = env("CASITA_CHUNK_COMPLETION_DELAY_MS", 0);
    let concurrency = env("CASITA_CHUNK_COMPLETION_CONCURRENCY", 4);
    let (store, backend) = store(None, delay as u64, budget, concurrency);
    let body = data(size);
    let begin = Instant::now();
    let digest = store.put_slice(&body).await.unwrap();
    let elapsed = begin.elapsed();
    audit(&store, digest, &body).await;
    assert_eq!(
        backend.started.load(Ordering::SeqCst),
        backend.completed.load(Ordering::SeqCst)
    );
    println!(
        "chunk_upload_completion_sample {{\"file_bytes\":{size},\"budget\":{budget},\"delay_ms\":{delay},\"concurrency\":{concurrency},\"wall_nanos\":{},\"root\":\"{digest}\",\"correctness\":\"reference FastCDC chunks and verified full readback; all uploads completed\"}}",
        elapsed.as_nanos()
    );
}

#[tokio::test]
async fn stalled_first_upload_bounds_completed_manifest_metadata() {
    // A writer buffers 16 upload windows of completed metadata behind a
    // straggler, never fewer than one 64-entry manifest page. Two uploads
    // fall below that floor, four meet it, and 32 scale past it. A 4 MiB
    // budget admits 64 units.
    for (concurrency, limit) in [(2, 64), (4, 64), (32, 512)] {
        let (store, backend) = store(Some(usize::MAX), 0, 4 * 1024 * 1024, concurrency);
        let body = data(1024 * 1024);
        let write = store.put_slice(&body);
        tokio::pin!(write);
        tokio::select! {
            _ = &mut write => panic!("the first source chunk must remain parked"),
            _ = async {
                tokio::time::timeout(Duration::from_secs(5), async {
                    while backend.started.load(Ordering::SeqCst) <= limit {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .expect("completion-order admission stopped before filling its window");
                // Give an unbounded writer time to overrun the window.
                tokio::time::sleep(Duration::from_millis(100)).await;
            } => {}
        }
        let started = backend.started.load(Ordering::SeqCst);
        backend.released.notify_one();
        // The straggler plus the completed entries that reached the limit,
        // and at most one more window that was already in flight.
        assert!(
            (limit + 1..=limit + concurrency).contains(&started),
            "a straggler with {concurrency} uploads admitted {started}, outside the window \
             of {limit} completed entries"
        );
        let digest = tokio::time::timeout(Duration::from_secs(5), &mut write)
            .await
            .unwrap()
            .unwrap();
        audit(&store, digest, &body).await;
    }
}

#[tokio::test]
async fn a_writer_stalled_behind_a_straggler_leaves_the_budget_to_other_writers() {
    // Two admission units: the parked first upload holds one. The stalled
    // writer cannot use the other until the straggler completes, so it must
    // not keep a queued reservation for it either.
    let (store, backend) = store(Some(usize::MAX), 0, 2 * 64 * 1024, 4);
    let body = data(1024 * 1024);
    let small = data(100);
    let write = store.put_slice(&body);
    tokio::pin!(write);
    tokio::select! {
        _ = &mut write => panic!("the first source chunk must remain parked"),
        _ = async {
            tokio::time::timeout(Duration::from_secs(5), async {
                while backend.started.load(Ordering::SeqCst) <= 64 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("completion-order admission stopped before filling its window");
            // Let the writer settle into waiting on its straggler.
            tokio::time::sleep(Duration::from_millis(100)).await;
            let other = tokio::time::timeout(Duration::from_secs(5), store.put_slice(&small))
                .await
                .expect("a stalled writer held budget another writer needed")
                .unwrap();
            assert_eq!(other, BlobId::new(Digest::hash(&small)));
        } => {}
    }
    backend.released.notify_one();
    let digest = tokio::time::timeout(Duration::from_secs(5), &mut write)
        .await
        .unwrap()
        .unwrap();
    audit(&store, digest, &body).await;
}

#[test]
fn paused_source_still_drives_admitted_chunk_uploads() {
    use futures::TryStreamExt;
    use object_store::ObjectStore;
    use tokio::io::AsyncWriteExt;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let objects = Arc::new(object_store::memory::InMemory::new());
        let store = ChunkedBlobStore::new(objects.clone(), Path::default(), 1024)
            .with_chunk_upload_concurrency(16.try_into().unwrap())
            .with_chunk_memory_budget_bytes(1024 * 1024);
        let bytes: Vec<u8> = (0..2048).map(|i| (i * 71) as u8).collect();
        let mut writer = store.open_write().await;
        writer.write_all(&bytes).await.unwrap();
        // Leave the source open below the concurrency limit. Drive
        // flush while a producer waits for an earlier chunk to reach storage.
        let uploaded = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                writer.flush().await.unwrap();
                if !objects
                    .list(Some(&Path::from("chunks")))
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap()
                    .is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            uploaded.is_ok(),
            "paused input must not strand already-admitted chunk work"
        );
        let (id, size) = writer.close().await.unwrap();
        assert_eq!(id, BlobId::new(blake3::hash(&bytes).into()));
        assert_eq!(size, bytes.len() as u64);
    });
}
