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
use std::time::Instant;

#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    chunk_puts: AtomicUsize,
}
impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("counted chunk store")
    }
}
#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        path: &Path,
        bytes: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if path.as_ref().starts_with("chunks/") {
            self.chunk_puts.fetch_add(1, Ordering::SeqCst);
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

fn data(size: usize, periodic: bool) -> Vec<u8> {
    let mut state = 0x68cb78419077a345u64;
    let mut bytes = Vec::with_capacity(size);
    for i in 0..size {
        if periodic && i >= 65536 {
            bytes.push(bytes[i % 65536]);
        } else {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            bytes.push(state as u8);
        }
    }
    bytes
}

async fn exercise(
    size: usize,
    avg: u32,
    concurrency: usize,
    budget: usize,
    backend: &str,
    content: &str,
) {
    let directory = tempfile::tempdir().unwrap();
    let inner: Arc<dyn ObjectStore> = match backend {
        "memory" => Arc::new(object_store::memory::InMemory::new()),
        "local" => Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap(),
        ),
        _ => panic!("unknown backend"),
    };
    assert!(matches!(content, "random" | "periodic"));
    let objects = Arc::new(CountingStore {
        inner,
        chunk_puts: AtomicUsize::new(0),
    });
    let store = ChunkedBlobStore::new(objects.clone(), Path::default(), avg)
        .with_chunk_upload_concurrency(concurrency.try_into().unwrap())
        .with_chunk_memory_budget_bytes(budget);
    // Fixture generation and independent reference hashes stay outside timing.
    let bytes = data(size, content == "periodic");
    let expected_id = BlobId::new(Digest::hash(&bytes));
    let expected: Vec<_> = fastcdc::v2020::FastCDC::new(
        &bytes,
        ((avg as usize / 2).clamp(fastcdc::v2020::MINIMUM_MIN, fastcdc::v2020::MINIMUM_MAX)) & !1,
        avg as usize,
        (avg as usize * 2).clamp(fastcdc::v2020::MAXIMUM_MIN, fastcdc::v2020::MAXIMUM_MAX),
    )
    .map(|part| {
        (
            Digest::hash(&bytes[part.offset..part.offset + part.length]),
            part.length as u64,
        )
    })
    .collect();
    let mut reference = blake3::Hasher::new();
    for (digest, size) in &expected {
        reference.update(&digest.as_bytes()[..]);
        reference.update(&size.to_le_bytes());
    }
    let manifest_hash = reference.finalize().to_hex();
    for phase in ["cold", "duplicate"] {
        let before = objects.chunk_puts.load(Ordering::SeqCst);
        let begin = Instant::now();
        let id = store.put_slice(&bytes).await.unwrap();
        let elapsed = begin.elapsed();
        let chunk_puts = objects.chunk_puts.load(Ordering::SeqCst) - before;
        if phase == "duplicate" {
            assert_eq!(chunk_puts, 0, "duplicates must not write chunks");
        } else {
            assert!(chunk_puts > 0);
        }
        assert_eq!(id, expected_id);
        let chunks = store.chunks(&id).await.unwrap().unwrap();
        assert_eq!(
            chunks
                .iter()
                .map(|part| (part.digest.digest(), part.size))
                .collect::<Vec<_>>(),
            expected
        );
        let mut reader = store
            .open_verified(&id, size as u64)
            .await
            .unwrap()
            .unwrap();
        let mut actual = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut actual)
            .await
            .unwrap();
        assert_eq!(actual, bytes);
        println!(
            "chunk_hash_sample {{\"file_bytes\":{size},\"average_chunk_bytes\":{avg},\"upload_concurrency\":{concurrency},\"memory_budget_bytes\":{budget},\"backend\":\"{backend}\",\"content\":\"{content}\",\"phase\":\"{phase}\",\"wall_nanos\":{},\"root\":\"{id}\",\"manifest_hash\":\"{manifest_hash}\",\"chunks\":{},\"chunk_puts\":{chunk_puts},\"correctness\":\"independent BLAKE3 and FastCDC, exhaustive verified readback, duplicate chunk-write count\"}}",
            elapsed.as_nanos(),
            expected.len()
        );
    }
}

#[tokio::test]
async fn hash_batch_probe_audits_small_default_and_many_chunk_paths() {
    for backend in ["memory", "local"] {
        for (size, avg) in [
            (255, 1024),
            (65536, 1024),
            (1048576, 262144),
            (2097152, 524286),
            (2097152, 524290),
        ] {
            exercise(size, avg, 4, 1048576, backend, "random").await;
        }
    }
}

#[tokio::test]
#[ignore = "run through benchmark run chunk-hash-batch"]
async fn benchmark_chunk_hash_batch() {
    let value = |name: &str, default: usize| {
        std::env::var(name)
            .map(|s| s.parse().unwrap())
            .unwrap_or(default)
    };
    let backend = std::env::var("CASITA_HASH_BACKEND").unwrap_or("memory".into());
    let content = std::env::var("CASITA_HASH_CONTENT").unwrap_or("random".into());
    exercise(
        value("CASITA_HASH_BYTES", 4 * 1048576),
        value("CASITA_HASH_AVERAGE", 262144) as u32,
        value("CASITA_HASH_CONCURRENCY", 4),
        value("CASITA_HASH_BUDGET", 4 * 1048576),
        &backend,
        &content,
    )
    .await;
}
