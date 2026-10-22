//! Full-byte verification under delayed storage, with bounded outstanding reads.
use super::*;
use crate::{ChunkedBlobStore, MemoryMetadataStore};
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{path::Path, *};
use std::{
    fmt,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
#[derive(Debug)]
struct Delayed {
    inner: Arc<dyn ObjectStore>,

    active: AtomicUsize,
    peak: AtomicUsize,
    completed: Mutex<Vec<Path>>,
    delay_ms: u64,
}
impl fmt::Display for Delayed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Delayed")
    }
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl ObjectStore for Delayed {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(path, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        let _guard = Active(&self.active);
        self.peak.fetch_max(active, Ordering::SeqCst);
        if self.delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
        }
        let result = self.inner.get_opts(path, options).await;
        self.completed.lock().unwrap().push(path.clone());
        result
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
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
    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

async fn fixture(
    count: usize,
    delay_ms: u64,
) -> (
    Repository<ChunkedBlobStore, MemoryMetadataStore>,
    Arc<Delayed>,
) {
    let source = tempfile::tempdir().unwrap();
    for index in 0..count {
        let mut bytes = format!("synthetic physical fsck payload {index}\n").into_bytes();
        // Cross the Bao proof threshold as well as exercising small bare chunks.
        if index % 4 == 0 {
            bytes.resize(16_385, b'x');
        }
        std::fs::write(source.path().join(format!("file-{index:04}")), bytes).unwrap();
    }
    let delayed = Arc::new(Delayed {
        inner: Arc::new(object_store::memory::InMemory::new()),
        active: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
        completed: Mutex::new(Vec::new()),
        delay_ms,
    });
    let payloads = ChunkedBlobStore::packed_with_options(
        delayed.clone(),
        Path::default(),
        crate::DEFAULT_AVG_CHUNK_SIZE,
        crate::PackOptions {
            target_size: 16 * 1024,
            cache_capacity: 0,
        },
    )
    .await
    .unwrap();
    let repository = Repository::new(payloads, MemoryMetadataStore::new().unwrap());
    repository
        .import(crate::import::FilesystemImport::new(
            source.path(),
            "bench/retained".parse().unwrap(),
        ))
        .await
        .unwrap();
    delayed.peak.store(0, Ordering::SeqCst);
    delayed.completed.lock().unwrap().clear();
    (repository, delayed)
}

#[tokio::test(start_paused = true)]
async fn physical_fsck_overlaps_delayed_reads_and_still_checks_every_payload() {
    // Below, at, and above the intended eight-read bound, plus a larger tree.
    for files in [6, 7, 8, 9, 166] {
        let (repository, objects) = fixture(files, 1).await;
        let report = repository.preview_fsck_repair(None).await.unwrap();
        assert!(report.is_healthy(), "{report:?}");
        assert_eq!(report.payloads_checked, files + 1);
        assert!(report.bytes_checked > files as u64 * 30);
        assert!(report.outboards_checked >= files.div_ceil(4));
        let peak = objects.peak.load(Ordering::SeqCst);
        assert!(
            peak > 1,
            "serial physical reads accumulate storage latency: peak={peak}"
        );
        assert!(
            peak <= 8,
            "physical verification must remain bounded: peak={peak}"
        );
        assert_eq!(objects.active.load(Ordering::SeqCst), 0);
        assert!(repository.fsck().await.unwrap().is_clean());
    }
}

#[tokio::test(start_paused = true)]
async fn physical_fsck_rejects_corrupt_and_missing_payloads_with_delayed_reads() {
    for missing in [false, true] {
        let (repository, objects) = fixture(16, 1).await;
        let mut packs = objects.inner.list(Some(&Path::from("packs")));
        let pack = packs.next().await.unwrap().unwrap().location;
        drop(packs);
        if missing {
            objects.inner.delete(&pack).await.unwrap();
        } else {
            objects
                .inner
                .put(
                    &pack,
                    bytes::Bytes::from_static(b"SYNTHETIC CORRUPTION").into(),
                )
                .await
                .unwrap();
        }
        let report = repository.preview_fsck_repair(None).await.unwrap();
        assert!(!report.is_healthy(), "damaged physical history accepted");
        assert_eq!(report.payloads_checked, 17);
        assert!(!report.findings.is_empty());
        assert_eq!(objects.active.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
#[ignore = "permanent physical-fsck benchmark corpus"]
async fn benchmark_physical_fsck_latency() {
    let files: usize = std::env::var("CASITA_PHYSICAL_FSCK_FILES")
        .unwrap()
        .parse()
        .unwrap();
    let delay_ms: u64 = std::env::var("CASITA_PHYSICAL_FSCK_DELAY_MS")
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=10_000).contains(&files) && delay_ms <= 10);
    let (repository, objects) = fixture(files, delay_ms).await;
    let started = std::time::Instant::now();
    let report = repository.preview_fsck_repair(None).await.unwrap();
    let nanos = started.elapsed().as_nanos();
    assert!(report.is_healthy(), "{report:?}");
    assert_eq!(report.payloads_checked, files + 1);
    assert!(report.bytes_checked > files as u64 * 30);
    assert!(report.outboards_checked >= files.div_ceil(4));
    assert!(repository.fsck().await.unwrap().is_clean());
    let peak = objects.peak.load(Ordering::SeqCst);
    assert_eq!(objects.active.load(Ordering::SeqCst), 0);
    // Full verification must still reject byte damage after the healthy scan.
    let mut packs = objects.inner.list(Some(&Path::from("packs")));
    let pack = packs.next().await.unwrap().unwrap().location;
    drop(packs);
    objects
        .inner
        .put(
            &pack,
            bytes::Bytes::from_static(b"SYNTHETIC CORRUPTION").into(),
        )
        .await
        .unwrap();
    assert!(
        !repository
            .preview_fsck_repair(None)
            .await
            .unwrap()
            .is_healthy()
    );
    println!(
        "physical_fsck_sample {}",
        serde_json::json!({
            "files": files, "delay_ms": delay_ms, "nanos": nanos,
            "payloads_checked": report.payloads_checked, "bytes_checked": report.bytes_checked,
        "outboards_checked": report.outboards_checked,
            "peak_storage_reads": peak, "full_logical_clean": true, "corruption_rejected": true,
        })
    );
}
