use super::*;
use crate::blob::{BlobBatchGuard, BlobReader};
use crate::digest::BlobId;
use crate::format::{FormatLimits, FormatRegistry};
use crate::metadata::{BackendWriteScope, DataPinLease};
use crate::{MemoryBlobStore, MemoryMetadataStore};
use async_trait::async_trait;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::io::AsyncWrite;

struct Control {
    opened: AtomicUsize,
    live: AtomicUsize,
    peak: AtomicUsize,
    gates: Vec<Semaphore>,
    started: mpsc::UnboundedSender<usize>,
    fail_first: bool,
}

struct ControlledStore {
    inner: MemoryBlobStore,
    control: Arc<Control>,
}

struct ControlledWriter {
    inner: Box<dyn BlobWriter>,
    control: Arc<Control>,
    index: usize,
    done: Option<(BlobId, u64)>,
}

impl Drop for ControlledWriter {
    fn drop(&mut self) {
        self.control.live.fetch_sub(1, Ordering::SeqCst);
    }
}

impl AsyncWrite for ControlledWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[async_trait]
impl BlobWriter for ControlledWriter {
    async fn close(&mut self) -> Result<(BlobId, u64), crate::error::Error> {
        if let Some(done) = self.done {
            return Ok(done);
        }
        if let Some(gate) = self.control.gates.get(self.index) {
            self.control.started.send(self.index).unwrap();
            gate.acquire().await.unwrap().forget();
            if self.index == 0 && self.control.fail_first {
                return Err(io::Error::other("injected close failure").into());
            }
        }
        let done = self.inner.close().await?;
        self.done = Some(done);
        Ok(done)
    }
}

#[async_trait]
impl BlobStore for ControlledStore {
    fn publication(&self) -> crate::blob::PayloadPublication<'_> {
        self.inner.publication()
    }
    fn write_scope(&self) -> BackendWriteScope {
        self.inner.write_scope()
    }
    fn begin_pinned_batch(&self, pin: DataPinLease) -> Result<BlobBatchGuard, crate::error::Error> {
        self.inner.begin_pinned_batch(pin)
    }
    async fn has(&self, id: &BlobId) -> Result<bool, crate::error::Error> {
        self.inner.has(id).await
    }
    async fn open_read(
        &self,
        id: &BlobId,
    ) -> Result<Option<Box<dyn BlobReader>>, crate::error::Error> {
        self.inner.open_read(id).await
    }
    async fn open_write(&self) -> Box<dyn BlobWriter> {
        let index = self.control.opened.fetch_add(1, Ordering::SeqCst);
        let live = self.control.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.control.peak.fetch_max(live, Ordering::SeqCst);
        Box::new(ControlledWriter {
            inner: self.inner.open_write().await,
            control: self.control.clone(),
            index,
            done: None,
        })
    }
}

fn controlled(
    files: usize,
    fail_first: bool,
) -> (
    Repository<ControlledStore, MemoryMetadataStore>,
    Arc<Control>,
    mpsc::UnboundedReceiver<usize>,
) {
    let (started, receiver) = mpsc::unbounded_channel();
    let control = Arc::new(Control {
        opened: AtomicUsize::new(0),
        live: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
        gates: (0..files).map(|_| Semaphore::new(0)).collect(),
        started,
        fail_first,
    });
    let repository = Repository::with_formats(
        ControlledStore {
            inner: MemoryBlobStore::new(),
            control: control.clone(),
        },
        MemoryMetadataStore::new().unwrap(),
        FormatRegistry::builtin(),
        FormatLimits {
            max_batch_objects: 2,
            ..Default::default()
        },
    );
    (repository, control, receiver)
}

async fn next_started(receiver: &mut mpsc::UnboundedReceiver<usize>) -> usize {
    tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn finalizers_overlap_with_one_limit_for_active_and_queued_files() {
    for concurrency in [1, 2, 4, 16] {
        let count = concurrency + 1;
        let names: Vec<_> = (0..count).map(|i| format!("file-{i:03}")).collect();
        let files: Vec<_> = names
            .iter()
            .map(|n| (n.as_str(), n.as_bytes(), 0o644))
            .collect();
        let input = super::tests::archive(&files).await;
        let (repository, control, mut started) = controlled(count, false);
        let root = RootName::try_from("tar/parallel").unwrap();
        let task = tokio::spawn({
            let repository = repository.clone();
            let root = root.clone();
            async move {
                repository
                    .import_tar(
                        std::io::Cursor::new(input),
                        root,
                        TarImportLimits {
                            max_in_flight_files: concurrency,
                            ..Default::default()
                        },
                    )
                    .await
            }
        });
        let mut observed = BTreeSet::new();
        for _ in 0..concurrency {
            observed.insert(next_started(&mut started).await);
        }
        assert_eq!(observed, (0..concurrency).collect());
        assert_eq!(control.opened.load(Ordering::SeqCst), concurrency);
        assert_eq!(control.peak.load(Ordering::SeqCst), concurrency);
        assert!(
            repository
                .metadata()
                .snapshot()
                .await
                .unwrap()
                .root(&root)
                .await
                .unwrap()
                .is_none()
        );
        // Let the last admitted file finish before the first. Its released
        // slot must admit the next file without waiting for archive order.
        control.gates[concurrency - 1].add_permits(1);
        assert_eq!(next_started(&mut started).await, concurrency);
        for gate in &control.gates {
            gate.add_permits(1);
        }
        let report = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(report.files, count);
        assert_eq!(control.live.load(Ordering::SeqCst), 0);
        assert_eq!(control.peak.load(Ordering::SeqCst), concurrency);
        let expected = Directory::try_from_iter(names.iter().map(|name| {
            (
                PathComponent::try_from(name.as_str()).unwrap(),
                Node::File {
                    digest: BlobId::new(blake3::hash(name.as_bytes()).into()),
                    size: name.len() as u64,
                    executable: false,
                },
            )
        }))
        .unwrap();
        assert_eq!(report.root, ObjectKey::directory(expected.digest()));
    }
}

#[tokio::test]
async fn finalizer_failure_and_cancellation_drop_all_writers_without_publishing() {
    for cancel in [false, true] {
        let (repository, control, mut started) = controlled(3, !cancel);
        let input = super::tests::archive(&[
            ("a", b"one", 0o644),
            ("b", b"two", 0o644),
            ("c", b"three", 0o644),
        ])
        .await;
        let root = RootName::try_from("tar/failed").unwrap();
        let task = tokio::spawn({
            let repository = repository.clone();
            let root = root.clone();
            async move {
                repository
                    .import_tar(
                        std::io::Cursor::new(input),
                        root,
                        TarImportLimits {
                            max_in_flight_files: 2,
                            ..Default::default()
                        },
                    )
                    .await
            }
        });
        for _ in 0..2 {
            next_started(&mut started).await;
        }
        if cancel {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            control.gates[0].add_permits(1);
            let error = tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap_err();
            assert!(error.to_string().contains("injected close failure"));
        }
        assert_eq!(control.live.load(Ordering::SeqCst), 0);
        assert!(
            repository
                .metadata()
                .snapshot()
                .await
                .unwrap()
                .root(&root)
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn duplicate_paths_and_truncation_cancel_queued_files() {
    for duplicate in [true, false] {
        let (repository, control, _started) = controlled(2, false);
        let mut input = super::tests::archive(&[
            ("a", b"one", 0o644),
            (if duplicate { "a" } else { "b" }, b"second", 0o644),
        ])
        .await;
        if !duplicate {
            input.truncate(3 * 512 + 2);
        }
        let root = RootName::try_from("tar/invalid").unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            repository.import_tar(
                std::io::Cursor::new(input),
                root.clone(),
                TarImportLimits::default(),
            ),
        )
        .await
        .unwrap();
        if duplicate {
            assert!(matches!(result, Err(TarImportError::DuplicatePath)));
        } else {
            assert!(result.is_err());
        }
        assert_eq!(control.live.load(Ordering::SeqCst), 0);
        assert!(
            repository
                .metadata()
                .snapshot()
                .await
                .unwrap()
                .root(&root)
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn zero_or_oversized_concurrency_is_rejected_before_opening_writers() {
    for count in [0, usize::MAX] {
        let (repository, control, _) = controlled(0, false);
        let input = super::tests::archive(&[]).await;
        assert!(matches!(
            repository
                .import_tar(
                    std::io::Cursor::new(input),
                    RootName::try_from("tar/invalid").unwrap(),
                    TarImportLimits {
                        max_in_flight_files: count,
                        ..Default::default()
                    }
                )
                .await,
            Err(TarImportError::InvalidLimits(_))
        ));
        assert_eq!(control.opened.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn streaming_and_finalization_share_a_tiny_chunk_budget_without_deadlock() {
    use object_store::{memory::InMemory, path::Path};
    let bodies: Vec<_> = [65535, 65536, 65537, 131073]
        .into_iter()
        .enumerate()
        .map(|(index, size)| {
            let mut bytes = vec![0; size];
            blake3::Hasher::new()
                .update(&(index as u64).to_le_bytes())
                .finalize_xof()
                .fill(&mut bytes);
            bytes
        })
        .collect();
    let names = ["a", "b", "c", "d"];
    let files: Vec<_> = names
        .iter()
        .zip(&bodies)
        .map(|(name, body)| (*name, body.as_slice(), 0o644))
        .collect();
    let input = super::tests::archive(&files).await;
    let mut roots = Vec::new();
    for concurrency in [1, 4, 16] {
        let payloads =
            crate::ChunkedBlobStore::new(Arc::new(InMemory::new()), Path::default(), 1024)
                .with_chunk_memory_budget_bytes(1);
        let repository = Repository::with_formats(
            payloads,
            MemoryMetadataStore::new().unwrap(),
            FormatRegistry::builtin(),
            FormatLimits {
                max_batch_objects: 1,
                ..Default::default()
            },
        );
        let report = tokio::time::timeout(
            Duration::from_secs(15),
            repository.import_tar(
                std::io::Cursor::new(&input),
                RootName::try_from("tar/bounded").unwrap(),
                TarImportLimits {
                    max_in_flight_files: concurrency,
                    ..Default::default()
                },
            ),
        )
        .await
        .unwrap()
        .unwrap();
        for bytes in &bodies {
            let id = BlobId::new(blake3::hash(bytes).into());
            assert_eq!(
                repository
                    .payloads()
                    .read_to_vec(&id)
                    .await
                    .unwrap()
                    .unwrap(),
                *bytes
            );
        }
        roots.push(report.root);
    }
    assert!(roots.windows(2).all(|pair| pair[0] == pair[1]));
}

#[tokio::test]
async fn forward_hardlinks_keep_their_meaning() {
    use ::tar::{Builder, EntryType, Header};
    let mut builder = Builder::new(Vec::new());
    for (path, target) in [("copy", "alias"), ("alias", "file")] {
        let mut header = Header::new_ustar();
        header.set_entry_type(EntryType::Link);
        header.set_path(path).unwrap();
        header.set_link_name(target).unwrap();
        header.set_size(0);
        header.set_cksum();
        builder.append(&header, &[][..]).unwrap();
    }
    let mut header = Header::new_ustar();
    header.set_size(5);
    header.set_mode(0o755);
    builder
        .append_data(&mut header, "file", &b"hello"[..])
        .unwrap();

    let input = builder.into_inner().unwrap();
    let mut expected = Directory::new();
    for (path, bytes, executable) in [
        ("copy", b"hello".as_slice(), true),
        ("alias", b"hello".as_slice(), true),
        ("file", b"hello".as_slice(), true),
    ] {
        expected
            .add(
                PathComponent::try_from(path).unwrap(),
                Node::File {
                    digest: BlobId::new(blake3::hash(bytes).into()),
                    size: bytes.len() as u64,
                    executable,
                },
            )
            .unwrap();
    }
    for concurrency in [1, 2, 16] {
        let repository = Repository::memory().unwrap();
        let report = repository
            .import_tar(
                std::io::Cursor::new(&input),
                RootName::try_from("tar/forward-links").unwrap(),
                TarImportLimits {
                    max_in_flight_files: concurrency,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(report.root, ObjectKey::directory(expected.digest()));
        assert_eq!(
            (
                report.files,
                report.hardlinks,
                report.file_bytes,
                report.sparse_expansion_bytes
            ),
            (1, 2, 5, 0)
        );
    }
}

#[tokio::test]
async fn old_gnu_sparse_entries_are_rejected_without_publishing_a_root() {
    use ::tar::{Builder, EntryType, Header};
    let mut builder = Builder::new(Vec::new());
    // One stored byte after a 4096-byte hole, with no extraction fixture.
    let mut header = Header::new_gnu();
    header.set_entry_type(EntryType::GNUSparse);
    header.set_path("sparse").unwrap();
    header.set_mode(0o644);
    header.set_size(1);
    let gnu = header.as_gnu_mut().unwrap();
    gnu.realsize.copy_from_slice(b"00000010001\0");
    gnu.sparse[0].offset.copy_from_slice(b"00000010000\0");
    gnu.sparse[0].numbytes.copy_from_slice(b"00000000001\0");
    header.set_cksum();
    builder.append(&header, &b"x"[..]).unwrap();
    let input = builder.into_inner().unwrap();
    let repository = Repository::memory().unwrap();
    let name = RootName::try_from("tar/sparse").unwrap();
    let error = repository
        .import_tar(
            std::io::Cursor::new(input),
            name.clone(),
            TarImportLimits::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, TarImportError::Tar(_)), "{error}");
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
