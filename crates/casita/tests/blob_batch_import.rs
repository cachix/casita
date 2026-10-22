//! Atomic blob intake through the stable application import interface.
#![cfg(feature = "native")]

use casita::{ErrorKind, Repository, RootName, import::BlobImport};
use std::{
    future::Future,
    io::Cursor,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

fn name(value: &str) -> RootName {
    value.parse().unwrap()
}

struct FailingReader(Arc<AtomicUsize>);
impl AsyncRead for FailingReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(Err(std::io::Error::other("input failed")))
    }
}

struct ReadActivity {
    active: AtomicUsize,
    peak: AtomicUsize,
    entered: tokio::sync::Semaphore,
}

impl Default for ReadActivity {
    fn default() -> Self {
        Self {
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            entered: tokio::sync::Semaphore::new(0),
        }
    }
}

struct GatedReader {
    activity: Arc<ReadActivity>,
    wait: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    bytes: Cursor<Vec<u8>>,
    active: bool,
}

impl GatedReader {
    fn new(
        index: u8,
        activity: Arc<ReadActivity>,
        mut gate: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        Self {
            activity,
            wait: Some(Box::pin(async move {
                while !*gate.borrow_and_update() {
                    gate.changed().await.unwrap();
                }
                // The first input reads later than its siblings, so report
                // ordering is checked independently of reader completion.
                if index == 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            })),
            bytes: Cursor::new(vec![index]),
            active: false,
        }
    }
}

impl AsyncRead for GatedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.wait.is_some() {
            if !self.active {
                self.active = true;
                let active = self.activity.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.activity.peak.fetch_max(active, Ordering::SeqCst);
                self.activity.entered.add_permits(1);
            }
            if self.wait.as_mut().unwrap().as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            self.wait = None;
            self.active = false;
            self.activity.active.fetch_sub(1, Ordering::SeqCst);
        }
        Pin::new(&mut self.bytes).poll_read(cx, buffer)
    }
}

impl Drop for GatedReader {
    fn drop(&mut self) {
        if self.active {
            self.activity.active.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

#[tokio::test]
async fn atomic_blob_batch_bounds_concurrent_readers_and_preserves_order() {
    let repository = Repository::memory().unwrap();
    let activity = Arc::new(ReadActivity::default());
    let (gate, receiver) = tokio::sync::watch::channel(false);
    let inputs = (0..33)
        .map(|index| {
            BlobImport::new(
                GatedReader::new(index, activity.clone(), receiver.clone()),
                name(&format!("root/{index}")),
            )
        })
        .collect::<Vec<_>>();
    let mut import = Box::pin(repository.import(BlobImport::batch(inputs)));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::select! {
            result = &mut import => panic!("readers passed a closed gate: {result:?}"),
            permits = activity.entered.acquire_many(16) => permits.unwrap().forget(),
        }
    })
    .await
    .expect("one full group of readers must start concurrently");
    assert_eq!(activity.active.load(Ordering::SeqCst), 16);
    gate.send(true).unwrap();
    let keys = import.await.unwrap();
    assert_eq!(activity.peak.load(Ordering::SeqCst), 16);
    assert_eq!(activity.active.load(Ordering::SeqCst), 0);
    assert_eq!(keys.len(), 33);
    for (index, key) in keys.into_iter().enumerate() {
        assert_eq!(
            repository
                .root(&name(&format!("root/{index}")))
                .await
                .unwrap(),
            Some(key.clone())
        );
        let mut reader = repository.open_verified(&key).await.unwrap().unwrap();
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, [index as u8]);
    }
    assert!(repository.fsck().await.unwrap().is_clean());
}

#[tokio::test]
async fn cancelling_atomic_blob_batch_drops_readers_and_leaves_no_roots() {
    let directory = tempfile::tempdir().unwrap();
    let repository = Repository::local(directory.path()).await.unwrap();
    let activity = Arc::new(ReadActivity::default());
    let (_gate, receiver) = tokio::sync::watch::channel(false);
    let inputs = (0..33)
        .map(|index| {
            BlobImport::new(
                GatedReader::new(index, activity.clone(), receiver.clone()),
                name(&format!("root/{index}")),
            )
        })
        .collect::<Vec<_>>();
    let mut import = Box::pin(repository.import(BlobImport::batch(inputs)));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::select! {
            result = &mut import => panic!("readers passed a closed gate: {result:?}"),
            permits = activity.entered.acquire_many(16) => permits.unwrap().forget(),
        }
    })
    .await
    .unwrap();
    drop(import);
    assert_eq!(activity.active.load(Ordering::SeqCst), 0);
    assert!(repository.roots().await.unwrap().is_empty());
    repository.flush().await.unwrap();
    repository.collect().await.unwrap();
    assert!(repository.fsck().await.unwrap().is_clean());
}

#[tokio::test]
async fn atomic_blob_batch_publishes_one_generation_and_ordered_reports() {
    let directory = tempfile::tempdir().unwrap();
    for repository in [
        Repository::memory().unwrap(),
        Repository::local(directory.path()).await.unwrap(),
    ] {
        let before = repository
            .metadata_reader()
            .await
            .unwrap()
            .generation()
            .unwrap()
            .get();
        let inputs = vec![
            BlobImport::new(Cursor::new(b"first".to_vec()), name("z")),
            BlobImport::new(Cursor::new(b"second".to_vec()), name("a")),
            BlobImport::new(Cursor::new(b"first".to_vec()), name("duplicate-content")),
        ];
        let future = repository.import(BlobImport::batch(inputs));
        fn send<T: Send>(value: T) -> T {
            value
        }
        let keys = send(future).await.unwrap();
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], keys[2]);
        assert_ne!(keys[0], keys[1]);
        let reader = repository.metadata_reader().await.unwrap();
        assert_eq!(reader.generation().unwrap().get(), before + 1);
        for (root, key) in ["z", "a", "duplicate-content"].iter().zip(&keys) {
            assert_eq!(reader.root(&name(root)).await.unwrap(), Some(key.clone()));
        }
        drop(reader);
        for (key, expected) in keys.iter().zip([b"first".as_slice(), b"second", b"first"]) {
            let mut reader = repository.open_verified(key).await.unwrap().unwrap();
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, expected);
        }
        assert!(repository.fsck().await.unwrap().is_clean());
    }
}

#[tokio::test]
async fn failed_atomic_blob_batch_keeps_every_root_and_revision_unchanged() {
    let directory = tempfile::tempdir().unwrap();
    let repository = Repository::local(directory.path()).await.unwrap();
    let original = repository
        .import(BlobImport::new(Cursor::new(b"old"), name("existing")))
        .await
        .unwrap();
    let revision = repository.metadata_reader().await.unwrap().revision();
    let reads = Arc::new(AtomicUsize::new(0));
    type Reader = Box<dyn AsyncRead + Unpin + Send>;
    let inputs: Vec<BlobImport<Reader>> = vec![
        BlobImport::new(Box::new(Cursor::new(b"replacement")), name("existing")),
        BlobImport::new(Box::new(FailingReader(reads.clone())), name("absent")),
    ];
    assert!(repository.import(BlobImport::batch(inputs)).await.is_err());
    assert_eq!(reads.load(Ordering::Relaxed), 1);
    assert_eq!(
        repository.root(&name("existing")).await.unwrap(),
        Some(original)
    );
    assert_eq!(repository.root(&name("absent")).await.unwrap(), None);
    assert_eq!(
        repository.metadata_reader().await.unwrap().revision(),
        revision
    );
    repository.flush().await.unwrap();
    repository.collect().await.unwrap();
    assert!(repository.fsck().await.unwrap().is_clean());
}

#[tokio::test]
async fn atomic_blob_batch_validates_names_and_empty_batches_before_reads() {
    let repository = Repository::memory().unwrap();
    let revision = repository.metadata_reader().await.unwrap().revision();
    let reads = Arc::new(AtomicUsize::new(0));
    let error = repository
        .import(BlobImport::batch(vec![
            BlobImport::new(FailingReader(reads.clone()), name("same")),
            BlobImport::new(FailingReader(reads.clone()), name("same")),
        ]))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(reads.load(Ordering::Relaxed), 0);
    let empty: Vec<BlobImport<FailingReader>> = Vec::new();
    assert!(
        repository
            .import(BlobImport::batch(empty))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        repository.metadata_reader().await.unwrap().revision(),
        revision
    );
}

#[cfg(feature = "experimental")]
#[tokio::test]
async fn atomic_blob_batch_obeys_the_configured_count_limit() {
    use casita::experimental::{
        FormatLimits, FormatRegistry, MemoryBlobStore, MemoryMetadataStore, Repository,
        RepositoryError,
    };
    for limits in [
        FormatLimits {
            max_batch_objects: 2,
            ..FormatLimits::default()
        },
        FormatLimits {
            max_root_changes: 2,
            ..FormatLimits::default()
        },
    ] {
        let repository = Repository::with_formats(
            MemoryBlobStore::new(),
            MemoryMetadataStore::new().unwrap(),
            FormatRegistry::builtin(),
            limits,
        );
        let inputs = (0..2)
            .map(|index| BlobImport::new(Cursor::new(vec![index]), name(&format!("root/{index}"))))
            .collect::<Vec<_>>();
        assert_eq!(
            repository
                .import(BlobImport::batch(inputs))
                .await
                .unwrap()
                .len(),
            2
        );
        let reads = Arc::new(AtomicUsize::new(0));
        let too_many = (0..3)
            .map(|index| {
                BlobImport::new(
                    FailingReader(reads.clone()),
                    name(&format!("overflow/{index}")),
                )
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            repository
                .import(BlobImport::batch(too_many))
                .await
                .unwrap_err(),
            RepositoryError::LimitExceeded(_)
        ));
        assert_eq!(reads.load(Ordering::Relaxed), 0);
    }
}
