#![cfg(all(feature = "native", feature = "git", feature = "experimental"))]

use casita::experimental::{
    FormatError, FormatLimits, FormatRegistry, GitObjectFormat, GitObjectKind, MemoryBlobStore,
    MemoryMetadataStore, MetadataStore, ObjectFormat, Repository, RepositoryError,
    VerificationContext, VerifiedObject, git_object_key_for_body,
};
use casita::{BlobId, Digest, NamespaceId, ObjectKey};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[path = "support/counting_blob_store.rs"]
mod counting_blob_store;
use counting_blob_store::CountingBlobStore;

/// Whether `error` is a source that ran past its declared length.
fn excess_bytes(error: &RepositoryError) -> bool {
    matches!(
        error,
        RepositoryError::Format(FormatError::Io(error))
            if error.kind() == std::io::ErrorKind::InvalidData
    )
}

/// Whether `error` came from the injected source or writer failure `message`.
fn injected(error: &RepositoryError, message: &str) -> bool {
    matches!(error, RepositoryError::Format(FormatError::Io(error)) if error.to_string() == message)
}

fn counted() -> (
    Repository<CountingBlobStore, MemoryMetadataStore>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let payloads = CountingBlobStore::new();
    let (reads, writes) = (payloads.reads.clone(), payloads.writes.clone());
    (
        Repository::new(payloads, MemoryMetadataStore::new().unwrap()),
        reads,
        writes,
    )
}

#[tokio::test]
async fn verified_stream_does_not_reread_written_payloads() {
    for format in [GitObjectFormat::Sha1, GitObjectFormat::Sha256] {
        for size in [0, 1, 65535, 65536, 65537, 1048576] {
            let (repository, reads, writes) = counted();
            let body = vec![42; size];
            let key = git_object_key_for_body(format, GitObjectKind::Blob, &body).unwrap();
            let session = repository.mutation_session().await.unwrap();
            let staged = session
                .stage_object_reader_with_size(
                    key.clone(),
                    size as u64,
                    &mut std::io::Cursor::new(&body),
                )
                .await
                .unwrap();
            assert_eq!(staged.record().key(), &key);
            assert_eq!(staged.record().payload(), BlobId::new(Digest::hash(&body)));
            assert_eq!(staged.record().payload_size(), size as u64);
            assert_eq!(reads.load(Ordering::SeqCst), 0);
            assert_eq!(writes.load(Ordering::SeqCst), 1);
            session.publish_unrooted(vec![staged]).await.unwrap();
            let (_, mut reader) = repository.open_payload(&key).await.unwrap().unwrap();
            let mut actual = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut actual)
                .await
                .unwrap();
            assert_eq!(actual, body);
        }
    }
}

#[tokio::test]
async fn verified_stream_checks_length_and_native_identity() {
    let (repository, reads, _) = counted();
    let key = git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Blob, b"body").unwrap();
    let session = repository.mutation_session().await.unwrap();
    let stage = |key: &ObjectKey, length, bytes: &'static [u8]| {
        let key = key.clone();
        let session = &session;
        async move {
            session
                .stage_object_reader_with_size(key, length, &mut std::io::Cursor::new(bytes))
                .await
                .expect_err("a mismatched stream must not stage")
        }
    };
    // The Git verifier checks the declared length and the OID itself.
    for (length, bytes, message) in [
        (
            4,
            b"bod".as_slice(),
            "declared 4 payload bytes but yielded 3",
        ),
        (5, b"body", "declared 5 payload bytes but yielded 4"),
        (4, b"BODY", "native Git OID mismatch"),
    ] {
        let error = stage(&key, length, bytes).await;
        assert!(
            matches!(&error, RepositoryError::Format(FormatError::InvalidPayload { message: actual, .. })
                if actual.contains(message)),
            "{length} declared bytes of {bytes:?}: {error:?}"
        );
    }
    for (length, bytes) in [(4, b"body!".as_slice()), (3, b"body")] {
        let error = stage(&key, length, bytes).await;
        assert!(excess_bytes(&error), "{length} declared bytes: {error:?}");
    }
    // A raw blob's verifier only hashes what it reads, so the stream itself
    // must refuse a source whose length differs from the declaration, even
    // when the key names exactly the bytes that arrived.
    for (body, length) in [(b"bod".as_slice(), 4), (b"body", 5)] {
        let raw = ObjectKey::blob(BlobId::new(Digest::hash(body)));
        let error = stage(&raw, length, body).await;
        assert!(
            matches!(
                error,
                RepositoryError::PayloadSizeMismatch { expected, actual }
                    if expected == length && actual == body.len() as u64
            ),
            "short raw stream: {error:?}"
        );
    }
    for (body, length) in [(b"body!".as_slice(), 4), (b"body", 3)] {
        let raw = ObjectKey::blob(BlobId::new(Digest::hash(body)));
        let error = stage(&raw, length, body).await;
        assert!(excess_bytes(&error), "long raw stream: {error:?}");
    }
    let raw = ObjectKey::blob(BlobId::new(Digest::hash(b"body")));
    let error = stage(&raw, 4, b"BODY").await;
    assert!(
        matches!(
            error,
            RepositoryError::Format(FormatError::NativeIdentityMismatch { .. })
        ),
        "{error:?}"
    );
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert_eq!(
        snapshot.object_batch(&[key, raw]).await.unwrap(),
        [None, None]
    );
}

#[tokio::test]
async fn verified_stream_extracts_native_tree_links() {
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let child =
        git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Blob, b"file").unwrap();
    let mut tree = b"100644 file\0".to_vec();
    tree.extend_from_slice(child.native_id());
    let key = git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Tree, &tree).unwrap();
    let session = repository.mutation_session().await.unwrap();
    let staged = session
        .stage_object_reader_with_size(key, tree.len() as u64, &mut std::io::Cursor::new(&tree))
        .await
        .unwrap();
    assert_eq!(staged.record().links(), &[child]);
}

#[tokio::test]
async fn declared_size_limit_rejects_before_reading_or_opening_a_writer() {
    let payloads = CountingBlobStore::new();
    let writes = payloads.writes.clone();
    let repository = Repository::with_formats(
        payloads,
        MemoryMetadataStore::new().unwrap(),
        FormatRegistry::builtin(),
        FormatLimits {
            max_payload_bytes: 3,
            ..Default::default()
        },
    );
    let mut source = std::io::Cursor::new(b"body");
    let key = git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Blob, b"body").unwrap();
    let session = repository.mutation_session().await.unwrap();
    assert!(matches!(
        session
            .stage_object_reader_with_size(key, 4, &mut source)
            .await,
        Err(RepositoryError::Format(FormatError::PayloadLimit {
            limit: 3
        }))
    ));
    assert_eq!(source.position(), 0);
    assert_eq!(writes.load(Ordering::SeqCst), 0);
}

struct EmptyReadVerifier {
    namespace: NamespaceId,
}
#[async_trait::async_trait]
impl ObjectFormat for EmptyReadVerifier {
    fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }
    async fn verify(
        &self,
        mut context: VerificationContext<'_>,
        _: &FormatLimits,
    ) -> Result<VerifiedObject, FormatError> {
        assert_eq!(context.read(&mut []).await?, 0);
        context.finish(Vec::new())
    }
}

#[tokio::test]
async fn an_empty_verifier_read_cannot_substitute_for_source_eof() {
    let key = ObjectKey::blob(BlobId::new(Digest::hash(b"")));
    let formats = FormatRegistry::new([Arc::new(EmptyReadVerifier {
        namespace: key.namespace().clone(),
    }) as Arc<dyn ObjectFormat>])
    .unwrap();
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        formats,
        FormatLimits::default(),
    );
    let session = repository.mutation_session().await.unwrap();
    // Even declared-empty sources must actually reach EOF through a nonempty read.
    assert!(matches!(
        session
            .stage_object_reader_with_size(key, 0, &mut std::io::Cursor::new(b"unexpected"))
            .await,
        Err(RepositoryError::Format(
            FormatError::PayloadNotFullyConsumed(_)
        ))
    ));
}

struct IgnoringReadErrorsVerifier {
    namespace: NamespaceId,
}
#[async_trait::async_trait]
impl ObjectFormat for IgnoringReadErrorsVerifier {
    fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }
    async fn verify(
        &self,
        mut context: VerificationContext<'_>,
        _: &FormatLimits,
    ) -> Result<VerifiedObject, FormatError> {
        let mut saw_error = false;
        // Deliberately ignore failures, but never loop forever on a poisoned
        // reader. Successful reads still go through the real hashing context.
        for _ in 0..5 {
            match context.read(&mut [0; 4]).await {
                Ok(0) => break,
                Ok(_) => {}
                Err(_) => saw_error = true,
            }
        }
        assert!(saw_error, "the fixture must exercise a read error");
        context.finish(Vec::new())
    }
}

struct RecoveringReadError {
    body: std::io::Cursor<&'static [u8]>,
    fail_at: Option<u64>,
}
impl tokio::io::AsyncRead for RecoveringReadError {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.fail_at == Some(self.body.position()) {
            self.fail_at = None;
            return std::task::Poll::Ready(Err(std::io::Error::other("recoverable source error")));
        }
        std::pin::Pin::new(&mut self.body).poll_read(cx, buffer)
    }
}

#[tokio::test]
async fn custom_verifiers_cannot_erase_stream_failures() {
    let key = ObjectKey::blob(BlobId::new(Digest::hash(b"body")));
    let formats = FormatRegistry::new([Arc::new(IgnoringReadErrorsVerifier {
        namespace: key.namespace().clone(),
    }) as Arc<dyn ObjectFormat>])
    .unwrap();
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        formats,
        FormatLimits::default(),
    );
    let session = repository.mutation_session().await.unwrap();
    assert!(
        matches!(
            session
                .stage_object_reader_with_size(key.clone(), 4, &mut std::io::Cursor::new(b"body!"))
                .await,
            Err(RepositoryError::Format(
                FormatError::PayloadNotFullyConsumed(_)
            ))
        ),
        "a caught excess-byte error must not authenticate a truncated prefix"
    );
    for fail_at in [0, 4] {
        let mut reader = RecoveringReadError {
            body: std::io::Cursor::new(b"body"),
            fail_at: Some(fail_at),
        };
        assert!(
            matches!(
                session
                    .stage_object_reader_with_size(key.clone(), 4, &mut reader)
                    .await,
                Err(RepositoryError::Format(
                    FormatError::PayloadNotFullyConsumed(_)
                ))
            ),
            "a caught source error must not be erased by a later EOF"
        );
    }
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
}

#[derive(Clone, Copy)]
enum WriterFault {
    Digest,
    Size,
    Write,
    Close,
}
struct FaultStore {
    inner: MemoryBlobStore,
    fault: WriterFault,
}
struct FaultWriter {
    inner: Box<dyn casita::experimental::BlobWriter>,
    fault: WriterFault,
}
impl tokio::io::AsyncWrite for FaultWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if matches!(self.fault, WriterFault::Write) {
            return std::task::Poll::Ready(Err(std::io::Error::other("injected write failure")));
        }
        std::pin::Pin::new(&mut self.inner).poll_write(cx, bytes)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
#[async_trait::async_trait]
impl casita::experimental::BlobWriter for FaultWriter {
    async fn close(&mut self) -> Result<(BlobId, u64), casita::experimental::Error> {
        let (id, size) = self.inner.close().await?;
        match self.fault {
            WriterFault::Digest => Ok((BlobId::new(Digest::hash(b"wrong digest")), size)),
            WriterFault::Size => Ok((id, size + 1)),
            WriterFault::Close => Err(casita::experimental::Error::Msg(
                "injected close failure".into(),
            )),
            WriterFault::Write => Ok((id, size)),
        }
    }
}
#[async_trait::async_trait]
impl casita::experimental::BlobStore for FaultStore {
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
    async fn has(&self, id: &BlobId) -> Result<bool, casita::experimental::Error> {
        self.inner.has(id).await
    }
    async fn open_read(
        &self,
        id: &BlobId,
    ) -> Result<Option<Box<dyn casita::experimental::BlobReader>>, casita::experimental::Error>
    {
        self.inner.open_read(id).await
    }
    async fn open_write(&self) -> Box<dyn casita::experimental::BlobWriter> {
        Box::new(FaultWriter {
            inner: self.inner.open_write().await,
            fault: self.fault,
        })
    }
}

#[tokio::test]
async fn writer_failures_and_false_backend_identity_never_produce_a_seal() {
    for fault in [
        WriterFault::Digest,
        WriterFault::Size,
        WriterFault::Write,
        WriterFault::Close,
    ] {
        let repository = Repository::new(
            FaultStore {
                inner: MemoryBlobStore::new(),
                fault,
            },
            MemoryMetadataStore::new().unwrap(),
        );
        let key =
            git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Blob, b"body").unwrap();
        let session = repository.mutation_session().await.unwrap();
        let error = session
            .stage_object_reader_with_size(key.clone(), 4, &mut std::io::Cursor::new(b"body"))
            .await
            .expect_err("a writer fault must not stage");
        let expected = match fault {
            WriterFault::Digest => {
                matches!(error, RepositoryError::PayloadIdentityMismatch { .. })
            }
            WriterFault::Size => matches!(
                error,
                RepositoryError::PayloadSizeMismatch {
                    expected: 4,
                    actual: 5
                }
            ),
            WriterFault::Write => injected(&error, "injected write failure"),
            WriterFault::Close => matches!(
                &error,
                RepositoryError::Payload(casita::experimental::Error::Msg(message))
                    if message == "injected close failure"
            ),
        };
        assert!(expected, "{error:?}");
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
    }
}

struct FailingSource<'a> {
    body: &'a [u8],
    position: usize,
    fail_after: usize,
}

impl tokio::io::AsyncRead for FailingSource<'_> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        output: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if output.remaining() == 0 {
            return std::task::Poll::Ready(Ok(()));
        }
        if self.position == self.fail_after {
            return std::task::Poll::Ready(Err(std::io::Error::other("injected source failure")));
        }
        let count = output.remaining().min(self.fail_after - self.position);
        output.put_slice(&self.body[self.position..self.position + count]);
        self.position += count;
        std::task::Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn source_errors_before_and_after_the_declared_body_never_produce_a_seal() {
    let body = b"body";
    for format in [GitObjectFormat::Sha1, GitObjectFormat::Sha256] {
        for fail_after in [0, 2, body.len()] {
            let (repository, reads, _) = counted();
            let key = git_object_key_for_body(format, GitObjectKind::Blob, body).unwrap();
            let session = repository.mutation_session().await.unwrap();
            let mut source = FailingSource {
                body,
                position: 0,
                fail_after,
            };
            let error = session
                .stage_object_reader_with_size(key.clone(), body.len() as u64, &mut source)
                .await
                .expect_err("a failed source must not stage");
            assert!(injected(&error, "injected source failure"), "{error:?}");
            assert_eq!(source.position, fail_after);
            assert_eq!(reads.load(Ordering::SeqCst), 0);
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
        }
    }
}

/// Permanent workload: benchmark run git-verified-stream.
#[tokio::test]
#[ignore = "run through benchmark run git-verified-stream"]
async fn benchmark_git_verified_stream() {
    let backend = std::env::var("CASITA_GIT_STREAM_BACKEND").unwrap();
    if backend == "local" {
        let destination = tempfile::tempdir().unwrap();
        let repository = Repository::local(destination.path()).await.unwrap();
        stream_benchmark(&repository).await;
        repository.flush().await.unwrap();
    } else {
        assert_eq!(backend, "memory");
        let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
        stream_benchmark(&repository).await;
    }
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
}

async fn stream_benchmark<PS: casita::experimental::BlobStore, SS: MetadataStore>(
    repository: &Repository<PS, SS>,
) {
    let bytes: usize = std::env::var("CASITA_GIT_STREAM_BYTES")
        .unwrap()
        .parse()
        .unwrap();
    let strategy = std::env::var("CASITA_GIT_STREAM_STRATEGY").unwrap();
    let backend = std::env::var("CASITA_GIT_STREAM_BACKEND").unwrap();
    let mut body = vec![0u8; bytes];
    let mut state = 0x9e3779b97f4a7c15u64;
    for part in body.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        part.copy_from_slice(&state.to_le_bytes()[..part.len()]);
    }
    let key = git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Blob, &body).unwrap();
    let session = repository.mutation_session().await.unwrap();
    let mut reader = std::io::Cursor::new(&body);
    let start = std::time::Instant::now();
    let staged = match strategy.as_str() {
        "reread" => session
            .stage_object_reader(key.clone(), &mut reader)
            .await
            .unwrap(),
        "stream" => session
            .stage_object_reader_with_size(key.clone(), bytes as u64, &mut reader)
            .await
            .unwrap(),
        _ => panic!("unknown ingestion strategy"),
    };
    let wall_nanos = start.elapsed().as_nanos();
    assert_eq!(staged.record().key(), &key);
    assert_eq!(staged.record().payload(), BlobId::new(Digest::hash(&body)));
    assert_eq!(staged.record().payload_size(), bytes as u64);
    session
        .publish_rooted(vec![staged], "native".try_into().unwrap(), key.clone())
        .await
        .unwrap();
    assert_eq!(
        repository.verify_closure(&key).await.unwrap(),
        casita::experimental::ClosureStatus::Complete { objects: 1 }
    );
    let (_, mut opened) = repository.open_payload(&key).await.unwrap().unwrap();
    let mut actual = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut opened, &mut actual)
        .await
        .unwrap();
    assert_eq!(actual, body);
    println!(
        "git_verified_stream_sample {}",
        serde_json::json!({
            "strategy": strategy, "backend": backend, "file_bytes": bytes,
            "wall_nanos": wall_nanos, "root": key.to_string(),
            "correctness": "exact identity, length, closure and byte-for-byte readback",
        })
    );
}
