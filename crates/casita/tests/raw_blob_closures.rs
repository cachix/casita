#![cfg(all(feature = "native", feature = "experimental"))]

use casita::experimental::{
    BlobFormat, ClosureStatus, DirectLinkView, DirectoryFormat, FormatError, FormatLimits,
    FormatRegistry, MemoryBlobStore, MemoryMetadataStore, MetadataStore, ObjectFormat, Repository,
    VerificationContext, VerifiedObject,
};
use casita::import::FilesystemImport;
use casita::{Directory, NamespaceId, Node, ObjectKey, ObjectRecord, PathComponent};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
#[path = "support/counting_blob_store.rs"]
mod counting_blob_store;
use counting_blob_store::CountingBlobStore;

fn counted() -> (
    Repository<CountingBlobStore, MemoryMetadataStore>,
    Arc<AtomicUsize>,
) {
    let payloads = CountingBlobStore::new();
    let reads = payloads.reads.clone();
    let repository = Repository::new(payloads, MemoryMetadataStore::new().unwrap());
    (repository, reads)
}

async fn witnesses<PS, SS: MetadataStore>(
    repository: &Repository<PS, SS>,
    keys: &[ObjectKey],
) -> Vec<bool> {
    repository
        .metadata()
        .snapshot()
        .await
        .unwrap()
        .validated_closures(keys)
        .await
        .unwrap()
}

#[tokio::test]
async fn unrooted_raw_blobs_are_complete_without_witnesses_or_reads() {
    let (repository, reads) = counted();
    let session = repository.mutation_session().await.unwrap();
    let mut keys = Vec::new();
    for size in [0, 1, 65535, 65536, 65537] {
        let object = session.stage_blob(&vec![b'x'; size]).await.unwrap();
        keys.push(object.record().key().clone());
        session.publish_unrooted(vec![object]).await.unwrap();
    }
    // Completeness follows from each present record; nothing is stored.
    assert_eq!(witnesses(&repository, &keys).await, vec![false; keys.len()]);
    for key in &keys {
        assert_eq!(
            repository.verify_closure_incremental(key).await.unwrap(),
            ClosureStatus::Complete { objects: 1 }
        );
    }
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    // An exhaustive audit still rereads every payload.
    for key in &keys {
        assert_eq!(
            repository.verify_closure(key).await.unwrap(),
            ClosureStatus::Complete { objects: 1 }
        );
    }
    assert_eq!(reads.load(Ordering::SeqCst), keys.len());
}

/// Completeness is derived from the record, so an incremental check cannot
/// notice a payload lost from storage; only an audit, which rereads, does.
#[tokio::test]
async fn audits_still_find_a_raw_blob_payload_lost_from_storage() {
    use casita::experimental::BlobGc;
    let payloads = CountingBlobStore::new();
    let storage = payloads.inner.clone();
    let repository = Repository::new(payloads, MemoryMetadataStore::new().unwrap());
    let session = repository.mutation_session().await.unwrap();
    let object = session
        .stage_blob(b"payload lost from storage")
        .await
        .unwrap();
    let key = object.record().key().clone();
    let payload = object.record().payload();
    session.publish_unrooted(vec![object]).await.unwrap();
    storage.delete_blob(&payload).await.unwrap();
    assert_eq!(
        repository.verify_closure_incremental(&key).await.unwrap(),
        ClosureStatus::Complete { objects: 1 }
    );
    let audit = repository.verify_closure(&key).await.unwrap();
    assert!(
        matches!(&audit, ClosureStatus::Invalid { object, reason }
            if object == &key && reason.contains("is absent")),
        "an audit must reread the payload: {audit:?}"
    );
}

#[tokio::test]
async fn rooting_a_raw_blob_records_its_witness_without_reading() {
    let (repository, reads) = counted();
    let session = repository.mutation_session().await.unwrap();
    let object = session.stage_blob(b"rooted raw blob").await.unwrap();
    let key = object.record().key().clone();
    session
        .publish_rooted(vec![object], "raw".try_into().unwrap(), key.clone())
        .await
        .unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    // Named targets keep their witness: fast application root changes need it.
    assert_eq!(witnesses(&repository, &[key]).await, [true]);
}

#[tokio::test]
async fn directories_over_raw_blobs_read_only_the_directory() {
    let (repository, reads) = counted();
    let session = repository.mutation_session().await.unwrap();
    let mut entries = Vec::new();
    let mut files = Vec::new();
    for index in 0..64_u32 {
        let body = format!("file {index}");
        let object = session.stage_blob(body.as_bytes()).await.unwrap();
        entries.push((
            PathComponent::try_from(format!("f{index:02}").as_str()).unwrap(),
            Node::File {
                digest: object.record().payload(),
                size: body.len() as u64,
                executable: false,
            },
        ));
        files.push(object.record().key().clone());
        session.publish_unrooted(vec![object]).await.unwrap();
    }
    let directory = session
        .stage_directory(&Directory::try_from_iter(entries).unwrap())
        .await
        .unwrap();
    let root = directory.record().key().clone();
    session
        .publish_rooted(vec![directory], "tree".try_into().unwrap(), root.clone())
        .await
        .unwrap();
    // The directory's own relations need its payload; its files do not.
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert_eq!(witnesses(&repository, &[root]).await, [true]);
    assert_eq!(
        witnesses(&repository, &files).await,
        vec![false; files.len()]
    );
}

#[tokio::test]
async fn filesystem_imports_witness_directories_but_not_files() {
    let source = tempfile::tempdir().unwrap();
    std::fs::create_dir(source.path().join("nested")).unwrap();
    for index in 0..16 {
        std::fs::write(
            source.path().join(format!("f{index}")),
            format!("top {index}"),
        )
        .unwrap();
        std::fs::write(
            source.path().join("nested").join(format!("f{index}")),
            format!("nested {index}"),
        )
        .unwrap();
    }
    let (repository, reads) = counted();
    let root = repository
        .import(FilesystemImport::new(
            source.path(),
            "tree".try_into().unwrap(),
        ))
        .await
        .unwrap();
    let snapshot = repository.metadata().snapshot().await.unwrap();
    let record = snapshot.object(&root).await.unwrap().unwrap();
    let mut directories = vec![root.clone()];
    let mut files = Vec::new();
    for link in record.links() {
        if link.namespace().as_str() == "casita.blob.v1" {
            files.push(link.clone());
        } else {
            directories.push(link.clone());
        }
    }
    assert_eq!(files.len(), 16);
    assert_eq!(directories.len(), 2);
    assert_eq!(witnesses(&repository, &directories).await, [true, true]);
    assert_eq!(
        witnesses(&repository, &files).await,
        vec![false; files.len()]
    );
    let before = reads.load(Ordering::SeqCst);
    assert!(matches!(
        repository.verify_closure_incremental(&root).await.unwrap(),
        ClosureStatus::Complete { .. }
    ));
    assert_eq!(reads.load(Ordering::SeqCst), before);
}

struct RejectRaw {
    inner: BlobFormat,
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl ObjectFormat for RejectRaw {
    fn namespace(&self) -> &NamespaceId {
        self.inner.namespace()
    }

    async fn verify(
        &self,
        context: VerificationContext<'_>,
        limits: &FormatLimits,
    ) -> Result<VerifiedObject, FormatError> {
        self.inner.verify(context, limits).await
    }

    async fn verify_links(
        &self,
        _context: VerificationContext<'_>,
        _object: &ObjectRecord,
        _links: &dyn DirectLinkView,
        _limits: &FormatLimits,
    ) -> Result<(), FormatError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(FormatError::InvalidPayload {
            namespace: self.namespace().clone(),
            message: "custom raw relation rejected".into(),
        })
    }
}

#[tokio::test]
async fn custom_registries_verify_raw_blobs_normally() {
    let calls = Arc::new(AtomicUsize::new(0));
    let registry = FormatRegistry::new([
        Arc::new(RejectRaw {
            inner: BlobFormat::default(),
            calls: calls.clone(),
        }) as Arc<dyn ObjectFormat>,
        Arc::new(DirectoryFormat::default()) as Arc<dyn ObjectFormat>,
    ])
    .unwrap();
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        registry,
        FormatLimits::default(),
    );
    let session = repository.mutation_session().await.unwrap();
    let object = session.stage_blob(b"custom raw body").await.unwrap();
    let key = object.record().key().clone();
    session.publish_unrooted(vec![object]).await.unwrap();
    assert!(matches!(
        repository.verify_closure_incremental(&key).await.unwrap(),
        ClosureStatus::Invalid { .. }
    ));
    assert!(
        session
            .publish_rooted(Vec::new(), "rejected".try_into().unwrap(), key.clone())
            .await
            .is_err()
    );
    assert!(calls.load(Ordering::SeqCst) >= 2);
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert_eq!(
        snapshot
            .root(&"rejected".try_into().unwrap())
            .await
            .unwrap(),
        None
    );
    assert_eq!(snapshot.validated_closures(&[key]).await.unwrap(), [false]);
}
