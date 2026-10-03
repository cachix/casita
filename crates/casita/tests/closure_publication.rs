#![cfg(all(feature = "native", feature = "experimental"))]

use casita::experimental::{
    ClosureStatus, FormatLimits, FormatRegistry, MemoryBlobStore, MemoryMetadataStore,
    MetadataStore, Repository, RepositoryError,
};
use casita::{BlobId, Digest, Directory, Node, ObjectKey, PathComponent};
use futures::TryStreamExt;
use std::collections::BTreeSet;

/// With nothing staged and nothing to check, an unnamed closure publication is
/// the same empty commit as an empty unrooted publication.
#[tokio::test]
async fn empty_closure_publication_matches_an_empty_unrooted_one() {
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let session = repository.mutation_session().await.unwrap();
    let unrooted = session.publish_unrooted(Vec::new()).await.unwrap();
    let closures = session
        .publish_closures(Vec::new(), BTreeSet::new())
        .await
        .unwrap();
    for result in [&unrooted, &closures] {
        assert_eq!(
            (
                result.objects_inserted,
                result.objects_removed,
                result.roots_changed
            ),
            (0, 0, 0)
        );
    }
    assert_ne!(closures.revision, unrooted.revision);
}

#[tokio::test]
async fn unnamed_directory_publication_records_requested_closure_witnesses() {
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let session = repository.mutation_session().await.unwrap();
    let staged = session.stage_directory(&Directory::new()).await.unwrap();
    let key = staged.record().key().clone();
    session
        .publish_closures(vec![staged], BTreeSet::from([key.clone()]))
        .await
        .unwrap();
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .validated_closures(&[key])
            .await
            .unwrap(),
        [true],
        "a checked unnamed directory needs a reusable closure witness"
    );
}

#[tokio::test]
async fn only_requested_directory_targets_are_marked_and_none_are_permanent_roots() {
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let session = repository.mutation_session().await.unwrap();
    let child = Directory::new();
    let parent = Directory::try_from_iter([(
        PathComponent::try_from("child").unwrap(),
        Node::Directory {
            digest: child.digest(),
            size: child.size(),
        },
    )])
    .unwrap();
    let child = session.stage_directory(&child).await.unwrap();
    let child_key = child.record().key().clone();
    let parent = session.stage_directory(&parent).await.unwrap();
    let parent_key = parent.record().key().clone();
    session
        .publish_closures(vec![child, parent], BTreeSet::from([parent_key.clone()]))
        .await
        .unwrap();
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert_eq!(
        snapshot
            .validated_closures(&[child_key.clone(), parent_key.clone()])
            .await
            .unwrap(),
        [false, true]
    );
    assert!(
        snapshot
            .roots()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    drop(snapshot);
    assert_eq!(
        repository
            .try_collect()
            .await
            .unwrap()
            .removed
            .logical_objects,
        0
    );
    drop(session);
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
    assert_eq!(
        repository.collect().await.unwrap().removed.logical_objects,
        2
    );
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .validated_payload_batch(&[child_key, parent_key])
            .await
            .unwrap(),
        [None, None]
    );
}

#[path = "support/counting_blob_store.rs"]
mod counting_blob_store;

#[tokio::test]
async fn existing_targets_acquire_reusable_witnesses_without_new_objects() {
    use std::sync::atomic::Ordering;
    let payloads = counting_blob_store::CountingBlobStore::new();
    let reads = payloads.reads.clone();
    let repository = Repository::new(payloads, MemoryMetadataStore::new().unwrap());
    let session = repository.mutation_session().await.unwrap();
    let directory = session.stage_directory(&Directory::new()).await.unwrap();
    let key = directory.record().key().clone();
    session.publish_unrooted(vec![directory]).await.unwrap();
    reads.store(0, Ordering::SeqCst);
    session
        .publish_closures(Vec::new(), BTreeSet::from([key.clone()]))
        .await
        .unwrap();
    assert!(
        reads.load(Ordering::SeqCst) > 0,
        "an unchecked directory needs normal verification"
    );
    reads.store(0, Ordering::SeqCst);
    session
        .publish_closures(Vec::new(), BTreeSet::from([key]))
        .await
        .unwrap();
    assert_eq!(
        reads.load(Ordering::SeqCst),
        0,
        "a recorded witness stops subsequent traversal"
    );
}

#[tokio::test]
async fn missing_descendants_reject_every_record_and_witness_in_the_batch() {
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let session = repository.mutation_session().await.unwrap();
    let valid = session.stage_directory(&Directory::new()).await.unwrap();
    let valid_key = valid.record().key().clone();
    let missing = BlobId::new(Digest::hash(b"absent"));
    let broken = Directory::try_from_iter([(
        PathComponent::try_from("missing").unwrap(),
        Node::File {
            digest: missing,
            size: 6,
            executable: false,
        },
    )])
    .unwrap();
    let broken = session.stage_directory(&broken).await.unwrap();
    let broken_key = broken.record().key().clone();
    assert!(matches!(
        session
            .publish_closures(
                vec![valid, broken],
                BTreeSet::from([valid_key.clone(), broken_key.clone()])
            )
            .await,
        Err(RepositoryError::RootNotPublishable {
            status: ClosureStatus::Missing { .. },
            ..
        })
    ));
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert!(snapshot.object(&valid_key).await.unwrap().is_none());
    assert!(snapshot.object(&broken_key).await.unwrap().is_none());
    assert_eq!(
        snapshot
            .validated_closures(&[valid_key, broken_key])
            .await
            .unwrap(),
        [false, false]
    );
}

#[tokio::test]
async fn complete_records_with_invalid_directory_relations_are_rejected() {
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let session = repository.mutation_session().await.unwrap();
    let child = Directory::new();
    let wrong = Directory::try_from_iter([(
        PathComponent::try_from("child").unwrap(),
        Node::Directory {
            digest: child.digest(),
            size: 1,
        },
    )])
    .unwrap();
    let child = session.stage_directory(&child).await.unwrap();
    let child_key = child.record().key().clone();
    let wrong = session.stage_directory(&wrong).await.unwrap();
    let key = wrong.record().key().clone();
    assert!(matches!(
        session
            .publish_closures(vec![child, wrong], BTreeSet::from([child_key, key.clone()]))
            .await,
        Err(RepositoryError::RootNotPublishable {
            status: ClosureStatus::Invalid { .. },
            ..
        })
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
}

#[tokio::test]
async fn requested_targets_and_staged_objects_each_obey_batch_limits() {
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        FormatRegistry::builtin(),
        FormatLimits {
            max_batch_objects: 1,
            ..Default::default()
        },
    );
    let session = repository.mutation_session().await.unwrap();
    let first = session.stage_blob(b"first").await.unwrap();
    let second = session.stage_blob(b"second").await.unwrap();
    let keys = BTreeSet::from([first.record().key().clone(), second.record().key().clone()]);
    assert!(matches!(
        session.publish_closures(Vec::new(), keys).await,
        Err(RepositoryError::LimitExceeded(_))
    ));
    assert!(matches!(
        session
            .publish_closures(vec![first, second], BTreeSet::new())
            .await,
        Err(RepositoryError::LimitExceeded(_))
    ));
    assert!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .object(&ObjectKey::blob(BlobId::new(Digest::hash(b"first"))))
            .await
            .unwrap()
            .is_none()
    );
}

struct RejectingBlob(casita::experimental::BlobFormat);
#[async_trait::async_trait]
impl casita::experimental::ObjectFormat for RejectingBlob {
    fn namespace(&self) -> &casita::NamespaceId {
        self.0.namespace()
    }
    async fn verify(
        &self,
        context: casita::experimental::VerificationContext<'_>,
        limits: &FormatLimits,
    ) -> Result<casita::experimental::VerifiedObject, casita::experimental::FormatError> {
        self.0.verify(context, limits).await
    }
    async fn verify_links(
        &self,
        _context: casita::experimental::VerificationContext<'_>,
        _object: &casita::experimental::ObjectRecord,
        _links: &dyn casita::experimental::DirectLinkView,
        _limits: &FormatLimits,
    ) -> Result<(), casita::experimental::FormatError> {
        Err(casita::experimental::FormatError::InvalidPayload {
            namespace: self.namespace().clone(),
            message: "custom relation rejects closure".into(),
        })
    }
}

#[tokio::test]
async fn custom_format_relations_cannot_be_bypassed_by_checked_publication() {
    use std::sync::Arc;
    let formats =
        FormatRegistry::new([Arc::new(RejectingBlob(Default::default()))
            as Arc<dyn casita::experimental::ObjectFormat>])
        .unwrap();
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        formats,
        FormatLimits::default(),
    );
    let session = repository.mutation_session().await.unwrap();
    let staged = session
        .stage_blob(b"valid body with a rejecting custom relation")
        .await
        .unwrap();
    let key = staged.record().key().clone();
    assert!(matches!(
        session
            .publish_closures(vec![staged], BTreeSet::from([key.clone()]))
            .await,
        Err(RepositoryError::RootNotPublishable {
            status: ClosureStatus::Invalid { .. },
            ..
        })
    ));
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert!(snapshot.object(&key).await.unwrap().is_none());
    assert_eq!(snapshot.validated_closures(&[key]).await.unwrap(), [false]);
}

#[tokio::test]
async fn checking_existing_targets_retains_their_entire_graph_in_a_new_session() {
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let seed = repository.mutation_session().await.unwrap();
    let file = seed
        .stage_blob(b"child kept by the checked parent")
        .await
        .unwrap();
    let file_key = file.record().key().clone();
    let tree = Directory::try_from_iter([(
        PathComponent::try_from("file").unwrap(),
        Node::File {
            digest: file.record().payload(),
            size: file.record().payload_size(),
            executable: false,
        },
    )])
    .unwrap();
    let tree = seed.stage_directory(&tree).await.unwrap();
    let key = tree.record().key().clone();
    seed.publish_unrooted(vec![file, tree]).await.unwrap();
    let previous = repository.owned_retention_hold().await.unwrap();
    drop(seed);
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
    let checked = repository.mutation_session().await.unwrap();
    checked
        .publish_closures(Vec::new(), BTreeSet::from([key.clone()]))
        .await
        .unwrap();
    drop(previous);
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
    assert_eq!(
        repository
            .try_collect()
            .await
            .unwrap()
            .removed
            .logical_objects,
        0
    );
    assert!(matches!(
        repository.verify_closure(&key).await.unwrap(),
        ClosureStatus::Complete { objects: 2 }
    ));
    drop(checked);
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
    assert_eq!(
        repository.collect().await.unwrap().removed.logical_objects,
        2
    );
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert!(snapshot.object(&file_key).await.unwrap().is_none());
    assert_eq!(snapshot.validated_closures(&[key]).await.unwrap(), [false]);
}

#[tokio::test]
async fn nested_targets_share_closure_proofs_in_any_order() {
    use std::sync::atomic::Ordering;
    let payloads = counting_blob_store::CountingBlobStore::new();
    let reads = payloads.reads.clone();
    let repository = Repository::new(payloads, MemoryMetadataStore::new().unwrap());
    let session = repository.mutation_session().await.unwrap();
    let mut directory = Directory::new();
    let mut staged = Vec::new();
    let mut targets = BTreeSet::new();
    const DEPTH: usize = 16;
    for _ in 0..DEPTH {
        let object = session.stage_directory(&directory).await.unwrap();
        targets.insert(object.record().key().clone());
        staged.push(object);
        directory = Directory::try_from_iter([(
            PathComponent::try_from("child").unwrap(),
            Node::Directory {
                digest: directory.digest(),
                size: directory.size(),
            },
        )])
        .unwrap();
    }
    reads.store(0, Ordering::SeqCst);
    session
        .publish_closures(staged, targets.clone())
        .await
        .unwrap();
    // Targets are walked in key order, unrelated to their nesting. Whichever
    // walk reaches a directory first proves it, and later walks stop there:
    // each directory reads its own payload once, and each parent reads its
    // immediate child's to check the declared size.
    assert_eq!(reads.load(Ordering::SeqCst), 2 * DEPTH - 1);
    assert!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .validated_closures(&targets.into_iter().collect::<Vec<_>>())
            .await
            .unwrap()
            .into_iter()
            .all(|valid| valid)
    );
}

#[tokio::test]
async fn raw_blob_targets_are_checked_without_witnesses_or_reads() {
    use std::sync::atomic::Ordering;
    let payloads = counting_blob_store::CountingBlobStore::new();
    let reads = payloads.reads.clone();
    let repository = Repository::new(payloads, MemoryMetadataStore::new().unwrap());
    let session = repository.mutation_session().await.unwrap();
    let staged = session.stage_blob(b"staged raw blob").await.unwrap();
    let staged_key = staged.record().key().clone();
    session
        .publish_closures(vec![staged], BTreeSet::from([staged_key.clone()]))
        .await
        .unwrap();
    let existing = session.stage_blob(b"existing raw blob").await.unwrap();
    let existing_key = existing.record().key().clone();
    session.publish_unrooted(vec![existing]).await.unwrap();
    reads.store(0, Ordering::SeqCst);
    session
        .publish_closures(Vec::new(), BTreeSet::from([existing_key.clone()]))
        .await
        .unwrap();
    assert_eq!(
        reads.load(Ordering::SeqCst),
        0,
        "a blob record proves itself"
    );
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .validated_closures(&[staged_key, existing_key])
            .await
            .unwrap(),
        [false, false],
        "raw blobs need no stored witness"
    );
    let absent = ObjectKey::blob(BlobId::new(Digest::hash(b"never stored")));
    assert!(matches!(
        session
            .publish_closures(Vec::new(), BTreeSet::from([absent]))
            .await,
        Err(RepositoryError::RootNotPublishable {
            status: ClosureStatus::Missing { from: None, .. },
            ..
        })
    ));
}
