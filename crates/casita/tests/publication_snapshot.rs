#![cfg(all(feature = "native", feature = "experimental"))]

use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use casita::experimental::{
    CommitResult, MemoryBlobStore, MemoryMetadataStore, MetadataError, MetadataMutation,
    MetadataSnapshot, MetadataStore, ObjectKey, Repository, RepositoryError, RepositoryRevision,
    RootChange, RootName,
};

struct SnapshotCheckingStore {
    inner: MemoryMetadataStore,
    latest: Mutex<Option<Weak<dyn MetadataSnapshot>>>,
    commits: AtomicUsize,
    race_once: bool,
}

#[async_trait]
impl MetadataStore for SnapshotCheckingStore {
    async fn try_collection_lease(
        &self,
    ) -> Result<Option<casita::experimental::RepositoryLease>, MetadataError> {
        self.inner.try_collection_lease().await
    }
    fn coordinates_payload_catalog(&self) -> bool {
        self.inner.coordinates_payload_catalog()
    }
    async fn pin_store(
        &self,
    ) -> Result<
        std::sync::Arc<dyn casita::experimental::PinStore>,
        casita::experimental::MetadataError,
    > {
        self.inner.pin_store().await
    }

    async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
        let snapshot = self.inner.snapshot().await?;
        *self.latest.lock().unwrap() = Some(Arc::downgrade(&snapshot));
        Ok(snapshot)
    }

    async fn commit(
        &self,
        expected: &RepositoryRevision,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        assert!(
            self.latest
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .upgrade()
                .is_none(),
            "publication kept its validation snapshot alive during commit"
        );
        if self.commits.fetch_add(1, Ordering::SeqCst) == 0 && self.race_once {
            self.inner.commit(expected, MetadataMutation::new()).await?;
        }
        self.inner.commit(expected, mutation).await
    }
}

fn store(race_once: bool) -> Arc<SnapshotCheckingStore> {
    Arc::new(SnapshotCheckingStore {
        inner: MemoryMetadataStore::new().unwrap(),
        latest: Mutex::default(),
        commits: AtomicUsize::new(0),
        race_once,
    })
}

#[tokio::test]
async fn publication_releases_its_snapshot_before_commit_and_each_retry() {
    for race in [false, true] {
        let state = store(race);
        let repository = Repository::new(MemoryBlobStore::new(), state.clone());
        let independent_reader = state.snapshot().await.unwrap();
        let session = repository.mutation_session().await.unwrap();
        let staged = session.stage_blob(b"snapshot lifetime").await.unwrap();
        let key = staged.record().key().clone();
        let name = RootName::try_from("published").unwrap();
        session
            .publish_rooted(vec![staged], name.clone(), key.clone())
            .await
            .unwrap();
        assert_eq!(
            state.commits.load(Ordering::SeqCst),
            if race { 2 } else { 1 }
        );
        let snapshot = state.inner.snapshot().await.unwrap();
        assert_eq!(snapshot.root(&name).await.unwrap(), Some(key.clone()));
        assert!(snapshot.object(&key).await.unwrap().is_some());
        assert_eq!(independent_reader.root(&name).await.unwrap(), None);
        assert_eq!(independent_reader.object(&key).await.unwrap(), None);
    }
}

#[tokio::test]
async fn releasing_the_snapshot_preserves_exact_revision_conflicts() {
    let state = store(true);
    let repository = Repository::new(MemoryBlobStore::new(), state.clone());
    let expected = state.snapshot().await.unwrap().revision();
    let session = repository.mutation_session().await.unwrap();
    let staged = session
        .stage_blob(b"must remain unpublished")
        .await
        .unwrap();
    let key: ObjectKey = staged.record().key().clone();
    let name = RootName::try_from("conflicted").unwrap();
    let error = session
        .publish_at_revision(
            expected,
            vec![staged],
            vec![RootChange::Set {
                name: name.clone(),
                target: key.clone(),
            }],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RepositoryError::Metadata(MetadataError::StaleRevision { .. })
    ));
    assert_eq!(state.commits.load(Ordering::SeqCst), 1);
    let snapshot = state.inner.snapshot().await.unwrap();
    assert_eq!(snapshot.root(&name).await.unwrap(), None);
    assert_eq!(snapshot.object(&key).await.unwrap(), None);
}

#[path = "support/counting_blob_store.rs"]
mod counting_blob_store;

/// Pins retain every closure a publication verified, so a retry after a
/// refused commit reuses its proofs instead of reading the graph again.
#[tokio::test]
async fn refused_commits_reuse_closure_proofs() {
    use casita::{Directory, Node, PathComponent};
    let mut reads_by_race = Vec::new();
    for race in [false, true] {
        let state = store(race);
        let payloads = counting_blob_store::CountingBlobStore::new();
        let reads = payloads.reads.clone();
        let repository = Repository::new(payloads, state.clone());
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
        let parent = session.stage_directory(&parent).await.unwrap();
        let target = parent.record().key().clone();
        reads.store(0, Ordering::SeqCst);
        session
            .publish_rooted(
                vec![child, parent],
                RootName::try_from("proofs").unwrap(),
                target,
            )
            .await
            .unwrap();
        assert_eq!(state.commits.load(Ordering::SeqCst), 1 + usize::from(race));
        reads_by_race.push(reads.load(Ordering::SeqCst));
    }
    assert!(reads_by_race[0] > 0);
    assert_eq!(reads_by_race[0], reads_by_race[1]);
}

/// Every walk in one publication stops at objects an earlier walk proved, so
/// nested root changes read each shared directory once rather than once per
/// enclosing root.
#[tokio::test]
async fn overlapping_root_changes_check_shared_descendants_once() {
    use casita::{Directory, Node, PathComponent};
    const DEPTH: usize = 16;
    let state = store(false);
    let payloads = counting_blob_store::CountingBlobStore::new();
    let reads = payloads.reads.clone();
    let repository = Repository::new(payloads, state.clone());
    let session = repository.mutation_session().await.unwrap();
    let mut directory = Directory::new();
    let mut staged = Vec::new();
    let mut roots = Vec::new();
    for level in 0..DEPTH {
        let object = session.stage_directory(&directory).await.unwrap();
        roots.push(RootChange::Set {
            name: RootName::try_from(format!("level-{level}").as_str()).unwrap(),
            target: object.record().key().clone(),
        });
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
    // The outermost root is checked first; its closure holds every later root.
    roots.reverse();
    reads.store(0, Ordering::SeqCst);
    session.publish(staged, roots.clone()).await.unwrap();
    // Checking a directory reads its payload and its child directory's. Only
    // the first walk checks anything; one walk per root would read ~DEPTH².
    assert_eq!(reads.load(Ordering::SeqCst), 2 * DEPTH - 1);
    let snapshot = state.inner.snapshot().await.unwrap();
    for change in roots {
        let RootChange::Set { name, target } = change else {
            unreachable!("only root sets were requested")
        };
        assert_eq!(snapshot.root(&name).await.unwrap(), Some(target.clone()));
        assert_eq!(
            snapshot
                .validated_closures(std::slice::from_ref(&target))
                .await
                .unwrap(),
            [true]
        );
    }
}
