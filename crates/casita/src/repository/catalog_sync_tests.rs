//! Exercise candidate/synchronization overlap through repository callers.

use super::*;
use crate::ChunkedBlobStore;
use crate::metadata::{CommitResult, MetadataSnapshot};
use crate::object_store::{ObjectStore, memory::InMemory, path::Path};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
struct CommitPause {
    reached: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    used: AtomicBool,
}

#[derive(Clone)]
struct PausedCommitStore {
    inner: crate::TursoMetadataStore,
    pause: Arc<CommitPause>,
    fail: bool,
}

#[async_trait]
impl MetadataStore for PausedCommitStore {
    async fn try_collection_lease(
        &self,
    ) -> Result<Option<crate::metadata::RepositoryLease>, MetadataError> {
        self.inner.try_collection_lease().await
    }
    async fn pin_store(&self) -> Result<Arc<dyn crate::metadata::PinStore>, MetadataError> {
        self.inner.pin_store().await
    }

    fn coordinates_payload_catalog(&self) -> bool {
        true
    }

    async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
        self.inner.snapshot().await
    }

    async fn commit(
        &self,
        expected: &crate::RepositoryRevision,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        if !self.pause.used.swap(true, Ordering::SeqCst) {
            self.pause.reached.notify_one();
            self.pause.resume.notified().await;
            if self.fail {
                return Err(MetadataError::Backend(
                    "injected catalog commit failure".into(),
                ));
            }
        }
        self.inner.commit(expected, mutation).await
    }
}

async fn payloads(objects: Arc<dyn ObjectStore>, catalog: &[u8]) -> ChunkedBlobStore {
    ChunkedBlobStore::packed_with_catalog(
        objects,
        Path::from("repository-catalog-sync"),
        1024,
        crate::PackOptions {
            target_size: u64::MAX,
            cache_capacity: 0,
        },
        catalog,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn catalog_sync_during_repository_commit_preserves_retry_and_admission() {
    for fail in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("state.db");
        let state = crate::TursoMetadataStore::open(&database).await.unwrap();
        // Separate metadata connections and packed indexes, as with independent
        // remote clients. Local SQLite supplies the real conditional commit.
        let other_state = crate::TursoMetadataStore::open(&database).await.unwrap();
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let empty = ChunkedBlobStore::empty_state_catalog().unwrap();
        let pause = Arc::new(CommitPause::default());
        let local = Repository::new(
            payloads(objects.clone(), &empty).await,
            PausedCommitStore {
                inner: state.clone(),
                pause: pause.clone(),
                fail,
            },
        );
        let remote = Repository::new(payloads(objects.clone(), &empty).await, other_state);
        let session = local.mutation_session().await.unwrap();
        let first = session.stage_blob(b"prepared local payload").await.unwrap();
        let first_key = first.record().key().clone();
        let first_payload = first.record().payload();
        let publish = session.publish_unrooted(vec![first]);
        let overlap = async {
            tokio::time::timeout(Duration::from_secs(10), pause.reached.notified())
                .await
                .unwrap();
            // Always release the tracked commit if a regression assertion fails.
            struct Resume<'a>(&'a CommitPause);
            impl Drop for Resume<'_> {
                fn drop(&mut self) {
                    self.0.resume.notify_one();
                }
            }
            let _resume = Resume(&pause);
            let other = remote.mutation_session().await.unwrap();
            let object = other
                .stage_blob(b"independent remote payload")
                .await
                .unwrap();
            let other_key = object.record().key().clone();
            other.publish_unrooted(vec![object]).await.unwrap();
            drop(other);
            // Both supported synchronization callers must complete while the
            // candidate remains unresolved, and scoped reads select their pin.
            let hold = tokio::time::timeout(Duration::from_secs(10), local.retention_hold())
                .await
                .unwrap()
                .unwrap();
            let (_, mut reader) = hold.open_payload(&other_key).await.unwrap().unwrap();
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, b"independent remote payload");
            assert!(hold.open_payload(&first_key).await.unwrap().is_none());
            let later = tokio::time::timeout(Duration::from_secs(10), local.mutation_session())
                .await
                .unwrap()
                .unwrap();
            let object = later.stage_blob(b"later admitted payload").await.unwrap();
            assert_eq!(
                local.payloads().read_to_vec(&first_payload).await.unwrap(),
                Some(b"prepared local payload".to_vec())
            );
            let later_key = object.record().key().clone();
            local.payloads().publication().flush().await.unwrap();
            drop(object);
            (later, later_key, other_key)
        };
        let (result, (later, later_key, other_key)) =
            tokio::time::timeout(Duration::from_secs(30), async {
                tokio::join!(publish, overlap)
            })
            .await
            .unwrap();
        let object = later.stage_blob(b"later admitted payload").await.unwrap();
        assert_eq!(object.record().key(), &later_key);
        if fail {
            assert!(
                matches!(result, Err(RepositoryError::Metadata(MetadataError::Backend(ref message))) if message.contains("injected catalog commit failure"))
            );
            let retry = later.stage_blob(b"prepared local payload").await.unwrap();
            later.publish_unrooted(vec![retry, object]).await.unwrap();
        } else {
            // The other handle advanced metadata, forcing a real stale-revision
            // abort and retry in Publication before this result can succeed.
            result.unwrap();
            later.publish_unrooted(vec![object]).await.unwrap();
        }
        drop(later);
        drop(session);
        let snapshot = state.snapshot().await.unwrap();
        let catalog = snapshot.payload_catalog().unwrap().to_vec();
        drop(snapshot);
        let reopened = Repository::new(
            payloads(objects, &catalog).await,
            crate::TursoMetadataStore::open(&database).await.unwrap(),
        );
        let hold = reopened.retention_hold().await.unwrap();
        for (key, expected) in [
            (first_key, b"prepared local payload".as_slice()),
            (other_key, b"independent remote payload"),
            (later_key, b"later admitted payload"),
        ] {
            let (_, mut reader) = hold.open_payload(&key).await.unwrap().unwrap();
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, expected);
        }
    }
}
