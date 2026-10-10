use super::*;
use crate::MemoryBlobStore;
use crate::metadata::{MemoryMetadataStore, PinResource};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy)]
enum Failure {
    Fenced,
    Mixed,
    CommitThenTransient,
}

struct Script {
    remaining: AtomicUsize,
    attempts: AtomicUsize,
    failure: Failure,
    payload: Mutex<Option<BlobId>>,
    repoint: Mutex<Option<(RootName, ObjectKey)>>,
    pause: Option<Arc<publication::MaintenancePause>>,
}

#[derive(Clone)]
struct RetryStore {
    inner: MemoryMetadataStore,
    script: Arc<Script>,
}

#[async_trait]
impl MetadataStore for RetryStore {
    async fn try_collection_lease(
        &self,
    ) -> Result<Option<crate::metadata::RepositoryLease>, MetadataError> {
        self.inner.try_collection_lease().await
    }

    fn coordinates_payload_catalog(&self) -> bool {
        self.inner.coordinates_payload_catalog()
    }

    async fn pin_store(&self) -> Result<Arc<dyn crate::metadata::PinStore>, MetadataError> {
        self.inner.pin_store().await
    }

    async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
        self.inner.snapshot().await
    }

    async fn commit(
        &self,
        expected: &crate::RepositoryRevision,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        let attempt = self.script.attempts.fetch_add(1, Ordering::SeqCst);
        let payload = *self.script.payload.lock().unwrap();
        if let Some(payload) = payload {
            assert!(
                self.pin_store()
                    .await?
                    .inventory()
                    .await?
                    .pins
                    .values()
                    .any(|pin| pin.resources.contains(&PinResource::Blob(payload))),
                "staging protection lost between attempts"
            );
        }
        if self
            .script
            .remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            if let Some(pause) = &self.script.pause {
                pause.reached.notify_one();
                pause.resume.notified().await;
            }
            let repoint = self.script.repoint.lock().unwrap().take();
            if let Some((name, key)) = repoint {
                let mut competing = MetadataMutation::new();
                competing.set_root(name, key);
                self.inner.commit(expected, competing).await?;
            }
            return match self.script.failure {
                Failure::Fenced => Err(MetadataError::MaintenanceFenced),
                Failure::Mixed if attempt.is_multiple_of(2) => {
                    Err(MetadataError::MaintenanceFenced)
                }
                Failure::Mixed => {
                    let result = self.inner.commit(expected, MetadataMutation::new()).await?;
                    Err(MetadataError::StaleRevision {
                        expected: *expected,
                        actual: result.revision,
                    })
                }
                Failure::CommitThenTransient => {
                    self.inner.commit(expected, mutation).await?;
                    Err(MetadataError::Transient("commit response lost".into()))
                }
            };
        }
        self.inner.commit(expected, mutation).await
    }
}

fn repository(
    failure: Failure,
    failures: usize,
    pause: Option<Arc<publication::MaintenancePause>>,
) -> Repository<MemoryBlobStore, RetryStore> {
    Repository::new(
        MemoryBlobStore::new(),
        RetryStore {
            inner: MemoryMetadataStore::new().unwrap(),
            script: Arc::new(Script {
                remaining: AtomicUsize::new(failures),
                attempts: AtomicUsize::new(0),
                failure,
                payload: Mutex::new(None),
                repoint: Mutex::new(None),
                pause,
            }),
        },
    )
}

#[tokio::test(start_paused = true)]
async fn maintenance_retries_keep_staging_and_exact_revision() {
    let repository = repository(Failure::Fenced, 3, None);
    let session = repository.mutation_session().await.unwrap();
    let object = session.stage_blob(b"protected retries").await.unwrap();
    *repository.state.script.payload.lock().unwrap() = Some(object.record().payload());
    let key = object.record().key().clone();
    let revision = repository.state.snapshot().await.unwrap().revision();
    session
        .publish_at_revision(revision, vec![object], vec![])
        .await
        .unwrap();
    assert_eq!(repository.state.script.attempts.load(Ordering::SeqCst), 4);
    assert!(
        repository
            .state
            .snapshot()
            .await
            .unwrap()
            .object(&key)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test(start_paused = true)]
async fn maintenance_and_stale_revision_share_one_attempt_budget() {
    let repository = repository(Failure::Mixed, usize::MAX, None);
    let session = repository.mutation_session().await.unwrap();
    let object = session.stage_blob(b"bounded retries").await.unwrap();
    *repository.state.script.payload.lock().unwrap() = Some(object.record().payload());
    assert!(matches!(
        session.publish_unrooted(vec![object]).await,
        Err(RepositoryError::Metadata(
            MetadataError::StaleRevision { .. }
        ))
    ));
    assert_eq!(repository.state.script.attempts.load(Ordering::SeqCst), 32);
    drop(session);
    crate::flush_repository_leases().await.unwrap();
    assert!(
        repository
            .state
            .pin_store()
            .await
            .unwrap()
            .inventory()
            .await
            .unwrap()
            .pins
            .is_empty()
    );
}

#[tokio::test(start_paused = true)]
async fn retry_window_does_not_start_an_attempt_after_its_deadline() {
    let mut budget = publication::PublicationRetry::new(crate::metadata::system_entropy());
    tokio::time::advance(std::time::Duration::from_secs(30)).await;
    assert!(!budget.wait().await);
}

#[tokio::test(start_paused = true)]
async fn ambiguous_commit_is_not_replayed() {
    let repository = repository(Failure::CommitThenTransient, 1, None);
    let session = repository.mutation_session().await.unwrap();
    let object = session.stage_blob(b"acknowledgment lost").await.unwrap();
    let key = object.record().key().clone();
    assert!(matches!(
        session.publish_unrooted(vec![object]).await,
        Err(RepositoryError::Metadata(MetadataError::Transient(_)))
    ));
    assert_eq!(repository.state.script.attempts.load(Ordering::SeqCst), 1);
    assert!(
        repository
            .state
            .snapshot()
            .await
            .unwrap()
            .object(&key)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test(start_paused = true)]
async fn maintenance_retry_rechecks_original_root_expectation() {
    let repository = repository(Failure::Fenced, 0, None);
    let session = repository.mutation_session().await.unwrap();
    let first = session.stage_blob(b"first").await.unwrap();
    let second = session.stage_blob(b"second").await.unwrap();
    let name = RootName::try_from("main").unwrap();
    let first_key = first.record().key().clone();
    let second_key = second.record().key().clone();
    session
        .publish(
            vec![first, second],
            vec![RootChange::Set {
                name: name.clone(),
                target: first_key.clone(),
            }],
        )
        .await
        .unwrap();
    repository.state.script.remaining.store(1, Ordering::SeqCst);
    *repository.state.script.repoint.lock().unwrap() = Some((name.clone(), second_key.clone()));
    let candidate = session.stage_blob(b"candidate").await.unwrap();
    let key = candidate.record().key().clone();
    let result = session
        .publish_if_roots_match(
            vec![candidate],
            vec![RootExpectation {
                name: name.clone(),
                target: Some(first_key.clone()),
            }],
            vec![RootChange::Set {
                name: name.clone(),
                target: key.clone(),
            }],
        )
        .await
        .unwrap();
    assert_eq!(
        result,
        ConditionalPublishResult::RootMismatch {
            name,
            expected: Some(first_key),
            actual: Some(second_key)
        }
    );
    assert_eq!(repository.state.script.attempts.load(Ordering::SeqCst), 2);
    assert!(
        repository
            .state
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
async fn cancelled_maintenance_attempt_keeps_protection_until_it_settles() {
    let pause = Arc::new(publication::MaintenancePause::default());
    let repository = Arc::new(repository(Failure::Fenced, 1, Some(pause.clone())));
    let owner = repository.clone();
    let task = tokio::spawn(async move {
        let session = owner.mutation_session().await.unwrap();
        let object = session.stage_blob(b"cancelled attempt").await.unwrap();
        *owner.state.script.payload.lock().unwrap() = Some(object.record().payload());
        session.publish_unrooted(vec![object]).await
    });
    pause.reached.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let pins = repository.state.pin_store().await.unwrap();
    assert!(!pins.inventory().await.unwrap().pins.is_empty());
    pause.resume.notify_one();
    crate::flush_repository_leases().await.unwrap();
    assert!(pins.inventory().await.unwrap().pins.is_empty());
    assert_eq!(repository.state.script.attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn collection_preserves_staged_payload_during_maintenance_refusal() {
    let pause = Arc::new(publication::MaintenancePause::default());
    let repository = Arc::new(repository(Failure::Fenced, 1, Some(pause.clone())));
    let owner = repository.clone();
    let task = tokio::spawn(async move {
        let session = owner.mutation_session().await.unwrap();
        let object = session.stage_blob(b"concurrent collection").await.unwrap();
        let key = object.record().key().clone();
        *owner.state.script.payload.lock().unwrap() = Some(object.record().payload());
        session
            .publish_rooted(
                vec![object],
                RootName::try_from("main").unwrap(),
                key.clone(),
            )
            .await
            .unwrap();
        key
    });
    pause.reached.notified().await;
    repository.collect().await.unwrap();
    pause.resume.notify_one();
    let key = task.await.unwrap();
    *repository.state.script.payload.lock().unwrap() = None;
    repository.collect().await.unwrap();
    let (_, mut read) = repository.open_payload(&key).await.unwrap().unwrap();
    let mut bytes = Vec::new();
    read.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"concurrent collection");
}

#[tokio::test(start_paused = true)]
async fn retry_jitter_uses_scoped_entropy_and_preserves_failure_fallback() {
    struct Jitter(bool);
    impl crate::metadata::EntropySource for Jitter {
        fn fill(&self, bytes: &mut [u8]) -> Result<(), MetadataError> {
            assert_eq!(bytes.len(), 2);
            if self.0 {
                bytes.copy_from_slice(&19_u16.to_le_bytes());
                Ok(())
            } else {
                Err(MetadataError::RevisionEntropy("injected failure".into()))
            }
        }
    }
    for (succeeds, expected_ms) in [(true, 20), (false, 1)] {
        let mut budget = publication::PublicationRetry::new(Arc::new(Jitter(succeeds)));
        let start = tokio::time::Instant::now();
        assert!(budget.wait().await);
        assert_eq!(
            start.elapsed(),
            std::time::Duration::from_millis(expected_ms)
        );
    }
}
