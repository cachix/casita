//! Completion must retire collector ownership without weakening recovery.
use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
struct ObservedStore {
    inner: MemoryPinStore,
    retired: AtomicBool,
    inventory_calls: AtomicUsize,
    finish_calls: AtomicUsize,
    fail_finish_once: AtomicBool,
    pause_ack: Mutex<Option<Arc<tokio::sync::Notify>>>,
}

#[async_trait::async_trait]
impl PinStore for ObservedStore {
    async fn inventory(&self) -> Result<PinInventory, MetadataError> {
        self.inventory_calls.fetch_add(1, Ordering::SeqCst);
        if self.retired.load(Ordering::SeqCst) {
            return Err(MetadataError::Backend(
                "collector accessed a retired backend after acknowledged completion".into(),
            ));
        }
        self.inner.inventory().await
    }
    async fn acquire_collection(
        &self,
        previous: Option<PinToken>,
    ) -> Result<Option<PinToken>, MetadataError> {
        self.inner.acquire_collection(previous).await
    }
    async fn finish_collection(&self, token: &PinToken) -> Result<(), MetadataError> {
        self.finish_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_finish_once.swap(false, Ordering::SeqCst) {
            return Err(MetadataError::Backend("finish was not acknowledged".into()));
        }
        self.inner.finish_collection(token).await?;
        let pause = self.pause_ack.lock().unwrap().take();
        if let Some(pause) = pause {
            pause.notified().await;
        }
        Ok(())
    }
    async fn register(&self, pin: DataPin) -> Result<Option<PinToken>, MetadataError> {
        self.inner.register(pin).await
    }
    async fn protect(
        &self,
        token: &PinToken,
        resources: BTreeSet<PinResource>,
    ) -> Result<bool, MetadataError> {
        self.inner.protect(token, resources).await
    }
    async fn release(&self, token: &PinToken) -> Result<(), MetadataError> {
        self.inner.release(token).await
    }
    async fn begin_prune(&self, revision: u64) -> Result<Option<PinToken>, MetadataError> {
        self.inner.begin_prune(revision).await
    }
    async fn finish_prune(&self, token: &PinToken) -> Result<(), MetadataError> {
        self.inner.finish_prune(token).await
    }
    async fn claim_deletions(
        &self,
        revision: u64,
        resources: BTreeSet<PinResource>,
    ) -> Result<Option<PinToken>, MetadataError> {
        self.inner.claim_deletions(revision, resources).await
    }
    async fn finish_deletions(&self, token: &PinToken) -> Result<(), MetadataError> {
        self.inner.finish_deletions(token).await
    }
}

async fn collector(store: &Arc<ObservedStore>) -> CollectorLease {
    CollectorLease::try_acquire(store.clone(), None)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn acknowledged_finish_permits_backend_teardown_without_later_io() {
    let store = Arc::new(ObservedStore::default());
    let lease = collector(&store).await;
    lease.finish().await.unwrap();
    assert!(store.inner.inventory().await.unwrap().collector.is_none());
    let before = store.inventory_calls.load(Ordering::SeqCst);
    store.retired.store(true, Ordering::SeqCst);
    drop(lease);
    let drained = crate::flush_repository_leases().await;
    assert!(
        drained.is_ok(),
        "completed collector queued cleanup: {drained:?}"
    );
    assert_eq!(store.inventory_calls.load(Ordering::SeqCst), before);
    assert_eq!(store.finish_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn unfinished_drop_releases_only_its_collector_and_preserves_live_pins() {
    let store = Arc::new(ObservedStore::default());
    let live = store
        .inner
        .register(DataPin {
            scope: PinScope::Staging,
            catalog: None,
            resources: BTreeSet::new(),
        })
        .await
        .unwrap()
        .unwrap();
    let lease = collector(&store).await;
    drop(lease);
    crate::flush_repository_leases().await.unwrap();
    let state = store.inner.inventory().await.unwrap();
    assert!(state.collector.is_none());
    assert!(state.pins.contains_key(&live));
    assert_eq!(store.finish_calls.load(Ordering::SeqCst), 1);
    store.inner.release(&live).await.unwrap();
}

#[tokio::test]
async fn unacknowledged_finish_keeps_drop_recovery_enabled() {
    let store = Arc::new(ObservedStore::default());
    let lease = collector(&store).await;
    let token = lease.token().clone();
    store.fail_finish_once.store(true, Ordering::SeqCst);
    assert!(lease.finish().await.is_err());
    assert_eq!(
        store.inner.inventory().await.unwrap().collector,
        Some(token)
    );
    drop(lease);
    crate::flush_repository_leases().await.unwrap();
    assert!(store.inner.inventory().await.unwrap().collector.is_none());
    assert_eq!(store.finish_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cancellation_before_finish_ack_keeps_inventory_recovery_enabled() {
    let store = Arc::new(ObservedStore::default());
    let lease = collector(&store).await;
    *store.pause_ack.lock().unwrap() = Some(Arc::new(tokio::sync::Notify::new()));
    let mut finishing = Box::pin(lease.finish());
    assert!(futures::poll!(&mut finishing).is_pending());
    assert!(store.inner.inventory().await.unwrap().collector.is_none());
    let before = store.inventory_calls.load(Ordering::SeqCst);
    drop(finishing);
    drop(lease);
    crate::flush_repository_leases().await.unwrap();
    assert_eq!(store.inventory_calls.load(Ordering::SeqCst), before + 1);
    assert_eq!(store.finish_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn unsettled_claims_survive_failed_finish_and_drop() {
    for deletion in [false, true] {
        let store = Arc::new(ObservedStore::default());
        let lease = collector(&store).await;
        let token = lease.token().clone();
        let revision = store.inner.inventory().await.unwrap().revision;
        let claim = if deletion {
            store
                .inner
                .claim_deletions(
                    revision,
                    BTreeSet::from([PinResource::StorageObject("unsettled".into())]),
                )
                .await
                .unwrap()
                .unwrap()
        } else {
            store.inner.begin_prune(revision).await.unwrap().unwrap()
        };
        assert!(lease.finish().await.is_err());
        let before = store.inner.inventory().await.unwrap();
        drop(lease);
        crate::flush_repository_leases().await.unwrap();
        let after = store.inner.inventory().await.unwrap();
        assert_eq!(after.collector, Some(token.clone()));
        assert_eq!(after.logical_prune, before.logical_prune);
        assert_eq!(after.deletions, before.deletions);
        assert_eq!(store.finish_calls.load(Ordering::SeqCst), 1);
        if deletion {
            store.inner.finish_deletions(&claim).await.unwrap();
        } else {
            store.inner.finish_prune(&claim).await.unwrap();
        }
        store.inner.finish_collection(&token).await.unwrap();
    }
}

#[tokio::test]
async fn stale_drop_cannot_finish_a_newer_collector() {
    let store = Arc::new(ObservedStore::default());
    let lease = collector(&store).await;
    let newer = store
        .inner
        .acquire_collection(Some(lease.token().clone()))
        .await
        .unwrap()
        .unwrap();
    drop(lease);
    crate::flush_repository_leases().await.unwrap();
    assert_eq!(
        store.inner.inventory().await.unwrap().collector,
        Some(newer.clone())
    );
    assert_eq!(store.finish_calls.load(Ordering::SeqCst), 0);
    store.inner.finish_collection(&newer).await.unwrap();
}
