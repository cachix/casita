//! Operation-owned pins and write lifetimes. Durable acquisition and storage
//! requests run to completion even when their receiving caller is cancelled.

#[cfg(test)]
#[path = "runtime/collector_regression_tests.rs"]
mod collector_regression_tests;

use super::*;
use crate::metadata::{run_lease_task, spawn_lease_task};
use futures::FutureExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, Weak};
use tracing::Instrument;

struct OwnedPin {
    token: PinToken,
    store: Arc<dyn PinStore>,
    confirmed: Mutex<BTreeSet<PinResource>>,
    present: Mutex<BTreeSet<PinResource>>,
    protection_gate: tokio::sync::Mutex<()>,
    pending_protection: Mutex<Vec<Weak<BTreeSet<PinResource>>>>,
    /// Resources from a batch the ledger refused. Later leaders leave them to
    /// their own requester, so one blocked waiter costs one edit per attempt
    /// instead of a refused edit for every unrelated caller queued with it.
    suspect_protection: Mutex<BTreeSet<PinResource>>,
}

type PendingRelease = futures::future::Shared<futures::future::BoxFuture<'static, ()>>;

type ReleaseQueues = Mutex<std::collections::HashMap<tokio::runtime::Id, Vec<PendingRelease>>>;

fn pending_releases() -> &'static ReleaseQueues {
    static RELEASES: std::sync::OnceLock<ReleaseQueues> = std::sync::OnceLock::new();
    RELEASES.get_or_init(Mutex::default)
}

fn queue_release(
    future: impl std::future::Future<Output = Result<(), MetadataError>> + Send + 'static,
) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        tracing::error!("pin ownership outlived its runtime; durable recovery is required");
        return;
    };
    let (send, receive) = tokio::sync::oneshot::channel();
    let completion = async move {
        let _ = receive.await;
    }
    .boxed()
    .shared();
    {
        let mut all = pending_releases().lock().unwrap();
        let pending = all.entry(runtime.id()).or_default();
        pending.retain(|release| release.clone().now_or_never().is_none());
        pending.push(completion);
    }
    spawn_lease_task(async move {
        let result = future.await;
        let _ = send.send(());
        result
    });
}

impl Drop for OwnedPin {
    fn drop(&mut self) {
        let store = self.store.clone();
        let token = self.token.clone();
        queue_release(async move {
            store.release(&token).await?;
            tracing::debug!(token = %token, process_id = std::process::id(), "online pin released");
            Ok(())
        }.instrument(tracing::debug_span!("pin.release")));
    }
}

/// Collector-only ownership. The surrounding operation must keep this lease
/// through all I/O. Unsettled prune/deletion claims survive a failed pass.
pub(crate) struct CollectorLease {
    store: Arc<dyn PinStore>,
    token: PinToken,
    finished: AtomicBool,
}

impl CollectorLease {
    pub(crate) fn token(&self) -> &PinToken {
        &self.token
    }
    pub(crate) async fn try_acquire(
        store: Arc<dyn PinStore>,
        previous: Option<PinToken>,
    ) -> Result<Option<Self>, MetadataError> {
        run_lease_task("collector admission", |send| async move {
            let result = store.acquire_collection(previous).await.map(|token| {
                token.map(|token| Self {
                    store,
                    token,
                    finished: AtomicBool::new(false),
                })
            });
            drop(send.send(result));
            Ok(())
        })
        .await?
    }

    pub(crate) async fn finish(&self) -> Result<(), MetadataError> {
        self.store.finish_collection(&self.token).await?;
        self.finished.store(true, Ordering::Release);
        Ok(())
    }
}

/// A deletion handle carrying the exact authority for an emergency sweep.
/// Its inventory remains truthful; only physical claim admission gains the
/// collector's permission to operate through its own logical fence.
pub(crate) struct PruningPinStore {
    pub(crate) inner: Arc<dyn PinStore>,
    pub(crate) collector: PinToken,
    pub(crate) prune: PinToken,
}

#[async_trait]
impl PinStore for PruningPinStore {
    async fn inventory(&self) -> Result<PinInventory, MetadataError> {
        self.inner.inventory().await
    }
    fn allows_deletion(&self, inventory: &PinInventory) -> bool {
        inventory.collector.as_ref() == Some(&self.collector)
            && inventory.logical_prune.as_ref() == Some(&self.prune)
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
        self.inner
            .claim_deletions_during_prune(revision, resources, &self.collector, &self.prune)
            .await
    }
    async fn finish_deletions(&self, token: &PinToken) -> Result<(), MetadataError> {
        self.inner.finish_deletions(token).await
    }
}

impl Drop for CollectorLease {
    fn drop(&mut self) {
        // Acknowledged completion has already retired this exact token. Keep
        // recovery enabled if finish failed or was cancelled before its reply.
        if self.finished.load(Ordering::Acquire) {
            return;
        }
        let store = self.store.clone();
        let token = self.token.clone();
        queue_release(async move {
            let inventory = store.inventory().await?;
            if inventory.collector.as_ref() != Some(&token) {
                return Ok(());
            }
            if inventory.logical_prune.is_some() || !inventory.deletions.is_empty() {
                // Recovery must settle these claims before discarding history.
                return Ok(());
            }
            store.finish_collection(&token).await
        });
    }
}

/// Settle already-queued data-pin and collector releases before taking a GC mark.
/// Live operations and releases queued later are not part of this barrier.
/// Concurrent collectors may await the same completion without consuming it.
pub(crate) async fn flush_pin_releases() {
    let pending = {
        let mut all = pending_releases().lock().unwrap();
        let Some(pending) = all.get_mut(&tokio::runtime::Handle::current().id()) else {
            return;
        };
        pending.retain(|release| release.clone().now_or_never().is_none());
        pending.clone()
    };
    futures::future::join_all(pending).await;
}

/// An operation's data pin. Clones share ownership; dropping the last clone
/// schedules exact-token release. Local read pins use process liveness; other
/// pins use durable records. Drain `flush_repository_leases` before
/// shutting down the runtime. A failed release preserves protection until
/// durable recovery or, for local read pins, owner exit.
#[derive(Clone)]
pub struct DataPinLease(Arc<OwnedPin>);

impl DataPinLease {
    /// Attempt admission without waiting for a conflicting prune or deletion.
    /// Acquisition itself settles after caller cancellation, then releases any
    /// pin the cancelled caller can no longer receive.
    pub async fn try_acquire(
        store: Arc<dyn PinStore>,
        pin: DataPin,
    ) -> Result<Option<Self>, MetadataError> {
        Self::try_acquire_kind(store, pin, false).await
    }

    pub(crate) async fn try_acquire_reader(
        store: Arc<dyn PinStore>,
        pin: DataPin,
    ) -> Result<Option<Self>, MetadataError> {
        Self::try_acquire_kind(store, pin, true).await
    }

    #[tracing::instrument(name = "pin.acquire", level = "debug", skip_all, fields(reader))]
    async fn try_acquire_kind(
        store: Arc<dyn PinStore>,
        pin: DataPin,
        reader: bool,
    ) -> Result<Option<Self>, MetadataError> {
        run_lease_task("pin acquisition", |send| async move {
            let registration = if reader {
                store.register_reader(pin.clone()).await
            } else {
                store.register(pin.clone()).await
            };
            let result = match registration {
                Ok(Some(token)) => {
                    let scope = match &pin.scope {
                        PinScope::Snapshot { .. } => "snapshot",
                        PinScope::Closures(_) => "closures",
                        PinScope::Staging => "staging",
                        PinScope::Metadata => "metadata",
                    };
                    tracing::debug!(token = %token, process_id = std::process::id(), scope,
                        "online pin acquired");
                    Ok(Some(Self(Arc::new(OwnedPin {
                        token,
                        store,
                        confirmed: Mutex::new(pin.resources),
                        present: Mutex::new(BTreeSet::new()),
                        protection_gate: tokio::sync::Mutex::new(()),
                        pending_protection: Mutex::new(Vec::new()),
                        suspect_protection: Mutex::new(BTreeSet::new()),
                    }))))
                }
                Ok(None) => Ok(None),
                Err(error) => Err(error),
            };
            drop(send.send(result));
            Ok(())
        })
        .await?
    }

    /// Wait for admission while allowing unrelated operations to proceed.
    pub async fn acquire(store: Arc<dyn PinStore>, pin: DataPin) -> Result<Self, MetadataError> {
        loop {
            if let Some(lease) = Self::try_acquire(store.clone(), pin.clone()).await? {
                return Ok(lease);
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    pub(crate) async fn acquire_reader(
        store: Arc<dyn PinStore>,
        pin: DataPin,
    ) -> Result<Self, MetadataError> {
        loop {
            if let Some(lease) = Self::try_acquire_reader(store.clone(), pin.clone()).await? {
                return Ok(lease);
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    pub fn token(&self) -> &PinToken {
        &self.0.token
    }

    /// Protect resources before exposing them to storage I/O. Repeated probes
    /// of already protected identities avoid durable ledger round trips.
    pub async fn protect(&self, resources: BTreeSet<PinResource>) -> Result<(), MetadataError> {
        while !self.try_protect(resources.clone()).await? {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        Ok(())
    }

    pub(crate) async fn try_protect(
        &self,
        resources: BTreeSet<PinResource>,
    ) -> Result<bool, MetadataError> {
        let missing = resources
            .difference(&self.0.confirmed.lock().unwrap())
            .cloned()
            .collect::<BTreeSet<_>>();
        if missing.is_empty() {
            return Ok(true);
        }
        let request = Arc::new(missing);
        {
            let mut pending = self.0.pending_protection.lock().unwrap();
            pending.retain(|request| request.strong_count() != 0);
            pending.push(Arc::downgrade(&request));
        }
        // Let already-ready sibling writes enqueue their identities before
        // the first caller starts a durable update. This adds no payload
        // buffering and never waits for another request or a timer.
        tokio::task::yield_now().await;
        // Waiters keep their own requests alive. Cancellation drops the weak
        // queue entry's owner; cancellation of this leader releases the gate
        // without losing other callers' requests or confirming unwritten data.
        let _guard = self.0.protection_gate.lock().await;
        let missing: BTreeSet<_> = request
            .difference(&self.0.confirmed.lock().unwrap())
            .cloned()
            .collect();
        if missing.is_empty() {
            return Ok(true);
        }
        let pending: BTreeSet<_> = self
            .0
            .pending_protection
            .lock()
            .unwrap()
            .iter()
            .filter_map(Weak::upgrade)
            .flat_map(|request| request.iter().cloned().collect::<Vec<_>>())
            .collect();
        let batch: BTreeSet<_> = {
            let confirmed = self.0.confirmed.lock().unwrap();
            let suspect = self.0.suspect_protection.lock().unwrap();
            pending
                .into_iter()
                .filter(|resource| {
                    !confirmed.contains(resource)
                        && (missing.contains(resource) || !suspect.contains(resource))
                })
                .collect()
        };
        if self.0.store.protect(&self.0.token, batch.clone()).await? {
            self.confirm(&batch);
            return Ok(true);
        }
        if batch == missing {
            self.0.suspect_protection.lock().unwrap().extend(batch);
            return Ok(false);
        }
        // A different request may overlap a deletion claim or exceed a limit.
        // It must not prevent an independently valid caller from proceeding,
        // and later leaders leave those resources to their own requester.
        // A ledger error is not such a refusal: retrying it here would only
        // spend a second retry budget while every waiter is held at the gate.
        self.0
            .suspect_protection
            .lock()
            .unwrap()
            .extend(batch.difference(&missing).cloned());
        let result = self.0.store.protect(&self.0.token, missing.clone()).await?;
        if result {
            self.confirm(&missing);
        } else {
            self.0.suspect_protection.lock().unwrap().extend(missing);
        }
        Ok(result)
    }

    fn confirm(&self, resources: &BTreeSet<PinResource>) {
        self.0
            .confirmed
            .lock()
            .unwrap()
            .extend(resources.iter().cloned());
        self.0
            .suspect_protection
            .lock()
            .unwrap()
            .retain(|resource| !resources.contains(resource));
    }
}

/// Active staging operations attached to a shared physical backend. Each write
/// captures strong ownership before its first await. Overlapping operations on
/// that backend conservatively share the resources they stage or reuse.
#[derive(Clone, Default)]
pub(crate) struct PinBindings(Arc<Mutex<Vec<Weak<OwnedPin>>>>);

tokio::task_local! {
    static BACKEND_WRITES: Vec<(PinBindings, WritePins)>;
}

/// Operation-specific write ownership for selected physical backends.
/// Mutation operations select their own staging pin; collection relies on its
/// durable collector ownership. Nested scopes affect only matching backends.
#[derive(Clone, Default)]
pub struct BackendWriteScope {
    bindings: Vec<(PinBindings, WritePins)>,
}

impl BackendWriteScope {
    pub(crate) fn with_pin(mut self, pin: DataPinLease) -> Self {
        for (_, pins) in &mut self.bindings {
            *pins = WritePins(vec![pin.clone()]);
        }
        self
    }
    pub(crate) fn include(mut self, other: Self) -> Self {
        self.bindings.extend(other.bindings);
        self
    }
    pub(crate) fn current() -> Self {
        Self {
            bindings: BACKEND_WRITES.try_with(Clone::clone).unwrap_or_default(),
        }
    }

    pub(crate) async fn run<F: std::future::Future>(self, future: F) -> F::Output {
        let mut bindings = Self::current().bindings;
        bindings.extend(self.bindings);
        BACKEND_WRITES.scope(bindings, future).await
    }
}

impl std::fmt::Debug for PinBindings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinBindings").finish_non_exhaustive()
    }
}

impl PinBindings {
    pub(crate) fn write_scope(&self) -> BackendWriteScope {
        BackendWriteScope {
            bindings: vec![(self.clone(), WritePins::default())],
        }
    }
    pub(crate) fn attach(&self, pin: &DataPinLease) {
        let mut active = self.0.lock().unwrap();
        active.retain(|pin| pin.strong_count() != 0);
        if !active
            .iter()
            .any(|entry| entry.ptr_eq(&Arc::downgrade(&pin.0)))
        {
            active.push(Arc::downgrade(&pin.0));
        }
    }

    pub(crate) fn capture(&self) -> WritePins {
        if let Some(pins) = BACKEND_WRITES
            .try_with(|bindings| {
                bindings
                    .iter()
                    .rev()
                    .find(|(binding, _)| Arc::ptr_eq(&binding.0, &self.0))
                    .map(|(_, pins)| pins.clone())
            })
            .ok()
            .flatten()
        {
            return pins;
        }
        let mut active = self.0.lock().unwrap();
        let pins = active
            .iter()
            .filter_map(Weak::upgrade)
            .map(DataPinLease)
            .collect();
        active.retain(|pin| pin.strong_count() != 0);
        WritePins(pins)
    }
}

/// Strong pin ownership for an individual stream or submitted storage request.
#[derive(Clone, Default)]
pub(crate) struct WritePins(Vec<DataPinLease>);

impl std::fmt::Debug for WritePins {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WritePins")
            .field("owners", &self.0.len())
            .finish()
    }
}

impl WritePins {
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn known_present(&self, resource: &PinResource) -> bool {
        !self.0.is_empty()
            && self
                .0
                .iter()
                .all(|pin| pin.0.present.lock().unwrap().contains(resource))
    }

    pub(crate) fn remember_present(&self, resource: PinResource) {
        for pin in &self.0 {
            pin.0.present.lock().unwrap().insert(resource.clone());
        }
    }
    pub(crate) async fn protect(&self, resources: BTreeSet<PinResource>) -> std::io::Result<()> {
        for pin in &self.0 {
            pin.protect(resources.clone())
                .await
                .map_err(std::io::Error::other)?;
        }
        Ok(())
    }

    /// Keep pin ownership until the storage request settles, including a local
    /// blocking write or a remote request that outlives its caller.
    pub(crate) fn write<T: Send + 'static>(
        self,
        resources: BTreeSet<PinResource>,
        operation: impl std::future::Future<Output = std::io::Result<T>> + Send + 'static,
    ) -> futures::future::BoxFuture<'static, std::io::Result<T>> {
        self.run(resources, operation, std::convert::identity)
    }

    pub(crate) fn run<T: Send + 'static, E: Send + 'static>(
        self,
        resources: BTreeSet<PinResource>,
        operation: impl std::future::Future<Output = Result<T, E>> + Send + 'static,
        failure: fn(std::io::Error) -> E,
    ) -> futures::future::BoxFuture<'static, Result<T, E>> {
        Box::pin(async move {
            if self.0.is_empty() {
                return operation.await;
            }
            let (send, receive) = tokio::sync::oneshot::channel();
            spawn_lease_task(async move {
                let result = match self.protect(resources).await {
                    Ok(()) => operation.await,
                    Err(error) => Err(failure(error)),
                };
                drop(send.send(result));
                drop(self);
                Ok(())
            });
            receive
                .await
                .map_err(|error| failure(std::io::Error::other(error)))?
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::flush_repository_leases;

    fn staging() -> DataPin {
        DataPin {
            scope: PinScope::Staging,
            catalog: None,
            resources: BTreeSet::new(),
        }
    }
    fn path(path: &str) -> BTreeSet<PinResource> {
        BTreeSet::from([PinResource::StorageObject(path.into())])
    }

    #[tokio::test]
    async fn ready_sibling_protections_coalesce_without_an_existing_gate_holder() {
        for count in [1, 15, 16, 17] {
            let store = Arc::new(MemoryPinStore::default());
            let pin = DataPinLease::acquire(store.clone(), staging())
                .await
                .unwrap();
            let before = store.inventory().await.unwrap().revision;
            let requests =
                (0..count).map(|index| pin.try_protect(path(&format!("object-{index}"))));
            for result in futures::future::join_all(requests).await {
                assert!(result.unwrap());
            }
            let inventory = store.inventory().await.unwrap();
            assert_eq!(inventory.revision, before + 1);
            assert_eq!(inventory.pins[pin.token()].resources.len(), count);
            assert!(matches!(
                pin.try_protect(path("object-0")).now_or_never(),
                Some(Ok(true))
            ));
            drop(pin);
            flush_repository_leases().await.unwrap();
        }
    }

    #[tokio::test]
    async fn concurrent_protection_is_one_durable_update_and_ignores_cancelled_waiters() {
        let store = Arc::new(MemoryPinStore::default());
        let pin = DataPinLease::acquire(store.clone(), staging())
            .await
            .unwrap();
        let before = store.inventory().await.unwrap().revision;
        let guard = pin.0.protection_gate.lock().await;
        let mut cancelled = Box::pin(pin.try_protect(path("cancelled")));
        assert!(futures::poll!(&mut cancelled).is_pending());
        drop(cancelled);
        let mut requests: Vec<_> = (0..64)
            .map(|index| Box::pin(pin.try_protect(path(&format!("object-{index}")))))
            .collect();
        for request in &mut requests {
            assert!(futures::poll!(request).is_pending());
        }
        drop(guard);
        for result in futures::future::join_all(requests).await {
            assert!(result.unwrap());
        }
        let inventory = store.inventory().await.unwrap();
        assert_eq!(inventory.revision, before + 1);
        let expected = (0..64)
            .flat_map(|index| path(&format!("object-{index}")))
            .collect();
        assert_eq!(inventory.pins[pin.token()].resources, expected);
        drop(pin);
        flush_repository_leases().await.unwrap();
    }

    #[tokio::test]
    async fn batched_conflict_does_not_block_an_unrelated_protection() {
        let store = Arc::new(MemoryPinStore::default());
        let pin = DataPinLease::acquire(store.clone(), staging())
            .await
            .unwrap();
        let revision = store.inventory().await.unwrap().revision;
        let claim = store
            .claim_deletions(revision, path("busy"))
            .await
            .unwrap()
            .unwrap();
        let guard = pin.0.protection_gate.lock().await;
        let mut free = Box::pin(pin.try_protect(path("free")));
        let mut busy = Box::pin(pin.try_protect(path("busy")));
        assert!(futures::poll!(&mut free).is_pending());
        assert!(futures::poll!(&mut busy).is_pending());
        drop(guard);
        let (free, busy) = tokio::join!(free, busy);
        assert!(free.unwrap());
        assert!(!busy.unwrap());
        assert_eq!(
            store.inventory().await.unwrap().pins[pin.token()].resources,
            path("free")
        );
        store.finish_deletions(&claim).await.unwrap();
        drop(pin);
        flush_repository_leases().await.unwrap();
    }

    /// A ledger that records each protection edit and, on request, fails
    /// every batched one.
    #[derive(Default)]
    struct RecordingStore {
        inner: MemoryPinStore,
        calls: Mutex<Vec<BTreeSet<PinResource>>>,
        fail_batches: bool,
    }

    #[async_trait::async_trait]
    impl PinStore for RecordingStore {
        async fn inventory(&self) -> Result<PinInventory, MetadataError> {
            self.inner.inventory().await
        }
        async fn register(&self, pin: DataPin) -> Result<Option<PinToken>, MetadataError> {
            self.inner.register(pin).await
        }
        async fn protect(
            &self,
            token: &PinToken,
            resources: BTreeSet<PinResource>,
        ) -> Result<bool, MetadataError> {
            self.calls.lock().unwrap().push(resources.clone());
            if self.fail_batches && resources.len() > 1 {
                return Err(MetadataError::Transient("ledger unavailable".into()));
            }
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

    #[tokio::test]
    async fn batched_ledger_error_is_returned_without_a_second_attempt() {
        let store = Arc::new(RecordingStore {
            fail_batches: true,
            ..RecordingStore::default()
        });
        let pin = DataPinLease::acquire(store.clone(), staging())
            .await
            .unwrap();
        let guard = pin.0.protection_gate.lock().await;
        let mut first = Box::pin(pin.try_protect(path("first")));
        let mut second = Box::pin(pin.try_protect(path("second")));
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        drop(guard);
        let (first, second) = tokio::join!(first, second);
        // The leader's batch failed, and that error is its answer; only the
        // waiter, now alone, protects its own resource.
        let (failed, protected) = match (first, second) {
            (Err(error), Ok(true)) | (Ok(true), Err(error)) => (error, true),
            other => panic!("{other:?}"),
        };
        assert!(matches!(failed, MetadataError::Transient(_)));
        assert!(protected);
        let calls = store.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 2, "{calls:?}");
        assert_eq!(calls[0].len(), 2);
        assert_eq!(calls[1].len(), 1);
        assert_eq!(
            store.inventory().await.unwrap().pins[pin.token()].resources,
            calls[1]
        );
        drop(pin);
        flush_repository_leases().await.unwrap();
    }

    #[tokio::test]
    async fn refused_waiter_is_left_out_of_later_batches_until_it_succeeds() {
        let store = Arc::new(RecordingStore::default());
        let pin = DataPinLease::acquire(store.clone(), staging())
            .await
            .unwrap();
        let revision = store.inventory().await.unwrap().revision;
        let claim = store
            .claim_deletions(revision, path("busy"))
            .await
            .unwrap()
            .unwrap();
        // The blocked writer's first attempt is refused on its own, and the
        // lease remembers its resource as suspect.
        assert!(!pin.try_protect(path("busy")).await.unwrap());
        assert_eq!(*pin.0.suspect_protection.lock().unwrap(), path("busy"));
        // The writer retries while an unrelated caller is ahead of it at the
        // gate. That leader unions the queue, but leaves the suspect resource
        // out, so its edit is accepted on the first attempt.
        let guard = pin.0.protection_gate.lock().await;
        let mut free = Box::pin(pin.try_protect(path("free")));
        let mut busy = Box::pin(pin.try_protect(path("busy")));
        assert!(futures::poll!(&mut free).is_pending());
        assert!(futures::poll!(&mut busy).is_pending());
        drop(guard);
        let (free, busy) = tokio::join!(free, busy);
        assert!(free.unwrap());
        assert!(!busy.unwrap());
        let calls = store.calls.lock().unwrap().clone();
        assert_eq!(
            calls,
            vec![path("busy"), path("free"), path("busy")],
            "{calls:?}"
        );
        // Once the claim settles the writer's own edit succeeds and the
        // resource is no longer suspect.
        store.finish_deletions(&claim).await.unwrap();
        assert!(pin.try_protect(path("busy")).await.unwrap());
        assert!(pin.0.suspect_protection.lock().unwrap().is_empty());
        assert_eq!(
            store.inventory().await.unwrap().pins[pin.token()].resources,
            path("free").union(&path("busy")).cloned().collect()
        );
        drop(pin);
        flush_repository_leases().await.unwrap();
    }

    #[tokio::test]
    async fn misattributed_suspects_still_protect_through_their_own_requester() {
        let store = Arc::new(RecordingStore::default());
        let pin = DataPinLease::acquire(store.clone(), staging())
            .await
            .unwrap();
        let revision = store.inventory().await.unwrap().revision;
        let claim = store
            .claim_deletions(revision, path("busy"))
            .await
            .unwrap()
            .unwrap();
        // The blocked writer leads with an unrelated request queued behind
        // it. The union is refused, so the innocent resource is marked
        // suspect along with the claimed one.
        let guard = pin.0.protection_gate.lock().await;
        let mut busy = Box::pin(pin.try_protect(path("busy")));
        let mut free = Box::pin(pin.try_protect(path("free")));
        assert!(futures::poll!(&mut busy).is_pending());
        assert!(futures::poll!(&mut free).is_pending());
        drop(guard);
        let (busy, free) = tokio::join!(busy, free);
        assert!(!busy.unwrap());
        assert!(free.unwrap());
        let calls = store.calls.lock().unwrap().clone();
        assert_eq!(
            calls,
            vec![
                path("busy").union(&path("free")).cloned().collect(),
                path("busy"),
                path("free"),
            ],
            "{calls:?}"
        );
        // Its requester led its own edit, which lifts the suspicion.
        assert_eq!(*pin.0.suspect_protection.lock().unwrap(), path("busy"));
        store.finish_deletions(&claim).await.unwrap();
        drop(pin);
        flush_repository_leases().await.unwrap();
    }

    #[tokio::test]
    async fn nested_writer_scope_selects_its_owner_and_restores_the_parent() {
        let store = Arc::new(MemoryPinStore::default());
        let first = DataPinLease::acquire(store.clone(), staging())
            .await
            .unwrap();
        let second = DataPinLease::acquire(store.clone(), staging())
            .await
            .unwrap();
        let bindings = PinBindings::default();
        bindings.attach(&first);
        bindings.attach(&second);
        bindings
            .write_scope()
            .with_pin(first.clone())
            .run(async {
                bindings
                    .capture()
                    .write(path("first-before"), async { Ok(()) })
                    .await
                    .unwrap();
                bindings
                    .write_scope()
                    .with_pin(second.clone())
                    .run(async {
                        bindings
                            .capture()
                            .write(path("second"), async { Ok(()) })
                            .await
                            .unwrap();
                    })
                    .await;
                bindings
                    .capture()
                    .write(path("first-after"), async { Ok(()) })
                    .await
                    .unwrap();
            })
            .await;
        let inventory = store.inventory().await.unwrap();
        let mut expected = path("first-before");
        expected.extend(path("first-after"));
        assert_eq!(inventory.pins[first.token()].resources, expected);
        assert_eq!(inventory.pins[second.token()].resources, path("second"));
        drop((first, second));
        flush_repository_leases().await.unwrap();
    }

    #[tokio::test]
    async fn write_scope_is_backend_specific_and_inherited_by_tracked_tasks() {
        let store = Arc::new(MemoryPinStore::default());
        let pin = DataPinLease::acquire(store.clone(), staging())
            .await
            .unwrap();
        let bindings = PinBindings::default();
        bindings.attach(&pin);
        let other_store = Arc::new(MemoryPinStore::default());
        let other_pin = DataPinLease::acquire(other_store.clone(), staging())
            .await
            .unwrap();
        let other = PinBindings::default();
        other.attach(&other_pin);
        let collector = store
            .begin_collection(store.inventory().await.unwrap().revision, None)
            .await
            .unwrap()
            .unwrap();
        let prune = store
            .begin_prune(store.inventory().await.unwrap().revision)
            .await
            .unwrap()
            .unwrap();
        let scoped = bindings.clone();
        let (send, receive) = tokio::sync::oneshot::channel();
        bindings
            .write_scope()
            .run(async move {
                assert!(scoped.capture().is_empty());
                assert!(!other.capture().is_empty());
                other
                    .capture()
                    .write(path("other-backend"), async { Ok(()) })
                    .await
                    .unwrap();
                spawn_lease_task(async move {
                    // This must inherit across another tracked task as well.
                    spawn_lease_task(async move {
                        scoped
                            .capture()
                            .write(path("collector-output"), async { Ok(()) })
                            .await
                            .unwrap();
                        send.send(()).unwrap();
                        Ok(())
                    });
                    Ok(())
                });
            })
            .await;
        tokio::time::timeout(std::time::Duration::from_secs(5), receive)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !bindings.capture().is_empty(),
            "the scope must not change the shared registry"
        );
        assert!(
            store.inventory().await.unwrap().pins[pin.token()]
                .resources
                .is_empty()
        );
        assert_eq!(
            other_store.inventory().await.unwrap().pins[other_pin.token()].resources,
            path("other-backend")
        );
        store.finish_prune(&prune).await.unwrap();
        store.finish_collection(&collector).await.unwrap();
        drop((pin, other_pin));
        flush_repository_leases().await.unwrap();
    }

    #[tokio::test]
    async fn last_owner_releases_only_its_pin() {
        let store = Arc::new(MemoryPinStore::default());
        let first = DataPinLease::acquire(store.clone(), staging())
            .await
            .unwrap();
        let first_token = first.token().clone();
        let survivor = first.clone();
        let other = DataPinLease::acquire(store.clone(), staging())
            .await
            .unwrap();
        drop(first);
        flush_repository_leases().await.unwrap();
        assert_eq!(store.inventory().await.unwrap().pins.len(), 2);
        drop(survivor);
        flush_repository_leases().await.unwrap();
        let inventory = store.inventory().await.unwrap();
        assert!(!inventory.pins.contains_key(&first_token));
        assert!(inventory.pins.contains_key(other.token()));
        drop(other);
        flush_repository_leases().await.unwrap();
    }

    #[tokio::test]
    async fn a_cancelled_writer_keeps_its_pin_until_storage_settles() {
        let store = Arc::new(MemoryPinStore::default());
        let pin = DataPinLease::acquire(store.clone(), staging())
            .await
            .unwrap();
        let bindings = PinBindings::default();
        bindings.attach(&pin);
        let writes = bindings.capture();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        let writer = tokio::spawn(writes.write(path("upload"), async move {
            entered.send(()).unwrap();
            resumed.await.unwrap();
            Ok(())
        }));
        started.await.unwrap();
        writer.abort();
        assert!(writer.await.unwrap_err().is_cancelled());
        drop(pin);
        let inventory = store.inventory().await.unwrap();
        assert_eq!(inventory.pins.len(), 1);
        assert!(
            store
                .claim_deletions(inventory.revision, path("upload"))
                .await
                .unwrap()
                .is_none()
        );
        let deletion = store
            .claim_deletions(inventory.revision, path("unrelated"))
            .await
            .unwrap()
            .unwrap();
        store.finish_deletions(&deletion).await.unwrap();
        let released = DataPinLease::acquire(store.clone(), staging())
            .await
            .unwrap();
        drop(released);
        tokio::time::timeout(std::time::Duration::from_secs(1), flush_pin_releases())
            .await
            .expect("a release barrier must not wait for the live upload");
        assert_eq!(store.inventory().await.unwrap().pins.len(), 1);
        resume.send(()).unwrap();
        flush_repository_leases().await.unwrap();
        assert!(store.inventory().await.unwrap().pins.is_empty());
    }

    #[tokio::test]
    async fn storage_is_not_called_until_a_conflicting_deletion_settles() {
        let store = Arc::new(MemoryPinStore::default());
        let pin = DataPinLease::acquire(store.clone(), staging())
            .await
            .unwrap();
        let inventory = store.inventory().await.unwrap();
        let deletion = store
            .claim_deletions(inventory.revision, path("same"))
            .await
            .unwrap()
            .unwrap();
        let bindings = PinBindings::default();
        bindings.attach(&pin);
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let operation_called = called.clone();
        let write = tokio::spawn(bindings.capture().write(path("same"), async move {
            operation_called.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }));
        // The pin ledger proves exclusion, rather than relying on this task
        // being polled before the assertion.
        assert!(!store.protect(pin.token(), path("same")).await.unwrap());
        assert!(!called.load(std::sync::atomic::Ordering::SeqCst));
        store.finish_deletions(&deletion).await.unwrap();
        write.await.unwrap().unwrap();
        assert!(called.load(std::sync::atomic::Ordering::SeqCst));
        drop(pin);
        flush_repository_leases().await.unwrap();
    }
}
