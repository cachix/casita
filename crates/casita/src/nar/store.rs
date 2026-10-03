//! Private local associations, kept in the metadata backend's verification
//! facts beside its validated-closure marks. They are not an application
//! metadata namespace, a portable object, or a GC root.
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::Mutex as AsyncMutex;

use super::{Facts, NarError};
use crate::metadata::{FactsEdit, MetadataError, VerificationFacts};

type Flights = Mutex<BTreeMap<Vec<u8>, Weak<AsyncMutex<()>>>>;

const GENERATION: &[u8] = b"nar-invalidation-generation";
const AUDIT_MARKER: &[u8] = b"require-native-audit";
/// Entries under this prefix hold the payload store's availability witness
/// for one identity: what an earlier walk found every payload living in.
pub(super) const WITNESS_PREFIX: &[u8] = b"casita-nar-witness-v1/";

fn witness_key(identity: &[u8]) -> Vec<u8> {
    [WITNESS_PREFIX, identity].concat()
}

pub(crate) struct NarStore {
    facts: Arc<dyn VerificationFacts>,
    pending_invalidations: AtomicU64,
    invalidations: AsyncMutex<()>,
    flights: Arc<Flights>,
}

impl NarStore {
    /// Handles whose facts share a scope coalesce in-flight measurements.
    /// Only the flight locks are shared: every handle reads and writes
    /// through its own backend, so a repository recreated at the same path
    /// never touches a predecessor's unlinked database that an older report
    /// still holds.
    pub(crate) fn new(facts: Arc<dyn VerificationFacts>) -> Arc<Self> {
        let flights = match facts.scope() {
            Some(path) => shared_flights(path),
            None => Arc::default(),
        };
        Arc::new(Self {
            facts,
            pending_invalidations: AtomicU64::new(0),
            invalidations: AsyncMutex::new(()),
            flights,
        })
    }
    #[cfg(test)]
    pub(crate) fn memory() -> Arc<Self> {
        Self::new(Arc::new(crate::metadata::MemoryVerificationFacts::default()))
    }
    // Weak entries are pruned on admission. Only active callers own locks;
    // unrelated identities never wait behind an archive being hashed.
    pub(super) fn flight(&self, key: &[u8]) -> Arc<AsyncMutex<()>> {
        let mut flights = self.flights.lock().unwrap();
        flights.retain(|_, value| value.strong_count() != 0);
        if let Some(lock) = flights.get(key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(AsyncMutex::new(()));
        flights.insert(key.to_vec(), Arc::downgrade(&lock));
        lock
    }
    fn invalidation_pending(&self) -> bool {
        self.pending_invalidations.load(Ordering::Acquire) != 0
    }
    pub(super) async fn get(&self, key: &[u8]) -> Result<Option<Facts>, NarError> {
        if key != AUDIT_MARKER && self.invalidation_pending() {
            return Ok(None);
        }
        let bytes = self.facts.get(key).await.map_err(NarError::storage)?;
        bytes.map(|bytes| Facts::decode(&bytes)).transpose()
    }
    /// The payload store's witness from the last complete availability walk
    /// of `identity`, if it left a reusable one. Invalidation clears it with
    /// the facts.
    pub(super) async fn witness(&self, identity: &[u8]) -> Result<Option<Vec<u8>>, NarError> {
        if self.invalidation_pending() {
            return Ok(None);
        }
        let witness = self
            .facts
            .get(&witness_key(identity))
            .await
            .map_err(NarError::storage)?;
        Ok(witness.filter(|witness| !witness.is_empty()))
    }
    pub(super) async fn set_witness(
        &self,
        identity: &[u8],
        witness: Vec<u8>,
    ) -> Result<(), NarError> {
        let key = witness_key(identity);
        let edit: FactsEdit = Box::new({
            let key = key.clone();
            move |_| vec![(key, Some(witness))]
        });
        self.facts
            .edit(vec![key], edit)
            .await
            .map_err(NarError::storage)
    }
    /// Invalidations so far. Capture it before checking the audit marker;
    /// `merge` and `restore` refuse once it has advanced.
    pub(crate) async fn generation(&self) -> Result<u64, MetadataError> {
        // A failed or cancelled clear leaves this handle closed. Finish it
        // before starting new verification, so its results use a generation
        // that supersedes every audit begun before the failure. Clearing only
        // the local flag could revive the associations we failed to delete.
        if self.invalidation_pending() {
            let _guard = self.invalidations.lock().await;
            self.finish_invalidation().await?;
        }
        let value = self.facts.get(GENERATION).await?;
        crate::metadata::facts_generation(value.as_deref())
    }
    /// Record facts, unless the audit marker is set and an invalidation
    /// landed after `generation` was captured: then nothing is written and
    /// `None` says the stored representation must verify again first.
    pub(super) async fn merge(
        &self,
        key: &[u8],
        facts: &Facts,
        generation: u64,
    ) -> Result<Option<Facts>, NarError> {
        // The marker and persisted generation reads and this write are one
        // atomic edit. An invalidation through any handle either precedes
        // the edit or deletes this row afterwards.
        let outcome = Arc::new(Mutex::new(None));
        let edit: FactsEdit = Box::new({
            let outcome = outcome.clone();
            let key = key.to_vec();
            let facts = facts.clone();
            move |values| {
                let mut values = values.into_iter();
                let old = values.next().flatten();
                let marker = values.next().flatten().is_some();
                let current =
                    match crate::metadata::facts_generation(values.next().flatten().as_deref()) {
                        Ok(current) => current,
                        Err(error) => {
                            *outcome.lock().unwrap() = Some(Err(NarError::storage(error)));
                            return Vec::new();
                        }
                    };
                if marker && current != generation {
                    *outcome.lock().unwrap() = Some(Ok(None));
                    return Vec::new();
                }
                let merged = merge(old.as_deref(), facts);
                // A conflict is a durable tombstone, never an overwrite of
                // one measured digest with another.
                let bytes = merged.as_ref().map(Facts::encode).unwrap_or_default();
                *outcome.lock().unwrap() = Some(merged.map(Some));
                vec![(key, Some(bytes))]
            }
        });
        self.facts
            .edit(
                vec![key.to_vec(), AUDIT_MARKER.to_vec(), GENERATION.to_vec()],
                edit,
            )
            .await
            .map_err(NarError::storage)?;
        let outcome = outcome.lock().unwrap().take();
        outcome.expect("the facts edit ran")
    }
    /// Invalidate all associations after a known read failure. Conservative
    /// repository-wide invalidation avoids maintaining a reverse closure index.
    pub(crate) async fn invalidate(&self) -> Result<(), NarError> {
        // Register before the first await: cancellation while waiting for the
        // lock must not discard a corruption notification.
        self.pending_invalidations.fetch_add(1, Ordering::AcqRel);
        let _guard = self.invalidations.lock().await;
        self.finish_invalidation().await.map_err(NarError::storage)
    }
    /// Called with `invalidations` held. Only acknowledge notifications seen
    /// before each clear started. Later ones need another clear, even if their
    /// callers were cancelled. Failure or cancellation leaves the current batch
    /// pending, including when its clear may already have committed.
    async fn finish_invalidation(&self) -> Result<(), MetadataError> {
        loop {
            let pending = self.pending_invalidations.load(Ordering::Acquire);
            if pending == 0 {
                return Ok(());
            }
            self.facts
                .clear(AUDIT_MARKER.to_vec(), GENERATION.to_vec())
                .await?;
            self.pending_invalidations
                .fetch_sub(pending, Ordering::AcqRel);
        }
    }
    /// Invalidate after a damaged payload read. A failed invalidation keeps
    /// this handle failing closed and is logged; it never replaces the read
    /// error that callers must see.
    pub(crate) async fn record_read_failure(&self) {
        if let Err(cause) = self.invalidate().await {
            tracing::warn!(
                error = %cause,
                "NAR association invalidation failed after a damaged payload read"
            );
        }
    }
    /// Drop the native-audit requirement after a complete physical audit found
    /// every payload intact. Facts and conflict tombstones are untouched: a
    /// clean audit proves stored bytes still match their digests.
    ///
    /// `generation` is what the audit captured before it started reading. A
    /// read that failed while the audit ran invalidated after that point,
    /// and its marker outlives the audit: the requirement stays and `false`
    /// is returned.
    pub(crate) async fn restore(&self, generation: u64) -> Result<bool, NarError> {
        let _guard = self.invalidations.lock().await;
        // A failed or cancelled invalidation may not have advanced the durable
        // generation. Do not let an older audit clear this handle's failure.
        if self.invalidation_pending() {
            return Ok(false);
        }
        let restored = Arc::new(Mutex::new(Ok(false)));
        self.facts
            .edit(
                vec![GENERATION.to_vec()],
                Box::new({
                    let restored = restored.clone();
                    move |values| match crate::metadata::facts_generation(values[0].as_deref()) {
                        Ok(current) if current == generation => {
                            *restored.lock().unwrap() = Ok(true);
                            vec![(AUDIT_MARKER.to_vec(), None)]
                        }
                        Ok(_) => Vec::new(),
                        Err(error) => {
                            *restored.lock().unwrap() = Err(NarError::storage(error));
                            Vec::new()
                        }
                    }
                }),
            )
            .await
            .map_err(NarError::storage)?;
        let restored = std::mem::replace(&mut *restored.lock().unwrap(), Ok(false))?;
        Ok(restored)
    }
    pub(super) async fn requires_native_audit(&self) -> Result<bool, NarError> {
        if self.invalidation_pending() {
            return Ok(true);
        }
        match self.get(AUDIT_MARKER).await {
            Err(NarError::Conflict) => Ok(true),
            Err(error) => Err(error),
            Ok(_) => Ok(false),
        }
    }
    pub(super) async fn page(
        &self,
        after: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>, NarError> {
        self.facts
            .page(after, limit)
            .await
            .map_err(NarError::storage)
    }
    /// Remove one association; a conflict tombstone stays.
    pub(super) async fn remove(&self, key: Vec<u8>) -> Result<(), NarError> {
        let edit: FactsEdit = Box::new({
            let key = key.clone();
            move |values| match values.first() {
                Some(Some(value)) if !value.is_empty() => vec![(key, None)],
                _ => Vec::new(),
            }
        });
        self.facts
            .edit(vec![key], edit)
            .await
            .map_err(NarError::storage)
    }
    pub(super) async fn quarantine(&self, key: &[u8]) -> Result<(), NarError> {
        let key = key.to_vec();
        let edit: FactsEdit = Box::new({
            let key = key.clone();
            move |_| vec![(key, Some(Vec::new()))]
        });
        self.facts
            .edit(vec![key], edit)
            .await
            .map_err(NarError::storage)
    }
}

/// Flight locks shared by every handle open on one local path.
fn shared_flights(path: std::path::PathBuf) -> Arc<Flights> {
    static FLIGHTS: std::sync::OnceLock<Mutex<BTreeMap<std::path::PathBuf, Weak<Flights>>>> =
        std::sync::OnceLock::new();
    let path = std::fs::canonicalize(&path).unwrap_or(path);
    let mut registry = FLIGHTS.get_or_init(Mutex::default).lock().unwrap();
    registry.retain(|_, flights| flights.strong_count() != 0);
    registry
        .get(&path)
        .and_then(Weak::upgrade)
        .unwrap_or_else(|| {
            let flights = Arc::<Flights>::default();
            registry.insert(path, Arc::downgrade(&flights));
            flights
        })
}

fn merge(old: Option<&[u8]>, facts: Facts) -> Result<Facts, NarError> {
    let Some(old) = old else {
        return Ok(facts);
    };
    let mut old = Facts::decode(old)?;
    if old.size != facts.size {
        return Err(NarError::Conflict);
    }
    for (key, value) in facts.values {
        if old
            .values
            .get(&key)
            .is_some_and(|previous| *previous != value)
        {
            return Err(NarError::Conflict);
        }
        old.values.insert(key, value);
    }
    Ok(old)
}

/// Finish durable invalidation before delivering a damaged read's error. The
/// background task continues if the caller cancels while invalidation waits.
/// Transient and unrelated backend errors are delivered without touching the
/// associations: they say nothing about stored bytes.
#[derive(Default)]
pub(crate) struct ReadHealth {
    store: Option<Arc<NarStore>>,
    pending: Option<futures::future::BoxFuture<'static, std::io::Result<()>>>,
}
impl ReadHealth {
    pub(crate) fn new(store: Option<Arc<NarStore>>) -> Self {
        Self {
            store,
            pending: None,
        }
    }
    /// Deliver queued invalidation before polling the payload again. Damaged
    /// reads invalidate associations before their error reaches the caller.
    pub(crate) fn poll_read<R: AsyncRead + Unpin + ?Sized>(
        &mut self,
        reader: &mut R,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if let Some(result) = self.poll(cx) {
            return result;
        }
        match Pin::new(reader).poll_read(cx, buffer) {
            Poll::Ready(Err(error)) => {
                self.failed(error);
                self.poll(cx).expect("queued invalidation")
            }
            other => other,
        }
    }
    pub(crate) fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Option<std::task::Poll<std::io::Result<()>>> {
        let pending = self.pending.as_mut()?;
        let result = pending.as_mut().poll(cx);
        if result.is_ready() {
            self.pending = None;
        }
        Some(result)
    }
    /// Forget an undelivered error. A seek abandons the failed position; any
    /// invalidation it queued keeps running on its own task.
    pub(crate) fn reset(&mut self) {
        self.pending = None;
    }
    pub(crate) fn failed(&mut self, error: std::io::Error) {
        let store = self
            .store
            .clone()
            .filter(|_| crate::blob::is_damaged_payload_io_error(&error));
        let Some(store) = store else {
            self.pending = Some(Box::pin(async move { Err(error) }));
            return;
        };
        // Readers are polled from whatever executor the caller uses. Only a
        // runtime can own the invalidation past a cancelled read; without one
        // it runs on the caller's own polls and the store fails closed if
        // those stop early.
        self.pending = Some(match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                let task = handle.spawn(async move { store.record_read_failure().await });
                Box::pin(async move {
                    if let Err(cause) = task.await {
                        tracing::warn!(error = %cause, "NAR association invalidation task failed");
                    }
                    Err(error)
                })
            }
            Err(_) => Box::pin(async move {
                store.record_read_failure().await;
                Err(error)
            }),
        });
    }
}
