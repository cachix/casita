//! Opt-in verified repair for a near/far pair of chunked blob stores.

use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite};
use tokio::sync::{Notify, OwnedRwLockReadGuard, RwLock};

use super::{
    BlobBatchGuard, BlobGc, BlobIntegrityError, BlobReader, BlobStore, BlobStreamReader,
    BlobWriter, ChunkMeta, ChunkedBlobStore, is_integrity_error,
};
use crate::digest::{BlobId, ChunkId};
use crate::error::Error;

/// A failed attempt to replace a corrupt near-tier representation from the far
/// tier.
///
/// Both the original near-tier failure and the later source/publication
/// failure are retained so an operator can distinguish damaged data from an
/// unavailable repair source.
#[derive(Clone, Debug, thiserror::Error)]
#[error("could not repair blob {digest} after near-tier failure `{near}`: {repair}")]
pub struct BlobRepairError {
    digest: BlobId,
    near: String,
    repair: String,
}

impl BlobRepairError {
    /// Blob whose near-tier representation needed repair.
    pub fn digest(&self) -> BlobId {
        self.digest
    }

    /// Original near-tier integrity or missing-data diagnostic.
    pub fn near_error(&self) -> &str {
        &self.near
    }

    /// Failure encountered while verifying or publishing the replacement.
    pub fn repair_error(&self) -> &str {
        &self.repair
    }
}

/// A corruption-aware near/far [`BlobStore`].
///
/// Normal reads prefer `near` and use `far` only when the blob is absent. If a
/// complete validation of a near reader finds typed corruption or a referenced
/// chunk disappears, the adapter verifies the complete far representation,
/// replaces the near representation, and only then returns a reader. Ordinary
/// permission, timeout, and backend errors never trigger repair.
///
/// Repair is single-flight per blob within all clones of this adapter. The
/// adapter's [`BlobGc`] implementation owns only the near tier. Repository
/// collection uses durable pins and may proceed during reads and repair. Raw
/// deletion calls serialize against raw readers, writers, and repair. The far
/// tier is a read-only repair source whose lifetime is managed by its owner.
#[derive(Clone)]
pub struct RepairingBlobStore {
    near: ChunkedBlobStore,
    far: ChunkedBlobStore,
    // Registry critical sections contain no I/O or awaits. Cancellation must
    // unlink its flight in Drop, without relying on another task being polled.
    repairs: Arc<StdMutex<HashMap<BlobId, Arc<RepairFlight>>>>,
    gate: Arc<RwLock<()>>,
}

impl RepairingBlobStore {
    /// Add opt-in verified repair to two compatible chunked stores.
    pub fn new(near: ChunkedBlobStore, far: ChunkedBlobStore) -> Self {
        Self {
            near,
            far,
            repairs: Arc::new(StdMutex::new(HashMap::new())),
            gate: Arc::new(RwLock::new(())),
        }
    }

    /// Writable tier repaired by this adapter.
    pub fn near(&self) -> &ChunkedBlobStore {
        &self.near
    }

    /// Read-only tier used as the independently verified repair source.
    pub fn far(&self) -> &ChunkedBlobStore {
        &self.far
    }

    #[cfg(test)]
    pub(crate) async fn repair_waiters(&self, digest: &BlobId) -> usize {
        self.repairs
            .lock()
            .expect("repair registry mutex poisoned")
            .get(digest)
            .map_or(0, |flight| flight.waiters.load(Ordering::SeqCst))
    }

    /// Read and independently Bao-verify one range, repairing a corrupt near
    /// payload or rebuilding a missing/corrupt near outboard when safe.
    #[tracing::instrument(
        name = "blob.repairing.verified_read",
        skip_all,
        fields(blob = %digest, offset = offset, len = len)
    )]
    pub async fn verified_read(
        &self,
        digest: &BlobId,
        offset: u64,
        len: u64,
    ) -> Result<Bytes, Error> {
        let _guard = self.gate.clone().read_owned().await;
        let initial = match self.near.verified_read(digest, offset, len).await {
            Ok(bytes) => return Ok(bytes),
            Err(error) => error,
        };

        match validated_reader(&self.near, digest).await {
            Ok(Some(_)) => {
                // The authoritative payload is healthy, so only derived Bao
                // state may be repaired. Preserve invalid ranges and unrelated
                // failures when the stored outboard is already exact.
                let expected = self.near.compute_outboard(digest).await?;
                let stored = match self.near.get_outboard(digest).await {
                    Ok(stored) => stored,
                    Err(error) if is_integrity_error(&error) => None,
                    Err(error) => return Err(error),
                };
                if stored.as_ref() == Some(&expected) {
                    return Err(initial);
                }
                tracing::info!(blob = %digest, "rebuilding corrupt near-tier outboard");
                self.near.put_outboard(digest, expected).await?;
            }
            Ok(None) => {
                tracing::debug!(blob = %digest, "near-tier blob absent; reading far tier");
                return self.far.verified_read(digest, offset, len).await;
            }
            Err(error) if repairable_near_error(&error) => {
                tracing::info!(blob = %digest, "near-tier blob requires repair");
                self.repair_once(*digest, error.to_string()).await?;
                // Outboards are derived from the newly verified local bytes,
                // never copied from the repair source.
                self.near.build_outboard(digest).await?;
            }
            Err(_) => return Err(initial),
        }

        self.near.verified_read(digest, offset, len).await
    }

    #[tracing::instrument(name = "blob.repairing.repair", skip_all, fields(blob = %digest))]
    async fn repair_once(&self, digest: BlobId, near: String) -> Result<(), Error> {
        let (flight, leader) = {
            let mut repairs = self.repairs.lock().expect("repair registry mutex poisoned");
            match repairs.get(&digest) {
                Some(flight) => (flight.clone(), false),
                None => {
                    let flight = Arc::new(RepairFlight::new());
                    repairs.insert(digest, flight.clone());
                    (flight, true)
                }
            }
        };

        if leader {
            tracing::debug!("leading coalesced blob repair");
            let mut completion =
                RepairLeader::new(digest, near.clone(), flight.clone(), self.repairs.clone());
            let outcome = self.perform_repair(digest, near).await;
            flight.complete(outcome.clone());
            remove_flight(&self.repairs, digest, &flight);
            completion.disarm();
            return outcome.map_err(|error| Error::Backend(Box::new(error)));
        }

        tracing::debug!("waiting for coalesced blob repair");
        flight
            .wait()
            .await
            .map_err(|error| Error::Backend(Box::new(error)))
    }

    #[tracing::instrument(
        name = "blob.repairing.perform_repair",
        skip_all,
        fields(blob = %digest)
    )]
    async fn perform_repair(&self, digest: BlobId, near: String) -> Result<(), BlobRepairError> {
        match validated_reader(&self.far, &digest).await {
            Ok(Some(_)) => {}
            Ok(None) => {
                return Err(BlobRepairError {
                    digest,
                    near,
                    repair: "far tier does not contain the blob".to_owned(),
                });
            }
            Err(error) => {
                return Err(BlobRepairError {
                    digest,
                    near,
                    repair: format!("far tier did not verify: {error}"),
                });
            }
        }

        if let Err(error) = self.near.repair_from_replica(&self.far, &digest).await {
            return Err(BlobRepairError {
                digest,
                near,
                repair: format!("verified replacement could not be published: {error}"),
            });
        }
        match validated_reader(&self.near, &digest).await {
            Ok(Some(_)) => {
                tracing::info!("near-tier blob repair completed and verified");
                Ok(())
            }
            Ok(None) => Err(BlobRepairError {
                digest,
                near,
                repair: "replacement was not readable after publication".to_owned(),
            }),
            Err(error) => Err(BlobRepairError {
                digest,
                near,
                repair: format!("replacement did not verify after publication: {error}"),
            }),
        }
    }

    async fn guarded_reader(&self, digest: &BlobId) -> Result<Option<Box<dyn BlobReader>>, Error> {
        let guard = self.gate.clone().read_owned().await;
        match validated_reader(&self.near, digest).await {
            Ok(Some(reader)) => Ok(Some(Box::new(GuardedReader::new(reader, guard)))),
            Ok(None) => Ok(validated_reader(&self.far, digest)
                .await?
                .map(|reader| Box::new(GuardedReader::new(reader, guard)) as Box<dyn BlobReader>)),
            Err(error) if repairable_near_error(&error) => {
                self.repair_once(*digest, error.to_string()).await?;
                let reader =
                    validated_reader(&self.near, digest)
                        .await?
                        .ok_or(Error::NotFound {
                            digest: (*digest).into(),
                        })?;
                Ok(Some(Box::new(GuardedReader::new(reader, guard))))
            }
            Err(error) => Err(error),
        }
    }
}

#[async_trait]
impl super::CatalogPublication for RepairingBlobStore {
    async fn refresh_discovery(&self) -> Result<(), Error> {
        let _guard = self.gate.read().await;
        self.near.publication().refresh_discovery().await
    }

    async fn flush(&self) -> Result<(), Error> {
        let _guard = self.gate.read().await;
        self.near.publication().flush().await
    }

    async fn synchronize_state_catalog(&self, catalog: Option<&[u8]>) -> Result<(), Error> {
        let _guard = self.gate.read().await;
        self.near
            .publication()
            .synchronize_state_catalog(catalog)
            .await
    }

    fn enable_state_catalog(&self) {
        self.near.publication().enable_state_catalog();
    }

    async fn prepare_state_commit(&self) -> Result<super::PreparedCatalog, Error> {
        let guard = self.gate.clone().read_owned().await;
        Ok(self
            .near
            .publication()
            .prepare_state_commit()
            .await?
            .with_protection(guard))
    }

    fn take_catalog_maintenance(&self) -> Option<super::CatalogMaintenance> {
        self.near.publication().take_catalog_maintenance()
    }
}

#[async_trait]
impl BlobStore for RepairingBlobStore {
    async fn overwrite(
        &self,
        digest: &BlobId,
        size: u64,
        offset: u64,
        replacement: &[u8],
    ) -> Result<(BlobId, Bytes), Error> {
        self.near.overwrite(digest, size, offset, replacement).await
    }
    async fn open_proof(
        &self,
        digest: &BlobId,
        size: u64,
    ) -> Result<Option<Box<dyn crate::blob::BlobStreamReader>>, Error> {
        self.near.open_proof(digest, size).await
    }
    fn write_scope(&self) -> crate::metadata::BackendWriteScope {
        self.near.write_scope()
    }

    async fn has(&self, digest: &BlobId) -> Result<bool, Error> {
        let _guard = self.gate.read().await;
        Ok(self.near.has(digest).await? || self.far.has(digest).await?)
    }

    async fn open_read(&self, digest: &BlobId) -> Result<Option<Box<dyn BlobReader>>, Error> {
        self.guarded_reader(digest).await
    }

    async fn read_to_vec(&self, digest: &BlobId) -> Result<Option<Vec<u8>>, Error> {
        let _guard = self.gate.read().await;
        match validated_bytes(&self.near, digest).await {
            Ok(Some(bytes)) => Ok(Some(bytes)),
            Ok(None) => validated_bytes(&self.far, digest).await,
            Err(error) if repairable_near_error(&error) => {
                self.repair_once(*digest, error.to_string()).await?;
                validated_bytes(&self.near, digest)
                    .await?
                    .map(Some)
                    .ok_or(Error::NotFound {
                        digest: (*digest).into(),
                    })
            }
            Err(error) => Err(error),
        }
    }

    async fn open_stream(
        &self,
        digest: &BlobId,
    ) -> Result<Option<Box<dyn BlobStreamReader>>, Error> {
        Ok(self
            .open_read(digest)
            .await?
            .map(|reader| Box::new(reader) as Box<dyn BlobStreamReader>))
    }

    async fn open_write(&self) -> Box<dyn BlobWriter> {
        let guard = self.gate.clone().read_owned().await;
        Box::new(GuardedWriter {
            inner: self.near.open_write().await,
            _guard: guard,
        })
    }

    fn begin_batch(&self) -> BlobBatchGuard {
        self.near.begin_batch()
    }

    fn begin_pinned_batch(
        &self,
        pin: crate::metadata::DataPinLease,
    ) -> Result<BlobBatchGuard, Error> {
        self.near.begin_pinned_batch(pin)
    }

    // Repairs replace near-tier representations, so sealing and catalog
    // publication run under the same gate as reads.
    fn publication(&self) -> super::PayloadPublication<'_> {
        if self.near.publication().is_cataloged() {
            super::PayloadPublication::Cataloged(self)
        } else {
            super::PayloadPublication::Immediate
        }
    }

    // Repairs and collection delete only near-tier representations.
    fn order_deletions_after(&self, commits: super::CommitDurability) {
        self.near.order_deletions_after(commits);
    }

    async fn chunks(&self, digest: &BlobId) -> Result<Option<Vec<ChunkMeta>>, Error> {
        let _guard = self.gate.read().await;
        match self.near.chunks(digest).await {
            Ok(Some(chunks)) => Ok(Some(chunks)),
            Ok(None) => self.far.chunks(digest).await,
            Err(error) if repairable_near_error(&error) => {
                self.repair_once(*digest, error.to_string()).await?;
                self.near.chunks(digest).await
            }
            Err(error) => Err(error),
        }
    }
}

#[async_trait]
impl BlobGc for RepairingBlobStore {
    async fn delete_blobs_pinned(
        &self,
        digests: &[BlobId],
        pins: Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
        before_prune: bool,
    ) -> Result<usize, Error> {
        self.near
            .delete_blobs_pinned(digests, pins, owned_claims, before_prune)
            .await
    }

    async fn delete_chunks_pinned(
        &self,
        digests: &[ChunkId],
        pins: Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
    ) -> Result<usize, Error> {
        self.near
            .delete_chunks_pinned(digests, pins, owned_claims)
            .await
    }

    async fn finish_deletions_pinned(
        &self,
        force_reclaim: bool,
        pins: Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
        before_prune: bool,
    ) -> Result<(), Error> {
        self.near
            .finish_deletions_pinned(force_reclaim, pins, owned_claims, before_prune)
            .await
    }

    async fn finish_collection_pinned(
        &self,
        force_reclaim: bool,
        pins: Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
    ) -> Result<(), Error> {
        self.near
            .finish_collection_pinned(force_reclaim, pins, owned_claims)
            .await
    }

    async fn finish_collection(&self, force_reclaim: bool) -> Result<(), Error> {
        let _guard = self.gate.write().await;
        self.near.finish_collection(force_reclaim).await
    }

    fn list_blobs(&self) -> BoxStream<'_, Result<BlobId, Error>> {
        self.near.list_blobs()
    }

    fn list_chunks(&self) -> BoxStream<'_, Result<ChunkId, Error>> {
        self.near.list_chunks()
    }

    async fn open_read_for_fsck(
        &self,
        digest: &BlobId,
        _manifest_present: bool,
    ) -> Result<Option<Box<dyn BlobReader>>, Error> {
        self.open_read(digest).await
    }

    async fn chunks_for_gc(
        &self,
        digest: &BlobId,
        _manifest_present: bool,
    ) -> Result<Option<Vec<ChunkMeta>>, Error> {
        self.chunks(digest).await
    }

    async fn delete_blob(&self, digest: &BlobId) -> Result<(), Error> {
        let _guard = self.gate.write().await;
        self.near.delete_blob(digest).await
    }

    async fn delete_chunk(&self, digest: &ChunkId) -> Result<(), Error> {
        let _guard = self.gate.write().await;
        self.near.delete_chunk(digest).await
    }

    async fn delete_chunks(&self, digests: &[ChunkId]) -> Result<(), Error> {
        let _guard = self.gate.write().await;
        self.near.delete_chunks(digests).await
    }

    async fn finish_deletions(&self) -> Result<(), Error> {
        let _guard = self.gate.write().await;
        self.near.finish_deletions().await
    }

    async fn reclaim_deletions(&self) -> Result<(), Error> {
        let _guard = self.gate.write().await;
        self.near.reclaim_deletions().await
    }

    async fn reclaim_metadata(&self) -> Result<(), Error> {
        let _guard = self.gate.write().await;
        self.near.reclaim_metadata().await
    }

    async fn reclaim_metadata_pinned(
        &self,
        pins: Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
    ) -> Result<(), Error> {
        self.near.reclaim_metadata_pinned(pins, owned_claims).await
    }

    async fn metadata_reclaim_due(&self) -> Result<bool, Error> {
        let _guard = self.gate.read().await;
        self.near.metadata_reclaim_due().await
    }
}

fn repairable_near_error(error: &Error) -> bool {
    is_integrity_error(error)
        || matches!(error, Error::Io(error) if error.kind() == io::ErrorKind::NotFound)
}

// A caller requesting a Vec already accepts whole-blob memory use. Validate
// that same allocation and return it, instead of seeking and decompressing twice.
async fn validated_bytes(
    store: &ChunkedBlobStore,
    digest: &BlobId,
) -> Result<Option<Vec<u8>>, Error> {
    let Some(bytes) = store.read_to_vec(digest).await? else {
        return Ok(None);
    };
    let (bytes, actual) = tokio::task::spawn_blocking(move || {
        let actual = BlobId::new(blake3::hash(&bytes).into());
        (bytes, actual)
    })
    .await
    .map_err(|error| io::Error::other(error.to_string()))?;
    if actual != *digest {
        return Err(io::Error::other(BlobIntegrityError::Blob { expected: *digest }).into());
    }
    Ok(Some(bytes))
}

async fn validated_reader(
    store: &ChunkedBlobStore,
    digest: &BlobId,
) -> Result<Option<Box<dyn BlobReader>>, Error> {
    let Some(mut reader) = store.open_read(digest).await? else {
        return Ok(None);
    };
    let mut hasher = blake3::Hasher::new();
    // Keep nested verification/repair futures small on Tokio worker stacks.
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let actual = BlobId::new(hasher.finalize().into());
    if actual != *digest {
        return Err(io::Error::other(BlobIntegrityError::Blob { expected: *digest }).into());
    }
    reader.seek(io::SeekFrom::Start(0)).await?;
    Ok(Some(reader))
}

struct RepairFlight {
    outcome: StdMutex<Option<Result<(), BlobRepairError>>>,
    ready: Notify,
    waiters: AtomicUsize,
}

impl RepairFlight {
    fn new() -> Self {
        Self {
            outcome: StdMutex::new(None),
            ready: Notify::new(),
            waiters: AtomicUsize::new(0),
        }
    }

    fn complete(&self, outcome: Result<(), BlobRepairError>) {
        *self.outcome.lock().expect("repair flight mutex poisoned") = Some(outcome);
        self.ready.notify_waiters();
    }

    async fn wait(&self) -> Result<(), BlobRepairError> {
        let _waiter = RepairWaiter::new(&self.waiters);
        loop {
            let notified = self.ready.notified();
            if let Some(outcome) = self
                .outcome
                .lock()
                .expect("repair flight mutex poisoned")
                .clone()
            {
                return outcome;
            }
            notified.await;
        }
    }
}

struct RepairWaiter<'a>(&'a AtomicUsize);

impl<'a> RepairWaiter<'a> {
    fn new(waiters: &'a AtomicUsize) -> Self {
        waiters.fetch_add(1, Ordering::SeqCst);
        Self(waiters)
    }
}

impl Drop for RepairWaiter<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn remove_flight(
    repairs: &StdMutex<HashMap<BlobId, Arc<RepairFlight>>>,
    digest: BlobId,
    flight: &Arc<RepairFlight>,
) {
    let mut repairs = repairs.lock().expect("repair registry mutex poisoned");
    if repairs
        .get(&digest)
        .is_some_and(|current| Arc::ptr_eq(current, flight))
    {
        repairs.remove(&digest);
    }
}

struct RepairLeader {
    digest: BlobId,
    near: String,
    flight: Arc<RepairFlight>,
    repairs: Arc<StdMutex<HashMap<BlobId, Arc<RepairFlight>>>>,
    armed: bool,
}

impl RepairLeader {
    fn new(
        digest: BlobId,
        near: String,
        flight: Arc<RepairFlight>,
        repairs: Arc<StdMutex<HashMap<BlobId, Arc<RepairFlight>>>>,
    ) -> Self {
        Self {
            digest,
            near,
            flight,
            repairs,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RepairLeader {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Retire the registration before waking existing waiters so a new
        // reader can lead a repair even without a running executor.
        remove_flight(&self.repairs, self.digest, &self.flight);
        self.flight.complete(Err(BlobRepairError {
            digest: self.digest,
            near: self.near.clone(),
            repair: "repair was cancelled before publication completed".to_owned(),
        }));
    }
}

struct GuardedReader {
    inner: Box<dyn BlobReader>,
    _guard: OwnedRwLockReadGuard<()>,
}

impl GuardedReader {
    fn new(inner: Box<dyn BlobReader>, guard: OwnedRwLockReadGuard<()>) -> Self {
        Self {
            inner,
            _guard: guard,
        }
    }
}

impl AsyncRead for GuardedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncSeek for GuardedReader {
    fn start_seek(mut self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        Pin::new(&mut self.inner).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Pin::new(&mut self.inner).poll_complete(cx)
    }
}

#[async_trait]
impl BlobReader for GuardedReader {
    async fn park(&mut self) {
        self.inner.park().await;
    }
}

struct GuardedWriter {
    inner: Box<dyn BlobWriter>,
    _guard: OwnedRwLockReadGuard<()>,
}

impl AsyncWrite for GuardedWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[async_trait]
impl BlobWriter for GuardedWriter {
    async fn close(&mut self) -> Result<(BlobId, u64), Error> {
        self.inner.close().await
    }
}

#[cfg(test)]
mod tests;
