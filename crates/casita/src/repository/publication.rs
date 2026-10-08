//! Publication policy and cancellation-safe commit lifetime.

use super::RepositoryError;
use crate::RepositoryRevision;
use crate::blob::BlobStore;
use crate::metadata::{CommitResult, MetadataMutation, MetadataSnapshot, MetadataStore};
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedMutexGuard};

#[derive(Clone)]
pub(super) struct Publication {
    payloads: Arc<dyn BlobStore>,
    metadata: Arc<dyn MetadataStore>,
    lock: Option<Arc<Mutex<()>>>,
    #[cfg(test)]
    maintenance_pause: Arc<std::sync::Mutex<Option<Arc<MaintenancePause>>>>,
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct MaintenancePause {
    pub(crate) reached: tokio::sync::Notify,
    pub(crate) resume: tokio::sync::Notify,
}

type LockedCommit = (
    Option<OwnedMutexGuard<()>>,
    Result<Option<CommitResult>, RepositoryError>,
);
pub(super) type Protection = Arc<dyn Send + Sync>;

/// One budget shared by stale-revision and pre-append maintenance retries.
/// Check deadlines between attempts, never by cancelling a submitted commit.
pub(super) struct PublicationRetry {
    attempts: u32,
    deadline: tokio::time::Instant,
}

impl PublicationRetry {
    pub(super) fn new() -> Self {
        Self {
            attempts: 1,
            deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        }
    }

    pub(super) async fn wait(&mut self) -> bool {
        if self.attempts >= 32 || tokio::time::Instant::now() >= self.deadline {
            return false;
        }
        let cap = (10_u64 << self.attempts.min(5)).min(250);
        let mut entropy = [0; 2];
        let _ = getrandom::fill(&mut entropy);
        let delay =
            std::time::Duration::from_millis(1 + u64::from(u16::from_le_bytes(entropy)) % cap);
        tokio::time::sleep_until((tokio::time::Instant::now() + delay).min(self.deadline)).await;
        self.attempts += 1;
        tokio::time::Instant::now() < self.deadline
    }
}

impl Publication {
    pub(super) fn new<PS: BlobStore + 'static, SS: MetadataStore + 'static>(
        payloads: Arc<PS>,
        metadata: Arc<SS>,
    ) -> Self {
        let lock = if metadata.coordinates_payload_catalog() {
            payloads.publication().enable_state_catalog();
            Some(Arc::new(Mutex::new(())))
        } else {
            None
        };
        Self {
            payloads,
            metadata,
            lock,
            #[cfg(test)]
            maintenance_pause: Arc::default(),
        }
    }

    #[cfg(test)]
    pub(super) fn pause_maintenance(&self) -> Arc<MaintenancePause> {
        let pause = Arc::new(MaintenancePause::default());
        *self.maintenance_pause.lock().unwrap() = Some(pause.clone());
        pause
    }

    pub(super) async fn lock(&self) -> Option<OwnedMutexGuard<()>> {
        match &self.lock {
            None => None,
            Some(lock) => Some(lock.clone().lock_owned().await),
        }
    }

    pub(super) fn try_lock(&self) -> Result<Option<OwnedMutexGuard<()>>, RepositoryError> {
        self.lock
            .as_ref()
            .map(|lock| {
                lock.clone()
                    .try_lock_owned()
                    .map_err(|_| RepositoryError::Busy("a catalog publication is active".into()))
            })
            .transpose()
    }

    #[tracing::instrument(name = "repository.catalog.synchronize", level = "debug", skip_all)]
    pub(super) async fn synchronize(
        &self,
        payloads: &impl BlobStore,
        snapshot: &dyn MetadataSnapshot,
    ) -> Result<(), RepositoryError> {
        if self.lock.is_some() {
            payloads
                .publication()
                .synchronize_state_catalog(snapshot.payload_catalog())
                .await?;
        }
        Ok(())
    }

    #[tracing::instrument(name = "repository.mutation.admit", level = "debug", skip_all)]
    pub(super) async fn admit_mutation(
        &self,
        payloads: &impl BlobStore,
        metadata: &impl MetadataStore,
        discovery_refreshed: bool,
        pin: &crate::metadata::DataPinLease,
    ) -> Result<(), RepositoryError> {
        if self.lock.is_some() {
            loop {
                let snapshot = metadata.snapshot().await?;
                let mut resources = snapshot.retention_resources();
                if let Some(catalog) = snapshot.payload_catalog() {
                    resources.insert(crate::metadata::PinResource::Catalog(catalog.to_vec()));
                }
                pin.protect(resources).await?;
                let current = metadata.snapshot().await?;
                if current.revision() != snapshot.revision()
                    || current.retention_resources() != snapshot.retention_resources()
                {
                    continue;
                }
                drop(current);
                self.synchronize(payloads, snapshot.as_ref()).await?;
                break;
            }
        }
        if !discovery_refreshed {
            payloads.publication().refresh_discovery().await?;
        }
        Ok(())
    }

    pub(super) async fn refresh_retained(
        &self,
        payloads: &impl BlobStore,
    ) -> Result<(), RepositoryError> {
        if self.lock.is_none() {
            payloads.publication().refresh_discovery().await?;
        }
        Ok(())
    }

    /// Fence pin admission for final logical validation and commit. The tracked
    /// task owns both the fence and collection protection through cancellation.
    pub(super) async fn prune(
        &self,
        protection: Protection,
        expected: RepositoryRevision,
        marked: Arc<crate::metadata::PinInventory>,
        claims: std::collections::BTreeSet<crate::metadata::PinToken>,
        existing_fence: Option<(crate::metadata::PinToken, crate::metadata::PinToken)>,
        retained: MetadataMutation,
    ) -> Result<CommitResult, RepositoryError> {
        let metadata = self.metadata.clone();
        crate::metadata::run_lease_task("logical prune", |send| async move {
            let _phase = super::CollectionPhase::new("prune_total");
            let _protection = protection;
            let mut cleanup_failure = None;
            let result = async {
                let admission_phase = super::CollectionPhase::new("prune_admission");
                let pins = metadata.pin_store().await?;
                let recovered = existing_fence.is_some();
                let (token, admitted_inventory) = match existing_fence {
                    Some((collector, prune)) => {
                        let inventory = pins.inventory().await?;
                        if inventory.collector.as_ref() != Some(&collector)
                            || inventory.logical_prune.as_ref() != Some(&prune)
                        {
                            return Err(RepositoryError::Busy(
                                "recovery lost its collector or logical fence".into(),
                            ));
                        }
                        (prune, None)
                    }
                    None => {
                        let collector = marked.collector.as_ref().ok_or_else(|| {
                            RepositoryError::Busy("logical prune requires collector ownership".into())
                        })?;
                        let (token, inventory) = pins.begin_prune_validating(collector, claims).await?
                            .ok_or_else(|| RepositoryError::Busy(
                                "collector ownership, logical fence, or deletion claims changed during mark".into()
                            ))?;
                        (token, Some(inventory))
                    }
                };
                drop(admission_phase);
                // Validate the inventory captured by successful ledger admission.
                // The fence prevents new logical protection while async lookups
                // run; all validation outcomes must pass through fence cleanup.
                let result = async {
                    let validation_phase = super::CollectionPhase::new("prune_validation");
                    if let Some(inventory) = admitted_inventory {
                        // Mark snapshots are released before execution so WAL
                        // checkpoints can proceed. A fresh snapshot is equivalent
                        // for validation only at the exact marked revision.
                        let snapshot = metadata.snapshot().await?;
                        if snapshot.revision() != expected {
                            return Err(crate::metadata::MetadataError::StaleRevision {
                                expected,
                                actual: snapshot.revision(),
                            }.into());
                        }
                        let retained_objects = retained.retained_objects().ok_or_else(|| {
                            crate::metadata::MetadataError::Backend(
                                "logical prune requires a retained object set".into(),
                            )
                        })?;
                        if let Some(conflict) = inventory
                            .logical_pin_conflict(
                                &marked,
                                retained_objects.as_ref(),
                                snapshot.as_ref(),
                            )
                            .await?
                        {
                            use crate::metadata::LogicalPinConflict;
                            let reason = match conflict {
                                LogicalPinConflict::SnapshotGeneration => {
                                    "snapshot generation advanced during collection mark"
                                }
                                LogicalPinConflict::UnmarkedRoot => {
                                    "logical pin protects existing unmarked object"
                                }
                            };
                            return Err(RepositoryError::Busy(reason.into()));
                        }
                    }
                    // The validation snapshot is out of scope before commit,
                    // which may checkpoint the WAL on this same connection.
                    drop(validation_phase);
                    let _commit_phase = super::CollectionPhase::new("prune_commit");
                    metadata
                        .commit(&expected, retained)
                        .await
                        .map_err(RepositoryError::from)
                }
                .await;
                if recovered && result.is_err() {
                    // An interrupted emergency sweep may already have removed
                    // bytes still referenced by unrooted records. Admission
                    // must stay fenced until a recovery commit succeeds.
                    return result;
                }
                // The metadata future has settled. Keep an uncertain release
                // durable for exact-token recovery instead of guessing success.
                let _release_phase = super::CollectionPhase::new("prune_release");
                if let Err(error) = pins.finish_prune(&token).await {
                    cleanup_failure = Some(crate::metadata::MetadataError::Backend(format!(
                        "logical prune fence release failed: {error}"
                    )));
                    return Err(error.into());
                }
                result
            }
            .await;
            drop(_phase);
            drop(send.send(result));
            cleanup_failure.map_or(Ok(()), Err)
        })
        .await?
    }

    /// Commit application metadata and roots over already-published content.
    /// There are no staged payloads to flush or catalog changes to publish.
    /// Keep the same cancellation-safe lock and pin lifetime as publication.
    pub(super) async fn commit_existing(
        &self,
        guard: Option<OwnedMutexGuard<()>>,
        protection: Protection,
        expected: RepositoryRevision,
        mutation: MetadataMutation,
    ) -> LockedCommit {
        let metadata = self.metadata.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        crate::metadata::spawn_lease_task(async move {
            let _protection = protection;
            let result = metadata
                .commit(&expected, mutation)
                .await
                .map(Some)
                .map_err(RepositoryError::from);
            let _ = sender.send((guard, result));
            Ok(())
        });
        receiver.await.unwrap_or_else(|error| {
            (
                None,
                Err(RepositoryError::Metadata(
                    crate::metadata::MetadataError::Backend(format!(
                        "metadata publication task failed: {error}"
                    )),
                )),
            )
        })
    }

    /// Once preparation starts, finish the operation even if its waiter goes
    /// away. Both the publication lock and collection protection belong to the
    /// task, including while an asynchronous backend is resolving its commit.
    /// `None` is a catalog-only mutation: skip metadata when no catalog changed.
    pub(super) async fn commit(
        &self,
        guard: Option<OwnedMutexGuard<()>>,
        protection: Protection,
        expected: RepositoryRevision,
        mutation: Option<MetadataMutation>,
        #[cfg(test)] profile: Arc<std::sync::Mutex<super::PublicationProfile>>,
    ) -> LockedCommit {
        let publication = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        crate::metadata::spawn_lease_task(async move {
            let _protection = protection;
            let catalog_only = mutation.is_none();
            let result = async {
                let mut mutation = mutation.unwrap_or_default();
                #[cfg(test)]
                let phase = super::PublicationTimer {
                    profile: &profile,
                    phase: 3,
                    started: std::time::Instant::now(),
                };
                let prepared = if publication.lock.is_some() {
                    publication
                        .payloads
                        .publication()
                        .prepare_state_commit()
                        .await?
                } else {
                    publication.payloads.publication().flush().await?;
                    crate::blob::PreparedCatalog::unchanged()
                }
                .with_protection(_protection.clone());
                match prepared.catalog() {
                    Some(catalog) => {
                        mutation.set_payload_catalog(catalog.to_vec());
                    }
                    None if catalog_only => return Ok(None),
                    None => {}
                }
                #[cfg(test)]
                drop(phase);
                #[cfg(test)]
                let phase = super::PublicationTimer {
                    profile: &profile,
                    phase: 4,
                    started: std::time::Instant::now(),
                };
                let committed = publication.metadata.commit(&expected, mutation).await;
                #[cfg(test)]
                drop(phase);
                #[cfg(test)]
                let _phase = super::PublicationTimer {
                    profile: &profile,
                    phase: 5,
                    started: std::time::Instant::now(),
                };
                if committed.is_ok() {
                    prepared.commit()?;
                } else {
                    prepared.abort()?;
                }
                committed.map(Some).map_err(RepositoryError::from)
            }
            .await;
            // Take the work before handing the lock back: no worker may escape
            // this publication's collection protection. Catalog-only commits
            // (including GC) need not start another optimization cycle.
            let maintenance = publication
                .payloads
                .publication()
                .take_catalog_maintenance();
            let maintenance = if catalog_only {
                drop(maintenance);
                None
            } else {
                maintenance
            };
            let _ = sender.send((guard, result));
            if let Some(maintenance) = maintenance {
                publication
                    .complete_maintenance(maintenance)
                    .await
                    .map_err(|error| {
                        crate::metadata::MetadataError::Backend(format!(
                            "catalog maintenance failed: {error}"
                        ))
                    })?;
            }
            Ok(())
        });
        receiver.await.unwrap_or_else(|error| {
            (
                None,
                Err(RepositoryError::Payload(error.to_string().into())),
            )
        })
    }

    async fn complete_maintenance(
        &self,
        mut maintenance: crate::blob::CatalogMaintenance,
    ) -> Result<(), RepositoryError> {
        // Foreground publication continues while immutable shards are built.
        // The result remains owned until it is installed or explicitly dropped.
        maintenance.run().await?;
        #[cfg(test)]
        {
            let pause = self.maintenance_pause.lock().unwrap().take();
            if let Some(pause) = pause {
                pause.reached.notify_one();
                pause.resume.notified().await;
            }
        }
        let _guard = self.lock().await;
        let result = async {
            let mut retry = PublicationRetry::new();
            loop {
                let snapshot = self.metadata.snapshot().await?;
                self.payloads
                    .publication()
                    .synchronize_state_catalog(snapshot.payload_catalog())
                    .await?;
                let expected = snapshot.revision();
                drop(snapshot);
                let prepared = self.payloads.publication().prepare_state_commit().await?;
                let Some(catalog) = prepared.catalog() else {
                    return Ok(());
                };
                let mut mutation = MetadataMutation::new();
                mutation.set_payload_catalog(catalog.to_vec());
                let committed = self.metadata.commit(&expected, mutation).await;
                if committed.is_ok() {
                    prepared.commit()?;
                } else {
                    prepared.abort()?;
                }
                match committed {
                    Ok(_) => return Ok(()),
                    Err(
                        error @ (crate::metadata::MetadataError::StaleRevision { .. }
                        | crate::metadata::MetadataError::MaintenanceFenced),
                    ) => {
                        if !retry.wait().await {
                            return Err(error.into());
                        }
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
        .await;
        // An advanced root may make this candidate obsolete. Discard it and
        // any replacement optimization queued by the catalog-only commit while
        // still serialized with foreground preparation.
        drop(self.payloads.publication().take_catalog_maintenance());
        drop(maintenance);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use futures::stream::BoxStream;

    use super::super::Repository;
    use crate::blob::{BlobGc, BlobReader, BlobWriter};
    use crate::error::Error;
    use crate::metadata::{CommitResult, MetadataError, MetadataMutation};
    use crate::{
        BlobId, ChunkId, MemoryBlobStore, MemoryMetadataStore, RepositoryRevision, RootName,
    };

    #[derive(Default)]
    struct Calls {
        capabilities: AtomicUsize,
        enable: AtomicUsize,
        flush: AtomicUsize,
        prepare: AtomicUsize,
        finish: AtomicUsize,
        refresh: AtomicUsize,
        synchronize: AtomicUsize,
        admission_gate: std::sync::Mutex<Option<(Arc<MaintenancePause>, bool)>>,
    }

    #[derive(Clone)]
    struct Storage {
        payloads: MemoryBlobStore,
        metadata: MemoryMetadataStore,
        catalog: bool,
        calls: Arc<Calls>,
    }

    #[async_trait]
    impl BlobStore for Storage {
        fn write_scope(&self) -> crate::metadata::BackendWriteScope {
            self.payloads.write_scope()
        }

        fn begin_pinned_batch(
            &self,
            pin: crate::metadata::DataPinLease,
        ) -> Result<crate::blob::BlobBatchGuard, crate::error::Error> {
            self.payloads.begin_pinned_batch(pin)
        }

        fn publication(&self) -> crate::blob::PayloadPublication<'_> {
            crate::blob::PayloadPublication::Cataloged(self)
        }
        async fn has(&self, digest: &BlobId) -> Result<bool, Error> {
            self.payloads.has(digest).await
        }
        async fn open_read(&self, digest: &BlobId) -> Result<Option<Box<dyn BlobReader>>, Error> {
            self.payloads.open_read(digest).await
        }
        async fn open_write(&self) -> Box<dyn BlobWriter> {
            assert_eq!(
                self.calls.enable.load(Ordering::SeqCst),
                usize::from(self.catalog)
            );
            self.payloads.open_write().await
        }
    }

    #[async_trait]
    impl crate::blob::CatalogPublication for Storage {
        async fn refresh_discovery(&self) -> Result<(), Error> {
            self.calls.refresh.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn flush(&self) -> Result<(), Error> {
            self.calls.flush.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn synchronize_state_catalog(&self, _catalog: Option<&[u8]>) -> Result<(), Error> {
            self.calls.synchronize.fetch_add(1, Ordering::SeqCst);
            let gate = self.calls.admission_gate.lock().unwrap().take();
            if let Some((pause, fail)) = gate {
                pause.reached.notify_one();
                pause.resume.notified().await;
                if fail {
                    return Err(Error::Msg("injected rotation admission failure".into()));
                }
            }
            Ok(())
        }
        fn enable_state_catalog(&self) {
            assert_eq!(self.calls.enable.fetch_add(1, Ordering::SeqCst), 0);
        }
        async fn prepare_state_commit(&self) -> Result<crate::blob::PreparedCatalog, Error> {
            self.calls.prepare.fetch_add(1, Ordering::SeqCst);
            let calls = self.calls.clone();
            Ok(crate::blob::PreparedCatalog::new(
                Some(b"committed catalog".to_vec()),
                move |outcome| {
                    assert_eq!(outcome, crate::blob::CatalogOutcome::Committed);
                    calls.finish.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
            ))
        }
        fn take_catalog_maintenance(&self) -> Option<crate::blob::CatalogMaintenance> {
            None
        }
    }

    #[async_trait]
    impl BlobGc for Storage {
        async fn reclaim_metadata_pinned(
            &self,
            pins: Arc<dyn crate::metadata::PinStore>,
            owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
        ) -> Result<(), Error> {
            self.payloads
                .reclaim_metadata_pinned(pins, owned_claims)
                .await
        }

        fn list_blobs(&self) -> BoxStream<'_, Result<BlobId, Error>> {
            self.payloads.list_blobs()
        }
        fn list_chunks(&self) -> BoxStream<'_, Result<ChunkId, Error>> {
            self.payloads.list_chunks()
        }
        async fn delete_blob(&self, digest: &BlobId) -> Result<(), Error> {
            self.payloads.delete_blob(digest).await
        }
        async fn delete_chunk(&self, digest: &ChunkId) -> Result<(), Error> {
            self.payloads.delete_chunk(digest).await
        }
        async fn delete_blobs_pinned(
            &self,
            digests: &[BlobId],
            pins: Arc<dyn crate::metadata::PinStore>,
            owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
            before_prune: bool,
        ) -> Result<usize, Error> {
            self.payloads
                .delete_blobs_pinned(digests, pins, owned_claims, before_prune)
                .await
        }
        async fn delete_chunks_pinned(
            &self,
            digests: &[ChunkId],
            pins: Arc<dyn crate::metadata::PinStore>,
            owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
        ) -> Result<usize, Error> {
            self.payloads
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
            self.payloads
                .finish_deletions_pinned(force_reclaim, pins, owned_claims, before_prune)
                .await
        }
        async fn finish_collection_pinned(
            &self,
            force_reclaim: bool,
            pins: Arc<dyn crate::metadata::PinStore>,
            owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
        ) -> Result<(), Error> {
            self.payloads
                .finish_collection_pinned(force_reclaim, pins, owned_claims)
                .await
        }
    }

    #[async_trait]
    impl MetadataStore for Storage {
        async fn try_collection_lease(
            &self,
        ) -> Result<Option<crate::metadata::RepositoryLease>, crate::metadata::MetadataError>
        {
            self.metadata.try_collection_lease().await
        }
        fn supports_metadata_records(&self) -> bool {
            true
        }
        async fn pin_store(
            &self,
        ) -> Result<std::sync::Arc<dyn crate::metadata::PinStore>, crate::metadata::MetadataError>
        {
            self.metadata.pin_store().await
        }

        fn coordinates_payload_catalog(&self) -> bool {
            assert_eq!(self.calls.capabilities.fetch_add(1, Ordering::SeqCst), 0);
            self.catalog
        }
        async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
            self.metadata.snapshot().await
        }
        async fn commit(
            &self,
            revision: &RepositoryRevision,
            mutation: MetadataMutation,
        ) -> Result<CommitResult, MetadataError> {
            self.metadata.commit(revision, mutation).await
        }
    }

    fn rotation_repository() -> (Repository<Storage, Storage>, Arc<Calls>) {
        let calls = Arc::new(Calls::default());
        let storage = Storage {
            payloads: MemoryBlobStore::new(),
            metadata: MemoryMetadataStore::new().unwrap(),
            catalog: true,
            calls: calls.clone(),
        };
        (Repository::new(storage.clone(), storage), calls)
    }

    #[tokio::test]
    async fn rotation_refreshes_catalog_protection_without_discovery() {
        let (repository, calls) = rotation_repository();
        let mut session = repository.mutation_session().await.unwrap();
        assert_eq!(calls.refresh.load(Ordering::SeqCst), 1);
        let before = calls.synchronize.load(Ordering::SeqCst);
        for _ in 0..3 {
            session.rotate().await.unwrap();
        }
        assert_eq!(calls.refresh.load(Ordering::SeqCst), 1);
        assert_eq!(calls.synchronize.load(Ordering::SeqCst), before + 3);
        drop(repository.mutation_session().await.unwrap());
        assert_eq!(calls.refresh.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn failed_and_cancelled_rotation_preserve_the_original_writer() {
        use tokio::io::AsyncReadExt;
        for cancel in [false, true] {
            let (repository, calls) = rotation_repository();
            let mut session = repository.mutation_session().await.unwrap();
            let object = session.stage_blob(b"old writer only").await.unwrap();
            let key = object.record().key().clone();
            session.publish_unrooted(vec![object]).await.unwrap();
            let pause = Arc::new(MaintenancePause::default());
            *calls.admission_gate.lock().unwrap() = Some((pause.clone(), !cancel));
            let mut rotating = Box::pin(session.rotate());
            tokio::select! {
                _ = pause.reached.notified() => {}
                result = &mut rotating => panic!("rotation ended before the gate: {result:?}"),
            }
            if !cancel {
                pause.resume.notify_one();
                let error = rotating.as_mut().await.unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("injected rotation admission failure")
                );
            }
            drop(rotating);
            crate::metadata::flush_repository_leases().await.unwrap();
            let inventory = repository
                .state
                .pin_store()
                .await
                .unwrap()
                .inventory()
                .await
                .unwrap();
            assert_eq!(
                inventory
                    .pins
                    .values()
                    .filter(|p| p.scope == crate::metadata::PinScope::Staging)
                    .count(),
                1
            );
            repository.collect().await.unwrap();
            let (_, mut payload) = repository.open_payload(&key).await.unwrap().unwrap();
            let mut bytes = Vec::new();
            payload.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, b"old writer only");
            drop(payload);
            let object = session.stage_blob(b"still usable").await.unwrap();
            let later = object.record().key().clone();
            session.publish_unrooted(vec![object]).await.unwrap();
            drop(session);
            crate::metadata::flush_repository_leases().await.unwrap();
            repository.collect().await.unwrap();
            assert!(repository.open_payload(&key).await.unwrap().is_none());
            assert!(repository.open_payload(&later).await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn rotation_protects_the_new_catalog_before_collection_and_old_pin_release() {
        use crate::metadata::{PinResource, PinScope};
        use tokio::io::AsyncReadExt;
        let (repository, calls) = rotation_repository();
        let mut session = repository.mutation_session().await.unwrap();
        let object = session.stage_blob(b"held through rotation").await.unwrap();
        let key = object.record().key().clone();
        session.publish_unrooted(vec![object]).await.unwrap();
        let held = repository.retention_hold().await.unwrap();
        crate::metadata::flush_repository_leases().await.unwrap();
        let pins = repository.state.pin_store().await.unwrap();
        let previous = pins.inventory().await.unwrap();
        let pause = Arc::new(MaintenancePause::default());
        *calls.admission_gate.lock().unwrap() = Some((pause.clone(), false));
        let mut rotating = Box::pin(session.rotate());
        tokio::select! {
            _ = pause.reached.notified() => {}
            result = &mut rotating => panic!("rotation ended before the gate: {result:?}"),
        }
        let during = pins.inventory().await.unwrap();
        let new_pins: Vec<_> = during
            .pins
            .iter()
            .filter(|(token, _)| !previous.pins.contains_key(token))
            .collect();
        assert_eq!(new_pins.len(), 1);
        let (new_token, pin) = new_pins[0];
        assert_eq!(pin.scope, PinScope::Staging);
        assert!(
            pin.resources
                .contains(&PinResource::Catalog(b"committed catalog".to_vec()))
        );
        repository.collect().await.unwrap();
        pause.resume.notify_one();
        rotating.as_mut().await.unwrap();
        drop(rotating);
        crate::metadata::flush_repository_leases().await.unwrap();
        let after = pins.inventory().await.unwrap();
        assert!(
            after.pins[new_token]
                .resources
                .contains(&PinResource::Catalog(b"committed catalog".to_vec()))
        );
        assert_eq!(
            after
                .pins
                .values()
                .filter(|p| p.scope == PinScope::Staging)
                .count(),
            1
        );
        repository.collect().await.unwrap();
        let (_, mut payload) = repository.open_payload(&key).await.unwrap().unwrap();
        let mut bytes = Vec::new();
        payload.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"held through rotation");
        drop(payload);
        let object = session.stage_blob(b"new writer works").await.unwrap();
        session.publish_unrooted(vec![object]).await.unwrap();
        drop(held);
    }

    #[tokio::test]
    async fn metadata_records_existing_roots_skip_payload_publication() {
        for catalog in [false, true] {
            let calls = Arc::new(Calls::default());
            let storage = Storage {
                payloads: MemoryBlobStore::new(),
                metadata: MemoryMetadataStore::new().unwrap(),
                catalog,
                calls: calls.clone(),
            };
            let repository = Repository::new(storage.clone(), storage);
            let session = repository.mutation_session().await.unwrap();
            let object = session.stage_blob(b"durable").await.unwrap();
            let target = object.record().key().clone();
            session
                .publish_rooted(
                    vec![object],
                    RootName::try_from("stage").unwrap(),
                    target.clone(),
                )
                .await
                .unwrap();
            let before = (
                calls.flush.load(Ordering::SeqCst),
                calls.prepare.load(Ordering::SeqCst),
                calls.finish.load(Ordering::SeqCst),
            );
            let key = crate::MetadataKey::new(
                crate::NamespaceId::try_from("obrador.v1").unwrap(),
                "paths/hash",
            );
            session
                .publish_with_metadata(
                    vec![],
                    vec![crate::MetadataCheck::Record {
                        key: key.clone(),
                        expected: None,
                    }],
                    vec![
                        crate::MetadataChange::Set {
                            key: key.clone(),
                            value: "descriptor".into(),
                        },
                        crate::MetadataChange::SetRoot {
                            name: RootName::try_from("roots/path").unwrap(),
                            target,
                        },
                    ],
                )
                .await
                .unwrap();
            assert_eq!(
                before,
                (
                    calls.flush.load(Ordering::SeqCst),
                    calls.prepare.load(Ordering::SeqCst),
                    calls.finish.load(Ordering::SeqCst)
                )
            );
            let snapshot = repository.metadata().snapshot().await.unwrap();
            assert_eq!(
                snapshot.get(&[key]).await.unwrap(),
                vec![Some("descriptor".into())]
            );
            assert_eq!(
                snapshot.payload_catalog(),
                catalog.then_some(b"committed catalog".as_slice())
            );
            drop(snapshot);
            let object = session.stage_blob(b"fresh").await.unwrap();
            let target = object.record().key().clone();
            session
                .publish_with_metadata(
                    vec![object],
                    vec![],
                    vec![crate::MetadataChange::SetRoot {
                        name: RootName::try_from("fresh").unwrap(),
                        target,
                    }],
                )
                .await
                .unwrap();
            assert!(
                calls.flush.load(Ordering::SeqCst) > before.0
                    || calls.prepare.load(Ordering::SeqCst) > before.1
            );
        }
    }

    #[tokio::test]
    async fn construction_selects_publication_before_writes_and_preserves_it_across_erasure() {
        for catalog in [false, true] {
            let calls = Arc::new(Calls::default());
            let storage = Storage {
                payloads: MemoryBlobStore::new(),
                metadata: MemoryMetadataStore::new().unwrap(),
                catalog,
                calls: calls.clone(),
            };
            let repository = Repository::new(storage.clone(), storage);
            assert_eq!(calls.capabilities.load(Ordering::SeqCst), 1);
            assert_eq!(calls.enable.load(Ordering::SeqCst), usize::from(catalog));
            let erased = repository.clone().into_builtin();
            let guard = repository.publication.lock().await;
            match &erased.publication.lock {
                None => assert!(guard.is_none()),
                Some(lock) => assert!(lock.try_lock().is_err()),
            }
            drop(guard);

            let mutation = erased.mutation_session().await.unwrap();
            let object = mutation
                .stage_blob(b"published through the selected strategy")
                .await
                .unwrap();
            let key = object.record().key().clone();
            mutation
                .publish_rooted(
                    vec![object],
                    RootName::try_from("test/strategy").unwrap(),
                    key.clone(),
                )
                .await
                .unwrap();
            drop(mutation);
            let hold = repository.retention_hold().await.unwrap();
            assert!(hold.open_payload(&key).await.unwrap().is_some());
            drop(hold);
            erased.collect().await.unwrap();
            let snapshot = repository.metadata().snapshot().await.unwrap();
            assert_eq!(
                snapshot.payload_catalog(),
                catalog.then_some(b"committed catalog".as_slice())
            );
            assert_eq!(calls.capabilities.load(Ordering::SeqCst), 1);
            assert_eq!(calls.enable.load(Ordering::SeqCst), usize::from(catalog));
            assert_eq!(calls.prepare.load(Ordering::SeqCst) > 0, catalog);
            assert_eq!(calls.finish.load(Ordering::SeqCst) > 0, catalog);
            assert_eq!(calls.flush.load(Ordering::SeqCst) > 0, !catalog);
        }
    }
}
