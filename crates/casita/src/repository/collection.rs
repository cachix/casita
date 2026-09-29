//! Logical and physical collection, recovery, and vacuum.

use super::*;

/// Candidate counts from a mark pass, shared by dry-run and real collection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CollectionPreview {
    /// Unreachable immutable logical records.
    pub logical_objects: usize,
    /// Unreferenced physical blob manifests or whole-blob entries.
    pub payload_blobs: usize,
    /// Unreferenced physical chunks.
    pub chunks: usize,
}

/// Candidate count from a logical-only mark pass.
///
/// Logical collection is for repositories whose payload backend does not own
/// physical deletion, such as a remote backend that shares payload bytes with
/// other repositories. It prunes unreachable records but deliberately makes
/// no statement about reclaimable blobs or chunks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogicalCollectionPreview {
    /// Unreachable immutable logical records.
    pub logical_objects: usize,
}

/// Completed generic collection outcome.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CollectionOutcome {
    /// Fresh logical revision, or `None` when no logical records needed pruning.
    pub revision: Option<crate::RepositoryRevision>,
    /// What the mark pass selected for removal.
    pub removed: CollectionPreview,
    /// Temporary traversal-state use while computing this collection plan.
    pub spill: SpillMetrics,
}

/// Completed logical-only collection outcome.
///
/// The remote payload service, not this per-repository operation, owns any
/// later physical reclamation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogicalCollectionOutcome {
    /// Fresh logical revision, or `None` when no logical records needed pruning.
    pub revision: Option<crate::RepositoryRevision>,
    /// What the mark pass selected for logical removal.
    pub removed: LogicalCollectionPreview,
    /// Temporary traversal-state use while computing this collection plan.
    pub spill: SpillMetrics,
}
pub(super) const METADATA_RECLAIM_INTERVAL: usize = 16;

impl<PS, SS> Repository<PS, SS>
where
    SS: MetadataStore,
{
    /// Preview logical collection without inspecting or reclaiming physical
    /// payload storage.
    ///
    /// This is for a repository whose payload backend is shared with other
    /// repositories. It reports only unreachable records; a shared backend
    /// must establish global payload liveness independently.
    #[tracing::instrument(
        name = "repository.collection.preview",
        skip_all,
        fields(mode = "logical")
    )]
    pub async fn preview_logical_collection(
        &self,
    ) -> Result<LogicalCollectionPreview, RepositoryError> {
        let guard = self.coordination.clone().lock_owned().await;
        let fs_guard = self.exclusive_fs().await?;
        let plan = self.logical_collection_plan(guard, fs_guard, true).await?;
        plan.protection.collector.finish().await?;
        Ok(plan.preview.clone())
    }

    /// Atomically prune unreachable records without reclaiming physical
    /// payloads or chunks.
    ///
    /// Unlike [`collect`](Self::collect), this method requires no
    /// [`BlobGc`] implementation. A remote payload service can
    /// use its own aggregate ownership ledger to reclaim bytes after every
    /// repository that references them has pruned its records.
    #[tracing::instrument(
        name = "repository.collection",
        skip_all,
        fields(mode = "logical", blocking = true)
    )]
    pub async fn collect_logical(&self) -> Result<LogicalCollectionOutcome, RepositoryError> {
        let guard = self.coordination.clone().lock_owned().await;
        let fs_guard = self.exclusive_fs().await?;
        let plan = self.logical_collection_plan(guard, fs_guard, true).await?;
        self.execute_logical_collection(plan).await
    }

    /// Logically collect only if exclusive ownership is immediately available.
    ///
    /// Like [`try_collect`](Self::try_collect), this never waits for a
    /// mutation, stable read, transfer, or another collection operation.
    #[tracing::instrument(
        name = "repository.collection",
        skip_all,
        fields(mode = "logical", blocking = false)
    )]
    pub async fn try_collect_logical(&self) -> Result<LogicalCollectionOutcome, RepositoryError> {
        let guard = self.coordination.clone().try_lock_owned().map_err(|_| {
            RepositoryError::Busy(
                "another collection or maintenance operation is active".to_owned(),
            )
        })?;
        let fs_guard = match self.profile.fs_coordination() {
            Some(coordination) => Some(coordination.try_exclusive().await?.ok_or_else(|| {
                RepositoryError::Busy(
                    "another process holds collection or maintenance ownership".to_owned(),
                )
            })?),
            None => None,
        };
        let plan = self.logical_collection_plan(guard, fs_guard, false).await?;
        self.execute_logical_collection(plan).await
    }

    pub(super) async fn execute_logical_collection(
        &self,
        mut plan: LogicalCollectionPlan,
    ) -> Result<LogicalCollectionOutcome, RepositoryError> {
        // Marking is complete. Retain the frozen sets and revision, not the
        // read transaction, across pruning and checkpointing.
        drop(plan.snapshot.take());
        if !plan.pins.deletions.is_empty() {
            return Err(RepositoryError::Busy(
                "physical collection must finish deletion recovery".into(),
            ));
        }
        if plan.preview.logical_objects == 0 && plan.pins.logical_prune.is_none() {
            plan.protection.collector.finish().await?;
            return Ok(LogicalCollectionOutcome {
                revision: None,
                removed: plan.preview,
                spill: plan.area.metrics(),
            });
        }

        let retained = MetadataMutation::install_retained_source(plan.live_objects);
        let commit = self
            .publication
            .prune(
                plan.protection.clone(),
                plan.snapshot_revision,
                plan.pins.clone(),
                BTreeSet::new(),
                plan.pins
                    .logical_prune
                    .clone()
                    .map(|prune| (plan.protection.collector.token().clone(), prune)),
                retained,
            )
            .await?;
        plan.protection.collector.finish().await?;
        Ok(LogicalCollectionOutcome {
            revision: Some(commit.revision),
            removed: plan.preview,
            spill: plan.area.metrics(),
        })
    }

    #[tracing::instrument(
        name = "repository.collection.mark",
        skip_all,
        fields(mode = "logical")
    )]
    pub(super) async fn logical_collection_plan(
        &self,
        guard: OwnedMutexGuard<()>,
        fs_guard: Option<ExclusiveLease>,
        wait: bool,
    ) -> Result<LogicalCollectionPlan, RepositoryError> {
        self.logical_collection_plan_with_recovery(guard, fs_guard, wait, None, None)
            .await
    }

    pub(super) async fn logical_collection_plan_with_recovery(
        &self,
        guard: OwnedMutexGuard<()>,
        fs_guard: Option<ExclusiveLease>,
        wait: bool,
        recovery: Option<&crate::metadata::PinToken>,
        remote: Option<crate::metadata::RepositoryLease>,
    ) -> Result<LogicalCollectionPlan, RepositoryError> {
        let _phase = CollectionPhase::new("logical_plan");
        let admission_phase = CollectionPhase::new("collector_admission");
        let lease_phase = CollectionPhase::new("collector_backend_lease");
        let remote_guard = match remote {
            Some(remote) => remote,
            None => self.collector_state_lease(wait).await?,
        };
        drop(lease_phase);
        let area = self.spill_area();
        let releases = CollectionPhase::new("collector_release_barrier");
        crate::metadata::flush_pin_releases().await;
        drop(releases);
        let inventory_phase = CollectionPhase::new("collector_inventory");
        let ledger = self.state.pin_store().await?;
        let mut pins = ledger.inventory().await?;
        drop(inventory_phase);
        // Preserve an interrupted emergency fence throughout takeover and
        // marking. Only a successful recovery commit may reopen admission.
        if let Some(expected) = recovery {
            if pins.collector.as_ref() != Some(expected) {
                return Err(RepositoryError::Busy(
                    "abandoned collector token no longer matches the pin ledger".into(),
                ));
            }
        } else if pins.collector.is_some() && !self.profile.collects_in_emergency() {
            return Err(RepositoryError::Busy(
                "another collector owns the pin ledger; exact-token recovery may be required"
                    .into(),
            ));
        }
        let acquire_phase = CollectionPhase::new("collector_ledger_acquire");
        let collector =
            crate::metadata::CollectorLease::try_acquire(ledger.clone(), pins.collector.clone())
                .await?
                .ok_or_else(|| {
                    RepositoryError::Busy("collector admission raced a pin update".into())
                })?;
        drop(acquire_phase);
        // Begin history retention before reading the metadata head. A writer
        // that finishes during marking can no longer disappear from liveness.
        drop(admission_phase);
        let _mark_phase = CollectionPhase::new("logical_mark");
        let snapshot = self.state.snapshot().await?;
        pins = ledger.inventory().await?;
        let mut live_objects =
            mark_named_roots(snapshot.as_ref(), self.limits.max_traversal_objects, &area).await?;
        mark_pin_scopes(
            snapshot.as_ref(),
            &pins,
            &mut live_objects,
            self.limits.max_traversal_objects,
            &area,
        )
        .await?;
        let live_objects = live_objects.freeze().await?;

        let mut logical_objects = 0usize;
        let mut records = snapshot.objects_unordered();
        while let Some(record) = records.next().await {
            let record = record?;
            if !live_objects.contains(record.key()).await? {
                logical_objects += 1;
            }
        }

        Ok(LogicalCollectionPlan {
            protection: Arc::new(ExclusiveProtection {
                collector,
                _guard: guard,
                _fs_guard: fs_guard,
                remote: std::sync::Mutex::new(remote_guard),
            }),
            area,
            snapshot_revision: snapshot.revision(),
            pins: Arc::new(pins),
            snapshot: Some(snapshot),
            live_objects: Arc::new(live_objects),
            preview: LogicalCollectionPreview { logical_objects },
        })
    }

    pub(super) async fn collector_state_lease(
        &self,
        wait: bool,
    ) -> Result<crate::RepositoryLease, RepositoryError> {
        loop {
            if let Some(lease) = self.state.try_collection_lease().await? {
                return Ok(lease);
            }
            if !wait {
                return Err(RepositoryError::Busy(
                    "another runner holds repository ownership".to_owned(),
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    pub(super) async fn exclusive_fs(&self) -> Result<Option<ExclusiveLease>, RepositoryError> {
        match self.profile.fs_coordination() {
            Some(coordination) => Ok(Some(coordination.exclusive().await?)),
            None => Ok(None),
        }
    }
}

impl<PS, SS> Repository<PS, SS>
where
    PS: BlobGc + 'static,
    SS: MetadataStore + 'static,
{
    pub(super) async fn try_reclaim_metadata(&self) -> Result<bool, RepositoryError> {
        if !self.payloads.metadata_reclaim_due().await? {
            return Ok(false);
        }
        let _guard = self.coordination.clone().try_lock_owned().map_err(|_| {
            RepositoryError::Busy(
                "another collection or maintenance operation is active".to_owned(),
            )
        })?;
        let Some(coordination) = self.profile.fs_coordination() else {
            return Ok(false);
        };
        let _fs_guard = coordination.try_exclusive().await?.ok_or_else(|| {
            RepositoryError::Busy(
                "another process holds collection or maintenance ownership".to_owned(),
            )
        })?;
        // Collector ownership does not exclude online publications. Fence this
        // handle's prepared catalog through reclamation, without making a new
        // mutation wait for the writer that currently owns publication. The
        // reverse wait is accepted: a publication that starts during the sweep
        // queues on this guard, exactly as its catalog preparation would queue
        // on the pack store's checkpoint lock, which reclamation holds for the
        // same span. Other handles and processes are fenced by the collector
        // lease and the catalog pins, not by this guard.
        let publication_guard = self.publication.try_lock()?;
        let repository = self.clone();
        crate::metadata::run_lease_task("metadata collection", |send| async move {
            // Keep collector history and ownership through every catalog
            // deletion, including when the caller cancels during admission.
            let _guard = _guard;
            let _fs_guard = _fs_guard;
            let _publication_guard = publication_guard;
            let scope = repository.payloads.write_scope();
            let result = scope
                .run(async {
                    if !repository.payloads.metadata_reclaim_due().await? {
                        return Ok(false);
                    }
                    crate::metadata::flush_pin_releases().await;
                    let pins = repository.state.pin_store().await?;
                    let collector =
                        crate::metadata::CollectorLease::try_acquire(pins.clone(), None)
                            .await?
                            .ok_or_else(|| {
                                RepositoryError::Busy(
                                    "another collector owns the pin ledger".into(),
                                )
                            })?;
                    repository.retained_snapshot().await?;
                    repository
                        .payloads
                        .reclaim_metadata_pinned(pins, BTreeSet::new())
                        .await?;
                    collector.finish().await?;
                    Ok(true)
                })
                .await;
            drop(send.send(result));
            Ok(())
        })
        .await?
    }

    /// Preview collection under exclusive ownership without changing logical
    /// or physical state.
    #[tracing::instrument(
        name = "repository.collection.preview",
        skip_all,
        fields(mode = "physical")
    )]
    pub async fn preview_collection(&self) -> Result<CollectionPreview, RepositoryError> {
        let guard = self.coordination.clone().lock_owned().await;
        let fs_guard = self.exclusive_fs().await?;
        let plan = self.collection_plan(guard, fs_guard, true).await?;
        tracing::info!(
            logical_objects = plan.preview.logical_objects,
            payload_blobs = plan.preview.payload_blobs,
            chunks = plan.preview.chunks,
            "collection preview completed"
        );
        plan.logical.protection.collector.finish().await?;
        Ok(plan.preview.clone())
    }

    /// Atomically prune unreachable logical records, then conservatively sweep
    /// physical payloads and chunks. Packing backends may durably defer a
    /// sparse pack rewrite; use [`Repository::vacuum`] when immediate physical
    /// reclamation matters. A physical deletion error leaves a valid logical
    /// state and merely leaks storage for a later retry.
    #[tracing::instrument(
        name = "repository.collection",
        skip_all,
        fields(mode = "physical", blocking = true, force_reclaim = false)
    )]
    pub async fn collect(&self) -> Result<CollectionOutcome, RepositoryError> {
        let outcome = self.run_collection(true, false).await?;
        tracing::info!(
            revision = ?outcome.revision,
            logical_objects = outcome.removed.logical_objects,
            payload_blobs = outcome.removed.payload_blobs,
            chunks = outcome.removed.chunks,
            spill_files = outcome.spill.files_opened,
            spill_peak_bytes = outcome.spill.peak_bytes,
            "collection completed"
        );
        Ok(outcome)
    }

    /// Resume an abandoned physical collection using its exact pin-ledger token.
    ///
    /// The caller MUST establish that the previous collector has stopped and
    /// all backend requests covered by its inherited deletion claims have
    /// settled. Cancellation, elapsed time,
    /// or a missing heartbeat does not establish this. For S3, recover its
    /// separate operational collector token first. Live reader and writer pins
    /// remain protected; this operation never clears their ownership.
    ///
    /// Recovery keeps every interrupted deletion claim and logical-prune fence
    /// through a fresh mark and successful metadata commit, then retries the
    /// sweep and catalog publication. Replaying an old token cannot take over a
    /// newer collector. Admission fails immediately if another collector is
    /// active. Once executing, recovery continues if its caller is cancelled.
    pub async fn recover_collection(
        &self,
        abandoned: &crate::metadata::PinToken,
    ) -> Result<CollectionOutcome, RepositoryError> {
        let guard = self
            .coordination
            .clone()
            .try_lock_owned()
            .map_err(|_| RepositoryError::Busy("another collector is active".into()))?;
        let fs_guard = match self.profile.fs_coordination() {
            Some(coordination) => {
                Some(coordination.try_exclusive().await?.ok_or_else(|| {
                    RepositoryError::Busy("another process owns collection".into())
                })?)
            }
            None => None,
        };
        let plan = self
            .collection_plan_with_recovery(guard, fs_guard, false, Some(abandoned), None)
            .await?;
        let outcome = self.execute_collection(plan, true).await?;
        if let Some(start) = self.profile.mutation_start() {
            start.collection_completed();
        }
        Ok(outcome)
    }

    /// Collect only if exclusive ownership is immediately available.
    ///
    /// This is intended for opportunistic housekeeping. It never waits for a
    /// mutation, stable reader, transfer, or another collector.
    #[tracing::instrument(
        name = "repository.collection",
        skip_all,
        fields(mode = "physical", blocking = false, force_reclaim = false)
    )]
    pub async fn try_collect(&self) -> Result<CollectionOutcome, RepositoryError> {
        self.run_collection(false, false).await
    }

    /// Collect unreachable state and force all durable packed garbage to be
    /// physically reclaimed, including sparse pack tombstones left by prior
    /// ordinary collections.
    #[tracing::instrument(
        name = "repository.collection",
        skip_all,
        fields(mode = "physical", blocking = true, force_reclaim = true)
    )]
    pub async fn vacuum(&self) -> Result<CollectionOutcome, RepositoryError> {
        let outcome = self.run_collection(true, true).await?;
        tracing::info!(
            revision = ?outcome.revision,
            logical_objects = outcome.removed.logical_objects,
            payload_blobs = outcome.removed.payload_blobs,
            chunks = outcome.removed.chunks,
            "vacuum completed"
        );
        Ok(outcome)
    }

    /// Vacuum only if exclusive ownership is immediately available.
    #[tracing::instrument(
        name = "repository.collection",
        skip_all,
        fields(mode = "physical", blocking = false, force_reclaim = true)
    )]
    pub async fn try_vacuum(&self) -> Result<CollectionOutcome, RepositoryError> {
        self.run_collection(false, true).await
    }

    pub(super) async fn run_collection(
        &self,
        wait: bool,
        force_reclaim: bool,
    ) -> Result<CollectionOutcome, RepositoryError> {
        let guard = if wait {
            self.coordination.clone().lock_owned().await
        } else {
            self.coordination.clone().try_lock_owned().map_err(|_| {
                RepositoryError::Busy(
                    "another collection or maintenance operation is active".to_owned(),
                )
            })?
        };
        let fs_guard = if wait {
            self.exclusive_fs().await?
        } else {
            match self.profile.fs_coordination() {
                Some(coordination) => {
                    Some(coordination.try_exclusive().await?.ok_or_else(|| {
                        RepositoryError::Busy(
                            "another process holds collection or maintenance ownership".to_owned(),
                        )
                    })?)
                }
                None => None,
            }
        };
        let plan = self.collection_plan(guard, fs_guard, wait).await?;
        let outcome = self.execute_collection(plan, force_reclaim).await?;
        if let Some(start) = self.profile.mutation_start() {
            start.collection_completed();
        }
        Ok(outcome)
    }

    #[tracing::instrument(
        name = "repository.collection.execute",
        skip_all,
        fields(force_reclaim = force_reclaim)
    )]
    pub(super) async fn execute_collection(
        &self,
        plan: CollectionPlan,
        force_reclaim: bool,
    ) -> Result<CollectionOutcome, RepositoryError> {
        let repository = self.clone();
        crate::metadata::run_lease_task("collection", |send| async move {
            let scope = repository.payloads.write_scope();
            let result = scope
                .run(repository.execute_collection_inner(plan, force_reclaim))
                .await;
            drop(send.send(result));
            Ok(())
        })
        .await?
    }

    pub(super) async fn execute_collection_inner(
        &self,
        mut plan: CollectionPlan,
        force_reclaim: bool,
    ) -> Result<CollectionOutcome, RepositoryError> {
        // No sweep operation reads the mark snapshot. Release it before
        // publication can checkpoint the WAL; pins, the expected revision,
        // and the frozen retained sets protect the collection protocol.
        drop(plan.logical.snapshot.take());
        if plan.preview.logical_objects == 0 && plan.logical.pins.logical_prune.is_none() {
            let revision = plan.logical.snapshot_revision;
            let (catalog_commit, removed) = self
                .sweep_and_publish_catalog(&mut plan, force_reclaim, revision, None)
                .await?;
            self.finish_payload_collection(&plan, force_reclaim).await?;
            self.reclaim_payload_metadata(&plan, force_reclaim).await?;
            self.finish_sweep_claims(&plan).await?;
            plan.logical.protection.collector.finish().await?;
            plan.logical.protection.release();
            return Ok(CollectionOutcome {
                revision: catalog_commit.map(|commit| commit.revision),
                removed,
                spill: plan.logical.area.metrics(),
            });
        }

        let retained = MetadataMutation::install_retained_source(plan.logical.live_objects.clone());
        let prune = || {
            self.publication.prune(
                plan.logical.protection.clone(),
                plan.logical.snapshot_revision,
                plan.logical.pins.clone(),
                plan.claims.lock().unwrap().clone(),
                plan.logical
                    .pins
                    .logical_prune
                    .clone()
                    .map(|prune| (plan.logical.protection.collector.token().clone(), prune)),
                retained.clone(),
            )
        };
        #[cfg(test)]
        let first_commit = if storage_full_once_for_test("CASITA_TEST_STORAGE_FULL_ONCE") {
            Err(RepositoryError::Metadata(MetadataError::StorageFull))
        } else {
            prune().await
        };
        #[cfg(not(test))]
        let first_commit = prune().await;

        let (commit, swept) = match first_commit {
            Ok(commit) => (commit, None),
            Err(RepositoryError::Metadata(MetadataError::StorageFull))
                if self.profile.collects_in_emergency() =>
            {
                let pins = self.state.pin_store().await?;
                let inventory = pins.inventory().await?;
                if !inventory.same_payload_pins(&plan.logical.pins) {
                    return Err(RepositoryError::Busy(
                        "pin scopes changed before emergency collection".into(),
                    ));
                }
                let claims = plan.claims.lock().unwrap().clone();
                let fence = match &inventory.logical_prune {
                    Some(fence)
                        if plan.logical.pins.logical_prune.as_ref() == Some(fence)
                            && inventory.collector.as_ref()
                                == Some(plan.logical.protection.collector.token()) =>
                    {
                        fence.clone()
                    }
                    Some(_) => {
                        return Err(RepositoryError::Busy(
                            "emergency recovery lost its logical fence".into(),
                        ));
                    }
                    None => pins
                        .begin_prune_recovering(inventory.revision, claims)
                        .await?
                        .ok_or_else(|| {
                            RepositoryError::Busy(
                                "emergency prune admission conflicted with a pin".into(),
                            )
                        })?,
                };
                plan.logical.protection.retain();
                plan.sweep_pins = Arc::new(crate::metadata::PruningPinStore {
                    inner: pins.clone(),
                    collector: plan.logical.protection.collector.token().clone(),
                    prune: fence.clone(),
                });
                // Keep the same logical fence across physical deletion and
                // metadata retry. Only its exact collector can claim paths;
                // newly rooted inputs cannot enter the pre-prune stale set.
                let removed = self.sweep_collection_payloads(&plan, true, false).await?;
                #[cfg(test)]
                abort_after_emergency_sweep_for_test();
                let inventory = pins.inventory().await?;
                if !inventory.same_payload_pins(&plan.logical.pins) {
                    return Err(RepositoryError::Busy(
                        "pin scopes changed during emergency collection".into(),
                    ));
                }
                if !plan.sweep_pins.allows_deletion(&inventory) {
                    return Err(RepositoryError::Busy(
                        "emergency collector lost its logical fence".into(),
                    ));
                }
                // The enclosing tracked operation owns this commit through
                // cancellation. A failed retry leaves the fence for recovery.
                let commit = self
                    .state
                    .commit(&plan.logical.snapshot_revision, retained)
                    .await?;
                pins.finish_prune(&fence).await?;
                plan.sweep_pins = pins;
                (commit, Some(removed))
            }
            Err(error) => return Err(error),
        };

        let (catalog_commit, removed) = self
            .sweep_and_publish_catalog(&mut plan, force_reclaim, commit.revision, swept)
            .await?;
        self.finish_payload_collection(&plan, force_reclaim).await?;
        self.reclaim_payload_metadata(&plan, force_reclaim).await?;
        self.finish_sweep_claims(&plan).await?;
        plan.logical.protection.collector.finish().await?;
        plan.logical.protection.release();

        Ok(CollectionOutcome {
            revision: Some(catalog_commit.map_or(commit.revision, |catalog| catalog.revision)),
            removed,
            spill: plan.logical.area.metrics(),
        })
    }

    /// A small SQLite prune can succeed by reusing WAL space while the payload
    /// filesystem is full. Recover allocation failures in the subsequent sweep
    /// or external-root upload under the same durable emergency fencing used
    /// before a logical prune. Generic, non-colocated stores never take this path.
    pub(super) async fn sweep_and_publish_catalog(
        &self,
        plan: &mut CollectionPlan,
        force_reclaim: bool,
        revision: crate::RepositoryRevision,
        mut removed: Option<CollectionPreview>,
    ) -> Result<(Option<CommitResult>, CollectionPreview), RepositoryError> {
        let result = async {
            #[cfg(test)]
            if storage_full_once_for_test("CASITA_TEST_POST_PRUNE_STORAGE_FULL_ONCE") {
                return Err(RepositoryError::Io(std::io::ErrorKind::StorageFull.into()));
            }
            if removed.is_none() {
                removed = Some(
                    self.sweep_collection_payloads(plan, force_reclaim, true)
                        .await?,
                );
            }
            self.commit_payload_catalog_from(Some(revision), plan.logical.protection.clone())
                .await
        }
        .await;
        match result {
            Ok(commit) => Ok((commit, removed.expect("successful sweep"))),
            Err(error) if self.profile.collects_in_emergency() && is_storage_full(&error) => {
                let pins = self.state.pin_store().await?;
                let inventory = pins.inventory().await?;
                if !inventory.same_payload_pins(&plan.logical.pins)
                    || inventory.logical_prune.is_some()
                    || inventory.collector.as_ref()
                        != Some(plan.logical.protection.collector.token())
                {
                    return Err(RepositoryError::Busy(
                        "pin scopes changed before post-prune emergency collection".into(),
                    ));
                }
                let claims = plan.claims.lock().unwrap().clone();
                let fence = pins
                    .begin_prune_recovering(inventory.revision, claims)
                    .await?
                    .ok_or_else(|| {
                        RepositoryError::Busy(
                            "post-prune emergency admission conflicted with a pin".into(),
                        )
                    })?;
                plan.logical.protection.retain();
                plan.sweep_pins = Arc::new(crate::metadata::PruningPinStore {
                    inner: pins.clone(),
                    collector: plan.logical.protection.collector.token().clone(),
                    prune: fence.clone(),
                });
                let swept = self.sweep_collection_payloads(plan, true, false).await?;
                #[cfg(test)]
                abort_after_emergency_sweep_for_test();
                let inventory = pins.inventory().await?;
                if !inventory.same_payload_pins(&plan.logical.pins)
                    || !plan.sweep_pins.allows_deletion(&inventory)
                {
                    return Err(RepositoryError::Busy(
                        "post-prune emergency collection lost its fence or pin scopes".into(),
                    ));
                }
                let commit = self
                    .commit_payload_catalog_from(Some(revision), plan.logical.protection.clone())
                    .await?;
                pins.finish_prune(&fence).await?;
                plan.sweep_pins = pins;
                Ok((commit, removed.unwrap_or(swept)))
            }
            Err(error) => Err(error),
        }
    }

    pub(super) async fn finish_payload_collection(
        &self,
        plan: &CollectionPlan,
        force_reclaim: bool,
    ) -> Result<(), RepositoryError> {
        let _phase = CollectionPhase::new("finish_payload_collection");
        let owned_claims = plan.claims.lock().unwrap().clone();
        self.payloads
            .finish_collection_pinned(force_reclaim, self.state.pin_store().await?, owned_claims)
            .await?;
        Ok(())
    }

    pub(super) async fn reclaim_payload_metadata(
        &self,
        plan: &CollectionPlan,
        force_reclaim: bool,
    ) -> Result<(), RepositoryError> {
        let _phase = CollectionPhase::new("reclaim_payload_metadata");
        // Maintenance keeps its admission hold through catalog publication or
        // discard. Exclusive collection therefore fences its inputs and output.
        if force_reclaim {
            // A new publication may prepare its catalog after collection's
            // catalog commit. Defer reclamation while that publication owns
            // the lock, leaving the reclaim marker for the next attempt.
            let _publication_guard = match self.publication.try_lock() {
                Ok(guard) => guard,
                Err(RepositoryError::Busy(_)) => return Ok(()),
                Err(error) => return Err(error),
            };
            let owned_claims = plan.claims.lock().unwrap().clone();
            self.payloads
                .reclaim_metadata_pinned(self.state.pin_store().await?, owned_claims)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn finish_sweep_claims(
        &self,
        plan: &CollectionPlan,
    ) -> Result<(), RepositoryError> {
        let _phase = CollectionPhase::new("finish_sweep_claims");
        let pins = self.state.pin_store().await?;
        let tokens = plan
            .claims
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        for token in tokens {
            pins.finish_deletions(&token).await?;
            plan.claims.lock().unwrap().remove(&token);
        }
        Ok(())
    }

    pub(super) async fn sweep_page(
        &self,
        plan: &CollectionPlan,
        mut page: SweepPage,
        logical_pruned: bool,
    ) -> Result<usize, RepositoryError> {
        use crate::metadata::PinResource;
        let pins = plan.sweep_pins.clone();
        // A page only shrinks. Once an immutable manifest's descendants have
        // been excluded, later retries need not read that manifest again.
        let mut expanded_blobs: BTreeSet<_> = plan
            .logical
            .pins
            .pins
            .values()
            .flat_map(|pin| &pin.resources)
            .filter_map(|resource| match resource {
                PinResource::Blob(blob) => Some(*blob),
                _ => None,
            })
            .collect();
        let claim_phase = CollectionPhase::new(match &page {
            SweepPage::Blobs(_) => "sweep_blob_claims",
            SweepPage::Chunks(_) => "sweep_chunk_claims",
        });
        loop {
            let _attempt = CollectionPhase::new(match &page {
                SweepPage::Blobs(_) => "sweep_blob_claim_attempt",
                SweepPage::Chunks(_) => "sweep_chunk_claim_attempt",
            });
            let inventory = pins.inventory().await?;
            if inventory.collector.as_ref() != plan.logical.pins.collector.as_ref() {
                return Err(RepositoryError::Busy("collection ownership changed".into()));
            }
            if !logical_pruned && !inventory.same_payload_pins(&plan.logical.pins) {
                return Err(RepositoryError::Busy(
                    "pin scopes changed during emergency collection sweep".into(),
                ));
            }
            // After logical pruning, later publications must protect every
            // physical identity before reuse. Collector history keeps those
            // resources visible even when the publishing session has ended.
            // Emergency sweep still needs the original logical mark unchanged.
            let protected: BTreeSet<_> = inventory
                .pins
                .values()
                .flat_map(|pin| pin.resources.iter())
                .collect();
            let mut checked_blobs = expanded_blobs.clone();
            let resources: BTreeSet<_> = match &mut page {
                SweepPage::Blobs(blobs) => {
                    blobs.retain(|blob| !protected.contains(&PinResource::Blob(*blob)));
                    blobs.iter().copied().map(PinResource::Blob).collect()
                }
                SweepPage::Chunks(chunks) => {
                    chunks.retain(|chunk| !protected.contains(&PinResource::Chunk(*chunk)));
                    // Existing-payload imports may pin a manifest without
                    // uploading its chunks. Expand new manifest pins before
                    // claiming remaining chunks; admission rejects any further
                    // manifest that has not been checked against this page.
                    for resource in &protected {
                        let PinResource::Blob(blob) = resource else {
                            continue;
                        };
                        if expanded_blobs.contains(blob) {
                            continue;
                        }
                        if chunks.is_empty() {
                            break;
                        }
                        let present = self.payloads.has(blob).await?;
                        if let Some(manifest) = self.payloads.chunks_for_gc(blob, present).await? {
                            let descendants: BTreeSet<_> =
                                manifest.into_iter().map(|chunk| chunk.digest).collect();
                            chunks.retain(|chunk| !descendants.contains(chunk));
                            expanded_blobs.insert(*blob);
                        }
                        // An absent manifest is checked for this attempt,
                        // but must be re-read on retry if it has since appeared.
                        checked_blobs.insert(*blob);
                    }
                    chunks.iter().copied().map(PinResource::Chunk).collect()
                }
            };
            if resources.is_empty() {
                return Ok(0);
            }
            let owned = plan.claims.lock().unwrap().clone();
            let covered: BTreeSet<_> = inventory
                .deletions
                .iter()
                .filter(|(token, _)| owned.contains(token))
                .flat_map(|(_, resources)| resources.iter().cloned())
                .collect();
            let resources: BTreeSet<_> = resources.difference(&covered).cloned().collect();
            if !pins.allows_deletion(&inventory)
                || inventory
                    .deletions
                    .iter()
                    .any(|(token, claim)| !owned.contains(token) && !claim.is_disjoint(&resources))
            {
                return Err(RepositoryError::Busy(
                    "collection deletion is fenced by another operation".into(),
                ));
            }
            if !resources.is_empty() {
                let claimed = if logical_pruned {
                    pins.claim_deletions_validated(
                        plan.logical.protection.collector.token(),
                        resources,
                        checked_blobs,
                    )
                    .await?
                } else {
                    pins.claim_deletions(inventory.revision, resources).await?
                };
                let Some(token) = claimed else {
                    // A concurrent pin won admission. Refresh this bounded page
                    // and skip its resources instead of restarting the mark.
                    continue;
                };
                plan.claims.lock().unwrap().insert(token);
            }
            break;
        }
        drop(claim_phase);
        let _delete_phase = CollectionPhase::new(match &page {
            SweepPage::Blobs(_) => "sweep_delete_blobs",
            SweepPage::Chunks(_) => "sweep_delete_chunks",
        });
        plan.logical.protection.retain();
        // The enclosing tracked collection owns this I/O through cancellation.
        // Claims remain until deferred physical cleanup has also settled.
        let removed = match page {
            SweepPage::Blobs(blobs) => {
                let owned_claims = plan.claims.lock().unwrap().clone();
                self.payloads
                    .delete_blobs_pinned(&blobs, pins, owned_claims, !logical_pruned)
                    .await?
            }
            SweepPage::Chunks(chunks) => {
                let owned_claims = plan.claims.lock().unwrap().clone();
                self.payloads
                    .delete_chunks_pinned(&chunks, pins, owned_claims)
                    .await?
            }
        };
        Ok(removed)
    }

    /// Delete the physical data the plan classified as stale, one bounded page
    /// at a time so a sweep never materializes the whole stale inventory.
    #[tracing::instrument(
        name = "repository.collection.sweep",
        skip_all,
        fields(force_reclaim = force_reclaim)
    )]
    pub(super) async fn sweep_collection_payloads(
        &self,
        plan: &CollectionPlan,
        force_reclaim: bool,
        logical_pruned: bool,
    ) -> Result<CollectionPreview, RepositoryError> {
        let _phase = CollectionPhase::new("sweep_payloads");
        let mut removed = CollectionPreview {
            logical_objects: plan.preview.logical_objects,
            payload_blobs: 0,
            chunks: 0,
        };
        let mut after = None;
        loop {
            let page = plan.stale_blobs.page(after, SWEEP_PAGE).await?;
            let Some(last) = page.last().copied() else {
                break;
            };
            removed.payload_blobs += self
                .sweep_page(plan, SweepPage::Blobs(page), logical_pruned)
                .await?;
            after = Some(last);
        }
        let mut after = None;
        loop {
            let page = plan.stale_chunks.page(after, SWEEP_PAGE).await?;
            let Some(last) = page.last().copied() else {
                break;
            };
            removed.chunks += self
                .sweep_page(plan, SweepPage::Chunks(page), logical_pruned)
                .await?;
            after = Some(last);
        }
        plan.logical.protection.retain();
        let owned_claims = plan.claims.lock().unwrap().clone();
        let _finish_phase = CollectionPhase::new("finish_deletions");
        self.payloads
            .finish_deletions_pinned(
                force_reclaim,
                plan.sweep_pins.clone(),
                owned_claims,
                !logical_pruned,
            )
            .await?;
        Ok(removed)
    }

    #[tracing::instrument(
        name = "repository.collection.mark",
        skip_all,
        fields(mode = "physical")
    )]
    pub(super) async fn collection_plan(
        &self,
        guard: OwnedMutexGuard<()>,
        fs_guard: Option<ExclusiveLease>,
        wait: bool,
    ) -> Result<CollectionPlan, RepositoryError> {
        self.collection_plan_with_recovery(guard, fs_guard, wait, None, None)
            .await
    }

    pub(super) async fn collection_plan_with_recovery(
        &self,
        guard: OwnedMutexGuard<()>,
        fs_guard: Option<ExclusiveLease>,
        wait: bool,
        recovery: Option<&crate::metadata::PinToken>,
        remote: Option<crate::metadata::RepositoryLease>,
    ) -> Result<CollectionPlan, RepositoryError> {
        let _phase = CollectionPhase::new("physical_plan");
        let logical = self
            .logical_collection_plan_with_recovery(guard, fs_guard, wait, recovery, remote)
            .await?;
        let _physical_mark_phase = CollectionPhase::new("physical_mark");
        if !logical.pins.deletions.is_empty()
            && !self.profile.collects_in_emergency()
            && recovery.is_none()
        {
            return Err(RepositoryError::Busy(
                "interrupted deletion claims require exact-token recovery".into(),
            ));
        }
        // The standard local profile owns the filesystem collector fence.
        // Its previous owner has either finished its tracked task or exited,
        // so local filesystem I/O cannot still arrive from that operation.
        // Preserve its claims throughout retry; never reopen admission first.
        let claims = Arc::new(std::sync::Mutex::new(
            logical.pins.deletions.keys().cloned().collect(),
        ));
        self.publication
            .synchronize(
                &self.payloads,
                logical.snapshot.as_ref().expect("mark snapshot").as_ref(),
            )
            .await?;
        let area = logical.area.clone();

        // Every inventory below is read in fixed-size pages, and every set
        // spills onto this operation's shared temporary-storage budget. A
        // repository larger than memory can therefore be collected without
        // re-materializing its live graph.
        let mut live_payloads = SpillSet::new(area.clone(), "live-payloads");
        let mut after = None;
        loop {
            let keys = logical
                .live_objects
                .page(after.clone(), CLOSURE_FRONTIER)
                .await?;
            let Some(last) = keys.last().cloned() else {
                break;
            };
            let found = logical
                .snapshot
                .as_ref()
                .expect("mark snapshot")
                .object_batch(&keys)
                .await?;
            for (key, record) in keys.into_iter().zip(found) {
                let record = record.ok_or_else(|| {
                    MetadataError::Corruption(format!(
                        "marked object {key} is absent from its snapshot"
                    ))
                })?;
                live_payloads.insert(record.payload()).await?;
            }
            after = Some(last);
        }

        let mut live_chunks = SpillSet::new(area.clone(), "live-chunks");
        let live_payloads = live_payloads.freeze().await?;

        // Build the manifest inventory once. Besides classifying stale
        // manifests below, it lets packed stores identify manifest-elided
        // one-chunk payloads without one guaranteed-miss object-store GET per
        // payload during the chunk mark pass.
        let mut physical_blobs = SpillSet::new(area.clone(), "physical-blobs");
        let mut blobs = self.payloads.list_blobs();
        while let Some(blob) = blobs.next().await {
            physical_blobs.insert(blob?).await?;
        }
        drop(blobs);
        let physical_blobs = physical_blobs.freeze().await?;
        let physical_blobs = &physical_blobs;

        let mut after = None;
        loop {
            let payloads = live_payloads.page(after, CLOSURE_FRONTIER).await?;
            let Some(last) = payloads.last().copied() else {
                break;
            };
            let reads = payloads.into_iter().map(|payload| async move {
                let manifest_present = physical_blobs.contains(&payload).await?;
                let chunks = self
                    .payloads
                    .chunks_for_gc(&payload, manifest_present)
                    .await?
                    .ok_or_else(|| {
                        MetadataError::Corruption(format!(
                            "reachable object payload {payload} is physically absent"
                        ))
                    })?;
                Ok::<_, RepositoryError>(chunks)
            });
            let reads = futures::stream::iter(reads).buffered(COLLECTION_PAYLOAD_READS);
            futures::pin_mut!(reads);
            while let Some(chunks) = reads.next().await {
                let chunks = chunks?;
                for chunk in chunks {
                    live_chunks.insert(chunk.digest).await?;
                }
            }
            after = Some(last);
        }

        // A writer may own bytes before publishing any logical record. Its
        // manifest may still be in flight; retain existing representations and
        // tolerate the absent ones until the write completes.
        let mut pinned_payloads = SpillSet::new(area.clone(), "pinned-payloads");
        for pin in logical.pins.pins.values() {
            for resource in &pin.resources {
                match resource {
                    crate::metadata::PinResource::Blob(blob) => {
                        pinned_payloads.insert(*blob).await?;
                    }
                    crate::metadata::PinResource::Chunk(chunk) => {
                        live_chunks.insert(*chunk).await?;
                    }
                    _ => {}
                }
            }
        }
        let pinned_payloads = pinned_payloads.freeze().await?;
        let mut after = None;
        loop {
            let page = pinned_payloads.page(after, CLOSURE_FRONTIER).await?;
            let Some(last) = page.last().copied() else {
                break;
            };
            for payload in page {
                if live_payloads.contains(&payload).await? {
                    continue;
                }
                let present = physical_blobs.contains(&payload).await?;
                if let Some(chunks) = self.payloads.chunks_for_gc(&payload, present).await? {
                    for chunk in chunks {
                        live_chunks.insert(chunk.digest).await?;
                    }
                }
            }
            after = Some(last);
        }

        let mut stale_blobs = SpillSet::new(area.clone(), "stale-blobs");
        let mut after = None;
        loop {
            let blobs = physical_blobs.page(after, CLOSURE_FRONTIER).await?;
            let Some(last) = blobs.last().copied() else {
                break;
            };
            for blob in blobs {
                if !live_payloads.contains(&blob).await? && !pinned_payloads.contains(&blob).await?
                {
                    stale_blobs.insert(blob).await?;
                }
            }
            after = Some(last);
        }

        drop(live_payloads);
        let live_chunks = live_chunks.freeze().await?;
        let mut stale_chunks = SpillSet::new(area.clone(), "stale-chunks");
        let mut chunks = self.payloads.list_chunks();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            if !live_chunks.contains(&chunk).await? {
                stale_chunks.insert(chunk).await?;
            }
        }
        drop(chunks);

        let preview = CollectionPreview {
            logical_objects: logical.preview.logical_objects,
            payload_blobs: stale_blobs.len(),
            chunks: stale_chunks.len(),
        };
        // Frozen from here on: the plan only reads these sets, in pages, and
        // the retained set may be read twice if the commit is retried.
        let stale_blobs = stale_blobs.freeze().await?;
        let stale_chunks = stale_chunks.freeze().await?;

        // Carry collector ownership and exact claims through mark, prune, and
        // sweep. Readers and writers continue through disjoint online pins.
        Ok(CollectionPlan {
            logical,
            sweep_pins: self.state.pin_store().await?,
            claims,
            stale_blobs,
            stale_chunks,
            preview,
        })
    }
}

/// Stale physical entries deleted per bounded sweep page.
const SWEEP_PAGE: usize = 1024;

/// Concurrent payload inventories during collection's chunk mark pass.
const COLLECTION_PAYLOAD_READS: usize = 8;

#[cfg(test)]
fn abort_after_emergency_sweep_for_test() {
    if std::env::var_os("CASITA_TEST_ABORT_AFTER_EMERGENCY_SWEEP").is_some() {
        std::process::abort();
    }
}

/// Whether this collection should behave as though the state engine had no
/// room left to commit in.
///
/// Reaching the emergency path for real needs a filesystem at zero free space,
/// and creating one without disturbing the host needs a mount namespace that
/// not every machine grants an unprivileged process. Standing in for it here
/// keeps the crash and the recovery after it testable anywhere; the
/// filesystem-level proof that a real ENOSPC arrives as `StorageFull` lives in
/// `tests/full_disk_gc.rs`, which runs wherever namespaces are permitted.
#[cfg(test)]
fn storage_full_once_for_test(variable: &str) -> bool {
    use std::sync::atomic::{AtomicBool, Ordering};
    static SPENT: AtomicBool = AtomicBool::new(false);
    std::env::var_os(variable).is_some() && !SPENT.swap(true, Ordering::SeqCst)
}

pub(super) struct ExclusiveProtection {
    pub(super) collector: crate::metadata::CollectorLease,
    pub(super) _guard: OwnedMutexGuard<()>,
    pub(super) _fs_guard: Option<ExclusiveLease>,
    pub(super) remote: std::sync::Mutex<crate::RepositoryLease>,
}

impl ExclusiveProtection {
    pub(super) fn retain(&self) {
        self.remote.lock().unwrap().retain_on_drop();
    }
    pub(super) fn release(&self) {
        self.remote.lock().unwrap().release_on_drop();
    }
}

pub(super) struct LogicalCollectionPlan {
    pub(super) protection: Arc<ExclusiveProtection>,
    pub(super) area: SpillArea,
    pub(super) snapshot_revision: crate::RepositoryRevision,
    pub(super) pins: Arc<crate::metadata::PinInventory>,
    pub(super) snapshot: Option<Arc<dyn MetadataSnapshot>>,
    pub(super) live_objects: Arc<FrozenSpillSet<ObjectKey>>,
    pub(super) preview: LogicalCollectionPreview,
}

pub(super) enum SweepPage {
    Blobs(Vec<BlobId>),
    Chunks(Vec<ChunkId>),
}

pub(super) struct CollectionPlan {
    pub(super) logical: LogicalCollectionPlan,
    pub(super) sweep_pins: Arc<dyn crate::metadata::PinStore>,
    pub(super) claims: Arc<std::sync::Mutex<BTreeSet<crate::metadata::PinToken>>>,
    pub(super) stale_blobs: FrozenSpillSet<BlobId>,
    pub(super) stale_chunks: FrozenSpillSet<ChunkId>,
    pub(super) preview: CollectionPreview,
}

/// Mark everything reachable from every named root.
///
/// The marked set spills like any other traversal, so collection on a
/// repository larger than memory is bounded by the traversal limits rather
/// than by the size of the live graph.
pub(super) async fn mark_named_roots(
    snapshot: &dyn MetadataSnapshot,
    max_objects: usize,
    area: &SpillArea,
) -> Result<SpillSet<ObjectKey>, RepositoryError> {
    let mut marked = SpillSet::new(area.clone(), "marked");
    let mut queue = TraversalQueue::new(area.clone());
    let mut roots = snapshot.roots();
    while let Some(root) = roots.next().await {
        let root = root?;
        queue.push((None, root.target().clone())).await?;
    }
    drop(roots);
    loop {
        // Resolved a frontier at a time for the same reason the closure walk
        // is: the mark pass reads every live record, and a per-record state
        // call costs far more than the record.
        let mut frontier = Vec::new();
        while frontier.len() < CLOSURE_FRONTIER {
            let Some(step) = queue.pop().await? else {
                break;
            };
            frontier.push(step);
        }
        if frontier.is_empty() {
            break;
        }
        let keys: Vec<_> = frontier.iter().map(|(_, key)| key.clone()).collect();
        let found = snapshot.object_batch(&keys).await?;

        for ((from, key), record) in frontier.into_iter().zip(found) {
            if !marked.insert(key.clone()).await? {
                continue;
            }
            if marked.len() > max_objects {
                return Err(RepositoryError::LimitExceeded(format!(
                    "collection mark exceeded {max_objects} objects"
                )));
            }
            let record = record.ok_or_else(|| {
                MetadataError::Corruption(match from {
                    Some(from) => format!("rooted object {from} links to missing {key}"),
                    None => format!("named root targets missing object {key}"),
                })
            })?;
            for target in record.links() {
                queue.push((Some(key.clone()), target.clone())).await?;
            }
        }
    }
    Ok(marked)
}

/// Extend the strict named-root mark with live operation scopes. A writer can
/// pin an input before its record exists, and an unrestricted snapshot can
/// contain incomplete unrooted records, so these scopes tolerate absent links.
pub(super) async fn mark_pin_scopes(
    snapshot: &dyn MetadataSnapshot,
    pins: &crate::metadata::PinInventory,
    marked: &mut SpillSet<ObjectKey>,
    max_objects: usize,
    area: &SpillArea,
) -> Result<(), RepositoryError> {
    use crate::metadata::{PinResource, PinScope};

    let mut queue = TraversalQueue::new(area.clone());
    if let Some(generation) = pins
        .pins
        .values()
        .filter_map(|pin| match pin.scope {
            PinScope::Snapshot { generation } => Some(generation),
            _ => None,
        })
        .max()
    {
        let mut records = snapshot.objects_created_through(generation);
        while let Some(record) = records.next().await {
            let record = record?;
            if marked.insert(record.key().clone()).await? {
                // An old incomplete object can gain a committed dependency
                // later. Retain that dependency to keep the current graph valid.
                for target in record.links() {
                    queue.push((None, target.clone())).await?;
                }
            }
            if marked.len() > max_objects {
                return Err(RepositoryError::LimitExceeded(format!(
                    "collection mark exceeded {max_objects} objects"
                )));
            }
        }
    }

    for pin in pins.pins.values() {
        if let PinScope::Closures(roots) = &pin.scope {
            for root in roots {
                queue.push((None, root.clone())).await?;
            }
        }
        for resource in &pin.resources {
            if let PinResource::Object(key) = resource {
                queue.push((None, key.clone())).await?;
            }
        }
    }
    loop {
        let mut keys = Vec::new();
        while keys.len() < CLOSURE_FRONTIER {
            let Some((_, key)) = queue.pop().await? else {
                break;
            };
            keys.push(key);
        }
        if keys.is_empty() {
            return Ok(());
        }
        let records = snapshot.object_batch(&keys).await?;
        for (key, record) in keys.into_iter().zip(records) {
            let Some(record) = record else { continue };
            if !marked.insert(key).await? {
                continue;
            }
            if marked.len() > max_objects {
                return Err(RepositoryError::LimitExceeded(format!(
                    "collection mark exceeded {max_objects} objects"
                )));
            }
            for target in record.links() {
                queue.push((None, target.clone())).await?;
            }
        }
    }
}
