//! Repository integrity checks and physical repair.

use super::*;

/// How an integrity finding affects repository correctness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsckDisposition {
    /// Reachable state violates a repository invariant.
    Corrupt,
    /// Unrooted staging residue or unreferenced physical data may be collected.
    Collectible,
    /// Exact validation could not run because a format is unavailable.
    Unchecked,
}

/// Stable category of an integrity finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsckIssueKind {
    /// Primary state could not be decoded or enumerated.
    StateEncoding,
    /// A stored forward link or root target has no record.
    MissingRecord,
    /// A record's payload is physically absent.
    MissingPayload,
    /// Payload identity, canonical encoding, stored links, or a direct relation
    /// failed verification.
    InvalidObject,
    /// No verifier is registered for the object's namespace.
    UnsupportedNamespace,
    /// A valid logical record is unreachable from every named root.
    UnrootedObject,
    /// Physical data is referenced by no logical record.
    UnreferencedPayload,
    /// A physical chunk is referenced by no present logical payload.
    UnreferencedChunk,
}

/// One deterministic integrity finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsckIssue {
    /// Correctness disposition.
    pub disposition: FsckDisposition,
    /// Machine-readable category.
    pub kind: FsckIssueKind,
    /// Logical object involved, when one can be identified.
    pub object: Option<ObjectKey>,
    /// Human-readable detail.
    pub message: String,
}

/// Integrity report over one logical snapshot and an exclusively held physical
/// store view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsckReport {
    /// Exact logical revision inspected.
    pub revision: crate::RepositoryRevision,
    /// Successfully decoded logical records inspected.
    pub objects_checked: usize,
    /// Successfully decoded named roots inspected.
    pub roots_checked: usize,
    /// Unique record payload identities checked.
    pub payloads_checked: usize,
    /// Temporary traversal-state use while inspecting this repository.
    pub spill: SpillMetrics,
    /// Findings in deterministic logical order followed by physical order.
    pub issues: Vec<FsckIssue>,
}

/// The remaining condition of one physical representation after an fsck repair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsckRepairFindingKind {
    /// A payload named by an authoritative record is absent.
    MissingPayload,
    /// A payload failed a checksum, size, manifest, or chunk verification.
    CorruptPayload,
    /// A payload could not be checked because storage returned an unrelated
    /// operational failure.
    UnavailablePayload,
    /// An existing Bao outboard does not match the verified payload.
    CorruptOutboard,
    /// An existing Bao outboard could not be read or verified.
    UnavailableOutboard,
}

/// One unresolved physical-storage condition reported by [`FsckRepairReport`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsckRepairFinding {
    /// Stable category of the condition.
    pub kind: FsckRepairFindingKind,
    /// Authoritative payload identity affected by the condition.
    pub payload: BlobId,
    /// Expected plaintext payload bytes, when known from the object record.
    pub bytes: u64,
    /// Human-readable backend or verification detail.
    pub message: String,
}

/// A safe physical action an fsck repair can plan or complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsckRepairActionKind {
    /// Replaced a missing or corrupt local representation from a replica.
    RepairPayloadFromReplica,
    /// Recomputed an existing Bao outboard from verified payload bytes.
    RebuildOutboard,
}

/// Whether an fsck-repair action was only reported or actually completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsckRepairActionStatus {
    /// Dry-run found an action that a normal fsck repair would perform.
    Planned,
    /// The action completed and its replacement was verified before use.
    Repaired,
}

/// One safe action reported by [`FsckRepairReport`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsckRepairAction {
    /// What the fsck repair rebuilt or replaced.
    pub kind: FsckRepairActionKind,
    /// Payload identity whose representation was affected.
    pub payload: BlobId,
    /// Plaintext bytes represented by the action.
    pub bytes: u64,
    /// Whether this was a dry-run plan or a completed repair.
    pub status: FsckRepairActionStatus,
}

/// Physical integrity report for one retained repository snapshot.
///
/// A normal fsck repair repairs only a corrupt local representation when an
/// independently verified replica is supplied, and rebuilds an already
/// present Bao outboard. It never modifies object records, roots, or other
/// logical state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsckRepairReport {
    /// Exact logical snapshot whose authoritative payload identities were
    /// inspected.
    pub revision: crate::RepositoryRevision,
    /// Unique payload identities inspected.
    pub payloads_checked: usize,
    /// Plaintext bytes completely verified from the local representation.
    pub bytes_checked: u64,
    /// Existing Bao outboards compared against recomputed values.
    pub outboards_checked: usize,
    /// Repairs planned or performed, in payload order.
    pub actions: Vec<FsckRepairAction>,
    /// Conditions that could not be safely repaired.
    pub findings: Vec<FsckRepairFinding>,
    /// Temporary traversal-state use while deduplicating payload identities.
    pub spill: SpillMetrics,
}

impl FsckRepairReport {
    /// Whether every inspected physical representation is healthy after the
    /// requested fsck-repair mode completed.
    pub fn is_healthy(&self) -> bool {
        self.findings.is_empty()
    }
}

impl FsckReport {
    /// Whether no reachable corruption was found. Unrooted garbage and
    /// unavailable format implementations do not make this false.
    pub fn is_healthy(&self) -> bool {
        !self
            .issues
            .iter()
            .any(|issue| issue.disposition == FsckDisposition::Corrupt)
    }

    /// Whether every object could be checked with no findings of any kind.
    pub fn is_clean(&self) -> bool {
        self.issues.is_empty()
    }
}

impl<SS> Repository<crate::ChunkedBlobStore, SS>
where
    SS: MetadataStore,
{
    /// Verify every payload named by one retained snapshot and automatically
    /// repair only safe physical state.
    ///
    /// A supplied `replica` is held for the complete operation and is read
    /// independently before it can replace a missing or corrupt local
    /// representation. The replacement manifest is published only after the
    /// complete blob has been re-verified by the local store. Existing Bao
    /// outboards are rebuilt from local verified bytes when they differ.
    ///
    /// This method never changes logical records or roots. A coordinating state
    /// backend may receive the repaired physical catalog in a metadata-only
    /// commit. Its retention pin keeps the inspected snapshot’s data alive
    /// while collection, ordinary readers and publication proceed. Concurrent fsck
    /// repairs may repeat identical work, but every physical publication is
    /// content-addressed and verified, so they cannot produce a different
    /// visible representation.
    pub async fn fsck_repair(
        &self,
        replica: Option<&Self>,
    ) -> Result<FsckRepairReport, RepositoryError> {
        // Keep the scan's buffers and async state out of callers' futures.
        // Nested inline copies can overflow ordinary Windows debug stacks.
        Box::pin(self.fsck_repair_inner(replica, false)).await
    }

    /// Report the safe repairs [`fsck_repair`](Self::fsck_repair) would perform without
    /// changing physical or logical state.
    pub async fn preview_fsck_repair(
        &self,
        replica: Option<&Self>,
    ) -> Result<FsckRepairReport, RepositoryError> {
        Box::pin(self.fsck_repair_inner(replica, true)).await
    }

    #[tracing::instrument(
        name = "repository.fsck_repair",
        skip_all,
        fields(dry_run = dry_run, replica = replica.is_some())
    )]
    pub(super) async fn fsck_repair_inner(
        &self,
        replica: Option<&Self>,
        dry_run: bool,
    ) -> Result<FsckRepairReport, RepositoryError> {
        // A payload this scan already verified may be damaged, and read, before
        // the scan ends. Capturing the generation first lets the store refuse
        // to lift the audit requirement that such a read placed meanwhile.
        let audit_generation = match &self.nar_store {
            Some(store) => store.generation().await?,
            None => 0,
        };
        // Pins retain source and target data through verification, repair,
        // and publication while collection can reclaim unrelated data.
        let target_hold = self.retention_hold().await?;
        let _replica_hold = match replica {
            Some(replica) => Some(replica.retention_hold().await?),
            None => None,
        };
        let snapshot = target_hold.snapshot();
        let revision = snapshot.revision();
        let area = self.spill_area();
        let mut seen = SpillSet::new(area.clone(), "fsck-repair-payloads");
        let mut report = FsckRepairReport {
            revision,
            payloads_checked: 0,
            bytes_checked: 0,
            outboards_checked: 0,
            actions: Vec::new(),
            findings: Vec::new(),
            spill: SpillMetrics::default(),
        };

        // Bao outboards are optional and uncommon for small-object trees.
        // Inventory the exact namespace once instead of performing one
        // guaranteed-miss filesystem/S3 lookup per payload.
        let mut physical_outboards = SpillSet::new(area.clone(), "fsck-repair-outboards");
        let mut outboards = self.payloads.list_outboards();
        while let Some(outboard) = outboards.next().await {
            physical_outboards.insert(outboard?).await?;
        }
        drop(outboards);

        let mut records = snapshot.objects_unordered();
        loop {
            let mut batch = Vec::with_capacity(FSCK_SCAN_WINDOW);
            while batch.len() < FSCK_SCAN_WINDOW {
                let Some(record) = records.next().await else {
                    break;
                };
                let record = record?;
                let payload = record.payload();
                if !seen.insert(payload).await? {
                    continue;
                }
                let order = self.payloads.physical_scan_order(&payload).await?;
                batch.push((order, payload, record.payload_size()));
            }
            if batch.is_empty() {
                break;
            }
            // State is ordered by logical object key, which is unrelated to
            // pack construction order. A scan in that order can reload the
            // same immutable packs thousands of times when the cache is
            // smaller than the repository. Sorting one bounded window keeps
            // memory independent of repository size and makes repeated reads
            // of a pack adjacent.
            batch.sort_unstable_by_key(|(pack, payload, _)| (pack.is_none(), *pack, *payload));
            if dry_run && replica.is_none() {
                // Read-only checks are independent, but waiting for every
                // payload and Bao proof in sequence amplifies storage latency.
                // Keep complete checks in a bounded window, consuming reports
                // in the original pack order. Repairs retain the serial path.
                let mut checks = Vec::with_capacity(batch.len());
                for (_, payload, payload_size) in batch {
                    let outboard_present = physical_outboards.contains(&payload).await?;
                    checks.push((payload, payload_size, outboard_present));
                }
                let revision = report.revision;
                let checks = futures::stream::iter(checks)
                    .map(|(payload, payload_size, outboard_present)| {
                        let mut checked = FsckRepairReport {
                            revision,
                            payloads_checked: 1,
                            bytes_checked: 0,
                            outboards_checked: 0,
                            actions: Vec::new(),
                            findings: Vec::new(),
                            spill: SpillMetrics::default(),
                        };
                        async move {
                            self.fsck_repair_payload(
                                None,
                                true,
                                payload,
                                payload_size,
                                outboard_present,
                                &mut checked,
                            )
                            .await?;
                            Ok::<_, RepositoryError>(checked)
                        }
                    })
                    .buffered(FSCK_READ_ONLY_CONCURRENCY);
                futures::pin_mut!(checks);
                while let Some(checked) = checks.next().await {
                    let mut checked = checked?;
                    report.payloads_checked = report
                        .payloads_checked
                        .checked_add(checked.payloads_checked)
                        .ok_or_else(|| {
                            RepositoryError::LimitExceeded(
                                "fsck repair payload count overflowed".to_owned(),
                            )
                        })?;
                    report.bytes_checked = report
                        .bytes_checked
                        .checked_add(checked.bytes_checked)
                        .ok_or_else(|| {
                            RepositoryError::LimitExceeded(
                                "fsck repair byte count overflowed".to_owned(),
                            )
                        })?;
                    report.outboards_checked = report
                        .outboards_checked
                        .checked_add(checked.outboards_checked)
                        .ok_or_else(|| {
                            RepositoryError::LimitExceeded(
                                "fsck repair outboard count overflowed".to_owned(),
                            )
                        })?;
                    report.actions.append(&mut checked.actions);
                    report.findings.append(&mut checked.findings);
                }
                continue;
            }
            for (_, payload, payload_size) in batch {
                report.payloads_checked =
                    report.payloads_checked.checked_add(1).ok_or_else(|| {
                        RepositoryError::LimitExceeded(
                            "fsck repair payload count overflowed".to_owned(),
                        )
                    })?;
                let outboard_present = physical_outboards.contains(&payload).await?;
                self.fsck_repair_payload(
                    replica,
                    dry_run,
                    payload,
                    payload_size,
                    outboard_present,
                    &mut report,
                )
                .await?;
            }
        }
        drop(records);
        if !dry_run {
            self.commit_payload_catalog(target_hold.protection.clone())
                .await?;
        }
        report.spill = area.metrics();
        // The scan is the audit's outcome: a failed association update is
        // logged (the store then fails closed in this process) rather than
        // discarding the report. A complete scan without findings proves
        // every payload intact, which lifts the native-audit requirement that
        // an earlier read failure placed on cold raw intake.
        if let Some(store) = &self.nar_store {
            if report.findings.is_empty() {
                match store.restore(audit_generation).await {
                    Ok(true) => (),
                    Ok(false) => tracing::info!(
                        "NAR audit requirement kept: a payload read failed during the scan"
                    ),
                    Err(error) => {
                        tracing::warn!(%error, "NAR audit requirement could not be lifted")
                    }
                }
            } else {
                store.record_read_failure().await;
            }
        }
        tracing::info!(
            revision = %report.revision,
            payloads_checked = report.payloads_checked,
            bytes_checked = report.bytes_checked,
            outboards_checked = report.outboards_checked,
            actions = report.actions.len(),
            findings = report.findings.len(),
            "physical repair scan completed"
        );
        Ok(report)
    }

    pub(super) async fn fsck_repair_payload(
        &self,
        replica: Option<&Self>,
        dry_run: bool,
        payload: BlobId,
        payload_size: u64,
        outboard_present: bool,
        report: &mut FsckRepairReport,
    ) -> Result<(), RepositoryError> {
        let local = verify_stored_blob(&self.payloads, &payload, payload_size).await;
        match local {
            StoredBlobStatus::Healthy { bytes } => {
                report.bytes_checked =
                    report.bytes_checked.checked_add(bytes).ok_or_else(|| {
                        RepositoryError::LimitExceeded(
                            "fsck repair byte count overflowed".to_owned(),
                        )
                    })?;
                if outboard_present {
                    self.fsck_repair_outboard(&payload, payload_size, dry_run, report)
                        .await;
                }
            }
            failed @ (StoredBlobStatus::Missing | StoredBlobStatus::Corrupt(_)) => {
                let Some(replica) = replica else {
                    report.findings.push(fsck_repair_payload_finding(
                        &failed,
                        payload,
                        payload_size,
                    ));
                    return Ok(());
                };

                // A replica is a repair source only after an independent
                // full read proves the exact digest and size expected by
                // this authoritative target record.
                match verify_stored_blob(replica.payloads(), &payload, payload_size).await {
                    StoredBlobStatus::Healthy { .. } => {
                        if dry_run {
                            report.actions.push(FsckRepairAction {
                                kind: FsckRepairActionKind::RepairPayloadFromReplica,
                                payload,
                                bytes: payload_size,
                                status: FsckRepairActionStatus::Planned,
                            });
                            report.findings.push(fsck_repair_payload_finding(
                                &failed,
                                payload,
                                payload_size,
                            ));
                        } else if let Err(error) = self
                            .payloads
                            .repair_from_replica(replica.payloads(), &payload)
                            .await
                        {
                            report.findings.push(FsckRepairFinding {
                                kind: FsckRepairFindingKind::UnavailablePayload,
                                payload,
                                bytes: payload_size,
                                message: format!(
                                    "verified replica could not be published locally: {error}"
                                ),
                            });
                        } else {
                            report.bytes_checked = report
                                .bytes_checked
                                .checked_add(payload_size)
                                .ok_or_else(|| {
                                RepositoryError::LimitExceeded(
                                    "fsck repair byte count overflowed".to_owned(),
                                )
                            })?;
                            report.actions.push(FsckRepairAction {
                                kind: FsckRepairActionKind::RepairPayloadFromReplica,
                                payload,
                                bytes: payload_size,
                                status: FsckRepairActionStatus::Repaired,
                            });
                            if outboard_present {
                                self.fsck_repair_outboard(&payload, payload_size, false, report)
                                    .await;
                            }
                        }
                    }
                    status => report.findings.push(FsckRepairFinding {
                        kind: fsck_repair_payload_finding_kind(&failed),
                        payload,
                        bytes: payload_size,
                        message: format!(
                            "local representation is damaged and replica is not verified: {}",
                            stored_blob_status_message(&status)
                        ),
                    }),
                }
            }
            unavailable @ StoredBlobStatus::Unavailable(_) => report.findings.push(
                fsck_repair_payload_finding(&unavailable, payload, payload_size),
            ),
        }
        Ok(())
    }

    pub(super) async fn fsck_repair_outboard(
        &self,
        payload: &BlobId,
        bytes: u64,
        dry_run: bool,
        report: &mut FsckRepairReport,
    ) {
        let stored = match self.payloads.get_outboard(payload).await {
            Ok(Some(outboard)) => Some(outboard),
            Ok(None) => return,
            // A paged outboard may fail before its logical bytes can be
            // assembled. Rebuild only after independently verifying payloads.
            Err(error) if is_integrity_error(&error) => None,
            Err(error) => {
                report.findings.push(FsckRepairFinding {
                    kind: FsckRepairFindingKind::UnavailableOutboard,
                    payload: *payload,
                    bytes,
                    message: error.to_string(),
                });
                return;
            }
        };
        report.outboards_checked += 1;
        let expected = match self.payloads.compute_outboard(payload).await {
            Ok(outboard) => outboard,
            Err(error) => {
                report.findings.push(FsckRepairFinding {
                    kind: if is_integrity_error(&error) {
                        FsckRepairFindingKind::CorruptPayload
                    } else {
                        FsckRepairFindingKind::UnavailableOutboard
                    },
                    payload: *payload,
                    bytes,
                    message: error.to_string(),
                });
                return;
            }
        };
        if stored.as_ref() == Some(&expected) {
            return;
        }
        if dry_run {
            report.actions.push(FsckRepairAction {
                kind: FsckRepairActionKind::RebuildOutboard,
                payload: *payload,
                bytes,
                status: FsckRepairActionStatus::Planned,
            });
            report.findings.push(FsckRepairFinding {
                kind: FsckRepairFindingKind::CorruptOutboard,
                payload: *payload,
                bytes,
                message: "stored Bao outboard does not match the verified payload".to_owned(),
            });
        } else if let Err(error) = self.payloads.put_outboard(payload, expected).await {
            report.findings.push(FsckRepairFinding {
                kind: FsckRepairFindingKind::UnavailableOutboard,
                payload: *payload,
                bytes,
                message: format!("could not publish rebuilt Bao outboard: {error}"),
            });
        } else {
            report.actions.push(FsckRepairAction {
                kind: FsckRepairActionKind::RebuildOutboard,
                payload: *payload,
                bytes,
                status: FsckRepairActionStatus::Repaired,
            });
        }
    }
}

pub(super) enum StoredBlobStatus {
    Healthy { bytes: u64 },
    Missing,
    Corrupt(String),
    Unavailable(String),
}

async fn verify_stored_blob(
    store: &crate::ChunkedBlobStore,
    expected: &BlobId,
    expected_size: u64,
) -> StoredBlobStatus {
    let mut reader = match store.open_read_strict(expected).await {
        Ok(Some(reader)) => reader,
        Ok(None) => return StoredBlobStatus::Missing,
        Err(error) if is_integrity_error(&error) => {
            return StoredBlobStatus::Corrupt(error.to_string());
        }
        Err(error) => return StoredBlobStatus::Unavailable(error.to_string()),
    };
    let mut hasher = blake3::Hasher::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        match AsyncReadExt::read(&mut reader, &mut buffer).await {
            Ok(0) => break,
            Ok(read) => {
                hasher.update(&buffer[..read]);
                bytes = match bytes.checked_add(read as u64) {
                    Some(bytes) => bytes,
                    None => return StoredBlobStatus::Corrupt("payload size overflowed".to_owned()),
                };
            }
            Err(error) if is_integrity_io_error(&error) => {
                return StoredBlobStatus::Corrupt(error.to_string());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return StoredBlobStatus::Missing;
            }
            Err(error) => return StoredBlobStatus::Unavailable(error.to_string()),
        }
    }
    if bytes != expected_size {
        return StoredBlobStatus::Corrupt(format!(
            "payload has {bytes} bytes, record requires {expected_size}"
        ));
    }
    let actual = BlobId::new(hasher.finalize().into());
    if actual != *expected {
        return StoredBlobStatus::Corrupt(format!(
            "payload hashes to {actual}, record requires {expected}"
        ));
    }
    StoredBlobStatus::Healthy { bytes }
}

fn fsck_repair_payload_finding(
    status: &StoredBlobStatus,
    payload: BlobId,
    bytes: u64,
) -> FsckRepairFinding {
    FsckRepairFinding {
        kind: fsck_repair_payload_finding_kind(status),
        payload,
        bytes,
        message: stored_blob_status_message(status),
    }
}

fn fsck_repair_payload_finding_kind(status: &StoredBlobStatus) -> FsckRepairFindingKind {
    match status {
        StoredBlobStatus::Missing => FsckRepairFindingKind::MissingPayload,
        StoredBlobStatus::Corrupt(_) => FsckRepairFindingKind::CorruptPayload,
        StoredBlobStatus::Unavailable(_) => FsckRepairFindingKind::UnavailablePayload,
        StoredBlobStatus::Healthy { .. } => {
            unreachable!("healthy payload has no fsck_repair finding")
        }
    }
}

fn stored_blob_status_message(status: &StoredBlobStatus) -> String {
    match status {
        StoredBlobStatus::Healthy { .. } => "payload is healthy".to_owned(),
        StoredBlobStatus::Missing => "payload is absent".to_owned(),
        StoredBlobStatus::Corrupt(message) | StoredBlobStatus::Unavailable(message) => {
            message.clone()
        }
    }
}

impl<PS, SS> Repository<PS, SS>
where
    PS: BlobGc + 'static,
    SS: MetadataStore + 'static,
{
    /// Verify logical state, payload identity, format relations, root closure,
    /// and physical coverage without reconstructing primary records from
    /// payload scans. The scan pins its snapshot while allowing collection of
    /// unrelated data. Returns `Busy` if collection prevents safe admission;
    /// retry after collection, or recover an interrupted collector first.
    #[tracing::instrument(name = "repository.fsck", skip_all)]
    pub async fn fsck(&self) -> Result<FsckReport, RepositoryError> {
        let (snapshot, _protection) = self.pinned_retained_snapshot(false, None).await?;
        let snapshot = snapshot.0;
        let revision = snapshot.revision();
        let mut issues = Vec::new();

        // Stream roots straight into the spillable queue. Retaining all root
        // records here would make fsck's memory depend on the number of names
        // rather than its traversal limits.
        let area = self.spill_area();
        let mut reachable = SpillSet::new(area.clone(), "fsck-reachable");
        let mut reported_missing = SpillSet::new(area.clone(), "fsck-missing");
        let mut queue = TraversalQueue::new(area.clone());
        let mut roots_checked = 0usize;
        let mut root_stream = snapshot.roots();
        while let Some(root) = root_stream.next().await {
            match root {
                Ok(root) => queue.push((None, root.target().clone())).await?,
                Err(error) => {
                    issues.push(FsckIssue {
                        disposition: FsckDisposition::Corrupt,
                        kind: FsckIssueKind::StateEncoding,
                        object: None,
                        message: error.to_string(),
                    });
                    break;
                }
            }
            roots_checked += 1;
            if roots_checked > self.limits.max_traversal_objects {
                return Err(RepositoryError::LimitExceeded(format!(
                    "fsck root enumeration exceeded {} roots",
                    self.limits.max_traversal_objects
                )));
            }
        }
        drop(root_stream);

        // Small and medium snapshots fit under the traversal's existing
        // in-memory object limit. Preloading their immutable records through
        // the sequential stream avoids a random SQLite lookup (and its page
        // reads) for every graph edge. If one more record crosses the bound,
        // discard the cache and retain the fully streaming path used for very
        // large repositories.
        let mut cached_records = HashMap::with_capacity(
            self.profile
                .spill_limits()
                .max_memory_objects
                .min(self.limits.max_traversal_objects),
        );
        let mut cache_complete = true;
        let mut cache_stream = snapshot.objects_unordered();
        while let Some(record) = cache_stream.next().await {
            let record = record?;
            if cached_records.len() >= self.profile.spill_limits().max_memory_objects {
                cache_complete = false;
                break;
            }
            cached_records.insert(record.key().clone(), record);
        }
        drop(cache_stream);
        let cached_records = cache_complete.then_some(cached_records);
        let cached_order = cached_records.as_ref().map(|records| {
            let mut keys = records.keys().cloned().collect::<Vec<_>>();
            keys.sort_unstable();
            keys
        });

        // Graph discovery itself does not open payloads, so it stays in
        // canonical breadth-first order. Payload verification below uses a
        // bounded physical-order window to avoid cycling the pack cache.
        loop {
            // Batched insertion may admit every key in the frontier. Keep its
            // worst-case overrun to the single key that proves the limit.
            let frontier_limit = CLOSURE_FRONTIER.min(
                self.limits
                    .max_traversal_objects
                    .saturating_sub(reachable.len())
                    .saturating_add(1),
            );
            let mut frontier = Vec::with_capacity(frontier_limit);
            while frontier.len() < frontier_limit {
                let Some(step) = queue.pop().await? else {
                    break;
                };
                frontier.push(step);
            }
            if frontier.is_empty() {
                break;
            }
            let keys = frontier
                .iter()
                .map(|(_, key)| key.clone())
                .collect::<Vec<_>>();
            let records = match &cached_records {
                Some(cached) => keys.iter().map(|key| cached.get(key).cloned()).collect(),
                None => snapshot.object_batch(&keys).await?,
            };
            let inserted = reachable.insert_batch(&keys).await?;
            for (((from, key), record), inserted) in frontier.into_iter().zip(records).zip(inserted)
            {
                if !inserted {
                    continue;
                }
                if reachable.len() > self.limits.max_traversal_objects {
                    return Err(RepositoryError::LimitExceeded(format!(
                        "fsck root traversal exceeded {} objects",
                        self.limits.max_traversal_objects
                    )));
                }
                let Some(record) = record else {
                    reported_missing.insert(key.clone()).await?;
                    issues.push(FsckIssue {
                        disposition: FsckDisposition::Corrupt,
                        kind: FsckIssueKind::MissingRecord,
                        object: Some(key.clone()),
                        message: match from {
                            Some(from) => {
                                format!("reachable object {from} links to missing {key}")
                            }
                            None => format!("named root targets missing object {key}"),
                        },
                    });
                    continue;
                };
                for target in record.links() {
                    queue.push((Some(key.clone()), target.clone())).await?;
                }
            }
        }

        // Capture physical manifests before validating payloads. The same
        // inventory selects both the bytes fsck reads and the chunks it marks,
        // so an ordinary-read accelerator cannot hide a present manifest.
        let mut physical_blobs = SpillSet::new(area.clone(), "fsck-physical-blobs");
        let insertion_batch = self
            .spill_limits()
            .max_memory_objects
            .clamp(1, FSCK_MEMBERSHIP_BATCH);
        let mut pending_blobs = Vec::with_capacity(insertion_batch);
        let mut blobs = self.payloads.list_blobs();
        while let Some(blob) = blobs.next().await {
            pending_blobs.push(blob?);
            if pending_blobs.len() == insertion_batch {
                physical_blobs.insert_batch(&pending_blobs).await?;
                pending_blobs.clear();
            }
        }
        drop(blobs);
        physical_blobs.insert_batch(&pending_blobs).await?;
        drop(pending_blobs);

        let mut record_payloads = SpillSet::new(area.clone(), "fsck-payloads");
        let mut pending_payloads = Vec::with_capacity(insertion_batch);
        let mut referenced_chunks = SpillSet::new(area.clone(), "fsck-chunks");
        let mut objects_checked = 0usize;
        let mut object_stream = snapshot.objects();
        let mut cached_at = 0usize;
        loop {
            let mut batch = Vec::with_capacity(FSCK_SCAN_WINDOW);
            let mut exhausted = false;
            while batch.len() < FSCK_SCAN_WINDOW {
                let next = match &cached_order {
                    Some(order) => match order.get(cached_at) {
                        Some(key) => {
                            cached_at += 1;
                            Some(Ok(cached_records
                                .as_ref()
                                .expect("cached order requires records")
                                .get(key)
                                .expect("cached order names an existing record")
                                .clone()))
                        }
                        None => None,
                    },
                    None => object_stream.next().await,
                };
                let Some(record) = next else {
                    exhausted = true;
                    break;
                };
                match record {
                    Ok(record) => {
                        let order = self.payloads.physical_scan_order(&record.payload()).await?;
                        batch.push((order, record));
                    }
                    Err(error) => {
                        issues.push(FsckIssue {
                            disposition: FsckDisposition::Corrupt,
                            kind: FsckIssueKind::StateEncoding,
                            object: None,
                            message: error.to_string(),
                        });
                        exhausted = true;
                        break;
                    }
                }
            }
            batch.sort_unstable_by_key(|(pack, record)| {
                (
                    pack.is_none(),
                    *pack,
                    record.payload(),
                    record.key().clone(),
                )
            });
            let mut manifests_present = Vec::with_capacity(batch.len());
            let mut rooted = Vec::with_capacity(batch.len());
            for records in batch.chunks(FSCK_MEMBERSHIP_BATCH) {
                let payloads: Vec<_> = records.iter().map(|(_, record)| record.payload()).collect();
                let keys: Vec<_> = records.iter().map(|(_, record)| record.key()).collect();
                manifests_present.extend(physical_blobs.contains_batch(&payloads).await?);
                rooted.extend(reachable.contains_batch(&keys).await?);
            }
            for (((_, record), manifest_present), rooted) in
                batch.into_iter().zip(manifests_present).zip(rooted)
            {
                objects_checked += 1;
                if objects_checked > self.limits.max_traversal_objects {
                    return Err(RepositoryError::LimitExceeded(format!(
                        "fsck object enumeration exceeded {} records",
                        self.limits.max_traversal_objects
                    )));
                }
                let record = &record;
                let key = record.key();
                let disposition = if rooted {
                    FsckDisposition::Corrupt
                } else {
                    FsckDisposition::Collectible
                };
                if !rooted {
                    issues.push(FsckIssue {
                        disposition: FsckDisposition::Collectible,
                        kind: FsckIssueKind::UnrootedObject,
                        object: Some(key.clone()),
                        message: format!("object {key} is unreachable from every named root"),
                    });
                }
                pending_payloads.push(record.payload());
                if pending_payloads.len() == insertion_batch {
                    record_payloads.insert_batch(&pending_payloads).await?;
                    pending_payloads.clear();
                }
                let mut missing_links = Vec::new();
                // The rooted traversal above already proved every direct
                // target exists. Only collectible records outside that graph
                // need a separate link-existence check.
                if !rooted {
                    let targets = record.links();
                    let linked = match &cached_records {
                        Some(cached) => targets
                            .iter()
                            .map(|target| cached.get(target).cloned())
                            .collect(),
                        None => snapshot.object_batch(targets).await?,
                    };
                    for (target, found) in targets.iter().zip(linked) {
                        if found.is_none() {
                            missing_links.push(target.clone());
                        }
                    }
                }
                for missing in &missing_links {
                    if reported_missing.insert(missing.clone()).await? {
                        issues.push(FsckIssue {
                            disposition,
                            kind: FsckIssueKind::MissingRecord,
                            object: Some(missing.clone()),
                            message: format!("object {key} links to missing {missing}"),
                        });
                    }
                }

                let Some(format) = self.formats.get(key.namespace()) else {
                    issues.push(FsckIssue {
                        disposition: FsckDisposition::Unchecked,
                        kind: FsckIssueKind::UnsupportedNamespace,
                        object: Some(key.clone()),
                        message: format!(
                            "no verifier is registered for namespace `{}`",
                            key.namespace()
                        ),
                    });
                    continue;
                };
                let Some(mut payload) = self
                    .payloads
                    .open_read_for_fsck(&record.payload(), manifest_present)
                    .await?
                else {
                    issues.push(FsckIssue {
                        disposition,
                        kind: FsckIssueKind::MissingPayload,
                        object: Some(key.clone()),
                        message: format!("record payload {} is absent", record.payload()),
                    });
                    continue;
                };
                let mut payload = BlobPayloadReader::new(payload.as_mut(), record.payload_size());
                let verification = if missing_links.is_empty() {
                    let allowed: BTreeSet<_> = record.links().iter().cloned().collect();
                    let view = RepositoryDirectLinkView {
                        payloads: &self.payloads,
                        snapshot: snapshot.as_ref(),
                        overlay: &BTreeMap::new(),
                        cached: cached_records.as_ref(),
                        allowed: &allowed,
                    };
                    format
                        .verify_links(
                            VerificationContext::new(key, &mut payload),
                            record,
                            &view,
                            &self.limits,
                        )
                        .await
                } else {
                    format
                        .verify(VerificationContext::new(key, &mut payload), &self.limits)
                        .await
                        .and_then(|verified| {
                            if verified.record() == record {
                                Ok(())
                            } else {
                                Err(FormatError::RecordMismatch(key.clone()))
                            }
                        })
                };
                if let Err(error) = verification {
                    issues.push(FsckIssue {
                        disposition,
                        kind: FsckIssueKind::InvalidObject,
                        object: Some(key.clone()),
                        message: error.to_string(),
                    });
                    continue;
                }

                let chunks = self
                    .payloads
                    .chunks_for_gc(&record.payload(), manifest_present)
                    .await?
                    .ok_or_else(|| {
                        MetadataError::Corruption(format!(
                            "verified payload {} disappeared during fsck",
                            record.payload()
                        ))
                    })?;
                for chunks in chunks.chunks(insertion_batch) {
                    let keys: Vec<_> = chunks.iter().map(|chunk| chunk.digest).collect();
                    referenced_chunks.insert_batch(&keys).await?;
                }
            }
            if exhausted {
                break;
            }
        }
        drop(object_stream);
        record_payloads.insert_batch(&pending_payloads).await?;
        drop(pending_payloads);

        // The physical inventories are sorted through the same spillable sets,
        // so issue order stays canonical without holding either listing whole.
        let payloads_checked = record_payloads.len();
        let mut listed = std::pin::pin!(physical_blobs.into_difference(record_payloads));
        while let Some(payload) = listed.next().await {
            let payload = payload?;
            issues.push(FsckIssue {
                disposition: FsckDisposition::Collectible,
                kind: FsckIssueKind::UnreferencedPayload,
                object: None,
                message: format!("physical payload {payload} has no logical record"),
            });
        }

        let mut physical_chunks = SpillSet::new(area.clone(), "fsck-physical-chunks");
        let mut pending_chunks = Vec::with_capacity(insertion_batch);
        let mut chunks = self.payloads.list_chunks();
        while let Some(chunk) = chunks.next().await {
            pending_chunks.push(chunk?);
            if pending_chunks.len() == insertion_batch {
                physical_chunks.insert_batch(&pending_chunks).await?;
                pending_chunks.clear();
            }
        }
        drop(chunks);
        physical_chunks.insert_batch(&pending_chunks).await?;
        let mut listed = std::pin::pin!(physical_chunks.into_difference(referenced_chunks));
        while let Some(chunk) = listed.next().await {
            let chunk = chunk?;
            issues.push(FsckIssue {
                disposition: FsckDisposition::Collectible,
                kind: FsckIssueKind::UnreferencedChunk,
                object: None,
                message: format!("physical chunk {chunk} has no logical payload"),
            });
        }
        let report = FsckReport {
            revision,
            objects_checked,
            roots_checked,
            payloads_checked,
            spill: area.metrics(),
            issues,
        };
        if report.issues.iter().any(|issue| {
            matches!(
                issue.kind,
                FsckIssueKind::StateEncoding
                    | FsckIssueKind::MissingRecord
                    | FsckIssueKind::MissingPayload
                    | FsckIssueKind::InvalidObject
            )
        }) && let Some(store) = &self.nar_store
        {
            // The report is the audit's outcome; a failed association update
            // is logged and the store fails closed in this process.
            store.record_read_failure().await;
        }
        tracing::info!(
            revision = %report.revision,
            objects_checked = report.objects_checked,
            roots_checked = report.roots_checked,
            payloads_checked = report.payloads_checked,
            findings = report.issues.len(),
            spill_files = report.spill.files_opened,
            spill_peak_bytes = report.spill.peak_bytes,
            "integrity scan completed"
        );
        Ok(report)
    }
}

/// Records reordered together for sequential immutable-pack integrity reads.
///
/// The tuple stored for each record is small, and this bound remains constant
/// even when the repository is much larger than memory.
const FSCK_SCAN_WINDOW: usize = 65_536;
// Complete read-only payload checks, not repository-sized reader buffering.
const FSCK_READ_ONLY_CONCURRENCY: usize = 8;

/// Bound temporary key buffers used to check each physical scan window.
const FSCK_MEMBERSHIP_BATCH: usize = 1024;
