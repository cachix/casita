//! Repository admission serialized by a separate WAL3 state log.
//!
//! Each newly acquired root is a non-expiring collector token covering mark,
//! prune, sweep and catalog publication. Readers and writers use online pins. Admission and release are exact-revision CAS mutations. The
//! separate log keeps operational holds out of user object identity/revisions.

use super::*;
use crate::invariant::{self, Violation};
use crate::metadata::RepositoryLease;

const MAX_HOLDS: usize = 4096;
const MAX_WRITER_BYTES: usize = 120;
const ANCHOR: &[u8] = b"casita WAL3 repository admission v1";

/// One durable repository hold, for diagnosis and offline crash recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Wal3RepositoryHold {
    /// Exact, unique token to supply to offline recovery.
    pub token: RootName,
    /// Diagnostic runner name; never used to establish ownership.
    pub writer: String,
    /// True for collector tokens. False identifies a legacy shared token,
    /// retained for diagnosis and exact-token recovery after an upgrade.
    pub exclusive: bool,
}

pub(super) struct Coordination {
    store: Wal3MetadataStore,
    anchor: crate::VerifiedObject,
    parent_commits: Arc<tokio::sync::Mutex<()>>,
    last_blocker_report: std::sync::Mutex<Option<Instant>>,
    #[cfg(test)]
    admission_pause: std::sync::Mutex<Option<Arc<CheckpointPause>>>,
}

impl Coordination {
    pub(super) async fn open(parent: &Wal3MetadataStore) -> Result<Arc<Self>, MetadataError> {
        if parent.writer_name.len() > MAX_WRITER_BYTES {
            return Err(MetadataError::Backend(
                "WAL3 runner name exceeds 120 bytes".to_owned(),
            ));
        }
        let mut store = Wal3MetadataStore::open(
            parent.storage.clone(),
            format!("{}/repository-holds-v1", parent.prefix),
            parent.writer_name.clone(),
        )
        .await?;
        store.is_coordination = true;
        let key = ObjectKey::blob(crate::BlobId::new(crate::Digest::hash(ANCHOR)));
        let mut reader = std::io::Cursor::new(ANCHOR);
        let anchor = crate::FormatRegistry::builtin()
            .verify(&key, &mut reader, &crate::FormatLimits::default())
            .await
            .map_err(|error| MetadataError::Backend(error.to_string()))?;
        Ok(Arc::new(Self {
            store,
            anchor,
            parent_commits: parent.commit_lock.clone(),
            last_blocker_report: std::sync::Mutex::new(None),
            #[cfg(test)]
            admission_pause: std::sync::Mutex::new(None),
        }))
    }

    async fn holds_at(
        &self,
        snapshot: &dyn MetadataSnapshot,
    ) -> Result<Vec<Wal3RepositoryHold>, MetadataError> {
        let mut roots = snapshot.roots();
        let mut holds = Vec::new();
        while let Some(root) = roots.try_next().await? {
            if holds.len() == MAX_HOLDS || root.target() != self.anchor.record().key() {
                return Err(MetadataError::Corruption(
                    "invalid repository hold catalog".to_owned(),
                ));
            }
            holds.push(decode_hold(root.name())?);
        }
        if holds.iter().any(|hold| hold.exclusive) && holds.len() != 1 {
            return Err(MetadataError::Corruption(
                "exclusive repository hold overlaps another hold".to_owned(),
            ));
        }
        Ok(holds)
    }

    pub(super) async fn holds(&self) -> Result<Vec<Wal3RepositoryHold>, MetadataError> {
        let (snapshot, _pin) = self.pinned_snapshot().await?;
        self.holds_at(snapshot.as_ref()).await
    }

    // Operational roots are lazy too. Collector admission cannot protect a
    // diagnostic read, or the read used to acquire that admission itself.
    pub(super) async fn pinned_snapshot(
        &self,
    ) -> Result<
        (
            Arc<dyn MetadataSnapshot>,
            Option<super::super::DataPinLease>,
        ),
        MetadataError,
    > {
        super::super::read_snapshot(&self.store).await
    }

    pub(super) async fn acquire(
        self: &Arc<Self>,
    ) -> Result<Option<RepositoryLease>, MetadataError> {
        let this = self.clone();
        // If the caller disappears, drop its lease before completing this
        // tracked task. Draining must observe the resulting release as well.
        crate::metadata::run_lease_task("repository admission", |send| async move {
            drop(send.send(this.acquire_inner().await));
            Ok(())
        })
        .await?
    }

    async fn acquire_inner(self: Arc<Self>) -> Result<Option<RepositoryLease>, MetadataError> {
        let mut entropy = [0; 32];
        getrandom::fill(&mut entropy)
            .map_err(|error| MetadataError::RevisionEntropy(error.to_string()))?;
        let token = RootName::try_from(format!(
            "exclusive/{}/w{}",
            data_encoding::HEXLOWER.encode(&entropy),
            data_encoding::HEXLOWER.encode(self.store.writer_name.as_bytes()),
        ))
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
        let mut lease = None;
        for attempt in 0..32 {
            let (snapshot, _pin) = self.pinned_snapshot().await?;
            let holds = self.holds_at(snapshot.as_ref()).await?;
            if holds.iter().any(|hold| hold.token == token) {
                return Ok(lease);
            }
            if !holds.is_empty() {
                self.report_blockers(&holds);
                return Ok(None);
            }
            if lease.is_none() {
                let cleanup = self.clone();
                let token = token.clone();
                lease = Some(RepositoryLease::new(move || async move {
                    // A cancelled caller may have a shielded state commit still
                    // completing. Keep its repository ownership until it ends.
                    let _commits = cleanup.parent_commits.lock().await;
                    cleanup.release(&token).await.map(|_| ())
                }));
            }
            let mut mutation = MetadataMutation::new();
            mutation.add_object(self.anchor.clone());
            mutation.set_root(token.clone(), self.anchor.record().key().clone());
            invariant::check(|| {
                validate_hold_admission(&holds, &token, self.anchor.record().key(), &mutation)
            })?;
            match self.store.commit(&snapshot.revision(), mutation).await {
                Ok(_) => {
                    tracing::info!(token = %token, writer = ?self.store.writer_name,
                        process_id = std::process::id(),
                        prefix = %self.store.prefix, "repository hold acquired");
                    #[cfg(test)]
                    {
                        let pause = self.admission_pause.lock().unwrap().take();
                        if let Some(pause) = pause {
                            pause.reached.notify_one();
                            pause.resume.notified().await;
                        }
                    }
                    return Ok(lease);
                }
                Err(
                    MetadataError::StaleRevision { .. }
                    | MetadataError::MaintenanceFenced
                    | MetadataError::Transient(_),
                ) => {
                    tokio::time::sleep(Duration::from_millis(1 << attempt.min(6))).await;
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        Err(MetadataError::Transient(
            "repository admission remained contended".to_owned(),
        ))
    }

    pub(super) async fn release(&self, token: &RootName) -> Result<bool, MetadataError> {
        decode_hold(token)?;
        for attempt in 0..32 {
            let (snapshot, _pin) = self.pinned_snapshot().await?;
            let present = snapshot.root(token).await?.is_some();
            let mut mutation = MetadataMutation::new();
            mutation.remove_root(token.clone());
            invariant::check(|| validate_hold_release(token, &mutation))?;
            // Even an absent token requires a CAS. This fences an admission
            // whose append outcome was ambiguous at the preceding revision.
            match self.store.commit(&snapshot.revision(), mutation).await {
                Ok(_) => {
                    tracing::info!(token = %token, present, prefix = %self.store.prefix, "repository hold released");
                    return Ok(present);
                }
                Err(
                    MetadataError::StaleRevision { .. }
                    | MetadataError::MaintenanceFenced
                    | MetadataError::Transient(_),
                ) => {
                    tokio::time::sleep(Duration::from_millis(1 << attempt.min(6))).await;
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        Err(MetadataError::Transient(
            "repository hold release remained contended".to_owned(),
        ))
    }

    #[cfg(test)]
    pub(super) fn metadata_store(&self) -> &Wal3MetadataStore {
        &self.store
    }

    #[cfg(test)]
    pub(super) fn pause_after_admission(&self, pause: Arc<CheckpointPause>) {
        *self.admission_pause.lock().unwrap() = Some(pause);
    }

    fn report_blockers(&self, holds: &[Wal3RepositoryHold]) {
        let mut last = self.last_blocker_report.lock().unwrap();
        if last.is_some_and(|at| at.elapsed() < Duration::from_secs(5)) {
            return;
        }
        *last = Some(Instant::now());
        tracing::warn!(prefix = %self.store.prefix,
            hold_count = holds.len(), "repository admission blocked; inspect with `casita holds s3://BUCKET/PREFIX`");
        for hold in holds.iter().take(10) {
            tracing::warn!(token = %hold.token, writer = ?hold.writer, exclusive = hold.exclusive,
                "blocking repository hold");
        }
    }
}

fn decode_hold(token: &RootName) -> Result<Wal3RepositoryHold, MetadataError> {
    let invalid = || MetadataError::Corruption("invalid WAL3 repository hold token".to_owned());
    let parts = token.as_str().split('/').collect::<Vec<_>>();
    if parts.len() != 3
        || parts[1].len() != 64
        || parts[2].len() > 1 + 2 * MAX_WRITER_BYTES
        || !parts[2].starts_with('w')
    {
        return Err(invalid());
    }
    let exclusive = match parts[0] {
        "shared" => false,
        "exclusive" => true,
        _ => return Err(invalid()),
    };
    data_encoding::HEXLOWER
        .decode(parts[1].as_bytes())
        .map_err(|_| invalid())?;
    let writer = String::from_utf8(
        data_encoding::HEXLOWER
            .decode(&parts[2].as_bytes()[1..])
            .map_err(|_| invalid())?,
    )
    .map_err(|_| invalid())?;
    Ok(Wal3RepositoryHold {
        token: token.clone(),
        writer,
        exclusive,
    })
}

/// Admission records the caller's exclusive token at most once, alone, as its
/// only root change. A duplicate token or an overlapping exclusive hold is
/// state `Coordination::holds_at` rejects as corruption.
fn validate_hold_admission(
    holds: &[Wal3RepositoryHold],
    token: &RootName,
    anchor: &ObjectKey,
    mutation: &MetadataMutation,
) -> Result<(), Violation> {
    const POINT: &str = "wal3.holds";
    invariant::ensure(POINT, holds.iter().all(|hold| hold.token != *token), || {
        format!("hold {token} is already recorded")
    })?;
    invariant::ensure(
        POINT,
        decode_hold(token).is_ok_and(|hold| hold.exclusive),
        || format!("admission records {token}, which is not an exclusive hold"),
    )?;
    invariant::ensure(POINT, holds.is_empty(), || {
        format!(
            "exclusive hold {token} overlaps {} recorded holds",
            holds.len()
        )
    })?;
    let recorded = RootChange::Set {
        name: token.clone(),
        target: anchor.clone(),
    };
    invariant::ensure(POINT, mutation.root_changes().eq([&recorded]), || {
        format!(
            "admission of {token} makes root changes {:?}",
            mutation.root_changes().collect::<Vec<_>>()
        )
    })
}

/// Release removes the caller's token and no other hold.
fn validate_hold_release(token: &RootName, mutation: &MetadataMutation) -> Result<(), Violation> {
    let removed = RootChange::Remove {
        name: token.clone(),
    };
    invariant::ensure("wal3.holds", mutation.root_changes().eq([&removed]), || {
        format!(
            "release of {token} makes root changes {:?}",
            mutation.root_changes().collect::<Vec<_>>()
        )
    })
}

impl Wal3MetadataStore {
    pub(super) async fn repository_coordination(
        &self,
    ) -> Result<&Arc<Coordination>, MetadataError> {
        self.coordination
            .get_or_try_init(|| Coordination::open(self))
            .await
    }

    /// List collector ownership and legacy shared tokens for recovery.
    pub async fn repository_holds(&self) -> Result<Vec<Wal3RepositoryHold>, MetadataError> {
        self.repository_coordination().await?.holds().await
    }

    /// Access the operational log's pin ledger for inspection and exact-token
    /// recovery. Obtaining this handle acquires no collector ownership.
    pub async fn repository_coordination_pin_store(
        &self,
    ) -> Result<Arc<dyn super::super::PinStore>, MetadataError> {
        self.repository_coordination()
            .await?
            .store
            .pin_store()
            .await
    }

    /// Retry the exact abandoned shard claims in the coordination log.
    /// The stopped-owner and settled-request requirements of
    /// [`Self::recover_wal_deletions`] apply to these claims too.
    pub async fn recover_repository_coordination_deletions(
        &self,
        claims: BTreeSet<super::super::PinToken>,
    ) -> Result<(), MetadataError> {
        if claims.is_empty() {
            return Ok(());
        }
        let hold = self.try_collection_lease().await?.ok_or_else(|| {
            MetadataError::Transient("another collector owns repository admission".into())
        })?;
        self.repository_coordination()
            .await?
            .store
            .recover_wal_deletions_owned(claims, hold)
            .await
    }

    /// Remove an exact abandoned hold during offline recovery.
    ///
    /// The owning runner MUST have been terminated and prevented from resuming.
    /// Stop every repository runner before recovering an exclusive collector
    /// hold, then verify/recover the payload catalog before resuming service.
    /// Elapsed time or a missing heartbeat never proves abandonment. Removing a
    /// live hold violates the repository's collection safety contract.
    pub async fn release_abandoned_repository_hold(
        &self,
        token: &RootName,
    ) -> Result<bool, MetadataError> {
        self.repository_coordination().await?.release(token).await
    }

    /// Compact and collect the operational hold log under collector admission.
    /// Online metadata pins protect concurrent admission and diagnostic reads.
    /// This preserves all currently recorded tokens and does not expire holds.
    pub async fn collect_repository_coordination(
        &self,
        reader_grace_period: Duration,
    ) -> Result<(), MetadataError> {
        self.run_wal_collection(reader_grace_period, true).await
    }

    pub(super) async fn run_wal_collection(
        &self,
        reader_grace_period: Duration,
        coordination_log: bool,
    ) -> Result<(), MetadataError> {
        let hold = loop {
            if let Some(hold) = self.try_collection_lease().await? {
                break hold;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        // Both logs use the parent repository's collector admission, acquired
        // before opening the target log and moved into the tracked task.
        let (store, task) = if coordination_log {
            (
                self.repository_coordination().await?.store.clone(),
                "repository coordination collection",
            )
        } else {
            (self.clone(), "WAL collection")
        };
        super::super::run_lease_task(task, |send| async move {
            let result = store.collect_wal_inner(reader_grace_period, hold).await;
            drop(send.send(result));
            Ok(())
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(kind: &str, entropy: u8) -> RootName {
        RootName::try_from(format!(
            "{kind}/{}/w{}",
            format!("{entropy:02x}").repeat(32),
            data_encoding::HEXLOWER.encode(b"runner"),
        ))
        .unwrap()
    }

    fn anchor() -> ObjectKey {
        ObjectKey::blob(crate::BlobId::new(crate::Digest::hash(ANCHOR)))
    }

    fn admit(name: &RootName, target: &ObjectKey) -> MetadataMutation {
        let mut mutation = MetadataMutation::new();
        mutation.set_root(name.clone(), target.clone());
        mutation
    }

    fn release(name: &RootName) -> MetadataMutation {
        let mut mutation = MetadataMutation::new();
        mutation.remove_root(name.clone());
        mutation
    }

    #[test]
    fn admission_records_one_exclusive_token_alone() {
        let anchor = anchor();
        let mine = token("exclusive", 0x11);
        let other = token("exclusive", 0x22);
        let shared = token("shared", 0x33);
        let hold = |name: &RootName| decode_hold(name).unwrap();
        assert_eq!(
            validate_hold_admission(&[], &mine, &anchor, &admit(&mine, &anchor)),
            Ok(())
        );
        for recorded in [hold(&mine), hold(&other), hold(&shared)] {
            let holds = [recorded];
            assert!(
                validate_hold_admission(&holds, &mine, &anchor, &admit(&mine, &anchor)).is_err()
            );
        }
        assert!(validate_hold_admission(&[], &shared, &anchor, &admit(&shared, &anchor)).is_err());
        let mut extra = admit(&mine, &anchor);
        extra.remove_root(other.clone());
        let elsewhere = ObjectKey::blob(crate::BlobId::new(crate::Digest::hash(b"elsewhere")));
        for mutation in [
            MetadataMutation::new(),
            admit(&other, &anchor),
            admit(&mine, &elsewhere),
            extra,
        ] {
            assert!(validate_hold_admission(&[], &mine, &anchor, &mutation).is_err());
        }
    }

    #[test]
    fn release_removes_only_the_callers_token() {
        let mine = token("exclusive", 0x11);
        let other = token("exclusive", 0x22);
        assert_eq!(validate_hold_release(&mine, &release(&mine)), Ok(()));
        let mut both = release(&mine);
        both.remove_root(other.clone());
        for mutation in [
            MetadataMutation::new(),
            release(&other),
            both,
            admit(&mine, &anchor()),
        ] {
            assert!(validate_hold_release(&mine, &mutation).is_err());
        }
    }
}
