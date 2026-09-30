//! Atomic arbitration between data pins, logical pruning and physical deletion.
//!
//! Pin registration and deletion claims share one revision. A collector must
//! expand logical scopes against the relevant snapshots before claiming a
//! deletion; this ledger does not perform graph traversal. Physical resources
//! must be protected before upload or reuse, not merely before publication.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;

use super::MetadataError;
use crate::invariant::{self, Violation};
use crate::{BlobId, ChunkId, ObjectKey};

mod codec;
mod persistent;
#[cfg(feature = "s3")]
pub(crate) use persistent::chroma_pin_store;
pub use persistent::{FilePinStore, ObjectPinStore};
mod runtime;
pub(crate) use runtime::PruningPinStore;
pub use runtime::{BackendWriteScope, DataPinLease};
pub(crate) use runtime::{CollectorLease, PinBindings, WritePins, flush_pin_releases};

/// Logical data a live operation may access.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum PinScope {
    /// Objects visible through a retained metadata generation.
    Snapshot { generation: u64 },
    /// Selected immutable objects and their reachable closures.
    Closures(BTreeSet<ObjectKey>),
    /// Unpublished physical data owned by a writer or maintenance operation.
    Staging,
    /// Metadata files used internally while loading or committing logical state.
    /// This scope cannot retain payloads, catalogs, or logical objects.
    Metadata,
}

/// A physical identity protected before reuse or upload.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum PinResource {
    Blob(BlobId),
    Chunk(ChunkId),
    /// Backend-relative physical path, including packs and catalog objects.
    StorageObject(String),
    /// A committed logical object and its closure used by a staging operation.
    Object(ObjectKey),
    /// An additional immutable catalog read during a long-running operation.
    Catalog(Vec<u8>),
    /// Immutable metadata-backend path used by a lazy logical snapshot.
    /// Distinct from payload paths even when the backends share a namespace.
    MetadataObject(String),
}

/// Unique operation identity. Names and elapsed time never establish ownership.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct PinToken([u8; 32]);

impl PinToken {
    fn fresh() -> Result<Self, MetadataError> {
        let mut bytes = [0; 32];
        getrandom::fill(&mut bytes)
            .map_err(|error| MetadataError::RevisionEntropy(error.to_string()))?;
        Ok(Self(bytes))
    }
}

impl std::fmt::Display for PinToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&data_encoding::HEXLOWER.encode(&self.0))
    }
}

impl std::str::FromStr for PinToken {
    type Err = MetadataError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || MetadataError::Corruption("invalid pin token".into());
        if value.len() != 64 {
            return Err(invalid());
        }
        let bytes = data_encoding::HEXLOWER
            .decode(value.as_bytes())
            .map_err(|_| invalid())?;
        Ok(Self(bytes.try_into().map_err(|_| invalid())?))
    }
}

/// One pin's logical scope and protected physical identities.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct DataPin {
    pub scope: PinScope,
    /// The immutable physical catalog used by this operation, if any.
    pub catalog: Option<Vec<u8>>,
    pub resources: BTreeSet<PinResource>,
}

impl DataPin {
    fn affects_payload_liveness(&self) -> bool {
        self.scope != PinScope::Metadata
            && !(self.scope == PinScope::Staging
                && self.catalog.is_none()
                && self.resources.is_empty())
    }

    fn validate(&self) -> Result<(), MetadataError> {
        if self.scope == PinScope::Metadata
            && (self.catalog.is_some() || !metadata_resources(&self.resources))
        {
            return Err(MetadataError::Corruption(
                "metadata pin contains payload or logical resources".into(),
            ));
        }
        Ok(())
    }
}

fn metadata_resources(resources: &BTreeSet<PinResource>) -> bool {
    resources
        .iter()
        .all(|resource| matches!(resource, PinResource::MetadataObject(_)))
}

/// Inventory used for marking. Claims must validate its exact revision.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PinInventory {
    pub revision: u64,
    /// Local process-reader owners. Persistent protocol bookkeeping, not data pins.
    #[doc(hidden)]
    pub reader_owners: BTreeSet<PinToken>,
    /// Durable upper bound on revisions published without a reader fsync.
    #[doc(hidden)]
    pub reader_revision_ceiling: u64,
    pub pins: BTreeMap<PinToken, DataPin>,
    pub deletions: BTreeMap<PinToken, BTreeSet<PinResource>>,
    pub logical_prune: Option<PinToken>,
    /// One collector owns a pass; readers and writers may still register pins.
    pub collector: Option<PinToken>,
    /// Released pins retained as liveness history until the collector finishes.
    /// Every token here remains in `pins` and cannot be used for new writes.
    pub retired: BTreeSet<PinToken>,
}

/// Atomic operations required of an online pin ledger.
///
/// This is protocol infrastructure. Constructing a ledger alone does not
/// enable online collection; admission, marking and storage I/O must share it.
///
/// A pin alone is insufficient to enable online collection: the collector must
/// mark its logical scope and catalog, and all physical writes and deletions
/// must participate. Releasing a token requires that its operation has settled;
/// cancellation or elapsed time is not evidence that an in-flight deletion has
/// stopped. Durable implementations must retain ambiguous claims for recovery.
#[async_trait]
pub trait PinStore: Send + Sync {
    async fn inventory(&self) -> Result<PinInventory, MetadataError>;
    /// Whether this deletion handle owns any active logical fence in the
    /// supplied inventory. Ordinary clients cannot delete through that fence.
    fn allows_deletion(&self, inventory: &PinInventory) -> bool {
        inventory.logical_prune.is_none()
    }
    /// Acquire collector-only ownership at this exact ledger revision. A
    /// previous token permits recovery only after proving its owner stopped.
    async fn begin_collection(
        &self,
        _revision: u64,
        _previous: Option<PinToken>,
    ) -> Result<Option<PinToken>, MetadataError> {
        Err(MetadataError::Backend(
            "pin ledger does not support collector ownership".into(),
        ))
    }
    /// Acquire ownership before marking, atomically checking the previous
    /// collector rather than an incidental pin revision. Recovery still requires
    /// external proof that the previous owner has stopped all I/O.
    /// Custom backends default to exact-revision retries.
    async fn acquire_collection(
        &self,
        previous: Option<PinToken>,
    ) -> Result<Option<PinToken>, MetadataError> {
        for _ in 0..32 {
            let inventory = self.inventory().await?;
            if inventory.collector != previous {
                return Ok(None);
            }
            if let Some(token) = self
                .begin_collection(inventory.revision, previous.clone())
                .await?
            {
                return Ok(Some(token));
            }
        }
        Ok(None)
    }
    /// Finish an exact collector token after every prune and deletion settles.
    /// Live pins remain; only released history is discarded.
    async fn finish_collection(&self, _token: &PinToken) -> Result<(), MetadataError> {
        Err(MetadataError::Backend(
            "pin ledger does not support collector ownership".into(),
        ))
    }

    /// Register a candidate scope/catalog before exposing the read. The caller
    /// must then validate the candidate's metadata revision, releasing and
    /// retrying if it changed. The ledger does not atomically bind parent
    /// metadata. `None` means a prune or conflicting physical deletion is active;
    /// disjoint deletion claims permit admission. Metadata-only scopes may also
    /// enter during logical pruning because they cannot retain payload data.
    async fn register(&self, pin: DataPin) -> Result<Option<PinToken>, MetadataError>;
    /// Register read-only protection. Local stores may bind its lifetime to a
    /// kernel owner lease; other stores retain the durable registration protocol.
    /// Such a pin must never own writes, publication, or deletion recovery.
    #[doc(hidden)]
    async fn register_reader(&self, pin: DataPin) -> Result<Option<PinToken>, MetadataError> {
        self.register(pin).await
    }

    /// Add write intents before uploading or reusing the corresponding bytes.
    async fn protect(
        &self,
        token: &PinToken,
        resources: BTreeSet<PinResource>,
    ) -> Result<bool, MetadataError>;
    async fn release(&self, token: &PinToken) -> Result<(), MetadataError>;
    /// Fence registration briefly while a marked logical mutation commits.
    async fn begin_prune(&self, revision: u64) -> Result<Option<PinToken>, MetadataError>;
    /// Resume logical pruning while retaining exactly these deletion claims.
    /// The caller must own the collector fence and establish that the previous
    /// owner can issue no further I/O. Tokens alone do not prove that fact.
    async fn begin_prune_recovering(
        &self,
        revision: u64,
        claims: BTreeSet<PinToken>,
    ) -> Result<Option<PinToken>, MetadataError> {
        if claims.is_empty() {
            self.begin_prune(revision).await
        } else {
            Err(MetadataError::Backend(
                "pin ledger does not support deletion-claim recovery".into(),
            ))
        }
    }
    /// Acquire the logical fence and return the exact inventory it protects.
    /// The caller must own this collector and validate the returned logical
    /// protections before committing against the marked metadata revision.
    /// This does not authorize physical deletion before that commit.
    ///
    /// Backends should combine capture and admission in one ledger transaction.
    /// The default retains compatibility using exact-revision admission retries.
    async fn begin_prune_validating(
        &self,
        collector: &PinToken,
        claims: BTreeSet<PinToken>,
    ) -> Result<Option<(PinToken, PinInventory)>, MetadataError> {
        for _ in 0..32 {
            let inventory = self.inventory().await?;
            if inventory.collector.as_ref() != Some(collector)
                || inventory.logical_prune.is_some()
                || inventory.deletions.keys().cloned().collect::<BTreeSet<_>>() != claims
            {
                return Ok(None);
            }
            if let Some(token) = self
                .begin_prune_recovering(inventory.revision, claims.clone())
                .await?
            {
                return Ok(Some((token, inventory)));
            }
        }
        Err(MetadataError::Transient(
            "pin ledger remained contended during prune admission".into(),
        ))
    }
    async fn finish_prune(&self, token: &PinToken) -> Result<(), MetadataError>;
    /// Claim an unpinned batch against the inventory used for marking. The
    /// caller must already have excluded all expanded logical/catalog liveness.
    async fn claim_deletions(
        &self,
        revision: u64,
        resources: BTreeSet<PinResource>,
    ) -> Result<Option<PinToken>, MetadataError>;
    /// Claim blob/chunk garbage after logical pruning. The caller must have
    /// excluded expanded logical/catalog liveness and checked the descendants
    /// of every listed blob against this exact candidate batch.
    ///
    /// Admission checks current direct protection and, for chunk claims,
    /// rejects any pinned blob absent from checked_blobs. Pin tokens, duplicate
    /// owners and unrelated direct resources need not match an older revision.
    /// Collector ownership, overlapping claims and logical fences still apply.
    /// This API never authorizes emergency deletion before logical pruning.
    async fn claim_deletions_validated(
        &self,
        collector: &PinToken,
        resources: BTreeSet<PinResource>,
        checked_blobs: BTreeSet<BlobId>,
    ) -> Result<Option<PinToken>, MetadataError> {
        let inventory = self.inventory().await?;
        if !inventory.can_claim_validated(collector, &resources, &checked_blobs)? {
            return Ok(None);
        }
        self.claim_deletions(inventory.revision, resources).await
    }
    /// Claim physical garbage while this collector holds the logical prune
    /// fence for emergency reclamation. Both tokens and the marked revision
    /// must still match. Owning the fence does not override existing data pins
    /// or other deletion claims, and it must remain held through the retry of
    /// the metadata commit. Callers must keep this ownership through all I/O.
    async fn claim_deletions_during_prune(
        &self,
        _revision: u64,
        _resources: BTreeSet<PinResource>,
        _collector: &PinToken,
        _prune: &PinToken,
    ) -> Result<Option<PinToken>, MetadataError> {
        Err(MetadataError::Backend(
            "pin ledger does not support fenced emergency deletion".into(),
        ))
    }
    async fn finish_deletions(&self, token: &PinToken) -> Result<(), MetadataError>;
}

/// Shared pin ledger for ephemeral storage. Clones share the same inventory.
#[derive(Clone, Default)]
pub struct MemoryPinStore {
    state: Arc<tokio::sync::Mutex<PinInventory>>,
}

pub(crate) enum LogicalPinConflict {
    SnapshotGeneration,
    UnmarkedRoot,
}

impl PinInventory {
    fn can_claim_validated(
        &self,
        collector: &PinToken,
        resources: &BTreeSet<PinResource>,
        checked_blobs: &BTreeSet<BlobId>,
    ) -> Result<bool, MetadataError> {
        if resources
            .iter()
            .any(|resource| !matches!(resource, PinResource::Blob(_) | PinResource::Chunk(_)))
        {
            return Err(MetadataError::Backend(
                "validated claims require blob or chunk resources".into(),
            ));
        }
        let chunks = resources
            .iter()
            .any(|resource| matches!(resource, PinResource::Chunk(_)));
        Ok(!resources.is_empty()
            && self.collector.as_ref() == Some(collector)
            && self.logical_prune.is_none()
            && !self.deleting(resources)
            && self.pins.values().all(|pin| {
                pin.resources.is_disjoint(resources)
                    && (!chunks
                        || pin.resources.iter().all(|resource| match resource {
                            PinResource::Blob(blob) => checked_blobs.contains(blob),
                            _ => true,
                        }))
            }))
    }

    /// Compare distinct protections, not ownership tokens or reader counts.
    /// Identical readers do not add liveness; any changed scope, catalog, or
    /// resource still invalidates the mark. Metadata and empty staging pins
    /// cannot affect payload liveness. Ledger revision checks still serialize
    /// admission, protection, pruning, and deletion claims.
    pub(crate) fn same_payload_pins(&self, other: &Self) -> bool {
        fn protections(inventory: &PinInventory) -> BTreeSet<&DataPin> {
            inventory
                .pins
                .values()
                .filter(|pin| pin.affects_payload_liveness())
                .collect()
        }
        protections(self) == protections(other)
    }

    fn logical_protection(&self) -> (Option<u64>, BTreeSet<&ObjectKey>) {
        let mut generation = None;
        let mut roots = BTreeSet::new();
        for pin in self.pins.values() {
            match &pin.scope {
                PinScope::Snapshot { generation: value } => {
                    generation = Some(generation.map_or(*value, |old: u64| old.max(*value)));
                }
                PinScope::Closures(keys) => roots.extend(keys),
                PinScope::Staging | PinScope::Metadata => {}
            }
            roots.extend(pin.resources.iter().filter_map(|resource| match resource {
                PinResource::Object(key) => Some(key),
                _ => None,
            }));
        }
        (generation, roots)
    }

    #[cfg(test)]
    fn same_logical_pins(&self, other: &Self) -> bool {
        self.logical_protection() == other.logical_protection()
    }

    /// New roots may already belong to the closed retained graph. Validate
    /// against that exact mark. Roots absent from its metadata snapshot have
    /// no record to prune; their eventual publication must advance the revision.
    /// Higher snapshot generations still require a fresh mark. The caller must
    /// fence this inventory's ledger revision before validation and commit
    /// against the marked metadata revision. Not valid for pre-prune deletion.
    pub(crate) async fn logical_pin_conflict(
        &self,
        marked: &Self,
        retained: &dyn super::RetainedObjects,
        snapshot: &dyn super::MetadataSnapshot,
    ) -> Result<Option<LogicalPinConflict>, MetadataError> {
        let (generation, roots) = self.logical_protection();
        let (old_generation, old_roots) = marked.logical_protection();
        if generation > old_generation {
            return Ok(Some(LogicalPinConflict::SnapshotGeneration));
        }
        for root in roots.difference(&old_roots) {
            if !retained.contains(root).await? && snapshot.object(root).await?.is_some() {
                return Ok(Some(LogicalPinConflict::UnmarkedRoot));
            }
        }
        Ok(None)
    }

    #[cfg(test)]
    async fn logical_pins_covered_by(
        &self,
        marked: &Self,
        retained: &dyn super::RetainedObjects,
        snapshot: &dyn super::MetadataSnapshot,
    ) -> Result<bool, MetadataError> {
        Ok(self
            .logical_pin_conflict(marked, retained, snapshot)
            .await?
            .is_none())
    }

    fn deleting(&self, resources: &BTreeSet<PinResource>) -> bool {
        self.deletions
            .values()
            .any(|batch| !batch.is_disjoint(resources))
    }

    fn advance(&mut self) -> Result<(), MetadataError> {
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| MetadataError::Backend("pin revision exhausted".into()))?;
        Ok(())
    }
}

/// Stable identifier of the invariants every durable inventory write keeps.
const INVENTORY_POINT: &str = "pins.inventory";

/// How far one durable inventory write may advance the ledger revision.
#[derive(Clone, Copy, Debug)]
enum RevisionStep {
    /// Object-store ledgers make one operation durable per conditional write.
    One,
    /// The local ledger makes a group of operations durable at once, and its
    /// reader reservations skip revisions that reader sidecars may have used.
    Forward,
}

/// A durable write of `next` over an inventory at `revision` advances the
/// revision by `step`. Claims validate an exact revision, so a revision that
/// repeats or goes back lets a stale claim pass.
fn validate_revision_step(revision: u64, next: u64, step: RevisionStep) -> Result<(), Violation> {
    let advanced = match step {
        RevisionStep::One => revision.checked_add(1) == Some(next),
        RevisionStep::Forward => next > revision,
    };
    invariant::ensure(INVENTORY_POINT, advanced, || {
        format!("revision {next} does not follow {revision} by {step:?}")
    })
}

/// `next` may durably replace `prev`: the revision advances by `step`, and
/// every record of `next` keeps the inventory invariants.
fn validate_inventory_successor(
    prev: &PinInventory,
    next: &PinInventory,
    step: RevisionStep,
) -> Result<(), Violation> {
    validate_revision_step(prev.revision, next.revision, step)?;
    validate_inventory_records(next, next.pins.keys(), next.deletions.keys())
}

/// The listed pins and deletion claims of `next`, and all of its ownership
/// fields, keep the inventory invariants:
///
/// - each token names one record: a pin, a deletion claim, the collector, the
///   logical prune or a reader owner;
/// - retired tokens stay pinned, under a collector;
/// - no resource is claimed twice, or claimed and pinned.
///
/// Decoding and journal replay refuse an inventory that breaks the last two,
/// so writing one would also leave the ledger unreadable.
///
/// Records left out were checked when they were written, so a write pays for
/// the records it changes and the claimed resources, and a changed claim for
/// every pinned resource. Listed tokens absent from `next` were removed.
fn validate_inventory_records<'a>(
    next: &PinInventory,
    pins: impl IntoIterator<Item = &'a PinToken>,
    deletions: impl IntoIterator<Item = &'a PinToken>,
) -> Result<(), Violation> {
    let unique = |role: &str, token: &PinToken| {
        let records = usize::from(next.pins.contains_key(token))
            + usize::from(next.deletions.contains_key(token))
            + usize::from(next.collector.as_ref() == Some(token))
            + usize::from(next.logical_prune.as_ref() == Some(token))
            + usize::from(next.reader_owners.contains(token));
        invariant::ensure(INVENTORY_POINT, records == 1, || {
            format!("{role} token {token} names {records} records")
        })
    };
    let pins: Vec<_> = pins
        .into_iter()
        .filter_map(|token| next.pins.get_key_value(token))
        .collect();
    let mut claims_changed = false;
    for token in deletions {
        if next.deletions.contains_key(token) {
            claims_changed = true;
            unique("deletion claim", token)?;
        }
    }
    for (token, _) in &pins {
        unique("pin", token)?;
    }
    for token in next.collector.iter() {
        unique("collector", token)?;
    }
    // A logical prune may run without a collector: `PinStore::begin_prune`
    // admits one, and only repository collection pairs the two.
    for token in next.logical_prune.iter() {
        unique("logical prune", token)?;
    }
    for token in &next.reader_owners {
        unique("reader owner", token)?;
    }
    invariant::ensure(
        INVENTORY_POINT,
        next.retired.is_empty() || next.collector.is_some(),
        || format!("{} retired pins without a collector", next.retired.len()),
    )?;
    if let Some(token) = next
        .retired
        .iter()
        .find(|token| !next.pins.contains_key(*token))
    {
        return Err(Violation::new(
            INVENTORY_POINT,
            format!("retired token {token} is not pinned"),
        ));
    }
    if next.deletions.is_empty() {
        return Ok(());
    }
    let mut claimed = BTreeMap::new();
    for (token, resources) in &next.deletions {
        for resource in resources {
            if let Some(other) = claimed.insert(resource, token) {
                return Err(Violation::new(
                    INVENTORY_POINT,
                    format!("deletion claims {other} and {token} cover the same resource"),
                ));
            }
        }
    }
    let uncovered = |token: &PinToken, pin: &DataPin| match pin
        .resources
        .iter()
        .find_map(|resource| claimed.get(resource))
    {
        Some(claim) => Err(Violation::new(
            INVENTORY_POINT,
            format!("deletion claim {claim} covers a resource pin {token} protects"),
        )),
        None => Ok(()),
    };
    if claims_changed {
        // A changed claim may cover any pin, not only a changed one.
        for (token, pin) in &next.pins {
            uncovered(token, pin)?;
        }
    } else {
        for (token, pin) in pins {
            uncovered(token, pin)?;
        }
    }
    Ok(())
}

#[async_trait]
impl PinStore for MemoryPinStore {
    async fn inventory(&self) -> Result<PinInventory, MetadataError> {
        Ok(self.state.lock().await.clone())
    }

    async fn begin_collection(
        &self,
        revision: u64,
        previous: Option<PinToken>,
    ) -> Result<Option<PinToken>, MetadataError> {
        let mut state = self.state.lock().await;
        if state.revision != revision || state.collector != previous {
            return Ok(None);
        }
        let token = PinToken::fresh()?;
        state.advance()?;
        state.collector = Some(token.clone());
        Ok(Some(token))
    }

    async fn acquire_collection(
        &self,
        previous: Option<PinToken>,
    ) -> Result<Option<PinToken>, MetadataError> {
        let mut state = self.state.lock().await;
        if state.collector != previous {
            return Ok(None);
        }
        let token = PinToken::fresh()?;
        state.advance()?;
        state.collector = Some(token.clone());
        Ok(Some(token))
    }

    async fn finish_collection(&self, token: &PinToken) -> Result<(), MetadataError> {
        let mut state = self.state.lock().await;
        if state.collector.as_ref() != Some(token) {
            return Ok(());
        }
        if state.logical_prune.is_some() || !state.deletions.is_empty() {
            return Err(MetadataError::Backend(
                "collector still owns an unfinished prune or deletion".into(),
            ));
        }
        state.advance()?;
        for token in std::mem::take(&mut state.retired) {
            state.pins.remove(&token);
        }
        state.collector = None;
        Ok(())
    }

    async fn register(&self, pin: DataPin) -> Result<Option<PinToken>, MetadataError> {
        pin.validate()?;
        let mut state = self.state.lock().await;
        if (state.logical_prune.is_some() && pin.scope != PinScope::Metadata)
            || state.deleting(&pin.resources)
        {
            return Ok(None);
        }
        let token = PinToken::fresh()?;
        state.advance()?;
        state.pins.insert(token.clone(), pin);
        Ok(Some(token))
    }

    async fn protect(
        &self,
        token: &PinToken,
        resources: BTreeSet<PinResource>,
    ) -> Result<bool, MetadataError> {
        let mut state = self.state.lock().await;
        if state.retired.contains(token) {
            return Err(MetadataError::Backend("pin has been released".into()));
        }
        let Some(pin) = state.pins.get(token) else {
            return Err(MetadataError::Backend("pin no longer exists".into()));
        };
        if pin.scope == PinScope::Metadata && !metadata_resources(&resources) {
            return Err(MetadataError::Corruption(
                "metadata pin cannot acquire payload or logical resources".into(),
            ));
        }
        if resources.is_subset(&pin.resources) {
            return Ok(true);
        }
        if (state.logical_prune.is_some() && pin.scope != PinScope::Metadata)
            || state.deleting(&resources)
        {
            return Ok(false);
        }
        state.advance()?;
        state
            .pins
            .get_mut(token)
            .expect("checked above")
            .resources
            .extend(resources);
        Ok(true)
    }

    async fn release(&self, token: &PinToken) -> Result<(), MetadataError> {
        let mut state = self.state.lock().await;
        if state.pins.contains_key(token) && !state.retired.contains(token) {
            state.advance()?;
            if state.collector.is_some() {
                state.retired.insert(token.clone());
            } else {
                state.pins.remove(token);
            }
        }
        Ok(())
    }

    async fn claim_deletions(
        &self,
        revision: u64,
        resources: BTreeSet<PinResource>,
    ) -> Result<Option<PinToken>, MetadataError> {
        let mut state = self.state.lock().await;
        if state.revision != revision
            || state.logical_prune.is_some()
            || state.deleting(&resources)
            || state
                .pins
                .values()
                .any(|pin| !pin.resources.is_disjoint(&resources))
        {
            return Ok(None);
        }
        let token = PinToken::fresh()?;
        state.advance()?;
        state.deletions.insert(token.clone(), resources);
        Ok(Some(token))
    }

    async fn claim_deletions_validated(
        &self,
        collector: &PinToken,
        resources: BTreeSet<PinResource>,
        checked_blobs: BTreeSet<BlobId>,
    ) -> Result<Option<PinToken>, MetadataError> {
        let mut state = self.state.lock().await;
        if !state.can_claim_validated(collector, &resources, &checked_blobs)? {
            return Ok(None);
        }
        let token = PinToken::fresh()?;
        state.advance()?;
        state.deletions.insert(token.clone(), resources);
        Ok(Some(token))
    }

    async fn claim_deletions_during_prune(
        &self,
        revision: u64,
        resources: BTreeSet<PinResource>,
        collector: &PinToken,
        prune: &PinToken,
    ) -> Result<Option<PinToken>, MetadataError> {
        let mut state = self.state.lock().await;
        if state.revision != revision
            || state.collector.as_ref() != Some(collector)
            || state.logical_prune.as_ref() != Some(prune)
            || state.deleting(&resources)
            || state
                .pins
                .values()
                .any(|pin| !pin.resources.is_disjoint(&resources))
        {
            return Ok(None);
        }
        let token = PinToken::fresh()?;
        state.advance()?;
        state.deletions.insert(token.clone(), resources);
        Ok(Some(token))
    }

    async fn finish_deletions(&self, token: &PinToken) -> Result<(), MetadataError> {
        let mut state = self.state.lock().await;
        if state.deletions.contains_key(token) {
            state.advance()?;
            state.deletions.remove(token);
        }
        Ok(())
    }

    async fn begin_prune(&self, revision: u64) -> Result<Option<PinToken>, MetadataError> {
        self.begin_prune_recovering(revision, BTreeSet::new()).await
    }

    async fn begin_prune_recovering(
        &self,
        revision: u64,
        claims: BTreeSet<PinToken>,
    ) -> Result<Option<PinToken>, MetadataError> {
        let mut state = self.state.lock().await;
        if state.revision != revision
            || state.logical_prune.is_some()
            || state.deletions.keys().cloned().collect::<BTreeSet<_>>() != claims
        {
            return Ok(None);
        }
        let token = PinToken::fresh()?;
        state.advance()?;
        state.logical_prune = Some(token.clone());
        Ok(Some(token))
    }

    async fn begin_prune_validating(
        &self,
        collector: &PinToken,
        claims: BTreeSet<PinToken>,
    ) -> Result<Option<(PinToken, PinInventory)>, MetadataError> {
        let mut state = self.state.lock().await;
        if state.collector.as_ref() != Some(collector)
            || state.logical_prune.is_some()
            || state.deletions.keys().cloned().collect::<BTreeSet<_>>() != claims
        {
            return Ok(None);
        }
        let inventory = state.clone();
        let token = PinToken::fresh()?;
        state.advance()?;
        state.logical_prune = Some(token.clone());
        Ok(Some((token, inventory)))
    }

    async fn finish_prune(&self, token: &PinToken) -> Result<(), MetadataError> {
        let mut state = self.state.lock().await;
        if state.logical_prune.as_ref() == Some(token) {
            state.advance()?;
            state.logical_prune = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
