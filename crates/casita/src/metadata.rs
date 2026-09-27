//! Revisioned repository metadata: object records, roots, and payload catalogs.
//!
//! Object records and named roots share one atomic commit boundary. Payload
//! bytes live outside this state and must be durable before mutation.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use auto_impl::auto_impl;
use futures::TryStreamExt;
use futures::stream::{self, BoxStream};
use imbl::{OrdMap, OrdSet};

use crate::format::VerifiedObject;
use crate::object::{ObjectKey, ObjectRecord, RepositoryRevision, RootName, RootRecord};

mod closure;
pub(crate) mod records;
pub use records::{
    MetadataChange, MetadataCheck, MetadataCommitResult, MetadataCursor, MetadataKey, MetadataPage,
    MetadataRecord,
};
mod lease;
pub(crate) use lease::{run_lease_task, spawn_lease_task};
mod pins;
pub(crate) use pins::LogicalPinConflict;
#[cfg(all(test, feature = "s3"))]
pub(crate) use pins::chroma_pin_store;
mod sqlite;
pub use lease::{RepositoryLease, flush_repository_leases};
pub use pins::{
    BackendWriteScope, DataPin, DataPinLease, FilePinStore, MemoryPinStore, ObjectPinStore,
    PinInventory, PinResource, PinScope, PinStore, PinToken,
};
pub(crate) use pins::{
    CollectorLease, PinBindings, PruningPinStore, WritePins, flush_pin_releases,
};
pub use sqlite::TursoMetadataStore;
#[cfg(feature = "s3")]
mod wal3;
#[cfg(feature = "s3")]
mod wal3_shard;
#[cfg(feature = "s3")]
pub use wal3::{Wal3MetadataStore, Wal3ReadStats, Wal3RepositoryHold};

/// Admit a stable metadata view without retaining its logical objects or payloads.
pub(crate) async fn read_snapshot(
    store: &(impl MetadataStore + ?Sized),
) -> Result<(Arc<dyn MetadataSnapshot>, Option<DataPinLease>), MetadataError> {
    for attempt in 0..32 {
        let candidate = store.snapshot().await?;
        let revision = candidate.revision();
        let resources = candidate.retention_resources();
        if resources.is_empty() {
            return Ok((candidate, None));
        }
        drop(candidate);
        if let Some(pin) = DataPinLease::try_acquire(
            store.pin_store().await?,
            DataPin {
                scope: PinScope::Metadata,
                catalog: None,
                resources: resources.clone(),
            },
        )
        .await?
        {
            let snapshot = store.snapshot().await?;
            if snapshot.revision() == revision && snapshot.retention_resources() == resources {
                return Ok((snapshot, Some(pin)));
            }
        }
        // A denied claim may already have deleted this candidate. Reload
        // instead of waiting to register its obsolete shard paths.
        tokio::time::sleep(std::time::Duration::from_millis(1 << attempt.min(6))).await;
    }
    Err(MetadataError::Transient(
        "metadata snapshot remained contended".to_owned(),
    ))
}

/// A change to one durable named root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RootChange {
    /// Create or atomically repoint a name.
    Set {
        /// Exact canonical name.
        name: RootName,
        /// Complete object graph to retain.
        target: ObjectKey,
    },
    /// Remove a name if present.
    Remove {
        /// Exact canonical name.
        name: RootName,
    },
}

impl RootChange {
    fn name(&self) -> &RootName {
        match self {
            Self::Set { name, .. } | Self::Remove { name } => name,
        }
    }
}

/// One atomic logical-state mutation.
///
/// Normal mutation may add verified objects and change roots together.
/// Collection instead installs an exact retained object-key set and cannot be
/// combined with mutation or root changes.
#[derive(Debug, Clone, Default)]
pub struct MetadataMutation {
    objects: Vec<VerifiedObject>,
    records: BTreeMap<MetadataKey, Option<bytes::Bytes>>,
    checks: Vec<MetadataCheck>,
    pub(crate) require_validated_roots: bool,
    roots: BTreeMap<RootName, RootChange>,
    retained_objects: Option<Arc<dyn RetainedObjects>>,
    validated_closures: Vec<ObjectKey>,
    payload_catalog: Option<Vec<u8>>,
}

/// A re-readable source of the exact object keys a collection retains.
///
/// Marking a repository larger than memory produces a retained set larger than
/// memory, so a state store reads it in canonical-order pages instead of
/// receiving one allocation. A source is immutable and may be paged more than
/// once: a commit retried after emergency reclamation reads it again.
#[async_trait]
pub trait RetainedObjects: Send + Sync + std::fmt::Debug {
    /// Distinct keys this source yields.
    fn len(&self) -> usize;

    /// Whether the source is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// At most `limit` keys strictly after `after`, in canonical order.
    async fn page(
        &self,
        after: Option<ObjectKey>,
        limit: usize,
    ) -> Result<Vec<ObjectKey>, MetadataError>;

    /// Whether the exact retained set contains `key`.
    ///
    /// Spill-backed sources override this with an indexed point lookup. The
    /// paged default preserves compatibility for custom sources while keeping
    /// memory bounded.
    async fn contains(&self, key: &ObjectKey) -> Result<bool, MetadataError> {
        let mut after = None;
        loop {
            let page = self.page(after.clone(), RETAINED_PAGE).await?;
            let Some(last) = page.last() else {
                return Ok(false);
            };
            match page.binary_search(key) {
                Ok(_) => return Ok(true),
                Err(_) if last >= key => return Ok(false),
                Err(_) => after = Some(last.clone()),
            }
        }
    }
}

/// Keys a store reads per page from a [`RetainedObjects`] source.
pub const RETAINED_PAGE: usize = 1024;

#[async_trait]
impl RetainedObjects for BTreeSet<ObjectKey> {
    fn len(&self) -> usize {
        BTreeSet::len(self)
    }

    async fn page(
        &self,
        after: Option<ObjectKey>,
        limit: usize,
    ) -> Result<Vec<ObjectKey>, MetadataError> {
        let range: Box<dyn Iterator<Item = &ObjectKey>> = match &after {
            Some(after) => Box::new(self.range((
                std::ops::Bound::Excluded(after.clone()),
                std::ops::Bound::Unbounded,
            ))),
            None => Box::new(self.iter()),
        };
        Ok(range.take(limit).cloned().collect())
    }

    async fn contains(&self, key: &ObjectKey) -> Result<bool, MetadataError> {
        Ok(BTreeSet::contains(self, key))
    }
}

impl MetadataMutation {
    pub(crate) fn take_root_changes(&mut self) -> Vec<RootChange> {
        std::mem::take(&mut self.roots).into_values().collect()
    }
    /// An empty normal mutation.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a repository-verified immutable record.
    pub fn add_object(&mut self, object: VerifiedObject) -> &mut Self {
        self.objects.push(object);
        self
    }

    /// Add several repository-verified records.
    pub fn add_objects(&mut self, objects: impl IntoIterator<Item = VerifiedObject>) -> &mut Self {
        self.objects.extend(objects);
        self
    }

    /// Record that each key's whole closure has been verified.
    ///
    /// Records are immutable and only collection removes them, so a closure
    /// that verified once stays verified. Remembering that lets a later
    /// traversal stop at a known-good object instead of re-walking everything
    /// beneath it. The claim must only be made for keys whose closure this
    /// same mutation's caller actually verified.
    pub(crate) fn mark_validated_closures(
        &mut self,
        keys: impl IntoIterator<Item = ObjectKey>,
    ) -> &mut Self {
        self.validated_closures.extend(keys);
        self
    }

    /// Keys whose closures this mutation records as verified.
    pub fn validated_closures(&self) -> &[ObjectKey] {
        &self.validated_closures
    }

    /// Commit the physical payload-discovery catalog beside this logical
    /// mutation. Only coordinating state backends preserve this metadata.
    pub(crate) fn set_payload_catalog(&mut self, catalog: Vec<u8>) -> &mut Self {
        self.payload_catalog = Some(catalog);
        self
    }

    /// Create or atomically repoint a named root.
    pub fn set_root(&mut self, name: RootName, target: ObjectKey) -> &mut Self {
        self.roots
            .insert(name.clone(), RootChange::Set { name, target });
        self
    }

    /// Remove a named root if it exists.
    pub fn remove_root(&mut self, name: RootName) -> &mut Self {
        self.roots.insert(name.clone(), RootChange::Remove { name });
        self
    }

    /// Build the exclusive logical-prune mutation used by collection.
    pub fn install_retained_objects(objects: BTreeSet<ObjectKey>) -> Self {
        Self::install_retained_source(Arc::new(objects))
    }

    /// A collection mutation whose retained set is read from `source` in pages,
    /// so a marked set larger than memory never has to be materialized.
    pub fn install_retained_source(source: Arc<dyn RetainedObjects>) -> Self {
        Self {
            retained_objects: Some(source),
            ..Self::default()
        }
    }

    /// Verified object additions in this mutation.
    pub fn objects(&self) -> &[VerifiedObject] {
        &self.objects
    }

    /// Root changes in canonical name order.
    pub fn root_changes(&self) -> impl Iterator<Item = &RootChange> {
        self.roots.values()
    }

    /// Exact retained set for a collection mutation, if this is one.
    pub fn retained_objects(&self) -> Option<&Arc<dyn RetainedObjects>> {
        self.retained_objects.as_ref()
    }
}

/// Result of one successful logical commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitResult {
    /// Fresh revision identifying the committed state.
    pub revision: RepositoryRevision,
    /// Newly inserted records; identical existing records are idempotent.
    pub objects_inserted: usize,
    /// Records atomically pruned by a collection mutation.
    pub objects_removed: usize,
    /// Root mappings whose final value differs from the previous state.
    pub roots_changed: usize,
}

/// Failures at the revisioned logical-state boundary.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MetadataError {
    /// Active database readers or writers prevent maintenance from completing.
    #[error("state maintenance is busy: {0}")]
    Busy(String),
    /// Checkpoint admission was refused before any logical commit was submitted.
    /// Retain staging protection, refresh expectations and retry within a budget.
    #[error("logical shard checkpoint publication is fenced by maintenance")]
    MaintenanceFenced,
    /// A root lacks a current root-protection or persisted verification witness.
    /// The repository must admit and pin its graph before ordinary publication.
    #[error("root requires repository graph verification")]
    RootVerificationRequired,
    /// Invalid application metadata input.
    #[error("invalid metadata request: {0}")]
    InvalidMetadata(String),
    /// First failed expected-value check; no state was changed.
    #[error("metadata check {index} failed")]
    CheckFailed { index: usize },
    /// The selected backend does not implement mutable application records.
    #[error("mutable metadata is unsupported by this backend")]
    UnsupportedMetadata,
    /// The state engine could not durably commit because its filesystem is full.
    #[error("state storage is full")]
    StorageFull,
    /// Optimistic commit expected an obsolete state.
    #[error("stale repository revision: expected {expected}, current state is {actual}")]
    StaleRevision {
        /// Revision supplied by the caller.
        expected: RepositoryRevision,
        /// Current committed revision.
        actual: RepositoryRevision,
    },
    /// The same immutable key already names a different record.
    #[error("immutable object conflict at {0}")]
    ImmutableConflict(ObjectKey),
    /// A root or forward link names an absent record.
    #[error("object graph is missing {missing}, referenced from {from:?}")]
    MissingObject {
        /// Missing key.
        missing: ObjectKey,
        /// Referrer, or `None` when a root directly names the missing key.
        from: Option<ObjectKey>,
    },
    /// A collection retained set is not closed over forward links and roots.
    #[error("invalid retained object set: {0}")]
    InvalidRetainedSet(String),
    /// Collection and mutation semantics were mixed in one commit.
    #[error("a retained-set installation cannot include object or root changes")]
    MixedCollectionMutation,
    /// A backend lock was poisoned by a panic.
    #[error("logical state lock is poisoned")]
    Poisoned,
    /// Durable logical state violates its own encoding or referential
    /// invariants.
    #[error("logical state is corrupt: {0}")]
    Corruption(String),
    /// Fresh revision entropy was unavailable.
    #[error("could not generate a fresh repository revision: {0}")]
    RevisionEntropy(String),
    /// A temporary state-backend failure that may succeed after bounded retry.
    #[error("transient state backend failure: {0}")]
    Transient(String),
    /// A backend-specific failure.
    #[error("state backend failed: {0}")]
    Backend(String),
}

/// Immutable, internally consistent logical-state view.
#[async_trait]
pub trait MetadataSnapshot: Send + Sync {
    /// Opaque application values in input order, including duplicates and misses.
    async fn get(&self, _keys: &[MetadataKey]) -> Result<Vec<Option<bytes::Bytes>>, MetadataError> {
        Err(MetadataError::UnsupportedMetadata)
    }

    /// At most limit+1 records for a byte prefix, strictly after a continuation.
    /// The extra record determines whether a next page exists.
    async fn scan(
        &self,
        _prefix: &MetadataKey,
        _after: Option<&[u8]>,
        _limit: usize,
    ) -> Result<Vec<MetadataRecord>, MetadataError> {
        Err(MetadataError::UnsupportedMetadata)
    }

    /// Revision cited by every observation through this snapshot.
    fn revision(&self) -> RepositoryRevision;

    /// Monotonic metadata generation at this revision. Object birth generations
    /// remain unchanged by idempotent insertion and checkpoint rewrites.
    /// Backends must persist the counter atomically with each successful commit.
    fn generation(&self) -> Result<u64, MetadataError> {
        Err(MetadataError::Backend(
            "metadata backend does not support snapshot generations".into(),
        ))
    }

    /// Stream current records first inserted at or before `generation`.
    /// Removing and later reinserting a record gives it a new birth generation.
    /// This is an unordered, bounded-memory scan used to expand snapshot pins.
    fn objects_created_through(
        &self,
        _generation: u64,
    ) -> BoxStream<'static, Result<ObjectRecord, MetadataError>> {
        Box::pin(stream::once(async {
            Err(MetadataError::Backend(
                "metadata backend does not support snapshot generations".into(),
            ))
        }))
    }

    /// Physical payload catalog committed with this exact logical revision.
    /// Backends that do not coordinate payload discovery return `None`.
    fn payload_catalog(&self) -> Option<&[u8]> {
        None
    }

    /// Immutable metadata files needed for future reads through this view.
    /// A repository pins these before exposing the snapshot and rechecks both
    /// the logical revision and this set after registration. In-memory views
    /// and transaction-owned snapshots need no external file protection.
    fn retention_resources(&self) -> BTreeSet<PinResource> {
        BTreeSet::new()
    }

    /// Look up an immutable object record.
    async fn object(&self, key: &ObjectKey) -> Result<Option<ObjectRecord>, MetadataError>;

    /// Look up several immutable object records, answered in input order.
    ///
    /// Graph traversals resolve records by the thousand, and a backend may pay
    /// a fixed cost per call that dwarfs the lookup itself: the SQLite backend
    /// hands each call to a blocking task and takes the connection's lock for
    /// it. Backends override this to pay that cost once for the whole batch.
    /// The default loops [`object`](Self::object).
    async fn object_batch(
        &self,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<ObjectRecord>>, MetadataError> {
        let mut records = Vec::with_capacity(keys.len());
        for key in keys {
            records.push(self.object(key).await?);
        }
        Ok(records)
    }

    /// Look up only each object's physical payload identity and declared size.
    ///
    /// Backends may answer this without decoding the object's link set. This
    /// matters for native Git views whose distinct-object catalog is small but
    /// whose historical trees can contain millions of repeated edges.
    async fn object_payload_batch(
        &self,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<(crate::BlobId, u64)>>, MetadataError> {
        Ok(self
            .object_batch(keys)
            .await?
            .into_iter()
            .map(|record| record.map(|record| (record.payload(), record.payload_size())))
            .collect())
    }

    /// Payload metadata only for objects whose closure is validated in this snapshot.
    /// Missing and unvalidated objects both return `None`, in input order.
    async fn validated_payload_batch(
        &self,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<(crate::BlobId, u64)>>, MetadataError> {
        let validated = self.validated_closures(keys).await?;
        Ok(self
            .object_payload_batch(keys)
            .await?
            .into_iter()
            .zip(validated)
            .map(|(payload, valid)| if valid { payload } else { None })
            .collect())
    }

    /// Whether each key's closure is already recorded as verified, in input
    /// order.
    ///
    /// A backend that keeps no such record answers `false` throughout, which
    /// costs a caller nothing but a full traversal.
    async fn validated_closures(&self, keys: &[ObjectKey]) -> Result<Vec<bool>, MetadataError> {
        Ok(vec![false; keys.len()])
    }

    /// Look up a named root.
    async fn root(&self, name: &RootName) -> Result<Option<ObjectKey>, MetadataError>;

    /// Stream every immutable object record in key order.
    fn objects(&self) -> BoxStream<'static, Result<ObjectRecord, MetadataError>>;

    /// Stream every immutable object record without an ordering guarantee.
    ///
    /// Whole-state scans such as integrity audit and collection do not need
    /// key order. Backends whose canonical index is separate from record
    /// storage can override this to use their cheaper physical scan order.
    fn objects_unordered(&self) -> BoxStream<'static, Result<ObjectRecord, MetadataError>> {
        self.objects()
    }

    /// Stream every named root in name order.
    fn roots(&self) -> BoxStream<'static, Result<RootRecord, MetadataError>>;

    /// Stream one root and its descendants in name order. Backends without a
    /// range index can filter the complete root stream.
    fn roots_under(
        &self,
        prefix: &RootName,
    ) -> BoxStream<'static, Result<RootRecord, MetadataError>> {
        let prefix = prefix.clone();
        let mut roots = self.roots();
        Box::pin(async_stream::try_stream! {
            while let Some(root) = roots.try_next().await? {
                if root.name().is_under(&prefix) {
                    yield root;
                }
            }
        })
    }
}

/// Writes an edit returns for [`VerificationFacts::edit`]: the entry to store
/// at each key, or `None` to delete it.
pub type FactsEdit =
    Box<dyn FnOnce(Vec<Option<Vec<u8>>>) -> Vec<(Vec<u8>, Option<Vec<u8>>)> + Send>;

/// Facts this repository verified itself, keyed by an opaque identity.
///
/// Like the validated-closure mark on object records, an entry says what the
/// local repository established and may trust again cheaply: a measured
/// digest, the storage objects a payload was found in, or a requirement it
/// placed on itself. Entries are not metadata records, portable objects, or
/// GC roots: they never enter a snapshot, a commit, or a transfer, a damaged
/// read may clear them from a read path, and nothing they describe is
/// retained on their account. An empty value is a tombstone that survives
/// clearing.
#[async_trait]
pub trait VerificationFacts: Send + Sync {
    /// Handles reporting the same scope observe the same entries, so callers
    /// may coalesce in-flight work on one identity across them.
    fn scope(&self) -> Option<std::path::PathBuf> {
        None
    }

    /// One entry, tombstones included.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetadataError>;

    /// Read `keys` and apply the writes `edit` returns, atomically.
    async fn edit(&self, keys: Vec<Vec<u8>>, edit: FactsEdit) -> Result<(), MetadataError>;

    /// Delete every non-empty entry and store a tombstone at `tombstone`,
    /// atomically, incrementing and retaining the little-endian u64 at
    /// `generation`. An absent generation starts at zero.
    async fn clear(&self, tombstone: Vec<u8>, generation: Vec<u8>) -> Result<(), MetadataError>;

    /// Keys of non-empty entries after `after`, ascending, at most `limit`.
    async fn page(&self, after: Vec<u8>, limit: usize) -> Result<Vec<Vec<u8>>, MetadataError>;
}

pub(crate) fn facts_generation(value: Option<&[u8]>) -> Result<u64, MetadataError> {
    match value {
        None => Ok(0),
        Some(value) => value
            .try_into()
            .map(u64::from_le_bytes)
            .map_err(|_| MetadataError::Corruption("invalid verification facts generation".into())),
    }
}

fn next_facts_generation(value: Option<&[u8]>) -> Result<u64, MetadataError> {
    facts_generation(value)?
        .checked_add(1)
        .ok_or_else(|| MetadataError::Backend("verification facts generation exhausted".into()))
}

/// Verification facts for ephemeral repositories.
#[derive(Default)]
pub struct MemoryVerificationFacts {
    rows: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
}

#[async_trait]
impl VerificationFacts for MemoryVerificationFacts {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetadataError> {
        let rows = self.rows.lock().map_err(|_| MetadataError::Poisoned)?;
        Ok(rows.get(key).cloned())
    }

    async fn edit(&self, keys: Vec<Vec<u8>>, edit: FactsEdit) -> Result<(), MetadataError> {
        let mut rows = self.rows.lock().map_err(|_| MetadataError::Poisoned)?;
        let current = keys.iter().map(|key| rows.get(key).cloned()).collect();
        for (key, value) in edit(current) {
            match value {
                Some(value) => {
                    rows.insert(key, value);
                }
                None => {
                    rows.remove(&key);
                }
            }
        }
        Ok(())
    }

    async fn clear(&self, tombstone: Vec<u8>, generation: Vec<u8>) -> Result<(), MetadataError> {
        let mut rows = self.rows.lock().map_err(|_| MetadataError::Poisoned)?;
        let next = next_facts_generation(rows.get(&generation).map(Vec::as_slice))?;
        rows.retain(|_, value| value.is_empty());
        rows.insert(tombstone, Vec::new());
        rows.insert(generation, next.to_le_bytes().to_vec());
        Ok(())
    }

    async fn page(&self, after: Vec<u8>, limit: usize) -> Result<Vec<Vec<u8>>, MetadataError> {
        use std::ops::Bound::{Excluded, Unbounded};
        let rows = self.rows.lock().map_err(|_| MetadataError::Poisoned)?;
        Ok(rows
            .range((Excluded(after), Unbounded))
            .filter(|(_, value)| !value.is_empty())
            .take(limit)
            .map(|(key, _)| key.clone())
            .collect())
    }
}

/// Atomic storage of repository metadata: records, roots, and one revision.
///
/// Payload bytes belong to the separate [`crate::blob::BlobStore`]. A composed
/// repository must make payloads durable before publishing their records and
/// coordinate retention with the storage that owns those payloads. A metadata
/// snapshot alone does not protect independently managed blob storage from GC.
#[async_trait]
#[auto_impl(&, Arc, Box)]
pub trait MetadataStore: Send + Sync {
    /// Whether this store atomically preserves opaque records and checks.
    fn supports_metadata_records(&self) -> bool {
        false
    }

    /// Whether root changes also maintain the built-in retention-policy
    /// record atomically. Custom stores opt in only after implementing that
    /// invariant; opaque application-record support alone is insufficient.
    fn supports_root_retention(&self) -> bool {
        false
    }

    /// Where this store keeps locally verified facts, if it keeps any.
    ///
    /// A store without one leaves every caller measuring again, which is the
    /// right answer for remote and custom backends whose bytes this process
    /// did not verify.
    fn verification_facts(&self) -> Option<Arc<dyn VerificationFacts>> {
        None
    }

    /// Read only the requested application records from one consistent view.
    /// A backend may avoid loading unrelated object or payload catalogs when
    /// the caller needs neither a revision token nor a retained snapshot.
    async fn get_records(
        &self,
        keys: &[MetadataKey],
    ) -> Result<Vec<Option<bytes::Bytes>>, MetadataError> {
        let (snapshot, _pin) = read_snapshot(self).await?;
        snapshot.get(keys).await
    }

    /// Check current record/root values and commit without requiring a global
    /// revision. Implementations must serialize checks and changes together.
    async fn commit_checked(
        &self,
        _mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        Err(MetadataError::UnsupportedMetadata)
    }

    /// The durable online pin ledger shared by every handle of this metadata
    /// repository. A backend must never substitute a per-handle ledger for
    /// shared state. Unsupported compositions fail instead of using blocking
    /// repository holds.
    async fn pin_store(&self) -> Result<Arc<dyn PinStore>, MetadataError> {
        Err(MetadataError::Backend(
            "metadata backend does not provide an online pin ledger".into(),
        ))
    }

    /// Attempt durable collector-only ownership; `None` means another
    /// collector owns maintenance. Readers and writers use online data pins.
    /// Stores shared between processes without a common lock must return a
    /// durable lease; local and in-memory stores return
    /// [`RepositoryLease::process_local`]. There is deliberately no default,
    /// so a wrapper cannot silently drop a shared store's exclusion.
    async fn try_collection_lease(&self) -> Result<Option<RepositoryLease>, MetadataError>;

    /// Whether commits and snapshots preserve [`MetadataMutation`]'s physical
    /// payload catalog witness.
    /// Repository construction reads this capability once to select its
    /// publication strategy; it must remain stable for the backend's lifetime.
    /// Wrappers must forward it: answering `false` for a coordinating store
    /// would publish records without their payload catalog.
    fn coordinates_payload_catalog(&self) -> bool;

    /// Open one immutable, internally consistent snapshot.
    async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError>;

    /// Commit against an exact expected revision or fail stale.
    async fn commit(
        &self,
        expected: &RepositoryRevision,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError>;

    /// Fold backend-specific transient write state into its compact durable
    /// representation after a large bounded operation.
    ///
    /// This is maintenance, not a logical commit: the default is a no-op and
    /// callers must not depend on it for correctness. Persistent backends may
    /// use it to bound journals or other append-only write amplification.
    /// Implementations must coordinate with publication and live snapshots,
    /// and report contention rather than claiming incomplete compaction succeeded.
    async fn compact_transient_state(&self) -> Result<(), MetadataError> {
        Ok(())
    }
}

#[derive(Clone)]
struct MemorySnapshot {
    records: OrdMap<MetadataKey, bytes::Bytes>,
    revision: RepositoryRevision,
    generation: u64,
    births: OrdMap<ObjectKey, u64>,
    objects: OrdMap<ObjectKey, ObjectRecord>,
    roots: OrdMap<RootName, ObjectKey>,
    validated: OrdSet<ObjectKey>,
    payload_catalog: Option<Arc<Vec<u8>>>,
}

#[async_trait]
impl MetadataSnapshot for MemorySnapshot {
    async fn get(&self, keys: &[MetadataKey]) -> Result<Vec<Option<bytes::Bytes>>, MetadataError> {
        records::validate_get(keys)?;
        let values: Vec<_> = keys
            .iter()
            .map(|key| self.records.get(key).cloned())
            .collect();
        if values
            .iter()
            .flatten()
            .map(bytes::Bytes::len)
            .sum::<usize>()
            > records::MAX_BATCH_BYTES
        {
            return Err(records::invalid(
                "metadata get result exceeds 16 MiB; split the batch",
            ));
        }
        Ok(values)
    }

    async fn scan(
        &self,
        prefix: &MetadataKey,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<MetadataRecord>, MetadataError> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        records::validate_scan(prefix, after, limit)?;
        let lower = match after {
            Some(key) => Excluded(MetadataKey::new(prefix.namespace.clone(), key.to_vec())),
            None => Included(prefix.clone()),
        };
        let mut result = Vec::new();
        let mut bytes = 0;
        for (key, value) in self
            .records
            .range((lower, Unbounded))
            .take_while(|(key, _)| {
                key.namespace == prefix.namespace && key.key.starts_with(&prefix.key)
            })
            .take(limit + 1)
        {
            bytes += key.key.len() + value.len();
            result.push(MetadataRecord {
                key: key.clone(),
                value: value.clone(),
            });
            if bytes > records::MAX_BATCH_BYTES {
                break;
            }
        }
        Ok(result)
    }

    fn revision(&self) -> RepositoryRevision {
        self.revision
    }

    fn generation(&self) -> Result<u64, MetadataError> {
        Ok(self.generation)
    }

    fn objects_created_through(
        &self,
        generation: u64,
    ) -> BoxStream<'static, Result<ObjectRecord, MetadataError>> {
        let snapshot = self.clone();
        Box::pin(async_stream::try_stream! {
            for (key, record) in snapshot.objects.iter() {
                let birth = snapshot.births.get(key).ok_or_else(|| {
                    MetadataError::Corruption(format!("missing birth generation for {key}"))
                })?;
                if *birth <= generation {
                    yield record.clone();
                }
            }
        })
    }

    fn payload_catalog(&self) -> Option<&[u8]> {
        self.payload_catalog.as_deref().map(Vec::as_slice)
    }

    async fn object(&self, key: &ObjectKey) -> Result<Option<ObjectRecord>, MetadataError> {
        Ok(self.objects.get(key).cloned())
    }

    async fn validated_closures(&self, keys: &[ObjectKey]) -> Result<Vec<bool>, MetadataError> {
        Ok(keys
            .iter()
            .map(|key| self.validated.contains(key))
            .collect())
    }

    async fn root(&self, name: &RootName) -> Result<Option<ObjectKey>, MetadataError> {
        Ok(self.roots.get(name).cloned())
    }

    fn objects(&self) -> BoxStream<'static, Result<ObjectRecord, MetadataError>> {
        let records: Vec<_> = self.objects.values().cloned().map(Ok).collect();
        Box::pin(stream::iter(records))
    }

    fn roots(&self) -> BoxStream<'static, Result<RootRecord, MetadataError>> {
        let roots: Vec<_> = self
            .roots
            .iter()
            .map(|(name, target)| Ok(RootRecord::new(name.clone(), target.clone())))
            .collect();
        Box::pin(stream::iter(roots))
    }
}

struct MemoryState {
    records: OrdMap<MetadataKey, bytes::Bytes>,
    revision: RepositoryRevision,
    generation: u64,
    births: OrdMap<ObjectKey, u64>,
    objects: OrdMap<ObjectKey, ObjectRecord>,
    roots: OrdMap<RootName, ObjectKey>,
    validated: OrdSet<ObjectKey>,
    payload_catalog: Option<Arc<Vec<u8>>>,
}

/// In-process implementation of the complete revisioned state contract.
#[derive(Clone)]
pub struct MemoryMetadataStore {
    state: Arc<Mutex<MemoryState>>,
    pins: Arc<MemoryPinStore>,
    facts: Arc<MemoryVerificationFacts>,
}

impl MemoryMetadataStore {
    /// Create an empty repository with a fresh initial revision.
    pub fn new() -> Result<Self, MetadataError> {
        Ok(Self {
            pins: Arc::new(MemoryPinStore::default()),
            facts: Arc::default(),
            state: Arc::new(Mutex::new(MemoryState {
                records: OrdMap::new(),
                revision: fresh_revision(None)?,
                generation: 0,
                births: OrdMap::new(),
                objects: OrdMap::new(),
                roots: OrdMap::new(),
                validated: OrdSet::new(),
                payload_catalog: None,
            })),
        })
    }
}

#[async_trait]
impl MetadataStore for MemoryMetadataStore {
    async fn try_collection_lease(&self) -> Result<Option<RepositoryLease>, MetadataError> {
        Ok(Some(RepositoryLease::process_local()))
    }
    fn coordinates_payload_catalog(&self) -> bool {
        false
    }
    fn supports_metadata_records(&self) -> bool {
        true
    }

    fn supports_root_retention(&self) -> bool {
        true
    }
    fn verification_facts(&self) -> Option<Arc<dyn VerificationFacts>> {
        Some(self.facts.clone())
    }
    async fn commit_checked(
        &self,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        self.commit_impl(None, mutation).await
    }

    async fn pin_store(&self) -> Result<Arc<dyn PinStore>, MetadataError> {
        Ok(self.pins.clone())
    }
    #[tracing::instrument(
        name = "state.snapshot",
        level = "debug",
        skip_all,
        fields(backend = "memory")
    )]
    async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
        let state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        Ok(Arc::new(MemorySnapshot {
            records: state.records.clone(),
            revision: state.revision,
            generation: state.generation,
            births: state.births.clone(),
            objects: state.objects.clone(),
            roots: state.roots.clone(),
            validated: state.validated.clone(),
            payload_catalog: state.payload_catalog.clone(),
        }))
    }

    #[tracing::instrument(
        name = "state.commit",
        skip_all,
        fields(
            backend = "memory",
            expected_revision = %expected,
            objects = mutation.objects.len(),
            root_changes = mutation.roots.len(),
            collection = mutation.retained_objects.is_some()
        )
    )]
    async fn commit(
        &self,
        expected: &RepositoryRevision,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        self.commit_impl(Some(*expected), mutation).await
    }
}

impl MemoryMetadataStore {
    async fn commit_impl(
        &self,
        expected: Option<RepositoryRevision>,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        // The ephemeral store keeps its whole state in memory anyway, so
        // reading the retained source into one set costs nothing it was not
        // already paying. It happens before the state lock because reading a
        // spilled source awaits, and persistent stores page instead.
        let retained = match &mutation.retained_objects {
            Some(source) => Some(read_retained(source.as_ref()).await?),
            None => None,
        };
        let mut state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        if let Some(expected) = expected
            && state.revision != expected
        {
            return Err(MetadataError::StaleRevision {
                expected,
                actual: state.revision,
            });
        }

        mutation.reject_mixed_collection()?;

        for (index, check) in mutation.checks.iter().enumerate() {
            let matches = match check {
                MetadataCheck::Record { key, expected } => {
                    state.records.get(key) == expected.as_ref()
                }
                MetadataCheck::Root { name, expected } => {
                    state.roots.get(name) == expected.as_ref()
                }
            };
            if !matches {
                return Err(MetadataError::CheckFailed { index });
            }
        }
        if mutation.require_validated_roots {
            for change in mutation.roots.values() {
                if let RootChange::Set { target, .. } = change
                    && (!state.objects.contains_key(target)
                        || !state.validated.contains(target)
                        || !state.roots.values().any(|root| root == target))
                {
                    return Err(MetadataError::RootVerificationRequired);
                }
            }
        }

        let generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| MetadataError::Backend("metadata generation exhausted".into()))?;
        // Clone tree roots, then copy only paths touched by this mutation.
        // All fallible validation stays in a private generation so a failed
        // commit cannot modify any visible revision or held snapshot.
        let preparing = tracing::debug_span!("state.memory.prepare").entered();
        let mut next_births = state.births.clone();
        let mut next_objects = state.objects.clone();
        let mut next_roots = state.roots.clone();
        let mut next_validated = state.validated.clone();
        let mut next_payload_catalog = state.payload_catalog.clone();
        drop(preparing);
        let applying = tracing::debug_span!("state.memory.apply").entered();
        let mut objects_inserted = 0usize;
        let mut objects_removed = 0usize;
        let mut roots_changed = 0usize;
        let mut named_roots_set = Vec::new();
        let mut named_roots_removed = Vec::new();

        if let Some(retained) = retained {
            validate_retained_set(&next_objects, &next_roots, &retained)?;
            let previous = next_objects.len();
            for key in state.objects.keys().filter(|key| !retained.contains(*key)) {
                next_objects.remove(key);
                next_births.remove(key);
            }
            for key in state
                .validated
                .iter()
                .filter(|key| !retained.contains(*key))
            {
                next_validated.remove(key);
            }
            objects_removed = previous - next_objects.len();
            // Removed objects and their validation marks disappear together.
        } else {
            for verified in mutation.objects {
                let record = verified.into_record();
                match next_objects.get(record.key()) {
                    Some(existing) if existing == &record => {}
                    Some(_) => {
                        return Err(MetadataError::ImmutableConflict(record.key().clone()));
                    }
                    None => {
                        next_births.insert(record.key().clone(), generation);
                        next_objects.insert(record.key().clone(), record);
                        objects_inserted += 1;
                    }
                }
            }

            let mut newly_named = Vec::new();
            for change in mutation.roots.into_values() {
                let name = change.name().clone();
                match change {
                    RootChange::Set { target, .. } => {
                        named_roots_set.push(name.clone());
                        if next_roots.get(&name) != Some(&target) {
                            roots_changed += 1;
                            next_roots.insert(name, target.clone());
                        }
                        newly_named.push(target);
                    }
                    RootChange::Remove { .. } => {
                        named_roots_removed.push(name.clone());
                        if next_roots.contains_key(&name) {
                            next_roots.remove(&name);
                            roots_changed += 1;
                        }
                    }
                }
            }
            let newly_validated: BTreeSet<_> =
                mutation.validated_closures.iter().cloned().collect();
            validate_root_closures(
                &next_objects,
                &newly_named,
                &next_validated,
                &newly_validated,
            )?;
            for key in newly_validated {
                if !next_validated.contains(&key) {
                    next_validated.insert(key);
                }
            }
        }
        if let Some(payload_catalog) = mutation.payload_catalog {
            next_payload_catalog = Some(Arc::new(payload_catalog));
        }

        drop(applying);
        let _publishing = tracing::debug_span!("state.memory.publish").entered();
        let revision = fresh_revision(Some(state.revision))?;
        for (key, value) in mutation.records {
            match value {
                Some(value) => {
                    state.records.insert(key, value);
                }
                None => {
                    state.records.remove(&key);
                }
            }
        }
        for name in named_roots_removed {
            state
                .records
                .remove(&crate::repository::root_policy::policy_key(&name));
        }
        for name in named_roots_set {
            let key = crate::repository::root_policy::policy_key(&name);
            if state
                .records
                .get(&key)
                .is_some_and(|value| crate::repository::root_policy::last_use(value).is_some())
            {
                state
                    .records
                    .insert(key, crate::repository::root_policy::policy_value());
            }
        }
        state.revision = revision;
        state.generation = generation;
        state.births = next_births;
        state.objects = next_objects;
        state.roots = next_roots;
        state.validated = next_validated;
        state.payload_catalog = next_payload_catalog;
        Ok(CommitResult {
            revision,
            objects_inserted,
            objects_removed,
            roots_changed,
        })
    }
}

/// Validate the closure of every name this commit points somewhere new.
fn validate_root_closures(
    objects: &OrdMap<ObjectKey, ObjectRecord>,
    named: &[ObjectKey],
    validated: &OrdSet<ObjectKey>,
    newly_validated: &BTreeSet<ObjectKey>,
) -> Result<(), MetadataError> {
    let mut walk = closure::ClosureWalk::new(named, newly_validated);
    while let Some(key) = walk.next_key() {
        walk.visit(
            objects
                .get(&key)
                .map(|record| (record.links(), validated.contains(&key))),
        )?;
    }
    Ok(())
}

/// Read a retained source into one set, page by page.
async fn read_retained(source: &dyn RetainedObjects) -> Result<BTreeSet<ObjectKey>, MetadataError> {
    let mut retained = BTreeSet::new();
    let mut after = None;
    loop {
        let page = source.page(after.clone(), RETAINED_PAGE).await?;
        let Some(last) = page.last().cloned() else {
            return Ok(retained);
        };
        retained.extend(page);
        after = Some(last);
    }
}

fn validate_retained_set(
    objects: &OrdMap<ObjectKey, ObjectRecord>,
    roots: &OrdMap<RootName, ObjectKey>,
    retained: &BTreeSet<ObjectKey>,
) -> Result<(), MetadataError> {
    for key in retained {
        if !objects.contains_key(key) {
            return Err(MetadataError::InvalidRetainedSet(format!(
                "unknown retained key {key}"
            )));
        }
    }
    for target in roots.values() {
        if !retained.contains(target) {
            return Err(MetadataError::InvalidRetainedSet(format!(
                "root target {target} is not retained"
            )));
        }
    }
    for key in retained {
        let record = objects
            .get(key)
            .expect("retained keys were checked for presence");
        if let Some(missing) = record.links().iter().find(|link| !retained.contains(*link)) {
            return Err(MetadataError::InvalidRetainedSet(format!(
                "retained object {key} links to omitted {missing}"
            )));
        }
    }
    Ok(())
}

fn fresh_revision(
    different_from: Option<RepositoryRevision>,
) -> Result<RepositoryRevision, MetadataError> {
    loop {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes)
            .map_err(|error| MetadataError::RevisionEntropy(error.to_string()))?;
        let revision = RepositoryRevision::from_bytes(bytes);
        if Some(revision) != different_from {
            return Ok(revision);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use futures::StreamExt;

    use super::*;
    use crate::format::{FormatLimits, FormatRegistry};
    use crate::{BlobId, Digest, ObjectKey, PayloadReader};

    pub(super) async fn blob(bytes: &[u8]) -> VerifiedObject {
        let key = ObjectKey::blob(BlobId::new(Digest::hash(bytes)));
        let mut reader = Cursor::new(bytes.to_vec());
        FormatRegistry::builtin()
            .verify(
                &key,
                &mut reader as &mut dyn PayloadReader,
                &FormatLimits::default(),
            )
            .await
            .unwrap()
    }

    pub(super) async fn assert_snapshot_generations(store: &impl MetadataStore) {
        use futures::TryStreamExt;
        let empty = store.snapshot().await.unwrap();
        let initial_generation = empty.generation().unwrap();
        let initial_revision = empty.revision();
        drop(empty);
        let old = blob(b"snapshot birth: old").await;
        let later = blob(b"snapshot birth: later").await;
        let mut mutation = MetadataMutation::new();
        mutation.add_object(old.clone());
        store.commit(&initial_revision, mutation).await.unwrap();
        let before = store.snapshot().await.unwrap();
        let watermark = before.generation().unwrap();
        assert!(watermark > initial_generation);
        let mut mutation = MetadataMutation::new();
        mutation.add_objects([old.clone(), later.clone()]);
        store.commit(&before.revision(), mutation).await.unwrap();
        let after = store.snapshot().await.unwrap();
        assert!(after.generation().unwrap() > watermark);
        let records = after
            .objects_created_through(watermark)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            records,
            vec![old.record().clone()],
            "idempotent insertion must preserve birth"
        );
        assert_eq!(
            before
                .objects_created_through(u64::MAX)
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            records
        );
        assert!(
            after
                .objects_created_through(initial_generation)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );
        let revision = after.revision();
        drop(before);
        drop(after);
        let removed = store
            .commit(
                &revision,
                MetadataMutation::install_retained_objects(BTreeSet::new()),
            )
            .await
            .unwrap();
        let mut mutation = MetadataMutation::new();
        mutation.add_object(old);
        store.commit(&removed.revision, mutation).await.unwrap();
        let reinserted = store.snapshot().await.unwrap();
        assert!(
            reinserted
                .objects_created_through(watermark)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty(),
            "reinsertion after prune must have a new birth"
        );
        assert_eq!(
            reinserted
                .objects_created_through(u64::MAX)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            1
        );
        let generation = reinserted.generation().unwrap();
        assert!(
            store
                .commit(&initial_revision, MetadataMutation::new())
                .await
                .is_err()
        );
        assert_eq!(
            store.snapshot().await.unwrap().generation().unwrap(),
            generation,
            "failed commits must not advance generations"
        );
    }

    #[tokio::test]
    async fn memory_snapshots_isolate_indexes_across_failed_commit_and_collection() {
        let store = MemoryMetadataStore::new().unwrap();
        let object = blob(b"retained snapshot object").await;
        let key = object.record().key().clone();
        let name = RootName::try_from("snapshot/root").unwrap();
        let mut mutation = MetadataMutation::new();
        mutation.add_object(object);
        mutation.set_root(name.clone(), key.clone());
        mutation.mark_validated_closures([key.clone()]);
        mutation.set_payload_catalog(vec![1, 2, 3]);
        store
            .commit(&store.snapshot().await.unwrap().revision(), mutation)
            .await
            .unwrap();
        let held = store.snapshot().await.unwrap();

        let missing = blob(b"missing object").await;
        let rejected = blob(b"rejected object").await;
        let rejected_key = rejected.record().key().clone();
        let record_key =
            MetadataKey::new(crate::NamespaceId::try_from("rollback.v1").unwrap(), "key");
        let mut invalid = MetadataMutation::new();
        invalid.add_object(rejected);
        invalid.mark_validated_closures([rejected_key.clone()]);
        invalid.records.insert(
            record_key.clone(),
            Some(bytes::Bytes::from_static(b"rejected")),
        );
        invalid.remove_root(name.clone());
        invalid.set_root(
            RootName::try_from("invalid").unwrap(),
            missing.record().key().clone(),
        );
        invalid.set_payload_catalog(vec![9]);
        assert!(store.commit(&held.revision(), invalid).await.is_err());
        let unchanged = store.snapshot().await.unwrap();
        assert_eq!(unchanged.revision(), held.revision());
        assert_eq!(unchanged.generation().unwrap(), held.generation().unwrap());
        assert!(unchanged.object(&rejected_key).await.unwrap().is_none());
        assert_eq!(
            unchanged.validated_closures(&[rejected_key]).await.unwrap(),
            vec![false]
        );
        assert_eq!(unchanged.get(&[record_key]).await.unwrap(), vec![None]);
        assert_eq!(unchanged.root(&name).await.unwrap(), Some(key.clone()));
        assert_eq!(unchanged.payload_catalog(), Some([1, 2, 3].as_slice()));
        drop(unchanged);

        let mut remove = MetadataMutation::new();
        remove.remove_root(name.clone());
        remove.set_payload_catalog(vec![4]);
        let revision = store
            .commit(&held.revision(), remove)
            .await
            .unwrap()
            .revision;
        store
            .commit(
                &revision,
                MetadataMutation::install_retained_objects(BTreeSet::new()),
            )
            .await
            .unwrap();
        let current = store.snapshot().await.unwrap();
        assert!(current.object(&key).await.unwrap().is_none());
        assert!(current.root(&name).await.unwrap().is_none());
        assert_eq!(
            current
                .validated_closures(std::slice::from_ref(&key))
                .await
                .unwrap(),
            vec![false]
        );
        assert_eq!(current.payload_catalog(), Some([4].as_slice()));
        assert!(held.object(&key).await.unwrap().is_some());
        assert_eq!(held.root(&name).await.unwrap(), Some(key.clone()));
        assert_eq!(held.validated_closures(&[key]).await.unwrap(), vec![true]);
        assert_eq!(held.payload_catalog(), Some([1, 2, 3].as_slice()));
        assert_eq!(
            held.objects_created_through(held.generation().unwrap())
                .collect::<Vec<_>>()
                .await
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn memory_record_pages_preserve_held_generations() {
        let store = MemoryMetadataStore::new().unwrap();
        let namespace = crate::NamespaceId::try_from("pages.v1").unwrap();
        let prefix = MetadataKey::new(namespace.clone(), "record/");
        let mut expected = BTreeMap::new();
        let mut seed = MetadataMutation::new();
        for index in 0..1024 {
            let key = MetadataKey::new(namespace.clone(), format!("record/{index:04}"));
            let value = bytes::Bytes::from_static(b"initial");
            expected.insert(key.clone(), value.clone());
            seed.records.insert(key, Some(value));
        }
        // Neighbouring prefixes and namespaces must never leak into pages.
        for (ns, key) in [("pages.v1", "record0/"), ("pages.v2", "record/0000")] {
            seed.records.insert(
                MetadataKey::new(crate::NamespaceId::try_from(ns).unwrap(), key),
                Some(bytes::Bytes::new()),
            );
        }
        let mut revision = store
            .commit(&store.snapshot().await.unwrap().revision(), seed)
            .await
            .unwrap()
            .revision;
        let mut held = Vec::new();
        for generation in 0..32 {
            held.push((store.snapshot().await.unwrap(), expected.clone()));
            let mut mutation = MetadataMutation::new();
            for offset in 0..32 {
                let index = (generation * 31 + offset * 17) % 1024;
                let key = MetadataKey::new(namespace.clone(), format!("record/{index:04}"));
                let value = if offset % 2 == 0 {
                    None
                } else {
                    Some(bytes::Bytes::from(format!("generation/{generation}")))
                };
                match &value {
                    Some(value) => {
                        expected.insert(key.clone(), value.clone());
                    }
                    None => {
                        expected.remove(&key);
                    }
                }
                mutation.records.insert(key, value);
            }
            revision = store.commit(&revision, mutation).await.unwrap().revision;
        }
        held.push((store.snapshot().await.unwrap(), expected));
        for (snapshot, expected) in held {
            let mut after = None;
            let mut actual = Vec::new();
            loop {
                let page = snapshot.scan(&prefix, after.as_deref(), 17).await.unwrap();
                if page.is_empty() {
                    break;
                }
                for record in page.into_iter().take(17) {
                    after = Some(record.key.key.clone());
                    actual.push((record.key, record.value));
                }
            }
            assert_eq!(actual, expected.into_iter().collect::<Vec<_>>());
        }
    }

    #[tokio::test]
    async fn memory_snapshot_generations_preserve_exact_births() {
        assert_snapshot_generations(&MemoryMetadataStore::new().unwrap()).await;
    }

    pub(super) async fn assert_online_snapshot_excludes_future_garbage(
        state: impl MetadataStore + 'static,
    ) {
        use crate::{BlobStore, repository::Repository};
        let payloads = crate::MemoryBlobStore::new();
        let repository = Repository::new(payloads.clone(), state);
        let mutation = repository.mutation_session().await.unwrap();
        let old = mutation.stage_blob(b"held old bytes").await.unwrap();
        let old_key = old.record().key().clone();
        let old_blob = old.record().payload();
        mutation.publish_unrooted(vec![old]).await.unwrap();
        drop(mutation);
        crate::flush_repository_leases().await.unwrap();
        let hold = repository.retention_hold().await.unwrap();
        let mutation = repository.mutation_session().await.unwrap();
        let later = mutation.stage_blob(b"future garbage").await.unwrap();
        let later_key = later.record().key().clone();
        let later_blob = later.record().payload();
        mutation.publish_unrooted(vec![later]).await.unwrap();
        drop(mutation);
        crate::flush_repository_leases().await.unwrap();
        // A separate, newer staging owner must survive even though it is
        // outside the older snapshot's generation.
        let staging = repository.mutation_session().await.unwrap();
        let pending = staging.stage_blob(b"later active writer").await.unwrap();
        let pending_blob = pending.record().payload();
        let pending_key = pending.record().key().clone();
        staging.publish_unrooted(vec![pending]).await.unwrap();
        assert!(hold.snapshot().object(&later_key).await.unwrap().is_none());
        assert_eq!(
            repository.collect().await.unwrap().removed.logical_objects,
            1
        );
        assert!(hold.snapshot().object(&old_key).await.unwrap().is_some());
        assert!(payloads.has(&old_blob).await.unwrap());
        assert!(payloads.has(&pending_blob).await.unwrap());
        assert!(
            repository
                .metadata()
                .snapshot()
                .await
                .unwrap()
                .object(&pending_key)
                .await
                .unwrap()
                .is_some()
        );
        assert!(!payloads.has(&later_blob).await.unwrap());
        drop(staging);
        drop(hold);
        crate::flush_repository_leases().await.unwrap();
        assert_eq!(
            repository.collect().await.unwrap().removed.logical_objects,
            2
        );
        assert!(!payloads.has(&old_blob).await.unwrap());
        assert!(!payloads.has(&pending_blob).await.unwrap());
        crate::flush_repository_leases().await.unwrap();
    }

    #[tokio::test]
    async fn memory_online_snapshot_excludes_future_garbage() {
        assert_online_snapshot_excludes_future_garbage(MemoryMetadataStore::new().unwrap()).await;
    }

    pub(super) async fn assert_exact_collection_recovery(state: impl MetadataStore + 'static) {
        use crate::{BlobGc, BlobStore, repository::Repository};
        let payloads = crate::MemoryBlobStore::new();
        let repository = Repository::new(payloads.clone(), state);
        let mutation = repository.mutation_session().await.unwrap();
        let live = mutation.stage_blob(b"live through recovery").await.unwrap();
        let live_key = live.record().key().clone();
        let live_blob = live.record().payload();
        mutation.publish_unrooted(vec![live]).await.unwrap();
        drop(mutation);
        crate::flush_repository_leases().await.unwrap();
        let reader = repository.retention_hold().await.unwrap();
        let mutation = repository.mutation_session().await.unwrap();
        let orphan = mutation
            .stage_blob(b"interrupted emergency deletion")
            .await
            .unwrap();
        let orphan_blob = orphan.record().payload();
        mutation.publish_unrooted(vec![orphan]).await.unwrap();
        drop(mutation);
        crate::flush_repository_leases().await.unwrap();
        let ledger = repository.metadata().pin_store().await.unwrap();
        let old = ledger
            .begin_collection(ledger.inventory().await.unwrap().revision, None)
            .await
            .unwrap()
            .unwrap();
        let fence = ledger
            .begin_prune(ledger.inventory().await.unwrap().revision)
            .await
            .unwrap()
            .unwrap();
        let claim = ledger
            .claim_deletions_during_prune(
                ledger.inventory().await.unwrap().revision,
                BTreeSet::from([PinResource::Blob(orphan_blob)]),
                &old,
                &fence,
            )
            .await
            .unwrap()
            .unwrap();
        // Model a stopped emergency collector whose delete completed, but
        // whose metadata prune and ownership release did not commit.
        payloads.delete_blob(&orphan_blob).await.unwrap();
        let interrupted = ledger.inventory().await.unwrap();
        assert!(repository.recover_collection(&claim).await.is_err());
        crate::flush_repository_leases().await.unwrap();
        assert_eq!(ledger.inventory().await.unwrap(), interrupted);
        let outcome = repository.recover_collection(&old).await.unwrap();
        assert_eq!(outcome.removed.logical_objects, 1);
        crate::flush_repository_leases().await.unwrap();
        let recovered = ledger.inventory().await.unwrap();
        assert!(recovered.collector.is_none());
        assert!(recovered.logical_prune.is_none());
        assert!(recovered.deletions.is_empty());
        assert_eq!(
            recovered.pins, interrupted.pins,
            "recovery must preserve the live reader"
        );
        assert!(reader.snapshot().object(&live_key).await.unwrap().is_some());
        assert!(payloads.has(&live_blob).await.unwrap());
        let newer = ledger
            .begin_collection(recovered.revision, None)
            .await
            .unwrap()
            .unwrap();
        assert!(repository.recover_collection(&old).await.is_err());
        crate::flush_repository_leases().await.unwrap();
        assert_eq!(
            ledger.inventory().await.unwrap().collector,
            Some(newer.clone())
        );
        ledger.finish_collection(&newer).await.unwrap();
        drop(reader);
        crate::flush_repository_leases().await.unwrap();
        assert_eq!(
            repository.collect().await.unwrap().removed.logical_objects,
            1
        );
        crate::flush_repository_leases().await.unwrap();
    }

    #[tokio::test]
    async fn memory_exact_collection_recovery_preserves_live_pins() {
        assert_exact_collection_recovery(MemoryMetadataStore::new().unwrap()).await;
    }

    #[tokio::test]
    async fn object_and_root_publish_atomically() {
        let store = MemoryMetadataStore::new().unwrap();
        let before = store.snapshot().await.unwrap();
        let object = blob(b"hello").await;
        let key = object.record().key().clone();
        let name = RootName::try_from("profiles/main").unwrap();
        let mut mutation = MetadataMutation::new();
        mutation
            .add_object(object)
            .set_root(name.clone(), key.clone());
        let result = store.commit(&before.revision(), mutation).await.unwrap();

        let after = store.snapshot().await.unwrap();
        assert_eq!(after.revision(), result.revision);
        assert_eq!(after.root(&name).await.unwrap(), Some(key.clone()));
        assert!(after.object(&key).await.unwrap().is_some());
        assert!(before.root(&name).await.unwrap().is_none());
        assert!(before.object(&key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn stale_revision_cannot_overwrite_newer_state() {
        let store = MemoryMetadataStore::new().unwrap();
        let initial = store.snapshot().await.unwrap();
        store
            .commit(&initial.revision(), MetadataMutation::new())
            .await
            .unwrap();
        assert!(matches!(
            store
                .commit(&initial.revision(), MetadataMutation::new())
                .await,
            Err(MetadataError::StaleRevision { .. })
        ));
    }

    #[tokio::test]
    async fn missing_root_target_aborts_without_revision_change() {
        let store = MemoryMetadataStore::new().unwrap();
        let initial = store.snapshot().await.unwrap();
        let missing = ObjectKey::blob(BlobId::new(Digest::from([9; 32])));
        let mut mutation = MetadataMutation::new();
        mutation.set_root(RootName::try_from("missing").unwrap(), missing);
        assert!(matches!(
            store.commit(&initial.revision(), mutation).await,
            Err(MetadataError::MissingObject { .. })
        ));
        assert_eq!(
            store.snapshot().await.unwrap().revision(),
            initial.revision()
        );
    }

    #[tokio::test]
    async fn collection_installs_exact_closed_set() {
        let store = MemoryMetadataStore::new().unwrap();
        let initial = store.snapshot().await.unwrap();
        let live = blob(b"live").await;
        let live_key = live.record().key().clone();
        let orphan = blob(b"orphan").await;
        let orphan_key = orphan.record().key().clone();
        let mut publish = MetadataMutation::new();
        publish
            .add_objects([live, orphan])
            .set_root(RootName::try_from("live").unwrap(), live_key.clone());
        let published = store.commit(&initial.revision(), publish).await.unwrap();

        let retained = BTreeSet::from([live_key.clone()]);
        let collected = store
            .commit(
                &published.revision,
                MetadataMutation::install_retained_objects(retained),
            )
            .await
            .unwrap();
        assert_eq!(collected.objects_removed, 1);
        let after = store.snapshot().await.unwrap();
        assert!(after.object(&live_key).await.unwrap().is_some());
        assert!(after.object(&orphan_key).await.unwrap().is_none());
        assert_eq!(after.objects().collect::<Vec<_>>().await.len(), 1);
    }
}

#[cfg(test)]
mod benchmarks;
#[cfg(test)]
mod primitive_benchmarks;

#[cfg(test)]
mod kv_benchmarks;
#[cfg(test)]
mod record_tests;
