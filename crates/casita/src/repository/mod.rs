//! Generic repository facade over physical payloads and revisioned state.
//!
//! Durable pins retain staged payloads and held snapshots during collection.
//! Collector ownership serializes maintenance; pin admission and deletion
//! claims coordinate data lifetimes across independent repository handles.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::blob::{BlobReader, BlobStore, is_integrity_error, is_integrity_io_error};
use crate::coordination::{ExclusiveLease, FsCoordination};
use crate::directory::Directory;
use crate::filesystem::FilesystemEntry;
use crate::filesystem::checkout::DirectorySource;
use crate::format::{
    BlobFormat, DirectLinkView, DirectoryFormat, FormatError, FormatLimits, FormatRegistry,
    PayloadReader, VerificationContext, VerifiedObject,
};
use crate::metadata::{
    CommitResult, MetadataError, MetadataMutation, MetadataSnapshot, MetadataStore, RootChange,
};
use crate::object::{ObjectKey, ObjectRecord, RootName};
use crate::path::{PathComponent, SymlinkTarget};
use crate::spill::{
    FrozenSpillSet, SpillArea, SpillLimits, SpillMetrics, SpillSet, TraversalQueue,
};
use crate::{BlobGc, BlobId, ChunkId, DirectoryId, Node};

// Implementation modules share only repository-scoped state. Public names
// remain re-exported here for the application and experimental facades.
mod closure;
mod collection;
mod error;
mod filesystem;
mod integrity;
mod mutation;
mod open;
mod profile;
mod read;
mod retention;
pub(crate) mod root_policy;

pub(crate) use closure::ClosureAudit;
pub use closure::ClosureStatus;
use closure::{
    BlobPayloadReader, CLOSURE_FRONTIER, ClosureVerifier, RepositoryDirectLinkView,
    overlay_records, verify_closure_against, verify_closure_with,
};
use collection::METADATA_RECLAIM_INTERVAL;
pub use collection::{
    CollectionOutcome, CollectionPreview, LogicalCollectionOutcome, LogicalCollectionPreview,
};
use error::is_storage_full;
pub use error::{RepositoryError, RepositoryErrorCategory};
pub(crate) use filesystem::stage_filesystem_file;
pub use integrity::{
    FsckDisposition, FsckIssue, FsckIssueKind, FsckRepairAction, FsckRepairActionKind,
    FsckRepairActionStatus, FsckRepairFinding, FsckRepairFindingKind, FsckRepairReport, FsckReport,
};
#[cfg(feature = "git")]
pub(crate) use mutation::PendingGitWitnesses;
#[cfg(test)]
use mutation::PublicationTimer;
pub use mutation::{ConditionalPublishResult, MutationSession, RootExpectation, StagedObject};
#[cfg(test)]
pub(crate) use mutation::{PUBLICATION_PHASES, PublicationProfile};
use open::{LocalMutationStart, MutationStart};
pub use profile::RepositoryProfile;
#[cfg(test)]
pub(crate) use read::ObjectReadProfile;
use retention::{DataProtection, HeldReader, pin_metadata_snapshot_kind};
pub use retention::{OwnedRetentionHold, RetentionHold};
pub use root_policy::RootRetention;

mod collection_timing;
#[cfg(test)]
mod mutation_catalog_tests;
mod publication;
#[cfg(test)]
mod publication_retry_tests;
pub(crate) use collection_timing::CollectionPhase;
use publication::Publication;

/// Generic repository over one physical payload store and one logical state
/// store.
pub struct Repository<PS, SS> {
    payloads: Arc<PS>,
    state: Arc<SS>,
    formats: FormatRegistry,
    limits: FormatLimits,
    coordination: Arc<AsyncMutex<()>>,
    publication: Publication,
    #[cfg(test)]
    publication_profile: Arc<std::sync::Mutex<PublicationProfile>>,
    /// Deployment policy: coordination, spill placement, local accelerators
    /// and maintenance. Shared by clones; a builder that edits it copies it
    /// for that handle only.
    profile: Arc<RepositoryProfile>,
    /// NAR associations, present whenever the state backend keeps
    /// verification facts. Derived from the backend, not from the profile.
    pub(crate) nar_store: Option<Arc<crate::nar::store::NarStore>>,
    staging_identity: Arc<()>,
}

impl<PS, SS> Clone for Repository<PS, SS> {
    fn clone(&self) -> Self {
        Self {
            payloads: self.payloads.clone(),
            state: self.state.clone(),
            formats: self.formats.clone(),
            limits: self.limits.clone(),
            coordination: self.coordination.clone(),
            publication: self.publication.clone(),
            #[cfg(test)]
            publication_profile: self.publication_profile.clone(),
            profile: self.profile.clone(),
            nar_store: self.nar_store.clone(),
            staging_identity: self.staging_identity.clone(),
        }
    }
}

impl<PS, SS> Repository<PS, SS> {
    /// Flush payload writes, await dropped leases, and compact transient metadata.
    /// Release snapshots before the final flush; a checkpoint blocked by readers
    /// returns a retryable busy error. Await cleanup before stopping the runtime.
    pub async fn flush(&self) -> Result<(), RepositoryError>
    where
        PS: BlobStore,
        SS: MetadataStore,
    {
        self.payloads().publication().flush().await?;
        crate::metadata::flush_repository_leases().await?;
        self.metadata().compact_transient_state().await?;
        Ok(())
    }

    /// Erase backend types without rebuilding the repository's coordination,
    /// publication, retention, or cache state. Useful for applications sharing
    /// one content domain between local stores, builders, and transfer services.
    #[cfg(feature = "experimental")]
    pub fn into_erased(self) -> Repository<Arc<dyn BlobGc>, Arc<dyn MetadataStore>>
    where
        PS: BlobGc + 'static,
        SS: MetadataStore + 'static,
    {
        self.into_builtin()
    }

    /// Erase built-in backend types while preserving the exact coordination,
    /// admission, staging, and cache state established by the constructor.
    pub(crate) fn into_builtin(self) -> Repository<Arc<dyn BlobGc>, Arc<dyn MetadataStore>>
    where
        PS: BlobGc + 'static,
        SS: MetadataStore + 'static,
    {
        Repository {
            payloads: Arc::new(self.payloads as Arc<dyn BlobGc>),
            state: Arc::new(self.state as Arc<dyn MetadataStore>),
            formats: self.formats,
            limits: self.limits,
            coordination: self.coordination,
            publication: self.publication,
            #[cfg(test)]
            publication_profile: self.publication_profile,
            profile: self.profile,
            nar_store: self.nar_store,
            staging_identity: self.staging_identity,
        }
    }

    /// Compose physical payloads and revisioned metadata with all current
    /// production formats through v0.5.
    ///
    /// The metadata backend must coordinate retention with the actual payload
    /// storage. Merely pairing independent stores does not establish a shared
    /// collection boundary across processes or services.
    /// Publication capabilities are selected once here, before payload writes.
    /// Backends must own their dependencies (`'static`) so an admitted publication
    /// can finish safely after its caller is cancelled. They need not be `Clone`.
    pub fn new(payloads: PS, metadata: SS) -> Self
    where
        PS: BlobStore + 'static,
        SS: MetadataStore + 'static,
    {
        Self::with_formats(
            payloads,
            metadata,
            FormatRegistry::builtin(),
            FormatLimits::default(),
        )
    }

    /// Compose a repository with an explicit immutable format registry and
    /// deployment limits.
    pub fn with_formats(
        payloads: PS,
        metadata: SS,
        formats: FormatRegistry,
        limits: FormatLimits,
    ) -> Self
    where
        PS: BlobStore + 'static,
        SS: MetadataStore + 'static,
    {
        let payloads = Arc::new(payloads);
        let metadata = Arc::new(metadata);
        // A payload deletion must not outrun the commit that allowed it.
        if let Some(commits) = metadata.commit_durability() {
            payloads.order_deletions_after(commits);
        }
        let publication = Publication::new(payloads.clone(), metadata.clone());
        let nar_store = metadata
            .verification_facts()
            .map(crate::nar::store::NarStore::new);
        Self {
            payloads,
            state: metadata,
            formats,
            limits,
            coordination: Arc::new(AsyncMutex::new(())),
            publication,
            #[cfg(test)]
            publication_profile: Arc::default(),
            profile: Arc::new(RepositoryProfile::generic()),
            nar_store,
            staging_identity: Arc::new(()),
        }
    }

    /// Extend retention/collection ownership across every process opening the
    /// same local repository root.
    ///
    /// Shorthand for editing this handle's profile with
    /// [`RepositoryProfile::with_fs_coordination`].
    pub fn with_fs_coordination(self, root: impl AsRef<Path>) -> Self {
        self.edit_profile(|profile| profile.with_fs_coordination(root))
    }

    /// Bound the memory and temporary bytes a traversal may use before, and
    /// after, it spills to local storage.
    pub fn with_spill_limits(self, limits: SpillLimits) -> Self {
        self.edit_profile(|profile| profile.with_spill_limits(limits))
    }

    /// Replace this handle's profile with an edited copy, keeping any
    /// maintenance hook already installed. Other clones are unaffected.
    fn edit_profile(mut self, edit: impl FnOnce(RepositoryProfile) -> RepositoryProfile) -> Self {
        self.profile = Arc::new(edit(RepositoryProfile::clone(&self.profile)));
        self
    }

    /// Active traversal spill bounds.
    pub fn spill_limits(&self) -> SpillLimits {
        self.profile.spill_limits()
    }

    /// Deployment policy this handle applies.
    pub fn profile(&self) -> &RepositoryProfile {
        &self.profile
    }

    /// Where this repository's traversals spill, and how far they may grow.
    pub(crate) fn spill_area(&self) -> SpillArea {
        self.profile.spill_area()
    }

    /// Physical payload backend.
    pub fn payloads(&self) -> &PS {
        &self.payloads
    }

    /// Revisioned metadata backend for object records, roots, and catalogs.
    pub fn metadata(&self) -> &SS {
        &self.state
    }

    /// Registered deterministic object formats.
    pub fn formats(&self) -> &FormatRegistry {
        &self.formats
    }

    /// Active deployment and hostile-input limits.
    pub fn limits(&self) -> &FormatLimits {
        &self.limits
    }
}

impl<PS, SS> Repository<PS, SS>
where
    PS: BlobGc + 'static,
    SS: MetadataStore + 'static,
{
    /// Apply a deployment profile, replacing the one this handle had.
    ///
    /// This is how a composition of caller-supplied backends takes on the
    /// behaviour of a built-in profile: `Repository::new(payloads, metadata)
    /// .with_profile(RepositoryProfile::local(root).await?)` coordinates,
    /// spills and maintains itself like [`Repository::local`] does. Existing
    /// clones keep their previous profile.
    pub fn with_profile(mut self, mut profile: RepositoryProfile) -> Self {
        // The maintenance hook collects through a clone of this handle. That
        // clone carries the profile without the hook, so collecting cannot
        // recurse into the hook, nor can the hook keep itself alive.
        profile.mutation_start = None;
        self.profile = Arc::new(profile.clone());
        profile.install_mutation_start(&self);
        self.profile = Arc::new(profile);
        self
    }
}

impl<SS> Repository<crate::ChunkedBlobStore, SS>
where
    SS: MetadataStore,
{
    /// Set the chunk upload window per new blob writer (default 32).
    /// Existing repository clones retain their setting; the chunk memory budget
    /// and storage coordination remain shared. One runs chunk uploads serially.
    pub fn with_chunk_upload_concurrency(mut self, concurrency: std::num::NonZeroUsize) -> Self {
        Arc::make_mut(&mut self.payloads).chunk_upload_concurrency = concurrency;
        self
    }
}

#[cfg(test)]
mod closure_benchmarks;
#[cfg(all(test, unix))]
mod collection_benchmark;
#[cfg(test)]
mod fsck_benchmark;

#[cfg(test)]
mod catalog_sync_tests;
#[cfg(test)]
mod tests;
