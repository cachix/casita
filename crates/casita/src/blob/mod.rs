//! The blob service: content-addressed storage of raw file contents.
//!
//! A blob is an opaque byte string addressed by the BLAKE3 hash of its full
//! content. The [`BlobStore`] trait is the storage interface; backends may
//! chunk and deduplicate blobs internally (see [`ChunkMeta`]), but the blob's
//! identity is always the hash of the whole content, independent of chunking.

use async_trait::async_trait;
use auto_impl::auto_impl;
use bytes::Bytes;
use futures::stream::BoxStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::digest::{BlobId, ChunkId};
use crate::error::Error;

mod chunk_index;
mod chunked;
mod chunked_reader;
mod combined;
#[cfg(test)]
pub(crate) mod crash_tests;
pub(crate) mod deletion_barrier;
pub use deletion_barrier::CommitDurability;
mod hashing_reader;
mod local_durability;
#[cfg(test)]
pub(crate) use local_durability::SYNCED_DIRECTORIES;
pub use local_durability::sync_directory;
mod memory;
mod pack;
mod pack_options;
pub use pack_options::PackOptions;
mod pinned_store;
mod publication;
pub use publication::{CatalogOutcome, PreparedCatalog};
mod repairing;

// the bucket layout and the object-store idioms every bucket-backed store
// shares: sharded paths, presence probes, idempotent deletes.
pub use chunked::{ChunkedBlobStore, DEFAULT_AVG_CHUNK_SIZE, DEFAULT_CHUNK_MEMORY_BUDGET_BYTES};
pub use combined::CombinedBlobStore;
pub use memory::MemoryBlobStore;
pub use pack::{
    DEFAULT_LOCAL_PACK_TARGET_SIZE, DEFAULT_PACK_CACHE_CAPACITY,
    DEFAULT_PACK_COMPACTION_DEAD_PERCENT, DEFAULT_PACK_TARGET_SIZE, PackReadStats,
};
pub use repairing::{BlobRepairError, RepairingBlobStore};

pub use crate::wire::{ChunkMeta, MAX_CHUNK_SIZE};

/// A physical representation failed a checksum, canonical decoding, or other
/// integrity check performed by a blob backend.
///
/// This is deliberately distinct from an ordinary I/O failure. Callers that
/// elect to recover a damaged representation from a replica may only do so
/// after this error, never after an unknown permission, timeout, or backend
/// failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BlobIntegrityError {
    /// A referenced Bao pack or its location index failed authentication.
    #[error("packed Bao metadata is invalid: {reason}")]
    BaoMetadata {
        /// Backend-specific validation detail.
        reason: String,
    },
    /// A referenced metadata page is missing or fails structural/hash checks.
    #[error("metadata page is invalid: {reason}")]
    MetadataPage {
        /// Backend-specific validation detail.
        reason: String,
    },
    /// The manifest naming a blob is malformed or otherwise invalid.
    #[error("blob manifest for {blob} is invalid: {reason}")]
    Manifest {
        /// Blob whose manifest could not be trusted.
        blob: BlobId,
        /// Backend-specific validation detail.
        reason: String,
    },
    /// A stored compressed chunk failed decompression, size, or digest checks.
    #[error("chunk {chunk} is invalid: {reason}")]
    Chunk {
        /// Chunk whose representation failed verification.
        chunk: ChunkId,
        /// Backend-specific validation detail.
        reason: String,
    },
    /// The chunks selected for a blob did not assemble to its expected digest.
    #[error("assembled blob does not match expected digest {expected}")]
    Blob {
        /// Whole-payload identity the caller expected.
        expected: BlobId,
    },
}

/// Whether an I/O error was deliberately raised for verified physical
/// corruption rather than an unrelated storage failure.
pub(crate) fn is_integrity_io_error(error: &std::io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|source| source.is::<BlobIntegrityError>())
}

/// Whether a blob-store operation failed after detecting corrupt physical
/// data. Unknown I/O and backend failures intentionally return `false`.
pub fn is_integrity_error(error: &Error) -> bool {
    matches!(error, Error::Io(error) if is_integrity_io_error(error))
}

/// Whether a payload read failed because stored bytes are missing, truncated,
/// or do not authenticate. Transient and unrelated backend failures return
/// `false`: they say nothing about the bytes on disk.
pub(crate) fn is_damaged_payload_io_error(error: &std::io::Error) -> bool {
    damaged_in_chain(error)
}

/// [`is_damaged_payload_io_error`] for blob-store operation failures.
pub(crate) fn is_damaged_payload_error(error: &Error) -> bool {
    match error {
        Error::Io(error) => is_damaged_payload_io_error(error),
        Error::Backend(error) => damaged_in_chain(error.as_ref()),
        _ => false,
    }
}

// Verified reads nest several wrappers (Bao decoding, the blob-store error,
// the chunk reader), and transparent wrappers omit themselves from the
// source chain. Walk every link rather than trusting the outermost kind.
fn damaged_in_chain(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if error.is::<BlobIntegrityError>() {
            return true;
        }
        if let Some(decode) = error.downcast_ref::<bao_tree::io::DecodeError>()
            && !matches!(decode, bao_tree::io::DecodeError::Io(_))
        {
            return true;
        }
        // `io::Error::source` skips the wrapped custom error itself; descend
        // through `get_ref` so that error is inspected too.
        let io = match error.downcast_ref::<Error>() {
            Some(Error::Io(io)) => Some(io),
            Some(_) => None,
            None => error.downcast_ref::<std::io::Error>(),
        };
        current = match io {
            Some(io) => {
                if matches!(
                    io.kind(),
                    std::io::ErrorKind::InvalidData
                        | std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::NotFound
                ) {
                    return true;
                }
                io.get_ref()
                    .map(|inner| inner as &(dyn std::error::Error + 'static))
            }
            None => error.source(),
        };
    }
    false
}

/// Content-addressed storage of blobs (raw file contents).
///
/// Blob and directory identifiers are deliberately not interchangeable:
///
/// ```compile_fail
/// use casita::experimental::{BlobStore, DirectoryId, MemoryBlobStore};
///
/// # async fn wrong_kind(store: &MemoryBlobStore, directory: DirectoryId) {
/// let _ = store.has(&directory).await;
/// # }
/// ```
#[async_trait]
#[auto_impl(&, Arc, Box)]
pub trait BlobStore: Send + Sync {
    /// Identify this backend's writes independently of concurrent operations.
    /// Wrappers should forward the scope of the backend receiving their writes.
    fn write_scope(&self) -> crate::metadata::BackendWriteScope {
        Default::default()
    }
    /// Whether a blob with this digest is present.
    async fn has(&self, digest: &BlobId) -> Result<bool, Error>;

    /// Check all physical constituents without reading file payloads.
    /// Only audited local backends enable NAR association reuse.
    async fn nar_available(&self, _digest: &BlobId) -> Result<bool, Error> {
        Ok(false)
    }

    /// Check every physical constituent of `digests` at once and describe
    /// what was checked.
    ///
    /// `None` means some constituent is missing. Otherwise the witness names
    /// the storage objects the payloads live in, so that while
    /// [`nar_witness_holds`](Self::nar_witness_holds) is true nothing has to
    /// be probed per payload again. An empty witness is a successful check
    /// that cannot be reused. The default probes each payload and reuses
    /// nothing.
    async fn nar_witness(&self, digests: &[BlobId]) -> Result<Option<Vec<u8>>, Error> {
        for digest in digests {
            if !self.nar_available(digest).await? {
                return Ok(None);
            }
        }
        Ok(Some(Vec::new()))
    }

    /// Whether every storage object a witness from
    /// [`nar_witness`](Self::nar_witness) named still exists.
    async fn nar_witness_holds(&self, _witness: &[u8]) -> Result<bool, Error> {
        Ok(false)
    }

    /// Whether each digest is present, in input order.
    ///
    /// The default loops [`has`](Self::has); backends may override it to batch
    /// the lookups.
    async fn has_batch(&self, digests: &[BlobId]) -> Result<Vec<bool>, Error> {
        let mut out = Vec::with_capacity(digests.len());
        for digest in digests {
            out.push(self.has(digest).await?);
        }
        Ok(out)
    }

    /// Open a blob for reading, or `Ok(None)` if it is absent. The returned
    /// reader supports seeking.
    async fn open_read(&self, digest: &BlobId) -> Result<Option<Box<dyn BlobReader>>, Error>;

    /// Open a complete Bao proof stream for a known plaintext size. The caller
    /// must retain the blob until the reader is dropped. Unsupported backends
    /// return an error rather than weakening verification.
    async fn open_proof(
        &self,
        _digest: &BlobId,
        _size: u64,
    ) -> Result<Option<Box<dyn BlobStreamReader>>, Error> {
        Err(Error::Msg(
            "payload backend does not support Bao streams".into(),
        ))
    }

    /// Every returned byte is authenticated against `digest` before release.
    /// Successful EOF authenticates `size`; the caller retains the blob.
    async fn open_verified(
        &self,
        digest: &BlobId,
        size: u64,
    ) -> Result<Option<Box<dyn BlobStreamReader>>, Error> {
        Ok(self
            .open_proof(digest, size)
            .await?
            .map(|reader| crate::verified::stream::decode(reader, *digest, size)))
    }

    /// Stage a same-length overwrite and return its new ID and an old-content
    /// Bao range proof. Publication must independently verify that proof and
    /// derive the new ID from the supplied replacement. The caller retains the
    /// old blob and owns the mutation's staging pin.
    async fn overwrite(
        &self,
        _digest: &BlobId,
        _size: u64,
        _offset: u64,
        _replacement: &[u8],
    ) -> Result<(BlobId, Bytes), Error> {
        Err(Error::Msg(
            "payload backend does not support partial overwrites".into(),
        ))
    }

    /// Open a reader whose physical dependencies are protected by `pin`.
    /// The caller protects the supplied exact catalog during this call. Before
    /// returning, implementations must pin every path needed by lazy reads and
    /// stop depending on mutable catalog lookups. Lazy/prefetched reads that
    /// can still supply bytes to the reader must own the pin.
    /// Backends that cannot make that guarantee must reject this operation.
    #[doc(hidden)]
    async fn open_read_scoped(
        &self,
        _digest: &BlobId,
        _pin: crate::metadata::DataPinLease,
        _catalog: Option<&[u8]>,
    ) -> Result<Option<Box<dyn BlobReader>>, Error> {
        Err(Error::Msg(
            "payload backend does not implement object-scoped reads".into(),
        ))
    }

    /// Open a Bao stream with the same physical-retention contract as
    /// `open_read_scoped`, including every lazy proof metadata dependency.
    #[doc(hidden)]
    async fn open_proof_scoped(
        &self,
        _digest: &BlobId,
        _size: u64,
        _pin: crate::metadata::DataPinLease,
        _catalog: Option<&[u8]>,
    ) -> Result<Option<Box<dyn BlobStreamReader>>, Error> {
        Err(Error::Msg(
            "payload backend does not implement scoped Bao reads".into(),
        ))
    }

    /// Open a blob as a sequential stream.
    ///
    /// The default adapts [`open_read`](Self::open_read). Streaming backends
    /// may override this to avoid buffering complete storage chunks. Such a
    /// stream can only authenticate a plain whole-content digest at EOF, so a
    /// caller must consume it to EOF and handle the final read error before
    /// treating earlier bytes as verified. Use a backend's proof-carrying API
    /// when every returned range must be verified before release.
    async fn open_stream(
        &self,
        digest: &BlobId,
    ) -> Result<Option<Box<dyn BlobStreamReader>>, Error> {
        Ok(self
            .open_read(digest)
            .await?
            .map(|reader| Box::new(reader) as Box<dyn BlobStreamReader>))
    }

    /// Open a writer for a new blob. The digest is only known once
    /// [`BlobWriter::close`] is called.
    ///
    /// This is pure storage: it takes no collection-coordination guard. Use a
    /// [`Repository`](crate::repository::Repository) mutation session when the write is
    /// intended to become part of logical repository state.
    async fn open_write(&self) -> Box<dyn BlobWriter>;

    /// Keep writes staged for a coordinated multi-object publication instead
    /// of sealing after each individual writer closes. Dropping the guard ends
    /// the batching lifetime; callers must still flush the store's
    /// [`publication`](Self::publication)
    /// before publishing references to the staged payloads.
    fn begin_batch(&self) -> BlobBatchGuard {
        BlobBatchGuard::default()
    }

    /// Attach a staging operation's online data pin. Every upload and dedup
    /// reuse must extend this pin before exposing bytes to collection. A
    /// backend without this protocol must reject online mutations, rather than
    /// substituting a repository-wide collection lock.
    fn begin_pinned_batch(
        &self,
        _pin: crate::metadata::DataPinLease,
    ) -> Result<BlobBatchGuard, Error> {
        Err(Error::Msg(
            "payload backend does not implement online write pins".into(),
        ))
    }

    /// How payloads written through this store become durable relative to
    /// logical commits. There is deliberately no default: a wrapper forwards
    /// the capability of the store receiving its writes, and a leaf store
    /// states whether each closed writer is already durable.
    fn publication(&self) -> PayloadPublication<'_>;

    /// Make every later deletion wait until `commits` has made the metadata
    /// store's acknowledged commits durable, so a payload never leaves storage
    /// while the commit that made it unreachable is still volatile. The
    /// repository calls this when it pairs this store with such a metadata
    /// store. Stores that delete must honour it and wrappers must forward it;
    /// the default suits stores that never delete.
    #[doc(hidden)]
    fn order_deletions_after(&self, _commits: CommitDurability) {}

    /// Write `data` as a blob and return its digest.
    ///
    /// Convenience over [`BlobStore::open_write`] for contents already held
    /// in memory.
    async fn put_slice(&self, data: &[u8]) -> Result<BlobId, Error> {
        let mut writer = self.open_write().await;
        writer.write_all(data).await?;
        let (digest, _size) = writer.close().await?;
        Ok(digest)
    }

    /// Read a blob's full contents, or `Ok(None)` if it is absent.
    ///
    /// Convenience over [`BlobStore::open_read`] for callers that want the
    /// whole blob in memory.
    async fn read_to_vec(&self, digest: &BlobId) -> Result<Option<Vec<u8>>, Error> {
        let Some(mut reader) = self.open_read(digest).await? else {
            return Ok(None);
        };
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await?;
        Ok(Some(buf))
    }

    /// The chunk list backing a blob.
    ///
    /// Returns `Ok(None)` if the blob is absent, or `Ok(Some(vec![]))` if the
    /// blob is present with no finer chunking (an opaque unit, or the empty
    /// blob). Chunking backends override this to expose their chunk map. The
    /// digests in the returned list are storage-internal chunk addresses, not
    /// blobs: they are not valid inputs to [`has`](Self::has) or
    /// [`open_read`](Self::open_read).
    async fn chunks(&self, digest: &BlobId) -> Result<Option<Vec<ChunkMeta>>, Error> {
        if self.has(digest).await? {
            Ok(Some(vec![]))
        } else {
            Ok(None)
        }
    }

    /// This backend's chunk-level sync capability, if it has one (see
    /// [`BlobSync`]). The default has none; sync then falls back to whole-blob
    /// streaming.
    fn as_blob_sync(&self) -> Option<&dyn BlobSync> {
        None
    }

    /// Read-only access to compressed chunks. By default a writable chunk
    /// backend also supplies this capability. Sources may expose reads without
    /// exposing destination mutation or collection operations.
    fn as_chunk_source(&self) -> Option<&dyn BlobChunkSource> {
        self.as_blob_sync().map(|sync| sync as &dyn BlobChunkSource)
    }
}

/// The durability contract a [`BlobStore`] offers repository publication.
#[derive(Clone, Copy)]
pub enum PayloadPublication<'a> {
    /// Every closed writer is durable and no physical catalog needs to be
    /// coordinated with logical state.
    Immediate,
    /// Writes are staged and must be sealed, and optionally catalogued,
    /// before logical state may reference them.
    Cataloged(&'a dyn CatalogPublication),
}

/// Sealing and catalog coordination for stores that stage writes.
///
/// No method has a default, so an adapter cannot silently lose one of them.
#[async_trait]
pub trait CatalogPublication: Send + Sync {
    /// Refresh mutable discovery metadata once before a repository mutation
    /// begins.
    #[doc(hidden)]
    async fn refresh_discovery(&self) -> Result<(), Error>;

    /// Make every payload staged by this process durable in the backing
    /// store, sealing any partial pack.
    async fn flush(&self) -> Result<(), Error>;

    /// Synchronize physical discovery metadata to one exact state snapshot.
    #[doc(hidden)]
    async fn synchronize_state_catalog(&self, catalog: Option<&[u8]>) -> Result<(), Error>;

    /// Select state-coordinated catalog publication for this store handle.
    /// Repository construction calls this before admitting payload writes;
    /// later reads and publications preserve that choice.
    #[doc(hidden)]
    fn enable_state_catalog(&self);

    /// Make payload bytes durable and return an owned catalog candidate.
    /// Keep the candidate until metadata acknowledges or rejects its commit;
    /// dropping it restores unpublished changes. Deferred work is retrieved
    /// through [`CatalogPublication::take_catalog_maintenance`] after completion.
    #[doc(hidden)]
    async fn prepare_state_commit(&self) -> Result<PreparedCatalog, Error>;

    /// Take deferred catalog work while publication still owns collection
    /// protection. The repository retains both the work and protection through
    /// a subsequent catalog commit, or discards the result before releasing it.
    #[doc(hidden)]
    fn take_catalog_maintenance(&self) -> Option<CatalogMaintenance>;
}

impl PayloadPublication<'_> {
    /// Whether writes must be sealed before publication.
    pub fn is_cataloged(self) -> bool {
        matches!(self, Self::Cataloged(_))
    }

    #[doc(hidden)]
    pub async fn refresh_discovery(self) -> Result<(), Error> {
        match self {
            Self::Immediate => Ok(()),
            Self::Cataloged(catalog) => catalog.refresh_discovery().await,
        }
    }

    /// Make every payload staged by this process durable.
    pub async fn flush(self) -> Result<(), Error> {
        match self {
            Self::Immediate => Ok(()),
            Self::Cataloged(catalog) => catalog.flush().await,
        }
    }

    #[doc(hidden)]
    pub async fn synchronize_state_catalog(self, catalog: Option<&[u8]>) -> Result<(), Error> {
        match self {
            Self::Immediate => Ok(()),
            Self::Cataloged(store) => store.synchronize_state_catalog(catalog).await,
        }
    }

    #[doc(hidden)]
    pub fn enable_state_catalog(self) {
        if let Self::Cataloged(catalog) = self {
            catalog.enable_state_catalog();
        }
    }

    #[doc(hidden)]
    pub async fn prepare_state_commit(self) -> Result<PreparedCatalog, Error> {
        match self {
            Self::Immediate => Ok(PreparedCatalog::unchanged()),
            Self::Cataloged(catalog) => catalog.prepare_state_commit().await,
        }
    }

    #[doc(hidden)]
    pub fn take_catalog_maintenance(self) -> Option<CatalogMaintenance> {
        match self {
            Self::Immediate => None,
            Self::Cataloged(catalog) => catalog.take_catalog_maintenance(),
        }
    }
}

/// Deferred catalog construction and the lifetime of its unpublished result.
/// Dropping this value discards any result that has not been published. Backends
/// must scope cleanup to this job, leaving newer maintenance jobs untouched.
#[doc(hidden)]
pub struct CatalogMaintenance {
    work: Option<futures::future::BoxFuture<'static, Result<(), Error>>>,
    discard: Option<Box<dyn FnOnce() + Send>>,
}

impl CatalogMaintenance {
    /// Construct work whose unpublished output remains owned until this value
    /// is dropped, including after the construction future has completed.
    pub fn new(
        work: impl std::future::Future<Output = Result<(), Error>> + Send + 'static,
        discard: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self {
            work: Some(Box::pin(work)),
            discard: Some(Box::new(discard)),
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), Error> {
        self.work.take().expect("maintenance runs once").await
    }
}

impl Drop for CatalogMaintenance {
    fn drop(&mut self) {
        drop(self.work.take());
        if let Some(discard) = self.discard.take() {
            discard();
        }
    }
}

/// RAII lifetime returned by [`BlobStore::begin_batch`].
#[derive(Default)]
pub struct BlobBatchGuard {
    counter: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
    pin: Option<crate::metadata::DataPinLease>,
}

impl BlobBatchGuard {
    pub(crate) fn counted(counter: std::sync::Arc<std::sync::atomic::AtomicUsize>) -> Self {
        counter.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Self {
            counter: Some(counter),
            pin: None,
        }
    }

    /// Keep staging protection until this batch and its submitted writes end.
    pub fn with_pin(mut self, pin: crate::metadata::DataPinLease) -> Self {
        self.pin = Some(pin);
        self
    }
}

impl BlobBatchGuard {
    /// Retain repair write protection without deferring physical publication
    /// for the lifetime of a read or integrity scan.
    pub(crate) fn without_batching(mut self) -> Self {
        self.end_batch();
        self
    }

    fn end_batch(&mut self) {
        if let Some(counter) = self.counter.take() {
            let previous = counter.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            debug_assert!(previous > 0, "blob batch depth underflow");
        }
    }
}

impl Drop for BlobBatchGuard {
    fn drop(&mut self) {
        self.end_batch();
    }
}

/// A physical payload store that can enumerate and delete blobs and chunks.
///
/// [`Repository::collect`](crate::repository::Repository::collect) uses this optional
/// capability after atomically pruning unreachable logical records.
#[async_trait]
#[auto_impl(&, Arc, Box)]
pub trait BlobGc: BlobStore {
    /// Delete representations retired by a successfully committed payload
    /// catalog. This raw hook requires the caller to establish safe access to
    /// retired representations. Online repository collection calls
    /// [`finish_collection_pinned`](Self::finish_collection_pinned) instead.
    /// A failure may leak storage but must preserve the committed live catalog.
    async fn finish_collection(&self, _force_reclaim: bool) -> Result<(), Error> {
        Ok(())
    }

    /// Finish deferred cleanup while preserving physical representations used
    /// by online pins, arbitrating physical paths through the supplied ledger.
    ///
    /// The four `*_pinned` methods have no defaults: ignoring the ledger would
    /// let collection delete bytes a live reader still depends on, so every
    /// store and wrapper states how it honours pins.
    async fn finish_collection_pinned(
        &self,
        force_reclaim: bool,
        pins: std::sync::Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
    ) -> Result<(), Error>;

    /// Stream every stored whole-payload identity.
    fn list_blobs(&self) -> BoxStream<'_, Result<BlobId, Error>>;
    /// Stream every stored physical chunk identity.
    fn list_chunks(&self) -> BoxStream<'_, Result<ChunkId, Error>>;

    /// Stable physical grouping key for bounded integrity scans.
    ///
    /// Stores that group many payloads in one immutable object can return
    /// that object's digest. Callers may reorder independent verification
    /// work by this hint; `None` preserves the backend's ordinary order.
    async fn physical_scan_order(&self, _digest: &BlobId) -> Result<Option<crate::Digest>, Error> {
        Ok(None)
    }

    /// Open the representation selected by a physical manifest inventory
    /// captured for an integrity scan.
    ///
    /// The default uses normal resolution. Backends with negative lookup
    /// accelerators should override this so fsck cannot skip a manifest known
    /// to be physically present.
    async fn open_read_for_fsck(
        &self,
        digest: &BlobId,
        _manifest_present: bool,
    ) -> Result<Option<Box<dyn BlobReader>>, Error> {
        self.open_read(digest).await
    }

    /// Resolve a payload's chunks using a manifest inventory captured by GC.
    ///
    /// The default ignores the hint. Object-store backends can use it to
    /// avoid a negative manifest lookup for manifest-elided payloads.
    async fn chunks_for_gc(
        &self,
        digest: &BlobId,
        _manifest_present: bool,
    ) -> Result<Option<Vec<ChunkMeta>>, Error> {
        self.chunks(digest).await
    }

    /// Delete a whole-payload manifest or bare payload.
    async fn delete_blob(&self, digest: &BlobId) -> Result<(), Error>;

    /// Delete a page under the collector's logical payload claims, returning
    /// the number admitted after physical-path pins are considered. Packed
    /// stores preserve historical representations until online reclamation.
    async fn delete_blobs_pinned(
        &self,
        digests: &[BlobId],
        pins: std::sync::Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
        before_prune: bool,
    ) -> Result<usize, Error>;
    /// Delete one physical chunk.
    async fn delete_chunk(&self, digest: &ChunkId) -> Result<(), Error>;

    /// Delete a bounded batch of physical chunks.
    ///
    /// The default preserves the single-chunk contract. Backends with shared
    /// indexes or bulk-delete APIs should override this to amortize locking and
    /// request overhead across a collection sweep page.
    async fn delete_chunks(&self, digests: &[ChunkId]) -> Result<(), Error> {
        for digest in digests {
            self.delete_chunk(digest).await?;
        }
        Ok(())
    }

    /// Delete a chunk page under the collector's claims, returning the number
    /// admitted after any additional physical-path pins are considered.
    async fn delete_chunks_pinned(
        &self,
        digests: &[ChunkId],
        pins: std::sync::Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
    ) -> Result<usize, Error>;

    /// Finish a batch of physical deletions. Packing backends use this to
    /// rewrite each affected pack once after its dead keys have been removed.
    async fn finish_deletions(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Finish deletions while forcing deferred physical garbage to be
    /// reclaimed. The default has no deferred representation, so this is
    /// equivalent to [`BlobGc::finish_deletions`].
    async fn reclaim_deletions(&self) -> Result<(), Error> {
        self.finish_deletions().await
    }

    /// Complete a sweep while retaining representations used by online pins.
    /// Emergency cleanup before logical pruning must also claim exact paths.
    async fn finish_deletions_pinned(
        &self,
        force_reclaim: bool,
        pins: std::sync::Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
        before_prune: bool,
    ) -> Result<(), Error>;

    /// Reclaim unreachable backend metadata after payload publication is
    /// complete and the caller has excluded competing readers and writers.
    ///
    /// The default has no separately allocated metadata. Packing backends use
    /// this hook to remove obsolete immutable catalog objects only when the
    /// repository can prove cross-process exclusivity.
    async fn reclaim_metadata(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Reclaim backend metadata using the shared online pin ledger. Backends
    /// must retain pinned catalog inputs and claim physical paths before I/O.
    /// `owned_claims` identifies claims this collector may reuse after proving
    /// their previous I/O has stopped; the collector releases those claims only
    /// after all of its cleanup finishes.
    async fn reclaim_metadata_pinned(
        &self,
        _pins: std::sync::Arc<dyn crate::metadata::PinStore>,
        _owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
    ) -> Result<(), Error> {
        Err(Error::from(
            "blob backend does not support pinned metadata reclamation",
        ))
    }

    /// Whether the backend has durably recorded metadata eligible for a
    /// later exclusive reclamation pass.
    async fn metadata_reclaim_due(&self) -> Result<bool, Error> {
        Ok(false)
    }
}

/// Read-only access to chunks in their stored, compressed form.
///
/// This capability does not own roots, metadata, writes, or garbage collection.
/// The caller must retain the source storage for the complete read lifetime.
/// Returned bytes remain untrusted until the receiver verifies the declared
/// plaintext size and digest. A direct remote reader can implement only this
/// interface while the destination implements [`BlobSync`].
#[async_trait]
#[auto_impl(&, Arc, Box)]
pub trait BlobChunkSource: Send + Sync {
    /// One compressed chunk, or `None` if absent. Implementations must bound
    /// network response allocations independently of the plaintext size.
    async fn get_chunk(&self, digest: &ChunkId) -> Result<Option<Bytes>, Error>;

    /// Probe physical availability in caller order without changing content.
    /// Backends can override this to avoid downloading compressed payloads.
    async fn has_chunks(&self, chunks: &[ChunkId]) -> Result<Vec<bool>, Error> {
        let mut present = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            present.push(self.get_chunk(chunk).await?.is_some());
        }
        Ok(present)
    }
}

/// Chunk-level sync: negotiate and move a blob's chunks in their stored
/// (compressed) form, and commit manifests verbatim, so only missing chunks
/// cross between repositories and nothing is re-chunked or re-compressed.
///
/// Destinations expose this via [`BlobStore::as_blob_sync`]. Sources need only
/// [`BlobChunkSource`] plus an ordered chunk map.
#[async_trait]
#[auto_impl(&, Arc, Box)]
pub trait BlobSync: BlobChunkSource {
    /// Which of `chunks` are missing here, deduplicated, preserving first-seen
    /// input order.
    async fn missing_chunks(&self, chunks: &[ChunkMeta]) -> Result<Vec<ChunkMeta>, Error>;

    /// Store one chunk delivered in compressed form, verifying it first:
    /// decompression is capped at `meta.size`, and the result must match both
    /// the declared size and digest. Requires a positive size no larger than
    /// [`MAX_CHUNK_SIZE`].
    async fn put_chunk(&self, meta: &ChunkMeta, compressed: Bytes) -> Result<(), Error>;

    /// Commit the manifest binding `blob` to `chunks`, all of which must
    /// already be stored. Verifies before it is visible: the assembled chunks
    /// must hash to `blob`, each at its declared size, and no declared size
    /// may be zero or exceed [`MAX_CHUNK_SIZE`]. Empty blobs use no chunks.
    async fn put_manifest(&self, blob: &BlobId, chunks: Vec<ChunkMeta>) -> Result<(), Error>;
}

/// A writer for a new blob. Bytes are written via [`tokio::io::AsyncWrite`];
/// [`BlobWriter::close`] finalizes and returns the content digest.
#[async_trait]
pub trait BlobWriter: tokio::io::AsyncWrite + Send + Unpin {
    /// Finalize the blob and return its digest and total size in bytes.
    ///
    /// Idempotent: calling `close` again returns the same result without
    /// re-writing.
    async fn close(&mut self) -> Result<(BlobId, u64), Error>;
}

/// A blob reader: the async, seekable content of a single blob.
#[async_trait]
pub trait BlobReader: tokio::io::AsyncRead + tokio::io::AsyncSeek + Send + Unpin + 'static {
    /// Cancel prefetch and release shared admission reservations before retaining
    /// an idle reader. Preserve its position and collection protection.
    async fn park(&mut self) {}
}

impl BlobReader for std::io::Cursor<Vec<u8>> {}
impl BlobReader for std::io::Cursor<bytes::Bytes> {}

/// A sequential blob reader. Integrity failures may be reported by the read
/// that reaches EOF; callers must not stop early when whole-blob verification
/// is required.
pub trait BlobStreamReader: tokio::io::AsyncRead + Send + Unpin + 'static {}

impl<T> BlobStreamReader for T where T: tokio::io::AsyncRead + Send + Unpin + 'static {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::MemoryBlobStore;

    #[tokio::test]
    async fn put_slice_read_to_vec_roundtrip() {
        let svc = MemoryBlobStore::new();

        let digest = svc.put_slice(b"convenient").await.unwrap();
        assert_eq!(digest, BlobId::new(blake3::hash(b"convenient").into()));
        assert_eq!(
            svc.read_to_vec(&digest).await.unwrap().as_deref(),
            Some(b"convenient".as_slice())
        );

        let missing = BlobId::new(blake3::hash(b"absent").into());
        assert_eq!(svc.read_to_vec(&missing).await.unwrap(), None);
    }
}
