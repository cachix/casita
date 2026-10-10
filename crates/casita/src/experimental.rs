//! Advanced repository composition and protocol APIs.
//!
//! Enable the `experimental` Cargo feature to use this namespace. These APIs
//! may change between releases; the supported application API is at the crate
//! root. This namespace includes the generic `Repository<PS, SS>`, backend and
//! format traits, archive framing, transport services, and dependency re-exports.
#![allow(unused_imports)]

#[cfg(feature = "native")]
pub use crate::blob::{
    BlobBatchGuard, BlobChunkSource, BlobGc, BlobIntegrityError, BlobReader, BlobRepairError,
    BlobStore, BlobStreamReader, BlobSync, BlobWriter, CatalogMaintenance, CatalogOutcome,
    CatalogPublication, ChunkedBlobStore, CombinedBlobStore, CommitDurability,
    DEFAULT_AVG_CHUNK_SIZE, DEFAULT_CHUNK_MEMORY_BUDGET_BYTES, DEFAULT_LOCAL_PACK_TARGET_SIZE,
    DEFAULT_PACK_CACHE_CAPACITY, DEFAULT_PACK_COMPACTION_DEAD_PERCENT, DEFAULT_PACK_TARGET_SIZE,
    MemoryBlobStore, PackOptions, PackReadStats, PayloadPublication, PreparedCatalog,
    RepairingBlobStore, is_integrity_error,
};
pub use crate::casitar::{
    CASITAR_MAGIC, CasitarError, CasitarFrameHeader, CasitarHeader, MAX_CASITAR_HEADER_BYTES,
    MAX_CASITAR_RECORD_BYTES, MAX_CASITAR_ROOTS,
};
#[cfg(feature = "native")]
pub use crate::casitar::{
    CasitarExportError, CasitarExportFilePolicy, CasitarExportReport, CasitarExportTarget,
    CasitarImportError, CasitarImportReport, CasitarReadFrame, CasitarReader,
    CasitarRootConflictPolicy, CasitarRootMapping, CasitarStats, CasitarStreamError,
    CasitarStreamLimits, CasitarWriter, DEFAULT_CASITAR_STREAM_BUFFER_BYTES,
    DEFAULT_MAX_CASITAR_STREAM_ITEMS,
};
#[cfg(feature = "native")]
pub use crate::collection::{
    DEFAULT_DISK_PRESSURE_USED_PERCENT, DiskPressureOutcome, DiskPressurePolicy, DiskUsage,
};
pub use crate::digest::{BlobId, ChunkId, Digest, DigestError, DirectoryId, ObjectId};
pub use crate::directory::Directory;
#[cfg(feature = "native")]
pub use crate::directory::read::read_directory_payload;
pub use crate::encode::DirectoryDecodeError;
pub use crate::error::{DirectoryError, Error, RetryDisposition};
pub use crate::format::{
    BlobFormat, DirectLinkView, DirectoryFormat, FormatError, FormatLimits, FormatRegistry,
    ObjectFormat, PayloadReader, VerificationContext, VerifiedObject,
};
#[cfg(feature = "git-fetch")]
pub use crate::git::fetch::{
    GitFetchError, GitFetchLimits, GitFetchPack, GitFetchRequest, GitFetchService,
};
#[cfg(feature = "git")]
pub use crate::git::gix_odb::{
    CasitaGixOdb, CasitaGixOdbError, CasitaGixOdbOptions, DEFAULT_GIX_BATCH_BYTES,
    DEFAULT_GIX_BATCH_OBJECTS, DEFAULT_GIX_CACHE_OBJECTS, DEFAULT_GIX_CHANNEL_CAPACITY,
    DEFAULT_GIX_STREAM_CHUNK_BYTES,
};
#[cfg(feature = "git-http")]
pub use crate::git::http::{
    GitHttpError, GitHttpEvent, GitHttpObserver, GitHttpOptions, GitHttpOutcome, GitHttpTimeout,
    serve_git_smart_http, serve_git_smart_http_with_shutdown,
};
#[cfg(feature = "native")]
pub use crate::git::repository::{
    DEFAULT_GIT_IMPORT_BUFFERED_BYTES, DEFAULT_GIT_IMPORT_CONCURRENCY,
    DEFAULT_MAX_CACHED_GIT_PACK_BYTES,
};
#[cfg(feature = "native")]
pub use crate::git::repository::{
    GitOidIndex, GitOidIndexCheckpoint, GitViewError, GitViewPublication, GitlinkCheckoutPolicy,
    checkout_git_tree, git_view_root_name, publish_git_view, read_git_view,
};
#[cfg(feature = "git")]
pub use crate::git::repository::{NativeGitImportOptions, NativeGitImportOutcome};
#[cfg(feature = "native")]
pub use crate::git::resolve_git_oid;
pub use crate::git::{
    CanonicalRefName, GIT_SHA1_BLOB_NAMESPACE, GIT_SHA1_COMMIT_NAMESPACE, GIT_SHA1_TAG_NAMESPACE,
    GIT_SHA1_TREE_NAMESPACE, GIT_SHA256_BLOB_NAMESPACE, GIT_SHA256_COMMIT_NAMESPACE,
    GIT_SHA256_TAG_NAMESPACE, GIT_SHA256_TREE_NAMESPACE, GIT_VIEW_NAMESPACE, GitError,
    GitNativeObjectFormat, GitObjectFormat, GitObjectKind, GitRefValue, GitTreeEntry, GitTreeMode,
    GitViewBody, GitViewFormat, MAX_SYMBOLIC_REF_DEPTH, git_key_parts, git_object_key,
    git_object_key_for_body, parse_git_tree,
};
#[cfg(feature = "git")]
pub use crate::importers::GitClosureImportError;
pub use crate::ipld::{
    BLAKE3_256_MULTIHASH, CASITA_LINKED_CODEC, IPLD_LINKED_NAMESPACE, IPLD_RAW_NAMESPACE, IpldCid,
    IpldError, LinkedIpld, LinkedIpldFormat, RAW_CODEC, RawIpldFormat,
};
pub use crate::linked::{
    LINKED_OBJECT_NAMESPACE, LinkedObject, LinkedObjectError, LinkedObjectFormat,
};
pub use crate::node::Node;
pub use crate::object::{
    BLOB_NAMESPACE, DIRECTORY_NAMESPACE, LogicalEncodingError, NamespaceId, NamespaceIdError,
    ObjectKey, ObjectKeyError, ObjectRecord, ObjectRecordError, RepositoryGeneration,
    RepositoryRevision, RepositoryRevisionError, RootName, RootNameError, RootRecord,
};
#[cfg(feature = "native")]
pub use crate::repository::RootRetention;

/// Encode the frozen object-key layout.
pub fn encode_object_key(key: &ObjectKey) -> Vec<u8> {
    key.encode()
}

/// Decode exactly one frozen object-key encoding.
pub fn decode_object_key(encoded: &[u8]) -> Result<ObjectKey, LogicalEncodingError> {
    ObjectKey::decode(encoded)
}

/// Construct a root record from validated parts without publishing it.
pub fn new_root_record(name: RootName, target: ObjectKey) -> RootRecord {
    RootRecord::new(name, target)
}

/// Encode the frozen root-record layout.
pub fn encode_root_record(record: &RootRecord) -> Vec<u8> {
    record.encode()
}

/// Decode exactly one frozen root-record encoding.
pub fn decode_root_record(encoded: &[u8]) -> Result<RootRecord, LogicalEncodingError> {
    RootRecord::decode(encoded)
}

/// Construct a structural object record, validating canonical link ordering.
///
/// This does not verify the payload, native identity, or claimed links.
/// Repository mutation requires the sealed result of an [`ObjectFormat`]
/// verifier, not an arbitrary record constructed here.
pub fn new_object_record(
    key: ObjectKey,
    payload: BlobId,
    payload_size: u64,
    links: Vec<ObjectKey>,
) -> Result<ObjectRecord, ObjectRecordError> {
    ObjectRecord::new(key, payload, payload_size, links)
}

/// Encode the frozen v0.2 object-record wire form.
pub fn encode_object_record(record: &ObjectRecord) -> Vec<u8> {
    record.encode()
}

/// Decode exactly one frozen v0.2 object-record wire form.
/// Decoding checks structure, not payload validity or graph completeness.
pub fn decode_object_record(encoded: &[u8]) -> Result<ObjectRecord, LogicalEncodingError> {
    ObjectRecord::decode(encoded)
}

#[cfg(feature = "native")]
pub use crate::metadata::{
    BackendWriteScope, CommitResult, DataPin, DataPinLease, EntropySource, FactsEdit, FilePinStore,
    MemoryMetadataStore, MemoryPinStore, MemoryVerificationFacts, MetadataError, MetadataMutation,
    MetadataSnapshot, MetadataStore, ObjectPinStore, PinInventory, PinResource, PinScope, PinStore,
    PinToken, RepositoryLease, RetainedObjects, RootChange, TursoMetadataStore, VerificationFacts,
    flush_repository_leases,
};
#[cfg(feature = "s3")]
pub use crate::metadata::{Wal3MetadataStore, Wal3ReadStats, Wal3RepositoryHold};
#[cfg(feature = "oci")]
pub use crate::oci::{OciImportError, OciImportLimits, OciImportReport, OciRootfsLimits};
pub use crate::path::{PathComponent, PathComponentError, SymlinkTarget, SymlinkTargetError};
#[cfg(feature = "native")]
pub use crate::repository::{
    ClosureStatus, CollectionOutcome, CollectionPreview, ConditionalPublishResult, FsckDisposition,
    FsckIssue, FsckIssueKind, FsckRepairAction, FsckRepairActionKind, FsckRepairActionStatus,
    FsckRepairFinding, FsckRepairFindingKind, FsckRepairReport, FsckReport,
    LogicalCollectionOutcome, LogicalCollectionPreview, MutationSession, OwnedRetentionHold,
    Repository, RepositoryError, RepositoryErrorCategory, RepositoryProfile, RetentionHold,
    RootExpectation, StagedObject,
};
#[cfg(feature = "native")]
pub use crate::spill::{SpillLimits, SpillMetrics};
#[cfg(feature = "native")]
pub use crate::sqlite::TursoDb;
#[cfg(feature = "native")]
pub use crate::sync::sliced::{
    BlobSliceSources, DISCOVERY_CHUNK_BYTES, DISCOVERY_SAMPLE, ENCODE_WINDOW, MAX_LITERAL_SEGMENT,
    MAX_SLICE_SOURCES, SLICED_MAGIC, SliceError, SliceIndex, SliceSources, SliceStats,
    decode_sliced, encode_literal_stream, encode_sliced, encode_sliced_stream,
};
#[cfg(feature = "ssh")]
pub use crate::sync::ssh::{
    SshEndpoint, SshEndpointError, SshTransferSource, SshTransportError,
    connect_transfer_stdio_source, serve_transfer_stdio,
};
#[cfg(feature = "native")]
pub use crate::sync::wire::TransferWireError;
#[cfg(feature = "native")]
pub use crate::sync::{
    DestinationRoot, HeldSession, MAX_TRANSFER_DISCOVERY_BATCH_OBJECTS, ObjectChunks,
    ObjectRequest, PathProof, PathProofEntry, PathProofResponse, PathTransferResult,
    RequestedStatus, SliceBasePair, SliceBases, SlicedReceipt, SplitTransferSession,
    TransferDiscovery, TransferError, TransferObjectBatch, TransferOptions, TransferPayloadReader,
    TransferProgress, TransferReadSession, TransferRequest, TransferResult, TransferSelection,
    TransferSource, transfer, transfer_path,
};
#[cfg(feature = "native")]
pub use crate::tar::{TarImportError, TarImportLimits, TarImportReport};
#[cfg(feature = "native")]
pub use crate::wire::{ChunkMeta, MAX_CHUNK_SIZE};
pub use async_trait::async_trait;
pub use blake3;
#[cfg(feature = "s3")]
pub use chroma_config as wal3_config;
#[cfg(feature = "s3")]
pub use chroma_storage as wal3_storage;
#[cfg(feature = "native")]
pub use object_store;

/// Executable contracts for custom storage backends.
#[cfg(feature = "native")]
pub mod conformance {
    pub use crate::conformance::*;
}

/// Standalone verified-range primitives.
#[cfg(feature = "native")]
pub mod verified {
    pub use crate::verified::*;
}
