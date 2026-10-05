//! Verification boundary for generic logical object formats.
//!
//! A format proves a namespace's native identity, canonical payload, and exact
//! forward links. Repository mutation accepts only the sealed
//! [`VerifiedObject`] produced through [`VerificationContext`].

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Arc;

use async_trait::async_trait;

use crate::digest::{BlobId, Digest};
use crate::directory::Directory;
use crate::node::Node;
use crate::object::{
    BLOB_NAMESPACE, DIRECTORY_NAMESPACE, NamespaceId, ObjectKey, ObjectRecord, ObjectRecordError,
};

/// Deployment limits for deterministic format verification.
#[derive(Debug, Clone)]
pub struct FormatLimits {
    /// Largest complete payload accepted by this repository.
    pub max_payload_bytes: u64,
    /// Largest metadata payload a verifier may materialize.
    pub max_metadata_bytes: u64,
    /// Largest number of canonical forward links one record may carry.
    pub max_links_per_object: usize,
    /// Largest number of direct entries in one canonical directory.
    pub max_directory_entries: usize,
    /// Largest number of records accepted by one logical mutation batch.
    pub max_batch_objects: usize,
    /// Largest number of root changes accepted by one logical commit.
    pub max_root_changes: usize,
    /// Largest graph traversal before the operation must spill or fail.
    pub max_traversal_objects: usize,
    /// Scratch-buffer size used while streaming opaque payloads.
    pub read_buffer_bytes: usize,
    /// Aggregate plaintext chunk/buffer bytes admitted concurrently by one
    /// transfer operation.
    pub max_transfer_in_flight_bytes: usize,
}

impl Default for FormatLimits {
    fn default() -> Self {
        Self {
            max_payload_bytes: u64::MAX,
            max_metadata_bytes: 256 * 1024 * 1024,
            max_links_per_object: 1_000_000,
            max_directory_entries: 1_000_000,
            max_batch_objects: 4_096,
            max_root_changes: 1_024,
            max_traversal_objects: 10_000_000,
            read_buffer_bytes: 64 * 1024,
            max_transfer_in_flight_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Minimal async reader used by portable format verification.
///
/// The trait is independent of a particular async runtime. Native builds
/// provide a blanket implementation for Tokio readers, including Casita's
/// existing [`BlobReader`](crate::BlobReader).
#[async_trait]
pub trait PayloadReader: Send {
    /// Exact number of payload bytes when the source can provide it cheaply.
    ///
    /// Formats whose native framing places a length before the payload (Git,
    /// for example) use this to remain streaming. Implementations may return
    /// `None`; those formats then reject the source instead of buffering an
    /// unbounded payload merely to discover its length.
    fn exact_len(&self) -> Option<u64> {
        None
    }

    /// Read some payload bytes, returning zero only at EOF.
    async fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize>;
}

#[cfg(feature = "native")]
#[async_trait]
impl<T> PayloadReader for T
where
    T: tokio::io::AsyncRead + Unpin + Send,
{
    async fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        tokio::io::AsyncReadExt::read(self, buffer).await
    }
}

/// Errors at the object-format verification boundary.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FormatError {
    /// The selected verifier does not own the key's namespace.
    #[error("format `{format}` cannot verify key in namespace `{actual}`")]
    NamespaceMismatch {
        /// Namespace implemented by the verifier.
        format: NamespaceId,
        /// Namespace carried by the key.
        actual: NamespaceId,
    },
    /// The namespace requires a digest-width native identifier.
    #[error("namespace `{namespace}` requires a 32-byte native identifier, got {actual}")]
    NativeIdLength {
        /// Namespace imposing the requirement.
        namespace: NamespaceId,
        /// Observed width.
        actual: usize,
    },
    /// Complete payload bytes disagree with the native identifier.
    #[error("native identity mismatch for {key}: expected {expected}, observed {actual}")]
    NativeIdentityMismatch {
        /// Logical key being verified.
        key: ObjectKey,
        /// Digest encoded in the native identifier.
        expected: Digest,
        /// Digest of the complete payload bytes.
        actual: Digest,
    },
    /// A verifier tried to accept a partially consumed payload.
    #[error("format verifier did not consume the complete payload for {0}")]
    PayloadNotFullyConsumed(ObjectKey),
    /// A payload or counter exceeded `u64`.
    #[error("payload size overflows u64")]
    PayloadSizeOverflow,
    /// The complete payload exceeded the repository's configured bound.
    #[error("payload exceeds configured limit of {limit} bytes")]
    PayloadLimit {
        /// Active deployment limit.
        limit: u64,
    },
    /// A metadata payload exceeded the configured materialization bound.
    #[error("metadata payload exceeds configured limit of {limit} bytes")]
    MetadataLimit {
        /// Active deployment limit.
        limit: u64,
    },
    /// A verifier produced too many canonical forward links.
    #[error("object has {actual} forward links, over the configured limit of {limit}")]
    LinkLimit {
        /// Observed canonical link count.
        actual: usize,
        /// Active deployment limit.
        limit: usize,
    },
    /// A canonical directory has too many direct entries.
    #[error("directory has {actual} entries, over the configured limit of {limit}")]
    DirectoryEntryLimit {
        /// Observed direct-entry count.
        actual: usize,
        /// Active deployment limit.
        limit: usize,
    },
    /// The payload is not canonical or otherwise violates its namespace.
    #[error("invalid payload for namespace `{namespace}`: {message}")]
    InvalidPayload {
        /// Namespace whose rules rejected it.
        namespace: NamespaceId,
        /// Precise decoder/verifier explanation.
        message: String,
    },
    /// The verifier returned malformed link metadata.
    #[error(transparent)]
    Record(#[from] ObjectRecordError),
    /// A direct target required for relation verification is absent.
    #[error("direct link {target} from {object} is missing")]
    MissingDirectLink {
        /// Object whose relation is being checked.
        object: ObjectKey,
        /// Missing declared target.
        target: ObjectKey,
    },
    /// A direct target's record or payload no longer matches its key.
    #[error("direct link {target} from {object} is invalid: {message}")]
    InvalidDirectLink {
        /// Object whose relation is being checked.
        object: ObjectKey,
        /// Invalid declared target.
        target: ObjectKey,
        /// Relation or verification failure.
        message: String,
    },
    /// A file entry's declared byte length disagrees with its direct record.
    #[error(
        "file link {target} from {object} declares {declared} bytes but the record has {actual}"
    )]
    ByteLengthMismatch {
        /// Parent directory key.
        object: ObjectKey,
        /// File object key.
        target: ObjectKey,
        /// Length in the directory entry.
        declared: u64,
        /// Verified record payload length.
        actual: u64,
    },
    /// A directory entry's descendant count disagrees with its direct payload.
    #[error(
        "directory link {target} from {object} declares {declared} descendants but the child has {actual}"
    )]
    DescendantCountMismatch {
        /// Parent directory key.
        object: ObjectKey,
        /// Child directory key.
        target: ObjectKey,
        /// Count in the parent entry.
        declared: u64,
        /// Count derived from the child payload.
        actual: u64,
    },
    /// Re-verification produced a record different from durable state.
    #[error("stored object record for {0} disagrees with its verified payload")]
    RecordMismatch(ObjectKey),
    /// No verifier is registered for a namespace.
    #[error("unsupported object namespace `{0}`")]
    UnsupportedNamespace(NamespaceId),
    /// Two format implementations claim one namespace.
    #[error("duplicate object format for namespace `{0}`")]
    DuplicateNamespace(NamespaceId),
    /// Reading payload bytes failed.
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Repository-controlled streaming context handed to an object verifier.
///
/// It hashes and counts every byte the format consumes. The only public route
/// to a [`VerifiedObject`] is [`finish`](Self::finish), which requires EOF and
/// therefore binds the record to the complete payload.
pub struct VerificationContext<'a> {
    key: &'a ObjectKey,
    reader: &'a mut dyn PayloadReader,
    hasher: blake3::Hasher,
    payload_size: u64,
    eof: bool,
}

impl<'a> VerificationContext<'a> {
    /// Create a verification context for an exact logical key and payload.
    ///
    /// Repositories use this constructor immediately before invoking the
    /// selected registered format. Exposing it also lets independent format
    /// implementations be tested without granting access to state commits.
    pub fn new(key: &'a ObjectKey, reader: &'a mut dyn PayloadReader) -> Self {
        Self {
            key,
            reader,
            hasher: blake3::Hasher::new(),
            payload_size: 0,
            eof: false,
        }
    }

    /// The exact key the repository asked this format to verify.
    pub fn key(&self) -> &ObjectKey {
        self.key
    }

    /// Exact complete payload length supplied by the underlying reader.
    pub fn exact_len(&self) -> Option<u64> {
        self.reader.exact_len()
    }

    /// Read and account for payload bytes.
    pub async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, FormatError> {
        if self.eof {
            return Ok(0);
        }
        let read = self.reader.read(buffer).await?;
        if read == 0 {
            self.eof = true;
            return Ok(0);
        }
        self.hasher.update(&buffer[..read]);
        self.payload_size = self
            .payload_size
            .checked_add(read as u64)
            .ok_or(FormatError::PayloadSizeOverflow)?;
        Ok(read)
    }

    /// Materialize the payload while enforcing a bound before growing the
    /// returned allocation beyond it.
    pub async fn read_to_end_bounded(&mut self, limit: u64) -> Result<Vec<u8>, FormatError> {
        let mut output = Vec::new();
        // This future is nested through directory verification, publication,
        // and callers' build graphs. Keep the scratch space out of each
        // enclosing future and its poll stack frame.
        let mut buffer = vec![0u8; 64 * 1024];
        loop {
            let read = self.read(&mut buffer).await?;
            if read == 0 {
                return Ok(output);
            }
            let new_len = (output.len() as u64)
                .checked_add(read as u64)
                .ok_or(FormatError::PayloadSizeOverflow)?;
            if new_len > limit {
                return Err(FormatError::MetadataLimit { limit });
            }
            output.extend_from_slice(&buffer[..read]);
        }
    }

    /// Digest of the bytes consumed so far. It is authoritative only after
    /// [`finish`](Self::finish) observes EOF.
    pub fn observed_digest(&self) -> Digest {
        self.hasher.clone().finalize().into()
    }

    /// Number of payload bytes consumed so far.
    pub fn observed_size(&self) -> u64 {
        self.payload_size
    }

    /// Seal a canonical set of verified links into repository input.
    pub fn finish(self, links: Vec<ObjectKey>) -> Result<VerifiedObject, FormatError> {
        if !self.eof {
            return Err(FormatError::PayloadNotFullyConsumed(self.key.clone()));
        }
        let payload = BlobId::new(self.hasher.finalize().into());
        let record = ObjectRecord::new(self.key.clone(), payload, self.payload_size, links)?;
        Ok(VerifiedObject { record })
    }
}

/// Sealed output of one registered format verifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedObject {
    record: ObjectRecord,
}

impl VerifiedObject {
    /// Inspect the verified immutable record.
    pub fn record(&self) -> &ObjectRecord {
        &self.record
    }

    /// Consume the seal after repository policy has accepted it.
    pub fn into_record(self) -> ObjectRecord {
        self.record
    }
}

/// Restricted access to records and verified payloads of declared direct links.
#[async_trait]
pub trait DirectLinkView: Send + Sync {
    /// Read a direct target's immutable record.
    async fn record(&self, key: &ObjectKey) -> Result<Option<ObjectRecord>, FormatError>;

    /// Open the verified plaintext payload of a direct target.
    async fn open_payload(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<Box<dyn PayloadReader>>, FormatError>;
}

/// Deterministic verifier for one exact object namespace.
#[async_trait]
pub trait ObjectFormat: Send + Sync {
    /// Namespace whose identity and payload rules this implementation owns.
    fn namespace(&self) -> &NamespaceId;

    /// Verify native identity, canonical payload, and exact forward links.
    async fn verify(
        &self,
        context: VerificationContext<'_>,
        limits: &FormatLimits,
    ) -> Result<VerifiedObject, FormatError>;

    /// Verify relations whose evidence includes linked objects.
    async fn verify_links(
        &self,
        context: VerificationContext<'_>,
        object: &ObjectRecord,
        _direct_links: &dyn DirectLinkView,
        limits: &FormatLimits,
    ) -> Result<(), FormatError> {
        let verified = self.verify(context, limits).await?;
        if verified.record() != object {
            return Err(FormatError::RecordMismatch(object.key().clone()));
        }
        Ok(())
    }
}

/// Immutable namespace-to-verifier registry.
#[derive(Clone)]
pub struct FormatRegistry {
    formats: Arc<BTreeMap<NamespaceId, Arc<dyn ObjectFormat>>>,
    // A namespace spelling cannot identify its verifier's relational rules, so
    // only the built-in factory may vouch for private construction proofs.
    #[cfg(feature = "native")]
    builtin: bool,
}

impl FormatRegistry {
    /// Build a registry, rejecting ambiguous namespace ownership.
    pub fn new(
        formats: impl IntoIterator<Item = Arc<dyn ObjectFormat>>,
    ) -> Result<Self, FormatError> {
        let mut by_namespace = BTreeMap::new();
        for format in formats {
            let namespace = format.namespace().clone();
            if by_namespace.insert(namespace.clone(), format).is_some() {
                return Err(FormatError::DuplicateNamespace(namespace));
            }
        }
        Ok(Self {
            formats: Arc::new(by_namespace),
            #[cfg(feature = "native")]
            builtin: false,
        })
    }

    /// Registry containing all built-in object formats.
    ///
    /// New built-in formats are included as they become available. Individual
    /// namespace versions define each format's identity and encoding rules.
    pub fn builtin() -> Self {
        let mut formats = vec![
            Arc::new(BlobFormat::default()) as Arc<dyn ObjectFormat>,
            Arc::new(DirectoryFormat::default()) as Arc<dyn ObjectFormat>,
        ];
        formats.extend(crate::ipld::formats());
        formats.extend(crate::git::formats());
        formats.push(Arc::new(crate::LinkedObjectFormat::default()));
        #[cfg_attr(not(feature = "native"), allow(unused_mut))]
        let mut registry = Self::new(formats).expect("the built-in namespaces are distinct");
        #[cfg(feature = "native")]
        {
            registry.builtin = true;
        }
        registry
    }

    /// Whether every verifier is the built-in one for its namespace.
    ///
    /// Importers prove the built-in rules while constructing a graph. A
    /// registry assembled by [`Self::new`] may add relations under the same
    /// namespace spellings, so its closures must be audited normally.
    #[cfg(feature = "native")]
    pub(crate) fn is_builtin(&self) -> bool {
        self.builtin
    }

    /// Whether a present record alone proves its complete closure.
    ///
    /// A built-in raw blob has no links and its identity is the payload digest
    /// the record binds, so its closure is the record and that payload. A
    /// built-in Git blob has no links either, and its record was admitted only
    /// after that immutable payload hashed to its object ID: checking its
    /// closure would repeat exactly that check. Payloads outlive their records
    /// under the same rules as a stored witness, so the repository derives
    /// this instead of storing one witness per blob.
    #[cfg(feature = "native")]
    pub(crate) fn intrinsically_complete(&self, record: &ObjectRecord) -> bool {
        let key = record.key();
        self.builtin
            && record.links().is_empty()
            && ((key.namespace().as_str() == BLOB_NAMESPACE
                && key.native_id() == record.payload().digest().as_bytes().as_slice())
                || is_git_blob(key))
    }

    /// Whether any present record under this key is intrinsically complete,
    /// so presence alone settles it without reading the record's links.
    #[cfg(feature = "git")]
    pub(crate) fn complete_when_present(&self, key: &ObjectKey) -> bool {
        self.builtin && is_git_blob(key)
    }

    /// Resolve the verifier for an exact namespace.
    pub fn get(&self, namespace: &NamespaceId) -> Option<&Arc<dyn ObjectFormat>> {
        self.formats.get(namespace)
    }

    /// Verify one key and complete payload using the selected format.
    pub async fn verify(
        &self,
        key: &ObjectKey,
        reader: &mut dyn PayloadReader,
        limits: &FormatLimits,
    ) -> Result<VerifiedObject, FormatError> {
        let format = self
            .get(key.namespace())
            .ok_or_else(|| FormatError::UnsupportedNamespace(key.namespace().clone()))?;
        format
            .verify(VerificationContext::new(key, reader), limits)
            .await
    }
}

/// Verifier for `casita.blob.v1`.
pub struct BlobFormat {
    namespace: NamespaceId,
}

impl BlobFormat {
    /// Seal a raw blob after the repository writer has consumed the complete
    /// source and returned its content digest and exact size.
    ///
    /// Unlike generic formats, `casita.blob.v1` has no framing or links: its
    /// native identity is exactly the payload digest. Reopening content that
    /// the content-addressed writer just accepted cannot prove anything more.
    #[cfg(feature = "native")]
    pub(crate) fn seal_written(
        payload: BlobId,
        payload_size: u64,
        limits: &FormatLimits,
    ) -> Result<VerifiedObject, FormatError> {
        if payload_size > limits.max_payload_bytes {
            return Err(FormatError::PayloadLimit {
                limit: limits.max_payload_bytes,
            });
        }
        let record =
            ObjectRecord::new(ObjectKey::blob(payload), payload, payload_size, Vec::new())?;
        Ok(VerifiedObject { record })
    }
}

impl Default for BlobFormat {
    fn default() -> Self {
        Self {
            namespace: BLOB_NAMESPACE.parse().expect("frozen namespace is valid"),
        }
    }
}

#[async_trait]
impl ObjectFormat for BlobFormat {
    fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }

    async fn verify(
        &self,
        mut context: VerificationContext<'_>,
        limits: &FormatLimits,
    ) -> Result<VerifiedObject, FormatError> {
        let expected = expected_digest(context.key(), &self.namespace)?;
        // Size hints only bound scratch space; even a zero or understated
        // length must still read to EOF and verify every byte.
        let buffer_bytes = context
            .exact_len()
            .and_then(|size| usize::try_from(size).ok())
            .unwrap_or(limits.read_buffer_bytes)
            .min(limits.read_buffer_bytes)
            .max(1);
        let mut buffer = vec![0u8; buffer_bytes];
        while context.read(&mut buffer).await? != 0 {
            if context.observed_size() > limits.max_payload_bytes {
                return Err(FormatError::PayloadLimit {
                    limit: limits.max_payload_bytes,
                });
            }
        }
        let actual = context.observed_digest();
        if actual != expected {
            return Err(FormatError::NativeIdentityMismatch {
                key: context.key().clone(),
                expected,
                actual,
            });
        }
        context.finish(Vec::new())
    }
}

/// Verifier for `casita.directory.v1`.
pub struct DirectoryFormat {
    namespace: NamespaceId,
}

impl Default for DirectoryFormat {
    fn default() -> Self {
        Self {
            namespace: DIRECTORY_NAMESPACE
                .parse()
                .expect("frozen namespace is valid"),
        }
    }
}

impl DirectoryFormat {
    /// Seal a directory whose canonical encoding was just written by the
    /// repository.
    ///
    /// [`Directory`] already enforces canonical names, ordering, and size
    /// arithmetic. The write result is still compared with the canonical
    /// directory digest before the record is accepted.
    #[cfg(feature = "native")]
    pub(crate) fn seal_written(
        directory: &Directory,
        payload: BlobId,
        payload_size: u64,
        limits: &FormatLimits,
    ) -> Result<VerifiedObject, FormatError> {
        let key = ObjectKey::directory(directory.digest());
        let expected = key
            .native_digest()
            .expect("the frozen directory key always contains one digest");
        let actual = payload.digest();
        if actual != expected {
            return Err(FormatError::NativeIdentityMismatch {
                key,
                expected,
                actual,
            });
        }
        if payload_size > limits.max_metadata_bytes.min(limits.max_payload_bytes) {
            return Err(FormatError::MetadataLimit {
                limit: limits.max_metadata_bytes.min(limits.max_payload_bytes),
            });
        }
        if directory.len() > limits.max_directory_entries {
            return Err(FormatError::DirectoryEntryLimit {
                actual: directory.len(),
                limit: limits.max_directory_entries,
            });
        }
        let mut links = BTreeSet::new();
        for (_, node) in directory.nodes() {
            match node {
                Node::Directory { digest, .. } => {
                    links.insert(ObjectKey::directory(*digest));
                }
                Node::File { digest, .. } => {
                    links.insert(ObjectKey::blob(*digest));
                }
                Node::Symlink { .. } => {}
            }
        }
        if links.len() > limits.max_links_per_object {
            return Err(FormatError::LinkLimit {
                actual: links.len(),
                limit: limits.max_links_per_object,
            });
        }
        let record = ObjectRecord::new(key, payload, payload_size, links.into_iter().collect())?;
        Ok(VerifiedObject { record })
    }

    async fn verify_and_decode(
        &self,
        mut context: VerificationContext<'_>,
        limits: &FormatLimits,
    ) -> Result<(VerifiedObject, Directory), FormatError> {
        let expected = expected_digest(context.key(), &self.namespace)?;
        let encoded = context
            .read_to_end_bounded(limits.max_metadata_bytes.min(limits.max_payload_bytes))
            .await?;
        let actual = context.observed_digest();
        if actual != expected {
            return Err(FormatError::NativeIdentityMismatch {
                key: context.key().clone(),
                expected,
                actual,
            });
        }
        let directory = crate::encode::decode_directory(&encoded).map_err(|error| {
            FormatError::InvalidPayload {
                namespace: self.namespace.clone(),
                message: error.to_string(),
            }
        })?;
        if directory.len() > limits.max_directory_entries {
            return Err(FormatError::DirectoryEntryLimit {
                actual: directory.len(),
                limit: limits.max_directory_entries,
            });
        }
        let mut links = BTreeSet::new();
        for (_, node) in directory.nodes() {
            match node {
                Node::Directory { digest, .. } => {
                    links.insert(ObjectKey::directory(*digest));
                }
                Node::File { digest, .. } => {
                    links.insert(ObjectKey::blob(*digest));
                }
                Node::Symlink { .. } => {}
            }
        }
        if links.len() > limits.max_links_per_object {
            return Err(FormatError::LinkLimit {
                actual: links.len(),
                limit: limits.max_links_per_object,
            });
        }
        let verified = context.finish(links.into_iter().collect())?;
        Ok((verified, directory))
    }
}

#[async_trait]
impl ObjectFormat for DirectoryFormat {
    fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }

    async fn verify(
        &self,
        context: VerificationContext<'_>,
        limits: &FormatLimits,
    ) -> Result<VerifiedObject, FormatError> {
        self.verify_and_decode(context, limits)
            .await
            .map(|(verified, _)| verified)
    }

    async fn verify_links(
        &self,
        context: VerificationContext<'_>,
        object: &ObjectRecord,
        direct_links: &dyn DirectLinkView,
        limits: &FormatLimits,
    ) -> Result<(), FormatError> {
        let (verified, directory) = self.verify_and_decode(context, limits).await?;
        if verified.record() != object {
            return Err(FormatError::RecordMismatch(object.key().clone()));
        }

        for (_, node) in directory.nodes() {
            match node {
                Node::File { digest, size, .. } => {
                    let target = ObjectKey::blob(*digest);
                    let child = required_record(direct_links, object.key(), &target).await?;
                    if child.key() != &target {
                        return Err(invalid_direct_record(object.key(), &target));
                    }
                    if child.payload_size() != *size {
                        return Err(FormatError::ByteLengthMismatch {
                            object: object.key().clone(),
                            target,
                            declared: *size,
                            actual: child.payload_size(),
                        });
                    }
                }
                Node::Directory { digest, size } => {
                    let target = ObjectKey::directory(*digest);
                    let child = required_record(direct_links, object.key(), &target).await?;
                    if child.key() != &target {
                        return Err(invalid_direct_record(object.key(), &target));
                    }
                    let mut payload =
                        direct_links.open_payload(&target).await?.ok_or_else(|| {
                            FormatError::MissingDirectLink {
                                object: object.key().clone(),
                                target: target.clone(),
                            }
                        })?;
                    let (child_verified, child_directory) = self
                        .verify_and_decode(
                            VerificationContext::new(&target, payload.as_mut()),
                            limits,
                        )
                        .await
                        .map_err(|error| FormatError::InvalidDirectLink {
                            object: object.key().clone(),
                            target: target.clone(),
                            message: error.to_string(),
                        })?;
                    if child_verified.record() != &child {
                        return Err(invalid_direct_record(object.key(), &target));
                    }
                    if child_directory.size() != *size {
                        return Err(FormatError::DescendantCountMismatch {
                            object: object.key().clone(),
                            target,
                            declared: *size,
                            actual: child_directory.size(),
                        });
                    }
                }
                Node::Symlink { .. } => {}
            }
        }
        Ok(())
    }
}

fn expected_digest(key: &ObjectKey, namespace: &NamespaceId) -> Result<Digest, FormatError> {
    if key.namespace() != namespace {
        return Err(FormatError::NamespaceMismatch {
            format: namespace.clone(),
            actual: key.namespace().clone(),
        });
    }
    key.native_digest()
        .ok_or_else(|| FormatError::NativeIdLength {
            namespace: namespace.clone(),
            actual: key.native_id().len(),
        })
}

async fn required_record(
    view: &dyn DirectLinkView,
    source: &ObjectKey,
    target: &ObjectKey,
) -> Result<ObjectRecord, FormatError> {
    view.record(target)
        .await?
        .ok_or_else(|| FormatError::MissingDirectLink {
            object: source.clone(),
            target: target.clone(),
        })
}

fn invalid_direct_record(source: &ObjectKey, target: &ObjectKey) -> FormatError {
    FormatError::InvalidDirectLink {
        object: source.clone(),
        target: target.clone(),
        message: "record does not match its declared key or verified payload".to_owned(),
    }
}

/// Whether a key names a native Git blob in either object format.
#[cfg(feature = "native")]
fn is_git_blob(key: &ObjectKey) -> bool {
    matches!(
        key.namespace().as_str(),
        crate::git::GIT_SHA1_BLOB_NAMESPACE | crate::git::GIT_SHA256_BLOB_NAMESPACE
    )
}

#[cfg(all(test, feature = "native"))]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::DirectoryId;
    use crate::path::{PathComponent, SymlinkTarget};

    fn pc(value: &str) -> PathComponent {
        value.try_into().unwrap()
    }

    async fn verify_bytes(
        format: &dyn ObjectFormat,
        key: &ObjectKey,
        bytes: &[u8],
    ) -> Result<VerifiedObject, FormatError> {
        let mut reader = Cursor::new(bytes.to_vec());
        format
            .verify(
                VerificationContext::new(key, &mut reader),
                &FormatLimits::default(),
            )
            .await
    }

    #[tokio::test]
    async fn bounded_metadata_reader_keeps_its_future_small() {
        let bytes = b"metadata";
        let key = ObjectKey::blob(BlobId::new(Digest::hash(bytes)));
        let mut reader = Cursor::new(bytes);
        let mut context = VerificationContext::new(&key, &mut reader);
        let read = context.read_to_end_bounded(bytes.len() as u64);
        assert!(std::mem::size_of_val(&read) < 4096);
        assert_eq!(read.await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn blob_verifier_binds_complete_payload() {
        let bytes = b"generic object payload";
        let key = ObjectKey::blob(BlobId::new(Digest::hash(bytes)));
        let verified = verify_bytes(&BlobFormat::default(), &key, bytes)
            .await
            .unwrap();
        assert_eq!(verified.record().key(), &key);
        assert_eq!(verified.record().payload_size(), bytes.len() as u64);
        assert!(verified.record().links().is_empty());

        let wrong = ObjectKey::blob(BlobId::new(Digest::hash(b"other")));
        assert!(matches!(
            verify_bytes(&BlobFormat::default(), &wrong, bytes).await,
            Err(FormatError::NativeIdentityMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn blob_verifier_uses_length_only_as_a_buffer_hint() {
        struct HintedReader<'a> {
            bytes: &'a [u8],
            length: Option<u64>,
            largest_buffer: usize,
            eof: bool,
        }

        #[async_trait]
        impl PayloadReader for HintedReader<'_> {
            fn exact_len(&self) -> Option<u64> {
                self.length
            }

            async fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                assert!(!buffer.is_empty());
                self.largest_buffer = self.largest_buffer.max(buffer.len());
                let count = std::io::Read::read(&mut self.bytes, buffer)?;
                self.eof |= count == 0;
                Ok(count)
            }
        }

        let bytes = b"the full payload must be verified despite its length hint";
        let key = ObjectKey::blob(BlobId::new(Digest::hash(bytes)));
        for length in [
            None,
            Some(0),
            Some(1),
            Some(bytes.len() as u64),
            Some(u64::MAX),
        ] {
            for capacity in [0, 1, 32, 65536] {
                let mut reader = HintedReader {
                    bytes,
                    length,
                    largest_buffer: 0,
                    eof: false,
                };
                let limits = FormatLimits {
                    read_buffer_bytes: capacity,
                    ..Default::default()
                };
                let verified = BlobFormat::default()
                    .verify(VerificationContext::new(&key, &mut reader), &limits)
                    .await
                    .unwrap();
                assert_eq!(verified.record().payload_size(), bytes.len() as u64);
                assert_eq!(verified.record().key(), &key);
                assert!(reader.eof);
                assert!(reader.largest_buffer <= capacity.max(1));
                if length == Some(bytes.len() as u64) {
                    assert!(reader.largest_buffer <= bytes.len());
                }

                reader.bytes = bytes;
                let limits = FormatLimits {
                    max_payload_bytes: bytes.len() as u64 - 1,
                    ..limits
                };
                assert!(matches!(
                    BlobFormat::default()
                        .verify(VerificationContext::new(&key, &mut reader), &limits)
                        .await,
                    Err(FormatError::PayloadLimit { .. })
                ));
            }
        }
    }

    #[tokio::test]
    async fn directory_verifier_extracts_sorted_unique_links() {
        let blob = BlobId::new(Digest::from([1; 32]));
        let child = DirectoryId::new(Digest::from([2; 32]));
        let directory = Directory::try_from_iter([
            (
                pc("a"),
                Node::File {
                    digest: blob,
                    size: 4,
                    executable: false,
                },
            ),
            (
                pc("b"),
                Node::File {
                    digest: blob,
                    size: 4,
                    executable: true,
                },
            ),
            (
                pc("child"),
                Node::Directory {
                    digest: child,
                    size: 0,
                },
            ),
            (
                pc("link"),
                Node::Symlink {
                    target: SymlinkTarget::try_from("a").unwrap(),
                },
            ),
        ])
        .unwrap();
        let encoded = directory.encode();
        let key = ObjectKey::directory(directory.digest());
        let verified = verify_bytes(&DirectoryFormat::default(), &key, &encoded)
            .await
            .unwrap();
        let expected: BTreeSet<_> = [ObjectKey::blob(blob), ObjectKey::directory(child)]
            .into_iter()
            .collect();
        assert_eq!(
            verified.record().links(),
            expected.into_iter().collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn directory_verifier_rejects_noncanonical_bytes() {
        let directory = Directory::new();
        let mut encoded = directory.encode();
        encoded.push(0);
        let key = ObjectKey::directory(DirectoryId::new(Digest::hash(&encoded)));
        assert!(matches!(
            verify_bytes(&DirectoryFormat::default(), &key, &encoded).await,
            Err(FormatError::InvalidPayload { .. })
        ));
    }
}
