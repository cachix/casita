//! Canonical NAR ingestion and repository-local measured associations.
//!
//! Reports retain content but do not register Nix paths or authenticate peer
//! metadata. Native intake uses the filesystem importer and, on a cache miss,
//! one verified canonical serialization. No temporary NAR is created.
//! Unknown/custom and remote backends always measure content again.

use crate::api::Repository;
use crate::repository::MutationSession;
use crate::{Directory, Node, ObjectKey, PathComponent, RetainedReader, SymlinkTarget};
use bytes::Bytes;
use sha2::{Digest as _, Sha256, Sha512};
use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

mod decoder;
pub(crate) mod store;
pub(crate) mod stream;

#[cfg(test)]
use crate::import::{FilesystemNarImport, NarImport};
#[cfg(test)]
mod tests;

/// Algorithms supported by NAR and content measurement.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum NarHashAlgorithm {
    /// MD5 for legacy Nix content addresses.
    Md5 = 4,
    /// Collision-detecting SHA-1.
    Sha1 = 1,
    /// SHA-256, always measured for the canonical NAR.
    Sha256 = 2,
    /// SHA-512.
    Sha512 = 3,
}
/// Hash domain. Digests in different domains are never interchangeable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum NarHashMethod {
    /// Canonical NAR bytes.
    Nar = 0,
    /// Contents of a regular root file.
    Flat = 1,
    /// Contents of a non-executable regular root file.
    Text = 2,
    /// Git blob/tree encoding (SHA-1 or SHA-256).
    Git = 3,
}
/// Requested measurements and reference-needle indices.
#[derive(Clone, Debug, Default)]
pub struct NarRequirements {
    hashes: BTreeSet<(NarHashMethod, NarHashAlgorithm)>,
    needles: Vec<Vec<u8>>,
}
impl NarRequirements {
    /// Add a tagged hash. Canonical NAR SHA-256 is implicit.
    pub fn hash(mut self, method: NarHashMethod, algorithm: NarHashAlgorithm) -> Self {
        self.hashes.insert((method, algorithm));
        self
    }
    /// Scan canonical NAR bytes for these candidate store-path hash parts,
    /// the way Nix discovers references: each needle is exactly 32 Nix
    /// base32 bytes and matches anywhere in the archive, including entry
    /// names and symlink targets. Results are indices into this exact list,
    /// including duplicate needles.
    pub fn reference_needles(mut self, needles: Vec<Vec<u8>>) -> Self {
        self.needles = needles;
        self
    }
    fn keys(&self) -> Vec<Vec<u8>> {
        let mut keys: BTreeSet<_> = self
            .hashes
            .iter()
            .map(|(m, a)| vec![*m as u8, *a as u8])
            .collect();
        keys.insert(vec![0, 2]);
        if !self.needles.is_empty() {
            let mut hash = blake3::Hasher::new();
            for needle in &self.needles {
                hash.update(&(needle.len() as u64).to_le_bytes());
                hash.update(needle);
            }
            let mut key = vec![4];
            key.extend_from_slice(hash.finalize().as_bytes());
            keys.insert(key);
        }
        keys.into_iter().collect()
    }
    pub(crate) fn validate(&self, root: Option<&Node>) -> Result<(), NarError> {
        if matches!(root, Some(Node::Directory { size: u64::MAX, .. })) {
            return Err(NarError::invalid("directory size overflows tree encoding"));
        }
        if !self.needles.is_empty() {
            stream::reference_pattern(&self.needles)?;
        }
        for (method, algorithm) in &self.hashes {
            if *method == NarHashMethod::Git
                && !matches!(algorithm, NarHashAlgorithm::Sha1 | NarHashAlgorithm::Sha256)
            {
                return Err(NarError::Invalid("Git supports SHA-1 and SHA-256".into()));
            }
        }
        if let Some(root) = root {
            self.validate_root(match root {
                Node::File { executable, .. } => Some(*executable),
                _ => None,
            })?;
        }
        Ok(())
    }
    /// `root_file` is `Some(executable)` for a file root. Flat and text
    /// digests exist only for files, text only for non-executable ones. Raw
    /// intake knows the root kind from its first frame and checks before
    /// staging anything.
    fn validate_root(&self, root_file: Option<bool>) -> Result<(), NarError> {
        for (method, _) in &self.hashes {
            match (method, root_file) {
                (NarHashMethod::Flat, Some(_)) | (NarHashMethod::Text, Some(false)) => (),
                (NarHashMethod::Flat | NarHashMethod::Text, _) => {
                    return Err(NarError::Invalid(
                        "flat requires a file; text requires a non-executable file".into(),
                    ));
                }
                _ => (),
            }
        }
        Ok(())
    }
}
/// Work performed by this request, excluding native ingestion/closure checks.
#[derive(Clone, Debug, Default)]
pub struct NarVerificationStats {
    /// Association supplied every requested fact.
    pub association_hit: bool,
    /// Canonical tree encoding passes (zero for raw streaming intake).
    pub encoding_passes: u64,
    /// File payload bytes read for measurement.
    pub hash_payload_bytes: u64,
    /// Verification elapsed time.
    pub verification_time: Duration,
}
/// Locally measured facts and an owned retention hold. Fields are private and
/// there is deliberately no constructor or deserializer accepting facts.
pub struct VerifiedNarReport {
    root: Node,
    facts: Facts,
    matches: Vec<usize>,
    stats: NarVerificationStats,
    reader: RetainedReader,
}
impl VerifiedNarReport {
    /// Complete immutable root, including executable bit and inline symlink.
    pub fn root(&self) -> &Node {
        &self.root
    }
    /// Length of canonical NAR serialization.
    pub fn nar_size(&self) -> u64 {
        self.facts.size
    }
    /// A measured digest in its exact domain.
    pub fn hash(&self, method: NarHashMethod, algorithm: NarHashAlgorithm) -> Option<&[u8]> {
        self.facts
            .values
            .get(&vec![method as u8, algorithm as u8])
            .map(Vec::as_slice)
    }
    /// Mandatory canonical SHA-256.
    pub fn nar_sha256(&self) -> &[u8] {
        self.hash(NarHashMethod::Nar, NarHashAlgorithm::Sha256)
            .unwrap()
    }
    /// Indices into the requested reference needles, in ascending order.
    pub fn reference_matches(&self) -> &[usize] {
        &self.matches
    }
    /// Measurement instrumentation.
    pub fn stats(&self) -> &NarVerificationStats {
        &self.stats
    }
    /// Retained view for publication or further verified reads.
    pub fn reader(&self) -> &RetainedReader {
        &self.reader
    }
}
/// NAR validation, native storage, or association failure.
#[derive(Debug, thiserror::Error)]
pub enum NarError {
    /// Malformed/noncanonical input or unsupported measurement request.
    #[error("invalid NAR request or encoding: {0}")]
    Invalid(String),
    /// I/O or truncated archive.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Native storage failure.
    #[error("NAR storage: {0}")]
    Storage(#[source] crate::Error),
    /// A local association disagreed with a fresh measurement. Reuse is disabled.
    #[error("conflicting or quarantined local NAR association")]
    Conflict,
}
impl NarError {
    /// Stable failure category, using the ordinary repository API categories.
    pub fn kind(&self) -> crate::ErrorKind {
        match self {
            Self::Invalid(_) => crate::ErrorKind::InvalidInput,
            Self::Io(error) => match error.kind() {
                std::io::ErrorKind::NotFound => crate::ErrorKind::Absent,
                std::io::ErrorKind::InvalidInput => crate::ErrorKind::InvalidInput,
                std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof => {
                    crate::ErrorKind::InvalidData
                }
                _ => crate::ErrorKind::Backend,
            },
            Self::Storage(error) => error.kind(),
            Self::Conflict => crate::ErrorKind::ImmutableConflict,
        }
    }

    /// Whether and when repeating this operation may succeed. `Never` means
    /// retrying unchanged input and repository state cannot resolve the failure.
    pub fn retry_disposition(&self) -> crate::RetryDisposition {
        match self {
            Self::Invalid(_) | Self::Conflict => crate::RetryDisposition::Never,
            Self::Storage(error) => error.retry_disposition(),
            Self::Io(_) if self.kind() != crate::ErrorKind::Backend => {
                crate::RetryDisposition::Never
            }
            Self::Io(error) => crate::error::wrapped_retry_disposition(error),
        }
    }

    pub(crate) fn storage(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        let error: Box<dyn std::error::Error + Send + Sync> = Box::new(error);
        let error = match error.downcast::<crate::Error>() {
            Ok(error) => return Self::Storage(*error),
            Err(error) => error,
        };
        let error = match error.downcast::<crate::repository::RepositoryError>() {
            Ok(error) => return Self::Storage(crate::Error::classified(error.category(), *error)),
            Err(error) => error,
        };
        let error = match error.downcast::<crate::metadata::MetadataError>() {
            Ok(error) => {
                return Self::storage(crate::repository::RepositoryError::Metadata(*error));
            }
            Err(error) => error,
        };
        let error = match error.downcast::<crate::error::Error>() {
            Ok(error) => return Self::storage(crate::repository::RepositoryError::Payload(*error)),
            Err(error) => error,
        };
        Self::Storage(crate::Error::classified(
            crate::ErrorKind::Backend,
            std::io::Error::other(error),
        ))
    }
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }
}
impl From<crate::Error> for NarError {
    fn from(error: crate::Error) -> Self {
        Self::storage(error)
    }
}

#[derive(Clone, Debug)]
struct Facts {
    size: u64,
    values: BTreeMap<Vec<u8>, Vec<u8>>,
}
impl Facts {
    fn complete(&self, request: &NarRequirements) -> bool {
        request.keys().iter().all(|k| self.values.contains_key(k))
    }
    fn encode(&self) -> Vec<u8> {
        let mut bytes = self.size.to_le_bytes().to_vec();
        for (key, value) in &self.values {
            bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
            bytes.extend_from_slice(key);
            bytes.extend_from_slice(value);
        }
        bytes
    }
    fn decode(mut bytes: &[u8]) -> Result<Self, NarError> {
        if bytes.is_empty() {
            return Err(NarError::Conflict);
        }
        fn take<'a>(bytes: &mut &'a [u8], len: usize) -> Result<&'a [u8], NarError> {
            if bytes.len() < len {
                return Err(NarError::invalid("truncated association"));
            }
            let (head, tail) = bytes.split_at(len);
            *bytes = tail;
            Ok(head)
        }
        let size = u64::from_le_bytes(take(&mut bytes, 8)?.try_into().unwrap());
        let mut values = BTreeMap::new();
        while !bytes.is_empty() {
            let k = u32::from_le_bytes(take(&mut bytes, 4)?.try_into().unwrap()) as usize;
            let v = u32::from_le_bytes(take(&mut bytes, 4)?.try_into().unwrap()) as usize;
            let key = take(&mut bytes, k)?.to_vec();
            let value = take(&mut bytes, v)?.to_vec();
            if values.insert(key, value).is_some() {
                return Err(NarError::invalid("duplicate association fact"));
            }
        }
        if values.get(&vec![0, 2]).is_none_or(|v| v.len() != 32) {
            return Err(NarError::invalid("association lacks SHA-256"));
        }
        Ok(Self { size, values })
    }
}

/// Versioned identity commits to every root field through the frozen Casita
/// directory encoding. Nested directory digests commit to the same fields.
const IDENTITY_VERSION: &[u8] = b"casita-tree-v1/nar-v1/verifier-v2/";

fn identity(root: &Node) -> Vec<u8> {
    let mut directory = Directory::new();
    directory
        .add(PathComponent::try_from("root").unwrap(), root.clone())
        .unwrap();
    let mut key = IDENTITY_VERSION.to_vec();
    key.extend_from_slice(&directory.encode());
    key
}
fn object_key(node: &Node) -> Option<ObjectKey> {
    match node {
        Node::File { digest, .. } => Some(ObjectKey::blob(*digest)),
        Node::Directory { digest, .. } => Some(ObjectKey::directory(*digest)),
        Node::Symlink { .. } => None,
    }
}

/// Inspect a retained tree's local association. Missing facts return `None`.
/// Incomplete content is an error, even if an association survives collection.
pub async fn lookup_nar(
    reader: &RetainedReader,
    root: &Node,
    request: &NarRequirements,
) -> Result<Option<VerifiedNarReport>, NarError> {
    request.validate(Some(root))?;
    available(reader, root).await?;
    let Some(store) = &reader.hold.repository().nar_store else {
        return Ok(None);
    };
    let Some(facts) = store.get(&identity(root)).await? else {
        return Ok(None);
    };
    if !facts.complete(request) {
        return Ok(None);
    }
    report(
        reader.clone(),
        root.clone(),
        facts,
        request,
        NarVerificationStats {
            association_hit: true,
            ..Default::default()
        },
    )
    .map(Some)
}
/// Measure only missing facts, or reuse locally measured facts without encoding
/// or reading file payloads. The reader must remain retained through publication.
pub async fn ensure_nar(
    reader: &RetainedReader,
    root: &Node,
    request: &NarRequirements,
) -> Result<VerifiedNarReport, NarError> {
    measure(reader, root, request, false).await
}
/// Explicit full-content audit. Reads and verifies payloads even on a cache hit.
pub async fn scrub_nar(
    reader: &RetainedReader,
    root: &Node,
    request: &NarRequirements,
) -> Result<VerifiedNarReport, NarError> {
    measure(reader, root, request, true).await
}

/// Records resolved per snapshot call while walking a closure's records.
const AVAILABILITY_FRONTIER: usize = 256;

async fn available(reader: &RetainedReader, root: &Node) -> Result<(), NarError> {
    let Some(root_key) = object_key(root) else {
        return Ok(());
    };
    let status = reader
        .hold
        .verify_closure_incremental(&root_key)
        .await
        .map_err(NarError::storage)?;
    if !matches!(status, crate::ClosureStatus::Complete { .. }) {
        return Err(NarError::storage(
            crate::repository::RepositoryError::ObjectNotReadable {
                object: root_key.clone(),
                status,
            },
        ));
    }
    let root_size = match root {
        Node::File { size, .. } => Some(*size),
        _ => None,
    };
    let Some(store) = &reader.hold.repository().nar_store else {
        if let Some(size) = root_size {
            root_size_check(reader, &root_key, size).await?;
        }
        return Ok(());
    };
    let payloads = reader.hold.repository().payloads();
    let identity = identity(root);
    // An earlier walk left the payload store's witness: the storage objects
    // every payload lives in. While they all still exist, walking the records
    // and probing each payload again proves nothing new.
    if let Some(witness) = store.witness(&identity).await?
        && payloads
            .nar_witness_holds(&witness)
            .await
            .map_err(NarError::storage)?
    {
        return Ok(());
    }
    // Do not treat a catalog witness as proof that its physical packs
    // still exist. Walk metadata links, then probe every payload constituent
    // in one batch.
    let mut frontier = vec![root_key.clone()];
    let mut seen = BTreeSet::from([root_key.clone()]);
    let mut digests = Vec::new();
    while !frontier.is_empty() {
        let batch: Vec<ObjectKey> = frontier
            .drain(..frontier.len().min(AVAILABILITY_FRONTIER))
            .collect();
        let records = reader
            .hold
            .object_batch(&batch)
            .await
            .map_err(NarError::storage)?;
        for (key, record) in batch.into_iter().zip(records) {
            let record = record.ok_or_else(|| {
                NarError::storage(crate::repository::RepositoryError::Absent(
                    "closure object".into(),
                ))
            })?;
            if let Some(size) = root_size
                && key == root_key
                && record.payload_size() != size
            {
                return Err(NarError::invalid("root file size mismatch"));
            }
            digests.push(record.payload());
            for link in record.links() {
                if seen.insert(link.clone()) {
                    if seen.len() > 1_000_000 {
                        return Err(NarError::invalid("closure limit exceeded"));
                    }
                    frontier.push(link.clone());
                }
            }
        }
    }
    match payloads.nar_witness(&digests).await {
        Ok(Some(witness)) => {
            if !witness.is_empty() {
                store.set_witness(&identity, witness).await?;
            }
            Ok(())
        }
        Ok(None) => {
            store.record_read_failure().await;
            let mut missing = root_key.clone();
            for (key, digest) in seen.iter().zip(&digests) {
                if matches!(payloads.nar_available(digest).await, Ok(false)) {
                    missing = key.clone();
                    break;
                }
            }
            let record = reader.object(&missing).await?;
            Err(NarError::storage(
                crate::repository::RepositoryError::MissingPayload(
                    record.map_or_else(|| digests[0], |record| record.payload()),
                ),
            ))
        }
        Err(error) => {
            if crate::blob::is_damaged_payload_error(&error) {
                store.record_read_failure().await;
            }
            Err(NarError::storage(error))
        }
    }
}

async fn root_size_check(
    reader: &RetainedReader,
    key: &ObjectKey,
    size: u64,
) -> Result<(), NarError> {
    let record = reader.object(key).await?.ok_or_else(|| {
        NarError::storage(crate::repository::RepositoryError::Absent("root".into()))
    })?;
    if record.payload_size() != size {
        return Err(NarError::invalid("root file size mismatch"));
    }
    Ok(())
}

async fn measure(
    reader: &RetainedReader,
    root: &Node,
    request: &NarRequirements,
    scrub: bool,
) -> Result<VerifiedNarReport, NarError> {
    let start = Instant::now();
    request.validate(Some(root))?;
    let store = reader.hold.repository().nar_store.as_ref();
    let key = identity(root);
    let flight = store.map(|s| s.flight(&key));
    let _guard = match &flight {
        Some(lock) => Some(lock.lock().await),
        None => None,
    };
    available(reader, root).await?;
    let mut old = match store {
        Some(store) if !scrub => store.get(&key).await?,
        _ => None,
    };
    if !scrub
        && let Some(facts) = &old
        && facts.complete(request)
    {
        return report(
            reader.clone(),
            root.clone(),
            facts.clone(),
            request,
            NarVerificationStats {
                association_hit: true,
                verification_time: start.elapsed(),
                ..Default::default()
            },
        );
    }
    // Cache hits write nothing, so only measurement needs a generation fence.
    // A partial hit may have been invalidated since the lookup above. Reload
    // any facts we will reuse after capturing the generation, or a newer
    // generation could allow those retired facts to be published again.
    let generation = match store {
        Some(store) => {
            let generation = store.generation().await.map_err(NarError::storage)?;
            if scrub || old.is_some() {
                old = store.get(&key).await?;
            }
            generation
        }
        None => 0,
    };
    let mut missing = request.clone();
    if !scrub && let Some(old) = &old {
        missing
            .hashes
            .retain(|(m, a)| !old.values.contains_key(&vec![*m as u8, *a as u8]));
        if request
            .keys()
            .iter()
            .filter(|k| k[0] == 4)
            .all(|k| old.values.contains_key(k))
        {
            missing.needles.clear();
        }
    }
    let measured = stream::measure_tree(reader, root, &missing, old.as_ref(), scrub).await;
    let (mut facts, mut stats) = match measured {
        Ok(result) => result,
        Err(error) => {
            if let Some(store) = store {
                // Verified opens and reads already invalidate on damage; this
                // covers tree decoding. Busy or throttled storage is not damage.
                let damaged = match &error {
                    NarError::Io(error) => crate::blob::is_damaged_payload_io_error(error),
                    NarError::Storage(error) => matches!(
                        error.kind(),
                        crate::ErrorKind::Corrupt | crate::ErrorKind::InvalidData
                    ),
                    NarError::Invalid(_) | NarError::Conflict => false,
                };
                if matches!(error, NarError::Conflict) {
                    store.quarantine(&key).await?;
                } else if damaged {
                    store.record_read_failure().await;
                }
            }
            return Err(error);
        }
    };
    if let Some(store) = store {
        // Measured from verified reads. An invalidation that landed since
        // withholds the association, not the measurement.
        if let Some(merged) = store.merge(&key, &facts, generation).await? {
            facts = merged;
        }
    }
    stats.verification_time = start.elapsed();
    report(reader.clone(), root.clone(), facts, request, stats)
}
fn report(
    reader: RetainedReader,
    root: Node,
    facts: Facts,
    request: &NarRequirements,
    stats: NarVerificationStats,
) -> Result<VerifiedNarReport, NarError> {
    let mut matches = Vec::new();
    if let Some(key) = request.keys().into_iter().find(|k| k[0] == 4) {
        let bytes = facts
            .values
            .get(&key)
            .ok_or_else(|| NarError::invalid("missing scan"))?;
        if bytes.len() % 8 != 0 {
            return Err(NarError::invalid("invalid scan record"));
        }
        for index in bytes.chunks_exact(8) {
            let index = u64::from_le_bytes(index.try_into().unwrap());
            if index >= request.needles.len() as u64 {
                return Err(NarError::invalid("invalid needle index"));
            }
            matches.push(index as usize);
        }
    }
    tracing::debug!(
        hit = stats.association_hit,
        encoding_passes = stats.encoding_passes,
        hash_payload_bytes = stats.hash_payload_bytes,
        verification_us = stats.verification_time.as_micros(),
        "NAR verification"
    );
    Ok(VerifiedNarReport {
        root,
        facts,
        matches,
        stats,
        reader,
    })
}

/// Bounded association maintenance result. Pass `next` back to continue a
/// sweep, or start with `None`. Maintenance never retains objects or removes
/// conflict tombstones.
#[derive(Debug)]
pub struct NarAssociationCleanup {
    /// Number of orphan records removed.
    pub removed: usize,
    /// Last examined private identity, or `None` at the end of this sweep.
    pub next: Option<Vec<u8>>,
}
/// Remove a bounded page of associations whose native root no longer exists.
/// Run periodically outside registration. Inline symlinks need no storage and
/// their cheap associations may be removed by any sweep.
pub async fn prune_nar_associations(
    repository: &Repository,
    after: Option<&[u8]>,
    limit: usize,
) -> Result<NarAssociationCleanup, NarError> {
    if !(1..=1024).contains(&limit) {
        return Err(NarError::invalid("cleanup limit must be 1..=1024"));
    }
    let Some(store) = &repository.inner.nar_store else {
        return Ok(NarAssociationCleanup {
            removed: 0,
            next: None,
        });
    };
    let keys = store
        .page(after.unwrap_or_default().to_vec(), limit)
        .await?;
    let next = if keys.len() == limit {
        keys.last().cloned()
    } else {
        None
    };
    let reader = repository.retained_reader().await?;
    let mut removed = 0;
    for key in keys {
        // A witness entry follows its identity's fate without counting as an
        // association of its own.
        let witness = key.strip_prefix(store::WITNESS_PREFIX);
        let identity = witness.unwrap_or(&key);
        let Some(encoded) = identity.strip_prefix(IDENTITY_VERSION) else {
            continue;
        };
        let directory = Directory::decode(encoded).map_err(NarError::storage)?;
        let root = directory
            .get("root")
            .ok_or_else(|| NarError::invalid("invalid association identity"))?;
        let absent = match object_key(root) {
            Some(key) => reader.object(&key).await?.is_none(),
            None => true,
        };
        if absent {
            let association = witness.is_none();
            store.remove(key).await?;
            if association {
                removed += 1;
            }
        }
    }
    Ok(NarAssociationCleanup { removed, next })
}
