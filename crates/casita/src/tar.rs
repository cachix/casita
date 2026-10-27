//! Bounded asynchronous import of POSIX tar archives into canonical filesystem
//! graphs.
//!
//! Regular file bodies stream directly from `tar-codec` into the
//! repository blob store; no extraction directory or whole-file buffer is
//! created. A bounded pipeline finalizes earlier files while the next body is
//! read. The decoder accepts POSIX ustar/pax and GNU archives with UTF-8
//! paths and link targets. Legacy v7, mixed-format streams, and sparse files
//! are rejected by the decoder.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::StreamExt;
use tar_codec::{Archive as _, Member, MemberPayload as _, SpecialKind, TarArchive};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use crate::directory::Directory;
use crate::node::Node;
use crate::object::{ObjectKey, RootName};
use crate::path::{PathComponent, SymlinkTarget};
use crate::repository::{Repository, RepositoryError, RepositoryErrorCategory};
use crate::{BlobStore, BlobWriter, MetadataStore};

/// Deployment bounds for one tar import.
///
/// The input is an already-decompressed tar stream. Apply a separate bound to
/// compressed transport bytes before passing a decoder here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TarImportLimits {
    /// Maximum raw tar bytes, including headers, extension records, payloads,
    /// and padding.
    pub max_archive_bytes: u64,
    /// Maximum logical entries, excluding tar extension records.
    pub max_entries: usize,
    /// Maximum files being copied, queued, finalized, or verified at once.
    /// One preserves serial file processing. The importer adds no whole-file
    /// buffer; each open writer retains its backend-specific streaming buffers.
    pub max_in_flight_files: usize,
    /// Maximum byte length of one archive pathname.
    pub max_path_bytes: usize,
    /// Maximum bytes in one regular file.
    pub max_file_bytes: u64,
    /// Maximum aggregate logical regular-file bytes.
    pub max_total_file_bytes: u64,
    /// Reserved sparse expansion bound. Sparse archives are rejected.
    pub max_sparse_expansion_bytes: u64,
}

impl Default for TarImportLimits {
    fn default() -> Self {
        Self {
            max_archive_bytes: 1 << 40,
            max_entries: 1_000_000,
            max_in_flight_files: 16,
            max_path_bytes: 4096,
            max_file_bytes: 1 << 38,
            max_total_file_bytes: 1 << 40,
            max_sparse_expansion_bytes: 1 << 38,
        }
    }
}

/// Successful tar import summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TarImportReport {
    /// The canonical root directory created from the archive.
    pub root: ObjectKey,
    /// Raw bytes consumed from the tar stream.
    pub archive_bytes: u64,
    /// Logical archive entries accepted.
    pub entries: usize,
    /// Regular-file entries accepted.
    pub files: usize,
    /// Directory entries accepted (implicit directories are not counted).
    pub directories: usize,
    /// Symlink entries accepted.
    pub symlinks: usize,
    /// Hard-link entries resolved to regular files.
    pub hardlinks: usize,
    /// Aggregate logical regular-file bytes.
    pub file_bytes: u64,
    /// Sparse expansion bytes, always zero because sparse archives are rejected.
    pub sparse_expansion_bytes: u64,
}

/// Failure while importing an untrusted tar stream.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TarImportError {
    /// The tar parser rejected malformed or truncated input.
    #[error("invalid tar stream: {0}")]
    Tar(#[source] io::Error),
    /// The input or blob-store I/O path failed.
    #[error("tar import I/O failed: {0}")]
    Io(#[source] io::Error),
    /// The caller's selected limits are internally inconsistent.
    #[error("invalid tar import limits: {0}")]
    InvalidLimits(&'static str),
    /// A tar pathname is absolute, empty, has unsafe components, or is too
    /// long for the selected model.
    #[error("tar entry path is invalid")]
    InvalidPath,
    /// A symlink or hard-link target is absent, unsafe, or invalid.
    #[error("tar link target is invalid")]
    InvalidLink,
    /// Two entries claim the same canonical path.
    #[error("tar archive contains a duplicate path")]
    DuplicatePath,
    /// A non-directory entry conflicts with a descendant path.
    #[error("tar archive contains conflicting paths")]
    ConflictingPath,
    /// A hard link does not ultimately name a regular file.
    #[error("tar hard link does not resolve to a regular file")]
    InvalidHardlink,
    /// GNU PAX sparse metadata is unsupported by the decoder.
    #[error("PAX GNU sparse files are not supported")]
    UnsupportedPaxSparse,
    /// The archive declares an unsupported filesystem object.
    #[error("tar entry type is unsupported: {0:?}")]
    UnsupportedEntry(SpecialKind),
    /// A configured resource limit was exceeded.
    #[error("tar import {field} {actual} exceeds limit {limit}")]
    LimitExceeded {
        /// Bounded field.
        field: &'static str,
        /// Observed value.
        actual: u64,
        /// Configured maximum.
        limit: u64,
    },
    /// Repository staging or publication failed.
    #[error(transparent)]
    Repository(#[from] RepositoryError),
}

impl TarImportError {
    /// Stable category shared with other repository-facing workflows.
    pub fn category(&self) -> RepositoryErrorCategory {
        match self {
            Self::Repository(error) => error.category(),
            Self::Io(_) => RepositoryErrorCategory::Backend,
            Self::Tar(error) => match error.kind() {
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof => {
                    RepositoryErrorCategory::InvalidData
                }
                _ => RepositoryErrorCategory::Backend,
            },
            Self::InvalidLimits(_)
            | Self::InvalidPath
            | Self::InvalidLink
            | Self::DuplicatePath
            | Self::ConflictingPath
            | Self::InvalidHardlink
            | Self::LimitExceeded { .. } => RepositoryErrorCategory::InvalidInput,
            Self::UnsupportedPaxSparse | Self::UnsupportedEntry(_) => {
                RepositoryErrorCategory::Unsupported
            }
        }
    }
}

#[derive(Debug, Clone)]
enum SourceEntry {
    File,
    Symlink(Node),
    Hardlink(PathKey),
    Directory,
}

type PathKey = Vec<PathComponent>;

/// A fully consumed tar body whose owned writer can finish independently of
/// the archive reader. Admission covers the producer and queue as well as the
/// finalizers, rather than giving each stage a separate concurrency allowance.
struct FileUpload {
    path: PathKey,
    writer: Box<dyn BlobWriter>,
    size: u64,
    executable: bool,
    _permit: OwnedSemaphorePermit,
}

/// Enforce the archive byte limit before the tar parser can retain extension
/// data. An extra probe at the boundary distinguishes a clean EOF from input
/// that exceeds the selected limit.
struct LimitedReader<R> {
    inner: R,
    max: u64,
    seen: u64,
    observed: Arc<AtomicU64>,
}

impl<R: AsyncRead + Unpin> AsyncRead for LimitedReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.seen == this.max {
            let mut probe = [0_u8; 1];
            let mut probe = ReadBuf::new(&mut probe);
            return match Pin::new(&mut this.inner).poll_read(cx, &mut probe) {
                Poll::Ready(Ok(())) if probe.filled().is_empty() => Poll::Ready(Ok(())),
                Poll::Ready(Ok(())) => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "tar archive exceeds configured byte limit",
                ))),
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Pending => Poll::Pending,
            };
        }

        let remaining = usize::try_from(this.max - this.seen).unwrap_or(usize::MAX);
        let capacity = buf.remaining().min(remaining);
        let mut inner_buf = ReadBuf::new(buf.initialize_unfilled_to(capacity));
        match Pin::new(&mut this.inner).poll_read(cx, &mut inner_buf) {
            Poll::Ready(Ok(())) => {
                let read = inner_buf.filled().len();
                buf.advance(read);
                this.seen += read as u64;
                this.observed.fetch_add(read as u64, Ordering::Relaxed);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<PS, SS> Repository<PS, SS>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    /// Stream a decompressed tar archive into the canonical filesystem graph.
    ///
    /// Accepts regular files, directories, symlinks, and hard links in
    /// POSIX ustar/pax or GNU archives with UTF-8 paths and link targets.
    /// Legacy v7, mixed-format streams, and sparse files are rejected.
    #[tracing::instrument(name = "tar.import", skip_all)]
    pub(crate) async fn import_tar<R>(
        &self,
        reader: R,
        name: RootName,
        limits: TarImportLimits,
    ) -> Result<TarImportReport, TarImportError>
    where
        R: AsyncRead + Unpin + Send,
    {
        self.import_tar_with_retention(reader, name, limits, None)
            .await
    }

    pub(crate) async fn import_tar_with_retention<R>(
        &self,
        reader: R,
        name: RootName,
        limits: TarImportLimits,
        retention: Option<crate::RootRetention>,
    ) -> Result<TarImportReport, TarImportError>
    where
        R: AsyncRead + Unpin + Send,
    {
        validate_limits(limits)?;
        if retention.is_some() && !self.metadata().supports_root_retention() {
            return Err(
                RepositoryError::Metadata(crate::MetadataError::UnsupportedMetadata).into(),
            );
        }
        let observed = Arc::new(AtomicU64::new(0));
        let mut reader = LimitedReader {
            inner: reader,
            max: limits.max_archive_bytes,
            seen: 0,
            observed: observed.clone(),
        };
        let mut archive = TarArchive::new(&mut reader);
        let mutation = self.mutation_session().await?;
        let batch = self.limits().max_batch_objects;
        if batch == 0 {
            return Err(RepositoryError::LimitExceeded(
                "tar import requires a nonzero mutation batch limit".to_owned(),
            )
            .into());
        }

        let mut pending = Vec::new();
        let mut tree = BTreeMap::<PathKey, SourceEntry>::new();
        let mut directory_paths = BTreeSet::<PathKey>::new();
        directory_paths.insert(Vec::new());
        let mut report = TarImportReport {
            root: ObjectKey::directory(crate::DirectoryId::new(crate::Digest::from([0; 32]))),
            archive_bytes: 0,
            entries: 0,
            files: 0,
            directories: 0,
            symlinks: 0,
            hardlinks: 0,
            file_bytes: 0,
            sparse_expansion_bytes: 0,
        };

        let concurrency = limits.max_in_flight_files.min(limits.max_entries);
        let admission = Arc::new(Semaphore::new(concurrency));
        let (sender, receiver) = mpsc::channel(concurrency);
        let produce = async {
            while let Some(entry) = archive.next_member().await.map_err(decode_error)? {
                increment_limit("entry count", &mut report.entries, limits.max_entries)?;
                let path = canonical_path(entry.metadata().path.as_bytes(), limits.max_path_bytes)?;
                insert_parent_directories(&mut directory_paths, &path);
                ensure_no_conflict(&tree, &path, matches!(entry, Member::Directory { .. }))?;

                let source = match entry {
                    Member::File {
                        size,
                        executable,
                        mut payload,
                        ..
                    } => {
                        check_limit("file bytes", size, limits.max_file_bytes)?;
                        add_limit(
                            "total file bytes",
                            &mut report.file_bytes,
                            size,
                            limits.max_total_file_bytes,
                        )?;
                        let permit = admission
                            .clone()
                            .acquire_owned()
                            .await
                            .expect("tar admission stays open");
                        let mut writer = mutation.repository().payloads().open_write().await;
                        let mut copied = 0;
                        let mut buffer = Vec::new();
                        while payload
                            .next_chunk(&mut buffer, 64 * 1024)
                            .await
                            .map_err(decode_error)?
                        {
                            tokio::io::AsyncWriteExt::write_all(&mut writer, &buffer)
                                .await
                                .map_err(TarImportError::Io)?;
                            copied += buffer.len() as u64;
                        }
                        if copied != size {
                            return Err(TarImportError::Tar(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "tar entry size does not match streamed data",
                            )));
                        }
                        sender
                            .send(FileUpload {
                                path: path.clone(),
                                writer,
                                size,
                                executable,
                                _permit: permit,
                            })
                            .await
                            .map_err(|_| {
                                TarImportError::Io(io::Error::new(
                                    io::ErrorKind::BrokenPipe,
                                    "tar file finalizer stopped",
                                ))
                            })?;
                        report.files += 1;
                        SourceEntry::File
                    }
                    Member::Directory { .. } => {
                        directory_paths.insert(path.clone());
                        report.directories += 1;
                        SourceEntry::Directory
                    }
                    Member::SymbolicLink { target, .. } => {
                        let target = target.as_bytes();
                        validate_symlink_target(&path, target)?;
                        report.symlinks += 1;
                        SourceEntry::Symlink(Node::Symlink {
                            target: SymlinkTarget::try_from(Bytes::copy_from_slice(target))
                                .map_err(|_| TarImportError::InvalidLink)?,
                        })
                    }
                    Member::HardLink {
                        target,
                        size,
                        payload,
                        ..
                    } => {
                        if size != 0 {
                            return Err(TarImportError::Tar(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "tar hard link has data",
                            )));
                        }
                        payload.skip().await.map_err(decode_error)?;
                        let target = target.as_bytes();
                        let target = canonical_path(target, limits.max_path_bytes)
                            .map_err(|_| TarImportError::InvalidLink)?;
                        report.hardlinks += 1;
                        SourceEntry::Hardlink(target)
                    }
                    Member::Special { kind, .. } => {
                        return Err(TarImportError::UnsupportedEntry(kind));
                    }
                };

                if tree.insert(path, source).is_some() {
                    return Err(TarImportError::DuplicatePath);
                }
            }
            drop(sender);
            Ok::<_, TarImportError>(())
        };
        let consume = async {
            let mut uploads = ReceiverStream::new(receiver)
                .map(|file: FileUpload| {
                    let mutation = &mutation;
                    async move {
                        let FileUpload {
                            path,
                            mut writer,
                            size,
                            executable,
                            _permit,
                        } = file;
                        let (digest, actual_size) = writer.close().await.map_err(|error| {
                            TarImportError::Repository(RepositoryError::Payload(error))
                        })?;
                        if actual_size != size {
                            return Err(TarImportError::Tar(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "tar entry size does not match streamed data",
                            )));
                        }
                        let object = mutation
                            .stage_existing(ObjectKey::blob(digest), digest)
                            .await?;
                        Ok((
                            path,
                            Node::File {
                                digest,
                                size,
                                executable,
                            },
                            object,
                        ))
                    }
                })
                .buffer_unordered(concurrency);
            let mut files = BTreeMap::new();
            while let Some(result) = uploads.next().await {
                let (path, node, object) = result?;
                files.insert(path, node);
                pending.push(object);
                if pending.len() == batch {
                    mutation
                        .publish_unrooted(std::mem::take(&mut pending))
                        .await?;
                }
            }
            Ok::<_, TarImportError>(files)
        };
        // Co-poll both halves: a writer copying a large body may need memory
        // permits held by an earlier finalizer. Both pipeline halves remain
        // owned by this import future. Scope every raw write to this mutation.
        let (_, files) = mutation
            .write_scope()
            .run(async { tokio::try_join!(produce, consume) })
            .await?;
        // Release the archive borrow, then validate transport trailers and limits.
        drop(archive);
        tokio::io::copy(&mut reader, &mut tokio::io::sink())
            .await
            .map_err(TarImportError::Tar)?;

        let leaves = resolve_links(&tree, files)?;
        for path in tree.keys() {
            insert_parent_directories(&mut directory_paths, path);
        }
        let mut directories: BTreeMap<PathKey, Directory> = directory_paths
            .iter()
            .cloned()
            .map(|path| (path, Directory::new()))
            .collect();
        for (path, node) in leaves {
            add_to_parent(&mut directories, &path, node)?;
        }

        let mut paths: Vec<_> = directories.keys().cloned().collect();
        paths.sort_by(|left, right| right.len().cmp(&left.len()).then_with(|| right.cmp(left)));
        let mut root = None;
        for path in paths {
            let directory = directories.remove(&path).expect("listed directory exists");
            let size = directory.size();
            let digest = directory.digest();
            let object = mutation.stage_directory(&directory).await?;
            let node = Node::Directory { digest, size };
            if path.is_empty() {
                root = Some(ObjectKey::directory(digest));
            } else {
                add_to_parent(&mut directories, &path, node)?;
            }
            pending.push(object);
            if pending.len() == batch {
                mutation
                    .publish_unrooted(std::mem::take(&mut pending))
                    .await?;
            }
        }
        let root = root.expect("the importer always creates a root directory");
        if let Some(retention) = retention {
            mutation
                .publish_with_metadata(
                    pending,
                    Vec::new(),
                    vec![
                        crate::MetadataChange::SetRoot {
                            name: name.clone(),
                            target: root.clone(),
                        },
                        crate::repository::root_policy::policy_change(&name, retention),
                    ],
                )
                .await?;
        } else {
            mutation.publish_rooted(pending, name, root.clone()).await?;
        }
        report.root = root;
        report.archive_bytes = observed.load(Ordering::Relaxed);
        tracing::info!(
            archive_bytes = report.archive_bytes,
            entries = report.entries,
            files = report.files,
            directories = report.directories,
            symlinks = report.symlinks,
            hardlinks = report.hardlinks,
            file_bytes = report.file_bytes,
            sparse_expansion_bytes = report.sparse_expansion_bytes,
            "tar import completed"
        );
        Ok(report)
    }
}

pub(crate) fn decode_error(error: tar_codec::DecodeError) -> TarImportError {
    if matches!(&error, tar_codec::DecodeError::PolicyViolation {
        violation: tar_codec::DecodePolicyViolation::PaxVendorExtension { vendor, name }, ..
    } if vendor == "GNU" && name.starts_with("sparse."))
    {
        return TarImportError::UnsupportedPaxSparse;
    }
    // Keep transport failures distinct from malformed archives.
    let mut cause: &dyn std::error::Error = &error;
    let mut kind = io::ErrorKind::InvalidData;
    loop {
        if let Some(io) = cause.downcast_ref::<io::Error>() {
            kind = io.kind();
            break;
        }
        match cause.source() {
            Some(source) => cause = source,
            None => break,
        }
    }
    TarImportError::Tar(io::Error::new(kind, error))
}

fn validate_limits(limits: TarImportLimits) -> Result<(), TarImportError> {
    if limits.max_in_flight_files == 0 || limits.max_in_flight_files > Semaphore::MAX_PERMITS {
        return Err(TarImportError::InvalidLimits(
            "max_in_flight_files is outside the supported range",
        ));
    }
    if limits.max_entries == 0 {
        return Err(TarImportError::InvalidLimits("max_entries must be nonzero"));
    }
    if limits.max_path_bytes == 0 {
        return Err(TarImportError::InvalidLimits(
            "max_path_bytes must be nonzero",
        ));
    }
    Ok(())
}

fn increment_limit(
    field: &'static str,
    value: &mut usize,
    limit: usize,
) -> Result<(), TarImportError> {
    *value = value.checked_add(1).ok_or(TarImportError::LimitExceeded {
        field,
        actual: u64::MAX,
        limit: limit as u64,
    })?;
    if *value > limit {
        return Err(TarImportError::LimitExceeded {
            field,
            actual: *value as u64,
            limit: limit as u64,
        });
    }
    Ok(())
}

fn check_limit(field: &'static str, actual: u64, limit: u64) -> Result<(), TarImportError> {
    if actual > limit {
        return Err(TarImportError::LimitExceeded {
            field,
            actual,
            limit,
        });
    }
    Ok(())
}

fn add_limit(
    field: &'static str,
    total: &mut u64,
    amount: u64,
    limit: u64,
) -> Result<(), TarImportError> {
    *total = total
        .checked_add(amount)
        .ok_or(TarImportError::LimitExceeded {
            field,
            actual: u64::MAX,
            limit,
        })?;
    check_limit(field, *total, limit)
}

fn canonical_path(path: &[u8], max_bytes: usize) -> Result<PathKey, TarImportError> {
    if path.is_empty() || path.starts_with(b"/") || path.len() > max_bytes {
        return Err(TarImportError::InvalidPath);
    }
    let path = path.strip_suffix(b"/").unwrap_or(path);
    if path.is_empty() {
        return Err(TarImportError::InvalidPath);
    }
    path.split(|byte| *byte == b'/')
        .map(|component| {
            PathComponent::try_from(Bytes::copy_from_slice(component))
                .map_err(|_| TarImportError::InvalidPath)
        })
        .collect()
}

fn validate_symlink_target(path: &[PathComponent], target: &[u8]) -> Result<(), TarImportError> {
    if target.is_empty() || target.starts_with(b"/") {
        return Err(TarImportError::InvalidLink);
    }
    let mut depth = path
        .len()
        .checked_sub(1)
        .ok_or(TarImportError::InvalidLink)?;
    for component in target.split(|byte| *byte == b'/') {
        match component {
            b"" | b"." => {}
            b".." => {
                depth = depth.checked_sub(1).ok_or(TarImportError::InvalidLink)?;
            }
            _ => depth = depth.checked_add(1).ok_or(TarImportError::InvalidLink)?,
        }
    }
    Ok(())
}

fn insert_parent_directories(directories: &mut BTreeSet<PathKey>, path: &[PathComponent]) {
    for depth in 0..path.len() {
        directories.insert(path[..depth].to_vec());
    }
}

fn ensure_no_conflict(
    tree: &BTreeMap<PathKey, SourceEntry>,
    path: &[PathComponent],
    directory: bool,
) -> Result<(), TarImportError> {
    if tree.contains_key(path) {
        return Err(TarImportError::DuplicatePath);
    }
    for depth in 1..path.len() {
        if tree
            .get(&path[..depth])
            .is_some_and(|entry| !matches!(entry, SourceEntry::Directory))
        {
            return Err(TarImportError::ConflictingPath);
        }
    }
    if !directory
        && tree
            .keys()
            .any(|other| other.starts_with(path) && other.len() > path.len())
    {
        return Err(TarImportError::ConflictingPath);
    }
    Ok(())
}

fn resolve_links(
    tree: &BTreeMap<PathKey, SourceEntry>,
    mut leaves: BTreeMap<PathKey, Node>,
) -> Result<BTreeMap<PathKey, Node>, TarImportError> {
    let mut links = Vec::new();
    for (path, entry) in tree {
        match entry {
            SourceEntry::Symlink(node) => {
                leaves.insert(path.clone(), node.clone());
            }
            SourceEntry::Hardlink(target) => links.push((path.clone(), target)),
            SourceEntry::File | SourceEntry::Directory => {}
        }
    }
    while !links.is_empty() {
        let mut unresolved = Vec::new();
        let mut made_progress = false;
        for (path, target) in links {
            match leaves.get(target) {
                Some(Node::File { .. }) => {
                    leaves.insert(path, leaves.get(target).expect("just found").clone());
                    made_progress = true;
                }
                Some(_) => return Err(TarImportError::InvalidHardlink),
                None => unresolved.push((path, target)),
            }
        }
        if !made_progress {
            return Err(TarImportError::InvalidHardlink);
        }
        links = unresolved;
    }
    Ok(leaves)
}

fn add_to_parent(
    directories: &mut BTreeMap<PathKey, Directory>,
    path: &[PathComponent],
    node: Node,
) -> Result<(), TarImportError> {
    let (name, parent) = path.split_last().ok_or(TarImportError::ConflictingPath)?;
    let directory = directories
        .get_mut(parent)
        .ok_or(TarImportError::ConflictingPath)?;
    directory
        .add(name.clone(), node)
        .map_err(|_| TarImportError::ConflictingPath)
}

#[cfg(test)]
mod pipeline_tests;

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use ::tar::{Builder, EntryType, Header};

    use super::*;
    use crate::{MemoryBlobStore, MemoryMetadataStore};

    pub(super) async fn archive(files: &[(&str, &[u8], u32)]) -> Vec<u8> {
        let mut builder = Builder::new(Vec::new());
        for (path, body, mode) in files {
            let mut header = Header::new_ustar();
            header.set_size(body.len() as u64);
            header.set_mode(*mode);
            builder.append_data(&mut header, path, *body).unwrap();
        }
        builder.into_inner().unwrap()
    }

    async fn repository() -> Repository<MemoryBlobStore, MemoryMetadataStore> {
        Repository::memory().unwrap()
    }

    #[tokio::test]
    async fn import_streams_files_into_a_canonical_directory_tree() {
        let repository = repository().await;
        let input = archive(&[
            ("bin/tool", b"#!/bin/sh\necho hi\n", 0o755),
            ("empty/.keep", b"", 0o644),
            ("README", b"hello\n", 0o644),
        ])
        .await;
        let root = repository
            .import_tar(
                Cursor::new(input),
                RootName::try_from("tar/demo").unwrap(),
                TarImportLimits::default(),
            )
            .await
            .unwrap()
            .root;

        let output = tempfile::tempdir().unwrap();
        repository.checkout(&root, output.path()).await.unwrap();
        assert_eq!(
            std::fs::read(output.path().join("README")).unwrap(),
            b"hello\n"
        );
        assert_eq!(
            std::fs::read(output.path().join("bin/tool")).unwrap(),
            b"#!/bin/sh\necho hi\n"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_ne!(
                std::fs::metadata(output.path().join("bin/tool"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o100,
                0
            );
        }
    }

    #[tokio::test]
    async fn entry_order_does_not_change_the_root_identity() {
        let first = archive(&[("z", b"last", 0o644), ("a", b"first", 0o644)]).await;
        let second = archive(&[("a", b"first", 0o644), ("z", b"last", 0o644)]).await;
        let repository = repository().await;
        let one = repository
            .import_tar(
                Cursor::new(first),
                RootName::try_from("tar/one").unwrap(),
                TarImportLimits::default(),
            )
            .await
            .unwrap()
            .root;
        let two = repository
            .import_tar(
                Cursor::new(second),
                RootName::try_from("tar/two").unwrap(),
                TarImportLimits::default(),
            )
            .await
            .unwrap()
            .root;
        assert_eq!(one, two);
    }

    #[tokio::test]
    async fn links_preserve_their_canonical_filesystem_meaning() {
        let mut builder = Builder::new(Vec::new());
        let mut file = Header::new_ustar();
        file.set_path("bin/tool").unwrap();
        file.set_size(5);
        file.set_mode(0o644);
        file.set_cksum();
        builder.append(&file, &b"tool\n"[..]).unwrap();

        let mut hardlink = Header::new_ustar();
        hardlink.set_entry_type(EntryType::Link);
        hardlink.set_path("bin/tool-copy").unwrap();
        hardlink.set_link_name("bin/tool").unwrap();
        hardlink.set_size(0);
        hardlink.set_mode(0o644);
        hardlink.set_cksum();
        builder.append(&hardlink, &[][..]).unwrap();

        let mut symlink = Header::new_ustar();
        symlink.set_entry_type(EntryType::Symlink);
        symlink.set_path("tool").unwrap();
        symlink.set_link_name("bin/tool").unwrap();
        symlink.set_size(0);
        symlink.set_mode(0o777);
        symlink.set_cksum();
        builder.append(&symlink, &[][..]).unwrap();

        let repository = repository().await;
        let root = repository
            .import_tar(
                Cursor::new(builder.into_inner().unwrap()),
                RootName::try_from("tar/links").unwrap(),
                TarImportLimits::default(),
            )
            .await
            .unwrap()
            .root;
        let output = tempfile::tempdir().unwrap();
        repository.checkout(&root, output.path()).await.unwrap();
        assert_eq!(
            std::fs::read(output.path().join("bin/tool-copy")).unwrap(),
            b"tool\n"
        );
        assert_eq!(
            std::fs::read_link(output.path().join("tool")).unwrap(),
            std::path::PathBuf::from("bin/tool")
        );
    }

    #[tokio::test]
    async fn empty_directories_round_trip() {
        let mut builder = Builder::new(Vec::new());
        let mut directory = Header::new_ustar();
        directory.set_entry_type(EntryType::Directory);
        directory.set_path("empty").unwrap();
        directory.set_size(0);
        directory.set_mode(0o755);
        directory.set_cksum();
        builder.append(&directory, &[][..]).unwrap();

        let repository = repository().await;
        let report = repository
            .import_tar(
                Cursor::new(builder.into_inner().unwrap()),
                RootName::try_from("tar/empty").unwrap(),
                TarImportLimits::default(),
            )
            .await
            .unwrap();
        assert_eq!(report.directories, 1);
        let root = report.root;
        let output = tempfile::tempdir().unwrap();
        repository.checkout(&root, output.path()).await.unwrap();
        let empty = output.path().join("empty");
        assert!(empty.is_dir());
        assert!(std::fs::read_dir(empty).unwrap().next().is_none());
    }

    #[tokio::test]
    async fn unsafe_paths_links_duplicates_and_limits_are_rejected() {
        let traversal = raw_archive(b"../outside", b'0', b"bad");
        assert!(matches!(
            repository()
                .await
                .import_tar(
                    Cursor::new(traversal),
                    RootName::try_from("tar/traversal").unwrap(),
                    TarImportLimits::default(),
                )
                .await,
            Err(TarImportError::InvalidPath)
        ));

        let mut links = Builder::new(Vec::new());
        let mut symlink = Header::new_ustar();
        symlink.set_entry_type(EntryType::Symlink);
        symlink.set_path("link").unwrap();
        symlink.set_link_name("../outside").unwrap();
        symlink.set_size(0);
        symlink.set_mode(0o777);
        symlink.set_cksum();
        links.append(&symlink, &[][..]).unwrap();
        assert!(matches!(
            repository()
                .await
                .import_tar(
                    Cursor::new(links.into_inner().unwrap()),
                    RootName::try_from("tar/link").unwrap(),
                    TarImportLimits::default(),
                )
                .await,
            Err(TarImportError::InvalidLink)
        ));

        let duplicate = archive(&[("same", b"one", 0o644), ("same", b"two", 0o644)]).await;
        assert!(matches!(
            repository()
                .await
                .import_tar(
                    Cursor::new(duplicate),
                    RootName::try_from("tar/duplicate").unwrap(),
                    TarImportLimits::default(),
                )
                .await,
            Err(TarImportError::DuplicatePath)
        ));

        let limits = TarImportLimits {
            max_file_bytes: 2,
            ..TarImportLimits::default()
        };
        assert!(matches!(
            repository()
                .await
                .import_tar(
                    Cursor::new(archive(&[("large", b"three", 0o644)]).await),
                    RootName::try_from("tar/limited").unwrap(),
                    limits,
                )
                .await,
            Err(TarImportError::LimitExceeded {
                field: "file bytes",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn pax_gnu_sparse_metadata_is_rejected_explicitly() {
        let record = pax_record("GNU.sparse.map", "0,1");
        let mut builder = Builder::new(Vec::new());
        let mut pax = Header::new_ustar();
        pax.set_entry_type(EntryType::XHeader);
        pax.set_path("PaxHeaders/sparse").unwrap();
        pax.set_size(record.len() as u64);
        pax.set_cksum();
        builder.append(&pax, &record[..]).unwrap();
        let mut file = Header::new_ustar();
        file.set_path("sparse").unwrap();
        file.set_size(0);
        file.set_cksum();
        builder.append(&file, &[][..]).unwrap();
        let input = builder.into_inner().unwrap();

        let error = repository()
            .await
            .import_tar(
                Cursor::new(input),
                RootName::try_from("tar/sparse").unwrap(),
                TarImportLimits::default(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, TarImportError::UnsupportedPaxSparse));
    }

    #[tokio::test]
    async fn pax_paths_and_executable_files_round_trip() {
        use tar_codec::{ArchiveBuilder as _, EntryMetadata, FilePayload, TarEncoder};
        let path = format!("{}file", "directory/".repeat(20));
        let mut input = Vec::new();
        let mut encoder = TarEncoder::new(&mut input).builder();
        encoder
            .add_file(
                &path,
                FilePayload::new(11, Cursor::new(b"pax content")),
                EntryMetadata::default().executable(true),
            )
            .await
            .unwrap();
        encoder.finish().await.unwrap();
        let repository = repository().await;
        let report = repository
            .import_tar(
                Cursor::new(input),
                RootName::try_from("tar/pax").unwrap(),
                TarImportLimits::default(),
            )
            .await
            .unwrap();
        let output = tempfile::tempdir().unwrap();
        repository
            .checkout(&report.root, output.path())
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(output.path().join(path)).unwrap(),
            b"pax content"
        );
        assert_eq!(report.file_bytes, 11);
    }

    #[tokio::test]
    async fn invalid_framing_and_non_utf8_paths_do_not_publish_roots() {
        let valid = raw_archive(b"file", b'0', b"content");
        let mut checksum = valid.clone();
        checksum[0] ^= 1;
        let mut legacy = valid.clone();
        legacy[257..265].fill(0);
        legacy[148..156].fill(b' ');
        let sum: u32 = legacy[..512].iter().map(|byte| u32::from(*byte)).sum();
        legacy[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
        for input in [
            checksum,
            legacy,
            valid[..515].to_vec(),
            raw_archive(b"non-utf8-\xff", b'0', b""),
            raw_archive(b"directory", b'5', b"unexpected body"),
        ] {
            let repository = repository().await;
            let name = RootName::try_from("tar/invalid").unwrap();
            let error = repository
                .import_tar(Cursor::new(input), name.clone(), TarImportLimits::default())
                .await
                .unwrap_err();
            assert_eq!(
                error.category(),
                RepositoryErrorCategory::InvalidData,
                "{error}"
            );
            assert!(
                repository
                    .metadata()
                    .snapshot()
                    .await
                    .unwrap()
                    .root(&name)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }

    fn pax_record(key: &str, value: &str) -> Vec<u8> {
        let mut length = key.len() + value.len() + 4;
        loop {
            let record = format!("{length} {key}={value}\n");
            if record.len() == length {
                return record.into_bytes();
            }
            length = record.len();
        }
    }

    fn raw_archive(path: &[u8], kind: u8, body: &[u8]) -> Vec<u8> {
        assert!(path.len() <= 100);
        let mut header = [0_u8; 512];
        header[..path.len()].copy_from_slice(path);
        header[100..108].copy_from_slice(b"0000644\0");
        header[108..116].copy_from_slice(b"0000000\0");
        header[116..124].copy_from_slice(b"0000000\0");
        let size = format!("{:011o}\0", body.len());
        header[124..136].copy_from_slice(size.as_bytes());
        header[136..148].copy_from_slice(b"00000000000\0");
        header[148..156].fill(b' ');
        header[156] = kind;
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");
        let checksum: u32 = header.iter().map(|byte| u32::from(*byte)).sum();
        let checksum = format!("{:06o}\0 ", checksum);
        header[148..156].copy_from_slice(checksum.as_bytes());

        let padding = (512 - body.len() % 512) % 512;
        let mut archive = Vec::with_capacity(512 + body.len() + padding + 1024);
        archive.extend_from_slice(&header);
        archive.extend_from_slice(body);
        archive.resize(512 + body.len() + padding, 0);
        archive.resize(archive.len() + 1024, 0);
        archive
    }
}
