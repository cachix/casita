//! Apply OCI filesystem changesets without extracting or following paths.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use bytes::Bytes;
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use tar_codec::{Archive as _, Member, MemberPayload as _, TarArchive};
use tokio::io::{AsyncRead, BufReader, ReadBuf};

use super::{OciImportError, blob_node, check_limit, digest_name};
use crate::blob::BlobStore;
use crate::metadata::MetadataStore;
use crate::repository::{MutationSession, StagedObject};
use crate::{Directory, Node, ObjectKey, PathComponent, SymlinkTarget};

/// Bounds for constructing one merged OCI filesystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OciRootfsLimits {
    /// Maximum decoded tar bytes in one layer, including padding and extensions.
    pub max_layer_bytes: u64,
    /// Maximum decoded tar bytes across all layers.
    pub max_total_archive_bytes: u64,
    /// Maximum logical tar entries across all layers, including whiteouts.
    pub max_entries: usize,
    /// Maximum nodes in the merged tree, including implicit directories.
    pub max_tree_entries: usize,
    /// Maximum byte length of a tar pathname or hardlink target.
    pub max_path_bytes: usize,
    /// Maximum bytes in one regular file.
    pub max_file_bytes: u64,
    /// Maximum regular file bytes staged across all layers, including overwritten files.
    pub max_total_file_bytes: u64,
}

impl Default for OciRootfsLimits {
    fn default() -> Self {
        Self {
            max_layer_bytes: 1 << 40,
            max_total_archive_bytes: 1 << 40,
            max_entries: 1_000_000,
            max_tree_entries: 1_000_000,
            max_path_bytes: 4096,
            max_file_bytes: 1 << 38,
            max_total_file_bytes: 1 << 40,
        }
    }
}

impl OciRootfsLimits {
    pub(super) fn validate(self) -> Result<(), OciImportError> {
        if self.max_entries == 0 || self.max_tree_entries == 0 || self.max_path_bytes == 0 {
            return Err(OciImportError::Invalid(
                "filesystem entry and path limits must be positive".into(),
            ));
        }
        Ok(())
    }
}

type Path = Vec<PathComponent>;

#[derive(Clone)]
enum Entry {
    Directory,
    Leaf(Node),
    Hardlink(Path),
}

enum Whiteout {
    Remove(Path),
    Opaque(Path),
}

#[derive(Default)]
struct Tree {
    entries: BTreeMap<Path, Entry>,
    archive_bytes: u64,
    entry_count: usize,
    file_bytes: u64,
}

fn layer_decode_error(error: tar_codec::DecodeError) -> OciImportError {
    match crate::tar::decode_error(error) {
        crate::tar::TarImportError::UnsupportedPaxSparse => {
            OciImportError::Unsupported("sparse layer files".into())
        }
        crate::tar::TarImportError::Tar(error) => OciImportError::Io(error),
        error => invalid(format!("invalid layer tar: {error}")),
    }
}

fn invalid(message: impl Into<String>) -> OciImportError {
    OciImportError::Invalid(message.into())
}

fn path(bytes: &[u8], max: usize) -> Result<Path, OciImportError> {
    if bytes.is_empty() || bytes.starts_with(b"/") || bytes.len() > max {
        return Err(invalid("layer path is empty, absolute, or too long"));
    }
    bytes
        .split(|byte| *byte == b'/')
        .filter(|part| !part.is_empty() && *part != b".")
        .map(|part| {
            PathComponent::try_from(Bytes::copy_from_slice(part))
                .map_err(|_| invalid("invalid layer path component"))
        })
        .collect()
}

fn remove_tree(
    entries: &mut BTreeMap<Path, Entry>,
    prefix: &[PathComponent],
    descendants_only: bool,
) {
    let removed: Vec<_> = entries
        .range(prefix.to_vec()..)
        .take_while(|(path, _)| path.starts_with(prefix))
        .filter(|(path, _)| !descendants_only || path.len() > prefix.len())
        .map(|(path, _)| path.clone())
        .collect();
    for path in removed {
        entries.remove(&path);
    }
}

fn check_conflict(
    updates: &BTreeMap<Path, Entry>,
    path: &[PathComponent],
    directory: bool,
) -> Result<(), OciImportError> {
    for depth in 1..path.len() {
        if updates
            .get(&path[..depth])
            .is_some_and(|entry| !matches!(entry, Entry::Directory))
        {
            return Err(invalid("layer entry descends through a non-directory"));
        }
    }
    if !directory
        && updates
            .range(path.to_vec()..)
            .next()
            .is_some_and(|(other, _)| other.starts_with(path) && other.len() > path.len())
    {
        return Err(invalid("layer non-directory conflicts with a child entry"));
    }
    Ok(())
}

fn ensure_parents(
    entries: &mut BTreeMap<Path, Entry>,
    path: &[PathComponent],
    max: usize,
) -> Result<(), OciImportError> {
    for depth in 1..path.len() {
        let parent = path[..depth].to_vec();
        match entries.get(&parent) {
            Some(Entry::Directory) => {}
            Some(_) => {
                return Err(invalid(
                    "layer path descends through a non-directory in an earlier layer",
                ));
            }
            None => {
                check_limit(
                    "filesystem tree entries",
                    entries.len() as u64 + 1,
                    max as u64,
                )?;
                entries.insert(parent, Entry::Directory);
            }
        }
    }
    Ok(())
}

/// Measure and hash every decoded byte, including extension records and tar padding.
struct Measured<R> {
    inner: R,
    hash: Sha256,
    seen: u64,
    max: u64,
}

impl<R: AsyncRead + Unpin> AsyncRead for Measured<R> {
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
            let mut byte = [0u8; 1];
            let mut probe = ReadBuf::new(&mut byte);
            return match Pin::new(&mut this.inner).poll_read(cx, &mut probe) {
                Poll::Ready(Ok(())) if probe.filled().is_empty() => Poll::Ready(Ok(())),
                Poll::Ready(Ok(())) => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "decoded OCI layer exceeds byte limit",
                ))),
                result => result,
            };
        }
        let capacity = buf
            .remaining()
            .min(usize::try_from(this.max - this.seen).unwrap_or(usize::MAX));
        let mut inner = ReadBuf::new(buf.initialize_unfilled_to(capacity));
        match Pin::new(&mut this.inner).poll_read(cx, &mut inner) {
            Poll::Ready(Ok(())) => {
                let count = inner.filled().len();
                this.hash.update(inner.filled());
                this.seen += count as u64;
                buf.advance(count);
                Poll::Ready(Ok(()))
            }
            result => result,
        }
    }
}

async fn flush<PS: BlobStore, SS: MetadataStore>(
    mutation: &MutationSession<'_, PS, SS>,
    staged: &mut Vec<StagedObject<'_>>,
) -> Result<(), OciImportError> {
    if !staged.is_empty() {
        mutation.publish_unrooted(std::mem::take(staged)).await?;
    }
    Ok(())
}

impl Tree {
    async fn apply<R, PS, SS>(
        &mut self,
        mutation: &MutationSession<'_, PS, SS>,
        reader: R,
        diff_id: &str,
        limits: OciRootfsLimits,
    ) -> Result<(), OciImportError>
    where
        R: AsyncRead + Unpin + Send,
        PS: BlobStore,
        SS: MetadataStore,
    {
        digest_name(diff_id)?;
        let max = limits.max_layer_bytes.min(
            limits
                .max_total_archive_bytes
                .saturating_sub(self.archive_bytes),
        );
        let mut reader = Measured {
            inner: reader,
            hash: Sha256::new(),
            seen: 0,
            max,
        };
        let mut archive = TarArchive::new(&mut reader);
        let mut updates = BTreeMap::new();
        let mut seen = BTreeSet::new();
        let mut whiteouts = Vec::new();
        let mut staged = Vec::new();
        let batch = mutation.repository().limits().max_batch_objects;
        if batch == 0 {
            return Err(invalid("repository mutation batch limit must be positive"));
        }
        while let Some(record) = archive.next_member().await.map_err(layer_decode_error)? {
            self.entry_count = self
                .entry_count
                .checked_add(1)
                .ok_or_else(|| invalid("entry count overflow"))?;
            check_limit(
                "filesystem entries",
                self.entry_count as u64,
                limits.max_entries as u64,
            )?;
            let pathname = path(record.metadata().path.as_bytes(), limits.max_path_bytes)?;
            if !seen.insert(pathname.clone()) {
                return Err(invalid("duplicate path in one OCI layer"));
            }
            let directory = matches!(record, Member::Directory { .. });
            if pathname.is_empty() {
                if !directory {
                    return Err(invalid("layer root entry must be an empty directory"));
                }
                continue;
            }
            let (name, parent) = pathname.split_last().expect("nonempty path");
            if let Some(target) = name.as_bytes().strip_prefix(b".wh.") {
                if !matches!(record, Member::File { size: 0, .. }) {
                    return Err(invalid("whiteout must be an empty regular file"));
                }
                if name.as_bytes() == b".wh..wh..opq" {
                    whiteouts.push(Whiteout::Opaque(parent.to_vec()));
                } else {
                    let name = PathComponent::try_from(Bytes::copy_from_slice(target))
                        .map_err(|_| invalid("invalid whiteout target"))?;
                    let mut target = parent.to_vec();
                    target.push(name);
                    whiteouts.push(Whiteout::Remove(target));
                }
                continue;
            }
            check_conflict(&updates, &pathname, directory)?;
            let entry = match record {
                Member::File {
                    size,
                    executable,
                    mut payload,
                    ..
                } => {
                    check_limit("filesystem file bytes", size, limits.max_file_bytes)?;
                    self.file_bytes = self
                        .file_bytes
                        .checked_add(size)
                        .ok_or_else(|| invalid("file size overflow"))?;
                    check_limit(
                        "filesystem total file bytes",
                        self.file_bytes,
                        limits.max_total_file_bytes,
                    )?;
                    let chunks = async_stream::stream! {
                        let mut buffer = Vec::new();
                        loop {
                            match payload.next_chunk(&mut buffer, 64 * 1024).await {
                                Ok(true) => yield Ok::<_, io::Error>(Bytes::copy_from_slice(&buffer)),
                                Ok(false) => break,
                                Err(error) => {
                                    yield Err(io::Error::new(io::ErrorKind::InvalidData, error));
                                    break;
                                }
                            }
                        }
                    };
                    let mut body = tokio_util::io::StreamReader::new(Box::pin(chunks));
                    let object = mutation.stage_blob_reader(&mut body).await?;
                    if object.record().payload_size() != size {
                        return Err(invalid("layer file size mismatch"));
                    }
                    let mut node = blob_node(&object);
                    if let Node::File {
                        executable: bit, ..
                    } = &mut node
                    {
                        *bit = executable;
                    }
                    staged.push(object);
                    if staged.len() >= batch {
                        flush(mutation, &mut staged).await?;
                    }
                    Entry::Leaf(node)
                }
                Member::Directory { .. } => Entry::Directory,
                Member::SymbolicLink { target, .. } => {
                    let target = target.as_bytes();
                    let target = SymlinkTarget::try_from(Bytes::copy_from_slice(target))
                        .map_err(|_| invalid("invalid symlink target"))?;
                    Entry::Leaf(Node::Symlink { target })
                }
                Member::HardLink {
                    target,
                    size,
                    payload,
                    ..
                } => {
                    if size != 0 {
                        return Err(invalid("layer hardlink has data"));
                    }
                    payload.skip().await.map_err(layer_decode_error)?;
                    let target = target.as_bytes();
                    let target = path(target, limits.max_path_bytes)?;
                    if target.is_empty() {
                        return Err(invalid("hardlink target is the root directory"));
                    }
                    Entry::Hardlink(target)
                }
                Member::Special { kind, .. } => {
                    return Err(OciImportError::Unsupported(format!(
                        "layer entry type {kind:?}"
                    )));
                }
            };
            updates.insert(pathname, entry);
        }
        drop(archive);
        tokio::io::copy(&mut reader, &mut tokio::io::sink()).await?;
        let actual = format!("sha256:{:x}", reader.hash.finalize());
        if actual != diff_id {
            return Err(invalid(format!(
                "uncompressed layer digest mismatch: expected {diff_id}, got {actual}"
            )));
        }
        self.archive_bytes += reader.seen;
        flush(mutation, &mut staged).await?;

        // Whiteouts affect only the previous layer's tree, irrespective of tar order.
        for whiteout in whiteouts {
            match whiteout {
                Whiteout::Remove(path) => remove_tree(&mut self.entries, &path, false),
                Whiteout::Opaque(path) => remove_tree(&mut self.entries, &path, true),
            }
        }
        let mut links = Vec::new();
        for (path, entry) in updates {
            ensure_parents(&mut self.entries, &path, limits.max_tree_entries)?;
            if !matches!(entry, Entry::Directory)
                || !matches!(self.entries.get(&path), Some(Entry::Directory))
            {
                remove_tree(&mut self.entries, &path, false);
            }
            check_limit(
                "filesystem tree entries",
                self.entries.len() as u64 + u64::from(!self.entries.contains_key(&path)),
                limits.max_tree_entries as u64,
            )?;
            if let Entry::Hardlink(target) = &entry {
                links.push((path.clone(), target.clone()));
            }
            self.entries.insert(path, entry);
        }
        for (start, _) in links {
            let mut path = start;
            let mut chain = BTreeSet::new();
            let node = loop {
                match self.entries.get(&path) {
                    Some(Entry::Leaf(node @ Node::File { .. })) => break node.clone(),
                    Some(Entry::Hardlink(target)) => {
                        if !chain.insert(path.clone()) {
                            return Err(invalid("cyclic hardlinks in layer"));
                        }
                        path = target.clone();
                    }
                    _ => return Err(invalid("hardlink does not resolve to a regular file")),
                }
            };
            for path in chain {
                self.entries.insert(path, Entry::Leaf(node.clone()));
            }
        }
        Ok(())
    }

    async fn finish<PS: BlobStore, SS: MetadataStore>(
        self,
        mutation: &MutationSession<'_, PS, SS>,
    ) -> Result<ObjectKey, OciImportError> {
        let mut directories = BTreeMap::new();
        directories.insert(Vec::new(), Directory::new());
        for (path, entry) in &self.entries {
            if matches!(entry, Entry::Directory) {
                directories.insert(path.clone(), Directory::new());
            }
        }
        for (path, entry) in self.entries {
            if let Entry::Leaf(node) = entry {
                let (name, parent) = path.split_last().expect("nonempty filesystem path");
                directories
                    .get_mut(parent)
                    .ok_or_else(|| invalid("missing parent directory"))?
                    .add(name.clone(), node)?;
            }
        }
        let mut paths: Vec<_> = directories.keys().cloned().collect();
        paths.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| b.cmp(a)));
        let batch = mutation.repository().limits().max_batch_objects;
        let mut staged = Vec::new();
        let mut root = None;
        for path in paths {
            let directory = directories.remove(&path).expect("listed directory");
            let key = ObjectKey::directory(directory.digest());
            if let Some((name, parent)) = path.split_last() {
                directories
                    .get_mut(parent)
                    .ok_or_else(|| invalid("missing parent directory"))?
                    .add(
                        name.clone(),
                        Node::Directory {
                            digest: directory.digest(),
                            size: directory.size(),
                        },
                    )?;
            } else {
                root = Some(key);
            }
            staged.push(mutation.stage_directory(&directory).await?);
            if staged.len() >= batch {
                flush(mutation, &mut staged).await?;
            }
        }
        flush(mutation, &mut staged).await?;
        Ok(root.expect("root directory always exists"))
    }
}

#[derive(Deserialize)]
struct ImageConfig {
    rootfs: ConfigRootfs,
}

#[derive(Deserialize)]
struct ConfigRootfs {
    #[serde(rename = "type")]
    kind: String,
    diff_ids: Vec<String>,
}

fn decode(
    reader: Box<dyn crate::blob::BlobStreamReader>,
    media_type: &str,
) -> Result<Box<dyn AsyncRead + Send + Unpin>, OciImportError> {
    match media_type {
        "application/vnd.oci.image.layer.v1.tar"
        | "application/vnd.oci.image.layer.nondistributable.v1.tar"
        | "application/vnd.docker.image.rootfs.diff.tar" => Ok(reader),
        "application/vnd.oci.image.layer.v1.tar+gzip"
        | "application/vnd.oci.image.layer.nondistributable.v1.tar+gzip"
        | "application/vnd.docker.image.rootfs.diff.tar.gzip"
        | "application/vnd.docker.image.rootfs.foreign.diff.tar.gzip" => {
            let mut decoder = GzipDecoder::new(BufReader::new(reader));
            decoder.multiple_members(true);
            Ok(Box::new(decoder))
        }
        "application/vnd.oci.image.layer.v1.tar+zstd"
        | "application/vnd.oci.image.layer.nondistributable.v1.tar+zstd" => {
            let mut decoder = ZstdDecoder::new(BufReader::new(reader));
            decoder.multiple_members(true);
            Ok(Box::new(decoder))
        }
        other => Err(OciImportError::Unsupported(format!(
            "layer media type {other}"
        ))),
    }
}

/// Apply layers as their compressed bytes arrive, while the same bytes are
/// staged as the original OCI blobs by the caller.
pub(super) struct RootfsBuilder {
    tree: Tree,
    diff_ids: Vec<String>,
    next_layer: usize,
    limits: OciRootfsLimits,
}

impl RootfsBuilder {
    pub(super) fn new(
        config_bytes: &[u8],
        layer_count: usize,
        limits: OciRootfsLimits,
    ) -> Result<Self, OciImportError> {
        let config: ImageConfig = serde_json::from_slice(config_bytes)
            .map_err(|error| invalid(format!("invalid image rootfs config: {error}")))?;
        if config.rootfs.kind != "layers" || config.rootfs.diff_ids.len() != layer_count {
            return Err(invalid(
                "image rootfs config does not match the manifest layers",
            ));
        }
        Ok(Self {
            tree: Tree::default(),
            diff_ids: config.rootfs.diff_ids,
            next_layer: 0,
            limits,
        })
    }

    pub(super) async fn apply<R, PS, SS>(
        &mut self,
        mutation: &MutationSession<'_, PS, SS>,
        compressed: R,
        media_type: &str,
    ) -> Result<(), OciImportError>
    where
        R: AsyncRead + Unpin + Send + 'static,
        PS: BlobStore,
        SS: MetadataStore,
    {
        let diff_id = self
            .diff_ids
            .get(self.next_layer)
            .ok_or_else(|| invalid("too many image layers"))?;
        let reader = decode(Box::new(compressed), media_type)?;
        self.tree
            .apply(mutation, reader, diff_id, self.limits)
            .await?;
        self.next_layer += 1;
        Ok(())
    }

    pub(super) async fn finish<PS: BlobStore, SS: MetadataStore>(
        self,
        mutation: &MutationSession<'_, PS, SS>,
    ) -> Result<ObjectKey, OciImportError> {
        if self.next_layer != self.diff_ids.len() {
            return Err(invalid("missing image layer"));
        }
        self.tree.finish(mutation).await
    }
}

#[cfg(test)]
mod tests;
