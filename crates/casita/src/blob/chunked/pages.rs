//! Immutable, content-addressed metadata trees. References carry subtree byte
//! spans, so chunk lookup and copy-on-write edits visit only the affected paths.

use super::{ChunkedBlobStore, put_object, sharded_path};
use crate::{
    ChunkId, Digest,
    blob::{ChunkMeta, MAX_CHUNK_SIZE},
};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io,
    sync::Arc,
};

pub(super) const FANOUT: usize = 64;
const LEAF_READ_CONCURRENCY: usize = 8;
pub(super) const LEAF_BYTES: usize = 4096;
const HEADER: usize = 24;
const MAX_PAGE: usize = HEADER + LEAF_BYTES;
const MAGIC: &[u8; 8] = b"CASPAGE1";
const NODE_MAGIC: &[u8; 8] = b"CASNODE1";
pub(super) const DESCRIPTOR_BYTES: usize = 56;
pub(super) const CHUNKS: u8 = 0;
pub(super) const OUTBOARD: u8 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Root {
    pub kind: u8,
    pub height: u8,
    pub span: u64,
    pub hash: Digest,
}

pub(super) fn invalid() -> io::Error {
    io::Error::other(crate::blob::BlobIntegrityError::MetadataPage {
        reason: "invalid metadata page or descriptor".into(),
    })
}
fn integer(bytes: &[u8]) -> io::Result<u64> {
    Ok(u64::from_le_bytes(bytes.try_into().map_err(|_| invalid())?))
}
fn header(magic: &[u8; 8], kind: u8, height: u8, span: u64) -> Vec<u8> {
    let mut bytes = Vec::from(magic.as_slice());
    bytes.extend_from_slice(&[kind, height, 0, 0, 0, 0, 0, 0]);
    bytes.extend_from_slice(&span.to_le_bytes());
    bytes
}
impl Root {
    pub fn encode(self) -> Vec<u8> {
        let mut bytes = header(MAGIC, self.kind, self.height, self.span);
        bytes.extend_from_slice(self.hash.as_bytes());
        bytes
    }
    pub fn decode(bytes: &[u8]) -> io::Result<Option<Self>> {
        if !bytes.starts_with(MAGIC) {
            return Ok(None);
        }
        if bytes.len() != DESCRIPTOR_BYTES {
            return Err(invalid());
        }
        let root = Self {
            kind: bytes[8],
            height: bytes[9],
            span: integer(&bytes[16..24])?,
            hash: Digest::try_from(&bytes[24..]).map_err(|_| invalid())?,
        };
        if root.kind > OUTBOARD || root.height > 11 || root.span == 0 || bytes[10..16] != [0; 6] {
            return Err(invalid());
        }
        Ok(Some(root))
    }
}

#[derive(Clone)]
pub(super) struct Pages {
    pub objects: Arc<dyn ObjectStore>,
    pub base: Path,
    pub immutable: bool,
}
impl From<&ChunkedBlobStore> for Pages {
    fn from(store: &ChunkedBlobStore) -> Self {
        Self {
            objects: store.object_store.clone(),
            base: store.base_path.clone(),
            immutable: store.immutable_cache,
        }
    }
}

#[derive(Clone)]
pub(super) enum Node {
    Chunks(Vec<ChunkMeta>),
    Bytes(Bytes),
    Branch(Vec<Root>),
}
impl Pages {
    pub fn path(&self, hash: &Digest) -> Path {
        sharded_path(&self.base, "pages", hash)
    }

    pub async fn load(&self, root: Root) -> io::Result<Node> {
        let bytes = self
            .objects
            .get_range(&self.path(&root.hash), 0..(MAX_PAGE + 1) as u64)
            .await
            .map_err(|error| match error {
                object_store::Error::NotFound { .. } => {
                    io::Error::other(crate::blob::BlobIntegrityError::MetadataPage {
                        reason: format!("missing referenced page {}", root.hash),
                    })
                }
                error => io::Error::other(error),
            })?;
        self.decode(root, bytes)
    }
    pub fn decode(&self, root: Root, bytes: Bytes) -> io::Result<Node> {
        if root.kind > OUTBOARD
            || root.height > 11
            || root.span == 0
            || bytes.len() < HEADER
            || bytes.len() > MAX_PAGE
            || Digest::from(blake3::hash(&bytes)) != root.hash
            || bytes[..HEADER] != header(NODE_MAGIC, root.kind, root.height, root.span)
        {
            return Err(invalid());
        }
        let payload = bytes.slice(HEADER..);
        if root.height == 0 && root.kind == OUTBOARD {
            if payload.len() as u64 != root.span || payload.is_empty() {
                return Err(invalid());
            }
            return Ok(Node::Bytes(payload));
        }
        if payload.is_empty() || !payload.len().is_multiple_of(40) || payload.len() / 40 > FANOUT {
            return Err(invalid());
        }
        let mut sum = 0u64;
        let mut entries = Vec::new();
        for entry in payload.chunks_exact(40) {
            let span = integer(&entry[..8])?;
            if span == 0 || (root.height == 0 && span > MAX_CHUNK_SIZE) {
                return Err(invalid());
            }
            sum = sum.checked_add(span).ok_or_else(invalid)?;
            entries.push((span, Digest::try_from(&entry[8..]).map_err(|_| invalid())?));
        }
        if sum != root.span {
            return Err(invalid());
        }
        Ok(if root.height == 0 {
            Node::Chunks(
                entries
                    .into_iter()
                    .map(|(size, hash)| ChunkMeta {
                        size,
                        digest: ChunkId::new(hash),
                    })
                    .collect(),
            )
        } else {
            Node::Branch(
                entries
                    .into_iter()
                    .map(|(span, hash)| Root {
                        kind: root.kind,
                        height: root.height - 1,
                        span,
                        hash,
                    })
                    .collect(),
            )
        })
    }
    async fn save(&self, kind: u8, height: u8, span: u64, payload: &[u8]) -> io::Result<Root> {
        let mut bytes = header(NODE_MAGIC, kind, height, span);
        bytes.extend_from_slice(payload);
        let root = Root {
            kind,
            height,
            span,
            hash: blake3::hash(&bytes).into(),
        };
        // Use the same strict bounds for producer and consumer.
        self.decode(root, Bytes::copy_from_slice(&bytes))?;
        put_object(&self.objects, &self.path(&root.hash), bytes, self.immutable)
            .await
            .map_err(io::Error::other)?;
        Ok(root)
    }
    pub async fn chunks_leaf(&self, chunks: &[ChunkMeta]) -> io::Result<Root> {
        let mut payload = Vec::new();
        let mut span = 0u64;
        for c in chunks {
            span = span.checked_add(c.size).ok_or_else(invalid)?;
            payload.extend_from_slice(&c.size.to_le_bytes());
            payload.extend_from_slice(c.digest.digest().as_bytes());
        }
        self.save(CHUNKS, 0, span, &payload).await
    }
    pub async fn bytes_leaf(&self, bytes: &[u8]) -> io::Result<Root> {
        self.save(OUTBOARD, 0, bytes.len() as u64, bytes).await
    }
    async fn branch(&self, children: &[Root]) -> io::Result<Root> {
        let first = children.first().ok_or_else(invalid)?;
        let mut payload = Vec::new();
        let mut span = 0u64;
        for child in children {
            if child.height != first.height || child.kind != first.kind {
                return Err(invalid());
            }
            span = span.checked_add(child.span).ok_or_else(invalid)?;
            payload.extend_from_slice(&child.span.to_le_bytes());
            payload.extend_from_slice(child.hash.as_bytes());
        }
        self.save(first.kind, first.height + 1, span, &payload)
            .await
    }
    pub async fn build_chunks(&self, chunks: &[ChunkMeta]) -> io::Result<Root> {
        let mut builder = Builder::new(self.clone());
        for group in chunks.chunks(FANOUT) {
            builder.push(self.chunks_leaf(group).await?).await?;
        }
        builder.finish().await
    }
    pub async fn build_bytes(&self, input: &mut impl io::Read) -> io::Result<Root> {
        let mut builder = Builder::new(self.clone());
        let mut buffer = vec![0; LEAF_BYTES];
        loop {
            let mut count = 0;
            while count < buffer.len() {
                match input.read(&mut buffer[count..]) {
                    Ok(0) => break,
                    Ok(n) => count += n,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e),
                }
            }
            if count == 0 {
                break;
            }
            builder
                .push(self.bytes_leaf(&buffer[..count]).await?)
                .await?;
        }
        builder.finish().await
    }
    pub async fn collect_chunks(&self, root: Root) -> io::Result<Vec<ChunkMeta>> {
        let mut stack = vec![root];
        let mut result = Vec::new();
        while let Some(node) = stack.pop() {
            match self.load(node).await? {
                Node::Chunks(chunks) => result.extend(chunks),
                Node::Branch(children) if node.height == 1 => {
                    // Sibling leaves are independent immutable objects. Keep
                    // file order, verification, and a bounded request/buffer
                    // budget even when the backend completes out of order.
                    let mut leaves = stream::iter(children)
                        .map(|child| self.load(child))
                        .buffered(LEAF_READ_CONCURRENCY);
                    while let Some(leaf) = leaves.try_next().await? {
                        match leaf {
                            Node::Chunks(chunks) => result.extend(chunks),
                            _ => return Err(invalid()),
                        }
                    }
                }
                Node::Branch(children) => stack.extend(children.into_iter().rev()),
                Node::Bytes(_) => return Err(invalid()),
            }
        }
        Ok(result)
    }
    pub async fn mark(&self, root: Root, live: &mut BTreeSet<Digest>) -> io::Result<()> {
        let mut stack = vec![root];
        while let Some(root) = stack.pop() {
            if !live.insert(root.hash) {
                continue;
            }
            if let Node::Branch(children) = self.load(root).await? {
                stack.extend(children);
            }
        }
        Ok(())
    }
}

/// Incremental canonical chunk manifest. Small manifests retain the flat wire
/// format; larger ones use the same leaf groups and tree shape as build_chunks.
/// Page writes are protected by the existing pinned object store. The caller
/// publishes the blob's manifest only after every chunk and page has succeeded.
pub(super) struct ChunkManifest {
    pages: Pages,
    buffer: Vec<ChunkMeta>,
    builder: Option<Builder>,
    count: usize,
    size: u64,
    first: Option<ChunkId>,
}
impl ChunkManifest {
    pub(super) fn new(pages: Pages) -> Self {
        Self {
            pages,
            buffer: Vec::new(),
            builder: None,
            count: 0,
            size: 0,
            first: None,
        }
    }
    pub(super) fn size(&self) -> u64 {
        self.size
    }
    pub(super) fn single_chunk(&self) -> Option<ChunkId> {
        if self.count == 1 { self.first } else { None }
    }
    pub(super) async fn push(&mut self, chunk: ChunkMeta) -> io::Result<()> {
        self.size = self.size.checked_add(chunk.size).ok_or_else(invalid)?;
        if self.count == 0 {
            self.first = Some(chunk.digest);
        }
        self.count += 1;
        // Keep exactly FANOUT entries flat until a further entry proves that
        // the paged representation is required.
        if self.builder.is_none() && self.buffer.len() == FANOUT {
            self.builder = Some(Builder::new(self.pages.clone()));
            self.flush_leaf().await?;
        }
        self.buffer.push(chunk);
        if self.builder.is_some() && self.buffer.len() == FANOUT {
            self.flush_leaf().await?;
        }
        Ok(())
    }
    async fn flush_leaf(&mut self) -> io::Result<()> {
        let leaf = self.pages.chunks_leaf(&self.buffer).await?;
        self.builder
            .as_mut()
            .expect("paged manifest")
            .push(leaf)
            .await?;
        self.buffer.clear();
        Ok(())
    }
    pub(super) async fn finish(mut self) -> io::Result<Vec<u8>> {
        if self.builder.is_none() {
            return Ok(super::manifest::encode_manifest(&self.buffer));
        }
        if !self.buffer.is_empty() {
            self.flush_leaf().await?;
        }
        Ok(self
            .builder
            .take()
            .expect("paged manifest")
            .finish()
            .await?
            .encode())
    }
}

/// Carry completed groups upward; ingestion retains at most 64 references per level.
struct Builder {
    pages: Pages,
    levels: Vec<Vec<Root>>,
}
impl Builder {
    fn new(pages: Pages) -> Self {
        Self {
            pages,
            levels: Vec::new(),
        }
    }
    async fn push(&mut self, mut root: Root) -> io::Result<()> {
        loop {
            let level = root.height as usize;
            if self.levels.len() <= level {
                self.levels.resize_with(level + 1, Vec::new);
            }
            self.levels[level].push(root);
            if self.levels[level].len() < FANOUT {
                return Ok(());
            }
            root = self
                .pages
                .branch(&std::mem::take(&mut self.levels[level]))
                .await?;
        }
    }
    async fn finish(mut self) -> io::Result<Root> {
        for level in 0..12 {
            if level >= self.levels.len() {
                return Err(invalid());
            }
            let entries = std::mem::take(&mut self.levels[level]);
            if entries.is_empty() {
                continue;
            }
            if entries.len() == 1 && self.levels[level + 1..].iter().all(Vec::is_empty) {
                return Ok(entries[0]);
            }
            let parent = self.pages.branch(&entries).await?;
            self.push(parent).await?;
        }
        Err(invalid())
    }
}

/// A small per-operation cache, never proportional to file size or edit history.
pub(super) struct Cursor {
    pub pages: Pages,
    pub root: Root,
    cache: VecDeque<(Root, Node)>,
    _pins: crate::metadata::WritePins,
}
impl Cursor {
    pub async fn new(store: &ChunkedBlobStore, root: Root) -> io::Result<Self> {
        let pages = Pages::from(store);
        let pins = store.pins.capture();
        pins.protect(BTreeSet::from([
            crate::metadata::PinResource::StorageObject(pages.path(&root.hash).to_string()),
        ]))
        .await?;
        Ok(Self {
            pages,
            root,
            cache: VecDeque::new(),
            _pins: pins,
        })
    }
    async fn load(&mut self, root: Root) -> io::Result<Node> {
        if let Some((_, node)) = self.cache.iter().find(|(cached, _)| *cached == root) {
            return Ok(node.clone());
        }
        let node = self.pages.load(root).await?;
        if self.cache.len() == 32 {
            self.cache.pop_front();
        }
        self.cache.push_back((root, node.clone()));
        Ok(node)
    }
    async fn leaf(&mut self, offset: u64) -> io::Result<(u64, Node)> {
        if offset >= self.root.span {
            return Err(invalid());
        }
        let mut root = self.root;
        let mut start = 0;
        loop {
            match self.load(root).await? {
                Node::Branch(children) => {
                    let mut found = None;
                    for child in children {
                        if offset < start + child.span {
                            found = Some(child);
                            break;
                        }
                        start += child.span;
                    }
                    root = found.ok_or_else(invalid)?;
                }
                leaf => return Ok((start, leaf)),
            }
        }
    }
    pub async fn chunk(&mut self, offset: u64) -> io::Result<(u64, ChunkMeta)> {
        let (mut start, Node::Chunks(chunks)) = self.leaf(offset).await? else {
            return Err(invalid());
        };
        for chunk in chunks {
            if offset < start + chunk.size {
                return Ok((start, chunk));
            }
            start += chunk.size;
        }
        Err(invalid())
    }
    pub async fn edit(&mut self, edits: &BTreeMap<u64, Edit>) -> io::Result<Root> {
        if edits.keys().any(|offset| *offset >= self.root.span) {
            return Err(invalid());
        }
        self.edit_node(self.root, 0, edits).await
    }
    fn edit_node<'a>(
        &'a mut self,
        root: Root,
        start: u64,
        edits: &'a BTreeMap<u64, Edit>,
    ) -> futures::future::BoxFuture<'a, io::Result<Root>> {
        Box::pin(async move {
            if edits.range(start..start + root.span).next().is_none() {
                return Ok(root);
            }
            match self.load(root).await? {
                Node::Branch(mut children) => {
                    let mut position = start;
                    for child in &mut children {
                        *child = self.edit_node(*child, position, edits).await?;
                        position += child.span;
                    }
                    self.pages.branch(&children).await
                }
                Node::Chunks(mut chunks) => {
                    let mut position = start;
                    for chunk in &mut chunks {
                        let next = position + chunk.size;
                        for (offset, edit) in edits.range(position..next) {
                            let Edit::Chunk(new) = edit else {
                                return Err(invalid());
                            };
                            if *offset != position || new.size != chunk.size {
                                return Err(invalid());
                            }
                            *chunk = new.clone();
                        }
                        position = next;
                    }
                    self.pages.chunks_leaf(&chunks).await
                }
                Node::Bytes(bytes) => {
                    let mut bytes = bytes.to_vec();
                    for (offset, edit) in edits.range(start..start + root.span) {
                        let Edit::Bytes(new) = edit else {
                            return Err(invalid());
                        };
                        let offset = (*offset - start) as usize;
                        bytes
                            .get_mut(offset..offset + new.len())
                            .ok_or_else(invalid)?
                            .copy_from_slice(new);
                    }
                    self.pages.bytes_leaf(&bytes).await
                }
            }
        })
    }
}
pub(super) enum Edit {
    Chunk(ChunkMeta),
    Bytes(Bytes),
}
impl iroh_io::AsyncSliceReader for Cursor {
    async fn size(&mut self) -> io::Result<u64> {
        Ok(self.root.span)
    }
    async fn read_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        let range = crate::verified::covered(offset, len as u64, self.root.span)?;
        let mut position = range.start;
        let mut result = Vec::with_capacity(len);
        while position < range.end {
            let (start, Node::Bytes(bytes)) = self.leaf(position).await? else {
                return Err(invalid());
            };
            let skip = (position - start) as usize;
            let take = (range.end - position).min((bytes.len() - skip) as u64) as usize;
            result.extend_from_slice(&bytes[skip..skip + take]);
            position += take as u64;
        }
        Ok(result.into())
    }
}

/// Probe only a bounded prefix. Old flat metadata is deliberately not read here.
pub(super) async fn descriptor(
    store: &ChunkedBlobStore,
    path: &Path,
    kind: u8,
) -> io::Result<Option<Root>> {
    if kind == OUTBOARD {
        let digest = crate::BlobId::new(super::digest_from_location(path)?);
        if let Some(bytes) = store.packed_outboard_root(&digest, None).await? {
            let root = Root::decode(&bytes)?;
            if root.is_some_and(|root| root.kind != kind) {
                return Err(invalid());
            }
            return Ok(root);
        }
    }
    loose_descriptor(store, path, kind).await
}

pub(super) async fn loose_descriptor(
    store: &ChunkedBlobStore,
    path: &Path,
    kind: u8,
) -> io::Result<Option<Root>> {
    Ok(match probe_loose_descriptor(store, path, kind).await? {
        LooseDescriptor::Paged(root) => Some(root),
        LooseDescriptor::Missing | LooseDescriptor::Flat => None,
    })
}

/// One bounded observation of loose metadata. A nonpaged prefix still needs
/// flat-manifest validation; only an actual NotFound permits the bare fallback.
pub(super) enum LooseDescriptor {
    Missing,
    Flat,
    Paged(Root),
}

pub(super) async fn probe_loose_descriptor(
    store: &ChunkedBlobStore,
    path: &Path,
    kind: u8,
) -> io::Result<LooseDescriptor> {
    let bytes = match store
        .object_store
        .get_range(path, 0..(DESCRIPTOR_BYTES + 1) as u64)
        .await
    {
        Ok(bytes) => bytes,
        Err(object_store::Error::NotFound { .. }) => return Ok(LooseDescriptor::Missing),
        Err(e) => return Err(io::Error::other(e)),
    };
    let root = Root::decode(&bytes)?;
    if root.is_some_and(|root| root.kind != kind) {
        return Err(invalid());
    }
    Ok(match root {
        Some(root) => LooseDescriptor::Paged(root),
        None => LooseDescriptor::Flat,
    })
}

/// Random-access plaintext through the indexed chunk tree, retaining one chunk.
pub(super) struct Data {
    store: ChunkedBlobStore,
    pub cursor: Cursor,
    start: u64,
    current: Bytes,
}
impl Data {
    pub async fn new(store: &ChunkedBlobStore, root: Root) -> io::Result<Self> {
        if root.kind != CHUNKS {
            return Err(invalid());
        }
        Ok(Self {
            store: store.clone(),
            cursor: Cursor::new(store, root).await?,
            start: 0,
            current: Bytes::new(),
        })
    }
}
impl iroh_io::AsyncSliceReader for Data {
    async fn size(&mut self) -> io::Result<u64> {
        Ok(self.cursor.root.span)
    }
    async fn read_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        use crate::blob::chunked_reader::ChunkSource;
        let range = crate::verified::covered(offset, len as u64, self.cursor.root.span)?;
        let mut position = range.start;
        let mut result = Vec::with_capacity(len);
        while position < range.end {
            if self.current.is_empty()
                || position < self.start
                || position >= self.start + self.current.len() as u64
            {
                let (start, chunk) = self.cursor.chunk(position).await?;
                self.current = self.store.fetch_chunk(chunk.digest, chunk.size).await?;
                self.start = start;
            }
            let from = (position - self.start) as usize;
            let take = (range.end - position).min((self.current.len() - from) as u64) as usize;
            result.extend_from_slice(&self.current[from..from + take]);
            position += take as u64;
        }
        Ok(result.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh_io::AsyncSliceReader;

    #[tokio::test]
    async fn chunk_tree_boundaries_share_unchanged_pages_and_bound_the_cache() {
        let store = ChunkedBlobStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            Path::default(),
            65536,
        );
        let pages = Pages::from(&store);
        for count in [1usize, 63, 64, 65, 4095, 4096, 4097] {
            let chunks: Vec<_> = (0..count)
                .map(|i| ChunkMeta {
                    digest: ChunkId::new(blake3::hash(&i.to_le_bytes()).into()),
                    size: 65536 + i as u64,
                })
                .collect();
            let root = pages.build_chunks(&chunks).await.unwrap();
            let mut cursor = Cursor::new(&store, root).await.unwrap();
            let at = count / 2;
            let start: u64 = chunks[..at].iter().map(|c| c.size).sum();
            assert_eq!(
                cursor.chunk(start + 3).await.unwrap(),
                (start, chunks[at].clone())
            );
            let mut expected = chunks.clone();
            expected[at].digest = ChunkId::new(blake3::hash(b"new chunk").into());
            let updated = cursor
                .edit(&BTreeMap::from([(
                    start,
                    Edit::Chunk(expected[at].clone()),
                )]))
                .await
                .unwrap();
            assert_eq!(pages.collect_chunks(updated).await.unwrap(), expected);
            assert_eq!(pages.collect_chunks(root).await.unwrap(), chunks);
            let mut old_pages = BTreeSet::new();
            let mut new_pages = BTreeSet::new();
            pages.mark(root, &mut old_pages).await.unwrap();
            pages.mark(updated, &mut new_pages).await.unwrap();
            assert_eq!(
                new_pages.difference(&old_pages).count(),
                root.height as usize + 1
            );
            assert!(cursor.cache.len() <= 32);
        }
    }

    #[tokio::test]
    async fn byte_tree_boundaries_and_cross_leaf_reads() {
        let store = ChunkedBlobStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            Path::default(),
            65536,
        );
        let pages = Pages::from(&store);
        for size in [4095, 4096, 4097, 4096 * 64 - 1, 4096 * 64, 4096 * 64 + 1] {
            let original: Vec<u8> = (0..size).map(|i| (i * 71) as u8).collect();
            let root = pages.build_bytes(&mut original.as_slice()).await.unwrap();
            let mut cursor = Cursor::new(&store, root).await.unwrap();
            assert_eq!(
                cursor.read_at(4093, 2).await.unwrap(),
                &original[4093..4095]
            );
            let at = (size / 2 / 64 * 64) as u64;
            let updated = cursor
                .edit(&BTreeMap::from([(
                    at,
                    Edit::Bytes(Bytes::from(vec![7; 64])),
                )]))
                .await
                .unwrap();
            let mut expected = original.clone();
            expected[at as usize..at as usize + 64].fill(7);
            assert_eq!(
                Cursor::new(&store, updated)
                    .await
                    .unwrap()
                    .read_at(0, size)
                    .await
                    .unwrap(),
                expected
            );
            assert_eq!(cursor.read_at(0, size).await.unwrap(), original);
        }
    }

    #[tokio::test]
    async fn forged_descriptors_pages_and_bounds_are_rejected() {
        let store = ChunkedBlobStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            Path::default(),
            65536,
        );
        let pages = Pages::from(&store);
        let root = pages.bytes_leaf(&[1; 64]).await.unwrap();
        let mut descriptor = root.encode();
        descriptor.push(0);
        assert!(Root::decode(&descriptor).is_err());
        let mut wrong = root;
        wrong.span += 1;
        assert!(pages.load(wrong).await.is_err());
        let mut cursor = Cursor::new(&store, root).await.unwrap();
        assert!(cursor.read_at(u64::MAX, 2).await.is_err());
        assert!(
            cursor
                .edit(&BTreeMap::from([(
                    63,
                    Edit::Bytes(Bytes::from_static(b"xx"))
                )]))
                .await
                .is_err()
        );
        put_object(&pages.objects, &pages.path(&root.hash), vec![9; 65], false)
            .await
            .unwrap();
        assert!(pages.load(root).await.is_err());
    }
}

#[cfg(test)]
mod parallel_tests;

#[cfg(test)]
mod streaming_manifest_tests {
    use super::*;

    #[tokio::test]
    async fn streaming_chunk_manifests_preserve_flat_and_paged_bytes_with_bounded_buffers() {
        let pages = Pages {
            objects: Arc::new(object_store::memory::InMemory::new()),
            base: Path::default(),
            immutable: false,
        };
        for count in [0usize, 1, 63, 64, 65, 4095, 4096, 4097, 10000] {
            let chunks: Vec<_> = (0..count)
                .map(|i| ChunkMeta {
                    digest: ChunkId::new(blake3::hash(&i.to_le_bytes()).into()),
                    size: 1024 + i as u64,
                })
                .collect();
            let expected = if count <= FANOUT {
                super::super::manifest::encode_manifest(&chunks)
            } else {
                pages.build_chunks(&chunks).await.unwrap().encode()
            };
            let mut streamed = ChunkManifest::new(pages.clone());
            for chunk in &chunks {
                streamed.push(chunk.clone()).await.unwrap();
                assert!(streamed.buffer.len() <= FANOUT);
                if let Some(builder) = &streamed.builder {
                    assert!(builder.levels.iter().all(|level| level.len() < FANOUT));
                }
            }
            assert_eq!(streamed.size(), chunks.iter().map(|c| c.size).sum::<u64>());
            assert_eq!(
                streamed.single_chunk(),
                if count == 1 {
                    Some(chunks[0].digest)
                } else {
                    None
                }
            );
            assert_eq!(streamed.finish().await.unwrap(), expected, "{count} chunks");
        }
    }
}
