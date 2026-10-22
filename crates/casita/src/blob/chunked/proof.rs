//! Read Bao proofs and flat manifest rows on demand, with bounded buffers.

use super::ChunkedBlobStore;
use crate::blob::chunked_reader::ChunkSource;
use crate::blob::{BlobReader, BlobStore};
use crate::error::Error;
use crate::{
    BlobId, ChunkId, Digest,
    blob::{BlobStreamReader, ChunkMeta, MAX_CHUNK_SIZE},
};
use bao_tree::{BaoTree, io::outboard::PreOrderOutboard};
use bytes::Bytes;
use object_store::{ObjectStoreExt, path::Path};
use std::io;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

pub(super) struct OutboardSource {
    inline: Option<Bytes>,
    paged: Option<super::pages::Cursor>,
    store: ChunkedBlobStore,
    path: Path,
    size: u64,
}

impl OutboardSource {
    pub async fn new(
        store: &ChunkedBlobStore,
        digest: &BlobId,
        size: u64,
        catalog: Option<&[u8]>,
    ) -> io::Result<Self> {
        let path = store.outboard_path(digest);
        // A single Bao group needs no outboard reads. Explicit preparation
        // may have stored an empty object, which some range APIs reject.
        if size == 0 {
            return Ok(Self {
                store: store.clone(),
                inline: None,
                path,
                size,
                paged: None,
            });
        }
        let inline = store.packed_outboard_root(digest, catalog).await?;
        let root = match &inline {
            Some(bytes) => super::pages::Root::decode(bytes)?,
            None => super::pages::loose_descriptor(store, &path, super::pages::OUTBOARD).await?,
        };
        let paged = match root {
            Some(root) => {
                if root.span != size || root.kind != super::pages::OUTBOARD {
                    return Err(io::Error::other("outboard size mismatch"));
                }
                Some(super::pages::Cursor::new(store, root).await?)
            }
            None => None,
        };
        if paged.is_none()
            && inline
                .as_ref()
                .is_some_and(|bytes| bytes.len() as u64 != size)
        {
            return Err(io::Error::other("packed outboard size mismatch"));
        }
        Ok(Self {
            inline,
            store: store.clone(),
            path,
            size,
            paged,
        })
    }
}

impl iroh_io::AsyncSliceReader for OutboardSource {
    async fn read_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        let end = offset
            .checked_add(len as u64)
            .filter(|end| *end <= self.size)
            .ok_or_else(|| io::Error::other("outboard read outside tree"))?;
        if let Some(paged) = &mut self.paged {
            return paged.read_at(offset, len).await;
        }
        if let Some(bytes) = &self.inline {
            return Ok(bytes.slice(offset as usize..end as usize));
        }
        self.store
            .object_store
            .get_range(&self.path, offset..end)
            .await
            .map_err(io::Error::other)
    }
    async fn size(&mut self) -> io::Result<u64> {
        Ok(self.size)
    }
}

struct ProofData {
    paged: Option<super::pages::Data>,
    store: ChunkedBlobStore,
    path: Path,
    count: u64,
    next_row: u64,
    rows: Bytes,
    bare: Option<ChunkMeta>,
    start: u64,
    current: Bytes,
    size: u64,
}

impl ProofData {
    async fn advance(&mut self) -> io::Result<bool> {
        let meta = if let Some(meta) = self.bare.take() {
            meta
        } else {
            if self.next_row == self.count {
                return Ok(false);
            }
            if self.rows.is_empty() {
                let count = (self.count - self.next_row).min(256);
                let begin = 8 + self.next_row * 40;
                self.rows = self
                    .store
                    .object_store
                    .get_range(&self.path, begin..begin + count * 40)
                    .await
                    .map_err(io::Error::other)?;
                if self.rows.len() != count as usize * 40 {
                    return Err(io::Error::other("short manifest page"));
                }
            }
            let row = self.rows.split_to(40);
            self.next_row += 1;
            ChunkMeta {
                digest: ChunkId::new(Digest::try_from(&row[..32]).map_err(io::Error::other)?),
                size: u64::from_le_bytes(row[32..].try_into().unwrap()),
            }
        };
        if meta.size == 0 || meta.size > MAX_CHUNK_SIZE {
            return Err(io::Error::other("invalid proof source chunk size"));
        }
        self.start = self
            .start
            .checked_add(self.current.len() as u64)
            .ok_or_else(|| io::Error::other("manifest size overflow"))?;
        self.current = self.store.fetch_chunk(meta.digest, meta.size).await?;
        Ok(true)
    }
}

impl iroh_io::AsyncSliceReader for ProofData {
    async fn read_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        let end = offset
            .checked_add(len as u64)
            .filter(|end| *end <= self.size)
            .ok_or_else(|| io::Error::other("proof data range outside file"))?;
        if let Some(paged) = &mut self.paged {
            return paged.read_at(offset, len).await;
        }
        if offset < self.start {
            return Err(io::Error::other("proof source cannot rewind"));
        }
        let mut result = Vec::with_capacity(len);
        let mut position = offset;
        while position < end {
            while position >= self.start + self.current.len() as u64 {
                if !self.advance().await? {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
            }
            let from = (position - self.start) as usize;
            let count = (end - position).min((self.current.len() - from) as u64) as usize;
            result.extend_from_slice(&self.current[from..from + count]);
            position += count as u64;
        }
        Ok(result.into())
    }
    async fn size(&mut self) -> io::Result<u64> {
        Ok(self.size)
    }
}

// Adapt the same frozen payload plan used by ordinary object readers.
struct ScopedProofData {
    reader: Box<dyn BlobReader>,
    size: u64,
}

impl iroh_io::AsyncSliceReader for ScopedProofData {
    async fn read_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        offset
            .checked_add(len as u64)
            .filter(|end| *end <= self.size)
            .ok_or_else(|| io::Error::other("proof data range outside file"))?;
        self.reader.seek(io::SeekFrom::Start(offset)).await?;
        let mut bytes = vec![0; len];
        self.reader.read_exact(&mut bytes).await?;
        Ok(bytes.into())
    }

    async fn size(&mut self) -> io::Result<u64> {
        Ok(self.size)
    }
}

impl ChunkedBlobStore {
    pub(super) async fn scoped_proof_reader(
        &self,
        digest: &BlobId,
        size: u64,
        pin: crate::metadata::DataPinLease,
        catalog: Option<&[u8]>,
    ) -> Result<Option<Box<dyn BlobStreamReader>>, Error> {
        let Some(reader) = self.open_read_scoped(digest, pin.clone(), catalog).await? else {
            return Ok(None);
        };
        let tree = BaoTree::new(size, crate::verified::BLOCK_SIZE);
        let source = OutboardSource::new(self, digest, tree.outboard_size(), catalog).await?;
        let mut resources = std::collections::BTreeSet::new();
        if tree.outboard_size() > 0 {
            resources.insert(crate::metadata::PinResource::StorageObject(
                source.path.to_string(),
            ));
        }
        if let Some(cursor) = &source.paged {
            // Page GC traces descendants of retained page roots.
            resources.insert(crate::metadata::PinResource::StorageObject(
                cursor.pages.path(&cursor.root.hash).to_string(),
            ));
        }
        pin.protect(resources).await.map_err(io::Error::other)?;
        let outboard = PreOrderOutboard {
            root: bao_tree::blake3::Hash::from(*digest.digest().as_bytes()),
            tree,
            data: source,
        };
        Ok(Some(crate::verified::stream::produced(
            move |writer| async move {
                let _pin = pin;
                bao_tree::io::fsm::encode_ranges_validated(
                    ScopedProofData { reader, size },
                    outboard,
                    &bao_tree::ChunkRanges::all(),
                    iroh_io::TokioStreamWriter(writer),
                )
                .await
                .map_err(io::Error::other)
            },
        )))
    }

    pub(super) async fn proof_reader(
        &self,
        digest: &BlobId,
        size: u64,
    ) -> Result<Option<Box<dyn BlobStreamReader>>, Error> {
        let path = self.blob_path(digest);
        use super::pages::LooseDescriptor;
        // Absence is observed once per open and never cached: an open
        // overlapping first publication may miss the manifest, and a fresh
        // open probes again.
        let (paged, metadata) =
            match super::pages::probe_loose_descriptor(self, &path, super::pages::CHUNKS).await? {
                LooseDescriptor::Paged(root) => {
                    if root.span != size {
                        return Err(io::Error::other("manifest size mismatch").into());
                    }
                    (Some(super::pages::Data::new(self, root).await?), None)
                }
                LooseDescriptor::Missing => (None, None),
                // Flat metadata needs its length, and may be removed meanwhile.
                LooseDescriptor::Flat => match self.object_store.head(&path).await {
                    Ok(meta) => (None, Some(meta)),
                    Err(object_store::Error::NotFound { .. }) => (None, None),
                    Err(error) => return Err(io::Error::other(error).into()),
                },
            };
        let (count, bare) = if paged.is_some() {
            (0, None)
        } else {
            match metadata {
                Some(meta) => {
                    if meta.size < 8 || (meta.size - 8) % 40 != 0 {
                        return Err(io::Error::other("invalid manifest length").into());
                    }
                    let header = self
                        .object_store
                        .get_range(&path, 0..8)
                        .await
                        .map_err(io::Error::other)?;
                    let count =
                        u64::from_le_bytes(header.as_ref().try_into().map_err(io::Error::other)?);
                    if count != (meta.size - 8) / 40 {
                        return Err(io::Error::other("invalid manifest count").into());
                    }
                    (count, None)
                }
                None => {
                    if !self.chunk_present(super::single_chunk_id(*digest)).await? {
                        return Ok(None);
                    }
                    (
                        0,
                        Some(ChunkMeta {
                            digest: super::single_chunk_id(*digest),
                            size,
                        }),
                    )
                }
            }
        };
        let tree = BaoTree::new(size, crate::verified::BLOCK_SIZE);
        let outboard = PreOrderOutboard {
            root: bao_tree::blake3::Hash::from(*digest.digest().as_bytes()),
            tree,
            data: OutboardSource::new(self, digest, tree.outboard_size(), None).await?,
        };
        let data = ProofData {
            paged,
            store: self.clone(),
            path,
            count,
            next_row: 0,
            rows: Bytes::new(),
            bare,
            start: 0,
            current: Bytes::new(),
            size,
        };
        Ok(Some(crate::verified::stream::produced(
            move |writer| async move {
                bao_tree::io::fsm::encode_ranges_validated(
                    data,
                    outboard,
                    &bao_tree::ChunkRanges::all(),
                    iroh_io::TokioStreamWriter(writer),
                )
                .await
                .map_err(io::Error::other)
            },
        )))
    }
}
