//! Shared write context for chunk hashing, deduplication, and upload.

use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;

use object_store::{ObjectStore, path::Path};

use super::{chunk_path, head_exists, put_object};
use crate::blob::ChunkMeta;
use crate::blob::chunk_index::ChunkIndex;
use crate::blob::pack::PackedChunks;
use crate::digest::ChunkId;
use crate::metadata::{PinResource, WritePins};

/// All uploads in one writer borrow the same context. Owned chunk bytes
/// and their admission guards cross into the blocking pool; queued uploads
/// need no per-chunk Arc clones.
pub(super) struct ChunkUploader<'a> {
    pub object_store: &'a Arc<dyn ObjectStore>,
    pub base_path: &'a Path,
    pub chunk_index: &'a ChunkIndex,
    pub packed_chunks: Option<&'a Arc<PackedChunks>>,
    pub immutable_cache: bool,
    pub pins: &'a WritePins,
}

impl ChunkUploader<'_> {
    /// Identities known before any probe or upload. Pack locations must still
    /// be admitted separately when catalog lookup discovers them.
    pub(super) fn protection_resources(&self, digest: ChunkId) -> BTreeSet<PinResource> {
        let mut resources = BTreeSet::from([PinResource::Chunk(digest)]);
        if self.packed_chunks.is_none() {
            resources.insert(PinResource::StorageObject(
                chunk_path(self.base_path, &digest).to_string(),
            ));
        }
        resources
    }

    /// Overwrites hash one replacement chunk; streaming writes use HashBatch.
    pub async fn upload(&self, data: Vec<u8>) -> io::Result<ChunkMeta> {
        let (digest, data) = tokio::task::spawn_blocking(move || {
            let digest = ChunkId::new(blake3::hash(&data).into());
            (digest, data)
        })
        .await
        .map_err(io::Error::other)?;
        self.upload_prehashed(data, digest, ()).await
    }

    /// The digest must come from this writer's own hashing of these bytes.
    /// This is never a caller-supplied identity or a storage-backend assertion.
    pub async fn upload_prehashed(
        &self,
        data: Vec<u8>,
        digest: ChunkId,
        mut guard: impl Send + 'static,
    ) -> io::Result<ChunkMeta> {
        let size = data.len() as u64;
        // Local ownership drops bytes before the guard on early returns too.
        let mut data = Some(data);
        self.pins.protect(self.protection_resources(digest)).await?;
        let _claim = self.chunk_index.claim_upload(digest).await;
        let path = chunk_path(self.base_path, &digest);
        // Packed catalogs are authoritative. Loose stores also deduplicate
        // against writes from other processes, with at most one probe.
        let present = if let Some(packed) = self.packed_chunks {
            packed.probe_for_write(&digest).await?
        } else if self.pins.is_empty() {
            if self.chunk_index.contains(&digest) {
                true
            } else if head_exists(self.object_store, &path).await? {
                self.chunk_index.insert(digest);
                true
            } else {
                false
            }
        } else {
            let resource = PinResource::StorageObject(path.to_string());
            if self.pins.known_present(&resource) {
                true
            } else if head_exists(self.object_store, &path).await? {
                self.pins.remember_present(resource);
                true
            } else {
                false
            }
        };
        if !present {
            let data = data.take().expect("chunk bytes are consumed once");
            let (compressed, returned_guard) = tokio::task::spawn_blocking(move || {
                let compressed =
                    crate::compression::compress(&data, zstd::DEFAULT_COMPRESSION_LEVEL);
                (compressed, guard)
            })
            .await
            .map_err(io::Error::other)?;
            guard = returned_guard;
            let compressed = compressed.map_err(io::Error::other)?;
            if let Some(packed) = self.packed_chunks {
                packed
                    .put(ChunkMeta { digest, size }, compressed.into())
                    .await?;
            } else {
                put_object(self.object_store, &path, compressed, self.immutable_cache)
                    .await
                    .map_err(io::Error::other)?;
                // The pins protected this path before the write, so it stays
                // present for their lifetime exactly as after a probe hit.
                if !self.pins.is_empty() {
                    self.pins
                        .remember_present(PinResource::StorageObject(path.to_string()));
                }
            }
            self.chunk_index.insert(digest);
        }
        drop(data);
        drop(guard);
        Ok(ChunkMeta { digest, size })
    }
}
