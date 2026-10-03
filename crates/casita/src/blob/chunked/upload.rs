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

/// All uploads in one writer borrow the same context. Only owned chunk bytes
/// cross into the blocking pool; queued uploads need no per-chunk Arc clones.
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

    pub async fn upload(&self, data: Vec<u8>) -> io::Result<ChunkMeta> {
        let (digest, data) = tokio::task::spawn_blocking(move || {
            let digest = ChunkId::new(blake3::hash(&data).into());
            (digest, data)
        })
        .await
        .map_err(io::Error::other)?;
        self.upload_prehashed(data, digest).await
    }

    /// The digest must come from this writer's own hashing of these bytes.
    /// This is never a caller-supplied identity or a storage-backend assertion.
    pub async fn upload_prehashed(&self, data: Vec<u8>, digest: ChunkId) -> io::Result<ChunkMeta> {
        let size = data.len() as u64;
        self.pins.protect(self.protection_resources(digest)).await?;
        let _claim = self.chunk_index.claim_upload(digest).await;
        let present = if let Some(packed) = self.packed_chunks {
            packed.probe_for_write(&digest).await?
        } else {
            let path = chunk_path(self.base_path, &digest);
            let resource = PinResource::StorageObject(path.to_string());
            if self.pins.is_empty() {
                self.chunk_index.contains(&digest)
            } else if self.pins.known_present(&resource) {
                true
            } else if head_exists(self.object_store, &path).await? {
                self.pins.remember_present(resource);
                true
            } else {
                false
            }
        };
        if !present {
            let path = chunk_path(self.base_path, &digest);
            // Packed catalogs are authoritative; loose stores also deduplicate
            // against writes from other processes.
            let remote_present = if self.packed_chunks.is_some() {
                false
            } else {
                head_exists(self.object_store, &path).await?
            };
            if !remote_present {
                let compressed = tokio::task::spawn_blocking(move || {
                    crate::compression::compress(&data, zstd::DEFAULT_COMPRESSION_LEVEL)
                })
                .await
                .map_err(io::Error::other)?
                .map_err(io::Error::other)?;
                if let Some(packed) = self.packed_chunks {
                    packed
                        .put(ChunkMeta { digest, size }, compressed.into())
                        .await?;
                } else {
                    put_object(self.object_store, &path, compressed, self.immutable_cache)
                        .await
                        .map_err(io::Error::other)?;
                }
            }
            self.chunk_index.insert(digest);
        }
        Ok(ChunkMeta { digest, size })
    }
}
