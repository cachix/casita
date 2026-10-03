//! Same-length edits copy metadata paths and reuse all other chunks and pages.

use super::{
    ChunkedBlobStore,
    pages::{self, Cursor, Edit, Pages},
    put_object,
};
use crate::blob::chunked_reader::ChunkSource;
use crate::{BlobId, blob::BlobStore, error::Error, metadata::PinResource};
use bytes::Bytes;
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::atomic::Ordering,
};

impl ChunkedBlobStore {
    pub(super) async fn overwrite_content(
        &self,
        old: &BlobId,
        size: u64,
        offset: u64,
        replacement: &[u8],
    ) -> Result<(BlobId, Bytes), Error> {
        let range = crate::verified::patch::aligned(size, offset, replacement.len() as u64)?;
        let pins = self.pins.capture();
        pins.protect(BTreeSet::from([
            PinResource::Blob(*old),
            PinResource::StorageObject(self.blob_path(old).to_string()),
            PinResource::StorageObject(self.outboard_path(old).to_string()),
        ]))
        .await?;
        if replacement.is_empty() {
            if !self.has(old).await? {
                return Err(Error::NotFound {
                    digest: (*old).into(),
                });
            }
            return Ok((*old, Bytes::new()));
        }
        let pages = Pages::from(self);
        let root = match pages::descriptor(self, &self.blob_path(old), pages::CHUNKS).await? {
            Some(root) => root,
            None => {
                // Compact flat manifests and elided single chunks become
                // indexed trees for the edit.
                let chunks = self.chunks(old).await?.ok_or(Error::NotFound {
                    digest: (*old).into(),
                })?;
                pages.build_chunks(&chunks).await?
            }
        };
        if root.span != size {
            return Err(io::Error::other("overwrite size mismatch").into());
        }
        let outboard_size =
            bao_tree::BaoTree::new(size, crate::verified::BLOCK_SIZE).outboard_size();
        let outboard_root = if outboard_size == 0 {
            None
        } else {
            Some(
                match pages::descriptor(self, &self.outboard_path(old), pages::OUTBOARD).await? {
                    Some(root) => root,
                    None => {
                        let bytes = self.get_outboard(old).await?.ok_or_else(|| {
                            Error::Msg(
                                "prepare Bao metadata before overwriting an existing blob".into(),
                            )
                        })?;
                        if bytes.len() as u64 != outboard_size {
                            return Err(
                                io::Error::other("overwrite outboard length mismatch").into()
                            );
                        }
                        pages.build_bytes(&mut bytes.as_ref()).await?
                    }
                },
            )
        };
        if outboard_root.is_some_and(|root| root.span != outboard_size) {
            return Err(io::Error::other("overwrite outboard length mismatch").into());
        }
        let data = pages::Data::new(self, root).await?;
        let outboard = bao_tree::io::outboard::PreOrderOutboard {
            root: bao_tree::blake3::Hash::from(*old.digest().as_bytes()),
            tree: bao_tree::BaoTree::new(size, crate::verified::BLOCK_SIZE),
            data: super::proof::OutboardSource::new(self, old, outboard_size, None).await?,
        };
        let mut proof = Vec::new();
        let ranges = bao_tree::io::round_up_to_chunks(&bao_tree::ByteRanges::from(range));
        bao_tree::io::fsm::encode_ranges_validated(data, outboard, &ranges, &mut proof)
            .await
            .map_err(io::Error::other)?;
        let update =
            crate::verified::patch::verify(&proof, *old, size, offset, replacement).await?;
        pins.protect(BTreeSet::from([PinResource::Blob(update.digest)]))
            .await?;
        let mut cursor = Cursor::new(self, root).await?;
        let mut edits = BTreeMap::new();
        let end = offset + replacement.len() as u64;
        let mut position = offset;
        while position < end {
            let (start, chunk) = cursor.chunk(position).await?;
            let chunk_end = start + chunk.size;
            let mut data = self.fetch_chunk(chunk.digest, chunk.size).await?.to_vec();
            let finish = end.min(chunk_end);
            data[(position - start) as usize..(finish - start) as usize].copy_from_slice(
                &replacement[(position - offset) as usize..(finish - offset) as usize],
            );
            let new = super::upload::ChunkUploader {
                object_store: &self.object_store,
                base_path: &self.base_path,
                chunk_index: &self.chunk_index,
                packed_chunks: self.packed_chunks.as_ref(),
                immutable_cache: self.immutable_cache,
                pins: &pins,
            }
            .upload(data)
            .await?;
            edits.insert(start, Edit::Chunk(new));
            position = finish;
        }
        let manifest = cursor.edit(&edits).await?;
        if let Some(root) = outboard_root {
            let edits = update
                .nodes
                .into_iter()
                .map(|(position, pair)| (position, Edit::Bytes(Bytes::copy_from_slice(&pair))))
                .collect();
            let outboard = Cursor::new(self, root).await?.edit(&edits).await?;
            self.put_outboard_root(&update.digest, outboard.encode().into())
                .await?;
        }
        // Publish only after both trees and all modified payload chunks exist.
        put_object(
            &self.object_store,
            &self.blob_path(&update.digest),
            manifest.encode(),
            self.immutable_cache,
        )
        .await
        .map_err(io::Error::other)?;
        if let Some(packed) = &self.packed_chunks {
            packed.register_manifest(update.digest);
            if self.batch_depth.load(Ordering::Acquire) == 0 {
                packed.flush().await?;
            }
        }
        Ok((update.digest, proof.into()))
    }
}
