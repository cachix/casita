//! Mark shared metadata from physical descriptors and retained page roots.
//! Online sweeps claim exact paths at the revision used to mark pin history.

use super::{
    ChunkedBlobStore, kind_prefix,
    pages::{self, Pages, Root},
};
use crate::{
    BlobId, Digest,
    blob::pinned_store::{validate_deletion_claim, validate_deletion_finish},
    invariant,
    metadata::{PinResource, PinStore, PinToken},
};
use futures::TryStreamExt;
use object_store::{ObjectStoreExt, path::Path};
use std::{collections::BTreeSet, io, sync::Arc};

impl ChunkedBlobStore {
    pub(super) async fn reclaim_pages(
        &self,
        ledger: Option<(Arc<dyn PinStore>, BTreeSet<PinToken>)>,
    ) -> io::Result<()> {
        let store = self.clone();
        let (send, receive) = tokio::sync::oneshot::channel();
        // Keep any acquired claim through submitted deletes on cancellation.
        crate::metadata::spawn_lease_task(async move {
            let result = store.reclaim_pages_inner(ledger).await;
            drop(send.send(result));
            Ok(())
        });
        receive.await.map_err(io::Error::other)?
    }

    async fn reclaim_pages_inner(
        &self,
        ledger: Option<(Arc<dyn PinStore>, BTreeSet<PinToken>)>,
    ) -> io::Result<()> {
        let pages = Pages::from(self);
        let collector = if let Some((pins, _)) = &ledger {
            Some(
                pins.inventory()
                    .await
                    .map_err(io::Error::other)?
                    .collector
                    .ok_or_else(|| {
                        io::Error::other("metadata deletion requires collector ownership")
                    })?,
            )
        } else {
            None
        };
        // Candidate inventory comes first. Pages created after it cannot be swept.
        let mut candidates = Vec::new();
        let mut listed = self
            .object_store
            .list(Some(&kind_prefix(&self.base_path, "pages")));
        while let Some(meta) = listed.try_next().await.map_err(io::Error::other)? {
            if let Ok(hash) = super::digest_from_location(&meta.location) {
                candidates.push((meta.location, Some(hash)));
            }
        }
        let have_pages = !candidates.is_empty();
        let mut live = BTreeSet::new();
        for kind in ["blobs", "bao"] {
            if kind == "blobs" && !have_pages {
                continue;
            }
            let mut listed = self
                .object_store
                .list(Some(&kind_prefix(&self.base_path, kind)));
            while let Some(meta) = listed.try_next().await.map_err(io::Error::other)? {
                // Neither valid flat format can have this length. Avoid a
                // GET for every ordinary small file during collection.
                if meta.size != pages::DESCRIPTOR_BYTES as u64 {
                    continue;
                }
                let expected = if kind == "blobs" {
                    pages::CHUNKS
                } else {
                    pages::OUTBOARD
                };
                if let Some(root) = pages::loose_descriptor(self, &meta.location, expected).await? {
                    if kind == "bao" {
                        let blob = BlobId::new(super::digest_from_location(&meta.location)?);
                        // An interrupted writer can leave an outboard descriptor
                        // without a blob. Its page pins still protect active work.
                        if !super::head_exists(&self.object_store, &self.blob_path(&blob)).await?
                            && !self.chunk_present(super::single_chunk_id(blob)).await?
                        {
                            candidates.push((meta.location, None));
                            continue;
                        }
                    }
                    pages.mark(root, &mut live).await?;
                }
            }
        }
        if have_pages && let Some(packed) = &self.packed_chunks {
            let mut roots = packed.sidecar_roots(None, pages::DESCRIPTOR_BYTES);
            while let Some(bytes) = roots.try_next().await? {
                if let Some(root) = Root::decode(&bytes)? {
                    pages.mark(root, &mut live).await?;
                }
            }
        }
        while !candidates.is_empty() {
            let mut protected = BTreeSet::new();
            let mut inventory = None;
            if let Some((pins, _)) = &ledger {
                let current = pins.inventory().await.map_err(io::Error::other)?;
                if current.collector != collector || !pins.allows_deletion(&current) {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "metadata collector is fenced",
                    ));
                }
                if have_pages && let Some(packed) = &self.packed_chunks {
                    let mut catalogs = BTreeSet::new();
                    for pin in current.pins.values() {
                        if let Some(catalog) = &pin.catalog {
                            catalogs.insert(catalog.as_slice());
                        }
                        for resource in &pin.resources {
                            if let PinResource::Catalog(catalog) = resource {
                                catalogs.insert(catalog.as_slice());
                            }
                        }
                    }
                    for catalog in catalogs {
                        let mut roots =
                            packed.sidecar_roots(Some(catalog), pages::DESCRIPTOR_BYTES);
                        while let Some(bytes) = roots.try_next().await? {
                            if let Some(root) = Root::decode(&bytes)? {
                                pages.mark(root, &mut live).await?;
                            }
                        }
                    }
                }
                for resource in current.pins.values().flat_map(|pin| &pin.resources) {
                    if let PinResource::StorageObject(path) = resource {
                        protected.insert(Path::from(path.as_str()));
                        if path.starts_with(kind_prefix(&self.base_path, "pages").as_ref()) {
                            let path = Path::from(path.as_str());
                            if let Ok(hash) = super::digest_from_location(&path) {
                                mark_pinned(&pages, hash, &mut live).await?;
                            }
                        }
                    }
                }
                inventory = Some(current);
            }
            candidates.retain(|(path, hash)| {
                !protected.contains(path) && hash.is_none_or(|hash| !live.contains(&hash))
            });
            if candidates.is_empty() {
                break;
            }
            let mut bytes = 0;
            let count = candidates
                .iter()
                .take(256)
                .take_while(|(path, _)| {
                    bytes += path.as_ref().len() + 16;
                    bytes <= 32 * 1024
                })
                .count();
            if count == 0 {
                return Err(io::Error::other(
                    "metadata path exceeds deletion claim budget",
                ));
            }
            let batch: Vec<_> = candidates[..count]
                .iter()
                .map(|(path, _)| path.clone())
                .collect();
            // Kept only to check the claim against the deletions it covers.
            let mut claimed = BTreeSet::new();
            let token = if let (Some((pins, owned)), Some(inventory)) = (&ledger, &inventory) {
                let mut resources: BTreeSet<_> = batch
                    .iter()
                    .map(|path| PinResource::StorageObject(path.to_string()))
                    .collect();
                for (token, covered) in &inventory.deletions {
                    if owned.contains(token) {
                        resources = resources.difference(covered).cloned().collect();
                    } else if !resources.is_disjoint(covered) {
                        return Err(io::Error::new(
                            io::ErrorKind::WouldBlock,
                            "metadata deletion overlaps an unfinished claim",
                        ));
                    }
                }
                if resources.is_empty() {
                    None
                } else {
                    if invariant::ENABLED {
                        claimed.clone_from(&resources);
                    }
                    let Some(token) = pins
                        .claim_deletions(inventory.revision, resources)
                        .await
                        .map_err(io::Error::other)?
                    else {
                        // A newly admitted reader/writer can retain entire subtrees.
                        // Re-mark its page roots before trying another claim.
                        continue;
                    };
                    Some(token)
                }
            } else {
                None
            };
            let deleted: BTreeSet<_> = if invariant::ENABLED {
                batch
                    .iter()
                    .map(|path| PinResource::StorageObject(path.to_string()))
                    .collect()
            } else {
                BTreeSet::new()
            };
            if let (Some((_, owned)), Some(inventory)) = (&ledger, &inventory) {
                invariant::check(|| validate_deletion_claim(inventory, owned, &claimed, &deleted))?;
            }
            let requested = batch.len();
            let mut settled = 0_usize;
            for path in batch {
                super::delete_object(&self.object_store, &path).await?;
                settled += 1;
            }
            if let (Some(token), Some((pins, _))) = (token, &ledger) {
                invariant::check(|| {
                    validate_deletion_finish(&claimed, &deleted, requested.saturating_sub(settled))
                })?;
                pins.finish_deletions(&token)
                    .await
                    .map_err(io::Error::other)?;
            }
            candidates.drain(..count);
        }
        Ok(())
    }
}

async fn mark_pinned(pages: &Pages, hash: Digest, live: &mut BTreeSet<Digest>) -> io::Result<()> {
    if live.contains(&hash) {
        return Ok(());
    }
    let bytes = match pages.objects.get_range(&pages.path(&hash), 0..4121).await {
        Ok(bytes) => bytes,
        // Pins are acquired before PUT. Missing newly staged pages have no
        // reachable children yet; the writer pins every completed child too.
        Err(object_store::Error::NotFound { .. }) => {
            return Ok(());
        }
        Err(e) => return Err(io::Error::other(e)),
    };
    if bytes.len() < 24 {
        return Err(io::Error::other("short pinned metadata page"));
    }
    let root = Root {
        kind: bytes[8],
        height: bytes[9],
        span: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
        hash,
    };
    pages.decode(root, bytes)?;
    pages.mark(root, live).await
}
