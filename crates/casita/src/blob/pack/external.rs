//! Small state/pin witnesses for immutable local catalog roots.
use super::*;

const MAGIC: &[u8; 8] = b"casitae1";
const MAX_ROOT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ExternalCatalog {
    generation: u64,
    length: u64,
    pub(super) digest: Digest,
}

impl ExternalCatalog {
    pub(super) fn decode(bytes: &[u8]) -> io::Result<Option<Self>> {
        if !bytes.starts_with(MAGIC) {
            return Ok(None);
        }
        if bytes.len() != 56 {
            return Err(io::Error::other(
                "invalid external catalog reference length",
            ));
        }
        let generation = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let length = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        if generation == 0 || length == 0 || length > MAX_ROOT_BYTES {
            return Err(io::Error::other(
                "invalid external catalog reference bounds",
            ));
        }
        Ok(Some(Self {
            generation,
            length,
            digest: Digest::from(<[u8; 32]>::try_from(&bytes[24..]).unwrap()),
        }))
    }

    pub(super) fn encode(self) -> Bytes {
        let mut bytes = Vec::with_capacity(56);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&self.generation.to_le_bytes());
        bytes.extend_from_slice(&self.length.to_le_bytes());
        bytes.extend_from_slice(self.digest.as_bytes());
        bytes.into()
    }
}

impl PackedChunks {
    /// All dependencies and the root are durable before this witness may enter
    /// a SQLite transaction. Upload failure leaves the old witness authoritative.
    pub(crate) async fn externalize_state_catalog(&self, catalog: &[u8]) -> io::Result<Vec<u8>> {
        if ExternalCatalog::decode(catalog)?.is_some() {
            // Callers have already synchronized this authenticated witness.
            return Ok(catalog.to_vec());
        }
        let root = decode_delta_catalog(catalog)?;
        if catalog.len() as u64 > MAX_ROOT_BYTES {
            return Err(io::Error::other("external catalog root is too large"));
        }
        let reference = ExternalCatalog {
            generation: root.generation,
            length: catalog.len() as u64,
            digest: blake3::hash(catalog).into(),
        };
        self.mark_catalog_reclaim_due().await?;
        let mut publication = self.catalog_object_publication();
        publication
            .put(reference.digest, Bytes::copy_from_slice(catalog))
            .await?;
        publication.finish().await?;
        #[cfg(test)]
        crate::blob::crash_tests::checkpoint("external-catalog-durable");
        Ok(reference.encode().to_vec())
    }
}

impl PackReader {
    /// Resolve one level only. A missing, corrupt, oversized or nested root is
    /// fatal: inventory discovery must never substitute unpublished objects.
    pub(super) async fn resolve_state_catalog(&self, catalog: &[u8]) -> io::Result<Bytes> {
        let Some(reference) = ExternalCatalog::decode(catalog)? else {
            return Ok(Bytes::copy_from_slice(catalog));
        };
        let path = sharded_path(&self.base, INDEXES_KIND, &reference.digest);
        self.read_counters
            .index_requests
            .fetch_add(1, Ordering::Relaxed);
        let result = self
            .object_store
            .get(&path)
            .await
            .map_err(io::Error::other)?;
        if result.meta.size != reference.length {
            return Err(io::Error::other("external catalog object length mismatch"));
        }
        let bytes = result.bytes().await.map_err(io::Error::other)?;
        if bytes.len() as u64 != reference.length
            || Digest::from(blake3::hash(&bytes)) != reference.digest
        {
            return Err(io::Error::other(
                "external catalog object identity mismatch",
            ));
        }
        let root = decode_delta_catalog(&bytes)?;
        if root.generation != reference.generation {
            return Err(io::Error::other("external catalog generation mismatch"));
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn local(root: &std::path::Path, catalog: &[u8]) -> Arc<PackedChunks> {
        let filesystem = object_store::local::LocalFileSystem::new_with_prefix(root).unwrap();
        let durability = LocalDurability::new(filesystem.clone(), root).unwrap();
        PackedChunks::open_with_initial_catalog(
            Arc::new(filesystem),
            Path::default(),
            u64::MAX,
            0,
            Some(catalog),
            Some(durability),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn external_roots_follow_publication_abort_and_retention() {
        let directory = tempfile::tempdir().unwrap();
        let empty = PackedChunks::empty_state_catalog().unwrap();
        let packed = local(directory.path(), &empty).await;
        let original = packed.externalize_state_catalog(&empty).await.unwrap();
        packed
            .synchronize_state_catalog(Some(&original))
            .await
            .unwrap();
        let original_ref = ExternalCatalog::decode(&original).unwrap().unwrap();
        packed.register_manifest(BlobId::new(blake3::hash(b"new manifest").into()));
        let aborted = packed.prepare_state_catalog().await.unwrap().unwrap();
        assert_eq!(aborted.len(), 56);
        packed
            .finish_state_catalog(CatalogOutcome::Aborted)
            .unwrap();
        let swept = packed.reclaim_catalog_objects(&[]).await.unwrap();
        assert_eq!(swept.deleted_objects, 1);
        assert_eq!(
            packed
                .resolve_state_catalog(&original)
                .await
                .unwrap()
                .as_ref(),
            empty
        );
        assert!(packed.resolve_state_catalog(&aborted).await.is_err());
        let next = packed.prepare_state_catalog().await.unwrap().unwrap();
        assert_eq!(next, aborted);
        packed
            .finish_state_catalog(CatalogOutcome::Committed)
            .unwrap();
        let swept = packed
            .reclaim_catalog_objects(&[original.clone().into()])
            .await
            .unwrap();
        assert_eq!(swept.deleted_objects, 0);
        assert_eq!(swept.retained_objects, 2);
        let historical = local(directory.path(), &original).await;
        assert!(
            !historical
                .catalog_contains_manifest(BlobId::new(blake3::hash(b"new manifest").into()))
                .await
                .unwrap()
        );
        assert!(
            local(directory.path(), &next)
                .await
                .catalog_contains_manifest(BlobId::new(blake3::hash(b"new manifest").into()))
                .await
                .unwrap()
        );
        drop(historical);
        let swept = packed.reclaim_catalog_objects(&[]).await.unwrap();
        assert_eq!(swept.deleted_objects, 1);
        assert!(
            packed
                .object_store
                .head(&sharded_path(
                    &packed.base,
                    INDEXES_KIND,
                    &original_ref.digest
                ))
                .await
                .is_err()
        );
        packed.resolve_state_catalog(&next).await.unwrap();
    }

    #[tokio::test]
    async fn external_root_reads_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let empty = PackedChunks::empty_state_catalog().unwrap();
        let packed = local(directory.path(), &empty).await;
        let descriptor = packed.externalize_state_catalog(&empty).await.unwrap();
        let reference = ExternalCatalog::decode(&descriptor).unwrap().unwrap();
        let path = sharded_path(&packed.base, INDEXES_KIND, &reference.digest);
        let mut wrong_generation = reference;
        wrong_generation.generation += 1;
        assert!(
            packed
                .resolve_state_catalog(&wrong_generation.encode())
                .await
                .is_err()
        );
        let mut wrong_length = reference;
        wrong_length.length += 1;
        assert!(
            packed
                .resolve_state_catalog(&wrong_length.encode())
                .await
                .is_err()
        );
        let mut corrupt = empty.clone();
        corrupt[0] ^= 1;
        packed
            .object_store
            .put(&path, Bytes::from(corrupt).into())
            .await
            .unwrap();
        assert!(packed.resolve_state_catalog(&descriptor).await.is_err());
        packed.object_store.delete(&path).await.unwrap();
        assert!(packed.resolve_state_catalog(&descriptor).await.is_err());
        // A malformed witness cannot masquerade as a legacy inline root.
        assert!(packed.resolve_state_catalog(b"casitae1").await.is_err());
        let mut oversized = reference;
        oversized.length = MAX_ROOT_BYTES + 1;
        assert!(ExternalCatalog::decode(&oversized.encode()).is_err());
    }
}
