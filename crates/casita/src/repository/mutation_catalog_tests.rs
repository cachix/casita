use super::*;
use crate::MemoryBlobStore;
use crate::metadata::{FilePinStore, MemoryMetadataStore, PinResource, PinScope, PinStore};

#[tokio::test]
async fn repeated_publication_does_not_accumulate_catalogs() {
    let directory = tempfile::tempdir().unwrap();
    let repository = Repository::local(directory.path()).await.unwrap();
    let session = repository.mutation_session().await.unwrap();
    let mut keys = Vec::new();
    for index in 0..8 {
        let object = session
            .stage_blob(format!("payload-{index}").as_bytes())
            .await
            .unwrap();
        keys.push(object.record().key().clone());
        session.publish_unrooted(vec![object]).await.unwrap();
        assert_eq!(
            repository
                .state
                .snapshot()
                .await
                .unwrap()
                .payload_catalog()
                .unwrap()
                .len(),
            56
        );
        crate::metadata::flush_repository_leases().await.unwrap();
        let inventory = repository
            .state
            .pin_store()
            .await
            .unwrap()
            .inventory()
            .await
            .unwrap();
        let catalogs = inventory
            .pins
            .values()
            .filter(|p| p.scope == PinScope::Staging)
            .flat_map(|p| &p.resources)
            .filter(|r| matches!(r, PinResource::Catalog(_)))
            .count();
        for pin in inventory.pins.values() {
            if let Some(catalog) = &pin.catalog {
                assert_eq!(catalog.len(), 56);
            }
            for resource in &pin.resources {
                if let PinResource::Catalog(catalog) = resource {
                    assert_eq!(catalog.len(), 56);
                }
            }
        }
        assert!(
            catalogs <= 1,
            "mutation retains {catalogs} catalog versions"
        );
        repository.collect().await.unwrap();
        for (index, key) in keys.iter().enumerate() {
            let (_, mut reader) = repository.open_payload(key).await.unwrap().unwrap();
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, format!("payload-{index}").as_bytes());
        }
    }
    drop(session);
    crate::metadata::flush_repository_leases().await.unwrap();
    repository.collect().await.unwrap();
    for key in &keys {
        assert!(
            repository
                .state
                .snapshot()
                .await
                .unwrap()
                .object(key)
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn rotation_retains_real_packed_payloads_through_collection() {
    let directory = tempfile::tempdir().unwrap();
    let repository = Repository::local(directory.path()).await.unwrap();
    let mut session = repository.mutation_session().await.unwrap();
    let mut keys = Vec::new();
    let mut held = None;
    for index in 0..4 {
        let object = session
            .stage_blob(format!("rotated-{index}").as_bytes())
            .await
            .unwrap();
        keys.push(object.record().key().clone());
        session.publish_unrooted(vec![object]).await.unwrap();
        held = Some(repository.retention_hold().await.unwrap());
        session.rotate().await.unwrap();
        crate::metadata::flush_repository_leases().await.unwrap();
        repository.collect().await.unwrap();
        for (index, key) in keys.iter().enumerate() {
            let (_, mut reader) = repository.open_payload(key).await.unwrap().unwrap();
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, format!("rotated-{index}").as_bytes());
        }
    }
    drop(session);
    drop(held);
    crate::metadata::flush_repository_leases().await.unwrap();
    repository.collect().await.unwrap();
    for key in keys {
        assert!(repository.open_payload(&key).await.unwrap().is_none());
    }
}

// The catalog is an opaque metadata witness here. This isolates its pin lifetime
// from pack encoding and exercises the real bounded file ledger.
struct CatalogState {
    inner: MemoryMetadataStore,
    pins: Arc<FilePinStore>,
    gate: Option<Arc<CommitGate>>,
}

#[derive(Default)]
struct CommitGate {
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
#[async_trait]
impl MetadataStore for CatalogState {
    async fn try_collection_lease(
        &self,
    ) -> Result<Option<crate::metadata::RepositoryLease>, crate::metadata::MetadataError> {
        self.inner.try_collection_lease().await
    }
    fn coordinates_payload_catalog(&self) -> bool {
        self.inner.coordinates_payload_catalog()
    }
    async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
        self.inner.snapshot().await
    }
    async fn commit(
        &self,
        expected: &crate::RepositoryRevision,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        if let Some(gate) = &self.gate {
            gate.entered.notify_one();
            gate.resume.notified().await;
        }
        self.inner.commit(expected, mutation).await
    }
    async fn pin_store(&self) -> Result<Arc<dyn PinStore>, MetadataError> {
        Ok(self.pins.clone())
    }
}

async fn catalog_boundary(count: usize) -> (u64, usize) {
    let directory = tempfile::tempdir().unwrap();
    let pins = Arc::new(FilePinStore::new(directory.path().join("pins")));
    let state = CatalogState {
        inner: MemoryMetadataStore::new().unwrap(),
        pins: pins.clone(),
        gate: None,
    };
    let repository = Repository::new(MemoryBlobStore::new(), state);
    let session = repository.mutation_session().await.unwrap();
    let started = std::time::Instant::now();
    let mut peak = 0;
    for index in 0..count {
        let snapshot = repository.state.snapshot().await.unwrap();
        let revision = snapshot.revision();
        drop(snapshot);
        let mut catalog = vec![0; 1024 * 1024];
        catalog[..8].copy_from_slice(&(index as u64).to_le_bytes());
        let mut change = MetadataMutation::new();
        change.set_payload_catalog(catalog);
        repository.state.commit(&revision, change).await.unwrap();
        let held = session.pinned_snapshot().await.unwrap();
        let inventory = pins.inventory().await.unwrap();
        let bytes: usize = inventory
            .pins
            .values()
            .map(|p| {
                p.catalog.as_ref().map_or(0, Vec::len)
                    + p.resources
                        .iter()
                        .map(|r| {
                            if let PinResource::Catalog(c) = r {
                                c.len()
                            } else {
                                0
                            }
                        })
                        .sum::<usize>()
            })
            .sum();
        peak = peak.max(bytes);
        drop(held);
        crate::metadata::flush_repository_leases().await.unwrap();
    }
    let nanos = started.elapsed().as_nanos() as u64;
    drop(session);
    crate::metadata::flush_repository_leases().await.unwrap();
    assert!(pins.inventory().await.unwrap().pins.is_empty());
    assert!(peak <= 1024 * 1024, "catalog history accumulated: {peak}");
    (nanos, peak)
}

#[tokio::test]
async fn cancelled_publisher_retains_catalog_until_commit_settles() {
    let directory = tempfile::tempdir().unwrap();
    let pins = Arc::new(FilePinStore::new(directory.path().join("pins")));
    let inner = MemoryMetadataStore::new().unwrap();
    let revision = inner.snapshot().await.unwrap().revision();
    let mut seed = MetadataMutation::new();
    seed.set_payload_catalog(vec![7; 65536]);
    inner.commit(&revision, seed).await.unwrap();
    let gate = Arc::new(CommitGate::default());
    let repository = Arc::new(Repository::new(
        MemoryBlobStore::new(),
        CatalogState {
            inner,
            pins: pins.clone(),
            gate: Some(gate.clone()),
        },
    ));
    let mut publisher = {
        let repository = repository.clone();
        tokio::spawn(async move {
            let session = repository.mutation_session().await.unwrap();
            let staged = session.stage_blob(b"cancelled caller").await.unwrap();
            session.publish_unrooted(vec![staged]).await.unwrap();
        })
    };
    // A publisher that fails before its commit never reaches the gate.
    tokio::select! {
        _ = gate.entered.notified() => {}
        result = &mut publisher => panic!("publisher ended before its commit: {result:?}"),
    }
    publisher.abort();
    assert!(publisher.await.unwrap_err().is_cancelled());
    let inventory = pins.inventory().await.unwrap();
    assert!(
        inventory
            .pins
            .values()
            .any(|pin| pin.catalog.as_deref() == Some(&vec![7; 65536]))
    );
    gate.resume.notify_one();
    crate::metadata::flush_repository_leases().await.unwrap();
    assert!(pins.inventory().await.unwrap().pins.is_empty());
    let key = ObjectKey::blob(BlobId::new(blake3::hash(b"cancelled caller").into()));
    assert!(
        repository
            .state
            .snapshot()
            .await
            .unwrap()
            .object(&key)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn catalog_history_crosses_former_inventory_limit() {
    for count in [63, 65] {
        catalog_boundary(count).await;
    }
}

#[tokio::test]
#[ignore = "permanent mutation-catalog benchmark"]
async fn benchmark_mutation_catalog_history() {
    let count = std::env::var("CASITA_BENCH_CATALOG_VERSIONS")
        .unwrap()
        .parse()
        .unwrap();
    let (nanos, peak) = catalog_boundary(count).await;
    println!(
        "catalog_history_sample {}",
        serde_json::json!({
            "count": count, "nanos": nanos, "peak_catalog_bytes": peak,
            "correctness": "bounded active catalog and empty released inventory"
        })
    );
}
