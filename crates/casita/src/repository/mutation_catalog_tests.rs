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

// The catalog is an opaque metadata witness here. This isolates its pin lifetime
// from pack encoding and exercises the real bounded file ledger.
struct CatalogState {
    inner: MemoryMetadataStore,
    pins: Arc<FilePinStore>,
    gate: Option<Arc<CommitGate>>,
    snapshot_gate: Option<Arc<SnapshotGate>>,
    coordinate_catalog: bool,
}

#[derive(Default)]
struct CommitGate {
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

struct SnapshotGate {
    pause: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

impl SnapshotGate {
    fn new() -> Self {
        Self {
            pause: std::sync::atomic::AtomicBool::new(true),
            entered: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        }
    }
}
#[async_trait]
impl MetadataStore for CatalogState {
    async fn try_collection_lease(
        &self,
    ) -> Result<Option<crate::metadata::RepositoryLease>, crate::metadata::MetadataError> {
        self.inner.try_collection_lease().await
    }
    fn coordinates_payload_catalog(&self) -> bool {
        self.coordinate_catalog
    }
    async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
        let snapshot = self.inner.snapshot().await?;
        if let Some(gate) = &self.snapshot_gate
            && gate.pause.swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            gate.entered.notify_one();
            gate.resume.notified().await;
        }
        Ok(snapshot)
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
        snapshot_gate: None,
        coordinate_catalog: false,
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
            snapshot_gate: None,
            coordinate_catalog: false,
        },
    ));
    let publisher = {
        let repository = repository.clone();
        tokio::spawn(async move {
            let session = repository.mutation_session().await.unwrap();
            let staged = session.stage_blob(b"cancelled caller").await.unwrap();
            session.publish_unrooted(vec![staged]).await.unwrap();
        })
    };
    gate.entered.notified().await;
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
async fn catalog_rotation_during_initial_pin_admission_is_rechecked() {
    let directory = tempfile::tempdir().unwrap();
    let pins = Arc::new(FilePinStore::new(directory.path().join("pins")));
    let inner = MemoryMetadataStore::new().unwrap();
    let first = vec![1; 56];
    let second = vec![2; 56];
    let revision = inner.snapshot().await.unwrap().revision();
    let mut mutation = MetadataMutation::new();
    mutation.set_payload_catalog(first.clone());
    inner.commit(&revision, mutation).await.unwrap();
    let gate = Arc::new(SnapshotGate::new());
    let repository = Arc::new(Repository::new(
        MemoryBlobStore::new(),
        CatalogState {
            inner,
            pins: pins.clone(),
            gate: None,
            snapshot_gate: Some(gate.clone()),
            coordinate_catalog: true,
        },
    ));
    let writer = {
        let repository = repository.clone();
        tokio::spawn(async move {
            let session = repository.mutation_session().await.unwrap();
            let inventory = pins.inventory().await.unwrap();
            let pin = inventory.pins.get(session.pin.token()).unwrap();
            assert!(pin.resources.contains(&PinResource::Catalog(first)));
            assert!(pin.resources.contains(&PinResource::Catalog(second)));
        })
    };
    gate.entered.notified().await;
    let revision = repository.state.inner.snapshot().await.unwrap().revision();
    let mut mutation = MetadataMutation::new();
    mutation.set_payload_catalog(vec![2; 56]);
    repository
        .state
        .inner
        .commit(&revision, mutation)
        .await
        .unwrap();
    gate.resume.notify_one();
    writer.await.unwrap();
    crate::metadata::flush_repository_leases().await.unwrap();
    assert!(
        repository
            .state
            .pins
            .inventory()
            .await
            .unwrap()
            .pins
            .is_empty()
    );
}

#[tokio::test]
#[ignore = "permanent mutation-pin-admission benchmark"]
async fn benchmark_mutation_pin_admission() {
    let bytes: usize = std::env::var("CASITA_BENCH_PIN_CATALOG_BYTES")
        .unwrap()
        .parse()
        .unwrap();
    let iterations: usize = std::env::var("CASITA_BENCH_PIN_ITERATIONS")
        .unwrap()
        .parse()
        .unwrap();
    assert!(iterations > 0);
    let directory = tempfile::tempdir().unwrap();
    let pins = Arc::new(FilePinStore::new(directory.path().join("pins")));
    let inner = MemoryMetadataStore::new().unwrap();
    let catalog = vec![7; bytes];
    if bytes > 0 {
        let revision = inner.snapshot().await.unwrap().revision();
        let mut mutation = MetadataMutation::new();
        mutation.set_payload_catalog(catalog.clone());
        inner.commit(&revision, mutation).await.unwrap();
    }
    let repository = Repository::new(
        MemoryBlobStore::new(),
        CatalogState {
            inner,
            pins: pins.clone(),
            gate: None,
            snapshot_gate: None,
            coordinate_catalog: true,
        },
    );
    pins.inventory().await.unwrap();
    let warmup = repository.mutation_session().await.unwrap();
    drop(warmup);
    crate::metadata::flush_repository_leases().await.unwrap();
    assert!(pins.inventory().await.unwrap().pins.is_empty());
    let mut nanos = 0_u128;
    let mut journal_syncs = 0_u64;
    let mut admission_operations = 0_u64;
    for _ in 0..iterations {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        let before = pins.test_stats();
        let start = std::time::Instant::now();
        let session = repository.mutation_session().await.unwrap();
        nanos += start.elapsed().as_nanos();
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let after = pins.test_stats();
            journal_syncs += after["journal_syncs"] - before["journal_syncs"];
            admission_operations += after["operations"] - before["operations"];
        }
        let inventory = pins.inventory().await.unwrap();
        let pin = inventory.pins.get(session.pin.token()).unwrap();
        assert_eq!(
            pin.resources
                .contains(&PinResource::Catalog(catalog.clone())),
            bytes > 0
        );
        drop(session);
        crate::metadata::flush_repository_leases().await.unwrap();
        assert!(pins.inventory().await.unwrap().pins.is_empty());
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        assert_eq!(admission_operations, iterations as u64);
        assert!(journal_syncs >= iterations as u64);
        assert!(journal_syncs <= 2 * iterations as u64);
        if bytes <= 56 {
            assert_eq!(journal_syncs, iterations as u64);
        }
    }
    println!(
        "mutation_pin_admission_sample {}",
        serde_json::json!({
            "catalog_bytes": bytes,
            "iterations": iterations,
            "nanos": nanos,
            "journal_syncs": journal_syncs,
            "admission_operations": admission_operations,
            "correctness": "current catalog protected and empty released inventory"
        })
    );
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
