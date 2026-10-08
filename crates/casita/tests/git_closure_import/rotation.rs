//! Observe real native-import pins and audit lifetime handoffs.

use super::*;
use casita::experimental::{
    CommitResult, MemoryBlobStore, MemoryMetadataStore, MetadataError, MetadataMutation,
    MetadataSnapshot, ObjectKey, PinResource, PinScope, PinStore, PinToken, Repository,
    RepositoryLease, RepositoryRevision, VerificationFacts, async_trait,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncReadExt;

struct ObservedMetadata {
    inner: MemoryMetadataStore,
    enabled: AtomicBool,
    largest_writer: AtomicUsize,
    publications: AtomicUsize,
    writers: Mutex<BTreeSet<PinToken>>,
    published: Mutex<BTreeMap<ObjectKey, u64>>,
}

impl ObservedMetadata {
    async fn observe(&self, keys: Vec<ObjectKey>) -> Result<(), MetadataError> {
        if self.enabled.load(Ordering::SeqCst) {
            let objects_published = !keys.is_empty();
            let generation = self.inner.snapshot().await?.generation()?;
            self.published
                .lock()
                .unwrap()
                .extend(keys.into_iter().map(|key| (key, generation)));
            let inventory = self.inner.pin_store().await?.inventory().await?;
            for (token, pin) in &inventory.pins {
                if pin.scope == PinScope::Staging {
                    self.writers.lock().unwrap().insert(token.clone());
                    let objects = pin
                        .resources
                        .iter()
                        .filter(|resource| matches!(resource, PinResource::Object(_)))
                        .count();
                    // Final proof retains all selected roots. This fixture
                    // selects many independent blobs to isolate staging;
                    // measure object publication, not that separate proof.
                    if objects_published {
                        self.largest_writer.fetch_max(objects, Ordering::SeqCst);
                    }
                }
            }
            self.publications.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

// Forward every capability of the memory store; observation never substitutes
// metadata, pins or collection behavior.
#[async_trait]
impl MetadataStore for ObservedMetadata {
    fn supports_metadata_records(&self) -> bool {
        self.inner.supports_metadata_records()
    }
    fn supports_root_retention(&self) -> bool {
        self.inner.supports_root_retention()
    }
    fn verification_facts(&self) -> Option<Arc<dyn VerificationFacts>> {
        self.inner.verification_facts()
    }
    fn commit_durability(&self) -> Option<casita::experimental::CommitDurability> {
        self.inner.commit_durability()
    }
    fn coordinates_payload_catalog(&self) -> bool {
        self.inner.coordinates_payload_catalog()
    }
    async fn get_records(
        &self,
        keys: &[casita::MetadataKey],
    ) -> Result<Vec<Option<bytes::Bytes>>, MetadataError> {
        self.inner.get_records(keys).await
    }
    async fn pin_store(&self) -> Result<Arc<dyn PinStore>, MetadataError> {
        self.inner.pin_store().await
    }
    async fn try_collection_lease(&self) -> Result<Option<RepositoryLease>, MetadataError> {
        self.inner.try_collection_lease().await
    }
    async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
        if self.enabled.load(Ordering::SeqCst) {
            // Force pending releases to finish so a dropped writer cannot mask
            // a gap before the replacement reader registers its protection.
            // Auditing is disabled before collection: a tracked collector must
            // not wait on itself through this global work drain.
            casita::experimental::flush_repository_leases().await?;
            let inventory = self.inner.pin_store().await?.inventory().await?;
            for (token, pin) in &inventory.pins {
                if pin.scope == PinScope::Staging {
                    self.writers.lock().unwrap().insert(token.clone());
                }
            }
            let generation = inventory
                .pins
                .values()
                .filter_map(|pin| match pin.scope {
                    PinScope::Snapshot { generation } => Some(generation),
                    _ => None,
                })
                .max();
            for (key, birth) in self.published.lock().unwrap().iter() {
                assert!(
                    generation.is_some_and(|generation| generation >= *birth)
                        || inventory
                            .pins
                            .values()
                            .any(|pin| pin.resources.contains(&PinResource::Object(key.clone()))),
                    "published object {key} lost protection before the next metadata read"
                );
            }
        }
        self.inner.snapshot().await
    }
    async fn commit(
        &self,
        expected: &RepositoryRevision,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        let keys = mutation
            .objects()
            .iter()
            .map(|object| object.record().key().clone())
            .collect();
        let result = self.inner.commit(expected, mutation).await?;
        self.observe(keys).await?;
        Ok(result)
    }
    async fn commit_checked(
        &self,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        let keys = mutation
            .objects()
            .iter()
            .map(|object| object.record().key().clone())
            .collect();
        let result = self.inner.commit_checked(mutation).await?;
        self.observe(keys).await?;
        Ok(result)
    }
    async fn compact_transient_state(&self) -> Result<(), MetadataError> {
        self.inner.compact_transient_state().await
    }
}

#[tokio::test]
async fn native_import_writer_boundaries_preserve_owned_and_borrowed_outputs() {
    use casita::experimental::{FormatLimits, FormatRegistry, flush_repository_leases};
    use casita::import::Importer;

    // Selected leaves isolate writer resources from a wide parent's link set.
    let source = Source::new("sha1");
    let bodies: Vec<_> = (0..113_u64).map(|n| n.to_le_bytes()).collect();
    let keys: Vec<_> = bodies
        .iter()
        .map(|body| {
            key(
                GitObjectFormat::Sha1,
                GitObjectKind::Blob,
                &source.blob(body),
            )
        })
        .collect();
    for count in [55_usize, 56, 57, 112, 113] {
        for (concurrency, budget) in [(1_usize, 1024_u64), (3, 1024), (3, 1)] {
            for owned in [false, true] {
                let metadata = Arc::new(ObservedMetadata {
                    inner: MemoryMetadataStore::new().unwrap(),
                    enabled: AtomicBool::new(true),
                    largest_writer: AtomicUsize::new(0),
                    publications: AtomicUsize::new(0),
                    writers: Mutex::new(BTreeSet::new()),
                    published: Mutex::new(BTreeMap::new()),
                });
                let repository = Repository::with_formats(
                    MemoryBlobStore::new(),
                    metadata.clone(),
                    FormatRegistry::builtin(),
                    FormatLimits {
                        max_batch_objects: 7,
                        ..Default::default()
                    },
                );
                let request = source
                    .request(keys[..count].to_vec())
                    .with_concurrency(concurrency.try_into().unwrap())
                    .with_max_buffered_bytes(budget.try_into().unwrap());
                let mut session = None;
                let mut imported = None;
                // The benchmark probe counts rotations through these events;
                // tie them to the distinct staging writers the store observes.
                let rotated = writer_rotations();
                let report = if owned {
                    imported = Some(repository.import(request).await.unwrap());
                    imported.as_ref().unwrap().report.clone()
                } else {
                    session = Some(repository.mutation_session().await.unwrap());
                    // The report itself carries no reader: only the supplied
                    // session must protect every imported object after return.
                    let report = request.import(session.as_ref().unwrap()).await.unwrap();
                    assert_eq!(report.imported_objects, count);
                    assert_eq!(report.reused_objects, 0);
                    // Observe the post-engine lifetime before acquiring any reader.
                    metadata.snapshot().await.unwrap();
                    metadata.enabled.store(false, Ordering::SeqCst);
                    assert_eq!(metadata.writers.lock().unwrap().len(), 1);
                    assert_eq!(metadata.largest_writer.load(Ordering::SeqCst), count);
                    report
                };
                assert_eq!(report.imported_objects, count);
                assert_eq!(report.reused_objects, 0);
                metadata.enabled.store(false, Ordering::SeqCst);
                let peak = metadata.largest_writer.load(Ordering::SeqCst);
                let writers = metadata.writers.lock().unwrap().len();
                assert_eq!(
                    writer_rotations() - rotated + 1,
                    writers,
                    "count={count} concurrency={concurrency} budget={budget} owned={owned}: rotation events"
                );
                if owned {
                    // Eight 7-object publications, plus the rest of a decoded
                    // group that straddles the boundary.
                    assert!(
                        peak < 56 + concurrency,
                        "owned count={count} concurrency={concurrency} budget={budget}: writer retained {peak}"
                    );
                    if concurrency == 1 || budget == 1 {
                        assert_eq!(writers, count.div_ceil(56), "no trailing empty writer");
                    }
                }
                flush_repository_leases().await.unwrap();
                repository.collect().await.unwrap();
                for (key, body) in keys[..count].iter().zip(&bodies) {
                    let (_, mut payload) = repository.open_payload(key).await.unwrap().unwrap();
                    let mut actual = Vec::new();
                    payload.read_to_end(&mut actual).await.unwrap();
                    assert_eq!(&actual, body);
                }
                drop(imported);
                drop(session);
                flush_repository_leases().await.unwrap();
                repository.collect().await.unwrap();
                for key in &keys[..count] {
                    assert!(repository.open_payload(key).await.unwrap().is_none());
                }
            }
        }
    }
}

#[tokio::test]
async fn native_import_writer_failure_after_rotation_leaves_no_false_witness() {
    use casita::experimental::{FormatLimits, FormatRegistry, flush_repository_leases};
    for cancelled in [false, true] {
        let source = Source::new("sha1");
        let mut entries = String::new();
        let mut blobs = BTreeMap::new();
        for index in 0..31_u64 {
            let body = index.to_le_bytes();
            let oid = source.blob(&body);
            entries.push_str(&format!("100644 blob {oid}\tf{index:04}\n"));
            blobs.insert(
                key(GitObjectFormat::Sha1, GitObjectKind::Blob, &oid),
                (oid, body),
            );
        }
        let root = tree(&source.tree(&entries));
        let (_, (last_oid, last_body)) = blobs.last_key_value().unwrap();
        if !cancelled {
            // Git links are canonicalized by key, so this missing leaf is
            // encountered after enough publications to replace the writer.
            source.remove(last_oid);
        }
        let metadata = Arc::new(ObservedMetadata {
            inner: MemoryMetadataStore::new().unwrap(),
            enabled: AtomicBool::new(true),
            largest_writer: AtomicUsize::new(0),
            publications: AtomicUsize::new(0),
            writers: Mutex::new(BTreeSet::new()),
            published: Mutex::new(BTreeMap::new()),
        });
        let repository = Repository::with_formats(
            MemoryBlobStore::new(),
            metadata.clone(),
            FormatRegistry::builtin(),
            FormatLimits {
                max_batch_objects: 1,
                ..Default::default()
            },
        );
        let observed = metadata.clone();
        let result = repository
            .import(
                source
                    .request(vec![root.clone()])
                    .with_concurrency(1.try_into().unwrap())
                    .with_cancellation_check(move || {
                        cancelled && observed.publications.load(Ordering::SeqCst) >= 10
                    }),
            )
            .await;
        metadata.enabled.store(false, Ordering::SeqCst);
        if cancelled {
            assert!(matches!(result, Err(GitClosureImportError::Cancelled)));
        } else {
            assert!(matches!(result, Err(GitClosureImportError::Source(_))));
            assert_eq!(&source.blob(last_body), last_oid);
        }
        assert!(
            metadata.writers.lock().unwrap().len() >= 2,
            "failure must occur after an actual writer rotation"
        );
        let partial = repository.owned_retention_hold().await.unwrap();
        assert!(partial.object(&root).await.unwrap().is_some());
        assert_eq!(
            partial
                .snapshot()
                .validated_closures(std::slice::from_ref(&root))
                .await
                .unwrap(),
            [false]
        );
        flush_repository_leases().await.unwrap();
        repository.collect().await.unwrap();
        let resumed = repository
            .import(source.request(vec![root.clone()]))
            .await
            .unwrap();
        assert_eq!(
            resumed.report.imported_objects + resumed.report.reused_objects,
            32
        );
        assert_eq!(
            repository.verify_closure(&root).await.unwrap(),
            ClosureStatus::Complete { objects: 32 }
        );
        for (key, (_, body)) in &blobs {
            let (_, mut payload) = repository.open_payload(key).await.unwrap().unwrap();
            let mut actual = Vec::new();
            payload.read_to_end(&mut actual).await.unwrap();
            assert_eq!(&actual, body);
        }
    }
}
