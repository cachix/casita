//! Mark traversal correctness and metadata read amplification.
use super::collection::{mark_named_roots, mark_pin_scopes};
use super::*;
use crate::metadata::{DataPin, PinScope};
use futures::stream::BoxStream;
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountedSnapshot {
    inner: Arc<dyn MetadataSnapshot>,
    reads: AtomicUsize,
    scanned: Arc<AtomicUsize>,
}

#[async_trait]
impl MetadataSnapshot for CountedSnapshot {
    fn revision(&self) -> crate::RepositoryRevision {
        self.inner.revision()
    }

    fn generation(&self) -> Result<u64, MetadataError> {
        self.inner.generation()
    }

    fn objects_created_through(
        &self,
        generation: u64,
    ) -> BoxStream<'static, Result<ObjectRecord, MetadataError>> {
        let scanned = self.scanned.clone();
        Box::pin(
            self.inner
                .objects_created_through(generation)
                .map(move |record| {
                    if record.is_ok() {
                        scanned.fetch_add(1, Ordering::Relaxed);
                    }
                    record
                }),
        )
    }

    async fn object(&self, key: &ObjectKey) -> Result<Option<ObjectRecord>, MetadataError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.object(key).await
    }

    async fn object_batch(
        &self,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<ObjectRecord>>, MetadataError> {
        self.reads.fetch_add(keys.len(), Ordering::Relaxed);
        self.inner.object_batch(keys).await
    }

    async fn root(&self, name: &RootName) -> Result<Option<ObjectKey>, MetadataError> {
        self.inner.root(name).await
    }

    fn objects(&self) -> BoxStream<'static, Result<ObjectRecord, MetadataError>> {
        self.inner.objects()
    }

    fn roots(&self) -> BoxStream<'static, Result<crate::RootRecord, MetadataError>> {
        self.inner.roots()
    }
}

/// The traversal before named roots were marked ahead of their record reads.
/// Kept only in the test module to compare strategies with identical compiled
/// dependencies and fixture construction.
async fn legacy_mark_named_roots(
    snapshot: &dyn MetadataSnapshot,
    max_objects: usize,
    area: &SpillArea,
) -> Result<SpillSet<ObjectKey>, RepositoryError> {
    let mut marked = SpillSet::new(area.clone(), "marked");
    let mut queue = TraversalQueue::new(area.clone());
    let mut roots = snapshot.roots();
    while let Some(root) = roots.next().await {
        queue.push((None, root?.target().clone())).await?;
    }
    drop(roots);
    loop {
        let mut frontier = Vec::new();
        while frontier.len() < CLOSURE_FRONTIER {
            let Some(step) = queue.pop().await? else {
                break;
            };
            frontier.push(step);
        }
        if frontier.is_empty() {
            break;
        }
        let keys: Vec<_> = frontier.iter().map(|(_, key)| key.clone()).collect();
        let found = snapshot.object_batch(&keys).await?;
        for ((from, key), record) in frontier.into_iter().zip(found) {
            if !marked.insert(key.clone()).await? {
                continue;
            }
            if marked.len() > max_objects {
                return Err(RepositoryError::LimitExceeded(format!(
                    "collection mark exceeded {max_objects} objects"
                )));
            }
            let record = record.ok_or_else(|| {
                MetadataError::Corruption(match from {
                    Some(from) => format!("rooted object {from} links to missing {key}"),
                    None => format!("named root targets missing object {key}"),
                })
            })?;
            for target in record.links() {
                queue.push((Some(key.clone()), target.clone())).await?;
            }
        }
    }
    Ok(marked)
}

/// The pin traversal before scanned snapshot dependencies were filtered,
/// kept beside the named-root reference for same-binary comparisons.
async fn legacy_mark_pin_scopes(
    snapshot: &dyn MetadataSnapshot,
    pins: &crate::metadata::PinInventory,
    marked: &mut SpillSet<ObjectKey>,
    max_objects: usize,
    area: &SpillArea,
) -> Result<(), RepositoryError> {
    use crate::metadata::{PinResource, PinScope};

    let mut queue = TraversalQueue::new(area.clone());
    if let Some(generation) = pins
        .pins
        .values()
        .filter_map(|pin| match pin.scope {
            PinScope::Snapshot { generation } => Some(generation),
            _ => None,
        })
        .max()
    {
        let mut records = snapshot.objects_created_through(generation);
        while let Some(record) = records.next().await {
            let record = record?;
            if marked.insert(record.key().clone()).await? {
                // An old incomplete object can gain a committed dependency
                // later. Retain that dependency to keep the current graph valid.
                for target in record.links() {
                    queue.push((None, target.clone())).await?;
                }
            }
            if marked.len() > max_objects {
                return Err(RepositoryError::LimitExceeded(format!(
                    "collection mark exceeded {max_objects} objects"
                )));
            }
        }
    }

    for pin in pins.pins.values() {
        if let PinScope::Closures(roots) = &pin.scope {
            for root in roots {
                queue.push((None, root.clone())).await?;
            }
        }
        for resource in &pin.resources {
            if let PinResource::Object(key) = resource {
                queue.push((None, key.clone())).await?;
            }
        }
    }
    loop {
        let mut keys = Vec::new();
        while keys.len() < CLOSURE_FRONTIER {
            let Some((_, key)) = queue.pop().await? else {
                break;
            };
            keys.push(key);
        }
        if keys.is_empty() {
            return Ok(());
        }
        let records = snapshot.object_batch(&keys).await?;
        for (key, record) in keys.into_iter().zip(records) {
            let Some(record) = record else { continue };
            if !marked.insert(key).await? {
                continue;
            }
            if marked.len() > max_objects {
                return Err(RepositoryError::LimitExceeded(format!(
                    "collection mark exceeded {max_objects} objects"
                )));
            }
            for target in record.links() {
                queue.push((None, target.clone())).await?;
            }
        }
    }
}

/// Named-root and pin marking where each parent has one edge: to a shared leaf,
/// to its own leaf, or to the next directory of a single-root chain. Setup and
/// audits are untimed.
#[tokio::test]
#[ignore = "performance probe; run through benchmark run collection-mark"]
async fn benchmark_collection_mark() {
    use crate::Digest;
    use std::io::Cursor;
    use std::time::Instant;

    let setting = |name, default| {
        std::env::var(name)
            .ok()
            .map(|value| value.parse::<usize>().unwrap())
            .unwrap_or(default)
    };
    let parents = setting("CASITA_MARK_PARENTS", 257);
    let shape = std::env::var("CASITA_MARK_SHAPE").unwrap_or_else(|_| "shared".into());
    assert!(matches!(shape.as_str(), "shared" | "distinct" | "chain"));
    let shared = shape != "distinct";
    let chain = shape == "chain";
    let memory_limit = setting("CASITA_MARK_MEMORY_LIMIT", 256);
    let iterations = setting("CASITA_MARK_ITERATIONS", 3);
    let strategy = std::env::var("CASITA_MARK_STRATEGY").unwrap_or_else(|_| "current".into());
    let mode = std::env::var("CASITA_MARK_MODE").unwrap_or_else(|_| "named".into());
    assert!(parents > 0 && memory_limit > 0 && iterations > 0);
    assert!(matches!(strategy.as_str(), "legacy" | "current"));
    assert!(matches!(
        mode.as_str(),
        "named"
            | "pins"
            | "snapshot-full"
            | "snapshot-partial"
            | "snapshot-sparse"
            | "snapshot-forward"
    ));
    let cutoff = match mode.as_str() {
        "snapshot-full" | "snapshot-forward" => parents,
        "snapshot-partial" => (parents / 2).max(1),
        "snapshot-sparse" => 1,
        _ => 0,
    };
    let forward = mode == "snapshot-forward";
    let mut deferred_leaves = Vec::new();
    let mut snapshot_generation = None;
    let mut snapshot_objects = 0;
    let temporary = tempfile::tempdir().unwrap();
    let store = crate::TursoMetadataStore::open(temporary.path().join("metadata.sqlite"))
        .await
        .unwrap();
    let formats = FormatRegistry::builtin();
    let limits = FormatLimits::default();
    let mut revision = store.snapshot().await.unwrap().revision();
    let mut expected = BTreeSet::new();
    let mut roots = BTreeSet::new();
    let mut child: Option<Directory> = None;
    let mut first = 0;
    while first < parents {
        let end = if first < cutoff {
            (first + 512).min(cutoff)
        } else {
            (first + 512).min(parents)
        };
        let mut mutation = MetadataMutation::new();
        for index in first..end {
            let bytes = (if shared { 0 } else { index } as u64).to_le_bytes();
            let blob = BlobId::new(Digest::hash(&bytes));
            let leaf = ObjectKey::blob(blob);
            if expected.insert(leaf.clone()) {
                let record = formats
                    .verify(&leaf, &mut Cursor::new(bytes), &limits)
                    .await
                    .unwrap();
                if forward {
                    deferred_leaves.push(record);
                } else {
                    mutation.add_object(record);
                }
            }
            let node = if let Some(child) = child.as_ref() {
                Node::Directory {
                    digest: child.digest(),
                    size: child.size(),
                }
            } else {
                Node::File {
                    digest: blob,
                    size: 8,
                    executable: false,
                }
            };
            let directory = Directory::try_from_iter([(
                PathComponent::try_from(format!("leaf-{index}")).unwrap(),
                node,
            )])
            .unwrap();
            let key = ObjectKey::directory(directory.digest());
            mutation.add_object(
                formats
                    .verify(&key, &mut Cursor::new(directory.encode()), &limits)
                    .await
                    .unwrap(),
            );
            if !chain || index + 1 == parents {
                if !forward {
                    mutation.set_root(format!("parent/{index}").parse().unwrap(), key.clone());
                }
                roots.insert(key.clone());
            }
            expected.insert(key);
            if chain {
                child = Some(directory);
            }
        }
        revision = store.commit(&revision, mutation).await.unwrap().revision;
        if end == cutoff {
            snapshot_generation = Some(store.snapshot().await.unwrap().generation().unwrap());
            snapshot_objects = if forward { parents } else { expected.len() };
        }
        first = end;
    }
    if forward {
        let mut leaves = deferred_leaves.into_iter();
        loop {
            let records: Vec<_> = leaves.by_ref().take(512).collect();
            if records.is_empty() {
                break;
            }
            let mut mutation = MetadataMutation::new();
            mutation.add_objects(records);
            revision = store.commit(&revision, mutation).await.unwrap().revision;
        }
        // No named roots are needed for this snapshot-only mode. Keeping the
        // old parents unrooted lets their children appear in a later generation.
    }
    let count = if shared { parents + 1 } else { 2 * parents };
    assert_eq!(expected.len(), count);
    assert_eq!(roots.len(), if chain { 1 } else { parents });
    // Reopen before measuring, so this also exercises persisted records.
    drop(store);
    let store = crate::TursoMetadataStore::open(temporary.path().join("metadata.sqlite"))
        .await
        .unwrap();
    let snapshot = CountedSnapshot {
        inner: store.snapshot().await.unwrap(),
        reads: AtomicUsize::new(0),
        scanned: Arc::new(AtomicUsize::new(0)),
    };
    assert_eq!(snapshot.revision(), revision);
    let ledger = store.pin_store().await.unwrap();
    let token = ledger
        .register(DataPin {
            scope: if matches!(mode.as_str(), "snapshot-full" | "snapshot-forward") {
                PinScope::Staging
            } else {
                PinScope::Closures(roots)
            },
            catalog: None,
            resources: BTreeSet::new(),
        })
        .await
        .unwrap()
        .unwrap();
    let snapshot_token = if let Some(generation) = snapshot_generation {
        Some(
            ledger
                .register(DataPin {
                    scope: PinScope::Snapshot { generation },
                    catalog: None,
                    resources: BTreeSet::new(),
                })
                .await
                .unwrap()
                .unwrap(),
        )
    } else {
        None
    };
    let inventory = ledger.inventory().await.unwrap();
    let mut samples = Vec::new();
    for iteration in 0..=iterations {
        let area = SpillArea::new(
            Some(temporary.path().join("spill")),
            SpillLimits {
                max_memory_objects: memory_limit,
                ..SpillLimits::default()
            },
        );
        snapshot.reads.store(0, Ordering::Relaxed);
        snapshot.scanned.store(0, Ordering::Relaxed);
        let started = Instant::now();
        let marked = if mode == "named" {
            if strategy == "legacy" {
                legacy_mark_named_roots(&snapshot, count, &area)
                    .await
                    .unwrap()
            } else {
                mark_named_roots(&snapshot, count, &area).await.unwrap()
            }
        } else {
            let mut marked = SpillSet::new(area.clone(), "pin-mark");
            if strategy == "legacy" && mode != "pins" {
                legacy_mark_pin_scopes(&snapshot, &inventory, &mut marked, count, &area)
                    .await
                    .unwrap();
            } else {
                mark_pin_scopes(&snapshot, &inventory, &mut marked, count, &area)
                    .await
                    .unwrap();
            }
            marked
        };
        let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap();
        let reads = snapshot.reads.load(Ordering::Relaxed);
        let scanned = snapshot.scanned.load(Ordering::Relaxed);
        assert_eq!(scanned, snapshot_objects);
        let metrics = area.metrics();
        assert_eq!(marked.len(), count);
        assert_eq!(marked.spilled(), count >= memory_limit);
        // Every expected key plus exact cardinality proves the full set.
        let keys: Vec<_> = expected.iter().collect();
        assert!(
            marked
                .contains_batch(&keys)
                .await
                .unwrap()
                .into_iter()
                .all(|v| v)
        );
        samples.push(serde_json::json!({
            "iteration": iteration, "warm": iteration > 0, "mode": mode,
            "nanos": nanos, "record_reads": reads, "scanned_records": scanned,
            "spill_files": metrics.files_opened, "spill_peak_bytes": metrics.peak_bytes,
        }));
    }
    ledger.release(&token).await.unwrap();
    if let Some(token) = snapshot_token {
        ledger.release(&token).await.unwrap();
    }
    println!(
        "mark_sample {}",
        serde_json::json!({
            "parents": parents, "shape": shape, "memory_limit": memory_limit,
            "strategy": strategy, "mode": mode,
            "objects": count, "edges": parents, "iterations": iterations,
            "samples": samples,
            "correctness": "exact marked keys, cardinality, spill boundary and reopened revision",
        })
    );
}

#[tokio::test]
async fn named_root_marking_reads_each_shared_record_once() {
    // 257 distinct parents cross the 256-key frontier boundary. All point to
    // one leaf, so both intra-frontier and earlier-frontier repeats occur.
    let repository = Repository::new(
        crate::MemoryBlobStore::new(),
        crate::MemoryMetadataStore::new().unwrap(),
    );
    let session = repository.mutation_session().await.unwrap();
    let leaf = session.stage_blob(b"shared").await.unwrap();
    let leaf_key = leaf.record().key().clone();
    let mut objects = vec![leaf];
    let mut roots = Vec::new();
    let mut expected = BTreeSet::from([leaf_key.clone()]);
    for index in 0..257 {
        let directory = Directory::try_from_iter([(
            PathComponent::try_from(format!("leaf-{index}")).unwrap(),
            Node::File {
                digest: BlobId::new(leaf_key.native_digest().unwrap()),
                size: 6,
                executable: false,
            },
        )])
        .unwrap();
        let object = session.stage_directory(&directory).await.unwrap();
        let key = object.record().key().clone();
        expected.insert(key.clone());
        roots.push(RootChange::Set {
            name: format!("parent/{index}").parse().unwrap(),
            target: key,
        });
        objects.push(object);
    }
    session.publish(objects, roots).await.unwrap();
    let snapshot = CountedSnapshot {
        inner: repository.metadata().snapshot().await.unwrap(),
        reads: AtomicUsize::new(0),
        scanned: Arc::new(AtomicUsize::new(0)),
    };
    for memory_limit in [8, 1000] {
        let area = SpillArea::new(
            None,
            SpillLimits {
                max_memory_objects: memory_limit,
                ..SpillLimits::default()
            },
        );
        snapshot.reads.store(0, Ordering::Relaxed);
        let marked = mark_named_roots(&snapshot, 258, &area).await.unwrap();
        assert_eq!(marked.len(), 258);
        assert_eq!(marked.spilled(), memory_limit == 8);
        for key in &expected {
            assert!(marked.contains(key).await.unwrap());
        }
        assert_eq!(
            snapshot.reads.load(Ordering::Relaxed),
            258,
            "shared named-root edges must not reread metadata"
        );
    }
}

#[tokio::test]
async fn collection_marks_continue_after_a_fully_repeated_frontier() {
    let repository = Repository::new(
        crate::MemoryBlobStore::new(),
        crate::MemoryMetadataStore::new().unwrap(),
    );
    let session = repository.mutation_session().await.unwrap();
    let common = session.stage_blob(b"common").await.unwrap();
    let last = session.stage_blob(b"last").await.unwrap();
    let common_key = common.record().key().clone();
    let last_key = last.record().key().clone();
    let mut roots = Vec::new();
    let mut pins = crate::metadata::PinInventory::default();
    // The second 256-entry frontier is entirely repeated, but a new object
    // still follows it. Root names and pin tokens fix that traversal order.
    for index in 0..513 {
        let key = if index == 512 { &last_key } else { &common_key };
        roots.push(RootChange::Set {
            name: format!("alias/{index:04}").parse().unwrap(),
            target: key.clone(),
        });
        pins.pins.insert(
            format!("{index:064x}").parse().unwrap(),
            DataPin {
                scope: PinScope::Closures(BTreeSet::from([key.clone()])),
                catalog: None,
                resources: BTreeSet::new(),
            },
        );
    }
    session.publish(vec![common, last], roots).await.unwrap();
    let snapshot = repository.metadata().snapshot().await.unwrap();
    for memory_limit in [8, 1000] {
        let area = SpillArea::new(
            None,
            SpillLimits {
                max_memory_objects: memory_limit,
                ..SpillLimits::default()
            },
        );
        let named = mark_named_roots(snapshot.as_ref(), 2, &area).await.unwrap();
        let mut pinned = SpillSet::new(area.clone(), "pins");
        mark_pin_scopes(snapshot.as_ref(), &pins, &mut pinned, 2, &area)
            .await
            .unwrap();
        for marked in [&named, &pinned] {
            assert_eq!(marked.len(), 2);
            assert!(marked.contains(&common_key).await.unwrap());
            assert!(marked.contains(&last_key).await.unwrap());
        }
        assert!(matches!(
            mark_named_roots(snapshot.as_ref(), 1, &area).await,
            Err(RepositoryError::LimitExceeded(_))
        ));
        let mut limited = SpillSet::new(area.clone(), "limited");
        assert!(matches!(
            mark_pin_scopes(snapshot.as_ref(), &pins, &mut limited, 1, &area).await,
            Err(RepositoryError::LimitExceeded(_))
        ));
    }
}

#[tokio::test]
async fn snapshot_marks_do_not_reread_scanned_dependencies() {
    let repository = Repository::new(
        crate::MemoryBlobStore::new(),
        crate::MemoryMetadataStore::new().unwrap(),
    );
    let session = repository.mutation_session().await.unwrap();
    let leaf = session.stage_blob(b"shared").await.unwrap();
    let leaf_key = leaf.record().key().clone();
    let mut expected = BTreeSet::from([leaf_key.clone()]);
    let mut objects = vec![leaf];
    for index in 0..257 {
        let directory = Directory::try_from_iter([(
            PathComponent::try_from(format!("leaf-{index}")).unwrap(),
            Node::File {
                digest: BlobId::new(leaf_key.native_digest().unwrap()),
                size: 6,
                executable: false,
            },
        )])
        .unwrap();
        let object = session.stage_directory(&directory).await.unwrap();
        expected.insert(object.record().key().clone());
        objects.push(object);
    }
    session.publish_unrooted(objects).await.unwrap();
    let snapshot = CountedSnapshot {
        inner: repository.metadata().snapshot().await.unwrap(),
        reads: AtomicUsize::new(0),
        scanned: Arc::new(AtomicUsize::new(0)),
    };
    let mut inventory = crate::metadata::PinInventory::default();
    inventory.pins.insert(
        format!("{:064x}", 1).parse().unwrap(),
        DataPin {
            scope: PinScope::Snapshot {
                generation: snapshot.generation().unwrap(),
            },
            catalog: None,
            resources: BTreeSet::new(),
        },
    );
    for memory_limit in [8, 1000] {
        let area = SpillArea::new(
            None,
            SpillLimits {
                max_memory_objects: memory_limit,
                ..SpillLimits::default()
            },
        );
        let mut marked = SpillSet::new(area.clone(), "snapshot-marks");
        snapshot.reads.store(0, Ordering::Relaxed);
        snapshot.scanned.store(0, Ordering::Relaxed);
        mark_pin_scopes(&snapshot, &inventory, &mut marked, 258, &area)
            .await
            .unwrap();
        assert_eq!(marked.len(), expected.len());
        let keys: Vec<_> = expected.iter().collect();
        assert!(
            marked
                .contains_batch(&keys)
                .await
                .unwrap()
                .into_iter()
                .all(|v| v)
        );
        assert_eq!(snapshot.scanned.load(Ordering::Relaxed), 258);
        assert_eq!(
            snapshot.reads.load(Ordering::Relaxed),
            0,
            "snapshot dependencies already visited by the generation scan must not be reread"
        );
    }
}

#[tokio::test]
async fn snapshot_marks_follow_new_dependencies_and_mixed_pins_after_repeated_frontiers() {
    use crate::metadata::PinResource;
    use std::io::Cursor;
    let store = crate::MemoryMetadataStore::new().unwrap();
    let formats = FormatRegistry::builtin();
    let limits = FormatLimits::default();
    let make_leaf = |bytes: &[u8]| ObjectKey::blob(BlobId::new(crate::Digest::hash(bytes)));
    let leaf = make_leaf(b"later");
    let anchor = make_leaf(b"anchor");
    let extra = make_leaf(b"extra");
    let unrelated = make_leaf(b"unrelated");
    let missing = make_leaf(b"not published");
    let child = Directory::try_from_iter([(
        PathComponent::try_from("leaf").unwrap(),
        Node::File {
            digest: BlobId::new(leaf.native_digest().unwrap()),
            size: 5,
            executable: false,
        },
    )])
    .unwrap();
    let child_key = ObjectKey::directory(child.digest());
    let parent = Directory::try_from_iter([(
        PathComponent::try_from("child").unwrap(),
        Node::Directory {
            digest: child.digest(),
            size: child.size(),
        },
    )])
    .unwrap();
    let parent_key = ObjectKey::directory(parent.digest());
    let revision = store.snapshot().await.unwrap().revision();
    let mut old = MetadataMutation::new();
    for (key, bytes) in [
        (&parent_key, parent.encode()),
        (&anchor, b"anchor".to_vec()),
    ] {
        old.add_object(
            formats
                .verify(key, &mut Cursor::new(bytes), &limits)
                .await
                .unwrap(),
        );
    }
    let revision = store.commit(&revision, old).await.unwrap().revision;
    let generation = store.snapshot().await.unwrap().generation().unwrap();
    let mut later = MetadataMutation::new();
    for (key, bytes) in [
        (&child_key, child.encode()),
        (&leaf, b"later".to_vec()),
        (&extra, b"extra".to_vec()),
        (&unrelated, b"unrelated".to_vec()),
    ] {
        later.add_object(
            formats
                .verify(key, &mut Cursor::new(bytes), &limits)
                .await
                .unwrap(),
        );
    }
    store.commit(&revision, later).await.unwrap();
    let snapshot = CountedSnapshot {
        inner: store.snapshot().await.unwrap(),
        reads: AtomicUsize::new(0),
        scanned: Arc::new(AtomicUsize::new(0)),
    };
    let mut pins = crate::metadata::PinInventory::default();
    pins.pins.insert(
        format!("{:064x}", 0).parse().unwrap(),
        DataPin {
            scope: PinScope::Snapshot { generation },
            catalog: None,
            resources: BTreeSet::new(),
        },
    );
    // More than two frontiers of old aliases precede a new resource. Missing
    // unpublished inputs must not occupy a mark or consume the object limit.
    for index in 1..=514 {
        pins.pins.insert(
            format!("{index:064x}").parse().unwrap(),
            DataPin {
                scope: if index < 514 {
                    PinScope::Closures(BTreeSet::from([anchor.clone()]))
                } else {
                    PinScope::Staging
                },
                catalog: None,
                resources: if index == 514 {
                    BTreeSet::from([
                        PinResource::Object(extra.clone()),
                        PinResource::Object(missing.clone()),
                    ])
                } else {
                    BTreeSet::new()
                },
            },
        );
    }
    let expected = [parent_key, anchor, child_key, leaf, extra];
    for memory_limit in [2, 1000] {
        let area = SpillArea::new(
            None,
            SpillLimits {
                max_memory_objects: memory_limit,
                ..SpillLimits::default()
            },
        );
        let mut marked = SpillSet::new(area.clone(), "mixed-snapshot");
        snapshot.scanned.store(0, Ordering::Relaxed);
        mark_pin_scopes(&snapshot, &pins, &mut marked, expected.len(), &area)
            .await
            .unwrap();
        assert_eq!(snapshot.scanned.load(Ordering::Relaxed), 2);
        assert_eq!(marked.len(), expected.len());
        assert!(
            marked
                .contains_batch(&expected)
                .await
                .unwrap()
                .into_iter()
                .all(|v| v)
        );
        assert!(!marked.contains(&unrelated).await.unwrap());
        assert!(!marked.contains(&missing).await.unwrap());
        let mut limited = SpillSet::new(area.clone(), "limited-snapshot");
        assert!(matches!(
            mark_pin_scopes(&snapshot, &pins, &mut limited, expected.len() - 1, &area).await,
            Err(RepositoryError::LimitExceeded(_))
        ));
    }
}

#[tokio::test]
async fn snapshot_dependency_prefix_preserves_following_resources_at_frontier_boundaries() {
    use crate::metadata::PinResource;
    for parents in [255, 256, 257] {
        let repository = Repository::new(
            crate::MemoryBlobStore::new(),
            crate::MemoryMetadataStore::new().unwrap(),
        );
        let session = repository.mutation_session().await.unwrap();
        let leaf = session.stage_blob(b"old").await.unwrap();
        let leaf_key = leaf.record().key().clone();
        let mut expected = BTreeSet::from([leaf_key.clone()]);
        let mut objects = vec![leaf];
        for index in 0..parents {
            let directory = Directory::try_from_iter([(
                PathComponent::try_from(format!("leaf-{index}")).unwrap(),
                Node::File {
                    digest: BlobId::new(leaf_key.native_digest().unwrap()),
                    size: 3,
                    executable: false,
                },
            )])
            .unwrap();
            let object = session.stage_directory(&directory).await.unwrap();
            expected.insert(object.record().key().clone());
            objects.push(object);
        }
        session.publish_unrooted(objects).await.unwrap();
        let generation = repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .generation()
            .unwrap();
        let later = session.stage_blob(b"new resource").await.unwrap();
        let later_key = later.record().key().clone();
        expected.insert(later_key.clone());
        session.publish_unrooted(vec![later]).await.unwrap();
        let snapshot = CountedSnapshot {
            inner: repository.metadata().snapshot().await.unwrap(),
            reads: AtomicUsize::new(0),
            scanned: Arc::new(AtomicUsize::new(0)),
        };
        let mut pins = crate::metadata::PinInventory::default();
        pins.pins.insert(
            format!("{:064x}", 1).parse().unwrap(),
            DataPin {
                scope: PinScope::Snapshot { generation },
                catalog: None,
                resources: BTreeSet::from([PinResource::Object(later_key)]),
            },
        );
        for memory_limit in [8, 1000] {
            let area = SpillArea::new(
                None,
                SpillLimits {
                    max_memory_objects: memory_limit,
                    ..SpillLimits::default()
                },
            );
            let mut marked = SpillSet::new(area.clone(), "snapshot-prefix");
            snapshot.reads.store(0, Ordering::Relaxed);
            snapshot.scanned.store(0, Ordering::Relaxed);
            mark_pin_scopes(&snapshot, &pins, &mut marked, expected.len(), &area)
                .await
                .unwrap();
            assert_eq!(marked.len(), expected.len());
            let keys: Vec<_> = expected.iter().collect();
            assert!(
                marked
                    .contains_batch(&keys)
                    .await
                    .unwrap()
                    .into_iter()
                    .all(|v| v)
            );
            assert_eq!(snapshot.scanned.load(Ordering::Relaxed), parents + 1);
            assert_eq!(snapshot.reads.load(Ordering::Relaxed), 1);
        }
    }
}

#[tokio::test]
async fn snapshot_new_dependencies_remain_complete_after_multiple_missed_frontiers() {
    use crate::metadata::PinResource;
    use std::io::Cursor;

    for parents in [511, 512, 513] {
        let store = crate::MemoryMetadataStore::new().unwrap();
        let formats = FormatRegistry::builtin();
        let limits = FormatLimits::default();
        let leaf = ObjectKey::blob(BlobId::new(crate::Digest::hash(b"leaf")));
        let extra = ObjectKey::blob(BlobId::new(crate::Digest::hash(b"resource")));
        let missing = ObjectKey::blob(BlobId::new(crate::Digest::hash(b"absent")));
        let mut expected = BTreeSet::from([leaf.clone(), extra.clone()]);
        let mut old = MetadataMutation::new();
        let mut later = MetadataMutation::new();
        for index in 0..parents {
            let child = Directory::try_from_iter([(
                PathComponent::try_from(format!("leaf-{index}")).unwrap(),
                Node::File {
                    digest: BlobId::new(leaf.native_digest().unwrap()),
                    size: 4,
                    executable: false,
                },
            )])
            .unwrap();
            let parent = Directory::try_from_iter([(
                PathComponent::try_from("child").unwrap(),
                Node::Directory {
                    digest: child.digest(),
                    size: child.size(),
                },
            )])
            .unwrap();
            for (directory, mutation) in [(&parent, &mut old), (&child, &mut later)] {
                let key = ObjectKey::directory(directory.digest());
                expected.insert(key.clone());
                mutation.add_object(
                    formats
                        .verify(&key, &mut Cursor::new(directory.encode()), &limits)
                        .await
                        .unwrap(),
                );
            }
        }
        let revision = store.snapshot().await.unwrap().revision();
        let revision = store.commit(&revision, old).await.unwrap().revision;
        let generation = store.snapshot().await.unwrap().generation().unwrap();
        for (key, bytes) in [
            (&leaf, b"leaf".as_slice()),
            (&extra, b"resource".as_slice()),
        ] {
            later.add_object(
                formats
                    .verify(key, &mut Cursor::new(bytes), &limits)
                    .await
                    .unwrap(),
            );
        }
        store.commit(&revision, later).await.unwrap();
        let snapshot = store.snapshot().await.unwrap();
        let mut pins = crate::metadata::PinInventory::default();
        pins.pins.insert(
            format!("{:064x}", 1).parse().unwrap(),
            DataPin {
                scope: PinScope::Snapshot { generation },
                catalog: None,
                resources: BTreeSet::from([
                    PinResource::Object(extra.clone()),
                    PinResource::Object(missing.clone()),
                ]),
            },
        );
        assert_eq!(expected.len(), 2 * parents + 2);
        for memory_limit in [8, 10000] {
            let area = SpillArea::new(
                None,
                SpillLimits {
                    max_memory_objects: memory_limit,
                    ..SpillLimits::default()
                },
            );
            let mut marked = SpillSet::new(area.clone(), "new-snapshot-dependencies");
            mark_pin_scopes(snapshot.as_ref(), &pins, &mut marked, expected.len(), &area)
                .await
                .unwrap();
            assert_eq!(marked.len(), expected.len());
            let keys: Vec<_> = expected.iter().collect();
            assert!(
                marked
                    .contains_batch(&keys)
                    .await
                    .unwrap()
                    .into_iter()
                    .all(|v| v)
            );
            assert!(!marked.contains(&missing).await.unwrap());
            let mut limited = SpillSet::new(area.clone(), "limited-new-dependencies");
            assert!(matches!(
                mark_pin_scopes(
                    snapshot.as_ref(),
                    &pins,
                    &mut limited,
                    expected.len() - 1,
                    &area
                )
                .await,
                Err(RepositoryError::LimitExceeded(_))
            ));
        }
    }
}
