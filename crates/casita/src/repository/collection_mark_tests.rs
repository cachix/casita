//! Mark traversal correctness and metadata read amplification.
use super::collection::{mark_named_roots, mark_pin_scopes};
use super::*;
use crate::metadata::{DataPin, PinScope};
use futures::stream::BoxStream;
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountedSnapshot {
    inner: Arc<dyn MetadataSnapshot>,
    reads: AtomicUsize,
}

#[async_trait]
impl MetadataSnapshot for CountedSnapshot {
    fn revision(&self) -> crate::RepositoryRevision {
        self.inner.revision()
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
    assert!(matches!(mode.as_str(), "named" | "pins"));
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
    for first in (0..parents).step_by(512) {
        let mut mutation = MetadataMutation::new();
        for index in first..(first + 512).min(parents) {
            let bytes = (if shared { 0 } else { index } as u64).to_le_bytes();
            let blob = BlobId::new(Digest::hash(&bytes));
            let leaf = ObjectKey::blob(blob);
            if expected.insert(leaf.clone()) {
                mutation.add_object(
                    formats
                        .verify(&leaf, &mut Cursor::new(bytes), &limits)
                        .await
                        .unwrap(),
                );
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
                mutation.set_root(format!("parent/{index}").parse().unwrap(), key.clone());
                roots.insert(key.clone());
            }
            expected.insert(key);
            if chain {
                child = Some(directory);
            }
        }
        revision = store.commit(&revision, mutation).await.unwrap().revision;
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
    };
    assert_eq!(snapshot.revision(), revision);
    let ledger = store.pin_store().await.unwrap();
    let token = ledger
        .register(DataPin {
            scope: PinScope::Closures(roots),
            catalog: None,
            resources: BTreeSet::new(),
        })
        .await
        .unwrap()
        .unwrap();
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
            mark_pin_scopes(&snapshot, &inventory, &mut marked, count, &area)
                .await
                .unwrap();
            marked
        };
        let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap();
        let reads = snapshot.reads.load(Ordering::Relaxed);
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
            "nanos": nanos, "record_reads": reads,
            "spill_files": metrics.files_opened, "spill_peak_bytes": metrics.peak_bytes,
        }));
    }
    ledger.release(&token).await.unwrap();
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
