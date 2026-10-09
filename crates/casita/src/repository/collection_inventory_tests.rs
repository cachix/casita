//! Full collection inventory correctness across spill and frontier boundaries.
use super::*;
use crate::{ChunkedBlobStore, Digest, MemoryMetadataStore};
use std::time::Instant;

pub(super) fn inventory_bytes(index: usize, multichunk: bool) -> Vec<u8> {
    (index as u64)
        .to_le_bytes()
        .into_iter()
        .cycle()
        .take(if multichunk { 4096 } else { 8 })
        .collect()
}

async fn inventory_case(count: usize, memory_limit: usize) -> serde_json::Value {
    let temporary = tempfile::tempdir().unwrap();
    let payloads = ChunkedBlobStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        object_store::path::Path::default(),
        1024,
    );
    let mut repository = Repository::new(payloads, MemoryMetadataStore::new().unwrap());
    // Fixture publication is outside the measurement. Restore the production
    // limit before planning so large cases exercise the same collection policy.
    let batch_limit = repository.limits.max_batch_objects;
    repository.limits.max_batch_objects = batch_limit.max(count + 6);
    let mutation = repository.mutation_session().await.unwrap();
    let mut staged = Vec::new();
    let mut entries = Vec::new();
    let mut live = Vec::new();
    for index in 0..count {
        let bytes = inventory_bytes(index, index + 1 < count);
        let blob = mutation.stage_blob(&bytes).await.unwrap();
        let payload = blob.record().payload();
        entries.push((
            PathComponent::try_from(format!("file-{index:08}").as_str()).unwrap(),
            Node::File {
                digest: payload,
                size: bytes.len() as u64,
                executable: false,
            },
        ));
        // Multiple edges to the same object must not change reachability.
        if index % 7 == 0 {
            entries.push((
                PathComponent::try_from(format!("shared-{index:08}").as_str()).unwrap(),
                Node::File {
                    digest: payload,
                    size: bytes.len() as u64,
                    executable: false,
                },
            ));
        }
        live.push((blob.record().clone(), payload, bytes));
        staged.push(blob);
    }
    let directory = Directory::try_from_iter(entries).unwrap();
    let root = mutation.stage_directory(&directory).await.unwrap();
    let root_key = root.record().key().clone();
    let root_record = root.record().clone();
    let root_payload = root.record().payload();
    let root_bytes = directory.encode();
    staged.push(root);
    let mut orphans = Vec::new();
    for index in 0..5u8 {
        let orphan = mutation
            .stage_blob(&inventory_bytes(count + 1 + usize::from(index), true))
            .await
            .unwrap();
        orphans.push((orphan.record().key().clone(), orphan.record().payload()));
        staged.push(orphan);
    }
    mutation
        .publish_rooted(
            staged,
            RootName::try_from("live").unwrap(),
            root_key.clone(),
        )
        .await
        .unwrap();
    drop(mutation);
    repository.limits.max_batch_objects = batch_limit;
    crate::flush_repository_leases().await.unwrap();
    let revision = repository.metadata().snapshot().await.unwrap().revision();
    let manifests: BTreeSet<_> = repository
        .payloads()
        .list_blobs()
        .try_collect()
        .await
        .unwrap();
    let mut expected_manifests = BTreeSet::new();
    let mut live_chunks = BTreeSet::new();
    let mut all_payloads = BTreeSet::from([root_payload]);
    for (index, (_, payload, _)) in live.iter().enumerate() {
        assert!(all_payloads.insert(*payload));
        let chunks = repository
            .payloads()
            .chunks(payload)
            .await
            .unwrap()
            .unwrap();
        if index + 1 < count {
            assert!(chunks.len() >= 2);
            expected_manifests.insert(*payload);
        } else {
            assert_eq!(chunks.len(), 1);
        }
        live_chunks.extend(chunks.into_iter().map(|chunk| chunk.digest));
    }
    let directory_chunks = repository
        .payloads()
        .chunks(&root_payload)
        .await
        .unwrap()
        .unwrap();
    if directory_chunks.len() > 1 {
        expected_manifests.insert(root_payload);
    }
    live_chunks.extend(directory_chunks.into_iter().map(|chunk| chunk.digest));
    for (_, payload) in &orphans {
        assert!(all_payloads.insert(*payload));
        assert!(
            repository
                .payloads()
                .chunks(payload)
                .await
                .unwrap()
                .unwrap()
                .len()
                >= 2
        );
        expected_manifests.insert(*payload);
    }
    assert_eq!(manifests, expected_manifests);
    assert_eq!(all_payloads.len(), count + 6);
    let before_chunks: BTreeSet<_> = repository
        .payloads()
        .list_chunks()
        .try_collect()
        .await
        .unwrap();
    // Bound every inventory and the widest shared-edge traversal frontier.
    let in_memory_limit = count * 16 + 128;
    assert!(in_memory_limit > before_chunks.len());
    assert!(in_memory_limit > manifests.len());
    assert!(in_memory_limit > count + count.div_ceil(7) + 1);
    let garbage_chunks = before_chunks.difference(&live_chunks).count();
    assert!(garbage_chunks > 0);
    let spill_directory = temporary.path().join(crate::spill::SPILL_DIRECTORY);
    let repository = repository
        .with_fs_coordination(temporary.path())
        .with_spill_limits(SpillLimits {
            max_memory_objects: memory_limit,
            ..SpillLimits::default()
        });
    let expected = CollectionPreview {
        logical_objects: 5,
        payload_blobs: 5,
        chunks: garbage_chunks,
    };
    let started = Instant::now();
    let plan = repository
        .collection_plan(
            repository.coordination.clone().lock_owned().await,
            repository.exclusive_fs().await.unwrap(),
            true,
        )
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(plan.preview, expected);
    let metrics = plan.logical.area.metrics();
    if memory_limit <= count {
        assert!(metrics.files_opened > 0);
    } else if memory_limit >= count * 16 + 128 {
        assert_eq!(metrics.files_opened, 0);
    }
    plan.logical.protection.collector.finish().await.unwrap();
    drop(plan);
    crate::flush_repository_leases().await.unwrap();
    assert_eq!(
        repository.metadata().snapshot().await.unwrap().revision(),
        revision
    );
    if spill_directory.exists() {
        assert!(
            std::fs::read_dir(&spill_directory)
                .unwrap()
                .next()
                .is_none()
        );
    }
    let outcome = repository.collect().await.unwrap();
    assert_eq!(outcome.removed, expected);
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert_eq!(snapshot.object(&root_key).await.unwrap(), Some(root_record));
    assert_eq!(
        snapshot
            .root(&RootName::try_from("live").unwrap())
            .await
            .unwrap(),
        Some(root_key)
    );
    assert_eq!(
        repository
            .payloads()
            .read_to_vec(&root_payload)
            .await
            .unwrap()
            .unwrap(),
        root_bytes
    );
    for (key, payload) in orphans {
        assert!(snapshot.object(&key).await.unwrap().is_none());
        assert!(!repository.payloads().has(&payload).await.unwrap());
    }
    for (record, payload, bytes) in live {
        assert_eq!(
            snapshot.object(record.key()).await.unwrap(),
            Some(record.clone())
        );
        assert_eq!(
            repository
                .payloads()
                .read_to_vec(&payload)
                .await
                .unwrap()
                .unwrap(),
            bytes
        );
    }
    let after_chunks: BTreeSet<_> = repository
        .payloads()
        .list_chunks()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(after_chunks, live_chunks);
    if spill_directory.exists() {
        assert!(
            std::fs::read_dir(&spill_directory)
                .unwrap()
                .next()
                .is_none()
        );
    }
    serde_json::json!({"count": count, "memory_limit": memory_limit, "seconds": elapsed.as_secs_f64(),
        "spill_files": metrics.files_opened, "spill_peak_bytes": metrics.peak_bytes,
        "logical_removed": 5, "payloads_removed": 5, "chunks_removed": garbage_chunks,
        "correctness": "exact preview and sweep; all live reads; shared edges; manifest presence and elision; unchanged preview revision; spill cleanup"})
}

#[tokio::test]
async fn collection_inventories_match_across_spill_and_frontier_boundaries() {
    for memory_limit in [17, 257, 258, 259, 4240] {
        inventory_case(257, memory_limit).await;
    }
}

#[tokio::test]
async fn pinned_payloads_keep_chunks_across_spill_and_page_boundaries() {
    use crate::metadata::{DataPin, DataPinLease, PinResource, PinScope};

    let temporary = tempfile::tempdir().unwrap();
    let repository = Repository::new(
        ChunkedBlobStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            object_store::path::Path::default(),
            1024,
        ),
        MemoryMetadataStore::new().unwrap(),
    );
    let mutation = repository.mutation_session().await.unwrap();
    let mut staged = Vec::new();
    let mut entries = Vec::new();
    let mut pinned = Vec::new();
    // Every third payload is also published; every fifth has one chunk and
    // no stored manifest. The rest are reachable only through the pin below.
    for index in 0..300 {
        let bytes = inventory_bytes(index, index % 5 != 0);
        let blob = mutation.stage_blob(&bytes).await.unwrap();
        let payload = blob.record().payload();
        if index % 3 == 0 {
            entries.push((
                PathComponent::try_from(format!("file-{index:08}").as_str()).unwrap(),
                Node::File {
                    digest: payload,
                    size: bytes.len() as u64,
                    executable: false,
                },
            ));
            staged.push(blob);
        }
        pinned.push((index % 3 == 0, payload, bytes));
    }
    let directory = mutation
        .stage_directory(&Directory::try_from_iter(entries).unwrap())
        .await
        .unwrap();
    let root = directory.record().key().clone();
    staged.push(directory);
    let orphan = mutation
        .stage_blob(&inventory_bytes(1000, true))
        .await
        .unwrap()
        .record()
        .payload();
    mutation
        .publish_rooted(staged, RootName::try_from("live").unwrap(), root)
        .await
        .unwrap();
    drop(mutation);
    crate::flush_repository_leases().await.unwrap();
    let repository = repository
        .with_fs_coordination(temporary.path())
        .with_spill_limits(SpillLimits {
            max_memory_objects: 17,
            ..SpillLimits::default()
        });
    let orphan_chunks: BTreeSet<_> = repository
        .payloads()
        .chunks(&orphan)
        .await
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|chunk| chunk.digest)
        .collect();
    let before_chunks: BTreeSet<_> = repository
        .payloads()
        .list_chunks()
        .try_collect()
        .await
        .unwrap();
    // Absent pins precede no physical data and must be tolerated.
    let resources: BTreeSet<_> = pinned
        .iter()
        .map(|(_, payload, _)| PinResource::Blob(*payload))
        .chain((0..3u8).map(|index| PinResource::Blob(BlobId::new(Digest::hash(&[b'p', index])))))
        .collect();
    assert!(resources.len() > CLOSURE_FRONTIER);
    let ledger = repository.metadata().pin_store().await.unwrap();
    let pin = DataPinLease::acquire(
        ledger,
        DataPin {
            scope: PinScope::Staging,
            catalog: None,
            resources,
        },
    )
    .await
    .unwrap();
    let outcome = repository.collect().await.unwrap();
    assert_eq!(outcome.removed.payload_blobs, 1);
    assert!(!repository.payloads().has(&orphan).await.unwrap());
    for (_, payload, bytes) in &pinned {
        assert_eq!(
            &repository
                .payloads()
                .read_to_vec(payload)
                .await
                .unwrap()
                .unwrap(),
            bytes
        );
    }
    assert_eq!(
        repository
            .payloads()
            .list_chunks()
            .try_collect::<BTreeSet<_>>()
            .await
            .unwrap(),
        &before_chunks - &orphan_chunks
    );
    drop(pin);
    crate::flush_repository_leases().await.unwrap();
    repository.collect().await.unwrap();
    for (published, payload, bytes) in pinned {
        if published {
            assert_eq!(
                repository
                    .payloads()
                    .read_to_vec(&payload)
                    .await
                    .unwrap()
                    .unwrap(),
                bytes
            );
        } else {
            assert!(!repository.payloads().has(&payload).await.unwrap());
        }
    }
    let spill = temporary.path().join(crate::spill::SPILL_DIRECTORY);
    assert!(!spill.exists() || std::fs::read_dir(&spill).unwrap().next().is_none());
}

#[tokio::test]
#[ignore = "run through benchmark collection-inventory"]
async fn benchmark_collection_inventory() {
    let count = std::env::var("CASITA_COLLECTION_INVENTORY_COUNT")
        .unwrap()
        .parse()
        .unwrap();
    let memory_limit = std::env::var("CASITA_COLLECTION_INVENTORY_MEMORY")
        .unwrap()
        .parse()
        .unwrap();
    println!(
        "collection_inventory_sample {}",
        inventory_case(count, memory_limit).await
    );
}
