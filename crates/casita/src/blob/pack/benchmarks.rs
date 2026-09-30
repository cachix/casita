//! Private production-format scale probes driven by `benchmark run catalog-index`.

use super::delta::{
    CatalogBase, CatalogRunQueryRef, CatalogRunRef, DecodedIndexDelta, DeltaCatalog,
    IndexMutations, MAX_INLINE_DELTA_BYTES, apply_index_delta, delta_catalog_needs_compaction,
    encode_decoded_index_delta, encode_delta_catalog, encode_index_mutations, reopen_delta_catalog,
};
use super::run::{
    CatalogRun, CatalogRunChunkBlockRef, CatalogRunQueryIndex, catalog_run_query_ref,
    decode_catalog_run, decode_catalog_run_routing, encode_catalog_run, encode_run_query_directory,
    merge_catalog_runs,
};
use super::shard::{
    DEFAULT_SHARD_TARGET_BYTES, ShardMap, decode_shard_map, encode_index_shards, encode_shard_map,
    recommended_shard_bits,
};
use super::*;

/// Production catalog encodings at both externalization boundaries, committed
/// through the real metadata backend. No payload objects are materialized.
#[tokio::test]
#[cfg(feature = "native")]
#[ignore = "performance probe; run through benchmark run catalog-wal"]
async fn benchmark_catalog_wal() {
    use crate::metadata::{MetadataMutation, MetadataStore, TursoMetadataStore};
    let case = std::env::var("CASITA_WAL_CASE").unwrap();
    let mode = std::env::var("CASITA_WAL_MODE").unwrap();
    let held = std::env::var("CASITA_WAL_HELD").unwrap() == "true";
    let iterations: usize = std::env::var("CASITA_WAL_ITERATIONS")
        .unwrap()
        .parse()
        .unwrap();
    assert!(iterations > 0);
    let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let packed = PackedChunks::open_with_state_catalog(
        objects.clone(),
        Path::from("wal-catalog"),
        u64::MAX,
        0,
        &PackedChunks::empty_state_catalog().unwrap(),
    )
    .await
    .unwrap();
    let catalog = if case.starts_with("base-") {
        let mut index = Index {
            manifests_complete: true,
            ..Index::default()
        };
        let empty_bytes = encode_index_checkpoint(&index, Digest::from(blake3::hash(b"fixture")))
            .unwrap()
            .len();
        let below = (INDEX_INLINE_BASE_MAX_BYTES - empty_bytes) / DIGEST_LEN;
        let count = match case.as_str() {
            "base-below" => below,
            "base-above" => below + 1,
            _ => panic!("unknown case"),
        };
        for ordinal in 0..count {
            index
                .manifests
                .insert(BlobId::new(benchmark_ordinal_digest(1, ordinal as u64)));
        }
        let (_, catalog) = packed
            .build_index_catalog(
                &index,
                None,
                true,
                true,
                &IndexCatalogWitness::default(),
                &LazyCatalogOverlay::default(),
            )
            .await
            .unwrap();
        let root = decode_delta_catalog(&catalog).unwrap();
        assert_eq!(
            matches!(root.base, CatalogBase::Inline(_)),
            case == "base-below"
        );
        let reopened = PackedChunks::open_with_state_catalog(
            objects.clone(),
            Path::from("wal-catalog"),
            u64::MAX,
            0,
            &catalog,
        )
        .await
        .unwrap();
        // Inline checkpoints materialize at open; sharded catalogs stay lazy.
        let loaded = reopened.index.read().unwrap().manifests.len();
        if case == "base-below" {
            assert_eq!(loaded, count);
        }
        for ordinal in [0, count / 2, count - 1] {
            assert!(
                reopened
                    .catalog_contains_manifest(BlobId::new(benchmark_ordinal_digest(
                        1,
                        ordinal as u64
                    )))
                    .await
                    .unwrap()
            );
        }
        assert!(
            !reopened
                .catalog_contains_manifest(BlobId::new(benchmark_ordinal_digest(2, 0)))
                .await
                .unwrap()
        );
        catalog.to_vec()
    } else {
        let count = match case.as_str() {
            "small" => 1,
            // Bracket the 1 MiB encoded-delta limit, including format overhead.
            "delta-below" => MAX_INLINE_DELTA_BYTES / DIGEST_LEN - 16,
            "delta-above" => MAX_INLINE_DELTA_BYTES / DIGEST_LEN + 1,
            _ => panic!("unknown case"),
        };
        for ordinal in 0..count {
            packed.register_manifest(BlobId::new(benchmark_ordinal_digest(1, ordinal as u64)));
        }
        let catalog = packed.prepare_state_catalog().await.unwrap().unwrap();
        packed.finish_state_catalog(true).unwrap();
        let root = decode_delta_catalog(&catalog).unwrap();
        assert_eq!(root.deltas.is_empty(), case == "delta-above");
        let reopened = PackedChunks::open_with_state_catalog(
            objects.clone(),
            Path::from("wal-catalog"),
            u64::MAX,
            0,
            &catalog,
        )
        .await
        .unwrap();
        for ordinal in [0, count / 2, count - 1] {
            assert!(
                reopened
                    .catalog_contains_manifest(BlobId::new(benchmark_ordinal_digest(
                        1,
                        ordinal as u64
                    )))
                    .await
                    .unwrap()
            );
        }
        assert!(
            !reopened
                .catalog_contains_manifest(BlobId::new(benchmark_ordinal_digest(2, 0)))
                .await
                .unwrap()
        );
        catalog
    };
    let catalog_bytes = catalog.len();
    let directory = tempfile::tempdir().unwrap();
    let external_publisher = if mode == "external" {
        let root = directory.path().join("blobs");
        std::fs::create_dir(&root).unwrap();
        let filesystem = object_store::local::LocalFileSystem::new_with_prefix(&root).unwrap();
        let durability = LocalDurability::new(filesystem.clone(), &root).unwrap();
        // Copy the fixture's immutable dependencies before timing. The real
        // root publisher then durably uploads the root on each measured commit.
        let mut listed = objects.list(None);
        while let Some(object) = listed.try_next().await.unwrap() {
            let bytes = objects
                .get(&object.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            durability.put(&object.location, bytes).await.unwrap();
        }
        Some(
            PackedChunks::open_with_initial_catalog(
                Arc::new(filesystem),
                Path::from("wal-catalog"),
                u64::MAX,
                0,
                Some(&catalog),
                Some(durability),
            )
            .await
            .unwrap(),
        )
    } else {
        None
    };
    // SQL-only control: a digest + generation + length. This does not implement
    // external-object durability, migration, or GC and is not an end-to-end fix.
    let stored = if let Some(publisher) = &external_publisher {
        publisher.externalize_state_catalog(&catalog).await.unwrap()
    } else if mode == "reference" {
        let mut reference = blake3::hash(&catalog).as_bytes().to_vec();
        reference.extend_from_slice(&1_u64.to_le_bytes());
        reference.extend_from_slice(&(catalog.len() as u64).to_le_bytes());
        reference
    } else {
        assert!(matches!(mode.as_str(), "resubmit" | "metadata-only"));
        catalog.clone()
    };
    let path = directory.path().join("casita.sqlite");
    let wal = directory.path().join("casita.sqlite-wal");
    let db = crate::sqlite::TursoDb::open(&path).unwrap();
    let metadata = TursoMetadataStore::from_db(db.clone()).await.unwrap();
    let mut revision = metadata.snapshot().await.unwrap().revision();
    let mut seed = MetadataMutation::new();
    seed.set_payload_catalog(stored.clone());
    revision = metadata.commit(&revision, seed).await.unwrap().revision;
    metadata.compact_transient_state().await.unwrap();
    let initial_revision = revision;
    let retained = if held {
        Some(metadata.snapshot().await.unwrap())
    } else {
        None
    };
    let size = |p: &std::path::Path| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0);
    let initial_wal_bytes = size(&wal);
    let mut wal_sizes = Vec::new();
    let started = std::time::Instant::now();
    for _ in 0..iterations {
        let mut mutation = MetadataMutation::new();
        if let Some(publisher) = &external_publisher {
            mutation
                .set_payload_catalog(publisher.externalize_state_catalog(&catalog).await.unwrap());
        } else if mode != "metadata-only" {
            mutation.set_payload_catalog(stored.clone());
        }
        revision = metadata.commit(&revision, mutation).await.unwrap().revision;
        wal_sizes.push(size(&wal));
    }
    let nanos = started.elapsed().as_nanos() as u64;
    let before_checkpoint = size(&wal);
    let checkpoint = db
        .write(|connection| {
            Box::pin(async move {
                let mut rows = connection
                    .query("PRAGMA wal_checkpoint(PASSIVE)", ())
                    .await?;
                let row = rows.next().await?.expect("checkpoint row");
                Ok((
                    row.get::<i64>(0)?,
                    row.get::<Option<i64>>(1)?,
                    row.get::<Option<i64>>(2)?,
                ))
            })
        })
        .await
        .unwrap();
    if let Some(snapshot) = &retained {
        assert_eq!(snapshot.revision(), initial_revision);
        assert_eq!(snapshot.payload_catalog(), Some(stored.as_slice()));
    }
    drop(retained);
    metadata.compact_transient_state().await.unwrap();
    let after_release_bytes = size(&wal);
    assert_eq!(after_release_bytes, 0, "unheld WAL must truncate");
    let db_bytes = size(&path);
    drop(metadata);
    drop(db);
    let reopened = TursoMetadataStore::open(&path).await.unwrap();
    let snapshot = reopened.snapshot().await.unwrap();
    assert_eq!(snapshot.revision(), revision);
    assert_eq!(snapshot.generation().unwrap(), 1 + iterations as u64);
    assert_eq!(snapshot.payload_catalog(), Some(stored.as_slice()));
    if let Some(publisher) = external_publisher {
        assert_eq!(
            publisher
                .resolve_state_catalog(snapshot.payload_catalog().unwrap())
                .await
                .unwrap()
                .as_ref(),
            catalog
        );
    }
    println!(
        "catalog_wal_sample {}",
        serde_json::json!({
            "case": case, "mode": mode, "held": held, "iterations": iterations,
            "catalog_bytes": catalog_bytes, "stored_bytes": stored.len(), "nanos": nanos,
            "initial_wal_bytes": initial_wal_bytes, "wal_sizes": wal_sizes,
            "before_checkpoint_bytes": before_checkpoint, "checkpoint": checkpoint,
            "after_release_bytes": after_release_bytes, "database_bytes": db_bytes,
            "correctness": "catalog boundary and membership, retained snapshot, exact reopened catalog and generation, released WAL truncated"
        })
    );
}

fn encode_inline_catalog(generation: u64, checkpoint: &[u8]) -> Bytes {
    encode_delta_catalog(&DeltaCatalog {
        sidecars: None,
        generation,
        base: CatalogBase::Inline(Bytes::copy_from_slice(checkpoint)),
        runs: BTreeMap::new(),
        deltas: Vec::new(),
    })
    .unwrap()
}

fn decode_inline_catalog(catalog: &[u8]) -> (u64, Bytes) {
    let root = decode_delta_catalog(catalog).unwrap();
    let CatalogBase::Inline(checkpoint) = root.base else {
        panic!("benchmark catalog must have an inline base");
    };
    (root.generation, checkpoint)
}

fn benchmark_peak_rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmHWM:")?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
        })
        .unwrap_or_default()
}

fn benchmark_median(mut values: Vec<u64>) -> u64 {
    values.sort_unstable();
    values[values.len() / 2]
}

fn benchmark_ordinal_digest(namespace: u8, ordinal: u64) -> Digest {
    let mut bytes = [0_u8; DIGEST_LEN];
    bytes[0] = namespace;
    bytes[DIGEST_LEN - 8..].copy_from_slice(&ordinal.to_be_bytes());
    Digest::from(bytes)
}

#[test]
#[ignore = "release-mode 500 TB run-routing map probe"]
fn benchmark_catalog_run_routing_scale() {
    let packs = std::env::var("CASITA_CATALOG_ROUTING_PACKS")
        .expect("CASITA_CATALOG_ROUTING_PACKS is required")
        .parse::<usize>()
        .expect("routing pack count must be an integer");
    let chunks = std::env::var("CASITA_CATALOG_ROUTING_CHUNKS")
        .expect("CASITA_CATALOG_ROUTING_CHUNKS is required")
        .parse::<usize>()
        .expect("routing chunk count must be an integer");
    let repetitions = std::env::var("CASITA_CATALOG_BENCH_REPETITIONS")
        .ok()
        .map(|value| value.parse::<usize>().expect("invalid repetition count"))
        .unwrap_or(3);
    assert!(packs > 0 && chunks > 0 && repetitions > 0);

    let block_entries = 1024_usize;
    let block_count = chunks.div_ceil(block_entries);
    let block_bytes = 100_000_u64;
    let blocks = (0..block_count)
        .map(|block| {
            let first_ordinal = block * block_entries;
            let last_ordinal = (first_ordinal + block_entries - 1).min(chunks - 1);
            CatalogRunChunkBlockRef {
                first: ChunkId::new(benchmark_ordinal_digest(1, first_ordinal as u64)),
                last: ChunkId::new(benchmark_ordinal_digest(1, last_ordinal as u64)),
                offset: block as u64 * block_bytes,
                encoded_bytes: block_bytes,
                digest: benchmark_ordinal_digest(3, block as u64),
            }
        })
        .collect::<Vec<_>>();
    let changed_packs = (0..packs)
        .map(|ordinal| PackId::new(benchmark_ordinal_digest(2, ordinal as u64)))
        .collect::<Vec<_>>();
    let directory_offset = block_count as u64 * block_bytes;
    let routing = Bytes::from(encode_run_query_directory(&CatalogRunQueryIndex {
        chunks: blocks,
        changed_packs,
    }));
    let run_digest = Digest::from(blake3::hash(b"500 TB catalog routing benchmark run"));
    let map = ShardMap {
        shard_bits: 13,
        chunks: Vec::new(),
        manifests: Vec::new(),
        packs: Vec::new(),
        run_routing: BTreeMap::from([(run_digest, routing.clone())]),
    };

    let mut encode_nanos = Vec::with_capacity(repetitions);
    let mut encoded = Bytes::new();
    for _ in 0..repetitions {
        let started = Instant::now();
        encoded = encode_shard_map(std::hint::black_box(&map)).unwrap();
        encode_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }

    let mut map_decode_nanos = Vec::with_capacity(repetitions);
    let mut decoded = None;
    for _ in 0..repetitions {
        let started = Instant::now();
        let value = decode_shard_map(std::hint::black_box(&encoded)).unwrap();
        map_decode_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        decoded = Some(value);
    }
    let decoded = decoded.unwrap();
    let decoded_routing = decoded.run_routing.get(&run_digest).unwrap().clone();
    assert_eq!(decoded_routing, routing);

    let reference = CatalogRunQueryRef {
        offset: directory_offset,
        encoded_bytes: routing.len() as u64,
        digest: Digest::from(blake3::hash(&routing)),
        routing: decoded_routing,
    };
    let mut routing_decode_nanos = Vec::with_capacity(repetitions);
    let mut index = None;
    for _ in 0..repetitions {
        let started = Instant::now();
        let value = decode_catalog_run_routing(std::hint::black_box(&reference)).unwrap();
        routing_decode_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        index = Some(value);
    }
    let index = index.unwrap();
    assert_eq!(index.chunks.len(), block_count);
    assert_eq!(index.changed_packs.len(), packs);
    assert!(index.changes_pack(&PackId::new(benchmark_ordinal_digest(
        2,
        (packs / 2) as u64
    ))));
    assert!(
        index
            .chunk_block(&ChunkId::new(benchmark_ordinal_digest(
                1,
                (chunks / 2) as u64
            )))
            .is_some()
    );

    println!("catalog_entries {chunks}");
    println!("catalog_routing_packs {packs}");
    println!("catalog_routing_chunks {chunks}");
    println!("catalog_routing_blocks {block_count}");
    println!("catalog_routing_bytes {}", routing.len());
    println!("catalog_routing_map_bytes {}", encoded.len());
    println!(
        "catalog_routing_encode_median_nanos {}",
        benchmark_median(encode_nanos)
    );
    println!(
        "catalog_routing_map_decode_median_nanos {}",
        benchmark_median(map_decode_nanos)
    );
    println!(
        "catalog_routing_decode_median_nanos {}",
        benchmark_median(routing_decode_nanos)
    );
    println!("catalog_routing_peak_rss_kib {}", benchmark_peak_rss_kib());
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "release-mode local catalog reclamation marker probe"]
async fn benchmark_catalog_reclaim_marker_probe() {
    let iterations = std::env::var("CASITA_CATALOG_MARKER_BENCH_ITERATIONS")
        .ok()
        .map(|value| value.parse::<u64>().expect("invalid marker iterations"))
        .unwrap_or(10_000);
    assert!(iterations > 0);
    let directory = tempfile::tempdir().unwrap();
    let objects: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap());
    let store = PackedChunks::open(objects, Path::from("payloads"), u64::MAX)
        .await
        .unwrap();

    assert!(!store.catalog_reclaim_due().await.unwrap());
    let started = Instant::now();
    for _ in 0..iterations {
        std::hint::black_box(store.catalog_reclaim_due().await.unwrap());
    }
    let absent_nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);

    store.mark_catalog_reclaim_due().await.unwrap();
    assert!(store.catalog_reclaim_due().await.unwrap());
    let started = Instant::now();
    for _ in 0..iterations {
        std::hint::black_box(store.catalog_reclaim_due().await.unwrap());
    }
    let present_nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);

    println!("catalog_marker_iterations {iterations}");
    println!(
        "catalog_marker_absent_nanos_per_probe {}",
        absent_nanos / iterations
    );
    println!(
        "catalog_marker_present_nanos_per_probe {}",
        present_nanos / iterations
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "release-mode local durable catalog publication probe"]
async fn benchmark_local_catalog_durable_publication() {
    let iterations = std::env::var("CASITA_CATALOG_DURABILITY_BENCH_ITERATIONS")
        .ok()
        .map(|value| value.parse::<u64>().expect("invalid durability iterations"))
        .unwrap_or(200);
    let batch_objects = std::env::var("CASITA_CATALOG_DURABILITY_BATCH_OBJECTS")
        .ok()
        .map(|value| value.parse::<u64>().expect("invalid durability batch size"))
        .unwrap_or(64);
    let batch_repetitions = std::env::var("CASITA_CATALOG_DURABILITY_BATCH_REPETITIONS")
        .ok()
        .map(|value| {
            value
                .parse::<u64>()
                .expect("invalid durability batch repetitions")
        })
        .unwrap_or(5);
    assert!(iterations > 0);
    assert!(batch_objects > 0 && batch_repetitions > 0);
    let directory = tempfile::tempdir().unwrap();
    let filesystem =
        object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap();
    let objects: Arc<dyn ObjectStore> = Arc::new(filesystem.clone());
    let durability = LocalDurability::new(filesystem, directory.path()).unwrap();
    let payload = Bytes::from(vec![0x5a; 4096]);
    let legacy_root = Path::from("legacy/pack-index-current");
    let durable_root = Path::from("durable/pack-index-current");

    objects
        .put(&legacy_root, payload.clone().into())
        .await
        .unwrap();
    durability
        .put(&durable_root, payload.clone())
        .await
        .unwrap();

    let mut legacy_root_nanos = Vec::with_capacity(iterations as usize);
    let mut durable_root_nanos = Vec::with_capacity(iterations as usize);
    let mut durable_object_nanos = Vec::with_capacity(iterations as usize);
    for ordinal in 0..iterations {
        let started = Instant::now();
        objects
            .put(&legacy_root, payload.clone().into())
            .await
            .unwrap();
        legacy_root_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));

        let started = Instant::now();
        durability
            .put(&durable_root, payload.clone())
            .await
            .unwrap();
        durable_root_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));

        let digest = benchmark_ordinal_digest(4, ordinal);
        let path = sharded_path(&Path::from("durable"), INDEXES_KIND, &digest);
        let started = Instant::now();
        durability.put(&path, payload.clone()).await.unwrap();
        durable_object_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }

    let legacy_root = benchmark_median(legacy_root_nanos);
    let durable_root = benchmark_median(durable_root_nanos);
    let durable_object = benchmark_median(durable_object_nanos);
    let batch_store = PackedChunks::open_with_cache_and_durability(
        objects,
        Path::from("batched"),
        u64::MAX,
        0,
        Some(durability.clone()),
    )
    .await
    .unwrap();
    let mut batch_nanos = Vec::with_capacity(batch_repetitions as usize);
    for repetition in 0..batch_repetitions {
        // Catalog objects are stored under the hash of their bytes. Build
        // distinct same-sized objects outside the timed region.
        let staged: Vec<(Digest, Bytes)> = (0..batch_objects)
            .map(|ordinal| {
                let ordinal = repetition
                    .checked_mul(batch_objects)
                    .unwrap()
                    .checked_add(ordinal)
                    .unwrap();
                let mut bytes = payload.to_vec();
                bytes[..8].copy_from_slice(&ordinal.to_be_bytes());
                let bytes = Bytes::from(bytes);
                (Digest::from(blake3::hash(&bytes)), bytes)
            })
            .collect();
        let started = Instant::now();
        let mut publication = batch_store.catalog_object_publication();
        for (digest, bytes) in staged {
            publication.put(digest, bytes).await.unwrap();
        }
        publication.finish().await.unwrap();
        batch_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
    let batch = benchmark_median(batch_nanos);
    println!("catalog_durability_iterations {iterations}");
    println!("catalog_legacy_root_median_nanos {legacy_root}");
    println!("catalog_durable_root_median_nanos {durable_root}");
    println!("catalog_durable_object_median_nanos {durable_object}");
    println!("catalog_durable_batch_objects {batch_objects}");
    println!("catalog_durable_batch_median_nanos {batch}");
    println!(
        "catalog_durable_batch_median_nanos_per_object {}",
        batch / batch_objects
    );
    println!(
        "catalog_durable_batch_speedup_x100 {}",
        durable_object
            .saturating_mul(batch_objects)
            .saturating_mul(100)
            / batch.max(1)
    );
    println!(
        "catalog_durable_root_slowdown_x100 {}",
        durable_root.saturating_mul(100) / legacy_root.max(1)
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "release-mode lazy sharded catalog generation probe"]
async fn benchmark_lazy_sharded_catalog_generate() {
    let entry_count = std::env::var("CASITA_CATALOG_BENCH_ENTRIES")
        .expect("CASITA_CATALOG_BENCH_ENTRIES is required")
        .parse::<usize>()
        .expect("catalog benchmark entry count must be an integer");
    let requested_shard_bits = std::env::var("CASITA_CATALOG_BENCH_SHARD_BITS")
        .ok()
        .map(|value| value.parse::<u8>().expect("invalid shard bits"))
        .unwrap_or(0);
    let shard_bits = if requested_shard_bits == 0 {
        recommended_shard_bits(entry_count as u64, DEFAULT_SHARD_TARGET_BYTES).unwrap()
    } else {
        requested_shard_bits
    };
    let input = std::env::var("CASITA_CATALOG_BENCH_INPUT")
        .expect("CASITA_CATALOG_BENCH_INPUT is required");
    let shard_dir = std::env::var("CASITA_CATALOG_BENCH_SHARD_DIR")
        .expect("CASITA_CATALOG_BENCH_SHARD_DIR is required");
    let root_path = std::env::var("CASITA_CATALOG_BENCH_SHARD_ROOT")
        .expect("CASITA_CATALOG_BENCH_SHARD_ROOT is required");
    std::fs::create_dir_all(&shard_dir).unwrap();

    let inline = std::fs::read(input).unwrap();
    let (_, checkpoint) = decode_inline_catalog(&inline);
    let index = decode_index_checkpoint_without_inventory(&checkpoint).unwrap();
    assert_eq!(index.chunks.len(), entry_count);
    let encoded = encode_index_shards(&index, shard_bits).unwrap();
    let map = decode_shard_map(&encoded.map).unwrap();
    let objects: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&shard_dir).unwrap());
    let base = Path::from("payloads");
    for (digest, bytes) in &encoded.objects {
        put_object(
            &objects,
            &sharded_path(&base, INDEXES_KIND, digest),
            bytes.clone(),
            true,
        )
        .await
        .unwrap();
    }
    put_object(
        &objects,
        &sharded_path(&base, INDEXES_KIND, &encoded.map_digest),
        encoded.map.clone(),
        true,
    )
    .await
    .unwrap();
    let root = encode_delta_catalog(&DeltaCatalog {
        sidecars: None,
        generation: 1,
        base: CatalogBase::Sharded {
            root: encoded.map_digest,
            shard_bits,
        },
        runs: BTreeMap::new(),
        deltas: Vec::new(),
    })
    .unwrap();
    std::fs::write(root_path, &root).unwrap();

    println!("catalog_entries {entry_count}");
    println!("catalog_lazy_root_bytes {}", root.len());
    println!("catalog_lazy_map_bytes {}", encoded.map.len());
    println!("catalog_lazy_shard_bits {shard_bits}");
    println!("catalog_lazy_chunk_shards {}", map.chunks.len());
    println!("catalog_lazy_manifest_shards {}", map.manifests.len());
    println!("catalog_lazy_pack_shards {}", map.packs.len());
    println!("catalog_lazy_shard_objects {}", encoded.objects.len());
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "release-mode lazy sharded catalog operation probe"]
async fn benchmark_lazy_sharded_catalog_operations() {
    let entry_count = std::env::var("CASITA_CATALOG_BENCH_ENTRIES")
        .expect("CASITA_CATALOG_BENCH_ENTRIES is required")
        .parse::<usize>()
        .expect("catalog benchmark entry count must be an integer");
    let repetitions = std::env::var("CASITA_CATALOG_BENCH_REPETITIONS")
        .ok()
        .map(|value| value.parse::<usize>().expect("invalid repetition count"))
        .unwrap_or(3);
    let manifest_percent = std::env::var("CASITA_CATALOG_BENCH_MANIFEST_PERCENT")
        .ok()
        .map(|value| value.parse::<usize>().expect("invalid manifest percentage"))
        .unwrap_or(0);
    let expected_manifests =
        usize::try_from((entry_count as u128 * manifest_percent as u128) / 100).unwrap();
    let shard_dir = std::env::var("CASITA_CATALOG_BENCH_SHARD_DIR")
        .expect("CASITA_CATALOG_BENCH_SHARD_DIR is required");
    let root_path = std::env::var("CASITA_CATALOG_BENCH_SHARD_ROOT")
        .expect("CASITA_CATALOG_BENCH_SHARD_ROOT is required");
    let catalog = std::fs::read(root_path).unwrap();
    let root = decode_delta_catalog(&catalog).unwrap();
    let CatalogBase::Sharded { .. } = root.base else {
        panic!("lazy benchmark requires a sharded catalog base");
    };
    let objects: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&shard_dir).unwrap());
    #[cfg(feature = "s3")]
    let objects: Arc<dyn ObjectStore> =
        if let Ok(bucket) = std::env::var("CASITA_CATALOG_BENCH_S3_BUCKET") {
            use futures::TryStreamExt;
            let remote: Arc<dyn ObjectStore> = Arc::new(
                object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_allow_http(true)
                    .build()
                    .unwrap(),
            );
            // Upload the authenticated metadata fixture before measuring. No
            // payload materialization or synthetic request counter is involved.
            let metadata = objects.list(None).try_collect::<Vec<_>>().await.unwrap();
            for entry in metadata {
                let bytes = objects
                    .get(&entry.location)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                remote.put(&entry.location, bytes.into()).await.unwrap();
            }
            remote
        } else {
            objects
        };
    let base = Path::from("payloads");
    let open_reader = || {
        PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            CATALOG_SHARD_CACHE_BYTES,
            &catalog,
        )
    };

    let mut open_nanos = Vec::with_capacity(repetitions);
    let mut reader = None;
    for _ in 0..repetitions {
        let started = Instant::now();
        let opened = open_reader().await.unwrap();
        open_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        let stats = opened.read_stats();
        assert_eq!(stats.index_requests, 1);
        assert_eq!(stats.list_requests, 0);
        assert_eq!(stats.footer_range_requests, 0);
        assert_eq!(stats.index_fallbacks, 0);
        assert_eq!(opened.index.read().unwrap().chunks.len(), 0);
        reader = Some(opened);
    }
    let reader = reader.unwrap();
    let map = reader
        .lazy_catalog
        .read()
        .unwrap()
        .base
        .as_ref()
        .unwrap()
        .map
        .clone();
    let open_peak_rss_kib = benchmark_peak_rss_kib();
    let open_stats = reader.read_stats();

    let hit = ChunkId::new(blake3::hash(&(entry_count as u64 / 2).to_le_bytes()).into());
    let mut first_lookup_nanos = Vec::with_capacity(repetitions);
    let mut first_lookup_stats = PackReadStats::default();
    let mut lookup_reader = None;
    for _ in 0..repetitions {
        let opened = open_reader().await.unwrap();
        opened.reset_read_stats();
        let started = Instant::now();
        assert_eq!(opened.metadata(&hit).await.unwrap(), Some(8192));
        first_lookup_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        first_lookup_stats = opened.read_stats();
        assert_eq!(first_lookup_stats.index_requests, 2);
        assert!(first_lookup_stats.index_bytes < 256 * 1024);
        lookup_reader = Some(opened);
    }
    let lookup_reader = lookup_reader.unwrap();
    lookup_reader.reset_read_stats();
    let mut cached_lookup_nanos = Vec::with_capacity(repetitions);
    for _ in 0..repetitions {
        let started = Instant::now();
        assert_eq!(lookup_reader.metadata(&hit).await.unwrap(), Some(8192));
        cached_lookup_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
    let cached_lookup_stats = lookup_reader.read_stats();
    assert_eq!(cached_lookup_stats.index_requests, 0);

    let mut chunk_list_nanos = Vec::with_capacity(repetitions);
    let mut chunk_list_stats = PackReadStats::default();
    for _ in 0..repetitions {
        let opened = open_reader().await.unwrap();
        opened.reset_read_stats();
        let started = Instant::now();
        let mut chunks = opened.list();
        let mut count = 0usize;
        while chunks.try_next().await.unwrap().is_some() {
            count += 1;
        }
        chunk_list_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        assert_eq!(count, entry_count);
        chunk_list_stats = opened.read_stats();
        assert_eq!(chunk_list_stats.index_requests, map.chunks.len() as u64);
    }

    let mut manifest_list_nanos = Vec::with_capacity(repetitions);
    let mut manifest_list_stats = PackReadStats::default();
    for _ in 0..repetitions {
        let opened = open_reader().await.unwrap();
        opened.reset_read_stats();
        let started = Instant::now();
        let mut manifests = opened.list_manifests();
        let mut count = 0usize;
        while manifests.try_next().await.unwrap().is_some() {
            count += 1;
        }
        manifest_list_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        assert_eq!(count, expected_manifests);
        manifest_list_stats = opened.read_stats();
        assert_eq!(
            manifest_list_stats.index_requests,
            map.manifests.len() as u64
        );
    }

    let mut gc_nanos = Vec::with_capacity(repetitions);
    let mut gc_stats = PackReadStats::default();
    for _ in 0..repetitions {
        let opened = open_reader().await.unwrap();
        opened.enable_state_catalog();
        opened.reset_read_stats();
        let started = Instant::now();
        opened.finish_deletions(true).await.unwrap();
        gc_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        gc_stats = opened.read_stats();
        assert_eq!(gc_stats.index_requests, map.packs.len() as u64);
        assert_eq!(gc_stats.index_put_requests, 0);
    }
    let stream_peak_rss_kib = benchmark_peak_rss_kib();

    println!("catalog_entries {entry_count}");
    println!(
        "catalog_lazy_open_median_nanos {}",
        benchmark_median(open_nanos)
    );
    println!("catalog_lazy_open_peak_rss_kib {open_peak_rss_kib}");
    println!(
        "catalog_lazy_open_index_requests {}",
        open_stats.index_requests
    );
    println!(
        "catalog_lazy_open_list_requests {}",
        open_stats.list_requests
    );
    println!(
        "catalog_lazy_open_footer_requests {}",
        open_stats.footer_range_requests
    );
    println!(
        "catalog_lazy_first_lookup_median_nanos {}",
        benchmark_median(first_lookup_nanos)
    );
    println!(
        "catalog_lazy_first_lookup_requests {}",
        first_lookup_stats.index_requests
    );
    println!(
        "catalog_lazy_first_lookup_bytes {}",
        first_lookup_stats.index_bytes
    );
    println!(
        "catalog_lazy_cached_lookup_median_nanos {}",
        benchmark_median(cached_lookup_nanos)
    );
    println!(
        "catalog_lazy_cached_lookup_requests {}",
        cached_lookup_stats.index_requests
    );
    println!(
        "catalog_lazy_chunk_list_median_nanos {}",
        benchmark_median(chunk_list_nanos)
    );
    println!(
        "catalog_lazy_chunk_list_requests {}",
        chunk_list_stats.index_requests
    );
    println!(
        "catalog_lazy_manifest_list_median_nanos {}",
        benchmark_median(manifest_list_nanos)
    );
    println!(
        "catalog_lazy_manifest_list_requests {}",
        manifest_list_stats.index_requests
    );
    println!(
        "catalog_lazy_gc_median_nanos {}",
        benchmark_median(gc_nanos)
    );
    println!("catalog_lazy_gc_requests {}", gc_stats.index_requests);
    println!("catalog_lazy_stream_peak_rss_kib {stream_peak_rss_kib}");
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "release-mode streaming sharded catalog rebase probe"]
async fn benchmark_lazy_sharded_catalog_rebase() {
    let entry_count = std::env::var("CASITA_CATALOG_BENCH_ENTRIES")
        .expect("CASITA_CATALOG_BENCH_ENTRIES is required")
        .parse::<usize>()
        .expect("catalog benchmark entry count must be an integer");
    let repetitions = std::env::var("CASITA_CATALOG_BENCH_REPETITIONS")
        .ok()
        .map(|value| value.parse::<usize>().expect("invalid repetition count"))
        .unwrap_or(3);
    let added_chunks = std::env::var("CASITA_CATALOG_BENCH_REBASE_CHUNKS")
        .ok()
        .map(|value| value.parse::<u64>().expect("invalid rebase chunk count"))
        .unwrap_or(2048);
    let shard_dir = std::env::var("CASITA_CATALOG_BENCH_SHARD_DIR")
        .expect("CASITA_CATALOG_BENCH_SHARD_DIR is required");
    let root_path = std::env::var("CASITA_CATALOG_BENCH_SHARD_ROOT")
        .expect("CASITA_CATALOG_BENCH_SHARD_ROOT is required");
    let catalog = std::fs::read(root_path).unwrap();
    let objects: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&shard_dir).unwrap());
    let base = Path::from("payloads");

    let mut elapsed = Vec::with_capacity(repetitions);
    let mut measured = PackReadStats::default();
    let mut old_objects = 0;
    let mut new_objects = 0;
    for repetition in 0..repetitions {
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &catalog,
        )
        .await
        .unwrap();
        let mut candidate = writer.index.read().unwrap().clone();
        let lazy = writer.lazy_catalog.read().unwrap().clone();
        let old = lazy.base.as_ref().unwrap();
        old_objects = old.map.chunks.len() + old.map.manifests.len() + old.map.packs.len();

        let mut pack_key = Vec::from(b"catalog streaming rebase pack ".as_slice());
        pack_key.extend_from_slice(&(repetition as u64).to_le_bytes());
        let pack = PackId::new(blake3::hash(&pack_key).into());
        let entries = (0..added_chunks)
            .map(|ordinal| {
                let mut chunk_key = pack_key.clone();
                chunk_key.extend_from_slice(&ordinal.to_le_bytes());
                PackEntry {
                    digest: ChunkId::new(blake3::hash(&chunk_key).into()),
                    offset: ordinal * 256,
                    framed_len: 256,
                    uncompressed_len: 8192,
                }
            })
            .collect::<Vec<_>>();
        let pack_len =
            added_chunks * 256 + encode_footer(&entries).len() as u64 + PACK_TRAILER_LEN as u64;
        candidate.add_pack(pack, pack_len, entries);
        let mut mutations = IndexMutations::default();
        mutations.record_pack(pack);
        let mut frozen = decode_delta_catalog(&catalog).unwrap();
        let generation = frozen.generation + 1;
        let run = CatalogRun {
            first_generation: generation,
            last_generation: generation,
            delta: encode_index_mutations(&candidate, &mutations).unwrap(),
        };
        let run_bytes = encode_catalog_run(&run).unwrap();
        let run_reference = CatalogRunRef {
            digest: Digest::from(blake3::hash(&run_bytes)),
            first_generation: generation,
            last_generation: generation,
            encoded_bytes: run_bytes.len() as u64,
            query: Some(catalog_run_query_ref(&run_bytes).unwrap()),
        };
        put_object(
            &objects,
            &sharded_path(&base, INDEXES_KIND, &run_reference.digest),
            run_bytes,
            true,
        )
        .await
        .unwrap();
        frozen.generation = generation;
        frozen.runs.insert(0, run_reference);

        writer
            .read_caches
            .catalog_shard_cache
            .lock()
            .unwrap()
            .clear();
        writer.reset_read_stats();
        let started = Instant::now();
        let rebased = writer
            .rebase_catalog_shards_streaming(
                &frozen,
                lazy.base.as_ref().expect("sharded benchmark base"),
            )
            .await
            .unwrap();
        elapsed.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        measured = writer.read_stats();
        new_objects =
            rebased.map.chunks.len() + rebased.map.manifests.len() + rebased.map.packs.len();
        assert_eq!(measured.index_requests, old_objects as u64 + 1);
        assert_eq!(measured.index_put_requests, new_objects as u64 + 1);
    }

    println!("catalog_entries {entry_count}");
    println!("catalog_rebase_added_chunks {added_chunks}");
    println!("catalog_rebase_old_objects {old_objects}");
    println!("catalog_rebase_new_objects {new_objects}");
    println!("catalog_rebase_get_requests {}", measured.index_requests);
    println!("catalog_rebase_get_bytes {}", measured.index_bytes);
    println!(
        "catalog_rebase_put_requests {}",
        measured.index_put_requests
    );
    println!("catalog_rebase_put_bytes {}", measured.index_put_bytes);
    println!("catalog_rebase_median_nanos {}", benchmark_median(elapsed));
    println!("catalog_rebase_peak_rss_kib {}", benchmark_peak_rss_kib());
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "release-mode deferred immutable catalog run probe"]
async fn benchmark_lazy_sharded_catalog_run() {
    let entry_count = std::env::var("CASITA_CATALOG_BENCH_ENTRIES")
        .expect("CASITA_CATALOG_BENCH_ENTRIES is required")
        .parse::<usize>()
        .expect("catalog benchmark entry count must be an integer");
    let shard_dir = std::env::var("CASITA_CATALOG_BENCH_SHARD_DIR")
        .expect("CASITA_CATALOG_BENCH_SHARD_DIR is required");
    let root_path = std::env::var("CASITA_CATALOG_BENCH_SHARD_ROOT")
        .expect("CASITA_CATALOG_BENCH_SHARD_ROOT is required");
    let base_catalog = std::fs::read(root_path).unwrap();
    let mut root = decode_delta_catalog(&base_catalog).unwrap();
    let objects: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&shard_dir).unwrap());
    let base = Path::from("payloads");

    let pack = PackId::new(blake3::hash(b"catalog lazy benchmark run pack").into());
    let entries = (0..2048_u64)
        .map(|ordinal| {
            let mut key = Vec::from(b"catalog lazy benchmark run chunk ".as_slice());
            key.extend_from_slice(&ordinal.to_le_bytes());
            PackEntry {
                digest: ChunkId::new(blake3::hash(&key).into()),
                offset: ordinal * 256,
                framed_len: 256,
                uncompressed_len: 8192,
            }
        })
        .collect::<Vec<_>>();
    let hit = entries[entries.len() / 2].digest;
    let pack_len = 2048 * 256 + encode_footer(&entries).len() as u64 + PACK_TRAILER_LEN as u64;
    let mut patch = Index::default();
    patch.add_pack(pack, pack_len, entries);
    let mut mutations = IndexMutations::default();
    mutations.record_pack(pack);
    let run = CatalogRun {
        first_generation: root.generation + 1,
        last_generation: root.generation + 1,
        delta: encode_index_mutations(&patch, &mutations).unwrap(),
    };
    let encoded_run = encode_catalog_run(&run).unwrap();
    let run_digest = Digest::from(blake3::hash(&encoded_run));
    put_object(
        &objects,
        &sharded_path(&base, INDEXES_KIND, &run_digest),
        encoded_run.clone(),
        true,
    )
    .await
    .unwrap();
    root.generation += 1;
    let mut query = catalog_run_query_ref(&encoded_run).unwrap();
    let routing_bytes = query.routing.len();
    let CatalogBase::Sharded {
        root: old_map_digest,
        shard_bits,
    } = root.base
    else {
        panic!("lazy run benchmark requires a sharded base")
    };
    let old_map_bytes = objects
        .get(&sharded_path(&base, INDEXES_KIND, &old_map_digest))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let mut map = decode_shard_map(&old_map_bytes).unwrap();
    map.run_routing.insert(run_digest, query.routing.clone());
    let map_bytes = encode_shard_map(&map).unwrap();
    let map_digest = Digest::from(blake3::hash(&map_bytes));
    put_object(
        &objects,
        &sharded_path(&base, INDEXES_KIND, &map_digest),
        map_bytes.clone(),
        true,
    )
    .await
    .unwrap();
    root.base = CatalogBase::Sharded {
        root: map_digest,
        shard_bits,
    };
    query.routing = Bytes::new();
    root.runs.insert(
        0,
        CatalogRunRef {
            digest: run_digest,
            first_generation: run.first_generation,
            last_generation: run.last_generation,
            encoded_bytes: encoded_run.len() as u64,
            query: Some(query),
        },
    );
    let catalog = encode_delta_catalog(&root).unwrap();

    let started = Instant::now();
    let reader = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &catalog)
        .await
        .unwrap();
    let open_nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let open_stats = reader.read_stats();
    assert_eq!(open_stats.index_requests, 1);
    assert_eq!(open_stats.index_run_objects, 1);

    reader.reset_read_stats();
    let started = Instant::now();
    assert_eq!(reader.metadata(&hit).await.unwrap(), Some(8192));
    let first_nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let first_stats = reader.read_stats();
    assert_eq!(first_stats.index_requests, 1);
    assert!(first_stats.index_bytes < encoded_run.len() as u64);
    assert!(reader.index.read().unwrap().packs.is_empty());

    reader.reset_read_stats();
    let started = Instant::now();
    assert_eq!(reader.metadata(&hit).await.unwrap(), Some(8192));
    let cached_nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let cached_stats = reader.read_stats();
    assert_eq!(cached_stats.index_requests, 0);

    println!("catalog_entries {entry_count}");
    println!("catalog_lazy_run_bytes {}", encoded_run.len());
    println!("catalog_lazy_run_routing_bytes {routing_bytes}");
    println!("catalog_lazy_run_map_bytes {}", map_bytes.len());
    println!("catalog_lazy_run_root_bytes {}", catalog.len());
    println!("catalog_lazy_run_open_nanos {open_nanos}");
    println!(
        "catalog_lazy_run_open_requests {}",
        open_stats.index_requests
    );
    println!("catalog_lazy_run_first_nanos {first_nanos}");
    println!(
        "catalog_lazy_run_first_requests {}",
        first_stats.index_requests
    );
    println!("catalog_lazy_run_first_bytes {}", first_stats.index_bytes);
    println!("catalog_lazy_run_cached_nanos {cached_nanos}");
    println!(
        "catalog_lazy_run_cached_requests {}",
        cached_stats.index_requests
    );
    println!("catalog_lazy_run_peak_rss_kib {}", benchmark_peak_rss_kib());
}

#[test]
#[ignore = "release-mode catalog scaling probe"]
fn benchmark_index_catalog_scale() {
    fn median(mut values: Vec<u64>) -> u64 {
        values.sort_unstable();
        values[values.len() / 2]
    }

    fn peak_rss_kib() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| {
                status.lines().find_map(|line| {
                    line.strip_prefix("VmHWM:")?
                        .split_whitespace()
                        .next()?
                        .parse()
                        .ok()
                })
            })
            .unwrap_or_default()
    }

    let entry_count = std::env::var("CASITA_CATALOG_BENCH_ENTRIES")
        .expect("CASITA_CATALOG_BENCH_ENTRIES is required")
        .parse::<usize>()
        .expect("catalog benchmark entry count must be an integer");
    let repetitions = std::env::var("CASITA_CATALOG_BENCH_REPETITIONS")
        .ok()
        .map(|value| value.parse::<usize>().expect("invalid repetition count"))
        .unwrap_or(3);
    let manifest_percent = std::env::var("CASITA_CATALOG_BENCH_MANIFEST_PERCENT")
        .ok()
        .map(|value| value.parse::<usize>().expect("invalid manifest percentage"))
        .unwrap_or(0);
    assert!(entry_count > 0);
    assert!(repetitions > 0);
    assert!(manifest_percent <= 100);

    const ENTRIES_PER_PACK: usize = 2048;
    const FRAMED_LEN: u64 = 256;
    let mut index = Index::default();
    for (pack_ordinal, first) in (0..entry_count).step_by(ENTRIES_PER_PACK).enumerate() {
        let in_pack = ENTRIES_PER_PACK.min(entry_count - first);
        let entries = (0..in_pack)
            .map(|ordinal| {
                let global = first + ordinal;
                PackEntry {
                    digest: ChunkId::new(blake3::hash(&(global as u64).to_le_bytes()).into()),
                    offset: ordinal as u64 * FRAMED_LEN,
                    framed_len: FRAMED_LEN,
                    uncompressed_len: 8192,
                }
            })
            .collect::<Vec<_>>();
        let mut pack_key = Vec::from(b"catalog scale pack ".as_slice());
        pack_key.extend_from_slice(&(pack_ordinal as u64).to_le_bytes());
        let pack = PackId::new(blake3::hash(&pack_key).into());
        let pack_len = in_pack as u64 * FRAMED_LEN
            + encode_footer(&entries).len() as u64
            + PACK_TRAILER_LEN as u64;
        index.add_pack(pack, pack_len, entries);
    }
    index.rebuild_chunks();
    let manifest_count =
        usize::try_from((entry_count as u128 * manifest_percent as u128) / 100).unwrap();
    index.manifests.extend((0..manifest_count).map(|ordinal| {
        let mut key = Vec::from(b"catalog scale manifest ".as_slice());
        key.extend_from_slice(&(ordinal as u64).to_le_bytes());
        BlobId::new(blake3::hash(&key).into())
    }));
    index.manifests_complete = true;

    let inventory = Digest::from(blake3::hash(b"catalog scale inventory"));
    let mut encode_nanos = Vec::with_capacity(repetitions);
    let mut hash_nanos = Vec::with_capacity(repetitions);
    let mut decode_nanos = Vec::with_capacity(repetitions);
    let mut catalog_len = 0;
    for generation in 1..=repetitions {
        let started = Instant::now();
        let checkpoint = encode_index_checkpoint(&index, inventory).unwrap();
        let catalog = encode_inline_catalog(generation as u64, &checkpoint);
        encode_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        catalog_len = catalog.len();
        if generation == 1
            && let Ok(path) = std::env::var("CASITA_CATALOG_BENCH_OUTPUT")
        {
            std::fs::write(path, &catalog).unwrap();
        }

        let started = Instant::now();
        let (decoded_generation, encoded) = decode_inline_catalog(&catalog);
        assert_eq!(decoded_generation, generation as u64);
        hash_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));

        let started = Instant::now();
        let decoded = decode_index_checkpoint_without_inventory(&encoded).unwrap();
        decode_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        assert_eq!(decoded.chunks.len(), entry_count);
        assert_eq!(decoded.manifests.len(), manifest_count);
        assert!(decoded.manifests_complete);
    }

    println!("catalog_entries {entry_count}");
    println!("catalog_manifest_percent {manifest_percent}");
    println!("catalog_manifests {manifest_count}");
    println!("catalog_packs {}", index.packs.len());
    println!("catalog_bytes {catalog_len}");
    println!(
        "catalog_milli_bytes_per_entry {}",
        catalog_len as u64 * 1000 / entry_count as u64
    );
    println!("catalog_encode_median_nanos {}", median(encode_nanos));
    println!("catalog_hash_median_nanos {}", median(hash_nanos));
    println!("catalog_decode_median_nanos {}", median(decode_nanos));
    println!("catalog_peak_rss_kib {}", peak_rss_kib());
}

#[test]
#[ignore = "release-mode catalog decode RSS probe"]
fn benchmark_index_catalog_decode_only() {
    fn median(mut values: Vec<u64>) -> u64 {
        values.sort_unstable();
        values[values.len() / 2]
    }

    fn peak_rss_kib() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| {
                status.lines().find_map(|line| {
                    line.strip_prefix("VmHWM:")?
                        .split_whitespace()
                        .next()?
                        .parse()
                        .ok()
                })
            })
            .unwrap_or_default()
    }

    let entry_count = std::env::var("CASITA_CATALOG_BENCH_ENTRIES")
        .expect("CASITA_CATALOG_BENCH_ENTRIES is required")
        .parse::<usize>()
        .expect("catalog benchmark entry count must be an integer");
    let repetitions = std::env::var("CASITA_CATALOG_BENCH_REPETITIONS")
        .ok()
        .map(|value| value.parse::<usize>().expect("invalid repetition count"))
        .unwrap_or(3);
    let manifest_percent = std::env::var("CASITA_CATALOG_BENCH_MANIFEST_PERCENT")
        .ok()
        .map(|value| value.parse::<usize>().expect("invalid manifest percentage"))
        .unwrap_or(0);
    assert!(manifest_percent <= 100);
    let expected_manifests =
        usize::try_from((entry_count as u128 * manifest_percent as u128) / 100).unwrap();
    let path = std::env::var("CASITA_CATALOG_BENCH_INPUT")
        .expect("CASITA_CATALOG_BENCH_INPUT is required");
    let catalog = std::fs::read(path).unwrap();
    let catalog_len = catalog.len();
    let (_, checkpoint) = decode_inline_catalog(&catalog);
    drop(catalog);

    let mut decode_nanos = Vec::with_capacity(repetitions);
    for _ in 0..repetitions {
        let started = Instant::now();
        let decoded = decode_index_checkpoint_without_inventory(&checkpoint).unwrap();
        decode_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        assert_eq!(decoded.chunks.len(), entry_count);
        assert_eq!(decoded.manifests.len(), expected_manifests);
        assert!(decoded.manifests_complete);
    }
    println!("catalog_entries {entry_count}");
    println!("catalog_manifest_percent {manifest_percent}");
    println!("catalog_manifests {expected_manifests}");
    println!("catalog_bytes {catalog_len}");
    println!("catalog_decode_median_nanos {}", median(decode_nanos));
    println!("catalog_peak_rss_kib {}", peak_rss_kib());
}

#[test]
#[ignore = "release-mode catalog publication scaling probe"]
fn benchmark_index_catalog_publication_scale() {
    fn median(mut values: Vec<u64>) -> u64 {
        values.sort_unstable();
        values[values.len() / 2]
    }

    let entry_count = std::env::var("CASITA_CATALOG_BENCH_ENTRIES")
        .expect("CASITA_CATALOG_BENCH_ENTRIES is required")
        .parse::<usize>()
        .expect("catalog benchmark entry count must be an integer");
    let repetitions = std::env::var("CASITA_CATALOG_BENCH_REPETITIONS")
        .ok()
        .map(|value| value.parse::<usize>().expect("invalid repetition count"))
        .unwrap_or(3);
    let requested_shard_bits = std::env::var("CASITA_CATALOG_BENCH_SHARD_BITS")
        .ok()
        .map(|value| value.parse::<u8>().expect("invalid shard bits"))
        .unwrap_or(0);
    let shard_bits = if requested_shard_bits == 0 {
        recommended_shard_bits(entry_count as u64, DEFAULT_SHARD_TARGET_BYTES).unwrap()
    } else {
        requested_shard_bits
    };
    let path = std::env::var("CASITA_CATALOG_BENCH_INPUT")
        .expect("CASITA_CATALOG_BENCH_INPUT is required");
    assert!(entry_count > 0);
    assert!(repetitions > 0);

    let catalog = std::fs::read(path).unwrap();
    let (_, base_checkpoint) = decode_inline_catalog(&catalog);
    let base = decode_index_checkpoint_without_inventory(&base_checkpoint).unwrap();
    assert_eq!(base.chunks.len(), entry_count);

    const UPDATE_MANIFESTS: usize = 1024;
    let mut next = base.clone();
    let pack = PackId::new(blake3::hash(b"catalog incremental publication pack").into());
    let entry = PackEntry {
        digest: ChunkId::new(blake3::hash(b"catalog incremental publication chunk").into()),
        offset: 0,
        framed_len: 256,
        uncompressed_len: 8192,
    };
    let pack_len =
        entry.framed_len + encode_footer(&[entry]).len() as u64 + PACK_TRAILER_LEN as u64;
    next.add_pack(pack, pack_len, vec![entry]);
    let manifests = (0..UPDATE_MANIFESTS).map(|ordinal| {
        let mut key = Vec::from(b"catalog incremental publication manifest ".as_slice());
        key.extend_from_slice(&(ordinal as u64).to_le_bytes());
        BlobId::new(blake3::hash(&key).into())
    });
    let mut mutations = IndexMutations::default();
    mutations.record_pack(pack);
    for manifest in manifests {
        next.manifests.insert(manifest);
        mutations.record_manifest_add(manifest);
    }

    let inventory = Digest::from(blake3::hash(b"catalog publication scale inventory"));
    let sample_delta = encode_index_mutations(&next, &mutations).unwrap();
    let run_batch_deltas = MAX_INLINE_DELTA_BYTES / (8 + sample_delta.len()) + 1;
    let run_deltas = (0..run_batch_deltas)
        .map(|batch| {
            let mut pack_key = Vec::from(b"catalog run pack ".as_slice());
            pack_key.extend_from_slice(&(batch as u64).to_le_bytes());
            let run_pack = PackId::new(blake3::hash(&pack_key).into());
            let mut chunk_key = Vec::from(b"catalog run chunk ".as_slice());
            chunk_key.extend_from_slice(&(batch as u64).to_le_bytes());
            let run_entry = PackEntry {
                digest: ChunkId::new(blake3::hash(&chunk_key).into()),
                offset: 0,
                framed_len: 256,
                uncompressed_len: 8192,
            };
            let mut patch = Index::default();
            patch.add_pack(run_pack, 512, vec![run_entry]);
            for ordinal in 0..UPDATE_MANIFESTS {
                let mut key = Vec::from(b"catalog run manifest ".as_slice());
                key.extend_from_slice(&(batch as u64).to_le_bytes());
                key.extend_from_slice(&(ordinal as u64).to_le_bytes());
                patch
                    .manifests
                    .insert(BlobId::new(blake3::hash(&key).into()));
            }
            encode_decoded_index_delta(&DecodedIndexDelta {
                removed_packs: Vec::new(),
                removed_manifests: HashSet::new(),
                patch,
            })
            .unwrap()
        })
        .collect::<Vec<_>>();
    let mut run_expected = base.clone();
    for delta in &run_deltas {
        apply_index_delta(&mut run_expected, delta).unwrap();
    }
    let started = Instant::now();
    let sharded = encode_index_shards(&next, shard_bits).unwrap();
    let shard_encode_nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let shard_map = decode_shard_map(&sharded.map).unwrap();
    let shard_objects = sharded.objects.len();
    let shard_object_bytes = sharded
        .objects
        .iter()
        .map(|(_, bytes)| bytes.len() as u64)
        .sum::<u64>();
    let shard_max_object_bytes = sharded
        .objects
        .iter()
        .map(|(_, bytes)| bytes.len() as u64)
        .max()
        .unwrap_or_default();
    let mut full_encode_nanos = Vec::with_capacity(repetitions);
    let mut delta_encode_nanos = Vec::with_capacity(repetitions);
    let mut reopen_nanos = Vec::with_capacity(repetitions);
    let mut run_seal_nanos = Vec::with_capacity(repetitions);
    let mut run_reopen_nanos = Vec::with_capacity(repetitions);
    let mut full_bytes = 0;
    let mut delta_bytes = 0;
    let mut delta_catalog_bytes = 0;
    let mut run_bytes = 0;
    let mut run_catalog_bytes = 0;
    let mut compaction_due = false;
    for generation in 1..=repetitions {
        let started = Instant::now();
        let checkpoint = encode_index_checkpoint(&next, inventory).unwrap();
        let full = encode_inline_catalog(generation as u64, &checkpoint);
        full_encode_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        full_bytes = full.len();

        let started = Instant::now();
        let delta = encode_index_mutations(&next, &mutations).unwrap();
        let delta_catalog = DeltaCatalog {
            sidecars: None,
            generation: generation as u64,
            base: CatalogBase::Checkpoint(Digest::from(blake3::hash(&base_checkpoint))),
            runs: Default::default(),
            deltas: vec![delta.clone()],
        };
        compaction_due = delta_catalog_needs_compaction(
            &DeltaCatalog {
                sidecars: None,
                deltas: Vec::new(),
                ..delta_catalog.clone()
            },
            &delta,
        );
        let pointer = encode_delta_catalog(&delta_catalog).unwrap();
        delta_encode_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        delta_bytes = delta.len();
        delta_catalog_bytes = pointer.len();

        let started = Instant::now();
        let reopened = reopen_delta_catalog(&pointer, Some(&base_checkpoint)).unwrap();
        reopen_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        assert_eq!(
            encode_index_checkpoint(&reopened, inventory).unwrap(),
            checkpoint
        );

        let started = Instant::now();
        let batch = run_deltas
            .iter()
            .enumerate()
            .map(|(ordinal, delta)| CatalogRun {
                first_generation: ordinal as u64 + 1,
                last_generation: ordinal as u64 + 1,
                delta: delta.clone(),
            })
            .collect::<Vec<_>>();
        let merged = merge_catalog_runs(&batch).unwrap();
        let encoded_run = encode_catalog_run(&merged).unwrap();
        let run_digest = Digest::from(blake3::hash(&encoded_run));
        let run_root = DeltaCatalog {
            sidecars: None,
            generation: run_batch_deltas as u64,
            base: CatalogBase::Checkpoint(Digest::from(blake3::hash(&base_checkpoint))),
            runs: BTreeMap::from([(
                0,
                CatalogRunRef {
                    digest: run_digest,
                    first_generation: merged.first_generation,
                    last_generation: merged.last_generation,
                    encoded_bytes: encoded_run.len() as u64,
                    query: Some(catalog_run_query_ref(&encoded_run).unwrap()),
                },
            )]),
            deltas: Vec::new(),
        };
        let encoded_root = encode_delta_catalog(&run_root).unwrap();
        run_seal_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        run_bytes = encoded_run.len();
        run_catalog_bytes = encoded_root.len();

        let started = Instant::now();
        let mut run_reopened = decode_index_checkpoint_without_inventory(&base_checkpoint).unwrap();
        let decoded_run = decode_catalog_run(&encoded_run).unwrap();
        apply_index_delta(&mut run_reopened, &decoded_run.delta).unwrap();
        run_reopen_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        assert_eq!(
            encode_index_checkpoint(&run_reopened, inventory).unwrap(),
            encode_index_checkpoint(&run_expected, inventory).unwrap()
        );
    }

    println!("catalog_entries {entry_count}");
    println!("catalog_update_chunks 1");
    println!("catalog_update_manifests {UPDATE_MANIFESTS}");
    println!("catalog_full_rewrite_bytes {full_bytes}");
    println!("catalog_delta_bytes {delta_bytes}");
    println!("catalog_delta_catalog_bytes {delta_catalog_bytes}");
    println!("catalog_delta_base_bytes {}", base_checkpoint.len());
    println!(
        "catalog_delta_reopen_bytes {}",
        base_checkpoint.len() + delta_catalog_bytes
    );
    println!("catalog_delta_reopen_requests 2");
    println!("catalog_delta_compaction_due {}", u8::from(compaction_due));
    println!("catalog_run_batch_deltas {run_batch_deltas}");
    println!("catalog_run_bytes {run_bytes}");
    println!("catalog_run_catalog_bytes {run_catalog_bytes}");
    println!("catalog_run_reopen_requests 3");
    println!("catalog_run_seal_median_nanos {}", median(run_seal_nanos));
    println!(
        "catalog_run_reopen_median_nanos {}",
        median(run_reopen_nanos)
    );
    println!("catalog_shard_bits {shard_bits}");
    println!("catalog_shard_map_bytes {}", sharded.map.len());
    println!("catalog_shard_objects {shard_objects}");
    println!("catalog_shard_object_bytes {shard_object_bytes}");
    println!("catalog_shard_max_object_bytes {shard_max_object_bytes}");
    println!("catalog_shard_chunk_objects {}", shard_map.chunks.len());
    println!(
        "catalog_shard_manifest_objects {}",
        shard_map.manifests.len()
    );
    println!("catalog_shard_pack_objects {}", shard_map.packs.len());
    println!("catalog_shard_encode_nanos {shard_encode_nanos}");
    println!(
        "catalog_full_encode_median_nanos {}",
        median(full_encode_nanos)
    );
    println!(
        "catalog_delta_encode_median_nanos {}",
        median(delta_encode_nanos)
    );
    println!("catalog_delta_reopen_median_nanos {}", median(reopen_nanos));
}

#[test]
#[ignore = "release-mode catalog lookup and GC scaling probe"]
fn benchmark_index_operations_scale() {
    fn median(mut values: Vec<u64>) -> u64 {
        values.sort_unstable();
        values[values.len() / 2]
    }

    fn elapsed_nanos(started: Instant) -> u64 {
        u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    fn benchmark_lookups(index: &Index, queries: &[ChunkId]) -> (u64, usize) {
        let started = Instant::now();
        let mut found = 0;
        for digest in queries {
            found += usize::from(std::hint::black_box(index.chunks.get(digest)).is_some());
        }
        (elapsed_nanos(started), found)
    }

    let entry_count = std::env::var("CASITA_CATALOG_BENCH_ENTRIES")
        .expect("CASITA_CATALOG_BENCH_ENTRIES is required")
        .parse::<usize>()
        .expect("catalog benchmark entry count must be an integer");
    let repetitions = std::env::var("CASITA_CATALOG_BENCH_REPETITIONS")
        .ok()
        .map(|value| value.parse::<usize>().expect("invalid repetition count"))
        .unwrap_or(3);
    let lookup_count = std::env::var("CASITA_CATALOG_BENCH_LOOKUPS")
        .ok()
        .map(|value| value.parse::<usize>().expect("invalid lookup count"))
        .unwrap_or(1_000_000);
    let reader_threads = std::env::var("CASITA_CATALOG_BENCH_THREADS")
        .ok()
        .map(|value| value.parse::<usize>().expect("invalid thread count"))
        .unwrap_or(8);
    let gc_percent = std::env::var("CASITA_CATALOG_BENCH_GC_PERCENT")
        .ok()
        .map(|value| value.parse::<usize>().expect("invalid GC percentage"))
        .unwrap_or(10);
    let manifest_percent = std::env::var("CASITA_CATALOG_BENCH_MANIFEST_PERCENT")
        .ok()
        .map(|value| value.parse::<usize>().expect("invalid manifest percentage"))
        .unwrap_or(0);
    let path = std::env::var("CASITA_CATALOG_BENCH_INPUT")
        .expect("CASITA_CATALOG_BENCH_INPUT is required");
    assert!(entry_count > 0);
    assert!(repetitions > 0);
    assert!(lookup_count > 0);
    assert!(reader_threads > 0);
    assert!((1..=100).contains(&gc_percent));
    assert!(manifest_percent <= 100);

    let catalog = std::fs::read(path).unwrap();
    let (_, checkpoint) = decode_inline_catalog(&catalog);
    let index = decode_index_checkpoint_without_inventory(&checkpoint).unwrap();
    assert_eq!(index.chunks.len(), entry_count);
    let expected_manifests =
        usize::try_from((entry_count as u128 * manifest_percent as u128) / 100).unwrap();
    assert_eq!(index.manifests.len(), expected_manifests);
    assert!(index.manifests_complete);

    // The multiplicative permutation makes adjacent queries jump around the
    // sorted digest array while remaining deterministic and repeatable.
    let hit_queries = (0..lookup_count)
        .map(|ordinal| {
            let source = ordinal.wrapping_mul(1_000_003) % entry_count;
            ChunkId::new(blake3::hash(&(source as u64).to_le_bytes()).into())
        })
        .collect::<Vec<_>>();
    let miss_queries = (0..lookup_count)
        .map(|ordinal| {
            let mut key = Vec::from(b"missing catalog lookup ".as_slice());
            key.extend_from_slice(&(ordinal as u64).to_le_bytes());
            ChunkId::new(blake3::hash(&key).into())
        })
        .collect::<Vec<_>>();
    let manifest_hits = index.manifests.iter().copied().collect::<Vec<_>>();
    // Bound the query corpus while still executing the configured number of
    // lookups. This keeps the manifest probe from adding tens of megabytes of
    // unrelated query allocation to the operations process.
    let manifest_misses = (0..lookup_count.min(100_000))
        .map(|ordinal| {
            let mut key = Vec::from(b"missing manifest lookup ".as_slice());
            key.extend_from_slice(&(ordinal as u64).to_le_bytes());
            BlobId::new(blake3::hash(&key).into())
        })
        .collect::<Vec<_>>();

    let mut hit_nanos = Vec::with_capacity(repetitions);
    let mut miss_nanos = Vec::with_capacity(repetitions);
    let mut list_nanos = Vec::with_capacity(repetitions);
    let mut manifest_hit_nanos = Vec::with_capacity(repetitions);
    let mut manifest_miss_nanos = Vec::with_capacity(repetitions);
    for _ in 0..repetitions {
        let (elapsed, found) = benchmark_lookups(&index, &hit_queries);
        assert_eq!(found, lookup_count);
        hit_nanos.push(elapsed);

        let (elapsed, found) = benchmark_lookups(&index, &miss_queries);
        assert_eq!(found, 0);
        miss_nanos.push(elapsed);

        let started = Instant::now();
        let ids = std::hint::black_box(index.chunks.ids());
        list_nanos.push(elapsed_nanos(started));
        assert_eq!(ids.len(), entry_count);

        let started = Instant::now();
        let mut found = 0usize;
        if !manifest_hits.is_empty() {
            for ordinal in 0..lookup_count {
                let digest = &manifest_hits[ordinal % manifest_hits.len()];
                found += usize::from(std::hint::black_box(index.manifests.contains(digest)));
            }
            assert_eq!(found, lookup_count);
        }
        manifest_hit_nanos.push(elapsed_nanos(started));

        let started = Instant::now();
        let mut found = 0usize;
        for ordinal in 0..lookup_count {
            let digest = &manifest_misses[ordinal % manifest_misses.len()];
            found += usize::from(std::hint::black_box(index.manifests.contains(digest)));
        }
        assert_eq!(found, 0);
        manifest_miss_nanos.push(elapsed_nanos(started));
    }

    let reader_threads = reader_threads.min(lookup_count);
    let shared = std::sync::RwLock::new(&index);
    let mut parallel_nanos = Vec::with_capacity(repetitions);
    for _ in 0..repetitions {
        let started = Instant::now();
        let found = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(reader_threads);
            for thread in 0..reader_threads {
                let first = lookup_count * thread / reader_threads;
                let end = lookup_count * (thread + 1) / reader_threads;
                let queries = &hit_queries[first..end];
                let shared = &shared;
                handles.push(scope.spawn(move || {
                    let mut found = 0;
                    for digest in queries {
                        let index = shared.read().unwrap();
                        found +=
                            usize::from(std::hint::black_box(index.chunks.get(digest)).is_some());
                    }
                    found
                }));
            }
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .sum::<usize>()
        });
        parallel_nanos.push(elapsed_nanos(started));
        assert_eq!(found, lookup_count);
    }

    let gc_count = (entry_count * gc_percent / 100).max(1);
    let gc_digests = (0..gc_count)
        .map(|ordinal| ChunkId::new(blake3::hash(&(ordinal as u64).to_le_bytes()).into()))
        .collect::<Vec<_>>();
    let mut gc_nanos = Vec::with_capacity(repetitions);
    for _ in 0..repetitions {
        let mut candidate = index.clone();
        let started = Instant::now();
        let removed = candidate.chunks.remove_many(&gc_digests);
        gc_nanos.push(elapsed_nanos(started));
        assert_eq!(removed.len(), gc_count);
        assert_eq!(candidate.chunks.base.len(), entry_count - gc_count);
    }

    let hit_nanos = median(hit_nanos);
    let miss_nanos = median(miss_nanos);
    let parallel_nanos = median(parallel_nanos);
    println!("index_entries {entry_count}");
    println!("index_manifest_percent {manifest_percent}");
    println!("index_manifests {expected_manifests}");
    println!("index_lookup_count {lookup_count}");
    println!("index_lookup_hit_median_nanos {hit_nanos}");
    println!(
        "index_lookup_hit_nanos_per_op {}",
        hit_nanos / lookup_count as u64
    );
    println!("index_lookup_miss_median_nanos {miss_nanos}");
    println!(
        "index_lookup_miss_nanos_per_op {}",
        miss_nanos / lookup_count as u64
    );
    println!(
        "index_manifest_lookup_hit_nanos_per_op {}",
        if manifest_hits.is_empty() {
            0
        } else {
            median(manifest_hit_nanos) / lookup_count as u64
        }
    );
    println!(
        "index_manifest_lookup_miss_nanos_per_op {}",
        median(manifest_miss_nanos) / lookup_count as u64
    );
    println!("index_list_median_nanos {}", median(list_nanos));
    println!("index_parallel_reader_threads {reader_threads}");
    println!("index_parallel_lookup_median_nanos {parallel_nanos}");
    println!(
        "index_parallel_lookup_nanos_per_op {}",
        parallel_nanos / lookup_count as u64
    );
    println!("index_gc_percent {gc_percent}");
    println!("index_gc_entries {gc_count}");
    println!("index_gc_median_nanos {}", median(gc_nanos));
}

/// Repeated protected reads across count- and byte-triggered catalog carry boundaries.
#[tokio::test]
#[ignore = "performance probe; run through benchmark run scoped-catalog"]
async fn benchmark_scoped_catalog() {
    use crate::metadata::{DataPin, DataPinLease, MemoryPinStore, PinScope, PinStore};
    let publications: usize = std::env::var("CASITA_SCOPED_PUBLICATIONS")
        .unwrap()
        .parse()
        .unwrap();
    let iterations: usize = std::env::var("CASITA_SCOPED_ITERATIONS")
        .unwrap()
        .parse()
        .unwrap();
    let case = std::env::var("CASITA_SCOPED_CASE").unwrap_or_else(|_| "manifests".into());
    assert!(matches!(
        case.as_str(),
        "manifests" | "packs-below" | "packs-above" | "packs-count-below" | "packs-count-above"
    ));
    assert!(iterations > 0);
    let growing_packs = case != "manifests";
    let chunks_per_pack = if case.starts_with("packs-count-") {
        1
    } else if growing_packs {
        64
    } else {
        0
    };
    let limit = if growing_packs { 1025 } else { publications };
    assert!(limit > 1);
    let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let writer = PackedChunks::open_with_state_catalog(
        objects,
        Path::from("scoped-catalog"),
        u64::MAX,
        0,
        &PackedChunks::empty_state_catalog().unwrap(),
    )
    .await
    .unwrap();
    let payload = b"scoped catalog sentinel";
    let meta = ChunkMeta {
        digest: ChunkId::new(blake3::hash(payload).into()),
        size: payload.len() as u64,
    };
    let compressed: Bytes = zstd::encode_all(payload.as_slice(), 0).unwrap().into();
    writer.put(meta.clone(), compressed.clone()).await.unwrap();
    let mut catalog = Vec::new();
    let mut previous = Vec::new();
    let mut before_previous = Vec::new();
    let mut actual_publications = 0;
    let mut carry_at = None;
    for ordinal in 0..limit {
        for chunk in 0..chunks_per_pack {
            let mut data = vec![0_u8; 512];
            data[..8].copy_from_slice(&(ordinal as u64).to_le_bytes());
            data[8..16].copy_from_slice(&(chunk as u64).to_le_bytes());
            writer
                .put(
                    ChunkMeta {
                        digest: ChunkId::new(blake3::hash(&data).into()),
                        size: data.len() as u64,
                    },
                    zstd::encode_all(data.as_slice(), 0).unwrap().into(),
                )
                .await
                .unwrap();
        }
        writer.register_manifest(BlobId::new(benchmark_ordinal_digest(1, ordinal as u64)));
        before_previous = previous;
        previous = catalog;
        catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(true).unwrap();
        actual_publications = ordinal + 1;
        if growing_packs && !decode_delta_catalog(&catalog).unwrap().runs.is_empty() {
            carry_at = Some(actual_publications);
            break;
        }
    }
    if growing_packs {
        assert!(
            carry_at.is_some(),
            "fixture never crossed the inline-delta limit"
        );
        assert!(decode_delta_catalog(&previous).unwrap().runs.is_empty());
        if case.ends_with("below") {
            catalog = previous;
            previous = before_previous;
            actual_publications -= 1;
        }
    }
    let newest = growing_packs.then(|| {
        let mut data = vec![0_u8; 512];
        data[..8].copy_from_slice(&((actual_publications - 1) as u64).to_le_bytes());
        (
            ChunkId::new(blake3::hash(&data).into()),
            Bytes::from(zstd::encode_all(data.as_slice(), 0).unwrap()),
        )
    });
    let root = decode_delta_catalog(&catalog).unwrap();
    let first_manifest = BlobId::new(benchmark_ordinal_digest(1, 0));
    let newest_manifest = BlobId::new(benchmark_ordinal_digest(
        1,
        (actual_publications - 1) as u64,
    ));
    let ledger = Arc::new(MemoryPinStore::default());
    let mut pins = Vec::new();
    for selected in [&catalog, &previous] {
        pins.push(
            DataPinLease::try_acquire(
                ledger.clone(),
                DataPin {
                    scope: PinScope::Snapshot {
                        generation: decode_delta_catalog(selected).unwrap().generation,
                    },
                    catalog: Some(selected.clone()),
                    resources: BTreeSet::new(),
                },
            )
            .await
            .unwrap()
            .unwrap(),
        );
    }
    let started = Instant::now();
    let snapshot = writer
        .scoped_catalog(&catalog, pins[0].clone())
        .await
        .unwrap();
    let cold_nanos = started.elapsed().as_nanos() as u64;
    assert_eq!(
        snapshot.read_bare_chunk(&meta.digest).await.unwrap(),
        Some(compressed.clone())
    );
    assert!(!snapshot.manifest_definitely_absent(&first_manifest));
    assert!(!snapshot.manifest_definitely_absent(&newest_manifest));
    if let Some((digest, expected)) = &newest {
        assert_eq!(
            snapshot.read_bare_chunk(digest).await.unwrap(),
            Some(expected.clone())
        );
    }
    drop(snapshot);
    let mut stable_nanos = 0;
    let mut alternating_nanos = 0;
    for alternating in [false, true] {
        for iteration in 0..iterations {
            let index = usize::from(alternating && iteration % 2 == 0);
            let selected = if index == 0 { &catalog } else { &previous };
            let started = Instant::now();
            let snapshot = writer
                .scoped_catalog(selected, pins[index].clone())
                .await
                .unwrap();
            let nanos = started.elapsed().as_nanos() as u64;
            if alternating {
                alternating_nanos += nanos;
            } else {
                stable_nanos += nanos;
            }
            assert_eq!(
                snapshot.read_bare_chunk(&meta.digest).await.unwrap(),
                Some(compressed.clone())
            );
            assert!(!snapshot.manifest_definitely_absent(&first_manifest));
            assert_eq!(
                snapshot.manifest_definitely_absent(&newest_manifest),
                index == 1,
                "the newest manifest must only be visible in the newer catalog"
            );
            if let Some((digest, expected)) = &newest {
                assert_eq!(
                    snapshot.read_bare_chunk(digest).await.unwrap(),
                    (index == 0).then(|| expected.clone()),
                    "new chunks must only be visible in the newer catalog"
                );
            }
        }
    }
    drop(pins);
    crate::metadata::flush_repository_leases().await.unwrap();
    assert!(ledger.inventory().await.unwrap().pins.is_empty());
    println!(
        "scoped_catalog_sample {}",
        serde_json::json!({
            "case": case, "publications": actual_publications, "iterations": iterations, "catalog_bytes": catalog.len(),
            "chunks_per_pack": chunks_per_pack, "carry_at": carry_at,
            "inline_deltas": root.deltas.len(), "runs": root.runs.len(), "cold_nanos": cold_nanos,
            "stable_nanos": stable_nanos, "alternating_nanos": alternating_nanos,
            "correctness": "exact sentinel bytes; growing chunk visibility; manifest visibility; all leases released",
        })
    );
}
