//! End-to-end local GC while historical selected readers remain live.
use super::*;
use crate::metadata::{MetadataStore, RootChange, flush_repository_leases};
use crate::sync::{TransferSelection, TransferSource};
use tokio::io::AsyncReadExt;

use crate::benchmark_timing as gc_timing;

fn payload(seed: usize) -> Vec<u8> {
    (0..128)
        .flat_map(|block| *blake3::hash(format!("gc-holds/{seed}/{block}").as_bytes()).as_bytes())
        .collect()
}

fn disk_bytes(path: &std::path::Path) -> u64 {
    if !path.exists() {
        return 0;
    }
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            if path.is_dir() {
                disk_bytes(&path)
            } else {
                std::fs::metadata(path).unwrap().len()
            }
        })
        .sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "performance corpus; benchmark run held-catalog-gc"]
async fn benchmark_held_catalog_gc() {
    let count: usize = std::env::var("CASITA_BENCH_GC_ENTRIES")
        .unwrap()
        .parse()
        .unwrap();
    let holds: usize = std::env::var("CASITA_BENCH_GC_HOLDS")
        .unwrap()
        .parse()
        .unwrap();
    assert!(count >= 2 && holds > 0);
    let timings = gc_timing::Timings::install();
    let directory = tempfile::tempdir().unwrap();
    let source = crate::repository::Repository::local(directory.path())
        .await
        .unwrap();
    // Exercise a realistic shared base without millions of setup publications.
    // This only changes when setup compacts; measured GC uses normal settings.
    source
        .payloads()
        .set_pack_catalog_rebase_run_bytes_for_test(1);
    let mutation = source.mutation_session().await.unwrap();
    let mut staged = Vec::new();
    for seed in 0..count {
        staged.push(mutation.stage_blob(&payload(seed)).await.unwrap());
    }
    let key = staged[0].record().key().clone();
    let name: crate::RootName = "selected".parse().unwrap();
    mutation
        .publish_rooted(staged, name.clone(), key.clone())
        .await
        .unwrap();
    drop(mutation);
    flush_repository_leases().await.unwrap();
    let snapshot = source.metadata().snapshot().await.unwrap();
    let catalog = source
        .payloads()
        .benchmark_packed()
        .resolve_state_catalog(snapshot.payload_catalog().unwrap())
        .await
        .unwrap();
    let root = decode_delta_catalog(&catalog).unwrap();
    assert!(
        matches!(root.base, CatalogBase::Sharded { .. }),
        "setup must produce a sharded base"
    );
    drop(snapshot);
    drop(source);
    flush_repository_leases().await.unwrap();
    // Fresh normal configuration, followed by ordinary reads and publications.
    let source = crate::repository::Repository::local(directory.path())
        .await
        .unwrap();
    let mut sessions = Vec::new();
    let mut readers = Vec::new();
    for hold in 0..holds {
        if hold > 0 {
            let mutation = source.mutation_session().await.unwrap();
            let extra = mutation.stage_blob(&payload(count + hold)).await.unwrap();
            mutation.publish_unrooted(vec![extra]).await.unwrap();
            drop(mutation);
            flush_repository_leases().await.unwrap();
        }
        let session = source
            .begin_transfer(TransferSelection::Selected {
                objects: vec![key.clone()],
                roots: Vec::new(),
            })
            .await
            .unwrap();
        let record = session.object(&key).await.unwrap().unwrap();
        let mut reader = session.open_payload(&record).await.unwrap().unwrap();
        let mut first = [0];
        reader.read_exact(&mut first).await.unwrap();
        assert_eq!(first[0], payload(0)[0]);
        readers.push(reader);
        sessions.push(session);
    }
    let mutation = source.mutation_session().await.unwrap();
    mutation
        .publish(Vec::new(), vec![RootChange::Remove { name }])
        .await
        .unwrap();
    drop(mutation);
    flush_repository_leases().await.unwrap();
    let ledger = source.metadata().pin_store().await.unwrap();
    let inventory = ledger.inventory().await.unwrap();
    let catalogs: BTreeSet<_> = inventory
        .pins
        .values()
        .filter_map(|pin| pin.catalog.as_deref())
        .collect();
    assert_eq!(
        catalogs.len(),
        holds,
        "each selected session must hold a distinct root"
    );
    let mut bases = BTreeSet::new();
    for catalog in &catalogs {
        let resolved = source
            .payloads()
            .benchmark_packed()
            .resolve_state_catalog(catalog)
            .await
            .unwrap();
        match decode_delta_catalog(&resolved).unwrap().base {
            CatalogBase::Sharded { root, .. } => {
                bases.insert(root);
            }
            _ => panic!("held root must retain its shared sharded base"),
        }
    }
    assert_eq!(bases.len(), 1);
    drop(inventory);
    let pack_root = directory.path().join("blobs/packs");
    let mut directories = vec![pack_root.clone()];
    let mut historical_packs = Vec::new();
    while let Some(path) = directories.pop() {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                directories.push(path);
            } else {
                historical_packs.push((path.clone(), std::fs::metadata(path).unwrap().len()));
            }
        }
    }
    let pack_bytes_before = disk_bytes(&directory.path().join("blobs/packs"));
    assert!(pack_bytes_before > 0);
    timings.reset();
    let start = Instant::now();
    let collected = source.try_collect().await.unwrap();
    let seconds = start.elapsed().as_secs_f64();
    let phases = timings.take(start);
    let ledger_timing = timings.take_ledger();
    let garbage = count + holds - 2;
    assert_eq!(collected.removed.logical_objects, garbage);
    let pack_bytes_during = disk_bytes(&directory.path().join("blobs/packs"));
    // GC can write a compacted current pack while old catalogs retain the
    // originals. Check every historical file, allowing those new packs.
    for (path, bytes) in &historical_packs {
        assert_eq!(std::fs::metadata(path).unwrap().len(), *bytes);
    }
    assert!(pack_bytes_during >= pack_bytes_before);
    for (session, mut reader) in sessions.iter().zip(readers) {
        let mut received = vec![payload(0)[0]];
        reader.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, payload(0));
        // Open again through each held catalog after collection, too.
        let record = session.object(&key).await.unwrap().unwrap();
        let mut fresh = session.open_payload(&record).await.unwrap().unwrap();
        let mut received = Vec::new();
        fresh.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, payload(0));
    }
    drop(sessions);
    flush_repository_leases().await.unwrap();
    timings.reset();
    let start = Instant::now();
    let released = source.try_collect().await.unwrap();
    let release_seconds = start.elapsed().as_secs_f64();
    let release_phases = timings.take(start);
    assert_eq!(released.removed.logical_objects, 1);
    let pack_bytes_after_release = disk_bytes(&directory.path().join("blobs/packs"));
    assert_eq!(pack_bytes_after_release, 0);
    println!(
        "held_catalog_gc_sample {}",
        serde_json::json!({
            "count": count, "holds": holds, "seconds": seconds, "phases": phases,
            "ledger": ledger_timing, "release_seconds": release_seconds,
            "release_phases": release_phases, "garbage_objects": garbage,
            "pack_bytes_before": pack_bytes_before, "pack_bytes_during": pack_bytes_during,
            "pack_bytes_after_release": pack_bytes_after_release,
        "historical_packs_preserved": true, "historical_pack_files": historical_packs.len(),
            "correctness": "distinct shared-base holds; exact logical removals; held streams readable; packs reclaimed after release",
        })
    );
}
