//! Deterministic catalog installation and candidate-lifetime regressions.

use super::tests::chunk;
use super::*;
use object_store::memory::InMemory;
use std::time::Duration;

async fn seed_catalog(
    objects: &Arc<dyn ObjectStore>,
    base: &Path,
    sharded: bool,
    manifest: BlobId,
) -> Vec<u8> {
    let seed = PackedChunks::open_with_state_catalog(
        objects.clone(),
        base.clone(),
        u64::MAX,
        0,
        &PackedChunks::empty_state_catalog().unwrap(),
    )
    .await
    .unwrap();
    seed.register_manifest(manifest);
    let (meta, bytes) = chunk(b"seed payload");
    seed.put(meta, bytes).await.unwrap();
    let candidate = seed.prepare_catalog().await.unwrap();
    let catalog = candidate.catalog().unwrap().to_vec();
    candidate.commit().unwrap();
    if !sharded {
        return catalog;
    }
    let index = seed.index.read().unwrap().clone();
    sharded_fixture(objects, base, &index).await
}

async fn sharded_fixture(objects: &Arc<dyn ObjectStore>, base: &Path, index: &Index) -> Vec<u8> {
    let encoded = encode_index_shards(index, 2).unwrap();
    for (digest, bytes) in encoded.objects {
        put_object(
            objects,
            &sharded_path(base, INDEXES_KIND, &digest),
            bytes,
            true,
        )
        .await
        .unwrap();
    }
    put_object(
        objects,
        &sharded_path(base, INDEXES_KIND, &encoded.map_digest),
        encoded.map,
        true,
    )
    .await
    .unwrap();
    encode_delta_catalog(&DeltaCatalog {
        sidecars: None,
        generation: 1,
        base: CatalogBase::Sharded {
            root: encoded.map_digest,
            shard_bits: 2,
        },
        runs: BTreeMap::new(),
        deltas: Vec::new(),
    })
    .unwrap()
    .to_vec()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_sync_installs_index_and_overlay_together() {
    for sharded in [false, true] {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("catalog-install-interleaving");
        let removed = BlobId::new(blake3::hash(b"removed manifest").into());
        let added = BlobId::new(blake3::hash(b"added manifest").into());
        let catalog = seed_catalog(&objects, &base, sharded, removed).await;
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &PackedChunks::empty_state_catalog().unwrap(),
        )
        .await
        .unwrap();
        let (meta, bytes) = chunk(b"pack installed beside manifest changes");
        let (start, started) = std::sync::mpsc::channel();
        let (done, finished) = std::sync::mpsc::channel();
        *writer.sync_install_hook.lock().unwrap() = Some(Box::new(move |packed| {
            // In the broken implementation the index lock is already released:
            // force the mutation to finish before the overlay is overwritten.
            // With coherent installation the writer must run after both stores.
            let installation_locked = packed.index.try_write().is_err();
            start.send(()).unwrap();
            if !installation_locked {
                finished.recv_timeout(Duration::from_secs(10)).unwrap();
            }
        }));
        let mutation = std::thread::spawn({
            let writer = writer.clone();
            let runtime = tokio::runtime::Handle::current();
            let meta = meta.clone();
            let bytes = bytes.clone();
            move || {
                started.recv_timeout(Duration::from_secs(10)).unwrap();
                writer.unregister_manifest(removed);
                writer.register_manifest(added);
                runtime.block_on(async {
                    writer.put(meta, bytes).await.unwrap();
                    writer.flush().await.unwrap();
                });
                let _ = done.send(());
            }
        });
        tokio::time::timeout(
            Duration::from_secs(20),
            writer.synchronize_state_catalog(Some(&catalog)),
        )
        .await
        .unwrap()
        .unwrap();
        mutation.join().unwrap();
        let manifests = writer
            .list_manifests()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(manifests, vec![added], "sharded={sharded}");
        assert_eq!(writer.get(&meta.digest).await.unwrap(), Some(bytes.clone()));
        let candidate = writer.prepare_catalog().await.unwrap();
        let merged = candidate.catalog().unwrap().to_vec();
        candidate.commit().unwrap();
        let reader = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &merged)
            .await
            .unwrap();
        assert_eq!(
            reader
                .list_manifests()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![added]
        );
        assert_eq!(reader.get(&meta.digest).await.unwrap(), Some(bytes));
    }
}

#[tokio::test]
async fn catalog_sync_preserves_owned_candidate_and_later_changes() {
    for sharded in [false, true] {
        for outcome in ["commit", "abort", "drop"] {
            let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let base = Path::from("catalog-candidate-lifetime");
            let removed = BlobId::new(blake3::hash(b"seed manifest").into());
            let transient = BlobId::new(blake3::hash(b"prepared then removed").into());
            let remote_manifest = BlobId::new(blake3::hash(b"remote manifest").into());
            let seed = seed_catalog(&objects, &base, sharded, removed).await;
            let local = PackedChunks::open_with_state_catalog(
                objects.clone(),
                base.clone(),
                u64::MAX,
                0,
                &seed,
            )
            .await
            .unwrap();
            let remote = PackedChunks::open_with_state_catalog(
                objects.clone(),
                base.clone(),
                u64::MAX,
                0,
                &seed,
            )
            .await
            .unwrap();
            let (first, first_bytes) = chunk(b"prepared local chunk");
            local.put(first.clone(), first_bytes.clone()).await.unwrap();
            local.unregister_manifest(removed);
            local.register_manifest(transient);
            let candidate = local.prepare_catalog().await.unwrap();
            assert!(candidate.catalog().is_some());
            let (later, later_bytes) = chunk(b"later local chunk");
            local.put(later.clone(), later_bytes.clone()).await.unwrap();
            local.flush().await.unwrap();
            local.register_manifest(removed);
            local.unregister_manifest(transient);
            let (other, other_bytes) = chunk(b"independently committed chunk");
            remote
                .put(other.clone(), other_bytes.clone())
                .await
                .unwrap();
            remote.register_manifest(remote_manifest);
            let other_candidate = remote.prepare_catalog().await.unwrap();
            let catalog = other_candidate.catalog().unwrap().to_vec();
            other_candidate.commit().unwrap();
            local
                .synchronize_state_catalog(Some(&catalog))
                .await
                .unwrap();
            assert_eq!(
                local
                    .list_manifests()
                    .try_collect::<BTreeSet<_>>()
                    .await
                    .unwrap(),
                BTreeSet::from([removed, remote_manifest])
            );
            // All three sources must be readable while the candidate is owned.
            for (meta, bytes) in [
                (&first, &first_bytes),
                (&later, &later_bytes),
                (&other, &other_bytes),
            ] {
                assert_eq!(
                    local.get(&meta.digest).await.unwrap().as_ref(),
                    Some(bytes),
                    "{outcome}, sharded={sharded}"
                );
            }
            match outcome {
                "commit" => candidate.commit().unwrap(),
                "abort" => candidate.abort().unwrap(),
                _ => drop(candidate),
            }
            let expected = BTreeSet::from([removed, remote_manifest]);
            assert_eq!(
                local
                    .list_manifests()
                    .try_collect::<BTreeSet<_>>()
                    .await
                    .unwrap(),
                expected
            );
            for (meta, bytes) in [
                (&first, &first_bytes),
                (&later, &later_bytes),
                (&other, &other_bytes),
            ] {
                assert_eq!(local.get(&meta.digest).await.unwrap().as_ref(), Some(bytes));
            }
            let retry = local.prepare_catalog().await.unwrap();
            let merged = retry.catalog().unwrap().to_vec();
            retry.commit().unwrap();
            let reader = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &merged)
                .await
                .unwrap();
            assert_eq!(
                reader
                    .list_manifests()
                    .try_collect::<BTreeSet<_>>()
                    .await
                    .unwrap(),
                expected
            );
            for (meta, bytes) in [
                (first, first_bytes),
                (later, later_bytes),
                (other, other_bytes),
            ] {
                assert_eq!(reader.get(&meta.digest).await.unwrap(), Some(bytes));
            }
            // The merged publication consumed all work; same-catalog sync and
            // absent-catalog sync must not manufacture another publication.
            local
                .synchronize_state_catalog(Some(&merged))
                .await
                .unwrap();
            local.synchronize_state_catalog(None).await.unwrap();
            assert!(local.prepare_catalog().await.unwrap().catalog().is_none());
        }
    }
}

#[tokio::test]
async fn catalog_sync_preserves_pack_removals() {
    for sharded in [false, true] {
        for prepared in [false, true] {
            let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let base = Path::from("catalog-sync-pack-removal");
            let manifest = BlobId::new(blake3::hash(b"seed manifest").into());
            let seed = seed_catalog(&objects, &base, sharded, manifest).await;
            let local = PackedChunks::open_with_state_catalog(
                objects.clone(),
                base.clone(),
                u64::MAX,
                0,
                &seed,
            )
            .await
            .unwrap();
            let remote = PackedChunks::open_with_state_catalog(
                objects.clone(),
                base.clone(),
                u64::MAX,
                0,
                &seed,
            )
            .await
            .unwrap();
            let (removed, _) = chunk(b"seed payload");
            local.delete_many(&[removed.digest]).await.unwrap();
            local.finish_deletions(true).await.unwrap();
            let candidate = if prepared {
                Some(local.prepare_catalog().await.unwrap())
            } else {
                None
            };
            let (other, bytes) = chunk(b"remote survives local pack removal");
            remote.put(other.clone(), bytes.clone()).await.unwrap();
            let publication = remote.prepare_catalog().await.unwrap();
            let catalog = publication.catalog().unwrap().to_vec();
            publication.commit().unwrap();
            local
                .synchronize_state_catalog(Some(&catalog))
                .await
                .unwrap();
            assert_eq!(local.get(&removed.digest).await.unwrap(), None);
            assert_eq!(local.get(&other.digest).await.unwrap(), Some(bytes.clone()));
            drop(candidate);
            let retry = local.prepare_catalog().await.unwrap();
            let merged = retry.catalog().unwrap().to_vec();
            retry.commit().unwrap();
            let reader = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &merged)
                .await
                .unwrap();
            assert_eq!(reader.get(&removed.digest).await.unwrap(), None);
            assert_eq!(reader.get(&other.digest).await.unwrap(), Some(bytes));
        }
    }
}

#[tokio::test]
async fn catalog_sync_invalidates_a_prepared_background_rebase() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("sync-prepared-background-rebase");
    let manifest = BlobId::new(Digest::hash(b"base manifest"));
    let seed = seed_catalog(&objects, &base, true, manifest).await;
    let writer =
        PackedChunks::open_with_state_catalog(objects.clone(), base.clone(), u64::MAX, 0, &seed)
            .await
            .unwrap();
    writer.set_catalog_rebase_run_bytes_for_test(1);
    let (first, bytes) = chunk(b"local chunk triggering rebase");
    writer.put(first.clone(), bytes.clone()).await.unwrap();
    let candidate = writer.prepare_catalog().await.unwrap();
    let before = candidate.catalog().unwrap().to_vec();
    candidate.commit().unwrap();
    let mut maintenance = writer.take_catalog_maintenance().unwrap();
    maintenance.run().await.unwrap();
    let rebase = writer.prepare_catalog().await.unwrap();
    assert!(rebase.catalog().is_some());
    let remote =
        PackedChunks::open_with_state_catalog(objects.clone(), base.clone(), u64::MAX, 0, &before)
            .await
            .unwrap();
    let (other, other_bytes) = chunk(b"remote chunk newer than ready rebase");
    remote
        .put(other.clone(), other_bytes.clone())
        .await
        .unwrap();
    let advance = remote.prepare_catalog().await.unwrap();
    let catalog = advance.catalog().unwrap().to_vec();
    advance.commit().unwrap();
    writer
        .synchronize_state_catalog(Some(&catalog))
        .await
        .unwrap();
    rebase.commit().unwrap();
    drop(maintenance);
    assert_eq!(
        writer.get(&first.digest).await.unwrap(),
        Some(bytes.clone())
    );
    assert_eq!(
        writer.get(&other.digest).await.unwrap(),
        Some(other_bytes.clone())
    );
    let merged = writer.prepare_catalog().await.unwrap();
    let catalog = merged.catalog().unwrap().to_vec();
    merged.commit().unwrap();
    let reader = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &catalog)
        .await
        .unwrap();
    assert_eq!(reader.get(&first.digest).await.unwrap(), Some(bytes));
    assert_eq!(reader.get(&other.digest).await.unwrap(), Some(other_bytes));
}

async fn assert_read_uses_one_catalog(operation: &'static str) {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("catalog-sync-read-view");
    let manifest = BlobId::new(Digest::hash(b"manifest"));
    let tombstone = Digest::hash(b"retained tombstone record");
    let (lazy, eager) = if operation == "tombstone" {
        let seed = seed_catalog(&objects, &base, false, manifest).await;
        let store = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &seed,
        )
        .await
        .unwrap();
        let mut index = store.index.read().unwrap().clone();
        let pack = *index.packs.keys().next().unwrap();
        let (meta, _) = chunk(b"seed payload");
        index.tombstoned.insert(pack, HashSet::from([meta.digest]));
        index
            .tombstone_records
            .insert(pack, HashSet::from([tombstone]));
        index.rebuild_chunks();
        let eager = encode_delta_catalog(&DeltaCatalog {
            sidecars: None,
            generation: 1,
            base: CatalogBase::Inline(
                encode_index_checkpoint(&index, Digest::hash(b"tombstone fixture")).unwrap(),
            ),
            runs: BTreeMap::new(),
            deltas: Vec::new(),
        })
        .unwrap()
        .to_vec();
        (sharded_fixture(&objects, &base, &index).await, eager)
    } else {
        (
            seed_catalog(&objects, &base, true, manifest).await,
            seed_catalog(&objects, &base, false, manifest).await,
        )
    };
    let (before, after) = if operation == "chunks" {
        (&eager, &lazy)
    } else {
        (&lazy, &eager)
    };
    let eager_reader =
        PackedChunks::open_with_state_catalog(objects.clone(), base.clone(), u64::MAX, 0, &eager)
            .await
            .unwrap();
    let (meta, bytes) = chunk(b"seed payload");
    let pack = *eager_reader
        .index
        .read()
        .unwrap()
        .packs
        .keys()
        .next()
        .unwrap();
    let writer = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, before)
        .await
        .unwrap();
    if operation != "tombstone" {
        assert_eq!(writer.get(&meta.digest).await.unwrap(), Some(bytes.clone()));
    }
    // This single-threaded test has no competing index owner: the capture
    // itself must keep the writer excluded until lazy state is selected.
    let capture_checked = Arc::new(AtomicBool::new(false));
    *writer.catalog_capture_hook.lock().unwrap() = Some(Box::new({
        let capture_checked = capture_checked.clone();
        move |packed| {
            assert!(
                packed.index.try_write().is_err(),
                "index guard was released before selecting lazy catalog state"
            );
            capture_checked.store(true, Ordering::SeqCst);
        }
    }));
    let (reached_tx, reached) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = tokio::sync::oneshot::channel();
    *writer.catalog_read_hook.lock().unwrap() = Some(FlushHandoffHook {
        reached: reached_tx,
        resume: resume_rx,
    });
    let read = tokio::spawn({
        let writer = writer.clone();
        async move {
            match operation {
                "chunk" => writer.get(&meta.digest).await.unwrap() == Some(bytes),
                "chunks" => {
                    writer.list().try_collect::<Vec<_>>().await.unwrap() == vec![meta.digest]
                }
                "manifests" => {
                    writer
                        .list_manifests()
                        .try_collect::<Vec<_>>()
                        .await
                        .unwrap()
                        == vec![manifest]
                }
                "pack_member" => writer.catalog_contains_pack(pack).await.unwrap(),
                "manifest_member" => writer.catalog_contains_manifest(manifest).await.unwrap(),
                "tombstone" => writer
                    .catalog_references_retirement(
                        &sharded_path(&writer.base, TOMBSTONES_KIND, &tombstone),
                        &mut None,
                    )
                    .await
                    .unwrap(),
                _ => unreachable!(),
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(10), reached)
        .await
        .unwrap()
        .unwrap();
    assert!(capture_checked.load(Ordering::SeqCst));
    writer.synchronize_state_catalog(Some(after)).await.unwrap();
    resume.send(()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(10), read)
            .await
            .unwrap()
            .unwrap(),
        "{operation} mixed two catalog views"
    );
}

#[tokio::test]
async fn catalog_sync_read_uses_one_catalog_view() {
    assert_read_uses_one_catalog("chunk").await;
}
#[tokio::test]
async fn catalog_sync_chunk_listing_uses_one_catalog_view() {
    assert_read_uses_one_catalog("chunks").await;
}
#[tokio::test]
async fn catalog_sync_manifest_listing_uses_one_catalog_view() {
    assert_read_uses_one_catalog("manifests").await;
}
#[tokio::test]
async fn catalog_sync_pack_membership_uses_one_catalog_view() {
    assert_read_uses_one_catalog("pack_member").await;
}
#[tokio::test]
async fn catalog_sync_manifest_membership_uses_one_catalog_view() {
    assert_read_uses_one_catalog("manifest_member").await;
}

#[tokio::test]
async fn catalog_sync_does_not_install_stale_pack_hydration() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("catalog-sync-hydration");
    let manifest = BlobId::new(Digest::hash(b"manifest"));
    let lazy = seed_catalog(&objects, &base, true, manifest).await;
    let writer = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &lazy)
        .await
        .unwrap();
    let (meta, _) = chunk(b"seed payload");
    let pack = writer.location(&meta.digest).await.unwrap().unwrap().pack;
    let (reached_tx, reached) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = tokio::sync::oneshot::channel();
    *writer.catalog_read_hook.lock().unwrap() = Some(FlushHandoffHook {
        reached: reached_tx,
        resume: resume_rx,
    });
    let hydrate = tokio::spawn({
        let writer = writer.clone();
        async move {
            writer.ensure_pack_loaded(pack).await.unwrap();
        }
    });
    tokio::time::timeout(Duration::from_secs(10), reached)
        .await
        .unwrap()
        .unwrap();
    writer
        .synchronize_state_catalog(Some(&PackedChunks::empty_state_catalog().unwrap()))
        .await
        .unwrap();
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), hydrate)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        writer.get(&meta.digest).await.unwrap(),
        None,
        "hydration reintroduced a pack from the previous catalog"
    );
}

#[tokio::test]
async fn catalog_sync_tombstone_membership_uses_one_catalog_view() {
    assert_read_uses_one_catalog("tombstone").await;
}

#[tokio::test]
async fn catalog_sync_preserves_concurrent_flush_into_sharded_catalog() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("sync-sharded-concurrent-flush");
    let incoming_manifest = BlobId::new(Digest::hash(b"incoming manifest"));
    let local_manifest = BlobId::new(Digest::hash(b"local manifest"));
    let incoming = seed_catalog(&objects, &base, true, incoming_manifest).await;
    let writer = PackedChunks::open_with_state_catalog(
        objects.clone(),
        base.clone(),
        u64::MAX,
        0,
        &PackedChunks::empty_state_catalog().unwrap(),
    )
    .await
    .unwrap();
    let (reached_tx, reached) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = tokio::sync::oneshot::channel();
    *writer.sync_dirty_hook.lock().unwrap() = Some(FlushHandoffHook {
        reached: reached_tx,
        resume: resume_rx,
    });
    let sync = tokio::spawn({
        let writer = writer.clone();
        async move {
            writer
                .synchronize_state_catalog(Some(&incoming))
                .await
                .unwrap();
        }
    });
    tokio::time::timeout(Duration::from_secs(10), reached)
        .await
        .unwrap()
        .unwrap();
    let (local, bytes) = chunk(b"local during sharded synchronization");
    writer.put(local.clone(), bytes.clone()).await.unwrap();
    writer.register_manifest(local_manifest);
    writer.flush().await.unwrap();
    assert_eq!(
        writer.get(&local.digest).await.unwrap(),
        Some(bytes.clone())
    );
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), sync)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        writer.get(&local.digest).await.unwrap(),
        Some(bytes.clone())
    );
    let candidate = writer.prepare_catalog().await.unwrap();
    let merged = candidate.catalog().unwrap().to_vec();
    candidate.commit().unwrap();
    let reader = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &merged)
        .await
        .unwrap();
    for (meta, expected) in [(local, bytes), chunk(b"seed payload")] {
        assert_eq!(reader.get(&meta.digest).await.unwrap(), Some(expected));
    }
    assert_eq!(
        reader
            .list_manifests()
            .try_collect::<BTreeSet<_>>()
            .await
            .unwrap(),
        BTreeSet::from([incoming_manifest, local_manifest])
    );
}

#[tokio::test]
async fn catalog_sync_preserves_resolution_during_decode() {
    for commit in [false, true] {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("sync-resolution-during-decode");
        let empty = PackedChunks::empty_state_catalog().unwrap();
        let local = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &empty,
        )
        .await
        .unwrap();
        let (first, bytes) = chunk(b"local candidate resolving during decode");
        local.put(first.clone(), bytes.clone()).await.unwrap();
        let candidate = local.prepare_catalog().await.unwrap();
        // A successful metadata commit may be followed by another writer's
        // publication before the local candidate's completion is delivered.
        let parent = if commit {
            candidate.catalog().unwrap()
        } else {
            &empty
        };
        let remote = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            parent,
        )
        .await
        .unwrap();
        let (other, other_bytes) = chunk(b"remote while candidate resolves");
        remote
            .put(other.clone(), other_bytes.clone())
            .await
            .unwrap();
        let advance = remote.prepare_catalog().await.unwrap();
        let incoming = advance.catalog().unwrap().to_vec();
        advance.commit().unwrap();
        let (reached_tx, reached) = tokio::sync::oneshot::channel();
        let (resume, resume_rx) = tokio::sync::oneshot::channel();
        *local.sync_dirty_hook.lock().unwrap() = Some(FlushHandoffHook {
            reached: reached_tx,
            resume: resume_rx,
        });
        let sync = tokio::spawn({
            let local = local.clone();
            let incoming = incoming.clone();
            async move {
                local
                    .synchronize_state_catalog(Some(&incoming))
                    .await
                    .unwrap();
            }
        });
        tokio::time::timeout(Duration::from_secs(10), reached)
            .await
            .unwrap()
            .unwrap();
        if commit {
            candidate.commit().unwrap();
        } else {
            candidate.abort().unwrap();
        }
        resume.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), sync)
            .await
            .unwrap()
            .unwrap();
        for (meta, expected) in [(&first, &bytes), (&other, &other_bytes)] {
            assert_eq!(
                local.get(&meta.digest).await.unwrap().as_ref(),
                Some(expected)
            );
        }
        let retry = local.prepare_catalog().await.unwrap();
        let merged = retry.catalog().map(ToOwned::to_owned).unwrap_or(incoming);
        retry.commit().unwrap();
        let reader = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &merged)
            .await
            .unwrap();
        for (meta, expected) in [(first, bytes), (other, other_bytes)] {
            assert_eq!(reader.get(&meta.digest).await.unwrap(), Some(expected));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_sync_readers_materialize_newly_selected_runs() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("catalog-sync-deferred-runs");
    let manifest = BlobId::new(Digest::hash(b"run-only manifest"));
    let eager = seed_catalog(&objects, &base, false, manifest).await;
    let fixture =
        PackedChunks::open_with_state_catalog(objects.clone(), base.clone(), u64::MAX, 0, &eager)
            .await
            .unwrap();
    let (meta, bytes) = chunk(b"seed payload");
    let index = fixture.index.read().unwrap().clone();
    let pack = *index.packs.keys().next().unwrap();
    let empty = Index {
        manifests_complete: true,
        ..Index::default()
    };
    let sharded = sharded_fixture(&objects, &base, &empty).await;
    let mut root = decode_delta_catalog(&sharded).unwrap();
    let mut changes = IndexMutations::default();
    changes.record_pack(pack);
    changes.record_manifest_add(manifest);
    let run = CatalogRun {
        first_generation: 2,
        last_generation: 2,
        delta: encode_index_mutations(&index, &changes).unwrap(),
    };
    let encoded = encode_catalog_run(&run).unwrap();
    let digest = Digest::from(blake3::hash(&encoded));
    put_object(
        &objects,
        &sharded_path(&base, INDEXES_KIND, &digest),
        encoded.clone(),
        true,
    )
    .await
    .unwrap();
    root.generation = 2;
    root.runs.insert(
        0,
        CatalogRunRef {
            digest,
            first_generation: 2,
            last_generation: 2,
            encoded_bytes: encoded.len() as u64,
            query: None,
        },
    );
    let deferred = encode_delta_catalog(&root).unwrap();
    for operation in ["chunks", "manifests", "pack", "manifest", "hydrate"] {
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &eager,
        )
        .await
        .unwrap();
        let (reached_tx, reached) = tokio::sync::oneshot::channel();
        let (resume, resume_rx) = tokio::sync::oneshot::channel();
        *writer.catalog_materialize_hook.lock().unwrap() = Some(FlushHandoffHook {
            reached: reached_tx,
            resume: resume_rx,
        });
        let read = tokio::spawn({
            let writer = writer.clone();
            let meta = meta.clone();
            async move {
                match operation {
                    "chunks" => assert_eq!(
                        writer.list().try_collect::<Vec<_>>().await.unwrap(),
                        vec![meta.digest]
                    ),
                    "manifests" => assert_eq!(
                        writer
                            .list_manifests()
                            .try_collect::<Vec<_>>()
                            .await
                            .unwrap(),
                        vec![manifest]
                    ),
                    "pack" => assert!(writer.catalog_contains_pack(pack).await.unwrap()),
                    "manifest" => {
                        assert!(writer.catalog_contains_manifest(manifest).await.unwrap())
                    }
                    "hydrate" => {
                        writer.ensure_pack_loaded(pack).await.unwrap();
                        assert!(writer.index.read().unwrap().packs.contains_key(&pack));
                    }
                    _ => unreachable!(),
                }
            }
        });
        tokio::time::timeout(Duration::from_secs(10), reached)
            .await
            .unwrap()
            .unwrap();
        writer
            .synchronize_state_catalog(Some(&deferred))
            .await
            .unwrap();
        resume.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), read)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(writer.get(&meta.digest).await.unwrap(), Some(bytes.clone()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_sync_retries_run_hydration_from_a_replaced_base() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("catalog-sync-run-base");
    let from_run = BlobId::new(Digest::hash(b"same run across bases"));
    let from_base = BlobId::new(Digest::hash(b"new base manifest"));
    let mut index = Index {
        manifests_complete: true,
        ..Index::default()
    };
    let old = sharded_fixture(&objects, &base, &index).await;
    index.manifests.insert(from_base);
    let new = sharded_fixture(&objects, &base, &index).await;
    index.manifests.insert(from_run);
    let mut changes = IndexMutations::default();
    changes.record_manifest_add(from_run);
    let run = CatalogRun {
        first_generation: 2,
        last_generation: 2,
        delta: encode_index_mutations(&index, &changes).unwrap(),
    };
    let bytes = encode_catalog_run(&run).unwrap();
    let digest = Digest::from(blake3::hash(&bytes));
    put_object(
        &objects,
        &sharded_path(&base, INDEXES_KIND, &digest),
        bytes.clone(),
        true,
    )
    .await
    .unwrap();
    let catalogs = [old, new].map(|catalog| {
        let mut root = decode_delta_catalog(&catalog).unwrap();
        root.generation = 2;
        root.runs.insert(
            0,
            CatalogRunRef {
                digest,
                first_generation: 2,
                last_generation: 2,
                encoded_bytes: bytes.len() as u64,
                query: None,
            },
        );
        encode_delta_catalog(&root).unwrap()
    });
    let writer = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &catalogs[0])
        .await
        .unwrap();
    let (reached_tx, reached) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = tokio::sync::oneshot::channel();
    *writer.catalog_read_hook.lock().unwrap() = Some(FlushHandoffHook {
        reached: reached_tx,
        resume: resume_rx,
    });
    let load = tokio::spawn({
        let writer = writer.clone();
        async move { writer.list_manifests().try_collect::<BTreeSet<_>>().await }
    });
    tokio::time::timeout(Duration::from_secs(10), reached)
        .await
        .unwrap()
        .unwrap();
    writer
        .synchronize_state_catalog(Some(&catalogs[1]))
        .await
        .unwrap();
    resume.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(10), load)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        result.expect("a replaced catalog must be retried"),
        BTreeSet::from([from_base, from_run])
    );
    assert_eq!(
        writer
            .list_manifests()
            .try_collect::<BTreeSet<_>>()
            .await
            .unwrap(),
        BTreeSet::from([from_base, from_run])
    );
}

#[tokio::test]
async fn catalog_sync_materialization_preserves_in_progress_preparation() {
    for outcome in ["commit", "abort", "cancel"] {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("in-progress-preparation");
        let empty = Index {
            manifests_complete: true,
            ..Index::default()
        };
        let sharded = sharded_fixture(&objects, &base, &empty).await;
        let mut root = decode_delta_catalog(&sharded).unwrap();
        let run = CatalogRun {
            first_generation: 2,
            last_generation: 2,
            delta: encode_index_mutations(&empty, &IndexMutations::default()).unwrap(),
        };
        let encoded = encode_catalog_run(&run).unwrap();
        let digest = Digest::from(blake3::hash(&encoded));
        put_object(
            &objects,
            &sharded_path(&base, INDEXES_KIND, &digest),
            encoded.clone(),
            true,
        )
        .await
        .unwrap();
        root.generation = 2;
        root.runs.insert(
            0,
            CatalogRunRef {
                digest,
                first_generation: 2,
                last_generation: 2,
                encoded_bytes: encoded.len() as u64,
                query: Some(catalog_run_query_ref(&encoded).unwrap()),
            },
        );
        let catalog = encode_delta_catalog(&root).unwrap();
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &catalog,
        )
        .await
        .unwrap();
        let (chunk, bytes) = chunk(b"preparing payload must remain visible");
        writer.put(chunk.clone(), bytes.clone()).await.unwrap();
        let count = delta::MAX_INLINE_DELTA_BYTES / DIGEST_LEN + 1;
        let mut expected = BTreeSet::new();
        for ordinal in 0..count {
            let id = BlobId::new(Digest::hash(&ordinal.to_le_bytes()));
            writer.register_manifest(id);
            expected.insert(id);
        }
        let (reached_tx, reached) = tokio::sync::oneshot::channel();
        let (resume, resume_rx) = tokio::sync::oneshot::channel();
        *writer.catalog_prepare_hook.lock().unwrap() = Some(FlushHandoffHook {
            reached: reached_tx,
            resume: resume_rx,
        });
        let mut prepare = Box::pin(writer.prepare_catalog());
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                _ = reached => {},
                _ = &mut prepare => panic!("preparation did not pause"),
            }
        })
        .await
        .unwrap();
        assert!(!writer.lazy_catalog.read().unwrap().run_refs.is_empty());
        let later = BlobId::new(Digest::hash(b"later local manifest"));
        writer.register_manifest(later);
        expected.insert(later);
        let visible = writer
            .list_manifests()
            .try_collect::<BTreeSet<_>>()
            .await
            .unwrap();
        assert!(
            visible == expected,
            "{outcome}: missing {} manifests, unexpected {}",
            expected.difference(&visible).count(),
            visible.difference(&expected).count()
        );
        assert_eq!(
            writer.get(&chunk.digest).await.unwrap(),
            Some(bytes.clone())
        );
        // Cancellation must drop the underlying future, not only its Pin reference.
        if outcome != "cancel" {
            resume.send(()).unwrap();
            let candidate = tokio::time::timeout(Duration::from_secs(10), &mut prepare)
                .await
                .unwrap()
                .unwrap();
            if outcome == "commit" {
                candidate.commit().unwrap();
            } else {
                candidate.abort().unwrap();
            }
        }
        drop(prepare);
        assert_eq!(
            writer.get(&chunk.digest).await.unwrap(),
            Some(bytes.clone())
        );
        let retry = writer.prepare_catalog().await.unwrap();
        let merged = retry.catalog().unwrap().to_vec();
        retry.commit().unwrap();
        let reader = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &merged)
            .await
            .unwrap();
        assert_eq!(reader.get(&chunk.digest).await.unwrap(), Some(bytes));
        assert_eq!(
            reader
                .list_manifests()
                .try_collect::<BTreeSet<_>>()
                .await
                .unwrap(),
            expected
        );
    }
}

async fn deferred_manifest_catalog(
    objects: &Arc<dyn ObjectStore>,
    base: &Path,
    manifest: BlobId,
) -> Vec<u8> {
    let mut index = Index {
        manifests_complete: true,
        ..Index::default()
    };
    let catalog = sharded_fixture(objects, base, &index).await;
    index.manifests.insert(manifest);
    let mut mutations = IndexMutations::default();
    mutations.record_manifest_add(manifest);
    let bytes = encode_catalog_run(&CatalogRun {
        first_generation: 2,
        last_generation: 2,
        delta: encode_index_mutations(&index, &mutations).unwrap(),
    })
    .unwrap();
    let digest = Digest::from(blake3::hash(&bytes));
    put_object(
        objects,
        &sharded_path(base, INDEXES_KIND, &digest),
        bytes.clone(),
        true,
    )
    .await
    .unwrap();
    let mut root = decode_delta_catalog(&catalog).unwrap();
    root.generation = 2;
    root.runs.insert(
        0,
        CatalogRunRef {
            digest,
            first_generation: 2,
            last_generation: 2,
            encoded_bytes: bytes.len() as u64,
            query: None,
        },
    );
    encode_delta_catalog(&root).unwrap().to_vec()
}

#[tokio::test]
async fn catalog_sync_materialization_retries_an_empty_replacement() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("materialization-empty-replacement");
    let removed = BlobId::new(Digest::hash(b"old deferred manifest"));
    let local = BlobId::new(Digest::hash(b"pending local manifest"));
    let catalog = deferred_manifest_catalog(&objects, &base, removed).await;
    let writer = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &catalog)
        .await
        .unwrap();
    writer.register_manifest(local);
    let (reached_tx, reached) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = tokio::sync::oneshot::channel();
    *writer.catalog_read_hook.lock().unwrap() = Some(FlushHandoffHook {
        reached: reached_tx,
        resume: resume_rx,
    });
    let read = tokio::spawn({
        let writer = writer.clone();
        async move { writer.list_manifests().try_collect::<BTreeSet<_>>().await }
    });
    tokio::time::timeout(Duration::from_secs(10), reached)
        .await
        .unwrap()
        .unwrap();
    writer
        .synchronize_state_catalog(Some(&PackedChunks::empty_state_catalog().unwrap()))
        .await
        .unwrap();
    resume.send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), read)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        BTreeSet::from([local])
    );
}

#[tokio::test]
async fn catalog_sync_cancelled_materialization_retry_preserves_pending() {
    for corrupt in [None, Some(false), Some(true)] {
        cancelled_materialization_retry_preserves_pending(corrupt).await;
    }
}

async fn cancelled_materialization_retry_preserves_pending(corrupt: Option<bool>) {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("materialization-retry-cancellation");
    let old = BlobId::new(Digest::hash(b"old deferred manifest"));
    let new = BlobId::new(Digest::hash(b"new deferred manifest"));
    let local = BlobId::new(Digest::hash(b"pending local manifest"));
    let before = deferred_manifest_catalog(&objects, &base, old).await;
    let after = deferred_manifest_catalog(&objects, &base, new).await;
    let writer =
        PackedChunks::open_with_state_catalog(objects.clone(), base.clone(), u64::MAX, 0, &before)
            .await
            .unwrap();
    writer.register_manifest(local);
    if let Some(corrupt) = corrupt {
        remove_or_corrupt_run(&objects, &base, &before, corrupt).await;
    }
    let (reached_tx, reached) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = tokio::sync::oneshot::channel();
    *writer.catalog_read_hook.lock().unwrap() = Some(FlushHandoffHook {
        reached: reached_tx,
        resume: resume_rx,
    });
    let read = tokio::spawn({
        let writer = writer.clone();
        async move { writer.list_manifests().try_collect::<BTreeSet<_>>().await }
    });
    tokio::time::timeout(Duration::from_secs(10), reached)
        .await
        .unwrap()
        .unwrap();
    writer
        .synchronize_state_catalog(Some(&after))
        .await
        .unwrap();
    let (retry_tx, retry_reached) = tokio::sync::oneshot::channel();
    let (_retry_resume, retry_rx) = tokio::sync::oneshot::channel();
    *writer.catalog_read_hook.lock().unwrap() = Some(FlushHandoffHook {
        reached: retry_tx,
        resume: retry_rx,
    });
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), retry_reached)
        .await
        .unwrap()
        .unwrap();
    read.abort();
    assert!(read.await.unwrap_err().is_cancelled());
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(10),
            writer.list_manifests().try_collect::<BTreeSet<_>>()
        )
        .await
        .unwrap()
        .unwrap(),
        BTreeSet::from([new, local])
    );
    let candidate = writer.prepare_catalog().await.unwrap();
    let merged = candidate.catalog().unwrap().to_vec();
    candidate.commit().unwrap();
    let reader = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &merged)
        .await
        .unwrap();
    assert_eq!(
        reader
            .list_manifests()
            .try_collect::<BTreeSet<_>>()
            .await
            .unwrap(),
        BTreeSet::from([new, local])
    );
}

#[tokio::test]
async fn catalog_sync_materialization_propagates_run_failures() {
    for corrupt in [false, true] {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("materialization-run-failure");
        let manifest = BlobId::new(Digest::hash(b"unreadable deferred manifest"));
        let catalog = deferred_manifest_catalog(&objects, &base, manifest).await;
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &catalog,
        )
        .await
        .unwrap();
        remove_or_corrupt_run(&objects, &base, &catalog, corrupt).await;
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            writer.list_manifests().try_collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        assert!(
            result.is_err(),
            "missing or corrupt runs must not be hidden by retry"
        );
    }
}

async fn remove_or_corrupt_run(
    objects: &Arc<dyn ObjectStore>,
    base: &Path,
    catalog: &[u8],
    corrupt: bool,
) {
    let root = decode_delta_catalog(catalog).unwrap();
    let run = root.runs.values().next().unwrap();
    let path = sharded_path(base, INDEXES_KIND, &run.digest);
    objects.delete(&path).await.unwrap();
    if corrupt {
        put_object(
            objects,
            &path,
            Bytes::from_static(b"corrupt catalog run"),
            true,
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn catalog_sync_materialization_retries_obsolete_run_failures() {
    for corrupt in [false, true] {
        for empty in [false, true] {
            let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let base = Path::from("obsolete-run-failure");
            let old = BlobId::new(Digest::hash(b"old deferred manifest"));
            let new = BlobId::new(Digest::hash(b"new deferred manifest"));
            let local = BlobId::new(Digest::hash(b"pending local manifest"));
            let before = deferred_manifest_catalog(&objects, &base, old).await;
            let after = if empty {
                PackedChunks::empty_state_catalog().unwrap()
            } else {
                deferred_manifest_catalog(&objects, &base, new).await
            };
            let writer = PackedChunks::open_with_state_catalog(
                objects.clone(),
                base.clone(),
                u64::MAX,
                0,
                &before,
            )
            .await
            .unwrap();
            writer.register_manifest(local);
            remove_or_corrupt_run(&objects, &base, &before, corrupt).await;
            let (reached_tx, reached) = tokio::sync::oneshot::channel();
            let (resume, resume_rx) = tokio::sync::oneshot::channel();
            *writer.catalog_read_hook.lock().unwrap() = Some(FlushHandoffHook {
                reached: reached_tx,
                resume: resume_rx,
            });
            let read = tokio::spawn({
                let writer = writer.clone();
                async move { writer.list_manifests().try_collect::<BTreeSet<_>>().await }
            });
            // Hold the failed load at its completion boundary, then replace the
            // selected catalog before the reader handles that obsolete result.
            tokio::time::timeout(Duration::from_secs(10), reached)
                .await
                .unwrap()
                .unwrap();
            writer
                .synchronize_state_catalog(Some(&after))
                .await
                .unwrap();
            resume.send(()).unwrap();
            let expected = if empty {
                BTreeSet::from([local])
            } else {
                BTreeSet::from([new, local])
            };
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(10), read)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                expected,
                "corrupt={corrupt}, empty={empty}"
            );
            let candidate = writer.prepare_catalog().await.unwrap();
            let merged = candidate.catalog().unwrap().to_vec();
            candidate.commit().unwrap();
            let reader = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &merged)
                .await
                .unwrap();
            assert_eq!(
                reader
                    .list_manifests()
                    .try_collect::<BTreeSet<_>>()
                    .await
                    .unwrap(),
                expected
            );
        }
    }
}

#[tokio::test]
async fn catalog_sync_materialization_reports_replacement_run_failure() {
    for corrupt in [false, true] {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("replacement-run-failure");
        let old = BlobId::new(Digest::hash(b"old deferred manifest"));
        let new = BlobId::new(Digest::hash(b"new deferred manifest"));
        let local = BlobId::new(Digest::hash(b"pending local manifest"));
        let before = deferred_manifest_catalog(&objects, &base, old).await;
        let after = deferred_manifest_catalog(&objects, &base, new).await;
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &before,
        )
        .await
        .unwrap();
        writer.register_manifest(local);
        // Different failure kinds distinguish the replacement's error from the
        // obsolete result: the reader must return the error for its current run.
        remove_or_corrupt_run(&objects, &base, &before, !corrupt).await;
        let (reached_tx, reached) = tokio::sync::oneshot::channel();
        let (resume, resume_rx) = tokio::sync::oneshot::channel();
        *writer.catalog_read_hook.lock().unwrap() = Some(FlushHandoffHook {
            reached: reached_tx,
            resume: resume_rx,
        });
        let read = tokio::spawn({
            let writer = writer.clone();
            async move { writer.list_manifests().try_collect::<BTreeSet<_>>().await }
        });
        tokio::time::timeout(Duration::from_secs(10), reached)
            .await
            .unwrap()
            .unwrap();
        writer
            .synchronize_state_catalog(Some(&after))
            .await
            .unwrap();
        remove_or_corrupt_run(&objects, &base, &after, corrupt).await;
        resume.send(()).unwrap();
        let error = tokio::time::timeout(Duration::from_secs(10), read)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error.kind(),
            if corrupt {
                io::ErrorKind::Other
            } else {
                io::ErrorKind::NotFound
            }
        );

        // Repairing the current run must allow a fresh attempt, with local work
        // intact and no load/transition lock retained by the failed attempt.
        deferred_manifest_catalog(&objects, &base, new).await;
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(10),
                writer.list_manifests().try_collect::<BTreeSet<_>>()
            )
            .await
            .unwrap()
            .unwrap(),
            BTreeSet::from([new, local])
        );
    }
}
