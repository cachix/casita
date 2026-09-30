use super::*;
use std::sync::atomic::Ordering;
mod extensions;

fn staging(label: &str) -> DataPin {
    DataPin {
        scope: PinScope::Staging,
        catalog: None,
        resources: BTreeSet::from([PinResource::StorageObject(label.into())]),
    }
}
fn paths(label: &str) -> BTreeSet<PinResource> {
    staging(label).resources
}
fn evict(store: &FilePinStore) {
    *store.local().unwrap().cache.lock().unwrap() = Cache::default();
}
async fn fresh(store: &FilePinStore) -> PinInventory {
    evict(store);
    store.inventory().await.unwrap()
}

#[tokio::test]
async fn appends_replay_exactly_and_reject_complete_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let store = FilePinStore::new(dir.path().join("pins"));
    let initial = store.register(staging("initial")).await.unwrap().unwrap();
    let cursor = store
        .local()
        .unwrap()
        .cache
        .lock()
        .unwrap()
        .snapshot
        .as_ref()
        .unwrap()
        .cursor;
    let next = store.register(staging("next")).await.unwrap().unwrap();
    let expected = store.inventory().await.unwrap();
    assert_eq!(fresh(&store).await, expected);
    assert!(
        store
            .local()
            .unwrap()
            .stats
            .adoptions
            .load(Ordering::Relaxed)
            > 0
    );
    let mut file = std::fs::File::options()
        .write(true)
        .open(&store.path)
        .unwrap();
    file.seek(SeekFrom::Start((cursor + HEADER) as u64))
        .unwrap();
    file.write_all(b"corrupt").unwrap();
    file.sync_all().unwrap();
    evict(&store);
    assert!(store.inventory().await.is_err());
    assert!(store.register(staging("must fail closed")).await.is_err());
    assert_ne!(initial, next);
}

#[tokio::test]
async fn ordered_group_has_one_barrier_and_checked_operations_keep_one_winner() {
    let dir = tempfile::tempdir().unwrap();
    let store = FilePinStore::new(dir.path().join("pins"));
    let first = store.register(staging("seed")).await.unwrap().unwrap();
    let before = store.inventory().await.unwrap();
    let frames = store.local().unwrap().stats.frames.load(Ordering::Relaxed);
    let outcomes = store
        .edit_group(&[
            Operation::BeginCollection(before.revision, None),
            Operation::BeginCollection(before.revision, None),
            Operation::Protect(first.clone(), paths("pack")),
            Operation::Release(first.clone()),
        ])
        .unwrap();
    assert!(matches!(&outcomes[0], Ok(Outcome::Token(Some(_)))));
    assert!(matches!(&outcomes[1], Ok(Outcome::Token(None))));
    assert_eq!(
        store.local().unwrap().stats.frames.load(Ordering::Relaxed) - frames,
        1
    );
    let after = fresh(&store).await;
    assert!(after.retired.contains(&first));
    assert!(
        after.pins[&first]
            .resources
            .contains(&PinResource::StorageObject("pack".into()))
    );
    store
        .finish_collection(after.collector.as_ref().unwrap())
        .await
        .unwrap();
    assert!(fresh(&store).await.pins.is_empty());
}

#[tokio::test]
async fn full_inventory_gc_transitions_share_the_journal_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    let store = FilePinStore::new(dir.path().join("pins"));
    let seed = store.register(staging("seed")).await.unwrap().unwrap();
    let collector = store.acquire_collection(None).await.unwrap().unwrap();
    assert_eq!(fresh(&store).await.collector, Some(collector.clone()));
    let protected = BTreeSet::from([PinResource::Blob(BlobId::new(crate::Digest::hash(
        b"protected",
    )))]);
    let garbage = BTreeSet::from([PinResource::Blob(BlobId::new(crate::Digest::hash(
        b"garbage",
    )))]);
    let stats = store.local().unwrap().stats.clone();
    let syncs = stats.syncs.load(Ordering::Relaxed);
    let outcomes = store
        .edit_group(&[
            Operation::Protect(seed.clone(), protected.clone()),
            Operation::ClaimValidated(collector.clone(), protected.clone(), BTreeSet::new()),
            Operation::ClaimValidated(collector.clone(), garbage.clone(), BTreeSet::new()),
            Operation::Release(seed.clone()),
        ])
        .unwrap();
    assert!(matches!(outcomes[1], Ok(Outcome::Token(None))));
    let Ok(Outcome::Token(Some(claim))) = &outcomes[2] else {
        panic!("claim rejected")
    };
    assert_eq!(stats.syncs.load(Ordering::Relaxed) - syncs, 1);
    let claimed = fresh(&store).await;
    assert_eq!(claimed.deletions[claim], garbage.clone());
    assert!(claimed.retired.contains(&seed));
    let (prune, snapshot) = store
        .begin_prune_validating(&collector, BTreeSet::from([claim.clone()]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot, claimed);
    assert_eq!(fresh(&store).await.logical_prune, Some(prune.clone()));
    let syncs = stats.syncs.load(Ordering::Relaxed);
    store
        .edit_group(&[
            Operation::FinishDeletion(claim.clone()),
            Operation::FinishPrune(prune),
            Operation::FinishCollection(collector),
        ])
        .unwrap();
    assert_eq!(stats.syncs.load(Ordering::Relaxed) - syncs, 1);
    let after = fresh(&store).await;
    assert!(after.pins.is_empty() && after.deletions.is_empty() && after.retired.is_empty());
    assert!(after.collector.is_none() && after.logical_prune.is_none());
}

#[tokio::test]
async fn unsupported_exchange_preserves_legacy_and_active_journal() {
    for journal in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut legacy = FilePinStore::new(dir.path().join("pins"));
        legacy.replacement = !journal;
        let seed = legacy
            .register(staging("acknowledged"))
            .await
            .unwrap()
            .unwrap();
        let store = FilePinStore::new(&legacy.path);
        let before = store.inventory().await.unwrap();
        let bytes = std::fs::read(&store.path).unwrap();
        let local = store.local().unwrap();
        local.deny_exchange.store(true, Ordering::Relaxed);
        // Force a checkpoint without changing the production threshold.
        let error = store.journal_test_checkpoint(&before).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("require filesystem atomic exchange")
        );
        assert_eq!(std::fs::read(&store.path).unwrap(), bytes);
        assert_eq!(fresh(&store).await, before);
        assert!(before.pins.contains_key(&seed));
        local.deny_exchange.store(false, Ordering::Relaxed);
        let next = store.register(staging("retry")).await.unwrap().unwrap();
        let recovered = fresh(&store).await;
        assert!(recovered.pins.contains_key(&seed) && recovered.pins.contains_key(&next));
    }
}

#[tokio::test]
#[ignore = "requires CASITA_LEDGER_FULL_DIR on a disposable filesystem smaller than 512 MiB"]
async fn physically_full_volume_preserves_acknowledged_state_and_recovers() {
    let root = std::env::var_os("CASITA_LEDGER_FULL_DIR").expect("dedicated test volume");
    let dir = tempfile::tempdir_in(root).unwrap();
    let store = FilePinStore::new(dir.path().join("pins"));
    let seed = store
        .register(staging("acknowledged"))
        .await
        .unwrap()
        .unwrap();
    let before = store.inventory().await.unwrap();
    let filler_path = dir.path().join("filler");
    let mut filler = std::fs::File::create(&filler_path).unwrap();
    let block = [0x5a; 64 * 1024];
    let mut written = 0;
    loop {
        match filler.write(&block) {
            Ok(count) => {
                assert_ne!(count, 0);
                written += count;
                assert!(written < 512 * 1024 * 1024, "test volume is too large");
            }
            Err(error) if error.kind() == std::io::ErrorKind::StorageFull => break,
            Err(error) => panic!("filling test volume: {error}"),
        }
    }
    // Reserved extents avoid growth, but a CoW filesystem can still need space
    // for overwrites or metadata. Either durable success or an explicit error
    // is acceptable; losing an acknowledged pin is not.
    let checkpoint = store.journal_test_checkpoint(&before);
    println!("full_volume_checkpoint: {checkpoint:?}");
    if let Err(error) = checkpoint {
        assert!(matches!(error, MetadataError::StorageFull), "{error}");
    }
    evict(&store);
    let registration = store.register(staging("while-full")).await;
    if let Err(error) = &registration {
        assert!(matches!(error, MetadataError::StorageFull), "{error}");
    }
    println!(
        "full_volume_registration_succeeded: {}",
        registration.is_ok()
    );
    drop(filler);
    std::fs::remove_file(filler_path).unwrap();
    let recovered = fresh(&store).await;
    assert!(recovered.pins.contains_key(&seed));
    if let Ok(Some(token)) = registration {
        assert!(recovered.pins.contains_key(&token));
    }
    let collector = store.acquire_collection(None).await.unwrap().unwrap();
    for token in recovered.pins.keys() {
        store.release(token).await.unwrap();
    }
    store.finish_collection(&collector).await.unwrap();
    assert!(fresh(&store).await.pins.is_empty());
}

#[tokio::test]
async fn cached_checkpoint_preserves_shared_protection_and_deletion_claims() {
    let dir = tempfile::tempdir().unwrap();
    let store = FilePinStore::new(dir.path().join("pins"));
    let shared = BTreeSet::from([
        PinResource::StorageObject("shared".into()),
        PinResource::MetadataObject("shared".into()),
        PinResource::Catalog(vec![1, 2, 3]),
    ]);
    let pin = DataPin {
        scope: PinScope::Staging,
        // A large catalog must survive checkpoints without becoming an addition.
        catalog: Some(vec![7; WINDOW]),
        resources: shared.clone(),
    };
    let first = store.register(pin.clone()).await.unwrap().unwrap();
    let second = store.register(pin).await.unwrap().unwrap();
    let revision = store.inventory().await.unwrap().revision;
    let claim = store
        .claim_deletions(revision, paths("claimed"))
        .await
        .unwrap()
        .unwrap();
    let checkpoints = store
        .local()
        .unwrap()
        .stats
        .checkpoints
        .load(Ordering::Relaxed);
    for (step, token) in [&first, &second, &first].into_iter().enumerate() {
        if step < 2 {
            // Fill the operation budget using real grouped edits, then let the
            // next addition take the cached checkpoint path.
            let operations = store
                .local()
                .unwrap()
                .cache
                .lock()
                .unwrap()
                .snapshot
                .as_ref()
                .unwrap()
                .operations;
            let warmup = (operations..CHECKPOINT_OPERATIONS)
                .map(|i| Operation::Protect(first.clone(), paths(&format!("padding/{step}/{i}"))))
                .collect::<Vec<_>>();
            for group in warmup.chunks(super::super::group::MAX_GROUP) {
                assert!(
                    store
                        .edit_group(group)
                        .unwrap()
                        .into_iter()
                        .all(|outcome| matches!(outcome, Ok(Outcome::Protected(true))))
                );
            }
        }
        // Include a duplicate resource to check multiplicities after checkpoint.
        let mut additions = shared.clone();
        additions.extend(paths("new"));
        assert!(store.protect(token, additions).await.unwrap());
    }
    assert_eq!(
        store
            .local()
            .unwrap()
            .stats
            .checkpoints
            .load(Ordering::Relaxed),
        // The third request is an exact duplicate and needs no journal write.
        checkpoints + 2
    );
    store.release(&first).await.unwrap();
    for resource in shared.iter().chain(paths("new").iter()) {
        let revision = store.inventory().await.unwrap().revision;
        assert!(
            store
                .claim_deletions(revision, BTreeSet::from([resource.clone()]))
                .await
                .unwrap()
                .is_none()
        );
    }
    assert!(store.register(staging("claimed")).await.unwrap().is_none());
    let expected = store.inventory().await.unwrap();
    {
        let local = store.local().unwrap();
        let cache = local.cache.lock().unwrap();
        assert_eq!(
            cache.snapshot.as_ref().unwrap().index.bytes,
            codec::encode(&expected).unwrap().len()
        );
    }
    assert_eq!(fresh(&store).await, expected);
    store.release(&second).await.unwrap();
    let revision = store.inventory().await.unwrap().revision;
    let mut released = shared;
    released.extend(paths("new"));
    let released_claim = store
        .claim_deletions(revision, released)
        .await
        .unwrap()
        .unwrap();
    store.finish_deletions(&released_claim).await.unwrap();
    store.finish_deletions(&claim).await.unwrap();
    let final_state = fresh(&store).await;
    assert!(final_state.pins.is_empty());
    assert!(final_state.deletions.is_empty());
}

#[tokio::test]
async fn checkpoint_and_gc_reuse_capacity_when_growth_is_denied() {
    use std::os::unix::fs::MetadataExt;
    let dir = tempfile::tempdir().unwrap();
    let store = FilePinStore::new(dir.path().join("pins"));
    let seed = store.register(staging("seed")).await.unwrap().unwrap();
    let active = std::fs::metadata(&store.path).unwrap();
    let spare = std::fs::metadata(store.spare_path().unwrap()).unwrap();
    let inodes = BTreeSet::from([active.ino(), spare.ino()]);
    store
        .local()
        .unwrap()
        .deny_growth
        .store(true, Ordering::Relaxed);
    for index in 0..CHECKPOINT_OPERATIONS + 1 {
        let label = format!("temporary/{index}");
        let token = store.register(staging(&label)).await.unwrap().unwrap();
        store.release(&token).await.unwrap();
    }
    assert!(
        store
            .local()
            .unwrap()
            .stats
            .checkpoints
            .load(Ordering::Relaxed)
            >= 3
    );
    let rev = store.inventory().await.unwrap().revision;
    let collector = store.begin_collection(rev, None).await.unwrap().unwrap();
    store.release(&seed).await.unwrap();
    let rev = store.inventory().await.unwrap().revision;
    let prune = store.begin_prune(rev).await.unwrap().unwrap();
    let rev = store.inventory().await.unwrap().revision;
    let claim = store
        .claim_deletions_during_prune(rev, paths("unrelated"), &collector, &prune)
        .await
        .unwrap()
        .unwrap();
    store.finish_deletions(&claim).await.unwrap();
    store.finish_prune(&prune).await.unwrap();
    store.finish_collection(&collector).await.unwrap();
    assert!(fresh(&store).await.pins.is_empty());
    let after = std::fs::metadata(&store.path).unwrap();
    let after_spare = std::fs::metadata(store.spare_path().unwrap()).unwrap();
    assert_eq!(BTreeSet::from([after.ino(), after_spare.ino()]), inodes);
    assert_eq!(
        (after.len(), after_spare.len()),
        (active.len(), spare.len())
    );
    let mut huge = staging("too-large-for-reserve");
    huge.catalog = Some(vec![1; WINDOW * 3]);
    assert!(matches!(
        store.register(huge).await,
        Err(MetadataError::StorageFull)
    ));
    assert!(fresh(&store).await.pins.is_empty());
}

#[tokio::test]
async fn full_legacy_ledger_can_collect_before_journal_upgrade() {
    let dir = tempfile::tempdir().unwrap();
    let mut legacy = FilePinStore::new(dir.path().join("pins"));
    legacy.replacement = true;
    let seed = legacy.register(staging("legacy")).await.unwrap().unwrap();
    let store = FilePinStore::new(&legacy.path);
    store
        .local()
        .unwrap()
        .deny_growth
        .store(true, Ordering::Relaxed);
    let current = store.inventory().await.unwrap();
    let collector = store
        .begin_collection(current.revision, None)
        .await
        .unwrap()
        .unwrap();
    store.release(&seed).await.unwrap();
    store.finish_collection(&collector).await.unwrap();
    assert!(store.inventory().await.unwrap().pins.is_empty());
    assert_eq!(&std::fs::read(&store.path).unwrap()[..8], b"CASPSL01");
    store
        .local()
        .unwrap()
        .deny_growth
        .store(false, Ordering::Relaxed);
    let upgraded = store.register(staging("upgraded")).await.unwrap().unwrap();
    assert_eq!(&std::fs::read(&store.path).unwrap()[..8], MAGIC);
    assert!(fresh(&store).await.pins.contains_key(&upgraded));
}

#[tokio::test]
async fn failed_append_invalidates_tentative_cache_and_retains_all_acknowledged_pins() {
    for phase in [
        "journal-partial-frame",
        "journal-before-sync",
        "journal-after-sync",
        "group-before-reply",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePinStore::new(dir.path().join("pins"));
        let seed = store
            .register(staging("acknowledged"))
            .await
            .unwrap()
            .unwrap();
        *store.local().unwrap().fail.lock().unwrap() = Some(phase);
        assert!(store.register(staging("ambiguous")).await.is_err());
        let recovered = fresh(&store).await;
        assert!(recovered.pins.contains_key(&seed));
        assert_eq!(
            recovered.pins.len(),
            if phase == "journal-partial-frame" {
                1
            } else {
                2
            }
        );
        let another = store
            .register(staging("after-recovery"))
            .await
            .unwrap()
            .unwrap();
        assert!(fresh(&store).await.pins.contains_key(&another));
    }
}

#[tokio::test]
async fn cancelled_acquisition_waits_for_journal_sync_before_releasing_its_token() {
    for phase in [
        "journal-partial-frame",
        "journal-before-sync",
        "journal-after-sync",
        "group-before-reply",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(FilePinStore::new(dir.path().join("pins")));
        let seed = store.register(staging("seed")).await.unwrap().unwrap();
        let (entered, receive) = std::sync::mpsc::channel();
        let (resume, wait) = std::sync::mpsc::channel();
        *store.local().unwrap().pause.lock().unwrap() = Some(super::super::group::Pause {
            phase,
            entered,
            resume: wait,
        });
        let task = tokio::spawn({
            let store = store.clone();
            async move { DataPinLease::try_acquire(store, staging("cancelled")).await }
        });
        tokio::task::spawn_blocking(move || {
            receive
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap()
        })
        .await
        .unwrap();
        assert!(
            !task.is_finished(),
            "no reply before the durability boundary"
        );
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        resume.send(()).unwrap();
        crate::metadata::flush_repository_leases().await.unwrap();
        let state = fresh(&store).await;
        assert_eq!(state.pins.len(), 1);
        assert!(state.pins.contains_key(&seed));
    }
}

#[test]
fn encoded_size_matches_the_wire_codec() {
    let mut state = PinInventory::default();
    for index in 0..32 {
        state.pins.insert(
            PinToken::fresh().unwrap(),
            staging(&format!("path-{index}")),
        );
        if index % 2 == 0 {
            state.reader_owners.insert(PinToken::fresh().unwrap());
            state.reader_revision_ceiling = 100;
        }
        assert_eq!(
            codec::encoded_len(&state).unwrap(),
            codec::encode(&state).unwrap().len()
        );
    }
}

async fn boundary_case(kind: &str, position: usize) -> serde_json::Value {
    use std::time::Instant;
    let dir = tempfile::tempdir().unwrap();
    let mut store = FilePinStore::new(dir.path().join("pins"));
    if kind == "migration-space" {
        store.replacement = true;
    }
    let seed = store.register(staging("seed")).await.unwrap().unwrap();
    store.replacement = false;
    let mut tokens = Vec::new();
    if kind == "checkpoint-operations" {
        // Group the warmup so the operation limit is reached while frame bytes
        // remain far below WINDOW. This gate must not pass via the byte limit.
        let operations = (1..position)
            .map(|index| Operation::Protect(seed.clone(), paths(&format!("extra/{index}"))))
            .collect::<Vec<_>>();
        for chunk in operations.chunks(super::super::group::MAX_GROUP) {
            let batch_store = store.clone();
            let batch = chunk.to_vec();
            let outcomes = tokio::task::spawn_blocking(move || batch_store.edit_group(&batch))
                .await
                .unwrap()
                .unwrap();
            assert!(
                outcomes
                    .into_iter()
                    .all(|outcome| matches!(outcome, Ok(Outcome::Protected(true))))
            );
        }
        let local = store.local().unwrap();
        let cache = local.cache.lock().unwrap();
        let snapshot = cache.snapshot.as_ref().unwrap();
        assert!(snapshot.cursor - snapshot.start < WINDOW / 2);
    } else if kind == "checkpoint-record-bytes" {
        let mut pin = staging("large-record");
        pin.catalog = Some(vec![7; position]);
        tokens.push(store.register(pin).await.unwrap().unwrap());
        // Start with an empty journal to isolate the size of a single changed
        // record from accumulated frame bytes and the operation-count limit.
        let state = store.inventory().await.unwrap();
        store.journal_test_checkpoint(&state).unwrap();
    } else if kind == "checkpoint-bytes" {
        store.ensure_journal_capacity(WINDOW * 4).unwrap();
        for index in 1..position {
            let mut pin = staging("large");
            pin.catalog = Some(vec![index as u8; WINDOW / 4]);
            tokens.push(store.register(pin).await.unwrap().unwrap());
        }
    } else if kind == "migration-space" {
        store
            .local()
            .unwrap()
            .deny_growth
            .store(position == 0, Ordering::Relaxed);
    } else {
        assert_eq!(kind, "group-size");
    }
    let revision = store.inventory().await.unwrap().revision;
    let before = store.local().unwrap().stats.snapshot();
    let start = Instant::now();
    let mut collector = None;
    match kind {
        "checkpoint-operations" => {
            assert!(
                store
                    .protect(&seed, paths(&format!("extra/{position}")))
                    .await
                    .unwrap()
            );
        }
        "checkpoint-record-bytes" => {
            for index in 0..3 {
                assert!(
                    store
                        .protect(&tokens[0], paths(&format!("tiny/{index}")))
                        .await
                        .unwrap()
                );
            }
        }
        "checkpoint-bytes" => {
            let mut pin = staging("large");
            pin.catalog = Some(vec![position as u8; WINDOW / 4]);
            tokens.push(store.register(pin).await.unwrap().unwrap());
        }
        "group-size" => {
            let operations = vec![Operation::Register(staging("group")); position];
            for chunk in operations.chunks(super::super::group::MAX_GROUP) {
                let batch_store = store.clone();
                let batch = chunk.to_vec();
                let outcomes = tokio::task::spawn_blocking(move || batch_store.edit_group(&batch))
                    .await
                    .unwrap()
                    .unwrap();
                for outcome in outcomes {
                    match outcome.unwrap() {
                        Outcome::Token(Some(token)) => tokens.push(token),
                        _ => panic!("registration failed"),
                    }
                }
            }
        }
        "migration-space" => {
            collector = store.begin_collection(revision, None).await.unwrap();
            assert!(collector.is_some());
        }
        _ => unreachable!(),
    }
    let nanos = start.elapsed().as_nanos() as u64;
    let metrics = store
        .local()
        .unwrap()
        .stats
        .snapshot()
        .into_iter()
        .map(|(name, value)| {
            (
                name,
                if name == "max_group" {
                    value
                } else {
                    value - before[name]
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    match kind {
        "checkpoint-record-bytes" => {
            // The retained catalog no longer enters a protection frame, on
            // either side of the former oversized-record cliff.
            assert_eq!(metrics["checkpoints"], 0);
            assert_eq!(metrics["journal_frames"], 3);
            assert_eq!(metrics["journal_syncs"], 3);
            assert_eq!(metrics["journal_bytes"], 3 * (BLOCK + HEADER) as u64);
        }
        "checkpoint-operations" | "checkpoint-bytes" => {
            let checkpoint = if kind == "checkpoint-operations" {
                position == CHECKPOINT_OPERATIONS + 1
            } else {
                position == 4
            };
            assert_eq!(metrics["checkpoints"], u64::from(checkpoint));
            assert_eq!(metrics["journal_frames"], u64::from(!checkpoint));
            assert_eq!(metrics["journal_syncs"], if checkpoint { 2 } else { 1 });
        }
        "group-size" => {
            let groups = position.div_ceil(super::super::group::MAX_GROUP) as u64;
            assert_eq!(metrics["groups"], groups);
            assert_eq!(metrics["journal_frames"], groups);
            assert_eq!(metrics["journal_syncs"], groups);
            assert_eq!(
                metrics["max_group"],
                position.min(super::super::group::MAX_GROUP) as u64
            );
        }
        "migration-space" => {
            assert_eq!(metrics["checkpoints"], position as u64);
            assert_eq!(metrics["replacement_updates"], u64::from(position == 0));
            let bytes = std::fs::read(&store.path).unwrap();
            assert_eq!(&bytes[..8], if position == 0 { b"CASPSL01" } else { MAGIC });
        }
        _ => unreachable!(),
    }
    let state = fresh(&store).await;
    assert_eq!(state.pins.len(), tokens.len() + 1);
    assert!(state.pins.contains_key(&seed));
    if kind == "checkpoint-operations" {
        assert_eq!(state.pins[&seed].resources.len(), position + 1);
    }
    for token in &tokens {
        assert!(state.pins.contains_key(token));
    }
    if kind == "checkpoint-record-bytes" {
        let pin = &state.pins[&tokens[0]];
        assert_eq!(pin.catalog.as_deref(), Some(vec![7; position].as_slice()));
        for index in 0..3 {
            assert!(paths(&format!("tiny/{index}")).is_subset(&pin.resources));
        }
    }
    if let Some(collector) = collector {
        store.finish_collection(&collector).await.unwrap();
    }
    for token in tokens {
        store.release(&token).await.unwrap();
    }
    store.release(&seed).await.unwrap();
    assert!(fresh(&store).await.pins.is_empty());
    serde_json::json!({"boundary":kind,"position":position,"nanos":nanos,"metrics":metrics,
        "journal_encoding":"resource-additions-v2",
        "correctness":"exact replay, bounded groups, exact checkpoint and migration boundaries, no leaked protection"})
}

#[tokio::test]
async fn checkpoint_and_group_boundaries_match_the_production_limits() {
    for position in [
        CHECKPOINT_OPERATIONS - 1,
        CHECKPOINT_OPERATIONS,
        CHECKPOINT_OPERATIONS + 1,
    ] {
        boundary_case("checkpoint-operations", position).await;
    }
    for position in [3, 4, 5] {
        boundary_case("checkpoint-bytes", position).await;
    }
    for position in [WINDOW - BLOCK, WINDOW, WINDOW + BLOCK] {
        boundary_case("checkpoint-record-bytes", position).await;
    }
    for position in [1, 2, 63, 64, 65] {
        boundary_case("group-size", position).await;
    }
    for position in [0, 1] {
        boundary_case("migration-space", position).await;
    }
}

#[tokio::test]
#[ignore = "permanent journal checkpoint, batch, and space boundaries; run benchmark ledger-boundaries"]
async fn benchmark_journal_boundaries() {
    let kind = std::env::var("CASITA_LEDGER_BOUNDARY").unwrap();
    let position = std::env::var("CASITA_LEDGER_POSITION")
        .unwrap()
        .parse()
        .unwrap();
    println!(
        "ledger_boundary_sample {}",
        boundary_case(&kind, position).await
    );
}

#[tokio::test]
#[ignore = "subprocess worker for killed ledger publishers"]
async fn ledger_process_worker() {
    let directory = PathBuf::from(std::env::var_os("CASITA_LEDGER_PROCESS_DIR").unwrap());
    let store = FilePinStore::new(directory.join("pins"));
    let extension = if std::env::var("CASITA_LEDGER_EXTEND").as_deref() == Ok("1") {
        Some(
            store
                .inventory()
                .await
                .unwrap()
                .pins
                .keys()
                .next()
                .unwrap()
                .clone(),
        )
    } else {
        None
    };
    if let Ok(label) = std::env::var("CASITA_LEDGER_CONCURRENT_WRITER") {
        for index in 0..12 {
            let resource = format!("writer/{label}/{index}");
            if let Some(token) = &extension {
                assert!(store.protect(token, paths(&resource)).await.unwrap());
            } else {
                store.register(staging(&resource)).await.unwrap().unwrap();
            }
        }
    } else {
        if let Some(token) = &extension {
            assert!(store.protect(token, paths("ambiguous")).await.unwrap());
        } else {
            store.register(staging("ambiguous")).await.unwrap().unwrap();
        }
        panic!("publisher did not stop at the requested crash phase");
    }
}

struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn process_command(directory: &std::path::Path) -> std::process::Command {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "metadata::pins::persistent::journal::tests::ledger_process_worker",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env("CASITA_LEDGER_PROCESS_DIR", directory)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    command
}

#[tokio::test]
async fn killed_publishers_preserve_acknowledged_pins_and_deletion_claims() {
    for extension in [false, true] {
        killed_publisher_case(extension).await;
    }
}

async fn killed_publisher_case(extension: bool) {
    for phase in [
        "journal-partial-frame",
        "journal-before-sync",
        "journal-after-sync",
        "group-before-reply",
        "checkpoint-before-sync",
        "checkpoint-before-exchange",
        "checkpoint-after-exchange",
        "checkpoint-after-directory-sync",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePinStore::new(dir.path().join("pins"));
        let seed = store
            .register(staging("acknowledged"))
            .await
            .unwrap()
            .unwrap();
        let revision = store.inventory().await.unwrap().revision;
        let claim = store
            .claim_deletions(revision, paths("claimed"))
            .await
            .unwrap()
            .unwrap();
        if phase.starts_with("checkpoint-") {
            // Advance the actual journal to its operation limit without changing
            // the acknowledged seed or deletion claim.
            let operations = store
                .local()
                .unwrap()
                .cache
                .lock()
                .unwrap()
                .snapshot
                .as_ref()
                .unwrap()
                .operations;
            for index in operations..CHECKPOINT_OPERATIONS {
                assert!(
                    store
                        .protect(&seed, paths(&format!("extra/{index}")))
                        .await
                        .unwrap()
                );
            }
        }
        let acknowledged = store.inventory().await.unwrap();
        let signal = dir.path().join("stopped");
        let mut child = Child(
            process_command(dir.path())
                .env("CASITA_LEDGER_EXTEND", if extension { "1" } else { "0" })
                .env("CASITA_LEDGER_CRASH_PHASE", phase)
                .env("CASITA_LEDGER_CRASH_SIGNAL", &signal)
                .spawn()
                .unwrap(),
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !signal.exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "worker exited before {phase}"
            );
            assert!(
                std::time::Instant::now() < deadline,
                "worker timed out at {phase}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let recovered = fresh(&store).await;
        if extension {
            let mut with_ambiguous = acknowledged.pins[&seed].clone();
            with_ambiguous.resources.extend(paths("ambiguous"));
            assert!(
                recovered.pins[&seed] == acknowledged.pins[&seed]
                    || recovered.pins[&seed] == with_ambiguous,
                "{phase}"
            );
        } else {
            assert_eq!(recovered.pins[&seed], acknowledged.pins[&seed], "{phase}");
        }
        assert_eq!(recovered.deletions, acknowledged.deletions, "{phase}");
        assert!(recovered.revision >= acknowledged.revision);
        assert!(store.register(staging("claimed")).await.unwrap().is_none());
        let next = store
            .register(staging("after-crash"))
            .await
            .unwrap()
            .unwrap();
        assert!(fresh(&store).await.pins.contains_key(&next));
        store.finish_deletions(&claim).await.unwrap();
    }
}

#[tokio::test]
async fn independent_processes_replay_and_append_without_losing_updates() {
    for extension in [false, true] {
        independent_process_case(extension).await;
    }
}

async fn independent_process_case(extension: bool) {
    let dir = tempfile::tempdir().unwrap();
    let store = FilePinStore::new(dir.path().join("pins"));
    store.register(staging("seed")).await.unwrap().unwrap();
    let mut children = (0..4)
        .map(|index| {
            Child(
                process_command(dir.path())
                    .env("CASITA_LEDGER_EXTEND", if extension { "1" } else { "0" })
                    .env("CASITA_LEDGER_CONCURRENT_WRITER", index.to_string())
                    .spawn()
                    .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    for child in &mut children {
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "concurrent ledger worker timed out"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
    let recovered = fresh(&store).await;
    assert_eq!(recovered.pins.len(), if extension { 1 } else { 49 });
    let resources = recovered
        .pins
        .values()
        .flat_map(|pin| pin.resources.iter().cloned())
        .collect::<BTreeSet<_>>();
    assert_eq!(resources.len(), 49);
    for writer in 0..4 {
        for index in 0..12 {
            assert!(resources.contains(&PinResource::StorageObject(format!(
                "writer/{writer}/{index}"
            ))));
        }
    }
    assert_eq!(recovered.revision, 49);
}

#[tokio::test]
async fn corrupt_frame_epoch_does_not_silently_discard_acknowledged_protection() {
    let dir = tempfile::tempdir().unwrap();
    let store = FilePinStore::new(dir.path().join("pins"));
    store.register(staging("seed")).await.unwrap().unwrap();
    let cursor = store
        .local()
        .unwrap()
        .cache
        .lock()
        .unwrap()
        .snapshot
        .as_ref()
        .unwrap()
        .cursor;
    store
        .register(staging("acknowledged"))
        .await
        .unwrap()
        .unwrap();
    let mut file = std::fs::File::options()
        .read(true)
        .write(true)
        .open(&store.path)
        .unwrap();
    file.seek(SeekFrom::Start((cursor + 8) as u64)).unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    file.seek(SeekFrom::Start((cursor + 8) as u64)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
    evict(&store);
    assert!(store.inventory().await.is_err());
    assert!(store.register(staging("must-fail-closed")).await.is_err());
}

#[test]
fn journal_append_refuses_frames_replay_would_drop_or_reject() {
    let epoch = [1; 32];
    let group = super::super::group::MAX_GROUP;
    assert_eq!(
        validate_journal_append(Some(&epoch), &epoch, group, CHECKPOINT_OPERATIONS, WINDOW),
        Ok(())
    );
    let stale = [2; 32];
    let broken = [
        ("stale epoch", Some(&stale), 1, 1, BLOCK),
        ("active file is not a journal", None, 1, 1, BLOCK),
        ("empty frame", Some(&epoch), 0, 1, BLOCK),
        ("oversized group", Some(&epoch), group + 1, group + 1, BLOCK),
        (
            "operations past a checkpoint",
            Some(&epoch),
            1,
            CHECKPOINT_OPERATIONS + 1,
            BLOCK,
        ),
        ("bytes past the window", Some(&epoch), 1, 1, WINDOW + BLOCK),
    ];
    for (case, active, operations, journal_operations, journal_bytes) in broken {
        assert!(
            validate_journal_append(
                active,
                &epoch,
                operations,
                journal_operations,
                journal_bytes
            )
            .is_err(),
            "{case}"
        );
    }
}

#[test]
fn checkpoint_refuses_to_change_what_it_compacts_or_reuse_its_epoch() {
    let mut replayed = PinInventory {
        revision: 7,
        ..PinInventory::default()
    };
    replayed
        .pins
        .insert(PinToken::fresh().unwrap(), staging("acknowledged"));
    assert_eq!(validate_journal_checkpoint(&replayed, &replayed), Ok(()));
    let dropped = PinInventory {
        revision: 7,
        ..PinInventory::default()
    };
    assert!(validate_journal_checkpoint(&replayed, &dropped).is_err());
    let epoch = [3; 32];
    assert_eq!(validate_checkpoint_epoch(None, &epoch), Ok(()));
    assert!(validate_checkpoint_epoch(Some(&epoch), &epoch).is_err());
}
