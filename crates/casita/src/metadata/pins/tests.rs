use super::*;

fn resources(name: &str) -> BTreeSet<PinResource> {
    BTreeSet::from([PinResource::StorageObject(name.into())])
}

fn staging(name: &str) -> DataPin {
    DataPin {
        scope: PinScope::Staging,
        catalog: None,
        resources: resources(name),
    }
}

#[tokio::test]
async fn validated_claims_accept_churn_but_reject_unchecked_or_overlapping_protection() {
    let directory = tempfile::tempdir().unwrap();
    let stores: Vec<Arc<dyn PinStore>> = vec![
        Arc::new(MemoryPinStore::default()),
        Arc::new(FilePinStore::new(directory.path().join("validated-claims"))),
        Arc::new(ObjectPinStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            "validated-claims".into(),
        )),
    ];
    let blob = |name: &[u8]| BlobId::new(crate::Digest::hash(name));
    let chunk = |name: &[u8]| PinResource::Chunk(ChunkId::new(crate::Digest::hash(name)));
    for store in stores {
        let collector = store.acquire_collection(None).await.unwrap().unwrap();
        let known = blob(b"known manifest");
        let late = blob(b"late manifest");
        let candidate = BTreeSet::from([chunk(b"candidate")]);
        let held = BTreeSet::from([chunk(b"held")]);
        let pin = store
            .register(DataPin {
                scope: PinScope::Staging,
                catalog: None,
                resources: BTreeSet::from([PinResource::Blob(known)]),
            })
            .await
            .unwrap()
            .unwrap();
        let late_pin = store
            .register(DataPin {
                scope: PinScope::Staging,
                catalog: None,
                resources: BTreeSet::from([PinResource::Blob(late)]),
            })
            .await
            .unwrap()
            .unwrap();
        assert!(
            store
                .claim_deletions_validated(&collector, candidate.clone(), BTreeSet::from([known]))
                .await
                .unwrap()
                .is_none(),
            "new manifests require expansion"
        );
        let checked = BTreeSet::from([known, late]);
        assert!(store.protect(&pin, held.clone()).await.unwrap());
        assert!(
            store
                .claim_deletions_validated(&collector, held.clone(), checked.clone())
                .await
                .unwrap()
                .is_none(),
            "direct chunk protection cannot be waived"
        );
        store.release(&pin).await.unwrap();
        assert!(
            store
                .claim_deletions_validated(&collector, held, checked.clone())
                .await
                .unwrap()
                .is_none(),
            "retired history remains protected"
        );
        assert!(
            store
                .claim_deletions_validated(
                    &PinToken::fresh().unwrap(),
                    candidate.clone(),
                    checked.clone()
                )
                .await
                .unwrap()
                .is_none()
        );
        let fence = store
            .begin_prune(store.inventory().await.unwrap().revision)
            .await
            .unwrap()
            .unwrap();
        assert!(
            store
                .claim_deletions_validated(&collector, candidate.clone(), checked.clone())
                .await
                .unwrap()
                .is_none(),
            "ordinary claims cannot cross a prune fence"
        );
        store.finish_prune(&fence).await.unwrap();
        let old = store.inventory().await.unwrap();
        let metadata = store
            .register(DataPin {
                scope: PinScope::Metadata,
                catalog: None,
                resources: BTreeSet::from([PinResource::MetadataObject(
                    "unrelated metadata".into(),
                )]),
            })
            .await
            .unwrap()
            .unwrap();
        assert!(
            store
                .protect(&late_pin, BTreeSet::from([chunk(b"unrelated chunk")]))
                .await
                .unwrap()
        );
        assert!(
            store
                .claim_deletions(old.revision, candidate.clone())
                .await
                .unwrap()
                .is_none()
        );
        let claim = store
            .claim_deletions_validated(&collector, candidate.clone(), checked.clone())
            .await
            .unwrap()
            .unwrap();
        assert!(
            store
                .claim_deletions_validated(&collector, candidate.clone(), checked.clone())
                .await
                .unwrap()
                .is_none(),
            "overlapping claims remain exclusive"
        );
        assert!(!store.protect(&late_pin, candidate.clone()).await.unwrap());
        assert!(
            store
                .register(DataPin {
                    scope: PinScope::Staging,
                    catalog: None,
                    resources: candidate.clone(),
                })
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .claim_deletions_validated(
                    &collector,
                    resources("unsupported storage path"),
                    checked.clone()
                )
                .await
                .is_err()
        );
        // Blob candidates do not require expanding other manifests, but direct
        // protection of the blob itself must still reject deletion.
        assert!(
            store
                .claim_deletions_validated(
                    &collector,
                    BTreeSet::from([PinResource::Blob(known)]),
                    BTreeSet::new()
                )
                .await
                .unwrap()
                .is_none()
        );
        let blob_claim = store
            .claim_deletions_validated(
                &collector,
                BTreeSet::from([PinResource::Blob(blob(b"garbage manifest"))]),
                BTreeSet::new(),
            )
            .await
            .unwrap()
            .unwrap();
        store.finish_deletions(&blob_claim).await.unwrap();
        store.finish_deletions(&claim).await.unwrap();
        store.release(&metadata).await.unwrap();
        store.release(&late_pin).await.unwrap();
        store.finish_collection(&collector).await.unwrap();
        assert!(store.inventory().await.unwrap().pins.is_empty());
    }
}

#[tokio::test]
async fn atomic_collector_takeover_keeps_history_and_requires_the_exact_previous_owner() {
    let directory = tempfile::tempdir().unwrap();
    let stores: Vec<Arc<dyn PinStore>> = vec![
        Arc::new(MemoryPinStore::default()),
        Arc::new(FilePinStore::new(directory.path().join("takeover"))),
        Arc::new(ObjectPinStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            "takeover".into(),
        )),
    ];
    for store in stores {
        let first = store.acquire_collection(None).await.unwrap().unwrap();
        let pin = store.register(staging("retired")).await.unwrap().unwrap();
        store.release(&pin).await.unwrap();
        let claim = store
            .claim_deletions(
                store.inventory().await.unwrap().revision,
                resources("garbage"),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(store.acquire_collection(None).await.unwrap().is_none());
        assert!(
            store
                .acquire_collection(Some(PinToken::fresh().unwrap()))
                .await
                .unwrap()
                .is_none()
        );
        let second = store
            .acquire_collection(Some(first.clone()))
            .await
            .unwrap()
            .unwrap();
        assert!(
            store
                .acquire_collection(Some(first.clone()))
                .await
                .unwrap()
                .is_none()
        );
        store.finish_collection(&first).await.unwrap();
        let inventory = store.inventory().await.unwrap();
        assert_eq!(inventory.collector, Some(second.clone()));
        assert!(inventory.retired.contains(&pin) && inventory.pins.contains_key(&pin));
        assert!(inventory.deletions.contains_key(&claim));
        store.finish_deletions(&claim).await.unwrap();
        store.finish_collection(&second).await.unwrap();
        assert!(store.inventory().await.unwrap().pins.is_empty());
    }
}

#[tokio::test]
async fn validating_prune_captures_current_pins_with_exact_collector_and_claims() {
    let directory = tempfile::tempdir().unwrap();
    let stores: Vec<Arc<dyn PinStore>> = vec![
        Arc::new(MemoryPinStore::default()),
        Arc::new(FilePinStore::new(directory.path().join("validating"))),
        Arc::new(ObjectPinStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            "validating".into(),
        )),
    ];
    for store in stores {
        let collector = store
            .begin_collection(store.inventory().await.unwrap().revision, None)
            .await
            .unwrap()
            .unwrap();
        let marked = store.inventory().await.unwrap();
        let pin = store.register(staging("late")).await.unwrap().unwrap();
        assert!(store.protect(&pin, resources("later")).await.unwrap());
        let retired = store.register(staging("retired")).await.unwrap().unwrap();
        store.release(&retired).await.unwrap();
        let claim = store
            .claim_deletions(
                store.inventory().await.unwrap().revision,
                resources("garbage"),
            )
            .await
            .unwrap()
            .unwrap();
        let claims = BTreeSet::from([claim.clone()]);
        let before = store.inventory().await.unwrap();
        assert!(before.revision > marked.revision);
        assert!(
            store
                .begin_prune_validating(&PinToken::fresh().unwrap(), claims.clone())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .begin_prune_validating(&collector, BTreeSet::new())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(store.inventory().await.unwrap(), before);
        let (prune, admitted) = store
            .begin_prune_validating(&collector, claims.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(admitted, before);
        assert!(
            admitted.pins[&pin]
                .resources
                .is_superset(&resources("later"))
        );
        assert!(admitted.retired.contains(&retired));
        assert!(store.register(staging("blocked")).await.unwrap().is_none());
        assert!(!store.protect(&pin, resources("blocked")).await.unwrap());
        assert!(
            store
                .begin_prune_validating(&collector, claims)
                .await
                .unwrap()
                .is_none()
        );
        store
            .finish_prune(&PinToken::fresh().unwrap())
            .await
            .unwrap();
        assert_eq!(
            store.inventory().await.unwrap().logical_prune,
            Some(prune.clone())
        );
        store.finish_prune(&prune).await.unwrap();
        assert!(store.protect(&pin, resources("allowed")).await.unwrap());
        store.finish_deletions(&claim).await.unwrap();
        store.release(&pin).await.unwrap();
        store.finish_collection(&collector).await.unwrap();
        let finished = store.inventory().await.unwrap();
        assert!(finished.pins.is_empty());
        assert!(finished.logical_prune.is_none());
        assert!(finished.collector.is_none());
        assert!(finished.deletions.is_empty());
    }
}

#[tokio::test]
async fn covered_roots_do_not_authorize_new_snapshot_generations() {
    let store = MemoryPinStore::default();
    let metadata = crate::MemoryMetadataStore::new().unwrap();
    let snapshot = crate::metadata::MetadataStore::snapshot(&metadata)
        .await
        .unwrap();
    let empty = store.inventory().await.unwrap();
    let retained = BTreeSet::<ObjectKey>::new();
    let zero = store
        .register(DataPin {
            scope: PinScope::Snapshot { generation: 0 },
            catalog: None,
            resources: BTreeSet::new(),
        })
        .await
        .unwrap()
        .unwrap();
    let marked = store.inventory().await.unwrap();
    assert!(
        !marked
            .logical_pins_covered_by(&empty, &retained, snapshot.as_ref())
            .await
            .unwrap()
    );
    store
        .register(DataPin {
            scope: PinScope::Snapshot { generation: 1 },
            catalog: None,
            resources: BTreeSet::new(),
        })
        .await
        .unwrap()
        .unwrap();
    let newer = store.inventory().await.unwrap();
    assert!(
        !newer
            .logical_pins_covered_by(&marked, &retained, snapshot.as_ref())
            .await
            .unwrap()
    );
    assert!(
        marked
            .logical_pins_covered_by(&newer, &retained, snapshot.as_ref())
            .await
            .unwrap()
    );
    store.release(&zero).await.unwrap();
    assert!(
        store
            .inventory()
            .await
            .unwrap()
            .logical_pins_covered_by(&newer, &retained, snapshot.as_ref())
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn logical_mark_ignores_physical_growth_but_tracks_generations_and_objects() {
    let directory = tempfile::tempdir().unwrap();
    let stores: Vec<Arc<dyn PinStore>> = vec![
        Arc::new(MemoryPinStore::default()),
        Arc::new(FilePinStore::new(directory.path().join("logical-mark"))),
        Arc::new(ObjectPinStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            "logical-mark".into(),
        )),
    ];
    let root = ObjectKey::blob(BlobId::new(crate::Digest::hash(b"root")));
    for store in stores {
        let empty = store.inventory().await.unwrap();
        let writer = store
            .register(DataPin {
                scope: PinScope::Staging,
                catalog: Some(b"new physical catalog".to_vec()),
                resources: resources("physical"),
            })
            .await
            .unwrap()
            .unwrap();
        let physical = store.inventory().await.unwrap();
        assert!(physical.same_logical_pins(&empty));
        assert!(!physical.same_payload_pins(&empty));
        assert!(
            store
                .protect(&writer, resources("more physical"))
                .await
                .unwrap()
        );
        assert!(store.inventory().await.unwrap().same_logical_pins(&empty));
        assert!(
            store
                .protect(&writer, BTreeSet::from([PinResource::Object(root.clone())]))
                .await
                .unwrap()
        );
        let object = store.inventory().await.unwrap();
        assert!(!object.same_logical_pins(&physical));
        let closure = store
            .register(DataPin {
                scope: PinScope::Closures(BTreeSet::from([root.clone()])),
                catalog: None,
                resources: BTreeSet::new(),
            })
            .await
            .unwrap()
            .unwrap();
        assert!(store.inventory().await.unwrap().same_logical_pins(&object));
        store.release(&writer).await.unwrap();
        assert!(store.inventory().await.unwrap().same_logical_pins(&object));
        store.release(&closure).await.unwrap();
        for generation in [0, 1, 2] {
            let before = store.inventory().await.unwrap();
            store
                .register(DataPin {
                    scope: PinScope::Snapshot { generation },
                    catalog: None,
                    resources: BTreeSet::new(),
                })
                .await
                .unwrap()
                .unwrap();
            assert!(!store.inventory().await.unwrap().same_logical_pins(&before));
        }
        let latest = store.inventory().await.unwrap();
        store
            .register(DataPin {
                scope: PinScope::Snapshot { generation: 1 },
                catalog: None,
                resources: BTreeSet::new(),
            })
            .await
            .unwrap()
            .unwrap();
        assert!(store.inventory().await.unwrap().same_logical_pins(&latest));
    }
}

#[tokio::test]
async fn collector_admission_ignores_duplicate_pin_churn() {
    let directory = tempfile::tempdir().unwrap();
    let stores: Vec<Arc<dyn PinStore>> = vec![
        Arc::new(MemoryPinStore::default()),
        Arc::new(FilePinStore::new(directory.path().join("admission-race"))),
        Arc::new(ObjectPinStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            "admission-race".into(),
        )),
    ];
    for store in stores {
        let pin = staging("same payload");
        let original = store.register(pin.clone()).await.unwrap().unwrap();
        let before = store.inventory().await.unwrap();
        let duplicate = store.register(pin).await.unwrap().unwrap();
        let current = store.inventory().await.unwrap();
        assert!(current.same_payload_pins(&before));
        assert!(
            store
                .begin_collection(before.revision, None)
                .await
                .unwrap()
                .is_none()
        );
        let collector = CollectorLease::try_acquire(store.clone(), None)
            .await
            .unwrap()
            .unwrap();
        // Refreshing a revision must never authorize replacing another collector.
        assert!(
            CollectorLease::try_acquire(store.clone(), None)
                .await
                .unwrap()
                .is_none()
        );
        store.release(&original).await.unwrap();
        store.release(&duplicate).await.unwrap();
        collector.finish().await.unwrap();
        assert!(store.inventory().await.unwrap().pins.is_empty());
    }
}

#[tokio::test]
async fn duplicate_pins_preserve_marks_but_new_protection_invalidates_them() {
    let directory = tempfile::tempdir().unwrap();
    let stores: Vec<Arc<dyn PinStore>> = vec![
        Arc::new(MemoryPinStore::default()),
        Arc::new(FilePinStore::new(directory.path().join("duplicates"))),
        Arc::new(ObjectPinStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            "duplicates".into(),
        )),
    ];
    for store in stores {
        let pin = DataPin {
            scope: PinScope::Snapshot { generation: 1 },
            catalog: Some(b"catalog".to_vec()),
            resources: resources("payload"),
        };
        let original = store.register(pin.clone()).await.unwrap().unwrap();
        let collector = store
            .begin_collection(store.inventory().await.unwrap().revision, None)
            .await
            .unwrap()
            .unwrap();
        let marked = store.inventory().await.unwrap();
        let duplicate = store.register(pin.clone()).await.unwrap().unwrap();
        store.release(&original).await.unwrap();
        assert!(store.inventory().await.unwrap().same_payload_pins(&marked));
        let prune = store
            .begin_prune(store.inventory().await.unwrap().revision)
            .await
            .unwrap()
            .unwrap();
        // Ignoring duplicate ownership must not bypass the admission fence.
        assert!(store.register(pin.clone()).await.unwrap().is_none());
        assert!(!store.protect(&duplicate, resources("new")).await.unwrap());
        store.finish_prune(&prune).await.unwrap();
        // Each part of the protection still participates in comparison.
        for changed in [
            DataPin {
                scope: PinScope::Snapshot { generation: 2 },
                ..pin.clone()
            },
            DataPin {
                catalog: Some(b"new catalog".to_vec()),
                ..pin.clone()
            },
            DataPin {
                resources: resources("new"),
                ..pin.clone()
            },
        ] {
            let before = store.inventory().await.unwrap();
            let token = store.register(changed).await.unwrap().unwrap();
            let current = store.inventory().await.unwrap();
            assert!(!current.same_payload_pins(&before));
            assert!(!before.same_payload_pins(&current));
            store.release(&token).await.unwrap();
        }
        store.release(&duplicate).await.unwrap();
        store.finish_collection(&collector).await.unwrap();
        assert!(store.inventory().await.unwrap().pins.is_empty());
    }
}

#[tokio::test]
async fn empty_writer_churn_does_not_hide_first_protection_or_catalogs() {
    let directory = tempfile::tempdir().unwrap();
    let stores: Vec<Arc<dyn PinStore>> = vec![
        Arc::new(MemoryPinStore::default()),
        Arc::new(FilePinStore::new(directory.path().join("idle"))),
        Arc::new(ObjectPinStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            "idle".into(),
        )),
    ];
    for store in stores {
        let empty = store.inventory().await.unwrap();
        let idle = DataPin {
            scope: PinScope::Staging,
            catalog: None,
            resources: BTreeSet::new(),
        };
        let token = store.register(idle.clone()).await.unwrap().unwrap();
        assert!(store.inventory().await.unwrap().same_payload_pins(&empty));
        // Even with no explicit resources, a catalog or snapshot changes what
        // GC must retain. Only an entirely empty staging pin is irrelevant.
        for pin in [
            DataPin {
                catalog: Some(b"catalog".to_vec()),
                ..idle.clone()
            },
            DataPin {
                scope: PinScope::Snapshot { generation: 0 },
                ..idle.clone()
            },
        ] {
            let protected = store.register(pin).await.unwrap().unwrap();
            assert!(!store.inventory().await.unwrap().same_payload_pins(&empty));
            store.release(&protected).await.unwrap();
        }
        let collector = store
            .begin_collection(store.inventory().await.unwrap().revision, None)
            .await
            .unwrap()
            .unwrap();
        let marked = store.inventory().await.unwrap();
        let prune = store.begin_prune(marked.revision).await.unwrap().unwrap();
        assert!(
            !store
                .protect(&token, resources("first-write"))
                .await
                .unwrap()
        );
        store.finish_prune(&prune).await.unwrap();
        assert!(
            store
                .protect(&token, resources("first-write"))
                .await
                .unwrap()
        );
        assert!(!store.inventory().await.unwrap().same_payload_pins(&marked));
        store.release(&token).await.unwrap();
        // Retirement retains the real protection through the collector pass.
        assert!(!store.inventory().await.unwrap().same_payload_pins(&marked));
        store.finish_collection(&collector).await.unwrap();
        assert!(store.inventory().await.unwrap().pins.is_empty());
    }
}

#[tokio::test]
async fn emergency_deletion_requires_the_exact_collector_and_prune_fence() {
    let directory = tempfile::tempdir().unwrap();
    let stores: Vec<Arc<dyn PinStore>> = vec![
        Arc::new(MemoryPinStore::default()),
        Arc::new(FilePinStore::new(directory.path().join("emergency"))),
        Arc::new(ObjectPinStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            "emergency".into(),
        )),
    ];
    for store in stores {
        let live = store.register(staging("live")).await.unwrap().unwrap();
        let collector = store
            .begin_collection(store.inventory().await.unwrap().revision, None)
            .await
            .unwrap()
            .unwrap();
        let before = store.inventory().await.unwrap();
        let prune = store.begin_prune(before.revision).await.unwrap().unwrap();
        let marked = store.inventory().await.unwrap();
        assert!(store.register(staging("new")).await.unwrap().is_none());
        assert!(!store.protect(&live, resources("new")).await.unwrap());
        assert!(
            store
                .claim_deletions(marked.revision, resources("dead"))
                .await
                .unwrap()
                .is_none()
        );
        for (revision, collector_token, prune_token, targets) in [
            (
                before.revision,
                collector.clone(),
                prune.clone(),
                resources("dead"),
            ),
            (
                marked.revision,
                live.clone(),
                prune.clone(),
                resources("dead"),
            ),
            (
                marked.revision,
                collector.clone(),
                live.clone(),
                resources("dead"),
            ),
            (
                marked.revision,
                collector.clone(),
                prune.clone(),
                resources("live"),
            ),
        ] {
            assert!(
                store
                    .claim_deletions_during_prune(revision, targets, &collector_token, &prune_token)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(store.inventory().await.unwrap(), marked);
        }
        let authorized = PruningPinStore {
            inner: store.clone(),
            collector: collector.clone(),
            prune: prune.clone(),
        };
        assert_eq!(authorized.inventory().await.unwrap(), marked);
        assert!(!store.allows_deletion(&marked));
        assert!(authorized.allows_deletion(&marked));
        let claim = authorized
            .claim_deletions(marked.revision, resources("dead"))
            .await
            .unwrap()
            .unwrap();
        let claimed = store.inventory().await.unwrap();
        assert!(
            store
                .claim_deletions_during_prune(
                    claimed.revision,
                    resources("dead"),
                    &collector,
                    &prune
                )
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(claimed.logical_prune.as_ref(), Some(&prune));
        assert!(store.finish_collection(&collector).await.is_err());
        // An exact takeover revokes the old collector even if its caller
        // retained a copy of the still-active logical fence token.
        let successor = store
            .begin_collection(claimed.revision, Some(collector.clone()))
            .await
            .unwrap()
            .unwrap();
        let current = store.inventory().await.unwrap();
        assert!(
            store
                .claim_deletions_during_prune(
                    current.revision,
                    resources("other"),
                    &collector,
                    &prune
                )
                .await
                .unwrap()
                .is_none()
        );
        let other = store
            .claim_deletions_during_prune(current.revision, resources("other"), &successor, &prune)
            .await
            .unwrap()
            .unwrap();
        store.release(&live).await.unwrap();
        let released = store.inventory().await.unwrap();
        assert!(
            store
                .claim_deletions_during_prune(
                    released.revision,
                    resources("live"),
                    &successor,
                    &prune
                )
                .await
                .unwrap()
                .is_none()
        );
        store.finish_deletions(&claim).await.unwrap();
        store.finish_deletions(&other).await.unwrap();
        assert!(store.register(staging("new")).await.unwrap().is_none());
        store.finish_prune(&prune).await.unwrap();
        let current = store.inventory().await.unwrap();
        assert!(
            store
                .claim_deletions_during_prune(
                    current.revision,
                    resources("later"),
                    &successor,
                    &prune
                )
                .await
                .unwrap()
                .is_none()
        );
        store.finish_collection(&successor).await.unwrap();
        assert!(store.inventory().await.unwrap().pins.is_empty());
    }
}

#[tokio::test]
async fn new_pin_invalidates_mark_and_only_protects_its_own_data() {
    let store = MemoryPinStore::default();
    let before = store.inventory().await.unwrap();
    let pin = store.register(staging("live")).await.unwrap().unwrap();
    assert!(
        store
            .claim_deletions(before.revision, resources("garbage"))
            .await
            .unwrap()
            .is_none()
    );
    let current = store.inventory().await.unwrap();
    assert!(
        store
            .claim_deletions(current.revision, resources("live"))
            .await
            .unwrap()
            .is_none()
    );
    let deletion = store
        .claim_deletions(current.revision, resources("garbage"))
        .await
        .unwrap()
        .unwrap();
    assert!(store.protect(&pin, resources("other")).await.unwrap());
    store.finish_deletions(&deletion).await.unwrap();
}

#[tokio::test]
async fn identical_path_cannot_be_recreated_until_deletion_has_settled() {
    let store = MemoryPinStore::default();
    let revision = store.inventory().await.unwrap().revision;
    let deletion = store
        .claim_deletions(revision, resources("same-digest"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .register(staging("same-digest"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .register(staging("unrelated"))
            .await
            .unwrap()
            .is_some()
    );
    store
        .finish_deletions(&PinToken::fresh().unwrap())
        .await
        .unwrap();
    assert!(
        store
            .register(staging("same-digest"))
            .await
            .unwrap()
            .is_none()
    );
    store.finish_deletions(&deletion).await.unwrap();
    let replacement = store
        .register(staging("same-digest"))
        .await
        .unwrap()
        .unwrap();
    store.finish_deletions(&deletion).await.unwrap();
    assert!(
        store
            .inventory()
            .await
            .unwrap()
            .pins
            .contains_key(&replacement)
    );
}

#[tokio::test]
async fn deletion_and_pin_registration_have_one_winner() {
    // Both orders are exercised deterministically elsewhere; this covers
    // arbitration through independently cloned handles to the same ledger.
    for _ in 0..32 {
        let store = MemoryPinStore::default();
        let writer = store.clone();
        let collector = store.clone();
        let (pin, deletion) = tokio::join!(
            writer.register(staging("same-path")),
            collector.claim_deletions(0, resources("same-path")),
        );
        assert_ne!(pin.unwrap().is_some(), deletion.unwrap().is_some());
    }
}

#[tokio::test]
async fn pruning_fences_new_scope_and_resources_but_existing_data_stays_usable() {
    let store = MemoryPinStore::default();
    let pin = store.register(staging("existing")).await.unwrap().unwrap();
    let revision = store.inventory().await.unwrap().revision;
    let prune = store.begin_prune(revision).await.unwrap().unwrap();
    assert!(store.register(staging("new")).await.unwrap().is_none());
    assert!(!store.protect(&pin, resources("new")).await.unwrap());
    assert!(store.protect(&pin, resources("existing")).await.unwrap());
    let during = store.inventory().await.unwrap().revision;
    assert!(
        store
            .claim_deletions(during, resources("garbage"))
            .await
            .unwrap()
            .is_none()
    );
    store
        .finish_prune(&PinToken::fresh().unwrap())
        .await
        .unwrap();
    assert!(store.register(staging("new")).await.unwrap().is_none());
    store.finish_prune(&prune).await.unwrap();
    assert!(store.register(staging("new")).await.unwrap().is_some());
    assert!(store.begin_prune(revision).await.unwrap().is_none());
}

#[tokio::test]
async fn post_prune_read_scopes_do_not_wait_for_unrelated_physical_deletion() {
    let store = MemoryPinStore::default();
    let deletion = store
        .claim_deletions(0, resources("pack"))
        .await
        .unwrap()
        .unwrap();
    let root = ObjectKey::blob(BlobId::new(crate::Digest::hash(b"root")));
    for scope in [
        PinScope::Snapshot {
            generation: u64::MAX,
        },
        PinScope::Closures(BTreeSet::from([root])),
    ] {
        let pin = DataPin {
            scope,
            catalog: Some(b"post-prune catalog".to_vec()),
            resources: BTreeSet::new(),
        };
        let token = store.register(pin.clone()).await.unwrap().unwrap();
        assert_eq!(store.inventory().await.unwrap().pins[&token], pin);
        let mut conflicting = pin;
        conflicting.resources = resources("pack");
        assert!(store.register(conflicting).await.unwrap().is_none());
        store.release(&token).await.unwrap();
    }
    let revision = store.inventory().await.unwrap().revision;
    assert!(store.begin_prune(revision).await.unwrap().is_none());
    store.finish_deletions(&deletion).await.unwrap();
    assert!(
        store
            .register(DataPin {
                scope: PinScope::Snapshot {
                    generation: u64::MAX
                },
                catalog: None,
                resources: BTreeSet::new()
            })
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn released_pin_cannot_be_extended_or_release_another_pin() {
    let store = MemoryPinStore::default();
    let old = store.register(staging("same")).await.unwrap().unwrap();
    let current = store.register(staging("same")).await.unwrap().unwrap();
    store.release(&old).await.unwrap();
    let revision = store.inventory().await.unwrap().revision;
    store.release(&old).await.unwrap();
    assert_eq!(store.inventory().await.unwrap().revision, revision);
    assert!(store.protect(&old, resources("other")).await.is_err());
    assert!(
        store
            .claim_deletions(revision, resources("same"))
            .await
            .unwrap()
            .is_none()
    );
    store.release(&current).await.unwrap();
    let revision = store.inventory().await.unwrap().revision;
    assert!(
        store
            .claim_deletions(revision, resources("same"))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn identities_in_different_storage_namespaces_do_not_alias() {
    let store = MemoryPinStore::default();
    let digest = crate::Digest::hash(b"same digest");
    let blob = BTreeSet::from([PinResource::Blob(BlobId::new(digest))]);
    let chunk = BTreeSet::from([PinResource::Chunk(ChunkId::new(digest))]);
    let pin = store
        .register(DataPin {
            scope: PinScope::Staging,
            catalog: None,
            resources: blob.clone(),
        })
        .await
        .unwrap()
        .unwrap();
    let revision = store.inventory().await.unwrap().revision;
    assert!(
        store
            .claim_deletions(revision, blob)
            .await
            .unwrap()
            .is_none()
    );
    let deleting = store
        .claim_deletions(revision, chunk.clone())
        .await
        .unwrap()
        .unwrap();
    assert!(!store.protect(&pin, chunk.clone()).await.unwrap());
    store.finish_deletions(&deleting).await.unwrap();
    assert!(store.protect(&pin, chunk).await.unwrap());
}

#[tokio::test]
async fn revision_exhaustion_never_partially_changes_protection() {
    let store = MemoryPinStore::default();
    let pin = store.register(staging("protected")).await.unwrap().unwrap();
    store.state.lock().await.revision = u64::MAX;
    let before = store.inventory().await.unwrap();
    assert!(store.release(&pin).await.is_err());
    assert!(store.protect(&pin, resources("new")).await.is_err());
    assert!(store.register(staging("new")).await.is_err());
    assert!(store.begin_prune(u64::MAX).await.is_err());
    assert!(
        store
            .claim_deletions(u64::MAX, resources("garbage"))
            .await
            .is_err()
    );
    assert_eq!(before, store.inventory().await.unwrap());
}

async fn persistent_contract(writer: &dyn PinStore, collector: &dyn PinStore) {
    let before = collector.inventory().await.unwrap().revision;
    let pin = writer.register(staging("live")).await.unwrap().unwrap();
    assert!(
        collector
            .claim_deletions(before, resources("garbage"))
            .await
            .unwrap()
            .is_none()
    );
    let revision = collector.inventory().await.unwrap().revision;
    let deletion = collector
        .claim_deletions(revision, resources("garbage"))
        .await
        .unwrap()
        .unwrap();
    assert!(!writer.protect(&pin, resources("garbage")).await.unwrap());
    assert!(
        writer
            .protect(&pin, resources("independent"))
            .await
            .unwrap()
    );
    assert!(
        collector.inventory().await.unwrap().pins[&pin]
            .resources
            .contains(&PinResource::StorageObject("independent".into()))
    );
    collector.finish_deletions(&deletion).await.unwrap();
    assert!(writer.protect(&pin, resources("garbage")).await.unwrap());
    writer.release(&pin).await.unwrap();
    let revision = collector.inventory().await.unwrap().revision;
    let pruning = collector.begin_prune(revision).await.unwrap().unwrap();
    assert!(writer.register(staging("new")).await.unwrap().is_none());
    collector.finish_prune(&pruning).await.unwrap();
    assert!(writer.register(staging("new")).await.unwrap().is_some());
}

#[tokio::test]
async fn independent_object_store_clients_share_arbitration() {
    let objects = Arc::new(object_store::memory::InMemory::new());
    let path = object_store::path::Path::from("pins");
    persistent_contract(
        &ObjectPinStore::new(objects.clone(), path.clone()),
        &ObjectPinStore::new(objects, path),
    )
    .await;
}

#[tokio::test]
async fn independent_file_clients_share_arbitration() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pins");
    persistent_contract(&FilePinStore::new(&path), &FilePinStore::new(&path)).await;
}

#[tokio::test]
async fn object_store_cas_loser_rechecks_the_winning_pin() {
    use object_store::{ObjectStore, ObjectStoreExt};
    let objects = Arc::new(object_store::memory::InMemory::new());
    let path = object_store::path::Path::from("pins");
    // Conditional creation must reject a second writer; an implementation
    // that silently falls back to overwrite would lose the first pin.
    let original = ObjectPinStore::new(objects.clone(), path.clone());
    let first = original.register(staging("first")).await.unwrap().unwrap();
    let (a, b) = tokio::join!(
        original.register(staging("second")),
        original.register(staging("third"))
    );
    let a = a.unwrap().unwrap();
    let b = b.unwrap().unwrap();
    let pins = original.inventory().await.unwrap().pins;
    assert!(pins.contains_key(&first) && pins.contains_key(&a) && pins.contains_key(&b));
    let mut bytes = objects
        .get(&path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec();
    bytes[8] ^= 1;
    objects
        .put_opts(&path, bytes.into(), Default::default())
        .await
        .unwrap();
    assert!(original.inventory().await.is_err());
    assert!(
        original
            .register(staging("must-not-reset-corruption"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn persisted_deletion_survives_owner_drop_and_requires_exact_token() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pins");
    let owner = FilePinStore::new(&path);
    let deletion = owner
        .claim_deletions(0, resources("same"))
        .await
        .unwrap()
        .unwrap();
    drop(owner);
    let reopened = FilePinStore::new(&path);
    assert!(reopened.register(staging("same")).await.unwrap().is_none());
    assert!(reopened.register(staging("other")).await.unwrap().is_some());
    reopened
        .finish_deletions(&PinToken::fresh().unwrap())
        .await
        .unwrap();
    assert!(reopened.register(staging("same")).await.unwrap().is_none());
    let parsed: PinToken = deletion.to_string().parse().unwrap();
    assert_eq!(parsed, deletion);
    reopened.finish_deletions(&parsed).await.unwrap();
    assert!(reopened.register(staging("same")).await.unwrap().is_some());
}

#[tokio::test]
async fn codec_roundtrip_and_all_truncations_fail_closed() {
    let memory = MemoryPinStore::default();
    let digest = crate::Digest::hash(b"root");
    memory
        .register(DataPin {
            scope: PinScope::Closures(BTreeSet::from([ObjectKey::blob(BlobId::new(digest))])),
            catalog: Some(b"catalog".to_vec()),
            resources: BTreeSet::from([
                PinResource::Blob(BlobId::new(digest)),
                PinResource::Chunk(ChunkId::new(digest)),
                PinResource::StorageObject("pack/path".into()),
                PinResource::MetadataObject("state/shard".into()),
            ]),
        })
        .await
        .unwrap()
        .unwrap();
    let revision = memory.inventory().await.unwrap().revision;
    memory
        .claim_deletions(revision, resources("unrelated"))
        .await
        .unwrap()
        .unwrap();
    let inventory = memory.inventory().await.unwrap();
    let encoded = codec::encode(&inventory).unwrap();
    assert_eq!(codec::decode(&encoded).unwrap(), inventory);
    for size in 0..encoded.len() {
        assert!(codec::decode(&encoded[..size]).is_err());
    }
    let mut invalid = encoded;
    invalid[8] ^= 1;
    assert!(codec::decode(&invalid).is_err());
    for token in ["", "00", &"g".repeat(64), &"0".repeat(66)] {
        assert!(token.parse::<PinToken>().is_err());
    }
}

#[tokio::test]
async fn metadata_handles_share_online_pins_without_logical_commits() {
    use crate::metadata::{MemoryMetadataStore, MetadataStore, TursoMetadataStore};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("metadata.sqlite");
    let memory = MemoryMetadataStore::new().unwrap();
    let local = TursoMetadataStore::open(&path).await.unwrap();
    let reopened = TursoMetadataStore::open(directory.path().join(".").join("metadata.sqlite"))
        .await
        .unwrap();
    let pairs: Vec<(Arc<dyn MetadataStore>, Arc<dyn MetadataStore>)> = vec![
        (Arc::new(memory.clone()), Arc::new(memory)),
        (Arc::new(local), Arc::new(reopened)),
    ];
    for (first, second) in pairs {
        let revision = first.snapshot().await.unwrap().revision();
        let writer = first.pin_store().await.unwrap();
        let pin = writer.register(staging("shared")).await.unwrap().unwrap();
        let collector = second.pin_store().await.unwrap();
        assert!(collector.inventory().await.unwrap().pins.contains_key(&pin));
        assert_eq!(second.snapshot().await.unwrap().revision(), revision);
        collector.release(&pin).await.unwrap();
        assert!(writer.inventory().await.unwrap().pins.is_empty());
    }
}

#[tokio::test]
async fn recovery_prune_requires_every_exact_claim_and_keeps_admission_closed() {
    let directory = tempfile::tempdir().unwrap();
    let stores: Vec<Arc<dyn PinStore>> = vec![
        Arc::new(MemoryPinStore::default()),
        Arc::new(FilePinStore::new(directory.path().join("pins"))),
    ];
    for store in stores {
        let first = store
            .claim_deletions(0, resources("first"))
            .await
            .unwrap()
            .unwrap();
        let before_second = store.inventory().await.unwrap().revision;
        let second = store
            .claim_deletions(before_second, resources("second"))
            .await
            .unwrap()
            .unwrap();
        let inventory = store.inventory().await.unwrap();
        let claims = BTreeSet::from([first.clone(), second.clone()]);
        assert!(
            store
                .begin_prune(inventory.revision)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .begin_prune_recovering(before_second, claims.clone())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .begin_prune_recovering(inventory.revision, BTreeSet::from([first.clone()]))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .begin_prune_recovering(
                    inventory.revision,
                    BTreeSet::from([first.clone(), PinToken::fresh().unwrap()])
                )
                .await
                .unwrap()
                .is_none()
        );
        let fence = store
            .begin_prune_recovering(inventory.revision, claims)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            store.inventory().await.unwrap().deletions,
            inventory.deletions
        );
        let read = DataPin {
            scope: PinScope::Snapshot {
                generation: u64::MAX,
            },
            catalog: None,
            resources: BTreeSet::new(),
        };
        assert!(store.register(read.clone()).await.unwrap().is_none());
        store.finish_prune(&fence).await.unwrap();
        let reader = store.register(read).await.unwrap().unwrap();
        assert_eq!(store.inventory().await.unwrap().deletions.len(), 2);
        store.finish_deletions(&first).await.unwrap();
        assert!(store.inventory().await.unwrap().pins.contains_key(&reader));
        store.finish_deletions(&second).await.unwrap();
        store.release(&reader).await.unwrap();
    }
}

#[tokio::test]
async fn collector_history_keeps_released_writes_live_until_the_pass_finishes() {
    let directory = tempfile::tempdir().unwrap();
    let stores: Vec<Arc<dyn PinStore>> = vec![
        Arc::new(MemoryPinStore::default()),
        Arc::new(FilePinStore::new(directory.path().join("history"))),
        Arc::new(ObjectPinStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            "history".into(),
        )),
    ];
    for store in stores {
        let live = store
            .register(staging("live-reader-input"))
            .await
            .unwrap()
            .unwrap();
        let initial = store.inventory().await.unwrap();
        let collector = store
            .begin_collection(initial.revision, None)
            .await
            .unwrap()
            .unwrap();
        let writer = store
            .register(staging("published-during-gc"))
            .await
            .unwrap()
            .unwrap();
        store.release(&writer).await.unwrap();
        let history = store.inventory().await.unwrap();
        assert!(history.pins.contains_key(&writer));
        assert!(history.retired.contains(&writer));
        assert!(
            store
                .protect(&writer, resources("new-write"))
                .await
                .is_err()
        );
        assert!(
            store
                .claim_deletions(history.revision, resources("published-during-gc"))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .begin_collection(history.revision, None)
                .await
                .unwrap()
                .is_none()
        );
        store.release(&writer).await.unwrap();
        assert_eq!(store.inventory().await.unwrap().revision, history.revision);
        // Exact-token recovery replaces collector ownership without forgetting
        // writes that completed under its predecessor.
        let resumed = store
            .begin_collection(history.revision, Some(collector.clone()))
            .await
            .unwrap()
            .unwrap();
        assert_ne!(resumed, collector);
        store.finish_collection(&collector).await.unwrap();
        assert!(store.inventory().await.unwrap().retired.contains(&writer));
        let garbage = store
            .claim_deletions(
                store.inventory().await.unwrap().revision,
                resources("unrelated-garbage"),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(store.finish_collection(&resumed).await.is_err());
        store.finish_deletions(&garbage).await.unwrap();
        store.finish_collection(&resumed).await.unwrap();
        let finished = store.inventory().await.unwrap();
        assert!(finished.collector.is_none());
        assert!(finished.retired.is_empty());
        assert!(finished.pins.contains_key(&live));
        assert!(!finished.pins.contains_key(&writer));
        store.release(&live).await.unwrap();
    }
}

#[tokio::test]
async fn metadata_pins_enter_prune_without_admitting_payloads_or_claimed_files() {
    let directory = tempfile::tempdir().unwrap();
    let stores: Vec<Arc<dyn PinStore>> = vec![
        Arc::new(MemoryPinStore::default()),
        Arc::new(FilePinStore::new(directory.path().join("metadata"))),
        Arc::new(ObjectPinStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            "metadata".into(),
        )),
    ];
    let metadata = |path: &str| DataPin {
        scope: PinScope::Metadata,
        catalog: None,
        resources: BTreeSet::from([PinResource::MetadataObject(path.into())]),
    };
    for store in stores {
        let collector = store.begin_collection(0, None).await.unwrap().unwrap();
        let claim = store
            .claim_deletions(
                store.inventory().await.unwrap().revision,
                metadata("deleting").resources,
            )
            .await
            .unwrap()
            .unwrap();
        let prune = store
            .begin_prune_recovering(
                store.inventory().await.unwrap().revision,
                BTreeSet::from([claim.clone()]),
            )
            .await
            .unwrap()
            .unwrap();
        let before = store.inventory().await.unwrap();
        assert!(
            store
                .register(metadata("deleting"))
                .await
                .unwrap()
                .is_none()
        );
        assert!(store.register(staging("writer")).await.unwrap().is_none());
        let pin = store
            .register(metadata("checkpoint"))
            .await
            .unwrap()
            .unwrap();
        assert!(
            store
                .protect(&pin, metadata("root-shard").resources)
                .await
                .unwrap()
        );
        assert!(
            !store
                .protect(&pin, metadata("deleting").resources)
                .await
                .unwrap()
        );
        assert!(store.protect(&pin, resources("payload")).await.is_err());
        for invalid in [
            DataPin {
                scope: PinScope::Metadata,
                ..staging("payload")
            },
            DataPin {
                catalog: Some(b"payload catalog".to_vec()),
                ..metadata("checkpoint")
            },
        ] {
            assert!(store.register(invalid).await.is_err());
        }
        let during = store.inventory().await.unwrap();
        assert!(during.same_payload_pins(&before));
        assert_eq!(
            codec::decode(&codec::encode(&during).unwrap()).unwrap(),
            during
        );
        store.release(&pin).await.unwrap();
        assert!(store.inventory().await.unwrap().same_payload_pins(&before));
        store.finish_deletions(&claim).await.unwrap();
        store.finish_prune(&prune).await.unwrap();
        let writer = store.register(staging("writer")).await.unwrap().unwrap();
        assert!(!store.inventory().await.unwrap().same_payload_pins(&before));
        store.release(&writer).await.unwrap();
        store.finish_collection(&collector).await.unwrap();
        assert!(store.inventory().await.unwrap().pins.is_empty());
    }
}

#[tokio::test]
async fn persisted_metadata_scope_cannot_hide_payload_resources() {
    let store = MemoryPinStore::default();
    store.register(staging("payload")).await.unwrap().unwrap();
    let mut bytes = codec::encode(&store.inventory().await.unwrap()).unwrap();
    // Header, revision, count and token precede the first scope byte.
    bytes[8 + 8 + 4 + 32] = 3;
    let checksum_offset = bytes.len() - 32;
    let checksum = blake3::hash(&bytes[..checksum_offset]);
    bytes[checksum_offset..].copy_from_slice(checksum.as_bytes());
    assert!(codec::decode(&bytes).is_err());
}

#[test]
fn snapshot_generation_codec_round_trips_and_preserves_legacy_protection() {
    let token = PinToken::fresh().unwrap();
    let inventory = PinInventory {
        pins: BTreeMap::from([(
            token.clone(),
            DataPin {
                scope: PinScope::Snapshot { generation: 42 },
                catalog: None,
                resources: BTreeSet::new(),
            },
        )]),
        ..PinInventory::default()
    };
    let bytes = codec::encode(&inventory).unwrap();
    assert_eq!(codec::decode(&bytes).unwrap(), inventory);
    // The previous format stored the unit Snapshot tag at this same position.
    // Such durable pins must keep conservative protection until exact release.
    let tag = 8 + 8 + 4 + 32;
    let mut legacy = bytes[..bytes.len() - 32].to_vec();
    legacy[..8].copy_from_slice(b"CASPIN02");
    legacy[tag] = 0;
    legacy.drain(tag + 1..tag + 9);
    let checksum = blake3::hash(&legacy);
    legacy.extend_from_slice(checksum.as_bytes());
    let decoded = codec::decode(&legacy).unwrap();
    assert_eq!(
        decoded.pins[&token].scope,
        PinScope::Snapshot {
            generation: u64::MAX
        }
    );
}

fn token(byte: u8) -> PinToken {
    PinToken([byte; 32])
}

/// A legal durable write: revision 4 to 5, one pin on "pinned" and one
/// deletion claim on "garbage". Each bug arm breaks it in one way.
fn legal_write() -> (PinInventory, PinInventory) {
    let prev = PinInventory {
        revision: 4,
        ..PinInventory::default()
    };
    let next = PinInventory {
        revision: 5,
        pins: BTreeMap::from([(token(1), staging("pinned"))]),
        deletions: BTreeMap::from([(token(2), resources("garbage"))]),
        ..PinInventory::default()
    };
    (prev, next)
}

#[test]
fn inventory_successor_refuses_illegal_writes() {
    let (prev, next) = legal_write();
    assert_eq!(
        validate_inventory_successor(&prev, &next, RevisionStep::One),
        Ok(())
    );
    let mut skipped = next.clone();
    skipped.revision = 6;
    assert!(validate_inventory_successor(&prev, &skipped, RevisionStep::One).is_err());
    assert_eq!(
        validate_inventory_successor(&prev, &skipped, RevisionStep::Forward),
        Ok(())
    );
    let mut repeated = next.clone();
    repeated.revision = prev.revision;
    assert!(validate_inventory_successor(&prev, &repeated, RevisionStep::Forward).is_err());
    type Break = fn(&mut PinInventory);
    let breaks: [(&str, Break); 8] = [
        ("claim reuses a pin token", |next| {
            next.deletions.insert(token(1), resources("other"));
        }),
        ("collector reuses a claim token", |next| {
            next.collector = Some(token(2))
        }),
        ("logical prune reuses the collector token", |next| {
            next.collector = Some(token(3));
            next.logical_prune = Some(token(3));
        }),
        ("reader owner reuses a pin token", |next| {
            next.reader_owners.insert(token(1));
        }),
        ("retired token is not pinned", |next| {
            next.collector = Some(token(3));
            next.retired.insert(token(4));
        }),
        ("retired pin without a collector", |next| {
            next.retired.insert(token(1));
        }),
        ("claim covers a pinned resource", |next| {
            next.deletions.insert(token(3), resources("pinned"));
        }),
        ("two claims cover one resource", |next| {
            next.deletions.insert(token(3), resources("garbage"));
        }),
    ];
    for (case, break_write) in breaks {
        let mut broken = next.clone();
        break_write(&mut broken);
        assert!(
            validate_inventory_successor(&prev, &broken, RevisionStep::One).is_err(),
            "{case}"
        );
    }
}

#[test]
fn inventory_changes_are_checked_against_unchanged_records() {
    let (_, next) = legal_write();
    let mut grown = next.clone();
    grown
        .pins
        .get_mut(&token(1))
        .unwrap()
        .resources
        .extend(resources("garbage"));
    assert!(validate_inventory_records(&grown, [&token(1)], []).is_err());
    let mut claimed = next.clone();
    claimed
        .deletions
        .get_mut(&token(2))
        .unwrap()
        .extend(resources("pinned"));
    assert!(validate_inventory_records(&claimed, [], [&token(2)]).is_err());
    let mut reused = next.clone();
    reused.pins.insert(token(2), staging("fresh"));
    assert!(validate_inventory_records(&reused, [&token(2)], []).is_err());
}
