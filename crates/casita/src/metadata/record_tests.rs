use crate::{
    MetadataChange as Change, MetadataCheck as Check, MetadataCommitResult as Outcome, MetadataKey,
    NamespaceId, Repository, RootName,
};
use bytes::Bytes;

fn key(value: impl Into<Bytes>) -> MetadataKey {
    MetadataKey::new(NamespaceId::try_from("obrador.v1").unwrap(), value)
}
fn set(name: &str, value: &str) -> Change {
    Change::Set {
        key: key(name.to_owned()),
        value: Bytes::from(value.to_owned()),
    }
}
async fn committed(repo: &Repository, checks: Vec<Check>, changes: Vec<Change>) {
    assert!(matches!(
        repo.commit(checks, changes).await.unwrap(),
        Outcome::Committed { .. }
    ));
}

#[tokio::test]
async fn metadata_records_roundtrip_checks_and_stable_prefix_pages() {
    let directory = tempfile::tempdir().unwrap();
    for repo in [
        Repository::memory().unwrap(),
        Repository::local(directory.path()).await.unwrap(),
    ] {
        let other = MetadataKey::new(NamespaceId::try_from("another.v1").unwrap(), "paths/a");
        committed(
            &repo,
            vec![Check::Record {
                key: key("paths/a"),
                expected: None,
            }],
            vec![
                set("paths/a", "first"),
                set("paths/b", "second"),
                set("paths/c", "third"),
                set("paths0/not-a-match", "neighbor"),
                set("paths/empty", ""),
                Change::Set {
                    key: other.clone(),
                    value: "other namespace".into(),
                },
            ],
        )
        .await;
        let reader = repo.metadata_reader().await.unwrap();
        let keys = vec![
            key("paths/b"),
            key("absent"),
            key("paths/a"),
            key("paths/b"),
            key("paths/empty"),
            other,
        ];
        assert_eq!(
            reader.get(&keys).await.unwrap(),
            vec![
                Some("second".into()),
                None,
                Some("first".into()),
                Some("second".into()),
                Some(Bytes::new()),
                Some("other namespace".into())
            ]
        );
        assert_eq!(
            repo.get(&keys).await.unwrap(),
            reader.get(&keys).await.unwrap()
        );
        assert!(repo.get(&[]).await.unwrap().is_empty());
        assert!(reader.get(&[]).await.unwrap().is_empty());
        let prefix = key("paths/");
        let first = reader.scan(&prefix, None, 1).await.unwrap();
        assert_eq!(first.records[0].key, key("paths/a"));
        let cursor = first.cursor.unwrap();
        committed(
            &repo,
            vec![Check::Record {
                key: key("paths/a"),
                expected: Some("first".into()),
            }],
            vec![
                set("paths/a", "updated"),
                Change::Delete {
                    key: key("paths/b"),
                },
            ],
        )
        .await;
        assert_eq!(
            reader.get(&[key("paths/a")]).await.unwrap(),
            vec![Some("first".into())]
        );
        assert!(repo.scan(&prefix, Some(&cursor), 1).await.is_err());
        assert!(reader.scan(&key("path"), Some(&cursor), 1).await.is_err());
        let mut names = vec![key("paths/a")];
        let mut cursor = Some(cursor);
        while let Some(current) = cursor {
            let page = reader.scan(&prefix, Some(&current), 1).await.unwrap();
            assert!(!page.records.is_empty());
            names.extend(page.records.into_iter().map(|r| r.key));
            cursor = page.cursor;
        }
        assert_eq!(
            names,
            vec![
                key("paths/a"),
                key("paths/b"),
                key("paths/c"),
                key("paths/empty")
            ]
        );
        assert!(
            reader
                .scan(&key("absent/"), None, 16)
                .await
                .unwrap()
                .records
                .is_empty()
        );
        assert!(reader.scan(&prefix, None, 0).await.is_err());
        assert!(reader.scan(&prefix, None, 1025).await.is_err());
        drop(reader);
        for mismatch in [0, 1] {
            let mut checks = vec![
                Check::Record {
                    key: key("paths/a"),
                    expected: Some("updated".into()),
                },
                Check::Record {
                    key: key("paths/c"),
                    expected: Some("third".into()),
                },
            ];
            checks[mismatch] = Check::Record {
                key: key("paths/a"),
                expected: None,
            };
            let revision = repo.metadata_reader().await.unwrap().revision();
            assert_eq!(
                repo.commit(
                    checks,
                    vec![
                        set("paths/new", "must not appear"),
                        Change::Delete {
                            key: key("paths/a")
                        }
                    ]
                )
                .await
                .unwrap(),
                Outcome::Conflict {
                    check_index: mismatch
                }
            );
            assert_eq!(repo.metadata_reader().await.unwrap().revision(), revision);
            assert_eq!(
                repo.get(&[key("paths/new"), key("paths/a")]).await.unwrap(),
                vec![None, Some("updated".into())]
            );
        }
        assert!(
            repo.commit(
                vec![],
                vec![
                    set("duplicate", "a"),
                    Change::Delete {
                        key: key("duplicate")
                    }
                ]
            )
            .await
            .is_err()
        );
    }
    let reopened = Repository::local(directory.path()).await.unwrap();
    assert_eq!(
        reopened
            .get(&[key("paths/a"), key("paths/b")])
            .await
            .unwrap(),
        vec![Some("updated".into()), None]
    );
}

#[tokio::test]
async fn metadata_records_binary_prefix_bounds() {
    let directory = tempfile::tempdir().unwrap();
    for repo in [
        Repository::memory().unwrap(),
        Repository::local(directory.path()).await.unwrap(),
    ] {
        let keys: Vec<_> = [
            vec![],
            vec![0],
            vec![0, 255],
            vec![1],
            vec![255],
            vec![255, 0],
            vec![255, 255],
        ]
        .into_iter()
        .map(key)
        .collect();
        committed(
            &repo,
            vec![],
            keys.iter()
                .map(|key| Change::Set {
                    key: key.clone(),
                    value: key.key.clone(),
                })
                .collect(),
        )
        .await;
        for prefix in [vec![], vec![0], vec![255], vec![255, 255], vec![254]] {
            let reader = repo.metadata_reader().await.unwrap();
            let mut cursor = None;
            let mut actual = Vec::new();
            loop {
                let page = reader
                    .scan(&key(prefix.clone()), cursor.as_ref(), 1)
                    .await
                    .unwrap();
                actual.extend(page.records.into_iter().map(|r| r.key));
                cursor = page.cursor;
                if cursor.is_none() {
                    break;
                }
            }
            assert_eq!(
                actual,
                keys.iter()
                    .filter(|key| key.key.starts_with(&prefix))
                    .cloned()
                    .collect::<Vec<_>>()
            );
        }
    }
}

#[tokio::test]
async fn metadata_records_and_roots_are_atomic_but_gc_is_distinct() {
    let directory = tempfile::tempdir().unwrap();
    let repo = Repository::local(directory.path()).await.unwrap();
    let staging = RootName::try_from("staging/path").unwrap();
    let root = RootName::try_from("roots/path").unwrap();
    let object = repo
        .import(crate::import::BlobImport::new(
            std::io::Cursor::new(b"payload"),
            staging.clone(),
        ))
        .await
        .unwrap();
    let descriptor = format!("{object}");
    committed(
        &repo,
        vec![Check::Root {
            name: staging.clone(),
            expected: Some(object.clone()),
        }],
        vec![
            set("paths/hash", &descriptor),
            set("referrers/target/hash", "hash"),
            Change::RemoveRoot { name: staging },
            Change::SetRoot {
                name: root.clone(),
                target: object.clone(),
            },
        ],
    )
    .await;
    assert_eq!(repo.collect().await.unwrap().logical_objects, 0);
    assert!(repo.object(&object).await.unwrap().is_some());
    assert_eq!(
        repo.commit(
            vec![Check::Root {
                name: root.clone(),
                expected: None
            }],
            vec![
                set("paths/hash", "bad"),
                Change::RemoveRoot { name: root.clone() }
            ]
        )
        .await
        .unwrap(),
        Outcome::Conflict { check_index: 0 }
    );
    assert_eq!(repo.root(&root).await.unwrap(), Some(object.clone()));
    assert_eq!(
        repo.get(&[key("paths/hash")]).await.unwrap(),
        vec![Some(descriptor.clone().into())]
    );
    // A failed root validation must also roll back application records.
    let absent = crate::ObjectKey::blob(crate::BlobId::new(crate::Digest::hash(b"absent")));
    assert!(
        repo.commit(
            vec![],
            vec![
                set("paths/failure", "bad"),
                Change::SetRoot {
                    name: root.clone(),
                    target: absent
                }
            ]
        )
        .await
        .is_err()
    );
    assert_eq!(repo.get(&[key("paths/failure")]).await.unwrap(), vec![None]);
    committed(
        &repo,
        vec![Check::Root {
            name: root.clone(),
            expected: Some(object.clone()),
        }],
        vec![Change::RemoveRoot { name: root }],
    )
    .await;
    let report = repo.collect().await.unwrap();
    assert_eq!(report.logical_objects, 1);
    assert!(repo.object(&object).await.unwrap().is_none());
    assert!(repo.open(&object).await.unwrap().is_none());
    assert_eq!(
        repo.get(&[key("paths/hash")]).await.unwrap(),
        vec![Some(descriptor.into())]
    );
    drop(repo);
    let repo = Repository::local(directory.path()).await.unwrap();
    assert!(repo.get(&[key("paths/hash")]).await.unwrap()[0].is_some());
    assert!(repo.roots().await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg_attr(
    windows,
    ignore = "concurrent handles deadlock on the Turso write lock: https://github.com/cachix/casita/issues/33"
)]
async fn metadata_records_concurrent_handles_conflict_only_on_checked_values() {
    use std::sync::Arc;
    let directory = tempfile::tempdir().unwrap();
    let first = Repository::local(directory.path()).await.unwrap();
    let second = Repository::local(directory.path()).await.unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let mut tasks = Vec::new();
    for (id, repo) in [first, second].into_iter().enumerate() {
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let outcome = repo
                .commit(
                    vec![Check::Record {
                        key: key("shared"),
                        expected: None,
                    }],
                    vec![
                        set("shared", &id.to_string()),
                        set(&format!("winner/{id}"), "yes"),
                    ],
                )
                .await
                .unwrap();
            for i in 0..20 {
                let name = format!("independent/{id}/{i}");
                committed(
                    &repo,
                    vec![Check::Record {
                        key: key(name.clone()),
                        expected: None,
                    }],
                    vec![set(&name, "ok")],
                )
                .await;
            }
            outcome
        }));
    }
    let mut won = 0;
    for task in tasks {
        if matches!(task.await.unwrap(), Outcome::Committed { .. }) {
            won += 1;
        }
    }
    assert_eq!(won, 1);
    let repo = Repository::local(directory.path()).await.unwrap();
    assert_eq!(
        repo.scan(&key("winner/"), None, 16)
            .await
            .unwrap()
            .records
            .len(),
        1
    );
    assert_eq!(
        repo.scan(&key("independent/"), None, 64)
            .await
            .unwrap()
            .records
            .len(),
        40
    );
}

#[tokio::test]
async fn metadata_records_result_budgets_preserve_pagination() {
    let directory = tempfile::tempdir().unwrap();
    for repo in [
        Repository::memory().unwrap(),
        Repository::local(directory.path()).await.unwrap(),
    ] {
        let value = Bytes::from(vec![0; 1024 * 1024]);
        for first in [0, 8, 16] {
            committed(
                &repo,
                vec![],
                (first..(first + 8).min(17))
                    .map(|i| Change::Set {
                        key: key(format!("large/{i:02}")),
                        value: value.clone(),
                    })
                    .collect(),
            )
            .await;
        }
        let reader = repo.metadata_reader().await.unwrap();
        let first = reader.scan(&key("large/"), None, 1024).await.unwrap();
        assert_eq!(first.records.len(), 15);
        let second = reader
            .scan(&key("large/"), first.cursor.as_ref(), 1024)
            .await
            .unwrap();
        assert_eq!(second.records.len(), 2);
        assert!(second.cursor.is_none());
        assert_eq!(second.records[0].key, key("large/15"));
        assert!(repo.get(&vec![key("large/00"); 17]).await.is_err());
        assert_eq!(
            repo.get(&vec![key("large/00"); 16]).await.unwrap().len(),
            16
        );
        assert!(reader.get(&vec![key("large/00"); 17]).await.is_err());
        assert_eq!(
            reader.get(&vec![key("large/00"); 16]).await.unwrap().len(),
            16
        );
        assert!(
            repo.commit(
                vec![],
                vec![Change::Set {
                    key: key("too-large"),
                    value: vec![0; 1024 * 1024 + 1].into()
                }]
            )
            .await
            .is_err()
        );
        assert!(repo.get(&[key(vec![0; 4097])]).await.is_err());
        assert!(repo.get(&vec![key("missing"); 4097]).await.is_err());
    }
}

#[tokio::test]
async fn metadata_records_unvalidated_roots_use_full_graph_verification() {
    use crate::metadata::MetadataStore;
    let directory = tempfile::tempdir().unwrap();
    let repo = Repository::local(directory.path()).await.unwrap();
    let session = repo.inner.mutation_session().await.unwrap();
    let object = session.stage_blob(b"unrooted").await.unwrap();
    let target = object.record().key().clone();
    let invalid_directory = crate::Directory::try_from_iter([(
        crate::PathComponent::try_from("file").unwrap(),
        crate::Node::File {
            digest: crate::BlobId::new(target.native_digest().unwrap()),
            size: 999,
            executable: false,
        },
    )])
    .unwrap();
    let tree = session.stage_directory(&invalid_directory).await.unwrap();
    let bad_target = tree.record().key().clone();
    session.publish_unrooted(vec![object, tree]).await.unwrap();
    let snapshot = repo.inner.metadata().snapshot().await.unwrap();
    assert_eq!(
        snapshot
            .validated_closures(std::slice::from_ref(&target))
            .await
            .unwrap(),
        vec![false]
    );
    drop(snapshot);
    // Metadata graph reachability alone would accept this directory. The
    // fallback must still verify its incorrect child payload-size declaration.
    assert!(
        repo.commit(
            vec![],
            vec![
                set("paths/invalid", "bad"),
                Change::SetRoot {
                    name: RootName::try_from("invalid").unwrap(),
                    target: bad_target
                }
            ]
        )
        .await
        .is_err()
    );
    assert_eq!(repo.get(&[key("paths/invalid")]).await.unwrap(), vec![None]);
    committed(
        &repo,
        vec![],
        vec![
            set("paths/valid", "ok"),
            Change::SetRoot {
                name: RootName::try_from("valid").unwrap(),
                target: target.clone(),
            },
        ],
    )
    .await;
    assert_eq!(
        repo.inner
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .validated_closures(&[target])
            .await
            .unwrap(),
        vec![true]
    );
    drop(session);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_records_verified_root_fast_path_races_collection() {
    let directory = tempfile::tempdir().unwrap();
    let repo = Repository::local(directory.path()).await.unwrap();
    let target = repo
        .import(crate::import::BlobImport::new(
            std::io::Cursor::new(b"keep alive"),
            RootName::try_from("live/0").unwrap(),
        ))
        .await
        .unwrap();
    let writer = repo.clone();
    let collector = Repository::local(directory.path()).await.unwrap();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let barrier_gc = barrier.clone();
    let gc = tokio::spawn(async move {
        barrier_gc.wait().await;
        for _ in 0..12 {
            assert_eq!(collector.collect().await.unwrap().logical_objects, 0);
            tokio::task::yield_now().await;
        }
    });
    let retained = target.clone();
    let write = tokio::spawn(async move {
        barrier.wait().await;
        for i in 0..24 {
            let old = RootName::try_from(format!("live/{i}").as_str()).unwrap();
            let new = RootName::try_from(format!("live/{}", i + 1).as_str()).unwrap();
            committed(
                &writer,
                vec![Check::Root {
                    name: old.clone(),
                    expected: Some(retained.clone()),
                }],
                vec![
                    Change::RemoveRoot { name: old },
                    Change::SetRoot {
                        name: new,
                        target: retained.clone(),
                    },
                    set("paths/live", &(i + 1).to_string()),
                ],
            )
            .await;
        }
    });
    write.await.unwrap();
    gc.await.unwrap();
    let reader = repo.metadata_reader().await.unwrap();
    assert_eq!(
        reader
            .root(&RootName::try_from("live/24").unwrap())
            .await
            .unwrap(),
        Some(target.clone())
    );
    assert_eq!(
        reader.get(&[key("paths/live")]).await.unwrap(),
        vec![Some("24".into())]
    );
    drop(reader);
    assert!(repo.open(&target).await.unwrap().is_some());
}

#[tokio::test]
async fn metadata_records_rootless_witness_cannot_bypass_emergency_fence() {
    use crate::metadata::{MetadataError, MetadataMutation, MetadataStore};
    let directory = tempfile::tempdir().unwrap();
    for repo in [
        Repository::memory().unwrap(),
        Repository::local(directory.path()).await.unwrap(),
    ] {
        let root = RootName::try_from("temporary").unwrap();
        let target = repo
            .import(crate::import::BlobImport::new(
                std::io::Cursor::new(b"verified but now rootless"),
                root.clone(),
            ))
            .await
            .unwrap();
        committed(&repo, vec![], vec![Change::RemoveRoot { name: root }]).await;
        repo.flush().await.unwrap();
        let state = repo.inner.metadata();
        assert_eq!(
            state
                .snapshot()
                .await
                .unwrap()
                .validated_closures(std::slice::from_ref(&target))
                .await
                .unwrap(),
            vec![true]
        );
        let changes = vec![
            set("paths/reuse", "must wait"),
            Change::SetRoot {
                name: RootName::try_from("reuse").unwrap(),
                target,
            },
        ];
        let mut fast = MetadataMutation::with_metadata(vec![], changes.clone()).unwrap();
        fast.require_validated_roots = true;
        assert!(matches!(
            state.commit_checked(fast).await,
            Err(MetadataError::RootVerificationRequired)
        ));
        let ledger = state.pin_store().await.unwrap();
        let collector = ledger
            .begin_collection(ledger.inventory().await.unwrap().revision, None)
            .await
            .unwrap()
            .unwrap();
        let fence = ledger
            .begin_prune(ledger.inventory().await.unwrap().revision)
            .await
            .unwrap()
            .unwrap();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(50),
                repo.commit(vec![], changes.clone())
            )
            .await
            .is_err()
        );
        assert_eq!(repo.get(&[key("paths/reuse")]).await.unwrap(), vec![None]);
        ledger.finish_prune(&fence).await.unwrap();
        ledger.finish_collection(&collector).await.unwrap();
        committed(&repo, vec![], changes).await;
    }
}

#[tokio::test]
async fn metadata_records_large_snapshot_survives_collection() {
    let directory = tempfile::tempdir().unwrap();
    let repo = Repository::local(directory.path()).await.unwrap();
    for first in (0..8192).step_by(1024) {
        committed(
            &repo,
            vec![],
            (first..first + 1024)
                .map(|i| Change::Set {
                    key: key(format!("paths/{i:05}")),
                    value: vec![b'x'; 256].into(),
                })
                .collect(),
        )
        .await;
    }
    let target = repo
        .import(crate::import::BlobImport::new(
            std::io::Cursor::new(b"live"),
            RootName::try_from("live").unwrap(),
        ))
        .await
        .unwrap();
    let reader = repo.metadata_reader().await.unwrap();
    committed(&repo, vec![], vec![set("paths/00000", "new")]).await;
    assert_eq!(repo.collect().await.unwrap().logical_objects, 0);
    assert_eq!(
        reader.get(&[key("paths/00000")]).await.unwrap(),
        vec![Some(vec![b'x'; 256].into())]
    );
    assert_eq!(
        reader
            .scan(&key("paths/"), None, 1024)
            .await
            .unwrap()
            .records
            .len(),
        1024
    );
    drop(reader);
    assert_eq!(
        repo.get(&[key("paths/00000")]).await.unwrap(),
        vec![Some("new".into())]
    );
    assert!(repo.open(&target).await.unwrap().is_some());
}
