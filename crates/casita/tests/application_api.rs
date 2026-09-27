//! Workflows using only the default, non-generic application API.
#![cfg(feature = "native")]

use std::error::Error as _;
use std::io::SeekFrom;
use std::time::Duration;

use casita::{
    BlobId, Digest, ErrorKind, MetadataChange, MetadataCheck, MetadataCommitResult, MetadataKey,
    ObjectKey, Repository, RootName,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

fn name(value: &str) -> RootName {
    value.try_into().unwrap()
}

#[tokio::test]
async fn root_prefix_reads_exact_descendants_and_retained_revision() {
    let directory = tempfile::tempdir().unwrap();
    for repository in [
        Repository::memory().unwrap(),
        Repository::local(directory.path()).await.unwrap(),
    ] {
        let target = repository
            .import(casita::import::BlobImport::new(
                &b"payload"[..],
                name("payload"),
            ))
            .await
            .unwrap();
        let mut changes = vec![MetadataChange::SetRoot {
            name: name("refs/开发"),
            target: target.clone(),
        }];
        for index in 0..300 {
            changes.push(MetadataChange::SetRoot {
                name: name(&format!("refs/开发/{index:03}")),
                target: target.clone(),
            });
        }
        changes.push(MetadataChange::SetRoot {
            name: name("refs/开发x/other"),
            target: target.clone(),
        });
        repository.commit(Vec::new(), changes).await.unwrap();

        let held = repository.retained_reader().await.unwrap();
        let prefix = name("refs/开发");
        let roots = held.roots_under(&prefix).await.unwrap();
        assert_eq!(roots.len(), 301);
        assert_eq!(roots.first().unwrap().name(), &prefix);
        assert_eq!(roots.last().unwrap().name(), &name("refs/开发/299"));
        assert_eq!(repository.roots_under(&prefix).await.unwrap(), roots);

        repository.remove_root(&prefix, &target).await.unwrap();
        assert_eq!(held.roots_under(&prefix).await.unwrap(), roots);
        assert_eq!(repository.roots_under(&prefix).await.unwrap().len(), 300);
    }
}

async fn flush_with_live_snapshot(repository: &Repository, local: bool) {
    let result = repository.flush().await;
    if local {
        assert_eq!(result.unwrap_err().kind(), ErrorKind::Busy);
    } else {
        result.unwrap();
    }
}

async fn publish_streaming_batch(repository: &Repository) -> Vec<ObjectKey> {
    let mut keys = Vec::new();
    for index in 0..48u8 {
        keys.push(
            repository
                .import(casita::import::BlobImport::new(
                    &vec![index; 4096][..],
                    name(&format!("stream/{index}")),
                ))
                .await
                .unwrap(),
        );
    }
    keys
}

fn wal_bytes(root: &std::path::Path) -> u64 {
    std::fs::metadata(root.join("casita.sqlite-wal"))
        .map(|m| m.len())
        .unwrap_or(0)
}

fn payload_files(
    root: &std::path::Path,
) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    fn visit(
        path: &std::path::Path,
        files: &mut std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>,
    ) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(&path, files);
            } else {
                files.insert(path.clone(), std::fs::read(&path).unwrap());
            }
        }
    }
    let mut files = std::collections::BTreeMap::new();
    visit(&root.join("blobs"), &mut files);
    files
}

#[tokio::test]
async fn flush_reclaims_streaming_publication_wal_without_collecting_payloads() {
    let directory = tempfile::tempdir().unwrap();
    let repository = Repository::local(directory.path()).await.unwrap();
    let reader = repository.metadata_reader().await.unwrap();
    let keys = publish_streaming_batch(&repository).await;
    let before = wal_bytes(directory.path());
    assert!(
        before > 64 * 1024,
        "streaming publications should grow the WAL: {before}"
    );
    drop(reader);
    let files = payload_files(directory.path());
    repository.flush().await.unwrap();
    let after = wal_bytes(directory.path());
    assert!(after <= 4096, "flush left {after} WAL bytes from {before}");
    assert_eq!(payload_files(directory.path()), files);
    for key in keys {
        assert!(repository.object(&key).await.unwrap().is_some());
    }
}

#[tokio::test]
async fn flush_waits_or_reports_busy_until_a_live_snapshot_is_released() {
    let directory = tempfile::tempdir().unwrap();
    let repository = Repository::local(directory.path()).await.unwrap();
    let reader = repository.metadata_reader().await.unwrap();
    let revision = reader.revision();
    publish_streaming_batch(&repository).await;
    let other = Repository::local(directory.path()).await.unwrap();
    let flushing = tokio::spawn(async move { other.flush().await });
    let mut flushing = flushing;
    tokio::select! {
        result = &mut flushing => {
            let error = result.unwrap().expect_err("a live old snapshot must prevent WAL truncation");
            assert_eq!(error.kind(), ErrorKind::Busy);
            assert_eq!(error.retry_disposition(), casita::RetryDisposition::Retry);
        }
        _ = tokio::time::sleep(Duration::from_millis(100)) => {
            assert_eq!(reader.revision(), revision);
            assert!(reader.roots().await.unwrap().is_empty());
            assert!(wal_bytes(directory.path()) > 64 * 1024);
            drop(reader);
            // A backend may finish waiting with either success or a retryable busy result.
            if let Err(error) = tokio::time::timeout(Duration::from_secs(40), flushing).await.unwrap().unwrap() {
                assert_eq!(error.kind(), ErrorKind::Busy);
            }
            repository.flush().await.unwrap();
            assert!(wal_bytes(directory.path()) <= 4096);
            return;
        }
    }
    assert_eq!(reader.revision(), revision);
    assert!(reader.roots().await.unwrap().is_empty());
    assert!(wal_bytes(directory.path()) > 64 * 1024);
    drop(reader);
    repository.flush().await.unwrap();
    assert!(wal_bytes(directory.path()) <= 4096);
}

#[tokio::test]
async fn writes_publish_verified_content_without_preparation() {
    async fn verify(repository: &Repository, key: &ObjectKey, expected: &[u8]) {
        let mut reader = repository.open_verified(key).await.unwrap().unwrap();
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.unwrap();
        assert_eq!(actual, expected);
    }

    for size in [16384, 16385, 65537] {
        let work = tempfile::tempdir().unwrap();
        let bytes: Vec<_> = (0..size).map(|i| (i * 31) as u8).collect();
        let source = Repository::local(work.path().join("blob")).await.unwrap();
        let key = source
            .import(casita::import::BlobImport::new(
                bytes.as_slice(),
                name("blob"),
            ))
            .await
            .unwrap();
        verify(&source, &key, &bytes).await;

        let archive = source.export_casitar(&key, Vec::new()).await.unwrap();
        let restored = Repository::local(work.path().join("casitar"))
            .await
            .unwrap();
        restored
            .import(casita::import::CasitarImport::new(
                archive.as_slice(),
                [name("restored")],
            ))
            .await
            .unwrap();
        verify(&restored, &key, &bytes).await;

        let copied = Repository::local(work.path().join("copy")).await.unwrap();
        copied
            .import(casita::import::CopyImport::new(
                &source,
                name("blob"),
                name("copy"),
            ))
            .await
            .unwrap();
        verify(&copied, &key, &bytes).await;

        let files = work.path().join("files");
        std::fs::create_dir(&files).unwrap();
        std::fs::write(files.join("file"), &bytes).unwrap();
        let filesystem = Repository::local(work.path().join("filesystem"))
            .await
            .unwrap();
        let root = filesystem
            .import(casita::import::FilesystemImport::new(&files, name("tree")))
            .await
            .unwrap();
        let child = filesystem.object(&root).await.unwrap().unwrap().links()[0].clone();
        verify(&filesystem, &child, &bytes).await;

        let mut tar = tokio_tar::Builder::new(Vec::new());
        let mut header = tokio_tar::Header::new_ustar();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        tar.append_data(&mut header, "file", bytes.as_slice())
            .await
            .unwrap();
        let archive = tar.into_inner().await.unwrap();
        let destination = Repository::local(work.path().join("tar")).await.unwrap();
        let report = destination
            .import(casita::import::TarImport::new(
                archive.as_slice(),
                name("tree"),
            ))
            .await
            .unwrap();
        let child = destination
            .object(&report.root)
            .await
            .unwrap()
            .unwrap()
            .links()[0]
            .clone();
        verify(&destination, &child, &bytes).await;

        let updated = source
            .overwrite_blob(&key, 1, b"updated", name("updated"))
            .await
            .unwrap();
        let mut expected = bytes;
        expected[1..8].copy_from_slice(b"updated");
        verify(&source, &updated, &expected).await;
    }
}

#[tokio::test]
async fn built_in_workflows_publish_copy_archive_and_restore_a_tree() {
    let work = tempfile::tempdir().unwrap();
    let source = work.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("hello"), b"application API").unwrap();
    // The type annotation is deliberately free of backend parameters.
    let repository: Repository = Repository::local(work.path().join("repository"))
        .await
        .unwrap();
    let key = repository
        .import(casita::import::FilesystemImport::new(
            &source,
            name("source"),
        ))
        .await
        .unwrap();
    assert_eq!(
        repository.root(&name("source")).await.unwrap(),
        Some(key.clone())
    );
    assert_eq!(repository.object(&key).await.unwrap().unwrap().key(), &key);

    let mirror = Repository::memory().unwrap();
    mirror
        .import(casita::import::BlobImport::new(
            &b"old destination"[..],
            name("copy"),
        ))
        .await
        .unwrap();
    assert_eq!(
        mirror
            .import(casita::import::CopyImport::new(
                &repository,
                name("source"),
                name("copy")
            ))
            .await
            .unwrap(),
        key
    );
    assert_eq!(mirror.root(&name("copy")).await.unwrap(), Some(key.clone()));
    let archive = mirror.export_casitar(&key, Vec::new()).await.unwrap();
    let restored = Repository::memory().unwrap();
    assert_eq!(
        restored
            .import(casita::import::CasitarImport::new(
                &archive[..],
                [name("restored")]
            ))
            .await
            .unwrap()
            .mappings[0]
            .root,
        key
    );
    restored
        .checkout(&key, work.path().join("checkout"))
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(work.path().join("checkout/hello")).unwrap(),
        b"application API"
    );
    assert_eq!(restored.roots().await.unwrap()[0].target(), &key);
    assert!(restored.fsck().await.unwrap().is_clean());

    let duplicate = restored
        .import(casita::import::CasitarImport::new(
            &archive[..],
            [name("restored")],
        ))
        .await
        .unwrap_err();
    assert_eq!(duplicate.kind(), ErrorKind::DestinationConflict);
    assert!(duplicate.source().is_some());
    assert!(
        restored
            .import(casita::import::CasitarImport::new(
                &archive[..archive.len() - 1],
                [name("truncated")]
            ))
            .await
            .is_err()
    );
    assert_eq!(restored.root(&name("truncated")).await.unwrap(), None);

    assert!(restored.remove_root(&name("restored"), &key).await.unwrap());
    assert!(restored.preview_collection().await.unwrap().logical_objects > 0);
    assert!(restored.collect().await.unwrap().logical_objects > 0);
    assert_eq!(restored.object(&key).await.unwrap(), None);
    restored.flush().await.unwrap();
}

#[tokio::test]
async fn streaming_reads_retain_data_across_local_handles_and_handle_drop() {
    let directory = tempfile::tempdir().unwrap();
    let writer = Repository::local(directory.path()).await.unwrap();
    let expected = b"retained payload";
    let key = writer
        .import(casita::import::BlobImport::new(&expected[..], name("blob")))
        .await
        .unwrap();
    assert_eq!(key, ObjectKey::blob(BlobId::new(Digest::hash(expected))));
    let collector = Repository::local(directory.path()).await.unwrap();
    let mut reader = writer.open(&key).await.unwrap().unwrap();
    assert_eq!(reader.record().payload_size(), expected.len() as u64);
    drop(writer);
    assert!(collector.remove_root(&name("blob"), &key).await.unwrap());
    let collection = tokio::time::timeout(Duration::from_secs(5), collector.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(collection.logical_objects, 0);
    reader.seek(SeekFrom::Start(9)).await.unwrap();
    let mut tail = Vec::new();
    reader.read_to_end(&mut tail).await.unwrap();
    assert_eq!(tail, b"payload");
    drop(reader);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), collector.collect())
            .await
            .unwrap()
            .unwrap()
            .logical_objects
            > 0
    );
    assert!(collector.open(&key).await.unwrap().is_none());
}

#[tokio::test]
async fn root_updates_validate_closures_and_compare_before_removing() {
    let repository = Repository::memory().unwrap();
    let first = repository
        .import(casita::import::BlobImport::new(
            &b"first"[..],
            name("current"),
        ))
        .await
        .unwrap();
    let second = repository
        .import(casita::import::BlobImport::new(
            &b"second"[..],
            name("current"),
        ))
        .await
        .unwrap();
    assert!(
        !repository
            .remove_root(&name("current"), &first)
            .await
            .unwrap()
    );
    assert_eq!(
        repository.root(&name("current")).await.unwrap(),
        Some(second)
    );
    repository
        .set_root(name("retained"), first.clone())
        .await
        .unwrap();
    assert_eq!(
        repository.root(&name("retained")).await.unwrap(),
        Some(first)
    );
    let missing = ObjectKey::blob(BlobId::new(Digest::hash(b"missing")));
    assert!(repository.set_root(name("invalid"), missing).await.is_err());
    assert_eq!(repository.root(&name("invalid")).await.unwrap(), None);
    assert!(
        repository
            .import(casita::import::TarImport::new(
                &b"not a tar archive"[..],
                name("invalid-tar")
            ))
            .await
            .is_err()
    );
    assert_eq!(repository.root(&name("invalid-tar")).await.unwrap(), None);
}

#[tokio::test]
async fn conditional_roots_create_replace_and_leave_rejected_updates_unchanged() {
    let repository = Repository::memory().unwrap();
    let first = repository
        .import(casita::import::BlobImport::new(
            &b"first"[..],
            name("staging/first"),
        ))
        .await
        .unwrap();
    let second = repository
        .import(casita::import::BlobImport::new(
            &b"second"[..],
            name("staging/second"),
        ))
        .await
        .unwrap();
    assert!(
        !repository
            .compare_and_set_root(name("current"), Some(&first), second.clone())
            .await
            .unwrap()
    );
    assert_eq!(repository.root(&name("current")).await.unwrap(), None);
    assert!(
        repository
            .compare_and_set_root(name("current"), None, first.clone())
            .await
            .unwrap()
    );
    assert!(
        !repository
            .compare_and_set_root(name("current"), None, second.clone())
            .await
            .unwrap()
    );
    assert!(
        !repository
            .compare_and_set_root(name("current"), Some(&second), second.clone())
            .await
            .unwrap()
    );
    assert_eq!(
        repository.root(&name("current")).await.unwrap(),
        Some(first.clone())
    );

    let missing = ObjectKey::blob(BlobId::new(Digest::hash(b"missing")));
    assert!(
        repository
            .compare_and_set_root(name("current"), Some(&first), missing.clone())
            .await
            .is_err()
    );
    assert!(
        repository
            .compare_and_set_root(name("absent"), None, missing)
            .await
            .is_err()
    );
    assert_eq!(repository.root(&name("absent")).await.unwrap(), None);
    assert_eq!(
        repository.root(&name("current")).await.unwrap(),
        Some(first.clone())
    );

    assert!(
        repository
            .compare_and_set_root(name("current"), Some(&first), second.clone())
            .await
            .unwrap()
    );
    assert!(
        !repository
            .compare_and_set_root(name("current"), Some(&first), first.clone())
            .await
            .unwrap()
    );
    assert_eq!(
        repository.root(&name("current")).await.unwrap(),
        Some(second.clone())
    );
    assert!(
        repository
            .remove_root(&name("staging/first"), &first)
            .await
            .unwrap()
    );
    assert!(
        repository
            .remove_root(&name("staging/second"), &second)
            .await
            .unwrap()
    );
    repository.collect().await.unwrap();
    assert!(repository.object(&first).await.unwrap().is_none());
    assert!(repository.object(&second).await.unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_root_replacement_has_one_winner_across_local_handles() {
    let directory = tempfile::tempdir().unwrap();
    let repository = Repository::local(directory.path()).await.unwrap();
    let original = repository
        .import(casita::import::BlobImport::new(
            &b"original"[..],
            name("current"),
        ))
        .await
        .unwrap();
    repository
        .set_root(name("staging/original"), original.clone())
        .await
        .unwrap();
    let first = repository
        .import(casita::import::BlobImport::new(
            &b"first"[..],
            name("staging/first"),
        ))
        .await
        .unwrap();
    let second = repository
        .import(casita::import::BlobImport::new(
            &b"second"[..],
            name("staging/second"),
        ))
        .await
        .unwrap();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let mut writers = Vec::new();
    for target in [first, second] {
        let writer = Repository::local(directory.path()).await.unwrap();
        let barrier = barrier.clone();
        let original = original.clone();
        writers.push(tokio::spawn(async move {
            barrier.wait().await;
            let committed = writer
                .compare_and_set_root(name("current"), Some(&original), target.clone())
                .await
                .unwrap();
            (committed, target)
        }));
    }
    let mut winner = None;
    for writer in writers {
        let (committed, target) = writer.await.unwrap();
        if committed {
            assert!(winner.replace(target).is_none(), "both writers committed");
        }
    }
    assert!(winner.is_some(), "neither writer committed");
    assert_eq!(repository.root(&name("current")).await.unwrap(), winner);
    let integrity = repository.fsck().await.unwrap();
    assert!(integrity.is_clean(), "{integrity:?}");
}

#[tokio::test]
async fn root_only_reads_stay_consistent_during_replacement_and_collection() {
    for local in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let reader = if local {
            Repository::local(directory.path()).await.unwrap()
        } else {
            Repository::memory().unwrap()
        };
        let first = reader
            .import(casita::import::BlobImport::new(
                &b"first"[..],
                name("keep/first"),
            ))
            .await
            .unwrap();
        let second = reader
            .import(casita::import::BlobImport::new(
                &b"second"[..],
                name("keep/second"),
            ))
            .await
            .unwrap();
        reader.set_root(name("left"), first.clone()).await.unwrap();
        reader.set_root(name("right"), first.clone()).await.unwrap();
        let writer = if local {
            Repository::local(directory.path()).await.unwrap()
        } else {
            reader.clone()
        };

        let writes = async {
            let mut garbage = Vec::new();
            for index in 0..4u8 {
                let key = writer
                    .import(casita::import::BlobImport::new(
                        &[index][..],
                        name("garbage"),
                    ))
                    .await
                    .unwrap();
                assert!(writer.remove_root(&name("garbage"), &key).await.unwrap());
                garbage.push(key);
                let target = if index % 2 == 0 { &second } else { &first };
                writer
                    .commit(
                        Vec::new(),
                        vec![
                            MetadataChange::SetRoot {
                                name: name("left"),
                                target: target.clone(),
                            },
                            MetadataChange::SetRoot {
                                name: name("right"),
                                target: target.clone(),
                            },
                        ],
                    )
                    .await
                    .unwrap();
                writer.collect().await.unwrap();
                tokio::task::yield_now().await;
            }
            garbage
        };
        let reads = async {
            for _ in 0..32 {
                let key = reader.root(&name("left")).await.unwrap().unwrap();
                assert!(key == first || key == second);
                let roots = reader.roots().await.unwrap();
                let left = roots
                    .iter()
                    .find(|root| root.name() == &name("left"))
                    .unwrap();
                let right = roots
                    .iter()
                    .find(|root| root.name() == &name("right"))
                    .unwrap();
                assert_eq!(
                    left.target(),
                    right.target(),
                    "root listing mixed revisions"
                );
                assert!(reader.root(&name("missing")).await.unwrap().is_none());
                tokio::task::yield_now().await;
            }
        };
        let (garbage, ()) = tokio::time::timeout(Duration::from_secs(30), async {
            tokio::join!(writes, reads)
        })
        .await
        .expect("root reads and online collection must complete");
        writer.flush().await.unwrap();
        writer.collect().await.unwrap();
        for key in garbage {
            assert!(reader.object(&key).await.unwrap().is_none());
        }
        assert_eq!(reader.root(&name("left")).await.unwrap(), Some(first));
    }
}

#[tokio::test]
async fn retained_metadata_and_content_share_a_snapshot_across_root_replacement_and_gc() {
    for local in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let repository = if local {
            Repository::local(directory.path()).await.unwrap()
        } else {
            Repository::memory().unwrap()
        };
        let expected = b"content selected through metadata";
        let original = repository
            .import(casita::import::BlobImport::new(
                &expected[..],
                name("current"),
            ))
            .await
            .unwrap();
        let prefix = MetadataKey::new("obrador.v1".parse().unwrap(), "paths/");
        let a = MetadataKey::new(prefix.namespace.clone(), "paths/a");
        let b = MetadataKey::new(prefix.namespace.clone(), "paths/b");
        repository
            .commit(
                Vec::new(),
                vec![
                    MetadataChange::Set {
                        key: a.clone(),
                        value: "original".into(),
                    },
                    MetadataChange::Set {
                        key: b.clone(),
                        value: "second".into(),
                    },
                ],
            )
            .await
            .unwrap();
        let metadata_only = repository.metadata_reader().await.unwrap();
        let original_record = metadata_only.object(&original).await.unwrap().unwrap();
        let original_roots = metadata_only.roots().await.unwrap();
        assert_eq!(original_roots, repository.roots().await.unwrap());
        let retained = repository.retained_reader().await.unwrap();
        assert_eq!(retained.revision(), metadata_only.revision());
        let page = retained.scan(&prefix, None, 1).await.unwrap();
        assert_eq!(page.records[0].key, a);
        let cursor = page.cursor.unwrap();
        // A clone owns the same protection and snapshot, not a new admission.
        let session = retained.clone();
        drop(retained);
        let writer = if local {
            Repository::local(directory.path()).await.unwrap()
        } else {
            repository.clone()
        };
        let newer = writer
            .import(casita::import::BlobImport::new(
                &b"new content"[..],
                name("staging/new"),
            ))
            .await
            .unwrap();
        assert!(matches!(
            writer
                .commit(
                    vec![MetadataCheck::Root {
                        name: name("current"),
                        expected: Some(original.clone())
                    },],
                    vec![
                        MetadataChange::SetRoot {
                            name: name("current"),
                            target: newer.clone()
                        },
                        MetadataChange::Set {
                            key: a.clone(),
                            value: "updated".into()
                        },
                        MetadataChange::Delete { key: b.clone() },
                    ]
                )
                .await
                .unwrap(),
            MetadataCommitResult::Committed { .. }
        ));
        drop(repository);
        flush_with_live_snapshot(&writer, local).await;
        // Collection happens after replacement and before looking up/opening
        // the old root. Protection must already have been admitted.
        assert_eq!(writer.collect().await.unwrap().logical_objects, 0);
        assert_eq!(session.revision(), metadata_only.revision());
        assert_eq!(metadata_only.roots().await.unwrap(), original_roots);
        assert_eq!(
            metadata_only.root(&name("current")).await.unwrap(),
            Some(original.clone())
        );
        let selected = session.root(&name("current")).await.unwrap().unwrap();
        assert_eq!(selected, original);
        assert_eq!(
            writer.root(&name("current")).await.unwrap(),
            Some(newer.clone())
        );
        assert_eq!(
            session.get(&[a.clone(), b.clone()]).await.unwrap(),
            vec![Some("original".into()), Some("second".into())]
        );
        let page = session.scan(&prefix, Some(&cursor), 1).await.unwrap();
        assert_eq!(page.records[0].key, b);
        assert_eq!(page.records[0].value.as_ref(), b"second");
        assert!(page.cursor.is_none());
        let current = writer.retained_reader().await.unwrap();
        assert_eq!(
            current
                .scan(&prefix, Some(&cursor), 1)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
        drop(current);
        assert!(session.object(&newer).await.unwrap().is_none());
        assert!(session.open(&newer).await.unwrap().is_none());
        assert_eq!(
            session.object(&selected).await.unwrap().unwrap().key(),
            &original
        );
        let mut stream = session.open(&selected).await.unwrap().unwrap();
        drop(session);
        flush_with_live_snapshot(&writer, local).await;
        assert_eq!(writer.collect().await.unwrap().logical_objects, 0);
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, expected);
        drop(stream);
        flush_with_live_snapshot(&writer, local).await;
        assert_eq!(writer.collect().await.unwrap().logical_objects, 1);
        assert!(writer.open(&original).await.unwrap().is_none());
        assert!(writer.object(&original).await.unwrap().is_none());
        assert_eq!(
            metadata_only.object(&original).await.unwrap(),
            Some(original_record)
        );
        assert!(metadata_only.object(&newer).await.unwrap().is_none());
        // An ordinary metadata snapshot still shows its old values but does
        // not retain content after the explicitly retained readers are gone.
        assert_eq!(
            metadata_only.get(&[a]).await.unwrap(),
            vec![Some("original".into())]
        );
        drop(metadata_only);
        writer.flush().await.unwrap();
    }
}

/// Garbage is created before opening the reader, so a snapshot-wide pin would
/// retain it. Exercise both in-memory and independent local repository handles.
#[tokio::test]
async fn single_object_reader_allows_collection_of_preexisting_unrelated_garbage() {
    for local in [false, true] {
        let work = tempfile::tempdir().unwrap();
        let path = work.path().join("repo");
        let repository = if local {
            Repository::local(&path).await.unwrap()
        } else {
            Repository::memory().unwrap()
        };
        let collector = if local {
            Repository::local(&path).await.unwrap()
        } else {
            repository.clone()
        };
        let kept = repository
            .import(casita::import::BlobImport::new(
                &b"keep this stream"[..],
                name("kept"),
            ))
            .await
            .unwrap();
        let garbage = repository
            .import(casita::import::BlobImport::new(
                &b"unrelated garbage"[..],
                name("garbage"),
            ))
            .await
            .unwrap();
        repository.remove_root(&name("kept"), &kept).await.unwrap();
        repository
            .remove_root(&name("garbage"), &garbage)
            .await
            .unwrap();
        repository.flush().await.unwrap();
        let mut reader = repository.open(&kept).await.unwrap().unwrap();
        assert_eq!(reader.record().key(), &kept);
        drop(repository);
        collector.flush().await.unwrap();
        let removed = collector.collect().await.unwrap();
        assert_eq!(
            removed.logical_objects, 1,
            "unrelated garbage must be collectible while the reader lives"
        );
        assert!(collector.object(&garbage).await.unwrap().is_none());
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"keep this stream");
        reader.seek(SeekFrom::Start(5)).await.unwrap();
        bytes.clear();
        reader.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"this stream");
        drop(reader);
        collector.flush().await.unwrap();
        assert_eq!(collector.collect().await.unwrap().logical_objects, 1);
        assert!(collector.open(&kept).await.unwrap().is_none());
        collector.flush().await.unwrap();
    }
}

#[tokio::test]
async fn single_object_reader_retains_the_objects_dependency_closure() {
    let work = tempfile::tempdir().unwrap();
    let source = work.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("child"), b"child payload").unwrap();
    let repository = Repository::local(work.path().join("repo")).await.unwrap();
    let root = repository
        .import(casita::import::FilesystemImport::new(&source, name("tree")))
        .await
        .unwrap();
    let garbage = repository
        .import(casita::import::BlobImport::new(
            &b"unrelated"[..],
            name("garbage"),
        ))
        .await
        .unwrap();
    repository.remove_root(&name("tree"), &root).await.unwrap();
    repository
        .remove_root(&name("garbage"), &garbage)
        .await
        .unwrap();
    repository.flush().await.unwrap();
    let reader = repository.open(&root).await.unwrap().unwrap();
    assert_eq!(repository.collect().await.unwrap().logical_objects, 1);
    let restored = work.path().join("restored");
    repository.checkout(&root, &restored).await.unwrap();
    assert_eq!(
        std::fs::read(restored.join("child")).unwrap(),
        b"child payload"
    );
    drop(reader);
    repository.flush().await.unwrap();
    assert_eq!(repository.collect().await.unwrap().logical_objects, 2);
    repository.flush().await.unwrap();
}

/// Both proof formats (flat and paged) must survive GC without retaining
/// unrelated objects. Explicit retained sessions keep their wider contract.
#[tokio::test]
async fn verified_readers_scope_retention_and_survive_gc_before_first_read() {
    for local in [false, true] {
        for (size, retained) in [(8192, false), (3 * 1024 * 1024, false), (8192, true)] {
            let work = tempfile::tempdir().unwrap();
            let repository = if local {
                Repository::local(work.path()).await.unwrap()
            } else {
                Repository::memory().unwrap()
            };
            let expected = vec![19; size];
            let key = repository
                .import(casita::import::BlobImport::new(
                    &expected[..],
                    name("selected"),
                ))
                .await
                .unwrap();
            let garbage = repository
                .import(casita::import::BlobImport::new(
                    &b"unrelated verified garbage"[..],
                    name("garbage"),
                ))
                .await
                .unwrap();
            repository
                .remove_root(&name("selected"), &key)
                .await
                .unwrap();
            repository
                .remove_root(&name("garbage"), &garbage)
                .await
                .unwrap();
            repository.flush().await.unwrap();
            let mut reader = if retained {
                let session = repository.retained_reader().await.unwrap();
                let reader = session.open_verified(&key).await.unwrap().unwrap();
                drop(session);
                reader
            } else {
                repository.open_verified(&key).await.unwrap().unwrap()
            };
            let collector = if local {
                Repository::local(work.path()).await.unwrap()
            } else {
                repository.clone()
            };
            drop(repository);
            flush_with_live_snapshot(&collector, local && retained).await;
            let removed = tokio::time::timeout(Duration::from_secs(30), collector.collect())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(removed.logical_objects, if retained { 0 } else { 1 });
            collector.vacuum().await.unwrap();
            let mut actual = Vec::new();
            reader.read_to_end(&mut actual).await.unwrap();
            assert_eq!(actual, expected);
            flush_with_live_snapshot(&collector, local && retained).await;
            assert_eq!(
                collector.collect().await.unwrap().logical_objects,
                0,
                "successful EOF must retain protection until the reader is dropped"
            );
            drop(reader);
            collector.flush().await.unwrap();
            assert_eq!(
                collector.collect().await.unwrap().logical_objects,
                if retained { 2 } else { 1 }
            );
            collector.flush().await.unwrap();
        }
    }
}
