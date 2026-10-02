#![cfg(feature = "experimental")]

//! Public-API coverage for the repository lifecycle.

#![cfg(feature = "native")]

use casita::experimental::{
    BlobStore as _, ClosureStatus, DestinationRoot, MetadataStore as _, ObjectRequest, Repository,
    RootName, TransferOptions, TransferRequest, transfer,
};

#[tokio::test]
async fn filesystem_sync_checkout_and_collection_share_one_repository_model() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input");
    std::fs::create_dir_all(input.join("nested")).unwrap();
    std::fs::write(input.join("hello.txt"), b"repository bytes").unwrap();
    std::fs::write(input.join("nested/data"), b"more bytes").unwrap();

    let source = Repository::local(temp.path().join("source")).await.unwrap();
    let source_name = RootName::try_from("releases/current").unwrap();
    let root = source
        .import(casita::import::FilesystemImport::new(
            &input,
            source_name.clone(),
        ))
        .await
        .unwrap();
    assert!(matches!(
        source.verify_closure(&root).await.unwrap(),
        ClosureStatus::Complete { .. }
    ));

    let destination = Repository::local(temp.path().join("destination"))
        .await
        .unwrap();
    let destination_name = RootName::try_from("mirrors/current").unwrap();
    let result = transfer(
        &source,
        &destination,
        TransferRequest {
            objects: vec![ObjectRequest {
                key: root.clone(),
                recursive: true,
            }],
            roots: vec![DestinationRoot {
                name: destination_name.clone(),
                target: root.clone(),
            }],
        },
        TransferOptions::default(),
    )
    .await
    .unwrap();
    assert!(result.progress.published_objects > 0);

    let checkout = temp.path().join("checkout");
    destination.checkout(&root, &checkout).await.unwrap();
    assert_eq!(
        std::fs::read(checkout.join("hello.txt")).unwrap(),
        b"repository bytes"
    );

    destination
        .remove_root_if_matches(&destination_name, &root)
        .await
        .unwrap()
        .expect("the synchronized root still matches");
    let outcome = destination.collect().await.unwrap();
    assert!(outcome.removed.logical_objects > 0);
    assert!(
        destination
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .object(&root)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn collection_on_a_second_handle_preserves_an_online_staged_publication() {
    let temp = tempfile::tempdir().unwrap();
    let repository_root = temp.path().join("repository");
    let writer = Repository::local(&repository_root).await.unwrap();
    let collector = Repository::local(&repository_root).await.unwrap();

    let mutation = writer.mutation_session().await.unwrap();
    let staged = mutation
        .stage_blob(b"durable before the root commit")
        .await
        .unwrap();
    let key = staged.record().key().clone();
    let payload = staged.record().payload();

    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), collector.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.removed.logical_objects, 0);
    assert!(writer.payloads().has(&payload).await.unwrap());

    mutation
        .publish_rooted(
            vec![staged],
            RootName::try_from("concurrent/current").unwrap(),
            key.clone(),
        )
        .await
        .unwrap();
    drop(mutation);

    let outcome = collector.collect().await.unwrap();
    assert_eq!(outcome.removed.logical_objects, 0);
    let snapshot = collector.metadata().snapshot().await.unwrap();
    assert_eq!(
        snapshot
            .root(&RootName::try_from("concurrent/current").unwrap())
            .await
            .unwrap(),
        Some(key)
    );
    assert!(collector.payloads().has(&payload).await.unwrap());
}

/// Re-importing a tree recognizes files it has already read, and that shortcut
/// must never survive the content changing underneath it.
///
/// The adversarial case is a rewrite that restores the modification time: only
/// `ctime`, which userspace cannot set, distinguishes it from an untouched
/// file.
#[tokio::test]
async fn an_import_recognizes_unchanged_files_and_never_stale_ones() {
    let temp = tempfile::tempdir().unwrap();
    let tree = temp.path().join("tree");
    std::fs::create_dir_all(&tree).unwrap();
    std::fs::write(tree.join("steady.txt"), b"unchanging").unwrap();
    std::fs::write(tree.join("rewritten.txt"), b"first-copy").unwrap();

    let repository = Repository::local(temp.path().join("repository"))
        .await
        .unwrap();
    let first = repository
        .import(casita::import::FilesystemImport::new(
            &tree,
            RootName::try_from("trees/first").unwrap(),
        ))
        .await
        .unwrap();

    // Nothing moved, so the same tree resolves to the same root.
    let unchanged = repository
        .import(casita::import::FilesystemImport::new(
            &tree,
            RootName::try_from("trees/unchanged").unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(first, unchanged);

    // Same length, and the modification time put back exactly where it was.
    let stat = std::fs::metadata(tree.join("rewritten.txt")).unwrap();
    let when = stat.modified().unwrap();
    std::fs::write(tree.join("rewritten.txt"), b"second-copy").unwrap();
    // Windows only sets file times through a handle opened for writing.
    std::fs::File::options()
        .write(true)
        .open(tree.join("rewritten.txt"))
        .unwrap()
        .set_modified(when)
        .unwrap();

    let rewritten = repository
        .import(casita::import::FilesystemImport::new(
            &tree,
            RootName::try_from("trees/rewritten").unwrap(),
        ))
        .await
        .unwrap();
    assert_ne!(
        first, rewritten,
        "a rewrite hidden behind a restored modification time must still be read"
    );

    // And the graph the shortcut produced is a real, complete one.
    assert!(matches!(
        repository.verify_closure(&rewritten).await.unwrap(),
        ClosureStatus::Complete { .. }
    ));
}

#[tokio::test]
async fn a_completed_filesystem_import_compacts_its_state_log() {
    let temp = tempfile::tempdir().unwrap();
    let tree = temp.path().join("tree");
    let repository_root = temp.path().join("repository");
    std::fs::create_dir_all(tree.join("nested")).unwrap();
    for index in 0..64 {
        std::fs::write(
            tree.join("nested").join(format!("file-{index}")),
            format!("contents-{index}"),
        )
        .unwrap();
    }

    let repository = Repository::local(&repository_root).await.unwrap();
    let root = repository
        .import(casita::import::FilesystemImport::new(
            &tree,
            RootName::try_from("trees/compacted").unwrap(),
        ))
        .await
        .unwrap();

    let log = repository_root.join("casita.sqlite-wal");
    assert_eq!(
        std::fs::metadata(log)
            .map(|metadata| metadata.len())
            .unwrap_or(0),
        0,
        "a successful filesystem import should leave no retained WAL history"
    );
    assert!(matches!(
        repository.verify_closure(&root).await.unwrap(),
        ClosureStatus::Complete { .. }
    ));
}

#[tokio::test]
async fn online_local_writer_process() {
    let Some(root) = std::env::var_os("CASITA_TEST_ONLINE_WRITER_ROOT") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let repository = Repository::local(&root).await.unwrap();
    let mutation = repository.mutation_session().await.unwrap();
    let staged = mutation
        .stage_blob(b"held by another process")
        .await
        .unwrap();
    let key = staged.record().key().clone();
    // Make the unpublished pack visible to the rival collector's physical
    // inventory; passing only with bytes in the writer's buffer is insufficient.
    repository.payloads().flush().await.unwrap();
    std::fs::write(root.join("writer-ready"), b"ready").unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while !root.join("writer-publish").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    mutation
        .publish_rooted(
            vec![staged],
            RootName::try_from("online/writer").unwrap(),
            key.clone(),
        )
        .await
        .unwrap();
    drop(mutation);
    assert!(matches!(
        repository.verify_closure(&key).await.unwrap(),
        ClosureStatus::Complete { objects: 1 }
    ));
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
}

#[tokio::test]
async fn local_collection_reclaims_unrelated_data_while_another_process_stages() {
    let directory = tempfile::tempdir().unwrap();
    let repository = Repository::local(directory.path()).await.unwrap();
    let mutation = repository.mutation_session().await.unwrap();
    let garbage = mutation
        .stage_blob(b"collect while the writer remains online")
        .await
        .unwrap();
    let garbage_key = garbage.record().key().clone();
    mutation.publish_unrooted(vec![garbage]).await.unwrap();
    drop(mutation);
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();

    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args(["online_local_writer_process", "--exact", "--nocapture"])
        .env("CASITA_TEST_ONLINE_WRITER_ROOT", directory.path())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while !directory.path().join("writer-ready").exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "writer exited before staging"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), repository.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.removed.logical_objects, 1);
    assert!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .object(&garbage_key)
            .await
            .unwrap()
            .is_none()
    );
    std::fs::write(directory.path().join("writer-publish"), b"publish").unwrap();
    let status = tokio::time::timeout(std::time::Duration::from_secs(20), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success());
    let key = repository
        .metadata()
        .snapshot()
        .await
        .unwrap()
        .root(&RootName::try_from("online/writer").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        repository.verify_closure(&key).await.unwrap(),
        ClosureStatus::Complete { objects: 1 }
    ));
}
