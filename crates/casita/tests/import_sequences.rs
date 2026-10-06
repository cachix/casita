//! The same request vocabulary on repositories and existing sessions.
#![cfg(feature = "native")]

use casita::{
    ObjectKey, Repository, RootName,
    import::{BlobImport, FilesystemImport, ImportSequence},
};
use std::io::Cursor;

fn name(value: &str) -> RootName {
    value.parse().unwrap()
}

#[tokio::test]
async fn sequences_publish_in_order_and_keep_successes_on_failure() {
    let repository = Repository::memory().unwrap();
    let request = ImportSequence::new(
        [b"first".as_slice(), b"second".as_slice()]
            .map(|bytes| BlobImport::new(Cursor::new(bytes), name("replaced"))),
    );
    let future = repository.import(request);
    fn send<T: Send>(value: T) -> T {
        value
    }
    let keys = send(future).await.unwrap();
    assert_eq!(keys.len(), 2);
    assert_ne!(keys[0], keys[1]);
    assert_eq!(
        repository.root(&name("replaced")).await.unwrap(),
        Some(keys[1].clone())
    );

    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("payload"), b"payload").unwrap();
    let request = ImportSequence::new([
        FilesystemImport::new(directory.path(), name("first")),
        FilesystemImport::new(directory.path().join("missing"), name("missing")),
        FilesystemImport::new(directory.path(), name("last")),
    ]);
    assert!(repository.import(request).await.is_err());
    assert!(repository.root(&name("first")).await.unwrap().is_some());
    assert!(repository.root(&name("last")).await.unwrap().is_none());
    repository.collect().await.unwrap();
    assert!(repository.fsck().await.unwrap().is_clean());
}

#[tokio::test]
async fn sequences_and_atomic_batches_reuse_an_existing_import_session() {
    let directory = tempfile::tempdir().unwrap();
    let repository = Repository::local(directory.path()).await.unwrap();
    let session = repository.import_session().await.unwrap();
    let first = session
        .import(BlobImport::new(Cursor::new(b"first"), name("first")))
        .await
        .unwrap();
    let sequence = session
        .import(ImportSequence::new([
            BlobImport::new(Cursor::new(b"second".as_slice()), name("second")),
            BlobImport::new(Cursor::new(b"third".as_slice()), name("third")),
        ]))
        .await
        .unwrap();
    let atomic = session
        .import(BlobImport::batch([
            BlobImport::new(Cursor::new(b"fourth".as_slice()), name("fourth")),
            BlobImport::new(Cursor::new(b"fifth".as_slice()), name("fifth")),
        ]))
        .await
        .unwrap();
    drop(session);
    repository.flush().await.unwrap();
    repository.collect().await.unwrap();
    for key in std::iter::once(first).chain(sequence).chain(atomic) {
        assert!(repository.object(&key).await.unwrap().is_some());
    }
    assert!(repository.fsck().await.unwrap().is_clean());
}

#[tokio::test]
async fn empty_import_sequences_leave_the_revision_unchanged() {
    let repository = Repository::memory().unwrap();
    let before = repository.metadata_reader().await.unwrap().revision();
    let inputs: Vec<BlobImport<Cursor<Vec<u8>>>> = Vec::new();
    let result: Vec<ObjectKey> = repository
        .import(ImportSequence::new(inputs))
        .await
        .unwrap();
    assert!(result.is_empty());
    assert_eq!(
        repository.metadata_reader().await.unwrap().revision(),
        before
    );
}

#[cfg(feature = "experimental")]
#[tokio::test]
async fn the_same_requests_work_on_experimental_repositories_and_sessions() {
    use casita::experimental::Repository;
    let repository = Repository::memory().unwrap();
    let keys = repository
        .import(ImportSequence::new([
            BlobImport::new(Cursor::new(b"first".as_slice()), name("first")),
            BlobImport::new(Cursor::new(b"second".as_slice()), name("second")),
        ]))
        .await
        .unwrap();
    assert_eq!(keys.len(), 2);
    let session = repository.mutation_session().await.unwrap();
    let keys = session
        .import(BlobImport::batch([
            BlobImport::new(Cursor::new(b"third".as_slice()), name("third")),
            BlobImport::new(Cursor::new(b"fourth".as_slice()), name("fourth")),
        ]))
        .await
        .unwrap();
    assert_eq!(keys.len(), 2);
    let keys = session
        .import(ImportSequence::new([BlobImport::new(
            Cursor::new(b"fifth".as_slice()),
            name("fifth"),
        )]))
        .await
        .unwrap();
    assert_eq!(keys.len(), 1);
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("file"), b"payload").unwrap();
    let keys = session
        .import(ImportSequence::new([
            casita::import::UnrootedFilesystemImport::new(directory.path()),
        ]))
        .await
        .unwrap();
    assert_eq!(keys.len(), 1);
}
