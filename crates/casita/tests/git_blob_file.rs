#![cfg(all(feature = "native", feature = "git", feature = "experimental"))]

use casita::experimental::{
    GitObjectFormat, GitObjectKind, MemoryBlobStore, MemoryMetadataStore, MetadataStore,
    Repository, RepositoryError,
};

#[path = "support/counting_blob_store.rs"]
mod counting_blob_store;
use counting_blob_store::CountingBlobStore;

#[tokio::test]
async fn git_blob_file_registration_preserves_content_without_payload_reads() {
    use std::sync::atomic::Ordering;
    let payloads = CountingBlobStore::new();
    let (reads, writes) = (payloads.reads.clone(), payloads.writes.clone());
    let repository = Repository::new(payloads, MemoryMetadataStore::new().unwrap());
    let body = b"shared git and plain file bytes";
    let git = casita::experimental::git_object_key_for_body(
        GitObjectFormat::Sha1,
        GitObjectKind::Blob,
        body,
    )
    .unwrap();
    let native_session = repository.mutation_session().await.unwrap();
    let native = native_session
        .stage_object(git.clone(), body)
        .await
        .unwrap();
    native_session.publish_unrooted(vec![native]).await.unwrap();
    let session = repository.mutation_session().await.unwrap();
    let staged = session.stage_git_blob_file(&git).await.unwrap();
    let file = staged.record().key().clone();
    assert_eq!(staged.record().payload_size(), 31);
    session
        .publish_rooted(vec![staged], "file".try_into().unwrap(), file.clone())
        .await
        .unwrap();
    assert_eq!(
        reads.load(Ordering::SeqCst),
        0,
        "neither registration nor rooting should reread verified blob content"
    );
    assert_eq!(
        writes.load(Ordering::SeqCst),
        1,
        "only the original native ingestion writes payload bytes"
    );
    assert!(matches!(
        session.stage_git_blob_file(&file).await,
        Err(RepositoryError::InvalidInput(_))
    ));
    let (_, mut opened) = repository.open_payload(&file).await.unwrap().unwrap();
    let mut bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut opened, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, b"shared git and plain file bytes");
    drop(native_session);
}

/// Permanent workload: benchmark run git-blob-file. Both strategies start
/// with an already durable native blob. Timing covers each strategy's own
/// metadata reads, staging and unnamed closure publication; fixture creation,
/// session setup and audits are outside it.
#[tokio::test]
#[ignore = "run through benchmark run git-blob-file"]
async fn benchmark_git_blob_file() {
    let backend = std::env::var("CASITA_GIT_ALIAS_BACKEND").unwrap();
    if backend == "local" {
        let destination = tempfile::tempdir().unwrap();
        let repository = Repository::local(destination.path()).await.unwrap();
        alias_benchmark(&repository).await;
        repository.flush().await.unwrap();
    } else {
        assert_eq!(backend, "memory");
        let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
        alias_benchmark(&repository).await;
    }
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
}

async fn alias_benchmark<PS: casita::experimental::BlobStore, SS: MetadataStore>(
    repository: &Repository<PS, SS>,
) {
    use casita::{BlobId, Digest, ObjectKey};
    let setting = |name: &str| std::env::var(name).unwrap();
    let bytes: usize = setting("CASITA_GIT_ALIAS_BYTES").parse().unwrap();
    let files: usize =
        std::env::var("CASITA_GIT_ALIAS_FILES").map_or(1, |files| files.parse().unwrap());
    let strategy = setting("CASITA_GIT_ALIAS_STRATEGY");
    let backend = setting("CASITA_GIT_ALIAS_BACKEND");
    let bodies: Vec<Vec<u8>> = (0..files as u64)
        .map(|file| {
            let mut body = vec![0u8; bytes];
            let mut state = 0x9e3779b97f4a7c15u64 ^ file.wrapping_mul(0xbf58476d1ce4e5b9);
            for part in body.chunks_mut(8) {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                part.copy_from_slice(&state.to_le_bytes()[..part.len()]);
            }
            // Distinct files even where the length leaves no room for state.
            if let Some(first) = body.first_mut() {
                *first = file as u8;
            }
            body
        })
        .collect();
    let mut git = Vec::with_capacity(files);
    let native_session = repository.mutation_session().await.unwrap();
    let mut natives = Vec::with_capacity(files);
    for body in &bodies {
        let key = casita::experimental::git_object_key_for_body(
            GitObjectFormat::Sha1,
            GitObjectKind::Blob,
            body,
        )
        .unwrap();
        natives.push(
            native_session
                .stage_object(key.clone(), body)
                .await
                .unwrap(),
        );
        git.push(key);
    }
    native_session.publish_unrooted(natives).await.unwrap();
    let mut expected: Vec<_> = bodies
        .iter()
        .map(|body| ObjectKey::blob(BlobId::new(Digest::hash(body))))
        .collect();
    assert_eq!(
        expected
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        files,
        "every file must be distinct"
    );

    let session = repository.mutation_session().await.unwrap();
    let start = std::time::Instant::now();
    let staged = match strategy.as_str() {
        "reread" => {
            let snapshot = repository.metadata().snapshot().await.unwrap();
            let mut staged = Vec::with_capacity(files);
            for key in &git {
                let record = snapshot.object(key).await.unwrap().unwrap();
                staged.push(
                    session
                        .stage_existing(ObjectKey::blob(record.payload()), record.payload())
                        .await
                        .unwrap(),
                );
            }
            staged
        }
        "alias" => session.stage_git_blob_files(&git).await.unwrap(),
        _ => panic!("unknown alias strategy"),
    };
    // Every target must be among the staged files, so a strategy that staged
    // another identity fails here; the audits below check sizes and bytes.
    session
        .publish_closures(staged, expected.iter().cloned().collect())
        .await
        .unwrap();
    let wall_nanos = start.elapsed().as_nanos();

    for (key, body) in expected.iter().zip(&bodies) {
        assert_eq!(
            repository.verify_closure(key).await.unwrap(),
            casita::experimental::ClosureStatus::Complete { objects: 1 }
        );
        let (_, mut reader) = repository.open_payload(key).await.unwrap().unwrap();
        let mut actual = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut actual)
            .await
            .unwrap();
        assert_eq!(&actual, body);
    }
    drop(session);
    expected.sort();
    let root = match expected.as_slice() {
        [single] => single.to_string(),
        keys => Digest::hash(
            keys.iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
                .as_bytes(),
        )
        .to_string(),
    };
    println!(
        "git_blob_file_sample {}",
        serde_json::json!({
            "strategy": strategy, "backend": backend, "file_bytes": bytes, "files": files,
            "wall_nanos": wall_nanos, "root": root,
            "correctness": "exact identity, length, closure and byte-for-byte readback",
        })
    );
}

#[tokio::test]
async fn aliases_support_both_git_hashes_and_empty_payloads() {
    use casita::{BlobId, Digest, ObjectKey};
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    for format in [GitObjectFormat::Sha1, GitObjectFormat::Sha256] {
        for body in [b"".as_slice(), b"native contents"] {
            let session = repository.mutation_session().await.unwrap();
            let git =
                casita::experimental::git_object_key_for_body(format, GitObjectKind::Blob, body)
                    .unwrap();
            let native = session.stage_object(git.clone(), body).await.unwrap();
            session.publish_unrooted(vec![native]).await.unwrap();
            let alias = session.stage_git_blob_file(&git).await.unwrap();
            assert_eq!(
                alias.record().key(),
                &ObjectKey::blob(BlobId::new(Digest::hash(body)))
            );
            assert_eq!(alias.record().payload_size(), body.len() as u64);
            session.publish_unrooted(vec![alias]).await.unwrap();
        }
        let tree = casita::experimental::git_object_key_for_body(format, GitObjectKind::Tree, b"")
            .unwrap();
        let missing = casita::experimental::git_object_key_for_body(
            format,
            GitObjectKind::Blob,
            b"not stored",
        )
        .unwrap();
        let session = repository.mutation_session().await.unwrap();
        assert!(matches!(
            session.stage_git_blob_file(&tree).await,
            Err(RepositoryError::InvalidInput(_))
        ));
        assert!(matches!(
            session.stage_git_blob_file(&missing).await,
            Err(RepositoryError::Absent(_))
        ));
    }
}

#[tokio::test]
async fn staged_alias_protects_reused_bytes_across_collection() {
    let body = b"protected by the receiving mutation";
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let git = casita::experimental::git_object_key_for_body(
        GitObjectFormat::Sha1,
        GitObjectKind::Blob,
        body,
    )
    .unwrap();
    let native_session = repository.mutation_session().await.unwrap();
    let native = native_session
        .stage_object(git.clone(), body)
        .await
        .unwrap();
    native_session.publish_unrooted(vec![native]).await.unwrap();
    drop(native_session);
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
    let native_present = || async {
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .object(&git)
            .await
            .unwrap()
            .is_some()
    };

    // The staged alias is the only hold left on the unrooted native record.
    let receiving = repository.mutation_session().await.unwrap();
    let alias = receiving.stage_git_blob_file(&git).await.unwrap();
    let key = alias.record().key().clone();
    let held = repository.collect().await.unwrap().removed;
    assert_eq!((held.logical_objects, held.payload_blobs), (0, 0));
    assert!(native_present().await);

    // Once published and released, the rooted file alone keeps the shared
    // payload: collection takes the native record and leaves the bytes.
    receiving
        .publish_rooted(vec![alias], "file".try_into().unwrap(), key.clone())
        .await
        .unwrap();
    drop(receiving);
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
    let released = repository.collect().await.unwrap().removed;
    assert_eq!((released.logical_objects, released.payload_blobs), (1, 0));
    assert!(!native_present().await);
    let (_, mut reader) = repository.open_payload(&key).await.unwrap().unwrap();
    let mut actual = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut actual)
        .await
        .unwrap();
    assert_eq!(actual, body);
}

/// Delivers one previously valid snapshot, as if collection completed between
/// the initial metadata read and admission of the alias's data protections.
struct StaleOnceMetadata {
    inner: MemoryMetadataStore,
    stale: std::sync::Mutex<Option<std::sync::Arc<dyn casita::experimental::MetadataSnapshot>>>,
}
#[async_trait::async_trait]
impl MetadataStore for StaleOnceMetadata {
    fn coordinates_payload_catalog(&self) -> bool {
        self.inner.coordinates_payload_catalog()
    }
    async fn try_collection_lease(
        &self,
    ) -> Result<Option<casita::experimental::RepositoryLease>, casita::experimental::MetadataError>
    {
        self.inner.try_collection_lease().await
    }
    async fn pin_store(
        &self,
    ) -> Result<
        std::sync::Arc<dyn casita::experimental::PinStore>,
        casita::experimental::MetadataError,
    > {
        self.inner.pin_store().await
    }
    async fn snapshot(
        &self,
    ) -> Result<
        std::sync::Arc<dyn casita::experimental::MetadataSnapshot>,
        casita::experimental::MetadataError,
    > {
        let stale = self.stale.lock().unwrap().take();
        if let Some(snapshot) = stale {
            return Ok(snapshot);
        }
        self.inner.snapshot().await
    }
    async fn commit(
        &self,
        expected: &casita::RepositoryRevision,
        mutation: casita::experimental::MetadataMutation,
    ) -> Result<casita::experimental::CommitResult, casita::experimental::MetadataError> {
        self.inner.commit(expected, mutation).await
    }
}

#[tokio::test]
async fn alias_rechecks_records_after_admitting_protection() {
    let repository = Repository::new(
        MemoryBlobStore::new(),
        StaleOnceMetadata {
            inner: MemoryMetadataStore::new().unwrap(),
            stale: std::sync::Mutex::new(None),
        },
    );
    let key = casita::experimental::git_object_key_for_body(
        GitObjectFormat::Sha1,
        GitObjectKind::Blob,
        b"collected",
    )
    .unwrap();
    let native = repository.mutation_session().await.unwrap();
    let staged = native
        .stage_object(key.clone(), b"collected")
        .await
        .unwrap();
    native.publish_unrooted(vec![staged]).await.unwrap();
    let old = repository.metadata().snapshot().await.unwrap();
    drop(native);
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
    repository.collect().await.unwrap();
    assert!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .object(&key)
            .await
            .unwrap()
            .is_none()
    );
    let receiving = repository.mutation_session().await.unwrap();
    *repository.metadata().stale.lock().unwrap() = Some(old);
    assert!(
        matches!(
            receiving.stage_git_blob_file(&key).await,
            Err(RepositoryError::Absent(_))
        ),
        "a record collected before pin admission cannot authenticate live payload bytes"
    );
}

#[tokio::test]
async fn batch_registration_keeps_order_and_refuses_any_invalid_member() {
    use casita::{BlobId, Digest, ObjectKey};
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let bodies: [&[u8]; 3] = [b"third in key order", b"first", b"second"];
    let native_session = repository.mutation_session().await.unwrap();
    let mut natives = Vec::new();
    let mut git = Vec::new();
    for body in bodies {
        let key = casita::experimental::git_object_key_for_body(
            GitObjectFormat::Sha1,
            GitObjectKind::Blob,
            body,
        )
        .unwrap();
        natives.push(
            native_session
                .stage_object(key.clone(), body)
                .await
                .unwrap(),
        );
        git.push(key);
    }
    native_session.publish_unrooted(natives).await.unwrap();
    let session = repository.mutation_session().await.unwrap();
    let staged = session.stage_git_blob_files(&git).await.unwrap();
    assert_eq!(
        staged
            .iter()
            .map(|staged| staged.record().key().clone())
            .collect::<Vec<_>>(),
        bodies
            .iter()
            .map(|body| ObjectKey::blob(BlobId::new(Digest::hash(body))))
            .collect::<Vec<_>>()
    );
    let missing = casita::experimental::git_object_key_for_body(
        GitObjectFormat::Sha1,
        GitObjectKind::Blob,
        b"never stored",
    )
    .unwrap();
    let tree = casita::experimental::git_object_key_for_body(
        GitObjectFormat::Sha1,
        GitObjectKind::Tree,
        b"",
    )
    .unwrap();
    for (invalid, absent) in [(missing, true), (tree, false)] {
        let mut keys = git.clone();
        keys.insert(1, invalid);
        let result = session.stage_git_blob_files(&keys).await;
        if absent {
            assert!(matches!(result, Err(RepositoryError::Absent(_))));
        } else {
            assert!(matches!(result, Err(RepositoryError::InvalidInput(_))));
        }
    }
    assert!(session.stage_git_blob_files(&[]).await.unwrap().is_empty());
    // A repeated blob registers once per occurrence, keeping positions.
    let repeated = session
        .stage_git_blob_files(&[git[0].clone(), git[1].clone(), git[0].clone()])
        .await
        .unwrap();
    assert_eq!(repeated[0].record(), repeated[2].record());
    assert_ne!(repeated[0].record(), repeated[1].record());
}

#[tokio::test]
async fn registration_requires_builtin_formats_and_bounded_batches() {
    use casita::experimental::{FormatLimits, FormatRegistry};
    let body = b"registered under limits";
    let key = casita::experimental::git_object_key_for_body(
        GitObjectFormat::Sha1,
        GitObjectKind::Blob,
        body,
    )
    .unwrap();
    let limited = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        FormatRegistry::builtin(),
        FormatLimits {
            max_batch_objects: 1,
            ..Default::default()
        },
    );
    let session = limited.mutation_session().await.unwrap();
    let native = session.stage_object(key.clone(), body).await.unwrap();
    session.publish_unrooted(vec![native]).await.unwrap();
    assert!(matches!(
        session
            .stage_git_blob_files(&[key.clone(), key.clone()])
            .await,
        Err(RepositoryError::LimitExceeded(_))
    ));
    assert!(session.stage_git_blob_file(&key).await.is_ok());

    // A replacement registry may verify raw blobs differently, so a Git
    // record cannot vouch for a file under it.
    let custom = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        FormatRegistry::new([
            std::sync::Arc::new(casita::experimental::BlobFormat::default())
                as std::sync::Arc<dyn casita::experimental::ObjectFormat>,
            std::sync::Arc::new(casita::experimental::GitNativeObjectFormat::new(
                GitObjectFormat::Sha1,
                GitObjectKind::Blob,
            )),
        ])
        .unwrap(),
        FormatLimits::default(),
    );
    let session = custom.mutation_session().await.unwrap();
    let native = session.stage_object(key.clone(), body).await.unwrap();
    session.publish_unrooted(vec![native]).await.unwrap();
    assert!(matches!(
        session.stage_git_blob_file(&key).await,
        Err(RepositoryError::InvalidInput(_))
    ));
}
