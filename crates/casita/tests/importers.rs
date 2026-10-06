//! One public import contract for every built-in format and custom inputs.
#![cfg(feature = "native")]

use casita::{
    ObjectKey, Repository, RootName,
    import::{CasitarImport, CopyImport, FilesystemImport, Importer, TarImport},
};

#[cfg(feature = "experimental")]
use casita::experimental::MetadataStore;

fn name(value: &str) -> RootName {
    value.try_into().unwrap()
}

async fn run<R: Sync, I: Importer<R>>(repository: &R, input: I) -> Result<I::Report, I::Error> {
    input.import(repository).await
}

fn send<T: Send>(value: T) -> T {
    value
}

#[tokio::test]
async fn copy_import_resolves_at_execution_and_preserves_destination_on_missing_source() {
    let repository = Repository::memory().unwrap();
    let source = name("source");
    let destination = name("copy");
    repository
        .import(casita::import::BlobImport::new(&b"old"[..], source.clone()))
        .await
        .unwrap();
    let request = CopyImport::new(&repository, source.clone(), destination.clone());
    let updated = repository
        .import(casita::import::BlobImport::new(&b"new"[..], source.clone()))
        .await
        .unwrap();
    let copied = send(run(&repository, request)).await.unwrap();
    assert_eq!(copied, updated);
    assert_eq!(
        repository.root(&destination).await.unwrap(),
        Some(updated.clone())
    );

    // Missing source names must not replace the destination or publish metadata.
    let revision = repository.metadata_reader().await.unwrap().revision();
    let error = send(run(
        &repository,
        CopyImport::new(&repository, name("missing"), destination.clone()),
    ))
    .await
    .unwrap_err();
    assert_eq!(error.kind(), casita::ErrorKind::Absent);
    assert_eq!(
        repository.metadata_reader().await.unwrap().revision(),
        revision
    );
    assert_eq!(
        repository.root(&destination).await.unwrap(),
        Some(updated.clone())
    );

    repository.remove_root(&source, &updated).await.unwrap();
    repository.collect().await.unwrap();
    let mut reader = repository.open(&copied).await.unwrap().unwrap();
    let mut bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, b"new");
}

#[cfg(feature = "experimental")]
#[tokio::test]
async fn copy_import_keeps_the_selected_revision_through_source_replacement_and_gc() {
    use casita::experimental::{
        MemoryBlobStore, MemoryMetadataStore, Repository as CoreRepository, TransferError,
        TransferReadSession, TransferSelection, TransferSource,
    };

    struct ReplacingSource(CoreRepository<MemoryBlobStore, MemoryMetadataStore>);

    #[async_trait::async_trait]
    impl TransferSource for ReplacingSource {
        async fn begin_transfer(
            &self,
            selection: TransferSelection,
        ) -> Result<Box<dyn TransferReadSession + '_>, TransferError> {
            let session = self.0.begin_transfer(selection).await?;
            self.0
                .import(casita::import::BlobImport::new(
                    &b"replacement"[..],
                    name("source"),
                ))
                .await?;
            self.0.collect().await?;
            Ok(session)
        }
    }

    let source = ReplacingSource(CoreRepository::memory().unwrap());
    let original = source
        .0
        .import(casita::import::BlobImport::new(
            &b"original"[..],
            name("source"),
        ))
        .await
        .unwrap();
    let destination = Repository::memory().unwrap();
    let copied = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        run(
            &destination,
            CopyImport::from_source(&source, name("source"), name("copy")),
        ),
    )
    .await
    .expect("online collection must complete while the transfer retains its source")
    .unwrap();
    assert_eq!(copied, original);
    let mut reader = destination.open(&copied).await.unwrap().unwrap();
    let mut bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, b"original");
}

#[cfg(feature = "experimental")]
#[tokio::test]
async fn copy_import_bridges_application_and_custom_storage_repositories() {
    let application = Repository::memory().unwrap();
    let core = casita::experimental::Repository::memory().unwrap();
    let key = application
        .import(casita::import::BlobImport::new(
            &b"copied bytes"[..],
            name("source"),
        ))
        .await
        .unwrap();
    let copied = send(run(
        &core,
        CopyImport::new(&application, name("source"), name("core")),
    ))
    .await
    .unwrap();
    assert_eq!(copied, key);
    let copied = send(run(
        &core,
        CopyImport::from_source(&core, name("core"), name("other")),
    ))
    .await
    .unwrap();
    assert_eq!(copied, key);
    let copied = send(run(
        &application,
        CopyImport::from_source(&core, name("other"), name("restored")),
    ))
    .await
    .unwrap();
    assert_eq!(copied, key);
    assert_eq!(
        application.root(&name("restored")).await.unwrap(),
        Some(key)
    );

    let error = send(run(
        &core,
        CopyImport::from_source(&core, name("missing"), name("core")),
    ))
    .await
    .unwrap_err();
    assert!(
        matches!(error, casita::experimental::TransferError::MissingSourceRoot(root) if root == name("missing"))
    );
}

#[tokio::test]
async fn generic_caller_preserves_reports_and_round_trips_across_formats() {
    let work = tempfile::tempdir().unwrap();
    std::fs::write(work.path().join("hello"), b"shared content").unwrap();
    std::fs::write(work.path().join(".casita"), b"control data").unwrap();
    let repository = Repository::memory().unwrap();
    let blob = send(run(
        &repository,
        casita::import::BlobImport::new(&b"shared content"[..], name("blob")),
    ))
    .await
    .unwrap();
    assert_eq!(
        repository.root(&name("blob")).await.unwrap(),
        Some(blob.clone())
    );
    let mut reader = repository.open(&blob).await.unwrap().unwrap();
    let mut bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, b"shared content");

    let key: ObjectKey = send(run(
        &repository,
        FilesystemImport::new(work.path(), name("filesystem"))
            .reread(true)
            .exclude(".casita"),
    ))
    .await
    .unwrap();

    let mut builder = tokio_tar::Builder::new(Vec::new());
    let mut header = tokio_tar::Header::new_ustar();
    header.set_size(14);
    header.set_mode(0o644);
    builder
        .append_data(&mut header, "hello", &b"shared content"[..])
        .await
        .unwrap();
    let tar = builder.into_inner().await.unwrap();
    assert!(
        repository
            .import(TarImport::new(&tar[..], name("too-small")).with_limits(
                casita::TarImportLimits {
                    max_file_bytes: 13,
                    ..Default::default()
                }
            ))
            .await
            .is_err()
    );
    assert!(repository.root(&name("too-small")).await.unwrap().is_none());
    let report = send(run(&repository, TarImport::new(&tar[..], name("tar"))))
        .await
        .unwrap();
    assert_eq!(report.root, key);
    assert_eq!(report.files, 1);

    let archive = repository.export_casitar(&key, Vec::new()).await.unwrap();
    let restored = Repository::memory().unwrap();
    let report = send(run(
        &restored,
        CasitarImport::new(&archive[..], [name("archive")]),
    ))
    .await
    .unwrap();
    assert_eq!(report.mappings[0].root, key);
    assert!(report.records_inserted > 0);
    let target = tempfile::tempdir().unwrap();
    restored.checkout(&key, target.path()).await.unwrap();
    assert_eq!(
        std::fs::read(target.path().join("hello")).unwrap(),
        b"shared content"
    );
    assert!(!target.path().join(".casita").exists());
}

#[cfg(feature = "experimental")]
#[tokio::test]
async fn inspected_casitar_readers_keep_original_limits_and_conflict_policy() {
    use casita::experimental::{CasitarImportError, CasitarReader, CasitarStreamError};
    use casita::{CasitarRootConflictPolicy, CasitarStreamLimits, ErrorKind};

    let source = Repository::memory().unwrap();
    let key = source
        .import(casita::import::BlobImport::new(
            &b"archive payload"[..],
            name("source"),
        ))
        .await
        .unwrap();
    let archive = source.export_casitar(&key, Vec::new()).await.unwrap();
    let destination = Repository::memory().unwrap();
    let revision = destination.metadata_reader().await.unwrap().revision();
    let tight = CasitarStreamLimits {
        max_payload_bytes: 1,
        ..Default::default()
    };

    let error = destination
        .import(CasitarImport::with_limits(
            &archive[..],
            [name("rejected")],
            tight,
        ))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(
        destination.metadata_reader().await.unwrap().revision(),
        revision
    );
    assert!(destination.roots().await.unwrap().is_empty());
    let core = casita::experimental::Repository::memory().unwrap();

    // Limits supplied during header inspection remain active during import.
    let reader = CasitarReader::open(&archive[..], tight).await.unwrap();
    assert!(matches!(
        core.import(CasitarImport::from_reader(reader, [name("limited")],))
            .await,
        Err(CasitarImportError::Stream(CasitarStreamError::Limit {
            field: "payload bytes",
            ..
        }))
    ));

    let reader = CasitarReader::open(&archive[..], CasitarStreamLimits::default())
        .await
        .unwrap();
    let report = destination
        .import(CasitarImport::from_reader(reader, [name("accepted")]))
        .await
        .unwrap();
    assert_eq!(report.mappings[0].root, key);
    let reader = CasitarReader::open(&archive[..], CasitarStreamLimits::default())
        .await
        .unwrap();
    let error = destination
        .import(CasitarImport::from_reader(reader, [name("accepted")]))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::DestinationConflict);

    let reader = CasitarReader::open(&archive[..], CasitarStreamLimits::default())
        .await
        .unwrap();
    let replaced = destination
        .import(
            CasitarImport::from_reader(reader, [name("accepted")])
                .with_conflict_policy(CasitarRootConflictPolicy::ReplaceIfUnchanged),
        )
        .await
        .unwrap();
    assert_eq!(replaced.mappings, report.mappings);
}

struct CustomImport;

#[tokio::test]
async fn failed_blob_import_preserves_the_existing_root() {
    struct FailingReader;
    impl tokio::io::AsyncRead for FailingReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            _: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::other("injected input failure")))
        }
    }
    let repository = Repository::memory().unwrap();
    let root = name("blob");
    let old = repository
        .import(casita::import::BlobImport::new(
            &b"original"[..],
            root.clone(),
        ))
        .await
        .unwrap();
    let input = tokio::io::AsyncReadExt::chain(&b"partial replacement"[..], FailingReader);
    assert!(
        repository
            .import(casita::import::BlobImport::new(input, root.clone()))
            .await
            .is_err()
    );
    assert_eq!(repository.root(&root).await.unwrap(), Some(old));
}

#[async_trait::async_trait]
impl Importer for CustomImport {
    type Report = ObjectKey;
    type Error = casita::Error;

    async fn import(self, repository: &Repository) -> Result<Self::Report, Self::Error> {
        repository
            .import(casita::import::BlobImport::new(
                &b"custom format input"[..],
                name("custom"),
            ))
            .await
    }
}

#[tokio::test]
async fn repository_accepts_a_downstream_importer() {
    let repository = Repository::memory().unwrap();
    let key = send(repository.import(CustomImport)).await.unwrap();
    assert_eq!(repository.root(&name("custom")).await.unwrap(), Some(key));
}

#[cfg(feature = "experimental")]
#[tokio::test]
async fn generic_caller_supports_custom_storage_and_existing_sessions() {
    let source = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("hello"), b"session import").unwrap();
    let repository = casita::experimental::Repository::memory().unwrap();
    let first = run(
        &repository,
        FilesystemImport::new(source.path(), name("repository")),
    )
    .await
    .unwrap();
    let session = repository.mutation_session().await.unwrap();
    let second = send(run(
        &session,
        FilesystemImport::new(source.path(), name("session")),
    ))
    .await
    .unwrap();
    assert_eq!(first, second);
    let unrooted = send(run(
        &session,
        casita::import::UnrootedFilesystemImport::new(source.path()),
    ))
    .await
    .unwrap();
    assert_eq!(first, unrooted);
    let blob = send(run(
        &session,
        casita::import::BlobImport::new(&b"raw bytes"[..], name("session/blob")),
    ))
    .await
    .unwrap();
    drop(session);
    let other = send(run(
        &repository,
        casita::import::BlobImport::new(&b"raw bytes"[..], name("repository/blob")),
    ))
    .await
    .unwrap();
    assert_eq!(blob, other);
}

#[cfg(feature = "git")]
#[tokio::test]
async fn generic_caller_imports_a_native_git_view() {
    let source = tempfile::tempdir().unwrap();
    let git = |args: &[&str]| {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(source.path())
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", source.path().join("empty-config"))
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    git(&["init", "--initial-branch=main"]);
    std::fs::write(source.path().join("hello"), b"native git").unwrap();
    git(&["add", "hello"]);
    git(&[
        "-c",
        "user.name=Import Test",
        "-c",
        "user.email=import@example.invalid",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "-m",
        "fixture",
    ]);
    git(&["branch", "other"]);
    git(&["tag", "v1"]);
    git(&["repack", "-ad"]);
    let repository = Repository::memory().unwrap();
    let report = send(run(
        &repository,
        casita::import::GitImport::new(source.path(), "upstream"),
    ))
    .await
    .unwrap();
    assert_eq!(
        repository.root(&name("git/upstream")).await.unwrap(),
        Some(report.view.clone())
    );
    assert!(report.objects > 0);

    // The packed fixture can retain a full-clone cache. Disabling it changes
    // the view, but preserves the imported native objects.
    let uncached = repository
        .import(
            casita::import::GitImport::new(source.path(), "uncached").with_max_cached_pack_bytes(0),
        )
        .await
        .unwrap();
    assert_ne!(uncached.view, report.view);
    assert_eq!(uncached.objects, report.objects);

    let selected = repository
        .import(
            casita::import::GitImport::new(source.path(), "selected")
                .with_refs(["refs/heads/main"])
                .unwrap()
                .with_max_cached_pack_bytes(0),
        )
        .await
        .unwrap();
    assert_ne!(selected.view, uncached.view);

    // Resetting the selection restores both branches and the tag.
    let reset = repository
        .import(
            casita::import::GitImport::new(source.path(), "reset")
                .with_refs(["refs/heads/main"])
                .unwrap()
                .with_refs(std::iter::empty::<&str>())
                .unwrap()
                .with_max_cached_pack_bytes(0),
        )
        .await
        .unwrap();
    assert_eq!(reset.view, uncached.view);

    // Removing the unselected refs from the source produces exactly the same
    // view as explicit selection, proving neither branch nor tag leaked in.
    git(&["branch", "-D", "other"]);
    git(&["tag", "-d", "v1"]);
    let main_only = repository
        .import(
            casita::import::GitImport::new(source.path(), "main-only")
                .with_max_cached_pack_bytes(0),
        )
        .await
        .unwrap();
    assert_eq!(main_only.view, selected.view);

    let error = casita::import::GitImport::new(source.path(), "invalid")
        .with_refs(["invalid ref"])
        .expect_err("invalid ref must be rejected by the builder");
    assert_eq!(error.kind(), casita::ErrorKind::InvalidInput);
}

#[cfg(feature = "experimental")]
#[tokio::test]
async fn casitar_request_preserves_multi_root_mapping_and_conflict_policy() {
    use casita::experimental::{CasitarStreamLimits, MetadataStore as _};
    let source = casita::experimental::Repository::memory().unwrap();
    let session = source.mutation_session().await.unwrap();
    let first = session.stage_blob(b"first root").await.unwrap();
    let first_key = first.record().key().clone();
    let second = session.stage_blob(b"second root").await.unwrap();
    let second_key = second.record().key().clone();
    session.publish_unrooted(vec![first, second]).await.unwrap();
    drop(session);
    let (archive, _) = source
        .export_casitar(
            [first_key.clone(), second_key.clone()],
            Vec::new(),
            CasitarStreamLimits::default(),
        )
        .await
        .unwrap();
    let destination = Repository::memory().unwrap();
    let names = vec![name("first"), name("second")];
    for invalid in [
        Vec::new(),
        vec![names[0].clone()],
        vec![names[0].clone(); 2],
    ] {
        let error = destination
            .import(CasitarImport::new(&archive[..], invalid))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), casita::ErrorKind::InvalidInput);
        assert!(destination.roots().await.unwrap().is_empty());
    }
    let request = || CasitarImport::new(&archive[..], names.clone());
    let report = destination.import(request()).await.unwrap();
    assert_eq!(report.mappings.len(), 2);
    for mapping in &report.mappings {
        assert_eq!(
            destination.root(&mapping.name).await.unwrap(),
            Some(mapping.root.clone())
        );
    }
    assert_eq!(
        destination.import(request()).await.unwrap_err().kind(),
        casita::ErrorKind::DestinationConflict
    );
    let replacement =
        request().with_conflict_policy(casita::CasitarRootConflictPolicy::ReplaceIfUnchanged);
    let repeated = destination.import(replacement).await.unwrap();
    assert_eq!(repeated.mappings, report.mappings);
    assert_eq!(repeated.records_inserted, 0);
    assert!(
        source
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .object(&first_key)
            .await
            .unwrap()
            .is_some()
    );
}

#[cfg(feature = "experimental")]
#[tokio::test]
async fn multi_root_import_contract_and_validation() {
    use casita::{
        experimental::{MetadataStore, Repository as CoreRepository},
        import::MultiRootFilesystemImport,
    };
    let temporary = tempfile::tempdir().unwrap();
    let a = temporary.path().join("a");
    let b = temporary.path().join("b");
    std::fs::create_dir(&a).unwrap();
    std::fs::create_dir(&b).unwrap();
    std::fs::write(a.join("same"), b"first").unwrap();
    std::fs::write(b.join("same"), b"second").unwrap();
    let repository = CoreRepository::memory().unwrap();
    let paths = vec![(a.clone(), name("a")), (b.clone(), name("b"))];
    let keys = send(run(
        &repository,
        MultiRootFilesystemImport::new(paths.clone()).reread(true),
    ))
    .await
    .unwrap();
    assert_eq!(keys.len(), 2);
    assert_ne!(keys[0], keys[1]);
    let session = repository.mutation_session().await.unwrap();
    assert_eq!(
        send(run(
            &session,
            MultiRootFilesystemImport::new(paths)
                .with_file_concurrency(std::num::NonZeroUsize::new(1).unwrap())
        ))
        .await
        .unwrap(),
        keys
    );
    let before = repository.metadata().snapshot().await.unwrap().revision();
    assert!(
        run(&session, MultiRootFilesystemImport::new(Vec::new()))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        run(
            &session,
            MultiRootFilesystemImport::new(vec![(a.clone(), name("a")), (b, name("a"))])
        )
        .await
        .is_err()
    );
    assert!(
        run(
            &session,
            MultiRootFilesystemImport::new(vec![
                (a, name("a")),
                (temporary.path().join("missing"), name("b"))
            ])
        )
        .await
        .is_err()
    );
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert_eq!(snapshot.revision(), before);
    assert_eq!(
        snapshot.root(&name("a")).await.unwrap(),
        Some(keys[0].clone())
    );
    assert_eq!(
        snapshot.root(&name("b")).await.unwrap(),
        Some(keys[1].clone())
    );
}

#[cfg(feature = "experimental")]
#[tokio::test]
async fn staging_importers_leave_objects_and_roots_unpublished() {
    use casita::import::BlobImport;
    let repository = casita::experimental::Repository::memory().unwrap();
    let session = repository.mutation_session().await.unwrap();
    let work = tempfile::tempdir().unwrap();
    std::fs::write(work.path().join("file"), b"filesystem").unwrap();
    let mut builder = tokio_tar::Builder::new(Vec::new());
    let mut header = tokio_tar::Header::new_ustar();
    header.set_size(3);
    header.set_mode(0o644);
    builder
        .append_data(&mut header, "tar-file", &b"tar"[..])
        .await
        .unwrap();
    let tar = builder.into_inner().await.unwrap();
    let blob = BlobImport::new(&b"blob"[..], name("staged/blob"))
        .stage(&session)
        .await
        .unwrap();
    let filesystem = FilesystemImport::new(work.path(), name("staged/filesystem"))
        .stage(&session)
        .await
        .unwrap();
    let tar = TarImport::new(tar.as_slice(), name("staged/tar"))
        .stage(&session)
        .await
        .unwrap();
    let snapshot = repository.metadata().snapshot().await.unwrap();
    let before = snapshot.generation().unwrap();
    for (root, key) in [
        ("staged/blob", &blob.report),
        ("staged/filesystem", &filesystem.report),
        ("staged/tar", &tar.report.root),
    ] {
        assert!(snapshot.root(&name(root)).await.unwrap().is_none());
        assert!(snapshot.object(key).await.unwrap().is_none());
    }
    drop(snapshot);
    let mut objects = blob.objects;
    objects.extend(filesystem.objects);
    objects.extend(tar.objects);
    session
        .publish(
            objects,
            vec![blob.root_change, filesystem.root_change, tar.root_change],
        )
        .await
        .unwrap();
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .generation()
            .unwrap(),
        before + 1
    );
    let reader = repository.retained_reader().await.unwrap();
    let output = tempfile::tempdir().unwrap();
    reader
        .checkout(&tar.report.root, output.path().join("tar"))
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(output.path().join("tar/tar-file")).unwrap(),
        b"tar"
    );
}

#[cfg(feature = "experimental")]
#[tokio::test]
async fn staged_filesystem_and_tar_respect_single_commit_object_limits() {
    use casita::experimental::{
        FormatLimits, FormatRegistry, MemoryBlobStore, MemoryMetadataStore,
    };
    let source = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("file"), b"file").unwrap();
    let mut builder = tokio_tar::Builder::new(Vec::new());
    let mut header = tokio_tar::Header::new_ustar();
    header.set_size(4);
    header.set_mode(0o644);
    builder
        .append_data(&mut header, "file", &b"file"[..])
        .await
        .unwrap();
    let tar = builder.into_inner().await.unwrap();
    // One file plus its directory: below, at, and above the object count.
    for limit in [1, 2, 3] {
        for is_tar in [false, true] {
            let repository = casita::experimental::Repository::with_formats(
                MemoryBlobStore::new(),
                MemoryMetadataStore::new().unwrap(),
                FormatRegistry::builtin(),
                FormatLimits {
                    max_batch_objects: limit,
                    ..FormatLimits::default()
                },
            );
            let session = repository.mutation_session().await.unwrap();
            let before = repository.metadata().snapshot().await.unwrap().revision();
            let staged = if is_tar {
                TarImport::new(tar.as_slice(), name("stage"))
                    .stage(&session)
                    .await
                    .map(|result| (result.objects, result.root_change))
                    .map_err(|e| e.to_string())
            } else {
                FilesystemImport::new(source.path(), name("stage"))
                    .stage(&session)
                    .await
                    .map(|result| (result.objects, result.root_change))
                    .map_err(|e| e.to_string())
            };
            assert_eq!(staged.is_ok(), limit >= 2);
            assert_eq!(
                repository.metadata().snapshot().await.unwrap().revision(),
                before
            );
            if let Ok((objects, change)) = staged {
                assert_eq!(objects.len(), 2);
                session.publish(objects, vec![change]).await.unwrap();
            }
        }
    }
}
