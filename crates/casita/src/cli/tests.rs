use std::path::PathBuf;

use super::*;
use casita::experimental::MetadataStore as _;

#[cfg(feature = "oci")]
#[test]
fn oci_import_options_parse() {
    let cli = Cli::try_parse_from([
        "casita",
        "import",
        "-i",
        "oci",
        "registry.example.com/team/app:latest",
        "--root",
        "images/app",
        "--oci-platform",
        "linux/arm64",
        "--oci-http",
        "--oci-rootfs-root",
        "filesystems/app",
        "--oci-rootfs-max-bytes",
        "8192",
        "--oci-rootfs-max-entries",
        "100",
    ])
    .unwrap();
    let Command::Import(args) = cli.command else {
        panic!("expected import")
    };
    assert_eq!(args.importer, Some(ImporterKind::Oci));
    assert_eq!(args.oci.platform.as_deref(), Some("linux/arm64"));
    assert!(args.oci.http);
    assert_eq!(args.oci.rootfs_root.as_deref(), Some("filesystems/app"));
    assert_eq!(args.oci.rootfs_max_bytes, 8192);
    assert_eq!(args.oci.rootfs_max_entries, 100);
}

#[test]
fn import_concurrency_options_require_positive_limits() {
    let cli = Cli::try_parse_from([
        "casita",
        "import",
        ".",
        "--filesystem-concurrency",
        "3",
        "--chunk-upload-concurrency",
        "7",
    ])
    .unwrap();
    let Command::Import(args) = cli.command else {
        panic!("expected import")
    };
    assert_eq!(args.file_concurrency.unwrap().get(), 3);
    assert_eq!(args.chunk_upload_concurrency.get(), 7);
    let cli = Cli::try_parse_from(["casita", "import", "."]).unwrap();
    let Command::Import(args) = cli.command else {
        panic!("expected import")
    };
    assert!(args.file_concurrency.is_none());
    assert_eq!(args.chunk_upload_concurrency.get(), 32);
    for option in ["--filesystem-concurrency", "--chunk-upload-concurrency"] {
        assert!(Cli::try_parse_from(["casita", "import", ".", option, "0"]).is_err());
    }
}

fn archive_limits() -> ArchiveLimitsArgs {
    ArchiveLimitsArgs {
        max_archive_bytes: CLI_MAX_CASITAR_ARCHIVE_BYTES,
        max_payload_bytes: CLI_MAX_CASITAR_PAYLOAD_BYTES,
        max_total_payload_bytes: CLI_MAX_CASITAR_TOTAL_PAYLOAD_BYTES,
        max_payloads: casita::experimental::DEFAULT_MAX_CASITAR_STREAM_ITEMS,
        max_records: casita::experimental::DEFAULT_MAX_CASITAR_STREAM_ITEMS,
    }
}

fn tar_import_args() -> TarImportArgs {
    TarImportArgs {
        max_archive_bytes: CLI_MAX_TAR_ARCHIVE_BYTES,
        max_entries: CLI_MAX_TAR_ENTRIES,
        max_path_bytes: CLI_MAX_TAR_PATH_BYTES,
        max_file_bytes: CLI_MAX_TAR_FILE_BYTES,
        max_total_file_bytes: CLI_MAX_TAR_TOTAL_FILE_BYTES,
        max_sparse_expansion_bytes: CLI_MAX_TAR_SPARSE_EXPANSION_BYTES,
        max_in_flight_files: casita::experimental::TarImportLimits::default().max_in_flight_files,
    }
}

#[test]
fn git_ingest_options_require_positive_limits() {
    let cli = Cli::try_parse_from([
        "casita",
        "import",
        "source",
        "--git-concurrency",
        "4",
        "--git-max-buffered-bytes",
        "65536",
    ])
    .unwrap();
    let Command::Import(args) = cli.command else {
        panic!("wrong command")
    };
    assert_eq!(args.git.concurrency.get(), 4);
    assert_eq!(args.git.max_buffered_bytes.get(), 65536);
    for flag in ["--git-concurrency", "--git-max-buffered-bytes"] {
        assert!(Cli::try_parse_from(["casita", "import", "source", flag, "0"]).is_err());
    }
}

fn git_import_args() -> GitImportArgs {
    GitImportArgs {
        view: None,
        refs: Vec::new(),
        max_cached_pack_bytes: casita::experimental::DEFAULT_MAX_CACHED_GIT_PACK_BYTES,
        concurrency: casita::experimental::DEFAULT_GIT_IMPORT_CONCURRENCY,
        max_buffered_bytes: casita::experimental::DEFAULT_GIT_IMPORT_BUFFERED_BYTES,
    }
}

fn casitar_import_args() -> CasitarImportArgs {
    CasitarImportArgs {
        roots: Vec::new(),
        root_prefix: None,
        replace: false,
        max_archive_bytes: CLI_MAX_CASITAR_ARCHIVE_BYTES,
        max_payload_bytes: CLI_MAX_CASITAR_PAYLOAD_BYTES,
        max_total_payload_bytes: CLI_MAX_CASITAR_TOTAL_PAYLOAD_BYTES,
        max_payloads: casita::experimental::DEFAULT_MAX_CASITAR_STREAM_ITEMS,
        max_records: casita::experimental::DEFAULT_MAX_CASITAR_STREAM_ITEMS,
    }
}

#[test]
fn archive_runtime_errors_are_categorized_and_usage_errors_exit_two() {
    let malformed: Error = Box::new(casita::experimental::CasitarStreamError::Format(
        casita::experimental::CasitarError::Truncated,
    ));
    assert_eq!(
        stable_error_category(malformed.as_ref()),
        casita::experimental::RepositoryErrorCategory::InvalidData
    );
    assert_eq!(error_exit_code(&malformed), 1);

    let usage = usage_error("invalid flag combination");
    assert_eq!(error_exit_code(&usage), 2);
}

#[cfg(feature = "git")]
#[test]
fn git_builder_errors_preserve_the_stable_category() {
    let error: Error = Box::new(
        casita::import::GitImport::new("unused", "upstream")
            .with_refs(["invalid ref"])
            .expect_err("invalid ref must be rejected"),
    );
    assert_eq!(
        stable_error_category(error.as_ref()),
        casita::ErrorKind::InvalidInput
    );
    assert_eq!(error_exit_code(&error), 1);
}

#[tokio::test]
async fn transfer_errors_have_the_same_category_in_cli_and_stable_api() {
    let source = casita::Repository::memory().unwrap();
    let destination = casita::Repository::memory().unwrap();
    let missing: casita::RootName = "missing".try_into().unwrap();
    let stable = destination
        .import(casita::import::CopyImport::new(
            &source,
            missing.clone(),
            "destination".try_into().unwrap(),
        ))
        .await
        .unwrap_err();
    let backend = casita::experimental::TransferError::MissingSourceRoot(missing);
    assert_eq!(stable.kind(), casita::ErrorKind::Absent);
    assert_eq!(stable_error_category(&backend), stable.kind());
    assert_eq!(backend.category(), stable.kind());

    // The transfer context must win over the nested metadata error's
    // classification: a source-side failure is a backend failure.
    let source_failure = casita::experimental::TransferError::SourceMetadata(
        casita::experimental::MetadataError::InvalidMetadata("source failure".into()),
    );
    assert_eq!(
        stable_error_category(&source_failure),
        casita::ErrorKind::Backend
    );
}

#[cfg(feature = "git")]
#[tokio::test]
async fn git_view_errors_have_the_same_category_in_cli_and_stable_api() {
    let source = tempfile::tempdir().unwrap();
    for args in [
        vec!["init", "--initial-branch=main"],
        vec![
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--allow-empty",
            "-m",
            "fixture",
        ],
    ] {
        let output = std::process::Command::new("git")
            .current_dir(source.path())
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", source.path().join("empty-config"))
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let repository = casita::Repository::memory().unwrap();
    let stable = repository
        .import(casita::import::GitImport::new(
            source.path(),
            "invalid/view",
        ))
        .await
        .unwrap_err();
    let backend = casita::experimental::git_view_root_name("invalid/view").unwrap_err();
    assert_eq!(stable.kind(), casita::ErrorKind::InvalidInput);
    assert_eq!(stable_error_category(&backend), stable.kind());
    assert_eq!(backend.category(), stable.kind());
}

#[test]
fn filesystem_import_rejects_git_specific_flags() {
    for flag in ["--git", "--rev", "--submodules", "--offline", "--shallow"] {
        assert!(Cli::try_parse_from(["casita", "import", "repo", flag]).is_err());
    }
}

#[test]
fn parses_global_tracing_options() {
    let cli = Cli::try_parse_from([
        "casita",
        "--log-filter",
        "casita=debug,object_store=info",
        "--log-format",
        "json",
        "root",
        "ls",
    ])
    .unwrap();
    assert_eq!(
        cli.log_filter.as_deref(),
        Some("casita=debug,object_store=info")
    );
    assert_eq!(cli.log_format, LogFormat::Json);

    let default = Cli::try_parse_from(["casita", "root", "ls"]).unwrap();
    assert_eq!(default.log_filter, None);
    assert_eq!(default.log_format, LogFormat::Compact);
}

#[tokio::test]
async fn detects_filesystem_git_tar_and_casitar_inputs() {
    let temp = tempfile::tempdir().unwrap();
    let filesystem = temp.path().join("filesystem");
    std::fs::create_dir(&filesystem).unwrap();
    assert_eq!(
        commands::detect_importer(&filesystem).await.unwrap(),
        ImporterKind::Filesystem
    );

    let git = temp.path().join("git");
    std::fs::create_dir_all(git.join(".git")).unwrap();
    assert_eq!(
        commands::detect_importer(&git).await.unwrap(),
        ImporterKind::Git
    );

    let casitar = temp.path().join("archive.casitar");
    std::fs::write(&casitar, casita::experimental::CASITAR_MAGIC).unwrap();
    assert_eq!(
        commands::detect_importer(&casitar).await.unwrap(),
        ImporterKind::Casitar
    );

    let tar = temp.path().join("archive.tar");
    let mut archive = tokio_tar::Builder::new(Vec::new());
    let mut header = tokio_tar::Header::new_ustar();
    header.set_size(0);
    header.set_mode(0o644);
    archive
        .append_data(&mut header, "empty", tokio::io::empty())
        .await
        .unwrap();
    std::fs::write(&tar, archive.into_inner().await.unwrap()).unwrap();
    assert_eq!(
        commands::detect_importer(&tar).await.unwrap(),
        ImporterKind::Tar
    );

    let unknown = temp.path().join("unknown");
    std::fs::write(&unknown, b"not an archive").unwrap();
    assert!(commands::detect_importer(&unknown).await.is_err());
}

#[test]
fn root_removal_requires_exactly_one_selector() {
    assert!(Cli::try_parse_from(["casita", "root", "rm"]).is_err());
    assert!(Cli::try_parse_from(["casita", "root", "rm", "pin"]).is_ok());
    assert!(Cli::try_parse_from(["casita", "root", "rm", "--prefix", "auto"]).is_ok());
    assert!(Cli::try_parse_from(["casita", "root", "rm", "pin", "--prefix", "auto"]).is_err());
}

#[test]
fn parses_sync_selectors() {
    let cli = Cli::try_parse_from([
        "casita",
        "sync",
        "--from",
        "source",
        "--to",
        "destination",
        "--object",
        "casita.blob.v1:AA",
        "--root",
        "releases/current",
    ])
    .unwrap();
    let Command::Sync(args) = cli.command else {
        panic!("expected sync command");
    };
    assert_eq!(args.from, "source");
    assert!(args.from_blobs.is_none());
    assert_eq!(args.to, "destination");
    assert_eq!(args.objects, ["casita.blob.v1:AA"]);
    assert_eq!(args.roots, ["releases/current"]);
    assert!(args.path.is_none());
    assert!(args.destination_root.is_none());
    assert!(!args.shallow);
}

#[test]
fn parses_s3_sync_endpoints_and_writer() {
    let cli = Cli::try_parse_from([
        "casita",
        "--pack-target-bytes",
        "16777216",
        "--pack-cache-bytes",
        "67108864",
        "sync",
        "--from",
        "s3://source-bucket/releases",
        "--to",
        "s3://mirror-bucket",
        "--writer",
        "runner-a",
        "--root",
        "releases/current",
    ])
    .unwrap();
    assert_eq!(cli.pack_target_bytes, Some(16 * 1024 * 1024));
    assert_eq!(cli.pack_cache_bytes, Some(64 * 1024 * 1024));
    let Command::Sync(args) = cli.command else {
        panic!("expected sync command");
    };
    assert_eq!(args.from, "s3://source-bucket/releases");
    assert_eq!(args.to, "s3://mirror-bucket");
    assert_eq!(args.writer.as_deref(), Some("runner-a"));
}

#[test]
fn parses_path_selected_sync() {
    let cli = Cli::try_parse_from([
        "casita",
        "sync",
        "--from",
        "source",
        "--to",
        "destination",
        "--root",
        "releases/current",
        "--path",
        "lib/python3.12",
        "--destination-root",
        "partial/python",
    ])
    .unwrap();
    let Command::Sync(args) = cli.command else {
        panic!("expected sync command");
    };
    assert_eq!(args.path.as_deref(), Some("lib/python3.12"));
    assert_eq!(args.destination_root.as_deref(), Some("partial/python"));
    assert!(
        Cli::try_parse_from([
            "casita",
            "sync",
            "--from",
            "source",
            "--to",
            "destination",
            "--root",
            "releases/current",
            "--destination-root",
            "partial/python",
        ])
        .is_err()
    );
}

#[test]
fn parses_fsck_with_default_repair_and_dry_run() {
    let cli = Cli::try_parse_from(["casita", "fsck", "--source", "replica"]).unwrap();
    let Command::Fsck(args) = cli.command else {
        panic!("expected fsck command");
    };
    assert!(!args.dry_run);
    assert!(!args.audit_only);
    assert_eq!(args.source, Some(PathBuf::from("replica")));

    let cli = Cli::try_parse_from(["casita", "fsck", "--dry-run"]).unwrap();
    let Command::Fsck(args) = cli.command else {
        panic!("expected fsck command");
    };
    assert!(args.dry_run);
    assert!(!args.audit_only);
    assert!(args.source.is_none());

    let cli = Cli::try_parse_from(["casita", "fsck", "--audit-only"]).unwrap();
    let Command::Fsck(args) = cli.command else {
        panic!("expected fsck command");
    };
    assert!(args.audit_only);
    assert!(!args.dry_run);
    assert!(args.source.is_none());
    assert!(Cli::try_parse_from(["casita", "fsck", "--audit-only", "--dry-run"]).is_err());
}

#[test]
fn parses_archive_workflows_and_requires_explicit_selectors_and_names() {
    let cli = Cli::try_parse_from([
        "casita",
        "archive",
        "create",
        "--root",
        "releases/current",
        "--object",
        "casita.blob.v1:AA",
        "--output",
        "release.casitar",
    ])
    .unwrap();
    assert!(matches!(
        cli.command,
        Command::Archive {
            command: ArchiveCommand::Create(_)
        }
    ));
    assert!(
        Cli::try_parse_from(["casita", "archive", "create", "--output", "release.casitar"])
            .is_err()
    );

    assert!(Cli::try_parse_from(["casita", "archive", "inspect", "-"]).is_ok());
    assert!(Cli::try_parse_from(["casita", "archive", "verify", "input.casitar"]).is_ok());
    assert!(Cli::try_parse_from(["casita", "archive", "import", "input.casitar"]).is_err());
    assert!(
        Cli::try_parse_from([
            "casita",
            "archive",
            "import",
            "input.casitar",
            "--root",
            "imports/one",
            "--root",
            "imports/two"
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "casita",
            "archive",
            "import",
            "input.casitar",
            "--root-prefix",
            "imports/release"
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "casita",
            "archive",
            "import",
            "input.casitar",
            "--root",
            "imports/one",
            "--root-prefix",
            "imports/release"
        ])
        .is_err()
    );
}

/// Runs on a deliberately small stack: unoptimized builds once needed about
/// 2 MiB here, because the dispatcher awaited every command future inline,
/// which overflowed Windows. A command awaited without `on_heap` regresses this
/// on every platform. MSVC frames are larger, so Windows keeps libtest's 2 MiB.
#[test]
fn archive_cli_create_inspect_verify_and_import_round_trip() {
    const STACK_BYTES: usize = if cfg!(windows) { 2 << 20 } else { 3 << 19 };
    std::thread::Builder::new()
        .stack_size(STACK_BYTES)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(archive_cli_create_inspect_verify_and_import_round_trip_case())
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn archive_cli_create_inspect_verify_and_import_round_trip_case() {
    let temp = tempfile::tempdir().unwrap();
    let source_repository = temp.path().join("source-repository");
    let destination_repository = temp.path().join("destination-repository");
    let unused_repository = temp.path().join("must-not-be-created");
    let input = temp.path().join("input");
    let archive = temp.path().join("release.casitar");
    std::fs::create_dir(&input).unwrap();
    std::fs::write(input.join("hello"), b"portable closure").unwrap();

    commands::run(Cli {
        log_filter: None,
        log_format: LogFormat::Compact,
        repository: Some(source_repository.clone()),
        spill_memory_objects: None,
        spill_bytes: None,
        pack_target_bytes: None,
        pack_cache_bytes: None,

        command: Command::Import(ImportArgs {
            path: input,
            importer: Some(ImporterKind::Filesystem),
            name: Some("source/release".into()),
            retention: None,
            rehash: false,
            file_concurrency: None,
            chunk_upload_concurrency: std::num::NonZeroUsize::new(32).unwrap(),
            tar: tar_import_args(),
            #[cfg(feature = "oci")]
            oci: OciImportArgs::default(),
            git: git_import_args(),
            casitar: casitar_import_args(),
        }),
    })
    .await
    .unwrap();
    commands::run(Cli {
        log_filter: None,
        log_format: LogFormat::Compact,
        repository: Some(source_repository.clone()),
        spill_memory_objects: Some(1),
        spill_bytes: None,
        pack_target_bytes: None,
        pack_cache_bytes: None,

        command: Command::Archive {
            command: ArchiveCommand::Create(ArchiveCreateArgs {
                roots: vec!["source/release".into()],
                objects: Vec::new(),
                output: archive.to_string_lossy().into_owned(),
                force: false,
                json: false,
                limits: archive_limits(),
            }),
        },
    })
    .await
    .unwrap();
    let first_bytes = std::fs::read(&archive).unwrap();

    commands::run(Cli {
        log_filter: None,
        log_format: LogFormat::Compact,
        repository: Some(unused_repository.clone()),
        spill_memory_objects: None,
        spill_bytes: None,
        pack_target_bytes: None,
        pack_cache_bytes: None,

        command: Command::Archive {
            command: ArchiveCommand::Inspect(ArchiveInspectArgs {
                input: archive.to_string_lossy().into_owned(),
                json: true,
                limits: archive_limits(),
            }),
        },
    })
    .await
    .unwrap();
    commands::run(Cli {
        log_filter: None,
        log_format: LogFormat::Compact,
        repository: Some(unused_repository.clone()),
        spill_memory_objects: Some(1),
        spill_bytes: None,
        pack_target_bytes: None,
        pack_cache_bytes: None,

        command: Command::Archive {
            command: ArchiveCommand::Verify(ArchiveVerifyArgs {
                input: archive.to_string_lossy().into_owned(),
                json: false,
                limits: archive_limits(),
            }),
        },
    })
    .await
    .unwrap();
    assert!(!unused_repository.exists());

    let create_again = commands::run(Cli {
        log_filter: None,
        log_format: LogFormat::Compact,
        repository: Some(source_repository.clone()),
        spill_memory_objects: None,
        spill_bytes: None,
        pack_target_bytes: None,
        pack_cache_bytes: None,

        command: Command::Archive {
            command: ArchiveCommand::Create(ArchiveCreateArgs {
                roots: vec!["source/release".into()],
                objects: Vec::new(),
                output: archive.to_string_lossy().into_owned(),
                force: false,
                json: false,
                limits: archive_limits(),
            }),
        },
    })
    .await;
    assert!(create_again.is_err());
    assert_eq!(std::fs::read(&archive).unwrap(), first_bytes);

    commands::run(Cli {
        log_filter: None,
        log_format: LogFormat::Compact,
        repository: Some(source_repository),
        spill_memory_objects: None,
        spill_bytes: None,
        pack_target_bytes: None,
        pack_cache_bytes: None,

        command: Command::Archive {
            command: ArchiveCommand::Create(ArchiveCreateArgs {
                roots: vec!["source/release".into()],
                objects: Vec::new(),
                output: archive.to_string_lossy().into_owned(),
                force: true,
                json: false,
                limits: archive_limits(),
            }),
        },
    })
    .await
    .unwrap();
    assert_eq!(std::fs::read(&archive).unwrap(), first_bytes);

    commands::run(Cli {
        log_filter: None,
        log_format: LogFormat::Compact,
        repository: Some(destination_repository.clone()),
        spill_memory_objects: Some(1),
        spill_bytes: None,
        pack_target_bytes: None,
        pack_cache_bytes: None,

        command: Command::Import(ImportArgs {
            path: archive,
            importer: None,
            name: None,
            retention: None,
            rehash: false,
            file_concurrency: None,
            chunk_upload_concurrency: std::num::NonZeroUsize::new(32).unwrap(),
            tar: tar_import_args(),
            #[cfg(feature = "oci")]
            oci: OciImportArgs::default(),
            git: git_import_args(),
            casitar: CasitarImportArgs {
                roots: Vec::new(),
                root_prefix: Some("archives/release".into()),
                replace: false,
                ..casitar_import_args()
            },
        }),
    })
    .await
    .unwrap();

    let repository = casita::experimental::Repository::local(destination_repository)
        .await
        .unwrap();
    let snapshot = repository.metadata().snapshot().await.unwrap();
    let name = casita::experimental::RootName::try_from("archives/release/0").unwrap();
    let target = snapshot.root(&name).await.unwrap().unwrap();
    assert!(matches!(
        repository.verify_closure(&target).await.unwrap(),
        casita::experimental::ClosureStatus::Complete { .. }
    ));
}

#[tokio::test]
async fn archive_cli_verify_rejects_corruption_without_opening_selected_repository() {
    let temp = tempfile::tempdir().unwrap();
    let archive = temp.path().join("corrupt.casitar");
    let unused_repository = temp.path().join("must-not-be-created");
    std::fs::write(&archive, b"not a Casitar archive").unwrap();

    let result = commands::run(Cli {
        log_filter: None,
        log_format: LogFormat::Compact,
        repository: Some(unused_repository.clone()),
        spill_memory_objects: None,
        spill_bytes: None,
        pack_target_bytes: None,
        pack_cache_bytes: None,

        command: Command::Archive {
            command: ArchiveCommand::Verify(ArchiveVerifyArgs {
                input: archive.to_string_lossy().into_owned(),
                json: false,
                limits: archive_limits(),
            }),
        },
    })
    .await;

    assert!(result.is_err());
    assert!(!unused_repository.exists());
}

#[test]
fn parses_native_git_workflows() {
    let cli = Cli::try_parse_from([
        "casita",
        "import",
        "-i",
        "tar",
        "release.tar",
        "--root",
        "releases/current",
        "--tar-max-entries",
        "42",
    ])
    .unwrap();
    let Command::Import(args) = cli.command else {
        panic!("expected tar import command");
    };
    assert_eq!(args.importer, Some(ImporterKind::Tar));
    assert_eq!(args.path, PathBuf::from("release.tar"));
    assert_eq!(args.name.as_deref(), Some("releases/current"));
    assert_eq!(args.tar.max_entries, 42);

    let cli = Cli::try_parse_from([
        "casita",
        "import",
        "-i",
        "casitar",
        "release.casitar",
        "--casitar-root-prefix",
        "releases/current",
        "--casitar-replace",
    ])
    .unwrap();
    let Command::Import(args) = cli.command else {
        panic!("expected Casitar import command");
    };
    assert_eq!(args.importer, Some(ImporterKind::Casitar));
    assert_eq!(
        args.casitar.root_prefix.as_deref(),
        Some("releases/current")
    );
    assert!(args.casitar.replace);

    let cli = Cli::try_parse_from([
        "casita",
        "import",
        "-i",
        "git",
        "repo",
        "--git-view",
        "upstream",
        "--git-ref",
        "refs/heads/main",
        "--git-max-cached-pack-bytes",
        "123",
    ])
    .unwrap();
    let Command::Import(args) = cli.command else {
        panic!("expected Git import command");
    };
    assert_eq!(args.importer, Some(ImporterKind::Git));
    assert_eq!(args.git.max_cached_pack_bytes, 123);

    let cli = Cli::try_parse_from([
        "casita",
        "git",
        "serve",
        "upstream",
        "--listen",
        "127.0.0.1:0",
    ])
    .unwrap();
    assert!(matches!(
        cli.command,
        Command::Git {
            command: NativeGitCommand::Serve { .. }
        }
    ));
}

#[tokio::test]
async fn tar_cli_import_publishes_a_complete_evictable_root() {
    let temp = tempfile::tempdir().unwrap();
    let repository_dir = temp.path().join("repository");
    let archive_path = temp.path().join("release.tar");
    let mut archive = tokio_tar::Builder::new(Vec::new());
    let mut header = tokio_tar::Header::new_ustar();
    header.set_size(5);
    header.set_mode(0o644);
    archive
        .append_data(&mut header, "README", b"hello".as_slice())
        .await
        .unwrap();
    std::fs::write(&archive_path, archive.into_inner().await.unwrap()).unwrap();

    commands::run(
        Cli::try_parse_from([
            "casita".into(),
            "--repository".into(),
            repository_dir.clone().into_os_string(),
            "import".into(),
            archive_path.into_os_string(),
            "--root".into(),
            "releases/current".into(),
            "--retention".into(),
            "evictable".into(),
            "--tar-max-in-flight-files".into(),
            "2".into(),
        ])
        .unwrap(),
    )
    .await
    .unwrap();

    let repository = casita::experimental::Repository::local(repository_dir)
        .await
        .unwrap();
    let root = repository
        .metadata()
        .snapshot()
        .await
        .unwrap()
        .root(&casita::experimental::RootName::try_from("releases/current").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        repository.verify_closure(&root).await.unwrap(),
        casita::experimental::ClosureStatus::Complete { .. }
    ));
    assert_eq!(
        repository
            .root_retention(&casita::experimental::RootName::try_from("releases/current").unwrap())
            .await
            .unwrap(),
        Some(casita::RootRetention::Evictable)
    );
}

#[tokio::test]
async fn sync_command_copies_a_named_root() {
    let temp = tempfile::tempdir().unwrap();
    let source_repository = temp.path().join("source-repository");
    let destination_repository = temp.path().join("destination-repository");
    let input = temp.path().join("input");
    std::fs::create_dir(&input).unwrap();
    std::fs::write(input.join("hello"), b"generic transfer").unwrap();

    commands::run(Cli {
        log_filter: None,
        log_format: LogFormat::Compact,
        repository: Some(source_repository.clone()),
        spill_memory_objects: None,
        spill_bytes: None,
        pack_target_bytes: None,
        pack_cache_bytes: None,

        command: Command::Import(ImportArgs {
            path: input,
            importer: Some(ImporterKind::Filesystem),
            name: Some("releases/current".into()),
            retention: None,
            rehash: false,
            file_concurrency: None,
            chunk_upload_concurrency: std::num::NonZeroUsize::new(32).unwrap(),
            tar: tar_import_args(),
            #[cfg(feature = "oci")]
            oci: OciImportArgs::default(),
            git: git_import_args(),
            casitar: casitar_import_args(),
        }),
    })
    .await
    .unwrap();
    let bounded = Cli::try_parse_from([
        "casita",
        "--spill-memory-objects",
        "1",
        "--spill-bytes",
        "0",
        "sync",
        "--from",
        source_repository.to_str().unwrap(),
        "--to",
        destination_repository.to_str().unwrap(),
        "--root",
        "releases/current",
    ])
    .unwrap();
    assert!(commands::run(bounded).await.is_err());
    let failed = casita::experimental::Repository::local(&destination_repository)
        .await
        .unwrap();
    assert!(
        failed
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .root(&casita::experimental::RootName::try_from("releases/current").unwrap())
            .await
            .unwrap()
            .is_none()
    );
    drop(failed);

    commands::run(Cli {
        log_filter: None,
        log_format: LogFormat::Compact,
        repository: None,
        spill_memory_objects: Some(1),
        spill_bytes: None,
        pack_target_bytes: None,
        pack_cache_bytes: None,

        command: Command::Sync(SyncArgs {
            from: source_repository.to_string_lossy().into_owned(),
            from_blobs: None,
            to: destination_repository.to_string_lossy().into_owned(),
            writer: None,
            objects: Vec::new(),
            roots: vec!["releases/current".into()],
            path: None,
            destination_root: None,
            shallow: false,
            incremental: false,
        }),
    })
    .await
    .unwrap();

    let repository = casita::experimental::Repository::local(destination_repository)
        .await
        .unwrap();
    let snapshot = repository.metadata().snapshot().await.unwrap();
    let name = casita::experimental::RootName::try_from("releases/current").unwrap();
    let target = snapshot.root(&name).await.unwrap().unwrap();
    assert!(matches!(
        repository.verify_closure(&target).await.unwrap(),
        casita::experimental::ClosureStatus::Complete { .. }
    ));
}
#[tokio::test]
async fn sync_command_split_source_copies_from_blob_replica_and_rejects_missing_payloads() {
    let temp = tempfile::tempdir().unwrap();
    let metadata_path = temp.path().join("metadata");
    let blobs_path = temp.path().join("blobs");
    let destination_path = temp.path().join("destination");
    let missing_path = temp.path().join("missing");
    let name = casita::RootName::try_from("main").unwrap();
    let metadata = casita::Repository::local(&metadata_path).await.unwrap();
    let key = metadata
        .import(casita::import::BlobImport::new(
            b"split payload".as_slice(),
            name.clone(),
        ))
        .await
        .unwrap();
    metadata.flush().await.unwrap();
    let blobs = casita::Repository::local(&blobs_path).await.unwrap();
    blobs
        .import(casita::import::CopyImport::new(
            &metadata,
            name.clone(),
            casita::RootName::try_from("replica").unwrap(),
        ))
        .await
        .unwrap();
    blobs
        .import(casita::import::BlobImport::new(
            b"different root".as_slice(),
            name.clone(),
        ))
        .await
        .unwrap();
    blobs.flush().await.unwrap();
    drop(metadata);
    drop(blobs);

    for blob_path in [&missing_path, &blobs_path] {
        let cli = Cli::try_parse_from([
            "casita",
            "sync",
            "--from",
            metadata_path.to_str().unwrap(),
            "--from-blobs",
            blob_path.to_str().unwrap(),
            "--to",
            destination_path.to_str().unwrap(),
            "--root",
            "main",
            "--incremental",
        ])
        .unwrap();
        let result = commands::run(cli).await;
        let destination = casita::Repository::local(&destination_path).await.unwrap();
        if blob_path == &missing_path {
            assert!(result.is_err());
            assert!(destination.root(&name).await.unwrap().is_none());
        } else {
            result.unwrap();
            assert_eq!(destination.root(&name).await.unwrap(), Some(key.clone()));
            let mut reader = destination.open(&key).await.unwrap().unwrap();
            let mut bytes = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut bytes)
                .await
                .unwrap();
            assert_eq!(bytes, b"split payload");
        }
        destination.flush().await.unwrap();
    }
}

#[tokio::test]
async fn sync_command_copies_one_path_under_an_explicit_destination_root() {
    let temp = tempfile::tempdir().unwrap();
    let source_repository = temp.path().join("source-repository");
    let destination_repository = temp.path().join("destination-repository");
    let input = temp.path().join("input");
    std::fs::create_dir_all(input.join("sub")).unwrap();
    std::fs::write(input.join("sub/selected"), b"selected payload").unwrap();
    std::fs::write(input.join("outside"), b"outside payload").unwrap();

    commands::run(Cli {
        log_filter: None,
        log_format: LogFormat::Compact,
        repository: Some(source_repository.clone()),
        spill_memory_objects: None,
        spill_bytes: None,
        pack_target_bytes: None,
        pack_cache_bytes: None,

        command: Command::Import(ImportArgs {
            path: input,
            importer: Some(ImporterKind::Filesystem),
            name: Some("releases/current".into()),
            retention: None,
            rehash: false,
            file_concurrency: None,
            chunk_upload_concurrency: std::num::NonZeroUsize::new(32).unwrap(),
            tar: tar_import_args(),
            #[cfg(feature = "oci")]
            oci: OciImportArgs::default(),
            git: git_import_args(),
            casitar: casitar_import_args(),
        }),
    })
    .await
    .unwrap();
    commands::run(Cli {
        log_filter: None,
        log_format: LogFormat::Compact,
        repository: None,
        spill_memory_objects: None,
        spill_bytes: None,
        pack_target_bytes: None,
        pack_cache_bytes: None,

        command: Command::Sync(SyncArgs {
            from: source_repository.to_string_lossy().into_owned(),
            from_blobs: None,
            to: destination_repository.to_string_lossy().into_owned(),
            writer: None,
            objects: Vec::new(),
            roots: vec!["releases/current".into()],
            path: Some("sub/selected".into()),
            destination_root: Some("partial/selected".into()),
            shallow: false,
            incremental: true,
        }),
    })
    .await
    .unwrap();

    let repository = casita::experimental::Repository::local(destination_repository)
        .await
        .unwrap();
    let snapshot = repository.metadata().snapshot().await.unwrap();
    let selected = casita::experimental::ObjectKey::blob(casita::experimental::BlobId::new(
        casita::experimental::Digest::hash(b"selected payload"),
    ));
    let outside = casita::experimental::ObjectKey::blob(casita::experimental::BlobId::new(
        casita::experimental::Digest::hash(b"outside payload"),
    ));
    assert_eq!(
        snapshot
            .root(&casita::experimental::RootName::try_from("partial/selected").unwrap())
            .await
            .unwrap(),
        Some(selected.clone())
    );
    assert!(snapshot.object(&selected).await.unwrap().is_some());
    assert!(snapshot.object(&outside).await.unwrap().is_none());
    assert!(
        snapshot
            .root(&casita::experimental::RootName::try_from("releases/current").unwrap())
            .await
            .unwrap()
            .is_none()
    );
}
