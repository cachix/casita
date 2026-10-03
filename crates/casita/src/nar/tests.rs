use super::*;
#[cfg(feature = "experimental")]
use crate::metadata::MetadataStore;
#[cfg(feature = "experimental")]
use crate::repository::Repository as CoreRepository;
use crate::{MetadataChange, RootName};
#[cfg(feature = "experimental")]
use async_trait::async_trait;
use std::path::PathBuf;

const HELLO: &[u8] = include_bytes!("../../tests/fixtures/nar/hello.nar");
const EXEC: &[u8] = include_bytes!("../../tests/fixtures/nar/hello-executable.nar");

#[path = "tests/invalidation_recovery.rs"]
mod invalidation_recovery;

/// Store-path hash parts, the only reference needles Nix looks for.
const NAME_HASH: &str = "dc04vv14dak1c1r48qa0m23vr9jy8sm0";
const CONTENT_HASH: &str = "zc842j0rz61mjsp3h3wp5ly71ak6qgdn";
const TARGET_HASH: &str = "a5cn2i4b83gnsm60d38l3kgb8qfplm11";
const MISSING_HASH: &str = "fn7zvafq26f0c8b17brs7s95s10ibfzs";

/// A tree whose references sit in file contents, an entry name, and a
/// symlink target: the three places Nix's whole-NAR scan reaches. Returns
/// the archive and its file payload length.
fn reference_nar() -> (Vec<u8>, u64) {
    use nix_archive::nar::{NamedNode, Node};
    let contents = format!("points at /nix/store/{CONTENT_HASH}-contents\n");
    let target = format!("/nix/store/{TARGET_HASH}-target");
    let children = [
        NamedNode {
            name: b"contents",
            node: Node::Regular {
                executable: false,
                contents: contents.as_bytes(),
            },
        },
        NamedNode {
            name: NAME_HASH.as_bytes(),
            node: Node::Regular {
                executable: true,
                contents: b"",
            },
        },
        NamedNode {
            name: b"link",
            node: Node::Symlink {
                target: target.as_bytes(),
            },
        },
    ];
    let mut out = Vec::new();
    nix_archive::nar::encode_tree(&mut out, &Node::Directory(&children)).unwrap();
    (out, contents.len() as u64)
}

#[test]
fn errors_preserve_categories_and_retry_guidance() {
    use crate::{ErrorKind, RetryDisposition};
    let delay = Duration::from_secs(3);
    let cases = [
        (
            NarError::invalid("bad request"),
            ErrorKind::InvalidInput,
            RetryDisposition::Never,
        ),
        (
            NarError::Conflict,
            ErrorKind::ImmutableConflict,
            RetryDisposition::Never,
        ),
        (
            NarError::from(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)),
            ErrorKind::InvalidData,
            RetryDisposition::Never,
        ),
        (
            NarError::from(std::io::Error::from(std::io::ErrorKind::TimedOut)),
            ErrorKind::Backend,
            RetryDisposition::Retry,
        ),
        (
            NarError::storage(crate::metadata::MetadataError::Busy("busy".into())),
            ErrorKind::Busy,
            RetryDisposition::Retry,
        ),
        (
            NarError::storage(crate::error::Error::Throttled {
                retry_after: Some(delay),
                source: Box::new(std::io::Error::other("throttled")),
            }),
            ErrorKind::Backend,
            RetryDisposition::RetryAfter(delay),
        ),
    ];
    for (error, kind, retry) in cases {
        assert_eq!(error.kind(), kind);
        assert_eq!(error.retry_disposition(), retry);
    }
    let application = crate::Error::classified(
        ErrorKind::Busy,
        crate::metadata::MetadataError::Busy("busy".into()),
    );
    let error = NarError::from(application);
    assert_eq!(error.kind(), ErrorKind::Busy);
    assert_eq!(error.retry_disposition(), RetryDisposition::Retry);
}

#[tokio::test]
async fn unavailable_tree_is_distinct_from_invalid_request() {
    let repo = Repository::memory().unwrap();
    let reader = repo.retained_reader().await.unwrap();
    let root = Node::File {
        digest: crate::BlobId::new(blake3::hash(b"absent").into()),
        size: 6,
        executable: false,
    };
    let error = lookup_nar(&reader, &root, &NarRequirements::default())
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), crate::ErrorKind::Absent);
    assert_eq!(error.retry_disposition(), crate::RetryDisposition::Never);
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn token(out: &mut Vec<u8>, s: &[u8]) {
    out.extend_from_slice(&(s.len() as u64).to_le_bytes());
    out.extend_from_slice(s);
    out.resize(out.len() + (8 - s.len() % 8) % 8, 0);
}
fn directory(entries: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut out = Vec::new();
    for s in [b"nix-archive-1".as_slice(), b"(", b"type", b"directory"] {
        token(&mut out, s);
    }
    for (name, node) in entries {
        for s in [b"entry".as_slice(), b"(", b"name", name, b"node"] {
            token(&mut out, s);
        }
        out.extend_from_slice(&node[24..]);
        token(&mut out, b")");
    }
    token(&mut out, b")");
    out
}
async fn publish(repo: &Repository, report: &VerifiedNarReport) {
    if let Some(target) = object_key(report.root()) {
        repo.commit(
            vec![],
            vec![MetadataChange::SetRoot {
                name: RootName::try_from("saved").unwrap(),
                target,
            }],
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn independent_nix_fixtures_and_hash_domains() {
    let repository = Repository::memory().unwrap();
    let requirements = NarRequirements::default()
        .hash(NarHashMethod::Text, NarHashAlgorithm::Sha256)
        .hash(NarHashMethod::Flat, NarHashAlgorithm::Sha256)
        .hash(NarHashMethod::Git, NarHashAlgorithm::Sha1)
        .hash(NarHashMethod::Nar, NarHashAlgorithm::Sha512)
        .reference_needles(vec![MISSING_HASH.into(), MISSING_HASH.into()]);
    let report = repository
        .import(NarImport::new(HELLO).requirements(requirements.clone()))
        .await
        .unwrap();
    assert_eq!(report.nar_size(), 120);
    assert_eq!(
        hex(report.nar_sha256()),
        "1c37d01af40be2e80691de3cc3df44377a699afbb17c68f080964b2fd071fc13"
    );
    assert_eq!(
        report
            .hash(NarHashMethod::Flat, NarHashAlgorithm::Sha256)
            .unwrap(),
        Sha256::digest(b"hello\n").as_slice()
    );
    assert_eq!(
        hex(report
            .hash(NarHashMethod::Git, NarHashAlgorithm::Sha1)
            .unwrap()),
        "ce013625030ba8dba906f756967f9e9ca394464a"
    );
    assert_eq!(report.reference_matches(), &[] as &[usize]);
    assert_eq!(report.stats().encoding_passes, 0);
    assert_eq!(report.stats().hash_payload_bytes, 6);
    let warm = ensure_nar(report.reader(), report.root(), &requirements)
        .await
        .unwrap();
    assert!(warm.stats().association_hit);
    assert_eq!(warm.stats().hash_payload_bytes, 0);
    let executable = repository.import(NarImport::new(EXEC)).await.unwrap();
    assert_eq!(
        hex(executable.nar_sha256()),
        "65436039d3f93ca19a8dbf1c60b15739ed58f53f14b8d372acc1b351533010fa"
    );
    assert_ne!(identity(report.root()), identity(executable.root()));
    assert!(
        ensure_nar(executable.reader(), executable.root(), &requirements)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn partial_hits_persist_and_scrub_reads_payload() {
    let repo = Repository::memory().unwrap();
    let report = repo.import(NarImport::new(HELLO)).await.unwrap();
    let requirements =
        NarRequirements::default().hash(NarHashMethod::Flat, NarHashAlgorithm::Sha256);
    assert!(
        lookup_nar(report.reader(), report.root(), &requirements)
            .await
            .unwrap()
            .is_none()
    );
    let added = ensure_nar(report.reader(), report.root(), &requirements)
        .await
        .unwrap();
    assert_eq!(added.stats().encoding_passes, 0);
    assert_eq!(added.stats().hash_payload_bytes, 6);
    assert!(
        ensure_nar(report.reader(), report.root(), &requirements)
            .await
            .unwrap()
            .stats()
            .association_hit
    );
    let more = requirements.hash(NarHashMethod::Nar, NarHashAlgorithm::Sha512);
    let added = ensure_nar(report.reader(), report.root(), &more)
        .await
        .unwrap();
    assert_eq!(added.stats().encoding_passes, 1);
    assert_eq!(
        added
            .hash(NarHashMethod::Nar, NarHashAlgorithm::Sha512)
            .unwrap(),
        Sha512::digest(HELLO).as_slice()
    );
    let scrub = scrub_nar(report.reader(), report.root(), &more)
        .await
        .unwrap();
    assert_eq!(scrub.stats().encoding_passes, 1);
    assert_eq!(scrub.stats().hash_payload_bytes, 6);
}

#[tokio::test]
async fn reject_noncanonical_truncated_and_trailing_input() {
    let repo = Repository::memory().unwrap();
    for end in 0..HELLO.len() {
        assert!(
            repo.import(NarImport::new(&HELLO[..end])).await.is_err(),
            "truncation {end}"
        );
    }
    let mut trailing = HELLO.to_vec();
    trailing.push(0);
    assert!(
        repo.import(NarImport::new(trailing.as_slice()))
            .await
            .is_err()
    );
    let mut padding = HELLO.to_vec();
    padding[20] = 1;
    assert!(
        repo.import(NarImport::new(padding.as_slice()))
            .await
            .is_err()
    );
    for entries in [
        vec![(b"z".as_slice(), HELLO), (b"a".as_slice(), HELLO)],
        vec![(b"a".as_slice(), HELLO), (b"a".as_slice(), HELLO)],
        vec![(b"..".as_slice(), HELLO)],
    ] {
        let bytes = directory(&entries);
        assert!(repo.import(NarImport::new(bytes.as_slice())).await.is_err());
    }
    let root = Node::File {
        digest: crate::BlobId::new(blake3::hash(b"hello\n").into()),
        size: 6,
        executable: false,
    };
    assert!(
        repo.inner
            .nar_store
            .as_ref()
            .unwrap()
            .get(&identity(&root))
            .await
            .unwrap()
            .is_none()
    );
    let framed = HELLO
        .iter()
        .chain(HELLO.iter())
        .copied()
        .collect::<Vec<_>>();
    let mut reader = framed.as_slice();
    repo.import(NarImport::new((&mut reader).take(HELLO.len() as u64)))
        .await
        .unwrap();
    assert_eq!(reader, HELLO);
}

#[tokio::test]
async fn native_reopen_and_metadata_only_changes_reuse() {
    let data = tempfile::tempdir().unwrap();
    let source = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("hello"), b"hello\n").unwrap();
    std::fs::create_dir(source.path().join("empty")).unwrap();
    // APFS rejects non-UTF-8 filenames; keep the native byte-name case on
    // other Unix filesystems while still exercising symlinks on macOS.
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        use std::os::unix::ffi::OsStrExt;
        std::fs::write(
            source.path().join(std::ffi::OsStr::from_bytes(b"\xff")),
            b"",
        )
        .unwrap();
    }
    #[cfg(unix)]
    std::os::unix::fs::symlink("hello", source.path().join("link")).unwrap();
    let root;
    {
        let repo = Repository::local(data.path()).await.unwrap();
        let report = repo
            .import(FilesystemNarImport::new(source.path()))
            .await
            .unwrap();
        assert_eq!(report.stats().encoding_passes, 1);
        root = report.root().clone();
        publish(&repo, &report).await;
        let again = repo
            .import(FilesystemNarImport::new(source.path()))
            .await
            .unwrap();
        assert!(again.stats().association_hit);
        assert_eq!(again.stats().hash_payload_bytes, 0);
        drop(again);
        drop(report);
        repo.flush().await.unwrap();
    }
    let repo = Repository::local(data.path()).await.unwrap();
    let again = repo
        .import(FilesystemNarImport::new(source.path()))
        .await
        .unwrap();
    assert_eq!(again.root(), &root);
    assert!(again.stats().association_hit);
    let key = crate::MetadataKey::new("peer.nar.v1".try_into().unwrap(), b"forged".to_vec());
    repo.commit(
        vec![],
        vec![MetadataChange::Set {
            key,
            value: Bytes::from_static(b"arbitrary forged hash"),
        }],
    )
    .await
    .unwrap();
    assert!(
        ensure_nar(again.reader(), again.root(), &NarRequirements::default())
            .await
            .unwrap()
            .stats()
            .association_hit
    );
    let scrub = scrub_nar(again.reader(), again.root(), &NarRequirements::default())
        .await
        .unwrap();
    assert_eq!(scrub.nar_sha256(), again.nar_sha256());
}

#[tokio::test]
async fn retention_collection_and_reimport() {
    let repo = Repository::memory().unwrap();
    let report = repo.import(NarImport::new(HELLO)).await.unwrap();
    let root = report.root().clone();
    repo.collect().await.unwrap();
    assert!(
        lookup_nar(report.reader(), &root, &NarRequirements::default())
            .await
            .unwrap()
            .is_some()
    );
    drop(report);
    repo.collect().await.unwrap();
    let reader = repo.retained_reader().await.unwrap();
    assert!(
        lookup_nar(&reader, &root, &NarRequirements::default())
            .await
            .is_err()
    );
    drop(reader);
    let report = repo.import(NarImport::new(HELLO)).await.unwrap();
    assert!(
        ensure_nar(report.reader(), &root, &NarRequirements::default())
            .await
            .unwrap()
            .stats()
            .association_hit
    );
}

#[tokio::test]
async fn concurrent_verification_coalesces_and_conflicts_quarantine() {
    let repo = Repository::memory().unwrap();
    let report = repo.import(NarImport::new(HELLO)).await.unwrap();
    let req = NarRequirements::default().hash(NarHashMethod::Nar, NarHashAlgorithm::Sha512);
    let (a, b) = tokio::join!(
        ensure_nar(report.reader(), report.root(), &req),
        ensure_nar(report.reader(), report.root(), &req)
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(a.stats().encoding_passes + b.stats().encoding_passes, 1);
    let store = repo.inner.nar_store.as_ref().unwrap();
    let mut forged = report.facts.clone();
    forged.size += 1;
    assert!(matches!(
        store
            .merge(
                &identity(report.root()),
                &forged,
                store.generation().await.unwrap()
            )
            .await,
        Err(NarError::Conflict)
    ));
    assert!(matches!(
        ensure_nar(report.reader(), report.root(), &req).await,
        Err(NarError::Conflict)
    ));
}

#[tokio::test]
async fn directory_reads_enforce_current_repository_limits() {
    let payloads = crate::blob::MemoryBlobStore::new();
    let metadata = crate::metadata::MemoryMetadataStore::new().unwrap();
    let repository = Repository {
        inner: crate::repository::Repository::new(payloads.clone(), metadata.clone())
            .into_builtin(),
    };
    let nar = directory(&[]);
    let report = repository
        .import(NarImport::new(nar.as_slice()))
        .await
        .unwrap();
    let Node::Directory { digest, .. } = report.root() else {
        panic!("expected directory root");
    };
    let key = ObjectKey::directory(*digest);
    for (max_metadata_bytes, max_payload_bytes) in [(7, 8), (8, 7), (8, 8)] {
        let repository = Repository {
            inner: crate::repository::Repository::with_formats(
                payloads.clone(),
                metadata.clone(),
                crate::format::FormatRegistry::builtin(),
                crate::format::FormatLimits {
                    max_metadata_bytes,
                    max_payload_bytes,
                    ..Default::default()
                },
            )
            .into_builtin(),
        };
        let reader = repository.retained_reader().await.unwrap();
        let result = stream::read_directory(&reader, &key).await;
        if max_metadata_bytes.min(max_payload_bytes) == 7 {
            let error = result.unwrap_err();
            assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
            assert!(
                error
                    .to_string()
                    .contains("directory payload exceeds 7 bytes")
            );
        } else {
            assert_eq!(result.unwrap(), crate::Directory::new());
        }
    }
}

#[tokio::test]
async fn nested_git_order_and_canonical_roundtrip() {
    let repo = Repository::memory().unwrap();
    let empty = directory(&[]);
    let nar = directory(&[(b"a", &empty), (b"a.b", HELLO), (b"z", EXEC)]);
    let requirements =
        NarRequirements::default().hash(NarHashMethod::Git, NarHashAlgorithm::Sha256);
    let report = repo
        .import(NarImport::new(nar.as_slice()).requirements(requirements.clone()))
        .await
        .unwrap();
    assert_eq!(report.nar_sha256(), Sha256::digest(&nar).as_slice());
    let scrub = scrub_nar(report.reader(), report.root(), &requirements)
        .await
        .unwrap();
    assert_eq!(scrub.nar_sha256(), report.nar_sha256());
    assert_eq!(
        scrub.hash(NarHashMethod::Git, NarHashAlgorithm::Sha256),
        report.hash(NarHashMethod::Git, NarHashAlgorithm::Sha256)
    );
    // nix-archive hashes the same tree from a borrowed node, so Casita's
    // stored-object walk must produce the identical Git tree. The file
    // `a.b` sorts before the directory `a` there, unlike in the NAR.
    let expected = {
        use nix_archive::nar::{NamedNode, Node};
        let hello = Node::Regular {
            executable: false,
            contents: b"hello\n",
        };
        let children = [
            NamedNode {
                name: b"a",
                node: Node::Directory(&[]),
            },
            NamedNode {
                name: b"a.b",
                node: hello,
            },
            NamedNode {
                name: b"z",
                node: Node::Regular {
                    executable: true,
                    contents: b"hello\n",
                },
            },
        ];
        nix_archive::git::hash_node::<Sha256>(&Node::Directory(&children))
            .unwrap()
            .hash
    };
    assert_eq!(
        report.hash(NarHashMethod::Git, NarHashAlgorithm::Sha256),
        Some(expected.as_slice())
    );
}

#[tokio::test]
async fn reference_needles_must_be_store_path_hash_parts() {
    let repo = Repository::memory().unwrap();
    let report = repo.import(NarImport::new(HELLO)).await.unwrap();
    let wrong_length = b"dc04vv14dak1c1r48qa0m23vr9jy8sm".to_vec();
    let wrong_alphabet = b"dc04vv14dak1c1r48qa0m23vr9jy8sme".to_vec();
    for needle in [
        b"hello\n".to_vec(),
        Vec::new(),
        wrong_length,
        wrong_alphabet,
    ] {
        let req = NarRequirements::default().reference_needles(vec![NAME_HASH.into(), needle]);
        let error = repo
            .import(NarImport::new(HELLO).requirements(req.clone()))
            .await
            .err()
            .expect("import must fail");
        assert!(matches!(error, NarError::Invalid(_)), "{error:?}");
        assert!(matches!(
            ensure_nar(report.reader(), report.root(), &req).await,
            Err(NarError::Invalid(_))
        ));
        assert!(matches!(
            lookup_nar(report.reader(), report.root(), &req).await,
            Err(NarError::Invalid(_))
        ));
    }
}

#[tokio::test]
async fn maintenance_and_unknown_versions() {
    let dir = tempfile::tempdir().unwrap();
    let repo = Repository::local(dir.path()).await.unwrap();
    let report = repo.import(NarImport::new(HELLO)).await.unwrap();
    let key = identity(report.root());
    let mut unknown = key.clone();
    unknown[0] = b'x';
    let store = repo.inner.nar_store.as_ref().unwrap();
    store
        .merge(&unknown, &report.facts, store.generation().await.unwrap())
        .await
        .unwrap();
    store.remove(key.clone()).await.unwrap();
    assert!(
        lookup_nar(report.reader(), report.root(), &NarRequirements::default())
            .await
            .unwrap()
            .is_none()
    );
    ensure_nar(report.reader(), report.root(), &NarRequirements::default())
        .await
        .unwrap();
    assert_eq!(
        prune_nar_associations(&repo, None, 1024)
            .await
            .unwrap()
            .removed,
        0
    );
    drop(report);
    repo.collect().await.unwrap();
    assert_eq!(
        prune_nar_associations(&repo, None, 1024)
            .await
            .unwrap()
            .removed,
        1
    );
    assert!(store.get(&key).await.unwrap().is_none());
}

#[tokio::test]
async fn aborted_stream_never_publishes_an_association() {
    use tokio::io::AsyncWriteExt;
    // Publish per node so content is observable before the association.
    let repo = memory_with_batch_limit(1);
    let (mut writer, reader) = tokio::io::duplex(1024);
    let task = tokio::spawn({
        let repo = repo.clone();
        async move { repo.import(NarImport::new(reader)).await }
    });
    writer.write_all(HELLO).await.unwrap();
    let root = Node::File {
        digest: crate::BlobId::new(blake3::hash(b"hello\n").into()),
        size: 6,
        executable: false,
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let held = repo.retained_reader().await.unwrap();
            if held
                .object(&object_key(&root).unwrap())
                .await
                .unwrap()
                .is_some()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert!(
        repo.inner
            .nar_store
            .as_ref()
            .unwrap()
            .get(&identity(&root))
            .await
            .unwrap()
            .is_none()
    );
    drop(writer);
    repo.collect().await.unwrap();
}

fn pack_files(path: &std::path::Path, files: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            pack_files(&entry.path(), files);
        } else {
            files.push(entry.path());
        }
    }
}

#[tokio::test]
async fn silent_corruption_requires_scrub_and_known_failure_invalidates_reuse() {
    let dir = tempfile::tempdir().unwrap();
    let root;
    {
        let repo = Repository::local(dir.path()).await.unwrap();
        let report = repo.import(NarImport::new(HELLO)).await.unwrap();
        root = report.root().clone();
        publish(&repo, &report).await;
        drop(report);
        repo.flush().await.unwrap();
    }
    let mut packs = Vec::new();
    pack_files(&dir.path().join("blobs/packs"), &mut packs);
    assert!(!packs.is_empty());
    let originals: Vec<_> = packs.iter().map(|p| std::fs::read(p).unwrap()).collect();
    for (path, bytes) in packs.iter().zip(&originals) {
        std::fs::write(path, vec![0; bytes.len()]).unwrap();
    }
    let repo = Repository::local(dir.path()).await.unwrap();
    let held = repo.retained_reader().await.unwrap();
    // A cache hit is not a fresh disk audit.
    assert!(
        lookup_nar(&held, &root, &NarRequirements::default())
            .await
            .unwrap()
            .is_some()
    );
    let read_failed = match held.open_verified(&object_key(&root).unwrap()).await {
        Ok(Some(mut reader)) => reader.read_to_end(&mut Vec::new()).await.is_err(),
        Err(_) => true,
        Ok(None) => panic!("retained record disappeared"),
    };
    assert!(read_failed);
    assert!(
        scrub_nar(&held, &root, &NarRequirements::default())
            .await
            .is_err()
    );
    assert!(
        lookup_nar(&held, &root, &NarRequirements::default())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        ensure_nar(&held, &root, &NarRequirements::default())
            .await
            .is_err()
    );
    for (path, bytes) in packs.iter().zip(&originals) {
        std::fs::write(path, bytes).unwrap();
    }
    // Out-of-band restoration also needs a reopen to discard physical read
    // caches; ordinary coordinated repair owns that cache invalidation.
    drop(held);
    drop(repo);
    let repo = Repository::local(dir.path()).await.unwrap();
    let held = repo.retained_reader().await.unwrap();
    // Restoration must be verified before another successful report is minted.
    let repaired = scrub_nar(&held, &root, &NarRequirements::default())
        .await
        .unwrap();
    assert_eq!(repaired.nar_sha256(), Sha256::digest(HELLO).as_slice());
}

#[tokio::test]
async fn missing_pack_cannot_satisfy_a_cached_association() {
    let dir = tempfile::tempdir().unwrap();
    let repo = Repository::local(dir.path()).await.unwrap();
    let report = repo.import(NarImport::new(HELLO)).await.unwrap();
    // A hit leaves the payload store's witness behind; a later hit reuses it
    // only while every pack it names still exists.
    assert!(
        lookup_nar(report.reader(), report.root(), &NarRequirements::default())
            .await
            .unwrap()
            .is_some()
    );
    let store = repo.inner.nar_store.as_ref().unwrap();
    assert!(
        store
            .witness(&identity(report.root()))
            .await
            .unwrap()
            .is_some()
    );
    let mut packs = Vec::new();
    pack_files(&dir.path().join("blobs/packs"), &mut packs);
    assert!(!packs.is_empty());
    for path in packs {
        std::fs::remove_file(path).unwrap();
    }
    assert!(
        lookup_nar(report.reader(), report.root(), &NarRequirements::default())
            .await
            .is_err()
    );
    assert!(
        store
            .witness(&identity(report.root()))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn availability_witness_follows_its_association() {
    let dir = tempfile::tempdir().unwrap();
    let repo = Repository::local(dir.path()).await.unwrap();
    let report = repo.import(NarImport::new(HELLO)).await.unwrap();
    let key = identity(report.root());
    let store = repo.inner.nar_store.as_ref().unwrap();
    assert!(store.witness(&key).await.unwrap().is_none());
    lookup_nar(report.reader(), report.root(), &NarRequirements::default())
        .await
        .unwrap()
        .unwrap();
    assert!(store.witness(&key).await.unwrap().is_some());
    // Invalidation clears the witness with the facts; the next walk
    // restores it even though the association itself is gone.
    store.invalidate().await.unwrap();
    assert!(store.witness(&key).await.unwrap().is_none());
    assert!(
        lookup_nar(report.reader(), report.root(), &NarRequirements::default())
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.witness(&key).await.unwrap().is_some());
    // Orphan cleanup removes the witness once its root is collected, without
    // counting it as an association: the invalidation already removed that.
    drop(report);
    let root = Node::File {
        digest: crate::BlobId::new(blake3::hash(b"hello\n").into()),
        size: 6,
        executable: false,
    };
    repo.collect().await.unwrap();
    let cleanup = prune_nar_associations(&repo, None, 16).await.unwrap();
    assert_eq!(cleanup.removed, 0);
    assert!(store.witness(&identity(&root)).await.unwrap().is_none());
    // An ephemeral repository's payload store leaves no reusable witness.
    let memory = Repository::memory().unwrap();
    let report = memory.import(NarImport::new(HELLO)).await.unwrap();
    lookup_nar(report.reader(), report.root(), &NarRequirements::default())
        .await
        .unwrap()
        .unwrap();
    let store = memory.inner.nar_store.as_ref().unwrap();
    assert!(
        store
            .witness(&identity(report.root()))
            .await
            .unwrap()
            .is_none()
    );
}

pub(super) fn crash_checkpoint(phase: &str) {
    if std::env::var("CASITA_NAR_CRASH_PHASE").as_deref() == Ok(phase) {
        std::process::exit(71);
    }
}
#[test]
fn nar_crash_worker() {
    let Some(path) = std::env::var_os("CASITA_NAR_CRASH_ROOT") else {
        return;
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let repository = Repository::local(path).await.unwrap();
            let report = repository.import(NarImport::new(HELLO)).await.unwrap();
            publish(&repository, &report).await;
            crash_checkpoint("publication");
        });
}
#[test]
fn crash_boundaries_reopen_with_content_before_association() {
    for phase in ["content", "association", "publication"] {
        let directory = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "nar::tests::nar_crash_worker", "--nocapture"])
            .env("CASITA_NAR_CRASH_ROOT", directory.path())
            .env("CASITA_NAR_CRASH_PHASE", phase)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(71),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let repository = Repository::local(directory.path()).await.unwrap();
                let held = repository.retained_reader().await.unwrap();
                let root = Node::File {
                    digest: crate::BlobId::new(blake3::hash(b"hello\n").into()),
                    size: 6,
                    executable: false,
                };
                let found = lookup_nar(&held, &root, &NarRequirements::default())
                    .await
                    .unwrap();
                assert_eq!(found.is_some(), phase != "content");
                assert_eq!(
                    held.roots().await.unwrap().len(),
                    usize::from(phase == "publication")
                );
                let verified = scrub_nar(&held, &root, &NarRequirements::default())
                    .await
                    .unwrap();
                assert_eq!(verified.nar_sha256(), Sha256::digest(HELLO).as_slice());
            });
    }
}

#[tokio::test]
async fn known_failure_requires_native_verification_before_new_raw_trust() {
    let repo = Repository::memory().unwrap();
    repo.inner
        .nar_store
        .as_ref()
        .unwrap()
        .invalidate()
        .await
        .unwrap();
    let report = repo.import(NarImport::new(HELLO)).await.unwrap();
    assert_eq!(report.stats().encoding_passes, 1);
    assert_eq!(report.stats().hash_payload_bytes, 12);
    let next = repo.import(NarImport::new(HELLO)).await.unwrap();
    assert_eq!(next.stats().encoding_passes, 0);
}

#[tokio::test]
async fn independent_nested_fixture_measures_and_scrubs() {
    let bytes = include_bytes!("../../tests/fixtures/nar/tree.nar");
    let repo = Repository::memory().unwrap();
    let req = NarRequirements::default()
        .hash(NarHashMethod::Nar, NarHashAlgorithm::Md5)
        .hash(NarHashMethod::Git, NarHashAlgorithm::Sha1);
    let report = repo
        .import(NarImport::new(bytes.as_slice()).requirements(req.clone()))
        .await
        .unwrap();
    assert_eq!(
        hex(report.nar_sha256()),
        "0ff00ea57506ae9afb2b4a8aa1f1dad978240c83a189bad2869497fe5b026fec"
    );
    assert_eq!(report.nar_size(), bytes.len() as u64);
    let scrub = scrub_nar(report.reader(), report.root(), &req)
        .await
        .unwrap();
    assert_eq!(scrub.facts.values, report.facts.values);
    assert_eq!(scrub.stats().hash_payload_bytes, 6);
}

#[tokio::test]
async fn fragmented_reference_scanning_reaches_contents_names_and_targets() {
    let (bytes, payload_bytes) = reference_nar();
    // Three-byte reads split every needle across scan calls.
    let stream = futures::stream::iter(
        bytes
            .chunks(3)
            .map(|chunk| Ok::<_, std::io::Error>(Bytes::copy_from_slice(chunk))),
    );
    let repo = Repository::memory().unwrap();
    let req = NarRequirements::default()
        .hash(NarHashMethod::Nar, NarHashAlgorithm::Md5)
        .reference_needles(vec![
            MISSING_HASH.into(),
            CONTENT_HASH.into(),
            TARGET_HASH.into(),
            NAME_HASH.into(),
        ]);
    let report = repo
        .import(NarImport::new(tokio_util::io::StreamReader::new(stream)).requirements(req.clone()))
        .await
        .unwrap();
    assert_eq!(report.nar_sha256(), Sha256::digest(&bytes).as_slice());
    assert_eq!(report.reference_matches(), &[1, 2, 3]);
    let scrub = scrub_nar(report.reader(), report.root(), &req)
        .await
        .unwrap();
    assert_eq!(scrub.facts.values, report.facts.values);
    assert_eq!(scrub.stats().hash_payload_bytes, payload_bytes);
    let changed = NarRequirements::default()
        .reference_needles(vec![CONTENT_HASH.into(), MISSING_HASH.into()]);
    let added = ensure_nar(report.reader(), report.root(), &changed)
        .await
        .unwrap();
    assert_eq!(added.reference_matches(), &[0]);
    assert_eq!(added.stats().encoding_passes, 1);
    assert!(
        ensure_nar(report.reader(), report.root(), &req)
            .await
            .unwrap()
            .stats()
            .association_hit
    );
}

#[tokio::test]
async fn hostile_depth_is_rejected_on_the_default_stack() {
    let repo = Repository::memory().unwrap();
    let mut bytes = HELLO.to_vec();
    for _ in 0..63 {
        bytes = directory(&[(b"a", &bytes)]);
    }
    let report = repo.import(NarImport::new(bytes.as_slice())).await.unwrap();
    let scrub = scrub_nar(report.reader(), report.root(), &NarRequirements::default())
        .await
        .unwrap();
    assert_eq!(scrub.nar_sha256(), report.nar_sha256());
    bytes = directory(&[(b"a", &bytes)]);
    assert!(repo.import(NarImport::new(bytes.as_slice())).await.is_err());
}

#[tokio::test]
async fn nix_archive_bridge_streams_large_files_and_rejects_late_errors() {
    let payload: Vec<u8> = (0..(3 * 64 * 1024 + 17)).map(|i| (i % 251) as u8).collect();
    let mut archive = Vec::new();
    nix_archive::nar::encode_regular(&mut archive, &payload, true).unwrap();
    let repo = Repository::memory().unwrap();
    let requirements =
        NarRequirements::default().hash(NarHashMethod::Flat, NarHashAlgorithm::Sha256);
    let report = tokio::time::timeout(
        Duration::from_secs(10),
        repo.import(NarImport::new(archive.as_slice()).requirements(requirements)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(report.nar_sha256(), Sha256::digest(&archive).as_slice());
    assert_eq!(
        report
            .hash(NarHashMethod::Flat, NarHashAlgorithm::Sha256)
            .unwrap(),
        Sha256::digest(&payload).as_slice()
    );
    assert_eq!(report.stats().hash_payload_bytes, payload.len() as u64);
    let scrub = scrub_nar(report.reader(), report.root(), &NarRequirements::default())
        .await
        .unwrap();
    assert_eq!(scrub.nar_sha256(), report.nar_sha256());

    // Exercise decoder failure after payload staging and while the input pump
    // still has more than a pipe's capacity left to send.
    archive.extend(vec![0; 2 * 64 * 1024]);
    let failed = Repository::memory().unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        failed.import(NarImport::new(archive.as_slice())),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    assert!(
        failed
            .inner
            .nar_store
            .as_ref()
            .unwrap()
            .get(&identity(report.root()))
            .await
            .unwrap()
            .is_none()
    );
}

#[cfg(feature = "experimental")]
/// A metadata backend that keeps no verification facts, as a remote or
/// custom composition would: every caller measures again.
struct FactlessMetadataStore(crate::MemoryMetadataStore);

#[cfg(feature = "experimental")]
#[async_trait]
impl MetadataStore for FactlessMetadataStore {
    async fn try_collection_lease(
        &self,
    ) -> Result<Option<crate::metadata::RepositoryLease>, crate::metadata::MetadataError> {
        self.0.try_collection_lease().await
    }
    fn coordinates_payload_catalog(&self) -> bool {
        self.0.coordinates_payload_catalog()
    }
    fn supports_metadata_records(&self) -> bool {
        self.0.supports_metadata_records()
    }
    async fn commit_checked(
        &self,
        mutation: crate::metadata::MetadataMutation,
    ) -> Result<crate::metadata::CommitResult, crate::metadata::MetadataError> {
        self.0.commit_checked(mutation).await
    }
    async fn pin_store(
        &self,
    ) -> Result<std::sync::Arc<dyn crate::metadata::PinStore>, crate::metadata::MetadataError> {
        self.0.pin_store().await
    }
    async fn snapshot(
        &self,
    ) -> Result<std::sync::Arc<dyn crate::metadata::MetadataSnapshot>, crate::metadata::MetadataError>
    {
        self.0.snapshot().await
    }
    async fn commit(
        &self,
        expected: &crate::RepositoryRevision,
        mutation: crate::metadata::MetadataMutation,
    ) -> Result<crate::metadata::CommitResult, crate::metadata::MetadataError> {
        self.0.commit(expected, mutation).await
    }
}

#[cfg(feature = "experimental")]
#[tokio::test]
async fn generic_import_dispatch_and_unaudited_backend_fallback() {
    let repository = CoreRepository::memory().unwrap();
    let report = repository.import(NarImport::new(HELLO)).await.unwrap();
    assert_eq!(report.stats().encoding_passes, 0);
    let source = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("hello"), b"hello\n").unwrap();
    let native = repository
        .import(FilesystemNarImport::new(source.path()))
        .await
        .unwrap();
    assert_eq!(native.stats().encoding_passes, 1);
    let custom = CoreRepository::new(
        crate::MemoryBlobStore::new(),
        FactlessMetadataStore(crate::MemoryMetadataStore::new().unwrap()),
    );
    let report = custom.import(NarImport::new(HELLO)).await.unwrap();
    for _ in 0..2 {
        let measured = ensure_nar(report.reader(), report.root(), &NarRequirements::default())
            .await
            .unwrap();
        assert!(!measured.stats().association_hit);
        assert_eq!(measured.stats().hash_payload_bytes, 6);
    }
}

#[tokio::test]
async fn invalid_caller_root_does_not_invalidate_verified_associations() {
    let repo = Repository::memory().unwrap();
    let bytes = directory(&[]);
    let report = repo.import(NarImport::new(bytes.as_slice())).await.unwrap();
    let Node::Directory { digest, size } = report.root() else {
        unreachable!()
    };
    let incorrect = Node::Directory {
        digest: *digest,
        size: size + 1,
    };
    assert!(
        ensure_nar(report.reader(), &incorrect, &NarRequirements::default())
            .await
            .is_err()
    );
    assert!(
        lookup_nar(report.reader(), report.root(), &NarRequirements::default())
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn only_damaged_reads_invalidate_associations() {
    use std::task::Poll;
    async fn deliver(health: &mut store::ReadHealth) -> std::io::Error {
        std::future::poll_fn(|cx| match health.poll(cx) {
            Some(Poll::Ready(result)) => Poll::Ready(result.unwrap_err()),
            Some(Poll::Pending) => Poll::Pending,
            None => panic!("no queued error"),
        })
        .await
    }
    let repo = Repository::memory().unwrap();
    let report = repo.import(NarImport::new(HELLO)).await.unwrap();
    let store = repo.inner.nar_store.clone();
    let mut health = store::ReadHealth::new(store);
    for kind in [
        std::io::ErrorKind::TimedOut,
        std::io::ErrorKind::Interrupted,
        std::io::ErrorKind::Other,
    ] {
        health.failed(std::io::Error::from(kind));
        assert_eq!(deliver(&mut health).await.kind(), kind);
        assert!(
            lookup_nar(report.reader(), report.root(), &NarRequirements::default())
                .await
                .unwrap()
                .is_some(),
            "{kind:?} is not evidence of damaged content"
        );
    }
    let corrupt = std::io::Error::other(bao_tree::io::DecodeError::LeafHashMismatch(
        bao_tree::ChunkNum(0),
    ));
    health.failed(corrupt);
    assert_eq!(deliver(&mut health).await.kind(), std::io::ErrorKind::Other);
    assert!(
        lookup_nar(report.reader(), report.root(), &NarRequirements::default())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn clean_physical_audit_lifts_native_audit_requirement() {
    let dir = tempfile::tempdir().unwrap();
    let core = crate::repository::Repository::local(dir.path())
        .await
        .unwrap();
    let repo = Repository {
        inner: core.clone().into_builtin(),
    };
    let store = repo.inner.nar_store.as_ref().unwrap();
    store.invalidate().await.unwrap();
    let report = repo.import(NarImport::new(HELLO)).await.unwrap();
    assert_eq!(report.stats().encoding_passes, 1);
    let scan = core.fsck_repair(None).await.unwrap();
    assert!(scan.findings.is_empty());
    let next = repo.import(NarImport::new(EXEC)).await.unwrap();
    assert_eq!(next.stats().encoding_passes, 0);
    assert_eq!(next.stats().hash_payload_bytes, 6);
    assert!(
        lookup_nar(report.reader(), report.root(), &NarRequirements::default())
            .await
            .unwrap()
            .is_some(),
        "restoration keeps facts recorded after the failure"
    );
}

fn memory_with_batch_limit(max_batch_objects: usize) -> Repository {
    let core = crate::repository::Repository::with_formats(
        crate::MemoryBlobStore::new(),
        crate::MemoryMetadataStore::new().unwrap(),
        crate::FormatRegistry::builtin(),
        crate::FormatLimits {
            max_batch_objects,
            ..Default::default()
        },
    );
    Repository {
        inner: core.into_builtin(),
    }
}

#[tokio::test]
async fn raw_intake_publishes_in_bounded_batches() {
    let (bytes, _) = reference_nar();
    for limit in [1, 2, 4096] {
        let repo = memory_with_batch_limit(limit);
        // Duplicate needles report every index.
        let req = NarRequirements::default()
            .hash(NarHashMethod::Git, NarHashAlgorithm::Sha1)
            .reference_needles(vec![CONTENT_HASH.into(), CONTENT_HASH.into()]);
        let report = repo
            .import(NarImport::new(bytes.as_slice()).requirements(req.clone()))
            .await
            .unwrap();
        assert_eq!(report.nar_sha256(), Sha256::digest(&bytes).as_slice());
        assert_eq!(report.reference_matches(), &[0, 1]);
        let scrub = scrub_nar(report.reader(), report.root(), &req)
            .await
            .unwrap();
        assert_eq!(scrub.facts.values, report.facts.values);
        assert!(matches!(
            report
                .reader()
                .hold
                .verify_closure_incremental(&object_key(report.root()).unwrap())
                .await
                .unwrap(),
            crate::ClosureStatus::Complete { .. }
        ));
    }
}

struct FailAfter<'a> {
    bytes: &'a [u8],
    kind: std::io::ErrorKind,
}
impl AsyncRead for FailAfter<'_> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.bytes.is_empty() {
            return Poll::Ready(Err(std::io::Error::from(self.kind)));
        }
        let n = self.bytes.len().min(buf.remaining());
        buf.put_slice(&self.bytes[..n]);
        self.bytes = &self.bytes[n..];
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn first_failing_stage_names_the_intake_error() {
    use crate::RetryDisposition;
    let payload: Vec<u8> = (0..(2 * 64 * 1024 + 5)).map(|i| (i % 253) as u8).collect();
    let mut big = Vec::new();
    nix_archive::nar::encode_regular(&mut big, &payload, false).unwrap();
    // The order violation is found at the second name, while more than a
    // pipe's capacity of that entry's contents is still waiting to be pumped.
    let archive = directory(&[(b"z", HELLO), (b"a", &big)]);
    for _ in 0..8 {
        let repo = Repository::memory().unwrap();
        let error = repo
            .import(NarImport::new(archive.as_slice()))
            .await
            .err()
            .expect("import must fail");
        assert!(matches!(error, NarError::Invalid(_)), "{error:?}");
        assert_eq!(error.retry_disposition(), RetryDisposition::Never);
    }
    // An input read failure is the cause even though the decoder then sees a
    // truncated archive and the consumer never receives a root.
    let archive = directory(&[(b"a", &big)]);
    let repo = Repository::memory().unwrap();
    let error = repo
        .import(NarImport::new(FailAfter {
            bytes: &archive[..archive.len() / 2],
            kind: std::io::ErrorKind::TimedOut,
        }))
        .await
        .err()
        .expect("import must fail");
    assert!(
        matches!(&error, NarError::Io(error) if error.kind() == std::io::ErrorKind::TimedOut),
        "{error:?}"
    );
}

#[tokio::test]
async fn root_kind_is_checked_before_anything_is_staged() {
    let repo = memory_with_batch_limit(1);
    let archive = directory(&[(b"a", HELLO)]);
    let flat = NarRequirements::default().hash(NarHashMethod::Flat, NarHashAlgorithm::Sha256);
    let error = repo
        .import(NarImport::new(archive.as_slice()).requirements(flat))
        .await
        .err()
        .expect("import must fail");
    assert!(matches!(error, NarError::Invalid(_)), "{error:?}");
    let file = Node::File {
        digest: crate::BlobId::new(blake3::hash(b"hello\n").into()),
        size: 6,
        executable: false,
    };
    let held = repo.retained_reader().await.unwrap();
    assert!(
        held.object(&object_key(&file).unwrap())
            .await
            .unwrap()
            .is_none(),
        "the child file was staged despite the root check"
    );
    let text = NarRequirements::default().hash(NarHashMethod::Text, NarHashAlgorithm::Sha256);
    let error = repo
        .import(NarImport::new(EXEC).requirements(text))
        .await
        .err()
        .expect("import must fail");
    assert!(matches!(error, NarError::Invalid(_)), "{error:?}");
}

async fn invalidation_after_the_check_refuses_the_association(repo: &Repository) {
    let report = repo.import(NarImport::new(HELLO)).await.unwrap();
    let store = repo.inner.nar_store.as_ref().unwrap();
    let key = identity(report.root());
    let generation = store.generation().await.unwrap();
    store.invalidate().await.unwrap();
    // The check preceded the invalidation: the write is refused.
    assert!(
        store
            .merge(&key, &report.facts, generation)
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.get(&key).await.unwrap().is_none());
    // A check made after it is honoured.
    assert!(
        store
            .merge(&key, &report.facts, store.generation().await.unwrap())
            .await
            .unwrap()
            .is_some()
    );
    assert!(store.get(&key).await.unwrap().is_some());
    // An audit that started before the invalidation cannot lift the requirement.
    assert!(!store.restore(generation).await.unwrap());
    assert!(store.requires_native_audit().await.unwrap());
    assert!(
        store
            .restore(store.generation().await.unwrap())
            .await
            .unwrap()
    );
    assert!(!store.requires_native_audit().await.unwrap());
    // Without the marker the generation is irrelevant.
    assert!(
        store
            .merge(&key, &report.facts, generation)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn invalidation_after_the_check_refuses_the_association_in_memory() {
    invalidation_after_the_check_refuses_the_association(&Repository::memory().unwrap()).await;
}

#[tokio::test]
async fn invalidation_after_the_check_refuses_the_association_locally() {
    let dir = tempfile::tempdir().unwrap();
    let repo = Repository::local(dir.path()).await.unwrap();
    invalidation_after_the_check_refuses_the_association(&repo).await;
}

#[tokio::test]
async fn invalidation_generation_is_shared_by_independent_local_handles() {
    let dir = tempfile::tempdir().unwrap();
    let first = Repository::local(dir.path()).await.unwrap();
    let second = Repository::local(dir.path()).await.unwrap();
    let report = first.import(NarImport::new(HELLO)).await.unwrap();
    let key = identity(report.root());
    let a = first.inner.nar_store.as_ref().unwrap();
    let b = second.inner.nar_store.as_ref().unwrap();
    assert!(!std::sync::Arc::ptr_eq(a, b));

    // Deterministically interleave B's invalidation between A's audit-marker
    // check and its merge, just as raw intake can do.
    let before = a.generation().await.unwrap();
    assert!(!a.requires_native_audit().await.unwrap());
    b.invalidate().await.unwrap();
    assert!(
        a.merge(&key, &report.facts, before)
            .await
            .unwrap()
            .is_none()
    );
    assert!(a.get(&key).await.unwrap().is_none());
    assert!(!a.restore(before).await.unwrap());
    assert!(b.requires_native_audit().await.unwrap());

    let after = a.generation().await.unwrap();
    assert_eq!(after, before + 1);
    assert_eq!(after, b.generation().await.unwrap());
    // Another invalidation while the marker is already present must also
    // supersede an in-progress audit, including one on the other handle.
    a.invalidate().await.unwrap();
    assert!(!b.restore(after).await.unwrap());
    let latest = b.generation().await.unwrap();
    assert_eq!(latest, after + 1);
    assert!(b.restore(latest).await.unwrap());
    assert!(!a.requires_native_audit().await.unwrap());
    // Clearing the audit marker must retain the generation for future audits.
    drop(report);
    drop(first);
    drop(second);
    let reopened = Repository::local(dir.path()).await.unwrap();
    let store = reopened.inner.nar_store.as_ref().unwrap();
    assert_eq!(store.generation().await.unwrap(), latest);
    store.invalidate().await.unwrap();
    assert!(!store.restore(latest).await.unwrap());
    assert_eq!(store.generation().await.unwrap(), latest + 1);
}

#[test]
fn damaged_read_invalidates_without_a_runtime() {
    let store = store::NarStore::memory();
    let mut health = store::ReadHealth::new(Some(store.clone()));
    health.failed(std::io::Error::other(
        bao_tree::io::DecodeError::LeafHashMismatch(bao_tree::ChunkNum(0)),
    ));
    let error = futures::executor::block_on(std::future::poll_fn(|cx| match health.poll(cx) {
        Some(Poll::Ready(result)) => Poll::Ready(result.unwrap_err()),
        Some(Poll::Pending) => Poll::Pending,
        None => panic!("no queued error"),
    }));
    assert_eq!(error.kind(), std::io::ErrorKind::Other);
    assert!(futures::executor::block_on(store.requires_native_audit()).unwrap());
}

#[tokio::test]
async fn cached_measurement_skips_generation_but_fences_partial_facts() {
    use crate::metadata::{FactsEdit, MetadataError, MetadataStore, VerificationFacts};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Return a snapshot of the association, then invalidate it through an
    // independently opened handle before the caller can use that snapshot.
    struct InterleavedFacts {
        inner: Arc<dyn VerificationFacts>,
        other: Arc<store::NarStore>,
        key: Vec<u8>,
        reads: AtomicUsize,
        invalidate_on: AtomicUsize,
        generations: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl VerificationFacts for InterleavedFacts {
        fn scope(&self) -> Option<PathBuf> {
            self.inner.scope()
        }
        async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetadataError> {
            let value = self.inner.get(key).await?;
            if key == b"nar-invalidation-generation" {
                self.generations.fetch_add(1, Ordering::SeqCst);
            }
            if key == self.key
                && self.reads.fetch_add(1, Ordering::SeqCst) + 1
                    == self.invalidate_on.load(Ordering::SeqCst)
            {
                self.other.invalidate().await.unwrap();
            }
            Ok(value)
        }
        async fn edit(&self, keys: Vec<Vec<u8>>, edit: FactsEdit) -> Result<(), MetadataError> {
            self.inner.edit(keys, edit).await
        }
        async fn clear(
            &self,
            tombstone: Vec<u8>,
            generation: Vec<u8>,
        ) -> Result<(), MetadataError> {
            self.inner.clear(tombstone, generation).await
        }
        async fn page(&self, after: Vec<u8>, limit: usize) -> Result<Vec<Vec<u8>>, MetadataError> {
            self.inner.page(after, limit).await
        }
    }

    for invalidate_on in [1, 2] {
        let data = tempfile::tempdir().unwrap();
        let mut repo = Repository::local(data.path()).await.unwrap();
        let other = Repository::local(data.path()).await.unwrap();
        let report = repo
            .import(NarImport::new(HELLO).requirements(
                NarRequirements::default().hash(NarHashMethod::Nar, NarHashAlgorithm::Sha512),
            ))
            .await
            .unwrap();
        let key = identity(report.root());
        let facts = Arc::new(InterleavedFacts {
            inner: repo.inner.metadata().verification_facts().unwrap(),
            other: other.inner.nar_store.as_ref().unwrap().clone(),
            key: key.clone(),
            reads: AtomicUsize::new(0),
            invalidate_on: AtomicUsize::new(0),
            generations: AtomicUsize::new(0),
        });
        repo.inner.nar_store = Some(store::NarStore::new(facts.clone()));
        let reader = repo.retained_reader().await.unwrap();
        let cached = ensure_nar(&reader, report.root(), &NarRequirements::default())
            .await
            .unwrap();
        assert!(cached.stats().association_hit);
        assert_eq!(cached.nar_sha256(), report.nar_sha256());
        assert_eq!(facts.generations.load(Ordering::SeqCst), 0);

        facts.reads.store(0, Ordering::SeqCst);
        facts.invalidate_on.store(invalidate_on, Ordering::SeqCst);
        let request =
            NarRequirements::default().reference_needles(vec![MISSING_HASH.as_bytes().to_vec()]);
        let measured = ensure_nar(&reader, report.root(), &request).await.unwrap();
        assert!(!measured.stats().association_hit);
        assert_eq!(measured.nar_sha256(), report.nar_sha256());
        assert_eq!(measured.stats().encoding_passes, 1);
        assert_eq!(measured.stats().hash_payload_bytes, 6);
        assert!(facts.reads.load(Ordering::SeqCst) >= invalidate_on);
        assert_eq!(facts.other.generation().await.unwrap(), 1);
        let cached = facts.other.get(&key).await.unwrap();
        if invalidate_on == 1 {
            // Invalidation preceded generation capture. Only newly measured
            // facts may return: the old, unrequested SHA-512 is a canary.
            let cached = cached.expect("newly measured facts are reusable");
            assert!(cached.complete(&request));
            assert!(!cached.values.contains_key(&vec![0, 3]));
        } else {
            // Invalidation followed generation capture and the reload.
            // The shared generation must still reject the merge.
            assert!(cached.is_none());
        }
        assert!(facts.other.requires_native_audit().await.unwrap());
    }
}

#[path = "../../../../benchmarks/fixtures/nar_import.rs"]
mod import_fixture;

#[tokio::test]
async fn concurrent_intake_preserves_order_across_windows_and_pipe_boundaries() {
    for (files, size) in [
        (15, 0),
        (16, 1024),
        (17, 65535),
        (17, 65536),
        (33, 65537),
        (17, 131073),
    ] {
        let bytes = import_fixture::archive(files, size);
        // A directory ending after a full staging window, followed by a sibling,
        // catches accidental attachment of completed files to the wrong frame.
        let archive = directory(&[(b"a", &bytes), (b"b", HELLO)]);
        let repo = memory_with_batch_limit(3);
        let req = NarRequirements::default().hash(NarHashMethod::Git, NarHashAlgorithm::Sha256);
        let report = tokio::time::timeout(
            Duration::from_secs(30),
            repo.import(NarImport::new(archive.as_slice()).requirements(req.clone())),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(report.nar_sha256(), Sha256::digest(&archive).as_slice());
        assert_eq!(report.stats().hash_payload_bytes, (files * size + 6) as u64);
        let scrub = scrub_nar(report.reader(), report.root(), &req)
            .await
            .unwrap();
        assert_eq!(scrub.facts.values, report.facts.values);
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn concurrent_raw_intake_groups_durable_pin_admissions() {
    let data = tempfile::tempdir().unwrap();
    let repo = Repository::local(data.path()).await.unwrap();
    let pins = crate::metadata::FilePinStore::new(data.path().join("casita.sqlite.online-pins"));
    let before = pins.test_stats();
    let bytes = import_fixture::archive(256, 1024);
    let report = repo.import(NarImport::new(bytes.as_slice())).await.unwrap();
    let after = pins.test_stats();
    let syncs = after["journal_syncs"] - before["journal_syncs"];
    eprintln!("256-file raw NAR: {syncs} journal syncs");
    assert!(syncs < 256, "expected fewer syncs than files, got {syncs}");
    assert_eq!(report.nar_sha256(), Sha256::digest(&bytes).as_slice());
    let scrub = scrub_nar(report.reader(), report.root(), &NarRequirements::default())
        .await
        .unwrap();
    assert_eq!(scrub.nar_sha256(), report.nar_sha256());
}

#[tokio::test]
async fn concurrent_intake_input_failure_does_not_hang_or_create_an_association() {
    let bytes = import_fixture::archive(64, 65537);
    let repo = Repository::memory().unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(30),
        repo.import(NarImport::new(FailAfter {
            bytes: &bytes[..bytes.len() / 2],
            kind: std::io::ErrorKind::TimedOut,
        })),
    )
    .await
    .unwrap()
    .err()
    .expect("input must fail");
    assert!(matches!(error, NarError::Io(error) if error.kind() == std::io::ErrorKind::TimedOut));
    // Cleanup must settle pending stages and leave the repository usable.
    repo.collect().await.unwrap();
    let report = repo.import(NarImport::new(bytes.as_slice())).await.unwrap();
    assert!(!report.stats().association_hit);
    let scrub = scrub_nar(report.reader(), report.root(), &NarRequirements::default())
        .await
        .unwrap();
    assert_eq!(scrub.nar_sha256(), report.nar_sha256());
}

#[tokio::test]
async fn cancelled_concurrent_intake_releases_staged_files() {
    use tokio::io::AsyncWriteExt;
    for size in [1024, 65536, 65537] {
        let bytes = import_fixture::archive(64, size);
        let source = Repository::memory().unwrap();
        let expected = source
            .import(NarImport::new(bytes.as_slice()))
            .await
            .unwrap();
        let root = expected.root().clone();
        let repo = memory_with_batch_limit(1);
        let (mut writer, reader) = tokio::io::duplex(1024);
        let task = tokio::spawn({
            let repo = repo.clone();
            async move { repo.import(NarImport::new(reader)).await }
        });
        // Leave the final directory frame incomplete with multiple file stages
        // already through the bounded pipeline.
        tokio::time::timeout(
            Duration::from_secs(30),
            writer.write_all(&bytes[..bytes.len() - 16]),
        )
        .await
        .unwrap()
        .unwrap();
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        drop(writer);
        assert!(
            repo.inner
                .nar_store
                .as_ref()
                .unwrap()
                .get(&identity(&root))
                .await
                .unwrap()
                .is_none()
        );
        repo.collect().await.unwrap();
        let held = repo.retained_reader().await.unwrap();
        let hash = blake3::hash(&0u64.to_le_bytes());
        let payload: Vec<_> = (0..size).map(|i| hash.as_bytes()[i % 32]).collect();
        let key = ObjectKey::blob(crate::BlobId::new(blake3::hash(&payload).into()));
        assert!(
            held.object(&key).await.unwrap().is_none(),
            "cancelled stages retained a file after collection"
        );
    }
}

#[tokio::test]
async fn concurrent_local_intake_drains_stages_before_directory_publication() {
    for files in [15, 16, 17] {
        let archive = import_fixture::nested_archive(files, 1024);
        let data = tempfile::tempdir().unwrap();
        let repo = Repository::local(data.path()).await.unwrap();
        let report = tokio::time::timeout(
            Duration::from_secs(15),
            repo.import(NarImport::new(archive.as_slice())),
        )
        .await
        .expect("concurrent stages must not block directory pin admission")
        .unwrap();
        assert_eq!(report.nar_sha256(), Sha256::digest(&archive).as_slice());
        let scrub = scrub_nar(report.reader(), report.root(), &NarRequirements::default())
            .await
            .unwrap();
        assert_eq!(scrub.nar_sha256(), report.nar_sha256());
    }
}

#[tokio::test]
async fn packed_outboard_roots_survive_reopen_and_collection() {
    for (files, size) in [(33, 16385), (2, 1064961)] {
        let directory = tempfile::tempdir().unwrap();
        let bytes = import_fixture::archive(files, size);
        let repo = Repository::local(directory.path()).await.unwrap();
        let report = repo.import(NarImport::new(bytes.as_slice())).await.unwrap();
        assert!(directory.path().join("blobs/bao-packs").exists());
        assert!(!directory.path().join("blobs/bao").exists());
        publish(&repo, &report).await;
        repo.collect().await.unwrap();
        let scrub = scrub_nar(report.reader(), report.root(), &NarRequirements::default())
            .await
            .unwrap();
        assert_eq!(scrub.nar_sha256(), Sha256::digest(&bytes).as_slice());
        drop(scrub);
        let root = report.root().clone();
        drop(report);
        repo.flush().await.unwrap();
        drop(repo);
        let repo = Repository::local(directory.path()).await.unwrap();
        let reader = repo.retained_reader().await.unwrap();
        let report = lookup_nar(&reader, &root, &NarRequirements::default())
            .await
            .unwrap()
            .unwrap();
        drop(reader);
        assert!(report.stats().association_hit);
        repo.collect().await.unwrap();
        let scrub = scrub_nar(report.reader(), report.root(), &NarRequirements::default())
            .await
            .unwrap();
        assert_eq!(scrub.nar_sha256(), Sha256::digest(&bytes).as_slice());
        drop(scrub);
        repo.commit(
            vec![],
            vec![MetadataChange::RemoveRoot {
                name: RootName::try_from("saved").unwrap(),
            }],
        )
        .await
        .unwrap();
        repo.collect().await.unwrap();
        let scrub = scrub_nar(report.reader(), report.root(), &NarRequirements::default())
            .await
            .unwrap();
        assert_eq!(scrub.nar_sha256(), Sha256::digest(&bytes).as_slice());
        drop(scrub);
        drop(report);
        repo.collect().await.unwrap();
        let mut remaining = Vec::new();
        pack_files(&directory.path().join("blobs/bao-packs"), &mut remaining);
        assert!(
            remaining.is_empty(),
            "unreachable packed outboards remained: {remaining:?}"
        );
    }
}

#[tokio::test]
async fn batched_directories_preserve_postorder_and_large_directory_progress() {
    let mut archives = Vec::new();
    for count in [15, 16, 17] {
        archives.push(import_fixture::directory_archive(count, 1, 32));
        archives.push(import_fixture::nested_directories(count));
    }
    archives.push(import_fixture::directory_archive(17, 0, 0));
    for links in [63, 64, 65] {
        archives.push(import_fixture::directory_archive(3, links, 4095));
    }
    for archive in archives {
        for batch_limit in [1, 3, 1024] {
            let repo = memory_with_batch_limit(batch_limit);
            let req = NarRequirements::default().hash(NarHashMethod::Git, NarHashAlgorithm::Sha256);
            let report = tokio::time::timeout(
                Duration::from_secs(30),
                repo.import(NarImport::new(archive.as_slice()).requirements(req.clone())),
            )
            .await
            .unwrap()
            .unwrap();
            let scrub = scrub_nar(report.reader(), report.root(), &req)
                .await
                .unwrap();
            assert_eq!(report.nar_sha256(), Sha256::digest(&archive).as_slice());
            assert_eq!(scrub.facts.values, report.facts.values);
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn directory_intake_groups_durable_pin_admissions() {
    let data = tempfile::tempdir().unwrap();
    let repo = Repository::local(data.path()).await.unwrap();
    let pins = crate::metadata::FilePinStore::new(data.path().join("casita.sqlite.online-pins"));
    let before = pins.test_stats();
    let bytes = import_fixture::directory_archive(256, 1, 32);
    let report = repo.import(NarImport::new(bytes.as_slice())).await.unwrap();
    let after = pins.test_stats();
    let syncs = after["journal_syncs"] - before["journal_syncs"];
    eprintln!("256-directory raw NAR: {syncs} journal syncs");
    assert!(syncs < 512, "directory admissions did not group: {syncs}");
    assert_eq!(report.nar_sha256(), Sha256::digest(&bytes).as_slice());
    let scrub = scrub_nar(report.reader(), report.root(), &NarRequirements::default())
        .await
        .unwrap();
    assert_eq!(scrub.nar_sha256(), report.nar_sha256());
}

#[tokio::test]
async fn directory_intake_publishes_before_more_input_and_cleans_up_on_abort() {
    use tokio::io::AsyncWriteExt;
    let bytes = import_fixture::directory_archive(17, 1, 32);
    let mut target = b"00000000-00000000".to_vec();
    target.resize(32, b'x');
    let child = Directory::try_from_iter([(
        PathComponent::try_from("link-00000000").unwrap(),
        Node::Symlink {
            target: SymlinkTarget::try_from(Bytes::from(target)).unwrap(),
        },
    )])
    .unwrap();
    let key = object_key(&Node::Directory {
        digest: child.digest(),
        size: child.size(),
    })
    .unwrap();
    let repo = memory_with_batch_limit(1);
    let (mut writer, reader) = tokio::io::duplex(1024);
    let task = tokio::spawn({
        let repo = repo.clone();
        async move { repo.import(NarImport::new(reader)).await }
    });
    tokio::time::timeout(Duration::from_secs(30), async {
        writer.write_all(&bytes[..bytes.len() - 16]).await.unwrap();
        loop {
            let held = repo.retained_reader().await.unwrap();
            if held.object(&key).await.unwrap().is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("completed directory must publish while source remains open");
    // Collection during intake must retain its already-published child.
    repo.collect().await.unwrap();
    assert!(
        repo.retained_reader()
            .await
            .unwrap()
            .object(&key)
            .await
            .unwrap()
            .is_some()
    );
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    drop(writer);
    repo.collect().await.unwrap();
    assert!(
        repo.retained_reader()
            .await
            .unwrap()
            .object(&key)
            .await
            .unwrap()
            .is_none()
    );
    let error = repo
        .import(NarImport::new(FailAfter {
            bytes: &bytes[..bytes.len() - 16],
            kind: std::io::ErrorKind::TimedOut,
        }))
        .await
        .err()
        .expect("truncated input must fail");
    assert!(matches!(error, NarError::Io(error) if error.kind() == std::io::ErrorKind::TimedOut));
    repo.collect().await.unwrap();
    let report = repo.import(NarImport::new(bytes.as_slice())).await.unwrap();
    assert!(!report.stats().association_hit);
    assert_eq!(report.nar_sha256(), Sha256::digest(&bytes).as_slice());
}

#[tokio::test]
async fn failed_directory_stage_releases_siblings_and_leaves_intake_usable() {
    let core = crate::repository::Repository::with_formats(
        crate::MemoryBlobStore::new(),
        crate::MemoryMetadataStore::new().unwrap(),
        crate::FormatRegistry::builtin(),
        crate::FormatLimits {
            max_metadata_bytes: 32,
            ..Default::default()
        },
    );
    let repo = Repository {
        inner: core.into_builtin(),
    };
    let bytes = import_fixture::directory_archive(17, 1, 32);
    // Directory payloads are written, then rejected by their format limit.
    // Dropping the remaining staging futures must release their protection.
    let error = tokio::time::timeout(
        Duration::from_secs(30),
        repo.import(NarImport::new(bytes.as_slice())),
    )
    .await
    .unwrap()
    .err()
    .expect("directory limit must reject staging");
    assert!(error.to_string().contains("32"), "{error}");
    repo.collect().await.unwrap();
    let report = repo.import(NarImport::new(HELLO)).await.unwrap();
    assert_eq!(report.nar_sha256(), Sha256::digest(HELLO).as_slice());
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn repeated_nar_intake_reports_maintenance_syncs() {
    for imports in [15, 16, 17, 18, 19, 32] {
        let data = tempfile::tempdir().unwrap();
        let repo = Repository::local(data.path()).await.unwrap();
        let pins =
            crate::metadata::FilePinStore::new(data.path().join("casita.sqlite.online-pins"));
        let before = pins.test_stats()["journal_syncs"];
        let mut held = Vec::new();
        for bytes in import_fixture::sequence(imports) {
            let report = repo.import(NarImport::new(bytes.as_slice())).await.unwrap();
            assert!(!report.stats().association_hit);
            assert_eq!(report.nar_sha256(), Sha256::digest(&bytes).as_slice());
            let scrub = scrub_nar(report.reader(), report.root(), &NarRequirements::default())
                .await
                .unwrap();
            assert_eq!(scrub.nar_sha256(), report.nar_sha256());
            held.push(report);
        }
        let syncs = pins.test_stats()["journal_syncs"] - before;
        eprintln!("{imports} retained NAR imports: {syncs} journal syncs");
        drop(held);
        crate::flush_repository_leases().await.unwrap();
        repo.collect().await.unwrap();
    }
}
