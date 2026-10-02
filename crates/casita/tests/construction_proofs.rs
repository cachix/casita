#![cfg(all(feature = "native", feature = "experimental"))]

use casita::experimental::{
    BlobFormat, DirectLinkView, DirectoryFormat, FormatError, FormatLimits, FormatRegistry,
    MemoryBlobStore, MemoryMetadataStore, MetadataStore, ObjectFormat, Repository,
    VerificationContext, VerifiedObject,
};
#[cfg(feature = "git")]
use casita::experimental::{GitNativeObjectFormat, GitObjectFormat, GitObjectKind, GitViewFormat};
use casita::import::FilesystemImport;
#[cfg(feature = "git")]
use casita::import::GitImport;
use casita::{NamespaceId, ObjectRecord};
#[cfg(feature = "git")]
use std::io::Write;
#[cfg(feature = "git")]
use std::process::{Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct CheckedFormat {
    inner: Arc<dyn ObjectFormat>,
    calls: Arc<AtomicUsize>,
    reject: bool,
}

#[async_trait::async_trait]
impl ObjectFormat for CheckedFormat {
    fn namespace(&self) -> &NamespaceId {
        self.inner.namespace()
    }

    async fn verify(
        &self,
        context: VerificationContext<'_>,
        limits: &FormatLimits,
    ) -> Result<VerifiedObject, FormatError> {
        self.inner.verify(context, limits).await
    }

    async fn verify_links(
        &self,
        context: VerificationContext<'_>,
        object: &ObjectRecord,
        links: &dyn DirectLinkView,
        limits: &FormatLimits,
    ) -> Result<(), FormatError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.reject {
            Err(FormatError::InvalidPayload {
                namespace: self.namespace().clone(),
                message: "custom publication rule rejected".into(),
            })
        } else {
            self.inner
                .verify_links(context, object, links, limits)
                .await
        }
    }
}

#[cfg(feature = "git")]
fn source(format: &str) -> (tempfile::TempDir, String) {
    let directory = tempfile::tempdir().unwrap();
    let init = Command::new("git")
        .args([
            "init",
            "--bare",
            "--quiet",
            &format!("--object-format={format}"),
        ])
        .arg(directory.path())
        .output()
        .unwrap();
    assert!(
        init.status.success(),
        "{}",
        String::from_utf8_lossy(&init.stderr)
    );
    let mut child = Command::new("git")
        .arg("-C")
        .arg(directory.path())
        .args(["hash-object", "-w", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"a verified body with additional publication rules")
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    (
        directory,
        String::from_utf8(output.stdout).unwrap().trim().to_owned(),
    )
}

#[cfg(feature = "git")]
async fn check_custom_publication(format: &str, reject_native: bool, reject_view: bool) {
    let (source, oid) = source(format);
    let object_format = if format == "sha1" {
        GitObjectFormat::Sha1
    } else {
        GitObjectFormat::Sha256
    };
    let native_calls = Arc::new(AtomicUsize::new(0));
    let view_calls = Arc::new(AtomicUsize::new(0));
    let formats = FormatRegistry::new([
        Arc::new(CheckedFormat {
            inner: Arc::new(GitNativeObjectFormat::new(
                object_format,
                GitObjectKind::Blob,
            )),
            calls: native_calls.clone(),
            reject: reject_native,
        }) as Arc<dyn ObjectFormat>,
        Arc::new(CheckedFormat {
            inner: Arc::new(GitViewFormat::default()),
            calls: view_calls.clone(),
            reject: reject_view,
        }) as Arc<dyn ObjectFormat>,
    ])
    .unwrap();
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        formats,
        FormatLimits::default(),
    );
    let result = repository
        .import(
            GitImport::new(source.path(), "custom")
                .with_revisions(vec![oid])
                .unwrap()
                .with_max_cached_pack_bytes(0),
        )
        .await;
    if reject_native || reject_view {
        assert!(
            result.is_err(),
            "construction must not bypass a custom relational rule"
        );
        assert!(native_calls.load(Ordering::SeqCst) > 0 || view_calls.load(Ordering::SeqCst) > 0);
        assert_eq!(
            repository
                .metadata()
                .snapshot()
                .await
                .unwrap()
                .root(&"git/custom".try_into().unwrap())
                .await
                .unwrap(),
            None
        );
    } else {
        let imported = result.unwrap();
        assert!(native_calls.load(Ordering::SeqCst) > 0);
        assert!(view_calls.load(Ordering::SeqCst) > 0);
        let snapshot = repository.metadata().snapshot().await.unwrap();
        assert_eq!(
            snapshot
                .root(&"git/custom".try_into().unwrap())
                .await
                .unwrap(),
            Some(imported.view.clone())
        );
        assert_eq!(
            snapshot.validated_closures(&[imported.view]).await.unwrap(),
            [true]
        );
    }
}

#[cfg(feature = "git")]
#[tokio::test]
async fn git_construction_checks_custom_native_relations() {
    for format in ["sha1", "sha256"] {
        check_custom_publication(format, true, false).await;
    }
}

#[cfg(feature = "git")]
#[tokio::test]
async fn git_construction_checks_custom_view_relations() {
    for format in ["sha1", "sha256"] {
        check_custom_publication(format, false, true).await;
    }
}

#[cfg(feature = "git")]
#[tokio::test]
async fn git_construction_publishes_after_custom_checks_pass() {
    for format in ["sha1", "sha256"] {
        check_custom_publication(format, false, false).await;
    }
}

#[tokio::test]
async fn filesystem_construction_checks_custom_directory_relations() {
    for reject in [false, true] {
        for empty in [false, true] {
            let source = tempfile::tempdir().unwrap();
            if !empty {
                std::fs::create_dir(source.path().join("child")).unwrap();
                std::fs::write(source.path().join("child/file"), b"file contents").unwrap();
            }
            let calls = Arc::new(AtomicUsize::new(0));
            let formats = FormatRegistry::new([
                Arc::new(BlobFormat::default()) as Arc<dyn ObjectFormat>,
                Arc::new(CheckedFormat {
                    inner: Arc::new(DirectoryFormat::default()),
                    calls: calls.clone(),
                    reject,
                }) as Arc<dyn ObjectFormat>,
            ])
            .unwrap();
            let repository = Repository::with_formats(
                MemoryBlobStore::new(),
                MemoryMetadataStore::new().unwrap(),
                formats,
                FormatLimits::default(),
            );
            let result = repository
                .import(FilesystemImport::new(
                    source.path(),
                    "custom".try_into().unwrap(),
                ))
                .await;
            assert!(
                calls.load(Ordering::SeqCst) > 0,
                "construction bypassed the registered directory rules"
            );
            let snapshot = repository.metadata().snapshot().await.unwrap();
            if reject {
                assert!(result.is_err());
                assert_eq!(
                    snapshot.root(&"custom".try_into().unwrap()).await.unwrap(),
                    None
                );
            } else {
                let root = result.unwrap();
                assert_eq!(
                    snapshot.root(&"custom".try_into().unwrap()).await.unwrap(),
                    Some(root.clone())
                );
                assert_eq!(snapshot.validated_closures(&[root]).await.unwrap(), [true]);
            }
        }
    }
}

#[tokio::test]
async fn custom_construction_audits_each_object_once() {
    // A chain of nested directories, each with its own file. Auditing every
    // constructed directory separately would reread each subtree once per
    // ancestor; shared proofs read every object exactly once.
    const DEPTH: usize = 24;
    let source = tempfile::tempdir().unwrap();
    let mut path = source.path().to_path_buf();
    for level in 0..DEPTH {
        std::fs::write(path.join("file"), format!("level {level}")).unwrap();
        path = path.join("child");
        std::fs::create_dir(&path).unwrap();
    }
    let directories = Arc::new(AtomicUsize::new(0));
    let blobs = Arc::new(AtomicUsize::new(0));
    let formats = FormatRegistry::new([
        Arc::new(CheckedFormat {
            inner: Arc::new(BlobFormat::default()),
            calls: blobs.clone(),
            reject: false,
        }) as Arc<dyn ObjectFormat>,
        Arc::new(CheckedFormat {
            inner: Arc::new(DirectoryFormat::default()),
            calls: directories.clone(),
            reject: false,
        }) as Arc<dyn ObjectFormat>,
    ])
    .unwrap();
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        formats,
        FormatLimits::default(),
    );
    let root = repository
        .import(FilesystemImport::new(
            source.path(),
            "nested".try_into().unwrap(),
        ))
        .await
        .unwrap();
    // DEPTH populated directories plus the empty innermost one.
    assert_eq!(directories.load(Ordering::SeqCst), DEPTH + 1);
    assert_eq!(blobs.load(Ordering::SeqCst), DEPTH);
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert_eq!(snapshot.validated_closures(&[root]).await.unwrap(), [true]);
}
