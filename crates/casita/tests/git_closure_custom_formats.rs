#![cfg(all(feature = "git", feature = "experimental"))]

use casita::experimental::{
    ClosureStatus, CommitResult, DirectLinkView, FormatError, FormatLimits, FormatRegistry,
    GitNativeObjectFormat, GitObjectFormat, GitObjectKind, MemoryBlobStore, MemoryMetadataStore,
    MetadataError, MetadataMutation, MetadataSnapshot, MetadataStore, ObjectFormat, PinStore,
    Repository, RepositoryLease, RepositoryRevision, SpillLimits, VerificationContext,
    VerifiedObject, git_object_key_for_body,
};
use casita::import::GitClosureImport;
use casita::{NamespaceId, ObjectKey, ObjectRecord};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::Notify;

/// The closure witnesses this revision's imports store, declared for the
/// benchmark harness rather than inferred from what the probe measures: a
/// present built-in Git blob is complete without a witness, so imports store
/// none for one unless it was selected.
const WITNESS_POLICY: &str = "derived-blobs";
/// Witnesses a built-in import stores per linear-history commit under the
/// declared policy: its commit and tree, and its blob only under
/// stored-blobs. Custom registries witness all three under any policy.
const BUILTIN_WITNESSES_PER_COMMIT: usize = match WITNESS_POLICY.as_bytes() {
    b"stored-blobs" => 3,
    b"derived-blobs" => 2,
    _ => panic!("unknown witness policy"),
};

struct CheckedNative {
    inner: GitNativeObjectFormat,
    calls: Arc<AtomicUsize>,
    /// Reject the link audit with this 1-based call number.
    reject_at: Option<usize>,
}

#[async_trait::async_trait]
impl ObjectFormat for CheckedNative {
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
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.reject_at == Some(call) {
            Err(FormatError::InvalidPayload {
                namespace: self.namespace().clone(),
                message: "custom link verification rejected".into(),
            })
        } else {
            self.inner
                .verify_links(context, object, links, limits)
                .await
        }
    }
}

#[tokio::test]
async fn closure_import_does_not_bypass_registered_link_verification() {
    let calls = Arc::new(AtomicUsize::new(0));
    let formats = FormatRegistry::new([Arc::new(CheckedNative {
        inner: GitNativeObjectFormat::new(GitObjectFormat::Sha1, GitObjectKind::Blob),
        calls: calls.clone(),
        reject_at: Some(1),
    }) as Arc<dyn ObjectFormat>])
    .unwrap();
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        formats,
        FormatLimits::default(),
    );
    let body = b"verified bytes with additional publication rules";
    let root = git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Blob, body).unwrap();
    let session = repository.mutation_session().await.unwrap();
    let staged = session.stage_object(root.clone(), body).await.unwrap();
    session.publish_unrooted(vec![staged]).await.unwrap();
    let result = repository
        .import(GitClosureImport::new("/missing", [root.clone()]))
        .await;
    assert!(
        result.is_err(),
        "a body seal does not prove custom relational rules"
    );
    assert!(calls.load(Ordering::SeqCst) > 0);
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .validated_closures(&[root])
            .await
            .unwrap(),
        [false]
    );
}

#[tokio::test]
async fn closure_import_reuses_a_proof_after_custom_link_verification() {
    let calls = Arc::new(AtomicUsize::new(0));
    let formats = FormatRegistry::new([Arc::new(CheckedNative {
        inner: GitNativeObjectFormat::new(GitObjectFormat::Sha1, GitObjectKind::Blob),
        calls: calls.clone(),
        reject_at: None,
    }) as Arc<dyn ObjectFormat>])
    .unwrap();
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        formats,
        FormatLimits::default(),
    );
    let body = b"verified bytes with additional publication rules";
    let root = git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Blob, body).unwrap();
    let session = repository.mutation_session().await.unwrap();
    let staged = session.stage_object(root.clone(), body).await.unwrap();
    session.publish_unrooted(vec![staged]).await.unwrap();
    let result = repository
        .import(GitClosureImport::new("/missing", [root.clone()]))
        .await;
    let imported = result.unwrap();
    let checked = calls.load(Ordering::SeqCst);
    assert!(checked > 0);
    let warm = repository
        .import(GitClosureImport::new("/missing", [root.clone()]))
        .await
        .unwrap();
    assert_eq!(warm.report.imported_objects, 0);
    assert_eq!(calls.load(Ordering::SeqCst), checked);
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .validated_closures(&[root])
            .await
            .unwrap(),
        [true]
    );
    drop(imported);
}

/// Every native Git kind, each audited through a counting link verifier.
fn counting_registry(calls: &Arc<AtomicUsize>) -> FormatRegistry {
    rejecting_registry(calls, None)
}

/// A counting registry whose link audit with this call number fails.
fn rejecting_registry(calls: &Arc<AtomicUsize>, reject_at: Option<usize>) -> FormatRegistry {
    FormatRegistry::new(
        [
            GitObjectKind::Blob,
            GitObjectKind::Tree,
            GitObjectKind::Commit,
            GitObjectKind::Tag,
        ]
        .into_iter()
        .map(|kind| {
            Arc::new(CheckedNative {
                inner: GitNativeObjectFormat::new(GitObjectFormat::Sha1, kind),
                calls: calls.clone(),
                reject_at,
            }) as Arc<dyn ObjectFormat>
        }),
    )
    .unwrap()
}

/// A packed linear SHA-1 history whose commits each add a distinct tree and
/// blob, so it holds exactly three objects per commit. Commits are returned
/// newest first.
fn linear_history(commits: usize) -> (tempfile::TempDir, Vec<ObjectKey>) {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let directory = tempfile::tempdir().unwrap();
    let git = |args: &[&str]| {
        let mut command = Command::new("git");
        command
            .arg("-C")
            .arg(directory.path())
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null");
        command
    };
    assert!(
        git(&["init", "--bare", "-q", "--object-format=sha1"])
            .status()
            .unwrap()
            .success()
    );
    let mut stream = Vec::new();
    for i in 0..commits {
        let message = format!("commit {i}\n");
        let contents = format!("contents {i}\n");
        write!(
            stream,
            "commit refs/heads/main\nmark :{}\ncommitter Casita <casita@example.com> 1700000000 +0000\ndata {}\n{message}",
            i + 1,
            message.len()
        )
        .unwrap();
        if i > 0 {
            writeln!(stream, "from :{i}").unwrap();
        }
        write!(
            stream,
            "M 100644 inline file\ndata {}\n{contents}\n",
            contents.len()
        )
        .unwrap();
    }
    let mut importer = git(&["fast-import", "--quiet"])
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    importer.stdin.take().unwrap().write_all(&stream).unwrap();
    assert!(importer.wait().unwrap().success());
    let listed = git(&["rev-list", "refs/heads/main"]).output().unwrap();
    assert!(listed.status.success());
    let history: Vec<_> = String::from_utf8(listed.stdout)
        .unwrap()
        .lines()
        .map(|oid| {
            casita::experimental::git_object_key(
                GitObjectFormat::Sha1,
                GitObjectKind::Commit,
                data_encoding::HEXLOWER.decode(oid.as_bytes()).unwrap(),
            )
            .unwrap()
        })
        .collect();
    assert_eq!(history.len(), commits);
    (directory, history)
}

/// A memory store recording how many closure witnesses each commit records.
/// It can race the first commit that records any, or pause after one.
struct WitnessLog {
    inner: MemoryMetadataStore,
    batches: Mutex<Vec<usize>>,
    first: AtomicBool,
    /// Commit an unrelated revision first, so the witness commit goes stale.
    race_first: bool,
    /// Signalled by the first snapshot after witness commit `pause_after`:
    /// the next batch begins, with no publication in flight. It awaits
    /// `resume`.
    pause: Option<(Arc<Notify>, Arc<Notify>)>,
    pause_after: usize,
    paused: AtomicBool,
}

impl WitnessLog {
    fn new() -> Self {
        Self {
            inner: MemoryMetadataStore::new().unwrap(),
            batches: Mutex::default(),
            first: AtomicBool::new(true),
            race_first: false,
            pause: None,
            pause_after: 1,
            paused: AtomicBool::new(false),
        }
    }

    /// Witness count of every committed publication that recorded any.
    fn batches(&self) -> Vec<usize> {
        self.batches.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl MetadataStore for WitnessLog {
    fn supports_metadata_records(&self) -> bool {
        self.inner.supports_metadata_records()
    }

    fn supports_root_retention(&self) -> bool {
        self.inner.supports_root_retention()
    }

    async fn try_collection_lease(&self) -> Result<Option<RepositoryLease>, MetadataError> {
        self.inner.try_collection_lease().await
    }

    fn coordinates_payload_catalog(&self) -> bool {
        self.inner.coordinates_payload_catalog()
    }

    async fn pin_store(&self) -> Result<Arc<dyn PinStore>, MetadataError> {
        self.inner.pin_store().await
    }

    async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
        if self.paused.swap(false, Ordering::SeqCst)
            && let Some((entered, resume)) = &self.pause
        {
            entered.notify_one();
            resume.notified().await;
        }
        self.inner.snapshot().await
    }

    async fn commit(
        &self,
        expected: &RepositoryRevision,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        let witnesses = mutation.validated_closures().len();
        let first = witnesses > 0 && self.first.swap(false, Ordering::SeqCst);
        if first && self.race_first {
            self.inner.commit(expected, MetadataMutation::new()).await?;
        }
        let result = self.inner.commit(expected, mutation).await?;
        if witnesses > 0 {
            let mut batches = self.batches.lock().unwrap();
            batches.push(witnesses);
            if batches.len() == self.pause_after && self.pause.is_some() {
                self.paused.store(true, Ordering::SeqCst);
            }
        }
        Ok(result)
    }
}

fn witness_repository(
    store: WitnessLog,
    formats: FormatRegistry,
    max_batch_objects: usize,
) -> (
    Arc<WitnessLog>,
    Repository<MemoryBlobStore, Arc<WitnessLog>>,
) {
    let store = Arc::new(store);
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        store.clone(),
        formats,
        FormatLimits {
            max_batch_objects,
            ..Default::default()
        },
    );
    (store, repository)
}

/// Every key carries a stored witness in the current state.
async fn all_witnessed(store: &WitnessLog, keys: &[ObjectKey]) -> bool {
    let snapshot = store.inner.snapshot().await.unwrap();
    let witnessed = snapshot.validated_closures(keys).await.unwrap();
    witnessed.into_iter().all(|witnessed| witnessed)
}

#[tokio::test]
async fn custom_registries_audit_each_object_of_a_history_once() {
    const COMMITS: usize = 32;
    let (source, history) = linear_history(COMMITS);
    let root = history[0].clone();
    // One witness batch holding every commit, and six smaller batches.
    for max_batch_objects in [4_096, 16] {
        let calls = Arc::new(AtomicUsize::new(0));
        let repository = Repository::with_formats(
            MemoryBlobStore::new(),
            MemoryMetadataStore::new().unwrap(),
            counting_registry(&calls),
            FormatLimits {
                max_batch_objects,
                ..Default::default()
            },
        );
        let request = GitClosureImport::new(source.path().join("objects"), [root.clone()]);
        let imported = repository.import(request.clone()).await.unwrap();
        assert_eq!(imported.report.imported_objects, 3 * COMMITS);
        // Each commit's closure contains every older commit. Auditing each
        // witness target separately would repeat those walks quadratically.
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3 * COMMITS,
            "{max_batch_objects}-object batches must audit each object exactly once"
        );
        let warm = repository.import(request).await.unwrap();
        assert_eq!(warm.report.imported_objects, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 3 * COMMITS);
        assert_eq!(
            repository.verify_closure(&root).await.unwrap(),
            ClosureStatus::Complete {
                objects: 3 * COMMITS
            }
        );
    }
}

/// The reviewed reproduction: a 256-commit history whose witnesses span many
/// 16-object batches. Every witness commit must stay within the publication
/// limit, including with spill budgets far below one batch.
#[tokio::test]
async fn witness_publications_stay_within_the_batch_limit() {
    const COMMITS: usize = 256;
    const BATCH: usize = 16;
    let (source, history) = linear_history(COMMITS);
    let root = history[0].clone();
    for (registry, max_memory_objects) in [("builtin", None), ("custom", None), ("custom", Some(4))]
    {
        let calls = Arc::new(AtomicUsize::new(0));
        let formats = match registry {
            "builtin" => FormatRegistry::builtin(),
            _ => counting_registry(&calls),
        };
        let (store, repository) = witness_repository(WitnessLog::new(), formats, BATCH);
        let repository = match max_memory_objects {
            Some(max_memory_objects) => repository.with_spill_limits(SpillLimits {
                max_memory_objects,
                ..SpillLimits::default()
            }),
            None => repository,
        };
        let imported = repository
            .import(GitClosureImport::new(
                source.path().join("objects"),
                [root.clone()],
            ))
            .await
            .unwrap();
        assert_eq!(imported.report.imported_objects, 3 * COMMITS);
        // Custom registries witness every object they audited, each once.
        let (witnesses, audits) = match registry {
            "builtin" => (BUILTIN_WITNESSES_PER_COMMIT * COMMITS, 0),
            _ => (3 * COMMITS, 3 * COMMITS),
        };
        let context = format!("{registry} registry, spill budget {max_memory_objects:?}");
        let batches = store.batches();
        assert!(
            batches.iter().all(|&batch| batch <= BATCH),
            "{context}: witness batches {batches:?} exceed {BATCH}"
        );
        assert_eq!(batches.iter().sum::<usize>(), witnesses, "{context}");
        assert_eq!(batches.len(), witnesses.div_ceil(BATCH), "{context}");
        assert_eq!(calls.load(Ordering::SeqCst), audits, "{context}");
        assert!(all_witnessed(&store, &history).await, "{context}");
        assert_eq!(
            repository.verify_closure(&root).await.unwrap(),
            ClosureStatus::Complete {
                objects: 3 * COMMITS
            }
        );
    }
}

/// Witness batches fill exactly to the publication limit: a selection owing
/// exactly one batch commits once, and one more commit spills only its own
/// witnesses into a second batch.
#[tokio::test]
async fn witness_batches_split_exactly_at_the_publication_limit() {
    const BATCH: usize = 12;
    for registry in ["builtin", "custom"] {
        let per_commit = match registry {
            "builtin" => BUILTIN_WITNESSES_PER_COMMIT,
            _ => 3,
        };
        assert_eq!(BATCH % per_commit, 0);
        let full = BATCH / per_commit;
        for (commits, expected) in [(full, vec![BATCH]), (full + 1, vec![BATCH, per_commit])] {
            let (source, history) = linear_history(commits);
            let calls = Arc::new(AtomicUsize::new(0));
            let formats = match registry {
                "builtin" => FormatRegistry::builtin(),
                _ => counting_registry(&calls),
            };
            let (store, repository) = witness_repository(WitnessLog::new(), formats, BATCH);
            repository
                .import(GitClosureImport::new(
                    source.path().join("objects"),
                    [history[0].clone()],
                ))
                .await
                .unwrap();
            assert_eq!(
                store.batches(),
                expected,
                "{registry} registry, {commits} commits"
            );
            assert!(all_witnessed(&store, &history).await);
        }
    }
}

/// Overlapping selected closures share one audit: each object is verified
/// once however many selected roots reach it.
#[tokio::test]
async fn overlapping_roots_audit_each_object_once() {
    const COMMITS: usize = 48;
    let (source, history) = linear_history(COMMITS);
    let roots = [
        history[0].clone(),
        history[COMMITS / 2].clone(),
        history[COMMITS - 1].clone(),
    ];
    let calls = Arc::new(AtomicUsize::new(0));
    let (store, repository) = witness_repository(WitnessLog::new(), counting_registry(&calls), 16);
    repository
        .import(GitClosureImport::new(source.path().join("objects"), roots))
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3 * COMMITS);
    let batches = store.batches();
    assert!(batches.iter().all(|&batch| batch <= 16), "{batches:?}");
    assert_eq!(batches.iter().sum::<usize>(), 3 * COMMITS);
    assert!(all_witnessed(&store, &history).await);
}

/// A rejection by the very last link audit publishes no witness at all: the
/// whole selection is proven before any batch becomes visible.
#[tokio::test]
async fn a_late_audit_rejection_publishes_no_witness() {
    const COMMITS: usize = 64;
    let (source, history) = linear_history(COMMITS);
    let calls = Arc::new(AtomicUsize::new(0));
    let (store, repository) = witness_repository(
        WitnessLog::new(),
        rejecting_registry(&calls, Some(3 * COMMITS)),
        16,
    );
    let result = repository
        .import(GitClosureImport::new(
            source.path().join("objects"),
            [history[0].clone()],
        ))
        .await;
    assert!(result.is_err(), "the final audit rejected its object");
    assert_eq!(calls.load(Ordering::SeqCst), 3 * COMMITS);
    assert_eq!(store.batches(), Vec::<usize>::new());
    let snapshot = store.inner.snapshot().await.unwrap();
    let witnessed = snapshot.validated_closures(&history).await.unwrap();
    assert!(witnessed.into_iter().all(|witnessed| !witnessed));
}

/// A revision race retries only the current witness batch, reusing proofs
/// established before any batch was published.
#[tokio::test]
async fn a_stale_witness_commit_retries_its_batch_without_reauditing() {
    const COMMITS: usize = 64;
    let (source, history) = linear_history(COMMITS);
    let calls = Arc::new(AtomicUsize::new(0));
    let (store, repository) = witness_repository(
        WitnessLog {
            race_first: true,
            ..WitnessLog::new()
        },
        counting_registry(&calls),
        16,
    );
    repository
        .import(GitClosureImport::new(
            source.path().join("objects"),
            [history[0].clone()],
        ))
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3 * COMMITS);
    let batches = store.batches();
    assert!(batches.iter().all(|&batch| batch <= 16), "{batches:?}");
    assert_eq!(batches.iter().sum::<usize>(), 3 * COMMITS);
    assert!(all_witnessed(&store, &history).await);
}

/// Collection between witness batches removes unrelated garbage but nothing
/// the import's proofs vouch for: the session still retains the closures.
#[tokio::test]
async fn collection_between_witness_batches_keeps_proven_closures() {
    const COMMITS: usize = 64;
    let (source, history) = linear_history(COMMITS);
    let root = history[0].clone();
    let entered = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let (store, repository) = witness_repository(
        WitnessLog {
            pause: Some((entered.clone(), resume.clone())),
            ..WitnessLog::new()
        },
        counting_registry(&calls),
        16,
    );
    let importer = repository.clone();
    let request = GitClosureImport::new(source.path().join("objects"), [root.clone()]);
    let import = tokio::spawn(async move { importer.import(request).await.map(drop) });
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        entered.notified().await;
        // Garbage born after the import's starting snapshot, which the
        // import's retention hold would otherwise protect.
        let session = repository.mutation_session().await.unwrap();
        let garbage = session.stage_blob(b"unretained").await.unwrap();
        session.publish_unrooted(vec![garbage]).await.unwrap();
        drop(session);
        casita::experimental::flush_repository_leases()
            .await
            .unwrap();
        let collected = repository.collect().await.unwrap();
        assert_eq!(collected.removed.logical_objects, 1);
        resume.notify_one();
        import.await.unwrap().unwrap();
    })
    .await
    .expect("collection and witness publication must not wait on each other");

    assert_eq!(calls.load(Ordering::SeqCst), 3 * COMMITS);
    let batches = store.batches();
    assert!(batches.len() > 1, "{batches:?}");
    assert!(batches.iter().all(|&batch| batch <= 16), "{batches:?}");
    assert_eq!(batches.iter().sum::<usize>(), 3 * COMMITS);
    assert!(all_witnessed(&store, &history).await);
    assert_eq!(
        repository.verify_closure(&root).await.unwrap(),
        ClosureStatus::Complete {
            objects: 3 * COMMITS
        }
    );
}

/// Cancelling an import between any two witness batches leaves only complete
/// closures witnessed, and the unwitnessed rest reachable from the selection:
/// repeating the import witnesses every object the first one owed, once.
/// Spill budgets below one batch publish from the spilled order.
#[tokio::test]
async fn a_repeated_import_completes_witnesses_cancelled_between_batches() {
    const COMMITS: usize = 8;
    const BATCH: usize = 4;
    let (source, history) = linear_history(COMMITS);
    let request = GitClosureImport::new(source.path().join("objects"), [history[0].clone()]);
    for (registry, max_memory_objects) in [
        ("builtin", None),
        ("builtin", Some(2)),
        ("custom", None),
        ("custom", Some(2)),
    ] {
        let owed = match registry {
            "builtin" => BUILTIN_WITNESSES_PER_COMMIT * COMMITS,
            _ => 3 * COMMITS,
        };
        for cancelled_after in 1..owed.div_ceil(BATCH) {
            let context = format!(
                "{registry} registry, spill budget {max_memory_objects:?}, \
                 cancelled after batch {cancelled_after}"
            );
            let entered = Arc::new(Notify::new());
            let calls = Arc::new(AtomicUsize::new(0));
            let formats = match registry {
                "builtin" => FormatRegistry::builtin(),
                _ => counting_registry(&calls),
            };
            let (store, repository) = witness_repository(
                WitnessLog {
                    pause: Some((entered.clone(), Arc::new(Notify::new()))),
                    pause_after: cancelled_after,
                    ..WitnessLog::new()
                },
                formats,
                BATCH,
            );
            let repository = match max_memory_objects {
                Some(max_memory_objects) => repository.with_spill_limits(SpillLimits {
                    max_memory_objects,
                    ..SpillLimits::default()
                }),
                None => repository,
            };
            let importer = repository.clone();
            let cancelled = request.clone();
            let import = tokio::spawn(async move { importer.import(cancelled).await.map(drop) });
            tokio::time::timeout(std::time::Duration::from_secs(60), entered.notified())
                .await
                .expect(&context);
            import.abort();
            assert!(import.await.unwrap_err().is_cancelled(), "{context}");
            assert_eq!(store.batches().len(), cancelled_after, "{context}");

            repository.import(request.clone()).await.unwrap();
            let batches = store.batches();
            assert!(
                batches.iter().all(|&batch| batch <= BATCH),
                "{context}: {batches:?}"
            );
            assert_eq!(
                batches.iter().sum::<usize>(),
                owed,
                "{context}: {batches:?}"
            );
            assert!(all_witnessed(&store, &history).await, "{context}");
            assert_eq!(
                repository.verify_closure(&history[0]).await.unwrap(),
                ClosureStatus::Complete {
                    objects: 3 * COMMITS
                },
                "{context}"
            );
        }
    }
}

/// Imports a linear history once, timing the cold import and recording how
/// many custom link audits it needed. Warm reuse and the exhaustive closure
/// audit are correctness gates outside the timed region.
#[tokio::test]
#[ignore = "run through benchmark run git-closure-audit"]
async fn benchmark_git_closure_audit() {
    let variable = |name: &str| std::env::var(name).unwrap();
    let commits: usize = variable("CASITA_GIT_AUDIT_COMMITS").parse().unwrap();
    let registry = variable("CASITA_GIT_AUDIT_REGISTRY");
    let max_batch_objects: usize = variable("CASITA_GIT_AUDIT_BATCH_OBJECTS").parse().unwrap();
    let (source, history) = linear_history(commits);
    let root = history[0].clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let formats = match registry.as_str() {
        "builtin" => FormatRegistry::builtin(),
        "custom" => counting_registry(&calls),
        other => panic!("unknown registry {other}"),
    };
    // Recording each commit's witness count is outside the timed region's
    // cost: one length read per metadata commit.
    let (store, repository) = witness_repository(WitnessLog::new(), formats, max_batch_objects);
    let request = GitClosureImport::new(source.path().join("objects"), [root.clone()]);
    let start = std::time::Instant::now();
    let imported = repository.import(request.clone()).await.unwrap();
    let nanos = start.elapsed().as_nanos();
    // Built-in registries trust construction, which no verifier call observes.
    let link_audits = (registry == "custom").then(|| calls.load(Ordering::SeqCst));
    let batches = store.batches();
    let objects = 3 * commits;
    assert_eq!(imported.report.imported_objects, objects);
    assert_eq!(imported.report.reused_objects, 0);
    // The declared witness policy fixes every count the harness checks.
    let witnesses = match registry.as_str() {
        "builtin" => BUILTIN_WITNESSES_PER_COMMIT * commits,
        _ => objects,
    };
    assert_eq!(batches.iter().sum::<usize>(), witnesses);
    assert_eq!(batches.len(), witnesses.div_ceil(max_batch_objects));
    assert!(batches.iter().all(|&batch| batch <= max_batch_objects));
    if let Some(link_audits) = link_audits {
        assert_eq!(link_audits, objects);
    }
    let warm = repository.import(request).await.unwrap();
    assert_eq!(
        repository.verify_closure(&root).await.unwrap(),
        ClosureStatus::Complete { objects }
    );
    println!(
        "git_closure_audit_sample {}",
        serde_json::json!({
            "commits": commits, "registry": registry,
            "publication_batch_objects": max_batch_objects, "objects": objects,
            "imported_objects": imported.report.imported_objects,
            "reused_objects": imported.report.reused_objects,
            "warm_imported_objects": warm.report.imported_objects,
            "warm_source_bytes": warm.report.source_bytes,
            "link_audits": link_audits, "wall_nanos": nanos, "root": root.to_string(),
            "witness_policy": WITNESS_POLICY,
            "witnesses": batches.iter().sum::<usize>(), "witness_commits": batches.len(),
            "max_witness_batch": batches.iter().copied().max().unwrap_or(0),
            "correctness": "exact import counts, exhaustive closure verification and source-free warm reuse"
        })
    );
}
