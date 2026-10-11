//! Deterministic process-death tests, compiled only into the library test binary.
//!
//! A successful control execution discovers every checkpoint occurrence. Each
//! replay stops at one occurrence until the supervisor kills the process, then
//! an independent process opens and audits the repository. This models process
//! death, not power loss: the kernel and its page cache remain alive.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use tokio::io::AsyncReadExt;

use super::local_durability::LocalDurability;
use crate::{
    BlobId, BlobStore, ClosureStatus, Directory, MetadataStore, Node, ObjectKey, RootChange,
    RootName, repository::Repository,
};

const CHILD_TEST: &str = "blob::crash_tests::publication_crash_worker";
const WORK_ENV: &str = "CASITA_PUBLICATION_CRASH_WORK";
const TARGET_ENV: &str = "CASITA_PUBLICATION_CRASH_TARGET";
const SCENARIO_ENV: &str = "CASITA_PUBLICATION_CRASH_SCENARIO";
const MODE_ENV: &str = "CASITA_PUBLICATION_CRASH_MODE";
const TIMEOUT: Duration = Duration::from_secs(60);

struct Recorder {
    work: PathBuf,
    target: String,
    counts: BTreeMap<String, usize>,
    trace: File,
}

static RECORDER: OnceLock<Mutex<Recorder>> = OnceLock::new();

/// Unarmed in ordinary tests; no environment mutation or production failpoints.
/// The mutex also prevents another instrumented operation passing the selected
/// checkpoint while the supervisor is terminating this process.
pub(crate) fn checkpoint(label: &str) {
    let Some(recorder) = RECORDER.get() else {
        return;
    };
    let mut recorder = recorder.lock().unwrap();
    let occurrence = recorder.counts.entry(label.to_owned()).or_default();
    *occurrence += 1;
    let event = format!("{label}:{}", *occurrence);
    writeln!(recorder.trace, "{event}").unwrap();
    recorder.trace.flush().unwrap();
    if recorder.target == event {
        let ready = recorder.work.join("ready");
        std::fs::write(ready.with_extension("tmp"), &event).unwrap();
        std::fs::rename(ready.with_extension("tmp"), ready).unwrap();
        loop {
            std::thread::park();
        }
    }
}

pub(crate) fn file_checkpoint(phase: &str, path: &Path) {
    if RECORDER.get().is_none() {
        return;
    }
    let kind = if path
        .file_name()
        .is_some_and(|name| name == "pack-index-current")
    {
        "pointer".to_owned()
    } else {
        format!(
            "catalog-object/{}",
            path.file_name().unwrap().to_string_lossy()
        )
    };
    checkpoint(&format!("{kind}/{phase}"));
}

pub(crate) fn object_checkpoint(phase: &str, path: &ObjectPath) {
    if RECORDER.get().is_none() {
        return;
    }
    // Ignore content addresses, whose values are immaterial to the boundary.
    let kind = path
        .as_ref()
        .split('/')
        .find(|part| {
            matches!(
                *part,
                "packs"
                    | "chunks"
                    | "manifests"
                    | "outboards"
                    | "pages"
                    | "blobs"
                    | "bao"
                    | "bao-packs"
                    | "bao-indexes"
            )
        })
        .unwrap_or("object");
    checkpoint(&format!("{kind}/{phase}"));
}

fn arm(work: &Path) {
    let recorder = Recorder {
        work: work.to_owned(),
        target: std::env::var(TARGET_ENV).unwrap(),
        counts: BTreeMap::new(),
        trace: File::create(work.join("trace")).unwrap(),
    };
    assert!(RECORDER.set(Mutex::new(recorder)).is_ok());
}

fn name(value: &str) -> RootName {
    RootName::try_from(value).unwrap()
}
fn key(bytes: &[u8]) -> ObjectKey {
    ObjectKey::blob(BlobId::new(blake3::hash(bytes).into()))
}
fn retained(index: usize) -> Vec<u8> {
    if index == 2 {
        let mut bytes = vec![0; 256 * 1024 + 9];
        blake3::Hasher::new()
            .update(b"previously acknowledged chunked payload")
            .finalize_xof()
            .fill(&mut bytes);
        return bytes;
    }
    format!("acknowledged generation {index}").into_bytes()
}

fn files() -> Vec<(&'static str, Vec<u8>)> {
    // Deterministic incompressible content crosses chunk/pack boundaries; the
    // second file uses the small-payload path and the third reuses old content.
    let mut large = vec![0; 1024 * 1024 + 17];
    blake3::Hasher::new()
        .update(b"crash matrix payload")
        .finalize_xof()
        .fill(&mut large);
    vec![
        ("large", large),
        ("small", b"new small file".to_vec()),
        ("reused", retained(0)),
        // Above the small-file cutoff, EOF lets the writer admit this blob,
        // its sole chunk, and its Bao path in one durable update.
        ("single", vec![42; 131073]),
    ]
}

fn directory() -> Directory {
    Directory::try_from_iter(files().into_iter().map(|(name, bytes)| {
        (
            crate::PathComponent::try_from(name).unwrap(),
            Node::File {
                digest: BlobId::new(blake3::hash(&bytes).into()),
                size: bytes.len() as u64,
                executable: false,
            },
        )
    }))
    .unwrap()
}

type LocalRepository = Repository<crate::ChunkedBlobStore, crate::TursoMetadataStore>;

async fn publish_blob(repository: &LocalRepository, root: &str, bytes: &[u8]) {
    let mutation = repository.mutation_session().await.unwrap();
    let object = mutation.stage_blob(bytes).await.unwrap();
    mutation
        .publish_rooted(vec![object], name(root), key(bytes))
        .await
        .unwrap();
}

async fn repository_writer(work: &Path, rebase: bool) {
    let repository = Repository::local_with_pack_options(
        work.join("repository"),
        crate::PackOptions {
            target_size: 64 * 1024,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    for index in 0..3 {
        publish_blob(&repository, &format!("retained/{index}"), &retained(index)).await;
    }
    publish_blob(&repository, "head", &retained(0)).await;
    publish_blob(&repository, "remove-me", &retained(1)).await;
    if rebase {
        // Exercise the real shard/map publication and reopen-time reclamation
        // path with a bounded fixture instead of thousands of generations.
        repository
            .payloads()
            .set_pack_catalog_rebase_run_bytes_for_test(1);
    }
    arm(work);
    let mutation = repository.mutation_session().await.unwrap();
    let mut staged = Vec::new();
    for (index, (_, bytes)) in files().into_iter().enumerate() {
        let object = if index == 0 {
            mutation
                .stage_blob_reader(&mut bytes.as_slice())
                .await
                .unwrap()
        } else {
            mutation.stage_blob(&bytes).await.unwrap()
        };
        staged.push(object);
    }
    let tree = mutation.stage_directory(&directory()).await.unwrap();
    let target = tree.record().key().clone();
    staged.push(tree);
    checkpoint("staging-complete");
    mutation
        .publish(
            staged,
            vec![
                RootChange::Set {
                    name: name("head"),
                    target: target.clone(),
                },
                RootChange::Set {
                    name: name("new-generation"),
                    target,
                },
                RootChange::Remove {
                    name: name("remove-me"),
                },
            ],
        )
        .await
        .unwrap();
    checkpoint("publication-acknowledged");
}

async fn assert_payload(repository: &LocalRepository, key: &ObjectKey, expected: &[u8]) {
    let snapshot = repository.metadata().snapshot().await.unwrap();
    let record = snapshot
        .object(key)
        .await
        .unwrap()
        .expect("committed object record");
    drop(snapshot);
    let mut reader = repository
        .payloads()
        .open_read(&record.payload())
        .await
        .unwrap()
        .expect("committed payload");
    let mut actual = Vec::new();
    reader.read_to_end(&mut actual).await.unwrap();
    assert_eq!(actual, expected, "restored bytes for {key:?}");
}

async fn audit_repository(repository: &LocalRepository, target: &str) {
    let snapshot = repository.metadata().snapshot().await.unwrap();
    for index in 0..3 {
        assert_eq!(
            snapshot
                .root(&name(&format!("retained/{index}")))
                .await
                .unwrap(),
            Some(key(&retained(index)))
        );
    }
    let head = snapshot.root(&name("head")).await.unwrap().unwrap();
    let generation = snapshot.root(&name("new-generation")).await.unwrap();
    let removed = snapshot.root(&name("remove-me")).await.unwrap();
    let new_key = ObjectKey::directory(directory().digest());
    if head == new_key {
        assert_eq!(
            generation,
            Some(new_key.clone()),
            "root additions must commit together"
        );
        assert!(
            removed.is_none(),
            "root removal must commit with replacement"
        );
    } else {
        assert_eq!(
            head,
            key(&retained(0)),
            "head must be a complete old or new version"
        );
        assert!(generation.is_none(), "no partial root transaction");
        assert_eq!(removed, Some(key(&retained(1))));
    }
    if target.starts_with("after-state-commit:")
        || target.starts_with("publication-acknowledged:")
        || target == "control"
    {
        assert_eq!(
            head, new_key,
            "committed/acknowledged publication must survive"
        );
    } else {
        assert_eq!(
            head,
            key(&retained(0)),
            "pre-commit interruption must preserve the old roots"
        );
    }
    drop(snapshot);
    for index in 0..3 {
        assert_payload(repository, &key(&retained(index)), &retained(index)).await;
    }
    if head == new_key {
        assert_payload(repository, &head, &directory().encode()).await;
        for (_, bytes) in files() {
            assert_payload(repository, &key(&bytes), &bytes).await;
        }
    }
    assert!(matches!(
        repository.verify_closure(&head).await.unwrap(),
        ClosureStatus::Complete { .. }
    ));
    let report = repository.fsck().await.unwrap();
    assert!(
        report.is_healthy(),
        "rooted corruption after {target}: {report:?}"
    );
}

async fn repository_verifier(work: &Path, target: &str) {
    let repository = Repository::local(work.join("repository")).await.unwrap();
    audit_repository(&repository, target).await;
    // Prove the killed process released both catalog and repository locks,
    // and that its WAL can accept further durable writes before collection.
    publish_blob(&repository, "recovery", b"written after recovery").await;
    repository.collect().await.unwrap();
    audit_repository(&repository, target).await;
    assert_payload(
        &repository,
        &key(b"written after recovery"),
        b"written after recovery",
    )
    .await;
    assert!(repository.fsck().await.unwrap().is_clean());
    drop(repository);
    let reopened = Repository::local(work.join("repository")).await.unwrap();
    audit_repository(&reopened, target).await;
    assert_payload(
        &reopened,
        &key(b"written after recovery"),
        b"written after recovery",
    )
    .await;
}

async fn catalog_writer(work: &Path, streamed: bool) {
    let root = work.join("catalog");
    std::fs::create_dir_all(&root).unwrap();
    let local =
        LocalDurability::new(LocalFileSystem::new_with_prefix(&root).unwrap(), &root).unwrap();
    let pointer = ObjectPath::from("pack-index-current");
    local
        .put(
            &ObjectPath::from("old"),
            Bytes::from_static(b"old acknowledged bytes"),
        )
        .await
        .unwrap();
    local
        .put(&pointer, Bytes::from_static(b"old"))
        .await
        .unwrap();
    arm(work);
    let lock = local.lock_catalog().await.unwrap();
    if streamed {
        let source = work.join("source");
        std::fs::write(&source, streamed_bytes()).unwrap();
        local
            .put_file(
                &ObjectPath::from("nested/a/left"),
                File::open(source).unwrap(),
            )
            .await
            .unwrap();
    } else {
        let left = local
            .prepare(
                &ObjectPath::from("nested/a/left"),
                Bytes::from_static(b"new left bytes"),
            )
            .await
            .unwrap();
        let right = local
            .prepare(
                &ObjectPath::from("nested/b/right"),
                Bytes::from_static(b"new right bytes"),
            )
            .await
            .unwrap();
        local.commit(vec![left, right]).await.unwrap();
    }
    assert!(
        lock.compare_and_put(
            &pointer,
            Bytes::from_static(b"new"),
            Some(*blake3::hash(b"old").as_bytes())
        )
        .await
        .unwrap()
    );
    checkpoint("publication-acknowledged");
}

async fn catalog_verifier(work: &Path, target: &str, streamed: bool) {
    let root = work.join("catalog");
    assert_eq!(
        std::fs::read(root.join("old")).unwrap(),
        b"old acknowledged bytes"
    );
    let current = std::fs::read(root.join("pack-index-current")).unwrap();
    let trace = std::fs::read_to_string(work.join("trace")).unwrap();
    let published = trace.lines().any(|event| event == "pointer/after-rename:1");
    assert_eq!(
        current,
        if published { b"new" } else { b"old" },
        "atomic pointer at {target}"
    );
    if current == b"new" {
        let left: Vec<u8> = if streamed {
            streamed_bytes()
        } else {
            b"new left bytes".to_vec()
        };
        assert_eq!(std::fs::read(root.join("nested/a/left")).unwrap(), left);
        if !streamed {
            assert_eq!(
                std::fs::read(root.join("nested/b/right")).unwrap(),
                b"new right bytes"
            );
        }
    }
    let local =
        LocalDurability::new(LocalFileSystem::new_with_prefix(&root).unwrap(), &root).unwrap();
    // Also verifies OS-lock release when killed while holding LocalCatalogLock.
    let lock = local.lock_catalog().await.unwrap();
    assert!(
        lock.compare_and_put(
            &ObjectPath::from("pack-index-current"),
            Bytes::from_static(b"recovered"),
            Some(*blake3::hash(&current).as_bytes())
        )
        .await
        .unwrap()
    );
    assert_eq!(
        std::fs::read(root.join("pack-index-current")).unwrap(),
        b"recovered"
    );
}

fn streamed_bytes() -> Vec<u8> {
    let mut bytes = vec![0; 2 * 1024 * 1024 + 13];
    blake3::Hasher::new()
        .update(b"streamed catalog crash fixture")
        .finalize_xof()
        .fill(&mut bytes);
    bytes
}

#[test]
fn publication_crash_worker() {
    let Some(work) = std::env::var_os(WORK_ENV) else {
        return;
    };
    let work = PathBuf::from(work);
    let scenario = std::env::var(SCENARIO_ENV).unwrap();
    let target = std::env::var(TARGET_ENV).unwrap();
    let verify = std::env::var(MODE_ENV).unwrap() == "verify";
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            match (scenario.as_str(), verify) {
                ("repository" | "repository-rebase", false) => {
                    repository_writer(&work, scenario == "repository-rebase").await
                }
                ("repository" | "repository-rebase", true) => {
                    repository_verifier(&work, &target).await
                }
                ("paged-overwrite", false) => paged_overwrite_writer(&work).await,
                ("paged-overwrite", true) => paged_overwrite_verifier(&work, &target).await,
                ("metadata", false) => metadata_writer(&work).await,
                ("metadata", true) => metadata_verifier(&work, &target).await,
                ("operation-marker", false) => operation_marker_writer(&work).await,
                ("operation-marker", true) => operation_marker_verifier(&work, &target).await,
                ("catalog-migration", false) => catalog_migration_writer(&work).await,
                ("catalog-migration", true) => catalog_migration_verifier(&work).await,
                ("batch" | "stream", false) => catalog_writer(&work, scenario == "stream").await,
                ("batch" | "stream", true) => {
                    catalog_verifier(&work, &target, scenario == "stream").await
                }
                _ => panic!("unknown crash scenario {scenario}"),
            }
        });
    if verify {
        std::fs::write(work.join("verified"), target).unwrap();
    }
}

struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn run_worker(work: &Path, scenario: &str, target: &str, verify: bool) {
    let log = work.join(if verify { "verify.log" } else { "writer.log" });
    let output = File::create(&log).unwrap();
    let mut worker = Worker(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CHILD_TEST, "--nocapture"])
            .env(WORK_ENV, work)
            .env(SCENARIO_ENV, scenario)
            .env(TARGET_ENV, target)
            .env(MODE_ENV, if verify { "verify" } else { "write" })
            .stdin(Stdio::null())
            .stdout(output.try_clone().unwrap())
            .stderr(output)
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + TIMEOUT;
    loop {
        if !verify && target != "control" && work.join("ready").exists() {
            assert_eq!(std::fs::read_to_string(work.join("ready")).unwrap(), target);
            worker.0.kill().unwrap();
            let status = worker.0.wait().unwrap();
            assert!(!status.success(), "worker must die without unwinding");
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                assert_eq!(status.signal(), Some(libc::SIGKILL));
            }
            return;
        }
        if let Some(status) = worker.0.try_wait().unwrap() {
            assert!(
                status.success() && (verify || target == "control"),
                "{scenario}/{target} verify={verify} exited before its checkpoint or failed: {status}\n{}",
                std::fs::read_to_string(&log).unwrap()
            );
            if verify {
                assert_eq!(
                    std::fs::read_to_string(work.join("verified")).unwrap(),
                    target,
                    "the verifier must actually execute its audit"
                );
            }
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{scenario}/{target} verify={verify} timed out\n{}",
            std::fs::read_to_string(&log).unwrap()
        );
        // Poll only for the explicit handshake; this delay never selects where
        // the child dies. The RAII guard kills/reaps children on every failure.
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn exercise(scenario: &str) {
    let control = tempfile::tempdir().unwrap();
    run_worker(control.path(), scenario, "control", false);
    run_worker(control.path(), scenario, "control", true);
    let trace = std::fs::read_to_string(control.path().join("trace")).unwrap();
    let events: Vec<_> = trace.lines().collect();
    assert!(events.contains(&"publication-acknowledged:1"));
    if scenario.starts_with("repository") {
        assert!(
            events.iter().all(|event| !event.starts_with("pointer/")),
            "repository publication must commit its catalog through SQLite"
        );
        for required in [
            "packs/before-put:1",
            "packs/after-put:1",
            "state-object-inserted:1",
            "state-root-changed:3",
            "before-state-commit:1",
            "after-state-commit:1",
        ] {
            assert!(
                events.contains(&required),
                "repository missed {required}: {events:?}"
            );
        }
    } else {
        for required in [
            "catalog-lock-acquired:1",
            "pointer/temporary-created:1",
            "pointer/before-file-sync:1",
            "pointer/after-file-sync:1",
            "pointer/before-rename:1",
            "pointer/after-rename:1",
        ] {
            assert!(events.contains(&required), "{scenario} missed {required}");
        }
        assert!(events.contains(&"catalog-object/left/after-file-sync:1"));
        if scenario == "batch" {
            assert!(events.contains(&"catalog-object/right/after-rename:1"));
        } else {
            assert!(events.contains(&"catalog-object/left/stream-block-written:3"));
        }
    }
    if scenario == "repository-rebase" {
        assert!(
            events
                .iter()
                .filter(|event| event.starts_with("catalog-object/")
                    && event.ends_with("/after-rename:1"))
                .count()
                >= 4,
            "real rebase must publish immutable shards and their map"
        );
    }
    if scenario != "repository" {
        for required in ["before-directory-sync:1", "after-directory-sync:1"] {
            assert!(events.contains(&required), "{scenario} missed {required}");
        }
        #[cfg(unix)]
        for required in [
            "before-directory-component-sync:1",
            "after-directory-component-sync:1",
        ] {
            assert!(events.contains(&required), "{scenario} missed {required}");
        }
    }
    for (index, event) in events.iter().enumerate() {
        eprintln!(
            "crash matrix {scenario} {}/{}, {event}",
            index + 1,
            events.len()
        );
        let work = tempfile::tempdir().unwrap();
        run_worker(work.path(), scenario, event, false);
        run_worker(work.path(), scenario, event, true);
    }
    eprintln!(
        "crash matrix {scenario}: {} process kills and fresh-process audits passed",
        events.len()
    );
}

#[test]
fn repository_publication_survives_every_process_crash_boundary() {
    exercise("repository");
}

async fn catalog_migration_writer(work: &Path) {
    let root = work.join("repository");
    let repository = Repository::local(&root).await.unwrap();
    publish_blob(
        &repository,
        "migration",
        b"old acknowledged migration payload",
    )
    .await;
    let snapshot = repository.metadata().snapshot().await.unwrap();
    let descriptor = snapshot.payload_catalog().unwrap();
    assert_eq!(descriptor.len(), 56);
    let digest = crate::Digest::from(<[u8; 32]>::try_from(&descriptor[24..]).unwrap());
    let path = super::chunked::sharded_path(&ObjectPath::default(), "pack-indexes", &digest);
    let object = root.join("blobs").join(path.as_ref());
    let inline = std::fs::read(&object).unwrap();
    // Exercise inline-to-external catalog recovery in the supported schema;
    // opening obsolete pre-release database schemas is intentionally rejected.
    let mut mutation = crate::MetadataMutation::new();
    mutation.set_payload_catalog(inline);
    repository
        .metadata()
        .commit(&snapshot.revision(), mutation)
        .await
        .unwrap();
    drop(snapshot);
    drop(repository);
    crate::flush_repository_leases().await.unwrap();
    std::fs::remove_file(object).unwrap();
    arm(work);
    let migrated = Repository::local(&root).await.unwrap();
    assert_eq!(
        migrated
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .payload_catalog()
            .unwrap()
            .len(),
        56
    );
    checkpoint("publication-acknowledged");
}

async fn catalog_migration_verifier(work: &Path) {
    let repository = Repository::local(work.join("repository")).await.unwrap();
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .root(&name("migration"))
            .await
            .unwrap(),
        Some(key(b"old acknowledged migration payload"))
    );
    assert_payload(
        &repository,
        &key(b"old acknowledged migration payload"),
        b"old acknowledged migration payload",
    )
    .await;
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .payload_catalog()
            .unwrap()
            .len(),
        56
    );
    repository.vacuum().await.unwrap();
    assert!(repository.fsck().await.unwrap().is_clean());
}

#[test]
fn external_catalog_migration_survives_process_death() {
    let control = tempfile::tempdir().unwrap();
    run_worker(control.path(), "catalog-migration", "control", false);
    run_worker(control.path(), "catalog-migration", "control", true);
    let trace = std::fs::read_to_string(control.path().join("trace")).unwrap();
    for required in [
        "external-catalog-durable:1",
        "before-catalog-migration:1",
        "after-catalog-migration:1",
    ] {
        assert!(
            trace.lines().any(|event| event == required),
            "missing {required}"
        );
    }
    for event in trace.lines() {
        let work = tempfile::tempdir().unwrap();
        run_worker(work.path(), "catalog-migration", event, false);
        run_worker(work.path(), "catalog-migration", event, true);
    }
}
#[test]
fn repository_catalog_rebase_survives_every_process_crash_boundary() {
    exercise("repository-rebase");
}
#[test]
fn catalog_batch_publication_survives_every_process_crash_boundary() {
    exercise("batch");
}
#[test]
fn streamed_catalog_publication_survives_every_process_crash_boundary() {
    exercise("stream");
}

fn metadata_key(value: &str) -> crate::MetadataKey {
    crate::MetadataKey::new(
        crate::NamespaceId::try_from("obrador.v1").unwrap(),
        value.to_owned(),
    )
}

async fn metadata_writer(work: &Path) {
    use crate::{MetadataChange as Change, MetadataCheck as Check};
    let repo = crate::Repository::local(work.join("repository"))
        .await
        .unwrap();
    let old = repo
        .import(crate::import::BlobImport::new(
            std::io::Cursor::new(b"old"),
            name("head"),
        ))
        .await
        .unwrap();
    let new = repo
        .import(crate::import::BlobImport::new(
            std::io::Cursor::new(b"new"),
            name("staged"),
        ))
        .await
        .unwrap();
    repo.commit(
        vec![],
        vec![
            Change::Set {
                key: metadata_key("paths/descriptor"),
                value: "old".into(),
            },
            Change::Set {
                key: metadata_key("delete"),
                value: "old".into(),
            },
        ],
    )
    .await
    .unwrap();
    arm(work);
    assert!(matches!(
        repo.commit(
            vec![Check::Root {
                name: name("head"),
                expected: Some(old)
            }],
            vec![
                Change::SetRoot {
                    name: name("head"),
                    target: new
                },
                Change::Set {
                    key: metadata_key("paths/descriptor"),
                    value: "new".into()
                },
                Change::Set {
                    key: metadata_key("referrers/target/source"),
                    value: "new".into()
                },
                Change::Delete {
                    key: metadata_key("delete")
                },
            ]
        )
        .await
        .unwrap(),
        crate::MetadataCommitResult::Committed { .. }
    ));
    checkpoint("publication-acknowledged");
}

async fn metadata_verifier(work: &Path, target: &str) {
    let repo = crate::Repository::local(work.join("repository"))
        .await
        .unwrap();
    let committed = target == "control"
        || target.starts_with("after-state-commit:")
        || target.starts_with("publication-acknowledged:");
    assert_eq!(
        repo.root(&name("head")).await.unwrap(),
        Some(key(if committed { b"new" } else { b"old" }))
    );
    let values = repo
        .get(&[
            metadata_key("paths/descriptor"),
            metadata_key("referrers/target/source"),
            metadata_key("delete"),
        ])
        .await
        .unwrap();
    assert_eq!(
        values,
        if committed {
            vec![Some("new".into()), Some("new".into()), None]
        } else {
            vec![Some("old".into()), None, Some("old".into())]
        }
    );
    // The killed writer must release locks and its abandoned transaction.
    repo.commit(
        vec![],
        vec![crate::MetadataChange::Set {
            key: metadata_key("recovery"),
            value: "ok".into(),
        }],
    )
    .await
    .unwrap();
}

#[test]
fn metadata_records_survive_process_crashes_atomically_with_roots() {
    let control = tempfile::tempdir().unwrap();
    run_worker(control.path(), "metadata", "control", false);
    run_worker(control.path(), "metadata", "control", true);
    let trace = std::fs::read_to_string(control.path().join("trace")).unwrap();
    assert!(trace.contains("state-metadata-changed:3"));
    assert!(trace.contains("after-state-commit:1"));
    for event in trace.lines() {
        let work = tempfile::tempdir().unwrap();
        run_worker(work.path(), "metadata", event, false);
        run_worker(work.path(), "metadata", event, true);
    }
}

fn operation_marker_key() -> crate::MetadataKey {
    crate::MetadataKey::new(
        crate::NamespaceId::try_from("casita.spike.operations.v1").unwrap(),
        "operation/transaction-crash",
    )
}

fn operation_marker_payload(writer: usize) -> Vec<u8> {
    let mut bytes = vec![0; 768 * 1024 + 9];
    blake3::Hasher::new()
        .update(b"operation marker shared prefix")
        .finalize_xof()
        .fill(&mut bytes[..256 * 1024]);
    blake3::Hasher::new()
        .update(format!("operation marker writer {writer}").as_bytes())
        .finalize_xof()
        .fill(&mut bytes[256 * 1024..]);
    bytes
}

fn operation_marker_value(writer: usize) -> Bytes {
    format!(
        "operation=transaction-crash;root=head;payload={:?}",
        key(&operation_marker_payload(writer))
    )
    .into()
}

fn operation_marker_checks() -> Vec<crate::MetadataCheck> {
    vec![crate::MetadataCheck::Record {
        key: operation_marker_key(),
        expected: None,
    }]
}

fn operation_marker_changes(writer: usize) -> Vec<crate::MetadataChange> {
    vec![
        crate::MetadataChange::SetRoot {
            name: name("head"),
            target: key(&operation_marker_payload(writer)),
        },
        crate::MetadataChange::Set {
            key: operation_marker_key(),
            value: operation_marker_value(writer),
        },
    ]
}

async fn operation_marker_writer(work: &Path) {
    let repository = Repository::local(work.join("repository")).await.unwrap();
    let old = operation_marker_payload(2);
    publish_blob(&repository, "head", &old).await;
    publish_blob(&repository, "retained", &old).await;
    let first_session = repository.mutation_session().await.unwrap();
    let second_session = repository.mutation_session().await.unwrap();
    let first = first_session
        .stage_blob(&operation_marker_payload(0))
        .await
        .unwrap();
    let second = second_session
        .stage_blob(&operation_marker_payload(1))
        .await
        .unwrap();
    std::fs::write(
        work.join("baseline-revision"),
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .revision()
            .to_string(),
    )
    .unwrap();
    // Fixture setup is complete. Occurrence 1 now belongs to the attempted
    // marker publication, rather than initialization or an earlier root.
    arm(work);
    let first_publish = first_session.publish_with_metadata(
        vec![first],
        operation_marker_checks(),
        operation_marker_changes(0),
    );
    let second_publish = second_session.publish_with_metadata(
        vec![second],
        operation_marker_checks(),
        operation_marker_changes(1),
    );
    let (first, second) = tokio::join!(first_publish, second_publish);
    let results = [first, second];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(
                result,
                Err(crate::repository::RepositoryError::Metadata(
                    crate::MetadataError::CheckFailed { index: 0 }
                ))
            ))
            .count(),
        1
    );
    checkpoint("publication-acknowledged");
}

async fn operation_marker_verifier(work: &Path, target: &str) {
    let repository = Repository::local(work.join("repository")).await.unwrap();
    let committed = target == "control"
        || target.starts_with("after-state-commit:")
        || target.starts_with("publication-acknowledged:");
    let snapshot = repository.metadata().snapshot().await.unwrap();
    let root = snapshot.root(&name("head")).await.unwrap().unwrap();
    let marker = snapshot
        .get(&[operation_marker_key()])
        .await
        .unwrap()
        .pop()
        .unwrap();
    let baseline = std::fs::read_to_string(work.join("baseline-revision")).unwrap();
    let winner = if committed {
        let winner = (0..2)
            .find(|writer| root == key(&operation_marker_payload(*writer)))
            .expect("committed root must identify one competing request");
        assert_eq!(
            marker,
            Some(operation_marker_value(winner)),
            "root and marker must commit together"
        );
        assert_ne!(snapshot.revision().to_string(), baseline);
        assert!(
            snapshot
                .object(&key(&operation_marker_payload(1 - winner)))
                .await
                .unwrap()
                .is_none()
        );
        Some(winner)
    } else {
        assert_eq!(
            root,
            key(&operation_marker_payload(2)),
            "uncommitted root must roll back"
        );
        assert_eq!(marker, None, "uncommitted marker must roll back");
        assert_eq!(snapshot.revision().to_string(), baseline);
        for writer in 0..2 {
            assert!(
                snapshot
                    .object(&key(&operation_marker_payload(writer)))
                    .await
                    .unwrap()
                    .is_none(),
                "uncommitted object insertion must roll back"
            );
        }
        None
    };
    assert_eq!(
        snapshot.root(&name("retained")).await.unwrap(),
        Some(key(&operation_marker_payload(2)))
    );
    drop(snapshot);
    assert_payload(
        &repository,
        &root,
        &operation_marker_payload(winner.unwrap_or(2)),
    )
    .await;
    assert_payload(
        &repository,
        &key(&operation_marker_payload(2)),
        &operation_marker_payload(2),
    )
    .await;

    // After rollback a fresh process can reuse the absent ID. After commit it
    // recovers the outcome from the marker and must reject a conflicting retry.
    let winner = match winner {
        Some(winner) => winner,
        None => {
            let session = repository.mutation_session().await.unwrap();
            let staged = session
                .stage_blob(&operation_marker_payload(0))
                .await
                .unwrap();
            session
                .publish_with_metadata(
                    vec![staged],
                    operation_marker_checks(),
                    operation_marker_changes(0),
                )
                .await
                .unwrap();
            0
        }
    };
    crate::metadata::flush_repository_leases().await.unwrap();
    let revision = repository.metadata().snapshot().await.unwrap().revision();
    let session = repository.mutation_session().await.unwrap();
    let loser = 1 - winner;
    let staged = session
        .stage_blob(&operation_marker_payload(loser))
        .await
        .unwrap();
    assert!(matches!(
        session
            .publish_with_metadata(
                vec![staged],
                operation_marker_checks(),
                operation_marker_changes(loser),
            )
            .await,
        Err(crate::repository::RepositoryError::Metadata(
            crate::MetadataError::CheckFailed { index: 0 }
        ))
    ));
    drop(session);
    crate::metadata::flush_repository_leases().await.unwrap();
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert_eq!(
        snapshot.revision(),
        revision,
        "conflicting retry cannot advance revision"
    );
    assert_eq!(
        snapshot.root(&name("head")).await.unwrap(),
        Some(key(&operation_marker_payload(winner)))
    );
    assert_eq!(
        snapshot.get(&[operation_marker_key()]).await.unwrap(),
        vec![Some(operation_marker_value(winner))]
    );
    assert!(
        snapshot
            .object(&key(&operation_marker_payload(loser)))
            .await
            .unwrap()
            .is_none()
    );
    drop(snapshot);
    repository.collect().await.unwrap();
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert_eq!(
        snapshot.root(&name("head")).await.unwrap(),
        Some(key(&operation_marker_payload(winner)))
    );
    assert_eq!(
        snapshot.root(&name("retained")).await.unwrap(),
        Some(key(&operation_marker_payload(2)))
    );
    assert_eq!(
        snapshot.get(&[operation_marker_key()]).await.unwrap(),
        vec![Some(operation_marker_value(winner))]
    );
    drop(snapshot);
    assert_payload(
        &repository,
        &key(&operation_marker_payload(winner)),
        &operation_marker_payload(winner),
    )
    .await;
    assert_payload(
        &repository,
        &key(&operation_marker_payload(2)),
        &operation_marker_payload(2),
    )
    .await;
    assert!(repository.fsck().await.unwrap().is_healthy());
}

#[test]
fn operation_marker_survives_transaction_crash_boundaries() {
    let control = tempfile::tempdir().unwrap();
    run_worker(control.path(), "operation-marker", "control", false);
    run_worker(control.path(), "operation-marker", "control", true);
    let trace = std::fs::read_to_string(control.path().join("trace")).unwrap();
    for target in [
        "state-object-inserted:1",
        "state-root-changed:1",
        "state-metadata-changed:1",
        "before-state-commit:1",
        "after-state-commit:1",
        "publication-acknowledged:1",
    ] {
        assert!(
            trace.lines().any(|event| event == target),
            "control missed {target}"
        );
        let work = tempfile::tempdir().unwrap();
        run_worker(work.path(), "operation-marker", target, false);
        run_worker(work.path(), "operation-marker", target, true);
        eprintln!("operation marker SIGKILL and fresh-process audit passed: {target}");
    }
}

#[test]
fn operation_marker_checker_rejects_wrong_commit_outcome() {
    let work = tempfile::tempdir().unwrap();
    run_worker(
        work.path(),
        "operation-marker",
        "before-state-commit:1",
        false,
    );
    // Lie to the independent reader about the crash phase. Its repository
    // audit must reject this even though opening the rolled-back file succeeds.
    let rejected = std::panic::catch_unwind(|| {
        run_worker(
            work.path(),
            "operation-marker",
            "after-state-commit:1",
            true,
        );
    });
    assert!(
        rejected.is_err(),
        "checker accepted the wrong commit outcome"
    );
    assert!(
        std::fs::read_to_string(work.path().join("verify.log"))
            .unwrap()
            .contains("committed root must identify one competing request"),
        "negative control must fail its graph audit rather than worker setup"
    );
    assert!(!work.path().join("verified").exists());
}

fn paged_contents() -> Vec<u8> {
    let mut bytes = vec![0; 2 * 1024 * 1024 + 17];
    blake3::Hasher::new()
        .update(b"shared metadata crash fixture")
        .finalize_xof()
        .fill(&mut bytes);
    bytes
}
async fn paged_overwrite_writer(work: &Path) {
    use crate::BlobSync as _;
    let repository = Repository::local_with_pack_options(
        work.join("repository"),
        crate::PackOptions {
            target_size: 64 * 1024,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let original = paged_contents();
    let old = BlobId::new(blake3::hash(&original).into());
    // Admission can collect unrooted payloads under disk pressure. Start the
    // mutation before writing and attach every fixture write to its pin.
    let mutation = repository.mutation_session().await.unwrap();
    // Fixed small chunks exercise a branch in both metadata trees without a
    // large fixture. Sync verifies the complete content before setup commits.
    mutation
        .write_scope()
        .run(async {
            let mut chunks = Vec::new();
            for bytes in original.chunks(16384) {
                let meta = crate::ChunkMeta {
                    digest: crate::ChunkId::new(blake3::hash(bytes).into()),
                    size: bytes.len() as u64,
                };
                repository
                    .payloads()
                    .put_chunk(&meta, zstd::encode_all(bytes, 0).unwrap().into())
                    .await
                    .unwrap();
                chunks.push(meta);
            }
            repository
                .payloads()
                .put_manifest(&old, chunks)
                .await
                .unwrap();
        })
        .await;
    // Exercise collection while the fixture is still unrooted regardless of
    // the host's free space. The staging pin must preserve its physical data.
    repository.collect().await.unwrap();
    let staged = mutation.stage_existing(key(&original), old).await.unwrap();
    mutation
        .publish(
            vec![staged],
            vec![
                RootChange::Set {
                    name: name("head"),
                    target: key(&original),
                },
                RootChange::Set {
                    name: name("old"),
                    target: key(&original),
                },
            ],
        )
        .await
        .unwrap();
    drop(mutation);
    arm(work);
    let mutation = repository.mutation_session().await.unwrap();
    let staged = mutation
        .stage_blob_overwrite(&key(&original), 16380, &[7; 300])
        .await
        .unwrap();
    let new = staged.record().key().clone();
    mutation
        .publish_rooted(vec![staged], name("head"), new)
        .await
        .unwrap();
    checkpoint("publication-acknowledged");
}
async fn paged_overwrite_verifier(work: &Path, target: &str) {
    let repository = Repository::local(work.join("repository")).await.unwrap();
    let original = paged_contents();
    let mut changed = original.clone();
    changed[16380..16680].fill(7);
    let committed = target == "control"
        || target.starts_with("after-state-commit:")
        || target.starts_with("publication-acknowledged:");
    let expected = if committed { &changed } else { &original };
    for collect in [false, true] {
        if collect {
            repository.collect().await.unwrap();
        }
        let snapshot = repository.metadata().snapshot().await.unwrap();
        assert_eq!(
            snapshot.root(&name("head")).await.unwrap(),
            Some(key(expected))
        );
        assert_eq!(
            snapshot.root(&name("old")).await.unwrap(),
            Some(key(&original))
        );
        drop(snapshot);
        assert_payload(&repository, &key(&original), &original).await;
        let digest = BlobId::new(blake3::hash(expected).into());
        let mut reader = repository
            .payloads()
            .open_verified(&digest, expected.len() as u64)
            .await
            .unwrap()
            .unwrap();
        let mut output = Vec::new();
        reader.read_to_end(&mut output).await.unwrap();
        assert_eq!(&output, expected);
    }
}

#[test]
fn paged_overwrite_survives_process_death_at_metadata_publication_boundaries() {
    let control = tempfile::tempdir().unwrap();
    run_worker(control.path(), "paged-overwrite", "control", false);
    run_worker(control.path(), "paged-overwrite", "control", true);
    let trace = std::fs::read_to_string(control.path().join("trace")).unwrap();
    assert!(trace.contains("pages/before-put:1"));
    assert!(trace.contains("blobs/after-put:1"));
    assert!(trace.contains("bao-packs/after-put:1"));
    assert!(trace.contains("bao-indexes/after-put:1"));
    for event in trace.lines().filter(|event| {
        [
            "pages/",
            "blobs/",
            "bao/",
            "bao-packs/",
            "bao-indexes/",
            "packs/",
            "before-state-commit:",
            "after-state-commit:",
            "publication-acknowledged:",
        ]
        .iter()
        .any(|prefix| event.starts_with(prefix))
    }) {
        let attempt = tempfile::tempdir().unwrap();
        run_worker(attempt.path(), "paged-overwrite", event, false);
        run_worker(attempt.path(), "paged-overwrite", event, true);
    }
}
