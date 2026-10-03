//! Raw-blob publication and closure checks on every metadata backend.
//!
//! Built-in raw blobs are complete by construction: publication stores no
//! witness for them and incremental closure checks never reopen them. Imports
//! publish blobs as filesystem-constructed closures, the path that formerly
//! stored a witness per blob. Its batch sizes straddle the WAL3 tail limits
//! that a 70-byte witness per blob would cross (about 700 and 1,100 blobs per
//! batch), so a regression to stored witnesses shows up as extra checkpoints
//! on one side of a boundary. Unrooted publication, which never stored blob
//! witnesses, is the control.

use super::*;
use std::time::Instant;

/// Kept in step with `RAW_BLOB_BATCHES` in `benchmarks/suites/native_probes.py`.
const BATCHES: [usize; 3] = [512, 1024, 4096];

fn configured(name: &str, default: usize) -> usize {
    std::env::var(name).ok().map_or(default, |value| {
        value.parse().expect("numeric probe setting")
    })
}

fn body(index: usize) -> [u8; 8] {
    (index as u64).to_le_bytes()
}

fn blob_keys(blobs: std::ops::Range<usize>) -> Vec<ObjectKey> {
    blobs
        .map(|index| ObjectKey::blob(BlobId::new(Digest::hash(&body(index)))))
        .collect()
}

#[derive(Clone, Copy)]
enum BlobPublication {
    /// An import's construction proof, which once witnessed every blob.
    Constructed,
    /// Plain unrooted records, which never carried blob witnesses.
    Unrooted,
}

/// Publish the raw blobs in `blobs`, timing only publication. Returns the
/// nanoseconds spent publishing and the number of records inserted.
async fn publish_blobs<PS: BlobStore, SS: MetadataStore>(
    repository: &Repository<PS, SS>,
    blobs: std::ops::Range<usize>,
    batch: usize,
    publication: BlobPublication,
) -> (u128, usize) {
    let mut nanos = 0;
    let mut inserted = 0;
    for start in blobs.clone().step_by(batch) {
        let session = repository.mutation_session().await.unwrap();
        let mut staged = Vec::with_capacity(batch);
        for index in start..(start + batch).min(blobs.end) {
            staged.push(session.stage_blob(&body(index)).await.unwrap());
        }
        let started = Instant::now();
        let result = match publication {
            BlobPublication::Constructed => session
                .publish_filesystem_constructed(staged, Vec::new())
                .await
                .unwrap(),
            BlobPublication::Unrooted => session.publish_unrooted(staged).await.unwrap(),
        };
        nanos += started.elapsed().as_nanos();
        inserted += result.objects_inserted;
    }
    (nanos, inserted)
}

/// One fresh repository's measurements, gated on the derived semantics.
async fn measure<PS: BlobStore, SS: MetadataStore>(
    label: &str,
    repository: &Repository<PS, SS>,
    blobs: usize,
    batch: usize,
    after_phase: impl Fn(&str),
) {
    use BlobPublication::{Constructed, Unrooted};
    let (constructed, inserted) = publish_blobs(repository, 0..blobs, batch, Constructed).await;
    assert_eq!(inserted, blobs, "every constructed blob is new");
    after_phase("constructed");
    // An unchanged republication inserts nothing, so a WAL3 delta carries no
    // object; the witness gate below covers the blobs' witnesses.
    let (republished, inserted) = publish_blobs(repository, 0..blobs, batch, Constructed).await;
    assert_eq!(inserted, 0, "republication must insert no objects");
    after_phase("republished");
    let (unrooted, inserted) = publish_blobs(repository, blobs..2 * blobs, batch, Unrooted).await;
    assert_eq!(inserted, blobs, "every unrooted blob is new");
    after_phase("unrooted");

    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert!(
        snapshot
            .validated_closures(&blob_keys(0..2 * blobs))
            .await
            .unwrap()
            .into_iter()
            .all(|witnessed| !witnessed),
        "raw blobs must not store closure witnesses, however they are published"
    );
    drop(snapshot);

    // Root one directory over a bounded prefix of the published blobs.
    let width = blobs.min(1024);
    let session = repository.mutation_session().await.unwrap();
    let directory = Directory::try_from_iter((0..width).map(|index| {
        (
            PathComponent::try_from(format!("f{index:05}").as_str()).unwrap(),
            Node::File {
                digest: BlobId::new(Digest::hash(&body(index))),
                size: 8,
                executable: false,
            },
        )
    }))
    .unwrap();
    let staged = session.stage_directory(&directory).await.unwrap();
    let root = staged.record().key().clone();
    let started = Instant::now();
    session
        .publish_rooted(
            vec![staged],
            RootName::try_from("probe").unwrap(),
            root.clone(),
        )
        .await
        .unwrap();
    let rooting = started.elapsed().as_nanos();
    drop(session);
    // The root's witness settles an incremental check at the root itself; an
    // audit still reads every blob beneath it.
    let started = Instant::now();
    let status = repository.verify_closure_incremental(&root).await.unwrap();
    let incremental = started.elapsed().as_nanos();
    assert_eq!(status, ClosureStatus::Complete { objects: 1 });
    let started = Instant::now();
    let status = repository.verify_closure(&root).await.unwrap();
    let audit = started.elapsed().as_nanos();
    assert_eq!(status, ClosureStatus::Complete { objects: width + 1 });
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .validated_closures(std::slice::from_ref(&root))
            .await
            .unwrap(),
        [true],
        "named roots keep their witness"
    );
    println!(
        "{label}_b{batch}_constructed_publish_nanos {constructed} \
         {label}_b{batch}_republish_nanos {republished} \
         {label}_b{batch}_unrooted_publish_nanos {unrooted} \
         {label}_b{batch}_rooting_nanos {rooting} \
         {label}_b{batch}_incremental_check_nanos {incremental} \
         {label}_b{batch}_audit_nanos {audit}"
    );
}

fn directory_bytes(path: &Path) -> u64 {
    let mut total = 0;
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                pending.push(entry.path());
            } else {
                total += metadata.len();
            }
        }
    }
    total
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "run through benchmark run raw-blob-closures"]
async fn benchmark_raw_blob_closures() {
    let blobs = configured("CASITA_RAW_BLOB_BENCH_BLOBS", 8192);
    // A smaller range would publish the larger batches as one short batch and
    // report it under the wrong size, losing the far side of the tail limits.
    assert!(
        blobs >= BATCHES[BATCHES.len() - 1],
        "CASITA_RAW_BLOB_BENCH_BLOBS must cover the largest batch"
    );
    println!("raw_blob_blobs {blobs}");
    for batch in BATCHES {
        let repository = Repository::new(
            crate::MemoryBlobStore::new(),
            crate::MemoryMetadataStore::new().unwrap(),
        );
        measure("memory", &repository, blobs, batch, |_| {}).await;

        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::local(directory.path()).await.unwrap();
        measure("local", &repository, blobs, batch, |_| {}).await;
        repository.flush().await.unwrap();
        drop(repository);

        #[cfg(feature = "s3")]
        {
            let directory = tempfile::tempdir().unwrap();
            let storage = Arc::new(chroma_storage::Storage::Local(
                chroma_storage::local::LocalStorage::new(directory.path().to_str().unwrap()),
            ));
            let state = crate::metadata::Wal3MetadataStore::open(storage, "probe/state", "probe")
                .await
                .unwrap();
            let repository = Repository::new(crate::MemoryBlobStore::new(), state);
            let path = directory.path().to_path_buf();
            let stats = |phase: &str| {
                let read = repository.metadata().read_stats();
                println!(
                    "wal3_b{batch}_{phase}_fragment_puts {} wal3_b{batch}_{phase}_shard_puts {} \
                     wal3_b{batch}_{phase}_storage_bytes {}",
                    read.fragment_put_requests,
                    read.logical_shard_put_requests,
                    directory_bytes(&path)
                );
                repository.metadata().reset_read_stats();
            };
            repository.metadata().reset_read_stats();
            measure("wal3", &repository, blobs, batch, stats).await;
        }
    }
    crate::flush_repository_leases().await.unwrap();
}

/// Stage one writer's chain of `depth` directories, each holding `files`
/// distinct files and the previous directory. Returns the staged objects in
/// post-order and the chain's top directory.
async fn stage_chain<'hold, PS: BlobStore, SS: MetadataStore>(
    session: &'hold MutationSession<'_, PS, SS>,
    writer: usize,
    depth: usize,
    files: usize,
) -> (Vec<StagedObject<'hold>>, ObjectKey) {
    let mut staged = Vec::new();
    let mut child: Option<Directory> = None;
    for level in 0..depth {
        let mut entries = Vec::with_capacity(files + 1);
        for index in 0..files {
            let bytes = format!("writer {writer} level {level} file {index}");
            let blob = session.stage_blob(bytes.as_bytes()).await.unwrap();
            entries.push((
                PathComponent::try_from(format!("f{index:04}").as_str()).unwrap(),
                Node::File {
                    digest: blob.record().payload(),
                    size: blob.record().payload_size(),
                    executable: false,
                },
            ));
            staged.push(blob);
        }
        if let Some(child) = &child {
            entries.push((
                PathComponent::try_from("child").unwrap(),
                Node::Directory {
                    digest: child.digest(),
                    size: child.size(),
                },
            ));
        }
        let directory = Directory::try_from_iter(entries).unwrap();
        staged.push(session.stage_directory(&directory).await.unwrap());
        child = Some(directory);
    }
    let top = staged.last().unwrap().record().key().clone();
    (staged, top)
}

/// Publish one rooted chain per writer, all writers at once. Returns the wall
/// time of the concurrent publications alone.
async fn publish_concurrently<PS, SS>(
    repository: &Repository<PS, SS>,
    writers: usize,
    depth: usize,
    files: usize,
) -> u128
where
    PS: BlobGc + 'static,
    SS: MetadataStore + 'static,
{
    let start = Arc::new(tokio::sync::Barrier::new(writers + 1));
    let mut tasks = Vec::with_capacity(writers);
    for writer in 0..writers {
        let repository = repository.clone();
        let start = start.clone();
        tasks.push(tokio::spawn(async move {
            let session = repository.mutation_session().await.unwrap();
            let (staged, top) = stage_chain(&session, writer, depth, files).await;
            start.wait().await;
            let name = RootName::try_from(format!("writer/{writer}").as_str()).unwrap();
            session
                .publish_rooted(staged, name, top.clone())
                .await
                .unwrap();
            top
        }));
    }
    start.wait().await;
    let started = Instant::now();
    let mut tops = Vec::with_capacity(writers);
    for task in tasks {
        tops.push(task.await.unwrap());
    }
    let wall = started.elapsed().as_nanos();
    for top in tops {
        assert_eq!(
            repository.verify_closure(&top).await.unwrap(),
            ClosureStatus::Complete {
                objects: depth * (files + 1)
            },
            "every concurrent publication must be complete"
        );
    }
    wall
}

/// Concurrent publishers verify their closures once, under the commit lock.
/// A single writer is the uncontended baseline; with several, the phases
/// show how long each writer queues for the lock and how long its walk holds
/// it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "run through benchmark run concurrent-publication"]
async fn benchmark_concurrent_publication() {
    let depth = configured("CASITA_PUBLICATION_BENCH_DEPTH", 64);
    let files = configured("CASITA_PUBLICATION_BENCH_FILES", 16);
    println!("publication_depth {depth} publication_files {files}");
    for writers in [1, 4, 16] {
        for local in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let label = if local { "local" } else { "memory" };
            let (wall, before, after) = if local {
                let repository = Repository::local(directory.path()).await.unwrap();
                let before = repository.publication_profile();
                let wall = publish_concurrently(&repository, writers, depth, files).await;
                let after = repository.publication_profile();
                repository.flush().await.unwrap();
                (wall, before, after)
            } else {
                let repository = Repository::new(
                    crate::MemoryBlobStore::new(),
                    crate::MemoryMetadataStore::new().unwrap(),
                );
                let before = repository.publication_profile();
                let wall = publish_concurrently(&repository, writers, depth, files).await;
                (wall, before, repository.publication_profile())
            };
            let mut line = format!("{label}_w{writers}_wall_nanos {wall}");
            for (index, phase) in PUBLICATION_PHASES.iter().enumerate() {
                line.push_str(&format!(
                    " {label}_w{writers}_{phase}_nanos {}",
                    after.nanos[index] - before.nanos[index]
                ));
            }
            println!("{line}");
        }
    }
    crate::flush_repository_leases().await.unwrap();
}
