//! Raw-blob publication to WAL3 before and after an object-shard checkpoint.
//! The `b` cases checkpoint one batch, so batches of 511, 512 and 513 records
//! leave its shard on both sides of a 512-record block. The `c` cases
//! checkpoint a larger corpus, so a 512-record publication spreads over many
//! blocks and, in a large enough corpus, over several shards.

use super::*;
use crate::metadata::Wal3MetadataStore;
use crate::{Digest, MemoryBlobStore};
use std::time::Instant;

type ProbeRepository = Repository<MemoryBlobStore, Wal3MetadataStore>;

/// Records published after a corpus checkpoint: one full object block.
const CORPUS_BATCH: usize = 512;

fn body(index: usize) -> [u8; 8] {
    (index as u64).to_le_bytes()
}

fn blob_key(index: usize) -> ObjectKey {
    ObjectKey::blob(BlobId::new(Digest::hash(&body(index))))
}

/// Publish `batch` raw blobs, staged outside the timed region; returns shard writes.
async fn publish(repository: &ProbeRepository, start: usize, batch: usize, prefix: &str) -> u64 {
    let session = repository.mutation_session().await.unwrap();
    let mut staged = Vec::with_capacity(batch);
    for index in start..start + batch {
        staged.push(session.stage_blob(&body(index)).await.unwrap());
    }
    repository.metadata().reset_read_stats();
    let started = Instant::now();
    let result = session
        .publish_filesystem_constructed(staged, Vec::new())
        .await
        .unwrap();
    let nanos = started.elapsed().as_nanos();
    assert_eq!(result.objects_inserted, batch);
    let stats = repository.metadata().read_stats();
    println!(
        "{prefix}_nanos {nanos} {prefix}_shard_gets {} {prefix}_shard_get_bytes {} \
         {prefix}_shard_cache_hits {} {prefix}_shard_puts {} {prefix}_fragment_gets {} \
         {prefix}_fragment_puts {} {prefix}_manifest_gets {} {prefix}_tail_deltas {}",
        stats.logical_shard_get_requests,
        stats.logical_shard_get_bytes,
        stats.logical_shard_cache_hits,
        stats.logical_shard_put_requests,
        stats.fragment_get_requests,
        stats.fragment_put_requests,
        stats.manifest_load_requests,
        stats.tail_deltas,
    );
    stats.logical_shard_put_requests
}

/// Publish `corpus` raw blobs untimed, in publications of the mutation limit.
async fn load(repository: &ProbeRepository, corpus: usize) {
    let limit = repository.limits().max_batch_objects;
    for start in (0..corpus).step_by(limit) {
        let session = repository.mutation_session().await.unwrap();
        let mut staged = Vec::with_capacity(limit);
        for index in start..corpus.min(start + limit) {
            staged.push(session.stage_blob(&body(index)).await.unwrap());
        }
        let count = staged.len();
        let result = session
            .publish_filesystem_constructed(staged, Vec::new())
            .await
            .unwrap();
        assert_eq!(result.objects_inserted, count);
    }
}

/// Commit empty metadata edits until the next full checkpoint writes object
/// shards, which they reach without changing the object corpus.
async fn checkpoint(repository: &ProbeRepository, prefix: &str) {
    repository.metadata().reset_read_stats();
    let mut checkpoint_commits = None;
    for commits in 1..=16 {
        let revision = repository.metadata().snapshot().await.unwrap().revision();
        repository
            .metadata()
            .commit(&revision, MetadataMutation::new())
            .await
            .unwrap();
        let stats = repository.metadata().read_stats();
        if stats.logical_shard_put_requests > 0 {
            assert_eq!(stats.tail_deltas, 0);
            checkpoint_commits = Some(commits);
            break;
        }
    }
    let (shards, blocks) = repository.metadata().checkpoint_object_layout();
    println!(
        "{prefix}_checkpoint_commits {} {prefix}_checkpoint_shards {shards} \
         {prefix}_checkpoint_blocks {blocks}",
        checkpoint_commits.expect("checkpoint reached")
    );
}

/// Look up `keys`, which must all be present raw blobs without closure
/// witnesses, then fsck all `objects`, which must all be unrooted.
async fn validate(repository: &ProbeRepository, keys: &[ObjectKey], objects: usize, prefix: &str) {
    let snapshot = repository.metadata().snapshot().await.unwrap();
    let records = snapshot.object_batch(keys).await.unwrap();
    assert_eq!(records.len(), keys.len());
    assert!(
        records
            .iter()
            .zip(keys)
            .all(|(record, key)| record.as_ref().is_some_and(|record| record.key() == key))
    );
    assert!(
        snapshot
            .validated_closures(keys)
            .await
            .unwrap()
            .into_iter()
            .all(|stored| !stored),
        "raw blobs must not acquire stored closure witnesses"
    );
    drop(snapshot);
    let fsck = repository.fsck().await.unwrap();
    assert_eq!(fsck.objects_checked, objects);
    assert_eq!(fsck.payloads_checked, objects);
    assert_eq!(fsck.issues.len(), objects, "{fsck:?}");
    assert!(
        fsck.issues.iter().all(|issue| {
            issue.kind == FsckIssueKind::UnrootedObject
                && issue.disposition == FsckDisposition::Collectible
        }),
        "only the deliberately unrooted objects may be reported: {fsck:?}"
    );
    println!("{prefix}_validated 1");
}

async fn open(storage: &Arc<chroma_storage::Storage>, writer: &str) -> Wal3MetadataStore {
    Wal3MetadataStore::open(storage.clone(), "probe/state", writer)
        .await
        .unwrap()
}

fn local_storage(directory: &tempfile::TempDir) -> Arc<chroma_storage::Storage> {
    Arc::new(chroma_storage::Storage::Local(
        chroma_storage::local::LocalStorage::new(directory.path().to_str().unwrap()),
    ))
}

/// Replace `repository` with one on a freshly opened handle to the same state.
async fn reopen(
    repository: ProbeRepository,
    storage: &Arc<chroma_storage::Storage>,
) -> ProbeRepository {
    let payloads = repository.payloads().clone();
    drop(repository);
    crate::flush_repository_leases().await.unwrap();
    Repository::new(payloads, open(storage, "reopened").await)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "run through benchmark run wal3-publication-checkpoints"]
async fn benchmark_wal3_publication_checkpoints() {
    for batch in [511, 512, 513] {
        for reopened in [false, true] {
            let temperature = if reopened { "reopened" } else { "warm" };
            let prefix = format!("wal3_b{batch}_{temperature}");
            let directory = tempfile::tempdir().unwrap();
            let storage = local_storage(&directory);
            let mut repository =
                Repository::new(MemoryBlobStore::new(), open(&storage, "first").await);
            assert_eq!(
                publish(&repository, 0, batch, &format!("{prefix}_before")).await,
                0,
                "the control must precede object-shard checkpointing"
            );
            checkpoint(&repository, &prefix).await;
            if reopened {
                repository = reopen(repository, &storage).await;
            }
            publish(&repository, batch, batch, &format!("{prefix}_after")).await;
            let keys = (0..2 * batch).map(blob_key).collect::<Vec<_>>();
            validate(&repository, &keys, 2 * batch, &prefix).await;
            drop(repository);
            crate::flush_repository_leases().await.unwrap();
        }
    }

    let corpora = std::env::var("CASITA_WAL3_CHECKPOINT_CORPORA")
        .unwrap_or_else(|_| "8192,131072".to_owned());
    for corpus in corpora.split(',').map(|corpus| {
        corpus
            .parse::<usize>()
            .expect("CASITA_WAL3_CHECKPOINT_CORPORA lists object counts")
    }) {
        assert!(corpus > 0, "a checkpoint corpus must hold objects");
        for reopened in [false, true] {
            let temperature = if reopened { "reopened" } else { "warm" };
            let prefix = format!("wal3_c{corpus}_{temperature}");
            let directory = tempfile::tempdir().unwrap();
            let storage = local_storage(&directory);
            let mut repository =
                Repository::new(MemoryBlobStore::new(), open(&storage, "first").await);
            load(&repository, corpus).await;
            checkpoint(&repository, &prefix).await;
            if reopened {
                repository = reopen(repository, &storage).await;
            }
            publish(
                &repository,
                corpus,
                CORPUS_BATCH,
                &format!("{prefix}_after"),
            )
            .await;
            // Every new record, and a spread of checkpointed ones found
            // through the shards; fsck covers the whole corpus.
            let keys = (0..corpus)
                .step_by(61)
                .chain(corpus..corpus + CORPUS_BATCH)
                .map(blob_key)
                .collect::<Vec<_>>();
            validate(&repository, &keys, corpus + CORPUS_BATCH, &prefix).await;
            drop(repository);
            crate::flush_repository_leases().await.unwrap();
        }
    }
}
