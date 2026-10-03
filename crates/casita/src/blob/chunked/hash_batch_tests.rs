//! Observe actual hash-job inputs without replacing hashing or storage.

use super::ChunkedBlobStore;
use crate::blob::{BlobStore, ChunkMeta};
use crate::digest::{BlobId, ChunkId};
use object_store::path::Path;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncReadExt;

struct Observation {
    digests: Vec<ChunkId>,
    /// Blocking jobs, as chunk count and bytes.
    jobs: Mutex<Vec<(usize, usize)>>,
    /// Lone small chunks hashed without a blocking job.
    inline: Mutex<usize>,
}
static OBSERVATIONS: Mutex<Vec<Arc<Observation>>> = Mutex::new(Vec::new());
struct Registration(Arc<Observation>);
impl Drop for Registration {
    fn drop(&mut self) {
        OBSERVATIONS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|entry| !Arc::ptr_eq(entry, &self.0));
    }
}

pub(super) fn record_hash_job<'a>(chunks: impl IntoIterator<Item = &'a [u8]>, inline: bool) {
    let observations = OBSERVATIONS.lock().unwrap().clone();
    if observations.is_empty() {
        return;
    }
    let digests: Vec<_> = chunks
        .into_iter()
        .map(|bytes| (ChunkId::new(blake3::hash(bytes).into()), bytes.len()))
        .collect();
    for observation in observations.iter() {
        let matched: Vec<_> = digests
            .iter()
            .filter(|(digest, _)| observation.digests.contains(digest))
            .collect();
        if matched.is_empty() {
            continue;
        }
        if inline {
            *observation.inline.lock().unwrap() += matched.len();
        } else {
            observation
                .jobs
                .lock()
                .unwrap()
                .push((matched.len(), matched.iter().map(|(_, bytes)| bytes).sum()));
        }
    }
}

fn data(size: usize, seed: u64) -> Vec<u8> {
    let mut state = 0x752a_1cb3_e465_a80du64 ^ seed;
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

#[tokio::test]
async fn writer_hashes_ready_chunks_in_fewer_jobs_without_changing_content() {
    let bytes = data(256 * 1024, 1);
    let expected: Vec<_> = fastcdc::v2020::FastCDC::new(&bytes, 512, 1024, 2048)
        .map(|chunk| ChunkMeta {
            digest: ChunkId::new(
                blake3::hash(&bytes[chunk.offset..chunk.offset + chunk.length]).into(),
            ),
            size: chunk.length as u64,
        })
        .collect();
    assert!(expected.len() > 16);
    let observed = Registration(Arc::new(Observation {
        digests: expected.iter().map(|chunk| chunk.digest).collect(),
        jobs: Mutex::new(Vec::new()),
        inline: Mutex::new(0),
    }));
    OBSERVATIONS.lock().unwrap().push(observed.0.clone());
    let store = ChunkedBlobStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        Path::default(),
        1024,
    )
    .with_chunk_upload_concurrency(16.try_into().unwrap())
    .with_chunk_memory_budget_bytes(1024 * 1024);
    let id = store.put_slice(&bytes).await.unwrap();
    assert_eq!(id, BlobId::new(blake3::hash(&bytes).into()));
    assert_eq!(store.chunks(&id).await.unwrap().unwrap(), expected);
    let mut actual = Vec::new();
    store
        .open_read(&id)
        .await
        .unwrap()
        .unwrap()
        .read_to_end(&mut actual)
        .await
        .unwrap();
    assert_eq!(actual, bytes);
    let jobs = observed.0.jobs.lock().unwrap().clone();
    let inline = *observed.0.inline.lock().unwrap();
    assert_eq!(
        jobs.iter().map(|(count, _)| count).sum::<usize>() + inline,
        expected.len(),
        "observe every real chunk hash"
    );
    assert!(
        jobs.iter()
            .all(|(count, bytes)| { (1..=4).contains(count) && *bytes <= 1024 * 1024 })
    );
    // Source readiness and upload completions legitimately flush partial
    // batches. A fixed percentage reduction is not a scheduling invariant;
    // the permanent benchmark measures the size of the improvement.
    assert!(
        jobs.len() < expected.len(),
        "ready chunks should amortize blocking-pool handoffs: {} jobs for {} chunks",
        jobs.len(),
        expected.len()
    );
}

#[test]
fn partial_hash_batches_progress_with_small_budgets_and_one_blocking_thread() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let bytes = data(32768, 3);
        for budget in [65536, 131072, 262144] {
            for concurrency in [1, 3, 4, 5] {
                let store = ChunkedBlobStore::new(
                    Arc::new(object_store::memory::InMemory::new()),
                    Path::default(),
                    1024,
                )
                .with_chunk_upload_concurrency(concurrency.try_into().unwrap())
                .with_chunk_memory_budget_bytes(budget);
                for _ in 0..2 {
                    let id = tokio::time::timeout(
                        std::time::Duration::from_secs(3),
                        store.put_slice(&bytes),
                    )
                    .await
                    .expect("partial batches must release admission")
                    .unwrap();
                    assert_eq!(id, BlobId::new(blake3::hash(&bytes).into()));
                    let mut actual = Vec::new();
                    store
                        .open_read(&id)
                        .await
                        .unwrap()
                        .unwrap()
                        .read_to_end(&mut actual)
                        .await
                        .unwrap();
                    assert_eq!(actual, bytes);
                    assert_eq!(store.chunk_memory_budget.free_bytes(), budget);
                }
            }
        }
    });
}

#[tokio::test]
async fn hash_jobs_bound_count_and_bytes_and_isolate_oversized_chunks() {
    use super::hash_batch::HashBatch;
    use crate::byte_budget::ByteBudget;
    let sizes = [1, 2, 3, 4, 524288, 524288, 524289, 524288, 1048577, 7];
    let inputs: Vec<_> = sizes
        .iter()
        .enumerate()
        .map(|(i, size)| data(*size, 100 + i as u64))
        .collect();
    let digests: Vec<_> = inputs
        .iter()
        .map(|bytes| ChunkId::new(blake3::hash(bytes).into()))
        .collect();
    let observed = Registration(Arc::new(Observation {
        digests: digests.clone(),
        jobs: Mutex::new(Vec::new()),
        inline: Mutex::new(0),
    }));
    OBSERVATIONS.lock().unwrap().push(observed.0.clone());
    let budget = ByteBudget::new(8 * 1024 * 1024);
    let mut batch = HashBatch::default();
    let mut receivers = Vec::new();
    for bytes in inputs {
        let permit = budget.reserve(bytes.len()).await;
        receivers.push(batch.push(bytes, permit));
    }
    batch.flush();
    for (receiver, digest) in receivers.into_iter().zip(digests) {
        let hashed = receiver.await.unwrap();
        assert_eq!(hashed.digest, digest);
    }
    let jobs = observed.0.jobs.lock().unwrap().clone();
    // The final flush holds only the 7-byte chunk, which is hashed inline.
    assert_eq!(*observed.0.inline.lock().unwrap(), 1);
    assert_eq!(
        jobs.iter().map(|(count, _)| count).sum::<usize>(),
        sizes.len() - 1
    );
    assert!(
        jobs.iter()
            .all(|(count, bytes)| *count <= 4 && (*bytes <= 1048576 || *count == 1))
    );
    assert!(
        jobs.contains(&(4, 10)),
        "count boundary must batch four small chunks"
    );
    assert!(
        jobs.contains(&(2, 1048576)),
        "exact byte boundary must remain one job"
    );
    assert!(
        jobs.contains(&(1, 1048577)),
        "oversized admission must run alone"
    );
    assert_eq!(budget.free_bytes(), 8 * 1024 * 1024);
}

#[test]
fn cancelled_hash_group_keeps_all_queued_chunk_permits() {
    use super::hash_batch::HashBatch;
    use crate::byte_budget::ByteBudget;
    struct Release(Option<std::sync::mpsc::Sender<()>>);
    impl Drop for Release {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let budget = ByteBudget::new(4 * 65536);
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let release = Release(Some(release));
        let blocker = tokio::task::spawn_blocking(move || {
            entered.send(()).unwrap();
            wait.recv().unwrap();
        });
        started.await.unwrap();
        let mut batch = HashBatch::default();
        let mut receivers = Vec::new();
        for _ in 0..4 {
            receivers.push(batch.push(vec![19; 65536], budget.reserve(65536).await));
        }
        assert_eq!(budget.free_bytes(), 0);
        drop(receivers);
        drop(batch);
        assert_eq!(
            budget.free_bytes(),
            0,
            "all four queued buffers still own admission"
        );
        drop(release);
        blocker.await.unwrap();
        let permit =
            tokio::time::timeout(std::time::Duration::from_secs(3), budget.reserve(4 * 65536))
                .await
                .unwrap();
        drop(permit);
        assert_eq!(budget.free_bytes(), 4 * 65536);
    });
}

#[tokio::test]
async fn only_a_lone_chunk_within_the_inline_bound_skips_the_blocking_pool() {
    use super::hash_batch::{HashBatch, INLINE_BYTES};
    use crate::byte_budget::ByteBudget;
    let budget = ByteBudget::new(1024 * 1024);
    // One flush per group: (chunk sizes, blocking jobs, inline hashes).
    let groups: [(&[usize], usize, usize); 4] = [
        (&[INLINE_BYTES], 0, 1),
        (&[INLINE_BYTES + 1], 1, 0),
        (&[1, 1], 1, 0),
        (&[INLINE_BYTES - 1], 0, 1),
    ];
    for (index, (sizes, jobs, inline)) in groups.into_iter().enumerate() {
        let inputs: Vec<_> = sizes
            .iter()
            .enumerate()
            .map(|(chunk, size)| data(*size, 1000 * index as u64 + chunk as u64))
            .collect();
        let digests: Vec<_> = inputs
            .iter()
            .map(|bytes| ChunkId::new(blake3::hash(bytes).into()))
            .collect();
        let observed = Registration(Arc::new(Observation {
            digests: digests.clone(),
            jobs: Mutex::new(Vec::new()),
            inline: Mutex::new(0),
        }));
        OBSERVATIONS.lock().unwrap().push(observed.0.clone());
        let mut batch = HashBatch::default();
        let mut receivers = Vec::new();
        for bytes in inputs {
            let permit = budget.reserve(bytes.len()).await;
            receivers.push(batch.push(bytes, permit));
        }
        batch.flush();
        for (receiver, digest) in receivers.into_iter().zip(digests) {
            assert_eq!(receiver.await.unwrap().digest, digest);
        }
        assert_eq!(observed.0.jobs.lock().unwrap().len(), jobs, "{sizes:?}");
        assert_eq!(*observed.0.inline.lock().unwrap(), inline, "{sizes:?}");
    }
    assert_eq!(budget.free_bytes(), 1024 * 1024);
}
