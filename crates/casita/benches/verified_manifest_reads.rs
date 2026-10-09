//! Verified reads across absent, flat, and paged manifest boundaries.
//! Fixture hashes and final output comparisons stay outside timing; all verified
//! read work, including authentication hashing, stays inside timing.
//! Each warmed iteration reads 64 prepared blobs (one repeated identity for the
//! empty case). Backend and packing vary together: memory-loose vs local-packed.
//! The memory-get-1ms backend charges 1 ms per GET or HEAD, as a stand-in for a
//! remote object store's request latency.
use casita::experimental::{BlobId, BlobStore, BlobSync, ChunkId, ChunkMeta, ChunkedBlobStore};
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main, measurement::WallTime,
};
use futures::{StreamExt, TryStreamExt, stream};
use object_store::{
    ObjectStore, ObjectStoreExt,
    local::LocalFileSystem,
    memory::InMemory,
    path::Path,
    throttle::{ThrottleConfig, ThrottledStore},
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::io::AsyncReadExt;

const BACKENDS: [&str; 3] = ["memory-loose", "local-packed", "memory-get-1ms"];

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(64)
        .enable_all()
        .build()
        .unwrap()
}

fn open(
    runtime: &tokio::runtime::Runtime,
    backend: &str,
    directory: &std::path::Path,
) -> (Arc<dyn ObjectStore>, ChunkedBlobStore) {
    let objects: Arc<dyn ObjectStore> = match backend {
        "memory-loose" => Arc::new(InMemory::new()),
        "local-packed" => Arc::new(LocalFileSystem::new_with_prefix(directory).unwrap()),
        "memory-get-1ms" => Arc::new(ThrottledStore::new(
            InMemory::new(),
            ThrottleConfig {
                wait_get_per_call: Duration::from_millis(1),
                ..Default::default()
            },
        )),
        other => unreachable!("unknown backend {other}"),
    };
    let store = if backend == "local-packed" {
        runtime
            .block_on(ChunkedBlobStore::local_packed(directory))
            .unwrap()
    } else {
        ChunkedBlobStore::new(objects.clone(), Path::default(), 262144)
    };
    (objects, store)
}

fn manifest_path(id: &BlobId) -> Path {
    let hex = id.digest().to_hex();
    Path::from(format!("blobs/b3/{}/{}", &hex[..2], hex))
}

async fn fixture(store: &ChunkedBlobStore, ordinal: usize, chunks: usize) -> (BlobId, Vec<u8>) {
    let mut payload = Vec::new();
    let mut manifest = Vec::new();
    for index in 0..chunks {
        let mut bytes = vec![0; 1024];
        for (offset, byte) in bytes.iter_mut().enumerate() {
            *byte =
                (offset.wrapping_mul(73) ^ index.wrapping_mul(31) ^ ordinal.wrapping_mul(17)) as u8;
        }
        bytes[..8].copy_from_slice(&(ordinal as u64).to_le_bytes());
        bytes[8..16].copy_from_slice(&(index as u64).to_le_bytes());
        let meta = ChunkMeta {
            digest: ChunkId::new(blake3::hash(&bytes).into()),
            size: bytes.len() as u64,
        };
        store
            .put_chunk(&meta, zstd::encode_all(bytes.as_slice(), 0).unwrap().into())
            .await
            .unwrap();
        payload.extend_from_slice(&bytes);
        manifest.push(meta);
    }
    let id = BlobId::new(blake3::hash(&payload).into());
    store.put_manifest(&id, manifest).await.unwrap();
    (id, payload)
}

/// Reads every fixture through authenticated EOF, `concurrency` at a time.
fn reads(
    group: &mut BenchmarkGroup<'_, WallTime>,
    runtime: &tokio::runtime::Runtime,
    id: BenchmarkId,
    store: &ChunkedBlobStore,
    fixtures: &[(BlobId, Vec<u8>)],
    concurrency: usize,
) {
    group.bench_function(id, |b| {
        b.iter_custom(|iterations| {
            runtime.block_on(async {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    let started = Instant::now();
                    let actual: Vec<Vec<u8>> = stream::iter(fixtures.iter())
                        .map(|(id, expected)| async {
                            let mut reader = store
                                .open_verified(id, expected.len() as u64)
                                .await?
                                .unwrap();
                            let mut actual = Vec::with_capacity(expected.len());
                            reader.read_to_end(&mut actual).await?;
                            Ok::<_, casita::experimental::Error>(actual)
                        })
                        .buffered(concurrency)
                        .try_collect()
                        .await
                        .unwrap();
                    elapsed += started.elapsed();
                    assert_eq!(actual.len(), fixtures.len());
                    for (actual, (_, expected)) in actual.iter().zip(fixtures) {
                        assert_eq!(actual, expected);
                    }
                }
                elapsed
            })
        });
    });
}

fn verified_manifest_reads(c: &mut Criterion) {
    let runtime = runtime();
    let mut group = c.benchmark_group("verified_manifest_reads");
    group
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(1));
    for backend in BACKENDS {
        // Explicit chunk counts cover both sides of the 64-entry flat/page boundary.
        // Empty payloads have a real flat manifest; one self chunk elides it.
        // Request latency only needs the absent manifest and its flat neighbours.
        for chunks in [0usize, 1, 2, 63, 64, 65] {
            if backend == "memory-get-1ms" && chunks > 2 {
                continue;
            }
            let directory = tempfile::tempdir().unwrap();
            let (objects, store) = open(&runtime, backend, directory.path());
            let fixtures: Vec<_> = runtime.block_on(async {
                let mut fixtures = Vec::new();
                for ordinal in 0..64 {
                    fixtures.push(fixture(&store, ordinal, chunks).await);
                }
                store.flush().await.unwrap();
                fixtures
            });
            // Verify the intended physical format, rather than assuming the fixture
            // construction happened to cross the desired branch.
            runtime.block_on(async {
                match objects.get(&manifest_path(&fixtures[0].0)).await {
                    Err(object_store::Error::NotFound { .. }) => assert_eq!(chunks, 1),
                    Ok(object) => {
                        assert_ne!(chunks, 1, "a single self chunk must elide its manifest");
                        let bytes = object.bytes().await.unwrap();
                        if chunks <= 64 {
                            assert_eq!(bytes.len(), 8 + chunks * 40);
                            assert_eq!(
                                u64::from_le_bytes(bytes[..8].try_into().unwrap()),
                                chunks as u64
                            );
                        } else {
                            assert_eq!(bytes.len(), 56);
                            assert!(bytes.starts_with(b"CASPAGE1"));
                        }
                    }
                    Err(error) => panic!("manifest fixture: {error}"),
                }
            });
            for concurrency in [1usize, 64] {
                let id = BenchmarkId::new(format!("{backend}/readers_{concurrency}"), chunks);
                reads(&mut group, &runtime, id, &store, &fixtures, concurrency);
            }
        }
    }
    group.finish();
}

// Each fixture is one self-addressed chunk. Both sides of the Bao group
// boundary are measured; larger reads retain the proof encode/decode path.
fn verified_bare_group_reads(c: &mut Criterion) {
    let runtime = runtime();
    let mut group = c.benchmark_group("verified_bare_group_reads");
    group
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(1));
    for backend in BACKENDS {
        for size in [1usize, 16383, 16384, 16385, 32768] {
            // Under request latency, the group boundary is enough.
            if backend == "memory-get-1ms" && !matches!(size, 16384 | 16385) {
                continue;
            }
            let directory = tempfile::tempdir().unwrap();
            let (objects, store) = open(&runtime, backend, directory.path());
            let fixtures: Vec<_> = runtime.block_on(async {
                let mut fixtures = Vec::new();
                for ordinal in 0..64usize {
                    let bytes: Vec<_> = (0..size)
                        .map(|offset| (offset.wrapping_mul(73) ^ ordinal.wrapping_mul(17)) as u8)
                        .collect();
                    let id = store.put_slice(&bytes).await.unwrap();
                    assert_eq!(id, BlobId::new(blake3::hash(&bytes).into()));
                    assert!(matches!(
                        objects.head(&manifest_path(&id)).await,
                        Err(object_store::Error::NotFound { .. })
                    ));
                    fixtures.push((id, bytes));
                }
                store.flush().await.unwrap();
                fixtures
            });
            for concurrency in [1usize, 64] {
                let id = BenchmarkId::new(format!("{backend}/readers_{concurrency}"), size);
                reads(&mut group, &runtime, id, &store, &fixtures, concurrency);
            }
        }
    }
    group.finish();
}

criterion_group!(benches, verified_manifest_reads, verified_bare_group_reads);
criterion_main!(benches);
