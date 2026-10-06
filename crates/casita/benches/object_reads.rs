//! Immutable read latency with and without a persistent metadata snapshot.
use casita::{ObjectKey, ObjectReader, Repository, RetainedReader};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::io::AsyncReadExt;

enum Session {
    Snapshot(RetainedReader),
    Objects(ObjectReader),
}

impl Session {
    async fn read(&self, key: &ObjectKey, expected: &[u8]) {
        let mut reader = match self {
            Self::Snapshot(session) => session.open_verified(key).await,
            Self::Objects(session) => session.open_verified(key).await,
        }
        .unwrap()
        .unwrap();
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, expected);
    }
}

fn object_reads(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("protected_object_reads");
    group.sample_size(10);
    for size in [128, 4096] {
        let directory = tempfile::tempdir().unwrap();
        let (repository, fixtures) = runtime.block_on(async {
            let repository = Repository::local(directory.path()).await.unwrap();
            let mut fixtures = Vec::new();
            for index in 0..128u64 {
                let mut bytes = vec![42; size];
                bytes[..8].copy_from_slice(&index.to_le_bytes());
                let key = repository
                    .import(casita::import::BlobImport::new(
                        bytes.as_slice(),
                        format!("file-{index}").parse().unwrap(),
                    ))
                    .await
                    .unwrap();
                assert_eq!(
                    key,
                    ObjectKey::blob(casita::BlobId::new(casita::Digest::hash(&bytes)))
                );
                fixtures.push((key, bytes));
            }
            repository.flush().await.unwrap();
            (repository, fixtures)
        });
        group.throughput(Throughput::Elements(fixtures.len() as u64));
        for objects in [false, true] {
            let session = runtime.block_on(async {
                let snapshot = repository.retained_reader().await.unwrap();
                if objects {
                    Session::Objects(snapshot.object_reader().unwrap())
                } else {
                    Session::Snapshot(snapshot)
                }
            });
            let read = || async {
                for (key, bytes) in &fixtures {
                    session.read(key, bytes).await;
                }
            };
            runtime.block_on(read());
            group.bench_function(
                BenchmarkId::new(if objects { "objects" } else { "snapshot" }, size),
                |b| b.to_async(&runtime).iter(read),
            );
            runtime.block_on(async {
                drop(session);
                repository.flush().await.unwrap();
            });
        }
        runtime.block_on(async { drop(repository) });
    }
    group.finish();
}

criterion_group!(benches, object_reads);
criterion_main!(benches);
