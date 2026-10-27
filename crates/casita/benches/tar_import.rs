//! Bounded tar finalization: serial and pipelined imports of identical archives.
//! Run with `cargo bench --features experimental --bench tar_import`.

mod bench_util;

use ::tar::{Builder, Header};
use casita::{
    experimental::{
        BlobId, BlobStore, DEFAULT_AVG_CHUNK_SIZE, Directory, MemoryMetadataStore, MetadataStore,
        Node, ObjectKey, PathComponent, Repository, RootName, TarImportLimits,
    },
    import::{Importer as _, TarImport},
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::io::Cursor;
use std::time::{Duration, Instant};

struct Fixture {
    archive: Vec<u8>,
    files: Vec<Vec<u8>>,
    root: ObjectKey,
}

async fn fixture(sizes: &[usize]) -> Fixture {
    let mut builder = Builder::new(Vec::new());
    let mut directory = Directory::new();
    let mut files = Vec::new();
    for (index, &size) in sizes.iter().enumerate() {
        let bytes = bench_util::random_bytes(97 + index as u64, size);
        let path = format!("file-{index:04}");
        let executable = index % 3 == 0;
        let mut header = Header::new_ustar();
        header.set_size(size as u64);
        header.set_mode(if executable { 0o755 } else { 0o644 });
        builder
            .append_data(&mut header, &path, bytes.as_slice())
            .unwrap();
        directory
            .add(
                PathComponent::try_from(path.as_str()).unwrap(),
                Node::File {
                    digest: BlobId::new(blake3::hash(&bytes).into()),
                    size: size as u64,
                    executable,
                },
            )
            .unwrap();
        files.push(bytes);
    }
    Fixture {
        archive: builder.into_inner().unwrap(),
        files,
        root: ObjectKey::directory(directory.digest()),
    }
}

fn tar_pipeline(c: &mut Criterion) {
    let rt = bench_util::runtime();
    let mut profile = bench_util::perf::PerfControl::open().unwrap();
    if profile.is_some() {
        eprintln!("Scoped tar import profiling enabled");
    }
    let mut group = c.benchmark_group("tar_import_pipeline");
    group.sample_size(10);
    let mut shapes: Vec<_> = [0, 1, 15, 16, 17, 256]
        .into_iter()
        .map(|count| (format!("small-{count}"), vec![1024; count]))
        .collect();
    shapes.push(("large".into(), vec![4 * 1024 * 1024; 4]));
    shapes.push((
        "mixed".into(),
        [vec![1024; 32], vec![4 * 1024 * 1024; 4]].concat(),
    ));
    for (shape, sizes) in shapes {
        let fixture = rt.block_on(fixture(&sizes));
        let total = sizes.iter().map(|&size| size as u64).sum();
        group.throughput(Throughput::Bytes(total));
        let order = if std::env::var_os("CASITA_TAR_REVERSE").is_some() {
            [16, 1]
        } else {
            [1, 16]
        };
        for concurrency in order {
            group.bench_function(BenchmarkId::new(&shape, concurrency), |b| {
                b.iter_custom(|iterations| {
                    rt.block_on(async {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iterations {
                            let (payloads, _) = bench_util::memory_store(DEFAULT_AVG_CHUNK_SIZE);
                            let repository =
                                Repository::new(payloads, MemoryMetadataStore::new().unwrap());
                            let root_name = RootName::try_from("tar/benchmark").unwrap();
                            let request = TarImport::new(
                                Cursor::new(fixture.archive.as_slice()),
                                root_name.clone(),
                            )
                            .with_limits(TarImportLimits {
                                max_in_flight_files: concurrency,
                                ..Default::default()
                            });
                            if let Some(profile) = &mut profile {
                                profile.command("enable").unwrap();
                            }
                            let started = Instant::now();
                            let report = request.import(&repository).await.unwrap();
                            elapsed += started.elapsed();
                            if let Some(profile) = &mut profile {
                                profile.command("disable").unwrap();
                            }
                            // No timed sample is accepted without an independently
                            // constructed canonical root and complete payload reads.
                            assert_eq!(report.root, fixture.root);
                            assert_eq!(report.files, fixture.files.len());
                            assert_eq!(report.entries, fixture.files.len());
                            assert_eq!(report.file_bytes, total);
                            assert_eq!(
                                repository
                                    .metadata()
                                    .snapshot()
                                    .await
                                    .unwrap()
                                    .root(&root_name)
                                    .await
                                    .unwrap(),
                                Some(fixture.root.clone())
                            );
                            for bytes in &fixture.files {
                                let id = BlobId::new(blake3::hash(bytes).into());
                                assert_eq!(
                                    repository
                                        .payloads()
                                        .read_to_vec(&id)
                                        .await
                                        .unwrap()
                                        .unwrap(),
                                    *bytes
                                );
                            }
                        }
                        elapsed
                    })
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, tar_pipeline);
criterion_main!(benches);
