//! Fresh durable raw NAR intake, including pin admission and publication.
use casita::{NarRequirements, Repository, import::NarImport, scrub_nar};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};

#[path = "../../../benchmarks/fixtures/nar_decoder.rs"]
mod decoder_fixture;
#[path = "../../../benchmarks/fixtures/nar_import.rs"]
mod fixture;

fn imports(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("nar_import");
    group
        .sample_size(10)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_secs(1));
    let mut cases: Vec<_> = [
        (1, 1024),
        (15, 1024),
        (16, 1024),
        (17, 1024),
        (64, 1024),
        (256, 1024),
        (1024, 1024),
        (32, 0),
        (32, 16383),
        (32, 16384),
        (32, 16385),
        (127, 16385),
        (128, 16385),
        (129, 16385),
        (629, 16385),
        (630, 16385),
        (631, 16385),
        (15, 16385),
        (16, 16385),
        (17, 16385),
        (32, 65535),
        (32, 65536),
        (512, 65536),
        (2048, 65536),
        (32, 65537),
        (16, 131071),
        (16, 131072),
        (16, 131073),
    ]
    .into_iter()
    .map(|(files, size)| {
        (
            format!("bytes-{size}"),
            files,
            size,
            fixture::archive(files, size),
        )
    })
    .collect();
    for files in [15, 16, 17] {
        cases.push((
            "nested-bytes-1024".to_owned(),
            files * 3,
            1024,
            fixture::nested_archive(files, 1024),
        ));
    }
    for directories in [15, 16, 17, 256] {
        cases.push((
            "directories".to_owned(),
            directories,
            0,
            fixture::directory_archive(directories, 1, 32),
        ));
    }
    for depth in [15, 16, 17] {
        cases.push((
            "nested-directories".to_owned(),
            depth,
            0,
            fixture::nested_directories(depth),
        ));
    }
    // 63 long symlinks encode below the 256 KiB directory budget; 64 and 65
    // exceed it. Oversized directories must still make progress alone.
    for links in [63, 64, 65] {
        cases.push((
            "directory-links-4095".to_owned(),
            links,
            0,
            fixture::directory_archive(3, links, 4095),
        ));
    }
    for (label, files, size, bytes) in cases {
        let hash = Sha256::digest(&bytes);
        group.bench_with_input(BenchmarkId::new(label, files), &bytes, |b, bytes| {
            b.iter_custom(|count| {
                runtime.block_on(async {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..count {
                        let data = tempfile::tempdir().unwrap();
                        let repo = Repository::local(data.path()).await.unwrap();
                        let start = Instant::now();
                        let report = repo.import(NarImport::new(bytes.as_slice())).await.unwrap();
                        elapsed += start.elapsed();
                        assert_eq!(report.nar_sha256(), hash.as_slice());
                        assert_eq!(report.nar_size(), bytes.len() as u64);
                        assert_eq!(report.stats().hash_payload_bytes, (files * size) as u64);
                        assert_eq!(report.stats().encoding_passes, 0);
                        let scrub =
                            scrub_nar(report.reader(), report.root(), &NarRequirements::default())
                                .await
                                .unwrap();
                        assert_eq!(scrub.nar_sha256(), hash.as_slice());
                    }
                    elapsed
                })
            });
        });
    }
    group.finish();
}

fn sequences(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("nar_import_sequence");
    group
        .sample_size(10)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_secs(1));
    for imports in [15, 16, 17, 18, 19, 32] {
        let archives = fixture::sequence(imports);
        group.bench_with_input(
            BenchmarkId::from_parameter(imports),
            &archives,
            |b, archives| {
                b.iter_custom(|count| {
                    runtime.block_on(async {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..count {
                            let data = tempfile::tempdir().unwrap();
                            let repo = Repository::local(data.path()).await.unwrap();
                            let mut held = Vec::new();
                            for bytes in archives {
                                let start = Instant::now();
                                let report =
                                    repo.import(NarImport::new(bytes.as_slice())).await.unwrap();
                                elapsed += start.elapsed();
                                assert!(!report.stats().association_hit);
                                assert_eq!(report.nar_size(), bytes.len() as u64);
                                assert_eq!(report.nar_sha256(), Sha256::digest(bytes).as_slice());
                                let scrub = scrub_nar(
                                    report.reader(),
                                    report.root(),
                                    &NarRequirements::default(),
                                )
                                .await
                                .unwrap();
                                assert_eq!(scrub.nar_sha256(), report.nar_sha256());
                                held.push(report);
                            }
                            drop(held);
                            repo.flush().await.unwrap();
                            repo.collect().await.unwrap();
                        }
                        elapsed
                    })
                });
            },
        );
    }
    group.finish();
}
fn decoder_pools(c: &mut Criterion) {
    let mut group = c.benchmark_group("nar_decoder_pool");
    group
        .sample_size(10)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_secs(1));
    for threads in [1, 2] {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(threads)
            .enable_all()
            .build()
            .unwrap();
        for size in [8 * 1024 * 1024, 32 * 1024 * 1024] {
            let archive = decoder_fixture::archive(size);
            let hash = Sha256::digest(&archive);
            group.bench_with_input(
                BenchmarkId::new(format!("workers-{threads}"), size),
                &archive,
                |b, bytes| {
                    b.iter_custom(|count| {
                        runtime.block_on(async {
                            let mut elapsed = Duration::ZERO;
                            for _ in 0..count {
                                let directory = tempfile::tempdir().unwrap();
                                let repo = Repository::local(directory.path()).await.unwrap();
                                let start = Instant::now();
                                let report =
                                    repo.import(NarImport::new(bytes.as_slice())).await.unwrap();
                                elapsed += start.elapsed();
                                assert_eq!(report.nar_size(), bytes.len() as u64);
                                assert_eq!(report.nar_sha256(), hash.as_slice());
                                assert_eq!(report.stats().hash_payload_bytes, size as u64);
                                let scrub = scrub_nar(
                                    report.reader(),
                                    report.root(),
                                    &NarRequirements::default(),
                                )
                                .await
                                .unwrap();
                                assert_eq!(scrub.nar_sha256(), hash.as_slice());
                                drop(scrub);
                                drop(report);
                                repo.flush().await.unwrap();
                            }
                            elapsed
                        })
                    });
                },
            );
        }
    }
    group.finish();
}

// 16 and 17 concurrent imports straddle the decoder admission limit; 32 queues
// half of them. Each import is its own task on a multi-threaded runtime.
fn decoder_admission(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("nar_decoder_admission");
    group
        .sample_size(10)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_secs(2));
    for imports in [16, 17, 32] {
        let archives: Vec<std::sync::Arc<[u8]>> = (0..imports)
            .map(|seed| decoder_fixture::seeded(2 * 1024 * 1024, seed).into())
            .collect();
        group.bench_with_input(
            BenchmarkId::from_parameter(imports),
            &archives,
            |b, archives| {
                b.iter_custom(|count| {
                    runtime.block_on(async {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..count {
                            let directory = tempfile::tempdir().unwrap();
                            let repo = Repository::local(directory.path()).await.unwrap();
                            let start = Instant::now();
                            let tasks: Vec<_> = archives
                                .iter()
                                .map(|archive| {
                                    let repo = repo.clone();
                                    let input = std::io::Cursor::new(archive.clone());
                                    tokio::spawn(
                                        async move { repo.import(NarImport::new(input)).await },
                                    )
                                })
                                .collect();
                            let reports = futures::future::try_join_all(tasks).await.unwrap();
                            elapsed += start.elapsed();
                            for (archive, report) in archives.iter().zip(reports) {
                                let report = report.unwrap();
                                assert!(!report.stats().association_hit);
                                assert_eq!(report.nar_size(), archive.len() as u64);
                                assert_eq!(report.nar_sha256(), Sha256::digest(archive).as_slice());
                                let scrub = scrub_nar(
                                    report.reader(),
                                    report.root(),
                                    &NarRequirements::default(),
                                )
                                .await
                                .unwrap();
                                assert_eq!(scrub.nar_sha256(), report.nar_sha256());
                            }
                            repo.flush().await.unwrap();
                        }
                        elapsed
                    })
                });
            },
        );
    }
    group.finish();
}
criterion_group!(
    benches,
    imports,
    sequences,
    decoder_pools,
    decoder_admission
);
criterion_main!(benches);
