//! Shared-session publication and shared-reader checkout versus individual calls.
//! Every sample checks commit counts, roots, and restored bytes outside timing.
mod bench_util;
use casita::{
    RootName,
    experimental::{MetadataStore, Repository},
    import::{BlobImport, FilesystemImport, TarImport},
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::time::{Duration, Instant};

fn artifact_batches(c: &mut Criterion) {
    let rt = bench_util::runtime();
    let source = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("file"), b"contents").unwrap();
    let tar = rt.block_on(async {
        let mut builder = tokio_tar::Builder::new(Vec::new());
        let mut header = tokio_tar::Header::new_ustar();
        header.set_size(8);
        header.set_mode(0o644);
        builder
            .append_data(&mut header, "file", &b"contents"[..])
            .await
            .unwrap();
        builder.into_inner().await.unwrap()
    });
    let mut group = c.benchmark_group("artifact_batches");
    group.sample_size(10);
    for count in [1, 8, 32] {
        for local in [false, true] {
            for batch in [false, true] {
                let case = format!(
                    "{}-{}",
                    if local { "local" } else { "memory" },
                    if batch { "batch" } else { "individual" }
                );
                group.bench_with_input(
                    BenchmarkId::new(format!("mixed-import-{case}"), count),
                    &count,
                    |b, &count| {
                        b.iter_custom(|iterations| {
                            rt.block_on(async {
                                let mut elapsed = Duration::ZERO;
                                for _ in 0..iterations {
                                    let store = tempfile::tempdir().unwrap();
                                    let repository = if local {
                                        Repository::local(store.path()).await.unwrap().into_erased()
                                    } else {
                                        Repository::memory().unwrap().into_erased()
                                    };
                                    let before = repository
                                        .metadata()
                                        .snapshot()
                                        .await
                                        .unwrap()
                                        .generation()
                                        .unwrap();
                                    let names: Vec<RootName> = (0..count)
                                        .map(|index| format!("bench/{index}").parse().unwrap())
                                        .collect();
                                    let start = Instant::now();
                                    let mut keys = Vec::new();
                                    if batch {
                                        let session = repository.mutation_session().await.unwrap();
                                        let mut objects = Vec::new();
                                        let mut changes = Vec::new();
                                        for (index, name) in names.iter().enumerate() {
                                            let staged = match index % 3 {
                                                0 => {
                                                    BlobImport::new(&b"contents"[..], name.clone())
                                                        .stage(&session)
                                                        .await
                                                        .unwrap()
                                                }
                                                1 => FilesystemImport::new(
                                                    source.path(),
                                                    name.clone(),
                                                )
                                                .reread(true)
                                                .stage(&session)
                                                .await
                                                .unwrap(),
                                                _ => {
                                                    let staged = TarImport::new(
                                                        tar.as_slice(),
                                                        name.clone(),
                                                    )
                                                    .stage(&session)
                                                    .await
                                                    .unwrap();
                                                    casita::import::StagedImport {
                                                        report: staged.report.root,
                                                        objects: staged.objects,
                                                        root_change: staged.root_change,
                                                        metadata_changes: staged.metadata_changes,
                                                    }
                                                }
                                            };
                                            keys.push(staged.report);
                                            objects.extend(staged.objects);
                                            changes.push(staged.root_change);
                                        }
                                        session.publish(objects, changes).await.unwrap();
                                    } else {
                                        for (index, name) in names.iter().enumerate() {
                                            keys.push(match index % 3 {
                                                0 => repository
                                                    .import(BlobImport::new(
                                                        &b"contents"[..],
                                                        name.clone(),
                                                    ))
                                                    .await
                                                    .unwrap(),
                                                1 => repository
                                                    .import(
                                                        FilesystemImport::new(
                                                            source.path(),
                                                            name.clone(),
                                                        )
                                                        .reread(true),
                                                    )
                                                    .await
                                                    .unwrap(),
                                                _ => {
                                                    repository
                                                        .import(TarImport::new(
                                                            tar.as_slice(),
                                                            name.clone(),
                                                        ))
                                                        .await
                                                        .unwrap()
                                                        .root
                                                }
                                            });
                                        }
                                    }
                                    elapsed += start.elapsed();
                                    let reader = repository.retained_reader().await.unwrap();
                                    assert_eq!(
                                        reader.generation().unwrap().get(),
                                        before + if batch { 1 } else { count as u64 }
                                    );
                                    let output = tempfile::tempdir().unwrap();
                                    for (index, (name, key)) in names.iter().zip(keys).enumerate() {
                                        assert_eq!(
                                            reader.root(name).await.unwrap(),
                                            Some(key.clone())
                                        );
                                        if index % 3 == 0 {
                                            use tokio::io::AsyncReadExt;
                                            let mut bytes = Vec::new();
                                            reader
                                                .open_verified(&key)
                                                .await
                                                .unwrap()
                                                .unwrap()
                                                .read_to_end(&mut bytes)
                                                .await
                                                .unwrap();
                                            assert_eq!(bytes, b"contents");
                                        } else {
                                            let path = output.path().join(index.to_string());
                                            reader.checkout(&key, &path).await.unwrap();
                                            assert_eq!(
                                                std::fs::read(path.join("file")).unwrap(),
                                                b"contents"
                                            );
                                        }
                                    }
                                }
                                elapsed
                            })
                        });
                    },
                );
                group.bench_with_input(
                    BenchmarkId::new(format!("checkout-{case}"), count),
                    &count,
                    |b, &count| {
                        b.iter_custom(|iterations| {
                            rt.block_on(async {
                                let mut elapsed = Duration::ZERO;
                                for _ in 0..iterations {
                                    let store = tempfile::tempdir().unwrap();
                                    let repository = if local {
                                        Repository::local(store.path()).await.unwrap().into_erased()
                                    } else {
                                        Repository::memory().unwrap().into_erased()
                                    };
                                    let key = repository
                                        .import(FilesystemImport::new(
                                            source.path(),
                                            "tree".parse().unwrap(),
                                        ))
                                        .await
                                        .unwrap();
                                    let output = tempfile::tempdir().unwrap();
                                    let start = Instant::now();
                                    if batch {
                                        let reader = repository.retained_reader().await.unwrap();
                                        for index in 0..count {
                                            reader
                                                .checkout(
                                                    &key,
                                                    output.path().join(index.to_string()),
                                                )
                                                .await
                                                .unwrap();
                                        }
                                    } else {
                                        for index in 0..count {
                                            repository
                                                .checkout(
                                                    &key,
                                                    output.path().join(index.to_string()),
                                                )
                                                .await
                                                .unwrap();
                                        }
                                    }
                                    elapsed += start.elapsed();
                                    for index in 0..count {
                                        assert_eq!(
                                            std::fs::read(
                                                output.path().join(index.to_string()).join("file")
                                            )
                                            .unwrap(),
                                            b"contents"
                                        );
                                    }
                                }
                                elapsed
                            })
                        });
                    },
                );
            }
        }
    }
    group.finish();
}
criterion_group!(benches, artifact_batches);
criterion_main!(benches);
