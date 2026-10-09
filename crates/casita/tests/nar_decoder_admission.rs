//! NAR decoding leaves blocking capacity available for durable storage.
#![cfg(feature = "native")]

use casita::{NarRequirements, Repository, import::NarImport, scrub_nar};
use sha2::{Digest, Sha256};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

#[path = "../../../benchmarks/fixtures/nar_decoder.rs"]
mod fixture;

struct Reap(Child);
impl Drop for Reap {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn isolated(name: &str, work: impl FnOnce()) {
    if std::env::var("CASITA_NAR_DECODER_CHILD").as_deref() == Ok(name) {
        work();
        return;
    }
    let mut child = Reap(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture", "--test-threads=1"])
            .env("CASITA_NAR_DECODER_CHILD", name)
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success(), "{name}: {status}");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "NAR intake exceeded process watchdog"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn durable_intake_crosses_the_buffer_window_with_one_worker() {
    isolated(
        "durable_intake_crosses_the_buffer_window_with_one_worker",
        || {
            for threads in [1, 2] {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .max_blocking_threads(threads)
                    .enable_all()
                    .build()
                    .unwrap();
                for size in [8 * 1024 * 1024, 32 * 1024 * 1024] {
                    let archive = fixture::archive(size);
                    runtime.block_on(async {
                        let directory = tempfile::tempdir().unwrap();
                        let repo = Repository::local(directory.path()).await.unwrap();
                        let report = repo
                            .import(NarImport::new(archive.as_slice()))
                            .await
                            .unwrap();
                        assert_eq!(report.nar_size(), archive.len() as u64);
                        assert_eq!(report.nar_sha256(), Sha256::digest(&archive).as_slice());
                        assert_eq!(report.stats().hash_payload_bytes, size as u64);
                        let scrub =
                            scrub_nar(report.reader(), report.root(), &NarRequirements::default())
                                .await
                                .unwrap();
                        assert_eq!(scrub.nar_sha256(), report.nar_sha256());
                        drop(scrub);
                        drop(report);
                        repo.flush().await.unwrap();
                    });
                }
            }
        },
    );
}

#[test]
fn concurrent_repositories_import_large_archives_with_one_worker() {
    isolated(
        "concurrent_repositories_import_large_archives_with_one_worker",
        || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .max_blocking_threads(1)
                .enable_all()
                .build()
                .unwrap();
            let archive = fixture::archive(32 * 1024 * 1024);
            runtime.block_on(async {
                let import = || async {
                    let directory = tempfile::tempdir().unwrap();
                    let repo = Repository::local(directory.path()).await.unwrap();
                    let report = repo
                        .import(NarImport::new(archive.as_slice()))
                        .await
                        .unwrap();
                    assert_eq!(report.nar_sha256(), Sha256::digest(&archive).as_slice());
                    let scrub =
                        scrub_nar(report.reader(), report.root(), &NarRequirements::default())
                            .await
                            .unwrap();
                    assert_eq!(scrub.nar_sha256(), report.nar_sha256());
                    drop(scrub);
                    drop(report);
                    repo.flush().await.unwrap();
                };
                futures::join!(import(), import());
            });
        },
    );
}

/// Imports distinct archives of `size` bytes concurrently into one repository.
fn concurrent_imports(blocking_threads: usize, imports: usize, size: usize) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(blocking_threads)
        .enable_all()
        .build()
        .unwrap();
    let archives: Vec<_> = (0..imports)
        .map(|seed| fixture::seeded(size, seed))
        .collect();
    runtime.block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let repo = Repository::local(directory.path()).await.unwrap();
        let reports = futures::future::join_all(
            archives
                .iter()
                .map(|archive| repo.import(NarImport::new(archive.as_slice()))),
        )
        .await;
        for (archive, report) in archives.iter().zip(reports) {
            let report = report.unwrap();
            assert!(!report.stats().association_hit);
            assert_eq!(report.nar_sha256(), Sha256::digest(archive).as_slice());
            let scrub = scrub_nar(report.reader(), report.root(), &NarRequirements::default())
                .await
                .unwrap();
            assert_eq!(scrub.nar_sha256(), report.nar_sha256());
        }
        repo.flush().await.unwrap();
    });
}

// Formerly each decoder held a blocking worker, so as many large imports as
// workers left none for storing their archives. Both sides of that limit:
#[test]
fn fewer_large_imports_than_blocking_workers() {
    isolated("fewer_large_imports_than_blocking_workers", || {
        concurrent_imports(4, 3, 32 * 1024 * 1024);
    });
}

#[test]
fn as_many_large_imports_as_blocking_workers() {
    isolated("as_many_large_imports_as_blocking_workers", || {
        concurrent_imports(4, 4, 32 * 1024 * 1024);
    });
}

#[test]
fn imports_beyond_decoder_capacity_share_one_worker() {
    isolated("imports_beyond_decoder_capacity_share_one_worker", || {
        // One more import than the 16 decoder slots: the last one queues for
        // admission while the others share the single blocking worker.
        concurrent_imports(1, 17, 2 * 1024 * 1024);
    });
}
