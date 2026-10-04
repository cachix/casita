//! Process ownership through the unchanged application read-session API.
#![cfg(all(feature = "native", feature = "experimental"))]

use casita::experimental::{FilePinStore, PinScope, PinStore};
use casita::{Repository, RootName};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

fn root() -> RootName {
    "current".parse().unwrap()
}
fn ledger(path: &std::path::Path) -> FilePinStore {
    FilePinStore::new(path.join("casita.sqlite.online-pins"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_readers_share_one_process_pin_through_concurrent_gc() {
    for core_api in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let repo = Repository::local(directory.path()).await.unwrap();
        let expected = vec![42; 2 * 1024 * 1024];
        let key = repo
            .import(casita::import::BlobImport::new(&expected[..], root()))
            .await
            .unwrap();
        repo.flush().await.unwrap();
        let session = if core_api {
            let core = casita::experimental::Repository::local(directory.path())
                .await
                .unwrap();
            core.retained_reader().await.unwrap()
        } else {
            repo.retained_reader().await.unwrap()
        };
        let clone = session.clone();
        let mut first = session.open(&key).await.unwrap().unwrap();
        let mut last = clone.open(&key).await.unwrap().unwrap();
        let pins = ledger(directory.path());
        let inventory = pins.inventory().await.unwrap();
        assert_eq!(inventory.pins.len(), 1);
        let token = inventory.pins.keys().next().unwrap().clone();
        assert!(matches!(
            inventory.pins[&token].scope,
            PinScope::Snapshot { .. }
        ));
        assert_eq!(inventory.reader_owners.len(), 1);
        let writer = Repository::local(directory.path()).await.unwrap();
        writer
            .import(casita::import::BlobImport::new(&b"replacement"[..], root()))
            .await
            .unwrap();
        assert_eq!(
            writer.flush().await.unwrap_err().kind(),
            casita::ErrorKind::Busy
        );
        drop(repo);
        let check = async {
            for _ in 0..8 {
                assert_eq!(session.root(&root()).await.unwrap(), Some(key.clone()));
                let mut reader = session.open(&key).await.unwrap().unwrap();
                let mut bytes = Vec::new();
                reader.read_to_end(&mut bytes).await.unwrap();
                assert_eq!(bytes, expected);
                tokio::task::yield_now().await;
            }
        };
        let gc = async {
            for _ in 0..8 {
                assert_eq!(writer.collect().await.unwrap().logical_objects, 0);
                tokio::task::yield_now().await;
            }
        };
        tokio::join!(check, gc);
        drop(session);
        drop(clone);
        assert_eq!(pins.inventory().await.unwrap().pins.len(), 1);
        let mut bytes = Vec::new();
        first.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, expected);
        drop(first);
        assert_eq!(writer.collect().await.unwrap().logical_objects, 0);
        assert!(pins.inventory().await.unwrap().pins.contains_key(&token));
        last.seek(std::io::SeekFrom::Start(1024 * 1024))
            .await
            .unwrap();
        bytes.clear();
        last.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, expected[1024 * 1024..]);
        drop(last);
        writer.flush().await.unwrap();
        assert!(pins.inventory().await.unwrap().pins.is_empty());
        assert_eq!(writer.collect().await.unwrap().logical_objects, 1);
        assert!(writer.open(&key).await.unwrap().is_none());
        // Dropping the writer releases its hold in a background task. Finish
        // that release before the temporary directory is removed, or it fails
        // on a missing pin ledger and the next pass's flush reports the error.
        drop(writer);
        casita::experimental::flush_repository_leases()
            .await
            .unwrap();
    }
}

#[cfg(unix)]
#[test]
fn retained_process_crash_child() {
    let Some(path) = std::env::var_os("CASITA_RETAINED_CRASH_REPO") else {
        return;
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let repo = Repository::local(&path).await.unwrap();
        let session = repo.retained_reader().await.unwrap();
        let key = session.root(&root()).await.unwrap().unwrap();
        let mut reader = session.open(&key).await.unwrap().unwrap();
        let session = if std::env::var_os("CASITA_RETAINED_CRASH_READERS_ONLY").is_some() {
            drop(session);
            None
        } else {
            // Keep both forms alive until the parent kills this process.
            Some(session)
        };
        drop(repo);
        // A write in the same process may have unsettled remote I/O even
        // after it dies, so its registration must remain durable.
        let durable = pins_durable_write(&ledger(std::path::Path::new(&path))).await;
        let ready = std::path::Path::new(&path).join("ready");
        let temporary = ready.with_extension("tmp");
        std::fs::write(&temporary, durable.to_string()).unwrap();
        std::fs::rename(temporary, ready).unwrap();
        let check = std::path::Path::new(&path).join("check-reader");
        while !check.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        if let Some(session) = &session {
            assert_eq!(session.root(&root()).await.unwrap(), Some(key));
        }
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"old snapshot");
        std::fs::write(check.with_extension("done"), []).unwrap();
        std::future::pending::<()>().await;
        drop(reader);
        drop(session);
    });
}

#[cfg(unix)]
#[test]
fn process_crashes_release_retained_protection_but_preserve_durable_writes() {
    use std::process::{Command, Stdio};
    struct KillOnDrop(std::process::Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    for readers_only in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let (repo, key) = runtime.block_on(async {
            let repo = Repository::local(directory.path()).await.unwrap();
            let key = repo
                .import(casita::import::BlobImport::new(
                    &b"old snapshot"[..],
                    root(),
                ))
                .await
                .unwrap();
            repo.flush().await.unwrap();
            (repo, key)
        });
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "retained_process_crash_child", "--nocapture"])
            .env("CASITA_RETAINED_CRASH_REPO", directory.path())
            .stdout(Stdio::null());
        if readers_only {
            command.env("CASITA_RETAINED_CRASH_READERS_ONLY", "1");
        }
        let mut child = KillOnDrop(command.spawn().unwrap());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !directory.path().join("ready").exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "reader child exited early"
            );
            assert!(
                std::time::Instant::now() < deadline,
                "reader child never became ready"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        runtime.block_on(async {
            repo.import(casita::import::BlobImport::new(
                &b"new snapshot"[..],
                root(),
            ))
            .await
            .unwrap();
            if let Err(error) = repo.flush().await {
                assert_eq!(error.kind(), casita::ErrorKind::Busy);
            }
            assert_eq!(repo.collect().await.unwrap().logical_objects, 0);
            assert_eq!(
                ledger(directory.path())
                    .inventory()
                    .await
                    .unwrap()
                    .pins
                    .len(),
                2
            );
        });
        std::fs::write(directory.path().join("check-reader"), []).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !directory.path().join("check-reader.done").exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "reader child failed after checkpoint"
            );
            assert!(
                std::time::Instant::now() < deadline,
                "reader child did not validate its snapshot"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        child.0.kill().unwrap();
        assert!(!child.0.wait().unwrap().success());
        let durable = std::fs::read_to_string(directory.path().join("ready"))
            .unwrap()
            .parse()
            .unwrap();
        runtime.block_on(async {
            let pins = ledger(directory.path());
            let inventory = pins.inventory().await.unwrap();
            assert_eq!(inventory.pins.len(), 1);
            assert!(inventory.pins.contains_key(&durable));
            assert_eq!(repo.collect().await.unwrap().logical_objects, 1);
            assert!(repo.open(&key).await.unwrap().is_none());
            pins.release(&durable).await.unwrap();
            repo.flush().await.unwrap();
            assert!(
                std::fs::metadata(directory.path().join("casita.sqlite-wal"))
                    .unwrap()
                    .len()
                    <= 4096
            );
        });
    }
}

#[cfg(unix)]
async fn pins_durable_write(pins: &FilePinStore) -> casita::experimental::PinToken {
    pins.register(casita::experimental::DataPin {
        scope: PinScope::Staging,
        catalog: None,
        resources: Default::default(),
    })
    .await
    .unwrap()
    .unwrap()
}

#[test]
fn retained_pin_releases_after_its_runtime_shuts_down() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let reader = runtime.block_on(async {
        let repo = Repository::local(directory.path()).await.unwrap();
        let key = repo
            .import(casita::import::BlobImport::new(
                &b"runtime independent ownership"[..],
                root(),
            ))
            .await
            .unwrap();
        repo.flush().await.unwrap();
        let session = repo.retained_reader().await.unwrap();
        session.open(&key).await.unwrap().unwrap()
    });
    drop(runtime);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        assert_eq!(
            ledger(directory.path())
                .inventory()
                .await
                .unwrap()
                .pins
                .len(),
            1
        );
    });
    drop(runtime);
    drop(reader);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        assert!(
            ledger(directory.path())
                .inventory()
                .await
                .unwrap()
                .pins
                .is_empty()
        );
    });
}
