//! Compare retained snapshots and detached object guards during ongoing writes.
//! Every case verifies the retained payload, GC safety and checkpoint outcome.
use casita::{ErrorKind, Repository};
use std::time::Instant;
use tokio::io::AsyncReadExt;

async fn scenario(detached: bool, writes: usize) {
    let directory = tempfile::tempdir().unwrap();
    let repository = Repository::local(directory.path()).await.unwrap();
    let name = "sentinel".parse().unwrap();
    let sentinel = b"retain this payload across writes and collection";
    let key = repository
        .import(casita::import::BlobImport::new(&sentinel[..], name))
        .await
        .unwrap();
    repository.flush().await.unwrap();
    let reader = repository.retained_reader().await.unwrap();
    let (snapshot, guard) = if detached {
        let guard = reader.retain_objects();
        drop(reader);
        (None, Some(guard))
    } else {
        (Some(reader), None)
    };
    repository
        .remove_root(&"sentinel".parse().unwrap(), &key)
        .await
        .unwrap();
    let wal = directory.path().join("casita.sqlite-wal");
    let wal_size = || std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
    let mut peak_wal = wal_size();
    let start = Instant::now();
    for index in 0..writes {
        let mut bytes = [42; 512];
        bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
        let imported = repository
            .import(casita::import::BlobImport::new(
                &bytes[..],
                "churn".parse().unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(
            imported,
            casita::ObjectKey::blob(casita::BlobId::new(casita::Digest::hash(&bytes)))
        );
        peak_wal = peak_wal.max(wal_size());
    }
    let seconds = start.elapsed().as_secs_f64();
    let after_writes = wal_size();
    let busy = match repository.flush().await {
        Ok(()) => false,
        Err(error) => {
            assert_eq!(error.kind(), ErrorKind::Busy);
            true
        }
    };
    assert_eq!(busy, !detached);
    let after_checkpoint = wal_size();
    if detached {
        assert_eq!(after_checkpoint, 0);
    }
    repository.collect().await.unwrap();
    {
        let fresh = repository.retained_reader().await.unwrap();
        let mut opened = fresh.open(&key).await.unwrap().unwrap();
        let mut bytes = Vec::new();
        opened.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, sentinel);
    }
    drop(snapshot);
    drop(guard);
    repository.collect().await.unwrap();
    assert!(repository.object(&key).await.unwrap().is_none());
    assert!(repository.fsck().await.unwrap().is_clean());
    repository.flush().await.unwrap();
    println!(
        "{}",
        serde_json::json!({
            "retention": if detached { "objects" } else { "snapshot" },
            "writes": writes, "payload_bytes": 512, "write_seconds": seconds,
            "peak_wal_bytes": peak_wal, "wal_after_writes_bytes": after_writes,
            "checkpoint_busy": busy, "wal_after_checkpoint_bytes": after_checkpoint,
            "correctness": "passed"
        })
    );
}

fn main() {
    let writes: Vec<usize> = std::env::var("CASITA_BENCH_RETAINED_WRITES")
        .unwrap_or_else(|_| "32,256,1024".into())
        .split(',')
        .map(|count| count.parse().unwrap())
        .collect();
    assert!(!writes.is_empty() && writes.iter().all(|count| *count > 0));
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            for count in writes {
                for detached in [false, true] {
                    scenario(detached, count).await;
                }
            }
        });
}
