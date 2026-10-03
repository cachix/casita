#![cfg(all(feature = "native", feature = "experimental"))]

use casita::experimental::{BlobStore, ChunkedBlobStore};
use casita::{BlobId, Digest};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// A periodic source keeps fixture memory fixed and the memory backend's unique
// payload set small. The manifest still contains one entry per source chunk.
fn pattern() -> Vec<u8> {
    let mut state = 0x239be41836a77195u64;
    (0..65536)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn peak_rss_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()
        .map(|kib| kib * 1024)
}

async fn exercise(size: usize, backend: &str) {
    let directory = tempfile::tempdir().unwrap();
    let objects: Arc<dyn ObjectStore> = match backend {
        "memory" => Arc::new(object_store::memory::InMemory::new()),
        "local" => Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap(),
        ),
        _ => panic!("unknown backend"),
    };
    let store = ChunkedBlobStore::new(objects.clone(), Path::default(), 1024)
        .with_chunk_upload_concurrency(4.try_into().unwrap())
        .with_chunk_memory_budget_bytes(1024 * 1024);
    let pattern = pattern();
    let mut expected = blake3::Hasher::new();
    for offset in (0..size).step_by(pattern.len()) {
        expected.update(&pattern[..pattern.len().min(size - offset)]);
    }
    let expected = BlobId::new(Digest::from(*expected.finalize().as_bytes()));
    let before_rss = peak_rss_bytes();
    let begin = Instant::now();
    let mut writer = store.open_write().await;
    for offset in (0..size).step_by(pattern.len()) {
        writer
            .write_all(&pattern[..pattern.len().min(size - offset)])
            .await
            .unwrap();
    }
    let (digest, actual_size) = writer.close().await.unwrap();
    let elapsed = begin.elapsed();
    let write_peak_rss = peak_rss_bytes();
    assert_eq!(digest, expected);
    assert_eq!(actual_size, size as u64);
    drop(writer);

    // Full verified streaming readback, without enumerating or materializing
    // all ChunkMeta records (which would obscure the writer's memory usage).
    let mut reader = store
        .open_verified(&digest, size as u64)
        .await
        .unwrap()
        .unwrap();
    let mut buffer = vec![0; 65536];
    let mut offset = 0;
    loop {
        let read = reader.read(&mut buffer).await.unwrap();
        if read == 0 {
            break;
        }
        assert!(offset + read <= size);
        for (index, byte) in buffer[..read].iter().enumerate() {
            assert_eq!(*byte, pattern[(offset + index) % pattern.len()]);
        }
        offset += read;
    }
    assert_eq!(offset, size);
    let hex = digest.digest().to_hex();
    let manifest = objects
        .get(&Path::from(format!("blobs/b3/{}/{hex}", &hex[..2])))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    println!(
        "chunk_manifest_sample {{\"file_bytes\":{size},\"backend\":\"{backend}\",\"wall_nanos\":{},\"root\":\"{digest}\",\"manifest_hash\":\"{}\",\"before_rss_bytes\":{},\"write_peak_rss_bytes\":{},\"correctness\":\"independent streaming BLAKE3 and exhaustive verified readback\"}}",
        elapsed.as_nanos(),
        Digest::hash(&manifest),
        before_rss.map_or("null".into(), |n| n.to_string()),
        write_peak_rss.map_or("null".into(), |n| n.to_string()),
    );
}

#[tokio::test]
async fn streaming_manifest_fixture_checks_both_backends_without_whole_file_buffers() {
    for backend in ["memory", "local"] {
        for size in [65536, 131072, 1048576] {
            exercise(size, backend).await;
        }
    }
}

#[tokio::test]
#[ignore = "run through benchmark run chunk-manifest-stream"]
async fn benchmark_chunk_manifest_stream() {
    let size = std::env::var("CASITA_MANIFEST_BYTES")
        .map(|s| s.parse().unwrap())
        .unwrap_or(64 * 1024 * 1024);
    let backend = std::env::var("CASITA_MANIFEST_BACKEND").unwrap_or("memory".into());
    exercise(size, &backend).await;
}
