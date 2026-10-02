//! Deterministic in-memory GET delay, exercising the production packed reader.
//! This models per-request latency, not TCP, shared bandwidth, disk, or FUSE.
use casita::experimental::object_store::{
    ObjectStoreExt,
    memory::InMemory,
    path::Path,
    throttle::{ThrottleConfig, ThrottledStore},
};
use casita::experimental::{BlobStore, ChunkedBlobStore, PackOptions, blake3};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::io::AsyncReadExt;

type Error = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        return Err(
            "usage: packed_read_latency FILE_BYTES GET_DELAY_MS CACHE_BYTES PACK_BYTES".into(),
        );
    }
    let file_bytes: usize = args[1].parse()?;
    let delay_ms: u64 = args[2].parse()?;
    let cache_bytes: u64 = args[3].parse()?;
    let pack_bytes: u64 = args[4].parse()?;
    if file_bytes == 0 || pack_bytes == 0 {
        return Err("positive file and pack sizes required".into());
    }
    let objects = Arc::new(ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig::default(),
    ));
    let options = PackOptions {
        target_size: pack_bytes,
        cache_capacity: cache_bytes,
    };
    let store = ChunkedBlobStore::packed_with_options(
        objects.clone(),
        Path::from("fixture"),
        256 * 1024,
        options,
    )
    .await?;
    let mut expected = vec![0; file_bytes];
    blake3::Hasher::new()
        .update(b"packed-demand-latency-v1")
        .finalize_xof()
        .fill(&mut expected);
    let expected_digest = blake3::hash(&expected);
    let id = store.put_slice(&expected).await?;
    store.flush().await?;
    drop(store);
    let store = ChunkedBlobStore::packed_with_options(
        objects.clone(),
        Path::from("fixture"),
        256 * 1024,
        options,
    )
    .await?;
    let calibration = Path::from("calibration");
    objects.put(&calibration, vec![1_u8].into()).await?;
    objects.config_mut(|c| c.wait_get_per_call = Duration::from_millis(delay_ms));
    let started = Instant::now();
    let bytes = objects.get(&calibration).await?.bytes().await?;
    let calibration_nanos = started.elapsed().as_nanos();
    if bytes.as_ref() != [1] || calibration_nanos < u128::from(delay_ms) * 900_000 {
        return Err("GET latency calibration failed".into());
    }
    for phase in ["cold", "warm"] {
        store.reset_pack_read_stats();
        let started = Instant::now();
        let mut actual = Vec::with_capacity(file_bytes);
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut reader = store.open_read(&id).await?.ok_or("blob missing")?;
            reader.read_to_end(&mut actual).await?;
            Ok::<(), Error>(())
        })
        .await??;
        let nanos = started.elapsed().as_nanos();
        if actual != expected || blake3::hash(&actual) != expected_digest {
            return Err("reconstruction verification failed".into());
        }
        let stats = store.pack_read_stats().ok_or("not packed")?;
        let requests = stats.chunk_range_requests + stats.whole_pack_requests;
        if phase == "cold" && requests == 0 {
            return Err("cold read performed no pack I/O".into());
        }
        if phase == "warm" && cache_bytes >= file_bytes as u64 * 2 && requests != 0 {
            return Err("fitting warm cache performed pack I/O".into());
        }
        println!(
            "latency_sample {{\"phase\":\"{phase}\",\"file_bytes\":{file_bytes},\"get_delay_ms\":{delay_ms},\"cache_bytes\":{cache_bytes},\"pack_bytes\":{pack_bytes},\"nanos\":{nanos},\"calibration_nanos\":{calibration_nanos},\"digest\":\"{expected_digest}\",\"pack_requests\":{requests},\"pack_read_bytes\":{},\"cache_hits\":{},\"readahead_deferrals\":{},\"buffer_bypasses\":{},\"correctness\":\"exact bytes and independent BLAKE3\"}}",
            stats.chunk_range_bytes + stats.whole_pack_bytes,
            stats.cache_hits,
            stats.readahead_deferrals,
            stats.buffer_bypasses
        );
    }
    Ok(())
}
