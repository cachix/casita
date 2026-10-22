//! Opt-in scale probes. Preparation and integrity audits are outside timings.
use std::path::Path;
use std::time::Instant;

use serde_json::{Value, json};
use tokio::io::AsyncReadExt;

use crate::{
    BlobId, BlobStore, ChunkedBlobStore, Directory, MetadataStore, Node, ObjectKey, RootChange,
    RootName, repository::Repository,
};

type Local = Repository<ChunkedBlobStore, crate::TursoMetadataStore>;

use crate::benchmark_timing as import_pin_timing;

fn setting(name: &str, default: usize) -> usize {
    std::env::var(name)
        .map(|value| value.parse().unwrap())
        .unwrap_or(default)
}

fn bytes(label: usize, size: usize) -> Vec<u8> {
    let mut bytes = vec![0; size];
    blake3::Hasher::new()
        .update(&(label as u64).to_le_bytes())
        .finalize_xof()
        .fill(&mut bytes);
    bytes
}

fn blob(bytes: &[u8]) -> BlobId {
    BlobId::new(blake3::hash(bytes).into())
}

fn output_root(index: usize) -> RootName {
    RootName::try_from(format!("outputs/{index:08}").as_str()).unwrap()
}
fn name(index: usize) -> RootName {
    RootName::try_from(format!("history/{index:08}").as_str()).unwrap()
}

fn tree(generation: usize) -> Directory {
    Directory::try_from_iter([
        (
            crate::PathComponent::try_from("shared").unwrap(),
            Node::File {
                digest: blob(&bytes(0, 65536)),
                size: 65536,
                executable: false,
            },
        ),
        (
            crate::PathComponent::try_from("delta").unwrap(),
            Node::File {
                digest: blob(&bytes(generation, 256)),
                size: 256,
                executable: false,
            },
        ),
    ])
    .unwrap()
}

fn disk_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let meta = entry.metadata().unwrap();
            if meta.is_dir() {
                disk_bytes(&entry.path())
            } else {
                meta.len()
            }
        })
        .sum()
}

fn emit(mut sample: Value) {
    sample["status"] = json!("ok");
    sample["implementation"] = json!("casita");
    println!("scale_sample {sample}");
}

struct LazyOutputFile {
    path: std::path::PathBuf,
    inner: Option<tokio::fs::File>,
}
impl LazyOutputFile {
    fn new(path: std::path::PathBuf) -> Self {
        Self { path, inner: None }
    }
}
impl tokio::io::AsyncRead for LazyOutputFile {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.inner.is_none() {
            match std::fs::File::open(&self.path) {
                Ok(file) => self.inner = Some(tokio::fs::File::from_std(file)),
                Err(error) => return std::task::Poll::Ready(Err(error)),
            }
        }
        tokio::io::AsyncRead::poll_read(
            std::pin::Pin::new(self.inner.as_mut().unwrap()),
            cx,
            buffer,
        )
    }
}

fn emit_output_import(mut sample: Value) {
    sample["status"] = json!("ok");
    sample["implementation"] = json!("casita");
    println!("output_import_sample {sample}");
}

fn latency(nanos: &[u64]) -> Value {
    let mut sorted = nanos.to_vec();
    sorted.sort_unstable();
    let percentile = |p: usize| sorted[(sorted.len() * p).div_ceil(100).saturating_sub(1)];
    json!({"operations": sorted.len(), "wall_seconds": nanos.iter().sum::<u64>() as f64 / 1e9,
        "p50_nanos": percentile(50), "p95_nanos": percentile(95), "p99_nanos": percentile(99), "max_nanos": sorted.last().unwrap()})
}

async fn read(store: &ChunkedBlobStore, id: &BlobId) -> Vec<u8> {
    let mut reader = store.open_read(id).await.unwrap().unwrap();
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    bytes
}

async fn audit_history(repository: &Local, count: usize) {
    let snapshot = repository.metadata().snapshot().await.unwrap();
    for generation in 1..=count {
        let directory = tree(generation);
        let key = ObjectKey::directory(directory.digest());
        assert_eq!(snapshot.root(&name(generation)).await.unwrap(), Some(key));
        assert_eq!(
            read(repository.payloads(), &blob(&bytes(generation, 256))).await,
            bytes(generation, 256)
        );
        assert_eq!(
            read(
                repository.payloads(),
                &BlobId::new(directory.digest().digest())
            )
            .await,
            directory.encode()
        );
    }
    drop(snapshot);
    assert_eq!(
        read(repository.payloads(), &blob(&bytes(0, 65536))).await,
        bytes(0, 65536)
    );
    let report = repository.fsck().await.unwrap();
    assert!(report.is_clean(), "history audit: {report:?}");
}

#[tokio::test]
#[ignore = "scale probe; benchmark run history-scale"]
async fn benchmark_retained_history() {
    let points: Vec<usize> = std::env::var("CASITA_SCALE_GENERATIONS")
        .unwrap_or_else(|_| "100,1000,10000".into())
        .split(',')
        .map(|v| v.parse().unwrap())
        .collect();
    assert!(!points.is_empty() && points[0] > 0 && points.windows(2).all(|w| w[0] < w[1]));
    let window = setting("CASITA_SCALE_WINDOW", 100);
    let seed_batch_size = setting("CASITA_SCALE_SEED_BATCH", 64);
    assert!(window > 0 && (1..=128).contains(&seed_batch_size));
    let temporary = tempfile::tempdir().unwrap();
    let mut repository = Repository::local(temporary.path()).await.unwrap();
    {
        let mutation = repository.mutation_session().await.unwrap();
        let shared = mutation.stage_blob(&bytes(0, 65536)).await.unwrap();
        mutation.publish_unrooted(vec![shared]).await.unwrap();
    }
    let mut start = 0;
    for point in points {
        let measured_start = if seed_batch_size == 1 {
            start
        } else {
            point.saturating_sub(window).max(start)
        };
        // Seed retained snapshots in bounded transactions. This isolates root
        // cardinality from one-small-pack-per-generation fragmentation; a
        // seed batch of one preserves that separate sequential stress case.
        for first in (start + 1..=measured_start).step_by(seed_batch_size) {
            let mutation = repository.mutation_session().await.unwrap();
            let mut staged = Vec::new();
            let mut roots = Vec::new();
            for generation in first..=(first + seed_batch_size - 1).min(measured_start) {
                let directory = tree(generation);
                staged.push(mutation.stage_blob(&bytes(generation, 256)).await.unwrap());
                staged.push(mutation.stage_directory(&directory).await.unwrap());
                roots.push(RootChange::Set {
                    name: name(generation),
                    target: ObjectKey::directory(directory.digest()),
                });
            }
            mutation.publish(staged, roots).await.unwrap();
        }
        let mut timings = Vec::new();
        let mut updates = Vec::new();
        let mut before_window = 0;
        for generation in measured_start + 1..=point {
            if generation == point.saturating_sub(window).max(measured_start) + 1 {
                before_window = disk_bytes(temporary.path());
            }
            let delta = bytes(generation, 256);
            let directory = tree(generation);
            let target = ObjectKey::directory(directory.digest());
            let root = name(generation);
            let before = repository.payloads().pack_read_stats().unwrap();
            let publication_before = repository.publication_profile();
            let started = Instant::now();
            let mutation = repository.mutation_session().await.unwrap();
            let session_nanos = started.elapsed().as_nanos() as u64;
            let phase = Instant::now();
            let changed = mutation.stage_blob(&delta).await.unwrap();
            let blob_nanos = phase.elapsed().as_nanos() as u64;
            let phase = Instant::now();
            let parent = mutation.stage_directory(&directory).await.unwrap();
            let directory_nanos = phase.elapsed().as_nanos() as u64;
            let phase = Instant::now();
            mutation
                .publish_rooted(vec![changed, parent], root, target)
                .await
                .unwrap();
            let publish_nanos = phase.elapsed().as_nanos() as u64;
            let nanos = started.elapsed().as_nanos() as u64;
            drop(mutation);
            let after = repository.payloads().pack_read_stats().unwrap();
            let publication_after = repository.publication_profile();
            let phases = crate::repository::PUBLICATION_PHASES.iter().enumerate().map(|(index, name)| {
                (*name, json!({"calls": publication_after.calls[index] - publication_before.calls[index],
                    "nanos": publication_after.nanos[index] - publication_before.nanos[index]}))
            }).collect::<std::collections::BTreeMap<_, _>>();
            timings.push(nanos);
            updates.push(json!({"generation": generation, "nanos": nanos,
                "session_nanos": session_nanos, "blob_nanos": blob_nanos,
                "directory_nanos": directory_nanos, "publish_nanos": publish_nanos,
                "publication_phases": phases,
                "catalog_get_requests": after.index_requests - before.index_requests,
                "catalog_get_bytes": after.index_bytes - before.index_bytes,
                "catalog_hash_nanos": after.index_hash_nanos - before.index_hash_nanos,
                "catalog_decode_nanos": after.index_decode_nanos - before.index_decode_nanos,
                "catalog_snapshot_calls": after.index_snapshot_calls - before.index_snapshot_calls,
                "catalog_snapshot_nanos": after.index_snapshot_nanos - before.index_snapshot_nanos,
                "catalog_snapshot_payload_bytes_lower_bound": after.index_snapshot_payload_bytes_lower_bound - before.index_snapshot_payload_bytes_lower_bound,
                "catalog_snapshot_accounting_nanos": after.index_snapshot_accounting_nanos - before.index_snapshot_accounting_nanos,
                "catalog_build_calls": after.index_build_calls - before.index_build_calls,
                "catalog_build_nanos": after.index_build_nanos - before.index_build_nanos,
                "catalog_put_requests": after.index_put_requests - before.index_put_requests,
                "catalog_put_bytes": after.index_put_bytes - before.index_put_bytes}));
        }
        let stored = disk_bytes(temporary.path());
        let mut sample = latency(&timings[timings.len().saturating_sub(window)..]);
        sample["interval_latency"] = latency(&timings);
        sample["operation"] = json!("tiny-delta-publication");
        sample["generations"] = json!(point);
        sample["seed_batch_size"] = json!(seed_batch_size);
        sample["coordinates_payload_catalog"] =
            json!(repository.metadata().coordinates_payload_catalog());
        sample["seeded_generations"] = json!(measured_start - start);
        sample["delta_bytes"] = json!(256);
        sample["repository_bytes"] = json!(stored);
        sample["window_storage_growth_bytes"] =
            json!(i128::from(stored) - i128::from(before_window));
        sample["updates"] = json!(updates);
        let stats = repository.payloads().pack_read_stats().unwrap();
        sample["catalog_sharded"] = json!(stats.index_sharded_base);
        sample["catalog_run_objects"] = json!(stats.index_run_objects);
        drop(repository);
        let started = Instant::now();
        repository = Repository::local(temporary.path()).await.unwrap();
        let open_nanos = started.elapsed().as_nanos() as u64;
        audit_history(&repository, point).await;
        let mut unchanged_sessions = Vec::new();
        for _ in 0..setting("CASITA_SCALE_IDLE_SESSIONS", 0) {
            let before = repository.payloads().pack_read_stats().unwrap();
            let started = Instant::now();
            let mutation = repository.mutation_session().await.unwrap();
            let nanos = started.elapsed().as_nanos() as u64;
            drop(mutation);
            let after = repository.payloads().pack_read_stats().unwrap();
            unchanged_sessions.push(json!({"nanos": nanos,
                "catalog_get_requests": after.index_requests - before.index_requests,
                "catalog_get_bytes": after.index_bytes - before.index_bytes,
                "catalog_hash_nanos": after.index_hash_nanos - before.index_hash_nanos,
                "catalog_decode_nanos": after.index_decode_nanos - before.index_decode_nanos}));
        }
        sample["unchanged_sessions"] = json!(unchanged_sessions);
        sample["correctness"] = json!("all retained roots + all bytes + reopen + clean fsck");
        emit(sample);
        emit(
            json!({"operation": "reopen", "generations": point, "seed_batch_size": seed_batch_size, "operations": 1, "wall_seconds": open_nanos as f64 / 1e9}),
        );
        start = point;
    }
}

fn sequence(objects: usize, operations: usize, pattern: &str) -> Vec<usize> {
    let mut state = 0xCA517A_u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state as usize
    };
    let mut shuffled: Vec<_> = (0..objects).collect();
    for i in (1..objects).rev() {
        shuffled.swap(i, next() % (i + 1));
    }
    (0..operations)
        .map(|i| match pattern {
            "sequential" => i % objects,
            "random" => shuffled[i % objects],
            "skewed" => {
                if i % 5 == 0 {
                    next() % objects
                } else {
                    next() % (objects / 8).max(1)
                }
            }
            _ => panic!("unknown access pattern"),
        })
        .collect()
}

#[tokio::test]
#[ignore = "scale probe; benchmark run pack-cache-scale"]
async fn benchmark_pack_cache_working_set() {
    let cache = setting("CASITA_SCALE_CACHE_BYTES", 8 * 1024 * 1024);
    let working = setting("CASITA_SCALE_WORKING_BYTES", 32 * 1024 * 1024);
    let object_bytes = 64 * 1024;
    assert!(
        cache >= 4 * object_bytes
            && working >= 2 * object_bytes
            && working.is_multiple_of(object_bytes)
    );
    let objects = working / object_bytes;
    let operations = setting("CASITA_SCALE_READS", objects * 4);
    assert!(operations >= objects);
    let temporary = tempfile::tempdir().unwrap();
    let expected: Vec<_> = (0..objects)
        .map(|index| bytes(index, object_bytes))
        .collect();
    let ids: Vec<_> = expected.iter().map(|value| blob(value)).collect();
    let store = ChunkedBlobStore::local_packed_with_options(
        temporary.path(),
        crate::PackOptions {
            target_size: 256 * 1024,
            cache_capacity: cache as u64,
        },
    )
    .await
    .unwrap();
    let batch = store.begin_batch();
    for value in &expected {
        store.put_slice(value).await.unwrap();
    }
    store.flush().await.unwrap();
    drop(batch);
    drop(store);
    let pack_bytes = disk_bytes(&temporary.path().join("packs"));
    assert_eq!(
        pack_bytes < cache as u64,
        working < cache,
        "fixture must straddle physical cache capacity"
    );
    for pattern in ["sequential", "random", "skewed"] {
        let order = sequence(objects, operations, pattern);
        let store = ChunkedBlobStore::local_packed_with_options(
            temporary.path(),
            crate::PackOptions {
                target_size: 256 * 1024,
                cache_capacity: cache as u64,
            },
        )
        .await
        .unwrap();
        for phase in ["cold", "warm"] {
            if phase == "warm" {
                for id in &ids {
                    std::hint::black_box(read(&store, id).await);
                }
            }
            store.reset_pack_read_stats();
            let mut nanos = Vec::with_capacity(operations);
            for &index in &order {
                let started = Instant::now();
                let actual = read(&store, &ids[index]).await;
                nanos.push(started.elapsed().as_nanos() as u64);
                assert_eq!(actual, expected[index]);
            }
            let stats = store.pack_read_stats().unwrap();
            if phase == "warm" && working < cache {
                assert_eq!(
                    stats.whole_pack_requests + stats.chunk_range_requests,
                    0,
                    "below-cache warm reads must be resident"
                );
            }
            if working > cache && pattern == "sequential" && phase == "warm" {
                assert!(
                    stats.cache_evictions > 0
                        && stats.whole_pack_bytes + stats.chunk_range_bytes > 0,
                    "above-cache fixture must actually evict and fetch"
                );
            }
            let mut sample = latency(&nanos);
            sample["pack_target_bytes"] = json!(256 * 1024);
            sample["operation"] = json!(format!("{pattern}-{phase}"));
            sample["cache_bytes"] = json!(cache);
            sample["working_set_bytes"] = json!(working);
            sample["physical_pack_bytes"] = json!(pack_bytes);
            sample["working_set_objects"] = json!(objects);
            sample["logical_read_bytes"] = json!(operations * object_bytes);
            sample["pack_range_requests"] = json!(stats.chunk_range_requests);
            sample["whole_pack_requests"] = json!(stats.whole_pack_requests);
            sample["backend_read_bytes"] = json!(stats.whole_pack_bytes + stats.chunk_range_bytes);
            sample["cache_hits"] = json!(stats.cache_hits);
            sample["cache_evictions"] = json!(stats.cache_evictions);
            sample["read_nanos"] = json!(nanos);
            sample["correctness"] = json!("every timed read compared byte-for-byte outside timing");
            emit(sample);
        }
    }
}

/// Compare per-output sessions, shared sessions, and shared publications.
/// Preparation, repository opening, payload audits, and fsck are outside the
/// measured region. All modes perform identical staging work and differ
/// only in mutation-session and metadata-publication batching. The shared-session
/// mode keeps one publication per output to isolate staging pin lifetime.
#[tokio::test]
#[ignore = "Casita output-import benchmark; benchmark run output-import"]
async fn benchmark_output_import() {
    let timings = import_pin_timing::Timings::install();
    let count = setting("CASITA_OUTPUT_IMPORT_COUNT", 8);
    let size = setting("CASITA_OUTPUT_IMPORT_SIZE", 4096);
    let mode = std::env::var("CASITA_OUTPUT_IMPORT_MODE").unwrap_or_else(|_| "per-output".into());
    assert!(count > 0 && count <= 1024);
    assert!(size <= 64 * 1024 * 1024);
    assert!(matches!(
        mode.as_str(),
        "per-output" | "shared-session" | "batch-api" | "atomic-api" | "batched"
    ));

    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("source");
    std::fs::create_dir(&source).unwrap();
    let expected: Vec<_> = (0..count).map(|index| bytes(index + 17, size)).collect();
    for (index, value) in expected.iter().enumerate() {
        std::fs::write(source.join(format!("output-{index:08}")), value).unwrap();
    }
    let repository = Repository::local(temporary.path().join("repository"))
        .await
        .unwrap();
    let expected_keys: Vec<_> = expected
        .iter()
        .map(|value| ObjectKey::blob(blob(value)))
        .collect();

    let application = crate::Repository {
        inner: repository.clone().into_builtin(),
    };
    crate::metadata::flush_repository_leases().await.unwrap();
    timings.reset();
    let started = Instant::now();
    let mut session_nanos = 0u64;
    let mut stage_nanos = 0u64;
    let mut publish_nanos = 0u64;
    let mut import_nanos = 0u64;
    if mode == "per-output" {
        for (index, expected_key) in expected_keys.iter().enumerate() {
            let phase = Instant::now();
            let mutation = application.inner.mutation_session().await.unwrap();
            session_nanos += phase.elapsed().as_nanos() as u64;
            let phase = Instant::now();
            let mut file = tokio::fs::File::open(source.join(format!("output-{index:08}")))
                .await
                .unwrap();
            let staged = mutation.stage_blob_reader(&mut file).await.unwrap();
            stage_nanos += phase.elapsed().as_nanos() as u64;
            let phase = Instant::now();
            mutation
                .publish_rooted(vec![staged], output_root(index), expected_key.clone())
                .await
                .unwrap();
            publish_nanos += phase.elapsed().as_nanos() as u64;
        }
    } else if mode == "shared-session" {
        // Exercise the application API while preserving per-output publication.
        let phase = Instant::now();
        let session = application.import_session().await.unwrap();
        session_nanos = phase.elapsed().as_nanos() as u64;
        for (index, expected_key) in expected_keys.iter().enumerate() {
            let mut file = tokio::fs::File::open(source.join(format!("output-{index:08}")))
                .await
                .unwrap();
            let phase = Instant::now();
            let staged = session.inner.stage_blob_reader(&mut file).await.unwrap();
            stage_nanos += phase.elapsed().as_nanos() as u64;
            let phase = Instant::now();
            session
                .inner
                .publish_rooted(vec![staged], output_root(index), expected_key.clone())
                .await
                .unwrap();
            publish_nanos += phase.elapsed().as_nanos() as u64;
        }
    } else if mode == "batch-api" {
        let phase = Instant::now();
        // Open lazily so the batch retains only one input file descriptor.
        let inputs = (0..count).map(|index| {
            let file = std::fs::File::open(source.join(format!("output-{index:08}"))).unwrap();
            crate::import::BlobImport::new(tokio::fs::File::from_std(file), output_root(index))
        });
        let keys = application
            .import(crate::import::ImportSequence::new(inputs))
            .await
            .unwrap();
        import_nanos = phase.elapsed().as_nanos() as u64;
        assert_eq!(keys, expected_keys);
    } else if mode == "atomic-api" {
        let phase = Instant::now();
        let inputs = expected
            .iter()
            .enumerate()
            .map(|(index, _)| {
                // Readers open on first use so preparing the atomic request does
                // not open every file descriptor at once.
                crate::import::BlobImport::new(
                    LazyOutputFile::new(source.join(format!("output-{index:08}"))),
                    output_root(index),
                )
            })
            .collect::<Vec<_>>();
        let keys = application
            .import(crate::import::BlobImport::batch(inputs))
            .await
            .unwrap();
        import_nanos = phase.elapsed().as_nanos() as u64;
        assert_eq!(keys, expected_keys);
    } else {
        let phase = Instant::now();
        let mutation = application.inner.mutation_session().await.unwrap();
        session_nanos = phase.elapsed().as_nanos() as u64;
        let mut staged = Vec::with_capacity(count);
        let mut roots = Vec::with_capacity(count);
        for (index, expected_key) in expected_keys.iter().enumerate() {
            let phase = Instant::now();
            let mut file = tokio::fs::File::open(source.join(format!("output-{index:08}")))
                .await
                .unwrap();
            staged.push(mutation.stage_blob_reader(&mut file).await.unwrap());
            stage_nanos += phase.elapsed().as_nanos() as u64;
            roots.push(RootChange::Set {
                name: output_root(index),
                target: expected_key.clone(),
            });
        }
        let phase = Instant::now();
        mutation.publish(staged, roots).await.unwrap();
        publish_nanos = phase.elapsed().as_nanos() as u64;
    }
    crate::metadata::flush_repository_leases().await.unwrap();
    let total_nanos = started.elapsed().as_nanos() as u64;
    let ledger = timings.take_ledger();
    let phases = timings.take(started);

    let snapshot = repository.metadata().snapshot().await.unwrap();
    for (index, expected_key) in expected_keys.iter().enumerate() {
        assert_eq!(
            snapshot.root(&output_root(index)).await.unwrap(),
            Some(expected_key.clone())
        );
        let actual = read(repository.payloads(), &blob(&expected[index])).await;
        assert_eq!(actual, expected[index]);
    }
    drop(snapshot);
    assert!(repository.fsck().await.unwrap().is_clean());
    emit_output_import(json!({
        "operation": "output-import",
        "ledger": ledger,
        "phases": phases,
        "mode": mode,
        "outputs": count,
        "output_bytes": size,
        "logical_bytes": count * size,
        "nanos": total_nanos,
        "session_nanos": session_nanos,
        "stage_nanos": stage_nanos,
        "publish_nanos": publish_nanos,
        "import_nanos": import_nanos,
        "correctness": "exact roots, byte-for-byte payload reads, clean fsck",
    }));
}

/// Separate roots, one session in both modes, fresh persistent repositories.
#[tokio::test]
#[ignore = "multi-root filesystem benchmark; benchmark run filesystem-outputs"]
async fn benchmark_filesystem_outputs() {
    let count = setting("CASITA_FS_OUTPUTS_COUNT", 8);
    let files = setting("CASITA_FS_OUTPUTS_FILES", 1);
    let size = setting("CASITA_FS_OUTPUTS_SIZE", 4096);
    let mode = std::env::var("CASITA_FS_OUTPUTS_MODE").unwrap_or_else(|_| "multi-root".into());
    assert!((1..=1024).contains(&count) && files <= 16384 && size <= 1048576);
    assert!(matches!(mode.as_str(), "per-output" | "multi-root"));
    let temporary = tempfile::tempdir().unwrap();
    let mut paths = Vec::new();
    let mut expected_keys = Vec::new();
    let mut expected_payloads = Vec::new();
    for index in 0..count {
        let path = temporary.path().join(format!("output-{index}"));
        std::fs::create_dir_all(path.join("nested")).unwrap();
        std::fs::create_dir(path.join("empty")).unwrap();
        let mut nested = Directory::default();
        for file in 0..files {
            let value = bytes(index * files + file + 17, size);
            let name = format!("file-{file:08}");
            let destination = path.join("nested").join(&name);
            std::fs::write(&destination, &value).unwrap();
            let executable = cfg!(unix) && file % 2 == 0;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(
                    &destination,
                    std::fs::Permissions::from_mode(if executable { 0o755 } else { 0o644 }),
                )
                .unwrap();
            }
            nested
                .add(
                    crate::PathComponent::try_from(name.as_str()).unwrap(),
                    Node::File {
                        digest: blob(&value),
                        size: size as u64,
                        executable,
                    },
                )
                .unwrap();
            expected_payloads.push(value);
        }
        let mut directory = Directory::default();
        for (name, child) in [("nested", nested), ("empty", Directory::default())] {
            directory
                .add(
                    crate::PathComponent::try_from(name).unwrap(),
                    Node::Directory {
                        digest: child.digest(),
                        size: child.size(),
                    },
                )
                .unwrap();
        }
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("../outside-missing", path.join("link")).unwrap();
            directory
                .add(
                    crate::PathComponent::try_from("link").unwrap(),
                    Node::Symlink {
                        target: crate::SymlinkTarget::try_from("../outside-missing").unwrap(),
                    },
                )
                .unwrap();
        }
        expected_keys.push(ObjectKey::directory(directory.digest()));
        paths.push((path, Some(output_root(index)), None));
    }
    let repository = Repository::local(temporary.path().join("repository"))
        .await
        .unwrap();
    let started = Instant::now();
    let mutation = repository.mutation_session().await.unwrap();
    let session_nanos = started.elapsed().as_nanos() as u64;
    let mut stage_nanos = 0;
    let mut traversal_nanos = 0;
    let mut publish_nanos = 0;
    let mut maintenance_nanos = 0;
    let mut pages = 0;
    let mut publications = 0;
    let mut actual_keys = Vec::new();
    let jobs = if mode == "multi-root" {
        vec![paths]
    } else {
        paths.into_iter().map(|path| vec![path]).collect()
    };
    let walks = jobs.len();
    for job in jobs {
        let (keys, stats) = mutation
            .import_paths_inner(
                job,
                false,
                crate::filesystem::DEFAULT_FILE_CONCURRENCY,
                mode == "multi-root",
            )
            .await
            .unwrap();
        actual_keys.extend(keys);
        stage_nanos += stats.stage_nanos;
        traversal_nanos += stats.traversal_nanos;
        publish_nanos += stats.publish_nanos;
        maintenance_nanos += stats.maintenance_nanos;
        pages += stats.pages;
        publications += stats.publications;
    }
    drop(mutation);
    let nanos = started.elapsed().as_nanos() as u64;
    let entries_per_root = files + 3 + usize::from(cfg!(unix));
    let expected_pages = if mode == "multi-root" {
        (count * entries_per_root).div_ceil(crate::filesystem::WALK_PAGE_ENTRIES)
    } else {
        count * entries_per_root.div_ceil(crate::filesystem::WALK_PAGE_ENTRIES)
    };
    assert_eq!(pages, expected_pages);
    let batch_size = crate::FormatLimits::default().max_batch_objects;
    let expected_publications = if mode == "multi-root" {
        count * (files + 3) / batch_size + 1
    } else {
        count * ((files + 3) / batch_size + 1)
    };
    assert_eq!(publications, expected_publications);
    assert_eq!(actual_keys, expected_keys);
    let snapshot = repository.metadata().snapshot().await.unwrap();
    for (index, key) in expected_keys.iter().enumerate() {
        assert_eq!(
            snapshot.root(&output_root(index)).await.unwrap(),
            Some(key.clone())
        );
    }
    drop(snapshot);
    for value in &expected_payloads {
        assert_eq!(read(repository.payloads(), &blob(value)).await, *value);
    }
    assert!(repository.fsck().await.unwrap().is_clean());
    println!(
        "filesystem_outputs_sample {}",
        json!({
            "operation": "filesystem-outputs", "mode": mode, "outputs": count,
            "files": files, "file_bytes": size, "logical_bytes": count * files * size,
            "nanos": nanos, "session_nanos": session_nanos, "stage_nanos": stage_nanos,
            "publish_nanos": publish_nanos, "maintenance_nanos": maintenance_nanos,
            "traversal_nanos": traversal_nanos, "pages": pages, "publications": publications,
            "walks": walks,
            "correctness": "independent tree digests, exact roots, byte-for-byte payload reads, clean fsck",
        })
    );
}
