//! Object-store-authoritative [`MetadataStore`] backed by Chroma's wal3.
//!
//! State is an immutable full checkpoint followed by a bounded cumulative
//! delta tail. Each new tail record repeats the small exact deltas since its
//! checkpoint, so reopening needs at most two fragment GETs: the tail and its
//! checkpoint. wal3's conditional manifest publication serializes competing
//! writers. Collection and every ninth mutation install a fresh checkpoint.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::TryStreamExt;
use futures::stream::BoxStream;

#[cfg(test)]
use super::wal3_shard::MAX_STATE_MAP_BYTES;
use super::wal3_shard::{
    DEFAULT_OBJECT_SHARD_TARGET_BYTES, DEFAULT_ROOT_SHARD_TARGET_BYTES, DEFAULT_SHARD_CACHE_BYTES,
    ObjectShardStorage, StateShardMap, decode_state_shard_map, encode_object_shard,
    encode_root_shard, encode_state_shard_map,
};
use super::{
    AddedObject, CollectedObjects, CommitChange, CommitResult, CommitState, MetadataError,
    MetadataMutation, MetadataSnapshot, MetadataStore, ObjectChange, RETAINED_PAGE,
    RetainedObjects, RootChange, fresh_revision, validate_commit_transition,
};
use crate::invariant::{self, Violation};
use crate::object::{ObjectKey, ObjectRecord, RepositoryRevision, RootName, RootRecord};

#[cfg(test)]
#[path = "wal3/experiments.rs"]
mod experiments;
#[cfg(test)]
#[path = "wal3/transition_tests.rs"]
mod transition_tests;

mod coordination;
pub use coordination::Wal3RepositoryHold;

const STATE_MAGIC_V4: &[u8] = b"casita.wal3.state.v4\0";
const STATE_MAGIC_V3: &[u8] = b"casita.wal3.state.v3\0";
const DELTA_MAGIC_V1: &[u8] = b"casita.wal3.delta.v1\0";
const MAX_PAYLOAD_CATALOG_BYTES: usize = 8 * 1024 * 1024;
const MAX_COMMIT_CONTENTION_ATTEMPTS: usize = 12;
const MAX_TAIL_DELTAS: usize = 8;
const MAX_DELTA_RECORD_BYTES: usize = 1024 * 1024;

type Wal3Writer = wal3::LogWriter<
    (wal3::FragmentSeqNo, wal3::LogPosition),
    wal3::S3FragmentManagerFactory,
    wal3::S3ManifestManagerFactory,
>;
type Wal3Reader = wal3::LogReader<
    (wal3::FragmentSeqNo, wal3::LogPosition),
    wal3::S3FragmentPuller,
    wal3::ManifestReader,
>;

/// Cumulative wal3 object-store work performed by one shared state-store handle.
///
/// Read counts include failed attempts. Successful append counts describe the
/// fragment and manifest PUTs that make one wal3 record durable. Nanosecond
/// fields measure elapsed wall time around the named operation and may overlap
/// when callers execute concurrently.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Wal3ReadStats {
    /// Initial stable-manifest GET performed while opening wal3's writer.
    pub writer_open_requests: u64,
    /// Time spent opening wal3's writer and its initial stable manifest.
    pub writer_open_nanos: u64,
    /// Strong manifest GET attempts made after the writer was opened.
    pub manifest_load_requests: u64,
    /// Time spent in those strong manifest GET attempts.
    pub manifest_load_nanos: u64,
    /// Conditional manifest witness checks for cached checkpoints.
    pub manifest_refresh_requests: u64,
    /// Time spent verifying cached manifest witnesses.
    pub manifest_refresh_nanos: u64,
    /// Full wal3 fragment GET attempts.
    pub fragment_get_requests: u64,
    /// Bytes returned by successful fragment GETs.
    pub fragment_get_bytes: u64,
    /// Records decoded from successful fragment GETs.
    pub fragment_records: u64,
    /// Uncompressed record-body bytes decoded from successful fragment GETs.
    pub fragment_record_bytes: u64,
    /// Time spent fetching fragment bytes.
    pub fragment_get_nanos: u64,
    /// Time spent verifying and decoding Parquet fragments.
    pub parquet_parse_nanos: u64,
    /// Time spent decoding Casita state records.
    pub state_decode_nanos: u64,
    /// Immutable fragment PUTs completed by successful state appends.
    pub fragment_put_requests: u64,
    /// Conditional manifest PUTs completed by successful state appends.
    pub manifest_put_requests: u64,
    /// Snapshots served after an unchanged manifest ETag.
    pub checkpoint_cache_hits: u64,
    /// Snapshots that required or attempted a fresh checkpoint load.
    pub checkpoint_cache_misses: u64,
    /// Encoded bytes in the latest checkpoint installed in this handle.
    pub checkpoint_bytes: u64,
    /// Object records in the latest checkpoint installed in this handle.
    pub checkpoint_objects: u64,
    /// Named roots in the latest checkpoint installed in this handle.
    pub checkpoint_roots: u64,
    /// Validated-closure keys in the latest checkpoint installed in this handle.
    pub checkpoint_validated: u64,
    /// Exact deltas represented by the latest cumulative tail record.
    pub tail_deltas: u64,
    /// Cold immutable logical-state shard GETs (objects and roots).
    pub logical_shard_get_requests: u64,
    /// Bytes returned by cold immutable logical-state shard GETs.
    pub logical_shard_get_bytes: u64,
    /// Content-addressed logical-state shard PUT attempts.
    pub logical_shard_put_requests: u64,
    /// Logical-state shard reads served from the bounded local cache.
    pub logical_shard_cache_hits: u64,
    /// Strong GET attempts for the checkpoint/GC publication barrier.
    pub logical_shard_barrier_get_requests: u64,
    /// Conditional PUT attempts for the checkpoint/GC publication barrier.
    pub logical_shard_barrier_put_requests: u64,
    /// LIST requests used by logical-state orphan collection.
    pub logical_shard_inventory_list_requests: u64,
    /// Batched DELETE requests used by logical-state orphan collection.
    pub logical_shard_delete_requests: u64,
}

#[derive(Default)]
struct Wal3ReadMetrics {
    writer_open_requests: AtomicU64,
    writer_open_nanos: AtomicU64,
    manifest_load_requests: AtomicU64,
    manifest_load_nanos: AtomicU64,
    manifest_refresh_requests: AtomicU64,
    manifest_refresh_nanos: AtomicU64,
    fragment_get_requests: AtomicU64,
    fragment_get_bytes: AtomicU64,
    fragment_records: AtomicU64,
    fragment_record_bytes: AtomicU64,
    fragment_get_nanos: AtomicU64,
    parquet_parse_nanos: AtomicU64,
    state_decode_nanos: AtomicU64,
    fragment_put_requests: AtomicU64,
    manifest_put_requests: AtomicU64,
    checkpoint_cache_hits: AtomicU64,
    checkpoint_cache_misses: AtomicU64,
    checkpoint_bytes: AtomicU64,
    checkpoint_objects: AtomicU64,
    checkpoint_roots: AtomicU64,
    checkpoint_validated: AtomicU64,
    tail_deltas: AtomicU64,
}

impl Wal3ReadMetrics {
    fn snapshot(&self) -> Wal3ReadStats {
        Wal3ReadStats {
            writer_open_requests: self.writer_open_requests.load(Ordering::Relaxed),
            writer_open_nanos: self.writer_open_nanos.load(Ordering::Relaxed),
            manifest_load_requests: self.manifest_load_requests.load(Ordering::Relaxed),
            manifest_load_nanos: self.manifest_load_nanos.load(Ordering::Relaxed),
            manifest_refresh_requests: self.manifest_refresh_requests.load(Ordering::Relaxed),
            manifest_refresh_nanos: self.manifest_refresh_nanos.load(Ordering::Relaxed),
            fragment_get_requests: self.fragment_get_requests.load(Ordering::Relaxed),
            fragment_get_bytes: self.fragment_get_bytes.load(Ordering::Relaxed),
            fragment_records: self.fragment_records.load(Ordering::Relaxed),
            fragment_record_bytes: self.fragment_record_bytes.load(Ordering::Relaxed),
            fragment_get_nanos: self.fragment_get_nanos.load(Ordering::Relaxed),
            parquet_parse_nanos: self.parquet_parse_nanos.load(Ordering::Relaxed),
            state_decode_nanos: self.state_decode_nanos.load(Ordering::Relaxed),
            fragment_put_requests: self.fragment_put_requests.load(Ordering::Relaxed),
            manifest_put_requests: self.manifest_put_requests.load(Ordering::Relaxed),
            checkpoint_cache_hits: self.checkpoint_cache_hits.load(Ordering::Relaxed),
            checkpoint_cache_misses: self.checkpoint_cache_misses.load(Ordering::Relaxed),
            checkpoint_bytes: self.checkpoint_bytes.load(Ordering::Relaxed),
            checkpoint_objects: self.checkpoint_objects.load(Ordering::Relaxed),
            checkpoint_roots: self.checkpoint_roots.load(Ordering::Relaxed),
            checkpoint_validated: self.checkpoint_validated.load(Ordering::Relaxed),
            tail_deltas: self.tail_deltas.load(Ordering::Relaxed),
            logical_shard_get_requests: 0,
            logical_shard_get_bytes: 0,
            logical_shard_put_requests: 0,
            logical_shard_cache_hits: 0,
            logical_shard_barrier_get_requests: 0,
            logical_shard_barrier_put_requests: 0,
            logical_shard_inventory_list_requests: 0,
            logical_shard_delete_requests: 0,
        }
    }

    fn reset(&self) {
        self.writer_open_requests.store(0, Ordering::Relaxed);
        self.writer_open_nanos.store(0, Ordering::Relaxed);
        self.manifest_load_requests.store(0, Ordering::Relaxed);
        self.manifest_load_nanos.store(0, Ordering::Relaxed);
        self.manifest_refresh_requests.store(0, Ordering::Relaxed);
        self.manifest_refresh_nanos.store(0, Ordering::Relaxed);
        self.fragment_get_requests.store(0, Ordering::Relaxed);
        self.fragment_get_bytes.store(0, Ordering::Relaxed);
        self.fragment_records.store(0, Ordering::Relaxed);
        self.fragment_record_bytes.store(0, Ordering::Relaxed);
        self.fragment_get_nanos.store(0, Ordering::Relaxed);
        self.parquet_parse_nanos.store(0, Ordering::Relaxed);
        self.state_decode_nanos.store(0, Ordering::Relaxed);
        self.fragment_put_requests.store(0, Ordering::Relaxed);
        self.manifest_put_requests.store(0, Ordering::Relaxed);
        self.checkpoint_cache_hits.store(0, Ordering::Relaxed);
        self.checkpoint_cache_misses.store(0, Ordering::Relaxed);
    }
}

/// Revisioned Casita state published through a wal3 object-store log.
///
/// All runners of one repository must use the same wal3 prefix.  `storage`
/// may be S3, an S3-compatible service, or Chroma's local test storage; its
/// conditional writes are wal3's cross-process serialization point.
#[derive(Clone)]
pub struct Wal3MetadataStore {
    storage: Arc<chroma_storage::Storage>,
    prefix: String,
    writer_name: String,
    writer: Arc<Wal3Writer>,
    // wal3 batches append requests with the same required fragment start. A
    // MetadataStore commit is a compare-and-swap, so clones must submit at most
    // one such append at a time.
    commit_lock: Arc<tokio::sync::Mutex<()>>,
    // The complete state record is immutable once its manifest is published.
    // Keep the decoded checkpoint beside that manifest's witness so snapshots
    // can refresh freshness with one conditional GET instead of downloading
    // and decoding the same fragment again.
    checkpoint: Arc<std::sync::Mutex<Option<CachedCheckpoint>>>,
    // Collapse concurrent cache misses into one manifest/fragment reload.
    snapshot_refresh_lock: Arc<tokio::sync::Mutex<()>>,
    read_metrics: Arc<Wal3ReadMetrics>,
    object_shards: ObjectShardStorage,
    coordination: Arc<tokio::sync::OnceCell<Arc<coordination::Coordination>>>,
    is_coordination: bool,
    #[cfg(test)]
    checkpoint_pause: Option<Arc<CheckpointPause>>,
}

#[cfg(test)]
#[derive(Default)]
struct CheckpointPause {
    reached: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

impl Wal3MetadataStore {
    /// Open an AWS S3-backed state log using the standard AWS credential
    /// provider chain.  Use [`Self::open`] with a custom
    /// [`chroma_storage::Storage`] for an S3-compatible endpoint or explicit
    /// credentials.
    #[tracing::instrument(name = "state.wal3.open", skip_all, fields(storage = "s3"))]
    pub async fn open_s3(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        writer: impl Into<String>,
    ) -> Result<Self, MetadataError> {
        use chroma_config::{Configurable, registry::Registry};

        let config =
            chroma_storage::config::StorageConfig::S3(chroma_storage::config::S3StorageConfig {
                bucket: bucket.into(),
                credentials: chroma_storage::config::S3CredentialsConfig::AWS,
                ..Default::default()
            });
        let storage = chroma_storage::Storage::try_from_config(&config, &Registry::default())
            .await
            .map_err(|error| MetadataError::Backend(format!("wal3 S3 configuration: {error}")))?;
        Self::open(Arc::new(storage), prefix, writer).await
    }

    /// Open (or atomically initialize) the state log below `prefix`.
    ///
    /// `writer` is diagnostic metadata written by wal3, normally a stable
    /// runner name or instance ID.  It is not an ownership lease: multiple
    /// runners may open this store, and a racing commit becomes a normal
    /// [`MetadataError::StaleRevision`].
    #[tracing::instrument(name = "state.wal3.open", skip_all, fields(storage = "configured"))]
    pub async fn open(
        storage: Arc<chroma_storage::Storage>,
        prefix: impl Into<String>,
        writer: impl Into<String>,
    ) -> Result<Self, MetadataError> {
        let prefix = prefix.into();
        let writer_name = writer.into();
        let write = wal3::LogWriterOptions::default();
        let read = wal3::LogReaderOptions::default();
        let (fragments, manifests) = wal3::create_s3_factories(
            write.clone(),
            read,
            storage.clone(),
            prefix.clone(),
            writer_name.clone(),
            Arc::new(()),
            Arc::new(()),
        );
        let read_metrics = Arc::new(Wal3ReadMetrics::default());
        let writer_started = Instant::now();
        let writer =
            wal3::LogWriter::open_or_initialize(write, &writer_name, fragments, manifests, None)
                .await
                .map_err(wal3_error)?;
        read_metrics
            .writer_open_requests
            .store(1, Ordering::Relaxed);
        read_metrics
            .writer_open_nanos
            .store(elapsed_nanos(writer_started), Ordering::Relaxed);
        // Opening the writer has already loaded a stable manifest. Reuse it
        // instead of immediately issuing a second manifest GET.
        let manifest_and_witness = writer.manifest_and_witness().await.map_err(wal3_error)?;
        let store = Self {
            coordination: Arc::new(tokio::sync::OnceCell::new()),
            is_coordination: false,
            object_shards: ObjectShardStorage::new(
                storage.clone(),
                prefix.clone(),
                DEFAULT_SHARD_CACHE_BYTES,
            ),
            storage,
            prefix,
            writer_name,
            writer: Arc::new(writer),
            commit_lock: Arc::new(tokio::sync::Mutex::new(())),
            checkpoint: Arc::new(std::sync::Mutex::new(None)),
            snapshot_refresh_lock: Arc::new(tokio::sync::Mutex::new(())),
            read_metrics,
            #[cfg(test)]
            checkpoint_pause: None,
        };
        let loaded = store.ensure_initialized(manifest_and_witness).await?;
        store.cache_loaded(loaded)?;
        Ok(store)
    }

    /// Return a point-in-time snapshot of cumulative wal3 read-path metrics.
    pub fn read_stats(&self) -> Wal3ReadStats {
        let mut stats = self.read_metrics.snapshot();
        let shards = self.object_shards.stats();
        stats.logical_shard_get_requests = shards.get_requests;
        stats.logical_shard_get_bytes = shards.get_bytes;
        stats.logical_shard_put_requests = shards.put_requests;
        stats.logical_shard_cache_hits = shards.cache_hits;
        stats.logical_shard_barrier_get_requests = shards.barrier_get_requests;
        stats.logical_shard_barrier_put_requests = shards.barrier_put_requests;
        stats.logical_shard_inventory_list_requests = shards.inventory_list_requests;
        stats.logical_shard_delete_requests = shards.delete_requests;
        stats
    }

    /// Reset cumulative wal3 read-path metrics for isolated phase measurements.
    ///
    /// Resetting is atomic per counter but not a transaction across counters;
    /// benchmark callers should avoid concurrent operations while resetting.
    /// The latest-checkpoint size and cardinality gauges are preserved.
    pub fn reset_read_stats(&self) {
        self.read_metrics.reset();
        self.object_shards.reset_stats();
    }

    /// Return the checkpoint already validated while opening this handle,
    /// without issuing another manifest freshness request.
    ///
    /// Repository construction uses this snapshot to seed payload discovery;
    /// ordinary reads should use [`MetadataStore::snapshot`] so they refresh
    /// against concurrent writers.
    #[doc(hidden)]
    pub fn opened_snapshot(&self) -> Arc<dyn MetadataSnapshot> {
        let state = self
            .checkpoint
            .lock()
            .unwrap()
            .as_ref()
            .expect("a successfully opened wal3 store has a checkpoint")
            .state
            .clone();
        Arc::new(Wal3Snapshot {
            state,
            shards: self.object_shards.clone(),
        })
    }

    async fn reader(&self) -> Result<Wal3Reader, MetadataError> {
        wal3::LogReader::open_classic(
            wal3::LogReaderOptions::default(),
            self.storage.clone(),
            self.prefix.clone(),
        )
        .await
        .map_err(wal3_error)
    }

    /// Advance wal3's intrinsic cursor to the latest full checkpoint and
    /// garbage-collect older state-log fragments.
    ///
    /// `reader_grace_period` must exceed the longest time a reader can retain
    /// a previously loaded manifest before fetching its fragments.  wal3's
    /// phase 2 first removes garbage from the live manifest; this method waits
    /// for that interval before phase 3 physically deletes the old objects.
    /// This waits for exclusive repository admission across runners. A failed
    /// or cancelled collection retains its durable hold for offline recovery.
    /// Drain [`crate::flush_repository_leases`] before runtime shutdown.
    #[tracing::instrument(
        name = "state.wal3.collect",
        skip_all,
        fields(reader_grace_ms = reader_grace_period.as_millis())
    )]
    pub async fn collect_wal(
        &self,
        reader_grace_period: std::time::Duration,
    ) -> Result<(), MetadataError> {
        self.run_wal_collection(reader_grace_period, false).await
    }

    async fn collect_wal_inner(
        &self,
        reader_grace_period: Duration,
        repository_hold: super::RepositoryLease,
    ) -> Result<(), MetadataError> {
        let mut scoped = self.clone();
        scoped.object_shards = self.object_shards.for_write_operation();
        scoped
            .collect_wal_scoped(reader_grace_period, repository_hold)
            .await
    }

    /// Retry the exact set of abandoned metadata-shard deletion claims.
    ///
    /// The old owner and all requests covered by these claims MUST have stopped.
    /// Recover its separate operational collector token first. This operation
    /// refuses payload claims, referenced shards, and paths outside this log.
    /// It retains each claim until its retry settles and runs through caller
    /// cancellation. Reinspect after an error: earlier claims may have finished.
    pub async fn recover_wal_deletions(
        &self,
        claims: BTreeSet<super::PinToken>,
    ) -> Result<(), MetadataError> {
        if claims.is_empty() {
            return Ok(());
        }
        let hold = self.try_collection_lease().await?.ok_or_else(|| {
            MetadataError::Transient("another collector owns repository admission".into())
        })?;
        self.recover_wal_deletions_owned(claims, hold).await
    }

    async fn recover_wal_deletions_owned(
        &self,
        claims: BTreeSet<super::PinToken>,
        mut hold: super::RepositoryLease,
    ) -> Result<(), MetadataError> {
        let store = self.clone();
        super::run_lease_task("metadata recovery", |send| async move {
            let _guard = store.commit_lock.lock().await;
            let result = async {
                let barrier = store.object_shards.acquire_gc_barrier().await?;
                let outcome = async {
                    let pins = store.pin_store().await?;
                    let live = store.live_shard_paths(&store.reader().await?).await?;
                    let paths = store.object_shards.deletion_recovery_paths(
                        &pins.inventory().await?,
                        &claims,
                        &live,
                    )?;
                    hold.retain_on_drop();
                    store
                        .object_shards
                        .recover_deletion_paths(paths, pins)
                        .await?;
                    hold.release_on_drop();
                    Ok(())
                }
                .await;
                let _ = store.object_shards.release_barrier(&barrier).await;
                outcome
            }
            .await;
            drop(send.send(result));
            Ok(())
        })
        .await?
    }

    async fn collect_wal_scoped(
        &self,
        reader_grace_period: Duration,
        mut repository_hold: super::RepositoryLease,
    ) -> Result<(), MetadataError> {
        let _commit_guard = self.commit_lock.lock().await;
        let lease = self.object_shards.acquire_gc_barrier().await?;
        repository_hold.retain_on_drop();
        let outcome = async {
            let loaded = self.load_state_at_manifest().await?;
            if loaded.state.is_none() {
                return Ok(());
            }
            let loaded = self.publish_gc_fence(loaded, &lease).await?;
            let candidates = self.object_shards.list_paths().await?;
            let checkpoint_position = loaded.base_position()?;
            let reader = self.reader().await?;
            boxed_wal_future(reader.update_intrinsic_cursor(
                checkpoint_position,
                wal3::now_micros(),
                &self.writer_name,
                false,
            ))
            .await
            .map_err(wal3_error)?;
            let options = wal3::GarbageCollectionOptions::default();
            let gc_state = boxed_wal_future(
                self.writer
                    .garbage_collect_phase1_compute_garbage(&options, Some(checkpoint_position)),
            )
            .await
            .map_err(wal3_error)?
            .unwrap_or_default();
            boxed_wal_future(self.writer.garbage_collect_phase2_update_manifest(&options))
                .await
                .map_err(wal3_error)?;
            tokio::time::sleep(reader_grace_period).await;
            boxed_wal_future(
                self.writer
                    .garbage_collect_phase3_delete_garbage(&options, &gc_state),
            )
            .await
            .map_err(wal3_error)?;

            let live = self.live_shard_paths(&reader).await?;
            if !self.object_shards.owns_barrier(&lease).await? {
                return Err(MetadataError::Transient(
                    "logical shard GC was fenced before orphan deletion".to_owned(),
                ));
            }
            let orphaned = candidates
                .into_iter()
                .filter(|path| !live.contains(path))
                .collect::<Vec<_>>();
            tracing::info!(
                orphaned_shards = orphaned.len(),
                "WAL3 garbage collection planned orphan deletion"
            );
            self.object_shards
                .delete_paths_pinned(&orphaned, self.pin_store().await?)
                .await
        }
        .await;
        let _ = self.object_shards.release_barrier(&lease).await;
        if outcome.is_ok() {
            repository_hold.release_on_drop();
        }
        outcome
    }

    async fn publish_gc_fence(
        &self,
        mut loaded: LoadedState,
        lease: &super::wal3_shard::ShardBarrierLease,
    ) -> Result<LoadedState, MetadataError> {
        for _ in 0..MAX_COMMIT_CONTENTION_ATTEMPTS {
            if !self.object_shards.owns_barrier(lease).await? {
                return Err(MetadataError::Transient(
                    "logical shard GC was fenced by another maintenance runner".to_owned(),
                ));
            }
            let state = loaded.state.as_ref().ok_or_else(|| {
                MetadataError::Corruption("initialized wal3 log has no state record".to_owned())
            })?;
            let compacted = compact_state_objects(&self.object_shards, state).await?;
            invariant::check(|| validate_compaction("wal3.gc_fence", state, &compacted))?;
            let record = encode_state(&compacted)?;
            match self
                .writer
                .append_with_options_outcome(
                    record,
                    Some(
                        wal3::AppendOptions::default()
                            .with_required_fragment_start(loaded.next_write_position),
                    ),
                )
                .await
            {
                Ok(wal3::AppendOutcome::Committed(_)) => {
                    self.record_successful_append();
                    return self.load_state_at_manifest().await;
                }
                Ok(wal3::AppendOutcome::Contended(_)) => {
                    loaded = self.load_state_at_manifest().await?;
                    if loaded.tail_deltas.is_empty()
                        && loaded.base_position()? == loaded.latest_position()?
                    {
                        if !self.object_shards.owns_barrier(lease).await? {
                            return Err(MetadataError::Transient(
                                "logical shard GC was fenced by another maintenance runner"
                                    .to_owned(),
                            ));
                        }
                        return Ok(loaded);
                    }
                }
                Err(error) => return Err(wal3_error(error)),
            }
        }
        Err(MetadataError::Transient(format!(
            "logical shard GC fence remained contended after {MAX_COMMIT_CONTENTION_ATTEMPTS} attempts"
        )))
    }

    async fn live_shard_paths(
        &self,
        reader: &Wal3Reader,
    ) -> Result<BTreeSet<String>, MetadataError> {
        let manifest = reader
            .manifest()
            .await
            .map_err(wal3_error)?
            .ok_or_else(|| {
                MetadataError::Corruption("wal3 manifest disappeared during GC".to_owned())
            })?;
        let mut short_read = false;
        let fragments = reader
            .scan_with_cache(
                &manifest,
                manifest.oldest_timestamp(),
                wal3::Limits::UNLIMITED,
                &mut short_read,
            )
            .await
            .map_err(wal3_error)?;
        if short_read {
            return Err(MetadataError::Corruption(
                "wal3 live-reference scan was unexpectedly truncated".to_owned(),
            ));
        }

        let mut live = BTreeSet::new();
        for fragment in fragments {
            for (_, record) in self.read_fragment_entries(reader, &fragment).await? {
                if record.starts_with(STATE_MAGIC_V3) || record.starts_with(STATE_MAGIC_V4) {
                    let state = decode_state(&record)?;
                    live.extend(
                        self.object_shards
                            .referenced_paths(state.base_objects.as_ref()),
                    );
                } else if record.starts_with(DELTA_MAGIC_V1) {
                    decode_delta_record(&record)?;
                } else {
                    return Err(MetadataError::Corruption(
                        "unknown wal3 record format during live-reference scan".to_owned(),
                    ));
                }
            }
        }
        Ok(live)
    }

    async fn ensure_initialized(
        &self,
        manifest_and_witness: wal3::ManifestAndWitness,
    ) -> Result<LoadedState, MetadataError> {
        let mut loaded = match self
            .load_state_from_manifest(manifest_and_witness.clone())
            .await
        {
            Ok(loaded) => loaded,
            Err(error) => {
                if !matches!(
                    self.manifest_is_current(&self.reader().await?, &manifest_and_witness)
                        .await,
                    Ok(false)
                ) {
                    return Err(error);
                }
                self.load_state_at_manifest().await?
            }
        };
        for _ in 0..4 {
            if loaded.state.is_some() {
                return Ok(loaded);
            }
            let initial = StateData::empty()?;
            let encoded = encode_state(&initial)?;
            let checkpoint_bytes = encoded.len() as u64;
            let append = self
                .writer
                .append_with_options_outcome(
                    encoded,
                    Some(
                        wal3::AppendOptions::default()
                            .with_required_fragment_start(loaded.next_write_position),
                    ),
                )
                .await;
            match append {
                Ok(wal3::AppendOutcome::Committed(_)) => {
                    self.record_successful_append();
                    let manifest_and_witness = self
                        .writer
                        .manifest_and_witness()
                        .await
                        .map_err(wal3_error)?;
                    manifest_and_witness.manifest.scrub().map_err(|error| {
                        MetadataError::Corruption(format!(
                            "invalid wal3 manifest after initialization: {error}"
                        ))
                    })?;
                    return Ok(LoadedState {
                        state: Some(initial),
                        next_write_position: manifest_and_witness.manifest.next_write_timestamp(),
                        manifest_and_witness,
                        checkpoint_bytes,
                        base_position: Some(loaded.next_write_position),
                        tail_deltas: Vec::new(),
                    });
                }
                Ok(wal3::AppendOutcome::Contended(
                    wal3::AppendContention::Retryable | wal3::AppendContention::Indeterminate,
                )) => {
                    loaded = self.load_state_at_manifest().await?;
                }
                Ok(wal3::AppendOutcome::Contended(wal3::AppendContention::Durable)) => {
                    loaded = self.load_state_at_manifest().await?;
                    if loaded.state.is_none() {
                        return Err(wal3_error(wal3::Error::LogContentionDurable));
                    }
                }
                Err(error) => return Err(wal3_error(error)),
            }
        }
        let loaded = self.load_state_at_manifest().await?;
        if loaded.state.is_some() {
            Ok(loaded)
        } else {
            Err(MetadataError::Transient(
                "wal3 initialization remained contended".to_owned(),
            ))
        }
    }

    /// Load the complete state checkpoint at the tail of one stable manifest.
    ///
    /// Returning the manifest's next write position beside that checkpoint is
    /// essential: a later append must condition on the exact manifest from
    /// which its expected state was derived, rather than observing a newer
    /// tail in a separate read.
    #[tracing::instrument(name = "state.wal3.load", level = "debug", skip_all)]
    async fn load_state_at_manifest(&self) -> Result<LoadedState, MetadataError> {
        let reader = self.reader().await?;
        for _ in 0..MAX_COMMIT_CONTENTION_ATTEMPTS {
            self.read_metrics
                .manifest_load_requests
                .fetch_add(1, Ordering::Relaxed);
            let started = Instant::now();
            let result = reader.manifest_and_witness().await;
            self.read_metrics
                .manifest_load_nanos
                .fetch_add(elapsed_nanos(started), Ordering::Relaxed);
            let manifest = result
                .map_err(wal3_error)?
                .ok_or_else(|| MetadataError::Backend("wal3 log is uninitialized".into()))?;
            match self.load_state_from_manifest(manifest.clone()).await {
                Ok(state) => return Ok(state),
                Err(error) => {
                    // GC removes fragments only after changing the manifest.
                    // Retry an invalidated view; preserve failures of the
                    // current view so real corruption is never hidden.
                    if !matches!(
                        self.manifest_is_current(&reader, &manifest).await,
                        Ok(false)
                    ) {
                        return Err(error);
                    }
                }
            }
        }
        Err(MetadataError::Transient(
            "metadata manifest kept changing during load".into(),
        ))
    }

    async fn manifest_is_current(
        &self,
        reader: &Wal3Reader,
        manifest: &wal3::ManifestAndWitness,
    ) -> Result<bool, MetadataError> {
        if matches!(self.storage.as_ref(), chroma_storage::Storage::Local(_)) {
            // Local test storage has no HEAD implementation and is mutable in
            // place. Compare a fresh full manifest instead of a cached witness.
            Ok(reader
                .manifest_and_witness()
                .await
                .map_err(wal3_error)?
                .as_ref()
                == Some(manifest))
        } else {
            reader.verify(manifest).await.map_err(wal3_error)
        }
    }

    async fn pin_state_shards(
        &self,
        state: &StateData,
    ) -> Result<Option<super::DataPinLease>, MetadataError> {
        let resources = self
            .object_shards
            .referenced_paths(state.base_objects.as_ref())
            .into_iter()
            .map(super::PinResource::MetadataObject)
            .collect::<BTreeSet<_>>();
        if resources.is_empty() {
            return Ok(None);
        }
        super::DataPinLease::try_acquire(
            self.pin_store().await?,
            super::DataPin {
                scope: super::PinScope::Metadata,
                catalog: None,
                resources,
            },
        )
        .await?
        .ok_or_else(|| MetadataError::Transient("metadata shard is claimed for deletion".into()))
        .map(Some)
    }

    async fn load_state_from_manifest(
        &self,
        manifest_and_witness: wal3::ManifestAndWitness,
    ) -> Result<LoadedState, MetadataError> {
        let manifest = &manifest_and_witness.manifest;
        manifest.scrub().map_err(|error| {
            MetadataError::Corruption(format!("invalid wal3 manifest: {error}"))
        })?;
        let next_write_position = manifest.next_write_timestamp();
        if manifest.fragments.is_empty()
            && manifest.snapshots.is_empty()
            && manifest.initial_offset.is_none()
            && manifest.initial_seq_no.is_none()
        {
            return Ok(LoadedState {
                state: None,
                next_write_position,
                manifest_and_witness,
                checkpoint_bytes: 0,
                base_position: None,
                tail_deltas: Vec::new(),
            });
        }
        let Some(latest_offset) = next_write_position.offset().checked_sub(1) else {
            return Err(MetadataError::Corruption(
                "wal3 manifest has an invalid next write position".to_owned(),
            ));
        };
        let latest_position = wal3::LogPosition::from_offset(latest_offset);
        let reader = self.reader().await?;
        let tail = self
            .read_record_at(&reader, manifest, latest_position)
            .await?;
        let started = Instant::now();
        let (state, checkpoint_bytes, base_position, tail_deltas) = if tail
            .starts_with(STATE_MAGIC_V3)
            || tail.starts_with(STATE_MAGIC_V4)
        {
            (
                decode_state(&tail)?,
                tail.len() as u64,
                latest_position,
                Vec::new(),
            )
        } else if tail.starts_with(DELTA_MAGIC_V1) {
            let (base_position, deltas) = decode_delta_record(&tail)?;
            if base_position >= latest_position {
                return Err(MetadataError::Corruption(
                    "wal3 delta tail does not reference an older checkpoint".to_owned(),
                ));
            }
            let checkpoint = self
                .read_record_at(&reader, manifest, base_position)
                .await?;
            if !(checkpoint.starts_with(STATE_MAGIC_V3) || checkpoint.starts_with(STATE_MAGIC_V4)) {
                return Err(MetadataError::Corruption(
                    "wal3 delta base is not a full checkpoint".to_owned(),
                ));
            }
            let checkpoint_bytes = checkpoint.len() as u64;
            let mut state = decode_state(&checkpoint)?;
            let _read_pin = self.pin_state_shards(&state).await?;
            if _read_pin.is_some()
                && !self
                    .manifest_is_current(&reader, &manifest_and_witness)
                    .await?
            {
                return Err(MetadataError::Transient(
                    "checkpoint changed during shard admission".into(),
                ));
            }
            for delta in &deltas {
                apply_delta(&self.object_shards, &mut state, delta).await?;
            }
            (state, checkpoint_bytes, base_position, deltas)
        } else {
            return Err(MetadataError::Corruption(
                "unknown wal3 tail record format".to_owned(),
            ));
        };
        self.read_metrics
            .state_decode_nanos
            .fetch_add(elapsed_nanos(started), Ordering::Relaxed);
        Ok(LoadedState {
            state: Some(state),
            next_write_position,
            manifest_and_witness,
            checkpoint_bytes,
            base_position: Some(base_position),
            tail_deltas,
        })
    }

    async fn read_record_at(
        &self,
        reader: &Wal3Reader,
        manifest: &wal3::Manifest,
        position: wal3::LogPosition,
    ) -> Result<Vec<u8>, MetadataError> {
        let mut short_read = false;
        let fragments = reader
            .scan_with_cache(
                manifest,
                position,
                wal3::Limits {
                    max_records: Some(1),
                    ..wal3::Limits::UNLIMITED
                },
                &mut short_read,
            )
            .await
            .map_err(wal3_error)?;
        if short_read {
            return Err(MetadataError::Corruption(
                "wal3 exact-record scan was unexpectedly truncated".to_owned(),
            ));
        }
        let mut result = None;
        for fragment in fragments {
            if let Some((_, bytes)) = self
                .read_fragment_entries(reader, &fragment)
                .await?
                .into_iter()
                .find(|(entry_position, _)| *entry_position == position)
            {
                if result.is_some() {
                    return Err(MetadataError::Corruption(
                        "wal3 record position appears in multiple fragments".to_owned(),
                    ));
                }
                result = Some(bytes);
            }
        }
        result.ok_or_else(|| {
            MetadataError::Corruption(format!(
                "wal3 manifest has no record at position {}",
                position.offset()
            ))
        })
    }

    #[tracing::instrument(name = "state.wal3.read_fragment", level = "debug", skip_all)]
    async fn read_fragment_entries(
        &self,
        reader: &Wal3Reader,
        fragment: &wal3::Fragment,
    ) -> Result<Vec<(wal3::LogPosition, Vec<u8>)>, MetadataError> {
        self.read_metrics
            .fragment_get_requests
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let fragment_result = reader.read_bytes(fragment).await;
        self.read_metrics
            .fragment_get_nanos
            .fetch_add(elapsed_nanos(started), Ordering::Relaxed);
        let fragment_bytes = fragment_result.map_err(wal3_error)?;
        self.read_metrics
            .fragment_get_bytes
            .fetch_add(fragment_bytes.len() as u64, Ordering::Relaxed);

        let started = Instant::now();
        let parse_result = reader
            .parse_parquet(fragment_bytes.as_slice(), fragment.start)
            .await;
        self.read_metrics
            .parquet_parse_nanos
            .fetch_add(elapsed_nanos(started), Ordering::Relaxed);
        let (setsum, entries, num_bytes, _) = parse_result.map_err(wal3_error)?;
        self.read_metrics
            .fragment_records
            .fetch_add(entries.len() as u64, Ordering::Relaxed);
        let record_bytes = entries.iter().fold(0u64, |total, (_, body)| {
            total.saturating_add(body.len() as u64)
        });
        self.read_metrics
            .fragment_record_bytes
            .fetch_add(record_bytes, Ordering::Relaxed);
        if setsum != fragment.setsum {
            return Err(MetadataError::Corruption(format!(
                "wal3 fragment {} has a mismatched setsum",
                fragment.path
            )));
        }
        if num_bytes != fragment.num_bytes {
            return Err(MetadataError::Corruption(format!(
                "wal3 fragment {} has a mismatched byte length",
                fragment.path
            )));
        }
        if entries.is_empty() || fragment.limit != fragment.start + entries.len() {
            return Err(MetadataError::Corruption(format!(
                "wal3 fragment {} has invalid record bounds",
                fragment.path
            )));
        }
        if entries
            .iter()
            .enumerate()
            .any(|(index, (position, _))| *position != fragment.start + index)
        {
            return Err(MetadataError::Corruption(format!(
                "wal3 fragment {} has non-contiguous record positions",
                fragment.path
            )));
        }
        Ok(entries)
    }

    fn cache_loaded(&self, loaded: LoadedState) -> Result<Arc<StateData>, MetadataError> {
        let base_position = loaded.base_position()?;
        let tail_delta_count = loaded.tail_deltas.len() as u64;
        let state = Arc::new(loaded.state.ok_or_else(|| {
            MetadataError::Corruption("initialized wal3 log has no state record".to_owned())
        })?);
        *self.checkpoint.lock().unwrap() = Some(CachedCheckpoint {
            manifest_and_witness: loaded.manifest_and_witness,
            state: state.clone(),
            base_position,
            tail_deltas: loaded.tail_deltas,
        });
        self.read_metrics
            .checkpoint_bytes
            .store(loaded.checkpoint_bytes, Ordering::Relaxed);
        self.read_metrics.checkpoint_objects.store(
            state
                .base_objects
                .object_count
                .saturating_add(state.objects.len() as u64),
            Ordering::Relaxed,
        );
        self.read_metrics
            .checkpoint_roots
            .store(state.root_count, Ordering::Relaxed);
        self.read_metrics.checkpoint_validated.store(
            state
                .base_objects
                .validated_count
                .saturating_add(state.validated.len() as u64),
            Ordering::Relaxed,
        );
        self.read_metrics
            .tail_deltas
            .store(tail_delta_count, Ordering::Relaxed);
        Ok(state)
    }

    fn record_successful_append(&self) {
        // The single-region S3 wal3 profile publishes one immutable Parquet
        // fragment, then advances MANIFEST with one conditional PUT.
        self.read_metrics
            .fragment_put_requests
            .fetch_add(1, Ordering::Relaxed);
        self.read_metrics
            .manifest_put_requests
            .fetch_add(1, Ordering::Relaxed);
    }
}

fn boxed_wal_future<'a, T>(
    future: impl Future<Output = T> + Send + 'a,
) -> Pin<Box<dyn Future<Output = T> + Send + 'a>> {
    Box::pin(future)
}

fn elapsed_nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

struct LoadedState {
    state: Option<StateData>,
    next_write_position: wal3::LogPosition,
    manifest_and_witness: wal3::ManifestAndWitness,
    checkpoint_bytes: u64,
    base_position: Option<wal3::LogPosition>,
    tail_deltas: Vec<StateDelta>,
}

#[derive(Clone)]
struct CachedCheckpoint {
    manifest_and_witness: wal3::ManifestAndWitness,
    state: Arc<StateData>,
    base_position: wal3::LogPosition,
    tail_deltas: Vec<StateDelta>,
}

impl LoadedState {
    fn latest_position(&self) -> Result<wal3::LogPosition, MetadataError> {
        self.next_write_position
            .offset()
            .checked_sub(1)
            .map(wal3::LogPosition::from_offset)
            .ok_or_else(|| {
                MetadataError::Corruption("wal3 has an invalid next write position".to_owned())
            })
    }

    fn base_position(&self) -> Result<wal3::LogPosition, MetadataError> {
        self.base_position.ok_or_else(|| {
            MetadataError::Corruption("initialized wal3 log has no checkpoint position".to_owned())
        })
    }

    /// Whether `other` was loaded from the same log records: the same next
    /// write position, checkpoint and tail. A record built on one appends
    /// validly after the other. Equal revisions do not imply this, because a
    /// WAL collection appends a checkpoint of the current revision.
    fn describes_same_log(&self, other: &Self) -> bool {
        self.next_write_position == other.next_write_position
            && self.base_position == other.base_position
            && self.tail_deltas == other.tail_deltas
    }
}

/// A record to append after one loaded log view, and what to cache once it
/// lands there.
struct PlannedAppend {
    record: Vec<u8>,
    /// The state after the append; a checkpoint record encodes exactly this.
    state: StateData,
    /// The view's tail plus this commit's delta, if it has one.
    tail_deltas: Vec<StateDelta>,
    /// The checkpoint a delta record builds on; `None` for a checkpoint
    /// record, which becomes the base where it lands.
    delta_base: Option<wal3::LogPosition>,
    checkpoint_bytes: u64,
}

#[derive(Clone)]
struct StateData {
    revision: RepositoryRevision,
    generation: u64,
    births: BTreeMap<ObjectKey, u64>,
    base_objects: Arc<StateShardMap>,
    // At most the bounded cumulative WAL tail. Full checkpoints fold this
    // overlay into immutable object shards before publication.
    objects: BTreeMap<ObjectKey, ObjectRecord>,
    // Exact changes since the base checkpoint; `None` is a root tombstone.
    roots: BTreeMap<RootName, Option<ObjectKey>>,
    root_count: u64,
    // Validation additions since the base checkpoint. This can include keys
    // already in `base_objects` as well as newly inserted overlay objects.
    validated: BTreeSet<ObjectKey>,
    payload_catalog: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct StateDelta {
    expected: RepositoryRevision,
    revision: RepositoryRevision,
    objects: Vec<ObjectRecord>,
    roots: Vec<RootChange>,
    validated: Vec<ObjectKey>,
    payload_catalog: Option<Vec<u8>>,
}

impl StateDelta {
    fn from_mutation(expected: RepositoryRevision, mutation: &MetadataMutation) -> Self {
        let mut objects = mutation
            .objects
            .iter()
            .map(|object| object.record().clone())
            .collect::<Vec<_>>();
        objects.sort_by(|left, right| left.key().cmp(right.key()));
        // MetadataMutation permits idempotent reinsertion. Keep its durable delta
        // canonical when the same verified record was supplied more than once.
        // Conflicting records remain adjacent and apply_mutation rejects them.
        objects.dedup();
        let mut validated = mutation.validated_closures.clone();
        validated.sort();
        validated.dedup();
        Self {
            expected,
            // Filled from apply_mutation's generated commit result.
            revision: expected,
            objects,
            roots: mutation.roots.values().cloned().collect(),
            validated,
            payload_catalog: mutation.payload_catalog.clone(),
        }
    }
}

impl StateData {
    fn object_birth(&self, key: &ObjectKey) -> Result<u64, MetadataError> {
        self.births
            .get(key)
            .copied()
            .ok_or_else(|| MetadataError::Corruption(format!("missing birth generation for {key}")))
    }

    fn empty() -> Result<Self, MetadataError> {
        Ok(Self {
            revision: fresh_revision(None)?,
            generation: 0,
            births: BTreeMap::new(),
            base_objects: Arc::new(StateShardMap::default()),
            objects: BTreeMap::new(),
            roots: BTreeMap::new(),
            root_count: 0,
            validated: BTreeSet::new(),
            payload_catalog: crate::ChunkedBlobStore::empty_state_catalog().map_err(|error| {
                MetadataError::Backend(format!("initialize empty payload catalog: {error}"))
            })?,
        })
    }
}

/// Folding the overlay into shards changes representation only. A reader of
/// the checkpoint rebuilds object, validation and root totals from the shard
/// map alone, so the fold must carry every object, mark and root.
fn validate_compaction(
    point: &'static str,
    before: &StateData,
    after: &StateData,
) -> Result<(), Violation> {
    invariant::ensure(
        point,
        after.revision == before.revision && after.generation == before.generation,
        || {
            format!(
                "compaction moved revision {} generation {} to revision {} generation {}",
                before.revision, before.generation, after.revision, after.generation
            )
        },
    )?;
    invariant::ensure(
        point,
        after.payload_catalog == before.payload_catalog,
        || "compaction changed the payload catalog".into(),
    )?;
    invariant::ensure(
        point,
        after.objects.is_empty()
            && after.births.is_empty()
            && after.validated.is_empty()
            && after.roots.is_empty(),
        || "compaction left an unmerged overlay".into(),
    )?;
    // Overlay objects are keys the base lacks and overlay marks are base keys
    // not yet validated, so neither overlaps its base count.
    let objects = before
        .base_objects
        .object_count
        .saturating_add(before.objects.len() as u64);
    invariant::ensure(point, after.base_objects.object_count == objects, || {
        format!(
            "compaction holds {} objects, the state {objects}",
            after.base_objects.object_count
        )
    })?;
    let validated = before
        .base_objects
        .validated_count
        .saturating_add(before.validated.len() as u64);
    invariant::ensure(
        point,
        after.base_objects.validated_count == validated,
        || {
            format!(
                "compaction holds {} validated objects, the state {validated}",
                after.base_objects.validated_count
            )
        },
    )?;
    invariant::ensure(
        point,
        after.base_objects.root_count == before.root_count && after.root_count == before.root_count,
        || {
            format!(
                "compaction holds {} roots, the state {}",
                after.base_objects.root_count, before.root_count
            )
        },
    )
}

/// One append a commit is about to make, and the log view it then caches.
struct AppendPlan<'a> {
    expected: RepositoryRevision,
    result: &'a CommitResult,
    /// The state cached once the append lands.
    state: &'a StateData,
    record: &'a [u8],
    /// The position the append requires, which the record then occupies.
    appended: wal3::LogPosition,
    /// The checkpoint of the log the append follows.
    base: Option<wal3::LogPosition>,
    /// The checkpoint and tail cached for the next commit.
    next_base: wal3::LogPosition,
    next_tail: &'a [StateDelta],
}

/// The cached view must be what reloading the log yields once the record
/// lands, because the next commit builds its delta on it without reading the
/// log. A delta must also build on the checkpoint of the log it follows:
/// collection deletes every record older than the newest checkpoint.
fn validate_append(plan: &AppendPlan<'_>) -> Result<(), Violation> {
    const POINT: &str = "wal3.commit";
    let state = plan.state;
    invariant::ensure(POINT, state.revision == plan.result.revision, || {
        format!(
            "caches revision {} but reports {}",
            state.revision, plan.result.revision
        )
    })?;
    let Some(base) = plan.base else {
        return Err(Violation::new(POINT, "the log has no checkpoint"));
    };
    if plan.record.starts_with(STATE_MAGIC_V4) {
        invariant::ensure(
            POINT,
            plan.next_base == plan.appended && plan.next_tail.is_empty(),
            || {
                format!(
                    "a checkpoint at {} caches base {} and {} tail deltas",
                    plan.appended.offset(),
                    plan.next_base.offset(),
                    plan.next_tail.len()
                )
            },
        )?;
        let decoded = decode_state(plan.record).map_err(|error| {
            Violation::new(POINT, format!("checkpoint does not decode: {error}"))
        })?;
        invariant::ensure(
            POINT,
            decoded.revision == state.revision
                && decoded.generation == state.generation
                && decoded.base_objects == state.base_objects
                && decoded.root_count == state.root_count
                && decoded.payload_catalog == state.payload_catalog
                && decoded.objects == state.objects
                && decoded.births == state.births
                && decoded.validated == state.validated
                && decoded.roots == state.roots,
            || "checkpoint encodes a different state than the one cached".into(),
        )
    } else if plan.record.starts_with(DELTA_MAGIC_V1) {
        let (record_base, deltas) = decode_delta_record(plan.record).map_err(|error| {
            Violation::new(POINT, format!("delta record does not decode: {error}"))
        })?;
        invariant::ensure(
            POINT,
            record_base == base && plan.next_base == base && base < plan.appended,
            || {
                format!(
                    "a delta at {} builds on {} and caches base {}, but follows checkpoint {}",
                    plan.appended.offset(),
                    record_base.offset(),
                    plan.next_base.offset(),
                    base.offset()
                )
            },
        )?;
        invariant::ensure(POINT, deltas == plan.next_tail, || {
            "delta record differs from the cached tail".into()
        })?;
        invariant::ensure(
            POINT,
            deltas.len() <= MAX_TAIL_DELTAS
                && deltas.iter().all(|delta| delta.revision != delta.expected)
                && deltas
                    .windows(2)
                    .all(|pair| pair[0].revision == pair[1].expected),
            || format!("{} tail deltas do not chain", deltas.len()),
        )?;
        let Some(last) = deltas.last() else {
            return Err(Violation::new(POINT, "delta record holds no delta"));
        };
        invariant::ensure(
            POINT,
            last.expected == plan.expected && last.revision == plan.result.revision,
            || {
                format!(
                    "last delta moves {} to {}, the commit {} to {}",
                    last.expected, last.revision, plan.expected, plan.result.revision
                )
            },
        )
    } else {
        Err(Violation::new(
            POINT,
            "record is neither a checkpoint nor a delta",
        ))
    }
}

#[derive(Clone)]
struct Wal3Snapshot {
    state: Arc<StateData>,
    shards: ObjectShardStorage,
}

#[async_trait]
impl MetadataSnapshot for Wal3Snapshot {
    fn revision(&self) -> RepositoryRevision {
        self.state.revision
    }

    fn generation(&self) -> Result<u64, MetadataError> {
        Ok(self.state.generation)
    }

    fn objects_created_through(
        &self,
        generation: u64,
    ) -> BoxStream<'static, Result<ObjectRecord, MetadataError>> {
        let state = self.state.clone();
        let shards = self.shards.clone();
        Box::pin(async_stream::try_stream! {
            for record in state.objects.values() {
                if state.object_birth(record.key())? <= generation {
                    yield record.clone();
                }
            }
            for reference in &state.base_objects.objects {
                for (record, _, birth) in shards.read(reference).await? {
                    if birth > state.generation {
                        Err(MetadataError::Corruption(format!(
                            "object {} was born after its metadata snapshot", record.key()
                        )))?;
                    }
                    if birth <= generation && !state.objects.contains_key(record.key()) {
                        yield record;
                    }
                }
            }
        })
    }

    fn payload_catalog(&self) -> Option<&[u8]> {
        Some(self.state.payload_catalog.as_slice())
    }

    fn retention_resources(&self) -> BTreeSet<super::PinResource> {
        self.shards
            .referenced_paths(self.state.base_objects.as_ref())
            .into_iter()
            .map(super::PinResource::MetadataObject)
            .collect()
    }

    async fn object(&self, key: &ObjectKey) -> Result<Option<ObjectRecord>, MetadataError> {
        if let Some(record) = self.state.objects.get(key) {
            return Ok(Some(record.clone()));
        }
        Ok(self
            .shards
            .lookup(self.state.base_objects.as_ref(), key)
            .await?
            .map(|(record, _, _)| record))
    }

    async fn object_batch(
        &self,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<ObjectRecord>>, MetadataError> {
        let mut records = Vec::with_capacity(keys.len());
        for key in keys {
            records.push(self.object(key).await?);
        }
        Ok(records)
    }

    async fn validated_closures(&self, keys: &[ObjectKey]) -> Result<Vec<bool>, MetadataError> {
        let mut validated = Vec::with_capacity(keys.len());
        for key in keys {
            if self.state.validated.contains(key) {
                validated.push(true);
            } else if self.state.objects.contains_key(key) {
                validated.push(false);
            } else {
                validated.push(
                    self.shards
                        .lookup(self.state.base_objects.as_ref(), key)
                        .await?
                        .is_some_and(|(_, validated, _)| validated),
                );
            }
        }
        Ok(validated)
    }

    async fn root(&self, name: &RootName) -> Result<Option<ObjectKey>, MetadataError> {
        if let Some(target) = self.state.roots.get(name) {
            return Ok(target.clone());
        }
        self.shards
            .lookup_root(self.state.base_objects.as_ref(), name)
            .await
    }

    fn objects(&self) -> BoxStream<'static, Result<ObjectRecord, MetadataError>> {
        let state = self.state.clone();
        let shards = self.shards.clone();
        Box::pin(async_stream::try_stream! {
            let mut overlay = state.objects.values().peekable();
            for reference in &state.base_objects.objects {
                for (record, _, _) in shards.read(reference).await? {
                    while overlay.peek().is_some_and(|candidate| candidate.key() < record.key()) {
                        yield overlay.next().expect("peeked overlay record").clone();
                    }
                    if overlay.peek().is_some_and(|candidate| candidate.key() == record.key()) {
                        yield overlay.next().expect("peeked overlay record").clone();
                    } else {
                        yield record;
                    }
                }
            }
            for record in overlay {
                yield record.clone();
            }
        })
    }

    fn roots(&self) -> BoxStream<'static, Result<RootRecord, MetadataError>> {
        let state = self.state.clone();
        let shards = self.shards.clone();
        Box::pin(async_stream::try_stream! {
            let mut overlay = state.roots.iter().peekable();
            for reference in &state.base_objects.roots {
                for root in shards.read_roots(reference).await? {
                    while overlay.peek().is_some_and(|(name, _)| *name < root.name()) {
                        let (name, target) = overlay.next().expect("peeked root overlay");
                        if let Some(target) = target {
                            yield RootRecord::new(name.clone(), target.clone());
                        }
                    }
                    if overlay.peek().is_some_and(|(name, _)| *name == root.name()) {
                        let (name, target) = overlay.next().expect("peeked root overlay");
                        if let Some(target) = target {
                            yield RootRecord::new(name.clone(), target.clone());
                        }
                    } else {
                        yield root;
                    }
                }
            }
            for (name, target) in overlay {
                if let Some(target) = target {
                    yield RootRecord::new(name.clone(), target.clone());
                }
            }
        })
    }
}

#[async_trait]
impl MetadataStore for Wal3MetadataStore {
    async fn pin_store(&self) -> Result<Arc<dyn super::PinStore>, MetadataError> {
        Ok(super::pins::chroma_pin_store(
            self.storage.clone(),
            format!("{}/online-pins-v1", self.prefix),
        ))
    }
    async fn try_collection_lease(&self) -> Result<Option<super::RepositoryLease>, MetadataError> {
        if self.is_coordination {
            return Ok(Some(super::RepositoryLease::default()));
        }
        self.repository_coordination().await?.acquire().await
    }

    fn coordinates_payload_catalog(&self) -> bool {
        true
    }

    #[tracing::instrument(
        name = "state.snapshot",
        level = "debug",
        skip_all,
        fields(backend = "wal3")
    )]
    async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
        let _refresh_guard = self.snapshot_refresh_lock.lock().await;
        let cached = self.checkpoint.lock().unwrap().clone();
        if let Some(cached) = cached {
            // Local test storage is mutable in place, so an unchanged manifest
            // cannot prove its fragment is unchanged. Reload it to preserve
            // snapshot's corruption detection. Object-store fragments are
            // immutable after publication and can use the manifest witness.
            if matches!(self.storage.as_ref(), chroma_storage::Storage::Local(_)) {
                self.read_metrics
                    .checkpoint_cache_misses
                    .fetch_add(1, Ordering::Relaxed);
                let state = self.cache_loaded(self.load_state_at_manifest().await?)?;
                return Ok(Arc::new(Wal3Snapshot {
                    state,
                    shards: self.object_shards.clone(),
                }));
            }
            let reader = self.reader().await?;
            // Verify the cached witness without loading the manifest body.
            // A changed witness requires a subsequent strong manifest load.
            self.read_metrics
                .manifest_refresh_requests
                .fetch_add(1, Ordering::Relaxed);
            let started = Instant::now();
            let verified = reader.verify(&cached.manifest_and_witness).await;
            self.read_metrics
                .manifest_refresh_nanos
                .fetch_add(elapsed_nanos(started), Ordering::Relaxed);
            match verified {
                Ok(true) => {
                    self.read_metrics
                        .checkpoint_cache_hits
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(revision = %cached.state.revision, cache = "hit", "WAL3 snapshot opened");
                    return Ok(Arc::new(Wal3Snapshot {
                        state: cached.state,
                        shards: self.object_shards.clone(),
                    }));
                }
                Ok(false) => {
                    self.read_metrics
                        .checkpoint_cache_misses
                        .fetch_add(1, Ordering::Relaxed);
                    let state = self.cache_loaded(self.load_state_at_manifest().await?)?;
                    tracing::debug!(revision = %state.revision, cache = "refresh", "WAL3 snapshot opened");
                    return Ok(Arc::new(Wal3Snapshot {
                        state,
                        shards: self.object_shards.clone(),
                    }));
                }
                Err(_) => {}
            }
        }
        self.read_metrics
            .checkpoint_cache_misses
            .fetch_add(1, Ordering::Relaxed);
        let state = self.cache_loaded(self.load_state_at_manifest().await?)?;
        Ok(Arc::new(Wal3Snapshot {
            state,
            shards: self.object_shards.clone(),
        }))
    }

    #[tracing::instrument(
        name = "state.commit",
        skip_all,
        fields(
            backend = "wal3",
            expected_revision = %expected,
            objects = mutation.objects.len(),
            root_changes = mutation.roots.len(),
            collection = mutation.retained_objects.is_some()
        )
    )]
    async fn commit(
        &self,
        expected: &RepositoryRevision,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        if mutation.has_metadata() {
            return Err(MetadataError::UnsupportedMetadata);
        }
        let guard = self.commit_lock.clone().lock_owned().await;
        let mut store = self.clone();
        store.object_shards = self.object_shards.for_write_operation();
        let expected = *expected;
        // Retain the commit mutex in the task even if its caller is cancelled.
        // Repository hold cleanup waits for this task before releasing admission.
        super::run_lease_task("WAL3 commit", |send| async move {
            let _guard = guard;
            let result = store.commit_unlocked(&expected, mutation).await;
            drop(send.send(result));
            Ok(())
        })
        .await?
    }
}

impl Wal3MetadataStore {
    async fn commit_unlocked(
        &self,
        expected: &RepositoryRevision,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        let retained = mutation.retained_objects.clone();
        let cached = if matches!(self.storage.as_ref(), chroma_storage::Storage::Local(_)) {
            None
        } else {
            self.checkpoint
                .lock()
                .unwrap()
                .clone()
                .filter(|cached| cached.state.revision == *expected)
        };
        let loaded = match cached {
            Some(cached) => LoadedState {
                state: Some(cached.state.as_ref().clone()),
                next_write_position: cached.manifest_and_witness.manifest.next_write_timestamp(),
                manifest_and_witness: cached.manifest_and_witness,
                checkpoint_bytes: self.read_metrics.checkpoint_bytes.load(Ordering::Relaxed),
                base_position: Some(cached.base_position),
                tail_deltas: cached.tail_deltas,
            },
            None => self.load_state_at_manifest().await?,
        };
        self.commit_loaded(*expected, mutation, retained, loaded)
            .await
    }

    #[tracing::instrument(
        name = "state.wal3.commit_loaded",
        skip_all,
        fields(expected_revision = %expected, collection = retained.is_some())
    )]
    async fn commit_loaded(
        &self,
        expected: RepositoryRevision,
        mutation: MetadataMutation,
        retained: Option<Arc<dyn RetainedObjects>>,
        loaded: LoadedState,
    ) -> Result<CommitResult, MetadataError> {
        let (mut view, read_pin) = self.admit_loaded(expected, loaded).await?;
        // Every view a record is built on keeps its shards pinned until the
        // commit settles, because a checkpoint record may reference them.
        let mut read_pins = Vec::from_iter(read_pin);
        let current = view.state.take().ok_or_else(|| {
            MetadataError::Corruption("initialized wal3 log has no state record".to_owned())
        })?;
        let collection = retained.is_some();
        // Both sides of the transition, kept only for checking it.
        let previous_generation = current.generation;
        let previous_objects = current
            .base_objects
            .object_count
            .saturating_add(current.objects.len() as u64);
        let previous_catalog = invariant::ENABLED.then(|| current.payload_catalog.clone());
        let declared_catalog = if invariant::ENABLED {
            mutation.payload_catalog.clone()
        } else {
            None
        };
        let declared_retained = retained.as_ref().map(|source| source.len() as u64);
        // Collection materializes replacement shards while applying its exact
        // retained set, so fence orphan GC before the first such upload.
        let mut checkpoint_lease = if collection {
            Some(self.object_shards.acquire_checkpoint_barrier().await?)
        } else {
            None
        };
        let outcome = async {
            let mut delta = (!collection).then(|| StateDelta::from_mutation(expected, &mutation));
            let (next, result, added) =
                apply_mutation(&self.object_shards, current, mutation, retained).await?;
            // The transition from a view's state to the state built on it.
            // A rebuild replays the same delta onto a logically identical
            // state, so the additions observed by `apply_mutation` still
            // describe it.
            let check_transition =
                |previous_generation: u64, previous_catalog: Option<&[u8]>, next: &StateData| {
                    let objects = match declared_retained {
                        Some(retained) => ObjectChange::Collected(CollectedObjects {
                            retained,
                            before: previous_objects,
                            after: next
                                .base_objects
                                .object_count
                                .saturating_add(next.objects.len() as u64),
                            // Root targets resolve only through shards this commit
                            // would have to read back; apply_mutation already
                            // refused a retained set that omits one.
                            missing_root_target: None,
                        }),
                        // A normal mutation leaves the base shards untouched, so a
                        // key missing from the overlay still resolves to its old
                        // record.
                        None => ObjectChange::Added(
                            added
                                .iter()
                                .map(|(key, before)| AddedObject {
                                    key,
                                    before: before.as_ref(),
                                    after: next.objects.get(key).or(before.as_ref()),
                                })
                                .collect(),
                        ),
                    };
                    validate_commit_transition(
                        "wal3.commit",
                        &CommitState {
                            revision: expected,
                            generation: previous_generation,
                            payload_catalog: previous_catalog,
                        },
                        &CommitState {
                            revision: next.revision,
                            generation: next.generation,
                            payload_catalog: Some(&next.payload_catalog),
                        },
                        &CommitChange {
                            payload_catalog: declared_catalog.as_deref(),
                            objects,
                            result: &result,
                        },
                    )
                };
            invariant::check(|| {
                check_transition(previous_generation, previous_catalog.as_deref(), &next)
            })?;
            if let Some(delta) = &mut delta {
                delta.revision = result.revision;
            }
            let mut plan = self
                .plan_append(&view, next, delta, &mut checkpoint_lease)
                .await?;
            #[cfg(test)]
            if plan.delta_base.is_none()
                && let Some(pause) = &self.checkpoint_pause
            {
                pause.reached.notify_one();
                pause.resume.notified().await;
            }
            for attempt in 0..MAX_COMMIT_CONTENTION_ATTEMPTS {
                // Check the plan against the view it was built on and this
                // attempt follows, including one a retry rebuilt: the record
                // and exactly what the success arm below caches.
                invariant::check(|| {
                    validate_append(&AppendPlan {
                        expected,
                        result: &result,
                        state: &plan.state,
                        record: &plan.record,
                        appended: view.next_write_position,
                        base: view.base_position,
                        next_base: plan.delta_base.unwrap_or(view.next_write_position),
                        next_tail: if plan.delta_base.is_some() {
                            &plan.tail_deltas
                        } else {
                            &[]
                        },
                    })
                })?;
                match self
                    .writer
                    .append_with_options_outcome(
                        plan.record.clone(),
                        Some(
                            wal3::AppendOptions::default()
                                .with_required_fragment_start(view.next_write_position),
                        ),
                    )
                    .await
                {
                    Ok(wal3::AppendOutcome::Committed(_)) => {
                        tracing::debug!(attempt = attempt + 1, "WAL3 append committed");
                        self.record_successful_append();
                        let manifest_and_witness = self
                            .writer
                            .manifest_and_witness()
                            .await
                            .map_err(wal3_error)?;
                        manifest_and_witness.manifest.scrub().map_err(|error| {
                            MetadataError::Corruption(format!(
                                "invalid wal3 manifest after commit: {error}"
                            ))
                        })?;
                        // The append required the position `view` ends at,
                        // so nothing was appended since `view` was loaded and
                        // the record occupies exactly that position.
                        let (base_position, tail_deltas) = match plan.delta_base {
                            Some(base_position) => (base_position, plan.tail_deltas),
                            None => (view.next_write_position, Vec::new()),
                        };
                        self.cache_loaded(LoadedState {
                            state: Some(plan.state),
                            next_write_position: manifest_and_witness
                                .manifest
                                .next_write_timestamp(),
                            manifest_and_witness,
                            checkpoint_bytes: plan.checkpoint_bytes,
                            base_position: Some(base_position),
                            tail_deltas,
                        })?;
                        return Ok(result);
                    }
                    Ok(wal3::AppendOutcome::Contended(wal3::AppendContention::Retryable)) => {
                        tracing::debug!(
                            attempt = attempt + 1,
                            contention = "retryable",
                            "WAL3 append contended"
                        );
                        let reloaded = self.load_state_at_manifest().await?;
                        let actual_revision = reloaded
                            .state
                            .as_ref()
                            .ok_or_else(|| {
                                MetadataError::Corruption(
                                    "wal3 contention left no state record".to_owned(),
                                )
                            })?
                            .revision;
                        if actual_revision == result.revision {
                            self.cache_loaded(reloaded)?;
                            self.record_successful_append();
                            return Ok(result);
                        }
                        if actual_revision != expected {
                            return Err(MetadataError::StaleRevision {
                                expected,
                                actual: actual_revision,
                            });
                        }
                        if let Some(lease) = &checkpoint_lease
                            && !self.object_shards.owns_barrier(lease).await?
                        {
                            return Err(MetadataError::Transient(
                                "logical checkpoint publication was fenced by maintenance"
                                    .to_owned(),
                            ));
                        }
                        if !view.describes_same_log(&reloaded) {
                            // An append kept the revision but moved the log,
                            // typically a WAL collection's checkpoint. The
                            // record and the view it would cache belong to
                            // the old log: a delta names a checkpoint that
                            // collection deletes, and a checkpoint would be
                            // cached at a position it no longer lands at.
                            let (admitted, read_pin) =
                                self.admit_loaded(expected, reloaded).await?;
                            read_pins.extend(read_pin);
                            view = admitted;
                            let delta = plan.tail_deltas.pop();
                            let next = match &delta {
                                // Replay the delta exactly as loading this
                                // log will, so the cache matches a reload.
                                Some(delta) => {
                                    let mut state = view.state.take().ok_or_else(|| {
                                        MetadataError::Corruption(
                                            "initialized wal3 log has no state record".to_owned(),
                                        )
                                    })?;
                                    let previous_generation = state.generation;
                                    let previous_catalog =
                                        invariant::ENABLED.then(|| state.payload_catalog.clone());
                                    apply_delta(&self.object_shards, &mut state, delta).await?;
                                    invariant::check(|| {
                                        check_transition(
                                            previous_generation,
                                            previous_catalog.as_deref(),
                                            &state,
                                        )
                                    })?;
                                    state
                                }
                                // A collection's checkpoint is its whole state
                                // and names no earlier record.
                                None => plan.state,
                            };
                            plan = self
                                .plan_append(&view, next, delta, &mut checkpoint_lease)
                                .await?;
                        }
                        let delay_ms = 1_u64 << attempt.min(6);
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                    Ok(wal3::AppendOutcome::Contended(wal3::AppendContention::Indeterminate)) => {
                        tracing::warn!(
                            attempt = attempt + 1,
                            contention = "indeterminate",
                            "reconciling WAL3 append contention"
                        );
                        return reconcile_contention(
                            self,
                            expected,
                            result,
                            wal3::AppendContention::Indeterminate,
                            checkpoint_lease.as_ref(),
                        )
                        .await;
                    }
                    Ok(wal3::AppendOutcome::Contended(wal3::AppendContention::Durable)) => {
                        tracing::debug!(
                            attempt = attempt + 1,
                            contention = "durable",
                            "reconciling WAL3 append contention"
                        );
                        return reconcile_contention(
                            self,
                            expected,
                            result,
                            wal3::AppendContention::Durable,
                            checkpoint_lease.as_ref(),
                        )
                        .await;
                    }
                    Err(error) => return Err(wal3_error(error)),
                }
            }
            Err(MetadataError::Transient(format!(
                "wal3 commit remained contended after {MAX_COMMIT_CONTENTION_ATTEMPTS} attempts"
            )))
        }
        .await;
        if let Some(lease) = &checkpoint_lease {
            // A durable commit must not be reported as failed merely because
            // releasing its maintenance barrier failed. The next GC can
            // forcibly fence and clear an abandoned lease.
            let _ = self.object_shards.release_barrier(lease).await;
        }
        outcome
    }

    /// Pin the shards `loaded` references and confirm its manifest is still
    /// current, reloading until both hold. A cached or reloaded view is only a
    /// hint until then: collection deletes the shards of a superseded
    /// checkpoint. A view referencing no shards has nothing to protect and is
    /// admitted as is; an append built on it still requires the exact position
    /// the view ends at, and is rebuilt if the log moved.
    async fn admit_loaded(
        &self,
        expected: RepositoryRevision,
        mut loaded: LoadedState,
    ) -> Result<(LoadedState, Option<super::DataPinLease>), MetadataError> {
        for _ in 0..MAX_COMMIT_CONTENTION_ATTEMPTS {
            let state = loaded.state.as_ref().ok_or_else(|| {
                MetadataError::Corruption("initialized wal3 log has no state record".into())
            })?;
            if state.revision != expected {
                return Err(MetadataError::StaleRevision {
                    expected,
                    actual: state.revision,
                });
            }
            let pin = match self.pin_state_shards(state).await {
                Ok(pin) => pin,
                Err(error) => {
                    if matches!(
                        self.manifest_is_current(
                            &self.reader().await?,
                            &loaded.manifest_and_witness
                        )
                        .await,
                        Ok(false)
                    ) {
                        loaded = self.load_state_at_manifest().await?;
                        continue;
                    }
                    return Err(error);
                }
            };
            if pin.is_none()
                || self
                    .manifest_is_current(&self.reader().await?, &loaded.manifest_and_witness)
                    .await?
            {
                return Ok((loaded, pin));
            }
            drop(pin);
            loaded = self.load_state_at_manifest().await?;
        }
        Err(MetadataError::Transient(
            "metadata changed during commit shard admission".into(),
        ))
    }

    /// Build the record that appends `next` after `view`. `delta` is `None`
    /// for a collection, whose record is always a checkpoint.
    async fn plan_append(
        &self,
        view: &LoadedState,
        next: StateData,
        delta: Option<StateDelta>,
        checkpoint_lease: &mut Option<super::wal3_shard::ShardBarrierLease>,
    ) -> Result<PlannedAppend, MetadataError> {
        // Collection is an exact replacement and must not retain or republish
        // pre-collection additions from the old cumulative tail.
        let mut tail_deltas = if delta.is_some() {
            view.tail_deltas.clone()
        } else {
            Vec::new()
        };
        tail_deltas.extend(delta);
        let base_position = view.base_position()?;
        let delta_record = encode_delta_record(base_position, &tail_deltas);
        let install_checkpoint = tail_deltas.len() > MAX_TAIL_DELTAS
            || delta_record.len() > MAX_DELTA_RECORD_BYTES
            || tail_deltas.is_empty();
        if !install_checkpoint {
            return Ok(PlannedAppend {
                record: delta_record,
                state: next,
                tail_deltas,
                delta_base: Some(base_position),
                checkpoint_bytes: view.checkpoint_bytes,
            });
        }
        if checkpoint_lease.is_none() {
            *checkpoint_lease = Some(self.object_shards.acquire_checkpoint_barrier().await?);
        }
        let state = compact_state_objects(&self.object_shards, &next).await?;
        invariant::check(|| validate_compaction("wal3.checkpoint", &next, &state))?;
        let record = encode_state(&state)?;
        Ok(PlannedAppend {
            checkpoint_bytes: record.len() as u64,
            record,
            state,
            tail_deltas,
            delta_base: None,
        })
    }
}

async fn reconcile_contention(
    store: &Wal3MetadataStore,
    expected: RepositoryRevision,
    result: CommitResult,
    outcome: wal3::AppendContention,
    checkpoint_lease: Option<&super::wal3_shard::ShardBarrierLease>,
) -> Result<CommitResult, MetadataError> {
    let loaded = store.load_state_at_manifest().await?;
    let actual = loaded
        .state
        .as_ref()
        .ok_or_else(|| {
            MetadataError::Corruption("wal3 contention left no state record".to_owned())
        })?
        .revision;
    if actual == result.revision {
        store.cache_loaded(loaded)?;
        store.record_successful_append();
        return Ok(result);
    }
    if actual != expected {
        return Err(MetadataError::StaleRevision { expected, actual });
    }
    if let Some(lease) = checkpoint_lease
        && !store.object_shards.owns_barrier(lease).await?
    {
        return Err(MetadataError::Transient(
            "logical checkpoint publication was fenced by maintenance".to_owned(),
        ));
    }
    match outcome {
        wal3::AppendContention::Retryable => Err(MetadataError::Transient(
            "wal3 rejected a commit without advancing the log".to_owned(),
        )),
        wal3::AppendContention::Indeterminate => Err(MetadataError::Backend(
            "wal3 could not determine whether the contended commit became durable".to_owned(),
        )),
        wal3::AppendContention::Durable => Err(MetadataError::Backend(
            "wal3 reported a durable commit that is absent from the stable manifest".to_owned(),
        )),
    }
}

fn next_generation(generation: u64) -> Result<u64, MetadataError> {
    generation
        .checked_add(1)
        .ok_or_else(|| MetadataError::Backend("metadata generation exhausted".into()))
}

/// Apply `mutation` to `state`. Invariant-checking builds also return each
/// added key with the record it resolved to before the mutation.
async fn apply_mutation(
    shards: &ObjectShardStorage,
    mut state: StateData,
    mutation: MetadataMutation,
    retained: Option<Arc<dyn RetainedObjects>>,
) -> Result<
    (
        StateData,
        CommitResult,
        Vec<(ObjectKey, Option<ObjectRecord>)>,
    ),
    MetadataError,
> {
    mutation.reject_mixed_collection()?;
    state.generation = next_generation(state.generation)?;
    let mut result = CommitResult {
        revision: fresh_revision(Some(state.revision))?,
        objects_inserted: 0,
        objects_removed: 0,
        roots_changed: 0,
    };
    let mut added = Vec::new();
    if let Some(retained) = retained {
        let previous = state
            .base_objects
            .object_count
            .saturating_add(state.objects.len() as u64);
        let dense_retention =
            state.base_objects.object_count > 0 && retained.len() as u64 >= previous.div_ceil(2);
        if dense_retention {
            state = compact_state_objects(shards, &state).await?;
        }
        let mut roots = Wal3Snapshot {
            state: Arc::new(state.clone()),
            shards: shards.clone(),
        }
        .roots();
        while let Some(root) = roots.try_next().await? {
            if !retained.contains(root.target()).await? {
                return Err(MetadataError::InvalidRetainedSet(format!(
                    "root target {} is not retained",
                    root.target()
                )));
            }
        }
        if dense_retention {
            state.base_objects =
                Arc::new(collect_dense_retained_shards(shards, &state, retained.clone()).await?);
        } else {
            let mut map = StateShardMap::default();
            let mut pending = Vec::new();
            let mut pending_bytes = 0_u64;
            let mut after = None;
            let mut observed = 0_usize;
            loop {
                let page = retained.page(after.clone(), RETAINED_PAGE).await?;
                validate_retained_page(after.as_ref(), &page)?;
                let Some(last) = page.last().cloned() else {
                    break;
                };
                for key in page {
                    let (record, validated, generation) = lookup_state_object(shards, &state, &key)
                        .await?
                        .ok_or_else(|| {
                            MetadataError::InvalidRetainedSet(format!("unknown retained key {key}"))
                        })?;
                    validate_retained_links(retained.as_ref(), &record).await?;
                    push_object_shard_entry(
                        shards,
                        &mut map,
                        &mut pending,
                        &mut pending_bytes,
                        (record, validated, generation),
                    )
                    .await?;
                    observed = observed.saturating_add(1);
                }
                after = Some(last);
            }
            if observed != retained.len() {
                return Err(MetadataError::InvalidRetainedSet(format!(
                    "retained source declared {} keys but yielded {observed}",
                    retained.len()
                )));
            }
            flush_object_shard(shards, &mut map, &mut pending).await?;
            map.roots.clone_from(&state.base_objects.roots);
            map.root_count = state.base_objects.root_count;
            state.base_objects = Arc::new(map);
        }
        state.objects.clear();
        state.births.clear();
        state.validated.clear();
        result.objects_removed =
            usize::try_from(previous.saturating_sub(retained.len() as u64)).unwrap_or(usize::MAX);
    } else {
        for verified in mutation.objects {
            let record = verified.into_record();
            let existing = lookup_state_object(shards, &state, record.key())
                .await?
                .map(|(existing, _, _)| existing);
            if invariant::ENABLED {
                added.push((record.key().clone(), existing.clone()));
            }
            match existing {
                Some(existing) if existing == record => {}
                Some(_) => return Err(MetadataError::ImmutableConflict(record.key().clone())),
                None => {
                    state.births.insert(record.key().clone(), state.generation);
                    state.objects.insert(record.key().clone(), record);
                    result.objects_inserted += 1;
                }
            }
        }
        let mut newly_named = Vec::new();
        for change in mutation.roots.into_values() {
            let name = change.name().clone();
            match change {
                RootChange::Set { target, .. } => {
                    let previous = lookup_state_root(shards, &state, &name).await?;
                    if previous != Some(target.clone()) {
                        result.roots_changed += 1;
                        if previous.is_none() {
                            state.root_count = state.root_count.saturating_add(1);
                        }
                        state.roots.insert(name, Some(target.clone()));
                    }
                    newly_named.push(target.clone());
                }
                RootChange::Remove { .. } => {
                    if lookup_state_root(shards, &state, &name).await?.is_some() {
                        result.roots_changed += 1;
                        state.root_count = state.root_count.saturating_sub(1);
                        state.roots.insert(name, None);
                    }
                }
            }
        }
        let newly_validated: BTreeSet<_> = mutation.validated_closures.into_iter().collect();
        validate_root_closures_sharded(shards, &state, &newly_named, &newly_validated).await?;
        for key in newly_validated {
            match lookup_state_object(shards, &state, &key).await? {
                Some((_, false, _)) => {
                    state.validated.insert(key);
                }
                Some((_, true, _)) => {}
                None => {
                    return Err(MetadataError::MissingObject {
                        missing: key,
                        from: None,
                    });
                }
            }
        }
    }
    if let Some(payload_catalog) = mutation.payload_catalog {
        if payload_catalog.is_empty() {
            return Err(MetadataError::Corruption(
                "payload catalog must not be empty".to_owned(),
            ));
        }
        if payload_catalog.len() > MAX_PAYLOAD_CATALOG_BYTES {
            return Err(MetadataError::Corruption(format!(
                "payload catalog has {} bytes, limit is {MAX_PAYLOAD_CATALOG_BYTES}",
                payload_catalog.len()
            )));
        }
        state.payload_catalog = payload_catalog;
    }
    state.revision = result.revision;
    Ok((state, result, added))
}

fn validate_retained_page(
    after: Option<&ObjectKey>,
    page: &[ObjectKey],
) -> Result<(), MetadataError> {
    if page.len() > RETAINED_PAGE
        || page.windows(2).any(|pair| pair[0] >= pair[1])
        || after.is_some_and(|previous| page.first().is_some_and(|first| first <= previous))
    {
        return Err(MetadataError::InvalidRetainedSet(
            "retained source did not return a canonical ordered page".to_owned(),
        ));
    }
    Ok(())
}

async fn validate_retained_links(
    retained: &dyn RetainedObjects,
    record: &ObjectRecord,
) -> Result<(), MetadataError> {
    for link in record.links() {
        if !retained.contains(link).await? {
            return Err(MetadataError::InvalidRetainedSet(format!(
                "retained object {} links to omitted {link}",
                record.key()
            )));
        }
    }
    Ok(())
}

struct RetainedPager {
    source: Arc<dyn RetainedObjects>,
    page: Vec<ObjectKey>,
    at: usize,
    after: Option<ObjectKey>,
    exhausted: bool,
    yielded: usize,
}

impl RetainedPager {
    fn new(source: Arc<dyn RetainedObjects>) -> Self {
        Self {
            source,
            page: Vec::new(),
            at: 0,
            after: None,
            exhausted: false,
            yielded: 0,
        }
    }

    async fn next(&mut self) -> Result<Option<ObjectKey>, MetadataError> {
        loop {
            if let Some(key) = self.page.get(self.at).cloned() {
                self.at += 1;
                self.yielded = self.yielded.saturating_add(1);
                return Ok(Some(key));
            }
            if self.exhausted {
                return Ok(None);
            }
            let page = self.source.page(self.after.clone(), RETAINED_PAGE).await?;
            validate_retained_page(self.after.as_ref(), &page)?;
            if page.is_empty() {
                self.exhausted = true;
                return Ok(None);
            }
            self.after = page.last().cloned();
            self.page = page;
            self.at = 0;
        }
    }
}

async fn collect_dense_retained_shards(
    shards: &ObjectShardStorage,
    state: &StateData,
    retained: Arc<dyn RetainedObjects>,
) -> Result<StateShardMap, MetadataError> {
    debug_assert!(state.objects.is_empty());
    debug_assert!(state.validated.is_empty());
    let mut map = StateShardMap {
        roots: state.base_objects.roots.clone(),
        root_count: state.base_objects.root_count,
        ..StateShardMap::default()
    };
    let mut pager = RetainedPager::new(retained.clone());
    let mut next_retained = pager.next().await?;
    for reference in &state.base_objects.objects {
        let entries = shards.read(reference).await?;
        let base_entries = entries.len();
        let mut kept = Vec::with_capacity(base_entries);
        for (record, validated, generation) in entries {
            if next_retained.as_ref().is_some_and(|key| key < record.key()) {
                return Err(MetadataError::InvalidRetainedSet(format!(
                    "unknown retained key {}",
                    next_retained.as_ref().expect("checked retained key")
                )));
            }
            if next_retained.as_ref() == Some(record.key()) {
                validate_retained_links(retained.as_ref(), &record).await?;
                kept.push((record, validated, generation));
                next_retained = pager.next().await?;
            }
        }
        if kept.len() == base_entries {
            append_object_shard_reference(&mut map, reference.clone());
        } else if !kept.is_empty() {
            let shard = encode_object_shard(&kept).map_err(|error| {
                MetadataError::Corruption(format!("encode retained logical object shard: {error}"))
            })?;
            shards.put(&shard).await?;
            append_object_shard_reference(&mut map, shard.reference);
        }
    }
    if let Some(key) = next_retained {
        return Err(MetadataError::InvalidRetainedSet(format!(
            "unknown retained key {key}"
        )));
    }
    if pager.yielded != retained.len() {
        return Err(MetadataError::InvalidRetainedSet(format!(
            "retained source declared {} keys but yielded {}",
            retained.len(),
            pager.yielded
        )));
    }
    Ok(map)
}

async fn apply_delta(
    shards: &ObjectShardStorage,
    state: &mut StateData,
    delta: &StateDelta,
) -> Result<(), MetadataError> {
    if state.revision != delta.expected || delta.revision == delta.expected {
        return Err(MetadataError::Corruption(
            "wal3 delta revision chain is invalid".to_owned(),
        ));
    }
    let generation = next_generation(state.generation)?;
    for record in &delta.objects {
        match lookup_state_object(shards, state, record.key()).await? {
            Some((existing, _, _)) if existing == *record => {}
            Some(_) => {
                return Err(MetadataError::Corruption(format!(
                    "wal3 delta changes immutable object {}",
                    record.key()
                )));
            }
            None => {
                state.births.insert(record.key().clone(), generation);
                state.objects.insert(record.key().clone(), record.clone());
            }
        }
    }
    let mut newly_named = Vec::new();
    for change in &delta.roots {
        match change {
            RootChange::Set { name, target } => {
                let previous = lookup_state_root(shards, state, name).await?;
                if previous != Some(target.clone()) {
                    if previous.is_none() {
                        state.root_count = state.root_count.saturating_add(1);
                    }
                    state.roots.insert(name.clone(), Some(target.clone()));
                }
                newly_named.push(target.clone());
            }
            RootChange::Remove { name } => {
                if lookup_state_root(shards, state, name).await?.is_some() {
                    state.root_count = state.root_count.saturating_sub(1);
                    state.roots.insert(name.clone(), None);
                }
            }
        }
    }
    let newly_validated = delta.validated.iter().cloned().collect::<BTreeSet<_>>();
    let mut actual_newly_validated = BTreeSet::new();
    for key in &newly_validated {
        match lookup_state_object(shards, state, key).await? {
            Some((_, false, _)) => {
                actual_newly_validated.insert(key.clone());
            }
            Some((_, true, _)) => {}
            None => {
                return Err(MetadataError::Corruption(format!(
                    "wal3 delta validates missing object {key}"
                )));
            }
        }
    }
    validate_root_closures_sharded(shards, state, &newly_named, &newly_validated)
        .await
        .map_err(|error| {
            MetadataError::Corruption(format!("invalid wal3 delta closure: {error}"))
        })?;
    state.validated.extend(actual_newly_validated);
    if let Some(payload_catalog) = &delta.payload_catalog {
        if payload_catalog.is_empty() || payload_catalog.len() > MAX_PAYLOAD_CATALOG_BYTES {
            return Err(MetadataError::Corruption(
                "wal3 delta has an invalid payload catalog".to_owned(),
            ));
        }
        state.payload_catalog.clone_from(payload_catalog);
    }
    state.revision = delta.revision;
    state.generation = generation;
    Ok(())
}

async fn lookup_state_object(
    shards: &ObjectShardStorage,
    state: &StateData,
    key: &ObjectKey,
) -> Result<Option<(ObjectRecord, bool, u64)>, MetadataError> {
    if let Some(record) = state.objects.get(key) {
        return Ok(Some((
            record.clone(),
            state.validated.contains(key),
            state.object_birth(key)?,
        )));
    }
    let found = shards.lookup(state.base_objects.as_ref(), key).await?;
    Ok(found.map(|(record, validated, generation)| {
        let validated = validated || state.validated.contains(key);
        (record, validated, generation)
    }))
}

async fn lookup_state_root(
    shards: &ObjectShardStorage,
    state: &StateData,
    name: &RootName,
) -> Result<Option<ObjectKey>, MetadataError> {
    if let Some(target) = state.roots.get(name) {
        return Ok(target.clone());
    }
    shards.lookup_root(state.base_objects.as_ref(), name).await
}

async fn validate_root_closures_sharded(
    shards: &ObjectShardStorage,
    state: &StateData,
    named: &[ObjectKey],
    newly_validated: &BTreeSet<ObjectKey>,
) -> Result<(), MetadataError> {
    let mut walk = super::closure::ClosureWalk::new(named, newly_validated);
    while let Some(key) = walk.next_key() {
        let found = lookup_state_object(shards, state, &key).await?;
        walk.visit(
            found
                .as_ref()
                .map(|(record, validated, _)| (record.links(), *validated)),
        )?;
    }
    Ok(())
}

async fn compact_state_objects(
    shards: &ObjectShardStorage,
    state: &StateData,
) -> Result<StateData, MetadataError> {
    let mut map = StateShardMap::default();
    compact_object_shards(shards, state, &mut map).await?;
    compact_root_shards(shards, state, &mut map).await?;
    let mut compacted = state.clone();
    compacted.base_objects = Arc::new(map);
    compacted.objects.clear();
    compacted.births.clear();
    compacted.validated.clear();
    compacted.roots.clear();
    Ok(compacted)
}

async fn compact_object_shards(
    shards: &ObjectShardStorage,
    state: &StateData,
    map: &mut StateShardMap,
) -> Result<(), MetadataError> {
    if state.objects.is_empty() && state.validated.is_empty() {
        map.objects.clone_from(&state.base_objects.objects);
        map.object_count = state.base_objects.object_count;
        map.validated_count = state.base_objects.validated_count;
        return Ok(());
    }

    let mut pending = Vec::new();
    let mut pending_bytes = 0_u64;
    let mut overlay = state.objects.values().peekable();
    for reference in &state.base_objects.objects {
        while overlay
            .peek()
            .is_some_and(|candidate| candidate.key() < &reference.first)
        {
            let record = overlay.next().expect("peeked object overlay").clone();
            let validated = state.validated.contains(record.key());
            push_object_shard_entry(
                shards,
                map,
                &mut pending,
                &mut pending_bytes,
                (record.clone(), validated, state.object_birth(record.key())?),
            )
            .await?;
        }
        flush_object_shard(shards, map, &mut pending).await?;
        pending_bytes = 0;

        let has_object_change = overlay
            .peek()
            .is_some_and(|candidate| candidate.key() <= &reference.last);
        let has_validation_change = state
            .validated
            .range(reference.first.clone()..=reference.last.clone())
            .next()
            .is_some();
        if !has_object_change && !has_validation_change {
            append_object_shard_reference(map, reference.clone());
            continue;
        }

        for (record, base_validated, generation) in shards.read(reference).await? {
            while overlay
                .peek()
                .is_some_and(|candidate| candidate.key() < record.key())
            {
                let overlay_record = overlay.next().expect("peeked object overlay").clone();
                let validated = state.validated.contains(overlay_record.key());
                push_object_shard_entry(
                    shards,
                    map,
                    &mut pending,
                    &mut pending_bytes,
                    (
                        overlay_record.clone(),
                        validated,
                        state.object_birth(overlay_record.key())?,
                    ),
                )
                .await?;
            }
            let entry = if overlay
                .peek()
                .is_some_and(|candidate| candidate.key() == record.key())
            {
                let overlay_record = overlay.next().expect("peeked object overlay").clone();
                let validated = base_validated || state.validated.contains(overlay_record.key());
                (overlay_record, validated, generation)
            } else {
                let validated = base_validated || state.validated.contains(record.key());
                (record, validated, generation)
            };
            push_object_shard_entry(shards, map, &mut pending, &mut pending_bytes, entry).await?;
        }
        while overlay
            .peek()
            .is_some_and(|candidate| candidate.key() <= &reference.last)
        {
            let record = overlay.next().expect("peeked object overlay").clone();
            let validated = state.validated.contains(record.key());
            push_object_shard_entry(
                shards,
                map,
                &mut pending,
                &mut pending_bytes,
                (record.clone(), validated, state.object_birth(record.key())?),
            )
            .await?;
        }
        flush_object_shard(shards, map, &mut pending).await?;
        pending_bytes = 0;
    }
    for record in overlay {
        push_object_shard_entry(
            shards,
            map,
            &mut pending,
            &mut pending_bytes,
            (
                record.clone(),
                state.validated.contains(record.key()),
                state.object_birth(record.key())?,
            ),
        )
        .await?;
    }
    flush_object_shard(shards, map, &mut pending).await
}

async fn compact_root_shards(
    shards: &ObjectShardStorage,
    state: &StateData,
    map: &mut StateShardMap,
) -> Result<(), MetadataError> {
    if state.roots.is_empty() {
        map.roots.clone_from(&state.base_objects.roots);
        map.root_count = state.base_objects.root_count;
        return Ok(());
    }

    let mut pending = Vec::new();
    let mut pending_bytes = 0_u64;
    let mut overlay = state.roots.iter().peekable();
    for reference in &state.base_objects.roots {
        while overlay
            .peek()
            .is_some_and(|(name, _)| *name < &reference.first)
        {
            let (name, target) = overlay.next().expect("peeked root overlay");
            if let Some(target) = target {
                push_root_shard_entry(
                    shards,
                    map,
                    &mut pending,
                    &mut pending_bytes,
                    RootRecord::new(name.clone(), target.clone()),
                )
                .await?;
            }
        }
        flush_root_shard(shards, map, &mut pending).await?;
        pending_bytes = 0;

        let affected = overlay
            .peek()
            .is_some_and(|(name, _)| *name <= &reference.last);
        if !affected {
            append_root_shard_reference(map, reference.clone());
            continue;
        }

        for root in shards.read_roots(reference).await? {
            while overlay.peek().is_some_and(|(name, _)| *name < root.name()) {
                let (name, target) = overlay.next().expect("peeked root overlay");
                if let Some(target) = target {
                    push_root_shard_entry(
                        shards,
                        map,
                        &mut pending,
                        &mut pending_bytes,
                        RootRecord::new(name.clone(), target.clone()),
                    )
                    .await?;
                }
            }
            if overlay.peek().is_some_and(|(name, _)| *name == root.name()) {
                let (name, target) = overlay.next().expect("peeked root overlay");
                if let Some(target) = target {
                    push_root_shard_entry(
                        shards,
                        map,
                        &mut pending,
                        &mut pending_bytes,
                        RootRecord::new(name.clone(), target.clone()),
                    )
                    .await?;
                }
            } else {
                push_root_shard_entry(shards, map, &mut pending, &mut pending_bytes, root).await?;
            }
        }
        while overlay
            .peek()
            .is_some_and(|(name, _)| *name <= &reference.last)
        {
            let (name, target) = overlay.next().expect("peeked root overlay");
            if let Some(target) = target {
                push_root_shard_entry(
                    shards,
                    map,
                    &mut pending,
                    &mut pending_bytes,
                    RootRecord::new(name.clone(), target.clone()),
                )
                .await?;
            }
        }
        flush_root_shard(shards, map, &mut pending).await?;
        pending_bytes = 0;
    }
    for (name, target) in overlay {
        if let Some(target) = target {
            push_root_shard_entry(
                shards,
                map,
                &mut pending,
                &mut pending_bytes,
                RootRecord::new(name.clone(), target.clone()),
            )
            .await?;
        }
    }
    flush_root_shard(shards, map, &mut pending).await
}

async fn push_object_shard_entry(
    shards: &ObjectShardStorage,
    map: &mut StateShardMap,
    pending: &mut Vec<(ObjectRecord, bool, u64)>,
    pending_bytes: &mut u64,
    entry: (ObjectRecord, bool, u64),
) -> Result<(), MetadataError> {
    let encoded_bytes = entry.0.encode().len() as u64 + 17;
    if !pending.is_empty()
        && pending_bytes.saturating_add(encoded_bytes) > DEFAULT_OBJECT_SHARD_TARGET_BYTES
    {
        flush_object_shard(shards, map, pending).await?;
        *pending_bytes = 0;
    }
    *pending_bytes = pending_bytes.saturating_add(encoded_bytes);
    pending.push(entry);
    Ok(())
}

fn append_object_shard_reference(
    map: &mut StateShardMap,
    reference: super::wal3_shard::ObjectShardRef,
) {
    map.object_count = map.object_count.saturating_add(reference.entries);
    map.validated_count = map.validated_count.saturating_add(reference.validated);
    map.objects.push(reference);
}

async fn flush_object_shard(
    shards: &ObjectShardStorage,
    map: &mut StateShardMap,
    pending: &mut Vec<(ObjectRecord, bool, u64)>,
) -> Result<(), MetadataError> {
    if pending.is_empty() {
        return Ok(());
    }
    let shard = encode_object_shard(pending).map_err(|error| {
        MetadataError::Corruption(format!("encode logical object shard: {error}"))
    })?;
    shards.put(&shard).await?;
    append_object_shard_reference(map, shard.reference);
    pending.clear();
    Ok(())
}

async fn push_root_shard_entry(
    shards: &ObjectShardStorage,
    map: &mut StateShardMap,
    pending: &mut Vec<RootRecord>,
    pending_bytes: &mut u64,
    entry: RootRecord,
) -> Result<(), MetadataError> {
    let encoded_bytes = entry.encode().len() as u64 + 8;
    if !pending.is_empty()
        && pending_bytes.saturating_add(encoded_bytes) > DEFAULT_ROOT_SHARD_TARGET_BYTES
    {
        flush_root_shard(shards, map, pending).await?;
        *pending_bytes = 0;
    }
    *pending_bytes = pending_bytes.saturating_add(encoded_bytes);
    pending.push(entry);
    Ok(())
}

fn append_root_shard_reference(
    map: &mut StateShardMap,
    reference: super::wal3_shard::RootShardRef,
) {
    map.root_count = map.root_count.saturating_add(reference.entries);
    map.roots.push(reference);
}

async fn flush_root_shard(
    shards: &ObjectShardStorage,
    map: &mut StateShardMap,
    pending: &mut Vec<RootRecord>,
) -> Result<(), MetadataError> {
    if pending.is_empty() {
        return Ok(());
    }
    let shard = encode_root_shard(pending).map_err(|error| {
        MetadataError::Corruption(format!("encode logical root shard: {error}"))
    })?;
    shards.put_root(&shard).await?;
    append_root_shard_reference(map, shard.reference);
    pending.clear();
    Ok(())
}

fn encode_delta_record(base_position: wal3::LogPosition, deltas: &[StateDelta]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(DELTA_MAGIC_V1);
    out.extend_from_slice(&base_position.offset().to_le_bytes());
    put_entries(&mut out, deltas.iter().map(encode_delta));
    out
}

fn encode_delta(delta: &StateDelta) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(delta.expected.as_bytes());
    out.extend_from_slice(delta.revision.as_bytes());
    put_entries(&mut out, delta.objects.iter().map(ObjectRecord::encode));
    put_entries(&mut out, delta.roots.iter().map(encode_root_change));
    put_entries(&mut out, delta.validated.iter().map(ObjectKey::encode));
    match &delta.payload_catalog {
        Some(catalog) => {
            out.push(1);
            out.extend_from_slice(&(catalog.len() as u64).to_le_bytes());
            out.extend_from_slice(catalog);
        }
        None => out.push(0),
    }
    out
}

fn encode_root_change(change: &RootChange) -> Vec<u8> {
    let mut out = Vec::new();
    match change {
        RootChange::Set { name, target } => {
            out.push(1);
            put_bytes(&mut out, name.as_str().as_bytes());
            put_bytes(&mut out, &target.encode());
        }
        RootChange::Remove { name } => {
            out.push(0);
            put_bytes(&mut out, name.as_str().as_bytes());
        }
    }
    out
}

fn decode_delta_record(
    bytes: &[u8],
) -> Result<(wal3::LogPosition, Vec<StateDelta>), MetadataError> {
    if bytes.len() > MAX_DELTA_RECORD_BYTES {
        return Err(MetadataError::Corruption(format!(
            "wal3 delta record has {} bytes, limit is {MAX_DELTA_RECORD_BYTES}",
            bytes.len()
        )));
    }
    let mut input = Input::new(bytes);
    if input.take(DELTA_MAGIC_V1.len())? != DELTA_MAGIC_V1 {
        return Err(MetadataError::Corruption(
            "unknown wal3 delta record format".to_owned(),
        ));
    }
    let base_position = wal3::LogPosition::from_offset(input.u64()?);
    let entries = input.entries()?;
    if entries.is_empty() || entries.len() > MAX_TAIL_DELTAS {
        return Err(MetadataError::Corruption(format!(
            "wal3 delta tail has {} entries, expected 1..={MAX_TAIL_DELTAS}",
            entries.len()
        )));
    }
    let deltas = entries
        .into_iter()
        .map(decode_delta)
        .collect::<Result<Vec<_>, _>>()?;
    input.finish()?;
    Ok((base_position, deltas))
}

fn decode_delta(bytes: &[u8]) -> Result<StateDelta, MetadataError> {
    let mut input = Input::new(bytes);
    let expected = RepositoryRevision::from_bytes(input.take(32)?.try_into().expect("exact width"));
    let revision = RepositoryRevision::from_bytes(input.take(32)?.try_into().expect("exact width"));
    let mut objects = Vec::new();
    let mut previous_object = None;
    for entry in input.entries()? {
        let record = ObjectRecord::decode(entry).map_err(|error| {
            MetadataError::Corruption(format!("invalid wal3 delta object record: {error}"))
        })?;
        if previous_object
            .as_ref()
            .is_some_and(|key| key >= record.key())
        {
            return Err(MetadataError::Corruption(
                "wal3 delta objects are not strictly ordered".to_owned(),
            ));
        }
        previous_object = Some(record.key().clone());
        objects.push(record);
    }
    let mut roots = Vec::new();
    let mut previous_root = None;
    for entry in input.entries()? {
        let change = decode_root_change(entry)?;
        if previous_root
            .as_ref()
            .is_some_and(|name: &RootName| name >= change.name())
        {
            return Err(MetadataError::Corruption(
                "wal3 delta root changes are not strictly ordered".to_owned(),
            ));
        }
        previous_root = Some(change.name().clone());
        roots.push(change);
    }
    let mut validated = Vec::new();
    for entry in input.entries()? {
        let key = ObjectKey::decode(entry).map_err(|error| {
            MetadataError::Corruption(format!("invalid wal3 delta validation key: {error}"))
        })?;
        if validated.last().is_some_and(|previous| previous >= &key) {
            return Err(MetadataError::Corruption(
                "wal3 delta validation keys are not strictly ordered".to_owned(),
            ));
        }
        validated.push(key);
    }
    let payload_catalog = match input.u8()? {
        0 => None,
        1 => {
            let len = usize::try_from(input.u64()?).map_err(|_| {
                MetadataError::Corruption("wal3 delta payload catalog is too large".to_owned())
            })?;
            if len == 0 || len > MAX_PAYLOAD_CATALOG_BYTES {
                return Err(MetadataError::Corruption(
                    "wal3 delta has an invalid payload catalog".to_owned(),
                ));
            }
            Some(input.take(len)?.to_vec())
        }
        _ => {
            return Err(MetadataError::Corruption(
                "wal3 delta has an invalid payload catalog tag".to_owned(),
            ));
        }
    };
    input.finish()?;
    Ok(StateDelta {
        expected,
        revision,
        objects,
        roots,
        validated,
        payload_catalog,
    })
}

fn decode_root_change(bytes: &[u8]) -> Result<RootChange, MetadataError> {
    let mut input = Input::new(bytes);
    let tag = input.u8()?;
    let name_bytes = input.bytes()?;
    let name = std::str::from_utf8(name_bytes)
        .map_err(|_| MetadataError::Corruption("wal3 delta root name is not UTF-8".to_owned()))?;
    let name = RootName::try_from(name)
        .map_err(|error| MetadataError::Corruption(format!("invalid wal3 delta root: {error}")))?;
    let change = match tag {
        0 => RootChange::Remove { name },
        1 => {
            let target = ObjectKey::decode(input.bytes()?).map_err(|error| {
                MetadataError::Corruption(format!("invalid wal3 delta root target: {error}"))
            })?;
            RootChange::Set { name, target }
        }
        _ => {
            return Err(MetadataError::Corruption(
                "wal3 delta has an invalid root-change tag".to_owned(),
            ));
        }
    };
    input.finish()?;
    Ok(change)
}

fn encode_state(state: &StateData) -> Result<Vec<u8>, MetadataError> {
    if !state.objects.is_empty() || !state.births.is_empty() || !state.validated.is_empty() {
        return Err(MetadataError::Corruption(
            "refusing to checkpoint an unmerged logical-state overlay".to_owned(),
        ));
    }
    let mut out = Vec::new();
    out.extend_from_slice(STATE_MAGIC_V4);
    out.extend_from_slice(state.revision.as_bytes());
    out.extend_from_slice(&state.generation.to_le_bytes());
    let map = encode_state_shard_map(state.base_objects.as_ref()).map_err(|error| {
        MetadataError::Corruption(format!("encode logical state shard map: {error}"))
    })?;
    put_bytes(&mut out, &map);
    out.extend_from_slice(&(state.payload_catalog.len() as u64).to_le_bytes());
    out.extend_from_slice(&state.payload_catalog);
    Ok(out)
}

fn put_entries(out: &mut Vec<u8>, entries: impl IntoIterator<Item = Vec<u8>>) {
    let entries = entries.into_iter().collect::<Vec<_>>();
    out.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for entry in entries {
        out.extend_from_slice(&(entry.len() as u64).to_le_bytes());
        out.extend_from_slice(&entry);
    }
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(bytes);
}

fn decode_state(bytes: &[u8]) -> Result<StateData, MetadataError> {
    let mut input = Input::new(bytes);
    let magic = input.take(STATE_MAGIC_V3.len())?;
    if magic != STATE_MAGIC_V3 && magic != STATE_MAGIC_V4 {
        return Err(MetadataError::Corruption(
            "unknown wal3 state record format".to_owned(),
        ));
    }
    let revision = RepositoryRevision::from_bytes(input.take(32)?.try_into().expect("exact width"));
    let generation = if magic == STATE_MAGIC_V4 {
        input.u64()?
    } else {
        0
    };
    let base_objects = decode_state_shard_map(input.bytes()?).map_err(|error| {
        MetadataError::Corruption(format!("invalid logical state shard map: {error}"))
    })?;
    let len = usize::try_from(input.u64()?).map_err(|_| {
        MetadataError::Corruption("payload catalog length overflows usize".to_owned())
    })?;
    if len == 0 || len > MAX_PAYLOAD_CATALOG_BYTES {
        return Err(MetadataError::Corruption(format!(
            "payload catalog has invalid length {len}"
        )));
    }
    let payload_catalog = input.take(len)?.to_vec();
    input.finish()?;
    let root_count = base_objects.root_count;
    Ok(StateData {
        revision,
        generation,
        births: BTreeMap::new(),
        base_objects: Arc::new(base_objects),
        objects: BTreeMap::new(),
        roots: BTreeMap::new(),
        root_count,
        validated: BTreeSet::new(),
        payload_catalog,
    })
}

struct Input<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Input<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], MetadataError> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| MetadataError::Corruption("truncated wal3 state record".to_owned()))?;
        let result = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(result)
    }

    fn u64(&mut self) -> Result<u64, MetadataError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("exact width"),
        ))
    }

    fn u8(&mut self) -> Result<u8, MetadataError> {
        Ok(self.take(1)?[0])
    }

    fn bytes(&mut self) -> Result<&'a [u8], MetadataError> {
        let len = usize::try_from(self.u64()?)
            .map_err(|_| MetadataError::Corruption("wal3 byte string is too large".to_owned()))?;
        self.take(len)
    }

    fn entries(&mut self) -> Result<Vec<&'a [u8]>, MetadataError> {
        let count = usize::try_from(self.u64()?).map_err(|_| {
            MetadataError::Corruption("wal3 state entry count overflows usize".to_owned())
        })?;
        if count > self.bytes.len().saturating_sub(self.offset) / 8 {
            return Err(MetadataError::Corruption(
                "wal3 state entry count exceeds remaining bytes".to_owned(),
            ));
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let len = usize::try_from(self.u64()?).map_err(|_| {
                MetadataError::Corruption("wal3 state entry length overflows usize".to_owned())
            })?;
            entries.push(self.take(len)?);
        }
        Ok(entries)
    }

    fn finish(&self) -> Result<(), MetadataError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(MetadataError::Corruption(
                "trailing bytes in wal3 state record".to_owned(),
            ))
        }
    }
}

fn wal3_error(error: wal3::Error) -> MetadataError {
    match error {
        wal3::Error::Backoff | wal3::Error::LogContentionRetry => {
            MetadataError::Transient(format!("wal3: {error}"))
        }
        wal3::Error::StorageError(ref storage)
            if matches!(storage.as_ref(), chroma_storage::StorageError::Backoff) =>
        {
            MetadataError::Transient(format!("wal3: {error}"))
        }
        wal3::Error::CorruptManifest(_)
        | wal3::Error::CorruptFragment(_)
        | wal3::Error::CorruptCursor(_)
        | wal3::Error::CorruptGarbage(_)
        | wal3::Error::ScrubError(_)
        | wal3::Error::ParquetError(_) => MetadataError::Corruption(format!("wal3: {error}")),
        error => MetadataError::Backend(format!("wal3: {error}")),
    }
}

#[cfg(test)]
mod tests {
    include!("wal3/coordination_tests.rs");
    use super::*;
    use crate::object_store::ObjectStore;
    use futures::TryStreamExt;
    use std::io::{Cursor, Read, Seek, SeekFrom, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::process::{Child, Command, Stdio};
    use std::sync::OnceLock;
    use std::time::{Duration, Instant};

    async fn verified_blob(bytes: &[u8]) -> crate::VerifiedObject {
        use crate::format::{FormatLimits, FormatRegistry};

        let key = ObjectKey::blob(crate::BlobId::new(crate::Digest::hash(bytes)));
        let mut reader = Cursor::new(bytes.to_vec());
        FormatRegistry::builtin()
            .verify(
                &key,
                &mut reader as &mut dyn crate::PayloadReader,
                &FormatLimits::default(),
            )
            .await
            .unwrap()
    }

    fn logical_record(index: u32) -> ObjectRecord {
        let key = ObjectKey::new(
            crate::NamespaceId::try_from("test.logical.v1").unwrap(),
            index.to_be_bytes().to_vec(),
        )
        .unwrap();
        let payload = crate::BlobId::new(crate::Digest::from(
            *blake3::hash(&index.to_le_bytes()).as_bytes(),
        ));
        ObjectRecord::new(key, payload, 4, Vec::new()).unwrap()
    }

    #[derive(Debug)]
    struct ProbedRetained {
        keys: Vec<ObjectKey>,
        page_calls: Arc<AtomicU64>,
        contains_calls: Arc<AtomicU64>,
    }

    #[async_trait]
    impl RetainedObjects for ProbedRetained {
        fn len(&self) -> usize {
            self.keys.len()
        }

        async fn page(
            &self,
            after: Option<ObjectKey>,
            limit: usize,
        ) -> Result<Vec<ObjectKey>, MetadataError> {
            self.page_calls.fetch_add(1, Ordering::Relaxed);
            let start = after
                .as_ref()
                .map_or(0, |after| self.keys.partition_point(|key| key <= after));
            Ok(self.keys[start..].iter().take(limit).cloned().collect())
        }

        async fn contains(&self, key: &ObjectKey) -> Result<bool, MetadataError> {
            self.contains_calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.keys.binary_search(key).is_ok())
        }
    }

    #[test]
    fn wal3_errors_preserve_retry_and_corruption_semantics() {
        assert!(matches!(
            wal3_error(wal3::Error::Backoff),
            MetadataError::Transient(_)
        ));
        assert!(matches!(
            wal3_error(wal3::Error::StorageError(Arc::new(
                chroma_storage::StorageError::Backoff,
            ))),
            MetadataError::Transient(_)
        ));
        assert!(matches!(
            wal3_error(wal3::Error::CorruptManifest("bad checksum".to_owned())),
            MetadataError::Corruption(_)
        ));
        assert!(matches!(
            wal3_error(wal3::Error::LogContentionFailure),
            MetadataError::Backend(_)
        ));
    }

    fn rustfs_test_lock() -> &'static tokio::sync::Mutex<()> {
        static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    struct Rustfs {
        _data: tempfile::TempDir,
        log: tempfile::NamedTempFile,
        child: Child,
        address: SocketAddr,
    }

    impl Rustfs {
        fn log_tail(log: &tempfile::NamedTempFile) -> String {
            let read = || -> std::io::Result<Vec<u8>> {
                let mut file = log.reopen()?;
                let length = file.metadata()?.len();
                file.seek(SeekFrom::Start(length.saturating_sub(64 * 1024)))?;
                let mut bytes = Vec::new();
                file.take(64 * 1024).read_to_end(&mut bytes)?;
                Ok(bytes)
            };
            match read() {
                Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                Err(error) => format!("could not read RustFS log: {error}"),
            }
        }

        fn ready(address: SocketAddr) -> bool {
            let Ok(mut stream) = TcpStream::connect_timeout(&address, Duration::from_millis(100))
            else {
                return false;
            };
            let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
            let _ = stream.set_write_timeout(Some(Duration::from_millis(100)));
            if stream
                .write_all(
                    b"GET /minio/health/ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                )
                .is_err()
            {
                return false;
            }
            let mut response = [0_u8; 32];
            stream
                .read(&mut response)
                .is_ok_and(|read| response[..read].starts_with(b"HTTP/1.1 200"))
        }

        fn start() -> Self {
            let data = tempfile::tempdir().unwrap();
            let log = tempfile::NamedTempFile::new().unwrap();
            let address = TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap();
            let console_address = TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap();
            let mut child = Command::new("rustfs")
                .arg("server")
                .arg(data.path())
                .arg("--address")
                .arg(address.to_string())
                .arg("--console-address")
                .arg(console_address.to_string())
                // Chroma's public S3 test client uses these MinIO-compatible
                // credentials for its localhost endpoint.
                .env("RUSTFS_ACCESS_KEY", "minio")
                .env("RUSTFS_SECRET_KEY", "minio123")
                .env("RUSTFS_OBS_LOG_STDOUT_ENABLED", "true")
                .env("RUSTFS_OBS_LOGGER_LEVEL", "info")
                .env_remove("RUSTFS_OBS_LOG_DIRECTORY")
                .stdout(Stdio::from(log.as_file().try_clone().unwrap()))
                .stderr(Stdio::from(log.as_file().try_clone().unwrap()))
                .spawn()
                .expect("rustfs from devenv.nix must start");
            let deadline = Instant::now() + Duration::from_secs(10);
            while Instant::now() < deadline {
                if Self::ready(address) {
                    return Self {
                        _data: data,
                        log,
                        child,
                        address,
                    };
                }
                std::thread::sleep(Duration::from_millis(25));
            }
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "rustfs did not become ready on {address} within ten seconds\n{}",
                Self::log_tail(&log)
            );
        }

        fn endpoint(&self) -> String {
            format!("http://{}", self.address)
        }
    }

    impl Drop for Rustfs {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            if std::thread::panicking() {
                eprintln!(
                    "RustFS server log (last 64 KiB):\n{}",
                    Self::log_tail(&self.log)
                );
            }
        }
    }

    async fn rustfs_storage(rustfs: &Rustfs) -> Arc<chroma_storage::Storage> {
        use chroma_config::Configurable;

        let bucket = "casita-test";
        let credentials = aws_sdk_s3::config::Credentials::new(
            "minio",
            "minio123",
            None,
            None,
            "casita-rustfs-test",
        );
        let client_config = aws_sdk_s3::config::Builder::new()
            .endpoint_url(rustfs.endpoint())
            .credentials_provider(credentials)
            .behavior_version_latest()
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .force_path_style(true)
            .build();
        aws_sdk_s3::Client::from_conf(client_config)
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .unwrap();

        let config = chroma_storage::config::StorageConfig::S3(chroma_storage::S3StorageConfig {
            bucket: bucket.to_owned(),
            credentials: chroma_storage::S3CredentialsConfig::Explicit {
                access_key_id: "minio".to_owned(),
                secret_access_key: "minio123".to_owned(),
                session_token: None,
                custom_endpoint: Some(rustfs.endpoint()),
                region: "us-east-1".to_owned(),
            },
            ..Default::default()
        });
        let storage = chroma_storage::S3Storage::try_from_config(
            &config,
            &chroma_config::registry::Registry::default(),
        )
        .await
        .unwrap();
        Arc::new(chroma_storage::Storage::S3(storage))
    }

    async fn rustfs_repository(
        storage: Arc<chroma_storage::Storage>,
        bucket: &str,
        endpoint: &str,
        prefix: &str,
        writer: &str,
    ) -> crate::repository::Repository<crate::ChunkedBlobStore, Wal3MetadataStore> {
        let objects = crate::object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_access_key_id("minio")
            .with_secret_access_key("minio123")
            .with_region("us-east-1")
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_checksum_algorithm(crate::object_store::aws::Checksum::SHA256)
            .build()
            .unwrap();
        let state = Wal3MetadataStore::open(storage, format!("{prefix}/state"), writer)
            .await
            .unwrap();
        let snapshot = state.opened_snapshot();
        let payloads = crate::ChunkedBlobStore::packed_with_catalog(
            Arc::new(objects),
            crate::object_store::path::Path::from(format!("{prefix}/payloads")),
            crate::blob::DEFAULT_AVG_CHUNK_SIZE,
            crate::PackOptions {
                target_size: crate::DEFAULT_PACK_TARGET_SIZE,
                cache_capacity: crate::DEFAULT_PACK_CACHE_CAPACITY,
            },
            snapshot.payload_catalog().unwrap(),
        )
        .await
        .unwrap();
        drop(snapshot);
        crate::repository::Repository::new(payloads, state)
    }

    #[test]
    fn legacy_checkpoints_receive_generation_zero() {
        let mut state = StateData::empty().unwrap();
        state.generation = 123;
        let encoded = encode_state(&state).unwrap();
        assert_eq!(decode_state(&encoded).unwrap().generation, 123);
        let mut legacy = encoded;
        legacy[..STATE_MAGIC_V3.len()].copy_from_slice(STATE_MAGIC_V3);
        let generation_offset = STATE_MAGIC_V3.len() + 32;
        legacy.drain(generation_offset..generation_offset + 8);
        let decoded = decode_state(&legacy).unwrap();
        assert_eq!(decoded.generation, 0);
        assert_eq!(decoded.revision, state.revision);
    }

    #[test]
    fn empty_state_encoding_round_trips() {
        let original = StateData::empty().unwrap();
        let decoded = decode_state(&encode_state(&original).unwrap()).unwrap();
        assert_eq!(decoded.revision, original.revision);
        assert_eq!(decoded.base_objects.object_count, 0);
        assert!(decoded.objects.is_empty());
        assert!(decoded.roots.is_empty());
        assert!(decoded.validated.is_empty());
        assert!(decoded.payload_catalog.starts_with(b"casitap1"));
    }

    #[tokio::test]
    async fn remote_exact_collection_recovery_preserves_live_pins() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let state =
            Wal3MetadataStore::open(rustfs_storage(&rustfs).await, "recovery/state", "recoverer")
                .await
                .unwrap();
        super::super::tests::assert_exact_collection_recovery(state).await;
    }

    #[tokio::test]
    async fn remote_online_snapshot_excludes_future_garbage() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let state = Wal3MetadataStore::open(
            rustfs_storage(&rustfs).await,
            "snapshot-birth/state",
            "reader",
        )
        .await
        .unwrap();
        super::super::tests::assert_online_snapshot_excludes_future_garbage(state).await;
    }

    #[tokio::test]
    async fn wal_snapshot_generations_survive_replay_and_checkpoint() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(chroma_storage::Storage::Local(
            chroma_storage::local::LocalStorage::new(directory.path().to_str().unwrap()),
        ));
        let state = Wal3MetadataStore::open(storage.clone(), "births", "writer")
            .await
            .unwrap();
        super::super::tests::assert_snapshot_generations(&state).await;
        let before = state.snapshot().await.unwrap();
        let watermark = before.generation().unwrap();
        let expected = before
            .objects_created_through(watermark)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        drop(before);
        for index in 0..=MAX_TAIL_DELTAS {
            let reopened = Wal3MetadataStore::open(storage.clone(), "births", "reader")
                .await
                .unwrap();
            let snapshot = reopened.snapshot().await.unwrap();
            assert_eq!(
                snapshot
                    .objects_created_through(watermark)
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap(),
                expected
            );
            assert_eq!(snapshot.generation().unwrap(), watermark + index as u64);
            let mut mutation = MetadataMutation::new();
            mutation.add_object(verified_blob(format!("new birth {index}").as_bytes()).await);
            state.commit(&snapshot.revision(), mutation).await.unwrap();
        }
        let final_state = state.snapshot().await.unwrap();
        assert!(
            !final_state.retention_resources().is_empty(),
            "test must cross a shard checkpoint"
        );
        assert_eq!(
            final_state
                .objects_created_through(watermark)
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            expected
        );
    }

    #[test]
    fn state_encoding_round_trips_payload_catalog() {
        let mut original = StateData::empty().unwrap();
        original.payload_catalog = b"exact v1 catalog root".to_vec();
        let encoded = encode_state(&original).unwrap();
        assert!(encoded.starts_with(STATE_MAGIC_V4));
        let decoded = decode_state(&encoded).unwrap();
        assert_eq!(decoded.payload_catalog, original.payload_catalog);
    }

    #[test]
    fn validation_witnesses_cost_one_bit_per_object() {
        let mut state = StateData::empty().unwrap();
        for index in 0..10_000u32 {
            let digest = crate::Digest::from(*blake3::hash(&index.to_le_bytes()).as_bytes());
            let payload = crate::BlobId::new(digest);
            let key = ObjectKey::blob(payload);
            state.objects.insert(
                key.clone(),
                ObjectRecord::new(key.clone(), payload, 4, Vec::new()).unwrap(),
            );
            state.births.insert(key.clone(), 0);
            state.validated.insert(key);
        }
        let entries = state
            .objects
            .values()
            .cloned()
            .map(|record| (record, true, 0))
            .collect::<Vec<_>>();
        let fully_validated = encode_object_shard(&entries).unwrap();
        let unvalidated = encode_object_shard(
            &entries
                .iter()
                .map(|(record, _, birth)| (record.clone(), false, *birth))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert_eq!(fully_validated.bytes.len(), unvalidated.bytes.len());
        assert_eq!(fully_validated.reference.validated, 10_000);
        assert_eq!(unvalidated.reference.validated, 0);
    }

    #[test]
    fn cumulative_delta_tail_is_bounded_and_independent_of_checkpoint_size() {
        let mut state = StateData::empty().unwrap();
        for index in 0..10_000u32 {
            let digest = crate::Digest::from(*blake3::hash(&index.to_le_bytes()).as_bytes());
            let payload = crate::BlobId::new(digest);
            let key = ObjectKey::blob(payload);
            state.objects.insert(
                key.clone(),
                ObjectRecord::new(key, payload, 4, Vec::new()).unwrap(),
            );
        }
        let entries = state
            .objects
            .values()
            .cloned()
            .map(|record| (record, false, 0))
            .collect::<Vec<_>>();
        let shard = encode_object_shard(&entries).unwrap();
        let shard_bytes = shard.bytes.len();
        state.base_objects = Arc::new(StateShardMap {
            objects: vec![shard.reference],
            object_count: entries.len() as u64,
            validated_count: 0,
            ..StateShardMap::default()
        });
        state.objects.clear();
        let checkpoint_bytes = encode_state(&state).unwrap().len();
        let mut deltas = Vec::new();
        for _ in 0..MAX_TAIL_DELTAS {
            let revision = fresh_revision(Some(state.revision)).unwrap();
            deltas.push(StateDelta {
                expected: state.revision,
                revision,
                objects: Vec::new(),
                roots: Vec::new(),
                validated: Vec::new(),
                payload_catalog: None,
            });
            state.revision = revision;
        }
        let encoded = encode_delta_record(wal3::LogPosition::from_offset(7), &deltas);
        let (base, decoded) = decode_delta_record(&encoded).unwrap();

        assert_eq!(base.offset(), 7);
        assert_eq!(decoded, deltas);
        assert!(encoded.len() < 1_000);
        assert!(checkpoint_bytes < 1_000);
        assert!(shard_bytes > checkpoint_bytes * 100);
    }

    #[tokio::test]
    async fn delta_recovery_classifies_invalid_validation_as_corruption() {
        let mut state = StateData::empty().unwrap();
        let revision = fresh_revision(Some(state.revision)).unwrap();
        let missing = ObjectKey::blob(crate::BlobId::new(crate::Digest::from([0x61; 32])));
        let delta = StateDelta {
            expected: state.revision,
            revision,
            objects: Vec::new(),
            roots: Vec::new(),
            validated: vec![missing],
            payload_catalog: None,
        };
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(chroma_storage::Storage::Local(
            chroma_storage::local::LocalStorage::new(directory.path().to_str().unwrap()),
        ));
        let shards = ObjectShardStorage::new(storage, "state".to_owned(), 1024);
        assert!(matches!(
            apply_delta(&shards, &mut state, &delta).await,
            Err(MetadataError::Corruption(_))
        ));
    }

    #[tokio::test]
    async fn checkpoint_rewrites_only_the_object_shard_touched_by_an_insert() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(chroma_storage::Storage::Local(
            chroma_storage::local::LocalStorage::new(directory.path().to_str().unwrap()),
        ));
        let writer = ObjectShardStorage::new(storage.clone(), "state".to_owned(), 0);
        let groups = [
            (0..10).map(|index| index * 2).collect::<Vec<_>>(),
            (10..20).map(|index| index * 2).collect::<Vec<_>>(),
            (20..30).map(|index| index * 2).collect::<Vec<_>>(),
        ];
        let mut map = StateShardMap::default();
        for group in groups {
            let entries = group
                .into_iter()
                .map(|index| (logical_record(index), false, 0))
                .collect::<Vec<_>>();
            let shard = encode_object_shard(&entries).unwrap();
            writer.put(&shard).await.unwrap();
            append_object_shard_reference(&mut map, shard.reference);
        }
        let original = map.objects.clone();
        let inserted = logical_record(25);
        let inserted_key = inserted.key().clone();
        let mut state = StateData::empty().unwrap();
        state.base_objects = Arc::new(map);
        state.births.insert(inserted_key.clone(), state.generation);
        state.objects.insert(inserted_key.clone(), inserted);

        let compactor = ObjectShardStorage::new(storage, "state".to_owned(), 0);
        let compacted = compact_state_objects(&compactor, &state).await.unwrap();
        let stats = compactor.stats();
        assert_eq!(stats.get_requests, 1);
        assert_eq!(stats.put_requests, 1);
        assert_eq!(compacted.base_objects.object_count, 31);
        assert_eq!(compacted.base_objects.objects.len(), 3);
        assert_eq!(compacted.base_objects.objects[0], original[0]);
        assert_ne!(compacted.base_objects.objects[1], original[1]);
        assert_eq!(compacted.base_objects.objects[2], original[2]);
        assert!(
            compactor
                .lookup(compacted.base_objects.as_ref(), &inserted_key)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn checkpoint_rewrites_only_the_root_shard_touched_by_a_change() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(chroma_storage::Storage::Local(
            chroma_storage::local::LocalStorage::new(directory.path().to_str().unwrap()),
        ));
        let writer = ObjectShardStorage::new(storage.clone(), "state".to_owned(), 0);
        let mut map = StateShardMap::default();
        for group in 0..3_u32 {
            let roots = (0..10_u32)
                .map(|offset| {
                    let index = group * 10 + offset;
                    RootRecord::new(
                        RootName::try_from(format!("root-{index:03}")).unwrap(),
                        logical_record(index).key().clone(),
                    )
                })
                .collect::<Vec<_>>();
            let shard = encode_root_shard(&roots).unwrap();
            writer.put_root(&shard).await.unwrap();
            append_root_shard_reference(&mut map, shard.reference);
        }
        let original = map.roots.clone();
        let changed_name = RootName::try_from("root-015").unwrap();
        let changed_target = logical_record(1_000).key().clone();
        let mut state = StateData::empty().unwrap();
        state.base_objects = Arc::new(map);
        state.root_count = 30;
        state
            .roots
            .insert(changed_name.clone(), Some(changed_target.clone()));

        let compactor = ObjectShardStorage::new(storage, "state".to_owned(), 0);
        let compacted = compact_state_objects(&compactor, &state).await.unwrap();
        let stats = compactor.stats();
        assert_eq!(stats.get_requests, 1);
        assert_eq!(stats.put_requests, 1);
        assert_eq!(compacted.base_objects.root_count, 30);
        assert_eq!(compacted.base_objects.roots.len(), 3);
        assert_eq!(compacted.base_objects.roots[0], original[0]);
        assert_ne!(compacted.base_objects.roots[1], original[1]);
        assert_eq!(compacted.base_objects.roots[2], original[2]);
        assert_eq!(
            compactor
                .lookup_root(compacted.base_objects.as_ref(), &changed_name)
                .await
                .unwrap(),
            Some(changed_target)
        );
    }

    #[tokio::test]
    async fn simulated_500_tb_clean_checkpoint_performs_zero_shard_io() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(chroma_storage::Storage::Local(
            chroma_storage::local::LocalStorage::new(directory.path().to_str().unwrap()),
        ));
        let shards = ObjectShardStorage::new(storage, "state".to_owned(), 0);
        let mut map = StateShardMap::default();
        for index in 0..50_000_u32 {
            let key = logical_record(index).key().clone();
            append_object_shard_reference(
                &mut map,
                crate::metadata::wal3_shard::ObjectShardRef {
                    first: key.clone(),
                    last: key,
                    digest: crate::Digest::from(*blake3::hash(&index.to_le_bytes()).as_bytes()),
                    entries: 1,
                    validated: u64::from(index % 3 == 0),
                    encoded_bytes: 1,
                },
            );
        }
        let mut state = StateData::empty().unwrap();
        state.base_objects = Arc::new(map);
        let compacted = compact_state_objects(&shards, &state).await.unwrap();

        assert_eq!(compacted.base_objects, state.base_objects);
        assert_eq!(shards.stats().get_requests, 0);
        assert_eq!(shards.stats().put_requests, 0);
        assert!(encode_state(&compacted).unwrap().len() < MAX_STATE_MAP_BYTES);
    }

    #[tokio::test]
    async fn no_op_collection_validates_but_reuses_every_object_shard() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(chroma_storage::Storage::Local(
            chroma_storage::local::LocalStorage::new(directory.path().to_str().unwrap()),
        ));
        let writer = ObjectShardStorage::new(storage.clone(), "state".to_owned(), 0);
        let mut map = StateShardMap::default();
        let mut retained = BTreeSet::new();
        let mut total_bytes = 0_usize;
        for group in 0..3_u32 {
            let entries = (0..10_u32)
                .map(|offset| {
                    let record = logical_record(group * 10 + offset);
                    retained.insert(record.key().clone());
                    (record, false, 0)
                })
                .collect::<Vec<_>>();
            let shard = encode_object_shard(&entries).unwrap();
            total_bytes += shard.bytes.len();
            writer.put(&shard).await.unwrap();
            append_object_shard_reference(&mut map, shard.reference);
        }
        let mut state = StateData::empty().unwrap();
        state.base_objects = Arc::new(map);
        let original = state.base_objects.clone();
        let mutation = MetadataMutation::install_retained_objects(retained);
        let retained_source = mutation.retained_objects().unwrap().clone();
        let reader = ObjectShardStorage::new(storage, "state".to_owned(), total_bytes * 2);

        let (collected, result, _) =
            apply_mutation(&reader, state, mutation, Some(retained_source))
                .await
                .unwrap();

        assert_eq!(result.objects_removed, 0);
        assert_eq!(collected.base_objects, original);
        assert_eq!(reader.stats().get_requests, 3);
        assert_eq!(reader.stats().put_requests, 0);
    }

    #[tokio::test]
    async fn dense_collection_puts_only_the_shard_containing_a_deletion() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(chroma_storage::Storage::Local(
            chroma_storage::local::LocalStorage::new(directory.path().to_str().unwrap()),
        ));
        let writer = ObjectShardStorage::new(storage.clone(), "state".to_owned(), 0);
        let mut map = StateShardMap::default();
        let mut retained = BTreeSet::new();
        let mut total_bytes = 0_usize;
        for group in 0..3_u32 {
            let entries = (0..10_u32)
                .map(|offset| {
                    let record = logical_record(group * 10 + offset);
                    retained.insert(record.key().clone());
                    (record, false, 0)
                })
                .collect::<Vec<_>>();
            let shard = encode_object_shard(&entries).unwrap();
            total_bytes += shard.bytes.len();
            writer.put(&shard).await.unwrap();
            append_object_shard_reference(&mut map, shard.reference);
        }
        let original = map.objects.clone();
        retained.remove(logical_record(15).key());
        let mut state = StateData::empty().unwrap();
        state.base_objects = Arc::new(map);
        let mutation = MetadataMutation::install_retained_objects(retained);
        let retained_source = mutation.retained_objects().unwrap().clone();
        let reader = ObjectShardStorage::new(storage, "state".to_owned(), total_bytes * 2);

        let (collected, result, _) =
            apply_mutation(&reader, state, mutation, Some(retained_source))
                .await
                .unwrap();

        assert_eq!(result.objects_removed, 1);
        assert_eq!(reader.stats().get_requests, 3);
        assert_eq!(reader.stats().put_requests, 1);
        assert_eq!(collected.base_objects.objects.len(), 3);
        assert_eq!(collected.base_objects.objects[0], original[0]);
        assert_ne!(collected.base_objects.objects[1], original[1]);
        assert_eq!(collected.base_objects.objects[2], original[2]);
    }

    #[test]
    fn state_encoding_rejects_truncation() {
        let state = StateData::empty().unwrap();
        let bytes = encode_state(&state).unwrap();
        assert!(matches!(
            decode_state(&bytes[..bytes.len() - 1]),
            Err(MetadataError::Corruption(_))
        ));
    }

    #[test]
    fn state_encoding_rejects_unknown_format() {
        let mut bytes = encode_state(&StateData::empty().unwrap()).unwrap();
        bytes[STATE_MAGIC_V3.len() - 2] = b'9';
        assert!(matches!(
            decode_state(&bytes),
            Err(MetadataError::Corruption(_))
        ));
    }

    #[tokio::test]
    async fn separate_handles_recover_the_shared_state_and_detect_staleness() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_str().unwrap();
        let storage = Arc::new(chroma_storage::Storage::Local(
            chroma_storage::local::LocalStorage::new(root),
        ));
        let first = Wal3MetadataStore::open(storage.clone(), "casita/state", "first")
            .await
            .unwrap();
        let initial = first.snapshot().await.unwrap().revision();
        let committed = first
            .commit(&initial, MetadataMutation::new())
            .await
            .unwrap();

        let second = Wal3MetadataStore::open(storage, "casita/state", "second")
            .await
            .unwrap();
        assert_eq!(
            second.snapshot().await.unwrap().revision(),
            committed.revision
        );
        assert!(matches!(
            second.commit(&initial, MetadataMutation::new()).await,
            Err(MetadataError::StaleRevision { .. })
        ));
    }

    #[tokio::test]
    async fn duplicate_idempotent_objects_produce_a_canonical_reopenable_delta() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(chroma_storage::Storage::Local(
            chroma_storage::local::LocalStorage::new(directory.path().to_str().unwrap()),
        ));
        let store = Wal3MetadataStore::open(storage.clone(), "casita/state", "first")
            .await
            .unwrap();
        let object = verified_blob(b"idempotent delta object").await;
        let key = object.record().key().clone();
        let mut mutation = MetadataMutation::new();
        mutation.add_object(object.clone()).add_object(object);
        let revision = store.opened_snapshot().revision();
        let committed = store.commit(&revision, mutation).await.unwrap();
        drop(store);

        let reopened = Wal3MetadataStore::open(storage, "casita/state", "second")
            .await
            .unwrap();
        let snapshot = reopened.opened_snapshot();
        assert_eq!(snapshot.revision(), committed.revision);
        assert!(snapshot.object(&key).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn reopen_replays_at_most_eight_deltas_then_ninth_commit_checkpoints() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(chroma_storage::Storage::Local(
            chroma_storage::local::LocalStorage::new(directory.path().to_str().unwrap()),
        ));
        let store = Wal3MetadataStore::open(storage.clone(), "casita/state", "first")
            .await
            .unwrap();
        let mut revision = store.opened_snapshot().revision();
        for _ in 0..MAX_TAIL_DELTAS {
            revision = store
                .commit(&revision, MetadataMutation::new())
                .await
                .unwrap()
                .revision;
        }
        let loaded = store.load_state_at_manifest().await.unwrap();
        assert_eq!(loaded.tail_deltas.len(), MAX_TAIL_DELTAS);
        assert_eq!(loaded.state.as_ref().unwrap().revision, revision);

        let reopened = Wal3MetadataStore::open(storage, "casita/state", "second")
            .await
            .unwrap();
        assert_eq!(reopened.opened_snapshot().revision(), revision);
        let committed = reopened
            .commit(&revision, MetadataMutation::new())
            .await
            .unwrap();
        let checkpoint = reopened.load_state_at_manifest().await.unwrap();
        assert!(checkpoint.tail_deltas.is_empty());
        assert_eq!(
            checkpoint.state.as_ref().unwrap().revision,
            committed.revision
        );
        assert_eq!(
            checkpoint.base_position.unwrap(),
            checkpoint.next_write_position - 1
        );
    }

    #[tokio::test]
    async fn ninth_commit_shards_objects_and_reopen_loads_them_lazily() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(chroma_storage::Storage::Local(
            chroma_storage::local::LocalStorage::new(directory.path().to_str().unwrap()),
        ));
        let store = Wal3MetadataStore::open(storage.clone(), "casita/state", "first")
            .await
            .unwrap();
        let object = verified_blob(b"sharded checkpoint object").await;
        let key = object.record().key().clone();
        let mut mutation = MetadataMutation::new();
        mutation.add_object(object);
        let mut revision = store
            .commit(&store.opened_snapshot().revision(), mutation)
            .await
            .unwrap()
            .revision;
        for _ in 1..=MAX_TAIL_DELTAS {
            revision = store
                .commit(&revision, MetadataMutation::new())
                .await
                .unwrap()
                .revision;
        }
        let loaded = store.load_state_at_manifest().await.unwrap();
        let state = loaded.state.as_ref().unwrap();
        assert!(loaded.tail_deltas.is_empty());
        assert_eq!(state.base_objects.object_count, 1);
        assert!(state.objects.is_empty());
        assert!(loaded.checkpoint_bytes < 1_024);
        drop(store);

        let reopened = Wal3MetadataStore::open(storage, "casita/state", "second")
            .await
            .unwrap();
        reopened.reset_read_stats();
        let snapshot = reopened.opened_snapshot();
        assert_eq!(snapshot.revision(), revision);
        assert!(snapshot.object(&key).await.unwrap().is_some());
        assert!(snapshot.object(&key).await.unwrap().is_some());
        let stats = reopened.read_stats();
        assert_eq!(stats.logical_shard_get_requests, 1);
        assert_eq!(stats.logical_shard_cache_hits, 1);
        assert!(stats.logical_shard_get_bytes > 0);
    }

    #[tokio::test]
    async fn wal_snapshot_replay_and_commit_read_shards_through_a_logical_prune_fence() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(chroma_storage::Storage::Local(
            chroma_storage::local::LocalStorage::new(directory.path().to_str().unwrap()),
        ));
        let store = Wal3MetadataStore::open(storage, "fenced/state", "writer")
            .await
            .unwrap();
        let object = verified_blob(b"fenced metadata read").await;
        let key = object.record().key().clone();
        let mut mutation = MetadataMutation::new();
        mutation.add_object(object);
        let mut revision = store
            .commit(&store.opened_snapshot().revision(), mutation)
            .await
            .unwrap()
            .revision;
        for _ in 0..MAX_TAIL_DELTAS {
            revision = store
                .commit(&revision, MetadataMutation::new())
                .await
                .unwrap()
                .revision;
        }
        assert!(
            !store
                .snapshot()
                .await
                .unwrap()
                .retention_resources()
                .is_empty()
        );
        // This delta needs the existing object shard during replay.
        let mut mutation = MetadataMutation::new();
        mutation.set_root("held/root".parse().unwrap(), key.clone());
        revision = store.commit(&revision, mutation).await.unwrap().revision;
        super::super::flush_pin_releases().await;
        let ledger = store.pin_store().await.unwrap();
        let fence = ledger
            .begin_prune(ledger.inventory().await.unwrap().revision)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            let snapshot = store.snapshot().await.unwrap();
            assert_eq!(snapshot.revision(), revision);
            assert!(snapshot.object(&key).await.unwrap().is_some());
            // Retain the object so the still-named root remains valid.
            store
                .commit(
                    &revision,
                    MetadataMutation::install_retained_objects(BTreeSet::from([key])),
                )
                .await
                .unwrap();
        })
        .await
        .expect("metadata reads must not wait on their own logical prune fence");
        assert_eq!(
            ledger.inventory().await.unwrap().logical_prune,
            Some(fence.clone())
        );
        ledger.finish_prune(&fence).await.unwrap();
        super::super::flush_pin_releases().await;
        assert!(ledger.inventory().await.unwrap().pins.is_empty());
    }

    #[tokio::test]
    async fn metadata_deletion_recovery_keeps_claims_through_cancelled_io() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let store = Wal3MetadataStore::open(
            rustfs_storage(&rustfs).await,
            "recover-wal/state",
            "recoverer",
        )
        .await
        .unwrap();
        let orphan = encode_object_shard(&[(
            verified_blob(b"recover metadata orphan")
                .await
                .record()
                .clone(),
            false,
            0,
        )])
        .unwrap();
        store.object_shards.put(&orphan).await.unwrap();
        super::super::flush_pin_releases().await;
        let path = store
            .object_shards
            .list_paths()
            .await
            .unwrap()
            .pop()
            .unwrap();
        let pins = store.pin_store().await.unwrap();
        let claim = pins
            .claim_deletions(
                pins.inventory().await.unwrap().revision,
                BTreeSet::from([super::super::PinResource::MetadataObject(path.clone())]),
            )
            .await
            .unwrap()
            .unwrap();
        let claims = BTreeSet::from([claim.clone()]);
        let wrong = BTreeSet::from(["11".repeat(32).parse().unwrap()]);
        assert!(store.recover_wal_deletions(wrong).await.is_err());
        crate::flush_repository_leases().await.unwrap();
        let before = pins.inventory().await.unwrap();
        let pause = store.object_shards.pause_deletion();
        let recoverer = store.clone();
        let requested = claims.clone();
        let task = tokio::spawn(async move { recoverer.recover_wal_deletions(requested).await });
        tokio::time::timeout(Duration::from_secs(10), pause.reached.notified())
            .await
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), crate::flush_repository_leases())
                .await
                .is_err()
        );
        assert_eq!(pins.inventory().await.unwrap().deletions, before.deletions);
        assert!(matches!(
            store.object_shards.put(&orphan).await,
            Err(MetadataError::Transient(_))
        ));
        assert!(store.try_collection_lease().await.unwrap().is_none());
        pause.resume.notify_one();
        tokio::time::timeout(Duration::from_secs(10), crate::flush_repository_leases())
            .await
            .unwrap()
            .unwrap();
        assert!(pins.inventory().await.unwrap().deletions.is_empty());
        assert!(
            !store
                .object_shards
                .list_paths()
                .await
                .unwrap()
                .contains(&path)
        );
        assert!(store.repository_holds().await.unwrap().is_empty());
        assert!(store.recover_wal_deletions(claims).await.is_err());
        crate::flush_repository_leases().await.unwrap();
    }

    #[tokio::test]
    async fn coordination_metadata_deletion_recovery_uses_its_own_ledger() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let parent = Wal3MetadataStore::open(
            rustfs_storage(&rustfs).await,
            "recover-coordination/state",
            "recoverer",
        )
        .await
        .unwrap();
        let coordination = parent.repository_coordination().await.unwrap();
        let log = coordination.metadata_store();
        let orphan = encode_root_shard(&[RootRecord::new(
            "obsolete/root".parse().unwrap(),
            verified_blob(b"obsolete anchor")
                .await
                .record()
                .key()
                .clone(),
        )])
        .unwrap();
        log.object_shards.put_root(&orphan).await.unwrap();
        super::super::flush_pin_releases().await;
        let path = log.object_shards.list_paths().await.unwrap().pop().unwrap();
        let pins = log.pin_store().await.unwrap();
        let claim = pins
            .claim_deletions(
                pins.inventory().await.unwrap().revision,
                BTreeSet::from([super::super::PinResource::MetadataObject(path.clone())]),
            )
            .await
            .unwrap()
            .unwrap();
        let claims = BTreeSet::from([claim]);
        assert!(parent.recover_wal_deletions(claims.clone()).await.is_err());
        crate::flush_repository_leases().await.unwrap();
        parent
            .recover_repository_coordination_deletions(claims)
            .await
            .unwrap();
        crate::flush_repository_leases().await.unwrap();
        assert!(
            !log.object_shards
                .list_paths()
                .await
                .unwrap()
                .contains(&path)
        );
        assert!(
            parent
                .repository_coordination_pin_store()
                .await
                .unwrap()
                .inventory()
                .await
                .unwrap()
                .deletions
                .is_empty()
        );
        assert!(parent.repository_holds().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn cancelled_metadata_gc_keeps_its_claim_until_deletion_settles() {
        use crate::metadata::{DataPin, PinResource, PinScope};
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let store = Wal3MetadataStore::open(storage, "cancel-metadata/state", "collector")
            .await
            .unwrap();
        let orphan = encode_object_shard(&[(
            verified_blob(b"cancelled metadata deletion")
                .await
                .record()
                .clone(),
            false,
            0,
        )])
        .unwrap();
        let orphan_path = store
            .object_shards
            .referenced_paths(&StateShardMap {
                objects: vec![orphan.reference.clone()],
                object_count: 1,
                ..Default::default()
            })
            .into_iter()
            .next()
            .unwrap();
        store.object_shards.put(&orphan).await.unwrap();
        let pause = store.object_shards.pause_deletion();
        let collector = store.clone();
        let task = tokio::spawn(async move { collector.collect_wal(Duration::ZERO).await });
        tokio::time::timeout(Duration::from_secs(10), pause.reached.notified())
            .await
            .unwrap();
        let pins = store.pin_store().await.unwrap();
        let claimed = pins.inventory().await.unwrap().deletions;
        assert_eq!(claimed.len(), 1);
        assert!(
            claimed
                .values()
                .next()
                .unwrap()
                .contains(&PinResource::MetadataObject(orphan_path.clone()))
        );
        assert!(
            pins.register(DataPin {
                scope: PinScope::Staging,
                catalog: None,
                resources: BTreeSet::from([PinResource::MetadataObject(orphan_path.clone())]),
            })
            .await
            .unwrap()
            .is_none()
        );
        let unrelated = pins
            .register(DataPin {
                scope: PinScope::Staging,
                catalog: None,
                resources: BTreeSet::from([PinResource::MetadataObject(
                    "unrelated/state-shard".into(),
                )]),
            })
            .await
            .unwrap()
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), crate::flush_repository_leases())
                .await
                .is_err()
        );
        assert_eq!(pins.inventory().await.unwrap().deletions, claimed);
        assert!(
            store
                .object_shards
                .list_paths()
                .await
                .unwrap()
                .contains(&orphan_path)
        );
        pause.resume.notify_one();
        tokio::time::timeout(Duration::from_secs(10), crate::flush_repository_leases())
            .await
            .unwrap()
            .unwrap();
        assert!(pins.inventory().await.unwrap().deletions.is_empty());
        assert!(
            pins.inventory()
                .await
                .unwrap()
                .pins
                .contains_key(&unrelated)
        );
        assert!(
            !store
                .object_shards
                .list_paths()
                .await
                .unwrap()
                .contains(&orphan_path)
        );
        assert!(store.repository_holds().await.unwrap().is_empty());
        pins.release(&unrelated).await.unwrap();
    }

    #[tokio::test]
    async fn metadata_gc_retains_pinned_lazy_snapshot_shards_and_collects_unrelated_files() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let store = Wal3MetadataStore::open(storage.clone(), "casita/state", "collector")
            .await
            .unwrap();
        let object = verified_blob(b"old lazy snapshot").await;
        let key = object.record().key().clone();
        let name: RootName = "old/root".parse().unwrap();
        let mut mutation = MetadataMutation::new();
        mutation
            .add_object(object)
            .set_root(name.clone(), key.clone());
        let mut revision = store
            .commit(&store.opened_snapshot().revision(), mutation)
            .await
            .unwrap()
            .revision;
        for _ in 0..MAX_TAIL_DELTAS {
            revision = store
                .commit(&revision, MetadataMutation::new())
                .await
                .unwrap()
                .revision;
        }
        // A separate handle has not cached the object or root shard contents.
        let reader = Wal3MetadataStore::open(storage, "casita/state", "reader")
            .await
            .unwrap();
        let (snapshot, pin) = super::super::read_snapshot(&reader).await.unwrap();
        assert!(pin.is_some());
        let resources = snapshot.retention_resources();
        assert!(
            resources.len() >= 2,
            "object and root shards must both be pinned"
        );
        let inventory = store.pin_store().await.unwrap().inventory().await.unwrap();
        assert!(inventory.pins.values().any(|pin| {
            pin.scope == super::super::PinScope::Metadata && pin.resources == resources
        }));
        // Model metadata becoming obsolete without retaining its logical graph.
        let mut remove = MetadataMutation::new();
        remove.remove_root(name.clone());
        revision = store.commit(&revision, remove).await.unwrap().revision;
        store
            .commit(
                &revision,
                MetadataMutation::install_retained_objects(BTreeSet::new()),
            )
            .await
            .unwrap();
        let orphan = encode_object_shard(&[(
            verified_blob(b"unrelated metadata garbage")
                .await
                .record()
                .clone(),
            false,
            0,
        )])
        .unwrap();
        let orphan_path = store
            .object_shards
            .referenced_paths(&StateShardMap {
                objects: vec![orphan.reference.clone()],
                object_count: 1,
                ..Default::default()
            })
            .into_iter()
            .next()
            .unwrap();
        store.object_shards.put(&orphan).await.unwrap();
        store.collect_wal(Duration::ZERO).await.unwrap();
        let remaining = store.object_shards.list_paths().await.unwrap();
        assert!(!remaining.contains(&orphan_path));
        for resource in &resources {
            let super::super::PinResource::MetadataObject(path) = resource else {
                panic!("unexpected resource")
            };
            assert!(remaining.contains(path));
        }
        assert_eq!(snapshot.root(&name).await.unwrap(), Some(key.clone()));
        assert!(snapshot.object(&key).await.unwrap().is_some());
        assert!(
            store
                .pin_store()
                .await
                .unwrap()
                .inventory()
                .await
                .unwrap()
                .deletions
                .is_empty()
        );
        drop(snapshot);
        drop(pin);
        super::super::flush_pin_releases().await;
        store.collect_wal(Duration::ZERO).await.unwrap();
        let remaining = store.object_shards.list_paths().await.unwrap();
        for resource in &resources {
            let super::super::PinResource::MetadataObject(path) = resource else {
                unreachable!()
            };
            assert!(!remaining.contains(path));
        }
    }

    #[tokio::test]
    async fn collect_wal_deletes_orphaned_shards_and_retains_live_state() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let store = Wal3MetadataStore::open(storage.clone(), "casita/state", "collector")
            .await
            .unwrap();
        let live_object = verified_blob(b"live logical shard object").await;
        let live_key = live_object.record().key().clone();
        let mut mutation = MetadataMutation::new();
        mutation.add_object(live_object);
        let mut revision = store
            .commit(&store.opened_snapshot().revision(), mutation)
            .await
            .unwrap()
            .revision;
        for _ in 0..MAX_TAIL_DELTAS {
            revision = store
                .commit(&revision, MetadataMutation::new())
                .await
                .unwrap()
                .revision;
        }
        let checkpoint = store.load_state_at_manifest().await.unwrap();
        assert!(checkpoint.tail_deltas.is_empty());
        let live_paths = store
            .object_shards
            .referenced_paths(checkpoint.state.as_ref().unwrap().base_objects.as_ref());
        assert!(!live_paths.is_empty());

        let orphan_object = verified_blob(b"unpublished logical shard object").await;
        let orphan = encode_object_shard(&[(orphan_object.record().clone(), false, 0)]).unwrap();
        let orphan_map = StateShardMap {
            objects: vec![orphan.reference.clone()],
            object_count: 1,
            ..StateShardMap::default()
        };
        let orphan_path = store
            .object_shards
            .referenced_paths(&orphan_map)
            .into_iter()
            .next()
            .unwrap();
        store.object_shards.put(&orphan).await.unwrap();
        assert!(
            store
                .object_shards
                .list_paths()
                .await
                .unwrap()
                .contains(&orphan_path)
        );

        store.reset_read_stats();
        store.collect_wal(Duration::ZERO).await.unwrap();

        let gc_stats = store.read_stats();
        assert_eq!(gc_stats.logical_shard_barrier_get_requests, 3);
        assert_eq!(gc_stats.logical_shard_barrier_put_requests, 2);
        assert_eq!(gc_stats.logical_shard_inventory_list_requests, 2);
        assert_eq!(gc_stats.logical_shard_delete_requests, 1);
        let remaining = store.object_shards.list_paths().await.unwrap();
        assert!(!remaining.contains(&orphan_path));
        assert!(live_paths.iter().all(|path| remaining.contains(path)));
        assert_eq!(store.snapshot().await.unwrap().revision(), revision);
        assert!(
            store
                .snapshot()
                .await
                .unwrap()
                .object(&live_key)
                .await
                .unwrap()
                .is_some()
        );
        drop(store);

        let reopened = Wal3MetadataStore::open(storage, "casita/state", "reopened")
            .await
            .unwrap();
        assert_eq!(reopened.opened_snapshot().revision(), revision);
        assert!(
            reopened
                .opened_snapshot()
                .object(&live_key)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn cancelled_checkpoint_keeps_its_write_pin_until_publication_settles() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let mut writer = Wal3MetadataStore::open(storage.clone(), "casita/state", "delayed")
            .await
            .unwrap();
        let mut expected = writer.opened_snapshot().revision();
        for _ in 0..MAX_TAIL_DELTAS {
            expected = writer
                .commit(&expected, MetadataMutation::new())
                .await
                .unwrap()
                .revision;
        }
        let collector = Wal3MetadataStore::open(storage, "casita/state", "collector")
            .await
            .unwrap();
        let object = verified_blob(b"checkpoint publication race").await;
        let key = object.record().key().clone();
        let birth = writer.snapshot().await.unwrap().generation().unwrap() + 1;
        let encoded = encode_object_shard(&[(object.record().clone(), false, birth)]).unwrap();
        let orphan_path = writer
            .object_shards
            .referenced_paths(&StateShardMap {
                objects: vec![encoded.reference],
                object_count: 1,
                ..StateShardMap::default()
            })
            .into_iter()
            .next()
            .unwrap();
        let pause = Arc::new(CheckpointPause::default());
        writer.checkpoint_pause = Some(pause.clone());
        let delayed = writer.clone();
        let delayed_object = object.clone();
        let commit = tokio::spawn(async move {
            let mut mutation = MetadataMutation::new();
            mutation.add_object(delayed_object);
            delayed.commit(&expected, mutation).await
        });

        pause.reached.notified().await;
        commit.abort();
        assert!(commit.await.unwrap_err().is_cancelled());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), crate::flush_repository_leases())
                .await
                .is_err()
        );
        assert!(
            writer
                .object_shards
                .list_paths()
                .await
                .unwrap()
                .contains(&orphan_path)
        );
        collector.collect_wal(Duration::ZERO).await.unwrap();
        assert!(
            writer
                .object_shards
                .list_paths()
                .await
                .unwrap()
                .contains(&orphan_path)
        );
        pause.resume.notify_one();

        crate::flush_repository_leases().await.unwrap();
        collector.collect_wal(Duration::ZERO).await.unwrap();
        assert!(
            !writer
                .object_shards
                .list_paths()
                .await
                .unwrap()
                .contains(&orphan_path),
            "a settled, fenced-out write must no longer retain its candidate"
        );
        let stable = collector.snapshot().await.unwrap();
        assert_eq!(stable.revision(), expected);
        assert!(stable.object(&key).await.unwrap().is_none());

        writer.checkpoint_pause = None;
        let mut retry = MetadataMutation::new();
        retry.add_object(object);
        let committed = writer.commit(&expected, retry).await.unwrap();
        let snapshot = writer.snapshot().await.unwrap();
        assert_eq!(snapshot.revision(), committed.revision);
        assert!(snapshot.object(&key).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn gc_fences_collection_shards_uploaded_before_publication() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let mut writer = Wal3MetadataStore::open(storage.clone(), "casita/state", "collector")
            .await
            .unwrap();
        let keep = verified_blob(b"retained across collection race").await;
        let remove = verified_blob(b"removed by collection race").await;
        let keep_key = keep.record().key().clone();
        let remove_key = remove.record().key().clone();
        let mut mutation = MetadataMutation::new();
        mutation.add_object(keep.clone()).add_object(remove);
        let mut expected = writer
            .commit(&writer.opened_snapshot().revision(), mutation)
            .await
            .unwrap()
            .revision;
        for _ in 0..MAX_TAIL_DELTAS {
            expected = writer
                .commit(&expected, MetadataMutation::new())
                .await
                .unwrap()
                .revision;
        }
        let maintenance = Wal3MetadataStore::open(storage, "casita/state", "maintenance")
            .await
            .unwrap();
        // Collection preserves the retained object's original first-commit birth.
        let replacement = encode_object_shard(&[(keep.record().clone(), false, 1)]).unwrap();
        let replacement_path = writer
            .object_shards
            .referenced_paths(&StateShardMap {
                objects: vec![replacement.reference],
                object_count: 1,
                ..StateShardMap::default()
            })
            .into_iter()
            .next()
            .unwrap();
        let pause = Arc::new(CheckpointPause::default());
        writer.checkpoint_pause = Some(pause.clone());
        let delayed = writer.clone();
        let retained = BTreeSet::from([keep_key.clone()]);
        let commit = tokio::spawn(async move {
            delayed
                .commit(
                    &expected,
                    MetadataMutation::install_retained_objects(retained),
                )
                .await
        });

        pause.reached.notified().await;
        assert!(
            writer
                .object_shards
                .list_paths()
                .await
                .unwrap()
                .contains(&replacement_path)
        );
        maintenance.collect_wal(Duration::ZERO).await.unwrap();
        assert!(
            writer
                .object_shards
                .list_paths()
                .await
                .unwrap()
                .contains(&replacement_path)
        );
        pause.resume.notify_one();
        let outcome = commit.await.unwrap();
        assert!(
            matches!(outcome, Err(MetadataError::Transient(_))),
            "unexpected delayed collection outcome: {outcome:?}"
        );
        crate::flush_repository_leases().await.unwrap();
        maintenance.collect_wal(Duration::ZERO).await.unwrap();
        assert!(
            !writer
                .object_shards
                .list_paths()
                .await
                .unwrap()
                .contains(&replacement_path),
            "a settled, fenced-out write must no longer retain its candidate"
        );
        let stable = maintenance.snapshot().await.unwrap();
        assert!(stable.object(&keep_key).await.unwrap().is_some());
        assert!(stable.object(&remove_key).await.unwrap().is_some());

        writer.checkpoint_pause = None;
        let committed = writer
            .commit(
                &expected,
                MetadataMutation::install_retained_objects(BTreeSet::from([keep_key.clone()])),
            )
            .await
            .unwrap();
        let collected = writer.snapshot().await.unwrap();
        assert_eq!(collected.revision(), committed.revision);
        assert!(collected.object(&keep_key).await.unwrap().is_some());
        assert!(collected.object(&remove_key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn newer_gc_lease_fences_an_older_maintenance_runner() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let first = Wal3MetadataStore::open(storage.clone(), "casita/state", "first-gc")
            .await
            .unwrap();
        let second = Wal3MetadataStore::open(storage, "casita/state", "second-gc")
            .await
            .unwrap();

        let old = first.object_shards.acquire_gc_barrier().await.unwrap();
        let current = second.object_shards.acquire_gc_barrier().await.unwrap();
        assert!(!first.object_shards.owns_barrier(&old).await.unwrap());
        assert!(second.object_shards.owns_barrier(&current).await.unwrap());
        first.object_shards.release_barrier(&old).await.unwrap();
        assert!(second.object_shards.owns_barrier(&current).await.unwrap());
        second
            .object_shards
            .release_barrier(&current)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn repository_retries_checkpoint_admission_after_gc_releases_barrier() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let store = Wal3MetadataStore::open(storage.clone(), "retry/state", "checkpoint-writer")
            .await
            .unwrap();
        let mut revision = store.opened_snapshot().revision();
        for _ in 0..MAX_TAIL_DELTAS {
            revision = store
                .commit(&revision, MetadataMutation::new())
                .await
                .unwrap()
                .revision;
        }
        let gc = store.object_shards.acquire_gc_barrier().await.unwrap();
        store.reset_read_stats();
        let payloads = crate::MemoryBlobStore::new();
        let repository = Arc::new(crate::repository::Repository::new(payloads.clone(), store));
        let writer = repository.clone();
        let name = RootName::try_from("checkpoint/retry").unwrap();
        let output = name.clone();
        let publishing = tokio::spawn(async move {
            let session = writer.mutation_session().await.unwrap();
            let object = session
                .stage_blob(b"retry across GC barrier")
                .await
                .unwrap();
            let key = object.record().key().clone();
            session
                .publish_rooted(vec![object], output, key.clone())
                .await
                .unwrap();
            key
        });
        tokio::time::timeout(Duration::from_secs(10), async {
            while repository
                .metadata()
                .read_stats()
                .logical_shard_barrier_get_requests
                < 2
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(!publishing.is_finished());
        assert_eq!(repository.metadata().read_stats().fragment_put_requests, 0);
        repository
            .metadata()
            .object_shards
            .release_barrier(&gc)
            .await
            .unwrap();
        let key = publishing.await.unwrap();
        crate::flush_repository_leases().await.unwrap();
        let reopened = Wal3MetadataStore::open(storage, "retry/state", "fresh-reader")
            .await
            .unwrap();
        assert_eq!(
            reopened
                .snapshot()
                .await
                .unwrap()
                .root(&name)
                .await
                .unwrap(),
            Some(key.clone())
        );
        let reader = crate::repository::Repository::new(payloads, reopened);
        let (_, mut stream) = reader.open_payload(&key).await.unwrap().unwrap();
        let mut bytes = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut bytes)
            .await
            .unwrap();
        assert_eq!(bytes, b"retry across GC barrier");
    }

    #[tokio::test]
    async fn steady_checkpoint_barrier_costs_one_get_and_two_puts() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let store = Wal3MetadataStore::open(storage, "casita/state", "request-counter")
            .await
            .unwrap();
        let mut revision = store.opened_snapshot().revision();

        // Cross the first checkpoint boundary to initialize the barrier marker.
        for _ in 0..=MAX_TAIL_DELTAS {
            revision = store
                .commit(&revision, MetadataMutation::new())
                .await
                .unwrap()
                .revision;
        }
        store.reset_read_stats();

        for _ in 0..MAX_TAIL_DELTAS {
            revision = store
                .commit(&revision, MetadataMutation::new())
                .await
                .unwrap()
                .revision;
        }
        let delta_stats = store.read_stats();
        assert_eq!(delta_stats.logical_shard_barrier_get_requests, 0);
        assert_eq!(delta_stats.logical_shard_barrier_put_requests, 0);

        revision = store
            .commit(&revision, MetadataMutation::new())
            .await
            .unwrap()
            .revision;
        let checkpoint_stats = store.read_stats();
        assert_eq!(checkpoint_stats.fragment_put_requests, 9);
        assert_eq!(checkpoint_stats.manifest_put_requests, 9);
        assert_eq!(checkpoint_stats.logical_shard_barrier_get_requests, 1);
        assert_eq!(checkpoint_stats.logical_shard_barrier_put_requests, 2);
        assert_eq!(store.snapshot().await.unwrap().revision(), revision);
    }

    #[tokio::test]
    async fn collection_pages_retained_state_without_materializing_a_global_set() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(chroma_storage::Storage::Local(
            chroma_storage::local::LocalStorage::new(directory.path().to_str().unwrap()),
        ));
        let store = Wal3MetadataStore::open(storage, "casita/state", "collector")
            .await
            .unwrap();
        let mut objects = Vec::new();
        for index in 0..1_300_u32 {
            objects.push(verified_blob(&index.to_le_bytes()).await);
        }
        let mut retained = objects
            .iter()
            .map(|object| object.record().key().clone())
            .collect::<Vec<_>>();
        retained.sort();
        retained.truncate(1_100);
        let root = RootName::try_from("main").unwrap();
        let mut mutation = MetadataMutation::new();
        mutation
            .add_objects(objects)
            .set_root(root.clone(), retained[0].clone());
        let revision = store
            .commit(&store.opened_snapshot().revision(), mutation)
            .await
            .unwrap()
            .revision;
        let page_calls = Arc::new(AtomicU64::new(0));
        let contains_calls = Arc::new(AtomicU64::new(0));
        let source = Arc::new(ProbedRetained {
            keys: retained.clone(),
            page_calls: page_calls.clone(),
            contains_calls: contains_calls.clone(),
        });
        let collected = store
            .commit(&revision, MetadataMutation::install_retained_source(source))
            .await
            .unwrap();
        assert_eq!(collected.objects_removed, 200);
        assert_eq!(page_calls.load(Ordering::Relaxed), 3);
        assert!(contains_calls.load(Ordering::Relaxed) >= 1);

        let snapshot = store.snapshot().await.unwrap();
        assert_eq!(
            snapshot.root(&root).await.unwrap(),
            Some(retained[0].clone())
        );
        let records = snapshot.objects().try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(records.len(), 1_100);
        assert!(records.windows(2).all(|pair| pair[0].key() < pair[1].key()));
        assert_eq!(
            records
                .iter()
                .map(|record| record.key().clone())
                .collect::<Vec<_>>(),
            retained
        );
    }

    #[tokio::test]
    async fn root_shard_tombstones_survive_checkpoint_and_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(chroma_storage::Storage::Local(
            chroma_storage::local::LocalStorage::new(directory.path().to_str().unwrap()),
        ));
        let store = Wal3MetadataStore::open(storage.clone(), "casita/state", "first")
            .await
            .unwrap();
        let object = verified_blob(b"root shard target").await;
        let key = object.record().key().clone();
        let name = RootName::try_from("main").unwrap();
        let mut mutation = MetadataMutation::new();
        mutation
            .add_object(object)
            .set_root(name.clone(), key.clone());
        let mut revision = store
            .commit(&store.opened_snapshot().revision(), mutation)
            .await
            .unwrap()
            .revision;
        for _ in 0..MAX_TAIL_DELTAS {
            revision = store
                .commit(&revision, MetadataMutation::new())
                .await
                .unwrap()
                .revision;
        }
        drop(store);

        let reopened = Wal3MetadataStore::open(storage.clone(), "casita/state", "second")
            .await
            .unwrap();
        reopened.reset_read_stats();
        assert_eq!(
            reopened.opened_snapshot().root(&name).await.unwrap(),
            Some(key)
        );
        assert_eq!(reopened.read_stats().logical_shard_get_requests, 1);
        let mut removal = MetadataMutation::new();
        removal.remove_root(name.clone());
        revision = reopened.commit(&revision, removal).await.unwrap().revision;
        assert_eq!(reopened.opened_snapshot().root(&name).await.unwrap(), None);
        for _ in 0..MAX_TAIL_DELTAS {
            revision = reopened
                .commit(&revision, MetadataMutation::new())
                .await
                .unwrap()
                .revision;
        }
        drop(reopened);

        let final_store = Wal3MetadataStore::open(storage, "casita/state", "third")
            .await
            .unwrap();
        let state = final_store
            .checkpoint
            .lock()
            .unwrap()
            .clone()
            .unwrap()
            .state;
        assert_eq!(state.revision, revision);
        assert_eq!(state.root_count, 0);
        assert!(state.base_objects.roots.is_empty());
        assert_eq!(
            final_store.opened_snapshot().root(&name).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn racing_initializers_publish_exactly_one_initial_checkpoint() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;

        let (first, second) = tokio::join!(
            Wal3MetadataStore::open(storage.clone(), "casita/state", "first"),
            Wal3MetadataStore::open(storage, "casita/state", "second")
        );
        let first = first.unwrap();
        let second = second.unwrap();
        let revision = first.snapshot().await.unwrap().revision();

        assert_eq!(second.snapshot().await.unwrap().revision(), revision);
        assert_eq!(
            first
                .reader()
                .await
                .unwrap()
                .next_write_timestamp()
                .await
                .unwrap()
                .offset(),
            2
        );
    }

    #[tokio::test]
    async fn unchanged_manifest_reuses_the_validated_checkpoint() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let store = Wal3MetadataStore::open(storage, "casita/state", "reader")
            .await
            .unwrap();
        let cached_before = store
            .checkpoint
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .state
            .clone();
        let open_stats = store.read_stats();
        assert_eq!(open_stats.fragment_get_requests, 0);
        assert_eq!(open_stats.fragment_get_bytes, 0);
        assert!(open_stats.checkpoint_bytes > 0);
        assert_eq!(open_stats.checkpoint_objects, 0);
        assert_eq!(open_stats.checkpoint_roots, 0);
        store.reset_read_stats();

        assert_eq!(store.opened_snapshot().revision(), cached_before.revision);
        let seeded_stats = store.read_stats();
        assert_eq!(seeded_stats.manifest_refresh_requests, 0);
        assert_eq!(seeded_stats.fragment_get_requests, 0);

        assert_eq!(
            store.snapshot().await.unwrap().revision(),
            cached_before.revision
        );
        let snapshot_stats = store.read_stats();
        assert_eq!(snapshot_stats.manifest_refresh_requests, 1);
        assert_eq!(snapshot_stats.checkpoint_cache_hits, 1);
        assert_eq!(snapshot_stats.checkpoint_cache_misses, 0);
        assert_eq!(snapshot_stats.manifest_load_requests, 0);
        assert_eq!(snapshot_stats.fragment_get_requests, 0);

        let cached_after = store
            .checkpoint
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .state
            .clone();
        assert!(Arc::ptr_eq(&cached_before, &cached_after));

        store.reset_read_stats();
        store
            .commit(&cached_after.revision, MetadataMutation::new())
            .await
            .unwrap();
        let commit_stats = store.read_stats();
        assert_eq!(commit_stats.manifest_refresh_requests, 0);
        assert_eq!(commit_stats.manifest_load_requests, 0);
        assert_eq!(commit_stats.fragment_get_requests, 0);
        assert_eq!(commit_stats.fragment_put_requests, 1);
        assert_eq!(commit_stats.manifest_put_requests, 1);
    }

    #[tokio::test]
    async fn changed_manifest_refreshes_the_cached_checkpoint() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let reader = Wal3MetadataStore::open(storage.clone(), "casita/state", "reader")
            .await
            .unwrap();
        let writer = Wal3MetadataStore::open(storage, "casita/state", "writer")
            .await
            .unwrap();
        let expected = reader.snapshot().await.unwrap().revision();
        let cached_before = reader
            .checkpoint
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .state
            .clone();
        let committed = writer
            .commit(&expected, MetadataMutation::new())
            .await
            .unwrap();
        reader.reset_read_stats();

        assert_eq!(
            reader.snapshot().await.unwrap().revision(),
            committed.revision
        );
        let refresh_stats = reader.read_stats();
        assert_eq!(refresh_stats.manifest_refresh_requests, 1);
        assert_eq!(refresh_stats.checkpoint_cache_hits, 0);
        assert_eq!(refresh_stats.checkpoint_cache_misses, 1);
        assert_eq!(refresh_stats.manifest_load_requests, 1);
        // One GET for the cumulative delta tail and one for its immutable
        // checkpoint, independent of the number of deltas in that tail.
        assert_eq!(refresh_stats.fragment_get_requests, 2);
        assert_eq!(refresh_stats.fragment_records, 2);
        assert!(refresh_stats.fragment_record_bytes >= refresh_stats.checkpoint_bytes);
        assert!(refresh_stats.fragment_get_bytes >= refresh_stats.checkpoint_bytes);

        let cached_after = reader
            .checkpoint
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .state
            .clone();
        assert!(!Arc::ptr_eq(&cached_before, &cached_after));
    }

    #[tokio::test]
    async fn cloned_handle_serializes_same_revision_commits() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(chroma_storage::Storage::Local(
            chroma_storage::local::LocalStorage::new(directory.path().to_str().unwrap()),
        ));
        let store = Wal3MetadataStore::open(storage, "casita/state", "writer")
            .await
            .unwrap();
        let clone = store.clone();
        let expected = store.snapshot().await.unwrap().revision();

        let (left, right) = tokio::join!(
            store.commit(&expected, MetadataMutation::new()),
            clone.commit(&expected, MetadataMutation::new())
        );
        let outcomes = [left, right];
        assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
        assert!(
            outcomes
                .iter()
                .any(|outcome| matches!(outcome, Err(MetadataError::StaleRevision { .. })))
        );
    }

    #[tokio::test]
    async fn high_fanout_cloned_commits_have_one_durable_winner() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(chroma_storage::Storage::Local(
            chroma_storage::local::LocalStorage::new(directory.path().to_str().unwrap()),
        ));
        let store = Wal3MetadataStore::open(storage, "casita/state", "writer")
            .await
            .unwrap();
        let expected = store.snapshot().await.unwrap().revision();
        let outcomes = futures::future::join_all((0..8).map(|_| {
            let store = store.clone();
            boxed_wal_future(async move { store.commit(&expected, MetadataMutation::new()).await })
        }))
        .await;
        let winners = outcomes
            .iter()
            .filter_map(|outcome| outcome.as_ref().ok())
            .collect::<Vec<_>>();

        assert_eq!(winners.len(), 1, "unexpected commit outcomes: {outcomes:?}");
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, Err(MetadataError::StaleRevision { .. })))
                .count(),
            7,
            "unexpected commit outcomes: {outcomes:?}"
        );
        assert_eq!(
            store.snapshot().await.unwrap().revision(),
            winners[0].revision
        );
    }

    #[tokio::test]
    async fn tail_checkpoint_is_selected_from_a_multi_record_fragment() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let store = Wal3MetadataStore::open(storage, "casita/state", "writer")
            .await
            .unwrap();
        let loaded = store.load_state_at_manifest().await.unwrap();
        let first = StateData::empty().unwrap();
        let second = StateData::empty().unwrap();
        store
            .writer
            .append_many_with_options(
                vec![
                    encode_state(&first).unwrap(),
                    encode_state(&second).unwrap(),
                ],
                Some(
                    wal3::AppendOptions::default()
                        .with_required_fragment_start(loaded.next_write_position),
                ),
                None,
            )
            .await
            .unwrap();

        assert_eq!(store.snapshot().await.unwrap().revision(), second.revision);
        let committed = store
            .commit(&second.revision, MetadataMutation::new())
            .await
            .unwrap();
        assert_eq!(
            committed.revision,
            store.snapshot().await.unwrap().revision()
        );
    }

    #[tokio::test]
    async fn commit_conditions_on_the_manifest_that_supplied_its_state() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let delayed = Wal3MetadataStore::open(storage.clone(), "casita/state", "delayed")
            .await
            .unwrap();
        let winner = Wal3MetadataStore::open(storage, "casita/state", "winner")
            .await
            .unwrap();
        let expected = delayed.snapshot().await.unwrap().revision();
        let loaded = delayed.load_state_at_manifest().await.unwrap();
        let committed = winner
            .commit(&expected, MetadataMutation::new())
            .await
            .unwrap();

        let outcome = delayed
            .commit_loaded(expected, MetadataMutation::new(), None, loaded)
            .await;
        assert!(
            matches!(
                outcome,
                Err(MetadataError::StaleRevision { actual, .. }) if actual == committed.revision
            ),
            "unexpected delayed commit outcome: {outcome:?}"
        );
    }

    #[tokio::test]
    async fn collect_wal_replaces_the_delta_tail_with_a_latest_fence() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let store = Wal3MetadataStore::open(storage, "casita/state", "maintenance")
            .await
            .unwrap();
        let mut expected = store.snapshot().await.unwrap().revision();
        for _ in 0..3 {
            expected = store
                .commit(&expected, MetadataMutation::new())
                .await
                .unwrap()
                .revision;
        }
        let old_checkpoint_position = store
            .load_state_at_manifest()
            .await
            .unwrap()
            .base_position()
            .unwrap();

        store.collect_wal(Duration::ZERO).await.unwrap();
        store.collect_wal(Duration::ZERO).await.unwrap();

        assert_eq!(store.snapshot().await.unwrap().revision(), expected);
        let fenced = store.load_state_at_manifest().await.unwrap();
        let fence_position = fenced.base_position().unwrap();
        assert!(fenced.tail_deltas.is_empty());
        assert_eq!(fence_position, fenced.latest_position().unwrap());
        assert!(fence_position > old_checkpoint_position);
        assert_eq!(
            store
                .reader()
                .await
                .unwrap()
                .oldest_timestamp()
                .await
                .unwrap(),
            fence_position
        );
    }

    #[tokio::test]
    async fn collect_wal_honors_an_older_named_cursor() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let prefix = "casita/state";
        let store = Wal3MetadataStore::open(storage.clone(), prefix, "maintenance")
            .await
            .unwrap();
        let old_object = verified_blob(b"state retained only by an older cursor").await;
        let old_key = old_object.record().key().clone();
        let mut mutation = MetadataMutation::new();
        mutation.add_object(old_object);
        let mut expected = store
            .commit(&store.snapshot().await.unwrap().revision(), mutation)
            .await
            .unwrap()
            .revision;
        for _ in 0..MAX_TAIL_DELTAS {
            expected = store
                .commit(&expected, MetadataMutation::new())
                .await
                .unwrap()
                .revision;
        }
        let pinned = store.load_state_at_manifest().await.unwrap();
        assert!(pinned.tail_deltas.is_empty());
        let pinned_position = pinned.latest_position().unwrap();
        let old_paths = store
            .object_shards
            .referenced_paths(pinned.state.as_ref().unwrap().base_objects.as_ref());
        assert!(!old_paths.is_empty());
        expected = store
            .commit(
                &expected,
                MetadataMutation::install_retained_objects(BTreeSet::new()),
            )
            .await
            .unwrap()
            .revision;
        assert!(
            store
                .snapshot()
                .await
                .unwrap()
                .object(&old_key)
                .await
                .unwrap()
                .is_none()
        );
        let cursor_store = wal3::CursorStore::new(
            wal3::CursorStoreOptions::default(),
            storage,
            prefix.to_owned(),
            "emergency".to_owned(),
        );
        let cursor_name = wal3::CursorName::new("emergency").unwrap();
        let cursor = wal3::Cursor {
            position: pinned_position,
            epoch_us: wal3::now_micros(),
            writer: String::new(),
        };
        boxed_wal_future(cursor_store.init(&cursor_name, cursor))
            .await
            .unwrap();

        boxed_wal_future(store.collect_wal(Duration::ZERO))
            .await
            .unwrap();

        assert_eq!(store.snapshot().await.unwrap().revision(), expected);
        let remaining = store.object_shards.list_paths().await.unwrap();
        assert!(old_paths.iter().all(|path| remaining.contains(path)));
        assert_eq!(
            store
                .reader()
                .await
                .unwrap()
                .oldest_timestamp()
                .await
                .unwrap(),
            pinned_position
        );
    }

    #[tokio::test]
    async fn snapshot_rejects_valid_fragment_with_the_wrong_setsum() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(chroma_storage::Storage::Local(
            chroma_storage::local::LocalStorage::new(directory.path().to_str().unwrap()),
        ));
        let target = Wal3MetadataStore::open(storage.clone(), "target", "target")
            .await
            .unwrap();
        let _source = Wal3MetadataStore::open(storage, "source", "source")
            .await
            .unwrap();
        let first_fragment = wal3::unprefixed_fragment_path(wal3::FragmentSeqNo::BEGIN.into());
        std::fs::copy(
            directory.path().join("source").join(&first_fragment),
            directory.path().join("target").join(&first_fragment),
        )
        .unwrap();

        assert!(matches!(
            target.snapshot().await,
            Err(MetadataError::Corruption(message)) if message.contains("mismatched setsum")
        ));
    }

    #[tokio::test]
    async fn rustfs_metadata_handles_share_online_pins_without_logical_commits() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let first = Wal3MetadataStore::open(storage.clone(), "pin-identity/state", "first")
            .await
            .unwrap();
        let second = Wal3MetadataStore::open(storage, "pin-identity/state", "second")
            .await
            .unwrap();
        let revision = first.snapshot().await.unwrap().revision();
        let pin = crate::metadata::DataPinLease::acquire(
            first.pin_store().await.unwrap(),
            crate::metadata::DataPin {
                scope: crate::metadata::PinScope::Staging,
                catalog: None,
                resources: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
        let ledger = second.pin_store().await.unwrap();
        assert!(
            ledger
                .inventory()
                .await
                .unwrap()
                .pins
                .contains_key(pin.token())
        );
        assert_eq!(second.snapshot().await.unwrap().revision(), revision);
        drop(pin);
        crate::flush_repository_leases().await.unwrap();
        assert!(ledger.inventory().await.unwrap().pins.is_empty());
    }

    #[tokio::test]
    async fn rustfs_serializes_racing_writers_with_the_wal_manifest() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let first = Wal3MetadataStore::open(storage.clone(), "casita/state", "first")
            .await
            .unwrap();
        let second = Wal3MetadataStore::open(storage, "casita/state", "second")
            .await
            .unwrap();
        let expected = first.snapshot().await.unwrap().revision();
        assert_eq!(second.snapshot().await.unwrap().revision(), expected);

        let (left, right) = tokio::join!(
            first.commit(&expected, MetadataMutation::new()),
            second.commit(&expected, MetadataMutation::new())
        );
        let outcomes = [left, right];
        assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
        assert!(
            outcomes
                .iter()
                .any(|result| matches!(result, Err(MetadataError::StaleRevision { .. }))),
            "unexpected racing writer outcomes: {outcomes:?}"
        );
    }

    #[tokio::test]
    async fn rustfs_transfers_payloads_and_publishes_the_root_between_s3_prefixes() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let endpoint = rustfs.endpoint();
        let bucket = storage.bucket_name().unwrap().to_owned();
        let source = rustfs_repository(
            storage.clone(),
            &bucket,
            &endpoint,
            "source",
            "source-runner",
        )
        .await;
        let destination = rustfs_repository(
            storage.clone(),
            &bucket,
            &endpoint,
            "destination",
            "destination-runner",
        )
        .await;
        let input = tempfile::tempdir().unwrap();
        source.payloads().reset_pack_read_stats();
        let mut payload = vec![0; 1024 * 1024];
        let mut random = 0x4d59_5df4_d0f3_3173_u64;
        for byte in &mut payload {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            *byte = random as u8;
        }
        std::fs::write(input.path().join("payload"), payload).unwrap();
        let name = RootName::try_from("releases/current").unwrap();
        let root = source
            .import(crate::import::FilesystemImport::new(
                input.path(),
                name.clone(),
            ))
            .await
            .unwrap();
        let publication = source.payloads().pack_read_stats().unwrap();
        assert_eq!(publication.index_put_requests, 0);
        assert!(
            source
                .metadata()
                .snapshot()
                .await
                .unwrap()
                .payload_catalog()
                .is_some()
        );
        let source_reader =
            rustfs_repository(storage, &bucket, &endpoint, "source", "source-reader").await;
        source_reader.payloads().reset_pack_read_stats();

        let result = crate::transfer(
            &source_reader,
            &destination,
            crate::TransferRequest {
                objects: vec![crate::ObjectRequest {
                    key: root.clone(),
                    recursive: true,
                }],
                roots: vec![crate::DestinationRoot {
                    name: name.clone(),
                    target: root.clone(),
                }],
            },
            crate::sync::TransferOptions::default(),
        )
        .await
        .unwrap();
        assert!(result.progress.payloads_sent > 0);
        assert!(result.progress.chunks_sent > 0);
        let source_reads = source_reader.payloads().pack_read_stats().unwrap();
        assert!(source_reads.chunk_range_requests > 0);
        assert!(source_reads.whole_pack_requests <= source_reads.chunk_range_requests);
        // Concurrent traversal need not visit adjacent ranges consecutively.
        // Whether it retained ranges or promoted packs, the whole small fixture
        // must now be readable from the bounded cache without backend reads.
        source_reader.payloads().reset_pack_read_stats();
        assert!(matches!(
            source_reader.verify_closure(&root).await.unwrap(),
            crate::ClosureStatus::Complete { .. }
        ));
        let warm_reads = source_reader.payloads().pack_read_stats().unwrap();
        assert!(warm_reads.cache_hits > 0);
        assert_eq!(warm_reads.chunk_range_requests, 0);
        assert_eq!(warm_reads.whole_pack_requests, 0);
        assert_eq!(
            destination
                .metadata()
                .snapshot()
                .await
                .unwrap()
                .root(&name)
                .await
                .unwrap(),
            Some(root.clone())
        );
        assert!(matches!(
            destination.verify_closure(&root).await.unwrap(),
            crate::ClosureStatus::Complete { .. }
        ));

        let objects = crate::object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(&bucket)
            .with_access_key_id("minio")
            .with_secret_access_key("minio123")
            .with_region("us-east-1")
            .with_endpoint(&endpoint)
            .with_allow_http(true)
            .with_checksum_algorithm(crate::object_store::aws::Checksum::SHA256)
            .build()
            .unwrap();
        for prefix in ["source", "destination"] {
            let packs = objects
                .list(Some(&crate::object_store::path::Path::from(format!(
                    "{prefix}/payloads/packs/b3"
                ))))
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let loose = objects
                .list(Some(&crate::object_store::path::Path::from(format!(
                    "{prefix}/payloads/chunks/b3"
                ))))
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert!(!packs.is_empty(), "{prefix} wrote no chunk pack");
            assert!(loose.is_empty(), "{prefix} still wrote loose chunks");
        }
    }

    #[tokio::test]
    async fn rustfs_racing_repositories_rebase_state_catalog_without_losing_packs() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let endpoint = rustfs.endpoint();
        let bucket = storage.bucket_name().unwrap().to_owned();
        let first = rustfs_repository(
            storage.clone(),
            &bucket,
            &endpoint,
            "racing",
            "first-runner",
        )
        .await;
        let second = rustfs_repository(
            storage.clone(),
            &bucket,
            &endpoint,
            "racing",
            "second-runner",
        )
        .await;
        first
            .payloads()
            .set_pack_catalog_rebase_run_bytes_for_test(1);
        second
            .payloads()
            .set_pack_catalog_rebase_run_bytes_for_test(1);
        first.payloads().reset_pack_read_stats();
        second.payloads().reset_pack_read_stats();

        let first_input = tempfile::tempdir().unwrap();
        let second_input = tempfile::tempdir().unwrap();
        std::fs::write(first_input.path().join("first"), b"first racing payload").unwrap();
        std::fs::write(second_input.path().join("second"), b"second racing payload").unwrap();
        let first_name = RootName::try_from("racing/first").unwrap();
        let second_name = RootName::try_from("racing/second").unwrap();
        let (first_result, second_result) = tokio::join!(
            first.import(crate::import::FilesystemImport::new(
                first_input.path(),
                first_name.clone()
            )),
            second.import(crate::import::FilesystemImport::new(
                second_input.path(),
                second_name.clone()
            )),
        );
        let first_root = first_result.unwrap();
        let second_root = second_result.unwrap();
        crate::flush_repository_leases().await.unwrap();
        // The winner may start the forced immutable-shard rebase while the
        // stale racer rebases its WAL mutation without duplicating that work.
        let first_index_puts = first
            .payloads()
            .pack_read_stats()
            .unwrap()
            .index_put_requests;
        let second_index_puts = second
            .payloads()
            .pack_read_stats()
            .unwrap()
            .index_put_requests;
        assert!(first_index_puts + second_index_puts > 0);

        // Open a fresh handle on the winning WAL root. A normal commit starts
        // the shard rewrite; draining waits for its independent atomic catalog
        // publication without requiring another user write.
        let installer = rustfs_repository(
            storage.clone(),
            &bucket,
            &endpoint,
            "racing",
            "installer-runner",
        )
        .await;
        installer
            .payloads()
            .set_pack_catalog_rebase_run_bytes_for_test(1);
        let trigger_input = tempfile::tempdir().unwrap();
        std::fs::write(
            trigger_input.path().join("trigger"),
            b"trigger background catalog rebase",
        )
        .unwrap();
        installer
            .import(crate::import::FilesystemImport::new(
                trigger_input.path(),
                RootName::try_from("racing/trigger").unwrap(),
            ))
            .await
            .unwrap();
        crate::flush_repository_leases().await.unwrap();
        let install_input = tempfile::tempdir().unwrap();
        std::fs::write(
            install_input.path().join("install"),
            b"install completed catalog rebase",
        )
        .unwrap();
        installer
            .import(crate::import::FilesystemImport::new(
                install_input.path(),
                RootName::try_from("racing/install").unwrap(),
            ))
            .await
            .unwrap();
        crate::flush_repository_leases().await.unwrap();

        let reader = rustfs_repository(storage, &bucket, &endpoint, "racing", "reader").await;
        let reader_stats = reader.payloads().pack_read_stats().unwrap();
        assert!(reader_stats.index_sharded_base);
        assert!(!reader_stats.index_checkpoint_base);
        assert_eq!(reader_stats.index_run_objects, 0);
        assert_eq!(reader_stats.index_pointer_requests, 0);
        assert_eq!(reader_stats.index_requests, 1);
        assert_eq!(reader_stats.list_requests, 0);
        let hold = reader.retention_hold().await.unwrap();
        assert_eq!(
            hold.snapshot().root(&first_name).await.unwrap(),
            Some(first_root.clone())
        );
        assert_eq!(
            hold.snapshot().root(&second_name).await.unwrap(),
            Some(second_root.clone())
        );
        assert!(matches!(
            hold.verify_closure(&first_root).await.unwrap(),
            crate::ClosureStatus::Complete { .. }
        ));
        assert!(matches!(
            hold.verify_closure(&second_root).await.unwrap(),
            crate::ClosureStatus::Complete { .. }
        ));
    }

    // A WAL collection appends a checkpoint without changing the revision. A
    // writer whose view of the log predates it misses that checkpoint's
    // position, reloads the same revision, and must rebuild its append on the
    // log as it now is. The writer's cached base references no shards, so it
    // is used without re-reading the manifest, which makes the miss certain.

    async fn assert_a_fresh_handle_reads(
        storage: Arc<chroma_storage::Storage>,
        prefix: &str,
        revision: RepositoryRevision,
        present: &[ObjectKey],
        absent: &[ObjectKey],
    ) {
        let reader = Wal3MetadataStore::open(storage, prefix, "reader")
            .await
            .unwrap();
        let snapshot = reader.snapshot().await.unwrap();
        assert_eq!(snapshot.revision(), revision);
        for key in present {
            assert!(snapshot.object(key).await.unwrap().is_some(), "{key}");
        }
        for key in absent {
            assert!(snapshot.object(key).await.unwrap().is_none(), "{key}");
        }
    }

    #[tokio::test]
    async fn rustfs_a_delta_commit_retried_past_a_wal_collection_keeps_the_log_readable() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let prefix = "retried-delta/state";
        let writer = Wal3MetadataStore::open(storage.clone(), prefix, "writer")
            .await
            .unwrap();
        let collector = Wal3MetadataStore::open(storage.clone(), prefix, "collector")
            .await
            .unwrap();
        let objects = [
            verified_blob(b"written before the collection").await,
            verified_blob(b"written after the collection").await,
            verified_blob(b"written by the next commit").await,
        ];
        let keys = objects
            .iter()
            .map(|object| object.record().key().clone())
            .collect::<Vec<_>>();
        let [before, retried, next] = objects;

        let mut mutation = MetadataMutation::new();
        mutation.add_object(before);
        let expected = writer
            .commit(&writer.opened_snapshot().revision(), mutation)
            .await
            .unwrap()
            .revision;
        collector.collect_wal(Duration::ZERO).await.unwrap();

        let mut mutation = MetadataMutation::new();
        mutation.add_object(retried);
        let expected = writer.commit(&expected, mutation).await.unwrap().revision;
        let mut mutation = MetadataMutation::new();
        mutation.add_object(next);
        let committed = writer.commit(&expected, mutation).await.unwrap().revision;
        assert_a_fresh_handle_reads(storage, prefix, committed, &keys, &[]).await;
    }

    #[tokio::test]
    async fn rustfs_a_checkpointing_commit_retried_past_a_wal_collection_keeps_the_log_readable() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let prefix = "retried-checkpoint/state";
        let writer = Wal3MetadataStore::open(storage.clone(), prefix, "writer")
            .await
            .unwrap();
        let collector = Wal3MetadataStore::open(storage.clone(), prefix, "collector")
            .await
            .unwrap();
        let mut expected = writer.opened_snapshot().revision();
        for _ in 0..MAX_TAIL_DELTAS {
            expected = writer
                .commit(&expected, MetadataMutation::new())
                .await
                .unwrap()
                .revision;
        }
        collector.collect_wal(Duration::ZERO).await.unwrap();

        // One delta past a full tail: this commit plans a checkpoint.
        let object = verified_blob(b"committed past the collection").await;
        let key = object.record().key().clone();
        let mut mutation = MetadataMutation::new();
        mutation.add_object(object);
        let expected = writer.commit(&expected, mutation).await.unwrap().revision;
        let committed = writer
            .commit(&expected, MetadataMutation::new())
            .await
            .unwrap()
            .revision;
        assert_a_fresh_handle_reads(storage, prefix, committed, &[key], &[]).await;
    }

    #[tokio::test]
    async fn rustfs_a_logical_collection_retried_past_a_wal_collection_keeps_the_log_readable() {
        let _lock = rustfs_test_lock().lock().await;
        let rustfs = Rustfs::start();
        let storage = rustfs_storage(&rustfs).await;
        let prefix = "retried-collection/state";
        let writer = Wal3MetadataStore::open(storage.clone(), prefix, "writer")
            .await
            .unwrap();
        let collector = Wal3MetadataStore::open(storage.clone(), prefix, "collector")
            .await
            .unwrap();
        let kept = verified_blob(b"kept by the logical collection").await;
        let dropped = verified_blob(b"dropped by the logical collection").await;
        let later = verified_blob(b"written after the logical collection").await;
        let kept_key = kept.record().key().clone();
        let dropped_key = dropped.record().key().clone();
        let later_key = later.record().key().clone();

        let mut mutation = MetadataMutation::new();
        mutation.add_object(kept).add_object(dropped);
        let expected = writer
            .commit(&writer.opened_snapshot().revision(), mutation)
            .await
            .unwrap()
            .revision;
        collector.collect_wal(Duration::ZERO).await.unwrap();

        let collected = writer
            .commit(
                &expected,
                MetadataMutation::install_retained_objects(BTreeSet::from([kept_key.clone()])),
            )
            .await
            .unwrap();
        assert_eq!(collected.objects_removed, 1);
        let mut mutation = MetadataMutation::new();
        mutation.add_object(later);
        let committed = writer
            .commit(&collected.revision, mutation)
            .await
            .unwrap()
            .revision;
        assert_a_fresh_handle_reads(
            storage,
            prefix,
            committed,
            &[kept_key, later_key],
            &[dropped_key],
        )
        .await;
    }
}
