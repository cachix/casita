//! Immutable, content-addressed logical-state shards for WAL3 checkpoints.
//!
//! Object and named-root shards preserve canonical order so complete
//! inventories can concatenate them without a global merge. Object shards
//! contain authenticated blocks and a compact trailing directory; a point
//! lookup routes map -> shard -> block and decodes at most
//! [`OBJECT_BLOCK_ENTRIES`] records.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use chroma_storage::{ETag, GetOptions, PutMode, PutOptions, Storage, StorageError};

use crate::Digest;
use crate::invariant::{self, Violation};
use crate::metadata::MetadataError;
use crate::object::{ObjectKey, ObjectRecord, RootName, RootRecord};

const MAP_MAGIC_V1: &[u8; 8] = b"casism01";
const SHARD_MAGIC_V1: &[u8; 8] = b"casis001";
const ROOT_SHARD_MAGIC_V1: &[u8; 8] = b"casir001";
const BLOCK_MAGIC_V2: &[u8; 8] = b"casib002";
const BLOCK_MAGIC_V1: &[u8; 8] = b"casib001";
const DIRECTORY_MAGIC_V1: &[u8; 8] = b"casid001";
const TRAILER_MAGIC_V1: &[u8; 8] = b"casit001";
const MAP_DOMAIN: &[u8] = b"casita logical state shard map v1\0";
const SHARD_DOMAIN: &[u8] = b"casita logical object shard v1\0";
const ROOT_SHARD_DOMAIN: &[u8] = b"casita logical root shard v1\0";
const BARRIER_MAGIC_V1: &[u8; 8] = b"casigc01";
const BARRIER_DOMAIN: &[u8] = b"casita logical shard publication barrier v1\0";
const OBJECT_BLOCK_ENTRIES: usize = 512;
const DIGEST_BYTES: usize = 32;
const TRAILER_BYTES: usize = 8 + 8 + 8 + DIGEST_BYTES;
pub(super) const DEFAULT_OBJECT_SHARD_TARGET_BYTES: u64 = 8 * 1024 * 1024;
pub(super) const DEFAULT_ROOT_SHARD_TARGET_BYTES: u64 = 256 * 1024;
pub(super) const MAX_OBJECT_SHARD_BYTES: usize = 64 * 1024 * 1024;
pub(super) const MAX_STATE_MAP_BYTES: usize = 32 * 1024 * 1024;
pub(super) const DEFAULT_SHARD_CACHE_BYTES: usize = 128 * 1024 * 1024;
const MAX_SHARD_REFS: usize = 1 << 20;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ObjectShardRef {
    pub(super) first: ObjectKey,
    pub(super) last: ObjectKey,
    pub(super) digest: Digest,
    pub(super) entries: u64,
    pub(super) validated: u64,
    pub(super) encoded_bytes: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct StateShardMap {
    pub(super) objects: Vec<ObjectShardRef>,
    pub(super) object_count: u64,
    pub(super) validated_count: u64,
    pub(super) roots: Vec<RootShardRef>,
    pub(super) root_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RootShardRef {
    pub(super) first: RootName,
    pub(super) last: RootName,
    pub(super) digest: Digest,
    pub(super) entries: u64,
    pub(super) encoded_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObjectBlockRef {
    first: ObjectKey,
    last: ObjectKey,
    offset: u64,
    encoded_bytes: u64,
    digest: Digest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct EncodedObjectShard {
    pub(super) reference: ObjectShardRef,
    pub(super) bytes: Bytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct EncodedRootShard {
    pub(super) reference: RootShardRef,
    pub(super) bytes: Bytes,
}

/// A content-addressed shard name holds only the bytes that hash to it.
/// Finding other bytes there is corruption or a BLAKE3 collision; referencing
/// them from a checkpoint would publish state nobody wrote.
fn validate_existing_shard(name: Digest, existing: &[u8]) -> Result<(), Violation> {
    let found = Digest::from(blake3::hash(existing));
    invariant::ensure("wal3.shard", found == name, || {
        format!("shard name {name} holds bytes hashing to {found}")
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BarrierMode {
    Idle = 0,
    Checkpoint = 1,
    GarbageCollection = 2,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BarrierState {
    generation: u64,
    mode: BarrierMode,
    token: [u8; 32],
}

#[derive(Clone, Debug)]
pub(super) struct ShardBarrierLease {
    generation: u64,
    mode: BarrierMode,
    token: [u8; 32],
    etag: Option<ETag>,
}

/// Acquisition advances the generation by exactly one, so a lease from any
/// earlier acquisition stops owning the barrier. It installs a held state
/// (a mode with a token, which `decode_barrier` requires) and preempts a
/// held barrier only when forced: a checkpoint never fences a collector.
fn validate_barrier_acquire(
    prev: &BarrierState,
    next: &BarrierState,
    force: bool,
) -> Result<(), Violation> {
    const POINT: &str = "wal3.barrier";
    invariant::ensure(
        POINT,
        prev.generation.checked_add(1) == Some(next.generation),
        || {
            format!(
                "acquisition moves generation {} to {}",
                prev.generation, next.generation
            )
        },
    )?;
    invariant::ensure(
        POINT,
        next.mode != BarrierMode::Idle && next.token != [0; 32],
        || format!("acquisition installs {:?} without a token", next.mode),
    )?;
    invariant::ensure(POINT, force || prev.mode == BarrierMode::Idle, || {
        format!("unforced acquisition preempts a {:?} barrier", prev.mode)
    })
}

/// Only the lease's owner releases the barrier, and release keeps the
/// generation, so the next acquisition still advances past this lease.
/// `held` is the state the conditional release replaces; the owner's is the
/// held state its acquisition installed.
fn validate_barrier_release(
    lease: &ShardBarrierLease,
    held: &BarrierState,
    next: &BarrierState,
) -> Result<(), Violation> {
    const POINT: &str = "wal3.barrier";
    invariant::ensure(
        POINT,
        held.mode != BarrierMode::Idle
            && held.mode == lease.mode
            && held.generation == lease.generation
            && held.token == lease.token,
        || {
            format!(
                "{:?} lease for generation {} releases {:?} barrier generation {} it does not own",
                lease.mode, lease.generation, held.mode, held.generation
            )
        },
    )?;
    invariant::ensure(
        POINT,
        next.mode == BarrierMode::Idle
            && next.token == [0; 32]
            && next.generation == held.generation,
        || {
            format!(
                "release of generation {} writes {:?} generation {}",
                held.generation, next.mode, next.generation
            )
        },
    )
}

/// Cumulative immutable-state shard work for one WAL3 handle.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ObjectShardStats {
    pub(super) get_requests: u64,
    pub(super) get_bytes: u64,
    pub(super) put_requests: u64,
    pub(super) cache_hits: u64,
    pub(super) barrier_get_requests: u64,
    pub(super) barrier_put_requests: u64,
    pub(super) inventory_list_requests: u64,
    pub(super) delete_requests: u64,
}

#[derive(Default)]
struct ObjectShardMetrics {
    get_requests: AtomicU64,
    get_bytes: AtomicU64,
    put_requests: AtomicU64,
    cache_hits: AtomicU64,
    barrier_get_requests: AtomicU64,
    barrier_put_requests: AtomicU64,
    inventory_list_requests: AtomicU64,
    delete_requests: AtomicU64,
}

/// Immutable object shards below one WAL3 repository prefix.
///
/// A point lookup downloads a complete shard on the first access. This keeps
/// cold S3 lookup cost to one request; subsequent records in that shard are
/// served by a strictly byte-bounded local cache.
#[derive(Clone)]
pub(super) struct ObjectShardStorage {
    storage: Arc<Storage>,
    prefix: Arc<str>,
    cache: Arc<Mutex<ShardCache>>,
    metrics: Arc<ObjectShardMetrics>,
    // Only operation-scoped clones carry this cell. Cached metadata views and
    // the long-lived store never keep abandoned write candidates pinned.
    write_pin: Option<Arc<tokio::sync::OnceCell<super::DataPinLease>>>,
    #[cfg(test)]
    deletion_pause: Arc<Mutex<Option<Arc<MetadataDeletionPause>>>>,
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct MetadataDeletionPause {
    pub(super) reached: tokio::sync::Notify,
    pub(super) resume: tokio::sync::Notify,
}

struct ShardCache {
    capacity: usize,
    bytes: usize,
    values: BTreeMap<Digest, Arc<Vec<u8>>>,
    recency: VecDeque<Digest>,
}

impl ObjectShardStorage {
    pub(super) fn new(storage: Arc<Storage>, prefix: String, cache_bytes: usize) -> Self {
        Self {
            storage,
            prefix: prefix.trim_end_matches('/').to_owned().into(),
            cache: Arc::new(Mutex::new(ShardCache::new(cache_bytes))),
            metrics: Arc::new(ObjectShardMetrics::default()),
            write_pin: None,
            #[cfg(test)]
            deletion_pause: Arc::default(),
        }
    }

    pub(super) fn for_write_operation(&self) -> Self {
        let mut scoped = self.clone();
        scoped.write_pin = Some(Arc::new(tokio::sync::OnceCell::new()));
        scoped
    }

    async fn pin_write(&self, path: &str) -> Result<super::DataPinLease, MetadataError> {
        let resources = BTreeSet::from([super::PinResource::MetadataObject(path.to_owned())]);
        let claimed =
            || MetadataError::Transient("metadata write path is claimed for deletion".into());
        let acquire = || async {
            super::DataPinLease::try_acquire(
                super::pins::chroma_pin_store(
                    self.storage.clone(),
                    format!("{}/online-pins-v1", self.prefix),
                ),
                super::DataPin {
                    scope: super::PinScope::Metadata,
                    catalog: None,
                    resources: resources.clone(),
                },
            )
            .await?
            .ok_or_else(claimed)
        };
        let pin = match &self.write_pin {
            Some(cell) => cell.get_or_try_init(acquire).await?.clone(),
            None => acquire().await?,
        };
        // A stale candidate must fail promptly, not wait forever on a durable
        // claim left by a failed collector. Its caller can retry after recovery.
        if !pin.try_protect(resources).await? {
            return Err(claimed());
        }
        Ok(pin)
    }

    #[cfg(test)]
    pub(super) fn pause_deletion(&self) -> Arc<MetadataDeletionPause> {
        let pause = Arc::new(MetadataDeletionPause::default());
        *self.deletion_pause.lock().unwrap() = Some(pause.clone());
        pause
    }

    pub(super) fn stats(&self) -> ObjectShardStats {
        ObjectShardStats {
            get_requests: self.metrics.get_requests.load(Ordering::Relaxed),
            get_bytes: self.metrics.get_bytes.load(Ordering::Relaxed),
            put_requests: self.metrics.put_requests.load(Ordering::Relaxed),
            cache_hits: self.metrics.cache_hits.load(Ordering::Relaxed),
            barrier_get_requests: self.metrics.barrier_get_requests.load(Ordering::Relaxed),
            barrier_put_requests: self.metrics.barrier_put_requests.load(Ordering::Relaxed),
            inventory_list_requests: self.metrics.inventory_list_requests.load(Ordering::Relaxed),
            delete_requests: self.metrics.delete_requests.load(Ordering::Relaxed),
        }
    }

    pub(super) fn reset_stats(&self) {
        self.metrics.get_requests.store(0, Ordering::Relaxed);
        self.metrics.get_bytes.store(0, Ordering::Relaxed);
        self.metrics.put_requests.store(0, Ordering::Relaxed);
        self.metrics.cache_hits.store(0, Ordering::Relaxed);
        self.metrics
            .barrier_get_requests
            .store(0, Ordering::Relaxed);
        self.metrics
            .barrier_put_requests
            .store(0, Ordering::Relaxed);
        self.metrics
            .inventory_list_requests
            .store(0, Ordering::Relaxed);
        self.metrics.delete_requests.store(0, Ordering::Relaxed);
    }

    pub(super) async fn acquire_checkpoint_barrier(
        &self,
    ) -> Result<ShardBarrierLease, MetadataError> {
        self.acquire_barrier(BarrierMode::Checkpoint, false).await
    }

    pub(super) async fn acquire_gc_barrier(&self) -> Result<ShardBarrierLease, MetadataError> {
        self.acquire_barrier(BarrierMode::GarbageCollection, true)
            .await
    }

    pub(super) async fn owns_barrier(
        &self,
        lease: &ShardBarrierLease,
    ) -> Result<bool, MetadataError> {
        let (state, _) = self.load_or_initialize_barrier().await?;
        Ok(state.generation == lease.generation && state.token == lease.token)
    }

    pub(super) async fn release_barrier(
        &self,
        lease: &ShardBarrierLease,
    ) -> Result<(), MetadataError> {
        if let Some(etag) = &lease.etag {
            let idle = BarrierState {
                generation: lease.generation,
                mode: BarrierMode::Idle,
                token: [0; 32],
            };
            invariant::check(|| {
                // The ETag names the state this lease installed.
                let held = BarrierState {
                    generation: lease.generation,
                    mode: lease.mode,
                    token: lease.token,
                };
                validate_barrier_release(lease, &held, &idle)
            })?;
            return match self
                .put_barrier(&idle, PutMode::IfMatch(etag.clone()))
                .await
            {
                Ok(_) | Err(MetadataError::Transient(_)) => Ok(()),
                Err(error) => Err(error),
            };
        }
        for _ in 0..8 {
            let (state, etag) = self.load_or_initialize_barrier().await?;
            if state.generation != lease.generation || state.token != lease.token {
                return Ok(());
            }
            let idle = BarrierState {
                generation: state.generation,
                mode: BarrierMode::Idle,
                token: [0; 32],
            };
            invariant::check(|| validate_barrier_release(lease, &state, &idle))?;
            match self.put_barrier(&idle, PutMode::IfMatch(etag)).await {
                Ok(_) => return Ok(()),
                Err(MetadataError::Transient(_)) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(MetadataError::Transient(
            "logical shard barrier release remained contended".to_owned(),
        ))
    }

    async fn acquire_barrier(
        &self,
        mode: BarrierMode,
        force: bool,
    ) -> Result<ShardBarrierLease, MetadataError> {
        for _ in 0..8 {
            let (current, etag) = self.load_or_initialize_barrier().await?;
            if !force && current.mode != BarrierMode::Idle {
                return Err(MetadataError::MaintenanceFenced);
            }
            let generation = current.generation.checked_add(1).ok_or_else(|| {
                MetadataError::Corruption("logical shard barrier overflow".to_owned())
            })?;
            let mut token = [0_u8; 32];
            getrandom::fill(&mut token).map_err(|error| {
                MetadataError::Backend(format!("logical shard barrier entropy: {error}"))
            })?;
            let next = BarrierState {
                generation,
                mode,
                token,
            };
            invariant::check(|| validate_barrier_acquire(&current, &next, force))?;
            match self.put_barrier(&next, PutMode::IfMatch(etag)).await {
                Ok(etag) => {
                    return Ok(ShardBarrierLease {
                        generation,
                        mode,
                        token,
                        etag,
                    });
                }
                Err(MetadataError::Transient(_)) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(MetadataError::Transient(
            "logical shard barrier acquisition remained contended".to_owned(),
        ))
    }

    async fn load_or_initialize_barrier(&self) -> Result<(BarrierState, ETag), MetadataError> {
        let path = self.barrier_path();
        loop {
            self.metrics
                .barrier_get_requests
                .fetch_add(1, Ordering::Relaxed);
            match boxed_storage_future(
                self.storage
                    .get_with_e_tag(&path, GetOptions::default().with_strong_consistency()),
            )
            .await
            {
                Ok((bytes, Some(etag))) => return Ok((decode_barrier(&bytes)?, etag)),
                Ok((_, None)) => {
                    return Err(MetadataError::Backend(
                        "logical shard barrier read returned no ETag".to_owned(),
                    ));
                }
                Err(StorageError::NotFound { .. }) => {
                    let idle = encode_barrier(&BarrierState {
                        generation: 0,
                        mode: BarrierMode::Idle,
                        token: [0; 32],
                    });
                    let options = PutOptions::default().with_mode(PutMode::IfNotExist);
                    self.metrics
                        .barrier_put_requests
                        .fetch_add(1, Ordering::Relaxed);
                    match boxed_storage_future(self.storage.put_bytes(&path, idle, options)).await {
                        Ok(_)
                        | Err(StorageError::AlreadyExists { .. })
                        | Err(StorageError::Precondition { .. }) => continue,
                        Err(error) => {
                            return Err(MetadataError::Backend(format!(
                                "initialize logical shard barrier {path}: {error}"
                            )));
                        }
                    }
                }
                Err(error) => {
                    return Err(MetadataError::Backend(format!(
                        "read logical shard barrier {path}: {error}"
                    )));
                }
            }
        }
    }

    async fn put_barrier(
        &self,
        state: &BarrierState,
        mode: PutMode,
    ) -> Result<Option<ETag>, MetadataError> {
        let path = self.barrier_path();
        let options = PutOptions::default().with_mode(mode);
        self.metrics
            .barrier_put_requests
            .fetch_add(1, Ordering::Relaxed);
        match boxed_storage_future(
            self.storage
                .put_bytes(&path, encode_barrier(state), options),
        )
        .await
        {
            Ok(etag) => Ok(etag),
            Err(StorageError::Precondition { .. }) | Err(StorageError::AlreadyExists { .. }) => {
                Err(MetadataError::Transient(
                    "logical shard barrier was concurrently changed".to_owned(),
                ))
            }
            Err(error) => Err(MetadataError::Backend(format!(
                "write logical shard barrier {path}: {error}"
            ))),
        }
    }

    pub(super) async fn lookup(
        &self,
        map: &StateShardMap,
        key: &ObjectKey,
    ) -> Result<Option<(ObjectRecord, bool, u64)>, MetadataError> {
        let Some(reference) = map.object_shard(key) else {
            return Ok(None);
        };
        let bytes = self.get(reference).await?;
        lookup_verified_object_shard(bytes.as_slice(), reference, key)
            .map_err(|error| shard_corruption(reference, error))
    }

    pub(super) async fn read(
        &self,
        reference: &ObjectShardRef,
    ) -> Result<Vec<(ObjectRecord, bool, u64)>, MetadataError> {
        let bytes = self.get(reference).await?;
        decode_verified_object_shard(bytes.as_slice(), reference)
            .map_err(|error| shard_corruption(reference, error))
    }

    pub(super) async fn lookup_root(
        &self,
        map: &StateShardMap,
        name: &RootName,
    ) -> Result<Option<ObjectKey>, MetadataError> {
        let Some(reference) = map.root_shard(name) else {
            return Ok(None);
        };
        let bytes = self.get_root(reference).await?;
        lookup_verified_root_shard(bytes.as_slice(), reference, name)
            .map_err(|error| root_shard_corruption(reference, error))
    }

    pub(super) async fn read_roots(
        &self,
        reference: &RootShardRef,
    ) -> Result<Vec<RootRecord>, MetadataError> {
        let bytes = self.get_root(reference).await?;
        decode_verified_root_shard(bytes.as_slice(), reference)
            .map_err(|error| root_shard_corruption(reference, error))
    }

    pub(super) async fn put(&self, shard: &EncodedObjectShard) -> Result<(), MetadataError> {
        let path = self.path(shard.reference.digest);
        let _pin = self.pin_write(&path).await?;
        let options = PutOptions::default().with_mode(PutMode::IfNotExist);
        let created = match boxed_storage_future(self.storage.put_bytes(
            &path,
            shard.bytes.to_vec(),
            options,
        ))
        .await
        {
            Ok(_) => true,
            Err(StorageError::AlreadyExists { .. }) | Err(StorageError::Precondition { .. }) => {
                false
            }
            Err(error) => {
                return Err(MetadataError::Backend(format!(
                    "write logical object shard {path}: {error}"
                )));
            }
        };
        self.metrics.put_requests.fetch_add(1, Ordering::Relaxed);
        if !created {
            self.check_existing_shard(&path, shard.reference.digest)
                .await?;
        }
        self.cache
            .lock()
            .map_err(|_| MetadataError::Poisoned)?
            .insert(shard.reference.digest, Arc::new(shard.bytes.to_vec()));
        Ok(())
    }

    pub(super) async fn put_root(&self, shard: &EncodedRootShard) -> Result<(), MetadataError> {
        let path = self.root_path(shard.reference.digest);
        let _pin = self.pin_write(&path).await?;
        let options = PutOptions::default().with_mode(PutMode::IfNotExist);
        let created = match boxed_storage_future(self.storage.put_bytes(
            &path,
            shard.bytes.to_vec(),
            options,
        ))
        .await
        {
            Ok(_) => true,
            Err(StorageError::AlreadyExists { .. }) | Err(StorageError::Precondition { .. }) => {
                false
            }
            Err(error) => {
                return Err(MetadataError::Backend(format!(
                    "write logical root shard {path}: {error}"
                )));
            }
        };
        self.metrics.put_requests.fetch_add(1, Ordering::Relaxed);
        if !created {
            self.check_existing_shard(&path, shard.reference.digest)
                .await?;
        }
        self.cache
            .lock()
            .map_err(|_| MetadataError::Poisoned)?
            .insert(shard.reference.digest, Arc::new(shard.bytes.to_vec()));
        Ok(())
    }

    /// A create that finds its content-addressed name taken treats the object
    /// as already written, so the bytes there must be the ones it names. The
    /// check costs a GET, which only checking builds pay. It is left out of
    /// the request metrics, which describe the protocol's own requests and
    /// back its request budgets.
    async fn check_existing_shard(&self, path: &str, name: Digest) -> Result<(), MetadataError> {
        if !invariant::ENABLED {
            return Ok(());
        }
        // An unverifiable name must not be referenced either; the write pin
        // excludes deletion, so a failed GET here is a store fault.
        let existing = boxed_storage_future(self.storage.get(path, GetOptions::default()))
            .await
            .map_err(|error| {
                MetadataError::Backend(format!(
                    "verify existing logical state shard {path}: {error}"
                ))
            })?;
        invariant::check(|| validate_existing_shard(name, &existing))?;
        Ok(())
    }

    async fn get(&self, reference: &ObjectShardRef) -> Result<Arc<Vec<u8>>, MetadataError> {
        if let Some(bytes) = self
            .cache
            .lock()
            .map_err(|_| MetadataError::Poisoned)?
            .get(reference.digest)
        {
            self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(bytes);
        }
        let path = self.path(reference.digest);
        self.metrics.get_requests.fetch_add(1, Ordering::Relaxed);
        let bytes = boxed_storage_future(self.storage.get(&path, GetOptions::default()))
            .await
            .map_err(|error| {
                MetadataError::Backend(format!("read logical object shard {path}: {error}"))
            })?;
        self.metrics
            .get_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        verify_object_shard(bytes.as_slice(), reference)
            .map_err(|error| shard_corruption(reference, error))?;
        self.cache
            .lock()
            .map_err(|_| MetadataError::Poisoned)?
            .insert(reference.digest, bytes.clone());
        Ok(bytes)
    }

    async fn get_root(&self, reference: &RootShardRef) -> Result<Arc<Vec<u8>>, MetadataError> {
        if let Some(bytes) = self
            .cache
            .lock()
            .map_err(|_| MetadataError::Poisoned)?
            .get(reference.digest)
        {
            self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(bytes);
        }
        let path = self.root_path(reference.digest);
        self.metrics.get_requests.fetch_add(1, Ordering::Relaxed);
        let bytes = boxed_storage_future(self.storage.get(&path, GetOptions::default()))
            .await
            .map_err(|error| {
                MetadataError::Backend(format!("read logical root shard {path}: {error}"))
            })?;
        self.metrics
            .get_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        verify_root_shard(bytes.as_slice(), reference)
            .map_err(|error| root_shard_corruption(reference, error))?;
        self.cache
            .lock()
            .map_err(|_| MetadataError::Poisoned)?
            .insert(reference.digest, bytes.clone());
        Ok(bytes)
    }

    fn path(&self, digest: Digest) -> String {
        let hex = digest.to_hex();
        format!("{}/state-objects/b3/{hex}", self.prefix)
    }

    fn root_path(&self, digest: Digest) -> String {
        let hex = digest.to_hex();
        format!("{}/state-roots/b3/{hex}", self.prefix)
    }

    fn barrier_path(&self) -> String {
        format!("{}/state-shards-gc-current", self.prefix)
    }

    pub(super) fn referenced_paths(&self, map: &StateShardMap) -> BTreeSet<String> {
        map.objects
            .iter()
            .map(|reference| self.path(reference.digest))
            .chain(
                map.roots
                    .iter()
                    .map(|reference| self.root_path(reference.digest)),
            )
            .collect()
    }

    pub(super) async fn list_paths(&self) -> Result<Vec<String>, MetadataError> {
        let mut paths = Vec::new();
        for prefix in [
            format!("{}/state-objects/b3", self.prefix),
            format!("{}/state-roots/b3", self.prefix),
        ] {
            self.metrics
                .inventory_list_requests
                .fetch_add(1, Ordering::Relaxed);
            let listed =
                boxed_storage_future(self.storage.list_prefix(&prefix, GetOptions::default()))
                    .await
                    .map_err(|error| {
                        MetadataError::Backend(format!(
                            "list immutable logical state at {prefix}: {error}"
                        ))
                    })?;
            paths.extend(listed.into_iter().map(|path| {
                if path.contains('/') {
                    path
                } else {
                    format!("{prefix}/{path}")
                }
            }));
        }
        Ok(paths)
    }

    /// The caller owns the shard GC barrier and tracks this complete operation
    /// through cancellation. Claims remain durable if a DELETE fails.
    pub(super) async fn delete_paths_pinned(
        &self,
        paths: &[String],
        pins: Arc<dyn crate::metadata::PinStore>,
    ) -> Result<(), MetadataError> {
        use crate::metadata::PinResource;
        for batch in paths.chunks(1_000) {
            loop {
                let inventory = pins.inventory().await?;
                if !pins.allows_deletion(&inventory) {
                    return Err(MetadataError::Transient(
                        "logical prune fences metadata deletion".into(),
                    ));
                }
                let selected = batch
                    .iter()
                    .filter(|path| {
                        let resource = PinResource::MetadataObject((*path).clone());
                        !inventory
                            .pins
                            .values()
                            .any(|pin| pin.resources.contains(&resource))
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                if selected.is_empty() {
                    break;
                }
                let resources = selected
                    .iter()
                    .cloned()
                    .map(PinResource::MetadataObject)
                    .collect::<BTreeSet<_>>();
                if inventory
                    .deletions
                    .values()
                    .any(|claim| !claim.is_disjoint(&resources))
                {
                    return Err(MetadataError::Transient(
                        "metadata deletion requires exact-claim recovery".into(),
                    ));
                }
                let Some(claim) = pins.claim_deletions(inventory.revision, resources).await? else {
                    continue;
                };
                #[cfg(test)]
                let pause = self.deletion_pause.lock().unwrap().take();
                #[cfg(test)]
                if let Some(pause) = pause {
                    pause.reached.notify_one();
                    pause.resume.notified().await;
                }
                self.delete_paths(&selected).await?;
                pins.finish_deletions(&claim).await?;
                break;
            }
        }
        Ok(())
    }

    pub(super) async fn delete_paths(&self, paths: &[String]) -> Result<(), MetadataError> {
        for batch in paths.chunks(1_000) {
            self.metrics.delete_requests.fetch_add(1, Ordering::Relaxed);
            boxed_storage_future(self.storage.delete_many(batch))
                .await
                .map_err(|error| {
                    MetadataError::Backend(format!(
                        "delete {} orphaned logical state shards: {error}",
                        batch.len()
                    ))
                })?;
        }
        Ok(())
    }

    pub(super) fn deletion_recovery_paths(
        &self,
        inventory: &super::PinInventory,
        expected: &BTreeSet<super::PinToken>,
        live: &BTreeSet<String>,
    ) -> Result<Vec<(super::PinToken, Vec<String>)>, MetadataError> {
        if inventory.collector.is_some() || inventory.logical_prune.is_some() {
            return Err(MetadataError::Transient(
                "payload collection requires its own recovery".into(),
            ));
        }
        if inventory.deletions.keys().cloned().collect::<BTreeSet<_>>() != *expected {
            return Err(MetadataError::Transient(
                "metadata deletion tokens no longer match".into(),
            ));
        }
        let prefixes = [
            format!("{}/state-objects/b3/", self.prefix),
            format!("{}/state-roots/b3/", self.prefix),
        ];
        inventory
            .deletions
            .iter()
            .map(|(token, resources)| {
                let paths = resources
                    .iter()
                    .map(|resource| {
                        let super::PinResource::MetadataObject(path) = resource else {
                            return Err(MetadataError::Corruption(
                                "metadata recovery cannot settle payload claims".into(),
                            ));
                        };
                        let digest = prefixes.iter().find_map(|prefix| path.strip_prefix(prefix));
                        if !digest.is_some_and(|digest| {
                            digest.len() == 64
                                && digest.bytes().all(|byte| {
                                    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
                                })
                        }) {
                            return Err(MetadataError::Corruption(
                                "metadata recovery path is outside this shard namespace".into(),
                            ));
                        }
                        if live.contains(path)
                            || inventory
                                .pins
                                .values()
                                .any(|pin| pin.resources.contains(resource))
                        {
                            return Err(MetadataError::Corruption(
                                "claimed metadata is still referenced; recovery refused".into(),
                            ));
                        }
                        Ok(path.clone())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((token.clone(), paths))
            })
            .collect()
    }

    pub(super) async fn recover_deletion_paths(
        &self,
        paths: Vec<(super::PinToken, Vec<String>)>,
        pins: Arc<dyn super::PinStore>,
    ) -> Result<(), MetadataError> {
        for (token, paths) in paths {
            #[cfg(test)]
            let pause = self.deletion_pause.lock().unwrap().take();
            #[cfg(test)]
            if let Some(pause) = pause {
                pause.reached.notify_one();
                pause.resume.notified().await;
            }
            self.delete_paths(&paths).await?;
            pins.finish_deletions(&token).await?;
        }
        Ok(())
    }
}

fn boxed_storage_future<'a, T>(
    future: impl Future<Output = T> + Send + 'a,
) -> Pin<Box<dyn Future<Output = T> + Send + 'a>> {
    Box::pin(future)
}

impl ShardCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            bytes: 0,
            values: BTreeMap::new(),
            recency: VecDeque::new(),
        }
    }

    fn get(&mut self, digest: Digest) -> Option<Arc<Vec<u8>>> {
        let value = self.values.get(&digest)?.clone();
        self.touch(digest);
        Some(value)
    }

    fn insert(&mut self, digest: Digest, bytes: Arc<Vec<u8>>) {
        if bytes.len() > self.capacity {
            return;
        }
        if let Some(previous) = self.values.insert(digest, bytes.clone()) {
            self.bytes = self.bytes.saturating_sub(previous.len());
        }
        self.bytes = self.bytes.saturating_add(bytes.len());
        self.touch(digest);
        while self.bytes > self.capacity {
            let Some(oldest) = self.recency.pop_front() else {
                break;
            };
            if let Some(removed) = self.values.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(removed.len());
            }
        }
    }

    fn touch(&mut self, digest: Digest) {
        self.recency.retain(|candidate| *candidate != digest);
        self.recency.push_back(digest);
    }
}

fn shard_corruption(reference: &ObjectShardRef, error: io::Error) -> MetadataError {
    MetadataError::Corruption(format!(
        "logical object shard {} is invalid: {error}",
        reference.digest
    ))
}

fn root_shard_corruption(reference: &RootShardRef, error: io::Error) -> MetadataError {
    MetadataError::Corruption(format!(
        "logical root shard {} is invalid: {error}",
        reference.digest
    ))
}

fn encode_barrier(state: &BarrierState) -> Vec<u8> {
    let mut body = Vec::with_capacity(8 + 1 + 32);
    body.extend_from_slice(&state.generation.to_le_bytes());
    body.push(state.mode as u8);
    body.extend_from_slice(&state.token);
    let checksum = domain_hash(BARRIER_DOMAIN, &body);
    let mut encoded = Vec::with_capacity(8 + DIGEST_BYTES + body.len());
    encoded.extend_from_slice(BARRIER_MAGIC_V1);
    encoded.extend_from_slice(checksum.as_bytes());
    encoded.extend_from_slice(&body);
    encoded
}

fn decode_barrier(bytes: &[u8]) -> Result<BarrierState, MetadataError> {
    if bytes.len() != 8 + DIGEST_BYTES + 8 + 1 + 32 || &bytes[..8] != BARRIER_MAGIC_V1 {
        return Err(MetadataError::Corruption(
            "invalid logical shard barrier".to_owned(),
        ));
    }
    let expected = Digest::try_from(&bytes[8..8 + DIGEST_BYTES]).map_err(|error| {
        MetadataError::Corruption(format!("invalid logical shard barrier checksum: {error}"))
    })?;
    let body = &bytes[8 + DIGEST_BYTES..];
    if domain_hash(BARRIER_DOMAIN, body) != expected {
        return Err(MetadataError::Corruption(
            "logical shard barrier checksum mismatch".to_owned(),
        ));
    }
    let generation = u64::from_le_bytes(body[..8].try_into().expect("exact generation width"));
    let mode = match body[8] {
        0 => BarrierMode::Idle,
        1 => BarrierMode::Checkpoint,
        2 => BarrierMode::GarbageCollection,
        _ => {
            return Err(MetadataError::Corruption(
                "logical shard barrier has an invalid mode".to_owned(),
            ));
        }
    };
    let token = body[9..].try_into().expect("exact barrier token width");
    if (mode == BarrierMode::Idle) != (token == [0; 32]) {
        return Err(MetadataError::Corruption(
            "logical shard barrier token does not match its mode".to_owned(),
        ));
    }
    Ok(BarrierState {
        generation,
        mode,
        token,
    })
}

impl StateShardMap {
    pub(super) fn object_shard(&self, key: &ObjectKey) -> Option<&ObjectShardRef> {
        let at = self.objects.partition_point(|shard| shard.last < *key);
        self.objects.get(at).filter(|shard| shard.first <= *key)
    }

    pub(super) fn root_shard(&self, name: &RootName) -> Option<&RootShardRef> {
        let at = self.roots.partition_point(|shard| shard.last < *name);
        self.roots.get(at).filter(|shard| shard.first <= *name)
    }

    fn validate(&self) -> io::Result<()> {
        if self.objects.len() > MAX_SHARD_REFS {
            return Err(io::Error::other("too many logical object shard references"));
        }
        let mut previous = None;
        let mut object_count = 0_u64;
        let mut validated_count = 0_u64;
        for shard in &self.objects {
            if shard.first > shard.last
                || shard.entries == 0
                || shard.validated > shard.entries
                || shard.encoded_bytes == 0
                || previous.as_ref().is_some_and(|last| last >= &shard.first)
            {
                return Err(io::Error::other("invalid logical object shard reference"));
            }
            previous = Some(shard.last.clone());
            object_count = object_count
                .checked_add(shard.entries)
                .ok_or_else(|| io::Error::other("logical object count overflow"))?;
            validated_count = validated_count
                .checked_add(shard.validated)
                .ok_or_else(|| io::Error::other("logical validation count overflow"))?;
        }
        if object_count != self.object_count || validated_count != self.validated_count {
            return Err(io::Error::other(
                "logical shard-map totals do not match its references",
            ));
        }
        let mut previous = None;
        let mut root_count = 0_u64;
        for shard in &self.roots {
            if shard.first > shard.last
                || shard.entries == 0
                || shard.encoded_bytes == 0
                || previous.as_ref().is_some_and(|last| last >= &shard.first)
            {
                return Err(io::Error::other("invalid logical root shard reference"));
            }
            previous = Some(shard.last.clone());
            root_count = root_count
                .checked_add(shard.entries)
                .ok_or_else(|| io::Error::other("logical root count overflow"))?;
        }
        if self.roots.len() > MAX_SHARD_REFS || root_count != self.root_count {
            return Err(io::Error::other(
                "logical root shard-map totals do not match its references",
            ));
        }
        Ok(())
    }
}

pub(super) fn encode_state_shard_map(map: &StateShardMap) -> io::Result<Bytes> {
    map.validate()?;
    let mut body = Vec::new();
    body.extend_from_slice(&map.object_count.to_le_bytes());
    body.extend_from_slice(&map.validated_count.to_le_bytes());
    body.extend_from_slice(&(map.objects.len() as u64).to_le_bytes());
    for shard in &map.objects {
        put_bytes(&mut body, &shard.first.encode());
        put_bytes(&mut body, &shard.last.encode());
        body.extend_from_slice(shard.digest.as_bytes());
        body.extend_from_slice(&shard.entries.to_le_bytes());
        body.extend_from_slice(&shard.validated.to_le_bytes());
        body.extend_from_slice(&shard.encoded_bytes.to_le_bytes());
    }
    body.extend_from_slice(&map.root_count.to_le_bytes());
    body.extend_from_slice(&(map.roots.len() as u64).to_le_bytes());
    for shard in &map.roots {
        put_bytes(&mut body, shard.first.as_str().as_bytes());
        put_bytes(&mut body, shard.last.as_str().as_bytes());
        body.extend_from_slice(shard.digest.as_bytes());
        body.extend_from_slice(&shard.entries.to_le_bytes());
        body.extend_from_slice(&shard.encoded_bytes.to_le_bytes());
    }
    let checksum = domain_hash(MAP_DOMAIN, &body);
    let mut encoded = Vec::with_capacity(8 + DIGEST_BYTES + body.len());
    encoded.extend_from_slice(MAP_MAGIC_V1);
    encoded.extend_from_slice(checksum.as_bytes());
    encoded.extend_from_slice(&body);
    if encoded.len() > MAX_STATE_MAP_BYTES {
        return Err(io::Error::other(
            "logical state shard map exceeds its size limit",
        ));
    }
    Ok(encoded.into())
}

pub(super) fn decode_state_shard_map(bytes: &[u8]) -> io::Result<StateShardMap> {
    if bytes.len() < 8 + DIGEST_BYTES + 24
        || bytes.len() > MAX_STATE_MAP_BYTES
        || &bytes[..8] != MAP_MAGIC_V1
    {
        return Err(io::Error::other("invalid logical state shard map"));
    }
    let expected = Digest::try_from(&bytes[8..8 + DIGEST_BYTES]).map_err(io::Error::other)?;
    let body = &bytes[8 + DIGEST_BYTES..];
    if domain_hash(MAP_DOMAIN, body) != expected {
        return Err(io::Error::other(
            "logical state shard map checksum mismatch",
        ));
    }
    let mut input = Input::new(body);
    let object_count = input.u64()?;
    let validated_count = input.u64()?;
    let count = input.count(MAX_SHARD_REFS, 64)?;
    let mut objects = Vec::with_capacity(count);
    for _ in 0..count {
        let first = ObjectKey::decode(input.bytes()?).map_err(io::Error::other)?;
        let last = ObjectKey::decode(input.bytes()?).map_err(io::Error::other)?;
        let digest = Digest::try_from(input.take(DIGEST_BYTES)?).map_err(io::Error::other)?;
        objects.push(ObjectShardRef {
            first,
            last,
            digest,
            entries: input.u64()?,
            validated: input.u64()?,
            encoded_bytes: input.u64()?,
        });
    }
    let root_count = input.u64()?;
    let count = input.count(MAX_SHARD_REFS, 56)?;
    let mut roots = Vec::with_capacity(count);
    for _ in 0..count {
        let first = RootName::try_from(
            std::str::from_utf8(input.bytes()?)
                .map_err(|_| io::Error::other("logical root boundary is not UTF-8"))?,
        )
        .map_err(io::Error::other)?;
        let last = RootName::try_from(
            std::str::from_utf8(input.bytes()?)
                .map_err(|_| io::Error::other("logical root boundary is not UTF-8"))?,
        )
        .map_err(io::Error::other)?;
        roots.push(RootShardRef {
            first,
            last,
            digest: Digest::try_from(input.take(DIGEST_BYTES)?).map_err(io::Error::other)?,
            entries: input.u64()?,
            encoded_bytes: input.u64()?,
        });
    }
    input.finish()?;
    let map = StateShardMap {
        objects,
        object_count,
        validated_count,
        roots,
        root_count,
    };
    map.validate()?;
    Ok(map)
}

pub(super) fn encode_object_shard(
    entries: &[(ObjectRecord, bool, u64)],
) -> io::Result<EncodedObjectShard> {
    if entries.is_empty() {
        return Err(io::Error::other(
            "refusing to encode an empty logical object shard",
        ));
    }
    for pair in entries.windows(2) {
        if pair[0].0.key() >= pair[1].0.key() {
            return Err(io::Error::other(
                "logical object shard records are not strictly ordered",
            ));
        }
    }
    let mut body = Vec::new();
    let mut blocks = Vec::new();
    for chunk in entries.chunks(OBJECT_BLOCK_ENTRIES) {
        let offset = (8 + DIGEST_BYTES + body.len()) as u64;
        let block = encode_object_block(chunk);
        let digest = Digest::from(blake3::hash(&block));
        blocks.push(ObjectBlockRef {
            first: chunk.first().expect("nonempty block").0.key().clone(),
            last: chunk.last().expect("nonempty block").0.key().clone(),
            offset,
            encoded_bytes: block.len() as u64,
            digest,
        });
        body.extend_from_slice(&block);
    }
    let directory_offset = (8 + DIGEST_BYTES + body.len()) as u64;
    let directory = encode_block_directory(&blocks);
    let directory_digest = Digest::from(blake3::hash(&directory));
    body.extend_from_slice(&directory);
    body.extend_from_slice(TRAILER_MAGIC_V1);
    body.extend_from_slice(&directory_offset.to_le_bytes());
    body.extend_from_slice(&(directory.len() as u64).to_le_bytes());
    body.extend_from_slice(directory_digest.as_bytes());
    let checksum = domain_hash(SHARD_DOMAIN, &body);
    let mut encoded = Vec::with_capacity(8 + DIGEST_BYTES + body.len());
    encoded.extend_from_slice(SHARD_MAGIC_V1);
    encoded.extend_from_slice(checksum.as_bytes());
    encoded.extend_from_slice(&body);
    if encoded.len() > MAX_OBJECT_SHARD_BYTES {
        return Err(io::Error::other(
            "logical object shard exceeds its size limit",
        ));
    }
    let digest = Digest::from(blake3::hash(&encoded));
    let validated = entries
        .iter()
        .filter(|(_, validated, _)| *validated)
        .count() as u64;
    Ok(EncodedObjectShard {
        reference: ObjectShardRef {
            first: entries.first().expect("nonempty shard").0.key().clone(),
            last: entries.last().expect("nonempty shard").0.key().clone(),
            digest,
            entries: entries.len() as u64,
            validated,
            encoded_bytes: encoded.len() as u64,
        },
        bytes: encoded.into(),
    })
}

pub(super) fn encode_root_shard(entries: &[RootRecord]) -> io::Result<EncodedRootShard> {
    if entries.is_empty() {
        return Err(io::Error::other(
            "refusing to encode an empty logical root shard",
        ));
    }
    for pair in entries.windows(2) {
        if pair[0].name() >= pair[1].name() {
            return Err(io::Error::other(
                "logical root shard records are not strictly ordered",
            ));
        }
    }
    let mut body = Vec::new();
    body.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for root in entries {
        put_bytes(&mut body, &root.encode());
    }
    let checksum = domain_hash(ROOT_SHARD_DOMAIN, &body);
    let mut encoded = Vec::with_capacity(8 + DIGEST_BYTES + body.len());
    encoded.extend_from_slice(ROOT_SHARD_MAGIC_V1);
    encoded.extend_from_slice(checksum.as_bytes());
    encoded.extend_from_slice(&body);
    if encoded.len() > MAX_OBJECT_SHARD_BYTES {
        return Err(io::Error::other(
            "logical root shard exceeds its size limit",
        ));
    }
    let digest = Digest::from(blake3::hash(&encoded));
    Ok(EncodedRootShard {
        reference: RootShardRef {
            first: entries.first().expect("nonempty root shard").name().clone(),
            last: entries.last().expect("nonempty root shard").name().clone(),
            digest,
            entries: entries.len() as u64,
            encoded_bytes: encoded.len() as u64,
        },
        bytes: encoded.into(),
    })
}

#[cfg(test)]
pub(super) fn decode_root_shard(
    bytes: &[u8],
    reference: &RootShardRef,
) -> io::Result<Vec<RootRecord>> {
    verify_root_shard(bytes, reference)?;
    decode_verified_root_shard(bytes, reference)
}

fn decode_verified_root_shard(
    bytes: &[u8],
    reference: &RootShardRef,
) -> io::Result<Vec<RootRecord>> {
    let mut input = Input::new(&bytes[8 + DIGEST_BYTES..]);
    let count = input.count(
        usize::try_from(reference.entries)
            .map_err(|_| io::Error::other("logical root count overflows usize"))?,
        8,
    )?;
    if count as u64 != reference.entries {
        return Err(io::Error::other("logical root shard count mismatch"));
    }
    let mut roots = Vec::with_capacity(count);
    for _ in 0..count {
        let root = RootRecord::decode(input.bytes()?).map_err(io::Error::other)?;
        if roots
            .last()
            .is_some_and(|previous: &RootRecord| previous.name() >= root.name())
        {
            return Err(io::Error::other(
                "logical root shard records are not strictly ordered",
            ));
        }
        roots.push(root);
    }
    input.finish()?;
    if roots.first().map(RootRecord::name) != Some(&reference.first)
        || roots.last().map(RootRecord::name) != Some(&reference.last)
    {
        return Err(io::Error::other("logical root shard metadata mismatch"));
    }
    Ok(roots)
}

#[cfg(test)]
pub(super) fn lookup_root_shard(
    bytes: &[u8],
    reference: &RootShardRef,
    name: &RootName,
) -> io::Result<Option<ObjectKey>> {
    verify_root_shard(bytes, reference)?;
    lookup_verified_root_shard(bytes, reference, name)
}

fn lookup_verified_root_shard(
    bytes: &[u8],
    reference: &RootShardRef,
    name: &RootName,
) -> io::Result<Option<ObjectKey>> {
    if name < &reference.first || name > &reference.last {
        return Ok(None);
    }
    let roots = decode_verified_root_shard(bytes, reference)?;
    Ok(roots
        .binary_search_by(|root| root.name().cmp(name))
        .ok()
        .map(|at| roots[at].target().clone()))
}

fn verify_root_shard(bytes: &[u8], reference: &RootShardRef) -> io::Result<()> {
    if bytes.len() < 8 + DIGEST_BYTES + 8
        || bytes.len() > MAX_OBJECT_SHARD_BYTES
        || &bytes[..8] != ROOT_SHARD_MAGIC_V1
        || bytes.len() as u64 != reference.encoded_bytes
        || Digest::from(blake3::hash(bytes)) != reference.digest
    {
        return Err(io::Error::other("invalid logical root shard"));
    }
    let expected = Digest::try_from(&bytes[8..8 + DIGEST_BYTES]).map_err(io::Error::other)?;
    if domain_hash(ROOT_SHARD_DOMAIN, &bytes[8 + DIGEST_BYTES..]) != expected {
        return Err(io::Error::other("logical root shard checksum mismatch"));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn lookup_object_shard(
    bytes: &[u8],
    reference: &ObjectShardRef,
    key: &ObjectKey,
) -> io::Result<Option<(ObjectRecord, bool, u64)>> {
    verify_object_shard(bytes, reference)?;
    lookup_verified_object_shard(bytes, reference, key)
}

fn lookup_verified_object_shard(
    bytes: &[u8],
    reference: &ObjectShardRef,
    key: &ObjectKey,
) -> io::Result<Option<(ObjectRecord, bool, u64)>> {
    if key < &reference.first || key > &reference.last {
        return Ok(None);
    }
    let blocks = decode_block_directory(bytes)?;
    let at = blocks.partition_point(|block| block.last < *key);
    let Some(block) = blocks.get(at).filter(|block| block.first <= *key) else {
        return Ok(None);
    };
    let encoded = exact_range(bytes, block.offset, block.encoded_bytes)?;
    if Digest::from(blake3::hash(encoded)) != block.digest {
        return Err(io::Error::other("logical object block checksum mismatch"));
    }
    lookup_object_block(encoded, key)
}

#[cfg(test)]
pub(super) fn decode_object_shard(
    bytes: &[u8],
    reference: &ObjectShardRef,
) -> io::Result<Vec<(ObjectRecord, bool, u64)>> {
    verify_object_shard(bytes, reference)?;
    decode_verified_object_shard(bytes, reference)
}

fn decode_verified_object_shard(
    bytes: &[u8],
    reference: &ObjectShardRef,
) -> io::Result<Vec<(ObjectRecord, bool, u64)>> {
    let blocks = decode_block_directory(bytes)?;
    let capacity = usize::try_from(reference.entries)
        .map_err(|_| io::Error::other("logical object count overflows usize"))?;
    let mut entries = Vec::with_capacity(capacity);
    for block in blocks {
        let encoded = exact_range(bytes, block.offset, block.encoded_bytes)?;
        if Digest::from(blake3::hash(encoded)) != block.digest {
            return Err(io::Error::other("logical object block checksum mismatch"));
        }
        entries.extend(decode_object_block(encoded)?);
    }
    if entries.len() as u64 != reference.entries
        || entries
            .iter()
            .filter(|(_, validated, _)| *validated)
            .count() as u64
            != reference.validated
        || entries.first().map(|entry| entry.0.key()) != Some(&reference.first)
        || entries.last().map(|entry| entry.0.key()) != Some(&reference.last)
    {
        return Err(io::Error::other("logical object shard metadata mismatch"));
    }
    Ok(entries)
}

fn verify_object_shard(bytes: &[u8], reference: &ObjectShardRef) -> io::Result<()> {
    if bytes.len() < 8 + DIGEST_BYTES + TRAILER_BYTES
        || bytes.len() > MAX_OBJECT_SHARD_BYTES
        || &bytes[..8] != SHARD_MAGIC_V1
        || bytes.len() as u64 != reference.encoded_bytes
        || Digest::from(blake3::hash(bytes)) != reference.digest
    {
        return Err(io::Error::other("invalid logical object shard"));
    }
    let expected = Digest::try_from(&bytes[8..8 + DIGEST_BYTES]).map_err(io::Error::other)?;
    if domain_hash(SHARD_DOMAIN, &bytes[8 + DIGEST_BYTES..]) != expected {
        return Err(io::Error::other("logical object shard checksum mismatch"));
    }
    Ok(())
}

fn encode_object_block(entries: &[(ObjectRecord, bool, u64)]) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(BLOCK_MAGIC_V2);
    encoded.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for (record, _, _) in entries {
        put_bytes(&mut encoded, &record.encode());
    }
    let mut validated = vec![0_u8; entries.len().div_ceil(8)];
    for (index, (_, is_validated, _)) in entries.iter().enumerate() {
        if *is_validated {
            validated[index / 8] |= 1 << (index % 8);
        }
    }
    encoded.extend_from_slice(&validated);
    for (_, _, generation) in entries {
        encoded.extend_from_slice(&generation.to_le_bytes());
    }
    encoded
}

fn lookup_object_block(
    bytes: &[u8],
    key: &ObjectKey,
) -> io::Result<Option<(ObjectRecord, bool, u64)>> {
    for (record, validated, generation) in decode_object_block(bytes)? {
        match record.key().cmp(key) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => return Ok(Some((record, validated, generation))),
            std::cmp::Ordering::Greater => return Ok(None),
        }
    }
    Ok(None)
}

fn decode_object_block(bytes: &[u8]) -> io::Result<Vec<(ObjectRecord, bool, u64)>> {
    let mut input = Input::new(bytes);
    let magic = input.take(8)?;
    if magic != BLOCK_MAGIC_V1 && magic != BLOCK_MAGIC_V2 {
        return Err(io::Error::other("invalid logical object block"));
    }
    let count = input.count(OBJECT_BLOCK_ENTRIES, 8)?;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let record = ObjectRecord::decode(input.bytes()?).map_err(io::Error::other)?;
        if entries
            .last()
            .is_some_and(|previous: &ObjectRecord| previous.key() >= record.key())
        {
            return Err(io::Error::other(
                "logical object block records are not strictly ordered",
            ));
        }
        entries.push(record);
    }
    let validated = input.take(count.div_ceil(8))?;
    if !count.is_multiple_of(8)
        && validated
            .last()
            .is_some_and(|last| last >> (count % 8) != 0)
    {
        return Err(io::Error::other(
            "logical validation bitset has nonzero padding bits",
        ));
    }
    let generations = if magic == BLOCK_MAGIC_V2 {
        (0..count)
            .map(|_| input.u64())
            .collect::<io::Result<Vec<_>>>()?
    } else {
        vec![0; count]
    };
    input.finish()?;
    Ok(entries
        .into_iter()
        .enumerate()
        .map(|(index, record)| {
            (
                record,
                validated[index / 8] & (1 << (index % 8)) != 0,
                generations[index],
            )
        })
        .collect())
}

fn encode_block_directory(blocks: &[ObjectBlockRef]) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(DIRECTORY_MAGIC_V1);
    encoded.extend_from_slice(&(blocks.len() as u64).to_le_bytes());
    for block in blocks {
        put_bytes(&mut encoded, &block.first.encode());
        put_bytes(&mut encoded, &block.last.encode());
        encoded.extend_from_slice(&block.offset.to_le_bytes());
        encoded.extend_from_slice(&block.encoded_bytes.to_le_bytes());
        encoded.extend_from_slice(block.digest.as_bytes());
    }
    encoded
}

fn decode_block_directory(bytes: &[u8]) -> io::Result<Vec<ObjectBlockRef>> {
    let trailer_start = bytes
        .len()
        .checked_sub(TRAILER_BYTES)
        .ok_or_else(|| io::Error::other("truncated logical object shard trailer"))?;
    let mut trailer = Input::new(&bytes[trailer_start..]);
    if trailer.take(8)? != TRAILER_MAGIC_V1 {
        return Err(io::Error::other("invalid logical object shard trailer"));
    }
    let offset = trailer.u64()?;
    let len = trailer.u64()?;
    let expected = Digest::try_from(trailer.take(DIGEST_BYTES)?).map_err(io::Error::other)?;
    trailer.finish()?;
    let directory = exact_range(bytes, offset, len)?;
    if offset
        .checked_add(len)
        .is_none_or(|end| end != trailer_start as u64)
        || Digest::from(blake3::hash(directory)) != expected
    {
        return Err(io::Error::other(
            "logical object directory checksum mismatch",
        ));
    }
    let mut input = Input::new(directory);
    if input.take(8)? != DIRECTORY_MAGIC_V1 {
        return Err(io::Error::other("invalid logical object block directory"));
    }
    let count = input.count(MAX_SHARD_REFS, 64)?;
    let mut blocks = Vec::with_capacity(count);
    for _ in 0..count {
        let first = ObjectKey::decode(input.bytes()?).map_err(io::Error::other)?;
        let last = ObjectKey::decode(input.bytes()?).map_err(io::Error::other)?;
        let block = ObjectBlockRef {
            first,
            last,
            offset: input.u64()?,
            encoded_bytes: input.u64()?,
            digest: Digest::try_from(input.take(DIGEST_BYTES)?).map_err(io::Error::other)?,
        };
        if block.first > block.last
            || block.offset < (8 + DIGEST_BYTES) as u64
            || block.encoded_bytes == 0
            || block
                .offset
                .checked_add(block.encoded_bytes)
                .is_none_or(|end| end > offset)
            || blocks
                .last()
                .is_some_and(|previous: &ObjectBlockRef| previous.last >= block.first)
        {
            return Err(io::Error::other("invalid logical object block reference"));
        }
        blocks.push(block);
    }
    input.finish()?;
    if blocks.is_empty() {
        return Err(io::Error::other("logical object shard has no blocks"));
    }
    Ok(blocks)
}

/// Estimate the number of fixed-byte shards and routing bytes without
/// allocating records. Used by the frontier benchmark to model billions of
/// logical objects backed by 500 TB of physical chunks.
#[cfg(test)]
pub(super) fn estimate_layout(
    entries: u64,
    average_record_bytes: u64,
    target_shard_bytes: u64,
    average_boundary_key_bytes: u64,
) -> io::Result<(u64, u64)> {
    if average_record_bytes == 0 || target_shard_bytes == 0 {
        return Err(io::Error::other(
            "logical shard layout inputs must be non-zero",
        ));
    }
    let total = u128::from(entries).saturating_mul(u128::from(average_record_bytes));
    let shards = total.div_ceil(u128::from(target_shard_bytes));
    let ref_bytes = u128::from(64 + average_boundary_key_bytes.saturating_mul(2));
    let map_bytes = 8_u128 + DIGEST_BYTES as u128 + 24 + shards.saturating_mul(ref_bytes);
    Ok((
        u64::try_from(shards).map_err(|_| io::Error::other("logical shard count overflow"))?,
        u64::try_from(map_bytes).map_err(|_| io::Error::other("logical map size overflow"))?,
    ))
}

fn domain_hash(domain: &[u8], body: &[u8]) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(body);
    Digest::from(hasher.finalize())
}

fn exact_range(bytes: &[u8], offset: u64, len: u64) -> io::Result<&[u8]> {
    let start = usize::try_from(offset).map_err(|_| io::Error::other("range offset overflow"))?;
    let len = usize::try_from(len).map_err(|_| io::Error::other("range length overflow"))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| io::Error::other("range end overflow"))?;
    bytes
        .get(start..end)
        .ok_or_else(|| io::Error::other("range lies outside logical object shard"))
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(bytes);
}

struct Input<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Input<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> io::Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| io::Error::other("truncated logical state shard"))?;
        let result = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(result)
    }

    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("eight bytes"),
        ))
    }

    fn bytes(&mut self) -> io::Result<&'a [u8]> {
        let len = usize::try_from(self.u64()?)
            .map_err(|_| io::Error::other("logical shard byte length overflow"))?;
        self.take(len)
    }

    fn count(&mut self, limit: usize, minimum_entry_bytes: usize) -> io::Result<usize> {
        let count = usize::try_from(self.u64()?)
            .map_err(|_| io::Error::other("logical shard count overflow"))?;
        if count > limit
            || count
                > self
                    .bytes
                    .len()
                    .saturating_sub(self.offset)
                    .checked_div(minimum_entry_bytes)
                    .unwrap_or(0)
        {
            return Err(io::Error::other(
                "logical shard count exceeds remaining bytes",
            ));
        }
        Ok(count)
    }

    fn finish(&self) -> io::Result<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(io::Error::other("trailing bytes in logical state shard"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BlobId, NamespaceId};

    fn record(index: u32) -> ObjectRecord {
        let key = ObjectKey::new(
            NamespaceId::try_from("bench.logical.v1").unwrap(),
            index.to_be_bytes().to_vec(),
        )
        .unwrap();
        let payload = BlobId::new(Digest::from(*blake3::hash(&index.to_le_bytes()).as_bytes()));
        ObjectRecord::new(key, payload, index as u64, Vec::new()).unwrap()
    }

    fn root(index: u32) -> RootRecord {
        RootRecord::new(
            RootName::try_from(format!("root-{index:08}")).unwrap(),
            record(index).key().clone(),
        )
    }

    #[test]
    fn legacy_object_blocks_receive_generation_zero() {
        let entries = vec![(record(1), true, 17), (record(2), false, 29)];
        let mut bytes = encode_object_block(&entries);
        bytes[..8].copy_from_slice(BLOCK_MAGIC_V1);
        bytes.truncate(bytes.len() - entries.len() * 8);
        let decoded = decode_object_block(&bytes).unwrap();
        assert_eq!(decoded, vec![(record(1), true, 0), (record(2), false, 0)]);
        let current = encode_object_block(&decoded);
        assert_eq!(decode_object_block(&current).unwrap(), decoded);
    }

    #[tokio::test]
    async fn checkpoint_admission_reports_typed_preappend_fence() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage::Local(chroma_storage::local::LocalStorage::new(
            directory.path().to_str().unwrap(),
        )));
        let shards = ObjectShardStorage::new(storage, "checkpoint-fence".into(), 0);
        let gc = shards.acquire_gc_barrier().await.unwrap();
        assert!(matches!(
            shards.acquire_checkpoint_barrier().await,
            Err(MetadataError::MaintenanceFenced)
        ));
        assert!(shards.owns_barrier(&gc).await.unwrap());
        shards.release_barrier(&gc).await.unwrap();
        let checkpoint = shards.acquire_checkpoint_barrier().await.unwrap();
        assert!(shards.owns_barrier(&checkpoint).await.unwrap());
        shards.release_barrier(&checkpoint).await.unwrap();
    }

    #[tokio::test]
    async fn metadata_write_pins_refuse_claimed_paths_and_cover_the_whole_operation() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage::Local(chroma_storage::local::LocalStorage::new(
            directory.path().to_str().unwrap(),
        )));
        let shards = ObjectShardStorage::new(storage.clone(), "claimed-writes".into(), 0);
        let ledger =
            super::super::pins::chroma_pin_store(storage, "claimed-writes/online-pins-v1".into());
        let object = encode_object_shard(&[(record(1), true, 1)]).unwrap();
        let root = encode_root_shard(&[root(1)]).unwrap();
        let object_path = shards.path(object.reference.digest);
        let root_path = shards.root_path(root.reference.digest);
        let object_resource = super::super::PinResource::MetadataObject(object_path.clone());
        let root_resource = super::super::PinResource::MetadataObject(root_path.clone());
        let claimed = ledger
            .claim_deletions(
                ledger.inventory().await.unwrap().revision,
                BTreeSet::from([object_resource.clone()]),
            )
            .await
            .unwrap()
            .unwrap();
        let operation = shards.for_write_operation();
        assert!(matches!(
            operation.put(&object).await,
            Err(MetadataError::Transient(_))
        ));
        assert!(!shards.list_paths().await.unwrap().contains(&object_path));
        ledger.finish_deletions(&claimed).await.unwrap();
        operation.put(&object).await.unwrap();
        let claimed = ledger
            .claim_deletions(
                ledger.inventory().await.unwrap().revision,
                BTreeSet::from([root_resource.clone()]),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            operation.put_root(&root).await,
            Err(MetadataError::Transient(_))
        ));
        assert!(!shards.list_paths().await.unwrap().contains(&root_path));
        ledger.finish_deletions(&claimed).await.unwrap();
        operation.put_root(&root).await.unwrap();
        let inventory = ledger.inventory().await.unwrap();
        assert_eq!(inventory.pins.len(), 1);
        assert_eq!(
            inventory.pins.values().next().unwrap().resources,
            BTreeSet::from([object_resource.clone(), root_resource])
        );
        assert!(
            ledger
                .claim_deletions(inventory.revision, BTreeSet::from([object_resource]))
                .await
                .unwrap()
                .is_none()
        );
        drop(operation);
        super::super::flush_pin_releases().await;
        assert!(ledger.inventory().await.unwrap().pins.is_empty());
    }

    #[test]
    fn metadata_recovery_rejects_referenced_foreign_and_payload_paths() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage::Local(chroma_storage::local::LocalStorage::new(
            directory.path().to_str().unwrap(),
        )));
        let shards = ObjectShardStorage::new(storage, "recovery-boundary".into(), 0);
        let token: super::super::PinToken = "22".repeat(32).parse().unwrap();
        let expected = BTreeSet::from([token.clone()]);
        let path = shards.path(Digest::hash(b"claimed"));
        let resource = super::super::PinResource::MetadataObject(path.clone());
        let mut inventory = super::super::PinInventory {
            deletions: BTreeMap::from([(token.clone(), BTreeSet::from([resource.clone()]))]),
            ..Default::default()
        };
        assert!(
            shards
                .deletion_recovery_paths(&inventory, &expected, &BTreeSet::from([path.clone()]))
                .is_err()
        );
        assert!(
            shards
                .deletion_recovery_paths(&inventory, &BTreeSet::new(), &BTreeSet::new())
                .is_err()
        );
        assert_eq!(
            shards
                .deletion_recovery_paths(&inventory, &expected, &BTreeSet::new())
                .unwrap(),
            vec![(token.clone(), vec![path])]
        );
        for invalid in [
            super::super::PinResource::MetadataObject("other/state-objects/b3/arbitrary".into()),
            super::super::PinResource::MetadataObject(
                "recovery-boundary/state-shards-gc-current".into(),
            ),
            super::super::PinResource::Blob(crate::BlobId::new(Digest::hash(b"payload"))),
        ] {
            inventory
                .deletions
                .insert(token.clone(), BTreeSet::from([invalid]));
            assert!(
                shards
                    .deletion_recovery_paths(&inventory, &expected, &BTreeSet::new())
                    .is_err()
            );
        }
        inventory
            .deletions
            .insert(token.clone(), BTreeSet::from([resource]));
        inventory.collector = Some(token);
        assert!(
            shards
                .deletion_recovery_paths(&inventory, &expected, &BTreeSet::new())
                .is_err()
        );
    }

    #[test]
    fn object_shard_routes_blocks_and_round_trips_in_key_order() {
        let entries = (0..1_100)
            .map(|index| (record(index), index % 3 == 0, u64::from(index)))
            .collect::<Vec<_>>();
        let encoded = encode_object_shard(&entries).unwrap();
        let map = StateShardMap {
            objects: vec![encoded.reference.clone()],
            object_count: entries.len() as u64,
            validated_count: entries.iter().filter(|entry| entry.1).count() as u64,
            ..StateShardMap::default()
        };
        let map = decode_state_shard_map(&encode_state_shard_map(&map).unwrap()).unwrap();
        assert_eq!(map.object_shard(entries[777].0.key()), map.objects.first());
        assert_eq!(
            lookup_object_shard(&encoded.bytes, &encoded.reference, entries[777].0.key()).unwrap(),
            Some(entries[777].clone())
        );
        assert_eq!(
            decode_object_shard(&encoded.bytes, &encoded.reference).unwrap(),
            entries
        );
    }

    #[test]
    fn shard_and_map_corruption_are_rejected() {
        let encoded = encode_object_shard(&[(record(1), true, 17)]).unwrap();
        let mut corrupt = encoded.bytes.to_vec();
        let at = corrupt.len() / 2;
        corrupt[at] ^= 1;
        assert!(lookup_object_shard(&corrupt, &encoded.reference, record(1).key()).is_err());

        let map = StateShardMap {
            objects: vec![encoded.reference],
            object_count: 1,
            validated_count: 1,
            ..StateShardMap::default()
        };
        let mut corrupt = encode_state_shard_map(&map).unwrap().to_vec();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        assert!(decode_state_shard_map(&corrupt).is_err());
    }

    #[test]
    fn barrier_round_trips_and_rejects_corruption_and_invalid_state() {
        let state = BarrierState {
            generation: 42,
            mode: BarrierMode::GarbageCollection,
            token: [0x5a; 32],
        };
        let encoded = encode_barrier(&state);
        assert_eq!(decode_barrier(&encoded).unwrap(), state);

        let mut corrupt = encoded;
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        assert!(matches!(
            decode_barrier(&corrupt),
            Err(MetadataError::Corruption(_))
        ));
        assert!(matches!(
            decode_barrier(&encode_barrier(&BarrierState {
                generation: 43,
                mode: BarrierMode::Idle,
                token: [1; 32],
            })),
            Err(MetadataError::Corruption(_))
        ));
    }

    fn held(generation: u64, mode: BarrierMode, token: u8) -> BarrierState {
        BarrierState {
            generation,
            mode,
            token: [token; 32],
        }
    }

    fn idle(generation: u64) -> BarrierState {
        BarrierState {
            generation,
            mode: BarrierMode::Idle,
            token: [0; 32],
        }
    }

    #[test]
    fn barrier_acquisition_advances_the_generation_by_exactly_one() {
        use BarrierMode::{Checkpoint, GarbageCollection};
        let next = held(8, GarbageCollection, 1);
        assert_eq!(validate_barrier_acquire(&idle(7), &next, false), Ok(()));
        assert_eq!(
            validate_barrier_acquire(&held(7, Checkpoint, 2), &next, true),
            Ok(())
        );
        for generation in [0, 7, 9] {
            let next = held(generation, GarbageCollection, 1);
            assert!(validate_barrier_acquire(&idle(7), &next, false).is_err());
        }
        let wrapped = held(0, Checkpoint, 1);
        assert!(validate_barrier_acquire(&idle(u64::MAX), &wrapped, true).is_err());
        // A held barrier needs a mode and a token, or it no longer decodes.
        assert!(validate_barrier_acquire(&idle(7), &idle(8), false).is_err());
        let tokenless = held(8, Checkpoint, 0);
        assert!(validate_barrier_acquire(&idle(7), &tokenless, false).is_err());
        // A checkpoint must not preempt a collector.
        let collector = held(7, GarbageCollection, 2);
        let checkpoint = held(8, Checkpoint, 1);
        assert!(validate_barrier_acquire(&collector, &checkpoint, false).is_err());
    }

    #[test]
    fn only_the_barrier_owner_releases_it_and_keeps_the_generation() {
        use BarrierMode::{Checkpoint, GarbageCollection};
        let lease = ShardBarrierLease {
            generation: 8,
            mode: GarbageCollection,
            token: [1; 32],
            etag: None,
        };
        let mine = held(8, GarbageCollection, 1);
        assert_eq!(validate_barrier_release(&lease, &mine, &idle(8)), Ok(()));
        for other in [
            held(8, GarbageCollection, 2),
            held(9, GarbageCollection, 1),
            held(8, Checkpoint, 1),
            idle(8),
        ] {
            assert!(validate_barrier_release(&lease, &other, &idle(8)).is_err());
        }
        for next in [
            idle(7),
            idle(9),
            mine.clone(),
            BarrierState {
                token: [1; 32],
                ..idle(8)
            },
        ] {
            assert!(validate_barrier_release(&lease, &mine, &next).is_err());
        }
    }

    #[test]
    fn an_existing_shard_must_hash_to_its_name() {
        let object = encode_object_shard(&[(record(1), true, 1)]).unwrap();
        let roots = encode_root_shard(&[root(1)]).unwrap();
        assert_eq!(
            validate_existing_shard(object.reference.digest, &object.bytes),
            Ok(())
        );
        assert_eq!(
            validate_existing_shard(roots.reference.digest, &roots.bytes),
            Ok(())
        );
        let mut corrupt = object.bytes.to_vec();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        assert!(validate_existing_shard(object.reference.digest, &corrupt).is_err());
        assert!(validate_existing_shard(object.reference.digest, &roots.bytes).is_err());
    }

    #[test]
    fn root_shard_and_routing_map_round_trip_in_name_order() {
        let roots = (0..1_100).map(root).collect::<Vec<_>>();
        let encoded = encode_root_shard(&roots).unwrap();
        let map = StateShardMap {
            roots: vec![encoded.reference.clone()],
            root_count: roots.len() as u64,
            ..StateShardMap::default()
        };
        let map = decode_state_shard_map(&encode_state_shard_map(&map).unwrap()).unwrap();
        assert_eq!(map.root_shard(roots[777].name()), map.roots.first());
        assert_eq!(
            lookup_root_shard(&encoded.bytes, &encoded.reference, roots[777].name()).unwrap(),
            Some(roots[777].target().clone())
        );
        assert_eq!(
            decode_root_shard(&encoded.bytes, &encoded.reference).unwrap(),
            roots
        );
    }

    #[test]
    fn simulated_500_tb_worst_case_routing_map_stays_bounded() {
        let chunks = 500_000_000_000_000_u64.div_ceil(256 * 1024);
        let (shards, map_bytes) =
            estimate_layout(chunks, 192, DEFAULT_OBJECT_SHARD_TARGET_BYTES, 128).unwrap();
        assert!(shards < 50_000, "unexpected shard count {shards}");
        assert!(
            map_bytes < MAX_STATE_MAP_BYTES as u64,
            "routing map needs {map_bytes} bytes"
        );
    }

    #[tokio::test]
    async fn immutable_storage_uses_one_cold_get_then_the_bounded_cache() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage::Local(chroma_storage::local::LocalStorage::new(
            directory.path().to_str().unwrap(),
        )));
        let entries = (0..1_100)
            .map(|index| (record(index), index % 3 == 0, u64::from(index)))
            .collect::<Vec<_>>();
        let encoded = encode_object_shard(&entries).unwrap();
        let map = StateShardMap {
            objects: vec![encoded.reference.clone()],
            object_count: entries.len() as u64,
            validated_count: encoded.reference.validated,
            ..StateShardMap::default()
        };
        let writer = ObjectShardStorage::new(storage.clone(), "repo/state".to_owned(), 0);
        writer.put(&encoded).await.unwrap();
        assert_eq!(writer.stats().put_requests, 1);

        let reader =
            ObjectShardStorage::new(storage, "repo/state".to_owned(), encoded.bytes.len() * 2);
        assert_eq!(
            reader.lookup(&map, entries[700].0.key()).await.unwrap(),
            Some(entries[700].clone())
        );
        assert_eq!(
            reader.lookup(&map, entries[701].0.key()).await.unwrap(),
            Some(entries[701].clone())
        );
        assert_eq!(reader.stats().get_requests, 1);
        assert_eq!(reader.stats().cache_hits, 1);
        assert_eq!(reader.stats().get_bytes, encoded.bytes.len() as u64);
    }

    #[test]
    #[ignore = "release-mode WAL3 logical-state shard scale probe"]
    fn benchmark_logical_state_shards_scale() {
        let entries = std::env::var("CASITA_LOGICAL_STATE_BENCH_ENTRIES")
            .expect("CASITA_LOGICAL_STATE_BENCH_ENTRIES is required")
            .parse::<u32>()
            .expect("logical state entry count must fit u32");
        assert!(entries > 0);
        let started = std::time::Instant::now();
        let mut map = StateShardMap::default();
        let mut pending = Vec::new();
        let mut pending_bytes = 0_u64;
        let mut max_shard_bytes = 0_usize;
        let mut last_shard = None;
        for index in 0..entries {
            let entry = (record(index), index % 3 == 0, u64::from(index));
            let encoded_bytes = entry.0.encode().len() as u64 + 9;
            if !pending.is_empty()
                && pending_bytes.saturating_add(encoded_bytes) > DEFAULT_OBJECT_SHARD_TARGET_BYTES
            {
                let shard = encode_object_shard(&pending).unwrap();
                max_shard_bytes = max_shard_bytes.max(shard.bytes.len());
                map.object_count += shard.reference.entries;
                map.validated_count += shard.reference.validated;
                map.objects.push(shard.reference.clone());
                last_shard = Some(shard);
                pending.clear();
                pending_bytes = 0;
            }
            pending_bytes = pending_bytes.saturating_add(encoded_bytes);
            pending.push(entry);
        }
        if !pending.is_empty() {
            let shard = encode_object_shard(&pending).unwrap();
            max_shard_bytes = max_shard_bytes.max(shard.bytes.len());
            map.object_count += shard.reference.entries;
            map.validated_count += shard.reference.validated;
            map.objects.push(shard.reference.clone());
            last_shard = Some(shard);
        }
        let encoded_map = encode_state_shard_map(&map).unwrap();
        let last_shard = last_shard.unwrap();
        let key = record(entries - 1).key().clone();
        assert!(
            lookup_object_shard(&last_shard.bytes, &last_shard.reference, &key)
                .unwrap()
                .is_some()
        );
        let peak_rss_kib = std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| {
                status.lines().find_map(|line| {
                    line.strip_prefix("VmHWM:")?
                        .split_whitespace()
                        .next()?
                        .parse::<u64>()
                        .ok()
                })
            })
            .unwrap_or_default();
        println!("logical_state_entries {entries}");
        println!("logical_state_shards {}", map.objects.len());
        println!("logical_state_routing_bytes {}", encoded_map.len());
        println!("logical_state_max_shard_bytes {max_shard_bytes}");
        println!(
            "logical_state_build_nanos {}",
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
        );
        println!("logical_state_peak_rss_kib {peak_rss_kib}");
    }
}
