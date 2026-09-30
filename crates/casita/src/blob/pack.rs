//! Immutable, self-describing packs of compressed chunks.
//!
//! The footer is deliberately at the end: a pack can be assembled in one
//! sequential pass and an object-store reader can rebuild the index with a
//! 16-byte trailer read followed by an exact footer range. Pack objects are
//! named by the BLAKE3 digest of their complete bytes.
//!
//! Sparse collection publishes one immutable, content-addressed GC delta for
//! all deferred packs in the pass. Each delta carries sorted `(pack, bitmap)`
//! entries keyed to exact footer ordinals; rebuilding unions generations, and
//! compaction removes a delta only after no live pack still references it.

mod fetch;
mod sidecars;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, PoisonError, RwLock};
use std::time::Instant;

#[derive(Clone)]
struct CatalogPinMark {
    store: Arc<dyn crate::metadata::PinStore>,
    inventory: Arc<crate::metadata::PinInventory>,
    owned_claims: Arc<BTreeSet<crate::metadata::PinToken>>,
}

// A mark-local cache of fully processed references. Keep payload table kinds
// separate, and include all reference metadata so a conflicting reference is
// still loaded and validated even when its immutable bytes were seen earlier.
#[derive(Default)]
struct PayloadShardMarks {
    packs: HashSet<ShardRef>,
    manifests: HashSet<ShardRef>,
}

struct PayloadPinMark {
    ledger: CatalogPinMark,
    retained: HashSet<Path>,
}

/// Retired payload deleted under a payload mark: no path is one the mark
/// retained for a pin or for a payload a pinned catalog still names.
fn validate_retired_deletions(paths: &[Path], retained: &HashSet<Path>) -> Result<(), Violation> {
    match paths.iter().find(|path| retained.contains(*path)) {
        Some(path) => Err(Violation::new(
            "pack.retire",
            format!("{path} is deleted although the payload mark retains it"),
        )),
        None => Ok(()),
    }
}

const PAYLOAD_DELETE_BATCH: usize = 1_000;

/// The current batch has neither acquired a claim nor started deletion.
/// Only ordinary post-publication cleanup may defer this outcome.
#[derive(Debug, thiserror::Error)]
#[error("catalog pins changed during reclamation")]
struct CatalogPinsChanged;

#[derive(Clone, Copy)]
enum PayloadRetirement<'a> {
    Standalone,
    Deferred,
    Emergency(&'a PayloadPinMark),
}

use bytes::Bytes;
use futures::{
    FutureExt, StreamExt, TryStreamExt,
    future::BoxFuture,
    stream::{BoxStream, FuturesUnordered},
};
use object_store::{
    Attribute, Attributes, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions,
    UpdateVersion, WriteMultipart, path::Path,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

use super::ChunkMeta;
use super::chunked::{
    check_content_address, delete_object, digest_from_location, kind_prefix, put_object,
    sharded_path, validate_content_address,
};
use super::local_durability::{LocalCatalogLock, LocalDurability, PreparedLocalPut};
use super::pinned_store::{validate_deletion_claim, validate_deletion_finish};
use crate::digest::{BlobId, ChunkId, DIGEST_LEN, Digest, PackId};
use crate::invariant::{self, Violation};

mod delta;
mod read;
pub(super) use read::CatalogSnapshot;
use read::{PackReader, SharedReadCaches};
#[cfg(test)]
#[path = "pack/gc_benchmark.rs"]
mod gc_benchmark;
#[path = "pack/run.rs"]
mod run;
#[path = "pack/run_stream.rs"]
mod run_stream;
#[path = "pack/shard.rs"]
mod shard;

use delta::{
    CatalogBase, CatalogRunRef, DecodedIndexDelta, DeltaCatalog, INDEX_CATALOG_MAGIC_V1,
    IndexMutations, apply_decoded_index_delta, decode_delta_catalog, decode_index_delta,
    delta_catalog_needs_compaction, encode_delta_catalog, encode_index_mutations,
};
use run::{
    CatalogRun, CatalogRunQueryIndex, catalog_run_query_ref, decode_catalog_run,
    decode_catalog_run_routing, encode_catalog_run, lookup_catalog_run_chunk_block,
    merge_catalog_runs,
};
use run_stream::{
    CatalogRunReaders, PreparedCatalogRunFile, StagedCatalogRun, merge_staged_runs, stage_bytes,
    stage_file,
};
use shard::{
    DEFAULT_SHARD_TARGET_BYTES, ShardMap, ShardRef, decode_chunk_shard, decode_pack_shard,
    decode_shard_map, digest_prefix, encode_chunk_shard_object, encode_index_shards,
    encode_manifest_shard_object, encode_pack_shard_object, encode_shard_map, list_chunk_shard,
    list_manifest_shard, lookup_chunk_locations_shard, manifest_shard_contains,
    recommended_shard_bits,
};

/// Default compressed target for object-store packs.
///
/// The remote default trades a modest compaction and memory cost for fewer
/// objects and footer reads. Local repositories use the smaller
/// [`DEFAULT_LOCAL_PACK_TARGET_SIZE`].
pub const DEFAULT_PACK_TARGET_SIZE: u64 = 16 * 1024 * 1024;

/// Default compressed target for local-filesystem packs.
pub const DEFAULT_LOCAL_PACK_TARGET_SIZE: u64 = 4 * 1024 * 1024;

/// Default memory budget for compressed-chunk caching on object stores.
pub const DEFAULT_PACK_CACHE_CAPACITY: u64 = 64 * 1024 * 1024;
/// Garbage density at which an ordinary collection rewrites a partially live
/// pack instead of publishing only a durable tombstone.
pub const DEFAULT_PACK_COMPACTION_DEAD_PERCENT: u8 = 50;
const PACK_CACHE_LOAD_STRIPES: usize = 64;
const MAX_CONCURRENT_FOOTER_READS: usize = 32;
const MAX_CONCURRENT_PACK_COMPACTIONS: usize = 2;
const MAX_CONCURRENT_CATALOG_SYNCS: usize = 8;
const PACK_MAGIC: [u8; 8] = *b"casitac1";
const PACK_TRAILER_LEN: usize = 16;
const PACK_ENTRY_LEN: usize = DIGEST_LEN + 8 + 8 + 8;
const PACKS_KIND: &str = "packs";
const REPLACEMENTS_KIND: &str = "pack-replacements";
const REPLACEMENT_MAGIC: [u8; 8] = *b"casitrp1";
// Written before replacements stopped sharing the Casitar archive magic.
const LEGACY_REPLACEMENT_MAGIC: [u8; 8] = *b"casitar1";
const TOMBSTONES_KIND: &str = "pack-tombstones";
const TOMBSTONE_MAGIC: [u8; 8] = *b"casitat1";
const TOMBSTONE_DELTA_MAGIC: [u8; 8] = *b"casitad1";
const INDEXES_KIND: &str = "pack-indexes";
pub(super) const INDEX_CHECKPOINT_MAGIC_V1: [u8; 8] = *b"casitai1";
const INDEX_POINTER_NAME: &str = "pack-index-current";
const INDEX_RECLAIM_MARKER_NAME: &str = "pack-index-reclaim-needed";
const MAX_INDEX_PUBLISH_ATTEMPTS: usize = 8;
const INDEX_INLINE_BASE_MAX_BYTES: usize = 4 * 1024 * 1024;
const CATALOG_SHARD_CACHE_BYTES: u64 = 64 * 1024 * 1024;
// Sharded catalogs carry run routing in the map GET already required at open.
// The 500 TB sweep therefore selects 4 GiB while retaining a 1 MiB in-root
// guard for small inline/checkpoint catalogs.
const CATALOG_REBASE_RUN_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const CATALOG_REBASE_INLINE_ROUTING_BYTES: u64 = 1024 * 1024;
const CATALOG_REBASE_RUN_REFS: usize = 12;
const CATALOG_RUN_REF_BYTES: u64 = 1 + DIGEST_LEN as u64 + 8 + 8 + 8 + 1;
const CATALOG_RUN_QUERY_REF_BYTES: u64 = 8 + 8 + DIGEST_LEN as u64 + 8;
const INDEX_FANOUT_BUCKETS: usize = 1 << 16;
const INDEX_FANOUT_MIN_ENTRIES: usize = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PackEntry {
    pub(crate) digest: ChunkId,
    pub(crate) offset: u64,
    pub(crate) framed_len: u64,
    pub(crate) uncompressed_len: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Location {
    pack: PackId,
    pack_len: u64,
    offset: u64,
    framed_len: u64,
    uncompressed_len: u64,
}

/// An immutable read plan, resolved while the caller protects its catalog.
#[derive(Clone)]
pub(super) struct FrozenChunk(Location);

/// Observable pack I/O counters for benchmarks and production telemetry.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PackReadStats {
    /// The active catalog has an immutable sharded base and is loaded lazily.
    pub index_sharded_base: bool,
    /// The active catalog has one external, content-addressed checkpoint base.
    pub index_checkpoint_base: bool,
    /// Immutable delta-run objects referenced by the active catalog.
    pub index_run_objects: u64,
    pub list_requests: u64,
    pub gc_manifest_list_requests: u64,
    pub gc_loose_chunk_list_requests: u64,
    pub footer_range_requests: u64,
    pub footer_range_bytes: u64,
    pub chunk_range_requests: u64,
    pub chunk_range_bytes: u64,
    pub whole_pack_requests: u64,
    pub whole_pack_bytes: u64,
    pub cache_hits: u64,
    /// Legacy telemetry field; compressed-chunk caching never promotes packs.
    pub cache_promotions: u64,
    pub cache_evictions: u64,
    /// Read-ahead windows skipped because the shared buffer budget was full.
    pub readahead_deferrals: u64,
    /// Windows a consumer was blocked on that the budget could not charge.
    pub buffer_bypasses: u64,
    pub gc_replacement_put_requests: u64,
    pub gc_replacement_put_bytes: u64,
    pub gc_marker_put_requests: u64,
    pub gc_marker_put_bytes: u64,
    pub gc_pack_delete_requests: u64,
    pub gc_manifest_delete_requests: u64,
    pub gc_outboard_delete_requests: u64,
    pub gc_loose_chunk_delete_requests: u64,
    pub gc_tombstone_put_requests: u64,
    pub gc_tombstone_put_bytes: u64,
    pub gc_tombstone_delete_requests: u64,
    pub gc_deferred_packs: u64,
    pub index_pointer_requests: u64,
    pub index_requests: u64,
    pub index_bytes: u64,
    pub index_hash_nanos: u64,
    pub index_decode_nanos: u64,
    /// Benchmark-only primary publication snapshot accounting. Payload bytes
    /// exclude spare capacity, hash-table metadata, and allocator overhead.
    #[cfg(test)]
    pub index_snapshot_calls: u64,
    #[cfg(test)]
    pub index_snapshot_nanos: u64,
    #[cfg(test)]
    pub index_snapshot_payload_bytes_lower_bound: u64,
    #[cfg(test)]
    pub index_snapshot_accounting_nanos: u64,
    #[cfg(test)]
    pub index_build_calls: u64,
    #[cfg(test)]
    pub index_build_nanos: u64,
    pub index_hits: u64,
    pub index_fallbacks: u64,
    pub index_put_requests: u64,
    pub index_put_bytes: u64,
}

#[derive(Default)]
struct PackReadCounters {
    list_requests: AtomicU64,
    gc_manifest_list_requests: AtomicU64,
    gc_loose_chunk_list_requests: AtomicU64,
    footer_range_requests: AtomicU64,
    footer_range_bytes: AtomicU64,
    chunk_range_requests: AtomicU64,
    chunk_range_bytes: AtomicU64,
    whole_pack_requests: AtomicU64,
    whole_pack_bytes: AtomicU64,
    cache_hits: AtomicU64,
    cache_evictions: AtomicU64,
    pub(super) readahead_deferrals: AtomicU64,
    pub(super) buffer_bypasses: AtomicU64,
    gc_replacement_put_requests: AtomicU64,
    gc_replacement_put_bytes: AtomicU64,
    gc_marker_put_requests: AtomicU64,
    gc_marker_put_bytes: AtomicU64,
    gc_pack_delete_requests: AtomicU64,
    gc_manifest_delete_requests: AtomicU64,
    gc_outboard_delete_requests: AtomicU64,
    gc_loose_chunk_delete_requests: AtomicU64,
    gc_tombstone_put_requests: AtomicU64,
    gc_tombstone_put_bytes: AtomicU64,
    gc_tombstone_delete_requests: AtomicU64,
    gc_deferred_packs: AtomicU64,
    index_pointer_requests: AtomicU64,
    index_requests: AtomicU64,
    index_bytes: AtomicU64,
    index_hash_nanos: AtomicU64,
    index_decode_nanos: AtomicU64,
    #[cfg(test)]
    index_snapshot_calls: AtomicU64,
    #[cfg(test)]
    index_snapshot_nanos: AtomicU64,
    #[cfg(test)]
    index_snapshot_payload_bytes_lower_bound: AtomicU64,
    #[cfg(test)]
    index_snapshot_accounting_nanos: AtomicU64,
    #[cfg(test)]
    index_build_calls: AtomicU64,
    #[cfg(test)]
    index_build_nanos: AtomicU64,
    index_hits: AtomicU64,
    index_fallbacks: AtomicU64,
    index_put_requests: AtomicU64,
    index_put_bytes: AtomicU64,
}

impl PackReadCounters {
    fn snapshot(&self) -> PackReadStats {
        PackReadStats {
            index_sharded_base: false,
            index_checkpoint_base: false,
            index_run_objects: 0,
            list_requests: self.list_requests.load(Ordering::Relaxed),
            gc_manifest_list_requests: self.gc_manifest_list_requests.load(Ordering::Relaxed),
            gc_loose_chunk_list_requests: self.gc_loose_chunk_list_requests.load(Ordering::Relaxed),
            footer_range_requests: self.footer_range_requests.load(Ordering::Relaxed),
            footer_range_bytes: self.footer_range_bytes.load(Ordering::Relaxed),
            chunk_range_requests: self.chunk_range_requests.load(Ordering::Relaxed),
            chunk_range_bytes: self.chunk_range_bytes.load(Ordering::Relaxed),
            whole_pack_requests: self.whole_pack_requests.load(Ordering::Relaxed),
            whole_pack_bytes: self.whole_pack_bytes.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_promotions: 0,
            cache_evictions: self.cache_evictions.load(Ordering::Relaxed),
            readahead_deferrals: self.readahead_deferrals.load(Ordering::Relaxed),
            buffer_bypasses: self.buffer_bypasses.load(Ordering::Relaxed),
            gc_replacement_put_requests: self.gc_replacement_put_requests.load(Ordering::Relaxed),
            gc_replacement_put_bytes: self.gc_replacement_put_bytes.load(Ordering::Relaxed),
            gc_marker_put_requests: self.gc_marker_put_requests.load(Ordering::Relaxed),
            gc_marker_put_bytes: self.gc_marker_put_bytes.load(Ordering::Relaxed),
            gc_pack_delete_requests: self.gc_pack_delete_requests.load(Ordering::Relaxed),
            gc_manifest_delete_requests: self.gc_manifest_delete_requests.load(Ordering::Relaxed),
            gc_outboard_delete_requests: self.gc_outboard_delete_requests.load(Ordering::Relaxed),
            gc_loose_chunk_delete_requests: self
                .gc_loose_chunk_delete_requests
                .load(Ordering::Relaxed),
            gc_tombstone_put_requests: self.gc_tombstone_put_requests.load(Ordering::Relaxed),
            gc_tombstone_put_bytes: self.gc_tombstone_put_bytes.load(Ordering::Relaxed),
            gc_tombstone_delete_requests: self.gc_tombstone_delete_requests.load(Ordering::Relaxed),
            gc_deferred_packs: self.gc_deferred_packs.load(Ordering::Relaxed),
            index_pointer_requests: self.index_pointer_requests.load(Ordering::Relaxed),
            index_requests: self.index_requests.load(Ordering::Relaxed),
            index_bytes: self.index_bytes.load(Ordering::Relaxed),
            index_hash_nanos: self.index_hash_nanos.load(Ordering::Relaxed),
            index_decode_nanos: self.index_decode_nanos.load(Ordering::Relaxed),
            #[cfg(test)]
            index_snapshot_calls: self.index_snapshot_calls.load(Ordering::Relaxed),
            #[cfg(test)]
            index_snapshot_nanos: self.index_snapshot_nanos.load(Ordering::Relaxed),
            #[cfg(test)]
            index_snapshot_payload_bytes_lower_bound: self
                .index_snapshot_payload_bytes_lower_bound
                .load(Ordering::Relaxed),
            #[cfg(test)]
            index_snapshot_accounting_nanos: self
                .index_snapshot_accounting_nanos
                .load(Ordering::Relaxed),
            #[cfg(test)]
            index_build_calls: self.index_build_calls.load(Ordering::Relaxed),
            #[cfg(test)]
            index_build_nanos: self.index_build_nanos.load(Ordering::Relaxed),
            index_hits: self.index_hits.load(Ordering::Relaxed),
            index_fallbacks: self.index_fallbacks.load(Ordering::Relaxed),
            index_put_requests: self.index_put_requests.load(Ordering::Relaxed),
            index_put_bytes: self.index_put_bytes.load(Ordering::Relaxed),
        }
    }

    fn reset(&self) {
        self.list_requests.store(0, Ordering::Relaxed);
        self.gc_manifest_list_requests.store(0, Ordering::Relaxed);
        self.gc_loose_chunk_list_requests
            .store(0, Ordering::Relaxed);
        self.footer_range_requests.store(0, Ordering::Relaxed);
        self.footer_range_bytes.store(0, Ordering::Relaxed);
        self.chunk_range_requests.store(0, Ordering::Relaxed);
        self.chunk_range_bytes.store(0, Ordering::Relaxed);
        self.whole_pack_requests.store(0, Ordering::Relaxed);
        self.whole_pack_bytes.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_evictions.store(0, Ordering::Relaxed);
        self.gc_replacement_put_requests.store(0, Ordering::Relaxed);
        self.gc_replacement_put_bytes.store(0, Ordering::Relaxed);
        self.gc_marker_put_requests.store(0, Ordering::Relaxed);
        self.gc_marker_put_bytes.store(0, Ordering::Relaxed);
        self.gc_pack_delete_requests.store(0, Ordering::Relaxed);
        self.gc_manifest_delete_requests.store(0, Ordering::Relaxed);
        self.gc_outboard_delete_requests.store(0, Ordering::Relaxed);
        self.gc_loose_chunk_delete_requests
            .store(0, Ordering::Relaxed);
        self.gc_tombstone_put_requests.store(0, Ordering::Relaxed);
        self.gc_tombstone_put_bytes.store(0, Ordering::Relaxed);
        self.gc_tombstone_delete_requests
            .store(0, Ordering::Relaxed);
        self.gc_deferred_packs.store(0, Ordering::Relaxed);
        self.index_pointer_requests.store(0, Ordering::Relaxed);
        self.index_requests.store(0, Ordering::Relaxed);
        self.index_bytes.store(0, Ordering::Relaxed);
        self.index_hash_nanos.store(0, Ordering::Relaxed);
        self.index_decode_nanos.store(0, Ordering::Relaxed);
        #[cfg(test)]
        {
            self.index_snapshot_calls.store(0, Ordering::Relaxed);
            self.index_snapshot_nanos.store(0, Ordering::Relaxed);
            self.index_snapshot_payload_bytes_lower_bound
                .store(0, Ordering::Relaxed);
            self.index_snapshot_accounting_nanos
                .store(0, Ordering::Relaxed);
            self.index_build_calls.store(0, Ordering::Relaxed);
            self.index_build_nanos.store(0, Ordering::Relaxed);
        }
        self.index_hits.store(0, Ordering::Relaxed);
        self.index_fallbacks.store(0, Ordering::Relaxed);
        self.index_put_requests.store(0, Ordering::Relaxed);
        self.index_put_bytes.store(0, Ordering::Relaxed);
    }
}

struct CachedCatalogShard {
    bytes: Bytes,
    routing: Option<Arc<Vec<shard::ChunkBlock>>>,
    last_used: u64,
}

impl CachedCatalogShard {
    fn weight(&self) -> u64 {
        self.bytes.len() as u64
            + self.routing.as_ref().map_or(0, |blocks| {
                (blocks.capacity() * std::mem::size_of::<shard::ChunkBlock>()) as u64
            })
    }
}

struct CatalogShardCache {
    capacity: u64,
    used: u64,
    clock: u64,
    shards: HashMap<Digest, CachedCatalogShard>,
}

impl CatalogShardCache {
    fn new(capacity: u64) -> Self {
        Self {
            capacity,
            used: 0,
            clock: 0,
            shards: HashMap::new(),
        }
    }

    fn get(&mut self, digest: Digest) -> Option<Bytes> {
        let shard = self.shards.get_mut(&digest)?;
        self.clock = self.clock.wrapping_add(1);
        shard.last_used = self.clock;
        Some(shard.bytes.clone())
    }

    fn insert(&mut self, digest: Digest, bytes: Bytes) {
        self.insert_with_routing(digest, bytes, None);
    }

    fn get_routing(&mut self, digest: Digest) -> Option<Arc<Vec<shard::ChunkBlock>>> {
        let shard = self.shards.get_mut(&digest)?;
        let routing = shard.routing.clone()?;
        self.clock = self.clock.wrapping_add(1);
        shard.last_used = self.clock;
        Some(routing)
    }

    fn insert_with_routing(
        &mut self,
        digest: Digest,
        bytes: Bytes,
        routing: Option<Arc<Vec<shard::ChunkBlock>>>,
    ) {
        let entry = CachedCatalogShard {
            bytes,
            routing,
            last_used: self.clock.wrapping_add(1),
        };
        let size = entry.weight();
        if self.capacity == 0 || size > self.capacity {
            return;
        }
        if entry.routing.is_some()
            && let Some(old) = self.shards.remove(&digest)
        {
            self.used = self.used.saturating_sub(old.weight());
        }
        if self.shards.contains_key(&digest) {
            return;
        }
        while self.used.saturating_add(size) > self.capacity {
            let Some(oldest) = self
                .shards
                .iter()
                .min_by_key(|(_, shard)| shard.last_used)
                .map(|(digest, _)| *digest)
            else {
                break;
            };
            if let Some(removed) = self.shards.remove(&oldest) {
                self.used = self.used.saturating_sub(removed.weight());
            }
        }
        self.clock = self.clock.wrapping_add(1);
        self.used = self.used.saturating_add(size);
        self.shards.insert(digest, entry);
    }

    #[cfg(test)]
    fn clear(&mut self) {
        self.used = 0;
        self.shards.clear();
    }
}

/// Test pause between taking the staging batch and installing it in flight.
#[cfg(test)]
struct FlushHandoffHook {
    reached: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
}

#[derive(Clone, Default)]
struct Batch {
    chunks: Vec<(ChunkMeta, Bytes)>,
    digests: HashSet<ChunkId>,
    bytes: u64,
}

impl Batch {
    fn push(&mut self, meta: ChunkMeta, compressed: Bytes) {
        self.bytes = self.bytes.saturating_add(compressed.len() as u64);
        self.digests.insert(meta.digest);
        self.chunks.push((meta, compressed));
    }

    fn get(&self, digest: &ChunkId) -> Option<(u64, Bytes)> {
        self.chunks
            .iter()
            .find(|(meta, _)| &meta.digest == digest)
            .map(|(meta, bytes)| (meta.size, bytes.clone()))
    }

    fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }
}

struct SealedPack {
    id: PackId,
    bytes: Bytes,
    entries: Vec<PackEntry>,
}

fn seal(batch: &Batch) -> io::Result<SealedPack> {
    if batch.is_empty() {
        return Err(io::Error::other("refusing to seal an empty chunk pack"));
    }
    let mut body = Vec::with_capacity(batch.bytes as usize);
    let mut entries = Vec::with_capacity(batch.chunks.len());
    for (meta, compressed) in &batch.chunks {
        let offset = body.len() as u64;
        body.extend_from_slice(compressed);
        entries.push(PackEntry {
            digest: meta.digest,
            offset,
            framed_len: compressed.len() as u64,
            uncompressed_len: meta.size,
        });
    }
    let footer = encode_footer(&entries);
    body.extend_from_slice(&footer);
    body.extend_from_slice(&(footer.len() as u64).to_le_bytes());
    body.extend_from_slice(&PACK_MAGIC);
    let id = PackId::new(blake3::hash(&body).into());
    Ok(SealedPack {
        id,
        bytes: body.into(),
        entries,
    })
}

fn encode_footer(entries: &[PackEntry]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + entries.len() * PACK_ENTRY_LEN);
    out.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for entry in entries {
        out.extend_from_slice(entry.digest.as_digest().as_bytes());
        out.extend_from_slice(&entry.offset.to_le_bytes());
        out.extend_from_slice(&entry.framed_len.to_le_bytes());
        out.extend_from_slice(&entry.uncompressed_len.to_le_bytes());
    }
    out
}

fn take_u64(bytes: &[u8], at: &mut usize) -> io::Result<u64> {
    let end = at
        .checked_add(8)
        .ok_or_else(|| io::Error::other("pack footer offset overflow"))?;
    let value = bytes
        .get(*at..end)
        .ok_or_else(|| io::Error::other("truncated pack footer"))?;
    *at = end;
    Ok(u64::from_le_bytes(value.try_into().expect("eight bytes")))
}

fn decode_footer(bytes: &[u8], body_len: u64) -> io::Result<Vec<PackEntry>> {
    let mut at = 0;
    let count = take_u64(bytes, &mut at)?;
    let count =
        usize::try_from(count).map_err(|_| io::Error::other("pack entry count overflow"))?;
    let expected = 8usize
        .checked_add(
            count
                .checked_mul(PACK_ENTRY_LEN)
                .ok_or_else(|| io::Error::other("pack footer size overflow"))?,
        )
        .ok_or_else(|| io::Error::other("pack footer size overflow"))?;
    if bytes.len() != expected {
        return Err(io::Error::other("pack footer has the wrong length"));
    }
    let mut entries = Vec::with_capacity(count);
    let mut seen = HashSet::with_capacity(count);
    for _ in 0..count {
        let end = at + DIGEST_LEN;
        let digest = Digest::try_from(&bytes[at..end]).map_err(io::Error::other)?;
        at = end;
        let offset = take_u64(bytes, &mut at)?;
        let framed_len = take_u64(bytes, &mut at)?;
        let uncompressed_len = take_u64(bytes, &mut at)?;
        let range_end = offset
            .checked_add(framed_len)
            .ok_or_else(|| io::Error::other("pack entry range overflow"))?;
        if framed_len == 0 || range_end > body_len {
            return Err(io::Error::other("pack entry lies outside its body"));
        }
        let digest = ChunkId::new(digest);
        if !seen.insert(digest) {
            return Err(io::Error::other("duplicate chunk in pack footer"));
        }
        entries.push(PackEntry {
            digest,
            offset,
            framed_len,
            uncompressed_len,
        });
    }
    Ok(entries)
}

fn decode_trailer(bytes: &[u8]) -> io::Result<u64> {
    if bytes.len() != PACK_TRAILER_LEN || bytes[8..] != PACK_MAGIC {
        return Err(io::Error::other("invalid chunk pack trailer"));
    }
    Ok(u64::from_le_bytes(
        bytes[..8].try_into().expect("eight bytes"),
    ))
}

fn pack_path(base: &Path, id: &PackId) -> Path {
    sharded_path(base, PACKS_KIND, id.as_digest())
}

/// Copy-on-write collection shared between the live index and publication
/// snapshots. Cloning shares the allocation; the first mutation while a
/// snapshot still holds it copies that one collection, so snapshots cost O(1)
/// instead of O(entries) per commit.
#[derive(Default)]
struct Shared<T>(Arc<T>);

impl<T> Shared<T> {
    fn new(value: T) -> Self {
        Self(Arc::new(value))
    }
}

impl<T> Clone for Shared<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> std::ops::Deref for Shared<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T: Clone> std::ops::DerefMut for Shared<T> {
    fn deref_mut(&mut self) -> &mut T {
        Arc::make_mut(&mut self.0)
    }
}

impl<T> From<T> for Shared<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<'a, T> IntoIterator for &'a Shared<T>
where
    &'a T: IntoIterator,
{
    type Item = <&'a T as IntoIterator>::Item;
    type IntoIter = <&'a T as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        (*self.0).into_iter()
    }
}

impl<T: Clone + IntoIterator> IntoIterator for Shared<T> {
    type Item = T::Item;
    type IntoIter = T::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        Arc::unwrap_or_clone(self.0).into_iter()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct IndexedLocation {
    digest: ChunkId,
    location: Location,
}

/// Flat immutable lookup plus a small mutable overlay for newly sealed packs.
///
/// Catalog and inventory rebuilds sort all locations into one allocation.
/// Ordinary writes stay O(1) by entering the overlay until the next rebuild or
/// merge. Concurrent writers can legitimately publish the same chunk in
/// different packs, so duplicate overlay locations remain available without
/// imposing one heap allocation on every catalog entry.
#[derive(Clone, Default)]
struct ChunkIndex {
    base: Shared<Vec<IndexedLocation>>,
    fanout: Shared<Box<[u32]>>,
    overlay: Shared<HashMap<ChunkId, Location>>,
    overlay_duplicates: Shared<HashMap<ChunkId, Vec<Location>>>,
}

impl ChunkIndex {
    fn insert(&mut self, digest: ChunkId, location: Location) {
        if let std::collections::hash_map::Entry::Vacant(entry) = self.overlay.entry(digest) {
            entry.insert(location);
        } else {
            self.overlay_duplicates
                .entry(digest)
                .or_default()
                .push(location);
        }
    }

    #[cfg(test)]
    fn contains_key(&self, digest: &ChunkId) -> bool {
        self.overlay.contains_key(digest) || !self.base_range(digest).is_empty()
    }

    fn get(&self, digest: &ChunkId) -> Option<Location> {
        self.overlay.get(digest).copied().or_else(|| {
            let range = self.base_range(digest);
            (!range.is_empty()).then(|| self.base[range.start].location)
        })
    }

    fn ids(&self) -> Vec<ChunkId> {
        let mut ids = self
            .base
            .iter()
            .map(|entry| entry.digest)
            .collect::<Vec<_>>();
        ids.dedup();
        ids.extend(
            self.overlay
                .keys()
                .filter(|digest| self.base_range(digest).is_empty())
                .copied(),
        );
        ids
    }

    fn len(&self) -> usize {
        self.ids().len()
    }

    #[cfg(test)]
    fn remove(&mut self, digest: &ChunkId) -> Vec<Location> {
        self.remove_many(&[*digest])
            .into_iter()
            .map(|(_, location)| location)
            .collect()
    }

    fn remove_many(&mut self, digests: &[ChunkId]) -> Vec<(ChunkId, Location)> {
        let mut removed = Vec::new();
        let mut removed_per_bucket =
            (!self.fanout.is_empty()).then(|| vec![0_u32; INDEX_FANOUT_BUCKETS]);
        // The immutable base is already sorted by digest. Sorting the requested
        // digests once turns the bulk removal into a linear merge instead of doing
        // one hash-table probe for every catalog entry.
        let mut sorted_digests = digests.to_vec();
        sorted_digests.sort_unstable();
        sorted_digests.dedup();
        let mut requested_at = 0;
        self.base.retain(|entry| {
            while sorted_digests
                .get(requested_at)
                .is_some_and(|digest| *digest < entry.digest)
            {
                requested_at += 1;
            }
            let remove = sorted_digests
                .get(requested_at)
                .is_some_and(|digest| *digest == entry.digest);
            if remove {
                if let Some(counts) = &mut removed_per_bucket {
                    counts[Self::fanout_bucket(&entry.digest)] += 1;
                }
                removed.push((entry.digest, entry.location));
            }
            !remove
        });
        self.apply_fanout_removals(removed_per_bucket);
        for digest in &sorted_digests {
            if let Some(location) = self.overlay.remove(digest) {
                removed.push((*digest, location));
            }
            if let Some(locations) = self.overlay_duplicates.remove(digest) {
                removed.extend(locations.into_iter().map(|location| (*digest, location)));
            }
        }
        removed
    }

    fn remove_pack(&mut self, pack: PackId) {
        self.retain_packs(|candidate| candidate != pack);
    }

    fn remove_packs(&mut self, packs: &HashSet<PackId>) {
        if packs.is_empty() {
            return;
        }
        if packs.len() == 1 {
            self.remove_pack(*packs.iter().next().expect("one affected pack"));
            return;
        }
        self.retain_packs(|candidate| !packs.contains(&candidate));
    }

    fn retain_packs(&mut self, keep_pack: impl Fn(PackId) -> bool) {
        let mut removed_per_bucket =
            (!self.fanout.is_empty()).then(|| vec![0_u32; INDEX_FANOUT_BUCKETS]);
        self.base.retain(|entry| {
            let keep = keep_pack(entry.location.pack);
            if !keep && let Some(counts) = &mut removed_per_bucket {
                counts[Self::fanout_bucket(&entry.digest)] += 1;
            }
            keep
        });
        self.apply_fanout_removals(removed_per_bucket);
        let mut retained = self
            .overlay
            .drain()
            .filter(|(_, location)| keep_pack(location.pack))
            .collect::<Vec<_>>();
        for (digest, locations) in self.overlay_duplicates.drain() {
            retained.extend(
                locations
                    .into_iter()
                    .filter(|location| keep_pack(location.pack))
                    .map(|location| (digest, location)),
            );
        }
        for (digest, location) in retained {
            self.insert(digest, location);
        }
    }

    fn replace_base(&mut self, mut entries: Vec<IndexedLocation>) {
        entries.sort_unstable_by(|left, right| {
            left.digest
                .cmp(&right.digest)
                .then_with(|| left.location.pack.cmp(&right.location.pack))
        });
        self.base = entries.into();
        self.rebuild_fanout();
        self.overlay.clear();
        self.overlay_duplicates.clear();
    }

    fn base_range(&self, digest: &ChunkId) -> Range<usize> {
        let (bucket_start, bucket_end) = if self.fanout.is_empty() {
            (0, self.base.len())
        } else {
            let bucket = Self::fanout_bucket(digest);
            (
                self.fanout[bucket] as usize,
                self.fanout[bucket + 1] as usize,
            )
        };
        let bucket = &self.base[bucket_start..bucket_end];
        let relative_start = bucket.partition_point(|entry| entry.digest < *digest);
        let start = bucket_start + relative_start;
        let end = bucket[relative_start..].partition_point(|entry| entry.digest == *digest) + start;
        start..end
    }

    fn rebuild_fanout(&mut self) {
        if self.base.len() < INDEX_FANOUT_MIN_ENTRIES {
            self.fanout = Shared::default();
            return;
        }
        let mut fanout = vec![0_u32; INDEX_FANOUT_BUCKETS + 1];
        for entry in &self.base {
            fanout[Self::fanout_bucket(&entry.digest) + 1] += 1;
        }
        for bucket in 1..fanout.len() {
            fanout[bucket] += fanout[bucket - 1];
        }
        self.fanout = fanout.into_boxed_slice().into();
    }

    fn apply_fanout_removals(&mut self, removed_per_bucket: Option<Vec<u32>>) {
        let Some(removed_per_bucket) = removed_per_bucket else {
            return;
        };
        if self.base.len() < INDEX_FANOUT_MIN_ENTRIES {
            self.fanout = Shared::default();
            return;
        }
        let mut next = vec![0_u32; INDEX_FANOUT_BUCKETS + 1];
        for bucket in 0..INDEX_FANOUT_BUCKETS {
            let before = self.fanout[bucket + 1] - self.fanout[bucket];
            next[bucket + 1] = next[bucket] + before - removed_per_bucket[bucket];
        }
        self.fanout = next.into_boxed_slice().into();
    }

    fn fanout_bucket(digest: &ChunkId) -> usize {
        let bytes = digest.as_digest().as_bytes();
        usize::from(u16::from_be_bytes([bytes[0], bytes[1]]))
    }
}

/// Exact manifest membership with compact immutable storage.
///
/// Catalog decoding and inventory rebuilds keep the complete set in one sorted
/// allocation. Newly published manifests enter a small hash overlay so writes
/// remain O(1); catalog publication folds that overlay into sorted bytes. This
/// preserves definitive negative answers without paying hash-table overhead
/// for every manifest in a large, stable catalog.
#[derive(Clone, Default)]
struct ManifestIndex {
    base: Shared<Vec<BlobId>>,
    fanout: Shared<Box<[u32]>>,
    overlay: Shared<HashSet<BlobId>>,
    removed: Shared<HashSet<BlobId>>,
}

impl ManifestIndex {
    fn from_unsorted(mut manifests: Vec<BlobId>) -> Self {
        manifests.sort_unstable();
        manifests.dedup();
        let mut index = Self {
            base: manifests.into(),
            fanout: Shared::default(),
            overlay: Shared::default(),
            removed: Shared::default(),
        };
        index.rebuild_fanout();
        index
    }

    fn from_sorted(manifests: Vec<BlobId>) -> Self {
        debug_assert!(manifests.windows(2).all(|pair| pair[0] < pair[1]));
        let mut index = Self {
            base: manifests.into(),
            fanout: Shared::default(),
            overlay: Shared::default(),
            removed: Shared::default(),
        };
        index.rebuild_fanout();
        index
    }

    fn contains(&self, digest: &BlobId) -> bool {
        !self.removed.contains(digest)
            && (self.overlay.contains(digest) || self.base_contains(digest))
    }

    fn insert(&mut self, digest: BlobId) -> bool {
        if self.removed.remove(&digest) {
            return true;
        }
        if self.base_contains(&digest) {
            return false;
        }
        self.overlay.insert(digest)
    }

    #[cfg(test)]
    fn extend(&mut self, manifests: impl IntoIterator<Item = BlobId>) {
        for manifest in manifests {
            self.insert(manifest);
        }
    }

    fn merge(&mut self, other: Self) {
        let mut manifests = Vec::with_capacity(self.len() + other.len());
        manifests.extend(self.sorted_ids());
        manifests.extend(other.sorted_ids());
        *self = Self::from_unsorted(manifests);
    }

    fn len(&self) -> usize {
        self.base.len() + self.overlay.len() - self.removed.len()
    }

    fn iter(&self) -> impl Iterator<Item = &BlobId> {
        self.base
            .iter()
            .filter(|digest| !self.removed.contains(digest))
            .chain(self.overlay.iter())
    }

    fn sorted_ids(&self) -> Vec<BlobId> {
        if self.overlay.is_empty() && self.removed.is_empty() {
            return (*self.base).clone();
        }
        let mut manifests = Vec::with_capacity(self.len());
        manifests.extend(
            self.base
                .iter()
                .filter(|digest| !self.removed.contains(digest))
                .copied(),
        );
        manifests.extend(self.overlay.iter().copied());
        manifests.sort_unstable();
        manifests
    }

    fn remove_many(&mut self, digests: &HashSet<BlobId>) {
        for digest in digests {
            self.remove(digest);
        }
    }

    fn remove(&mut self, digest: &BlobId) -> bool {
        if self.overlay.remove(digest) {
            return true;
        }
        self.base_contains(digest) && self.removed.insert(*digest)
    }

    fn base_contains(&self, digest: &BlobId) -> bool {
        let base = if self.fanout.is_empty() {
            self.base.as_slice()
        } else {
            let bucket = Self::fanout_bucket(digest);
            &self.base[self.fanout[bucket] as usize..self.fanout[bucket + 1] as usize]
        };
        base.binary_search(digest).is_ok()
    }

    fn rebuild_fanout(&mut self) {
        if self.base.len() < INDEX_FANOUT_MIN_ENTRIES {
            self.fanout = Shared::default();
            return;
        }
        let mut fanout = vec![0_u32; INDEX_FANOUT_BUCKETS + 1];
        for manifest in &self.base {
            fanout[Self::fanout_bucket(manifest) + 1] += 1;
        }
        for bucket in 1..fanout.len() {
            fanout[bucket] += fanout[bucket - 1];
        }
        self.fanout = fanout.into_boxed_slice().into();
    }

    fn fanout_bucket(digest: &BlobId) -> usize {
        let bytes = digest.as_digest().as_bytes();
        usize::from(u16::from_be_bytes([bytes[0], bytes[1]]))
    }
}

#[derive(Clone, Default)]
struct Index {
    chunks: ChunkIndex,
    packs: Shared<HashMap<PackId, Vec<PackEntry>>>,
    pack_lengths: Shared<HashMap<PackId, u64>>,
    superseded: Shared<HashSet<PackId>>,
    tombstoned: Shared<HashMap<PackId, HashSet<ChunkId>>>,
    tombstone_records: Shared<HashMap<PackId, HashSet<Digest>>>,
    manifests: ManifestIndex,
    manifests_complete: bool,
}

impl Index {
    fn add_pack(&mut self, pack: PackId, pack_len: u64, entries: Vec<PackEntry>) {
        if self.superseded.contains(&pack) || self.packs.contains_key(&pack) {
            return;
        }
        for entry in &entries {
            if self
                .tombstoned
                .get(&pack)
                .is_some_and(|dead| dead.contains(&entry.digest))
            {
                continue;
            }
            self.chunks.insert(
                entry.digest,
                Location {
                    pack,
                    pack_len,
                    offset: entry.offset,
                    framed_len: entry.framed_len,
                    uncompressed_len: entry.uncompressed_len,
                },
            );
        }
        self.pack_lengths.insert(pack, pack_len);
        self.packs.insert(pack, entries);
    }

    fn add_pack_metadata(&mut self, pack: PackId, pack_len: u64, entries: Vec<PackEntry>) -> bool {
        if self.superseded.contains(&pack) || self.packs.contains_key(&pack) {
            return false;
        }
        self.pack_lengths.insert(pack, pack_len);
        self.packs.insert(pack, entries);
        true
    }

    fn remove_pack(&mut self, pack: PackId) {
        if self.packs.remove(&pack).is_none() {
            self.superseded.insert(pack);
            return;
        }
        self.pack_lengths.remove(&pack);
        self.tombstoned.remove(&pack);
        self.tombstone_records.remove(&pack);
        self.chunks.remove_pack(pack);
        self.superseded.insert(pack);
    }

    fn unreferenced_tombstone_records(&self, candidates: HashSet<Digest>) -> Vec<Digest> {
        candidates
            .into_iter()
            .filter(|candidate| {
                !self
                    .tombstone_records
                    .values()
                    .any(|records| records.contains(candidate))
            })
            .collect()
    }

    fn merge(&mut self, other: Index) {
        self.superseded.extend(other.superseded);
        self.pack_lengths.extend(other.pack_lengths);
        self.packs.extend(other.packs);
        self.manifests.merge(other.manifests);
        self.manifests_complete |= other.manifests_complete;
        for (pack, dead) in other.tombstoned {
            self.tombstoned.entry(pack).or_default().extend(dead);
        }
        for (pack, records) in other.tombstone_records {
            self.tombstone_records
                .entry(pack)
                .or_default()
                .extend(records);
        }
        for pack in &self.superseded {
            self.packs.remove(pack);
            self.pack_lengths.remove(pack);
            self.tombstoned.remove(pack);
            self.tombstone_records.remove(pack);
        }
        self.rebuild_chunks();
    }

    fn rebuild_chunks(&mut self) {
        let capacity = self.packs.values().map(Vec::len).sum();
        let mut chunks = Vec::with_capacity(capacity);
        for (pack, entries) in &self.packs {
            let pack_len = self.pack_lengths.get(pack).copied().unwrap_or_default();
            for entry in entries {
                if self
                    .tombstoned
                    .get(pack)
                    .is_some_and(|dead| dead.contains(&entry.digest))
                {
                    continue;
                }
                chunks.push(IndexedLocation {
                    digest: entry.digest,
                    location: Location {
                        pack: *pack,
                        pack_len,
                        offset: entry.offset,
                        framed_len: entry.framed_len,
                        uncompressed_len: entry.uncompressed_len,
                    },
                });
            }
        }
        self.chunks.replace_base(chunks);
    }
}

#[derive(Clone, Default)]
struct IndexCatalogWitness {
    external: Option<external::ExternalCatalog>,
    version: Option<UpdateVersion>,
    pointer_digest: Option<Digest>,
    generation: u64,
    root: Option<DeltaCatalog>,
    runs: BTreeMap<u8, CatalogRun>,
    /// Set only while a newly built root is awaiting its atomic publication.
    prepared_rebase: Option<ShardedIndexBase>,
    /// A routing-only map replacement awaiting the same atomic publication.
    prepared_map: Option<ShardedIndexBase>,
}

/// A pack catalog pointer about to replace the witnessed `previous` one,
/// checked before the pointer CAS or before the metadata commit that carries
/// it as a state catalog:
///
/// - `pointer` decodes, to exactly the `built` root;
/// - its generation advances past the witnessed pointer's;
/// - no pack it retires stays referenced: neither a pack whose payload
///   `retired` hands to deletion nor one that `delta` removes or supersedes
///   is present in `candidate` or re-added by `delta`;
/// - given the debug-only `written` witness, every catalog object the pointer
///   names directly is named by `previous` or was durably written by this
///   handle, so no reader follows it to an object that was never stored.
fn validate_catalog_pointer(
    previous: &IndexCatalogWitness,
    built: &DeltaCatalog,
    pointer: &[u8],
    delta: Option<&[u8]>,
    candidate: &Index,
    retired: &HashSet<PackId>,
    written: Option<&HashSet<Digest>>,
) -> Result<(), Violation> {
    const POINT: &str = "pack.pointer";
    let next = decode_delta_catalog(pointer).map_err(|error| {
        Violation::new(POINT, format!("the new pointer does not decode: {error}"))
    })?;
    invariant::ensure(POINT, next == *built, || {
        "the new pointer decodes to a different root than was built".into()
    })?;
    let floor = previous.root.as_ref().map_or(previous.generation, |root| {
        root.generation.max(previous.generation)
    });
    invariant::ensure(POINT, next.generation > floor, || {
        format!(
            "generation {} does not advance past the witnessed {floor}",
            next.generation
        )
    })?;
    let delta = delta.map(decode_index_delta).transpose().map_err(|error| {
        Violation::new(
            POINT,
            format!("the published delta does not decode: {error}"),
        )
    })?;
    let dropped = delta.iter().flat_map(|delta| {
        delta
            .removed_packs
            .iter()
            .chain(delta.patch.superseded.iter())
    });
    for pack in retired.iter().chain(dropped) {
        let referenced = candidate.packs.contains_key(pack)
            || delta
                .as_ref()
                .is_some_and(|delta| delta.patch.packs.contains_key(pack));
        invariant::ensure(POINT, !referenced, || {
            format!("retired pack {pack} is still referenced")
        })?;
    }
    if let Some(written) = written {
        let known: HashSet<Digest> = previous
            .root
            .iter()
            .flat_map(catalog_root_references)
            .collect();
        for reference in catalog_root_references(&next) {
            invariant::ensure(
                POINT,
                known.contains(&reference) || written.contains(&reference),
                || {
                    format!(
                        "catalog object {reference} is neither named by the witnessed pointer \
                         nor written by this handle"
                    )
                },
            )?;
        }
    }
    Ok(())
}

/// The catalog objects a root names directly: its checkpoint or shard map,
/// and its immutable runs. Shards are named by the map, not by the root.
fn catalog_root_references(root: &DeltaCatalog) -> impl Iterator<Item = Digest> + '_ {
    let base = match &root.base {
        CatalogBase::Inline(_) => None,
        CatalogBase::Checkpoint(digest) | CatalogBase::Sharded { root: digest, .. } => {
            Some(*digest)
        }
    };
    base.into_iter()
        .chain(root.runs.values().map(|run| run.digest))
}

/// The packs whose payload a publication's retirements hand to deletion.
fn retired_packs(base: &Path, retirements: &HashSet<Path>) -> HashSet<PackId> {
    let prefix = kind_prefix(base, PACKS_KIND);
    retirements
        .iter()
        .filter(|path| path.prefix_matches(&prefix))
        .filter_map(|path| digest_from_location(path).ok())
        .map(PackId::new)
        .collect()
}

#[derive(Clone)]
struct ShardedIndexBase {
    map: Arc<ShardMap>,
}

#[derive(Clone, Default)]
struct LazyCatalogOverlay {
    base: Option<ShardedIndexBase>,
    changed_packs: HashSet<PackId>,
    removed_manifests: HashSet<BlobId>,
    /// Immutable runs are retained by reference on open and materialized only
    /// when an operation needs their exact overlay.
    run_refs: BTreeMap<u8, CatalogRunRef>,
    root_deltas: Vec<Bytes>,
}

impl LazyCatalogOverlay {
    fn apply(&mut self, delta: &DecodedIndexDelta) {
        self.changed_packs
            .extend(delta.removed_packs.iter().copied());
        self.changed_packs.extend(delta.patch.packs.keys().copied());
        self.changed_packs
            .extend(delta.patch.superseded.iter().copied());
        self.removed_manifests
            .extend(delta.removed_manifests.iter().copied());
        for manifest in delta.patch.manifests.sorted_ids() {
            self.removed_manifests.remove(&manifest);
        }
    }

    fn record_manifest_add(&mut self, manifest: BlobId) {
        self.removed_manifests.remove(&manifest);
    }

    fn record_manifest_remove(&mut self, manifest: BlobId) {
        if self.base.is_some() {
            self.removed_manifests.insert(manifest);
        }
    }
}

struct LoadedIndexCatalog {
    index: Option<Index>,
    witness: IndexCatalogWitness,
    lazy: LazyCatalogOverlay,
}

/// One publication owns both its delta and the paths that delta retires.
#[derive(Default)]
struct CatalogChanges {
    mutations: IndexMutations,
    retirements: HashSet<Path>,
}

impl std::ops::Deref for CatalogChanges {
    type Target = IndexMutations;
    fn deref(&self) -> &Self::Target {
        &self.mutations
    }
}

impl std::ops::DerefMut for CatalogChanges {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.mutations
    }
}

impl CatalogChanges {
    fn is_empty(&self) -> bool {
        self.mutations.is_empty() && self.retirements.is_empty()
    }

    fn prepend(&mut self, older: Self) {
        self.mutations.prepend(older.mutations);
        self.retirements.extend(older.retirements);
    }
}

/// Consume the captured queue once; only the current batch is copied for I/O.
/// Failed or cancelled cleanup restores both that batch and the unvisited tail.
struct RetirementCleanup<'a> {
    ready: &'a StdMutex<HashSet<Path>>,
    paths: std::collections::hash_set::IntoIter<Path>,
    pending: Vec<Path>,
}

impl RetirementCleanup<'_> {
    fn next_batch(&mut self) -> bool {
        debug_assert!(self.pending.is_empty());
        self.pending
            .extend(self.paths.by_ref().take(PAYLOAD_DELETE_BATCH));
        !self.pending.is_empty()
    }

    fn finish_batch(&mut self, retained: Vec<Path>) {
        if !retained.is_empty() {
            self.ready.lock().unwrap().extend(retained);
        }
        self.pending.clear();
    }
}

impl Drop for RetirementCleanup<'_> {
    fn drop(&mut self) {
        self.ready
            .lock()
            .unwrap()
            .extend(self.pending.drain(..).chain(self.paths.by_ref()));
    }
}

struct PreparedIndexCatalog {
    sidecars: BTreeMap<BlobId, u64>,
    changes: CatalogChanges,
    witness: IndexCatalogWitness,
    background_start: Option<CatalogRebaseSnapshot>,
    background_install: Option<PreparedCatalogRebaseInstall>,
}

/// Pending deltas belong to this guard across catalog-building awaits. Both
/// errors and cancellation restore them ahead of mutations staged meanwhile.
struct CatalogPreparation<'a> {
    packed: &'a PackedChunks,
    changes: Option<CatalogChanges>,
    background_start: Option<u64>,
}

impl Drop for CatalogPreparation<'_> {
    fn drop(&mut self) {
        let Some(mutations) = self.changes.take() else {
            return;
        };
        self.packed
            .pending_catalog
            .lock()
            .unwrap()
            .prepend(mutations);
        if let Some(id) = self.background_start {
            let mut background = self.packed.background_catalog_rebase.lock().unwrap();
            if background.job.as_ref().is_some_and(|job| job.id == id) {
                background.job = None;
            }
        }
        self.packed.index_dirty.store(true, Ordering::Release);
    }
}

struct CatalogRebaseSnapshot {
    id: u64,
    candidate: Index,
    lazy: LazyCatalogOverlay,
    /// Frozen root and hydrated map installed by the triggering commit. When
    /// present, background rebase streams this exact epoch from scratch files.
    root: Option<DeltaCatalog>,
    base: Option<ShardedIndexBase>,
}

struct PreparedCatalogRebaseInstall {
    id: u64,
    base: ShardedIndexBase,
    candidate: Index,
    mutations: IndexMutations,
}

enum CatalogBackgroundRebasePhase {
    Armed,
    Building,
    Ready(ShardedIndexBase),
    Failed(Box<CatalogRebaseSnapshot>),
}

struct CatalogBackgroundRebase {
    id: u64,
    phase: CatalogBackgroundRebasePhase,
    followups: IndexMutations,
}

#[derive(Default)]
struct CatalogBackgroundRebaseState {
    pending: Option<CatalogRebaseSnapshot>,
    next_id: u64,
    job: Option<CatalogBackgroundRebase>,
}

enum PackCompaction {
    Complete,
    Deferred(DeferredTombstone),
}

struct DeferredTombstone {
    tombstone: Tombstone,
    dead: HashSet<ChunkId>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CatalogReclaimStats {
    pub(crate) listed_objects: u64,
    pub(crate) retained_objects: u64,
    pub(crate) deleted_objects: u64,
    pub(crate) deleted_bytes: u64,
}

struct CatalogObjectPublication<'a> {
    packed: &'a PackedChunks,
    pending: FuturesUnordered<BoxFuture<'static, io::Result<PreparedLocalPut>>>,
    prepared: Vec<PreparedLocalPut>,
    /// Objects of the local batch, recorded as written once it commits.
    staged: Vec<Digest>,
}

impl CatalogObjectPublication<'_> {
    async fn put(&mut self, digest: Digest, bytes: Bytes) -> io::Result<()> {
        self.packed
            .read_counters
            .index_put_requests
            .fetch_add(1, Ordering::Relaxed);
        self.packed
            .read_counters
            .index_put_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let path = sharded_path(&self.packed.base, INDEXES_KIND, &digest);
        // A local batch becomes durable at `LocalDurability::commit`, which
        // sees only temporary files, so its objects are checked here too.
        check_content_address(&path, &bytes)?;
        let Some(local) = &self.packed.local_durability else {
            put_object(&self.packed.object_store, &path, bytes, true)
                .await
                .map_err(io::Error::other)?;
            self.packed.record_catalog_objects_written([digest]);
            return Ok(());
        };

        let local = local.clone();
        self.pending
            .push(async move { local.prepare(&path, bytes).await }.boxed());
        if crate::invariant::DEBUG {
            self.staged.push(digest);
        }
        if self.pending.len() >= MAX_CONCURRENT_CATALOG_SYNCS {
            self.collect_one().await?;
        }
        Ok(())
    }

    async fn collect_one(&mut self) -> io::Result<()> {
        if let Some(prepared) = self.pending.next().await {
            self.prepared.push(prepared?);
        }
        Ok(())
    }

    async fn finish(mut self) -> io::Result<()> {
        while !self.pending.is_empty() {
            self.collect_one().await?;
        }
        if let Some(local) = &self.packed.local_durability {
            local.commit(self.prepared).await?;
            self.packed.record_catalog_objects_written(self.staged);
        }
        Ok(())
    }
}

/// Chunk storage over immutable content-addressed packs.
pub(crate) struct PackedChunks {
    sidecars: StdMutex<sidecars::Staging>,
    pins: crate::metadata::PinBindings,
    object_store: Arc<dyn ObjectStore>,
    local_durability: Option<LocalDurability>,
    base: Path,
    target_size: u64,
    index: RwLock<Index>,
    // Lock order: `staging` before `inflight`. Every move of a chunk between
    // the two, and every lookup across both, holds `staging`, so a staged chunk
    // is always visible in one of them until its pack is in `index`.
    staging: Mutex<Batch>,
    inflight: Mutex<Option<Arc<Batch>>>,
    flush_lock: Mutex<()>,
    rebuild_lock: Mutex<()>,
    checkpoint_lock: Mutex<()>,
    index_catalog: StdMutex<IndexCatalogWitness>,
    lazy_catalog: RwLock<LazyCatalogOverlay>,
    pending_catalog: StdMutex<CatalogChanges>,
    catalog_prepared: AtomicBool,
    #[cfg(test)]
    prepared_index_catalog: StdMutex<Option<PreparedIndexCatalog>>,
    #[cfg(test)]
    flush_handoff_hook: StdMutex<Option<FlushHandoffHook>>,
    index_dirty: AtomicBool,
    state_catalog_mode: AtomicBool,
    dirty_packs: Mutex<HashSet<PackId>>,
    deleted: Mutex<HashSet<ChunkId>>,
    published_retirements: StdMutex<HashSet<Path>>,
    read_caches: Arc<SharedReadCaches>,
    scoped_catalog_cache: read::CatalogLookupCache,
    fetch: Arc<fetch::State>,
    catalog_run_load: Mutex<()>,
    catalog_run_indexes: Arc<StdMutex<HashMap<Digest, Arc<CatalogRunQueryIndex>>>>,
    catalog_rebase_run_bytes: AtomicU64,
    background_catalog_rebase: StdMutex<CatalogBackgroundRebaseState>,
    /// Catalog objects this handle durably wrote and has not reclaimed since:
    /// the in-memory witness that a new pointer names only stored objects.
    /// Filled in debug builds only, where `validate_catalog_pointer` reads it.
    catalog_objects_written: StdMutex<HashSet<Digest>>,
    read_counters: Arc<PackReadCounters>,
}

impl std::fmt::Debug for PackedChunks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PackedChunks")
            .field("base", &self.base)
            .field("target_size", &self.target_size)
            .finish_non_exhaustive()
    }
}

impl PackedChunks {
    pub(crate) fn empty_state_catalog() -> io::Result<Vec<u8>> {
        let index = Index {
            manifests_complete: true,
            ..Index::default()
        };
        let checkpoint = encode_index_checkpoint(
            &index,
            Digest::from(blake3::hash(b"casita authoritative pack index v1\0")),
        )?;
        Ok(encode_delta_catalog(&DeltaCatalog {
            sidecars: None,
            generation: 1,
            base: CatalogBase::Inline(checkpoint),
            runs: BTreeMap::new(),
            deltas: Vec::new(),
        })?
        .to_vec())
    }

    #[cfg(test)]
    pub(crate) async fn open(
        object_store: Arc<dyn ObjectStore>,
        base: Path,
        target_size: u64,
    ) -> io::Result<Arc<Self>> {
        Self::open_with_cache(object_store, base, target_size, 0).await
    }

    #[cfg(test)]
    pub(crate) async fn open_with_cache(
        object_store: Arc<dyn ObjectStore>,
        base: Path,
        target_size: u64,
        cache_capacity: u64,
    ) -> io::Result<Arc<Self>> {
        Self::open_with_cache_and_durability(object_store, base, target_size, cache_capacity, None)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn open_with_cache_and_durability(
        object_store: Arc<dyn ObjectStore>,
        base: Path,
        target_size: u64,
        cache_capacity: u64,

        local_durability: Option<LocalDurability>,
    ) -> io::Result<Arc<Self>> {
        Self::open_with_initial_catalog(
            object_store,
            base,
            target_size,
            cache_capacity,
            None,
            local_durability,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn open_with_state_catalog(
        object_store: Arc<dyn ObjectStore>,
        base: Path,
        target_size: u64,
        cache_capacity: u64,

        catalog: &[u8],
    ) -> io::Result<Arc<Self>> {
        Self::open_with_initial_catalog(
            object_store,
            base,
            target_size,
            cache_capacity,
            Some(catalog),
            None,
        )
        .await
    }

    #[tracing::instrument(
        name = "blob.pack_index.open",
        skip_all,
        fields(target_bytes = target_size, cache_bytes = cache_capacity, state_catalog = catalog.is_some())
    )]
    pub(crate) async fn open_with_initial_catalog(
        object_store: Arc<dyn ObjectStore>,
        base: Path,
        target_size: u64,
        cache_capacity: u64,

        catalog: Option<&[u8]>,
        local_durability: Option<LocalDurability>,
    ) -> io::Result<Arc<Self>> {
        let this = Arc::new(Self::new_unopened(
            object_store,
            base,
            target_size,
            cache_capacity,
            catalog.is_some(),
            local_durability,
        )?);
        match catalog {
            Some(catalog) => this.synchronize_state_catalog(Some(catalog)).await?,
            None => this.rebuild().await?,
        }
        Ok(this)
    }

    fn new_unopened(
        object_store: Arc<dyn ObjectStore>,
        base: Path,
        target_size: u64,
        cache_capacity: u64,
        state_catalog_mode: bool,
        local_durability: Option<LocalDurability>,
    ) -> io::Result<Self> {
        if target_size == 0 {
            return Err(io::Error::other("pack target size must be non-zero"));
        }
        Ok(Self {
            sidecars: Default::default(),
            pins: Default::default(),
            object_store,
            local_durability,
            base,
            target_size,
            index: RwLock::new(Index::default()),
            staging: Mutex::new(Batch::default()),
            inflight: Mutex::new(None),
            flush_lock: Mutex::new(()),
            rebuild_lock: Mutex::new(()),
            checkpoint_lock: Mutex::new(()),
            index_catalog: StdMutex::new(IndexCatalogWitness::default()),
            lazy_catalog: RwLock::new(LazyCatalogOverlay::default()),
            pending_catalog: StdMutex::new(CatalogChanges::default()),
            catalog_prepared: AtomicBool::new(false),
            #[cfg(test)]
            prepared_index_catalog: StdMutex::new(None),
            #[cfg(test)]
            flush_handoff_hook: StdMutex::new(None),
            index_dirty: AtomicBool::new(false),
            state_catalog_mode: AtomicBool::new(state_catalog_mode),
            dirty_packs: Mutex::new(HashSet::new()),
            deleted: Mutex::new(HashSet::new()),
            published_retirements: StdMutex::new(HashSet::new()),
            read_caches: Arc::new(SharedReadCaches::new()),
            scoped_catalog_cache: read::CatalogLookupCache::default(),
            fetch: Arc::new(fetch::State::new(cache_capacity)),
            catalog_run_load: Mutex::new(()),
            catalog_run_indexes: Arc::new(StdMutex::new(HashMap::new())),
            catalog_rebase_run_bytes: AtomicU64::new(CATALOG_REBASE_RUN_BYTES),
            background_catalog_rebase: StdMutex::new(CatalogBackgroundRebaseState::default()),
            catalog_objects_written: StdMutex::new(HashSet::new()),
            read_counters: Arc::default(),
        })
    }

    /// Debug builds only: the witness grows with every catalog object this
    /// handle writes, and only the pointer invariant reads it.
    fn record_catalog_objects_written(&self, digests: impl IntoIterator<Item = Digest>) {
        if crate::invariant::DEBUG {
            self.catalog_objects_written
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend(digests);
        }
    }

    /// Check a catalog pointer before it replaces `previous`; see
    /// [`validate_catalog_pointer`].
    fn check_catalog_pointer(
        &self,
        previous: &IndexCatalogWitness,
        built: &DeltaCatalog,
        pointer: &[u8],
        delta: Option<&[u8]>,
        candidate: &Index,
        retired: &HashSet<Path>,
    ) -> Result<(), Violation> {
        invariant::check(|| {
            let written = self
                .catalog_objects_written
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            validate_catalog_pointer(
                previous,
                built,
                pointer,
                delta,
                candidate,
                &retired_packs(&self.base, retired),
                crate::invariant::DEBUG.then_some(&*written),
            )
        })
    }

    pub(crate) async fn probe(&self, digest: &ChunkId) -> io::Result<bool> {
        self.probe_inner(digest, &crate::metadata::WritePins::default())
            .await
    }

    /// The pack file currently holding `digest`, once it has been flushed
    /// into one. Packs are immutable, so the file's existence keeps the chunk
    /// readable.
    pub(crate) async fn pack_path_of(&self, digest: &ChunkId) -> io::Result<Option<Path>> {
        Ok(self
            .location(digest)
            .await?
            .map(|location| pack_path(&self.base, &location.pack)))
    }

    pub(crate) fn attach_pin(&self, pin: &crate::metadata::DataPinLease) {
        self.pins.attach(pin);
    }

    pub(crate) fn write_scope(&self) -> crate::metadata::BackendWriteScope {
        self.pins.write_scope()
    }

    pub(crate) async fn probe_for_write(&self, digest: &ChunkId) -> io::Result<bool> {
        let pins = self.pins.capture();
        pins.protect(BTreeSet::from([crate::metadata::PinResource::Chunk(
            *digest,
        )]))
        .await?;
        self.probe_inner(digest, &pins).await
    }

    async fn probe_inner(
        &self,
        digest: &ChunkId,
        pins: &crate::metadata::WritePins,
    ) -> io::Result<bool> {
        // Check in the order chunks move (staging, inflight, index) so a
        // concurrent flush cannot slip a chunk past this probe.
        if self.is_staged(digest).await {
            return Ok(true);
        }
        if let Some(location) = self.location(digest).await? {
            if !pins.is_empty() {
                let path = pack_path(&self.base, &location.pack);
                let resource = crate::metadata::PinResource::StorageObject(path.to_string());
                pins.protect(BTreeSet::from([resource.clone()])).await?;
                if pins.known_present(&resource) {
                    return Ok(true);
                }
                // The cached location may predate a completed collection.
                // Protect its path first, then validate existence once per pin.
                if super::chunked::head_exists(&self.object_store, &path).await? {
                    pins.remember_present(resource);
                    return Ok(true);
                }
            } else {
                // Emergency local collection may free a wholly dead pack before
                // SQLite can commit. After a crash, its old catalog entry must
                // never cause a later import to skip writing the missing bytes.
                if self.local_durability.is_none()
                    || super::chunked::head_exists(
                        &self.object_store,
                        &pack_path(&self.base, &location.pack),
                    )
                    .await?
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Record that a whole-payload manifest is visible. The membership set is
    /// published with the pack catalog by the next flush, before callers may
    pub(crate) fn register_manifest(&self, digest: BlobId) {
        let mut index = self.index.write().unwrap();
        if index.manifests.insert(digest) {
            self.lazy_catalog
                .write()
                .unwrap()
                .record_manifest_add(digest);
            self.pending_catalog
                .lock()
                .unwrap()
                .record_manifest_add(digest);
            if let Some(job) = &mut self.background_catalog_rebase.lock().unwrap().job {
                job.followups.record_manifest_add(digest);
            }
            self.index_dirty.store(true, Ordering::Release);
        }
    }

    fn record_pack_mutation(&self, pack: PackId) {
        self.pending_catalog.lock().unwrap().record_pack(pack);
        if let Some(job) = &mut self.background_catalog_rebase.lock().unwrap().job {
            job.followups.record_pack(pack);
        }
        self.lazy_catalog
            .write()
            .unwrap()
            .changed_packs
            .insert(pack);
    }

    pub(crate) fn unregister_manifest(&self, digest: BlobId) {
        self.unregister_manifest_retiring(digest, HashSet::new());
    }

    fn unregister_manifest_retiring(&self, digest: BlobId, paths: HashSet<Path>) {
        let mut index = self.index.write().unwrap();
        let removed_overlay = index.manifests.remove(&digest);
        let has_lazy_base = self.lazy_catalog.read().unwrap().base.is_some();
        if removed_overlay || has_lazy_base || !paths.is_empty() {
            self.lazy_catalog
                .write()
                .unwrap()
                .record_manifest_remove(digest);
            {
                let mut pending = self.pending_catalog.lock().unwrap();
                pending.record_manifest_remove(digest);
                pending.retirements.extend(paths);
            }
            if let Some(job) = &mut self.background_catalog_rebase.lock().unwrap().job {
                job.followups.record_manifest_remove(digest);
            }
            self.index_dirty.store(true, Ordering::Release);
        }
    }

    /// Whether the complete catalog proves that no manifest exists for this
    /// payload. An incomplete catalog is deliberately inconclusive.
    pub(crate) fn manifest_definitely_absent(&self, digest: &BlobId) -> bool {
        let index = self.index.read().unwrap();
        let lazy = self.lazy_catalog.read().unwrap();
        self.reader()
            .manifest_definitely_absent(&index, &lazy, digest)
    }

    pub(crate) async fn refresh(&self) -> io::Result<()> {
        // The catalog embedded in the logical state snapshot is authoritative
        // in coordinated repositories. The standalone pointer is advisory and
        // may describe another revision, so callers must synchronize through
        // Repository::synchronized_snapshot instead of refreshing it here.
        if self.state_catalog_mode.load(Ordering::Acquire) {
            return Ok(());
        }
        self.rebuild().await
    }

    pub(crate) async fn metadata(&self, digest: &ChunkId) -> io::Result<Option<u64>> {
        if let Some(location) = self.location(digest).await? {
            return Ok(Some(location.uncompressed_len));
        }
        if let Some((size, _)) = self.staged(digest).await {
            return Ok(Some(size));
        }
        if self.state_catalog_mode.load(Ordering::Acquire) {
            return Ok(None);
        }
        self.rebuild().await?;
        Ok(self
            .location(digest)
            .await?
            .map(|location| location.uncompressed_len))
    }

    /// A private catalog view cannot be advanced by writers/readers sharing this
    /// backend. It is used only during admission, while the exact catalog is pinned.
    pub(super) async fn resolve_state_catalog(&self, catalog: &[u8]) -> io::Result<Bytes> {
        self.reader().resolve_state_catalog(catalog).await
    }

    fn reader(&self) -> PackReader {
        PackReader {
            object_store: self.object_store.clone(),
            base: self.base.clone(),
            fetch: self.fetch.clone(),
            read_caches: self.read_caches.clone(),
            read_counters: self.read_counters.clone(),
            catalog_run_indexes: self.catalog_run_indexes.clone(),
        }
    }

    pub(super) async fn scoped_catalog(
        &self,
        catalog: &[u8],
        pin: crate::metadata::DataPinLease,
    ) -> io::Result<CatalogSnapshot> {
        CatalogSnapshot::open(self.reader(), catalog, pin, &self.scoped_catalog_cache).await
    }

    pub(super) fn planned_reader(
        self: &Arc<Self>,
        chunks: Vec<ChunkMeta>,
        frozen: BTreeMap<ChunkId, FrozenChunk>,
        pin: Option<crate::metadata::DataPinLease>,
        decode: crate::byte_budget::ByteBudget,
        expected: BlobId,
    ) -> super::chunked_reader::ChunkedReader {
        fetch::reader(self.reader(), chunks, frozen, pin, decode, expected)
    }

    pub(super) async fn manifest_reader(
        self: &Arc<Self>,
        chunks: &[ChunkMeta],
        decode: crate::byte_budget::ByteBudget,
        expected: BlobId,
    ) -> io::Result<Option<super::chunked_reader::ChunkedReader>> {
        let Some(frozen) = self.freeze_manifest(chunks).await? else {
            return Ok(None);
        };
        Ok(Some(self.planned_reader(
            chunks.to_vec(),
            frozen,
            None,
            decode,
            expected,
        )))
    }

    pub(super) async fn manifest_stream(
        self: &Arc<Self>,
        chunks: &[ChunkMeta],
        decode: crate::byte_budget::ByteBudget,
        expected: BlobId,
    ) -> io::Result<Option<BoxStream<'static, io::Result<Bytes>>>> {
        let Some(frozen) = self.freeze_manifest(chunks).await? else {
            return Ok(None);
        };
        Ok(Some(fetch::stream(
            self.reader(),
            chunks.to_vec(),
            frozen,
            decode,
            expected,
        )))
    }

    async fn freeze_manifest(
        &self,
        chunks: &[ChunkMeta],
    ) -> io::Result<Option<BTreeMap<ChunkId, FrozenChunk>>> {
        let mut frozen = BTreeMap::new();
        for chunk in chunks {
            if let std::collections::btree_map::Entry::Vacant(entry) = frozen.entry(chunk.digest) {
                let Some(location) = self.freeze_read(&chunk.digest).await? else {
                    // An unflushed staging batch still uses ordinary chunk reads.
                    return Ok(None);
                };
                entry.insert(location);
            }
        }
        Ok(Some(frozen))
    }

    #[tracing::instrument(name = "blob.pack.freeze_read", level = "debug", skip_all)]
    pub(super) async fn freeze_read(&self, digest: &ChunkId) -> io::Result<Option<FrozenChunk>> {
        Ok(self.location(digest).await?.map(FrozenChunk))
    }

    #[tracing::instrument(name = "blob.pack.read_chunk", level = "debug", skip_all)]
    pub(crate) async fn get(&self, digest: &ChunkId) -> io::Result<Option<Bytes>> {
        if let Some((_, bytes)) = self.staged(digest).await {
            return Ok(Some(bytes));
        }
        if let Some(location) = self.location(digest).await? {
            match self.read_location(*digest, location).await {
                Ok(bytes) => return Ok(Some(bytes)),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    if self.state_catalog_mode.load(Ordering::Acquire) {
                        return Err(error);
                    }
                    // A collector can delete an old pack immediately before
                    // advancing the catalog. Recover from the immutable
                    // inventory rather than reloading the same stale pointer.
                    self.rebuild_from_inventory().await?;
                    let Some(location) = self.location(digest).await? else {
                        return Ok(None);
                    };
                    return self.read_location(*digest, location).await.map(Some);
                }
                Err(error) => return Err(error),
            }
        }
        if self.state_catalog_mode.load(Ordering::Acquire) {
            return Ok(None);
        }
        self.rebuild().await?;
        let Some(location) = self.location(digest).await? else {
            return Ok(None);
        };
        self.read_location(*digest, location).await.map(Some)
    }

    #[tracing::instrument(
        name = "blob.pack.stage_chunk",
        level = "debug",
        skip_all,
        fields(plaintext_bytes = meta.size, stored_bytes = compressed.len())
    )]
    pub(crate) async fn put(&self, meta: ChunkMeta, compressed: Bytes) -> io::Result<()> {
        if self.probe_for_write(&meta.digest).await? {
            return Ok(());
        }
        // Keep admission and sealing in one critical section. Compression can
        // still proceed concurrently, but completed chunks cannot pile into a
        // staging batch while another task uploads it and blow past the target.
        let _flush = self.flush_lock.lock().await;
        if self.probe_for_write(&meta.digest).await? {
            return Ok(());
        }
        let full = {
            let mut staging = self.staging.lock().await;
            if staging.digests.contains(&meta.digest) {
                return Ok(());
            }
            staging.push(meta, compressed);
            staging.bytes >= self.target_size
        };
        if full {
            self.flush_locked().await?;
        }
        Ok(())
    }

    #[tracing::instrument(name = "blob.pack.flush", level = "debug", skip_all)]
    pub(crate) async fn flush(&self) -> io::Result<()> {
        let _flush = self.flush_lock.lock().await;
        self.flush_locked().await?;
        if self.state_catalog_mode.load(Ordering::Acquire) {
            return Ok(());
        }
        self.publish_current_index(false).await
    }

    pub(crate) fn enable_state_catalog(&self) {
        self.state_catalog_mode.store(true, Ordering::Release);
    }

    pub(crate) fn uses_state_catalog(&self) -> bool {
        self.state_catalog_mode.load(Ordering::Acquire)
    }

    pub(crate) async fn finish_collection(&self, force_reclaim: bool) -> io::Result<()> {
        self.finish_collection_inner(force_reclaim, None).await
    }

    pub(crate) async fn finish_collection_pinned(
        &self,
        force_reclaim: bool,
        store: Arc<dyn crate::metadata::PinStore>,
        owned_claims: BTreeSet<crate::metadata::PinToken>,
    ) -> io::Result<()> {
        let mark = self.payload_pin_mark(store, owned_claims).await?;
        self.finish_collection_inner(force_reclaim, Some(&mark))
            .await
    }

    async fn payload_pin_mark(
        &self,
        store: Arc<dyn crate::metadata::PinStore>,
        owned_claims: BTreeSet<crate::metadata::PinToken>,
    ) -> io::Result<PayloadPinMark> {
        let inventory = Arc::new(store.inventory().await.map_err(io::Error::other)?);
        if inventory.collector.is_none() {
            return Err(io::Error::other(
                "deferred payload cleanup requires collector ownership",
            ));
        }
        let mut catalogs = BTreeSet::new();
        let mut retained = HashSet::new();
        for pin in inventory.pins.values() {
            if let Some(catalog) = &pin.catalog {
                catalogs.insert(catalog.as_slice());
            }
            for resource in &pin.resources {
                match resource {
                    crate::metadata::PinResource::Catalog(catalog) => {
                        catalogs.insert(catalog.as_slice());
                    }
                    crate::metadata::PinResource::StorageObject(path) => {
                        retained.insert(Path::from(path.as_str()));
                    }
                    crate::metadata::PinResource::Blob(blob) => {
                        self.mark_manifest_paths(*blob, &mut retained);
                    }
                    crate::metadata::PinResource::Chunk(chunk) => {
                        retained.insert(sharded_path(&self.base, "bao", chunk.as_digest()));
                    }
                    _ => {}
                }
            }
        }
        let mut shards = PayloadShardMarks::default();
        for catalog in catalogs {
            self.mark_catalog_payloads(catalog, &mut retained, &mut shards)
                .await?;
        }
        Ok(PayloadPinMark {
            ledger: CatalogPinMark {
                store,
                inventory,
                owned_claims: Arc::new(owned_claims),
            },
            retained,
        })
    }

    /// Emergency deletion may happen now. Otherwise return a candidate to
    /// record atomically with the catalog mutation; this never queues it alone.
    async fn retire_collected_path(
        &self,
        path: Path,
        retirement: PayloadRetirement<'_>,
    ) -> io::Result<Option<Path>> {
        match retirement {
            PayloadRetirement::Standalone
                if self.local_durability.is_some() || !self.uses_state_catalog() =>
            {
                delete_object(&self.object_store, &path).await?;
                Ok(None)
            }
            PayloadRetirement::Emergency(mark) if self.local_durability.is_some() => {
                Ok((!self.delete_retired_payload(&path, Some(mark)).await?).then_some(path))
            }
            _ => Ok(Some(path)),
        }
    }

    pub(crate) async fn retire_manifests_pinned(
        &self,
        blobs: &[BlobId],
        store: Arc<dyn crate::metadata::PinStore>,
        owned_claims: BTreeSet<crate::metadata::PinToken>,
        before_prune: bool,
    ) -> io::Result<()> {
        let mark = if before_prune {
            Some(self.payload_pin_mark(store, owned_claims).await?)
        } else {
            None
        };
        let retirement = mark
            .as_ref()
            .map_or(PayloadRetirement::Deferred, PayloadRetirement::Emergency);
        self.retire_manifests(blobs, retirement).await
    }

    pub(crate) async fn retire_manifest(&self, blob: BlobId) -> io::Result<()> {
        self.retire_manifests(&[blob], PayloadRetirement::Standalone)
            .await
    }

    async fn retire_manifests(
        &self,
        blobs: &[BlobId],
        retirement: PayloadRetirement<'_>,
    ) -> io::Result<()> {
        if self.uses_state_catalog() {
            self.remove_sidecars(blobs.iter().copied())?;
        }
        for blob in blobs {
            let mut paths = HashSet::new();
            for kind in ["blobs", "bao"] {
                paths.extend(
                    self.retire_collected_path(
                        sharded_path(&self.base, kind, blob.as_digest()),
                        retirement,
                    )
                    .await?,
                );
            }
            self.unregister_manifest_retiring(*blob, paths);
        }
        Ok(())
    }

    fn mark_manifest_paths(&self, blob: BlobId, retained: &mut HashSet<Path>) {
        for kind in ["blobs", "bao"] {
            retained.insert(sharded_path(&self.base, kind, blob.as_digest()));
        }
    }

    fn mark_index_payloads(&self, index: &Index, retained: &mut HashSet<Path>) {
        for (pack, entries) in &index.packs {
            retained.insert(pack_path(&self.base, pack));
            for entry in entries {
                retained.insert(sharded_path(&self.base, "bao", entry.digest.as_digest()));
            }
        }
        for blob in index.manifests.iter() {
            self.mark_manifest_paths(*blob, retained);
        }
    }

    async fn mark_catalog_payloads(
        &self,
        catalog: &[u8],
        retained: &mut HashSet<Path>,
        shards: &mut PayloadShardMarks,
    ) -> io::Result<()> {
        let loaded = self
            .decode_v1_index_catalog(
                catalog,
                UpdateVersion {
                    e_tag: None,
                    version: None,
                },
            )
            .await?;
        let index = loaded
            .index
            .ok_or_else(|| io::Error::other("invalid pinned payload catalog"))?;
        self.mark_sidecars(
            loaded.witness.root.as_ref().and_then(|root| root.sidecars),
            retained,
        )
        .await?;
        self.mark_index_payloads(&index, retained);
        // Retain base and run representations conservatively: a historical
        // catalog may still resolve them even after the current catalog has
        // compacted the corresponding chunks into a replacement pack.
        if let Some(base) = loaded.lazy.base {
            for reference in &base.map.packs {
                if shards.packs.contains(reference) {
                    continue;
                }
                let bytes = self.load_catalog_shard(*reference).await?;
                if bytes.len() as u64 != reference.encoded_bytes {
                    return Err(io::Error::other("catalog shard identity mismatch"));
                }
                let index = decode_pack_shard(&bytes, reference.prefix, reference.entries)?;
                self.mark_index_payloads(&index, retained);
                shards.packs.insert(*reference);
            }
            for reference in &base.map.manifests {
                if shards.manifests.contains(reference) {
                    continue;
                }
                let bytes = self.load_catalog_shard(*reference).await?;
                if bytes.len() as u64 != reference.encoded_bytes {
                    return Err(io::Error::other("catalog shard identity mismatch"));
                }
                for blob in list_manifest_shard(&bytes, reference.prefix)? {
                    self.mark_manifest_paths(blob, retained);
                }
                shards.manifests.insert(*reference);
            }
        }
        for reference in loaded.lazy.run_refs.values() {
            let run = self.load_catalog_run(reference.clone()).await?;
            self.mark_index_payloads(&decode_index_delta(&run.delta)?.patch, retained);
        }
        Ok(())
    }

    /// Delete retired payload representations, under a payload mark when
    /// collection runs online. The mark's `retained` set must exclude them.
    async fn delete_payload_batch(
        &self,
        paths: Vec<Path>,
        mark: Option<&PayloadPinMark>,
    ) -> io::Result<()> {
        if let Some(mark) = mark {
            invariant::check(|| validate_retired_deletions(&paths, &mark.retained))?;
        }
        self.delete_catalog_batch(paths, mark.map(|mark| &mark.ledger))
            .await
    }

    async fn delete_retired_payload(
        &self,
        path: &Path,
        mark: Option<&PayloadPinMark>,
    ) -> io::Result<bool> {
        if let Some(mark) = mark {
            if mark.retained.contains(path) {
                return Ok(false);
            }
            self.delete_payload_batch(vec![path.clone()], Some(mark))
                .await?;
        } else {
            delete_object(&self.object_store, path).await?;
        }
        Ok(true)
    }

    async fn finish_collection_inner(
        &self,
        force_reclaim: bool,
        mark: Option<&PayloadPinMark>,
    ) -> io::Result<()> {
        if (self.index_dirty.load(Ordering::Acquire)
            || self.catalog_prepared.load(Ordering::Acquire))
            && mark.is_none_or(|mark| mark.ledger.inventory.logical_prune.is_some())
        {
            return Err(io::Error::other(
                "cannot delete retired representations before catalog publication",
            ));
        }
        match self.reclaim_payloads(force_reclaim, mark).await {
            Err(error)
                if error
                    .get_ref()
                    .is_some_and(|error| error.is::<CatalogPinsChanged>()) =>
            {
                // Earlier batches have settled. Keep the remaining paths for
                // a fresh mark next pass, allowing this collector to finish
                // and release retired pin history in the meantime.
                tracing::debug!("payload cleanup deferred after catalog pins changed");
                Ok(())
            }
            result => result,
        }
    }

    async fn reclaim_payloads(
        &self,
        force_reclaim: bool,
        mark: Option<&PayloadPinMark>,
    ) -> io::Result<()> {
        let mut retired = RetirementCleanup {
            ready: &self.published_retirements,
            paths: std::mem::take(&mut *self.published_retirements.lock().unwrap()).into_iter(),
            pending: Vec::new(),
        };
        // Check authoritative membership, not the mutable overlay of a newer
        // writer. A content-addressed path may have become referenced again.
        let catalog = if retired.paths.len() == 0 {
            None
        } else {
            let root = self.index_catalog.lock().unwrap().root.clone();
            match root {
                Some(root) => {
                    let bytes = encode_delta_catalog(&root)?;
                    let view = PackedChunks::open_with_initial_catalog(
                        self.object_store.clone(),
                        self.base.clone(),
                        u64::MAX,
                        0,
                        Some(&bytes),
                        None,
                    )
                    .await?;
                    view.ensure_catalog_runs_loaded().await?;
                    Some(view)
                }
                None => None,
            }
        };
        let mut tombstones = None;
        // Use the same bounded, cancellation-safe deletion path as catalog
        // cleanup. One claim and durable directory sync group covers a batch;
        // repeating those steps for every retired file dominates small GCs.
        while retired.next_batch() {
            let mut deletes = Vec::new();
            let mut retained = Vec::new();
            for path in &retired.pending {
                if let Some(catalog) = &catalog
                    && catalog
                        .catalog_references_retirement(path, &mut tombstones)
                        .await?
                {
                    // A later committed catalog owns this path again. A future
                    // removal will publish its own retirement candidate.
                } else if mark.is_some_and(|mark| mark.retained.contains(path)) {
                    retained.push(path.clone());
                } else {
                    deletes.push(path.clone());
                }
            }
            if !deletes.is_empty() {
                self.delete_payload_batch(deletes, mark).await?;
            }
            retired.finish_batch(retained);
        }
        drop(retired);
        // Orphan discovery still reads the mutable index. Unlike published
        // retirement batches, it must wait for pending publication to settle.
        if self.index_dirty.load(Ordering::Acquire) || self.catalog_prepared.load(Ordering::Acquire)
        {
            return Ok(());
        }
        if force_reclaim && self.uses_state_catalog() {
            self.reclaim_retired_packs(mark).await?;
        }
        if self.uses_state_catalog() && (force_reclaim || self.local_durability.is_some()) {
            self.reclaim_unpublished_payloads(mark).await?;
            self.reclaim_sidecars(mark).await?;
        }
        Ok(())
    }

    async fn catalog_references_retirement(
        &self,
        path: &Path,
        tombstones: &mut Option<HashSet<Digest>>,
    ) -> io::Result<bool> {
        let relative = path
            .as_ref()
            .strip_prefix(self.base.as_ref())
            .unwrap_or(path.as_ref());
        let kind = relative
            .trim_start_matches('/')
            .split('/')
            .next()
            .unwrap_or("");
        let digest = digest_from_location(path)?;
        match kind {
            PACKS_KIND => self.catalog_contains_pack(PackId::new(digest)).await,
            "blobs" => self.catalog_contains_manifest(BlobId::new(digest)).await,
            "bao" => Ok(self.catalog_contains_manifest(BlobId::new(digest)).await?
                || self.location(&ChunkId::new(digest)).await?.is_some()),
            TOMBSTONES_KIND => {
                if tombstones.is_none() {
                    let mut records: HashSet<_> = self
                        .index
                        .read()
                        .unwrap()
                        .tombstone_records
                        .values()
                        .flatten()
                        .copied()
                        .collect();
                    let lazy = self.lazy_catalog.read().unwrap().clone();
                    if let Some(base) = lazy.base {
                        for reference in &base.map.packs {
                            let bytes = self.load_catalog_shard(*reference).await?;
                            let index =
                                decode_pack_shard(&bytes, reference.prefix, reference.entries)?;
                            for (pack, references) in index.tombstone_records {
                                if !lazy.changed_packs.contains(&pack) {
                                    records.extend(references);
                                }
                            }
                        }
                    }
                    *tombstones = Some(records);
                }
                Ok(tombstones.as_ref().unwrap().contains(&digest))
            }
            _ => Err(io::Error::other("unknown retired payload kind")),
        }
    }

    /// Exclusive collection also removes uploads abandoned before any catalog
    /// commit. Never import those objects into the authoritative read catalog.
    async fn reclaim_unpublished_payloads(&self, mark: Option<&PayloadPinMark>) -> io::Result<()> {
        self.ensure_catalog_runs_loaded().await?;
        let mut deletes = Vec::new();
        for kind in [PACKS_KIND, "blobs", "bao"] {
            let prefix = kind_prefix(&self.base, kind);
            self.read_counters
                .list_requests
                .fetch_add(1, Ordering::Relaxed);
            let mut objects = self.object_store.list(Some(&prefix));
            while let Some(object) = objects.try_next().await.map_err(io::Error::other)? {
                let Ok(digest) = digest_from_location(&object.location) else {
                    continue;
                };
                let retained = if kind == PACKS_KIND {
                    self.catalog_contains_pack(PackId::new(digest)).await?
                } else {
                    self.catalog_contains_manifest(BlobId::new(digest)).await?
                        || (kind == "bao" && self.location(&ChunkId::new(digest)).await?.is_some())
                };
                if !retained && mark.is_none_or(|mark| !mark.retained.contains(&object.location)) {
                    deletes.push(object.location);
                    if deletes.len() == PAYLOAD_DELETE_BATCH {
                        self.delete_payload_batch(std::mem::take(&mut deletes), mark)
                            .await?;
                    }
                }
            }
        }
        if !deletes.is_empty() {
            self.delete_payload_batch(deletes, mark).await?;
        }
        Ok(())
    }

    async fn catalog_contains_manifest(&self, digest: BlobId) -> io::Result<bool> {
        if self.index.read().unwrap().manifests.contains(&digest) {
            return Ok(true);
        }
        let lazy = self.lazy_catalog.read().unwrap().clone();
        if lazy.removed_manifests.contains(&digest) {
            return Ok(false);
        }
        let Some(base) = lazy.base else {
            return Ok(false);
        };
        let prefix = digest_prefix(digest.as_digest(), base.map.shard_bits)?;
        let Ok(at) = base
            .map
            .manifests
            .binary_search_by_key(&prefix, |shard| shard.prefix)
        else {
            return Ok(false);
        };
        let bytes = self.load_catalog_shard(base.map.manifests[at]).await?;
        manifest_shard_contains(&bytes, prefix, &digest)
    }

    /// Replay durable replacement records after a collector died between its
    /// catalog commit and deletion. Exact current catalog membership, not the
    /// existence of a replacement marker, determines whether deletion is safe.
    async fn reclaim_retired_packs(&self, mark: Option<&PayloadPinMark>) -> io::Result<()> {
        self.ensure_catalog_runs_loaded().await?;
        let mut deletes = Vec::new();
        let prefix = kind_prefix(&self.base, REPLACEMENTS_KIND);
        self.read_counters
            .list_requests
            .fetch_add(1, Ordering::Relaxed);
        let mut markers = self.object_store.list(Some(&prefix));
        while let Some(marker) = markers.try_next().await.map_err(io::Error::other)? {
            let expected = digest_from_location(&marker.location)?;
            let maximum = (8 + DIGEST_LEN * 2 + 1) as u64;
            if marker.size > maximum {
                return Err(io::Error::other("oversized pack replacement record"));
            }
            let bytes = self
                .object_store
                .get_range(&marker.location, 0..marker.size)
                .await
                .map_err(io::Error::other)?;
            if Digest::from(blake3::hash(&bytes)) != expected {
                return Err(io::Error::other("pack replacement identity mismatch"));
            }
            let (old, _) = decode_replacement(&bytes)?;
            if !self.catalog_contains_pack(old).await? {
                self.read_counters
                    .gc_pack_delete_requests
                    .fetch_add(1, Ordering::Relaxed);
                let path = pack_path(&self.base, &old);
                if mark.is_none_or(|mark| !mark.retained.contains(&path)) {
                    deletes.push(path);
                    if deletes.len() == PAYLOAD_DELETE_BATCH {
                        self.delete_payload_batch(std::mem::take(&mut deletes), mark)
                            .await?;
                    }
                }
            }
        }
        if !deletes.is_empty() {
            self.delete_payload_batch(deletes, mark).await?;
        }
        Ok(())
    }

    async fn catalog_contains_pack(&self, pack: PackId) -> io::Result<bool> {
        if self.index.read().unwrap().packs.contains_key(&pack) {
            return Ok(true);
        }
        let lazy = self.lazy_catalog.read().unwrap().clone();
        if lazy.changed_packs.contains(&pack) {
            return Ok(false);
        }
        let Some(base) = lazy.base else {
            return Ok(false);
        };
        let prefix = digest_prefix(pack.as_digest(), base.map.shard_bits)?;
        let Ok(at) = base
            .map
            .packs
            .binary_search_by_key(&prefix, |shard| shard.prefix)
        else {
            return Ok(false);
        };
        let reference = base.map.packs[at];
        let bytes = self.load_catalog_shard(reference).await?;
        Ok(decode_pack_shard(&bytes, prefix, reference.entries)?
            .packs
            .contains_key(&pack))
    }

    /// Reconstruct the exact catalog committed beside one logical state
    /// revision. Local unpublished mutations are replayed over that base.
    pub(crate) async fn synchronize_state_catalog(&self, catalog: Option<&[u8]>) -> io::Result<()> {
        let Some(catalog) = catalog else {
            return Ok(());
        };
        if let Some(reference) = external::ExternalCatalog::decode(catalog)?
            && self.index_catalog.lock().unwrap().external == Some(reference)
        {
            return Ok(());
        }
        if self
            .index_catalog
            .lock()
            .unwrap()
            .root
            .as_ref()
            .and_then(|root| encode_delta_catalog(root).ok())
            .is_some_and(|current| current.as_ref() == catalog)
        {
            return Ok(());
        }

        // A background base is derived from the previously synchronized root.
        // Once another writer advances that root, any in-flight result is only
        // an unreachable immutable candidate and must never be installed.
        self.background_catalog_rebase.lock().unwrap().job = None;

        let _rebuild = self.rebuild_lock.lock().await;
        let _checkpoint = self.checkpoint_lock.lock().await;
        let version = UpdateVersion {
            e_tag: None,
            version: None,
        };
        let mut loaded = self.decode_v1_index_catalog(catalog, version).await?;
        loaded.witness.version = None;
        let Some(mut index) = loaded.index else {
            return Err(io::Error::other(
                "state-committed pack catalog could not be reconstructed",
            ));
        };
        let mut lazy = loaded.lazy;
        self.read_counters
            .index_hits
            .fetch_add(1, Ordering::Relaxed);
        let dirty = self.index_dirty.load(Ordering::Acquire);
        {
            let mut current = self.index.write().unwrap();
            if dirty {
                let local = current.clone();
                let pending = self.pending_catalog.lock().unwrap();
                if !pending.is_empty() {
                    let delta = encode_index_mutations(&local, &pending)?;
                    let decoded = decode_index_delta(&delta)?;
                    lazy.apply(&decoded);
                    apply_decoded_index_delta(&mut index, decoded);
                }
            } else {
                self.pending_catalog.lock().unwrap().mutations = IndexMutations::default();
            }
            *current = index;
        }
        *self.lazy_catalog.write().unwrap() = lazy;
        self.catalog_run_indexes.lock().unwrap().clear();
        *self.index_catalog.lock().unwrap() = loaded.witness;
        Ok(())
    }

    /// Prepare an owned durable candidate. Its completion owns both the exact
    /// changes captured here and the staging leases active at preparation.
    pub(crate) async fn prepare_catalog(self: &Arc<Self>) -> io::Result<super::PreparedCatalog> {
        let pins = self.pins.capture();
        let prepared = match self.prepare_catalog_parts().await? {
            None => super::PreparedCatalog::unchanged(),
            Some((state, catalog)) => {
                let packed = self.clone();
                super::PreparedCatalog::new(Some(catalog), move |outcome| {
                    packed
                        .resolve_prepared_catalog(
                            state,
                            outcome == super::CatalogOutcome::Committed,
                        )
                        .map_err(Into::into)
                })
            }
        };
        Ok(prepared.with_protection(pins))
    }

    // Legacy test adapter for fixtures that explicitly exercise intermediate
    // preparation state. Production candidates own this state in their handle.
    #[cfg(test)]
    pub(crate) async fn prepare_state_catalog(self: &Arc<Self>) -> io::Result<Option<Vec<u8>>> {
        let Some((prepared, catalog)) = self.prepare_catalog_parts().await? else {
            return Ok(None);
        };
        *self.prepared_index_catalog.lock().unwrap() = Some(prepared);
        Ok(Some(catalog))
    }

    #[cfg(test)]
    pub(crate) fn finish_state_catalog(self: &Arc<Self>, committed: bool) -> io::Result<()> {
        let Some(prepared) = self.prepared_index_catalog.lock().unwrap().take() else {
            return Ok(());
        };
        self.resolve_prepared_catalog(prepared, committed)
    }

    fn resolve_prepared_catalog(
        self: &Arc<Self>,
        prepared: PreparedIndexCatalog,
        committed: bool,
    ) -> io::Result<()> {
        let result = self.finish_prepared_catalog(prepared, committed);
        self.catalog_prepared.store(false, Ordering::Release);
        result
    }

    #[tracing::instrument(name = "blob.pack_index.prepare", level = "debug", skip_all)]
    async fn prepare_catalog_parts(
        self: &Arc<Self>,
    ) -> io::Result<Option<(PreparedIndexCatalog, Vec<u8>)>> {
        let _flush = self.flush_lock.lock().await;
        self.flush_locked().await?;
        let _checkpoint = self.checkpoint_lock.lock().await;
        if self.catalog_prepared.load(Ordering::Acquire) {
            return Err(io::Error::other(
                "a pack catalog state commit is already prepared",
            ));
        }
        let retry = {
            let mut background = self.background_catalog_rebase.lock().unwrap();
            match &mut background.job {
                Some(job) => {
                    match std::mem::replace(&mut job.phase, CatalogBackgroundRebasePhase::Building)
                    {
                        CatalogBackgroundRebasePhase::Failed(snapshot) => Some(*snapshot),
                        phase => {
                            job.phase = phase;
                            None
                        }
                    }
                }
                None => None,
            }
        };
        let retry_started = retry.is_some();
        if let Some(retry) = retry {
            self.queue_catalog_rebase(retry);
        }
        let dirty = self.index_dirty.swap(false, Ordering::AcqRel);
        if !dirty {
            return Ok(None);
        }
        if retry_started
            && self.pending_catalog.lock().unwrap().is_empty()
            && !self.has_pending_sidecars()
        {
            return Ok(None);
        }
        let (
            previous,
            candidate,
            mutations,
            lazy,
            delta,
            mut background_start,
            mut background_install,
        ) = loop {
            let previous = self.index_catalog.lock().unwrap().clone();
            let captured = {
                // Preserve the normal index -> mutations -> background lock
                // order so mutations cannot fall between a rebase snapshot
                // and installing the job that tracks its follow-ups.
                let index = self.index.read().unwrap();
                let mut pending = self.pending_catalog.lock().unwrap();
                let lazy = self.lazy_catalog.read().unwrap().clone();
                let candidate = self.publication_snapshot(&index);
                let mutations = std::mem::take(&mut *pending);
                let delta = if mutations.is_empty() {
                    None
                } else {
                    match encode_index_mutations(&candidate, &mutations) {
                        Ok(delta) => Some(delta),
                        Err(error) => {
                            pending.prepend(mutations);
                            self.index_dirty.store(true, Ordering::Release);
                            return Err(error);
                        }
                    }
                };
                let mut background = self.background_catalog_rebase.lock().unwrap();
                let ready = background
                    .job
                    .as_ref()
                    .is_some_and(|job| matches!(job.phase, CatalogBackgroundRebasePhase::Ready(_)));
                let needs_materialized_runs = !lazy.run_refs.is_empty()
                    && !ready
                    && match delta.as_deref() {
                        Some(delta) => previous.root.as_ref().is_none_or(|root| {
                            background.job.is_none()
                                && self.catalog_rebase_due(root, delta)
                                && (self.local_durability.is_some()
                                    || !matches!(root.base, CatalogBase::Sharded { .. }))
                        }),
                        None => true,
                    };
                if needs_materialized_runs {
                    Err(mutations)
                } else {
                    let mut background_start = None;
                    let mut background_install = None;
                    // Local rebases stay within the mutation's filesystem
                    // lease so exclusive catalog reclamation cannot overtake
                    // a background task. Remote rebases remain asynchronous.
                    if let Some(job) = &background.job
                        && let CatalogBackgroundRebasePhase::Ready(base) = &job.phase
                    {
                        background_install = Some(PreparedCatalogRebaseInstall {
                            id: job.id,
                            base: base.clone(),
                            candidate: candidate.clone(),
                            mutations: job.followups.clone(),
                        });
                    } else if self.local_durability.is_none()
                        && background.job.is_none()
                        && delta.as_deref().is_some_and(|delta| {
                            previous
                                .root
                                .as_ref()
                                .is_some_and(|root| self.catalog_rebase_due(root, delta))
                        })
                    {
                        background.next_id = background.next_id.wrapping_add(1).max(1);
                        let id = background.next_id;
                        background.job = Some(CatalogBackgroundRebase {
                            id,
                            phase: CatalogBackgroundRebasePhase::Armed,
                            followups: IndexMutations::default(),
                        });
                        background_start = Some(CatalogRebaseSnapshot {
                            id,
                            candidate: candidate.clone(),
                            lazy: lazy.clone(),
                            root: None,
                            base: None,
                        });
                    }
                    Ok((
                        candidate,
                        mutations,
                        lazy,
                        delta,
                        background_start,
                        background_install,
                    ))
                }
            };
            match captured {
                Ok((candidate, mutations, lazy, delta, background_start, background_install)) => {
                    break (
                        previous,
                        candidate,
                        mutations,
                        lazy,
                        delta,
                        background_start,
                        background_install,
                    );
                }
                Err(mutations) => {
                    self.pending_catalog.lock().unwrap().prepend(mutations);
                }
            }

            // The captured mutations were removed before deciding that a
            // run carry or rebase needs a complete snapshot. Put them back
            // before asynchronous run loading so concurrent changes are
            // replayed together by ensure_catalog_runs_loaded().
            self.index_dirty.store(true, Ordering::Release);
            self.ensure_catalog_runs_loaded().await?;
        };
        let mut preparation = CatalogPreparation {
            packed: self,
            changes: Some(mutations),
            background_start: background_start.as_ref().map(|start| start.id),
        };
        #[cfg(test)]
        let build_started = Instant::now();
        let built = match &mut background_install {
            Some(install) => {
                match self
                    .build_background_rebase_catalog(
                        &candidate,
                        &install.mutations,
                        &previous,
                        &install.base,
                    )
                    .await
                {
                    Ok((witness, catalog, installed)) => {
                        install.base = installed;
                        Ok((witness, catalog))
                    }
                    Err(error) => Err(error),
                }
            }
            None => {
                self.build_index_catalog(
                    &candidate,
                    delta.as_deref(),
                    false,
                    self.local_durability.is_some(),
                    &previous,
                    &lazy,
                )
                .await
            }
        };
        #[cfg(test)]
        self.record_index_build(build_started);
        let (mut witness, _catalog) = match built {
            Ok(built) => built,
            Err(error) => {
                return Err(io::Error::other(error));
            }
        };
        let (sidecar_root, sidecars) = self
            .prepare_sidecars(previous.root.as_ref().and_then(|root| root.sidecars))
            .await?;
        let root = witness
            .root
            .as_mut()
            .ok_or_else(|| io::Error::other("missing prepared catalog root"))?;
        root.sidecars = sidecar_root;
        let catalog = encode_delta_catalog(root)?;
        // A state catalog may deliberately reintroduce identical pack bytes a
        // pending retirement still names (see `flush_locked`), and deletion
        // re-checks catalog membership, so only the delta's own removals and
        // supersessions count as retired for this candidate.
        let published_delta = if background_install.is_none() {
            delta.as_deref()
        } else {
            None
        };
        self.check_catalog_pointer(
            &previous,
            root,
            &catalog,
            published_delta,
            &candidate,
            &HashSet::new(),
        )?;
        witness.pointer_digest = Some(blake3::hash(&catalog).into());
        if let Some(start) = &mut background_start
            && let Some(root) = &witness.root
            && matches!(root.base, CatalogBase::Sharded { .. })
        {
            start.root = Some(root.clone());
            start.base = witness.prepared_map.clone().or_else(|| lazy.base.clone());
        }
        let catalog = if self.local_durability.is_some() {
            let descriptor = self.externalize_state_catalog(&catalog).await?;
            witness.external = external::ExternalCatalog::decode(&descriptor)?;
            Bytes::from(descriptor)
        } else {
            catalog
        };
        witness.version = None;
        let prepared = PreparedIndexCatalog {
            sidecars,
            changes: preparation
                .changes
                .take()
                .expect("prepared catalog owns its mutations"),
            witness,
            background_start,
            background_install,
        };
        self.catalog_prepared.store(true, Ordering::Release);
        Ok(Some((prepared, catalog.to_vec())))
    }

    fn finish_prepared_catalog(
        self: &Arc<Self>,
        prepared: PreparedIndexCatalog,
        committed: bool,
    ) -> io::Result<()> {
        if !committed {
            self.pending_catalog
                .lock()
                .unwrap()
                .prepend(prepared.changes);
            if let Some(start) = &prepared.background_start {
                let mut background = self.background_catalog_rebase.lock().unwrap();
                if background
                    .job
                    .as_ref()
                    .is_some_and(|job| job.id == start.id)
                {
                    background.job = None;
                }
            }
            self.index_dirty.store(true, Ordering::Release);
            return Ok(());
        }

        let mut publication = CatalogPreparation {
            packed: self,
            changes: Some(prepared.changes),
            background_start: None,
        };
        let mut witness = prepared.witness;
        if let Some(install) = prepared.background_install {
            self.install_rebased_base_with_overlay(
                install.base,
                &install.candidate,
                &install.mutations,
            )?;
            let mut background = self.background_catalog_rebase.lock().unwrap();
            if background
                .job
                .as_ref()
                .is_some_and(|job| job.id == install.id)
            {
                background.job = None;
            }
        } else if let Some(base) = witness.prepared_rebase.take() {
            self.install_rebased_base(base)?;
        } else if let Some(base) = witness.prepared_map.take()
            && let Some(current) = &mut self.lazy_catalog.write().unwrap().base
        {
            *current = base;
        }
        if let Some(root) = &witness.root {
            self.refresh_lazy_catalog_root(root)?;
        }
        *self.index_catalog.lock().unwrap() = witness;
        self.finish_sidecars(prepared.sidecars);
        self.published_retirements
            .lock()
            .unwrap()
            .extend(publication.changes.take().unwrap().retirements);
        if let Some(start) = prepared.background_start {
            let start_now = {
                let mut background = self.background_catalog_rebase.lock().unwrap();
                if let Some(job) = &mut background.job
                    && job.id == start.id
                    && matches!(job.phase, CatalogBackgroundRebasePhase::Armed)
                {
                    job.phase = CatalogBackgroundRebasePhase::Building;
                    true
                } else {
                    false
                }
            };
            if start_now {
                self.queue_catalog_rebase(start);
            }
        }
        Ok(())
    }

    /// Make the in-process lazy overlay match a root that has just become
    /// authoritative. Sharded maps own the potentially large routing bytes;
    /// the root retains only authenticated references to them.
    fn refresh_lazy_catalog_root(&self, root: &DeltaCatalog) -> io::Result<()> {
        let mut lazy = self.lazy_catalog.write().unwrap();
        let Some(base) = &lazy.base else {
            lazy.run_refs.clear();
            lazy.root_deltas.clear();
            return Ok(());
        };
        let mut references = root.runs.clone();
        for reference in references.values_mut() {
            let Some(query) = reference.query.as_mut() else {
                continue;
            };
            if query.routing.is_empty() {
                query.routing = base
                    .map
                    .run_routing
                    .get(&reference.digest)
                    .cloned()
                    .ok_or_else(|| {
                        io::Error::other("sharded catalog map is missing run routing")
                    })?;
            }
            decode_catalog_run_routing(query)?;
        }
        lazy.run_refs = references;
        lazy.root_deltas = root.deltas.clone();
        Ok(())
    }

    fn queue_catalog_rebase(&self, start: CatalogRebaseSnapshot) {
        self.background_catalog_rebase.lock().unwrap().pending = Some(start);
    }

    pub(super) fn take_catalog_maintenance(self: &Arc<Self>) -> Option<super::CatalogMaintenance> {
        let start = self
            .background_catalog_rebase
            .lock()
            .unwrap()
            .pending
            .take()?;
        let id = start.id;
        let packed = self.clone();
        let cleanup = self.clone();
        Some(super::CatalogMaintenance::new(
            async move {
                packed
                    .build_queued_catalog_rebase(start)
                    .await
                    .map_err(Into::into)
            },
            move || {
                let mut background = cleanup.background_catalog_rebase.lock().unwrap();
                if background.job.as_ref().is_some_and(|job| job.id == id) {
                    background.job = None;
                }
            },
        ))
    }

    async fn build_queued_catalog_rebase(&self, start: CatalogRebaseSnapshot) -> io::Result<()> {
        let result = match (&start.root, &start.base) {
            (Some(root), Some(base)) => self.rebase_catalog_shards_streaming(root, base).await,
            _ => {
                self.rebase_catalog_shards(&start.candidate, &start.lazy)
                    .await
            }
        };
        let mut background = self.background_catalog_rebase.lock().unwrap();
        let Some(job) = &mut background.job else {
            return Ok(());
        };
        if job.id != start.id || !matches!(job.phase, CatalogBackgroundRebasePhase::Building) {
            return Ok(());
        }
        match result {
            Ok(base) => job.phase = CatalogBackgroundRebasePhase::Ready(base),
            Err(error) => {
                job.phase = CatalogBackgroundRebasePhase::Failed(Box::new(start));
                self.index_dirty.store(true, Ordering::Release);
                return Err(io::Error::other(format!(
                    "background catalog rebase failed: {error}"
                )));
            }
        }
        self.index_dirty.store(true, Ordering::Release);
        Ok(())
    }

    fn install_rebased_base_with_overlay(
        &self,
        base: ShardedIndexBase,
        candidate: &Index,
        committed: &IndexMutations,
    ) -> io::Result<()> {
        let mut overlay = Index {
            manifests_complete: true,
            ..Index::default()
        };
        let mut lazy = LazyCatalogOverlay {
            base: Some(base),
            ..LazyCatalogOverlay::default()
        };
        if !committed.is_empty() {
            let delta = encode_index_mutations(candidate, committed)?;
            let decoded = decode_index_delta(&delta)?;
            lazy.apply(&decoded);
            apply_decoded_index_delta(&mut overlay, decoded);
        }
        let mut current = self.index.write().unwrap();
        let pending = self.pending_catalog.lock().unwrap();
        if !pending.is_empty() {
            let delta = encode_index_mutations(&current, &pending)?;
            let decoded = decode_index_delta(&delta)?;
            lazy.apply(&decoded);
            apply_decoded_index_delta(&mut overlay, decoded);
        }
        *current = overlay;
        *self.lazy_catalog.write().unwrap() = lazy;
        self.catalog_run_indexes.lock().unwrap().clear();
        Ok(())
    }

    fn install_rebased_base(&self, base: ShardedIndexBase) -> io::Result<()> {
        let mut current = self.index.write().unwrap();
        let pending = self.pending_catalog.lock().unwrap();
        let mut overlay = Index {
            manifests_complete: true,
            ..Index::default()
        };
        let mut lazy = LazyCatalogOverlay {
            base: Some(base),
            ..LazyCatalogOverlay::default()
        };
        if !pending.is_empty() {
            let delta = encode_index_mutations(&current, &pending)?;
            let decoded = decode_index_delta(&delta)?;
            lazy.apply(&decoded);
            apply_decoded_index_delta(&mut overlay, decoded);
        }
        *current = overlay;
        *self.lazy_catalog.write().unwrap() = lazy;
        self.catalog_run_indexes.lock().unwrap().clear();
        Ok(())
    }

    async fn flush_locked(&self) -> io::Result<()> {
        self.flush_sidecars().await?;
        let batch = {
            let mut staging = self.staging.lock().await;
            if staging.is_empty() {
                return Ok(());
            }
            let batch = Arc::new(std::mem::take(&mut *staging));
            #[cfg(test)]
            self.pause_at_flush_handoff().await;
            // Install before releasing `staging` so readers never observe the
            // batch in neither place.
            *self.inflight.lock().await = Some(Arc::clone(&batch));
            batch
        };
        let result = async {
            let sealed = seal(&batch)?;
            let path = pack_path(&self.base, &sealed.id);
            check_content_address(&path, &sealed.bytes)?;
            put_object(&self.object_store, &path, sealed.bytes.clone(), true)
                .await
                .map_err(io::Error::other)?;
            Ok::<_, io::Error>(sealed)
        }
        .await;
        drop(batch);
        match result {
            Ok(sealed) => {
                // Cache independent frames so reads share one budget regardless of
                // whether the bytes came from publication or a remote fetch.
                let evictions = {
                    let mut cache = self.fetch.cache.lock().unwrap();
                    sealed
                        .entries
                        .iter()
                        .map(|entry| {
                            let start = entry.offset as usize;
                            cache.insert(
                                entry.digest,
                                &sealed.bytes[start..start + entry.framed_len as usize],
                            )
                        })
                        .sum()
                };
                self.read_counters
                    .cache_evictions
                    .fetch_add(evictions, Ordering::Relaxed);
                {
                    let mut index = self.index.write().unwrap();
                    // In the authoritative state catalog a new publication can
                    // deliberately reintroduce identical, previously retired bytes.
                    if self.uses_state_catalog() {
                        index.superseded.remove(&sealed.id);
                    }
                    index.add_pack(sealed.id, sealed.bytes.len() as u64, sealed.entries);
                    self.record_pack_mutation(sealed.id);
                }
                self.index_dirty.store(true, Ordering::Release);
                // The pack is indexed above, so clearing here leaves no gap.
                *self.inflight.lock().await = None;
                Ok(())
            }
            Err(error) => {
                let mut staging = self.staging.lock().await;
                let failed = self.inflight.lock().await.take().expect("inflight batch");
                let failed = Arc::try_unwrap(failed).unwrap_or_else(|shared| Batch::clone(&shared));
                let staging = &mut *staging;
                let mut chunks: Vec<_> = failed
                    .chunks
                    .into_iter()
                    .filter(|(meta, _)| staging.digests.insert(meta.digest))
                    .collect();
                chunks.append(&mut staging.chunks);
                staging.chunks = chunks;
                staging.bytes = staging
                    .chunks
                    .iter()
                    .map(|(_, bytes)| bytes.len() as u64)
                    .sum();
                Err(error)
            }
        }
    }

    pub(crate) fn list(&self) -> BoxStream<'_, io::Result<ChunkId>> {
        Box::pin(async_stream::try_stream! {
            self.ensure_catalog_runs_loaded().await?;
            let lazy = self.lazy_catalog.read().unwrap().clone();
            let deleted = self.deleted.lock().await.clone();
            let local = {
                let index = self.index.read().unwrap();
                if lazy.base.is_none() {
                    index.chunks.ids()
                } else {
                    let mut ids = HashSet::new();
                    for pack in &lazy.changed_packs {
                        let dead = index.tombstoned.get(pack);
                        if let Some(entries) = index.packs.get(pack) {
                            ids.extend(entries.iter().filter_map(|entry| {
                                (!dead.is_some_and(|dead| dead.contains(&entry.digest)))
                                    .then_some(entry.digest)
                            }));
                        }
                    }
                    let mut ids = ids.into_iter().collect::<Vec<_>>();
                    ids.sort_unstable();
                    ids
                }
            };
            let local_set = local.iter().copied().collect::<HashSet<_>>();
            if let Some(base) = lazy.base {
                for reference in base.map.chunks.iter().copied() {
                    let bytes = self.load_catalog_shard(reference).await?;
                    for digest in list_chunk_shard(&bytes, reference.prefix, &lazy.changed_packs)? {
                        if !local_set.contains(&digest) && !deleted.contains(&digest) {
                            yield digest;
                        }
                    }
                }
            }
            for digest in local {
                if !deleted.contains(&digest) {
                    yield digest;
                }
            }
        })
    }

    pub(crate) fn list_manifests(&self) -> BoxStream<'_, io::Result<BlobId>> {
        Box::pin(async_stream::try_stream! {
            self.ensure_catalog_runs_loaded().await?;
            let local = {
                let index = self.index.read().unwrap();
                index
                    .manifests_complete
                    .then(|| index.manifests.sorted_ids())
            }.ok_or_else(|| io::Error::other(
                "packed manifest catalog is not authoritative",
            ))?;
            let lazy = self.lazy_catalog.read().unwrap().clone();
            let local_set = local.iter().copied().collect::<HashSet<_>>();
            if let Some(base) = lazy.base {
                for reference in base.map.manifests.iter().copied() {
                    let bytes = self.load_catalog_shard(reference).await?;
                    for manifest in list_manifest_shard(&bytes, reference.prefix)? {
                        if !lazy.removed_manifests.contains(&manifest)
                            && !local_set.contains(&manifest)
                        {
                            yield manifest;
                        }
                    }
                }
            }
            for manifest in local {
                yield manifest;
            }
        })
    }

    pub(crate) fn read_stats(&self) -> PackReadStats {
        let mut stats = self.read_counters.snapshot();
        let witness = self.index_catalog.lock().unwrap();
        if let Some(root) = &witness.root {
            stats.index_sharded_base = matches!(root.base, CatalogBase::Sharded { .. });
            stats.index_checkpoint_base = matches!(root.base, CatalogBase::Checkpoint(_));
            stats.index_run_objects = root.runs.len() as u64;
        }
        stats
    }

    pub(crate) fn reset_read_stats(&self) {
        self.read_counters.reset();
    }

    pub(crate) fn record_gc_manifest_delete(&self) {
        self.read_counters
            .gc_manifest_delete_requests
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_gc_outboard_delete(&self) {
        self.read_counters
            .gc_outboard_delete_requests
            .fetch_add(1, Ordering::Relaxed);
    }

    #[tracing::instrument(name = "blob.pack.delete_chunks", skip_all, fields(chunks = digests.len()))]
    pub(crate) async fn delete_many(&self, digests: &[ChunkId]) -> io::Result<()> {
        if self.uses_state_catalog() {
            self.remove_sidecars(
                digests
                    .iter()
                    .map(|digest| BlobId::new(*digest.as_digest())),
            )?;
        }
        let (mut deleted, mut dirty) = {
            let mut index = self.index.write().unwrap();
            let locations = index.chunks.remove_many(digests);
            let mut deleted = HashSet::with_capacity(digests.len());
            let mut dirty = HashSet::new();
            for (digest, location) in locations {
                deleted.insert(digest);
                dirty.insert(location.pack);
            }
            (deleted, dirty)
        };
        for digest in digests {
            let locations = self.base_locations(digest).await?;
            if !locations.is_empty() {
                deleted.insert(*digest);
                dirty.extend(locations.into_iter().map(|location| location.pack));
            }
        }
        if deleted.is_empty() {
            return Ok(());
        }
        self.deleted.lock().await.extend(deleted);
        self.dirty_packs.lock().await.extend(dirty);
        Ok(())
    }

    #[tracing::instrument(
        name = "blob.pack.compact_deleted",
        skip_all,
        fields(force_reclaim = force_reclaim)
    )]
    pub(crate) async fn finish_deletions(&self, force_reclaim: bool) -> io::Result<()> {
        self.finish_deletions_inner(force_reclaim, PayloadRetirement::Standalone)
            .await
    }

    pub(crate) async fn finish_deletions_pinned(
        &self,
        force_reclaim: bool,
        store: Arc<dyn crate::metadata::PinStore>,
        owned_claims: BTreeSet<crate::metadata::PinToken>,
        before_prune: bool,
    ) -> io::Result<()> {
        let mark = if before_prune {
            Some(self.payload_pin_mark(store, owned_claims).await?)
        } else {
            None
        };
        let retirement = mark
            .as_ref()
            .map_or(PayloadRetirement::Deferred, PayloadRetirement::Emergency);
        self.finish_deletions_inner(force_reclaim, retirement).await
    }

    async fn finish_deletions_inner(
        &self,
        force_reclaim: bool,
        retirement: PayloadRetirement<'_>,
    ) -> io::Result<()> {
        self.prune_orphan_sidecars().await?;
        let mut dirty = std::mem::take(&mut *self.dirty_packs.lock().await);
        if force_reclaim {
            dirty.extend(self.index.read().unwrap().tombstoned.keys().copied());
        }
        let has_sharded_pack_state = force_reclaim
            && self
                .lazy_catalog
                .read()
                .unwrap()
                .base
                .as_ref()
                .is_some_and(|base| !base.map.packs.is_empty());
        if dirty.is_empty() && !has_sharded_pack_state {
            if self.state_catalog_mode.load(Ordering::Acquire) {
                return Ok(());
            }
            self.publish_current_index(false).await?;
            return self.finish_collection(false).await;
        }
        let deleted = self.deleted.lock().await.clone();
        let deleted = &deleted;
        let compactions = self
            .dirty_pack_stream(dirty, force_reclaim)
            .map(|old| async move {
                match old {
                    Ok(old) => {
                        let result = self
                            .compact_pack(old, deleted, force_reclaim, retirement)
                            .await;
                        (Some(old), result)
                    }
                    Err(error) => (None, Err(error)),
                }
            });
        let mut first_error = None;
        let mut deferred = Vec::new();
        let mut saw_dirty_pack = false;
        let mut results = compactions.buffer_unordered(MAX_CONCURRENT_PACK_COMPACTIONS);
        while let Some((old, result)) = results.next().await {
            saw_dirty_pack |= old.is_some();
            match result {
                Ok(PackCompaction::Complete) => {}
                Ok(PackCompaction::Deferred(tombstone)) => deferred.push(tombstone),
                Err(error) => {
                    if let Some(old) = old {
                        self.dirty_packs.lock().await.insert(old);
                    }
                    first_error.get_or_insert(error);
                }
            }
        }

        if !saw_dirty_pack {
            if let Some(error) = first_error {
                return Err(error);
            }
            if self.state_catalog_mode.load(Ordering::Acquire) {
                return Ok(());
            }
            self.publish_current_index(false).await?;
            return self.finish_collection(false).await;
        }

        if !deferred.is_empty() {
            let tombstones = deferred
                .iter()
                .map(|deferred| deferred.tombstone.clone())
                .collect::<Vec<_>>();
            let delta = match encode_tombstone_delta(&tombstones) {
                Ok(delta) => delta,
                Err(error) => {
                    self.dirty_packs
                        .lock()
                        .await
                        .extend(deferred.iter().map(|deferred| deferred.tombstone.pack));
                    return Err(error);
                }
            };
            let delta_id = Digest::from(blake3::hash(&delta));
            let delta_path = sharded_path(&self.base, TOMBSTONES_KIND, &delta_id);
            self.read_counters
                .gc_tombstone_put_requests
                .fetch_add(1, Ordering::Relaxed);
            self.read_counters
                .gc_tombstone_put_bytes
                .fetch_add(delta.len() as u64, Ordering::Relaxed);
            if let Err(error) = put_object(&self.object_store, &delta_path, delta, true).await {
                let mut retry = self.dirty_packs.lock().await;
                retry.extend(deferred.iter().map(|deferred| deferred.tombstone.pack));
                first_error.get_or_insert_with(|| io::Error::other(error));
            } else {
                {
                    let mut index = self.index.write().unwrap();
                    let mut candidates = HashSet::new();
                    for deferred in &deferred {
                        let pack = deferred.tombstone.pack;
                        index
                            .tombstoned
                            .entry(pack)
                            .or_default()
                            .extend(deferred.dead.iter().copied());
                        candidates.extend(
                            index
                                .tombstone_records
                                .insert(pack, HashSet::from([delta_id]))
                                .unwrap_or_default(),
                        );
                    }
                    let mut mutations = self.pending_catalog.lock().unwrap();
                    for deferred in &deferred {
                        mutations.record_pack(deferred.tombstone.pack);
                        self.lazy_catalog
                            .write()
                            .unwrap()
                            .changed_packs
                            .insert(deferred.tombstone.pack);
                    }
                    candidates.remove(&delta_id);
                    for record in index.unreferenced_tombstone_records(candidates) {
                        mutations.retirements.insert(sharded_path(
                            &self.base,
                            TOMBSTONES_KIND,
                            &record,
                        ));
                        self.read_counters
                            .gc_tombstone_delete_requests
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    self.index_dirty.store(true, Ordering::Release);
                }
                self.read_counters
                    .gc_deferred_packs
                    .fetch_add(deferred.len() as u64, Ordering::Relaxed);
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        self.deleted.lock().await.clear();
        self.index_dirty.store(true, Ordering::Release);
        if self.state_catalog_mode.load(Ordering::Acquire) {
            return Ok(());
        }
        self.publish_current_index(false).await?;
        self.finish_collection(false).await
    }

    /// Stream packs that need reclamation. The immutable base is visited one
    /// pack-state shard at a time, so forced GC is bounded by one shard plus
    /// its configured cache instead of by the repository-wide pack catalog.
    fn dirty_pack_stream(
        &self,
        explicit: HashSet<PackId>,
        force_reclaim: bool,
    ) -> BoxStream<'_, io::Result<PackId>> {
        Box::pin(async_stream::try_stream! {
            self.ensure_catalog_runs_loaded().await?;
            let lazy = self.lazy_catalog.read().unwrap().clone();
            if force_reclaim && let Some(base) = lazy.base {
                for reference in base.map.packs.iter().copied() {
                    let bytes = self.load_catalog_shard(reference).await?;
                    let shard = decode_pack_shard(&bytes, reference.prefix, reference.entries)?;
                    for pack in shard.tombstoned.keys().copied() {
                        if !explicit.contains(&pack) && !lazy.changed_packs.contains(&pack) {
                            yield pack;
                        }
                    }
                }
            }
            for pack in explicit {
                yield pack;
            }
        })
    }

    async fn location(&self, digest: &ChunkId) -> io::Result<Option<Location>> {
        if self.deleted.lock().await.contains(digest) {
            return Ok(None);
        }
        if let Some(location) = self.index.read().unwrap().chunks.get(digest) {
            return Ok(Some(location));
        }
        Ok(self.base_locations(digest).await?.into_iter().next())
    }

    /// Return the immutable pack that would serve one chunk without reading
    /// its bytes. Integrity scans use this as a stable sort key so a bounded
    /// window visits each pack once instead of repeatedly evicting and
    /// reloading packs in digest order.
    pub(crate) async fn scan_order_pack(&self, digest: &ChunkId) -> io::Result<Option<PackId>> {
        Ok(self.location(digest).await?.map(|location| location.pack))
    }

    async fn base_locations(&self, digest: &ChunkId) -> io::Result<Vec<Location>> {
        let mut lazy = self.lazy_catalog.read().unwrap().clone();
        if lazy
            .run_refs
            .values()
            .any(|reference| reference.query.is_none())
        {
            self.ensure_catalog_runs_loaded().await?;
            if let Some(location) = self.index.read().unwrap().chunks.get(digest) {
                return Ok(vec![location]);
            }
            lazy = self.lazy_catalog.read().unwrap().clone();
        }
        self.reader().base_locations(&lazy, digest).await
    }

    #[cfg(test)]
    async fn load_catalog_shard_range(
        &self,
        reference: ShardRef,
        offset: u64,
        length: u64,
        digest: Digest,
    ) -> io::Result<Bytes> {
        self.reader()
            .load_catalog_shard_range(reference, offset, length, digest)
            .await
    }

    async fn load_catalog_shard(&self, reference: ShardRef) -> io::Result<Bytes> {
        self.reader().load_catalog_shard(reference).await
    }

    /// Download and authenticate one immutable shard map.
    async fn load_catalog_map(&self, digest: Digest) -> io::Result<ShardMap> {
        self.read_counters
            .index_requests
            .fetch_add(1, Ordering::Relaxed);
        let bytes = self
            .object_store
            .get(&sharded_path(&self.base, INDEXES_KIND, &digest))
            .await
            .map_err(object_store_io_error)?
            .bytes()
            .await
            .map_err(io::Error::other)?;
        self.read_counters
            .index_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        if Digest::from(blake3::hash(&bytes)) != digest {
            return Err(io::Error::other("catalog shard-map identity mismatch"));
        }
        decode_shard_map(&bytes)
    }

    async fn load_catalog_run(&self, reference: CatalogRunRef) -> io::Result<CatalogRun> {
        self.reader().load_catalog_run(reference).await
    }

    /// Download and authenticate one immutable run into bounded local scratch
    /// space. A carry performs one whole-object GET per input run, avoiding
    /// both range-request amplification and a run-sized heap allocation.
    async fn stage_catalog_run(&self, reference: CatalogRunRef) -> io::Result<StagedCatalogRun> {
        self.read_counters
            .index_requests
            .fetch_add(1, Ordering::Relaxed);
        let result = self
            .object_store
            .get(&sharded_path(&self.base, INDEXES_KIND, &reference.digest))
            .await
            .map_err(object_store_io_error)?;
        let temporary = tempfile::NamedTempFile::new()?;
        let output = temporary.as_file().try_clone()?;
        let mut output = tokio::fs::File::from_std(output);
        let mut stream = result.into_stream();
        let mut hasher = blake3::Hasher::new();
        let mut encoded_bytes = 0_u64;
        let hash_started = Instant::now();
        while let Some(bytes) = stream.try_next().await.map_err(io::Error::other)? {
            encoded_bytes = encoded_bytes
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| io::Error::other("catalog run length overflow"))?;
            hasher.update(&bytes);
            output.write_all(&bytes).await?;
        }
        output.flush().await?;
        self.read_counters
            .index_bytes
            .fetch_add(encoded_bytes, Ordering::Relaxed);
        self.read_counters.index_hash_nanos.fetch_add(
            u64::try_from(hash_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        let digest = Digest::from(hasher.finalize());
        if encoded_bytes != reference.encoded_bytes || digest != reference.digest {
            return Err(io::Error::other("catalog run identity mismatch"));
        }
        // Drop the asynchronous clone before the blocking parser takes
        // ownership of the temporary file.
        drop(output);
        tokio::task::spawn_blocking(move || stage_file(temporary, reference))
            .await
            .map_err(io::Error::other)?
    }

    /// Publish a merged run while keeping heap use independent of its size.
    /// Small runs retain the cheaper single-PUT path; large runs use 128 MiB
    /// multipart pieces with one in-flight part.
    async fn put_prepared_catalog_run(
        &self,
        prepared: PreparedCatalogRunFile,
    ) -> io::Result<CatalogRunRef> {
        const SINGLE_PUT_MAX_BYTES: u64 = 64 * 1024 * 1024;
        const MULTIPART_BYTES: usize = 128 * 1024 * 1024;
        const COPY_BUFFER_BYTES: usize = 1024 * 1024;

        let reference = prepared.reference.clone();
        let path = sharded_path(&self.base, INDEXES_KIND, &reference.digest);
        self.read_counters
            .index_put_requests
            .fetch_add(1, Ordering::Relaxed);
        self.read_counters
            .index_put_bytes
            .fetch_add(reference.encoded_bytes, Ordering::Relaxed);
        if let Some(local) = &self.local_durability {
            let mut source = prepared.file.as_file().try_clone()?;
            if crate::invariant::DEBUG {
                // Debug builds only: re-reading the staged run doubles its
                // hashing. Clones share the file offset; `source` seeks after.
                let mut staged = source.try_clone()?;
                let digest = tokio::task::spawn_blocking(move || {
                    std::io::Seek::seek(&mut staged, std::io::SeekFrom::Start(0))?;
                    let mut hasher = blake3::Hasher::new();
                    std::io::copy(&mut staged, &mut hasher)?;
                    Ok::<_, io::Error>(Digest::from(hasher.finalize()))
                })
                .await
                .map_err(io::Error::other)??;
                invariant::check(|| validate_content_address(&path, &digest))?;
            }
            std::io::Seek::seek(&mut source, std::io::SeekFrom::Start(0))?;
            local.put_file(&path, source).await?;
            self.record_catalog_objects_written([reference.digest]);
            return Ok(reference);
        }

        let mut source = tokio::fs::File::from_std(prepared.file.as_file().try_clone()?);
        source.seek(std::io::SeekFrom::Start(0)).await?;
        if reference.encoded_bytes <= SINGLE_PUT_MAX_BYTES {
            let capacity = usize::try_from(reference.encoded_bytes)
                .map_err(|_| io::Error::other("catalog run is too large for a single PUT"))?;
            let mut bytes = Vec::with_capacity(capacity);
            source.read_to_end(&mut bytes).await?;
            check_content_address(&path, &bytes)?;
            put_object(&self.object_store, &path, Bytes::from(bytes), true)
                .await
                .map_err(io::Error::other)?;
            self.record_catalog_objects_written([reference.digest]);
            return Ok(reference);
        }

        let mut attributes = Attributes::new();
        attributes.insert(
            Attribute::CacheControl,
            "public, max-age=31536000, immutable".into(),
        );
        let upload = match self
            .object_store
            .put_multipart_opts(
                &path,
                PutMultipartOptions {
                    attributes,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(upload) => upload,
            Err(
                object_store::Error::NotImplemented { .. }
                | object_store::Error::NotSupported { .. },
            ) => self
                .object_store
                .put_multipart(&path)
                .await
                .map_err(io::Error::other)?,
            Err(error) => return Err(io::Error::other(error)),
        };
        let mut upload = WriteMultipart::new_with_chunk_size(upload, MULTIPART_BYTES);
        let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
        // Debug builds only: hashing the streamed run doubles its hashing.
        let mut hasher = crate::invariant::DEBUG.then(blake3::Hasher::new);
        loop {
            let read = match source.read(&mut buffer).await {
                Ok(read) => read,
                Err(error) => {
                    let _ = upload.abort().await;
                    return Err(error);
                }
            };
            if read == 0 {
                break;
            }
            if let Err(error) = upload.wait_for_capacity(1).await {
                let _ = upload.abort().await;
                return Err(io::Error::other(error));
            }
            upload.write(&buffer[..read]);
            if let Some(hasher) = &mut hasher {
                hasher.update(&buffer[..read]);
            }
        }
        // Completing the upload is what makes the run visible.
        if let Some(hasher) = hasher {
            let digest = Digest::from(hasher.finalize());
            if let Err(violation) = invariant::check(|| validate_content_address(&path, &digest)) {
                let _ = upload.abort().await;
                return Err(violation.into());
            }
        }
        upload.finish().await.map_err(io::Error::other)?;
        self.record_catalog_objects_written([reference.digest]);
        Ok(reference)
    }

    /// Materialize the bounded immutable-run overlay on first use. Opening a
    /// sharded catalog therefore reads only its map; a read-only process that
    /// never touches payload membership pays no run GETs or decode memory.
    async fn ensure_catalog_runs_loaded(&self) -> io::Result<()> {
        if self.lazy_catalog.read().unwrap().run_refs.is_empty() {
            return Ok(());
        }
        let _load = self.catalog_run_load.lock().await;
        let snapshot = self.lazy_catalog.read().unwrap().clone();
        if snapshot.run_refs.is_empty() {
            return Ok(());
        }

        let (overlay, materialized, loaded) = self.reader().materialize_runs(&snapshot).await?;
        let mut overlay = overlay;
        let mut materialized = materialized;

        // Mutations may have landed while remote runs were loading. Capture
        // and replay their latest exact state while holding the normal index
        // then mutation lock order.
        let mut current = self.index.write().unwrap();
        let pending = self.pending_catalog.lock().unwrap();
        let mut lazy = self.lazy_catalog.write().unwrap();
        if lazy.run_refs != snapshot.run_refs || lazy.root_deltas != snapshot.root_deltas {
            return Err(io::Error::other(
                "pack catalog changed while immutable runs were loading",
            ));
        }
        if !pending.is_empty() {
            let delta = encode_index_mutations(&current, &pending)?;
            let decoded = decode_index_delta(&delta)?;
            materialized.apply(&decoded);
            apply_decoded_index_delta(&mut overlay, decoded);
        }
        *current = overlay;
        *lazy = materialized;
        self.index_catalog.lock().unwrap().runs.extend(loaded);
        Ok(())
    }

    async fn ensure_pack_loaded(&self, pack: PackId) -> io::Result<()> {
        if self.index.read().unwrap().packs.contains_key(&pack) {
            return Ok(());
        }
        let lazy = self.lazy_catalog.read().unwrap().clone();
        if lazy.changed_packs.contains(&pack) {
            return Ok(());
        }
        let Some(base) = lazy.base else {
            return Ok(());
        };
        let prefix = digest_prefix(pack.as_digest(), base.map.shard_bits)?;
        let Ok(at) = base
            .map
            .packs
            .binary_search_by_key(&prefix, |shard| shard.prefix)
        else {
            return Ok(());
        };
        let reference = base.map.packs[at];
        let bytes = self.load_catalog_shard(reference).await?;
        let mut shard = decode_pack_shard(&bytes, prefix, reference.entries)?;
        let Some(entries) = shard.packs.remove(&pack) else {
            return Ok(());
        };
        let pack_len = shard
            .pack_lengths
            .remove(&pack)
            .ok_or_else(|| io::Error::other("catalog pack shard has no pack length"))?;
        let dead = shard.tombstoned.remove(&pack);
        let records = shard.tombstone_records.remove(&pack);
        let mut index = self.index.write().unwrap();
        if index.packs.contains_key(&pack) {
            return Ok(());
        }
        if let Some(dead) = dead {
            index.tombstoned.insert(pack, dead);
        }
        if let Some(records) = records {
            index.tombstone_records.insert(pack, records);
        }
        index.add_pack(pack, pack_len, entries);
        Ok(())
    }

    #[cfg(test)]
    async fn pause_at_flush_handoff(&self) {
        let hook = self.flush_handoff_hook.lock().unwrap().take();
        if let Some(hook) = hook {
            let _ = hook.reached.send(());
            let _ = hook.resume.await;
        }
    }

    async fn is_staged(&self, digest: &ChunkId) -> bool {
        let staging = self.staging.lock().await;
        staging.digests.contains(digest)
            || self
                .inflight
                .lock()
                .await
                .as_ref()
                .is_some_and(|batch| batch.digests.contains(digest))
    }

    async fn staged(&self, digest: &ChunkId) -> Option<(u64, Bytes)> {
        let staging = self.staging.lock().await;
        if let Some(found) = staging.get(digest) {
            return Some(found);
        }
        self.inflight
            .lock()
            .await
            .as_ref()
            .and_then(|batch| batch.get(digest))
    }

    async fn read_location(&self, digest: ChunkId, location: Location) -> io::Result<Bytes> {
        self.reader().read_location(digest, location).await
    }

    async fn rebuild(&self) -> io::Result<()> {
        self.rebuild_inner(false).await
    }

    async fn rebuild_from_inventory(&self) -> io::Result<()> {
        self.rebuild_inner(true).await
    }

    #[tracing::instrument(
        name = "blob.pack_index.rebuild",
        skip_all,
        fields(force_inventory = force_inventory)
    )]
    async fn rebuild_inner(&self, force_inventory: bool) -> io::Result<()> {
        let _rebuild = self.rebuild_lock.lock().await;
        if !force_inventory {
            let Some(loaded) = self.load_authoritative_index_catalog(true).await? else {
                self.read_counters
                    .index_hits
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(());
            };
            if let Some(mut index) = loaded.index {
                let mut lazy = loaded.lazy;
                self.read_counters
                    .index_hits
                    .fetch_add(1, Ordering::Relaxed);
                let dirty = self.index_dirty.load(Ordering::Acquire);
                {
                    let mut current = self.index.write().unwrap();
                    if dirty {
                        let local = current.clone();
                        index.merge(local.clone());
                        let pending = self.pending_catalog.lock().unwrap();
                        if !pending.is_empty() {
                            let delta = encode_index_mutations(&local, &pending)?;
                            let decoded = decode_index_delta(&delta)?;
                            lazy.apply(&decoded);
                            apply_decoded_index_delta(&mut index, decoded);
                        }
                    } else {
                        self.pending_catalog.lock().unwrap().mutations = IndexMutations::default();
                    }
                    *current = index;
                }
                *self.lazy_catalog.write().unwrap() = lazy;
                // Only a successfully installed index may satisfy the next
                // unchanged-pointer check; overlay decoding can still fail.
                *self.index_catalog.lock().unwrap() = loaded.witness;
                if !dirty {
                    self.index_dirty.store(false, Ordering::Release);
                }
                return Ok(());
            }
            // Keep the CAS version needed to repair an invalid catalog, but
            // never reuse an index whose catalog could not be reconstructed.
            *self.index_catalog.lock().unwrap() = IndexCatalogWitness {
                root: None,
                ..loaded.witness
            };
        }
        self.read_counters
            .index_fallbacks
            .fetch_add(1, Ordering::Relaxed);

        let pack_prefix = kind_prefix(&self.base, PACKS_KIND);
        let replacement_prefix = kind_prefix(&self.base, REPLACEMENTS_KIND);
        let tombstone_prefix = kind_prefix(&self.base, TOMBSTONES_KIND);
        self.read_counters
            .list_requests
            .fetch_add(4, Ordering::Relaxed);
        let packs = self
            .object_store
            .list(Some(&pack_prefix))
            .try_collect::<Vec<_>>()
            .await
            .map_err(io::Error::other)?;
        let replacements = self
            .object_store
            .list(Some(&replacement_prefix))
            .try_collect::<Vec<_>>()
            .await
            .map_err(io::Error::other)?;
        let tombstones = self
            .object_store
            .list(Some(&tombstone_prefix))
            .try_collect::<Vec<_>>()
            .await
            .map_err(io::Error::other)?;
        let manifest_prefix = kind_prefix(&self.base, "blobs");
        let manifests = self
            .object_store
            .list(Some(&manifest_prefix))
            .try_collect::<Vec<_>>()
            .await
            .map_err(io::Error::other)?;

        let available: HashSet<PackId> = packs
            .iter()
            .map(|meta| digest_from_location(&meta.location).map(PackId::new))
            .collect::<io::Result<_>>()?;
        let mut superseded = HashSet::new();
        for meta in replacements {
            let expected = digest_from_location(&meta.location)?;
            let bytes = self
                .object_store
                .get(&meta.location)
                .await
                .map_err(io::Error::other)?
                .bytes()
                .await
                .map_err(io::Error::other)?;
            if Digest::from(blake3::hash(&bytes)) != expected {
                return Err(io::Error::other("pack replacement record hash mismatch"));
            }
            let (old, new) = decode_replacement(&bytes)?;
            if new.is_none_or(|id| available.contains(&id)) {
                superseded.insert(old);
            }
        }

        let mut tombstones_by_pack: HashMap<PackId, Vec<(Digest, Tombstone)>> = HashMap::new();
        for meta in tombstones {
            let expected = digest_from_location(&meta.location)?;
            let bytes = self
                .object_store
                .get(&meta.location)
                .await
                .map_err(io::Error::other)?
                .bytes()
                .await
                .map_err(io::Error::other)?;
            if Digest::from(blake3::hash(&bytes)) != expected {
                return Err(io::Error::other("pack tombstone record hash mismatch"));
            }
            for tombstone in decode_tombstone_record(&bytes)? {
                tombstones_by_pack
                    .entry(tombstone.pack)
                    .or_default()
                    .push((expected, tombstone));
            }
        }

        let mut rebuilt = Index {
            superseded: superseded.into(),
            manifests: ManifestIndex::from_unsorted(
                manifests
                    .into_iter()
                    .map(|meta| digest_from_location(&meta.location).map(BlobId::new))
                    .collect::<io::Result<_>>()?,
            ),
            manifests_complete: true,
            ..Index::default()
        };
        let live_packs = packs
            .into_iter()
            .map(|meta| {
                let pack = PackId::new(digest_from_location(&meta.location)?);
                Ok((pack, meta))
            })
            .collect::<io::Result<Vec<_>>>()?
            .into_iter()
            .filter(|(pack, _)| !rebuilt.superseded.contains(pack));
        let mut footers = futures::stream::iter(live_packs)
            .map(|(pack, meta)| async move {
                let entries = read_footer(
                    &self.object_store,
                    &meta.location,
                    meta.size,
                    &self.read_counters,
                )
                .await?;
                Ok::<_, io::Error>((pack, meta.size, entries))
            })
            .buffer_unordered(MAX_CONCURRENT_FOOTER_READS)
            .try_collect::<Vec<_>>()
            .await?;
        footers.sort_unstable_by_key(|(pack, _, _)| *pack);
        for (pack, pack_len, entries) in footers {
            if let Some(tombstones) = tombstones_by_pack.remove(&pack) {
                let mut dead = HashSet::new();
                for (record, tombstone) in tombstones {
                    if tombstone.entry_count != entries.len() as u64 {
                        return Err(io::Error::other(
                            "pack tombstone footer entry count mismatch",
                        ));
                    }
                    for (ordinal, entry) in entries.iter().enumerate() {
                        if tombstone.contains(ordinal) {
                            dead.insert(entry.digest);
                        }
                    }
                    rebuilt
                        .tombstone_records
                        .entry(pack)
                        .or_default()
                        .insert(record);
                }
                if !dead.is_empty() {
                    rebuilt.tombstoned.insert(pack, dead);
                }
            }
            rebuilt.add_pack_metadata(pack, pack_len, entries);
        }
        rebuilt.rebuild_chunks();
        {
            let mut current = self.index.write().unwrap();
            *current = rebuilt;
            self.pending_catalog.lock().unwrap().mutations = IndexMutations::default();
        }
        *self.lazy_catalog.write().unwrap() = LazyCatalogOverlay::default();
        self.catalog_run_indexes.lock().unwrap().clear();
        // Inventory reconstruction is authoritative even if checkpoint
        // publication fails. A later open can repeat the fallback.
        self.index_dirty.store(true, Ordering::Release);
        if !self.state_catalog_mode.load(Ordering::Acquire) {
            let _ = self.publish_current_index(true).await;
        }
        Ok(())
    }

    async fn load_authoritative_index_catalog(
        &self,
        reuse_unchanged: bool,
    ) -> io::Result<Option<LoadedIndexCatalog>> {
        self.read_counters
            .index_pointer_requests
            .fetch_add(1, Ordering::Relaxed);
        let pointer = match self
            .object_store
            .get(&self.base.clone().join(INDEX_POINTER_NAME))
            .await
        {
            Ok(pointer) => pointer,
            Err(object_store::Error::NotFound { .. }) => {
                return Ok(Some(LoadedIndexCatalog {
                    index: None,
                    witness: IndexCatalogWitness::default(),
                    lazy: LazyCatalogOverlay::default(),
                }));
            }
            Err(error) => return Err(io::Error::other(error)),
        };
        let version = UpdateVersion {
            e_tag: pointer.meta.e_tag.clone(),
            version: pointer.meta.version.clone(),
        };
        let catalog = pointer.bytes().await.map_err(io::Error::other)?;
        if reuse_unchanged {
            let started = Instant::now();
            let digest = Digest::from(blake3::hash(&catalog));
            self.read_counters.index_hash_nanos.fetch_add(
                u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            let mut witness = self.index_catalog.lock().unwrap();
            if witness.root.is_some() && witness.pointer_digest == Some(digest) {
                // The pointer was still read from storage: independent writers
                // remain visible, and staged local changes stay in the overlay.
                witness.version = Some(version);
                self.read_counters
                    .index_bytes
                    .fetch_add(catalog.len() as u64, Ordering::Relaxed);
                return Ok(None);
            }
        }
        if catalog.starts_with(&INDEX_CATALOG_MAGIC_V1)
            || catalog.starts_with(&delta::INDEX_CATALOG_MAGIC_V2)
        {
            return self
                .decode_v1_index_catalog(&catalog, version)
                .await
                .map(Some);
        }
        Ok(Some(LoadedIndexCatalog {
            index: None,
            witness: IndexCatalogWitness {
                version: Some(version),
                pointer_digest: Some(blake3::hash(&catalog).into()),
                ..IndexCatalogWitness::default()
            },
            lazy: LazyCatalogOverlay::default(),
        }))
    }

    async fn decode_v1_index_catalog(
        &self,
        catalog: &[u8],
        version: UpdateVersion,
    ) -> io::Result<LoadedIndexCatalog> {
        self.reader()
            .decode_v1_index_catalog(catalog, version)
            .await
    }

    #[cfg(test)]
    fn record_index_build(&self, started: Instant) {
        self.read_counters.index_build_nanos.fetch_add(
            started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.read_counters
            .index_build_calls
            .fetch_add(1, Ordering::Relaxed);
    }

    fn publication_snapshot(&self, index: &Index) -> Index {
        #[cfg(test)]
        let started = Instant::now();
        let snapshot = index.clone();
        #[cfg(test)]
        {
            self.read_counters.index_snapshot_nanos.fetch_add(
                started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            let accounting = Instant::now();
            // Count the payload the snapshot shares, separately from clone
            // timing. Collections are shared copy-on-write, so this is the
            // state a snapshot keeps alive, not bytes copied per snapshot. It
            // is a lower bound, not a claim about exact allocator consumption.
            let bytes = std::mem::size_of_val(index.chunks.base.as_slice())
                + std::mem::size_of_val(index.chunks.fanout.as_ref())
                + index.chunks.overlay.len() * std::mem::size_of::<(ChunkId, Location)>()
                + index.chunks.overlay_duplicates.len()
                    * std::mem::size_of::<(ChunkId, Vec<Location>)>()
                + index
                    .chunks
                    .overlay_duplicates
                    .values()
                    .map(|v| std::mem::size_of_val(v.as_slice()))
                    .sum::<usize>()
                + index.packs.len() * std::mem::size_of::<(PackId, Vec<PackEntry>)>()
                + index
                    .packs
                    .values()
                    .map(|v| std::mem::size_of_val(v.as_slice()))
                    .sum::<usize>()
                + index.pack_lengths.len() * std::mem::size_of::<(PackId, u64)>()
                + index.superseded.len() * std::mem::size_of::<PackId>()
                + index.tombstoned.len() * std::mem::size_of::<(PackId, HashSet<ChunkId>)>()
                + index
                    .tombstoned
                    .values()
                    .map(|v| v.len() * std::mem::size_of::<ChunkId>())
                    .sum::<usize>()
                + index.tombstone_records.len() * std::mem::size_of::<(PackId, HashSet<Digest>)>()
                + index
                    .tombstone_records
                    .values()
                    .map(|v| v.len() * std::mem::size_of::<Digest>())
                    .sum::<usize>()
                + std::mem::size_of_val(index.manifests.base.as_slice())
                + std::mem::size_of_val(index.manifests.fanout.as_ref())
                + (index.manifests.overlay.len() + index.manifests.removed.len())
                    * std::mem::size_of::<BlobId>();
            self.read_counters
                .index_snapshot_payload_bytes_lower_bound
                .fetch_add(bytes as u64, Ordering::Relaxed);
            self.read_counters
                .index_snapshot_calls
                .fetch_add(1, Ordering::Relaxed);
            self.read_counters
                .index_snapshot_accounting_nanos
                .fetch_add(
                    accounting
                        .elapsed()
                        .as_nanos()
                        .try_into()
                        .unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
        }
        snapshot
    }

    #[tracing::instrument(
        name = "blob.pack_index.publish",
        skip_all,
        fields(force = force)
    )]
    async fn publish_current_index(&self, force: bool) -> io::Result<()> {
        let _checkpoint = self.checkpoint_lock.lock().await;
        if !force && !self.index_dirty.load(Ordering::Acquire) {
            return Ok(());
        }
        // Hold the process-shared lock across snapshot selection, merge and
        // pointer CAS. Waiting happens before taking pending mutations, so
        // cancellation while waiting leaves all unpublished work intact.
        let local_lock = match &self.local_durability {
            Some(local) => Some(local.lock_catalog().await?),
            None => None,
        };
        let dirty = self.index_dirty.swap(false, Ordering::AcqRel);
        if !force && !dirty {
            return Ok(());
        }
        if let Err(error) = self.ensure_catalog_runs_loaded().await {
            self.index_dirty.store(dirty, Ordering::Release);
            return Err(error);
        }
        // Every mutation takes the index lock before the mutation lock. Taking
        // both here makes the snapshot and its exact change set indivisible.
        let (mut candidate, mutations, mut lazy) = {
            let index = self.index.read().unwrap();
            let mut pending = self.pending_catalog.lock().unwrap();
            let lazy = self.lazy_catalog.read().unwrap().clone();
            (
                self.publication_snapshot(&index),
                std::mem::take(&mut *pending),
                lazy,
            )
        };
        let mut preparation = CatalogPreparation {
            packed: self,
            changes: Some(mutations),
            background_start: None,
        };
        let mutations = preparation.changes.as_ref().unwrap();
        let delta = if mutations.is_empty() {
            None
        } else {
            Some(encode_index_mutations(&candidate, mutations)?)
        };
        let mut witness = self.index_catalog.lock().unwrap().clone();
        for _ in 0..MAX_INDEX_PUBLISH_ATTEMPTS {
            match self
                .publish_index_catalog(
                    &candidate,
                    delta.as_deref(),
                    &mutations.retirements,
                    force,
                    &witness,
                    &lazy,
                    local_lock.as_ref(),
                )
                .await
            {
                Ok(mut next) => {
                    let rebased = next.prepared_rebase.take();
                    let published_map = next.prepared_map.take();
                    if let Some(base) = rebased {
                        self.install_rebased_base(base)?;
                        *self.index_catalog.lock().unwrap() = next;
                        self.published_retirements
                            .lock()
                            .unwrap()
                            .extend(preparation.changes.take().unwrap().retirements);
                        return Ok(());
                    }
                    // Preserve mutations that landed after the snapshot while
                    // also retaining unrelated state learned after contention.
                    let mut current = self.index.write().unwrap();
                    let pending = self.pending_catalog.lock().unwrap();
                    if !pending.is_empty() {
                        let pending_delta = encode_index_mutations(&current, &pending)?;
                        let decoded = decode_index_delta(&pending_delta)?;
                        lazy.apply(&decoded);
                        apply_decoded_index_delta(&mut candidate, decoded);
                    }
                    if let (Some(current), Some(base)) = (&mut lazy.base, published_map) {
                        *current = base;
                    }
                    *current = candidate;
                    *self.lazy_catalog.write().unwrap() = lazy;
                    *self.index_catalog.lock().unwrap() = next;
                    self.published_retirements
                        .lock()
                        .unwrap()
                        .extend(preparation.changes.take().unwrap().retirements);
                    return Ok(());
                }
                Err(error) if index_publish_contended(&error) => {
                    let loaded = self
                        .load_authoritative_index_catalog(false)
                        .await?
                        .expect("catalog reuse is disabled during CAS retry");
                    witness = loaded.witness;
                    lazy = loaded.lazy;
                    if let Some(mut latest) = loaded.index {
                        if let Some(delta) = &delta {
                            let decoded = decode_index_delta(delta)?;
                            lazy.apply(&decoded);
                            apply_decoded_index_delta(&mut latest, decoded);
                        } else {
                            latest.merge(candidate);
                        }
                        candidate = latest;
                    } else {
                        return Err(io::Error::other(
                            "contended pack catalog could not be reconstructed",
                        ));
                    }
                }
                Err(error) => {
                    return Err(io::Error::other(error));
                }
            }
        }
        Err(io::Error::other("pack index catalog remained contended"))
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the pointer check needs the retirements published beside the delta"
    )]
    async fn publish_index_catalog(
        &self,
        candidate: &Index,
        delta: Option<&[u8]>,
        retirements: &HashSet<Path>,
        force: bool,
        witness: &IndexCatalogWitness,
        lazy: &LazyCatalogOverlay,
        local_lock: Option<&LocalCatalogLock>,
    ) -> Result<IndexCatalogWitness, object_store::Error> {
        #[cfg(test)]
        let build_started = Instant::now();
        let (mut next, catalog) = self
            .build_index_catalog(candidate, delta, force, true, witness, lazy)
            .await?;
        #[cfg(test)]
        self.record_index_build(build_started);
        // Both CAS paths below: the shared store's conditional PUT and
        // `LocalCatalogLock::compare_and_put`.
        if let Some(root) = &next.root {
            self.check_catalog_pointer(witness, root, &catalog, delta, candidate, retirements)
                .map_err(|violation| object_store::Error::Generic {
                    store: "pack index catalog",
                    source: Box::new(violation),
                })?;
        }
        self.read_counters
            .index_put_requests
            .fetch_add(1, Ordering::Relaxed);
        self.read_counters
            .index_put_bytes
            .fetch_add(catalog.len() as u64, Ordering::Relaxed);
        let mode = match &witness.version {
            Some(version) => PutMode::Update(version.clone()),
            None => PutMode::Create,
        };
        let catalog_path = self.base.clone().join(INDEX_POINTER_NAME);
        let result = if let Some(local) = local_lock {
            let published = local
                .compare_and_put(
                    &catalog_path,
                    catalog.clone(),
                    witness.pointer_digest.map(|digest| *digest.as_bytes()),
                )
                .await
                .map_err(|source| object_store::Error::Generic {
                    store: "durable local pack catalog",
                    source: Box::new(source),
                })?;
            if !published {
                return Err(object_store::Error::Precondition {
                    path: catalog_path.to_string(),
                    source: Box::new(io::Error::other(
                        "local catalog advanced during publication",
                    )),
                });
            }
            object_store::PutResult {
                e_tag: None,
                version: None,
                extensions: Default::default(),
            }
        } else {
            // The pointer is the shared catalog's compare-and-swap. A store
            // that cannot condition the write would let concurrent publishers
            // overwrite each other, so refuse rather than degrade to `put`.
            self.object_store
                .put_opts(
                    &catalog_path,
                    catalog.clone().into(),
                    PutOptions {
                        mode,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|error| match error {
                    object_store::Error::NotImplemented { .. }
                    | object_store::Error::NotSupported { .. } => {
                        object_store::Error::NotSupported {
                            source: Box::new(io::Error::other(format!(
                                "pack catalog publication requires conditional writes \
                                 from the object store (for a local directory, use \
                                 ChunkedBlobStore::local_packed): {error}"
                            ))),
                        }
                    }
                    error => error,
                })?
        };
        next.version = Some(result.into());
        next.pointer_digest = Some(blake3::hash(&catalog).into());
        Ok(next)
    }

    fn catalog_rebase_due(&self, previous: &DeltaCatalog, next: &[u8]) -> bool {
        let run_bytes = previous
            .runs
            .values()
            .map(|run| run.encoded_bytes)
            .chain(previous.deltas.iter().map(|delta| delta.len() as u64 + 8))
            .fold(next.len() as u64 + 8, u64::saturating_add);
        let routing_bytes = previous.runs.values().fold(0_u64, |total, run| {
            total.saturating_add(CATALOG_RUN_REF_BYTES).saturating_add(
                run.query.as_ref().map_or(0, |query| {
                    CATALOG_RUN_QUERY_REF_BYTES.saturating_add(query.routing.len() as u64)
                }),
            )
        });
        run_bytes >= self.catalog_rebase_run_bytes.load(Ordering::Relaxed)
            || (!matches!(previous.base, CatalogBase::Sharded { .. })
                && routing_bytes >= CATALOG_REBASE_INLINE_ROUTING_BYTES)
            || previous.runs.len() >= CATALOG_REBASE_RUN_REFS
    }

    async fn build_background_rebase_catalog(
        &self,
        candidate: &Index,
        mutations: &IndexMutations,
        previous: &IndexCatalogWitness,
        ready: &ShardedIndexBase,
    ) -> Result<(IndexCatalogWitness, Bytes, ShardedIndexBase), object_store::Error> {
        let generation =
            previous
                .generation
                .checked_add(1)
                .ok_or_else(|| object_store::Error::Generic {
                    store: "pack index catalog",
                    source: "pack index catalog generation overflow".into(),
                })?;
        let mut installed = ready.clone();
        let mut root = DeltaCatalog {
            sidecars: None,
            generation,
            base: CatalogBase::Inline(Bytes::new()),
            runs: BTreeMap::new(),
            deltas: Vec::new(),
        };
        let mut loaded_runs = BTreeMap::new();
        let delta = if mutations.is_empty() {
            None
        } else {
            Some(
                encode_index_mutations(candidate, mutations).map_err(|error| {
                    object_store::Error::Generic {
                        store: "pack index catalog",
                        source: Box::new(error),
                    }
                })?,
            )
        };
        if let Some(delta) = delta {
            if !delta_catalog_needs_compaction(&root, &delta) {
                root.deltas.push(delta);
            } else {
                let run = CatalogRun {
                    first_generation: generation,
                    last_generation: generation,
                    delta,
                };
                let encoded =
                    encode_catalog_run(&run).map_err(|error| object_store::Error::Generic {
                        store: "pack index catalog",
                        source: Box::new(error),
                    })?;
                let mut query = catalog_run_query_ref(&encoded).map_err(|error| {
                    object_store::Error::Generic {
                        store: "pack index catalog",
                        source: Box::new(error),
                    }
                })?;
                let digest = Digest::from(blake3::hash(&encoded));
                self.mark_catalog_reclaim_due().await.map_err(|error| {
                    object_store::Error::Generic {
                        store: "pack index catalog",
                        source: Box::new(error),
                    }
                })?;
                self.put_catalog_object(digest, encoded.clone())
                    .await
                    .map_err(|error| object_store::Error::Generic {
                        store: "pack index catalog",
                        source: Box::new(error),
                    })?;
                let mut map = (*ready.map).clone();
                map.run_routing.insert(digest, query.routing.clone());
                query.routing = Bytes::new();
                let map_bytes =
                    encode_shard_map(&map).map_err(|error| object_store::Error::Generic {
                        store: "pack index catalog",
                        source: Box::new(error),
                    })?;
                let map_digest = Digest::from(blake3::hash(&map_bytes));
                self.put_catalog_object(map_digest, map_bytes)
                    .await
                    .map_err(|error| object_store::Error::Generic {
                        store: "pack index catalog",
                        source: Box::new(error),
                    })?;
                installed = ShardedIndexBase { map: Arc::new(map) };
                root.runs.insert(
                    0,
                    CatalogRunRef {
                        digest,
                        first_generation: generation,
                        last_generation: generation,
                        encoded_bytes: encoded.len() as u64,
                        query: Some(query),
                    },
                );
                loaded_runs.insert(0, run);
            }
        }
        let map =
            encode_shard_map(&installed.map).map_err(|error| object_store::Error::Generic {
                store: "pack index catalog",
                source: Box::new(error),
            })?;
        root.base = CatalogBase::Sharded {
            root: Digest::from(blake3::hash(&map)),
            shard_bits: installed.map.shard_bits,
        };
        let catalog =
            encode_delta_catalog(&root).map_err(|error| object_store::Error::Generic {
                store: "pack index catalog",
                source: Box::new(error),
            })?;
        Ok((
            IndexCatalogWitness {
                version: previous.version.clone(),
                external: None,
                pointer_digest: previous.pointer_digest,
                generation,
                root: Some(root),
                runs: loaded_runs,
                prepared_rebase: None,
                prepared_map: None,
            },
            catalog,
            installed,
        ))
    }

    #[cfg(test)]
    pub(crate) fn set_catalog_rebase_run_bytes_for_test(&self, bytes: u64) {
        assert!(bytes > 0, "catalog rebase threshold must be non-zero");
        self.catalog_rebase_run_bytes
            .store(bytes, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(super) async fn wait_for_background_catalog_rebase(&self) -> io::Result<()> {
        let pending = self
            .background_catalog_rebase
            .lock()
            .unwrap()
            .pending
            .take();
        if let Some(start) = pending {
            self.build_queued_catalog_rebase(start).await?;
        }
        let completed = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let status = {
                    let background = self.background_catalog_rebase.lock().unwrap();
                    background.job.as_ref().map(|job| match &job.phase {
                        CatalogBackgroundRebasePhase::Ready(_) => Ok(true),
                        CatalogBackgroundRebasePhase::Failed(_) => {
                            Err(io::Error::other("background catalog rebase failed"))
                        }
                        CatalogBackgroundRebasePhase::Armed
                        | CatalogBackgroundRebasePhase::Building => Ok(false),
                    })
                };
                match status {
                    Some(Ok(true)) => return Ok(()),
                    Some(Err(error)) => return Err(error),
                    Some(Ok(false)) => {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    }
                    None => return Err(io::Error::other("no background catalog rebase")),
                }
            }
        })
        .await;
        completed.unwrap_or_else(|_| {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "background catalog rebase did not finish within 30 seconds",
            ))
        })
    }

    async fn put_catalog_object(&self, digest: Digest, bytes: Bytes) -> io::Result<()> {
        self.read_counters
            .index_put_requests
            .fetch_add(1, Ordering::Relaxed);
        self.read_counters
            .index_put_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let path = sharded_path(&self.base, INDEXES_KIND, &digest);
        check_content_address(&path, &bytes)?;
        if let Some(local) = &self.local_durability {
            local.put(&path, bytes).await?;
        } else {
            put_object(&self.object_store, &path, bytes, true)
                .await
                .map_err(io::Error::other)?;
        }
        self.record_catalog_objects_written([digest]);
        Ok(())
    }

    fn catalog_object_publication(&self) -> CatalogObjectPublication<'_> {
        CatalogObjectPublication {
            packed: self,
            pending: FuturesUnordered::new(),
            prepared: Vec::new(),
            staged: Vec::new(),
        }
    }

    async fn mark_catalog_reclaim_due(&self) -> io::Result<()> {
        if self.uses_state_catalog() && self.local_durability.is_none() {
            return Ok(());
        }
        let marker = self.base.clone().join(INDEX_RECLAIM_MARKER_NAME);
        let bytes = Bytes::from_static(b"catalog garbage may be present\n");
        if let Some(local) = &self.local_durability {
            local.put(&marker, bytes).await?;
        } else {
            self.object_store
                .put(&marker, bytes.into())
                .await
                .map_err(io::Error::other)?;
        }
        Ok(())
    }

    pub(crate) async fn catalog_reclaim_due(&self) -> io::Result<bool> {
        let marker = self.base.clone().join(INDEX_RECLAIM_MARKER_NAME);
        match self.object_store.head(&marker).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(io::Error::other(error)),
        }
    }

    /// Remove immutable catalog objects unreachable from the installed root or
    /// any explicitly pinned historical root.
    ///
    /// The checkpoint lock fences publication in this handle. Callers must
    /// additionally exclude publishers and readers in every other process;
    /// [`Repository::vacuum`](crate::repository::Repository::vacuum) owns that
    /// admission boundary, including maintenance through publication or discard.
    /// Other callers must also protect unpublished maintenance candidates.
    #[tracing::instrument(name = "blob.pack_index.reclaim", skip_all, fields(pinned_catalogs = pinned_catalogs.len()))]
    pub(crate) async fn reclaim_catalog_objects(
        &self,
        pinned_catalogs: &[Bytes],
    ) -> io::Result<CatalogReclaimStats> {
        self.reclaim_catalog_objects_inner(pinned_catalogs, None)
            .await
    }

    /// Advisory cleanup can stop between settled batches when another writer
    /// admits new catalog protection. Keep the marker for a fresh mark later.
    pub(crate) async fn reclaim_catalog_metadata_pinned(
        &self,
        store: Arc<dyn crate::metadata::PinStore>,
        owned_claims: BTreeSet<crate::metadata::PinToken>,
    ) -> io::Result<()> {
        match self
            .reclaim_catalog_objects_pinned(store, owned_claims)
            .await
        {
            Err(error)
                if error
                    .get_ref()
                    .is_some_and(|error| error.is::<CatalogPinsChanged>()) =>
            {
                tracing::debug!("catalog cleanup deferred after catalog pins changed");
                Ok(())
            }
            result => result.map(|_| ()),
        }
    }

    pub(crate) async fn reclaim_catalog_objects_pinned(
        &self,
        store: Arc<dyn crate::metadata::PinStore>,
        owned_claims: BTreeSet<crate::metadata::PinToken>,
    ) -> io::Result<CatalogReclaimStats> {
        let inventory = Arc::new(store.inventory().await.map_err(io::Error::other)?);
        let mut catalogs = BTreeSet::new();
        for pin in inventory.pins.values() {
            if let Some(catalog) = &pin.catalog {
                catalogs.insert(Bytes::copy_from_slice(catalog));
            }
            for resource in &pin.resources {
                if let crate::metadata::PinResource::Catalog(catalog) = resource {
                    catalogs.insert(Bytes::copy_from_slice(catalog));
                }
            }
        }
        let mark = CatalogPinMark {
            store,
            inventory,
            owned_claims: Arc::new(owned_claims),
        };
        self.reclaim_catalog_objects_inner(&catalogs.into_iter().collect::<Vec<_>>(), Some(&mark))
            .await
    }

    async fn reclaim_catalog_objects_inner(
        &self,
        pinned_catalogs: &[Bytes],
        pin_mark: Option<&CatalogPinMark>,
    ) -> io::Result<CatalogReclaimStats> {
        const DELETE_BATCH: usize = 1_000;

        let _checkpoint = self.checkpoint_lock.lock().await;
        if self.catalog_prepared.load(Ordering::Acquire) {
            return Err(io::Error::other(
                "cannot reclaim catalog objects while a state commit is prepared",
            ));
        }

        let current = {
            let witness = self.index_catalog.lock().unwrap();
            match witness.external {
                Some(reference) => Some(reference.encode()),
                None => witness
                    .root
                    .as_ref()
                    .map(encode_delta_catalog)
                    .transpose()?,
            }
        };
        let mut retained = HashSet::new();
        if let Some(current) = &current {
            self.mark_catalog_objects(current, &mut retained).await?;
        }
        for catalog in pinned_catalogs {
            self.mark_catalog_objects(catalog, &mut retained).await?;
        }

        let retained_paths: HashSet<_> = pin_mark
            .into_iter()
            .flat_map(|mark| mark.inventory.pins.values())
            .flat_map(|pin| &pin.resources)
            .filter_map(|resource| match resource {
                crate::metadata::PinResource::StorageObject(path) => Some(path.as_str()),
                _ => None,
            })
            .collect();
        let prefix = kind_prefix(&self.base, INDEXES_KIND);
        let mut listed = self.object_store.list(Some(&prefix));
        let mut deletes = Vec::with_capacity(DELETE_BATCH);
        let mut delete_bytes = 0usize;
        let mut stats = CatalogReclaimStats::default();
        while let Some(object) = listed.next().await {
            let object = object.map_err(io::Error::other)?;
            stats.listed_objects = stats.listed_objects.saturating_add(1);
            let Ok(digest) = digest_from_location(&object.location) else {
                // Foreign or malformed objects in the namespace are not ours
                // to delete automatically.
                continue;
            };
            if retained.contains(&digest) || retained_paths.contains(object.location.as_ref()) {
                stats.retained_objects = stats.retained_objects.saturating_add(1);
                continue;
            }
            stats.deleted_objects = stats.deleted_objects.saturating_add(1);
            stats.deleted_bytes = stats.deleted_bytes.saturating_add(object.size);
            if crate::invariant::DEBUG {
                // A reclaimed object no longer witnesses a later pointer.
                self.catalog_objects_written
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&digest);
            }
            // Bound the durable claim by encoded size as well as object count;
            // long repository prefixes must still fit the local GC reserve.
            let resource_bytes = object.location.as_ref().len().saturating_add(9);
            if !deletes.is_empty() && delete_bytes.saturating_add(resource_bytes) > 32 * 1024 {
                self.delete_catalog_batch(std::mem::take(&mut deletes), pin_mark)
                    .await?;
                delete_bytes = 0;
            }
            delete_bytes = delete_bytes.saturating_add(resource_bytes);
            deletes.push(object.location);
            if deletes.len() == DELETE_BATCH {
                self.delete_catalog_batch(std::mem::take(&mut deletes), pin_mark)
                    .await?;
                delete_bytes = 0;
            }
        }
        if !deletes.is_empty() {
            self.delete_catalog_batch(deletes, pin_mark).await?;
        }
        if pinned_catalogs.is_empty() && pin_mark.is_none_or(|mark| mark.inventory.pins.is_empty())
        {
            let marker = self.base.clone().join(INDEX_RECLAIM_MARKER_NAME);
            self.delete_catalog_batch(vec![marker], pin_mark).await?;
        }
        tracing::info!(
            listed_objects = stats.listed_objects,
            retained_objects = stats.retained_objects,
            deleted_objects = stats.deleted_objects,
            deleted_bytes = stats.deleted_bytes,
            "pack catalog reclamation completed"
        );
        Ok(stats)
    }

    async fn mark_catalog_objects(
        &self,
        catalog: &[u8],
        retained: &mut HashSet<Digest>,
    ) -> io::Result<()> {
        let resolved = if let Some(reference) = external::ExternalCatalog::decode(catalog)? {
            retained.insert(reference.digest);
            Some(self.resolve_state_catalog(catalog).await?)
        } else {
            None
        };
        let root = decode_delta_catalog(resolved.as_deref().unwrap_or(catalog))?;
        for run in root.runs.values() {
            retained.insert(run.digest);
        }
        match root.base {
            CatalogBase::Inline(_) => Ok(()),
            CatalogBase::Checkpoint(digest) => {
                retained.insert(digest);
                Ok(())
            }
            CatalogBase::Sharded { root, shard_bits } => {
                retained.insert(root);
                let path = sharded_path(&self.base, INDEXES_KIND, &root);
                let bytes = self
                    .object_store
                    .get(&path)
                    .await
                    .map_err(io::Error::other)?
                    .bytes()
                    .await
                    .map_err(io::Error::other)?;
                if Digest::from(blake3::hash(&bytes)) != root {
                    return Err(io::Error::other("catalog shard-map identity mismatch"));
                }
                let map = decode_shard_map(&bytes)?;
                if map.shard_bits != shard_bits {
                    return Err(io::Error::other("catalog shard-map width mismatch"));
                }
                retained.extend(
                    map.chunks
                        .iter()
                        .chain(&map.manifests)
                        .chain(&map.packs)
                        .map(|reference| reference.digest),
                );
                Ok(())
            }
        }
    }

    async fn delete_catalog_batch(
        &self,
        paths: Vec<Path>,
        mark: Option<&CatalogPinMark>,
    ) -> io::Result<()> {
        let local = self.local_durability.clone();
        let objects = self.object_store.clone();
        let mark = mark.cloned();
        let (send, receive) = tokio::sync::oneshot::channel();
        crate::metadata::spawn_lease_task(async move {
            let result = async {
                let requested: BTreeSet<_> = if mark.is_some() {
                    paths
                        .iter()
                        .map(|path| crate::metadata::PinResource::StorageObject(path.to_string()))
                        .collect()
                } else {
                    BTreeSet::new()
                };
                let claim = if let Some(mark) = &mark {
                    Some(loop {
                        let inventory = mark.store.inventory().await.map_err(io::Error::other)?;
                        if inventory.collector != mark.inventory.collector {
                            return Err(io::Error::new(
                                io::ErrorKind::WouldBlock,
                                "collector changed during catalog reclamation",
                            ));
                        }
                        if !inventory.same_payload_pins(&mark.inventory) {
                            if inventory.collector.is_some() && inventory.logical_prune.is_none() {
                                return Err(io::Error::new(
                                    io::ErrorKind::WouldBlock,
                                    CatalogPinsChanged,
                                ));
                            }
                            return Err(io::Error::new(
                                io::ErrorKind::WouldBlock,
                                "catalog pins changed during reclamation",
                            ));
                        }
                        let covered: BTreeSet<_> = inventory
                            .deletions
                            .iter()
                            .filter(|(token, _)| mark.owned_claims.contains(token))
                            .flat_map(|(_, resources)| resources.iter().cloned())
                            .collect();
                        let resources: BTreeSet<_> =
                            requested.difference(&covered).cloned().collect();
                        if resources.is_empty() {
                            break (None, inventory, resources);
                        }
                        if !mark.store.allows_deletion(&inventory)
                            || inventory
                                .pins
                                .values()
                                .any(|pin| !pin.resources.is_disjoint(&resources))
                            || inventory.deletions.iter().any(|(token, claim)| {
                                !mark.owned_claims.contains(token) && !claim.is_disjoint(&resources)
                            })
                        {
                            return Err(io::Error::new(
                                io::ErrorKind::WouldBlock,
                                "catalog deletion conflicts with a pin or deletion",
                            ));
                        }
                        // Kept only to check the claim against the deletions it covers.
                        let claimed = if invariant::ENABLED {
                            resources.clone()
                        } else {
                            BTreeSet::new()
                        };
                        if let Some(token) = mark
                            .store
                            .claim_deletions(inventory.revision, resources)
                            .await
                            .map_err(io::Error::other)?
                        {
                            break (Some(token), inventory, claimed);
                        }
                        // Concurrent disjoint compactions also advance this
                        // ledger. Retry admission against their settled state.
                    })
                } else {
                    None
                };
                if let (Some(mark), Some((_, inventory, claimed))) = (&mark, &claim) {
                    invariant::check(|| {
                        validate_deletion_claim(inventory, &mark.owned_claims, claimed, &requested)
                    })?;
                }
                let issued = paths.len();
                let mut settled = 0_usize;
                if let Some(local) = local {
                    if paths.len() == 1 {
                        local.delete(&paths[0]).await?;
                    } else {
                        local.delete_many(paths).await?;
                    }
                    settled = issued;
                } else {
                    let locations =
                        futures::stream::iter(paths.into_iter().map(Ok::<_, object_store::Error>))
                            .boxed();
                    let mut deletes = objects.delete_stream(locations);
                    while let Some(result) = deletes.next().await {
                        match result {
                            Ok(_) | Err(object_store::Error::NotFound { .. }) => settled += 1,
                            Err(error) => return Err(io::Error::other(error)),
                        }
                    }
                }
                if let (Some(mark), Some((Some(token), _, claimed))) = (&mark, &claim) {
                    invariant::check(|| {
                        validate_deletion_finish(
                            claimed,
                            &requested,
                            issued.saturating_sub(settled),
                        )
                    })?;
                    mark.store
                        .finish_deletions(token)
                        .await
                        .map_err(io::Error::other)?;
                }
                Ok(())
            }
            .await;
            drop(send.send(result));
            Ok(())
        });
        receive.await.map_err(io::Error::other)?
    }

    async fn stage_catalog_root_overlay(
        &self,
        root: &DeltaCatalog,
        base: &ShardedIndexBase,
    ) -> io::Result<CatalogRunReaders> {
        let mut staged = Vec::with_capacity(root.runs.len() + root.deltas.len());
        let mut references = root.runs.values().cloned().collect::<Vec<_>>();
        references.sort_unstable_by_key(|reference| reference.first_generation);
        for mut reference in references {
            if let Some(query) = reference.query.as_mut()
                && query.routing.is_empty()
            {
                query.routing = base
                    .map
                    .run_routing
                    .get(&reference.digest)
                    .cloned()
                    .ok_or_else(|| io::Error::other("catalog rebase map is missing run routing"))?;
            }
            staged.push(self.stage_catalog_run(reference).await?);
        }
        let delta_first = root
            .generation
            .checked_sub(root.deltas.len() as u64)
            .and_then(|generation| generation.checked_add(1))
            .ok_or_else(|| io::Error::other("catalog rebase delta generations underflow"))?;
        for (ordinal, delta) in root.deltas.iter().enumerate() {
            let generation = delta_first + ordinal as u64;
            let run = CatalogRun {
                first_generation: generation,
                last_generation: generation,
                delta: delta.clone(),
            };
            let encoded = encode_catalog_run(&run)?;
            let reference = CatalogRunRef {
                digest: Digest::from(blake3::hash(&encoded)),
                first_generation: generation,
                last_generation: generation,
                encoded_bytes: encoded.len() as u64,
                query: Some(catalog_run_query_ref(&encoded)?),
            };
            staged.push(stage_bytes(&encoded, reference)?);
        }
        staged.sort_unstable_by_key(|run| run.reference.first_generation);
        tokio::task::spawn_blocking(move || {
            let prepared = merge_staged_runs(&staged)?;
            stage_file(prepared.file, prepared.reference)?.into_readers()
        })
        .await
        .map_err(io::Error::other)?
    }

    /// Fold one frozen run epoch into the immutable shard base without ever
    /// constructing the repository-wide Index. At most one old shard, one new
    /// shard, the changed-pack routing set, and small iterator buffers are live.
    async fn rebase_catalog_shards_streaming(
        &self,
        root: &DeltaCatalog,
        old: &ShardedIndexBase,
    ) -> io::Result<ShardedIndexBase> {
        self.mark_catalog_reclaim_due().await?;
        let mut overlay = self.stage_catalog_root_overlay(root, old).await?;
        let changed_packs = overlay
            .changed_packs()
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let base_entries = old
            .map
            .chunks
            .iter()
            .map(|shard| shard.entries)
            .sum::<u64>();
        let shard_bits = old.map.shard_bits.max(recommended_shard_bits(
            base_entries.saturating_add(overlay.chunk_entries()),
            DEFAULT_SHARD_TARGET_BYTES,
        )?);
        let expansion = shard_bits - old.map.shard_bits;
        let parent_ref = |refs: &[ShardRef], prefix: u32| {
            let parent = prefix >> expansion;
            refs.binary_search_by_key(&parent, |reference| reference.prefix)
                .ok()
                .map(|at| refs[at])
        };
        let prefix_count = 1_u32 << shard_bits;
        let mut publication = self.catalog_object_publication();

        let mut chunk_refs = Vec::new();
        let mut next_chunk = overlay.next_chunk()?;
        for prefix in 0..prefix_count {
            let has_overlay = next_chunk.as_ref().is_some_and(|entry| {
                digest_prefix(entry.digest.as_digest(), shard_bits)
                    .is_ok_and(|known| known == prefix)
            });
            let parent = parent_ref(&old.map.chunks, prefix);
            if parent.is_none() && !has_overlay {
                continue;
            }
            let mut entries = if let Some(reference) = parent {
                let bytes = self.load_catalog_shard(reference).await?;
                decode_chunk_shard(&bytes, reference.prefix)?
                    .into_iter()
                    .filter(|entry| {
                        !changed_packs.contains(&entry.location.pack)
                            && digest_prefix(entry.digest.as_digest(), shard_bits)
                                .is_ok_and(|known| known == prefix)
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            while next_chunk.as_ref().is_some_and(|entry| {
                digest_prefix(entry.digest.as_digest(), shard_bits)
                    .is_ok_and(|known| known == prefix)
            }) {
                entries.push(next_chunk.take().expect("matched streamed chunk"));
                next_chunk = overlay.next_chunk()?;
            }
            if !entries.is_empty() {
                let (reference, bytes) =
                    encode_chunk_shard_object(shard_bits, prefix, &mut entries)?;
                publication.put(reference.digest, bytes).await?;
                chunk_refs.push(reference);
            }
        }
        if next_chunk.is_some() {
            return Err(io::Error::other("streamed chunk lies outside shard width"));
        }

        let mut manifest_refs = Vec::new();
        let mut next_added = overlay.next_added_manifest()?;
        let mut next_removed = overlay.next_removed_manifest()?;
        for prefix in 0..prefix_count {
            let added_here = next_added.as_ref().is_some_and(|manifest| {
                digest_prefix(manifest.as_digest(), shard_bits).is_ok_and(|known| known == prefix)
            });
            let removed_here = next_removed.as_ref().is_some_and(|manifest| {
                digest_prefix(manifest.as_digest(), shard_bits).is_ok_and(|known| known == prefix)
            });
            let parent = parent_ref(&old.map.manifests, prefix);
            if parent.is_none() && !added_here && !removed_here {
                continue;
            }
            let mut removed = HashSet::new();
            while next_removed.as_ref().is_some_and(|manifest| {
                digest_prefix(manifest.as_digest(), shard_bits).is_ok_and(|known| known == prefix)
            }) {
                removed.insert(next_removed.take().expect("matched removed manifest"));
                next_removed = overlay.next_removed_manifest()?;
            }
            let mut manifests = if let Some(reference) = parent {
                let bytes = self.load_catalog_shard(reference).await?;
                list_manifest_shard(&bytes, reference.prefix)?
                    .into_iter()
                    .filter(|manifest| {
                        !removed.contains(manifest)
                            && digest_prefix(manifest.as_digest(), shard_bits)
                                .is_ok_and(|known| known == prefix)
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            while next_added.as_ref().is_some_and(|manifest| {
                digest_prefix(manifest.as_digest(), shard_bits).is_ok_and(|known| known == prefix)
            }) {
                manifests.push(next_added.take().expect("matched added manifest"));
                next_added = overlay.next_added_manifest()?;
            }
            manifests.sort_unstable();
            manifests.dedup();
            if !manifests.is_empty() {
                let (reference, bytes) =
                    encode_manifest_shard_object(shard_bits, prefix, &manifests)?;
                publication.put(reference.digest, bytes).await?;
                manifest_refs.push(reference);
            }
        }
        if next_added.is_some() || next_removed.is_some() {
            return Err(io::Error::other(
                "streamed manifest lies outside shard width",
            ));
        }

        let mut pack_refs = Vec::new();
        let mut next_pack = overlay.next_pack()?;
        let mut next_removed_pack = overlay.next_removed_pack()?;
        let mut next_superseded = overlay.next_superseded()?;
        for prefix in 0..prefix_count {
            let present_here = next_pack.as_ref().is_some_and(|(pack, _)| {
                digest_prefix(pack.as_digest(), shard_bits).is_ok_and(|known| known == prefix)
            });
            let removed_here = next_removed_pack.as_ref().is_some_and(|pack| {
                digest_prefix(pack.as_digest(), shard_bits).is_ok_and(|known| known == prefix)
            });
            let superseded_here = next_superseded.as_ref().is_some_and(|pack| {
                digest_prefix(pack.as_digest(), shard_bits).is_ok_and(|known| known == prefix)
            });
            let parent = parent_ref(&old.map.packs, prefix);
            if parent.is_none() && !present_here && !removed_here && !superseded_here {
                continue;
            }
            while next_removed_pack.as_ref().is_some_and(|pack| {
                digest_prefix(pack.as_digest(), shard_bits).is_ok_and(|known| known == prefix)
            }) {
                next_removed_pack = overlay.next_removed_pack()?;
            }
            let mut merged = Index::default();
            if let Some(reference) = parent {
                let bytes = self.load_catalog_shard(reference).await?;
                let base = decode_pack_shard(&bytes, reference.prefix, reference.entries)?;
                for (pack, entries) in base.packs {
                    if changed_packs.contains(&pack)
                        || digest_prefix(pack.as_digest(), shard_bits)? != prefix
                    {
                        continue;
                    }
                    let pack_len = base.pack_lengths.get(&pack).copied().unwrap_or_default();
                    merged.add_pack_metadata(pack, pack_len, entries);
                    if let Some(dead) = base.tombstoned.get(&pack) {
                        merged.tombstoned.insert(pack, dead.clone());
                    }
                    if let Some(records) = base.tombstone_records.get(&pack) {
                        merged.tombstone_records.insert(pack, records.clone());
                    }
                }
                merged
                    .superseded
                    .extend(base.superseded.into_iter().filter(|pack| {
                        !changed_packs.contains(pack)
                            && digest_prefix(pack.as_digest(), shard_bits)
                                .is_ok_and(|known| known == prefix)
                    }));
            }
            while next_pack.as_ref().is_some_and(|(pack, _)| {
                digest_prefix(pack.as_digest(), shard_bits).is_ok_and(|known| known == prefix)
            }) {
                let (pack, single) = next_pack.take().expect("matched streamed pack");
                let entries = single.packs.get(&pack).cloned().ok_or_else(|| {
                    io::Error::other("streamed pack record contains the wrong pack")
                })?;
                merged.add_pack_metadata(
                    pack,
                    single.pack_lengths.get(&pack).copied().unwrap_or_default(),
                    entries,
                );
                if let Some(dead) = single.tombstoned.get(&pack) {
                    merged.tombstoned.insert(pack, dead.clone());
                }
                if let Some(records) = single.tombstone_records.get(&pack) {
                    merged.tombstone_records.insert(pack, records.clone());
                }
                next_pack = overlay.next_pack()?;
            }
            while next_superseded.as_ref().is_some_and(|pack| {
                digest_prefix(pack.as_digest(), shard_bits).is_ok_and(|known| known == prefix)
            }) {
                merged
                    .superseded
                    .insert(next_superseded.take().expect("matched superseded pack"));
                next_superseded = overlay.next_superseded()?;
            }
            if !merged.packs.is_empty() || !merged.superseded.is_empty() {
                let (reference, bytes) = encode_pack_shard_object(shard_bits, prefix, &merged)?;
                publication.put(reference.digest, bytes).await?;
                pack_refs.push(reference);
            }
        }
        if next_pack.is_some() || next_removed_pack.is_some() || next_superseded.is_some() {
            return Err(io::Error::other("streamed pack lies outside shard width"));
        }

        let map = Arc::new(ShardMap {
            shard_bits,
            chunks: chunk_refs,
            manifests: manifest_refs,
            packs: pack_refs,
            run_routing: BTreeMap::new(),
        });
        let map_bytes = encode_shard_map(&map)?;
        let map_digest = Digest::from(blake3::hash(&map_bytes));
        publication.put(map_digest, map_bytes).await?;
        publication.finish().await?;
        Ok(ShardedIndexBase { map })
    }

    async fn rebase_catalog_shards(
        &self,
        candidate: &Index,
        lazy: &LazyCatalogOverlay,
    ) -> io::Result<ShardedIndexBase> {
        // Write the persistent cleanup hint before any new immutable object. A
        // process crash at any later point therefore leaves startup maintenance
        // enough information to collect both superseded and never-published
        // shards.
        self.mark_catalog_reclaim_due().await?;
        let mut publication = self.catalog_object_publication();
        let Some(old) = &lazy.base else {
            let shard_bits =
                recommended_shard_bits(candidate.chunks.len() as u64, DEFAULT_SHARD_TARGET_BYTES)?;
            let encoded = encode_index_shards(candidate, shard_bits)?;
            for (digest, bytes) in encoded.objects {
                publication.put(digest, bytes).await?;
            }
            let map = Arc::new(decode_shard_map(&encoded.map)?);
            publication.put(encoded.map_digest, encoded.map).await?;
            publication.finish().await?;
            return Ok(ShardedIndexBase { map });
        };

        let base_entries = old
            .map
            .chunks
            .iter()
            .map(|shard| shard.entries)
            .sum::<u64>();
        let added_entries = lazy
            .changed_packs
            .iter()
            .filter_map(|pack| candidate.packs.get(pack))
            .map(|entries| entries.len() as u64)
            .sum::<u64>();
        let shard_bits = old.map.shard_bits.max(recommended_shard_bits(
            base_entries.saturating_add(added_entries),
            DEFAULT_SHARD_TARGET_BYTES,
        )?);
        let expansion = shard_bits - old.map.shard_bits;

        let expanded = |refs: &[ShardRef]| {
            let mut prefixes = BTreeSet::new();
            for reference in refs {
                let first = reference.prefix << expansion;
                for suffix in 0..(1_u32 << expansion) {
                    prefixes.insert(first | suffix);
                }
            }
            prefixes
        };
        let parent_ref = |refs: &[ShardRef], prefix: u32| {
            let parent = prefix >> expansion;
            refs.binary_search_by_key(&parent, |reference| reference.prefix)
                .ok()
                .map(|at| refs[at])
        };

        let mut overlay_chunks: BTreeMap<u32, Vec<IndexedLocation>> = BTreeMap::new();
        for pack in &lazy.changed_packs {
            if candidate.superseded.contains(pack) {
                continue;
            }
            let Some(entries) = candidate.packs.get(pack) else {
                continue;
            };
            let pack_len = candidate
                .pack_lengths
                .get(pack)
                .copied()
                .unwrap_or_default();
            let dead = candidate.tombstoned.get(pack);
            for entry in entries {
                if dead.is_some_and(|dead| dead.contains(&entry.digest)) {
                    continue;
                }
                overlay_chunks
                    .entry(digest_prefix(entry.digest.as_digest(), shard_bits)?)
                    .or_default()
                    .push(IndexedLocation {
                        digest: entry.digest,
                        location: Location {
                            pack: *pack,
                            pack_len,
                            offset: entry.offset,
                            framed_len: entry.framed_len,
                            uncompressed_len: entry.uncompressed_len,
                        },
                    });
            }
        }
        let mut chunk_prefixes = expanded(&old.map.chunks);
        chunk_prefixes.extend(overlay_chunks.keys().copied());
        let mut chunk_refs = Vec::new();
        for prefix in chunk_prefixes {
            let mut entries = if let Some(reference) = parent_ref(&old.map.chunks, prefix) {
                let bytes = self.load_catalog_shard(reference).await?;
                decode_chunk_shard(&bytes, reference.prefix)?
                    .into_iter()
                    .filter(|entry| {
                        !lazy.changed_packs.contains(&entry.location.pack)
                            && digest_prefix(entry.digest.as_digest(), shard_bits)
                                .is_ok_and(|known| known == prefix)
                    })
                    .collect()
            } else {
                Vec::new()
            };
            entries.extend(overlay_chunks.remove(&prefix).unwrap_or_default());
            if entries.is_empty() {
                continue;
            }
            let (reference, bytes) = encode_chunk_shard_object(shard_bits, prefix, &mut entries)?;
            publication.put(reference.digest, bytes).await?;
            chunk_refs.push(reference);
        }

        let local_manifests = candidate.manifests.sorted_ids();
        let mut overlay_manifests: BTreeMap<u32, Vec<BlobId>> = BTreeMap::new();
        for manifest in &local_manifests {
            overlay_manifests
                .entry(digest_prefix(manifest.as_digest(), shard_bits)?)
                .or_default()
                .push(*manifest);
        }
        let mut manifest_prefixes = expanded(&old.map.manifests);
        manifest_prefixes.extend(overlay_manifests.keys().copied());
        let local_manifest_set = local_manifests.iter().copied().collect::<HashSet<_>>();
        let mut manifest_refs = Vec::new();
        for prefix in manifest_prefixes {
            let mut manifests = if let Some(reference) = parent_ref(&old.map.manifests, prefix) {
                let bytes = self.load_catalog_shard(reference).await?;
                list_manifest_shard(&bytes, reference.prefix)?
                    .into_iter()
                    .filter(|manifest| {
                        !lazy.removed_manifests.contains(manifest)
                            && !local_manifest_set.contains(manifest)
                            && digest_prefix(manifest.as_digest(), shard_bits)
                                .is_ok_and(|known| known == prefix)
                    })
                    .collect()
            } else {
                Vec::new()
            };
            manifests.extend(overlay_manifests.remove(&prefix).unwrap_or_default());
            manifests.sort_unstable();
            manifests.dedup();
            if manifests.is_empty() {
                continue;
            }
            let (reference, bytes) = encode_manifest_shard_object(shard_bits, prefix, &manifests)?;
            publication.put(reference.digest, bytes).await?;
            manifest_refs.push(reference);
        }

        let mut overlay_packs: BTreeMap<u32, Vec<PackId>> = BTreeMap::new();
        for pack in &lazy.changed_packs {
            if candidate.packs.contains_key(pack) || candidate.superseded.contains(pack) {
                overlay_packs
                    .entry(digest_prefix(pack.as_digest(), shard_bits)?)
                    .or_default()
                    .push(*pack);
            }
        }
        let mut pack_prefixes = expanded(&old.map.packs);
        pack_prefixes.extend(overlay_packs.keys().copied());
        let mut pack_refs = Vec::new();
        for prefix in pack_prefixes {
            let mut merged = Index::default();
            if let Some(reference) = parent_ref(&old.map.packs, prefix) {
                let bytes = self.load_catalog_shard(reference).await?;
                let base = decode_pack_shard(&bytes, reference.prefix, reference.entries)?;
                for (pack, entries) in base.packs {
                    if lazy.changed_packs.contains(&pack)
                        || digest_prefix(pack.as_digest(), shard_bits)? != prefix
                    {
                        continue;
                    }
                    let pack_len = base.pack_lengths.get(&pack).copied().unwrap_or_default();
                    merged.add_pack_metadata(pack, pack_len, entries);
                    if let Some(dead) = base.tombstoned.get(&pack) {
                        merged.tombstoned.insert(pack, dead.clone());
                    }
                    if let Some(records) = base.tombstone_records.get(&pack) {
                        merged.tombstone_records.insert(pack, records.clone());
                    }
                }
                merged
                    .superseded
                    .extend(base.superseded.into_iter().filter(|pack| {
                        !lazy.changed_packs.contains(pack)
                            && digest_prefix(pack.as_digest(), shard_bits)
                                .is_ok_and(|known| known == prefix)
                    }));
            }
            for pack in overlay_packs.remove(&prefix).unwrap_or_default() {
                if let Some(entries) = candidate.packs.get(&pack) {
                    merged.add_pack_metadata(
                        pack,
                        candidate
                            .pack_lengths
                            .get(&pack)
                            .copied()
                            .unwrap_or_default(),
                        entries.clone(),
                    );
                    if let Some(dead) = candidate.tombstoned.get(&pack) {
                        merged.tombstoned.insert(pack, dead.clone());
                    }
                    if let Some(records) = candidate.tombstone_records.get(&pack) {
                        merged.tombstone_records.insert(pack, records.clone());
                    }
                }
                if candidate.superseded.contains(&pack) {
                    merged.superseded.insert(pack);
                }
            }
            if merged.packs.is_empty() && merged.superseded.is_empty() {
                continue;
            }
            let (reference, bytes) = encode_pack_shard_object(shard_bits, prefix, &merged)?;
            publication.put(reference.digest, bytes).await?;
            pack_refs.push(reference);
        }

        let map = Arc::new(ShardMap {
            shard_bits,
            chunks: chunk_refs,
            manifests: manifest_refs,
            packs: pack_refs,
            run_routing: BTreeMap::new(),
        });
        let map_bytes = encode_shard_map(&map)?;
        let map_digest = Digest::from(blake3::hash(&map_bytes));
        publication.put(map_digest, map_bytes).await?;
        publication.finish().await?;
        Ok(ShardedIndexBase { map })
    }

    async fn externalize_catalog_run_routing(
        &self,
        base: &CatalogBase,
        refs: &mut BTreeMap<u8, CatalogRunRef>,
        current: Option<&ShardMap>,
    ) -> io::Result<(CatalogBase, Option<ShardedIndexBase>)> {
        let CatalogBase::Sharded { shard_bits, .. } = base else {
            return Ok((base.clone(), None));
        };
        let current =
            current.ok_or_else(|| io::Error::other("sharded catalog has no loaded map"))?;
        if current.shard_bits != *shard_bits {
            return Err(io::Error::other("sharded catalog map width changed"));
        }
        let mut map = current.clone();
        let previous_routing = std::mem::take(&mut map.run_routing);
        for reference in refs.values_mut() {
            let Some(query) = reference.query.as_mut() else {
                continue;
            };
            let routing = if query.routing.is_empty() {
                previous_routing
                    .get(&reference.digest)
                    .cloned()
                    .ok_or_else(|| io::Error::other("catalog run has no external routing"))?
            } else {
                query.routing.clone()
            };
            let mut authenticated = query.clone();
            authenticated.routing = routing.clone();
            decode_catalog_run_routing(&authenticated)?;
            map.run_routing.insert(reference.digest, routing);
            query.routing = Bytes::new();
        }
        let encoded = encode_shard_map(&map)?;
        let digest = Digest::from(blake3::hash(&encoded));
        self.put_catalog_object(digest, encoded).await?;
        let installed = ShardedIndexBase { map: Arc::new(map) };
        Ok((
            CatalogBase::Sharded {
                root: digest,
                shard_bits: *shard_bits,
            },
            Some(installed),
        ))
    }

    async fn build_index_catalog(
        &self,
        candidate: &Index,
        delta: Option<&[u8]>,
        force: bool,
        synchronous_rebase: bool,
        witness: &IndexCatalogWitness,
        lazy: &LazyCatalogOverlay,
    ) -> Result<(IndexCatalogWitness, Bytes), object_store::Error> {
        if force && lazy.base.is_some() {
            return Err(object_store::Error::Generic {
                store: "pack index catalog",
                source: "cannot checkpoint a partially materialized sharded catalog".into(),
            });
        }
        // A publication without index mutations, such as one that only updates
        // sidecars, would otherwise fall through to the checkpoint branch below
        // and rebuild the base from `candidate`. After a lazy open of a sharded
        // base, `candidate` holds only the entries this process materialized,
        // so that checkpoint would silently drop every other chunk, pack and
        // manifest. Publish an empty delta over the unchanged base instead.
        let empty_delta;
        let delta = match delta {
            None if !force && lazy.base.is_some() && witness.root.is_some() => {
                empty_delta = encode_index_mutations(candidate, &IndexMutations::default())
                    .map_err(|error| object_store::Error::Generic {
                        store: "pack index catalog",
                        source: Box::new(error),
                    })?;
                Some(&empty_delta[..])
            }
            delta => delta,
        };
        let generation =
            witness
                .generation
                .checked_add(1)
                .ok_or_else(|| object_store::Error::Generic {
                    store: "pack index catalog",
                    source: "pack index catalog generation overflow".into(),
                })?;
        let (root, loaded_runs, prepared_rebase, prepared_map) = if synchronous_rebase
            && !force
            && let (Some(previous), Some(delta)) = (&witness.root, delta)
            && self.catalog_rebase_due(previous, delta)
        {
            let rebased = self
                .rebase_catalog_shards(candidate, lazy)
                .await
                .map_err(|error| object_store::Error::Generic {
                    store: "pack index catalog",
                    source: Box::new(error),
                })?;
            let map =
                encode_shard_map(&rebased.map).map_err(|error| object_store::Error::Generic {
                    store: "pack index catalog",
                    source: Box::new(error),
                })?;
            let root_digest = Digest::from(blake3::hash(&map));
            (
                DeltaCatalog {
                    sidecars: None,
                    generation,
                    base: CatalogBase::Sharded {
                        root: root_digest,
                        shard_bits: rebased.map.shard_bits,
                    },
                    runs: BTreeMap::new(),
                    deltas: Vec::new(),
                },
                BTreeMap::new(),
                Some(rebased),
                None,
            )
        } else if !force
            && let (Some(previous), Some(delta)) = (&witness.root, delta)
            && !delta_catalog_needs_compaction(previous, delta)
        {
            let mut root = previous.clone();
            root.generation = generation;
            root.deltas.push(Bytes::copy_from_slice(delta));
            (root, witness.runs.clone(), None, None)
        } else if !force && let (Some(previous), Some(delta)) = (&witness.root, delta) {
            let first_generation = generation
                .checked_sub(previous.deltas.len() as u64)
                .ok_or_else(|| object_store::Error::Generic {
                    store: "pack index catalog",
                    source: "catalog delta generations underflow".into(),
                })?;
            let batch = previous
                .deltas
                .iter()
                .map(Bytes::clone)
                .chain(std::iter::once(Bytes::copy_from_slice(delta)))
                .enumerate()
                .map(|(ordinal, delta)| CatalogRun {
                    first_generation: first_generation + ordinal as u64,
                    last_generation: first_generation + ordinal as u64,
                    delta,
                })
                .collect::<Vec<_>>();
            let incoming =
                merge_catalog_runs(&batch).map_err(|error| object_store::Error::Generic {
                    store: "pack index catalog",
                    source: Box::new(error),
                })?;
            let target_level = (0..run::MAX_CATALOG_RUN_LEVELS)
                .find(|level| !previous.runs.contains_key(level))
                .ok_or_else(|| object_store::Error::Generic {
                    store: "pack index catalog",
                    source: "catalog run levels are exhausted".into(),
                })?;
            let encoded =
                encode_catalog_run(&incoming).map_err(|error| object_store::Error::Generic {
                    store: "pack index catalog",
                    source: Box::new(error),
                })?;
            let incoming_reference = CatalogRunRef {
                digest: Digest::from(blake3::hash(&encoded)),
                first_generation: incoming.first_generation,
                last_generation: incoming.last_generation,
                encoded_bytes: encoded.len() as u64,
                query: Some(catalog_run_query_ref(&encoded).map_err(|error| {
                    object_store::Error::Generic {
                        store: "pack index catalog",
                        source: Box::new(error),
                    }
                })?),
            };

            // Sharded roots externalize routing into their map. A lazily
            // opened catalog holds that map as its base. A checkpoint that
            // outgrew the inline limit publishes a sharded root while this
            // process keeps its index materialized, so the map is read back
            // instead: installing it as a lazy base would claim the
            // materialized index is only an overlay.
            let loaded_map = match (&previous.base, &lazy.base) {
                (CatalogBase::Sharded { root, .. }, None) => {
                    Some(self.load_catalog_map(*root).await.map_err(|error| {
                        object_store::Error::Generic {
                            store: "pack index catalog",
                            source: Box::new(error),
                        }
                    })?)
                }
                _ => None,
            };
            let current_map = lazy
                .base
                .as_ref()
                .map(|base| &*base.map)
                .or(loaded_map.as_ref());

            let mut staged = Vec::with_capacity(usize::from(target_level) + 1);
            for level in 0..target_level {
                let reference =
                    previous
                        .runs
                        .get(&level)
                        .ok_or_else(|| object_store::Error::Generic {
                            store: "pack index catalog",
                            source: "catalog run carry has a missing lower level".into(),
                        })?;
                // The lazy references are the authenticated, hydrated copies.
                let mut reference = lazy
                    .run_refs
                    .values()
                    .find(|hydrated| hydrated.digest == reference.digest)
                    .unwrap_or(reference)
                    .clone();
                if let Some(query) = reference.query.as_mut()
                    && query.routing.is_empty()
                    && let Some(map) = current_map
                {
                    query.routing =
                        map.run_routing
                            .get(&reference.digest)
                            .cloned()
                            .ok_or_else(|| object_store::Error::Generic {
                                store: "pack index catalog",
                                source: "sharded catalog map is missing run routing".into(),
                            })?;
                }
                staged.push(self.stage_catalog_run(reference).await.map_err(|error| {
                    object_store::Error::Generic {
                        store: "pack index catalog",
                        source: Box::new(error),
                    }
                })?);
            }
            staged.push(stage_bytes(&encoded, incoming_reference).map_err(|error| {
                object_store::Error::Generic {
                    store: "pack index catalog",
                    source: Box::new(error),
                }
            })?);
            staged.sort_unstable_by_key(|run| run.reference.first_generation);
            let prepared = tokio::task::spawn_blocking(move || merge_staged_runs(&staged))
                .await
                .map_err(|error| object_store::Error::Generic {
                    store: "pack index catalog",
                    source: Box::new(error),
                })?
                .map_err(|error| object_store::Error::Generic {
                    store: "pack index catalog",
                    source: Box::new(error),
                })?;
            self.mark_catalog_reclaim_due().await.map_err(|error| {
                object_store::Error::Generic {
                    store: "pack index catalog",
                    source: Box::new(error),
                }
            })?;
            let merged_reference =
                self.put_prepared_catalog_run(prepared)
                    .await
                    .map_err(|error| object_store::Error::Generic {
                        store: "pack index catalog",
                        source: Box::new(error),
                    })?;
            let mut refs = previous.runs.clone();
            for level in 0..=target_level {
                refs.remove(&level);
            }
            refs.insert(target_level, merged_reference);
            let (base, prepared_map) = self
                .externalize_catalog_run_routing(&previous.base, &mut refs, current_map)
                .await
                .map_err(|error| object_store::Error::Generic {
                    store: "pack index catalog",
                    source: Box::new(error),
                })?;
            (
                DeltaCatalog {
                    sidecars: None,
                    generation,
                    base,
                    runs: refs,
                    deltas: Vec::new(),
                },
                witness
                    .runs
                    .iter()
                    .filter(|(level, _)| **level > target_level)
                    .map(|(level, run)| (*level, run.clone()))
                    .collect(),
                None,
                prepared_map,
            )
        } else {
            // `candidate` is the complete index only without a lazy base.
            if lazy.base.is_some() {
                return Err(object_store::Error::Generic {
                    store: "pack index catalog",
                    source: "cannot checkpoint a partially materialized sharded catalog".into(),
                });
            }
            let checkpoint = encode_index_checkpoint(
                candidate,
                Digest::from(blake3::hash(b"casita authoritative pack index v1\0")),
            )
            .map_err(|error| object_store::Error::Generic {
                store: "pack index catalog",
                source: Box::new(error),
            })?;
            let base = if checkpoint.len() <= INDEX_INLINE_BASE_MAX_BYTES {
                CatalogBase::Inline(checkpoint)
            } else {
                drop(checkpoint);
                let shard_bits = recommended_shard_bits(
                    candidate.chunks.len() as u64,
                    DEFAULT_SHARD_TARGET_BYTES,
                )
                .map_err(|error| object_store::Error::Generic {
                    store: "pack index catalog",
                    source: Box::new(error),
                })?;
                let sharded = encode_index_shards(candidate, shard_bits).map_err(|error| {
                    object_store::Error::Generic {
                        store: "pack index catalog",
                        source: Box::new(error),
                    }
                })?;
                self.mark_catalog_reclaim_due().await.map_err(|error| {
                    object_store::Error::Generic {
                        store: "pack index catalog",
                        source: Box::new(error),
                    }
                })?;
                let mut publication = self.catalog_object_publication();
                for (digest, bytes) in &sharded.objects {
                    publication
                        .put(*digest, bytes.clone())
                        .await
                        .map_err(|error| object_store::Error::Generic {
                            store: "pack index catalog",
                            source: Box::new(error),
                        })?;
                }
                publication
                    .put(sharded.map_digest, sharded.map)
                    .await
                    .map_err(|error| object_store::Error::Generic {
                        store: "pack index catalog",
                        source: Box::new(error),
                    })?;
                publication
                    .finish()
                    .await
                    .map_err(|error| object_store::Error::Generic {
                        store: "pack index catalog",
                        source: Box::new(error),
                    })?;
                CatalogBase::Sharded {
                    root: sharded.map_digest,
                    shard_bits,
                }
            };
            (
                DeltaCatalog {
                    sidecars: None,
                    generation,
                    base,
                    runs: Default::default(),
                    deltas: Vec::new(),
                },
                BTreeMap::new(),
                None,
                None,
            )
        };
        let catalog =
            encode_delta_catalog(&root).map_err(|error| object_store::Error::Generic {
                store: "pack index catalog",
                source: Box::new(error),
            })?;
        Ok((
            IndexCatalogWitness {
                version: witness.version.clone(),
                external: None,
                pointer_digest: witness.pointer_digest,
                generation,
                root: Some(root),
                runs: loaded_runs,
                prepared_rebase,
                prepared_map,
            },
            catalog,
        ))
    }

    async fn put_replacement_marker(
        &self,
        old: PackId,
        new: Option<PackId>,
    ) -> io::Result<ReplacementMarker> {
        let marker = encode_replacement(old, new);
        let marker_id = Digest::from(blake3::hash(&marker));
        let path = sharded_path(&self.base, REPLACEMENTS_KIND, &marker_id);
        self.read_counters
            .gc_marker_put_requests
            .fetch_add(1, Ordering::Relaxed);
        self.read_counters
            .gc_marker_put_bytes
            .fetch_add(marker.len() as u64, Ordering::Relaxed);
        if let Some(local) = &self.local_durability {
            local.put(&path, marker.clone()).await?;
        } else {
            put_object(&self.object_store, &path, marker.clone(), true)
                .await
                .map_err(io::Error::other)?;
        }
        Ok(ReplacementMarker(marker))
    }

    #[tracing::instrument(name = "blob.pack.compact", skip_all)]
    async fn compact_pack(
        &self,
        old: PackId,
        deleted: &HashSet<ChunkId>,
        force_reclaim: bool,
        retirement: PayloadRetirement<'_>,
    ) -> io::Result<PackCompaction> {
        use crate::repository::CollectionPhase;

        let _total = CollectionPhase::new("compact_pack");
        let load = CollectionPhase::new("compact_pack_load");
        self.ensure_pack_loaded(old).await?;
        drop(load);
        let classify = CollectionPhase::new("compact_pack_classify");
        let (entries, old_len) = {
            let index = self.index.read().unwrap();
            (
                index.packs.get(&old).cloned().unwrap_or_default(),
                index.pack_lengths.get(&old).copied().unwrap_or_default(),
            )
        };
        if entries.is_empty() {
            return Ok(PackCompaction::Complete);
        }
        let mut dead = self
            .index
            .read()
            .unwrap()
            .tombstoned
            .get(&old)
            .cloned()
            .unwrap_or_default();
        dead.extend(deleted.iter().copied());
        let live_entries = entries
            .iter()
            .copied()
            .filter(|entry| !dead.contains(&entry.digest))
            .collect::<Vec<_>>();
        let dead_bytes = entries
            .iter()
            .filter(|entry| dead.contains(&entry.digest))
            .map(|entry| entry.framed_len as u128)
            .sum::<u128>();
        let body_bytes = entries
            .iter()
            .map(|entry| entry.framed_len as u128)
            .sum::<u128>();

        if !force_reclaim
            && !live_entries.is_empty()
            && dead_bytes * 100 < body_bytes * u128::from(DEFAULT_PACK_COMPACTION_DEAD_PERCENT)
        {
            return Ok(PackCompaction::Deferred(DeferredTombstone {
                tombstone: make_tombstone(old, &entries, &dead),
                dead,
            }));
        }
        drop(classify);
        let copy = CollectionPhase::new("compact_pack_copy");
        let mut survivors = Batch::default();
        let old_bytes = if live_entries.is_empty() {
            None
        } else {
            self.read_counters
                .whole_pack_requests
                .fetch_add(1, Ordering::Relaxed);
            let bytes = self
                .object_store
                .get(&pack_path(&self.base, &old))
                .await
                .map_err(io::Error::other)?
                .bytes()
                .await
                .map_err(io::Error::other)?;
            self.read_counters
                .whole_pack_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            Some(bytes)
        };
        if let Some(bytes) = &old_bytes {
            if bytes.len() as u64 != old_len {
                return Err(io::Error::other("pack length changed during compaction"));
            }
            if PackId::new(blake3::hash(bytes).into()) != old {
                return Err(io::Error::other("pack hash mismatch during compaction"));
            }
        }
        for entry in live_entries {
            let pack = old_bytes.as_ref().expect("live entries loaded their pack");
            let start = usize::try_from(entry.offset)
                .map_err(|_| io::Error::other("pack entry offset overflow"))?;
            let len = usize::try_from(entry.framed_len)
                .map_err(|_| io::Error::other("pack entry length overflow"))?;
            let end = start
                .checked_add(len)
                .ok_or_else(|| io::Error::other("pack entry range overflow"))?;
            if end > pack.len() {
                return Err(io::Error::other("pack entry lies outside its object"));
            }
            survivors.push(
                ChunkMeta {
                    digest: entry.digest,
                    size: entry.uncompressed_len,
                },
                pack.slice(start..end),
            );
        }
        drop(copy);
        // A wholly dead pack is the emergency-space primitive: remove its
        // large object before creating the tiny tombstone marker. This order
        // is safe because a crash after deletion cannot resurrect an absent
        // content-addressed object, and it lets GC make progress on a truly
        // full local filesystem. Partially live packs remain copy-on-write.
        if survivors.is_empty() {
            let tombstone_records = self
                .index
                .read()
                .unwrap()
                .tombstone_records
                .get(&old)
                .cloned()
                .unwrap_or_default();
            self.read_counters
                .gc_pack_delete_requests
                .fetch_add(1, Ordering::Relaxed);
            let retire = CollectionPhase::new("compact_pack_retire");
            let retired = self
                .retire_collected_path(pack_path(&self.base, &old), retirement)
                .await?;
            drop(retire);
            let marker_write = CollectionPhase::new("compact_pack_marker_write");
            let marker = self.put_replacement_marker(old, None).await?;
            invariant::check(|| validate_replacement_reference(&marker, old, None))?;
            drop(marker_write);
            {
                let mut index = self.index.write().unwrap();
                index.remove_pack(old);
                self.record_pack_mutation(old);
                let mut pending = self.pending_catalog.lock().unwrap();
                pending.retirements.extend(retired);
                for record in index.unreferenced_tombstone_records(tombstone_records) {
                    pending
                        .retirements
                        .insert(sharded_path(&self.base, TOMBSTONES_KIND, &record));
                    self.read_counters
                        .gc_tombstone_delete_requests
                        .fetch_add(1, Ordering::Relaxed);
                }
                self.index_dirty.store(true, Ordering::Release);
            }
            return Ok(PackCompaction::Complete);
        }

        let replacement = {
            let seal_phase = CollectionPhase::new("compact_pack_seal");
            let sealed = seal(&survivors)?;
            drop(seal_phase);
            let _write = CollectionPhase::new("compact_pack_replacement_write");
            let pack_len = sealed.bytes.len() as u64;
            self.read_counters
                .gc_replacement_put_requests
                .fetch_add(1, Ordering::Relaxed);
            self.read_counters
                .gc_replacement_put_bytes
                .fetch_add(pack_len, Ordering::Relaxed);
            let path = pack_path(&self.base, &sealed.id);
            check_content_address(&path, &sealed.bytes)?;
            put_object(&self.object_store, &path, sealed.bytes, true)
                .await
                .map_err(io::Error::other)?;
            Some((sealed.id, pack_len, sealed.entries))
        };
        let marker_write = CollectionPhase::new("compact_pack_marker_write");
        let replacement_id = replacement.as_ref().map(|(id, _, _)| *id);
        let marker = self.put_replacement_marker(old, replacement_id).await?;
        invariant::check(|| validate_replacement_reference(&marker, old, replacement_id))?;

        drop(marker_write);
        {
            let mut index = self.index.write().unwrap();
            let tombstone_records = index
                .tombstone_records
                .get(&old)
                .cloned()
                .unwrap_or_default();
            index.remove_pack(old);
            let mut mutations = self.pending_catalog.lock().unwrap();
            mutations.record_pack(old);
            if let Some((id, pack_len, entries)) = replacement {
                index.add_pack(id, pack_len, entries);
                mutations.record_pack(id);
            }
            mutations.retirements.insert(pack_path(&self.base, &old));
            for record in index.unreferenced_tombstone_records(tombstone_records) {
                mutations
                    .retirements
                    .insert(sharded_path(&self.base, TOMBSTONES_KIND, &record));
                self.read_counters
                    .gc_tombstone_delete_requests
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.index_dirty.store(true, Ordering::Release);
            let mut lazy = self.lazy_catalog.write().unwrap();
            lazy.changed_packs.insert(old);
            if let Some(id) = replacement_id {
                lazy.changed_packs.insert(id);
            }
        }
        self.read_counters
            .gc_pack_delete_requests
            .fetch_add(1, Ordering::Relaxed);
        Ok(PackCompaction::Complete)
    }
}

fn index_publish_contended(error: &object_store::Error) -> bool {
    matches!(
        error,
        object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }
    )
}

fn object_store_io_error(error: object_store::Error) -> io::Error {
    if matches!(error, object_store::Error::NotFound { .. }) {
        io::Error::new(io::ErrorKind::NotFound, error)
    } else {
        io::Error::other(error)
    }
}

fn encode_index_checkpoint(index: &Index, inventory: Digest) -> io::Result<Bytes> {
    let mut packs = index.packs.iter().collect::<Vec<_>>();
    packs.sort_unstable_by_key(|(pack, _)| **pack);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&INDEX_CHECKPOINT_MAGIC_V1);
    bytes.extend_from_slice(inventory.as_bytes());
    bytes.extend_from_slice(&(packs.len() as u64).to_le_bytes());
    for (pack, entries) in packs {
        let pack_len = index
            .pack_lengths
            .get(pack)
            .copied()
            .ok_or_else(|| io::Error::other("indexed pack has no length"))?;
        bytes.extend_from_slice(pack.as_digest().as_bytes());
        bytes.extend_from_slice(&pack_len.to_le_bytes());
        let footer = encode_footer(entries);
        bytes.extend_from_slice(&(footer.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&footer);

        let mut bitmap = vec![0u8; entries.len().div_ceil(8)];
        if let Some(dead) = index.tombstoned.get(pack) {
            for (ordinal, entry) in entries.iter().enumerate() {
                if dead.contains(&entry.digest) {
                    bitmap[ordinal / 8] |= 1 << (ordinal % 8);
                }
            }
        }
        bytes.extend_from_slice(&(bitmap.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&bitmap);

        let mut records = index
            .tombstone_records
            .get(pack)
            .into_iter()
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        records.sort_unstable();
        bytes.extend_from_slice(&(records.len() as u64).to_le_bytes());
        for record in records {
            bytes.extend_from_slice(record.as_bytes());
        }
    }
    let mut superseded = index.superseded.iter().copied().collect::<Vec<_>>();
    superseded.sort_unstable();
    bytes.extend_from_slice(&(superseded.len() as u64).to_le_bytes());
    for pack in superseded {
        bytes.extend_from_slice(pack.as_digest().as_bytes());
    }
    bytes.push(u8::from(index.manifests_complete));
    let manifests = index.manifests.sorted_ids();
    bytes.extend_from_slice(&(manifests.len() as u64).to_le_bytes());
    for manifest in manifests {
        bytes.extend_from_slice(manifest.as_digest().as_bytes());
    }
    Ok(bytes.into())
}

fn take_index_bytes<'a>(bytes: &'a [u8], at: &mut usize, len: usize) -> io::Result<&'a [u8]> {
    let end = at
        .checked_add(len)
        .ok_or_else(|| io::Error::other("pack index offset overflow"))?;
    let value = bytes
        .get(*at..end)
        .ok_or_else(|| io::Error::other("truncated pack index"))?;
    *at = end;
    Ok(value)
}

fn take_index_u64(bytes: &[u8], at: &mut usize) -> io::Result<u64> {
    Ok(u64::from_le_bytes(
        take_index_bytes(bytes, at, 8)?
            .try_into()
            .expect("eight bytes"),
    ))
}

fn take_index_count(bytes: &[u8], at: &mut usize, element_min: usize) -> io::Result<usize> {
    let count = usize::try_from(take_index_u64(bytes, at)?)
        .map_err(|_| io::Error::other("pack index count overflow"))?;
    if count > bytes.len().saturating_sub(*at) / element_min {
        return Err(io::Error::other("invalid pack index count"));
    }
    Ok(count)
}

#[cfg(test)]
fn decode_index_checkpoint(bytes: &[u8], inventory: Digest) -> io::Result<Index> {
    decode_index_checkpoint_inner(bytes, Some(inventory))
}

fn decode_index_checkpoint_without_inventory(bytes: &[u8]) -> io::Result<Index> {
    decode_index_checkpoint_inner(bytes, None)
}

fn decode_index_checkpoint_inner(bytes: &[u8], inventory: Option<Digest>) -> io::Result<Index> {
    const HEADER_LEN: usize = 8 + DIGEST_LEN;
    if bytes.len() < HEADER_LEN || bytes.get(..8) != Some(INDEX_CHECKPOINT_MAGIC_V1.as_slice()) {
        return Err(io::Error::other("invalid pack index checkpoint"));
    }
    let encoded_inventory = Digest::try_from(&bytes[8..HEADER_LEN]).map_err(io::Error::other)?;
    if inventory.is_some_and(|inventory| encoded_inventory != inventory) {
        return Err(io::Error::other("stale pack index inventory"));
    }
    let mut at = HEADER_LEN;
    let pack_count = take_index_count(bytes, &mut at, DIGEST_LEN + 8 * 4)?;
    let mut index = Index::default();
    let mut previous_pack = None;
    for _ in 0..pack_count {
        let pack = PackId::new(
            Digest::try_from(take_index_bytes(bytes, &mut at, DIGEST_LEN)?)
                .map_err(io::Error::other)?,
        );
        if previous_pack.is_some_and(|previous| previous >= pack) {
            return Err(io::Error::other("pack index is not strictly sorted"));
        }
        previous_pack = Some(pack);
        let pack_len = take_index_u64(bytes, &mut at)?;
        let footer_len = usize::try_from(take_index_u64(bytes, &mut at)?)
            .map_err(|_| io::Error::other("pack index footer length overflow"))?;
        let footer = take_index_bytes(bytes, &mut at, footer_len)?;
        let body_len = pack_len
            .checked_sub(footer_len as u64)
            .and_then(|length| length.checked_sub(PACK_TRAILER_LEN as u64))
            .ok_or_else(|| io::Error::other("pack index footer exceeds pack length"))?;
        let entries = decode_footer(footer, body_len)?;
        if entries.is_empty() {
            return Err(io::Error::other("pack index contains an empty pack"));
        }
        let bitmap_len = usize::try_from(take_index_u64(bytes, &mut at)?)
            .map_err(|_| io::Error::other("pack index bitmap length overflow"))?;
        if bitmap_len != entries.len().div_ceil(8) {
            return Err(io::Error::other(
                "pack index tombstone bitmap length mismatch",
            ));
        }
        let bitmap = take_index_bytes(bytes, &mut at, bitmap_len)?;
        if !entries.len().is_multiple_of(8)
            && bitmap.last().is_some_and(|last| {
                let used = entries.len() % 8;
                last & !((1u8 << used) - 1) != 0
            })
        {
            return Err(io::Error::other("non-canonical pack index bitmap"));
        }
        let dead = entries
            .iter()
            .enumerate()
            .filter(|(ordinal, _)| bitmap[*ordinal / 8] & (1 << (*ordinal % 8)) != 0)
            .map(|(_, entry)| entry.digest)
            .collect::<HashSet<_>>();
        if !dead.is_empty() {
            index.tombstoned.insert(pack, dead);
        }
        let record_count = take_index_count(bytes, &mut at, DIGEST_LEN)?;
        let mut records = HashSet::with_capacity(record_count);
        let mut previous_record = None;
        for _ in 0..record_count {
            let record = Digest::try_from(take_index_bytes(bytes, &mut at, DIGEST_LEN)?)
                .map_err(io::Error::other)?;
            if previous_record.is_some_and(|previous| previous >= record) {
                return Err(io::Error::other(
                    "pack index tombstone records are not strictly sorted",
                ));
            }
            previous_record = Some(record);
            records.insert(record);
        }
        if !records.is_empty() {
            index.tombstone_records.insert(pack, records);
        }
        index.add_pack_metadata(pack, pack_len, entries);
    }
    let superseded_count = take_index_count(bytes, &mut at, DIGEST_LEN)?;
    let mut previous = None;
    for _ in 0..superseded_count {
        let pack = PackId::new(
            Digest::try_from(take_index_bytes(bytes, &mut at, DIGEST_LEN)?)
                .map_err(io::Error::other)?,
        );
        if previous.is_some_and(|previous| previous >= pack) || index.packs.contains_key(&pack) {
            return Err(io::Error::other(
                "invalid or unsorted superseded pack index",
            ));
        }
        previous = Some(pack);
        index.superseded.insert(pack);
    }
    index.manifests_complete = match take_index_bytes(bytes, &mut at, 1)?[0] {
        0 => false,
        1 => true,
        _ => {
            return Err(io::Error::other("invalid manifest completeness flag"));
        }
    };
    let manifest_count = take_index_count(bytes, &mut at, DIGEST_LEN)?;
    let mut manifests = Vec::with_capacity(manifest_count);
    let mut previous = None;
    for _ in 0..manifest_count {
        let manifest = BlobId::new(
            Digest::try_from(take_index_bytes(bytes, &mut at, DIGEST_LEN)?)
                .map_err(io::Error::other)?,
        );
        if previous.is_some_and(|previous| previous >= manifest) {
            return Err(io::Error::other("manifest catalog is not strictly sorted"));
        }
        previous = Some(manifest);
        manifests.push(manifest);
    }
    index.manifests = ManifestIndex::from_sorted(manifests);
    if at != bytes.len() {
        return Err(io::Error::other("trailing bytes in pack index checkpoint"));
    }
    index.rebuild_chunks();
    Ok(index)
}

async fn read_footer(
    object_store: &Arc<dyn ObjectStore>,
    path: &Path,
    pack_len: u64,
    counters: &PackReadCounters,
) -> io::Result<Vec<PackEntry>> {
    if pack_len < PACK_TRAILER_LEN as u64 {
        return Err(io::Error::other("truncated chunk pack"));
    }
    let trailer_start = pack_len - PACK_TRAILER_LEN as u64;
    counters
        .footer_range_requests
        .fetch_add(1, Ordering::Relaxed);
    let trailer = object_store
        .get_range(path, trailer_start..pack_len)
        .await
        .map_err(io::Error::other)?;
    counters
        .footer_range_bytes
        .fetch_add(trailer.len() as u64, Ordering::Relaxed);
    if trailer.len() != PACK_TRAILER_LEN {
        return Err(io::Error::other("truncated chunk pack trailer"));
    }
    let footer_len = decode_trailer(&trailer)?;
    let footer_end = trailer_start;
    let footer_start = footer_end
        .checked_sub(footer_len)
        .ok_or_else(|| io::Error::other("pack footer extends before the object"))?;
    counters
        .footer_range_requests
        .fetch_add(1, Ordering::Relaxed);
    let footer = object_store
        .get_range(path, footer_start..footer_end)
        .await
        .map_err(io::Error::other)?;
    counters
        .footer_range_bytes
        .fetch_add(footer.len() as u64, Ordering::Relaxed);
    decode_footer(&footer, footer_start)
}

fn encode_replacement(old: PackId, new: Option<PackId>) -> Bytes {
    let mut bytes = Vec::with_capacity(8 + DIGEST_LEN * 2 + 1);
    bytes.extend_from_slice(&REPLACEMENT_MAGIC);
    bytes.extend_from_slice(old.as_digest().as_bytes());
    bytes.push(u8::from(new.is_some()));
    if let Some(new) = new {
        bytes.extend_from_slice(new.as_digest().as_bytes());
    }
    bytes.into()
}

fn decode_replacement(bytes: &[u8]) -> io::Result<(PackId, Option<PackId>)> {
    let minimum = 8 + DIGEST_LEN + 1;
    if bytes.len() < minimum
        || (bytes[..8] != REPLACEMENT_MAGIC && bytes[..8] != LEGACY_REPLACEMENT_MAGIC)
    {
        return Err(io::Error::other("invalid pack replacement record"));
    }
    let old = PackId::new(Digest::try_from(&bytes[8..8 + DIGEST_LEN]).map_err(io::Error::other)?);
    let flag = bytes[8 + DIGEST_LEN];
    let new = match flag {
        0 if bytes.len() == minimum => None,
        1 if bytes.len() == minimum + DIGEST_LEN => Some(PackId::new(
            Digest::try_from(&bytes[minimum..]).map_err(io::Error::other)?,
        )),
        _ => return Err(io::Error::other("invalid pack replacement record length")),
    };
    Ok((old, new))
}

/// A pack replacement record whose write has completed: production code gets
/// one only from `PackedChunks::put_replacement_marker`.
struct ReplacementMarker(Bytes);

/// Removing `old` from the index, and installing `new` in its place, needs
/// the durable record that names exactly that replacement first: a catalog
/// rebuilt from storage treats `old` as superseded only through it, and
/// `reclaim_retired_packs` replays the deletion of `old` from it.
fn validate_replacement_reference(
    marker: &ReplacementMarker,
    old: PackId,
    new: Option<PackId>,
) -> Result<(), Violation> {
    const POINT: &str = "pack.replacement";
    let recorded = decode_replacement(&marker.0).map_err(|error| {
        Violation::new(
            POINT,
            format!("the replacement record does not decode: {error}"),
        )
    })?;
    invariant::ensure(POINT, recorded == (old, new), || {
        format!("the record names {recorded:?}, but the index replaces {old} with {new:?}")
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Tombstone {
    pack: PackId,
    entry_count: u64,
    bitmap: Bytes,
}

impl Tombstone {
    fn contains(&self, ordinal: usize) -> bool {
        self.bitmap[ordinal / 8] & (1 << (ordinal % 8)) != 0
    }
}

#[cfg(test)]
fn encode_tombstone(pack: PackId, entries: &[PackEntry], dead: &HashSet<ChunkId>) -> Bytes {
    let tombstone = make_tombstone(pack, entries, dead);
    let mut bytes = Vec::with_capacity(8 + DIGEST_LEN + 8 + tombstone.bitmap.len());
    bytes.extend_from_slice(&TOMBSTONE_MAGIC);
    append_tombstone(&mut bytes, &tombstone);
    bytes.into()
}

fn make_tombstone(pack: PackId, entries: &[PackEntry], dead: &HashSet<ChunkId>) -> Tombstone {
    let bitmap_len = entries.len().div_ceil(8);
    let mut bitmap = vec![0; bitmap_len];
    for (ordinal, entry) in entries.iter().enumerate() {
        if dead.contains(&entry.digest) {
            bitmap[ordinal / 8] |= 1 << (ordinal % 8);
        }
    }
    Tombstone {
        pack,
        entry_count: entries.len() as u64,
        bitmap: bitmap.into(),
    }
}

fn append_tombstone(bytes: &mut Vec<u8>, tombstone: &Tombstone) {
    bytes.extend_from_slice(tombstone.pack.as_digest().as_bytes());
    bytes.extend_from_slice(&tombstone.entry_count.to_le_bytes());
    bytes.extend_from_slice(&tombstone.bitmap);
}

fn encode_tombstone_delta(tombstones: &[Tombstone]) -> io::Result<Bytes> {
    if tombstones.is_empty() {
        return Err(io::Error::other(
            "refusing to encode an empty tombstone delta",
        ));
    }
    let mut tombstones = tombstones.to_vec();
    tombstones.sort_unstable_by_key(|tombstone| tombstone.pack);
    if tombstones
        .windows(2)
        .any(|pair| pair[0].pack == pair[1].pack)
    {
        return Err(io::Error::other("duplicate pack in tombstone delta"));
    }
    let payload_len = tombstones.iter().try_fold(0usize, |total, tombstone| {
        total.checked_add(DIGEST_LEN + 8 + tombstone.bitmap.len())
    });
    let capacity = 16usize
        .checked_add(payload_len.ok_or_else(|| io::Error::other("tombstone delta size overflow"))?)
        .ok_or_else(|| io::Error::other("tombstone delta size overflow"))?;
    let mut bytes = Vec::with_capacity(capacity);
    bytes.extend_from_slice(&TOMBSTONE_DELTA_MAGIC);
    bytes.extend_from_slice(&(tombstones.len() as u64).to_le_bytes());
    for tombstone in &tombstones {
        append_tombstone(&mut bytes, tombstone);
    }
    Ok(bytes.into())
}

fn decode_tombstone(bytes: &[u8]) -> io::Result<Tombstone> {
    const HEADER_LEN: usize = 8 + DIGEST_LEN + 8;
    if bytes.len() < HEADER_LEN || bytes[..8] != TOMBSTONE_MAGIC {
        return Err(io::Error::other("invalid pack tombstone record"));
    }
    let pack = PackId::new(Digest::try_from(&bytes[8..8 + DIGEST_LEN]).map_err(io::Error::other)?);
    let entry_count = u64::from_le_bytes(
        bytes[8 + DIGEST_LEN..HEADER_LEN]
            .try_into()
            .expect("eight bytes"),
    );
    let bitmap_len = entry_count
        .checked_add(7)
        .ok_or_else(|| io::Error::other("pack tombstone entry count overflow"))?
        / 8;
    let bitmap_len = usize::try_from(bitmap_len)
        .map_err(|_| io::Error::other("pack tombstone bitmap length overflow"))?;
    if bytes.len() != HEADER_LEN + bitmap_len {
        return Err(io::Error::other("invalid pack tombstone record length"));
    }
    validate_tombstone(pack, entry_count, &bytes[HEADER_LEN..])
}

fn validate_tombstone(pack: PackId, entry_count: u64, bitmap: &[u8]) -> io::Result<Tombstone> {
    if !entry_count.is_multiple_of(8) && !bitmap.is_empty() {
        let used = entry_count % 8;
        let unused_mask = !((1u8 << used) - 1);
        if bitmap[bitmap.len() - 1] & unused_mask != 0 {
            return Err(io::Error::other("non-canonical pack tombstone bitmap"));
        }
    }
    if bitmap.iter().all(|byte| *byte == 0) {
        return Err(io::Error::other("empty pack tombstone record"));
    }
    Ok(Tombstone {
        pack,
        entry_count,
        bitmap: Bytes::copy_from_slice(bitmap),
    })
}

fn decode_tombstone_record(bytes: &[u8]) -> io::Result<Vec<Tombstone>> {
    if bytes.starts_with(&TOMBSTONE_MAGIC) {
        return decode_tombstone(bytes).map(|tombstone| vec![tombstone]);
    }
    if bytes.len() < 16 || bytes[..8] != TOMBSTONE_DELTA_MAGIC {
        return Err(io::Error::other("invalid pack tombstone record"));
    }
    let count = u64::from_le_bytes(bytes[8..16].try_into().expect("eight bytes"));
    let count =
        usize::try_from(count).map_err(|_| io::Error::other("tombstone delta count overflow"))?;
    if count == 0 || count > bytes.len().saturating_sub(16) / (DIGEST_LEN + 8 + 1) {
        return Err(io::Error::other("invalid tombstone delta count"));
    }
    let mut at = 16usize;
    let mut tombstones = Vec::with_capacity(count);
    for _ in 0..count {
        let header_end = at
            .checked_add(DIGEST_LEN + 8)
            .ok_or_else(|| io::Error::other("tombstone delta offset overflow"))?;
        let header = bytes
            .get(at..header_end)
            .ok_or_else(|| io::Error::other("truncated tombstone delta"))?;
        let pack = PackId::new(Digest::try_from(&header[..DIGEST_LEN]).map_err(io::Error::other)?);
        let entry_count = u64::from_le_bytes(
            header[DIGEST_LEN..]
                .try_into()
                .expect("eight-byte entry count"),
        );
        let bitmap_len = entry_count
            .checked_add(7)
            .ok_or_else(|| io::Error::other("pack tombstone entry count overflow"))?
            / 8;
        let bitmap_len = usize::try_from(bitmap_len)
            .map_err(|_| io::Error::other("pack tombstone bitmap length overflow"))?;
        let bitmap_end = header_end
            .checked_add(bitmap_len)
            .ok_or_else(|| io::Error::other("tombstone delta offset overflow"))?;
        let bitmap = bytes
            .get(header_end..bitmap_end)
            .ok_or_else(|| io::Error::other("truncated tombstone delta bitmap"))?;
        if tombstones
            .last()
            .is_some_and(|previous: &Tombstone| previous.pack >= pack)
        {
            return Err(io::Error::other(
                "tombstone delta packs are not strictly sorted",
            ));
        }
        tombstones.push(validate_tombstone(pack, entry_count, bitmap)?);
        at = bitmap_end;
    }
    if at != bytes.len() {
        return Err(io::Error::other("trailing bytes in tombstone delta"));
    }
    Ok(tombstones)
}

#[cfg(test)]
#[path = "pack/benchmarks.rs"]
mod benchmarks;

#[path = "pack/external.rs"]
mod external;

#[cfg(test)]
mod fragmentation;

#[cfg(all(test, feature = "s3"))]
mod fragmentation_network;

#[cfg(all(test, feature = "s3"))]
mod planned_reads;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::{
        BlobChunkSource, BlobStore, BlobSync, ChunkedBlobStore, DEFAULT_AVG_CHUNK_SIZE,
    };
    use async_trait::async_trait;
    use futures::{StreamExt, TryStreamExt};
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutPayload, PutResult,
    };
    use std::fmt;

    #[derive(Debug)]
    struct FailingCatalogPutStore {
        inner: Arc<InMemory>,
        fail_at: AtomicU64,
        catalog_puts: AtomicU64,
        delete_fail_at: Arc<AtomicU64>,
        catalog_deletes: Arc<AtomicU64>,
        delete_resume: Arc<tokio::sync::Notify>,
        fail_pack_puts: AtomicBool,
    }

    impl FailingCatalogPutStore {
        fn new() -> Self {
            Self {
                inner: Arc::new(InMemory::new()),
                fail_at: AtomicU64::new(u64::MAX),
                catalog_puts: AtomicU64::new(0),
                delete_fail_at: Arc::new(AtomicU64::new(u64::MAX)),
                catalog_deletes: Arc::new(AtomicU64::new(0)),
                delete_resume: Arc::new(tokio::sync::Notify::new()),
                fail_pack_puts: AtomicBool::new(false),
            }
        }

        fn fail_catalog_put(&self, ordinal: u64) {
            assert!(ordinal > 0);
            self.catalog_puts.store(0, Ordering::SeqCst);
            self.fail_at.store(ordinal, Ordering::SeqCst);
        }

        fn pause_catalog_put(&self) {
            self.catalog_puts.store(0, Ordering::SeqCst);
            self.fail_at.store(0, Ordering::SeqCst);
        }

        fn disarm(&self) {
            self.fail_at.store(u64::MAX, Ordering::SeqCst);
            self.delete_fail_at.store(u64::MAX, Ordering::SeqCst);
        }

        fn fail_catalog_delete(&self, ordinal: u64) {
            assert!(ordinal > 0);
            self.catalog_deletes.store(0, Ordering::SeqCst);
            self.delete_fail_at.store(ordinal, Ordering::SeqCst);
        }
    }

    impl fmt::Display for FailingCatalogPutStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("FailingCatalogPutStore")
        }
    }

    #[async_trait]
    impl ObjectStore for FailingCatalogPutStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            if self.fail_pack_puts.load(Ordering::SeqCst)
                && location.parts().any(|part| part.as_ref() == PACKS_KIND)
            {
                return Err(object_store::Error::Generic {
                    store: "pack-put-test",
                    source: "injected pack PUT failure".into(),
                });
            }
            if location.as_ref().contains("/pack-indexes/") {
                let ordinal = self.catalog_puts.fetch_add(1, Ordering::SeqCst) + 1;
                if self.fail_at.load(Ordering::SeqCst) == 0 {
                    std::future::pending::<()>().await;
                }
                if ordinal == self.fail_at.load(Ordering::SeqCst) {
                    return Err(object_store::Error::Generic {
                        store: "catalog-rebase-test",
                        source: "injected catalog shard PUT failure".into(),
                    });
                }
            }
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            let inner = self.inner.clone();
            let delete_fail_at = self.delete_fail_at.clone();
            let catalog_deletes = self.catalog_deletes.clone();
            let delete_resume = self.delete_resume.clone();
            Box::pin(locations.then(move |location| {
                let inner = inner.clone();
                let delete_fail_at = delete_fail_at.clone();
                let catalog_deletes = catalog_deletes.clone();
                let delete_resume = delete_resume.clone();
                async move {
                    let location = location?;
                    let ordinal = catalog_deletes.fetch_add(1, Ordering::SeqCst) + 1;
                    if delete_fail_at.load(Ordering::SeqCst) == 0 {
                        delete_resume.notified().await;
                    }
                    if ordinal == delete_fail_at.load(Ordering::SeqCst) {
                        return Err(object_store::Error::Generic {
                            store: "catalog-reclamation-test",
                            source: "injected catalog object DELETE failure".into(),
                        });
                    }
                    inner.delete(&location).await?;
                    Ok(location)
                }
            }))
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn chunk(data: &[u8]) -> (ChunkMeta, Bytes) {
        let compressed = zstd::bulk::compress(data, 3).unwrap();
        (
            ChunkMeta {
                digest: ChunkId::new(blake3::hash(data).into()),
                size: data.len() as u64,
            },
            compressed.into(),
        )
    }

    #[tokio::test]
    async fn staged_chunk_stays_visible_across_flush_handoff() {
        let packed = PackedChunks::open_with_state_catalog(
            Arc::new(InMemory::new()),
            Path::from("flush-handoff"),
            u64::MAX,
            0,
            &PackedChunks::empty_state_catalog().unwrap(),
        )
        .await
        .unwrap();
        let (meta, bytes) = chunk(b"read while its batch changes hands");
        packed.put(meta.clone(), bytes.clone()).await.unwrap();
        let (reached_tx, reached) = tokio::sync::oneshot::channel();
        let (resume, resume_rx) = tokio::sync::oneshot::channel();
        *packed.flush_handoff_hook.lock().unwrap() = Some(FlushHandoffHook {
            reached: reached_tx,
            resume: resume_rx,
        });
        let flush = tokio::spawn({
            let packed = packed.clone();
            async move { packed.flush().await }
        });
        reached.await.unwrap();
        // The flush task is parked in the handoff and, on this single threaded
        // runtime, only advances once the reads below yield. They therefore
        // observe the handoff state itself, or wait for it to complete.
        resume.send(()).unwrap();
        let probed = packed.probe(&meta.digest).await.unwrap();
        let read = packed.get(&meta.digest).await.unwrap();
        flush.await.unwrap().unwrap();
        assert!(probed, "probe missed a chunk mid flush");
        assert_eq!(read, Some(bytes.clone()), "read missed a chunk mid flush");
        assert_eq!(packed.get(&meta.digest).await.unwrap(), Some(bytes));
    }

    #[tokio::test]
    async fn failed_pack_upload_requeues_batch_in_order() {
        let fault = Arc::new(FailingCatalogPutStore::new());
        let objects: Arc<dyn ObjectStore> = fault.clone();
        let writer = PackedChunks::open(objects, Path::from("failed-pack-upload"), u64::MAX)
            .await
            .unwrap();
        let chunks: Vec<_> = (0..3)
            .map(|index| chunk(format!("requeued chunk {index}").as_bytes()))
            .collect();
        for (meta, bytes) in &chunks {
            writer.put(meta.clone(), bytes.clone()).await.unwrap();
        }
        fault.fail_pack_puts.store(true, Ordering::SeqCst);
        let error = writer.flush().await.unwrap_err();
        assert!(error.to_string().contains("injected pack PUT failure"));
        {
            let staging = writer.staging.lock().await;
            let order: Vec<_> = staging.chunks.iter().map(|(meta, _)| meta.digest).collect();
            let expected: Vec<_> = chunks.iter().map(|(meta, _)| meta.digest).collect();
            assert_eq!(order, expected);
            assert_eq!(staging.digests.len(), chunks.len());
            let bytes: u64 = chunks.iter().map(|(_, bytes)| bytes.len() as u64).sum();
            assert_eq!(staging.bytes, bytes);
        }
        assert!(writer.inflight.lock().await.is_none());
        for (meta, bytes) in &chunks {
            assert_eq!(writer.get(&meta.digest).await.unwrap(), Some(bytes.clone()));
        }
        fault.fail_pack_puts.store(false, Ordering::SeqCst);
        writer.flush().await.unwrap();
        assert!(writer.staging.lock().await.is_empty());
        for (meta, bytes) in &chunks {
            assert_eq!(writer.get(&meta.digest).await.unwrap(), Some(bytes.clone()));
        }
    }

    #[tokio::test]
    async fn seal_rebuild_and_range_read_roundtrip() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("payloads");
        let store = PackedChunks::open(objects.clone(), base.clone(), 1024)
            .await
            .unwrap();
        let (a, a_bytes) = chunk(b"first chunk");
        let (b, b_bytes) = chunk(&vec![7; 50_000]);
        store.put(a.clone(), a_bytes.clone()).await.unwrap();
        store.put(b.clone(), b_bytes.clone()).await.unwrap();
        store.flush().await.unwrap();

        let reopened = PackedChunks::open(objects, base, 1024).await.unwrap();
        let open_stats = reopened.read_stats();
        assert_eq!(open_stats.list_requests, 0);
        assert_eq!(open_stats.index_hits, 1);
        assert_eq!(open_stats.index_pointer_requests, 1);
        assert_eq!(open_stats.index_requests, 0);
        assert_eq!(open_stats.footer_range_requests, 0);
        assert_eq!(open_stats.footer_range_bytes, 0);
        assert_eq!(reopened.get(&a.digest).await.unwrap(), Some(a_bytes));
        assert_eq!(reopened.get(&b.digest).await.unwrap(), Some(b_bytes));
        assert_eq!(reopened.metadata(&b.digest).await.unwrap(), Some(b.size));
    }

    #[tokio::test]
    async fn compaction_survives_reopen_without_resurrecting_deleted_chunks() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("payloads");
        let store = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let (a, a_bytes) = chunk(b"live");
        let (b, _) = chunk(b"dead");
        store.put(a.clone(), a_bytes.clone()).await.unwrap();
        store.put(b.clone(), chunk(b"dead").1).await.unwrap();
        store.flush().await.unwrap();
        store.reset_read_stats();
        store.delete_many(&[b.digest, b.digest]).await.unwrap();
        store.finish_deletions(true).await.unwrap();
        let stats = store.read_stats();
        assert_eq!(stats.chunk_range_requests, 0);
        assert_eq!(stats.whole_pack_requests, 1);
        assert_eq!(stats.gc_replacement_put_requests, 1);
        assert_eq!(stats.gc_marker_put_requests, 1);
        assert_eq!(stats.gc_pack_delete_requests, 1);

        let reopened = PackedChunks::open(objects, base, u64::MAX).await.unwrap();
        assert_eq!(reopened.get(&a.digest).await.unwrap(), Some(a_bytes));
        assert_eq!(reopened.get(&b.digest).await.unwrap(), None);
    }

    #[tokio::test]
    async fn local_marker_failure_keeps_old_pack_and_retries() {
        let directory = tempfile::tempdir().unwrap();
        let fs = object_store::local::LocalFileSystem::new_with_prefix(directory.path())
            .unwrap()
            .with_fsync(true);
        let durability = LocalDurability::new(fs.clone(), directory.path()).unwrap();
        let objects: Arc<dyn ObjectStore> = Arc::new(fs);
        let store = PackedChunks::open_with_initial_catalog(
            objects.clone(),
            Path::default(),
            u64::MAX,
            0,
            Some(&PackedChunks::empty_state_catalog().unwrap()),
            Some(durability),
        )
        .await
        .unwrap();
        let (meta, bytes) = chunk(b"failed local marker must not retire its pack");
        store.put(meta.clone(), bytes).await.unwrap();
        store.prepare_state_catalog().await.unwrap().unwrap();
        store.finish_state_catalog(true).unwrap();
        let old = store.location(&meta.digest).await.unwrap().unwrap().pack;
        let marker = encode_replacement(old, None);
        let path = sharded_path(
            &Path::default(),
            REPLACEMENTS_KIND,
            &Digest::from(blake3::hash(&marker)),
        );
        let obstacle = directory.path().join(path.as_ref());
        std::fs::create_dir_all(&obstacle).unwrap();
        store.delete_many(&[meta.digest]).await.unwrap();
        assert!(
            store
                .finish_deletions_inner(true, PayloadRetirement::Deferred)
                .await
                .is_err()
        );
        assert!(store.index.read().unwrap().packs.contains_key(&old));
        assert!(store.dirty_packs.lock().await.contains(&old));
        assert!(
            objects
                .head(&pack_path(&Path::default(), &old))
                .await
                .is_ok()
        );
        std::fs::remove_dir(&obstacle).unwrap();
        store
            .finish_deletions_inner(true, PayloadRetirement::Deferred)
            .await
            .unwrap();
        assert!(!store.index.read().unwrap().packs.contains_key(&old));
        assert!(!store.dirty_packs.lock().await.contains(&old));
        let actual = objects.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(actual, marker);
        assert!(
            objects
                .head(&pack_path(&Path::default(), &old))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn online_local_sweep_preserves_pinned_packs_and_reclaims_unrelated_data() {
        use crate::metadata::{DataPin, PinScope, PinStore};
        for before_prune in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let fs = object_store::local::LocalFileSystem::new_with_prefix(directory.path())
                .unwrap()
                .with_fsync(true);
            let durability = LocalDurability::new(fs.clone(), directory.path()).unwrap();
            let objects: Arc<dyn ObjectStore> = Arc::new(fs);
            let store = PackedChunks::open_with_initial_catalog(
                objects.clone(),
                Path::default(),
                u64::MAX,
                0,
                Some(&PackedChunks::empty_state_catalog().unwrap()),
                Some(durability),
            )
            .await
            .unwrap();
            let (held, held_bytes) = chunk(b"held historical local bytes");
            store.put(held.clone(), held_bytes).await.unwrap();
            let held_manifest = BlobId::new(Digest::hash(b"held manifest"));
            let garbage_manifest = BlobId::new(Digest::hash(b"garbage manifest"));
            let held_manifest_paths = ["blobs", "bao"]
                .map(|kind| sharded_path(&Path::default(), kind, held_manifest.as_digest()));
            let garbage_manifest_paths = ["blobs", "bao"]
                .map(|kind| sharded_path(&Path::default(), kind, garbage_manifest.as_digest()));
            for path in &held_manifest_paths {
                put_object(
                    &objects,
                    path,
                    Bytes::from_static(b"held representation"),
                    true,
                )
                .await
                .unwrap();
            }
            store.register_manifest(held_manifest);
            let held_catalog = store.prepare_state_catalog().await.unwrap().unwrap();
            store.finish_state_catalog(true).unwrap();
            let held_pack = store.location(&held.digest).await.unwrap().unwrap().pack;
            let held_path = pack_path(&Path::default(), &held_pack);
            let (garbage, garbage_bytes) = chunk(b"unrelated local bytes");
            store.put(garbage.clone(), garbage_bytes).await.unwrap();
            for path in &garbage_manifest_paths {
                put_object(
                    &objects,
                    path,
                    Bytes::from_static(b"unrelated representation"),
                    true,
                )
                .await
                .unwrap();
            }
            store.register_manifest(garbage_manifest);
            store.prepare_state_catalog().await.unwrap().unwrap();
            store.finish_state_catalog(true).unwrap();
            let garbage_pack = store.location(&garbage.digest).await.unwrap().unwrap().pack;
            let garbage_path = pack_path(&Path::default(), &garbage_pack);
            assert_ne!(held_pack, garbage_pack);
            let ledger = Arc::new(crate::metadata::MemoryPinStore::default());
            let pin = ledger
                .register(DataPin {
                    scope: PinScope::Snapshot {
                        generation: u64::MAX,
                    },
                    catalog: Some(held_catalog),
                    resources: BTreeSet::new(),
                })
                .await
                .unwrap()
                .unwrap();
            let collector = ledger
                .begin_collection(ledger.inventory().await.unwrap().revision, None)
                .await
                .unwrap()
                .unwrap();
            store
                .retire_manifests_pinned(
                    &[held_manifest, garbage_manifest],
                    ledger.clone(),
                    BTreeSet::new(),
                    before_prune,
                )
                .await
                .unwrap();
            for path in &held_manifest_paths {
                assert!(objects.head(path).await.is_ok());
            }
            for path in &garbage_manifest_paths {
                assert_eq!(objects.head(path).await.is_err(), before_prune);
            }
            store
                .delete_many(&[held.digest, garbage.digest])
                .await
                .unwrap();
            store
                .finish_deletions_pinned(true, ledger.clone(), BTreeSet::new(), before_prune)
                .await
                .unwrap();
            assert!(objects.head(&held_path).await.is_ok());
            assert_eq!(
                objects.head(&garbage_path).await.is_err(),
                before_prune,
                "only emergency GC may reclaim before catalog publication"
            );
            store.prepare_state_catalog().await.unwrap().unwrap();
            store.finish_state_catalog(true).unwrap();
            store
                .finish_collection_pinned(true, ledger.clone(), BTreeSet::new())
                .await
                .unwrap();
            assert!(objects.head(&held_path).await.is_ok());
            assert!(objects.head(&garbage_path).await.is_err());
            for path in &held_manifest_paths {
                assert!(objects.head(path).await.is_ok());
            }
            for path in &garbage_manifest_paths {
                assert!(objects.head(path).await.is_err());
            }
            ledger.release(&pin).await.unwrap();
            ledger.finish_collection(&collector).await.unwrap();
            let collector = ledger
                .begin_collection(ledger.inventory().await.unwrap().revision, None)
                .await
                .unwrap()
                .unwrap();
            store
                .finish_collection_pinned(true, ledger.clone(), BTreeSet::new())
                .await
                .unwrap();
            assert!(objects.head(&held_path).await.is_err());
            for path in &held_manifest_paths {
                assert!(objects.head(path).await.is_err());
            }
            ledger.finish_collection(&collector).await.unwrap();
        }
    }

    #[tokio::test]
    async fn local_emergency_deletion_does_not_deduplicate_against_missing_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let fs = object_store::local::LocalFileSystem::new_with_prefix(directory.path())
            .unwrap()
            .with_fsync(true);
        let durability = LocalDurability::new(fs.clone(), directory.path()).unwrap();
        let objects: Arc<dyn ObjectStore> = Arc::new(fs);
        let empty = PackedChunks::empty_state_catalog().unwrap();
        let store = PackedChunks::open_with_initial_catalog(
            objects.clone(),
            Path::default(),
            u64::MAX,
            0,
            Some(&empty),
            Some(durability.clone()),
        )
        .await
        .unwrap();
        let (payload, bytes) = chunk(b"unrooted bytes deleted during emergency collection");
        store.put(payload.clone(), bytes.clone()).await.unwrap();
        let before = store.prepare_state_catalog().await.unwrap().unwrap();
        store.finish_state_catalog(true).unwrap();
        store.delete_many(&[payload.digest]).await.unwrap();
        store.finish_deletions(true).await.unwrap();
        drop(store); // process dies before its metadata commit

        let reopened = PackedChunks::open_with_initial_catalog(
            objects,
            Path::default(),
            u64::MAX,
            0,
            Some(&before),
            Some(durability),
        )
        .await
        .unwrap();
        assert!(!reopened.probe(&payload.digest).await.unwrap());
        reopened.put(payload.clone(), bytes.clone()).await.unwrap();
        reopened.prepare_state_catalog().await.unwrap().unwrap();
        reopened.finish_state_catalog(true).unwrap();
        assert_eq!(reopened.get(&payload.digest).await.unwrap(), Some(bytes));
    }

    #[tokio::test]
    async fn owned_catalog_abort_restores_changes_and_owns_staging_pins() {
        use crate::metadata::{DataPin, DataPinLease, MemoryPinStore, PinScope, PinStore};
        for explicit_abort in [false, true] {
            let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let base = Path::from("owned-publication");
            let writer = PackedChunks::open_with_state_catalog(
                objects.clone(),
                base.clone(),
                u64::MAX,
                0,
                &PackedChunks::empty_state_catalog().unwrap(),
            )
            .await
            .unwrap();
            let ledger = Arc::new(MemoryPinStore::default());
            let pin = DataPinLease::try_acquire(
                ledger.clone(),
                DataPin {
                    scope: PinScope::Staging,
                    catalog: None,
                    resources: BTreeSet::new(),
                },
            )
            .await
            .unwrap()
            .unwrap();
            writer.attach_pin(&pin);
            let (first, first_bytes) = chunk(b"owned unpublished candidate");
            writer
                .put(first.clone(), first_bytes.clone())
                .await
                .unwrap();
            let prepared = writer.prepare_catalog().await.unwrap();
            assert!(prepared.catalog().is_some());
            assert!(writer.prepare_catalog().await.is_err());
            drop(pin);
            crate::metadata::flush_repository_leases().await.unwrap();
            assert!(!ledger.inventory().await.unwrap().pins.is_empty());
            let (second, second_bytes) = chunk(b"staged after preparation");
            writer
                .put(second.clone(), second_bytes.clone())
                .await
                .unwrap();
            if explicit_abort {
                prepared.abort().unwrap();
            } else {
                drop(prepared);
            }
            assert!(!writer.catalog_prepared.load(Ordering::Acquire));
            crate::metadata::flush_repository_leases().await.unwrap();
            assert!(ledger.inventory().await.unwrap().pins.is_empty());
            let retry = writer.prepare_catalog().await.unwrap();
            let catalog = retry.catalog().unwrap().to_vec();
            // The handle also keeps the physical backend alive through commit.
            drop(writer);
            retry.commit().unwrap();
            let reopened =
                PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &catalog)
                    .await
                    .unwrap();
            for (meta, bytes) in [(first, first_bytes), (second, second_bytes)] {
                assert_eq!(reopened.get(&meta.digest).await.unwrap(), Some(bytes));
            }
            assert!(
                reopened
                    .prepare_catalog()
                    .await
                    .unwrap()
                    .catalog()
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn published_retirements_progress_during_newer_publication_and_abort() {
        use crate::metadata::{DataPin, PinResource, PinScope, PinStore};

        for local in [false, true] {
            for prepared in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let (objects, durability): (Arc<dyn ObjectStore>, _) = if local {
                    let fs =
                        object_store::local::LocalFileSystem::new_with_prefix(directory.path())
                            .unwrap()
                            .with_fsync(true);
                    let durability = LocalDurability::new(fs.clone(), directory.path()).unwrap();
                    (Arc::new(fs), Some(durability))
                } else {
                    (Arc::new(InMemory::new()), None)
                };
                let base = Path::from("publication-race");
                let store = PackedChunks::open_with_initial_catalog(
                    objects.clone(),
                    base.clone(),
                    u64::MAX,
                    0,
                    Some(&PackedChunks::empty_state_catalog().unwrap()),
                    durability,
                )
                .await
                .unwrap();
                let (live, live_bytes) = chunk(b"live across collection");
                let (dead, dead_bytes) = chunk(b"obsolete before collection");
                store.put(live.clone(), live_bytes.clone()).await.unwrap();
                store.put(dead.clone(), dead_bytes).await.unwrap();
                store.prepare_state_catalog().await.unwrap().unwrap();
                store.finish_state_catalog(true).unwrap();
                let old_pack = store.location(&dead.digest).await.unwrap().unwrap().pack;
                let old_path = pack_path(&base, &old_pack);
                let (later, later_bytes) = chunk(b"retired by the next publication");
                store.put(later.clone(), later_bytes).await.unwrap();
                store.prepare_state_catalog().await.unwrap().unwrap();
                store.finish_state_catalog(true).unwrap();
                let later_pack = store.location(&later.digest).await.unwrap().unwrap().pack;
                let later_path = pack_path(&base, &later_pack);

                let ledger = Arc::new(crate::metadata::MemoryPinStore::default());
                let collector = ledger.acquire_collection(None).await.unwrap().unwrap();
                store.delete_many(&[dead.digest]).await.unwrap();
                store.finish_deletions(true).await.unwrap();
                store.prepare_state_catalog().await.unwrap().unwrap();
                // Publication may overlap a newer removal. Only the captured
                // retirement belongs to the commit being acknowledged.
                store.delete_many(&[later.digest]).await.unwrap();
                store
                    .finish_deletions_pinned(true, ledger.clone(), BTreeSet::new(), false)
                    .await
                    .unwrap();
                assert!(
                    store
                        .pending_catalog
                        .lock()
                        .unwrap()
                        .retirements
                        .contains(&later_path)
                );
                store.finish_state_catalog(true).unwrap();
                assert!(store.index_dirty.load(Ordering::Acquire));
                assert!(
                    !store
                        .published_retirements
                        .lock()
                        .unwrap()
                        .contains(&later_path)
                );
                assert!(store.prepared_index_catalog.lock().unwrap().is_none());
                assert!(
                    store
                        .published_retirements
                        .lock()
                        .unwrap()
                        .contains(&old_path)
                );

                // GC's catalog is committed. A writer on the shared backend
                // stages its next upload before GC reaches payload cleanup.
                let (new, new_bytes) = chunk(b"new writer's unpublished bytes");
                store.put(new.clone(), new_bytes.clone()).await.unwrap();
                store.flush().await.unwrap();
                let new_pack = store.location(&new.digest).await.unwrap().unwrap().pack;
                let new_path = pack_path(&base, &new_pack);
                let pin = ledger
                    .register(DataPin {
                        scope: PinScope::Staging,
                        catalog: None,
                        resources: BTreeSet::from([PinResource::StorageObject(
                            new_path.to_string(),
                        )]),
                    })
                    .await
                    .unwrap()
                    .unwrap();
                if prepared {
                    store.prepare_state_catalog().await.unwrap().unwrap();
                    assert!(!store.index_dirty.load(Ordering::Acquire));
                    assert!(store.prepared_index_catalog.lock().unwrap().is_some());
                } else {
                    assert!(store.index_dirty.load(Ordering::Acquire));
                    assert!(store.prepared_index_catalog.lock().unwrap().is_none());
                }

                // Unpinned cleanup must still reject unpublished catalogs.
                assert_eq!(
                    store
                        .finish_collection(false)
                        .await
                        .unwrap_err()
                        .to_string(),
                    "cannot delete retired representations before catalog publication"
                );
                // Emergency cleanup remains strict; online GC and vacuum
                // can drain the older published batch.
                let fence = ledger
                    .begin_prune(ledger.inventory().await.unwrap().revision)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    store
                        .finish_collection_pinned(true, ledger.clone(), BTreeSet::new())
                        .await
                        .unwrap_err()
                        .to_string(),
                    "cannot delete retired representations before catalog publication"
                );
                assert_eq!(
                    ledger.inventory().await.unwrap().logical_prune.as_ref(),
                    Some(&fence)
                );
                ledger.finish_prune(&fence).await.unwrap();
                for force_reclaim in [false, true] {
                    store
                        .finish_collection_pinned(force_reclaim, ledger.clone(), BTreeSet::new())
                        .await
                        .unwrap();
                }
                assert!(objects.head(&old_path).await.is_err());
                assert!(objects.head(&later_path).await.is_ok());
                assert!(objects.head(&new_path).await.is_ok());
                assert!(
                    !store
                        .published_retirements
                        .lock()
                        .unwrap()
                        .contains(&old_path)
                );
                assert!(ledger.inventory().await.unwrap().deletions.is_empty());

                // The collector can settle while the next batch is pending.
                ledger.finish_collection(&collector).await.unwrap();
                assert!(ledger.inventory().await.unwrap().collector.is_none());
                if prepared {
                    // A failed publication restores its mutations. Cleanup
                    // must still defer on the next pass until retry succeeds.
                    store.finish_state_catalog(false).unwrap();
                    assert!(store.index_dirty.load(Ordering::Acquire));
                }
                let collector = ledger.acquire_collection(None).await.unwrap().unwrap();
                store
                    .finish_collection_pinned(false, ledger.clone(), BTreeSet::new())
                    .await
                    .unwrap();
                assert!(objects.head(&old_path).await.is_err());
                assert!(objects.head(&later_path).await.is_ok());
                store.prepare_state_catalog().await.unwrap().unwrap();
                store.finish_state_catalog(true).unwrap();
                store
                    .finish_collection_pinned(true, ledger.clone(), BTreeSet::new())
                    .await
                    .unwrap();
                assert!(objects.head(&old_path).await.is_err());
                assert!(objects.head(&new_path).await.is_ok());
                assert!(objects.head(&later_path).await.is_err());
                assert_eq!(store.get(&live.digest).await.unwrap(), Some(live_bytes));
                assert_eq!(store.get(&new.digest).await.unwrap(), Some(new_bytes));
                ledger.release(&pin).await.unwrap();
                ledger.finish_collection(&collector).await.unwrap();
                let inventory = ledger.inventory().await.unwrap();
                assert!(inventory.pins.is_empty() && inventory.deletions.is_empty());
                assert!(inventory.collector.is_none() && inventory.logical_prune.is_none());
            }
        }
    }

    #[tokio::test]
    async fn state_catalog_gc_preserves_old_packs_until_catalog_publication() {
        for (local, keep_live) in [(false, false), (false, true), (true, true)] {
            let directory = tempfile::tempdir().unwrap();
            let (objects, durability): (Arc<dyn ObjectStore>, _) = if local {
                let fs = object_store::local::LocalFileSystem::new_with_prefix(directory.path())
                    .unwrap()
                    .with_fsync(true);
                let durability = LocalDurability::new(fs.clone(), directory.path()).unwrap();
                (Arc::new(fs), Some(durability))
            } else {
                (Arc::new(InMemory::new()), None)
            };
            let base = Path::from("retirement");
            let initial = PackedChunks::empty_state_catalog().unwrap();
            let store = PackedChunks::open_with_initial_catalog(
                objects.clone(),
                base.clone(),
                u64::MAX,
                0,
                Some(&initial),
                durability.clone(),
            )
            .await
            .unwrap();
            let (live, live_bytes) = chunk(b"still live");
            let (dead, dead_bytes) = chunk(b"collect me");
            if keep_live {
                store.put(live.clone(), live_bytes.clone()).await.unwrap();
            }
            store.put(dead.clone(), dead_bytes.clone()).await.unwrap();
            let before = store.prepare_state_catalog().await.unwrap().unwrap();
            store.finish_state_catalog(true).unwrap();
            let old_pack = store.location(&dead.digest).await.unwrap().unwrap().pack;
            store.delete_many(&[dead.digest]).await.unwrap();
            store.finish_deletions(true).await.unwrap();
            assert!(objects.head(&pack_path(&base, &old_pack)).await.is_ok());
            assert!(store.finish_collection(false).await.is_err());
            let _candidate = store.prepare_state_catalog().await.unwrap().unwrap();
            store.finish_state_catalog(false).unwrap();
            assert!(objects.head(&pack_path(&base, &old_pack)).await.is_ok());
            drop(store); // interrupted before publishing the catalog

            let reopened = PackedChunks::open_with_initial_catalog(
                objects.clone(),
                base.clone(),
                u64::MAX,
                0,
                Some(&before),
                durability.clone(),
            )
            .await
            .unwrap();
            assert_eq!(reopened.get(&dead.digest).await.unwrap(), Some(dead_bytes));
            if keep_live {
                assert_eq!(
                    reopened.get(&live.digest).await.unwrap(),
                    Some(live_bytes.clone())
                );
            }
            reopened.delete_many(&[dead.digest]).await.unwrap();
            reopened.finish_deletions(true).await.unwrap();
            let after = reopened.prepare_state_catalog().await.unwrap().unwrap();
            reopened.finish_state_catalog(true).unwrap();
            reopened.finish_collection(false).await.unwrap();
            assert!(matches!(
                objects.head(&pack_path(&base, &old_pack)).await,
                Err(object_store::Error::NotFound { .. })
            ));
            let current = PackedChunks::open_with_initial_catalog(
                objects,
                base,
                u64::MAX,
                0,
                Some(&after),
                durability,
            )
            .await
            .unwrap();
            assert_eq!(current.get(&dead.digest).await.unwrap(), None);
            if keep_live {
                assert_eq!(current.get(&live.digest).await.unwrap(), Some(live_bytes));
            }
        }
    }

    #[tokio::test]
    async fn state_catalog_vacuum_recovers_retired_packs_without_deleting_reintroduced_content() {
        for restart in [false, true] {
            for reintroduce in [false, true] {
                let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
                let base = Path::from("retirement-recovery");
                let store = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
                    .await
                    .unwrap();
                store.enable_state_catalog();
                let (meta, bytes) = chunk(b"retired content");
                store.put(meta.clone(), bytes.clone()).await.unwrap();
                store.prepare_state_catalog().await.unwrap().unwrap();
                store.finish_state_catalog(true).unwrap();
                let old = store.location(&meta.digest).await.unwrap().unwrap().pack;
                store.delete_many(&[meta.digest]).await.unwrap();
                store.finish_deletions(true).await.unwrap();
                let catalog = store.prepare_state_catalog().await.unwrap().unwrap();
                store.finish_state_catalog(true).unwrap();
                assert!(objects.head(&pack_path(&base, &old)).await.is_ok());
                let mut recovered = if restart {
                    drop(store); // crash after publication, before physical deletion
                    PackedChunks::open_with_state_catalog(
                        objects.clone(),
                        base.clone(),
                        u64::MAX,
                        0,
                        &catalog,
                    )
                    .await
                    .unwrap()
                } else {
                    store
                };
                if reintroduce {
                    recovered.put(meta.clone(), bytes.clone()).await.unwrap();
                    let current = recovered.prepare_state_catalog().await.unwrap().unwrap();
                    recovered.finish_state_catalog(true).unwrap();
                    if restart {
                        recovered = PackedChunks::open_with_state_catalog(
                            objects.clone(),
                            base.clone(),
                            u64::MAX,
                            0,
                            &current,
                        )
                        .await
                        .unwrap();
                    }
                    assert_eq!(
                        recovered
                            .location(&meta.digest)
                            .await
                            .unwrap()
                            .unwrap()
                            .pack,
                        old
                    );
                }
                recovered.finish_collection(true).await.unwrap();
                assert_eq!(
                    objects.head(&pack_path(&base, &old)).await.is_ok(),
                    reintroduce
                );
                if reintroduce {
                    assert_eq!(recovered.get(&meta.digest).await.unwrap(), Some(bytes));
                }
            }
        }
    }

    #[tokio::test]
    async fn wholly_dead_pack_is_deleted_without_reading_or_rewriting_it() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("payloads");
        let store = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let (dead, dead_bytes) = chunk(b"entire pack is dead");
        store.put(dead.clone(), dead_bytes).await.unwrap();
        store.flush().await.unwrap();
        store.reset_read_stats();

        store.delete_many(&[dead.digest]).await.unwrap();
        store.finish_deletions(true).await.unwrap();
        let stats = store.read_stats();
        assert_eq!(stats.whole_pack_requests, 0);
        assert_eq!(stats.gc_replacement_put_requests, 0);
        assert_eq!(stats.gc_marker_put_requests, 1);
        assert_eq!(stats.gc_pack_delete_requests, 1);

        let reopened = PackedChunks::open(objects, base, u64::MAX).await.unwrap();
        assert_eq!(reopened.get(&dead.digest).await.unwrap(), None);
    }

    #[tokio::test]
    async fn sparse_deletions_persist_as_tombstones_and_vacuum_later_reclaims() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("payloads");
        let store = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let chunks = (0..5)
            .map(|index| chunk(format!("equal-sized-chunk-{index}").as_bytes()))
            .collect::<Vec<_>>();
        for (meta, bytes) in &chunks {
            store.put(meta.clone(), bytes.clone()).await.unwrap();
        }
        store.flush().await.unwrap();
        store.reset_read_stats();

        store.delete_many(&[chunks[0].0.digest]).await.unwrap();
        store.finish_deletions(false).await.unwrap();
        let stats = store.read_stats();
        assert_eq!(stats.whole_pack_requests, 0);
        assert_eq!(stats.gc_replacement_put_requests, 0);
        assert_eq!(stats.gc_pack_delete_requests, 0);
        assert_eq!(stats.gc_tombstone_put_requests, 1);
        assert_eq!(stats.gc_deferred_packs, 1);

        let reopened = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        assert_eq!(reopened.get(&chunks[0].0.digest).await.unwrap(), None);
        assert!(reopened.get(&chunks[1].0.digest).await.unwrap().is_some());

        reopened.reset_read_stats();
        // No deletion is pending in this handle. Forced reclamation must
        // still discover and compact the tombstone recovered during open.
        reopened.finish_deletions(true).await.unwrap();
        let stats = reopened.read_stats();
        assert_eq!(stats.whole_pack_requests, 1);
        assert_eq!(stats.gc_replacement_put_requests, 1);
        assert_eq!(stats.gc_marker_put_requests, 1);
        assert_eq!(stats.gc_pack_delete_requests, 1);

        let reopened = PackedChunks::open(objects, base, u64::MAX).await.unwrap();
        assert_eq!(reopened.get(&chunks[0].0.digest).await.unwrap(), None);
        for (meta, _) in &chunks[1..] {
            assert!(reopened.get(&meta.digest).await.unwrap().is_some());
        }
    }

    #[tokio::test]
    async fn matching_inline_index_catalog_elides_inventory_and_footer_reads() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("payloads");
        let store = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let chunks = (0..5)
            .map(|index| chunk(format!("checkpoint-chunk-{index}").as_bytes()))
            .collect::<Vec<_>>();
        for (meta, bytes) in &chunks {
            store.put(meta.clone(), bytes.clone()).await.unwrap();
        }
        store.flush().await.unwrap();
        store.delete_many(&[chunks[0].0.digest]).await.unwrap();
        store.finish_deletions(false).await.unwrap();

        // Flush and collection both publish the authoritative catalog, so a
        // new process needs only one catalog object GET.
        let rebuilt = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        assert_eq!(rebuilt.read_stats().index_hits, 1);
        assert_eq!(rebuilt.read_stats().index_fallbacks, 0);
        assert_eq!(rebuilt.read_stats().list_requests, 0);
        assert_eq!(rebuilt.read_stats().footer_range_requests, 0);
        let checkpoints = objects
            .list(Some(&kind_prefix(&base, INDEXES_KIND)))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(checkpoints.is_empty());
        drop(rebuilt);

        let checkpointed = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let stats = checkpointed.read_stats();
        assert_eq!(stats.index_hits, 1);
        assert_eq!(stats.index_pointer_requests, 1);
        assert_eq!(stats.index_requests, 0);
        assert!(stats.index_bytes > 0);
        assert_eq!(stats.footer_range_requests, 0);
        assert_eq!(checkpointed.get(&chunks[0].0.digest).await.unwrap(), None);
        assert!(
            checkpointed
                .get(&chunks[1].0.digest)
                .await
                .unwrap()
                .is_some()
        );

        // Tombstone record references are part of the catalog, not merely
        // the lookup table, so forced reclamation remains possible.
        checkpointed.reset_read_stats();
        checkpointed.finish_deletions(true).await.unwrap();
        assert_eq!(checkpointed.read_stats().gc_replacement_put_requests, 1);
    }

    #[tokio::test]
    async fn v1_records_exact_manifest_delta_with_one_catalog_put_and_one_get_open() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("v1-exact-delta");
        let store = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let (meta, compressed) = chunk(b"v1 base pack");
        store.put(meta, compressed).await.unwrap();
        store.flush().await.unwrap();

        let manifest = BlobId::new(blake3::hash(b"v1 delta manifest").into());
        store.reset_read_stats();
        store.register_manifest(manifest);
        store.flush().await.unwrap();
        let stats = store.read_stats();
        assert_eq!(stats.index_put_requests, 1);

        let pointer = objects
            .get(&base.clone().join(INDEX_POINTER_NAME))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let root = decode_delta_catalog(&pointer).unwrap();
        assert!(matches!(root.base, CatalogBase::Inline(_)));
        // The initial inventory rebuild writes the base; the pack and manifest
        // publications then append one exact delta each.
        assert_eq!(root.deltas.len(), 2);
        drop(store);

        let reopened = PackedChunks::open(objects, base, u64::MAX).await.unwrap();
        let stats = reopened.read_stats();
        assert_eq!(stats.index_pointer_requests, 1);
        assert_eq!(stats.index_requests, 0);
        assert_eq!(stats.list_requests, 0);
        assert!(!reopened.manifest_definitely_absent(&manifest));
    }

    #[tokio::test]
    async fn v1_seals_byte_bounded_inline_deltas_into_one_immutable_run() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("v1-leveled-run");
        let store = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let (meta, compressed) = chunk(b"v1 run base pack");
        store.put(meta.clone(), compressed.clone()).await.unwrap();
        store.flush().await.unwrap();

        let manifest_count = delta::MAX_INLINE_DELTA_BYTES / DIGEST_LEN + 1;
        let manifests = (0_u64..manifest_count as u64)
            .map(|ordinal| {
                let mut key = Vec::from(b"v1 run manifest ".as_slice());
                key.extend_from_slice(&ordinal.to_le_bytes());
                BlobId::new(blake3::hash(&key).into())
            })
            .collect::<Vec<_>>();
        for manifest in &manifests {
            store.register_manifest(*manifest);
        }
        store.reset_read_stats();
        store.flush().await.unwrap();
        let seal_stats = store.read_stats();

        let pointer = objects
            .get(&base.clone().join(INDEX_POINTER_NAME))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let root = decode_delta_catalog(&pointer).unwrap();
        assert!(root.deltas.is_empty());
        assert_eq!(root.runs.len(), 1);
        assert_eq!(seal_stats.index_put_requests, 2);
        let run_ref = root.runs.get(&0).unwrap();
        assert_eq!(run_ref.first_generation, 2);
        assert_eq!(run_ref.last_generation, 3);
        let run_bytes = objects
            .get(&sharded_path(&base, INDEXES_KIND, &run_ref.digest))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(run_bytes.len() as u64, run_ref.encoded_bytes);
        assert_eq!(Digest::from(blake3::hash(&run_bytes)), run_ref.digest);
        drop(store);

        let reopened = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let open_stats = reopened.read_stats();
        assert_eq!(open_stats.index_pointer_requests, 1);
        assert_eq!(open_stats.index_requests, 1);
        assert_eq!(open_stats.list_requests, 0);
        assert_eq!(
            reopened.get(&meta.digest).await.unwrap(),
            Some(compressed.clone())
        );
        for manifest in [
            manifests[0],
            manifests[manifests.len() / 2],
            manifests[manifests.len() - 1],
        ] {
            assert!(!reopened.manifest_definitely_absent(&manifest));
        }
        drop(reopened);

        objects
            .put(
                &sharded_path(&base, INDEXES_KIND, &run_ref.digest),
                Bytes::from_static(b"corrupt catalog run").into(),
            )
            .await
            .unwrap();
        let repaired = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let repair_stats = repaired.read_stats();
        assert_eq!(repair_stats.index_hits, 0);
        assert_eq!(repair_stats.index_fallbacks, 1);
        assert_eq!(repair_stats.list_requests, 4);
        assert_eq!(repair_stats.index_put_requests, 1);
        assert_eq!(repaired.get(&meta.digest).await.unwrap(), Some(compressed));
        let repaired_pointer = objects
            .get(&base.clone().join(INDEX_POINTER_NAME))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let repaired_root = decode_delta_catalog(&repaired_pointer).unwrap();
        assert!(repaired_root.runs.is_empty());
        assert!(repaired_root.deltas.is_empty());
    }

    #[tokio::test]
    async fn state_committed_catalog_avoids_pointer_put_and_synchronizes_reader() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("state-committed-catalog");
        let writer = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let (meta, compressed) = chunk(b"state committed pack");
        writer.put(meta.clone(), compressed.clone()).await.unwrap();
        writer.flush().await.unwrap();
        let pointer_path = base.clone().join(INDEX_POINTER_NAME);
        let old_pointer = objects
            .get(&pointer_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        let manifest = BlobId::new(blake3::hash(b"state committed manifest").into());
        writer.register_manifest(manifest);
        writer.reset_read_stats();
        let catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
        assert_eq!(writer.read_stats().index_put_requests, 0);
        assert_eq!(
            objects
                .get(&pointer_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            old_pointer
        );
        writer.finish_state_catalog(true).unwrap();

        let reader = PackedChunks::open(objects, base, u64::MAX).await.unwrap();
        assert!(reader.manifest_definitely_absent(&manifest));
        reader
            .synchronize_state_catalog(Some(&catalog))
            .await
            .unwrap();
        assert!(!reader.manifest_definitely_absent(&manifest));
        assert_eq!(
            reader.get(&meta.digest).await.unwrap(),
            Some(compressed.clone())
        );
    }

    #[tokio::test]
    async fn large_prepared_catalog_run_uses_bounded_multipart_upload() {
        const RUN_BYTES: u64 = 64 * 1024 * 1024 + 1;
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("multipart-catalog-run");
        let writer = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let temporary = tempfile::NamedTempFile::new().unwrap();
        temporary.as_file().set_len(RUN_BYTES).unwrap();
        // A run is stored under the hash of its bytes: here, zeros.
        let digest = Digest::from(blake3::hash(&vec![0_u8; RUN_BYTES as usize]));
        writer.reset_read_stats();
        let reference = writer
            .put_prepared_catalog_run(PreparedCatalogRunFile {
                file: temporary,
                reference: CatalogRunRef {
                    digest,
                    first_generation: 1,
                    last_generation: 1,
                    encoded_bytes: RUN_BYTES,
                    query: None,
                },
            })
            .await
            .unwrap();

        assert_eq!(reference.digest, digest);
        assert_eq!(writer.read_stats().index_put_requests, 1);
        assert_eq!(writer.read_stats().index_put_bytes, RUN_BYTES);
        assert_eq!(
            objects
                .head(&sharded_path(&base, INDEXES_KIND, &digest))
                .await
                .unwrap()
                .size,
            RUN_BYTES
        );
    }

    #[tokio::test]
    async fn state_catalog_seeded_open_skips_pointer_and_inventory() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("state-seeded-open");
        let writer = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let (meta, compressed) = chunk(b"state-seeded catalog");
        writer.put(meta.clone(), compressed.clone()).await.unwrap();
        writer.flush().await.unwrap();
        writer.enable_state_catalog();
        let manifest = BlobId::new(blake3::hash(b"state-seeded manifest").into());
        writer.register_manifest(manifest);
        let catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(true).unwrap();

        objects
            .delete(&base.clone().join(INDEX_POINTER_NAME))
            .await
            .unwrap();
        let seeded = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &catalog,
        )
        .await
        .unwrap();
        let seeded_stats = seeded.read_stats();
        assert!(!seeded_stats.index_sharded_base);
        assert!(!seeded_stats.index_checkpoint_base);
        assert_eq!(seeded_stats.index_run_objects, 0);
        assert_eq!(seeded_stats.index_pointer_requests, 0);
        assert_eq!(seeded_stats.list_requests, 0);
        assert_eq!(seeded_stats.index_hits, 1);
        assert!(!seeded.manifest_definitely_absent(&manifest));
        assert_eq!(seeded.get(&meta.digest).await.unwrap(), Some(compressed));

        seeded.reset_read_stats();
        seeded.refresh().await.unwrap();
        let refreshed_stats = seeded.read_stats();
        assert_eq!(refreshed_stats.index_pointer_requests, 0);
        assert_eq!(refreshed_stats.list_requests, 0);
    }

    #[test]
    fn decoded_chunk_routing_shares_the_catalog_cache_byte_budget() {
        let digest = Digest::from([1; 32]);
        let routing = Arc::new(vec![shard::ChunkBlock {
            first: digest,
            last: digest,
            offset: 21,
            length: 96,
            digest,
        }]);
        let weight = 8 + std::mem::size_of::<shard::ChunkBlock>() as u64;
        let mut cache = CatalogShardCache::new(weight);
        cache.insert(digest, Bytes::from_static(b"rawbytes"));
        cache.insert_with_routing(
            digest,
            Bytes::from_static(b"rawbytes"),
            Some(routing.clone()),
        );
        assert_eq!(cache.used, weight);
        assert!(Arc::ptr_eq(&cache.get_routing(digest).unwrap(), &routing));
        cache.insert(Digest::from([2; 32]), Bytes::from_static(b"next"));
        assert!(cache.get_routing(digest).is_none());
        assert_eq!(cache.used, 4);
        let mut tiny = CatalogShardCache::new(8);
        tiny.insert(digest, Bytes::from_static(b"rawbytes"));
        tiny.insert_with_routing(digest, Bytes::from_static(b"rawbytes"), Some(routing));
        assert_eq!(tiny.get(digest).unwrap(), Bytes::from_static(b"rawbytes"));
        assert_eq!(tiny.used, 8);
    }

    #[tokio::test]
    async fn catalog_range_reads_reject_corrupt_routing_and_chunk_blocks() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("corrupt-routed-shard");
        let reader = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let mut entries = (0..2048_u64)
            .map(|ordinal| {
                let mut bytes = [0_u8; 32];
                bytes[24..].copy_from_slice(&ordinal.to_be_bytes());
                IndexedLocation {
                    digest: ChunkId::new(Digest::from(bytes)),
                    location: Location {
                        pack: PackId::new(Digest::from([1; 32])),
                        pack_len: 4096,
                        offset: 0,
                        framed_len: 64,
                        uncompressed_len: 32,
                    },
                }
            })
            .collect::<Vec<_>>();
        let (reference, bytes) = shard::encode_chunk_shard_object(1, 0, &mut entries).unwrap();
        let (routing_digest, routing_len) = reference.routing.unwrap();
        let routing_offset = reference.encoded_bytes - routing_len;
        let routing = &bytes[routing_offset as usize..];
        let blocks = shard::decode_chunk_routing(routing, reference).unwrap();
        let path = sharded_path(&base, INDEXES_KIND, &reference.digest);
        for (offset, length, digest) in [
            (routing_offset, routing_len, routing_digest),
            (blocks[0].offset, blocks[0].length, blocks[0].digest),
        ] {
            let mut corrupt = bytes.to_vec();
            corrupt[offset as usize] ^= 1;
            objects.put(&path, corrupt.into()).await.unwrap();
            assert!(
                reader
                    .load_catalog_shard_range(reference, offset, length, digest)
                    .await
                    .is_err()
            );
            objects.put(&path, bytes.clone().into()).await.unwrap();
            assert_eq!(
                reader
                    .load_catalog_shard_range(reference, offset, length, digest)
                    .await
                    .unwrap(),
                bytes.slice(offset as usize..(offset + length) as usize)
            );
        }
    }

    #[tokio::test]
    async fn sharded_base_open_is_lazy_and_chunk_reads_are_cached() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("lazy-sharded-base");
        let writer = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let (meta, compressed) = chunk(b"lazy sharded catalog payload");
        writer.put(meta.clone(), compressed.clone()).await.unwrap();
        writer.flush().await.unwrap();
        let manifest = BlobId::new(blake3::hash(b"lazy sharded manifest").into());
        writer.register_manifest(manifest);
        let index = writer.index.read().unwrap().clone();
        let encoded = encode_index_shards(&index, 4).unwrap();
        for (digest, bytes) in &encoded.objects {
            put_object(
                &objects,
                &sharded_path(&base, INDEXES_KIND, digest),
                bytes.clone(),
                true,
            )
            .await
            .unwrap();
        }
        put_object(
            &objects,
            &sharded_path(&base, INDEXES_KIND, &encoded.map_digest),
            encoded.map,
            true,
        )
        .await
        .unwrap();
        let catalog = encode_delta_catalog(&DeltaCatalog {
            sidecars: None,
            generation: 7,
            base: CatalogBase::Sharded {
                root: encoded.map_digest,
                shard_bits: 4,
            },
            runs: BTreeMap::new(),
            deltas: Vec::new(),
        })
        .unwrap();

        let reader = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &catalog,
        )
        .await
        .unwrap();
        let open_stats = reader.read_stats();
        assert!(open_stats.index_sharded_base);
        assert!(!open_stats.index_checkpoint_base);
        assert_eq!(open_stats.index_run_objects, 0);
        assert_eq!(open_stats.index_requests, 1, "open reads only the map");
        assert!(reader.index.read().unwrap().packs.is_empty());
        assert_eq!(reader.index.read().unwrap().chunks.len(), 0);
        assert!(!reader.manifest_definitely_absent(&manifest));

        reader.reset_read_stats();
        assert_eq!(
            reader.get(&meta.digest).await.unwrap(),
            Some(compressed.clone())
        );
        assert_eq!(reader.read_stats().index_requests, 1);
        assert_eq!(
            reader.get(&meta.digest).await.unwrap(),
            Some(compressed.clone())
        );
        assert_eq!(
            reader.read_stats().index_requests,
            1,
            "second lookup hits shard cache"
        );

        reader.reset_read_stats();
        assert_eq!(
            reader.list().try_collect::<Vec<_>>().await.unwrap(),
            vec![meta.digest]
        );
        assert_eq!(
            reader
                .list_manifests()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![manifest]
        );
        assert_eq!(
            reader.read_stats().index_requests,
            1,
            "chunk enumeration reused its cached shard and fetched one manifest shard"
        );
        reader.finish_deletions(true).await.unwrap();
        assert_eq!(reader.prepare_state_catalog().await.unwrap(), None);
        assert_eq!(reader.get(&meta.digest).await.unwrap(), Some(compressed));
    }

    #[tokio::test]
    async fn sidecar_only_publication_keeps_a_lazy_sharded_base() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("lazy-sharded-sidecar-only");
        let writer = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let (meta, compressed) = chunk(b"payload known only to the sharded base");
        writer.put(meta.clone(), compressed.clone()).await.unwrap();
        writer.flush().await.unwrap();
        let index = writer.index.read().unwrap().clone();
        let encoded = encode_index_shards(&index, 4).unwrap();
        for (digest, bytes) in &encoded.objects {
            put_object(
                &objects,
                &sharded_path(&base, INDEXES_KIND, digest),
                bytes.clone(),
                true,
            )
            .await
            .unwrap();
        }
        put_object(
            &objects,
            &sharded_path(&base, INDEXES_KIND, &encoded.map_digest),
            encoded.map,
            true,
        )
        .await
        .unwrap();
        let catalog = encode_delta_catalog(&DeltaCatalog {
            sidecars: None,
            generation: 7,
            base: CatalogBase::Sharded {
                root: encoded.map_digest,
                shard_bits: 4,
            },
            runs: BTreeMap::new(),
            deltas: Vec::new(),
        })
        .unwrap();
        let reader = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &catalog,
        )
        .await
        .unwrap();
        assert_eq!(reader.index.read().unwrap().chunks.len(), 0);

        // Only a sidecar changes, so this publication carries no index delta.
        let blob = BlobId::new(blake3::hash(b"sidecar-only publication").into());
        reader
            .put_sidecar(blob, Bytes::from_static(b"outboard"))
            .await
            .unwrap();
        let next = reader
            .prepare_state_catalog()
            .await
            .unwrap()
            .expect("a dirty sidecar publishes a catalog");
        reader.finish_state_catalog(true).unwrap();
        let root = decode_delta_catalog(&next).unwrap();
        assert!(
            matches!(root.base, CatalogBase::Sharded { root, .. } if root == encoded.map_digest),
            "the lazily opened base must not be replaced by a checkpoint of the materialized index"
        );

        let reopened = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &next)
            .await
            .unwrap();
        assert_eq!(reopened.get(&meta.digest).await.unwrap(), Some(compressed));
        assert_eq!(
            reopened.sidecar(blob, None).await.unwrap(),
            Some(Bytes::from_static(b"outboard"))
        );
    }

    #[tokio::test]
    async fn scoped_catalog_cache_does_not_retain_reader_leases() {
        use crate::metadata::{DataPin, DataPinLease, MemoryPinStore, PinScope, PinStore};
        let catalog = PackedChunks::empty_state_catalog().unwrap();
        let reader = PackedChunks::open_with_state_catalog(
            Arc::new(InMemory::new()),
            Path::from("unpinned-cache"),
            u64::MAX,
            0,
            &catalog,
        )
        .await
        .unwrap();
        let ledger = Arc::new(MemoryPinStore::default());
        let pin = DataPinLease::try_acquire(
            ledger.clone(),
            DataPin {
                scope: PinScope::Snapshot { generation: 0 },
                catalog: Some(catalog.clone()),
                resources: BTreeSet::new(),
            },
        )
        .await
        .unwrap()
        .unwrap();
        let snapshot = reader.scoped_catalog(&catalog, pin).await.unwrap();
        let counters = Arc::downgrade(&snapshot.counters());
        drop(snapshot);
        crate::metadata::flush_repository_leases().await.unwrap();
        assert!(ledger.inventory().await.unwrap().pins.is_empty());
        assert!(
            counters.upgrade().is_some(),
            "lookup remains cached without its lease"
        );
        drop(reader);
        assert!(
            counters.upgrade().is_none(),
            "cache must not form a reference cycle"
        );
    }

    #[tokio::test]
    async fn catalog_snapshots_share_bytes_but_isolate_visibility_and_retain_plans() {
        use crate::metadata::{
            DataPin, DataPinLease, MemoryPinStore, PinResource, PinScope, PinStore,
        };
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("lazy-sharded-base");
        let writer = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let (meta, compressed) = chunk(b"lazy sharded catalog payload");
        writer.put(meta.clone(), compressed.clone()).await.unwrap();
        writer.flush().await.unwrap();
        let manifest = BlobId::new(blake3::hash(b"lazy sharded manifest").into());
        writer.register_manifest(manifest);
        let index = writer.index.read().unwrap().clone();
        let encoded = encode_index_shards(&index, 4).unwrap();
        for (digest, bytes) in &encoded.objects {
            put_object(
                &objects,
                &sharded_path(&base, INDEXES_KIND, digest),
                bytes.clone(),
                true,
            )
            .await
            .unwrap();
        }
        put_object(
            &objects,
            &sharded_path(&base, INDEXES_KIND, &encoded.map_digest),
            encoded.map,
            true,
        )
        .await
        .unwrap();
        let catalog = encode_delta_catalog(&DeltaCatalog {
            sidecars: None,
            generation: 7,
            base: CatalogBase::Sharded {
                root: encoded.map_digest,
                shard_bits: 4,
            },
            runs: BTreeMap::new(),
            deltas: Vec::new(),
        })
        .unwrap();

        let reader = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &catalog,
        )
        .await
        .unwrap();

        let ledger = Arc::new(MemoryPinStore::default());
        let pin = DataPinLease::try_acquire(
            ledger.clone(),
            DataPin {
                scope: PinScope::Snapshot { generation: 7 },
                catalog: Some(catalog.to_vec()),
                resources: BTreeSet::new(),
            },
        )
        .await
        .unwrap()
        .unwrap();
        // Independent admissions share lookup state, not protection leases.
        let second_pin = DataPinLease::try_acquire(
            ledger.clone(),
            DataPin {
                scope: PinScope::Snapshot { generation: 7 },
                catalog: Some(catalog.to_vec()),
                resources: BTreeSet::new(),
            },
        )
        .await
        .unwrap()
        .unwrap();
        let (first, second) = tokio::join!(
            reader.scoped_catalog(&catalog, pin.clone()),
            reader.scoped_catalog(&catalog, second_pin),
        );
        let first = first.unwrap();
        let second = second.unwrap();
        let first_counters = first.counters();
        let second_counters = second.counters();
        first_counters.reset();
        second_counters.reset();
        assert!(Arc::ptr_eq(&first_counters, &second_counters));
        // An unrelated catalog can have the same generation. It must replace
        // the cache without changing the already admitted readers' visibility.
        let mut empty =
            decode_delta_catalog(&PackedChunks::empty_state_catalog().unwrap()).unwrap();
        empty.generation = 7;
        let empty = encode_delta_catalog(&empty).unwrap();
        let empty_pin = DataPinLease::try_acquire(
            ledger.clone(),
            DataPin {
                scope: PinScope::Snapshot { generation: 7 },
                catalog: Some(empty.to_vec()),
                resources: BTreeSet::new(),
            },
        )
        .await
        .unwrap()
        .unwrap();
        let empty_snapshot = reader.scoped_catalog(&empty, empty_pin).await.unwrap();
        assert!(!Arc::ptr_eq(&first_counters, &empty_snapshot.counters()));
        assert!(
            empty_snapshot
                .read_bare_chunk(&meta.digest)
                .await
                .unwrap()
                .is_none()
        );
        drop(empty_snapshot);
        // Both preparations race on one cold shard of the shared immutable view.
        let requested = [meta.clone(), meta.clone()];
        let (first, second) = tokio::join!(
            first.prepare_read(&requested),
            second.prepare_read(&requested),
        );
        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(first_counters.index_requests.load(Ordering::Relaxed), 1);
        let location = reader.location(&meta.digest).await.unwrap().unwrap();
        let resource = crate::metadata::PinResource::StorageObject(
            pack_path(&reader.base, &location.pack).to_string(),
        );
        crate::metadata::flush_repository_leases().await.unwrap();
        let inventory = ledger.inventory().await.unwrap();
        assert_eq!(inventory.pins.len(), 2);
        assert!(inventory.pins.values().all(|pin| {
            pin.resources.contains(&resource)
                && pin.resources.contains(&PinResource::Chunk(meta.digest))
        }));
        let third = reader.scoped_catalog(&catalog, pin.clone()).await.unwrap();
        reader
            .synchronize_state_catalog(Some(&PackedChunks::empty_state_catalog().unwrap()))
            .await
            .unwrap();
        assert!(reader.get(&meta.digest).await.unwrap().is_none());
        let backend = Arc::downgrade(&reader);
        drop(reader);
        assert!(
            backend.upgrade().is_none(),
            "snapshots and plans must not retain the writer"
        );
        assert_eq!(
            third.read_bare_chunk(&meta.digest).await.unwrap(),
            Some(compressed.clone())
        );
        drop(third);
        assert_eq!(first.read_chunk(&meta.digest).await.unwrap(), compressed);
        assert_eq!(second.read_chunk(&meta.digest).await.unwrap(), compressed);
        let missing = ChunkId::new(blake3::hash(b"outside read plan").into());
        assert!(first.read_chunk(&missing).await.is_err());
        drop(pin);
        drop(first);
        crate::metadata::flush_repository_leases().await.unwrap();
        assert_eq!(ledger.inventory().await.unwrap().pins.len(), 1);
        drop(second);
        crate::metadata::flush_repository_leases().await.unwrap();
        assert!(ledger.inventory().await.unwrap().pins.is_empty());
    }

    #[tokio::test]
    async fn sharded_base_defers_immutable_runs_until_first_membership_read() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("lazy-sharded-runs");
        let writer = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let (base_chunk, base_bytes) = chunk(b"lazy run base chunk");
        writer
            .put(base_chunk.clone(), base_bytes.clone())
            .await
            .unwrap();
        writer.flush().await.unwrap();
        let base_index = writer.index.read().unwrap().clone();
        let encoded = encode_index_shards(&base_index, 4).unwrap();
        for (digest, bytes) in &encoded.objects {
            put_object(
                &objects,
                &sharded_path(&base, INDEXES_KIND, digest),
                bytes.clone(),
                true,
            )
            .await
            .unwrap();
        }
        put_object(
            &objects,
            &sharded_path(&base, INDEXES_KIND, &encoded.map_digest),
            encoded.map,
            true,
        )
        .await
        .unwrap();

        let (run_chunk, run_bytes) = chunk(b"lazy run overlay chunk");
        writer
            .put(run_chunk.clone(), run_bytes.clone())
            .await
            .unwrap();
        writer.flush().await.unwrap();
        let manifest = BlobId::new(blake3::hash(b"lazy run manifest").into());
        writer.register_manifest(manifest);
        let mut next = writer.index.read().unwrap().clone();
        let base_pack = *base_index.packs.keys().next().unwrap();
        next.remove_pack(base_pack);
        let run_pack = *next
            .packs
            .keys()
            .find(|pack| !base_index.packs.contains_key(pack))
            .unwrap();
        let mut mutations = IndexMutations::default();
        mutations.record_pack(run_pack);
        mutations.record_pack(base_pack);
        mutations.record_manifest_add(manifest);
        let run = CatalogRun {
            first_generation: 2,
            last_generation: 2,
            delta: encode_index_mutations(&next, &mutations).unwrap(),
        };
        let run_object = encode_catalog_run(&run).unwrap();
        let run_digest = Digest::from(blake3::hash(&run_object));
        put_object(
            &objects,
            &sharded_path(&base, INDEXES_KIND, &run_digest),
            run_object.clone(),
            true,
        )
        .await
        .unwrap();
        let later_manifest = BlobId::new(blake3::hash(b"lazy inline manifest").into());
        let mut final_index = next.clone();
        final_index.manifests.remove(&manifest);
        final_index.manifests.insert(later_manifest);
        let mut inline_mutations = IndexMutations::default();
        inline_mutations.record_manifest_remove(manifest);
        inline_mutations.record_manifest_add(later_manifest);
        let inline_delta = encode_index_mutations(&final_index, &inline_mutations).unwrap();
        let catalog = encode_delta_catalog(&DeltaCatalog {
            sidecars: None,
            generation: 3,
            base: CatalogBase::Sharded {
                root: encoded.map_digest,
                shard_bits: 4,
            },
            runs: BTreeMap::from([(
                0,
                CatalogRunRef {
                    digest: run_digest,
                    first_generation: 2,
                    last_generation: 2,
                    encoded_bytes: run_object.len() as u64,
                    query: Some(catalog_run_query_ref(&run_object).unwrap()),
                },
            )]),
            deltas: vec![inline_delta],
        })
        .unwrap();

        // Snapshot readers use the same visibility rules for legacy whole
        // runs and routed runs, without constructing a mutable pack backend.
        for queryable in [false, true] {
            let mut root = decode_delta_catalog(&catalog).unwrap();
            if !queryable {
                root.runs.get_mut(&0).unwrap().query = None;
            }
            let selected = encode_delta_catalog(&root).unwrap();
            let ledger = Arc::new(crate::metadata::MemoryPinStore::default());
            let pin = crate::metadata::DataPinLease::try_acquire(
                ledger,
                crate::metadata::DataPin {
                    scope: crate::metadata::PinScope::Snapshot { generation: 3 },
                    catalog: Some(selected.to_vec()),
                    resources: BTreeSet::new(),
                },
            )
            .await
            .unwrap()
            .unwrap();
            let snapshot = writer.scoped_catalog(&selected, pin).await.unwrap();
            let counters = snapshot.counters();
            assert_eq!(
                counters.index_requests.load(Ordering::Relaxed),
                1,
                "opening remains lazy"
            );
            assert!(snapshot.manifest_definitely_absent(&manifest));
            assert!(!snapshot.manifest_definitely_absent(&later_manifest));
            assert_eq!(
                snapshot.read_bare_chunk(&base_chunk.digest).await.unwrap(),
                None
            );
            assert_eq!(
                snapshot.read_bare_chunk(&run_chunk.digest).await.unwrap(),
                Some(run_bytes.clone())
            );
            let requests = counters.index_requests.load(Ordering::Relaxed);
            assert_eq!(
                snapshot.read_bare_chunk(&run_chunk.digest).await.unwrap(),
                Some(run_bytes.clone())
            );
            assert_eq!(counters.index_requests.load(Ordering::Relaxed), requests);
        }

        let reader = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &catalog,
        )
        .await
        .unwrap();
        let open = reader.read_stats();
        assert_eq!(open.index_requests, 1, "open fetches only the shard map");
        assert_eq!(open.index_run_objects, 1);
        assert!(reader.index.read().unwrap().packs.is_empty());
        assert!(reader.manifest_definitely_absent(&manifest));
        assert!(!reader.manifest_definitely_absent(&later_manifest));

        reader.reset_read_stats();
        assert_eq!(reader.prepare_state_catalog().await.unwrap(), None);
        assert_eq!(
            reader.read_stats().index_requests,
            0,
            "a no-op publication does not materialize runs"
        );
        assert_eq!(
            reader.metadata(&run_chunk.digest).await.unwrap(),
            Some(run_chunk.size)
        );
        let first = reader.read_stats();
        assert_eq!(
            first.index_requests, 1,
            "first use fetches only the root-routed chunk block"
        );
        assert!(first.index_bytes < run_object.len() as u64);
        assert!(reader.index.read().unwrap().packs.is_empty());
        assert_eq!(
            reader.metadata(&run_chunk.digest).await.unwrap(),
            Some(run_chunk.size)
        );
        assert_eq!(
            reader.read_stats().index_requests,
            1,
            "the root routing and chunk block stay cached"
        );
        assert_eq!(
            reader.get(&run_chunk.digest).await.unwrap(),
            Some(run_bytes)
        );
        assert_eq!(reader.get(&base_chunk.digest).await.unwrap(), None);
        assert!(reader.manifest_definitely_absent(&manifest));
        assert!(!reader.manifest_definitely_absent(&later_manifest));

        let published_manifest = BlobId::new(blake3::hash(b"lazy published manifest").into());
        reader.reset_read_stats();
        reader.register_manifest(published_manifest);
        let published = reader.prepare_state_catalog().await.unwrap().unwrap();
        assert_eq!(
            reader.read_stats().index_requests,
            0,
            "an inline mutation must not fetch immutable run objects"
        );
        let published_root = decode_delta_catalog(&published).unwrap();
        assert_eq!(published_root.runs.len(), 1);
        assert_eq!(published_root.deltas.len(), 2);
        reader.finish_state_catalog(true).unwrap();

        // Force the next publication to carry the lazy level-0 run without
        // materializing it as an Index. The carry should issue one whole-run
        // GET and leave the in-process routing pointed at the merged object.
        let carry_manifests = (0_u64..=(delta::MAX_INLINE_DELTA_BYTES / DIGEST_LEN) as u64)
            .map(|ordinal| {
                let mut key = Vec::from(b"streaming lazy carry manifest ".as_slice());
                key.extend_from_slice(&ordinal.to_le_bytes());
                BlobId::new(blake3::hash(&key).into())
            })
            .collect::<Vec<_>>();
        reader.reset_read_stats();
        for manifest in &carry_manifests {
            reader.register_manifest(*manifest);
        }
        let carried = reader.prepare_state_catalog().await.unwrap().unwrap();
        let carry_stats = reader.read_stats();
        assert_eq!(carry_stats.index_requests, 1);
        assert!(carry_stats.index_bytes >= run_object.len() as u64);
        let carried_root = decode_delta_catalog(&carried).unwrap();
        assert!(carried_root.deltas.is_empty());
        assert_eq!(carried_root.runs.len(), 1);
        let carried_run = carried_root.runs.get(&1).unwrap();
        assert_eq!(carried_run.first_generation, 2);
        assert_eq!(carried_run.last_generation, 5);
        reader.finish_state_catalog(true).unwrap();
        assert_eq!(reader.lazy_catalog.read().unwrap().run_refs.len(), 1);
        assert_eq!(
            reader.metadata(&run_chunk.digest).await.unwrap(),
            Some(run_chunk.size)
        );

        let reopened = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &carried)
            .await
            .unwrap();
        assert!(!reopened.manifest_definitely_absent(&published_manifest));
        assert!(!reopened.manifest_definitely_absent(&carry_manifests[0]));
        assert!(!reopened.manifest_definitely_absent(carry_manifests.last().unwrap()));
        assert_eq!(
            reopened.metadata(&run_chunk.digest).await.unwrap(),
            Some(run_chunk.size)
        );
    }

    #[tokio::test]
    async fn sharded_run_publication_moves_routing_into_the_open_map() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("external-run-routing");
        let store = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let empty_map = Arc::new(ShardMap {
            shard_bits: 1,
            chunks: Vec::new(),
            manifests: Vec::new(),
            packs: Vec::new(),
            run_routing: BTreeMap::new(),
        });
        let empty_map_bytes = encode_shard_map(&empty_map).unwrap();
        let empty_map_digest = Digest::from(blake3::hash(&empty_map_bytes));
        let empty_delta = delta::encode_index_delta(&Index::default(), &Index::default()).unwrap();
        let previous = DeltaCatalog {
            sidecars: None,
            generation: 1025,
            base: CatalogBase::Sharded {
                root: empty_map_digest,
                shard_bits: 1,
            },
            runs: BTreeMap::new(),
            deltas: vec![empty_delta; 1024],
        };
        let witness = IndexCatalogWitness {
            generation: previous.generation,
            root: Some(previous),
            ..IndexCatalogWitness::default()
        };
        let lazy = LazyCatalogOverlay {
            base: Some(ShardedIndexBase { map: empty_map }),
            ..LazyCatalogOverlay::default()
        };
        let pack = PackId::new(blake3::hash(b"external routing pack").into());
        let wanted = PackEntry {
            digest: ChunkId::new(blake3::hash(b"external routing chunk").into()),
            offset: 0,
            framed_len: 128,
            uncompressed_len: 4096,
        };
        let mut candidate = Index::default();
        candidate.add_pack(pack, 1024, vec![wanted]);
        candidate.rebuild_chunks();
        let mut mutations = IndexMutations::default();
        mutations.record_pack(pack);
        let delta = encode_index_mutations(&candidate, &mutations).unwrap();

        let (_, catalog) = store
            .build_index_catalog(&candidate, Some(&delta), false, true, &witness, &lazy)
            .await
            .unwrap();
        let root = decode_delta_catalog(&catalog).unwrap();
        let reference = root.runs.values().next().unwrap();
        let run_digest = reference.digest;
        assert!(
            reference
                .query
                .as_ref()
                .is_some_and(|query| query.routing.is_empty())
        );
        let CatalogBase::Sharded {
            root: map_digest, ..
        } = root.base
        else {
            panic!("run publication lost its sharded base")
        };
        assert_ne!(map_digest, empty_map_digest);
        let map_bytes = objects
            .get(&sharded_path(&base, INDEXES_KIND, &map_digest))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let map = decode_shard_map(&map_bytes).unwrap();
        assert!(map.run_routing.contains_key(&run_digest));

        let reader = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &catalog)
            .await
            .unwrap();
        assert_eq!(reader.read_stats().index_requests, 1);
        reader.reset_read_stats();
        assert_eq!(reader.metadata(&wanted.digest).await.unwrap(), Some(4096));
        assert_eq!(reader.read_stats().index_requests, 1);
    }

    #[tokio::test]
    async fn materialized_checkpoint_that_outgrows_inline_carries_runs_into_its_map() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("materialized-sharded-carry");
        let empty = PackedChunks::empty_state_catalog().unwrap();
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &empty,
        )
        .await
        .unwrap();
        let manifests = |label: &[u8], count: usize| {
            (0..count as u64)
                .map(|ordinal| {
                    let mut key = Vec::from(label);
                    key.extend_from_slice(&ordinal.to_le_bytes());
                    BlobId::new(blake3::hash(&key).into())
                })
                .collect::<Vec<_>>()
        };
        let carry_len = delta::MAX_INLINE_DELTA_BYTES / DIGEST_LEN + 1;

        let checkpointed = manifests(
            b"materialized checkpoint manifest ",
            INDEX_INLINE_BASE_MAX_BYTES / DIGEST_LEN + 1,
        );
        for digest in &checkpointed {
            writer.register_manifest(*digest);
        }
        writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(true).unwrap();

        // A commit with no index mutation (here a sidecar) checkpoints the
        // whole materialized index. It outgrows the inline limit, so this
        // process publishes a sharded root without ever loading its map.
        writer
            .put_sidecar(checkpointed[0], Bytes::from_static(b"outboard"))
            .await
            .unwrap();
        writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(true).unwrap();
        let root = writer.index_catalog.lock().unwrap().root.clone().unwrap();
        assert!(matches!(root.base, CatalogBase::Sharded { .. }));
        assert!(root.runs.is_empty());
        assert!(writer.lazy_catalog.read().unwrap().base.is_none());

        // Carry into level 0, merge into level 1, then land beside it at
        // level 0; the untouched level-1 run keeps its routing in the map.
        let mut catalog = Vec::new();
        let mut carried = Vec::new();
        for (carry, levels) in [(0_u8, &[0_u8][..]), (1, &[1]), (2, &[0, 1])] {
            let digests = manifests(
                &[b"materialized carry manifest ".as_slice(), &[carry]].concat(),
                carry_len,
            );
            for digest in &digests {
                writer.register_manifest(*digest);
            }
            catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
            writer.finish_state_catalog(true).unwrap();
            let root = writer.index_catalog.lock().unwrap().root.clone().unwrap();
            assert!(root.deltas.is_empty());
            assert_eq!(root.runs.keys().copied().collect::<Vec<_>>(), levels);
            carried.extend(digests);
        }

        let reopened = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &catalog)
            .await
            .unwrap();
        for digest in [&checkpointed[0], &carried[0], carried.last().unwrap()] {
            assert!(!reopened.manifest_definitely_absent(digest));
        }
    }

    #[tokio::test]
    async fn standalone_carries_preserve_a_materialized_sharded_checkpoint() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("standalone-materialized-sharded-carries");
        let writer = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let (first, first_bytes) = chunk(b"first materialized shard");
        writer
            .put(first.clone(), first_bytes.clone())
            .await
            .unwrap();
        writer.flush().await.unwrap();

        // A sharded checkpoint can still have a complete in-process index in
        // standalone mode. Keep this fixture small by publishing its shards
        // directly instead of filling the 4 MiB checkpoint threshold.
        let publish_map = |index: &Index| {
            let encoded = encode_index_shards(index, 2).unwrap();
            let map = decode_shard_map(&encoded.map).unwrap();
            (encoded, map)
        };
        let (initial, _) = publish_map(&writer.index.read().unwrap());
        for (digest, bytes) in initial.objects {
            put_object(
                &objects,
                &sharded_path(&base, INDEXES_KIND, &digest),
                bytes,
                true,
            )
            .await
            .unwrap();
        }
        put_object(
            &objects,
            &sharded_path(&base, INDEXES_KIND, &initial.map_digest),
            initial.map,
            true,
        )
        .await
        .unwrap();
        let empty_delta = delta::encode_index_delta(&Index::default(), &Index::default()).unwrap();
        let prepare_carry = |witness: &mut IndexCatalogWitness, root, generation| {
            witness.generation = generation;
            witness.root = Some(DeltaCatalog {
                sidecars: None,
                generation,
                base: CatalogBase::Sharded {
                    root,
                    shard_bits: 2,
                },
                runs: BTreeMap::new(),
                deltas: vec![empty_delta.clone(); 1024],
            });
            witness.runs.clear();
        };
        {
            let mut witness = writer.index_catalog.lock().unwrap();
            prepare_carry(&mut witness, initial.map_digest, 1025);
        }
        assert!(writer.lazy_catalog.read().unwrap().base.is_none());
        writer.register_manifest(BlobId::new(blake3::hash(b"first carry").into()));
        writer.flush().await.unwrap();
        assert!(writer.lazy_catalog.read().unwrap().base.is_none());

        let (added, added_bytes) = chunk(b"shard added between carries");
        writer
            .put(added.clone(), added_bytes.clone())
            .await
            .unwrap();
        writer.flush().await.unwrap();
        let (checkpoint, map) = publish_map(&writer.index.read().unwrap());
        assert_ne!(checkpoint.map_digest, initial.map_digest);
        assert!(!map.chunks.is_empty());
        for (digest, bytes) in checkpoint.objects {
            put_object(
                &objects,
                &sharded_path(&base, INDEXES_KIND, &digest),
                bytes,
                true,
            )
            .await
            .unwrap();
        }
        put_object(
            &objects,
            &sharded_path(&base, INDEXES_KIND, &checkpoint.map_digest),
            checkpoint.map,
            true,
        )
        .await
        .unwrap();
        {
            let mut witness = writer.index_catalog.lock().unwrap();
            prepare_carry(&mut witness, checkpoint.map_digest, 2051);
        }
        writer.register_manifest(BlobId::new(blake3::hash(b"second carry").into()));
        writer.flush().await.unwrap();
        assert!(writer.lazy_catalog.read().unwrap().base.is_none());

        let reopened = PackedChunks::open(objects, base, u64::MAX).await.unwrap();
        // A standalone `get` may recover from inventory, so inspect the
        // published catalog before asking it to read payload bytes.
        assert!(reopened.location(&first.digest).await.unwrap().is_some());
        assert!(reopened.location(&added.digest).await.unwrap().is_some());
        assert_eq!(
            reopened.get(&first.digest).await.unwrap(),
            Some(first_bytes)
        );
        assert_eq!(
            reopened.get(&added.digest).await.unwrap(),
            Some(added_bytes)
        );
    }

    #[tokio::test]
    async fn forced_gc_rewrites_a_lazy_sharded_pack_without_hydrating_the_catalog() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("lazy-sharded-gc");
        let seed = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let (removed, removed_bytes) = chunk(b"removed lazy sharded payload");
        let (survivor, survivor_bytes) = chunk(b"surviving lazy sharded payload");
        seed.put(removed.clone(), removed_bytes).await.unwrap();
        seed.put(survivor.clone(), survivor_bytes.clone())
            .await
            .unwrap();
        seed.flush().await.unwrap();
        let original_pack = *seed.index.read().unwrap().packs.keys().next().unwrap();

        let encoded = encode_index_shards(&seed.index.read().unwrap().clone(), 4).unwrap();
        for (digest, bytes) in &encoded.objects {
            put_object(
                &objects,
                &sharded_path(&base, INDEXES_KIND, digest),
                bytes.clone(),
                true,
            )
            .await
            .unwrap();
        }
        put_object(
            &objects,
            &sharded_path(&base, INDEXES_KIND, &encoded.map_digest),
            encoded.map,
            true,
        )
        .await
        .unwrap();
        let catalog = encode_delta_catalog(&DeltaCatalog {
            sidecars: None,
            generation: 11,
            base: CatalogBase::Sharded {
                root: encoded.map_digest,
                shard_bits: 4,
            },
            runs: BTreeMap::new(),
            deltas: Vec::new(),
        })
        .unwrap();

        let collector = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &catalog,
        )
        .await
        .unwrap();
        collector.enable_state_catalog();
        assert!(collector.index.read().unwrap().packs.is_empty());
        collector.delete_many(&[removed.digest]).await.unwrap();
        collector.finish_deletions(true).await.unwrap();
        let collected = collector.prepare_state_catalog().await.unwrap().unwrap();
        collector.finish_state_catalog(true).unwrap();

        let reopened =
            PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &collected)
                .await
                .unwrap();
        assert!(
            reopened
                .lazy_catalog
                .read()
                .unwrap()
                .changed_packs
                .contains(&original_pack)
        );
        assert_eq!(reopened.get(&removed.digest).await.unwrap(), None);
        assert_eq!(
            reopened.get(&survivor.digest).await.unwrap(),
            Some(survivor_bytes)
        );
        assert!(reopened.index.read().unwrap().packs.len() <= 1);
    }

    #[tokio::test]
    async fn forced_gc_streams_preexisting_tombstones_from_pack_shards() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("lazy-sharded-tombstone-reclaim");
        let seed = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let (removed, removed_bytes) = chunk(b"tombstoned payload 0000000000000000");
        let (first, first_bytes) = chunk(b"surviving payload 111111111111111111");
        let (second, second_bytes) = chunk(b"surviving payload 222222222222222222");
        seed.put(removed.clone(), removed_bytes).await.unwrap();
        seed.put(first.clone(), first_bytes.clone()).await.unwrap();
        seed.put(second.clone(), second_bytes.clone())
            .await
            .unwrap();
        seed.flush().await.unwrap();
        seed.delete_many(&[removed.digest]).await.unwrap();
        seed.finish_deletions(false).await.unwrap();
        assert_eq!(seed.index.read().unwrap().tombstoned.len(), 1);

        let encoded = encode_index_shards(&seed.index.read().unwrap().clone(), 4).unwrap();
        for (digest, bytes) in &encoded.objects {
            put_object(
                &objects,
                &sharded_path(&base, INDEXES_KIND, digest),
                bytes.clone(),
                true,
            )
            .await
            .unwrap();
        }
        put_object(
            &objects,
            &sharded_path(&base, INDEXES_KIND, &encoded.map_digest),
            encoded.map,
            true,
        )
        .await
        .unwrap();
        let catalog = encode_delta_catalog(&DeltaCatalog {
            sidecars: None,
            generation: 17,
            base: CatalogBase::Sharded {
                root: encoded.map_digest,
                shard_bits: 4,
            },
            runs: BTreeMap::new(),
            deltas: Vec::new(),
        })
        .unwrap();

        let collector = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &catalog,
        )
        .await
        .unwrap();
        assert!(collector.index.read().unwrap().tombstoned.is_empty());
        collector.finish_deletions(true).await.unwrap();
        let collected = collector.prepare_state_catalog().await.unwrap().unwrap();
        collector.finish_state_catalog(true).unwrap();

        let reopened =
            PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &collected)
                .await
                .unwrap();
        assert_eq!(reopened.get(&removed.digest).await.unwrap(), None);
        assert_eq!(
            reopened.get(&first.digest).await.unwrap(),
            Some(first_bytes)
        );
        assert_eq!(
            reopened.get(&second.digest).await.unwrap(),
            Some(second_bytes)
        );
    }

    #[tokio::test]
    async fn streaming_rebase_folds_lazy_overlays_into_a_new_sharded_base() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("streaming-sharded-rebase");
        let seed = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let (removed, removed_bytes) = chunk(b"rebase removed pack");
        let (unchanged, unchanged_bytes) = chunk(b"rebase unchanged pack");
        seed.put(removed.clone(), removed_bytes).await.unwrap();
        seed.flush().await.unwrap();
        seed.put(unchanged.clone(), unchanged_bytes.clone())
            .await
            .unwrap();
        seed.flush().await.unwrap();
        let removed_manifest = BlobId::new(blake3::hash(b"rebase removed manifest").into());
        seed.register_manifest(removed_manifest);

        let encoded = encode_index_shards(&seed.index.read().unwrap().clone(), 2).unwrap();
        for (digest, bytes) in &encoded.objects {
            put_object(
                &objects,
                &sharded_path(&base, INDEXES_KIND, digest),
                bytes.clone(),
                true,
            )
            .await
            .unwrap();
        }
        put_object(
            &objects,
            &sharded_path(&base, INDEXES_KIND, &encoded.map_digest),
            encoded.map,
            true,
        )
        .await
        .unwrap();
        let initial = encode_delta_catalog(&DeltaCatalog {
            sidecars: None,
            generation: 23,
            base: CatalogBase::Sharded {
                root: encoded.map_digest,
                shard_bits: 2,
            },
            runs: BTreeMap::new(),
            deltas: Vec::new(),
        })
        .unwrap();
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &initial,
        )
        .await
        .unwrap();
        writer.delete_many(&[removed.digest]).await.unwrap();
        writer.finish_deletions(true).await.unwrap();
        let (added, added_bytes) = chunk(b"rebase newly added pack");
        writer
            .put(added.clone(), added_bytes.clone())
            .await
            .unwrap();
        writer.flush().await.unwrap();
        writer.unregister_manifest(removed_manifest);
        let added_manifest = BlobId::new(blake3::hash(b"rebase added manifest").into());
        writer.register_manifest(added_manifest);

        let candidate = writer.index.read().unwrap().clone();
        let lazy = writer.lazy_catalog.read().unwrap().clone();
        assert!(candidate.packs.len() < seed.index.read().unwrap().packs.len() + 1);
        let mutations = writer.pending_catalog.lock().unwrap().clone();
        let run = CatalogRun {
            first_generation: 24,
            last_generation: 24,
            delta: encode_index_mutations(&candidate, &mutations).unwrap(),
        };
        let run_bytes = encode_catalog_run(&run).unwrap();
        let run_reference = CatalogRunRef {
            digest: Digest::from(blake3::hash(&run_bytes)),
            first_generation: 24,
            last_generation: 24,
            encoded_bytes: run_bytes.len() as u64,
            query: Some(catalog_run_query_ref(&run_bytes).unwrap()),
        };
        put_object(
            &objects,
            &sharded_path(&base, INDEXES_KIND, &run_reference.digest),
            run_bytes,
            true,
        )
        .await
        .unwrap();
        let frozen = DeltaCatalog {
            sidecars: None,
            generation: 24,
            base: CatalogBase::Sharded {
                root: encoded.map_digest,
                shard_bits: 2,
            },
            runs: BTreeMap::from([(0, run_reference)]),
            deltas: Vec::new(),
        };
        writer
            .read_caches
            .catalog_shard_cache
            .lock()
            .unwrap()
            .clear();
        writer.reset_read_stats();
        let rebased = writer
            .rebase_catalog_shards_streaming(
                &frozen,
                lazy.base.as_ref().expect("sharded test base"),
            )
            .await
            .unwrap();
        let rebase_stats = writer.read_stats();
        assert_eq!(
            rebase_stats.index_requests - 1,
            lazy.base
                .as_ref()
                .map(|base| {
                    (base.map.chunks.len() + base.map.manifests.len() + base.map.packs.len()) as u64
                })
                .unwrap_or_default()
        );
        assert_eq!(
            rebase_stats.index_put_requests,
            (rebased.map.chunks.len() + rebased.map.manifests.len() + rebased.map.packs.len() + 1)
                as u64
        );
        // Model the prepare/commit boundary: mutations above are absorbed by
        // the new base, while a write racing after preparation must remain in
        // the local overlay for the following WAL3 revision.
        let _absorbed = std::mem::take(&mut *writer.pending_catalog.lock().unwrap());
        let (later, later_bytes) = chunk(b"rebase post-prepare pack");
        writer
            .put(later.clone(), later_bytes.clone())
            .await
            .unwrap();
        writer.flush().await.unwrap();
        writer.install_rebased_base(rebased.clone()).unwrap();
        assert_eq!(writer.index.read().unwrap().packs.len(), 1);
        assert_eq!(
            writer.get(&added.digest).await.unwrap(),
            Some(added_bytes.clone())
        );
        assert_eq!(writer.get(&later.digest).await.unwrap(), Some(later_bytes));

        let map = encode_shard_map(&rebased.map).unwrap();
        let map_digest = Digest::from(blake3::hash(&map));
        let catalog = encode_delta_catalog(&DeltaCatalog {
            sidecars: None,
            generation: 24,
            base: CatalogBase::Sharded {
                root: map_digest,
                shard_bits: rebased.map.shard_bits,
            },
            runs: BTreeMap::new(),
            deltas: Vec::new(),
        })
        .unwrap();

        let reopened = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &catalog)
            .await
            .unwrap();
        assert_eq!(reopened.get(&removed.digest).await.unwrap(), None);
        assert_eq!(
            reopened.get(&unchanged.digest).await.unwrap(),
            Some(unchanged_bytes)
        );
        assert_eq!(
            reopened.get(&added.digest).await.unwrap(),
            Some(added_bytes)
        );
        assert_eq!(
            reopened
                .list_manifests()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![added_manifest]
        );
    }

    #[tokio::test]
    async fn state_rebase_roots_are_atomic_and_post_prepare_writes_survive() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("atomic-state-rebase");
        let empty = PackedChunks::empty_state_catalog().unwrap();
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &empty,
        )
        .await
        .unwrap();
        let (old, old_bytes) = chunk(b"catalog before rebase");
        writer.put(old.clone(), old_bytes.clone()).await.unwrap();
        let old_catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(true).unwrap();

        writer.set_catalog_rebase_run_bytes_for_test(1);
        let (added, added_bytes) = chunk(b"catalog included by rebase");
        writer
            .put(added.clone(), added_bytes.clone())
            .await
            .unwrap();
        let trigger_catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
        assert!(!matches!(
            decode_delta_catalog(&trigger_catalog).unwrap().base,
            CatalogBase::Sharded { .. }
        ));

        let before_commit = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &old_catalog,
        )
        .await
        .unwrap();
        assert_eq!(
            before_commit.get(&old.digest).await.unwrap(),
            Some(old_bytes.clone())
        );
        assert_eq!(before_commit.get(&added.digest).await.unwrap(), None);

        writer.finish_state_catalog(true).unwrap();
        // The triggering commit is immediately readable while immutable shard
        // construction proceeds independently.
        let after_commit = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &trigger_catalog,
        )
        .await
        .unwrap();
        assert_eq!(
            after_commit.get(&added.digest).await.unwrap(),
            Some(added_bytes.clone())
        );

        let (later, later_bytes) = chunk(b"catalog write after rebase prepare");
        writer
            .put(later.clone(), later_bytes.clone())
            .await
            .unwrap();
        writer.wait_for_background_catalog_rebase().await.unwrap();
        let rebased_catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
        assert!(matches!(
            decode_delta_catalog(&rebased_catalog).unwrap().base,
            CatalogBase::Sharded { .. }
        ));

        let (post_prepare, post_prepare_bytes) = chunk(b"catalog write after install prepare");
        writer
            .put(post_prepare.clone(), post_prepare_bytes.clone())
            .await
            .unwrap();
        writer.finish_state_catalog(true).unwrap();
        assert!(writer.read_stats().index_sharded_base);
        assert_eq!(
            writer.get(&later.digest).await.unwrap(),
            Some(later_bytes.clone())
        );
        assert_eq!(
            writer.get(&post_prepare.digest).await.unwrap(),
            Some(post_prepare_bytes.clone())
        );

        let committed = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &rebased_catalog,
        )
        .await
        .unwrap();
        assert_eq!(
            committed.get(&later.digest).await.unwrap(),
            Some(later_bytes.clone())
        );
        assert_eq!(committed.get(&post_prepare.digest).await.unwrap(), None);

        let next_catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(true).unwrap();
        let next = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &next_catalog)
            .await
            .unwrap();
        assert_eq!(next.get(&old.digest).await.unwrap(), Some(old_bytes));
        assert_eq!(next.get(&added.digest).await.unwrap(), Some(added_bytes));
        assert_eq!(next.get(&later.digest).await.unwrap(), Some(later_bytes));
        assert_eq!(
            next.get(&post_prepare.digest).await.unwrap(),
            Some(post_prepare_bytes)
        );
    }

    #[tokio::test]
    async fn failed_catalog_deletion_requires_owned_claims_for_recovery() {
        use crate::metadata::PinStore;
        let fault = Arc::new(FailingCatalogPutStore::new());
        let objects: Arc<dyn ObjectStore> = fault.clone();
        let base = Path::from("catalog-claim-recovery");
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &PackedChunks::empty_state_catalog().unwrap(),
        )
        .await
        .unwrap();
        let bytes = Bytes::from_static(b"orphan catalog object");
        let path = sharded_path(&base, INDEXES_KIND, &Digest::from(blake3::hash(&bytes)));
        put_object(&objects, &path, bytes, true).await.unwrap();
        let ledger = Arc::new(crate::metadata::MemoryPinStore::default());
        fault.fail_catalog_delete(1);
        assert!(
            writer
                .reclaim_catalog_objects_pinned(ledger.clone(), BTreeSet::new())
                .await
                .is_err()
        );
        let failed = ledger.inventory().await.unwrap();
        assert_eq!(failed.deletions.len(), 1);
        assert!(failed.deletions.values().next().unwrap().contains(
            &crate::metadata::PinResource::StorageObject(path.to_string())
        ));
        assert!(objects.head(&path).await.is_ok());
        fault.disarm();
        assert_eq!(
            writer
                .reclaim_catalog_objects_pinned(ledger.clone(), BTreeSet::new())
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        // The injected failure has returned and issued no remote request.
        // The owning collector can retry using its original exact claim.
        let owned = failed.deletions.keys().cloned().collect::<BTreeSet<_>>();
        writer
            .reclaim_catalog_objects_pinned(ledger.clone(), owned.clone())
            .await
            .unwrap();
        assert!(objects.head(&path).await.is_err());
        assert_eq!(
            ledger.inventory().await.unwrap().deletions,
            failed.deletions
        );
        // The collector retains adopted claims through its other cleanup too.
        for token in owned {
            ledger.finish_deletions(&token).await.unwrap();
        }
        assert!(ledger.inventory().await.unwrap().deletions.is_empty());
    }

    #[tokio::test]
    async fn deferred_pack_cleanup_preserves_historical_catalogs_and_reclaims_unrelated_uploads() {
        use crate::metadata::{DataPin, PinScope, PinStore};
        for sharded in [false, true] {
            let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let base = Path::from("pinned-retired-packs");
            let writer = PackedChunks::open_with_state_catalog(
                objects.clone(),
                base.clone(),
                u64::MAX,
                0,
                &PackedChunks::empty_state_catalog().unwrap(),
            )
            .await
            .unwrap();
            if sharded {
                writer.set_catalog_rebase_run_bytes_for_test(1);
            }
            let (live, live_bytes) = chunk(b"current reader's live bytes");
            let (old, old_bytes) = chunk(b"historical reader's bytes");
            writer.put(live.clone(), live_bytes.clone()).await.unwrap();
            writer.put(old.clone(), old_bytes.clone()).await.unwrap();
            let mut catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
            writer.finish_state_catalog(true).unwrap();
            if sharded {
                writer.wait_for_background_catalog_rebase().await.unwrap();
                catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
                writer.finish_state_catalog(true).unwrap();
                assert!(matches!(
                    decode_delta_catalog(&catalog).unwrap().base,
                    CatalogBase::Sharded { .. }
                ));
            }
            let old_pack = writer.location(&old.digest).await.unwrap().unwrap().pack;
            let old_path = pack_path(&base, &old_pack);
            let reader = PackedChunks::open_with_state_catalog(
                objects.clone(),
                base.clone(),
                u64::MAX,
                0,
                &catalog,
            )
            .await
            .unwrap();
            let ledger = Arc::new(crate::metadata::MemoryPinStore::default());
            let collector = ledger.acquire_collection(None).await.unwrap().unwrap();
            let early_mark = writer
                .payload_pin_mark(ledger.clone(), BTreeSet::new())
                .await
                .unwrap();
            let pin = ledger
                .register(DataPin {
                    scope: PinScope::Snapshot {
                        generation: u64::MAX,
                    },
                    catalog: Some(catalog),
                    resources: BTreeSet::new(),
                })
                .await
                .unwrap()
                .unwrap();
            writer.delete_many(&[old.digest]).await.unwrap();
            writer.finish_deletions(true).await.unwrap();
            writer.prepare_state_catalog().await.unwrap().unwrap();
            writer.finish_state_catalog(true).unwrap();
            assert_ne!(
                writer.location(&live.digest).await.unwrap().unwrap().pack,
                old_pack
            );
            let garbage = Bytes::from_static(b"unpublished pack upload");
            let garbage_path = pack_path(&base, &PackId::new(blake3::hash(&garbage).into()));
            put_object(&objects, &garbage_path, garbage, true)
                .await
                .unwrap();
            writer
                .finish_collection_inner(true, Some(&early_mark))
                .await
                .unwrap();
            assert!(objects.head(&old_path).await.is_ok());
            assert!(
                writer
                    .published_retirements
                    .lock()
                    .unwrap()
                    .contains(&old_path)
            );
            assert!(ledger.inventory().await.unwrap().deletions.is_empty());
            ledger.finish_collection(&collector).await.unwrap();
            let collector = ledger.acquire_collection(None).await.unwrap().unwrap();
            writer
                .finish_collection_pinned(true, ledger.clone(), BTreeSet::new())
                .await
                .unwrap();
            assert!(objects.head(&garbage_path).await.is_err());
            assert!(objects.head(&old_path).await.is_ok());
            assert_eq!(reader.get(&old.digest).await.unwrap(), Some(old_bytes));
            assert!(
                writer
                    .published_retirements
                    .lock()
                    .unwrap()
                    .contains(&old_path)
            );
            drop(reader);
            ledger.release(&pin).await.unwrap();
            // Releasing during this pass retains history until collector exit.
            writer
                .finish_collection_pinned(true, ledger.clone(), BTreeSet::new())
                .await
                .unwrap();
            assert!(objects.head(&old_path).await.is_ok());
            ledger.finish_collection(&collector).await.unwrap();
            let collector = ledger
                .begin_collection(ledger.inventory().await.unwrap().revision, None)
                .await
                .unwrap()
                .unwrap();
            writer
                .finish_collection_pinned(true, ledger.clone(), BTreeSet::new())
                .await
                .unwrap();
            assert!(objects.head(&old_path).await.is_err());
            assert_eq!(writer.get(&live.digest).await.unwrap(), Some(live_bytes));
            ledger.finish_collection(&collector).await.unwrap();
        }
    }

    #[test]
    fn retirement_queue_restores_pending_and_unvisited_paths_without_replaying_finished_batches() {
        let original: HashSet<_> = (0..2501)
            .map(|n| Path::from(format!("retired/{n}")))
            .collect();
        let ready = StdMutex::new(original.clone());
        let mut cleanup = RetirementCleanup {
            ready: &ready,
            paths: std::mem::take(&mut *ready.lock().unwrap()).into_iter(),
            pending: Vec::new(),
        };
        assert!(cleanup.next_batch());
        assert_eq!(cleanup.pending.len(), 1000);
        let held = cleanup.pending[0].clone();
        let removed: HashSet<_> = cleanup.pending.iter().skip(1).cloned().collect();
        cleanup.finish_batch(vec![held.clone()]);
        let concurrent = Path::from("new/retirement");
        ready.lock().unwrap().insert(concurrent.clone());
        assert!(cleanup.next_batch());
        assert_eq!(cleanup.pending.len(), 1000);
        assert_eq!(cleanup.paths.len(), 501);
        drop(cleanup); // Cancellation/error during the second batch.
        let mut expected: HashSet<_> = original.difference(&removed).cloned().collect();
        expected.insert(concurrent);
        assert_eq!(*ready.lock().unwrap(), expected);
        assert!(ready.lock().unwrap().contains(&held));
    }

    #[tokio::test]
    async fn payload_cleanup_visits_an_all_held_queue_once_and_reclaims_after_release() {
        use crate::metadata::{DataPin, PinResource, PinScope, PinStore};
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            Path::default(),
            u64::MAX,
            0,
            &PackedChunks::empty_state_catalog().unwrap(),
        )
        .await
        .unwrap();
        let paths: HashSet<_> = (0..2501usize)
            .map(|n| sharded_path(&Path::default(), "blobs", &Digest::hash(&n.to_le_bytes())))
            .collect();
        for path in &paths {
            put_object(&objects, path, Bytes::from_static(b"held"), true)
                .await
                .unwrap();
        }
        writer
            .published_retirements
            .lock()
            .unwrap()
            .extend(paths.iter().cloned());
        let ledger = Arc::new(crate::metadata::MemoryPinStore::default());
        let pin = ledger
            .register(DataPin {
                scope: PinScope::Staging,
                catalog: None,
                resources: paths
                    .iter()
                    .map(|p| PinResource::StorageObject(p.to_string()))
                    .collect(),
            })
            .await
            .unwrap()
            .unwrap();
        let collector = ledger.acquire_collection(None).await.unwrap().unwrap();
        let before = ledger.inventory().await.unwrap().revision;
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            writer.finish_collection_pinned(false, ledger.clone(), BTreeSet::new()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(ledger.inventory().await.unwrap().revision, before);
        assert_eq!(*writer.published_retirements.lock().unwrap(), paths);
        ledger.release(&pin).await.unwrap();
        ledger.finish_collection(&collector).await.unwrap();
        let collector = ledger.acquire_collection(None).await.unwrap().unwrap();
        writer
            .finish_collection_pinned(false, ledger.clone(), BTreeSet::new())
            .await
            .unwrap();
        assert!(writer.published_retirements.lock().unwrap().is_empty());
        assert!(
            objects
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );
        ledger.finish_collection(&collector).await.unwrap();
    }

    #[tokio::test]
    async fn catalog_cleanup_defers_changed_pins_until_a_fresh_mark() {
        use crate::metadata::{DataPin, PinScope, PinStore};
        let fault = Arc::new(FailingCatalogPutStore::new());
        let objects: Arc<dyn ObjectStore> = fault.clone();
        let base = Path::from("catalog-cleanup-new-pin");
        let empty = PackedChunks::empty_state_catalog().unwrap();
        let writer = Arc::new(
            PackedChunks::open_with_state_catalog(
                objects.clone(),
                base.clone(),
                u64::MAX,
                0,
                &empty,
            )
            .await
            .unwrap(),
        );
        let catalog = writer.externalize_state_catalog(&empty).await.unwrap();
        writer
            .synchronize_state_catalog(Some(&catalog))
            .await
            .unwrap();
        for index in 0..1001usize {
            let bytes = Bytes::copy_from_slice(&index.to_le_bytes());
            let path = sharded_path(&base, INDEXES_KIND, &Digest::from(blake3::hash(&bytes)));
            put_object(&objects, &path, bytes, true).await.unwrap();
        }
        let marker = base.clone().join(INDEX_RECLAIM_MARKER_NAME);
        put_object(
            &objects,
            &marker,
            Bytes::from_static(b"cleanup pending"),
            true,
        )
        .await
        .unwrap();
        let ledger = Arc::new(crate::metadata::MemoryPinStore::default());
        let collector = ledger.acquire_collection(None).await.unwrap().unwrap();
        fault.delete_fail_at.store(0, Ordering::SeqCst);
        let task = tokio::spawn({
            let writer = writer.clone();
            let ledger = ledger.clone();
            async move {
                writer
                    .reclaim_catalog_metadata_pinned(ledger, BTreeSet::new())
                    .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while fault.catalog_deletes.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let pin = ledger
            .register(DataPin {
                scope: PinScope::Staging,
                catalog: Some(catalog.clone()),
                resources: BTreeSet::new(),
            })
            .await
            .unwrap()
            .unwrap();
        fault.disarm();
        fault.delete_resume.notify_one();
        task.await.unwrap().unwrap();
        assert!(ledger.inventory().await.unwrap().deletions.is_empty());
        assert!(objects.head(&marker).await.is_ok());
        let prefix = kind_prefix(&base, INDEXES_KIND);
        assert!(
            objects
                .list(Some(&prefix))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len()
                > 1
        );
        writer.resolve_state_catalog(&catalog).await.unwrap();
        writer
            .reclaim_catalog_metadata_pinned(ledger.clone(), BTreeSet::new())
            .await
            .unwrap();
        assert_eq!(
            objects
                .list(Some(&prefix))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            1
        );
        writer.resolve_state_catalog(&catalog).await.unwrap();
        ledger.release(&pin).await.unwrap();
        ledger.finish_collection(&collector).await.unwrap();
    }

    #[tokio::test]
    async fn payload_cleanup_cancellation_keeps_inflight_deletion_tracked() {
        use crate::metadata::PinStore;
        let fault = Arc::new(FailingCatalogPutStore::new());
        let objects: Arc<dyn ObjectStore> = fault.clone();
        let base = Path::from("payload-cleanup-cancellation");
        let writer = Arc::new(
            PackedChunks::open_with_state_catalog(
                objects.clone(),
                base.clone(),
                u64::MAX,
                0,
                &PackedChunks::empty_state_catalog().unwrap(),
            )
            .await
            .unwrap(),
        );
        let bytes = Bytes::from_static(b"retired payload awaiting deletion");
        let path = sharded_path(&base, "blobs", &Digest::from(blake3::hash(&bytes)));
        put_object(&objects, &path, bytes, true).await.unwrap();
        writer
            .published_retirements
            .lock()
            .unwrap()
            .insert(path.clone());
        for index in 0..1000usize {
            let bytes = Bytes::copy_from_slice(&index.to_le_bytes());
            let extra = sharded_path(&base, "blobs", &Digest::from(blake3::hash(&bytes)));
            put_object(&objects, &extra, bytes, true).await.unwrap();
            writer.published_retirements.lock().unwrap().insert(extra);
        }
        let ledger = Arc::new(crate::metadata::MemoryPinStore::default());
        let collector = ledger.acquire_collection(None).await.unwrap().unwrap();
        fault.delete_fail_at.store(0, Ordering::SeqCst);
        let task = tokio::spawn({
            let writer = writer.clone();
            let ledger = ledger.clone();
            async move {
                writer
                    .finish_collection_pinned(true, ledger, BTreeSet::new())
                    .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while fault.catalog_deletes.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let inventory = ledger.inventory().await.unwrap();
        let claimed = inventory.deletions.values().next().unwrap();
        assert_eq!(claimed.len(), 1000);
        use crate::metadata::{DataPin, PinResource, PinScope};
        assert!(
            ledger
                .register(DataPin {
                    scope: PinScope::Staging,
                    catalog: None,
                    resources: claimed.clone(),
                })
                .await
                .unwrap()
                .is_none(),
            "overlapping admission must fail immediately"
        );
        let unrelated = ledger
            .register(DataPin {
                scope: PinScope::Staging,
                catalog: None,
                resources: BTreeSet::from([PinResource::StorageObject("unrelated/path".into())]),
            })
            .await
            .unwrap()
            .unwrap();
        let metadata = ledger
            .register(DataPin {
                scope: PinScope::Metadata,
                catalog: None,
                resources: BTreeSet::new(),
            })
            .await
            .unwrap()
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(ledger.inventory().await.unwrap().deletions.len(), 1);
        assert!(ledger.finish_collection(&collector).await.is_err());
        assert!(objects.head(&path).await.is_ok());
        fault.disarm();
        fault.delete_resume.notify_one();
        crate::metadata::flush_repository_leases().await.unwrap();
        // The cancelled caller leaves the second batch for a fresh pass.
        assert_eq!(
            objects
                .list(Some(&base))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(ledger.inventory().await.unwrap().deletions.is_empty());
        // Cancellation left the retired path queued; retry settles it too.
        writer
            .finish_collection_pinned(true, ledger.clone(), BTreeSet::new())
            .await
            .unwrap();
        assert!(writer.published_retirements.lock().unwrap().is_empty());
        assert!(objects.head(&path).await.is_err());
        ledger.release(&unrelated).await.unwrap();
        ledger.release(&metadata).await.unwrap();
        ledger.finish_collection(&collector).await.unwrap();
    }

    #[tokio::test]
    async fn payload_cleanup_defers_late_pins_and_finishes_without_losing_protection() {
        use crate::metadata::{DataPin, PinResource, PinScope, PinStore};
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("payload-cleanup-late-pin");
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &PackedChunks::empty_state_catalog().unwrap(),
        )
        .await
        .unwrap();
        let bytes = Bytes::from_static(b"payload protected after the cleanup mark");
        let path = sharded_path(&base, "blobs", &Digest::from(blake3::hash(&bytes)));
        put_object(&objects, &path, bytes.clone(), true)
            .await
            .unwrap();
        writer
            .published_retirements
            .lock()
            .unwrap()
            .insert(path.clone());
        let ledger = Arc::new(crate::metadata::MemoryPinStore::default());
        let collector = ledger.acquire_collection(None).await.unwrap().unwrap();
        let mark = writer
            .payload_pin_mark(ledger.clone(), BTreeSet::new())
            .await
            .unwrap();
        let pin = ledger
            .register(DataPin {
                scope: PinScope::Staging,
                catalog: None,
                resources: BTreeSet::from([PinResource::StorageObject(path.to_string())]),
            })
            .await
            .unwrap()
            .unwrap();
        let retired = ledger
            .register(DataPin {
                scope: PinScope::Staging,
                catalog: None,
                resources: BTreeSet::from([PinResource::StorageObject(
                    "other retired protection".into(),
                )]),
            })
            .await
            .unwrap()
            .unwrap();
        ledger.release(&retired).await.unwrap();
        let fence = ledger
            .begin_prune(ledger.inventory().await.unwrap().revision)
            .await
            .unwrap()
            .unwrap();
        // The same stale mark must still fail while an emergency prune fence
        // is active; only ordinary cleanup can defer it.
        assert_eq!(
            writer
                .finish_collection_inner(true, Some(&mark))
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(ledger.finish_collection(&collector).await.is_err());
        ledger.finish_prune(&fence).await.unwrap();
        writer
            .finish_collection_inner(true, Some(&mark))
            .await
            .unwrap();
        assert!(writer.published_retirements.lock().unwrap().contains(&path));
        assert!(ledger.inventory().await.unwrap().deletions.is_empty());
        ledger.finish_collection(&collector).await.unwrap();
        let inventory = ledger.inventory().await.unwrap();
        assert!(inventory.collector.is_none());
        assert!(!inventory.pins.contains_key(&retired));
        assert!(inventory.pins.contains_key(&pin));
        assert_eq!(
            objects.get(&path).await.unwrap().bytes().await.unwrap(),
            bytes
        );

        let collector = ledger.acquire_collection(None).await.unwrap().unwrap();
        writer
            .finish_collection_pinned(true, ledger.clone(), BTreeSet::new())
            .await
            .unwrap();
        assert!(objects.head(&path).await.is_ok());
        ledger.release(&pin).await.unwrap();
        ledger.finish_collection(&collector).await.unwrap();
        let collector = ledger.acquire_collection(None).await.unwrap().unwrap();
        writer
            .finish_collection_pinned(true, ledger.clone(), BTreeSet::new())
            .await
            .unwrap();
        assert!(objects.head(&path).await.is_err());
        assert!(writer.published_retirements.lock().unwrap().is_empty());
        ledger.finish_collection(&collector).await.unwrap();
    }

    #[tokio::test]
    async fn payload_cleanup_keeps_failed_deletion_claims_and_rejects_lost_ownership() {
        use crate::metadata::PinStore;
        let fault = Arc::new(FailingCatalogPutStore::new());
        let objects: Arc<dyn ObjectStore> = fault.clone();
        let base = Path::from("payload-cleanup-failure");
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &PackedChunks::empty_state_catalog().unwrap(),
        )
        .await
        .unwrap();
        let bytes = Bytes::from_static(b"retired payload");
        let path = sharded_path(&base, "blobs", &Digest::from(blake3::hash(&bytes)));
        put_object(&objects, &path, bytes, true).await.unwrap();
        writer
            .published_retirements
            .lock()
            .unwrap()
            .insert(path.clone());
        let second = sharded_path(&base, "blobs", &Digest::from(blake3::hash(b"second")));
        put_object(&objects, &second, Bytes::from_static(b"second"), true)
            .await
            .unwrap();
        writer
            .published_retirements
            .lock()
            .unwrap()
            .insert(second.clone());
        let ledger = Arc::new(crate::metadata::MemoryPinStore::default());
        let collector = ledger.acquire_collection(None).await.unwrap().unwrap();
        let mark = writer
            .payload_pin_mark(ledger.clone(), BTreeSet::new())
            .await
            .unwrap();
        let replacement = ledger
            .acquire_collection(Some(collector))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            writer
                .finish_collection_inner(true, Some(&mark))
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(objects.head(&path).await.is_ok());
        fault.fail_catalog_delete(2);
        assert!(
            writer
                .finish_collection_pinned(true, ledger.clone(), BTreeSet::new())
                .await
                .is_err()
        );
        let failed = ledger.inventory().await.unwrap();
        assert_eq!(failed.deletions.len(), 1);
        assert_eq!(
            failed.deletions.values().next().unwrap().len(),
            2,
            "the partial failure must retain the whole batch claim"
        );
        assert!(
            writer
                .published_retirements
                .lock()
                .unwrap()
                .contains(&second)
        );
        assert!(writer.published_retirements.lock().unwrap().contains(&path));
        assert!(ledger.finish_collection(&replacement).await.is_err());
        fault.disarm();
        assert_eq!(
            writer
                .finish_collection_pinned(true, ledger.clone(), BTreeSet::new())
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        let owned = failed.deletions.keys().cloned().collect::<BTreeSet<_>>();
        writer
            .finish_collection_pinned(true, ledger.clone(), owned.clone())
            .await
            .unwrap();
        assert!(objects.head(&path).await.is_err());
        assert!(objects.head(&second).await.is_err());
        assert!(writer.published_retirements.lock().unwrap().is_empty());
        assert_eq!(
            ledger.inventory().await.unwrap().deletions,
            failed.deletions
        );
        for token in owned {
            ledger.finish_deletions(&token).await.unwrap();
        }
        ledger.finish_collection(&replacement).await.unwrap();
    }

    #[tokio::test]
    async fn payload_cleanup_bounds_claim_batches_across_the_batch_limit() {
        use crate::metadata::PinStore;
        for orphan in [false, true] {
            for count in [999usize, 1001] {
                let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
                let base = Path::from("retired-batch-boundary");
                let writer = PackedChunks::open_with_state_catalog(
                    objects.clone(),
                    base.clone(),
                    u64::MAX,
                    0,
                    &PackedChunks::empty_state_catalog().unwrap(),
                )
                .await
                .unwrap();
                let mut paths = Vec::new();
                for index in 0..count {
                    let bytes = Bytes::copy_from_slice(&index.to_le_bytes());
                    let path = sharded_path(&base, "blobs", &Digest::from(blake3::hash(&bytes)));
                    put_object(&objects, &path, bytes, true).await.unwrap();
                    paths.push(path);
                }
                if !orphan {
                    writer
                        .published_retirements
                        .lock()
                        .unwrap()
                        .extend(paths.iter().cloned());
                }
                let ledger = Arc::new(crate::metadata::MemoryPinStore::default());
                let collector = ledger.acquire_collection(None).await.unwrap().unwrap();
                let before = ledger.inventory().await.unwrap().revision;
                if orphan {
                    let mark = writer
                        .payload_pin_mark(ledger.clone(), BTreeSet::new())
                        .await
                        .unwrap();
                    writer
                        .reclaim_unpublished_payloads(Some(&mark))
                        .await
                        .unwrap();
                } else {
                    writer
                        .finish_collection_pinned(false, ledger.clone(), BTreeSet::new())
                        .await
                        .unwrap();
                }
                let after = ledger.inventory().await.unwrap();
                assert_eq!(
                    after.revision - before,
                    2 * count.div_ceil(1000) as u64,
                    "one claim/release pair per bounded batch, not per file"
                );
                assert!(after.deletions.is_empty());
                assert!(writer.published_retirements.lock().unwrap().is_empty());
                for path in paths {
                    assert!(objects.head(&path).await.is_err());
                }
                ledger.finish_collection(&collector).await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn payload_mark_deduplicates_shared_shards_without_losing_root_additions() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("shared-payload-mark");
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &PackedChunks::empty_state_catalog().unwrap(),
        )
        .await
        .unwrap();
        let mut index = Index {
            manifests_complete: true,
            ..Index::default()
        };
        let mut expected = HashSet::new();
        let mut maps = Vec::new();
        let mut catalogs = Vec::new();
        for generation in 1..=2 {
            for entry in 0..if generation == 1 { 32 } else { 1 } {
                let id = |kind| Digest::hash(format!("{generation}/{entry}/{kind}").as_bytes());
                let pack = PackId::new(id("pack"));
                let chunk = ChunkId::new(id("chunk"));
                let blob = BlobId::new(id("blob"));
                index.add_pack(
                    pack,
                    100 + 8 + PACK_ENTRY_LEN as u64 + PACK_TRAILER_LEN as u64,
                    vec![PackEntry {
                        digest: chunk,
                        offset: 0,
                        framed_len: 100,
                        uncompressed_len: 100,
                    }],
                );
                index.manifests.insert(blob);
                expected.insert(pack_path(&base, &pack));
                expected.insert(sharded_path(&base, "bao", chunk.as_digest()));
                for kind in ["blobs", "bao"] {
                    expected.insert(sharded_path(&base, kind, blob.as_digest()));
                }
            }
            let encoded = encode_index_shards(&index, 4).unwrap();
            maps.push(decode_shard_map(&encoded.map).unwrap());
            for (digest, bytes) in encoded.objects {
                put_object(
                    &objects,
                    &sharded_path(&base, INDEXES_KIND, &digest),
                    bytes,
                    true,
                )
                .await
                .unwrap();
            }
            put_object(
                &objects,
                &sharded_path(&base, INDEXES_KIND, &encoded.map_digest),
                encoded.map,
                true,
            )
            .await
            .unwrap();
            let blob = BlobId::new(Digest::hash(format!("inline-{generation}").as_bytes()));
            for kind in ["blobs", "bao"] {
                expected.insert(sharded_path(&base, kind, blob.as_digest()));
            }
            let mut inline = Index::default();
            inline.manifests.insert(blob);
            let mut mutations = IndexMutations::default();
            mutations.record_manifest_add(blob);
            catalogs.push(
                encode_delta_catalog(&DeltaCatalog {
                    sidecars: None,
                    generation,
                    base: CatalogBase::Sharded {
                        root: encoded.map_digest,
                        shard_bits: 4,
                    },
                    runs: BTreeMap::new(),
                    deltas: vec![encode_index_mutations(&inline, &mutations).unwrap()],
                })
                .unwrap(),
            );
        }
        assert!(
            maps[0]
                .packs
                .iter()
                .any(|reference| maps[1].packs.contains(reference))
        );
        assert!(
            maps[1]
                .packs
                .iter()
                .any(|reference| !maps[0].packs.contains(reference))
        );
        let mut retained = HashSet::new();
        let mut shards = PayloadShardMarks::default();
        for catalog in &catalogs {
            writer
                .mark_catalog_payloads(catalog, &mut retained, &mut shards)
                .await
                .unwrap();
        }
        assert_eq!(retained, expected);
        assert_eq!(
            shards.packs,
            maps.iter()
                .flat_map(|map| map.packs.iter().copied())
                .collect()
        );
        assert_eq!(
            shards.manifests,
            maps.iter()
                .flat_map(|map| map.manifests.iter().copied())
                .collect()
        );
        // The visited references must not survive the marking pass.
        let mut again = HashSet::new();
        let mut fresh = PayloadShardMarks::default();
        for catalog in &catalogs {
            writer
                .mark_catalog_payloads(catalog, &mut again, &mut fresh)
                .await
                .unwrap();
        }
        assert_eq!(again, expected);

        // Even with the original references and bytes already cached, a map
        // with conflicting metadata must not take the deduplication shortcut.
        for fault in [
            "pack-prefix",
            "pack-count",
            "pack-length",
            "manifest-prefix",
            "manifest-length",
        ] {
            let mut map = maps[0].clone();
            match fault {
                "pack-prefix" => map.packs[0].prefix ^= 1,
                "pack-count" => map.packs[0].entries += 1,
                "pack-length" => map.packs[0].encoded_bytes += 1,
                "manifest-prefix" => map.manifests[0].prefix ^= 1,
                "manifest-length" => map.manifests[0].encoded_bytes += 1,
                _ => unreachable!(),
            }
            // A single reference avoids unrelated map ordering errors.
            map.packs.truncate(1);
            map.manifests.truncate(1);
            let bytes = encode_shard_map(&map).unwrap();
            let digest = Digest::hash(&bytes);
            put_object(
                &objects,
                &sharded_path(&base, INDEXES_KIND, &digest),
                bytes,
                true,
            )
            .await
            .unwrap();
            let catalog = encode_delta_catalog(&DeltaCatalog {
                sidecars: None,
                generation: 3,
                base: CatalogBase::Sharded {
                    root: digest,
                    shard_bits: 4,
                },
                runs: BTreeMap::new(),
                deltas: Vec::new(),
            })
            .unwrap();
            assert!(
                writer
                    .mark_catalog_payloads(&catalog, &mut retained, &mut shards)
                    .await
                    .is_err(),
                "{fault}"
            );
        }
    }

    #[tokio::test]
    #[ignore = "performance corpus; benchmark run catalog-marking"]
    async fn benchmark_catalog_marking() {
        use crate::metadata::{DataPin, PinScope, PinStore};
        fn rss_bytes() -> Option<u64> {
            std::fs::read_to_string("/proc/self/status")
                .ok()?
                .lines()
                .find_map(|line| {
                    line.strip_prefix("VmRSS:")?
                        .split_whitespace()
                        .next()?
                        .parse::<u64>()
                        .ok()
                })
                .map(|kb| kb * 1024)
        }
        let count: usize = std::env::var("CASITA_BENCH_MARK_ENTRIES")
            .unwrap()
            .parse()
            .unwrap();
        let holds: usize = std::env::var("CASITA_BENCH_MARK_HOLDS")
            .unwrap()
            .parse()
            .unwrap();
        let mode = std::env::var("CASITA_BENCH_MARK_MODE").unwrap();
        assert!(count > 0 && holds > 0);
        assert!(["identical", "overlap", "disjoint"].contains(&mode.as_str()));
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("catalog-marking");
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &PackedChunks::empty_state_catalog().unwrap(),
        )
        .await
        .unwrap();
        let ledger = Arc::new(crate::metadata::MemoryPinStore::default());
        let collector = ledger.acquire_collection(None).await.unwrap().unwrap();
        let mut expected = HashSet::new();
        let mut roots = Vec::new();
        for group in 0..if mode == "disjoint" { holds } else { 1 } {
            let mut index = Index {
                manifests_complete: true,
                ..Index::default()
            };
            for entry in 0..count {
                let id = |kind| Digest::hash(format!("{group}/{entry}/{kind}").as_bytes());
                let pack = PackId::new(id("pack"));
                let chunk = ChunkId::new(id("chunk"));
                let blob = BlobId::new(id("blob"));
                index.add_pack(
                    pack,
                    100 + 8 + PACK_ENTRY_LEN as u64 + PACK_TRAILER_LEN as u64,
                    vec![PackEntry {
                        digest: chunk,
                        offset: 0,
                        framed_len: 100,
                        uncompressed_len: 100,
                    }],
                );
                index.manifests.insert(blob);
                expected.insert(pack_path(&base, &pack));
                expected.insert(sharded_path(&base, "bao", chunk.as_digest()));
                expected.insert(sharded_path(&base, "bao", blob.as_digest()));
                expected.insert(sharded_path(&base, "blobs", blob.as_digest()));
            }
            let encoded = encode_index_shards(&index, 4).unwrap();
            for (digest, bytes) in encoded.objects {
                put_object(
                    &objects,
                    &sharded_path(&base, INDEXES_KIND, &digest),
                    bytes,
                    true,
                )
                .await
                .unwrap();
            }
            put_object(
                &objects,
                &sharded_path(&base, INDEXES_KIND, &encoded.map_digest),
                encoded.map,
                true,
            )
            .await
            .unwrap();
            roots.push(encoded.map_digest);
        }
        for hold in 0..holds {
            let catalog = encode_delta_catalog(&DeltaCatalog {
                sidecars: None,
                generation: if mode == "identical" {
                    1
                } else {
                    hold as u64 + 1
                },
                base: CatalogBase::Sharded {
                    root: roots[if mode == "disjoint" { hold } else { 0 }],
                    shard_bits: 4,
                },
                runs: BTreeMap::new(),
                deltas: Vec::new(),
            })
            .unwrap();
            ledger
                .register(DataPin {
                    scope: PinScope::Snapshot {
                        generation: u64::MAX,
                    },
                    catalog: Some(catalog.to_vec()),
                    resources: BTreeSet::new(),
                })
                .await
                .unwrap()
                .unwrap();
        }
        writer.reset_read_stats();
        let rss_before = rss_bytes();
        let started = Instant::now();
        let mark = writer
            .payload_pin_mark(ledger.clone(), BTreeSet::new())
            .await
            .unwrap();
        let nanos = started.elapsed().as_nanos();
        let rss_after = rss_bytes();
        assert_eq!(mark.retained, expected);
        assert_eq!(mark.retained.len(), count * 4 * roots.len());
        let path_bytes: usize = mark.retained.iter().map(|path| path.as_ref().len()).sum();
        let stats = writer.read_stats();
        println!(
            "catalog_marking_sample {}",
            serde_json::json!({
                "count": count, "holds": holds, "mode": mode, "nanos": nanos,
                "rss_before": rss_before, "rss_after": rss_after,
                "paths": mark.retained.len(), "path_bytes": path_bytes,
                "index_requests": stats.index_requests, "index_bytes": stats.index_bytes,
                "correctness": "exact union of pack, chunk outboard, blob and blob outboard paths",
            })
        );
        ledger.finish_collection(&collector).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "performance corpus; benchmark run cleanup-batches"]
    async fn benchmark_retirement_queue_memory() {
        fn rss_bytes() -> Option<u64> {
            std::fs::read_to_string("/proc/self/status")
                .ok()?
                .lines()
                .find_map(|line| {
                    line.strip_prefix("VmRSS:")?
                        .split_whitespace()
                        .next()?
                        .parse::<u64>()
                        .ok()
                })
                .map(|kb| kb * 1024)
        }
        let count: usize = std::env::var("CASITA_BENCH_CLEANUP_PATHS")
            .unwrap()
            .parse()
            .unwrap();
        assert!(count > 0);
        let fault = Arc::new(FailingCatalogPutStore::new());
        let objects: Arc<dyn ObjectStore> = fault.clone();
        let writer = Arc::new(
            PackedChunks::open_with_state_catalog(
                objects,
                Path::from("queue-memory"),
                u64::MAX,
                0,
                &PackedChunks::empty_state_catalog().unwrap(),
            )
            .await
            .unwrap(),
        );
        for index in 0..count {
            let path = sharded_path(&writer.base, "blobs", &Digest::hash(&index.to_le_bytes()));
            writer.published_retirements.lock().unwrap().insert(path);
        }
        let rss_before = rss_bytes();
        fault.delete_fail_at.store(0, Ordering::SeqCst);
        let start = std::time::Instant::now();
        let task = tokio::spawn({
            let writer = writer.clone();
            async move { writer.finish_collection_inner(false, None).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            while fault.catalog_deletes.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let rss_at_first_delete = rss_bytes();
        fault.disarm();
        fault.delete_resume.notify_one();
        task.await.unwrap().unwrap();
        let nanos = start.elapsed().as_nanos();
        assert_eq!(fault.catalog_deletes.load(Ordering::SeqCst), count as u64);
        assert!(writer.published_retirements.lock().unwrap().is_empty());
        println!(
            "retirement_queue_sample {}",
            serde_json::json!({
                "count": count, "nanos": nanos, "rss_before": rss_before,
                "rss_at_first_delete": rss_at_first_delete,
                "correctness": "every candidate visited once; queue empty",
            })
        );
    }

    #[tokio::test]
    #[ignore = "performance corpus; benchmark run cleanup-batches"]
    async fn benchmark_cleanup_batch_boundary() {
        use crate::metadata::{DataPin, FilePinStore, PinResource, PinScope, PinStore};
        let count: usize = std::env::var("CASITA_BENCH_CLEANUP_PATHS")
            .unwrap()
            .parse()
            .unwrap();
        assert!(count > 0);
        let temp = tempfile::tempdir().unwrap();
        let fs = object_store::local::LocalFileSystem::new_with_prefix(temp.path()).unwrap();
        let durability = LocalDurability::new(fs.clone(), temp.path()).unwrap();
        let objects: Arc<dyn ObjectStore> = Arc::new(fs);
        let base = Path::from("payloads");
        let writer = PackedChunks::open_with_initial_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            Some(&PackedChunks::empty_state_catalog().unwrap()),
            Some(durability),
        )
        .await
        .unwrap();
        let mut paths = Vec::new();
        for index in 0..count {
            let bytes = Bytes::copy_from_slice(&index.to_le_bytes());
            let path = sharded_path(&base, "blobs", &Digest::from(blake3::hash(&bytes)));
            put_object(&objects, &path, bytes, true).await.unwrap();
            paths.push(path);
        }
        writer
            .published_retirements
            .lock()
            .unwrap()
            .extend(paths.iter().cloned());
        let held = sharded_path(&base, "blobs", &Digest::hash(b"held"));
        put_object(&objects, &held, Bytes::from_static(b"held"), true)
            .await
            .unwrap();
        let ledger = Arc::new(FilePinStore::new(temp.path().join("pins")));
        let pin = ledger
            .register(DataPin {
                scope: PinScope::Staging,
                catalog: None,
                resources: BTreeSet::from([PinResource::StorageObject(held.to_string())]),
            })
            .await
            .unwrap()
            .unwrap();
        let collector = ledger.acquire_collection(None).await.unwrap().unwrap();
        let mark = writer
            .payload_pin_mark(ledger.clone(), BTreeSet::new())
            .await
            .unwrap();
        let revision = ledger.inventory().await.unwrap().revision;
        let start = std::time::Instant::now();
        writer.reclaim_payloads(false, Some(&mark)).await.unwrap();
        let nanos = start.elapsed().as_nanos();
        let inventory = ledger.inventory().await.unwrap();
        let pairs = (inventory.revision - revision) / 2;
        assert_eq!(
            inventory.revision - revision,
            2 * count.div_ceil(1000) as u64
        );
        assert!(inventory.deletions.is_empty());
        assert!(writer.published_retirements.lock().unwrap().is_empty());
        for path in paths {
            assert!(objects.head(&path).await.is_err());
        }
        assert_eq!(
            objects.get(&held).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"held")
        );
        ledger.release(&pin).await.unwrap();
        ledger.finish_collection(&collector).await.unwrap();
        println!(
            "cleanup_batch_sample {}",
            serde_json::json!({"count": count, "nanos": nanos,
            "claim_pairs": pairs, "correctness": "held path survives; candidates and claims reclaimed"})
        );
    }

    #[tokio::test]
    async fn catalog_deletion_rejects_a_pin_registered_after_its_mark() {
        use crate::metadata::{DataPin, PinResource, PinScope, PinStore};
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("catalog-late-pin");
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &PackedChunks::empty_state_catalog().unwrap(),
        )
        .await
        .unwrap();
        let bytes = Bytes::from_static(b"catalog input acquired after marking");
        let path = sharded_path(&base, INDEXES_KIND, &Digest::from(blake3::hash(&bytes)));
        put_object(&objects, &path, bytes.clone(), true)
            .await
            .unwrap();
        let ledger = Arc::new(crate::metadata::MemoryPinStore::default());
        let mark = CatalogPinMark {
            store: ledger.clone(),
            inventory: Arc::new(ledger.inventory().await.unwrap()),
            owned_claims: Arc::default(),
        };
        let token = ledger
            .register(DataPin {
                scope: PinScope::Staging,
                catalog: None,
                resources: BTreeSet::from([PinResource::StorageObject(path.to_string())]),
            })
            .await
            .unwrap()
            .unwrap();
        let error = writer
            .delete_catalog_batch(vec![path.clone()], Some(&mark))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(
            objects.get(&path).await.unwrap().bytes().await.unwrap(),
            bytes
        );
        let inventory = ledger.inventory().await.unwrap();
        assert!(inventory.deletions.is_empty());
        assert!(inventory.pins.contains_key(&token));
        ledger.release(&token).await.unwrap();
    }

    #[tokio::test]
    async fn catalog_reclamation_preserves_pinned_roots_then_removes_them() {
        use crate::metadata::{DataPin, PinResource, PinScope, PinStore};
        let ledger = Arc::new(crate::metadata::MemoryPinStore::default());
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("catalog-reclamation-pins");
        let empty = PackedChunks::empty_state_catalog().unwrap();
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &empty,
        )
        .await
        .unwrap();
        writer.set_catalog_rebase_run_bytes_for_test(1);

        let (old, old_bytes) = chunk(b"catalog pin old chunk");
        writer.put(old.clone(), old_bytes.clone()).await.unwrap();
        let old_trigger = writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(true).unwrap();
        writer.wait_for_background_catalog_rebase().await.unwrap();
        let old_catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(true).unwrap();
        assert_ne!(old_catalog, old_trigger);
        let old_root = match decode_delta_catalog(&old_catalog).unwrap().base {
            CatalogBase::Sharded { root, .. } => root,
            other => panic!("expected old sharded root, got {other:?}"),
        };
        let old_reader = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &old_catalog,
        )
        .await
        .unwrap();

        let (added, added_bytes) = chunk(b"catalog pin new chunk");
        writer
            .put(added.clone(), added_bytes.clone())
            .await
            .unwrap();
        let current_trigger = writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(true).unwrap();
        writer.wait_for_background_catalog_rebase().await.unwrap();
        let current_catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(true).unwrap();
        assert_ne!(current_catalog, current_trigger);
        let current_root = match decode_delta_catalog(&current_catalog).unwrap().base {
            CatalogBase::Sharded { root, .. } => root,
            other => panic!("expected current sharded root, got {other:?}"),
        };
        assert_ne!(old_root, current_root);

        let orphan = Bytes::from_static(b"unreachable catalog object");
        let orphan_digest = Digest::from(blake3::hash(&orphan));
        put_object(
            &objects,
            &sharded_path(&base, INDEXES_KIND, &orphan_digest),
            orphan,
            true,
        )
        .await
        .unwrap();

        let invalid_pin = Bytes::from_static(b"truncated pinned catalog");
        assert!(
            writer
                .reclaim_catalog_objects(&[invalid_pin])
                .await
                .is_err()
        );
        assert!(
            objects
                .head(&sharded_path(&base, INDEXES_KIND, &orphan_digest))
                .await
                .is_ok(),
            "mark failure must abort before the sweep"
        );

        let candidate = Bytes::from_static(b"unpublished pinned catalog input");
        let candidate_path =
            sharded_path(&base, INDEXES_KIND, &Digest::from(blake3::hash(&candidate)));
        put_object(&objects, &candidate_path, candidate, true)
            .await
            .unwrap();
        let pin = ledger
            .register(DataPin {
                scope: PinScope::Staging,
                catalog: Some(old_catalog.clone()),
                resources: BTreeSet::from([PinResource::StorageObject(candidate_path.to_string())]),
            })
            .await
            .unwrap()
            .unwrap();
        let pinned = writer
            .reclaim_catalog_objects_pinned(ledger.clone(), BTreeSet::new())
            .await
            .unwrap();
        assert!(objects.head(&candidate_path).await.is_ok());
        assert!(ledger.inventory().await.unwrap().deletions.is_empty());
        assert!(pinned.deleted_objects >= 1);
        assert!(pinned.deleted_bytes > 0);
        assert!(
            objects
                .head(&sharded_path(&base, INDEXES_KIND, &old_root))
                .await
                .is_ok()
        );
        assert!(
            objects
                .head(&sharded_path(&base, INDEXES_KIND, &current_root))
                .await
                .is_ok()
        );
        assert!(
            objects
                .head(&sharded_path(&base, INDEXES_KIND, &orphan_digest))
                .await
                .is_err()
        );
        assert_eq!(
            old_reader.get(&old.digest).await.unwrap(),
            Some(old_bytes.clone())
        );

        ledger.release(&pin).await.unwrap();
        let pin = ledger
            .register(DataPin {
                scope: PinScope::Staging,
                catalog: None,
                resources: BTreeSet::from([
                    PinResource::Catalog(old_catalog.clone()),
                    PinResource::StorageObject(candidate_path.to_string()),
                ]),
            })
            .await
            .unwrap()
            .unwrap();
        writer
            .reclaim_catalog_objects_pinned(ledger.clone(), BTreeSet::new())
            .await
            .unwrap();
        assert!(
            objects
                .head(&sharded_path(&base, INDEXES_KIND, &old_root))
                .await
                .is_ok()
        );
        assert!(objects.head(&candidate_path).await.is_ok());
        ledger.release(&pin).await.unwrap();
        let unpinned = writer
            .reclaim_catalog_objects_pinned(ledger.clone(), BTreeSet::new())
            .await
            .unwrap();
        assert!(objects.head(&candidate_path).await.is_err());
        assert!(ledger.inventory().await.unwrap().deletions.is_empty());
        assert!(unpinned.deleted_objects >= 1);
        assert!(
            objects
                .head(&sharded_path(&base, INDEXES_KIND, &old_root))
                .await
                .is_err()
        );
        let current_reader =
            PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &current_catalog)
                .await
                .unwrap();
        assert_eq!(
            current_reader.get(&old.digest).await.unwrap(),
            Some(old_bytes)
        );
        assert_eq!(
            current_reader.get(&added.digest).await.unwrap(),
            Some(added_bytes)
        );
    }

    #[tokio::test]
    async fn catalog_reclamation_refuses_a_prepared_state_commit() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("catalog-reclamation-prepared");
        let empty = PackedChunks::empty_state_catalog().unwrap();
        let writer = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &empty)
            .await
            .unwrap();
        writer.set_catalog_rebase_run_bytes_for_test(1);
        let (meta, compressed) = chunk(b"prepared catalog root");
        writer.put(meta, compressed).await.unwrap();
        let prepared = writer.prepare_state_catalog().await.unwrap().unwrap();

        let error = writer.reclaim_catalog_objects(&[]).await.unwrap_err();
        assert!(error.to_string().contains("state commit is prepared"));

        writer.finish_state_catalog(false).unwrap();
        let retry = writer.prepare_state_catalog().await.unwrap().unwrap();
        assert_eq!(retry, prepared);
        writer.finish_state_catalog(true).unwrap();
    }

    #[tokio::test]
    async fn standalone_rebase_marks_reclamation_due_until_the_sweep_finishes() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("catalog-reclamation-marker");
        let writer = PackedChunks::open(objects, base, u64::MAX).await.unwrap();
        writer.set_catalog_rebase_run_bytes_for_test(1);
        let (meta, compressed) = chunk(b"catalog marker payload");
        writer.put(meta.clone(), compressed.clone()).await.unwrap();
        writer.flush().await.unwrap();

        assert!(writer.catalog_reclaim_due().await.unwrap());
        writer.reclaim_catalog_objects(&[]).await.unwrap();
        assert!(!writer.catalog_reclaim_due().await.unwrap());
        assert_eq!(writer.get(&meta.digest).await.unwrap(), Some(compressed));
    }

    #[tokio::test]
    async fn interrupted_rebase_upload_reopens_old_root_and_collects_partial_shards() {
        for failed_put in [1, 2] {
            let fault = Arc::new(FailingCatalogPutStore::new());
            let objects: Arc<dyn ObjectStore> = fault.clone();
            let base = Path::from(format!("interrupted-rebase-upload-{failed_put}"));
            let writer = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
                .await
                .unwrap();
            let (old, old_bytes) = chunk(b"published before interrupted rebase");
            writer.put(old.clone(), old_bytes.clone()).await.unwrap();
            writer.flush().await.unwrap();

            writer.set_catalog_rebase_run_bytes_for_test(1);
            let (unpublished, unpublished_bytes) = chunk(b"interrupted rebase addition");
            writer
                .put(unpublished.clone(), unpublished_bytes)
                .await
                .unwrap();
            fault.fail_catalog_put(failed_put);
            let error = writer.flush().await.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("injected catalog shard PUT failure")
            );
            assert!(writer.catalog_reclaim_due().await.unwrap());
            drop(writer);

            let reopened = PackedChunks::open(objects, base, u64::MAX).await.unwrap();
            assert_eq!(reopened.get(&old.digest).await.unwrap(), Some(old_bytes));
            assert_eq!(reopened.get(&unpublished.digest).await.unwrap(), None);
            fault.disarm();
            let reclaimed = reopened.reclaim_catalog_objects(&[]).await.unwrap();
            assert_eq!(reclaimed.deleted_objects, failed_put - 1);
            assert!(!reopened.catalog_reclaim_due().await.unwrap());
        }
    }

    #[tokio::test]
    async fn interrupted_catalog_sweep_keeps_marker_and_retries_from_published_root() {
        let fault = Arc::new(FailingCatalogPutStore::new());
        let objects: Arc<dyn ObjectStore> = fault.clone();
        let base = Path::from("interrupted-catalog-sweep");
        let writer = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        writer.set_catalog_rebase_run_bytes_for_test(1);
        let (old, old_bytes) = chunk(b"catalog sweep old payload");
        writer.put(old.clone(), old_bytes.clone()).await.unwrap();
        writer.flush().await.unwrap();
        let (added, added_bytes) = chunk(b"catalog sweep current payload");
        writer
            .put(added.clone(), added_bytes.clone())
            .await
            .unwrap();
        writer.flush().await.unwrap();

        for seed in [b"sweep orphan one".as_slice(), b"sweep orphan two"] {
            let bytes = Bytes::copy_from_slice(seed);
            let digest = Digest::from(blake3::hash(&bytes));
            put_object(
                &objects,
                &sharded_path(&base, INDEXES_KIND, &digest),
                bytes,
                true,
            )
            .await
            .unwrap();
        }
        fault.fail_catalog_delete(2);
        let error = writer.reclaim_catalog_objects(&[]).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("injected catalog object DELETE failure")
        );
        assert!(writer.catalog_reclaim_due().await.unwrap());
        drop(writer);

        let reopened = PackedChunks::open(objects, base, u64::MAX).await.unwrap();
        assert_eq!(
            reopened.get(&old.digest).await.unwrap(),
            Some(old_bytes.clone())
        );
        assert_eq!(
            reopened.get(&added.digest).await.unwrap(),
            Some(added_bytes.clone())
        );
        fault.disarm();
        assert!(
            reopened
                .reclaim_catalog_objects(&[])
                .await
                .unwrap()
                .deleted_objects
                >= 1
        );
        assert!(!reopened.catalog_reclaim_due().await.unwrap());
        assert_eq!(reopened.get(&old.digest).await.unwrap(), Some(old_bytes));
        assert_eq!(
            reopened.get(&added.digest).await.unwrap(),
            Some(added_bytes)
        );
    }

    #[tokio::test]
    async fn failed_owned_maintenance_discards_candidate_before_reclamation() {
        let fault = Arc::new(FailingCatalogPutStore::new());
        let objects: Arc<dyn ObjectStore> = fault.clone();
        let base = Path::from("failed-owned-maintenance");
        let empty = PackedChunks::empty_state_catalog().unwrap();
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &empty,
        )
        .await
        .unwrap();
        let (first, bytes) = chunk(b"first maintained chunk");
        writer.put(first.clone(), bytes.clone()).await.unwrap();
        writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(true).unwrap();
        writer.set_catalog_rebase_run_bytes_for_test(1);
        let (second, second_bytes) = chunk(b"second maintained chunk");
        writer
            .put(second.clone(), second_bytes.clone())
            .await
            .unwrap();
        let catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(true).unwrap();
        let mut maintenance = writer.take_catalog_maintenance().unwrap();
        fault.fail_catalog_put(2);
        assert!(maintenance.run().await.is_err());
        drop(maintenance);
        assert!(
            writer
                .background_catalog_rebase
                .lock()
                .unwrap()
                .job
                .is_none()
        );
        fault.disarm();
        let collector = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &catalog,
        )
        .await
        .unwrap();
        collector.reclaim_catalog_objects(&[]).await.unwrap();
        writer
            .synchronize_state_catalog(Some(&catalog))
            .await
            .unwrap();
        let (third, third_bytes) = chunk(b"retry maintenance");
        writer
            .put(third.clone(), third_bytes.clone())
            .await
            .unwrap();
        writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(true).unwrap();
        let mut maintenance = writer.take_catalog_maintenance().unwrap();
        maintenance.run().await.unwrap();
        let catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(true).unwrap();
        drop(maintenance);
        let reopened = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &catalog)
            .await
            .unwrap();
        for (meta, bytes) in [(first, bytes), (second, second_bytes), (third, third_bytes)] {
            assert_eq!(reopened.get(&meta.digest).await.unwrap(), Some(bytes));
        }
    }

    #[tokio::test]
    async fn failed_rebase_shard_put_keeps_old_root_and_retries() {
        let fault = Arc::new(FailingCatalogPutStore::new());
        let objects: Arc<dyn ObjectStore> = fault.clone();
        let base = Path::from("failed-state-rebase");
        let empty = PackedChunks::empty_state_catalog().unwrap();
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &empty,
        )
        .await
        .unwrap();
        let (old, old_bytes) = chunk(b"rebase upload old chunk");
        writer.put(old.clone(), old_bytes.clone()).await.unwrap();
        let old_catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(true).unwrap();

        writer.set_catalog_rebase_run_bytes_for_test(1);
        let (added, added_bytes) = chunk(b"rebase upload new chunk");
        writer
            .put(added.clone(), added_bytes.clone())
            .await
            .unwrap();
        fault.fail_catalog_put(2);
        let trigger_catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(true).unwrap();
        let error = writer
            .wait_for_background_catalog_rebase()
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("background catalog rebase failed")
        );
        assert!(writer.prepared_index_catalog.lock().unwrap().is_none());

        let unchanged = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &old_catalog,
        )
        .await
        .unwrap();
        assert_eq!(
            unchanged.get(&old.digest).await.unwrap(),
            Some(old_bytes.clone())
        );
        assert_eq!(unchanged.get(&added.digest).await.unwrap(), None);

        let triggered = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &trigger_catalog,
        )
        .await
        .unwrap();
        assert_eq!(
            triggered.get(&old.digest).await.unwrap(),
            Some(old_bytes.clone())
        );
        assert_eq!(
            triggered.get(&added.digest).await.unwrap(),
            Some(added_bytes.clone())
        );

        fault.disarm();
        assert_eq!(writer.prepare_state_catalog().await.unwrap(), None);
        writer.wait_for_background_catalog_rebase().await.unwrap();
        let retry_catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(true).unwrap();
        let reopened =
            PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &retry_catalog)
                .await
                .unwrap();
        assert_eq!(reopened.get(&old.digest).await.unwrap(), Some(old_bytes));
        assert_eq!(
            reopened.get(&added.digest).await.unwrap(),
            Some(added_bytes)
        );
    }

    #[tokio::test]
    async fn aborted_rebase_commit_leaves_old_root_readable_and_retryable() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("aborted-state-rebase");
        let empty = PackedChunks::empty_state_catalog().unwrap();
        let writer = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &empty,
        )
        .await
        .unwrap();
        let (old, old_bytes) = chunk(b"rebase commit old chunk");
        writer.put(old.clone(), old_bytes.clone()).await.unwrap();
        let old_catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(true).unwrap();

        writer.set_catalog_rebase_run_bytes_for_test(1);
        let (added, added_bytes) = chunk(b"rebase commit retried chunk");
        writer
            .put(added.clone(), added_bytes.clone())
            .await
            .unwrap();
        let uncommitted = writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(false).unwrap();

        let old_reader = PackedChunks::open_with_state_catalog(
            objects.clone(),
            base.clone(),
            u64::MAX,
            0,
            &old_catalog,
        )
        .await
        .unwrap();
        assert_eq!(old_reader.get(&old.digest).await.unwrap(), Some(old_bytes));
        assert_eq!(old_reader.get(&added.digest).await.unwrap(), None);

        let retry = writer.prepare_state_catalog().await.unwrap().unwrap();
        assert_eq!(retry, uncommitted);
        writer.finish_state_catalog(true).unwrap();
        let reopened = PackedChunks::open_with_state_catalog(objects, base, u64::MAX, 0, &retry)
            .await
            .unwrap();
        assert_eq!(
            reopened.get(&added.digest).await.unwrap(),
            Some(added_bytes)
        );
    }

    #[tokio::test]
    async fn run_bytes_trigger_a_bounded_base_rebase() {
        let root = DeltaCatalog {
            sidecars: None,
            generation: 3,
            base: CatalogBase::Inline(Bytes::from_static(b"base")),
            runs: BTreeMap::from([(
                0,
                CatalogRunRef {
                    digest: Digest::from(blake3::hash(b"run")),
                    first_generation: 1,
                    last_generation: 2,
                    encoded_bytes: CATALOG_REBASE_RUN_BYTES - 16,
                    query: None,
                },
            )]),
            deltas: Vec::new(),
        };
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = PackedChunks::open(objects, Path::from("rebase-threshold"), u64::MAX)
            .await
            .unwrap();
        assert!(store.catalog_rebase_due(&root, &[0; 8]));
        let small = DeltaCatalog {
            sidecars: None,
            generation: 1,
            base: CatalogBase::Inline(Bytes::new()),
            runs: BTreeMap::new(),
            deltas: Vec::new(),
        };
        assert!(!store.catalog_rebase_due(&small, &[0; 8]));
        store.set_catalog_rebase_run_bytes_for_test(1);
        assert!(store.catalog_rebase_due(&small, &[0; 8]));

        let routing_limited = DeltaCatalog {
            sidecars: None,
            generation: 3,
            base: CatalogBase::Inline(Bytes::new()),
            runs: BTreeMap::from([(
                0,
                CatalogRunRef {
                    digest: Digest::from(blake3::hash(b"routing-limited run")),
                    first_generation: 1,
                    last_generation: 2,
                    encoded_bytes: 1,
                    query: Some(delta::CatalogRunQueryRef {
                        offset: 0,
                        encoded_bytes: 1,
                        digest: Digest::from(blake3::hash(b"routing-limited query")),
                        routing: Bytes::from(vec![0; CATALOG_REBASE_INLINE_ROUTING_BYTES as usize]),
                    }),
                },
            )]),
            deltas: Vec::new(),
        };
        let fresh = PackedChunks::open(
            Arc::new(InMemory::new()),
            Path::from("rebase-routing-threshold"),
            u64::MAX,
        )
        .await
        .unwrap();
        assert!(fresh.catalog_rebase_due(&routing_limited, &[0; 8]));

        let reference_limited = DeltaCatalog {
            sidecars: None,
            generation: CATALOG_REBASE_RUN_REFS as u64 + 1,
            base: CatalogBase::Inline(Bytes::new()),
            runs: (0..CATALOG_REBASE_RUN_REFS)
                .map(|level| {
                    (
                        level as u8,
                        CatalogRunRef {
                            digest: Digest::from(blake3::hash(&level.to_le_bytes())),
                            first_generation: level as u64 + 1,
                            last_generation: level as u64 + 1,
                            encoded_bytes: 1,
                            query: None,
                        },
                    )
                })
                .collect(),
            deltas: Vec::new(),
        };
        assert!(fresh.catalog_rebase_due(&reference_limited, &[0; 8]));
    }

    #[tokio::test]
    async fn cancelled_publication_restores_mutations_and_retirements() {
        for standalone in [false, true] {
            let fault = Arc::new(FailingCatalogPutStore::new());
            let objects: Arc<dyn ObjectStore> = fault.clone();
            let empty = PackedChunks::empty_state_catalog().unwrap();
            let base = Path::from("cancelled-catalog");
            let store = if standalone {
                PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
                    .await
                    .unwrap()
            } else {
                PackedChunks::open_with_state_catalog(
                    objects.clone(),
                    base.clone(),
                    u64::MAX,
                    0,
                    &empty,
                )
                .await
                .unwrap()
            };
            let old = BlobId::new(blake3::hash(b"old manifest").into());
            let old_path = sharded_path(&base, "blobs", old.as_digest());
            put_object(
                &objects,
                &old_path,
                Bytes::from_static(b"old manifest"),
                true,
            )
            .await
            .unwrap();
            store.register_manifest(old);
            if standalone {
                store.flush().await.unwrap();
            } else {
                store.prepare_state_catalog().await.unwrap().unwrap();
                store.finish_state_catalog(true).unwrap();
            }
            store.unregister_manifest_retiring(old, HashSet::from([old_path.clone()]));
            let mut last = None;
            for i in 0u32..20_000 {
                let (meta, bytes) = chunk(&i.to_le_bytes());
                last = Some(meta.digest);
                store.put(meta, bytes).await.unwrap();
            }
            fault.pause_catalog_put();
            {
                let prepare = async {
                    if standalone {
                        store.flush().await.map(|_| None)
                    } else {
                        store.prepare_state_catalog().await
                    }
                };
                tokio::pin!(prepare);
                tokio::select! {
                    _ = async { while fault.catalog_puts.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; } } => {},
                    result = &mut prepare => panic!("prepare did not pause: {result:?}"),
                }
            }
            fault.disarm();
            assert!(store.prepared_index_catalog.lock().unwrap().is_none());
            assert!(!store.pending_catalog.lock().unwrap().is_empty());
            assert!(store.index_dirty.load(Ordering::SeqCst));
            assert!(
                store
                    .pending_catalog
                    .lock()
                    .unwrap()
                    .retirements
                    .contains(&old_path)
            );
            assert!(
                !store
                    .published_retirements
                    .lock()
                    .unwrap()
                    .contains(&old_path)
            );
            assert!(objects.head(&old_path).await.is_ok());
            let reopened = if standalone {
                store.flush().await.unwrap();
                PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
                    .await
                    .unwrap()
            } else {
                let catalog = store.prepare_state_catalog().await.unwrap().unwrap();
                store.finish_state_catalog(true).unwrap();
                PackedChunks::open_with_state_catalog(
                    objects.clone(),
                    base.clone(),
                    u64::MAX,
                    0,
                    &catalog,
                )
                .await
                .unwrap()
            };
            assert!(
                store
                    .published_retirements
                    .lock()
                    .unwrap()
                    .contains(&old_path)
            );
            store.finish_collection(false).await.unwrap();
            assert!(objects.head(&old_path).await.is_err());
            assert!(reopened.probe(&last.unwrap()).await.unwrap());
        }
    }

    #[tokio::test]
    async fn aborted_state_catalog_commit_restores_delta_and_preserves_later_mutations() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("aborted-state-catalog");
        let writer = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let (meta, compressed) = chunk(b"retryable state catalog");
        writer.put(meta.clone(), compressed.clone()).await.unwrap();

        let first = BlobId::new(blake3::hash(b"first state manifest").into());
        let later = BlobId::new(blake3::hash(b"later state manifest").into());
        writer.register_manifest(first);
        let first_catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.register_manifest(later);

        let reader = PackedChunks::open(objects, base, u64::MAX).await.unwrap();
        reader
            .synchronize_state_catalog(Some(&first_catalog))
            .await
            .unwrap();
        assert!(!reader.manifest_definitely_absent(&first));
        assert!(reader.manifest_definitely_absent(&later));

        writer.finish_state_catalog(false).unwrap();
        let retry_catalog = writer.prepare_state_catalog().await.unwrap().unwrap();
        assert_ne!(retry_catalog, first_catalog);
        writer.finish_state_catalog(true).unwrap();

        reader
            .synchronize_state_catalog(Some(&retry_catalog))
            .await
            .unwrap();
        assert!(!reader.manifest_definitely_absent(&first));
        assert!(!reader.manifest_definitely_absent(&later));
        assert_eq!(reader.get(&meta.digest).await.unwrap(), Some(compressed));
    }

    #[tokio::test]
    async fn coordinated_gc_defers_catalog_publication_to_state_commit() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("state-catalog-gc");
        let writer = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        writer.enable_state_catalog();
        let (meta, compressed) = chunk(b"state catalog gc payload");
        writer.put(meta.clone(), compressed).await.unwrap();
        let initial = writer.prepare_state_catalog().await.unwrap().unwrap();
        writer.finish_state_catalog(true).unwrap();

        writer.delete_many(&[meta.digest]).await.unwrap();
        writer.reset_read_stats();
        writer.finish_deletions(true).await.unwrap();
        assert_eq!(writer.read_stats().index_put_requests, 0);
        let collected = writer.prepare_state_catalog().await.unwrap().unwrap();
        assert_ne!(collected, initial);
        writer.finish_state_catalog(true).unwrap();

        let reader = PackedChunks::open(objects, base, u64::MAX).await.unwrap();
        reader
            .synchronize_state_catalog(Some(&collected))
            .await
            .unwrap();
        assert_eq!(reader.get(&meta.digest).await.unwrap(), None);
    }

    #[tokio::test]
    async fn missing_authoritative_checkpoint_falls_back_to_inventory() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("missing-checkpoint");
        let store = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let (meta, compressed) = chunk(b"recoverable pack");
        store.put(meta.clone(), compressed.clone()).await.unwrap();
        store.flush().await.unwrap();

        let missing = Digest::from(blake3::hash(b"missing checkpoint"));
        let catalog = DeltaCatalog {
            sidecars: None,
            generation: 1,
            base: CatalogBase::Checkpoint(missing),
            runs: BTreeMap::new(),
            deltas: Vec::new(),
        };
        objects
            .put(
                &base.clone().join(INDEX_POINTER_NAME),
                encode_delta_catalog(&catalog).unwrap().into(),
            )
            .await
            .unwrap();

        let reopened = PackedChunks::open(objects, base, u64::MAX).await.unwrap();
        let stats = reopened.read_stats();
        assert_eq!(stats.index_fallbacks, 1);
        assert_eq!(stats.list_requests, 4);
        assert_eq!(reopened.get(&meta.digest).await.unwrap(), Some(compressed));
    }

    #[tokio::test]
    async fn corrupt_inline_catalog_falls_back_and_repairs_itself() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("corrupt-v1-catalog");
        let store = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let (meta, compressed) = chunk(b"catalog checksum recovery");
        store.put(meta.clone(), compressed.clone()).await.unwrap();
        store.flush().await.unwrap();
        drop(store);

        let path = base.clone().join(INDEX_POINTER_NAME);
        let mut corrupt = objects
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        *corrupt.last_mut().unwrap() ^= 0xff;
        objects.put(&path, corrupt.into()).await.unwrap();

        let repaired = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let stats = repaired.read_stats();
        assert_eq!(stats.index_hits, 0);
        assert_eq!(stats.index_fallbacks, 1);
        assert_eq!(stats.list_requests, 4);
        assert_eq!(stats.index_put_requests, 1);
        assert_eq!(repaired.get(&meta.digest).await.unwrap(), Some(compressed));
        drop(repaired);

        let reopened = PackedChunks::open(objects, base, u64::MAX).await.unwrap();
        let stats = reopened.read_stats();
        assert_eq!(stats.index_hits, 1);
        assert_eq!(stats.list_requests, 0);
        assert_eq!(stats.index_requests, 0);
    }

    #[tokio::test]
    async fn missing_catalog_pack_forces_inventory_recovery() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("missing-catalog-pack");
        let store = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let (meta, compressed) = chunk(b"deleted before pointer advance");
        store.put(meta.clone(), compressed).await.unwrap();
        store.flush().await.unwrap();
        let pack = *store.index.read().unwrap().packs.keys().next().unwrap();
        objects.delete(&pack_path(&base, &pack)).await.unwrap();
        store.reset_read_stats();

        assert_eq!(store.get(&meta.digest).await.unwrap(), None);
        let stats = store.read_stats();
        assert_eq!(stats.list_requests, 4);
        assert_eq!(stats.index_fallbacks, 1);
    }

    #[tokio::test]
    async fn sparse_packs_share_one_delta_and_shared_record_survives_partial_vacuum() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("payloads");
        let mut groups = Vec::new();
        for group in 0..2 {
            let mut batch = Batch::default();
            let mut digests = Vec::new();
            for entry in 0..4 {
                let (meta, bytes) = chunk(format!("group-{group}-entry-{entry}").as_bytes());
                digests.push(meta.digest);
                batch.push(meta, bytes);
            }
            let sealed = seal(&batch).unwrap();
            put_object(&objects, &pack_path(&base, &sealed.id), sealed.bytes, true)
                .await
                .unwrap();
            groups.push(digests);
        }

        let store = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        store.reset_read_stats();
        store
            .delete_many(&[groups[0][0], groups[1][0]])
            .await
            .unwrap();
        store.finish_deletions(false).await.unwrap();
        let stats = store.read_stats();
        assert_eq!(stats.gc_tombstone_put_requests, 1);
        assert_eq!(stats.gc_deferred_packs, 2);
        let records = objects
            .list(Some(&kind_prefix(&base, TOMBSTONES_KIND)))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(records.len(), 1);
        let delta = objects
            .get(&records[0].location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(decode_tombstone_record(&delta).unwrap().len(), 2);

        // This takes only the first pack to 50%, so it is compacted. The
        // shared delta must remain because the second pack still needs it.
        let reopened = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        reopened.delete_many(&[groups[0][1]]).await.unwrap();
        reopened.finish_deletions(false).await.unwrap();

        let reopened = PackedChunks::open(objects, base, u64::MAX).await.unwrap();
        assert_eq!(reopened.get(&groups[0][0]).await.unwrap(), None);
        assert_eq!(reopened.get(&groups[0][1]).await.unwrap(), None);
        assert_eq!(reopened.get(&groups[1][0]).await.unwrap(), None);
        assert!(reopened.get(&groups[1][1]).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn later_tombstones_compact_using_cumulative_dead_density() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("payloads");
        let store = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let chunks = (0..5)
            .map(|index| chunk(format!("equal-sized-chunk-{index}").as_bytes()))
            .collect::<Vec<_>>();
        for (meta, bytes) in &chunks {
            store.put(meta.clone(), bytes.clone()).await.unwrap();
        }
        store.flush().await.unwrap();
        store.delete_many(&[chunks[0].0.digest]).await.unwrap();
        store.finish_deletions(false).await.unwrap();

        let reopened = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        reopened.reset_read_stats();
        reopened.delete_many(&[chunks[1].0.digest]).await.unwrap();
        reopened.finish_deletions(false).await.unwrap();
        let stats = reopened.read_stats();
        assert_eq!(stats.gc_tombstone_put_requests, 1);
        assert_eq!(stats.gc_replacement_put_requests, 0);

        let reopened = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        reopened.reset_read_stats();
        reopened.delete_many(&[chunks[2].0.digest]).await.unwrap();
        reopened.finish_deletions(false).await.unwrap();
        let stats = reopened.read_stats();
        assert_eq!(stats.gc_tombstone_put_requests, 0);
        assert_eq!(stats.gc_replacement_put_requests, 1);

        let reopened = PackedChunks::open(objects, base, u64::MAX).await.unwrap();
        assert_eq!(reopened.get(&chunks[0].0.digest).await.unwrap(), None);
        assert_eq!(reopened.get(&chunks[1].0.digest).await.unwrap(), None);
        assert_eq!(reopened.get(&chunks[2].0.digest).await.unwrap(), None);
        for (meta, _) in &chunks[3..] {
            assert!(reopened.get(&meta.digest).await.unwrap().is_some());
        }
    }

    #[tokio::test]
    async fn catalog_pointer_refuses_stores_without_conditional_updates() {
        // LocalFileSystem supports create-only puts but not `PutMode::Update`.
        // Without the local catalog lock, publication must fail rather than
        // overwrite a pointer another writer may have advanced.
        let directory = tempfile::tempdir().unwrap();
        let objects: Arc<dyn ObjectStore> = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap(),
        );
        let base = Path::from("repository");
        let store = ChunkedBlobStore::packed_with_options(
            objects.clone(),
            base.clone(),
            DEFAULT_AVG_CHUNK_SIZE,
            crate::PackOptions {
                target_size: u64::MAX,
                cache_capacity: 0,
            },
        )
        .await
        .unwrap();
        let pointer = base.clone().join(INDEX_POINTER_NAME);
        let read_pointer = || async { objects.get(&pointer).await.unwrap().bytes().await.unwrap() };
        let opened = read_pointer().await;
        let error = store.put_slice(b"small file").await.unwrap_err();
        let unsupported = matches!(
            &error,
            crate::error::Error::Io(io) if matches!(
                io.get_ref().and_then(|inner| inner.downcast_ref::<object_store::Error>()),
                Some(object_store::Error::NotSupported { .. })
            )
        );
        assert!(unsupported, "{error}");
        assert_eq!(
            read_pointer().await,
            opened,
            "the pointer must not be overwritten"
        );
    }

    #[tokio::test]
    async fn chunked_store_batches_small_blobs_and_reopens_from_pack_footers() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("repository");
        let store = ChunkedBlobStore::packed_with_options(
            objects.clone(),
            base.clone(),
            DEFAULT_AVG_CHUNK_SIZE,
            crate::PackOptions {
                target_size: u64::MAX,
                cache_capacity: 0,
            },
        )
        .await
        .unwrap();
        let batch = store.begin_batch();
        let first = store.put_slice(b"first small file").await.unwrap();
        let second = store.put_slice(b"second small file").await.unwrap();

        assert_eq!(
            store.read_to_vec(&first).await.unwrap().unwrap(),
            b"first small file"
        );
        assert!(
            objects
                .list(Some(&kind_prefix(&base, PACKS_KIND)))
                .next()
                .await
                .is_none()
        );
        store.flush().await.unwrap();
        drop(batch);
        assert_eq!(
            objects
                .list(Some(&kind_prefix(&base, PACKS_KIND)))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(
            objects
                .list(Some(&kind_prefix(&base, "chunks")))
                .next()
                .await
                .is_none()
        );

        let reopened = ChunkedBlobStore::packed_with_options(
            objects,
            base,
            DEFAULT_AVG_CHUNK_SIZE,
            crate::PackOptions {
                target_size: u64::MAX,
                cache_capacity: 0,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            reopened.read_to_vec(&first).await.unwrap().unwrap(),
            b"first small file"
        );
        assert_eq!(
            reopened.read_to_vec(&second).await.unwrap().unwrap(),
            b"second small file"
        );
    }

    #[tokio::test]
    async fn manifest_catalog_preserves_manifest_precedence_and_elided_fast_path() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("manifest-membership");
        let store = ChunkedBlobStore::packed_with_options(
            objects.clone(),
            base.clone(),
            DEFAULT_AVG_CHUNK_SIZE,
            crate::PackOptions {
                target_size: u64::MAX,
                cache_capacity: 0,
            },
        )
        .await
        .unwrap();
        let small = store.put_slice(b"manifest-elided payload").await.unwrap();
        let large_data = (0..700_000)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let large = store.put_slice(&large_data).await.unwrap();
        store.flush().await.unwrap();
        drop(store);

        let catalog = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        assert!(catalog.manifest_definitely_absent(&small));
        assert!(!catalog.manifest_definitely_absent(&large));

        // A manifest explicitly registered for a digest must retain precedence
        // over an otherwise valid self-chunk, including corruption reporting.
        put_object(
            &objects,
            &sharded_path(&base, "blobs", small.as_digest()),
            Bytes::from_static(b"not a manifest"),
            true,
        )
        .await
        .unwrap();
        catalog.register_manifest(small);
        catalog.flush().await.unwrap();
        drop(catalog);

        let reopened = ChunkedBlobStore::packed_with_options(
            objects,
            base,
            DEFAULT_AVG_CHUNK_SIZE,
            crate::PackOptions {
                target_size: u64::MAX,
                cache_capacity: 0,
            },
        )
        .await
        .unwrap();
        assert!(reopened.open_read(&small).await.is_err());
        assert_eq!(
            reopened.read_to_vec(&large).await.unwrap().unwrap(),
            large_data
        );
    }

    #[tokio::test]
    async fn direct_put_manifest_flushes_packed_chunks_and_membership() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source = ChunkedBlobStore::packed_with_options(
            objects.clone(),
            Path::from("direct-manifest-source"),
            64 * 1024,
            crate::PackOptions {
                target_size: u64::MAX,
                cache_capacity: 0,
            },
        )
        .await
        .unwrap();
        let small_data = b"direct self chunk";
        let small_blob = source.put_slice(small_data).await.unwrap();
        let small_chunks = source.chunks(&small_blob).await.unwrap().unwrap();
        assert_eq!(small_chunks.len(), 1);
        let data = (0..400_000)
            .map(|index| (index % 247) as u8)
            .collect::<Vec<_>>();
        let blob = source.put_slice(&data).await.unwrap();
        let chunks = source.chunks(&blob).await.unwrap().unwrap();
        assert!(chunks.len() > 1);

        let destination_base = Path::from("direct-manifest-destination");
        let destination = ChunkedBlobStore::packed_with_options(
            objects.clone(),
            destination_base.clone(),
            DEFAULT_AVG_CHUNK_SIZE,
            crate::PackOptions {
                target_size: u64::MAX,
                cache_capacity: 0,
            },
        )
        .await
        .unwrap();
        for chunk in &small_chunks {
            let compressed = source.get_chunk(&chunk.digest).await.unwrap().unwrap();
            destination.put_chunk(chunk, compressed).await.unwrap();
        }
        destination
            .put_manifest(&small_blob, small_chunks)
            .await
            .unwrap();
        for chunk in &chunks {
            let compressed = source.get_chunk(&chunk.digest).await.unwrap().unwrap();
            destination.put_chunk(chunk, compressed).await.unwrap();
        }
        destination.put_manifest(&blob, chunks).await.unwrap();
        drop(destination);

        // No explicit flush: successful direct manifest publication is itself
        // the durability boundary when no batch guard is active.
        let reopened = ChunkedBlobStore::packed_with_options(
            objects,
            destination_base,
            DEFAULT_AVG_CHUNK_SIZE,
            crate::PackOptions {
                target_size: u64::MAX,
                cache_capacity: 0,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            reopened.read_to_vec(&small_blob).await.unwrap().unwrap(),
            small_data
        );
        assert_eq!(reopened.read_to_vec(&blob).await.unwrap().unwrap(), data);
    }

    #[tokio::test]
    async fn an_open_reader_discovers_another_writers_new_pack() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("shared");
        let reader = ChunkedBlobStore::packed_with_options(
            objects.clone(),
            base.clone(),
            DEFAULT_AVG_CHUNK_SIZE,
            crate::PackOptions {
                target_size: u64::MAX,
                cache_capacity: 0,
            },
        )
        .await
        .unwrap();
        let writer = ChunkedBlobStore::packed_with_options(
            objects,
            base,
            DEFAULT_AVG_CHUNK_SIZE,
            crate::PackOptions {
                target_size: u64::MAX,
                cache_capacity: 0,
            },
        )
        .await
        .unwrap();
        let id = writer.put_slice(b"published elsewhere").await.unwrap();
        writer.flush().await.unwrap();

        assert!(reader.has(&id).await.unwrap());
        assert_eq!(
            reader.read_to_vec(&id).await.unwrap().unwrap(),
            b"published elsewhere"
        );
    }

    #[tokio::test]
    async fn concurrent_catalog_publishers_merge_without_losing_packs() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("concurrent-catalog");
        let seed = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        drop(seed);
        let left = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let right = PackedChunks::open(objects.clone(), base.clone(), u64::MAX)
            .await
            .unwrap();
        let (left_meta, left_bytes) = chunk(b"left writer");
        let (right_meta, right_bytes) = chunk(b"right writer");
        left.put(left_meta.clone(), left_bytes.clone())
            .await
            .unwrap();
        right
            .put(right_meta.clone(), right_bytes.clone())
            .await
            .unwrap();
        let left_manifest = BlobId::new(blake3::hash(b"left manifest").into());
        let right_manifest = BlobId::new(blake3::hash(b"right manifest").into());
        left.register_manifest(left_manifest);
        right.register_manifest(right_manifest);

        let (left_result, right_result) = tokio::join!(left.flush(), right.flush());
        left_result.unwrap();
        right_result.unwrap();

        let reopened = PackedChunks::open(objects, base, u64::MAX).await.unwrap();
        let stats = reopened.read_stats();
        assert_eq!(stats.list_requests, 0);
        assert_eq!(stats.index_hits, 1);
        assert_eq!(
            reopened.get(&left_meta.digest).await.unwrap(),
            Some(left_bytes)
        );
        assert_eq!(
            reopened.get(&right_meta.digest).await.unwrap(),
            Some(right_bytes)
        );
        assert!(!reopened.manifest_definitely_absent(&left_manifest));
        assert!(!reopened.manifest_definitely_absent(&right_manifest));
    }

    #[tokio::test]
    async fn unchanged_catalog_refresh_keeps_overlay_and_observes_external_changes() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("unchanged-refresh");
        let local = PackedChunks::open(objects.clone(), base.clone(), 1)
            .await
            .unwrap();
        let (initial, initial_bytes) = chunk(b"initial published chunk");
        local
            .put(initial.clone(), initial_bytes.clone())
            .await
            .unwrap();
        local.flush().await.unwrap();
        let remote = PackedChunks::open(objects, base, 1).await.unwrap();
        let (staged, staged_bytes) = chunk(b"local unpublished chunk");
        local
            .put(staged.clone(), staged_bytes.clone())
            .await
            .unwrap();
        local.reset_read_stats();
        local.refresh().await.unwrap();
        let stats = local.read_stats();
        assert_eq!(stats.index_pointer_requests, 1);
        assert_eq!(stats.index_decode_nanos, 0);
        assert_eq!(stats.list_requests, 0);
        assert_eq!(
            local.get(&staged.digest).await.unwrap(),
            Some(staged_bytes.clone())
        );

        let (added, added_bytes) = chunk(b"external publication");
        remote
            .put(added.clone(), added_bytes.clone())
            .await
            .unwrap();
        remote.flush().await.unwrap();
        local.refresh().await.unwrap();
        assert_eq!(local.get(&added.digest).await.unwrap(), Some(added_bytes));
        assert_eq!(
            local.get(&initial.digest).await.unwrap(),
            Some(initial_bytes)
        );
        assert_eq!(local.get(&staged.digest).await.unwrap(), Some(staged_bytes));
        assert!(local.read_stats().index_decode_nanos > 0);
    }

    #[tokio::test]
    async fn refresh_preserves_locally_sealed_unpublished_packs() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("refresh-with-local-pack");
        let seed = PackedChunks::open(objects.clone(), base.clone(), 1)
            .await
            .unwrap();
        drop(seed);
        let local = PackedChunks::open(objects.clone(), base.clone(), 1)
            .await
            .unwrap();
        let remote = PackedChunks::open(objects.clone(), base.clone(), 1)
            .await
            .unwrap();
        let (local_meta, local_bytes) = chunk(b"sealed locally");
        let (remote_meta, remote_bytes) = chunk(b"published remotely");

        // A one-byte target seals during put, before the explicit publication
        // flush that advances the catalog pointer.
        local
            .put(local_meta.clone(), local_bytes.clone())
            .await
            .unwrap();
        remote
            .put(remote_meta.clone(), remote_bytes.clone())
            .await
            .unwrap();
        remote.flush().await.unwrap();
        local.refresh().await.unwrap();
        local.flush().await.unwrap();

        let reopened = PackedChunks::open(objects, base, 1).await.unwrap();
        assert_eq!(
            reopened.get(&local_meta.digest).await.unwrap(),
            Some(local_bytes)
        );
        assert_eq!(
            reopened.get(&remote_meta.digest).await.unwrap(),
            Some(remote_bytes)
        );
    }

    #[tokio::test]
    async fn concurrent_duplicate_puts_create_one_index_entry() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("dedup");
        let store = PackedChunks::open(objects, base, u64::MAX).await.unwrap();
        let (meta, compressed) = chunk(b"the same chunk from every task");
        let writes = (0..32).map(|_| {
            let store = store.clone();
            let meta = meta.clone();
            let compressed = compressed.clone();
            async move { store.put(meta, compressed).await }
        });
        futures::future::try_join_all(writes).await.unwrap();
        store.flush().await.unwrap();

        let index = store.index.read().unwrap();
        assert_eq!(index.chunks.len(), 1);
        assert_eq!(index.packs.values().map(Vec::len).sum::<usize>(), 1);
    }

    #[tokio::test]
    async fn concurrent_writes_exceed_the_target_by_at_most_one_chunk() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("bounded");
        let target = 256;
        let store = PackedChunks::open(objects, base, target).await.unwrap();
        let chunks = (0..32)
            .map(|value| chunk(&vec![value; 512]))
            .collect::<Vec<_>>();
        let largest = chunks
            .iter()
            .map(|(_, compressed)| compressed.len() as u64)
            .max()
            .unwrap();
        futures::future::try_join_all(chunks.into_iter().map(|(meta, compressed)| {
            let store = store.clone();
            async move { store.put(meta, compressed).await }
        }))
        .await
        .unwrap();
        store.flush().await.unwrap();

        let index = store.index.read().unwrap();
        for entries in index.packs.values() {
            let body: u64 = entries.iter().map(|entry| entry.framed_len).sum();
            assert!(body <= target + largest, "pack body {body} exceeded bound");
        }
    }

    #[tokio::test]
    async fn repeated_chunk_reads_share_the_bounded_cache() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("cached");
        let store =
            PackedChunks::open_with_cache(objects.clone(), base.clone(), u64::MAX, 1024 * 1024)
                .await
                .unwrap();
        let (first, first_bytes) = chunk(b"first cached chunk");
        let (second, second_bytes) = chunk(b"second cached chunk");
        let (third, third_bytes) = chunk(b"third cached chunk");
        store.put(first.clone(), first_bytes.clone()).await.unwrap();
        store
            .put(second.clone(), second_bytes.clone())
            .await
            .unwrap();
        store.put(third.clone(), third_bytes.clone()).await.unwrap();
        store.flush().await.unwrap();
        drop(store);
        let store = PackedChunks::open_with_cache(objects, base, u64::MAX, 1024 * 1024)
            .await
            .unwrap();
        store.reset_read_stats();

        assert_eq!(store.get(&first.digest).await.unwrap(), Some(first_bytes));
        let (second_result, third_result) =
            tokio::join!(store.get(&second.digest), store.get(&third.digest),);
        assert_eq!(second_result.unwrap(), Some(second_bytes));
        assert_eq!(third_result.unwrap(), Some(third_bytes));
        let stats = store.read_stats();
        assert_eq!(stats.chunk_range_requests, 3);
        assert_eq!(stats.whole_pack_requests, 0);
        assert_eq!(stats.cache_promotions, 0);
        store.get(&first.digest).await.unwrap();
        store.get(&second.digest).await.unwrap();
        store.get(&third.digest).await.unwrap();
        assert_eq!(store.read_stats().cache_hits, 3);
        assert_eq!(store.read_stats().chunk_range_requests, 3);
    }

    #[tokio::test]
    async fn opening_rejects_a_truncated_pack() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("truncated");
        let bytes = Bytes::from_static(b"not a complete pack");
        let id = PackId::new(blake3::hash(&bytes).into());
        objects
            .put(&pack_path(&base, &id), bytes.into())
            .await
            .unwrap();

        assert!(PackedChunks::open(objects, base, u64::MAX).await.is_err());
    }

    #[tokio::test]
    async fn a_corrupt_packed_chunk_is_reported_as_integrity_failure() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("corrupt");
        let store = ChunkedBlobStore::packed_with_options(
            objects.clone(),
            base.clone(),
            DEFAULT_AVG_CHUNK_SIZE,
            crate::PackOptions {
                target_size: u64::MAX,
                cache_capacity: 0,
            },
        )
        .await
        .unwrap();
        let id = store.put_slice(b"integrity checked bytes").await.unwrap();
        store.flush().await.unwrap();
        let pack = objects
            .list(Some(&kind_prefix(&base, PACKS_KIND)))
            .try_next()
            .await
            .unwrap()
            .unwrap();
        let mut bytes = objects
            .get(&pack.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        bytes[0] ^= 0xff;
        objects
            .put(&pack.location, Bytes::from(bytes).into())
            .await
            .unwrap();

        let error = store.read_to_vec(&id).await.unwrap_err();
        assert!(crate::blob::is_integrity_error(&error), "{error}");
    }

    #[test]
    fn malformed_footer_and_replacement_are_rejected() {
        assert!(decode_footer(&[], 0).is_err());
        assert!(decode_trailer(&[0; PACK_TRAILER_LEN]).is_err());
        assert!(decode_replacement(b"garbage").is_err());
        assert!(decode_tombstone(b"garbage").is_err());
        assert!(decode_tombstone_record(b"garbage").is_err());
    }

    #[test]
    fn replacement_magic_is_distinct_from_casitar_and_legacy_records_decode() {
        assert_ne!(&REPLACEMENT_MAGIC, crate::casitar::CASITAR_MAGIC);
        let old = PackId::new(blake3::hash(b"old").into());
        let new = PackId::new(blake3::hash(b"new").into());
        for replacement in [Some(new), None] {
            let encoded = encode_replacement(old, replacement);
            assert_eq!(&encoded[..8], &REPLACEMENT_MAGIC);
            assert_eq!(decode_replacement(&encoded).unwrap(), (old, replacement));

            let mut legacy = encoded.to_vec();
            legacy[..8].copy_from_slice(&LEGACY_REPLACEMENT_MAGIC);
            assert_eq!(decode_replacement(&legacy).unwrap(), (old, replacement));
        }
    }

    #[test]
    fn index_checkpoint_rejects_entries_outside_the_pack_body() {
        let inventory = Digest::from(blake3::hash(b"inventory"));
        let pack = PackId::new(blake3::hash(b"pack").into());
        let (meta, _) = chunk(b"chunk");
        let entries = vec![PackEntry {
            digest: meta.digest,
            offset: 10,
            framed_len: 10,
            uncompressed_len: meta.size,
        }];
        let pack_len = 15 + encode_footer(&entries).len() as u64 + PACK_TRAILER_LEN as u64;
        let mut index = Index::default();
        index.add_pack(pack, pack_len, entries);

        let checkpoint = encode_index_checkpoint(&index, inventory).unwrap();
        assert!(decode_index_checkpoint(&checkpoint, inventory).is_err());
    }

    #[test]
    fn duplicate_chunk_locations_survive_base_and_overlay_pack_removal() {
        let (meta, _) = chunk(b"shared concurrent chunk");
        let entry = PackEntry {
            digest: meta.digest,
            offset: 0,
            framed_len: 10,
            uncompressed_len: meta.size,
        };
        let first = PackId::new(blake3::hash(b"first duplicate pack").into());
        let second = PackId::new(blake3::hash(b"second duplicate pack").into());
        let mut index = Index::default();
        index.add_pack(first, 100, vec![entry]);
        index.rebuild_chunks();
        index.add_pack(second, 100, vec![entry]);

        assert_eq!(index.chunks.len(), 1);
        assert_eq!(index.chunks.base.len(), 1);
        assert_eq!(index.chunks.overlay.len(), 1);
        assert_eq!(index.chunks.get(&meta.digest).unwrap().pack, second);

        index.remove_pack(second);
        assert_eq!(index.chunks.len(), 1);
        assert!(index.chunks.overlay.is_empty());
        assert_eq!(index.chunks.get(&meta.digest).unwrap().pack, first);

        let mut decoded = Index::default();
        decoded.add_pack(first, 100, vec![entry]);
        decoded.add_pack(second, 100, vec![entry]);
        decoded.rebuild_chunks();
        assert_eq!(decoded.chunks.base.len(), 2);
        let missing = ChunkId::new(blake3::hash(b"missing sorted digest").into());
        assert_eq!(decoded.chunks.get(&missing), None);
        decoded.remove_pack(first);
        assert_eq!(decoded.chunks.get(&meta.digest).unwrap().pack, second);

        assert_eq!(decoded.chunks.remove(&meta.digest).len(), 1);
        assert!(!decoded.chunks.contains_key(&meta.digest));
    }

    #[test]
    fn manifest_index_keeps_a_sorted_base_and_exact_write_overlay() {
        let manifest = |label: &[u8]| BlobId::new(blake3::hash(label).into());
        let first = manifest(b"first manifest");
        let second = manifest(b"second manifest");
        let third = manifest(b"third manifest");
        let fourth = manifest(b"fourth manifest");

        let mut left = ManifestIndex::from_unsorted(vec![second, first, first]);
        assert_eq!(left.len(), 2);
        assert!(left.base.is_sorted());
        assert!(!left.insert(first));
        assert!(left.insert(third));
        assert_eq!(left.overlay.len(), 1);

        let right = ManifestIndex::from_unsorted(vec![fourth, third]);
        left.merge(right);
        assert_eq!(left.len(), 4);
        assert!(left.overlay.is_empty());
        assert!(left.base.is_sorted());
        for digest in [first, second, third, fourth] {
            assert!(left.contains(&digest));
        }

        let large = ManifestIndex::from_unsorted(
            (0..INDEX_FANOUT_MIN_ENTRIES)
                .map(|ordinal| manifest(&(ordinal as u64).to_le_bytes()))
                .collect(),
        );
        assert_eq!(large.fanout.len(), INDEX_FANOUT_BUCKETS + 1);
        assert!(large.contains(&manifest(&17_u64.to_le_bytes())));
        assert!(!large.contains(&manifest(b"absent fanout manifest")));
    }

    #[test]
    fn fanout_tracks_bulk_and_pack_removals() {
        let first = PackId::new(blake3::hash(b"first fanout pack").into());
        let second = PackId::new(blake3::hash(b"second fanout pack").into());
        let make_entries = |pack, marker| {
            (0..INDEX_FANOUT_MIN_ENTRIES)
                .map(|ordinal| {
                    let mut key = vec![marker];
                    key.extend_from_slice(&(ordinal as u64).to_le_bytes());
                    IndexedLocation {
                        digest: ChunkId::new(blake3::hash(&key).into()),
                        location: Location {
                            pack,
                            pack_len: 1_000_000,
                            offset: ordinal as u64 * 16,
                            framed_len: 16,
                            uncompressed_len: 8,
                        },
                    }
                })
                .collect::<Vec<_>>()
        };
        let mut entries = make_entries(first, 1);
        entries.extend(make_entries(second, 2));
        let mut chunks = ChunkIndex::default();
        chunks.replace_base(entries);
        assert_eq!(chunks.fanout.len(), INDEX_FANOUT_BUCKETS + 1);
        assert_eq!(
            chunks.fanout.last().copied().unwrap() as usize,
            chunks.base.len()
        );

        let mut removed = chunks
            .base
            .iter()
            .filter(|entry| entry.location.pack == first)
            .step_by(97)
            .take(100)
            .map(|entry| entry.digest)
            .collect::<Vec<_>>();
        let removed_count = removed.len();
        removed.push(removed[0]);
        assert_eq!(chunks.remove_many(&removed).len(), removed_count);
        assert!(removed.iter().all(|digest| chunks.get(digest).is_none()));
        assert_eq!(
            chunks.fanout.last().copied().unwrap() as usize,
            chunks.base.len()
        );

        chunks.remove_pack(first);
        assert_eq!(chunks.base.len(), INDEX_FANOUT_MIN_ENTRIES);
        assert_eq!(chunks.fanout.len(), INDEX_FANOUT_BUCKETS + 1);
        assert!(
            chunks
                .base
                .iter()
                .all(|entry| entry.location.pack == second)
        );
        assert!(
            chunks
                .base
                .iter()
                .step_by(997)
                .all(|entry| chunks.get(&entry.digest).is_some())
        );
    }

    #[test]
    fn tombstone_bitmap_round_trips_footer_ordinals() {
        let (first, _) = chunk(b"first");
        let (second, _) = chunk(b"second");
        let entries = vec![
            PackEntry {
                digest: first.digest,
                offset: 0,
                framed_len: 10,
                uncompressed_len: first.size,
            },
            PackEntry {
                digest: second.digest,
                offset: 10,
                framed_len: 11,
                uncompressed_len: second.size,
            },
        ];
        let pack = PackId::new(blake3::hash(b"pack").into());
        let bytes = encode_tombstone(pack, &entries, &HashSet::from([second.digest]));
        let decoded = decode_tombstone(&bytes).unwrap();
        assert_eq!(decoded.pack, pack);
        assert_eq!(decoded.entry_count, 2);
        assert!(!decoded.contains(0));
        assert!(decoded.contains(1));

        let other_pack = PackId::new(blake3::hash(b"other pack").into());
        let other = make_tombstone(other_pack, &entries, &HashSet::from([first.digest]));
        let delta = encode_tombstone_delta(&[other, decoded]).unwrap();
        assert_eq!(&delta[..8], &TOMBSTONE_DELTA_MAGIC);
        let decoded = decode_tombstone_record(&delta).unwrap();
        assert_eq!(decoded.len(), 2);
        assert!(decoded[0].pack < decoded[1].pack);
        let mut trailing = delta.to_vec();
        trailing.push(0);
        assert!(decode_tombstone_record(&trailing).is_err());
    }

    fn empty_inline_root(generation: u64) -> DeltaCatalog {
        DeltaCatalog {
            sidecars: None,
            generation,
            base: CatalogBase::Inline(
                encode_index_checkpoint(
                    &Index::default(),
                    Digest::from(blake3::hash(b"casita authoritative pack index v1\0")),
                )
                .unwrap(),
            ),
            runs: BTreeMap::new(),
            deltas: Vec::new(),
        }
    }

    #[test]
    fn a_catalog_pointer_decodes_advances_and_drops_what_it_retires() {
        let sealed = |data: &[u8]| {
            let (meta, bytes) = chunk(data);
            let mut batch = Batch::default();
            batch.push(meta, bytes);
            seal(&batch).unwrap()
        };
        let (kept, dropped) = (sealed(b"kept pack"), sealed(b"dropped pack"));
        let mut candidate = Index::default();
        candidate.add_pack(kept.id, kept.bytes.len() as u64, kept.entries.clone());
        candidate.remove_pack(dropped.id);
        let mut mutations = IndexMutations::default();
        mutations.record_pack(kept.id);
        mutations.record_pack(dropped.id);
        let delta = encode_index_mutations(&candidate, &mutations).unwrap();
        let previous = IndexCatalogWitness {
            generation: 1,
            root: Some(empty_inline_root(1)),
            ..IndexCatalogWitness::default()
        };
        let mut built = empty_inline_root(2);
        built.deltas.push(delta.clone());
        let pointer = encode_delta_catalog(&built).unwrap();
        let nothing = HashSet::new();
        let check = |built: &DeltaCatalog,
                     pointer: &[u8],
                     candidate: &Index,
                     retired: &HashSet<PackId>,
                     written: Option<&HashSet<Digest>>| {
            validate_catalog_pointer(
                &previous,
                built,
                pointer,
                Some(&delta),
                candidate,
                retired,
                written,
            )
        };
        assert_eq!(
            check(
                &built,
                &pointer,
                &candidate,
                &nothing,
                Some(&HashSet::new())
            ),
            Ok(())
        );

        // The pointer does not decode, or decodes to another root.
        assert!(check(&built, b"garbage", &candidate, &nothing, None).is_err());
        let other = encode_delta_catalog(&empty_inline_root(2)).unwrap();
        assert!(check(&built, &other, &candidate, &nothing, None).is_err());

        // The generation does not advance past the witnessed pointer.
        let mut stale = built.clone();
        stale.generation = 1;
        let stale_pointer = encode_delta_catalog(&stale).unwrap();
        assert!(check(&stale, &stale_pointer, &candidate, &nothing, None).is_err());

        // A pack whose payload the publication retires is still indexed.
        let retired = HashSet::from([kept.id]);
        assert!(check(&built, &pointer, &candidate, &retired, None).is_err());

        // The delta supersedes a pack the published index still references.
        let mut resurrected = candidate.clone();
        resurrected.superseded.remove(&dropped.id);
        resurrected.add_pack(dropped.id, dropped.bytes.len() as u64, dropped.entries);
        assert!(check(&built, &pointer, &resurrected, &nothing, None).is_err());

        // The pointer names a catalog object that was never stored.
        let map = Digest::from(blake3::hash(b"unwritten shard map"));
        let mut sharded = built.clone();
        sharded.base = CatalogBase::Sharded {
            root: map,
            shard_bits: 12,
        };
        let sharded_pointer = encode_delta_catalog(&sharded).unwrap();
        assert!(
            check(
                &sharded,
                &sharded_pointer,
                &candidate,
                &nothing,
                Some(&HashSet::new())
            )
            .is_err()
        );
        assert_eq!(
            check(
                &sharded,
                &sharded_pointer,
                &candidate,
                &nothing,
                Some(&HashSet::from([map]))
            ),
            Ok(())
        );
    }

    #[test]
    fn marked_deletions_never_include_a_retained_payload() {
        let kept = Path::from("packs/b3/aa/kept");
        let gone = Path::from("packs/b3/bb/gone");
        let retained = HashSet::from([kept.clone()]);
        assert_eq!(
            validate_retired_deletions(std::slice::from_ref(&gone), &retained),
            Ok(())
        );
        assert!(validate_retired_deletions(&[gone, kept], &retained).is_err());
    }

    #[test]
    fn a_pack_is_replaced_only_under_the_record_that_names_the_replacement() {
        let old = PackId::new(blake3::hash(b"old pack").into());
        let new = PackId::new(blake3::hash(b"new pack").into());
        let marker = ReplacementMarker(encode_replacement(old, Some(new)));
        assert_eq!(
            validate_replacement_reference(&marker, old, Some(new)),
            Ok(())
        );
        assert!(validate_replacement_reference(&marker, new, Some(new)).is_err());
        assert!(validate_replacement_reference(&marker, old, None).is_err());
        let garbage = ReplacementMarker(Bytes::from_static(b"garbage"));
        assert!(validate_replacement_reference(&garbage, old, Some(new)).is_err());
    }
}
