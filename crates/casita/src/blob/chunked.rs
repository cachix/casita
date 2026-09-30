//! [`ChunkedBlobStore`]: the FastCDC-deduplicating blob backend.
//!
//! Blobs are stored as a *manifest* (an ordered list of chunk references) plus a
//! shared pool of `zstd`-compressed, content-defined chunks. Two blobs that
//! share content share chunks on disk. Unpacked stores use this loose layout
//! under the configured base path:
//!
//! ```text
//! blobs/b3/<xx>/<hex>   manifest: [(chunk-digest, size)]   (keyed by blob digest)
//! chunks/b3/<xx>/<hex>  zstd(chunk bytes)                  (keyed by chunk digest)
//! ```
//!
//! [`ChunkedBlobStore::packed`] instead groups the same compressed chunk
//! frames into immutable `packs/b3/<xx>/<pack-hash>` objects. Each pack ends
//! in a self-describing footer, so its derived in-memory index is rebuilt from
//! object-store tail reads and each chunk remains one exact range read. Packed
//! stores use their catalog as the authoritative chunk and manifest inventory;
//! they never probe or scan the loose chunk namespace.
//!
//! A blob's identity is the BLAKE3 hash of its full content, independent of how
//! it happens to be chunked. Single-chunk blobs elide their manifest entirely:
//! the chunk digest equals the blob digest (both hash the same bytes), so the
//! chunk alone is the blob and readers fall back from the manifest path to the
//! chunk path. Multi-chunk blobs store their true manifest, and the empty blob
//! stores an empty one so its presence stays an object lookup. Elision halves
//! the object count (and the per-object write cost) for small-file-heavy
//! trees.

use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use object_store::{
    Attribute, Attributes, ObjectStore, ObjectStoreExt, PutOptions, PutPayload, path::Path,
};

use super::chunk_index::ChunkIndex;
use super::chunked_reader::{ChunkSource, ChunkedReader};
use super::local_durability::LocalDurability;
use super::pack::{PackReadStats, PackedChunks};
use super::{
    BlobBatchGuard, BlobChunkSource, BlobIntegrityError, BlobReader, BlobStore, BlobStreamReader,
    BlobSync, BlobWriter, ChunkMeta, MAX_CHUNK_SIZE,
};
use crate::digest::{BlobId, ChunkId, Digest};
use crate::error::Error;

/// Default average FastCDC chunk size (256 KiB). Min/max are avg/2 and avg*2.
pub const DEFAULT_AVG_CHUNK_SIZE: u32 = 256 * 1024;

/// Maximum number of concurrent existence probes, independent of uploads.
const CONCURRENT_CHUNK_PROBES: usize = 32;
const DEFAULT_CHUNK_UPLOAD_CONCURRENCY: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(32).unwrap();

/// Default aggregate uncompressed chunk working set shared by all clones of a
/// [`ChunkedBlobStore`]. Per-writer concurrency remains separately bounded.
pub const DEFAULT_CHUNK_MEMORY_BUDGET_BYTES: usize = 64 * 1024 * 1024;

/// The cache policy for content-addressed objects: their bytes can never
/// change under a given key, so CDNs and browsers may cache them forever.
const IMMUTABLE_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";

/// The largest a zstd frame header can be (`ZSTD_FRAMEHEADERSIZE_MAX`): magic,
/// the frame header descriptor, the window descriptor, a dictionary id, and
/// the frame content size. Reading this prefix is enough to size any frame
/// that declares its content size.
const ZSTD_FRAME_HEADER_MAX: u64 = 18;

/// Keep short, bounded decompression and hashing on the async worker. Below
/// these limits the blocking-pool handoff costs substantially more than the
/// CPU work itself; larger or unsized frames retain the blocking path so one
/// read cannot monopolize an executor worker.
const INLINE_CHUNK_OUTPUT_MAX: usize = 64 * 1024;
const INLINE_CHUNK_INPUT_MAX: usize = 128 * 1024;
const STREAM_DECODE_BLOCK_BYTES: usize = 256 * 1024;
const STREAM_CHANNEL_DEPTH: usize = 2;

pub(super) fn stream_chunk_working_set(compressed_len: usize, exact_size: Option<usize>) -> usize {
    let inline = exact_size.is_some_and(|size| size <= INLINE_CHUNK_OUTPUT_MAX)
        && compressed_len <= INLINE_CHUNK_INPUT_MAX;
    if inline {
        return exact_size.unwrap_or_default();
    }
    STREAM_DECODE_BLOCK_BYTES.saturating_add(
        exact_size
            .unwrap_or(MAX_CHUNK_SIZE as usize)
            .min(STREAM_DECODE_BLOCK_BYTES.saturating_mul(STREAM_CHANNEL_DEPTH)),
    )
}

/// PUT `payload`, attaching the immutable cache header when asked. Callers
/// writing mutable data must never pass `immutable = true`.
pub(crate) async fn put_object(
    object_store: &Arc<dyn ObjectStore>,
    path: &Path,
    payload: impl Into<PutPayload>,
    immutable: bool,
) -> Result<(), object_store::Error> {
    #[cfg(test)]
    super::crash_tests::object_checkpoint("before-put", path);
    let payload = payload.into();
    if immutable {
        let mut attributes = Attributes::new();
        attributes.insert(Attribute::CacheControl, IMMUTABLE_CACHE_CONTROL.into());
        match object_store
            .put_opts(
                path,
                payload.clone(),
                PutOptions {
                    attributes,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => {}
            Err(object_store::Error::NotImplemented { .. }) => {
                // LocalFileSystem has immutable bytes but no HTTP metadata.
                // Preserve the storage guarantee while omitting an attribute
                // meaningful only to HTTP caches.
                object_store.put(path, payload).await?;
            }
            Err(error) => return Err(error),
        }
    } else {
        object_store.put(path, payload).await?;
    }
    #[cfg(test)]
    super::crash_tests::object_checkpoint("after-put", path);
    Ok(())
}

mod manifest;
mod overwrite;
mod page_gc;
mod pages;
mod proof;
mod upload;
mod verified;
mod writer;
use manifest::decode_manifest;
pub(crate) use manifest::decompress_capped;
#[cfg(test)]
use manifest::encode_manifest;

fn decompress_verified_chunk(
    compressed: &[u8],
    digest: ChunkId,
    limit: usize,
    exact_size: Option<usize>,
) -> io::Result<Vec<u8>> {
    // A declared size is also the work/allocation cap, not merely a check
    // after decoding. A corrupt small declaration must not move a large
    // expansion onto the inline path.
    let limit = exact_size.map_or(limit, |size| limit.min(size));
    let data = decompress_capped(compressed, limit).map_err(|error| {
        io::Error::other(BlobIntegrityError::Chunk {
            chunk: digest,
            reason: error.to_string(),
        })
    })?;
    if exact_size.is_some_and(|size| data.len() != size) {
        return Err(io::Error::other(BlobIntegrityError::Chunk {
            chunk: digest,
            reason: "length does not match its declared size".to_owned(),
        }));
    }
    let got = ChunkId::new(blake3::hash(&data).into());
    if got != digest {
        return Err(io::Error::other(BlobIntegrityError::Chunk {
            chunk: digest,
            reason: "contents do not match digest".to_owned(),
        }));
    }
    Ok(data)
}

pub(crate) async fn decode_guarded(
    compressed: Bytes,
    digest: ChunkId,
    size: u64,
    guard: impl Send + 'static,
) -> io::Result<Bytes> {
    let size = usize::try_from(size).map_err(io::Error::other)?;
    if size <= INLINE_CHUNK_OUTPUT_MAX && compressed.len() <= INLINE_CHUNK_INPUT_MAX {
        return decompress_verified_chunk(&compressed, digest, size, Some(size)).map(Bytes::from);
    }
    let mut task = DecodeTask(Some(tokio::task::spawn_blocking(move || {
        let _guard = guard;
        decompress_verified_chunk(&compressed, digest, size, Some(size)).map(Bytes::from)
    })));
    let result = task.0.as_mut().unwrap().await;
    task.0.take();
    result.map_err(io::Error::other)?
}

// A cancelled caller cannot stop a running blocking decoder. Retain its guard
// until completion, and include that completion in repository lease draining.
struct DecodeTask(Option<tokio::task::JoinHandle<io::Result<Bytes>>>);
impl Drop for DecodeTask {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
            if tokio::runtime::Handle::try_current().is_ok() {
                crate::metadata::spawn_lease_task(async move {
                    match task.await {
                        Ok(_) => Ok(()), // Decode errors belonged to the cancelled reader.
                        Err(error) if error.is_cancelled() => Ok(()),
                        Err(error) => {
                            Err(crate::metadata::MetadataError::Backend(error.to_string()))
                        }
                    }
                });
            }
        }
    }
}

async fn decompress_verified_chunk_adaptive(
    compressed: Bytes,
    digest: ChunkId,
    limit: usize,
    exact_size: Option<usize>,
) -> io::Result<Vec<u8>> {
    let inline = exact_size.is_some_and(|size| size <= INLINE_CHUNK_OUTPUT_MAX)
        && compressed.len() <= INLINE_CHUNK_INPUT_MAX;
    if inline {
        return decompress_verified_chunk(&compressed, digest, limit, exact_size);
    }
    tokio::task::spawn_blocking(move || {
        decompress_verified_chunk(&compressed, digest, limit, exact_size)
    })
    .await
    .map_err(io::Error::other)?
}

fn decompress_verified_chunk_stream(
    compressed: Bytes,
    digest: ChunkId,
    limit: usize,
    exact_size: Option<usize>,
) -> BoxStream<'static, io::Result<Bytes>> {
    decompress_verified_chunk_stream_guarded(compressed, digest, limit, exact_size, ())
}

pub(super) fn decompress_verified_chunk_stream_guarded<G: Send + 'static>(
    compressed: Bytes,
    digest: ChunkId,
    limit: usize,
    exact_size: Option<usize>,
    guard: G,
) -> BoxStream<'static, io::Result<Bytes>> {
    let inline = exact_size.is_some_and(|size| size <= INLINE_CHUNK_OUTPUT_MAX)
        && compressed.len() <= INLINE_CHUNK_INPUT_MAX;
    if inline {
        return Box::pin(futures::stream::once(async move {
            let _guard = guard;
            decompress_verified_chunk(&compressed, digest, limit, exact_size).map(Bytes::from)
        }));
    }

    let (tx, rx) = tokio::sync::mpsc::channel::<io::Result<Bytes>>(STREAM_CHANNEL_DEPTH);
    tokio::spawn(async move {
        let worker_tx = tx.clone();
        let result = tokio::task::spawn_blocking(move || -> io::Result<()> {
            let _guard = guard;
            use std::io::Read as _;

            let limit = exact_size.map_or(limit, |size| limit.min(size));
            let mut decoder = zstd::Decoder::new(&compressed[..]).map_err(|error| {
                io::Error::other(BlobIntegrityError::Chunk {
                    chunk: digest,
                    reason: error.to_string(),
                })
            })?;
            let mut hasher = blake3::Hasher::new();
            let mut total = 0usize;
            let mut buffer = vec![0u8; STREAM_DECODE_BLOCK_BYTES];
            while total < limit {
                let wanted = buffer.len().min(limit - total);
                let read = decoder.read(&mut buffer[..wanted]).map_err(|error| {
                    io::Error::other(BlobIntegrityError::Chunk {
                        chunk: digest,
                        reason: error.to_string(),
                    })
                })?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
                total += read;
                if worker_tx
                    .blocking_send(Ok(Bytes::copy_from_slice(&buffer[..read])))
                    .is_err()
                {
                    return Ok(());
                }
            }
            let mut extra = [0u8; 1];
            if decoder.read(&mut extra).map_err(|error| {
                io::Error::other(BlobIntegrityError::Chunk {
                    chunk: digest,
                    reason: error.to_string(),
                })
            })? != 0
            {
                return Err(io::Error::other(BlobIntegrityError::Chunk {
                    chunk: digest,
                    reason: "decompressed chunk exceeds size limit".to_owned(),
                }));
            }
            if exact_size.is_some_and(|size| total != size) {
                return Err(io::Error::other(BlobIntegrityError::Chunk {
                    chunk: digest,
                    reason: "length does not match its declared size".to_owned(),
                }));
            }
            let got = ChunkId::new(hasher.finalize().into());
            if got != digest {
                return Err(io::Error::other(BlobIntegrityError::Chunk {
                    chunk: digest,
                    reason: "contents do not match digest".to_owned(),
                }));
            }
            Ok(())
        })
        .await;

        let error = match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error),
            Err(error) => Some(io::Error::other(error)),
        };
        if let Some(error) = error {
            let _ = tx.send(Err(error)).await;
        }
    });
    Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))
}

/// A blob service backed by an [`ObjectStore`], with FastCDC chunking and
/// global chunk deduplication.
#[derive(Clone)]
pub struct ChunkedBlobStore {
    object_store: Arc<dyn ObjectStore>,
    base_path: Path,
    avg_chunk_size: u32,
    pub(crate) chunk_upload_concurrency: std::num::NonZeroUsize,
    chunk_index: ChunkIndex,
    packed_chunks: Option<Arc<PackedChunks>>,
    batch_depth: Arc<AtomicUsize>,
    immutable_cache: bool,
    chunk_memory_budget: crate::byte_budget::ByteBudget,
    pins: crate::metadata::PinBindings,
    /// Shared with every component that deletes; see [`BlobStore::order_deletions_after`].
    deletions: super::deletion_barrier::DeletionBarrier,
}

impl ChunkedBlobStore {
    #[cfg(test)]
    pub(crate) fn benchmark_packed(&self) -> Arc<PackedChunks> {
        self.packed_chunks.as_ref().unwrap().clone()
    }

    /// Build a service over an existing object store, storing under `base_path`.
    /// Chunk sizes are clamped to FastCDC's supported ranges and rounded down
    /// to even values. The minimum is half the average and the maximum twice
    /// the average, subject to those bounds and rounding.
    pub fn new(object_store: Arc<dyn ObjectStore>, base_path: Path, avg_chunk_size: u32) -> Self {
        let pins = crate::metadata::PinBindings::default();
        let deletions = super::deletion_barrier::DeletionBarrier::default();
        let object_store = super::pinned_store::PinnedObjectStore::wrap(
            object_store,
            pins.clone(),
            deletions.clone(),
        );
        Self {
            object_store,
            base_path,
            avg_chunk_size,
            chunk_upload_concurrency: DEFAULT_CHUNK_UPLOAD_CONCURRENCY,
            chunk_index: ChunkIndex::default(),
            packed_chunks: None,
            batch_depth: Arc::new(AtomicUsize::new(0)),
            immutable_cache: false,
            chunk_memory_budget: crate::byte_budget::ByteBudget::new(
                DEFAULT_CHUNK_MEMORY_BUDGET_BYTES,
            ),
            pins,
            deletions,
        }
    }

    /// Build a packed service with explicit pack and compressed-chunk cache tuning.
    /// Blob manifests and Bao outboards remain ordinary content-addressed objects.
    pub async fn packed_with_options(
        object_store: Arc<dyn ObjectStore>,
        base_path: Path,
        avg_chunk_size: u32,
        options: crate::PackOptions,
    ) -> Result<Self, Error> {
        Self::open_packed(object_store, base_path, avg_chunk_size, options, None, None).await
    }

    #[tracing::instrument(
        name = "blob.packed.open",
        skip_all,
        fields(pack_target_bytes = options.target_size, pack_cache_bytes = options.cache_capacity)
    )]
    async fn open_packed(
        object_store: Arc<dyn ObjectStore>,
        base_path: Path,
        avg_chunk_size: u32,
        options: crate::PackOptions,
        local_durability: Option<LocalDurability>,
        catalog: Option<&[u8]>,
    ) -> Result<Self, Error> {
        let crate::PackOptions {
            target_size: pack_target_size,
            cache_capacity: pack_cache_capacity,
        } = options;
        let pins = crate::metadata::PinBindings::default();
        let deletions = super::deletion_barrier::DeletionBarrier::default();
        let object_store = super::pinned_store::PinnedObjectStore::wrap(
            object_store,
            pins.clone(),
            deletions.clone(),
        );
        let local_durability = local_durability.map(|local| {
            local
                .with_pins(pins.clone())
                .with_deletion_barrier(deletions.clone())
        });
        let packed_chunks = PackedChunks::open_with_initial_catalog(
            object_store.clone(),
            base_path.clone(),
            pack_target_size,
            pack_cache_capacity,
            catalog,
            local_durability,
        )
        .await?;
        Ok(Self {
            object_store,
            base_path,
            avg_chunk_size,
            chunk_upload_concurrency: DEFAULT_CHUNK_UPLOAD_CONCURRENCY,
            chunk_index: ChunkIndex::default(),
            packed_chunks: Some(packed_chunks),
            batch_depth: Arc::new(AtomicUsize::new(0)),
            immutable_cache: false,
            chunk_memory_budget: crate::byte_budget::ByteBudget::new(
                DEFAULT_CHUNK_MEMORY_BUDGET_BYTES,
            ),
            pins,
            deletions,
        })
    }

    /// Open the exact catalog from a protected metadata snapshot, bypassing
    /// advisory-pointer and inventory discovery. Keep snapshot protection through
    /// repository construction so collection cannot retire the catalog meanwhile.
    pub async fn packed_with_catalog(
        object_store: Arc<dyn ObjectStore>,
        base_path: Path,
        avg_chunk_size: u32,
        options: crate::PackOptions,
        catalog: &[u8],
    ) -> Result<Self, Error> {
        Self::open_packed(
            object_store,
            base_path,
            avg_chunk_size,
            options,
            None,
            Some(catalog),
        )
        .await
    }

    pub(crate) fn empty_state_catalog() -> io::Result<Vec<u8>> {
        PackedChunks::empty_state_catalog()
    }

    pub(crate) async fn externalize_state_catalog(&self, catalog: &[u8]) -> Result<Vec<u8>, Error> {
        self.packed_chunks
            .as_ref()
            .ok_or_else(|| io::Error::other("external catalogs require packed storage"))?
            .externalize_state_catalog(catalog)
            .await
            .map_err(Into::into)
    }

    /// Build a packed service with the default remote pack target and bounded cache.
    pub async fn packed(
        object_store: Arc<dyn ObjectStore>,
        base_path: Path,
        avg_chunk_size: u32,
    ) -> Result<Self, Error> {
        Self::packed_with_options(
            object_store,
            base_path,
            avg_chunk_size,
            crate::PackOptions::default(),
        )
        .await
    }

    /// Build a local packed service with the smaller local pack target and bounded cache.
    pub async fn local_packed(root: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        Self::local_packed_with_options(root, crate::PackOptions::local()).await
    }

    /// Build a local packed service with explicit pack and compressed-chunk cache tuning.
    pub async fn local_packed_with_options(
        root: impl AsRef<std::path::Path>,
        options: crate::PackOptions,
    ) -> Result<Self, Error> {
        Self::local_packed_with_catalog(root, options, None).await
    }

    pub(crate) async fn local_packed_with_catalog(
        root: impl AsRef<std::path::Path>,
        options: crate::PackOptions,
        catalog: Option<&[u8]>,
    ) -> Result<Self, Error> {
        let root = root.as_ref();
        let fs = object_store::local::LocalFileSystem::new_with_prefix(root)
            .map_err(io::Error::other)?
            .with_fsync(true);
        let local_durability = LocalDurability::new(fs.clone(), root)?;
        Self::open_packed(
            Arc::new(fs),
            Path::default(),
            DEFAULT_AVG_CHUNK_SIZE,
            options,
            Some(local_durability),
            catalog,
        )
        .await
    }

    /// Make every payload staged by this process durable. Loose stores are
    /// durable after each write; packed stores seal their partial pack.
    pub async fn flush(&self) -> Result<(), Error> {
        self.publication().flush().await
    }

    /// Return pack-object I/O counters when this store uses packed chunks.
    pub fn pack_read_stats(&self) -> Option<PackReadStats> {
        self.packed_chunks
            .as_ref()
            .map(|packed| packed.read_stats())
    }

    /// Reset pack-object I/O counters without dropping cached bytes.
    pub fn reset_pack_read_stats(&self) {
        if let Some(packed) = &self.packed_chunks {
            packed.reset_read_stats();
        }
    }

    #[cfg(test)]
    pub(crate) fn set_pack_catalog_rebase_run_bytes_for_test(&self, bytes: u64) {
        self.packed_chunks
            .as_ref()
            .expect("test rebase threshold requires packed chunks")
            .set_catalog_rebase_run_bytes_for_test(bytes);
    }

    /// Attach `Cache-Control: public, max-age=31536000, immutable` to every
    /// content object written (chunks, manifests, outboards), so a bucket
    /// served through a CDN caches them forever. For backends that support
    /// object attributes such as S3 and R2. Repository state is stored by the
    /// separate [`MetadataStore`](crate::MetadataStore) backend and is never covered
    /// by this setting.
    pub fn with_immutable_cache_control(mut self) -> Self {
        self.immutable_cache = true;
        self
    }

    /// Set the maximum in-flight chunk uploads per new blob writer (default 32).
    /// The shared chunk memory budget can further reduce concurrency. This does
    /// not change existence-probe concurrency or the number of file writers.
    pub fn with_chunk_upload_concurrency(mut self, concurrency: std::num::NonZeroUsize) -> Self {
        self.chunk_upload_concurrency = concurrency;
        self
    }

    /// Set the aggregate uncompressed chunk working-set budget shared by this
    /// store and clones made from it for in-flight decoding and uploads.
    /// Repeatedly sought readers may additionally retain verified decoded bytes:
    /// at most 2 MiB per reader and 32 MiB across the process, including bytes
    /// held by active reads. This separate nonblocking cache never holds decode
    /// permits. The in-flight limit is rounded up to 64 KiB units;
    /// zero therefore still admits one unit so progress is always possible.
    pub fn with_chunk_memory_budget_bytes(mut self, bytes: usize) -> Self {
        self.chunk_memory_budget = crate::byte_budget::ByteBudget::new(bytes);
        self
    }

    /// Build a service backed by a local filesystem directory (which must
    /// already exist).
    pub fn local(root: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        let fs = object_store::local::LocalFileSystem::new_with_prefix(root)
            .map_err(io::Error::other)?;
        Ok(Self::new(
            Arc::new(fs),
            Path::default(),
            DEFAULT_AVG_CHUNK_SIZE,
        ))
    }

    fn blob_path(&self, digest: &BlobId) -> Path {
        blob_path(&self.base_path, digest)
    }

    fn chunk_path(&self, digest: &ChunkId) -> Path {
        chunk_path(&self.base_path, digest)
    }

    fn outboard_path(&self, digest: &BlobId) -> Path {
        sharded_path(&self.base_path, "bao", digest.as_digest())
    }

    async fn compressed_chunk(&self, digest: &ChunkId) -> io::Result<Option<Bytes>> {
        if let Some(packed) = &self.packed_chunks {
            return packed.get(digest).await;
        }
        match self.object_store.get(&self.chunk_path(digest)).await {
            Ok(result) => Ok(Some(result.bytes().await.map_err(io::Error::other)?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(io::Error::other(error)),
        }
    }

    /// Whether a chunk is present, refreshing a packed catalog miss once or
    /// consulting the loose in-process index before a `HEAD`.
    async fn chunk_present(&self, digest: ChunkId) -> io::Result<bool> {
        if let Some(packed) = &self.packed_chunks {
            if packed.probe(&digest).await? {
                return Ok(true);
            }
            if self.batch_depth.load(std::sync::atomic::Ordering::Acquire) > 0 {
                return Ok(false);
            }
            // A different S3/wal3 writer may have published a pack since this
            // process opened. Refresh once so manifest-elided blobs become
            // visible cross-process without a per-chunk object request.
            packed.refresh().await?;
            return packed.probe(&digest).await;
        }
        if self.chunk_index.contains(&digest) {
            return Ok(true);
        }
        let present = head_exists(&self.object_store, &self.chunk_path(&digest)).await?;
        if present {
            self.chunk_index.insert(digest);
        }
        Ok(present)
    }

    async fn chunk_present_for_write(&self, digest: ChunkId) -> io::Result<bool> {
        if let Some(packed) = &self.packed_chunks {
            return packed.probe_for_write(&digest).await;
        }
        let pins = self.pins.capture();
        if pins.is_empty() {
            return self.chunk_present(digest).await;
        }
        let path = self.chunk_path(&digest);
        let resource = crate::metadata::PinResource::StorageObject(path.to_string());
        pins.protect(std::collections::BTreeSet::from([
            crate::metadata::PinResource::Chunk(digest),
            resource.clone(),
        ]))
        .await?;
        if pins.known_present(&resource) {
            return Ok(true);
        }
        let present = head_exists(&self.object_store, &path).await?;
        if present {
            pins.remember_present(resource);
            self.chunk_index.insert(digest);
        }
        Ok(present)
    }

    /// Metadata for a digest stored only as a bare chunk: an elided
    /// single-chunk blob, or an interior chunk of some other blob addressed
    /// as content. Either way the chunk is the whole content; its
    /// uncompressed size comes from the zstd frame header, which is always
    /// present because chunks are compressed from complete buffers.
    ///
    /// The header is the first few bytes, so this reads a range, not the
    /// object: `chunks()` on a manifest-elided blob must not download the
    /// blob. (`get_range` clamps a range past the end, and a zstd frame is
    /// never empty, so a short chunk comes back whole.)
    async fn bare_chunk_meta(&self, digest: &ChunkId) -> io::Result<Option<ChunkMeta>> {
        if let Some(packed) = &self.packed_chunks {
            return Ok(packed.metadata(digest).await?.map(|size| ChunkMeta {
                digest: *digest,
                size,
            }));
        }
        let path = self.chunk_path(digest);
        let header = match self
            .object_store
            .get_range(&path, 0..ZSTD_FRAME_HEADER_MAX)
            .await
        {
            Ok(bytes) => bytes,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(io::Error::other(e)),
        };
        let declared = zstd::zstd_safe::get_frame_content_size(&header).map_err(|e| {
            io::Error::other(BlobIntegrityError::Chunk {
                chunk: *digest,
                reason: format!("bad zstd frame: {e:?}"),
            })
        })?;
        if let Some(size) = declared {
            return Ok(Some(ChunkMeta {
                digest: *digest,
                size,
            }));
        }

        // frames from writers that streamed (or another store's chunks) may
        // not declare a content size; measure by capped decompression.
        let compressed = match self.object_store.get(&path).await {
            Ok(res) => res.bytes().await.map_err(io::Error::other)?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(io::Error::other(e)),
        };
        let digest = *digest;
        let size = tokio::task::spawn_blocking(move || {
            decompress_capped(&compressed, MAX_CHUNK_SIZE as usize).map_err(|error| {
                io::Error::other(BlobIntegrityError::Chunk {
                    chunk: digest,
                    reason: error.to_string(),
                })
            })
        })
        .await
        .map_err(io::Error::other)??
        .len() as u64;
        Ok(Some(ChunkMeta { digest, size }))
    }

    /// Read and verify a digest stored only as a bare chunk (no manifest): an
    /// elided single-chunk blob, or a bare/interior chunk of some other blob
    /// addressed as content. Unlike the manifest path, there is no later EOF
    /// check to catch corruption, so the whole chunk is decompressed and
    /// hash-checked here, eagerly.
    async fn open_bare_chunk(&self, digest: &BlobId) -> Result<Option<Box<dyn BlobReader>>, Error> {
        self.open_bare_chunk_from(digest, None).await
    }

    // A scoped catalog resolves the location; the original backend may still
    // serve its immutable pack bytes from the shared bounded payload cache.
    async fn open_bare_chunk_from(
        &self,
        digest: &BlobId,
        snapshot: Option<&super::pack::CatalogSnapshot>,
    ) -> Result<Option<Box<dyn BlobReader>>, Error> {
        let chunk = single_chunk_id(*digest);
        let compressed = if let Some(packed) = &self.packed_chunks {
            let bytes = if let Some(snapshot) = snapshot {
                snapshot.read_bare_chunk(&chunk).await?
            } else {
                packed.get(&chunk).await?
            };
            let Some(bytes) = bytes else {
                return Ok(None);
            };
            bytes
        } else {
            match self.object_store.get(&self.chunk_path(&chunk)).await {
                Ok(res) => res.bytes().await.map_err(io::Error::other)?,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(e) => return Err(io::Error::other(e).into()),
            }
        };
        let want = chunk;
        // a legitimate chunk decompresses to at most the FastCDC max (avg*2)
        // when cut locally, or MAX_CHUNK_SIZE when it arrived via sync from a
        // store with larger chunking parameters (manifests travel verbatim);
        // cap there so a hostile or corrupt chunk cannot inflate without
        // bound before the digest check runs.
        let limit = (self.avg_chunk_size as usize)
            .saturating_mul(4)
            .max(MAX_CHUNK_SIZE as usize);
        // Complete-buffer writers include the uncompressed size in the zstd
        // frame. That lets short reads avoid a blocking-pool round trip while
        // old/streamed frames with no declaration retain the conservative
        // blocking path.
        let exact_size = zstd::zstd_safe::get_frame_content_size(&compressed)
            .map_err(|error| {
                io::Error::other(BlobIntegrityError::Chunk {
                    chunk: want,
                    reason: format!("bad zstd frame: {error:?}"),
                })
            })?
            .and_then(|size| usize::try_from(size).ok());
        let data = decompress_verified_chunk_adaptive(compressed, want, limit, exact_size).await?;
        Ok(Some(Box::new(io::Cursor::new(data))))
    }

    /// Fetch and decode the manifest stored for `digest`, or `None` if there
    /// is no manifest (the digest is not a stored blob).
    async fn manifest(&self, digest: &BlobId) -> io::Result<Option<Vec<ChunkMeta>>> {
        match self.object_store.get(&self.blob_path(digest)).await {
            Ok(res) => {
                let bytes = res.bytes().await.map_err(io::Error::other)?;
                if let Some(root) = pages::Root::decode(&bytes)? {
                    if root.kind != pages::CHUNKS {
                        return Err(pages::invalid());
                    }
                    let cursor = pages::Cursor::new(self, root).await?;
                    return Ok(Some(cursor.pages.collect_chunks(root).await?));
                }
                Ok(Some(decode_manifest(&bytes).map_err(|error| {
                    io::Error::other(BlobIntegrityError::Manifest {
                        blob: *digest,
                        reason: error.to_string(),
                    })
                })?))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(io::Error::other(e)),
        }
    }

    /// Open only the manifest-backed representation of `digest`. `None`
    /// means the manifest object is absent; this never falls back to a
    /// same-digest chunk.
    async fn open_manifest_read(
        &self,
        digest: &BlobId,
    ) -> Result<Option<Box<dyn BlobReader>>, Error> {
        let Some(chunks) = self.manifest(digest).await? else {
            return Ok(None);
        };
        if let Some(packed) = &self.packed_chunks
            && let Some(reader) = packed
                .manifest_reader(&chunks, self.chunk_memory_budget.clone(), *digest)
                .await?
        {
            return Ok(Some(Box::new(reader)));
        }
        let source: Arc<dyn ChunkSource> = Arc::new(self.clone());
        let reader = ChunkedReader::new(
            source,
            chunks.into_iter().map(|chunk| (chunk.digest, chunk.size)),
            Some(*digest),
        );
        Ok(Some(Box::new(reader)))
    }

    /// Resolve with the physical manifest as the authority. Integrity scans
    /// use this path so an optimization catalog can never hide a malformed
    /// manifest that is actually present.
    pub(crate) async fn open_read_strict(
        &self,
        digest: &BlobId,
    ) -> Result<Option<Box<dyn BlobReader>>, Error> {
        match self.open_manifest_read(digest).await? {
            Some(reader) => Ok(Some(reader)),
            None => self.open_bare_chunk(digest).await,
        }
    }

    /// Replace one blob representation from an independently verified
    /// compatible replica.
    ///
    /// Chunks are individually checked before they overwrite their
    /// content-addressed locations. The replacement manifest is published
    /// only after [`BlobSync::put_manifest`] has assembled those chunks and
    /// verified the complete `digest`. Consequently a corrupt or incomplete
    /// replica cannot replace the destination manifest.
    #[tracing::instrument(name = "blob.repair", skip_all, fields(blob = %digest))]
    pub async fn repair_from_replica(
        &self,
        replica: &ChunkedBlobStore,
        digest: &BlobId,
    ) -> Result<(), Error> {
        let chunks = replica.chunks(digest).await?.ok_or(Error::NotFound {
            digest: (*digest).into(),
        })?;
        let replica_sync = replica
            .as_blob_sync()
            .expect("ChunkedBlobStore always implements BlobSync");
        let mut copied = std::collections::HashSet::new();
        for chunk in &chunks {
            if !copied.insert(chunk.digest) {
                continue;
            }
            let compressed = replica_sync
                .get_chunk(&chunk.digest)
                .await?
                .ok_or_else(|| Error::Msg(format!("replica is missing chunk {}", chunk.digest)))?;
            // Unlike normal writes, this deliberately overwrites a known-bad
            // chunk. `put_chunk` decompresses and hashes it first.
            self.put_chunk(chunk, compressed).await?;
        }
        // This is the atomic representation publication point: it validates
        // the full assembled payload against `digest` before replacing a
        // manifest (or validates the self-chunk in the elided case).
        #[cfg(test)]
        if std::env::var_os("CASITA_TEST_ABORT_BEFORE_REPAIR_MANIFEST").is_some() {
            std::process::abort();
        }
        self.put_manifest(digest, chunks).await
    }
}

#[async_trait]
impl super::CatalogPublication for Arc<PackedChunks> {
    async fn refresh_discovery(&self) -> Result<(), Error> {
        Ok(self.refresh().await?)
    }

    #[tracing::instrument(name = "blob.flush", level = "debug", skip_all)]
    async fn flush(&self) -> Result<(), Error> {
        Ok(PackedChunks::flush(self).await?)
    }

    #[tracing::instrument(
        name = "blob.catalog.synchronize",
        level = "debug",
        skip_all,
        fields(catalog_bytes = catalog.map_or(0, <[u8]>::len))
    )]
    async fn synchronize_state_catalog(&self, catalog: Option<&[u8]>) -> Result<(), Error> {
        Ok(PackedChunks::synchronize_state_catalog(self, catalog).await?)
    }

    fn enable_state_catalog(&self) {
        PackedChunks::enable_state_catalog(self);
    }

    #[tracing::instrument(name = "blob.catalog.prepare_commit", level = "debug", skip_all)]
    async fn prepare_state_commit(&self) -> Result<super::PreparedCatalog, Error> {
        Ok(self.prepare_catalog().await?)
    }

    fn take_catalog_maintenance(&self) -> Option<super::CatalogMaintenance> {
        PackedChunks::take_catalog_maintenance(self)
    }
}

#[async_trait]
impl BlobStore for ChunkedBlobStore {
    async fn overwrite(
        &self,
        digest: &BlobId,
        size: u64,
        offset: u64,
        replacement: &[u8],
    ) -> Result<(BlobId, Bytes), Error> {
        self.overwrite_content(digest, size, offset, replacement)
            .await
    }
    async fn open_proof(
        &self,
        digest: &BlobId,
        size: u64,
    ) -> Result<Option<Box<dyn crate::blob::BlobStreamReader>>, Error> {
        self.proof_reader(digest, size).await
    }
    async fn open_proof_scoped(
        &self,
        digest: &BlobId,
        size: u64,
        pin: crate::metadata::DataPinLease,
        catalog: Option<&[u8]>,
    ) -> Result<Option<Box<dyn crate::blob::BlobStreamReader>>, Error> {
        self.scoped_proof_reader(digest, size, pin, catalog).await
    }

    fn write_scope(&self) -> crate::metadata::BackendWriteScope {
        let scope = self.pins.write_scope();
        match &self.packed_chunks {
            Some(packed) => scope.include(packed.write_scope()),
            None => scope,
        }
    }

    async fn nar_available(&self, digest: &BlobId) -> Result<bool, Error> {
        if let Some(chunks) = self.manifest(digest).await? {
            for chunk in chunks {
                if !self.chunk_present(chunk.digest).await? {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
        Ok(self.chunk_present(single_chunk_id(*digest)).await?)
    }

    async fn nar_witness(&self, digests: &[BlobId]) -> Result<Option<Vec<u8>>, Error> {
        let Some(packed) = &self.packed_chunks else {
            for digest in digests {
                if !self.nar_available(digest).await? {
                    return Ok(None);
                }
            }
            return Ok(Some(Vec::new()));
        };
        // Every payload resolves to the immutable pack files holding its
        // chunks, plus its manifest object when it has one. Those are the
        // storage objects whose existence keeps it readable, and there are
        // far fewer of them than chunks, so each is checked once.
        let mut objects = std::collections::BTreeSet::new();
        let mut durable = true;
        for digest in digests {
            let chunks = if packed.manifest_definitely_absent(digest) {
                None
            } else {
                self.manifest(digest).await?
            };
            let chunks: Vec<ChunkId> = match chunks {
                Some(chunks) => {
                    objects.insert(self.blob_path(digest).to_string());
                    chunks.into_iter().map(|chunk| chunk.digest).collect()
                }
                None => vec![single_chunk_id(*digest)],
            };
            for chunk in chunks {
                match packed.pack_path_of(&chunk).await? {
                    Some(path) => {
                        objects.insert(path.to_string());
                    }
                    None => {
                        // Staged or in flight: present, but nowhere a witness
                        // can name yet.
                        if !self.chunk_present(chunk).await? {
                            return Ok(None);
                        }
                        durable = false;
                    }
                }
            }
        }
        for path in &objects {
            if !head_exists(&self.object_store, &Path::from(path.as_str())).await? {
                return Ok(None);
            }
        }
        if !durable {
            return Ok(Some(Vec::new()));
        }
        let mut witness = WITNESS_MAGIC.to_vec();
        for path in &objects {
            witness.extend_from_slice(path.as_bytes());
            witness.push(b'\n');
        }
        Ok(Some(witness))
    }

    async fn nar_witness_holds(&self, witness: &[u8]) -> Result<bool, Error> {
        let Some(paths) = witness.strip_prefix(WITNESS_MAGIC) else {
            return Ok(false);
        };
        for path in paths
            .split(|byte| *byte == b'\n')
            .filter(|path| !path.is_empty())
        {
            let Ok(path) = std::str::from_utf8(path) else {
                return Ok(false);
            };
            if !head_exists(&self.object_store, &Path::from(path)).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn has(&self, digest: &BlobId) -> Result<bool, Error> {
        // a stored blob has a manifest; a single-chunk blob elides its manifest,
        // so a bare chunk at the blob's own digest is that blob's content and is
        // readable in its own right (see the module docs, manifest elision).
        if self
            .packed_chunks
            .as_ref()
            .is_some_and(|packed| packed.manifest_definitely_absent(digest))
        {
            return Ok(self.chunk_present(single_chunk_id(*digest)).await?);
        }
        if head_exists(&self.object_store, &self.blob_path(digest)).await? {
            return Ok(true);
        }
        Ok(self.chunk_present(single_chunk_id(*digest)).await?)
    }

    async fn has_batch(&self, digests: &[BlobId]) -> Result<Vec<bool>, Error> {
        // Bound outstanding probes and preserve input order. A slow earlier
        // probe can delay admission even when later probes have completed.
        let probes: Vec<_> = digests.iter().map(|d| self.has(d)).collect();
        let present: Vec<bool> = futures::stream::iter(probes)
            .buffered(CONCURRENT_CHUNK_PROBES)
            .try_collect()
            .await?;
        Ok(present)
    }

    #[tracing::instrument(name = "blob.read", level = "debug", skip_all)]
    async fn open_read(&self, digest: &BlobId) -> Result<Option<Box<dyn BlobReader>>, Error> {
        // A complete packed-store catalog can prove that the manifest lookup
        // would miss. The same-digest chunk is then the only committed
        // representation, so verify and return it directly. Legacy or
        // incomplete catalogs deliberately retain manifest-first resolution.
        if self
            .packed_chunks
            .as_ref()
            .is_some_and(|packed| packed.manifest_definitely_absent(digest))
        {
            match self.open_bare_chunk(digest).await {
                Ok(Some(reader)) => return Ok(Some(reader)),
                Ok(None) => {}
                Err(bare_error) => {
                    // The catalog may be stale while another writer repairs or
                    // publishes an alternate representation. Prefer a newly
                    // visible manifest; preserve the original chunk error when
                    // no manifest exists.
                    return match self.open_manifest_read(digest).await {
                        Ok(Some(reader)) => Ok(Some(reader)),
                        Ok(None) => Err(bare_error),
                        Err(manifest_error) => Err(manifest_error),
                    };
                }
            }
        }

        // a blob is its manifest plus the chunks it references; reconstruct it
        // through a ChunkedReader that fetches each chunk via ChunkSource. The
        // reader verifies the assembled bytes against `digest` at EOF, so a
        // tampered manifest cannot serve content that is not this blob. This
        // covers the empty blob (an empty manifest) and multi-chunk blobs
        // uniformly.
        self.open_read_strict(digest).await
    }

    async fn open_read_scoped(
        &self,
        digest: &BlobId,
        pin: crate::metadata::DataPinLease,
        catalog: Option<&[u8]>,
    ) -> Result<Option<Box<dyn BlobReader>>, Error> {
        // Resolve against the protected catalog, not shared mutable backend
        // state that another operation may synchronize while we await I/O.
        let snapshot = match &self.packed_chunks {
            Some(packed) => {
                let catalog = catalog.ok_or_else(|| {
                    Error::Msg("scoped packed read requires a protected catalog".into())
                })?;
                Some(packed.scoped_catalog(catalog, pin.clone()).await?)
            }
            None => None,
        };
        // Only a fully buffered bare-chunk reader may bypass the pinned plan.
        // If the fast path contradicts a manifest, that manifest still needs
        // its locations resolved against this exact snapshot.
        let chunks = if snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.manifest_definitely_absent(digest))
        {
            match self.open_bare_chunk_from(digest, snapshot.as_ref()).await {
                Ok(reader) => return Ok(reader),
                Err(error) => self.manifest(digest).await?.ok_or(error)?,
            }
        } else {
            match self.manifest(digest).await? {
                Some(chunks) => chunks,
                None => return self.open_bare_chunk_from(digest, snapshot.as_ref()).await,
            }
        };
        if let Some(snapshot) = snapshot {
            let plan = snapshot.prepare_read(&chunks).await?;
            return Ok(Some(Box::new(plan.into_reader(
                chunks,
                self.chunk_memory_budget.clone(),
                *digest,
            ))));
        }
        let mut resources = std::collections::BTreeSet::new();
        for chunk in &chunks {
            resources.insert(crate::metadata::PinResource::Chunk(chunk.digest));
            resources.insert(crate::metadata::PinResource::StorageObject(
                self.chunk_path(&chunk.digest).to_string(),
            ));
        }
        pin.protect(resources).await.map_err(io::Error::other)?;
        let source: Arc<dyn ChunkSource> = Arc::new(PinnedLooseChunks {
            store: self.clone(),
            _pin: pin,
        });
        Ok(Some(Box::new(ChunkedReader::new(
            source,
            chunks.into_iter().map(|chunk| (chunk.digest, chunk.size)),
            Some(*digest),
        ))))
    }

    /// A sequential reader that must be drained or dropped, never parked: it
    /// holds a chunk of the shared plaintext budget while it yields, and that
    /// budget admits every writer of the store as well. Callers that hold a
    /// reader open across other work want `open_read`, which releases its
    /// reservation before each chunk is returned.
    async fn open_stream(
        &self,
        digest: &BlobId,
    ) -> Result<Option<Box<dyn BlobStreamReader>>, Error> {
        let expected = *digest;
        match self.manifest(digest).await? {
            Some(chunks) => {
                if let Some(packed) = &self.packed_chunks
                    && let Some(stream) = packed
                        .manifest_stream(&chunks, self.chunk_memory_budget.clone(), expected)
                        .await?
                {
                    return Ok(Some(Box::new(tokio_util::io::StreamReader::new(stream))));
                }

                let store = self.clone();
                let stream: BoxStream<'static, io::Result<Bytes>> = Box::pin(
                    async_stream::try_stream! {
                        let mut hasher = blake3::Hasher::new();
                        for chunk in chunks {
                            let compressed = store
                                .compressed_chunk(&chunk.digest)
                                .await?
                                .ok_or_else(|| {
                                    io::Error::new(io::ErrorKind::NotFound, "chunk not found")
                                })?;
                            let size = usize::try_from(chunk.size)
                                .map_err(|_| io::Error::other("chunk size does not fit in memory"))?;
                            let _memory = store
                                .chunk_memory_budget
                                .reserve(stream_chunk_working_set(compressed.len(), Some(size)))
                                .await;
                            let mut decoded = decompress_verified_chunk_stream(
                                compressed,
                                chunk.digest,
                                size,
                                Some(size),
                            );
                            while let Some(bytes) = decoded.try_next().await? {
                                hasher.update(&bytes);
                                yield bytes;
                            }
                        }
                        let got = BlobId::new(hasher.finalize().into());
                        if got != expected {
                            Err(io::Error::other(BlobIntegrityError::Blob { expected }))?;
                        }
                    },
                );
                let reader = tokio_util::io::StreamReader::new(stream);
                Ok(Some(Box::new(reader)))
            }
            None => {
                let chunk = single_chunk_id(expected);
                let Some(compressed) = self.compressed_chunk(&chunk).await? else {
                    return Ok(None);
                };
                let exact_size = zstd::zstd_safe::get_frame_content_size(&compressed)
                    .map_err(|error| {
                        io::Error::other(BlobIntegrityError::Chunk {
                            chunk,
                            reason: format!("bad zstd frame: {error:?}"),
                        })
                    })?
                    .and_then(|size| usize::try_from(size).ok());
                let limit = (self.avg_chunk_size as usize)
                    .saturating_mul(4)
                    .max(MAX_CHUNK_SIZE as usize);
                let memory_budget = self.chunk_memory_budget.clone();
                let working_set = stream_chunk_working_set(compressed.len(), exact_size);
                let stream: BoxStream<'static, io::Result<Bytes>> =
                    Box::pin(async_stream::try_stream! {
                        let _memory = memory_budget.reserve(working_set).await;
                        let mut decoded = decompress_verified_chunk_stream(
                            compressed,
                            chunk,
                            limit,
                            exact_size,
                        );
                        while let Some(bytes) = decoded.try_next().await? {
                            yield bytes;
                        }
                    });
                let reader = tokio_util::io::StreamReader::new(stream);
                Ok(Some(Box::new(reader)))
            }
        }
    }

    async fn open_write(&self) -> Box<dyn BlobWriter> {
        writer::open(
            self.object_store.clone(),
            self.base_path.clone(),
            self.chunk_index.clone(),
            self.packed_chunks.clone(),
            self.batch_depth.clone(),
            self.avg_chunk_size,
            self.immutable_cache,
            self.chunk_memory_budget.clone(),
            self.chunk_upload_concurrency,
            self.pins.capture(),
        )
    }

    fn begin_batch(&self) -> BlobBatchGuard {
        match &self.packed_chunks {
            Some(_) => BlobBatchGuard::counted(self.batch_depth.clone()),
            None => BlobBatchGuard::default(),
        }
    }

    fn begin_pinned_batch(
        &self,
        pin: crate::metadata::DataPinLease,
    ) -> Result<BlobBatchGuard, Error> {
        self.pins.attach(&pin);
        if let Some(packed) = &self.packed_chunks {
            packed.attach_pin(&pin);
        }
        Ok(self.begin_batch().with_pin(pin))
    }

    fn publication(&self) -> super::PayloadPublication<'_> {
        match &self.packed_chunks {
            Some(packed) => super::PayloadPublication::Cataloged(packed),
            None => super::PayloadPublication::Immediate,
        }
    }

    fn order_deletions_after(&self, commits: super::CommitDurability) {
        self.deletions.order_after(commits);
    }

    async fn chunks(&self, digest: &BlobId) -> Result<Option<Vec<ChunkMeta>>, Error> {
        match self.manifest(digest).await? {
            Some(chunks) => Ok(Some(chunks)),
            // no manifest: an elided single-chunk blob, or a bare chunk of
            // some other blob addressed as content. The chunk is the blob.
            None => Ok(self
                .bare_chunk_meta(&single_chunk_id(*digest))
                .await?
                .map(|meta| vec![meta])),
        }
    }

    fn as_blob_sync(&self) -> Option<&dyn BlobSync> {
        Some(self)
    }
}

/// Loose chunks need only retain the admitted object pin while being fetched.
struct PinnedLooseChunks {
    store: ChunkedBlobStore,
    _pin: crate::metadata::DataPinLease,
}

#[async_trait]
impl ChunkSource for PinnedLooseChunks {
    async fn fetch_chunk(&self, digest: ChunkId, size: u64) -> io::Result<Bytes> {
        self.store.fetch_chunk(digest, size).await
    }
}

#[async_trait]
impl ChunkSource for ChunkedBlobStore {
    async fn fetch_chunk(&self, digest: ChunkId, size: u64) -> io::Result<Bytes> {
        let _memory = self
            .chunk_memory_budget
            .reserve(usize::try_from(size).unwrap_or(usize::MAX))
            .await;
        let compressed = if let Some(packed) = &self.packed_chunks {
            packed
                .get(&digest)
                .await?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "chunk not found"))?
        } else {
            match self.object_store.get(&self.chunk_path(&digest)).await {
                Ok(res) => res.bytes().await.map_err(io::Error::other)?,
                Err(object_store::Error::NotFound { .. }) => {
                    return Err(io::Error::new(io::ErrorKind::NotFound, "chunk not found"));
                }
                Err(e) => return Err(io::Error::other(e)),
            }
        };
        // cap decompression at the manifest-recorded size (its true length),
        // not the reading store's configured average: a store reopened with a
        // smaller average must still read chunks written under a larger one.
        // The manifest size is already bounded by MAX_CHUNK_SIZE on decode.
        let limit = usize::try_from(size)
            .map_err(|_| io::Error::other("chunk size does not fit in memory"))?;
        // The manifest's exact size bounds inline work and is also checked
        // after decompression because the seek table is built from it.
        let data =
            decompress_verified_chunk_adaptive(compressed, digest, limit, Some(limit)).await?;
        Ok(Bytes::from(data))
    }
}

#[async_trait]
impl BlobChunkSource for ChunkedBlobStore {
    async fn has_chunks(&self, chunks: &[ChunkId]) -> Result<Vec<bool>, Error> {
        let requested = chunks
            .iter()
            .map(|digest| ChunkMeta {
                digest: *digest,
                size: 0,
            })
            .collect::<Vec<_>>();
        let missing = self
            .missing_chunks(&requested)
            .await?
            .into_iter()
            .map(|chunk| chunk.digest)
            .collect::<std::collections::HashSet<_>>();
        Ok(chunks
            .iter()
            .map(|chunk| !missing.contains(chunk))
            .collect())
    }

    async fn get_chunk(&self, digest: &ChunkId) -> Result<Option<Bytes>, Error> {
        if let Some(packed) = &self.packed_chunks {
            return Ok(packed.get(digest).await?);
        }
        match self.object_store.get(&self.chunk_path(digest)).await {
            Ok(res) => Ok(Some(res.bytes().await.map_err(io::Error::other)?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(io::Error::other(e).into()),
        }
    }
}

#[async_trait]
impl BlobSync for ChunkedBlobStore {
    #[tracing::instrument(name = "blob.missing_chunks", level = "debug", skip_all, fields(chunks = chunks.len()))]
    async fn missing_chunks(&self, chunks: &[ChunkMeta]) -> Result<Vec<ChunkMeta>, Error> {
        if let Some(packed) = &self.packed_chunks
            && self.batch_depth.load(std::sync::atomic::Ordering::Acquire) == 0
        {
            // One refresh covers chunks another S3 writer may have published;
            // never turn every genuinely new chunk into a bucket listing.
            packed.refresh().await?;
        }
        let mut seen = std::collections::HashSet::new();
        let unique: Vec<ChunkMeta> = chunks
            .iter()
            .filter(|c| seen.insert(c.digest))
            .cloned()
            .collect();

        let probes: Vec<_> = unique
            .iter()
            .map(|c| async move { self.chunk_present_for_write(c.digest).await })
            .collect();
        let present: Vec<bool> = futures::stream::iter(probes)
            .buffered(CONCURRENT_CHUNK_PROBES)
            .try_collect()
            .await?;

        Ok(unique
            .into_iter()
            .zip(present)
            .filter(|(_, present)| !present)
            .map(|(chunk, _)| chunk)
            .collect())
    }

    #[tracing::instrument(
        name = "blob.put_chunk",
        level = "debug",
        skip_all,
        fields(plaintext_bytes = meta.size, stored_bytes = compressed.len())
    )]
    async fn put_chunk(&self, meta: &ChunkMeta, compressed: Bytes) -> Result<(), Error> {
        if meta.size == 0 {
            return Err(
                io::Error::other("zero-length chunk; empty blobs use an empty manifest").into(),
            );
        }
        self.pins
            .capture()
            .protect(std::collections::BTreeSet::from([
                crate::metadata::PinResource::Chunk(meta.digest),
            ]))
            .await?;
        if meta.size > MAX_CHUNK_SIZE {
            return Err(io::Error::other(format!(
                "chunk {} declares {} bytes, over the {MAX_CHUNK_SIZE} byte ceiling",
                meta.digest, meta.size
            ))
            .into());
        }
        let want = meta.digest;
        let declared = meta.size;
        let _memory = self
            .chunk_memory_budget
            .reserve(usize::try_from(declared).unwrap_or(usize::MAX))
            .await;
        let inline = usize::try_from(declared).is_ok_and(|size| {
            size <= INLINE_CHUNK_OUTPUT_MAX && compressed.len() <= INLINE_CHUNK_INPUT_MAX
        });
        let verify = move || -> io::Result<Bytes> {
            let data = decompress_capped(&compressed, declared as usize)?;
            if data.len() as u64 != declared {
                return Err(io::Error::other(format!(
                    "chunk {want} decompressed to {} bytes, declared {declared}",
                    data.len()
                )));
            }
            let got = ChunkId::new(blake3::hash(&data).into());
            if got != want {
                return Err(io::Error::other(format!(
                    "chunk contents hash to {got}, declared {want}"
                )));
            }
            Ok(compressed)
        };
        // A blocking-pool handoff costs more than bounded decompression and
        // hashing for the overwhelmingly common small-file chunk.
        let verified = if inline {
            verify()?
        } else {
            tokio::task::spawn_blocking(verify)
                .await
                .map_err(io::Error::other)??
        };

        if let Some(packed) = &self.packed_chunks {
            packed.put(meta.clone(), verified).await?;
        } else {
            put_object(
                &self.object_store,
                &self.chunk_path(&want),
                verified,
                self.immutable_cache,
            )
            .await
            .map_err(io::Error::other)?;
        }
        self.chunk_index.insert(want);
        Ok(())
    }

    #[tracing::instrument(name = "blob.put_manifest", level = "debug", skip_all, fields(chunks = chunks.len()))]
    async fn put_manifest(&self, blob: &BlobId, chunks: Vec<ChunkMeta>) -> Result<(), Error> {
        if chunks.iter().any(|chunk| chunk.size == 0) {
            return Err(io::Error::other("chunk manifest: zero-length chunk").into());
        }
        self.pins
            .capture()
            .protect(std::collections::BTreeSet::from([
                crate::metadata::PinResource::Blob(*blob),
            ]))
            .await?;
        if let Some(bad) = chunks.iter().find(|c| c.size > MAX_CHUNK_SIZE) {
            return Err(io::Error::other(format!(
                "manifest for {blob} declares chunk {} at {} bytes, over the \
                 {MAX_CHUNK_SIZE} byte ceiling",
                bad.digest, bad.size
            ))
            .into());
        }

        if !self.pins.capture().is_empty() {
            for chunk in &chunks {
                if !self.chunk_present_for_write(chunk.digest).await? {
                    return Err(io::Error::other(format!(
                        "manifest for {blob} references missing chunk {}",
                        chunk.digest
                    ))
                    .into());
                }
            }
        }

        // manifest elision carries through sync: a single self-chunk manifest
        // is redundant (identity is the binding), so require the chunk and
        // store nothing.
        if chunks.len() == 1 && chunks[0].digest == single_chunk_id(*blob) {
            if !self.chunk_present(single_chunk_id(*blob)).await? {
                return Err(io::Error::other(format!(
                    "manifest for {blob} references missing chunk {blob}"
                ))
                .into());
            }
            if chunks[0].size > crate::verified::ingest::BLOCK_BYTES as u64 {
                self.build_outboard(blob).await?;
            }
            if let Some(packed) = &self.packed_chunks
                && self.batch_depth.load(std::sync::atomic::Ordering::Acquire) == 0
            {
                packed.flush().await?;
            }
            return Ok(());
        }

        // verify the binding before the manifest becomes visible: the
        // assembled chunks must hash to the blob digest. One sequential pass
        // over chunks that just arrived (page-cache warm).
        let mut hasher = crate::verified::ingest::IngestHasher::default();
        for chunk in &chunks {
            let _memory = self
                .chunk_memory_budget
                .reserve(usize::try_from(chunk.size).unwrap_or(usize::MAX))
                .await;
            let compressed = BlobChunkSource::get_chunk(self, &chunk.digest)
                .await?
                .ok_or_else(|| {
                    io::Error::other(format!(
                        "manifest for {blob} references missing chunk {}",
                        chunk.digest
                    ))
                })?;
            let declared = chunk.size;
            let digest = chunk.digest;
            hasher = tokio::task::spawn_blocking(
                move || -> io::Result<crate::verified::ingest::IngestHasher> {
                    let data = decompress_capped(&compressed, declared as usize)?;
                    if data.len() as u64 != declared {
                        return Err(io::Error::other(format!(
                            "chunk {digest} decompressed to {} bytes, manifest declares {declared}",
                            data.len()
                        )));
                    }
                    hasher.update(&data)?;
                    Ok(hasher)
                },
            )
            .await
            .map_err(io::Error::other)??;
        }
        let (got, outboard) = hasher.finish()?;
        if got != *blob {
            return Err(io::Error::other(format!(
                "manifest chunks assemble to {got}, expected blob {blob}"
            ))
            .into());
        }

        writer::store_outboard(
            &self.object_store,
            &self.base_path,
            *blob,
            outboard,
            self.immutable_cache,
            self.packed_chunks.as_ref(),
        )
        .await?;

        writer::store_manifest(
            &self.object_store,
            &self.base_path,
            *blob,
            &chunks,
            self.immutable_cache,
        )
        .await?;
        if let Some(packed) = &self.packed_chunks {
            packed.register_manifest(*blob);
            // Outside an explicit batch, `put_manifest` itself is the
            // publication boundary: make both referenced staged chunks and
            // manifest membership durable before returning. Mutation sessions
            // defer this shared flush until their atomic logical publication.
            if self.batch_depth.load(std::sync::atomic::Ordering::Acquire) == 0 {
                packed.flush().await?;
            }
        }
        Ok(())
    }
}

/// Whether an object exists at `path` (a `HEAD` with `NotFound` folded to
/// `false`).
pub(crate) async fn head_exists(
    object_store: &Arc<dyn ObjectStore>,
    path: &Path,
) -> io::Result<bool> {
    match object_store.head(path).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(e) => Err(io::Error::other(e)),
    }
}

/// Delete an object, folding "already gone" into success: a concurrent
/// collector may have won the race.
pub(crate) async fn delete_object(
    object_store: &Arc<dyn ObjectStore>,
    path: &Path,
) -> io::Result<()> {
    match object_store.delete(path).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(io::Error::other(e)),
    }
}

fn blob_path(base: &Path, digest: &BlobId) -> Path {
    sharded_path(base, "blobs", digest.as_digest())
}

fn chunk_path(base: &Path, digest: &ChunkId) -> Path {
    sharded_path(base, "chunks", digest.as_digest())
}

/// A manifest-elided blob is deliberately stored under the chunk capability
/// with the same digest bytes. This is the only blob-to-chunk conversion in
/// the backend.
fn single_chunk_id(blob: BlobId) -> ChunkId {
    ChunkId::new(blob.digest())
}

/// Prefix of a NAR availability witness: the storage objects it names, one
/// path per line.
const WITNESS_MAGIC: &[u8] = b"casita-nar-witness/packs-v1\n";

/// `<base>/<kind>/b3/<first-2-hex>/<full-hex>`, sharded on the first digest byte.
pub(crate) fn sharded_path(base: &Path, kind: &str, digest: &Digest) -> Path {
    let hex = digest.to_hex();
    kind_prefix(base, kind).join(&hex[..2]).join(hex.as_str())
}

impl ChunkedBlobStore {
    /// Stream the identities that actually have Bao outboards.
    ///
    /// Integrity repair inventories this namespace once instead of issuing
    /// one guaranteed-miss lookup for every small payload in repositories
    /// where outboards are uncommon.
    pub(crate) fn list_outboards(&self) -> BoxStream<'_, Result<BlobId, Error>> {
        Box::pin(async_stream::try_stream! {
            let mut seen = std::collections::BTreeSet::new();
            if let Some(packed) = &self.packed_chunks {
                for blob in packed.sidecar_blobs().await? {
                    seen.insert(blob);
                    yield blob;
                }
            }
            let mut loose = self.list_kind("bao");
            while let Some(digest) = loose.next().await {
                let blob = BlobId::new(digest?);
                if seen.insert(blob) { yield blob; }
            }
        })
    }

    fn list_kind(&self, kind: &str) -> futures::stream::BoxStream<'_, io::Result<Digest>> {
        let prefix = kind_prefix(&self.base_path, kind);
        Box::pin(self.object_store.list(Some(&prefix)).map(|res| {
            let meta = res.map_err(io::Error::other)?;
            digest_from_location(&meta.location)
        }))
    }
}

#[async_trait]
impl crate::blob::BlobGc for ChunkedBlobStore {
    async fn delete_blobs_pinned(
        &self,
        digests: &[BlobId],
        pins: Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
        before_prune: bool,
    ) -> Result<usize, Error> {
        if let Some(packed) = &self.packed_chunks {
            packed
                .retire_manifests_pinned(digests, pins, owned_claims, before_prune)
                .await?;
            Ok(digests.len())
        } else {
            let groups = digests
                .iter()
                .map(|digest| vec![self.blob_path(digest), self.outboard_path(digest)])
                .collect();
            Ok(super::pinned_store::delete_pinned_groups(
                self.object_store.clone(),
                pins,
                owned_claims,
                groups,
                |_| {},
            )
            .await?)
        }
    }

    async fn delete_chunks_pinned(
        &self,
        digests: &[ChunkId],
        pins: Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
    ) -> Result<usize, Error> {
        if self.packed_chunks.is_some() {
            self.delete_chunks(digests).await?;
            return Ok(digests.len());
        }
        let groups = digests
            .iter()
            .map(|digest| vec![self.chunk_path(digest)])
            .collect();
        let digests = digests.to_vec();
        let index = self.chunk_index.clone();
        Ok(super::pinned_store::delete_pinned_groups(
            self.object_store.clone(),
            pins,
            owned_claims,
            groups,
            move |selected| {
                for at in selected {
                    index.remove(&digests[*at]);
                }
            },
        )
        .await?)
    }

    async fn finish_deletions_pinned(
        &self,
        force_reclaim: bool,
        pins: Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
        before_prune: bool,
    ) -> Result<(), Error> {
        if let Some(packed) = &self.packed_chunks {
            packed
                .finish_deletions_pinned(force_reclaim, pins, owned_claims, before_prune)
                .await?;
        }
        Ok(())
    }

    async fn finish_collection_pinned(
        &self,
        force_reclaim: bool,
        pins: Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
    ) -> Result<(), Error> {
        if let Some(packed) = &self.packed_chunks {
            packed
                .finish_collection_pinned(force_reclaim, pins.clone(), owned_claims.clone())
                .await?;
        }
        if !force_reclaim {
            self.reclaim_pages(Some((pins, owned_claims))).await?;
        }
        Ok(())
    }

    async fn finish_collection(&self, force_reclaim: bool) -> Result<(), Error> {
        if let Some(packed) = &self.packed_chunks {
            packed.finish_collection(force_reclaim).await?;
        }
        if !force_reclaim {
            self.reclaim_pages(None).await?;
        }
        Ok(())
    }
    fn list_blobs(&self) -> futures::stream::BoxStream<'_, Result<BlobId, Error>> {
        if let Some(packed) = &self.packed_chunks {
            return Box::pin(
                packed
                    .list_manifests()
                    .map(|result| result.map_err(Error::from)),
            );
        }
        Box::pin(
            self.list_kind("blobs")
                .map(|r| r.map(BlobId::new).map_err(Error::from)),
        )
    }

    fn list_chunks(&self) -> futures::stream::BoxStream<'_, Result<ChunkId, Error>> {
        if let Some(packed) = &self.packed_chunks {
            return Box::pin(packed.list().map(|result| result.map_err(Error::from)));
        }
        Box::pin(
            self.list_kind("chunks")
                .map(|r| r.map(ChunkId::new).map_err(Error::from)),
        )
    }

    async fn physical_scan_order(&self, digest: &BlobId) -> Result<Option<Digest>, Error> {
        let Some(packed) = &self.packed_chunks else {
            return Ok(None);
        };
        // Most filesystem objects fit in one self-addressed chunk. Ordering a
        // bounded integrity-scan window by its pack turns cache-thrashing
        // random reads into one sequential visit per pack. Manifest-backed
        // blobs deliberately return no hint: resolving their first chunk just
        // to sort would add an object read before the verified read itself.
        if !packed.manifest_definitely_absent(digest) {
            return Ok(None);
        }
        Ok(packed
            .scan_order_pack(&single_chunk_id(*digest))
            .await?
            .map(|pack| pack.digest()))
    }

    async fn open_read_for_fsck(
        &self,
        digest: &BlobId,
        manifest_present: bool,
    ) -> Result<Option<Box<dyn BlobReader>>, Error> {
        if manifest_present {
            self.open_manifest_read(digest).await
        } else {
            self.open_bare_chunk(digest).await
        }
    }

    async fn chunks_for_gc(
        &self,
        digest: &BlobId,
        manifest_present: bool,
    ) -> Result<Option<Vec<ChunkMeta>>, Error> {
        if manifest_present {
            return Ok(self.manifest(digest).await?);
        }
        Ok(self
            .bare_chunk_meta(&single_chunk_id(*digest))
            .await?
            .map(|meta| vec![meta]))
    }

    async fn delete_blob(&self, digest: &BlobId) -> Result<(), Error> {
        // the manifest and the (best-effort) bao outboard are independent
        // objects; delete them concurrently.
        if let Some(packed) = &self.packed_chunks {
            packed.record_gc_manifest_delete();
            packed.record_gc_outboard_delete();
        }
        let manifest_path = self.blob_path(digest);
        let outboard_path = self.outboard_path(digest);
        if let Some(packed) = &self.packed_chunks
            && packed.uses_state_catalog()
        {
            packed.retire_manifest(*digest).await?;
            return Ok(());
        }
        let (manifest, outboard) = tokio::join!(
            self.object_store.delete(&manifest_path),
            delete_object(&self.object_store, &outboard_path),
        );
        manifest.map_err(io::Error::other)?;
        if let Some(packed) = &self.packed_chunks {
            packed.unregister_manifest(*digest);
        }
        Ok(outboard?)
    }

    async fn delete_chunk(&self, digest: &ChunkId) -> Result<(), Error> {
        self.delete_chunks(std::slice::from_ref(digest)).await
    }

    #[tracing::instrument(name = "blob.delete_chunks", level = "debug", skip_all, fields(chunks = digests.len()))]
    async fn delete_chunks(&self, digests: &[ChunkId]) -> Result<(), Error> {
        if let Some(packed) = &self.packed_chunks {
            packed.delete_many(digests).await?;
            for digest in digests {
                self.chunk_index.remove(digest);
            }
            return Ok(());
        }

        // Forget every key before physical deletion. Even if one delete then
        // fails, a later import cannot deduplicate against an entry whose
        // representation may already be gone.
        for digest in digests {
            self.chunk_index.remove(digest);
        }
        let paths = digests
            .iter()
            .map(|digest| self.chunk_path(digest))
            .collect::<Vec<_>>();
        // `AmazonS3::delete_stream` turns each 1,000 paths into one S3
        // DeleteObjects request. The local implementation still uses a
        // bounded window, so this is the efficient primitive for both
        // backends.
        let locations =
            futures::stream::iter(paths.into_iter().map(Ok::<_, object_store::Error>)).boxed();
        let mut deletes = self.object_store.delete_stream(locations);
        while let Some(result) = deletes.next().await {
            match result {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(io::Error::other(error).into()),
            }
        }
        Ok(())
    }

    #[tracing::instrument(
        name = "blob.finish_deletions",
        skip_all,
        fields(force_reclaim = false)
    )]
    async fn finish_deletions(&self) -> Result<(), Error> {
        if let Some(packed) = &self.packed_chunks {
            packed.finish_deletions(false).await?;
        }
        Ok(())
    }

    #[tracing::instrument(name = "blob.finish_deletions", skip_all, fields(force_reclaim = true))]
    async fn reclaim_deletions(&self) -> Result<(), Error> {
        if let Some(packed) = &self.packed_chunks {
            packed.finish_deletions(true).await?;
        }
        Ok(())
    }

    #[tracing::instrument(name = "blob.catalog.reclaim", skip_all)]
    async fn reclaim_metadata(&self) -> Result<(), Error> {
        self.reclaim_pages(None).await?;
        if let Some(packed) = &self.packed_chunks {
            packed.refresh().await?;
            packed.reclaim_catalog_objects(&[]).await?;
        }
        Ok(())
    }

    async fn reclaim_metadata_pinned(
        &self,
        pins: Arc<dyn crate::metadata::PinStore>,
        owned_claims: std::collections::BTreeSet<crate::metadata::PinToken>,
    ) -> Result<(), Error> {
        self.reclaim_pages(Some((pins.clone(), owned_claims.clone())))
            .await?;
        if let Some(packed) = &self.packed_chunks {
            packed.refresh().await?;
            packed
                .reclaim_catalog_metadata_pinned(pins, owned_claims)
                .await?;
        }
        Ok(())
    }

    async fn metadata_reclaim_due(&self) -> Result<bool, Error> {
        match &self.packed_chunks {
            Some(packed) => Ok(packed.catalog_reclaim_due().await?),
            None => Ok(false),
        }
    }
}

/// The `<base>/<kind>/b3` listing prefix.
pub(crate) fn kind_prefix(base: &Path, kind: &str) -> Path {
    base.clone().join(kind).join("b3")
}

/// Parse a stored object's digest from its path (the filename is its hex digest).
pub(crate) fn digest_from_location(location: &Path) -> io::Result<Digest> {
    let name = location
        .filename()
        .ok_or_else(|| io::Error::other("stored object has no filename"))?;
    Digest::from_hex(name).map_err(io::Error::other)
}

/// A self-addressed object is stored under the BLAKE3 digest of its exact
/// bytes. That is what makes rewriting it with `PutMode::Overwrite`
/// idempotent (F-11, D4).
pub(crate) fn validate_content_address(
    location: &Path,
    actual: &Digest,
) -> Result<(), crate::invariant::Violation> {
    const POINT: &str = "blob.content_address";
    let named = digest_from_location(location).map_err(|error| {
        crate::invariant::Violation::new(POINT, format!("{location} names no digest: {error}"))
    })?;
    crate::invariant::ensure(POINT, named == *actual, || {
        format!("{location} would hold bytes hashing to {actual}")
    })
}

/// Check a self-addressed write before it becomes durable. Debug builds only:
/// hashing the payload again doubles the write's hashing cost.
pub(crate) fn check_content_address(location: &Path, bytes: &[u8]) -> io::Result<()> {
    if crate::invariant::DEBUG {
        crate::invariant::check(|| {
            validate_content_address(location, &Digest::from(blake3::hash(bytes)))
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
