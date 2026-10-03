//! Streaming FastCDC chunking, deduplicated upload, and blob-writer state.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures::stream::{FuturesOrdered, StreamExt, TryStreamExt};
use object_store::{ObjectStore, path::Path};

use super::manifest::encode_manifest;
use super::upload::ChunkUploader;
use super::{blob_path, put_object, single_chunk_id};
use crate::blob::chunk_index::ChunkIndex;
use crate::blob::hashing_reader::{HashCompletion, HashingReader};
use crate::blob::pack::PackedChunks;
use crate::blob::{BlobWriter, ChunkMeta};
use crate::byte_budget::ByteBudget;
use crate::digest::BlobId;
use crate::error::Error;
use crate::metadata::{PinResource, WritePins};
use std::collections::BTreeSet;

/// Open a writer whose duplex reader feeds the background upload pipeline.
#[allow(clippy::too_many_arguments)]
pub(super) fn open(
    object_store: Arc<dyn ObjectStore>,
    base_path: Path,
    chunk_index: ChunkIndex,
    packed_chunks: Option<Arc<PackedChunks>>,
    batch_depth: Arc<AtomicUsize>,
    avg: u32,
    immutable_cache: bool,
    memory_budget: ByteBudget,
    concurrency: std::num::NonZeroUsize,
    pins: WritePins,
) -> Box<dyn BlobWriter> {
    let buf_size = (avg as usize).saturating_mul(8).max(64 * 1024);
    let (writer, reader) = tokio::io::duplex(buf_size);
    let fut = Box::pin(chunk_and_upload(
        reader,
        object_store,
        base_path,
        chunk_index,
        packed_chunks.clone(),
        avg,
        immutable_cache,
        memory_budget,
        concurrency,
        pins.clone(),
    ));

    Box::new(ChunkedBlobWriter {
        writer: Some(writer),
        fut: Some(fut),
        done: None,
        failure: None,
        packed_chunks,
        batch_depth,
        finalized: false,
        _pins: pins,
    })
}

/// Read `reader` to EOF, chunking it with FastCDC (min/max sized at half and
/// double the average), deduplicating and uploading each chunk, and writing the
/// blob manifest. Returns the whole-blob digest and size.
#[allow(clippy::too_many_arguments)]
async fn chunk_and_upload(
    reader: tokio::io::DuplexStream,
    object_store: Arc<dyn ObjectStore>,
    base_path: Path,
    chunk_index: ChunkIndex,
    packed_chunks: Option<Arc<PackedChunks>>,
    avg: u32,
    immutable_cache: bool,
    memory_budget: ByteBudget,
    concurrency: std::num::NonZeroUsize,
    pins: WritePins,
) -> io::Result<(BlobId, u64)> {
    // Clamp to FastCDC's supported ranges and round down to even sizes,
    // as required by v5's two-byte scan. Keep the public configuration u32.
    use fastcdc::v2020::{
        AVERAGE_MAX, AVERAGE_MIN, MAXIMUM_MAX, MAXIMUM_MIN, MINIMUM_MAX, MINIMUM_MIN,
    };
    let avg = (avg as usize).clamp(AVERAGE_MIN, AVERAGE_MAX) & !1;
    let min = (avg / 2).clamp(MINIMUM_MIN, MINIMUM_MAX) & !1;
    let max = avg.saturating_mul(2).clamp(MAXIMUM_MIN, MAXIMUM_MAX);
    let completion = HashCompletion::default();
    let mut hashing = HashingReader::new(reader, &completion);
    let uploader = ChunkUploader {
        object_store: &object_store,
        base_path: &base_path,
        chunk_index: &chunk_index,
        packed_chunks: packed_chunks.as_ref(),
        immutable_cache,
        pins: &pins,
    };

    // FastCDC never cuts below its minimum, so a stream that ends inside the
    // first `min` bytes is exactly one chunk and needs no chunker at all.
    // Taking that case here keeps the chunker's `max`-sized input buffer (half
    // a megabyte at the default average) off the path that every small file
    // walks, and the resulting chunk is byte-identical to the one the chunker
    // would have produced.
    let mut head = Vec::new();
    {
        use tokio::io::AsyncReadExt;
        (&mut hashing)
            .take(min as u64)
            .read_to_end(&mut head)
            .await?;
    }

    if head.len() < min {
        // The empty blob chunks to nothing and keeps its empty manifest, the
        // one single-chunk shape the elision below does not apply to.
        if head.is_empty() {
            let (blob_digest, _) = hashing.finish()?;
            pins.protect(BTreeSet::from([PinResource::Blob(blob_digest)]))
                .await?;
            put_object(
                &object_store,
                &blob_path(&base_path, &blob_digest),
                encode_manifest(&[]),
                immutable_cache,
            )
            .await
            .map_err(io::Error::other)?;
            if let Some(packed) = &packed_chunks {
                packed.register_manifest(blob_digest);
            }
            return Ok((blob_digest, 0));
        }
        let size = head.len() as u64;
        let permit = memory_budget.reserve(head.len()).await;
        let (blob_digest, outboard) = hashing.finish()?;
        let mut resources = blob_resources(&base_path, blob_digest, outboard.len);
        // The chunk and its loose path, when present, are known alongside the
        // blob and Bao path before this upload. Admit the full set together.
        resources.extend(uploader.protection_resources(single_chunk_id(blob_digest)));
        pins.protect(resources).await?;
        let meta = uploader
            .upload_prehashed(head, single_chunk_id(blob_digest), permit)
            .await?;
        // A lone chunk is stored under the blob digest already, so the manifest
        // it would carry is redundant, exactly as in the chunked path below.
        debug_assert_eq!(meta.digest, single_chunk_id(blob_digest));
        debug_assert_eq!(meta.size, size);
        store_outboard(
            &object_store,
            &base_path,
            blob_digest,
            outboard,
            immutable_cache,
            packed_chunks.as_ref(),
        )
        .await?;
        return Ok((blob_digest, size));
    }

    let chunks: Vec<ChunkMeta> = {
        use tokio::io::AsyncReadExt;
        let mut source = std::io::Cursor::new(head).chain(&mut hashing);
        let mut chunker = fastcdc::v2020::AsyncStreamCDC::new(&mut source, min, avg, max);
        let stream = chunker.as_stream();
        futures::pin_mut!(stream);
        let mut uploads = FuturesOrdered::new();
        let mut chunks = Vec::new();

        loop {
            // Reserve the maximum possible output before asking FastCDC to
            // allocate the next chunk. The permit follows that chunk through
            // hashing, compression, and upload. If the shared budget is full,
            // keep polling this writer's queued uploads so their permits can be
            // released; merely waiting for admission here would deadlock a
            // budget smaller than the per-writer concurrency window.
            let permit = if uploads.is_empty() {
                memory_budget.reserve(max).await
            } else {
                tokio::select! {
                    permit = memory_budget.reserve(max) => permit,
                    completed = uploads.next() => {
                        chunks.push(completed.expect("the upload queue is nonempty")?);
                        continue;
                    }
                }
            };
            let Some(chunk) = stream.next().await else {
                drop(permit);
                break;
            };
            let chunk = chunk.map_err(io::Error::other)?;
            // FastCDC can observe EOF while filling its buffer before emitting
            // the first chunk. Reuse the whole-blob hash only when that chunk
            // covers every observed byte. No lookahead or extra buffering.
            let completed = completion.single_blob(chunk.offset, chunk.data.len());
            let uploader = &uploader;
            let pins = &pins;
            let base_path = &base_path;
            uploads.push_back(async move {
                match completed {
                    Some((blob, outboard_len)) => {
                        let chunk_id = single_chunk_id(blob);
                        let mut resources = blob_resources(base_path, blob, outboard_len);
                        // Admit the loose path with the known blob identity.
                        resources.extend(uploader.protection_resources(chunk_id));
                        pins.protect(resources).await?;
                        uploader
                            .upload_prehashed(chunk.data, chunk_id, permit)
                            .await
                    }
                    None => uploader.upload(chunk.data, permit).await,
                }
            });

            if uploads.len() == concurrency.get() {
                chunks.push(
                    uploads
                        .next()
                        .await
                        .expect("the upload queue is nonempty")?,
                );
            }
        }

        chunks.extend(uploads.try_collect::<Vec<_>>().await?);
        chunks
    };

    let (blob_digest, outboard) = hashing.finish()?;
    pins.protect(blob_resources(&base_path, blob_digest, outboard.len))
        .await?;
    let blob_size = chunks.iter().map(|c| c.size).sum();

    // A single chunk is already stored under the blob digest, so its manifest
    // is redundant. The empty blob retains its empty manifest.
    let elide = chunks.len() == 1 && chunks[0].digest == single_chunk_id(blob_digest);
    if !elide {
        store_manifest(
            &object_store,
            &base_path,
            blob_digest,
            &chunks,
            immutable_cache,
        )
        .await?;
        if let Some(packed) = &packed_chunks {
            packed.register_manifest(blob_digest);
        }
    }

    store_outboard(
        &object_store,
        &base_path,
        blob_digest,
        outboard,
        immutable_cache,
        packed_chunks.as_ref(),
    )
    .await?;
    Ok((blob_digest, blob_size))
}

// The Bao root path is known with the blob identity. Admit it before sidecar
// writes so the pinned object store can reuse the same durable protection.
// Paged outboard children still acquire their own pins when their hashes exist.
fn blob_resources(base: &Path, digest: BlobId, outboard_len: u64) -> BTreeSet<PinResource> {
    let mut resources = BTreeSet::from([PinResource::Blob(digest)]);
    if outboard_len != 0 {
        resources.insert(PinResource::StorageObject(
            super::sharded_path(base, "bao", digest.as_digest()).to_string(),
        ));
    }
    resources
}

pub(super) async fn store_manifest(
    objects: &Arc<dyn ObjectStore>,
    base: &Path,
    digest: BlobId,
    chunks: &[ChunkMeta],
    immutable: bool,
) -> io::Result<()> {
    let bytes = if chunks.len() > super::pages::FANOUT {
        super::pages::Pages {
            objects: objects.clone(),
            base: base.clone(),
            immutable,
        }
        .build_chunks(chunks)
        .await?
        .encode()
    } else {
        encode_manifest(chunks)
    };
    put_object(objects, &blob_path(base, &digest), bytes, immutable)
        .await
        .map_err(io::Error::other)
}

pub(super) async fn store_outboard(
    objects: &Arc<dyn ObjectStore>,
    base: &Path,
    digest: BlobId,
    mut outboard: crate::verified::ingest::OutboardData,
    immutable: bool,
    packed: Option<&Arc<PackedChunks>>,
) -> io::Result<()> {
    use std::io::Read;
    if outboard.len == 0 {
        return Ok(());
    }
    let bytes = if outboard.len > super::pages::LEAF_BYTES as u64 {
        super::pages::Pages {
            objects: objects.clone(),
            base: base.clone(),
            immutable,
        }
        .build_bytes(&mut outboard.file)
        .await?
        .encode()
    } else {
        let mut bytes = Vec::new();
        outboard.file.read_to_end(&mut bytes)?;
        bytes
    };
    if let Some(packed) = packed.filter(|packed| packed.uses_state_catalog()) {
        return packed.put_sidecar(digest, bytes.into()).await;
    }
    put_object(
        objects,
        &super::sharded_path(base, "bao", digest.as_digest()),
        bytes,
        immutable,
    )
    .await
    .map_err(io::Error::other)
}

type UploadFut = Pin<Box<dyn Future<Output = io::Result<(BlobId, u64)>> + Send>>;

/// Writer half: bytes written here are streamed (via a duplex pipe) into the
/// background [`chunk_and_upload`] future, which is driven forward whenever the
/// writer is polled so the pipe never deadlocks.
struct ChunkedBlobWriter {
    _pins: WritePins,
    writer: Option<tokio::io::DuplexStream>,
    fut: Option<UploadFut>,
    done: Option<(BlobId, u64)>,
    /// Set if the background upload failed; kept so a later `close` (or write)
    /// surfaces the real error instead of a misleading "already closed".
    failure: Option<String>,
    packed_chunks: Option<Arc<PackedChunks>>,
    batch_depth: Arc<AtomicUsize>,
    finalized: bool,
}

impl ChunkedBlobWriter {
    async fn finalize_storage(&mut self) -> Result<(), Error> {
        if self.finalized {
            return Ok(());
        }
        if let Some(packed) = &self.packed_chunks
            && self.batch_depth.load(Ordering::Acquire) == 0
        {
            packed.flush().await?;
        }
        self.finalized = true;
        Ok(())
    }

    /// The remembered upload failure, re-raised so every entry point reports the
    /// original cause rather than a misleading "already closed".
    fn failed(&self) -> Option<io::Error> {
        self.failure
            .as_ref()
            .map(|msg| io::Error::other(msg.clone()))
    }

    /// Poll the background upload so the duplex buffer drains. Returns an error
    /// if the upload finished early with a failure, remembering it so a later
    /// call reports the same cause.
    fn drive_upload(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if let Some(fut) = self.fut.as_mut()
            && let Poll::Ready(res) = fut.as_mut().poll(cx)
        {
            self.fut = None;
            match res {
                Ok(done) => self.done = Some(done),
                Err(e) => {
                    self.failure = Some(e.to_string());
                    return Err(e);
                }
            }
        }
        Ok(())
    }
}

impl tokio::io::AsyncWrite for ChunkedBlobWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Some(e) = this.failed() {
            return Poll::Ready(Err(e));
        }
        if let Err(e) = this.drive_upload(cx) {
            return Poll::Ready(Err(e));
        }
        match this.writer.as_mut() {
            Some(w) => Pin::new(w).poll_write(cx, buf),
            // the writer is gone only after close took it: accepting bytes here
            // would silently discard them (they are not in the finished blob).
            None => Poll::Ready(Err(io::Error::other("write after blob writer closed"))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Some(e) = this.failed() {
            return Poll::Ready(Err(e));
        }
        if let Err(e) = this.drive_upload(cx) {
            return Poll::Ready(Err(e));
        }
        match this.writer.as_mut() {
            Some(w) => Pin::new(w).poll_flush(cx),
            None => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[async_trait]
impl BlobWriter for ChunkedBlobWriter {
    async fn close(&mut self) -> Result<(BlobId, u64), Error> {
        if let Some(done) = self.done {
            self.finalize_storage().await?;
            return Ok(done);
        }
        if let Some(e) = self.failed() {
            return Err(e.into());
        }
        // signal EOF to the reader side, so chunking can finish.
        if let Some(mut writer) = self.writer.take() {
            use tokio::io::AsyncWriteExt;
            writer.shutdown().await?;
        }
        let fut = self
            .fut
            .take()
            .ok_or_else(|| io::Error::other("blob writer already closed"))?;
        match fut.await {
            Ok(done) => {
                self.done = Some(done);
                self.finalize_storage().await?;
                Ok(done)
            }
            Err(e) => {
                self.failure = Some(e.to_string());
                Err(e.into())
            }
        }
    }
}
