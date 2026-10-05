//! Streaming FastCDC chunking, deduplicated upload, and blob-writer state.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures::future::{FusedFuture, FutureExt};
use futures::stream::{FuturesUnordered, StreamExt};
use object_store::{ObjectStore, path::Path};

use super::hash_batch::{HashBatch, Hashed};
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
use std::collections::{BTreeMap, BTreeSet};

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

/// Upload windows of completed metadata a writer buffers behind its earliest
/// pending chunk before it stops admitting new chunks.
const REORDER_WINDOWS: usize = 16;

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

    let mut manifest = super::pages::ChunkManifest::new(super::pages::Pages {
        objects: object_store.clone(),
        base: base_path.clone(),
        immutable: immutable_cache,
    });
    {
        use tokio::io::AsyncReadExt;
        let mut source = std::io::Cursor::new(head).chain(&mut hashing);
        let mut chunker = fastcdc::v2020::AsyncStreamCDC::new(&mut source, min, avg, max);
        let stream = chunker.as_stream();
        futures::pin_mut!(stream);
        let mut uploads = FuturesUnordered::new();
        let mut hashes = HashBatch::default();
        let mut reordered = BTreeMap::new();
        let mut next_offset = 0;
        // Uploads keep flowing behind a straggler until its peers have each
        // completed REORDER_WINDOWS more uploads. An entry is only chunk
        // metadata, so even a wide window costs kilobytes.
        let reorder_limit =
            super::pages::FANOUT.max(concurrency.get().saturating_mul(REORDER_WINDOWS));
        // A waiting reservation keeps its place in the budget's FIFO queue
        // while this writer drains completions, rather than requeueing behind
        // other writers each time one of its own uploads finishes.
        let admission = futures::future::OptionFuture::from(None);
        futures::pin_mut!(admission);

        loop {
            // Bound completed metadata behind a straggler. In-flight uploads
            // may contribute at most another concurrency-window of entries.
            if reordered.len() >= reorder_limit {
                // Budget is useless until the straggler completes. Leave the
                // queue rather than hold units other writers could use.
                admission.set(None.into());
                hashes.flush();
                let completed = uploads
                    .next()
                    .await
                    .ok_or_else(|| io::Error::other("missing earlier chunk upload"))??;
                append_completed(
                    &mut manifest,
                    &mut reordered,
                    &mut next_offset,
                    completed,
                    &mut uploads,
                )
                .await?;
                continue;
            }
            // Reserve the maximum possible output before asking FastCDC to
            // allocate the next chunk. The permit follows that chunk through
            // hashing, compression, and upload. If the shared budget is full,
            // keep polling this writer's queued uploads so their permits can be
            // released; merely waiting for admission here would deadlock a
            // budget smaller than the per-writer concurrency window.
            let permit = if admission.is_terminated()
                && let Some(permit) = memory_budget.try_reserve(max)
            {
                permit
            } else {
                hashes.flush();
                if admission.is_terminated() {
                    admission.set(Some(memory_budget.reserve(max).fuse()).into());
                }
                let permit = if uploads.is_empty() {
                    (&mut admission).await
                } else {
                    tokio::select! {
                        permit = &mut admission => permit,
                        completed = uploads.next() => {
                            append_completed(
                                &mut manifest,
                                &mut reordered,
                                &mut next_offset,
                                completed.expect("the upload queue is nonempty")?,
                                &mut uploads,
                            )
                            .await?;
                            continue;
                        }
                    }
                };
                permit.expect("admission was pending")
            };
            let chunk = {
                let next = stream.next();
                futures::pin_mut!(next);
                match futures::poll!(next.as_mut()) {
                    Poll::Ready(chunk) => chunk,
                    Poll::Pending => {
                        // A producer may pause before the group is full. Submit
                        // its partial job and keep driving storage while waiting.
                        hashes.flush();
                        loop {
                            tokio::select! {
                                chunk = &mut next => break chunk,
                                completed = uploads.next(), if !uploads.is_empty() => {
                                    append_completed(
                                        &mut manifest,
                                        &mut reordered,
                                        &mut next_offset,
                                        completed.expect("nonempty uploads")?,
                                        &mut uploads,
                                    )
                                    .await?;
                                }
                            }
                        }
                    }
                }
            };
            let Some(chunk) = chunk else {
                drop(permit);
                break;
            };
            let chunk = chunk.map_err(io::Error::other)?;
            // FastCDC can observe EOF while filling its buffer before emitting
            // the first chunk. Reuse the whole-blob hash only when that chunk
            // covers every observed byte. No lookahead or extra buffering.
            let completed = completion.single_blob(chunk.offset, chunk.data.len());
            let offset = chunk.offset;
            let hashed = match completed {
                Some((blob, _)) => {
                    futures::future::Either::Left(futures::future::ready(Ok(Hashed {
                        data: chunk.data,
                        digest: single_chunk_id(blob),
                        guard: permit,
                    })))
                }
                None => futures::future::Either::Right(hashes.push(chunk.data, permit)),
            };
            let uploader = &uploader;
            let pins = &pins;
            let base_path = &base_path;
            uploads.push(async move {
                let hashed = hashed.await.map_err(io::Error::other)?;
                if let Some((blob, outboard_len)) = completed {
                    // As for a small file: admit the chunk and its loose path
                    // with the blob, so the upload needs no protection of its own.
                    let mut resources = blob_resources(base_path, blob, outboard_len);
                    resources.extend(uploader.protection_resources(hashed.digest));
                    pins.protect(resources).await?;
                }
                let meta = uploader
                    .upload_prehashed(hashed.data, hashed.digest, hashed.guard)
                    .await?;
                Ok::<_, io::Error>((offset, meta))
            });

            if uploads.len() == concurrency.get() {
                hashes.flush();
                let completed = uploads
                    .next()
                    .await
                    .expect("the upload queue is nonempty")?;
                append_completed(
                    &mut manifest,
                    &mut reordered,
                    &mut next_offset,
                    completed,
                    &mut uploads,
                )
                .await?;
            }
        }

        hashes.flush();
        while let Some(completed) = uploads.next().await {
            append_completed(
                &mut manifest,
                &mut reordered,
                &mut next_offset,
                completed?,
                &mut uploads,
            )
            .await?;
        }
        if !reordered.is_empty() {
            return Err(io::Error::other("incomplete chunk manifest order"));
        }
    }

    // Every hashed byte must belong to exactly one manifest entry; the blob
    // identity would otherwise name content the manifest cannot reproduce.
    if manifest.size() != hashing.size() {
        return Err(io::Error::other(
            "chunk manifest does not cover the hashed payload",
        ));
    }

    let (blob_digest, outboard) = hashing.finish()?;
    pins.protect(blob_resources(&base_path, blob_digest, outboard.len))
        .await?;
    let blob_size = manifest.size();

    // A single chunk is already stored under the blob digest, so its manifest
    // is redundant. The empty blob retains its empty manifest.
    let elide = manifest.single_chunk() == Some(single_chunk_id(blob_digest));
    if !elide {
        let bytes = manifest.finish().await?;
        put_object(
            &object_store,
            &blob_path(&base_path, &blob_digest),
            bytes,
            immutable_cache,
        )
        .await
        .map_err(io::Error::other)?;
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

// Restore source order without retaining every completed chunk. Pages are
// emitted as soon as their contiguous prefix is available.
async fn append_completed<F>(
    manifest: &mut super::pages::ChunkManifest,
    reordered: &mut BTreeMap<u64, ChunkMeta>,
    next_offset: &mut u64,
    (offset, chunk): (u64, ChunkMeta),
    uploads: &mut FuturesUnordered<F>,
) -> io::Result<()>
where
    F: Future<Output = io::Result<(u64, ChunkMeta)>>,
{
    if reordered.insert(offset, chunk).is_some() {
        return Err(io::Error::other("duplicate chunk offset"));
    }
    while let Some(chunk) = reordered.remove(next_offset) {
        let end = next_offset
            .checked_add(chunk.size)
            .ok_or_else(|| io::Error::other("chunk offset overflow"))?;
        let saving = manifest.push(chunk);
        futures::pin_mut!(saving);
        loop {
            // An upload may own the shared pin-protection gate across ledger
            // I/O. Page writes need that gate too, so continue polling uploads
            // while saving a leaf. No new source chunks are admitted here;
            // extra completions fit within the existing concurrency window.
            tokio::select! {
                result = &mut saving => { result?; break; }
                completed = uploads.next(), if !uploads.is_empty() => {
                    let (offset, chunk) = completed.expect("nonempty uploads")?;
                    if reordered.insert(offset, chunk).is_some() {
                        return Err(io::Error::other("duplicate chunk offset"));
                    }
                }
            }
        }
        *next_offset = end;
    }
    Ok(())
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

#[cfg(test)]
mod pin_tests;
