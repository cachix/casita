//! Test-only real-I/O prototype. It deliberately does not replace production
//! pin admission, seeking, compaction, or cross-reader request sharing.
use super::*;
use crate::blob::ChunkMeta;
use futures::FutureExt;
use futures::stream::BoxStream;

const BUFFER_BYTES: usize = 64 * 1024 * 1024;

#[derive(Default)]
pub(super) struct Meter {
    live: AtomicU64,
    peak: AtomicU64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::{BlobStore, ChunkedBlobStore};
    use object_store::memory::InMemory;
    use object_store::throttle::{ThrottleConfig, ThrottledStore};
    use std::time::Duration;

    async fn fixture() -> (
        Arc<ThrottledStore<InMemory>>,
        Arc<PlannedReader>,
        Vec<ChunkMeta>,
        Vec<u8>,
        BlobId,
    ) {
        let objects = Arc::new(ThrottledStore::new(
            InMemory::new(),
            ThrottleConfig::default(),
        ));
        let store = ChunkedBlobStore::packed_with_options(
            objects.clone(),
            Path::default(),
            64 * 1024,
            crate::PackOptions {
                target_size: 256 * 1024,
                cache_capacity: 0,
            },
        )
        .await
        .unwrap();
        let mut data = vec![0; 2 * 1024 * 1024];
        blake3::Hasher::new()
            .update(b"planned-stream-test")
            .finalize_xof()
            .fill(&mut data);
        let id = store.put_slice(&data).await.unwrap();
        store.flush().await.unwrap();
        let chunks = store.chunks(&id).await.unwrap().unwrap();
        let reader = PlannedReader::new(store.benchmark_packed(), 4 * 1024 * 1024, 1024 * 1024);
        reader.packed.reset_read_stats();
        (objects, reader, chunks, data, id)
    }

    async fn collect(mut stream: BoxStream<'static, io::Result<Bytes>>) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        while let Some(bytes) = stream.next().await {
            out.extend_from_slice(&bytes?);
        }
        Ok(out)
    }

    async fn wait_drained(reader: &PlannedReader) {
        let permit =
            tokio::time::timeout(Duration::from_secs(1), reader.buffers.reserve(BUFFER_BYTES))
                .await
                .unwrap();
        drop(permit);
        let requests = tokio::time::timeout(
            Duration::from_secs(1),
            reader.requests.clone().acquire_many_owned(4),
        )
        .await
        .unwrap()
        .unwrap();
        drop(requests);
        tokio::time::timeout(Duration::from_secs(1), async {
            while reader.meter.live.load(Ordering::Relaxed) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn lookahead_downloads_while_consumer_is_idle() {
        use tokio::io::AsyncReadExt;
        let (_, mut reader, chunks, data, id) = fixture().await;
        // One chunk per window makes progress beyond reader readahead visible.
        Arc::get_mut(&mut reader).unwrap().window = 1;
        // Demand the second chunk to start the pump. A first chunk that
        // completes immediately need not poll the reader's second future.
        let consumed = chunks[0].size as usize + 1;
        let mut input = reader.reader_mode(chunks, Some(id), true).await.unwrap();
        let mut prefix = vec![0; consumed];
        input.read_exact(&mut prefix).await.unwrap();
        assert_eq!(prefix, data[..consumed]);
        tokio::time::timeout(Duration::from_secs(1), async {
            while reader.packed.read_stats().chunk_range_requests < 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(input);
        wait_drained(&reader).await;
        reader.meter.reset();
    }

    #[tokio::test]
    async fn lookahead_cancels_a_blocked_background_window() {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        let (objects, reader, chunks, data, id) = fixture().await;
        reader
            .reader_mode(vec![chunks[0].clone()], None, true)
            .await
            .unwrap()
            .read_exact(&mut [0])
            .await
            .unwrap();
        objects.config_mut(|c| c.wait_get_per_call = Duration::from_secs(60));
        let mut input = reader
            .reader_mode(chunks.clone(), Some(id), true)
            .await
            .unwrap();
        let mut first = vec![0; chunks[0].size as usize];
        input.read_exact(&mut first).await.unwrap();
        assert_eq!(first, data[..first.len()]);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), input.read_exact(&mut [0]))
                .await
                .is_err()
        );
        // Seeking aborts the old pump when the replacement demand is fetched.
        input.seek(io::SeekFrom::Start(0)).await.unwrap();
        let mut byte = [0];
        tokio::time::timeout(Duration::from_secs(1), input.read_exact(&mut byte))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(byte[0], data[0]);
        drop(input);
        wait_drained(&reader).await;
        reader.meter.reset();
        objects.config_mut(|c| c.wait_get_per_call = Duration::ZERO);
    }

    #[tokio::test]
    async fn pipeline_keeps_eof_verification_seeks_and_warm_cache() {
        for lookahead in [false, true] {
            use tokio::io::{AsyncReadExt, AsyncSeekExt};
            let (_, reader, chunks, data, id) = fixture().await;
            let mut actual = Vec::new();
            reader
                .reader_mode(chunks.clone(), Some(id), lookahead)
                .await
                .unwrap()
                .read_to_end(&mut actual)
                .await
                .unwrap();
            assert_eq!(actual, data);
            wait_drained(&reader).await;
            reader.meter.reset();
            reader.packed.reset_read_stats();
            let mut input = reader
                .reader_mode(chunks.clone(), Some(id), lookahead)
                .await
                .unwrap();
            for offset in [1000000, 17, 1900000, 0] {
                input.seek(io::SeekFrom::Start(offset)).await.unwrap();
                let mut got = [0; 8192];
                input.read_exact(&mut got).await.unwrap();
                assert_eq!(got, data[offset as usize..offset as usize + got.len()]);
            }
            drop(input);
            assert_eq!(reader.packed.read_stats().chunk_range_requests, 0);
            wait_drained(&reader).await;
            reader.meter.reset();
            let wrong = BlobId::new(blake3::hash(b"wrong whole blob").into());
            assert!(
                reader
                    .reader_mode(chunks, Some(wrong), lookahead)
                    .await
                    .unwrap()
                    .read_to_end(&mut Vec::new())
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn pipeline_uses_frozen_locations_after_catalog_changes() {
        for lookahead in [false, true] {
            use tokio::io::AsyncReadExt;
            let (_, reader, chunks, data, id) = fixture().await;
            let mut input = reader
                .reader_mode(chunks.clone(), Some(id), lookahead)
                .await
                .unwrap();
            {
                let mut index = reader.packed.index.write().unwrap();
                for chunk in &chunks {
                    let mut location = index.chunks.get(&chunk.digest).unwrap();
                    location.offset = u64::MAX / 2;
                    index.chunks.overlay.insert(chunk.digest, location);
                }
            }
            let mut actual = Vec::new();
            input.read_to_end(&mut actual).await.unwrap();
            assert_eq!(actual, data);
        }
    }

    #[tokio::test]
    async fn pipeline_short_read_and_cancel_release_fetch_budget() {
        for lookahead in [false, true] {
            use tokio::io::AsyncReadExt;
            let (objects, reader, chunks, data, _) = fixture().await;
            let mut short = reader
                .reader_mode(vec![chunks[0].clone()], None, lookahead)
                .await
                .unwrap();
            let mut byte = [0];
            short.read_exact(&mut byte).await.unwrap();
            assert_eq!(byte[0], data[0]);
            drop(short);
            assert_eq!(reader.packed.read_stats().chunk_range_requests, 1);
            wait_drained(&reader).await;
            reader.meter.reset();
            objects.config_mut(|c| c.wait_get_per_call = Duration::from_secs(60));
            let mut blocked = reader
                .reader_mode(chunks[1..].to_vec(), None, lookahead)
                .await
                .unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(20), blocked.read_exact(&mut byte))
                    .await
                    .is_err()
            );
            drop(blocked);
            let permit =
                tokio::time::timeout(Duration::from_secs(1), reader.buffers.reserve(BUFFER_BYTES))
                    .await
                    .unwrap();
            drop(permit);
            wait_drained(&reader).await;
            reader.meter.reset();
            objects.config_mut(|c| c.wait_get_per_call = Duration::ZERO);
        }
    }

    #[tokio::test]
    async fn pipeline_overlapping_readers_and_corruption() {
        for lookahead in [false, true] {
            use tokio::io::AsyncReadExt;
            let (objects, mut reader, chunks, data, id) = fixture().await;
            let inner = Arc::get_mut(&mut reader).unwrap();
            inner.window = 64 * 1024;
            inner.buffers = crate::byte_budget::ByteBudget::new(256 * 1024);
            let read = || async {
                let mut actual = Vec::new();
                reader
                    .reader_mode(chunks.clone(), Some(id), lookahead)
                    .await
                    .unwrap()
                    .read_to_end(&mut actual)
                    .await
                    .unwrap();
                assert_eq!(actual, data);
            };
            tokio::time::timeout(Duration::from_secs(10), async {
                tokio::join!(read(), read());
            })
            .await
            .unwrap();
            wait_drained(&reader).await;
            reader.meter.reset();
            // Use a fresh cache so corruption cannot be hidden by verified entries.
            let fresh = PlannedReader::new(reader.packed.clone(), 0, 1024 * 1024);
            let bad = fresh
                .packed
                .freeze_read(&chunks[5].digest)
                .await
                .unwrap()
                .unwrap();
            let path = pack_path(&Path::default(), &bad.0.pack);
            let mut body = objects
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_vec();
            body[bad.0.offset as usize..bad.0.offset as usize + 4].fill(0);
            objects.put(&path, Bytes::from(body).into()).await.unwrap();
            let mut actual = Vec::new();
            assert!(
                fresh
                    .reader_mode(chunks.clone(), Some(id), lookahead)
                    .await
                    .unwrap()
                    .read_to_end(&mut actual)
                    .await
                    .is_err()
            );
            let prefix = chunks[..5].iter().map(|c| c.size as usize).sum::<usize>();
            assert_eq!(actual, data[..prefix]);
            wait_drained(&fresh).await;
            fresh.meter.reset();
        }
    }

    #[tokio::test]
    async fn full_stream_verifies_and_warm_cache_uses_no_gets() {
        let (_, reader, chunks, data, id) = fixture().await;
        assert_eq!(
            collect(reader.stream(chunks.clone(), Some(id)))
                .await
                .unwrap(),
            data
        );
        assert!(reader.meter.peak() <= 5 * 1024 * 1024 / 4);
        reader.meter.reset();
        reader.packed.reset_read_stats();
        assert_eq!(
            collect(reader.stream(chunks, Some(id))).await.unwrap(),
            data
        );
        assert_eq!(reader.packed.read_stats().chunk_range_requests, 0);
        assert_eq!(reader.meter.peak(), 0);
    }

    #[tokio::test]
    async fn first_chunk_and_explicit_short_range_do_not_prefetch() {
        let (_, reader, chunks, data, id) = fixture().await;
        let first = chunks[0].clone();
        let mut stream = reader.stream(chunks, Some(id));
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            data[..first.size as usize]
        );
        assert_eq!(reader.packed.read_stats().chunk_range_requests, 1);
        drop(stream);
        reader.meter.reset();
        reader.packed.reset_read_stats();
        assert_eq!(
            collect(reader.stream(vec![first.clone()], None))
                .await
                .unwrap(),
            data[..first.size as usize]
        );
        assert_eq!(reader.packed.read_stats().chunk_range_requests, 0);
    }

    #[tokio::test]
    async fn cancel_blocked_range_releases_window_budget() {
        let (objects, reader, chunks, _, id) = fixture().await;
        objects.config_mut(|c| c.wait_get_per_call = Duration::from_secs(60));
        let mut stream = reader.stream(chunks, Some(id));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), stream.next())
                .await
                .is_err()
        );
        assert_eq!(reader.packed.read_stats().chunk_range_requests, 1);
        drop(stream);
        reader.meter.reset();
        let permit =
            tokio::time::timeout(Duration::from_secs(1), reader.buffers.reserve(BUFFER_BYTES))
                .await
                .unwrap();
        drop(permit);
        objects.config_mut(|c| c.wait_get_per_call = Duration::ZERO);
    }

    #[tokio::test]
    async fn later_corrupt_chunk_is_never_yielded() {
        let (objects, reader, chunks, data, id) = fixture().await;
        let bad = reader
            .packed
            .location(&chunks[5].digest)
            .await
            .unwrap()
            .unwrap();
        let path = pack_path(&Path::default(), &bad.pack);
        let mut body = objects
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        body[bad.offset as usize..bad.offset as usize + 4].fill(0);
        objects.put(&path, Bytes::from(body).into()).await.unwrap();
        let mut stream = reader.stream(chunks.clone(), Some(id));
        let mut offset = 0;
        for chunk in &chunks[..5] {
            let bytes = stream.next().await.unwrap().unwrap();
            assert_eq!(bytes, data[offset..offset + chunk.size as usize]);
            offset += chunk.size as usize;
        }
        assert!(stream.next().await.unwrap().is_err());
        drop(stream);
        reader.meter.reset();
    }

    #[tokio::test]
    async fn overlapping_readers_share_a_small_buffer_budget_without_deadlock() {
        let (_, mut reader, chunks, data, id) = fixture().await;
        let inner = Arc::get_mut(&mut reader).unwrap();
        inner.window = 64 * 1024;
        inner.buffers = crate::byte_budget::ByteBudget::new(256 * 1024);
        let pair = async {
            tokio::join!(
                collect(reader.stream(chunks.clone(), Some(id))),
                collect(reader.stream(chunks.clone(), Some(id)))
            )
        };
        let (a, b) = tokio::time::timeout(Duration::from_secs(10), pair)
            .await
            .unwrap();
        assert_eq!(a.unwrap(), data);
        assert_eq!(b.unwrap(), data);
        assert!(reader.meter.peak() <= 256 * 1024);
        reader.meter.reset();
    }
}
impl Meter {
    pub(super) fn reset(&self) {
        assert_eq!(self.live.load(Ordering::Relaxed), 0);
        self.peak.store(0, Ordering::Relaxed);
    }
    pub(super) fn peak(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }
}
struct Response {
    bytes: Bytes,
    meter: Arc<Meter>,
}
impl Drop for Response {
    fn drop(&mut self) {
        self.meter
            .live
            .fetch_sub(self.bytes.len() as u64, Ordering::Relaxed);
    }
}

struct ChunkCache {
    capacity: u64,
    used: u64,
    clock: u64,
    entries: HashMap<ChunkId, (Bytes, u64)>,
}
impl ChunkCache {
    fn get(&mut self, id: ChunkId) -> Option<Bytes> {
        let (bytes, used) = self.entries.get_mut(&id)?;
        self.clock += 1;
        *used = self.clock;
        Some(bytes.clone())
    }
    fn insert(&mut self, id: ChunkId, bytes: &[u8]) -> u64 {
        if self.entries.contains_key(&id) || bytes.len() as u64 > self.capacity {
            return 0;
        }
        let mut evictions = 0;
        while self.used + bytes.len() as u64 > self.capacity {
            let oldest = *self
                .entries
                .iter()
                .min_by_key(|(_, (_, used))| used)
                .unwrap()
                .0;
            self.used -= self.entries.remove(&oldest).unwrap().0.len() as u64;
            evictions += 1;
        }
        self.clock += 1;
        self.used += bytes.len() as u64;
        // Own only this frame; never retain a whole covering response through
        // a tiny cache slice. Evict before allocating the replacement copy.
        self.entries
            .insert(id, (Bytes::copy_from_slice(bytes), self.clock));
        evictions
    }
}

struct Need {
    chunk: ChunkMeta,
    location: Location,
    hit: Option<Bytes>,
}
#[derive(Clone)]
struct Request {
    pack: PackId,
    start: u64,
    end: u64,
    members: Vec<usize>,
}

fn plan(needs: &[Need], window: u64) -> Vec<Request> {
    let mut packs: BTreeMap<PackId, Vec<usize>> = BTreeMap::new();
    let mut seen = HashSet::new();
    for (i, need) in needs.iter().enumerate() {
        if need.hit.is_none() && seen.insert(need.chunk.digest) {
            packs.entry(need.location.pack).or_default().push(i);
        }
    }
    let mut requests = Vec::new();
    for (pack, mut members) in packs {
        members.sort_by_key(|i| needs[*i].location.offset);
        let start = needs[members[0]].location.offset;
        let last = needs[*members.last().unwrap()].location;
        let end = last.offset + last.framed_len;
        let useful: u64 = members.iter().map(|i| needs[*i].location.framed_len).sum();
        if end - start <= window && (end - start - useful).saturating_mul(4) <= useful {
            requests.push(Request {
                pack,
                start,
                end,
                members,
            });
        } else {
            let mut runs: Vec<Request> = Vec::new();
            for i in members {
                let location = needs[i].location;
                if let Some(run) = runs.last_mut()
                    && run.end == location.offset
                    && location.offset + location.framed_len - run.start <= window
                {
                    run.end += location.framed_len;
                    run.members.push(i);
                } else {
                    runs.push(Request {
                        pack,
                        start: location.offset,
                        end: location.offset + location.framed_len,
                        members: vec![i],
                    });
                }
            }
            requests.extend(runs);
        }
    }
    // Prioritize ranges by their first consumer, not by object hash order.
    requests.sort_by_key(|r| *r.members.iter().min().unwrap());
    requests
}

pub(super) struct PlannedReader {
    packed: Arc<PackedChunks>,
    window: u64,
    cache: StdMutex<ChunkCache>,
    buffers: crate::byte_budget::ByteBudget,
    decode: crate::byte_budget::ByteBudget,
    requests: Arc<tokio::sync::Semaphore>,
    pub(super) meter: Arc<Meter>,
}
impl PlannedReader {
    pub(super) fn new(packed: Arc<PackedChunks>, cache: u64, window: u64) -> Arc<Self> {
        assert!(window > 0 && window <= 16 * 1024 * 1024);
        Arc::new(Self {
            packed,
            window,
            cache: StdMutex::new(ChunkCache {
                capacity: cache,
                used: 0,
                clock: 0,
                entries: HashMap::new(),
            }),
            buffers: crate::byte_budget::ByteBudget::new(BUFFER_BYTES),
            decode: crate::byte_budget::ByteBudget::new(BUFFER_BYTES),
            requests: Arc::new(tokio::sync::Semaphore::new(4)),
            meter: Arc::new(Meter::default()),
        })
    }

    async fn fetch(&self, request: &Request) -> io::Result<Arc<Response>> {
        self.packed
            .read_counters
            .chunk_range_requests
            .fetch_add(1, Ordering::Relaxed);
        let bytes = self
            .packed
            .object_store
            .get_range(
                &pack_path(&self.packed.base, &request.pack),
                request.start..request.end,
            )
            .await
            .map_err(object_store_io_error)?;
        if bytes.len() as u64 != request.end - request.start {
            return Err(io::Error::other("short planned range"));
        }
        self.packed
            .read_counters
            .chunk_range_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let live = self
            .meter
            .live
            .fetch_add(bytes.len() as u64, Ordering::Relaxed)
            + bytes.len() as u64;
        self.meter.peak.fetch_max(live, Ordering::Relaxed);
        Ok(Arc::new(Response {
            bytes,
            meter: self.meter.clone(),
        }))
    }

    async fn decode_frame(&self, frame: Frame) -> io::Result<Bytes> {
        let decode = Arc::new(self.decode.reserve(frame.chunk.size as usize).await);
        let decoded = crate::blob::chunked::decode_guarded(
            frame.bytes.clone(),
            frame.chunk.digest,
            frame.chunk.size,
            (frame.guard.clone(), decode),
        )
        .await?;
        if frame.missed {
            let evicted = self
                .cache
                .lock()
                .unwrap()
                .insert(frame.chunk.digest, &frame.bytes);
            self.packed
                .read_counters
                .cache_evictions
                .fetch_add(evicted, Ordering::Relaxed);
        }
        Ok(decoded)
    }

    pub(super) fn stream(
        self: &Arc<Self>,
        chunks: Vec<ChunkMeta>,
        expected: Option<BlobId>,
    ) -> BoxStream<'static, io::Result<Bytes>> {
        let reader = self.clone();
        let mut frames = self.frames(chunks, None);
        Box::pin(async_stream::try_stream! {
            let mut hasher = blake3::Hasher::new();
            while let Some(frame) = frames.next().await {
                let decoded = reader.decode_frame(frame?).await?;
                hasher.update(&decoded);
                yield decoded;
            }
            if let Some(expected) = expected
                && BlobId::new(hasher.finalize().into()) != expected {
                Err(io::Error::other("planned stream blob digest mismatch"))?;
            }
        })
    }

    pub(super) async fn reader_mode(
        self: &Arc<Self>,
        chunks: Vec<ChunkMeta>,
        expected: Option<BlobId>,
        lookahead: bool,
    ) -> io::Result<crate::blob::chunked_reader::ChunkedReader> {
        let mut frozen = BTreeMap::new();
        for chunk in &chunks {
            if let std::collections::btree_map::Entry::Vacant(entry) = frozen.entry(chunk.digest) {
                let location = self
                    .packed
                    .freeze_read(&chunk.digest)
                    .await?
                    .ok_or_else(|| io::Error::other("missing frozen chunk"))?;
                entry.insert(location);
            }
        }
        let source = Arc::new(PlannedSource {
            lookahead,
            reader: self.clone(),
            frozen: Arc::new(frozen),
            chunks: chunks.clone(),
            cursor: tokio::sync::Mutex::new(Cursor {
                at: 0,
                frames: None,
            }),
        });
        Ok(crate::blob::chunked_reader::ChunkedReader::new(
            source,
            chunks.into_iter().map(|chunk| (chunk.digest, chunk.size)),
            expected,
        ))
    }

    async fn prepare_window(
        self: &Arc<Self>,
        chunks: Vec<ChunkMeta>,
        frozen: &BTreeMap<ChunkId, FrozenChunk>,
    ) -> io::Result<Vec<Frame>> {
        let mut needs = Vec::with_capacity(chunks.len());
        let mut bytes = 0;
        for chunk in chunks {
            let location = frozen
                .get(&chunk.digest)
                .ok_or_else(|| io::Error::other("chunk outside frozen plan"))?
                .0;
            bytes += location.framed_len;
            needs.push(Need {
                chunk,
                location,
                hit: None,
            });
        }
        // Hits and misses partition the window: retained hit allocations plus
        // responses (at most 1.25 times missing bytes) need at most 1.25 W,
        // not W + 1.25 W. Reserve before cloning any cache allocation.
        let charged = bytes + bytes.div_ceil(4);
        if charged > BUFFER_BYTES as u64 {
            return Err(io::Error::other("look-ahead window exceeds buffer budget"));
        }
        let guard = Arc::new(self.buffers.reserve(charged as usize).await);
        for need in &mut needs {
            need.hit = self.cache.lock().unwrap().get(need.chunk.digest);
            if need.hit.is_some() {
                self.packed
                    .read_counters
                    .cache_hits
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        let requests = plan(&needs, self.window);
        let work: Vec<_> = requests
            .iter()
            .map(|request| {
                let reader = self.clone();
                async move {
                    let _request = reader.requests.clone().acquire_owned().await.unwrap();
                    reader.fetch(request).await
                }
                .boxed()
            })
            .collect();
        let responses: Vec<Arc<Response>> = futures::stream::iter(work)
            .buffered(4)
            .try_collect()
            .await?;
        let retained = needs
            .iter()
            .filter_map(|n| n.hit.as_ref())
            .map(|b| b.len() as u64)
            .sum::<u64>()
            + responses.iter().map(|r| r.bytes.len() as u64).sum::<u64>();
        if retained > charged {
            return Err(io::Error::other("look-ahead ownership exceeds reservation"));
        }
        let mut owners = HashMap::new();
        for (i, request) in requests.iter().enumerate() {
            for member in &request.members {
                owners.insert(needs[*member].chunk.digest, i);
            }
        }
        let mut frames = Vec::with_capacity(needs.len());
        for need in needs {
            let missed = need.hit.is_none();
            let (bytes, owner) = if let Some(hit) = need.hit {
                (hit, None)
            } else {
                let i = owners[&need.chunk.digest];
                let start = (need.location.offset - requests[i].start) as usize;
                (
                    responses[i]
                        .bytes
                        .slice(start..start + need.location.framed_len as usize),
                    Some(responses[i].clone()),
                )
            };
            frames.push(Frame {
                bytes,
                chunk: need.chunk,
                missed,
                guard: (guard.clone(), owner),
            });
        }
        Ok(frames)
    }

    fn lookahead_frames(
        self: &Arc<Self>,
        chunks: Vec<ChunkMeta>,
        frozen: Arc<BTreeMap<ChunkId, FrozenChunk>>,
    ) -> BoxStream<'static, io::Result<Frame>> {
        let reader = self.clone();
        Box::pin(async_stream::try_stream! {
            if let Some(first) = chunks.first() {
                // Complete the initial demanded fetch before launching wider
                // look-ahead. Explicit short reads contain only this chunk.
                for frame in reader.prepare_window(vec![first.clone()], &frozen).await? { yield frame; }
            }
            let mut windows = Vec::new();
            let mut window = Vec::new();
            let mut bytes = 0;
            for chunk in chunks.into_iter().skip(1) {
                let size = frozen[&chunk.digest].0.framed_len;
                if !window.is_empty() && bytes + size > reader.window {
                    windows.push(std::mem::take(&mut window));
                    bytes = 0;
                }
                bytes += size;
                window.push(chunk);
            }
            if !window.is_empty() { windows.push(window); }
            if !windows.is_empty() {
                let (send, receive) = tokio::sync::mpsc::channel(1);
                let task = tokio::spawn(async move {
                    let work = futures::stream::iter(windows).map(move |chunks| {
                        let reader = reader.clone();
                        let frozen = frozen.clone();
                        async move { reader.prepare_window(chunks, &frozen).await }.boxed()
                    });
                    let mut work = work.buffered(2);
                    while let Some(result) = work.next().await {
                        let failed = result.is_err();
                        if send.send(result).await.is_err() || failed { break; }
                    }
                });
                let mut pump = WindowPump { receive, task };
                while let Some(window) = pump.receive.recv().await {
                    for frame in window? { yield frame; }
                }
                // A panicked producer must not look like successful EOF.
                (&mut pump.task).await.map_err(io::Error::other)?;
            }
        })
    }

    /// The caller supplies precisely the chunks it may read. An explicit short
    /// range can pass one chunk. Dropping the stream drops pending range futures.
    fn frames(
        self: &Arc<Self>,
        chunks: Vec<ChunkMeta>,
        frozen: Option<Arc<BTreeMap<ChunkId, FrozenChunk>>>,
    ) -> BoxStream<'static, io::Result<Frame>> {
        let reader = self.clone();
        Box::pin(async_stream::try_stream! {
            let mut at = 0;
            while at < chunks.len() {
                let mut needs = Vec::new();
                let mut bytes = 0;
                let first = at == 0;
                while at < chunks.len() {
                    let chunk = chunks[at].clone();
                    let location = if let Some(frozen) = &frozen {
                        frozen.get(&chunk.digest).ok_or_else(|| io::Error::other("chunk outside frozen plan"))?.0
                    } else {
                        reader.packed.location(&chunk.digest).await?
                            .ok_or_else(|| io::Error::other("missing planned chunk"))?
                    };
                    if !needs.is_empty() && bytes + location.framed_len > reader.window {
                        break;
                    }
                    bytes += location.framed_len;
                    needs.push(Need {chunk, location, hit: None});
                    at += 1;
                    // Yield the first demanded chunk before speculative work.
                    if first { break; }
                }
                // Reserve before cloning cache hits: otherwise another reader
                // could evict their backing allocations while we await permits.
                let charged = bytes + bytes + bytes.div_ceil(4);
                if charged > BUFFER_BYTES as u64 {
                    Err(io::Error::other("prototype window exceeds buffer budget"))?;
                }
                // Account for completed and prefetched responses plus cache
                // allocations kept alive by window hits after cache eviction.
                let _buffers = Arc::new(reader.buffers.reserve(charged as usize).await);
                for need in &mut needs {
                    need.hit = reader.cache.lock().unwrap().get(need.chunk.digest);
                    if need.hit.is_some() {
                        reader.packed.read_counters.cache_hits.fetch_add(1, Ordering::Relaxed);
                    }
                }
                let requests = plan(&needs, reader.window);
                let mut owners = HashMap::new();
                for (i, request) in requests.iter().enumerate() {
                    for member in &request.members {
                        owners.insert(needs[*member].chunk.digest, i);
                    }
                }
                let work: Vec<_> = requests.iter().map(|request| {
                    let reader = reader.clone();
                    let request = request.clone();
                    async move { reader.fetch(&request).await }.boxed()
                }).collect();
                let mut pending = futures::stream::iter(work).buffered(4);
                let mut responses: Vec<Arc<Response>> = Vec::new();
                for need in &needs {
                    let compressed = if let Some(hit) = &need.hit {
                        hit.clone()
                    } else {
                        let index = owners[&need.chunk.digest];
                        while responses.len() <= index {
                            responses.push(pending.next().await.unwrap()?);
                        }
                        let start = (need.location.offset - requests[index].start) as usize;
                        responses[index].bytes.slice(start..start + need.location.framed_len as usize)
                    };
                    let owner = owners.get(&need.chunk.digest).and_then(|i| responses.get(*i)).cloned();
                    yield Frame { bytes: compressed, chunk: need.chunk.clone(),
                        guard: (_buffers.clone(), owner), missed: need.hit.is_none() };
                }
            }
        })
    }
}

struct Frame {
    bytes: Bytes,
    chunk: ChunkMeta,
    guard: (
        Arc<tokio::sync::OwnedSemaphorePermit>,
        Option<Arc<Response>>,
    ),
    missed: bool,
}
struct Cursor {
    at: usize,
    frames: Option<BoxStream<'static, io::Result<Frame>>>,
}
struct PlannedSource {
    lookahead: bool,
    reader: Arc<PlannedReader>,
    frozen: Arc<BTreeMap<ChunkId, FrozenChunk>>,
    chunks: Vec<ChunkMeta>,
    cursor: tokio::sync::Mutex<Cursor>,
}
#[async_trait::async_trait]
impl crate::blob::chunked_reader::ChunkSource for PlannedSource {
    async fn fetch_chunk(&self, digest: ChunkId, size: u64) -> io::Result<Bytes> {
        let frame = {
            let mut cursor = self.cursor.lock().await;
            if self
                .chunks
                .get(cursor.at)
                .is_none_or(|c| c.digest != digest || c.size != size)
            {
                cursor.frames = None;
                cursor.at = self
                    .chunks
                    .iter()
                    .position(|c| c.digest == digest && c.size == size)
                    .ok_or_else(|| io::Error::other("chunk outside frozen read plan"))?;
            }
            if cursor.frames.is_none() {
                let chunks = self.chunks[cursor.at..].to_vec();
                cursor.frames = Some(if self.lookahead {
                    self.reader.lookahead_frames(chunks, self.frozen.clone())
                } else {
                    self.reader.frames(chunks, Some(self.frozen.clone()))
                });
            }
            let frame = cursor
                .frames
                .as_mut()
                .unwrap()
                .next()
                .await
                .ok_or_else(|| io::Error::other("short compressed plan"))??;
            cursor.at += 1;
            if cursor.at == self.chunks.len() {
                // Do not retain the final window through an exhausted source.
                cursor.frames = None;
            }
            frame
        };
        // Keep decompression outside the planner lock: ChunkedReader owns its
        // normal two-chunk overlap, seeking, and whole-blob verification.
        self.reader.decode_frame(frame).await
    }
}

struct WindowPump {
    receive: tokio::sync::mpsc::Receiver<io::Result<Vec<Frame>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for WindowPump {
    fn drop(&mut self) {
        self.task.abort();
    }
}
