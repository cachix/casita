//! Ordered compressed fetches, sharing one byte cache and bounded I/O budget.
//! The task owns the admitted physical plan and its pin until all I/O stops.
use super::*;
use crate::blob::chunked_reader::{ChunkSource, ChunkedReader};
use crate::byte_budget::ByteBudget;

const WINDOW_BYTES: u64 = 16 * 1024 * 1024;
const BUFFER_BYTES: usize = 64 * 1024 * 1024;

pub(super) struct Cache {
    capacity: u64,
    used: u64,
    clock: u64,
    entries: HashMap<ChunkId, (Bytes, u64)>,
    order: BTreeMap<u64, ChunkId>,
}
impl Cache {
    fn new(capacity: u64) -> Self {
        Self {
            capacity,
            used: 0,
            clock: 0,
            entries: HashMap::new(),
            order: BTreeMap::new(),
        }
    }
    fn tick(&mut self) -> u64 {
        if self.clock == u64::MAX {
            let ids: Vec<_> = self.order.values().copied().collect();
            self.order.clear();
            for (i, id) in ids.into_iter().enumerate() {
                self.entries.get_mut(&id).unwrap().1 = i as u64;
                self.order.insert(i as u64, id);
            }
            self.clock = self.entries.len() as u64;
        }
        self.clock += 1;
        self.clock
    }
    pub(super) fn get(&mut self, id: ChunkId) -> Option<Bytes> {
        let age = self.tick();
        let (bytes, previous) = self.entries.get_mut(&id)?;
        self.order.remove(previous);
        *previous = age;
        self.order.insert(age, id);
        Some(bytes.clone())
    }
    pub(super) fn insert(&mut self, id: ChunkId, bytes: &[u8]) -> u64 {
        let size = bytes.len() as u64;
        if size == 0 || size > self.capacity || self.entries.contains_key(&id) {
            return 0;
        }
        let mut evicted = 0;
        while self.used > self.capacity - size {
            let (_, oldest) = self.order.pop_first().unwrap();
            self.used -= self.entries.remove(&oldest).unwrap().0.len() as u64;
            evicted += 1;
        }
        let age = self.tick();
        // Evict before copying; never retain a covering response through a slice.
        self.entries
            .insert(id, (Bytes::copy_from_slice(bytes), age));
        self.order.insert(age, id);
        self.used += size;
        evicted
    }
}

pub(super) struct State {
    pub(super) cache: StdMutex<Cache>,
    buffers: ByteBudget,
    requests: Arc<tokio::sync::Semaphore>,
}
impl State {
    pub(super) fn new(capacity: u64) -> Self {
        Self {
            cache: StdMutex::new(Cache::new(capacity)),
            buffers: ByteBudget::new(BUFFER_BYTES),
            requests: Arc::new(tokio::sync::Semaphore::new(4)),
        }
    }
}

struct ReadPlan {
    chunks: Vec<ChunkMeta>,
    frozen: BTreeMap<ChunkId, FrozenChunk>,
    _pin: Option<crate::metadata::DataPinLease>,
}
struct Context {
    packed: PackReader,
    plan: ReadPlan,
    decode: ByteBudget,
}
struct Frame {
    bytes: Bytes,
    chunk: ChunkMeta,
    /// The window reservation, held until every frame slicing into it is
    /// consumed. Absent when the window was fetched uncharged.
    lease: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
    missed: bool,
}

/// How a compressed window is admitted against the shared buffer budget.
///
/// A reservation lives as long as the frames that slice into it, and a reader
/// can sit idle mid-blob with frames in hand, so waiting on a budget that idle
/// readers hold is a cycle. Read-ahead is therefore the only thing that ever
/// waits for room, by giving up, and a window a consumer is blocked on always
/// makes progress.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Admission {
    /// Fetch only while the budget has room for the whole window.
    WhenFree,
    /// Fetch as much of the window as the budget can serve right now, at
    /// least one chunk, uncharged when it can serve nothing.
    Demand,
}

/// One read-ahead outcome, in the order the consumer needs the chunks.
enum Prefetch {
    /// Read-ahead fetched the window before the consumer asked for it.
    Ready(Vec<Frame>),
    /// The budget had no room to spare; the consumer fetches this range.
    Deferred(Range<usize>),
}
struct Cursor {
    at: usize,
    frames: Option<BoxStream<'static, io::Result<Frame>>>,
}
struct Source {
    context: Arc<Context>,
    cursor: Mutex<Cursor>,
    #[cfg(test)]
    profile: Option<Arc<SeekProfile>>,
}

#[cfg(test)]
#[derive(Default)]
struct SeekProfile {
    calls: AtomicU64,
    decoded_bytes: AtomicU64,
    fetch_ns: AtomicU64,
    admission_ns: AtomicU64,
    decode_ns: AtomicU64,
}

pub(super) fn reader(
    packed: PackReader,
    chunks: Vec<ChunkMeta>,
    frozen: BTreeMap<ChunkId, FrozenChunk>,
    pin: Option<crate::metadata::DataPinLease>,
    decode: ByteBudget,
    expected: BlobId,
) -> ChunkedReader {
    let table = chunks
        .iter()
        .map(|c| (c.digest, c.size))
        .collect::<Vec<_>>();
    let source = Source {
        #[cfg(test)]
        profile: None,
        context: Arc::new(Context {
            packed,
            plan: ReadPlan {
                chunks,
                frozen,
                _pin: pin,
            },
            decode,
        }),
        cursor: Mutex::new(Cursor {
            at: 0,
            frames: None,
        }),
    };
    ChunkedReader::new(Arc::new(source), table, Some(expected))
}

/// Stream a blob's plaintext in order. Unlike the seeking reader, a decode
/// reservation is held while the decoded bytes are yielded, so a caller that
/// stops draining holds a chunk of the plaintext budget that every writer of
/// the same store also draws on. Drain it or drop it; do not park it.
pub(super) fn stream(
    packed: PackReader,
    chunks: Vec<ChunkMeta>,
    frozen: BTreeMap<ChunkId, FrozenChunk>,
    decode: ByteBudget,
    expected: BlobId,
) -> BoxStream<'static, io::Result<Bytes>> {
    let context = Arc::new(Context {
        packed,
        plan: ReadPlan {
            chunks,
            frozen,
            _pin: None,
        },
        decode,
    });
    Box::pin(async_stream::try_stream! {
        let mut frames = context.frames(0);
        let mut hasher = blake3::Hasher::new();
        while let Some(frame) = frames.try_next().await? {
            let size = usize::try_from(frame.chunk.size).map_err(io::Error::other)?;
            let memory = Arc::new(context.decode.reserve(super::super::chunked::stream_chunk_working_set(frame.bytes.len(), Some(size))).await);
            let mut decoded = super::super::chunked::decompress_verified_chunk_stream_guarded(
                frame.bytes.clone(), frame.chunk.digest, size, Some(size),
                (frame.lease.clone(), memory, context.clone()),
            );
            while let Some(bytes) = decoded.try_next().await? {
                hasher.update(&bytes);
                yield bytes;
            }
            if frame.missed {
                let evicted = context.packed.fetch.cache.lock().unwrap().insert(frame.chunk.digest, &frame.bytes);
                context.packed.read_counters.cache_evictions.fetch_add(evicted, Ordering::Relaxed);
            }
        }
        if BlobId::new(hasher.finalize().into()) != expected {
            Err(io::Error::other(crate::blob::BlobIntegrityError::Blob { expected }))?;
        }
    })
}

struct Need {
    chunk: ChunkMeta,
    location: Location,
    hit: Option<Bytes>,
    retained_len: u64,
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

impl Context {
    /// Fetch a prefix of `range`. `None` means read-ahead found no room; a
    /// demanded window always returns at least its first chunk.
    async fn window(
        self: &Arc<Self>,
        range: Range<usize>,
        admission: Admission,
    ) -> io::Result<Option<Vec<Frame>>> {
        let mut needs = Vec::with_capacity(range.len());
        let mut bytes = 0u64;
        for chunk in &self.plan.chunks[range] {
            let location = self
                .plan
                .frozen
                .get(&chunk.digest)
                .ok_or_else(|| io::Error::other("chunk outside admitted read plan"))?
                .0;
            if location.framed_len == 0
                || location
                    .offset
                    .checked_add(location.framed_len)
                    .is_none_or(|end| end > location.pack_len)
            {
                return Err(io::Error::other("invalid admitted pack range"));
            }
            // The logical cache can retain a different encoding of this digest.
            // Inspect its size without cloning bytes, which is what the
            // reservation below is sized from.
            let retained_len = self
                .packed
                .fetch
                .cache
                .lock()
                .unwrap()
                .entries
                .get(&chunk.digest)
                .map_or(location.framed_len, |(bytes, _)| {
                    location.framed_len.max(bytes.len() as u64)
                });
            bytes = bytes
                .checked_add(retained_len)
                .ok_or_else(|| io::Error::other("fetch window overflow"))?;
            needs.push(Need {
                chunk: chunk.clone(),
                location,
                hit: None,
                retained_len,
            });
        }
        if bytes.saturating_add(bytes.div_ceil(4)) > BUFFER_BYTES as u64 {
            // Cached encodings need not have the same compression ratio. A
            // window that no longer fits can always use its physical ranges.
            bytes = 0;
            for need in &mut needs {
                need.retained_len = need.location.framed_len;
                bytes = bytes
                    .checked_add(need.retained_len)
                    .ok_or_else(|| io::Error::other("fetch window overflow"))?;
            }
        }
        // Hits and misses partition the window; gaps add at most 25% of misses.
        let charge = |bytes: u64| bytes.saturating_add(bytes.div_ceil(4));
        let mut charged = charge(bytes);
        let mut lease = (charged <= BUFFER_BYTES as u64)
            .then(|| self.packed.fetch.buffers.try_reserve(charged as usize))
            .flatten()
            .map(Arc::new);
        if lease.is_none() {
            if admission == Admission::WhenFree {
                self.packed
                    .read_counters
                    .readahead_deferrals
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
            // Keep the longest prefix the budget can serve without waiting.
            // One chunk is the smallest unit of progress and takes what room
            // is left, or none at all, so a consumer is never held up by
            // buffers that another reader will not release until it is polled.
            let free = self.packed.fetch.buffers.free_bytes() as u64;
            let mut kept = 0usize;
            let mut prefix = 0u64;
            for need in &needs {
                let next = prefix.saturating_add(need.retained_len);
                if kept > 0 && charge(next) > free {
                    break;
                }
                prefix = next;
                kept += 1;
            }
            needs.truncate(kept);
            charged = charge(prefix);
            if charged > BUFFER_BYTES as u64 {
                return Err(io::Error::other("compressed window exceeds read budget"));
            }
            lease = self
                .packed
                .fetch
                .buffers
                .try_reserve(charged as usize)
                .map(Arc::new);
            if lease.is_none() {
                self.packed
                    .read_counters
                    .buffer_bypasses
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        for need in &mut needs {
            need.hit = self
                .packed
                .fetch
                .cache
                .lock()
                .unwrap()
                .get(need.chunk.digest)
                // A concurrent eviction/publication may install a larger frame
                // after reservation. Fetch the admitted physical range instead.
                .filter(|bytes| bytes.len() as u64 <= need.retained_len);
            if need.hit.is_some() {
                self.packed
                    .read_counters
                    .cache_hits
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        let requests = plan(&needs, WINDOW_BYTES);
        let work: Vec<_> = requests
            .iter()
            .cloned()
            .map(|request| {
                let context = self.clone();
                async move {
                    let _request = context
                        .packed
                        .fetch
                        .requests
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(io::Error::other)?;
                    context
                        .packed
                        .read_counters
                        .chunk_range_requests
                        .fetch_add(1, Ordering::Relaxed);
                    let bytes = context
                        .packed
                        .object_store
                        .get_range(
                            &pack_path(&context.packed.base, &request.pack),
                            request.start..request.end,
                        )
                        .await
                        .map_err(object_store_io_error)?;
                    context
                        .packed
                        .read_counters
                        .chunk_range_bytes
                        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    if bytes.len() as u64 != request.end - request.start {
                        return Err(io::Error::other("short planned pack range"));
                    }
                    Ok(bytes)
                }
                .boxed()
            })
            .collect();
        let responses: Vec<Bytes> = futures::stream::iter(work)
            .buffered(4)
            .try_collect()
            .await?;
        let retained = needs
            .iter()
            .filter_map(|n| n.hit.as_ref())
            .map(|b| b.len() as u64)
            .sum::<u64>()
            + responses.iter().map(|b| b.len() as u64).sum::<u64>();
        if retained > charged {
            return Err(io::Error::other("fetch window exceeded reservation"));
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
            let bytes = if let Some(hit) = need.hit {
                hit
            } else {
                let i = owners[&need.chunk.digest];
                let start = (need.location.offset - requests[i].start) as usize;
                responses[i].slice(start..start + need.location.framed_len as usize)
            };
            frames.push(Frame {
                bytes,
                chunk: need.chunk,
                lease: lease.clone(),
                missed,
            });
        }
        Ok(Some(frames))
    }

    /// Fetch a prefix of `range` for a consumer that is blocked on it.
    async fn demand(self: &Arc<Self>, range: Range<usize>) -> io::Result<Vec<Frame>> {
        Ok(self
            .window(range, Admission::Demand)
            .await?
            .expect("a demanded window fetches at least its first chunk"))
    }

    fn frames(self: &Arc<Self>, start: usize) -> BoxStream<'static, io::Result<Frame>> {
        let context = self.clone();
        Box::pin(async_stream::try_stream! {
            if start < context.plan.chunks.len() {
                for frame in context.demand(start..start + 1).await? { yield frame; }
            }
            let mut windows = Vec::new();
            let mut begin = start + 1;
            let mut bytes = 0u64;
            for (at, chunk) in context.plan.chunks.iter().enumerate().skip(begin) {
                let size = context.plan.frozen[&chunk.digest].0.framed_len;
                if at > begin && bytes.saturating_add(size) > WINDOW_BYTES {
                    windows.push(begin..at);
                    begin = at;
                    bytes = 0;
                }
                bytes = bytes.saturating_add(size);
            }
            if begin < context.plan.chunks.len() { windows.push(begin..context.plan.chunks.len()); }
            if !windows.is_empty() {
                let (send, receive) = tokio::sync::mpsc::channel(1);
                let ahead = context.clone();
                let task = tokio::spawn(async move {
                    // Context owns the durable lease throughout queued and active I/O.
                    let work = futures::stream::iter(windows).map(move |range| {
                        let context = ahead.clone();
                        async move {
                            // Window I/O must keep running even while the pump
                            // is blocked delivering an earlier result. Otherwise
                            // unpolled ranges can retain every request permit
                            // needed by a consumer fetching a deferred window.
                            let mut task = WindowTask::spawn(async move {
                                match context.window(range.clone(), Admission::WhenFree).await {
                                    Ok(Some(frames)) => Ok(Prefetch::Ready(frames)),
                                    Ok(None) => Ok(Prefetch::Deferred(range)),
                                    Err(error) => Err(error),
                                }
                            });
                            task.join().await
                        }
                        .boxed()
                    });
                    let mut work = work.buffered(2);
                    while let Some(result) = work.next().await {
                        let failed = result.is_err();
                        if send.send(result).await.is_err() || failed { break; }
                    }
                });
                let mut pump = Pump { receive, task: Some(task) };
                while let Some(window) = pump.receive.recv().await {
                    match window? {
                        Prefetch::Ready(frames) => {
                            for frame in frames { yield frame; }
                        }
                        Prefetch::Deferred(range) => {
                            // Read-ahead found no room. Serve the consumer
                            // itself, taking whatever the budget allows.
                            let mut at = range.start;
                            while at < range.end {
                                let frames = context.demand(at..range.end).await?;
                                at += frames.len();
                                for frame in frames { yield frame; }
                            }
                        }
                    }
                }
                pump.task.take().unwrap().await.map_err(io::Error::other)?;
            }
        })
    }
}

#[async_trait::async_trait]
impl ChunkSource for Source {
    async fn park(&self) {
        // Dropping frames also drops the pump, aborting queued/in-flight windows.
        // Keep the frozen plan and pin so this reader can resume without reopening.
        self.cursor.lock().await.frames = None;
    }

    async fn fetch_chunk(&self, digest: ChunkId, size: u64) -> io::Result<Bytes> {
        #[cfg(test)]
        let fetch_started = self.profile.as_ref().map(|_| std::time::Instant::now());
        let context = &self.context;
        let frame = {
            let mut cursor = self.cursor.lock().await;
            if context
                .plan
                .chunks
                .get(cursor.at)
                .is_none_or(|c| c.digest != digest || c.size != size)
            {
                cursor.frames = None;
                cursor.at = context
                    .plan
                    .chunks
                    .iter()
                    .position(|c| c.digest == digest && c.size == size)
                    .ok_or_else(|| io::Error::other("chunk outside admitted read plan"))?;
            }
            if cursor.frames.is_none() {
                cursor.frames = Some(context.frames(cursor.at));
            }
            let frame = cursor
                .frames
                .as_mut()
                .unwrap()
                .next()
                .await
                .ok_or_else(|| io::Error::other("short compressed read plan"))??;
            cursor.at += 1;
            if cursor.at == context.plan.chunks.len() {
                cursor.frames = None;
            }
            frame
        };
        #[cfg(test)]
        if let (Some(profile), Some(started)) = (&self.profile, fetch_started) {
            profile
                .fetch_ns
                .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
        #[cfg(test)]
        let admission_started = self.profile.as_ref().map(|_| std::time::Instant::now());
        let memory = Arc::new(context.decode.reserve(size as usize).await);
        #[cfg(test)]
        if let (Some(profile), Some(started)) = (&self.profile, admission_started) {
            profile
                .admission_ns
                .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
        #[cfg(test)]
        let decode_started = self.profile.as_ref().map(|_| std::time::Instant::now());
        let decoded = super::super::chunked::decode_guarded(
            frame.bytes.clone(),
            digest,
            size,
            (frame.lease.clone(), memory, context.clone()),
        )
        .await?;
        #[cfg(test)]
        if let (Some(profile), Some(started)) = (&self.profile, decode_started) {
            profile.calls.fetch_add(1, Ordering::Relaxed);
            profile
                .decoded_bytes
                .fetch_add(decoded.len() as u64, Ordering::Relaxed);
            profile
                .decode_ns
                .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
        if frame.missed {
            let evicted = context
                .packed
                .fetch
                .cache
                .lock()
                .unwrap()
                .insert(digest, &frame.bytes);
            context
                .packed
                .read_counters
                .cache_evictions
                .fetch_add(evicted, Ordering::Relaxed);
        }
        Ok(decoded)
    }
}
struct Pump {
    receive: tokio::sync::mpsc::Receiver<io::Result<Prefetch>>,
    task: Option<tokio::task::JoinHandle<()>>,
}
// Each buffered window owns one independently polled task. The two-window
// limit and shared byte budget still bound queued and active compressed data.
struct WindowTask {
    task: Option<tokio::task::JoinHandle<io::Result<Prefetch>>>,
}
impl WindowTask {
    fn spawn(
        work: impl std::future::Future<Output = io::Result<Prefetch>> + Send + 'static,
    ) -> Self {
        Self {
            task: Some(tokio::spawn(work)),
        }
    }
    async fn join(&mut self) -> io::Result<Prefetch> {
        let result = self.task.as_mut().unwrap().await;
        self.task.take();
        result.map_err(io::Error::other)?
    }
}
fn abort_and_track<T: Send + 'static>(task: tokio::task::JoinHandle<T>) {
    task.abort();
    // Aborting schedules cancellation. Track the join so shutdown waits for
    // both pump and window tasks to release their plans, buffers, and pins.
    if tokio::runtime::Handle::try_current().is_ok() {
        crate::metadata::spawn_lease_task(async move {
            match task.await {
                Ok(_) => Ok(()),
                Err(error) if error.is_cancelled() => Ok(()),
                Err(error) => Err(crate::metadata::MetadataError::Backend(error.to_string())),
            }
        });
    }
}
impl Drop for WindowTask {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            abort_and_track(task);
        }
    }
}
impl Drop for Pump {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            abort_and_track(task);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::BlobReader;

    #[tokio::test]
    async fn backpressured_pump_releases_requests_for_demand() {
        use object_store::memory::InMemory;
        use object_store::throttle::{ThrottleConfig, ThrottledStore};
        use std::time::{Duration, Instant};

        // Cover both sides of the four-request I/O concurrency limit.
        for count in [2, 3, 4, 8] {
            let objects = Arc::new(ThrottledStore::new(
                InMemory::new(),
                ThrottleConfig::default(),
            ));
            let packed =
                PackedChunks::open_with_cache(objects.clone(), Path::default(), 64 * 1024, 0)
                    .await
                    .unwrap();
            let mut data = vec![0; (count + 1) * 64 * 1024];
            blake3::Hasher::new()
                .update(b"reserved-demand-request")
                .finalize_xof()
                .fill(&mut data);
            let mut chunks = Vec::new();
            for bytes in data.chunks(64 * 1024) {
                let chunk = ChunkMeta {
                    digest: ChunkId::new(blake3::hash(bytes).into()),
                    size: bytes.len() as u64,
                };
                packed
                    .put(
                        chunk.clone(),
                        Bytes::from(zstd::encode_all(bytes, 0).unwrap()),
                    )
                    .await
                    .unwrap();
                chunks.push(chunk);
            }
            packed.flush().await.unwrap();
            let context = Arc::new(Context {
                packed: packed.reader(),
                plan: ReadPlan {
                    frozen: packed.freeze_manifest(&chunks).await.unwrap().unwrap(),
                    chunks,
                    _pin: None,
                },
                decode: ByteBudget::new(BUFFER_BYTES),
            });
            objects.config_mut(|c| c.wait_get_per_call = Duration::from_millis(100));
            let (send, receive) = tokio::sync::mpsc::channel(1);
            send.send(()).await.unwrap();
            let ahead = context.clone();
            let pending = tokio::spawn(async move {
                let mut task = WindowTask::spawn(async move {
                    Ok(Prefetch::Ready(
                        ahead.window(0..count, Admission::WhenFree).await?.unwrap(),
                    ))
                });
                // The pump cannot poll its window handle until this full
                // channel is released, but the range futures must keep running.
                let _ = send.send(()).await;
                task.join().await
            });
            let expected_free = 4 - count.min(4);
            tokio::time::timeout(Duration::from_secs(2), async {
                while packed.fetch.requests.available_permits() > expected_free {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            // Give every pending request a chance to attempt admission.
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }
            assert_eq!(packed.fetch.requests.available_permits(), expected_free);
            objects.config_mut(|c| c.wait_get_per_call = Duration::ZERO);
            let started = Instant::now();
            let frames =
                tokio::time::timeout(Duration::from_secs(2), context.demand(count..count + 1))
                    .await
                    .expect("demand must progress while the pump delivery is blocked")
                    .unwrap();
            let elapsed = started.elapsed();
            assert_eq!(frames.len(), 1);
            let chunk = &context.plan.chunks[count];
            let decoded = zstd::decode_all(frames[0].bytes.as_ref()).unwrap();
            assert_eq!(decoded.len() as u64, chunk.size);
            assert_eq!(ChunkId::new(blake3::hash(&decoded).into()), chunk.digest);
            assert_eq!(decoded, data[count * 64 * 1024..]);
            drop(frames);
            assert!(
                !pending.is_finished(),
                "the delivery channel must remain blocked"
            );
            drop(receive);
            match pending.await.unwrap().unwrap() {
                Prefetch::Ready(frames) => assert_eq!(frames.len(), count),
                Prefetch::Deferred(_) => panic!("small speculative window was deferred"),
            }
            let all = packed
                .fetch
                .requests
                .clone()
                .try_acquire_many_owned(4)
                .expect("cancellation releases every request permit");
            drop(all);
            let buffers = packed
                .fetch
                .buffers
                .try_reserve(BUFFER_BYTES)
                .expect("cancellation releases compressed buffers");
            drop(buffers);
            println!(
                "demand_progress_sample {}",
                serde_json::json!({
                    "speculative_requests": count,
                    "demand_nanos": elapsed.as_nanos() as u64,
                    "correctness": "exact bytes, verified digest, demand progress, permits released",
                })
            );
        }
    }

    fn id(n: u8) -> ChunkId {
        ChunkId::new(Digest::from([n; 32]))
    }

    #[tokio::test]
    async fn cache_reservation_uses_retained_encoding_size() {
        use object_store::memory::InMemory;
        let packed = PackedChunks::open_with_cache(
            Arc::new(InMemory::new()),
            Path::default(),
            256 * 1024,
            256 * 1024,
        )
        .await
        .unwrap();
        packed
            .fetch
            .cache
            .lock()
            .unwrap()
            .insert(id(1), &[7; 128 * 1024]);
        let context = Arc::new(Context {
            packed: packed.reader(),
            plan: ReadPlan {
                chunks: vec![ChunkMeta {
                    digest: id(1),
                    size: 1024,
                }],
                frozen: BTreeMap::from([(
                    id(1),
                    FrozenChunk(Location {
                        pack: PackId::new(Digest::from([2; 32])),
                        pack_len: 8,
                        offset: 0,
                        framed_len: 8,
                        uncompressed_len: 1024,
                    }),
                )]),
                _pin: None,
            },
            decode: ByteBudget::new(BUFFER_BYTES),
        });
        let frames = context.demand(0..1).await.unwrap();
        assert_eq!(frames[0].bytes.len(), 128 * 1024);
        assert_eq!(frames[0].lease.as_ref().unwrap().num_permits(), 3);
        assert_eq!(packed.read_stats().chunk_range_requests, 0);
    }

    #[tokio::test]
    async fn public_stream_uses_planned_cache_with_a_tiny_decode_budget() {
        use crate::blob::{BlobStore, ChunkedBlobStore};
        use object_store::memory::InMemory;
        use tokio::io::AsyncReadExt;
        let objects = Arc::new(InMemory::new());
        let writer = ChunkedBlobStore::packed_with_options(
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
            .update(b"production-stream")
            .finalize_xof()
            .fill(&mut data);
        let digest = writer.put_slice(&data).await.unwrap();
        writer.flush().await.unwrap();
        drop(writer);
        let store = ChunkedBlobStore::packed_with_options(
            objects,
            Path::default(),
            64 * 1024,
            crate::PackOptions {
                target_size: 256 * 1024,
                cache_capacity: 4 * 1024 * 1024,
            },
        )
        .await
        .unwrap()
        .with_chunk_memory_budget_bytes(1);
        for warm in [false, true] {
            store.reset_pack_read_stats();
            let mut stream = store.open_stream(&digest).await.unwrap().unwrap();
            let mut actual = Vec::new();
            stream.read_to_end(&mut actual).await.unwrap();
            assert_eq!(actual, data);
            let stats = store.pack_read_stats().unwrap();
            assert_eq!(stats.whole_pack_requests, 0);
            assert_eq!(stats.chunk_range_requests == 0, warm);
        }
    }

    #[tokio::test]
    async fn blocked_fetch_seek_and_park_return_all_permits() {
        use object_store::memory::InMemory;
        use object_store::throttle::{ThrottleConfig, ThrottledStore};
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        let objects = Arc::new(ThrottledStore::new(
            InMemory::new(),
            ThrottleConfig::default(),
        ));
        let packed =
            PackedChunks::open_with_cache(objects.clone(), Path::default(), 256 * 1024, 128 * 1024)
                .await
                .unwrap();
        let mut data = vec![0; 2 * 1024 * 1024];
        blake3::Hasher::new()
            .update(b"production-fetch-cancellation")
            .finalize_xof()
            .fill(&mut data);
        let mut chunks = Vec::new();
        for bytes in data.chunks(64 * 1024) {
            let chunk = ChunkMeta {
                digest: ChunkId::new(blake3::hash(bytes).into()),
                size: bytes.len() as u64,
            };
            packed
                .put(
                    chunk.clone(),
                    Bytes::from(zstd::encode_all(bytes, 0).unwrap()),
                )
                .await
                .unwrap();
            chunks.push(chunk);
        }
        packed.flush().await.unwrap();
        drop(packed);
        let packed =
            PackedChunks::open_with_cache(objects.clone(), Path::default(), 256 * 1024, 128 * 1024)
                .await
                .unwrap();
        packed.get(&chunks[0].digest).await.unwrap().unwrap();
        let frozen = packed.freeze_manifest(&chunks).await.unwrap().unwrap();
        let expected = BlobId::new(blake3::hash(&data).into());
        objects.config_mut(|c| c.wait_get_per_call = Duration::from_secs(60));
        let mut input = reader(
            packed.reader(),
            chunks,
            frozen,
            None,
            ByteBudget::new(BUFFER_BYTES),
            expected,
        );
        let mut first = vec![0; 64 * 1024];
        input.read_exact(&mut first).await.unwrap();
        assert_eq!(first, data[..first.len()]);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), input.read_exact(&mut [0]))
                .await
                .is_err()
        );
        input.seek(io::SeekFrom::Start(0)).await.unwrap();
        let mut byte = [0];
        tokio::time::timeout(Duration::from_secs(2), input.read_exact(&mut byte))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(byte[0], data[0]);
        input.park().await;
        tokio::time::timeout(
            Duration::from_secs(2),
            crate::metadata::flush_repository_leases(),
        )
        .await
        .unwrap()
        .unwrap();
        let reservation = tokio::time::timeout(
            Duration::from_secs(2),
            packed.fetch.buffers.reserve(BUFFER_BYTES),
        )
        .await
        .unwrap();
        drop(reservation);
        let permits = tokio::time::timeout(
            Duration::from_secs(2),
            packed.fetch.requests.clone().acquire_many_owned(4),
        )
        .await
        .unwrap()
        .unwrap();
        drop(permits);
        // Keeping the reader alive must not retain reservations, and parking
        // must leave it usable at exactly the previous position.
        objects.config_mut(|c| c.wait_get_per_call = Duration::ZERO);
        let mut rest = Vec::new();
        input.read_to_end(&mut rest).await.unwrap();
        assert_eq!(rest, data[1..]);
    }

    #[test]
    fn cache_budget_boundaries_and_lru() {
        for capacity in [0, 3, 4, 7, 8] {
            let mut cache = Cache::new(capacity);
            cache.insert(id(1), b"one!");
            cache.insert(id(2), b"two!");
            assert!(cache.used <= capacity);
            assert_eq!(cache.get(id(1)).is_some(), capacity == 8);
            assert_eq!(cache.get(id(2)).is_some(), capacity >= 4);
        }
        let mut cache = Cache::new(8);
        cache.insert(id(1), b"one!");
        cache.insert(id(2), b"two!");
        let retained = cache.get(id(1)).unwrap();
        assert_eq!(cache.insert(id(3), b"new!"), 1);
        assert!(cache.get(id(2)).is_none());
        assert_eq!(retained.as_ref(), b"one!");
        assert_eq!(cache.used, 8);
        cache.clock = u64::MAX;
        cache.get(id(1)).unwrap();
        assert_eq!(cache.insert(id(4), b"last"), 1);
        assert!(cache.get(id(3)).is_none());
        assert!(cache.get(id(1)).is_some());
    }

    #[tokio::test]
    async fn cancelled_pump_drops_ownership_before_lease_flush_returns() {
        let owner = Arc::new(());
        let task_owner = owner.clone();
        let (send, receive) = tokio::sync::mpsc::channel(1);
        let (started, running) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _send = send;
            let mut window = WindowTask::spawn(async move {
                let _owner = task_owner;
                started.send(()).unwrap();
                futures::future::pending::<io::Result<Prefetch>>().await
            });
            let _ = window.join().await;
        });
        running.await.unwrap();
        let pump = Pump {
            receive,
            task: Some(task),
        };
        drop(pump);
        crate::metadata::flush_repository_leases().await.unwrap();
        assert_eq!(Arc::strong_count(&owner), 1);
    }
}

#[cfg(test)]
#[path = "tests/seek_replay.rs"]
mod seek_replay;
