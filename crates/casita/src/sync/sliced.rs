//! Sliced payload frames: one blob expressed as literal segments plus copies
//! of byte ranges from blobs the receiver already holds.
//!
//! The sender indexes sampled 1 KiB content-defined chunks of the blobs both
//! peers hold, confirms every candidate by byte comparison, and extends each
//! match in both directions, so one copy token covers the whole run between
//! two edits. A frame with no sources is a whole blob in compressed literal
//! segments, so this is the only payload encoding a peer needs. The receiver
//! resolves copies from its own verified blobs; the caller checks the written
//! bytes against the declared identity before publication.
//!
//! Frame layout, little-endian, self-delimiting:
//!
//! ```text
//! "casita-sliced-payload-v1\0", 32-byte BlobId, u64 plaintext_size, token*
//! source token  = u8 3, 32-byte BlobId          (appends to the source table)
//! literal token = u8 0, u64 plaintext_len, u64 frame_len, zstd frame
//! copy token    = u8 1, u64 source_index, u64 offset, u64 len
//! end token     = u8 2
//! ```
//!
//! Sources are declared as tokens so a sender can stream a large blob in
//! windows and name a source the first time a copy needs it. Literal segments
//! hold at most 1 MiB of plaintext and copies are resolved in bounded pieces,
//! so both peers work in bounded memory regardless of blob size.

use std::collections::HashMap;
use std::io::{self, SeekFrom};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::blob::{BlobReader, BlobStore};
use crate::{BlobId, Digest};

/// Frame magic including its terminating NUL.
pub const SLICED_MAGIC: &[u8] = b"casita-sliced-payload-v1\0";
/// Most distinct source blobs one frame may name.
pub const MAX_SLICE_SOURCES: usize = 4096;
/// Most plaintext bytes in one literal segment.
pub const MAX_LITERAL_SEGMENT: usize = 1024 * 1024;
/// Average discovery chunk; minimum and maximum are half and double.
pub const DISCOVERY_CHUNK_BYTES: usize = 1024;
/// One in this many discovery chunks is indexed, selected by content.
pub const DISCOVERY_SAMPLE: u64 = 4;
/// Plaintext a sender holds while encoding; a copy never crosses a window.
pub const ENCODE_WINDOW: usize = 64 * 1024 * 1024;

const MAX_CANDIDATES: usize = 8;
const READ_WINDOW: usize = 1024 * 1024;
const INDEX_WINDOW: usize = 64 * 1024 * 1024;
const TOKEN_LITERAL: u8 = 0;
const TOKEN_COPY: u8 = 1;
const TOKEN_END: u8 = 2;
const TOKEN_SOURCE: u8 = 3;
const ZSTD_LEVEL: i32 = zstd::DEFAULT_COMPRESSION_LEVEL;

/// Why a sliced frame could not be encoded or decoded.
#[derive(Debug, thiserror::Error)]
pub enum SliceError {
    /// The frame names a source blob the receiver does not hold. The frame
    /// has been consumed completely, so the transport remains usable, but
    /// bytes before the source token may already have reached the sink.
    #[error("sliced payload names source {0} which is not held")]
    MissingSource(BlobId),
    /// The frame violates the format or its declared bounds.
    #[error("invalid sliced payload: {0}")]
    Invalid(String),
    /// Reading the frame or a source, or writing the output, failed.
    #[error(transparent)]
    Io(#[from] io::Error),
}

fn invalid(message: impl Into<String>) -> SliceError {
    SliceError::Invalid(message.into())
}

/// Byte-level access to blobs a peer holds, used by the sender to confirm and
/// extend matches and by the receiver to resolve copies.
#[async_trait]
pub trait SliceSources: Send + Sync {
    /// Whether the blob is held completely.
    async fn has(&self, source: &BlobId) -> io::Result<bool>;

    /// Up to `len` bytes at `offset`; fewer only at the end of the blob. An
    /// absent blob is an error.
    async fn read_range(&self, source: &BlobId, offset: u64, len: usize) -> io::Result<Bytes>;
}

/// [`SliceSources`] over any [`BlobStore`]. A few sources stay open, each
/// with its last decoded window, so candidate checks, match extension, and
/// copies of one region read the backend once rather than decoding a stored
/// chunk for every kilobyte compared.
pub struct BlobSliceSources<B> {
    store: B,
    open: Mutex<Vec<OpenSource>>,
}

struct OpenSource {
    id: BlobId,
    reader: Box<dyn BlobReader>,
    /// Plaintext starting at `window_offset`, once loaded.
    window_offset: u64,
    window: Bytes,
    loaded: bool,
    /// Whether the window reaches the end of the blob.
    ended: bool,
}

const OPEN_SOURCES: usize = 8;
/// Bytes decoded per backend read; ranges are served from this window.
const SOURCE_WINDOW: usize = 4 * 1024 * 1024;

impl<B: BlobStore> BlobSliceSources<B> {
    /// Wrap a store; the caller retains every blob it will name.
    pub fn new(store: B) -> Self {
        Self {
            store,
            open: Mutex::new(Vec::new()),
        }
    }
}

impl OpenSource {
    fn serve(&self, offset: u64, len: usize) -> Option<Bytes> {
        if !self.loaded {
            return None;
        }
        let start = offset.checked_sub(self.window_offset)? as usize;
        if start > self.window.len() {
            return None;
        }
        let end = start.saturating_add(len);
        // A window that reaches the blob end serves a range past it short,
        // exactly as the backend would.
        (end <= self.window.len() || self.ended)
            .then(|| self.window.slice(start..end.min(self.window.len())))
    }
}

#[async_trait]
impl<B: BlobStore> SliceSources for BlobSliceSources<B> {
    async fn has(&self, source: &BlobId) -> io::Result<bool> {
        self.store.has(source).await.map_err(io::Error::other)
    }

    async fn read_range(&self, source: &BlobId, offset: u64, len: usize) -> io::Result<Bytes> {
        let mut open = self.open.lock().await;
        let cached = open.iter().position(|entry| entry.id == *source);
        let mut entry = match cached {
            Some(position) => open.remove(position),
            None => OpenSource {
                id: *source,
                reader: self
                    .store
                    .open_read(source)
                    .await
                    .map_err(io::Error::other)?
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("source {source} is absent"),
                        )
                    })?,
                window_offset: 0,
                window: Bytes::new(),
                loaded: false,
                ended: false,
            },
        };
        let served = match entry.serve(offset, len) {
            Some(bytes) => bytes,
            None => {
                // Align the window so a backward extension that walks off its
                // start still finds most of the previous region cached.
                let window_offset = offset - offset % (SOURCE_WINDOW as u64 / 4);
                let want = SOURCE_WINDOW.max((offset - window_offset) as usize + len);
                entry.reader.seek(SeekFrom::Start(window_offset)).await?;
                let mut buffer = Vec::with_capacity(want);
                (&mut entry.reader)
                    .take(want as u64)
                    .read_to_end(&mut buffer)
                    .await?;
                entry.window_offset = window_offset;
                entry.ended = buffer.len() < want;
                entry.window = Bytes::from(buffer);
                entry.loaded = true;
                entry.serve(offset, len).unwrap_or_default()
            }
        };
        if open.len() == OPEN_SOURCES {
            open.remove(0);
        }
        open.push(entry);
        Ok(served)
    }
}

#[derive(Clone, Copy, Debug)]
struct Location {
    source: u32,
    offset: u64,
    len: u32,
}

/// Sampled discovery index over blobs both peers hold.
pub struct SliceIndex {
    entries: HashMap<u64, Vec<Location>>,
    sources: Vec<BlobId>,
    indexed_bytes: u64,
    max_entries: usize,
}

/// Discovery chunks as `(offset, length, gear)`, where `gear` is the cutter's
/// rolling hash at the chunk end.
fn cut(bytes: &[u8]) -> impl Iterator<Item = (usize, usize, u64)> + '_ {
    fastcdc::v2020::FastCDC::new(
        bytes,
        DISCOVERY_CHUNK_BYTES / 2,
        DISCOVERY_CHUNK_BYTES,
        DISCOVERY_CHUNK_BYTES * 2,
    )
    .map(|chunk| (chunk.offset, chunk.length, chunk.hash))
}

/// The content key of one discovery chunk, or `None` when it is not sampled.
/// Sampling is decided from the cutter's gear hash, which is a function of
/// the chunk content alone, so only sampled chunks are ever hashed. The
/// cut condition zeroes a spread of gear bits, so the bits are mixed first.
fn sample_key(chunk: &[u8], gear: u64) -> Option<u64> {
    let mixed = gear.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 58;
    if !mixed.is_multiple_of(DISCOVERY_SAMPLE) {
        return None;
    }
    let hash = blake3::hash(chunk);
    Some(u64::from_le_bytes(
        hash.as_bytes()[..8].try_into().expect("eight bytes"),
    ))
}

impl SliceIndex {
    /// An empty index holding at most `max_entries` sampled chunks.
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            sources: Vec::new(),
            indexed_bytes: 0,
            max_entries,
        }
    }

    /// Whether the entry limit has been reached; further blobs are ignored.
    pub fn is_full(&self) -> bool {
        self.entries.len() >= self.max_entries
    }

    /// Blobs indexed so far.
    pub fn indexed_blobs(&self) -> usize {
        self.sources.len()
    }

    /// Plaintext bytes indexed so far.
    pub fn indexed_bytes(&self) -> u64 {
        self.indexed_bytes
    }

    /// Sampled chunks indexed so far.
    pub fn entries(&self) -> usize {
        self.entries.len()
    }

    /// Whether the index already covers this blob.
    pub fn contains(&self, id: &BlobId) -> bool {
        self.sources.contains(id)
    }

    fn insert(&mut self, key: u64, location: Location) {
        let candidates = self.entries.entry(key).or_default();
        if candidates.len() == MAX_CANDIDATES {
            candidates.remove(0);
        }
        candidates.push(location);
    }

    /// Index one held blob by streaming it in bounded windows. Returns the
    /// bytes read; a full index or an already indexed blob reads nothing.
    pub async fn index_blob(
        &mut self,
        id: BlobId,
        reader: impl AsyncRead + Unpin,
    ) -> io::Result<u64> {
        self.index_blob_windowed(id, reader, INDEX_WINDOW).await
    }

    async fn index_blob_windowed(
        &mut self,
        id: BlobId,
        mut reader: impl AsyncRead + Unpin,
        window_bytes: usize,
    ) -> io::Result<u64> {
        if self.is_full() || self.contains(&id) || self.sources.len() == u32::MAX as usize {
            return Ok(0);
        }
        let source = self.sources.len() as u32;
        self.sources.push(id);
        let mut window = Vec::with_capacity(window_bytes.min(4 * 1024 * 1024));
        let mut base = 0u64;
        let mut total = 0u64;
        loop {
            let carried = window.len();
            let read = (&mut reader)
                .take((window_bytes - carried) as u64)
                .read_to_end(&mut window)
                .await?;
            total += read as u64;
            let eof = read == 0 || window.len() < window_bytes;
            let mut keep_from = window.len();
            for (offset, len, gear) in cut(&window).collect::<Vec<_>>() {
                if !eof && offset + len == window.len() {
                    // A window boundary is not a content boundary; carry the
                    // tail into the next window instead of indexing it.
                    keep_from = offset;
                    break;
                }
                if let Some(key) = sample_key(&window[offset..offset + len], gear) {
                    self.insert(
                        key,
                        Location {
                            source,
                            offset: base + offset as u64,
                            len: len as u32,
                        },
                    );
                    if self.is_full() {
                        self.indexed_bytes += total;
                        return Ok(total);
                    }
                }
            }
            if eof {
                break;
            }
            base += keep_from as u64;
            window.drain(..keep_from);
        }
        self.indexed_bytes += total;
        Ok(total)
    }
}

/// Accounting for one encoded or decoded frame.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SliceStats {
    /// Copy tokens.
    pub copies: u64,
    /// Literal tokens.
    pub literals: u64,
    /// Plaintext bytes resolved from held blobs.
    pub copy_bytes: u64,
    /// Plaintext bytes carried in literal segments.
    pub literal_bytes: u64,
    /// Encoded frame bytes.
    pub frame_bytes: u64,
    /// Distinct source blobs named.
    pub sources: u64,
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_header(output: &mut Vec<u8>, id: &BlobId, size: u64) {
    output.extend_from_slice(SLICED_MAGIC);
    output.extend_from_slice(id.digest().as_bytes());
    put_u64(output, size);
}

fn put_source(output: &mut Vec<u8>, source: &BlobId, stats: &mut SliceStats) {
    output.push(TOKEN_SOURCE);
    output.extend_from_slice(source.digest().as_bytes());
    stats.sources += 1;
}

fn put_literal(output: &mut Vec<u8>, plain: &[u8], stats: &mut SliceStats) -> io::Result<()> {
    debug_assert!(!plain.is_empty() && plain.len() <= MAX_LITERAL_SEGMENT);
    let frame = crate::compression::compress(plain, ZSTD_LEVEL)?;
    output.push(TOKEN_LITERAL);
    put_u64(output, plain.len() as u64);
    put_u64(output, frame.len() as u64);
    output.extend_from_slice(&frame);
    stats.literals += 1;
    stats.literal_bytes += plain.len() as u64;
    Ok(())
}

fn put_literals(output: &mut Vec<u8>, plain: &[u8], stats: &mut SliceStats) -> io::Result<()> {
    for segment in plain.chunks(MAX_LITERAL_SEGMENT) {
        put_literal(output, segment, stats)?;
    }
    Ok(())
}

fn put_copy(output: &mut Vec<u8>, source: u64, offset: u64, len: u64, stats: &mut SliceStats) {
    output.push(TOKEN_COPY);
    put_u64(output, source);
    put_u64(output, offset);
    put_u64(output, len);
    stats.copies += 1;
    stats.copy_bytes += len;
}

struct Match {
    start: usize,
    end: usize,
    source: u32,
    source_start: u64,
}

/// Grow a confirmed match at `bytes[start..end]` == source at `source_start`
/// backward to `floor` and forward to the end of either side.
async fn extend(
    bytes: &[u8],
    start: usize,
    end: usize,
    floor: usize,
    source_id: &BlobId,
    source_start: u64,
    sources: &dyn SliceSources,
) -> io::Result<(usize, usize, u64)> {
    let mut start = start;
    let mut source_start = source_start;
    while start > floor && source_start > 0 {
        let want = (start - floor).min(READ_WINDOW).min(source_start as usize);
        let window = sources
            .read_range(source_id, source_start - want as u64, want)
            .await?;
        if window.len() != want {
            break;
        }
        let back = common_suffix(&bytes[start - want..start], &window);
        start -= back;
        source_start -= back as u64;
        if back < want {
            break;
        }
    }
    let mut end = end;
    let mut source_end = source_start + (end - start) as u64;
    while end < bytes.len() {
        let want = (bytes.len() - end).min(READ_WINDOW);
        let window = sources.read_range(source_id, source_end, want).await?;
        if window.is_empty() {
            break;
        }
        let forward = common_prefix(&bytes[end..end + window.len()], &window);
        end += forward;
        source_end += forward as u64;
        if forward < want {
            break;
        }
    }
    Ok((start, end, source_start))
}

/// Length of the longest common prefix, compared in blocks so equal runs move
/// at memcmp speed and only the block holding the first difference is
/// scanned byte by byte.
fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    const BLOCK: usize = 4096;
    let len = a.len().min(b.len());
    let mut done = 0;
    while done < len {
        let end = (done + BLOCK).min(len);
        if a[done..end] != b[done..end] {
            return done
                + a[done..end]
                    .iter()
                    .zip(&b[done..end])
                    .take_while(|(x, y)| x == y)
                    .count();
        }
        done = end;
    }
    len
}

/// Length of the longest common suffix of two equally long slices.
fn common_suffix(a: &[u8], b: &[u8]) -> usize {
    const BLOCK: usize = 4096;
    debug_assert_eq!(a.len(), b.len());
    let len = a.len().min(b.len());
    let mut done = 0;
    while done < len {
        let take = (len - done).min(BLOCK);
        let (a_block, b_block) = (
            &a[len - done - take..len - done],
            &b[len - done - take..len - done],
        );
        if a_block != b_block {
            return done
                + a_block
                    .iter()
                    .rev()
                    .zip(b_block.iter().rev())
                    .take_while(|(x, y)| x == y)
                    .count();
        }
        done += take;
    }
    len
}

/// Source blobs named so far in one frame, by index position.
#[derive(Default)]
struct Named {
    ids: Vec<BlobId>,
    by_source: HashMap<u32, u64>,
}

/// Encode one window of plaintext, appending tokens to `frame`.
async fn encode_window(
    bytes: &[u8],
    index: &SliceIndex,
    sources: &dyn SliceSources,
    named: &mut Named,
    frame: &mut Vec<u8>,
    stats: &mut SliceStats,
) -> Result<(), SliceError> {
    let mut matches: Vec<Match> = Vec::new();
    let mut covered = 0usize;
    if !index.entries.is_empty() {
        for (offset, len, gear) in cut(bytes) {
            if offset < covered {
                continue;
            }
            let Some(key) = sample_key(&bytes[offset..offset + len], gear) else {
                continue;
            };
            let Some(candidates) = index.entries.get(&key) else {
                continue;
            };
            let mut best: Option<Match> = None;
            for candidate in candidates.iter().rev() {
                if candidate.len as usize != len {
                    continue;
                }
                let source_id = &index.sources[candidate.source as usize];
                let window = sources.read_range(source_id, candidate.offset, len).await?;
                if window.len() != len || window[..] != bytes[offset..offset + len] {
                    continue;
                }
                let (start, end, source_start) = extend(
                    bytes,
                    offset,
                    offset + len,
                    covered,
                    source_id,
                    candidate.offset,
                    sources,
                )
                .await?;
                if best
                    .as_ref()
                    .is_none_or(|best| end - start > best.end - best.start)
                {
                    best = Some(Match {
                        start,
                        end,
                        source: candidate.source,
                        source_start,
                    });
                }
            }
            if let Some(found) = best {
                covered = found.end;
                matches.push(found);
            }
        }
    }
    let mut position = 0usize;
    for found in &matches {
        if found.start > position {
            put_literals(frame, &bytes[position..found.start], stats)?;
        }
        let source = match named.by_source.get(&found.source) {
            Some(&source) => source,
            None if named.ids.len() < MAX_SLICE_SOURCES => {
                let id = index.sources[found.source as usize];
                named.ids.push(id);
                let position = named.ids.len() as u64 - 1;
                named.by_source.insert(found.source, position);
                put_source(frame, &id, stats);
                position
            }
            None => {
                // The source table is full; carry the run as literals.
                put_literals(frame, &bytes[found.start..found.end], stats)?;
                position = found.end;
                continue;
            }
        };
        put_copy(
            frame,
            source,
            found.source_start,
            (found.end - found.start) as u64,
            stats,
        );
        position = found.end;
    }
    if position < bytes.len() {
        put_literals(frame, &bytes[position..], stats)?;
    }
    Ok(())
}

/// Encode a blob of exactly `size` bytes from `reader` against the index,
/// streaming the frame to `output` one window at a time. Copies are confirmed
/// byte for byte through `sources` before they are emitted; an empty index
/// yields literal segments only.
pub async fn encode_sliced_stream(
    id: BlobId,
    size: u64,
    reader: impl AsyncRead + Unpin,
    index: &SliceIndex,
    sources: &dyn SliceSources,
    output: &mut (impl AsyncWrite + Unpin),
) -> Result<SliceStats, SliceError> {
    encode_sliced_windowed(id, size, reader, index, sources, output, ENCODE_WINDOW).await
}

async fn encode_sliced_windowed(
    id: BlobId,
    size: u64,
    mut reader: impl AsyncRead + Unpin,
    index: &SliceIndex,
    sources: &dyn SliceSources,
    output: &mut (impl AsyncWrite + Unpin),
    window_bytes: usize,
) -> Result<SliceStats, SliceError> {
    let mut stats = SliceStats::default();
    // One frame is always draining to the peer while the next window is read
    // and encoded, so a paced link carries the encoder's cost instead of
    // adding to it. Buffers alternate between the two roles.
    let mut ready = Vec::new();
    put_header(&mut ready, &id, size);
    stats.frame_bytes += ready.len() as u64;
    let mut spare = Vec::new();
    let mut named = Named::default();
    let mut window = vec![0u8; window_bytes.min(size.max(1) as usize)];
    let mut produced = 0u64;
    let mut last = false;
    while !last {
        let mut next = std::mem::take(&mut spare);
        next.clear();
        let encode = async {
            let mut filled = 0;
            while filled < window.len() {
                let read = reader.read(&mut window[filled..]).await?;
                if read == 0 {
                    break;
                }
                filled += read;
            }
            if filled == 0 {
                return Ok::<usize, SliceError>(0);
            }
            if produced + filled as u64 > size {
                return Err(invalid("source stream is longer than its declared size"));
            }
            encode_window(
                &window[..filled],
                index,
                sources,
                &mut named,
                &mut next,
                &mut stats,
            )
            .await?;
            Ok(filled)
        };
        let drain = async {
            output.write_all(&ready).await?;
            Ok::<(), SliceError>(())
        };
        let (filled, ()) = tokio::try_join!(encode, drain)?;
        produced += filled as u64;
        stats.frame_bytes += next.len() as u64;
        last = filled < window.len();
        spare = std::mem::replace(&mut ready, next);
    }
    if produced != size {
        return Err(invalid("source stream ended before its declared size"));
    }
    output.write_all(&ready).await?;
    output.write_all(&[TOKEN_END]).await?;
    stats.frame_bytes += 1;
    Ok(stats)
}

/// Encode a whole blob as literal segments while streaming it, for blobs the
/// sender has no index for. Memory stays at one segment.
pub async fn encode_literal_stream(
    id: BlobId,
    size: u64,
    reader: impl AsyncRead + Unpin,
    output: &mut (impl AsyncWrite + Unpin),
) -> Result<SliceStats, SliceError> {
    let empty = SliceIndex::new(0);
    let sources = NoSources;
    encode_sliced_windowed(
        id,
        size,
        reader,
        &empty,
        &sources,
        output,
        MAX_LITERAL_SEGMENT,
    )
    .await
}

/// A peer that holds nothing: every copy token is a missing source. Used for
/// frames that must be literal only, such as single payload responses.
pub struct NoSources;

#[async_trait]
impl SliceSources for NoSources {
    async fn has(&self, _source: &BlobId) -> io::Result<bool> {
        Ok(false)
    }

    async fn read_range(&self, source: &BlobId, _offset: u64, _len: usize) -> io::Result<Bytes> {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("source {source} is absent"),
        ))
    }
}

/// Encode an in-memory blob against the index into one frame.
pub async fn encode_sliced(
    id: BlobId,
    bytes: &[u8],
    index: &SliceIndex,
    sources: &dyn SliceSources,
) -> Result<(Vec<u8>, SliceStats), SliceError> {
    let mut frame = Vec::new();
    let stats = encode_sliced_stream(
        id,
        bytes.len() as u64,
        std::io::Cursor::new(bytes),
        index,
        sources,
        &mut frame,
    )
    .await?;
    Ok((frame, stats))
}

async fn read_u64(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<u64> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes).await?;
    Ok(u64::from_le_bytes(bytes))
}

async fn read_id(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<BlobId> {
    let mut bytes = [0u8; 32];
    reader.read_exact(&mut bytes).await?;
    Ok(BlobId::new(Digest::from(bytes)))
}

/// Decode one frame from `reader`, writing the plaintext to `sink`. The frame
/// must declare exactly `expected` and `expected_size`; every named source
/// must be held. On [`SliceError::MissingSource`] the whole frame has been
/// read, so a persistent transport stays consistent, but the sink may hold a
/// prefix and must be discarded. The caller verifies the written bytes
/// against `expected` before publication.
pub async fn decode_sliced(
    reader: &mut (impl AsyncRead + Unpin),
    expected: BlobId,
    expected_size: u64,
    sources: &dyn SliceSources,
    sink: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<SliceStats, SliceError> {
    let mut magic = vec![0u8; SLICED_MAGIC.len()];
    reader.read_exact(&mut magic).await?;
    if magic != SLICED_MAGIC {
        return Err(invalid("wrong magic"));
    }
    let id = read_id(reader).await?;
    if id != expected {
        return Err(invalid(format!("frame declares {id}, expected {expected}")));
    }
    let size = read_u64(reader).await?;
    if size != expected_size {
        return Err(invalid(format!(
            "frame declares {size} bytes, expected {expected_size}"
        )));
    }
    let mut stats = SliceStats {
        frame_bytes: (SLICED_MAGIC.len() + 32 + 8) as u64,
        ..SliceStats::default()
    };
    let mut named: Vec<BlobId> = Vec::new();
    let mut produced = 0u64;
    loop {
        let mut kind = [0u8; 1];
        reader.read_exact(&mut kind).await?;
        stats.frame_bytes += 1;
        match kind[0] {
            TOKEN_SOURCE => {
                let source = read_source(reader, named.len(), &mut stats).await?;
                if !sources.has(&source).await? {
                    drain(reader, size, produced, named.len() + 1, &mut stats).await?;
                    return Err(SliceError::MissingSource(source));
                }
                named.push(source);
            }
            TOKEN_LITERAL => {
                let plain = read_literal(reader, size - produced, &mut stats).await?;
                produced += plain.len() as u64;
                sink.write_all(&plain).await?;
            }
            TOKEN_COPY => {
                let (source, offset, len) =
                    read_copy(reader, named.len(), size - produced, &mut stats).await?;
                let mut done = 0u64;
                while done < len {
                    let want = (len - done).min(READ_WINDOW as u64) as usize;
                    let piece = sources
                        .read_range(&named[source], offset + done, want)
                        .await?;
                    if piece.len() != want {
                        return Err(invalid("copy reaches past the end of its source"));
                    }
                    sink.write_all(&piece).await?;
                    done += want as u64;
                }
                produced += len;
            }
            TOKEN_END => break,
            _ => return Err(invalid("unknown token")),
        }
    }
    if produced != size {
        return Err(invalid(format!(
            "frame produced {produced} of {size} declared bytes"
        )));
    }
    Ok(stats)
}

async fn read_source(
    reader: &mut (impl AsyncRead + Unpin),
    named: usize,
    stats: &mut SliceStats,
) -> Result<BlobId, SliceError> {
    if named >= MAX_SLICE_SOURCES {
        return Err(invalid("too many sources"));
    }
    let source = read_id(reader).await?;
    stats.frame_bytes += 32;
    stats.sources += 1;
    Ok(source)
}

async fn read_literal(
    reader: &mut (impl AsyncRead + Unpin),
    remaining: u64,
    stats: &mut SliceStats,
) -> Result<Vec<u8>, SliceError> {
    let plain_len = read_u64(reader).await?;
    if plain_len == 0 || plain_len > MAX_LITERAL_SEGMENT as u64 || plain_len > remaining {
        return Err(invalid("literal length outside its bounds"));
    }
    let plain_len = plain_len as usize;
    let frame_len = read_u64(reader).await?;
    if frame_len == 0 || frame_len > zstd::zstd_safe::compress_bound(plain_len) as u64 {
        return Err(invalid("literal frame length outside its bounds"));
    }
    let mut frame = vec![0u8; frame_len as usize];
    reader.read_exact(&mut frame).await?;
    stats.frame_bytes += 16 + frame_len;
    let plain = crate::compression::decompress(&frame, plain_len)
        .map_err(|error| invalid(format!("literal segment: {error}")))?;
    if plain.len() != plain_len {
        return Err(invalid("literal segment decoded to a different length"));
    }
    stats.literals += 1;
    stats.literal_bytes += plain_len as u64;
    Ok(plain)
}

async fn read_copy(
    reader: &mut (impl AsyncRead + Unpin),
    sources: usize,
    remaining: u64,
    stats: &mut SliceStats,
) -> Result<(usize, u64, u64), SliceError> {
    let source = read_u64(reader).await?;
    let offset = read_u64(reader).await?;
    let len = read_u64(reader).await?;
    stats.frame_bytes += 24;
    if source >= sources as u64 {
        return Err(invalid("copy names a source outside the frame's table"));
    }
    if len == 0 || len > remaining || offset.checked_add(len).is_none() {
        return Err(invalid("copy length outside its bounds"));
    }
    stats.copies += 1;
    stats.copy_bytes += len;
    Ok((source as usize, offset, len))
}

/// Consume the rest of a frame without producing output.
async fn drain(
    reader: &mut (impl AsyncRead + Unpin),
    size: u64,
    mut produced: u64,
    mut sources: usize,
    stats: &mut SliceStats,
) -> Result<(), SliceError> {
    loop {
        let mut kind = [0u8; 1];
        reader.read_exact(&mut kind).await?;
        stats.frame_bytes += 1;
        match kind[0] {
            TOKEN_SOURCE => {
                read_source(reader, sources, stats).await?;
                sources += 1;
            }
            TOKEN_LITERAL => {
                let plain = read_literal(reader, size - produced, stats).await?;
                produced += plain.len() as u64;
            }
            TOKEN_COPY => {
                let (_, _, len) = read_copy(reader, sources, size - produced, stats).await?;
                produced += len;
            }
            TOKEN_END => return Ok(()),
            _ => return Err(invalid("unknown token")),
        }
    }
}

/// Decode arbitrary bytes as a literal-only frame for fuzzing; every outcome
/// is acceptable except a panic or unbounded allocation.
#[cfg(feature = "fuzzing")]
pub fn fuzz_decode(bytes: &[u8]) {
    let expected = BlobId::new(Digest::from([0u8; 32]));
    let mut output = Vec::new();
    let _ = futures::executor::block_on(decode_sliced(
        &mut std::io::Cursor::new(bytes),
        expected,
        bytes.len() as u64,
        &NoSources,
        &mut output,
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryBlobStore;
    use std::sync::Arc;

    fn pseudo_random(seed: u64, len: usize) -> Vec<u8> {
        let mut state = seed;
        let mut output = Vec::with_capacity(len + 8);
        while output.len() < len {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut value = state;
            value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            output.extend_from_slice(&(value ^ (value >> 31)).to_le_bytes());
        }
        output.truncate(len);
        output
    }

    /// A rebuilt store path: the same bytes with a different 32-byte hash
    /// every `spacing` bytes.
    fn rebuilt(base: &[u8], spacing: usize) -> Vec<u8> {
        let mut bytes = base.to_vec();
        let mut position = spacing / 2;
        while position + 32 <= bytes.len() {
            for byte in &mut bytes[position..position + 32] {
                *byte = byte.wrapping_add(1);
            }
            position += spacing;
        }
        bytes
    }

    async fn store_blob(store: &MemoryBlobStore, bytes: &[u8]) -> BlobId {
        let mut writer = store.open_write().await;
        writer.write_all(bytes).await.unwrap();
        writer.close().await.unwrap().0
    }

    fn id_of(bytes: &[u8]) -> BlobId {
        BlobId::new(Digest::from(*blake3::hash(bytes).as_bytes()))
    }

    async fn roundtrip(
        base: &[u8],
        new: &[u8],
        receiver_has_base: bool,
    ) -> Result<(SliceStats, SliceStats, Vec<u8>), SliceError> {
        let sender = MemoryBlobStore::new();
        let base_id = store_blob(&sender, base).await;
        let mut index = SliceIndex::new(1 << 20);
        let reader = sender.open_read(&base_id).await.unwrap().unwrap();
        assert_eq!(
            index.index_blob(base_id, reader).await.unwrap(),
            base.len() as u64
        );
        let sender_sources = BlobSliceSources::new(Arc::new(sender));
        let (frame, sent) = encode_sliced(id_of(new), new, &index, &sender_sources).await?;
        assert_eq!(frame.len() as u64, sent.frame_bytes);

        let receiver = MemoryBlobStore::new();
        if receiver_has_base {
            store_blob(&receiver, base).await;
        }
        let receiver_sources = BlobSliceSources::new(Arc::new(receiver));
        let mut output = Vec::new();
        let mut cursor = std::io::Cursor::new(frame.clone());
        let received = decode_sliced(
            &mut cursor,
            id_of(new),
            new.len() as u64,
            &receiver_sources,
            &mut output,
        )
        .await?;
        assert_eq!(
            cursor.position() as usize,
            frame.len(),
            "frame fully consumed"
        );
        Ok((sent, received, output))
    }

    #[tokio::test]
    async fn rebuilt_blob_becomes_a_few_copies_and_tiny_literals() {
        let base = pseudo_random(1, 3 * 1024 * 1024 + 123);
        let new = rebuilt(&base, 256 * 1024);
        let (sent, received, output) = roundtrip(&base, &new, true).await.unwrap();
        assert_eq!(output, new);
        assert_eq!(sent, received);
        assert_eq!(sent.sources, 1);
        assert_eq!(sent.copies, 13);
        assert_eq!(sent.literal_bytes, 12 * 32);
        assert!(sent.frame_bytes < 2048, "{sent:?}");
    }

    #[tokio::test]
    async fn matches_extend_across_read_windows() {
        let base = pseudo_random(2, 5 * READ_WINDOW);
        let mut new = base.clone();
        new[4 * READ_WINDOW + 500] ^= 0xFF;
        let (sent, _, output) = roundtrip(&base, &new, true).await.unwrap();
        assert_eq!(output, new);
        assert_eq!(sent.copies, 2, "{sent:?}");
        assert_eq!(sent.copy_bytes, new.len() as u64 - 1);
        assert_eq!(sent.literal_bytes, 1);
    }

    #[tokio::test]
    async fn encoding_windows_only_split_copies_at_their_boundaries() {
        let base = pseudo_random(11, 3 * 1024 * 1024 + 100 * 1024);
        let sender = MemoryBlobStore::new();
        let base_id = store_blob(&sender, &base).await;
        let mut index = SliceIndex::new(1 << 20);
        index
            .index_blob(base_id, std::io::Cursor::new(base.clone()))
            .await
            .unwrap();
        let sources = BlobSliceSources::new(Arc::new(sender));
        let mut frame = Vec::new();
        let stats = encode_sliced_windowed(
            base_id,
            base.len() as u64,
            std::io::Cursor::new(base.clone()),
            &index,
            &sources,
            &mut frame,
            1024 * 1024,
        )
        .await
        .unwrap();
        assert_eq!(stats.copies, 4);
        assert_eq!(stats.copy_bytes, base.len() as u64);
        assert_eq!(stats.literal_bytes, 0);
        assert_eq!(stats.sources, 1);
        let mut output = Vec::new();
        decode_sliced(
            &mut std::io::Cursor::new(frame),
            base_id,
            base.len() as u64,
            &sources,
            &mut output,
        )
        .await
        .unwrap();
        assert_eq!(output, base);
    }

    #[tokio::test]
    async fn unrelated_blob_is_literal_only_in_bounded_segments() {
        let base = pseudo_random(3, 64 * 1024);
        let new = pseudo_random(4, 2 * MAX_LITERAL_SEGMENT + 7);
        let (sent, received, output) = roundtrip(&base, &new, false).await.unwrap();
        assert_eq!(output, new);
        assert_eq!(sent.copies, 0);
        assert_eq!(sent.sources, 0);
        assert_eq!(sent.literals, 3);
        assert_eq!(received.literal_bytes, new.len() as u64);
    }

    #[tokio::test]
    async fn empty_blob_roundtrips() {
        let (sent, _, output) = roundtrip(b"base", b"", false).await.unwrap();
        assert!(output.is_empty());
        assert_eq!(sent.literals, 0);
    }

    #[tokio::test]
    async fn missing_source_is_reported_after_consuming_the_frame() {
        let base = pseudo_random(5, 256 * 1024);
        let new = rebuilt(&base, 64 * 1024);
        let error = roundtrip(&base, &new, false).await.unwrap_err();
        assert!(matches!(error, SliceError::MissingSource(id) if id == id_of(&base)));
    }

    #[tokio::test]
    async fn literal_stream_matches_in_memory_encoding() {
        let bytes = pseudo_random(6, MAX_LITERAL_SEGMENT + 1);
        let mut streamed = Vec::new();
        let stats = encode_literal_stream(
            id_of(&bytes),
            bytes.len() as u64,
            std::io::Cursor::new(bytes.clone()),
            &mut streamed,
        )
        .await
        .unwrap();
        assert_eq!(stats.literals, 2);
        assert_eq!(stats.frame_bytes, streamed.len() as u64);
        let index = SliceIndex::new(16);
        let sources = BlobSliceSources::new(MemoryBlobStore::new());
        let (frame, _) = encode_sliced(id_of(&bytes), &bytes, &index, &sources)
            .await
            .unwrap();
        assert_eq!(streamed, frame);
        assert!(matches!(
            encode_literal_stream(
                id_of(&bytes),
                3,
                std::io::Cursor::new(bytes.clone()),
                &mut Vec::new()
            )
            .await,
            Err(SliceError::Invalid(_))
        ));
        assert!(matches!(
            encode_literal_stream(
                id_of(&bytes),
                bytes.len() as u64 + 1,
                std::io::Cursor::new(bytes),
                &mut Vec::new()
            )
            .await,
            Err(SliceError::Invalid(_))
        ));
    }

    async fn decode_bytes(frame: &[u8], id: BlobId, size: u64) -> Result<Vec<u8>, SliceError> {
        let sources = BlobSliceSources::new(MemoryBlobStore::new());
        let mut output = Vec::new();
        decode_sliced(
            &mut std::io::Cursor::new(frame),
            id,
            size,
            &sources,
            &mut output,
        )
        .await?;
        Ok(output)
    }

    #[tokio::test]
    async fn malformed_frames_are_rejected() {
        let bytes = pseudo_random(7, 4096);
        let id = id_of(&bytes);
        let index = SliceIndex::new(16);
        let sources = BlobSliceSources::new(MemoryBlobStore::new());
        let (frame, _) = encode_sliced(id, &bytes, &index, &sources).await.unwrap();
        assert_eq!(decode_bytes(&frame, id, 4096).await.unwrap(), bytes);

        let mut wrong_magic = frame.clone();
        wrong_magic[0] ^= 1;
        assert!(matches!(
            decode_bytes(&wrong_magic, id, 4096).await,
            Err(SliceError::Invalid(_))
        ));
        assert!(matches!(
            decode_bytes(&frame, id_of(b"other"), 4096).await,
            Err(SliceError::Invalid(_))
        ));
        assert!(matches!(
            decode_bytes(&frame, id, 4095).await,
            Err(SliceError::Invalid(_))
        ));

        let mut truncated = frame.clone();
        truncated.pop();
        assert!(matches!(
            decode_bytes(&truncated, id, 4096).await,
            Err(SliceError::Io(_))
        ));

        let header = SLICED_MAGIC.len() + 32 + 8;
        let mut zero_literal = frame.clone();
        zero_literal[header + 1..header + 9].copy_from_slice(&0u64.to_le_bytes());
        assert!(matches!(
            decode_bytes(&zero_literal, id, 4096).await,
            Err(SliceError::Invalid(_))
        ));

        let mut short = frame.clone();
        short[header + 1..header + 9].copy_from_slice(&4095u64.to_le_bytes());
        assert!(matches!(
            decode_bytes(&short, id, 4096).await,
            Err(SliceError::Invalid(_))
        ));

        let mut unknown_token = frame.clone();
        unknown_token[header] = 7;
        assert!(matches!(
            decode_bytes(&unknown_token, id, 4096).await,
            Err(SliceError::Invalid(_))
        ));

        let mut copy_without_source = Vec::new();
        put_header(&mut copy_without_source, &id, 4096);
        put_copy(
            &mut copy_without_source,
            0,
            0,
            4096,
            &mut SliceStats::default(),
        );
        copy_without_source.push(TOKEN_END);
        assert!(matches!(
            decode_bytes(&copy_without_source, id, 4096).await,
            Err(SliceError::Invalid(_))
        ));

        let store = MemoryBlobStore::new();
        let held = store_blob(&store, &bytes).await;
        let mut too_many = Vec::new();
        put_header(&mut too_many, &id, 4096);
        for _ in 0..=MAX_SLICE_SOURCES {
            put_source(&mut too_many, &held, &mut SliceStats::default());
        }
        let mut output = Vec::new();
        let sources = BlobSliceSources::new(store);
        assert!(matches!(
            decode_sliced(
                &mut std::io::Cursor::new(too_many),
                id,
                4096,
                &sources,
                &mut output
            )
            .await,
            Err(SliceError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn copies_past_the_source_end_are_rejected() {
        let base = pseudo_random(8, 8192);
        let store = MemoryBlobStore::new();
        let base_id = store_blob(&store, &base).await;
        let sources = BlobSliceSources::new(store);
        let new = pseudo_random(9, 100);
        let mut frame = Vec::new();
        put_header(&mut frame, &id_of(&new), 100);
        put_source(&mut frame, &base_id, &mut SliceStats::default());
        put_copy(&mut frame, 0, 8192 - 50, 100, &mut SliceStats::default());
        frame.push(TOKEN_END);
        let mut output = Vec::new();
        let error = decode_sliced(
            &mut std::io::Cursor::new(frame),
            id_of(&new),
            100,
            &sources,
            &mut output,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, SliceError::Invalid(_)), "{error}");
    }

    /// A reader that never fills more than a few bytes per poll.
    struct ShortReads(std::io::Cursor<Vec<u8>>);

    impl AsyncRead for ShortReads {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
            output: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            let position = self.0.position() as usize;
            let data = self.0.get_ref();
            let take = output.remaining().min(777).min(data.len() - position);
            output.put_slice(&data[position..position + take]);
            self.0.set_position((position + take) as u64);
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn index_windows_carry_boundary_chunks() {
        // One window, small windows, and short transport reads must index the
        // same chunks, so positions never depend on how bytes arrive.
        let base = pseudo_random(10, 300 * 1024);
        let mut whole = SliceIndex::new(1 << 20);
        whole
            .index_blob(id_of(&base), std::io::Cursor::new(base.clone()))
            .await
            .unwrap();
        let mut windowed = SliceIndex::new(1 << 20);
        windowed
            .index_blob_windowed(
                id_of(&base),
                ShortReads(std::io::Cursor::new(base.clone())),
                64 * 1024,
            )
            .await
            .unwrap();
        assert_eq!(whole.entries(), windowed.entries());
        assert!(whole.entries() > 40, "{}", whole.entries());
        for (key, locations) in &whole.entries {
            let other = &windowed.entries[key];
            assert_eq!(locations.len(), other.len());
            for (a, b) in locations.iter().zip(other) {
                assert_eq!((a.offset, a.len), (b.offset, b.len));
            }
        }
        let mut full = SliceIndex::new(3);
        full.index_blob(id_of(&base), std::io::Cursor::new(base.clone()))
            .await
            .unwrap();
        assert!(full.is_full());
        assert_eq!(
            full.index_blob(id_of(b"more"), std::io::Cursor::new(b"more".to_vec()))
                .await
                .unwrap(),
            0
        );
    }
}
