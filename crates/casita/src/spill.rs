//! Traversal state that stays in memory while it is small and spills to
//! bounded local storage when it is not.
//!
//! Closure verification, collection marking, and whole-repository planning all
//! keep two things while they run: the set of objects already visited, and the
//! work still queued. Holding both entirely in memory makes the largest usable
//! graph a function of process memory, which is not what the specifications
//! promise. [`SpillSet`] and [`TraversalQueue`] keep the same structures with
//! the same visit order, but move them into a temporary SQLite-format database
//! once they outgrow their memory budget.
//!
//! Three budgets apply independently:
//!
//! - `max_memory_objects` bounds how many entries a structure holds in memory
//!   before it spills, and thereafter bounds its read and write buffers;
//! - `max_spill_bytes` bounds the aggregate temporary bytes an operation may
//!   occupy, even when it uses several sets and queues;
//! - the caller's `max_traversal_objects` bounds total visited objects, whether
//!   or not anything spilled.
//!
//! Spill state is never repository state. It holds no roots, is never read as
//! authority, and is deleted when the traversal ends, whether it succeeded,
//! failed, or was cancelled. A traversal that dies with its process leaves its
//! files behind; [`SpillArea::sweep_stale`] removes those at repository open,
//! and it takes a lock so a live traversal in another process is never swept.

use std::borrow::Borrow;
use std::collections::{BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::{StreamExt, future::BoxFuture};
use turso::{Builder, Connection, Database, params};

use crate::digest::{BlobId, ChunkId, Digest};
use crate::error::Error;
use crate::object::ObjectKey;

/// Directory name for spill files below a local repository root.
pub(crate) const SPILL_DIRECTORY: &str = "spill";

/// How many entries move between memory and storage per statement batch.
const BATCH: usize = 1024;

/// Bounds for traversal state that may spill to local storage.
///
/// `max_memory_objects` applies per structure. `max_spill_bytes` applies to all
/// spilled structures created for one repository operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpillLimits {
    /// Entries one structure holds in memory before spilling.
    pub max_memory_objects: usize,
    /// Aggregate temporary bytes one operation may occupy.
    pub max_spill_bytes: u64,
}

/// Measured temporary-state use for one repository operation.
///
/// This is deliberately a summary rather than a live handle: callers can
/// record it with a benchmark result after temporary files have been removed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SpillMetrics {
    /// Number of temporary spill databases opened by the operation.
    pub files_opened: u64,
    /// Greatest aggregate temporary footprint observed, including WAL files.
    pub peak_bytes: u64,
}

impl Default for SpillLimits {
    fn default() -> Self {
        Self {
            // Roughly 50 MB of keys before a traversal reaches for storage.
            max_memory_objects: 250_000,
            max_spill_bytes: 64 * 1024 * 1024 * 1024,
        }
    }
}

/// Where spilled traversal state lives, and how large it may grow.
#[derive(Debug, Clone)]
pub(crate) struct SpillArea {
    /// Directory for spill files, or the platform temporary directory.
    directory: Option<PathBuf>,
    limits: SpillLimits,
    budget: Arc<SpillBudget>,
}

/// The shared, operation-wide accounting for temporary traversal files.
#[derive(Debug)]
struct SpillBudget {
    limit: u64,
    used: AtomicU64,
    peak: AtomicU64,
    files_opened: AtomicU64,
}

impl SpillBudget {
    fn new(limit: u64) -> Self {
        Self {
            limit,
            used: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            files_opened: AtomicU64::new(0),
        }
    }

    fn replace_usage(&self, previous: &AtomicU64, next: u64) -> Result<(), Error> {
        loop {
            let old_file = previous.load(Ordering::Acquire);
            let old_total = self.used.load(Ordering::Acquire);
            let next_total = old_total.saturating_sub(old_file).saturating_add(next);
            self.peak.fetch_max(next_total, Ordering::AcqRel);
            if next_total > self.limit {
                return Err(Error::LimitExceeded(format!(
                    "spilled traversal state reached {next_total} bytes across this operation, \
                     limit is {}",
                    self.limit
                )));
            }
            if self
                .used
                .compare_exchange(old_total, next_total, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                previous.store(next, Ordering::Release);
                return Ok(());
            }
        }
    }

    fn release(&self, bytes: u64) {
        self.used.fetch_sub(bytes, Ordering::AcqRel);
    }

    fn metrics(&self) -> SpillMetrics {
        SpillMetrics {
            files_opened: self.files_opened.load(Ordering::Acquire),
            peak_bytes: self.peak.load(Ordering::Acquire),
        }
    }
}

/// Distinguishes concurrent spills within one process.
static SPILL_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
struct SpillOpenPause {
    directory: PathBuf,
    opened: std::sync::mpsc::SyncSender<PathBuf>,
    resume: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
static SPILL_OPEN_PAUSE: std::sync::OnceLock<std::sync::Mutex<Option<SpillOpenPause>>> =
    std::sync::OnceLock::new();

/// Pause exactly the next successful spill open below one selected directory.
/// Scoping matters because Rust tests run concurrently and an unrelated spill
/// must not consume the hook. Repository tests use this to cancel a traversal
/// after it owns a real temporary database, rather than merely after an
/// unrelated asynchronous snapshot read.
#[cfg(test)]
pub(crate) struct SpillOpenPauseHandle {
    opened: std::sync::mpsc::Receiver<PathBuf>,
    resume: Option<std::sync::mpsc::SyncSender<()>>,
}

#[cfg(test)]
impl SpillOpenPauseHandle {
    pub(crate) fn install(directory: impl Into<PathBuf>) -> Self {
        let (opened_tx, opened) = std::sync::mpsc::sync_channel(1);
        let (resume, resume_rx) = std::sync::mpsc::sync_channel(1);
        let slot = SPILL_OPEN_PAUSE.get_or_init(|| std::sync::Mutex::new(None));
        let mut slot = slot.lock().expect("spill pause mutex is not poisoned");
        assert!(slot.is_none(), "only one spill-open pause may be active");
        *slot = Some(SpillOpenPause {
            directory: directory.into(),
            opened: opened_tx,
            resume: resume_rx,
        });
        Self {
            opened,
            resume: Some(resume),
        }
    }

    pub(crate) fn wait_until_open(&self) -> PathBuf {
        self.opened
            .recv()
            .expect("the traversal must report its opened spill database")
    }

    pub(crate) fn resume(mut self) {
        if let Some(resume) = self.resume.take() {
            let _ = resume.send(());
        }
    }
}

#[cfg(test)]
impl Drop for SpillOpenPauseHandle {
    fn drop(&mut self) {
        if let Some(resume) = self.resume.take() {
            let slot = SPILL_OPEN_PAUSE.get_or_init(|| std::sync::Mutex::new(None));
            if let Some(pause) = slot
                .lock()
                .expect("spill pause mutex is not poisoned")
                .take()
            {
                drop(pause);
            } else {
                let _ = resume.send(());
            }
        }
    }
}

#[cfg(test)]
fn pause_after_spill_open(path: &Path) {
    let Some(slot) = SPILL_OPEN_PAUSE.get() else {
        return;
    };
    let pause = {
        let mut slot = slot.lock().expect("spill pause mutex is not poisoned");
        if slot.as_ref().is_some_and(|pause| {
            path.parent()
                .is_some_and(|parent| parent == pause.directory)
        }) {
            slot.take()
        } else {
            None
        }
    };
    if let Some(pause) = pause {
        let _ = pause.opened.send(path.to_path_buf());
        let _ = pause.resume.recv();
    }
}

impl SpillArea {
    pub(crate) fn new(directory: Option<PathBuf>, limits: SpillLimits) -> Self {
        Self {
            directory,
            limits,
            budget: Arc::new(SpillBudget::new(limits.max_spill_bytes)),
        }
    }

    fn directory(&self) -> PathBuf {
        self.directory.clone().unwrap_or_else(std::env::temp_dir)
    }

    pub(crate) fn metrics(&self) -> SpillMetrics {
        self.budget.metrics()
    }

    /// Reserve one spill file name, with its lock held for the caller.
    fn reserve(&self, kind: &str) -> Result<(PathBuf, std::fs::File), Error> {
        let directory = self.directory();
        std::fs::create_dir_all(&directory)?;
        let sequence = SPILL_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let stem = format!("{kind}-{}-{sequence}", std::process::id());
        let path = directory.join(format!("{stem}.sqlite"));
        let lock_path = directory.join(format!("{stem}.lock"));
        let lock = std::fs::File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        // Held for the traversal's lifetime, so a sweeper in another process
        // can tell a live spill from an abandoned one.
        lock.try_lock().map_err(|error| {
            Error::from(format!(
                "spill lock {} is busy: {error}",
                lock_path.display()
            ))
        })?;
        Ok((path, lock))
    }

    /// Remove spill files left behind by processes that are gone.
    ///
    /// A file whose lock is still held belongs to a live traversal, possibly in
    /// another process, and is left alone. This is best effort: a sweep failure
    /// is not a reason to refuse to open a repository.
    pub(crate) fn sweep_stale(directory: impl AsRef<Path>) {
        let Ok(entries) = std::fs::read_dir(directory.as_ref()) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("lock") {
                continue;
            }
            let Ok(lock) = std::fs::File::options().read(true).write(true).open(&path) else {
                continue;
            };
            if lock.try_lock().is_err() {
                continue;
            }
            let _ = lock.unlock();
            drop(lock);
            let _ = std::fs::remove_file(path.with_extension("sqlite"));
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// A key that can be stored in, and read back from, spilled state.
pub(crate) trait SpillKey: Ord + Clone + Send + Sync + 'static {
    /// Bytes that order exactly like the key itself.
    fn encode_spill(&self) -> Vec<u8>;
    fn decode_spill(bytes: &[u8]) -> Result<Self, Error>;
}

impl SpillKey for ObjectKey {
    fn encode_spill(&self) -> Vec<u8> {
        // The canonical encoding is namespace-then-native-id, which sorts the
        // same way `ObjectKey` does, so a spilled set enumerates in the order
        // an in-memory `BTreeSet` would.
        self.encode()
    }

    fn decode_spill(bytes: &[u8]) -> Result<Self, Error> {
        Self::decode(bytes)
            .map_err(|error| Error::from(format!("spilled object key is invalid: {error}")))
    }
}

impl SpillKey for BlobId {
    fn encode_spill(&self) -> Vec<u8> {
        self.digest().as_bytes().to_vec()
    }

    fn decode_spill(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::new(digest_from(bytes)?))
    }
}

impl SpillKey for ChunkId {
    fn encode_spill(&self) -> Vec<u8> {
        self.digest().as_bytes().to_vec()
    }

    fn decode_spill(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::new(digest_from(bytes)?))
    }
}

fn digest_from(bytes: &[u8]) -> Result<Digest, Error> {
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| Error::from("spilled digest is not 32 bytes"))?;
    Ok(Digest::from(bytes))
}

/// A deduplicating set that spills to local storage.
///
/// Insertion order does not matter; enumeration is always in canonical key
/// order, in memory and spilled alike.
pub(crate) struct SpillSet<K: SpillKey> {
    area: SpillArea,
    kind: &'static str,
    /// Entries not yet written to storage. Before spilling this is the whole
    /// set; after spilling it is the write buffer.
    memory: BTreeSet<K>,
    storage: Option<SpillDb>,
    len: usize,
}

impl<K: SpillKey> SpillSet<K> {
    pub(crate) fn new(area: SpillArea, kind: &'static str) -> Self {
        Self {
            area,
            kind,
            memory: BTreeSet::new(),
            storage: None,
            len: 0,
        }
    }

    /// Number of distinct keys the set holds.
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Whether the set has moved to local storage.
    #[cfg(test)]
    pub(crate) fn spilled(&self) -> bool {
        self.storage.is_some()
    }

    /// Insert `key`, reporting whether it was new.
    pub(crate) async fn insert(&mut self, key: K) -> Result<bool, Error> {
        if self.contains(&key).await? {
            return Ok(false);
        }
        self.memory.insert(key);
        self.len += 1;
        if self.memory.len() >= self.area.limits.max_memory_objects {
            self.spill().await?;
        }
        Ok(true)
    }

    /// Insert keys in caller order, reporting only the first insertion of each
    /// key as new. Like `insert`, a failure may leave a partially updated set.
    pub(crate) async fn insert_batch(&mut self, mut keys: &[K]) -> Result<Vec<bool>, Error> {
        let limit = self.area.limits.max_memory_objects.max(1);
        let mut inserted = Vec::with_capacity(keys.len());
        while !keys.is_empty() {
            // A cancelled flush can leave a full buffer. Retry it before
            // accepting more keys, keeping both the buffer and probes bounded.
            if self.memory.len() >= limit {
                self.spill().await?;
            }
            let count = keys.len().min(BATCH).min(limit - self.memory.len());
            let (batch, rest) = keys.split_at(count);
            let present = self.contains_batch(batch).await?;
            // No flush occurs inside this loop, so the memory set also catches
            // duplicates first seen in this batch, even at the spill boundary.
            for (key, present) in batch.iter().zip(present) {
                let new = !present && self.memory.insert(key.clone());
                self.len += usize::from(new);
                inserted.push(new);
            }
            if self.memory.len() >= limit {
                self.spill().await?;
            }
            keys = rest;
        }
        Ok(inserted)
    }

    /// Whether `key` is present.
    pub(crate) async fn contains(&self, key: &K) -> Result<bool, Error> {
        if self.memory.contains(key) {
            return Ok(true);
        }
        let Some(storage) = &self.storage else {
            return Ok(false);
        };
        let encoded = key.encode_spill();
        storage
            .with_connection(move |connection| {
                Box::pin(async move {
                    let mut rows = connection
                        .prepare_cached("SELECT 1 FROM entries WHERE key = ?1")
                        .await?
                        .query(params![encoded.as_slice()])
                        .await?;
                    Ok(rows.next().await?.is_some())
                })
            })
            .await
    }

    /// Test keys in input order, including duplicates, with bounded disk work.
    ///
    /// Each blocking job holds at most one memory buffer's worth of encoded
    /// keys, capped at `BATCH`. Only the returned flags grow with the input.
    pub(crate) async fn contains_batch<Q: Borrow<K> + Sync>(
        &self,
        keys: &[Q],
    ) -> Result<Vec<bool>, Error> {
        let mut keys = keys.iter();
        let Some(storage) = &self.storage else {
            return Ok(keys
                .map(|key| self.memory.contains(Q::borrow(key)))
                .collect());
        };
        let batch_size = self.area.limits.max_memory_objects.clamp(1, BATCH);
        let mut present = Vec::new();
        loop {
            let start = present.len();
            let mut pending = Vec::new();
            for key in keys.by_ref().take(batch_size) {
                let key = Q::borrow(key);
                let found = self.memory.contains(key);
                if !found {
                    pending.push((present.len(), key.encode_spill()));
                }
                present.push(found);
            }
            if present.len() == start {
                return Ok(present);
            }
            if pending.is_empty() {
                continue;
            }
            let found = storage
                .with_connection(move |connection| {
                    Box::pin(async move {
                        let mut statement = connection
                            .prepare_cached("SELECT 1 FROM entries WHERE key = ?1")
                            .await?;
                        let mut found = Vec::new();
                        for (index, key) in pending {
                            let mut rows = statement.query(params![key.as_slice()]).await?;
                            if rows.next().await?.is_some() {
                                found.push(index);
                            }
                        }
                        Ok(found)
                    })
                })
                .await?;
            for index in found {
                present[index] = true;
            }
        }
    }

    /// Visit every key in canonical order.
    #[cfg(test)]
    pub(crate) async fn for_each<F>(&mut self, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(K) -> Result<(), Error>,
    {
        if self.storage.is_none() {
            for key in &self.memory {
                visit(key.clone())?;
            }
            return Ok(());
        }
        // Everything lives in storage once the write buffer is flushed, so one
        // ordered scan reads the whole set without materializing it.
        self.flush().await?;
        let mut after: Option<Vec<u8>> = None;
        loop {
            let batch = self.page(after.clone()).await?;
            if batch.is_empty() {
                return Ok(());
            }
            after = batch.last().cloned();
            for encoded in batch {
                visit(K::decode_spill(&encoded)?)?;
            }
        }
    }

    /// Stream every key in canonical order, consuming the set.
    ///
    /// This is the asynchronous companion to [`SpillSet::for_each`], for
    /// callers whose per-key work is itself asynchronous: deleting a physical
    /// blob, or handing a retained set to a state store row by row.
    pub(crate) fn into_stream(mut self) -> impl futures::Stream<Item = Result<K, Error>> {
        async_stream::try_stream! {
            if self.storage.is_none() {
                for key in std::mem::take(&mut self.memory) {
                    yield key;
                }
                return;
            }
            self.flush().await?;
            let mut after: Option<Vec<u8>> = None;
            loop {
                let batch = self.page(after.clone()).await?;
                if batch.is_empty() {
                    return;
                }
                after = batch.last().cloned();
                for encoded in batch {
                    yield K::decode_spill(&encoded)?;
                }
            }
        }
    }

    /// Stream keys in this set but not `other`, in canonical order.
    ///
    /// Both sets are finished: merge their ordered streams instead of doing
    /// a disk-backed membership lookup for each key. Each stream retains at
    /// most one page of spilled keys and one key is held as lookahead.
    pub(crate) fn into_difference(
        self,
        other: Self,
    ) -> impl futures::Stream<Item = Result<K, Error>> {
        async_stream::try_stream! {
            let mut left = std::pin::pin!(self.into_stream());
            let mut right = std::pin::pin!(other.into_stream());
            let mut next_right = None;
            let mut right_exhausted = false;
            while let Some(key) = left.next().await {
                let key = key?;
                while !right_exhausted
                    && next_right.as_ref().is_none_or(|other| other < &key)
                {
                    next_right = right.next().await.transpose()?;
                    right_exhausted = next_right.is_none();
                }
                if next_right.as_ref() != Some(&key) {
                    yield key;
                }
            }
        }
    }

    /// Stop accepting keys, so the set can be read in pages repeatedly.
    pub(crate) async fn freeze(mut self) -> Result<FrozenSpillSet<K>, Error> {
        self.flush().await?;
        Ok(FrozenSpillSet { inner: self })
    }

    /// One ordered page of spilled keys after `bound`.
    async fn page(&self, bound: Option<Vec<u8>>) -> Result<Vec<Vec<u8>>, Error> {
        self.page_limited(bound, BATCH).await
    }

    /// One ordered page of at most `limit` spilled keys after `bound`.
    async fn page_limited(
        &self,
        bound: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let storage = self.storage.as_ref().expect("the set is spilled");
        let limit = limit as i64;
        storage
            .with_connection(move |connection| {
                Box::pin(async move {
                    let mut rows =
                        match &bound {
                            Some(bound) => connection
                                .prepare_cached(
                                    "SELECT key FROM entries WHERE key > ?1 ORDER BY key LIMIT ?2",
                                )
                                .await?
                                .query(params![bound.as_slice(), limit])
                                .await?,
                            None => {
                                connection
                                    .prepare_cached("SELECT key FROM entries ORDER BY key LIMIT ?1")
                                    .await?
                                    .query(params![limit])
                                    .await?
                            }
                        };
                    let mut batch = Vec::new();
                    while let Some(row) = rows.next().await? {
                        let key: Vec<u8> = row.get(0)?;
                        batch.push(key);
                    }
                    Ok(batch)
                })
            })
            .await
    }

    /// Write the memory buffer out, but only once the set has spilled.
    ///
    /// A set that still fits in memory stays there: reading it back does not
    /// create local storage it never needed.
    async fn flush(&mut self) -> Result<(), Error> {
        if self.storage.is_none() {
            return Ok(());
        }
        self.spill().await
    }

    /// Move the memory buffer into storage, opening it on first use.
    async fn spill(&mut self) -> Result<(), Error> {
        if self.memory.is_empty() {
            return Ok(());
        }
        let pending_entries = self.memory.len();
        if self.storage.is_none() {
            self.storage = Some(SpillDb::open(&self.area, self.kind, SET_SCHEMA).await?);
            tracing::info!(kind = self.kind, "spill set moved to temporary storage");
        }
        let storage = self.storage.as_ref().expect("just opened");
        let pending: Vec<Vec<u8>> = self
            .memory
            .iter()
            .map(|key| key.encode_spill())
            .collect::<Vec<_>>();
        storage
            .with_connection(move |connection| {
                Box::pin(async move {
                    let transaction = connection.transaction().await?;
                    let mut insert = transaction
                        .prepare_cached("INSERT OR IGNORE INTO entries (key) VALUES (?1)")
                        .await?;
                    for key in &pending {
                        insert.execute(params![key.as_slice()]).await?;
                    }
                    drop(insert);
                    transaction.commit().await?;
                    Ok(())
                })
            })
            .await?;
        self.memory.clear();
        tracing::debug!(
            kind = self.kind,
            pending_entries,
            "spill set batch persisted"
        );
        storage.check_size()
    }
}

/// A finished [`SpillSet`], readable in canonical-order pages as often as the
/// caller likes.
///
/// Collection needs exactly this: it counts the marked set, tests membership
/// while classifying stale data, and then hands the same set to a state store
/// that reads it page by page, possibly twice if a commit is retried.
pub(crate) struct FrozenSpillSet<K: SpillKey> {
    inner: SpillSet<K>,
}

impl<K: SpillKey> FrozenSpillSet<K> {
    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether `key` belongs to this finished set.
    pub(crate) async fn contains(&self, key: &K) -> Result<bool, Error> {
        self.inner.contains(key).await
    }

    /// Membership in caller order, sharing connections across bounded batches.
    pub(crate) async fn contains_batch<Q: Borrow<K> + Sync>(
        &self,
        keys: &[Q],
    ) -> Result<Vec<bool>, Error> {
        self.inner.contains_batch(keys).await
    }

    /// At most `limit` keys strictly after `after`, in canonical order.
    pub(crate) async fn page(&self, after: Option<K>, limit: usize) -> Result<Vec<K>, Error> {
        if self.inner.storage.is_none() {
            let range: Box<dyn Iterator<Item = &K>> = match &after {
                Some(after) => Box::new(self.inner.memory.range((
                    std::ops::Bound::Excluded(after.clone()),
                    std::ops::Bound::Unbounded,
                ))),
                None => Box::new(self.inner.memory.iter()),
            };
            return Ok(range.take(limit).cloned().collect());
        }
        let bound = after.map(|key| key.encode_spill());
        self.inner
            .page_limited(bound, limit)
            .await?
            .iter()
            .map(|encoded| K::decode_spill(encoded))
            .collect()
    }
}

#[async_trait::async_trait]
impl crate::metadata::RetainedObjects for FrozenSpillSet<ObjectKey> {
    fn len(&self) -> usize {
        FrozenSpillSet::len(self)
    }

    async fn page(
        &self,
        after: Option<ObjectKey>,
        limit: usize,
    ) -> Result<Vec<ObjectKey>, crate::metadata::MetadataError> {
        FrozenSpillSet::page(self, after, limit)
            .await
            .map_err(|error| crate::metadata::MetadataError::Backend(error.to_string()))
    }

    async fn contains(&self, key: &ObjectKey) -> Result<bool, crate::metadata::MetadataError> {
        FrozenSpillSet::contains(self, key)
            .await
            .map_err(|error| crate::metadata::MetadataError::Backend(error.to_string()))
    }
}

impl<K: SpillKey> std::fmt::Debug for FrozenSpillSet<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrozenSpillSet")
            .field("len", &self.inner.len)
            .field("spilled", &self.inner.storage.is_some())
            .finish()
    }
}

/// One breadth-first traversal step: the object that linked here, and the key
/// to visit.
pub(crate) type TraversalStep = (Option<ObjectKey>, ObjectKey);

/// A first-in first-out work queue that spills to local storage.
///
/// Order is exactly the order an in-memory queue would produce: the head
/// buffer holds the oldest items, storage the middle, and the tail buffer the
/// newest.
pub(crate) struct TraversalQueue {
    area: SpillArea,
    head: VecDeque<TraversalStep>,
    tail: VecDeque<TraversalStep>,
    storage: Option<SpillDb>,
    /// Positions already written, so storage stays strictly ordered.
    next_position: i64,
    /// Positions already read back.
    read_position: i64,
    len: usize,
}

impl TraversalQueue {
    pub(crate) fn new(area: SpillArea) -> Self {
        Self {
            area,
            head: VecDeque::new(),
            tail: VecDeque::new(),
            storage: None,
            next_position: 0,
            read_position: 0,
            len: 0,
        }
    }

    /// Steps still queued, in memory and storage together.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether the queue has moved to local storage.
    #[cfg(test)]
    pub(crate) fn spilled(&self) -> bool {
        self.storage.is_some()
    }

    pub(crate) async fn push(&mut self, step: TraversalStep) -> Result<(), Error> {
        self.tail.push_back(step);
        self.len += 1;
        if self.tail.len() >= self.area.limits.max_memory_objects {
            self.flush().await?;
        }
        Ok(())
    }

    pub(crate) async fn pop(&mut self) -> Result<Option<TraversalStep>, Error> {
        if let Some(step) = self.head.pop_front() {
            self.len -= 1;
            return Ok(Some(step));
        }
        if self.next_position > self.read_position {
            self.refill().await?;
            if let Some(step) = self.head.pop_front() {
                self.len -= 1;
                return Ok(Some(step));
            }
        }
        let step = self.tail.pop_front();
        if step.is_some() {
            self.len -= 1;
        }
        Ok(step)
    }

    async fn flush(&mut self) -> Result<(), Error> {
        if self.tail.is_empty() {
            return Ok(());
        }
        let pending_entries = self.tail.len();
        if self.storage.is_none() {
            self.storage = Some(SpillDb::open(&self.area, "queue", QUEUE_SCHEMA).await?);
            tracing::info!("traversal queue moved to temporary storage");
        }
        let storage = self.storage.as_ref().expect("just opened");
        let mut position = self.next_position;
        let pending: Vec<(i64, Option<Vec<u8>>, Vec<u8>)> = self
            .tail
            .drain(..)
            .map(|(from, key)| {
                let row = (
                    position,
                    from.map(|from| from.encode_spill()),
                    key.encode_spill(),
                );
                position += 1;
                row
            })
            .collect();
        self.next_position = position;
        storage
            .with_connection(move |connection| {
                Box::pin(async move {
                    let transaction = connection.transaction().await?;
                    for (position, from, key) in &pending {
                        match from {
                            Some(from) => {
                                transaction
                                    .execute(
                                        "INSERT INTO queue (position, source, key) \
                                         VALUES (?1, ?2, ?3)",
                                        params![*position, from.as_slice(), key.as_slice()],
                                    )
                                    .await?
                            }
                            None => {
                                transaction
                                    .execute(
                                        "INSERT INTO queue (position, source, key) \
                                         VALUES (?1, NULL, ?2)",
                                        params![*position, key.as_slice()],
                                    )
                                    .await?
                            }
                        };
                    }
                    transaction.commit().await?;
                    Ok(())
                })
            })
            .await?;
        tracing::debug!(pending_entries, "traversal queue batch persisted");
        storage.check_size()
    }

    /// Read the oldest stored steps back into the head buffer.
    async fn refill(&mut self) -> Result<(), Error> {
        let storage = self.storage.as_ref().expect("storage holds queued steps");
        let from_position = self.read_position;
        let rows = storage
            .with_connection(move |connection| {
                Box::pin(async move {
                    let mut rows = connection
                        .query(
                            "SELECT position, source, key FROM queue WHERE position >= ?1 \
                             ORDER BY position LIMIT ?2",
                            params![from_position, BATCH as i64],
                        )
                        .await?;
                    let mut batch = Vec::new();
                    while let Some(row) = rows.next().await? {
                        let position: i64 = row.get(0)?;
                        let source: Option<Vec<u8>> = row.get(1)?;
                        let key: Vec<u8> = row.get(2)?;
                        batch.push((position, source, key));
                    }
                    Ok(batch)
                })
            })
            .await?;
        let Some((last, _, _)) = rows.last() else {
            self.read_position = self.next_position;
            return Ok(());
        };
        let last = *last;
        let loaded_entries = rows.len();
        for (_, source, key) in rows {
            let source = source
                .map(|bytes| ObjectKey::decode_spill(&bytes))
                .transpose()?;
            self.head
                .push_back((source, ObjectKey::decode_spill(&key)?));
        }
        self.read_position = last + 1;
        // Rows already served are dead weight; deleting them keeps the
        // temporary file proportional to the queue, not to the whole walk.
        storage
            .with_connection(move |connection| {
                Box::pin(async move {
                    connection
                        .execute("DELETE FROM queue WHERE position <= ?1", params![last])
                        .await?;
                    Ok(())
                })
            })
            .await?;
        tracing::debug!(loaded_entries, "traversal queue batch refilled");
        Ok(())
    }
}

const SET_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS entries (key BLOB PRIMARY KEY);";
const QUEUE_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS queue (position INTEGER PRIMARY KEY, source BLOB, key BLOB NOT NULL);";

/// One temporary SQLite-format database holding spilled traversal state.
///
/// Fields drop in declaration order, which is exactly the order the files need:
/// the connection closes, then the database, and only then does [`SpillFiles`]
/// unlink anything. Windows refuses to remove a file that is still open.
struct SpillDb {
    connection: Arc<tokio::sync::Mutex<Connection>>,
    _database: Database,
    files: SpillFiles,
    accounting: SpillAccounting,
}

/// Releases this file's contribution to an operation-wide spill budget.
#[derive(Debug)]
struct SpillAccounting {
    budget: Arc<SpillBudget>,
    bytes: AtomicU64,
}

impl SpillAccounting {
    fn new(budget: Arc<SpillBudget>) -> Self {
        Self {
            budget,
            bytes: AtomicU64::new(0),
        }
    }

    fn replace_usage(&self, bytes: u64) -> Result<(), Error> {
        self.budget.replace_usage(&self.bytes, bytes)
    }
}

impl Drop for SpillAccounting {
    fn drop(&mut self) {
        self.budget.release(self.bytes.load(Ordering::Acquire));
    }
}

/// The temporary files one spill owns, removed when the spill ends for any
/// reason: success, error, cancellation, or an unwinding panic.
struct SpillFiles {
    path: PathBuf,
    lock_path: PathBuf,
    lock: Option<std::fs::File>,
}

impl SpillDb {
    /// Create the temporary database on the blocking pool.
    ///
    /// Turso parses and performs file I/O synchronously inside `poll`, so
    /// opening follows the same rule as every statement: off the executor and
    /// off the caller's stack, which a deep traversal may have mostly spent.
    async fn open(area: &SpillArea, kind: &'static str, schema: &str) -> Result<Self, Error> {
        let (path, lock) = area.reserve(kind)?;
        // The guard is built before the database, so an open that fails part
        // way (a full filesystem is the obvious way) still removes what it
        // created. It travels with the open: if the caller is cancelled, the
        // finished open drops it after the database, not before.
        let files = SpillFiles {
            lock_path: path.with_extension("lock"),
            path: path.clone(),
            lock: Some(lock),
        };
        let path_str = path
            .to_str()
            .ok_or_else(|| Error::from(format!("spill path {} is not UTF-8", path.display())))?
            .to_owned();
        let schema = schema.to_owned();
        let (connection, database, files) = tokio::task::spawn_blocking(move || {
            futures::executor::block_on(async move {
                let database = Builder::new_local(&path_str).build().await?;
                let connection = database.connect()?;
                // Spill state is rebuilt from scratch after any interruption,
                // so durability buys nothing and costs an fsync per batch.
                connection
                    .execute_batch("PRAGMA synchronous = OFF;")
                    .await?;
                connection.execute_batch(&schema).await?;
                Ok::<_, Error>((connection, database, files))
            })
        })
        .await??;
        area.budget.files_opened.fetch_add(1, Ordering::AcqRel);
        #[cfg(test)]
        pause_after_spill_open(&path);
        Ok(Self {
            connection: Arc::new(tokio::sync::Mutex::new(connection)),
            _database: database,
            files,
            accounting: SpillAccounting::new(area.budget.clone()),
        })
    }

    /// Run one statement batch on the blocking pool.
    ///
    /// Turso performs its file I/O synchronously inside `poll`, so spilled
    /// operations follow the same rule as repository state: off the executor,
    /// and run to completion even if the caller is cancelled.
    async fn with_connection<T, F>(&self, f: F) -> Result<T, Error>
    where
        F: for<'a> FnOnce(&'a mut Connection) -> BoxFuture<'a, Result<T, Error>> + Send + 'static,
        T: Send + 'static,
    {
        let mut guard = self.connection.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || {
            futures::executor::block_on(async move {
                // Cached preparation skips Turso's dangling-transaction
                // cleanup. An empty batch performs that cleanup without
                // compiling a query, preserving reads after an errored or
                // panicking write on this connection.
                guard.execute_batch("").await?;
                f(&mut guard).await
            })
        })
        .await?
    }

    /// Fail once the temporary file outgrows its budget.
    fn check_size(&self) -> Result<(), Error> {
        let mut bytes = std::fs::metadata(&self.files.path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        // The write-ahead log is part of the temporary footprint.
        if let Ok(wal) = std::fs::metadata(wal_path(&self.files.path)) {
            bytes = bytes.saturating_add(wal.len());
        }
        self.accounting.replace_usage(bytes)
    }
}

fn wal_path(path: &Path) -> PathBuf {
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    PathBuf::from(wal)
}

impl Drop for SpillFiles {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(wal_path(&self.path));
        if let Some(lock) = self.lock.take() {
            // Unlock explicitly: a forked child may hold a copy of the
            // descriptor, and closing it there would not release the lock.
            let _ = lock.unlock();
            drop(lock);
            let _ = std::fs::remove_file(&self.lock_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    fn area(temp: &tempfile::TempDir, max_memory_objects: usize) -> SpillArea {
        SpillArea::new(
            Some(temp.path().to_path_buf()),
            SpillLimits {
                max_memory_objects,
                max_spill_bytes: SpillLimits::default().max_spill_bytes,
            },
        )
    }

    fn key(byte: u8) -> ObjectKey {
        ObjectKey::blob(BlobId::new(Digest::from([byte; 32])))
    }

    /// Every spill file the area currently holds.
    fn files(temp: &tempfile::TempDir) -> Vec<PathBuf> {
        let mut paths: Vec<_> = std::fs::read_dir(temp.path())
            .map(|entries| entries.flatten().map(|entry| entry.path()).collect())
            .unwrap_or_default();
        paths.sort();
        paths
    }

    #[tokio::test]
    async fn batched_insertions_match_sequential_sets_across_flush_boundaries() {
        let keys: Vec<_> = (0u32..2051)
            .map(|index| {
                let mut digest = [0; 32];
                digest[..4].copy_from_slice(&index.to_be_bytes());
                ObjectKey::blob(BlobId::new(Digest::from(digest)))
            })
            .collect();
        for limit in [1, 17, 1023, 1024, 1025, 4096] {
            let temp = tempfile::tempdir().unwrap();
            let mut set = SpillSet::new(area(&temp, limit), "batched");
            let mut expected = BTreeSet::new();
            for key in &keys[..37] {
                assert_eq!(
                    set.insert(key.clone()).await.unwrap(),
                    expected.insert(key.clone())
                );
            }
            assert!(set.insert_batch(&[]).await.unwrap().is_empty());
            let requests: Vec<_> = keys
                .iter()
                .rev()
                .flat_map(|key| [key.clone(), keys[0].clone(), key.clone()])
                .collect();
            for width in [255, 1024, requests.len()] {
                for batch in requests.chunks(width) {
                    let flags: Vec<_> = batch
                        .iter()
                        .map(|key| expected.insert(key.clone()))
                        .collect();
                    assert_eq!(set.insert_batch(batch).await.unwrap(), flags);
                    assert_eq!(set.len(), expected.len());
                    assert!(set.memory.len() < limit);
                }
            }
            let actual: Vec<_> = set.into_stream().try_collect().await.unwrap();
            assert_eq!(actual, expected.into_iter().collect::<Vec<_>>());
            assert!(files(&temp).is_empty());
        }
    }

    #[tokio::test]
    async fn batched_insertions_enforce_spill_budget_and_clean_up() {
        let temp = tempfile::tempdir().unwrap();
        let limits = SpillLimits {
            max_memory_objects: 4,
            max_spill_bytes: 1,
        };
        let area = SpillArea::new(Some(temp.path().to_path_buf()), limits);
        let mut set = SpillSet::new(area, "batched-budget");
        let keys: Vec<_> = (0..32).map(key).collect();
        let error = set.insert_batch(&keys).await.unwrap_err();
        assert!(matches!(error, Error::LimitExceeded(_)), "{error}");
        drop(set);
        assert!(files(&temp).is_empty());
    }

    #[tokio::test]
    async fn cancelled_batch_can_resume_a_full_write_buffer() {
        let temp = tempfile::tempdir().unwrap();
        let mut set = SpillSet::new(area(&temp, 2), "cancelled-batch");
        set.insert_batch(&[key(1), key(2)]).await.unwrap();
        set.insert(key(3)).await.unwrap();
        // Model the full buffer at the start of a flush, then cancel that
        // flush while it waits for the connection. A later batch must retry
        // the write before it can accept more keys.
        let guard = set
            .storage
            .as_ref()
            .unwrap()
            .connection
            .clone()
            .lock_owned()
            .await;
        set.memory.insert(key(4));
        set.len += 1;
        let mut pending = Box::pin(set.spill());
        assert!(futures::poll!(&mut pending).is_pending());
        drop(pending);
        drop(guard);
        assert_eq!(
            set.insert_batch(&[key(5), key(1), key(5)]).await.unwrap(),
            vec![true, false, false]
        );
        assert_eq!(set.len(), 5);
        assert!(set.memory.len() < 2);
        drop(set);
        assert!(files(&temp).is_empty());
    }

    #[tokio::test]
    async fn a_small_set_never_touches_storage() {
        let temp = tempfile::tempdir().unwrap();
        let mut set = SpillSet::new(area(&temp, 1024), "visited");

        assert!(set.insert(key(2)).await.unwrap());
        assert!(set.insert(key(1)).await.unwrap());
        assert!(!set.insert(key(1)).await.unwrap());
        assert_eq!(set.len(), 2);
        assert!(set.contains(&key(1)).await.unwrap());
        assert!(!set.contains(&key(3)).await.unwrap());
        assert!(!set.spilled());
        assert!(files(&temp).is_empty());

        let mut seen = Vec::new();
        set.for_each(|key| {
            seen.push(key);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(seen, vec![key(1), key(2)]);
    }

    #[tokio::test]
    async fn a_set_larger_than_memory_spills_and_still_answers_exactly() {
        let temp = tempfile::tempdir().unwrap();
        let mut set = SpillSet::new(area(&temp, 4), "visited");

        // Inserted out of order, and with repeats, to prove dedup survives the
        // move to storage.
        for byte in [9u8, 3, 7, 1, 5, 3, 9, 2, 8, 4, 6, 0] {
            set.insert(key(byte)).await.unwrap();
        }
        assert!(set.spilled());
        assert_eq!(set.len(), 10);
        assert!(!files(&temp).is_empty());

        for byte in 0..10u8 {
            assert!(set.contains(&key(byte)).await.unwrap(), "missing {byte}");
        }
        assert!(!set.contains(&key(200)).await.unwrap());
        assert!(!set.insert(key(7)).await.unwrap());

        // Enumeration is canonical order, exactly as an in-memory set would be.
        let mut seen = Vec::new();
        set.for_each(|key| {
            seen.push(key);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(seen, (0..10u8).map(key).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn cached_set_statements_rebind_across_flushes_and_pages() {
        let temp = tempfile::tempdir().unwrap();
        let mut set = SpillSet::new(area(&temp, 17), "visited");
        let expected: Vec<_> = (0u32..2051)
            .map(|index| {
                let mut bytes = [0u8; 32];
                bytes[..4].copy_from_slice(&index.to_be_bytes());
                ObjectKey::blob(BlobId::new(Digest::from(bytes)))
            })
            .collect();
        let absent = key(255);
        for item in expected.iter().rev() {
            assert!(set.insert(item.clone()).await.unwrap());
            // Hits leave the one-row result unexhausted. Following them with
            // misses and writes exercises reset and rebinding of cached SQL.
            assert!(set.contains(item).await.unwrap());
            assert!(!set.contains(&absent).await.unwrap());
            assert!(!set.insert(item.clone()).await.unwrap());
        }
        assert_eq!(set.len(), expected.len());
        let frozen = set.freeze().await.unwrap();
        for limit in [1, 127, 1024, 4096] {
            let mut observed = Vec::new();
            let mut after = None;
            loop {
                let page = frozen.page(after, limit).await.unwrap();
                assert!(page.len() <= limit);
                if page.is_empty() {
                    break;
                }
                after = page.last().cloned();
                observed.extend(page);
            }
            assert_eq!(observed, expected);
        }
        drop(frozen);
        assert!(files(&temp).is_empty());
    }

    #[tokio::test]
    async fn streamed_difference_matches_sets_across_pages_and_storage_modes() {
        let numbered = |index: u32| {
            let mut bytes = [0u8; 32];
            bytes[..4].copy_from_slice(&index.to_be_bytes());
            ObjectKey::blob(BlobId::new(Digest::from(bytes)))
        };
        let left: BTreeSet<_> = (0..3077).filter(|i| i % 3 != 0).map(numbered).collect();
        let right: BTreeSet<_> = (0..4111).filter(|i| i % 5 != 0).map(numbered).collect();
        for (left_limit, right_limit) in [(17, 17), (4096, 4096), (17, 4096), (4096, 17)] {
            let temp = tempfile::tempdir().unwrap();
            let mut a = SpillSet::new(area(&temp, left_limit), "left");
            let mut b = SpillSet::new(area(&temp, right_limit), "right");
            for key in left.iter().rev() {
                a.insert(key.clone()).await.unwrap();
            }
            for key in &right {
                b.insert(key.clone()).await.unwrap();
            }
            let observed: Vec<_> = a.into_difference(b).try_collect().await.unwrap();
            assert_eq!(
                observed,
                left.difference(&right).cloned().collect::<Vec<_>>()
            );
            assert!(files(&temp).is_empty());
        }
    }

    #[tokio::test]
    async fn streamed_difference_handles_empty_equal_and_disjoint_sets() {
        for (left, right) in [
            (vec![], vec![1, 2, 3]),
            (vec![1, 2, 3], vec![]),
            (vec![1, 2, 3], vec![1, 2, 3]),
            (vec![3, 4, 5], vec![1, 2]),
            (vec![1, 2], vec![3, 4, 5]),
            (vec![1, 2, 3, 4, 5], vec![1, 3]),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let mut a = SpillSet::new(area(&temp, 2), "left");
            let mut b = SpillSet::new(area(&temp, 2), "right");
            for item in &left {
                a.insert(key(*item)).await.unwrap();
            }
            for item in &right {
                b.insert(key(*item)).await.unwrap();
            }
            let expected: Vec<_> = left
                .into_iter()
                .filter(|item| !right.contains(item))
                .map(key)
                .collect();
            let observed: Vec<_> = a.into_difference(b).try_collect().await.unwrap();
            assert_eq!(observed, expected);
            assert!(files(&temp).is_empty());
        }
    }

    #[tokio::test]
    async fn dropping_a_difference_stream_releases_both_spill_sets() {
        let temp = tempfile::tempdir().unwrap();
        let mut left = SpillSet::new(area(&temp, 2), "left");
        let mut right = SpillSet::new(area(&temp, 2), "right");
        for item in 0..10 {
            left.insert(key(item)).await.unwrap();
            if item % 2 == 0 {
                right.insert(key(item)).await.unwrap();
            }
        }
        {
            let mut difference = std::pin::pin!(left.into_difference(right));
            assert_eq!(difference.next().await.unwrap().unwrap(), key(1));
            assert!(!files(&temp).is_empty());
        }
        assert!(files(&temp).is_empty());
    }

    #[tokio::test]
    async fn batched_membership_preserves_order_across_storage_and_buffer() {
        let expected: Vec<_> = (0u32..2051)
            .map(|index| {
                let mut bytes = [0u8; 32];
                bytes[..4].copy_from_slice(&index.to_be_bytes());
                ObjectKey::blob(BlobId::new(Digest::from(bytes)))
            })
            .collect();
        let queries: Vec<_> = expected
            .iter()
            .rev()
            .flat_map(|item| [item.clone(), key(255), item.clone()])
            .collect();
        let answers: Vec<_> = queries
            .iter()
            .map(|item| expected.binary_search(item).is_ok())
            .collect();
        for limit in [17, 1024, 4096] {
            let temp = tempfile::tempdir().unwrap();
            let mut set = SpillSet::new(area(&temp, limit), "visited");
            assert!(
                set.contains_batch(&[] as &[ObjectKey])
                    .await
                    .unwrap()
                    .is_empty()
            );
            for item in &expected {
                set.insert(item.clone()).await.unwrap();
            }
            assert_eq!(set.spilled(), expected.len() >= limit);
            // Spilled cases have both persisted keys and unflushed entries.
            assert!(!set.memory.is_empty());
            for _ in 0..2 {
                assert_eq!(set.contains_batch(&queries).await.unwrap(), answers);
            }
            assert!(
                set.contains_batch(&[] as &[ObjectKey])
                    .await
                    .unwrap()
                    .is_empty()
            );
            let set = set.freeze().await.unwrap();
            assert_eq!(set.contains_batch(&queries).await.unwrap(), answers);
            assert!(
                set.contains_batch(&[] as &[ObjectKey])
                    .await
                    .unwrap()
                    .is_empty()
            );
            drop(set);
            assert!(files(&temp).is_empty());
        }
    }

    #[tokio::test]
    async fn cached_set_reads_roll_back_an_abandoned_write() {
        let temp = tempfile::tempdir().unwrap();
        let mut set = SpillSet::new(area(&temp, 2), "visited");
        set.insert(key(1)).await.unwrap();
        set.insert(key(2)).await.unwrap();
        assert!(set.contains(&key(1)).await.unwrap());
        assert!(!set.contains(&key(3)).await.unwrap());
        let storage = set.storage.as_ref().unwrap();
        let failure = storage
            .with_connection(|connection| {
                Box::pin(async move {
                    let transaction = connection.transaction().await?;
                    let mut insert = transaction
                        .prepare_cached("INSERT INTO entries (key) VALUES (?1)")
                        .await?;
                    insert.execute(params![key(3).encode_spill()]).await?;
                    // The second insertion fails after a successful write;
                    // dropping the transaction defers rollback in Turso.
                    insert.execute(params![key(3).encode_spill()]).await?;
                    Ok(())
                })
            })
            .await;
        assert!(failure.is_err());
        assert_eq!(
            set.insert_batch(&[key(4), key(1), key(4)]).await.unwrap(),
            vec![true, false, false]
        );
        assert_eq!(
            set.contains_batch(&[key(3), key(1), key(3)]).await.unwrap(),
            vec![false, true, false]
        );
        assert!(!set.contains(&key(3)).await.unwrap());
        assert!(set.contains(&key(1)).await.unwrap());
        assert!(!set.insert(key(4)).await.unwrap());
        assert!(set.insert(key(5)).await.unwrap());
        let frozen = set.freeze().await.unwrap();
        assert_eq!(
            frozen.page(None, 10).await.unwrap(),
            vec![key(1), key(2), key(4), key(5)]
        );
        drop(frozen);
        assert!(files(&temp).is_empty());
    }

    #[tokio::test]
    async fn a_set_that_outgrows_its_spill_budget_fails() {
        let temp = tempfile::tempdir().unwrap();
        let area = SpillArea::new(
            Some(temp.path().to_path_buf()),
            SpillLimits {
                max_memory_objects: 4,
                max_spill_bytes: 1,
            },
        );
        let mut set = SpillSet::new(area, "visited");

        let mut result = Ok(true);
        for byte in 0..32u8 {
            result = set.insert(key(byte)).await;
            if result.is_err() {
                break;
            }
        }
        let error = result.expect_err("a one byte budget cannot hold a spilled set");
        assert!(error.to_string().contains("limit is 1"), "{error}");
    }

    #[test]
    fn one_operation_shares_its_spill_budget_and_releases_it_on_drop() {
        let budget = Arc::new(SpillBudget::new(10));
        let first = SpillAccounting::new(budget.clone());
        let second = SpillAccounting::new(budget.clone());

        first.replace_usage(6).unwrap();
        let error = second
            .replace_usage(5)
            .expect_err("two spill files cannot exceed one operation budget");
        assert!(error.to_string().contains("across this operation"));
        assert_eq!(budget.used.load(Ordering::Acquire), 6);

        drop(first);
        second.replace_usage(5).unwrap();
        assert_eq!(budget.used.load(Ordering::Acquire), 5);
    }

    #[tokio::test]
    async fn a_queue_keeps_first_in_first_out_across_the_spill_boundary() {
        let temp = tempfile::tempdir().unwrap();
        let mut queue = TraversalQueue::new(area(&temp, 4));
        let expected: Vec<TraversalStep> = (0..32u8)
            .map(|byte| {
                let from = if byte == 0 { None } else { Some(key(byte - 1)) };
                (from, key(byte))
            })
            .collect();

        for step in &expected {
            queue.push(step.clone()).await.unwrap();
        }
        assert!(queue.spilled());
        assert_eq!(queue.len(), expected.len());

        let mut popped = Vec::new();
        while let Some(step) = queue.pop().await.unwrap() {
            popped.push(step);
        }
        assert_eq!(popped, expected);
        assert!(queue.is_empty());
    }

    #[tokio::test]
    async fn interleaved_pushes_and_pops_keep_their_order() {
        let temp = tempfile::tempdir().unwrap();
        let mut queue = TraversalQueue::new(area(&temp, 4));
        let mut expected = VecDeque::new();
        let mut popped = Vec::new();

        for byte in 0..40u8 {
            let step = (None, key(byte));
            queue.push(step.clone()).await.unwrap();
            expected.push_back(step);
            if byte % 3 == 0 {
                assert_eq!(queue.pop().await.unwrap(), expected.pop_front());
                popped.push(byte);
            }
        }
        while let Some(step) = queue.pop().await.unwrap() {
            assert_eq!(Some(step), expected.pop_front());
        }
        assert!(expected.is_empty());
        assert!(!popped.is_empty());
    }

    #[tokio::test]
    async fn spill_files_disappear_with_the_traversal() {
        let temp = tempfile::tempdir().unwrap();
        let mut set = SpillSet::new(area(&temp, 2), "visited");
        for byte in 0..8u8 {
            set.insert(key(byte)).await.unwrap();
        }
        assert!(!files(&temp).is_empty());

        drop(set);
        assert!(
            files(&temp).is_empty(),
            "spill state outlived its traversal: {:?}",
            files(&temp)
        );
    }

    #[tokio::test]
    async fn sweeping_removes_abandoned_state_and_spares_live_state() {
        let temp = tempfile::tempdir().unwrap();
        // What a killed process leaves behind: an unlocked pair of files.
        std::fs::write(temp.path().join("visited-1-0.sqlite"), b"stale").unwrap();
        std::fs::write(temp.path().join("visited-1-0.lock"), b"").unwrap();

        let mut live = SpillSet::new(area(&temp, 2), "visited");
        for byte in 0..8u8 {
            live.insert(key(byte)).await.unwrap();
        }
        let live_files = files(&temp);

        SpillArea::sweep_stale(temp.path());

        let remaining = files(&temp);
        assert!(!remaining.contains(&temp.path().join("visited-1-0.sqlite")));
        assert!(!remaining.contains(&temp.path().join("visited-1-0.lock")));
        for path in live_files {
            if path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("visited-1-0")
            {
                continue;
            }
            assert!(
                remaining.contains(&path),
                "swept a live spill file: {path:?}"
            );
        }
        // The live set still answers from storage the sweeper left alone.
        assert!(live.contains(&key(3)).await.unwrap());
    }
}
