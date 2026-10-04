//! A cheap in-memory presence cache for chunk digests.
//!
//! FastCDC deduplication needs a "do I already have this chunk?" check per
//! chunk. On an object store, doing a `HEAD` per chunk throttles writes. The
//! [`ChunkIndex`] remembers chunks known to be present so repeat chunks (within
//! a process, across many blobs) skip the round-trip entirely. It is an
//! optimization for the warm case: a cold index just falls back to a `HEAD`. It
//! must never claim a chunk is present after it is gone, though, or a later
//! write would skip re-uploading a chunk that no longer exists; deletions
//! therefore evict it (see [`ChunkIndex::remove`]). A persistent (e.g.
//! SQLite-backed) index can replace this later without touching callers.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

use crate::digest::ChunkId;

/// An in-memory set of chunk digests known to be present in the store.
#[derive(Clone, Default)]
pub struct ChunkIndex {
    known: Arc<RwLock<HashSet<ChunkId>>>,
    uploading: Arc<Mutex<HashMap<ChunkId, Arc<tokio::sync::Mutex<()>>>>>,
}

/// Exclusive right to check and upload one chunk; see [`ChunkIndex::claim_upload`].
pub(crate) struct UploadClaim {
    index: ChunkIndex,
    digest: ChunkId,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for UploadClaim {
    fn drop(&mut self) {
        drop(self.guard.take());
        let mut uploading = self.index.uploading.lock().unwrap();
        // Waiters clone the lock under this map lock, so a count of one means
        // nobody else wants it.
        if uploading
            .get(&self.digest)
            .is_some_and(|lock| Arc::strong_count(lock) == 1)
        {
            uploading.remove(&self.digest);
        }
    }
}

impl ChunkIndex {
    /// Whether this chunk is known to be present.
    pub(crate) fn contains(&self, digest: &ChunkId) -> bool {
        self.known.read().unwrap().contains(digest)
    }

    /// Record that this chunk is present.
    pub(crate) fn insert(&self, digest: ChunkId) {
        self.known.write().unwrap().insert(digest);
    }

    /// Wait for exclusive use of `digest`, then check and upload it.
    ///
    /// Concurrent writers of one chunk within the process take turns, so the
    /// later ones find it present instead of writing it again. Besides saving
    /// the duplicate write, this avoids two staged uploads of one path, which
    /// Windows can fail with access denied (apache/arrow-rs-object-store#714).
    pub(crate) async fn claim_upload(&self, digest: ChunkId) -> UploadClaim {
        let lock = self
            .uploading
            .lock()
            .unwrap()
            .entry(digest)
            .or_default()
            .clone();
        UploadClaim {
            index: self.clone(),
            digest,
            guard: Some(lock.lock_owned().await),
        }
    }

    /// Forget this chunk (e.g. after it is deleted from the store), so a later
    /// write does not dedup against a cached entry and skip re-uploading a
    /// chunk that is no longer on disk.
    pub(crate) fn remove(&self, digest: &ChunkId) {
        self.known.write().unwrap().remove(digest);
    }
}
