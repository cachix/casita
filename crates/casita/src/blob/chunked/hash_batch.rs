//! Small CPU jobs over already-admitted chunks. Storage remains per chunk.

use tokio::sync::{OwnedSemaphorePermit, oneshot};

use crate::digest::ChunkId;

const MAX_CHUNKS: usize = 4;
const MAX_BYTES: usize = 1024 * 1024;
/// A lone chunk up to this size costs less to hash than a blocking-pool round
/// trip. Groups flush with one chunk whenever the writer has nothing else to
/// overlap, as at upload concurrency 1 or with a one-chunk memory budget.
pub(super) const INLINE_BYTES: usize = 4 * 1024;

pub(super) struct Hashed {
    // Drop bytes before returning their admission on every cancellation path.
    pub data: Vec<u8>,
    pub digest: ChunkId,
    pub guard: OwnedSemaphorePermit,
}

struct Pending {
    data: Vec<u8>,
    guard: OwnedSemaphorePermit,
    result: oneshot::Sender<Hashed>,
}

#[derive(Default)]
pub(super) struct HashBatch {
    pending: Vec<Pending>,
    bytes: usize,
}

impl HashBatch {
    pub fn push(
        &mut self,
        data: Vec<u8>,
        guard: OwnedSemaphorePermit,
    ) -> oneshot::Receiver<Hashed> {
        if self.bytes.saturating_add(data.len()) > MAX_BYTES {
            self.flush();
        }
        let (result, receiver) = oneshot::channel();
        self.bytes += data.len();
        self.pending.push(Pending {
            data,
            guard,
            result,
        });
        // An oversized chunk was already admitted by the shared byte budget;
        // run it alone rather than imposing an impossible second admission.
        if self.pending.len() == MAX_CHUNKS || self.bytes >= MAX_BYTES {
            self.flush();
        }
        receiver
    }

    pub fn flush(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let mut pending = std::mem::take(&mut self.pending);
        self.bytes = 0;
        if pending.len() == 1 && pending[0].data.len() <= INLINE_BYTES {
            let item = pending.pop().expect("one pending chunk");
            #[cfg(test)]
            super::hash_batch_tests::record_hash_job(std::iter::once(item.data.as_slice()), true);
            let digest = ChunkId::new(blake3::hash(&item.data).into());
            let _ = item.result.send(Hashed {
                data: item.data,
                digest,
                guard: item.guard,
            });
            return;
        }
        // Each receiver observes job failure as a closed channel. The job owns
        // admission until its bytes are released, even if the writer is gone.
        tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            super::hash_batch_tests::record_hash_job(
                pending.iter().map(|p| p.data.as_slice()),
                false,
            );
            for item in pending {
                if item.result.is_closed() {
                    continue;
                }
                let digest = ChunkId::new(blake3::hash(&item.data).into());
                // Sending never waits on storage or another blocking job.
                let _ = item.result.send(Hashed {
                    data: item.data,
                    digest,
                    guard: item.guard,
                });
            }
        });
    }
}
