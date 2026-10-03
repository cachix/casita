use super::*;
use crate::metadata::{DataPin, DataPinLease, MemoryPinStore, PinScope};
use crate::{ChunkId, Digest};
use std::sync::atomic::AtomicBool;
use tokio::sync::Notify;

struct DelayedLedger {
    inner: MemoryPinStore,
    armed: AtomicBool,
    reached: Notify,
    resume: Notify,
}

#[async_trait]
impl crate::metadata::PinStore for DelayedLedger {
    async fn inventory(&self) -> Result<crate::metadata::PinInventory, crate::MetadataError> {
        self.inner.inventory().await
    }
    async fn register(
        &self,
        pin: crate::metadata::DataPin,
    ) -> Result<Option<crate::metadata::PinToken>, crate::MetadataError> {
        self.inner.register(pin).await
    }
    async fn protect(
        &self,
        token: &crate::metadata::PinToken,
        resources: std::collections::BTreeSet<crate::metadata::PinResource>,
    ) -> Result<bool, crate::MetadataError> {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.reached.notify_one();
            self.resume.notified().await;
        }
        self.inner.protect(token, resources).await
    }

    async fn release(&self, token: &crate::metadata::PinToken) -> Result<(), crate::MetadataError> {
        self.inner.release(token).await
    }
    async fn begin_prune(
        &self,
        revision: u64,
    ) -> Result<Option<crate::metadata::PinToken>, crate::MetadataError> {
        self.inner.begin_prune(revision).await
    }
    async fn finish_prune(
        &self,
        token: &crate::metadata::PinToken,
    ) -> Result<(), crate::MetadataError> {
        self.inner.finish_prune(token).await
    }
    async fn claim_deletions(
        &self,
        revision: u64,
        resources: std::collections::BTreeSet<crate::metadata::PinResource>,
    ) -> Result<Option<crate::metadata::PinToken>, crate::MetadataError> {
        self.inner.claim_deletions(revision, resources).await
    }
    async fn finish_deletions(
        &self,
        token: &crate::metadata::PinToken,
    ) -> Result<(), crate::MetadataError> {
        self.inner.finish_deletions(token).await
    }
}

#[tokio::test]
async fn page_flush_polls_uploads_holding_the_pin_gate() {
    let ledger = Arc::new(DelayedLedger {
        inner: MemoryPinStore::default(),
        armed: AtomicBool::new(true),
        reached: Notify::new(),
        resume: Notify::new(),
    });
    let pin = DataPinLease::acquire(
        ledger.clone(),
        DataPin {
            scope: PinScope::Staging,
            catalog: None,
            resources: BTreeSet::new(),
        },
    )
    .await
    .unwrap();
    let store = super::super::ChunkedBlobStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        Path::default(),
        1024,
    );
    use crate::blob::BlobStore;
    let batch = store.begin_pinned_batch(pin.clone()).unwrap();
    let pages = super::super::pages::Pages::from(&store);
    let mut manifest = super::super::pages::ChunkManifest::new(pages);
    let chunk = |i: u64| ChunkMeta {
        digest: ChunkId::new(Digest::hash(&i.to_le_bytes())),
        size: 1,
    };
    for i in 0..64 {
        manifest.push(chunk(i)).await.unwrap();
    }
    let mut uploads = FuturesUnordered::new();
    uploads.push({
        let pin = pin.clone();
        let tail = chunk(65);
        async move {
            pin.protect(BTreeSet::from([PinResource::Chunk(tail.digest)]))
                .await
                .map_err(io::Error::other)?;
            Ok::<_, io::Error>((65, tail))
        }
    });
    // Park a real DataPinLease protection call after it acquires its gate.
    tokio::select! {
        _ = uploads.next() => panic!("the ledger update must remain suspended"),
        _ = ledger.reached.notified() => {}
    }
    // Its I/O is ready, but only polling the owning upload releases the gate.
    ledger.resume.notify_one();
    let mut reordered = BTreeMap::new();
    let mut offset = 64;
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        append_completed(
            &mut manifest,
            &mut reordered,
            &mut offset,
            (64, chunk(64)),
            &mut uploads,
        ),
    )
    .await
    .expect("page flush stopped polling the upload holding its pin gate")
    .unwrap();
    // The final manifest must still be written in source order.
    while let Some(completed) = uploads.next().await {
        append_completed(
            &mut manifest,
            &mut reordered,
            &mut offset,
            completed.unwrap(),
            &mut uploads,
        )
        .await
        .unwrap();
    }
    assert_eq!(offset, 66);
    assert!(reordered.is_empty());
    manifest.finish().await.unwrap();
    drop(uploads);
    drop(batch);
    drop(pin);
    crate::flush_repository_leases().await.unwrap();
    use crate::metadata::PinStore;
    assert!(ledger.inventory().await.unwrap().pins.is_empty());
}
