//! Production publication and collection, with separately owned submitted commits.
use crate::{Faults, Scenario as PinScenario, SeededEntropy, Shared, ledger, serve};
use async_trait::async_trait;
use casita::experimental::*;
use futures::{TryStreamExt, stream, stream::BoxStream};
use serde::Serialize;
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, oneshot};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scenario {
    CancelBeforeApply,
    CancelAfterApply,
    LostCommitResponse,
    StaleMark,
    NetworkBeforeApply,
    NetworkLostAck,
    RestartBeforeCache,
    RestartAfterCache,
}
impl Scenario {
    pub const ALL: [Self; 8] = [
        Self::CancelBeforeApply,
        Self::CancelAfterApply,
        Self::LostCommitResponse,
        Self::StaleMark,
        Self::NetworkBeforeApply,
        Self::NetworkLostAck,
        Self::RestartBeforeCache,
        Self::RestartAfterCache,
    ];
    pub const NETWORK: [Self; 2] = [Self::NetworkBeforeApply, Self::NetworkLostAck];
    pub const RESTARTS: [Self; 2] = [Self::RestartBeforeCache, Self::RestartAfterCache];
    fn restart(self) -> bool {
        matches!(self, Self::RestartBeforeCache | Self::RestartAfterCache)
    }
    fn networked(self) -> bool {
        matches!(self, Self::NetworkBeforeApply | Self::NetworkLostAck) || self.restart()
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::CancelBeforeApply => "cancel-before-apply",
            Self::CancelAfterApply => "cancel-after-apply",
            Self::LostCommitResponse => "lost-commit-response",
            Self::StaleMark => "stale-mark",
            Self::NetworkBeforeApply => "network-cancel-before-apply",
            Self::NetworkLostAck => "network-lost-ack",
            Self::RestartBeforeCache => "network-restart-before-cache",
            Self::RestartAfterCache => "network-restart-after-cache",
        }
    }
}
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct Report {
    pub events: Vec<String>,
    pub event_times_ns: Vec<u128>,
    pub revisions: Vec<String>,
    pub surviving_blobs: Vec<String>,
    pub deleted_blobs: Vec<String>,
    pub caller_cancelled: bool,
    pub pending_bytes_survived: bool,
    pub published_graph_readable: bool,
    pub garbage_collected: bool,
    pub spill_files: u64,
    pub stale_mark_rejected: bool,
    pub ledger_bytes: Vec<u8>,
    pub rpc_requests: usize,
    pub rpc_target_applications: usize,
    pub rpc_duplicate_replies: usize,
    pub rpc_lost_acknowledgements: usize,
    pub rpc_transport_errors: usize,
    pub rpc_server_errors: usize,
    pub rpc_server_epochs: usize,
    pub rpc_restarts: usize,
    pub rpc_discarded_cache_entries: usize,
    pub rpc_journal_recoveries: usize,
    pub rpc_original_result: String,
    pub rpc_recovered_result: String,
}
pub(crate) type Log = Arc<Mutex<Report>>;
pub(crate) fn event(log: &Log, value: impl Into<String>) {
    let mut log = log.lock().unwrap();
    log.events.push(value.into());
    log.event_times_ns
        .push(turmoil::sim_elapsed().unwrap().as_nanos());
}

#[derive(Clone)]
struct SubmittedMetadata {
    inner: MemoryMetadataStore,
    pins: Arc<dyn PinStore>,
    next: Arc<AtomicBool>,
    entered: Arc<Notify>,
    resume: Arc<Notify>,
    scenario: Scenario,
    log: Log,
    seed: u64,
    network: Option<crate::metadata_rpc::Network>,
}
#[async_trait]
impl MetadataStore for SubmittedMetadata {
    fn coordinates_payload_catalog(&self) -> bool {
        false
    }
    fn entropy_source(&self) -> Arc<dyn EntropySource> {
        self.inner.entropy_source()
    }
    async fn try_collection_lease(&self) -> Result<Option<RepositoryLease>, MetadataError> {
        self.inner.try_collection_lease().await
    }
    async fn pin_store(&self) -> Result<Arc<dyn PinStore>, MetadataError> {
        Ok(self.pins.clone())
    }
    async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
        self.inner.snapshot().await
    }
    async fn commit(
        &self,
        expected: &RepositoryRevision,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        if let Some(network) = &self.network {
            return network
                .commit(expected, mutation, self.next.swap(false, Ordering::SeqCst))
                .await;
        }
        if !self.next.swap(false, Ordering::SeqCst) {
            return self.inner.commit(expected, mutation).await;
        }
        let (reply, response) = oneshot::channel();
        let worker = self.clone();
        let expected = *expected;
        event(&self.log, "commit submitted to independent worker");
        // The worker owns the request, including its mutation, independently
        // of both the caller and the future awaiting the acknowledgement.
        tokio::spawn(async move {
            if worker.scenario == Scenario::CancelBeforeApply {
                worker.entered.notify_one();
                worker.resume.notified().await;
            }
            tokio::time::sleep(Duration::from_millis(1 + worker.seed % 17)).await;
            let result = worker.inner.commit(&expected, mutation).await;
            event(
                &worker.log,
                format!("worker applied commit: {}", result.is_ok()),
            );
            if worker.scenario != Scenario::CancelBeforeApply {
                worker.entered.notify_one();
                worker.resume.notified().await;
            }
            tokio::time::sleep(Duration::from_millis(1 + (worker.seed / 17) % 13)).await;
            if worker.scenario == Scenario::LostCommitResponse {
                event(
                    &worker.log,
                    "committed response replaced by transport error",
                );
                let _ = reply.send(Err(MetadataError::Transient(
                    "committed response lost".into(),
                )));
            } else {
                event(&worker.log, "commit acknowledgement delivered");
                let _ = reply.send(result);
            }
        });
        response
            .await
            .map_err(|_| MetadataError::Transient("worker disappeared".into()))?
    }
}

/// Forward safety hooks unchanged, but canonicalize the memory backend's
/// randomized HashMap enumeration before the collector sees it.
#[derive(Clone)]
struct Blobs {
    inner: MemoryBlobStore,
    log: Log,
    pause_listing: Arc<AtomicBool>,
    entered: Arc<Notify>,
    resume: Arc<Notify>,
}
#[async_trait]
impl BlobStore for Blobs {
    fn publication(&self) -> PayloadPublication<'_> {
        self.inner.publication()
    }
    fn write_scope(&self) -> BackendWriteScope {
        self.inner.write_scope()
    }
    fn begin_pinned_batch(&self, pin: DataPinLease) -> Result<BlobBatchGuard, Error> {
        self.inner.begin_pinned_batch(pin)
    }
    async fn has(&self, id: &BlobId) -> Result<bool, Error> {
        self.inner.has(id).await
    }
    async fn open_read(&self, id: &BlobId) -> Result<Option<Box<dyn BlobReader>>, Error> {
        self.inner.open_read(id).await
    }
    async fn open_read_scoped(
        &self,
        id: &BlobId,
        pin: DataPinLease,
        catalog: Option<&[u8]>,
    ) -> Result<Option<Box<dyn BlobReader>>, Error> {
        self.inner.open_read_scoped(id, pin, catalog).await
    }
    async fn open_proof(
        &self,
        id: &BlobId,
        size: u64,
    ) -> Result<Option<Box<dyn BlobStreamReader>>, Error> {
        self.inner.open_proof(id, size).await
    }
    async fn open_proof_scoped(
        &self,
        id: &BlobId,
        size: u64,
        pin: DataPinLease,
        catalog: Option<&[u8]>,
    ) -> Result<Option<Box<dyn BlobStreamReader>>, Error> {
        self.inner.open_proof_scoped(id, size, pin, catalog).await
    }
    async fn open_write(&self) -> Box<dyn BlobWriter> {
        self.inner.open_write().await
    }
}
#[async_trait]
impl BlobGc for Blobs {
    fn list_blobs(&self) -> BoxStream<'_, Result<BlobId, Error>> {
        Box::pin(
            stream::once(async {
                if self.pause_listing.swap(false, Ordering::SeqCst) {
                    event(&self.log, "collector completed old logical mark");
                    self.entered.notify_one();
                    self.resume.notified().await;
                }
                let mut ids = self.inner.list_blobs().try_collect::<Vec<_>>().await?;
                ids.sort();
                Ok::<_, Error>(stream::iter(ids.into_iter().map(Ok)))
            })
            .try_flatten(),
        )
    }
    fn list_chunks(&self) -> BoxStream<'_, Result<ChunkId, Error>> {
        self.inner.list_chunks()
    }
    async fn delete_blob(&self, id: &BlobId) -> Result<(), Error> {
        self.inner.delete_blob(id).await
    }
    async fn delete_chunk(&self, id: &ChunkId) -> Result<(), Error> {
        self.inner.delete_chunk(id).await
    }
    async fn delete_blobs_pinned(
        &self,
        ids: &[BlobId],
        pins: Arc<dyn PinStore>,
        owned: BTreeSet<PinToken>,
        before: bool,
    ) -> Result<usize, Error> {
        let count = self
            .inner
            .delete_blobs_pinned(ids, pins, owned, before)
            .await?;
        let mut log = self.log.lock().unwrap();
        log.deleted_blobs
            .extend(ids.iter().map(ToString::to_string));
        Ok(count)
    }
    async fn delete_chunks_pinned(
        &self,
        ids: &[ChunkId],
        pins: Arc<dyn PinStore>,
        owned: BTreeSet<PinToken>,
    ) -> Result<usize, Error> {
        self.inner.delete_chunks_pinned(ids, pins, owned).await
    }
    async fn finish_deletions_pinned(
        &self,
        force: bool,
        pins: Arc<dyn PinStore>,
        owned: BTreeSet<PinToken>,
        before: bool,
    ) -> Result<(), Error> {
        self.inner
            .finish_deletions_pinned(force, pins, owned, before)
            .await
    }
    async fn finish_collection_pinned(
        &self,
        force: bool,
        pins: Arc<dyn PinStore>,
        owned: BTreeSet<PinToken>,
    ) -> Result<(), Error> {
        self.inner
            .finish_collection_pinned(force, pins, owned)
            .await
    }
    async fn reclaim_metadata_pinned(
        &self,
        pins: Arc<dyn PinStore>,
        owned: BTreeSet<PinToken>,
    ) -> Result<(), Error> {
        self.inner.reclaim_metadata_pinned(pins, owned).await
    }
}

pub fn run(seed: u64, scenario: Scenario, release_pending_pin: bool) -> Result<Report, String> {
    run_with_faults(seed, scenario, release_pending_pin, false)
}
fn run_with_faults(
    seed: u64,
    scenario: Scenario,
    release_pending_pin: bool,
    lose_journal: bool,
) -> Result<Report, String> {
    let storage = Shared::default();
    let log = Log::default();
    let network = if scenario.networked() {
        let server = crate::metadata_rpc::Network::new(
            MemoryMetadataStore::new_with_entropy(Arc::new(SeededEntropy::new(seed, "metadata")))
                .map_err(|error| error.to_string())?,
            log.clone(),
            seed,
            scenario == Scenario::NetworkBeforeApply,
            scenario == Scenario::NetworkLostAck,
        );
        Some(if scenario.restart() {
            server.with_restart(
                if scenario == Scenario::RestartBeforeCache {
                    crate::metadata_rpc::RestartStage::BeforeCache
                } else {
                    crate::metadata_rpc::RestartStage::AfterCache
                },
                lose_journal,
            )
        } else {
            server
        })
    } else {
        None
    };
    let mut sim = crate::harness::simulation(seed, 20, 30);
    let server = storage.clone();
    sim.host("store", move || {
        serve(
            server.clone(),
            PinScenario::PinVsDeletion,
            Faults::default(),
        )
    });
    if let Some(server) = network.clone() {
        sim.host("metadata", move || server.clone().serve());
    }
    let driver = network.clone();
    let output = log.clone();
    sim.client("repository", async move {
        exercise(seed, scenario, release_pending_pin, output, network).await
    });
    if scenario.restart() {
        loop {
            let finished = sim
                .step()
                .map_err(|error| format!("seed={seed} {}: {error}", scenario.name()))?;
            let server = driver.as_ref().unwrap();
            if server.take_restart_request() {
                sim.crash("metadata");
                let discarded = server.discard_volatile_cache();
                {
                    let mut report = log.lock().unwrap();
                    report.rpc_restarts += 1;
                    report.rpc_lost_acknowledgements += 1;
                    report.rpc_discarded_cache_entries = discarded;
                    report
                        .events
                        .push("driver crashed metadata host and discarded volatile cache".into());
                    report.event_times_ns.push(sim.elapsed().as_nanos());
                }
                sim.bounce("metadata");
            }
            if finished {
                break;
            }
        }
    } else {
        sim.run()
            .map_err(|error| format!("seed={seed} {}: {error}", scenario.name()))?;
    }
    let mut result = log.lock().unwrap().clone();
    result.ledger_bytes = storage
        .lock()
        .unwrap()
        .objects
        .get(crate::LEDGER)
        .map(|(bytes, _)| bytes.clone())
        .unwrap_or_default();
    Ok(result)
}

async fn exercise(
    seed: u64,
    scenario: Scenario,
    release_pending_pin: bool,
    output: Log,
    network: Option<crate::metadata_rpc::Network>,
) -> crate::SimResult {
    let pins: Arc<dyn PinStore> = Arc::new(ledger("repository", seed));
    let meta = SubmittedMetadata {
        inner: if let Some(network) = &network {
            network.inner.clone()
        } else {
            MemoryMetadataStore::new_with_entropy(Arc::new(SeededEntropy::new(seed, "metadata")))?
        },
        pins: pins.clone(),
        next: Arc::new(AtomicBool::new(false)),
        entered: network
            .as_ref()
            .map(|n| n.entered.clone())
            .unwrap_or_default(),
        resume: network
            .as_ref()
            .map(|n| n.resume.clone())
            .unwrap_or_default(),
        scenario,
        log: output.clone(),
        seed,
        network: network.clone(),
    };
    let blobs = Blobs {
        inner: MemoryBlobStore::new(),
        log: output.clone(),
        pause_listing: Arc::new(AtomicBool::new(false)),
        entered: meta.entered.clone(),
        resume: meta.resume.clone(),
    };
    let repository = Repository::new(blobs.clone(), meta.clone());
    let collector = Repository::new(blobs.clone(), meta.clone());
    let root = RootName::try_from("live")?;
    let initial = repository.mutation_session().await?;
    let shared = initial.stage_blob(b"shared payload").await?;
    let shared_id = shared.record().payload();
    let shared_key = shared.record().key().clone();
    let mut initial_objects = vec![shared];
    if scenario == Scenario::StaleMark {
        initial_objects.push(initial.stage_blob(b"unreachable logical record").await?);
    }
    initial
        .publish_rooted(initial_objects, root.clone(), shared_key.clone())
        .await?;
    drop(initial);
    flush_repository_leases().await?;
    let garbage = blobs.put_slice(b"unreferenced garbage").await?;
    let mutation = repository.mutation_session().await?;
    let shared = mutation.stage_blob(b"shared payload").await?;
    let new = mutation.stage_blob(b"new payload").await?;
    let new_id = new.record().payload();
    let directory = Directory::try_from_iter([
        (
            PathComponent::try_from("shared")?,
            Node::File {
                digest: shared_id,
                size: 14,
                executable: false,
            },
        ),
        (
            PathComponent::try_from("new")?,
            Node::File {
                digest: new_id,
                size: 11,
                executable: false,
            },
        ),
    ])?;
    let parent_bytes = directory.encode();
    let parent = mutation.stage_directory(&directory).await?;
    let parent_id = parent.record().payload();
    let parent_key = parent.record().key().clone();
    let revision = meta.snapshot().await?.revision().to_string();
    output.lock().unwrap().revisions.push(revision);
    if scenario == Scenario::StaleMark {
        blobs.pause_listing.store(true, Ordering::SeqCst);
        let collect = collector.collect();
        tokio::pin!(collect);
        tokio::select! {
            _ = meta.entered.notified() => {},
            result = &mut collect => return Err(format!("collection completed before stale snapshot: {result:?}").into()),
        }
        mutation
            .publish_rooted(vec![shared, new, parent], root.clone(), parent_key.clone())
            .await?;
        drop(mutation);
        event(&output, "new graph published after old logical mark");
        meta.resume.notify_one();
        let result = collect.await;
        match result {
            Err(RepositoryError::Metadata(MetadataError::StaleRevision { .. })) => {}
            other => {
                return Err(format!("stale mark must reject obsolete revision: {other:?}").into());
            }
        }
        output.lock().unwrap().stale_mark_rejected = true;
        if !output.lock().unwrap().deleted_blobs.is_empty() {
            return Err("stale mark deleted payloads before revision check".into());
        }
        flush_repository_leases().await?;
        let intact = blobs.has(&new_id).await? && blobs.has(&parent_id).await?;
        output.lock().unwrap().pending_bytes_survived = intact;
        event(&output, "obsolete collection rejected before sweep");
    } else {
        meta.next.store(true, Ordering::SeqCst);
        {
            let publish = mutation.publish_rooted(
                vec![shared, new, parent],
                root.clone(),
                parent_key.clone(),
            );
            tokio::pin!(publish);
            tokio::select! {
                _ = meta.entered.notified() => {},
                result = &mut publish => return Err(format!("publication completed before pause: {result:?}").into()),
            }
            event(&output, "caller cancelled after submission");
            output.lock().unwrap().caller_cancelled = true;
        }
        drop(mutation);
        if release_pending_pin {
            // Negative control intentionally breaks ownership until settlement.
            for (token, pin) in pins.inventory().await?.pins {
                if matches!(pin.scope, PinScope::Staging) {
                    pins.release(&token).await?;
                }
            }
            event(
                &output,
                "negative control released pending staging ownership",
            );
        }
        let collected = collector.collect().await?;
        output.lock().unwrap().spill_files += collected.spill.files_opened;
        let pending_ok = blobs.has(&shared_id).await?
            && blobs.has(&new_id).await?
            && blobs.has(&parent_id).await?;
        output.lock().unwrap().pending_bytes_survived = pending_ok;
        event(
            &output,
            format!("GC while acknowledgement pending: bytes retained={pending_ok}"),
        );
        {
            let drain = flush_repository_leases();
            tokio::pin!(drain);
            if futures::poll!(&mut drain).is_ready() {
                return Err("cancelled publication stopped tracking before acknowledgement".into());
            }
        }
        if scenario.restart() {
            network.as_ref().unwrap().request_restart();
        } else {
            meta.resume.notify_one();
        }
        // Drain even the negative control before reporting its checker verdict.
        flush_repository_leases().await?;
        let revision = meta.snapshot().await?.revision().to_string();
        output.lock().unwrap().revisions.push(revision);
        if !pending_ok {
            return Err("pending publication protection lost".into());
        }
    }
    let snapshot = meta.snapshot().await?;
    if snapshot.root(&root).await?.as_ref() != Some(&parent_key) {
        return Err("late commit did not publish expected root".into());
    }
    let collected = collector.collect().await?;
    output.lock().unwrap().spill_files += collected.spill.files_opened;
    for (key, id, expected) in [
        (shared_key, shared_id, b"shared payload".as_slice()),
        (ObjectKey::blob(new_id), new_id, b"new payload".as_slice()),
    ] {
        if meta.snapshot().await?.object(&key).await?.is_none()
            || blobs.read_to_vec(&id).await?.as_deref() != Some(expected)
        {
            return Err("published graph has missing bytes or records".into());
        }
    }
    if blobs.read_to_vec(&parent_id).await?.as_deref() != Some(parent_bytes.as_slice()) {
        return Err("published directory bytes differ from expected graph".into());
    }
    let hold = repository.retention_hold().await?;
    if hold.open_payload(&parent_key).await?.is_none() {
        return Err("published root unreadable".into());
    }
    drop(hold);
    flush_repository_leases().await?;
    let inventory = pins.inventory().await?;
    if !inventory.pins.is_empty()
        || !inventory.deletions.is_empty()
        || inventory.collector.is_some()
        || inventory.logical_prune.is_some()
    {
        return Err("settled publication or GC leaked ownership".into());
    }
    let mut ids = blobs.list_blobs().try_collect::<Vec<_>>().await?;
    ids.sort();
    let garbage_collected = !blobs.has(&garbage).await?;
    if let Some(network) = &network {
        if network.pending_commands() != 0 {
            return Err("network registry leaked a submitted command".into());
        }
        let report = output.lock().unwrap();
        if scenario.restart()
            && (report.rpc_original_result.is_empty()
                || report.rpc_recovered_result != report.rpc_original_result
                || report.rpc_journal_recoveries != 1)
        {
            return Err("restart recovery lost original commit result".into());
        }
        if scenario.restart() {
            let reply: serde_json::Value = serde_json::from_str(&report.rpc_recovered_result)?;
            if reply["Applied"]["revision"].as_str() != report.revisions.last().map(String::as_str)
            {
                return Err("restart replay changed committed metadata revision".into());
            }
        }
        if scenario.restart()
            && (report.rpc_restarts != 1
                || report.rpc_server_epochs != 2
                || report.rpc_transport_errors != 1
                || report.rpc_duplicate_replies != 1
                || report.rpc_discarded_cache_entries
                    != if scenario == Scenario::RestartBeforeCache {
                        1
                    } else {
                        2
                    })
        {
            return Err("restart and fresh-cache recovery oracle was vacuous".into());
        }
        if report.rpc_server_errors != 0 || report.rpc_target_applications != 1 {
            return Err("networked publication was not applied exactly once".into());
        }
        if scenario == Scenario::NetworkLostAck
            && (report.rpc_lost_acknowledgements != 1
                || report.rpc_transport_errors != 1
                || report.rpc_duplicate_replies != 1)
        {
            return Err("lost acknowledgement recovery oracle was vacuous".into());
        }
    }
    let mut report = output.lock().unwrap();
    report.published_graph_readable = true;
    report.garbage_collected = garbage_collected;
    report.surviving_blobs = ids.iter().map(ToString::to_string).collect();
    if !garbage_collected || report.spill_files != 0 {
        return Err("GC oracle or no-filesystem bound failed".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn publication_gc_corpus_replays() {
        for seed in 0..32 {
            for scenario in Scenario::ALL {
                let first = run(seed, scenario, false).unwrap();
                let second = run(seed, scenario, false).unwrap();
                assert_eq!(first, second, "seed={seed}, {}", scenario.name());
                assert!(
                    first.pending_bytes_survived
                        && first.published_graph_readable
                        && first.garbage_collected
                );
            }
        }
    }
    #[test]
    fn restart_checker_rejects_missing_durable_result() {
        let error = run_with_faults(7, Scenario::RestartBeforeCache, false, true)
            .expect_err("checker accepted metadata persisted without its result journal");
        assert!(
            error.contains("restart recovery lost original commit result"),
            "{error}"
        );
    }
    #[test]
    fn network_checker_rejects_early_ownership_release() {
        let error = run(7, Scenario::NetworkBeforeApply, true)
            .expect_err("checker accepted early network pin release");
        assert!(
            error.contains("pending publication protection lost"),
            "{error}"
        );
    }
    #[test]
    fn checker_rejects_early_ownership_release() {
        let error =
            run(7, Scenario::CancelBeforeApply, true).expect_err("checker accepted early release");
        assert!(
            error.contains("pending publication protection lost"),
            "{error}"
        );
    }
}
