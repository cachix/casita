//! Shared production chunks and independently owned DELETE requests.
use crate::{Faults, Scenario as PinScenario, SeededEntropy, Shared, ledger, serve};
use async_trait::async_trait;
use bytes::Bytes;
use casita::experimental::*;
use futures::{StreamExt, TryStreamExt, stream, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, oneshot};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scenario {
    DeleteBeforeApply,
    DeleteAfterApply,
    WritersBeforeApply,
    WritersAfterApply,
    CancelWriterBeforeApply,
    CancelWriterAfterApply,
}
impl Scenario {
    pub const ALL: [Self; 6] = [
        Self::DeleteBeforeApply,
        Self::DeleteAfterApply,
        Self::WritersBeforeApply,
        Self::WritersAfterApply,
        Self::CancelWriterBeforeApply,
        Self::CancelWriterAfterApply,
    ];
    pub const WRITERS: [Self; 2] = [Self::WritersBeforeApply, Self::WritersAfterApply];
    pub const CANCELLATIONS: [Self; 2] =
        [Self::CancelWriterBeforeApply, Self::CancelWriterAfterApply];
    pub fn name(self) -> &'static str {
        match self {
            Self::DeleteBeforeApply => "chunk-delete-before-apply",
            Self::DeleteAfterApply => "chunk-delete-after-apply",
            Self::WritersBeforeApply => "chunk-writers-before-apply",
            Self::WritersAfterApply => "chunk-writers-after-apply",
            Self::CancelWriterBeforeApply => "chunk-cancel-writer-before-apply",
            Self::CancelWriterAfterApply => "chunk-cancel-writer-after-apply",
        }
    }
    fn writer_count(self) -> usize {
        match self {
            Self::DeleteBeforeApply | Self::DeleteAfterApply => 1,
            Self::WritersBeforeApply
            | Self::WritersAfterApply
            | Self::CancelWriterBeforeApply
            | Self::CancelWriterAfterApply => 3,
        }
    }
    fn before_apply(self) -> bool {
        matches!(
            self,
            Self::DeleteBeforeApply | Self::WritersBeforeApply | Self::CancelWriterBeforeApply
        )
    }
    fn cancellation(self) -> bool {
        matches!(
            self,
            Self::CancelWriterBeforeApply | Self::CancelWriterAfterApply
        )
    }
    fn publishes(self, index: usize) -> bool {
        !self.cancellation() || index != 0
    }
    fn publishing_count(self) -> usize {
        self.writer_count() - usize::from(self.cancellation())
    }
    fn writer_root(self, index: usize) -> String {
        if self.writer_count() == 1 {
            "B".into()
        } else {
            format!("B/{index}")
        }
    }
}
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct Report {
    pub events: Vec<(String, String, u128)>,
    pub shared_chunks: Vec<String>,
    pub unique_chunks: Vec<String>,
    pub deleted_paths: Vec<String>,
    pub restored_paths: Vec<String>,
    pub writer_waited: bool,
    pub collector_cancelled: bool,
    pub roots_readable: bool,
    pub spill_files: u64,
    pub writers_started: Vec<String>,
    pub writers_admitted: Vec<String>,
    pub writers_published: Vec<String>,
    pub first_commit_revisions: Vec<String>,
    pub stale_revision_conflicts: usize,
    pub overlapping_staging_pins: usize,
    pub cancelled_writers: Vec<String>,
    pub cancelled_pin_released: bool,
    pub surviving_staging_pins: usize,
    pub collection_with_surviving_pins: bool,
    pub partial_restore_preserved: Vec<String>,
}
type Log = Arc<Mutex<Report>>;
fn event(log: &Log, action: &str, path: &Path) {
    log.lock().unwrap().events.push((
        action.into(),
        path.to_string(),
        turmoil::sim_elapsed().unwrap().as_nanos(),
    ));
}
fn chunk_path(id: ChunkId) -> Path {
    let hex = id.digest().to_hex();
    format!("payload/chunks/b3/{}/{hex}", &hex[..2]).into()
}
fn error(value: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> object_store::Error {
    object_store::Error::Generic {
        store: "chunk-simulation",
        source: value.into(),
    }
}

struct Fixture {
    a: BlobId,
    b: BlobId,
    a_bytes: Vec<u8>,
    b_bytes: Vec<u8>,
    objects: Vec<(Path, Bytes)>,
    b_paths: BTreeSet<Path>,
    shared: BTreeSet<ChunkId>,
    unique: BTreeSet<ChunkId>,
    b_chunks: BTreeSet<ChunkId>,
}
fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        // Hashing/compression use the real blocking pool here, outside Turmoil.
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let objects = Arc::new(InMemory::new());
                let blobs = ChunkedBlobStore::new(objects.clone(), "payload".into(), 1024);
                fn data(label: &[u8], size: usize) -> Vec<u8> {
                    let mut bytes = vec![0; size];
                    blake3::Hasher::new()
                        .update(label)
                        .finalize_xof()
                        .fill(&mut bytes);
                    bytes
                }
                let prefix = data(b"casita shared physical prefix", 8192);
                let mut a_bytes = prefix.clone();
                a_bytes.extend(data(b"casita unique A tail", 4096));
                let mut b_bytes = prefix;
                b_bytes.extend(data(b"casita unique B tail", 4096));
                let a = blobs.put_slice(&a_bytes).await.unwrap();
                let b = blobs.put_slice(&b_bytes).await.unwrap();
                let a_chunks: BTreeSet<_> = blobs
                    .chunks(&a)
                    .await
                    .unwrap()
                    .unwrap()
                    .into_iter()
                    .map(|c| c.digest)
                    .collect();
                let b_chunks: BTreeSet<_> = blobs
                    .chunks(&b)
                    .await
                    .unwrap()
                    .unwrap()
                    .into_iter()
                    .map(|c| c.digest)
                    .collect();
                let shared = a_chunks
                    .intersection(&b_chunks)
                    .copied()
                    .collect::<BTreeSet<_>>();
                let unique = b_chunks
                    .difference(&a_chunks)
                    .copied()
                    .collect::<BTreeSet<_>>();
                assert!(
                    shared.len() >= 2 && unique.len() >= 2,
                    "fixture must exercise genuine partial chunk sharing"
                );
                let b_hex = b.digest().to_hex();
                let b_chunk_paths: BTreeSet<_> =
                    b_chunks.iter().map(|id| chunk_path(*id)).collect();
                let mut metas = objects.list(None).try_collect::<Vec<_>>().await.unwrap();
                metas.sort_by(|a, b| a.location.cmp(&b.location));
                let mut stored = Vec::new();
                let mut b_paths = BTreeSet::new();
                for meta in metas {
                    if b_chunk_paths.contains(&meta.location)
                        || meta.location.as_ref().ends_with(&b_hex)
                    {
                        b_paths.insert(meta.location.clone());
                    }
                    let bytes = objects
                        .get(&meta.location)
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap();
                    stored.push((meta.location, bytes));
                }
                Fixture {
                    a,
                    b,
                    a_bytes,
                    b_bytes,
                    objects: stored,
                    b_paths,
                    shared,
                    unique,
                    b_chunks,
                }
            })
    })
}

#[derive(Clone)]
struct Store {
    inner: Arc<InMemory>,
    log: Log,
    target: Path,
    scenario: Scenario,
    seed: u64,
    armed: Arc<AtomicBool>,
    entered: Arc<Notify>,
    resume: Arc<Notify>,
}
impl fmt::Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DelayedChunkStore")
    }
}
impl fmt::Display for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("delayed-chunks")
    }
}
impl Store {
    async fn delete_owned(&self, path: Path) -> object_store::Result<Path> {
        let (reply, response) = oneshot::channel();
        let worker = self.clone();
        event(&self.log, "delete-submitted", &path);
        tokio::spawn(async move {
            let pause = path == worker.target && worker.armed.swap(false, Ordering::SeqCst);
            if pause && worker.scenario.before_apply() {
                worker.entered.notify_one();
                worker.resume.notified().await;
            }
            tokio::time::sleep(Duration::from_millis(1 + worker.seed % 17)).await;
            let result = worker.inner.delete(&path).await;
            if result.is_ok() {
                event(&worker.log, "delete-applied", &path);
                worker
                    .log
                    .lock()
                    .unwrap()
                    .deleted_paths
                    .push(path.to_string());
            }
            if pause && !worker.scenario.before_apply() {
                worker.entered.notify_one();
                worker.resume.notified().await;
            }
            tokio::time::sleep(Duration::from_millis(1 + (worker.seed / 17) % 13)).await;
            event(&worker.log, "delete-acknowledged", &path);
            let _ = reply.send(result.map(|_| path));
        });
        response.await.map_err(error)?
    }
}
#[async_trait]
impl ObjectStore for Store {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        event(&self.log, "restore-submitted", path);
        tokio::time::sleep(Duration::from_millis(1 + self.seed % 7)).await;
        let result = self.inner.put_opts(path, payload, options).await?;
        event(&self.log, "restore-applied", path);
        self.log
            .lock()
            .unwrap()
            .restored_paths
            .push(path.to_string());
        Ok(result)
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let store = self.clone();
        Box::pin(paths.then(move |path| {
            let store = store.clone();
            async move { store.delete_owned(path?).await }
        }))
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner = self.inner.clone();
        let prefix = prefix.cloned();
        Box::pin(
            stream::once(async move {
                let mut metas = inner.list(prefix.as_ref()).try_collect::<Vec<_>>().await?;
                metas.sort_by(|a, b| a.location.cmp(&b.location));
                Ok::<_, object_store::Error>(stream::iter(metas.into_iter().map(Ok)))
            })
            .try_flatten(),
        )
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
// One waiter per channel avoids simultaneous watch-channel wakeups. The last
// arrival releases all waiters in a stable order; commit application additionally
// uses seed-derived rank delays while every caller retains its old revision.
struct Gate {
    waiting: Mutex<BTreeMap<usize, oneshot::Sender<()>>>,
    participants: usize,
}
impl Gate {
    fn new(participants: usize) -> Self {
        Self {
            waiting: Mutex::new(BTreeMap::new()),
            participants,
        }
    }
    async fn wait(&self, index: usize) {
        let (sender, receiver) = oneshot::channel();
        {
            let mut waiting = self.waiting.lock().unwrap();
            assert!(waiting.insert(index, sender).is_none());
            if waiting.len() == self.participants {
                for (_, sender) in std::mem::take(&mut *waiting) {
                    sender.send(()).expect("gate caller cancelled");
                }
            }
        }
        receiver.await.expect("gate closed before all arrivals");
    }
}
struct StagedGate {
    waiting: Mutex<BTreeMap<usize, (PinToken, oneshot::Sender<()>)>>,
    ready: Notify,
}
impl StagedGate {
    async fn wait(&self, index: usize, token: PinToken, writers: usize) {
        let (sender, receiver) = oneshot::channel();
        {
            let mut waiting = self.waiting.lock().unwrap();
            assert!(waiting.insert(index, (token, sender)).is_none());
            if waiting.len() == writers {
                self.ready.notify_one();
            }
        }
        receiver.await.expect("staged writer gate closed");
    }
}
struct CommitGate {
    armed: AtomicBool,
    arrivals: AtomicUsize,
    barrier: Gate,
    seed: u64,
    writers: usize,
    log: Log,
}
#[derive(Clone)]
struct Metadata {
    inner: MemoryMetadataStore,
    pins: Arc<dyn PinStore>,
    gate: Arc<CommitGate>,
    writer_index: usize,
}
#[async_trait]
impl MetadataStore for Metadata {
    fn entropy_source(&self) -> Arc<dyn EntropySource> {
        self.inner.entropy_source()
    }
    fn coordinates_payload_catalog(&self) -> bool {
        false
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
        revision: &RepositoryRevision,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        if self.gate.armed.load(Ordering::SeqCst)
            && self.gate.arrivals.fetch_add(1, Ordering::SeqCst) < self.gate.writers
        {
            self.gate
                .log
                .lock()
                .unwrap()
                .first_commit_revisions
                .push(revision.to_string());
            self.gate.barrier.wait(self.writer_index).await;
            let rank = (self.writer_index + (self.gate.seed % self.gate.writers as u64) as usize)
                % self.gate.writers;
            tokio::time::sleep(Duration::from_millis((rank + 1) as u64)).await;
        }
        let result = self.inner.commit(revision, mutation).await;
        if matches!(result, Err(MetadataError::StaleRevision { .. })) {
            self.gate.log.lock().unwrap().stale_revision_conflicts += 1;
        }
        result
    }
}
fn blobs(store: &Store) -> ChunkedBlobStore {
    ChunkedBlobStore::new(Arc::new(store.clone()), "payload".into(), 1024)
}

pub fn run(seed: u64, scenario: Scenario, release_claims_early: bool) -> Result<Report, String> {
    run_with_faults(seed, scenario, release_claims_early, false, None)
}
/// Cancel the first writer after a prefix of missing chunks, with two live survivors.
pub fn run_partial_restore(
    seed: u64,
    scenario: Scenario,
    almost_complete: bool,
) -> Result<Report, String> {
    if !scenario.cancellation() {
        return Err("partial restore requires a cancellation scenario".into());
    }
    run_with_faults(seed, scenario, false, false, Some(almost_complete))
}
fn run_with_faults(
    seed: u64,
    scenario: Scenario,
    release_claims_early: bool,
    release_survivors_early: bool,
    partial_restore: Option<bool>,
) -> Result<Report, String> {
    let fixture = fixture();
    let log = Log::default();
    let state = Shared::default();
    let mut sim = crate::harness::simulation(seed, 20, 30);
    sim.host("store", move || {
        serve(state.clone(), PinScenario::PinVsDeletion, Faults::default())
    });
    let output = log.clone();
    sim.client("chunks", async move {
        exercise(
            seed,
            scenario,
            release_claims_early,
            release_survivors_early,
            partial_restore,
            fixture,
            output,
        )
        .await
    });
    sim.run()
        .map_err(|e| format!("seed={seed} {}: {e}", scenario.name()))?;
    let report = log.lock().unwrap().clone();
    Ok(report)
}
async fn exercise(
    seed: u64,
    scenario: Scenario,
    release_claims_early: bool,
    release_survivors_early: bool,
    partial_restore: Option<bool>,
    fixture: &'static Fixture,
    log: Log,
) -> crate::SimResult {
    let pins: Arc<dyn PinStore> = Arc::new(ledger("chunks", seed));
    let meta = Metadata {
        inner: MemoryMetadataStore::new_with_entropy(Arc::new(SeededEntropy::new(
            seed,
            "chunk-metadata",
        )))?,
        pins: pins.clone(),
        gate: Arc::new(CommitGate {
            armed: AtomicBool::new(false),
            arrivals: AtomicUsize::new(0),
            barrier: Gate::new(scenario.publishing_count()),
            seed,
            writers: scenario.publishing_count(),
            log: log.clone(),
        }),
        writer_index: 0,
    };
    let store = Store {
        inner: Arc::new(InMemory::new()),
        log: log.clone(),
        target: chunk_path(*fixture.unique.first().unwrap()),
        scenario,
        seed,
        armed: Arc::new(AtomicBool::new(true)),
        entered: Arc::new(Notify::new()),
        resume: Arc::new(Notify::new()),
    };
    for (path, bytes) in &fixture.objects {
        store.inner.put(path, bytes.clone().into()).await?;
    }
    {
        let mut report = log.lock().unwrap();
        report.shared_chunks = fixture.shared.iter().map(ToString::to_string).collect();
        report.unique_chunks = fixture.unique.iter().map(ToString::to_string).collect();
    }
    let repository = Repository::new(blobs(&store), meta.clone());
    let initial = repository.mutation_session().await?;
    let a = initial
        .stage_existing(ObjectKey::blob(fixture.a), fixture.a)
        .await?;
    let b = initial
        .stage_existing(ObjectKey::blob(fixture.b), fixture.b)
        .await?;
    initial
        .publish_rooted(
            vec![a, b],
            RootName::try_from("A")?,
            ObjectKey::blob(fixture.a),
        )
        .await?;
    drop(initial);
    flush_repository_leases().await?;
    let collector = Repository::new(blobs(&store), meta.clone());
    {
        let collect = collector.collect();
        tokio::pin!(collect);
        tokio::select! { _=store.entered.notified()=>{}, result=&mut collect=>return Err(format!("GC finished before DELETE pause: {result:?}").into()) }
        event(&log, "collector-caller-cancelled", &store.target);
        log.lock().unwrap().collector_cancelled = true;
    }
    let inventory = pins.inventory().await?;
    let protected = PinResource::StorageObject(store.target.to_string());
    if !inventory
        .deletions
        .values()
        .any(|claim| claim.contains(&protected))
    {
        return Err("pending DELETE has no physical ownership claim".into());
    }
    let probe = pins
        .register(DataPin {
            scope: PinScope::Staging,
            catalog: None,
            resources: BTreeSet::from([protected]),
        })
        .await?;
    if let Some(token) = probe {
        pins.release(&token).await?;
        return Err("pending DELETE admitted conflicting upload".into());
    }
    if release_claims_early {
        for token in inventory.deletions.keys() {
            pins.finish_deletions(token).await?;
        }
        event(
            &log,
            "negative-control-released-delete-claims",
            &store.target,
        );
    }
    meta.gate
        .armed
        .store(!scenario.cancellation(), Ordering::SeqCst);
    let admitted = Arc::new(Gate::new(scenario.writer_count()));
    let staged = Arc::new(StagedGate {
        waiting: Mutex::new(BTreeMap::new()),
        ready: Notify::new(),
    });
    let mut writers = Vec::new();
    for index in 0..scenario.writer_count() {
        let writer_store = store.clone();
        let root = scenario.writer_root(index);
        let writer_pins: Arc<dyn PinStore> = if scenario.writer_count() == 1 {
            pins.clone()
        } else {
            Arc::new(ledger(&format!("chunk-writer-{index}"), seed))
        };
        let mut writer_meta = meta.clone();
        writer_meta.pins = writer_pins.clone();
        writer_meta.writer_index = index;
        let admitted = admitted.clone();
        let staged = staged.clone();
        writers.push(tokio::spawn(async move {
            writer_store
                .log
                .lock()
                .unwrap()
                .writers_started
                .push(root.clone());
            event(
                &writer_store.log,
                &format!("writer-started:{root}"),
                &writer_store.target,
            );
            // Restore verified wire objects with independent production staging leases.
            let mut resources: BTreeSet<_> = fixture
                .b_paths
                .iter()
                .map(|p| PinResource::StorageObject(p.to_string()))
                .collect();
            resources.insert(PinResource::Blob(fixture.b));
            resources.extend(fixture.b_chunks.iter().copied().map(PinResource::Chunk));
            let guard = DataPinLease::acquire(
                writer_pins.clone(),
                DataPin {
                    scope: PinScope::Staging,
                    catalog: None,
                    resources,
                },
            )
            .await?;
            let last_admitted = {
                let mut report = writer_store.log.lock().unwrap();
                report.writers_admitted.push(root.clone());
                report.writers_admitted.len() == scenario.writer_count()
            };
            if last_admitted {
                let inventory = writer_pins.inventory().await?;
                let overlapping = inventory
                    .pins
                    .values()
                    .filter(|pin| {
                        pin.scope == PinScope::Staging
                            && pin.resources.contains(&PinResource::Blob(fixture.b))
                            && pin.resources.contains(&PinResource::StorageObject(
                                writer_store.target.to_string(),
                            ))
                    })
                    .count();
                writer_store.log.lock().unwrap().overlapping_staging_pins = overlapping;
                if overlapping != scenario.writer_count() {
                    return Err(
                        "competing writers did not hold distinct overlapping staging pins".into(),
                    );
                }
            }
            event(
                &writer_store.log,
                &format!("writer-admitted:{root}"),
                &writer_store.target,
            );
            // All writers must hold overlapping resources before any restores or publishes.
            admitted.wait(index).await;
            if let Some(almost_complete) = partial_restore {
                if index == 0 {
                    let count = if almost_complete {
                        fixture.unique.len() - 1
                    } else {
                        1
                    };
                    for id in fixture.unique.iter().take(count) {
                        let path = chunk_path(*id);
                        let (_, bytes) = fixture
                            .objects
                            .iter()
                            .find(|(p, _)| *p == path)
                            .expect("fixture chunk missing");
                        if writer_store.inner.head(&path).await.is_ok() {
                            return Err("partial chunk was already restored".into());
                        }
                        writer_store.put(&path, bytes.clone().into()).await?;
                    }
                }
                staged
                    .wait(index, guard.token().clone(), scenario.writer_count())
                    .await;
            }
            for (path, bytes) in &fixture.objects {
                if fixture.b_paths.contains(path)
                    && matches!(
                        writer_store.inner.head(path).await,
                        Err(object_store::Error::NotFound { .. })
                    )
                {
                    writer_store.put(path, bytes.clone().into()).await?;
                }
            }
            let writer_repository = Repository::new(blobs(&writer_store), writer_meta);
            let mutation = writer_repository.mutation_session().await?;
            let b = mutation
                .stage_existing(ObjectKey::blob(fixture.b), fixture.b)
                .await?;
            if scenario.cancellation() && partial_restore.is_none() {
                staged
                    .wait(index, guard.token().clone(), scenario.writer_count())
                    .await;
            }
            mutation
                .publish_rooted(
                    vec![b],
                    RootName::try_from(root.as_str())?,
                    ObjectKey::blob(fixture.b),
                )
                .await?;
            event(
                &writer_store.log,
                &format!("writer-published:{root}"),
                &writer_store.target,
            );
            writer_store
                .log
                .lock()
                .unwrap()
                .writers_published
                .push(root);
            drop(mutation);
            drop(guard);
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        }));
    }
    if !release_claims_early {
        tokio::time::sleep(Duration::from_millis(150)).await;
        {
            let report = log.lock().unwrap();
            if report.writers_started.len() != scenario.writer_count() {
                return Err("not every competing writer attempted admission".into());
            }
            if !report.writers_admitted.is_empty() || !report.writers_published.is_empty() {
                return Err("writer admitted before DELETE acknowledgement".into());
            }
        }
        log.lock().unwrap().writer_waited = true;
        event(&log, "writers-waited-for-delete-settlement", &store.target);
        store.resume.notify_one();
    }
    if scenario.cancellation() {
        staged.ready.notified().await;
        let tokens: BTreeMap<_, _> = staged
            .waiting
            .lock()
            .unwrap()
            .iter()
            .map(|(i, (token, _))| (*i, token.clone()))
            .collect();
        let cancelled = writers.remove(0);
        cancelled.abort();
        if !cancelled
            .await
            .expect_err("paused writer completed before cancellation")
            .is_cancelled()
        {
            return Err("writer panicked instead of being cancelled".into());
        }
        staged.waiting.lock().unwrap().remove(&0);
        event(&log, "writer-cancelled:B/0", &store.target);
        log.lock()
            .unwrap()
            .cancelled_writers
            .push(scenario.writer_root(0));
        // The other writers are paused before publication, so only lease cleanup
        // runs here. Submitted metadata requests are covered in repository_cases.
        flush_repository_leases().await?;
        let inventory = pins.inventory().await?;
        if inventory.pins.contains_key(&tokens[&0]) {
            return Err("cancelled writer leaked its physical staging token".into());
        }
        let surviving = tokens
            .iter()
            .filter(|(i, token)| **i != 0 && inventory.pins.contains_key(*token))
            .count();
        if surviving != scenario.publishing_count() {
            return Err("cancelling one writer released another writer's pin".into());
        }
        {
            let mut report = log.lock().unwrap();
            report.cancelled_pin_released = true;
            report.surviving_staging_pins = surviving;
            if !report.writers_published.is_empty() {
                return Err("writer published before cancellation collection window".into());
            }
        }
        if meta
            .snapshot()
            .await?
            .root(&RootName::try_from("B/0")?)
            .await?
            .is_some()
        {
            return Err("cancelled unsubmitted publication created a root".into());
        }
        let mut partial = Vec::new();
        if partial_restore.is_some() {
            for id in &fixture.unique {
                let path = chunk_path(*id);
                if store.inner.head(&path).await.is_ok() {
                    let bytes = store.inner.get(&path).await?.bytes().await?;
                    let expected = &fixture
                        .objects
                        .iter()
                        .find(|(p, _)| *p == path)
                        .expect("fixture chunk missing")
                        .1;
                    if bytes != *expected {
                        return Err("partial restore wrote incorrect wire bytes".into());
                    }
                    partial.push((path, bytes));
                }
            }
            let expected = if partial_restore == Some(true) {
                fixture.unique.len() - 1
            } else {
                1
            };
            if partial.len() != expected || partial.len() >= fixture.unique.len() {
                return Err("cancellation did not interrupt a partial restore".into());
            }
        }
        if release_survivors_early {
            for (token, pin) in &inventory.pins {
                if pin.scope == PinScope::Staging {
                    pins.release(token).await?;
                }
            }
            event(
                &log,
                "negative-control-released-surviving-pins",
                &store.target,
            );
        }
        let gc = Repository::new(blobs(&store), meta.clone())
            .collect()
            .await?;
        if gc.spill.files_opened != 0 {
            return Err("cancellation collection spilled to real filesystem".into());
        }
        let reader = blobs(&store);
        if partial_restore.is_some() {
            for (path, expected) in partial {
                let actual = match store.inner.get(&path).await {
                    Ok(result) => result.bytes().await.ok(),
                    Err(_) => None,
                };
                if actual.as_ref() != Some(&expected) {
                    return Err("surviving writer lost partially restored chunks during GC".into());
                }
                log.lock()
                    .unwrap()
                    .partial_restore_preserved
                    .push(path.to_string());
            }
            if reader.read_to_vec(&fixture.a).await?.as_ref() != Some(&fixture.a_bytes) {
                return Err("partial restore collection damaged shared root A".into());
            }
        } else if !matches!(reader.read_to_vec(&fixture.b).await, Ok(Some(ref bytes)) if bytes == &fixture.b_bytes)
        {
            return Err("surviving writer lost staged chunks during GC".into());
        }
        flush_repository_leases().await?;
        let inventory = pins.inventory().await?;
        if tokens
            .iter()
            .any(|(i, token)| *i != 0 && !inventory.pins.contains_key(token))
        {
            return Err("collection released a surviving writer's staging token".into());
        }
        log.lock().unwrap().collection_with_surviving_pins = true;
        event(
            &log,
            "collection-preserved-surviving-staged-payload",
            &store.target,
        );
        meta.gate.armed.store(true, Ordering::SeqCst);
        for (_, (_, sender)) in std::mem::take(&mut *staged.waiting.lock().unwrap()) {
            sender.send(()).expect("surviving writer cancelled");
        }
    }
    for writer in writers {
        writer.await?.map_err(|e| e.to_string())?;
    }
    if release_claims_early {
        store.resume.notify_one();
    }
    flush_repository_leases().await?;
    // Fresh handles avoid trusting the collector's or writer's chunk caches.
    let reader = blobs(&store);
    for id in &fixture.shared {
        if store.inner.head(&chunk_path(*id)).await.is_err()
            || log
                .lock()
                .unwrap()
                .deleted_paths
                .contains(&chunk_path(*id).to_string())
        {
            return Err("shared chunk deleted while A remained rooted".into());
        }
    }
    for id in &fixture.b_chunks {
        if store.inner.head(&chunk_path(*id)).await.is_err() {
            return Err("late delete removed published chunk".into());
        }
    }
    for (root, id, expected) in std::iter::once(("A".to_string(), fixture.a, &fixture.a_bytes))
        .chain(
            (0..scenario.writer_count())
                .filter(|i| scenario.publishes(*i))
                .map(|i| (scenario.writer_root(i), fixture.b, &fixture.b_bytes)),
        )
    {
        if meta
            .snapshot()
            .await?
            .root(&RootName::try_from(root.as_str())?)
            .await?
            != Some(ObjectKey::blob(id))
            || reader.read_to_vec(&id).await?.as_ref() != Some(expected)
        {
            return Err("published chunk graph unreadable or bytes changed".into());
        }
    }
    let settled = pins.inventory().await?;
    if !settled.pins.is_empty()
        || !settled.deletions.is_empty()
        || settled.collector.is_some()
        || settled.logical_prune.is_some()
    {
        return Err("settled chunk GC leaked ownership".into());
    }
    let final_gc = Repository::new(blobs(&store), meta.clone())
        .collect()
        .await?;
    if final_gc.spill.files_opened != 0 {
        return Err("chunk simulation spilled to real filesystem".into());
    }
    let reader = blobs(&store);
    for (root, id, expected) in std::iter::once(("A".to_string(), fixture.a, &fixture.a_bytes))
        .chain(
            (0..scenario.writer_count())
                .filter(|i| scenario.publishes(*i))
                .map(|i| (scenario.writer_root(i), fixture.b, &fixture.b_bytes)),
        )
    {
        if meta
            .snapshot()
            .await?
            .root(&RootName::try_from(root.as_str())?)
            .await?
            != Some(ObjectKey::blob(id))
            || reader.read_to_vec(&id).await?.as_ref() != Some(expected)
        {
            return Err("final GC damaged a published chunk graph".into());
        }
    }
    if scenario.cancellation()
        && meta
            .snapshot()
            .await?
            .root(&RootName::try_from("B/0")?)
            .await?
            .is_some()
    {
        return Err("cancelled writer's unsubmitted root appeared after GC".into());
    }
    let mut report = log.lock().unwrap();
    for id in &fixture.shared {
        if report.restored_paths.contains(&chunk_path(*id).to_string()) {
            return Err("shared chunk was recreated instead of reused".into());
        }
    }
    for id in &fixture.unique {
        let path = chunk_path(*id).to_string();
        if !report.deleted_paths.contains(&path) || !report.restored_paths.contains(&path) {
            return Err("unique chunk did not exercise deletion and recreation".into());
        }
        let acknowledged = report
            .events
            .iter()
            .position(|(action, p, _)| action == "delete-acknowledged" && p == &path);
        let restored = report
            .events
            .iter()
            .position(|(action, p, _)| action == "restore-submitted" && p == &path);
        if !matches!((acknowledged, restored), (Some(a), Some(r)) if a < r) {
            return Err("chunk recreation preceded DELETE acknowledgement".into());
        }
    }
    let mut expected_roots: Vec<_> = (0..scenario.writer_count())
        .filter(|i| scenario.publishes(*i))
        .map(|i| scenario.writer_root(i))
        .collect();
    expected_roots.sort();
    let mut actual_roots = report.writers_published.clone();
    actual_roots.sort();
    if actual_roots != expected_roots {
        return Err("acknowledged writer publications were missing or duplicated".into());
    }
    if report.first_commit_revisions.len() != scenario.publishing_count()
        || report
            .first_commit_revisions
            .iter()
            .any(|r| r != &report.first_commit_revisions[0])
        || report.stale_revision_conflicts < scenario.publishing_count() - 1
    {
        return Err("competing publication retry oracle was vacuous".into());
    }
    report.roots_readable = true;
    report.spill_files = final_gc.spill.files_opened;
    if report.deleted_paths.is_empty() || report.restored_paths.is_empty() {
        return Err("delete/recreation oracle was vacuous".into());
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn concurrent_simulations_preserve_complete_cancellation_replay() {
        let expected: Vec<_> = Scenario::CANCELLATIONS
            .into_iter()
            .map(|scenario| (scenario, run(0, scenario, false).unwrap()))
            .collect();
        let start = std::sync::Barrier::new(4);
        std::thread::scope(|scope| {
            let workers: Vec<_> = (0..4)
                .map(|_| {
                    let expected = &expected;
                    let start = &start;
                    scope.spawn(move || {
                        start.wait();
                        for _ in 0..2 {
                            for (scenario, expected) in expected {
                                assert_eq!(
                                    &run(0, *scenario, false).unwrap(),
                                    expected,
                                    "concurrent seed=0, {}",
                                    scenario.name()
                                );
                            }
                        }
                    })
                })
                .collect();
            for worker in workers {
                worker.join().unwrap();
            }
        });
    }

    #[test]
    fn partial_restore_corpus_replays() {
        for seed in 0..16 {
            for scenario in Scenario::CANCELLATIONS {
                for almost_complete in [false, true] {
                    let first = run_partial_restore(seed, scenario, almost_complete).unwrap();
                    assert_eq!(
                        first,
                        run_partial_restore(seed, scenario, almost_complete).unwrap()
                    );
                    assert!(
                        !first.partial_restore_preserved.is_empty()
                            && first.roots_readable
                            && first.cancelled_pin_released
                    );
                }
            }
        }
    }
    #[test]
    fn partial_restore_rejects_missing_survivor_pins() {
        for scenario in Scenario::CANCELLATIONS {
            for almost_complete in [false, true] {
                let error =
                    run_with_faults(7, scenario, false, true, Some(almost_complete)).unwrap_err();
                assert!(
                    error.contains("lost partially restored chunks during GC"),
                    "{error}"
                );
            }
        }
    }
    #[test]
    fn shared_chunk_corpus_replays() {
        for seed in 0..32 {
            for scenario in Scenario::ALL {
                let first = run(seed, scenario, false).unwrap();
                let second = run(seed, scenario, false).unwrap();
                assert_eq!(first, second, "seed={seed}, {}", scenario.name());
                assert!(first.writer_waited && first.collector_cancelled && first.roots_readable);
            }
        }
    }
    #[test]
    fn checker_rejects_release_of_surviving_staging_pins() {
        let error = run_with_faults(7, Scenario::CancelWriterBeforeApply, false, true, None)
            .expect_err("checker accepted GC destroying surviving writers' staged chunks");
        assert!(
            error.contains("surviving writer lost staged chunks during GC"),
            "{error}"
        );
    }
    #[test]
    fn competing_writers_reject_early_delete_claim_release() {
        let error = run(7, Scenario::WritersBeforeApply, true)
            .expect_err("checker accepted late DELETE with competing publications");
        assert!(
            error.contains("late delete removed published chunk"),
            "{error}"
        );
    }
    #[test]
    fn checker_rejects_delete_claim_release_before_settlement() {
        let error = run(7, Scenario::DeleteBeforeApply, true)
            .expect_err("checker accepted stale DELETE destroying new publication");
        assert!(
            error.contains("late delete removed published chunk"),
            "{error}"
        );
    }
}
