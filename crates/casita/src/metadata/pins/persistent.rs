//! Durable recovery ledgers and separately coordinated process-reader state.
//! Unknown write outcomes leave protection recorded, with no timeout or
//! unconditional PUT fallback.
use super::*;
#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod benchmarks;
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod filesystem;
mod group;
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod journal;
mod readers;
#[cfg(test)]
mod remote_benchmark;
mod timing;
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion, path::Path};
use readers::ReaderOwner;
use std::io::{Read, Write};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
use timing::LedgerPhase;

#[async_trait]
trait Backend: Send + Sync {
    type Version: Send;
    async fn edit(&self, operation: Operation) -> Result<Outcome, MetadataError> {
        optimistic_edit(self, operation).await
    }
    async fn admit_reader(&self, pin: DataPin) -> Result<Outcome, MetadataError> {
        self.edit(Operation::Register(pin)).await
    }
    async fn load(&self) -> Result<(PinInventory, Self::Version), MetadataError>;
    async fn compare_exchange(
        &self,
        expected: Self::Version,
        state: PinInventory,
    ) -> Result<bool, MetadataError>;
}

/// Pin ledger stored using conditional object-store writes.
///
/// The object store must provide linearizable GET and conditional PUT. Missing
/// conditional-write support is an error; it never falls back to overwrite.
/// Use a dedicated path and never delete/reset the ledger while runners exist.
/// All pins and deletion claims are non-expiring, including after process exit.
/// The encoded ledger is limited to 64 MiB; exceeding it fails without losing
/// existing protection.
#[derive(Clone)]
pub struct ObjectPinStore {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

impl ObjectPinStore {
    pub fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self { store, path }
    }
}

fn backend(error: impl std::fmt::Display) -> MetadataError {
    MetadataError::Backend(format!("pin ledger: {error}"))
}

#[async_trait]
impl Backend for ObjectPinStore {
    type Version = Option<UpdateVersion>;

    async fn load(&self) -> Result<(PinInventory, Self::Version), MetadataError> {
        let object = match self.store.get(&self.path).await {
            Ok(object) => object,
            Err(object_store::Error::NotFound { .. }) => {
                return Ok((PinInventory::default(), None));
            }
            Err(error) => return Err(backend(error)),
        };
        if object.meta.size > codec::MAX_BYTES as u64 {
            return Err(backend("oversized inventory"));
        }
        let version = UpdateVersion {
            e_tag: object.meta.e_tag.clone(),
            version: object.meta.version.clone(),
        };
        if version.e_tag.is_none() && version.version.is_none() {
            return Err(backend("conditional writes require an object version"));
        }
        let mut bytes = Vec::new();
        let mut stream = object.into_stream();
        while let Some(part) = stream.try_next().await.map_err(backend)? {
            if bytes.len().saturating_add(part.len()) > codec::MAX_BYTES {
                return Err(backend("oversized inventory"));
            }
            bytes.extend_from_slice(&part);
        }
        Ok((codec::decode(&bytes)?, Some(version)))
    }

    async fn compare_exchange(
        &self,
        expected: Self::Version,
        state: PinInventory,
    ) -> Result<bool, MetadataError> {
        let bytes = codec::encode(&state)?;
        let mode = expected.map_or(PutMode::Create, PutMode::Update);
        match self
            .store
            .put_opts(
                &self.path,
                bytes.into(),
                PutOptions {
                    mode,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => Ok(false),
            Err(error) => Err(backend(error)),
        }
    }
}

/// Pin ledger shared by local processes through a file lock and durable updates.
///
/// Staging, explicit retention, and deletion records survive process death.
/// Their recovery must establish that the owner has stopped before releasing
/// its exact token. Ordinary read pins share a process-owned kernel lock and a
/// separate atomic inventory; collectors ignore them after that owner exits.
/// A durable format fence and revision reservation coordinate both inventories.
/// Linux and macOS append checksummed deltas, sharing one sync across up to 64 queued
/// operations. Checkpoints reuse two preallocated files with atomic exchange,
/// so bounded prune/claim updates reuse allocated capacity on a full disk.
/// Growing that capacity still requires headroom; other platforms use durable
/// replacement files. The encoded ledger is limited to 64 MiB.
#[derive(Clone)]
pub struct FilePinStore {
    path: PathBuf,
    #[cfg(test)]
    replacement: bool,
    process: u32,
    local: Arc<std::sync::OnceLock<Arc<group::Local>>>,
    reader_owner: Arc<std::sync::Mutex<Option<Arc<ReaderOwner>>>>,
    #[cfg(test)]
    reader_pause: Arc<std::sync::Mutex<Option<readers::TestPause>>>,
}

impl FilePinStore {
    #[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
    pub(crate) fn test_stats(&self) -> BTreeMap<&'static str, u64> {
        self.local().unwrap().stats.snapshot()
    }

    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            #[cfg(test)]
            replacement: false,
            process: std::process::id(),
            local: Default::default(),
            reader_owner: Default::default(),
            #[cfg(test)]
            reader_pause: Default::default(),
        }
    }

    fn check_process(&self) -> Result<(), MetadataError> {
        if self.process != std::process::id() {
            return Err(backend("reopen the local pin store after fork"));
        }
        Ok(())
    }

    #[cfg(test)]
    fn write_locked(&self, bytes: &[u8]) -> Result<(), MetadataError> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        if !self.replacement {
            return self.journal_test_checkpoint(&codec::decode(bytes)?);
        }
        self.write_replacement_locked(bytes)
    }

    fn write_replacement_locked(&self, bytes: &[u8]) -> Result<(), MetadataError> {
        let _phase = LedgerPhase::new("replacement_write_total");
        self.local()?
            .stats
            .replacements
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        self.exchange_slot(bytes)?;
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let mut temporary = tempfile::NamedTempFile::new_in(self.parent()).map_err(backend)?;
            temporary.write_all(bytes).map_err(backend)?;
            temporary.as_file().sync_all().map_err(backend)?;
            temporary.persist(&self.path).map_err(backend)?;
            std::fs::File::open(self.parent())
                .and_then(|directory| directory.sync_all())
                .map_err(backend)?;
        }
        Ok(())
    }

    fn parent(&self) -> &std::path::Path {
        self.path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new("."))
    }

    fn lock_file(&self) -> Result<std::fs::File, MetadataError> {
        let name = self
            .path
            .file_name()
            .ok_or_else(|| backend("inventory path has no filename"))?;
        let mut name = name.to_os_string();
        name.push(".lock");
        std::fs::create_dir_all(self.parent()).map_err(backend)?;
        std::fs::File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.parent().join(name))
            .map_err(backend)
    }

    fn read_locked(&self) -> Result<PinInventory, MetadataError> {
        let _phase = LedgerPhase::new("read_decode");
        #[cfg(test)]
        if self.replacement {
            return self.read_replacement_locked();
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            self.journal_read()
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            self.read_replacement_locked()
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn persist_inventory(&self, state: &PinInventory) -> Result<(), MetadataError> {
        self.write_replacement_locked(&codec::encode(state)?)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn persist_inventory_deferred(
        &self,
        state: &PinInventory,
        _defer: bool,
    ) -> Result<(), MetadataError> {
        self.persist_inventory(state)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn flush_pending_journal(&self) -> Result<(), MetadataError> {
        Ok(())
    }

    fn read_replacement_locked(&self) -> Result<PinInventory, MetadataError> {
        let file = match std::fs::File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(PinInventory::default());
            }
            Err(error) => return Err(backend(error)),
        };
        let mut file = file;
        let mut header = [0u8; 16];
        file.read_exact(&mut header).map_err(backend)?;
        if header.starts_with(b"CASPSL01") {
            let length = u64::from_le_bytes(header[8..16].try_into().unwrap());
            let length = usize::try_from(length).map_err(backend)?;
            if length > codec::MAX_BYTES {
                return Err(backend("oversized pin slot"));
            }
            let mut bytes = vec![0; length];
            file.read_exact(&mut bytes).map_err(backend)?;
            codec::decode(&bytes)
        } else {
            let mut bytes = header.to_vec();
            file.take((codec::MAX_BYTES + 1 - header.len()) as u64)
                .read_to_end(&mut bytes)
                .map_err(backend)?;
            codec::decode(&bytes)
        }
    }

    /// Reuse two allocated inodes. Atomic exchange keeps the previous active
    /// ledger intact until the complete replacement has been synced, and does
    /// not require a new directory entry or payload extent on a full disk.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn exchange_slot(&self, bytes: &[u8]) -> Result<(), MetadataError> {
        fn grow_slot(file: &mut std::fs::File, capacity: usize) -> Result<(), MetadataError> {
            let allocated = file.metadata().map_err(backend)?.len();
            file.seek(SeekFrom::End(0)).map_err(backend)?;
            let mut remaining = (capacity as u64).saturating_sub(allocated);
            let zeros = [0u8; 8192];
            while remaining > 0 {
                let count = remaining.min(zeros.len() as u64) as usize;
                file.write_all(&zeros[..count]).map_err(backend)?;
                remaining -= count as u64;
            }
            Ok(())
        }
        fn write_slot(
            file: &mut std::fs::File,
            bytes: &[u8],
            capacity: usize,
        ) -> Result<(), MetadataError> {
            // The final content sync also persists capacity growth. The spare
            // is not authoritative until that sync and the name exchange, so
            // syncing its allocation separately would add an unnecessary wait
            // under the exclusive ledger lock.
            grow_slot(file, capacity)?;
            file.seek(SeekFrom::Start(0)).map_err(backend)?;
            file.write_all(b"CASPSL01").map_err(backend)?;
            file.write_all(&(bytes.len() as u64).to_le_bytes())
                .map_err(backend)?;
            file.write_all(bytes).map_err(backend)?;
            let _phase = LedgerPhase::new("payload_sync");
            crate::blob::sync_ordered(file).map_err(backend)
        }

        // Keep both slots large enough for the new inventory plus bounded
        // collection claims, before making that inventory authoritative.
        let capacity = (bytes.len() + 16 + 64 * 1024)
            .next_power_of_two()
            .max(128 * 1024);
        let framed = match std::fs::File::open(&self.path) {
            Ok(mut file) => {
                let mut magic = [0; 8];
                file.read_exact(&mut magic).map_err(backend)?;
                &magic == b"CASPSL01"
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(backend(error)),
        };
        if !framed {
            // Initial creation and migration preserve the old logical state.
            // Neither may leave a half-written active ledger after a crash.
            let previous = codec::encode(&self.read_replacement_locked()?)?;
            let mut initial = tempfile::NamedTempFile::new_in(self.parent()).map_err(backend)?;
            write_slot(initial.as_file_mut(), &previous, capacity)?;
            initial.persist(&self.path).map_err(backend)?;
            std::fs::File::open(self.parent())
                .and_then(|file| crate::blob::sync_ordered(&file))
                .map_err(backend)?;
        } else {
            let mut active = std::fs::File::options()
                .write(true)
                .open(&self.path)
                .map_err(backend)?;
            grow_slot(&mut active, capacity)?;
            // Keep the other slot's reserve durable before exchanging names.
            let _phase = LedgerPhase::new("capacity_sync");
            crate::blob::sync_ordered(&active).map_err(backend)?;
        }
        let mut spare_name = self
            .path
            .file_name()
            .ok_or_else(|| backend("inventory path has no filename"))?
            .to_os_string();
        spare_name.push(".spare");
        let spare_path = self.parent().join(spare_name);
        let mut spare = std::fs::File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&spare_path)
            .map_err(backend)?;
        write_slot(&mut spare, bytes, capacity)?;
        let _phase = LedgerPhase::new("exchange_directory_sync");
        self.exchange_checkpoint(&spare_path).map_err(backend)?;
        // The only flush that waits for the drive: it persists every slot
        // write, reserve and name synced before it (see `sync_ordered`).
        std::fs::File::open(self.parent())
            .and_then(|file| file.sync_all())
            .map_err(backend)
    }
}

#[async_trait]
impl Backend for FilePinStore {
    type Version = u64;

    async fn admit_reader(&self, pin: DataPin) -> Result<Outcome, MetadataError> {
        self.check_process()?;
        if !matches!(pin.scope, PinScope::Snapshot { .. } | PinScope::Closures(_)) {
            return Err(backend("process reader cannot own mutation recovery"));
        }
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.register_local_reader(pin))
            .await
            .map_err(backend)?
    }

    async fn edit(&self, operation: Operation) -> Result<Outcome, MetadataError> {
        self.check_process()?;
        #[cfg(test)]
        if self.replacement {
            let this = self.clone();
            return tokio::task::spawn_blocking(move || {
                this.edit_group(&[operation])?.pop().unwrap()
            })
            .await
            .map_err(backend)?;
        }
        self.grouped_edit(operation).await
    }

    async fn load(&self) -> Result<(PinInventory, Self::Version), MetadataError> {
        self.check_process()?;
        let this = self.clone();
        let queue = LedgerPhase::new("blocking_queue");
        tokio::task::spawn_blocking(move || {
            drop(queue);
            let setup = LedgerPhase::new("lock_setup");
            let lock = this.lock_file()?;
            drop(setup);
            let wait = LedgerPhase::new("exclusive_lock_wait");
            // A lost volatile file can require durable clock recovery.
            lock.lock().map_err(backend)?;
            drop(wait);
            let result = (|| {
                let mut durable = this.read_locked()?;
                let readers = this.readers_locked(&mut durable)?;
                let state = this.merge_readers(&durable, &readers)?;
                let revision = state.revision;
                Ok((state, revision))
            })();
            lock.unlock().map_err(backend)?;
            result
        })
        .await
        .map_err(backend)?
    }

    async fn compare_exchange(
        &self,
        _expected: Self::Version,
        _state: PinInventory,
    ) -> Result<bool, MetadataError> {
        Err(backend("local ledger requires its locked edit protocol"))
    }
}

#[derive(Clone)]
enum Operation {
    BeginCollection(u64, Option<PinToken>),
    AcquireCollection(Option<PinToken>),
    FinishCollection(PinToken),
    Register(DataPin),
    Protect(PinToken, BTreeSet<PinResource>),
    Release(PinToken),
    BeginPrune(u64, BTreeSet<PinToken>),
    BeginPruneValidating(PinToken, BTreeSet<PinToken>),
    FinishPrune(PinToken),
    Claim(u64, BTreeSet<PinResource>),
    ClaimValidated(PinToken, BTreeSet<PinResource>, BTreeSet<BlobId>),
    ClaimDuringPrune(u64, BTreeSet<PinResource>, PinToken, PinToken),
    FinishDeletion(PinToken),
}

enum Outcome {
    Prune(Box<Option<(PinToken, PinInventory)>>),
    Token(Option<PinToken>),
    Protected(bool),
    Finished,
}

impl Operation {
    async fn apply(&self, state: &MemoryPinStore) -> Result<Outcome, MetadataError> {
        Ok(match self {
            Self::BeginCollection(revision, previous) => {
                Outcome::Token(state.begin_collection(*revision, previous.clone()).await?)
            }
            Self::AcquireCollection(previous) => {
                Outcome::Token(state.acquire_collection(previous.clone()).await?)
            }
            Self::FinishCollection(token) => {
                state.finish_collection(token).await?;
                Outcome::Finished
            }
            Self::Register(pin) => Outcome::Token(state.register(pin.clone()).await?),
            Self::Protect(token, resources) => {
                Outcome::Protected(state.protect(token, resources.clone()).await?)
            }
            Self::Release(token) => {
                state.release(token).await?;
                Outcome::Finished
            }
            Self::BeginPrune(revision, claims) => Outcome::Token(
                state
                    .begin_prune_recovering(*revision, claims.clone())
                    .await?,
            ),
            Self::BeginPruneValidating(collector, claims) => Outcome::Prune(Box::new(
                state
                    .begin_prune_validating(collector, claims.clone())
                    .await?,
            )),
            Self::FinishPrune(token) => {
                state.finish_prune(token).await?;
                Outcome::Finished
            }
            Self::Claim(revision, resources) => {
                Outcome::Token(state.claim_deletions(*revision, resources.clone()).await?)
            }
            Self::ClaimValidated(collector, resources, checked_blobs) => Outcome::Token(
                state
                    .claim_deletions_validated(collector, resources.clone(), checked_blobs.clone())
                    .await?,
            ),
            Self::ClaimDuringPrune(revision, resources, collector, prune) => Outcome::Token(
                state
                    .claim_deletions_during_prune(*revision, resources.clone(), collector, prune)
                    .await?,
            ),
            Self::FinishDeletion(token) => {
                state.finish_deletions(token).await?;
                Outcome::Finished
            }
        })
    }
}

async fn edit(store: &impl Backend, operation: Operation) -> Result<Outcome, MetadataError> {
    store.edit(operation).await
}

const EDIT_ATTEMPTS: u32 = 32;

fn edit_backoff(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_millis(1 << attempt.min(6))
}

/// One load, apply and conditional store. `None` means another writer moved
/// the ledger first and the operation must be re-applied to the new state.
async fn attempt_edit(
    store: &(impl Backend + ?Sized),
    operation: &Operation,
) -> Result<Option<Outcome>, MetadataError> {
    let (before, version) = store.load().await?;
    let revision = before.revision;
    let memory = MemoryPinStore {
        state: Arc::new(tokio::sync::Mutex::new(before)),
    };
    let result = operation.apply(&memory).await?;
    let after = memory.inventory().await?;
    if after.revision == revision || store.compare_exchange(version, after).await? {
        return Ok(Some(result));
    }
    Ok(None)
}

async fn optimistic_edit(
    store: &(impl Backend + ?Sized),
    operation: Operation,
) -> Result<Outcome, MetadataError> {
    for attempt in 0..EDIT_ATTEMPTS {
        if let Some(result) = attempt_edit(store, &operation).await? {
            return Ok(result);
        }
        tokio::time::sleep(edit_backoff(attempt)).await;
    }
    Err(MetadataError::Transient(
        "pin ledger remained contended".into(),
    ))
}

#[async_trait]
impl<T: Backend> PinStore for T {
    async fn begin_collection(
        &self,
        revision: u64,
        previous: Option<PinToken>,
    ) -> Result<Option<PinToken>, MetadataError> {
        match edit(self, Operation::BeginCollection(revision, previous)).await? {
            Outcome::Token(token) => Ok(token),
            _ => unreachable!(),
        }
    }
    async fn acquire_collection(
        &self,
        previous: Option<PinToken>,
    ) -> Result<Option<PinToken>, MetadataError> {
        match edit(self, Operation::AcquireCollection(previous)).await? {
            Outcome::Token(token) => Ok(token),
            _ => unreachable!(),
        }
    }
    async fn finish_collection(&self, token: &PinToken) -> Result<(), MetadataError> {
        edit(self, Operation::FinishCollection(token.clone())).await?;
        Ok(())
    }
    async fn inventory(&self) -> Result<PinInventory, MetadataError> {
        Ok(self.load().await?.0)
    }
    async fn register(&self, pin: DataPin) -> Result<Option<PinToken>, MetadataError> {
        match edit(self, Operation::Register(pin)).await? {
            Outcome::Token(token) => Ok(token),
            _ => unreachable!(),
        }
    }
    async fn register_reader(&self, pin: DataPin) -> Result<Option<PinToken>, MetadataError> {
        match Backend::admit_reader(self, pin).await? {
            Outcome::Token(token) => Ok(token),
            _ => unreachable!(),
        }
    }
    async fn protect(
        &self,
        token: &PinToken,
        resources: BTreeSet<PinResource>,
    ) -> Result<bool, MetadataError> {
        match edit(self, Operation::Protect(token.clone(), resources)).await? {
            Outcome::Protected(protected) => Ok(protected),
            _ => unreachable!(),
        }
    }
    async fn release(&self, token: &PinToken) -> Result<(), MetadataError> {
        edit(self, Operation::Release(token.clone())).await?;
        Ok(())
    }
    async fn begin_prune(&self, revision: u64) -> Result<Option<PinToken>, MetadataError> {
        self.begin_prune_recovering(revision, BTreeSet::new()).await
    }
    async fn begin_prune_recovering(
        &self,
        revision: u64,
        claims: BTreeSet<PinToken>,
    ) -> Result<Option<PinToken>, MetadataError> {
        match edit(self, Operation::BeginPrune(revision, claims)).await? {
            Outcome::Token(token) => Ok(token),
            _ => unreachable!(),
        }
    }
    async fn begin_prune_validating(
        &self,
        collector: &PinToken,
        claims: BTreeSet<PinToken>,
    ) -> Result<Option<(PinToken, PinInventory)>, MetadataError> {
        match edit(
            self,
            Operation::BeginPruneValidating(collector.clone(), claims),
        )
        .await?
        {
            Outcome::Prune(admission) => Ok(*admission),
            _ => unreachable!(),
        }
    }
    async fn finish_prune(&self, token: &PinToken) -> Result<(), MetadataError> {
        edit(self, Operation::FinishPrune(token.clone())).await?;
        Ok(())
    }
    async fn claim_deletions(
        &self,
        revision: u64,
        resources: BTreeSet<PinResource>,
    ) -> Result<Option<PinToken>, MetadataError> {
        match edit(self, Operation::Claim(revision, resources)).await? {
            Outcome::Token(token) => Ok(token),
            _ => unreachable!(),
        }
    }
    async fn claim_deletions_validated(
        &self,
        collector: &PinToken,
        resources: BTreeSet<PinResource>,
        checked_blobs: BTreeSet<BlobId>,
    ) -> Result<Option<PinToken>, MetadataError> {
        match edit(
            self,
            Operation::ClaimValidated(collector.clone(), resources, checked_blobs),
        )
        .await?
        {
            Outcome::Token(token) => Ok(token),
            _ => unreachable!(),
        }
    }
    async fn claim_deletions_during_prune(
        &self,
        revision: u64,
        resources: BTreeSet<PinResource>,
        collector: &PinToken,
        prune: &PinToken,
    ) -> Result<Option<PinToken>, MetadataError> {
        match edit(
            self,
            Operation::ClaimDuringPrune(revision, resources, collector.clone(), prune.clone()),
        )
        .await?
        {
            Outcome::Token(token) => Ok(token),
            _ => unreachable!(),
        }
    }

    async fn finish_deletions(&self, token: &PinToken) -> Result<(), MetadataError> {
        edit(self, Operation::FinishDeletion(token.clone())).await?;
        Ok(())
    }
}

#[cfg(feature = "s3")]
pub(crate) fn chroma_pin_store(
    storage: Arc<chroma_storage::Storage>,
    path: String,
) -> Arc<dyn PinStore> {
    let edits = remote_edits(Arc::as_ptr(&storage) as usize, &path);
    Arc::new(ChromaPinStore {
        storage,
        path,
        edits,
    })
}

// Independently requested pin handles on one storage client must share their
// edit queue. CAS still arbitrates with other clients/processes; this queue
// prevents our own chunk uploads from exhausting each other's retry budgets.
#[cfg(any(feature = "s3", test))]
fn remote_edits(client: usize, path: &str) -> Arc<tokio::sync::Mutex<()>> {
    type Queues =
        std::collections::HashMap<(usize, String), std::sync::Weak<tokio::sync::Mutex<()>>>;
    static QUEUES: std::sync::OnceLock<std::sync::Mutex<Queues>> = std::sync::OnceLock::new();
    let mut queues = QUEUES.get_or_init(Default::default).lock().unwrap();
    queues.retain(|_, queue| queue.strong_count() != 0);
    let slot = queues.entry((client, path.into())).or_default();
    slot.upgrade().unwrap_or_else(|| {
        let queue = Arc::new(tokio::sync::Mutex::new(()));
        *slot = Arc::downgrade(&queue);
        queue
    })
}

// The queue covers one load-apply-store round, never the backoff between
// rounds: an edit that keeps losing to another process must not stall every
// unrelated edit on this client behind its sleeps.
#[cfg(any(feature = "s3", test))]
async fn queued_remote_edit(
    store: &impl Backend,
    queue: &tokio::sync::Mutex<()>,
    operation: Operation,
) -> Result<Outcome, MetadataError> {
    for attempt in 0..EDIT_ATTEMPTS {
        let guard = queue.lock().await;
        let result = attempt_edit(store, &operation).await?;
        drop(guard);
        if let Some(result) = result {
            return Ok(result);
        }
        tokio::time::sleep(edit_backoff(attempt)).await;
    }
    Err(MetadataError::Transient(
        "pin ledger remained contended".into(),
    ))
}

/// Uses the same storage and prefix identity as the parent WAL3 metadata store,
/// including custom Chroma storage configurations and credentials.
#[cfg(feature = "s3")]
struct ChromaPinStore {
    storage: Arc<chroma_storage::Storage>,
    path: String,
    edits: Arc<tokio::sync::Mutex<()>>,
}

#[cfg(feature = "s3")]
#[async_trait]
impl Backend for ChromaPinStore {
    type Version = Option<chroma_storage::ETag>;

    async fn edit(&self, operation: Operation) -> Result<Outcome, MetadataError> {
        queued_remote_edit(self, &self.edits, operation).await
    }

    async fn load(&self) -> Result<(PinInventory, Self::Version), MetadataError> {
        use chroma_storage::{GetOptions, StorageError};
        match Box::pin(
            self.storage
                .get_with_e_tag(&self.path, GetOptions::default().with_strong_consistency()),
        )
        .await
        {
            Ok((bytes, Some(etag))) => Ok((codec::decode(&bytes)?, Some(etag))),
            Ok((_, None)) => Err(backend("conditional pin writes require an ETag")),
            Err(StorageError::NotFound { .. }) => Ok((PinInventory::default(), None)),
            Err(error) => Err(backend(error)),
        }
    }

    async fn compare_exchange(
        &self,
        expected: Self::Version,
        state: PinInventory,
    ) -> Result<bool, MetadataError> {
        use chroma_storage::{PutMode, PutOptions, StorageError};
        let mode = expected.map_or(PutMode::IfNotExist, PutMode::IfMatch);
        let options = PutOptions::default().with_mode(mode);
        match Box::pin(
            self.storage
                .put_bytes(&self.path, codec::encode(&state)?, options),
        )
        .await
        {
            Ok(_) => Ok(true),
            Err(StorageError::AlreadyExists { .. } | StorageError::Precondition { .. }) => {
                Ok(false)
            }
            Err(error) => Err(backend(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn staging(path: &str) -> DataPin {
        DataPin {
            scope: PinScope::Staging,
            catalog: None,
            resources: resources(path),
        }
    }
    fn resources(path: &str) -> BTreeSet<PinResource> {
        BTreeSet::from([PinResource::StorageObject(path.into())])
    }
    fn object_store() -> ObjectPinStore {
        ObjectPinStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            "pins".into(),
        )
    }

    struct QueuedBackend {
        inner: ObjectPinStore,
        queue: Arc<tokio::sync::Mutex<()>>,
        conflicts: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Backend for QueuedBackend {
        type Version = Option<UpdateVersion>;

        async fn edit(&self, operation: Operation) -> Result<Outcome, MetadataError> {
            queued_remote_edit(self, &self.queue, operation).await
        }

        async fn load(&self) -> Result<(PinInventory, Self::Version), MetadataError> {
            let state = self.inner.load().await?;
            tokio::task::yield_now().await;
            Ok(state)
        }

        async fn compare_exchange(
            &self,
            expected: Self::Version,
            state: PinInventory,
        ) -> Result<bool, MetadataError> {
            let won = self.inner.compare_exchange(expected, state).await?;
            if !won {
                self.conflicts.fetch_add(1, Ordering::SeqCst);
            }
            Ok(won)
        }
    }

    /// A ledger where every write that protects `stuck` loses its conditional
    /// store, as if another process kept winning the same CAS.
    struct StuckBackend {
        inner: ObjectPinStore,
        queue: Arc<tokio::sync::Mutex<()>>,
    }

    #[async_trait]
    impl Backend for StuckBackend {
        type Version = Option<UpdateVersion>;

        async fn edit(&self, operation: Operation) -> Result<Outcome, MetadataError> {
            queued_remote_edit(self, &self.queue, operation).await
        }

        async fn load(&self) -> Result<(PinInventory, Self::Version), MetadataError> {
            self.inner.load().await
        }

        async fn compare_exchange(
            &self,
            expected: Self::Version,
            state: PinInventory,
        ) -> Result<bool, MetadataError> {
            let stuck = state.pins.values().any(|pin| {
                pin.resources
                    .contains(&PinResource::StorageObject("stuck".into()))
            });
            if stuck {
                return Ok(false);
            }
            self.inner.compare_exchange(expected, state).await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn queued_edit_releases_the_queue_while_backing_off() {
        let inner = object_store();
        let queue = Arc::new(tokio::sync::Mutex::new(()));
        let stuck = Arc::new(StuckBackend {
            inner: inner.clone(),
            queue: queue.clone(),
        });
        let free = StuckBackend {
            inner: inner.clone(),
            queue,
        };
        let losing = tokio::spawn({
            let stuck = stuck.clone();
            async move { stuck.register(staging("stuck")).await }
        });
        tokio::task::yield_now().await;
        // The unrelated edit completes while the losing one is still retrying.
        let token = free.register(staging("free")).await.unwrap().unwrap();
        assert!(!losing.is_finished());
        assert!(inner.inventory().await.unwrap().pins.contains_key(&token));
        assert!(matches!(
            losing.await.unwrap(),
            Err(MetadataError::Transient(_))
        ));
    }

    #[tokio::test]
    async fn remote_handles_queue_local_edits_without_cas_amplification() {
        let inner = object_store();
        let identity = Arc::new(());
        let client = Arc::as_ptr(&identity) as usize;
        let conflicts = Arc::new(AtomicUsize::new(0));
        let handles: Vec<_> = (0..64)
            .map(|_| QueuedBackend {
                inner: inner.clone(),
                queue: remote_edits(client, "pins"),
                conflicts: conflicts.clone(),
            })
            .collect();
        assert!(Arc::ptr_eq(&handles[0].queue, &handles[63].queue));
        assert!(!Arc::ptr_eq(
            &handles[0].queue,
            &remote_edits(client, "other")
        ));
        let tokens = futures::future::join_all(handles.iter().enumerate().map(
            |(index, store)| async move {
                store
                    .register(staging(&format!("object-{index}")))
                    .await
                    .unwrap()
                    .unwrap()
            },
        ))
        .await;
        let inventory = inner.inventory().await.unwrap();
        assert_eq!(inventory.pins.len(), tokens.len());
        for token in &tokens {
            assert!(inventory.pins.contains_key(token));
        }
        for result in futures::future::join_all(
            handles
                .iter()
                .zip(&tokens)
                .map(|(store, token)| store.release(token)),
        )
        .await
        {
            result.unwrap();
        }
        assert!(inner.inventory().await.unwrap().pins.is_empty());
        assert_eq!(conflicts.load(Ordering::SeqCst), 0);

        // Cancelling an edit waiting for the local queue must leave the next
        // edit able to proceed, without creating an unowned pin.
        let store = Arc::new(handles.into_iter().next().unwrap());
        let guard = store.queue.lock().await;
        let waiting = store.clone();
        let task = tokio::spawn(async move { waiting.register(staging("cancelled")).await });
        tokio::task::yield_now().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        drop(guard);
        store.register(staging("next")).await.unwrap().unwrap();
        let inventory = inner.inventory().await.unwrap();
        assert_eq!(inventory.pins.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn independent_file_handles_preserve_concurrent_pin_updates() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("pins");
        let tokens = futures::future::join_all((0..40).map(|index| {
            let store = FilePinStore::new(&path);
            async move {
                let token = store
                    .register(staging(&format!("initial-{index}")))
                    .await
                    .unwrap()
                    .unwrap();
                assert!(
                    store
                        .protect(&token, resources(&format!("extra-{index}")))
                        .await
                        .unwrap()
                );
                (index, token)
            }
        }))
        .await;
        let inventory = FilePinStore::new(&path).inventory().await.unwrap();
        assert_eq!(inventory.pins.len(), tokens.len());
        for (index, token) in &tokens {
            let pin = &inventory.pins[token];
            assert!(
                pin.resources
                    .is_superset(&resources(&format!("initial-{index}")))
            );
            assert!(
                pin.resources
                    .is_superset(&resources(&format!("extra-{index}")))
            );
        }
        futures::future::join_all(tokens.into_iter().map(|(_, token)| {
            let store = FilePinStore::new(&path);
            async move {
                store.release(&token).await.unwrap();
            }
        }))
        .await;
        assert!(
            FilePinStore::new(&path)
                .inventory()
                .await
                .unwrap()
                .pins
                .is_empty()
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn file_slots_reuse_inodes_and_ignore_an_interrupted_spare_write() {
        use std::os::unix::fs::MetadataExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("pins");
        let spare = directory.path().join("pins.spare");
        let store = FilePinStore::new(&path);
        let first = store.register(staging("first")).await.unwrap().unwrap();
        let active_inode = std::fs::metadata(&path).unwrap().ino();
        let spare_inode = std::fs::metadata(&spare).unwrap().ino();
        let capacity = std::fs::metadata(&path).unwrap().len();
        assert!(capacity >= 128 * 1024);
        assert_eq!(std::fs::metadata(&spare).unwrap().len(), capacity);
        let mut interrupted = std::fs::File::options().write(true).open(&spare).unwrap();
        interrupted.write_all(b"interrupted replacement").unwrap();
        interrupted.sync_all().unwrap();
        drop(interrupted);
        assert!(store.inventory().await.unwrap().pins.contains_key(&first));
        let second = store.register(staging("second")).await.unwrap().unwrap();
        // Ordinary updates append in place. A checkpoint exchanges the same
        // allocated inodes and overwrites the interrupted spare contents.
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), active_inode);
        let state = store.inventory().await.unwrap();
        store.write_locked(&codec::encode(&state).unwrap()).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), spare_inode);
        assert_eq!(std::fs::metadata(&spare).unwrap().ino(), active_inode);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), capacity);
        assert_eq!(std::fs::metadata(&spare).unwrap().len(), capacity);
        let inventory = FilePinStore::new(&path).inventory().await.unwrap();
        assert!(inventory.pins.contains_key(&first) && inventory.pins.contains_key(&second));
        // Corrupting the authoritative file must fail closed, never silently
        // resurrecting an older ledger from the spare inode.
        let mut active = std::fs::File::options().write(true).open(&path).unwrap();
        active.write_all(b"corrupt active").unwrap();
        active.sync_all().unwrap();
        assert!(store.inventory().await.is_err());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn file_slots_migrate_the_existing_ledger_without_losing_pins() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("pins");
        let memory = MemoryPinStore::default();
        let first = memory.register(staging("old")).await.unwrap().unwrap();
        std::fs::write(
            &path,
            codec::encode(&memory.inventory().await.unwrap()).unwrap(),
        )
        .unwrap();
        let store = FilePinStore::new(path);
        let second = store.register(staging("new")).await.unwrap().unwrap();
        let inventory = store.inventory().await.unwrap();
        assert_eq!(inventory.pins.len(), 2);
        assert!(inventory.pins.contains_key(&first) && inventory.pins.contains_key(&second));
    }

    struct RacingBackend {
        inner: ObjectPinStore,
        loaded: tokio::sync::Barrier,
        reads: AtomicUsize,
        conflicts: AtomicUsize,
    }

    #[async_trait]
    impl Backend for RacingBackend {
        type Version = Option<UpdateVersion>;
        async fn load(&self) -> Result<(PinInventory, Self::Version), MetadataError> {
            let snapshot = self.inner.load().await?;
            if self.reads.fetch_add(1, Ordering::SeqCst) < 2 {
                self.loaded.wait().await;
            }
            Ok(snapshot)
        }
        async fn compare_exchange(
            &self,
            expected: Self::Version,
            state: PinInventory,
        ) -> Result<bool, MetadataError> {
            let won = self.inner.compare_exchange(expected, state).await?;
            if !won {
                self.conflicts.fetch_add(1, Ordering::SeqCst);
            }
            Ok(won)
        }
    }

    #[tokio::test]
    async fn conditional_write_loser_reloads_and_preserves_the_winner() {
        let backend = RacingBackend {
            inner: object_store(),
            loaded: tokio::sync::Barrier::new(2),
            reads: AtomicUsize::new(0),
            conflicts: AtomicUsize::new(0),
        };
        let (first, second) = tokio::join!(
            backend.register(staging("first")),
            backend.register(staging("second"))
        );
        let first = first.unwrap().unwrap();
        let second = second.unwrap().unwrap();
        assert_eq!(backend.conflicts.load(Ordering::SeqCst), 1);
        let inventory = backend.inventory().await.unwrap();
        assert_eq!(inventory.pins.len(), 2);
        assert!(inventory.pins.contains_key(&first) && inventory.pins.contains_key(&second));
    }

    struct UncertainBackend {
        inner: ObjectPinStore,
        entered: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
        settled: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl Backend for UncertainBackend {
        type Version = Option<UpdateVersion>;
        async fn load(&self) -> Result<(PinInventory, Self::Version), MetadataError> {
            self.inner.load().await
        }
        async fn compare_exchange(
            &self,
            expected: Self::Version,
            state: PinInventory,
        ) -> Result<bool, MetadataError> {
            let inner = self.inner.clone();
            let resume = self.resume.clone();
            let settled = self.settled.clone();
            let (send, receive) = tokio::sync::oneshot::channel();
            // Model a submitted remote request whose server-side commit
            // survives cancellation of the caller's receive future.
            tokio::spawn(async move {
                resume.notified().await;
                let result = inner.compare_exchange(expected, state).await;
                let _ = send.send(result);
                settled.notify_one();
            });
            self.entered.notify_one();
            receive.await.map_err(backend)?
        }
    }

    #[tokio::test]
    async fn cancelled_claim_keeps_protection_when_remote_commit_finishes_late() {
        let remote = object_store();
        let backend = Arc::new(UncertainBackend {
            inner: remote.clone(),
            entered: Arc::new(tokio::sync::Notify::new()),
            resume: Arc::new(tokio::sync::Notify::new()),
            settled: Arc::new(tokio::sync::Notify::new()),
        });
        let task = {
            let backend = backend.clone();
            tokio::spawn(async move { backend.claim_deletions(0, resources("late-delete")).await })
        };
        backend.entered.notified().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        backend.resume.notify_one();
        backend.settled.notified().await;
        let inventory = remote.inventory().await.unwrap();
        assert_eq!(inventory.deletions.len(), 1);
        assert!(
            remote
                .register(staging("late-delete"))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            remote
                .register(staging("unrelated"))
                .await
                .unwrap()
                .is_some()
        );
        let token = inventory.deletions.keys().next().unwrap();
        remote.finish_deletions(token).await.unwrap();
        assert!(
            remote
                .register(staging("late-delete"))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn cancelled_runtime_admission_releases_its_late_pin() {
        let remote = object_store();
        let backend = Arc::new(UncertainBackend {
            inner: remote.clone(),
            entered: Arc::new(tokio::sync::Notify::new()),
            resume: Arc::new(tokio::sync::Notify::new()),
            settled: Arc::new(tokio::sync::Notify::new()),
        });
        let acquiring = {
            let backend = backend.clone();
            tokio::spawn(
                async move { DataPinLease::try_acquire(backend, staging("cancelled")).await },
            )
        };
        backend.entered.notified().await;
        acquiring.abort();
        assert!(acquiring.await.is_err());
        backend.resume.notify_one();
        // The cancelled receive drops the newly acquired pin and submits its
        // exact-token release. The drain must include that second request.
        backend.entered.notified().await;
        backend.resume.notify_one();
        crate::metadata::flush_repository_leases().await.unwrap();
        assert!(remote.inventory().await.unwrap().pins.is_empty());
    }
}
