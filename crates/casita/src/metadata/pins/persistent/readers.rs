//! Process-owned read protection. The durable ledger fences older collectors
//! and reserves a revision range; the complete volatile reader inventory is
//! atomically renamed under the same ledger lock, without a per-read fsync.
//! A kernel lock, never a PID or a timeout, establishes owner liveness.
use super::*;
use std::sync::{Mutex, OnceLock, Weak};

pub(super) const RESERVATION: u64 = 65_536;
const MAGIC: &[u8; 8] = b"CASREAD1";

pub(super) struct ReaderOwner {
    token: PinToken,
    process: u32,
    _lock: std::fs::File,
}

#[derive(Clone, Default)]
pub(super) struct ReaderState {
    pub(super) revision: u64,
    pub(super) owners: BTreeMap<PinToken, PinInventory>,
}

#[cfg(unix)]
#[derive(PartialEq, Eq)]
struct ReaderFileIdentity {
    device: u64,
    inode: u64,
    length: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}

#[cfg(unix)]
impl ReaderFileIdentity {
    fn of(file: &std::fs::File) -> Result<Self, MetadataError> {
        use std::os::unix::fs::MetadataExt;
        let metadata = file.metadata().map_err(backend)?;
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            length: metadata.len(),
            modified: (metadata.mtime(), metadata.mtime_nsec()),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }
}

#[cfg(unix)]
pub(super) struct CachedReaders {
    // Keep the inode alive so replacement cannot recycle its identity.
    _file: std::fs::File,
    identity: ReaderFileIdentity,
    state: ReaderState,
}

fn registry() -> &'static Mutex<BTreeMap<PathBuf, Weak<ReaderOwner>>> {
    static OWNERS: OnceLock<Mutex<BTreeMap<PathBuf, Weak<ReaderOwner>>>> = OnceLock::new();
    OWNERS.get_or_init(Default::default)
}

#[cfg(test)]
pub(super) struct TestPause {
    phase: &'static str,
    entered: std::sync::mpsc::Sender<()>,
    resume: std::sync::mpsc::Receiver<()>,
}

impl FilePinStore {
    #[cfg(test)]
    pub(super) fn checkpoint(&self, phase: &'static str) {
        let pause = {
            let mut pending = self.reader_pause.lock().unwrap();
            if pending.as_ref().is_some_and(|pause| pause.phase == phase) {
                pending.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            let _ = pause.entered.send(());
            let _ = pause.resume.recv();
        }
        if std::env::var("CASITA_READER_CRASH_PHASE").is_ok_and(|target| target == phase) {
            let signal = std::env::var_os("CASITA_READER_CRASH_SIGNAL").expect("crash signal path");
            std::fs::write(signal, phase).unwrap();
            loop {
                std::thread::park();
            }
        }
    }
    fn reader_path(&self) -> PathBuf {
        let mut path = self.path.as_os_str().to_os_string();
        path.push(".readers");
        path.into()
    }

    fn owner_path(&self, token: &PinToken) -> PathBuf {
        let mut path = self.path.as_os_str().to_os_string();
        path.push(format!(".reader-{token}.lock"));
        path.into()
    }

    fn owner_alive(&self, token: &PinToken) -> Result<bool, MetadataError> {
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .open(self.owner_path(token))
            .map_err(backend)?;
        match file.try_lock() {
            Ok(()) => {
                file.unlock().map_err(backend)?;
                Ok(false)
            }
            Err(std::fs::TryLockError::WouldBlock) => Ok(true),
            Err(std::fs::TryLockError::Error(error)) => Err(backend(error)),
        }
    }

    pub(super) fn live_owners(
        &self,
        durable: &PinInventory,
    ) -> Result<BTreeSet<PinToken>, MetadataError> {
        durable
            .reader_owners
            .iter()
            .filter_map(|owner| match self.owner_alive(owner) {
                Ok(true) => Some(Ok(owner.clone())),
                Ok(false) => None,
                Err(error) => Some(Err(error)),
            })
            .collect()
    }

    pub(super) fn write_readers(&self, state: &ReaderState) -> Result<(), MetadataError> {
        let bytes = state.encode()?;
        let mut temporary = tempfile::NamedTempFile::new_in(self.parent()).map_err(backend)?;
        temporary.write_all(&bytes).map_err(backend)?;
        // No sync: every visible reader is protected by a live kernel owner.
        // A process crash cannot tear a completed rename; after host failure
        // no owner survives and the durable revision ceiling fences old clocks.
        #[cfg(test)]
        self.checkpoint("reader-before-rename");
        let published = temporary.persist(self.reader_path()).map_err(backend)?;
        #[cfg(unix)]
        {
            // Failure to populate an optimization must not turn a published
            // transition into an apparent failed write. The next read will
            // validate the file normally if metadata could not be obtained.
            let cached = ReaderFileIdentity::of(&published)
                .ok()
                .and_then(|identity| {
                    state.valid_reader_records().then(|| CachedReaders {
                        _file: published,
                        identity,
                        state: state.clone(),
                    })
                });
            if let Ok(local) = self.local()
                && let Ok(mut cache) = local.reader_cache.lock()
            {
                *cache = cached;
            }
        }
        #[cfg(not(unix))]
        drop(published);
        #[cfg(test)]
        self.checkpoint("reader-after-rename");
        Ok(())
    }

    pub(super) fn read_reader_inventory(&self) -> Result<ReaderState, MetadataError> {
        // Callers hold the ledger lock. Cooperating publishers therefore cannot
        // replace this file between the identity check and using cached state.
        let file = std::fs::File::open(self.reader_path()).map_err(backend)?;
        #[cfg(unix)]
        let identity = ReaderFileIdentity::of(&file)?;
        #[cfg(unix)]
        {
            if let Ok(local) = self.local()
                && let Ok(cache) = local.reader_cache.lock()
                && let Some(cached) = cache.as_ref().filter(|cached| cached.identity == identity)
            {
                return Ok(cached.state.clone());
            }
        }
        if file.metadata().map_err(backend)?.len() > codec::MAX_BYTES as u64 {
            return Err(backend("oversized reader inventory"));
        }
        let mut bytes = Vec::new();
        (&file)
            .take(codec::MAX_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(backend)?;
        let state = ReaderState::decode(&bytes)?;
        #[cfg(unix)]
        {
            if let Ok(local) = self.local()
                && let Ok(mut cache) = local.reader_cache.lock()
            {
                *cache = Some(CachedReaders {
                    _file: file,
                    identity,
                    state: state.clone(),
                });
            }
        }
        Ok(state)
    }

    pub(super) fn readers_locked(
        &self,
        durable: &mut PinInventory,
    ) -> Result<ReaderState, MetadataError> {
        if durable.reader_owners.is_empty() {
            // Clearing the last durable owner fences every revision it could
            // have used. Orphan sidecar bytes are now irrelevant. In particular,
            // collection must not need a new sidecar allocation on a full disk.
            return Ok(ReaderState {
                revision: durable.revision,
                ..Default::default()
            });
        }
        let loaded = self.read_reader_inventory();
        let live = self.live_owners(durable)?;
        if let Ok(state) = loaded {
            if state.revision > durable.reader_revision_ceiling {
                return Err(backend("reader revision exceeds its durable reservation"));
            }
            if !live.is_empty() {
                return Ok(state);
            }
            // A host crash may leave an intact but older file. Once the last
            // registered owner is gone, retire its entire reserved clock even
            // if this version decodes successfully.
        }
        // Lost/unsynced state may be reset only if no process can still use it.
        // Advance past the entire reserved range, not merely the last durable
        // revision, so a stale collector cannot pass an ABA revision check.
        if !live.is_empty() {
            return Err(backend(
                "live reader owner has unavailable reader inventory",
            ));
        }
        durable.revision = durable
            .revision
            .max(durable.reader_revision_ceiling)
            .checked_add(1)
            .ok_or_else(|| backend("pin revision exhausted"))?;
        durable.reader_revision_ceiling = durable
            .revision
            .checked_add(RESERVATION)
            .ok_or_else(|| backend("pin revision exhausted"))?;
        durable.reader_owners.clear();
        self.persist_inventory(durable)?;
        let state = ReaderState {
            revision: durable.revision,
            ..Default::default()
        };
        // No live reader can consume this state. The next owner writes its
        // complete inventory before admission; GC needs only the durable fence.
        Ok(state)
    }

    pub(super) fn merge_readers(
        &self,
        durable: &PinInventory,
        readers: &ReaderState,
    ) -> Result<PinInventory, MetadataError> {
        let mut result = durable.clone();
        result.revision = result.revision.max(readers.revision);
        for owner in self.live_owners(durable)? {
            let pins = readers
                .owners
                .get(&owner)
                .ok_or_else(|| backend("live reader owner is missing from inventory"))?;
            for (token, pin) in &pins.pins {
                if result.pins.insert(token.clone(), pin.clone()).is_some() {
                    return Err(backend("duplicate process reader token"));
                }
            }
        }
        Ok(result)
    }

    fn reserve(&self, durable: &mut PinInventory, next: u64) -> Result<(), MetadataError> {
        if next > durable.reader_revision_ceiling {
            durable.revision = next;
            durable.reader_revision_ceiling = next
                .checked_add(RESERVATION)
                .ok_or_else(|| backend("pin revision exhausted"))?;
            self.persist_inventory(durable)?;
        }
        Ok(())
    }

    fn owner(&self) -> Result<Arc<ReaderOwner>, MetadataError> {
        let mut cached = self.reader_owner.lock().unwrap();
        if let Some(owner) = cached
            .as_ref()
            .filter(|owner| owner.process == std::process::id())
        {
            return Ok(owner.clone());
        }
        *cached = None;
        // Canonical parent identity unifies independent handles and symlinks.
        std::fs::create_dir_all(self.parent()).map_err(backend)?;
        let key = self.parent().canonicalize().map_err(backend)?.join(
            self.path
                .file_name()
                .ok_or_else(|| backend("missing inventory filename"))?,
        );
        let mut registry = registry().lock().unwrap();
        registry.retain(|_, value| value.strong_count() != 0);
        if let Some(owner) = registry
            .get(&key)
            .and_then(Weak::upgrade)
            .filter(|owner| owner.process == std::process::id())
        {
            *cached = Some(owner.clone());
            return Ok(owner);
        }
        let lock = self.lock_file()?;
        lock.lock().map_err(backend)?;
        let result: Result<Arc<ReaderOwner>, MetadataError> = (|| {
            let mut durable = self.read_locked()?;
            let mut readers = self.readers_locked(&mut durable)?;
            let token = PinToken::fresh()?;
            let owner_lock = std::fs::File::options()
                .create_new(true)
                .read(true)
                .write(true)
                .open(self.owner_path(&token))
                .map_err(backend)?;
            owner_lock.lock().map_err(backend)?;
            owner_lock.sync_all().map_err(backend)?;
            #[cfg(test)]
            self.checkpoint("owner-locked");
            let owner = Arc::new(ReaderOwner {
                token: token.clone(),
                process: std::process::id(),
                _lock: owner_lock,
            });
            durable.reader_owners = self.live_owners(&durable)?;
            readers
                .owners
                .retain(|id, _| durable.reader_owners.contains(id));
            durable.revision = durable
                .revision
                .max(readers.revision)
                .checked_add(1)
                .ok_or_else(|| backend("pin revision exhausted"))?;
            durable.reader_revision_ceiling = durable.reader_revision_ceiling.max(
                durable
                    .revision
                    .checked_add(RESERVATION)
                    .ok_or_else(|| backend("pin revision exhausted"))?,
            );
            durable.reader_owners.insert(token.clone());
            readers.revision = durable.revision;
            readers.owners.insert(token, PinInventory::default());
            // Publish the durable fence/reservation before volatile state.
            // If the owner dies in between, its kernel lock is gone and its
            // missing (never-exposed) reader entry is ignored by collectors.
            self.persist_inventory(&durable)?;
            #[cfg(test)]
            self.checkpoint("owner-fenced");
            self.write_readers(&readers)?;
            Ok(owner)
        })();
        lock.unlock().map_err(backend)?;
        let owner = result?;
        registry.insert(key, Arc::downgrade(&owner));
        *cached = Some(owner.clone());
        Ok(owner)
    }

    pub(super) fn register_local_reader(&self, pin: DataPin) -> Result<Outcome, MetadataError> {
        let owner = self.owner()?;
        let lock = self.lock_file()?;
        lock.lock().map_err(backend)?;
        let result = (|| {
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            if let Some(result) = self.register_cached_reader(&pin, &owner.token)? {
                return Ok(result);
            }
            let mut durable = self.read_locked()?;
            let mut readers = self.readers_locked(&mut durable)?;
            let before = self.merge_readers(&durable, &readers)?;
            if before.logical_prune.is_some() || before.deleting(&pin.resources) {
                return Ok(Outcome::Token(None));
            }
            pin.validate()?;
            let token = PinToken::fresh()?;
            let next = before
                .revision
                .checked_add(1)
                .ok_or_else(|| backend("pin revision exhausted"))?;
            self.reserve(&mut durable, next)?;
            readers.revision = next;
            readers
                .owners
                .get_mut(&owner.token)
                .ok_or_else(|| backend("reader owner was fenced"))?
                .pins
                .insert(token.clone(), pin);
            self.write_readers(&readers)?;
            #[cfg(test)]
            self.checkpoint("reader-registered");
            Ok(Outcome::Token(Some(token)))
        })();
        lock.unlock().map_err(backend)?;
        result
    }

    pub(super) fn edit_with_readers(
        &self,
        operation: Operation,
        defer: bool,
    ) -> Result<Outcome, MetadataError> {
        let mut durable = self.read_locked()?;
        let mut readers = self.readers_locked(&mut durable)?;
        let before = self.merge_readers(&durable, &readers)?;
        let reader_token = match &operation {
            Operation::Protect(token, _) | Operation::Release(token) => Some(token),
            _ => None,
        };
        if let Some(token) = reader_token {
            let owner = readers.owners.iter().find_map(|(owner, state)| {
                (durable.reader_owners.contains(owner)
                    && state.pins.contains_key(token)
                    && before.pins.contains_key(token))
                .then_some(owner.clone())
            });
            if let Some(owner) = owner {
                let state = readers.owners.get_mut(&owner).unwrap();
                match &operation {
                    Operation::Protect(_, resources) => {
                        if before.deleting(resources) {
                            return Ok(Outcome::Protected(false));
                        }
                        let pin = state.pins.get_mut(token).unwrap();
                        if resources.is_subset(&pin.resources) {
                            return Ok(Outcome::Protected(true));
                        }
                        pin.resources.extend(resources.iter().cloned());
                    }
                    Operation::Release(_) => {
                        // Read pins cannot publish new durable data. Once the
                        // last reader relinquishes them, no retired write
                        // history is required for the active collector.
                        state.pins.remove(token);
                    }
                    _ => unreachable!(),
                }
                let next = before
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| backend("pin revision exhausted"))?;
                self.reserve(&mut durable, next)?;
                readers.revision = next;
                self.write_readers(&readers)?;
                #[cfg(test)]
                self.checkpoint(if matches!(operation, Operation::Protect(..)) {
                    "reader-protected"
                } else {
                    "reader-released"
                });
                return Ok(if matches!(operation, Operation::Protect(..)) {
                    Outcome::Protected(true)
                } else {
                    Outcome::Finished
                });
            }
        }
        let revision = before.revision;
        let memory = MemoryPinStore {
            state: Arc::new(tokio::sync::Mutex::new(before)),
            entropy: system_entropy(),
        };
        let (result, mut after) = futures::executor::block_on(async {
            let result = operation.apply(&memory).await?;
            Ok::<_, MetadataError>((result, std::mem::take(&mut *memory.state.lock().await)))
        })?;
        if after.revision != revision {
            // Only recovery records enter the durable inventory. The visible
            // revision includes reader transitions, so every prune/claim still
            // validates the exact combined state it marked.
            for state in readers.owners.values() {
                for token in state.pins.keys() {
                    after.pins.remove(token);
                }
            }
            after.reader_owners = self.live_owners(&durable)?;
            self.persist_inventory_deferred(&after, defer)?;
        }
        Ok(result)
    }
}

impl ReaderState {
    #[cfg(unix)]
    fn valid_reader_records(&self) -> bool {
        self.owners.values().all(|state| {
            state.collector.is_none()
                && state.logical_prune.is_none()
                && state.deletions.is_empty()
                && state.retired.is_empty()
                && state.reader_owners.is_empty()
                && state.reader_revision_ceiling == 0
                && state.pins.values().all(|pin| {
                    matches!(pin.scope, PinScope::Snapshot { .. } | PinScope::Closures(_))
                })
        })
    }
    fn encode(&self) -> Result<Vec<u8>, MetadataError> {
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&self.revision.to_le_bytes());
        bytes.extend_from_slice(&(self.owners.len() as u64).to_le_bytes());
        for (owner, state) in &self.owners {
            let encoded = codec::encode(state)?;
            if bytes.len().saturating_add(encoded.len()).saturating_add(72) > codec::MAX_BYTES {
                return Err(backend("oversized reader inventory"));
            }
            bytes.extend_from_slice(&owner.0);
            bytes.extend_from_slice(&(encoded.len() as u64).to_le_bytes());
            bytes.extend_from_slice(&encoded);
        }
        let checksum = blake3::hash(&bytes);
        bytes.extend_from_slice(checksum.as_bytes());
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self, MetadataError> {
        fn take<'a>(bytes: &mut &'a [u8], n: usize) -> Result<&'a [u8], MetadataError> {
            if n > bytes.len() {
                return Err(backend("truncated reader inventory"));
            }
            let (part, rest) = bytes.split_at(n);
            *bytes = rest;
            Ok(part)
        }
        fn number(bytes: &mut &[u8]) -> Result<u64, MetadataError> {
            Ok(u64::from_le_bytes(take(bytes, 8)?.try_into().unwrap()))
        }
        if bytes.len() < 56 || bytes.len() > codec::MAX_BYTES {
            return Err(backend("invalid reader inventory size"));
        }
        let (mut bytes, checksum) = bytes.split_at(bytes.len() - 32);
        if blake3::hash(bytes).as_bytes() != checksum || take(&mut bytes, 8)? != MAGIC {
            return Err(backend("invalid reader inventory checksum or version"));
        }
        let revision = number(&mut bytes)?;
        let count = number(&mut bytes)?;
        if count > bytes.len() as u64 / 40 {
            return Err(backend("invalid reader owner count"));
        }
        let mut owners = BTreeMap::new();
        for _ in 0..count {
            let owner = PinToken(take(&mut bytes, 32)?.try_into().unwrap());
            let length = usize::try_from(number(&mut bytes)?).map_err(backend)?;
            let state = codec::decode(take(&mut bytes, length)?)?;
            if state.collector.is_some()
                || state.logical_prune.is_some()
                || !state.deletions.is_empty()
                || !state.retired.is_empty()
                || !state.reader_owners.is_empty()
                || state.reader_revision_ceiling != 0
                || state.pins.values().any(|pin| {
                    !matches!(pin.scope, PinScope::Snapshot { .. } | PinScope::Closures(_))
                })
            {
                return Err(backend("reader inventory contains recovery records"));
            }
            if owners.insert(owner, state).is_some() {
                return Err(backend("duplicate reader owner"));
            }
        }
        if !bytes.is_empty() {
            return Err(backend("trailing reader inventory bytes"));
        }
        Ok(Self { revision, owners })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> DataPin {
        DataPin {
            scope: PinScope::Snapshot { generation: 42 },
            catalog: None,
            resources: BTreeSet::new(),
        }
    }
    fn resource(value: &str) -> BTreeSet<PinResource> {
        BTreeSet::from([PinResource::StorageObject(value.into())])
    }

    #[tokio::test]
    async fn cached_reader_inventory_observes_independent_replacements_and_releases() {
        let dir = tempfile::tempdir().unwrap();
        let first = FilePinStore::new(dir.path().join("pins"));
        let second = FilePinStore::new(first.path.clone());
        let a = first.register_reader(snapshot()).await.unwrap().unwrap();
        assert!(first.inventory().await.unwrap().pins.contains_key(&a));
        let b = second.register_reader(snapshot()).await.unwrap().unwrap();
        first.release(&a).await.unwrap();
        let inventory = first.inventory().await.unwrap();
        assert!(!inventory.pins.contains_key(&a));
        assert!(inventory.pins.contains_key(&b));
        second.release(&b).await.unwrap();
        assert!(first.inventory().await.unwrap().pins.is_empty());
    }

    #[tokio::test]
    async fn cached_reader_inventory_fails_closed_on_in_place_corruption_and_removal() {
        use std::io::{Seek, SeekFrom};
        let dir = tempfile::tempdir().unwrap();
        let store = FilePinStore::new(dir.path().join("pins"));
        let token = store.register_reader(snapshot()).await.unwrap().unwrap();
        assert!(store.inventory().await.unwrap().pins.contains_key(&token));
        let path = store.reader_path();
        let valid = std::fs::read(&path).unwrap();
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(8)).unwrap();
        file.write_all(&[valid[8] ^ 1]).unwrap();
        // Same inode and length: timestamps must invalidate the cached state.
        assert!(store.inventory().await.is_err());
        std::fs::write(&path, &valid).unwrap();
        assert!(store.inventory().await.unwrap().pins.contains_key(&token));
        std::fs::remove_file(&path).unwrap();
        assert!(store.inventory().await.is_err());
    }

    #[tokio::test]
    async fn cached_reader_inventory_observes_foreign_atomic_publication() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePinStore::new(dir.path().join("pins"));
        let token = store.register_reader(snapshot()).await.unwrap().unwrap();
        assert_eq!(store.inventory().await.unwrap().pins[&token], snapshot());
        let lock = store.lock_file().unwrap();
        lock.lock().unwrap();
        let mut replacement =
            ReaderState::decode(&std::fs::read(store.reader_path()).unwrap()).unwrap();
        replacement.revision += 1;
        for state in replacement.owners.values_mut() {
            if let Some(pin) = state.pins.get_mut(&token) {
                pin.scope = PinScope::Snapshot { generation: 43 };
            }
        }
        let mut temporary = tempfile::NamedTempFile::new_in(dir.path()).unwrap();
        temporary.write_all(&replacement.encode().unwrap()).unwrap();
        temporary.persist(store.reader_path()).unwrap();
        lock.unlock().unwrap();
        assert_eq!(
            store.inventory().await.unwrap().pins[&token].scope,
            PinScope::Snapshot { generation: 43 }
        );
        store.release(&token).await.unwrap();
    }

    #[test]
    fn reader_format_fences_legacy_collectors_and_survives_empty_owners() {
        let mut state = PinInventory::default();
        assert_eq!(&codec::encode(&state).unwrap()[..8], b"CASPIN03");
        state.reader_revision_ceiling = RESERVATION;
        state.reader_owners.insert(PinToken::fresh().unwrap());
        let mut encoded = codec::encode(&state).unwrap();
        assert_eq!(&encoded[..8], b"CASPIN04");
        assert_eq!(codec::decode(&encoded).unwrap(), state);
        // Even rewriting the version to one understood by older collectors
        // cannot silently erase the reader fence: trailing fields are invalid.
        encoded[..8].copy_from_slice(b"CASPIN03");
        let end = encoded.len() - 32;
        let checksum = blake3::hash(&encoded[..end]);
        encoded[end..].copy_from_slice(checksum.as_bytes());
        assert!(codec::decode(&encoded).is_err());
        state.reader_owners.clear();
        assert_eq!(&codec::encode(&state).unwrap()[..8], b"CASPIN04");
        assert_eq!(
            codec::decode(&codec::encode(&state).unwrap()).unwrap(),
            state
        );
    }

    #[tokio::test]
    async fn warm_reader_admission_protection_and_release_do_not_rewrite_durable_ledger() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePinStore::new(dir.path().join("pins"));
        let token = store.register_reader(snapshot()).await.unwrap().unwrap();
        store.release(&token).await.unwrap();
        let durable = std::fs::read(&store.path).unwrap();
        for _ in 0..16 {
            let before = store.inventory().await.unwrap();
            let token = store.register_reader(snapshot()).await.unwrap().unwrap();
            // A mark taken before this reader cannot authorize a deletion.
            assert!(
                store
                    .claim_deletions(before.revision, resource("unrelated"))
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                store
                    .protect(&token, resource("reader-pack"))
                    .await
                    .unwrap()
            );
            let admitted = store.inventory().await.unwrap();
            assert!(
                store
                    .claim_deletions(admitted.revision, resource("reader-pack"))
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(admitted.pins.contains_key(&token));
            assert!(!store.read_locked().unwrap().pins.contains_key(&token));
            store.release(&token).await.unwrap();
            assert!(store.inventory().await.unwrap().pins.is_empty());
            assert_eq!(std::fs::read(&store.path).unwrap(), durable);
        }
    }

    #[tokio::test]
    async fn owners_share_across_handles_and_reader_drop_never_retires_a_broad_scope() {
        let dir = tempfile::tempdir().unwrap();
        let first = Arc::new(FilePinStore::new(dir.path().join("pins")));
        let second = Arc::new(FilePinStore::new(dir.path().join("pins")));
        let a = DataPinLease::acquire_reader(first.clone(), snapshot())
            .await
            .unwrap();
        let b = DataPinLease::acquire_reader(second.clone(), snapshot())
            .await
            .unwrap();
        assert_eq!(first.read_locked().unwrap().reader_owners.len(), 1);
        let state = second.inventory().await.unwrap();
        assert_eq!(state.pins.len(), 2);
        let collector = second
            .begin_collection(state.revision, None)
            .await
            .unwrap()
            .unwrap();
        drop(a);
        crate::metadata::flush_repository_leases().await.unwrap();
        let state = second.inventory().await.unwrap();
        assert_eq!(state.pins.len(), 1);
        assert!(state.retired.is_empty());
        assert!(state.pins.contains_key(b.token()));
        drop(first);
        assert_eq!(second.inventory().await.unwrap().pins.len(), 1);
        drop(b);
        crate::metadata::flush_repository_leases().await.unwrap();
        second.finish_collection(&collector).await.unwrap();
        assert!(second.inventory().await.unwrap().pins.is_empty());
    }

    #[tokio::test]
    async fn missing_reader_inventory_fails_closed_until_owner_stops_then_fences_old_revisions() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePinStore::new(dir.path().join("pins"));
        let writer = store
            .register(DataPin {
                scope: PinScope::Staging,
                catalog: None,
                resources: resource("writer-pack"),
            })
            .await
            .unwrap()
            .unwrap();
        store.register_reader(snapshot()).await.unwrap().unwrap();
        let before = store.inventory().await.unwrap();
        let deletion = store
            .claim_deletions(before.revision, resource("garbage"))
            .await
            .unwrap()
            .unwrap();
        let ceiling = store.read_locked().unwrap().reader_revision_ceiling;
        let original = std::fs::read(store.reader_path()).unwrap();
        std::fs::write(store.reader_path(), b"interrupted host write").unwrap();
        assert!(store.inventory().await.is_err());
        assert!(
            store
                .claim_deletions(before.revision, resource("writer-pack"))
                .await
                .is_err()
        );
        // A live owner returning to the original intact state remains readable.
        std::fs::write(store.reader_path(), &original).unwrap();
        assert_eq!(store.inventory().await.unwrap().pins.len(), 2);
        std::fs::remove_file(store.reader_path()).unwrap();
        assert!(store.inventory().await.is_err());
        // Make replacement impossible, as when a full filesystem cannot
        // allocate a new reader inventory. Durable GC recovery still proceeds.
        std::fs::create_dir(store.reader_path()).unwrap();
        drop(store);
        let recovered = FilePinStore::new(dir.path().join("pins"));
        let state = recovered.inventory().await.unwrap();
        assert!(state.revision > ceiling);
        assert_eq!(state.pins.len(), 1);
        assert!(state.pins.contains_key(&writer));
        assert!(state.deletions.contains_key(&deletion));
        assert!(
            recovered
                .claim_deletions(before.revision, resource("other"))
                .await
                .unwrap()
                .is_none()
        );
        recovered.finish_deletions(&deletion).await.unwrap();
        recovered.release(&writer).await.unwrap();
    }

    #[tokio::test]
    async fn last_owner_exit_fences_an_intact_but_stale_reader_clock() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePinStore::new(dir.path().join("pins"));
        let first = store.register_reader(snapshot()).await.unwrap().unwrap();
        let previous = std::fs::read(store.reader_path()).unwrap();
        store.release(&first).await.unwrap();
        let second = store.register_reader(snapshot()).await.unwrap().unwrap();
        assert!(store.protect(&second, resource("pack")).await.unwrap());
        let before = store.inventory().await.unwrap();
        let ceiling = store.read_locked().unwrap().reader_revision_ceiling;
        let path = store.reader_path();
        drop(store);
        // Simulate a host crash retaining an earlier, correctly checksummed
        // rename, not just a missing or torn file.
        std::fs::write(path, previous).unwrap();
        let recovered = FilePinStore::new(dir.path().join("pins"));
        let inventory = recovered.inventory().await.unwrap();
        assert!(inventory.revision > ceiling);
        assert!(inventory.pins.is_empty());
        assert!(inventory.reader_owners.is_empty());
        assert!(
            recovered
                .claim_deletions(before.revision, resource("unrelated"))
                .await
                .unwrap()
                .is_none()
        );
        let new = recovered
            .register_reader(snapshot())
            .await
            .unwrap()
            .unwrap();
        assert!(recovered.inventory().await.unwrap().revision > inventory.revision);
        recovered.release(&new).await.unwrap();
    }

    #[tokio::test]
    async fn reader_and_durable_transitions_share_the_prune_and_delete_fences() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePinStore::new(dir.path().join("pins"));
        let initial = store.register_reader(snapshot()).await.unwrap().unwrap();
        store.release(&initial).await.unwrap();
        let revision = store.inventory().await.unwrap().revision;
        let prune = store.begin_prune(revision).await.unwrap().unwrap();
        assert!(store.register_reader(snapshot()).await.unwrap().is_none());
        store.finish_prune(&prune).await.unwrap();
        let revision = store.inventory().await.unwrap().revision;
        let deletion = store
            .claim_deletions(revision, resource("claimed"))
            .await
            .unwrap()
            .unwrap();
        let mut pin = snapshot();
        pin.resources = resource("claimed");
        assert!(store.register_reader(pin).await.unwrap().is_none());
        let reader = store.register_reader(snapshot()).await.unwrap().unwrap();
        assert!(!store.protect(&reader, resource("claimed")).await.unwrap());
        store.finish_deletions(&deletion).await.unwrap();
        assert!(store.protect(&reader, resource("claimed")).await.unwrap());
        store.release(&reader).await.unwrap();
        assert!(store.inventory().await.unwrap().pins.is_empty());
    }

    #[tokio::test]
    async fn fork_inherited_store_cannot_release_parent_protection() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePinStore::new(dir.path().join("pins"));
        let token = store.register_reader(snapshot()).await.unwrap().unwrap();
        let mut inherited = store.clone();
        inherited.process = std::process::id().wrapping_add(1);
        assert!(inherited.release(&token).await.is_err());
        assert!(inherited.register_reader(snapshot()).await.is_err());
        assert!(store.inventory().await.unwrap().pins.contains_key(&token));
        store.release(&token).await.unwrap();
    }

    #[tokio::test]
    async fn reader_inventory_encoding_is_bounded_and_rejects_durable_recovery_records() {
        let mut state = ReaderState::default();
        let owner = PinToken::fresh().unwrap();
        let token = PinToken::fresh().unwrap();
        state.owners.insert(owner.clone(), PinInventory::default());
        state
            .owners
            .get_mut(&owner)
            .unwrap()
            .pins
            .insert(token.clone(), snapshot());
        let encoded = state.encode().unwrap();
        assert_eq!(
            ReaderState::decode(&encoded).unwrap().owners[&owner].pins[&token],
            snapshot()
        );
        for cut in [0, 8, 16, 24, encoded.len() - 1] {
            assert!(ReaderState::decode(&encoded[..cut]).is_err());
        }
        state
            .owners
            .get_mut(&owner)
            .unwrap()
            .pins
            .get_mut(&token)
            .unwrap()
            .scope = PinScope::Staging;
        assert!(ReaderState::decode(&state.encode().unwrap()).is_err());
    }
    #[tokio::test]
    async fn cancelled_reader_publication_finishes_and_releases_its_late_token() {
        for phase in [
            "owner-fenced",
            "reader-before-rename",
            "reader-after-rename",
            "reader-registered",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let store = Arc::new(FilePinStore::new(dir.path().join("pins")));
            if phase != "owner-fenced" {
                let token = store.register_reader(snapshot()).await.unwrap().unwrap();
                store.release(&token).await.unwrap();
            }
            let (entered, receive) = std::sync::mpsc::channel();
            let (resume, wait) = std::sync::mpsc::channel();
            *store.reader_pause.lock().unwrap() = Some(TestPause {
                phase,
                entered,
                resume: wait,
            });
            let task = tokio::spawn({
                let store = store.clone();
                async move { DataPinLease::try_acquire_reader(store, snapshot()).await }
            });
            tokio::task::spawn_blocking(move || {
                receive
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .unwrap()
            })
            .await
            .unwrap();
            task.abort();
            assert!(matches!(task.await, Err(error) if error.is_cancelled()));
            resume.send(()).unwrap();
            crate::metadata::flush_repository_leases().await.unwrap();
            assert!(store.inventory().await.unwrap().pins.is_empty(), "{phase}");
            assert!(store.read_locked().unwrap().pins.is_empty(), "{phase}");
        }
    }
    async fn coordination_case(active: usize, iterations: usize) -> serde_json::Value {
        use std::time::Instant;
        let dir = tempfile::tempdir().unwrap();
        let store = FilePinStore::new(dir.path().join("pins"));
        let writer = store
            .register(DataPin {
                scope: PinScope::Staging,
                catalog: None,
                resources: resource("durable-writer"),
            })
            .await
            .unwrap()
            .unwrap();
        let mut samples = Vec::new();
        let before = std::fs::read(&store.path).unwrap();
        let start = Instant::now();
        let first = store.register_reader(snapshot()).await.unwrap().unwrap();
        let nanos = start.elapsed().as_nanos() as u64;
        assert_ne!(std::fs::read(&store.path).unwrap(), before);
        samples.push(
            serde_json::json!({"phase": "cold-register", "nanos": nanos, "durable_changed": 1}),
        );
        store.release(&first).await.unwrap();
        let mut background = Vec::new();
        for _ in 0..active {
            background.push(store.register_reader(snapshot()).await.unwrap().unwrap());
        }
        for _ in 0..iterations {
            let before = std::fs::read(&store.path).unwrap();
            let revision = store.inventory().await.unwrap().revision;
            let start = Instant::now();
            let token = store.register_reader(snapshot()).await.unwrap().unwrap();
            let nanos = start.elapsed().as_nanos() as u64;
            samples.push(
                serde_json::json!({"phase": "warm-register", "nanos": nanos, "durable_changed": 0}),
            );
            assert!(
                store
                    .claim_deletions(revision, resource("unrelated"))
                    .await
                    .unwrap()
                    .is_none()
            );
            let start = Instant::now();
            assert!(
                store
                    .protect(&token, resource("reader-pack"))
                    .await
                    .unwrap()
            );
            let nanos = start.elapsed().as_nanos() as u64;
            samples.push(
                serde_json::json!({"phase": "warm-protect", "nanos": nanos, "durable_changed": 0}),
            );
            let revision = store.inventory().await.unwrap().revision;
            assert!(
                store
                    .claim_deletions(revision, resource("reader-pack"))
                    .await
                    .unwrap()
                    .is_none()
            );
            let start = Instant::now();
            store.release(&token).await.unwrap();
            let nanos = start.elapsed().as_nanos() as u64;
            samples.push(
                serde_json::json!({"phase": "warm-release", "nanos": nanos, "durable_changed": 0}),
            );
            assert_eq!(std::fs::read(&store.path).unwrap(), before);
        }
        // Position the real production counter at its last reserved revision.
        // Setup is outside timing; no 65,536-operation warmup is required.
        let lock = store.lock_file().unwrap();
        lock.lock().unwrap();
        let mut durable = store.read_locked().unwrap();
        let readers = store.readers_locked(&mut durable).unwrap();
        durable.reader_revision_ceiling =
            store.merge_readers(&durable, &readers).unwrap().revision + 1;
        store
            .write_locked(&codec::encode(&durable).unwrap())
            .unwrap();
        lock.unlock().unwrap();
        let before = std::fs::read(&store.path).unwrap();
        let start = Instant::now();
        let token = store.register_reader(snapshot()).await.unwrap().unwrap();
        let nanos = start.elapsed().as_nanos() as u64;
        assert_eq!(std::fs::read(&store.path).unwrap(), before);
        assert_eq!(
            store.inventory().await.unwrap().revision,
            durable.reader_revision_ceiling
        );
        samples.push(serde_json::json!({"phase": "last-reserved-register", "nanos": nanos, "durable_changed": 0}));
        let start = Instant::now();
        assert!(
            store
                .protect(&token, resource("reader-pack"))
                .await
                .unwrap()
        );
        let nanos = start.elapsed().as_nanos() as u64;
        let renewed = store.read_locked().unwrap();
        assert_eq!(
            renewed.reader_revision_ceiling,
            durable.reader_revision_ceiling + 1 + RESERVATION
        );
        assert_ne!(std::fs::read(&store.path).unwrap(), before);
        samples.push(serde_json::json!({"phase": "reservation-rollover-protect", "nanos": nanos, "durable_changed": 1}));
        let before = std::fs::read(&store.path).unwrap();
        let start = Instant::now();
        store.release(&token).await.unwrap();
        let nanos = start.elapsed().as_nanos() as u64;
        assert_eq!(std::fs::read(&store.path).unwrap(), before);
        samples.push(
            serde_json::json!({"phase": "renewed-release", "nanos": nanos, "durable_changed": 0}),
        );
        for token in background {
            store.release(&token).await.unwrap();
        }
        let inventory = store.inventory().await.unwrap();
        assert_eq!(inventory.pins.len(), 1);
        assert!(inventory.pins.contains_key(&writer));
        assert_eq!(store.read_locked().unwrap().pins, inventory.pins);
        store.release(&writer).await.unwrap();
        assert!(store.inventory().await.unwrap().pins.is_empty());
        serde_json::json!({"active_readers": active, "iterations": iterations, "reservation": RESERVATION,
            "samples": samples, "correctness": "durable write protection preserved, no leaked readers, stale and protected deletion claims rejected, exact reservation boundary"})
    }

    #[tokio::test]
    async fn reader_revision_reservation_boundary_preserves_protection() {
        coordination_case(1, 2).await;
    }

    #[tokio::test]
    #[ignore = "permanent reader coordination benchmark; run benchmark reader-coordination"]
    async fn benchmark_reader_coordination() {
        let active = std::env::var("CASITA_ACTIVE_READERS")
            .unwrap()
            .parse()
            .unwrap();
        let iterations = std::env::var("CASITA_READER_ITERATIONS")
            .unwrap()
            .parse()
            .unwrap();
        println!(
            "reader_coordination_sample {}",
            coordination_case(active, iterations).await
        );
    }
}
