//! Local append journal. Frames occupy disjoint 4 KiB blocks; a complete frame
//! is acknowledged only after sync. Checkpoints reuse the two allocated slots.
use super::*;
use std::io::{Seek, SeekFrom};
use std::os::unix::fs::FileExt;
mod incremental;
use incremental::{Changes, Index, Touched};

pub(super) const BLOCK: usize = 4096;
pub(super) const WINDOW: usize = 1024 * 1024;
pub(super) const CHECKPOINT_OPERATIONS: usize = 256;
const MAGIC: &[u8; 8] = b"CASPJL01";
const FRAME: &[u8; 8] = b"CASDLT01";
// V2 appends a bounded inventory containing only resource additions. Older
// readers reject its frame magic rather than silently losing protection.
const EXTENSION_FRAME: &[u8; 8] = b"CASDLT02";
const END: &[u8; 8] = b"CASEND01";
const HEADER: usize = 136;

#[derive(Default)]
pub(super) struct Cache {
    snapshot: Option<Snapshot>,
    pending: Option<Changes>,
    pending_operations: usize,
}
struct Snapshot {
    epoch: [u8; 32],
    state: PinInventory,
    index: Index,
    cursor: usize,
    start: usize,
    operations: usize,
}

#[cfg(test)]
impl Cache {
    pub(super) fn replay_position(&self) -> (usize, usize) {
        let snapshot = self.snapshot.as_ref().unwrap();
        (snapshot.operations, snapshot.cursor - snapshot.start)
    }
}

fn align(value: usize) -> usize {
    value.div_ceil(BLOCK) * BLOCK
}
fn number(bytes: &[u8]) -> Result<usize, MetadataError> {
    usize::try_from(u64::from_le_bytes(bytes.try_into().map_err(backend)?)).map_err(backend)
}
fn io(error: std::io::Error) -> MetadataError {
    if error.kind() == std::io::ErrorKind::StorageFull {
        MetadataError::StorageFull
    } else {
        backend(error)
    }
}
fn bad() -> MetadataError {
    backend("corrupt local pin journal")
}

fn apply(
    state: &mut PinInventory,
    mut bytes: &[u8],
    frame: &[u8],
) -> Result<(Touched, usize), MetadataError> {
    if frame != FRAME && frame != EXTENSION_FRAME {
        return Err(bad());
    }
    let globals = codec::globals_len(state);
    fn take<'a>(bytes: &mut &'a [u8], count: usize) -> Result<&'a [u8], MetadataError> {
        if count > bytes.len() {
            return Err(bad());
        }
        let (part, rest) = bytes.split_at(count);
        *bytes = rest;
        Ok(part)
    }
    let length = number(take(&mut bytes, 8)?)?;
    let mut delta = codec::decode(take(&mut bytes, length)?)?;
    let mut lists = Vec::new();
    for _ in 0..3 {
        let count = number(take(&mut bytes, 8)?)?;
        if count > bytes.len() / 32 {
            return Err(bad());
        }
        let mut tokens = BTreeSet::new();
        for _ in 0..count {
            if !tokens.insert(PinToken(take(&mut bytes, 32)?.try_into().unwrap())) {
                return Err(bad());
            }
        }
        lists.push(tokens);
    }
    let extensions = if frame == EXTENSION_FRAME {
        let mut inventory = codec::decode(bytes)?;
        let pins = std::mem::take(&mut inventory.pins);
        if inventory != PinInventory::default() || pins.is_empty() {
            return Err(bad());
        }
        pins
    } else {
        if !bytes.is_empty() {
            return Err(bad());
        }
        BTreeMap::new()
    };
    if delta.revision <= state.revision {
        return Err(bad());
    }
    let mut touched = Touched::default();
    for token in lists[0].iter().chain(delta.pins.keys()) {
        touched.pin(state, token);
    }
    for token in lists[1].iter().chain(delta.deletions.keys()) {
        touched.deletion(state, token);
    }
    for token in &lists[0] {
        if state.pins.remove(token).is_none() {
            return Err(bad());
        }
    }
    for token in &lists[1] {
        if state.deletions.remove(token).is_none() {
            return Err(bad());
        }
    }
    state.pins.append(&mut delta.pins);
    state.deletions.append(&mut delta.deletions);
    state.revision = delta.revision;
    state.reader_owners = delta.reader_owners;
    state.reader_revision_ceiling = delta.reader_revision_ceiling;
    state.logical_prune = delta.logical_prune;
    state.collector = delta.collector;
    state.retired = lists.pop().unwrap();
    if (!state.retired.is_empty() && state.collector.is_none())
        || state
            .retired
            .iter()
            .any(|token| !state.pins.contains_key(token))
    {
        return Err(bad());
    }
    touched.apply_extensions(state, extensions)?;
    Ok((touched, globals))
}

/// Stable identifiers of the local journal's commit invariants.
const APPEND_POINT: &str = "pins.journal.append";
const CHECKPOINT_POINT: &str = "pins.journal.checkpoint";

/// A frame appended to the active journal is one a fresh reader replays: it
/// carries the epoch of the active checkpoint (`None` when the active file is
/// not a journal) and 1..=MAX_GROUP operations, and the journal it extends
/// stays within CHECKPOINT_OPERATIONS operations and WINDOW bytes. Replay
/// stops without an error at a frame from another epoch, dropping it and every
/// later frame, and refuses the whole ledger over the other limits.
fn validate_journal_append(
    active: Option<&[u8; 32]>,
    epoch: &[u8; 32],
    operations: usize,
    journal_operations: usize,
    journal_bytes: usize,
) -> Result<(), Violation> {
    invariant::ensure(APPEND_POINT, active == Some(epoch), || {
        "frame epoch is not the active checkpoint's".into()
    })?;
    invariant::ensure(
        APPEND_POINT,
        (1..=super::group::MAX_GROUP).contains(&operations),
        || format!("frame carries {operations} operations"),
    )?;
    invariant::ensure(
        APPEND_POINT,
        journal_operations <= CHECKPOINT_OPERATIONS && journal_bytes <= WINDOW,
        || format!("journal would hold {journal_operations} operations in {journal_bytes} bytes"),
    )
}

/// A checkpoint compacts the journal without changing it: it holds exactly
/// what a fresh reader replays from the active checkpoint, its frames and the
/// pending frame the checkpoint replaces.
fn validate_journal_checkpoint(
    replayed: &PinInventory,
    checkpoint: &PinInventory,
) -> Result<(), Violation> {
    invariant::ensure(CHECKPOINT_POINT, replayed == checkpoint, || {
        format!(
            "checkpoint at revision {} differs from the journal replayed to revision {}",
            checkpoint.revision, replayed.revision
        )
    })
}

/// A checkpoint starts a new epoch: readers tell a new checkpoint, and the
/// frames that belong to it, from the one they cached by epoch alone.
fn validate_checkpoint_epoch(
    previous: Option<&[u8; 32]>,
    epoch: &[u8; 32],
) -> Result<(), Violation> {
    invariant::ensure(CHECKPOINT_POINT, previous != Some(epoch), || {
        "checkpoint reuses the epoch it replaces".into()
    })
}

impl FilePinStore {
    #[cfg(test)]
    pub(super) fn ledger_checkpoint(&self, phase: &'static str) -> Result<(), MetadataError> {
        let local = self.local()?;
        let pause = {
            let mut pending = local.pause.lock().unwrap();
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
        let mut fail = local.fail.lock().unwrap();
        if *fail == Some(phase) {
            *fail = None;
            return Err(backend("injected journal I/O failure"));
        }
        drop(fail);
        if std::env::var("CASITA_LEDGER_CRASH_PHASE").is_ok_and(|target| target == phase) {
            std::fs::write(
                std::env::var_os("CASITA_LEDGER_CRASH_SIGNAL").unwrap(),
                phase,
            )
            .unwrap();
            loop {
                std::thread::park();
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn journal_test_checkpoint(
        &self,
        state: &PinInventory,
    ) -> Result<(), MetadataError> {
        let local = self.local()?;
        let mut cache = local.cache.lock().map_err(backend)?;
        self.refresh_journal(&mut cache)?;
        self.checkpoint_journal(&mut cache, state)
    }

    pub(super) fn journal_read(&self) -> Result<PinInventory, MetadataError> {
        let local = self.local()?;
        let mut cache = local.cache.lock().map_err(backend)?;
        let result = self.refresh_journal(&mut cache).map(|legacy| {
            if let Some(state) = legacy {
                state
            } else {
                local
                    .stats
                    .inventory_copies
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                cache.snapshot.as_ref().unwrap().state.clone()
            }
        });
        if result.is_err() {
            *cache = Cache::default();
        }
        result
    }

    fn refresh_journal(&self, cache: &mut Cache) -> Result<Option<PinInventory>, MetadataError> {
        self.replay_journal(cache, true)
    }

    /// What a fresh reader recovers from the active ledger, leaving the
    /// files, the cache and the statistics untouched.
    fn replay_active_journal(&self) -> Result<PinInventory, MetadataError> {
        let mut cache = Cache::default();
        match self.replay_journal(&mut cache, false)? {
            Some(legacy) => Ok(legacy),
            None => cache
                .snapshot
                .map(|snapshot| snapshot.state)
                .ok_or_else(bad),
        }
    }

    /// Brings `cache` up to date with the ledger. A reader about to act on the
    /// result `adopt`s it, syncing the checkpoint name and replayed frames a
    /// killed publisher may have left unsynced.
    fn replay_journal(
        &self,
        cache: &mut Cache,
        adopt: bool,
    ) -> Result<Option<PinInventory>, MetadataError> {
        let mut file = match std::fs::File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                *cache = Cache::default();
                return Ok(Some(PinInventory::default()));
            }
            Err(error) => return Err(io(error)),
        };
        let mut prefix = [0; 8];
        file.read_exact(&mut prefix).map_err(io)?;
        if &prefix != MAGIC {
            *cache = Cache::default();
            return self.read_replacement_locked().map(Some);
        }
        let mut header = [0; 112];
        file.seek(SeekFrom::Start(0)).map_err(io)?;
        file.read_exact(&mut header).map_err(io)?;
        if blake3::hash(&header[..80]).as_bytes() != &header[80..112] {
            return Err(bad());
        }
        let epoch: [u8; 32] = header[8..40].try_into().unwrap();
        let length = number(&header[40..48])?;
        if length > codec::MAX_BYTES {
            return Err(bad());
        }
        let fresh = cache
            .snapshot
            .as_ref()
            .is_none_or(|snapshot| snapshot.epoch != epoch);
        if fresh {
            if cache.pending.is_some() {
                return Err(backend("journal changed during a locked group"));
            }
            let mut bytes = vec![0; length];
            file.seek(SeekFrom::Start(BLOCK as u64)).map_err(io)?;
            file.read_exact(&mut bytes).map_err(io)?;
            if blake3::hash(&bytes).as_bytes() != &header[48..80] {
                return Err(bad());
            }
            let state = codec::decode(&bytes)?;
            let cursor = align(BLOCK + length);
            cache.snapshot = Some(Snapshot {
                epoch,
                index: Index::new(&state)?,
                state,
                cursor,
                start: cursor,
                operations: 0,
            });
            // A publisher killed after exchange may not have synced the name.
            if adopt {
                std::fs::File::open(self.parent())
                    .and_then(|file| file.sync_all())
                    .map_err(io)?;
            }
        }
        let snapshot = cache.snapshot.as_mut().unwrap();
        let capacity = usize::try_from(file.metadata().map_err(io)?.len()).map_err(backend)?;
        let mut replayed = false;
        loop {
            if snapshot.cursor + HEADER > capacity {
                return Err(bad());
            }
            let mut head = [0; HEADER];
            file.seek(SeekFrom::Start(snapshot.cursor as u64))
                .map_err(io)?;
            file.read_exact(&mut head).map_err(io)?;
            if head.iter().all(|byte| *byte == 0) {
                break;
            }
            if &head[..8] != FRAME && &head[..8] != EXTENSION_FRAME {
                return Err(bad());
            }
            if blake3::hash(&head[..104]).as_bytes() != &head[104..136] {
                return Err(bad());
            }
            // Validate before testing the epoch: corruption of an acknowledged
            // frame's epoch must not silently truncate the recovered inventory.
            if head[8..40] != epoch {
                break;
            }
            let length = number(&head[40..48])?;
            let operations = number(&head[48..56])?;
            if length > codec::MAX_BYTES || operations == 0 || operations > super::group::MAX_GROUP
            {
                return Err(bad());
            }
            let total = align(HEADER + length + 40);
            if snapshot.cursor + total > capacity {
                return Err(bad());
            }
            let mut trailer = [0; 40];
            file.seek(SeekFrom::Start((snapshot.cursor + total - 40) as u64))
                .map_err(io)?;
            file.read_exact(&mut trailer).map_err(io)?;
            if &trailer[..8] != END || trailer[8..] != head[104..136] {
                break;
            }
            let mut bytes = vec![0; length];
            file.seek(SeekFrom::Start((snapshot.cursor + HEADER) as u64))
                .map_err(io)?;
            file.read_exact(&mut bytes).map_err(io)?;
            if blake3::hash(&bytes).as_bytes() != &head[72..104]
                || u64::from_le_bytes(head[56..64].try_into().unwrap()) != snapshot.state.revision
            {
                return Err(bad());
            }
            if cache.pending.is_some() {
                return Err(backend("journal advanced during a locked group"));
            }
            let (touched, globals) = apply(&mut snapshot.state, &bytes, &head[..8])?;
            snapshot.index.update(globals, &touched, &snapshot.state)?;
            if snapshot.state.revision != u64::from_le_bytes(head[64..72].try_into().unwrap()) {
                return Err(bad());
            }
            snapshot.cursor += total;
            snapshot.operations += operations;
            if snapshot.operations > CHECKPOINT_OPERATIONS
                || snapshot.cursor - snapshot.start > WINDOW
            {
                return Err(bad());
            }
            replayed = true;
        }
        if replayed && adopt {
            // Complete frames from a killed writer can be visible before sync.
            // Adopt them durably before exposing their revision reservations.
            file.sync_all().map_err(io)?;
            local_stats(self)?
                .adoptions
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(None)
    }

    pub(super) fn persist_inventory(&self, state: &PinInventory) -> Result<(), MetadataError> {
        self.persist_inventory_deferred(state, false)
    }

    pub(super) fn persist_inventory_deferred(
        &self,
        state: &PinInventory,
        defer: bool,
    ) -> Result<(), MetadataError> {
        #[cfg(test)]
        if self.replacement {
            return self.write_replacement_locked(&codec::encode(state)?);
        }
        let local = self.local()?;
        let mut cache = local.cache.lock().map_err(backend)?;
        let result = (|| {
            if cache.snapshot.is_none() {
                let result = self.checkpoint_journal(&mut cache, state);
                // Upgrading a legacy ledger must not consume the headroom that
                // collection needs to escape a full filesystem. Until journal
                // activation succeeds, retain the preallocated replacement path.
                if matches!(result, Err(MetadataError::StorageFull))
                    && self.read_replacement_locked().is_ok()
                {
                    *cache = Cache::default();
                    return self.write_replacement_locked(&codec::encode(state)?);
                }
                return result;
            }
            if state.revision <= cache.snapshot.as_ref().unwrap().state.revision {
                return Err(backend("non-increasing durable journal revision"));
            }
            local
                .stats
                .inventory_diffs
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let snapshot = cache.snapshot.as_mut().unwrap();
            let touched = Touched::diff(&snapshot.state, state);
            let globals = codec::globals_len(&snapshot.state);
            let revision = snapshot.state.revision;
            snapshot.index.update(globals, &touched, state)?;
            snapshot.state = state.clone();
            cache
                .pending
                .get_or_insert_with(|| Changes::new(revision))
                .absorb(&touched);
            cache.pending_operations += 1;
            if !defer {
                self.flush_journal(&mut cache)?;
            }
            Ok(())
        })();
        if result.is_err() {
            *cache = Cache::default();
        }
        result
    }

    pub(super) fn flush_pending_journal(&self) -> Result<(), MetadataError> {
        #[cfg(test)]
        if self.replacement {
            return Ok(());
        }
        let local = self.local()?;
        let mut cache = local.cache.lock().map_err(backend)?;
        self.flush_journal(&mut cache)
    }

    fn flush_journal(&self, cache: &mut Cache) -> Result<(), MetadataError> {
        let _phase = LedgerPhase::new("journal_flush");
        let Some(before) = &cache.pending else {
            return Ok(());
        };
        let snapshot = cache.snapshot.as_ref().unwrap();
        // The pending frame is the commit, whether appended or folded into a
        // checkpoint, so its changes are checked before either.
        invariant::check(|| {
            validate_revision_step(
                before.revision,
                snapshot.state.revision,
                RevisionStep::Forward,
            )?;
            let (pins, deletions) = before.touched();
            validate_inventory_records(&snapshot.state, pins, deletions)
        })?;
        let body = before.encode(&snapshot.state)?;
        let total = align(HEADER + body.len() + 40);
        if snapshot.operations + cache.pending_operations > CHECKPOINT_OPERATIONS
            || snapshot.cursor - snapshot.start + total > WINDOW
        {
            let frame = before.frame();
            return self.checkpoint_cached_journal(cache, frame, &body);
        }
        let state_bytes = snapshot.index.bytes;
        self.ensure_journal_capacity(align(BLOCK + state_bytes) + WINDOW + BLOCK)?;
        let mut bytes = vec![0; total];
        bytes[..8].copy_from_slice(before.frame());
        bytes[8..40].copy_from_slice(&snapshot.epoch);
        bytes[40..48].copy_from_slice(&(body.len() as u64).to_le_bytes());
        bytes[48..56].copy_from_slice(&(cache.pending_operations as u64).to_le_bytes());
        bytes[56..64].copy_from_slice(&before.revision.to_le_bytes());
        bytes[64..72].copy_from_slice(&snapshot.state.revision.to_le_bytes());
        bytes[72..104].copy_from_slice(blake3::hash(&body).as_bytes());
        let checksum = *blake3::hash(&bytes[..104]).as_bytes();
        bytes[104..136].copy_from_slice(&checksum);
        bytes[HEADER..HEADER + body.len()].copy_from_slice(&body);
        bytes[total - 40..total - 32].copy_from_slice(END);
        bytes[total - 32..].copy_from_slice(&checksum);
        let mut file = std::fs::File::options()
            .read(true)
            .write(true)
            .open(&self.path)
            .map_err(io)?;
        if invariant::ENABLED {
            let mut active = [0; 40];
            file.read_exact_at(&mut active, 0).map_err(io)?;
            invariant::check(|| {
                validate_journal_append(
                    active
                        .split_first_chunk::<8>()
                        .filter(|(magic, _)| *magic == MAGIC)
                        .and_then(|(_, epoch)| epoch.first_chunk()),
                    &snapshot.epoch,
                    cache.pending_operations,
                    snapshot.operations + cache.pending_operations,
                    snapshot.cursor - snapshot.start + total,
                )
            })?;
        }
        // Clear the next header before replacing any interrupted tail. This
        // write never touches an acknowledged frame or checkpoint.
        file.seek(SeekFrom::Start((snapshot.cursor + total) as u64))
            .map_err(io)?;
        file.write_all(&[0; HEADER]).map_err(io)?;
        file.seek(SeekFrom::Start(snapshot.cursor as u64))
            .map_err(io)?;
        #[cfg(test)]
        {
            let split = HEADER + body.len() / 2;
            file.write_all(&bytes[..split]).map_err(io)?;
            self.ledger_checkpoint("journal-partial-frame")?;
            file.write_all(&bytes[split..]).map_err(io)?;
            self.ledger_checkpoint("journal-before-sync")?;
        }
        #[cfg(not(test))]
        file.write_all(&bytes).map_err(io)?;
        let sync = LedgerPhase::new("journal_append_sync");
        file.sync_all().map_err(io)?;
        drop(sync);
        #[cfg(test)]
        self.ledger_checkpoint("journal-after-sync")?;
        let stats = local_stats(self)?;
        stats
            .frames
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        stats.bytes.fetch_add(
            (bytes.len() + HEADER) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        stats
            .syncs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let snapshot = cache.snapshot.as_mut().unwrap();
        snapshot.cursor += total;
        snapshot.operations += cache.pending_operations;
        cache.pending = None;
        cache.pending_operations = 0;
        Ok(())
    }

    fn ensure_journal_capacity(&self, needed: usize) -> Result<(), MetadataError> {
        let capacity = needed.next_power_of_two();
        for path in [self.path.clone(), self.spare_path()?] {
            let mut file = std::fs::File::options()
                .read(true)
                .write(true)
                .open(path)
                .map_err(io)?;
            let size = file.metadata().map_err(io)?.len();
            if size < capacity as u64 {
                #[cfg(test)]
                if self
                    .local()?
                    .deny_growth
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    return Err(MetadataError::StorageFull);
                }
                file.seek(SeekFrom::End(0)).map_err(io)?;
                let zeros = [0; 8192];
                let mut remaining = capacity as u64 - size;
                while remaining != 0 {
                    let count = remaining.min(zeros.len() as u64) as usize;
                    file.write_all(&zeros[..count]).map_err(io)?;
                    remaining -= count as u64;
                }
                file.sync_all().map_err(io)?;
            }
        }
        Ok(())
    }

    pub(super) fn spare_path(&self) -> Result<PathBuf, MetadataError> {
        let mut name = self.path.file_name().ok_or_else(bad)?.to_os_string();
        name.push(".spare");
        Ok(self.parent().join(name))
    }

    /// Writes the cached inventory, including the pending `frame` with `body`,
    /// as the new checkpoint.
    fn checkpoint_cached_journal(
        &self,
        cache: &mut Cache,
        frame: &[u8; 8],
        body: &[u8],
    ) -> Result<(), MetadataError> {
        let _phase = LedgerPhase::new("journal_checkpoint");
        // Replaying the journal again costs about as much as the checkpoint
        // itself, so only debug builds check what it compacts.
        if invariant::DEBUG {
            let mut replayed = self.replay_active_journal()?;
            apply(&mut replayed, body, frame)?;
            let snapshot = cache.snapshot.as_ref().ok_or_else(bad)?;
            invariant::check(|| validate_journal_checkpoint(&replayed, &snapshot.state))?;
        }
        let snapshot = cache.snapshot.as_mut().unwrap();
        let bytes = codec::encode(&snapshot.state)?;
        let (epoch, start) = self.write_checkpoint(&bytes, Some(&snapshot.epoch))?;
        // Edits already updated and validated this index. Retain both it and
        // the inventory, advancing journal positions only after durability.
        snapshot.epoch = epoch;
        snapshot.cursor = start;
        snapshot.start = start;
        snapshot.operations = 0;
        cache.pending = None;
        cache.pending_operations = 0;
        Ok(())
    }

    fn checkpoint_journal(
        &self,
        cache: &mut Cache,
        state: &PinInventory,
    ) -> Result<(), MetadataError> {
        let _phase = LedgerPhase::new("journal_checkpoint");
        let bytes = codec::encode(state)?;
        if cache.snapshot.is_none() {
            let before = self.read_replacement_locked()?;
            // Activation carries the legacy ledger over, unchanged or with the
            // write that triggered it.
            invariant::check(|| {
                if before == *state {
                    Ok(())
                } else {
                    validate_inventory_successor(&before, state, RevisionStep::Forward)
                }
            })?;
            self.exchange_slot(&codec::encode(&before)?)?;
        }
        let previous = cache.snapshot.as_ref().map(|snapshot| snapshot.epoch);
        let (epoch, start) = self.write_checkpoint(&bytes, previous.as_ref())?;
        cache.snapshot = Some(Snapshot {
            epoch,
            state: state.clone(),
            index: Index::new(state)?,
            cursor: start,
            start,
            operations: 0,
        });
        cache.pending = None;
        cache.pending_operations = 0;
        Ok(())
    }

    /// Writes `bytes` as a checkpoint under a new epoch, replacing the
    /// checkpoint at epoch `previous`, if any.
    fn write_checkpoint(
        &self,
        bytes: &[u8],
        previous: Option<&[u8; 32]>,
    ) -> Result<([u8; 32], usize), MetadataError> {
        let start = align(BLOCK + bytes.len());
        self.ensure_journal_capacity(start + WINDOW + BLOCK)?;
        let epoch = PinToken::fresh()?.0;
        invariant::check(|| validate_checkpoint_epoch(previous, &epoch))?;
        let mut header = [0; BLOCK];
        header[..8].copy_from_slice(MAGIC);
        header[8..40].copy_from_slice(&epoch);
        header[40..48].copy_from_slice(&(bytes.len() as u64).to_le_bytes());
        header[48..80].copy_from_slice(blake3::hash(bytes).as_bytes());
        let checksum = *blake3::hash(&header[..80]).as_bytes();
        header[80..112].copy_from_slice(&checksum);
        let spare = self.spare_path()?;
        let mut file = std::fs::File::options()
            .write(true)
            .open(&spare)
            .map_err(io)?;
        file.write_all(&header).map_err(io)?;
        file.write_all(bytes).map_err(io)?;
        file.seek(SeekFrom::Start(start as u64)).map_err(io)?;
        file.write_all(&[0; HEADER]).map_err(io)?;
        #[cfg(test)]
        self.ledger_checkpoint("checkpoint-before-sync")?;
        file.sync_all().map_err(io)?;
        #[cfg(test)]
        self.ledger_checkpoint("checkpoint-before-exchange")?;
        self.exchange_checkpoint(&spare).map_err(io)?;
        #[cfg(test)]
        self.ledger_checkpoint("checkpoint-after-exchange")?;
        std::fs::File::open(self.parent())
            .and_then(|file| file.sync_all())
            .map_err(io)?;
        let stats = local_stats(self)?;
        stats
            .checkpoints
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        stats.bytes.fetch_add(
            (BLOCK + bytes.len() + HEADER) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        stats
            .syncs
            .fetch_add(2, std::sync::atomic::Ordering::Relaxed);
        #[cfg(test)]
        self.ledger_checkpoint("checkpoint-after-directory-sync")?;
        Ok((epoch, start))
    }
}
fn local_stats(store: &FilePinStore) -> Result<Arc<super::group::Stats>, MetadataError> {
    Ok(store.local()?.stats.clone())
}

#[cfg(test)]
mod tests;
