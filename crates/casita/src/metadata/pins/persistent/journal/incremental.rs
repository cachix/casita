//! Derived indexes and touched-record tracking, including protection additions.
use super::*;

#[derive(Default)]
pub(super) struct Touched {
    pins: BTreeMap<PinToken, Option<DataPin>>,
    // Protect only adds resources. Keep its delta instead of cloning the
    // growing pin and removing/reinserting every existing index entry.
    extensions: BTreeMap<PinToken, BTreeSet<PinResource>>,
    deletions: BTreeMap<PinToken, Option<BTreeSet<PinResource>>>,
}
impl Touched {
    pub(super) fn pin(&mut self, state: &PinInventory, token: &PinToken) {
        self.pins
            .entry(token.clone())
            .or_insert_with(|| state.pins.get(token).cloned());
    }
    pub(super) fn deletion(&mut self, state: &PinInventory, token: &PinToken) {
        self.deletions
            .entry(token.clone())
            .or_insert_with(|| state.deletions.get(token).cloned());
    }
    pub(super) fn apply_extensions(
        &mut self,
        state: &mut PinInventory,
        extensions: BTreeMap<PinToken, DataPin>,
    ) -> Result<(), MetadataError> {
        for (token, extension) in extensions {
            if self.pins.contains_key(&token)
                || state.retired.contains(&token)
                || extension.scope != PinScope::Staging
                || extension.catalog.is_some()
                || extension.resources.is_empty()
            {
                return Err(bad());
            }
            let pin = state.pins.get_mut(&token).ok_or_else(bad)?;
            if !pin.resources.is_disjoint(&extension.resources) {
                return Err(bad());
            }
            pin.resources.extend(extension.resources.iter().cloned());
            pin.validate()?;
            self.extensions.insert(token, extension.resources);
        }
        Ok(())
    }
    pub(super) fn diff(before: &PinInventory, after: &PinInventory) -> Self {
        let mut touched = Self::default();
        for key in before.pins.keys().chain(after.pins.keys()) {
            if before.pins.get(key) != after.pins.get(key) {
                touched.pin(before, key);
            }
        }
        for key in before.deletions.keys().chain(after.deletions.keys()) {
            if before.deletions.get(key) != after.deletions.get(key) {
                touched.deletion(before, key);
            }
        }
        touched
    }
}

pub(super) struct Changes {
    pub revision: u64,
    pins: BTreeMap<PinToken, bool>,
    extensions: BTreeMap<PinToken, BTreeSet<PinResource>>,
    deletions: BTreeMap<PinToken, bool>,
}
impl Changes {
    pub fn new(revision: u64) -> Self {
        Self {
            revision,
            pins: BTreeMap::new(),
            extensions: BTreeMap::new(),
            deletions: BTreeMap::new(),
        }
    }
    pub fn absorb(&mut self, touched: &Touched) {
        for (key, before) in &touched.pins {
            self.pins.entry(key.clone()).or_insert(before.is_some());
            // A full replacement/removal subsumes preceding additions. This
            // also handles release during collection, which retains the pin.
            self.extensions.remove(key);
        }
        for (key, resources) in &touched.extensions {
            // Newly registered or otherwise replaced pins are encoded whole
            // from the final state, including any subsequent additions.
            if !self.pins.contains_key(key) {
                self.extensions
                    .entry(key.clone())
                    .or_default()
                    .extend(resources.iter().cloned());
            }
        }
        for (key, before) in &touched.deletions {
            self.deletions
                .entry(key.clone())
                .or_insert(before.is_some());
        }
    }
    /// Pins and deletion claims this frame replaces, removes or extends.
    pub fn touched(
        &self,
    ) -> (
        impl Iterator<Item = &PinToken>,
        impl Iterator<Item = &PinToken>,
    ) {
        (
            self.pins.keys().chain(self.extensions.keys()),
            self.deletions.keys(),
        )
    }
    pub fn frame(&self) -> &'static [u8; 8] {
        if self.extensions.is_empty() {
            FRAME
        } else {
            EXTENSION_FRAME
        }
    }
    pub fn encode(&self, state: &PinInventory) -> Result<Vec<u8>, MetadataError> {
        let changed = PinInventory {
            revision: state.revision,
            reader_owners: state.reader_owners.clone(),
            reader_revision_ceiling: state.reader_revision_ceiling,
            pins: self
                .pins
                .keys()
                .filter_map(|key| {
                    state
                        .pins
                        .get(key)
                        .map(|value| (key.clone(), value.clone()))
                })
                .collect(),
            deletions: self
                .deletions
                .keys()
                .filter_map(|key| {
                    state
                        .deletions
                        .get(key)
                        .map(|value| (key.clone(), value.clone()))
                })
                .collect(),
            logical_prune: state.logical_prune.clone(),
            collector: state.collector.clone(),
            retired: BTreeSet::new(),
        };
        let encoded = codec::encode(&changed)?;
        let mut output = (encoded.len() as u64).to_le_bytes().to_vec();
        output.extend(encoded);
        let removed_pins: Vec<_> = self
            .pins
            .iter()
            .filter(|(key, existed)| **existed && !state.pins.contains_key(*key))
            .map(|(key, _)| key)
            .collect();
        let removed_deletions: Vec<_> = self
            .deletions
            .iter()
            .filter(|(key, existed)| **existed && !state.deletions.contains_key(*key))
            .map(|(key, _)| key)
            .collect();
        for tokens in [
            removed_pins,
            removed_deletions,
            state.retired.iter().collect(),
        ] {
            output.extend_from_slice(&(tokens.len() as u64).to_le_bytes());
            for token in tokens {
                output.extend_from_slice(&token.0);
            }
        }
        if !self.extensions.is_empty() {
            // Reuse the bounded resource codec. These synthetic staging pins
            // contain additions only; their scope/catalog are not replacements.
            let extensions = PinInventory {
                pins: self
                    .extensions
                    .iter()
                    .map(|(token, resources)| {
                        (
                            token.clone(),
                            DataPin {
                                scope: PinScope::Staging,
                                catalog: None,
                                resources: resources.clone(),
                            },
                        )
                    })
                    .collect(),
                ..Default::default()
            };
            output.extend(codec::encode(&extensions)?);
        }
        // Oversized deltas become checkpoints; they are never written as frames.
        Ok(output)
    }
}

#[derive(Default)]
pub(super) struct Index {
    pub bytes: usize,
    protected: BTreeMap<PinResource, usize>,
    deleting: BTreeMap<PinResource, usize>,
}
fn adjust(counts: &mut BTreeMap<PinResource, usize>, resources: &BTreeSet<PinResource>, add: bool) {
    for resource in resources {
        if add {
            *counts.entry(resource.clone()).or_default() += 1;
        } else {
            let count = counts.get_mut(resource).expect("indexed resource");
            *count -= 1;
            if *count == 0 {
                counts.remove(resource);
            }
        }
    }
}
impl Index {
    pub fn new(state: &PinInventory) -> Result<Self, MetadataError> {
        let mut index = Self {
            bytes: codec::encoded_len(state)?,
            ..Self::default()
        };
        for pin in state.pins.values() {
            adjust(&mut index.protected, &pin.resources, true);
        }
        for resources in state.deletions.values() {
            adjust(&mut index.deleting, resources, true);
        }
        Ok(index)
    }
    pub fn update(
        &mut self,
        before_globals: usize,
        touched: &Touched,
        state: &PinInventory,
    ) -> Result<(), MetadataError> {
        self.bytes = self.bytes - before_globals + codec::globals_len(state);
        for (key, before) in &touched.pins {
            if let Some(pin) = before {
                self.bytes -= codec::pin_len(pin);
                adjust(&mut self.protected, &pin.resources, false);
            }
            if let Some(pin) = state.pins.get(key) {
                self.bytes += codec::pin_len(pin);
                adjust(&mut self.protected, &pin.resources, true);
            }
        }
        for resources in touched.extensions.values() {
            // The resource-count prefix is already included in the pin size.
            self.bytes += codec::resources_len(resources) - 4;
            adjust(&mut self.protected, resources, true);
        }
        for (key, before) in &touched.deletions {
            if let Some(resources) = before {
                self.bytes -= 32 + codec::resources_len(resources);
                adjust(&mut self.deleting, resources, false);
            }
            if let Some(resources) = state.deletions.get(key) {
                self.bytes += 32 + codec::resources_len(resources);
                adjust(&mut self.deleting, resources, true);
            }
        }
        if self.bytes > codec::MAX_BYTES {
            return Err(backend("oversized pin inventory"));
        }
        Ok(())
    }
    fn deleting(&self, resources: &BTreeSet<PinResource>) -> bool {
        resources.iter().any(|key| self.deleting.contains_key(key))
    }
    fn protected(&self, resources: &BTreeSet<PinResource>) -> bool {
        resources.iter().any(|key| self.protected.contains_key(key))
    }
}

// Mirrors MemoryPinStore's arbitration, with resource lookups through the index.
// Differential protocol tests exercise these outcomes against that reference.
fn edit(
    state: &mut PinInventory,
    index: &Index,
    operation: &Operation,
    readers: &super::super::readers::ReaderState,
    live: &BTreeSet<PinToken>,
    touched: &mut Touched,
) -> Result<Outcome, MetadataError> {
    match operation {
        Operation::AcquireCollection(_)
        | Operation::BeginPruneValidating(_, _)
        | Operation::ClaimValidated(_, _, _) => {
            unreachable!("full-inventory operations bypass the incremental editor")
        }
        Operation::Register(pin) => {
            pin.validate()?;
            if (state.logical_prune.is_some() && pin.scope != PinScope::Metadata)
                || index.deleting(&pin.resources)
            {
                return Ok(Outcome::Token(None));
            }
            let token = PinToken::fresh()?;
            state.advance()?;
            touched.pin(state, &token);
            state.pins.insert(token.clone(), pin.clone());
            Ok(Outcome::Token(Some(token)))
        }
        Operation::Protect(token, resources) => {
            if state.retired.contains(token) {
                return Err(backend("pin has been released"));
            }
            let pin = state
                .pins
                .get(token)
                .ok_or_else(|| backend("pin no longer exists"))?;
            if pin.scope == PinScope::Metadata && !metadata_resources(resources) {
                return Err(MetadataError::Corruption(
                    "metadata pin cannot acquire payload or logical resources".into(),
                ));
            }
            if resources.is_subset(&pin.resources) {
                return Ok(Outcome::Protected(true));
            }
            if (state.logical_prune.is_some() && pin.scope != PinScope::Metadata)
                || index.deleting(resources)
            {
                return Ok(Outcome::Protected(false));
            }
            state.advance()?;
            let pin = state.pins.get_mut(token).unwrap();
            let additions: BTreeSet<_> = resources.difference(&pin.resources).cloned().collect();
            pin.resources.extend(additions.iter().cloned());
            touched.extensions.insert(token.clone(), additions);
            Ok(Outcome::Protected(true))
        }
        Operation::Release(token) => {
            if state.pins.contains_key(token) && !state.retired.contains(token) {
                state.advance()?;
                touched.pin(state, token);
                if state.collector.is_some() {
                    state.retired.insert(token.clone());
                } else {
                    state.pins.remove(token);
                }
            }
            Ok(Outcome::Finished)
        }
        Operation::Claim(revision, resources)
        | Operation::ClaimDuringPrune(revision, resources, _, _) => {
            let fenced = match operation {
                Operation::ClaimDuringPrune(_, _, collector, prune) => {
                    state.collector.as_ref() == Some(collector)
                        && state.logical_prune.as_ref() == Some(prune)
                }
                _ => state.logical_prune.is_none(),
            };
            if state.revision != *revision
                || !fenced
                || index.deleting(resources)
                || index.protected(resources)
                || live.iter().any(|owner| {
                    readers.owners[owner]
                        .pins
                        .values()
                        .any(|pin| !pin.resources.is_disjoint(resources))
                })
            {
                return Ok(Outcome::Token(None));
            }
            let token = PinToken::fresh()?;
            state.advance()?;
            touched.deletion(state, &token);
            state.deletions.insert(token.clone(), resources.clone());
            Ok(Outcome::Token(Some(token)))
        }
        Operation::FinishDeletion(token) => {
            if state.deletions.contains_key(token) {
                state.advance()?;
                touched.deletion(state, token);
                state.deletions.remove(token);
            }
            Ok(Outcome::Finished)
        }
        Operation::BeginCollection(revision, previous) => {
            if state.revision != *revision || state.collector != *previous {
                return Ok(Outcome::Token(None));
            }
            let token = PinToken::fresh()?;
            state.advance()?;
            state.collector = Some(token.clone());
            Ok(Outcome::Token(Some(token)))
        }
        Operation::FinishCollection(token) => {
            if state.collector.as_ref() != Some(token) {
                return Ok(Outcome::Finished);
            }
            if state.logical_prune.is_some() || !state.deletions.is_empty() {
                return Err(backend(
                    "collector still owns an unfinished prune or deletion",
                ));
            }
            state.advance()?;
            for token in std::mem::take(&mut state.retired) {
                touched.pin(state, &token);
                state.pins.remove(&token);
            }
            state.collector = None;
            Ok(Outcome::Finished)
        }
        Operation::BeginPrune(revision, claims) => {
            if state.revision != *revision
                || state.logical_prune.is_some()
                || !state.deletions.keys().eq(claims.iter())
            {
                return Ok(Outcome::Token(None));
            }
            let token = PinToken::fresh()?;
            state.advance()?;
            state.logical_prune = Some(token.clone());
            Ok(Outcome::Token(Some(token)))
        }
        Operation::FinishPrune(token) => {
            if state.logical_prune.as_ref() == Some(token) {
                state.advance()?;
                state.logical_prune = None;
            }
            Ok(Outcome::Finished)
        }
    }
}

impl FilePinStore {
    fn cached_readers(
        &self,
        durable: &PinInventory,
    ) -> Result<Option<(super::super::readers::ReaderState, BTreeSet<PinToken>)>, MetadataError>
    {
        let live = self.live_owners(durable)?;
        // Owner death and legacy migration retain the existing recovery path.
        if !durable.reader_owners.is_empty() && live.is_empty() {
            return Ok(None);
        }
        let readers = if live.is_empty() {
            super::super::readers::ReaderState::default()
        } else {
            self.read_reader_inventory()?
        };
        if readers.revision > durable.reader_revision_ceiling {
            return Err(backend("reader revision exceeds its durable reservation"));
        }
        let mut tokens = BTreeSet::new();
        for owner in &live {
            let pins = &readers
                .owners
                .get(owner)
                .ok_or_else(|| backend("live reader owner is missing from inventory"))?
                .pins;
            for token in pins.keys() {
                if durable.pins.contains_key(token) || !tokens.insert(token) {
                    return Err(backend("duplicate process reader token"));
                }
            }
        }
        Ok(Some((readers, live)))
    }

    pub(in crate::metadata::pins::persistent) fn register_cached_reader(
        &self,
        pin: &DataPin,
        owner: &PinToken,
    ) -> Result<Option<Outcome>, MetadataError> {
        let local = self.local()?;
        let mut cache = local.cache.lock().map_err(backend)?;
        let result = (|| {
            self.refresh_journal(&mut cache)?;
            let Some(snapshot) = cache.snapshot.as_mut() else {
                return Ok(None);
            };
            let Some((mut readers, _live)) = self.cached_readers(&snapshot.state)? else {
                return Ok(None);
            };
            if snapshot.state.logical_prune.is_some() || snapshot.index.deleting(&pin.resources) {
                return Ok(Some(Outcome::Token(None)));
            }
            pin.validate()?;
            let token = PinToken::fresh()?;
            let next = snapshot
                .state
                .revision
                .max(readers.revision)
                .checked_add(1)
                .ok_or_else(|| backend("pin revision exhausted"))?;
            if next > snapshot.state.reader_revision_ceiling {
                let revision = snapshot.state.revision;
                let globals = codec::globals_len(&snapshot.state);
                snapshot.state.revision = next;
                snapshot.state.reader_revision_ceiling = next
                    .checked_add(super::super::readers::RESERVATION)
                    .ok_or_else(|| backend("pin revision exhausted"))?;
                snapshot
                    .index
                    .update(globals, &Touched::default(), &snapshot.state)?;
                cache.pending.get_or_insert_with(|| Changes::new(revision));
                cache.pending_operations += 1;
                self.flush_journal(&mut cache)?;
            }
            readers.revision = next;
            readers
                .owners
                .get_mut(owner)
                .ok_or_else(|| backend("reader owner was fenced"))?
                .pins
                .insert(token.clone(), pin.clone());
            self.write_readers(&readers)?;
            #[cfg(test)]
            self.checkpoint("reader-registered");
            Ok(Some(Outcome::Token(Some(token))))
        })();
        if result.is_err() {
            *cache = Cache::default();
        }
        result
    }

    pub(in crate::metadata::pins::persistent) fn edit_cached(
        &self,
        operation: &Operation,
    ) -> Result<Option<Outcome>, MetadataError> {
        // These operations validate or return the complete merged inventory.
        // Use the existing full-inventory transition, including live readers.
        if matches!(
            operation,
            Operation::AcquireCollection(_)
                | Operation::BeginPruneValidating(_, _)
                | Operation::ClaimValidated(_, _, _)
        ) {
            return Ok(None);
        }
        #[cfg(test)]
        if self.replacement {
            return Ok(None);
        }
        let local = self.local()?;
        let mut cache = local.cache.lock().map_err(backend)?;
        self.refresh_journal(&mut cache)?;
        let Some(snapshot) = cache.snapshot.as_mut() else {
            return Ok(None);
        };
        let Some((mut readers, live)) = self.cached_readers(&snapshot.state)? else {
            return Ok(None);
        };
        let revision = snapshot.state.revision;
        let visible = revision.max(readers.revision);
        let globals = codec::globals_len(&snapshot.state);
        let reader_token = match operation {
            Operation::Protect(token, _) | Operation::Release(token) => Some(token),
            _ => None,
        };
        if let Some(token) = reader_token
            && let Some(owner) = live
                .iter()
                .find(|owner| readers.owners[*owner].pins.contains_key(token))
        {
            let pin = readers
                .owners
                .get_mut(owner)
                .unwrap()
                .pins
                .get_mut(token)
                .unwrap();
            match operation {
                Operation::Protect(_, resources) => {
                    if snapshot.index.deleting(resources) {
                        return Ok(Some(Outcome::Protected(false)));
                    }
                    if resources.is_subset(&pin.resources) {
                        return Ok(Some(Outcome::Protected(true)));
                    }
                    pin.resources.extend(resources.iter().cloned());
                }
                Operation::Release(_) => {
                    readers.owners.get_mut(owner).unwrap().pins.remove(token);
                }
                _ => unreachable!(),
            }
            let next = visible
                .checked_add(1)
                .ok_or_else(|| backend("pin revision exhausted"))?;
            if next > snapshot.state.reader_revision_ceiling {
                snapshot.state.revision = next;
                snapshot.state.reader_revision_ceiling = next
                    .checked_add(super::super::readers::RESERVATION)
                    .ok_or_else(|| backend("pin revision exhausted"))?;
                snapshot
                    .index
                    .update(globals, &Touched::default(), &snapshot.state)?;
                cache.pending.get_or_insert_with(|| Changes::new(revision));
                cache.pending_operations += 1;
                self.flush_journal(&mut cache)?;
            }
            readers.revision = next;
            self.write_readers(&readers)?;
            #[cfg(test)]
            self.checkpoint(if matches!(operation, Operation::Protect(..)) {
                "reader-protected"
            } else {
                "reader-released"
            });
            return Ok(Some(if matches!(operation, Operation::Protect(..)) {
                Outcome::Protected(true)
            } else {
                Outcome::Finished
            }));
        }
        snapshot.state.revision = visible;
        let mut touched = Touched::default();
        let result = edit(
            &mut snapshot.state,
            &snapshot.index,
            operation,
            &readers,
            &live,
            &mut touched,
        )?;
        if snapshot.state.revision == visible {
            snapshot.state.revision = revision;
            return Ok(Some(result));
        }
        snapshot.state.reader_owners = live;
        snapshot.index.update(globals, &touched, &snapshot.state)?;
        cache
            .pending
            .get_or_insert_with(|| Changes::new(revision))
            .absorb(&touched);
        cache.pending_operations += 1;
        local
            .stats
            .cached_edits
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Some(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn resources(index: usize) -> BTreeSet<PinResource> {
        BTreeSet::from([if index.is_multiple_of(3) {
            PinResource::MetadataObject(format!("shared/{}", index % 7))
        } else {
            PinResource::StorageObject(format!("shared/{}", index % 7))
        }])
    }
    async fn compare(state: &mut PinInventory, index: &mut Index, operation: Operation) {
        let before = state.clone();
        let reference = MemoryPinStore {
            state: Arc::new(tokio::sync::Mutex::new(before.clone())),
        };
        let expected = operation.apply(&reference).await;
        let mut touched = Touched::default();
        let actual = edit(
            state,
            index,
            &operation,
            &Default::default(),
            &BTreeSet::new(),
            &mut touched,
        );
        let mut expected_state = reference.inventory().await.unwrap();
        match (&actual, &expected) {
            (Ok(Outcome::Token(Some(actual))), Ok(Outcome::Token(Some(expected)))) => {
                if let Some(pin) = expected_state.pins.remove(expected) {
                    expected_state.pins.insert(actual.clone(), pin);
                }
                if let Some(claim) = expected_state.deletions.remove(expected) {
                    expected_state.deletions.insert(actual.clone(), claim);
                }
                if expected_state.collector.as_ref() == Some(expected) {
                    expected_state.collector = Some(actual.clone());
                }
                if expected_state.logical_prune.as_ref() == Some(expected) {
                    expected_state.logical_prune = Some(actual.clone());
                }
            }
            (Ok(Outcome::Token(None)), Ok(Outcome::Token(None)))
            | (Ok(Outcome::Finished), Ok(Outcome::Finished)) => {}
            (Ok(Outcome::Protected(a)), Ok(Outcome::Protected(b))) => assert_eq!(a, b),
            (Err(a), Err(b)) => {
                assert_eq!(std::mem::discriminant(a), std::mem::discriminant(b));
                assert_eq!(*state, before);
            }
            _ => panic!("cached and reference outcomes differ"),
        }
        assert_eq!(*state, expected_state);
        index
            .update(codec::globals_len(&before), &touched, state)
            .unwrap();
        assert_eq!(index.bytes, codec::encode(state).unwrap().len());
        let rebuilt = Index::new(state).unwrap();
        assert_eq!(index.protected, rebuilt.protected);
        assert_eq!(index.deleting, rebuilt.deleting);
        if state.revision != before.revision {
            let mut changes = Changes::new(before.revision);
            changes.absorb(&touched);
            let mut replay_index = Index::new(&before).unwrap();
            let mut replayed = before;
            let (replay_touched, replay_globals) = apply(
                &mut replayed,
                &changes.encode(state).unwrap(),
                changes.frame(),
            )
            .unwrap();
            replay_index
                .update(replay_globals, &replay_touched, &replayed)
                .unwrap();
            assert_eq!(replay_index.bytes, index.bytes);
            assert_eq!(replay_index.protected, index.protected);
            assert_eq!(replay_index.deleting, index.deleting);
            assert_eq!(&replayed, state);
        }
    }
    #[tokio::test]
    async fn indexed_edits_match_memory_arbitration_and_exact_replay() {
        let mut state = PinInventory::default();
        let mut index = Index::new(&state).unwrap();
        let unknown = PinToken::fresh().unwrap();
        let mut random = 0xc4517au64;
        for step in 0..1200 {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let pick = (random as usize) % 17;
            let token = state
                .pins
                .keys()
                .nth(pick % state.pins.len().max(1))
                .cloned()
                .unwrap_or_else(|| unknown.clone());
            let revision = state.revision.saturating_sub(u64::from(pick == 0));
            let op = match pick {
                0..=4 => Operation::Register(DataPin {
                    scope: if pick == 0 {
                        PinScope::Metadata
                    } else {
                        PinScope::Staging
                    },
                    catalog: None,
                    resources: resources(step),
                }),
                5 | 6 => Operation::Protect(token, resources(step)),
                7 | 8 => Operation::Release(token),
                9 => Operation::Claim(revision, resources(step)),
                10 => Operation::FinishDeletion(
                    state
                        .deletions
                        .keys()
                        .next()
                        .cloned()
                        .unwrap_or_else(|| unknown.clone()),
                ),
                11 => Operation::BeginCollection(revision, None),
                12 => Operation::FinishCollection(
                    state.collector.clone().unwrap_or_else(|| unknown.clone()),
                ),
                13 => Operation::BeginPrune(revision, state.deletions.keys().cloned().collect()),
                14 => Operation::FinishPrune(
                    state
                        .logical_prune
                        .clone()
                        .unwrap_or_else(|| unknown.clone()),
                ),
                15 => Operation::ClaimDuringPrune(
                    revision,
                    resources(step),
                    state.collector.clone().unwrap_or_else(|| unknown.clone()),
                    state
                        .logical_prune
                        .clone()
                        .unwrap_or_else(|| unknown.clone()),
                ),
                _ => Operation::BeginCollection(revision, state.collector.clone()),
            };
            compare(&mut state, &mut index, op).await;
        }
        state.revision = u64::MAX;
        compare(
            &mut state,
            &mut index,
            Operation::Register(DataPin {
                scope: PinScope::Metadata,
                catalog: None,
                resources: BTreeSet::new(),
            }),
        )
        .await;
    }

    #[tokio::test]
    async fn extensions_preserve_shared_counts_and_duplicate_protection() {
        let token_a = PinToken::fresh().unwrap();
        let token_b = PinToken::fresh().unwrap();
        let retained: BTreeSet<_> = (0..4096)
            .map(|i| PinResource::StorageObject(format!("retained/{i}")))
            .collect();
        let pin = DataPin {
            scope: PinScope::Staging,
            catalog: None,
            resources: retained,
        };
        let mut state = PinInventory {
            revision: 1,
            pins: BTreeMap::from([(token_a.clone(), pin.clone()), (token_b.clone(), pin)]),
            ..Default::default()
        };
        let mut index = Index::new(&state).unwrap();
        let additions = BTreeSet::from([
            PinResource::StorageObject("retained/0".into()),
            PinResource::StorageObject("new".into()),
            PinResource::Catalog(vec![1, 2, 3]),
            PinResource::MetadataObject("new".into()),
        ]);
        for token in [&token_a, &token_b, &token_a] {
            compare(
                &mut state,
                &mut index,
                Operation::Protect(token.clone(), additions.clone()),
            )
            .await;
        }
        compare(&mut state, &mut index, Operation::Release(token_a)).await;
        let revision = state.revision;
        compare(
            &mut state,
            &mut index,
            Operation::Claim(revision, additions.clone()),
        )
        .await;
        assert!(state.deletions.is_empty());
        compare(&mut state, &mut index, Operation::Release(token_b)).await;
        let revision = state.revision;
        compare(
            &mut state,
            &mut index,
            Operation::Claim(revision, additions),
        )
        .await;
        assert_eq!(state.deletions.len(), 1);
    }

    #[tokio::test]
    async fn grouped_net_changes_and_shared_resources_keep_exact_counts() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePinStore::new(dir.path().join("pins"));
        let pin = DataPin {
            scope: PinScope::Staging,
            catalog: None,
            resources: resources(1),
        };
        let a = store.register(pin.clone()).await.unwrap().unwrap();
        let b = store.register(pin).await.unwrap().unwrap();
        let revision = store.inventory().await.unwrap().revision;
        let collector = store
            .begin_collection(revision, None)
            .await
            .unwrap()
            .unwrap();
        store
            .edit_group(&[
                Operation::Protect(a.clone(), resources(2)),
                Operation::Release(a),
                Operation::Release(b),
            ])
            .unwrap();
        let state = store.inventory().await.unwrap();
        assert!(
            store
                .claim_deletions(state.revision, resources(1))
                .await
                .unwrap()
                .is_none()
        );
        store.finish_collection(&collector).await.unwrap();
        let state = store.inventory().await.unwrap();
        let claim = store
            .claim_deletions(state.revision, resources(1))
            .await
            .unwrap()
            .unwrap();
        let cached = store
            .local()
            .unwrap()
            .cache
            .lock()
            .unwrap()
            .snapshot
            .as_ref()
            .unwrap()
            .state
            .clone();
        *store.local().unwrap().cache.lock().unwrap() = Cache::default();
        assert_eq!(store.inventory().await.unwrap(), cached);
        store.finish_deletions(&claim).await.unwrap();
    }
}
