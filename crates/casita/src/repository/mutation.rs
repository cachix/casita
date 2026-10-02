//! Staging, conditional publication, and mutation lifetimes.

use super::*;

/// Private construction evidence and public requests for normal verification
/// must remain distinct, even when they share the publication transaction.
#[derive(Default)]
pub(super) struct ClosurePublication {
    pub(super) constructed: BTreeSet<ObjectKey>,
    pub(super) requested: BTreeSet<ObjectKey>,
}

impl ClosurePublication {
    fn constructed(constructed: BTreeSet<ObjectKey>) -> Self {
        Self {
            constructed,
            requested: BTreeSet::new(),
        }
    }
}

/// One root value that must still match before a conditional mutation can
/// commit.
///
/// `None` requires the name to be absent. Expectations are checked again after
/// every repository revision race, so unrelated mutations can be retried
/// without overwriting a concurrently changed root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootExpectation {
    /// Exact root name to compare.
    pub name: RootName,
    /// Exact required target, or `None` when the root must be absent.
    pub target: Option<ObjectKey>,
}

/// Outcome of a mutation guarded by root-value expectations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConditionalPublishResult {
    /// Staged objects and root changes committed atomically.
    Committed(CommitResult),
    /// A watched root no longer has the value used to derive the mutation.
    RootMismatch {
        /// First mismatched name in canonical root-name order.
        name: RootName,
        /// Value required by the caller.
        expected: Option<ObjectKey>,
        /// Value observed in the current snapshot.
        actual: Option<ObjectKey>,
    },
}

/// A verified payload staged in this repository during a live mutation
/// session. Its private seal prevents publishing verification performed
/// against another physical store.
#[derive(Debug)]
pub struct StagedObject<'hold> {
    pub(super) verified: VerifiedObject,
    pub(super) repository: Arc<()>,
    pub(super) _hold: std::marker::PhantomData<&'hold ()>,
}

impl StagedObject<'_> {
    /// Verified immutable record that mutation will install.
    pub fn record(&self) -> &ObjectRecord {
        self.verified.record()
    }
}

#[cfg(test)]
pub(crate) const PUBLICATION_PHASES: [&str; 6] = [
    "coordination_wait",
    "snapshot",
    "validation",
    "payload_prepare",
    "state_commit",
    "payload_finish",
];

#[cfg(test)]
#[derive(Clone, Copy, Default)]
pub(crate) struct PublicationProfile {
    pub(crate) calls: [u64; 6],
    pub(crate) nanos: [u64; 6],
    /// Most keys one closure walk collected for witnesses.
    pub(crate) witness_inventory_peak: usize,
}

#[cfg(test)]
pub(super) struct PublicationTimer<'a> {
    pub(super) profile: &'a std::sync::Mutex<PublicationProfile>,
    pub(super) phase: usize,
    pub(super) started: std::time::Instant,
}

#[cfg(test)]
impl Drop for PublicationTimer<'_> {
    fn drop(&mut self) {
        let nanos = self
            .started
            .elapsed()
            .as_nanos()
            .try_into()
            .unwrap_or(u64::MAX);
        let mut profile = self.profile.lock().unwrap();
        profile.calls[self.phase] += 1;
        profile.nanos[self.phase] += nanos;
    }
}

impl<PS, SS> Repository<PS, SS> {
    /// Import an external input through its format-specific request.
    #[cfg(feature = "experimental")]
    pub async fn import<I: crate::import::Importer<Self>>(
        &self,
        input: I,
    ) -> Result<I::Report, I::Error> {
        input.import(self).await
    }

    #[cfg(not(feature = "experimental"))]
    pub(crate) async fn import<I: crate::importers::BackendImporter<Self>>(
        &self,
        input: I,
    ) -> Result<I::Report, I::Error> {
        input.import_into(self).await
    }

    #[cfg(test)]
    pub(crate) fn publication_profile(&self) -> PublicationProfile {
        *self.publication_profile.lock().unwrap()
    }

    #[cfg(test)]
    pub(crate) fn pause_catalog_maintenance_for_test(&self) -> Arc<publication::MaintenancePause> {
        self.publication.pause_maintenance()
    }

    #[cfg(test)]
    pub(super) fn record_witness_inventory(&self, keys: usize) {
        let mut profile = self.publication_profile.lock().unwrap();
        profile.witness_inventory_peak = profile.witness_inventory_peak.max(keys);
    }

    #[cfg(test)]
    pub(super) fn time_publication_phase(&self, phase: usize) -> PublicationTimer<'_> {
        PublicationTimer {
            profile: &self.publication_profile,
            phase,
            started: std::time::Instant::now(),
        }
    }
}

impl<PS, SS> Repository<PS, SS>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    pub(super) async fn commit_payload_catalog(
        &self,
        protection: publication::Protection,
    ) -> Result<Option<CommitResult>, RepositoryError> {
        self.commit_payload_catalog_from(None, protection).await
    }

    #[tracing::instrument(name = "repository.commit_payload_catalog", skip_all)]
    pub(super) async fn commit_payload_catalog_from(
        &self,
        mut expected: Option<crate::RepositoryRevision>,
        protection: publication::Protection,
    ) -> Result<Option<CommitResult>, RepositoryError> {
        let _phase = CollectionPhase::new("catalog_commit");
        let mut state_commit = self.publication.lock().await;
        if state_commit.is_none() {
            return Ok(None);
        }
        let mut retry = publication::PublicationRetry::new();
        loop {
            let _attempt = CollectionPhase::new("catalog_commit_attempt");
            let revision = match expected.take() {
                Some(revision) => revision,
                None => self.synchronized_snapshot().await?.revision(),
            };
            let (guard, committed) = self
                .publication
                .commit(
                    state_commit.take(),
                    protection.clone(),
                    revision,
                    None,
                    #[cfg(test)]
                    self.publication_profile.clone(),
                )
                .await;
            state_commit = guard;
            match committed {
                Ok(commit) => return Ok(commit),
                Err(
                    error @ RepositoryError::Metadata(
                        MetadataError::StaleRevision { .. } | MetadataError::MaintenanceFenced,
                    ),
                ) => {
                    if !retry.wait().await {
                        return Err(error);
                    }
                    tracing::debug!(%error, "retrying payload catalog commit with a fresh snapshot");
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Begin a mutation lifetime protected from collection.
    ///
    /// The standard local profile first probes disk pressure and attempts one
    /// nonblocking collection when usage reaches the configured threshold. A
    /// busy repository skips that opportunistic pass and admits the mutation;
    /// a later mutation retries it. Generic repository compositions install no
    /// automatic start policy.
    #[tracing::instrument(name = "repository.mutation_session", skip_all)]
    pub async fn mutation_session(&self) -> Result<MutationSession<'_, PS, SS>, RepositoryError> {
        let discovery_refreshed = match self.profile.mutation_start() {
            Some(start) => start.before_mutation(self.profile.spill_limits()).await?,
            None => false,
        };
        let pin = crate::metadata::DataPinLease::acquire(
            self.state.pin_store().await?,
            crate::metadata::DataPin {
                scope: crate::metadata::PinScope::Staging,
                catalog: None,
                resources: BTreeSet::new(),
            },
        )
        .await?;
        let payload_batch = self.payloads.begin_pinned_batch(pin.clone())?;
        // One discovery refresh is sufficient for the complete mutation. A
        // racing writer may cause a redundant immutable upload, but cannot
        // change identity; refreshing every negative probe would amplify one
        // logical transfer into thousands of object-store requests.
        self.publication
            .admit_mutation(&self.payloads, &self.state, discovery_refreshed, &pin)
            .await?;
        Ok(MutationSession {
            repository: self,
            pin,
            protection: Arc::new(DataProtection {
                _payload_batch: payload_batch,
                _pin: None,
            }),
        })
    }

    /// Remove a name only if it still points at the caller's recorded key.
    pub async fn remove_root_if_matches(
        &self,
        name: &RootName,
        expected: &ObjectKey,
    ) -> Result<Option<CommitResult>, RepositoryError> {
        self.mutation_session()
            .await?
            .remove_root_if_matches(name, expected)
            .await
    }
}

/// One coordinated staging and mutation lifetime.
pub struct MutationSession<'a, PS, SS> {
    pub(super) repository: &'a Repository<PS, SS>,
    pub(super) protection: Arc<DataProtection>,
    pub(super) pin: crate::metadata::DataPinLease,
}

impl<PS, SS> MutationSession<'_, PS, SS> {
    /// Repository protected by this mutation/collection hold.
    pub fn repository(&self) -> &Repository<PS, SS> {
        self.repository
    }
}

impl<PS, SS> MutationSession<'_, PS, SS>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    pub(crate) fn write_scope(&self) -> crate::metadata::BackendWriteScope {
        self.repository
            .payloads
            .write_scope()
            .with_pin(self.pin.clone())
    }

    /// Retain selected objects and their closures before reading reusable state.
    pub(crate) async fn retain_objects(
        &self,
        keys: impl IntoIterator<Item = ObjectKey>,
    ) -> Result<(), RepositoryError> {
        self.pin
            .protect(
                keys.into_iter()
                    .map(crate::metadata::PinResource::Object)
                    .collect(),
            )
            .await?;
        Ok(())
    }

    // Catalog protection belongs to the operation using this snapshot, not to
    // the entire mutation. Otherwise every publication retains another full
    // catalog and a long import eventually fills the pin inventory.
    pub(super) async fn pinned_snapshot(
        &self,
    ) -> Result<(Arc<dyn MetadataSnapshot>, crate::metadata::DataPinLease), RepositoryError> {
        self.write_scope()
            .run(async {
                let (snapshot, pin) =
                    pin_metadata_snapshot_kind(self.repository.state.as_ref(), true, true, None)
                        .await?;
                self.repository
                    .publication
                    .synchronize(&self.repository.payloads, snapshot.as_ref())
                    .await?;
                Ok((snapshot, pin))
            })
            .await
    }

    /// Store and verify raw bytes as `casita.blob.v1`.
    ///
    /// Same-length edits can use [`Self::stage_blob_overwrite`] to reuse
    /// authenticated BLAKE3 subtrees and unchanged physical chunks.
    #[tracing::instrument(
        name = "repository.stage_blob",
        level = "debug",
        skip_all,
        fields(bytes = bytes.len())
    )]
    pub async fn stage_blob<'hold>(
        &'hold self,
        bytes: &[u8],
    ) -> Result<StagedObject<'hold>, RepositoryError> {
        self.write_scope()
            .run(async {
                let payload_size = bytes.len() as u64;
                if payload_size > self.repository.limits.max_payload_bytes {
                    return Err(FormatError::PayloadLimit {
                        limit: self.repository.limits.max_payload_bytes,
                    }
                    .into());
                }
                let expected = BlobId::new(blake3::hash(bytes).into());
                let payload = self.repository.payloads.put_slice(bytes).await?;
                if payload != expected {
                    return Err(RepositoryError::PayloadIdentityMismatch {
                        expected,
                        actual: payload,
                    });
                }
                let verified =
                    BlobFormat::seal_written(payload, payload_size, &self.repository.limits)?;
                Ok(StagedObject {
                    verified,
                    repository: self.repository.staging_identity.clone(),
                    _hold: std::marker::PhantomData,
                })
            })
            .await
    }

    /// Stage a same-length edit to an existing raw blob. The backend supplies
    /// a proof of the old bytes; the repository independently authenticates
    /// that proof and derives the new content ID before sealing publication.
    pub async fn stage_blob_overwrite<'hold>(
        &'hold self,
        key: &ObjectKey,
        offset: u64,
        replacement: &[u8],
    ) -> Result<StagedObject<'hold>, RepositoryError> {
        self.write_scope()
            .run(async {
                self.retain_objects([key.clone()]).await?;
                let (snapshot, _snapshot_pin) = self.pinned_snapshot().await?;
                let record = snapshot
                    .object(key)
                    .await?
                    .ok_or_else(|| RepositoryError::Absent(key.to_string()))?;
                if record.key() != &ObjectKey::blob(record.payload()) || !record.links().is_empty()
                {
                    return Err(crate::error::Error::Msg(
                        "overwrite requires a raw blob object".into(),
                    )
                    .into());
                }
                crate::verified::patch::aligned(
                    record.payload_size(),
                    offset,
                    replacement.len() as u64,
                )
                .map_err(crate::error::Error::from)?;
                let (actual, proof) = self
                    .repository
                    .payloads
                    .overwrite(
                        &record.payload(),
                        record.payload_size(),
                        offset,
                        replacement,
                    )
                    .await?;
                let expected = crate::verified::patch::verify(
                    &proof,
                    record.payload(),
                    record.payload_size(),
                    offset,
                    replacement,
                )
                .await?
                .digest;
                if actual != expected {
                    return Err(RepositoryError::PayloadIdentityMismatch { expected, actual });
                }
                let verified = BlobFormat::seal_written(
                    actual,
                    record.payload_size(),
                    &self.repository.limits,
                )?;
                Ok(StagedObject {
                    verified,
                    repository: self.repository.staging_identity.clone(),
                    _hold: std::marker::PhantomData,
                })
            })
            .await
    }

    /// Store bytes and verify them under an exact caller-supplied logical key.
    /// The selected namespace still reproduces and checks the native identity,
    /// canonical payload, and forward links before returning a staged seal.
    pub async fn stage_object<'hold>(
        &'hold self,
        key: ObjectKey,
        bytes: &[u8],
    ) -> Result<StagedObject<'hold>, RepositoryError> {
        self.write_scope()
            .run(async {
                let mut reader =
                    BlobPayloadReader::new(std::io::Cursor::new(bytes), bytes.len() as u64);
                #[cfg(all(test, feature = "git"))]
                let timer = crate::git::repository::import_profile::time(5);
                let verified = self
                    .repository
                    .formats
                    .verify(&key, &mut reader, &self.repository.limits)
                    .await?;
                #[cfg(all(test, feature = "git"))]
                drop(timer);
                #[cfg(all(test, feature = "git"))]
                let timer = crate::git::repository::import_profile::time(6);
                let payload = self.repository.payloads.put_slice(bytes).await?;
                #[cfg(all(test, feature = "git"))]
                drop(timer);
                if verified.record().payload() != payload {
                    return Err(RepositoryError::PayloadIdentityMismatch {
                        expected: verified.record().payload(),
                        actual: payload,
                    });
                }
                Ok(StagedObject {
                    verified,
                    repository: self.repository.staging_identity.clone(),
                    _hold: std::marker::PhantomData,
                })
            })
            .await
    }

    /// Stream bytes and verify them under an exact caller-supplied logical key.
    ///
    /// This is the streaming counterpart to [`stage_object`](Self::stage_object):
    /// the payload store computes its physical identity while consuming the
    /// reader, then the selected logical format independently reproduces the
    /// caller-supplied identity and extracts canonical links.
    #[tracing::instrument(name = "repository.stage_object", level = "debug", skip_all)]
    pub async fn stage_object_reader<'hold>(
        &'hold self,
        key: ObjectKey,
        reader: &mut (impl AsyncRead + Unpin),
    ) -> Result<StagedObject<'hold>, RepositoryError> {
        self.write_scope()
            .run(async {
                let mut writer = self.repository.payloads.open_write().await;
                tokio::io::copy(reader, &mut writer).await?;
                let (payload, _) = writer.close().await?;
                self.stage_existing(key, payload).await
            })
            .await
    }

    /// Stream and verify raw bytes as `casita.blob.v1`.
    #[tracing::instrument(name = "repository.stage_blob", level = "debug", skip_all)]
    pub async fn stage_blob_reader<'hold>(
        &'hold self,
        reader: &mut (impl AsyncRead + Unpin),
    ) -> Result<StagedObject<'hold>, RepositoryError> {
        self.write_scope()
            .run(async {
                let mut writer = self.repository.payloads.open_write().await;
                let mut hasher = blake3::Hasher::new();
                let mut observed_size = 0u64;
                let mut buffer = vec![0u8; self.repository.limits.read_buffer_bytes.max(1)];
                loop {
                    let read = reader.read(&mut buffer).await?;
                    if read == 0 {
                        break;
                    }
                    observed_size = observed_size
                        .checked_add(read as u64)
                        .ok_or(FormatError::PayloadSizeOverflow)?;
                    if observed_size > self.repository.limits.max_payload_bytes {
                        return Err(FormatError::PayloadLimit {
                            limit: self.repository.limits.max_payload_bytes,
                        }
                        .into());
                    }
                    hasher.update(&buffer[..read]);
                    writer.write_all(&buffer[..read]).await?;
                }
                let (payload, payload_size) = writer.close().await?;
                let expected = BlobId::new(hasher.finalize().into());
                if payload != expected {
                    return Err(RepositoryError::PayloadIdentityMismatch {
                        expected,
                        actual: payload,
                    });
                }
                if payload_size != observed_size {
                    return Err(RepositoryError::PayloadSizeMismatch {
                        expected: observed_size,
                        actual: payload_size,
                    });
                }
                let verified =
                    BlobFormat::seal_written(payload, payload_size, &self.repository.limits)?;
                Ok(StagedObject {
                    verified,
                    repository: self.repository.staging_identity.clone(),
                    _hold: std::marker::PhantomData,
                })
            })
            .await
    }

    /// Store and verify a canonical filesystem directory as
    /// `casita.directory.v1`.
    #[tracing::instrument(
        name = "repository.stage_directory",
        level = "debug",
        skip_all,
        fields(entries = directory.len())
    )]
    pub async fn stage_directory<'hold>(
        &'hold self,
        directory: &Directory,
    ) -> Result<StagedObject<'hold>, RepositoryError> {
        self.write_scope()
            .run(async {
                let encoded = directory.encode();
                let payload = self.repository.payloads.put_slice(&encoded).await?;
                let verified = DirectoryFormat::seal_written(
                    directory,
                    payload,
                    encoded.len() as u64,
                    &self.repository.limits,
                )?;
                Ok(StagedObject {
                    verified,
                    repository: self.repository.staging_identity.clone(),
                    _hold: std::marker::PhantomData,
                })
            })
            .await
    }

    /// Durably protect a bounded set of existing records before staging them.
    /// This grants no trust: callers must still verify every record through
    /// `stage_existing`. Its repeated protection checks use the confirmed cache.
    pub(crate) async fn protect_existing_records(
        &self,
        records: &[ObjectRecord],
    ) -> Result<(), RepositoryError> {
        if records.len() > self.repository.limits.max_batch_objects {
            return Err(RepositoryError::LimitExceeded(
                "existing-record protection exceeds mutation batch limit".to_owned(),
            ));
        }
        self.pin
            .protect(
                records
                    .iter()
                    .flat_map(|record| {
                        [
                            crate::metadata::PinResource::Object(record.key().clone()),
                            crate::metadata::PinResource::Blob(record.payload()),
                        ]
                    })
                    .collect(),
            )
            .await?;
        Ok(())
    }

    /// Register a stored, repository-verified native Git blob as an ordinary
    /// file without reading or writing its payload again. Git blob bodies are
    /// exactly file contents; their verified records already authenticate the
    /// raw payload digest and length. Other Git kinds are rejected.
    #[cfg(feature = "git")]
    #[tracing::instrument(name = "repository.stage_git_blob_file", level = "debug", skip_all)]
    pub async fn stage_git_blob_file<'hold>(
        &'hold self,
        key: &ObjectKey,
    ) -> Result<StagedObject<'hold>, RepositoryError> {
        let mut staged = self.stage_git_blob_files(std::slice::from_ref(key)).await?;
        Ok(staged.pop().expect("one staged file per Git blob"))
    }

    /// Register stored native Git blobs as ordinary files, in order, with one
    /// metadata read, one protection request and one recheck for the batch.
    ///
    /// The count is bounded by `max_batch_objects`. A repository with a
    /// replacement format registry must stage file bytes normally: its blob
    /// verifier may impose rules a Git record does not prove.
    #[cfg(feature = "git")]
    #[tracing::instrument(
        name = "repository.stage_git_blob_files",
        level = "debug",
        skip_all,
        fields(blobs = keys.len())
    )]
    pub async fn stage_git_blob_files<'hold>(
        &'hold self,
        keys: &[ObjectKey],
    ) -> Result<Vec<StagedObject<'hold>>, RepositoryError> {
        if keys.len() > self.repository.limits.max_batch_objects {
            return Err(RepositoryError::LimitExceeded(format!(
                "registration has {} Git blobs, limit is {}",
                keys.len(),
                self.repository.limits.max_batch_objects
            )));
        }
        if !self.repository.formats.is_builtin() {
            return Err(RepositoryError::InvalidInput(
                "Git blob registration requires the built-in format registry".into(),
            ));
        }
        for key in keys {
            let (_, kind, _) = crate::git::git_key_parts(key)
                .map_err(|error| RepositoryError::InvalidInput(error.to_string()))?;
            if kind != crate::git::GitObjectKind::Blob {
                return Err(RepositoryError::InvalidInput(format!(
                    "plain files require a native Git blob, not {key}"
                )));
            }
        }
        self.write_scope()
            .run(async {
                // Read metadata first to discover all required protections.
                // Recheck after admission: collection may have removed a
                // candidate before the receiving mutation could protect it.
                let records = {
                    let (snapshot, _metadata_pin) =
                        crate::metadata::read_snapshot(self.repository.state.as_ref()).await?;
                    snapshot.object_batch(keys).await?
                };
                let mut verified = Vec::with_capacity(keys.len());
                let mut resources = BTreeSet::new();
                for (key, record) in keys.iter().zip(&records) {
                    let record = record
                        .as_ref()
                        .ok_or_else(|| RepositoryError::Absent(key.to_string()))?;
                    if !record.links().is_empty() {
                        return Err(RepositoryError::InvalidInput(
                            "Git blobs cannot have forward links".into(),
                        ));
                    }
                    let file = BlobFormat::seal_written(
                        record.payload(),
                        record.payload_size(),
                        &self.repository.limits,
                    )?;
                    resources.insert(crate::metadata::PinResource::Object(key.clone()));
                    resources.insert(crate::metadata::PinResource::Object(
                        file.record().key().clone(),
                    ));
                    resources.insert(crate::metadata::PinResource::Blob(record.payload()));
                    verified.push(file);
                }
                self.pin.protect(resources).await?;
                let (current, _current_pin) =
                    crate::metadata::read_snapshot(self.repository.state.as_ref()).await?;
                for ((key, before), after) in keys
                    .iter()
                    .zip(&records)
                    .zip(current.object_batch(keys).await?)
                {
                    if before.as_ref() != after.as_ref() {
                        return Err(RepositoryError::Absent(key.to_string()));
                    }
                }
                Ok(verified
                    .into_iter()
                    .map(|verified| StagedObject {
                        verified,
                        repository: self.repository.staging_identity.clone(),
                        _hold: std::marker::PhantomData,
                    })
                    .collect())
            })
            .await
    }

    /// Verify an already durable payload under an exact logical key.
    #[tracing::instrument(name = "repository.stage_existing", level = "debug", skip_all)]
    pub async fn stage_existing<'hold>(
        &'hold self,
        key: ObjectKey,
        payload: BlobId,
    ) -> Result<StagedObject<'hold>, RepositoryError> {
        self.write_scope()
            .run(async {
                self.pin
                    .protect(BTreeSet::from([
                        crate::metadata::PinResource::Object(key.clone()),
                        crate::metadata::PinResource::Blob(payload),
                    ]))
                    .await?;
                let mut reader = self
                    .repository
                    .payloads
                    .open_read(&payload)
                    .await?
                    .ok_or(RepositoryError::MissingPayload(payload))?;
                let exact_len =
                    tokio::io::AsyncSeekExt::seek(&mut reader, std::io::SeekFrom::End(0)).await?;
                tokio::io::AsyncSeekExt::seek(&mut reader, std::io::SeekFrom::Start(0)).await?;
                let mut reader = BlobPayloadReader::new(reader.as_mut(), exact_len);
                let verified = self
                    .repository
                    .formats
                    .verify(&key, &mut reader, &self.repository.limits)
                    .await?;
                if verified.record().payload() != payload {
                    return Err(RepositoryError::PayloadIdentityMismatch {
                        expected: payload,
                        actual: verified.record().payload(),
                    });
                }
                Ok(StagedObject {
                    verified,
                    repository: self.repository.staging_identity.clone(),
                    _hold: std::marker::PhantomData,
                })
            })
            .await
    }

    /// Import an input within this existing mutation lifetime.
    #[cfg(feature = "experimental")]
    pub async fn import<I: crate::import::Importer<Self>>(
        &self,
        input: I,
    ) -> Result<I::Report, I::Error> {
        self.write_scope()
            .run(async { input.import(self).await })
            .await
    }

    #[cfg(not(feature = "experimental"))]
    pub(crate) async fn import<I: crate::importers::BackendImporter<Self>>(
        &self,
        input: I,
    ) -> Result<I::Report, I::Error> {
        self.write_scope()
            .run(async { input.import_into(self).await })
            .await
    }

    /// Atomically publish staged records and root changes at the current
    /// revision. Expensive closure verification happens against the snapshot
    /// before the short optimistic commit.
    /// Stale revisions and pre-append maintenance refusals share a 32-attempt,
    /// 30-second retry window. Submitted storage work is allowed to settle;
    /// the window is not a timeout that cancels an in-flight commit.
    #[tracing::instrument(
        name = "repository.publish",
        skip_all,
        fields(objects = staged.len(), root_changes = root_changes.len())
    )]
    pub async fn publish(
        &self,
        staged: Vec<StagedObject<'_>>,
        root_changes: Vec<RootChange>,
    ) -> Result<CommitResult, RepositoryError> {
        match self
            .publish_inner(staged, Vec::new(), root_changes, None, BTreeSet::new())
            .await?
        {
            ConditionalPublishResult::Committed(result) => Ok(result),
            ConditionalPublishResult::RootMismatch { .. } => {
                unreachable!("an unconditional mutation has no root expectations")
            }
        }
    }

    /// Publish only against one exact repository revision, without retrying
    /// stale revisions. Pre-append maintenance refusals use bounded retries
    /// while preserving this exact expectation. Evidence cites the state it
    /// read: a concurrent writer returns `StaleRevision` so the caller can
    /// rebuild the derived result from a matching snapshot.
    #[tracing::instrument(
        name = "repository.publish",
        skip_all,
        fields(objects = staged.len(), root_changes = root_changes.len(), exact_revision = true)
    )]
    pub async fn publish_at_revision(
        &self,
        expected: crate::RepositoryRevision,
        staged: Vec<StagedObject<'_>>,
        root_changes: Vec<RootChange>,
    ) -> Result<CommitResult, RepositoryError> {
        match self
            .publish_inner(
                staged,
                Vec::new(),
                root_changes,
                Some(expected),
                BTreeSet::new(),
            )
            .await?
        {
            ConditionalPublishResult::Committed(result) => Ok(result),
            ConditionalPublishResult::RootMismatch { .. } => {
                unreachable!("an exact-revision mutation has no root expectations")
            }
        }
    }

    /// Atomically publish only while each named root still has its expected
    /// value.
    ///
    /// A stale repository revision caused by an unrelated mutation retries
    /// against a fresh snapshot. If an expected root differs, nothing is
    /// committed and [`ConditionalPublishResult::RootMismatch`] reports the
    /// first mismatch in canonical root-name order. Payloads already written
    /// while staging remain safe collectible residue.
    #[tracing::instrument(
        name = "repository.publish",
        skip_all,
        fields(
            objects = staged.len(),
            root_expectations = expectations.len(),
            root_changes = root_changes.len(),
            conditional = true
        )
    )]
    pub async fn publish_if_roots_match(
        &self,
        staged: Vec<StagedObject<'_>>,
        expectations: Vec<RootExpectation>,
        root_changes: Vec<RootChange>,
    ) -> Result<ConditionalPublishResult, RepositoryError> {
        self.publish_inner(staged, expectations, root_changes, None, BTreeSet::new())
            .await
    }

    /// Publish one batch whose complete filesystem closures were established
    /// while the importer constructed them bottom-up.
    ///
    /// This is deliberately crate-private and namespace-restricted. A generic
    /// [`StagedObject`] proves its own payload and links, but arbitrary formats
    /// may still have relational rules in `verify_links`; only the filesystem
    /// and NAR importers have the file sizes, child-directory sizes, and
    /// post-order construction evidence needed to prove those rules without
    /// rereading. Publishing records the closures as validated, so a reader
    /// taken afterwards checks them in constant time.
    pub(crate) async fn publish_filesystem_constructed(
        &self,
        staged: Vec<StagedObject<'_>>,
        root_changes: Vec<RootChange>,
    ) -> Result<CommitResult, RepositoryError> {
        self.publish_filesystem_constructed_with_metadata(staged, root_changes, None)
            .await
    }

    pub(crate) async fn publish_filesystem_constructed_with_metadata(
        &self,
        staged: Vec<StagedObject<'_>>,
        root_changes: Vec<RootChange>,
        policy: Option<crate::MetadataChange>,
    ) -> Result<CommitResult, RepositoryError> {
        let mut constructed = BTreeSet::new();
        for object in &staged {
            let key = object.record().key();
            if !matches!(
                key.namespace().as_str(),
                crate::BLOB_NAMESPACE | crate::DIRECTORY_NAMESPACE
            ) {
                return Err(RepositoryError::InvalidInput(format!(
                    "filesystem construction proof cannot cover {key}"
                )));
            }
            constructed.insert(key.clone());
        }
        let metadata = policy
            .map(|change| MetadataMutation::with_metadata(Vec::new(), vec![change]))
            .transpose()?;
        match self
            .publish_inner_with_metadata(
                staged,
                Vec::new(),
                root_changes,
                None,
                ClosurePublication::constructed(constructed),
                metadata,
            )
            .await?
        {
            ConditionalPublishResult::Committed(result) => Ok(result),
            ConditionalPublishResult::RootMismatch { .. } => {
                unreachable!("a filesystem import has no root expectations")
            }
        }
    }

    /// Publish a native Git view whose complete closure the importer just
    /// traversed, verified, and durably checkpointed in bounded batches.
    ///
    /// This is crate-private and namespace-restricted because an arbitrary
    /// staged object proves only itself, not its whole closure. The native Git
    /// importer supplies the stronger construction proof: it starts from the
    /// exact selected refs and follows every verified canonical link until the
    /// queue is empty before calling this method.
    #[cfg(feature = "git")]
    pub(crate) async fn publish_git_constructed(
        &self,
        view: StagedObject<'_>,
        name: RootName,
        target: ObjectKey,
    ) -> Result<CommitResult, RepositoryError> {
        if target.namespace().as_str() != crate::GIT_VIEW_NAMESPACE
            || view.record().key() != &target
            || !name.as_str().starts_with("git/")
        {
            return Err(RepositoryError::InvalidInput(
                "constructed Git publication requires one matching git.view.v1 object and git/* root"
                    .to_owned(),
            ));
        }
        match self
            .publish_inner(
                vec![view],
                Vec::new(),
                vec![RootChange::Set {
                    name,
                    target: target.clone(),
                }],
                None,
                BTreeSet::from([target]),
            )
            .await?
        {
            ConditionalPublishResult::Committed(result) => Ok(result),
            ConditionalPublishResult::RootMismatch { .. } => {
                unreachable!("a constructed Git publication has no root expectations")
            }
        }
    }

    /// Atomically publish verified content, application records, and roots.
    /// Opaque record bytes never imply object links or GC protection.
    pub async fn publish_with_metadata(
        &self,
        staged: Vec<StagedObject<'_>>,
        checks: Vec<crate::MetadataCheck>,
        changes: Vec<crate::MetadataChange>,
    ) -> Result<CommitResult, RepositoryError> {
        if !self.repository.state.supports_metadata_records() {
            return Err(MetadataError::UnsupportedMetadata.into());
        }
        let mut mutation = MetadataMutation::with_metadata(checks, changes)?;
        let roots = mutation.take_root_changes();
        match self
            .publish_inner_with_metadata(
                staged,
                Vec::new(),
                roots,
                None,
                ClosurePublication::default(),
                Some(mutation),
            )
            .await?
        {
            ConditionalPublishResult::Committed(result) => Ok(result),
            ConditionalPublishResult::RootMismatch { .. } => {
                unreachable!("checks are evaluated by the metadata transaction")
            }
        }
    }

    pub(super) async fn publish_inner(
        &self,
        staged: Vec<StagedObject<'_>>,
        expectations: Vec<RootExpectation>,
        root_changes: Vec<RootChange>,
        exact_revision: Option<crate::RepositoryRevision>,
        constructed_closures: BTreeSet<ObjectKey>,
    ) -> Result<ConditionalPublishResult, RepositoryError> {
        self.publish_inner_with_metadata(
            staged,
            expectations,
            root_changes,
            exact_revision,
            ClosurePublication::constructed(constructed_closures),
            None,
        )
        .await
    }

    #[tracing::instrument(
        name = "repository.publish.commit",
        skip_all,
        fields(
            objects = staged.len(),
            root_expectations = expectations.len(),
            root_changes = root_changes.len(),
            exact_revision = exact_revision.is_some(),
            constructed_closures = closures.constructed.len(),
            checked_closures = closures.requested.len()
        )
    )]
    pub(super) async fn publish_inner_with_metadata(
        &self,
        staged: Vec<StagedObject<'_>>,
        expectations: Vec<RootExpectation>,
        root_changes: Vec<RootChange>,
        exact_revision: Option<crate::RepositoryRevision>,
        closures: ClosurePublication,
        metadata: Option<MetadataMutation>,
    ) -> Result<ConditionalPublishResult, RepositoryError> {
        self.write_scope()
            .run(async {
                if staged.len() > self.repository.limits.max_batch_objects {
                    return Err(RepositoryError::LimitExceeded(format!(
                        "mutation has {} objects, limit is {}",
                        staged.len(),
                        self.repository.limits.max_batch_objects
                    )));
                }
                if closures.requested.len() > self.repository.limits.max_batch_objects {
                    return Err(RepositoryError::LimitExceeded(format!(
                        "mutation checks {} closures, limit is {}",
                        closures.requested.len(),
                        self.repository.limits.max_batch_objects
                    )));
                }
                if root_changes.len() > self.repository.limits.max_root_changes {
                    return Err(RepositoryError::LimitExceeded(format!(
                        "mutation has {} root changes, limit is {}",
                        root_changes.len(),
                        self.repository.limits.max_root_changes
                    )));
                }
                if expectations.len() > self.repository.limits.max_root_changes {
                    return Err(RepositoryError::LimitExceeded(format!(
                        "mutation has {} root expectations, limit is {}",
                        expectations.len(),
                        self.repository.limits.max_root_changes
                    )));
                }
                let mut expectations_by_name = BTreeMap::new();
                for expectation in expectations {
                    let name = expectation.name.clone();
                    if expectations_by_name
                        .insert(name.clone(), expectation)
                        .is_some()
                    {
                        return Err(RepositoryError::InvalidInput(format!(
                            "mutation has duplicate root expectation for {name}"
                        )));
                    }
                }
                let mut overlay = BTreeMap::new();
                for object in &staged {
                    let record = object.record();
                    if !Arc::ptr_eq(&object.repository, &self.repository.staging_identity) {
                        return Err(RepositoryError::ForeignStagedObject(record.key().clone()));
                    }
                    match overlay.get(record.key()) {
                        Some(existing) if existing == record => {}
                        Some(_) => {
                            return Err(RepositoryError::StagedConflict(record.key().clone()));
                        }
                        None => {
                            overlay.insert(record.key().clone(), record.clone());
                        }
                    }
                }
                let verified: Vec<_> = staged
                    .iter()
                    .map(|object| object.verified.clone())
                    .collect();
                let mut inputs = BTreeSet::new();
                for record in overlay.values() {
                    inputs.insert(crate::metadata::PinResource::Object(record.key().clone()));
                    inputs.extend(
                        record
                            .links()
                            .iter()
                            .cloned()
                            .map(crate::metadata::PinResource::Object),
                    );
                }
                for change in &root_changes {
                    if let RootChange::Set { target, .. } = change {
                        inputs.insert(crate::metadata::PinResource::Object(target.clone()));
                    }
                }
                inputs.extend(
                    closures
                        .requested
                        .iter()
                        .cloned()
                        .map(crate::metadata::PinResource::Object),
                );
                self.pin.protect(inputs).await?;
                #[cfg(test)]
                let phase = self.repository.time_publication_phase(0);
                let mut state_commit = self.repository.publication.lock().await;
                #[cfg(test)]
                drop(phase);

                // Closures are verified once, under the commit lock: walks
                // that run alongside other publishers' commits contend for the
                // same state backend and slow every writer. The pins above
                // retain every closure a walk reaches, so a retry against a
                // newer revision reuses the proofs instead of walking again.
                let mut newly_verified = None;
                let mut retry = publication::PublicationRetry::new();
                loop {
                    #[cfg(test)]
                    let phase = self.repository.time_publication_phase(1);
                    let (snapshot, _snapshot_pin) = self.pinned_snapshot().await?;
                    #[cfg(test)]
                    drop(phase);
                    #[cfg(test)]
                    let phase = self.repository.time_publication_phase(2);
                    if let Some(mismatch) = publication_preconditions(
                        snapshot.as_ref(),
                        exact_revision,
                        &expectations_by_name,
                    )
                    .await?
                    {
                        return Ok(mismatch);
                    }
                    let newly_verified = match &mut newly_verified {
                        Some(proofs) => proofs,
                        None => newly_verified.insert(
                            self.verify_publication(
                                snapshot.as_ref(),
                                &overlay,
                                &staged,
                                &root_changes,
                                &closures,
                            )
                            .await?,
                        ),
                    };

                    let mut mutation = metadata.clone().unwrap_or_default();
                    mutation.add_objects(verified.iter().cloned());
                    // Recorded in the same transaction that publishes them, so a
                    // shortcut can never outlive the objects it vouches for.
                    mutation.mark_validated_closures(newly_verified.iter().cloned());
                    for change in &root_changes {
                        match change {
                            RootChange::Set { name, target } => {
                                mutation.set_root(name.clone(), target.clone());
                            }
                            RootChange::Remove { name } => {
                                mutation.remove_root(name.clone());
                            }
                        }
                    }
                    // Release the metadata connection before committing, but move
                    // its catalog pin into the cancellation-safe publication task.
                    // The task keeps it until its storage work has settled.
                    let expected_revision = snapshot.revision();
                    drop(snapshot);
                    let protection = Arc::new((self.protection.clone(), _snapshot_pin));
                    #[cfg(test)]
                    drop(phase);
                    let (guard, committed) = if staged.is_empty() && metadata.is_some() {
                        self.repository
                            .publication
                            .commit_existing(
                                state_commit.take(),
                                protection,
                                expected_revision,
                                mutation,
                            )
                            .await
                    } else {
                        self.repository
                            .publication
                            .commit(
                                state_commit.take(),
                                protection,
                                expected_revision,
                                Some(mutation),
                                #[cfg(test)]
                                self.repository.publication_profile.clone(),
                            )
                            .await
                    };
                    state_commit = guard;
                    let committed = committed
                        .map(|commit| commit.expect("logical publication always commits metadata"));
                    match committed {
                        Ok(result) => {
                            tracing::info!(
                                revision = %result.revision,
                                objects_inserted = result.objects_inserted,
                                roots_changed = result.roots_changed,
                                "repository publication committed"
                            );
                            return Ok(ConditionalPublishResult::Committed(result));
                        }
                        Err(
                            error @ RepositoryError::Metadata(MetadataError::StaleRevision {
                                ..
                            }),
                        ) if exact_revision.is_some() => {
                            return Err(error);
                        }
                        Err(error @ RepositoryError::Metadata(
                            MetadataError::StaleRevision { .. } | MetadataError::MaintenanceFenced,
                        )) => {
                            if !retry.wait().await {
                                return Err(error);
                            }
                            tracing::debug!(%error, "retrying repository publication with a fresh snapshot");
                        }
                        Err(error) => return Err(error),
                    }
                }
            })
            .await
    }

    /// Prove every closure this publication records a witness for, and
    /// every closure it names, against one snapshot.
    async fn verify_publication(
        &self,
        snapshot: &dyn MetadataSnapshot,
        overlay: &BTreeMap<ObjectKey, ObjectRecord>,
        staged: &[StagedObject<'_>],
        root_changes: &[RootChange],
        closures: &ClosurePublication,
    ) -> Result<Vec<ObjectKey>, RepositoryError> {
        let constructed_closures = &closures.constructed;
        let formats = &self.repository.formats;
        let trust_construction = formats.is_builtin();
        let mut newly_verified = Vec::new();
        // Every walk is planned first and driven by one await: a debug build
        // reserves frame space per await point, and publication already runs
        // beneath deep import and command futures. `true` witnesses every
        // object the walk verifies; `false` only the target, planned below.
        let mut walks = Vec::new();
        if trust_construction {
            // A raw blob's record already proves its closure, so a witness
            // per file would only add metadata writes.
            newly_verified.extend(
                constructed_closures
                    .iter()
                    .filter(|key| {
                        !overlay
                            .get(*key)
                            .is_some_and(|record| formats.intrinsically_complete(record))
                    })
                    .cloned(),
            );
        } else {
            // Construction proves the built-in rules, not those a replacement
            // verifier adds under the same namespace. Staging order is
            // bottom-up, so parents stop at their already proven children.
            walks.extend(
                staged
                    .iter()
                    .map(|object| object.record().key())
                    .filter(|key| constructed_closures.contains(*key))
                    .map(|key| (key.clone(), true)),
            );
        }
        for change in root_changes {
            if let RootChange::Set { target, .. } = change {
                // Named targets keep a stored witness even where completeness
                // is derived: fast application root changes accept only a
                // persisted one.
                newly_verified.push(target.clone());
                if trust_construction && constructed_closures.contains(target) {
                    // The target record is part of this exact overlay, and the
                    // private caller established its complete closure while
                    // constructing the filesystem graph bottom-up.
                    debug_assert!(overlay.contains_key(target));
                    continue;
                }
                walks.push((target.clone(), true));
            }
        }
        // Requested targets always pass normal format and link checks, but
        // only the targets themselves gain witnesses: recording every object
        // a walk visits would accumulate the transitive inventory.
        let requested: Vec<_> = closures.requested.iter().cloned().collect();
        let records = overlay_records(snapshot, overlay, &requested).await?;
        for (target, record) in requested.into_iter().zip(records) {
            let Some(record) = record else {
                return Err(RepositoryError::RootNotPublishable {
                    root: target.clone(),
                    status: ClosureStatus::Missing {
                        from: None,
                        missing: target,
                    },
                });
            };
            // A present raw blob is complete; the pins keep its payload.
            if !formats.intrinsically_complete(&record) {
                newly_verified.push(target.clone());
                walks.push((target, false));
            }
        }
        // Every walk shares the objects earlier walks proved, so each object
        // is read at most once per publication.
        // Only walks that witness what they visit collect it: the proof set
        // spills, but this list stays in memory until the commit.
        let mut proven = SpillSet::new(self.repository.spill_area(), "publication");
        let mut visited = Vec::new();
        for (target, witness_visited) in walks {
            let status = verify_closure_with(
                self.repository.closure_verifier().with_proofs(&mut proven),
                snapshot,
                overlay,
                &target,
                None,
                ClosureAudit::Incremental,
                witness_visited.then_some(&mut visited),
            )
            .await?;
            #[cfg(test)]
            self.repository.record_witness_inventory(visited.len());
            match status {
                ClosureStatus::Complete { .. } => newly_verified.append(&mut visited),
                status => {
                    return Err(RepositoryError::RootNotPublishable {
                        root: target,
                        status,
                    });
                }
            }
        }
        Ok(newly_verified)
    }

    /// Publish staged records without naming them. They remain collectible and
    /// can complete a graph published in later bounded batches.
    pub async fn publish_unrooted(
        &self,
        staged: Vec<StagedObject<'_>>,
    ) -> Result<CommitResult, RepositoryError> {
        self.publish(staged, Vec::new()).await
    }

    /// Publish records and verify selected complete closures without naming
    /// them. Targets may refer to staged or previously published objects.
    /// Every target must pass normal format and link verification before any
    /// records or closure witnesses become visible.
    ///
    /// Only requested targets acquire closure witnesses, and a built-in raw
    /// blob needs none: its record already proves its closure. This does not
    /// accumulate the transitive object inventory. Target count and
    /// staged-object count are each bounded by `max_batch_objects`. Already
    /// verified closures may be trusted, so this is a completeness check
    /// rather than a fresh corruption audit. Witnesses do not retain objects:
    /// after the mutation and other holds end, they remain collectible.
    pub async fn publish_closures(
        &self,
        staged: Vec<StagedObject<'_>>,
        targets: BTreeSet<ObjectKey>,
    ) -> Result<CommitResult, RepositoryError> {
        match self
            .publish_inner_with_metadata(
                staged,
                Vec::new(),
                Vec::new(),
                None,
                ClosurePublication {
                    requested: targets,
                    ..Default::default()
                },
                // Existing targets need only the metadata commit path; no
                // payload flush is needed when nothing is staged.
                Some(MetadataMutation::new()),
            )
            .await?
        {
            ConditionalPublishResult::Committed(result) => Ok(result),
            ConditionalPublishResult::RootMismatch { .. } => {
                unreachable!("unnamed closure publication has no root expectations")
            }
        }
    }

    /// Atomically publish records and set one exact named root.
    pub async fn publish_rooted(
        &self,
        staged: Vec<StagedObject<'_>>,
        name: RootName,
        target: ObjectKey,
    ) -> Result<CommitResult, RepositoryError> {
        self.publish(staged, vec![RootChange::Set { name, target }])
            .await
    }

    /// Remove a named root only while it still resolves to the exact target
    /// recorded by the caller.
    ///
    /// A concurrent state change retries the comparison against a fresh
    /// snapshot. `None` means the name was absent or had been repointed.
    pub async fn remove_root_if_matches(
        &self,
        name: &RootName,
        expected: &ObjectKey,
    ) -> Result<Option<CommitResult>, RepositoryError> {
        loop {
            let (snapshot, _snapshot_pin) = self.pinned_snapshot().await?;
            if snapshot.root(name).await?.as_ref() != Some(expected) {
                return Ok(None);
            }
            let mut mutation = MetadataMutation::new();
            mutation.remove_root(name.clone());
            match self
                .repository
                .state
                .commit(&snapshot.revision(), mutation)
                .await
            {
                Ok(result) => return Ok(Some(result)),
                Err(MetadataError::StaleRevision { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
}

/// Refuse a publication whose exact revision or root expectations no longer
/// hold. A stale exact revision is final; a root mismatch is a result.
async fn publication_preconditions(
    snapshot: &dyn MetadataSnapshot,
    exact_revision: Option<crate::RepositoryRevision>,
    expectations: &BTreeMap<RootName, RootExpectation>,
) -> Result<Option<ConditionalPublishResult>, RepositoryError> {
    if let Some(expected) = exact_revision
        && snapshot.revision() != expected
    {
        return Err(MetadataError::StaleRevision {
            expected,
            actual: snapshot.revision(),
        }
        .into());
    }
    for expectation in expectations.values() {
        let actual = snapshot.root(&expectation.name).await?;
        if actual != expectation.target {
            return Ok(Some(ConditionalPublishResult::RootMismatch {
                name: expectation.name.clone(),
                expected: expectation.target.clone(),
                actual,
            }));
        }
    }
    Ok(None)
}
