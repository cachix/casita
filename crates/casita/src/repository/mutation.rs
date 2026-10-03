//! Staging, conditional publication, and mutation lifetimes.

use super::*;

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
        let pins = self.state.pin_store().await?;
        // Register the current catalog with the staging pin when possible.
        // Admission still rechecks the metadata revision below, so a catalog
        // change between the snapshot and registration is protected there.
        let initial = if self.publication.coordinates_payload_catalog() {
            let snapshot = self.state.snapshot().await?;
            let mut resources = snapshot.retention_resources();
            if let Some(catalog) = snapshot.payload_catalog() {
                resources.insert(crate::metadata::PinResource::Catalog(catalog.to_vec()));
            }
            if resources.is_empty() {
                None
            } else {
                crate::metadata::DataPinLease::try_acquire(
                    pins.clone(),
                    crate::metadata::DataPin {
                        scope: crate::metadata::PinScope::Staging,
                        catalog: None,
                        resources,
                    },
                )
                .await?
            }
        } else {
            None
        };
        let pin = match initial {
            Some(pin) => pin,
            None => {
                crate::metadata::DataPinLease::acquire(
                    pins,
                    crate::metadata::DataPin {
                        scope: crate::metadata::PinScope::Staging,
                        catalog: None,
                        resources: BTreeSet::new(),
                    },
                )
                .await?
            }
        };
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
                constructed,
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
                BTreeSet::new(),
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
            constructed_closures,
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
            constructed_closures = constructed_closures.len()
        )
    )]
    pub(super) async fn publish_inner_with_metadata(
        &self,
        staged: Vec<StagedObject<'_>>,
        expectations: Vec<RootExpectation>,
        root_changes: Vec<RootChange>,
        exact_revision: Option<crate::RepositoryRevision>,
        constructed_closures: BTreeSet<ObjectKey>,
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
                self.pin.protect(inputs).await?;
                #[cfg(test)]
                let phase = self.repository.time_publication_phase(0);
                let mut state_commit = self.repository.publication.lock().await;
                #[cfg(test)]
                drop(phase);

                let mut retry = publication::PublicationRetry::new();
                loop {
                    #[cfg(test)]
                    let phase = self.repository.time_publication_phase(1);
                    let (snapshot, _snapshot_pin) = self.pinned_snapshot().await?;
                    #[cfg(test)]
                    drop(phase);
                    #[cfg(test)]
                    let phase = self.repository.time_publication_phase(2);
                    if let Some(expected) = exact_revision
                        && snapshot.revision() != expected
                    {
                        return Err(MetadataError::StaleRevision {
                            expected,
                            actual: snapshot.revision(),
                        }
                        .into());
                    }
                    for expectation in expectations_by_name.values() {
                        let actual = snapshot.root(&expectation.name).await?;
                        if actual != expectation.target {
                            return Ok(ConditionalPublishResult::RootMismatch {
                                name: expectation.name.clone(),
                                expected: expectation.target.clone(),
                                actual,
                            });
                        }
                    }
                    let mut newly_verified: Vec<_> = constructed_closures.iter().cloned().collect();
                    for change in &root_changes {
                        if let RootChange::Set { target, .. } = change {
                            if constructed_closures.contains(target) {
                                // The target record is part of this exact overlay, and
                                // the private caller established its complete closure
                                // while constructing the filesystem graph bottom-up.
                                debug_assert!(overlay.contains_key(target));
                                continue;
                            }
                            let status = verify_closure_with(
                                self.repository.closure_verifier(),
                                snapshot.as_ref(),
                                &overlay,
                                target,
                                None,
                                ClosureAudit::Incremental,
                                Some(&mut newly_verified),
                            )
                            .await?;
                            if !matches!(status, ClosureStatus::Complete { .. }) {
                                return Err(RepositoryError::RootNotPublishable {
                                    root: target.clone(),
                                    status,
                                });
                            }
                        }
                    }

                    let mut mutation = metadata.clone().unwrap_or_default();
                    mutation.add_objects(verified.iter().cloned());
                    // Recorded in the same transaction that publishes them, so a
                    // shortcut can never outlive the objects it vouches for.
                    mutation.mark_validated_closures(newly_verified);
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

    /// Publish staged records without naming them. They remain collectible and
    /// can complete a graph published in later bounded batches.
    pub async fn publish_unrooted(
        &self,
        staged: Vec<StagedObject<'_>>,
    ) -> Result<CommitResult, RepositoryError> {
        self.publish(staged, Vec::new()).await
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
