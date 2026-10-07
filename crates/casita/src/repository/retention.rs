//! Snapshot admission, durable pins, and retained reader lifetimes.

use super::*;

impl<PS, SS> Repository<PS, SS>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    #[tracing::instrument(name = "repository.snapshot", level = "debug", skip_all)]
    pub(crate) async fn synchronized_snapshot(
        &self,
    ) -> Result<Arc<dyn MetadataSnapshot>, RepositoryError> {
        let snapshot = self.state.snapshot().await?;
        self.publication
            .synchronize(&self.payloads, snapshot.as_ref())
            .await?;
        tracing::debug!(revision = %snapshot.revision(), "repository snapshot synchronized");
        Ok(snapshot)
    }

    pub(super) async fn retained_snapshot(
        &self,
    ) -> Result<Arc<dyn MetadataSnapshot>, RepositoryError> {
        let snapshot = self.synchronized_snapshot().await?;
        // A process can open its catalog before acquiring collector
        // ownership. Refresh its inventory after reading the logical
        // snapshot. Mutation sessions already refresh once on admission;
        // their metadata batches must not repeatedly decode the whole catalog.
        if self.profile.coordinates_processes() {
            self.publication.refresh_retained(&self.payloads).await?;
        }
        Ok(snapshot)
    }

    pub(super) async fn pinned_retained_snapshot(
        &self,
        wait: bool,
        roots: Option<&BTreeSet<ObjectKey>>,
    ) -> Result<(HeldSnapshot, Arc<DataProtection>), RepositoryError> {
        self.pinned_retained_snapshot_kind(wait, roots, false).await
    }

    pub(super) async fn pinned_retained_snapshot_kind(
        &self,
        wait: bool,
        roots: Option<&BTreeSet<ObjectKey>>,
        reader: bool,
    ) -> Result<(HeldSnapshot, Arc<DataProtection>), RepositoryError> {
        let (snapshot, pin) = if reader {
            pin_metadata_snapshot_kind(self.state.as_ref(), wait, true, roots).await?
        } else {
            pin_metadata_snapshot(self.state.as_ref(), wait, roots).await?
        };
        self.publication
            .synchronize(&self.payloads, snapshot.as_ref())
            .await?;
        if self.profile.coordinates_processes() {
            self.publication.refresh_retained(&self.payloads).await?;
        }
        let payload_batch = self
            .payloads
            .begin_pinned_batch(pin.clone())?
            .without_batching();
        let protection = Arc::new(DataProtection {
            _payload_batch: payload_batch,
            _pin: Some(pin),
        });
        Ok((HeldSnapshot(snapshot), protection))
    }

    /// Take a stable-read retention hold. Its snapshot remains logically
    /// immutable and its pin retains the snapshot’s data during collection.
    #[tracing::instrument(name = "repository.retention_hold", skip_all)]
    pub async fn retention_hold(&self) -> Result<RetentionHold<'_, PS, SS>, RepositoryError> {
        let (snapshot, protection) = self.pinned_retained_snapshot(true, None).await?;
        Ok(RetentionHold {
            repository: self,
            snapshot,
            protection,
        })
    }

    /// Protect selected immutable graphs and their snapshot catalog.
    pub(crate) async fn retention_hold_for(
        &self,
        roots: &BTreeSet<ObjectKey>,
    ) -> Result<RetentionHold<'_, PS, SS>, RepositoryError> {
        let (snapshot, protection) = self
            .pinned_retained_snapshot_kind(true, Some(roots), true)
            .await?;
        Ok(RetentionHold {
            repository: self,
            snapshot,
            protection,
        })
    }

    /// Take a stable-read hold that owns a clone of the repository facade.
    ///
    /// Long-running services use this form to bind one exact logical snapshot
    /// without a self-referential borrow. The facade shares its backend handles;
    /// the backend implementations do not need to implement `Clone`.
    pub async fn owned_retention_hold(
        &self,
    ) -> Result<OwnedRetentionHold<PS, SS>, RepositoryError> {
        self.owned_retention_hold_kind(false).await
    }

    pub(crate) async fn owned_read_hold(
        &self,
    ) -> Result<OwnedRetentionHold<PS, SS>, RepositoryError> {
        self.owned_retention_hold_kind(true).await
    }

    pub(super) async fn owned_retention_hold_kind(
        &self,
        reader: bool,
    ) -> Result<OwnedRetentionHold<PS, SS>, RepositoryError> {
        let repository = self.clone();
        let (snapshot, protection) = repository
            .pinned_retained_snapshot_kind(true, None, reader)
            .await?;
        Ok(OwnedRetentionHold {
            repository,
            snapshot,
            _protection: protection,
        })
    }
}

/// Pin and validate a logical snapshot before opening any of its external
/// metadata or payload catalog files, including during S3 bootstrap.
pub(super) async fn pin_metadata_snapshot(
    state: &impl MetadataStore,
    wait: bool,
    roots: Option<&BTreeSet<ObjectKey>>,
) -> Result<(Arc<dyn MetadataSnapshot>, crate::metadata::DataPinLease), RepositoryError> {
    pin_metadata_snapshot_kind(state, wait, false, roots).await
}

pub(super) async fn pin_metadata_snapshot_kind(
    state: &impl MetadataStore,
    wait: bool,
    reader: bool,
    roots: Option<&BTreeSet<ObjectKey>>,
) -> Result<(Arc<dyn MetadataSnapshot>, crate::metadata::DataPinLease), RepositoryError> {
    let pins = state.pin_store().await?;
    loop {
        let candidate = state.snapshot().await?;
        let revision = candidate.revision();
        let generation = candidate.generation()?;
        let catalog = candidate.payload_catalog().map(ToOwned::to_owned);
        let resources = candidate.retention_resources();
        drop(candidate);
        let candidate_pin = crate::metadata::DataPin {
            scope: match roots {
                Some(keys) => crate::metadata::PinScope::Closures(keys.clone()),
                None => crate::metadata::PinScope::Snapshot { generation },
            },
            catalog,
            resources: resources.clone(),
        };
        let pin = if reader {
            crate::metadata::DataPinLease::try_acquire_reader(pins.clone(), candidate_pin).await?
        } else {
            crate::metadata::DataPinLease::try_acquire(pins.clone(), candidate_pin).await?
        };
        let Some(pin) = pin else {
            if !wait {
                return Err(RepositoryError::Busy(
                    "snapshot admission conflicts with collection; retry or recover the interrupted collector".into(),
                ));
            }
            // GC may have retired this candidate while admission was pending.
            // Reload its head rather than waiting on an obsolete claimed path.
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            continue;
        };
        // Registration and the metadata commit use separate stores. Only
        // expose a candidate whose revision survived registration.
        let snapshot = state.snapshot().await?;
        if snapshot.revision() != revision || snapshot.retention_resources() != resources {
            continue;
        }
        return Ok((snapshot, pin));
    }
}

/// Read operations shared by both hold ownership forms. The surrounding hold
/// owns collection protection; this wrapper does not acquire or release leases.
pub(super) struct HeldSnapshot(pub(super) Arc<dyn MetadataSnapshot>);

impl HeldSnapshot {
    pub(super) fn as_ref(&self) -> &dyn MetadataSnapshot {
        self.0.as_ref()
    }

    pub(super) async fn object(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRecord>, RepositoryError> {
        Ok(self.0.object(key).await?)
    }

    pub(super) async fn object_batch(
        &self,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<ObjectRecord>>, RepositoryError> {
        Ok(self.0.object_batch(keys).await?)
    }

    pub(super) async fn open_payload<PS: BlobStore>(
        &self,
        payloads: &PS,
        key: &ObjectKey,
        protection: Arc<DataProtection>,
    ) -> Result<Option<(ObjectRecord, Box<dyn BlobReader>)>, RepositoryError> {
        let Some(record) = self.object(key).await? else {
            return Ok(None);
        };
        let reader = payloads
            .open_read(&record.payload())
            .await?
            .ok_or(RepositoryError::MissingPayload(record.payload()))?;
        Ok(Some((
            record,
            Box::new(HeldReader {
                reader,
                _protection: protection,
            }),
        )))
    }

    pub(super) async fn verify_closure<PS: BlobStore>(
        &self,
        verifier: ClosureVerifier<'_, PS>,
        root: &ObjectKey,
        audit: ClosureAudit,
    ) -> Result<ClosureStatus, RepositoryError> {
        verify_closure_with(
            verifier,
            self.as_ref(),
            &BTreeMap::new(),
            root,
            None,
            audit,
            None,
        )
        .await
    }
}

// A stream can outlive the facade hold that opened it. Carry its protection
// until the stream itself is dropped, including while a read or seek is pending.
pub(super) struct HeldReader<R> {
    pub(super) reader: R,
    pub(super) _protection: Arc<dyn Send + Sync>,
}

impl<R: AsyncRead + Unpin> AsyncRead for HeldReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}

impl<R: tokio::io::AsyncSeek + Unpin> tokio::io::AsyncSeek for HeldReader<R> {
    fn start_seek(
        mut self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        std::pin::Pin::new(&mut self.reader).start_seek(position)
    }

    fn poll_complete(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        std::pin::Pin::new(&mut self.reader).poll_complete(cx)
    }
}

#[async_trait::async_trait]
impl BlobReader for HeldReader<Box<dyn BlobReader>> {
    async fn park(&mut self) {
        self.reader.park().await;
    }
}

/// Stable logical snapshot protected from physical collection.
pub struct RetentionHold<'a, PS, SS> {
    pub(super) repository: &'a Repository<PS, SS>,
    pub(super) snapshot: HeldSnapshot,
    pub(super) protection: Arc<DataProtection>,
}

/// Owned stable logical snapshot protected from physical collection.
pub struct OwnedRetentionHold<PS, SS> {
    pub(super) repository: Repository<PS, SS>,
    pub(super) snapshot: HeldSnapshot,
    pub(super) _protection: Arc<DataProtection>,
}

impl<PS, SS> OwnedRetentionHold<PS, SS>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    /// Repository whose coordination and physical payloads this hold owns.
    pub fn repository(&self) -> &Repository<PS, SS> {
        &self.repository
    }

    /// Share collection protection without retaining the logical snapshot.
    #[cfg(feature = "native")]
    pub(crate) fn data_protection(&self) -> Arc<dyn Send + Sync> {
        self._protection.clone()
    }

    /// Immutable logical snapshot tied to this hold.
    pub fn snapshot(&self) -> &dyn MetadataSnapshot {
        self.snapshot.as_ref()
    }

    /// Look up one immutable record in this exact snapshot.
    pub async fn object(&self, key: &ObjectKey) -> Result<Option<ObjectRecord>, RepositoryError> {
        self.snapshot.object(key).await
    }

    /// Look up several immutable records in this exact snapshot, in input
    /// order, paying the backend's per-call cost once for the whole batch.
    pub(crate) async fn object_batch(
        &self,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<ObjectRecord>>, RepositoryError> {
        self.snapshot.object_batch(keys).await
    }

    /// Open one snapshot record's physical payload while collection is excluded.
    pub async fn open_payload(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<(ObjectRecord, Box<dyn BlobReader>)>, RepositoryError> {
        self.snapshot
            .open_payload(&self.repository.payloads, key, self._protection.clone())
            .await
    }

    /// Verify a closure against the owned exact snapshot.
    pub async fn verify_closure(&self, root: &ObjectKey) -> Result<ClosureStatus, RepositoryError> {
        self.snapshot
            .verify_closure(
                self.repository.closure_verifier(),
                root,
                ClosureAudit::Exhaustive,
            )
            .await
    }

    /// Check a closure is complete, trusting closures already verified.
    ///
    /// This is a fast precondition check rather than a fresh audit: closures
    /// already verified for the snapshot may be trusted, and a present
    /// built-in raw or Git blob is complete without reading its payload.
    pub async fn verify_closure_incremental(
        &self,
        root: &ObjectKey,
    ) -> Result<ClosureStatus, RepositoryError> {
        self.snapshot
            .verify_closure(
                self.repository.closure_verifier(),
                root,
                ClosureAudit::Incremental,
            )
            .await
    }
}

impl<PS, SS> RetentionHold<'_, PS, SS>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    pub(crate) async fn retain_only(
        &mut self,
        roots: BTreeSet<ObjectKey>,
    ) -> Result<(), RepositoryError> {
        // Keep the old protection until the new claim covers this exact view.
        let pin = crate::metadata::DataPinLease::acquire_reader(
            self.repository.state.pin_store().await?,
            crate::metadata::DataPin {
                scope: crate::metadata::PinScope::Closures(roots),
                catalog: self
                    .snapshot
                    .as_ref()
                    .payload_catalog()
                    .map(ToOwned::to_owned),
                resources: self.snapshot.as_ref().retention_resources(),
            },
        )
        .await?;
        let batch = self
            .repository
            .payloads
            .begin_pinned_batch(pin.clone())?
            .without_batching();
        self.protection = Arc::new(DataProtection {
            _payload_batch: batch,
            _pin: Some(pin),
        });
        Ok(())
    }

    /// Immutable logical snapshot tied to this hold.
    pub fn snapshot(&self) -> &dyn MetadataSnapshot {
        self.snapshot.as_ref()
    }

    /// Look up one immutable record in this exact snapshot.
    pub async fn object(&self, key: &ObjectKey) -> Result<Option<ObjectRecord>, RepositoryError> {
        self.snapshot.object(key).await
    }

    /// Open one snapshot record's physical payload while this hold prevents
    /// collection. The caller may run [`verify_closure`](Self::verify_closure)
    /// first when it needs namespace and direct-link verification as well as
    /// the payload store's content verification.
    pub async fn open_payload(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<(ObjectRecord, Box<dyn BlobReader>)>, RepositoryError> {
        self.snapshot
            .open_payload(&self.repository.payloads, key, self.protection.clone())
            .await
    }

    /// Verify a closure while its physical payloads cannot be collected.
    pub async fn verify_closure(&self, root: &ObjectKey) -> Result<ClosureStatus, RepositoryError> {
        self.snapshot
            .verify_closure(
                self.repository.closure_verifier(),
                root,
                ClosureAudit::Exhaustive,
            )
            .await
    }

    /// Check a closure is complete, trusting closures already verified.
    ///
    /// This is a fast precondition check rather than a fresh audit: closures
    /// already verified for the snapshot may be trusted, and a present
    /// built-in raw or Git blob is complete without reading its payload.
    pub(crate) async fn verify_closure_incremental(
        &self,
        root: &ObjectKey,
    ) -> Result<ClosureStatus, RepositoryError> {
        self.snapshot
            .verify_closure(
                self.repository.closure_verifier(),
                root,
                ClosureAudit::Incremental,
            )
            .await
    }

    /// Verify a closure and add every verified object to `union`.
    ///
    /// Planning a transfer or an archive needs the union of several closures,
    /// which is the same traversal a verification already performs. Collecting
    /// into a caller-owned spillable set avoids walking twice and keeps the
    /// union out of memory once it grows.
    pub(crate) async fn verify_closure_into(
        &self,
        root: &ObjectKey,
        union: &mut SpillSet<ObjectKey>,
    ) -> Result<ClosureStatus, RepositoryError> {
        verify_closure_against(
            self.repository.closure_verifier(),
            self.snapshot.as_ref(),
            &BTreeMap::new(),
            root,
            Some(union),
        )
        .await
    }

    /// Where this hold's repository spills traversal state.
    pub(crate) fn spill_area(&self) -> SpillArea {
        self.repository.spill_area()
    }
}

pub(super) struct DataProtection {
    pub(super) _payload_batch: crate::blob::BlobBatchGuard,
    pub(super) _pin: Option<crate::metadata::DataPinLease>,
}
