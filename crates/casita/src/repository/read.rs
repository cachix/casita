//! Verified object reads and filesystem checkout.

use super::*;

pub(super) type OpenedObject = (ObjectRecord, Box<dyn BlobReader>);

/// Phase measurements for the permanent object-read benchmark; no production timers.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct ObjectReadProfile {
    /// Benchmark control: retain the previous durable object-read protocol.
    pub durable_readers: bool,
    pub admission_nanos: u64,
    pub resolve_nanos: u64,
    pub pause_after_pin: Option<(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
}

impl<PS, SS> Repository<PS, SS> {
    /// Open an application retained reader without rebuilding this repository's
    /// coordination or caches. Lookup and opening share one protected snapshot.
    #[cfg(feature = "experimental")]
    pub async fn retained_reader(&self) -> Result<crate::RetainedReader, RepositoryError>
    where
        PS: BlobGc + 'static,
        SS: MetadataStore + 'static,
    {
        let hold = self.clone().into_builtin().owned_read_hold().await?;
        Ok(crate::RetainedReader::new(hold))
    }
}

impl<PS, SS> Repository<PS, SS>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    /// Open one object's payload, retaining its dependency closure until the
    /// reader is dropped. Unrelated objects in the admitted snapshot are not
    /// retained. The reader owns its protection and may outlive this repository.
    pub async fn open_payload(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<(ObjectRecord, Box<dyn BlobReader>)>, RepositoryError> {
        let (snapshot, protection) = self
            .pinned_retained_snapshot(true, Some(&BTreeSet::from([key.clone()])))
            .await?;
        snapshot.open_payload(&self.payloads, key, protection).await
    }

    /// Open one immutable object without retaining unrelated logical objects
    /// or a historical payload catalog for the reader's lifetime. Objects with
    /// links retain their closure so collection preserves graph consistency.
    /// The requested closure and candidate catalog remain protected while
    /// resolving the physical read plan and handing it off to the read pin.
    #[tracing::instrument(name = "repository.open_object", level = "debug", skip_all)]
    pub(crate) async fn open_object(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<OpenedObject>, RepositoryError>
    where
        PS: 'static,
        SS: 'static,
    {
        self.open_object_inner(
            key,
            #[cfg(test)]
            &mut ObjectReadProfile::default(),
        )
        .await
    }

    pub(crate) async fn open_object_inner(
        &self,
        key: &ObjectKey,
        #[cfg(test)] profile: &mut ObjectReadProfile,
    ) -> Result<Option<OpenedObject>, RepositoryError>
    where
        PS: 'static,
        SS: 'static,
    {
        Ok(self
            .open_object_with(
                key,
                |store, record, pin, catalog| {
                    Box::pin(async move {
                        store
                            .open_read_scoped(&record.payload(), pin, catalog)
                            .await
                    })
                },
                #[cfg(test)]
                profile,
            )
            .await?
            .map(|(record, reader)| (record, Box::new(reader) as Box<dyn BlobReader>)))
    }

    pub(crate) async fn open_object_verified(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<(ObjectRecord, Box<dyn crate::blob::BlobStreamReader>)>, RepositoryError>
    where
        PS: 'static,
        SS: 'static,
    {
        Ok(self
            .open_object_with(
                key,
                |store, record, pin, catalog| {
                    Box::pin(async move {
                        store
                            .open_proof_scoped(
                                &record.payload(),
                                record.payload_size(),
                                pin,
                                catalog,
                            )
                            .await
                    })
                },
                #[cfg(test)]
                &mut ObjectReadProfile::default(),
            )
            .await?
            .map(|(record, reader)| {
                let protection = reader._protection.clone();
                let verified = crate::verified::stream::decode(
                    reader,
                    record.payload(),
                    record.payload_size(),
                );
                (
                    record,
                    Box::new(HeldReader {
                        reader: verified,
                        _protection: protection,
                    }) as Box<dyn crate::blob::BlobStreamReader>,
                )
            }))
    }

    pub(super) async fn open_object_with<R, F>(
        &self,
        key: &ObjectKey,
        open: F,
        #[cfg(test)] profile: &mut ObjectReadProfile,
    ) -> Result<Option<(ObjectRecord, HeldReader<R>)>, RepositoryError>
    where
        PS: 'static,
        SS: 'static,
        R: AsyncRead + Unpin + Send + 'static,
        F: for<'a> FnOnce(
            &'a PS,
            &'a ObjectRecord,
            crate::metadata::DataPinLease,
            Option<&'a [u8]>,
        )
            -> futures::future::BoxFuture<'a, Result<Option<R>, crate::error::Error>>,
    {
        #[cfg(test)]
        let started = std::time::Instant::now();
        // Scoped backends resolve the supplied catalog themselves. Avoid
        // synchronizing a shared backend or attaching this temporary pin to
        // unrelated writes merely to admit a read.
        #[cfg(not(test))]
        let process_readers = true;
        #[cfg(test)]
        let process_readers = !profile.durable_readers;
        let (snapshot, snapshot_pin) = pin_metadata_snapshot_kind(
            self.state.as_ref(),
            true,
            process_readers,
            Some(&BTreeSet::from([key.clone()])),
        )
        .await?;
        #[cfg(test)]
        if let Some((entered, resume)) = profile.pause_after_pin.take() {
            let _ = entered.send(());
            let _ = resume.await;
        }
        let Some(record) = snapshot.object(key).await? else {
            return Ok(None);
        };
        let candidate_pin = crate::metadata::DataPin {
            scope: crate::metadata::PinScope::Closures(BTreeSet::from([key.clone()])),
            catalog: None,
            resources: BTreeSet::from([crate::metadata::PinResource::Blob(record.payload())]),
        };
        let pin = if process_readers {
            crate::metadata::DataPinLease::acquire_reader(
                self.state.pin_store().await?,
                candidate_pin,
            )
            .await?
        } else {
            crate::metadata::DataPinLease::acquire(self.state.pin_store().await?, candidate_pin)
                .await?
        };
        #[cfg(test)]
        {
            profile.admission_nanos = started.elapsed().as_nanos() as u64;
        }
        #[cfg(test)]
        let resolving = std::time::Instant::now();
        let reader = open(
            &self.payloads,
            &record,
            pin.clone(),
            snapshot.payload_catalog(),
        )
        .await?
        .ok_or(RepositoryError::MissingPayload(record.payload()))?;
        #[cfg(test)]
        {
            profile.resolve_nanos = resolving.elapsed().as_nanos() as u64;
        }
        // Neither a lazy metadata snapshot nor its catalog escapes into this
        // reader. The candidate-catalog pin can be released only after the
        // read pin and all physical dependencies have been admitted.
        drop(snapshot);
        drop(snapshot_pin);
        Ok(Some((
            record,
            HeldReader {
                reader,
                _protection: Arc::new((self.clone(), pin)),
            },
        )))
    }

    /// Materialize a verified canonical-directory graph while a retention hold
    /// prevents its payloads from being collected. The materialized tree is not
    /// flushed to stable storage.
    #[tracing::instrument(name = "repository.checkout", skip_all)]
    pub async fn checkout(
        &self,
        root: &ObjectKey,
        target: impl AsRef<Path>,
    ) -> Result<(), RepositoryError> {
        if root.namespace().as_str() != crate::DIRECTORY_NAMESPACE {
            return Err(RepositoryError::InvalidInput(format!(
                "checkout requires `{}`, got `{}`",
                crate::DIRECTORY_NAMESPACE,
                root.namespace()
            )));
        }
        let digest = root.native_digest().ok_or_else(|| {
            RepositoryError::InvalidInput(format!("directory key {root} is not digest-width"))
        })?;
        let hold = self
            .retention_hold_for(&BTreeSet::from([root.clone()]))
            .await?;
        // Materializing asks whether the graph is complete, not whether the
        // bytes on disk have decayed since they were verified; `fsck` answers
        // the second question, and the checkout below reads and verifies every
        // payload it writes out anyway.
        let status = hold.verify_closure_incremental(root).await?;
        if !matches!(status, ClosureStatus::Complete { .. }) {
            return Err(RepositoryError::RootNotPublishable {
                root: root.clone(),
                status,
            });
        }
        let directories = RepositoryDirectoryView {
            payloads: &self.payloads,
            snapshot: hold.snapshot(),
        };
        crate::filesystem::checkout::checkout(
            &self.payloads,
            &directories,
            &DirectoryId::new(digest),
            target,
        )
        .await?;
        Ok(())
    }
}

pub(super) struct RepositoryDirectoryView<'a, PS> {
    pub(super) payloads: &'a PS,
    pub(super) snapshot: &'a dyn MetadataSnapshot,
}

#[async_trait]
impl<PS: BlobStore> DirectorySource for RepositoryDirectoryView<'_, PS> {
    async fn get(&self, digest: &DirectoryId) -> Result<Option<Directory>, crate::error::Error> {
        let key = ObjectKey::directory(*digest);
        let Some(record) = self
            .snapshot
            .object(&key)
            .await
            .map_err(|error| crate::error::Error::Backend(Box::new(error)))?
        else {
            return Ok(None);
        };
        let mut reader = self
            .payloads
            .open_read(&record.payload())
            .await?
            .ok_or_else(|| {
                crate::error::Error::Backend(Box::new(MetadataError::Corruption(format!(
                    "directory {key} has no physical payload {}",
                    record.payload()
                ))))
            })?;
        let mut encoded = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut encoded).await?;
        let directory = Directory::decode(&encoded)?;
        if directory.digest() != *digest || record.payload() != BlobId::new(Digest::hash(&encoded))
        {
            return Err(crate::error::Error::Backend(Box::new(
                MetadataError::Corruption(format!(
                    "directory payload for {key} does not match its record"
                )),
            )));
        }
        Ok(Some(directory))
    }
}
