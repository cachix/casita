//! Object-store writes and loose deletions coordinated through online pins.
//! Logical/chunk identities and dedup cache hits are pinned by their callers;
//! this adapter covers physical names, including catalog and multipart writes.

use async_trait::async_trait;
use futures::{StreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, UploadPart, path::Path,
};
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::invariant::{self, Violation};
use crate::metadata::{PinBindings, PinInventory, PinResource, PinToken, WritePins};

/// Physical deletions a collector issues under the ledger it claimed against.
///
/// `inventory` is the ledger at the revision a new claim was conditional on,
/// so it is also the state that claim landed in. Every deleted resource is
/// claimed by this collector, either by `claimed` or by a claim it already
/// `owned`; no pin protects a claimed or deleted resource; and no other
/// collector's claim covers one.
pub(crate) fn validate_deletion_claim(
    inventory: &PinInventory,
    owned: &BTreeSet<PinToken>,
    claimed: &BTreeSet<PinResource>,
    deleted: &BTreeSet<PinResource>,
) -> Result<(), Violation> {
    const POINT: &str = "gc.claim";
    let owned_claims: BTreeSet<&PinResource> = inventory
        .deletions
        .iter()
        .filter(|(token, _)| owned.contains(token))
        .flat_map(|(_, resources)| resources)
        .collect();
    if let Some(resource) = deleted
        .iter()
        .find(|resource| !claimed.contains(resource) && !owned_claims.contains(resource))
    {
        return Err(Violation::new(
            POINT,
            format!("{resource:?} is deleted without a claim this collector owns"),
        ));
    }
    if let Some(token) = inventory.pins.iter().find_map(|(token, pin)| {
        (!pin.resources.is_disjoint(claimed) || !pin.resources.is_disjoint(deleted))
            .then_some(token)
    }) {
        return Err(Violation::new(
            POINT,
            format!("pin {token} protects a claimed or deleted resource"),
        ));
    }
    if let Some(token) = inventory.deletions.iter().find_map(|(token, claim)| {
        (!owned.contains(token) && (!claim.is_disjoint(claimed) || !claim.is_disjoint(deleted)))
            .then_some(token)
    }) {
        return Err(Violation::new(
            POINT,
            format!("claim {token} of another collector covers a deleted resource"),
        ));
    }
    Ok(())
}

/// Releasing a deletion claim lets writers pin and rewrite its resources, so
/// it happens only after every DELETE of the batch settled (`unsettled` is
/// zero), and only for resources that batch `deleted`.
pub(crate) fn validate_deletion_finish(
    claimed: &BTreeSet<PinResource>,
    deleted: &BTreeSet<PinResource>,
    unsettled: usize,
) -> Result<(), Violation> {
    const POINT: &str = "gc.finish";
    invariant::ensure(POINT, unsettled == 0, || {
        format!("{unsettled} DELETE requests of the batch have not settled")
    })?;
    invariant::ensure(POINT, claimed.is_subset(deleted), || {
        "the claim covers resources the batch did not delete".into()
    })
}

/// Claim a bounded page of loose representations before invalidating caches
/// or issuing DELETE. Every group represents one logical blob or chunk; a pin
/// on any of its paths preserves the whole group for this pass.
pub(crate) async fn delete_pinned_groups(
    objects: Arc<dyn ObjectStore>,
    pins: Arc<dyn crate::metadata::PinStore>,
    owned_claims: BTreeSet<crate::metadata::PinToken>,
    groups: Vec<Vec<Path>>,
    invalidate: impl FnOnce(&[usize]) + Send + 'static,
) -> std::io::Result<usize> {
    let (send, receive) = tokio::sync::oneshot::channel();
    crate::metadata::spawn_lease_task(async move {
        let result = async {
            let mut selected: Vec<_> = (0..groups.len()).collect();
            let collector = pins
                .inventory()
                .await
                .map_err(std::io::Error::other)?
                .collector
                .ok_or_else(|| {
                    std::io::Error::other("loose deletion requires collector ownership")
                })?;
            let (token, inventory, claimed) = loop {
                let inventory = pins.inventory().await.map_err(std::io::Error::other)?;
                if inventory.collector.as_ref() != Some(&collector)
                    || !pins.allows_deletion(&inventory)
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "loose deletion collector is fenced",
                    ));
                }
                let protected: BTreeSet<_> = inventory
                    .pins
                    .values()
                    .flat_map(|pin| pin.resources.iter())
                    .collect();
                selected.retain(|at| {
                    groups[*at].iter().all(|path| {
                        !protected.contains(&PinResource::StorageObject(path.to_string()))
                    })
                });
                if selected.is_empty() {
                    return Ok(0);
                }
                let requested: BTreeSet<_> = selected
                    .iter()
                    .flat_map(|at| &groups[*at])
                    .map(|path| PinResource::StorageObject(path.to_string()))
                    .collect();
                let covered: BTreeSet<_> = inventory
                    .deletions
                    .iter()
                    .filter(|(token, _)| owned_claims.contains(token))
                    .flat_map(|(_, resources)| resources.iter().cloned())
                    .collect();
                let resources: BTreeSet<_> = requested.difference(&covered).cloned().collect();
                if inventory.deletions.iter().any(|(token, claim)| {
                    !owned_claims.contains(token) && !claim.is_disjoint(&resources)
                }) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "loose deletion overlaps an unfinished claim",
                    ));
                }
                if resources.is_empty() {
                    break (None, inventory, resources);
                }
                // Kept only to check the claim against the deletions it covers.
                let claimed = if invariant::ENABLED {
                    resources.clone()
                } else {
                    BTreeSet::new()
                };
                if let Some(token) = pins
                    .claim_deletions(inventory.revision, resources)
                    .await
                    .map_err(std::io::Error::other)?
                {
                    break (Some(token), inventory, claimed);
                }
            };
            let count = selected.len();
            let paths: Vec<_> = selected
                .iter()
                .flat_map(|at| groups[*at].iter().cloned())
                .collect();
            let deleted: BTreeSet<_> = if invariant::ENABLED {
                paths
                    .iter()
                    .map(|path| PinResource::StorageObject(path.to_string()))
                    .collect()
            } else {
                BTreeSet::new()
            };
            invariant::check(|| {
                validate_deletion_claim(&inventory, &owned_claims, &claimed, &deleted)
            })?;
            invalidate(&selected);
            let requested = paths.len();
            let mut settled = 0_usize;
            let locations =
                futures::stream::iter(paths.into_iter().map(Ok::<_, object_store::Error>)).boxed();
            let mut deletes = objects.delete_stream(locations);
            while let Some(result) = deletes.next().await {
                match result {
                    Ok(_) | Err(object_store::Error::NotFound { .. }) => settled += 1,
                    Err(error) => return Err(std::io::Error::other(error)),
                }
            }
            if let Some(token) = token {
                invariant::check(|| {
                    validate_deletion_finish(&claimed, &deleted, requested.saturating_sub(settled))
                })?;
                pins.finish_deletions(&token)
                    .await
                    .map_err(std::io::Error::other)?;
            }
            Ok(count)
        }
        .await;
        drop(send.send(result));
        Ok(())
    });
    receive.await.map_err(std::io::Error::other)?
}

pub(crate) struct PinnedObjectStore {
    inner: Arc<dyn ObjectStore>,
    pins: PinBindings,
    deletions: super::deletion_barrier::DeletionBarrier,
}

impl PinnedObjectStore {
    pub(crate) fn wrap(
        inner: Arc<dyn ObjectStore>,
        pins: PinBindings,
        deletions: super::deletion_barrier::DeletionBarrier,
    ) -> Arc<dyn ObjectStore> {
        Arc::new(Self {
            inner,
            pins,
            deletions,
        })
    }
}

fn resources(paths: &[&Path]) -> BTreeSet<PinResource> {
    paths
        .iter()
        .map(|path| PinResource::StorageObject(path.to_string()))
        .collect()
}

fn error(error: std::io::Error) -> object_store::Error {
    object_store::Error::Generic {
        store: "online-pins",
        source: Box::new(error),
    }
}

impl std::fmt::Display for PinnedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Pinned({})", self.inner)
    }
}
impl std::fmt::Debug for PinnedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

#[async_trait]
impl ObjectStore for PinnedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let protected = resources(&[location]);
        let location = location.clone();
        let inner = self.inner.clone();
        self.pins
            .capture()
            .run(
                protected,
                async move { inner.put_opts(&location, payload, options).await },
                error,
            )
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let protected = resources(&[location]);
        let location = location.clone();
        let inner = self.inner.clone();
        let pins = self.pins.capture();
        let upload = pins
            .clone()
            .run(
                protected,
                async move { inner.put_multipart_opts(&location, options).await },
                error,
            )
            .await?;
        Ok(Box::new(PinnedMultipart {
            inner: Some(upload),
            pins,
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    /// Every deletion waits for the barrier, so it cannot reach storage ahead
    /// of the metadata commit that allowed it.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = self.inner.clone();
        let deletions = self.deletions.clone();
        futures::stream::once(async move {
            match deletions.before_deletion().await {
                Ok(()) => inner.delete_stream(locations),
                Err(failure) => futures::stream::iter([Err(error(failure))]).boxed(),
            }
        })
        .flatten()
        .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
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
        let protected = resources(&[from, to]);
        let (from, to) = (from.clone(), to.clone());
        let inner = self.inner.clone();
        self.pins
            .capture()
            .run(
                protected,
                async move { inner.copy_opts(&from, &to, options).await },
                error,
            )
            .await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        let protected = resources(&[from, to]);
        let (from, to) = (from.clone(), to.clone());
        let inner = self.inner.clone();
        self.pins
            .capture()
            .run(
                protected,
                async move { inner.rename_opts(&from, &to, options).await },
                error,
            )
            .await
    }
}

struct PinnedMultipart {
    inner: Option<Box<dyn MultipartUpload>>,
    pins: WritePins,
}

impl std::fmt::Debug for PinnedMultipart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedMultipart").finish_non_exhaustive()
    }
}

impl PinnedMultipart {
    fn take(&mut self) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.take().ok_or_else(|| {
            error(std::io::Error::other(
                "multipart completion or abort is already in flight",
            ))
        })
    }
}

#[async_trait]
impl MultipartUpload for PinnedMultipart {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let Some(inner) = &mut self.inner else {
            return Box::pin(async {
                Err(error(std::io::Error::other(
                    "multipart completion or abort is already in flight",
                )))
            });
        };
        let upload = inner.put_part(data);
        let pins = self.pins.clone();
        Box::pin(async move {
            let _pins = pins;
            upload.await
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let mut upload = self.take()?;
        let (upload, result) = self
            .pins
            .clone()
            .run(
                BTreeSet::new(),
                async move {
                    let result = upload.complete().await;
                    Ok((upload, result))
                },
                error,
            )
            .await?;
        self.inner = Some(upload);
        result
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let mut upload = self.take()?;
        let (upload, result) = self
            .pins
            .clone()
            .run(
                BTreeSet::new(),
                async move {
                    let result = upload.abort().await;
                    Ok((upload, result))
                },
                error,
            )
            .await?;
        self.inner = Some(upload);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{
        DataPin, DataPinLease, MemoryPinStore, PinScope, PinStore, flush_repository_leases,
    };
    use object_store::ObjectStoreExt;

    fn token(ordinal: u8) -> PinToken {
        format!("{ordinal:064x}").parse().unwrap()
    }

    fn objects(names: &[&str]) -> BTreeSet<PinResource> {
        names
            .iter()
            .map(|name| PinResource::StorageObject((*name).into()))
            .collect()
    }

    #[test]
    fn a_collector_deletes_only_what_it_claimed_and_nobody_pins() {
        let owned_claim = token(1);
        let inventory = PinInventory {
            pins: std::collections::BTreeMap::from([(
                token(3),
                DataPin {
                    scope: PinScope::Staging,
                    catalog: None,
                    resources: objects(&["pinned"]),
                },
            )]),
            deletions: std::collections::BTreeMap::from([
                (owned_claim.clone(), objects(&["owned"])),
                (token(2), objects(&["foreign"])),
            ]),
            ..PinInventory::default()
        };
        let owned = BTreeSet::from([owned_claim]);
        let claimed = objects(&["fresh"]);
        assert_eq!(
            validate_deletion_claim(&inventory, &owned, &claimed, &objects(&["fresh", "owned"])),
            Ok(())
        );
        let unclaimed = objects(&["fresh", "unclaimed"]);
        assert!(validate_deletion_claim(&inventory, &owned, &claimed, &unclaimed).is_err());
        let pinned = objects(&["pinned"]);
        assert!(validate_deletion_claim(&inventory, &owned, &pinned, &pinned).is_err());
        let foreign = objects(&["foreign"]);
        assert!(validate_deletion_claim(&inventory, &owned, &foreign, &foreign).is_err());
    }

    #[test]
    fn a_deletion_claim_is_released_only_after_its_deletes_settle() {
        let deleted = objects(&["a", "b"]);
        assert_eq!(
            validate_deletion_finish(&objects(&["a"]), &deleted, 0),
            Ok(())
        );
        assert!(validate_deletion_finish(&objects(&["a"]), &deleted, 1).is_err());
        assert!(validate_deletion_finish(&objects(&["a", "c"]), &deleted, 0).is_err());
    }

    #[tokio::test]
    async fn multipart_upload_retains_its_pin_through_completion() {
        let ledger = Arc::new(MemoryPinStore::default());
        let pin = DataPinLease::acquire(
            ledger.clone(),
            DataPin {
                scope: PinScope::Staging,
                catalog: None,
                resources: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
        let bindings = PinBindings::default();
        bindings.attach(&pin);
        let store = PinnedObjectStore::wrap(
            Arc::new(object_store::memory::InMemory::new()),
            bindings,
            Default::default(),
        );
        let path = Path::from("pack");
        let mut upload = store.put_multipart(&path).await.unwrap();
        drop(pin);
        upload.put_part(b"chunk".to_vec().into()).await.unwrap();
        let inventory = ledger.inventory().await.unwrap();
        assert!(
            ledger
                .claim_deletions(inventory.revision, resources(&[&path]))
                .await
                .unwrap()
                .is_none()
        );
        upload.complete().await.unwrap();
        assert_eq!(
            store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"chunk"
        );
        drop(upload);
        flush_repository_leases().await.unwrap();
        assert!(ledger.inventory().await.unwrap().pins.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_deletion_that_cannot_flush_committed_state_never_reaches_storage() {
        let directory = tempfile::tempdir().unwrap();
        let unflushable = directory.path().join("missing/casita.sqlite");
        let barrier = crate::blob::deletion_barrier::DeletionBarrier::default();
        barrier.order_after(crate::blob::CommitDurability::for_database(&unflushable).unwrap());
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = PinnedObjectStore::wrap(inner.clone(), PinBindings::default(), barrier);
        let path = Path::from("payload");
        inner.put(&path, b"payload".to_vec().into()).await.unwrap();
        assert!(store.delete(&path).await.is_err());
        assert!(inner.head(&path).await.is_ok());
    }
}
