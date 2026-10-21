//! Opt-in retention policy for local named roots.

use std::collections::BTreeSet;
use std::future::Future;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::TryStreamExt;

use crate::metadata::{MetadataChange, MetadataCheck, MetadataKey, MetadataMutation};
use crate::{NamespaceId, RootName};

use super::{
    BlobGc, BlobStore, ConditionalPublishResult, MetadataError, MetadataStore, ObjectKey,
    Repository, RepositoryError, RootChange, RootExpectation, mutation::ClosurePublication,
};

/// Whether disk-pressure collection may release a named root.
///
/// Every root is permanent unless explicitly marked evictable. Ordinary
/// collection preserves both kinds; only the local pressure policy releases
/// evictable roots before collecting their now-unreachable data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootRetention {
    /// Keep the root until a caller removes it.
    Permanent,
    /// Permit local disk-pressure collection to remove the root.
    Evictable,
}

pub(crate) const POLICY_NAMESPACE: &str = "casita.root-retention.v1";
pub(crate) const PRESSURE_RECOVERY_USED_PERCENT: u8 = 75;

pub(crate) fn policy_key(name: &RootName) -> MetadataKey {
    MetadataKey::new(
        NamespaceId::try_from(POLICY_NAMESPACE).expect("valid internal namespace"),
        name.as_str().as_bytes().to_vec(),
    )
}

pub(crate) fn policy_change(name: &RootName, retention: RootRetention) -> MetadataChange {
    let key = policy_key(name);
    match retention {
        RootRetention::Permanent => MetadataChange::Delete { key },
        RootRetention::Evictable => MetadataChange::Set {
            key,
            value: policy_value(),
        },
    }
}

pub(crate) fn policy_value() -> Bytes {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            elapsed.as_nanos().min(u128::from(u64::MAX)) as u64
        });
    let mut bytes = Vec::with_capacity(9);
    bytes.push(1);
    bytes.extend_from_slice(&nanos.to_be_bytes());
    Bytes::from(bytes)
}

pub(crate) fn last_use(value: &[u8]) -> Option<u64> {
    if value.len() != 9 || value[0] != 1 {
        return None;
    }
    Some(u64::from_be_bytes(value[1..].try_into().ok()?))
}

impl<PS, SS> Repository<PS, SS>
where
    SS: MetadataStore,
{
    /// Read a root's retention policy. Missing roots return `None`.
    pub async fn root_retention(
        &self,
        name: &RootName,
    ) -> Result<Option<RootRetention>, RepositoryError> {
        let snapshot = self.state.snapshot().await?;
        if snapshot.root(name).await?.is_none() {
            return Ok(None);
        }
        if !self.state.supports_root_retention() {
            return Ok(Some(RootRetention::Permanent));
        }
        let policy = snapshot.get(&[policy_key(name)]).await?;
        Ok(Some(if policy[0].as_deref().and_then(last_use).is_some() {
            RootRetention::Evictable
        } else {
            RootRetention::Permanent
        }))
    }

    /// Change the retention policy of an existing local root.
    ///
    /// The update checks the root's current target and retries a changed
    /// revision. A removed root is never recreated by this operation.
    pub async fn set_root_retention(
        &self,
        name: &RootName,
        retention: RootRetention,
    ) -> Result<(), RepositoryError> {
        if !self.state.supports_root_retention() {
            return Err(RepositoryError::Metadata(
                MetadataError::UnsupportedMetadata,
            ));
        }
        loop {
            let snapshot = self.state.snapshot().await?;
            let target = snapshot
                .root(name)
                .await?
                .ok_or_else(|| RepositoryError::Absent(format!("root {name}")))?;
            let key = policy_key(name);
            let change = match retention {
                RootRetention::Permanent => MetadataChange::Delete { key },
                RootRetention::Evictable => MetadataChange::Set {
                    key,
                    value: policy_value(),
                },
            };
            let mutation = MetadataMutation::with_metadata(
                vec![MetadataCheck::Root {
                    name: name.clone(),
                    expected: Some(target),
                }],
                vec![change],
            )?;
            match self.state.commit(&snapshot.revision(), mutation).await {
                Ok(_) => return Ok(()),
                Err(MetadataError::StaleRevision { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Record a successful use of an evictable root for pressure eviction order.
    pub async fn touch_root(
        &self,
        name: &RootName,
        target: &ObjectKey,
    ) -> Result<(), RepositoryError> {
        if !self.state.supports_root_retention() {
            return Ok(());
        }
        let key = policy_key(name);
        let current = self.state.get_records(std::slice::from_ref(&key)).await?;
        let Some(current) = current.into_iter().next().flatten() else {
            return Ok(());
        };
        if last_use(&current).is_none() {
            return Ok(());
        }
        let mutation = MetadataMutation::with_metadata(
            vec![
                MetadataCheck::Root {
                    name: name.clone(),
                    expected: Some(target.clone()),
                },
                MetadataCheck::Record {
                    key: key.clone(),
                    expected: Some(current),
                },
            ],
            vec![MetadataChange::Set {
                key,
                value: policy_value(),
            }],
        )?;
        match self.state.commit_checked(mutation).await {
            Ok(_) | Err(MetadataError::CheckFailed { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

impl<PS, SS> Repository<PS, SS>
where
    PS: BlobStore,
    SS: MetadataStore + 'static,
{
    /// Set a root only while its target matches `expected`, including absence.
    ///
    /// A mismatch publishes nothing and returns the observed value. Unrelated
    /// revision changes are retried under the normal publication budget. The
    /// target closure is verified before publication. An optional retention
    /// policy changes in the same commit and requires a local metadata backend;
    /// omitting it preserves the existing policy.
    ///
    /// This compares the current target, not its history: removing and
    /// recreating a name with the same target can satisfy the expectation.
    pub async fn set_root_if_matches(
        &self,
        name: RootName,
        target: ObjectKey,
        expected: Option<ObjectKey>,
        retention: Option<RootRetention>,
    ) -> Result<ConditionalPublishResult, RepositoryError> {
        if retention.is_some() && !self.state.supports_root_retention() {
            return Err(MetadataError::UnsupportedMetadata.into());
        }
        let metadata = retention
            .map(|retention| {
                MetadataMutation::with_metadata(Vec::new(), vec![policy_change(&name, retention)])
            })
            .transpose()?;
        self.mutation_session()
            .await?
            .publish_inner_with_metadata(
                Vec::new(),
                vec![RootExpectation {
                    name: name.clone(),
                    target: expected,
                }],
                vec![RootChange::Set { name, target }],
                None,
                ClosurePublication::default(),
                metadata,
            )
            .await
    }

    /// Set a root and its retention policy in one local publication.
    pub async fn set_root_with_retention(
        &self,
        name: RootName,
        target: ObjectKey,
        retention: RootRetention,
    ) -> Result<(), RepositoryError> {
        if !self.state.supports_root_retention() {
            return Err(RepositoryError::Metadata(
                MetadataError::UnsupportedMetadata,
            ));
        }
        let _hold = self
            .retention_hold_for(&BTreeSet::from([target.clone()]))
            .await?;
        let policy = policy_change(&name, retention);
        self.mutation_session()
            .await?
            .publish_with_metadata(
                Vec::new(),
                Vec::new(),
                vec![MetadataChange::SetRoot { name, target }, policy],
            )
            .await?;
        Ok(())
    }
}

impl<PS, SS> Repository<PS, SS>
where
    PS: BlobGc + 'static,
    SS: MetadataStore + 'static,
{
    /// Release least recently used evictable roots until disk pressure eases.
    /// The local profile calls this after collecting already-unreachable data.
    pub(crate) async fn evict_roots_under_pressure(
        &self,
        path: &Path,
    ) -> Result<usize, RepositoryError> {
        self.evict_roots_until(|| crate::collection::probe_disk_usage(path))
            .await
    }

    pub(crate) async fn evict_roots_until<F, Fut>(
        &self,
        mut probe: F,
    ) -> Result<usize, RepositoryError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<crate::DiskUsage, RepositoryError>>,
    {
        if !self.state.supports_root_retention() {
            return Ok(0);
        }
        let mut released = 0;
        loop {
            let usage = probe().await?;
            if !usage.used_percent_at_least(PRESSURE_RECOVERY_USED_PERCENT) {
                return Ok(released);
            }
            let Some(name) = self.evict_next_root().await? else {
                return Ok(released);
            };
            released += 1;
            tracing::info!(root = %name, "evicted root under disk pressure");
            self.try_vacuum().await?;
        }
    }

    /// Release one oldest evictable root with a revision check. Returns `None`
    /// when no eligible root remains. Kept separate from the disk probe so
    /// correctness tests can exercise selection without filling a filesystem.
    pub(crate) async fn evict_next_root(&self) -> Result<Option<RootName>, RepositoryError> {
        if !self.state.supports_root_retention() {
            return Ok(None);
        }
        loop {
            let snapshot = self.state.snapshot().await?;
            let roots = snapshot.roots().try_collect::<Vec<_>>().await?;
            let mut candidates = Vec::new();
            for batch in roots.chunks(1024) {
                let keys = batch
                    .iter()
                    .map(|root| policy_key(root.name()))
                    .collect::<Vec<_>>();
                let values = snapshot.get(&keys).await?;
                for (root, value) in batch.iter().zip(values) {
                    if let Some(value) = value
                        && let Some(used) = last_use(&value)
                    {
                        candidates.push((used, root.name().clone(), root.target().clone(), value));
                    }
                }
            }
            candidates.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
            let Some((_, name, target, value)) = candidates.into_iter().next() else {
                return Ok(None);
            };
            let key = policy_key(&name);
            let mutation = MetadataMutation::with_metadata(
                vec![
                    MetadataCheck::Root {
                        name: name.clone(),
                        expected: Some(target),
                    },
                    MetadataCheck::Record {
                        key: key.clone(),
                        expected: Some(value),
                    },
                ],
                vec![
                    MetadataChange::RemoveRoot { name: name.clone() },
                    MetadataChange::Delete { key },
                ],
            )?;
            match self.state.commit(&snapshot.revision(), mutation).await {
                Ok(_) => return Ok(Some(name)),
                Err(MetadataError::StaleRevision { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
}
