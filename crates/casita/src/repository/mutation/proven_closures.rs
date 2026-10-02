//! Git closure proofs established once, then witnessed in bounded batches.

use std::cmp::Reverse;
use std::collections::VecDeque;

use super::*;

/// Witnesses a native Git import owes: every object it reached from the
/// selected roots that had none, ranked by the traversal frontier that first
/// reached it.
///
/// An object is first reached through a link from an earlier frontier, and
/// that linking object owes a witness too. Publishing later frontiers first
/// therefore witnesses each object before the object it was reached through.
/// Whatever prefix of the publication completes, every object still owed a
/// witness keeps an unwitnessed path from a selected root, so repeating the
/// import reaches, proves and witnesses all of them.
pub(crate) struct PendingGitWitnesses {
    keys: SpillSet<Discovered>,
    frontiers: u64,
}

/// A key ordered latest frontier first.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Discovered {
    frontier: Reverse<u64>,
    key: ObjectKey,
}

impl crate::spill::SpillKey for Discovered {
    fn encode_spill(&self) -> Vec<u8> {
        // A fixed-width big-endian complement sorts like `Reverse`, and the
        // key's canonical encoding sorts like the key.
        let mut encoded = (u64::MAX - self.frontier.0).to_be_bytes().to_vec();
        encoded.extend(self.key.encode_spill());
        encoded
    }

    fn decode_spill(bytes: &[u8]) -> Result<Self, crate::error::Error> {
        let (frontier, key) = bytes
            .split_first_chunk::<8>()
            .ok_or_else(|| crate::error::Error::from("spilled witness rank is truncated"))?;
        Ok(Self {
            frontier: Reverse(u64::MAX - u64::from_be_bytes(*frontier)),
            key: ObjectKey::decode_spill(key)?,
        })
    }
}

impl PendingGitWitnesses {
    pub(crate) fn new(area: SpillArea) -> Self {
        Self {
            keys: SpillSet::new(area, "git-import-pending-witnesses"),
            frontiers: 0,
        }
    }

    /// Record the keys owing witnesses that one traversal frontier reached
    /// first. Call once per frontier, in traversal order.
    pub(crate) async fn insert_frontier(
        &mut self,
        keys: &[ObjectKey],
    ) -> Result<(), crate::error::Error> {
        let frontier = Reverse(self.frontiers);
        self.frontiers += 1;
        let keys: Vec<_> = keys
            .iter()
            .map(|key| Discovered {
                frontier,
                key: key.clone(),
            })
            .collect();
        self.keys.insert_batch(&keys).await?;
        Ok(())
    }
}

/// Owed Git witnesses whose closures are proven complete under this
/// repository's registry, in publication order.
///
/// A proof borrows its session, whose staging pin retains the selected roots'
/// closures: it cannot outlive that protection or be published by another
/// session. Beyond the set's spill budget, publication holds one batch in
/// memory.
pub(crate) struct ProvenGitClosures<'s, 'r, PS, SS> {
    session: &'s MutationSession<'r, PS, SS>,
    pending: FrozenSpillSet<Discovered>,
}

impl<'r, PS, SS> MutationSession<'r, PS, SS>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    /// Retain `roots` and prove the closures of every witness `pending` owes.
    ///
    /// The importer must already have verified and durably published every
    /// record reachable from `roots`. That construction is the whole proof for
    /// the built-in registry. A custom registry may add relational rules, so
    /// each closure is audited against one snapshot, every walk stopping at
    /// objects earlier walks proved: each object is audited at most once.
    /// Walks record proofs before they finish, so only a complete audit
    /// returns; a rejection discards them, before any witness is visible.
    pub(crate) async fn prove_git_closures<'s>(
        &'s self,
        roots: &[ObjectKey],
        pending: PendingGitWitnesses,
    ) -> Result<ProvenGitClosures<'s, 'r, PS, SS>, RepositoryError> {
        // Retained for the session, not a snapshot: proofs taken against this
        // one stay valid for every later witness commit.
        self.retain_objects(roots.iter().cloned()).await?;
        let pending = pending.keys.freeze().await?;
        if !self.repository.formats.is_builtin() {
            self.audit_git_closures(roots, &pending).await?;
        }
        Ok(ProvenGitClosures {
            session: self,
            pending,
        })
    }

    /// Prove every owed closure under a custom registry. An owed object is
    /// either proven here or already witnessed, so all may be witnessed.
    async fn audit_git_closures(
        &self,
        roots: &[ObjectKey],
        pending: &FrozenSpillSet<Discovered>,
    ) -> Result<(), RepositoryError> {
        let (snapshot, _snapshot_pin) = self.pinned_snapshot().await?;
        let overlay = BTreeMap::new();
        let mut proven = SpillSet::new(self.repository.spill_area(), "git-closure-proofs");
        // Root walks prove nearly every owed object, so the pass over them
        // mostly skips each page with one batched probe. It still proves any
        // object beneath a closure another publisher witnessed meanwhile.
        let mut targets: VecDeque<_> = roots.iter().cloned().collect();
        let mut after = None;
        loop {
            let Some(target) = targets.pop_front() else {
                let page = pending.page(after.take(), CLOSURE_FRONTIER).await?;
                let Some(last) = page.last() else {
                    return Ok(());
                };
                after = Some(last.clone());
                let keys: Vec<_> = page.into_iter().map(|owed| owed.key).collect();
                let done = proven.contains_batch(&keys).await?;
                targets.extend(
                    keys.into_iter()
                        .zip(done)
                        .filter_map(|(key, done)| (!done).then_some(key)),
                );
                continue;
            };
            let status = verify_closure_with(
                self.repository.closure_verifier().with_proofs(&mut proven),
                snapshot.as_ref(),
                &overlay,
                &target,
                None,
                ClosureAudit::Incremental,
                None,
            )
            .await?;
            if !matches!(status, ClosureStatus::Complete { .. }) {
                return Err(RepositoryError::RootNotPublishable {
                    root: target,
                    status,
                });
            }
        }
    }
}

impl<PS, SS> ProvenGitClosures<'_, '_, PS, SS>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    /// Commit every owed witness, at most `max_batch_objects` per publication,
    /// so neither memory nor any one commit grows with the imported graph. A
    /// revision race retries only the current batch.
    ///
    /// Each batch witnesses complete closures only, and in the order
    /// [`PendingGitWitnesses`] describes, so an interrupted publication leaves
    /// valid witnesses and a repeated import witnesses the remainder.
    pub(crate) async fn publish(self) -> Result<(), RepositoryError> {
        let Self { session, pending } = self;
        let limit = session.repository.limits.max_batch_objects;
        if limit == 0 && pending.len() > 0 {
            return Err(RepositoryError::LimitExceeded(
                "closure witnesses require a nonzero publication batch limit".into(),
            ));
        }
        let mut after = None;
        loop {
            let page = pending.page(after.take(), limit).await?;
            let Some(last) = page.last() else {
                return Ok(());
            };
            after = Some(last.clone());
            let mut batch = BTreeSet::new();
            for owed in page {
                // The construction proof covers native Git keys only.
                crate::git::git_key_parts(&owed.key)
                    .map_err(|error| RepositoryError::InvalidInput(error.to_string()))?;
                batch.insert(owed.key);
            }
            session
                .publish_inner_with_metadata(
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    None,
                    ClosurePublication {
                        proven: batch,
                        ..Default::default()
                    },
                    // Witnesses alone need only the metadata commit path.
                    Some(MetadataMutation::new()),
                )
                .await?;
        }
    }
}
