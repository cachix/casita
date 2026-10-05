//! Bounded graph verification and format relation views.

use super::*;

/// Semantic result of checking one object's complete forward graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClosureStatus {
    /// Every reachable record and payload exists and all format relations hold.
    Complete {
        /// Distinct objects traversed, including the root itself.
        ///
        /// The traversal spills past its memory budget, so the set itself is
        /// deliberately not returned: a complete closure can be larger than
        /// the process that verified it.
        objects: usize,
    },
    /// One deterministic reachable boundary is absent.
    Missing {
        /// Object containing the missing link, or `None` for the requested
        /// root itself.
        from: Option<ObjectKey>,
        /// First missing key in breadth-first canonical traversal.
        missing: ObjectKey,
    },
    /// An object, payload, or intrinsic direct-link relation is invalid.
    Invalid {
        /// Object at which validation failed.
        object: ObjectKey,
        /// Stable human-readable reason; callers classify the variant rather
        /// than parsing this text.
        reason: String,
    },
    /// A reachable namespace has no registered verifier.
    Unsupported {
        /// Object that selected the unavailable namespace.
        object: ObjectKey,
    },
}

impl<PS, SS> Repository<PS, SS> {
    /// What one closure verification needs from this repository.
    pub(super) fn closure_verifier(&self) -> ClosureVerifier<'_, PS> {
        ClosureVerifier {
            payloads: &self.payloads,
            formats: &self.formats,
            limits: &self.limits,
            area: self.spill_area(),
            proven: None,
        }
    }
}

impl<PS, SS> Repository<PS, SS>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    /// Verify a committed graph against one immutable state snapshot.
    #[tracing::instrument(name = "repository.verify_closure", skip_all)]
    pub async fn verify_closure(&self, root: &ObjectKey) -> Result<ClosureStatus, RepositoryError> {
        self.retention_hold_for(&BTreeSet::from([root.clone()]))
            .await?
            .verify_closure(root)
            .await
    }

    /// Check a committed graph is complete, trusting closures already verified.
    ///
    /// This is a fast precondition check rather than a fresh audit: closures
    /// already verified for the snapshot may be trusted, and a present
    /// built-in raw or Git blob is complete without reading its payload.
    pub async fn verify_closure_incremental(
        &self,
        root: &ObjectKey,
    ) -> Result<ClosureStatus, RepositoryError> {
        self.retention_hold_for(&BTreeSet::from([root.clone()]))
            .await?
            .verify_closure_incremental(root)
            .await
    }
}

/// Objects a closure traversal resolves per state call.
///
/// Wide enough that the per-call cost stops mattering, small enough that the
/// resolved records stay a bounded working set beside the spilling queue.
pub(super) const CLOSURE_FRONTIER: usize = 256;

/// What one closure verification needs from its repository.
pub(super) struct ClosureVerifier<'a, PS> {
    pub(super) payloads: &'a PS,
    pub(super) formats: &'a FormatRegistry,
    pub(super) limits: &'a FormatLimits,
    pub(super) area: SpillArea,
    /// Objects whose complete closures earlier walks proved against this same
    /// snapshot and overlay. An incremental walk stops at them and adds every
    /// object it verifies. A walk that does not complete may leave partial
    /// proofs behind, so callers must discard the set after any such status.
    pub(super) proven: Option<&'a mut SpillSet<ObjectKey>>,
}

impl<'a, PS> ClosureVerifier<'a, PS> {
    /// Share proofs with the other walks of one publication attempt.
    pub(super) fn with_proofs(mut self, proven: &'a mut SpillSet<ObjectKey>) -> Self {
        self.proven = Some(proven);
        self
    }
}

/// Verify one closure, optionally recording every verified key in `union`.
///
/// The visited set and work queue spill to local storage past their memory
/// budget, so the largest verifiable graph is bounded by the traversal limits
/// rather than by process memory. Visit order, and therefore the exact
/// [`ClosureStatus`] reported for a broken graph, is the same either way.
pub(super) async fn verify_closure_against<PS: BlobStore>(
    verifier: ClosureVerifier<'_, PS>,
    snapshot: &dyn MetadataSnapshot,
    overlay: &BTreeMap<ObjectKey, ObjectRecord>,
    root: &ObjectKey,
    union: Option<&mut SpillSet<ObjectKey>>,
) -> Result<ClosureStatus, RepositoryError> {
    verify_closure_with(
        verifier,
        snapshot,
        overlay,
        root,
        union,
        ClosureAudit::Exhaustive,
        None,
    )
    .await
}

/// How much of a closure a verification re-reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClosureAudit {
    /// Read and re-verify every reachable object.
    ///
    /// This is what `fsck` owes its caller: only reading the bytes back can
    /// find storage that decayed under a graph that is still well formed.
    Exhaustive,
    /// Stop at objects whose closure the repository already verified, or
    /// whose record alone proves it, as for a built-in raw or Git blob.
    ///
    /// Records are immutable and only collection removes them, so a verified
    /// closure stays verified and re-reading it proves nothing new. This is
    /// what a precondition check wants: mutation, checkout and transfer ask
    /// "is this graph complete", not "has storage decayed".
    Incremental,
}

/// Verify one closure, cutting the walk short where the audit allows.
///
/// `newly_verified` collects the keys this walk proved, which is what a caller
/// records so a later incremental walk can stop at them.
pub(super) async fn verify_closure_with<PS: BlobStore>(
    verifier: ClosureVerifier<'_, PS>,
    snapshot: &dyn MetadataSnapshot,
    overlay: &BTreeMap<ObjectKey, ObjectRecord>,
    root: &ObjectKey,
    mut union: Option<&mut SpillSet<ObjectKey>>,
    audit: ClosureAudit,
    mut newly_verified: Option<&mut Vec<ObjectKey>>,
) -> Result<ClosureStatus, RepositoryError> {
    let ClosureVerifier {
        payloads,
        formats,
        limits,
        area,
        mut proven,
    } = verifier;
    // An incremental walk stops at verified objects, so it cannot also produce
    // the complete union a transfer or archive plan needs.
    debug_assert!(
        !(matches!(audit, ClosureAudit::Incremental) && union.is_some()),
        "an incremental closure walk cannot collect a complete union"
    );
    // Shared proofs are shortcuts; an exhaustive audit must reread everything.
    debug_assert!(
        !(matches!(audit, ClosureAudit::Exhaustive) && proven.is_some()),
        "an exhaustive closure audit cannot trust earlier walks"
    );
    // Objects read in the current frontier, published to `proven` after it.
    let mut proved = Vec::new();
    let mut reachable = SpillSet::new(area.clone(), "closure");
    let mut queue = TraversalQueue::new(area.clone());
    queue.push((None, root.clone())).await?;
    loop {
        // Resolve a whole frontier per state call. Asking for one record at a
        // time cost a blocking-task handoff and a connection lock per object,
        // which dominated the walk; the queue is breadth-first, so draining a
        // prefix of it visits exactly the same objects in exactly the same
        // order as popping one at a time.
        let mut frontier = Vec::new();
        while frontier.len() < CLOSURE_FRONTIER {
            let Some(step) = queue.pop().await? else {
                break;
            };
            frontier.push(step);
        }
        if frontier.is_empty() {
            break;
        }
        let keys: Vec<_> = frontier.iter().map(|(_, key)| key.clone()).collect();
        // An already-verified object stands in for everything beneath it, so
        // the walk neither reads it nor descends into it.
        let mut settled = match audit {
            ClosureAudit::Exhaustive => vec![false; keys.len()],
            ClosureAudit::Incremental => snapshot.validated_closures(&keys).await?,
        };
        if let Some(proven) = proven.as_deref() {
            for (settled, proven) in settled.iter_mut().zip(proven.contains_batch(&keys).await?) {
                *settled |= proven;
            }
        }
        let found = overlay_records(snapshot, overlay, &keys).await?;

        for (((from, key), record), settled) in frontier.into_iter().zip(found).zip(settled) {
            if !reachable.insert(key.clone()).await? {
                continue;
            }
            if reachable.len() > limits.max_traversal_objects {
                return Err(RepositoryError::LimitExceeded(format!(
                    "closure traversal exceeded {} objects",
                    limits.max_traversal_objects
                )));
            }
            if let Some(union) = union.as_deref_mut() {
                union.insert(key.clone()).await?;
            }
            let Some(record) = record else {
                return Ok(ClosureStatus::Missing { from, missing: key });
            };
            // Only after the record is known to be present: collection prunes
            // these marks with the objects they vouch for, and a walk that
            // trusted a mark without looking would be unable to notice if one
            // ever outlived its object.
            // A built-in blob's record is its own completeness proof.
            if settled
                || (matches!(audit, ClosureAudit::Incremental)
                    && formats.intrinsically_complete(&record))
            {
                continue;
            }
            let Some(format) = formats.get(key.namespace()) else {
                return Ok(ClosureStatus::Unsupported { object: key });
            };
            let Some(mut payload) = payloads.open_read(&record.payload()).await? else {
                return Ok(ClosureStatus::Invalid {
                    object: key,
                    reason: format!("physical payload {} is absent", record.payload()),
                });
            };
            let mut payload = BlobPayloadReader::new(payload.as_mut(), record.payload_size());

            let targets: Vec<_> = record.links().to_vec();
            for (target, found) in targets
                .iter()
                .zip(overlay_records(snapshot, overlay, &targets).await?)
            {
                if found.is_none() {
                    return Ok(ClosureStatus::Missing {
                        from: Some(key.clone()),
                        missing: target.clone(),
                    });
                }
            }

            let direct_links: BTreeSet<_> = record.links().iter().cloned().collect();
            let view = RepositoryDirectLinkView {
                payloads,
                snapshot,
                overlay,
                cached: None,
                allowed: &direct_links,
            };
            if let Err(error) = format
                .verify_links(
                    VerificationContext::new(&key, &mut payload),
                    &record,
                    &view,
                    limits,
                )
                .await
            {
                return Ok(ClosureStatus::Invalid {
                    object: key,
                    reason: error.to_string(),
                });
            }
            for target in record.links() {
                queue
                    .push((Some(record.key().clone()), target.clone()))
                    .await?;
            }
            if proven.is_some() {
                proved.push(key.clone());
            }
            if let Some(newly_verified) = newly_verified.as_deref_mut() {
                newly_verified.push(key);
            }
        }
        // Once this walk completes, every object it read has a complete
        // closure: each link was queued and then verified, settled, or already
        // proven. Publishing per frontier keeps memory bounded; this walk never
        // revisits a key, and a failing walk's caller discards the set.
        if let Some(proven) = proven.as_deref_mut() {
            proven.insert_batch(&proved).await?;
            proved.clear();
        }
    }
    Ok(ClosureStatus::Complete {
        objects: reachable.len(),
    })
}

async fn overlay_record(
    snapshot: &dyn MetadataSnapshot,
    overlay: &BTreeMap<ObjectKey, ObjectRecord>,
    key: &ObjectKey,
) -> Result<Option<ObjectRecord>, MetadataError> {
    match overlay.get(key) {
        Some(record) => Ok(Some(record.clone())),
        None => snapshot.object(key).await,
    }
}

/// Resolve many keys at once, answered in input order.
///
/// Staged records still win over committed ones; only the keys the overlay
/// cannot answer reach the state backend, and they reach it in one call.
pub(super) async fn overlay_records(
    snapshot: &dyn MetadataSnapshot,
    overlay: &BTreeMap<ObjectKey, ObjectRecord>,
    keys: &[ObjectKey],
) -> Result<Vec<Option<ObjectRecord>>, MetadataError> {
    let mut wanted = Vec::new();
    for key in keys {
        if !overlay.contains_key(key) {
            wanted.push(key.clone());
        }
    }
    let mut committed = snapshot.object_batch(&wanted).await?.into_iter();
    let mut resolved = Vec::with_capacity(keys.len());
    for key in keys {
        resolved.push(match overlay.get(key) {
            Some(record) => Some(record.clone()),
            None => committed.next().flatten(),
        });
    }
    Ok(resolved)
}

pub(super) struct RepositoryDirectLinkView<'a, PS> {
    pub(super) payloads: &'a PS,
    pub(super) snapshot: &'a dyn MetadataSnapshot,
    pub(super) overlay: &'a BTreeMap<ObjectKey, ObjectRecord>,
    pub(super) cached: Option<&'a HashMap<ObjectKey, ObjectRecord>>,
    pub(super) allowed: &'a BTreeSet<ObjectKey>,
}

#[async_trait]
impl<PS: BlobStore> DirectLinkView for RepositoryDirectLinkView<'_, PS> {
    async fn record(&self, key: &ObjectKey) -> Result<Option<ObjectRecord>, FormatError> {
        if !self.allowed.contains(key) {
            return Ok(None);
        }
        if let Some(record) = self.overlay.get(key) {
            return Ok(Some(record.clone()));
        }
        if let Some(cached) = self.cached {
            return Ok(cached.get(key).cloned());
        }
        overlay_record(self.snapshot, self.overlay, key)
            .await
            .map_err(|error| FormatError::InvalidDirectLink {
                object: key.clone(),
                target: key.clone(),
                message: error.to_string(),
            })
    }

    async fn open_payload(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<Box<dyn PayloadReader>>, FormatError> {
        let Some(record) = self.record(key).await? else {
            return Ok(None);
        };
        let reader = self
            .payloads
            .open_read(&record.payload())
            .await
            .map_err(|error| FormatError::InvalidDirectLink {
                object: key.clone(),
                target: key.clone(),
                message: error.to_string(),
            })?;
        Ok(reader.map(|reader| {
            Box::new(BlobPayloadReader::new(reader, record.payload_size()))
                as Box<dyn PayloadReader>
        }))
    }
}

pub(super) struct BlobPayloadReader<R> {
    pub(super) inner: R,
    pub(super) exact_len: u64,
}

impl<R> BlobPayloadReader<R> {
    pub(super) fn new(inner: R, exact_len: u64) -> Self {
        Self { inner, exact_len }
    }
}

#[async_trait]
impl<R> PayloadReader for BlobPayloadReader<R>
where
    R: AsyncRead + Unpin + Send,
{
    fn exact_len(&self) -> Option<u64> {
        Some(self.exact_len)
    }

    async fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        tokio::io::AsyncReadExt::read(&mut self.inner, buffer).await
    }
}
