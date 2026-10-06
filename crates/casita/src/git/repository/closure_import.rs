//! Inventory-free Git closure ingestion. Stored records prove their own native
//! identities; only a validated closure, or a built-in blob's presence, proves
//! it is safe to stop descending.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use gix::objs::FindExt;
use gix::odb::HeaderExt;

use crate::ObjectKey;
use crate::blob::BlobStore;
use crate::git::{GitError, GitObjectFormat, GitObjectKind, git_key_parts};
use crate::importers::{
    CancellationCheck, GitClosureImport, GitClosureImportError, GitClosureImportReport,
};
use crate::metadata::MetadataStore;
use crate::repository::{MutationSession, PendingGitWitnesses, RepositoryError};
use crate::spill::{SpillSet, TraversalQueue};

const FRONTIER: usize = 256;
const PACK_CACHE_BYTES: usize = 16 * 1024 * 1024;

type Result<T> = std::result::Result<T, GitClosureImportError>;

fn source_error(error: impl std::fmt::Display) -> GitClosureImportError {
    GitClosureImportError::Source(error.to_string())
}

fn native_kind(kind: gix::objs::Kind) -> GitObjectKind {
    match kind {
        gix::objs::Kind::Blob => GitObjectKind::Blob,
        gix::objs::Kind::Tree => GitObjectKind::Tree,
        gix::objs::Kind::Commit => GitObjectKind::Commit,
        gix::objs::Kind::Tag => GitObjectKind::Tag,
    }
}

/// A source object whose stored type differs from its type-qualified key. A
/// selected root is the caller's mistake; a linked object is invalid source data.
fn kind_mismatch(
    roots: &[ObjectKey],
    key: &ObjectKey,
    expected: GitObjectKind,
    actual: GitObjectKind,
) -> GitClosureImportError {
    if roots.binary_search(key).is_ok() {
        GitClosureImportError::RootKind {
            root: key.clone(),
            actual,
        }
    } else {
        GitError::InvalidObject(format!(
            "Git object {key} is linked as a {} but stored as a {}",
            expected.as_str(),
            actual.as_str()
        ))
        .into()
    }
}

struct Source {
    objects: gix::odb::Handle,
    /// Canonically ordered selected roots, for classifying type mismatches.
    roots: Arc<[ObjectKey]>,
    cancellation: CancellationCheck,
    decoded_bytes: u64,
}

impl Source {
    fn open(
        path: PathBuf,
        format: GitObjectFormat,
        limit: u64,
        roots: Arc<[ObjectKey]>,
        cancellation: CancellationCheck,
    ) -> Result<Self> {
        let options = gix::odb::store::init::Options {
            object_hash: match format {
                GitObjectFormat::Sha1 => gix::hash::Kind::Sha1,
                GitObjectFormat::Sha256 => gix::hash::Kind::Sha256,
            },
            alloc_limit_bytes: usize::try_from(limit).ok(),
            ..Default::default()
        };
        let objects = gix::odb::at_opts(path, [], options)
            .map_err(source_error)?
            .with_pack_cache(|| {
                Box::new(gix::odb::pack::cache::lru::MemoryCappedHashmap::new(
                    PACK_CACHE_BYTES,
                ))
            });
        Ok(Self {
            objects,
            roots,
            cancellation,
            decoded_bytes: 0,
        })
    }

    fn decode(
        &mut self,
        pending: &mut VecDeque<ObjectKey>,
        count: usize,
        budget: u64,
        payload_limit: u64,
        metadata_limit: u64,
    ) -> Result<Vec<(ObjectKey, Vec<u8>)>> {
        let mut decoded = Vec::new();
        let mut bytes = 0u64;
        while decoded.len() < count {
            self.cancellation.check()?;
            let Some(key) = pending.front() else { break };
            let (_, kind, oid) = git_key_parts(key)?;
            let oid = gix::ObjectId::from_bytes_or_panic(oid);
            let header = self.objects.header(oid).map_err(source_error)?;
            // Reject a wrong type from the header alone: its size limit and
            // decoding cost belong to a different kind than the one selected.
            let actual = native_kind(header.kind());
            if actual != kind {
                return Err(kind_mismatch(&self.roots, key, kind, actual));
            }
            let size = header.size();
            let limit = if kind == GitObjectKind::Blob {
                payload_limit
            } else {
                payload_limit.min(metadata_limit)
            };
            if size > limit {
                return Err(RepositoryError::LimitExceeded(format!(
                    "Git object {key} has {size} bytes, limit is {limit}"
                ))
                .into());
            }
            if !decoded.is_empty() && (bytes > budget || size > budget.saturating_sub(bytes)) {
                break;
            }
            let mut body = Vec::new();
            let object = self.objects.find(&oid, &mut body).map_err(source_error)?;
            let body_kind = native_kind(object.kind);
            if body_kind != kind || body.len() as u64 != size {
                return Err(GitError::InvalidObject(format!(
                    "Git object {key} decoded as a {} of {} bytes, but its header declares a {} of {size} bytes",
                    body_kind.as_str(),
                    body.len(),
                    kind.as_str()
                ))
                .into());
            }
            bytes = bytes
                .checked_add(size)
                .ok_or_else(|| source_error("decoded byte count overflow"))?;
            decoded.push((pending.pop_front().expect("front exists"), body));
        }
        self.decoded_bytes = self.decoded_bytes.saturating_add(bytes);
        Ok(decoded)
    }
}

pub(crate) async fn import<PS: BlobStore, SS: MetadataStore>(
    session: &MutationSession<'_, PS, SS>,
    request: &GitClosureImport,
) -> Result<GitClosureImportReport> {
    request.cancellation.check()?;
    let repository = session.repository();
    let limits = repository.limits();
    if limits.max_batch_objects == 0 {
        return Err(RepositoryError::LimitExceeded(
            "Git closure import requires a nonzero publication batch limit".into(),
        )
        .into());
    }
    if request.roots.len() > limits.max_traversal_objects {
        return Err(RepositoryError::LimitExceeded(
            "Git root selection exceeds traversal limit".into(),
        )
        .into());
    }
    let mut format = None;
    for key in &request.roots {
        // A malformed root is the caller's selection, not invalid source data.
        let (current, _, _) = git_key_parts(key).map_err(|error| {
            GitClosureImportError::InvalidSelection(format!("root {key}: {error}"))
        })?;
        if format.is_some_and(|previous| previous != current) {
            return Err(GitClosureImportError::InvalidSelection(
                "mixed Git hash formats".into(),
            ));
        }
        format = Some(current);
    }
    let mut report = GitClosureImportReport {
        roots: request.roots.clone(),
        ..Default::default()
    };
    let Some(format) = format else {
        return Ok(report);
    };
    let roots: Arc<[ObjectKey]> = request.roots.as_slice().into();
    // Protect the existing graph before inspecting records. New objects are
    // protected by the mutation's staging pin; both protections overlap until
    // selected roots and all their now-complete links have been retained.
    let formats = repository.formats();
    let hold = repository.retention_hold().await?;
    let snapshot = hold.snapshot();
    let area = repository.spill_area();
    let mut visited = SpillSet::new(area.clone(), "git-import-visited");
    let mut pending = PendingGitWitnesses::new(area.clone());
    let mut queue = TraversalQueue::new(area);
    for root in &request.roots {
        queue
            .push((None, root.clone()))
            .await
            .map_err(RepositoryError::from)?;
    }
    let mut source: Option<Source> = None;
    let mut staged = Vec::new();
    let mut staged_links = 0usize;
    loop {
        request.cancellation.check()?;
        let mut keys = Vec::new();
        while keys.len() < FRONTIER {
            let Some((_, key)) = queue.pop().await.map_err(RepositoryError::from)? else {
                break;
            };
            keys.push(key);
        }
        if keys.is_empty() {
            break;
        }
        let fresh = visited
            .insert_batch(&keys)
            .await
            .map_err(RepositoryError::from)?;
        keys = keys
            .into_iter()
            .zip(fresh)
            .filter_map(|(key, fresh)| fresh.then_some(key))
            .collect();
        if visited.len() > limits.max_traversal_objects {
            return Err(RepositoryError::LimitExceeded(format!(
                "Git closure traversal exceeded {} objects",
                limits.max_traversal_objects
            ))
            .into());
        }
        // A present built-in Git blob is complete by derivation: it needs a
        // presence probe only, and no witness unless it was selected, since
        // fast application root changes accept only a stored one.
        let (leaves, keys): (Vec<_>, Vec<_>) = keys
            .into_iter()
            .partition(|key| formats.complete_when_present(key));
        let mut missing = VecDeque::new();
        let mut unresolved = Vec::new();
        if !leaves.is_empty() {
            let present = snapshot
                .object_payload_batch(&leaves)
                .await
                .map_err(RepositoryError::from)?;
            let mut selected = Vec::new();
            for (key, present) in leaves.into_iter().zip(present) {
                if roots.binary_search(&key).is_ok() {
                    selected.push(key.clone());
                }
                if present.is_some() {
                    report.reused_objects += 1;
                } else {
                    missing.push_back(key);
                }
            }
            if !selected.is_empty() {
                let witnessed = snapshot
                    .validated_closures(&selected)
                    .await
                    .map_err(RepositoryError::from)?;
                unresolved.extend(
                    selected
                        .into_iter()
                        .zip(witnessed)
                        .filter_map(|(key, witnessed)| (!witnessed).then_some(key)),
                );
            }
        }
        if !keys.is_empty() {
            // Complete boundaries need only a presence/witness probe, not their
            // potentially huge link arrays. Persistent backends answer this from
            // payload-summary columns without decoding the object record.
            let complete = snapshot
                .validated_payload_batch(&keys)
                .await
                .map_err(RepositoryError::from)?;
            let mut linked = Vec::new();
            for (key, complete) in keys.into_iter().zip(complete) {
                if complete.is_some() {
                    report.reused_objects += 1;
                } else {
                    linked.push(key);
                }
            }
            let records = snapshot
                .object_batch(&linked)
                .await
                .map_err(RepositoryError::from)?;
            for (key, record) in linked.iter().zip(records) {
                if let Some(record) = record {
                    report.reused_objects += 1;
                    for child in record.links() {
                        queue
                            .push((None, child.clone()))
                            .await
                            .map_err(RepositoryError::from)?;
                    }
                } else {
                    missing.push_back(key.clone());
                }
            }
            unresolved.extend(linked);
        }
        // Visited keys are already distinct. Each but a root was first reached
        // from an earlier frontier, which fixes when its witness publishes.
        pending
            .insert_frontier(&unresolved)
            .await
            .map_err(RepositoryError::from)?;
        while !missing.is_empty() {
            request.cancellation.check()?;
            let path = request.objects_dir.clone();
            let concurrency = request.concurrency.get().min(limits.max_batch_objects);
            let budget = request.max_buffered_bytes.get();
            let payload_limit = limits.max_payload_bytes;
            let metadata_limit = limits.max_metadata_bytes;
            let roots = roots.clone();
            let cancellation = request.cancellation.clone();
            let (next_source, next_missing, decoded) = tokio::task::spawn_blocking(move || {
                let mut source = match source {
                    Some(source)
                        if source.decoded_bytes < super::MAX_GIT_SOURCE_MAPPING_WINDOW_BYTES =>
                    {
                        source
                    }
                    _ => Source::open(path, format, payload_limit, roots, cancellation)?,
                };
                let decoded = source.decode(
                    &mut missing,
                    concurrency,
                    budget,
                    payload_limit,
                    metadata_limit,
                )?;
                Ok::<_, GitClosureImportError>((source, missing, decoded))
            })
            .await
            .map_err(source_error)??;
            request.cancellation.check()?;
            source = Some(next_source);
            missing = next_missing;
            report.imported_objects += decoded.len();
            for (_, body) in &decoded {
                report.source_bytes = report
                    .source_bytes
                    .checked_add(body.len() as u64)
                    .ok_or_else(|| source_error("imported byte count overflow"))?;
            }
            // Drain every active writer before publication: a payload flush can
            // wait on a writer, and must not prevent that writer being polled.
            let objects: Vec<_> = futures::stream::iter(decoded)
                .map(|(key, body)| async move { session.stage_object(key, &body).await })
                .buffer_unordered(concurrency)
                .try_collect()
                .await?;
            for object in objects {
                for child in object.record().links() {
                    queue
                        .push((None, child.clone()))
                        .await
                        .map_err(RepositoryError::from)?;
                }
                staged_links += object.record().links().len();
                staged.push(object);
                if staged.len() == limits.max_batch_objects
                    || staged_links >= super::MAX_GIT_IMPORT_BATCH_LINKS
                {
                    request.cancellation.check()?;
                    session
                        .publish_unrooted(std::mem::take(&mut staged))
                        .await?;
                    staged_links = 0;
                }
            }
        }
    }
    if !staged.is_empty() {
        request.cancellation.check()?;
        session.publish_unrooted(staged).await?;
    }
    // Only now does every discovered native record have all its canonical
    // dependencies. Built-in formats add no rules beyond construction, and a
    // custom registry audits its own before any mark becomes visible.
    // Mark in bounded batches without rereading payloads or retaining an O(N)
    // in-memory inventory. A failed discovery cannot publish any false marks.
    request.cancellation.check()?;
    let proof = session.prove_git_closures(&request.roots, pending).await?;
    request.cancellation.check()?;
    proof.publish().await?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::process::{Command, Stdio};

    /// A bare SHA-1 object directory holding these loose blobs, and their keys.
    fn loose_blobs(bodies: &[&[u8]]) -> (tempfile::TempDir, Vec<ObjectKey>) {
        let directory = tempfile::tempdir().unwrap();
        let initialized = Command::new("git")
            .args(["init", "--bare", "-q", "--object-format=sha1"])
            .arg(directory.path())
            .output()
            .unwrap();
        assert!(initialized.status.success());
        let mut keys = Vec::new();
        for body in bodies {
            let mut child = Command::new("git")
                .arg("-C")
                .arg(directory.path())
                .args(["hash-object", "-w", "--stdin"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            child.stdin.take().unwrap().write_all(body).unwrap();
            assert!(child.wait_with_output().unwrap().status.success());
            keys.push(
                crate::git::git_object_key_for_body(
                    GitObjectFormat::Sha1,
                    GitObjectKind::Blob,
                    body,
                )
                .unwrap(),
            );
        }
        (directory, keys)
    }

    fn open(directory: &tempfile::TempDir, limit: u64, roots: &[ObjectKey]) -> Source {
        Source::open(
            directory.path().join("objects"),
            GitObjectFormat::Sha1,
            limit,
            roots.into(),
            CancellationCheck::default(),
        )
        .unwrap()
    }

    #[test]
    fn an_oversized_serial_body_does_not_admit_an_empty_sibling() {
        let (directory, keys) = loose_blobs(&[b"oversized", b""]);
        let mut keys = VecDeque::from(keys);
        let mut source = open(&directory, 16, &[]);
        let first = source.decode(&mut keys, 2, 1, 16, 16).unwrap();
        assert_eq!(
            first.len(),
            1,
            "oversized bodies must occupy their own window"
        );
        assert_eq!(keys.len(), 1);
        let second = source.decode(&mut keys, 2, 1, 16, 16).unwrap();
        assert_eq!(second.len(), 1);
        assert!(second[0].1.is_empty());
        assert!(keys.is_empty());
    }

    #[test]
    fn a_wrong_type_is_rejected_from_its_header_before_size_limits_or_decoding() {
        let body = [b'x'; 64];
        let (directory, blobs) = loose_blobs(&[&body]);
        let (format, _, oid) = git_key_parts(&blobs[0]).unwrap();
        let tree = crate::git::git_object_key(format, GitObjectKind::Tree, oid.to_vec()).unwrap();
        // The blob exceeds the metadata limit a tree would be held to. Only the
        // type error describes the request; neither path decodes the body.
        for (roots, category) in [
            (
                vec![tree.clone()],
                crate::RepositoryErrorCategory::InvalidInput,
            ),
            (Vec::new(), crate::RepositoryErrorCategory::InvalidData),
        ] {
            let mut source = open(&directory, 1024, &roots);
            let mut pending = VecDeque::from([tree.clone()]);
            let error = source.decode(&mut pending, 1, 1024, 1024, 16).unwrap_err();
            assert_eq!(error.category(), category, "{error}");
            match error {
                GitClosureImportError::RootKind { root, actual } => {
                    assert_eq!(root, tree);
                    assert_eq!(actual, GitObjectKind::Blob);
                }
                GitClosureImportError::Git(GitError::InvalidObject(message)) => {
                    assert!(message.contains("linked as a tree but stored as a blob"));
                }
                other => panic!("unexpected error: {other}"),
            }
            assert_eq!(source.decoded_bytes, 0);
            assert_eq!(pending.len(), 1);
        }
    }
}
