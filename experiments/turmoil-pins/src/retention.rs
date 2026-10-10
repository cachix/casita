//! Application retention policy probe using native, atomic Casita metadata.
//! Generation allocation and tombstone retirement remain application responsibilities.
use crate::fencing::{advance, attempt};
use bytes::Bytes;
use casita::experimental::*;
use casita::{MetadataChange, MetadataCheck, MetadataKey};
use serde::Serialize;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
#[derive(Debug, Serialize)]
pub struct Report {
    pub tombstone_survived_gc: bool,
    pub duplicates_rejected: usize,
    pub old_generation_expired: bool,
    pub reused_suffix_readable_after_reopen: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Recovery {
    Committed,
    Retired,
    Expired,
    Unknown,
}
async fn recover(
    state: &TursoMetadataStore,
    fence: &MetadataKey,
    marker: &MetadataKey,
    generation: Bytes,
    ignore_generation: bool,
) -> Result<Recovery> {
    // One snapshot binds the generation and marker observations.
    let values = state
        .snapshot()
        .await?
        .get(&[fence.clone(), marker.clone()])
        .await?;
    if !ignore_generation && values[0].as_ref() != Some(&generation) {
        return Ok(Recovery::Expired);
    }
    Ok(match values[1].as_deref() {
        Some(b"retired") => Recovery::Retired,
        Some(_) => Recovery::Committed,
        None => Recovery::Unknown,
    })
}
pub async fn run(omit_fence: bool, ignore_generation: bool) -> Result<Report> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("retention.sqlite");
    let state = TursoMetadataStore::open(&path).await?;
    let blobs = MemoryBlobStore::new();
    let repository = Repository::new(blobs.clone(), state.clone());
    let namespace = "casita.spike.retention.v1".parse()?;
    let fence = MetadataKey::new(namespace, "generation");
    let marker = MetadataKey::new("casita.spike.retention.v1".parse()?, "1/operation7");
    let token = |epoch| Bytes::from(format!("generation/{epoch}"));
    advance(&repository, &fence, None, token(1)).await?;
    let (original, accepted) =
        attempt(&repository, &fence, &marker, token(1), b"original", false).await?;
    if !accepted {
        return Err("initial operation rejected".into());
    }
    repository
        .mutation_session()
        .await?
        .publish_with_metadata(
            vec![],
            vec![
                MetadataCheck::Record {
                    key: fence.clone(),
                    expected: Some(token(1)),
                },
                MetadataCheck::Record {
                    key: marker.clone(),
                    expected: Some(Bytes::from_static(b"original")),
                },
            ],
            vec![
                MetadataChange::Set {
                    key: marker.clone(),
                    value: Bytes::from_static(b"retired"),
                },
                MetadataChange::RemoveRoot {
                    name: "live".try_into()?,
                },
            ],
        )
        .await?;
    flush_repository_leases().await?;
    repository.collect().await?;
    flush_repository_leases().await?;
    if state.snapshot().await?.object(&original).await?.is_some()
        || blobs
            .read_to_vec(&BlobId::new(Digest::hash(b"original")))
            .await?
            .is_some()
        || recover(&state, &fence, &marker, token(1), false).await? != Recovery::Retired
    {
        return Err("retired marker or object GC invariant failed".into());
    }
    for payload in [b"original".as_slice(), b"different".as_slice()] {
        let before = state.snapshot().await?.revision();
        let (key, accepted) =
            attempt(&repository, &fence, &marker, token(1), payload, false).await?;
        let snapshot = state.snapshot().await?;
        if accepted
            || snapshot.revision() != before
            || snapshot.object(&key).await?.is_some()
            || snapshot.root(&"live".try_into()?).await?.is_some()
        {
            return Err("tombstone permitted operation ID reuse".into());
        }
    }
    // Prune the tombstone only in the transaction that expires its generation.
    repository
        .mutation_session()
        .await?
        .publish_with_metadata(
            vec![],
            vec![
                MetadataCheck::Record {
                    key: fence.clone(),
                    expected: Some(token(1)),
                },
                MetadataCheck::Record {
                    key: marker.clone(),
                    expected: Some(Bytes::from_static(b"retired")),
                },
            ],
            vec![
                MetadataChange::Set {
                    key: fence.clone(),
                    value: token(2),
                },
                MetadataChange::Delete {
                    key: marker.clone(),
                },
            ],
        )
        .await?;
    if recover(&state, &fence, &marker, token(1), ignore_generation).await? != Recovery::Expired {
        return Err("pruned old operation reported Unknown instead of Expired".into());
    }
    let before = state.snapshot().await?.revision();
    let (key, accepted) = attempt(
        &repository,
        &fence,
        &marker,
        token(1),
        b"late retry",
        omit_fence,
    )
    .await?;
    let snapshot = state.snapshot().await?;
    if accepted
        || snapshot.revision() != before
        || snapshot.object(&key).await?.is_some()
        || snapshot.root(&"live".try_into()?).await?.is_some()
        || snapshot.get(std::slice::from_ref(&marker)).await?[0].is_some()
    {
        return Err("pruned marker allowed expired generation to republish".into());
    }
    drop(snapshot);
    let current_marker = MetadataKey::new("casita.spike.retention.v1".parse()?, "2/operation7");
    let (winner, accepted) = attempt(
        &repository,
        &fence,
        &current_marker,
        token(2),
        b"new generation",
        false,
    )
    .await?;
    if !accepted {
        return Err("new generation could not reuse short operation suffix".into());
    }
    drop(repository);
    drop(state);
    flush_repository_leases().await?;
    let reopened = TursoMetadataStore::open(&path).await?;
    if recover(&reopened, &fence, &marker, token(1), false).await? != Recovery::Expired
        || recover(&reopened, &fence, &current_marker, token(2), false).await?
            != Recovery::Committed
        || reopened
            .snapshot()
            .await?
            .root(&"live".try_into()?)
            .await?
            .as_ref()
            != Some(&winner)
        || reopened
            .snapshot()
            .await?
            .get(std::slice::from_ref(&current_marker))
            .await?[0]
            .as_deref()
            != Some(b"new generation")
    {
        return Err("retention policy changed after reopening".into());
    }
    let repository = Repository::new(blobs, reopened);
    let (_, mut reader) = repository
        .open_payload(&winner)
        .await?
        .ok_or("reused operation payload missing")?;
    let mut bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut bytes).await?;
    if bytes != b"new generation" {
        return Err("reused operation payload corrupted".into());
    }
    Ok(Report {
        tombstone_survived_gc: true,
        duplicates_rejected: 2,
        old_generation_expired: true,
        reused_suffix_readable_after_reopen: true,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn retention_preserves_tombstones_and_expires_old_operations() {
        run(false, false).await.unwrap();
    }
    #[tokio::test]
    async fn retention_rejects_retry_without_generation_check() {
        assert!(
            run(true, false)
                .await
                .unwrap_err()
                .to_string()
                .contains("expired generation to republish")
        );
    }
    #[tokio::test]
    async fn retention_rejects_unknown_for_pruned_old_operation() {
        assert!(
            run(false, true)
                .await
                .unwrap_err()
                .to_string()
                .contains("Unknown instead of Expired")
        );
    }
}
