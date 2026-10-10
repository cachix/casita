//! Native fencing transaction probe. This does not fence local journal files.
use bytes::Bytes;
use casita::experimental::*;
use casita::{MetadataChange, MetadataCheck, MetadataKey};
use serde::Serialize;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
#[derive(Debug, Serialize)]
pub struct Report {
    pub stale_rejections: usize,
    pub checked_after_reopen: bool,
    pub exact_payload: bool,
}
pub(crate) async fn advance(
    repository: &Repository<MemoryBlobStore, TursoMetadataStore>,
    fence: &MetadataKey,
    expected: Option<Bytes>,
    next: Bytes,
) -> Result<()> {
    repository
        .mutation_session()
        .await?
        .publish_with_metadata(
            vec![],
            vec![MetadataCheck::Record {
                key: fence.clone(),
                expected,
            }],
            vec![MetadataChange::Set {
                key: fence.clone(),
                value: next,
            }],
        )
        .await?;
    Ok(())
}
pub(crate) async fn attempt(
    repository: &Repository<MemoryBlobStore, TursoMetadataStore>,
    fence: &MetadataKey,
    marker: &MetadataKey,
    token: Bytes,
    payload: &[u8],
    omit_fence: bool,
) -> Result<(ObjectKey, bool)> {
    let session = repository.mutation_session().await?;
    let staged = session.stage_blob(payload).await?;
    let key = staged.record().key().clone();
    let mut checks = Vec::new();
    if !omit_fence {
        checks.push(MetadataCheck::Record {
            key: fence.clone(),
            expected: Some(token),
        });
    }
    checks.push(MetadataCheck::Record {
        key: marker.clone(),
        expected: None,
    });
    let outcome = session
        .publish_with_metadata(
            vec![staged],
            checks,
            vec![
                MetadataChange::SetRoot {
                    name: "live".try_into()?,
                    target: key.clone(),
                },
                MetadataChange::Set {
                    key: marker.clone(),
                    value: Bytes::copy_from_slice(payload),
                },
            ],
        )
        .await;
    match outcome {
        Ok(_) => Ok((key, true)),
        Err(RepositoryError::Metadata(MetadataError::CheckFailed { .. })) => Ok((key, false)),
        Err(error) => Err(error.into()),
    }
}
pub async fn run(omit_fence: bool) -> Result<Report> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("fencing.sqlite");
    let state = TursoMetadataStore::open(&path).await?;
    let blobs = MemoryBlobStore::new();
    let repository = Repository::new(blobs.clone(), state.clone());
    let fence = MetadataKey::new("casita.spike.fencing.v1".parse()?, "owner");
    let marker = MetadataKey::new("casita.spike.fencing.v1".parse()?, "operation");
    let token = |epoch| Bytes::from(format!("generation/{epoch}"));
    advance(&repository, &fence, None, token(1)).await?;
    advance(&repository, &fence, Some(token(1)), token(2)).await?;
    let before = state.snapshot().await?.revision();
    let (stale_key, accepted) = attempt(
        &repository,
        &fence,
        &marker,
        token(1),
        b"stale owner",
        omit_fence,
    )
    .await?;
    let snapshot = state.snapshot().await?;
    if accepted
        || snapshot.revision() != before
        || snapshot.object(&stale_key).await?.is_some()
        || snapshot.root(&"live".try_into()?).await?.is_some()
        || snapshot.get(std::slice::from_ref(&marker)).await?[0].is_some()
    {
        return Err("stale fencing generation changed publication".into());
    }
    drop(snapshot);
    let (winner, accepted) = attempt(
        &repository,
        &fence,
        &marker,
        token(2),
        b"current owner",
        false,
    )
    .await?;
    if !accepted {
        return Err("current fencing generation was rejected".into());
    }
    advance(&repository, &fence, Some(token(2)), token(3)).await?;
    let revision = state.snapshot().await?.revision();
    drop(repository);
    drop(state);
    flush_repository_leases().await?;
    let reopened = TursoMetadataStore::open(&path).await?;
    let repository = Repository::new(blobs, reopened.clone());
    for old in [1, 2] {
        let fresh_marker =
            MetadataKey::new("casita.spike.fencing.v1".parse()?, format!("stale/{old}"));
        let (loser, accepted) = attempt(
            &repository,
            &fence,
            &fresh_marker,
            token(old),
            b"reopened stale owner",
            false,
        )
        .await?;
        let snapshot = reopened.snapshot().await?;
        if accepted
            || snapshot.revision() != revision
            || snapshot.object(&loser).await?.is_some()
            || snapshot.get(std::slice::from_ref(&fresh_marker)).await?[0].is_some()
            || snapshot.root(&"live".try_into()?).await?.as_ref() != Some(&winner)
            || snapshot.get(std::slice::from_ref(&fence)).await?[0].as_ref() != Some(&token(3))
            || snapshot.get(std::slice::from_ref(&marker)).await?[0].as_ref()
                != Some(&Bytes::from_static(b"current owner"))
        {
            return Err("reopened stale owner escaped fencing transaction".into());
        }
    }
    let (_, mut reader) = repository
        .open_payload(&winner)
        .await?
        .ok_or("fenced payload missing")?;
    let mut bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut bytes).await?;
    if bytes != b"current owner" {
        return Err("fenced payload differs from winning request".into());
    }
    Ok(Report {
        stale_rejections: 3,
        checked_after_reopen: true,
        exact_payload: true,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn fencing_rejects_stale_publications_before_and_after_reopen() {
        run(false).await.unwrap();
    }
    #[tokio::test]
    async fn fencing_checker_rejects_missing_atomic_generation_check() {
        let error = run(true).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("stale fencing generation changed publication"),
            "{error}"
        );
    }
    #[cfg(unix)]
    #[test]
    fn replacing_lock_file_admits_two_owners_and_does_not_fence_journal_writes() {
        use crate::intent_journal::{self, FileStorage, Intent};
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("recovery.lock");
        let old = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .unwrap();
        old.try_lock().unwrap();
        std::fs::rename(&path, directory.path().join("retired.lock")).unwrap();
        let new = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .unwrap();
        new.try_lock().unwrap();
        let mut storage = FileStorage {
            directory: directory.path().into(),
        };
        intent_journal::persist(&mut storage, &Intent::new("current owner").unwrap(), |_| {
            Ok(())
        })
        .unwrap();
        // Old and new locks are still held, but they refer to different inodes.
        intent_journal::persist(&mut storage, &Intent::new("stale owner").unwrap(), |_| {
            Ok(())
        })
        .unwrap();
        let loaded: Intent<String> = intent_journal::load(&storage).unwrap();
        assert_eq!(loaded.request, "stale owner");
        assert_ne!(
            loaded.fingerprint,
            Intent::new("current owner").unwrap().fingerprint
        );
    }
}
