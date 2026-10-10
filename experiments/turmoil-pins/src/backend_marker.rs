//! Real on-disk transaction probe, deliberately outside the deterministic simulator.
use bytes::Bytes;
use casita::experimental::*;
use casita::{MetadataChange, MetadataCheck, MetadataKey};
use serde::Serialize;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy, Debug)]
pub enum Scenario {
    LostAcknowledgement,
    CompetingWriters,
}

impl Scenario {
    pub const ALL: [Self; 2] = [Self::LostAcknowledgement, Self::CompetingWriters];
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub scenario: String,
    pub successful_applications: usize,
    pub rejected_duplicates: usize,
    pub marker_recovered: bool,
    pub root_recovered: bool,
    pub winner_payload_readable: bool,
    pub loser_object_absent: bool,
    pub persisted_revision_unchanged: bool,
    pub duplicate_rejected_after_reopen: bool,
}

/// Drop all database handles and reopen the file, without relying on a result cache.
pub async fn run(scenario: Scenario) -> Result<Report> {
    run_with_fault(scenario, false).await
}

async fn run_with_fault(scenario: Scenario, omit_marker: bool) -> Result<Report> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("state.sqlite");
    let state = TursoMetadataStore::open(&path).await?;
    // Only metadata persistence is under test. Payload storage survives in this process.
    let blobs = MemoryBlobStore::new();
    let repository = Repository::new(blobs.clone(), state.clone());
    let marker = MetadataKey::new("casita.spike.operations.v1".parse()?, "operation/7");
    let root = RootName::try_from("live")?;
    let first_session = repository.mutation_session().await?;
    let second_session = repository.mutation_session().await?;
    let payloads: [&[u8]; 2] = [b"writer zero payload", b"writer one payload"];
    let first = first_session.stage_blob(payloads[0]).await?;
    let second = second_session.stage_blob(payloads[1]).await?;
    let keys = [first.record().key().clone(), second.record().key().clone()];
    let ids = [first.record().payload(), second.record().payload()];
    // Bind the operation ID to its request, so a different writer cannot claim its outcome.
    let values =
        [0, 1].map(|writer| Bytes::from(format!("operation=7;root=live;payload={}", ids[writer])));
    let changes = |writer: usize| {
        let mut changes = vec![MetadataChange::SetRoot {
            name: root.clone(),
            target: keys[writer].clone(),
        }];
        if !omit_marker {
            changes.push(MetadataChange::Set {
                key: marker.clone(),
                value: values[writer].clone(),
            });
        }
        changes
    };
    let checks = || {
        vec![MetadataCheck::Record {
            key: marker.clone(),
            expected: None,
        }]
    };
    let first_publish = first_session.publish_with_metadata(vec![first], checks(), changes(0));
    let second_publish = second_session.publish_with_metadata(vec![second], checks(), changes(1));
    let (first_result, second_result) = match scenario {
        Scenario::CompetingWriters => tokio::join!(first_publish, second_publish),
        Scenario::LostAcknowledgement => {
            let applied = first_publish.await;
            // The server applies the transaction but its reply is discarded. A retry with
            // the same operation ID must be rejected, even with a different requested root.
            let revision = state.snapshot().await?.revision();
            let duplicate = second_publish.await;
            if !omit_marker && state.snapshot().await?.revision() != revision {
                return Err("duplicate advanced the committed revision".into());
            }
            (applied, duplicate)
        }
    };
    let results = [first_result, second_result];
    let successful_applications = results.iter().filter(|result| result.is_ok()).count();
    let rejected_duplicates = results
        .iter()
        .filter(|result| {
            matches!(
                result,
                Err(RepositoryError::Metadata(MetadataError::CheckFailed {
                    index: 0
                }))
            )
        })
        .count();
    let winner = results
        .iter()
        .position(|result| result.is_ok())
        .ok_or("no writer committed")?;
    // Discard CommitResults, just as the client with the missing reply would do.
    drop(results);
    drop(first_session);
    drop(second_session);
    flush_repository_leases().await?;
    let persisted_revision = state.snapshot().await?.revision();
    drop(repository);
    drop(state);

    let reopened = TursoMetadataStore::open(&path).await?;
    let snapshot = reopened.snapshot().await?;
    let marker_recovered =
        snapshot.get(std::slice::from_ref(&marker)).await?[0].as_ref() == Some(&values[winner]);
    if !marker_recovered {
        return Err("published root lacks matching durable operation marker".into());
    }
    let root_recovered = snapshot.root(&root).await?.as_ref() == Some(&keys[winner]);
    let loser_object_absent = snapshot.object(&keys[1 - winner]).await?.is_none();
    let persisted_revision_unchanged = snapshot.revision() == persisted_revision;
    let winner_payload_readable = snapshot.object(&keys[winner]).await?.is_some()
        && blobs.read_to_vec(&ids[winner]).await?.as_deref() == Some(payloads[winner]);
    drop(snapshot);
    let reader = Repository::new(blobs, reopened.clone());
    let readable = reader.open_payload(&keys[winner]).await?.is_some();
    // A new caller with no daemon cache tries to reuse the durable operation ID.
    // Its different object, root change, and marker replacement must all roll back.
    let retry_session = reader.mutation_session().await?;
    let retry = retry_session
        .stage_blob(b"writer after reopen payload")
        .await?;
    let retry_key = retry.record().key().clone();
    let retry_result = retry_session
        .publish_with_metadata(
            vec![retry],
            checks(),
            vec![
                MetadataChange::SetRoot {
                    name: root.clone(),
                    target: retry_key.clone(),
                },
                MetadataChange::Set {
                    key: marker.clone(),
                    value: Bytes::from_static(b"conflicting request"),
                },
            ],
        )
        .await;
    drop(retry_session);
    let after_retry = reopened.snapshot().await?;
    let duplicate_rejected_after_reopen = matches!(
        retry_result,
        Err(RepositoryError::Metadata(MetadataError::CheckFailed {
            index: 0
        }))
    ) && after_retry.revision() == persisted_revision
        && after_retry.object(&retry_key).await?.is_none()
        && after_retry.root(&root).await?.as_ref() == Some(&keys[winner])
        && after_retry.get(std::slice::from_ref(&marker)).await?[0].as_ref()
            == Some(&values[winner]);
    drop(after_retry);
    drop(reader);
    flush_repository_leases().await?;
    if successful_applications != 1
        || rejected_duplicates != 1
        || !root_recovered
        || !loser_object_absent
        || !persisted_revision_unchanged
        || !winner_payload_readable
        || !readable
        || !duplicate_rejected_after_reopen
    {
        return Err("operation marker transaction invariant failed".into());
    }
    Ok(Report {
        scenario: format!("{scenario:?}"),
        successful_applications,
        rejected_duplicates,
        marker_recovered,
        root_recovered,
        winner_payload_readable,
        loser_object_absent,
        persisted_revision_unchanged,
        duplicate_rejected_after_reopen,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn real_backend_recovers_unacknowledged_publication() {
        run(Scenario::LostAcknowledgement).await.unwrap();
    }

    #[tokio::test]
    async fn real_backend_admits_one_writer_per_operation() {
        run(Scenario::CompetingWriters).await.unwrap();
    }

    #[tokio::test]
    async fn checker_rejects_publication_without_atomic_marker() {
        let error = run_with_fault(Scenario::LostAcknowledgement, true)
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "published root lacks matching durable operation marker"
        );
    }
}
