//! Process death after publication, followed by an independent filesystem audit.
use bytes::Bytes;
use casita::experimental::*;
use casita::{MetadataChange, MetadataCheck, MetadataKey};
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const EXIT_AFTER_COMMIT: i32 = 73;

#[derive(Clone, Copy, Debug)]
pub enum Scenario {
    SingleWriter,
    CompetingWriters,
}
impl Scenario {
    pub const ALL: [Self; 2] = [Self::SingleWriter, Self::CompetingWriters];
    pub fn name(self) -> &'static str {
        match self {
            Self::SingleWriter => "single",
            Self::CompetingWriters => "writers",
        }
    }
    pub fn parse(name: &str) -> Result<Self> {
        match name {
            "single" => Ok(Self::SingleWriter),
            "writers" => Ok(Self::CompetingWriters),
            _ => Err("unknown crash scenario".into()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Report {
    pub scenario: String,
    pub writer_exit_without_cleanup: bool,
    pub independent_reader: bool,
    pub recovered_writer: usize,
    pub exact_packed_payload_recovered: bool,
    pub duplicate_rejected: bool,
    pub conflict_left_revision_unchanged: bool,
    pub loser_object_absent: bool,
    pub integrity_healthy: bool,
    pub collectible_residue: usize,
}

/// The only durable outcome evidence is the repository itself, not a saved reply.
pub fn run(executable: &Path, scenario: Scenario, omit_marker: bool) -> Result<Report> {
    let work = tempfile::tempdir()?;
    let writer = child(
        executable,
        if omit_marker {
            "write-no-marker"
        } else {
            "write"
        },
        work.path(),
        scenario,
    )?;
    if writer.0.code() != Some(EXIT_AFTER_COMMIT) || !writer.1.is_empty() {
        return Err(format!(
            "writer failed before crash boundary: {:?}: {}",
            writer.0, writer.2
        )
        .into());
    }
    let reader = child(executable, "read", work.path(), scenario)?;
    if !reader.0.success() {
        return Err(format!("independent recovery failed: {}", reader.2.trim()).into());
    }
    let mut report: Report = serde_json::from_str(&reader.1)?;
    report.writer_exit_without_cleanup = true;
    report.independent_reader = true;
    Ok(report)
}

fn child(
    executable: &Path,
    mode: &str,
    work: &Path,
    scenario: Scenario,
) -> Result<(std::process::ExitStatus, String, String)> {
    // Files avoid pipe capacity deadlocks and keep failure diagnostics available.
    let stdout = tempfile::NamedTempFile::new()?;
    let stderr = tempfile::NamedTempFile::new()?;
    let mut child = Command::new(executable)
        .args(["backend-crash-worker", mode])
        .arg(work)
        .arg(scenario.name())
        .stdin(Stdio::null())
        .stdout(stdout.reopen()?)
        .stderr(stderr.reopen()?)
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            return Err(format!("backend crash {mode} worker timed out").into());
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    Ok((
        status,
        std::fs::read_to_string(stdout.path())?,
        std::fs::read_to_string(stderr.path())?,
    ))
}

fn marker() -> MetadataKey {
    MetadataKey::new(
        "casita.spike.operations.v1".parse().unwrap(),
        "operation/crash-7",
    )
}
fn payload(writer: usize) -> Vec<u8> {
    let mut bytes = vec![0; 768 * 1024 + 9];
    blake3::Hasher::new()
        .update(b"shared crash fixture prefix")
        .finalize_xof()
        .fill(&mut bytes[..256 * 1024]);
    blake3::Hasher::new()
        .update(format!("crash fixture writer {writer}").as_bytes())
        .finalize_xof()
        .fill(&mut bytes[256 * 1024..]);
    bytes
}
fn key(bytes: &[u8]) -> ObjectKey {
    ObjectKey::blob(BlobId::new(Digest::hash(bytes)))
}
fn value(writer: usize) -> Bytes {
    Bytes::from(format!(
        "operation=crash-7;root=live;payload={}",
        key(&payload(writer))
    ))
}
fn checks() -> Vec<MetadataCheck> {
    vec![MetadataCheck::Record {
        key: marker(),
        expected: None,
    }]
}
fn changes(writer: usize, target: ObjectKey, omit_marker: bool) -> Vec<MetadataChange> {
    let mut changes = vec![MetadataChange::SetRoot {
        name: "live".try_into().unwrap(),
        target,
    }];
    if !omit_marker {
        changes.push(MetadataChange::Set {
            key: marker(),
            value: value(writer),
        });
    }
    changes
}

/// Invoked only by the isolated supervisor through a private CLI command.
pub async fn worker(mode: &str, work: &Path, scenario: Scenario) -> Result<()> {
    if mode == "read" {
        println!(
            "{}",
            serde_json::to_string(&recover(work, scenario).await?)?
        );
        return Ok(());
    }
    if !matches!(mode, "write" | "write-no-marker") {
        return Err("unknown crash worker mode".into());
    }
    let repository = Repository::local(work.join("repository")).await?;
    let first_session = repository.mutation_session().await?;
    let first = first_session.stage_blob(&payload(0)).await?;
    let first_key = first.record().key().clone();
    let first_publish = first_session.publish_with_metadata(
        vec![first],
        checks(),
        changes(0, first_key, mode == "write-no-marker"),
    );
    match scenario {
        Scenario::SingleWriter => {
            first_publish.await?;
        }
        Scenario::CompetingWriters => {
            let second_session = repository.mutation_session().await?;
            let second = second_session.stage_blob(&payload(1)).await?;
            let second_key = second.record().key().clone();
            let second_publish = second_session.publish_with_metadata(
                vec![second],
                checks(),
                changes(1, second_key, false),
            );
            let (first, second) = tokio::join!(first_publish, second_publish);
            let results = [first, second];
            if results.iter().filter(|r| r.is_ok()).count() != 1
                || results
                    .iter()
                    .filter(|r| {
                        matches!(
                            r,
                            Err(RepositoryError::Metadata(MetadataError::CheckFailed {
                                index: 0
                            }))
                        )
                    })
                    .count()
                    != 1
            {
                return Err("competing writers did not admit exactly one operation".into());
            }
            // Keep staging handles alive across the process-death boundary.
            std::process::exit(EXIT_AFTER_COMMIT);
        }
    }
    // No result is sent to the supervisor. No stack unwinding, handle drops, or
    // explicit flush follows commit. This is process death, not power loss.
    std::process::exit(EXIT_AFTER_COMMIT);
}

async fn recover(work: &Path, scenario: Scenario) -> Result<Report> {
    let repository = Repository::local(work.join("repository")).await?;
    let snapshot = repository.metadata().snapshot().await?;
    let saved = snapshot.get(&[marker()]).await?.pop().flatten();
    let winner = (0..match scenario {
        Scenario::SingleWriter => 1,
        Scenario::CompetingWriters => 2,
    })
        .find(|writer| saved.as_ref() == Some(&value(*writer)))
        .ok_or("published root lacks matching durable operation marker")?;
    let expected = payload(winner);
    let root = RootName::try_from("live")?;
    let winner_key = key(&expected);
    if snapshot.root(&root).await?.as_ref() != Some(&winner_key)
        || snapshot.object(&winner_key).await?.is_none()
    {
        return Err("post-crash marker does not match published graph".into());
    }
    let loser_object_absent = snapshot.object(&key(&payload(1 - winner))).await?.is_none();
    let revision = snapshot.revision();
    drop(snapshot);
    let (_, mut reader) = repository
        .open_payload(&winner_key)
        .await?
        .ok_or("post-crash root unreadable")?;
    let mut actual = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut actual).await?;
    drop(reader);
    let stats = repository
        .payloads()
        .pack_read_stats()
        .ok_or("packed payload store missing")?;
    let exact_packed_payload_recovered = actual == expected && stats.chunk_range_requests > 0;
    let session = repository.mutation_session().await?;
    let staged = session
        .stage_blob(b"conflicting retry after process death")
        .await?;
    let retry_key = staged.record().key().clone();
    let result = session
        .publish_with_metadata(
            vec![staged],
            checks(),
            changes(winner, retry_key.clone(), false),
        )
        .await;
    drop(session);
    flush_repository_leases().await?;
    let after = repository.metadata().snapshot().await?;
    let duplicate_rejected = matches!(
        result,
        Err(RepositoryError::Metadata(MetadataError::CheckFailed {
            index: 0
        }))
    ) && after.root(&root).await?.as_ref() == Some(&winner_key)
        && after.get(&[marker()]).await?[0].as_ref() == Some(&value(winner))
        && after.object(&retry_key).await?.is_none();
    let conflict_left_revision_unchanged = after.revision() == revision;
    drop(after);
    let integrity = repository.fsck().await?;
    let integrity_healthy = integrity.is_healthy()
        && integrity
            .issues
            .iter()
            .all(|issue| issue.disposition == FsckDisposition::Collectible);
    let collectible_residue = integrity.issues.len();
    if !exact_packed_payload_recovered
        || !duplicate_rejected
        || !conflict_left_revision_unchanged
        || !loser_object_absent
        || !integrity_healthy
    {
        return Err(format!("post-crash publication invariant failed: exact_packed_payload={exact_packed_payload_recovered}, duplicate={duplicate_rejected}, revision={conflict_left_revision_unchanged}, loser_absent={loser_object_absent}, integrity={integrity_healthy}, findings={:?}", integrity.issues).into());
    }
    Ok(Report {
        scenario: scenario.name().into(),
        writer_exit_without_cleanup: false,
        independent_reader: false,
        recovered_writer: winner,
        exact_packed_payload_recovered,
        duplicate_rejected,
        conflict_left_revision_unchanged,
        loser_object_absent,
        integrity_healthy,
        collectible_residue,
    })
}
