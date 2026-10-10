//! Native process-death checks for a spike-only, atomically replaced client intent.
use crate::intent_journal::{self, Cut, Failure, FileStorage, Phase, Stage, Storage};
use bytes::Bytes;
use casita::experimental::*;
use casita::{MetadataChange, MetadataCheck, MetadataKey};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};
type Intent = intent_journal::Intent<Request>;
pub mod overlap;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy, Debug)]
pub enum Boundary {
    Submitted,
    Committed,
    Unknown,
    RecoveryTemp,
    RecoveryRenamed,
    Recovered,
}
impl Boundary {
    pub const ALL: [Self; 6] = [
        Self::Submitted,
        Self::Committed,
        Self::Unknown,
        Self::RecoveryTemp,
        Self::RecoveryRenamed,
        Self::Recovered,
    ];
    pub fn name(self) -> &'static str {
        match self {
            Self::Submitted => "submitted",
            Self::Committed => "committed",
            Self::Unknown => "unknown",
            Self::RecoveryTemp => "recovery-temp",
            Self::RecoveryRenamed => "recovery-renamed",
            Self::Recovered => "recovered",
        }
    }
    pub fn parse(s: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|b| b.name() == s)
            .ok_or_else(|| "unknown client journal boundary".into())
    }
}
#[derive(Clone, Copy, Default)]
pub enum Fault {
    #[default]
    None,
    MissingIntent,
    ChangedIdentity,
}
#[derive(Serialize, Deserialize)]
struct Request {
    operation: String,
    root: String,
    payload: Vec<u8>,
}
impl Request {
    fn key(&self) -> ObjectKey {
        ObjectKey::blob(BlobId::new(Digest::hash(&self.payload)))
    }
    fn marker(&self) -> MetadataKey {
        MetadataKey::new(
            "casita.spike.client-intents.v1".parse().unwrap(),
            self.operation.clone(),
        )
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Report {
    pub boundary: String,
    pub killed_without_cleanup: bool,
    pub independent_recovery: bool,
    pub initial_phase: String,
    pub recovered: bool,
    pub exact_payload: bool,
    pub recovery_left_revision_unchanged: bool,
    pub terminal_journal_verified: bool,
}

fn checkpoint(work: &Path) -> Result<()> {
    fs::write(work.join("ready"), b"ready")?;
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}
fn save(work: &Path, intent: &Intent, stop: Option<Boundary>) -> Result<()> {
    let mut storage = FileStorage {
        directory: work.into(),
    };
    intent_journal::persist(&mut storage, intent, |stage| {
        if matches!(
            (stop, stage),
            (Some(Boundary::RecoveryTemp), Stage::SyncFile)
                | (Some(Boundary::RecoveryRenamed), Stage::Rename)
                | (Some(Boundary::Recovered), Stage::SyncDirectory)
        ) {
            checkpoint(work)?;
        }
        Ok(())
    })
}
fn load(work: &Path) -> Result<Intent> {
    intent_journal::load(&FileStorage {
        directory: work.into(),
    })
}

pub fn run(executable: &Path, boundary: Boundary, fault: Fault) -> Result<Report> {
    let work = tempfile::tempdir()?;
    child(executable, "write", work.path(), boundary, true)?;
    match fault {
        Fault::None => (),
        Fault::MissingIntent => fs::remove_file(work.path().join("intent.json"))?,
        Fault::ChangedIdentity => {
            let mut intent = load(work.path())?;
            intent.request.operation.push_str("-wrong");
            fs::write(
                work.path().join("intent.json"),
                serde_json::to_vec(&intent)?,
            )?;
        }
    }
    let output = child(executable, "recover", work.path(), boundary, false)?;
    let mut report: Report = serde_json::from_str(&output)?;
    // A third process audits what the recovering client actually persisted.
    child(executable, "audit", work.path(), boundary, false)?;
    report.terminal_journal_verified = true;
    report.killed_without_cleanup = true;
    report.independent_recovery = true;
    Ok(report)
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ReadFailureReport {
    pub failure: String,
    pub failed_processes: usize,
    pub stopped_before_repository_access: bool,
    pub journal_unchanged: bool,
    pub metadata_unchanged: bool,
    pub recovery: Report,
}
/// Each failed reader is a fresh process. Repair never reconstructs an intent.
pub fn run_read_failure(
    executable: &Path,
    boundary: Boundary,
    failure: intent_journal::ReadFailure,
) -> Result<ReadFailureReport> {
    let work = tempfile::tempdir()?;
    child(executable, "write", work.path(), boundary, true)?;
    let current = fs::read(work.path().join("intent.json"))?;
    let revision = child(executable, "revision", work.path(), boundary, false)?;
    let mode = format!("recover-read/{}", failure.name());
    for _ in 0..3 {
        let error = child(executable, &mode, work.path(), boundary, false)
            .err()
            .ok_or("faulted recovery unexpectedly succeeded")?;
        if !error.to_string().contains(failure.diagnostic()) {
            return Err(format!("wrong read failure: {error}").into());
        }
        if work.path().join("recovery-dispatched").exists() {
            return Err("recovery accessed repository without validated intent".into());
        }
        if fs::read(work.path().join("intent.json"))? != current {
            return Err("failed journal reader changed durable intent".into());
        }
        if child(executable, "revision", work.path(), boundary, false)? != revision {
            return Err("failed journal reader changed repository metadata".into());
        }
    }
    let output = child(executable, "recover", work.path(), boundary, false)?;
    let mut recovery: Report = serde_json::from_str(&output)?;
    child(executable, "audit", work.path(), boundary, false)?;
    if !recovery.recovery_left_revision_unchanged
        || !work.path().join("recovery-dispatched").exists()
    {
        return Err("healthy reader failed recovery gate".into());
    }
    recovery.killed_without_cleanup = true;
    recovery.independent_recovery = true;
    recovery.terminal_journal_verified = true;
    Ok(ReadFailureReport {
        failure: failure.name().into(),
        failed_processes: 3,
        stopped_before_repository_access: true,
        journal_unchanged: true,
        metadata_unchanged: true,
        recovery,
    })
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorCrashReport {
    pub save: usize,
    pub stage: String,
    pub phase_after_crash: String,
    pub missing_intent_stopped: bool,
    pub io_error_before_kill: String,
    pub recovery: Report,
    pub partial_len: Option<usize>,
    pub partial_full_len: Option<usize>,
    pub partial_json_invalid: bool,
    pub retry_attempts_before_kill: usize,
    pub retry_exhausted_before_kill: bool,
}
pub fn run_error_crash(executable: &Path, stage: Stage, save: usize) -> Result<ErrorCrashReport> {
    run_failure_crash(executable, stage.into(), save, false)
}
pub fn run_partial_write_crash(
    executable: &Path,
    cut: Cut,
    save: usize,
) -> Result<ErrorCrashReport> {
    run_failure_crash(executable, Failure::Partial(cut), save, false)
}
pub fn run_persistent_crash(
    executable: &Path,
    failure: Failure,
    save: usize,
) -> Result<ErrorCrashReport> {
    run_failure_crash(executable, failure, save, true)
}
fn run_failure_crash(
    executable: &Path,
    failure: Failure,
    save: usize,
    persistent: bool,
) -> Result<ErrorCrashReport> {
    if !(1..=3).contains(&save) {
        return Err("error crash requires save 1..=3".into());
    }
    let work = tempfile::tempdir()?;
    let boundary = if save == 1 {
        Boundary::Submitted
    } else {
        Boundary::Unknown
    };
    let mode = format!(
        "{}/{save}/{}",
        if persistent { "persistent" } else { "error" },
        failure.name()
    );
    child(executable, &mode, work.path(), boundary, true)?;
    let io_error_before_kill = fs::read_to_string(work.path().join("io-error"))?;
    if !io_error_before_kill.contains("injected journal") {
        return Err("writer did not report I/O error before kill".into());
    }
    let (retry_attempts_before_kill, retry_exhausted_before_kill) = if persistent {
        let stats: intent_journal::RetryStats =
            serde_json::from_slice(&fs::read(work.path().join("retry-budget"))?)?;
        if stats.saved || stats.attempts != 3 || stats.errors.len() != 3 {
            return Err("native outage ignored journal retry budget".into());
        }
        (stats.attempts, true)
    } else {
        (1, false)
    };
    let loaded = FileStorage {
        directory: work.path().into(),
    }
    .read()?;
    let phase_after_crash = match loaded {
        None => "Missing".into(),
        Some(_) => format!("{:?}", load(work.path())?.phase),
    };
    let expected = failure.surviving_phase(save);
    let (partial_len, partial_full_len, partial_json_invalid) =
        if let Failure::Partial(cut) = failure {
            let full_len =
                fs::read_to_string(work.path().join("partial-full-len"))?.parse::<usize>()?;
            let bytes = FileStorage {
                directory: work.path().into(),
            }
            .temporary()?
            .ok_or("native partial write left no temporary bytes")?;
            let invalid = serde_json::from_slice::<serde_json::Value>(&bytes).is_err();
            if bytes.len() != cut.offset(full_len) || !invalid {
                return Err("native partial write did not leave expected truncated JSON".into());
            }
            (Some(bytes.len()), Some(full_len), true)
        } else {
            (None, None, false)
        };
    if !phase_after_crash.starts_with(expected) {
        return Err("native error crash exposed wrong surviving phase".into());
    }
    let missing_intent_stopped = expected == "Missing";
    let output = child(
        executable,
        if missing_intent_stopped {
            "recover-empty"
        } else {
            "recover"
        },
        work.path(),
        boundary,
        false,
    )?;
    let mut recovery: Report = serde_json::from_str(&output)?;
    child(
        executable,
        if missing_intent_stopped {
            "audit-empty"
        } else {
            "audit"
        },
        work.path(),
        boundary,
        false,
    )?;
    recovery.killed_without_cleanup = true;
    recovery.independent_recovery = true;
    recovery.terminal_journal_verified = true;
    if recovery.recovered != (save > 1)
        || recovery.exact_payload != (save > 1)
        || !recovery.recovery_left_revision_unchanged
    {
        return Err("native error crash recovery failed".into());
    }
    Ok(ErrorCrashReport {
        save,
        stage: failure.name().into(),
        phase_after_crash,
        missing_intent_stopped,
        io_error_before_kill,
        recovery,
        partial_len,
        partial_full_len,
        partial_json_invalid,
        retry_attempts_before_kill,
        retry_exhausted_before_kill,
    })
}
async fn save_at(
    work: &Path,
    intent: &Intent,
    stop: Option<Boundary>,
    save_number: usize,
    error: Option<(usize, Failure)>,
    persistent: bool,
) -> Result<()> {
    if let Some((selected, stage)) = error
        && selected == save_number
    {
        let mut storage = FileStorage {
            directory: work.into(),
        };
        let error = if persistent {
            let active = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
            let mut outage = intent_journal::Outage {
                storage,
                failure: stage,
                active,
            };
            let stats = intent_journal::persist_bounded(
                &mut outage,
                intent,
                intent_journal::RetryPolicy::default(),
                |_, _| Ok(()),
            )
            .await?;
            if stats.saved || stats.attempts != 3 {
                return Err("native outage did not exhaust retry budget".into());
            }
            fs::write(work.join("retry-budget"), serde_json::to_vec(&stats)?)?;
            stats.errors.join("\n")
        } else {
            let mut injected = intent_journal::Injected {
                storage: &mut storage,
                fail: Some(stage),
            };
            intent_journal::persist(&mut injected, intent, |_| Ok(()))
                .expect_err("selected I/O error was not injected")
                .to_string()
        };
        // This instrumentation reports only the failure, never a receipt or
        // request from which the restarted client could reconstruct identity.
        fs::write(work.join("io-error"), error)?;
        if let Failure::Partial(_) = stage {
            fs::write(
                work.join("partial-full-len"),
                serde_json::to_vec(intent)?.len().to_string(),
            )?;
        }
        return checkpoint(work);
    }
    save(work, intent, stop)
}
fn child(
    executable: &Path,
    mode: &str,
    work: &Path,
    boundary: Boundary,
    kill: bool,
) -> Result<String> {
    let stdout = tempfile::NamedTempFile::new()?;
    let stderr = tempfile::NamedTempFile::new()?;
    let mut child = Command::new(executable)
        .args(["client-journal-worker", mode])
        .arg(work)
        .arg(boundary.name())
        .stdin(Stdio::null())
        .stdout(stdout.reopen()?)
        .stderr(stderr.reopen()?)
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if kill && work.join("ready").exists() {
            child.kill()?;
            break child.wait()?;
        }
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            return Err(format!("client journal {mode} timed out").into());
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    if kill {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            if status.signal() != Some(9) {
                return Err(format!(
                    "worker did not reach SIGKILL boundary: {status}: {}",
                    fs::read_to_string(stderr.path())?
                )
                .into());
            }
        }
        #[cfg(not(unix))]
        if status.success() {
            return Err("worker exited before kill".into());
        }
    } else if !status.success() {
        return Err(fs::read_to_string(stderr.path())?.into());
    }
    Ok(fs::read_to_string(stdout.path())?)
}

pub async fn worker(mode: &str, work: &Path, boundary: Boundary) -> Result<()> {
    if mode == "revision" {
        let repository = Repository::local(work.join("repository")).await?;
        println!("{}", repository.metadata().snapshot().await?.revision());
        return Ok(());
    }
    if matches!(mode, "recover-empty" | "audit-empty") {
        if (FileStorage {
            directory: work.into(),
        })
        .read()?
        .is_some()
        {
            return Err("empty recovery unexpectedly found intent".into());
        }
        let repository = Repository::local(work.join("repository")).await?;
        let snapshot = repository.metadata().snapshot().await?;
        let revision = snapshot.revision();
        let marker = MetadataKey::new(
            "casita.spike.client-intents.v1".parse().unwrap(),
            "client-operation/7",
        );
        if snapshot.root(&"live".try_into()?).await?.is_some()
            || snapshot.get(&[marker]).await?[0].is_some()
        {
            return Err("missing intent was reconstructed and published".into());
        }
        let unchanged = repository.metadata().snapshot().await?.revision() == revision;
        if !unchanged {
            return Err("empty recovery mutated metadata".into());
        }
        if mode == "recover-empty" {
            println!(
                "{}",
                serde_json::to_string(&Report {
                    boundary: boundary.name().into(),
                    killed_without_cleanup: false,
                    independent_recovery: false,
                    initial_phase: "Missing".into(),
                    recovered: false,
                    exact_payload: false,
                    recovery_left_revision_unchanged: unchanged,
                    terminal_journal_verified: false
                })?
            );
        }
        return Ok(());
    }
    let persistent = mode.starts_with("persistent/");
    let error = if let Some(encoded) = mode
        .strip_prefix("error/")
        .or_else(|| mode.strip_prefix("persistent/"))
    {
        let (save, stage) = encoded.split_once('/').ok_or("invalid error crash mode")?;
        Some((save.parse::<usize>()?, Failure::parse(stage)?))
    } else {
        None
    };
    if matches!(mode, "write" | "write-owned") || error.is_some() {
        let owned_writer = mode == "write-owned";
        let _ownership = if owned_writer {
            Some(overlap::acquire(work)?)
        } else {
            None
        };
        let request = Request {
            operation: "client-operation/7".into(),
            root: "live".into(),
            payload: (0..65537).map(|i| (i % 251) as u8).collect(),
        };
        let mut intent = Intent::new(request)?;
        // Initialize server storage first so absence can be queried after a pre-dispatch kill.
        let repository = Repository::local(work.join("repository")).await?;
        save_at(work, &intent, None, 1, error, persistent).await?;
        if error.is_none() && matches!(boundary, Boundary::Submitted) {
            if owned_writer {
                overlap::hold(work)?;
            } else {
                checkpoint(work)?;
            }
        }
        let session = repository.mutation_session().await?;
        let staged = session.stage_blob(&intent.request.payload).await?;
        session
            .publish_with_metadata(
                vec![staged],
                vec![MetadataCheck::Record {
                    key: intent.request.marker(),
                    expected: None,
                }],
                vec![
                    MetadataChange::SetRoot {
                        name: intent.request.root.as_str().try_into()?,
                        target: intent.request.key(),
                    },
                    MetadataChange::Set {
                        key: intent.request.marker(),
                        value: Bytes::from(intent.fingerprint.clone()),
                    },
                ],
            )
            .await?;
        if error.is_none() && matches!(boundary, Boundary::Committed) {
            if owned_writer {
                overlap::hold(work)?;
            } else {
                checkpoint(work)?;
            }
        }
        intent.mark_unknown()?;
        save_at(work, &intent, None, 2, error, persistent).await?;
        if error.is_none() && matches!(boundary, Boundary::Unknown) {
            if owned_writer {
                overlap::hold(work)?;
            } else {
                checkpoint(work)?;
            }
        }
        let snapshot = repository.metadata().snapshot().await?;
        intent.mark_recovered(&intent.fingerprint.clone(), snapshot.revision().to_string())?;
        save_at(work, &intent, Some(boundary), 3, error, persistent).await?;
        return Err("writer missed crash boundary".into());
    }
    if matches!(mode, "submit-owned" | "submit-unowned") {
        let _ownership = if mode == "submit-owned" {
            Some(overlap::acquire(work)?)
        } else {
            None
        };
        // Submission must not overwrite an existing or ambiguous durable request.
        fs::write(work.join("submission-dispatched"), b"validated owner")?;
        if (FileStorage {
            directory: work.into(),
        })
        .read()?
        .is_some()
        {
            let _: Intent = load(work)?;
            return Err("durable intent already exists; recover before submission".into());
        }
        return Err("submission probe requires an existing durable intent".into());
    }
    // Ownership covers validated loading, repository reads and journal replacement.
    // The lock file remains after death; the kernel releases the held lock.
    let _ownership = if mode.starts_with("recover-owned") {
        Some(overlap::acquire(work)?)
    } else {
        None
    };
    if mode == "recover-owned-held-before" {
        overlap::hold(work)?;
    }
    let read_failure = mode
        .strip_prefix("recover-read/")
        .map(intent_journal::ReadFailure::parse)
        .transpose()?;
    if !matches!(
        mode,
        "recover"
            | "audit"
            | "recover-owned"
            | "recover-owned-held-before"
            | "recover-owned-held-temp"
    ) && read_failure.is_none()
    {
        return Err("unknown client journal worker".into());
    }
    let mut intent = intent_journal::load::<Request>(&intent_journal::ReadInjected {
        storage: &FileStorage {
            directory: work.into(),
        },
        failure: read_failure,
    })?;
    // This gate precedes every repository access in intent-based recovery.
    fs::write(work.join("recovery-dispatched"), b"validated")?;
    let initial_phase = format!("{:?}", intent.phase);
    let repository = Repository::local(work.join("repository")).await?;
    let snapshot = repository.metadata().snapshot().await?;
    let revision = snapshot.revision();
    let marker = snapshot
        .get(&[intent.request.marker()])
        .await?
        .pop()
        .flatten();
    let recovered = marker.as_ref() == Some(&Bytes::from(intent.fingerprint.clone()));
    let expected_recovered = !matches!(boundary, Boundary::Submitted);
    if recovered != expected_recovered {
        return Err("read-only recovery disagrees with crash boundary".into());
    }
    let root = snapshot
        .root(&intent.request.root.as_str().try_into()?)
        .await?;
    let mut exact_payload = false;
    if recovered {
        if root.as_ref() != Some(&intent.request.key()) {
            return Err("marker and root disagree".into());
        }
        let (_, mut reader) = repository
            .open_payload(&intent.request.key())
            .await?
            .ok_or("recovered payload absent")?;
        let mut actual = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut actual).await?;
        exact_payload = actual == intent.request.payload;
        if !exact_payload {
            return Err("recovered payload differs from durable intent".into());
        }
    } else if marker.is_some() || root.is_some() || matches!(intent.phase, Phase::Recovered { .. })
    {
        return Err("unpublished intent incorrectly resolved".into());
    }
    let terminal = if recovered {
        Phase::Recovered {
            observed_revision: revision.to_string(),
        }
    } else {
        Phase::Unknown
    };
    if mode == "audit" {
        if intent.phase != terminal {
            return Err("recovery did not persist terminal journal state".into());
        }
        return Ok(());
    }
    match terminal {
        Phase::Recovered { observed_revision } => {
            intent.mark_recovered(&intent.fingerprint.clone(), observed_revision)?
        }
        Phase::Unknown => intent.mark_unknown()?,
        Phase::Submitted => unreachable!(),
    }
    if mode == "recover-owned-held-temp" {
        let mut storage = FileStorage {
            directory: work.into(),
        };
        intent_journal::persist(&mut storage, &intent, |stage| {
            if stage == Stage::SyncFile {
                overlap::hold(work)?;
            }
            Ok(())
        })?;
        return Err("owned recovery missed temporary sync boundary".into());
    }
    let mut storage = FileStorage {
        directory: work.into(),
    };
    let stats = intent_journal::persist_bounded(
        &mut storage,
        &intent,
        intent_journal::RetryPolicy::default(),
        |_, _| Ok(()),
    )
    .await?;
    if !stats.saved || stats.attempts != 1 {
        return Err("healthy recovery journal save failed".into());
    }
    let after = repository.metadata().snapshot().await?;
    let unchanged = after.revision() == revision;
    if !unchanged {
        return Err("client recovery mutated repository metadata".into());
    }
    println!(
        "{}",
        serde_json::to_string(&Report {
            boundary: boundary.name().into(),
            killed_without_cleanup: false,
            independent_recovery: false,
            initial_phase,
            recovered,
            exact_payload,
            recovery_left_revision_unchanged: unchanged,
            terminal_journal_verified: false
        })?
    );
    Ok(())
}
