//! Spike-only nonblocking file ownership across recovery processes.
use super::*;

#[derive(Debug, Serialize)]
pub struct OverlapReport {
    pub boundary: String,
    pub interaction: String,
    pub submission_refused_existing_intent: bool,
    pub owner_after_temporary_sync: bool,
    pub rejected_contenders: usize,
    pub journal_unchanged_while_owned: bool,
    pub metadata_unchanged: bool,
    pub recovery_after_owner_death: Report,
}
struct Owner(std::process::Child);
impl Drop for Owner {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
pub(super) fn acquire(work: &Path) -> Result<fs::File> {
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(work.join("recovery.lock"))?;
    match lock.try_lock() {
        Ok(()) => (),
        Err(fs::TryLockError::WouldBlock) => return Err("journal ownership busy".into()),
        Err(fs::TryLockError::Error(error)) => return Err(error.into()),
    }
    Ok(lock)
}
pub(super) fn hold(work: &Path) -> Result<()> {
    fs::write(work.join("owner-ready"), b"locked")?;
    checkpoint(work)
}
pub fn run(
    executable: &Path,
    boundary: Boundary,
    temporary: bool,
    bypass: bool,
) -> Result<OverlapReport> {
    run_case(executable, boundary, temporary, bypass, "recoverers")
}
/// Both submission/recovery owner directions, using the same lock identity.
pub fn run_submission(
    executable: &Path,
    boundary: Boundary,
    temporary: bool,
    submission_owns: bool,
    bypass: bool,
) -> Result<OverlapReport> {
    if submission_owns
        && (temporary
            || !matches!(
                boundary,
                Boundary::Submitted | Boundary::Committed | Boundary::Unknown
            ))
    {
        return Err("unsupported submission ownership boundary".into());
    }
    run_case(
        executable,
        boundary,
        temporary,
        bypass,
        if submission_owns {
            "submission-owns"
        } else {
            "recovery-owns"
        },
    )
}
fn run_case(
    executable: &Path,
    boundary: Boundary,
    temporary: bool,
    bypass: bool,
    interaction: &str,
) -> Result<OverlapReport> {
    let work = tempfile::tempdir()?;
    let submission_owns = interaction == "submission-owns";
    let submission_contends = interaction == "recovery-owns";
    let original_revision = if submission_owns {
        None
    } else {
        child(executable, "write", work.path(), boundary, true)?;
        Some(child(executable, "revision", work.path(), boundary, false)?)
    };
    let stdout = tempfile::NamedTempFile::new()?;
    let stderr = tempfile::NamedTempFile::new()?;
    let mode = if submission_owns {
        "write-owned"
    } else if temporary {
        "recover-owned-held-temp"
    } else {
        "recover-owned-held-before"
    };
    let mut owner = Owner(
        Command::new(executable)
            .args(["client-journal-worker", mode])
            .arg(work.path())
            .arg(boundary.name())
            .stdin(Stdio::null())
            .stdout(stdout.reopen()?)
            .stderr(stderr.reopen()?)
            .spawn()?,
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while !work.path().join("owner-ready").exists() {
        if let Some(status) = owner.0.try_wait()? {
            return Err(format!(
                "ownership holder exited {status}: {}",
                fs::read_to_string(stderr.path())?
            )
            .into());
        }
        if Instant::now() >= deadline {
            return Err("ownership holder timed out".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    // The owner may have validated its own request already. Count only contenders.
    let dispatch = work.path().join("recovery-dispatched");
    if dispatch.exists() {
        fs::remove_file(&dispatch)?;
    }
    let submission_dispatch = work.path().join("submission-dispatched");
    let original = fs::read(work.path().join("intent.json"))?;
    let temporary_bytes = (FileStorage {
        directory: work.path().into(),
    })
    .temporary()?;
    for _ in 0..3 {
        let result = child(
            executable,
            if submission_contends {
                if bypass {
                    "submit-unowned"
                } else {
                    "submit-owned"
                }
            } else if bypass {
                "recover"
            } else {
                "recover-owned"
            },
            work.path(),
            boundary,
            false,
        );
        if dispatch.exists() || submission_dispatch.exists() {
            return Err("contender dispatched while another process owned intent".into());
        }
        let error = result
            .err()
            .ok_or("contender unexpectedly acquired owned intent")?;
        if !error.to_string().contains("journal ownership busy") {
            return Err(error);
        }
        if fs::read(work.path().join("intent.json"))? != original
            || (FileStorage {
                directory: work.path().into(),
            })
            .temporary()?
                != temporary_bytes
        {
            return Err("rejected contender changed owned journal".into());
        }
    }
    // Do not reopen the database while its holder is deliberately parked.
    owner.0.kill()?;
    let status = owner.0.wait()?;
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if status.signal() != Some(9) {
            return Err("ownership holder was not SIGKILLed".into());
        }
    }
    let sealed_revision = child(executable, "revision", work.path(), boundary, false)?;
    if original_revision
        .as_ref()
        .is_some_and(|before| before != &sealed_revision)
    {
        return Err("ownership contention changed metadata".into());
    }
    if submission_contends {
        let error = child(executable, "submit-owned", work.path(), boundary, false)
            .err()
            .ok_or("submission accepted an existing intent")?;
        if !error.to_string().contains("durable intent already exists")
            || fs::read(work.path().join("intent.json"))? != original
            || (FileStorage {
                directory: work.path().into(),
            })
            .temporary()?
                != temporary_bytes
        {
            return Err("submission replaced prior intent after owner death".into());
        }
    }
    let output = child(executable, "recover-owned", work.path(), boundary, false)?;
    let mut recovery: Report = serde_json::from_str(&output)?;
    child(executable, "audit", work.path(), boundary, false)?;
    if child(executable, "revision", work.path(), boundary, false)? != sealed_revision {
        return Err("recovery after owner death changed metadata".into());
    }
    recovery.terminal_journal_verified = true;
    recovery.independent_recovery = true;
    recovery.killed_without_cleanup = true;
    Ok(OverlapReport {
        boundary: boundary.name().into(),
        interaction: interaction.into(),
        submission_refused_existing_intent: submission_contends,
        owner_after_temporary_sync: temporary,
        rejected_contenders: 3,
        journal_unchanged_while_owned: true,
        metadata_unchanged: true,
        recovery_after_owner_death: recovery,
    })
}
