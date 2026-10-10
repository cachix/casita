//! Shared spike intent transitions and replace protocol. No production journal API.
use casita::experimental::Digest;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    Submitted,
    Unknown,
    Recovered { observed_revision: String },
}
#[derive(Serialize, Deserialize)]
pub struct Intent<R> {
    pub request: R,
    pub fingerprint: String,
    pub phase: Phase,
}
impl<R: Serialize> Intent<R> {
    pub fn new(request: R) -> Result<Self> {
        let fingerprint = fingerprint(&request)?;
        Ok(Self {
            request,
            fingerprint,
            phase: Phase::Submitted,
        })
    }
    pub fn mark_unknown(&mut self) -> Result<()> {
        if matches!(self.phase, Phase::Recovered { .. }) {
            return Err("recovered intent cannot become unknown".into());
        }
        self.phase = Phase::Unknown;
        Ok(())
    }
    pub fn mark_recovered(
        &mut self,
        expected_fingerprint: &str,
        observed_revision: String,
    ) -> Result<()> {
        self.validate()?;
        if expected_fingerprint != self.fingerprint {
            return Err("recovery identity mismatch".into());
        }
        self.phase = Phase::Recovered { observed_revision };
        Ok(())
    }
    fn validate(&self) -> Result<()> {
        if self.fingerprint != fingerprint(&self.request)? {
            return Err("client intent identity mismatch".into());
        }
        Ok(())
    }
}
pub fn fingerprint<R: Serialize>(request: &R) -> Result<String> {
    Ok(Digest::hash(&serde_json::to_vec(request)?).to_string())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Write,
    SyncFile,
    Rename,
    SyncDirectory,
}
impl Stage {
    pub fn name(self) -> &'static str {
        match self {
            Self::Write => "write",
            Self::SyncFile => "sync-file",
            Self::Rename => "rename",
            Self::SyncDirectory => "sync-directory",
        }
    }
    pub fn parse(name: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|stage| stage.name() == name)
            .ok_or_else(|| "unknown journal stage".into())
    }
    pub const ALL: [Self; 4] = [
        Self::Write,
        Self::SyncFile,
        Self::Rename,
        Self::SyncDirectory,
    ];
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cut {
    Empty,
    FirstByte,
    Half,
    BeforeEnd,
}
impl Cut {
    pub const ALL: [Self; 4] = [Self::Empty, Self::FirstByte, Self::Half, Self::BeforeEnd];
    pub fn name(self) -> &'static str {
        match self {
            Self::Empty => "partial-empty",
            Self::FirstByte => "partial-first-byte",
            Self::Half => "partial-half",
            Self::BeforeEnd => "partial-before-end",
        }
    }
    pub fn offset(self, len: usize) -> usize {
        match self {
            Self::Empty => 0,
            Self::FirstByte => 1.min(len.saturating_sub(1)),
            Self::Half => len / 2,
            Self::BeforeEnd => len.saturating_sub(1),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    Before(Stage),
    Partial(Cut),
}
impl From<Stage> for Failure {
    fn from(stage: Stage) -> Self {
        Self::Before(stage)
    }
}
impl Failure {
    pub fn name(self) -> &'static str {
        match self {
            Self::Before(stage) => stage.name(),
            Self::Partial(cut) => cut.name(),
        }
    }
    pub fn parse(name: &str) -> Result<Self> {
        if let Some(cut) = Cut::ALL.into_iter().find(|cut| cut.name() == name) {
            return Ok(Self::Partial(cut));
        }
        Ok(Self::Before(Stage::parse(name)?))
    }
    pub fn replacement_visible(self) -> bool {
        self == Self::Before(Stage::SyncDirectory)
    }
    pub fn surviving_phase(self, save: usize) -> &'static str {
        match (save, self.replacement_visible()) {
            (1, false) => "Missing",
            (1, true) | (2, false) => "Submitted",
            (2, true) | (3, false) => "Unknown",
            (3, true) => "Recovered",
            _ => panic!("save must be 1..=3"),
        }
    }
}
pub trait Storage {
    fn read(&self) -> io::Result<Option<Vec<u8>>>;
    fn temporary(&self) -> io::Result<Option<Vec<u8>>>;
    fn apply(&mut self, stage: Stage, bytes: &[u8]) -> io::Result<()>;
}
/// Faults at the recovery read boundary, independent of replacement failures.
#[derive(Clone, Copy, Debug)]
pub enum ReadFailure {
    InputOutput,
    PermissionDenied,
    Truncated,
    ChangedIdentity,
}
impl ReadFailure {
    pub const ALL: [Self; 4] = [
        Self::InputOutput,
        Self::PermissionDenied,
        Self::Truncated,
        Self::ChangedIdentity,
    ];
    pub fn name(self) -> &'static str {
        match self {
            Self::InputOutput => "read-io",
            Self::PermissionDenied => "read-permission",
            Self::Truncated => "read-truncated",
            Self::ChangedIdentity => "read-identity",
        }
    }
    pub fn parse(name: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|failure| failure.name() == name)
            .ok_or_else(|| "unknown journal read failure".into())
    }
    pub fn diagnostic(self) -> &'static str {
        match self {
            Self::InputOutput | Self::PermissionDenied => "injected journal read failure",
            Self::Truncated => "EOF while parsing",
            Self::ChangedIdentity => "client intent identity mismatch",
        }
    }
}
pub struct ReadInjected<'a, S: ?Sized> {
    pub storage: &'a S,
    pub failure: Option<ReadFailure>,
}
impl<S: Storage + ?Sized> Storage for ReadInjected<'_, S> {
    fn read(&self) -> io::Result<Option<Vec<u8>>> {
        match self.failure {
            Some(ReadFailure::InputOutput | ReadFailure::PermissionDenied) => {
                let kind = if matches!(self.failure, Some(ReadFailure::PermissionDenied)) {
                    io::ErrorKind::PermissionDenied
                } else {
                    io::ErrorKind::Other
                };
                Err(io::Error::new(kind, "injected journal read failure"))
            }
            Some(ReadFailure::Truncated) => Ok(self
                .storage
                .read()?
                .map(|bytes| bytes[..bytes.len() / 2].to_vec())),
            Some(ReadFailure::ChangedIdentity) => {
                let Some(bytes) = self.storage.read()? else {
                    return Ok(None);
                };
                let mut value: serde_json::Value = serde_json::from_slice(&bytes)?;
                value["fingerprint"] = serde_json::Value::String("wrong".into());
                Ok(Some(serde_json::to_vec(&value)?))
            }
            None => self.storage.read(),
        }
    }
    fn temporary(&self) -> io::Result<Option<Vec<u8>>> {
        self.storage.temporary()
    }
    fn apply(&mut self, _: Stage, _: &[u8]) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "read fault wrapper is read-only",
        ))
    }
}
pub fn load<R: Serialize + DeserializeOwned>(
    storage: &(impl Storage + ?Sized),
) -> Result<Intent<R>> {
    let bytes = storage.read()?.ok_or("durable client intent unavailable")?;
    let intent: Intent<R> = serde_json::from_slice(&bytes)?;
    intent.validate()?;
    Ok(intent)
}
pub fn persist<R: Serialize>(
    storage: &mut (impl Storage + ?Sized),
    intent: &Intent<R>,
    mut after: impl FnMut(Stage) -> Result<()>,
) -> Result<()> {
    intent.validate()?;
    let bytes = serde_json::to_vec(intent)?;
    for stage in Stage::ALL {
        storage.apply(stage, &bytes)?;
        after(stage)?;
    }
    Ok(())
}
#[derive(Default)]
pub struct MemoryStorage {
    pub current: Option<Vec<u8>>,
    temporary: Option<Vec<u8>>,
}
impl Storage for MemoryStorage {
    fn read(&self) -> io::Result<Option<Vec<u8>>> {
        Ok(self.current.clone())
    }
    fn temporary(&self) -> io::Result<Option<Vec<u8>>> {
        Ok(self.temporary.clone())
    }
    fn apply(&mut self, stage: Stage, bytes: &[u8]) -> io::Result<()> {
        match stage {
            Stage::Write => self.temporary = Some(bytes.to_vec()),
            Stage::Rename => {
                self.current = Some(
                    self.temporary
                        .take()
                        .ok_or_else(|| io::Error::other("temporary journal absent"))?,
                )
            }
            Stage::SyncFile | Stage::SyncDirectory => (),
        }
        Ok(())
    }
}
pub struct FileStorage {
    pub directory: PathBuf,
}
impl Storage for FileStorage {
    fn read(&self) -> io::Result<Option<Vec<u8>>> {
        match fs::read(self.directory.join("intent.json")) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
    fn temporary(&self) -> io::Result<Option<Vec<u8>>> {
        match fs::read(self.directory.join("intent.tmp")) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
    fn apply(&mut self, stage: Stage, bytes: &[u8]) -> io::Result<()> {
        match stage {
            Stage::Write => fs::File::create(self.directory.join("intent.tmp"))?.write_all(bytes),
            Stage::SyncFile => fs::File::open(self.directory.join("intent.tmp"))?.sync_all(),
            Stage::Rename => fs::rename(
                self.directory.join("intent.tmp"),
                self.directory.join("intent.json"),
            ),
            Stage::SyncDirectory => fs::File::open(&self.directory)?.sync_all(),
        }
    }
}
/// One-shot failure before an operation, or after writing a temporary prefix.
/// A sync-directory error
/// follows replacement: callers must reload, since the new record may be visible.
pub struct Injected<'a, S: ?Sized> {
    pub storage: &'a mut S,
    pub fail: Option<Failure>,
}
impl<S: Storage + ?Sized> Storage for Injected<'_, S> {
    fn read(&self) -> io::Result<Option<Vec<u8>>> {
        self.storage.read()
    }
    fn temporary(&self) -> io::Result<Option<Vec<u8>>> {
        self.storage.temporary()
    }
    fn apply(&mut self, stage: Stage, bytes: &[u8]) -> io::Result<()> {
        if stage == Stage::Write
            && let Some(Failure::Partial(cut)) = self.fail
        {
            self.fail = None;
            self.storage
                .apply(Stage::Write, &bytes[..cut.offset(bytes.len())])?;
            return Err(io::Error::other(format!(
                "injected journal {} failure after prefix write",
                cut.name()
            )));
        }
        if self.fail == Some(Failure::Before(stage)) {
            self.fail = None;
            return Err(io::Error::other(format!(
                "injected journal {stage:?} failure"
            )));
        }
        self.storage.apply(stage, bytes)
    }
}
/// A storage outage lasts until its shared switch is explicitly cleared.
pub struct Outage<S> {
    pub storage: S,
    pub failure: Failure,
    pub active: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
impl<S: Storage> Storage for Outage<S> {
    fn read(&self) -> io::Result<Option<Vec<u8>>> {
        self.storage.read()
    }
    fn temporary(&self) -> io::Result<Option<Vec<u8>>> {
        self.storage.temporary()
    }
    fn apply(&mut self, stage: Stage, bytes: &[u8]) -> io::Result<()> {
        if self.active.load(std::sync::atomic::Ordering::SeqCst) {
            Injected {
                storage: &mut self.storage,
                fail: Some(self.failure),
            }
            .apply(stage, bytes)
        } else {
            self.storage.apply(stage, bytes)
        }
    }
}
#[derive(Clone, Copy)]
pub struct RetryPolicy {
    pub attempts: usize,
    pub delay: std::time::Duration,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            attempts: 3,
            delay: std::time::Duration::from_millis(5),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryStats {
    pub attempts: usize,
    pub saved: bool,
    pub errors: Vec<String>,
}
/// Retries only a complete local replacement, never a remote publication.
/// Visibility after rename is insufficient: all four operations must succeed.
pub async fn persist_bounded<R: Serialize>(
    storage: &mut impl Storage,
    intent: &Intent<R>,
    policy: RetryPolicy,
    mut on_error: impl FnMut(usize, &dyn Storage) -> Result<()>,
) -> Result<RetryStats> {
    if policy.attempts == 0 {
        return Err("journal retry budget must allow an attempt".into());
    }
    intent.validate()?;
    let mut stats = RetryStats {
        attempts: 0,
        saved: false,
        errors: Vec::new(),
    };
    for attempt in 1..=policy.attempts {
        stats.attempts = attempt;
        match persist(storage, intent, |_| Ok(())) {
            Ok(()) => {
                stats.saved = true;
                return Ok(stats);
            }
            Err(error) => {
                stats.errors.push(error.to_string());
                on_error(attempt, storage)?;
            }
        }
        if attempt < policy.attempts {
            tokio::time::sleep(policy.delay).await;
        }
    }
    Ok(stats)
}
pub fn verify_partial(
    storage: &(impl Storage + ?Sized),
    cut: Cut,
    expected: &[u8],
) -> Result<usize> {
    let bytes = storage
        .temporary()?
        .ok_or("partial write left no temporary file")?;
    let offset = cut.offset(expected.len());
    if bytes != expected[..offset] || serde_json::from_slice::<serde_json::Value>(&bytes).is_ok() {
        return Err("temporary bytes are not the expected truncated intent".into());
    }
    Ok(bytes.len())
}
#[cfg(test)]
mod tests {
    use super::*;
    fn read_failure_matrix(storage: &mut impl Storage) {
        let mut intent = Intent::new("operation/7").unwrap();
        for phase in [
            Phase::Submitted,
            Phase::Unknown,
            Phase::Recovered {
                observed_revision: "r1".into(),
            },
        ] {
            intent.phase = phase.clone();
            persist(storage, &intent, |_| Ok(())).unwrap();
            let before = storage.read().unwrap();
            // Even a valid temporary record cannot replace unreadable current identity.
            storage
                .apply(Stage::Write, &serde_json::to_vec(&intent).unwrap())
                .unwrap();
            for failure in ReadFailure::ALL {
                let mut injected = ReadInjected {
                    storage: &*storage,
                    failure: Some(failure),
                };
                for _ in 0..3 {
                    let error = load::<String>(&injected)
                        .err()
                        .expect("read fault accepted");
                    assert!(error.to_string().contains(failure.diagnostic()), "{error}");
                }
                assert_eq!(storage.read().unwrap(), before);
                injected.failure = None;
                let loaded: Intent<String> = load(&injected).unwrap();
                assert_eq!(loaded.phase, phase);
                assert_eq!(loaded.fingerprint, intent.fingerprint);
            }
        }
    }
    #[test]
    fn modeled_read_errors_stop_until_valid_identity_is_available() {
        read_failure_matrix(&mut MemoryStorage::default());
    }
    #[test]
    fn native_read_errors_stop_until_valid_identity_is_available() {
        let work = tempfile::tempdir().unwrap();
        read_failure_matrix(&mut FileStorage {
            directory: work.path().into(),
        });
    }
    #[test]
    fn terminal_intent_cannot_regress_or_accept_another_identity() {
        let mut intent = Intent::new("operation/7").unwrap();
        assert!(intent.mark_recovered("wrong", "r1".into()).is_err());
        assert_eq!(intent.phase, Phase::Submitted);
        intent.mark_unknown().unwrap();
        intent
            .mark_recovered(&intent.fingerprint.clone(), "r1".into())
            .unwrap();
        assert!(intent.mark_unknown().is_err());
        assert!(matches!(intent.phase, Phase::Recovered { .. }));
    }
    fn failure_matrix(storage: &mut (impl Storage + ?Sized)) {
        let mut intent = Intent::new("operation/7").unwrap();
        persist(storage, &intent, |_| Ok(())).unwrap();
        for phase in [
            Phase::Unknown,
            Phase::Recovered {
                observed_revision: "r1".into(),
            },
        ] {
            for stage in Stage::ALL {
                intent.phase = if matches!(phase, Phase::Unknown) {
                    Phase::Submitted
                } else {
                    Phase::Unknown
                };
                persist(storage, &intent, |_| Ok(())).unwrap();
                let previous: Intent<String> = load(storage).unwrap();
                intent.phase = phase.clone();
                let mut injected = Injected {
                    storage,
                    fail: Some(stage.into()),
                };
                let error = persist(&mut injected, &intent, |_| Ok(())).unwrap_err();
                assert!(error.to_string().contains("injected journal"));
                let loaded: Intent<String> = load(&injected).unwrap();
                assert_eq!(
                    loaded.phase,
                    if stage == Stage::SyncDirectory {
                        phase.clone()
                    } else {
                        previous.phase
                    }
                );
                assert_eq!(loaded.fingerprint, intent.fingerprint);
                persist(&mut injected, &intent, |_| Ok(())).unwrap();
                assert_eq!(load::<String>(&injected).unwrap().phase, phase);
            }
        }
    }
    #[test]
    fn modeled_replace_failures_preserve_complete_intent() {
        failure_matrix(&mut MemoryStorage::default());
    }
    #[test]
    fn native_replace_failures_preserve_complete_intent() {
        let directory = tempfile::tempdir().unwrap();
        failure_matrix(&mut FileStorage {
            directory: directory.path().into(),
        });
        for stage in Stage::ALL {
            let first = tempfile::tempdir().unwrap();
            initial_failure(
                &mut FileStorage {
                    directory: first.path().into(),
                },
                stage,
            );
        }
    }
    fn initial_failure(storage: &mut (impl Storage + ?Sized), stage: Stage) {
        let intent = Intent::new("operation/7").unwrap();
        let mut injected = Injected {
            storage,
            fail: Some(stage.into()),
        };
        assert!(persist(&mut injected, &intent, |_| Ok(())).is_err());
        assert_eq!(
            injected.read().unwrap().is_some(),
            stage == Stage::SyncDirectory
        );
        persist(&mut injected, &intent, |_| Ok(())).unwrap();
        assert_eq!(load::<String>(&injected).unwrap().phase, Phase::Submitted);
    }
    #[test]
    fn first_save_failure_reports_error_before_retry() {
        for stage in Stage::ALL {
            initial_failure(&mut MemoryStorage::default(), stage);
        }
    }
    fn partial_matrix(mut fresh: impl FnMut() -> Box<dyn Storage>) {
        for cut in Cut::ALL {
            for save in 1..=3 {
                let mut storage = fresh();
                let mut intent = Intent::new(("operation/7", "Unicode λ payload")).unwrap();
                if save > 1 {
                    persist(&mut *storage, &intent, |_| Ok(())).unwrap();
                    intent.mark_unknown().unwrap();
                }
                if save > 2 {
                    persist(&mut *storage, &intent, |_| Ok(())).unwrap();
                    intent
                        .mark_recovered(&intent.fingerprint.clone(), "revision/1".into())
                        .unwrap();
                }
                let previous = storage.read().unwrap();
                let expected = serde_json::to_vec(&intent).unwrap();
                let mut injected = Injected {
                    storage: &mut *storage,
                    fail: Some(Failure::Partial(cut)),
                };
                assert!(
                    persist(&mut injected, &intent, |_| Ok(()))
                        .unwrap_err()
                        .to_string()
                        .contains("prefix write")
                );
                assert_eq!(injected.read().unwrap(), previous);
                assert_eq!(
                    verify_partial(&injected, cut, &expected).unwrap(),
                    cut.offset(expected.len())
                );
                // A local retry rewrites the prefix completely before rename.
                persist(&mut injected, &intent, |_| Ok(())).unwrap();
                assert_eq!(injected.read().unwrap(), Some(expected));
                assert!(injected.temporary().unwrap().is_none());
            }
        }
    }
    #[test]
    fn modeled_partial_write_preserves_current_and_retry_replaces_prefix() {
        partial_matrix(|| Box::new(MemoryStorage::default()));
    }
    #[test]
    fn native_partial_write_preserves_current_and_retry_replaces_prefix() {
        let directory = tempfile::tempdir().unwrap();
        let mut index = 0;
        partial_matrix(|| {
            index += 1;
            let path = directory.path().join(index.to_string());
            fs::create_dir(&path).unwrap();
            Box::new(FileStorage { directory: path })
        });
    }
    async fn outage_matrix<S: Storage>(mut fresh: impl FnMut() -> S) {
        for failure in Stage::ALL
            .into_iter()
            .map(Failure::Before)
            .chain(Cut::ALL.into_iter().map(Failure::Partial))
        {
            for save in 1..=3 {
                let mut storage = fresh();
                let mut intent = Intent::new("operation/7").unwrap();
                if save > 1 {
                    persist(&mut storage, &intent, |_| Ok(())).unwrap();
                    intent.mark_unknown().unwrap();
                }
                if save > 2 {
                    persist(&mut storage, &intent, |_| Ok(())).unwrap();
                    intent
                        .mark_recovered(&intent.fingerprint.clone(), "r1".into())
                        .unwrap();
                }
                let before = storage.read().unwrap();
                let active = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
                let mut outage = Outage {
                    storage,
                    failure,
                    active: active.clone(),
                };
                let mut observed = 0;
                let stats =
                    persist_bounded(&mut outage, &intent, RetryPolicy::default(), |_, _| {
                        observed += 1;
                        Ok(())
                    })
                    .await
                    .unwrap();
                assert!(!stats.saved);
                assert_eq!(stats.attempts, 3);
                assert_eq!(stats.errors.len(), 3);
                assert_eq!(observed, 3);
                assert_eq!(
                    outage.read().unwrap(),
                    if failure.replacement_visible() {
                        Some(serde_json::to_vec(&intent).unwrap())
                    } else {
                        before
                    }
                );
                active.store(false, std::sync::atomic::Ordering::SeqCst);
                let healed =
                    persist_bounded(&mut outage, &intent, RetryPolicy::default(), |_, _| Ok(()))
                        .await
                        .unwrap();
                assert!(healed.saved);
                assert_eq!(healed.attempts, 1);
                assert!(healed.errors.is_empty());
                assert_eq!(load::<String>(&outage).unwrap().phase, intent.phase);
            }
        }
    }
    #[tokio::test]
    async fn persistent_outage_exhausts_budget_and_heals_without_losing_intent() {
        outage_matrix(MemoryStorage::default).await;
    }
    #[tokio::test]
    async fn native_persistent_outage_exhausts_budget_and_heals() {
        let directory = tempfile::tempdir().unwrap();
        let mut index = 0;
        outage_matrix(|| {
            index += 1;
            let path = directory.path().join(index.to_string());
            fs::create_dir(&path).unwrap();
            FileStorage { directory: path }
        })
        .await;
    }
}
