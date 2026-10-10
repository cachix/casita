//! Effect recovery through atomic application records, rather than a reply journal.
use crate::SeededEntropy;
use bytes::Bytes;
use casita::experimental::*;
use casita::{MetadataChange, MetadataCheck, MetadataKey};
use serde::{Deserialize, Serialize};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Notify,
};
use turmoil::net::{TcpListener, TcpStream};
mod client_restart;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scenario {
    LostAck,
    Restart,
    RestartAdvanced,
    Reuse,
    Writers,
    PartitionBeforeApply,
    PartitionAfterApply,
    ClientRestartBeforeApply,
    ClientRestartAfterApply,
}
impl Scenario {
    pub const ALL: [Self; 9] = [
        Self::LostAck,
        Self::Restart,
        Self::RestartAdvanced,
        Self::Reuse,
        Self::Writers,
        Self::PartitionBeforeApply,
        Self::PartitionAfterApply,
        Self::ClientRestartBeforeApply,
        Self::ClientRestartAfterApply,
    ];
    pub const PARTITIONS: [Self; 2] = [Self::PartitionBeforeApply, Self::PartitionAfterApply];
    pub const CLIENT_RESTARTS: [Self; 2] = [
        Self::ClientRestartBeforeApply,
        Self::ClientRestartAfterApply,
    ];
    pub fn name(self) -> &'static str {
        match self {
            Self::LostAck => "marker-lost-ack",
            Self::Restart => "marker-restart",
            Self::RestartAdvanced => "marker-restart-advanced",
            Self::Reuse => "marker-id-reuse",
            Self::Writers => "marker-writers",
            Self::PartitionBeforeApply => "marker-partition-before-apply",
            Self::PartitionAfterApply => "marker-partition-after-apply",
            Self::ClientRestartBeforeApply => "marker-client-restart-before-apply",
            Self::ClientRestartAfterApply => "marker-client-restart-after-apply",
        }
    }
    fn restart(self) -> bool {
        matches!(self, Self::Restart | Self::RestartAdvanced)
    }
    fn partition(self) -> bool {
        Self::PARTITIONS.contains(&self)
    }
    fn client_restart(self) -> bool {
        Self::CLIENT_RESTARTS.contains(&self)
    }
    fn pause_before(self) -> bool {
        matches!(
            self,
            Self::PartitionBeforeApply | Self::ClientRestartBeforeApply
        )
    }
}
#[derive(Default, Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Report {
    pub events: Vec<String>,
    pub times_ns: Vec<u128>,
    pub requests: usize,
    pub server_errors: usize,
    pub target_applications: usize,
    pub marker_recoveries: usize,
    pub rejected_reuse: usize,
    pub transport_failures: usize,
    pub server_epochs: usize,
    pub restarts: usize,
    pub staged_writers: usize,
    pub original_revision: String,
    pub recovery_revision: String,
    pub winner: usize,
    pub graph_readable_after_gc: bool,
    pub garbage_collected: bool,
    pub partitions: usize,
    pub repairs: usize,
    pub deadline_expirations: usize,
    pub deadline_elapsed_ns: Vec<u128>,
    pub unknown_outcomes: usize,
    pub marker_absent_queries: usize,
    pub query_requests: usize,
    pub abandoned_connections: usize,
    pub abandoned_replies: usize,
    pub gc_during_unknown: bool,
    pub pending_bytes_survived: bool,
    pub client_journal_read_failures: usize,
    pub client_journal_read_reboots: usize,
    pub client_journal_read_diagnostics: Vec<String>,
    pub client_journal_read_repaired: bool,
    pub client_journal_read_unchanged: bool,
    pub client_epochs: usize,
    pub client_restarts: usize,
    pub client_intent_writes: usize,
    pub client_journal_failures: usize,
    pub client_journal_batches: Vec<crate::intent_journal::RetryStats>,
    pub client_journal_retry_exhaustions: usize,
    pub client_journal_storage_repaired: bool,
    pub client_journal_outage_quiescent: bool,
    pub client_journal_partial_len: usize,
    pub client_journal_partial_full_len: usize,
    pub client_journal_partial_proven: bool,
    pub client_journal_retries: usize,
    pub client_journal_error_crashes: usize,
    pub client_journal_phase_after_crash: String,
    pub client_missing_intent_stopped: bool,
    pub client_recovery_left_revision_unchanged: bool,
    pub client_intent_recoveries: usize,
    pub client_intent_bytes: String,
    pub gc_during_client_outage: bool,
}
#[derive(Clone, Serialize, Deserialize)]
struct Request {
    operation: String,
    root: String,
    payload: Vec<u8>,
    actor: usize,
}
impl Request {
    fn fixture(actor: usize) -> Self {
        Self {
            operation: "publication/7".into(),
            root: "live".into(),
            payload: format!("writer {actor} TCP publication").into_bytes(),
            actor,
        }
    }
    fn marker_key(&self) -> MetadataKey {
        MetadataKey::new(
            "casita.spike.rpc.v1".parse().unwrap(),
            self.operation.clone(),
        )
    }
    fn fingerprint(&self) -> String {
        crate::intent_journal::fingerprint(self).unwrap()
    }
    fn object_key(&self) -> ObjectKey {
        ObjectKey::blob(BlobId::new(Digest::hash(&self.payload)))
    }
}
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Response {
    Applied {
        revision: String,
    },
    RecoveredEffect {
        request_fingerprint: String,
        observed_revision: String,
    },
    RejectedReuse,
    MarkerAbsent,
}
#[derive(Serialize, Deserialize)]
struct Envelope {
    request: Request,
    query_only: bool,
}
#[derive(Debug, PartialEq, Eq)]
enum ClientOutcome {
    Known(Response),
    Unknown {
        operation: String,
        request_fingerprint: String,
    },
    DefinitiveFailure,
}
#[derive(Clone, Copy, Default)]
struct Faults {
    omit_marker: bool,
    ignore_identity: bool,
    deadline_as_failure: bool,
    query_reapplies: bool,
    lose_client_intent: bool,
    restart_republishes: bool,
    journal_read_failure: Option<crate::intent_journal::ReadFailure>,
    ignore_journal_read_failure: bool,
    recovery_save_republishes: bool,
    combined_persistent_save: bool,
    retry_before_save_repair: bool,
    restart_with_save_outage: bool,
    restart_extra_attempt: bool,
    combined_save_crash: bool,
    save_crash_republishes: bool,
    journal_failure: Option<crate::intent_journal::Failure>,
    journal_failure_at: usize,
    crash_on_journal_error: bool,
    promote_temporary: bool,
    persistent_journal: bool,
    repair_within_budget: bool,
    extra_journal_attempt: bool,
}
const RECOVERY_DEADLINE: Duration = Duration::from_millis(100);
#[derive(Clone)]
struct Server {
    repository: Repository<MemoryBlobStore, MemoryMetadataStore>,
    log: Arc<Mutex<Report>>,
    scenario: Scenario,
    seed: u64,
    entered: Arc<Notify>,
    restart_requested: Arc<AtomicBool>,
    fault_used: Arc<AtomicBool>,
    staged: Arc<AtomicUsize>,
    gates: [Arc<Notify>; 2],
    faults: Faults,
    resume: Arc<Notify>,
}
impl Server {
    fn event(&self, event: impl Into<String>) {
        let mut report = self.log.lock().unwrap();
        report.events.push(event.into());
        report
            .times_ns
            .push(turmoil::sim_elapsed().unwrap().as_nanos());
    }
    async fn recover(&self, request: &Request) -> Result<Option<Response>, MetadataError> {
        let snapshot = self.repository.metadata().snapshot().await?;
        let value = snapshot.get(&[request.marker_key()]).await?.pop().flatten();
        if let Some(value) = value {
            if value.as_ref() != request.fingerprint().as_bytes() && !self.faults.ignore_identity {
                self.log.lock().unwrap().rejected_reuse += 1;
                self.event("rejected operation ID reuse with another request");
                return Ok(Some(Response::RejectedReuse));
            }
            self.log.lock().unwrap().marker_recoveries += 1;
            self.event("recovered committed effect from application marker");
            return Ok(Some(Response::RecoveredEffect {
                request_fingerprint: request.fingerprint(),
                observed_revision: snapshot.revision().to_string(),
            }));
        }
        Ok(None)
    }
    async fn serve(self) -> crate::SimResult {
        let listener = TcpListener::bind("0.0.0.0:9200").await?;
        self.log.lock().unwrap().server_epochs += 1;
        self.event("marker server listening without a result cache or journal");
        loop {
            let (socket, _) = listener.accept().await?;
            let server = self.clone();
            tokio::spawn(async move {
                if let Err(error) = server.handle(socket).await {
                    server.log.lock().unwrap().server_errors += 1;
                    server.event(format!("marker server error: {error}"));
                }
            });
        }
    }
    async fn handle(&self, mut socket: TcpStream) -> crate::SimResult {
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).await?;
        if bytes.is_empty() {
            self.log.lock().unwrap().abandoned_connections += 1;
            return Ok(());
        }
        let envelope: Envelope = serde_json::from_slice(&bytes)?;
        let request = envelope.request;
        self.log.lock().unwrap().requests += 1;
        if envelope.query_only {
            self.log.lock().unwrap().query_requests += 1;
        }
        self.event(format!(
            "received {} from actor {}",
            request.operation, request.actor
        ));
        let response = if let Some(response) = self.recover(&request).await? {
            response
        } else if envelope.query_only && !self.faults.query_reapplies {
            Response::MarkerAbsent
        } else {
            let session = self.repository.mutation_session().await?;
            let staged = session.stage_blob(&request.payload).await?;
            let partition_gate = (self.scenario.partition() || self.scenario.client_restart())
                && !self.fault_used.swap(true, Ordering::SeqCst);
            if partition_gate && self.scenario.pause_before() {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            if self.scenario == Scenario::Writers {
                self.log.lock().unwrap().staged_writers += 1;
                if self.staged.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
                    self.gates[0].notify_one();
                    self.gates[1].notify_one();
                }
                self.gates[request.actor].notified().await;
                // Distinct delays choose both possible winners across seeds without
                // relying on the order in which simultaneous waiters wake.
                tokio::time::sleep(Duration::from_millis(
                    1 + 3 * (request.actor as u64 ^ (self.seed % 2)),
                ))
                .await;
            }
            let mut changes = vec![MetadataChange::SetRoot {
                name: RootName::try_from(request.root.clone())?,
                target: request.object_key(),
            }];
            if !self.faults.omit_marker {
                changes.push(MetadataChange::Set {
                    key: request.marker_key(),
                    value: Bytes::from(request.fingerprint()),
                });
            }
            let result = session
                .publish_with_metadata(
                    vec![staged],
                    vec![MetadataCheck::Record {
                        key: request.marker_key(),
                        expected: None,
                    }],
                    changes,
                )
                .await;
            match result {
                Ok(result) => {
                    if request.operation == "publication/7" {
                        let mut log = self.log.lock().unwrap();
                        log.target_applications += 1;
                        log.original_revision = result.revision.to_string();
                    }
                    self.event(format!("atomically applied {}", request.operation));
                    if partition_gate && !self.scenario.pause_before() {
                        self.entered.notify_one();
                        self.resume.notified().await;
                    }
                    if matches!(
                        self.scenario,
                        Scenario::LostAck | Scenario::Restart | Scenario::RestartAdvanced
                    ) && request.operation == "publication/7"
                        && !self.fault_used.swap(true, Ordering::SeqCst)
                    {
                        if self.scenario.restart() {
                            self.entered.notify_one();
                            // Driver crashes this host while the response is held.
                            std::future::pending::<()>().await;
                        }
                        self.event("dropped applied publication acknowledgement");
                        return Ok(());
                    }
                    Response::Applied {
                        revision: result.revision.to_string(),
                    }
                }
                Err(RepositoryError::Metadata(MetadataError::CheckFailed { index: 0 })) => self
                    .recover(&request)
                    .await?
                    .ok_or("marker conflict lacks durable marker")?,
                Err(error) => return Err(error.into()),
            }
        };
        let delivery = async {
            socket.write_all(&serde_json::to_vec(&response)?).await?;
            socket.shutdown().await?;
            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
        if let Err(error) = delivery {
            if !self.scenario.partition() && !self.scenario.client_restart() {
                return Err(error);
            }
            self.log.lock().unwrap().abandoned_replies += 1;
            self.event("reply connection abandoned after client deadline");
        }
        Ok(())
    }
    async fn exchange(&self, request: &Request) -> ResponseResult {
        self.exchange_mode(request, false).await
    }
    async fn exchange_mode(&self, request: &Request, query_only: bool) -> ResponseResult {
        let mut socket = TcpStream::connect("metadata:9200").await?;
        socket
            .write_all(&serde_json::to_vec(&Envelope {
                request: request.clone(),
                query_only,
            })?)
            .await?;
        socket.shutdown().await?;
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).await?;
        if bytes.is_empty() {
            return Err("publication acknowledgement lost at EOF".into());
        }
        Ok(serde_json::from_slice(&bytes)?)
    }
    async fn bounded(&self, request: &Request, query_only: bool) -> ClientOutcome {
        let started = turmoil::sim_elapsed().unwrap();
        let unknown = || ClientOutcome::Unknown {
            operation: request.operation.clone(),
            request_fingerprint: request.fingerprint(),
        };
        match tokio::time::timeout(RECOVERY_DEADLINE, self.exchange_mode(request, query_only)).await
        {
            Ok(Ok(Response::MarkerAbsent)) => {
                self.log.lock().unwrap().marker_absent_queries += 1;
                self.log.lock().unwrap().unknown_outcomes += 1;
                unknown()
            }
            Ok(Ok(response)) => ClientOutcome::Known(response),
            other => {
                let mut log = self.log.lock().unwrap();
                log.transport_failures += 1;
                if other.is_err() {
                    log.deadline_expirations += 1;
                    log.deadline_elapsed_ns
                        .push((turmoil::sim_elapsed().unwrap() - started).as_nanos());
                }
                log.unknown_outcomes += 1;
                if self.faults.deadline_as_failure {
                    ClientOutcome::DefinitiveFailure
                } else {
                    unknown()
                }
            }
        }
    }
    async fn retry(&self, request: &Request) -> ResponseResult {
        match self.exchange(request).await {
            Ok(response) => Ok(response),
            Err(_) => {
                self.log.lock().unwrap().transport_failures += 1;
                self.event("client retries identical operation after transport failure");
                if self.scenario.restart() {
                    while self.log.lock().unwrap().server_epochs < 2 {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
                self.exchange(request).await
            }
        }
    }
}

type ResponseResult = Result<Response, Box<dyn std::error::Error>>;

pub fn run(seed: u64, scenario: Scenario) -> Result<Report, String> {
    run_with_fault(seed, scenario, Faults::default())
}
/// Fail validated journal loading in three fresh hosts before explicit repair.
pub fn run_journal_read_failure(
    seed: u64,
    scenario: Scenario,
    failure: crate::intent_journal::ReadFailure,
) -> Result<Report, String> {
    if !scenario.client_restart() {
        return Err("journal read failure requires client restart".into());
    }
    let report = run_with_fault(
        seed,
        scenario,
        Faults {
            journal_read_failure: Some(failure),
            ..Faults::default()
        },
    )?;
    if report.client_journal_read_failures != 3
        || report.client_journal_read_reboots != 3
        || !report.client_journal_read_repaired
        || !report.client_journal_read_unchanged
    {
        return Err("journal read failure coverage was vacuous".into());
    }
    Ok(report)
}
/// Combine three failed reader hosts with an error or partial write on Recovered save.
pub fn run_journal_read_save_failure(
    seed: u64,
    scenario: Scenario,
    read: crate::intent_journal::ReadFailure,
    save: crate::intent_journal::Failure,
) -> Result<Report, String> {
    if !scenario.client_restart() {
        return Err("combined journal faults require client restart".into());
    }
    let report = run_with_fault(
        seed,
        scenario,
        Faults {
            journal_read_failure: Some(read),
            journal_failure: Some(save),
            journal_failure_at: 3,
            ..Faults::default()
        },
    )?;
    if report.client_journal_read_failures != 3
        || report.client_journal_read_reboots != 3
        || !report.client_journal_read_repaired
        || !report.client_journal_read_unchanged
        || report.client_journal_failures != 1
        || report.client_journal_retries != 1
    {
        return Err("combined journal read/save coverage was vacuous".into());
    }
    if matches!(save, crate::intent_journal::Failure::Partial(_))
        && !report.client_journal_partial_proven
    {
        return Err("combined journal failure missed partial-write checker".into());
    }
    Ok(report)
}
/// Crash the healthy reader after its Recovered save fails, before local retry.
pub fn run_journal_read_save_crash(
    seed: u64,
    scenario: Scenario,
    read: crate::intent_journal::ReadFailure,
    save: crate::intent_journal::Failure,
) -> Result<Report, String> {
    if !scenario.client_restart() {
        return Err("combined save crash requires client restart".into());
    }
    let report = run_with_fault(
        seed,
        scenario,
        Faults {
            journal_read_failure: Some(read),
            journal_failure: Some(save),
            journal_failure_at: 3,
            combined_save_crash: true,
            ..Faults::default()
        },
    )?;
    if report.client_journal_read_failures != 3
        || report.client_journal_read_reboots != 3
        || !report.client_journal_read_repaired
        || !report.client_journal_read_unchanged
        || report.client_journal_failures != 1
        || report.client_journal_error_crashes != 1
        || report.client_journal_retries != 0
        || !report.client_recovery_left_revision_unchanged
        || (matches!(save, crate::intent_journal::Failure::Partial(_))
            && !report.client_journal_partial_proven)
    {
        return Err("combined read/save crash coverage was vacuous".into());
    }
    Ok(report)
}
/// Read repair followed by a persistent Recovered-save outage and explicit repair.
pub fn run_journal_read_save_outage(
    seed: u64,
    scenario: Scenario,
    read: crate::intent_journal::ReadFailure,
    save: crate::intent_journal::Failure,
    early_repair: bool,
) -> Result<Report, String> {
    if !scenario.client_restart() {
        return Err("combined save outage requires client restart".into());
    }
    let report = run_with_fault(
        seed,
        scenario,
        Faults {
            journal_read_failure: Some(read),
            journal_failure: Some(save),
            journal_failure_at: 3,
            persistent_journal: true,
            combined_persistent_save: true,
            repair_within_budget: early_repair,
            ..Faults::default()
        },
    )?;
    let expected_batches: Vec<(usize, bool)> = if early_repair {
        vec![(1, true), (1, true), (2, true)]
    } else {
        vec![(1, true), (1, true), (3, false), (1, true)]
    };
    let actual: Vec<_> = report
        .client_journal_batches
        .iter()
        .map(|batch| (batch.attempts, batch.saved))
        .collect();
    if actual != expected_batches
        || report.client_journal_read_failures != 3
        || report.client_journal_read_reboots != 3
        || !report.client_journal_read_repaired
        || !report.client_journal_read_unchanged
        || !report.client_journal_storage_repaired
        || report.client_journal_failures != if early_repair { 1 } else { 3 }
        || report.client_journal_retries != if early_repair { 1 } else { 2 }
        || report.client_journal_retry_exhaustions != usize::from(!early_repair)
        || (!early_repair && !report.client_journal_outage_quiescent)
        || (matches!(save, crate::intent_journal::Failure::Partial(_))
            && !report.client_journal_partial_proven)
    {
        return Err("combined persistent save outage coverage was vacuous".into());
    }
    Ok(report)
}
/// Exhaust persistent recovery-save retries, crash, repair, then boot a fresh reader.
pub fn run_journal_read_save_outage_crash(
    seed: u64,
    scenario: Scenario,
    read: crate::intent_journal::ReadFailure,
    save: crate::intent_journal::Failure,
) -> Result<Report, String> {
    if !scenario.client_restart() {
        return Err("persistent save crash requires client restart".into());
    }
    let report = run_with_fault(
        seed,
        scenario,
        Faults {
            journal_read_failure: Some(read),
            journal_failure: Some(save),
            journal_failure_at: 3,
            persistent_journal: true,
            combined_persistent_save: true,
            combined_save_crash: true,
            ..Faults::default()
        },
    )?;
    let batches: Vec<_> = report
        .client_journal_batches
        .iter()
        .map(|batch| (batch.attempts, batch.saved))
        .collect();
    if batches != vec![(1, true), (1, true), (3, false), (1, true)]
        || report.client_journal_read_failures != 3
        || report.client_journal_read_reboots != 3
        || !report.client_journal_read_repaired
        || !report.client_journal_read_unchanged
        || !report.client_journal_storage_repaired
        || !report.client_journal_outage_quiescent
        || report.client_journal_failures != 3
        || report.client_journal_retries != 2
        || report.client_journal_retry_exhaustions != 1
        || report.client_journal_error_crashes != 1
        || !report.client_recovery_left_revision_unchanged
        || (matches!(save, crate::intent_journal::Failure::Partial(_))
            && !report.client_journal_partial_proven)
    {
        return Err("persistent save crash coverage was vacuous".into());
    }
    Ok(report)
}
/// Restart before persistent save storage is repaired, exhaust a fresh bounded batch.
pub fn run_journal_restarted_outage(
    seed: u64,
    scenario: Scenario,
    read: crate::intent_journal::ReadFailure,
    save: crate::intent_journal::Failure,
) -> Result<Report, String> {
    if !scenario.client_restart() {
        return Err("restarted outage requires client restart".into());
    }
    let report = run_with_fault(
        seed,
        scenario,
        Faults {
            journal_read_failure: Some(read),
            journal_failure: Some(save),
            journal_failure_at: 3,
            persistent_journal: true,
            combined_persistent_save: true,
            combined_save_crash: true,
            restart_with_save_outage: true,
            ..Faults::default()
        },
    )?;
    let batches: Vec<_> = report
        .client_journal_batches
        .iter()
        .map(|batch| (batch.attempts, batch.saved))
        .collect();
    if batches != vec![(1, true), (1, true), (3, false), (3, false), (1, true)]
        || report.client_journal_failures != 6
        || report.client_journal_retries != 4
        || report.client_journal_retry_exhaustions != 2
        || report.client_journal_error_crashes != 1
        || report.client_journal_read_failures != 3
        || report.client_journal_read_reboots != 3
        || !report.client_journal_read_repaired
        || !report.client_journal_read_unchanged
        || !report.client_journal_storage_repaired
        || !report.client_journal_outage_quiescent
        || !report.client_recovery_left_revision_unchanged
        || (matches!(save, crate::intent_journal::Failure::Partial(_))
            && !report.client_journal_partial_proven)
    {
        return Err("restarted persistent outage coverage was vacuous".into());
    }
    Ok(report)
}
/// Exercise one one-shot client journal I/O failure in the restart protocol.
pub fn run_journal_failure(
    seed: u64,
    scenario: Scenario,
    stage: crate::intent_journal::Stage,
    write: usize,
) -> Result<Report, String> {
    if !scenario.client_restart() || !(1..=3).contains(&write) {
        return Err("journal failure requires client restart and save 1..=3".into());
    }
    let report = run_with_fault(
        seed,
        scenario,
        Faults {
            journal_failure: Some(stage.into()),
            journal_failure_at: write,
            ..Faults::default()
        },
    )?;
    if report.client_journal_failures != 1 {
        return Err("journal failure was not exercised exactly once".into());
    }
    Ok(report)
}
/// Crash before retrying a failed client journal save.
pub fn run_journal_error_crash(
    seed: u64,
    scenario: Scenario,
    stage: crate::intent_journal::Stage,
    write: usize,
) -> Result<Report, String> {
    if !scenario.client_restart() || !(1..=3).contains(&write) {
        return Err("journal error crash requires client restart and save 1..=3".into());
    }
    run_with_fault(
        seed,
        scenario,
        Faults {
            journal_failure: Some(stage.into()),
            journal_failure_at: write,
            crash_on_journal_error: true,
            ..Faults::default()
        },
    )
}
pub fn run_partial_write(
    seed: u64,
    scenario: Scenario,
    cut: crate::intent_journal::Cut,
    write: usize,
    crash: bool,
) -> Result<Report, String> {
    if !scenario.client_restart() || !(1..=3).contains(&write) {
        return Err("partial write requires client restart and save 1..=3".into());
    }
    let report = run_with_fault(
        seed,
        scenario,
        Faults {
            journal_failure: Some(crate::intent_journal::Failure::Partial(cut)),
            journal_failure_at: write,
            crash_on_journal_error: crash,
            ..Faults::default()
        },
    )?;
    if !report.client_journal_partial_proven
        || report.client_journal_partial_full_len <= report.client_journal_partial_len
    {
        return Err("partial write evidence was vacuous".into());
    }
    Ok(report)
}
pub fn run_persistent_journal(
    seed: u64,
    scenario: Scenario,
    failure: crate::intent_journal::Failure,
    write: usize,
    repair_within_budget: bool,
) -> Result<Report, String> {
    if !scenario.client_restart() || !(1..=3).contains(&write) {
        return Err("persistent journal requires client restart and save 1..=3".into());
    }
    let report = run_with_fault(
        seed,
        scenario,
        Faults {
            journal_failure: Some(failure),
            journal_failure_at: write,
            persistent_journal: true,
            repair_within_budget,
            crash_on_journal_error: !repair_within_budget,
            ..Faults::default()
        },
    )?;
    if !report.client_journal_storage_repaired
        || report.client_journal_retry_exhaustions != usize::from(!repair_within_budget)
        || !report
            .client_journal_batches
            .iter()
            .any(|batch| batch.attempts == if repair_within_budget { 2 } else { 3 })
    {
        return Err("persistent journal retry oracle was vacuous".into());
    }
    Ok(report)
}
fn run_with_fault(seed: u64, scenario: Scenario, faults: Faults) -> Result<Report, String> {
    if scenario.client_restart() {
        return client_restart::run(seed, scenario, faults);
    }
    let server = make_server(seed, scenario, faults)?;
    let log = server.log.clone();
    let mut sim = crate::harness::simulation(seed, 10, 30);
    let host = server.clone();
    sim.host("metadata", move || host.clone().serve());
    let client = server.clone();
    sim.client("client", async move { exercise(client).await });
    loop {
        let finished = sim
            .step()
            .map_err(|error| format!("seed={seed} {}: {error}", scenario.name()))?;
        if server.restart_requested.swap(false, Ordering::SeqCst) {
            sim.crash("metadata");
            log.lock().unwrap().restarts += 1;
            sim.bounce("metadata");
        }
        if finished {
            break;
        }
    }
    let report = log.lock().unwrap().clone();
    Ok(report)
}

fn make_server(seed: u64, scenario: Scenario, faults: Faults) -> Result<Server, String> {
    let state = MemoryMetadataStore::new_with_entropy(Arc::new(SeededEntropy::new(
        seed,
        "marker-metadata",
    )))
    .map_err(|error| error.to_string())?;
    let log = Arc::new(Mutex::new(Report::default()));
    Ok(Server {
        repository: Repository::new(MemoryBlobStore::new(), state),
        log: log.clone(),
        scenario,
        seed,
        entered: Arc::default(),
        restart_requested: Arc::default(),
        fault_used: Arc::default(),
        staged: Arc::default(),
        gates: [Arc::default(), Arc::default()],
        faults,
        resume: Arc::default(),
    })
}

async fn exercise(server: Server) -> crate::SimResult {
    let garbage = server
        .repository
        .payloads()
        .put_slice(b"unreferenced marker-protocol garbage")
        .await?;
    let first = Request::fixture(0);
    let response = if server.scenario.partition() {
        partition_exercise(&server, &first, garbage).await?
    } else if server.scenario.restart() {
        let pending = server.retry(&first);
        tokio::pin!(pending);
        tokio::select! {
            _ = server.entered.notified() => {},
            result = &mut pending => return Err(format!("publication completed before restart gate: {result:?}").into()),
        }
        if server.scenario == Scenario::RestartAdvanced {
            let mut other = Request::fixture(2);
            other.operation = "independent/8".into();
            other.root = "unrelated".into();
            if !matches!(server.exchange(&other).await?, Response::Applied { .. }) {
                return Err("unrelated revision did not advance".into());
            }
        }
        server.restart_requested.store(true, Ordering::SeqCst);
        pending.await?
    } else if server.scenario == Scenario::Writers {
        let second = Request::fixture(1);
        let (first, second) = tokio::join!(server.exchange(&first), server.exchange(&second));
        let responses = [first?, second?];
        let winner = responses
            .iter()
            .position(|r| matches!(r, Response::Applied { .. }))
            .ok_or("no writer applied")?;
        if !matches!(responses[1 - winner], Response::RejectedReuse) {
            return Err("operation ID reuse accepted another request".into());
        }
        server.log.lock().unwrap().winner = winner;
        Response::Applied {
            revision: server.log.lock().unwrap().original_revision.clone(),
        }
    } else {
        server.retry(&first).await?
    };
    if server.scenario.partition()
        || matches!(
            server.scenario,
            Scenario::LostAck | Scenario::Restart | Scenario::RestartAdvanced
        )
    {
        match response {
            Response::RecoveredEffect {
                request_fingerprint,
                observed_revision,
            } if request_fingerprint == first.fingerprint() => {
                server.log.lock().unwrap().recovery_revision = observed_revision
            }
            _ => {
                return Err(
                    "lost acknowledgement did not recover an explicit effect outcome".into(),
                );
            }
        }
    }
    if server.scenario == Scenario::Reuse
        && !matches!(
            server.exchange(&Request::fixture(1)).await?,
            Response::RejectedReuse
        )
    {
        return Err("operation ID reuse accepted another request".into());
    }
    audit(&server, garbage).await
}

async fn audit(server: &Server, garbage: BlobId) -> crate::SimResult {
    flush_repository_leases().await?;
    let winner = Request::fixture(server.log.lock().unwrap().winner);
    let snapshot = server.repository.metadata().snapshot().await?;
    let observed_revision = server.log.lock().unwrap().recovery_revision.clone();
    if !observed_revision.is_empty() && snapshot.revision().to_string() != observed_revision {
        return Err("recovered effect reported the wrong observed revision".into());
    }
    if snapshot.get(&[winner.marker_key()]).await?[0].as_deref()
        != Some(winner.fingerprint().as_bytes())
    {
        return Err("publication missing atomic operation marker".into());
    }
    if snapshot.root(&RootName::try_from("live")?).await?.as_ref() != Some(&winner.object_key())
        || snapshot
            .object(&Request::fixture(1 - winner.actor).object_key())
            .await?
            .is_some()
    {
        return Err("operation marker and published graph disagree".into());
    }
    drop(snapshot);
    server.repository.collect().await?;
    let after_gc = server.repository.metadata().snapshot().await?;
    if after_gc.root(&RootName::try_from("live")?).await?.as_ref() != Some(&winner.object_key())
        || after_gc.get(&[winner.marker_key()]).await?[0].as_deref()
            != Some(winner.fingerprint().as_bytes())
    {
        return Err("GC changed the recovered root or operation marker".into());
    }
    if server.scenario == Scenario::RestartAdvanced {
        let other = Request::fixture(2);
        if after_gc
            .root(&RootName::try_from("unrelated")?)
            .await?
            .as_ref()
            != Some(&other.object_key())
            || server
                .repository
                .payloads()
                .read_to_vec(&BlobId::new(Digest::hash(&other.payload)))
                .await?
                .as_deref()
                != Some(other.payload.as_slice())
        {
            return Err("intervening publication did not survive recovery and GC".into());
        }
    }
    drop(after_gc);
    if server
        .repository
        .payloads()
        .read_to_vec(&BlobId::new(Digest::hash(&winner.payload)))
        .await?
        .as_deref()
        != Some(winner.payload.as_slice())
    {
        return Err("recovered effect lost its payload during GC".into());
    }
    let (_, mut reader) = server
        .repository
        .open_payload(&winner.object_key())
        .await?
        .ok_or("recovered root is unreadable")?;
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await?;
    if bytes != winner.payload {
        return Err("recovered root bytes differ from requested effect".into());
    }
    drop(reader);
    flush_repository_leases().await?;
    let garbage_collected = !server.repository.payloads().has(&garbage).await?;
    let mut log = server.log.lock().unwrap();
    if !garbage_collected
        || log.server_errors != 0
        || log.target_applications != 1
        || log.server_epochs != if server.scenario.restart() { 2 } else { 1 }
        || (server.scenario.restart() && log.restarts != 1)
        || (matches!(
            server.scenario,
            Scenario::LostAck | Scenario::Restart | Scenario::RestartAdvanced
        ) && (log.marker_recoveries != 1 || log.transport_failures != 1))
        || (matches!(server.scenario, Scenario::Reuse | Scenario::Writers)
            && log.rejected_reuse != 1)
        || (server.scenario == Scenario::Writers
            && (log.staged_writers != 2 || log.winner != (server.seed % 2) as usize))
        || (server.scenario == Scenario::RestartAdvanced
            && log.original_revision == log.recovery_revision)
        || (server.scenario.partition()
            && (log.partitions != 1
                || log.repairs != 1
                || !(1..=2).contains(&log.deadline_expirations)
                || log.transport_failures != 2
                || log.marker_recoveries != 1
                || !log.gc_during_unknown
                || !log.pending_bytes_survived
                || log.marker_absent_queries
                    != usize::from(server.scenario == Scenario::PartitionBeforeApply)
                || log.query_requests
                    != if server.scenario == Scenario::PartitionBeforeApply {
                        2
                    } else {
                        1
                    }
                || log.unknown_outcomes
                    != if server.scenario == Scenario::PartitionBeforeApply {
                        3
                    } else {
                        2
                    }
                || log.deadline_elapsed_ns.len() != log.deadline_expirations
                || log.deadline_elapsed_ns.iter().any(|elapsed| {
                    *elapsed < RECOVERY_DEADLINE.as_nanos()
                        || *elapsed > (RECOVERY_DEADLINE + Duration::from_millis(1)).as_nanos()
                })))
        || (server.scenario.client_restart()
            && !server.faults.crash_on_journal_error
            && (log.client_epochs
                != if server.faults.combined_save_crash {
                    6
                } else if server.faults.journal_read_failure.is_some() {
                    5
                } else {
                    2
                }
                || log.client_restarts != 1
                || log.client_intent_writes != 3
                || log.client_intent_recoveries
                    != if server.faults.combined_save_crash {
                        2
                    } else {
                        1
                    }
                || !log.gc_during_client_outage
                || !log.pending_bytes_survived
                || log.marker_recoveries
                    != if server.faults.combined_save_crash {
                        2
                    } else {
                        1
                    }
                || log.transport_failures != 1
                || log.deadline_expirations != 1
                || log.marker_absent_queries != usize::from(server.scenario.pause_before())
                || log.query_requests
                    != (if server.scenario.pause_before() { 2 } else { 1 })
                        + usize::from(server.faults.combined_save_crash)
                || log.unknown_outcomes != if server.scenario.pause_before() { 2 } else { 1 }))
    {
        return Err(format!("marker protocol oracle was vacuous: {log:?}").into());
    }
    log.graph_readable_after_gc = true;
    log.garbage_collected = true;
    Ok(())
}

fn require_unknown(outcome: &ClientOutcome, request: &Request) -> crate::SimResult {
    if *outcome
        != (ClientOutcome::Unknown {
            operation: request.operation.clone(),
            request_fingerprint: request.fingerprint(),
        })
    {
        return Err("deadline falsely resolved operation".into());
    }
    Ok(())
}

async fn partition_exercise(server: &Server, request: &Request, garbage: BlobId) -> ResponseResult {
    let first = server.bounded(request, false);
    tokio::pin!(first);
    tokio::select! {
        _ = server.entered.notified() => {},
        result = &mut first => return Err(format!("publication completed before partition gate: {result:?}").into()),
    }
    turmoil::partition("client", "metadata");
    server.log.lock().unwrap().partitions += 1;
    server.event("partitioned accepted publication before acknowledgement");
    let unknown = first.await;
    require_unknown(&unknown, request)?;
    server.event("publication deadline returned unknown outcome with original identity");
    let blocked_query = server.bounded(request, true).await;
    require_unknown(&blocked_query, request)?;
    server.event("recovery query deadline preserved unknown outcome");

    // The fixture's independent observer checks both underlying states while
    // the client sees the same outcome. The paused handler still owns its pin.
    server.repository.collect().await?;
    let snapshot = server.repository.metadata().snapshot().await?;
    let applied = server.scenario == Scenario::PartitionAfterApply;
    if snapshot.get(&[request.marker_key()]).await?[0].is_some() != applied
        || snapshot.root(&RootName::try_from("live")?).await?.is_some() != applied
        || server.log.lock().unwrap().target_applications != usize::from(applied)
    {
        return Err("partition did not straddle the requested commit boundary".into());
    }
    drop(snapshot);
    let protected = server
        .repository
        .payloads()
        .read_to_vec(&BlobId::new(Digest::hash(&request.payload)))
        .await?
        .as_deref()
        == Some(request.payload.as_slice());
    if !protected || server.repository.payloads().has(&garbage).await? {
        return Err("unknown outcome lost publication protection during GC".into());
    }
    server.log.lock().unwrap().gc_during_unknown = true;
    server.log.lock().unwrap().pending_bytes_survived = protected;
    turmoil::repair("client", "metadata");
    server.log.lock().unwrap().repairs += 1;
    server.event("repaired link and queried original operation ID");
    if !applied {
        let absence = server.bounded(request, true).await;
        if !matches!(absence, ClientOutcome::Unknown { .. })
            || server.log.lock().unwrap().target_applications != 0
        {
            return Err("marker query changed an unresolved publication".into());
        }
        require_unknown(&absence, request)?;
        server.event("absent marker remains unknown while original handler is pending");
    }
    server.resume.notify_one();
    tokio::time::timeout(RECOVERY_DEADLINE, async {
        while server.log.lock().unwrap().target_applications == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .map_err(|_| "owned publication did not settle after repair")?;
    match server.bounded(request, true).await {
        ClientOutcome::Known(response @ Response::RecoveredEffect { .. }) => Ok(response),
        _ => Err("healed query failed to resolve committed effect".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn marker_corpus_replays() {
        for seed in 0..32 {
            for scenario in Scenario::ALL {
                let first = run(seed, scenario).unwrap();
                let second = run(seed, scenario).unwrap();
                assert_eq!(first, second, "seed={seed}, {}", scenario.name());
            }
        }
    }
    #[test]
    fn journal_read_failures_replay_across_fresh_client_hosts() {
        for seed in 0..32 {
            for scenario in Scenario::CLIENT_RESTARTS {
                for failure in crate::intent_journal::ReadFailure::ALL {
                    let first = run_journal_read_failure(seed, scenario, failure).unwrap();
                    assert_eq!(
                        first,
                        run_journal_read_failure(seed, scenario, failure).unwrap(),
                        "seed={seed}, {}, {}",
                        scenario.name(),
                        failure.name()
                    );
                    assert_eq!(first.client_epochs, 5);
                    assert_eq!(first.client_journal_read_diagnostics.len(), 3);
                }
            }
        }
    }
    #[test]
    fn checker_rejects_recovery_that_ignores_current_journal_read_failure() {
        for scenario in Scenario::CLIENT_RESTARTS {
            for failure in crate::intent_journal::ReadFailure::ALL {
                let error = run_with_fault(
                    7,
                    scenario,
                    Faults {
                        journal_read_failure: Some(failure),
                        ignore_journal_read_failure: true,
                        ..Faults::default()
                    },
                )
                .unwrap_err();
                assert!(
                    error.contains("journal read failure dispatched recovery before repair"),
                    "{error}"
                );
            }
        }
    }
    #[test]
    fn combined_journal_read_and_recovery_save_faults_replay() {
        use crate::intent_journal::{Cut, Failure, ReadFailure, Stage};
        for seed in 0..4 {
            for scenario in Scenario::CLIENT_RESTARTS {
                for read in ReadFailure::ALL {
                    for save in Stage::ALL
                        .into_iter()
                        .map(Failure::Before)
                        .chain(Cut::ALL.into_iter().map(Failure::Partial))
                    {
                        let first =
                            run_journal_read_save_failure(seed, scenario, read, save).unwrap();
                        assert_eq!(
                            first,
                            run_journal_read_save_failure(seed, scenario, read, save).unwrap(),
                            "seed={seed}, {}, {}, {}",
                            scenario.name(),
                            read.name(),
                            save.name()
                        );
                    }
                }
            }
        }
    }
    #[test]
    fn checker_rejects_remote_dispatch_during_recovery_save_retry() {
        use crate::intent_journal::{Cut, Failure, Stage};
        for scenario in Scenario::CLIENT_RESTARTS {
            for save in Stage::ALL
                .into_iter()
                .map(Failure::Before)
                .chain(Cut::ALL.into_iter().map(Failure::Partial))
            {
                let error = run_with_fault(
                    7,
                    scenario,
                    Faults {
                        journal_read_failure: Some(crate::intent_journal::ReadFailure::InputOutput),
                        journal_failure: Some(save),
                        journal_failure_at: 3,
                        recovery_save_republishes: true,
                        ..Faults::default()
                    },
                )
                .unwrap_err();
                assert!(
                    error.contains("recovery journal retry dispatched remote operation"),
                    "{error}"
                );
            }
        }
    }
    #[test]
    fn checker_rejects_promoting_alternate_intent_after_read_repair() {
        for scenario in Scenario::CLIENT_RESTARTS {
            for read in crate::intent_journal::ReadFailure::ALL {
                let error = run_with_fault(
                    7,
                    scenario,
                    Faults {
                        journal_read_failure: Some(read),
                        journal_failure: Some(crate::intent_journal::Stage::Write.into()),
                        journal_failure_at: 3,
                        promote_temporary: true,
                        ..Faults::default()
                    },
                )
                .unwrap_err();
                assert!(
                    error.contains("journal failure before rename changed durable intent"),
                    "{error}"
                );
            }
        }
    }
    #[test]
    fn combined_journal_read_save_crashes_replay() {
        use crate::intent_journal::{Cut, Failure, ReadFailure, Stage};
        for seed in 0..4 {
            for scenario in Scenario::CLIENT_RESTARTS {
                for read in ReadFailure::ALL {
                    for save in Stage::ALL
                        .into_iter()
                        .map(Failure::Before)
                        .chain(Cut::ALL.into_iter().map(Failure::Partial))
                    {
                        let first =
                            run_journal_read_save_crash(seed, scenario, read, save).unwrap();
                        assert_eq!(
                            first,
                            run_journal_read_save_crash(seed, scenario, read, save).unwrap(),
                            "seed={seed}, {}, {}, {}",
                            scenario.name(),
                            read.name(),
                            save.name()
                        );
                    }
                }
            }
        }
    }
    #[test]
    fn checker_rejects_resubmission_after_combined_save_crash() {
        use crate::intent_journal::{Cut, Failure, Stage};
        for scenario in Scenario::CLIENT_RESTARTS {
            for save in Stage::ALL
                .into_iter()
                .map(Failure::Before)
                .chain(Cut::ALL.into_iter().map(Failure::Partial))
            {
                let error = run_with_fault(
                    7,
                    scenario,
                    Faults {
                        journal_read_failure: Some(crate::intent_journal::ReadFailure::InputOutput),
                        journal_failure: Some(save),
                        journal_failure_at: 3,
                        combined_save_crash: true,
                        save_crash_republishes: true,
                        ..Faults::default()
                    },
                )
                .unwrap_err();
                assert!(
                    error.contains("save-crash recovery resubmitted original publication"),
                    "{error}"
                );
            }
        }
    }
    #[test]
    fn checker_rejects_promoting_temporary_intent_after_combined_save_crash() {
        for scenario in Scenario::CLIENT_RESTARTS {
            for save in [
                crate::intent_journal::Stage::Write.into(),
                crate::intent_journal::Failure::Partial(crate::intent_journal::Cut::Half),
            ] {
                let error = run_with_fault(
                    7,
                    scenario,
                    Faults {
                        journal_read_failure: Some(crate::intent_journal::ReadFailure::InputOutput),
                        journal_failure: Some(save),
                        journal_failure_at: 3,
                        combined_save_crash: true,
                        promote_temporary: true,
                        ..Faults::default()
                    },
                )
                .unwrap_err();
                assert!(
                    error.contains("save crash invalid durable intent")
                        || error.contains("save crash lost original durable intent identity"),
                    "{error}"
                );
            }
        }
    }
    #[test]
    fn combined_persistent_save_outages_replay() {
        use crate::intent_journal::{Cut, Failure, ReadFailure, Stage};
        for seed in 0..2 {
            for scenario in Scenario::CLIENT_RESTARTS {
                for read in ReadFailure::ALL {
                    for save in Stage::ALL
                        .into_iter()
                        .map(Failure::Before)
                        .chain(Cut::ALL.into_iter().map(Failure::Partial))
                    {
                        for early in [true, false] {
                            let first =
                                run_journal_read_save_outage(seed, scenario, read, save, early)
                                    .unwrap();
                            assert_eq!(
                                first,
                                run_journal_read_save_outage(seed, scenario, read, save, early)
                                    .unwrap(),
                                "seed={seed}, {}, {}, {}, early={early}",
                                scenario.name(),
                                read.name(),
                                save.name()
                            );
                        }
                    }
                }
            }
        }
    }
    #[test]
    fn checker_rejects_combined_save_outage_retry_without_explicit_repair() {
        for scenario in Scenario::CLIENT_RESTARTS {
            let error = run_with_fault(
                7,
                scenario,
                Faults {
                    journal_read_failure: Some(crate::intent_journal::ReadFailure::InputOutput),
                    journal_failure: Some(crate::intent_journal::Stage::Rename.into()),
                    journal_failure_at: 3,
                    persistent_journal: true,
                    combined_persistent_save: true,
                    retry_before_save_repair: true,
                    ..Faults::default()
                },
            )
            .unwrap_err();
            assert!(
                error.contains("persistent recovery save did not remain quiescent"),
                "{error}"
            );
        }
    }
    #[test]
    fn checker_rejects_combined_save_outage_fourth_attempt() {
        for scenario in Scenario::CLIENT_RESTARTS {
            let error = run_with_fault(
                7,
                scenario,
                Faults {
                    journal_read_failure: Some(crate::intent_journal::ReadFailure::InputOutput),
                    journal_failure: Some(crate::intent_journal::Stage::Rename.into()),
                    journal_failure_at: 3,
                    persistent_journal: true,
                    combined_persistent_save: true,
                    extra_journal_attempt: true,
                    ..Faults::default()
                },
            )
            .unwrap_err();
            assert!(
                error.contains("combined recovery save exceeded retry budget"),
                "{error}"
            );
        }
    }
    #[test]
    fn checker_rejects_combined_save_outage_remote_dispatch() {
        for scenario in Scenario::CLIENT_RESTARTS {
            for early in [true, false] {
                let error = run_with_fault(
                    7,
                    scenario,
                    Faults {
                        journal_read_failure: Some(crate::intent_journal::ReadFailure::InputOutput),
                        journal_failure: Some(crate::intent_journal::Stage::Rename.into()),
                        journal_failure_at: 3,
                        persistent_journal: true,
                        combined_persistent_save: true,
                        repair_within_budget: early,
                        recovery_save_republishes: true,
                        ..Faults::default()
                    },
                )
                .unwrap_err();
                assert!(
                    error.contains("recovery journal retry dispatched remote operation"),
                    "{error}"
                );
            }
        }
    }
    #[test]
    fn persistent_save_crash_corpus_replays() {
        use crate::intent_journal::{Cut, Failure, ReadFailure, Stage};
        for seed in 0..4 {
            for scenario in Scenario::CLIENT_RESTARTS {
                for read in ReadFailure::ALL {
                    for save in Stage::ALL
                        .into_iter()
                        .map(Failure::Before)
                        .chain(Cut::ALL.into_iter().map(Failure::Partial))
                    {
                        let first =
                            run_journal_read_save_outage_crash(seed, scenario, read, save).unwrap();
                        assert_eq!(
                            first,
                            run_journal_read_save_outage_crash(seed, scenario, read, save).unwrap(),
                            "seed={seed}, {}, {}, {}",
                            scenario.name(),
                            read.name(),
                            save.name()
                        );
                    }
                }
            }
        }
    }
    #[test]
    fn persistent_save_crash_rejects_fourth_attempt() {
        for scenario in Scenario::CLIENT_RESTARTS {
            let error = run_with_fault(
                7,
                scenario,
                Faults {
                    journal_read_failure: Some(crate::intent_journal::ReadFailure::InputOutput),
                    journal_failure: Some(crate::intent_journal::Stage::Rename.into()),
                    journal_failure_at: 3,
                    persistent_journal: true,
                    combined_persistent_save: true,
                    combined_save_crash: true,
                    extra_journal_attempt: true,
                    ..Faults::default()
                },
            )
            .unwrap_err();
            assert!(
                error.contains("persistent save crash did not follow exhausted retry budget"),
                "{error}"
            );
        }
    }
    #[test]
    fn persistent_save_crash_rejects_resubmission_after_repair() {
        for scenario in Scenario::CLIENT_RESTARTS {
            let error = run_with_fault(
                7,
                scenario,
                Faults {
                    journal_read_failure: Some(crate::intent_journal::ReadFailure::InputOutput),
                    journal_failure: Some(crate::intent_journal::Stage::Rename.into()),
                    journal_failure_at: 3,
                    persistent_journal: true,
                    combined_persistent_save: true,
                    combined_save_crash: true,
                    save_crash_republishes: true,
                    ..Faults::default()
                },
            )
            .unwrap_err();
            assert!(
                error.contains("save-crash recovery resubmitted original publication"),
                "{error}"
            );
        }
    }
    #[test]
    fn persistent_save_crash_rejects_temporary_promotion() {
        for scenario in Scenario::CLIENT_RESTARTS {
            for save in [
                crate::intent_journal::Stage::Write.into(),
                crate::intent_journal::Failure::Partial(crate::intent_journal::Cut::Half),
            ] {
                let error = run_with_fault(
                    7,
                    scenario,
                    Faults {
                        journal_read_failure: Some(crate::intent_journal::ReadFailure::InputOutput),
                        journal_failure: Some(save),
                        journal_failure_at: 3,
                        persistent_journal: true,
                        combined_persistent_save: true,
                        combined_save_crash: true,
                        promote_temporary: true,
                        ..Faults::default()
                    },
                )
                .unwrap_err();
                assert!(
                    error.contains("save crash invalid durable intent")
                        || error.contains("save crash lost original durable intent identity"),
                    "{error}"
                );
            }
        }
    }
    #[test]
    fn restarted_outage_corpus_replays() {
        use crate::intent_journal::{Cut, Failure, ReadFailure, Stage};
        for seed in 0..4 {
            for scenario in Scenario::CLIENT_RESTARTS {
                for read in ReadFailure::ALL {
                    for save in Stage::ALL
                        .into_iter()
                        .map(Failure::Before)
                        .chain(Cut::ALL.into_iter().map(Failure::Partial))
                    {
                        let first =
                            run_journal_restarted_outage(seed, scenario, read, save).unwrap();
                        assert_eq!(
                            first,
                            run_journal_restarted_outage(seed, scenario, read, save).unwrap(),
                            "seed={seed}, {}, {}, {}",
                            scenario.name(),
                            read.name(),
                            save.name()
                        );
                    }
                }
            }
        }
    }
    #[test]
    fn restarted_outage_rejects_extra_retry_or_implicit_repair() {
        for scenario in Scenario::CLIENT_RESTARTS {
            for extra in [true, false] {
                let error = run_with_fault(
                    7,
                    scenario,
                    Faults {
                        journal_read_failure: Some(crate::intent_journal::ReadFailure::InputOutput),
                        journal_failure: Some(crate::intent_journal::Stage::Rename.into()),
                        journal_failure_at: 3,
                        persistent_journal: true,
                        combined_persistent_save: true,
                        combined_save_crash: true,
                        restart_with_save_outage: true,
                        restart_extra_attempt: extra,
                        retry_before_save_repair: !extra,
                        ..Faults::default()
                    },
                )
                .unwrap_err();
                let expected = if extra {
                    "combined recovery save exceeded retry budget"
                } else {
                    "persistent recovery save did not remain quiescent"
                };
                assert!(error.contains(expected), "{error}");
            }
        }
    }
    #[test]
    fn restarted_outage_rejects_remote_resubmission() {
        for scenario in Scenario::CLIENT_RESTARTS {
            let error = run_with_fault(
                7,
                scenario,
                Faults {
                    journal_read_failure: Some(crate::intent_journal::ReadFailure::InputOutput),
                    journal_failure: Some(crate::intent_journal::Stage::Rename.into()),
                    journal_failure_at: 3,
                    persistent_journal: true,
                    combined_persistent_save: true,
                    combined_save_crash: true,
                    restart_with_save_outage: true,
                    save_crash_republishes: true,
                    ..Faults::default()
                },
            )
            .unwrap_err();
            assert!(
                error.contains("save-crash recovery resubmitted original publication"),
                "{error}"
            );
        }
    }
    #[test]
    fn client_restart_replays_journal_failures_at_each_transition() {
        for seed in 0..16 {
            for scenario in Scenario::CLIENT_RESTARTS {
                for stage in crate::intent_journal::Stage::ALL {
                    for write in 1..=3 {
                        let faults = Faults {
                            journal_failure: Some(stage.into()),
                            journal_failure_at: write,
                            ..Faults::default()
                        };
                        let first = run_with_fault(seed, scenario, faults).unwrap();
                        let second = run_with_fault(seed, scenario, faults).unwrap();
                        assert_eq!(
                            first,
                            second,
                            "seed={seed}, {}, {stage:?}, write={write}",
                            scenario.name()
                        );
                        assert_eq!(first.client_journal_failures, 1);
                        assert_eq!(first.client_intent_writes, 3);
                        assert_eq!(first.target_applications, 1);
                    }
                }
            }
        }
    }
    #[test]
    fn journal_error_crash_corpus_replays() {
        for seed in 0..16 {
            for scenario in Scenario::CLIENT_RESTARTS {
                for stage in crate::intent_journal::Stage::ALL {
                    for write in 1..=3 {
                        let first = run_journal_error_crash(seed, scenario, stage, write).unwrap();
                        let second = run_journal_error_crash(seed, scenario, stage, write).unwrap();
                        assert_eq!(
                            first,
                            second,
                            "seed={seed}, {}, {stage:?}, save={write}",
                            scenario.name()
                        );
                    }
                }
            }
        }
    }
    #[test]
    fn checker_rejects_reconstructing_intent_after_first_save_error() {
        let error = run_with_fault(
            7,
            Scenario::ClientRestartBeforeApply,
            Faults {
                journal_failure: Some(crate::intent_journal::Stage::Rename.into()),
                journal_failure_at: 1,
                crash_on_journal_error: true,
                restart_republishes: true,
                ..Faults::default()
            },
        )
        .unwrap_err();
        assert!(
            error.contains("client reconstructed a request after losing first intent"),
            "{error}"
        );
    }
    #[test]
    fn checker_rejects_resubmission_after_error_crash() {
        let error = run_with_fault(
            7,
            Scenario::ClientRestartAfterApply,
            Faults {
                journal_failure: Some(crate::intent_journal::Stage::Rename.into()),
                journal_failure_at: 2,
                crash_on_journal_error: true,
                restart_republishes: true,
                ..Faults::default()
            },
        )
        .unwrap_err();
        assert!(
            error.contains(
                "restarted client resubmitted or failed read-only recovery after journal error"
            ),
            "{error}"
        );
    }
    #[test]
    fn partial_write_retry_and_crash_corpus_replays() {
        for seed in 0..16 {
            for scenario in Scenario::CLIENT_RESTARTS {
                for cut in crate::intent_journal::Cut::ALL {
                    for write in 1..=3 {
                        for crash in [false, true] {
                            let first =
                                run_partial_write(seed, scenario, cut, write, crash).unwrap();
                            let second =
                                run_partial_write(seed, scenario, cut, write, crash).unwrap();
                            assert_eq!(
                                first,
                                second,
                                "seed={seed}, {}, {cut:?}, save={write}, crash={crash}",
                                scenario.name()
                            );
                            assert_eq!(first.client_journal_retries, usize::from(!crash));
                        }
                    }
                }
            }
        }
    }
    #[test]
    fn checker_rejects_promoting_truncated_temporary_intent() {
        let error = run_with_fault(
            7,
            Scenario::ClientRestartAfterApply,
            Faults {
                journal_failure: Some(crate::intent_journal::Failure::Partial(
                    crate::intent_journal::Cut::Half,
                )),
                journal_failure_at: 2,
                crash_on_journal_error: true,
                promote_temporary: true,
                ..Faults::default()
            },
        )
        .unwrap_err();
        assert!(
            error.contains("truncated temporary record replaced current intent"),
            "{error}"
        );
    }
    #[test]
    fn persistent_journal_outages_replay_repair_before_and_after_exhaustion() {
        use crate::intent_journal::{Cut, Failure, Stage};
        for seed in 0..16 {
            for scenario in Scenario::CLIENT_RESTARTS {
                for failure in Stage::ALL
                    .into_iter()
                    .map(Failure::Before)
                    .chain(Cut::ALL.into_iter().map(Failure::Partial))
                {
                    for save in 1..=3 {
                        for early in [true, false] {
                            let first =
                                run_persistent_journal(seed, scenario, failure, save, early)
                                    .unwrap();
                            let second =
                                run_persistent_journal(seed, scenario, failure, save, early)
                                    .unwrap();
                            assert_eq!(
                                first,
                                second,
                                "seed={seed}, {}, {}, save={save}, repair_within_budget={early}",
                                scenario.name(),
                                failure.name()
                            );
                            assert_eq!(first.client_journal_failures, if early { 1 } else { 3 });
                            assert_eq!(first.client_journal_retries, if early { 1 } else { 2 });
                            assert_eq!(first.client_journal_outage_quiescent, !early);
                        }
                    }
                }
            }
        }
    }
    #[test]
    fn checker_rejects_journal_attempt_beyond_budget() {
        let error = run_with_fault(
            7,
            Scenario::ClientRestartAfterApply,
            Faults {
                journal_failure: Some(crate::intent_journal::Stage::SyncDirectory.into()),
                journal_failure_at: 2,
                persistent_journal: true,
                crash_on_journal_error: true,
                extra_journal_attempt: true,
                ..Faults::default()
            },
        )
        .unwrap_err();
        assert!(
            error.contains("persistent outage did not exhaust exactly three attempts"),
            "{error}"
        );
    }
    #[test]
    fn checker_rejects_missing_marker() {
        let error = run_with_fault(
            7,
            Scenario::LostAck,
            Faults {
                omit_marker: true,
                ..Faults::default()
            },
        )
        .unwrap_err();
        assert!(
            error.contains("lost acknowledgement did not recover an explicit effect outcome"),
            "{error}"
        );
    }
    #[test]
    fn checker_rejects_unbound_request_identity() {
        let error = run_with_fault(
            7,
            Scenario::Reuse,
            Faults {
                ignore_identity: true,
                ..Faults::default()
            },
        )
        .unwrap_err();
        assert!(
            error.contains("operation ID reuse accepted another request"),
            "{error}"
        );
    }
    #[test]
    fn checker_rejects_definitive_failure_at_deadline() {
        let error = run_with_fault(
            7,
            Scenario::PartitionAfterApply,
            Faults {
                deadline_as_failure: true,
                ..Faults::default()
            },
        )
        .unwrap_err();
        assert!(
            error.contains("deadline falsely resolved operation"),
            "{error}"
        );
    }
    #[test]
    fn checker_rejects_queries_that_republish() {
        let error = run_with_fault(
            7,
            Scenario::PartitionBeforeApply,
            Faults {
                query_reapplies: true,
                ..Faults::default()
            },
        )
        .unwrap_err();
        assert!(
            error.contains("marker query changed an unresolved publication"),
            "{error}"
        );
    }
    #[test]
    fn checker_rejects_lost_client_intent() {
        let error = run_with_fault(
            7,
            Scenario::ClientRestartAfterApply,
            Faults {
                lose_client_intent: true,
                ..Faults::default()
            },
        )
        .unwrap_err();
        assert!(
            error.contains("client restart lost durable operation identity"),
            "{error}"
        );
    }
    #[test]
    fn checker_rejects_republishing_after_client_restart() {
        let error = run_with_fault(
            7,
            Scenario::ClientRestartBeforeApply,
            Faults {
                restart_republishes: true,
                ..Faults::default()
            },
        )
        .unwrap_err();
        assert!(
            error.contains("restarted client resubmitted an unresolved publication"),
            "{error}"
        );
    }
}
