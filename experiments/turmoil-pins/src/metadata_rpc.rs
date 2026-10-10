//! Commit envelopes cross simulated TCP; verified commands use a test-only registry.
use crate::repository_cases::{Log, event};
use casita::experimental::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Notify,
};
use turmoil::net::{TcpListener, TcpStream};

#[derive(Serialize, Deserialize)]
struct Request {
    id: u64,
    expected: String,
}
#[derive(Clone, Serialize, Deserialize)]
enum Response {
    Applied {
        revision: String,
        objects_inserted: usize,
        objects_removed: usize,
        roots_changed: usize,
    },
    Stale {
        expected: String,
        actual: String,
    },
    Failed(String),
}
impl Response {
    fn from_result(result: Result<CommitResult, MetadataError>) -> Self {
        match result {
            Ok(result) => Self::Applied {
                revision: result.revision.to_string(),
                objects_inserted: result.objects_inserted,
                objects_removed: result.objects_removed,
                roots_changed: result.roots_changed,
            },
            Err(MetadataError::StaleRevision { expected, actual }) => Self::Stale {
                expected: expected.to_string(),
                actual: actual.to_string(),
            },
            Err(error) => Self::Failed(error.to_string()),
        }
    }
    fn into_result(self) -> Result<CommitResult, MetadataError> {
        let parse = |revision: String| {
            revision
                .parse::<RepositoryRevision>()
                .map_err(|error| MetadataError::Backend(format!("invalid RPC revision: {error}")))
        };
        match self {
            Self::Applied {
                revision,
                objects_inserted,
                objects_removed,
                roots_changed,
            } => Ok(CommitResult {
                revision: parse(revision)?,
                objects_inserted,
                objects_removed,
                roots_changed,
            }),
            Self::Stale { expected, actual } => Err(MetadataError::StaleRevision {
                expected: parse(expected)?,
                actual: parse(actual)?,
            }),
            Self::Failed(error) => Err(MetadataError::Backend(error)),
        }
    }
}
struct Submitted {
    expected: RepositoryRevision,
    mutation: MetadataMutation,
    target: bool,
}
#[derive(Clone, Serialize, Deserialize)]
struct Completed {
    expected: String,
    result: Response,
}
#[derive(Default)]
struct State {
    pending: BTreeMap<u64, Submitted>,
    completed: BTreeMap<u64, Completed>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RestartStage {
    BeforeCache,
    AfterCache,
}
#[derive(Clone)]
pub(crate) struct Network {
    pub inner: MemoryMetadataStore,
    pub entered: Arc<Notify>,
    pub resume: Arc<Notify>,
    state: Arc<Mutex<State>>,
    next_id: Arc<AtomicU64>,
    journal: Arc<Mutex<BTreeMap<u64, Vec<u8>>>>,
    epochs: Arc<AtomicU64>,
    restart_requested: Arc<AtomicBool>,
    restart_stage: Option<RestartStage>,
    lose_journal: bool,
    pause_before: bool,
    lose_ack: bool,
    log: Log,
    seed: u64,
}
impl Network {
    pub fn new(
        inner: MemoryMetadataStore,
        log: Log,
        seed: u64,
        pause_before: bool,
        lose_ack: bool,
    ) -> Self {
        Self {
            inner,
            log,
            seed,
            pause_before,
            lose_ack,
            state: Arc::default(),
            next_id: Arc::new(AtomicU64::new(0)),
            journal: Arc::default(),
            epochs: Arc::new(AtomicU64::new(0)),
            restart_requested: Arc::new(AtomicBool::new(false)),
            restart_stage: None,
            lose_journal: false,
            entered: Arc::default(),
            resume: Arc::default(),
        }
    }
    pub fn with_restart(mut self, stage: RestartStage, lose_journal: bool) -> Self {
        self.restart_stage = Some(stage);
        self.lose_journal = lose_journal;
        self
    }
    pub fn request_restart(&self) {
        event(&self.log, "client requested metadata host restart");
        self.restart_requested.store(true, Ordering::SeqCst);
    }
    pub fn take_restart_request(&self) -> bool {
        self.restart_requested.swap(false, Ordering::SeqCst)
    }
    pub fn discard_volatile_cache(&self) -> usize {
        let mut state = self.state.lock().unwrap();
        let count = state.completed.len();
        state.completed.clear();
        count
    }
    pub async fn serve(self) -> crate::SimResult {
        let listener = TcpListener::bind("0.0.0.0:9100").await?;
        self.discard_volatile_cache();
        let epoch = self.epochs.fetch_add(1, Ordering::SeqCst) + 1;
        self.log.lock().unwrap().rpc_server_epochs = epoch as usize;
        event(
            &self.log,
            format!("metadata server epoch {epoch} listening"),
        );
        loop {
            let (socket, _) = listener.accept().await?;
            let server = self.clone();
            tokio::spawn(async move {
                if let Err(error) = server.handle(socket).await {
                    server.log.lock().unwrap().rpc_server_errors += 1;
                    event(&server.log, format!("RPC server error: {error}"));
                }
            });
        }
    }
    async fn handle(&self, mut socket: TcpStream) -> Result<(), MetadataError> {
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).await.map_err(transient)?;
        let request: Request = serde_json::from_slice(&bytes).map_err(transient)?;
        self.log.lock().unwrap().rpc_requests += 1;
        event(&self.log, format!("RPC received operation {}", request.id));
        let cached = {
            let state = self.state.lock().unwrap();
            state
                .completed
                .get(&request.id)
                .map(|cached| (cached.expected.clone(), cached.result.clone()))
        };
        let cached = if cached.is_some() {
            cached
        } else {
            let durable = self.journal.lock().unwrap().get(&request.id).cloned();
            if let Some(bytes) = durable {
                let completed: Completed = serde_json::from_slice(&bytes).map_err(transient)?;
                self.log.lock().unwrap().rpc_journal_recoveries += 1;
                event(
                    &self.log,
                    format!("RPC rebuilt operation {} from result journal", request.id),
                );
                self.state
                    .lock()
                    .unwrap()
                    .completed
                    .insert(request.id, completed.clone());
                Some((completed.expected, completed.result))
            } else {
                None
            }
        };
        let response = if let Some((expected, result)) = cached {
            if expected != request.expected {
                return Err(MetadataError::Backend(
                    "operation ID reused with another revision".into(),
                ));
            }
            self.log.lock().unwrap().rpc_duplicate_replies += 1;
            event(
                &self.log,
                format!("RPC recovered cached operation {}", request.id),
            );
            result
        } else {
            let submitted = self
                .state
                .lock()
                .unwrap()
                .pending
                .remove(&request.id)
                .ok_or_else(|| MetadataError::Backend("unknown RPC operation".into()))?;
            if submitted.expected.to_string() != request.expected {
                return Err(MetadataError::Backend(
                    "RPC revision differs from submitted command".into(),
                ));
            }
            // Ownership transfers only after the request arrives. This handler
            // owns the verified mutation even if its connection loses the reply.
            if submitted.target {
                event(&self.log, "network worker owns submitted mutation");
                if self.pause_before {
                    self.entered.notify_one();
                    self.resume.notified().await;
                }
            }
            tokio::time::sleep(Duration::from_millis(1 + self.seed % 17)).await;
            let result = self
                .inner
                .commit(&submitted.expected, submitted.mutation)
                .await;
            if submitted.target {
                self.log.lock().unwrap().rpc_target_applications += usize::from(result.is_ok());
                event(
                    &self.log,
                    format!("network worker applied commit: {}", result.is_ok()),
                );
            }
            let response = Response::from_result(result);
            let completed = Completed {
                expected: request.expected,
                result: response.clone(),
            };
            if submitted.target && self.restart_stage.is_some() {
                self.log.lock().unwrap().rpc_original_result =
                    serde_json::to_string(&response).map_err(transient)?;
            }
            // The positive durability model publishes metadata and its serialized
            // reply without a suspension point between them. This is a modeled
            // atomic boundary, not a filesystem transaction implementation.
            if !(submitted.target && self.lose_journal) {
                self.journal.lock().unwrap().insert(
                    request.id,
                    serde_json::to_vec(&completed).map_err(transient)?,
                );
            }
            if submitted.target && self.restart_stage == Some(RestartStage::BeforeCache) {
                event(
                    &self.log,
                    "server paused before volatile result cache population",
                );
                self.entered.notify_one();
                self.resume.notified().await;
            }
            self.state
                .lock()
                .unwrap()
                .completed
                .insert(request.id, completed);
            if submitted.target
                && !self.pause_before
                && self.restart_stage != Some(RestartStage::BeforeCache)
            {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            if submitted.target && self.lose_ack {
                self.log.lock().unwrap().rpc_lost_acknowledgements += 1;
                event(
                    &self.log,
                    "network worker dropped socket after commit application",
                );
                // Actual EOF replaces the response; the client must recover the
                // same operation through a new TCP connection.
                return Ok(());
            }
            response
        };
        let bytes = serde_json::to_vec(&response).map_err(transient)?;
        socket.write_all(&bytes).await.map_err(transient)?;
        socket.shutdown().await.map_err(transient)?;
        event(&self.log, format!("RPC replied operation {}", request.id));
        Ok(())
    }
    pub async fn commit(
        &self,
        expected: &RepositoryRevision,
        mutation: MetadataMutation,
        target: bool,
    ) -> Result<CommitResult, MetadataError> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.state.lock().unwrap().pending.insert(
            id,
            Submitted {
                expected: *expected,
                mutation,
                target,
            },
        );
        let request = serde_json::to_vec(&Request {
            id,
            expected: expected.to_string(),
        })
        .map_err(transient)?;
        for attempt in 0..2 {
            event(
                &self.log,
                format!("RPC sending operation {id}, attempt {attempt}"),
            );
            match self.exchange(&request).await {
                Ok(response) => {
                    if target && self.restart_stage.is_some() {
                        self.log.lock().unwrap().rpc_recovered_result =
                            serde_json::to_string(&response).map_err(transient)?;
                    }
                    return response.into_result();
                }
                Err(error) => {
                    self.log.lock().unwrap().rpc_transport_errors += 1;
                    event(
                        &self.log,
                        format!("RPC transport failed for operation {id}"),
                    );
                    if attempt == 1 {
                        return Err(error);
                    }
                    // Only the affected commit waits for the restarted listener.
                    // This is simulated time; no wall-clock readiness races.
                    if target && self.restart_stage.is_some() {
                        while self.epochs.load(Ordering::SeqCst) < 2 {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    }
                }
            }
        }
        unreachable!()
    }
    async fn exchange(&self, request: &[u8]) -> Result<Response, MetadataError> {
        let mut socket = TcpStream::connect("metadata:9100")
            .await
            .map_err(transient)?;
        socket.write_all(request).await.map_err(transient)?;
        socket.shutdown().await.map_err(transient)?;
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).await.map_err(transient)?;
        if bytes.is_empty() {
            return Err(MetadataError::Transient(
                "metadata acknowledgement lost at EOF".into(),
            ));
        }
        serde_json::from_slice(&bytes).map_err(transient)
    }
    pub fn pending_commands(&self) -> usize {
        self.state.lock().unwrap().pending.len()
    }
}
fn transient(error: impl std::fmt::Display) -> MetadataError {
    MetadataError::Transient(error.to_string())
}
