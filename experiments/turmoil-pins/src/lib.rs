//! A bounded Turmoil spike exercising the production ObjectPinStore over TCP.
//! The server models atomic conditional writes, not S3's HTTP protocol.
#![forbid(unsafe_code)]
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use casita::experimental::{
    DataPin, ObjectPinStore, PinResource, PinScope, PinStore, object_store,
};
use futures::{stream, stream::BoxStream};
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    path::Path,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use turmoil::net::{TcpListener, TcpStream};

mod entropy;
use entropy::SeededEntropy;
pub mod backend_crash;
pub mod backend_marker;
pub mod chunk_cases;
pub mod client_journal;
pub mod fencing;
pub mod harness;
pub mod intent_journal;
pub mod journal_writers;
pub mod marker_rpc;
mod metadata_rpc;
pub mod repository_cases;
pub mod retention;

const LEDGER: &str = "pins/inventory";
type SimResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scenario {
    ConcurrentPins,
    LostResponse,
    PinVsDeletion,
    CrashedOwner,
}
impl Scenario {
    pub const ALL: [Self; 4] = [
        Self::ConcurrentPins,
        Self::LostResponse,
        Self::PinVsDeletion,
        Self::CrashedOwner,
    ];
    pub fn name(self) -> &'static str {
        match self {
            Self::ConcurrentPins => "concurrent-pins",
            Self::LostResponse => "lost-response",
            Self::PinVsDeletion => "pin-vs-deletion",
            Self::CrashedOwner => "crashed-owner",
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct Report {
    pub trace: Vec<String>,
    pub event_times_ns: Vec<u128>,
    pub accepted_writes: u64,
    pub conflicts: u64,
    pub lost_responses: u64,
    pub pins: usize,
    pub deletions: usize,
    pub writer_admitted: bool,
    pub deletion_admitted: bool,
    // Intentionally retained to expose OS entropy escaping the simulation seed.
    pub ledger_bytes: Vec<u8>,
}
impl Report {
    /// Normalize the ledger bytes for the OS-entropy comparison configuration.
    pub fn semantic_replay(&self) -> Self {
        let mut report = self.clone();
        report.ledger_bytes.clear();
        report
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Faults {
    /// Negative control: violates storage CAS to demonstrate a detectable lost update.
    pub ignore_preconditions: bool,
}

#[derive(Debug, Serialize, Deserialize)]
enum Request {
    Get {
        actor: String,
        path: String,
    },
    Put {
        actor: String,
        path: String,
        data: Vec<u8>,
        mode: Mode,
    },
}
#[derive(Debug, Serialize, Deserialize)]
enum Mode {
    Create,
    Update(String),
    Overwrite,
}
#[derive(Debug, Serialize, Deserialize)]
struct Response {
    code: u16,
    data: Vec<u8>,
    version: u64,
}

#[derive(Default)]
struct State {
    objects: BTreeMap<String, (Vec<u8>, u64)>,
    report: Report,
    gets: usize,
    completed: usize,
    writer_ready: bool,
    crashed: bool,
    tokens: Vec<casita::experimental::PinToken>,
}
type Shared = Arc<Mutex<State>>;

fn resources(name: &str) -> BTreeSet<PinResource> {
    BTreeSet::from([PinResource::StorageObject(name.into())])
}
fn pin(name: &str) -> DataPin {
    DataPin {
        scope: PinScope::Staging,
        catalog: None,
        resources: resources(name),
    }
}
fn ledger(actor: &str, seed: u64) -> ObjectPinStore {
    #[cfg(feature = "explicit-entropy")]
    {
        ObjectPinStore::new_with_entropy(
            Arc::new(Remote {
                actor: actor.into(),
            }),
            LEDGER.into(),
            Arc::new(SeededEntropy::new(seed, actor)),
        )
    }
    #[cfg(not(feature = "explicit-entropy"))]
    {
        let _ = seed;
        ObjectPinStore::new(
            Arc::new(Remote {
                actor: actor.into(),
            }),
            LEDGER.into(),
        )
    }
}
fn transport(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> object_store::Error {
    object_store::Error::Generic {
        store: "turmoil-spike",
        source: error.into(),
    }
}
fn unsupported(operation: &str) -> object_store::Error {
    object_store::Error::NotImplemented {
        operation: operation.into(),
        implementer: "turmoil-spike".into(),
    }
}

#[derive(Debug)]
struct Remote {
    actor: String,
}
impl fmt::Display for Remote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "simulated-store/{}", self.actor)
    }
}
impl Remote {
    async fn request(&self, request: Request, path: &Path) -> object_store::Result<Response> {
        let mut socket = TcpStream::connect("store:9000").await.map_err(transport)?;
        let bytes = serde_json::to_vec(&request).map_err(transport)?;
        socket.write_all(&bytes).await.map_err(transport)?;
        socket.shutdown().await.map_err(transport)?;
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).await.map_err(transport)?;
        if bytes.is_empty() {
            return Err(transport(std::io::Error::other("committed response lost")));
        }
        let response: Response = serde_json::from_slice(&bytes).map_err(transport)?;
        match response.code {
            200 => Ok(response),
            404 => Err(object_store::Error::NotFound {
                path: path.to_string(),
                source: "missing object".into(),
            }),
            409 => Err(object_store::Error::AlreadyExists {
                path: path.to_string(),
                source: "create conflict".into(),
            }),
            412 => Err(object_store::Error::Precondition {
                path: path.to_string(),
                source: "version conflict".into(),
            }),
            code => Err(transport(format!("unexpected response {code}"))),
        }
    }
}

#[async_trait]
impl ObjectStore for Remote {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let mode = match options.mode {
            PutMode::Create => Mode::Create,
            PutMode::Update(version) => Mode::Update(
                version
                    .e_tag
                    .ok_or_else(|| unsupported("version without ETag"))?,
            ),
            PutMode::Overwrite => Mode::Overwrite,
        };
        let data = payload
            .iter()
            .flat_map(|part| part.iter().copied())
            .collect();
        let response = self
            .request(
                Request::Put {
                    actor: self.actor.clone(),
                    path: path.to_string(),
                    data,
                    mode,
                },
                path,
            )
            .await?;
        Ok(PutResult {
            e_tag: Some(response.version.to_string()),
            version: None,
            extensions: Default::default(),
        })
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        // This intentionally narrow adapter must fail instead of silently bypassing an option.
        if options.range.is_some()
            || options.head
            || options.if_match.is_some()
            || options.if_none_match.is_some()
            || options.if_modified_since.is_some()
            || options.if_unmodified_since.is_some()
            || options.version.is_some()
        {
            return Err(unsupported("GET options"));
        }
        let response = self
            .request(
                Request::Get {
                    actor: self.actor.clone(),
                    path: path.to_string(),
                },
                path,
            )
            .await?;
        let data = Bytes::from(response.data);
        let size = data.len() as u64;
        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(stream::once(async move { Ok(data) }))),
            meta: ObjectMeta {
                location: path.clone(),
                last_modified: chrono::DateTime::UNIX_EPOCH,
                size,
                e_tag: Some(response.version.to_string()),
                version: None,
            },
            range: 0..size,
            attributes: Attributes::default(),
            extensions: Default::default(),
        })
    }
    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(unsupported("multipart"))
    }
    fn delete_stream(
        &self,
        _: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        Box::pin(stream::once(async { Err(unsupported("delete")) }))
    }
    fn list(&self, _: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        Box::pin(stream::once(async { Err(unsupported("list")) }))
    }
    async fn list_with_delimiter(&self, _: Option<&Path>) -> object_store::Result<ListResult> {
        Err(unsupported("list"))
    }
    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> object_store::Result<()> {
        Err(unsupported("copy"))
    }
}

async fn serve(state: Shared, scenario: Scenario, faults: Faults) -> SimResult {
    let listener = TcpListener::bind("0.0.0.0:9000").await?;
    // Force the concurrent writers to read the same initial version, so CAS is exercised on every seed.
    let first_reads = Arc::new(tokio::sync::Barrier::new(2));
    loop {
        let (mut socket, _) = listener.accept().await?;
        let state = state.clone();
        let first_reads = first_reads.clone();
        tokio::spawn(async move {
            let mut bytes = Vec::new();
            socket.read_to_end(&mut bytes).await.unwrap();
            let request: Request = serde_json::from_slice(&bytes).unwrap();
            let at = turmoil::sim_elapsed().unwrap().as_nanos();
            let (response, gate, lose) = {
                let mut state = state.lock().unwrap();
                state.report.event_times_ns.push(at);
                match request {
                    Request::Get { actor, path } => {
                        let object = state.objects.get(&path).cloned();
                        let response = match object {
                            Some((data, version)) => Response {
                                code: 200,
                                data,
                                version,
                            },
                            None => Response {
                                code: 404,
                                data: vec![],
                                version: 0,
                            },
                        };
                        state.report.trace.push(format!(
                            "{actor}:get:{}:{}",
                            response.code, response.version
                        ));
                        state.gets += 1;
                        (
                            response,
                            scenario == Scenario::ConcurrentPins && state.gets <= 2,
                            false,
                        )
                    }
                    Request::Put {
                        actor,
                        path,
                        data,
                        mode,
                    } => {
                        let old = state.objects.get(&path);
                        let conflict = match mode {
                            Mode::Create if old.is_some() => 409,
                            Mode::Update(expected)
                                if old
                                    .is_none_or(|(_, version)| version.to_string() != expected) =>
                            {
                                412
                            }
                            _ => 0,
                        };
                        if conflict != 0 && !faults.ignore_preconditions {
                            state.report.conflicts += 1;
                            state.report.trace.push(format!("{actor}:put:{conflict}"));
                            (
                                Response {
                                    code: conflict,
                                    data: vec![],
                                    version: 0,
                                },
                                false,
                                false,
                            )
                        } else {
                            let version = old.map_or(1, |(_, version)| version + 1);
                            state.objects.insert(path, (data, version));
                            state.report.accepted_writes += 1;
                            state
                                .report
                                .trace
                                .push(format!("{actor}:put:200:{version}"));
                            let lose = scenario == Scenario::LostResponse
                                && state.report.lost_responses == 0;
                            if lose {
                                state.report.lost_responses += 1;
                                state.report.trace.push(format!("{actor}:response-lost"));
                                state.report.event_times_ns.push(at);
                            }
                            (
                                Response {
                                    code: 200,
                                    data: vec![],
                                    version,
                                },
                                false,
                                lose,
                            )
                        }
                    }
                }
            };
            if gate {
                first_reads.wait().await;
            }
            if !lose {
                socket
                    .write_all(&serde_json::to_vec(&response).unwrap())
                    .await
                    .unwrap();
            }
            socket.shutdown().await.unwrap();
        });
    }
}

/// Returns a checker failure rather than panicking, so negative controls can assert its verdict.
pub fn run(seed: u64, scenario: Scenario, faults: Faults) -> Result<Report, String> {
    let state = Shared::default();
    let mut sim = crate::harness::simulation(seed, 20, 10);
    let server = state.clone();
    sim.host("store", move || serve(server.clone(), scenario, faults));

    for (index, actor) in ["writer", "peer"].into_iter().enumerate() {
        if index == 1 && matches!(scenario, Scenario::LostResponse | Scenario::CrashedOwner) {
            continue;
        }
        let shared = state.clone();
        let task = move || {
            let shared = shared.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                let store = ledger(actor, seed);
                if scenario == Scenario::PinVsDeletion && index == 1 {
                    let inventory = store.inventory().await?;
                    let claim = store
                        .claim_deletions(inventory.revision, resources("shared"))
                        .await?;
                    let mut state = shared.lock().unwrap();
                    state.report.deletion_admitted = claim.is_some();
                    if let Some(token) = claim {
                        state.tokens.push(token);
                    }
                    state.completed += 1;
                } else {
                    let resource = if scenario == Scenario::ConcurrentPins {
                        actor
                    } else {
                        "shared"
                    };
                    let token = store.register(pin(resource)).await?;
                    {
                        let mut state = shared.lock().unwrap();
                        if index == 0 {
                            state.report.writer_admitted = token.is_some();
                        }
                        if let Some(token) = token {
                            state.tokens.push(token);
                        }
                        state.completed += 1;
                        state.writer_ready = true;
                    }
                    if scenario == Scenario::CrashedOwner {
                        // No DataPinLease is used: this tests a durable ledger owner's death,
                        // not suppression of Rust Drop or outstanding request completion.
                        std::future::pending::<()>().await;
                    }
                }
                Ok(()) as SimResult
            }
        };
        if scenario == Scenario::CrashedOwner {
            sim.host(actor, task);
        } else {
            sim.client(actor, task());
        }
    }

    let observer = state.clone();
    sim.client("observer", async move {
        let needed = if matches!(scenario, Scenario::LostResponse | Scenario::CrashedOwner) {
            1
        } else {
            2
        };
        loop {
            let done = {
                let state = observer.lock().unwrap();
                state.completed == needed && (scenario != Scenario::CrashedOwner || state.crashed)
            };
            if done {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        if scenario == Scenario::CrashedOwner {
            // Durable pins do not expire just because virtual time advances.
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let store = ledger("observer", seed);
        let inventory = store.inventory().await?;
        if scenario == Scenario::CrashedOwner {
            let claim = store
                .claim_deletions(inventory.revision, resources("shared"))
                .await?;
            if claim.is_some() {
                return Err("crashed owner's durable pin was ignored".into());
            }
        }
        let mut state = observer.lock().unwrap();
        state.report.pins = inventory.pins.len();
        state.report.deletions = inventory.deletions.len();
        // Check every acknowledged token independently of the memory ledger implementation.
        if !state.tokens.iter().all(|token| {
            inventory.pins.contains_key(token) || inventory.deletions.contains_key(token)
        }) {
            return Err("acknowledged ownership disappeared".into());
        }
        Ok(())
    });

    if scenario == Scenario::CrashedOwner {
        while !state.lock().unwrap().writer_ready {
            if sim
                .step()
                .map_err(|error| format!("seed={seed}, {}: {error}", scenario.name()))?
            {
                return Err("simulation finished before writer admission".into());
            }
        }
        sim.crash("writer");
        let mut state = state.lock().unwrap();
        state.crashed = true;
        state.report.trace.push("writer:crashed".into());
        state.report.event_times_ns.push(sim.elapsed().as_nanos());
    }
    sim.run().map_err(|error| {
        format!(
            "seed={seed}, {}: {error}; trace={:?}",
            scenario.name(),
            state.lock().unwrap().report.trace
        )
    })?;
    let mut state = state.lock().unwrap();
    state.report.ledger_bytes = state
        .objects
        .get(LEDGER)
        .map(|(bytes, _)| bytes.clone())
        .unwrap_or_default();
    let report = state.report.clone();
    let valid = match scenario {
        Scenario::ConcurrentPins => {
            report.pins == 2 && report.deletions == 0 && report.conflicts > 0
        }
        Scenario::LostResponse => {
            report.pins == 1 && report.accepted_writes == 1 && report.lost_responses == 1
        }
        Scenario::PinVsDeletion => {
            report.writer_admitted != report.deletion_admitted
                && report.pins + report.deletions == 1
        }
        Scenario::CrashedOwner => report.pins == 1 && report.deletions == 0,
    };
    if !valid {
        return Err(format!(
            "seed={seed}, {}: checker rejected {report:?}",
            scenario.name()
        ));
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn seeded_corpus_and_semantic_replay() {
        let mut race_outcomes = [0; 2];
        for seed in 0..64 {
            for scenario in Scenario::ALL {
                let first = run(seed, scenario, Faults::default()).unwrap();
                let second = run(seed, scenario, Faults::default()).unwrap();
                if scenario == Scenario::PinVsDeletion {
                    race_outcomes[usize::from(first.deletion_admitted)] += 1;
                }
                assert_eq!(first.trace.len(), first.event_times_ns.len());
                #[cfg(feature = "explicit-entropy")]
                assert_eq!(
                    first,
                    second,
                    "full replay: seed={seed}, {}",
                    scenario.name()
                );
                assert_eq!(
                    first.semantic_replay(),
                    second.semantic_replay(),
                    "seed={seed}, {}",
                    scenario.name()
                );
            }
        }
        assert!(
            race_outcomes.iter().all(|count| *count > 0),
            "missing race outcome: {race_outcomes:?}"
        );
    }
    #[test]
    fn checker_rejects_broken_conditional_store() {
        let verdict = run(
            0,
            Scenario::ConcurrentPins,
            Faults {
                ignore_preconditions: true,
            },
        );
        let error = verdict.expect_err("checker accepted a lost update");
        assert!(
            error.contains("acknowledged ownership disappeared"),
            "unexpected negative-control failure: {error}"
        );
    }
    #[test]
    fn measure_entropy_control() {
        let first = run(7, Scenario::LostResponse, Faults::default()).unwrap();
        let second = run(7, Scenario::LostResponse, Faults::default()).unwrap();
        assert_eq!(first.semantic_replay(), second.semantic_replay());
        #[cfg(not(feature = "explicit-entropy"))]
        assert_ne!(
            first.ledger_bytes, second.ledger_bytes,
            "reassess entropy finding if token generation changes"
        );
        #[cfg(feature = "explicit-entropy")]
        assert_eq!(
            first.ledger_bytes, second.ledger_bytes,
            "entropy control did not reproduce pin tokens"
        );
    }
}
