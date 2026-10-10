//! Client intent is serialized outside the host. No receipt survives in daemon memory.
use super::*;

use crate::intent_journal::{self, MemoryStorage, Phase, Stage, Storage};
type Intent = intent_journal::Intent<Request>;
mod error_crash;
mod read_failure;
mod save_crash;
mod save_outage;
#[derive(Clone, Default)]
struct ClientState {
    durable: Arc<Mutex<MemoryStorage>>,
    save_exhausted: Arc<Notify>,
    save_repaired: Arc<Notify>,
    save_crash_requested: Arc<AtomicBool>,
    save_crashed: Arc<Notify>,
    save_requests: Arc<AtomicUsize>,
    save_queries: Arc<AtomicUsize>,
    save_revision: Arc<Mutex<String>>,
    read_failed: Arc<Notify>,
    read_repaired: Arc<AtomicBool>,
    read_crash_requested: Arc<AtomicBool>,
    journal_fault_used: Arc<AtomicBool>,
    outage_active: Arc<AtomicBool>,
    first_storage_error: Arc<Notify>,
    epoch: Arc<AtomicUsize>,
    crash_requested: Arc<AtomicBool>,
    bounce_requested: Arc<AtomicBool>,
    crashed: Arc<Notify>,
    queried: Arc<Notify>,
    continue_recovery: Arc<Notify>,
    finished: Arc<Notify>,
    setup_ready: Arc<Notify>,
}
// Each synchronous storage operation releases its lock before retry sleeps.
struct SharedMemory(Arc<Mutex<MemoryStorage>>);
impl Storage for SharedMemory {
    fn read(&self) -> std::io::Result<Option<Vec<u8>>> {
        self.0.lock().unwrap().read()
    }
    fn temporary(&self) -> std::io::Result<Option<Vec<u8>>> {
        self.0.lock().unwrap().temporary()
    }
    fn apply(&mut self, stage: Stage, bytes: &[u8]) -> std::io::Result<()> {
        self.0.lock().unwrap().apply(stage, bytes)
    }
}
impl ClientState {
    fn save(&self, server: &Server, intent: &Intent) -> Result<bool, Box<dyn std::error::Error>> {
        let write = server.log.lock().unwrap().client_intent_writes + 1;
        let fail = server.faults.journal_failure.filter(|_| {
            write == server.faults.journal_failure_at
                && !self.journal_fault_used.swap(true, Ordering::SeqCst)
        });
        let mut storage = self.durable.lock().unwrap();
        let previous = storage.read()?;
        let mut injected = intent_journal::Injected {
            storage: &mut *storage,
            fail,
        };
        if let Err(error) = intent_journal::persist(&mut injected, intent, |_| Ok(())) {
            if fail.is_none() {
                return Err(error);
            }
            let stage = fail.unwrap();
            server.log.lock().unwrap().client_journal_failures += 1;
            if server.faults.crash_on_journal_error || server.faults.combined_save_crash {
                server.event(format!(
                    "client journal {stage:?} failed on save {write}; crash before reload or retry"
                ));
                return Ok(false);
            }
            server.event(format!(
                "client journal {stage:?} failed on save {write}; reload before retry"
            ));
            if server.faults.promote_temporary {
                // Negative control promotes a valid alternate intent after read repair.
                injected.apply(Stage::Rename, &[])?;
            }
            let visible = injected.read()?;
            if stage.replacement_visible() {
                let loaded: Intent = intent_journal::load(&injected)?;
                if loaded.phase != intent.phase || loaded.fingerprint != intent.fingerprint {
                    return Err("journal directory sync error lost replacement identity".into());
                }
            } else if visible != previous {
                return Err("journal failure before rename changed durable intent".into());
            }
            if write == 1 && server.log.lock().unwrap().target_applications != 0 {
                return Err("client dispatched before intent persistence succeeded".into());
            }
            if let intent_journal::Failure::Partial(cut) = stage {
                let expected = serde_json::to_vec(intent)?;
                let len = intent_journal::verify_partial(&injected, cut, &expected)?;
                let mut log = server.log.lock().unwrap();
                log.client_journal_partial_len = len;
                log.client_journal_partial_full_len = expected.len();
                log.client_journal_partial_proven = true;
            }
            // Retrying this local write never retransmits the remote operation.
            server.log.lock().unwrap().client_journal_retries += 1;
            intent_journal::persist(&mut injected, intent, |_| Ok(()))?;
        }
        let bytes = injected.read()?.ok_or("saved client intent missing")?;
        server.log.lock().unwrap().client_intent_bytes = String::from_utf8(bytes)?;
        server.log.lock().unwrap().client_intent_writes += 1;
        Ok(true)
    }
    async fn save_for(
        &self,
        server: &Server,
        intent: &Intent,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        if !server.faults.persistent_journal {
            return self.save(server, intent);
        }
        let write = server.log.lock().unwrap().client_intent_writes + 1;
        if write == server.faults.journal_failure_at
            && !self.journal_fault_used.swap(true, Ordering::SeqCst)
        {
            self.outage_active.store(true, Ordering::SeqCst);
        }
        let failure = server.faults.journal_failure.unwrap();
        let mut storage = intent_journal::Outage {
            storage: SharedMemory(self.durable.clone()),
            failure,
            active: self.outage_active.clone(),
        };
        let previous = storage.read()?;
        let expected = serde_json::to_vec(intent)?;
        let mut policy = intent_journal::RetryPolicy::default();
        if server.faults.extra_journal_attempt
            || (server.faults.restart_extra_attempt && self.epoch.load(Ordering::SeqCst) >= 6)
        {
            policy.attempts += 1;
        }
        let stats =
            intent_journal::persist_bounded(&mut storage, intent, policy, |attempt, storage| {
                server.log.lock().unwrap().client_journal_failures += 1;
                server.event(format!(
                    "journal save {write} failed on bounded attempt {attempt}"
                ));
                if failure.replacement_visible() {
                    if storage.read()?.as_ref() != Some(&expected) {
                        return Err("outage lost replacement after directory sync error".into());
                    }
                } else if storage.read()? != previous {
                    return Err("outage before rename changed current intent".into());
                }
                if let intent_journal::Failure::Partial(cut) = failure {
                    let len = intent_journal::verify_partial(storage, cut, &expected)?;
                    let mut log = server.log.lock().unwrap();
                    log.client_journal_partial_len = len;
                    log.client_journal_partial_full_len = expected.len();
                    log.client_journal_partial_proven = true;
                }
                if write == 1 && server.log.lock().unwrap().target_applications != 0 {
                    return Err("publication dispatched during initial storage outage".into());
                }
                self.first_storage_error.notify_one();
                Ok(())
            })
            .await?;
        let saved = stats.saved;
        {
            let mut log = server.log.lock().unwrap();
            log.client_journal_retries += stats.attempts - 1;
            log.client_journal_retry_exhaustions += usize::from(!saved);
            log.client_journal_batches.push(stats);
        }
        if !saved {
            server.event(
                "journal retry budget exhausted; no further attempt until explicit recovery",
            );
            return Ok(false);
        }
        server.log.lock().unwrap().client_intent_bytes =
            String::from_utf8(storage.read()?.ok_or("bounded save lost current intent")?)?;
        server.log.lock().unwrap().client_intent_writes += 1;
        Ok(true)
    }
    async fn boot(self, server: Server) -> crate::SimResult {
        let epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
        server.log.lock().unwrap().client_epochs = epoch;
        server.event(format!("client epoch {epoch} booted"));
        if epoch == 1 {
            self.setup_ready.notified().await;
            let request = Request::fixture(0);
            let mut intent = Intent::new(request)?;
            if !self.save_for(&server, &intent).await? {
                return Err("journal exhausted before storage was repaired".into());
            }
            server.event("client saved intent before sending publication");
            let outcome = server.bounded(&intent.request, false).await;
            require_unknown(&outcome, &intent.request)?;
            intent.mark_unknown()?;
            if !self.save_for(&server, &intent).await? {
                return Err("journal exhausted before storage was repaired".into());
            }
            server.event("client saved unknown outcome before requesting crash");
            self.crash_requested.store(true, Ordering::SeqCst);
            std::future::pending::<()>().await;
        }
        // The fresh boot has no captured request or response. Only serialized
        // intent bytes cross the client-host crash boundary.
        read_failure::validate_or_park(&self, &server).await?;
        if self.durable.lock().unwrap().read()?.is_none() {
            return Err("client restart lost durable operation identity".into());
        }
        let mut intent: Intent = intent_journal::load(&*self.durable.lock().unwrap())?;
        if server.faults.combined_save_crash && epoch == 6 {
            return save_crash::recover(&self, &server, intent).await;
        }
        if !matches!(intent.phase, Phase::Unknown)
            || intent.fingerprint != intent.request.fingerprint()
        {
            return Err("client restart loaded inconsistent operation identity".into());
        }
        server.log.lock().unwrap().client_intent_recoveries += 1;
        server.event("fresh client deserialized unknown intent and queries original ID");
        let first = server
            .bounded(&intent.request, !server.faults.restart_republishes)
            .await;
        let response = if server.scenario.pause_before() {
            if !matches!(first, ClientOutcome::Unknown { .. }) {
                return Err("restarted client resubmitted an unresolved publication".into());
            }
            require_unknown(&first, &intent.request)?;
            self.queried.notify_one();
            self.continue_recovery.notified().await;
            server.bounded(&intent.request, true).await
        } else {
            first
        };
        let observed_revision = match response {
            ClientOutcome::Known(Response::RecoveredEffect {
                request_fingerprint,
                observed_revision,
            }) if request_fingerprint == intent.fingerprint => observed_revision,
            _ => return Err("restarted client failed to recover the original effect".into()),
        };
        server.log.lock().unwrap().recovery_revision = observed_revision.clone();
        intent.mark_recovered(&intent.fingerprint.clone(), observed_revision)?;
        if !read_failure::save_recovered(&self, &server, &intent).await? {
            return Err("journal exhausted before storage was repaired".into());
        }
        server.event("fresh client saved recovered effect without resubmitting");
        self.finished.notify_one();
        Ok(())
    }
}

pub(super) fn run(seed: u64, scenario: Scenario, faults: Faults) -> Result<Report, String> {
    if faults.crash_on_journal_error {
        return error_crash::run(seed, scenario, faults);
    }
    let server = make_server(seed, scenario, faults)?;
    let state = ClientState::default();
    let mut sim = turmoil::Builder::new()
        .rng_seed(seed)
        .enable_random_order()
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(10))
        .simulation_duration(Duration::from_secs(30))
        .build();
    let metadata = server.clone();
    sim.host("metadata", move || metadata.clone().serve());
    let client_server = server.clone();
    let client_state = state.clone();
    sim.host("client", move || {
        client_state.clone().boot(client_server.clone())
    });
    if faults.persistent_journal && faults.repair_within_budget {
        let repair_state = state.clone();
        let repair_server = server.clone();
        sim.client("disk-repair", async move {
            repair_state.first_storage_error.notified().await;
            tokio::time::sleep(Duration::from_millis(1)).await;
            repair_state.outage_active.store(false, Ordering::SeqCst);
            repair_server
                .log
                .lock()
                .unwrap()
                .client_journal_storage_repaired = true;
            repair_server.event("storage repaired before next bounded retry");
            Ok(()) as crate::SimResult
        });
    }
    let observer_server = server.clone();
    let observer_state = state.clone();
    sim.client("observer", async move {
        observe(observer_server, observer_state).await
    });
    loop {
        let finished = sim
            .step()
            .map_err(|error| format!("seed={seed} {}: {error}", scenario.name()))?;
        if state.crash_requested.swap(false, Ordering::SeqCst) {
            sim.crash("client");
            if faults.lose_client_intent {
                state.durable.lock().unwrap().current = None;
            }
            let mut log = server.log.lock().unwrap();
            log.client_restarts += 1;
            log.events
                .push("driver crashed client and discarded its runtime state".into());
            log.times_ns.push(sim.elapsed().as_nanos());
            drop(log);
            state.crashed.notify_one();
        }
        if state.read_crash_requested.swap(false, Ordering::SeqCst) {
            sim.crash("client");
            let mut log = server.log.lock().unwrap();
            log.client_journal_read_reboots += 1;
            log.events
                .push("driver discarded failed reader host runtime without cleanup".into());
            log.times_ns.push(sim.elapsed().as_nanos());
        }
        if state.save_crash_requested.swap(false, Ordering::SeqCst) {
            sim.crash("client");
            if faults.promote_temporary {
                state
                    .durable
                    .lock()
                    .unwrap()
                    .apply(Stage::Rename, &[])
                    .map_err(|error| error.to_string())?;
            }
            let mut log = server.log.lock().unwrap();
            log.client_journal_error_crashes += 1;
            log.client_journal_phase_after_crash =
                match intent_journal::load::<Request>(&*state.durable.lock().unwrap()) {
                    Ok(intent) => format!("{:?}", intent.phase),
                    Err(error) => {
                        return Err(format!("save crash invalid durable intent: {error}"));
                    }
                };
            log.events.push(if faults.combined_persistent_save {
                "driver crashed repaired client after recovery-save retry exhaustion, before storage repair".into()
            } else {
                "driver crashed repaired client after recovery-save error, before reload or retry".into()
            });
            log.times_ns.push(sim.elapsed().as_nanos());
            drop(log);
            state.save_crashed.notify_one();
        }
        if state.bounce_requested.swap(false, Ordering::SeqCst) {
            sim.bounce("client");
        }
        if finished {
            break;
        }
    }
    let report = server.log.lock().unwrap().clone();
    Ok(report)
}

async fn observe(server: Server, state: ClientState) -> crate::SimResult {
    let request = Request::fixture(0);
    let garbage = server
        .repository
        .payloads()
        .put_slice(b"client-outage unreferenced garbage")
        .await?;
    state.setup_ready.notify_one();
    server.entered.notified().await;
    state.crashed.notified().await;
    server.repository.collect().await?;
    let snapshot = server.repository.metadata().snapshot().await?;
    let applied = !server.scenario.pause_before();
    if snapshot.get(&[request.marker_key()]).await?[0].is_some() != applied
        || snapshot.root(&RootName::try_from("live")?).await?.is_some() != applied
        || server.log.lock().unwrap().target_applications != usize::from(applied)
    {
        return Err("client restart missed requested application boundary".into());
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
        return Err("client outage lost pending publication protection during GC".into());
    }
    server.log.lock().unwrap().gc_during_client_outage = true;
    server.log.lock().unwrap().pending_bytes_survived = protected;
    server.event("observer collected while client was down and retained publication bytes");
    state.bounce_requested.store(true, Ordering::SeqCst);
    read_failure::observe(&state, &server).await?;
    if !applied {
        state.queried.notified().await;
        if server.log.lock().unwrap().target_applications != 0 {
            return Err("fresh client query applied the pending publication".into());
        }
    }
    server.resume.notify_one();
    tokio::time::timeout(RECOVERY_DEADLINE, async {
        while server.log.lock().unwrap().target_applications == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .map_err(|_| "original handler did not settle after client restart")?;
    state.continue_recovery.notify_one();
    if server.faults.combined_save_crash {
        save_crash::observe(&state, &server).await?;
    }
    if server.faults.combined_persistent_save
        && !server.faults.combined_save_crash
        && !server.faults.repair_within_budget
    {
        save_outage::observe(&state, &server).await?;
    }
    state.finished.notified().await;
    if server.faults.combined_save_crash {
        save_crash::verify(&state, &server).await?;
    }
    let intent: Intent = intent_journal::load(&*state.durable.lock().unwrap())?;
    if intent.request.fingerprint() != request.fingerprint()
        || intent.fingerprint != request.fingerprint()
        || !matches!(intent.phase, Phase::Recovered { ref observed_revision } if *observed_revision == server.log.lock().unwrap().recovery_revision)
    {
        return Err("client did not persist the recovered original operation".into());
    }
    audit(&server, garbage).await
}
