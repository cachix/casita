//! Client host death immediately after journal error, without a local retry.
use super::*;

async fn park_after_error(state: &ClientState) -> crate::SimResult {
    state.crash_requested.store(true, Ordering::SeqCst);
    std::future::pending::<crate::SimResult>().await
}
async fn boot(state: ClientState, server: Server) -> crate::SimResult {
    let epoch = state.epoch.fetch_add(1, Ordering::SeqCst) + 1;
    server.log.lock().unwrap().client_epochs = epoch;
    server.event(format!("error-crash client epoch {epoch} booted"));
    if epoch == 1 {
        state.setup_ready.notified().await;
        let mut intent = Intent::new(Request::fixture(0))?;
        if !state.save_for(&server, &intent).await? {
            return park_after_error(&state).await;
        }
        let outcome = server.bounded(&intent.request, false).await;
        require_unknown(&outcome, &intent.request)?;
        intent.mark_unknown()?;
        if !state.save_for(&server, &intent).await? {
            return park_after_error(&state).await;
        }
        // Only save 3 reaches this gate. Settle the original handler before
        // recovering its effect and attempting to persist Recovered.
        state.queried.notify_one();
        state.continue_recovery.notified().await;
        let response = server.bounded(&intent.request, true).await;
        let revision = recovered_revision(response, &intent)?;
        intent.mark_recovered(&intent.fingerprint.clone(), revision)?;
        if !state.save_for(&server, &intent).await? {
            return park_after_error(&state).await;
        }
        return Err("journal error crash did not reach its selected save".into());
    }
    if state.durable.lock().unwrap().read()?.is_none() {
        // Absence is permitted only for first-save failures before rename.
        // There is no durable identity from which this client could dispatch.
        if server.faults.restart_republishes {
            // Negative control: recover identity from fixture defaults instead.
            server.bounded(&Request::fixture(0), false).await;
        }
        server.log.lock().unwrap().client_missing_intent_stopped = true;
        server.event("fresh client has no intent and stops without reconstructing a request");
        state.finished.notify_one();
        return Ok(());
    }
    let mut intent: Intent = intent_journal::load(&*state.durable.lock().unwrap())?;
    server.log.lock().unwrap().client_intent_recoveries += 1;
    let response = server
        .bounded(&intent.request, !server.faults.restart_republishes)
        .await;
    if matches!(response, ClientOutcome::Unknown { .. }) {
        require_unknown(&response, &intent.request)?;
        intent.mark_unknown()?;
    } else {
        let revision = recovered_revision(response, &intent)?;
        server.log.lock().unwrap().recovery_revision = revision.clone();
        intent.mark_recovered(&intent.fingerprint.clone(), revision)?;
    }
    if !state.save_for(&server, &intent).await? {
        return Err("fresh client repeated journal fault".into());
    }
    state.finished.notify_one();
    Ok(())
}
fn recovered_revision(
    response: ClientOutcome,
    intent: &Intent,
) -> Result<String, Box<dyn std::error::Error>> {
    match response {
        ClientOutcome::Known(Response::RecoveredEffect {
            request_fingerprint,
            observed_revision,
        }) if request_fingerprint == intent.fingerprint => Ok(observed_revision),
        _ => Err(
            "restarted client resubmitted or failed read-only recovery after journal error".into(),
        ),
    }
}

pub(super) fn run(seed: u64, scenario: Scenario, faults: Faults) -> Result<Report, String> {
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
        boot(client_state.clone(), client_server.clone())
    });
    let observer_server = server.clone();
    let observer_state = state.clone();
    sim.client("observer", async move {
        observe(observer_server, observer_state).await
    });
    loop {
        let done = sim.step().map_err(|error| {
            format!(
                "seed={seed}, {}, journal error crash: {error}",
                scenario.name()
            )
        })?;
        if state.crash_requested.swap(false, Ordering::SeqCst) {
            sim.crash("client");
            if faults.promote_temporary {
                state
                    .durable
                    .lock()
                    .unwrap()
                    .apply(Stage::Rename, &[])
                    .map_err(|e| e.to_string())?;
            }
            let phase = match state
                .durable
                .lock()
                .unwrap()
                .read()
                .map_err(|e| e.to_string())?
            {
                None => "Missing".into(),
                Some(bytes) => format!(
                    "{:?}",
                    serde_json::from_slice::<Intent>(&bytes)
                        .map_err(|e| format!(
                            "truncated temporary record replaced current intent: {e}"
                        ))?
                        .phase
                ),
            };
            let mut log = server.log.lock().unwrap();
            log.client_restarts += 1;
            log.client_journal_error_crashes += 1;
            log.client_journal_phase_after_crash = phase;
            log.events
                .push("driver crashed client after I/O error and before any local retry".into());
            log.times_ns.push(sim.elapsed().as_nanos());
            drop(log);
            state.crashed.notify_one();
        }
        if state.bounce_requested.swap(false, Ordering::SeqCst) {
            sim.bounce("client");
        }
        if done {
            break;
        }
    }
    let report = server.log.lock().unwrap().clone();
    Ok(report)
}
async fn settle(server: &Server) -> crate::SimResult {
    server.resume.notify_one();
    tokio::time::timeout(RECOVERY_DEADLINE, async {
        while server.log.lock().unwrap().target_applications == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .map_err(|_| "accepted publication did not settle while client was down")?;
    Ok(())
}
async fn observe(server: Server, state: ClientState) -> crate::SimResult {
    let request = Request::fixture(0);
    let write = server.faults.journal_failure_at;
    let garbage = server
        .repository
        .payloads()
        .put_slice(b"journal-error unreferenced garbage")
        .await?;
    state.setup_ready.notify_one();
    if write > 1 {
        server.entered.notified().await;
    }
    if write == 3 {
        state.queried.notified().await;
        settle(&server).await?;
        state.continue_recovery.notify_one();
    }
    state.crashed.notified().await;
    let failure = server.faults.journal_failure.unwrap();
    let expected_phase = failure.surviving_phase(write);
    if let intent_journal::Failure::Partial(cut) = failure {
        let mut expected = Intent::new(request.clone())?;
        if write > 1 {
            expected.mark_unknown()?;
        }
        if write > 2 {
            expected.mark_recovered(
                &expected.fingerprint.clone(),
                server
                    .repository
                    .metadata()
                    .snapshot()
                    .await?
                    .revision()
                    .to_string(),
            )?;
        }
        let bytes = serde_json::to_vec(&expected)?;
        let len = intent_journal::verify_partial(&*state.durable.lock().unwrap(), cut, &bytes)?;
        let mut log = server.log.lock().unwrap();
        log.client_journal_partial_len = len;
        log.client_journal_partial_full_len = bytes.len();
        log.client_journal_partial_proven = true;
    }
    if !server
        .log
        .lock()
        .unwrap()
        .client_journal_phase_after_crash
        .starts_with(expected_phase)
    {
        return Err("journal error crash exposed unexpected surviving phase".into());
    }
    let applied = write == 3 || (write == 2 && !server.scenario.pause_before());
    let snapshot = server.repository.metadata().snapshot().await?;
    if snapshot.get(&[request.marker_key()]).await?[0].is_some() != applied
        || snapshot.root(&RootName::try_from("live")?).await?.is_some() != applied
        || server.log.lock().unwrap().target_applications != usize::from(applied)
    {
        return Err("journal error crash missed server application boundary".into());
    }
    drop(snapshot);
    server.repository.collect().await?;
    if server.repository.payloads().has(&garbage).await? {
        return Err("journal error outage left unreferenced garbage".into());
    }
    server.log.lock().unwrap().gc_during_client_outage = true;
    server.log.lock().unwrap().garbage_collected = true;
    if write > 1 {
        if server
            .repository
            .payloads()
            .read_to_vec(&BlobId::new(Digest::hash(&request.payload)))
            .await?
            .as_deref()
            != Some(request.payload.as_slice())
        {
            return Err("journal error outage lost accepted publication bytes".into());
        }
        server.log.lock().unwrap().pending_bytes_survived = true;
        if write == 2 {
            settle(&server).await?;
        }
    }
    if server.faults.persistent_journal {
        let failures = server.log.lock().unwrap().client_journal_failures;
        let requests = server.log.lock().unwrap().requests;
        if failures != 3 || server.log.lock().unwrap().client_journal_retry_exhaustions != 1 {
            return Err("persistent outage did not exhaust exactly three attempts".into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        if server.log.lock().unwrap().client_journal_failures != failures
            || server.log.lock().unwrap().requests != requests
        {
            return Err("client kept retrying after its journal budget was exhausted".into());
        }
        server.log.lock().unwrap().client_journal_outage_quiescent = true;
        state.outage_active.store(false, Ordering::SeqCst);
        server.log.lock().unwrap().client_journal_storage_repaired = true;
        server.event("storage repaired after exhausted client stayed quiescent");
    }
    let revision_before_recovery = server.repository.metadata().snapshot().await?.revision();
    state.bounce_requested.store(true, Ordering::SeqCst);
    state.finished.notified().await;
    let revision_after_recovery = server.repository.metadata().snapshot().await?.revision();
    if revision_before_recovery != revision_after_recovery {
        return Err("error-crash client recovery mutated repository metadata".into());
    }
    server
        .log
        .lock()
        .unwrap()
        .client_recovery_left_revision_unchanged = true;
    let missing = expected_phase == "Missing";
    if !missing {
        let intent: Intent = intent_journal::load(&*state.durable.lock().unwrap())?;
        if intent.fingerprint != request.fingerprint()
            || (write == 1 && intent.phase != Phase::Unknown)
            || (write > 1
                && !matches!(intent.phase, Phase::Recovered { ref observed_revision } if *observed_revision==server.log.lock().unwrap().recovery_revision))
        {
            return Err("error-crash recovery persisted wrong identity or status".into());
        }
    } else if state.durable.lock().unwrap().read()?.is_some()
        || server.log.lock().unwrap().requests != 0
    {
        return Err("client reconstructed a request after losing first intent".into());
    }
    if write > 1 {
        audit(&server, garbage).await?;
    } else {
        let snapshot = server.repository.metadata().snapshot().await?;
        if snapshot.root(&RootName::try_from("live")?).await?.is_some()
            || snapshot.get(&[request.marker_key()]).await?[0].is_some()
            || snapshot.object(&request.object_key()).await?.is_some()
        {
            return Err("client published after initial journal error".into());
        }
    }
    let log = server.log.lock().unwrap();
    if write > 1 && log.query_requests != if write == 3 { 2 } else { 1 } {
        return Err(
            "restarted client resubmitted or failed read-only recovery after journal error".into(),
        );
    }
    if log.client_epochs != 2
        || log.client_restarts != 1
        || log.client_journal_error_crashes != 1
        || log.client_journal_failures
            != if server.faults.persistent_journal {
                3
            } else {
                1
            }
        || log.client_journal_retries
            != if server.faults.persistent_journal {
                2
            } else {
                0
            }
        || log.client_missing_intent_stopped != missing
        || log.client_intent_recoveries != usize::from(!missing)
        || log.target_applications != usize::from(write > 1)
        || log.query_requests
            != if missing {
                0
            } else if write == 3 {
                2
            } else {
                1
            }
        || log.requests
            != if write == 1 {
                usize::from(!missing)
            } else if write == 3 {
                3
            } else {
                2
            }
        || log.client_intent_writes
            != if missing {
                0
            } else if write == 1 {
                1
            } else {
                write
            }
        || log.marker_recoveries
            != if write == 1 {
                0
            } else if write == 3 {
                2
            } else {
                1
            }
        || log.server_errors != 0
        || log.deadline_expirations != usize::from(write > 1)
        || log.transport_failures != usize::from(write > 1)
        || !log.garbage_collected
        || !log.client_recovery_left_revision_unchanged
        || !log.gc_during_client_outage
    {
        return Err(format!("journal error crash oracle was vacuous: {log:?}").into());
    }
    Ok(())
}
