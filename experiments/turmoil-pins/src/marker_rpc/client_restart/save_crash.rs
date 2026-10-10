//! Crash after a recovery-save error or exhaustion of its persistent retry budget.
use super::*;
pub(super) async fn park(state: &ClientState) -> Result<bool, Box<dyn std::error::Error>> {
    state.save_crash_requested.store(true, Ordering::SeqCst);
    std::future::pending().await
}
pub(super) async fn recover(
    state: &ClientState,
    server: &Server,
    mut intent: Intent,
) -> crate::SimResult {
    let request = Request::fixture(0);
    if intent.fingerprint != request.fingerprint()
        || intent.request.fingerprint() != request.fingerprint()
    {
        return Err("save crash lost original durable intent identity".into());
    }
    server.log.lock().unwrap().client_intent_recoveries += 1;
    let response = server
        .bounded(&intent.request, !server.faults.save_crash_republishes)
        .await;
    let revision = match response {
        ClientOutcome::Known(Response::RecoveredEffect {
            request_fingerprint,
            observed_revision,
        }) if request_fingerprint == intent.fingerprint => observed_revision,
        _ => return Err("save-crash client failed read-only effect recovery".into()),
    };
    if let Phase::Recovered { observed_revision } = &intent.phase {
        if *observed_revision != revision {
            return Err("save-crash recovery changed terminal revision".into());
        }
    } else if !matches!(intent.phase, Phase::Unknown) {
        return Err("save-crash client loaded unexpected phase".into());
    }
    intent.mark_recovered(&intent.fingerprint.clone(), revision)?;
    let mut saved = state.save_for(server, &intent).await?;
    if !saved && server.faults.restart_with_save_outage {
        saved = super::save_outage::await_repair(state, server, &intent).await?;
    }
    if !saved {
        return Err("fresh client repeated consumed save fault".into());
    }
    server.event(
        "fresh client recovered after failed save without reconstructing or resubmitting intent",
    );
    state.finished.notify_one();
    Ok(())
}
pub(super) async fn observe(state: &ClientState, server: &Server) -> crate::SimResult {
    state.save_crashed.notified().await;
    let intent: Intent = intent_journal::load(&*state.durable.lock().unwrap())
        .map_err(|error| format!("save crash invalid durable intent: {error}"))?;
    let request = Request::fixture(0);
    if intent.fingerprint != request.fingerprint()
        || intent.request.fingerprint() != request.fingerprint()
    {
        return Err("save crash lost original durable intent identity".into());
    }
    let failure = server.faults.journal_failure.unwrap();
    let revision = server.log.lock().unwrap().recovery_revision.clone();
    let expected = if failure.replacement_visible() {
        Phase::Recovered {
            observed_revision: revision.clone(),
        }
    } else {
        Phase::Unknown
    };
    if intent.phase != expected {
        return Err("save crash exposed wrong surviving journal phase".into());
    }
    if let intent_journal::Failure::Partial(cut) = failure {
        let mut recovered = Intent::new(request)?;
        recovered.mark_recovered(&recovered.fingerprint.clone(), revision)?;
        let expected_bytes = serde_json::to_vec(&recovered)?;
        let len =
            intent_journal::verify_partial(&*state.durable.lock().unwrap(), cut, &expected_bytes)?;
        let mut log = server.log.lock().unwrap();
        log.client_journal_partial_len = len;
        log.client_journal_partial_full_len = expected_bytes.len();
        log.client_journal_partial_proven = true;
    }
    let log = server.log.lock().unwrap().clone();
    if server.faults.combined_persistent_save {
        if log.client_intent_writes != 2
            || log.client_journal_retries != 2
            || log.client_journal_failures != 3
            || log.client_journal_error_crashes != 1
            || log.client_journal_retry_exhaustions != 1
            || log
                .client_journal_batches
                .last()
                .is_none_or(|batch| batch.attempts != 3 || batch.saved)
        {
            return Err("persistent save crash did not follow exhausted retry budget".into());
        }
        let (current, temporary) = {
            let storage = state.durable.lock().unwrap();
            (storage.read()?, storage.temporary()?)
        };
        let revision = server.repository.metadata().snapshot().await?.revision();
        tokio::time::sleep(Duration::from_millis(25)).await;
        let after = server.log.lock().unwrap().clone();
        let (visible, leftover) = {
            let storage = state.durable.lock().unwrap();
            (storage.read()?, storage.temporary()?)
        };
        if log.requests != after.requests
            || log.client_journal_batches != after.client_journal_batches
            || log.client_journal_failures != after.client_journal_failures
            || log.client_journal_retries != after.client_journal_retries
            || current != visible
            || temporary != leftover
            || server.repository.metadata().snapshot().await?.revision() != revision
            || !state.outage_active.load(Ordering::SeqCst)
        {
            return Err("persistent save crash changed state before repair".into());
        }
        if !server.faults.restart_with_save_outage {
            state.outage_active.store(false, Ordering::SeqCst);
            server.log.lock().unwrap().client_journal_storage_repaired = true;
        }
        server.log.lock().unwrap().client_journal_outage_quiescent = true;
        server.event(if server.faults.restart_with_save_outage {
            "observer retained persistent save fault across restart after 25ms with failed client down"
        } else { "observer repaired persistent save storage after 25ms with failed client down" });
    } else if log.client_intent_writes != 2
        || log.client_journal_retries != 0
        || log.client_journal_failures != 1
        || log.client_journal_error_crashes != 1
    {
        return Err("save crash did not precede local retry".into());
    }
    state.save_requests.store(log.requests, Ordering::SeqCst);
    state
        .save_queries
        .store(log.query_requests, Ordering::SeqCst);
    let saved_revision = server
        .repository
        .metadata()
        .snapshot()
        .await?
        .revision()
        .to_string();
    *state.save_revision.lock().unwrap() = saved_revision;
    server.event(
        "observer verified surviving current journal and unchanged state while failed saver was down",
    );
    state.bounce_requested.store(true, Ordering::SeqCst);
    if server.faults.restart_with_save_outage {
        super::save_outage::observe(state, server).await?;
    }
    Ok(())
}
pub(super) async fn verify(state: &ClientState, server: &Server) -> crate::SimResult {
    let log = server.log.lock().unwrap().clone();
    if log.requests != state.save_requests.load(Ordering::SeqCst) + 1
        || log.query_requests != state.save_queries.load(Ordering::SeqCst) + 1
    {
        return Err("save-crash recovery resubmitted original publication".into());
    }
    let before = state.save_revision.lock().unwrap().clone();
    if server
        .repository
        .metadata()
        .snapshot()
        .await?
        .revision()
        .to_string()
        != before
        || log.target_applications != 1
    {
        return Err("save-crash recovery changed published effect".into());
    }
    server
        .log
        .lock()
        .unwrap()
        .client_recovery_left_revision_unchanged = true;
    Ok(())
}
