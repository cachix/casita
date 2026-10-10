//! Read failures persist across three fresh client hosts, then storage is repaired.
use super::*;

pub(super) async fn validate_or_park(state: &ClientState, server: &Server) -> crate::SimResult {
    let Some(failure) = server.faults.journal_read_failure else {
        return Ok(());
    };
    if state.read_repaired.load(Ordering::SeqCst) {
        return Ok(());
    }
    let error = {
        let storage = state.durable.lock().unwrap();
        intent_journal::load::<Request>(&intent_journal::ReadInjected {
            storage: &*storage,
            failure: Some(failure),
        })
        .err()
        .ok_or("journal read fault did not fail validated loading")?
    };
    if !error.to_string().contains(failure.diagnostic()) {
        return Err(format!("wrong journal read diagnostic: {error}").into());
    }
    {
        let mut log = server.log.lock().unwrap();
        log.client_journal_read_failures += 1;
        log.client_journal_read_diagnostics.push(error.to_string());
    }
    server.event(format!(
        "fresh client stopped on {} before recovery dispatch",
        failure.name()
    ));
    state.read_failed.notify_one();
    if server.faults.ignore_journal_read_failure {
        // Negative control bypasses the failed read and uses unvalidated availability.
        return Ok(());
    }
    state.read_crash_requested.store(true, Ordering::SeqCst);
    std::future::pending::<crate::SimResult>().await
}
pub(super) async fn observe(state: &ClientState, server: &Server) -> crate::SimResult {
    if server.faults.journal_read_failure.is_none() {
        return Ok(());
    }
    let current = state.durable.lock().unwrap().read()?;
    // A valid alternate identity must never substitute for a failed current read.
    let decoy = Intent::new(Request::fixture(1))?;
    state
        .durable
        .lock()
        .unwrap()
        .apply(Stage::Write, &serde_json::to_vec(&decoy)?)?;
    let temporary = state.durable.lock().unwrap().temporary()?;
    let revision = server.repository.metadata().snapshot().await?.revision();
    let requests = server.log.lock().unwrap().requests;
    for attempt in 1..=3 {
        state.read_failed.notified().await;
        let log = server.log.lock().unwrap().clone();
        if log.client_intent_recoveries != 0 || log.requests != requests || log.query_requests != 0
        {
            return Err("journal read failure dispatched recovery before repair".into());
        }
        let (visible, leftover) = {
            let storage = state.durable.lock().unwrap();
            (storage.read()?, storage.temporary()?)
        };
        if visible != current
            || leftover != temporary
            || server.repository.metadata().snapshot().await?.revision() != revision
        {
            return Err("failed journal reader changed durable state".into());
        }
        if log.client_journal_read_failures != attempt || log.client_intent_writes != 2 {
            return Err("journal read outage did not remain quiescent".into());
        }
        server.event(format!(
            "observer verified failed client boot {attempt}: no dispatch or durable changes"
        ));
        if attempt == 3 {
            state.read_repaired.store(true, Ordering::SeqCst);
            server.log.lock().unwrap().client_journal_read_repaired = true;
            server.log.lock().unwrap().client_journal_read_unchanged = true;
            server.event("observer repaired current journal reads before fresh healthy boot");
        }
        state.bounce_requested.store(true, Ordering::SeqCst);
    }
    Ok(())
}

/// A local journal retry must not dispatch a fresh query or publication.
pub(super) async fn save_recovered(
    state: &ClientState,
    server: &Server,
    intent: &Intent,
) -> Result<bool, Box<dyn std::error::Error>> {
    if server.faults.journal_read_failure.is_none() || server.faults.journal_failure.is_none() {
        return state.save_for(server, intent).await;
    }
    let requests = server.log.lock().unwrap().requests;
    let applications = server.log.lock().unwrap().target_applications;
    let revision = server.repository.metadata().snapshot().await?.revision();
    let mut saved = state.save_for(server, intent).await?;
    if !saved && server.faults.combined_persistent_save && !server.faults.combined_save_crash {
        saved = super::save_outage::await_repair(state, server, intent).await?;
    }
    if server.faults.recovery_save_republishes {
        // The marker prevents duplicate application, but this dispatch is still forbidden.
        server.bounded(&intent.request, false).await;
    }
    if server.log.lock().unwrap().requests != requests {
        return Err("recovery journal retry dispatched remote operation".into());
    }
    let applications_after = server.log.lock().unwrap().target_applications;
    if applications_after != applications
        || server.repository.metadata().snapshot().await?.revision() != revision
    {
        return Err("recovery journal retry changed published effect".into());
    }
    if !saved && server.faults.combined_save_crash {
        return super::save_crash::park(state).await;
    }
    server.event(
        "observer verified recovery save retry stayed local and preserved repository revision",
    );
    Ok(saved)
}
