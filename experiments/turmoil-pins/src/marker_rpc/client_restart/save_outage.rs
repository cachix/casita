//! Observe a live client parked after its recovery-save retry budget is exhausted.
use super::*;
pub(super) async fn await_repair(
    state: &ClientState,
    server: &Server,
    intent: &Intent,
) -> Result<bool, Box<dyn std::error::Error>> {
    state.save_exhausted.notify_one();
    if server.faults.retry_before_save_repair {
        // Negative control starts another batch while the persistent fault is active.
        state.save_for(server, intent).await?;
    }
    state.save_repaired.notified().await;
    state.save_for(server, intent).await
}
pub(super) async fn observe(state: &ClientState, server: &Server) -> crate::SimResult {
    state.save_exhausted.notified().await;
    let before = server.log.lock().unwrap().clone();
    if before
        .client_journal_batches
        .last()
        .is_none_or(|batch| batch.attempts != 3 || batch.saved)
    {
        return Err("combined recovery save exceeded retry budget".into());
    }
    let exhaustions = if server.faults.restart_with_save_outage {
        2
    } else {
        1
    };
    if before.client_journal_failures != 3 * exhaustions
        || before.client_intent_writes != 2
        || before.client_journal_retry_exhaustions != exhaustions
    {
        return Err("persistent recovery save did not remain quiescent".into());
    }
    let (current, temporary) = {
        let storage = state.durable.lock().unwrap();
        let intent: Intent = intent_journal::load(&*storage)?;
        let expected = Request::fixture(0).fingerprint();
        if intent.fingerprint != expected || intent.request.fingerprint() != expected {
            return Err("recovery save outage lost original identity".into());
        }
        let phase = if server.faults.journal_failure.unwrap().replacement_visible() {
            Phase::Recovered {
                observed_revision: before.recovery_revision.clone(),
            }
        } else {
            Phase::Unknown
        };
        if intent.phase != phase {
            return Err("recovery save outage exposed wrong phase".into());
        }
        (storage.read()?, storage.temporary()?)
    };
    let revision = server.repository.metadata().snapshot().await?.revision();
    tokio::time::sleep(Duration::from_millis(25)).await;
    let after = server.log.lock().unwrap().clone();
    let (visible, leftover) = {
        let storage = state.durable.lock().unwrap();
        (storage.read()?, storage.temporary()?)
    };
    if before.client_journal_batches != after.client_journal_batches
        || before.client_journal_failures != after.client_journal_failures
        || before.client_journal_retries != after.client_journal_retries
        || before.client_intent_writes != after.client_intent_writes
        || before.requests != after.requests
        || before.target_applications != after.target_applications
        || current != visible
        || temporary != leftover
        || server.repository.metadata().snapshot().await?.revision() != revision
        || !state.outage_active.load(Ordering::SeqCst)
    {
        return Err("persistent recovery save did not remain quiescent".into());
    }
    server.log.lock().unwrap().client_journal_outage_quiescent = true;
    state.outage_active.store(false, Ordering::SeqCst);
    server.log.lock().unwrap().client_journal_storage_repaired = true;
    server.event(
        "observer repaired persistent recovery-save outage after 25ms of live-client quiescence",
    );
    state.save_repaired.notify_one();
    Ok(())
}
