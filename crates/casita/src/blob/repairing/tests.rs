use super::*;
#[cfg(feature = "native")]
use crate::test_util::native::{small_chunked_store, write_blob};

fn registered_leader() -> RepairLeader {
    let digest = BlobId::new([7; 32].into());
    let flight = Arc::new(RepairFlight::new());
    let leader = RepairLeader::new(
        digest,
        "original near-tier corruption".to_owned(),
        flight,
        Arc::default(),
    );
    leader
        .repairs
        .try_lock()
        .unwrap()
        .insert(digest, leader.flight.clone());
    leader
}

#[test]
fn cancelled_leader_releases_its_flight_without_a_runtime() {
    let leader = registered_leader();
    let digest = leader.digest;
    let repairs = leader.repairs.clone();
    drop(leader);

    assert!(
        !repairs.try_lock().unwrap().contains_key(&digest),
        "cancelled repair stayed registered without a runtime to schedule cleanup"
    );
}

#[cfg(feature = "native")]
#[test]
fn cancelled_registration_does_not_block_the_next_real_repair() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (near, near_dir) = small_chunked_store();
    let (far, _far_dir) = small_chunked_store();
    let data = b"a cancelled repair must not strand these verified bytes".to_vec();
    let digest = runtime.block_on(async {
        let digest = write_blob(&near, &data).await;
        assert_eq!(write_blob(&far, &data).await, digest);
        let chunk = near.chunks(&digest).await.unwrap().unwrap()[0].digest;
        let path = crate::blob::chunked::sharded_path(
            &object_store::path::Path::default(),
            "chunks",
            chunk.as_digest(),
        );
        std::fs::write(near_dir.path().join(path.as_ref()), b"not zstd").unwrap();
        assert!(
            validated_reader(&near, &digest).await.is_err(),
            "fixture must require a near-tier repair"
        );
        digest
    });
    let repairing = RepairingBlobStore::new(near.clone(), far);
    let leader = RepairLeader::new(
        digest,
        "near-tier chunk is corrupt".to_owned(),
        Arc::new(RepairFlight::new()),
        repairing.repairs.clone(),
    );
    repairing
        .repairs
        .try_lock()
        .unwrap()
        .insert(digest, leader.flight.clone());

    // A cancelled future may be dropped outside an entered runtime. Exercise
    // the next actual read, including far verification and near publication.
    drop(leader);
    assert_eq!(
        runtime.block_on(repairing.read_to_vec(&digest)).unwrap(),
        Some(data.clone())
    );
    assert_eq!(
        runtime.block_on(near.read_to_vec(&digest)).unwrap(),
        Some(data)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_leader_releases_its_flight_before_a_cleanup_task_runs() {
    let leader = registered_leader();
    let digest = leader.digest;
    let repairs = leader.repairs.clone();
    drop(leader);

    // Do not yield: a new reader must not depend on the scheduler choosing an
    // unrelated cleanup task before it can start a replacement repair.
    assert!(
        !repairs.try_lock().unwrap().contains_key(&digest),
        "cancelled repair stayed registered until a cleanup task was polled"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_leader_preserves_the_diagnostic_for_an_existing_waiter() {
    let leader = registered_leader();
    let digest = leader.digest;
    let flight = leader.flight.clone();
    let mut waiter = Box::pin(flight.wait());
    assert!(futures::poll!(waiter.as_mut()).is_pending());
    assert_eq!(flight.waiters.load(Ordering::SeqCst), 1);

    drop(leader);

    let Poll::Ready(Err(error)) = futures::poll!(waiter.as_mut()) else {
        panic!("registered waiter did not receive the cancellation outcome");
    };
    assert_eq!(error.digest(), digest);
    assert_eq!(error.near_error(), "original near-tier corruption");
    assert_eq!(
        error.repair_error(),
        "repair was cancelled before publication completed"
    );
    assert_eq!(flight.waiters.load(Ordering::SeqCst), 0);
}

#[test]
fn cancelling_an_old_leader_preserves_a_replacement_flight() {
    let leader = registered_leader();
    let digest = leader.digest;
    let repairs = leader.repairs.clone();
    let replacement = Arc::new(RepairFlight::new());
    repairs
        .try_lock()
        .unwrap()
        .insert(digest, replacement.clone());

    drop(leader);

    assert!(Arc::ptr_eq(
        repairs.try_lock().unwrap().get(&digest).unwrap(),
        &replacement
    ));
    assert!(replacement.outcome.lock().unwrap().is_none());
}

#[test]
fn disarmed_leader_preserves_the_completed_outcome() {
    let mut leader = registered_leader();
    let flight = leader.flight.clone();
    flight.complete(Ok(()));
    leader.disarm();

    drop(leader);

    assert!(matches!(*flight.outcome.lock().unwrap(), Some(Ok(()))));
}
