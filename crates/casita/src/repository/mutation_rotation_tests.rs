use super::*;
use crate::metadata::{PinResource, PinScope, flush_repository_leases};
use crate::{MemoryBlobStore, MemoryMetadataStore};
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountAdmission {
    repository: Repository<MemoryBlobStore, MemoryMetadataStore>,
    calls: Arc<AtomicUsize>,
    collect: bool,
}

#[async_trait]
impl MutationStart for CountAdmission {
    async fn before_mutation(&self, _: SpillLimits) -> Result<bool, RepositoryError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.collect {
            self.repository.try_collect().await?;
        }
        Ok(false)
    }
}

#[tokio::test]
async fn rotation_preserves_outputs_without_readmitting_the_operation() {
    let mut repository =
        Repository::new(MemoryBlobStore::new(), MemoryMetadataStore::new().unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    Arc::make_mut(&mut repository.profile).mutation_start = Some(Arc::new(CountAdmission {
        repository: repository.clone(),
        calls: calls.clone(),
        collect: true,
    }));
    let mut session = repository.mutation_session().await.unwrap();
    let mut keys = Vec::new();
    let mut held = None;
    for index in 0..3 {
        let bytes = format!("rotation-{index}");
        let object = session.stage_blob(bytes.as_bytes()).await.unwrap();
        keys.push(object.record().key().clone());
        session.publish_unrooted(vec![object]).await.unwrap();
        held = Some(repository.retention_hold().await.unwrap());
        session.rotate().await.unwrap();
        flush_repository_leases().await.unwrap();
        let inventory = repository
            .state
            .pin_store()
            .await
            .unwrap()
            .inventory()
            .await
            .unwrap();
        assert_eq!(
            inventory
                .pins
                .values()
                .filter(|p| p.scope == PinScope::Staging)
                .count(),
            1
        );
        assert!(
            inventory
                .pins
                .values()
                .filter(|p| p.scope == PinScope::Staging)
                .all(|p| !p
                    .resources
                    .iter()
                    .any(|r| matches!(r, PinResource::Object(_))))
        );
        repository.collect().await.unwrap();
        for (index, key) in keys.iter().enumerate() {
            let (_, mut payload) = repository.open_payload(key).await.unwrap().unwrap();
            let mut bytes = Vec::new();
            payload.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, format!("rotation-{index}").as_bytes());
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(repository.mutation_session().await.unwrap());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    drop(session);
    drop(held);
    flush_repository_leases().await.unwrap();
    repository.collect().await.unwrap();
    for key in keys {
        assert!(repository.open_payload(&key).await.unwrap().is_none());
    }
    flush_repository_leases().await.unwrap();
    assert!(
        repository
            .state
            .pin_store()
            .await
            .unwrap()
            .inventory()
            .await
            .unwrap()
            .pins
            .is_empty()
    );
}

// Inventory and correctness checks are outside the accumulated timer. Lease
// draining is included so asynchronous drops cannot move work out of a sample.
async fn rotation_sample(count: usize, mode: &str, eligible: bool) -> serde_json::Value {
    assert!(count > 0);
    assert!(matches!(mode, "long" | "sessions" | "rotate"));
    let mut repository =
        Repository::new(MemoryBlobStore::new(), MemoryMetadataStore::new().unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    Arc::make_mut(&mut repository.profile).mutation_start = Some(Arc::new(CountAdmission {
        repository: repository.clone(),
        calls: calls.clone(),
        collect: eligible,
    }));
    let initial = std::time::Instant::now();
    let mut session = repository.mutation_session().await.unwrap();
    let mut elapsed = initial.elapsed();
    let mut held = None;
    let mut keys = Vec::with_capacity(count * 64);
    let mut peak = 0;
    for publication in 0..count {
        let started = std::time::Instant::now();
        if publication > 0 && publication % 8 == 0 && mode != "long" {
            held = Some(repository.retention_hold().await.unwrap());
            if mode == "rotate" {
                session.rotate().await.unwrap();
            } else {
                drop(session);
                session = repository.mutation_session().await.unwrap();
            }
        }
        let mut staged = Vec::with_capacity(64);
        for offset in 0..64 {
            let mut bytes = vec![7; 256];
            bytes[..8].copy_from_slice(&((publication * 64 + offset) as u64).to_le_bytes());
            let object = session.stage_blob(&bytes).await.unwrap();
            keys.push(object.record().key().clone());
            staged.push(object);
        }
        session.publish_unrooted(staged).await.unwrap();
        flush_repository_leases().await.unwrap();
        elapsed += started.elapsed();
        let inventory = repository
            .state
            .pin_store()
            .await
            .unwrap()
            .inventory()
            .await
            .unwrap();
        let writer_objects = inventory
            .pins
            .values()
            .filter(|p| p.scope == PinScope::Staging)
            .map(|p| {
                p.resources
                    .iter()
                    .filter(|r| matches!(r, PinResource::Object(_)))
                    .count()
            })
            .max()
            .unwrap_or(0);
        peak = peak.max(writer_objects);
    }
    let finishing = std::time::Instant::now();
    let final_hold = repository.retention_hold().await.unwrap();
    drop(held);
    drop(session);
    flush_repository_leases().await.unwrap();
    elapsed += finishing.elapsed();
    let expected_admissions = if mode == "sessions" {
        count.div_ceil(8)
    } else {
        1
    };
    assert_eq!(calls.load(Ordering::SeqCst), expected_admissions);
    assert_eq!(peak, 64 * if mode == "long" { count } else { count.min(8) });
    repository.collect().await.unwrap();
    for (index, key) in keys.iter().enumerate() {
        let (_, mut reader) = repository.open_payload(key).await.unwrap().unwrap();
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        let mut expected = vec![7; 256];
        expected[..8].copy_from_slice(&(index as u64).to_le_bytes());
        assert_eq!(bytes, expected);
    }
    drop(final_hold);
    flush_repository_leases().await.unwrap();
    repository.collect().await.unwrap();
    for key in keys {
        assert!(repository.open_payload(&key).await.unwrap().is_none());
    }
    flush_repository_leases().await.unwrap();
    assert!(
        repository
            .state
            .pin_store()
            .await
            .unwrap()
            .inventory()
            .await
            .unwrap()
            .pins
            .is_empty()
    );
    serde_json::json!({"count": count, "mode": mode, "eligible": eligible,
        "batch_size": 64, "group_size": 8, "admissions": expected_admissions,
        "collections": if eligible { expected_admissions } else { 0 },
        "peak_writer_objects": peak, "nanos": elapsed.as_nanos() as u64,
        "correctness": "exact readback after collection; bounded writer resources; exact admissions; released pins"})
}

#[tokio::test]
async fn rotation_boundaries_preserve_every_published_object() {
    for count in [7, 8, 9, 16, 17] {
        for mode in ["long", "sessions", "rotate"] {
            for eligible in [false, true] {
                rotation_sample(count, mode, eligible).await;
            }
        }
    }
}

#[tokio::test]
#[ignore = "performance probe; run through benchmark run mutation-rotation"]
async fn benchmark_mutation_rotation() {
    let count = std::env::var("CASITA_ROTATION_COUNT")
        .unwrap()
        .parse()
        .unwrap();
    let mode = std::env::var("CASITA_ROTATION_MODE").unwrap();
    let eligible = match std::env::var("CASITA_ROTATION_ELIGIBLE").unwrap().as_str() {
        "0" => false,
        "1" => true,
        other => panic!("invalid eligibility: {other}"),
    };
    println!(
        "mutation_rotation_sample {}",
        rotation_sample(count, &mode, eligible).await
    );
}
