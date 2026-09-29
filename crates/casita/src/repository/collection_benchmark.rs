//! Collection cost of ordering deletions after their commits, for
//! `benchmark run deletion-ordering`.

use super::*;
use std::collections::BTreeSet;
use std::time::Instant;

/// A collection pass flushes before every deletion batch; a pass with nothing
/// to delete pays nothing. Garbage counts span a single payload to many
/// deletion batches, so the probe reports how flush cost grows with garbage.
/// Fixture construction is outside timing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "performance probe; run through benchmark run deletion-ordering"]
async fn benchmark_collection_deletion_ordering() {
    let iterations: usize = std::env::var("CASITA_DELETION_ORDERING_BENCH_ITERATIONS")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(3);
    let counts: Vec<usize> = std::env::var("CASITA_DELETION_ORDERING_BENCH_OBJECTS")
        .unwrap_or_else(|_| "1,64,1024".into())
        .split(',')
        .map(|v| v.parse().unwrap())
        .collect();
    assert!(iterations > 0 && !counts.is_empty());
    for &count in &counts {
        let (mut deleting, mut idle) = (0_u128, 0_u128);
        let mut flushes_per_pass = BTreeSet::new();
        for iteration in 0..iterations {
            let directory = tempfile::tempdir().unwrap();
            let repository = Repository::local(directory.path()).await.unwrap();
            let commits = repository.metadata().commit_durability().unwrap();
            let session = repository.mutation_session().await.unwrap();
            let mut objects = Vec::new();
            let mut roots = Vec::new();
            for index in 0..count {
                let bytes = format!("garbage {iteration} {index}\n").repeat(1024);
                let object = session.stage_blob(bytes.as_bytes()).await.unwrap();
                roots.push(RootChange::Set {
                    name: format!("garbage/{index}").parse().unwrap(),
                    target: object.record().key().clone(),
                });
                objects.push(object);
            }
            session.publish(objects, roots).await.unwrap();
            drop(session);
            let removals = (0..count)
                .map(|index| RootChange::Remove {
                    name: format!("garbage/{index}").parse().unwrap(),
                })
                .collect();
            repository
                .mutation_session()
                .await
                .unwrap()
                .publish(Vec::new(), removals)
                .await
                .unwrap();

            let before = commits.flushes();
            let started = Instant::now();
            let outcome = repository.collect().await.unwrap();
            deleting += started.elapsed().as_nanos();
            assert_eq!(outcome.removed.logical_objects, count, "{outcome:?}");
            assert!(outcome.removed.chunks + outcome.removed.payload_blobs >= count);
            let flushed = commits.flushes() - before;
            assert!(flushed > 0, "{count} payloads deleted without a flush");
            flushes_per_pass.insert(flushed);

            let before = commits.flushes();
            let started = Instant::now();
            let outcome = repository.collect().await.unwrap();
            idle += started.elapsed().as_nanos();
            assert_eq!(outcome.removed, Default::default(), "{outcome:?}");
            assert_eq!(
                commits.flushes(),
                before,
                "a pass that deleted nothing flushed"
            );
            let reopened = Repository::local(directory.path()).await.unwrap();
            assert!(reopened.fsck().await.unwrap().is_clean());
        }
        println!(
            "deletion_ordering_objects_{count}_collect_nanos {}",
            deleting / iterations as u128
        );
        println!(
            "deletion_ordering_objects_{count}_idle_collect_nanos {}",
            idle / iterations as u128
        );
        assert_eq!(
            flushes_per_pass.len(),
            1,
            "flush count varied across identical passes: {flushes_per_pass:?}"
        );
        println!(
            "deletion_ordering_objects_{count}_flushes_per_pass {}",
            flushes_per_pass.first().unwrap()
        );
    }
    println!("deletion_ordering_iterations {iterations}");
}
