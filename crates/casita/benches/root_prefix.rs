//! Named-root prefix lookup with correctness gates on both sides of the
//! SQLite result-page boundary. Run through `benchmark all --suites root-prefix`.

use casita::{MetadataChange, Repository, RootName};
use serde_json::json;
use std::time::{Duration, Instant};

fn name(value: &str) -> RootName {
    value.try_into().unwrap()
}

fn median(mut samples: Vec<Duration>) -> f64 {
    samples.sort();
    samples[samples.len() / 2].as_secs_f64() * 1_000_000.0
}

async fn case(total: usize, matched: usize, iterations: usize) {
    let directory = tempfile::tempdir().unwrap();
    let repository = Repository::local(directory.path()).await.unwrap();
    let target = repository
        .import(casita::import::BlobImport::new(
            &b"root-prefix-payload"[..],
            name("payload"),
        ))
        .await
        .unwrap();
    let prefix = name("nix-referrer/0123456789abcdef");
    let changes = (0..total)
        .map(|index| MetadataChange::SetRoot {
            name: if index < matched {
                name(&format!("{prefix}/{index:05}"))
            } else {
                name(&format!("nix-path/{index:05}"))
            },
            target: target.clone(),
        })
        .collect();
    repository.commit(Vec::new(), changes).await.unwrap();
    let reader = repository.retained_reader().await.unwrap();
    let expected = reader.roots_under(&prefix).await.unwrap();
    assert_eq!(expected.len(), matched);
    assert!(expected.iter().all(|root| root.name().is_under(&prefix)));
    assert_eq!(reader.roots().await.unwrap().len(), total + 1);

    let mut prefix_samples = Vec::with_capacity(iterations);
    let mut all_samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        assert_eq!(reader.roots_under(&prefix).await.unwrap(), expected);
        prefix_samples.push(started.elapsed());

        let started = Instant::now();
        assert_eq!(reader.roots().await.unwrap().len(), total + 1);
        all_samples.push(started.elapsed());
    }
    drop(reader);
    assert!(repository.fsck().await.unwrap().is_clean());
    println!(
        "{}",
        json!({
            "total_roots": total + 1,
            "matched_roots": matched,
            "iterations": iterations,
            "prefix_p50_us": median(prefix_samples),
            "full_scan_p50_us": median(all_samples),
            "correctness": "passed",
        })
    );
}

fn main() {
    let iterations = std::env::var("CASITA_BENCH_ROOT_PREFIX_ITERATIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(5);
    assert!(iterations > 0);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        case(255, 255, iterations).await;
        case(257, 257, iterations).await;
        case(4096, 8, iterations).await;
        case(4096, 257, iterations).await;
    });
}
