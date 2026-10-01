//! One persistent dynamic graph per CLI invocation. Time `build` externally.
#[path = "../benches/support/dynamic.rs"]
#[allow(dead_code)] // The in-process profiler also uses this shared fixture.
mod dynamic;

use anyhow::{ensure, Result};
use dynamic::{Config, Fixture};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Instant;
use tracing::{span::Id, Subscriber};
use tracing_subscriber::{layer::Context, prelude::*, registry::LookupSpan, Layer};

#[derive(Clone, Default)]
struct Builds(Arc<AtomicUsize>, Arc<AtomicUsize>);

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Builds {
    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        match ctx.span(&id).unwrap().name() {
            "local_build" => {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
            "sandbox_prepared_dispatch" => {
                self.1.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }
}

fn main() -> Result<()> {
    let process_start = Instant::now();
    let args: Vec<_> = std::env::args().skip(1).collect();
    ensure!(
        args.len() == 7,
        "usage: dynamic-cli prepare|build|verify|run-memory|run-persistent ROOT NODES JOBS chain|wide on|off THREADS"
    );
    let phase = args[0].clone();
    ensure!(
        matches!(
            phase.as_str(),
            "prepare" | "build" | "verify" | "run-memory" | "run-persistent"
        ),
        "invalid phase"
    );
    let root = std::path::PathBuf::from(&args[1]);
    let nodes: usize = args[2].parse()?;
    let jobs: usize = args[3].parse()?;
    let shape = args[4].clone();
    ensure!(
        matches!(args[5].as_str(), "on" | "off"),
        "invalid sandbox mode"
    );
    let sandboxed = args[5] == "on";
    let threads: usize = args[6].parse()?;
    ensure!(
        nodes > 0 && nodes <= usize::MAX / 2 && jobs > 0 && threads > 0,
        "invalid counts"
    );
    let one_shot = matches!(phase.as_str(), "run-memory" | "run-persistent");
    if phase == "prepare" || one_shot {
        std::fs::create_dir(&root)?;
    }
    let builds = Builds::default();
    tracing_subscriber::registry()
        .with(
            builds
                .clone()
                .with_filter(tracing_subscriber::filter::filter_fn(|m| {
                    m.is_span() && matches!(m.name(), "local_build" | "sandbox_prepared_dispatch")
                })),
        )
        .init();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .thread_stack_size(8 * 1024 * 1024)
        .enable_all()
        .build()?;
    let run = async move {
        let setup_start = Instant::now();
        let config = Config {
            reserve: if sandboxed { 16 } else { 0 },
            preparations: 0,
            shared_mounts: true,
            prepared: sandboxed,
            unsandboxed: !sandboxed,
        };
        let fixture = if phase == "run-memory" {
            Fixture::memory_in(&root, nodes, jobs, config, &shape).await?
        } else {
            Fixture::persistent(
                &root,
                nodes,
                jobs,
                config,
                &shape,
                phase != "prepare" && !one_shot,
            )
            .await?
        };
        let setup_ms = setup_start.elapsed().as_secs_f64() * 1000.0;
        let mut graph_ms = 0.0;
        let mut verify_ms = 0.0;
        let mut paths = Vec::new();
        if phase != "prepare" {
            let (elapsed, results) = fixture.run().await?;
            graph_ms = elapsed.as_secs_f64() * 1000.0;
            paths = results
                .iter()
                .map(|r| r.artifact().store_path().to_owned())
                .collect();
            // Both one-shot backends verify inline; memory cannot be reopened after exit.
            if phase == "verify" || one_shot {
                let verify_start = Instant::now();
                fixture.verify(results).await?;
                verify_ms = verify_start.elapsed().as_secs_f64() * 1000.0;
            }
        }
        let actual = builds.0.load(Ordering::Relaxed);
        let dispatches = builds.1.load(Ordering::Relaxed);
        let expected = if phase == "build" || one_shot {
            2 * nodes
        } else {
            0
        };
        ensure!(
            actual == expected,
            "expected {expected} builds, got {actual}"
        );
        ensure!(
            dispatches == if sandboxed { expected } else { 0 },
            "incorrect prepared dispatch count"
        );
        let mut report = serde_json::json!({
            "phase": phase, "shape": shape, "sandboxed": sandboxed, "builds": actual,
            "prepared_dispatches": dispatches, "runtime_threads": threads, "jobs": jobs,
            "preparations": fixture.effective_preparations, "available_cpus": fixture.available_cpus,
            "outputs": paths, "setup_ms": setup_ms, "graph_ms": graph_ms, "verify_ms": verify_ms,
        });
        let drop_start = Instant::now();
        drop(fixture);
        report["fixture_drop_ms"] = serde_json::json!(drop_start.elapsed().as_secs_f64() * 1000.0);
        Ok::<_, anyhow::Error>(report)
    };
    let result: Result<_> = runtime.block_on(async { tokio::spawn(run).await? });
    let shutdown_start = Instant::now();
    // Durable pin releases must finish on both success and failure. Include
    // this work in shutdown and whole-process timing.
    let released = runtime.block_on(obrador_core::CasitaStore::flush_releases());
    drop(runtime);
    let mut report = result?;
    released?;
    report["runtime_shutdown_ms"] =
        serde_json::json!(shutdown_start.elapsed().as_secs_f64() * 1000.0);
    report["inside_process_ms"] = serde_json::json!(process_start.elapsed().as_secs_f64() * 1000.0);
    println!("{report}");
    Ok(())
}
