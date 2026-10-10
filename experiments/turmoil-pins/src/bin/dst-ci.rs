//! Small CI entrypoint. Full fault matrices and native probes stay in the spike CLI.
use casita_turmoil_pins_spike::{
    Faults, Scenario, chunk_cases,
    harness::Corpus,
    intent_journal::{Failure, ReadFailure, Stage},
    marker_rpc, repository_cases,
};
use std::path::Path;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    if !cfg!(feature = "explicit-entropy") {
        return Err("dst-ci requires explicit-entropy for complete replay".into());
    }
    let mut args = std::env::args().skip(1);
    let seeds: u64 = args.next().unwrap_or_else(|| "4".into()).parse()?;
    if !(1..=64).contains(&seeds) {
        return Err("seed count must be between 1 and 64".into());
    }
    let artifact = args
        .next()
        .unwrap_or_else(|| "target/dst/reports.jsonl".into());
    if args.next().is_some() {
        return Err("usage: dst-ci [SEEDS=4] [REPORTS_JSONL]".into());
    }
    let mut corpus = Corpus::create(Path::new(&artifact))?;
    for seed in 0..seeds {
        for scenario in Scenario::ALL {
            corpus.check(seed, scenario.name(), || {
                casita_turmoil_pins_spike::run(seed, scenario, Faults::default())
            })?;
        }
        for scenario in repository_cases::Scenario::ALL {
            corpus.check(seed, scenario.name(), || {
                repository_cases::run(seed, scenario, false)
            })?;
        }
        for scenario in marker_rpc::Scenario::ALL {
            corpus.check(seed, scenario.name(), || marker_rpc::run(seed, scenario))?;
        }
        for scenario in chunk_cases::Scenario::ALL {
            corpus.check(seed, scenario.name(), || {
                chunk_cases::run(seed, scenario, false)
            })?;
        }
        for scenario in chunk_cases::Scenario::CANCELLATIONS {
            for almost_complete in [false, true] {
                corpus.check(
                    seed,
                    &format!("partial-restore/{}/{almost_complete}", scenario.name()),
                    || chunk_cases::run_partial_restore(seed, scenario, almost_complete),
                )?;
            }
        }
        // Both restart boundaries, every read failure and every save stage.
        // Broader partial-write matrices are also covered by the library tests.
        for scenario in marker_rpc::Scenario::CLIENT_RESTARTS {
            for read in ReadFailure::ALL {
                for stage in Stage::ALL {
                    corpus.check(
                        seed,
                        &format!("restarted-outage/{}/{read:?}/{stage:?}", scenario.name()),
                        || {
                            marker_rpc::run_journal_restarted_outage(
                                seed,
                                scenario,
                                read,
                                Failure::Before(stage),
                            )
                        },
                    )?;
                }
            }
        }
    }
    println!("{}", serde_json::to_string(&corpus.summary()?)?);
    Ok(())
}
