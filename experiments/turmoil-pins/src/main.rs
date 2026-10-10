use casita_turmoil_pins_spike::{Faults, Scenario, chunk_cases, repository_cases, run};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let command = args.next().unwrap_or_else(|| "corpus".into());
    if command == "client-journal-worker" {
        let mode = args.next().ok_or("missing worker mode")?;
        let work = args.next().ok_or("missing worker directory")?;
        let boundary = casita_turmoil_pins_spike::client_journal::Boundary::parse(
            &args.next().ok_or("missing boundary")?,
        )?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        return runtime.block_on(casita_turmoil_pins_spike::client_journal::worker(
            &mode,
            std::path::Path::new(&work),
            boundary,
        ));
    }
    if command == "backend-crash-worker" {
        let mode = args.next().ok_or("missing worker mode")?;
        let work = args.next().ok_or("missing worker directory")?;
        let scenario = casita_turmoil_pins_spike::backend_crash::Scenario::parse(
            &args.next().ok_or("missing worker scenario")?,
        )?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        return runtime.block_on(casita_turmoil_pins_spike::backend_crash::worker(
            &mode,
            std::path::Path::new(&work),
            scenario,
        ));
    }
    let value = args.next().unwrap_or_else(|| "64".into()).parse::<u64>()?;
    match command.as_str() {
        "partial-restore-corpus" => {
            let mut reports = Vec::new();
            for seed in 0..value {
                for scenario in chunk_cases::Scenario::CANCELLATIONS {
                    for almost_complete in [false, true] {
                        let first =
                            chunk_cases::run_partial_restore(seed, scenario, almost_complete)?;
                        if first
                            != chunk_cases::run_partial_restore(seed, scenario, almost_complete)?
                        {
                            return Err(format!("partial restore replay diverged: seed={seed}, {}, almost_complete={almost_complete}", scenario.name()).into());
                        }
                        reports.push(first);
                    }
                }
            }
            println!(
                "{} partial restore cases passed, each replayed; reports_digest={}",
                reports.len(),
                casita::experimental::Digest::hash(&serde_json::to_vec(&reports)?)
            );
        }
        "native-retention" => {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            for _ in 0..value {
                println!(
                    "{}",
                    serde_json::to_string(
                        &runtime
                            .block_on(casita_turmoil_pins_spike::retention::run(false, false))?
                    )?
                );
            }
        }
        "native-fencing" => {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            for _ in 0..value {
                println!(
                    "{}",
                    serde_json::to_string(
                        &runtime.block_on(casita_turmoil_pins_spike::fencing::run(false))?
                    )?
                );
            }
        }
        "journal-read-save-outage-corpus" => {
            use casita_turmoil_pins_spike::{
                intent_journal::{Cut, Failure, ReadFailure, Stage},
                marker_rpc,
            };
            let mut reports = Vec::new();
            for seed in 0..value {
                for scenario in marker_rpc::Scenario::CLIENT_RESTARTS {
                    for read in ReadFailure::ALL {
                        for save in Stage::ALL
                            .into_iter()
                            .map(Failure::Before)
                            .chain(Cut::ALL.into_iter().map(Failure::Partial))
                        {
                            for early in [true, false] {
                                let first = marker_rpc::run_journal_read_save_outage(
                                    seed, scenario, read, save, early,
                                )?;
                                let second = marker_rpc::run_journal_read_save_outage(
                                    seed, scenario, read, save, early,
                                )?;
                                if first != second {
                                    return Err(format!("combined save outage replay diverged: seed={seed}, {}, {}, {}, early={early}", scenario.name(), read.name(), save.name()).into());
                                }
                                reports.push(first);
                            }
                        }
                    }
                }
            }
            println!(
                "{} combined save outage cases passed, each replayed; reports_digest={}",
                reports.len(),
                casita::experimental::Digest::hash(&serde_json::to_vec(&reports)?)
            );
        }
        "journal-restarted-outage-corpus"
        | "journal-read-save-outage-crash-corpus"
        | "journal-read-save-crash-corpus"
        | "journal-read-save-corpus" => {
            use casita_turmoil_pins_spike::{
                intent_journal::{Cut, Failure, ReadFailure, Stage},
                marker_rpc,
            };
            let mut reports = Vec::new();
            for seed in 0..value {
                for scenario in marker_rpc::Scenario::CLIENT_RESTARTS {
                    for read in ReadFailure::ALL {
                        for save in Stage::ALL
                            .into_iter()
                            .map(Failure::Before)
                            .chain(Cut::ALL.into_iter().map(Failure::Partial))
                        {
                            let run = if command == "journal-restarted-outage-corpus" {
                                marker_rpc::run_journal_restarted_outage
                            } else if command == "journal-read-save-outage-crash-corpus" {
                                marker_rpc::run_journal_read_save_outage_crash
                            } else if command == "journal-read-save-crash-corpus" {
                                marker_rpc::run_journal_read_save_crash
                            } else {
                                marker_rpc::run_journal_read_save_failure
                            };
                            let first = run(seed, scenario, read, save)?;
                            let second = run(seed, scenario, read, save)?;
                            if first != second {
                                return Err(format!(
                                    "combined journal replay diverged: seed={seed}, {}, {}, {}",
                                    scenario.name(),
                                    read.name(),
                                    save.name()
                                )
                                .into());
                            }
                            reports.push(first);
                        }
                    }
                }
            }
            println!(
                "{} {} cases passed, each replayed; reports_digest={}",
                reports.len(),
                command,
                casita::experimental::Digest::hash(&serde_json::to_vec(&reports)?)
            );
        }
        "journal-read-corpus" => {
            use casita_turmoil_pins_spike::{intent_journal::ReadFailure, marker_rpc};
            let mut reports = Vec::new();
            for seed in 0..value {
                for scenario in marker_rpc::Scenario::CLIENT_RESTARTS {
                    for failure in ReadFailure::ALL {
                        let first = marker_rpc::run_journal_read_failure(seed, scenario, failure)?;
                        let second = marker_rpc::run_journal_read_failure(seed, scenario, failure)?;
                        if first != second {
                            return Err(format!(
                                "journal read replay diverged: seed={seed}, {}, {}",
                                scenario.name(),
                                failure.name()
                            )
                            .into());
                        }
                        reports.push(first);
                    }
                }
            }
            println!(
                "{} journal read cases passed, each replayed; reports_digest={}",
                reports.len(),
                casita::experimental::Digest::hash(&serde_json::to_vec(&reports)?)
            );
        }
        "client-journal-submission" => {
            use casita_turmoil_pins_spike::client_journal::{Boundary, overlap};
            let executable = std::env::current_exe()?;
            for _ in 0..value {
                for boundary in [Boundary::Submitted, Boundary::Committed, Boundary::Unknown] {
                    println!(
                        "{}",
                        serde_json::to_string(&overlap::run_submission(
                            &executable,
                            boundary,
                            false,
                            true,
                            false
                        )?)?
                    );
                }
                for boundary in [Boundary::Submitted, Boundary::Unknown, Boundary::Recovered] {
                    for temporary in [false, true] {
                        println!(
                            "{}",
                            serde_json::to_string(&overlap::run_submission(
                                &executable,
                                boundary,
                                temporary,
                                false,
                                false
                            )?)?
                        );
                    }
                }
            }
        }
        "client-journal-overlap" => {
            let executable = std::env::current_exe()?;
            for _ in 0..value {
                for boundary in [
                    casita_turmoil_pins_spike::client_journal::Boundary::Submitted,
                    casita_turmoil_pins_spike::client_journal::Boundary::Unknown,
                    casita_turmoil_pins_spike::client_journal::Boundary::Recovered,
                ] {
                    for temporary in [false, true] {
                        println!(
                            "{}",
                            serde_json::to_string(
                                &casita_turmoil_pins_spike::client_journal::overlap::run(
                                    &executable,
                                    boundary,
                                    temporary,
                                    false
                                )?
                            )?
                        );
                    }
                }
            }
        }
        "journal-writer-corpus" => {
            let mut reports = Vec::new();
            for seed in 0..value {
                for stage in 0..4 {
                    let first = casita_turmoil_pins_spike::journal_writers::run(seed, stage)?;
                    let second = casita_turmoil_pins_spike::journal_writers::run(seed, stage)?;
                    if first != second {
                        return Err(format!(
                            "journal writer replay diverged: seed={seed}, stage={stage}"
                        )
                        .into());
                    }
                    reports.push(first);
                }
            }
            println!(
                "{} journal writer cases passed, each replayed; reports_digest={}",
                reports.len(),
                casita::experimental::Digest::hash(&serde_json::to_vec(&reports)?)
            );
        }
        "journal-failure-corpus" | "journal-error-crash-corpus" => {
            let run = if command == "journal-error-crash-corpus" {
                casita_turmoil_pins_spike::marker_rpc::run_journal_error_crash
            } else {
                casita_turmoil_pins_spike::marker_rpc::run_journal_failure
            };
            let mut reports = Vec::new();
            for seed in 0..value {
                for scenario in casita_turmoil_pins_spike::marker_rpc::Scenario::CLIENT_RESTARTS {
                    for stage in casita_turmoil_pins_spike::intent_journal::Stage::ALL {
                        for write in 1..=3 {
                            let first = run(seed, scenario, stage, write)?;
                            let second = run(seed, scenario, stage, write)?;
                            if first != second {
                                return Err(format!("journal failure replay diverged: seed={seed}, {}, {stage:?}, save={write}", scenario.name()).into());
                            }
                            reports.push(first);
                        }
                    }
                }
            }
            println!(
                "{} {} cases passed, each replayed; reports_digest={}",
                reports.len(),
                command,
                casita::experimental::Digest::hash(&serde_json::to_vec(&reports)?)
            );
        }
        "persistent-journal-corpus" => {
            use casita_turmoil_pins_spike::intent_journal::{Cut, Failure, Stage};
            let mut reports = Vec::new();
            for seed in 0..value {
                for scenario in casita_turmoil_pins_spike::marker_rpc::Scenario::CLIENT_RESTARTS {
                    for failure in Stage::ALL
                        .into_iter()
                        .map(Failure::Before)
                        .chain(Cut::ALL.into_iter().map(Failure::Partial))
                    {
                        for save in 1..=3 {
                            for early in [true, false] {
                                let first =
                                    casita_turmoil_pins_spike::marker_rpc::run_persistent_journal(
                                        seed, scenario, failure, save, early,
                                    )?;
                                let second =
                                    casita_turmoil_pins_spike::marker_rpc::run_persistent_journal(
                                        seed, scenario, failure, save, early,
                                    )?;
                                if first != second {
                                    return Err(format!("persistent journal replay diverged: seed={seed}, {}, {}, save={save}, early_repair={early}",scenario.name(),failure.name()).into());
                                }
                                reports.push(first);
                            }
                        }
                    }
                }
            }
            println!(
                "{} persistent journal cases passed, each replayed; reports_digest={}",
                reports.len(),
                casita::experimental::Digest::hash(&serde_json::to_vec(&reports)?)
            );
        }
        "partial-journal-corpus" => {
            let mut reports = Vec::new();
            for seed in 0..value {
                for scenario in casita_turmoil_pins_spike::marker_rpc::Scenario::CLIENT_RESTARTS {
                    for cut in casita_turmoil_pins_spike::intent_journal::Cut::ALL {
                        for save in 1..=3 {
                            for crash in [false, true] {
                                let first =
                                    casita_turmoil_pins_spike::marker_rpc::run_partial_write(
                                        seed, scenario, cut, save, crash,
                                    )?;
                                let second =
                                    casita_turmoil_pins_spike::marker_rpc::run_partial_write(
                                        seed, scenario, cut, save, crash,
                                    )?;
                                if first != second {
                                    return Err(format!("partial journal replay diverged: seed={seed}, {}, {cut:?}, save={save}, crash={crash}", scenario.name()).into());
                                }
                                reports.push(first);
                            }
                        }
                    }
                }
            }
            println!(
                "{} partial journal cases passed, each replayed; reports_digest={}",
                reports.len(),
                casita::experimental::Digest::hash(&serde_json::to_vec(&reports)?)
            );
        }
        "client-restart-corpus" => {
            for seed in 0..value {
                for scenario in casita_turmoil_pins_spike::marker_rpc::Scenario::CLIENT_RESTARTS {
                    let first = casita_turmoil_pins_spike::marker_rpc::run(seed, scenario)?;
                    let second = casita_turmoil_pins_spike::marker_rpc::run(seed, scenario)?;
                    if first != second {
                        return Err(format!(
                            "client restart replay diverged: seed={seed}, {}",
                            scenario.name()
                        )
                        .into());
                    }
                }
            }
            println!(
                "{} client restart scenarios passed, each replayed",
                value
                    * casita_turmoil_pins_spike::marker_rpc::Scenario::CLIENT_RESTARTS.len() as u64
            );
        }
        "partition-corpus" => {
            for seed in 0..value {
                for scenario in casita_turmoil_pins_spike::marker_rpc::Scenario::PARTITIONS {
                    let first = casita_turmoil_pins_spike::marker_rpc::run(seed, scenario)?;
                    let second = casita_turmoil_pins_spike::marker_rpc::run(seed, scenario)?;
                    if first != second {
                        return Err(format!(
                            "partition replay diverged: seed={seed}, {}",
                            scenario.name()
                        )
                        .into());
                    }
                }
            }
            println!(
                "{} partition and deadline scenarios passed, each replayed",
                value * casita_turmoil_pins_spike::marker_rpc::Scenario::PARTITIONS.len() as u64
            );
        }
        "marker-corpus" => {
            for seed in 0..value {
                for scenario in casita_turmoil_pins_spike::marker_rpc::Scenario::ALL {
                    let first = casita_turmoil_pins_spike::marker_rpc::run(seed, scenario)?;
                    let second = casita_turmoil_pins_spike::marker_rpc::run(seed, scenario)?;
                    if first != second {
                        return Err(format!(
                            "marker replay diverged: seed={seed}, {}",
                            scenario.name()
                        )
                        .into());
                    }
                }
            }
            println!(
                "{} marker protocol scenarios passed, each replayed",
                value * casita_turmoil_pins_spike::marker_rpc::Scenario::ALL.len() as u64
            );
        }
        "client-journal-read-failure" => {
            use casita_turmoil_pins_spike::{
                client_journal::{self, Boundary},
                intent_journal::ReadFailure,
            };
            let executable = std::env::current_exe()?;
            for _ in 0..value {
                for boundary in [Boundary::Submitted, Boundary::Unknown, Boundary::Recovered] {
                    for failure in ReadFailure::ALL {
                        println!(
                            "{}",
                            serde_json::to_string(&client_journal::run_read_failure(
                                &executable,
                                boundary,
                                failure,
                            )?)?
                        );
                    }
                }
            }
        }
        "client-journal-persistent-crash" => {
            use casita_turmoil_pins_spike::intent_journal::{Cut, Failure, Stage};
            let executable = std::env::current_exe()?;
            for _ in 0..value {
                for failure in Stage::ALL
                    .into_iter()
                    .map(Failure::Before)
                    .chain(Cut::ALL.into_iter().map(Failure::Partial))
                {
                    for save in 1..=3 {
                        let report =
                            casita_turmoil_pins_spike::client_journal::run_persistent_crash(
                                &executable,
                                failure,
                                save,
                            )?;
                        println!("{}", serde_json::to_string(&report)?);
                    }
                }
            }
        }
        "client-journal-partial-crash" => {
            let executable = std::env::current_exe()?;
            for _ in 0..value {
                for cut in casita_turmoil_pins_spike::intent_journal::Cut::ALL {
                    for save in 1..=3 {
                        let report =
                            casita_turmoil_pins_spike::client_journal::run_partial_write_crash(
                                &executable,
                                cut,
                                save,
                            )?;
                        println!("{}", serde_json::to_string(&report)?);
                    }
                }
            }
        }
        "client-journal-error-crash" => {
            let executable = std::env::current_exe()?;
            for _ in 0..value {
                for stage in casita_turmoil_pins_spike::intent_journal::Stage::ALL {
                    for save in 1..=3 {
                        let report = casita_turmoil_pins_spike::client_journal::run_error_crash(
                            &executable,
                            stage,
                            save,
                        )?;
                        println!("{}", serde_json::to_string(&report)?);
                    }
                }
            }
        }
        "client-journal" => {
            let executable = std::env::current_exe()?;
            for _ in 0..value {
                for boundary in casita_turmoil_pins_spike::client_journal::Boundary::ALL {
                    let report = casita_turmoil_pins_spike::client_journal::run(
                        &executable,
                        boundary,
                        casita_turmoil_pins_spike::client_journal::Fault::None,
                    )?;
                    println!("{}", serde_json::to_string(&report)?);
                }
            }
        }
        "backend-crash" => {
            let executable = std::env::current_exe()?;
            for _ in 0..value {
                for scenario in casita_turmoil_pins_spike::backend_crash::Scenario::ALL {
                    let report = casita_turmoil_pins_spike::backend_crash::run(
                        &executable,
                        scenario,
                        false,
                    )?;
                    println!("{}", serde_json::to_string(&report)?);
                }
            }
        }
        "backend-marker" => {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            for _ in 0..value {
                for scenario in casita_turmoil_pins_spike::backend_marker::Scenario::ALL {
                    let report = runtime
                        .block_on(casita_turmoil_pins_spike::backend_marker::run(scenario))?;
                    println!("{}", serde_json::to_string(&report)?);
                }
            }
        }
        "corpus" => {
            let mut outcomes = [0; 2];
            for seed in 0..value {
                for scenario in Scenario::ALL {
                    let first = run(seed, scenario, Faults::default())?;
                    let second = run(seed, scenario, Faults::default())?;
                    if cfg!(feature = "explicit-entropy") && first != second {
                        return Err(format!(
                            "full replay diverged: seed={seed}, {}",
                            scenario.name()
                        )
                        .into());
                    }
                    if first.semantic_replay() != second.semantic_replay() {
                        return Err(format!(
                            "semantic replay diverged: seed={seed}, {}",
                            scenario.name()
                        )
                        .into());
                    }
                    if scenario == Scenario::PinVsDeletion {
                        outcomes[usize::from(first.deletion_admitted)] += 1;
                    }
                }
                for scenario in repository_cases::Scenario::ALL {
                    let first = repository_cases::run(seed, scenario, false)?;
                    let second = repository_cases::run(seed, scenario, false)?;
                    if first != second {
                        return Err(format!(
                            "repository replay diverged: seed={seed}, {}",
                            scenario.name()
                        )
                        .into());
                    }
                }
                for scenario in casita_turmoil_pins_spike::marker_rpc::Scenario::ALL {
                    let first = casita_turmoil_pins_spike::marker_rpc::run(seed, scenario)?;
                    let second = casita_turmoil_pins_spike::marker_rpc::run(seed, scenario)?;
                    if first != second {
                        return Err(format!(
                            "marker replay diverged: seed={seed}, {}",
                            scenario.name()
                        )
                        .into());
                    }
                }
            }
            run_chunk_corpus(value, &chunk_cases::Scenario::ALL)?;
            println!(
                "{} scenarios passed, each replayed; pin-vs-deletion: writer={} deletion={}",
                value
                    * (Scenario::ALL.len()
                        + repository_cases::Scenario::ALL.len()
                        + casita_turmoil_pins_spike::marker_rpc::Scenario::ALL.len()
                        + chunk_cases::Scenario::ALL.len()) as u64,
                outcomes[0],
                outcomes[1]
            );
        }
        "chunk-corpus" => {
            run_chunk_corpus(value, &chunk_cases::Scenario::ALL)?;
            println!(
                "{} chunk scenarios passed, each replayed",
                value * chunk_cases::Scenario::ALL.len() as u64
            );
        }
        "writer-corpus" => {
            run_chunk_corpus(value, &chunk_cases::Scenario::WRITERS)?;
            println!(
                "{} competing-writer scenarios passed, each replayed",
                value * chunk_cases::Scenario::WRITERS.len() as u64
            );
        }
        "cancel-corpus" => {
            run_chunk_corpus(value, &chunk_cases::Scenario::CANCELLATIONS)?;
            println!(
                "{} writer-cancellation scenarios passed, each replayed",
                value * chunk_cases::Scenario::CANCELLATIONS.len() as u64
            );
        }
        "network-corpus" => {
            for seed in 0..value {
                for scenario in repository_cases::Scenario::NETWORK {
                    let first = repository_cases::run(seed, scenario, false)?;
                    let second = repository_cases::run(seed, scenario, false)?;
                    if first != second {
                        return Err(format!(
                            "network replay diverged: seed={seed}, {}",
                            scenario.name()
                        )
                        .into());
                    }
                }
            }
            println!(
                "{} network metadata scenarios passed, each replayed",
                value * repository_cases::Scenario::NETWORK.len() as u64
            );
        }
        "restart-corpus" => {
            for seed in 0..value {
                for scenario in repository_cases::Scenario::RESTARTS {
                    let first = repository_cases::run(seed, scenario, false)?;
                    let second = repository_cases::run(seed, scenario, false)?;
                    if first != second {
                        return Err(format!(
                            "restart replay diverged: seed={seed}, {}",
                            scenario.name()
                        )
                        .into());
                    }
                }
            }
            println!(
                "{} metadata restart scenarios passed, each replayed",
                value * repository_cases::Scenario::RESTARTS.len() as u64
            );
        }
        "seed" => {
            let name = args.next().unwrap_or_else(|| "lost-response".into());
            if let Some(scenario) = casita_turmoil_pins_spike::marker_rpc::Scenario::ALL
                .into_iter()
                .find(|s| s.name() == name)
            {
                let first = casita_turmoil_pins_spike::marker_rpc::run(value, scenario)?;
                let second = casita_turmoil_pins_spike::marker_rpc::run(value, scenario)?;
                println!("{}", serde_json::to_string_pretty(&first)?);
                println!("full_replay={}", first == second);
                return Ok(());
            }
            if let Some(scenario) = chunk_cases::Scenario::ALL
                .into_iter()
                .find(|s| s.name() == name)
            {
                let first = chunk_cases::run(value, scenario, false)?;
                let second = chunk_cases::run(value, scenario, false)?;
                println!("{}", serde_json::to_string_pretty(&first)?);
                println!("full_replay={}", first == second);
                return Ok(());
            }
            if let Some(scenario) = repository_cases::Scenario::ALL
                .into_iter()
                .find(|s| s.name() == name)
            {
                let first = repository_cases::run(value, scenario, false)?;
                let second = repository_cases::run(value, scenario, false)?;
                println!("{}", serde_json::to_string_pretty(&first)?);
                println!("full_replay={}", first == second);
                return Ok(());
            }
            let scenario = Scenario::ALL
                .into_iter()
                .find(|scenario| scenario.name() == name)
                .ok_or("unknown scenario")?;
            let first = run(value, scenario, Faults::default())?;
            let second = run(value, scenario, Faults::default())?;
            println!(
                "{}",
                serde_json::to_string_pretty(&first.semantic_replay())?
            );
            println!(
                "semantic_replay={} byte_replay={}",
                first.semantic_replay() == second.semantic_replay(),
                first.ledger_bytes == second.ledger_bytes
            );
        }
        _ => {
            return Err(
                "usage: casita-turmoil-pins-spike [native-retention COUNT | partial-restore-corpus COUNT | native-fencing COUNT | journal-restarted-outage-corpus COUNT | journal-read-save-outage-crash-corpus COUNT | journal-read-save-outage-corpus COUNT | journal-read-save-crash-corpus COUNT | journal-read-save-corpus COUNT | journal-read-corpus COUNT | client-journal-submission COUNT | client-journal-overlap COUNT | journal-writer-corpus COUNT | client-journal-read-failure COUNT | persistent-journal-corpus COUNT | client-journal-persistent-crash COUNT | partial-journal-corpus COUNT | client-journal-partial-crash COUNT | journal-error-crash-corpus COUNT | client-journal-error-crash COUNT | journal-failure-corpus COUNT | client-journal COUNT | client-restart-corpus COUNT | partition-corpus COUNT | marker-corpus COUNT | backend-crash COUNT | backend-marker COUNT | corpus COUNT | chunk-corpus COUNT | writer-corpus COUNT | cancel-corpus COUNT | network-corpus COUNT | restart-corpus COUNT | seed SEED SCENARIO]".into(),
            );
        }
    }
    Ok(())
}

fn run_chunk_corpus(
    count: u64,
    scenarios: &[chunk_cases::Scenario],
) -> Result<(), Box<dyn std::error::Error>> {
    for seed in 0..count {
        for &scenario in scenarios {
            let first = chunk_cases::run(seed, scenario, false)?;
            let second = chunk_cases::run(seed, scenario, false)?;
            if first != second {
                return Err(
                    format!("chunk replay diverged: seed={seed}, {}", scenario.name()).into(),
                );
            }
        }
    }
    Ok(())
}
