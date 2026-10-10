//! Concurrent replacement schedules, with exclusive journal paths per writer.
use crate::intent_journal::{self, Intent, MemoryStorage, Phase, Stage, Storage};
use serde::Serialize;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct Report {
    pub schedule: Vec<(usize, usize, u128)>,
    pub crash_after: usize,
    pub surviving: Vec<String>,
    pub recovered: Vec<String>,
}
fn initial(writer: usize) -> Result<Intent<String>> {
    Intent::new(format!("operation-{writer}:payload-{writer}"))
}
fn check(storage: &dyn Storage, writer: usize, expected_phase: Phase) -> Result<String> {
    let intent: Intent<String> = intent_journal::load(storage)?;
    if intent.request != initial(writer)?.request || intent.phase != expected_phase {
        return Err(format!("writer {writer} journal lost identity or phase").into());
    }
    Ok(String::from_utf8(
        storage.read()?.ok_or("journal missing")?,
    )?)
}
fn exercise(
    mut stores: [Box<dyn Storage>; 2],
    schedule: Vec<(usize, usize, u128)>,
    crash_after: usize,
) -> Result<Report> {
    let mut intents = [initial(0)?, initial(1)?];
    for writer in 0..2 {
        intent_journal::persist(&mut *stores[writer], &intents[writer], |_| Ok(()))?;
        intents[writer].mark_unknown()?;
    }
    for &(writer, index, _) in &schedule {
        // The abandoned writer performs no cleanup or subsequent storage stages.
        if writer == 0 && index > crash_after {
            continue;
        }
        let bytes = serde_json::to_vec(&intents[writer])?;
        stores[writer].apply(Stage::ALL[index], &bytes)?;
        if index == 3 {
            check(&*stores[writer], writer, Phase::Unknown)?;
        }
    }
    let surviving = vec![
        check(
            &*stores[0],
            0,
            if crash_after < 2 {
                Phase::Submitted
            } else {
                Phase::Unknown
            },
        )?,
        check(&*stores[1], 1, Phase::Unknown)?,
    ];
    let mut recovered = Vec::new();
    for (writer, store) in stores.iter_mut().enumerate() {
        // Reload only the current record, even when an abandoned temporary file exists.
        let mut loaded: Intent<String> = intent_journal::load(&**store)?;
        loaded.mark_recovered(&initial(writer)?.fingerprint, format!("revision-{writer}"))?;
        intent_journal::persist(&mut **store, &loaded, |_| Ok(()))?;
        recovered.push(check(
            &**store,
            writer,
            Phase::Recovered {
                observed_revision: format!("revision-{writer}"),
            },
        )?);
    }
    Ok(Report {
        schedule,
        crash_after,
        surviving,
        recovered,
    })
}
/// Turmoil chooses stage order and virtual timing; actual files are checked separately.
pub fn run(seed: u64, crash_after: usize) -> Result<Report> {
    if crash_after >= Stage::ALL.len() {
        return Err("crash stage must be 0..4".into());
    }
    let schedule = Arc::new(Mutex::new(Vec::new()));
    let mut sim = turmoil::Builder::new()
        .rng_seed(seed)
        .enable_random_order()
        .simulation_duration(Duration::from_secs(1))
        .build();
    for writer in 0..2 {
        let trace = schedule.clone();
        sim.client(format!("writer-{writer}"), async move {
            for stage in 0..4 {
                tokio::time::sleep(Duration::from_millis(1)).await;
                trace
                    .lock()
                    .unwrap()
                    .push((writer, stage, turmoil::elapsed().as_nanos()));
            }
            Ok(())
        });
    }
    sim.run()?;
    let trace = schedule.lock().unwrap().clone();
    exercise(
        [
            Box::<MemoryStorage>::default(),
            Box::<MemoryStorage>::default(),
        ],
        trace,
        crash_after,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_journal::FileStorage;
    fn schedules(
        prefix: &mut Vec<(usize, usize, u128)>,
        counts: [usize; 2],
        out: &mut Vec<Vec<(usize, usize, u128)>>,
    ) {
        if counts == [4, 4] {
            out.push(prefix.clone());
            return;
        }
        for writer in 0..2 {
            if counts[writer] == 4 {
                continue;
            }
            prefix.push((writer, counts[writer], prefix.len() as u128));
            let mut next = counts;
            next[writer] += 1;
            schedules(prefix, next, out);
            prefix.pop();
        }
    }
    #[test]
    fn every_two_writer_stage_interleaving_preserves_separate_native_journals() {
        let mut all = Vec::new();
        schedules(&mut Vec::new(), [0, 0], &mut all);
        assert_eq!(all.len(), 70);
        for trace in all {
            for crash_after in 0..4 {
                let modeled = exercise(
                    [
                        Box::<MemoryStorage>::default(),
                        Box::<MemoryStorage>::default(),
                    ],
                    trace.clone(),
                    crash_after,
                )
                .unwrap();
                let directory = tempfile::tempdir().unwrap();
                let stores = std::array::from_fn(|writer| {
                    let path = directory.path().join(format!("writer-{writer}"));
                    std::fs::create_dir(&path).unwrap();
                    Box::new(FileStorage { directory: path }) as Box<dyn Storage>
                });
                assert_eq!(
                    modeled,
                    exercise(stores, trace.clone(), crash_after).unwrap()
                );
            }
        }
    }
    #[test]
    fn seeded_writer_stage_schedules_replay_complete_reports() {
        let mut orders = std::collections::BTreeSet::new();
        for seed in 0..64 {
            for stage in 0..4 {
                let first = run(seed, stage).unwrap();
                assert_eq!(
                    first,
                    run(seed, stage).unwrap(),
                    "seed={seed}, stage={stage}"
                );
                orders.insert(
                    first
                        .schedule
                        .iter()
                        .map(|&(writer, _, _)| writer)
                        .collect::<Vec<_>>(),
                );
            }
        }
        assert!(orders.len() > 1, "corpus never varied writer ordering");
    }
    fn shared_path_negative_control(mut store: Box<dyn Storage>) {
        let first = initial(0).unwrap();
        let second = initial(1).unwrap();
        let first_bytes = serde_json::to_vec(&first).unwrap();
        let second_bytes = serde_json::to_vec(&second).unwrap();
        store.apply(Stage::Write, &first_bytes).unwrap();
        store.apply(Stage::Write, &second_bytes).unwrap();
        // Writer 0 now syncs and renames writer 1's temporary file successfully.
        for stage in [Stage::SyncFile, Stage::Rename, Stage::SyncDirectory] {
            store.apply(stage, &first_bytes).unwrap();
        }
        let error = check(&*store, 0, Phase::Submitted).unwrap_err();
        assert!(error.to_string().contains("lost identity"), "{error}");
        check(&*store, 1, Phase::Submitted).unwrap();
    }
    #[test]
    fn checker_rejects_valid_but_wrong_identity_from_shared_temporary_path() {
        shared_path_negative_control(Box::<MemoryStorage>::default());
        let directory = tempfile::tempdir().unwrap();
        shared_path_negative_control(Box::new(FileStorage {
            directory: directory.path().into(),
        }));
    }
}
