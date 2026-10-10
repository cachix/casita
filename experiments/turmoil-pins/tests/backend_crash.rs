use casita_turmoil_pins_spike::backend_crash::{self, Scenario};
use std::path::Path;

fn executable() -> &'static Path {
    Path::new(env!("CARGO_BIN_EXE_casita-turmoil-pins-spike"))
}

#[test]
fn recover_after_writer_exits_without_cleanup() {
    let report = backend_crash::run(executable(), Scenario::SingleWriter, false).unwrap();
    assert!(report.writer_exit_without_cleanup && report.independent_reader);
}

#[test]
fn recover_competing_writers_after_process_death() {
    backend_crash::run(executable(), Scenario::CompetingWriters, false).unwrap();
}

#[test]
fn independent_reader_rejects_missing_marker() {
    let error = backend_crash::run(executable(), Scenario::SingleWriter, true).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("published root lacks matching durable operation marker"),
        "{error}"
    );
}
