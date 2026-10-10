use casita_turmoil_pins_spike::client_journal::{self, Boundary, Fault};
use std::path::Path;
fn executable() -> &'static Path {
    Path::new(env!("CARGO_BIN_EXE_casita-turmoil-pins-spike"))
}
#[test]
fn native_client_intent_survives_six_kill_boundaries() {
    for boundary in Boundary::ALL {
        let report = client_journal::run(executable(), boundary, Fault::None).unwrap();
        assert!(
            report.killed_without_cleanup
                && report.independent_recovery
                && report.terminal_journal_verified
                && report.recovery_left_revision_unchanged
        );
        assert_eq!(report.recovered, !matches!(boundary, Boundary::Submitted));
        assert_eq!(report.exact_payload, report.recovered);
        let expected_phase = match boundary {
            Boundary::Submitted | Boundary::Committed => "Submitted",
            Boundary::Unknown | Boundary::RecoveryTemp => "Unknown",
            Boundary::RecoveryRenamed | Boundary::Recovered => "Recovered",
        };
        assert!(
            report.initial_phase.starts_with(expected_phase),
            "{report:?}"
        );
    }
}
#[test]
fn recovery_rejects_missing_durable_identity() {
    let error =
        client_journal::run(executable(), Boundary::Unknown, Fault::MissingIntent).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("durable client intent unavailable"),
        "{error}"
    );
}
#[test]
fn recovery_rejects_changed_durable_identity() {
    let error =
        client_journal::run(executable(), Boundary::Unknown, Fault::ChangedIdentity).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("client intent identity mismatch"),
        "{error}"
    );
}
#[test]
fn native_client_crashes_after_error_before_journal_retry() {
    for stage in casita_turmoil_pins_spike::intent_journal::Stage::ALL {
        for save in 1..=3 {
            let report = client_journal::run_error_crash(executable(), stage, save).unwrap();
            assert!(
                report.recovery.killed_without_cleanup
                    && report.recovery.independent_recovery
                    && report.recovery.terminal_journal_verified
            );
            assert_eq!(
                report.missing_intent_stopped,
                save == 1
                    && stage != casita_turmoil_pins_spike::intent_journal::Stage::SyncDirectory
            );
        }
    }
}
#[test]
fn native_partial_write_then_sigkill_ignores_truncated_temporary_file() {
    for cut in casita_turmoil_pins_spike::intent_journal::Cut::ALL {
        for save in 1..=3 {
            let report = client_journal::run_partial_write_crash(executable(), cut, save).unwrap();
            assert!(report.partial_json_invalid);
            assert!(report.partial_len.unwrap() < report.partial_full_len.unwrap());
            assert_eq!(report.missing_intent_stopped, save == 1);
            assert!(
                report.recovery.killed_without_cleanup
                    && report.recovery.independent_recovery
                    && report.recovery.terminal_journal_verified
            );
        }
    }
}
#[test]
fn native_persistent_outage_exhaustion_then_sigkill_recovers_after_healing() {
    use casita_turmoil_pins_spike::intent_journal::{Cut, Failure, Stage};
    for failure in Stage::ALL
        .into_iter()
        .map(Failure::Before)
        .chain(Cut::ALL.into_iter().map(Failure::Partial))
    {
        for save in 1..=3 {
            let report = client_journal::run_persistent_crash(executable(), failure, save).unwrap();
            assert_eq!(report.retry_attempts_before_kill, 3);
            assert!(
                report.retry_exhausted_before_kill
                    && report.recovery.killed_without_cleanup
                    && report.recovery.independent_recovery
                    && report.recovery.terminal_journal_verified
            );
        }
    }
}

#[test]
fn native_read_failures_block_dispatch_across_fresh_processes_until_repair() {
    use casita_turmoil_pins_spike::intent_journal::ReadFailure;
    for boundary in [Boundary::Submitted, Boundary::Unknown, Boundary::Recovered] {
        for failure in ReadFailure::ALL {
            let report = client_journal::run_read_failure(executable(), boundary, failure).unwrap();
            assert_eq!(report.failed_processes, 3);
            assert!(
                report.stopped_before_repository_access
                    && report.journal_unchanged
                    && report.metadata_unchanged
                    && report.recovery.terminal_journal_verified
            );
            assert_eq!(
                report.recovery.recovered,
                !matches!(boundary, Boundary::Submitted)
            );
        }
    }
}

#[test]
fn overlapping_recoverers_reject_contention_then_recover_after_owner_sigkill() {
    for boundary in [Boundary::Submitted, Boundary::Unknown, Boundary::Recovered] {
        for temporary in [false, true] {
            let report =
                client_journal::overlap::run(executable(), boundary, temporary, false).unwrap();
            assert_eq!(report.rejected_contenders, 3);
            assert!(report.journal_unchanged_while_owned && report.metadata_unchanged);
            assert!(report.recovery_after_owner_death.terminal_journal_verified);
            assert_eq!(
                report.recovery_after_owner_death.recovered,
                !matches!(boundary, Boundary::Submitted)
            );
        }
    }
}
#[test]
fn overlap_checker_rejects_recovery_that_bypasses_ownership() {
    let error =
        client_journal::overlap::run(executable(), Boundary::Unknown, false, true).unwrap_err();
    assert!(
        error.to_string().contains("contender dispatched while"),
        "{error}"
    );
}

#[test]
fn submission_and_recovery_share_ownership_and_preserve_existing_identity() {
    for boundary in [Boundary::Submitted, Boundary::Committed, Boundary::Unknown] {
        let report =
            client_journal::overlap::run_submission(executable(), boundary, false, true, false)
                .unwrap();
        assert_eq!(report.rejected_contenders, 3);
        assert!(report.recovery_after_owner_death.terminal_journal_verified);
        assert_eq!(
            report.recovery_after_owner_death.recovered,
            !matches!(boundary, Boundary::Submitted)
        );
    }
    for boundary in [Boundary::Submitted, Boundary::Unknown, Boundary::Recovered] {
        for temporary in [false, true] {
            let report = client_journal::overlap::run_submission(
                executable(),
                boundary,
                temporary,
                false,
                false,
            )
            .unwrap();
            assert!(
                report.submission_refused_existing_intent
                    && report.journal_unchanged_while_owned
                    && report.metadata_unchanged
            );
        }
    }
}
#[test]
fn submission_checker_rejects_bypassed_recovery_ownership() {
    let error = client_journal::overlap::run_submission(
        executable(),
        Boundary::Unknown,
        false,
        false,
        true,
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("contender dispatched while"),
        "{error}"
    );
}
