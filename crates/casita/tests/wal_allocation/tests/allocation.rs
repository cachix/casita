#![forbid(unsafe_code)]
#![cfg(target_os = "linux")]
use std::os::unix::process::ExitStatusExt;
use std::process::Command;

fn bounded(mode: &str) {
    let output = Command::new("prlimit")
        .args([
            "--as=268435456",
            "--",
            env!("CARGO_BIN_EXE_wal-allocation-reproducer"),
            mode,
        ])
        .output()
        .expect("prlimit must start the frozen child");
    println!(
        "mode={mode} status={} signal={:?} stdout={} stderr={}",
        output.status,
        output.status.signal(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.status.success(),
        "{mode} must return a checked result within the unchanged 256 MiB child limit"
    );
}
#[test]
fn valid_snappy_body_positions_epoch_and_setsum_fit_the_bound() {
    bounded("valid");
}
#[test]
fn truncated_fragment_is_rejected_within_the_bound() {
    bounded("truncated");
}
#[test]
fn default_statistics_skip_rejects_unbacked_prefix_within_the_bound() {
    bounded("statistics-default");
}
#[test]
fn default_wal_rejects_unbacked_decompressed_size_without_reserving_it() {
    bounded("page-size");
}
