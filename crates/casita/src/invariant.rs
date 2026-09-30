//! Commit-transition invariants.
//!
//! Each commit point checks that the transition it is about to make durable
//! keeps the invariants the reliability contract relies on. Validators are
//! pure functions of the previous and next state that return
//! `Result<(), Violation>`, so a unit test can hand one an illegal transition
//! and watch it fire. [`check`] decides what a violation does:
//!
//! - Debug builds panic, so every unit test, the crash matrix, proptests and
//!   fuzzing exercise the checks.
//! - Release builds compiled with `RUSTFLAGS="--cfg casita_invariants"` return
//!   the violation, which the commit point reports as corruption instead of
//!   committing. Soak runs use this with optimizations on.
//! - Other release builds skip the checks, including the release test
//!   binaries benchmarks build with `--all-features`, so probe timings measure
//!   production code. Validators still compile there, so they cannot rot
//!   behind a `cfg`.
//!
//! This is a `cfg` rather than a Cargo feature for that reason: a feature
//! would be switched on by every `--all-features` build.
//!
//! A check runs before the commit it guards: a violation never becomes
//! durable. State a validator needs that the commit point would otherwise not
//! keep (for example, the previous inventory) is captured only when
//! [`ENABLED`] is true.

use std::fmt;

/// Whether commit points validate their transitions in this build.
pub(crate) const ENABLED: bool = cfg!(any(debug_assertions, casita_invariants));

/// Whether this build also runs the costly checks: those that re-hash
/// payloads or retain per-write state. Debug builds only, never
/// `casita_invariants` release builds.
pub(crate) const DEBUG: bool = cfg!(debug_assertions);

/// A transition that breaks a commit invariant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Violation {
    point: &'static str,
    detail: String,
}

impl Violation {
    /// `point` names the commit point, `detail` the broken invariant and the
    /// values that broke it.
    pub(crate) fn new(point: &'static str, detail: impl Into<String>) -> Self {
        Self {
            point,
            detail: detail.into(),
        }
    }
}

impl fmt::Display for Violation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.point, self.detail)
    }
}

impl std::error::Error for Violation {}

/// Returns `Err` with a [`Violation`] from `point` unless `holds`.
pub(crate) fn ensure(
    point: &'static str,
    holds: bool,
    detail: impl FnOnce() -> String,
) -> Result<(), Violation> {
    if holds {
        Ok(())
    } else {
        Err(Violation::new(point, detail()))
    }
}

/// Run `validate` when this build checks invariants.
///
/// Panics on a violation in debug builds; in `casita_invariants` release
/// builds, returns it so the caller refuses the commit.
pub(crate) fn check(validate: impl FnOnce() -> Result<(), Violation>) -> Result<(), Violation> {
    if !ENABLED {
        return Ok(());
    }
    enforce(validate())
}

fn enforce(result: Result<(), Violation>) -> Result<(), Violation> {
    match result {
        Err(violation) if cfg!(debug_assertions) => {
            panic!("commit invariant violated at {violation}")
        }
        result => result,
    }
}

impl From<Violation> for std::io::Error {
    fn from(violation: Violation) -> Self {
        Self::new(std::io::ErrorKind::InvalidData, violation)
    }
}

#[cfg(feature = "native")]
impl From<Violation> for crate::metadata::MetadataError {
    fn from(violation: Violation) -> Self {
        Self::Corruption(format!("commit invariant violated at {violation}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_holding_invariant_passes() {
        assert_eq!(check(|| ensure("point", true, String::new)), Ok(()));
    }

    #[test]
    #[should_panic(expected = "commit invariant violated at point: broken")]
    fn test_builds_panic_on_a_violation() {
        let _ = check(|| ensure("point", false, || "broken".into()));
    }
}
