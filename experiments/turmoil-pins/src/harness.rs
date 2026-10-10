//! Common simulation configuration and strict, artifact-producing replay checks.
use casita::experimental::Digest;
use serde::Serialize;
use std::{fs::File, io::Write, path::Path, time::Duration};

pub(crate) fn simulation(seed: u64, latency_ms: u64, budget_secs: u64) -> turmoil::Sim<'static> {
    turmoil::Builder::new()
        .rng_seed(seed)
        .enable_random_order()
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(latency_ms))
        .simulation_duration(Duration::from_secs(budget_secs))
        .build()
}

/// Retains complete reports, including identities and timing, without normalizing entropy.
pub struct Corpus {
    output: File,
    reports: Vec<serde_json::Value>,
}
#[derive(Debug, Serialize)]
pub struct Summary {
    pub cases: usize,
    pub reports_digest: String,
}
impl Corpus {
    pub fn create(path: &Path) -> std::io::Result<Self> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        Ok(Self {
            output: File::create(path)?,
            reports: Vec::new(),
        })
    }
    fn record(&mut self, value: &serde_json::Value) -> Result<(), String> {
        serde_json::to_writer(&mut self.output, value).map_err(|e| e.to_string())?;
        writeln!(self.output).map_err(|e| e.to_string())?;
        self.output.flush().map_err(|e| e.to_string())
    }
    pub fn check<R: Serialize + PartialEq>(
        &mut self,
        seed: u64,
        case: &str,
        mut run: impl FnMut() -> Result<R, String>,
    ) -> Result<(), String> {
        let mut pair = Vec::new();
        for attempt in 1..=2 {
            // A panic or process timeout still leaves the active case identifiable.
            self.record(&serde_json::json!({"seed": seed, "case": case, "attempt": attempt, "status": "started"}))?;
            match run() {
                Ok(report) => {
                    self.record(&serde_json::json!({"seed": seed, "case": case, "attempt": attempt, "report": report}))?;
                    pair.push(report);
                }
                Err(error) => {
                    self.record(&serde_json::json!({"seed": seed, "case": case, "attempt": attempt, "error": error}))?;
                    return Err(format!("seed={seed} {case} attempt={attempt}: {error}"));
                }
            }
        }
        if pair[0] != pair[1] {
            return Err(format!(
                "full replay diverged: seed={seed} {case}; both reports retained in artifact"
            ));
        }
        self.reports
            .push(serde_json::json!({"seed": seed, "case": case, "report": pair[0]}));
        Ok(())
    }
    pub fn summary(&self) -> Result<Summary, serde_json::Error> {
        Ok(Summary {
            cases: self.reports.len(),
            reports_digest: Digest::hash(&serde_json::to_vec(&self.reports)?).to_string(),
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replay_rejects_identity_drift_and_preserves_both_reports() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("reports.jsonl");
        let mut corpus = Corpus::create(&path).unwrap();
        let mut identity = 0;
        let error = corpus
            .check(17, "ownership", || {
                identity += 1;
                Ok(serde_json::json!({"token": identity}))
            })
            .unwrap_err();
        assert!(error.contains("full replay diverged: seed=17 ownership"));
        let records: Vec<serde_json::Value> = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(records[1]["report"]["token"], 1);
        assert_eq!(records[3]["report"]["token"], 2);
        assert_eq!(corpus.summary().unwrap().cases, 0);
    }
    #[test]
    fn simulation_failure_retains_case_seed_attempt_and_error() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("reports.jsonl");
        let mut corpus = Corpus::create(&path).unwrap();
        let error = corpus
            .check::<()>(23, "publication", || Err("root lost".into()))
            .unwrap_err();
        assert!(error.contains("seed=23 publication attempt=1: root lost"));
        let records: Vec<serde_json::Value> = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(records[1]["error"], "root lost");
        assert_eq!(records[1]["seed"], 23);
    }
}
