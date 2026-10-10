use casita::experimental::{Digest, EntropySource, MetadataError};
use std::sync::Mutex;

/// A reproducible, domain-separated identity stream for tests only.
/// Never use this provider for production ownership identities.
pub struct SeededEntropy {
    seed: u64,
    domain: String,
    counter: Mutex<u64>,
}
impl SeededEntropy {
    pub fn new(seed: u64, domain: &str) -> Self {
        Self {
            seed,
            domain: domain.into(),
            counter: Mutex::new(0),
        }
    }
}
impl EntropySource for SeededEntropy {
    fn fill(&self, bytes: &mut [u8]) -> Result<(), MetadataError> {
        let mut counter = self.counter.lock().unwrap();
        for part in bytes.chunks_mut(32) {
            let input = format!("casita-dst:{}:{}:{}", self.seed, self.domain, *counter);
            *counter += 1;
            part.copy_from_slice(&Digest::hash(input.as_bytes()).as_bytes()[..part.len()]);
        }
        Ok(())
    }
}
