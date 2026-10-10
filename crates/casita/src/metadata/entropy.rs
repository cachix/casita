//! Ownership identity entropy, scoped independently of process-global RNGs.
use std::sync::{Arc, OnceLock};

use super::MetadataError;

/// Supplies entropy for ownership tokens, revisions, and retry jitter.
///
/// Implementations must be thread safe and produce independent, unique
/// ownership identities across clients and lifetimes. A seeded implementation
/// is suitable only for isolated simulations. Production constructors use OS
/// entropy; experimental constructors accept an explicit source.
pub trait EntropySource: Send + Sync {
    /// Fill the entire buffer, or fail before an identity is installed.
    fn fill(&self, bytes: &mut [u8]) -> Result<(), MetadataError>;
}

struct OsEntropy;
impl EntropySource for OsEntropy {
    fn fill(&self, bytes: &mut [u8]) -> Result<(), MetadataError> {
        getrandom::fill(bytes).map_err(|error| MetadataError::RevisionEntropy(error.to_string()))
    }
}

pub(crate) fn system_entropy() -> Arc<dyn EntropySource> {
    static SOURCE: OnceLock<Arc<dyn EntropySource>> = OnceLock::new();
    SOURCE.get_or_init(|| Arc::new(OsEntropy)).clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{
        DataPin, MemoryMetadataStore, MemoryPinStore, MetadataMutation, MetadataStore, PinScope,
        PinStore,
    };
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Script {
        calls: AtomicUsize,
        fail_after: usize,
    }
    impl EntropySource for Script {
        fn fill(&self, bytes: &mut [u8]) -> Result<(), MetadataError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call >= self.fail_after {
                return Err(MetadataError::RevisionEntropy(
                    "injected entropy failure".into(),
                ));
            }
            bytes.fill(call as u8);
            Ok(())
        }
    }
    fn staging() -> DataPin {
        DataPin {
            scope: PinScope::Staging,
            catalog: None,
            resources: BTreeSet::new(),
        }
    }
    #[tokio::test]
    async fn entropy_failure_does_not_install_pin_or_metadata_revision() {
        let pins = MemoryPinStore::new_with_entropy(Arc::new(Script {
            calls: AtomicUsize::new(0),
            fail_after: 0,
        }));
        let before = pins.inventory().await.unwrap();
        assert!(matches!(
            pins.register(staging()).await,
            Err(MetadataError::RevisionEntropy(_))
        ));
        let after = pins.inventory().await.unwrap();
        assert_eq!(before.revision, after.revision);
        assert!(after.pins.is_empty());
        let meta = MemoryMetadataStore::new_with_entropy(Arc::new(Script {
            calls: AtomicUsize::new(0),
            fail_after: 1,
        }))
        .unwrap();
        let before = meta.snapshot().await.unwrap();
        let record =
            crate::MetadataKey::new("casita.entropy.tests.v1".parse().unwrap(), "uncommitted");
        let mutation = MetadataMutation::with_metadata(
            vec![],
            vec![crate::MetadataChange::Set {
                key: record.clone(),
                value: bytes::Bytes::from_static(b"must not become visible"),
            }],
        )
        .unwrap();
        assert!(matches!(
            meta.commit(&before.revision(), mutation).await,
            Err(MetadataError::RevisionEntropy(_))
        ));
        let after = meta.snapshot().await.unwrap();
        assert_eq!(before.revision(), after.revision());
        assert_eq!(before.generation().unwrap(), after.generation().unwrap());
        assert!(after.get(std::slice::from_ref(&record)).await.unwrap()[0].is_none());
        assert!(before.get(std::slice::from_ref(&record)).await.unwrap()[0].is_none());
    }
    #[tokio::test]
    async fn clones_share_identity_stream() {
        let pins = MemoryPinStore::new_with_entropy(Arc::new(Script {
            calls: AtomicUsize::new(0),
            fail_after: 2,
        }));
        let first = pins.register(staging()).await.unwrap().unwrap();
        let second = pins.clone().register(staging()).await.unwrap().unwrap();
        assert_ne!(first, second);
        assert_eq!(pins.inventory().await.unwrap().pins.len(), 2);
    }
    #[tokio::test]
    async fn repeated_revision_is_bounded_and_atomic() {
        struct Constant(AtomicUsize);
        impl EntropySource for Constant {
            fn fill(&self, bytes: &mut [u8]) -> Result<(), MetadataError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                bytes.fill(7);
                Ok(())
            }
        }
        let entropy = Arc::new(Constant(AtomicUsize::new(0)));
        let meta = MemoryMetadataStore::new_with_entropy(entropy.clone()).unwrap();
        let revision = meta.snapshot().await.unwrap().revision();
        assert!(matches!(
            meta.commit(&revision, MetadataMutation::new()).await,
            Err(MetadataError::RevisionEntropy(_))
        ));
        assert_eq!(entropy.0.load(Ordering::SeqCst), 33);
        assert_eq!(meta.snapshot().await.unwrap().revision(), revision);
    }
}
