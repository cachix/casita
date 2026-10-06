//! Bounded metadata materialization must scale scratch space to small objects
//! without trusting a reader's length hint for validation or EOF.
#![cfg(feature = "experimental")]

use casita::experimental::{BlobId, FormatError, ObjectKey, PayloadReader, VerificationContext};

struct HintedReader<'a> {
    bytes: &'a [u8],
    length: Option<u64>,
    largest_buffer: usize,
    eof: bool,
    max_read: usize,
    reads: usize,
}

#[async_trait::async_trait]
impl PayloadReader for HintedReader<'_> {
    fn exact_len(&self) -> Option<u64> {
        self.length
    }

    async fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.reads += 1;
        assert!(!buffer.is_empty());
        self.largest_buffer = self.largest_buffer.max(buffer.len());
        let end = buffer.len().min(self.max_read);
        let count = std::io::Read::read(&mut self.bytes, &mut buffer[..end])?;
        self.eof |= count == 0;
        Ok(count)
    }
}

#[tokio::test]
async fn metadata_scratch_scales_to_the_length_hint() {
    for size in [0, 1, 128, 65535, 65536, 65537] {
        let bytes = vec![42; size];
        let key = ObjectKey::blob(BlobId::new(blake3::hash(&bytes).into()));
        let mut reader = HintedReader {
            bytes: &bytes,
            length: Some(size as u64),
            largest_buffer: 0,
            eof: false,
            max_read: usize::MAX,
            reads: 0,
        };
        let mut context = VerificationContext::new(&key, &mut reader);
        let decoded = context.read_to_end_bounded(size as u64).await.unwrap();
        assert!(
            decoded.capacity() <= size,
            "capacity exceeds metadata limit at size {size}"
        );
        assert_eq!(decoded, bytes);
        let verified = context.finish(Vec::new()).unwrap();
        assert_eq!(verified.record().key(), &key);
        assert_eq!(
            verified.record().payload(),
            BlobId::new(blake3::hash(&bytes).into())
        );
        assert!(reader.eof);
        assert!(
            reader.largest_buffer <= size.clamp(1, 65536),
            "size {size}: {}",
            reader.largest_buffer
        );
    }
}

#[tokio::test]
async fn metadata_length_hints_do_not_replace_limits_or_eof() {
    let bytes = b"complete metadata";
    let digest = blake3::hash(bytes).into();
    let key = ObjectKey::blob(BlobId::new(digest));
    for length in [
        None,
        Some(0),
        Some(1),
        Some(bytes.len() as u64),
        Some(u64::MAX),
    ] {
        for limit in [0, bytes.len() as u64 - 1, bytes.len() as u64] {
            let mut reader = HintedReader {
                bytes,
                length,
                largest_buffer: 0,
                eof: false,
                max_read: 3,
                reads: 0,
            };
            let mut context = VerificationContext::new(&key, &mut reader);
            let result = context.read_to_end_bounded(limit).await;
            if limit < bytes.len() as u64 {
                match result {
                    Err(FormatError::MetadataLimit { limit: actual }) => assert_eq!(actual, limit),
                    other => panic!("expected the metadata limit, got {other:?}"),
                }
            } else {
                assert_eq!(result.unwrap(), bytes);
                assert_eq!(context.observed_digest(), digest);
                assert_eq!(
                    context.finish(Vec::new()).unwrap().record().payload_size(),
                    bytes.len() as u64
                );
                assert!(reader.eof);
            }
            assert!(reader.largest_buffer <= 65536);
        }
    }
}

#[tokio::test]
async fn metadata_growth_handles_short_reads_and_inaccurate_hints() {
    let bytes: Vec<_> = (0..262145).map(|i| (i % 251) as u8).collect();
    let digest = blake3::hash(&bytes).into();
    let key = ObjectKey::blob(BlobId::new(digest));
    for length in [None, Some(0), Some(1), Some(65536), Some(u64::MAX)] {
        for max_read in [127, 65536] {
            let mut reader = HintedReader {
                bytes: &bytes,
                length,
                largest_buffer: 0,
                eof: false,
                max_read,
                reads: 0,
            };
            let mut context = VerificationContext::new(&key, &mut reader);
            assert_eq!(context.read_to_end_bounded(1 << 20).await.unwrap(), bytes);
            assert_eq!(context.observed_digest(), digest);
            assert_eq!(
                context.finish(Vec::new()).unwrap().record().payload_size(),
                bytes.len() as u64
            );
            assert!(reader.eof);
            assert!(reader.largest_buffer <= 65536);
            // An understated hint costs at most two reads into its small
            // buffer: one that fills it and one that exceeds it. Full-size
            // reads and EOF follow.
            assert!(
                reader.reads <= bytes.len().div_ceil(max_read) + 3,
                "{} reads with hint {length:?} and reads of {max_read} bytes",
                reader.reads
            );
        }
    }
}

#[tokio::test]
async fn metadata_allocation_stays_within_small_and_non_power_of_two_limits() {
    for size in [0, 1, 7, 8, 9, 65537] {
        let bytes = vec![42; size];
        let key = ObjectKey::blob(BlobId::new(blake3::hash(&bytes).into()));
        for length in [None, Some(0), Some(1), Some(u64::MAX)] {
            let mut reader = HintedReader {
                bytes: &bytes,
                length,
                largest_buffer: 0,
                eof: false,
                max_read: 127,
                reads: 0,
            };
            let mut context = VerificationContext::new(&key, &mut reader);
            let decoded = context.read_to_end_bounded(size as u64).await.unwrap();
            assert_eq!(decoded, bytes);
            assert!(decoded.capacity() <= size);
            context.finish(Vec::new()).unwrap();
        }
    }
}
