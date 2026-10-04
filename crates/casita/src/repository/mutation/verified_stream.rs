use super::*;
use crate::format::PayloadReader;

/// The verifier drives both hashing and writes. Empty reads are not EOF, and
/// a declared length never hides excess source bytes from the final read.
pub(super) struct WritingReader<'a, R> {
    source: &'a mut R,
    writer: Box<dyn crate::blob::BlobWriter>,
    expected: u64,
    observed: u64,
    eof: bool,
    read_failed: bool,
}

impl<'a, R> WritingReader<'a, R> {
    pub(super) fn new(
        source: &'a mut R,
        writer: Box<dyn crate::blob::BlobWriter>,
        expected: u64,
    ) -> Self {
        Self {
            source,
            writer,
            expected,
            observed: 0,
            eof: false,
            read_failed: false,
        }
    }

    pub(super) async fn finish(&mut self, record: &ObjectRecord) -> Result<(), RepositoryError> {
        if self.read_failed || !self.eof {
            return Err(FormatError::PayloadNotFullyConsumed(record.key().clone()).into());
        }
        if self.observed != self.expected {
            return Err(RepositoryError::PayloadSizeMismatch {
                expected: self.expected,
                actual: self.observed,
            });
        }
        if record.payload_size() != self.observed {
            return Err(RepositoryError::PayloadSizeMismatch {
                expected: self.observed,
                actual: record.payload_size(),
            });
        }
        let (payload, size) = self.writer.close().await?;
        if size != self.observed {
            return Err(RepositoryError::PayloadSizeMismatch {
                expected: self.observed,
                actual: size,
            });
        }
        if payload != record.payload() {
            return Err(RepositoryError::PayloadIdentityMismatch {
                expected: record.payload(),
                actual: payload,
            });
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<R: AsyncRead + Unpin + Send> PayloadReader for WritingReader<'_, R> {
    fn exact_len(&self) -> Option<u64> {
        Some(self.expected)
    }

    async fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.read_failed {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "verified stream cannot resume after a failed or cancelled read",
            ));
        }
        if buffer.is_empty() || self.eof {
            return Ok(0);
        }
        // A custom verifier can catch an error or drop this future. Neither
        // may erase bytes already consumed from the source or partly written.
        // Clear the failure only after the whole read/write step succeeds.
        self.read_failed = true;
        let limit = self
            .expected
            .saturating_sub(self.observed)
            .saturating_add(1)
            .min(buffer.len() as u64) as usize;
        let count = tokio::io::AsyncReadExt::read(self.source, &mut buffer[..limit]).await?;
        if count == 0 {
            self.eof = true;
            self.read_failed = false;
            return Ok(0);
        }
        let observed = self
            .observed
            .checked_add(count as u64)
            .filter(|&size| size <= self.expected)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "stream exceeds declared payload size",
                )
            })?;
        self.writer.write_all(&buffer[..count]).await?;
        self.observed = observed;
        self.read_failed = false;
        Ok(count)
    }
}
