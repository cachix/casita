//! Bounds-checked binary primitives with format-owned error reporting.

/// Failures common to binary cursors. Formats map these to their own errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadError {
    UnexpectedEof,
    LengthOverflow,
    TrailingBytes,
}

/// A cursor over borrowed bytes. Count limits and canonical rules belong to
/// the format; this reader only checks slicing, widths, and complete consumption.
pub(crate) struct Reader<'a, E> {
    bytes: &'a [u8],
    pos: usize,
    error: fn(ReadError) -> E,
}

impl<'a, E> Reader<'a, E> {
    pub(crate) fn new(bytes: &'a [u8], error: fn(ReadError) -> E) -> Self {
        Self {
            bytes,
            pos: 0,
            error,
        }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.pos
    }

    pub(crate) fn read(&mut self, len: usize) -> Result<&'a [u8], E> {
        let end = self
            .pos
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| (self.error)(ReadError::UnexpectedEof))?;
        let bytes = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(bytes)
    }

    pub(crate) fn read_u8(&mut self) -> Result<u8, E> {
        Ok(self.read(1)?[0])
    }

    pub(crate) fn read_u64(&mut self) -> Result<u64, E> {
        Ok(u64::from_le_bytes(
            self.read(8)?.try_into().expect("eight bytes"),
        ))
    }

    pub(crate) fn read_len_prefixed(&mut self) -> Result<&'a [u8], E> {
        let len = usize::try_from(self.read_u64()?)
            .map_err(|_| (self.error)(ReadError::LengthOverflow))?;
        self.read(len)
    }

    pub(crate) fn finish(&self) -> Result<(), E> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err((self.error)(ReadError::TrailingBytes))
        }
    }
}
