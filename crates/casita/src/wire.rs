//! Frozen physical chunk-manifest encoding.
//!
//! Payload chunking is private repository layout, but compatible payload
//! stores can copy compressed chunks and manifests without re-chunking. This
//! module defines that bounded shared representation.

use std::io;

use crate::digest::{ChunkId, DIGEST_LEN, Digest};

/// The largest physical chunk accepted from any compatible payload store.
pub const MAX_CHUNK_SIZE: u64 = 16 * 1024 * 1024;

/// BLAKE3 identity and plaintext length of one payload chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkMeta {
    /// BLAKE3 digest of the uncompressed chunk contents.
    pub digest: ChunkId,
    /// Positive plaintext chunk length, at most [`MAX_CHUNK_SIZE`]. Empty
    /// payloads use an empty manifest, not a zero-length chunk.
    pub size: u64,
}

/// One encoded row: `32-byte digest + u64le size`.
const CHUNK_META_LEN: usize = DIGEST_LEN + 8;

fn put_chunk_metas(chunks: &[ChunkMeta], output: &mut Vec<u8>) {
    output.extend_from_slice(&(chunks.len() as u64).to_le_bytes());
    for chunk in chunks {
        output.extend_from_slice(chunk.digest.digest().as_bytes());
        output.extend_from_slice(&chunk.size.to_le_bytes());
    }
}

fn take_chunk_metas(
    reader: &mut crate::encode::Reader<'_>,
    allocation_budget: usize,
) -> io::Result<Vec<ChunkMeta>> {
    let decode_error =
        |error: crate::encode::DecodeError| io::Error::other(format!("chunk manifest: {error}"));
    let count = reader.read_u64().map_err(decode_error)?;
    let mut chunks = Vec::with_capacity((count as usize).min(allocation_budget / CHUNK_META_LEN));
    let mut total = 0u64;
    for _ in 0..count {
        let digest = Digest::try_from(reader.read(DIGEST_LEN).map_err(decode_error)?)
            .map_err(io::Error::other)?;
        let size = reader.read_u64().map_err(decode_error)?;
        if size == 0 {
            return Err(io::Error::other("chunk manifest: zero-length chunk"));
        }
        if size > MAX_CHUNK_SIZE {
            return Err(io::Error::other("chunk manifest: chunk size exceeds limit"));
        }
        total = total
            .checked_add(size)
            .ok_or_else(|| io::Error::other("chunk manifest: total size overflows"))?;
        chunks.push(ChunkMeta {
            digest: ChunkId::new(digest),
            size,
        });
    }
    if reader.remaining() != 0 {
        return Err(io::Error::other("chunk manifest: trailing bytes"));
    }
    Ok(chunks)
}

/// Encode `u64le count` followed by `(digest, size)` rows.
pub fn encode_manifest(chunks: &[ChunkMeta]) -> Vec<u8> {
    let mut output = Vec::with_capacity(8 + chunks.len() * CHUNK_META_LEN);
    put_chunk_metas(chunks, &mut output);
    output
}

/// Decode a bounded canonical chunk manifest.
pub fn decode_manifest(bytes: &[u8]) -> io::Result<Vec<ChunkMeta>> {
    let mut reader = crate::encode::reader(bytes);
    take_chunk_metas(&mut reader, bytes.len().saturating_sub(8))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(data: &[u8], size: u64) -> ChunkMeta {
        ChunkMeta {
            digest: ChunkId::new(blake3::hash(data).into()),
            size,
        }
    }

    #[test]
    fn manifest_roundtrip_is_canonical() {
        let manifest = vec![chunk(b"one", 1024), chunk(b"two", 77)];
        let encoded = encode_manifest(&manifest);
        assert_eq!(decode_manifest(&encoded).unwrap(), manifest);
        assert_eq!(decode_manifest(&encode_manifest(&[])).unwrap(), vec![]);

        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode_manifest(&trailing).is_err());
    }

    #[test]
    fn hostile_counts_and_sizes_are_rejected() {
        assert!(decode_manifest(&u64::MAX.to_le_bytes()).is_err());
        let empty = chunk(b"", 0);
        assert!(decode_manifest(&encode_manifest(&[empty])).is_err());

        let over = chunk(b"large", MAX_CHUNK_SIZE + 1);
        let error = decode_manifest(&encode_manifest(&[over])).unwrap_err();
        assert!(error.to_string().contains("chunk size exceeds limit"));

        let at_limit = chunk(b"limit", MAX_CHUNK_SIZE);
        assert_eq!(
            decode_manifest(&encode_manifest(std::slice::from_ref(&at_limit))).unwrap(),
            vec![at_limit]
        );
    }
}
