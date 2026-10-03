//! Canonical, deterministic byte encoding of a [`Directory`], and its inverse.
//!
//! A directory's digest is defined as `BLAKE3(encode_directory(dir))`. This
//! encoding is therefore **frozen**: it must never change once any data has
//! been stored, or existing digests would no longer verify. The same encoding
//! doubles as the on-disk storage format for directory services (it is fully
//! decodable via [`decode_directory`]).
//!
//! Format:
//!
//! ```text
//! u64le  entry_count
//! repeat entry_count times, in name-sorted order:
//!   u8    tag           (0 = directory, 1 = file, 2 = symlink)
//!   u64le name_len; name bytes
//!   tag == directory: 32-byte digest; u64le size
//!   tag == file:      32-byte digest; u64le size; u8 executable (0|1)
//!   tag == symlink:   u64le target_len; target bytes
//! ```
//!
//! Determinism comes from three properties: entries are iterated in the
//! directory's lexicographic name order (a `BTreeMap`), every variable-length
//! field is length-prefixed, and integers use fixed-width little-endian bytes.

use bytes::Bytes;

use crate::digest::{BlobId, DIGEST_LEN, Digest, DirectoryId};
use crate::directory::Directory;
use crate::error::DirectoryError;
use crate::node::Node;
use crate::path::{PathComponent, PathComponentError, SymlinkTarget, SymlinkTargetError};

const TAG_DIRECTORY: u8 = 0;
const TAG_FILE: u8 = 1;
const TAG_SYMLINK: u8 = 2;

pub(crate) fn encode_directory(dir: &Directory) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(dir.len() as u64).to_le_bytes());

    for (name, node) in dir.nodes() {
        match node {
            Node::Directory { digest, size } => {
                out.push(TAG_DIRECTORY);
                push_len_prefixed(&mut out, name.as_bytes());
                out.extend_from_slice(digest.digest().as_bytes());
                out.extend_from_slice(&size.to_le_bytes());
            }
            Node::File {
                digest,
                size,
                executable,
            } => {
                out.push(TAG_FILE);
                push_len_prefixed(&mut out, name.as_bytes());
                out.extend_from_slice(digest.digest().as_bytes());
                out.extend_from_slice(&size.to_le_bytes());
                out.push(u8::from(*executable));
            }
            Node::Symlink { target } => {
                out.push(TAG_SYMLINK);
                push_len_prefixed(&mut out, name.as_bytes());
                push_len_prefixed(&mut out, target.as_bytes());
            }
        }
    }

    out
}

fn push_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(bytes);
}

/// Errors decoding the canonical directory encoding.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum DirectoryDecodeError {
    /// The input ended before a field was fully read.
    #[error("unexpected end of input while decoding directory")]
    UnexpectedEof,
    /// A node record had an unknown tag byte.
    #[error("unknown node tag {0}")]
    UnknownTag(u8),
    /// A digest field was invalid.
    #[error("invalid digest: {0}")]
    Digest(#[from] crate::digest::DigestError),
    /// An entry name was invalid.
    #[error("invalid name: {0}")]
    Name(#[from] PathComponentError),
    /// A symlink target was invalid.
    #[error("invalid symlink target: {0}")]
    SymlinkTarget(#[from] SymlinkTargetError),
    /// The decoded entries did not form a valid directory.
    #[error("invalid directory: {0}")]
    Directory(#[from] DirectoryError),
    /// Entries were not in the canonical strictly-ascending-by-name order, so
    /// these bytes are not the one valid preimage of any digest.
    #[error("directory entries are not in canonical order")]
    NonCanonicalOrder,
    /// The executable flag is canonically encoded as exactly zero or one.
    #[error("invalid executable flag {0} (expected 0 or 1)")]
    InvalidExecutable(u8),
    /// Bytes remained after the declared number of entries.
    #[error("trailing bytes after directory encoding")]
    TrailingBytes,
    /// A `u64` byte-string length cannot be represented on this platform.
    #[error("encoded length does not fit in memory on this platform")]
    LengthOverflow,
}

pub(crate) use DirectoryDecodeError as DecodeError;

// only the native store backends decode directories today; the Worker will
// too once its transfer endpoints land.
#[cfg_attr(not(feature = "native"), allow(dead_code))]
pub(crate) fn decode_directory(bytes: &[u8]) -> Result<Directory, DecodeError> {
    let mut r = reader(bytes);
    let count = r.read_u64()?;

    let mut dir = Directory::new();
    let mut prev: Option<PathComponent> = None;
    for _ in 0..count {
        let tag = r.read_u8()?;
        let name = PathComponent::try_from(Bytes::copy_from_slice(r.read_len_prefixed()?))?;
        // the canonical encoding lists entries strictly ascending by name;
        // reject anything else so a digest has exactly one valid preimage.
        if prev.as_ref().is_some_and(|p| &name <= p) {
            return Err(DecodeError::NonCanonicalOrder);
        }
        prev = Some(name.clone());
        let node = match tag {
            TAG_DIRECTORY => {
                let digest = DirectoryId::new(Digest::try_from(r.read(DIGEST_LEN)?)?);
                let size = r.read_u64()?;
                Node::Directory { digest, size }
            }
            TAG_FILE => {
                let digest = BlobId::new(Digest::try_from(r.read(DIGEST_LEN)?)?);
                let size = r.read_u64()?;
                let executable = match r.read_u8()? {
                    0 => false,
                    1 => true,
                    other => return Err(DecodeError::InvalidExecutable(other)),
                };
                Node::File {
                    digest,
                    size,
                    executable,
                }
            }
            TAG_SYMLINK => {
                let target =
                    SymlinkTarget::try_from(Bytes::copy_from_slice(r.read_len_prefixed()?))?;
                Node::Symlink { target }
            }
            other => return Err(DecodeError::UnknownTag(other)),
        };
        dir.add(name, node)?;
    }

    r.finish()?;

    Ok(dir)
}

/// Directory and chunk-manifest cursors preserve the directory decoder's
/// existing primitive errors, including its EOF diagnostic.
pub(crate) type Reader<'a> = crate::binary::Reader<'a, DecodeError>;

pub(crate) fn reader(bytes: &[u8]) -> Reader<'_> {
    crate::binary::Reader::new(bytes, |error| match error {
        crate::binary::ReadError::UnexpectedEof => DecodeError::UnexpectedEof,
        crate::binary::ReadError::LengthOverflow => DecodeError::LengthOverflow,
        crate::binary::ReadError::TrailingBytes => DecodeError::TrailingBytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::path::SymlinkTarget;
    use crate::test_util::pc;

    #[test]
    fn encode_decode_roundtrip() {
        let dir = Directory::try_from_iter([
            (
                pc("bin"),
                Node::Directory {
                    digest: DirectoryId::new([3u8; 32].into()),
                    size: 4,
                },
            ),
            (
                pc("run.sh"),
                Node::File {
                    digest: BlobId::new([9u8; 32].into()),
                    size: 128,
                    executable: true,
                },
            ),
            (
                pc("link"),
                Node::Symlink {
                    target: SymlinkTarget::try_from("../elsewhere").unwrap(),
                },
            ),
        ])
        .unwrap();

        let encoded = encode_directory(&dir);
        let decoded = decode_directory(&encoded).unwrap();
        assert_eq!(dir, decoded);
        assert_eq!(dir.digest(), decoded.digest());
    }

    #[test]
    fn decode_rejects_non_canonical_order() {
        // two valid entries whose names descend (b before a): a non-canonical
        // preimage the decoder must reject rather than silently re-sort.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&2u64.to_le_bytes());
        for name in ["b", "a"] {
            bytes.push(TAG_SYMLINK);
            push_len_prefixed(&mut bytes, name.as_bytes());
            push_len_prefixed(&mut bytes, b"target");
        }
        assert_eq!(
            decode_directory(&bytes).unwrap_err(),
            DecodeError::NonCanonicalOrder
        );

        // Equal adjacent names are the duplicate form of the same canonical
        // ordering violation.
        let mut duplicate = Vec::new();
        duplicate.extend_from_slice(&2u64.to_le_bytes());
        for _ in 0..2 {
            duplicate.push(TAG_SYMLINK);
            push_len_prefixed(&mut duplicate, b"a");
            push_len_prefixed(&mut duplicate, b"target");
        }
        assert_eq!(
            decode_directory(&duplicate).unwrap_err(),
            DecodeError::NonCanonicalOrder
        );
    }

    #[test]
    fn truncated_input_errors() {
        let dir = Directory::try_from_iter([(
            pc("a"),
            Node::Symlink {
                target: SymlinkTarget::try_from("x").unwrap(),
            },
        )])
        .unwrap();
        let encoded = encode_directory(&dir);
        for end in 0..encoded.len() {
            assert_eq!(
                decode_directory(&encoded[..end]).unwrap_err(),
                DecodeError::UnexpectedEof,
                "truncated at {end}"
            );
        }
    }

    #[test]
    fn decode_rejects_unknown_tag() {
        // a single entry whose tag byte names no node kind: the decoder must
        // reject it rather than guess a shape for the trailing bytes.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.push(99);
        push_len_prefixed(&mut bytes, b"a");
        assert_eq!(
            decode_directory(&bytes).unwrap_err(),
            DecodeError::UnknownTag(99)
        );
    }

    #[test]
    fn decode_rejects_non_boolean_executable_flag() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.push(TAG_FILE);
        push_len_prefixed(&mut bytes, b"program");
        bytes.extend_from_slice(&[7u8; DIGEST_LEN]);
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.push(2);

        assert_eq!(
            decode_directory(&bytes).unwrap_err(),
            DecodeError::InvalidExecutable(2)
        );
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let mut bytes = encode_directory(&Directory::new());
        bytes.push(0);
        assert_eq!(
            decode_directory(&bytes).unwrap_err(),
            DecodeError::TrailingBytes
        );
    }

    #[test]
    fn decode_rejects_traversal_entry_names() {
        // a hostile encoding cannot smuggle `/`, `..`, `.`, or an empty name
        // past the component validator, so no digest has such a preimage.
        for (name, expected) in [
            (b"".as_slice(), PathComponentError::Empty),
            (b"/".as_slice(), PathComponentError::Slash),
            (b"..".as_slice(), PathComponentError::Parent),
            (b".".as_slice(), PathComponentError::CurDir),
        ] {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&1u64.to_le_bytes());
            bytes.push(TAG_SYMLINK);
            push_len_prefixed(&mut bytes, name);
            push_len_prefixed(&mut bytes, b"target");
            assert_eq!(
                decode_directory(&bytes).unwrap_err(),
                DecodeError::Name(expected)
            );
        }

        let too_long = vec![b'x'; crate::path::MAX_NAME_LEN + 1];
        for (name, expected) in [
            (b"a\0b".as_slice(), PathComponentError::Null),
            (too_long.as_slice(), PathComponentError::TooLong),
        ] {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&1u64.to_le_bytes());
            bytes.push(TAG_SYMLINK);
            push_len_prefixed(&mut bytes, name);
            push_len_prefixed(&mut bytes, b"target");
            assert_eq!(
                decode_directory(&bytes).unwrap_err(),
                DecodeError::Name(expected)
            );
        }
    }

    #[test]
    fn decode_rejects_invalid_symlink_targets() {
        // an empty or NUL-bearing target fails the target validator on the way
        // back in, mirroring what construction refuses.
        for (target, expected) in [
            (b"".as_slice(), SymlinkTargetError::Empty),
            (b"a\0b".as_slice(), SymlinkTargetError::Null),
        ] {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&1u64.to_le_bytes());
            bytes.push(TAG_SYMLINK);
            push_len_prefixed(&mut bytes, b"link");
            push_len_prefixed(&mut bytes, target);
            assert_eq!(
                decode_directory(&bytes).unwrap_err(),
                DecodeError::SymlinkTarget(expected)
            );
        }

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.push(TAG_SYMLINK);
        push_len_prefixed(&mut bytes, b"link");
        push_len_prefixed(&mut bytes, &vec![b'x'; crate::path::MAX_TARGET_LEN + 1]);
        assert_eq!(
            decode_directory(&bytes).unwrap_err(),
            DecodeError::SymlinkTarget(SymlinkTargetError::TooLong)
        );
    }

    #[test]
    fn declared_count_never_drives_allocation() {
        let bytes = u64::MAX.to_le_bytes();
        assert_eq!(
            decode_directory(&bytes).unwrap_err(),
            DecodeError::UnexpectedEof
        );
    }
}
