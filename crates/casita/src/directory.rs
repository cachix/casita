//! [`Directory`], a Merkle-DAG node mapping names to child [`Node`]s.

use std::collections::btree_map::{self, BTreeMap};

#[cfg(test)]
use crate::digest::BlobId;
use crate::digest::DirectoryId;
use crate::encode::encode_directory;
use crate::error::DirectoryError;
use crate::node::Node;
use crate::path::PathComponent;

#[cfg(feature = "native")]
pub(crate) mod read;

/// A directory: a name-sorted, unique-name map of child [`Node`]s.
///
/// Addressed by the BLAKE3 digest of its canonical encoding (see
/// [`Directory::digest`]). Because entries are held in a `BTreeMap`, iteration
/// order (and therefore the digest) is independent of insertion order.
///
/// The `size` (recursive descendant count) is maintained incrementally and its
/// computation is overflow-checked, so a maliciously deep tree cannot silently
/// wrap `u64`.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct Directory {
    nodes: BTreeMap<PathComponent, Node>,
    size: u64,
}

impl Directory {
    /// An empty directory.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a directory from `(name, node)` pairs, validating uniqueness and
    /// size as each is inserted.
    pub fn try_from_iter<I>(iter: I) -> Result<Self, DirectoryError>
    where
        I: IntoIterator<Item = (PathComponent, Node)>,
    {
        let mut dir = Directory::new();
        for (name, node) in iter {
            dir.add(name, node)?;
        }
        Ok(dir)
    }

    /// Insert a child node under `name`.
    ///
    /// Fails with [`DirectoryError::DuplicateName`] if the name is already
    /// present, or [`DirectoryError::SizeOverflow`] if the recursive size would
    /// exceed `u64`.
    pub fn add(&mut self, name: PathComponent, node: Node) -> Result<(), DirectoryError> {
        let contribution = node_size_contribution(&node)?;
        let new_size = self
            .size
            .checked_add(contribution)
            .ok_or(DirectoryError::SizeOverflow)?;

        match self.nodes.entry(name) {
            btree_map::Entry::Occupied(o) => Err(DirectoryError::DuplicateName(o.key().clone())),
            btree_map::Entry::Vacant(v) => {
                v.insert(node);
                self.size = new_size;
                Ok(())
            }
        }
    }

    /// Number of direct entries.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the directory has no entries.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// The recursive descendant count: `len() + Σ child-directory sizes`.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// The content address of this directory: `BLAKE3(canonical encoding)`.
    pub fn digest(&self) -> DirectoryId {
        DirectoryId::new(blake3::hash(&encode_directory(self)).into())
    }

    /// The canonical encoding: the frozen storage and wire format.
    pub fn encode(&self) -> Vec<u8> {
        encode_directory(self)
    }

    /// Decode a canonical encoding. Callers must check the result's
    /// [`digest`](Directory::digest) against the address it was fetched
    /// from; the encoding itself carries no checksum.
    pub fn decode(bytes: &[u8]) -> Result<Directory, crate::DirectoryDecodeError> {
        crate::encode::decode_directory(bytes)
    }

    /// Look up a child node by name. Any byte string probes the map —
    /// `dir.get("src")`, a [`PathComponent`], raw bytes — and a name that
    /// would not even validate simply returns `None`.
    pub fn get(&self, name: impl AsRef<[u8]>) -> Option<&Node> {
        self.nodes.get(name.as_ref())
    }

    /// Iterate entries in lexicographic name order.
    pub fn nodes(&self) -> impl Iterator<Item = (&PathComponent, &Node)> {
        self.nodes.iter()
    }
}

impl<'a> IntoIterator for &'a Directory {
    type Item = (&'a PathComponent, &'a Node);
    type IntoIter = btree_map::Iter<'a, PathComponent, Node>;

    fn into_iter(self) -> Self::IntoIter {
        self.nodes.iter()
    }
}

/// The amount a node adds to its parent's recursive size: `1` for a leaf, or
/// `1 + child_size` for a directory pointer (overflow-checked).
fn node_size_contribution(node: &Node) -> Result<u64, DirectoryError> {
    match node {
        Node::Directory { size, .. } => 1u64.checked_add(*size).ok_or(DirectoryError::SizeOverflow),
        Node::File { .. } | Node::Symlink { .. } => Ok(1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::Node;
    use crate::path::SymlinkTarget;
    use crate::test_util::pc;

    fn sym(s: &str) -> Node {
        Node::Symlink {
            target: SymlinkTarget::try_from(s).unwrap(),
        }
    }

    #[test]
    fn digest_is_insertion_order_independent() {
        let entries_a = [
            (pc("a"), sym("x")),
            (
                pc("b"),
                Node::File {
                    digest: BlobId::new([7u8; 32].into()),
                    size: 3,
                    executable: false,
                },
            ),
        ];
        let entries_b = [entries_a[1].clone(), entries_a[0].clone()];

        let a = Directory::try_from_iter(entries_a).unwrap();
        let b = Directory::try_from_iter(entries_b).unwrap();

        assert_eq!(a, b);
        assert_eq!(a.digest(), b.digest());
    }

    #[test]
    fn digest_changes_with_content() {
        let d1 = Directory::try_from_iter([(pc("a"), sym("x"))]).unwrap();
        let d2 = Directory::try_from_iter([(pc("a"), sym("y"))]).unwrap();
        assert_ne!(d1.digest(), d2.digest());
    }

    #[test]
    fn get_accepts_any_byte_string() {
        let d = Directory::try_from_iter([(pc("a"), sym("x"))]).unwrap();
        assert_eq!(d.get("a"), d.get(pc("a")));
        assert!(d.get("a").is_some());
        assert!(d.get(b"a".as_slice()).is_some());
        // names that would not validate are simply absent, never an error.
        assert!(d.get("").is_none());
        assert!(d.get("a/b").is_none());
    }

    #[test]
    fn duplicate_name_rejected() {
        let mut d = Directory::new();
        d.add(pc("a"), sym("x")).unwrap();
        assert!(matches!(
            d.add(pc("a"), sym("y")).unwrap_err(),
            DirectoryError::DuplicateName(_)
        ));
    }

    #[test]
    fn size_counts_recursively() {
        // directory pointer of size 5 → 1 + 5 = 6; plus one file → 1; total 7.
        let d = Directory::try_from_iter([
            (
                pc("sub"),
                Node::Directory {
                    digest: DirectoryId::new([1u8; 32].into()),
                    size: 5,
                },
            ),
            (
                pc("f"),
                Node::File {
                    digest: BlobId::new([2u8; 32].into()),
                    size: 10,
                    executable: true,
                },
            ),
        ])
        .unwrap();
        assert_eq!(d.len(), 2);
        assert_eq!(d.size(), 7);
    }

    #[test]
    fn size_overflow_rejected() {
        let mut d = Directory::new();
        d.add(
            pc("big"),
            Node::Directory {
                digest: DirectoryId::new([0u8; 32].into()),
                size: u64::MAX,
            },
        )
        .unwrap_err();
    }

    #[test]
    fn empty_directory_has_stable_digest() {
        assert_eq!(Directory::new().digest(), Directory::new().digest());
        assert_eq!(Directory::new().size(), 0);
        assert!(Directory::new().is_empty());
    }
}
