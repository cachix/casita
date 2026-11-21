//! Native Git objects and immutable repository views.
//!
//! Git's object type is part of the logical namespace, so an untyped wire OID
//! never silently selects an object of another kind. Payloads are the exact
//! native object bodies; verification reconstructs Git's `type size\0`
//! framing and derives the complete generic forward-link set.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use sha1_checked::{Digest as _, Sha1};
use sha2::Sha256;

use crate::format::{FormatError, FormatLimits, ObjectFormat, VerificationContext, VerifiedObject};
use crate::object::{NamespaceId, ObjectKey};

/// Git SHA-1 blob namespace.
pub const GIT_SHA1_BLOB_NAMESPACE: &str = "git.sha1.blob.v1";
/// Git SHA-1 tree namespace.
pub const GIT_SHA1_TREE_NAMESPACE: &str = "git.sha1.tree.v1";
/// Git SHA-1 commit namespace.
pub const GIT_SHA1_COMMIT_NAMESPACE: &str = "git.sha1.commit.v1";
/// Git SHA-1 annotated-tag namespace.
pub const GIT_SHA1_TAG_NAMESPACE: &str = "git.sha1.tag.v1";
/// Git SHA-256 blob namespace.
pub const GIT_SHA256_BLOB_NAMESPACE: &str = "git.sha256.blob.v1";
/// Git SHA-256 tree namespace.
pub const GIT_SHA256_TREE_NAMESPACE: &str = "git.sha256.tree.v1";
/// Git SHA-256 commit namespace.
pub const GIT_SHA256_COMMIT_NAMESPACE: &str = "git.sha256.commit.v1";
/// Git SHA-256 annotated-tag namespace.
pub const GIT_SHA256_TAG_NAMESPACE: &str = "git.sha256.tag.v1";
/// Immutable Git repository-view namespace.
pub const GIT_VIEW_NAMESPACE: &str = "git.view.v1";

const VIEW_MAGIC: &[u8] = b"casita-git-view-v1\0";
const MAX_REF_NAME_BYTES: usize = 1024;
const MAX_VIEW_OBJECTS: usize = 10_000_000;
/// Frozen symbolic-ref traversal ceiling.
pub const MAX_SYMBOLIC_REF_DEPTH: usize = 32;

/// Native Git object hash algorithm selected by one immutable view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GitObjectFormat {
    /// Git's historical SHA-1 format, with collision detection required.
    Sha1,
    /// Git's SHA-256 object format.
    Sha256,
}

impl GitObjectFormat {
    /// Native OID width in bytes.
    pub const fn oid_len(self) -> usize {
        match self {
            Self::Sha1 => 20,
            Self::Sha256 => 32,
        }
    }

    const fn tag(self) -> u8 {
        match self {
            Self::Sha1 => 1,
            Self::Sha256 => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, GitError> {
        match tag {
            1 => Ok(Self::Sha1),
            2 => Ok(Self::Sha256),
            _ => Err(GitError::InvalidView(format!(
                "unknown object-format tag {tag}"
            ))),
        }
    }
}

/// Exact native Git object kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GitObjectKind {
    /// File, symlink target, or opaque byte payload.
    Blob,
    /// Canonical Git tree body.
    Tree,
    /// Canonical Git commit body.
    Commit,
    /// Canonical annotated tag body.
    Tag,
}

impl GitObjectKind {
    /// Git's frozen type spelling used in native hash framing.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Blob => "blob",
            Self::Tree => "tree",
            Self::Commit => "commit",
            Self::Tag => "tag",
        }
    }
}

/// A validated Git tree entry mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitTreeMode {
    /// `040000`: subtree.
    Tree,
    /// `100644`: ordinary file.
    Blob,
    /// `100755`: executable file.
    BlobExecutable,
    /// `120000`: symlink whose target is held in a blob.
    Symlink,
    /// `160000`: submodule commit, preserved but not a generic link.
    Gitlink,
}

impl GitTreeMode {
    fn parse(mode: &[u8]) -> Result<Self, GitError> {
        match mode {
            b"40000" => Ok(Self::Tree),
            b"100644" => Ok(Self::Blob),
            b"100755" => Ok(Self::BlobExecutable),
            b"120000" => Ok(Self::Symlink),
            b"160000" => Ok(Self::Gitlink),
            _ => Err(GitError::InvalidObject(format!(
                "unsupported or non-canonical tree mode `{}`",
                String::from_utf8_lossy(mode)
            ))),
        }
    }

    const fn linked_kind(self) -> Option<GitObjectKind> {
        match self {
            Self::Tree => Some(GitObjectKind::Tree),
            Self::Blob | Self::BlobExecutable | Self::Symlink => Some(GitObjectKind::Blob),
            Self::Gitlink => None,
        }
    }

    const fn sorts_as_tree(self) -> bool {
        matches!(self, Self::Tree)
    }
}

/// One parsed entry from an exact native Git tree body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitTreeEntry {
    /// Native mode semantics.
    pub mode: GitTreeMode,
    /// Raw Git path-component bytes.
    pub name: Vec<u8>,
    /// Native, untyped OID embedded by Git.
    pub oid: Vec<u8>,
}

/// Native Git/view decoding or semantic failure.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum GitError {
    /// The selected namespace is not one of the frozen Git namespaces.
    #[error("unsupported Git namespace `{0}`")]
    UnsupportedNamespace(String),
    /// A native OID has the wrong width.
    #[error("Git {format:?} OID must be {expected} bytes, got {actual}")]
    OidLength {
        /// Selected object format.
        format: GitObjectFormat,
        /// Required width.
        expected: usize,
        /// Observed width.
        actual: usize,
    },
    /// The object body is not structurally valid for its type.
    #[error("invalid native Git object: {0}")]
    InvalidObject(String),
    /// SHA-1 collision detection rejected the framed object.
    #[error("Git SHA-1 collision detected")]
    Sha1Collision,
    /// Native bytes do not reproduce the requested OID.
    #[error("native Git OID mismatch: expected {expected}, observed {actual}")]
    OidMismatch {
        /// Lowercase hexadecimal expected OID.
        expected: String,
        /// Lowercase hexadecimal observed OID.
        actual: String,
    },
    /// Git view bytes or relations are invalid.
    #[error("invalid Git view: {0}")]
    InvalidView(String),
    /// A ref name violates the frozen canonical profile.
    #[error("invalid canonical Git ref name `{name}`: {reason}")]
    InvalidRefName {
        /// Supplied name.
        name: String,
        /// Validation reason.
        reason: String,
    },
    /// A symbolic ref points outside the immutable view.
    #[error("symbolic Git ref `{from}` targets absent ref `{target}`")]
    MissingSymbolicTarget {
        /// Ref containing the symbolic value.
        from: String,
        /// Missing target.
        target: String,
    },
    /// Symbolic resolution encountered a cycle.
    #[error("symbolic Git ref cycle reached `{0}`")]
    SymbolicCycle(String),
    /// Symbolic resolution exceeded the frozen depth bound.
    #[error("symbolic Git ref resolution exceeded {MAX_SYMBOLIC_REF_DEPTH} hops")]
    SymbolicDepth,
    /// A requested ref is absent.
    #[error("Git ref `{0}` is absent")]
    MissingRef(String),
    /// No type-qualified record has the requested native OID.
    #[error("Git OID `{0}` is absent")]
    MissingOid(String),
    /// Multiple type-qualified records share one untyped native OID.
    #[error("Git OID `{oid}` is ambiguous across {matches} object types")]
    AmbiguousOid {
        /// Lowercase hexadecimal OID.
        oid: String,
        /// Number of matching namespaces.
        matches: usize,
    },
    /// An embedded generic key is malformed.
    #[error("invalid embedded object key: {0}")]
    ObjectKey(String),
    /// A state lookup failed.
    #[error("Git object lookup failed: {0}")]
    State(String),
}

/// Construct a type-qualified key from one already known native OID.
pub fn git_object_key(
    format: GitObjectFormat,
    kind: GitObjectKind,
    oid: impl Into<Bytes>,
) -> Result<ObjectKey, GitError> {
    let oid = oid.into();
    require_oid_len(format, &oid)?;
    ObjectKey::new(namespace(format, kind), oid)
        .map_err(|error| GitError::ObjectKey(error.to_string()))
}

/// Hash one complete native body with Git framing and return its qualified key.
pub fn git_object_key_for_body(
    format: GitObjectFormat,
    kind: GitObjectKind,
    body: &[u8],
) -> Result<ObjectKey, GitError> {
    git_object_key(format, kind, hash_body(format, kind, body)?)
}

/// Parse and validate an exact native tree body.
pub fn parse_git_tree(format: GitObjectFormat, body: &[u8]) -> Result<Vec<GitTreeEntry>, GitError> {
    let oid_len = format.oid_len();
    let mut cursor = 0usize;
    let mut entries = Vec::new();
    while cursor < body.len() {
        let mode_end = body[cursor..]
            .iter()
            .position(|byte| *byte == b' ')
            .map(|offset| cursor + offset)
            .ok_or_else(|| GitError::InvalidObject("tree entry has no mode separator".into()))?;
        let mode = GitTreeMode::parse(&body[cursor..mode_end])?;
        cursor = mode_end + 1;
        let name_end = body[cursor..]
            .iter()
            .position(|byte| *byte == 0)
            .map(|offset| cursor + offset)
            .ok_or_else(|| GitError::InvalidObject("tree entry has no name terminator".into()))?;
        let name = &body[cursor..name_end];
        validate_tree_name(name)?;
        cursor = name_end + 1;
        let oid_end = cursor
            .checked_add(oid_len)
            .ok_or_else(|| GitError::InvalidObject("tree OID offset overflow".into()))?;
        let oid = body
            .get(cursor..oid_end)
            .ok_or_else(|| GitError::InvalidObject("tree entry has a truncated OID".into()))?;
        cursor = oid_end;

        let entry = GitTreeEntry {
            mode,
            name: name.to_vec(),
            oid: oid.to_vec(),
        };
        if let Some(previous) = entries.last()
            && git_tree_name_cmp(previous, &entry) != Ordering::Less
        {
            return Err(GitError::InvalidObject(
                "tree entries are not in canonical Git order".into(),
            ));
        }
        entries.push(entry);
    }
    Ok(entries)
}

fn validate_tree_name(name: &[u8]) -> Result<(), GitError> {
    if name.is_empty() || name == b"." || name == b".." || name.contains(&0) || name.contains(&b'/')
    {
        return Err(GitError::InvalidObject(
            "tree entry name is not a safe path component".into(),
        ));
    }
    Ok(())
}

/// Whether writing a tree entry with this name would create, or on some
/// filesystem alias, the `.git` administrative directory.
///
/// This mirrors the component checks of Git's `verify_path` with both
/// `core.protectNTFS` and `core.protectHFS` enabled, whatever the host
/// platform, because a checkout may later be copied to or shared with another
/// filesystem. It matches `.git` in any case, the spellings NTFS resolves to it
/// (trailing dots or spaces, a `:` stream suffix such as `::$INDEX_ALLOCATION`,
/// the `git~1` short name, or a `\` separator), and the spellings HFS+ folds to
/// it by ignoring certain Unicode codepoints.
///
/// [`parse_git_tree`] deliberately accepts such names: they are well formed,
/// Git only reports them from `fsck`, and repositories containing them exist,
/// so the exact objects must still import, verify, and transfer unchanged.
/// Only materializing them onto a filesystem is refused.
pub(crate) fn is_git_admin_alias(name: &[u8]) -> bool {
    is_ntfs_dot_git(name) || is_hfs_dot_git(name)
}

fn is_ntfs_dot_git(name: &[u8]) -> bool {
    let strip = |prefix: &[u8]| {
        name.get(..prefix.len())
            .filter(|head| head.eq_ignore_ascii_case(prefix))
            .map(|_| &name[prefix.len()..])
    };
    let Some(rest) = strip(b".git").or_else(|| strip(b"git~1")) else {
        return false;
    };
    let rest = &rest[rest
        .iter()
        .position(|byte| *byte != b'.' && *byte != b' ')
        .unwrap_or(rest.len())..];
    matches!(rest.first(), None | Some(b'\\' | b':'))
}

fn is_hfs_dot_git(name: &[u8]) -> bool {
    // HFS+ stores names as Unicode and drops these codepoints when comparing,
    // so `.g\u{200c}it` names the same entry as `.git`. Git ignores exactly
    // this set; bytes that are not UTF-8 are escaped rather than folded.
    let Ok(name) = std::str::from_utf8(name) else {
        return false;
    };
    let mut characters = name.chars().filter(|character| {
        !matches!(
            character,
            '\u{200c}'..='\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{206a}'..='\u{206f}' | '\u{feff}'
        )
    });
    ['.', 'g', 'i', 't']
        .into_iter()
        .all(|expected| characters.next().map(|c| c.to_ascii_lowercase()) == Some(expected))
        && characters.next().is_none()
}

fn git_tree_name_cmp(left: &GitTreeEntry, right: &GitTreeEntry) -> Ordering {
    let shared = left.name.len().min(right.name.len());
    let prefix = left.name[..shared].cmp(&right.name[..shared]);
    if prefix != Ordering::Equal {
        return prefix;
    }
    let left_next = left
        .name
        .get(shared)
        .copied()
        .unwrap_or(if left.mode.sorts_as_tree() { b'/' } else { 0 });
    let right_next = right
        .name
        .get(shared)
        .copied()
        .unwrap_or(if right.mode.sorts_as_tree() { b'/' } else { 0 });
    left_next.cmp(&right_next)
}

/// A canonical full ref name stored in a Git view.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CanonicalRefName(String);

impl CanonicalRefName {
    /// Validated ref text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for CanonicalRefName {
    type Error = GitError;

    fn try_from(name: &str) -> Result<Self, Self::Error> {
        validate_ref_name(name)?;
        Ok(Self(name.to_owned()))
    }
}

impl TryFrom<String> for CanonicalRefName {
    type Error = GitError;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        validate_ref_name(&name)?;
        Ok(Self(name))
    }
}

impl std::fmt::Display for CanonicalRefName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

fn validate_ref_name(name: &str) -> Result<(), GitError> {
    let invalid = |reason: &str| GitError::InvalidRefName {
        name: name.to_owned(),
        reason: reason.to_owned(),
    };
    if name.is_empty() || name.len() > MAX_REF_NAME_BYTES {
        return Err(invalid("length is outside the frozen bound"));
    }
    if !name.starts_with("refs/") {
        return Err(invalid("name must be fully qualified below `refs/`"));
    }
    if name.ends_with('/') || name.ends_with('.') || name.contains("..") || name.contains("@{") {
        return Err(invalid("name has a forbidden suffix or sequence"));
    }
    if name.bytes().any(|byte| {
        byte <= 0x20
            || byte == 0x7f
            || matches!(byte, b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\')
    }) {
        return Err(invalid("name contains a forbidden byte"));
    }
    for component in name.split('/') {
        if component.is_empty()
            || component.starts_with('.')
            || component.ends_with(".lock")
            || component == "."
            || component == ".."
        {
            return Err(invalid("name contains a forbidden component"));
        }
    }
    Ok(())
}

/// One direct or same-view symbolic Git ref value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitRefValue {
    /// Exact native Git object selected by this ref.
    Direct(ObjectKey),
    /// Another canonical ref in the same immutable view.
    Symbolic(CanonicalRefName),
}

/// Canonical immutable snapshot of a Git repository's selected refs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitViewBody {
    /// One native object format for the whole view.
    pub object_format: GitObjectFormat,
    /// Canonically ordered direct and symbolic refs.
    pub refs: BTreeMap<CanonicalRefName, GitRefValue>,
    /// Ref selected as the default branch, when any.
    pub default_ref: Option<CanonicalRefName>,
    /// Optional exact native pack retained as a derived full-clone cache.
    ///
    /// Native import installs this only when one source pack's verified index
    /// is exactly the view inventory. The generic object link keeps the blob
    /// alive; fetch falls back to bounded pack generation for every other
    /// request shape.
    pub pack: Option<ObjectKey>,
    /// Exact native objects reachable from every selected direct ref.
    ///
    /// This immutable inventory lets a server bind in time proportional to
    /// distinct objects instead of re-traversing repeated edges across a long
    /// history of wide trees. Native object records still retain their links,
    /// so generic verification, transfer, and collection remain exact.
    pub objects: BTreeSet<ObjectKey>,
}

impl GitViewBody {
    /// Validate and encode the frozen native Git view payload.
    pub fn encode(&self) -> Result<Vec<u8>, GitError> {
        self.validate()?;
        let mut output = VIEW_MAGIC.to_vec();
        output.push(self.object_format.tag());
        put_u64(&mut output, self.refs.len() as u64);
        for (name, value) in &self.refs {
            put_bytes(&mut output, name.as_str().as_bytes());
            match value {
                GitRefValue::Direct(key) => {
                    output.push(0);
                    put_bytes(&mut output, &key.encode());
                }
                GitRefValue::Symbolic(target) => {
                    output.push(1);
                    put_bytes(&mut output, target.as_str().as_bytes());
                }
            }
        }
        put_u64(&mut output, self.objects.len() as u64);
        for key in &self.objects {
            put_bytes(&mut output, &key.encode());
        }
        match &self.default_ref {
            None => output.push(0),
            Some(name) => {
                output.push(1);
                put_bytes(&mut output, name.as_str().as_bytes());
            }
        }
        match &self.pack {
            None => output.push(0),
            Some(pack) => {
                output.push(1);
                put_bytes(&mut output, &pack.encode());
            }
        }
        Ok(output)
    }

    /// Decode exactly one frozen canonical view payload.
    pub fn decode(payload: &[u8]) -> Result<Self, GitError> {
        let mut decoder = ViewDecoder::new(payload);
        decoder.magic(VIEW_MAGIC)?;
        let object_format = GitObjectFormat::from_tag(decoder.byte()?)?;
        let count = decoder.count()?;
        let mut refs = BTreeMap::new();
        let mut previous: Option<CanonicalRefName> = None;
        for _ in 0..count {
            let name = CanonicalRefName::try_from(decoder.text(MAX_REF_NAME_BYTES)?.to_owned())?;
            if previous.as_ref().is_some_and(|prior| prior >= &name) {
                return Err(GitError::InvalidView(
                    "refs are not in canonical strict order".into(),
                ));
            }
            previous = Some(name.clone());
            let value = match decoder.byte()? {
                0 => GitRefValue::Direct(
                    ObjectKey::decode(decoder.bytes(4096)?)
                        .map_err(|error| GitError::ObjectKey(error.to_string()))?,
                ),
                1 => GitRefValue::Symbolic(CanonicalRefName::try_from(
                    decoder.text(MAX_REF_NAME_BYTES)?.to_owned(),
                )?),
                tag => {
                    return Err(GitError::InvalidView(format!(
                        "unknown ref-value tag {tag}"
                    )));
                }
            };
            refs.insert(name, value);
        }
        let object_count = decoder.count()?;
        if object_count > MAX_VIEW_OBJECTS {
            return Err(GitError::InvalidView(format!(
                "view object inventory exceeds {MAX_VIEW_OBJECTS} entries"
            )));
        }
        let mut objects = BTreeSet::new();
        for _ in 0..object_count {
            let key = ObjectKey::decode(decoder.bytes(4096)?)
                .map_err(|error| GitError::ObjectKey(error.to_string()))?;
            if !objects.insert(key) {
                return Err(GitError::InvalidView(
                    "view object inventory contains a duplicate".into(),
                ));
            }
        }
        let default_ref = match decoder.byte()? {
            0 => None,
            1 => Some(CanonicalRefName::try_from(
                decoder.text(MAX_REF_NAME_BYTES)?.to_owned(),
            )?),
            tag => {
                return Err(GitError::InvalidView(format!(
                    "unknown default-ref tag {tag}"
                )));
            }
        };
        let pack = match decoder.byte()? {
            0 => None,
            1 => Some(
                ObjectKey::decode(decoder.bytes(4096)?)
                    .map_err(|error| GitError::ObjectKey(error.to_string()))?,
            ),
            tag => {
                return Err(GitError::InvalidView(format!(
                    "unknown cached-pack tag {tag}"
                )));
            }
        };
        decoder.finish()?;
        let body = Self {
            object_format,
            refs,
            default_ref,
            pack,
            objects,
        };
        body.validate()?;
        Ok(body)
    }

    /// BLAKE3-qualified identity of the canonical view payload.
    pub fn object_key(&self) -> Result<ObjectKey, GitError> {
        let encoded = self.encode()?;
        ObjectKey::new(
            NamespaceId::try_from(GIT_VIEW_NAMESPACE).expect("frozen namespace is valid"),
            Bytes::copy_from_slice(blake3::hash(&encoded).as_bytes()),
        )
        .map_err(|error| GitError::ObjectKey(error.to_string()))
    }

    /// Resolve one ref through same-view symbolic aliases.
    pub fn resolve_ref(&self, name: &CanonicalRefName) -> Result<&ObjectKey, GitError> {
        let mut current = name;
        let mut visited = BTreeSet::new();
        for _ in 0..=MAX_SYMBOLIC_REF_DEPTH {
            if !visited.insert(current.clone()) {
                return Err(GitError::SymbolicCycle(current.to_string()));
            }
            match self
                .refs
                .get(current)
                .ok_or_else(|| GitError::MissingRef(current.to_string()))?
            {
                GitRefValue::Direct(key) => return Ok(key),
                GitRefValue::Symbolic(next) => current = next,
            }
        }
        Err(GitError::SymbolicDepth)
    }

    /// Exact direct targets retained by this view.
    pub fn direct_targets(&self) -> BTreeSet<ObjectKey> {
        self.refs
            .values()
            .filter_map(|value| match value {
                GitRefValue::Direct(key) => Some(key.clone()),
                GitRefValue::Symbolic(_) => None,
            })
            .collect()
    }

    /// Complete canonical inventory of native objects reachable from the view.
    pub fn objects(&self) -> &BTreeSet<ObjectKey> {
        &self.objects
    }

    fn validate(&self) -> Result<(), GitError> {
        if self.refs.len() > 1_000_000 {
            return Err(GitError::InvalidView("too many refs".into()));
        }
        if self.objects.len() > MAX_VIEW_OBJECTS {
            return Err(GitError::InvalidView(format!(
                "view object inventory exceeds {MAX_VIEW_OBJECTS} entries"
            )));
        }
        for key in &self.objects {
            let (format, _) = namespace_parts(key.namespace())?;
            if format != self.object_format {
                return Err(GitError::InvalidView(format!(
                    "object inventory entry {key} uses a different object format"
                )));
            }
        }
        if let Some(target) = self
            .direct_targets()
            .into_iter()
            .find(|target| !self.objects.contains(target))
        {
            return Err(GitError::InvalidView(format!(
                "direct ref target {target} is absent from the object inventory"
            )));
        }
        for (name, value) in &self.refs {
            validate_ref_name(name.as_str())?;
            match value {
                GitRefValue::Direct(key) => {
                    let (format, _) = namespace_parts(key.namespace())?;
                    if format != self.object_format {
                        return Err(GitError::InvalidView(format!(
                            "direct ref `{name}` selects a different object format"
                        )));
                    }
                    require_oid_len(format, key.native_id())?;
                }
                GitRefValue::Symbolic(target) => {
                    if !self.refs.contains_key(target) {
                        return Err(GitError::MissingSymbolicTarget {
                            from: name.to_string(),
                            target: target.to_string(),
                        });
                    }
                }
            }
        }
        for name in self.refs.keys() {
            self.resolve_ref(name)?;
        }
        if let Some(default_ref) = &self.default_ref {
            if !self.refs.contains_key(default_ref) {
                return Err(GitError::MissingRef(default_ref.to_string()));
            }
            self.resolve_ref(default_ref)?;
        }
        if let Some(pack) = &self.pack
            && pack.namespace().as_str() != crate::BLOB_NAMESPACE
        {
            return Err(GitError::InvalidView(format!(
                "cached native pack {pack} is not a raw Casita blob"
            )));
        }
        Ok(())
    }
}

/// Verifier for one exact native Git type/hash namespace.
pub struct GitNativeObjectFormat {
    namespace: NamespaceId,
    object_format: GitObjectFormat,
    kind: GitObjectKind,
}

impl GitNativeObjectFormat {
    /// Construct the verifier selected by an exact object format and type.
    pub fn new(object_format: GitObjectFormat, kind: GitObjectKind) -> Self {
        Self {
            namespace: namespace(object_format, kind),
            object_format,
            kind,
        }
    }
}

#[async_trait]
impl ObjectFormat for GitNativeObjectFormat {
    fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }

    async fn verify(
        &self,
        mut context: VerificationContext<'_>,
        limits: &FormatLimits,
    ) -> Result<VerifiedObject, FormatError> {
        if context.key().namespace() != &self.namespace {
            return Err(FormatError::NamespaceMismatch {
                format: self.namespace.clone(),
                actual: context.key().namespace().clone(),
            });
        }
        require_oid_len(self.object_format, context.key().native_id())
            .map_err(|error| invalid_payload(&self.namespace, error))?;
        let exact_len = context
            .exact_len()
            .ok_or_else(|| FormatError::InvalidPayload {
                namespace: self.namespace.clone(),
                message: "native Git verification requires an exact payload length".into(),
            })?;
        if exact_len > limits.max_payload_bytes {
            return Err(FormatError::PayloadLimit {
                limit: limits.max_payload_bytes,
            });
        }
        let header = git_header(self.kind, exact_len);
        let mut native = NativeHasher::new(self.object_format, &header);
        let mut metadata = if self.kind == GitObjectKind::Blob {
            None
        } else {
            Some(Vec::new())
        };
        let metadata_limit = limits.max_metadata_bytes.min(limits.max_payload_bytes);
        let mut buffer = vec![0u8; limits.read_buffer_bytes.max(1)];
        loop {
            let read = context.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            native.update(&buffer[..read]);
            if let Some(body) = &mut metadata {
                let next = (body.len() as u64)
                    .checked_add(read as u64)
                    .ok_or(FormatError::PayloadSizeOverflow)?;
                if next > metadata_limit {
                    return Err(FormatError::MetadataLimit {
                        limit: metadata_limit,
                    });
                }
                body.extend_from_slice(&buffer[..read]);
            }
        }
        if context.observed_size() != exact_len {
            return Err(FormatError::InvalidPayload {
                namespace: self.namespace.clone(),
                message: format!(
                    "reader declared {exact_len} payload bytes but yielded {}",
                    context.observed_size()
                ),
            });
        }
        let actual = native
            .finish()
            .map_err(|error| invalid_payload(&self.namespace, error))?;
        if actual.as_slice() != context.key().native_id() {
            return Err(invalid_payload(
                &self.namespace,
                GitError::OidMismatch {
                    expected: hex(context.key().native_id()),
                    actual: hex(&actual),
                },
            ));
        }
        let links = match (self.kind, metadata.as_deref()) {
            (GitObjectKind::Blob, _) => Ok(Vec::new()),
            (GitObjectKind::Tree, Some(body)) => tree_links(self.object_format, body),
            (GitObjectKind::Commit, Some(body)) => commit_links(self.object_format, body),
            (GitObjectKind::Tag, Some(body)) => tag_links(self.object_format, body),
            _ => unreachable!("metadata is allocated for every non-blob kind"),
        }
        .map_err(|error| invalid_payload(&self.namespace, error))?;
        if links.len() > limits.max_links_per_object {
            return Err(FormatError::LinkLimit {
                actual: links.len(),
                limit: limits.max_links_per_object,
            });
        }
        context.finish(links)
    }
}

/// Verifier for `git.view.v1`.
pub struct GitViewFormat {
    namespace: NamespaceId,
}

impl Default for GitViewFormat {
    fn default() -> Self {
        Self {
            namespace: NamespaceId::try_from(GIT_VIEW_NAMESPACE)
                .expect("frozen namespace is valid"),
        }
    }
}

#[async_trait]
impl ObjectFormat for GitViewFormat {
    fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }

    async fn verify(
        &self,
        mut context: VerificationContext<'_>,
        limits: &FormatLimits,
    ) -> Result<VerifiedObject, FormatError> {
        if context.key().namespace() != &self.namespace {
            return Err(FormatError::NamespaceMismatch {
                format: self.namespace.clone(),
                actual: context.key().namespace().clone(),
            });
        }
        if context.key().native_id().len() != 32 {
            return Err(FormatError::NativeIdLength {
                namespace: self.namespace.clone(),
                actual: context.key().native_id().len(),
            });
        }
        let body = context
            .read_to_end_bounded(limits.max_metadata_bytes.min(limits.max_payload_bytes))
            .await?;
        if context.observed_digest().as_bytes() != context.key().native_id() {
            return Err(FormatError::InvalidPayload {
                namespace: self.namespace.clone(),
                message: "view payload does not reproduce its BLAKE3 identity".into(),
            });
        }
        let view =
            GitViewBody::decode(&body).map_err(|error| invalid_payload(&self.namespace, error))?;
        // The inventory is a bind accelerator, not a second ownership graph.
        // Direct targets retain the view through the ordinary native links;
        // publication separately proves that the inventory is their exact
        // closure.
        let mut links: Vec<_> = view.direct_targets().into_iter().collect();
        links.extend(view.pack.iter().cloned());
        links.sort();
        links.dedup();
        if links.len() > limits.max_links_per_object {
            return Err(FormatError::LinkLimit {
                actual: links.len(),
                limit: limits.max_links_per_object,
            });
        }
        context.finish(links)
    }
}

pub(crate) fn formats() -> Vec<Arc<dyn ObjectFormat>> {
    let mut formats: Vec<Arc<dyn ObjectFormat>> = Vec::with_capacity(9);
    for object_format in [GitObjectFormat::Sha1, GitObjectFormat::Sha256] {
        for kind in [
            GitObjectKind::Blob,
            GitObjectKind::Tree,
            GitObjectKind::Commit,
            GitObjectKind::Tag,
        ] {
            formats.push(Arc::new(GitNativeObjectFormat::new(object_format, kind)));
        }
    }
    formats.push(Arc::new(GitViewFormat::default()));
    formats
}

fn tree_links(format: GitObjectFormat, body: &[u8]) -> Result<Vec<ObjectKey>, GitError> {
    let mut links = BTreeSet::new();
    for entry in parse_git_tree(format, body)? {
        if let Some(kind) = entry.mode.linked_kind() {
            links.insert(git_object_key(format, kind, entry.oid)?);
        }
    }
    Ok(links.into_iter().collect())
}

fn commit_links(format: GitObjectFormat, body: &[u8]) -> Result<Vec<ObjectKey>, GitError> {
    let headers = parse_headers(body)?;
    let trees: Vec<_> = headers
        .iter()
        .filter(|(name, _)| *name == b"tree")
        .collect();
    if trees.len() != 1 {
        return Err(GitError::InvalidObject(
            "commit must contain exactly one tree header".into(),
        ));
    }
    let mut links = BTreeSet::new();
    links.insert(git_object_key(
        format,
        GitObjectKind::Tree,
        parse_hex_oid(format, trees[0].1)?,
    )?);
    for (_, value) in headers.iter().filter(|(name, _)| *name == b"parent") {
        let parent = git_object_key(format, GitObjectKind::Commit, parse_hex_oid(format, value)?)?;
        if !links.insert(parent) {
            return Err(GitError::InvalidObject(
                "commit contains a duplicate parent".into(),
            ));
        }
    }
    Ok(links.into_iter().collect())
}

fn tag_links(format: GitObjectFormat, body: &[u8]) -> Result<Vec<ObjectKey>, GitError> {
    let headers = parse_headers(body)?;
    let objects: Vec<_> = headers
        .iter()
        .filter(|(name, _)| *name == b"object")
        .collect();
    let types: Vec<_> = headers
        .iter()
        .filter(|(name, _)| *name == b"type")
        .collect();
    if objects.len() != 1 || types.len() != 1 {
        return Err(GitError::InvalidObject(
            "tag must contain exactly one object and type header".into(),
        ));
    }
    let kind = match types[0].1 {
        b"blob" => GitObjectKind::Blob,
        b"tree" => GitObjectKind::Tree,
        b"commit" => GitObjectKind::Commit,
        b"tag" => GitObjectKind::Tag,
        other => {
            return Err(GitError::InvalidObject(format!(
                "unknown tag target type `{}`",
                String::from_utf8_lossy(other)
            )));
        }
    };
    Ok(vec![git_object_key(
        format,
        kind,
        parse_hex_oid(format, objects[0].1)?,
    )?])
}

type GitHeader<'a> = (&'a [u8], &'a [u8]);

fn parse_headers(body: &[u8]) -> Result<Vec<GitHeader<'_>>, GitError> {
    let header_end = body
        .windows(2)
        .position(|window| window == b"\n\n")
        .unwrap_or(body.len());
    let block = &body[..header_end];
    let mut headers = Vec::new();
    for line in block.split(|byte| *byte == b'\n') {
        if line.is_empty() || line[0] == b' ' {
            continue;
        }
        let Some(space) = line.iter().position(|byte| *byte == b' ') else {
            return Err(GitError::InvalidObject(
                "Git header line has no value separator".into(),
            ));
        };
        let name = &line[..space];
        let value = &line[space + 1..];
        if name.is_empty()
            || value.is_empty()
            || name
                .iter()
                .any(|byte| !byte.is_ascii_lowercase() && !byte.is_ascii_digit() && *byte != b'-')
            || name[0] == b'-'
            || name.last() == Some(&b'-')
        {
            return Err(GitError::InvalidObject(
                "Git header name or value is invalid".into(),
            ));
        }
        headers.push((name, value));
    }
    Ok(headers)
}

fn parse_hex_oid(format: GitObjectFormat, value: &[u8]) -> Result<Vec<u8>, GitError> {
    if value.len() != format.oid_len() * 2 {
        return Err(GitError::OidLength {
            format,
            expected: format.oid_len(),
            actual: value.len() / 2,
        });
    }
    let mut oid = Vec::with_capacity(format.oid_len());
    for pair in value.chunks_exact(2) {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        oid.push((high << 4) | low);
    }
    Ok(oid)
}

fn hex_nibble(byte: u8) -> Result<u8, GitError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(GitError::InvalidObject(
            "OID is not canonical lowercase hexadecimal".into(),
        )),
    }
}

fn namespace(format: GitObjectFormat, kind: GitObjectKind) -> NamespaceId {
    let value = match (format, kind) {
        (GitObjectFormat::Sha1, GitObjectKind::Blob) => GIT_SHA1_BLOB_NAMESPACE,
        (GitObjectFormat::Sha1, GitObjectKind::Tree) => GIT_SHA1_TREE_NAMESPACE,
        (GitObjectFormat::Sha1, GitObjectKind::Commit) => GIT_SHA1_COMMIT_NAMESPACE,
        (GitObjectFormat::Sha1, GitObjectKind::Tag) => GIT_SHA1_TAG_NAMESPACE,
        (GitObjectFormat::Sha256, GitObjectKind::Blob) => GIT_SHA256_BLOB_NAMESPACE,
        (GitObjectFormat::Sha256, GitObjectKind::Tree) => GIT_SHA256_TREE_NAMESPACE,
        (GitObjectFormat::Sha256, GitObjectKind::Commit) => GIT_SHA256_COMMIT_NAMESPACE,
        (GitObjectFormat::Sha256, GitObjectKind::Tag) => GIT_SHA256_TAG_NAMESPACE,
    };
    NamespaceId::try_from(value).expect("frozen namespace is valid")
}

fn namespace_parts(namespace: &NamespaceId) -> Result<(GitObjectFormat, GitObjectKind), GitError> {
    match namespace.as_str() {
        GIT_SHA1_BLOB_NAMESPACE => Ok((GitObjectFormat::Sha1, GitObjectKind::Blob)),
        GIT_SHA1_TREE_NAMESPACE => Ok((GitObjectFormat::Sha1, GitObjectKind::Tree)),
        GIT_SHA1_COMMIT_NAMESPACE => Ok((GitObjectFormat::Sha1, GitObjectKind::Commit)),
        GIT_SHA1_TAG_NAMESPACE => Ok((GitObjectFormat::Sha1, GitObjectKind::Tag)),
        GIT_SHA256_BLOB_NAMESPACE => Ok((GitObjectFormat::Sha256, GitObjectKind::Blob)),
        GIT_SHA256_TREE_NAMESPACE => Ok((GitObjectFormat::Sha256, GitObjectKind::Tree)),
        GIT_SHA256_COMMIT_NAMESPACE => Ok((GitObjectFormat::Sha256, GitObjectKind::Commit)),
        GIT_SHA256_TAG_NAMESPACE => Ok((GitObjectFormat::Sha256, GitObjectKind::Tag)),
        other => Err(GitError::UnsupportedNamespace(other.to_owned())),
    }
}

/// Split a native Git object key into its object format, kind and OID,
/// rejecting non-Git namespaces and OIDs of the wrong length.
pub fn git_key_parts(key: &ObjectKey) -> Result<(GitObjectFormat, GitObjectKind, &[u8]), GitError> {
    let (format, kind) = namespace_parts(key.namespace())?;
    require_oid_len(format, key.native_id())?;
    Ok((format, kind, key.native_id()))
}

fn require_oid_len(format: GitObjectFormat, oid: &[u8]) -> Result<(), GitError> {
    if oid.len() == format.oid_len() {
        Ok(())
    } else {
        Err(GitError::OidLength {
            format,
            expected: format.oid_len(),
            actual: oid.len(),
        })
    }
}

fn git_header(kind: GitObjectKind, size: u64) -> Vec<u8> {
    format!("{} {size}\0", kind.as_str()).into_bytes()
}

fn hash_body(
    format: GitObjectFormat,
    kind: GitObjectKind,
    body: &[u8],
) -> Result<Vec<u8>, GitError> {
    let mut hasher = NativeHasher::new(format, &git_header(kind, body.len() as u64));
    hasher.update(body);
    hasher.finish()
}

enum NativeHasher {
    Sha1(Box<Sha1>),
    Sha256(Sha256),
}

impl NativeHasher {
    fn new(format: GitObjectFormat, header: &[u8]) -> Self {
        match format {
            GitObjectFormat::Sha1 => {
                let mut hasher = Sha1::new();
                hasher.update(header);
                Self::Sha1(Box::new(hasher))
            }
            GitObjectFormat::Sha256 => {
                let mut hasher = Sha256::new();
                hasher.update(header);
                Self::Sha256(hasher)
            }
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Sha1(hasher) => hasher.update(bytes),
            Self::Sha256(hasher) => hasher.update(bytes),
        }
    }

    fn finish(self) -> Result<Vec<u8>, GitError> {
        match self {
            Self::Sha1(hasher) => {
                let result = (*hasher).try_finalize();
                if result.has_collision() {
                    Err(GitError::Sha1Collision)
                } else {
                    Ok(result.hash().to_vec())
                }
            }
            Self::Sha256(hasher) => Ok(hasher.finalize().to_vec()),
        }
    }
}

fn invalid_payload(namespace: &NamespaceId, error: GitError) -> FormatError {
    FormatError::InvalidPayload {
        namespace: namespace.clone(),
        message: error.to_string(),
    }
}

fn hex(bytes: &[u8]) -> String {
    data_encoding::HEXLOWER.encode(bytes)
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    put_u64(output, bytes.len() as u64);
    output.extend_from_slice(bytes);
}

struct ViewDecoder<'a> {
    input: &'a [u8],
    cursor: usize,
}

impl<'a> ViewDecoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, cursor: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], GitError> {
        let end = self
            .cursor
            .checked_add(len)
            .ok_or_else(|| GitError::InvalidView("payload offset overflow".into()))?;
        let bytes = self
            .input
            .get(self.cursor..end)
            .ok_or_else(|| GitError::InvalidView("truncated payload".into()))?;
        self.cursor = end;
        Ok(bytes)
    }

    fn magic(&mut self, magic: &[u8]) -> Result<(), GitError> {
        if self.take(magic.len())? == magic {
            Ok(())
        } else {
            Err(GitError::InvalidView("invalid payload magic".into()))
        }
    }

    fn byte(&mut self) -> Result<u8, GitError> {
        Ok(self.take(1)?[0])
    }

    fn u64(&mut self) -> Result<u64, GitError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("exact width requested"),
        ))
    }

    fn count(&mut self) -> Result<usize, GitError> {
        let count = usize::try_from(self.u64()?)
            .map_err(|_| GitError::InvalidView("ref count overflows usize".into()))?;
        if count > 1_000_000 {
            return Err(GitError::InvalidView("too many refs".into()));
        }
        Ok(count)
    }

    fn bytes(&mut self, limit: usize) -> Result<&'a [u8], GitError> {
        let len = usize::try_from(self.u64()?)
            .map_err(|_| GitError::InvalidView("field length overflows usize".into()))?;
        if len > limit {
            return Err(GitError::InvalidView(format!(
                "field length {len} exceeds limit {limit}"
            )));
        }
        self.take(len)
    }

    fn text(&mut self, limit: usize) -> Result<&'a str, GitError> {
        std::str::from_utf8(self.bytes(limit)?)
            .map_err(|_| GitError::InvalidView("ref name is not UTF-8".into()))
    }

    fn finish(self) -> Result<(), GitError> {
        if self.cursor == self.input.len() {
            Ok(())
        } else {
            Err(GitError::InvalidView("trailing payload bytes".into()))
        }
    }
}

/// Resolve one untyped OID by exact lookup across all four namespaces.
#[cfg(feature = "native")]
pub async fn resolve_git_oid(
    snapshot: &dyn crate::metadata::MetadataSnapshot,
    format: GitObjectFormat,
    oid: &[u8],
) -> Result<ObjectKey, GitError> {
    resolve_git_oid_record(snapshot, format, oid)
        .await
        .map(|(key, _)| key)
}

/// Resolve one untyped OID and return its qualified key and record together.
///
/// Keeping the record avoids a fifth state lookup in synchronous object-store
/// adapters after the four namespaces have already been searched.
#[cfg(feature = "native")]
pub(crate) async fn resolve_git_oid_record(
    snapshot: &dyn crate::metadata::MetadataSnapshot,
    format: GitObjectFormat,
    oid: &[u8],
) -> Result<(ObjectKey, crate::object::ObjectRecord), GitError> {
    require_oid_len(format, oid)?;
    let keys = [
        GitObjectKind::Blob,
        GitObjectKind::Tree,
        GitObjectKind::Commit,
        GitObjectKind::Tag,
    ]
    .into_iter()
    .map(|kind| git_object_key(format, kind, Bytes::copy_from_slice(oid)))
    .collect::<Result<Vec<_>, _>>()?;
    let records = snapshot
        .object_batch(&keys)
        .await
        .map_err(|error| GitError::State(error.to_string()))?;
    let mut matches = keys
        .into_iter()
        .zip(records)
        .filter_map(|(key, record)| record.map(|record| (key, record)));
    let Some(found) = matches.next() else {
        return Err(GitError::MissingOid(hex(oid)));
    };
    if matches.next().is_some() {
        let count = 2 + matches.count();
        return Err(GitError::AmbiguousOid {
            oid: hex(oid),
            matches: count,
        });
    }
    Ok(found)
}

/// Resolve a header without decoding record links; all namespaces still participate.
#[cfg(feature = "git")]
pub(crate) async fn resolve_git_oid_payload(
    snapshot: &dyn crate::metadata::MetadataSnapshot,
    format: GitObjectFormat,
    oid: &[u8],
) -> Result<(ObjectKey, (crate::BlobId, u64)), GitError> {
    require_oid_len(format, oid)?;
    let keys = [
        GitObjectKind::Blob,
        GitObjectKind::Tree,
        GitObjectKind::Commit,
        GitObjectKind::Tag,
    ]
    .into_iter()
    .map(|kind| git_object_key(format, kind, Bytes::copy_from_slice(oid)))
    .collect::<Result<Vec<_>, _>>()?;
    let records = snapshot
        .object_payload_batch(&keys)
        .await
        .map_err(|error| GitError::State(error.to_string()))?;
    let mut matches = keys
        .into_iter()
        .zip(records)
        .filter_map(|(key, record)| record.map(|record| (key, record)));
    let Some(found) = matches.next() else {
        return Err(GitError::MissingOid(hex(oid)));
    };
    if matches.next().is_some() {
        let count = 2 + matches.count();
        return Err(GitError::AmbiguousOid {
            oid: hex(oid),
            matches: count,
        });
    }
    Ok(found)
}

#[cfg(all(test, feature = "native"))]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::format::{FormatRegistry, PayloadReader};

    struct ExactReader {
        inner: Cursor<Vec<u8>>,
        len: u64,
    }

    #[async_trait]
    impl PayloadReader for ExactReader {
        fn exact_len(&self) -> Option<u64> {
            Some(self.len)
        }

        async fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            std::io::Read::read(&mut self.inner, buffer)
        }
    }

    async fn verify(
        format: GitObjectFormat,
        kind: GitObjectKind,
        body: &[u8],
    ) -> crate::ObjectRecord {
        let key = git_object_key_for_body(format, kind, body).unwrap();
        let mut reader = ExactReader {
            inner: Cursor::new(body.to_vec()),
            len: body.len() as u64,
        };
        FormatRegistry::builtin()
            .verify(&key, &mut reader, &FormatLimits::default())
            .await
            .unwrap()
            .into_record()
    }

    #[tokio::test]
    async fn native_empty_blob_vectors_are_frozen() {
        let sha1 =
            git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Blob, b"").unwrap();
        let sha256 =
            git_object_key_for_body(GitObjectFormat::Sha256, GitObjectKind::Blob, b"").unwrap();
        assert_eq!(
            hex(sha1.native_id()),
            "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
        );
        assert_eq!(
            hex(sha256.native_id()),
            "473a0f4c3be8a93681a267e3b1e9a7dcda1185436fe141f7749120a303721813"
        );
        assert!(
            verify(GitObjectFormat::Sha1, GitObjectKind::Blob, b"")
                .await
                .links()
                .is_empty()
        );
    }

    #[test]
    fn known_sha1_collision_is_rejected_by_the_frozen_hasher() {
        // SHA-mbles chosen-prefix collision fixture from sha1-checked's
        // published test corpus. Embedding it keeps this security contract
        // deterministic and independent of network access.
        let fixture = data_encoding::BASE64
            .decode(b"mQQNBH/oF4ABIAD/S2V5IGlzIHBhcnQgb2YgYSBjb2xsaXNpb24hIEl0J3MgYSB0cmFwIXnGGvCvzAVFFdknTnMHYksdx/sjmIu43otXXbp7nqsxwWdLbZdDeKgncy/1hRx2ouYHcrWkfOHqxAu5k8EtjHDiSk+NX83twbMsnPGeMa8kKXWdQuTf2zFxn1h2I+5VKTm23NxFn8pTVTtw+H7eMKJH6jr2x1mi8gsyDXYNtk/0eQhP08yzzdSDYtlqnEMGF8r/bDbGN+U/3ihBf2Jv7FTteUOkbl9XMPK7OPsd9uAJABDQDiSteL+SZBmTYI6NFYp4nzTEb+HmAn81pMv7gnB2xQ7KDot8ymm7LCt5Aln5v5Vw3Y1EN6MRX6/3w8rAmtJSZgVcJxBHVReOrv+CWiyqKs+13mTOdkHcWaVBqfycdWdW4uI9xxPIwkyXkKprDjin9V8URSocooUN3ZVi/ZoYrUJJaqlwCPdGcvaO9GHriLCZM9YmtPkYdJzAJ/3dbEJfxCFoNdATTRUoW6sst4Sk98u0+1FNS/D2I3zwCp6fEyuaBm5v0X9sQph0eFhv9lGvlnR/tCa5hyuaiOQGP1m7M0zABlD4OoDEJ1G3GXTTAPwoGaLo8eMsG1HLGOa/xNubrvZ11Kr1sVdKBH+PbdLsFTqTQSKTl02Sj4jO2TY8/vl84udCvzTJa47zh1Z2/qXMqOX33qC6skE9TeAO5x7gHxYr220er9kl5q66rmo1TvF88gWkBPvbEvxFTUH92VzyRZZkoq0DLR2mCnMmQHXX8eDWwUA656DYYd8/5XBxiN1eB9FYm5+LZjBVP4/DUrPgwn2oC926TGQCDQ==")
            .unwrap();
        assert_eq!(fixture.len(), 640);
        let mut hasher = NativeHasher::new(GitObjectFormat::Sha1, &[]);
        hasher.update(&fixture);
        assert_eq!(hasher.finish(), Err(GitError::Sha1Collision));
    }

    #[tokio::test]
    async fn tree_commit_tag_links_and_gitlink_are_exact() {
        let blob =
            git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Blob, b"x").unwrap();
        let subtree =
            git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Tree, b"").unwrap();
        let mut tree = Vec::new();
        tree.extend_from_slice(b"100644 a\0");
        tree.extend_from_slice(blob.native_id());
        tree.extend_from_slice(b"160000 module\0");
        tree.extend_from_slice(&[7; 20]);
        tree.extend_from_slice(b"40000 z\0");
        tree.extend_from_slice(subtree.native_id());
        let tree_record = verify(GitObjectFormat::Sha1, GitObjectKind::Tree, &tree).await;
        assert_eq!(
            tree_record.links(),
            &[blob.clone(), subtree.clone()]
                .into_iter()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
        );

        let commit_body = format!(
            "tree {}\nauthor A <a@b> 0 +0000\ncommitter A <a@b> 0 +0000\n\nmsg\n",
            hex(
                git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Tree, &tree)
                    .unwrap()
                    .native_id()
            )
        );
        let commit = git_object_key_for_body(
            GitObjectFormat::Sha1,
            GitObjectKind::Commit,
            commit_body.as_bytes(),
        )
        .unwrap();
        let commit_record = verify(
            GitObjectFormat::Sha1,
            GitObjectKind::Commit,
            commit_body.as_bytes(),
        )
        .await;
        assert_eq!(commit_record.links().len(), 1);

        let tag_body = format!(
            "object {}\ntype commit\ntag v1\ntagger A <a@b> 0 +0000\n\nrelease\n",
            hex(commit.native_id())
        );
        let tag_record = verify(
            GitObjectFormat::Sha1,
            GitObjectKind::Tag,
            tag_body.as_bytes(),
        )
        .await;
        assert_eq!(tag_record.links(), &[commit]);
    }

    #[test]
    fn view_roundtrip_symbolic_resolution_and_validation() {
        let main = CanonicalRefName::try_from("refs/heads/main").unwrap();
        let alias = CanonicalRefName::try_from("refs/heads/default").unwrap();
        let commit = git_object_key_for_body(
            GitObjectFormat::Sha1,
            GitObjectKind::Commit,
            b"tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\n\n",
        )
        .unwrap();
        let view = GitViewBody {
            object_format: GitObjectFormat::Sha1,
            refs: BTreeMap::from([
                (alias.clone(), GitRefValue::Symbolic(main.clone())),
                (main.clone(), GitRefValue::Direct(commit.clone())),
            ]),
            default_ref: Some(alias.clone()),
            pack: None,
            objects: BTreeSet::from([commit.clone()]),
        };
        let encoded = view.encode().unwrap();
        assert_eq!(GitViewBody::decode(&encoded).unwrap(), view);
        assert_eq!(view.resolve_ref(&alias).unwrap(), &commit);
        assert!(CanonicalRefName::try_from("refs/heads/bad..name").is_err());

        let cyclic = GitViewBody {
            object_format: GitObjectFormat::Sha1,
            refs: BTreeMap::from([
                (alias.clone(), GitRefValue::Symbolic(main.clone())),
                (main, GitRefValue::Symbolic(alias)),
            ]),
            default_ref: None,
            pack: None,
            objects: BTreeSet::new(),
        };
        assert!(matches!(cyclic.encode(), Err(GitError::SymbolicCycle(_))));
    }

    #[test]
    fn git_admin_aliases_follow_git_verify_path() {
        for name in [
            ".git",
            ".GIT",
            ".Git",
            ".git.",
            ".git. .",
            ".git ",
            ".git::$INDEX_ALLOCATION",
            ".GIT::$index_allocation",
            ".git:stream",
            ".git\\hooks",
            "git~1",
            "GIT~1",
            "git~1.. ",
            ".g\u{200c}it",
            "\u{feff}.GI\u{202e}T\u{206f}",
        ] {
            assert!(is_git_admin_alias(name.as_bytes()), "{name:?}");
        }
        for name in [
            "git",
            ".gi",
            ".gitx",
            ".gitignore",
            ".gitmodules",
            ".git.x",
            "x.git",
            "git~2",
            "git~10",
            ".g\u{00ef}t",
            ".git\u{200b}",
        ] {
            assert!(!is_git_admin_alias(name.as_bytes()), "{name:?}");
        }
        assert!(!is_git_admin_alias(b".git\xff"));
        // Well formed trees naming `.git` still parse: refusal is a checkout
        // decision, so such objects import and transfer unchanged.
        let mut tree = b"100644 .GIT\0".to_vec();
        tree.extend_from_slice(&[1; 20]);
        assert_eq!(
            parse_git_tree(GitObjectFormat::Sha1, &tree).unwrap()[0].name,
            b".GIT"
        );
    }
}
