//! Repository operations for immutable native Git views.

#[cfg(feature = "git")]
use std::collections::BTreeSet;
use std::collections::{BTreeMap, VecDeque};
#[cfg(feature = "git")]
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(feature = "git")]
use std::sync::atomic::AtomicBool;

use futures::TryStreamExt;
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;

use crate::BlobStore;
use crate::filesystem::checkout::{CheckoutStage, checkout_path_error};
use crate::git::{GIT_VIEW_NAMESPACE, GitError, GitObjectFormat, GitViewBody, git_key_parts};
use crate::metadata::MetadataStore;
use crate::object::{ObjectKey, RepositoryRevision, RootName};
use crate::repository::{ClosureStatus, Repository, RepositoryError};

/// Default upper bound for retaining a verified source-native pack as a
/// rebuildable full-clone cache.
pub const DEFAULT_MAX_CACHED_GIT_PACK_BYTES: u64 = 8 * 1024 * 1024 * 1024;
/// Maximum simultaneously staged source objects by default.
pub const DEFAULT_GIT_IMPORT_CONCURRENCY: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(16).unwrap();
/// Default budget for decoded source bytes held by staging futures.
pub const DEFAULT_GIT_IMPORT_BUFFERED_BYTES: std::num::NonZeroU64 =
    std::num::NonZeroU64::new(64 * 1024 * 1024).unwrap();
#[cfg(feature = "git")]
const MAX_GIT_IMPORT_BATCH_LINKS: usize = 1_000_000;
#[cfg(feature = "git")]
const MAX_GIT_SOURCE_MAPPING_WINDOW_BYTES: u64 = 128 * 1024 * 1024;

/// Policy for `160000` entries during native Git tree checkout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitlinkCheckoutPolicy {
    /// Refuse a tree containing a gitlink.
    Error,
    /// Materialize a gitlink as an empty directory, matching `git archive`.
    Skip,
}

/// Result of atomically installing one immutable Git view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitViewPublication {
    /// Exact view object selected by the root.
    pub view: ObjectKey,
    /// Revision containing the new view/root selection.
    pub revision: RepositoryRevision,
}

/// Source witness for one complete rebuildable Git OID index generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GitOidIndexCheckpoint {
    /// Exact repository state scanned completely.
    pub revision: RepositoryRevision,
    /// Number of type-qualified Git records indexed.
    pub objects: usize,
}

#[derive(Clone)]
struct GitOidIndexGeneration {
    checkpoint: GitOidIndexCheckpoint,
    entries: BTreeMap<(GitObjectFormat, Vec<u8>), Vec<ObjectKey>>,
}

/// Optional complete, atomically replaced acceleration index for untyped OIDs.
#[derive(Clone, Default)]
pub struct GitOidIndex {
    generation: Arc<RwLock<Option<GitOidIndexGeneration>>>,
}

impl GitOidIndex {
    /// Empty derived index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Current complete checkpoint, when built.
    pub async fn checkpoint(&self) -> Option<GitOidIndexCheckpoint> {
        self.generation
            .read()
            .await
            .as_ref()
            .map(|generation| generation.checkpoint)
    }

    /// Rebuild from every Git-native object record in one stable snapshot and
    /// atomically replace the visible generation.
    #[tracing::instrument(name = "git.oid_index.rebuild", skip_all)]
    pub async fn rebuild<PS, SS>(
        &self,
        repository: &Repository<PS, SS>,
    ) -> Result<GitOidIndexCheckpoint, GitViewError>
    where
        PS: BlobStore,
        SS: MetadataStore,
    {
        let hold = repository.retention_hold().await?;
        let snapshot = hold.snapshot();
        let revision = snapshot.revision();
        let mut records = snapshot.objects_unordered();
        let max_objects = repository.limits().max_traversal_objects;
        let mut entries = BTreeMap::<(GitObjectFormat, Vec<u8>), Vec<ObjectKey>>::new();
        let mut objects = 0usize;
        let mut scanned = 0usize;
        while let Some(record) = records
            .try_next()
            .await
            .map_err(RepositoryError::Metadata)?
        {
            scanned = scanned.checked_add(1).ok_or_else(|| {
                RepositoryError::LimitExceeded("Git OID index object count overflowed".into())
            })?;
            if scanned > max_objects {
                return Err(RepositoryError::LimitExceeded(format!(
                    "Git OID index rebuild exceeded {max_objects} objects"
                ))
                .into());
            }
            let Ok((format, _, oid)) = git_key_parts(record.key()) else {
                continue;
            };
            entries
                .entry((format, oid.to_vec()))
                .or_default()
                .push(record.key().clone());
            objects += 1;
        }
        for matches in entries.values_mut() {
            matches.sort();
        }
        let checkpoint = GitOidIndexCheckpoint { revision, objects };
        *self.generation.write().await = Some(GitOidIndexGeneration {
            checkpoint,
            entries,
        });
        tracing::info!(revision = %checkpoint.revision, objects, "Git OID index rebuilt");
        Ok(checkpoint)
    }

    /// Resolve through the complete index. Ambiguity is retained rather than
    /// hidden by an arbitrary object-kind preference.
    pub async fn resolve(
        &self,
        format: GitObjectFormat,
        oid: &[u8],
    ) -> Result<ObjectKey, GitError> {
        if oid.len() != format.oid_len() {
            return Err(GitError::OidLength {
                format,
                expected: format.oid_len(),
                actual: oid.len(),
            });
        }
        let generation = self.generation.read().await;
        let generation = generation
            .as_ref()
            .ok_or_else(|| GitError::State("Git OID index has not been built".into()))?;
        let matches = generation
            .entries
            .get(&(format, oid.to_vec()))
            .map(Vec::as_slice)
            .unwrap_or_default();
        match matches {
            [] => Err(GitError::MissingOid(data_encoding::HEXLOWER.encode(oid))),
            [key] => Ok(key.clone()),
            many => Err(GitError::AmbiguousOid {
                oid: data_encoding::HEXLOWER.encode(oid),
                matches: many.len(),
            }),
        }
    }
}

/// Native Git view import/mutation failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum GitViewError {
    /// Native format or view validation failed.
    #[error(transparent)]
    Git(#[from] GitError),
    /// Generic repository mutation/read failed.
    #[error(transparent)]
    Repository(#[from] RepositoryError),
    /// The selected root name is malformed.
    #[error("invalid Git view name: {0}")]
    InvalidViewName(String),
    /// A local Git source could not be opened or decoded.
    #[cfg(feature = "git")]
    #[error("native Git import failed: {0}")]
    Import(String),
}

impl GitViewError {
    /// Stable failure category shared by application and transport adapters.
    pub fn category(&self) -> crate::RepositoryErrorCategory {
        use crate::RepositoryErrorCategory as Category;
        match self {
            Self::Repository(error) => error.category(),
            Self::InvalidViewName(_) => Category::InvalidInput,
            Self::Git(_) => Category::InvalidData,
            #[cfg(feature = "git")]
            Self::Import(_) => Category::Backend,
        }
    }
}

/// Derive the ordinary root name for one Git view selector.
pub fn git_view_root_name(view_name: &str) -> Result<RootName, GitViewError> {
    if view_name.is_empty() || view_name.contains('/') {
        return Err(GitViewError::InvalidViewName(
            "the view name must be one non-empty root segment".into(),
        ));
    }
    RootName::try_from(format!("git/{view_name}"))
        .map_err(|error| GitViewError::InvalidViewName(error.to_string()))
}

/// Verify, store, and atomically select one complete immutable Git view.
#[tracing::instrument(name = "git.view.publish", skip_all)]
pub async fn publish_git_view<PS, SS>(
    repository: &Repository<PS, SS>,
    view_name: &str,
    view: &GitViewBody,
) -> Result<GitViewPublication, GitViewError>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    if view.pack.is_some() {
        return Err(GitError::InvalidView(
            "caller-authored Git views cannot install a native-pack cache".into(),
        )
        .into());
    }
    let root_name = git_view_root_name(view_name)?;
    let payload = view.encode()?;
    validate_git_view_inventory(repository, view).await?;
    let key = view.object_key()?;
    let mutation = repository.mutation_session().await?;
    let object = mutation.stage_object(key.clone(), &payload).await?;
    let result = mutation
        .publish_rooted(vec![object], root_name, key.clone())
        .await?;
    tracing::info!(revision = %result.revision, "Git view published");
    Ok(GitViewPublication {
        view: key,
        revision: result.revision,
    })
}

/// Prove that a caller-authored acceleration inventory is exactly the closure
/// of the selected direct refs. Native import builds the same set while it
/// verifies source objects and therefore uses the narrower constructed
/// publication path instead of paying for this second traversal.
async fn validate_git_view_inventory<PS, SS>(
    repository: &Repository<PS, SS>,
    view: &GitViewBody,
) -> Result<(), GitViewError>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    const LOOKUP_BATCH: usize = 1_024;

    let hold = repository.retention_hold().await?;
    let mut reachable = view.direct_targets();
    let mut queue: VecDeque<_> = reachable.iter().cloned().collect();
    while !queue.is_empty() {
        let mut keys = Vec::with_capacity(LOOKUP_BATCH.min(queue.len()));
        while keys.len() < LOOKUP_BATCH {
            let Some(key) = queue.pop_front() else {
                break;
            };
            keys.push(key);
        }
        let found = hold
            .snapshot()
            .object_batch(&keys)
            .await
            .map_err(RepositoryError::Metadata)?;
        for (key, record) in keys.into_iter().zip(found) {
            let record = record.ok_or_else(|| RepositoryError::Absent(key.to_string()))?;
            for link in record.links() {
                if !view.objects().contains(link) {
                    return Err(GitError::InvalidView(format!(
                        "reachable object {link} is absent from the object inventory"
                    ))
                    .into());
                }
                if reachable.insert(link.clone()) {
                    queue.push_back(link.clone());
                }
            }
        }
    }
    if let Some(extra) = view.objects().iter().find(|key| !reachable.contains(*key)) {
        return Err(GitError::InvalidView(format!(
            "object inventory contains unreachable object {extra}"
        ))
        .into());
    }
    Ok(())
}

/// Read and verify the currently selected immutable view.
#[tracing::instrument(name = "git.view.read", level = "debug", skip_all)]
pub async fn read_git_view<PS, SS>(
    repository: &Repository<PS, SS>,
    view_name: &str,
) -> Result<Option<(ObjectKey, GitViewBody)>, GitViewError>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    let root_name = git_view_root_name(view_name)?;
    let hold = repository.retention_hold().await?;
    let Some(key) = hold
        .snapshot()
        .root(&root_name)
        .await
        .map_err(RepositoryError::Metadata)?
    else {
        return Ok(None);
    };
    if key.namespace().as_str() != GIT_VIEW_NAMESPACE {
        return Err(GitError::InvalidView(format!(
            "root `{root_name}` selects non-view object {key}"
        ))
        .into());
    }
    // Publication records a durable closure witness for this exact immutable
    // view. Re-reading it should validate that witness, not rescan the entire
    // Git history on every `show` operation.
    match hold.verify_closure_incremental(&key).await? {
        ClosureStatus::Complete { .. } => {}
        status => {
            return Err(RepositoryError::ObjectNotReadable {
                object: key,
                status,
            }
            .into());
        }
    }
    let Some((record, mut reader)) = hold.open_payload(&key).await? else {
        return Err(RepositoryError::Absent(key.to_string()).into());
    };
    let capacity = usize::try_from(record.payload_size()).map_err(|_| {
        RepositoryError::LimitExceeded("Git view payload does not fit this platform".into())
    })?;
    if record.payload_size() > repository.limits().max_metadata_bytes {
        return Err(
            RepositoryError::LimitExceeded("Git view exceeds metadata read limit".into()).into(),
        );
    }
    let mut payload = Vec::with_capacity(capacity);
    reader
        .read_to_end(&mut payload)
        .await
        .map_err(RepositoryError::Io)?;
    Ok(Some((key, GitViewBody::decode(&payload)?)))
}

/// Safely project one exact native Git tree into an empty directory.
///
/// Native object identities remain unchanged: no filters, attributes, or
/// line-ending conversion run. File mode, executable mode, and symlink mode
/// are interpreted directly from the verified tree payload.
#[tracing::instrument(name = "git.tree.checkout", skip_all, fields(?gitlinks))]
pub async fn checkout_git_tree<PS, SS>(
    repository: &Repository<PS, SS>,
    tree: &ObjectKey,
    target: impl AsRef<std::path::Path>,
    gitlinks: GitlinkCheckoutPolicy,
) -> Result<(), GitViewError>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    use crate::git::GitObjectKind;

    let (format, kind, _) = git_key_parts(tree)?;
    if kind != GitObjectKind::Tree {
        return Err(
            GitError::InvalidObject(format!("checkout requires a Git tree, got {kind:?}")).into(),
        );
    }
    let hold = repository.retention_hold().await?;
    match hold.verify_closure(tree).await? {
        ClosureStatus::Complete { .. } => {}
        status => {
            return Err(RepositoryError::ObjectNotReadable {
                object: tree.clone(),
                status,
            }
            .into());
        }
    }
    // Staged exactly like the canonical checkout: the tree is written into a
    // fresh sibling and renamed into place only once complete, so a failure
    // leaves the target as it was.
    let stage = CheckoutStage::create(target.as_ref())
        .await
        .map_err(RepositoryError::Payload)?;
    checkout_git_tree_staged(repository, &hold, format, tree, stage.path(), gitlinks).await?;
    stage
        .publish()
        .await
        .map_err(|error| RepositoryError::Payload(error.into()))?
        .map_err(RepositoryError::Payload)?;
    Ok(())
}

/// Materialize one verified Git tree below an already created staging root.
/// [`CheckoutStage`] owns cleanup until its caller publishes it.
async fn checkout_git_tree_staged<PS, SS>(
    repository: &Repository<PS, SS>,
    hold: &crate::RetentionHold<'_, PS, SS>,
    format: GitObjectFormat,
    tree: &ObjectKey,
    staging: &std::path::Path,
    gitlinks: GitlinkCheckoutPolicy,
) -> Result<(), GitViewError>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    use crate::git::{GitObjectKind, GitTreeMode, git_object_key, parse_git_tree};

    // Handle-rooted, like the canonical checkout: every write below resolves
    // against this handle, so a swapped component cannot redirect it out of the
    // selected tree.
    let target = crate::filesystem::root::FsRoot::open_write(staging)
        .await
        .map_err(RepositoryError::Payload)?;

    let mut pending = vec![(tree.clone(), std::path::PathBuf::new())];
    let mut files = Vec::new();
    let mut symlinks = Vec::new();
    while let Some((tree_key, directory_path)) = pending.pop() {
        let (_, reader) = hold
            .open_payload(&tree_key)
            .await?
            .ok_or_else(|| RepositoryError::Absent(tree_key.to_string()))?;
        let mut body = Vec::new();
        reader
            .take(repository.limits().max_metadata_bytes.saturating_add(1))
            .read_to_end(&mut body)
            .await
            .map_err(RepositoryError::Io)?;
        if body.len() as u64 > repository.limits().max_metadata_bytes {
            return Err(RepositoryError::LimitExceeded(
                "Git tree exceeds metadata read limit".into(),
            )
            .into());
        }
        for entry in parse_git_tree(format, &body)? {
            // Refused here rather than while parsing: these trees are valid
            // objects that import unchanged, but writing one would plant Git
            // metadata (for example hooks) in a future worktree.
            if crate::git::is_git_admin_alias(&entry.name) {
                return Err(RepositoryError::InvalidInput(format!(
                    "refusing to check out Git tree entry `{}`: it names the `.git` directory",
                    directory_path
                        .join(String::from_utf8_lossy(&entry.name).as_ref())
                        .display()
                ))
                .into());
            }
            let component = crate::filesystem::names::os_str_from_bytes(&entry.name)
                .map_err(RepositoryError::Payload)?;
            #[cfg(windows)]
            crate::filesystem::names::check_windows_stored_name(&entry.name, &directory_path)
                .map_err(|error| RepositoryError::InvalidInput(error.to_string()))?;
            let path = directory_path.join(component);
            match entry.mode {
                GitTreeMode::Tree => {
                    target
                        .create_dir(&path)
                        .await
                        .map_err(|error| checkout_git_path_error(&path, error))?;
                    pending.push((
                        git_object_key(format, GitObjectKind::Tree, entry.oid)?,
                        path,
                    ));
                }
                GitTreeMode::Blob | GitTreeMode::BlobExecutable => files.push((
                    git_object_key(format, GitObjectKind::Blob, entry.oid)?,
                    entry.mode == GitTreeMode::BlobExecutable,
                    path,
                )),
                GitTreeMode::Symlink => symlinks.push((
                    git_object_key(format, GitObjectKind::Blob, entry.oid)?,
                    path,
                )),
                GitTreeMode::Gitlink => match gitlinks {
                    GitlinkCheckoutPolicy::Error => {
                        return Err(RepositoryError::InvalidInput(format!(
                            "Git tree contains gitlink at {}",
                            path.display()
                        ))
                        .into());
                    }
                    GitlinkCheckoutPolicy::Skip => {
                        target
                            .create_dir(&path)
                            .await
                            .map_err(|error| checkout_git_path_error(&path, error))?;
                    }
                },
            }
        }
    }

    for (key, executable, path) in files {
        write_git_file(hold, &target, &key, executable, &path).await?;
    }
    for (key, path) in symlinks {
        let (_, reader) = hold
            .open_payload(&key)
            .await?
            .ok_or_else(|| RepositoryError::Absent(key.to_string()))?;
        let mut bytes = Vec::new();
        reader
            .take(4096)
            .read_to_end(&mut bytes)
            .await
            .map_err(RepositoryError::Io)?;
        let link = crate::SymlinkTarget::try_from(bytes::Bytes::from(bytes))
            .map_err(|error| RepositoryError::InvalidInput(error.to_string()))?;
        create_git_symlink(&target, &link, &path).await?;
    }
    Ok(())
}

async fn write_git_file<PS, SS>(
    hold: &crate::RetentionHold<'_, PS, SS>,
    root: &crate::filesystem::root::FsRoot,
    key: &ObjectKey,
    executable: bool,
    path: &std::path::Path,
) -> Result<(), GitViewError>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    let (_, mut reader) = hold
        .open_payload(key)
        .await?
        .ok_or_else(|| RepositoryError::Absent(key.to_string()))?;
    let mut file = root
        .create_file(path, executable)
        .await
        .map_err(|error| checkout_git_path_error(path, error))?;
    tokio::io::copy(&mut reader, &mut file)
        .await
        .map_err(RepositoryError::Io)?;
    Ok(())
}

/// A fresh staging root holds no caller files, so a collision while creating
/// an entry means the target filesystem equates two distinct Git names.
fn checkout_git_path_error(path: &std::path::Path, error: crate::error::Error) -> RepositoryError {
    RepositoryError::Payload(checkout_path_error(path, error))
}

#[cfg(unix)]
async fn create_git_symlink(
    root: &crate::filesystem::root::FsRoot,
    target: &crate::SymlinkTarget,
    path: &std::path::Path,
) -> Result<(), GitViewError> {
    let target = crate::filesystem::names::os_str_from_bytes(target.as_bytes())
        .map_err(RepositoryError::Payload)?;
    root.symlink(path, std::path::Path::new(&target), false)
        .await
        .map_err(|error| checkout_git_path_error(path, error))?;
    Ok(())
}

#[cfg(windows)]
async fn create_git_symlink(
    root: &crate::filesystem::root::FsRoot,
    target: &crate::SymlinkTarget,
    path: &std::path::Path,
) -> Result<(), GitViewError> {
    let stored = std::str::from_utf8(target.as_bytes()).map_err(|_| {
        RepositoryError::InvalidInput("symlink target is not UTF-8 on this platform".into())
    })?;
    let native = std::path::PathBuf::from(stored.replace('/', "\\"));
    let resolved = path
        .parent()
        .map_or_else(|| native.clone(), |parent| parent.join(&native));
    let is_dir = root.is_directory(&resolved).await;
    root.symlink(path, &native, is_dir)
        .await
        .map_err(|error| checkout_git_path_error(path, error))?;
    Ok(())
}

/// Selection for native import from one local Git object database.
#[cfg(feature = "git")]
#[derive(Debug, Clone)]
pub struct NativeGitImportOptions {
    /// View-root segment below `git/`.
    pub view_name: String,
    /// Exact refs to import. Empty selects local branches and tags.
    pub refs: Vec<crate::CanonicalRefName>,
    /// Exact object IDs retained under `refs/casita/pins/<oid>`.
    pub revisions: Vec<String>,
    /// Largest exact source-native pack retained as a full-clone cache.
    ///
    /// Set this to zero to avoid the additional storage. The cache is used
    /// only when one verified source pack is exactly the selected view.
    pub max_cached_pack_bytes: u64,
    /// Maximum simultaneously staged native objects. One selects serial staging.
    pub concurrency: std::num::NonZeroUsize,
    /// Budget for decoded source bytes held by staging futures. An object larger
    /// than this budget runs alone. This excludes Gix caches, delta-decoding
    /// workspace, metadata, and payload-store buffers; it is not an RSS limit.
    pub max_buffered_bytes: std::num::NonZeroU64,
}

#[cfg(feature = "git")]
impl Default for NativeGitImportOptions {
    fn default() -> Self {
        Self {
            view_name: String::new(),
            refs: Vec::new(),
            revisions: Vec::new(),
            max_cached_pack_bytes: DEFAULT_MAX_CACHED_GIT_PACK_BYTES,
            concurrency: DEFAULT_GIT_IMPORT_CONCURRENCY,
            max_buffered_bytes: DEFAULT_GIT_IMPORT_BUFFERED_BYTES,
        }
    }
}

/// Result of importing and selecting a complete local Git view.
#[cfg(feature = "git")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeGitImportOutcome {
    /// Exact selected view object.
    pub view: ObjectKey,
    /// Number of distinct reachable native objects visited.
    pub objects: usize,
    /// Revision containing the view and atomic root update.
    pub revision: RepositoryRevision,
}

#[cfg(feature = "git")]
impl<PS, SS> Repository<PS, SS>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    /// Import exact native objects from a local Git repository without checkout.
    ///
    /// An empty ref selection includes `refs/heads/*` and `refs/tags/*`. Object
    /// batches are published unrooted under one mutation hold; the view and
    /// its ordinary `git/<view-name>` root are committed together only after
    /// the complete selected closure has been verified.
    #[tracing::instrument(name = "git.view.import_native", skip_all)]
    pub(crate) async fn import_native_git_view(
        &self,
        source: impl AsRef<Path>,
        options: &NativeGitImportOptions,
    ) -> Result<NativeGitImportOutcome, GitViewError> {
        import_native_git_view_impl(self, source, options).await
    }
}

#[cfg(feature = "git")]
async fn import_native_git_view_impl<PS, SS>(
    repository: &Repository<PS, SS>,
    source: impl AsRef<Path>,
    options: &NativeGitImportOptions,
) -> Result<NativeGitImportOutcome, GitViewError>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    use std::collections::{BTreeMap, BTreeSet, VecDeque};

    use bstr::ByteSlice;

    use crate::git::{GitObjectFormat, GitRefValue, git_key_parts, git_object_key};

    let source_path = source.as_ref().to_path_buf();
    let mut source = gix::open_opts(&source_path, gix::open::Options::isolated())
        .map_err(|error| GitViewError::Import(error.to_string()))?;
    source.object_cache_size_if_unset(64 * 1024 * 1024);
    let object_format = match source.object_hash() {
        gix::hash::Kind::Sha1 => GitObjectFormat::Sha1,
        gix::hash::Kind::Sha256 => GitObjectFormat::Sha256,
        _ => {
            return Err(GitViewError::Import(
                "this build cannot open the repository's object hash format".into(),
            ));
        }
    };

    let selected: Option<BTreeSet<_>> = if options.refs.is_empty() {
        None
    } else {
        Some(options.refs.iter().cloned().collect())
    };
    let mut refs = BTreeMap::new();
    let mut queue = VecDeque::new();
    {
        let platform = source
            .references()
            .map_err(|error| GitViewError::Import(error.to_string()))?;
        let iterator = platform
            .all()
            .map_err(|error| GitViewError::Import(error.to_string()))?;
        for reference in iterator {
            let reference = reference.map_err(|error| GitViewError::Import(error.to_string()))?;
            let name_text = reference
                .name()
                .as_bstr()
                .to_str()
                .map_err(|error| GitViewError::Import(error.to_string()))?;
            if selected.is_none()
                && !name_text.starts_with("refs/heads/")
                && !name_text.starts_with("refs/tags/")
            {
                continue;
            }
            let name = crate::CanonicalRefName::try_from(name_text)?;
            if selected
                .as_ref()
                .is_some_and(|names| !names.contains(&name))
            {
                continue;
            }
            let target = reference.target();
            let value = if let Some(oid) = target.try_id() {
                let object = source
                    .find_object(oid)
                    .map_err(|error| GitViewError::Import(error.to_string()))?;
                let kind = gix_kind(object.kind)?;
                let key = git_object_key(object_format, kind, oid.as_bytes().to_vec())?;
                queue.push_back(key.clone());
                GitRefValue::Direct(key)
            } else {
                let target = target.try_name().expect("a non-object target is symbolic");
                let text = target
                    .as_bstr()
                    .to_str()
                    .map_err(|error| GitViewError::Import(error.to_string()))?;
                GitRefValue::Symbolic(crate::CanonicalRefName::try_from(text)?)
            };
            refs.insert(name, value);
        }
    }
    if let Some(selected) = &selected
        && let Some(missing) = selected.iter().find(|name| !refs.contains_key(*name))
    {
        return Err(GitViewError::Import(format!(
            "selected ref `{missing}` is absent"
        )));
    }
    for revision in &options.revisions {
        let oid = gix::ObjectId::from_hex(revision.as_bytes())
            .map_err(|e| GitViewError::Import(e.to_string()))?;
        let object = source
            .find_object(oid)
            .map_err(|e| GitViewError::Import(e.to_string()))?;
        let key = git_object_key(
            object_format,
            gix_kind(object.kind)?,
            oid.as_bytes().to_vec(),
        )?;
        let name = crate::CanonicalRefName::try_from(format!("refs/casita/pins/{oid}"))?;
        refs.insert(name, GitRefValue::Direct(key.clone()));
        queue.push_back(key);
    }
    if refs.is_empty() {
        return Err(GitViewError::Import(
            "the selection contains no Git refs".into(),
        ));
    }

    // Preserve the symbolic default branch when it belongs to this view.
    let default_ref = source
        .head()
        .ok()
        .and_then(|head| head.referent_name().map(|name| name.as_bstr().to_vec()))
        .and_then(|bytes| std::str::from_utf8(&bytes).ok().map(ToOwned::to_owned))
        .and_then(|name| crate::CanonicalRefName::try_from(name).ok())
        .filter(|name| refs.contains_key(name));

    let root_name = git_view_root_name(&options.view_name)?;
    // Enter the mutation domain before reading a packed prior view. Opening
    // that payload may finish a catalog rebase and mark superseded immutable
    // metadata for deferred reclamation; checking maintenance after the read
    // would put a repository-wide sweep directly on this import's latency
    // path.
    #[cfg(test)]
    let timer = import_profile::time(0);
    let mutation = repository.mutation_session().await?;
    #[cfg(test)]
    drop(timer);
    // Git object identities make the selected closure immutable. If the
    // currently rooted view selects the same refs and default branch, walking
    // and restaging every source object cannot change the result. Revalidate
    // that the same view is still current in one fresh snapshot before taking
    // this fast path so a concurrent root replacement is never hidden.
    #[cfg(test)]
    let timer = import_profile::time(1);
    let existing_view = read_git_view(repository, &options.view_name).await?;
    #[cfg(test)]
    drop(timer);
    if let Some((existing_key, existing)) = &existing_view
        && existing.object_format == object_format
        && existing.refs == refs
        && existing.default_ref == default_ref
    {
        let snapshot = repository
            .metadata()
            .snapshot()
            .await
            .map_err(RepositoryError::Metadata)?;
        if snapshot
            .root(&root_name)
            .await
            .map_err(RepositoryError::Metadata)?
            .as_ref()
            == Some(existing_key)
        {
            return Ok(NativeGitImportOutcome {
                view: existing_key.clone(),
                objects: existing.objects().len(),
                revision: snapshot.revision(),
            });
        }
    }

    // A one-direct-ref fast-forward has an exact reusable boundary: reaching
    // the previous tip proves its complete immutable inventory is a subset of
    // the new closure. Multi-tip views fall back to the full walk because
    // unioning an old global inventory could retain objects from a rewritten
    // or removed ref.
    let incremental_base = existing_view.as_ref().and_then(|(_, existing)| {
        if existing.object_format != object_format
            || existing.default_ref != default_ref
            || existing.refs.len() != refs.len()
        {
            return None;
        }
        let mut old_target = None;
        for (name, old_value) in &existing.refs {
            match (old_value, refs.get(name)) {
                (GitRefValue::Symbolic(old), Some(GitRefValue::Symbolic(new))) if old == new => {}
                (GitRefValue::Direct(old), Some(GitRefValue::Direct(_)))
                    if old_target.is_none() =>
                {
                    old_target = Some(old.clone());
                }
                _ => return None,
            }
        }
        old_target.map(|target| (target, existing.objects().clone()))
    });
    let mut visited = BTreeSet::new();
    let mut staged = Vec::new();
    let mut staged_links = 0usize;
    let mut source_mapping_bytes = 0u64;
    let mut published_checkpoint = false;
    let publish_batch_objects = repository.limits().max_batch_objects;
    use futures::StreamExt;
    let mut in_flight = futures::stream::FuturesUnordered::new();
    let mut completed = VecDeque::new();
    let mut buffered_bytes = 0u64;
    let mut pending = None;
    // Finish discovering a fast-forward's parent chain before admitting trees
    // queued behind it. The old closure can then be reused even if its source
    // objects are no longer available locally.
    let mut awaiting_parent = false;
    loop {
        if completed.is_empty() && !awaiting_parent && in_flight.len() < options.concurrency.get() {
            if pending.is_none()
                && let Some(key) = queue.pop_front()
            {
                if !visited.insert(key.clone()) {
                    continue;
                }
                if visited.len() > repository.limits().max_traversal_objects {
                    return Err(RepositoryError::LimitExceeded(format!(
                        "native Git traversal exceeded {} objects",
                        repository.limits().max_traversal_objects
                    ))
                    .into());
                }
                if let Some((target, inventory)) = &incremental_base
                    && target == &key
                {
                    visited.extend(inventory.iter().cloned());
                    if visited.len() > repository.limits().max_traversal_objects {
                        return Err(RepositoryError::LimitExceeded(format!(
                            "native Git traversal exceeded {} objects",
                            repository.limits().max_traversal_objects
                        ))
                        .into());
                    }
                    continue;
                }
                let (format, expected_kind, oid) = git_key_parts(&key)?;
                if format != object_format {
                    return Err(GitViewError::Import(
                        "selected Git closure crosses object formats".into(),
                    ));
                }
                let oid = gix::ObjectId::from_bytes_or_panic(oid);
                // With an empty window even an oversized object can run alone;
                // avoid an extra header lookup in the serial path. Otherwise
                // inspect size before decoding, retaining only one pending key.
                let size = if in_flight.is_empty() {
                    None
                } else {
                    #[cfg(test)]
                    let _timer = import_profile::time(2);
                    Some(
                        source
                            .find_header(oid)
                            .map_err(|error| GitViewError::Import(error.to_string()))?
                            .size(),
                    )
                };
                pending = Some((key, oid, expected_kind, size));
            }
            if pending.as_ref().is_some_and(|(_, _, _, size)| {
                in_flight.is_empty()
                    || size.is_some_and(|size| {
                        size <= options
                            .max_buffered_bytes
                            .get()
                            .saturating_sub(buffered_bytes)
                            && buffered_bytes <= options.max_buffered_bytes.get()
                    })
            }) {
                let (key, oid, expected_kind, expected_size) = pending.take().unwrap();
                #[cfg(test)]
                let timer = import_profile::time(3);
                let source_object = source
                    .find_object(oid)
                    .map_err(|error| GitViewError::Import(error.to_string()))?
                    .detach();
                #[cfg(test)]
                drop(timer);
                let actual_kind = gix_kind(source_object.kind)?;
                if actual_kind != expected_kind {
                    return Err(GitViewError::Import(format!(
                        "Git OID {} was linked as {expected_kind:?} but stores {actual_kind:?}",
                        source_object.id
                    )));
                }
                let size = source_object.data.len() as u64;
                if expected_size.is_some_and(|expected| expected != size) {
                    return Err(GitViewError::Import(
                        "Git object size changed after header lookup".into(),
                    ));
                }
                buffered_bytes = buffered_bytes.checked_add(size).ok_or_else(|| {
                    RepositoryError::LimitExceeded(
                        "native Git buffered byte count overflowed".into(),
                    )
                })?;
                awaiting_parent =
                    incremental_base.is_some() && actual_kind == crate::GitObjectKind::Commit;
                let mutation = &mutation;
                in_flight.push(async move {
                    let staged = mutation.stage_object(key, &source_object.data).await;
                    // Release the decoded body as soon as staging completes.
                    // Keeping detached data out of Gix's buffer pool prevents
                    // completed large buffers accumulating there.
                    drop(source_object);
                    (size, actual_kind, staged)
                });
                #[cfg(test)]
                import_profile::admitted(size, in_flight.len(), buffered_bytes);
                source_mapping_bytes = source_mapping_bytes.checked_add(size).ok_or_else(|| {
                    RepositoryError::LimitExceeded(
                        "native Git source mapping size overflowed".into(),
                    )
                })?;
                if source_mapping_bytes >= MAX_GIT_SOURCE_MAPPING_WINDOW_BYTES {
                    // Detached bodies own their bytes, so no staging future
                    // retains a pack mapping when the source handle is replaced.
                    source = gix::open_opts(&source_path, gix::open::Options::isolated())
                        .map_err(|error| GitViewError::Import(error.to_string()))?;
                    source.object_cache_size_if_unset(64 * 1024 * 1024);
                    source_mapping_bytes = 0;
                }
                continue;
            }
        }
        let next = match completed.pop_front() {
            Some(object) => Some(object),
            None => {
                #[cfg(test)]
                let _timer = import_profile::time(4);
                in_flight.next().await
            }
        };
        let Some((size, actual_kind, object)) = next else {
            break;
        };
        buffered_bytes -= size;
        if actual_kind == crate::GitObjectKind::Commit {
            awaiting_parent = false;
        }
        let object = object?;
        if incremental_base.is_some() && actual_kind == crate::GitObjectKind::Commit {
            for link in object.record().links().iter().rev() {
                let (_, linked_kind, _) = git_key_parts(link)?;
                if linked_kind == crate::GitObjectKind::Commit {
                    queue.push_front(link.clone());
                } else {
                    queue.push_back(link.clone());
                }
            }
        } else {
            queue.extend(object.record().links().iter().cloned());
        }
        staged_links = staged_links
            .checked_add(object.record().links().len())
            .ok_or_else(|| {
                RepositoryError::LimitExceeded("native Git batch link count overflowed".into())
            })?;
        staged.push(object);
        if staged.len() == publish_batch_objects || staged_links >= MAX_GIT_IMPORT_BATCH_LINKS {
            // A payload backend may flush behind a lock held by an unfinished
            // writer. Keep polling every active write before awaiting a flush.
            // Completed seals fit in the bounded concurrency window and are
            // consumed before admitting more source objects; publications still
            // obey their independent object/link batch limits.
            #[cfg(test)]
            let timer = import_profile::time(7);
            while let Some((size, kind, object)) = in_flight.next().await {
                completed.push_back((size, kind, Ok(object?)));
            }
            #[cfg(test)]
            drop(timer);
            #[cfg(test)]
            let _timer = import_profile::time(8);
            mutation
                .publish_unrooted(std::mem::take(&mut staged))
                .await?;
            staged_links = 0;
            published_checkpoint = true;
        }
    }
    if !staged.is_empty() {
        #[cfg(test)]
        let _timer = import_profile::time(8);
        mutation.publish_unrooted(staged).await?;
    }

    #[cfg(test)]
    let timer = import_profile::time(9);
    let pack = match exact_source_pack(
        &source,
        object_format,
        &visited,
        options.max_cached_pack_bytes,
    ) {
        Some(path) => {
            let mut source_pack = tokio::fs::File::open(&path)
                .await
                .map_err(|error| GitViewError::Import(error.to_string()))?;
            let staged_pack = mutation.stage_blob_reader(&mut source_pack).await?;
            let key = staged_pack.record().key().clone();
            mutation.publish_unrooted(vec![staged_pack]).await?;
            Some(key)
        }
        None => None,
    };

    #[cfg(test)]
    drop(timer);
    #[cfg(test)]
    let timer = import_profile::time(10);
    let view = GitViewBody {
        object_format,
        refs,
        default_ref,
        pack,
        objects: visited.clone(),
    };
    let view_payload = view.encode()?;
    let view_key = view.object_key()?;
    let view_object = mutation
        .stage_object(view_key.clone(), &view_payload)
        .await?;
    let result = mutation
        .publish_git_constructed(view_object, root_name, view_key.clone())
        .await?;
    #[cfg(test)]
    drop(timer);
    // Fold a genuinely multi-checkpoint import into compact state. A small
    // fast-forward has only its final object batch plus the rooted view commit;
    // rewriting the complete state there makes incremental latency
    // proportional to repository size while saving only a bounded WAL tail.
    if published_checkpoint {
        #[cfg(test)]
        let _timer = import_profile::time(11);
        let _ = repository.metadata().compact_transient_state().await;
    }
    tracing::info!(
        revision = %result.revision,
        objects = visited.len(),
        object_format = ?object_format,
        cached_pack = view.pack.is_some(),
        "native Git view imported"
    );
    Ok(NativeGitImportOutcome {
        view: view_key,
        objects: visited.len(),
        revision: result.revision,
    })
}

#[cfg(feature = "git")]
fn exact_source_pack(
    source: &gix::Repository,
    object_format: GitObjectFormat,
    objects: &BTreeSet<ObjectKey>,
    max_pack_bytes: u64,
) -> Option<PathBuf> {
    let object_hash = match object_format {
        GitObjectFormat::Sha1 => gix::hash::Kind::Sha1,
        GitObjectFormat::Sha256 => gix::hash::Kind::Sha256,
    };
    let object_ids: BTreeSet<Vec<u8>> =
        objects.iter().map(|key| key.native_id().to_vec()).collect();
    if object_ids.len() != objects.len() {
        return None;
    }

    let pack_dir = source.objects.store_ref().path().join("pack");
    let mut indexes = std::fs::read_dir(pack_dir)
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "idx"))
        .collect::<Vec<_>>();
    indexes.sort();
    for index_path in indexes {
        let pack_path = index_path.with_extension("pack");
        let pack_size = match std::fs::metadata(&pack_path) {
            Ok(metadata) if metadata.is_file() => metadata.len(),
            _ => continue,
        };
        if pack_size > max_pack_bytes {
            continue;
        }
        let index = match gix::odb::pack::index::File::at(&index_path, object_hash) {
            Ok(index) => index,
            Err(_) => continue,
        };
        if index.num_objects() as usize != objects.len()
            || index
                .iter()
                .any(|entry| !object_ids.contains(entry.oid.as_bytes()))
        {
            continue;
        }
        let pack = match gix::odb::pack::data::File::at(&pack_path, object_hash) {
            Ok(pack) => pack,
            Err(_) => continue,
        };
        let interrupted = AtomicBool::new(false);
        if pack.num_objects() != index.num_objects()
            || pack.data_len() as u64 != pack_size
            || pack.checksum() != index.pack_checksum()
            || index
                .verify_checksum(&mut gix::progress::Discard, &interrupted)
                .is_err()
            || pack
                .verify_checksum(&mut gix::progress::Discard, &interrupted)
                .is_err()
        {
            continue;
        }
        return Some(pack_path);
    }
    None
}

#[cfg(feature = "git")]
fn gix_kind(kind: gix::object::Kind) -> Result<crate::GitObjectKind, GitViewError> {
    match kind {
        gix::object::Kind::Blob => Ok(crate::GitObjectKind::Blob),
        gix::object::Kind::Tree => Ok(crate::GitObjectKind::Tree),
        gix::object::Kind::Commit => Ok(crate::GitObjectKind::Commit),
        gix::object::Kind::Tag => Ok(crate::GitObjectKind::Tag),
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use futures::stream::{self, BoxStream};
    use std::collections::{BTreeMap, BTreeSet};
    #[cfg(all(feature = "git", unix))]
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::{
        BlobId, Digest, GitObjectFormat, GitObjectKind, GitRefValue, MemoryBlobStore,
        MemoryMetadataStore, MetadataError, MetadataSnapshot, MetadataStore, ObjectRecord,
        RootName, RootRecord, git_object_key, git_object_key_for_body, repository::Repository,
    };

    use super::*;

    // Only the Unix native import test below constructs this store.
    #[cfg(all(feature = "git", unix))]
    #[derive(Clone)]
    struct CountingCompactState {
        inner: MemoryMetadataStore,
        compactions: Arc<AtomicUsize>,
    }

    #[cfg(all(feature = "git", unix))]
    #[async_trait]
    impl MetadataStore for CountingCompactState {
        async fn try_collection_lease(
            &self,
        ) -> Result<Option<crate::metadata::RepositoryLease>, crate::metadata::MetadataError>
        {
            self.inner.try_collection_lease().await
        }
        fn coordinates_payload_catalog(&self) -> bool {
            self.inner.coordinates_payload_catalog()
        }
        async fn pin_store(
            &self,
        ) -> Result<std::sync::Arc<dyn crate::metadata::PinStore>, crate::metadata::MetadataError>
        {
            self.inner.pin_store().await
        }

        async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
            self.inner.snapshot().await
        }

        async fn commit(
            &self,
            expected: &RepositoryRevision,
            mutation: crate::MetadataMutation,
        ) -> Result<crate::CommitResult, MetadataError> {
            self.inner.commit(expected, mutation).await
        }

        async fn compact_transient_state(&self) -> Result<(), MetadataError> {
            self.compactions.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    struct CatalogSnapshot {
        revision: RepositoryRevision,
        records: BTreeMap<ObjectKey, ObjectRecord>,
    }

    #[async_trait]
    impl MetadataSnapshot for CatalogSnapshot {
        fn revision(&self) -> RepositoryRevision {
            self.revision
        }

        async fn object(&self, key: &ObjectKey) -> Result<Option<ObjectRecord>, MetadataError> {
            Ok(self.records.get(key).cloned())
        }

        async fn root(&self, _name: &RootName) -> Result<Option<ObjectKey>, MetadataError> {
            Ok(None)
        }

        fn objects(&self) -> BoxStream<'static, Result<ObjectRecord, MetadataError>> {
            Box::pin(stream::iter(
                self.records.values().cloned().map(Ok).collect::<Vec<_>>(),
            ))
        }

        fn roots(&self) -> BoxStream<'static, Result<RootRecord, MetadataError>> {
            Box::pin(stream::empty())
        }
    }

    #[tokio::test]
    async fn direct_and_indexed_oid_lookup_preserve_missing_and_ambiguity() {
        let oid = vec![0x42; GitObjectFormat::Sha1.oid_len()];
        let blob = git_object_key(GitObjectFormat::Sha1, GitObjectKind::Blob, oid.clone()).unwrap();
        let tree = git_object_key(GitObjectFormat::Sha1, GitObjectKind::Tree, oid.clone()).unwrap();
        let record = |key: ObjectKey, marker| {
            ObjectRecord::new(key, BlobId::new(Digest::from([marker; 32])), 0, Vec::new()).unwrap()
        };
        let snapshot = CatalogSnapshot {
            revision: RepositoryRevision::from_bytes([0x11; 32]),
            records: BTreeMap::from([
                (blob.clone(), record(blob.clone(), 1)),
                (tree.clone(), record(tree.clone(), 2)),
            ]),
        };

        let direct = crate::resolve_git_oid(&snapshot, GitObjectFormat::Sha1, &oid)
            .await
            .unwrap_err();
        assert!(matches!(direct, GitError::AmbiguousOid { matches: 2, .. }));

        let index = GitOidIndex::new();
        *index.generation.write().await = Some(GitOidIndexGeneration {
            checkpoint: GitOidIndexCheckpoint {
                revision: snapshot.revision,
                objects: 2,
            },
            entries: BTreeMap::from([((GitObjectFormat::Sha1, oid.clone()), vec![blob, tree])]),
        });
        let indexed = index
            .resolve(GitObjectFormat::Sha1, &oid)
            .await
            .unwrap_err();
        assert_eq!(indexed, direct);

        let missing_oid = vec![0x24; GitObjectFormat::Sha1.oid_len()];
        let direct = crate::resolve_git_oid(&snapshot, GitObjectFormat::Sha1, &missing_oid)
            .await
            .unwrap_err();
        let indexed = index
            .resolve(GitObjectFormat::Sha1, &missing_oid)
            .await
            .unwrap_err();
        assert_eq!(indexed, direct);
        assert!(matches!(direct, GitError::MissingOid(_)));
    }

    #[tokio::test]
    async fn view_replacement_is_atomic_and_old_view_collects() {
        let repository =
            Repository::new(MemoryBlobStore::new(), MemoryMetadataStore::new().unwrap());
        let name = crate::CanonicalRefName::try_from("refs/heads/main").unwrap();
        let first_target =
            git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Blob, b"one").unwrap();
        let second_target =
            git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Blob, b"two").unwrap();

        // Native targets must exist before a complete view can be rooted.
        let mutation = repository.mutation_session().await.unwrap();
        let one = mutation
            .stage_object(first_target.clone(), b"one")
            .await
            .unwrap();
        let two = mutation
            .stage_object(second_target.clone(), b"two")
            .await
            .unwrap();
        let cached_pack = mutation.stage_blob(b"derived pack").await.unwrap();
        let cached_pack_key = cached_pack.record().key().clone();
        mutation
            .publish_unrooted(vec![one, two, cached_pack])
            .await
            .unwrap();
        drop(mutation);

        let oid_index = GitOidIndex::new();
        oid_index.rebuild(&repository).await.unwrap();
        let indexed = oid_index
            .resolve(GitObjectFormat::Sha1, first_target.native_id())
            .await
            .unwrap();
        let snapshot = repository.metadata().snapshot().await.unwrap();
        let direct = crate::resolve_git_oid(
            snapshot.as_ref(),
            GitObjectFormat::Sha1,
            first_target.native_id(),
        )
        .await
        .unwrap();
        assert_eq!(indexed, direct);

        let first = GitViewBody {
            object_format: GitObjectFormat::Sha1,
            refs: BTreeMap::from([(name.clone(), GitRefValue::Direct(first_target.clone()))]),
            default_ref: Some(name.clone()),
            pack: None,
            objects: BTreeSet::from([first_target.clone()]),
        };
        let mut overbroad = first.clone();
        overbroad.objects.insert(second_target.clone());
        assert!(matches!(
            publish_git_view(&repository, "overbroad", &overbroad).await,
            Err(GitViewError::Git(GitError::InvalidView(message)))
                if message.contains("unreachable object")
        ));
        let mut caller_cached = first.clone();
        caller_cached.pack = Some(cached_pack_key);
        assert!(matches!(
            publish_git_view(&repository, "caller-cached", &caller_cached).await,
            Err(GitViewError::Git(GitError::InvalidView(message)))
                if message.contains("caller-authored")
        ));
        let first_publication = publish_git_view(&repository, "demo", &first).await.unwrap();
        let second = GitViewBody {
            object_format: GitObjectFormat::Sha1,
            refs: BTreeMap::from([(name.clone(), GitRefValue::Direct(second_target.clone()))]),
            default_ref: Some(name),
            pack: None,
            objects: BTreeSet::from([second_target.clone()]),
        };
        let second_publication = publish_git_view(&repository, "demo", &second)
            .await
            .unwrap();
        assert_ne!(first_publication.view, second_publication.view);
        assert_eq!(
            read_git_view(&repository, "demo").await.unwrap().unwrap().0,
            second_publication.view
        );

        repository.collect().await.unwrap();
        let snapshot = repository.metadata().snapshot().await.unwrap();
        assert!(
            snapshot
                .object(&first_publication.view)
                .await
                .unwrap()
                .is_none()
        );
        assert!(snapshot.object(&first_target).await.unwrap().is_none());
        assert!(
            snapshot
                .object(&second_publication.view)
                .await
                .unwrap()
                .is_some()
        );
        assert!(snapshot.object(&second_target).await.unwrap().is_some());
    }

    type MemoryRepository = Repository<MemoryBlobStore, MemoryMetadataStore>;

    /// Publish native Git objects unrooted, children before parents, and return
    /// their keys in input order.
    async fn stage_git_objects(
        repository: &MemoryRepository,
        objects: &[(GitObjectKind, Vec<u8>)],
    ) -> Vec<ObjectKey> {
        let mutation = repository.mutation_session().await.unwrap();
        let mut keys = Vec::new();
        let mut staged = Vec::new();
        for (kind, body) in objects {
            let key = git_object_key_for_body(GitObjectFormat::Sha1, *kind, body).unwrap();
            staged.push(mutation.stage_object(key.clone(), body).await.unwrap());
            keys.push(key);
        }
        mutation.publish_unrooted(staged).await.unwrap();
        keys
    }

    /// Encode a native tree body from entries already in canonical Git order.
    fn git_tree_body(entries: &[(&str, &[u8], &ObjectKey)]) -> Vec<u8> {
        let mut body = Vec::new();
        for (mode, name, key) in entries {
            body.extend_from_slice(mode.as_bytes());
            body.push(b' ');
            body.extend_from_slice(name);
            body.push(0);
            body.extend_from_slice(key.native_id());
        }
        body
    }

    fn directory_names(path: &std::path::Path) -> Vec<std::ffi::OsString> {
        let mut names = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[tokio::test]
    async fn failed_git_checkout_leaves_no_partial_target() {
        let repository =
            Repository::new(MemoryBlobStore::new(), MemoryMetadataStore::new().unwrap());
        let blob =
            git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Blob, b"data").unwrap();
        let module =
            git_object_key(GitObjectFormat::Sha1, GitObjectKind::Commit, vec![7; 20]).unwrap();
        // The gitlink sits below a directory that is created before the walk
        // reaches it, so the refusal happens after checkout has begun writing.
        let nested = git_tree_body(&[("100644", b"file", &blob), ("160000", b"vendor", &module)]);
        let nested_key =
            git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Tree, &nested).unwrap();
        let root = git_tree_body(&[("100644", b"a.txt", &blob), ("40000", b"sub", &nested_key)]);
        let keys = stage_git_objects(
            &repository,
            &[
                (GitObjectKind::Blob, b"data".to_vec()),
                (GitObjectKind::Tree, nested),
                (GitObjectKind::Tree, root),
            ],
        )
        .await;
        let tree = &keys[2];

        let parent = tempfile::tempdir().unwrap();
        let absent = parent.path().join("absent");
        let error = checkout_git_tree(&repository, tree, &absent, GitlinkCheckoutPolicy::Error)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("gitlink"), "{error}");
        assert!(!absent.exists());

        let empty = parent.path().join("empty");
        std::fs::create_dir(&empty).unwrap();
        checkout_git_tree(&repository, tree, &empty, GitlinkCheckoutPolicy::Error)
            .await
            .unwrap_err();
        assert!(directory_names(&empty).is_empty());
        // No staging sibling survives either failure.
        assert_eq!(directory_names(parent.path()), vec!["empty"]);

        let published = parent.path().join("published");
        checkout_git_tree(&repository, tree, &published, GitlinkCheckoutPolicy::Skip)
            .await
            .unwrap();
        assert_eq!(std::fs::read(published.join("sub/file")).unwrap(), b"data");
        assert!(published.join("sub/vendor").is_dir());
        assert_eq!(directory_names(parent.path()), vec!["empty", "published"]);
    }

    #[tokio::test]
    async fn git_checkout_refuses_dot_git_entries_without_partial_target() {
        let repository =
            Repository::new(MemoryBlobStore::new(), MemoryMetadataStore::new().unwrap());
        let tree_key = |body: &[u8]| {
            git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Tree, body).unwrap()
        };
        let hook = b"#!/bin/sh\nexit 0\n".to_vec();
        let hook_key =
            git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Blob, &hook).unwrap();
        let hooks = git_tree_body(&[("100755", b"pre-commit", &hook_key)]);
        let dot_git = git_tree_body(&[("40000", b"hooks", &tree_key(&hooks))]);
        let dot_git_key = tree_key(&dot_git);
        let nested_dot_git = git_tree_body(&[("40000", b".Git", &dot_git_key)]);
        let nested_short_name = git_tree_body(&[("100644", b"git~1", &hook_key)]);
        let roots = [
            git_tree_body(&[
                ("40000", b".git", &dot_git_key),
                ("100644", b"a", &hook_key),
            ]),
            git_tree_body(&[("100644", b".GIT", &hook_key)]),
            git_tree_body(&[
                ("100644", b"a", &hook_key),
                ("40000", b"sub", &tree_key(&nested_dot_git)),
            ]),
            git_tree_body(&[("40000", b"sub", &tree_key(&nested_short_name))]),
        ];
        let allowed = git_tree_body(&[
            ("100644", b".gitignore", &hook_key),
            ("100644", b".gitmodules", &hook_key),
        ]);
        let mut objects = vec![
            (GitObjectKind::Blob, hook),
            (GitObjectKind::Tree, hooks),
            (GitObjectKind::Tree, dot_git),
            (GitObjectKind::Tree, nested_dot_git),
            (GitObjectKind::Tree, nested_short_name),
            (GitObjectKind::Tree, allowed.clone()),
        ];
        objects.extend(roots.iter().map(|root| (GitObjectKind::Tree, root.clone())));
        stage_git_objects(&repository, &objects).await;

        let parent = tempfile::tempdir().unwrap();
        for (index, root) in roots.iter().enumerate() {
            let target = parent.path().join(format!("rejected-{index}"));
            let error = checkout_git_tree(
                &repository,
                &tree_key(root),
                &target,
                GitlinkCheckoutPolicy::Skip,
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("`.git`"), "{error}");
            assert!(!target.exists(), "tree {index} left a partial target");
        }
        assert!(directory_names(parent.path()).is_empty());

        let target = parent.path().join("allowed");
        checkout_git_tree(
            &repository,
            &tree_key(&allowed),
            &target,
            GitlinkCheckoutPolicy::Skip,
        )
        .await
        .unwrap();
        assert_eq!(directory_names(&target), vec![".gitignore", ".gitmodules"]);
    }

    #[cfg(all(feature = "git", unix))]
    #[tokio::test]
    async fn fast_forward_import_reuses_the_previous_exact_inventory() {
        use std::process::Command;

        let source_dir = tempfile::tempdir().unwrap();
        let git = |args: &[&str]| {
            let output = Command::new("git")
                .current_dir(source_dir.path())
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_SYSTEM", "/dev/null")
                .env("GIT_AUTHOR_NAME", "test")
                .env("GIT_AUTHOR_EMAIL", "test@example.com")
                .env("GIT_COMMITTER_NAME", "test")
                .env("GIT_COMMITTER_EMAIL", "test@example.com")
                .env("GIT_AUTHOR_DATE", "2026-01-01T00:00:00Z")
                .env("GIT_COMMITTER_DATE", "2026-01-01T00:00:00Z")
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        };
        git(&["init", "-q", "-b", "main"]);
        std::fs::write(source_dir.path().join("old"), b"old").unwrap();
        git(&["add", "old"]);
        git(&["commit", "-q", "-m", "old"]);

        let repository = Repository::with_formats(
            MemoryBlobStore::new(),
            MemoryMetadataStore::new().unwrap(),
            crate::FormatRegistry::builtin(),
            crate::FormatLimits::default(),
        );
        let options = NativeGitImportOptions {
            view_name: "origin".into(),
            refs: Vec::new(),
            ..NativeGitImportOptions::default()
        };
        repository
            .import_native_git_view(source_dir.path(), &options)
            .await
            .unwrap();
        let (_, old_view) = read_git_view(&repository, "origin").await.unwrap().unwrap();

        std::fs::write(source_dir.path().join("new"), b"new").unwrap();
        git(&["add", "new"]);
        git(&["commit", "-q", "-m", "new"]);
        for key in old_view.objects() {
            let (_, _, oid) = git_key_parts(key).unwrap();
            let hex = data_encoding::HEXLOWER.encode(oid);
            std::fs::remove_file(
                source_dir
                    .path()
                    .join(".git/objects")
                    .join(&hex[..2])
                    .join(&hex[2..]),
            )
            .unwrap();
        }

        let imported = repository
            .import_native_git_view(source_dir.path(), &options)
            .await
            .unwrap();
        let (_, new_view) = read_git_view(&repository, "origin").await.unwrap().unwrap();
        assert!(new_view.objects().is_superset(old_view.objects()));
        assert!(matches!(
            repository.verify_closure(&imported.view).await.unwrap(),
            ClosureStatus::Complete { .. }
        ));
    }

    #[cfg(all(feature = "git", unix))]
    #[tokio::test]
    async fn native_import_and_generic_transfer_preserve_complete_view() {
        use std::process::Command;

        use crate::CanonicalRefName;
        use crate::sync::{DestinationRoot, ObjectRequest, TransferRequest, transfer};

        let source_dir = tempfile::tempdir().unwrap();
        let git = |args: &[&str]| {
            let output = Command::new("git")
                .current_dir(source_dir.path())
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_SYSTEM", "/dev/null")
                .env("GIT_AUTHOR_NAME", "test")
                .env("GIT_AUTHOR_EMAIL", "test@example.com")
                .env("GIT_COMMITTER_NAME", "test")
                .env("GIT_COMMITTER_EMAIL", "test@example.com")
                .env("GIT_AUTHOR_DATE", "2026-01-01T00:00:00Z")
                .env("GIT_COMMITTER_DATE", "2026-01-01T00:00:00Z")
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            output
        };
        git(&["init", "-q", "-b", "main"]);
        std::fs::write(source_dir.path().join("hello.txt"), b"native bytes").unwrap();
        std::fs::write(source_dir.path().join("run.sh"), b"#!/bin/sh\nexit 0\n").unwrap();
        let mut permissions = std::fs::metadata(source_dir.path().join("run.sh"))
            .unwrap()
            .permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o755);
        std::fs::set_permissions(source_dir.path().join("run.sh"), permissions).unwrap();
        std::os::unix::fs::symlink("hello.txt", source_dir.path().join("hello-link")).unwrap();
        git(&["add", "hello.txt", "run.sh", "hello-link"]);
        git(&["commit", "-q", "-m", "first"]);
        git(&["tag", "-a", "v1", "-m", "release"]);
        let commit = git(&["rev-parse", "HEAD"]);
        let commit = String::from_utf8(commit.stdout).unwrap();
        git(&[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("160000,{},vendor", commit.trim()),
        ]);
        git(&["commit", "-q", "-m", "gitlink"]);

        let compactions = Arc::new(AtomicUsize::new(0));
        let state = CountingCompactState {
            inner: MemoryMetadataStore::new().unwrap(),
            compactions: compactions.clone(),
        };
        let limits = crate::FormatLimits {
            max_batch_objects: 2,
            ..crate::FormatLimits::default()
        };
        let source = Repository::with_formats(
            MemoryBlobStore::new(),
            state,
            crate::FormatRegistry::builtin(),
            limits,
        );
        let imported = source
            .import_native_git_view(
                source_dir.path(),
                &NativeGitImportOptions {
                    view_name: "origin".into(),
                    refs: Vec::new(),
                    ..NativeGitImportOptions::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            compactions.load(Ordering::Relaxed),
            1,
            "native Git import must compact after rooting the view"
        );
        let unchanged = source
            .import_native_git_view(
                source_dir.path(),
                &NativeGitImportOptions {
                    view_name: "origin".into(),
                    refs: Vec::new(),
                    ..NativeGitImportOptions::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(unchanged, imported);
        assert_eq!(
            compactions.load(Ordering::Relaxed),
            1,
            "an unchanged native Git import must not rewrite or compact state"
        );
        let (_, view) = read_git_view(&source, "origin").await.unwrap().unwrap();
        assert!(
            view.refs
                .contains_key(&CanonicalRefName::try_from("refs/heads/main").unwrap())
        );
        assert!(
            view.refs
                .contains_key(&CanonicalRefName::try_from("refs/tags/v1").unwrap())
        );
        assert_eq!(
            view.default_ref.as_ref().map(CanonicalRefName::as_str),
            Some("refs/heads/main")
        );
        assert!(matches!(
            source.verify_closure(&imported.view).await.unwrap(),
            ClosureStatus::Complete { .. }
        ));
        let main = CanonicalRefName::try_from("refs/heads/main").unwrap();
        let commit = view.resolve_ref(&main).unwrap();
        let hold = source.retention_hold().await.unwrap();
        let commit_record = hold.object(commit).await.unwrap().unwrap();
        let tree = commit_record
            .links()
            .iter()
            .find(|key| key.namespace().as_str() == crate::GIT_SHA1_TREE_NAMESPACE)
            .unwrap()
            .clone();
        drop(hold);
        let checkout_parent = tempfile::tempdir().unwrap();
        let rejected = checkout_parent.path().join("rejected-gitlink");
        let error = checkout_git_tree(&source, &tree, &rejected, GitlinkCheckoutPolicy::Error)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("gitlink"));

        let conflict = checkout_parent.path().join("conflict");
        std::fs::create_dir(&conflict).unwrap();
        std::fs::write(conflict.join("occupied"), b"keep").unwrap();
        let error = checkout_git_tree(&source, &tree, &conflict, GitlinkCheckoutPolicy::Skip)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            GitViewError::Repository(RepositoryError::Payload(
                crate::error::Error::TargetNotEmpty { .. }
            ))
        ));
        assert_eq!(std::fs::read(conflict.join("occupied")).unwrap(), b"keep");

        // A target that is itself a link is refused rather than written
        // through, so a Git checkout cannot be aimed outside its own tree.
        let outside = checkout_parent.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        let linked = checkout_parent.path().join("linked");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, &linked).unwrap();
        #[cfg(windows)]
        {
            let status = std::process::Command::new("cmd")
                .args(["/C", "mklink", "/J"])
                .arg(&linked)
                .arg(&outside)
                .status()
                .unwrap();
            assert!(status.success(), "creating a junction failed");
        }
        assert!(
            checkout_git_tree(&source, &tree, &linked, GitlinkCheckoutPolicy::Skip)
                .await
                .is_err()
        );
        assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 0);

        let checkout = checkout_parent.path().join("tree");
        checkout_git_tree(&source, &tree, &checkout, GitlinkCheckoutPolicy::Skip)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(checkout.join("hello.txt")).unwrap(),
            b"native bytes"
        );
        assert_eq!(
            std::fs::read_link(checkout.join("hello-link")).unwrap(),
            std::path::PathBuf::from("hello.txt")
        );
        let permissions = std::fs::metadata(checkout.join("run.sh"))
            .unwrap()
            .permissions();
        assert_ne!(
            std::os::unix::fs::PermissionsExt::mode(&permissions) & 0o111,
            0
        );
        assert!(checkout.join("vendor").is_dir());
        assert!(
            std::fs::read_dir(checkout.join("vendor"))
                .unwrap()
                .next()
                .is_none()
        );

        let destination =
            Repository::new(MemoryBlobStore::new(), MemoryMetadataStore::new().unwrap());
        transfer(
            &source,
            &destination,
            TransferRequest {
                objects: vec![ObjectRequest {
                    key: imported.view.clone(),
                    recursive: true,
                }],
                roots: vec![DestinationRoot {
                    name: git_view_root_name("copy").unwrap(),
                    target: imported.view.clone(),
                }],
            },
            crate::sync::TransferOptions::default(),
        )
        .await
        .unwrap();
        let copied = read_git_view(&destination, "copy").await.unwrap().unwrap();
        assert_eq!(copied, (imported.view, view));
    }
}

#[cfg(all(test, feature = "git"))]
mod ingest_tests;

#[cfg(all(test, feature = "git"))]
pub(crate) mod import_profile;
