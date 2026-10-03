//! Filesystem ingestion within an existing mutation lifetime.

use super::*;
use std::collections::HashSet;

const DIRECTORY_STAGE_CONCURRENCY: usize = 16;

#[cfg(test)]
mod tests;

#[derive(Default)]
pub(crate) struct FilesystemImportStats {
    pub pages: usize,
    pub publications: usize,
    pub stage_nanos: u64,
    pub traversal_nanos: u64,
    pub publish_nanos: u64,
    pub maintenance_nanos: u64,
}

impl<PS, SS> MutationSession<'_, PS, SS>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    #[tracing::instrument(
        name = "repository.import_path.walk",
        skip_all,
        fields(recognize = recognize, excluded = excluded.is_some())
    )]
    pub(crate) async fn import_path_inner(
        &self,
        path: impl AsRef<Path>,
        name: Option<RootName>,
        recognize: bool,
        excluded: Option<&Path>,
        file_concurrency: std::num::NonZeroUsize,
    ) -> Result<ObjectKey, RepositoryError> {
        self.import_path_inner_with_retention(
            path,
            name,
            recognize,
            excluded,
            file_concurrency,
            None,
        )
        .await
    }

    pub(crate) async fn import_path_inner_with_retention(
        &self,
        path: impl AsRef<Path>,
        name: Option<RootName>,
        recognize: bool,
        excluded: Option<&Path>,
        file_concurrency: std::num::NonZeroUsize,
        retention: Option<crate::RootRetention>,
    ) -> Result<ObjectKey, RepositoryError> {
        let (mut keys, _) = self
            .import_paths_inner_with_retention(
                vec![(
                    path.as_ref().to_path_buf(),
                    name,
                    excluded.map(Path::to_path_buf),
                )],
                recognize,
                file_concurrency,
                false,
                retention,
            )
            .await?;
        Ok(keys.remove(0))
    }

    pub(crate) async fn import_paths_inner(
        &self,
        paths: Vec<(PathBuf, Option<RootName>, Option<PathBuf>)>,
        recognize: bool,
        file_concurrency: std::num::NonZeroUsize,
        forest: bool,
    ) -> Result<(Vec<ObjectKey>, FilesystemImportStats), RepositoryError> {
        self.import_paths_inner_with_retention(paths, recognize, file_concurrency, forest, None)
            .await
    }

    pub(crate) async fn import_paths_inner_with_retention(
        &self,
        paths: Vec<(PathBuf, Option<RootName>, Option<PathBuf>)>,
        recognize: bool,
        file_concurrency: std::num::NonZeroUsize,
        forest: bool,
        retention: Option<crate::RootRetention>,
    ) -> Result<(Vec<ObjectKey>, FilesystemImportStats), RepositoryError> {
        self.write_scope().run(async {

        if retention.is_some() {
            if !self.repository.state.supports_root_retention() {
                return Err(MetadataError::UnsupportedMetadata.into());
            }
            if paths.len() != 1 || paths[0].1.is_none() {
                return Err(RepositoryError::InvalidInput(
                    "retention requires one named filesystem root".into(),
                ));
            }
        }

        if self.repository.limits.max_batch_objects == 0 {
            return Err(RepositoryError::LimitExceeded(
                "filesystem import requires a nonzero mutation batch limit".to_owned(),
            ));
        }
        let batch_size = self.repository.limits.max_batch_objects;

        let mut names = HashSet::new();
        for (_, name, _) in &paths {
            if let Some(name) = name
                && !names.insert(name.clone()) {
                return Err(RepositoryError::InvalidInput("duplicate filesystem root name".into()));
            }
        }
        if names.len() > self.repository.limits.max_root_changes {
            return Err(RepositoryError::LimitExceeded("too many filesystem roots".into()));
        }
        let mut stats = FilesystemImportStats::default();
        if paths.is_empty() { return Ok((Vec::new(), stats)); }
        let mut roots = Vec::with_capacity(paths.len());
        let phase = std::time::Instant::now();
        for (path, _, excluded) in &paths {
            roots.push((crate::filesystem::root::FsRoot::open_read(path).await?, excluded.clone()));
        }
        let file_objects = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let recognize_page = |identities: Vec<Option<crate::filesystem::root::FileIdentity>>| async move {
                if !recognize {
                    return Ok(vec![None; identities.len()]);
                }
                self.recognize_files(identities)
                    .await
                    .map_err(|error| crate::error::Error::Backend(Box::new(error)))
            };
        let ingest_file = |index: usize, path: PathBuf, identity: Option<crate::filesystem::root::FileIdentity>| {
                let file_objects = file_objects.clone();
                let root = &roots[index].0;
                async move {
                    let (size, executable, object) = stage_filesystem_file(self, root, &path)
                        .await
                        .map_err(|error| crate::error::Error::Backend(Box::new(error)))?;
                    let digest = BlobId::new(
                        object
                            .record()
                            .key()
                            .native_digest()
                            .expect("the blob verifier requires a digest native id"),
                    );
                    file_objects
                        .lock()
                        .await
                        .push((object, identity.map(|identity| (identity, digest))));
                    Ok((size, executable, digest))
                }
            };
        let traversal_nanos = std::sync::atomic::AtomicU64::new(0);
        let mut pages = crate::filesystem::walk_import_pages(&roots, recognize_page, ingest_file,
            self.repository.limits.max_traversal_objects, file_concurrency, forest, &traversal_nanos);
        stats.stage_nanos += phase.elapsed().as_nanos() as u64;

        let mut directories: HashMap<(usize, PathBuf), Directory> = HashMap::new();
        // Files and directories share one post-order publication queue. A
        // parent can therefore only be committed in the same or a later batch
        // than every newly staged child it names.
        let mut pending = Vec::new();
        let mut root_keys = vec![None; roots.len()];
        loop {
            let phase = std::time::Instant::now();
            let entries = pages.try_next().await?;
            let Some(entries) = entries else {
                stats.stage_nanos += phase.elapsed().as_nanos() as u64;
                break;
            };
            stats.pages += 1;
            pending.extend(std::mem::take(&mut *file_objects.lock().await));
            let mut completed_directories = Vec::new();
            for (root_index, entry) in entries {
                let (path, node) = match entry {
                    FilesystemEntry::Regular {
                        path,
                        size,
                        executable,
                        digest,
                    } => (
                        path,
                        Node::File {
                            digest,
                            size,
                            executable,
                        },
                    ),
                    FilesystemEntry::Symlink { path, target } => (
                        path,
                        Node::Symlink {
                            target: SymlinkTarget::try_from(Bytes::from(target))
                                .map_err(|error| RepositoryError::Payload(error.into()))?,
                        },
                    ),
                    FilesystemEntry::Directory { path } => {
                        let directory = directories.remove(&(root_index, path.clone())).unwrap_or_default();
                        let size = directory.size();
                        let digest = directory.digest();
                        completed_directories.push(directory);
                        (path, Node::Directory { digest, size })
                    }
                };

                let is_root = path.as_os_str().is_empty();
                if is_root {
                    root_keys[root_index] = match node {
                        Node::Directory { digest, .. } => Some(ObjectKey::directory(digest)),
                        Node::File { .. } => {
                            return Err(RepositoryError::InvalidInput(
                                "filesystem-graph import requires a directory root; ingest a raw file as casita.blob.v1"
                                    .to_owned(),
                            ));
                        }
                        Node::Symlink { .. } => {
                            return Err(RepositoryError::InvalidInput(
                                "a top-level symlink has no standalone v0.1 object identity"
                                    .to_owned(),
                            ));
                        }
                    };
                    continue;
                }

                let parent = path.parent().unwrap_or_else(|| Path::new("")).to_path_buf();
                let name = path.file_name().ok_or_else(|| {
                    RepositoryError::InvalidInput("import entry has no file name".to_owned())
                })?;
                let name = PathComponent::try_from(Bytes::copy_from_slice(
                    crate::filesystem::names::os_str_bytes(name)?,
                ))
                .map_err(|error| RepositoryError::Payload(error.into()))?;
                directories
                    .entry((root_index, parent))
                    .or_default()
                    .add(name, node)
                    .map_err(|error| RepositoryError::Payload(error.into()))?;
            }

            // Identities are available before payload writes: construct parents
            // from their children above, then stage this bounded walk page.
            // Ordered buffering preserves child-before-parent publication even
            // when writes finish out of order. Each stage still awaits durable
            // protection through the mutation's write scope before writing.
            let mut staged = futures::stream::iter(completed_directories)
                .map(|directory| async move { self.stage_directory(&directory).await })
                .buffered(DIRECTORY_STAGE_CONCURRENCY);
            while let Some(object) = staged.try_next().await? {
                pending.push((object, None));
            }

            stats.stage_nanos += phase.elapsed().as_nanos() as u64;
            while pending.len() >= batch_size {
                let phase = std::time::Instant::now();
                let remainder = pending.split_off(batch_size);
                self.publish_filesystem_checkpoint(pending, Vec::new(), recognize)
                    .await?;
                pending = remainder;
                stats.publications += 1;
                stats.publish_nanos += phase.elapsed().as_nanos() as u64;
            }
        }

        if let Some(orphan) = directories.keys().next() {
            return Err(RepositoryError::InvalidInput(format!(
                "import entry under {} has no directory entry",
                orphan.1.display()
            )));
        }
        let root_keys: Vec<_> = root_keys.into_iter().map(|key| key.ok_or_else(||
            RepositoryError::InvalidInput("filesystem walk produced no root entry".into())
        )).collect::<Result<_, _>>()?;
        let changes: Vec<RootChange> = paths.into_iter().zip(&root_keys).filter_map(|((_, name, _), key)|
            name.map(|name| RootChange::Set { name, target: key.clone() })).collect();
        let phase = std::time::Instant::now();
        let policy = retention.map(|retention| {
            let RootChange::Set { name, .. } = &changes[0] else {
                unreachable!("a retained filesystem import sets one root")
            };
            crate::repository::root_policy::policy_change(name, retention)
        });
        self.publish_filesystem_checkpoint_with_policy(pending, changes, recognize, policy)
            .await?;
        stats.publications += 1;
        stats.publish_nanos += phase.elapsed().as_nanos() as u64;
        // A filesystem tree is deliberately published in bounded commits so
        // an interrupted import leaves reusable progress. Once the final root
        // is durable, let the state backend fold the resulting transient log
        // back into its compact representation. This is best-effort physical
        // maintenance: the named tree is already committed and remains the
        // successful outcome if another process temporarily prevents it.
        let phase = std::time::Instant::now();
        let _ = self.repository.state.compact_transient_state().await;
        stats.maintenance_nanos += phase.elapsed().as_nanos() as u64;
        tracing::info!(roots = root_keys.len(), "filesystem import completed");
        stats.traversal_nanos = traversal_nanos.load(std::sync::atomic::Ordering::Relaxed);
        Ok((root_keys, stats))

        }).await
    }

    /// Commit one bounded post-order import checkpoint, then make its file
    /// identities reusable. Remembering happens after the logical commit: a
    /// crash can lose an accelerator entry, but can never make a later import
    /// trust payload bytes whose object record was not made durable.
    pub(super) async fn publish_filesystem_checkpoint(
        &self,
        staged: Vec<(
            StagedObject<'_>,
            Option<(crate::filesystem::root::FileIdentity, BlobId)>,
        )>,
        root_changes: Vec<RootChange>,
        remember: bool,
    ) -> Result<CommitResult, RepositoryError> {
        self.publish_filesystem_checkpoint_with_policy(staged, root_changes, remember, None)
            .await
    }

    async fn publish_filesystem_checkpoint_with_policy(
        &self,
        staged: Vec<(
            StagedObject<'_>,
            Option<(crate::filesystem::root::FileIdentity, BlobId)>,
        )>,
        root_changes: Vec<RootChange>,
        remember: bool,
        policy: Option<crate::MetadataChange>,
    ) -> Result<CommitResult, RepositoryError> {
        let mut objects = Vec::with_capacity(staged.len());
        let mut learned = Vec::new();
        for (object, identity) in staged {
            objects.push(object);
            learned.extend(identity);
        }
        let result = self
            .publish_filesystem_constructed_with_metadata(objects, root_changes, policy)
            .await?;
        if let Some(cache) = self.repository.profile.ingest_cache().filter(|_| remember)
            && !learned.is_empty()
        {
            cache.remember(&learned).await?;
        }
        Ok(result)
    }
}

impl<PS, SS> MutationSession<'_, PS, SS>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    /// Which of these files the repository already holds the content of.
    ///
    /// A file is recognized only when an earlier import recorded its exact
    /// device, inode, size and timestamps, and the object recorded for it is
    /// still committed. Collection can have removed that object since, so the
    /// recorded digests are confirmed against state before any of them is
    /// trusted; this session's retention hold then keeps them alive.
    pub(super) async fn recognize_files(
        &self,
        identities: Vec<Option<crate::filesystem::root::FileIdentity>>,
    ) -> Result<Vec<Option<(u64, BlobId)>>, RepositoryError> {
        let mut answers = vec![None; identities.len()];
        let Some(cache) = self.repository.profile.ingest_cache() else {
            return Ok(answers);
        };

        let known: Vec<_> = identities
            .iter()
            .enumerate()
            .filter_map(|(at, identity)| identity.map(|identity| (at, identity)))
            .collect();
        let recalled = cache
            .recall(&known.iter().map(|(_, id)| *id).collect::<Vec<_>>())
            .await?;

        let candidates: Vec<_> = known
            .iter()
            .zip(recalled)
            .filter_map(|((at, _), digest)| digest.map(|digest| (*at, digest)))
            .collect();
        let keys: Vec<_> = candidates
            .iter()
            .map(|(_, digest)| ObjectKey::blob(*digest))
            .collect();
        self.pin
            .protect(
                keys.iter()
                    .cloned()
                    .map(crate::metadata::PinResource::Object)
                    .collect(),
            )
            .await?;
        let (snapshot, _snapshot_pin) = self.pinned_snapshot().await?;
        let formats = &self.repository.formats;
        let payloads = if formats.is_builtin() {
            // A present built-in raw blob proves its own closure.
            snapshot
                .object_batch(&keys)
                .await?
                .into_iter()
                .map(|record| {
                    record
                        .filter(|record| formats.intrinsically_complete(record))
                        .map(|record| (record.payload(), record.payload_size()))
                })
                .collect()
        } else {
            snapshot.validated_payload_batch(&keys).await?
        };
        for ((at, digest), payload) in candidates.into_iter().zip(payloads) {
            if let Some((payload, size)) = payload
                && payload == digest
            {
                answers[at] = Some((size, digest));
            }
        }
        Ok(answers)
    }
}

pub(crate) async fn stage_filesystem_file<'hold, PS, SS>(
    mutation: &'hold MutationSession<'_, PS, SS>,
    root: &crate::filesystem::root::FsRoot,
    path: &Path,
) -> Result<(u64, bool, StagedObject<'hold>), RepositoryError>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    let (file, metadata) = root.open_file(path).await?;
    let executable = crate::filesystem::is_executable(&metadata);
    // Most trees are mostly small files, and a fixed read buffer would allocate
    // orders of magnitude more than each one needs. The cap still applies; only
    // the floor moves.
    let capacity = usize::try_from(metadata.len())
        .unwrap_or(crate::filesystem::FILE_READ_BUFFER_SIZE)
        .clamp(1, crate::filesystem::FILE_READ_BUFFER_SIZE);
    let mut file = tokio::io::BufReader::with_capacity(capacity, file);
    let object = mutation.stage_blob_reader(&mut file).await?;
    Ok((object.record().payload_size(), executable, object))
}
