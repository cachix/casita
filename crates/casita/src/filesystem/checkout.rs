//! Materialize a verified canonical directory graph on the filesystem.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use futures::{StreamExt, TryStreamExt, stream};

use crate::blob::BlobStore;
use crate::digest::{BlobId, DirectoryId};
use crate::error::Error;
use crate::filesystem::names;
use crate::filesystem::root::FsRoot;
use crate::node::Node;
use crate::path::SymlinkTarget;

/// Read-only directory lookup supplied by a repository retention hold.
#[async_trait::async_trait]
pub(crate) trait DirectorySource: Send + Sync {
    async fn get(&self, digest: &DirectoryId) -> Result<Option<crate::Directory>, Error>;
}

/// Write `root` into an empty `target`, without overwriting existing content.
///
/// The complete tree is first written to a fresh sibling on the target
/// filesystem. That makes target-specific filename rules (case folding,
/// Unicode normalization, and native name encoding) a strict admission check:
/// Casita either writes the exact names or leaves `target` free of partial
/// content. On success the sibling is renamed into place.
/// On Windows filesystems that cannot replace an existing empty directory,
/// publication briefly removes that directory before renaming the sibling.
///
/// Every directory, file, and link is created relative to one open handle on
/// the staging root, so nothing this writes can land outside it even if a
/// component is swapped for a link while the checkout runs. See
/// [`crate::filesystem::root`] for the exact boundary.
#[tracing::instrument(name = "filesystem.checkout", skip_all)]
pub(crate) async fn checkout<BS, DS>(
    payloads: &BS,
    directories: &DS,
    root: &DirectoryId,
    target: impl AsRef<Path>,
) -> Result<(), Error>
where
    BS: BlobStore,
    DS: DirectorySource,
{
    let root_directory = directories.get(root).await?.ok_or(Error::NotFound {
        digest: (*root).into(),
    })?;
    let target = target.as_ref();
    let stage = CheckoutStage::create(target).await?;
    checkout_staged(payloads, directories, root, root_directory, stage.path()).await?;
    stage.publish().await??;
    Ok(())
}

/// Materialize one verified directory graph below an already-created staging
/// root. [`CheckoutStage`] owns cleanup until its caller publishes it.
async fn checkout_staged<BS, DS>(
    payloads: &BS,
    directories: &DS,
    root: &DirectoryId,
    root_directory: crate::Directory,
    staging: &Path,
) -> Result<(), Error>
where
    BS: BlobStore,
    DS: DirectorySource,
{
    let target = FsRoot::open_write(staging).await?;
    let entries = prepare_directories(directories, root, root_directory, &target).await?;

    tracing::debug!(
        directories = entries.directory_count,
        files = entries.files.len(),
        symlinks = entries.symlinks.len(),
        "filesystem checkout entries prepared"
    );
    let target = &target;
    stream::iter(entries.files)
        .map(|file| async move {
            write_file(payloads, target, &file.digest, file.executable, &file.path)
                .await
                .map_err(|error| checkout_path_error(&file.path, error))
        })
        .buffer_unordered(16)
        .try_collect::<Vec<()>>()
        .await?;

    // Symlinks come last, after anything they may point at exists.
    for link in entries.symlinks {
        create_symlink(target, &link.target, &link.path)
            .await
            .map_err(|error| checkout_path_error(&link.path, error))?;
    }
    tracing::info!(
        directories = entries.directory_count,
        "filesystem checkout materialized"
    );
    Ok(())
}

struct FileEntry {
    digest: BlobId,
    executable: bool,
    path: PathBuf,
}

struct SymlinkEntry {
    target: SymlinkTarget,
    path: PathBuf,
}

struct PreparedEntries {
    files: Vec<FileEntry>,
    symlinks: Vec<SymlinkEntry>,
    directory_count: usize,
}

/// Create parent directories before writing their files and links. The returned
/// entries wait until every parent directory exists.
async fn prepare_directories<DS: DirectorySource>(
    directories: &DS,
    root: &DirectoryId,
    root_directory: crate::Directory,
    target: &FsRoot,
) -> Result<PreparedEntries, Error> {
    let mut entries = PreparedEntries {
        files: Vec::new(),
        symlinks: Vec::new(),
        directory_count: 0,
    };
    let mut pending = vec![(*root, PathBuf::new())];
    let mut root_directory = Some(root_directory);
    while let Some((digest, directory_path)) = pending.pop() {
        entries.directory_count += 1;
        let directory = match root_directory.take() {
            Some(directory) => directory,
            None => directories.get(&digest).await?.ok_or(Error::NotFound {
                digest: digest.into(),
            })?,
        };
        #[cfg(windows)]
        check_windows_directory_names(&directory, &directory_path)?;

        for (name, node) in directory.nodes() {
            let child = directory_path.join(names::os_str_from_bytes(name.as_bytes())?);
            match node {
                Node::Directory { digest, .. } => {
                    target
                        .create_dir(&child)
                        .await
                        .map_err(|error| checkout_path_error(&child, error))?;
                    pending.push((*digest, child));
                }
                Node::File {
                    digest, executable, ..
                } => entries.files.push(FileEntry {
                    digest: *digest,
                    executable: *executable,
                    path: child,
                }),
                Node::Symlink { target } => entries.symlinks.push(SymlinkEntry {
                    target: target.clone(),
                    path: child,
                }),
            }
        }
    }
    Ok(entries)
}

/// A newly-created staging directory contains no caller files. Consequently an
/// `AlreadyExists` result while creating a stored entry means that the target
/// filesystem equates two distinct Casita names (for example by case folding
/// or Unicode normalization), rather than that checkout may overwrite data.
pub(crate) fn checkout_path_error(path: &Path, error: Error) -> Error {
    match error {
        Error::Io(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            Error::TargetNameConflict {
                path: path.to_path_buf(),
            }
        }
        error => error,
    }
}

/// Unique sibling directory used to make a checkout all-or-nothing from the
/// caller's point of view.
///
/// The final target is never populated directly. A fresh directory beside it
/// both guarantees the same target filesystem semantics and gives the normal
/// rename operation a single complete tree to publish. The target itself may
/// already exist, but must remain an empty real directory.
pub(crate) struct CheckoutStage {
    temporary: Option<PathBuf>,
    destination: PathBuf,
}

static NEXT_CHECKOUT_STAGE: AtomicU64 = AtomicU64::new(0);

impl CheckoutStage {
    #[tracing::instrument(name = "filesystem.checkout.stage", level = "debug", skip_all)]
    pub(crate) async fn create(destination: &Path) -> Result<Self, Error> {
        // Preserve the public "empty target" contract while avoiding creation
        // of an absent target before the staged checkout has succeeded.
        match tokio::fs::symlink_metadata(destination).await {
            Ok(_) => {
                let existing = FsRoot::open_read(destination).await?;
                if !existing.is_empty().await? {
                    return Err(Error::TargetNotEmpty {
                        path: existing.path().to_path_buf(),
                    });
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }

        let file_name = destination.file_name().ok_or_else(|| {
            Error::from(format!(
                "checkout target {} has no final path component",
                destination.display()
            ))
        })?;
        let parent = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));

        for _ in 0..128 {
            let sequence = NEXT_CHECKOUT_STAGE.fetch_add(1, Ordering::Relaxed);
            // Do not extend the destination's basename: it may already be
            // near the filesystem's per-component length limit.
            let name = format!(".casita-checkout-{}-{sequence}", std::process::id());
            if file_name == std::ffi::OsStr::new(&name) {
                continue;
            }
            let temporary = parent.join(name);
            match tokio::fs::create_dir(&temporary).await {
                Ok(()) => {
                    return Ok(Self {
                        temporary: Some(temporary),
                        destination: destination.to_path_buf(),
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate a unique sibling checkout staging directory",
        )
        .into())
    }

    pub(crate) fn path(&self) -> &Path {
        self.temporary
            .as_deref()
            .expect("staging path exists until publication")
    }

    /// The blocking task owns cleanup as well as the rename. Dropping its
    /// join handle cannot remove the stage while publication is still running.
    #[tracing::instrument(name = "filesystem.checkout.publish", level = "debug", skip_all)]
    pub(crate) fn publish(mut self) -> tokio::task::JoinHandle<Result<(), Error>> {
        tokio::task::spawn_blocking(move || {
            let result = std::fs::rename(self.path(), &self.destination).map_err(Error::from);
            #[cfg(windows)]
            let result = result.or_else(|error| {
                publish_over_empty_directory(self.path(), &self.destination, error)
            });
            result?;
            drop(self.temporary.take());
            Ok(())
        })
    }
}

/// Some Windows filesystems cannot rename a directory over an existing empty
/// directory. Remove that empty destination and restore it if the second rename
/// fails. A concurrent observer may briefly see the destination absent.
#[cfg(windows)]
fn publish_over_empty_directory(
    temporary: &Path,
    destination: &Path,
    rename_error: Error,
) -> Result<(), Error> {
    match std::fs::symlink_metadata(destination) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        _ => return Err(rename_error),
    }
    if std::fs::read_dir(destination)?.next().is_some() {
        return Err(Error::TargetNotEmpty {
            path: destination.to_path_buf(),
        });
    }
    std::fs::remove_dir(destination)?;
    if let Err(error) = std::fs::rename(temporary, destination) {
        let _ = std::fs::create_dir(destination);
        return Err(error.into());
    }
    Ok(())
}

impl Drop for CheckoutStage {
    fn drop(&mut self) {
        if let Some(temporary) = &self.temporary {
            let _ = std::fs::remove_dir_all(temporary);
        }
    }
}

#[cfg(unix)]
async fn create_symlink(root: &FsRoot, target: &SymlinkTarget, path: &Path) -> Result<(), Error> {
    let target = names::os_str_from_bytes(target.as_bytes())?;
    root.symlink(path, Path::new(&target), false).await
}

#[cfg(windows)]
async fn create_symlink(root: &FsRoot, target: &SymlinkTarget, path: &Path) -> Result<(), Error> {
    let stored = std::str::from_utf8(target.as_bytes()).map_err(|_| -> Error {
        format!("symlink target {target} is not valid UTF-8, which Windows cannot materialize")
            .into()
    })?;
    let native = PathBuf::from(stored.replace('/', "\\"));
    // Windows picks the link flavor at creation time. A target inside the
    // checkout decides it; one that would leave the root is not consulted and
    // materializes as a file link, exactly as before.
    let resolved = path
        .parent()
        .map_or_else(|| native.clone(), |parent| parent.join(&native));
    let is_directory = root.is_directory(&resolved).await;
    root.symlink(path, &native, is_directory)
        .await
        .map_err(|error| -> Error {
            let raw = match &error {
                Error::Io(io) => io.raw_os_error(),
                _ => None,
            };
            if raw == Some(1314) {
                format!(
                    "creating the symlink {} requires Windows Developer Mode or administrator rights: {error}",
                    path.display()
                )
                .into()
            } else {
                error
            }
        })
}

#[cfg(windows)]
fn check_windows_directory_names(
    directory: &crate::Directory,
    directory_path: &Path,
) -> Result<(), Error> {
    let mut seen = std::collections::HashMap::<String, &str>::new();
    for (name, _) in directory.nodes() {
        let name = names::check_windows_stored_name(name.as_bytes(), directory_path)?;
        if let Some(prior) = seen.insert(name.to_lowercase(), name) {
            return Err(format!(
                "cannot materialize {}: `{prior}` and `{name}` differ only by case",
                directory_path.display()
            )
            .into());
        }
    }
    Ok(())
}

async fn write_file<BS: BlobStore>(
    payloads: &BS,
    root: &FsRoot,
    digest: &BlobId,
    executable: bool,
    path: &Path,
) -> Result<(), Error> {
    let mut reader = payloads.open_read(digest).await?.ok_or(Error::NotFound {
        digest: (*digest).into(),
    })?;
    let mut file = root.create_file(path, executable).await?;
    tokio::io::copy(&mut reader, &mut file).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use async_trait::async_trait;

    use super::*;
    use crate::{Digest, Directory, MemoryBlobStore, Node, PathComponent};

    struct Directories {
        entries: BTreeMap<DirectoryId, Directory>,
    }

    #[async_trait]
    impl DirectorySource for Directories {
        async fn get(&self, digest: &DirectoryId) -> Result<Option<Directory>, Error> {
            Ok(self.entries.get(digest).cloned())
        }
    }

    /// Return a root that creates one staging directory before discovering its
    /// child is missing. This exercises cleanup after a materialization failure
    /// rather than a failure caught before the target is touched.
    fn root_with_missing_child() -> (DirectoryId, Directories) {
        let missing = DirectoryId::new(Digest::from([7; 32]));
        let root = Directory::try_from_iter([(
            PathComponent::try_from("nested").unwrap(),
            Node::Directory {
                digest: missing,
                size: 0,
            },
        )])
        .unwrap();
        let root_id = root.digest();
        (
            root_id,
            Directories {
                entries: BTreeMap::from([(root_id, root)]),
            },
        )
    }

    #[cfg(target_os = "macos")]
    fn root_with_case_collision() -> (DirectoryId, Directories) {
        let child = Directory::new();
        let child_id = child.digest();
        let root = Directory::try_from_iter([
            (
                PathComponent::try_from("Foo").unwrap(),
                Node::Directory {
                    digest: child_id,
                    size: 0,
                },
            ),
            (
                PathComponent::try_from("foo").unwrap(),
                Node::Directory {
                    digest: child_id,
                    size: 0,
                },
            ),
        ])
        .unwrap();
        let root_id = root.digest();
        (
            root_id,
            Directories {
                entries: BTreeMap::from([(root_id, root), (child_id, child)]),
            },
        )
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn checkout_accepts_a_long_destination_name() {
        for existing in [false, true] {
            let temporary = tempfile::tempdir().unwrap();
            let destination = temporary.path().join("x".repeat(240));
            // Establish that the destination name is valid on this filesystem.
            std::fs::create_dir(&destination).unwrap();
            if !existing {
                std::fs::remove_dir(&destination).unwrap();
            }
            let child = Directory::new();
            let child_id = child.digest();
            let root = Directory::try_from_iter([(
                PathComponent::try_from("nested").unwrap(),
                Node::Directory {
                    digest: child_id,
                    size: 0,
                },
            )])
            .unwrap();
            let root_id = root.digest();
            let directories = Directories {
                entries: BTreeMap::from([(root_id, root), (child_id, child)]),
            };

            checkout(
                &MemoryBlobStore::new(),
                &directories,
                &root_id,
                &destination,
            )
            .await
            .unwrap();

            assert!(destination.join("nested").is_dir());
            assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 1);
        }
    }

    #[tokio::test]
    async fn failed_checkout_leaves_an_absent_target_absent_and_removes_staging() {
        let temporary = tempfile::tempdir().unwrap();
        let destination = temporary.path().join("checkout");
        let (root, directories) = root_with_missing_child();

        assert!(
            checkout(&MemoryBlobStore::new(), &directories, &root, &destination)
                .await
                .is_err()
        );

        assert!(!destination.exists());
        assert!(
            std::fs::read_dir(temporary.path())
                .unwrap()
                .next()
                .is_none()
        );
    }

    #[tokio::test]
    async fn failed_checkout_leaves_an_existing_empty_target_empty() {
        let temporary = tempfile::tempdir().unwrap();
        let destination = temporary.path().join("checkout");
        std::fs::create_dir(&destination).unwrap();
        let (root, directories) = root_with_missing_child();

        assert!(
            checkout(&MemoryBlobStore::new(), &directories, &root, &destination)
                .await
                .is_err()
        );

        assert!(destination.is_dir());
        assert!(std::fs::read_dir(&destination).unwrap().next().is_none());
        assert_eq!(
            std::fs::read_dir(temporary.path()).unwrap().count(),
            1,
            "failed checkout leaked a sibling staging directory"
        );
    }

    #[tokio::test]
    async fn checkout_replaces_an_existing_empty_target() {
        let temporary = tempfile::tempdir().unwrap();
        let destination = temporary.path().join("checkout");
        std::fs::create_dir(&destination).unwrap();
        let root = Directory::new();
        let root_id = root.digest();
        let directories = Directories {
            entries: BTreeMap::from([(root_id, root)]),
        };

        checkout(
            &MemoryBlobStore::new(),
            &directories,
            &root_id,
            &destination,
        )
        .await
        .unwrap();

        assert!(destination.is_dir());
        assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn publication_preserves_a_destination_filled_after_staging() {
        let temporary = tempfile::tempdir().unwrap();
        let destination = temporary.path().join("checkout");
        std::fs::create_dir(&destination).unwrap();
        let stage = CheckoutStage::create(&destination).await.unwrap();
        std::fs::write(stage.path().join("new"), b"staged").unwrap();
        std::fs::write(destination.join("existing"), b"caller data").unwrap();

        assert!(stage.publish().await.unwrap().is_err());

        assert_eq!(
            std::fs::read(destination.join("existing")).unwrap(),
            b"caller data"
        );
        assert!(!destination.join("new").exists());
        assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn publication_does_not_follow_a_destination_link_added_after_staging() {
        let temporary = tempfile::tempdir().unwrap();
        let outside = temporary.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("existing"), b"caller data").unwrap();
        let destination = temporary.path().join("checkout");
        let stage = CheckoutStage::create(&destination).await.unwrap();
        std::fs::write(stage.path().join("new"), b"staged").unwrap();
        std::os::unix::fs::symlink(&outside, &destination).unwrap();

        assert!(stage.publish().await.unwrap().is_err());

        assert!(
            std::fs::symlink_metadata(&destination)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            std::fs::read(outside.join("existing")).unwrap(),
            b"caller data"
        );
        assert!(!outside.join("new").exists());
        assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 2);
    }

    #[cfg(windows)]
    #[test]
    fn windows_fallback_replaces_an_existing_empty_directory() {
        let temporary = tempfile::tempdir().unwrap();
        let staging = temporary.path().join("staging");
        let destination = temporary.path().join("checkout");
        std::fs::create_dir(&staging).unwrap();
        std::fs::write(staging.join("file"), b"payload").unwrap();
        std::fs::create_dir(&destination).unwrap();

        publish_over_empty_directory(
            &staging,
            &destination,
            io::Error::from(io::ErrorKind::AlreadyExists).into(),
        )
        .unwrap();

        assert_eq!(std::fs::read(destination.join("file")).unwrap(), b"payload");
        assert!(!staging.exists());
    }

    #[cfg(windows)]
    #[test]
    fn windows_fallback_restores_the_empty_directory_after_rename_failure() {
        let temporary = tempfile::tempdir().unwrap();
        let destination = temporary.path().join("checkout");
        std::fs::create_dir(&destination).unwrap();

        assert!(
            publish_over_empty_directory(
                &temporary.path().join("missing-stage"),
                &destination,
                io::Error::from(io::ErrorKind::AlreadyExists).into(),
            )
            .is_err()
        );

        assert!(destination.is_dir());
        assert!(std::fs::read_dir(&destination).unwrap().next().is_none());
    }

    #[test]
    fn dropping_a_queued_publication_keeps_its_staging_directory() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let temporary = tempfile::tempdir().unwrap();
            let destination = temporary.path().join("checkout");
            let stage = CheckoutStage::create(&destination).await.unwrap();
            std::fs::write(stage.path().join("file"), b"payload").unwrap();

            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
            let blocker = tokio::task::spawn_blocking(move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
            started_rx.await.unwrap();

            let publication = stage.publish();
            drop(publication);
            assert!(!destination.exists());
            release_tx.send(()).unwrap();
            blocker.await.unwrap();

            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while !destination.exists() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(std::fs::read(destination.join("file")).unwrap(), b"payload");
            assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 1);
        });
    }

    #[test]
    fn existing_names_in_a_fresh_stage_are_typed_as_target_name_conflicts() {
        let error = checkout_path_error(
            Path::new("nested/Foo"),
            Error::Io(io::Error::from(io::ErrorKind::AlreadyExists)),
        );
        assert!(
            matches!(error, Error::TargetNameConflict { path } if path == Path::new("nested/Foo"))
        );
    }

    /// APFS is normally case-insensitive, but macOS also supports
    /// case-sensitive APFS volumes. Probe the actual test filesystem so this
    /// remains a valid regression test in either configuration without trying
    /// to create or reformat a volume.
    #[cfg(target_os = "macos")]
    fn target_is_case_insensitive(parent: &Path) -> bool {
        let probe = parent.join("casita-case-probe");
        std::fs::write(&probe, b"probe").unwrap();
        parent.join("CASITA-CASE-PROBE").exists()
    }

    /// A portable tree may contain both spellings. On the usual
    /// case-insensitive APFS target, checkout must reject it in the sibling
    /// staging directory and leave the requested path absent. A developer may
    /// run this same test on a case-sensitive APFS volume, where both names
    /// must materialize exactly instead.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn macos_checkout_observes_the_target_case_rules() {
        let temporary = tempfile::tempdir().unwrap();
        let case_insensitive = target_is_case_insensitive(temporary.path());
        let destination = temporary.path().join("checkout");
        let (root, directories) = root_with_case_collision();

        let result = checkout(&MemoryBlobStore::new(), &directories, &root, &destination).await;
        if case_insensitive {
            let error = result.unwrap_err();
            assert!(matches!(
                error,
                Error::TargetNameConflict { path } if path == Path::new("foo")
            ));
            assert!(!destination.exists());
            assert!(
                std::fs::read_dir(temporary.path())
                    .unwrap()
                    .all(|entry| entry.unwrap().file_name() == "casita-case-probe")
            );
        } else {
            result.unwrap();
            assert!(destination.join("Foo").is_dir());
            assert!(destination.join("foo").is_dir());
        }
    }
}
