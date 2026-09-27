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
    if directories.get(root).await?.is_none() {
        return Err(Error::NotFound {
            digest: (*root).into(),
        });
    }
    let target = target.as_ref();
    let mut stage = CheckoutStage::create(target).await?;
    let result = checkout_staged(payloads, directories, root, stage.path()).await;
    match result {
        Ok(()) => stage.publish().await,
        Err(error) => Err(error),
    }
}

/// Materialize one verified directory graph below an already-created staging
/// root. [`CheckoutStage`] owns cleanup until its caller publishes it.
async fn checkout_staged<BS, DS>(
    payloads: &BS,
    directories: &DS,
    root: &DirectoryId,
    staging: &Path,
) -> Result<(), Error>
where
    BS: BlobStore,
    DS: DirectorySource,
{
    let target = FsRoot::open_write(staging).await?;

    let mut files = Vec::<(BlobId, bool, PathBuf)>::new();
    let mut symlinks = Vec::<(SymlinkTarget, PathBuf)>::new();
    let mut pending = vec![(*root, PathBuf::new())];
    let mut directory_count = 0usize;
    while let Some((digest, directory_path)) = pending.pop() {
        directory_count += 1;
        let directory = directories.get(&digest).await?.ok_or(Error::NotFound {
            digest: digest.into(),
        })?;
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
                } => files.push((*digest, *executable, child)),
                Node::Symlink { target } => symlinks.push((target.clone(), child)),
            }
        }
    }

    tracing::debug!(
        directories = directory_count,
        files = files.len(),
        symlinks = symlinks.len(),
        "filesystem checkout planned"
    );
    let target = &target;
    stream::iter(files)
        .map(|(digest, executable, path)| async move {
            write_file(payloads, target, &digest, executable, &path)
                .await
                .map_err(|error| checkout_path_error(&path, error))
        })
        .buffer_unordered(16)
        .try_collect::<Vec<()>>()
        .await?;

    // Symlinks come last, after anything they may point at exists.
    for (link_target, path) in symlinks {
        create_symlink(target, &link_target, &path)
            .await
            .map_err(|error| checkout_path_error(&path, error))?;
    }
    tracing::info!(
        directories = directory_count,
        "filesystem checkout materialized"
    );
    Ok(())
}

/// A newly-created staging directory contains no caller files. Consequently an
/// `AlreadyExists` result while creating a stored entry means that the target
/// filesystem equates two distinct Casita names (for example by case folding
/// or Unicode normalization), rather than that checkout may overwrite data.
fn checkout_path_error(path: &Path, error: Error) -> Error {
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
struct CheckoutStage {
    temporary: PathBuf,
    destination: PathBuf,
    published: bool,
}

static NEXT_CHECKOUT_STAGE: AtomicU64 = AtomicU64::new(0);

impl CheckoutStage {
    #[tracing::instrument(name = "filesystem.checkout.stage", level = "debug", skip_all)]
    async fn create(destination: &Path) -> Result<Self, Error> {
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
                        temporary,
                        destination: destination.to_path_buf(),
                        published: false,
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

    fn path(&self) -> &Path {
        &self.temporary
    }

    #[tracing::instrument(name = "filesystem.checkout.publish", level = "debug", skip_all)]
    async fn publish(&mut self) -> Result<(), Error> {
        let temporary = self.temporary.clone();
        let destination = self.destination.clone();
        tokio::task::spawn_blocking(move || -> Result<(), Error> {
            match std::fs::rename(&temporary, &destination) {
                Ok(()) => Ok(()),
                Err(error) => Err(error.into()),
            }
        })
        .await??;
        self.published = true;
        Ok(())
    }
}

impl Drop for CheckoutStage {
    fn drop(&mut self) {
        if !self.published {
            let _ = std::fs::remove_dir_all(&self.temporary);
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
const WINDOWS_RESERVED_NAMES: [&str; 22] = [
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

#[cfg(windows)]
fn check_windows_directory_names(
    directory: &crate::Directory,
    directory_path: &Path,
) -> Result<(), Error> {
    let mut seen = std::collections::HashMap::<String, &str>::new();
    for (name, _) in directory.nodes() {
        let name = std::str::from_utf8(name.as_bytes()).map_err(|_| -> Error {
            format!("stored name {name} is not valid UTF-8, which Windows cannot materialize")
                .into()
        })?;
        check_windows_name(name).map_err(|reason| -> Error {
            format!(
                "cannot materialize `{name}` under {}: {reason}",
                directory_path.display()
            )
            .into()
        })?;
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

/// Validate one name against Windows path and device-name rules.
#[cfg(windows)]
pub(crate) fn check_windows_name(name: &str) -> Result<(), String> {
    for character in name.chars() {
        match character {
            '\\' => return Err("`\\` is a path separator on Windows".into()),
            ':' => return Err("`:` opens a drive or NTFS stream".into()),
            '<' | '>' | '"' | '|' | '?' | '*' => {
                return Err(format!(
                    "`{character}` is not allowed in Windows file names"
                ));
            }
            '\0'..='\x1f' => {
                return Err(format!(
                    "control character {:#04x} is not allowed in Windows file names",
                    character as u32
                ));
            }
            _ => {}
        }
    }
    if name.ends_with('.') || name.ends_with(' ') {
        return Err("Windows strips a trailing dot or space".into());
    }
    let base = name.split('.').next().unwrap_or(name);
    if WINDOWS_RESERVED_NAMES
        .iter()
        .any(|reserved| base.eq_ignore_ascii_case(reserved))
    {
        return Err(format!("`{base}` is a reserved device name on Windows"));
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
