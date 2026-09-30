//! Power-loss-safe publication for the filesystem-backed packed catalog.

use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use object_store::local::LocalFileSystem;
use object_store::path::Path;

#[cfg(test)]
thread_local! {
    pub(crate) static SYNCED_DIRECTORIES: std::cell::RefCell<Option<Vec<PathBuf>>> = const { std::cell::RefCell::new(None) };
    /// The directories among [`SYNCED_DIRECTORIES`] whose sync waited for the drive.
    static PERSISTED_DIRECTORIES: std::cell::RefCell<Vec<PathBuf>> = const { std::cell::RefCell::new(Vec::new()) };
}

static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Flush `file` so that it reaches storage before any write issued after this
/// call, without waiting for it to persist.
///
/// On Apple platforms a plain `fsync` hands data to the drive, whose volatile
/// cache may persist later writes first, and `sync_all` (`F_FULLFSYNC`) waits
/// until the drive has emptied that whole cache. A publication that syncs a
/// file and then its directories needs that wait only once. So every sync but
/// the last issues an I/O barrier (`F_BARRIERFSYNC`), which no later write can
/// overtake, and the last is a `sync_all`, which persists everything before
/// it. Filesystems without barriers get the full flush. Elsewhere `fsync`
/// already persists the data, so this is `sync_all`.
pub(crate) fn sync_ordered(file: &File) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    {
        use nix::errno::Errno;
        use nix::fcntl::{FcntlArg, fcntl};
        match fcntl(file, FcntlArg::F_BARRIERFSYNC) {
            Ok(_) => return Ok(()),
            Err(Errno::ENOTSUP | Errno::EINVAL | Errno::ENOTTY) => {}
            Err(errno) => return Err(errno.into()),
        }
    }
    file.sync_all()
}

/// The concrete filesystem handle retained alongside the erased object store.
///
/// `object_store::local::LocalFileSystem` already publishes through a sibling
/// temporary file, but does not flush that file or the containing directory.
/// Catalog publication needs the stronger ordering supplied here so an
/// installed root cannot survive a power loss without all immutable catalog
/// objects it names.
#[derive(Clone, Debug)]
pub(crate) struct LocalDurability {
    filesystem: LocalFileSystem,
    root: Arc<PathBuf>,
    pins: crate::metadata::PinBindings,
    deletions: super::deletion_barrier::DeletionBarrier,
}

/// Held from catalog snapshot selection through durable pointer publication.
/// Clones keep a cancelled caller's blocking write protected until it finishes.
#[derive(Clone, Debug)]
pub(crate) struct LocalCatalogLock {
    durability: LocalDurability,
    _file: Arc<LockedCatalogFile>,
}

#[derive(Debug)]
struct LockedCatalogFile(File);

impl Drop for LockedCatalogFile {
    fn drop(&mut self) {
        // A concurrent fork can inherit a descriptor until the child execs.
        // Closing our copy alone would leave the lock held by that duplicate.
        // Unlock only when the last writer guard finishes, including a blocking
        // publication that outlives its cancelled caller. Close is the fallback
        // if unlocking fails, since Drop cannot report an error.
        let _ = self.0.unlock();
    }
}

#[derive(Debug)]
pub(crate) struct PreparedLocalPut {
    temporary: PathBuf,
    destination: PathBuf,
    pins: crate::metadata::WritePins,
}

impl Drop for PreparedLocalPut {
    fn drop(&mut self) {
        // A committed temporary path no longer exists. On preparation or
        // publication failure this removes the unpublished sibling instead.
        let _ = std::fs::remove_file(&self.temporary);
    }
}

impl LocalCatalogLock {
    /// Compare and durably replace a catalog pointer under a process-shared
    /// lock. The lock inode is separate from the pointer's renamed inode.
    pub(crate) async fn compare_and_put(
        &self,
        location: &Path,
        bytes: Bytes,
        expected: Option<[u8; 32]>,
    ) -> io::Result<bool> {
        let destination = self
            .durability
            .filesystem
            .path_to_filesystem(location)
            .map_err(io::Error::other)?;
        let guard = self.clone();
        self.durability
            .pins
            .capture()
            .write(pin_path(location), async move {
                tokio::task::spawn_blocking(move || {
                    let held = guard;
                    let current = match std::fs::read(&destination) {
                        Ok(bytes) => Some(*blake3::hash(&bytes).as_bytes()),
                        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
                        Err(error) => return Err(error),
                    };
                    if current != expected {
                        return Ok(false);
                    }
                    durable_put(&held.durability.root, &destination, &bytes)?;
                    Ok(true)
                })
                .await
                .map_err(io::Error::other)?
            })
            .await
    }
}

impl LocalDurability {
    pub(crate) fn with_pins(mut self, pins: crate::metadata::PinBindings) -> Self {
        self.pins = pins;
        self
    }

    /// Order deletions after the metadata commits that allow them.
    pub(crate) fn with_deletion_barrier(
        mut self,
        deletions: super::deletion_barrier::DeletionBarrier,
    ) -> Self {
        self.deletions = deletions;
        self
    }

    pub(crate) async fn lock_catalog(&self) -> io::Result<LocalCatalogLock> {
        let durability = self.clone();
        tokio::task::spawn_blocking(move || {
            let file = File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(durability.root.join("pack-catalog.lock"))?;
            file.lock()?;
            #[cfg(test)]
            super::crash_tests::checkpoint("catalog-lock-acquired");
            Ok(LocalCatalogLock {
                durability,
                _file: Arc::new(LockedCatalogFile(file)),
            })
        })
        .await
        .map_err(io::Error::other)?
    }

    #[cfg(test)]
    async fn compare_and_put(
        &self,
        location: &Path,
        bytes: Bytes,
        expected: Option<[u8; 32]>,
    ) -> io::Result<bool> {
        self.lock_catalog()
            .await?
            .compare_and_put(location, bytes, expected)
            .await
    }

    pub(crate) fn new(filesystem: LocalFileSystem, root: impl AsRef<FsPath>) -> io::Result<Self> {
        // LocalFileSystem round-trips its canonical root through a file URL.
        // On Windows that removes the verbatim prefix (\\?\), so retain the
        // same spelling it uses for every destination, after verifying identity.
        let backend_root = filesystem
            .path_to_filesystem(&Path::from("pack-catalog.lock"))
            .map_err(io::Error::other)?
            .parent()
            .ok_or_else(|| io::Error::other("catalog lock path has no parent"))?
            .to_path_buf();
        if std::fs::canonicalize(&backend_root)? != std::fs::canonicalize(root)? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "durability root differs from object-store root",
            ));
        }
        Ok(Self {
            filesystem,
            root: Arc::new(backend_root),
            pins: Default::default(),
            deletions: Default::default(),
        })
    }

    #[tracing::instrument(
        name = "blob.local_durability.put",
        level = "debug",
        skip_all,
        fields(bytes = bytes.len())
    )]
    pub(crate) async fn put(&self, location: &Path, bytes: Bytes) -> io::Result<()> {
        let destination = self
            .filesystem
            .path_to_filesystem(location)
            .map_err(io::Error::other)?;
        let root = Arc::clone(&self.root);
        // Durable filesystem operations have no portable non-blocking API.
        // Keep their write and fsync latency off Tokio's async worker threads.
        self.pins
            .capture()
            .write(pin_path(location), async move {
                tokio::task::spawn_blocking(move || durable_put(&root, &destination, &bytes))
                    .await
                    .map_err(io::Error::other)?
            })
            .await
    }

    /// Durably publish a potentially large object without first materializing
    /// it as one contiguous byte allocation.
    #[tracing::instrument(name = "blob.local_durability.put_file", level = "debug", skip_all)]
    pub(crate) async fn put_file(&self, location: &Path, source: File) -> io::Result<()> {
        let destination = self
            .filesystem
            .path_to_filesystem(location)
            .map_err(io::Error::other)?;
        let root = Arc::clone(&self.root);
        self.pins
            .capture()
            .write(pin_path(location), async move {
                tokio::task::spawn_blocking(move || durable_put_file(&root, &destination, source))
                    .await
                    .map_err(io::Error::other)?
            })
            .await
    }

    #[tracing::instrument(
        name = "blob.local_durability.prepare",
        level = "debug",
        skip_all,
        fields(bytes = bytes.len())
    )]
    pub(crate) async fn prepare(
        &self,
        location: &Path,
        bytes: Bytes,
    ) -> io::Result<PreparedLocalPut> {
        let destination = self
            .filesystem
            .path_to_filesystem(location)
            .map_err(io::Error::other)?;
        let pins = self.pins.capture();
        let mut prepared = pins
            .clone()
            .write(pin_path(location), async move {
                tokio::task::spawn_blocking(move || prepare_put(destination, &bytes))
                    .await
                    .map_err(io::Error::other)?
            })
            .await?;
        prepared.pins = pins;
        Ok(prepared)
    }

    #[tracing::instrument(
        name = "blob.local_durability.commit",
        level = "debug",
        skip_all,
        fields(objects = prepared.len())
    )]
    pub(crate) async fn commit(&self, prepared: Vec<PreparedLocalPut>) -> io::Result<()> {
        let root = Arc::clone(&self.root);
        tokio::task::spawn_blocking(move || commit_prepared(&root, prepared))
            .await
            .map_err(io::Error::other)?
    }

    #[tracing::instrument(name = "blob.local_durability.delete", level = "debug", skip_all)]
    pub(crate) async fn delete(&self, location: &Path) -> io::Result<()> {
        let destination = self
            .filesystem
            .path_to_filesystem(location)
            .map_err(io::Error::other)?;
        let root = Arc::clone(&self.root);
        self.deletions.before_deletion().await?;
        tokio::task::spawn_blocking(move || durable_delete(&root, &destination))
            .await
            .map_err(io::Error::other)?
    }

    #[tracing::instrument(
        name = "blob.local_durability.delete_many",
        level = "debug",
        skip_all,
        fields(objects = locations.len())
    )]
    pub(crate) async fn delete_many(&self, locations: Vec<Path>) -> io::Result<()> {
        let destinations = locations
            .iter()
            .map(|location| {
                self.filesystem
                    .path_to_filesystem(location)
                    .map_err(io::Error::other)
            })
            .collect::<io::Result<Vec<_>>>()?;
        let root = Arc::clone(&self.root);
        self.deletions.before_deletion().await?;
        tokio::task::spawn_blocking(move || durable_delete_many(&root, destinations))
            .await
            .map_err(io::Error::other)?
    }
}

fn durable_put(root: &FsPath, destination: &FsPath, bytes: &[u8]) -> io::Result<()> {
    let prepared = prepare_put(destination.to_path_buf(), bytes)?;
    commit_prepared(root, vec![prepared])
}

fn durable_put_file(root: &FsPath, destination: &FsPath, source: File) -> io::Result<()> {
    let parent = destination
        .parent()
        .ok_or_else(|| io::Error::other("durable object path has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let (temporary, mut output) = create_temporary(destination)?;
    #[cfg(test)]
    super::crash_tests::file_checkpoint("temporary-created", destination);
    let copied = (|| {
        let mut input = BufReader::new(source);
        let mut buffer = vec![0_u8; 1024 * 1024];
        loop {
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            output.write_all(&buffer[..read])?;
            #[cfg(test)]
            super::crash_tests::file_checkpoint("stream-block-written", destination);
        }
        #[cfg(test)]
        super::crash_tests::file_checkpoint("before-file-sync", destination);
        sync_ordered(&output)?;
        #[cfg(test)]
        super::crash_tests::file_checkpoint("after-file-sync", destination);
        Ok::<_, io::Error>(())
    })();
    if let Err(error) = copied {
        drop(output);
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    drop(output);
    commit_prepared(
        root,
        vec![PreparedLocalPut {
            temporary,
            destination: destination.to_path_buf(),
            pins: Default::default(),
        }],
    )
}

fn prepare_put(destination: PathBuf, bytes: &[u8]) -> io::Result<PreparedLocalPut> {
    let parent = destination
        .parent()
        .ok_or_else(|| io::Error::other("durable object path has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let (temporary, mut file) = create_temporary(&destination)?;
    #[cfg(test)]
    super::crash_tests::file_checkpoint("temporary-created", &destination);
    if let Err(error) = file.write_all(bytes).and_then(|()| {
        #[cfg(test)]
        super::crash_tests::file_checkpoint("before-file-sync", &destination);
        sync_ordered(&file)
    }) {
        drop(file);
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    #[cfg(test)]
    super::crash_tests::file_checkpoint("after-file-sync", &destination);
    drop(file);
    Ok(PreparedLocalPut {
        temporary,
        destination,
        pins: Default::default(),
    })
}

fn pin_path(location: &Path) -> std::collections::BTreeSet<crate::metadata::PinResource> {
    std::collections::BTreeSet::from([crate::metadata::PinResource::StorageObject(
        location.to_string(),
    )])
}

fn commit_prepared(root: &FsPath, prepared: Vec<PreparedLocalPut>) -> io::Result<()> {
    let mut parents = Vec::with_capacity(prepared.len());
    for object in &prepared {
        let parent = object
            .destination
            .parent()
            .ok_or_else(|| io::Error::other("durable object path has no parent"))?;
        #[cfg(test)]
        super::crash_tests::file_checkpoint("before-rename", &object.destination);
        std::fs::rename(&object.temporary, &object.destination)?;
        #[cfg(test)]
        super::crash_tests::file_checkpoint("after-rename", &object.destination);
        parents.push(parent.to_path_buf());
    }
    #[cfg(test)]
    super::crash_tests::checkpoint("before-directory-sync");
    sync_directory_chains(root, parents)?;
    #[cfg(test)]
    super::crash_tests::checkpoint("after-directory-sync");
    Ok(())
}

fn durable_delete(root: &FsPath, destination: &FsPath) -> io::Result<()> {
    sync_directory_chain(root, &remove_file_for_sync(root, destination)?)
}

fn durable_delete_many(root: &FsPath, destinations: Vec<PathBuf>) -> io::Result<()> {
    let mut parents = Vec::with_capacity(destinations.len());
    for destination in destinations {
        parents.push(remove_file_for_sync(root, &destination)?);
    }
    sync_directory_chains(root, parents)
}

fn remove_file_for_sync(root: &FsPath, destination: &FsPath) -> io::Result<PathBuf> {
    if !destination.starts_with(root) {
        return Err(io::Error::other(
            "durable object path escaped its filesystem root",
        ));
    }
    let missing = match std::fs::remove_file(destination) {
        Ok(()) => false,
        Err(error) if error.kind() == io::ErrorKind::NotFound => true,
        Err(error) => return Err(error),
    };
    let mut parent = destination
        .parent()
        .ok_or_else(|| io::Error::other("durable object path has no parent"))?;
    // A previous attempt may have unlinked the file before failing its sync
    // or another deletion. Absence alone does not establish durability.
    // Missing directory trees still require syncing their surviving ancestor.
    while missing && parent != root && !parent.try_exists()? {
        parent = parent
            .parent()
            .ok_or_else(|| io::Error::other("durable object path has no parent"))?;
    }
    Ok(parent.to_path_buf())
}

fn create_temporary(destination: &FsPath) -> io::Result<(PathBuf, File)> {
    for _ in 0..16 {
        // object_store reserves a trailing `#<digits>` suffix for staged local
        // uploads and excludes such files from listings after a process crash.
        let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let unique = (u64::from(std::process::id()) << 32) | sequence;
        let mut name: OsString = destination.as_os_str().to_owned();
        name.push(format!("#{unique}"));
        let path = PathBuf::from(name);
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a durable object temporary file",
    ))
}

/// Flush `directory` so an entry just renamed or linked into it survives power
/// loss. Like catalog publication's directory flushes, this is a no-op on
/// non-Unix platforms.
///
/// Used by the `casita` CLI; not an application compatibility surface.
pub fn sync_directory(directory: &FsPath) -> io::Result<()> {
    sync_directory_chain(directory, directory)
}

#[cfg(unix)]
fn sync_directory_chain(root: &FsPath, parent: &FsPath) -> io::Result<()> {
    sync_directory_chains(root, [parent.to_path_buf()])
}

#[cfg(unix)]
fn sync_directory_chains(
    root: &FsPath,
    parents: impl IntoIterator<Item = PathBuf>,
) -> io::Result<()> {
    let mut directories = std::collections::BTreeSet::new();
    for parent in parents {
        if !parent.starts_with(root) {
            return Err(io::Error::other(
                "durable object path escaped its filesystem root",
            ));
        }
        let mut current = Some(parent.as_path());
        while let Some(directory) = current {
            directories.insert(directory.to_path_buf());
            if directory == root {
                break;
            }
            current = directory.parent();
        }
        if !directories.contains(root) {
            return Err(io::Error::other(
                "durable object path did not reach its filesystem root",
            ));
        }
    }
    let mut directories = directories.into_iter().collect::<Vec<_>>();
    // Deepest first, so the root, which every chain contains, comes last. Its
    // flush waits for the drive and so persists the files and directories
    // synced before it (see `sync_ordered`).
    directories.sort_unstable_by_key(|path| std::cmp::Reverse(path.components().count()));
    let last = directories.len().saturating_sub(1);
    for (index, directory) in directories.into_iter().enumerate() {
        #[cfg(test)]
        super::crash_tests::checkpoint("before-directory-component-sync");
        let handle = File::open(&directory)?;
        if index == last {
            handle.sync_all()?;
            #[cfg(test)]
            PERSISTED_DIRECTORIES.with_borrow_mut(|persisted| persisted.push(directory.clone()));
        } else {
            sync_ordered(&handle)?;
        }
        #[cfg(test)]
        SYNCED_DIRECTORIES.with_borrow_mut(|record| {
            if let Some(paths) = record {
                paths.push(directory);
            }
        });
        #[cfg(test)]
        super::crash_tests::checkpoint("after-directory-component-sync");
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory_chain(root: &FsPath, parent: &FsPath) -> io::Result<()> {
    sync_directory_chains(root, [parent.to_path_buf()])
}

#[cfg(not(unix))]
fn sync_directory_chains(
    root: &FsPath,
    parents: impl IntoIterator<Item = PathBuf>,
) -> io::Result<()> {
    for parent in parents {
        if !parent.starts_with(root) {
            return Err(io::Error::other(
                "durable object path escaped its filesystem root",
            ));
        }
    }
    // Rust does not expose portable directory-handle flushing on non-Unix
    // platforms. Every data file is still flushed before its atomic rename.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn deletion_retry_syncs_previously_removed_files_and_missing_parents() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let left = root.join("left/object");
        let right = root.join("right/object");
        std::fs::create_dir_all(left.parent().unwrap()).unwrap();
        std::fs::create_dir_all(&right).unwrap(); // Fail after deleting left.
        std::fs::write(&left, b"left").unwrap();
        assert!(durable_delete_many(root, vec![left.clone(), right.clone()]).is_err());
        assert!(!left.exists());
        std::fs::remove_dir(&right).unwrap();
        std::fs::write(&right, b"right").unwrap();
        SYNCED_DIRECTORIES.with_borrow_mut(|record| *record = Some(Vec::new()));
        durable_delete_many(root, vec![left.clone(), right.clone()]).unwrap();
        let synced = SYNCED_DIRECTORIES.with_borrow_mut(|record| record.take().unwrap());
        assert!(
            synced.contains(&left.parent().unwrap().to_path_buf()),
            "retry must flush the earlier unlink even when the file is already absent"
        );
        assert!(synced.contains(&right.parent().unwrap().to_path_buf()));
        assert_eq!(synced.iter().filter(|path| *path == root).count(), 1);
        SYNCED_DIRECTORIES.with_borrow_mut(|record| *record = Some(Vec::new()));
        durable_delete(root, &left).unwrap();
        durable_delete_many(root, vec![root.join("never/created/object")]).unwrap();
        let synced = SYNCED_DIRECTORIES.with_borrow_mut(|record| record.take().unwrap());
        assert!(synced.contains(&left.parent().unwrap().to_path_buf()));
        assert!(synced.contains(&root.to_path_buf()));
    }

    /// Every directory of a new chain is synced, deepest first, and only the
    /// last flush, of the root, waits for the drive. That flush is what makes
    /// the whole publication durable, so it must come after every other sync.
    #[cfg(unix)]
    #[test]
    fn a_publication_waits_for_the_drive_once_after_every_other_sync() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let object = root.join("pack-indexes/b3/ab/object");
        SYNCED_DIRECTORIES.with_borrow_mut(|record| *record = Some(Vec::new()));
        PERSISTED_DIRECTORIES.with_borrow_mut(Vec::clear);
        durable_put(root, &object, b"catalog shard").unwrap();
        let synced = SYNCED_DIRECTORIES.with_borrow_mut(|record| record.take().unwrap());
        let parents = ["pack-indexes/b3/ab", "pack-indexes/b3", "pack-indexes"];
        let mut chain = parents.map(|parent| root.join(parent)).to_vec();
        chain.push(root.to_path_buf());
        assert_eq!(synced, chain);
        assert_eq!(PERSISTED_DIRECTORIES.take(), [root.to_path_buf()]);
        assert_eq!(std::fs::read(&object).unwrap(), b"catalog shard");
    }

    #[tokio::test]
    async fn prepared_publication_owns_its_pin_until_commit_finishes() {
        use crate::metadata::{
            DataPin, DataPinLease, MemoryPinStore, PinBindings, PinScope, PinStore,
            flush_repository_leases,
        };
        use std::collections::BTreeSet;

        let directory = tempfile::tempdir().unwrap();
        let ledger = Arc::new(MemoryPinStore::default());
        let pin = DataPinLease::acquire(
            ledger.clone(),
            DataPin {
                scope: PinScope::Staging,
                catalog: None,
                resources: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
        let bindings = PinBindings::default();
        bindings.attach(&pin);
        let durability = LocalDurability::new(
            LocalFileSystem::new_with_prefix(directory.path()).unwrap(),
            directory.path(),
        )
        .unwrap()
        .with_pins(bindings);
        let path = Path::from("catalog/shard");
        let prepared = durability
            .prepare(&path, Bytes::from_static(b"catalog contents"))
            .await
            .unwrap();
        drop(pin);
        let inventory = ledger.inventory().await.unwrap();
        assert!(
            ledger
                .claim_deletions(inventory.revision, pin_path(&path))
                .await
                .unwrap()
                .is_none()
        );
        durability.commit(vec![prepared]).await.unwrap();
        assert_eq!(
            std::fs::read(directory.path().join("catalog/shard")).unwrap(),
            b"catalog contents"
        );
        flush_repository_leases().await.unwrap();
        assert!(ledger.inventory().await.unwrap().pins.is_empty());
    }

    #[test]
    fn rejects_a_durability_root_from_another_store() {
        let left = tempfile::tempdir().unwrap();
        let right = tempfile::tempdir().unwrap();
        let filesystem = LocalFileSystem::new_with_prefix(left.path()).unwrap();
        assert_eq!(
            LocalDurability::new(filesystem, right.path())
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[tokio::test]
    async fn catalog_lock_stays_held_until_the_last_writer_clone_finishes() {
        let directory = tempfile::tempdir().unwrap();
        let durability = LocalDurability::new(
            LocalFileSystem::new_with_prefix(directory.path()).unwrap(),
            directory.path(),
        )
        .unwrap();
        let guard = durability.lock_catalog().await.unwrap();
        // Like a descriptor inherited between fork and exec, this duplicate
        // must not prolong the lock after the last writer guard is gone.
        #[cfg(unix)]
        let duplicate = guard._file.0.try_clone().unwrap();
        let writing = guard.clone();
        drop(guard);
        let probe = File::options()
            .read(true)
            .write(true)
            .open(directory.path().join("pack-catalog.lock"))
            .unwrap();
        assert!(matches!(
            probe.try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
        drop(writing);
        probe.try_lock().unwrap();
        #[cfg(unix)]
        drop(duplicate);
    }

    #[tokio::test]
    async fn competing_catalog_updates_have_one_winner() {
        let directory = tempfile::tempdir().unwrap();
        let open = || {
            LocalDurability::new(
                LocalFileSystem::new_with_prefix(directory.path()).unwrap(),
                directory.path(),
            )
            .unwrap()
        };
        let left = open();
        let right = open();
        let location = Path::from("pack-index-current");
        assert!(
            left.compare_and_put(&location, Bytes::from_static(b"base"), None)
                .await
                .unwrap()
        );
        let expected = Some(*blake3::hash(b"base").as_bytes());
        let (a, b) = tokio::join!(
            left.compare_and_put(&location, Bytes::from_static(b"left"), expected),
            right.compare_and_put(&location, Bytes::from_static(b"right"), expected),
        );
        assert_ne!(a.unwrap(), b.unwrap());
        let bytes = std::fs::read(directory.path().join("pack-index-current")).unwrap();
        assert!(bytes == b"left" || bytes == b"right");
        assert!(
            !left
                .compare_and_put(&location, Bytes::from_static(b"stale"), None)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn durable_publication_overwrites_atomically_and_deletes_cleanly() {
        let temporary = tempfile::tempdir().unwrap();
        let filesystem = LocalFileSystem::new_with_prefix(temporary.path()).unwrap();
        let durability = LocalDurability::new(filesystem, temporary.path()).unwrap();
        let location = Path::from("pack-index-current");

        durability
            .put(&location, Bytes::from_static(b"old root"))
            .await
            .unwrap();
        durability
            .put(&location, Bytes::from_static(b"complete new root"))
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(temporary.path().join("pack-index-current")).unwrap(),
            b"complete new root"
        );
        assert!(
            std::fs::read_dir(temporary.path())
                .unwrap()
                .all(|entry| !entry.unwrap().file_name().to_string_lossy().contains('#'))
        );

        durability.delete(&location).await.unwrap();
        durability.delete(&location).await.unwrap();
        assert!(!temporary.path().join("pack-index-current").exists());
    }

    #[tokio::test]
    async fn durable_publication_flushes_new_directory_chains() {
        let temporary = tempfile::tempdir().unwrap();
        let filesystem = LocalFileSystem::new_with_prefix(temporary.path()).unwrap();
        let durability = LocalDurability::new(filesystem, temporary.path()).unwrap();
        let location = Path::from("pack-indexes/b3/ab/object");

        durability
            .put(&location, Bytes::from_static(b"catalog shard"))
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(temporary.path().join("pack-indexes/b3/ab/object")).unwrap(),
            b"catalog shard"
        );
    }

    #[tokio::test]
    async fn prepared_group_is_invisible_until_commit_and_cleans_abandoned_temps() {
        let temporary = tempfile::tempdir().unwrap();
        let filesystem = LocalFileSystem::new_with_prefix(temporary.path()).unwrap();
        let durability = LocalDurability::new(filesystem, temporary.path()).unwrap();
        let left = Path::from("pack-indexes/b3/aa/left");
        let right = Path::from("pack-indexes/b3/bb/right");

        let (left_prepared, right_prepared) = tokio::join!(
            durability.prepare(&left, Bytes::from_static(b"left shard")),
            durability.prepare(&right, Bytes::from_static(b"right shard")),
        );
        let left_prepared = left_prepared.unwrap();
        let right_prepared = right_prepared.unwrap();
        assert!(!left_prepared.destination.exists());
        assert!(!right_prepared.destination.exists());
        assert!(left_prepared.temporary.exists());
        assert!(right_prepared.temporary.exists());

        durability
            .commit(vec![left_prepared, right_prepared])
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(temporary.path().join("pack-indexes/b3/aa/left")).unwrap(),
            b"left shard"
        );
        assert_eq!(
            std::fs::read(temporary.path().join("pack-indexes/b3/bb/right")).unwrap(),
            b"right shard"
        );

        let abandoned = durability
            .prepare(
                &Path::from("pack-indexes/b3/cc/abandoned"),
                Bytes::from_static(b"abandoned shard"),
            )
            .await
            .unwrap();
        let abandoned_path = abandoned.temporary.clone();
        drop(abandoned);
        assert!(!abandoned_path.exists());

        durability.delete_many(vec![left, right]).await.unwrap();
        assert!(!temporary.path().join("pack-indexes/b3/aa/left").exists());
        assert!(!temporary.path().join("pack-indexes/b3/bb/right").exists());
    }
}
