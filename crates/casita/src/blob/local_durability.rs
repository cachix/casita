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
    static SYNCED_DIRECTORIES: std::cell::RefCell<Option<Vec<PathBuf>>> = const { std::cell::RefCell::new(None) };
    static SYNCED_FILES: std::cell::RefCell<Option<Vec<PathBuf>>> = const { std::cell::RefCell::new(None) };
}

static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

const RECLAIM_MARKER_BYTES: &[u8] = b"catalog garbage may be present\n";

#[cfg(test)]
#[derive(Debug, Default)]
struct MarkerPause {
    entered: tokio::sync::Notify,
    resumed: std::sync::Mutex<bool>,
    resume: std::sync::Condvar,
}

#[cfg(test)]
impl MarkerPause {
    fn wait(&self) {
        let mut resumed = self.resumed.lock().unwrap();
        self.entered.notify_one();
        while !*resumed {
            resumed = self.resume.wait(resumed).unwrap();
        }
    }

    fn release(&self) {
        *self.resumed.lock().unwrap() = true;
        self.resume.notify_all();
    }
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
    #[cfg(test)]
    marker_pause: Option<Arc<MarkerPause>>,
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
            #[cfg(test)]
            marker_pause: None,
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

    /// Keep the reclamation hint durable without replacing an existing inode.
    /// Presence is not a durability proof: another publisher may have renamed
    /// the hint but not flushed its directory yet. Always flush both the file
    /// and its directory chain before dependent catalog objects are published.
    #[tracing::instrument(
        name = "blob.local_durability.ensure_reclaim_marker",
        level = "debug",
        skip_all
    )]
    pub(crate) async fn ensure_reclaim_marker(&self, location: &Path) -> io::Result<()> {
        let destination = self
            .filesystem
            .path_to_filesystem(location)
            .map_err(io::Error::other)?;
        let root = Arc::clone(&self.root);
        #[cfg(test)]
        let pause = self.marker_pause.clone();
        self.pins
            .capture()
            .write(pin_path(location), async move {
                tokio::task::spawn_blocking(move || {
                    #[cfg(test)]
                    if let Some(pause) = pause {
                        pause.wait();
                    }
                    durable_ensure_reclaim_marker(&root, &destination)
                })
                .await
                .map_err(io::Error::other)?
            })
            .await
    }

    /// Admit marker and object together, then durably publish them in order.
    pub(crate) async fn put_with_reclaim_marker(
        &self,
        marker: &Path,
        location: &Path,
        bytes: Bytes,
    ) -> io::Result<()> {
        self.publish_with_reclaim_marker(marker, location, move |root, destination| {
            durable_put(root, destination, &bytes)
        })
        .await
    }

    pub(crate) async fn put_file_with_reclaim_marker(
        &self,
        marker: &Path,
        location: &Path,
        source: File,
    ) -> io::Result<()> {
        self.publish_with_reclaim_marker(marker, location, move |root, destination| {
            durable_put_file(root, destination, source)
        })
        .await
    }

    // Admit both identities before either I/O operation. The owned write scope
    // fences collection until both ordered durability barriers settle, even
    // when the caller is cancelled between marker and object publication.
    async fn publish_with_reclaim_marker(
        &self,
        marker: &Path,
        location: &Path,
        publish: impl FnOnce(&FsPath, &FsPath) -> io::Result<()> + Send + 'static,
    ) -> io::Result<()> {
        let marker_destination = self
            .filesystem
            .path_to_filesystem(marker)
            .map_err(io::Error::other)?;
        let destination = self
            .filesystem
            .path_to_filesystem(location)
            .map_err(io::Error::other)?;
        let root = Arc::clone(&self.root);
        let mut resources = pin_path(marker);
        resources.extend(pin_path(location));
        #[cfg(test)]
        let pause = self.marker_pause.clone();
        self.pins
            .capture()
            .write(resources, async move {
                tokio::task::spawn_blocking(move || {
                    #[cfg(test)]
                    if let Some(pause) = pause {
                        pause.wait();
                    }
                    durable_ensure_reclaim_marker(&root, &marker_destination)?;
                    publish(&root, &destination)
                })
                .await
                .map_err(io::Error::other)?
            })
            .await
    }

    /// Durably publish a potentially large object without materializing it.
    #[cfg(test)]
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

#[cfg(unix)]
fn durable_ensure_reclaim_marker(root: &FsPath, destination: &FsPath) -> io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;

    if !destination.starts_with(root) {
        return Err(io::Error::other(
            "reclaim marker escaped its filesystem root",
        ));
    }
    let file = match OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(destination)
    {
        Ok(file) => file,
        Err(error)
            if error.kind() == io::ErrorKind::NotFound
                || error.kind() == io::ErrorKind::PermissionDenied
                || error.raw_os_error() == Some(libc::ELOOP) =>
        {
            return durable_put(root, destination, RECLAIM_MARKER_BYTES);
        }
        Err(error) => return Err(error),
    };
    if !file.metadata()?.is_file() {
        drop(file);
        return durable_put(root, destination, RECLAIM_MARKER_BYTES);
    }
    #[cfg(test)]
    super::crash_tests::file_checkpoint("before-marker-sync", destination);
    file.sync_all()?;
    #[cfg(test)]
    SYNCED_FILES.with_borrow_mut(|record| {
        if let Some(paths) = record {
            paths.push(destination.to_path_buf());
        }
    });
    #[cfg(test)]
    super::crash_tests::file_checkpoint("after-marker-sync", destination);
    let parent = destination
        .parent()
        .ok_or_else(|| io::Error::other("reclaim marker has no parent"))?;
    #[cfg(test)]
    super::crash_tests::file_checkpoint("before-marker-directory-sync", destination);
    sync_directory_chain(root, parent)?;
    #[cfg(test)]
    super::crash_tests::file_checkpoint("after-marker-directory-sync", destination);
    Ok(())
}

#[cfg(not(unix))]
fn durable_ensure_reclaim_marker(root: &FsPath, destination: &FsPath) -> io::Result<()> {
    // Keep the existing publication path where portable no-follow opens and
    // directory flushing cannot establish the Unix reuse contract.
    durable_put(root, destination, RECLAIM_MARKER_BYTES)
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
        output.sync_all()?;
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
        file.sync_all()
    }) {
        drop(file);
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    #[cfg(test)]
    super::crash_tests::file_checkpoint("after-file-sync", &destination);
    #[cfg(test)]
    SYNCED_FILES.with_borrow_mut(|record| {
        if let Some(paths) = record {
            paths.push(destination.clone());
        }
    });
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
    directories.sort_unstable_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        #[cfg(test)]
        super::crash_tests::checkpoint("before-directory-component-sync");
        File::open(&directory)?.sync_all()?;
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
    fn reclaim_marker_reuses_the_inode_but_flushes_file_and_directory() {
        use std::os::unix::fs::MetadataExt;

        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let marker = root.join("nested/pack-index-reclaim-needed");
        SYNCED_FILES.with_borrow_mut(|record| *record = Some(Vec::new()));
        SYNCED_DIRECTORIES.with_borrow_mut(|record| *record = Some(Vec::new()));
        durable_ensure_reclaim_marker(root, &marker).unwrap();
        assert_eq!(std::fs::read(&marker).unwrap(), RECLAIM_MARKER_BYTES);
        let original = File::open(&marker).unwrap();
        let inode = original.metadata().unwrap().ino();
        // An independently visible but not explicitly synced marker must not
        // cause the reuse path to skip either durability barrier.
        std::fs::write(&marker, RECLAIM_MARKER_BYTES).unwrap();
        durable_ensure_reclaim_marker(root, &marker).unwrap();
        assert_eq!(std::fs::metadata(&marker).unwrap().ino(), inode);
        assert_eq!(std::fs::read(&marker).unwrap(), RECLAIM_MARKER_BYTES);
        let files = SYNCED_FILES.with_borrow_mut(|record| record.take().unwrap());
        let directories = SYNCED_DIRECTORIES.with_borrow_mut(|record| record.take().unwrap());
        assert_eq!(files, vec![marker.clone(), marker.clone()]);
        assert_eq!(directories.iter().filter(|path| **path == root).count(), 2);
        assert_eq!(
            directories
                .iter()
                .filter(|path| **path == root.join("nested"))
                .count(),
            2
        );
        assert_eq!(
            std::fs::read_dir(marker.parent().unwrap()).unwrap().count(),
            1
        );
    }

    #[cfg(unix)]
    #[test]
    fn reclaim_marker_replaces_symlinks_without_opening_their_targets() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("other-object");
        std::fs::write(&target, b"unchanged object").unwrap();
        let marker = directory.path().join("pack-index-reclaim-needed");
        symlink(&target, &marker).unwrap();
        durable_ensure_reclaim_marker(directory.path(), &marker).unwrap();
        assert!(!std::fs::symlink_metadata(&marker).unwrap().is_symlink());
        assert_eq!(std::fs::read(&marker).unwrap(), RECLAIM_MARKER_BYTES);
        assert_eq!(std::fs::read(&target).unwrap(), b"unchanged object");
    }

    #[tokio::test]
    async fn reclaim_marker_observes_another_handles_clear_and_recreates_after_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let open = || {
            LocalDurability::new(
                LocalFileSystem::new_with_prefix(directory.path()).unwrap(),
                directory.path(),
            )
            .unwrap()
        };
        let writer = open();
        let collector = open();
        let path = Path::from("pack-index-reclaim-needed");
        writer.ensure_reclaim_marker(&path).await.unwrap();
        collector.delete(&path).await.unwrap();
        assert!(!directory.path().join(path.as_ref()).exists());
        writer.ensure_reclaim_marker(&path).await.unwrap();
        drop(writer);
        let reopened = open();
        reopened.ensure_reclaim_marker(&path).await.unwrap();
        collector.delete(&path).await.unwrap();
        reopened.ensure_reclaim_marker(&path).await.unwrap();
        assert_eq!(
            std::fs::read(directory.path().join(path.as_ref())).unwrap(),
            RECLAIM_MARKER_BYTES
        );
    }

    #[tokio::test]
    async fn cancelled_reclaim_marker_flush_keeps_collection_fenced_until_io_settles() {
        use crate::metadata::{
            DataPin, DataPinLease, MemoryPinStore, PinBindings, PinScope, PinStore,
            flush_repository_leases,
        };
        use std::collections::BTreeSet;

        struct ResumeOnDrop(Arc<MarkerPause>);
        impl Drop for ResumeOnDrop {
            fn drop(&mut self) {
                self.0.release();
            }
        }

        for (already_present, operation) in [
            (false, 0),
            (true, 0),
            (false, 1),
            (true, 1),
            (false, 2),
            (true, 2),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = Path::from("pack-index-reclaim-needed");
            let mut durability = LocalDurability::new(
                LocalFileSystem::new_with_prefix(directory.path()).unwrap(),
                directory.path(),
            )
            .unwrap();
            if already_present {
                durability.ensure_reclaim_marker(&path).await.unwrap();
            }
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
            durability = durability.with_pins(bindings);
            let pause = Arc::new(MarkerPause::default());
            let resume = ResumeOnDrop(pause.clone());
            durability.marker_pause = Some(pause.clone());
            let writing = durability.clone();
            let location = path.clone();
            let object = Path::from("catalog-object");
            let object_location = object.clone();
            let task = tokio::spawn(async move {
                match operation {
                    0 => writing.ensure_reclaim_marker(&location).await,
                    1 => {
                        writing
                            .put_with_reclaim_marker(
                                &location,
                                &object_location,
                                Bytes::from_static(b"catalog"),
                            )
                            .await
                    }
                    _ => {
                        let mut source = tempfile::tempfile().unwrap();
                        source.write_all(b"catalog").unwrap();
                        std::io::Seek::seek(&mut source, std::io::SeekFrom::Start(0)).unwrap();
                        writing
                            .put_file_with_reclaim_marker(&location, &object_location, source)
                            .await
                    }
                }
            });
            tokio::time::timeout(std::time::Duration::from_secs(5), pause.entered.notified())
                .await
                .unwrap();
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            drop(pin);
            let collector = ledger.acquire_collection(None).await.unwrap().unwrap();
            let inventory = ledger.inventory().await.unwrap();
            assert_eq!(inventory.pins.len(), 1);
            assert!(inventory.pins.values().next().unwrap().resources.contains(
                &crate::metadata::PinResource::StorageObject(path.to_string())
            ));
            let mut protected = pin_path(&path);
            if operation != 0 {
                protected.extend(pin_path(&object));
                assert!(!directory.path().join(object.as_ref()).exists());
            }
            assert_eq!(inventory.pins.values().next().unwrap().resources, protected);
            assert!(
                ledger
                    .claim_deletions(inventory.revision, protected)
                    .await
                    .unwrap()
                    .is_none()
            );
            drop(resume);
            flush_repository_leases().await.unwrap();
            // Released pins remain retired until this collector finishes.
            ledger.finish_collection(&collector).await.unwrap();
            let collector = ledger.acquire_collection(None).await.unwrap().unwrap();
            let inventory = ledger.inventory().await.unwrap();
            assert!(inventory.pins.is_empty());
            assert_eq!(
                std::fs::read(directory.path().join(path.as_ref())).unwrap(),
                RECLAIM_MARKER_BYTES
            );
            if operation != 0 {
                assert_eq!(
                    std::fs::read(directory.path().join(object.as_ref())).unwrap(),
                    b"catalog"
                );
            }
            let claim = ledger
                .claim_deletions(inventory.revision, pin_path(&path))
                .await
                .unwrap()
                .unwrap();
            durability.delete(&path).await.unwrap();
            ledger.finish_deletions(&claim).await.unwrap();
            ledger.finish_collection(&collector).await.unwrap();
        }
    }

    #[tokio::test]
    async fn combined_publication_preserves_marker_before_object_failure_ordering() {
        let directory = tempfile::tempdir().unwrap();
        let durability = LocalDurability::new(
            LocalFileSystem::new_with_prefix(directory.path()).unwrap(),
            directory.path(),
        )
        .unwrap();
        let marker = Path::from("marker");
        let object = Path::from("object");
        std::fs::create_dir(directory.path().join("marker")).unwrap();
        assert!(
            durability
                .put_with_reclaim_marker(&marker, &object, Bytes::from_static(b"catalog"))
                .await
                .is_err()
        );
        assert!(!directory.path().join("object").exists());
        std::fs::remove_dir(directory.path().join("marker")).unwrap();
        std::fs::create_dir(directory.path().join("object")).unwrap();
        assert!(
            durability
                .put_with_reclaim_marker(&marker, &object, Bytes::from_static(b"catalog"))
                .await
                .is_err()
        );
        assert_eq!(
            std::fs::read(directory.path().join("marker")).unwrap(),
            RECLAIM_MARKER_BYTES
        );
        assert!(directory.path().join("object").is_dir());
    }

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
