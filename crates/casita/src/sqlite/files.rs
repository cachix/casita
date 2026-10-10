//! The files a [`TursoDb`](super::TursoDb) commits through, and the locks that
//! keep an ordinary SQLite client from removing them.
//!
//! An ordinary SQLite client does not see Turso's multi-process WAL
//! coordination, which uses its own `-tshm` file and takes no lock on the
//! database file. Such a client builds its own WAL index from the frames it
//! finds and, when its last connection closes, takes the database file's
//! exclusive lock, checkpoints the frames it knew into the database file and
//! unlinks the WAL. A live casita process keeps committing to the unlinked
//! file; the next process to open the repository creates a new WAL and starts
//! from the checkpointed database file. Those commits are lost, and so is
//! anything a deletion justified by them removed. A client that writes or
//! checkpoints while casita is running damages the WAL the same way.
//!
//! A process with the database open therefore holds SQLite's own locks
//! against that, shared by all its handles of the database and taken before
//! the engine first touches its files:
//!
//! - a read lock on the database file's pending and reserved bytes and the
//!   first byte of its shared range, so no SQLite client can take a reserved,
//!   pending or exclusive lock: none can checkpoint and unlink the WAL on its
//!   last close, use exclusive locking mode, change the journal mode or write
//!   in rollback mode;
//! - a read lock on the write, checkpoint and recovery bytes of the WAL index
//!   (`-shm`), which Turso never uses, so no SQLite client can rebuild its
//!   index from the WAL, append to it or checkpoint it.
//!
//! The database lock comes first. A client removes the WAL index only on its
//! last close, under the exclusive lock that refuses, so the index locked is
//! the one at its path, the one a client would attach to. Each later write or
//! deletion confirms that, and locks the current index if something else
//! removed it.
//!
//! These are shared locks, so casita processes never contend with each other.
//! A SQLite client still opens the database, and then fails with
//! `SQLITE_BUSY` or `SQLITE_PROTOCOL` instead of reading it. A client that
//! holds a conflicting lock first makes opening fail as busy until it closes.
//!
//! Where the WAL index neither exists nor can be created, as on a full
//! filesystem, there is nothing to lock. The process then also takes a write
//! lock on one other byte of the shared range, so no client gets the shared
//! lock it needs before it would create the index, until a write or deletion
//! of the process can create and lock the index itself. Each process claims
//! its own byte, so casita processes still do not contend.
//!
//! Opening, writes and payload deletions also check that the database and WAL
//! at their paths are the files this handle opened, so a commit that reached
//! files a new process would not read is never acknowledged, and nothing is
//! deleted on its strength. That covers what the locks cannot: a removal by
//! some other means; Windows, where they are not taken; and on platforms with
//! only process-scoped locks, a window after something else in this process
//! closed a descriptor of the file, until the next check takes them again.
//!
//! Both conditions are for the user to resolve, so they surface as typed
//! state errors: [`MetadataError::ForeignSqliteLock`] clears once the client
//! closes, and [`MetadataError::DatabaseReplaced`] is permanent. Once a
//! handle has seen its files replaced, every write, deletion and read through
//! it fails that way, including reads of snapshots taken before, since what
//! they would read is no longer durable.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use crate::metadata::MetadataError;

/// Why a handle cannot vouch that what it commits or reads is durable.
#[derive(Debug)]
pub(crate) enum DurabilityError {
    /// A condition for the user to resolve:
    /// [`MetadataError::ForeignSqliteLock`] or
    /// [`MetadataError::DatabaseReplaced`].
    Condition(MetadataError),
    /// The check itself failed.
    Io(io::Error),
}

impl From<MetadataError> for DurabilityError {
    fn from(error: MetadataError) -> Self {
        Self::Condition(error)
    }
}

impl From<io::Error> for DurabilityError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<DurabilityError> for crate::error::Error {
    fn from(error: DurabilityError) -> Self {
        match error {
            // The metadata store and `RepositoryError` recover the condition
            // from the box.
            DurabilityError::Condition(error) => Self::Backend(Box::new(error)),
            DurabilityError::Io(error) => Self::Io(error),
        }
    }
}

/// For payload deletions, which fail with I/O errors. The condition stays
/// recoverable through `get_ref`, and a lock held by a client is also
/// retryable by its kind alone.
impl From<DurabilityError> for io::Error {
    fn from(error: DurabilityError) -> Self {
        match error {
            DurabilityError::Condition(error) => {
                let kind = match error {
                    MetadataError::ForeignSqliteLock { .. } => io::ErrorKind::WouldBlock,
                    _ => io::ErrorKind::Other,
                };
                Self::new(kind, error)
            }
            DurabilityError::Io(error) => error,
        }
    }
}

/// SQLite's database-file lock bytes (`os.h`): the pending byte, the reserved
/// byte, then the shared range, all of which a client's shared lock reads.
#[cfg(unix)]
const PENDING_BYTE: u64 = 0x4000_0000;
#[cfg(unix)]
const SHARED_RANGE: (u64, u64) = (PENDING_BYTE + 2, 510);
/// The pending byte, the reserved byte and the first shared byte. Every lock
/// beyond shared writes one of them, so a read lock here refuses them all and
/// leaves the rest of the shared range free.
#[cfg(unix)]
const DATABASE_LOCKS: (u64, u64) = (PENDING_BYTE, 3);
/// The rest of the shared range. A write lock on any one of these bytes keeps
/// every client from taking a shared lock.
#[cfg(unix)]
const CLAIMABLE: (u64, u64) = (SHARED_RANGE.0 + 1, SHARED_RANGE.1 - 1);
/// SQLite's WAL-index write, checkpoint and recovery locks (`wal.c`).
#[cfg(unix)]
const LOG_INDEX_LOCKS: (u64, u64) = (120, 3);

/// One file as this handle opened it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

impl FileIdentity {
    #[cfg(unix)]
    fn of_file(file: &std::fs::File) -> io::Result<Self> {
        use std::os::unix::fs::MetadataExt as _;

        let metadata = file.metadata()?;
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    #[cfg(windows)]
    fn of_file(file: &std::fs::File) -> io::Result<Self> {
        let information = winapi_util::file::information(file)?;
        Ok(Self {
            device: information.volume_serial_number(),
            inode: information.file_index(),
        })
    }

    /// The file at `path`, if there is one.
    #[cfg(unix)]
    fn of(path: &Path) -> io::Result<Option<Self>> {
        use std::os::unix::fs::MetadataExt as _;

        match std::fs::metadata(path) {
            Ok(metadata) => Ok(Some(Self {
                device: metadata.dev(),
                inode: metadata.ino(),
            })),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    #[cfg(windows)]
    fn of(path: &Path) -> io::Result<Option<Self>> {
        use std::os::windows::fs::OpenOptionsExt as _;

        // Attribute access only, which no other open's sharing mode refuses.
        const FILE_READ_ATTRIBUTES: u32 = 0x80;
        match std::fs::OpenOptions::new()
            .access_mode(FILE_READ_ATTRIBUTES)
            .open(path)
        {
            Ok(file) => Ok(Some(Self::of_file(&file)?)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }
}

/// Where a database's files live.
#[derive(Debug)]
struct Paths {
    database: PathBuf,
    log: PathBuf,
    /// SQLite's WAL index, which only the unix fence locks.
    #[cfg(unix)]
    log_index: PathBuf,
}

impl Paths {
    fn of(database: &Path) -> Self {
        let sibling = |suffix: &str| {
            let mut path = database.as_os_str().to_owned();
            path.push(suffix);
            PathBuf::from(path)
        };
        Self {
            database: database.to_owned(),
            log: sibling("-wal"),
            #[cfg(unix)]
            log_index: sibling("-shm"),
        }
    }
}

/// The database file and WAL a handle commits through, as it opened them,
/// and the locks that keep SQLite clients from removing them. Every check a
/// write, deletion or read needs is one call here.
#[derive(Debug)]
pub(crate) struct DatabaseFiles {
    paths: Paths,
    opened: (FileIdentity, FileIdentity),
    fence: Arc<Fence>,
    /// The first file found removed or replaced. Set once, never cleared:
    /// the handle cannot reach durable state again.
    replaced: OnceLock<PathBuf>,
}

/// The locks on a database the engine is about to open, and its files as
/// found before.
#[derive(Debug)]
pub(crate) struct LockedFiles {
    paths: Paths,
    /// The database file the locks are on.
    database: FileIdentity,
    /// The WAL, unless absent or about to be discarded by the engine.
    log: Option<FileIdentity>,
    fence: Arc<Fence>,
}

impl DatabaseFiles {
    /// Take the locks for `database` before the engine opens it, so no
    /// SQLite client can interfere with opening or initializing it. Creates
    /// the database file if it is missing, as the engine would.
    pub(crate) fn lock(database: &Path) -> Result<LockedFiles, DurabilityError> {
        let paths = Paths::of(database);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(database)?;
        let identity = FileIdentity::of_file(&file)?;
        // The engine discards a WAL it finds next to an empty database file,
        // which belonged to a removed database, and creates a new one.
        let empty = file.metadata()?.len() == 0;
        let fence = Fence::shared(&paths, file, identity)?;
        let log = if empty {
            None
        } else {
            FileIdentity::of(&paths.log)?
        };
        Ok(LockedFiles {
            paths,
            database: identity,
            log,
            fence,
        })
    }

    /// Before a transaction or a payload deletion: hold the locks again and
    /// confirm the files.
    pub(crate) fn before_change(&self) -> Result<(), DurabilityError> {
        self.fence.hold(&self.paths)?;
        self.after_commit()
    }

    /// After a transaction: fail unless the database and WAL at their paths
    /// are the files this handle opened, since otherwise the commit reached
    /// files no new process reads.
    pub(crate) fn after_commit(&self) -> Result<(), DurabilityError> {
        self.before_read()?;
        let (database, log) = self.opened;
        for (path, opened) in [(&self.paths.database, database), (&self.paths.log, log)] {
            if FileIdentity::of(path)? != Some(opened) {
                let path = self.replaced.get_or_init(|| {
                    tracing::error!(
                        path = %path.display(),
                        "the database or write-ahead log this process commits to was \
                         removed or replaced, typically by a SQLite client that \
                         deleted the log; commits since are not durable, and this \
                         process refuses every further operation until it restarts"
                    );
                    path.clone()
                });
                return Err(replaced(path).into());
            }
        }
        Ok(())
    }

    /// Before a read, including each read of a retained snapshot: fail once
    /// the files are known to be replaced, without touching the filesystem.
    /// Opening, writes and deletions are what detect it.
    pub(crate) fn before_read(&self) -> Result<(), MetadataError> {
        match self.replaced.get() {
            Some(path) => Err(replaced(path)),
            None => Ok(()),
        }
    }
}

impl LockedFiles {
    /// Once the engine has opened the database, which creates the WAL:
    /// confirm that the database is the file locked and the WAL the one
    /// found before, and start checking them.
    pub(crate) fn opened(self) -> Result<DatabaseFiles, DurabilityError> {
        let Self {
            paths,
            database,
            log,
            fence,
        } = self;
        if FileIdentity::of(&paths.database)? != Some(database) {
            return Err(replaced(&paths.database).into());
        }
        let log = match (FileIdentity::of(&paths.log)?, log) {
            (Some(opened), found) if found.is_none_or(|found| found == opened) => opened,
            _ => return Err(replaced(&paths.log).into()),
        };
        Ok(DatabaseFiles {
            paths,
            opened: (database, log),
            fence,
            replaced: OnceLock::new(),
        })
    }
}

fn replaced(path: &Path) -> MetadataError {
    MetadataError::DatabaseReplaced {
        path: path.to_owned(),
    }
}

/// Shared locks on the database file and the WAL index, held for as long as
/// any handle in this process has the database open.
#[cfg(unix)]
#[derive(Debug)]
struct Fence {
    /// Open for writing, for the claim a missing WAL index needs.
    database: std::fs::File,
    log_index: std::sync::Mutex<LogIndex>,
}

#[cfg(unix)]
#[derive(Debug)]
enum LogIndex {
    /// Not looked for yet.
    Unopened,
    /// Read-locked, so no SQLite client can attach to the WAL.
    Locked(std::fs::File),
    /// Neither exists nor could be created, for example on a full filesystem.
    /// Until it can be, a write lock on the claimed byte of the shared range
    /// keeps every SQLite client from reading the database, so none gets to
    /// create the index and attach.
    Missing { claimed: u64 },
}

/// Every database file this process holds a fence on. Process-scoped locks
/// are released when the process closes any descriptor of the file, so the
/// handles of one database share a fence that closes only with the last.
#[cfg(unix)]
static FENCES: std::sync::Mutex<Vec<(FileIdentity, std::sync::Weak<Fence>)>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(unix)]
impl Fence {
    /// The fence on `database`, shared with this process's other handles of
    /// it, with its locks held.
    fn shared(
        paths: &Paths,
        database: std::fs::File,
        identity: FileIdentity,
    ) -> Result<Arc<Self>, DurabilityError> {
        let mut fences = FENCES
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        fences.retain(|(_, fence)| fence.strong_count() > 0);
        let known = fences
            .iter()
            .find(|(known, _)| *known == identity)
            .and_then(|(_, fence)| fence.upgrade());
        if let Some(fence) = known {
            // Closing the descriptor just opened may release the locks.
            drop(database);
            fence.hold(paths)?;
            return Ok(fence);
        }
        let fence = Arc::new(Self {
            database,
            log_index: std::sync::Mutex::new(LogIndex::Unopened),
        });
        fence.hold(paths)?;
        fences.push((identity, Arc::downgrade(&fence)));
        Ok(fence)
    }

    /// Take the locks, or take them again: process-scoped locks are released
    /// when the process closes any descriptor of the file.
    ///
    /// The database file comes first. A client unlinks the WAL index only on
    /// its last close, under the exclusive lock this refuses, so from here on
    /// the index at its path is the one a client would attach to. Locking an
    /// index opened before could protect a file a client has just unlinked.
    fn hold(&self, paths: &Paths) -> Result<(), DurabilityError> {
        if !set_lock(&self.database, libc::F_RDLCK, DATABASE_LOCKS)? {
            return Err(locked(&paths.database));
        }
        let mut log_index = self
            .log_index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let LogIndex::Locked(file) = &*log_index {
            if FileIdentity::of(&paths.log_index)? == Some(FileIdentity::of_file(file)?) {
                if !set_lock(file, libc::F_RDLCK, LOG_INDEX_LOCKS)? {
                    return Err(locked(&paths.log_index));
                }
                return Ok(());
            }
            // Removed by other means, or by a client while process-scoped
            // locks had lapsed: lock what a client would attach to now.
            tracing::warn!(
                path = %paths.log_index.display(),
                "the SQLite WAL index was removed or replaced; locking the current one"
            );
            *log_index = LogIndex::Unopened;
        }
        let claimed = match *log_index {
            LogIndex::Missing { claimed } => Some(claimed),
            _ => None,
        };
        if claimed.is_none()
            && let Some(file) = open_log_index(paths, &self.database)?
        {
            if !set_lock(&file, libc::F_RDLCK, LOG_INDEX_LOCKS)? {
                return Err(locked(&paths.log_index));
            }
            *log_index = LogIndex::Locked(file);
            return Ok(());
        }
        // Without an index, claim a byte, then look again: no client can
        // create the index once the claim is held.
        let claimed = claim(&self.database, claimed)?.ok_or_else(|| locked(&paths.database))?;
        *log_index = LogIndex::Missing { claimed };
        let Some(file) = open_log_index(paths, &self.database)? else {
            return Ok(());
        };
        if !set_lock(&file, libc::F_RDLCK, LOG_INDEX_LOCKS)? {
            return Err(locked(&paths.log_index));
        }
        set_lock(&self.database, libc::F_UNLCK, (claimed, 1))?;
        *log_index = LogIndex::Locked(file);
        Ok(())
    }
}

/// Release the locks explicitly rather than by closing the descriptors: a
/// child process this process is spawning shares their open file
/// descriptions until it executes, and keeps open-file-description locks
/// alive that long.
#[cfg(unix)]
impl Drop for Fence {
    fn drop(&mut self) {
        let whole_file = (0, 0);
        let _ = set_lock(&self.database, libc::F_UNLCK, whole_file);
        if let LogIndex::Locked(file) = &*self
            .log_index
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            let _ = set_lock(file, libc::F_UNLCK, whole_file);
        }
    }
}

#[cfg(unix)]
fn locked(path: &Path) -> DurabilityError {
    MetadataError::ForeignSqliteLock {
        path: path.to_owned(),
    }
    .into()
}

#[cfg(all(test, unix))]
thread_local! {
    /// Runs once, between opening the WAL index and locking it: where a
    /// client closing would do the most harm.
    static AFTER_OPENING_LOG_INDEX: std::cell::Cell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::Cell::new(None) };
}

/// Open the WAL index, creating it if missing, or `None` if it neither exists
/// nor can be created. Only with the database file locked.
#[cfg(unix)]
fn open_log_index(paths: &Paths, database: &std::fs::File) -> io::Result<Option<std::fs::File>> {
    let file = open_or_create_log_index(paths, database)?;
    #[cfg(test)]
    if file.is_some()
        && let Some(hook) = AFTER_OPENING_LOG_INDEX.take()
    {
        hook();
    }
    Ok(file)
}

#[cfg(unix)]
fn open_or_create_log_index(
    paths: &Paths,
    database: &std::fs::File,
) -> io::Result<Option<std::fs::File>> {
    use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};

    // Like SQLite, give the WAL index the database file's permissions, so
    // another user who may open the database may also open it.
    let mode = database.metadata()?.permissions().mode() & 0o777;
    match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(mode)
        .open(&paths.log_index)
    {
        Ok(file) => Ok(Some(file)),
        // Opening an existing repository must not need space or write access
        // (collection runs on a full filesystem): lock the WAL index if it
        // exists.
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::PermissionDenied
                    | io::ErrorKind::StorageFull
                    | io::ErrorKind::QuotaExceeded
                    | io::ErrorKind::ReadOnlyFilesystem
            ) =>
        {
            match std::fs::File::open(&paths.log_index) {
                Ok(file) => Ok(Some(file)),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

/// Write-lock one claimable byte of the database file, trying `previous`
/// first, or return `None` when a client's shared lock covers them all.
#[cfg(unix)]
fn claim(database: &std::fs::File, previous: Option<u64>) -> io::Result<Option<u64>> {
    let (first, count) = CLAIMABLE;
    // Processes start at different bytes, so they seldom try each other's.
    let start = u64::from(std::process::id()) % count;
    let bytes = (0..count).map(|offset| first + (start + offset) % count);
    for byte in previous.into_iter().chain(bytes) {
        if set_lock(database, libc::F_WRLCK, (byte, 1))? {
            return Ok(Some(byte));
        }
    }
    Ok(None)
}

/// Take a lock of `kind` on `length` bytes from `start` without waiting, or
/// release it. Returns whether it is held. Linux uses open-file-description
/// locks, which belong to this descriptor, so neither closing another
/// descriptor of the file nor another handle in this process releases them.
#[cfg(unix)]
fn set_lock(file: &std::fs::File, kind: libc::c_int, range: (u64, u64)) -> io::Result<bool> {
    use nix::fcntl::FcntlArg;

    let lock = byte_range(kind, range)?;
    #[cfg(target_os = "linux")]
    let request = FcntlArg::F_OFD_SETLK(&lock);
    #[cfg(not(target_os = "linux"))]
    let request = FcntlArg::F_SETLK(&lock);
    match nix::fcntl::fcntl(file, request) {
        Ok(_) => Ok(true),
        Err(nix::errno::Errno::EAGAIN | nix::errno::Errno::EACCES) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// An `fcntl` lock description of `length` bytes from `start`.
#[cfg(unix)]
fn byte_range(kind: libc::c_int, (start, length): (u64, u64)) -> io::Result<libc::flock> {
    Ok(libc::flock {
        l_type: kind.try_into().map_err(io::Error::other)?,
        l_whence: libc::SEEK_SET.try_into().map_err(io::Error::other)?,
        l_start: start.try_into().map_err(io::Error::other)?,
        l_len: length.try_into().map_err(io::Error::other)?,
        l_pid: 0,
        #[cfg(target_os = "freebsd")]
        l_sysid: 0,
    })
}

/// On Windows SQLite locks the same bytes with `LockFileEx`, which is not
/// taken here: only the identity check guards a Windows repository.
#[cfg(not(unix))]
#[derive(Debug)]
struct Fence;

#[cfg(not(unix))]
impl Fence {
    fn shared(
        _paths: &Paths,
        _database: std::fs::File,
        _identity: FileIdentity,
    ) -> Result<Arc<Self>, DurabilityError> {
        Ok(Arc::new(Self))
    }

    fn hold(&self, _paths: &Paths) -> Result<(), DurabilityError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::TursoDb;
    use super::*;
    use crate::RetryDisposition;
    use crate::repository::{RepositoryError, RepositoryErrorCategory};

    /// The condition behind a check's failure.
    fn condition<T: std::fmt::Debug>(result: Result<T, DurabilityError>) -> MetadataError {
        match result {
            Err(DurabilityError::Condition(error)) => error,
            other => panic!("expected a condition, got {other:?}"),
        }
    }

    /// The condition behind a `TursoDb` operation's failure, recovered
    /// the way the metadata store recovers it.
    fn engine_condition<T: std::fmt::Debug>(
        result: Result<T, crate::error::Error>,
    ) -> MetadataError {
        match result {
            Err(crate::error::Error::Backend(error)) => *error
                .downcast::<MetadataError>()
                .expect("a typed condition"),
            other => panic!("expected a typed condition, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_replaced_write_ahead_log_ends_the_handle_for_good() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("casita.sqlite");
        let log = directory.path().join("casita.sqlite-wal");
        let db = TursoDb::open(&path).unwrap();
        db.files().before_change().unwrap();
        // Reads find nothing wrong until a check sees the files.
        std::fs::remove_file(&log).unwrap();
        db.files().before_read().unwrap();

        let error = condition(db.files().after_commit());
        assert!(matches!(&error, MetadataError::DatabaseReplaced { path } if *path == log));
        assert_eq!(error.retry_disposition(), RetryDisposition::Never);
        // A process opening now creates a different log, and nothing makes
        // the handle usable again. Windows may keep a removed file's name
        // until its last handle closes.
        #[cfg(unix)]
        std::fs::write(&log, b"").unwrap();
        for check in [
            db.files().before_change(),
            db.files().after_commit(),
            db.files().before_read().map_err(DurabilityError::from),
        ] {
            assert!(matches!(
                condition(check),
                MetadataError::DatabaseReplaced { .. }
            ));
        }
        let read = db.read(|_| Box::pin(async { Ok(()) })).await;
        assert!(matches!(
            engine_condition(read),
            MetadataError::DatabaseReplaced { .. }
        ));
        let write = db.write(|_| Box::pin(async { Ok(()) })).await;
        assert!(matches!(
            engine_condition(write),
            MetadataError::DatabaseReplaced { .. }
        ));
    }

    /// The engine opens whatever is at the paths once the locks are held, so
    /// a file replaced in between fails the open rather than going unchecked.
    #[cfg(unix)]
    #[test]
    fn opening_confirms_the_files_locked_and_found_before() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("casita.sqlite");
        drop(TursoDb::open(&path).unwrap());
        let paths = Paths::of(&path);
        fn replace(file: &Path) {
            let copy = file.with_extension("copy");
            std::fs::copy(file, &copy).unwrap();
            std::fs::rename(&copy, file).unwrap();
        }
        fn remove(file: &Path) {
            std::fs::remove_file(file).unwrap();
        }

        drop(DatabaseFiles::lock(&path).unwrap().opened().unwrap());
        for (changed, change) in [
            (&paths.database, replace as fn(&Path)),
            (&paths.log, replace),
            (&paths.log, remove),
        ] {
            let locked = DatabaseFiles::lock(&path).unwrap();
            change(changed);
            let error = condition(locked.opened());
            assert!(
                matches!(&error, MetadataError::DatabaseReplaced { path } if path == changed),
                "{error}"
            );
            drop(TursoDb::open(&path).unwrap());
        }
    }

    /// The engine replaces a WAL it finds next to an empty database file,
    /// left by a removed database. That is not a replacement to refuse.
    #[cfg(unix)]
    #[tokio::test]
    async fn the_log_of_a_removed_database_is_discarded_not_refused() {
        use std::os::unix::fs::MetadataExt as _;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("casita.sqlite");
        let log = directory.path().join("casita.sqlite-wal");
        let db = TursoDb::open(&path).unwrap();
        let frames = std::fs::read(&log).unwrap();
        // More than the 32-byte header: the schema is still in the log.
        assert!(frames.len() > 32, "{} bytes", frames.len());
        drop(db);
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&log, &frames).unwrap();
        let orphan = std::fs::metadata(&log).unwrap().ino();

        let db = TursoDb::open(&path).unwrap();
        assert_ne!(std::fs::metadata(&log).unwrap().ino(), orphan);
        db.write(|_| Box::pin(async { Ok(()) })).await.unwrap();
    }

    /// Deletions fail with I/O errors inside payload errors; the repository
    /// still classifies them by the condition underneath.
    #[test]
    fn conditions_keep_their_meaning_through_payload_deletions() {
        let cases = [
            (
                MetadataError::ForeignSqliteLock {
                    path: "casita.sqlite".into(),
                },
                RepositoryErrorCategory::Busy,
                RetryDisposition::Retry,
            ),
            (
                MetadataError::DatabaseReplaced {
                    path: "casita.sqlite-wal".into(),
                },
                RepositoryErrorCategory::RestartRequired,
                RetryDisposition::Never,
            ),
        ];
        for (condition, category, retry) in cases {
            let message = condition.to_string();
            let deletion = io::Error::from(DurabilityError::Condition(condition));
            let error = RepositoryError::Payload(crate::error::Error::Io(deletion));
            assert_eq!(error.category(), category, "{message}");
            assert_eq!(error.retry_disposition(), retry, "{message}");
            assert_eq!(error.to_string(), message);
        }
    }

    /// A SQLite client holding the locks the fence needs, from its own open
    /// file description: an exclusive-mode client on the database file, or
    /// one in the middle of writing on the WAL index.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_client_holding_a_conflicting_lock_is_a_typed_retryable_condition() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("casita.sqlite");
        drop(TursoDb::open(&path).unwrap());
        let paths = Paths::of(&path);
        for (locked, range) in [
            (&paths.database, EXCLUSIVE),
            (&paths.log_index, LOG_INDEX_LOCKS),
        ] {
            let client = lock(locked, libc::F_WRLCK, range);
            assert_busy(&path, locked);
            drop(client);
            drop(TursoDb::open(&path).unwrap());
        }
    }

    /// The locks come before the engine opens or initializes anything.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_engine_touches_nothing_while_a_client_holds_the_database() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("casita.sqlite");
        let paths = Paths::of(&path);
        std::fs::write(&path, b"").unwrap();
        let client = lock(&path, libc::F_WRLCK, EXCLUSIVE);
        assert_busy(&path, &path);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        assert!(!paths.log.exists());
        drop(client);
        drop(TursoDb::open(&path).unwrap());
        assert!(paths.log.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn handles_share_the_locks_and_keep_them_when_another_closes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("casita.sqlite");
        let first = TursoDb::open(&path).unwrap();
        let second = TursoDb::open(&path).unwrap();
        assert!(Arc::ptr_eq(&first.files().fence, &second.files().fence));
        first.files().before_change().unwrap();
        second.files().before_change().unwrap();
        drop(second);
        #[cfg(target_os = "linux")]
        assert_locked(&path);
        // A descriptor closed elsewhere in this process does not release the
        // first handle's locks for good: its next check holds them again.
        drop(std::fs::File::open(&path).unwrap());
        drop(std::fs::File::open(Paths::of(&path).log_index).unwrap());
        first.files().before_change().unwrap();
        #[cfg(target_os = "linux")]
        assert_locked(&path);
    }

    /// A client attached before casita opens closes while the locks are being
    /// taken, right after the WAL index is opened. Its close must not unlink
    /// the index casita is about to lock: clients would attach to a new one.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_client_closing_while_the_locks_are_taken_keeps_the_wal_index() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("casita.sqlite");
        let paths = Paths::of(&path);
        drop(TursoDb::open(&path).unwrap());
        let client = rusqlite::Connection::open(&path).unwrap();
        client
            .query_row("SELECT count(*) FROM sqlite_schema", (), |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        let index = FileIdentity::of(&paths.log_index).unwrap().unwrap();
        let log = FileIdentity::of(&paths.log).unwrap().unwrap();

        AFTER_OPENING_LOG_INDEX.set(Some(Box::new(move || drop(client))));
        let db = TursoDb::open(&path).unwrap();
        assert!(
            AFTER_OPENING_LOG_INDEX.take().is_none(),
            "the client did not close"
        );
        assert_eq!(FileIdentity::of(&paths.log_index).unwrap(), Some(index));
        assert_eq!(FileIdentity::of(&paths.log).unwrap(), Some(log));
        match &*db.files().fence.log_index.lock().unwrap() {
            LogIndex::Locked(file) => assert_eq!(FileIdentity::of_file(file).unwrap(), index),
            other => panic!("the WAL index is not locked: {other:?}"),
        }
        assert_locked(&path);
    }

    /// The index at its path is what a client attaches to, so a removed one
    /// is replaced and locked at the next change.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_next_change_locks_a_replacement_for_a_removed_wal_index() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("casita.sqlite");
        let paths = Paths::of(&path);
        let db = TursoDb::open(&path).unwrap();
        std::fs::remove_file(&paths.log_index).unwrap();
        db.files().before_change().unwrap();
        assert!(paths.log_index.exists());
        assert_locked(&path);
    }

    /// Without a WAL index to lock, a client must not get to create one, not
    /// even once there is space for it, until the process has locked it.
    /// The client runs in this process: its POSIX locks still conflict with
    /// the fence's open-file-description locks.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_wal_index_that_cannot_be_created_keeps_clients_out_until_it_is_locked() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("casita.sqlite");
        let paths = Paths::of(&path);
        let Some(full) = Full::of(directory.path(), &paths) else {
            return;
        };
        let db = TursoDb::open(&path).unwrap();
        assert_client_refused(&path);
        assert!(!paths.log_index.exists());

        drop(full);
        assert_client_refused(&path);
        assert!(!paths.log_index.exists());
        let shared = || lock_possible(&path, libc::F_RDLCK, SHARED_RANGE);
        assert!(!shared(), "a client could take a shared lock");
        // Other casita processes are not kept out.
        assert!(lock_possible(&path, libc::F_RDLCK, DATABASE_LOCKS));

        db.write(|_| Box::pin(async { Ok(()) })).await.unwrap();
        assert!(paths.log_index.exists());
        assert!(shared(), "the claim outlived the WAL index");
        // From here clients fail as with any repository, which
        // `tests/foreign_sqlite_client.rs` covers; SQLite retries a locked
        // WAL index for seconds before it gives up.
        assert_locked(&path);
    }

    /// Each process without a WAL index claims a byte no other holds; none
    /// is left while a client holds a shared lock.
    #[cfg(target_os = "linux")]
    #[test]
    fn claims_avoid_each_other_and_yield_to_clients() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("casita.sqlite");
        let paths = Paths::of(&path);
        let Some(_full) = Full::of(directory.path(), &paths) else {
            return;
        };
        let (first, count) = CLAIMABLE;
        let last = (first + count - 1, 1);
        let others = lock(&path, libc::F_WRLCK, (first, count - 1));
        let db = TursoDb::open(&path).unwrap();
        assert!(!lock_possible(&path, libc::F_WRLCK, last));
        drop(db);
        assert!(lock_possible(&path, libc::F_WRLCK, last));

        let all = lock(&path, libc::F_WRLCK, last);
        assert_busy(&path, &paths.database);
        drop((others, all));
        let client = lock(&path, libc::F_RDLCK, SHARED_RANGE);
        assert_busy(&path, &paths.database);
        drop(client);
        drop(TursoDb::open(&path).unwrap());
    }

    /// What an exclusive lock writes.
    #[cfg(target_os = "linux")]
    const EXCLUSIVE: (u64, u64) = (PENDING_BYTE, 2 + SHARED_RANGE.1);

    /// A repository without a WAL index on a filesystem where none can be
    /// created, until dropped; `None` where permissions do not stop this
    /// process creating files, as for root.
    #[cfg(target_os = "linux")]
    struct Full<'a>(&'a Path);

    #[cfg(target_os = "linux")]
    impl<'a> Full<'a> {
        fn of(directory: &'a Path, paths: &Paths) -> Option<Self> {
            use std::os::unix::fs::PermissionsExt as _;

            drop(TursoDb::open(&paths.database).unwrap());
            std::fs::remove_file(&paths.log_index).unwrap();
            std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o555)).unwrap();
            let full = Self(directory);
            std::fs::File::create(directory.join("probe"))
                .is_err()
                .then_some(full)
        }
    }

    #[cfg(target_os = "linux")]
    impl Drop for Full<'_> {
        fn drop(&mut self) {
            use std::os::unix::fs::PermissionsExt as _;

            std::fs::set_permissions(self.0, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    /// Opening `database` fails as busy on the lock a client holds on
    /// `locked`.
    #[cfg(target_os = "linux")]
    fn assert_busy(database: &Path, locked: &Path) {
        let error = TursoDb::open(database).unwrap_err();
        assert_eq!(error.retry_disposition(), RetryDisposition::Retry);
        let error = engine_condition::<()>(Err(error));
        assert!(
            matches!(&error, MetadataError::ForeignSqliteLock { path } if path == locked),
            "{error}"
        );
    }

    /// A SQLite client can neither read nor write the database.
    #[cfg(target_os = "linux")]
    fn assert_client_refused(path: &Path) {
        let client = rusqlite::Connection::open(path).unwrap();
        client.busy_timeout(std::time::Duration::ZERO).unwrap();
        let read = client.query_row("SELECT count(*) FROM sqlite_schema", (), |row| {
            row.get::<_, i64>(0)
        });
        assert!(read.is_err(), "a client read the database: {read:?}");
        let write = client.execute_batch("CREATE TABLE client(value)");
        assert!(write.is_err(), "a client wrote the database");
    }

    /// No other lock owner could take what a SQLite client needs. Only
    /// Linux checks: process-scoped lock queries do not report this
    /// process's own locks.
    #[cfg(target_os = "linux")]
    fn assert_locked(path: &Path) {
        assert!(!lock_possible(path, libc::F_WRLCK, EXCLUSIVE));
        assert!(!lock_possible(
            &Paths::of(path).log_index,
            libc::F_WRLCK,
            LOG_INDEX_LOCKS
        ));
    }

    /// A lock another lock owner holds until dropped.
    #[cfg(target_os = "linux")]
    #[track_caller]
    fn lock(path: &Path, kind: libc::c_int, range: (u64, u64)) -> Held {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let lock = byte_range(kind, range).unwrap();
        nix::fcntl::fcntl(&file, nix::fcntl::FcntlArg::F_OFD_SETLK(&lock)).unwrap();
        Held(file)
    }

    /// A lock that ends when dropped, even while a child process another
    /// test is spawning still shares its open file description.
    #[cfg(target_os = "linux")]
    struct Held(std::fs::File);

    #[cfg(target_os = "linux")]
    impl Drop for Held {
        fn drop(&mut self) {
            assert!(set_lock(&self.0, libc::F_UNLCK, (0, 0)).unwrap());
        }
    }

    /// Whether another lock owner could take a lock of `kind` on `range`.
    #[cfg(target_os = "linux")]
    fn lock_possible(path: &Path, kind: libc::c_int, range: (u64, u64)) -> bool {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let mut lock = byte_range(kind, range).unwrap();
        nix::fcntl::fcntl(&file, nix::fcntl::FcntlArg::F_OFD_GETLK(&mut lock)).unwrap();
        i32::from(lock.l_type) == libc::F_UNLCK
    }
}
