//! Orders payload deletions after the metadata commits that allow them.
//!
//! On Apple platforms the state engine syncs its WAL with a plain `fsync`,
//! which leaves committed frames in the drive's volatile cache for the drive
//! to write back in any order. Collection and catalog maintenance delete
//! payloads that a commit made unreachable, so a power loss could keep such a
//! deletion while losing the commit behind it, and the repository would
//! reopen referencing payloads that no longer exist. Commits keep their plain
//! sync; a deletion first makes them durable instead.
//!
//! A commit is equally lost when the WAL it went to is unlinked, as an
//! ordinary SQLite client does on close (see `sqlite::DatabaseFiles`). So
//! before every deletion batch, on every platform, a local database also
//! holds the locks that keep such clients out and checks its files.

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

/// How to make a metadata store's acknowledged commits durable: flush the
/// drive cache holding its database (`F_FULLFSYNC`, which `File::sync_all`
/// issues on Apple platforms) before each deletion batch, and check that a
/// new process would read the files the commits went to. A WAL writer can
/// finish its commit sync without changing file size or mtime, so file stamps
/// cannot prove that a previous flush covered an acknowledged commit.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct CommitDurability(Arc<Database>);

#[derive(Debug)]
struct Database {
    directory: PathBuf,
    /// Whether a commit's sync can stop short of stable storage: Apple
    /// platforms. Unix test builds flush everywhere so every CI platform
    /// exercises the ordering.
    flush: bool,
    files: Option<Arc<crate::sqlite::DatabaseFiles>>,
    #[cfg(test)]
    flushes: std::sync::atomic::AtomicUsize,
}

impl CommitDurability {
    /// For the local Turso database `db` commits through.
    pub(crate) fn for_turso(db: &crate::sqlite::TursoDb) -> Self {
        let mut database = Database::in_directory_of(db.path());
        database.flush = cfg!(any(target_vendor = "apple", all(test, unix)));
        database.files = Some(db.files().clone());
        Self(Arc::new(database))
    }

    /// Flush the drive cache holding the database file at `database` before
    /// every deletion batch, with no further checks.
    #[cfg(all(test, unix))]
    pub(crate) fn new(database: &Path) -> Self {
        Self(Arc::new(Database::in_directory_of(database)))
    }

    /// Make every commit already written to the database durable.
    async fn ensure(&self) -> io::Result<()> {
        let database = self.0.clone();
        tokio::task::spawn_blocking(move || database.ensure())
            .await
            .map_err(io::Error::other)?
    }

    #[cfg(all(test, unix))]
    pub(crate) fn flushes(&self) -> usize {
        self.0.flushes.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Database {
    fn in_directory_of(database: &Path) -> Self {
        let directory = match database.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
            _ => PathBuf::from("."),
        };
        Self {
            directory,
            flush: true,
            files: None,
            #[cfg(test)]
            flushes: Default::default(),
        }
    }

    fn ensure(&self) -> io::Result<()> {
        // A deletion is only as durable as the commit that allowed it: refuse
        // while that commit may have gone to an unlinked WAL, or while a SQLite
        // client could roll it back on close.
        if let Some(files) = &self.files {
            files.before_change()?;
        }
        if !self.flush {
            return Ok(());
        }
        // Opening the directory, not the database, leaves the engine's
        // process-scoped file locks alone: closing any descriptor of a locked
        // file would release them.
        File::open(&self.directory)?.sync_all()?;
        tracing::debug!("flushed committed state before deleting payloads");
        #[cfg(test)]
        self.flushes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

/// Where a payload store's deletions wait for the commits that allow them.
/// Clones share one set, so pairing a metadata store after the store handed
/// clones to its deleting components still reaches all of them.
#[derive(Clone, Debug, Default)]
pub(crate) struct DeletionBarrier(Arc<Mutex<Vec<CommitDurability>>>);

impl DeletionBarrier {
    pub(crate) fn order_after(&self, commits: CommitDurability) {
        let mut all = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if !all.iter().any(|known| Arc::ptr_eq(&known.0, &commits.0)) {
            all.push(commits);
        }
    }

    /// Make the commits a deletion may depend on durable before it runs.
    pub(crate) async fn before_deletion(&self) -> io::Result<()> {
        let all = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        for commits in all {
            commits.ensure().await?;
        }
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    async fn commit(db: &Arc<crate::sqlite::TursoDb>) {
        db.write(|connection| {
            Box::pin(async move {
                connection
                    .execute_batch("CREATE TABLE IF NOT EXISTS t (v); INSERT INTO t VALUES (1);")
                    .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flushes_before_each_deletion_batch() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("casita.sqlite");
        let db = crate::sqlite::TursoDb::open(&path).unwrap();
        let commits = CommitDurability::new(&path);
        let barrier = DeletionBarrier::default();
        barrier.order_after(commits.clone());

        barrier.before_deletion().await.unwrap();
        barrier.before_deletion().await.unwrap();
        assert_eq!(commits.flushes(), 2, "each batch requires a flush");
        commit(&db).await;
        barrier.before_deletion().await.unwrap();
        assert_eq!(commits.flushes(), 3, "a commit since the last flush");
        // A second handle stands in for another process's commit.
        commit(&crate::sqlite::TursoDb::open(&path).unwrap()).await;
        barrier.before_deletion().await.unwrap();
        assert_eq!(commits.flushes(), 4, "another writer's commit");
    }

    #[tokio::test]
    async fn deletions_wait_for_every_paired_metadata_store() {
        let directory = tempfile::tempdir().unwrap();
        let [first, second] = ["first.sqlite", "second.sqlite"].map(|name| {
            let path = directory.path().join(name);
            std::fs::write(&path, b"committed").unwrap();
            CommitDurability::new(&path)
        });
        let barrier = DeletionBarrier::default();
        barrier.order_after(first.clone());
        barrier.order_after(second.clone());
        barrier.order_after(first.clone());
        barrier.before_deletion().await.unwrap();
        assert_eq!((first.flushes(), second.flushes()), (1, 1));
    }

    #[tokio::test]
    async fn wal_sync_after_flush_does_not_make_the_next_flush_optional() {
        use std::io::Write;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("casita.sqlite");
        let mut wal = File::create(directory.path().join("casita.sqlite-wal")).unwrap();
        wal.write_all(b"pending commit").unwrap();
        let before = wal.metadata().unwrap();
        let commits = CommitDurability::new(&path);
        let barrier = DeletionBarrier::default();
        barrier.order_after(commits.clone());

        // A concurrent writer may have written its last WAL frame but not
        // finished syncing it when the deletion barrier runs.
        barrier.before_deletion().await.unwrap();
        wal.sync_all().unwrap();
        let after = wal.metadata().unwrap();
        assert_eq!(before.len(), after.len());
        assert_eq!(before.modified().unwrap(), after.modified().unwrap());
        barrier.before_deletion().await.unwrap();
        assert_eq!(commits.flushes(), 2);
    }
}
