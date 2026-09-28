//! The shared Turso connection layer behind persistent repository state.
//!
//! Turso operates directly on the repository's SQLite-format database.
//! [`TursoMetadataStore`](crate::TursoMetadataStore) speaks SQL through [`TursoDb`]
//! and shares pooled connections across snapshots.
//!
//! The SQL above this module deliberately stays in the conservative SQLite
//! subset: plain tables, `INSERT OR REPLACE`, parameterized reads/deletes,
//! bytewise `BLOB` keys, and binary-collated `TEXT`. The on-disk contract is
//! `<repository>/casita.sqlite`.
//!
//! Turso's experimental multi-process WAL is enabled when the database is
//! built, on every supported platform. MVCC is intentionally not enabled:
//! casita serializes writes on one connection in-process, while the WAL
//! coordinates separate processes and keeps readers moving during a write.
//!
//! Database transactions protect logical state; durable repository pins retain
//! data used by mutations and stable reads during physical collection.

use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use futures::future::BoxFuture;
use turso::{Builder, Connection, Database};

use crate::error::Error;

/// Accumulated lock-wait budget.
pub(crate) const BUSY_TIMEOUT_MS: u64 = 30_000;
/// Bound idle connections, not live snapshots: readers never wait for a slot.
const IDLE_READ_CONNECTIONS: usize = 8;

/// A snapshot has exclusive use of this connection. End its transaction before
/// returning it to the pool so an idle connection cannot hold an old WAL view.
pub(crate) struct ReadConnection {
    connection: Option<Connection>,
    idle: Arc<Mutex<Vec<Connection>>>,
}

impl Deref for ReadConnection {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        self.connection.as_ref().expect("live read connection")
    }
}

impl Drop for ReadConnection {
    fn drop(&mut self) {
        let Some(connection) = self.connection.take() else {
            return;
        };
        // Local read rollback normally completes on its first poll. Never run a
        // nested executor or leave a transaction waiting in the idle pool: if
        // cleanup needs asynchronous work or fails, discard the connection just
        // as an unpooled snapshot would. This also works outside any runtime.
        if !matches!(
            connection.execute_batch("ROLLBACK;").now_or_never(),
            Some(Ok(()))
        ) || !matches!(connection.is_autocommit(), Ok(true))
        {
            return;
        }
        if let Ok(mut idle) = self.idle.lock()
            && idle.len() < IDLE_READ_CONNECTIONS
        {
            idle.push(connection);
        }
    }
}

/// Generic logical schema at `<repository>/casita.sqlite`; payload bytes live
/// separately under `blobs/`.
const REPOSITORY_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS repository_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    revision  BLOB NOT NULL,
    generation INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0),
    payload_catalog_ref BLOB
);
CREATE TABLE IF NOT EXISTS objects (
    namespace    TEXT NOT NULL,
    native_id    BLOB NOT NULL,
    payload      BLOB NOT NULL,
    payload_size BLOB NOT NULL,
    record        BLOB NOT NULL,
    created_generation INTEGER NOT NULL CHECK (created_generation >= 0),
    validated    INTEGER NOT NULL DEFAULT 0 CHECK (validated IN (0, 1)),
    PRIMARY KEY (namespace, native_id)
);
CREATE TABLE IF NOT EXISTS named_roots (
    name      TEXT PRIMARY KEY,
    namespace TEXT NOT NULL,
    native_id BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS named_roots_target ON named_roots(namespace, native_id);
CREATE TABLE IF NOT EXISTS metadata_records (
    namespace TEXT NOT NULL,
    key BLOB NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (namespace, key)
);
CREATE TABLE IF NOT EXISTS verification_facts (
    identity BLOB PRIMARY KEY,
    facts    BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS ingest_cache (
    device      INTEGER NOT NULL,
    inode       INTEGER NOT NULL,
    size        INTEGER NOT NULL,
    mtime_sec   INTEGER NOT NULL,
    mtime_nsec  INTEGER NOT NULL,
    ctime_sec   INTEGER NOT NULL,
    ctime_nsec  INTEGER NOT NULL,
    blob_digest BLOB NOT NULL,
    PRIMARY KEY (device, inode)
);
";

/// The current pre-release generic repository schema's `PRAGMA user_version`.
///
/// Earlier development schemas expanded every object link into its own SQL
/// row. They are deliberately not migration inputs: repositories can be
/// recreated while Casita remains unreleased.
/// Only the current schema is supported; older development schemas must be
/// recreated or re-imported.
const REPOSITORY_USER_VERSION: i64 = 6;

/// A Turso database shared by persistent repository-state components.
///
/// One connection is reserved for writes and guarded by an async mutex so a
/// process issues at most one write transaction at a time. Reads borrow a
/// small pool of independent Turso connections and may proceed concurrently.
pub struct TursoDb {
    path: PathBuf,
    database: Database,
    writer: Arc<tokio::sync::Mutex<Connection>>,
    idle_readers: Arc<Mutex<Vec<Connection>>>,
}

impl TursoDb {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
    /// Open or create the SQLite-format database at `path`, enable Turso's
    /// multi-process WAL, and apply the repository schema.
    ///
    /// Turso's asynchronous open and schema setup are driven to completion
    /// here; query execution remains fully async.
    ///
    /// `path` must be valid UTF-8 because Turso's builder takes a `&str`.
    ///
    /// This drives async setup to completion with a blocking executor, so an
    /// async caller should run it under [`spawn_blocking`](tokio::task::spawn_blocking)
    /// rather than on a runtime worker.
    #[tracing::instrument(name = "state.turso.open", skip_all)]
    pub fn open(path: impl AsRef<Path>) -> Result<Arc<Self>, Error> {
        let path = path.as_ref().to_path_buf();
        let path_str = path.to_str().ok_or_else(|| {
            Error::from(format!(
                "Turso database path is not valid UTF-8: {}",
                path.display()
            ))
        })?;

        let (database, writer) = futures::executor::block_on(async {
            let builder = Builder::new_local(path_str);
            // Windows coordinates the shared WAL through the IOCP backend:
            // the default syscall backend does not implement the byte-range
            // locking and shutdown checkpointing a second process needs.
            // `tests/turso_multiprocess.rs` is the regression test.
            #[cfg(windows)]
            let builder = builder.with_io("experimental_win_iocp");
            let database = builder.experimental_multiprocess_wal(true).build().await?;
            let mut writer = database.connect()?;
            // Acquire the WAL write lock before reading a revision. A deferred
            // read-to-write upgrade can fail BUSY_SNAPSHOT under foreign writers.
            writer.set_transaction_behavior(turso::transaction::TransactionBehavior::Immediate);
            configure_connection(&writer)?;

            // Opening an already-current repository is a read-only operation.
            // Besides avoiding needless WAL traffic in the common case, this
            // is what lets an existing repository open after its filesystem
            // has reached ENOSPC so collection can reclaim space.
            let found = query_i64(&writer, "PRAGMA user_version").await?;
            if found != REPOSITORY_USER_VERSION {
                initialize_schema(&writer, found, &path).await?;
            }
            // `synchronous` is connection-local and changes no persistent
            // page, so configuring it does not consume filesystem space.
            writer.execute_batch("PRAGMA synchronous = FULL;").await?;
            Ok::<_, Error>((database, writer))
        })?;

        Ok(Arc::new(Self {
            path,
            database,
            writer: Arc::new(tokio::sync::Mutex::new(writer)),
            idle_readers: Arc::new(Mutex::new(Vec::new())),
        }))
    }

    /// Run one operation on the process's serialized write connection.
    ///
    /// The operation runs to completion on the blocking pool via
    /// [`spawn_blocking`](tokio::task::spawn_blocking). This keeps two
    /// properties the previous rusqlite engine had for free:
    ///
    /// - **off the executor**: Turso's local backend performs its file I/O
    ///   (including the commit fsync) synchronously inside `poll`, so running
    ///   it on a runtime worker would block every other task on a
    ///   current-thread runtime; the blocking pool absorbs it instead.
    /// - **cancellation-safe**: a `spawn_blocking` task runs to completion even
    ///   if the caller drops the returned future, so a cancelled write cannot
    ///   abandon a half-applied transaction on the shared writer connection and
    ///   strand it (Turso only rolls a dropped transaction back lazily, on the
    ///   connection's next use).
    pub(crate) async fn write<T, F>(self: &Arc<Self>, f: F) -> Result<T, Error>
    where
        F: for<'a> FnOnce(&'a mut Connection) -> BoxFuture<'a, Result<T, Error>> + Send + 'static,
        T: Send + 'static,
    {
        // The owned guard keeps the one writer connection (and its lazy
        // dangling-transaction state) across calls; the blocking task holds it
        // until the write finishes, so the next write waits rather than racing.
        let mut guard = self.writer.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || futures::executor::block_on(f(&mut guard))).await?
    }

    /// Replace the serialized writer after a failed transaction.
    ///
    /// Turso rolls abandoned transactions back lazily when their connection is
    /// reused. An ENOSPC during commit can leave that cleanup path unable to
    /// distinguish an already-aborted transaction, so emergency collection
    /// retries on a fresh connection instead.
    pub(crate) async fn reset_writer(&self) -> Result<(), Error> {
        let mut writer = self.database.connect()?;
        writer.set_transaction_behavior(turso::transaction::TransactionBehavior::Immediate);
        configure_connection(&writer)?;
        writer.execute_batch("PRAGMA synchronous = FULL;").await?;
        *self.writer.lock().await = writer;
        Ok(())
    }

    /// Read on one query-only transaction without loading a metadata snapshot.
    /// Used for one-shot application-record reads: acquisition, state validation,
    /// queries, and rollback run in one blocking worker to avoid worker hops.
    /// Use `begin_read_snapshot` when the caller must retain the same view across
    /// multiple operations.
    /// The worker owns rollback even when the caller cancels. Only a finished
    /// transaction can return to the shared idle connection pool.
    pub(crate) async fn read<T, F>(self: &Arc<Self>, f: F) -> Result<T, Error>
    where
        F: for<'a> FnOnce(&'a Connection) -> BoxFuture<'a, Result<T, Error>> + Send + 'static,
        T: Send + 'static,
    {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            futures::executor::block_on(async move {
                let mut reader = this.begin_read_transaction().await?;
                let result = f(&reader).await;
                // Take ownership so a failed rollback discards the connection.
                let connection = reader.connection.take().expect("live read connection");
                connection.execute_batch("ROLLBACK;").await?;
                if connection.is_autocommit()?
                    && let Ok(mut idle) = this.idle_readers.lock()
                    && idle.len() < IDLE_READ_CONNECTIONS
                {
                    idle.push(connection);
                }
                result
            })
        })
        .await?
    }

    async fn begin_read_transaction(self: &Arc<Self>) -> Result<ReadConnection, Error> {
        let cached = self
            .idle_readers
            .lock()
            .ok()
            .and_then(|mut idle| idle.pop());
        let connection = match cached {
            Some(connection) => connection,
            None => {
                let connection = self.database.connect()?;
                configure_reader(&connection).await?;
                connection
            }
        };
        // Own cleanup before BEGIN: a failed/cancelled acquisition
        // cannot return a connection with an unfinished transaction.
        let connection = ReadConnection {
            connection: Some(connection),
            idle: self.idle_readers.clone(),
        };
        connection
            .execute_batch("BEGIN DEFERRED TRANSACTION;")
            .await?;
        Ok(connection)
    }

    /// Borrow an exclusive query-only connection and begin a fresh transaction.
    /// Used by retained metadata snapshots for repeated lookups and stable scan
    /// pagination. The caller owns the transaction until the connection is dropped;
    /// use `read` for a one-shot operation that can finish in one worker.
    /// The first state query establishes its WAL snapshot; retaining the
    /// returned connection keeps every later query on that same immutable
    /// logical view.
    pub(crate) async fn begin_read_snapshot(self: &Arc<Self>) -> Result<ReadConnection, Error> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            futures::executor::block_on(async move { this.begin_read_transaction().await })
        })
        .await?
    }
}

impl std::fmt::Debug for TursoDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TursoDb").field("path", &self.path).finish()
    }
}

async fn query_i64(conn: &Connection, sql: &str) -> Result<i64, Error> {
    let mut rows = conn.query(sql, ()).await?;
    Ok(rows
        .next()
        .await?
        .ok_or_else(|| format!("{sql} returned no row"))?
        .get(0)?)
}

/// Create the repository schema in a database that has none, or reject it.
///
/// Whether to initialize depends on the database contents, not on whether the
/// file existed: a crash after Turso created the file (possibly empty, possibly
/// with a WAL) but before the schema committed must not strand the repository.
/// An unversioned database is ours to initialize only while it holds no schema
/// object at all; any foreign table means it is not a casita database.
/// The decision is made again under the WAL write lock, so openers that bypass
/// the repository lease still cannot both initialize or race a foreign writer.
async fn initialize_schema(writer: &Connection, found: i64, path: &Path) -> Result<(), Error> {
    let unsupported = |found: i64| {
        Error::from(format!(
            "database {} has unsupported schema version {found}; this casita release \
             supports only version {REPOSITORY_USER_VERSION} and does not migrate \
             pre-release repositories; recreate or re-import it",
            path.display()
        ))
    };
    if found != 0 {
        return Err(unsupported(found));
    }
    writer.execute_batch("BEGIN IMMEDIATE;").await?;
    let decided = async {
        let found = query_i64(writer, "PRAGMA user_version").await?;
        if found == REPOSITORY_USER_VERSION {
            // Another opener initialized it while this one waited.
            return Ok(false);
        }
        let objects = query_i64(writer, "SELECT count(*) FROM sqlite_schema").await?;
        if found != 0 || objects != 0 {
            return Err(unsupported(found));
        }
        Ok(true)
    }
    .await;
    match decided {
        Ok(true) => {
            writer
                .execute_batch(format!(
                    "{REPOSITORY_SCHEMA}\n\
                     PRAGMA user_version = {REPOSITORY_USER_VERSION};\nCOMMIT;"
                ))
                .await?;
            Ok(())
        }
        Ok(false) => Ok(writer.execute_batch("ROLLBACK;").await?),
        Err(error) => {
            // Report why the database was refused, not a cleanup failure; the
            // caller drops this connection with the failed open anyway.
            let _ = writer.execute_batch("ROLLBACK;").await;
            Err(error)
        }
    }
}

fn configure_connection(conn: &Connection) -> Result<(), Error> {
    conn.busy_timeout(std::time::Duration::from_millis(BUSY_TIMEOUT_MS))?;
    Ok(())
}

/// Configure a read connection. Beyond the busy timeout, reads run under
/// `PRAGMA query_only` so a mistaken write on a read path fails at the engine
/// level instead of mutating repository state or taking the WAL write lock.
async fn configure_reader(conn: &Connection) -> Result<(), Error> {
    configure_connection(conn)?;
    conn.execute_batch("PRAGMA query_only = true;").await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn read_number(db: &Arc<TursoDb>) -> i64 {
        db.read(|connection| {
            Box::pin(async move {
                let mut rows = connection
                    .query("SELECT value FROM read_pool_test", ())
                    .await?;
                Ok(rows.next().await?.unwrap().get(0)?)
            })
        })
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn short_reads_reuse_only_finished_transactions_and_see_foreign_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pool.sqlite");
        let db = TursoDb::open(&path).unwrap();
        db.write(|connection| Box::pin(async move {
            connection.execute_batch("CREATE TABLE read_pool_test(value INTEGER); INSERT INTO read_pool_test VALUES(1);").await?;
            Ok(())
        })).await.unwrap();
        let retained = db.begin_read_snapshot().await.unwrap();
        let mut rows = retained
            .query("SELECT value FROM read_pool_test", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            1
        );
        drop(rows);
        assert_eq!(read_number(&db).await, 1);
        let other = TursoDb::open(&path).unwrap();
        other
            .write(|connection| {
                Box::pin(async move {
                    connection
                        .execute_batch("UPDATE read_pool_test SET value = 2")
                        .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        assert_eq!(read_number(&db).await, 2);
        let writer = other.clone();
        db.read(move |connection| {
            Box::pin(async move {
                let mut rows = connection
                    .query("SELECT value FROM read_pool_test", ())
                    .await?;
                assert_eq!(rows.next().await?.unwrap().get::<i64>(0)?, 2);
                drop(rows);
                writer
                    .write(|connection| {
                        Box::pin(async move {
                            connection
                                .execute_batch("UPDATE read_pool_test SET value = 4")
                                .await?;
                            Ok(())
                        })
                    })
                    .await?;
                let mut rows = connection
                    .query("SELECT value FROM read_pool_test", ())
                    .await?;
                assert_eq!(rows.next().await?.unwrap().get::<i64>(0)?, 2);
                Ok(())
            })
        })
        .await
        .unwrap();
        assert_eq!(read_number(&db).await, 4);
        let mut rows = retained
            .query("SELECT value FROM read_pool_test", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            1
        );
        assert!(
            db.read(|connection| Box::pin(async move {
                connection
                    .execute_batch("UPDATE read_pool_test SET value = 3")
                    .await?;
                Ok(())
            }))
            .await
            .is_err()
        );
        assert_eq!(read_number(&db).await, 4);
        let pool = db.idle_readers.lock().unwrap();
        assert_eq!(pool.len(), 1);
        assert!(
            pool.iter()
                .all(|connection| connection.is_autocommit().unwrap())
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn short_read_pool_is_bounded_on_both_sides_of_its_idle_limit() {
        for width in [1, IDLE_READ_CONNECTIONS, IDLE_READ_CONNECTIONS + 1] {
            let dir = tempfile::tempdir().unwrap();
            let db = TursoDb::open(dir.path().join("pool.sqlite")).unwrap();
            let barrier = Arc::new(tokio::sync::Barrier::new(width));
            let results = futures::future::join_all((0..width).map(|_| {
                let barrier = barrier.clone();
                db.read(move |connection| {
                    Box::pin(async move {
                        let mut rows = connection.query("PRAGMA query_only", ()).await?;
                        let value: i64 = rows.next().await?.unwrap().get(0)?;
                        drop(rows);
                        barrier.wait().await;
                        Ok(value)
                    })
                })
            }))
            .await;
            assert!(results.into_iter().all(|result| result.unwrap() == 1));
            let pool = db.idle_readers.lock().unwrap();
            assert_eq!(pool.len(), width.min(IDLE_READ_CONNECTIONS));
            assert!(
                pool.iter()
                    .all(|connection| connection.is_autocommit().unwrap())
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelling_a_short_read_keeps_cleanup_owned_by_its_worker() {
        let dir = tempfile::tempdir().unwrap();
        let db = TursoDb::open(dir.path().join("pool.sqlite")).unwrap();
        let (entered, reached) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        let task_db = db.clone();
        let task = tokio::spawn(async move {
            task_db
                .read(move |connection| {
                    Box::pin(async move {
                        let mut rows = connection.query("SELECT 1", ()).await?;
                        assert_eq!(rows.next().await?.unwrap().get::<i64>(0)?, 1);
                        drop(rows);
                        entered.send(()).unwrap();
                        resumed.await.unwrap();
                        Ok(())
                    })
                })
                .await
        });
        reached.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(db.idle_readers.lock().unwrap().is_empty());
        resume.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Some(connection) = db.idle_readers.lock().unwrap().first() {
                    assert!(connection.is_autocommit().unwrap());
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    async fn snapshot_generation(connection: &Connection) -> i64 {
        connection
            .query("SELECT generation FROM repository_state", ())
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap()
            .get(0)
            .unwrap()
    }

    #[tokio::test]
    async fn reused_readers_are_fresh_query_only_and_do_not_retain_transactions() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("casita.sqlite");
        let database = TursoDb::open(&path).unwrap();
        database.writer.lock().await.execute_batch(
            "INSERT INTO repository_state(singleton, revision, generation) VALUES (1, X'01', 1);"
        ).await.unwrap();
        let old = database.begin_read_snapshot().await.unwrap();
        assert_eq!(snapshot_generation(&old).await, 1);
        let other = TursoDb::open(&path).unwrap();
        other
            .writer
            .lock()
            .await
            .execute_batch("UPDATE repository_state SET generation = 2;")
            .await
            .unwrap();
        let fresh = database.begin_read_snapshot().await.unwrap();
        assert_eq!(snapshot_generation(&fresh).await, 2);
        assert_eq!(snapshot_generation(&old).await, 1);
        // Drop inside a futures executor, then on a plain thread with no Tokio
        // runtime. Neither cleanup path may depend on an ambient executor.
        futures::executor::block_on(async {
            drop(fresh);
        });
        std::thread::spawn(move || drop(old)).join().unwrap();
        {
            let idle = database.idle_readers.lock().unwrap();
            assert_eq!(idle.len(), 2);
            assert!(
                idle.iter()
                    .all(|connection| connection.is_autocommit().unwrap())
            );
        }
        other
            .writer
            .lock()
            .await
            .execute_batch("UPDATE repository_state SET generation = 3;")
            .await
            .unwrap();
        let reused = database.begin_read_snapshot().await.unwrap();
        assert_eq!(snapshot_generation(&reused).await, 3);
        assert!(
            reused
                .execute_batch("DELETE FROM repository_state;")
                .await
                .is_err()
        );
        assert!(
            reused
                .query("SELECT missing FROM repository_state", ())
                .await
                .is_err()
        );
        drop(reused);
        assert_eq!(database.idle_readers.lock().unwrap().len(), 2);
        let reused = database.begin_read_snapshot().await.unwrap();
        assert_eq!(snapshot_generation(&reused).await, 3);
    }

    #[tokio::test]
    async fn reader_cache_bounds_idle_connections_without_limiting_live_snapshots() {
        let temp = tempfile::tempdir().unwrap();
        let database = TursoDb::open(temp.path().join("casita.sqlite")).unwrap();
        for count in [7, 8, 9, 16] {
            let readers =
                futures::future::try_join_all((0..count).map(|_| database.begin_read_snapshot()))
                    .await
                    .unwrap();
            assert_eq!(readers.len(), count);
            assert!(database.idle_readers.lock().unwrap().is_empty());
            drop(readers);
            let idle = database.idle_readers.lock().unwrap();
            assert_eq!(idle.len(), count.min(IDLE_READ_CONNECTIONS));
            assert!(
                idle.iter()
                    .all(|connection| connection.is_autocommit().unwrap())
            );
        }
    }

    /// Permanent benchmark: acquisition/query/release bursts across the idle
    /// cache bound. The fresh control empties the idle cache after every burst;
    /// connection destruction is included in its timed cost.
    #[tokio::test]
    #[ignore = "run through benchmark run snapshot-connections"]
    async fn benchmark_snapshot_connections() {
        let width: usize = std::env::var("CASITA_SNAPSHOT_WIDTH")
            .unwrap()
            .parse()
            .unwrap();
        let iterations: usize = std::env::var("CASITA_SNAPSHOT_ITERATIONS")
            .unwrap()
            .parse()
            .unwrap();
        let mode = std::env::var("CASITA_SNAPSHOT_MODE").unwrap();
        assert!(width > 0 && iterations > 0);
        assert!(matches!(mode.as_str(), "reused" | "fresh"));
        let temp = tempfile::tempdir().unwrap();
        let database = TursoDb::open(temp.path().join("casita.sqlite")).unwrap();
        database.writer.lock().await.execute_batch(
            "INSERT INTO repository_state(singleton, revision, generation) VALUES (1, X'01', 0);"
        ).await.unwrap();
        let mut elapsed = std::time::Duration::ZERO;
        for generation in 0..=iterations {
            // Commit outside the measured burst. Each lease must observe the
            // new state, including reused connections from the previous burst.
            database
                .writer
                .lock()
                .await
                .execute_batch(format!(
                    "UPDATE repository_state SET generation = {generation};"
                ))
                .await
                .unwrap();
            let started = std::time::Instant::now();
            let readers = futures::future::try_join_all((0..width).map(|_| async {
                let connection = database.begin_read_snapshot().await.unwrap();
                assert_eq!(snapshot_generation(&connection).await, generation as i64);
                Ok::<_, Error>(connection)
            }))
            .await
            .unwrap();
            drop(readers);
            if mode == "fresh" {
                database.idle_readers.lock().unwrap().clear();
            }
            let duration = started.elapsed();
            if generation > 0 {
                elapsed += duration;
            }
            let idle = database.idle_readers.lock().unwrap();
            assert_eq!(
                idle.len(),
                if mode == "fresh" {
                    0
                } else {
                    width.min(IDLE_READ_CONNECTIONS)
                }
            );
            assert!(
                idle.iter()
                    .all(|connection| connection.is_autocommit().unwrap())
            );
        }
        let reader = database.begin_read_snapshot().await.unwrap();
        assert!(
            reader
                .execute_batch("DELETE FROM repository_state;")
                .await
                .is_err()
        );
        drop(reader);
        println!(
            "snapshot_connection_sample {}",
            serde_json::json!({
                "width": width, "iterations": iterations, "mode": mode,
                "idle_limit": IDLE_READ_CONNECTIONS, "wall_nanos": elapsed.as_nanos() as u64,
                "correctness": "fresh committed generation, query-only, bounded idle cache, no idle transaction"
            })
        );
    }

    fn user_version(database: &Arc<TursoDb>) -> i64 {
        futures::executor::block_on(async {
            let writer = database.writer.lock().await;
            let mut rows = writer.query("PRAGMA user_version", ()).await.unwrap();
            rows.next().await.unwrap().unwrap().get(0).unwrap()
        })
    }

    fn set_user_version(database: &Arc<TursoDb>, version: i64) {
        futures::executor::block_on(async {
            database
                .writer
                .lock()
                .await
                .execute_batch(format!("PRAGMA user_version = {version};"))
                .await
                .unwrap();
        });
    }

    #[test]
    fn fresh_database_starts_at_current_schema_and_reopens_without_migration() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("casita.sqlite");

        let database = TursoDb::open(&path).unwrap();
        assert_eq!(user_version(&database), REPOSITORY_USER_VERSION);
        drop(database);

        let reopened = TursoDb::open(&path).unwrap();
        assert_eq!(user_version(&reopened), REPOSITORY_USER_VERSION);
    }

    #[test]
    fn existing_nonrelease_schema_versions_are_rejected() {
        let temp = tempfile::tempdir().unwrap();

        for version in [0, 1, 2, 3, 4, 5, 7] {
            let path = temp.path().join(format!("schema-{version}.sqlite"));
            let database = TursoDb::open(&path).unwrap();
            set_user_version(&database, version);
            drop(database);

            let error = TursoDb::open(&path).unwrap_err().to_string();
            assert!(error.contains(&format!("unsupported schema version {version}")));
            assert!(error.contains(&format!("supports only version {REPOSITORY_USER_VERSION}")));
            assert!(error.contains("does not migrate pre-release repositories"));
        }
    }

    #[test]
    fn rejected_older_schemas_preserve_their_schema_and_data() {
        async fn schema(database: &TursoDb) -> Vec<String> {
            let writer = database.writer.lock().await;
            let mut rows = writer
                .query(
                    "SELECT sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY name",
                    (),
                )
                .await
                .unwrap();
            let mut result = Vec::new();
            while let Some(row) = rows.next().await.unwrap() {
                result.push(row.get::<String>(0).unwrap());
            }
            result
        }
        for version in [4, 5] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("casita.sqlite");
            let database = TursoDb::open(&path).unwrap();
            futures::executor::block_on(async {
                let writer = database.writer.lock().await;
                writer.execute_batch("INSERT INTO repository_state(singleton, revision, generation, payload_catalog_ref) VALUES(1, X'01', 42, X'0203'); ALTER TABLE repository_state RENAME COLUMN payload_catalog_ref TO payload_catalog;").await.unwrap();
                if version == 4 {
                    writer
                        .execute_batch(
                            "DROP TABLE metadata_records; DROP INDEX named_roots_target;",
                        )
                        .await
                        .unwrap();
                }
            });
            set_user_version(&database, version);
            let before = futures::executor::block_on(schema(&database));
            let error = TursoDb::open(&path).unwrap_err().to_string();
            assert!(error.contains(&format!("unsupported schema version {version}")));
            assert_eq!(user_version(&database), version);
            assert_eq!(futures::executor::block_on(schema(&database)), before);
            futures::executor::block_on(async {
                let writer = database.writer.lock().await;
                let mut rows = writer
                    .query(
                        "SELECT revision, generation, payload_catalog FROM repository_state",
                        (),
                    )
                    .await
                    .unwrap();
                let row = rows.next().await.unwrap().unwrap();
                assert_eq!(row.get::<Vec<u8>>(0).unwrap(), vec![1]);
                assert_eq!(row.get::<i64>(1).unwrap(), 42);
                assert_eq!(row.get::<Vec<u8>>(2).unwrap(), vec![2, 3]);
            });
        }
    }

    /// Build the database the way `TursoDb::open` does, without casita's schema.
    fn open_raw(path: &Path) -> (Database, Connection) {
        futures::executor::block_on(async {
            let builder = Builder::new_local(path.to_str().unwrap());
            #[cfg(windows)]
            let builder = builder.with_io("experimental_win_iocp");
            let database = builder
                .experimental_multiprocess_wal(true)
                .build()
                .await
                .unwrap();
            let connection = database.connect().unwrap();
            (database, connection)
        })
    }

    fn schema_names(database: &Arc<TursoDb>) -> Vec<String> {
        futures::executor::block_on(async {
            let writer = database.writer.lock().await;
            let mut rows = writer
                .query("SELECT name FROM sqlite_schema ORDER BY name", ())
                .await
                .unwrap();
            let mut result = Vec::new();
            while let Some(row) = rows.next().await.unwrap() {
                result.push(row.get::<String>(0).unwrap());
            }
            result
        })
    }

    #[test]
    fn zero_byte_database_file_is_initialized() {
        // A crash right after the file was created leaves it empty.
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("casita.sqlite");
        std::fs::File::create(&path).unwrap();

        let database = TursoDb::open(&path).unwrap();
        assert_eq!(user_version(&database), REPOSITORY_USER_VERSION);
        assert!(schema_names(&database).contains(&"repository_state".to_owned()));
        drop(database);
        let reopened = TursoDb::open(&path).unwrap();
        assert_eq!(user_version(&reopened), REPOSITORY_USER_VERSION);
    }

    #[test]
    fn database_left_without_schema_by_an_interrupted_open_is_initialized() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("casita.sqlite");
        // Simulate a crash between Turso creating the database and the schema
        // transaction committing: the file (and its WAL) exist, but no schema
        // object or user_version was ever published.
        let (database, connection) = open_raw(&path);
        futures::executor::block_on(async {
            connection
                .execute_batch("BEGIN IMMEDIATE; CREATE TABLE interrupted(x); ROLLBACK;")
                .await
                .unwrap();
        });
        drop(connection);
        drop(database);
        assert!(path.exists());

        let database = TursoDb::open(&path).unwrap();
        assert_eq!(user_version(&database), REPOSITORY_USER_VERSION);
        let names = schema_names(&database);
        assert!(names.contains(&"repository_state".to_owned()));
        assert!(!names.contains(&"interrupted".to_owned()));
    }

    #[test]
    fn unversioned_database_with_foreign_schema_is_rejected_untouched() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("casita.sqlite");
        let (database, connection) = open_raw(&path);
        futures::executor::block_on(async {
            connection
                .execute_batch("CREATE TABLE foreign_data(x); INSERT INTO foreign_data VALUES (7);")
                .await
                .unwrap();
        });
        drop(connection);
        drop(database);

        let error = TursoDb::open(&path).unwrap_err().to_string();
        assert!(error.contains("unsupported schema version 0"), "{error}");

        let (database, connection) = open_raw(&path);
        futures::executor::block_on(async {
            let mut rows = connection
                .query("SELECT name FROM sqlite_schema ORDER BY name", ())
                .await
                .unwrap();
            let mut names = Vec::new();
            while let Some(row) = rows.next().await.unwrap() {
                names.push(row.get::<String>(0).unwrap());
            }
            assert_eq!(names, ["foreign_data"]);
            drop(rows);
            let mut rows = connection.query("PRAGMA user_version", ()).await.unwrap();
            assert_eq!(
                rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
                0
            );
        });
        drop(connection);
        drop(database);
    }
}
