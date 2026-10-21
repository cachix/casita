#![cfg(all(feature = "experimental", feature = "native", unix))]

//! A live repository survives an ordinary SQLite client.
//!
//! A stock SQLite connection does not see Turso's multi-process coordination.
//! Left alone, it builds its own WAL index from the frames it finds, and when
//! its last connection closes it checkpoints the frames it knew into the
//! database file and unlinks the WAL. A live casita process keeps writing to
//! the unlinked file, the next process to open the repository starts from the
//! checkpointed database file, and payload deletions justified by the lost
//! commits leave the surviving state referencing payloads that no longer
//! exist. Writing, checkpointing or changing the journal mode damage the WAL
//! the same way.
//!
//! Each test checks the outcome the way that surfaced: a separate process opens
//! the repository and reads the published root's payload, after enough
//! mutations for online catalog collection to reclaim what later commits
//! superseded. Clients run in their own processes, like a database browser.

use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use casita::experimental::{
    MetadataError, MetadataStore as _, Repository, RepositoryError, RepositoryErrorCategory,
    RetryDisposition, RootName,
};
use tokio::io::AsyncReadExt as _;

const READER_TEST: &str = "child_reads_the_durable_root";
const REPOSITORY_ENV: &str = "CASITA_FOREIGN_SQLITE_REPOSITORY";
const CLIENT_TEST: &str = "child_sqlite_client";
const CLIENT_DATABASE_ENV: &str = "CASITA_FOREIGN_SQLITE_DATABASE";
const CLIENT_ACTION_ENV: &str = "CASITA_FOREIGN_SQLITE_ACTION";
const CLIENT_HOLD_ENV: &str = "CASITA_FOREIGN_SQLITE_HOLD";

const FIRST: &[u8] = b"published before a SQLite client opened the database";
const SECOND: &[u8] = b"published after a SQLite client opened the database";

/// Mutations between online catalog collections (`METADATA_RECLAIM_INTERVAL`).
const METADATA_RECLAIM_INTERVAL: usize = 16;

type LocalRepository =
    Repository<casita::experimental::ChunkedBlobStore, casita::experimental::TursoMetadataStore>;

fn root() -> RootName {
    RootName::try_from("foreign-sqlite/root").unwrap()
}

async fn publish(
    repository: &LocalRepository,
    bytes: &[u8],
) -> Result<(), Box<casita::experimental::RepositoryError>> {
    let mutation = repository.mutation_session().await?;
    let staged = mutation.stage_blob(bytes).await?;
    let key = staged.record().key().clone();
    mutation.publish_rooted(vec![staged], root(), key).await?;
    Ok(())
}

/// Run enough mutations for the online catalog collector to reclaim what the
/// latest commit superseded, as a busy server does.
async fn mutate_until_reclaimed(repository: &LocalRepository) {
    for _ in 0..=METADATA_RECLAIM_INTERVAL {
        let _ = repository.mutation_session().await;
    }
}

async fn close(repository: LocalRepository) {
    drop(repository);
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
}

fn wal(directory: &Path) -> PathBuf {
    directory.join("casita.sqlite-wal")
}

fn inode(path: &Path) -> Option<u64> {
    std::fs::metadata(path).ok().map(|metadata| metadata.ino())
}

/// What a newly started process sees as the root's payload.
fn read_in_new_process(repository: &Path) -> Result<Vec<u8>, String> {
    let output = Command::new(std::env::current_exe().unwrap())
        .args([READER_TEST, "--exact", "--nocapture"])
        .env(REPOSITORY_ENV, repository)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    match stdout
        .lines()
        .find_map(|line| line.strip_prefix("payload="))
    {
        Some(hex) if output.status.success() => Ok((0..hex.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).unwrap())
            .collect()),
        _ => Err(String::from_utf8_lossy(&output.stderr).into_owned()),
    }
}

/// The reader half: open the repository afresh and read the root's payload.
/// Without the environment variable this is an ordinary no-op test.
#[test]
fn child_reads_the_durable_root() {
    let Ok(path) = std::env::var(REPOSITORY_ENV) else {
        return;
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let repository = Repository::local(&path).await.unwrap();
        let snapshot = repository.metadata().snapshot().await.unwrap();
        let key = snapshot
            .root(&root())
            .await
            .unwrap()
            .expect("a durable root");
        drop(snapshot);
        let hold = repository.retention_hold().await.unwrap();
        let (_, mut reader) = hold.open_payload(&key).await.unwrap().unwrap();
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        println!("payload={hex}");
        drop(reader);
        drop(hold);
        close(repository).await;
    });
}

/// What a stock SQLite client does to the database.
#[derive(Clone, Copy, Debug)]
enum Action {
    /// Read the committed state, as a database browser does on opening.
    Read,
    /// Read through a read-only connection.
    ReadOnly,
    /// Change a table.
    Write,
    /// Checkpoint the WAL and truncate it.
    Checkpoint,
    /// Read in exclusive locking mode, which keeps the WAL index in memory.
    ExclusiveRead,
    /// Leave WAL mode.
    LeaveWal,
}

impl Action {
    fn name(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::ReadOnly => "read-only",
            Self::Write => "write",
            Self::Checkpoint => "checkpoint",
            Self::ExclusiveRead => "exclusive-read",
            Self::LeaveWal => "leave-wal",
        }
    }

    fn parse(name: &str) -> Self {
        [
            Self::Read,
            Self::ReadOnly,
            Self::Write,
            Self::Checkpoint,
            Self::ExclusiveRead,
            Self::LeaveWal,
        ]
        .into_iter()
        .find(|action| action.name() == name)
        .unwrap()
    }

    fn run(self, database: &Path) -> rusqlite::Result<rusqlite::Connection> {
        use rusqlite::OpenFlags;

        let flags = match self {
            Self::ReadOnly => OpenFlags::SQLITE_OPEN_READ_ONLY,
            _ => OpenFlags::default(),
        };
        let connection = rusqlite::Connection::open_with_flags(database, flags)?;
        connection.busy_timeout(std::time::Duration::ZERO)?;
        let generation = |connection: &rusqlite::Connection| {
            connection.query_row("SELECT generation FROM repository_state", [], |row| {
                row.get::<_, i64>(0)
            })
        };
        match self {
            Self::Read | Self::ReadOnly => {
                generation(&connection)?;
            }
            Self::Write => connection.execute_batch(
                "CREATE TABLE IF NOT EXISTS foreign_client (value); \
                 INSERT INTO foreign_client VALUES (1);",
            )?,
            Self::Checkpoint => {
                connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))?
            }
            Self::ExclusiveRead => {
                connection.query_row("PRAGMA locking_mode = EXCLUSIVE", [], |_| Ok(()))?;
                generation(&connection)?;
            }
            Self::LeaveWal => {
                connection.query_row("PRAGMA journal_mode = DELETE", [], |_| Ok(()))?
            }
        }
        Ok(connection)
    }
}

/// The client half: a stock SQLite connection in its own process. It acts,
/// reports, optionally waits to be released, and closes.
#[test]
fn child_sqlite_client() {
    let Ok(database) = std::env::var(CLIENT_DATABASE_ENV) else {
        return;
    };
    let database = PathBuf::from(database);
    let action = Action::parse(&std::env::var(CLIENT_ACTION_ENV).unwrap());
    let connection = action.run(&database);
    println!(
        "client={}",
        match &connection {
            Ok(_) => "ok".to_owned(),
            Err(error) => format!("error:{error}"),
        }
    );
    if let Ok(release) = std::env::var(CLIENT_HOLD_ENV) {
        let release = PathBuf::from(release);
        std::fs::write(release.with_extension("ready"), b"").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while !release.exists() {
            assert!(std::time::Instant::now() < deadline, "never released");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
    drop(connection);
}

fn client_command(database: &Path, action: Action) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([CLIENT_TEST, "--exact", "--nocapture"])
        .env(CLIENT_DATABASE_ENV, database)
        .env(CLIENT_ACTION_ENV, action.name());
    command
}

fn client_outcome(stdout: &[u8]) -> Result<(), String> {
    let stdout = String::from_utf8_lossy(stdout);
    match stdout.lines().find_map(|line| line.strip_prefix("client=")) {
        Some("ok") => Ok(()),
        Some(error) => Err(error.to_owned()),
        None => panic!("the SQLite client did not run: {stdout}"),
    }
}

/// Run a client to completion, closing it.
fn run_client(database: &Path, action: Action) -> Result<(), String> {
    let output = client_command(database, action).output().unwrap();
    assert!(output.status.success(), "{output:?}");
    client_outcome(&output.stdout)
}

/// A client that stays open until released.
struct HeldClient {
    child: std::process::Child,
    release: PathBuf,
}

impl HeldClient {
    fn start(database: &Path, action: Action) -> Self {
        let release = database.with_file_name("client-release");
        let child = client_command(database, action)
            .env(CLIENT_HOLD_ENV, &release)
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while !release.with_extension("ready").exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "the SQLite client did not start"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        Self { child, release }
    }

    /// Let the client close, and return what its action reported.
    fn close(self) -> Result<(), String> {
        std::fs::write(&self.release, b"").unwrap();
        let output = self.child.wait_with_output().unwrap();
        assert!(output.status.success(), "{output:?}");
        client_outcome(&output.stdout)
    }
}

/// A client that runs while casita has the repository open fails without
/// touching the WAL, and casita's commits stay durable.
async fn client_cannot_damage_a_live_repository(action: Action) {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("casita.sqlite");
    let repository = Repository::local(directory.path()).await.unwrap();
    publish(&repository, FIRST).await.unwrap();
    let log = inode(&wal(directory.path()));

    let client = run_client(&database, action);
    let second = publish(&repository, SECOND).await;
    mutate_until_reclaimed(&repository).await;

    assert_eq!(
        read_in_new_process(directory.path()),
        Ok(SECOND.to_vec()),
        "after a SQLite client ({action:?}) ran: {client:?}, publishing: {second:?}"
    );
    assert_eq!(
        inode(&wal(directory.path())),
        log,
        "a SQLite client ({action:?}) removed or replaced the live WAL"
    );
    assert!(
        client.is_err(),
        "a SQLite client ({action:?}) opened a live repository"
    );
    close(repository).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reading_client_cannot_remove_the_live_wal() {
    client_cannot_damage_a_live_repository(Action::Read).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_only_client_cannot_open_a_live_repository() {
    client_cannot_damage_a_live_repository(Action::ReadOnly).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writing_client_cannot_damage_the_live_wal() {
    client_cannot_damage_a_live_repository(Action::Write).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_checkpointing_client_cannot_truncate_the_live_wal() {
    client_cannot_damage_a_live_repository(Action::Checkpoint).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_exclusive_mode_client_cannot_remove_the_live_wal() {
    client_cannot_damage_a_live_repository(Action::ExclusiveRead).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_cannot_take_a_live_repository_out_of_wal_mode() {
    client_cannot_damage_a_live_repository(Action::LeaveWal).await;
}

/// A repository with FIRST published and no process left holding it open.
async fn closed_repository() -> tempfile::TempDir {
    let directory = tempfile::tempdir().unwrap();
    let repository = Repository::local(directory.path()).await.unwrap();
    publish(&repository, FIRST).await.unwrap();
    close(repository).await;
    directory
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_open_before_casita_cannot_remove_the_wal_when_it_closes() {
    let directory = closed_repository().await;
    let database = directory.path().join("casita.sqlite");
    let client = HeldClient::start(&database, Action::Read);

    let repository = Repository::local(directory.path()).await.unwrap();
    let log = inode(&wal(directory.path()));
    publish(&repository, SECOND).await.unwrap();
    mutate_until_reclaimed(&repository).await;
    assert_eq!(
        client.close(),
        Ok(()),
        "the client read before casita opened"
    );

    assert_eq!(read_in_new_process(directory.path()), Ok(SECOND.to_vec()));
    assert_eq!(
        inode(&wal(directory.path())),
        log,
        "the client removed the WAL on close"
    );
    close(repository).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_exclusive_client_open_before_casita_keeps_it_from_opening() {
    let directory = closed_repository().await;
    let database = directory.path().join("casita.sqlite");
    let client = HeldClient::start(&database, Action::ExclusiveRead);

    // The client will checkpoint and remove the WAL when it closes, and casita
    // cannot stop it: opening is busy until then.
    let error = match Repository::local(directory.path()).await {
        Ok(_) => panic!("opened while a SQLite client holds the database exclusively"),
        Err(error) => error,
    };
    assert!(
        matches!(
            &error,
            RepositoryError::Metadata(MetadataError::ForeignSqliteLock { path }) if *path == database
        ),
        "{error:?}"
    );
    assert_eq!(error.category(), RepositoryErrorCategory::Busy);
    assert_eq!(error.retry_disposition(), RetryDisposition::Retry);
    assert_eq!(client.close(), Ok(()));

    let repository = Repository::local(directory.path()).await.unwrap();
    publish(&repository, SECOND).await.unwrap();
    assert_eq!(read_in_new_process(directory.path()), Ok(SECOND.to_vec()));
    close(repository).await;
}

/// The locks cannot stop every removal: a copy put in the WAL's place, as a
/// file tool or restore might, leaves this process writing to a file no new
/// process reads. Nothing committed there may be acknowledged, nothing may be
/// deleted on its strength, and the process must not go on serving it: the
/// user has to restart it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commits_deletions_and_reads_stop_once_the_wal_is_replaced() {
    let directory = tempfile::tempdir().unwrap();
    let repository = Repository::local(directory.path()).await.unwrap();
    publish(&repository, FIRST).await.unwrap();
    let retained = repository.metadata().snapshot().await.unwrap();
    let published = retained.root(&root()).await.unwrap();
    assert!(published.is_some());
    let copy = directory.path().join("wal-copy");
    std::fs::copy(wal(directory.path()), &copy).unwrap();
    std::fs::rename(&copy, wal(directory.path())).unwrap();

    let second = publish(&repository, SECOND).await;
    mutate_until_reclaimed(&repository).await;
    let error = second.expect_err("a commit to a replaced WAL was acknowledged");
    assert_restart_required(&error);
    assert!(
        matches!(
            &*error,
            RepositoryError::Metadata(MetadataError::DatabaseReplaced { path })
                if *path == wal(directory.path())
        ),
        "{error:?}"
    );
    match repository.metadata().snapshot().await {
        Ok(_) => panic!("read the state of a replaced WAL"),
        Err(error) => assert_restart_required(&RepositoryError::Metadata(error)),
    }
    // Nor through a snapshot taken before the replacement was found.
    match retained.root(&root()).await {
        Ok(_) => panic!("a retained snapshot read the state of a replaced WAL"),
        Err(error) => assert_restart_required(&RepositoryError::Metadata(error)),
    }
    assert_eq!(read_in_new_process(directory.path()), Ok(FIRST.to_vec()));
    close(repository).await;
}

fn assert_restart_required(error: &RepositoryError) {
    assert_eq!(
        error.category(),
        RepositoryErrorCategory::RestartRequired,
        "{error}"
    );
    assert_eq!(error.retry_disposition(), RetryDisposition::Never);
}
