//! Collector ownership and tracked tasks requiring asynchronous storage I/O.

use futures::FutureExt;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};

use super::MetadataError;

type Release = Box<
    dyn FnOnce() -> Pin<Box<dyn Future<Output = Result<(), MetadataError>> + Send>> + Send + Sync,
>;

/// Backend-owned collector serialization. Readers and writers use online data
/// pins instead; this lease excludes competing collectors.
///
/// Dropping a hold schedules its durable release on the current Tokio runtime.
/// Call [`flush_repository_leases`] before shutting down that runtime. If release
/// cannot complete, the durable hold remains in place rather than expiring.
#[derive(Default)]
pub struct RepositoryLease {
    release: Option<Release>,
    retained: bool,
}

impl RepositoryLease {
    /// Construct collector ownership with an asynchronous, idempotent release operation.
    pub fn new<F, Fut>(release: F) -> Self
    where
        F: FnOnce() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), MetadataError>> + Send + 'static,
    {
        Self {
            release: Some(Box::new(|| Box::pin(release()))),
            retained: false,
        }
    }

    /// Ownership for a store whose collectors are already serialized by the
    /// opening process: the repository's collector mutex and, for local
    /// repositories, `gc.lock`. It holds nothing durable and releases nothing.
    pub fn process_local() -> Self {
        Self::default()
    }

    pub(crate) fn retain_on_drop(&mut self) {
        self.retained = true;
    }

    pub(crate) fn release_on_drop(&mut self) {
        self.retained = false;
    }
}

#[derive(Default)]
struct Releases {
    pending: usize,
    error: Option<MetadataError>,
    changed: Arc<tokio::sync::Notify>,
}

fn releases() -> &'static Mutex<HashMap<tokio::runtime::Id, Releases>> {
    static RELEASES: OnceLock<Mutex<HashMap<tokio::runtime::Id, Releases>>> = OnceLock::new();
    RELEASES.get_or_init(Mutex::default)
}

/// Track protected publication, catalog maintenance, admission, and release
/// through caller cancellation.
pub(crate) fn spawn_lease_task(
    future: impl Future<Output = Result<(), MetadataError>> + Send + 'static,
) {
    let runtime = tokio::runtime::Handle::current();
    let id = runtime.id();
    let mut all = releases().lock().unwrap_or_else(|e| e.into_inner());
    all.entry(id).or_default().pending += 1;
    let collection_writes = super::BackendWriteScope::current();
    runtime.spawn(async move {
        let result = std::panic::AssertUnwindSafe(collection_writes.run(future))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| {
                Err(MetadataError::Backend(
                    "repository background task panicked".to_owned(),
                ))
            });
        let mut all = releases().lock().unwrap_or_else(|e| e.into_inner());
        let pending = all.entry(id).or_default();
        pending.pending -= 1;
        if let Err(error) = result {
            tracing::error!("repository background task failed: {error}");
            pending.error.get_or_insert(error);
        }
        pending.changed.notify_waiters();
    });
}

/// Run a tracked task with a foreground reply. Sending the reply does not end
/// tracking: the task may continue cleanup and report its failure to draining.
/// Errors carried in the reply belong to the caller, not the shutdown result.
pub(crate) async fn run_lease_task<T, F>(
    name: &str,
    task: impl FnOnce(tokio::sync::oneshot::Sender<T>) -> F,
) -> Result<T, MetadataError>
where
    T: Send + 'static,
    F: Future<Output = Result<(), MetadataError>> + Send + 'static,
{
    let (send, receive) = tokio::sync::oneshot::channel();
    spawn_lease_task(task(send));
    receive
        .await
        .map_err(|error| MetadataError::Backend(format!("{name} task: {error}")))
}

impl Drop for RepositoryLease {
    fn drop(&mut self) {
        let Some(release) = self.release.take() else {
            return;
        };
        if self.retained {
            tracing::warn!("interrupted collection retains durable ownership for offline recovery");
            return;
        }
        if tokio::runtime::Handle::try_current().is_err() {
            tracing::error!("repository hold outlived its runtime; durable recovery is required");
            return;
        }
        spawn_lease_task(async move { release().await });
    }
}

/// Finish releases scheduled by dropped repository sessions and retention holds.
///
/// Call this after dropping the application's holds, before shutting down Tokio.
/// The CLI does this on both success and failure. This does not release holds
/// that are still alive. It also waits for admission and publication after caller
/// cancellation, including catalog maintenance, finalization, and the resulting
/// hold releases.
/// A failed hold release leaves durable protection in place and must be resolved
/// before physical collection. Failed catalog maintenance discards its candidate
/// before releasing protection; its error is also reported here.
pub async fn flush_repository_leases() -> Result<(), MetadataError> {
    let id = tokio::runtime::Handle::current().id();
    loop {
        let changed = {
            let mut all = releases().lock().unwrap_or_else(|e| e.into_inner());
            let Some(pending) = all.get(&id) else {
                return Ok(());
            };
            if pending.pending == 0 {
                return all.remove(&id).unwrap().error.map_or(Ok(()), Err);
            }
            // Register under the same lock used by completion. Reacquire the
            // current entry each loop: another drain may remove an idle entry
            // before a new task creates its replacement.
            let mut changed = Box::pin(pending.changed.clone().notified_owned());
            changed.as_mut().enable();
            changed
        };
        changed.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unrelated_runtime_completion_does_not_wake_a_lease_drain() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use std::task::{Context, Wake, Waker};
        struct CountWake(AtomicUsize);
        impl Wake for CountWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let owner = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let other = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let resume = Arc::new(tokio::sync::Notify::new());
        let wait = resume.clone();
        let wakes = Arc::new(CountWake(AtomicUsize::new(0)));
        let waker = Waker::from(wakes.clone());
        owner.block_on(async {
            spawn_lease_task(async move {
                wait.notified().await;
                Ok(())
            });
            tokio::task::yield_now().await;
        });
        let mut drain = Box::pin(flush_repository_leases());
        {
            let _entered = owner.enter();
            assert!(
                drain
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
        }
        other.block_on(async {
            spawn_lease_task(async { Ok(()) });
            flush_repository_leases().await.unwrap();
        });
        let unrelated_wakes = wakes.0.load(Ordering::SeqCst);
        // Clean up both runtimes before asserting, including on the broken implementation.
        resume.notify_one();
        owner.block_on(drain.as_mut()).unwrap();
        assert_eq!(
            unrelated_wakes, 0,
            "another runtime woke this runtime's drain"
        );
    }

    #[tokio::test]
    async fn foreground_reply_does_not_hide_later_cleanup_failure() {
        let resume = std::sync::Arc::new(tokio::sync::Notify::new());
        let wait = resume.clone();
        let result = run_lease_task("foreground", |send| async move {
            drop(send.send(Err::<(), _>(MetadataError::Backend(
                "operation failed".into(),
            ))));
            wait.notified().await;
            Err(MetadataError::Backend("cleanup failed".into()))
        })
        .await
        .unwrap();
        assert!(
            matches!(result, Err(MetadataError::Backend(message)) if message == "operation failed")
        );
        let drain = flush_repository_leases();
        tokio::pin!(drain);
        assert!(futures::poll!(&mut drain).is_pending());
        resume.notify_one();
        assert!(
            matches!(drain.await, Err(MetadataError::Backend(message)) if message == "cleanup failed")
        );
        flush_repository_leases().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_reply_drops_returned_ownership_and_drains_its_release() {
        let entered = std::sync::Arc::new(tokio::sync::Notify::new());
        let resume = std::sync::Arc::new(tokio::sync::Notify::new());
        let released = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let started = entered.clone();
        let wait = resume.clone();
        let completed = released.clone();
        let waiter = tokio::spawn(run_lease_task("cancelled", |send| async move {
            started.notify_one();
            wait.notified().await;
            let lease = RepositoryLease::new(move || async move {
                completed.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            });
            drop(send.send(lease));
            Ok(())
        }));
        entered.notified().await;
        waiter.abort();
        assert!(matches!(waiter.await, Err(error) if error.is_cancelled()));
        let drain = flush_repository_leases();
        tokio::pin!(drain);
        assert!(futures::poll!(&mut drain).is_pending());
        resume.notify_one();
        drain.await.unwrap();
        assert!(released.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn task_panic_reaches_both_reply_and_shutdown() {
        let result = run_lease_task(
            "panicking",
            |_send: tokio::sync::oneshot::Sender<()>| async {
                panic!("injected task panic");
            },
        )
        .await;
        assert!(
            matches!(result, Err(MetadataError::Backend(message)) if message.starts_with("panicking task:"))
        );
        assert!(matches!(flush_repository_leases().await,
            Err(MetadataError::Backend(message)) if message == "repository background task panicked"));
    }

    #[tokio::test]
    async fn cancelled_flush_keeps_tracking_release_and_reports_failure() {
        let resume = std::sync::Arc::new(tokio::sync::Notify::new());
        let wait = resume.clone();
        drop(RepositoryLease::new(move || async move {
            wait.notified().await;
            Err(MetadataError::Backend("release unavailable".into()))
        }));
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                flush_repository_leases()
            )
            .await
            .is_err()
        );
        resume.notify_one();
        let result = flush_repository_leases().await;
        assert!(
            matches!(result, Err(MetadataError::Backend(message)) if message == "release unavailable")
        );
        flush_repository_leases().await.unwrap();
    }
}
