//! Synchronous NAR parsing must not occupy the pool needed by its consumer.
use super::NarError;
use std::sync::{Arc, LazyLock};
use tokio::sync::{Semaphore, oneshot};

// Additional process-wide capacity, shared even across repositories/runtimes.
// A stalled input holds a slot until it is completed or abandoned. This is a
// resource bound, not a throughput optimum or a promise about input latency.
const DECODERS: usize = 16;
static SLOTS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(DECODERS)));

pub(super) async fn run(work: impl FnOnce() + Send + 'static) -> Result<(), NarError> {
    run_on(SLOTS.clone(), work).await
}

async fn run_on(
    slots: Arc<Semaphore>,
    work: impl FnOnce() + Send + 'static,
) -> Result<(), NarError> {
    let runtime = tokio::runtime::Handle::current();
    let permit = slots
        .acquire_owned()
        .await
        .expect("decoder admission stays open");
    let (send, receive) = oneshot::channel();
    let worker = std::thread::Builder::new()
        .name("casita-nar-decode".into())
        .spawn(move || {
            let _permit = permit;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                let _entered = runtime.enter();
                work();
            }));
            // An abandoned caller closes the pipes the decoder can wait on.
            // The handle provides context, not runtime ownership; no worker
            // may borrow the caller's session or depend on async Drop joining it.
            drop(send.send(result));
        })
        .map_err(NarError::storage)?;
    drop(worker);
    receive.await.map_err(NarError::storage)?.map_err(|panic| {
        let message = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("non-string panic");
        NarError::storage(std::io::Error::other(format!(
            "NAR decoder panicked: {message}"
        )))
    })
}

#[cfg(test)]
mod tests;
