use super::*;
use std::future::Future;
use std::io::Read;
use std::sync::mpsc;
use std::task::{Context, Waker};
use std::time::{Duration, Instant};

const DEADLINE: Duration = Duration::from_secs(5);

fn returned(slots: &Semaphore, count: usize) {
    let deadline = Instant::now() + DEADLINE;
    while slots.available_permits() != count {
        assert!(Instant::now() < deadline, "decoder capacity did not return");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn admission_is_bounded_across_runtimes_and_abandonment() {
    let runtimes: Vec<_> = (0..2)
        .map(|_| {
            tokio::runtime::Builder::new_current_thread()
                .max_blocking_threads(1)
                .enable_all()
                .build()
                .unwrap()
        })
        .collect();
    let slots = Arc::new(Semaphore::new(DECODERS));
    let (started, running) = mpsc::channel();
    let mut releases = Vec::new();
    let mut active = Vec::new();
    let mut cx = Context::from_waker(Waker::noop());
    for index in 0..DECODERS {
        let (release, wait) = mpsc::channel();
        releases.push(release);
        let started = started.clone();
        let mut work = Box::pin(run_on(slots.clone(), move || {
            started.send(index).unwrap();
            wait.recv_timeout(DEADLINE).unwrap();
        }));
        let _entered = runtimes[index % 2].enter();
        assert!(work.as_mut().poll(&mut cx).is_pending());
        active.push(work);
        assert_eq!(slots.available_permits(), DECODERS - index - 1);
    }
    for _ in 0..DECODERS {
        running.recv_timeout(DEADLINE).unwrap();
    }
    let (queued, entered) = mpsc::channel();
    let mut abandoned = Box::pin(run_on(slots.clone(), move || {
        queued.send(()).unwrap();
    }));
    {
        let _entered = runtimes[0].enter();
        assert!(abandoned.as_mut().poll(&mut cx).is_pending());
    }
    drop(abandoned);
    assert_eq!(entered.try_recv(), Err(mpsc::TryRecvError::Disconnected));
    let (next_started, next_running) = mpsc::channel();
    let mut next = Box::pin(run_on(slots.clone(), move || {
        next_started.send(()).unwrap();
    }));
    {
        let _entered = runtimes[1].enter();
        assert!(next.as_mut().poll(&mut cx).is_pending());
    }
    drop(active);
    assert_eq!(slots.available_permits(), 0);
    // Existing workers retain their slots even after callers and runtimes go.
    for runtime in runtimes {
        runtime.shutdown_background();
    }
    assert_eq!(slots.available_permits(), 0);
    for release in releases {
        release.send(()).unwrap();
    }
    futures::executor::block_on(next).unwrap();
    next_running.recv_timeout(DEADLINE).unwrap();
    returned(&slots, DECODERS);
}

#[test]
fn closing_the_pipe_after_runtime_teardown_releases_the_worker() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let slots = Arc::new(Semaphore::new(1));
    let (writer, reader) = tokio::io::duplex(1);
    let (started, running) = mpsc::channel();
    let (finished, complete) = mpsc::channel();
    let mut work = Box::pin(run_on(slots.clone(), move || {
        let mut bridge = tokio_util::io::SyncIoBridge::new(reader);
        started.send(()).unwrap();
        let mut bytes = Vec::new();
        bridge.read_to_end(&mut bytes).unwrap();
        assert!(bytes.is_empty());
        finished.send(()).unwrap();
    }));
    {
        let _entered = runtime.enter();
        assert!(
            work.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    running.recv_timeout(DEADLINE).unwrap();
    drop(work);
    runtime.shutdown_background();
    assert_eq!(slots.available_permits(), 0);
    drop(writer);
    complete.recv_timeout(DEADLINE).unwrap();
    returned(&slots, 1);
}

#[tokio::test]
async fn panics_are_storage_errors_and_return_capacity() {
    let slots = Arc::new(Semaphore::new(1));
    let error = run_on(slots.clone(), || panic!("decoder fixture panic"))
        .await
        .unwrap_err();
    assert!(matches!(&error, NarError::Storage(_)));
    assert!(error.to_string().contains("decoder fixture panic"));
    run_on(slots.clone(), || {}).await.unwrap();
    returned(&slots, 1);
}
