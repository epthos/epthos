//! Implementation of the Clock trait for tests.
//!
//! This allows changing the current time, and synchronizing tests
//! with sleep() operations.
use crate::clock::Clock;
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, Weak},
    task::{Context, Poll, Waker},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::oneshot;

pub struct FakeClock {
    inner: Arc<Mutex<InnerFakeClock>>,
}

/// FakeClockHandler exposes controls over a FakeClock instance.
pub struct FakeClockHandler {
    inner: Arc<Mutex<InnerFakeClock>>,
}

impl FakeClockHandler {
    /// Creates a new FakeClock and return it with a handler to control it.
    pub fn new() -> (FakeClock, FakeClockHandler) {
        let inner = Arc::new(Mutex::new(InnerFakeClock {
            now: UNIX_EPOCH,
            pending: HashMap::new(),
            expected: HashMap::new(),
        }));
        (
            FakeClock {
                inner: inner.clone(),
            },
            FakeClockHandler { inner },
        )
    }

    /// Sets the time returned by the fake clock. Time does _not_ move
    /// automatically.
    #[allow(dead_code)] // TODO: use it!
    pub fn set_now(&self, now: SystemTime) {
        let mut inner = self.inner.lock().unwrap();
        inner.now = now;
    }

    /// Waits for code to sleep() with the specified label. When that happens,
    /// returns a SleepHandle. This handle allows the test code to let the sleep()
    /// call return when it wants it to.
    pub async fn wait(&self, id: &str) -> SleepHandle {
        let (tx, rx) = oneshot::channel();
        {
            let mut inner = self.inner.lock().unwrap();
            // Either the sleep was already initiated...
            let early_result = get_pending_future(&mut inner, id);
            if let Some(handle) = early_result {
                tracing::debug!("sleep({}) was pending already", id);
                return handle;
            }
            // ... or we can put a oneshot with the lock held, so it's
            // already present when it arrives.
            inner.expected.insert(id.to_owned(), tx);
        }
        tracing::debug!("waiting for sleep({})", id);
        rx.await.unwrap();
        // Now we should have received the result when we lock again.
        let mut inner = self.inner.lock().unwrap();
        get_pending_future(&mut inner, id).unwrap()
    }
}

pub struct SleepHandle {
    pub delay: Duration,
    inner: Arc<Mutex<InnerFakeSleep>>,
}

impl SleepHandle {
    /// Finishes the sleep() call.
    pub fn done(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.ready = true;
        if let Some(waker) = inner.waker.take() {
            waker.wake();
        }
    }
}

fn get_pending_future(inner: &mut MutexGuard<'_, InnerFakeClock>, id: &str) -> Option<SleepHandle> {
    if let Some(weak) = inner.pending.get_mut(id) {
        let Some(mu) = weak.upgrade() else {
            return None;
        };
        let v = mu.lock().unwrap();
        return Some(SleepHandle {
            delay: v.delay,
            inner: mu.clone(),
        });
    }
    None
}

struct InnerFakeClock {
    now: SystemTime,
    pending: HashMap<String, Weak<Mutex<InnerFakeSleep>>>,
    expected: HashMap<String, oneshot::Sender<()>>,
}

impl InnerFakeClock {
    fn register(&mut self, id: String, sleep: Weak<Mutex<InnerFakeSleep>>) {
        if let Some(expected) = self.expected.remove(&id) {
            expected.send(()).expect("failed to send");
        }
        self.pending.insert(id, sleep);
    }
}

impl Clock for FakeClock {
    fn now(&self) -> SystemTime {
        let inner = self.inner.lock().unwrap();
        inner.now
    }

    fn sleep(&self, delay: Duration, id: &str) -> impl Future<Output = ()> + '_ {
        let inner = InnerFakeSleep {
            delay,
            id: id.to_owned(),
            waker: None,
            parent: Arc::downgrade(&self.inner),
            ready: false,
        };
        FakeSleep {
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

struct FakeSleep {
    inner: Arc<Mutex<InnerFakeSleep>>,
}

struct InnerFakeSleep {
    delay: Duration,
    id: String,
    waker: Option<Waker>,
    parent: Weak<Mutex<InnerFakeClock>>,
    ready: bool,
}

impl Future for FakeSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.inner.lock().unwrap();
        match inner.parent.upgrade() {
            Some(lock) => {
                if inner.ready {
                    Poll::Ready(())
                } else {
                    inner.waker = Some(cx.waker().clone());
                    let mut clock = lock.lock().unwrap();
                    clock.register(inner.id.clone(), Arc::downgrade(&self.inner));

                    Poll::Pending
                }
            }
            // If there is nobody to wake this up, we have no other option than
            // completing right away.
            None => Poll::Ready(()),
        }
    }
}
