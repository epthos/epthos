//! Implementation of the Clock trait for tests.
//!
//! This allows changing the current time, and synchronizing tests
//! with sleep() operations.
use crate::clock::Clock;
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Waker},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::oneshot;

/// Remote-controlled Clock implementation, for tests.
pub struct FakeClock {
    inner: Arc<Mutex<ClockState>>,
}

/// Handler exposes controls over a FakeClock instance.
#[derive(Clone)]
pub struct Handler {
    inner: Arc<Mutex<ClockState>>,
}

impl Handler {
    /// Creates a new FakeClock and return it with a handler to control it.
    pub fn new() -> (FakeClock, Handler) {
        let inner = Arc::new(Mutex::new(ClockState {
            now: UNIX_EPOCH,
            sleepers: HashMap::new(),
        }));
        (
            FakeClock {
                inner: inner.clone(),
            },
            Handler { inner },
        )
    }

    /// Sets the time returned by the fake clock. Time does _not_ move
    /// automatically.
    #[allow(dead_code)] // TODO: use it!
    pub fn set_now(&self, now: SystemTime) {
        let mut inner = self.inner.lock().unwrap();
        inner.now = now;
    }

    /// Waits for code to sleep() with the specified label and minimum
    /// incarnation. Incarnation starts at 1, and increases every time
    /// a sleep request with the given id is scheduled. The value passed
    /// here is inclusive. The function returns the duration passed to
    /// the sleep request that was awaited.
    pub async fn wait(&self, id: &str, incarnation: u64) -> Duration {
        let (tx, rx) = oneshot::channel();
        {
            let mut inner = self.inner.lock().unwrap();
            let replacement = match inner.sleepers.remove(id) {
                Some(SleepInfo {
                    unblock,
                    state: SleepState::SleeperOnly(so),
                }) => SleepInfo {
                    unblock,
                    state: SleepState::Syncing(Syncing {
                        current: so.current,
                        waker: so.waker,
                        duration: so.duration,
                        target: incarnation,
                        waiter: tx,
                    }),
                },
                None => SleepInfo {
                    unblock: false,
                    state: SleepState::WaiterOnly(WaiterOnly {
                        target: incarnation,
                        waiter: tx,
                    }),
                },
                Some(other) => other,
            };
            inner.sleepers.insert(id.to_owned(), replacement);
            inner.process(id);
        }
        let (_, duration) = rx.await.expect("sender failed");
        duration
    }

    /// Immediately unblock the current sleep() and return the incarnation it
    /// was at.
    pub fn unblock_once(&self, id: &str) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        match inner.sleepers.get_mut(id) {
            Some(ref mut info) => {
                info.unblock = true;
                // Trigger any waker if available.
                match info.state {
                    SleepState::SleeperOnly(ref mut so) => {
                        if let Some(waker) = so.waker.take() {
                            waker.wake();
                        }
                        so.current
                    }
                    SleepState::Syncing(ref mut sy) => {
                        if let Some(waker) = sy.waker.take() {
                            waker.wake();
                        }
                        sy.current
                    }
                    _ => 0,
                }
            }
            None => {
                inner.sleepers.insert(
                    id.to_owned(),
                    SleepInfo {
                        unblock: true,
                        state: SleepState::UnblockOnly,
                    },
                );
                0
            }
        }
    }

    // Test helper that returns true if there is a waiter for |id|.
    #[cfg(test)]
    pub fn has_waiter(&self, id: &str) -> bool {
        let inner = self.inner.lock().unwrap();
        match inner.sleepers.get(id) {
            Some(SleepInfo {
                unblock: _,
                state: SleepState::WaiterOnly(_),
            }) => true,
            Some(SleepInfo {
                unblock: _,
                state: SleepState::Syncing(_),
            }) => true,
            _ => false,
        }
    }

    // Test helper that returns true if there is a sleeper for |id|.
    #[cfg(test)]
    pub fn has_sleeper(&self, id: &str) -> bool {
        let inner = self.inner.lock().unwrap();
        match inner.sleepers.get(id) {
            Some(SleepInfo {
                unblock: _,
                state: SleepState::SleeperOnly(_),
            }) => true,
            Some(SleepInfo {
                unblock: _,
                state: SleepState::Syncing(_),
            }) => true,
            _ => false,
        }
    }
}

// Shared state between the Handler and its FakeClock.
struct ClockState {
    // Current time being returned.
    now: SystemTime,
    // All pending sleeper instances by id.
    sleepers: HashMap<String, SleepInfo>,
}

#[derive(Debug)]
struct SleepInfo {
    state: SleepState,
    // If true, the sleep will be unblocked right away.
    unblock: bool,
}

#[derive(Debug)]
enum SleepState {
    // unblock is called before anything else.
    UnblockOnly,
    // wait is called before the sleeper is created.
    WaiterOnly(WaiterOnly),
    // sleep is called before we wait for it.
    SleeperOnly(SleeperOnly),
    // sleep and wait are synchronizing.
    Syncing(Syncing),
}

#[derive(Debug)]
struct WaiterOnly {
    // Unblock the waiter when we get to that incarnation.
    target: u64,
    // How to unblock the waiter.
    waiter: oneshot::Sender<(u64, Duration)>,
}

#[derive(Debug)]
struct SleeperOnly {
    // Current incarnation of the sleeper.
    current: u64,
    // Current waker if it hasn't been fired yet.
    waker: Option<Waker>,
    // Current duration.
    duration: Duration,
}

#[derive(Debug)]
struct Syncing {
    // Current incarnation of the sleeper.
    current: u64,
    // Current waker if it hasn't been fired yet.
    waker: Option<Waker>,
    // Current duration.
    duration: Duration,
    // Unblock the waiter when we get to that incarnation.
    target: u64,
    // How to unblock the waiter.
    waiter: oneshot::Sender<(u64, Duration)>,
}

impl ClockState {
    // Called when a sleeper is _created_. Yields a new incarnation.
    fn on_new(&mut self, id: String, duration: Duration) {
        let (new_state, unblock) = match self.sleepers.remove(&id) {
            // New incarnation of an existing Sleeper.
            Some(SleepInfo {
                unblock,
                state: SleepState::SleeperOnly(so),
            }) => (
                SleepState::SleeperOnly(SleeperOnly {
                    current: so.current + 1,
                    waker: None,
                    duration,
                }),
                unblock,
            ),
            // We already had a waiter, the pair is now complete.
            Some(SleepInfo {
                unblock,
                state: SleepState::WaiterOnly(wo),
            }) => (
                SleepState::Syncing(Syncing {
                    current: 1,
                    waker: None,
                    duration,
                    target: wo.target,
                    waiter: wo.waiter,
                }),
                unblock,
            ),
            // New incarnation of an existing Sleeper already in a pair.
            Some(SleepInfo {
                unblock,
                state: SleepState::Syncing(sy),
            }) => (
                SleepState::Syncing(Syncing {
                    current: sy.current + 1,
                    waker: None,
                    duration,
                    target: sy.target,
                    waiter: sy.waiter,
                }),
                unblock,
            ),
            // Brand new Sleeper in its first incarnation.
            Some(SleepInfo {
                unblock,
                state: SleepState::UnblockOnly,
            }) => (
                SleepState::SleeperOnly(SleeperOnly {
                    current: 1,
                    waker: None,
                    duration,
                }),
                unblock,
            ),
            None => (
                SleepState::SleeperOnly(SleeperOnly {
                    current: 1,
                    waker: None,
                    duration,
                }),
                false,
            ),
        };
        self.sleepers.insert(
            id.clone(),
            SleepInfo {
                unblock,
                state: new_state,
            },
        );
        self.process(&id);
    }

    fn on_poll(&mut self, id: &str, waker: Waker) -> bool {
        let info = self.sleepers.get_mut(id);
        let unblock = match info {
            Some(SleepInfo {
                unblock,
                state: SleepState::SleeperOnly(so),
            }) => {
                so.waker = Some(waker);
                *unblock
            }
            Some(SleepInfo {
                unblock,
                state: SleepState::Syncing(sy),
            }) => {
                sy.waker = Some(waker);
                *unblock
            }
            Some(SleepInfo { unblock, state: _ }) => *unblock,
            None => false,
        };
        self.process(id);
        unblock
    }

    fn process(&mut self, id: &str) {
        match self.sleepers.remove(id) {
            None => {}
            Some(SleepInfo {
                unblock,
                state: SleepState::Syncing(sy),
            }) => {
                if sy.current >= sy.target {
                    let _ = sy.waiter.send((sy.current, sy.duration));
                    // Now that we consumed the waiter, we are back to SleeperOnly.
                    self.sleepers.insert(
                        id.to_owned(),
                        SleepInfo {
                            unblock,
                            state: SleepState::SleeperOnly(SleeperOnly {
                                current: sy.current,
                                waker: sy.waker,
                                duration: sy.duration,
                            }),
                        },
                    );
                } else {
                    // Reconstruct the destructed value.
                    self.sleepers.insert(
                        id.to_owned(),
                        SleepInfo {
                            unblock,
                            state: SleepState::Syncing(sy),
                        },
                    );
                }
            }
            Some(state) => {
                self.sleepers.insert(id.to_owned(), state);
            }
        };
    }
}

impl Clock for FakeClock {
    fn now(&self) -> SystemTime {
        let inner = self.inner.lock().unwrap();
        inner.now
    }

    fn sleep(&self, delay: Duration, id: &str) -> impl Future<Output = ()> + '_ {
        let mut inner = self.inner.lock().unwrap();
        inner.on_new(id.to_owned(), delay);
        let inner_sleeper = SleeperState {
            id: id.to_owned(),
            parent: Arc::downgrade(&self.inner),
        };
        Sleeper {
            inner: Arc::new(Mutex::new(inner_sleeper)),
        }
    }
}

// Future that pretends to wait for a set duration.
struct Sleeper {
    inner: Arc<Mutex<SleeperState>>,
}

// Internal state of the Sleeper, registered with the Handler.
struct SleeperState {
    id: String,
    parent: Weak<Mutex<ClockState>>,
}

impl Future for Sleeper {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.inner.lock().unwrap();
        match inner.parent.upgrade() {
            Some(lock) => {
                let mut handler = lock.lock().unwrap();
                let ready = handler.on_poll(&inner.id, cx.waker().clone());
                if ready {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
            // If there is nobody to wake this up, we have no other option than
            // completing right away.
            None => Poll::Ready(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn set_time() {
        // Handler can set the time for the fake clock.
        let (clock, handler) = Handler::new();

        // Default value.
        assert_eq!(clock.now(), SystemTime::UNIX_EPOCH);

        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(123);
        handler.set_now(now);
        assert_eq!(clock.now(), now);
    }

    #[test(tokio::test)]
    async fn wait_before_sleep() -> anyhow::Result<()> {
        // Handle the case where the clock watcher starts waiting before the sleep
        // call is issued.
        let (clock, handler) = Handler::new();
        let ah = handler.clone();
        let hh = tokio::task::spawn(async move {
            let delay = ah.wait("for_test", 0).await;
            assert_eq!(delay, Duration::from_secs(123));

            let incarnation = ah.unblock_once("for_test");
            assert_eq!(incarnation, 1); // We unblocked the first sleep
        });
        while !handler.has_waiter("for_test") {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        clock.sleep(Duration::from_secs(123), "for_test").await;

        hh.await?;
        Ok(())
    }

    #[test(tokio::test)]
    async fn wait_after_sleep() -> anyhow::Result<()> {
        // Handle the case where the clock watcher starts waiting after the sleep
        // call is issued.
        let (clock, handler) = Handler::new();
        let hh = tokio::task::spawn(async move {
            clock.sleep(Duration::from_secs(123), "for_test").await;
        });
        while !handler.has_sleeper("for_test") {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let delay = handler.wait("for_test", 0).await;
        assert_eq!(delay, Duration::from_secs(123));

        let incarnation = handler.unblock_once("for_test");
        assert_eq!(incarnation, 1); // We unblocked the first sleep

        hh.await?;
        Ok(())
    }

    #[test(tokio::test)]
    async fn wait_across_sleeps() -> anyhow::Result<()> {
        // Handle the case where the clock watcher starts on one sleep instance,
        // sleep is re-started, and the watcher finally completes.
        let (clock, handler) = Handler::new();
        let hh = tokio::task::spawn(async move {
            // First sleep incarnation. It'll be dropped as soon as the main
            // thread triggers the oneshot.
            tokio::select! {
                _ = clock.sleep(Duration::from_secs(123), "for_test") => {},
                _ = tokio::time::sleep(Duration::from_millis(1)) => {},
            }
            clock.sleep(Duration::from_secs(321), "for_test").await;
        });
        let delay = handler.wait("for_test", 2).await;
        assert_eq!(delay, Duration::from_secs(321));

        let incarnation = handler.unblock_once("for_test");
        assert_eq!(incarnation, 2);

        hh.await?;

        Ok(())
    }
}
