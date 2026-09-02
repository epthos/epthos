//! Helpers to execute code following the Actor pattern, where one
//! part is running in various tokio::task runners (regular async, blocking
//! or limited to a single LocalSet), and interactions go through a mpsc
//! channel. The helpers ensure that default logging and error handling is
//! done consistently.
//!
//! ```
//! use actor::{Async, Result, Shutdown, Tracker};
//! use tokio_util::sync::CancellationToken;
//! use tokio::sync::mpsc::Receiver;
//! use tokio::sync::oneshot::{self, Sender as SingleSender};

//! let token = CancellationToken::new();
//! let mut tracker = Tracker::new(token.clone());

//! # tokio_test::block_on (async {
//!
//! let tx = tracker.start(Counter::default(), "Task");

//! let (count_tx, count_rx) = oneshot::channel();
//! tx.send(Ops::Count(count_tx)).await.unwrap();
//! let count = count_rx.await.unwrap();
//!
//! assert_eq!(1, count);
//!
//! token.cancel();
//! assert!(actor::combine(tracker.run().await).is_ok());
//!
//! # }); // block_on
//!
//! #[derive(Default)]
//! struct Counter {}
//!
//! enum Ops {
//!     Count(SingleSender<i32>)
//! }
//! impl Async for Counter {
//!     type Operation = Ops;
//!
//!     async fn run(self, mut rx: Receiver<Ops>) -> Result<()> {
//!         let mut count = 0;
//!         loop {
//!             let Ops::Count(tx)  = rx.recv().await.shutdown()?;
//!             count += 1;
//!             tx.send(count).shutdown()?;
//!         }
//!     }
//! }
//! ```
//!
//! The main driver is expected to instantiate Actors::new(), and pass the
//! manager for each actor to register its runner.

use error_collection::Errors;
use std::result::Result as StdResult;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::{JoinError, JoinSet, LocalSet},
};
use tokio_util::sync::{CancellationToken, DropGuard};

pub mod router;

/// [`Async`] actor runs a "Send" future.
pub trait Async {
    /// Typically, an enum with all supported operations.
    type Operation;

    /// Perform the work. When passed to [`Tracker::start()`], this will
    /// be executed on the default tokio runtime.
    fn run(self, rx: Receiver<Self::Operation>) -> impl Future<Output = Result<()>> + Send;
}

/// [`Local`] actor runs a "!Send" future.
pub trait Local {
    /// Typically, an enum with all supported operations.
    type Operation;

    /// Perform the work. When passed to [`Tracker::start_local()`], this will
    /// be executed in a single thread of the tokio runtime.
    fn run(self, rx: Receiver<Self::Operation>) -> impl Future<Output = Result<()>>;
}

/// [`Blocking`] actor runs a blocking thread on the tokio runtime.
pub trait Blocking {
    /// Typically, an enum with all supported operations.
    type Operation;

    /// Perform the work. When passed to [`Tracker::start_blocking()`], this
    /// will be executed in a single blocking thread. The function must check
    /// the token and immediatley return when it's cancelled.
    fn run(self, rx: Receiver<Self::Operation>, token: CancellationToken) -> Result<()>;
}

/// The [`Tracker`] is resposible for coordinating the shutdown of all the running
/// actors in the system.
pub struct Tracker {
    guard: DropGuard,
    set: JoinSet<()>,
    err: Vec<StdResult<(), JoinError>>,
}

impl Tracker {
    /// Create a new [`Tracker`] that will shut down all the actors in its care
    /// when the provided `token` is cancelled.
    pub fn new(token: CancellationToken) -> Self {
        let guard = token.drop_guard();
        Tracker {
            guard,
            set: JoinSet::new(),
            err: Vec::new(),
        }
    }

    /// Start an [`Async`] actor.
    pub fn start<A>(&mut self, actor: A, name: &'static str) -> Sender<A::Operation>
    where
        A: Async + Send + 'static,
        A::Operation: Send + 'static,
    {
        let token = self.guard.token().child_token();
        let (tx, rx) = mpsc::channel::<A::Operation>(1);
        self.set.spawn(async move {
            tracing::info!("{}: starting", name);
            tokio::select! {
                _ = token.cancelled() => { tracing::debug!("{}: cancelled", name); },
                result = actor.run(rx) => collapse(result, name),
            }
            tracing::info!("{}: done", name);
        });
        tx
    }

    /// Start a [`Blocking`] actor.
    pub fn start_blocking<A>(&mut self, actor: A, name: &'static str) -> Sender<A::Operation>
    where
        A: Blocking + Send + 'static,
        A::Operation: Send + 'static,
    {
        let token = self.guard.token().child_token();
        let (tx, rx) = mpsc::channel::<A::Operation>(1);
        self.set.spawn_blocking(move || {
            tracing::info!("{}: starting", name);
            collapse(actor.run(rx, token), name);
            tracing::info!("{}: done", name);
        });
        tx
    }

    /// Start a [`Local`] actor.
    pub async fn start_local<A, F>(&mut self, f: F, name: &'static str) -> Sender<A::Operation>
    where
        F: FnOnce() -> A + Send + 'static,
        A: Local + 'static,
        A::Operation: Send + 'static,
    {
        let token = self.guard.token().child_token();
        let (dm_tx, dm_rx) = std::sync::mpsc::sync_channel::<Sender<A::Operation>>(0);

        // The whole setup here is intended to start a LocalSet in a dedicated thread
        // so we can benefit from controlled parallelism between the subsystems. The
        // thread is fully self-contained: it owns its runtime and its actor, so
        // nothing here needs to borrow from `self` (which must stay `'static` to
        // cross into spawn_blocking).
        let thread = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("thread");
            let local = LocalSet::new();

            rt.block_on(local.run_until(async move {
                let actor = f();
                let (tx, rx) = mpsc::channel::<A::Operation>(1);
                dm_tx
                    .send(tx)
                    .unwrap_or_else(|_| panic!("{}: failed to pass tx back", name));

                tracing::info!("{}: starting", name);
                tokio::select! {
                    _ = token.cancelled() => {},
                    result = actor.run(rx) => collapse(result, name),
                }
                tracing::info!("{}: done", name);
            }))
        });

        // Wait for the actor's Sender off the async runtime, so we never block a
        // worker thread on the std mpsc recv.
        let creation = tokio::task::spawn_blocking(move || dm_rx.recv()).await;

        // Fold the dedicated thread's completion into the shared JoinSet so
        // `run()` still observes its panics/errors, without ever handing the
        // non-Send actor or the JoinSet itself across the thread boundary.
        self.set.spawn(async move {
            if let Err(e) = thread.await {
                if e.is_panic() {
                    std::panic::resume_unwind(e.into_panic());
                }
            }
        });

        match creation {
            Ok(Ok(tx)) => tx,
            Ok(Err(_)) | Err(_) => {
                // Actor thread died before sending its handle back; the error is
                // already recorded via the JoinSet task spawned above.
                let (tx, _rx) = mpsc::channel::<A::Operation>(1);
                tx
            }
        }
    }

    /// Run all the actors to completion, shutting down all of them as soon as one
    /// completes or the token is cancelled.
    pub async fn run(mut self) -> Vec<StdResult<(), JoinError>> {
        // Failure during initialization: no work can be done.
        if !self.err.is_empty() {
            self.guard.token().cancel();
        }
        while let Some(result) = self.set.join_next().await {
            self.guard.token().cancel();
            self.err.push(result);
        }
        self.err
    }
}

/// Turns the result of [`Tracker::run`] into a single error.
pub fn combine(results: Vec<StdResult<(), JoinError>>) -> anyhow::Result<()> {
    let mut errors = Errors::new();
    for err in results {
        errors.collect(err);
    }
    errors.as_result()
}

/// Convenience errors to help with actor shutdowns: some errors are necessarily
/// caused by _other_ actors shutting down, like channel errors. Return the
/// [`Terminal::Shutdown`] error for those, to avoid unnecessary logging of the
/// problem. See [`Shutdown::shutdown`] for a convenient way to map to that type.
#[derive(thiserror::Error, Debug)]
pub enum Terminal {
    #[error("Shutting down cleanly")]
    Shutdown,
    #[error("Shutting down on fatal error")]
    Internal(#[from] anyhow::Error),
}

pub type Result<T> = StdResult<T, Terminal>;

/// Helper trait to cleanly shutdown on benign errors. For instance
/// `receiver.recv().shutdown()?` will map the `None` of a [`Receiver::recv`]
/// call to indicate that a required peer is now gone.
pub trait Shutdown<T, E> {
    fn shutdown(self) -> Result<T>;
}

impl<T, E> Shutdown<T, E> for StdResult<T, E> {
    fn shutdown(self) -> Result<T> {
        match self {
            Ok(ok) => Ok(ok),
            Err(_) => Err(Terminal::Shutdown),
        }
    }
}

impl<T> Shutdown<T, Terminal> for Option<T> {
    fn shutdown(self) -> Result<T> {
        match self {
            Some(ok) => Ok(ok),
            None => Err(Terminal::Shutdown),
        }
    }
}

/// Unwraps a `Result<T>`, returning `()` on `Shutdown` and panicking on `Internal`.
#[macro_export]
macro_rules! terminal {
    ($e:expr) => {
        match $e {
            Ok(ok) => ok,
            Err($crate::Terminal::Shutdown) => return Ok(()),
            Err($crate::Terminal::Internal(err)) => panic!("{err:?}"),
        }
    };
}

/// Helper that goes from the actor-specific [`Terminal`] error to the simpler
/// status of a [`JoinHandle`].
pub(crate) fn collapse(result: Result<()>, name: &'static str) {
    match result {
        Ok(()) => (),
        Err(Terminal::Shutdown) => (),
        Err(Terminal::Internal(internal)) => panic!("{} failed: {}", name, &internal),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::bail;
    use itertools::Itertools;
    use std::{future::pending, thread::sleep, time::Duration};

    #[test_log::test(tokio::test)]
    async fn clean_termination() -> anyhow::Result<()> {
        let mut a = Tracker::new(CancellationToken::new());
        a.start(
            Run {
                f: || async { Ok(()) },
            },
            "async",
        );
        a.start_local(
            || Run {
                f: || async { Ok(()) },
            },
            "local",
        )
        .await;
        a.start_blocking(Run { f: |_token| Ok(()) }, "blocking");

        combine(a.run().await)?;
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn termination_hatch() -> anyhow::Result<()> {
        let mut a = Tracker::new(CancellationToken::new());
        a.start(
            Run {
                f: || async { Err(Terminal::Shutdown) },
            },
            "test",
        );
        a.start_local(
            || Run {
                f: || async { Err(Terminal::Shutdown) },
            },
            "test",
        )
        .await;
        a.start_blocking(
            Run {
                f: |_token| Err(Terminal::Shutdown),
            },
            "blocking",
        );
        combine(a.run().await)?;
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn direct_panic() -> anyhow::Result<()> {
        {
            let mut a = Tracker::new(CancellationToken::new());
            a.start(
                Run {
                    f: || async {
                        panic!("boom");
                    },
                },
                "test",
            );
            validate_panic(a).await?;
        }
        {
            let mut a = Tracker::new(CancellationToken::new());
            a.start_local(
                || Run {
                    f: || async {
                        panic!("boom");
                    },
                },
                "test",
            )
            .await;
            validate_panic(a).await?;
        }
        {
            let mut a = Tracker::new(CancellationToken::new());
            a.start_blocking(
                Run {
                    f: |_token| panic!("boom"),
                },
                "blocking",
            );
            validate_panic(a).await?;
        }
        Ok(())
    }

    async fn validate_panic(a: Tracker) -> anyhow::Result<()> {
        let result = a.run().await.into_iter().exactly_one()?;
        if let Err(e) = result {
            assert!(e.is_panic());
        } else {
            bail!("Expected an error");
        }
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn internal_to_panic() -> anyhow::Result<()> {
        {
            let mut a = Tracker::new(CancellationToken::new());
            a.start(
                Run {
                    f: || async { Err(anyhow::anyhow!("boom").into()) },
                },
                "test",
            );
            validate_panic(a).await?;
        }
        {
            let mut a = Tracker::new(CancellationToken::new());
            a.start_local(
                || Run {
                    f: || async { Err(anyhow::anyhow!("boom").into()) },
                },
                "test",
            )
            .await;
            validate_panic(a).await?;
        }
        {
            let mut a = Tracker::new(CancellationToken::new());
            a.start_blocking(
                Run {
                    f: |_token| Err(anyhow::anyhow!("boom").into()),
                },
                "blocking",
            );
            validate_panic(a).await?;
        }
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn use_token_to_cancel() -> anyhow::Result<()> {
        let token = CancellationToken::new();
        let mut a = Tracker::new(token.clone());
        a.start(
            Run {
                f: || async { pending().await },
            },
            "test",
        );
        a.start_local(
            || Run {
                f: || async { pending().await },
            },
            "test",
        )
        .await;
        a.start_blocking(
            Run {
                f: |token: CancellationToken| {
                    while !token.is_cancelled() {
                        sleep(Duration::from_millis(1));
                    }
                    Ok(())
                },
            },
            "blocking",
        );
        token.cancel();
        combine(a.run().await)?;
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn one_termination_triggers_others() -> anyhow::Result<()> {
        let token = CancellationToken::new();
        let mut a = Tracker::new(token.clone());
        a.start_local(
            || Run {
                f: || async { pending().await },
            },
            "blocked",
        )
        .await;
        a.start(
            Run {
                f: || async { Ok(()) },
            },
            "done",
        );
        combine(a.run().await)?;
        Ok(())
    }

    enum NoOp {}
    struct Run<F> {
        f: F,
    }

    impl<F, Fut> Async for Run<F>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<()>> + Send,
    {
        type Operation = NoOp;

        async fn run(self, _rx: Receiver<NoOp>) -> Result<()> {
            (self.f)().await
        }
    }

    impl<F, Fut> Local for Run<F>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<()>> + Send,
    {
        type Operation = NoOp;

        async fn run(self, _rx: Receiver<NoOp>) -> Result<()> {
            (self.f)().await
        }
    }

    impl<F> Blocking for Run<F>
    where
        F: FnOnce(CancellationToken) -> Result<()> + Send,
    {
        type Operation = NoOp;

        fn run(self, _rx: Receiver<NoOp>, token: CancellationToken) -> Result<()> {
            (self.f)(token)
        }
    }
}
