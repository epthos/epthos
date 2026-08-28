//! Helper to run singly-threaded async code.

use anyhow::Context;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::{JoinHandle, LocalSet},
};

use crate::fatal::{self, Fatal};

// The singly-threaded code is accessed by sending Operations.
pub trait Solo {
    /// Typically, an enum with all supported operations.
    type Operation;

    // Perform the work. When using start() below, this will be executed in a
    // single thread.
    async fn run(self, rx: Receiver<Self::Operation>) -> fatal::Result<()>;
}

pub struct Handle<O> {
    pub sender: Sender<O>,
    pub handle: JoinHandle<()>,
}

/// Spawn a new thread and start the Solo instance provided by f(). The returned
/// values are how the Solo runner is controlled. "name" is used for unambiguous
/// logging.
/// It's necessary to use a provider function as the Solo instance itself is typically
/// not Send, or we would not need to run it on a single thread in the first place.
pub fn start<F, I>(f: F, name: &'static str) -> anyhow::Result<Handle<I::Operation>>
where
    F: FnOnce() -> I + Send + 'static,
    I: Solo + 'static,
    I::Operation: Send + 'static,
{
    let (dm_tx, dm_rx) = std::sync::mpsc::sync_channel::<Sender<I::Operation>>(0);
    // The whole setup here is intended to start a LocalSet in a dedicated thread
    // so we can benefit from controlled parallelism between the subsystems.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let handle = tokio::task::spawn_blocking(move || -> () {
        // We let the thread fail altogether in case of issues, so we don't need to
        // handle two failure cases that are equivalent.
        let (tx, rx) = mpsc::channel::<I::Operation>(1);
        dm_tx
            .send(tx)
            .unwrap_or_else(|_| panic!("{}: failed to pass tx back", name));
        let local = LocalSet::new();

        rt.block_on(local.run_until(async {
            let async_work = f();
            let handle = local.spawn_local(async move {
                tracing::info!("{} is starting", name);
                async_work.run(rx).await
            });
            // Only one of these ever actually panics: either we resume the
            // original panic unchanged (no new panic raised, so no risk of
            // panicking while already unwinding, which would abort the
            // whole process instead of just failing this thread), or we
            // raise a single fresh panic from a clean (non-unwinding) result.
            match handle.await {
                // Clean shutdown, the run function simply returned or used the
                // quick bail-out option.
                Ok(Ok(())) | Ok(Err(Fatal::Shutdown)) => {}
                // The run function returned an internal error.
                Ok(Err(err)) => panic!("Worker {} failed: {:?}", name, err),
                Err(join_err) if join_err.is_panic() => {
                    std::panic::resume_unwind(join_err.into_panic())
                }
                Err(join_err) => panic!("Worker {} cancelled: {:?}", name, join_err),
            }
        }))
    });
    let tx = dm_rx
        .recv()
        .context(format!("{}: failed to receive Sender", name))?;
    Ok(Handle { sender: tx, handle })
}
