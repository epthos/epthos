//! Helper to run singly-threaded async code.

use anyhow::Context;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::{JoinHandle, LocalSet},
};

// The singly-threaded code is accessed by sending Operations.
pub trait Solo {
    /// Typically, an enum with all supported operations.
    type Operation;

    // Perform the work. When using start() below, this will be executed in a
    // single thread.
    async fn run(&mut self, rx: Receiver<Self::Operation>) -> anyhow::Result<()>;
}

/// Spawn a new thread and start the Solo instance provided by f(). The returned
/// values are how the Solo runner is controlled. "name" is used for unambiguous
/// logging.
/// It's necessary to use a provider function as the Solo instance itself is typically
/// not Send, or we would not need to run it on a single thread in the first place.
pub fn start<F, I>(
    f: F,
    name: &'static str,
) -> anyhow::Result<(Sender<I::Operation>, JoinHandle<anyhow::Result<()>>)>
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

    let handle = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let (tx, rx) = mpsc::channel::<I::Operation>(1);
        dm_tx
            .send(tx)
            .context(format!("{}: failed to pass tx back", name))?;
        let local = LocalSet::new();

        rt.block_on(local.run_until(async {
            let mut async_work = f();
            let handle = local.spawn_local(async move {
                tracing::info!("{} is starting", name);
                let result: anyhow::Result<()> = async_work.run(rx).await.into();
                match &result {
                    Ok(_) => {
                        tracing::info!("{} has shut down", name);
                    }
                    Err(err) => {
                        tracing::error!("{} failed: {:?}", name, err);
                    }
                }
                result
            });
            handle.await?
        }))
    });
    let tx = dm_rx
        .recv()
        .context(format!("{}: failed to receive Sender", name))?;
    Ok((tx, handle))
}
