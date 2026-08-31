use anyhow::{Context, Result};
use server::Server;
use settings::process;
use tokio_util::sync::CancellationToken;

mod server;

#[tokio::main]
async fn main() -> Result<()> {
    let settings = sink_settings::load().context("Failed to load the Sink settings")?;
    process::init(settings.process())?;
    let token = CancellationToken::new();
    let server = Server::builder(token.clone())
        .settings(&settings)
        .context("Failed to build the Sink configuration")?
        .build()
        .context("Failed to configure the Sink")?;

    let mut serving = Box::pin(server.serve());
    loop {
        tokio::select! {
            // External shutdown trigger.
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Ctrl-C, shutting down.");
                token.cancel();
            }
            // Shutdown confirmation.
            done = &mut serving => {
                done.context("Server completed")?;
                break;
            }
        }
    }
    tracing::info!("Clean shutdown complete");
    Ok(())
}
