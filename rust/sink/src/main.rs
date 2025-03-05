use anyhow::{Context, Result};
use server::Server;
use settings::process;

mod server;

#[tokio::main]
async fn main() -> Result<()> {
    let settings = sink_settings::load().context("Failed to load the Sink settings")?;
    let _guard = process::init(settings.process())?;

    let server = Server::builder()
        .settings(&settings)
        .context("Failed to build the Sink configuration")?
        .build()
        .context("Failed to configure the Sink")?;
    server.serve().await.context("Failed to run the Sink")?;

    Ok(())
}
