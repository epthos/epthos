use ::settings::process;
use anyhow::{Context, Result};
use std::{path::Path, sync::Arc};

mod clock;
mod datamanager;
mod datastore;
mod disk;
mod filemanager;
mod filestore;
mod model;
mod server;
mod solo;
mod watcher;

#[cfg(test)]
mod fake_clock;

// Creates a new Source key and saves it to the specified path.
fn new_source(path: &Path, rnd: &crypto::Random) -> anyhow::Result<crypto::key::Durable> {
    let durable = rnd.generate_root_key()?;
    durable.to_file(path)?;
    Ok(durable)
}

/// Runs a Source binary, which is in charge of a user's data source.
#[tokio::main]
async fn main() -> Result<()> {
    let settings = source_settings::load().context("Failed to load the Source settings")?;
    let _guard = process::init(settings.process())?;

    let rnd = Arc::new(crypto::Random::new());
    // TODO: we should be very conservative about regenerating the key, in case something requires
    // manual intervention.
    let durable = crypto::key::Durable::from_file(settings.backup().keyfile())
        .or_else(|_| new_source(settings.backup().keyfile(), rnd.as_ref()))?;
    let source_key = crypto::Keys::new(durable);

    let server = server::builder()
        .settings(&settings)
        .crypto(rnd, source_key)
        .build()
        .await
        .context("Failed to configure the Source")?;
    server.serve().await.context("Server completed")?;
    Ok(())
}
