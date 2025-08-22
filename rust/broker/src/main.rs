use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use server::Server;
use settings::process;

mod server;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    home: Option<PathBuf>,
}

/// Runs a Broker binary, which is in charge of mediating between Sources and Sinks.
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let settings =
        server::settings::load(args.home).context("Failed to load the Broker settings")?;
    process::init(settings.process())?;

    let server = Server::builder()
        .settings(&settings)?
        .build()
        .context("Failed to configure the Broker")?;
    server.serve().await.context("Failed to run the Broker")?;

    Ok(())
}
