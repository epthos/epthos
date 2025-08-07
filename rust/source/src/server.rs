use self::{builder::Builder, peer::Peer};
use crate::{datamanager, filemanager};
use anyhow::{Context, Result};
use std::path::PathBuf;

mod builder;
mod peer;

/// A Source server, which watches the filesystem and backs data up to a Sink.
pub struct Server<P: Peer> {
    roots: Vec<PathBuf>,
    _peer: P,
    manager: filemanager::FileManager,
    datamanager: datamanager::DataManager,
}

pub fn builder() -> Builder {
    Builder::default()
}

impl<P: Peer> Server<P> {
    /// Run the server.
    pub async fn serve(mut self) -> Result<()> {
        // Set up the file store.
        self.manager.set_roots(self.roots).await?;
        // We stop the server at the first failure of a submodule, as there is
        // no real way to continue at the moment.
        // TODO: maybe do an orderly shutdown of the remaining subsystems?
        tokio::select! {
            r = self.manager.monitor() => {
                r.context("FileManager thread")?.context("FileManager status")?;
            }
            r = self.datamanager.monitor() => {
                r.context("DataManager thread")?.context("DataManager status")?;
            }
        }
        self.manager.monitor().await??;
        Ok(())
    }
}
