use self::{builder::Builder, peer::Peer};
use crate::{datamanager, filemanager};
use anyhow::Result;
use std::path::PathBuf;

mod builder;
mod peer;

/// A Source server, which watches the filesystem and backs data up to a Sink.
pub struct Server<P: Peer> {
    roots: Vec<PathBuf>,
    _peer: P,
    manager: filemanager::FileManager,
    datamanager: datamanager::Datamanager,
}

pub fn builder() -> Builder {
    Builder::default()
}

impl<P: Peer> Server<P> {
    /// Run the server.
    pub async fn serve(mut self) -> Result<()> {
        // Set up the file store.
        self.manager.set_roots(self.roots).await?;
        self.manager.monitor().await??;
        Ok(())
    }
}
