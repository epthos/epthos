//! Async API to fetch chunks for backups.
//!
//! Chunking can be interrupted and resumed safely.

use crate::model::Chunk;
use std::path::PathBuf;
use tokio::sync::mpsc;

pub struct Address {
    file: PathBuf,
}

pub trait Chunker {
    /// Continue chunking from address. Each generated chunk returns
    /// the address needed to resume if interrupted.
    fn chunk(&self, address: Address) -> anyhow::Result<mpsc::Receiver<(Chunk, Address)>>;
}
