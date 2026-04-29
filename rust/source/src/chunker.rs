//! Async API to fetch chunks for backups.
//!
//! Chunking can be interrupted and resumed safely.

use crate::{
    disk::{self, Disk},
    model::Chunk,
};
use std::path::PathBuf;
use tokio::sync::mpsc;

pub struct Address {
    pub file: PathBuf,
    pub offset: usize,
}

pub struct ChunkOp {
    pub address: Address,
    pub msg: ChunkMsg,
}

pub enum ChunkMsg {
    Next(Chunk),
    Done,
    Error(disk::DiskError),
}

pub trait Chunker {
    /// Continue chunking from address. Results are sent to `tx`; the caller
    /// owns the corresponding receiver and may share `tx` across multiple calls.
    fn chunk(&self, address: Address, tx: mpsc::Sender<ChunkOp>);
}

pub struct RealChunker<D: Disk + Clone> {
    disk: D,
}

impl<D> RealChunker<D>
where
    D: Disk + Clone + Send + 'static,
{
    pub fn new(disk: D) -> RealChunker<D> {
        RealChunker { disk }
    }
}

impl<D> Chunker for RealChunker<D>
where
    D: Disk + Clone + Send + 'static,
{
    fn chunk(&self, address: Address, tx: mpsc::Sender<ChunkOp>) {
        let disk = self.disk.clone();
        // This is using regular threads as the disk IO is not async anyways.
        std::thread::spawn(move || {
            let start = address.offset;
            // Clone file so the iterator borrows the clone, not address.file,
            // leaving address.file free to move into ChunkOp throughout.
            let file_path = address.file.clone();
            let iter = match disk.chunk(&file_path, start) {
                Ok(iter) => iter,
                Err(err) => {
                    tx.blocking_send(ChunkOp { address, msg: ChunkMsg::Error(err) }).unwrap();
                    return;
                }
            };
            let mut offset = start;
            for chunk in iter {
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(err) => {
                        tx.blocking_send(ChunkOp { address: Address { file: address.file.clone(), offset }, msg: ChunkMsg::Error(err) }).unwrap();
                        return;
                    }
                };
                offset = chunk.next();
                tx.blocking_send(ChunkOp { address: Address { file: address.file.clone(), offset }, msg: ChunkMsg::Next(chunk) }).unwrap();
            }
            // iter consumed; file_path no longer borrowed.
            tx.blocking_send(ChunkOp { address: Address { file: address.file, offset }, msg: ChunkMsg::Done }).unwrap();
        });
    }
}
