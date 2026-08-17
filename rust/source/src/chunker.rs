//! Async API to fetch chunks for backups.
//!
//! Chunking can be interrupted and resumed safely.

use crate::{
    disk::{self, Disk},
    model::{Chunk, FileSize, ModificationTime},
};
use std::path::PathBuf;
use tokio::sync::mpsc;

#[derive(Debug)]
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
    Done(FileSize, ModificationTime),
    Error(disk::DiskError),
}

pub trait Chunker {
    /// Chunk a file, starting at `address`. Chunking will proceed until
    /// the file is read entirely and we return a `ChunkMsg::Done` or an
    /// error occurs.
    /// The caller can abort chunking by dropping the receiver end of the
    /// mpsc channel.
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
        std::thread::spawn(move || -> Option<()> {
            let start = address.offset;
            // Step 1: initialize the disk chunker at the right locaton.
            //
            // We clone file so the iterator borrows the clone, not address.file,
            // leaving address.file free to move into ChunkOp throughout.
            let file_path = address.file.clone();
            let iter = match disk.chunk(&file_path, start) {
                Ok(iter) => iter,
                Err(err) => {
                    return try_send(
                        &tx,
                        ChunkOp {
                            address,
                            msg: ChunkMsg::Error(err),
                        },
                    );
                }
            };
            // Step 2: we return all the chunks in the file. The sender provides the
            // backpressure if we read too fast. Similarly, this is interrupted by
            // dropping the receiver on the other side.
            let mut offset = start;
            for chunk in iter {
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(err) => {
                        return try_send(
                            &tx,
                            ChunkOp {
                                address: Address {
                                    file: address.file.clone(),
                                    offset,
                                },
                                msg: ChunkMsg::Error(err),
                            },
                        );
                    }
                };
                offset = chunk.next();
                try_send(
                    &tx,
                    ChunkOp {
                        address: Address {
                            file: address.file.clone(),
                            offset,
                        },
                        msg: ChunkMsg::Next(chunk),
                    },
                )?;
            }
            // Step 3: finalize the chunking by returning the latest file metadata.
            let (fsize, mtime) = match disk.metadata(&file_path) {
                Ok(m) => m,
                Err(err) => {
                    return try_send(
                        &tx,
                        ChunkOp {
                            address: Address {
                                file: address.file,
                                offset,
                            },
                            msg: ChunkMsg::Error(err),
                        },
                    );
                }
            };
            try_send(
                &tx,
                ChunkOp {
                    address: Address {
                        file: address.file,
                        offset,
                    },
                    msg: ChunkMsg::Done(fsize, mtime),
                },
            )
        });
    }
}

// Helper method to log issues on sending.
fn try_send(tx: &mpsc::Sender<ChunkOp>, op: ChunkOp) -> Option<()> {
    if let Err(e) = tx.blocking_send(op) {
        tracing::info!(
            "chunk receiver gone at {:?}+{}, stopping",
            e.0.address.file,
            e.0.address.offset
        );
        return None;
    }
    Some(())
}
