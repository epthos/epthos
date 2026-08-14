//! The DataManager performs backup creation.

use crate::{
    chunker::{Address, ChunkMsg, ChunkOp, Chunker, RealChunker},
    datastore::Datastore,
    disk::{self, Disk, Snapshot},
    filestore::HashUpdate,
    model::FileHashBuilder,
    solo::{self, Solo},
};
use anyhow::{Context, bail};
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
};
use tokio::{
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

/// This is the front API, called by the filemanager to trigger backups
/// when they are needed. The system defines the pushback mechanism, as
/// backup slots are made available based on the capacity of the datamanager.
pub trait DataManager {
    type Slot: BackupSlot;

    /// Provides the ongoing backups and the receiver for their completion.
    /// This is to be invoked only once at initialization, to restore the
    /// client's internal state.
    async fn in_flight(&mut self) -> anyhow::Result<Vec<InFlight>>;

    /// Provides the receiver of new backup slots.
    fn backup_slots(&mut self) -> &mut Receiver<Self::Slot>;
}

/// A BackupSlot is a slot for an additional backup that can be handled by
/// the system.
pub trait BackupSlot {
    /// Enqueue a backup for the specified path. Upon backup completion (either
    /// successful or not), the oneshot receiver will be triggered with the result.
    async fn enqueue(self, path: PathBuf) -> anyhow::Result<oneshot::Receiver<BackupResult>>;
}

/// Overall result for a backup request. When successful, the hash update matches
/// the hash that would have been computed just by reading the file for a deep
/// check.
#[derive(Debug)]
pub struct BackupResult {
    pub path: PathBuf,
    pub update: HashUpdate,
}

/// Description of an ongoing backup, returned by DataManager::in_flight to let
/// the caller get receivers for backups that are still being worked on.
#[derive(Debug)]
pub struct InFlight {
    pub path: PathBuf,
    pub recv: oneshot::Receiver<BackupResult>,
}

/// Create a new production data manager operating on the provided
/// database path.
pub async fn new(
    db: &Path,
    token: CancellationToken,
) -> anyhow::Result<(DataManagerImpl, JoinHandle<()>)> {
    let disk = disk::new()?;
    DataManagerImpl::new(Datastore::new(db)?, disk, token).await
}

// =============================================================================

// The implementation uses the Solo helper to run the single thread with the db
// interactions following the actor pattern.
pub struct DataManagerImpl {
    tx: Sender<Op>,
    slot_rx: Receiver<BackupSlotImpl>,
}

impl DataManagerImpl {
    async fn new<D>(
        store: Datastore,
        disk: D,
        token: CancellationToken,
    ) -> anyhow::Result<(DataManagerImpl, JoinHandle<()>)>
    where
        D: Disk + Clone + Send + 'static,
    {
        let runner_token = token.clone();
        let f = move || Runner {
            store,
            disk,
            token: runner_token,
        };
        let handle = solo::start(f, "DataManager")?;
        // Get ready to receive backup slots from the runner.
        let (slot_tx, slot_rx) = mpsc::channel(1);
        handle
            .sender
            .send(Op::Init((slot_tx, handle.sender.clone())))
            .await
            .context("Runner failed")?;
        Ok((
            DataManagerImpl {
                tx: handle.sender,
                slot_rx,
            },
            handle.handle,
        ))
    }
}

impl DataManager for DataManagerImpl {
    type Slot = BackupSlotImpl;

    fn backup_slots(&mut self) -> &mut Receiver<BackupSlotImpl> {
        &mut self.slot_rx
    }

    async fn in_flight(&mut self) -> anyhow::Result<Vec<InFlight>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Op::InFlight(tx)).await?;
        rx.await.context("from DataManager")
    }
}

/// Internal operations supported by the Runner.
#[derive(Debug)]
enum Op {
    // Initialize the Runner, which needs to know how to return backup slots.
    Init((Sender<BackupSlotImpl>, Sender<Op>)),
    // Enqueue a new backup. This is only performed by the backup slot, so the
    // user of the DataManager trait can't control the parallelism.
    Enqueue(PathBuf, oneshot::Sender<BackupResult>),
    // Expected once, at startup, to synchronize the state of existing backups.
    InFlight(oneshot::Sender<Vec<InFlight>>),
}

pub struct BackupSlotImpl {
    tx: Sender<Op>,
}

impl BackupSlot for BackupSlotImpl {
    async fn enqueue(self, path: PathBuf) -> anyhow::Result<oneshot::Receiver<BackupResult>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Op::Enqueue(path, tx))
            .await
            .context("Runner failed")?;
        Ok(rx)
    }
}

struct Runner<D>
where
    D: Disk + Clone + Send + 'static,
{
    store: Datastore,
    disk: D,
    token: CancellationToken,
}

struct PendingBackup {
    path: PathBuf,
    tx: oneshot::Sender<BackupResult>,
    hash_builder: FileHashBuilder,
}

impl<D: Disk + Clone + Send + 'static> Solo for Runner<D> {
    type Operation = Op;

    async fn run(self, mut rx: Receiver<Op>) -> anyhow::Result<()> {
        let chunker = RealChunker::new(self.disk.clone());
        let (chunk_tx, mut chunk_rx) = mpsc::channel::<ChunkOp>(1);

        // Initial handshake: the handler must call Init.
        let Some(Op::Init((slot_sender, op_sender))) = rx.recv().await else {
            bail!("Initialization failed");
        };
        // remaining should come from actual capacity vs currently pending backups.
        let mut remaining = 1;
        let mut pending = VecDeque::new();
        loop {
            tracing::debug!(
                "starting with {} remaining, {} pending",
                remaining,
                pending.len()
            );
            tokio::select! {
                // Try to hand a backup slot to the caller, if there is capacity.
                // We use reserve() as selecting on send would lose the message.
                permit = slot_sender.reserve(), if remaining > 0 => {
                    match permit {
                        Ok(permit) => {
                            tracing::debug!("sending one slot");
                            permit.send(BackupSlotImpl { tx: op_sender.clone() });
                            remaining -= 1;
                        },
                        Err(e) => return Err(e).context("receiver is gone"),
                    }
                }
                // Process an incoming request from the handler or any backup slot.
                op = rx.recv() => {
                    tracing::debug!("received Op={:?}", &op);
                    match op {
                        Some(Op::Init(_)) => {
                            // This only happens at startup!
                            bail!("Init received after start");
                        },
                        // A backup slot was consumed to start a new backup.
                        Some(Op::Enqueue(path, tx)) => {
                            tracing::debug!("Enqueuing backup for {:?}", &path);
                            self.store.add(path.clone().into())?;
                            // Enqueue the backup.
                            chunker.chunk(Address{file:path.clone(), offset:0}, chunk_tx.clone());
                            pending.push_front(PendingBackup{path, tx, hash_builder: FileHashBuilder::new()});
                        },
                        // The handler requests the list of in-flight backups.
                        Some(Op::InFlight(op_tx)) => {
                            let mut response = vec![];
                            for path in self.store.list()? {
                                let path : PathBuf = path.try_into()?;
                                chunker.chunk(Address { file: path.clone(), offset: 0 }, chunk_tx.clone());
                                let (bk_tx, bk_rx) = oneshot::channel();
                                response.push(InFlight{path: path.clone(), recv: bk_rx});
                                pending.push_front(PendingBackup{path, tx: bk_tx, hash_builder: FileHashBuilder::new()});
                                remaining -= 1;
                            }
                            if  op_tx.send(response).is_err() {
                                bail!("peer died");
                            }
                        },
                        // End signal: _all_ senders are gone (incl all slots and
                        // the handler as well).
                        None => break,
                    }
                }
                // Handle a chunk from an in-flight backup.
                op = chunk_rx.recv() => {
                    match op {
                        // Successfully received a chunk.
                        Some(ChunkOp { address: addr, msg: ChunkMsg::Next(chunk) }) => {
                            tracing::debug!("chunk offset={} file={:?}", addr.offset, addr.file);
                            if let Some(p) = pending.iter_mut().find(|p| p.path == addr.file) {
                                p.hash_builder.update(&chunk);
                            }
                        },
                        // File is fully chunked.
                        Some(ChunkOp { address: addr, msg: ChunkMsg::Done(fsize, mtime) }) => {
                            tracing::debug!("done chunking {:?}", addr.file);
                            if let Some(idx) = pending.iter().position(|p| p.path == addr.file) {
                                let p = pending.remove(idx).unwrap();
                                self.store.remove(p.path.clone().into())?;
                                let snapshot = Snapshot {
                                    hash: p.hash_builder.finish(),
                                    fsize,
                                    mtime,
                                };
                                let result = HashUpdate::Hash(snapshot);
                                if let Err(result) = p.tx.send(BackupResult { path: p.path, update: result }) {
                                    bail!("failed to send {:?}", result);
                                }
                                remaining += 1;
                            }
                        },
                        // IO error while chunking the file.
                        Some(ChunkOp { address: addr, msg: ChunkMsg::Error(err) }) => {
                            // TODO: this should finish the current slot and propagate the error.
                            tracing::warn!("chunk error at {:?}+{}: {}", addr.file, addr.offset, err);
                        },
                        None => unreachable!("chunk_tx held by runner"),
                    }
                }
                // Orderly shutdown requested.
                _ = self.token.cancelled() => {
                    tracing::info!("Shutting down");
                    break;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        datamanager::DataManagerImpl, datastore::Datastore, fake_disk::FakeDisk, model::Chunk,
    };
    use anyhow::Context;
    use std::{path::PathBuf, time::Duration};

    #[tokio::test]
    #[test_log::test]
    async fn smoke_test() -> anyhow::Result<()> {
        let token = CancellationToken::new();
        // Successfully fetch two chunks from a file.
        let ds = Datastore::new_in_memory()?;
        let disk = FakeDisk::new();
        let (mut dm, handle) = DataManagerImpl::new(ds, disk.clone(), token.clone()).await?;

        let slot = dm.backup_slots().recv().await.context("no slot!")?;
        let backup_done = slot.enqueue(PathBuf::from("/a")).await?;

        let chunk = disk
            .get_chunk_handle(&PathBuf::from("/a"), Duration::from_secs(1))
            .await?;

        // Feed two chunks, then end iteration.
        chunk.send(Some(Ok(Chunk::Hole {
            offset: 0,
            size: 16,
        })));
        chunk.send(Some(Ok(Chunk::Hole {
            offset: 16,
            size: 16,
        })));
        chunk.send(None);

        // Backup completes, slot is released.
        let _ = dm
            .backup_slots()
            .recv()
            .await
            .context("no slot after backup")?;
        let result = backup_done.await?;
        assert_eq!(result.path, PathBuf::from("/a"));

        token.cancel();
        handle.await.context("DataManager")
    }
}
