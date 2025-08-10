//! The DataManager performs backup creation.

use crate::{
    clock::{self, Clock},
    datastore::Datastore,
    solo::{self, Solo},
};
use anyhow::{Context, bail};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};

pub trait DataManager {
    type Slot: BackupSlot;

    /// Provides the receiver of new backup slots.
    fn backup_slots(&mut self) -> &mut Receiver<Self::Slot>;
    /// Shuts down the manager.
    async fn shutdown(self) -> anyhow::Result<()>;
}

pub trait BackupSlot {
    async fn enqueue(self, path: PathBuf) -> anyhow::Result<()>;
}

pub async fn new(db: &Path) -> anyhow::Result<DataManagerImpl> {
    let clock = clock::new();
    DataManagerImpl::new(Datastore::new(db)?, clock).await
}

pub struct DataManagerImpl {
    tx: Sender<Op>,
    handle: JoinHandle<anyhow::Result<()>>,
    slot_rx: Receiver<BackupSlotImpl>,
}

impl DataManagerImpl {
    async fn new<C>(store: Datastore, clock: C) -> anyhow::Result<DataManagerImpl>
    where
        C: Clock + Send + 'static,
    {
        let f = move || Runner {
            _store: store,
            clock,
        };
        let handle = solo::start(f, "DataManager")?;
        // Get ready to receive backup slots from the runner.
        let (slot_tx, slot_rx) = mpsc::channel(1);
        handle
            .sender
            .send(Op::Init((slot_tx, handle.sender.clone())))
            .await
            .context("Runner failed")?;
        Ok(DataManagerImpl {
            tx: handle.sender,
            handle: handle.handle,
            slot_rx,
        })
    }
}

impl DataManager for DataManagerImpl {
    type Slot = BackupSlotImpl;

    fn backup_slots(&mut self) -> &mut Receiver<BackupSlotImpl> {
        &mut self.slot_rx
    }

    async fn shutdown(self) -> anyhow::Result<()> {
        let _ = self.tx.send(Op::Shutdown).await;
        self.handle.await??;
        Ok(())
    }
}

#[derive(Debug)]
enum Op {
    // Initialize the Runner, which needs to know how to return backup slots.
    Init((Sender<BackupSlotImpl>, Sender<Op>)),
    Enqueue(PathBuf),
    // We can't rely on dropping the sender in the manager as we clone it in every
    // backup slot too.
    Shutdown,
}

pub struct BackupSlotImpl {
    tx: Sender<Op>,
}

impl BackupSlot for BackupSlotImpl {
    async fn enqueue(self, path: PathBuf) -> anyhow::Result<()> {
        self.tx
            .send(Op::Enqueue(path))
            .await
            .context("Runner failed")?;
        Ok(())
    }
}

struct Runner<C>
where
    C: Clock,
{
    _store: Datastore,
    clock: C,
}

impl<C: Clock> Solo for Runner<C> {
    type Operation = Op;

    async fn run(self, mut rx: Receiver<Op>) -> anyhow::Result<()> {
        let Some(Op::Init((slot_sender, op_sender))) = rx.recv().await else {
            bail!("Initialization failed");
        };
        let mut remaining = 1;
        let mut pending = 0;
        loop {
            tracing::debug!("starting with {} remaining, {} pending", remaining, pending);
            tokio::select! {
                _ = slot_sender.send(BackupSlotImpl { tx: op_sender.clone() }), if remaining > 0 => {
                    tracing::debug!("sent one slot");
                    remaining -= 1;
                }
                op = rx.recv() => {
                    tracing::debug!("received Op={:?}", &op);
                    match op {
                        Some(Op::Init(_)) => {
                            // This only happens at startup!
                            bail!("Init received after start");
                        },
                        Some(Op::Enqueue(path)) => {
                            tracing::debug!("Enqueuing backup for {:?}", &path);
                            // Enqueue the backup.
                            pending += 1;
                        },
                        None | Some(Op::Shutdown) => break,
                    }
                }
                _ = self.clock.sleep(Duration::from_secs(10), "pause"), if pending > 0 => {
                    tracing::debug!("One backup completed");
                    pending -= 1;
                    remaining += 1;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{datamanager::DataManagerImpl, datastore::Datastore, fake_clock::FakeClockHandler};
    use anyhow::Context;
    use std::{path::PathBuf, time::Duration};
    use test_log::test;
    use tokio::sync::mpsc::error::TryRecvError;

    #[test(tokio::test)]
    async fn smoke_test() -> anyhow::Result<()> {
        let ds = Datastore::new_in_memory()?;
        let (clock, clock_state) = FakeClockHandler::new();
        let mut dm = DataManagerImpl::new(ds, clock).await?;

        let slot = dm.backup_slots().recv().await.context("no slot!")?;
        slot.enqueue(PathBuf::from("/a")).await?;

        let handle = clock_state.wait("pause").await;
        assert_eq!(handle.delay, Duration::from_secs(10));

        if let Err(e) = dm.backup_slots().try_recv() {
            assert_eq!(e, TryRecvError::Empty);
        } else {
            panic!("unexpected slot");
        }
        // Pretend the backup completed.
        handle.done();
        let _ = dm.backup_slots().recv().await.context("no slot!")?;
        tracing::debug!("received");

        dm.shutdown().await?;
        Ok(())
    }
}
