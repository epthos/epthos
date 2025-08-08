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

pub struct DataManager {
    tx: Sender<Operation>,
    handle: JoinHandle<anyhow::Result<()>>,
    slot_rx: Receiver<BackupSlot>,
}

pub async fn new(db: &Path) -> anyhow::Result<DataManager> {
    let clock = clock::new();
    DataManager::new(Datastore::new(db)?, clock).await
}

impl DataManager {
    async fn new<C>(store: Datastore, clock: C) -> anyhow::Result<DataManager>
    where
        C: Clock + Send + 'static,
    {
        let f = move || Runner {
            _store: store,
            clock,
        };
        let (tx, handle) = solo::start(f, "DataManager")?;
        // Get ready to receive backup slots from the runner.
        let (slot_tx, slot_rx) = mpsc::channel(1);
        tx.send(Operation::Init((slot_tx, tx.clone())))
            .await
            .context("Runner failed")?;
        Ok(DataManager {
            tx,
            handle,
            slot_rx,
        })
    }

    pub fn monitor(&mut self) -> &mut JoinHandle<anyhow::Result<()>> {
        &mut self.handle
    }

    pub fn backup_slots(&mut self) -> &mut Receiver<BackupSlot> {
        &mut self.slot_rx
    }

    #[allow(dead_code)]
    pub async fn shutdown(self) -> anyhow::Result<()> {
        let _ = self.tx.send(Operation::Shutdown).await;
        self.handle.await??;
        Ok(())
    }
}

#[derive(Debug)]
enum Operation {
    // Initialize the Runner, which needs to know how to return backup slots.
    Init((Sender<BackupSlot>, Sender<Operation>)),
    Enqueue(PathBuf),
    // We can't rely on dropping the sender in the manager as we clone it in every
    // backup slot too.
    Shutdown,
}

pub struct BackupSlot {
    tx: Sender<Operation>,
}

impl BackupSlot {
    pub async fn enqueue(self, path: PathBuf) -> anyhow::Result<()> {
        self.tx
            .send(Operation::Enqueue(path))
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
    type Operation = Operation;
    async fn run(&mut self, mut rx: Receiver<Operation>) -> anyhow::Result<()> {
        let Some(Operation::Init((slot_sender, op_sender))) = rx.recv().await else {
            bail!("Initialization failed");
        };
        let mut remaining = 1;
        let mut pending = 0;
        loop {
            tracing::debug!("starting with {} remaining, {} pending", remaining, pending);
            tokio::select! {
                _ = slot_sender.send(BackupSlot { tx: op_sender.clone() }), if remaining > 0 => {
                    tracing::debug!("sent one slot");
                    remaining -= 1;
                }
                op = rx.recv() => {
                    tracing::debug!("received Op={:?}", &op);
                    match op {
                        Some(Operation::Init(_)) => {
                            // This only happens at startup!
                            bail!("Init received after start");
                        },
                        Some(Operation::Enqueue(path)) => {
                            tracing::debug!("Enqueuing backup for {:?}", &path);
                            // Enqueue the backup.
                            pending += 1;
                        },
                        None | Some(Operation::Shutdown) => break,
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
    use std::{path::PathBuf, time::Duration};

    use crate::{datamanager::DataManager, datastore::Datastore, fake_clock::FakeClockHandler};
    use anyhow::Context;
    use test_log::test;
    use tokio::sync::mpsc::error::TryRecvError;

    #[test(tokio::test)]
    async fn smoke_test() -> anyhow::Result<()> {
        let ds = Datastore::new_in_memory()?;
        let (clock, clock_state) = FakeClockHandler::new();
        let mut dm = DataManager::new(ds, clock).await?;

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
