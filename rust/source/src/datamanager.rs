//! The datamanager performs backup creation.

use std::path::Path;

use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::{JoinHandle, LocalSet},
};

use crate::datastore::Datastore;

pub struct Datamanager {
    tx: Sender<Operation>,
    handle: JoinHandle<anyhow::Result<()>>,
}

enum Operation {}

pub fn new(local: &LocalSet, db: &Path) -> anyhow::Result<Datamanager> {
    Ok(Datamanager::new(local, Datastore::new(db)?))
}

impl Datamanager {
    fn new(local: &LocalSet, store: Datastore) -> Datamanager {
        let (tx, rx) = mpsc::channel::<Operation>(1);
        let mut runner = Runner { store };
        let handle = local.spawn_local(async move {
            let result = runner.run(rx).await;
            if let Err(err) = &result {
                tracing::error!("Datamanager's runner failed: {:?}", err)
            }
            result
        });
        Datamanager { tx, handle }
    }
}

struct Runner {
    store: Datastore,
}

impl Runner {
    async fn run(&mut self, mut rx: Receiver<Operation>) -> anyhow::Result<()> {
        tracing::info!("Datamanager is starting");
        loop {
            tokio::select! {
                op = rx.recv() => {
                    match op {
                        None => break,
                    }
                }
            }
        }
        tracing::info!("Datamanager is shutting down");
        Ok(())
    }
}
