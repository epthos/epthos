//! The DataManager performs backup creation.

use std::path::Path;

use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    datastore::Datastore,
    solo::{self, Solo},
};

pub struct DataManager {
    tx: Sender<Operation>,
    handle: JoinHandle<anyhow::Result<()>>,
}

enum Operation {}

pub fn new(db: &Path) -> anyhow::Result<DataManager> {
    DataManager::new(Datastore::new(db)?)
}

impl DataManager {
    fn new(store: Datastore) -> anyhow::Result<DataManager> {
        let f = move || Runner { store };
        let (tx, handle) = solo::start(f, "DataManager")?;
        Ok(DataManager { tx, handle })
    }

    pub fn monitor(&mut self) -> &mut JoinHandle<anyhow::Result<()>> {
        &mut self.handle
    }
}

struct Runner {
    store: Datastore,
}

impl Solo for Runner {
    type Operation = Operation;
    async fn run(&mut self, mut rx: Receiver<Operation>) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                op = rx.recv() => {
                    match op {
                        None => break,
                    }
                }
            }
        }
        Ok(())
    }
}
