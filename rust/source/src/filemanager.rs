//! Filemanager maintains the state of the filesystem and
//! detects changes that need to be backed up.

use crate::{
    clock::{self, Clock},
    datamanager::{BackupResult, BackupSlot, DataManager, DataManagerImpl},
    disk::{self, Disk},
    filestore::{Connection, Filestore, HashUpdate, Next, Scanner, Timing},
    model::Stats,
    solo::{self, Solo},
    watcher,
};
use anyhow::{Context, bail};
use std::{
    collections::{HashSet, VecDeque},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests;

/// The async public API.
#[derive(Clone)]
pub struct FileManager {
    tx: Sender<Operation>,
}

pub struct FileManagerContext {
    pub manager: FileManager,
    pub handle: JoinHandle<()>,
}

/// Create a production Manager, using the production store, and
/// doing full scans at the specified period. The store is single-threaded
/// and will execute in the context of store_local.
pub fn new(
    db: &Path,
    rand: crypto::SharedRandom,
    datamanager: DataManagerImpl,
    token: CancellationToken,
) -> anyhow::Result<FileManagerContext> {
    let store = Connection::new(db, rand, Timing::default())?;
    let disk = disk::new()?;
    let clock = clock::new();
    let (watcher, watcher_handle) = watcher::new(token.child_token())?;

    FileManager::create(
        store,
        disk,
        clock,
        watcher,
        watcher_handle,
        datamanager,
        token,
    )
}

impl FileManager {
    fn create<S, D, C, DM>(
        store: S,
        disk: D,
        clock: C,
        watcher: Box<dyn watcher::Watcher + Send>,
        watcher_handle: JoinHandle<()>,
        datamanager: DM,
        token: CancellationToken,
    ) -> anyhow::Result<FileManagerContext>
    where
        S: Filestore + Send + 'static,
        D: Disk + Send + 'static,
        C: Clock + Send + 'static,
        DM: DataManager + Send + 'static,
    {
        let f = move || Runner {
            store,
            disk,
            clock,
            watcher,
            watcher_handle,
            datamanager,
            token,
        };
        let handle = solo::start(f, "FileManager")?;
        Ok(FileManagerContext {
            manager: FileManager { tx: handle.sender },
            handle: handle.handle,
        })
    }

    // set_roots() will update the roots for file scanning. The updated value will be used
    // as soon as possible.
    #[tracing::instrument(skip(self))]
    pub async fn set_roots(&self, roots: Vec<PathBuf>) -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel::<anyhow::Result<()>>(1);
        let op = Operation::SetRoots(roots, tx);
        self.tx.send(op).await?;
        rx.recv().await.context("WatcherImpl closed early")??;
        Ok(())
    }

    #[allow(dead_code)] // TODO: use it!
    pub async fn get_stats(&self) -> anyhow::Result<Stats> {
        let (tx, rx) = oneshot::channel::<anyhow::Result<Stats>>();
        self.tx.send(Operation::GetStats(tx)).await?;
        rx.await.context("FileManager Runner closed early")?
    }
}

// ---------------------------------------------------------------------------
// Actor implementation
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum Operation {
    SetRoots(Vec<PathBuf>, Sender<anyhow::Result<()>>),
    GetStats(oneshot::Sender<anyhow::Result<Stats>>),
}

struct Runner<S, D, C, DM>
where
    S: Filestore,
    D: Disk,
    C: Clock,
    DM: DataManager,
{
    store: S,
    disk: D,
    clock: C,
    watcher: Box<dyn watcher::Watcher + Send>,
    watcher_handle: JoinHandle<()>,
    datamanager: DM,
    token: CancellationToken,
}

// The agent side of the manager. Holds the mutable store and watcher, and
// performs the dispatching logic.
impl<S: Filestore, D: Disk, C: Clock, DM: DataManager> Solo for Runner<S, D, C, DM> {
    type Operation = Operation;

    async fn run(mut self, mut rx: Receiver<Operation>) -> anyhow::Result<()> {
        // Backups currently in flight in the datamanager.
        let mut inflight_backups = self.sync_inflight_backups().await?;
        let mut backup_slots: VecDeque<DM::Slot> = VecDeque::new();

        // The work loop will continuously refresh the filesystem when a scan is
        // active, hash files that haven't changed in a while, and otherwise respond
        // to client requests. Its triggers are a mix of async events and time based
        // changes that the underlying filestore manages.
        loop {
            tracing::debug!(
                "loop with {} in flight backups and {} slots",
                inflight_backups.len(),
                backup_slots.len(),
            );
            let now = self.clock.now();

            // Perform each possible non-blocking operation, and return the delay until
            // the next such work.
            let hash_delay =
                Runner::<S, D, C, DM>::next_hash(&self.disk, &self.clock, &mut self.store, now)?;
            let backup_delay = Runner::<S, D, C, DM>::next_backup(
                &self.clock,
                &mut self.store,
                now,
                &mut backup_slots,
                &mut inflight_backups,
            )
            .await?;
            let scan_delay =
                Runner::<S, D, C, DM>::next_scan(&self.disk, &self.clock, &mut self.store, now)?;

            tokio::select! {
                // Cancellations management.
                _ = self.token.cancelled() => {
                    tracing::info!("Shutting down");
                    break;
                }
                // Avoid re-polling if we already cancelled, as this causes a panic.
                watcher_result = &mut self.watcher_handle, if !self.token.is_cancelled() => {
                    self.token.cancel();
                    watcher_result.context("Watcher")?;
                }

                // Unblock if any action can be taken right away.
                _ = hash_delay => {}
                _ = backup_delay => {}
                _ = scan_delay => {}

                // Interactions with other systems.
                op = rx.recv() => {
                    tracing::debug!("handling client operation {:?}", &op);
                    match op {
                        None => break,
                        Some(Operation::SetRoots(roots, tx)) => {
                            let refs: Vec<&Path> = roots.iter().map(|r| r.as_ref()).collect();
                            let _ = tx.send(self.set_roots(&refs)).await;
                        },
                        Some(Operation::GetStats(tx)) => {
                            let _ = tx.send(self.store.get_stats());
                        }
                    }
                }

                // Slots management: monitor completion and availability.
                done = inflight_backups.next() => {
                    tracing::info!("backup completed: {:?}", &done);
                    self.store.backup_done(done.path, self.clock.now(), done.update)?;
                }
                slot = self.datamanager.backup_slots().recv() => {
                    match slot {
                        Some(slot) => {
                            backup_slots.push_back(slot);
                        },
                        None => {
                           bail!("DataManager failed");
                        },
                    };
                },

                // This can be unbounded. Goes last so it doesn't take away work from the rest.
                update = self.watcher.next().recv() => {
                    tracing::debug!("handling watcher operation {:?}", &update);
                    match update {
                        None => {
                            tracing::error!("watcher died...");
                            break;
                        },
                        // Directory changes are not supported, we'll rely on the tree scan.
                        Some(watcher::Update::Directory(_)) => {},
                        Some(watcher::Update::File(path)) => {
                            if let Ok((fsize, mtime)) = self.disk.metadata(&path) {
                                self.store.metadata_update(path, now, fsize, mtime)?;
                            }
                        },
                    }
                }
            }
        }
        Ok(())
    }
}

impl<S: Filestore, D: Disk, C: Clock, DM: DataManager> Runner<S, D, C, DM> {
    fn set_roots(&mut self, roots: &[&Path]) -> anyhow::Result<()> {
        self.watcher.set_roots(roots)?;
        let changed = self.store.set_roots(roots)?;
        // We intentionally trigger a full scan when the roots are actually
        // modified. Note that the roots are always set at least at startup, so
        // this needs to be conditional.
        if changed {
            self.store.tree_scan_start(self.clock.now())?;
        }
        Ok(())
    }

    /// Hash the next file that's due for hashing, or return a sleep future until the
    /// next one is due.
    fn next_hash<'b>(
        disk: &D,
        clock: &'b C,
        store: &mut S,
        now: SystemTime,
    ) -> anyhow::Result<impl Future<Output = ()> + use<'b, S, D, C, DM>> {
        const NAME: &str = "hash";
        match store.hash_next(now)? {
            Next::Next(file, ()) => {
                tracing::debug!("hashing stale file {:?}", &file);
                // TODO: this can take arbitrarily long and prevent shutdown /
                // stale other operations.
                let update = match disk::snapshot(disk, &file) {
                    Ok(snapshot) => HashUpdate::Hash(snapshot),
                    Err(err) => HashUpdate::Unreadable(err),
                };
                store.hash_update(file, now, update)?;
                Ok(clock.sleep(Duration::ZERO, NAME))
            }
            Next::Done(delay) => Ok(clock.sleep(to_duration(now, delay), NAME)),
        }
    }

    /// Backup the next file that needs backing up, or return a sleep future until the
    /// next file is due. This takes into consideration available slots.
    async fn next_backup<'b, 'c>(
        clock: &'b C,
        store: &'c mut S,
        now: SystemTime,
        backup_slots: &mut VecDeque<DM::Slot>,
        inflight_backups: &mut VecFutures<BackupResult>,
    ) -> anyhow::Result<impl Future<Output = ()> + use<'b, S, D, C, DM>> {
        const NAME: &str = "backup";
        if backup_slots.is_empty() {
            // No open backup slot? Wait "forever".
            return Ok(clock.sleep(Duration::from_secs(3600), NAME));
        }
        match store.backup_next(now)? {
            Next::Next(path, _egroup) => {
                tracing::info!("starting new backup for {:?}", &path);
                let slot = backup_slots.pop_front().unwrap();
                // We enqueue first, so that if there is a crash we can use _running_ backups to
                // fill in the list of _started_ backups, without waiting for the backup queue.
                let rx = slot.enqueue(path.clone()).await?;
                inflight_backups.add(rx);
                store.backup_start(path)?;
                Ok(clock.sleep(Duration::ZERO, NAME))
            }
            Next::Done(delay) => Ok(clock.sleep(to_duration(now, delay), NAME)),
        }
    }

    /// Scan the next directory that's ready for scanning, or return a sleep future until such
    /// a directory is due.
    fn next_scan<'b>(
        disk: &D,
        clock: &'b C,
        store: &mut S,
        now: SystemTime,
    ) -> anyhow::Result<impl Future<Output = ()> + use<'b, S, D, C, DM>> {
        const NAME: &str = "tree_scan";
        match store.tree_scan_next()? {
            Next::Done(delay) => Ok(clock.sleep(to_duration(now, delay), NAME)),
            Next::Next(dir, mut updater) => {
                // TODO: this can take arbitrarily long and prevent shutdown /
                // stale other operations.
                tracing::debug!("scanning {:?}", &dir);
                match disk.scan(&dir) {
                    Ok(subdirs) => {
                        let mut complete = true;
                        for entry in subdirs {
                            match entry {
                                Ok(entry) => updater.update(clock.now(), &entry)?,
                                Err(_) => complete = false,
                            }
                        }
                        updater.commit(complete)?;
                    }
                    Err(e) => {
                        updater.error(e.into())?;
                    }
                }
                Ok(clock.sleep(Duration::ZERO, NAME))
            }
        }
    }

    /// Synchronize the in-flight backups known to the filemanager with the ones known to
    /// the datamanager.
    async fn sync_inflight_backups(&mut self) -> anyhow::Result<VecFutures<BackupResult>> {
        let pending_backups = VecFutures::new();
        let mut expected: HashSet<PathBuf> = self
            .store
            .backup_pending()?
            .into_iter()
            .map(|item| item.0)
            .collect();
        for actual in self.datamanager.in_flight().await?.into_iter() {
            // TODO: test that path.
            if !expected.contains(&actual.path) {
                // Unusual path: we have a running backup, but we don't track it
                // as pending. This is recoverable easily, simply start it now.
                // Note: this will blow up if the file is not dirty, denoting a
                // real discrepancy in the two stores.
                tracing::info!(
                    "Backup for {:?} is running but not marked as pending",
                    &actual.path
                );
                self.store.backup_start(actual.path.clone())?;
            } else {
                expected.remove(&actual.path);
            }
            // In all cases, we know the running backup now.
            tracing::debug!("tracking pending backup for {:?}", &actual.path);
            pending_backups.add(actual.recv);
        }
        if !expected.is_empty() {
            tracing::info!("The following backups will be retried: {:?}", expected);
            self.store.backups_cancel(expected)?;
        }
        Ok(pending_backups)
    }
}

fn to_duration(now: SystemTime, delay: SystemTime) -> Duration {
    if let Ok(duration) = delay.duration_since(now) {
        duration
    } else {
        Duration::ZERO
    }
}

// Helper to manage a vector of oneshot results.

struct VecFutures<O> {
    pending: Arc<Mutex<Vec<oneshot::Receiver<O>>>>,
}

impl<O> VecFutures<O> {
    pub fn new() -> Self {
        VecFutures {
            pending: Arc::new(Mutex::new(vec![])),
        }
    }
    pub fn add(&self, o: oneshot::Receiver<O>) {
        let mut v = self.pending.lock().unwrap();
        v.push(o);
    }
    pub fn next(&self) -> VecFuture<O> {
        VecFuture {
            pending: self.pending.clone(),
        }
    }
    pub fn len(&self) -> usize {
        self.pending.lock().unwrap().len()
    }
}

struct VecFuture<O> {
    pending: Arc<Mutex<Vec<oneshot::Receiver<O>>>>,
}

impl<O> Future for VecFuture<O> {
    type Output = O;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut v = self.pending.lock().unwrap();
        let item = v
            .iter_mut()
            .enumerate()
            .find_map(|(i, f)| match Pin::new(f).poll(cx) {
                Poll::Ready(e) => Some((i, e)),
                Poll::Pending => None,
            });
        match item {
            Some((i, e)) => {
                // We consumed the receiver, we must drop it from the vec to avoid
                // waiting for it once more, whether it succeeded or not.
                v.remove(i);
                match e {
                    Ok(e) => Poll::Ready(e),
                    Err(_) => Poll::Pending,
                }
            }
            None => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod vec_futures {
    use crate::filemanager::VecFutures;
    use anyhow::bail;
    use rand::{TryRng, rngs::SysRng};
    use std::{collections::HashSet, time::Duration};
    use tokio::sync::{
        mpsc,
        oneshot::{self, Receiver},
    };

    #[tokio::test]
    async fn empty_vec() -> anyhow::Result<()> {
        let v: VecFutures<()> = VecFutures::new();
        let delay = tokio::time::sleep(Duration::from_millis(1));
        let mut c = 0;
        loop {
            c += 1;
            tokio::select! {
                _ = v.next() => {
                    bail!("should not select");
                },
                _ = delay => break,
            }
        }
        assert_eq!(c, 1);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn mix_add_and_trigger() -> anyhow::Result<()> {
        let v: VecFutures<i32> = VecFutures::new();
        let mut got: HashSet<i32> = HashSet::new();
        let (tx, mut rx) = mpsc::channel::<Receiver<i32>>(1);
        let total = 10;
        let h = tokio::task::spawn(async move {
            let mut current = 0;
            let mut pending = vec![];
            loop {
                let can_add = current < total && SysRng.try_next_u32().unwrap() % 2 == 0;
                if can_add {
                    tracing::info!("sending item {}", current);
                    let (otx, orx) = oneshot::channel();
                    tx.send(orx).await.unwrap();
                    pending.push((otx, current));
                    current += 1;
                } else {
                    if pending.is_empty() {
                        if current >= total {
                            break;
                        }
                        continue;
                    }
                    let idx = SysRng.try_next_u32().unwrap() as usize % pending.len();
                    let (otx, value) = pending.remove(idx);
                    tracing::info!("finishing item {}", value);
                    otx.send(value).unwrap();
                }
            }
        });
        loop {
            tokio::select! {
                c = v.next() => {
                    tracing::info!("received finished item {}", c);
                    got.insert(c);
                },
                Some(r) = rx.recv() => {
                    tracing::info!("adding pending item");
                    v.add(r);
                }
            }
            if got.len() >= total as usize {
                break;
            }
        }
        h.await?;
        assert_eq!(got, HashSet::from_iter(0..10));
        Ok(())
    }
}
