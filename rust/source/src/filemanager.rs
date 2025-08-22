//! Filemanager maintains the state of the filesystem and
//! detects changes that need to be backed up.

use crate::{
    clock::{self, Clock},
    datamanager::{BackupSlot, DataManager, DataManagerImpl},
    disk::{self, Disk},
    filestore::{self, Connection, Filestore, HashUpdate, Next, Scanner, Timing},
    model::Stats,
    solo::{self, Solo},
    watcher,
};
use anyhow::{Context, bail};
use std::{
    cmp::min,
    collections::HashSet,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
    task::JoinHandle,
};

#[cfg(test)]
mod tests;

/// The async public API.
pub struct FileManager {
    tx: Sender<Operation>,
    handle: JoinHandle<anyhow::Result<()>>,
}

/// Create a production Manager, using the production store, and
/// doing full scans at the specified period. The store is single-threaded
/// and will execute in the context of store_local.
pub fn new(
    db: &Path,
    rand: crypto::SharedRandom,
    datamanager: DataManagerImpl,
) -> anyhow::Result<FileManager> {
    let store = Connection::new(db, rand, Timing::default())?;
    let disk = disk::new()?;
    let clock = clock::new();

    FileManager::new(store, disk, clock, watcher::new()?, datamanager)
}

impl FileManager {
    fn new<S, D, C, DM>(
        store: S,
        disk: D,
        clock: C,
        watcher: Box<dyn watcher::Watcher + Send>,
        datamanager: DM,
    ) -> anyhow::Result<FileManager>
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
            datamanager,
        };
        let handle = solo::start(f, "FileManager")?;
        Ok(FileManager {
            tx: handle.sender,
            handle: handle.handle,
        })
    }

    /// Shutdown can be called in parallel with any pending call and will interrupt them,
    /// shutting down the file manager as early as possible.
    #[allow(dead_code)]
    pub async fn shutdown(self) -> anyhow::Result<()> {
        drop(self.tx);
        self.handle.await?
    }

    pub fn monitor(&mut self) -> &mut JoinHandle<anyhow::Result<()>> {
        &mut self.handle
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
    datamanager: DM,
}

fn duration_or_zero(now: SystemTime, target: &Option<SystemTime>) -> Duration {
    if let Some(target) = target.as_ref() {
        if now > *target {
            Duration::from_secs(0)
        } else {
            target.duration_since(now).unwrap()
        }
    } else {
        Duration::from_secs(0)
    }
}

// The agent side of the manager. Holds the mutable store and watcher, and
// performs the dispatching logic.
impl<S: Filestore, D: Disk, C: Clock, DM: DataManager> Solo for Runner<S, D, C, DM> {
    type Operation = Operation;

    async fn run(mut self, mut rx: Receiver<Operation>) -> anyhow::Result<()> {
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
                tracing::info!(
                    "Backup for {:?} is running but not marked as pending",
                    &actual.path
                );
                self.store.backup_start(actual.path)?;
            } else {
                expected.remove(&actual.path);
            }
            // In all cases, we know the running backup now.
            pending_backups.add(actual.recv);
        }
        // Really unexpected path: backups we expected to see running already, but which
        // are not. This should not happen unless there is data corruption as we start it
        // first then mark it as started.
        if !expected.is_empty() {
            bail!("The following backups are missing: {:?}", expected);
        }

        let mut scan_delay: Option<SystemTime> = None;
        let mut backup_slot: Option<DM::Slot> = None;
        let mut event_last_displayed = EarliestEvent::unset("nope");

        // The work loop will continuously refresh the filesystem when a scan is
        // active, hash files that haven't changed in a while, and otherwise respond
        // to client requests.
        loop {
            let now = self.clock.now();
            // hash_delay is assessed at every round as many operations can request a file be hashed,
            // independently of timing.
            let mut hash_delay: Option<SystemTime> = None;
            let mut next_event = EarliestEvent::unset("nope");
            match self.store.hash_next(now)? {
                Next::Next(file, ()) => {
                    tracing::debug!("hashing stale file {:?}", &file);
                    let update = match self.disk.snapshot(&file) {
                        Ok(snapshot) => HashUpdate::Hash(snapshot),
                        Err(err) => HashUpdate::Unreadable(err),
                    };
                    self.store.hash_update(file, now, update)?;
                }
                Next::Done(delay) => {
                    next_event = min(next_event, EarliestEvent::new(delay, "hash"));
                    hash_delay = Some(delay);
                }
            }
            let mut backup_delay: Option<SystemTime> = None;
            // No need to look up backups that need to be done if there is no open slot.
            if backup_slot.is_some() {
                match self.store.backup_next(now)? {
                    Next::Next(path, _egroup) => {
                        tracing::info!("starting new backup for {:?}", &path);
                        let slot = backup_slot.take().unwrap();
                        // We enqueue first, so that if there is a crash we can use _running_ backups to
                        // fill in the list of _started_ backups, without waiting for the backup queue.
                        let rx = slot.enqueue(path.clone()).await?;
                        pending_backups.add(rx);
                        self.store.backup_start(path)?;
                    }
                    Next::Done(delay) => {
                        next_event = min(next_event, EarliestEvent::new(delay, "backup"));
                        backup_delay = Some(delay);
                    }
                }
            }
            if scan_delay.is_none() {
                match self.store.tree_scan_next()? {
                    filestore::Next::Done(delay) => {
                        next_event = min(next_event, EarliestEvent::new(delay, "tree scan"));
                        scan_delay = Some(delay);
                        tracing::info!("finished tree scan, next at {}", isotime(delay)?);
                    }
                    filestore::Next::Next(dir, mut updater) => {
                        tracing::debug!("scanning {:?}", &dir);
                        match self.disk.scan(&dir) {
                            Ok(subdirs) => {
                                let mut complete = true;
                                for entry in subdirs {
                                    match entry {
                                        Ok(entry) => updater.update(self.clock.now(), &entry)?,
                                        Err(_) => complete = false,
                                    }
                                }
                                updater.commit(complete)?;
                            }
                            Err(e) => {
                                updater.error(e)?;
                            }
                        }
                    }
                }
            }
            let scan_sleep = duration_or_zero(now, &scan_delay);
            let hash_sleep = duration_or_zero(now, &hash_delay);
            let back_sleep = duration_or_zero(now, &backup_delay);
            // Any delay set as None indicates that we could process the underlying work
            // right away. This is a bit more complex for backups, as we also need to know
            // that the datamanager has the capacity to accept a new backup.
            let can_backup = backup_slot.is_some() && backup_delay.is_none();
            let more_pending = [&scan_delay, &hash_delay].iter().any(|d| d.is_none()) || can_backup;
            if !more_pending && next_event.is_valid() {
                // Do not repeat the same message, as we might be spinning a few
                // time, say when the watcher repeatedly interrupts us.
                if next_event != event_last_displayed {
                    tracing::info!(
                        "idling until next {} at {}",
                        next_event.event,
                        isotime(next_event.time)?,
                    );
                    event_last_displayed = next_event;
                }
            }
            tokio::select! {
                _ = self.clock.sleep(Duration::from_millis(1), "tick"), if more_pending => {
                    // The artificial delay allows for pending events (client, watcher)
                    // to take place.
                }
                _ = self.clock.sleep(scan_sleep, "tree_scan"), if scan_delay.is_some() => {
                    tracing::info!("starting a new tree scan");
                    scan_delay = None;
                    if let Err(err) = self.store.tree_scan_start(now) {
                        tracing::error!("tree_scan_start() failed: {:?}", err);
                    }
                }
                _ = self.clock.sleep(hash_sleep, "hash"), if hash_delay.is_some() => { }
                _ = self.clock.sleep(back_sleep, "backup"), if backup_delay.is_some() => { }
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
                slot = self.datamanager.backup_slots().recv(), if backup_slot.is_none() => {
                    match slot {
                        Some(slot) => {
                            backup_slot = Some(slot);
                        },
                        None => {
                           bail!("DataManager failed");
                        },
                    };
                },
                done = pending_backups.next() => {
                    tracing::info!("backup completed: {:?}", &done);
                    self.store.backup_done(done.path, self.clock.now(), done.update)?;
                }
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
        self.datamanager.shutdown().await?;
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
}

fn isotime<T>(dt: T) -> anyhow::Result<String>
where
    T: Into<time::OffsetDateTime>,
{
    dt.into()
        .format(&time::format_description::well_known::Iso8601::DEFAULT)
        .context("can't format timestamp")
}

/// Helper type to track the earliest event that will happen next.
struct EarliestEvent<'a> {
    time: SystemTime,
    event: &'a str,
}

impl<'a> EarliestEvent<'a> {
    /// Create an invalid earliest event.
    fn unset(name: &'a str) -> EarliestEvent<'a> {
        EarliestEvent::new(UNIX_EPOCH, name)
    }
    fn new(time: SystemTime, event: &'a str) -> EarliestEvent<'a> {
        EarliestEvent { time, event }
    }
    fn is_valid(&self) -> bool {
        self.time != UNIX_EPOCH
    }
}

impl std::cmp::Eq for EarliestEvent<'_> {}

impl std::cmp::PartialOrd for EarliestEvent<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl std::cmp::PartialEq for EarliestEvent<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
    }
}

impl std::cmp::Ord for EarliestEvent<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.time == UNIX_EPOCH {
            if other.time == UNIX_EPOCH {
                std::cmp::Ordering::Equal
            } else {
                std::cmp::Ordering::Greater
            }
        } else if other.time == UNIX_EPOCH {
            std::cmp::Ordering::Less
        } else {
            self.time.cmp(&other.time)
        }
    }
}

#[cfg(test)]
mod earliest_event_test {
    use std::{
        cmp::min,
        time::{Duration, UNIX_EPOCH},
    };

    use crate::filemanager::EarliestEvent;

    #[test]
    fn ordered_as_intended() {
        let mut earliest = EarliestEvent::unset("wrong");
        assert!(!earliest.is_valid()); // we don't have an earliest event.

        let t10 = UNIX_EPOCH + Duration::from_secs(10);
        earliest = min(earliest, EarliestEvent::new(t10, "10s"));
        // We expect any non epoch time to be earlier... (the weird part)
        assert!(earliest.is_valid());
        assert_eq!(earliest.time, t10);

        let t5 = UNIX_EPOCH + Duration::from_secs(5);
        earliest = min(earliest, EarliestEvent::new(t5, "5s"));
        assert_eq!(earliest.time, t5);

        let t15 = UNIX_EPOCH + Duration::from_secs(15);
        earliest = min(earliest, EarliestEvent::new(t15, "15s"));
        assert_eq!(earliest.time, t5);
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
    use rand::{TryRngCore, rngs::OsRng};
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
                let can_add = current < total && OsRng.try_next_u32().unwrap() % 2 == 0;
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
                    let idx = OsRng.try_next_u32().unwrap() as usize % pending.len();
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
