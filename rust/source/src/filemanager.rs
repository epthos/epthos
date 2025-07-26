//! Filemanager maintains the state of the filesystem and
//! detects changes that need to be backed up.

use crate::{
    clock::{self, Clock},
    disk::{self, Disk},
    filestore::{self, Connection, Filestore, HashUpdate, Next, ScanUpdate, Scanner, Timing},
    watcher,
};
use anyhow::{Context, bail};
use std::{
    cmp::min,
    fs::{self, DirEntry},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::{JoinHandle, LocalSet},
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
    store_local: &LocalSet,
    db: &Path,
    rand: crypto::SharedRandom,
) -> anyhow::Result<FileManager> {
    let store = Connection::new(db, rand, Timing::default())?;
    let disk = disk::new()?;
    let clock = clock::new();

    Ok(FileManager::new(
        store_local,
        store,
        disk,
        clock,
        watcher::new()?,
    ))
}

impl FileManager {
    fn new<S: Filestore + 'static, D: Disk + 'static, C: Clock + 'static>(
        store_local: &LocalSet,
        store: S,
        disk: D,
        clock: C,
        watcher: Box<dyn watcher::Watcher + Send>,
    ) -> FileManager {
        let (tx, rx) = mpsc::channel::<Operation>(1);
        let mut runner = Runner {
            store,
            disk,
            clock,
            watcher,
        };
        let handle = store_local.spawn_local(async move {
            let result = runner.run(rx).await;
            if let Err(err) = &result {
                tracing::error!("FileManager's Runner failed: {:?}", err);
            }
            result
        });
        FileManager { tx, handle }
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
}

#[derive(Debug)]
enum Operation {
    SetRoots(Vec<PathBuf>, Sender<anyhow::Result<()>>),
}

struct Runner<S, D, C>
where
    S: Filestore,
    D: Disk,
    C: Clock,
{
    store: S,
    disk: D,
    clock: C,
    watcher: Box<dyn watcher::Watcher + Send>,
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
impl<S: Filestore, D: Disk, C: Clock> Runner<S, D, C> {
    async fn run(&mut self, mut rx: Receiver<Operation>) -> anyhow::Result<()> {
        tracing::info!("FileManager starting");

        // TODO: ensure those backups are known to the backup layer.
        let _ = self.store.backup_pending()?;

        let mut scan_delay: Option<SystemTime> = None;
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
            match self.store.backup_next(now)? {
                Next::Next(path, _egroup) => {
                    // TODO: enqueue backup.
                    self.store.backup_start(path)?;
                }
                Next::Done(delay) => {
                    next_event = min(next_event, EarliestEvent::new(delay, "backup"));
                    backup_delay = Some(delay);
                }
            }
            if scan_delay.is_none() {
                match self.store.tree_scan_next()? {
                    filestore::Next::Done(delay) => {
                        next_event = min(next_event, EarliestEvent::new(delay, "tree scan"));
                        scan_delay = Some(delay);
                    }
                    filestore::Next::Next(dir, mut updater) => {
                        tracing::debug!("scanning {:?}", &dir);
                        // TODO: move to disk module.
                        match fs::read_dir(&dir) {
                            Ok(subdirs) => {
                                let mut complete = true;
                                for entry in subdirs {
                                    match scan_entry(entry) {
                                        Ok(entry) => updater.update(&entry)?,
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
            // right away.
            let more_pending = [&scan_delay, &hash_delay, &backup_delay]
                .iter()
                .any(|d| d.is_none());
            if !more_pending && next_event.is_valid() {
                tracing::info!(
                    "waiting for next {} at {}",
                    next_event.event,
                    isotime(next_event.time)?,
                );
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
                        }
                    }
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
        Ok(())
    }

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

fn scan_entry(entry: std::io::Result<DirEntry>) -> anyhow::Result<ScanUpdate> {
    let entry = entry.context("invalid entry")?;
    let file_type = entry.file_type().context("file_type()")?;
    if file_type.is_file() {
        let md = entry.metadata().context("metadata")?;
        Ok(ScanUpdate::File(
            entry.file_name(),
            SystemTime::now(),
            md.len(),
            md.modified()?,
        ))
    } else if file_type.is_dir() {
        Ok(ScanUpdate::Directory(entry.file_name()))
    } else if file_type.is_symlink() {
        // TODO: represent symlinks?
        bail!("symlinks are not supported yet");
    } else {
        tracing::info!("file [{:?}] has unsupported type", &entry);
        bail!("unsupported entry {:?}", entry);
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
