//! Filemanager maintains the state of the filesystem and
//! detects changes that need to be backed up.

use crate::{
    disk::{self, Disk},
    filestore::{self, Connection, Filestore, HashNext, HashUpdate, ScanUpdate, Scanner},
    watcher,
};
use anyhow::{Context, bail};
use std::{
    fs::{self, DirEntry},
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::{JoinHandle, LocalSet},
};

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
    scan_period: Duration,
    db: &Path,
    rand: crypto::SharedRandom,
) -> anyhow::Result<FileManager> {
    let store = Connection::new(db, rand)?;
    let disk = disk::new()?;

    Ok(FileManager::new(
        store_local,
        store,
        disk,
        watcher::new()?,
        scan_period,
    ))
}

impl FileManager {
    fn new<S: Filestore + 'static, D: Disk + 'static>(
        store_local: &LocalSet,
        store: S,
        disk: D,
        watcher: Box<dyn watcher::Watcher + Send>,
        scan_period: Duration,
    ) -> FileManager {
        let (tx, rx) = mpsc::channel::<Operation>(1);
        let mut runner = Runner {
            store,
            disk,
            watcher,
            scan_period,
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

struct Runner<S, D>
where
    S: Filestore,
    D: Disk,
{
    store: S,
    disk: D,
    watcher: Box<dyn watcher::Watcher + Send>,
    scan_period: Duration,
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

// Arbitrary size limit on the number of recently modified files.
const HASH_DELAY: Duration = Duration::from_secs(3600);

// The agent side of the manager. Holds the mutable store and watcher, and
// performs the dispatching logic.
impl<S: Filestore, D: Disk> Runner<S, D> {
    async fn run(&mut self, mut rx: Receiver<Operation>) -> anyhow::Result<()> {
        tracing::info!("FileManager starting");
        let mut scan_delay: Option<SystemTime> = None;
        // The work loop will continuously refresh the filesystem when a scan is
        // active, hash files that haven't changed in a while, and otherwise respond
        // to client requests.
        loop {
            let now = SystemTime::now();
            // hash_delay is assessed at every round as many operations can request a file be hashed,
            // independently of timing.
            let mut hash_delay: Option<SystemTime> = None;
            match self.store.hash_next(now)? {
                HashNext::Next(file) => {
                    tracing::debug!("hashing stale file {:?}", &file);
                    let update = match self.disk.snapshot(&file) {
                        Ok(snapshot) => HashUpdate::Hash(snapshot),
                        Err(err) => HashUpdate::Unreadable(err),
                    };
                    self.store.hash_update(file, now + HASH_DELAY, update)?;
                }
                HashNext::Done(delay) => {
                    tracing::debug!("no next file to hash, waiting until {:?}", isotime(delay));
                    hash_delay = Some(delay);
                }
            }
            if scan_delay.is_none() {
                match self.store.tree_scan_next()? {
                    filestore::ScanNext::Done(delay) => {
                        tracing::info!("tree scan done, waiting until {:?}", isotime(delay));
                        scan_delay = Some(delay);
                    }
                    filestore::ScanNext::Next(dir, mut updater) => {
                        tracing::debug!("scanning {:?}", &dir);
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
            let mut poll = tokio::time::interval(Duration::from_millis(1));
            tracing::debug!("waiting for next action");
            tokio::select! {
                _ = tokio::time::sleep(scan_sleep), if scan_delay.is_some() => {
                    tracing::info!("ready to start a new tree scan");
                    scan_delay = None;
                    if let Err(err) = self.store.tree_scan_start(now + self.scan_period) {
                        tracing::error!("tree_scan_start() failed: {:?}", err);
                    }
                }
                _ = tokio::time::sleep(hash_sleep), if hash_delay.is_some() => {
                    tracing::info!("ready to hash a new file");
                }
                _ = poll.tick(), if scan_delay.is_none() || hash_delay.is_none() => {
                    // If either delay is none, we know that more work can be done right away.
                    // The artificial delay still allows for pending events (client, watcher)
                    // to take place.
                }
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
                            tracing::debug!("Received watcher: {:?}", &path);
                            if let Ok((fsize, mtime)) = self.disk.metadata(&path) {
                                self.store.metadata_update(path, fsize, mtime)?;
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
            self.store
                .tree_scan_start(SystemTime::now() + self.scan_period)?;
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        filestore::{self, HashNext, HashUpdate},
        model::{FileSize, ModificationTime},
    };
    use std::{
        collections::{HashSet, VecDeque},
        path::PathBuf,
        sync::{Arc, Mutex},
    };
    use test_log::test;
    use tokio::task::LocalSet;

    // TODO:
    //  - Update the queue when there are dups.
    //  - Handle updates to files!

    #[test(tokio::test)]
    async fn detect_rescans_needed() -> anyhow::Result<()> {
        let local = LocalSet::new();
        let local_ref = &local;
        local
            .run_until(async move {
                let mut store = StoreState::default();
                // Convince the manager to avoid running a scan right away after
                // the first one.
                // TODO: We should mock time instead.
                store.next_scan = SystemTime::now() + Duration::from_secs(30);
                let (manager, store_state, _tx) =
                    test_manager(local_ref, WatcherState::default(), store);

                manager.set_roots(vec![Path::new("/a").into()]).await?;
                let sc1 = store_state.lock().unwrap().scan_round;

                manager.set_roots(vec![Path::new("/a").into()]).await?;
                let sc2 = store_state.lock().unwrap().scan_round;

                manager.set_roots(vec![Path::new("/b").into()]).await?;
                let sc3 = store_state.lock().unwrap().scan_round;

                assert_eq!(sc1, sc2); // no new scan
                assert_ne!(sc2, sc3); // new root -> new scan.

                manager.shutdown().await?;
                Ok(())
            })
            .await
    }

    #[test(tokio::test)]
    async fn set_roots() -> anyhow::Result<()> {
        let local = LocalSet::new();
        let local_ref = &local;
        local
            .run_until(async move {
                let (manager, store_state, _tx) =
                    test_manager(local_ref, WatcherState::default(), StoreState::default());

                manager.set_roots(vec![Path::new("/a").into()]).await?;
                manager.shutdown().await?;

                let inner = store_state.lock().unwrap();
                assert_eq!(inner.roots, vec![Path::new("/a")]);

                Ok(())
            })
            .await
    }

    // ---------------------- helpers -----------------------

    /// Creates a test manager with fake watcher and store states that can be
    /// controlled by the test.
    fn test_manager(
        store_local: &LocalSet,
        watcher_state: WatcherState,
        store_state: StoreState,
    ) -> (FileManager, Arc<Mutex<StoreState>>, Sender<watcher::Update>) {
        let watcher_state = Arc::new(Mutex::new(watcher_state));
        let store_state = Arc::new(Mutex::new(store_state));
        let (tx, rx) = mpsc::channel(1);

        let manager = FileManager::new(
            store_local,
            FakeStore::new(store_state.clone()),
            FakeDisk::new(),
            Box::new(FakeWatcher::new(watcher_state.clone(), rx)),
            std::time::Duration::from_secs(10),
        );

        (manager, store_state, tx)
    }

    #[derive(Default)]
    struct WatcherState {
        roots: Vec<PathBuf>,
    }

    #[derive(Debug)]
    struct StoreState {
        roots: Vec<PathBuf>,
        dirs_to_scan: VecDeque<PathBuf>,
        next_scan: SystemTime,
        scan_round: i32,
    }

    impl Default for StoreState {
        fn default() -> StoreState {
            StoreState {
                roots: vec![],
                dirs_to_scan: VecDeque::new(),
                next_scan: SystemTime::UNIX_EPOCH,
                scan_round: 0,
            }
        }
    }

    struct FakeDisk {}

    impl FakeDisk {
        fn new() -> FakeDisk {
            FakeDisk {}
        }
    }

    impl Disk for FakeDisk {}

    struct FakeWatcher {
        state: Arc<Mutex<WatcherState>>,
        rx: mpsc::Receiver<watcher::Update>,
    }

    #[derive(Debug)]
    struct FakeStore {
        state: Arc<Mutex<StoreState>>,
    }

    impl FakeWatcher {
        fn new(
            state: Arc<Mutex<WatcherState>>,
            rx: mpsc::Receiver<watcher::Update>,
        ) -> FakeWatcher {
            FakeWatcher { state, rx }
        }
    }

    impl FakeStore {
        fn new(state: Arc<Mutex<StoreState>>) -> FakeStore {
            FakeStore { state }
        }
    }

    impl watcher::Watcher for FakeWatcher {
        fn set_roots(&mut self, roots: &[&Path]) -> anyhow::Result<()> {
            let mut state = self.state.lock().unwrap();
            state.roots = roots.iter().map(|r| r.to_path_buf()).collect();
            Ok(())
        }

        fn next(&mut self) -> &mut mpsc::Receiver<watcher::Update> {
            &mut self.rx
        }
    }

    impl Filestore for FakeStore {
        type Scanner<'a> = FakeUpdater;

        fn set_roots(&mut self, roots: &[&Path]) -> anyhow::Result<bool> {
            let mut state = self.state.lock().unwrap();

            let new_set: HashSet<PathBuf> = roots.iter().map(|r| r.to_path_buf()).collect();
            let old_set: HashSet<PathBuf> = state.roots.iter().map(|r| r.to_path_buf()).collect();

            state.roots = roots.iter().map(|r| r.to_path_buf()).collect();
            Ok(new_set != old_set)
        }

        fn tree_scan_start(&mut self, next_scan: std::time::SystemTime) -> anyhow::Result<()> {
            let mut state = self.state.lock().unwrap();
            state.next_scan = next_scan;
            state.scan_round += 1;
            Ok(())
        }

        fn hash_next(&mut self, now: SystemTime) -> anyhow::Result<filestore::HashNext> {
            Ok(HashNext::Done(now + Duration::from_secs(10)))
        }

        fn tree_scan_next(&mut self) -> anyhow::Result<filestore::ScanNext<Self::Scanner<'_>>> {
            let state = self.state.lock().unwrap();
            if state.dirs_to_scan.is_empty() {
                tracing::info!("scan is done");
                Ok(filestore::ScanNext::Done(state.next_scan))
            } else {
                // Keep returning the same element until we get an update for it.
                let next = state.dirs_to_scan.front().unwrap().clone();
                tracing::info!("returning next directory {:?}", &next);
                Ok(filestore::ScanNext::Next(next, FakeUpdater {}))
            }
        }

        fn hash_update(
            &mut self,
            _file: PathBuf,
            _next: SystemTime,
            _update: HashUpdate,
        ) -> anyhow::Result<()> {
            todo!()
        }

        fn metadata_update(
            &mut self,
            _path: PathBuf,
            _fsize: FileSize,
            _mtime: ModificationTime,
        ) -> anyhow::Result<()> {
            todo!()
        }
    }

    struct FakeUpdater {}
    impl filestore::Scanner for FakeUpdater {
        fn update(&mut self, _: &filestore::ScanUpdate) -> anyhow::Result<()> {
            todo!()
        }

        fn commit(self, _complete: bool) -> anyhow::Result<()> {
            Ok(())
        }

        fn error(self, _error: std::io::Error) -> anyhow::Result<()> {
            todo!()
        }
    }
}
