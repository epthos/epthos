use super::*;
use crate::{
    datamanager::{BackupResult, BackupSlot, InFlight},
    disk::ScanEntry,
    fake_clock::FakeClockHandler,
    fake_disk::FakeDisk,
    filestore::{self, HashUpdate, Next},
    model::{FileSize, ModificationTime},
};
use crypto::model::EncryptionGroup;
use std::{
    collections::{HashSet, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::UNIX_EPOCH,
};
use test_log::test;

// Have one file dirty, with next set at +5.
// Send a metadata update within that time, with
// an actual change.
// Check that the next is set further out.
// This should not be true if the update is a noop.

#[test(tokio::test)]
async fn wait_for_tree_scan() -> anyhow::Result<()> {
    let (manager_ctx, _store, clock, _tx) =
        test_manager(WatcherState::default(), StoreState::default());

    // Not a very deep test: we just confirm that when there is nothing
    // to scan, we wait until the next round.
    let handle = clock.wait("tree_scan").await;
    assert_eq!(handle.delay, Duration::ZERO); // it's UNIX_EPOCH and we scan next then.
    manager_ctx.manager.shutdown();
    manager_ctx.handle.await??;

    Ok(())
}

#[test(tokio::test)]
async fn detect_rescans_needed() -> anyhow::Result<()> {
    let mut store = StoreState::default();
    // Convince the manager to avoid running a scan right away after
    // the first one.
    store.next_scan = t(10);

    let (manager_ctx, store_state, clock, _tx) = test_manager(WatcherState::default(), store);

    manager_ctx
        .manager
        .set_roots(vec![Path::new("/a").into()])
        .await?;
    clock.wait("tree_scan").await;
    let sc1 = store_state.lock().unwrap().scan_round;

    manager_ctx
        .manager
        .set_roots(vec![Path::new("/a").into()])
        .await?;
    clock.wait("tree_scan").await;
    let sc2 = store_state.lock().unwrap().scan_round;

    manager_ctx
        .manager
        .set_roots(vec![Path::new("/b").into()])
        .await?;
    clock.wait("tree_scan").await;
    let sc3 = store_state.lock().unwrap().scan_round;

    assert_eq!(sc1, sc2); // no new scan
    assert_ne!(sc2, sc3); // new root -> new scan.

    manager_ctx.manager.shutdown();
    manager_ctx.handle.await??;
    Ok(())
}

#[test(tokio::test)]
async fn set_roots() -> anyhow::Result<()> {
    let (manager_ctx, store_state, _clock, _tx) =
        test_manager(WatcherState::default(), StoreState::default());

    manager_ctx
        .manager
        .set_roots(vec![Path::new("/a").into()])
        .await?;
    manager_ctx.manager.shutdown();
    manager_ctx.handle.await??;

    let inner = store_state.lock().unwrap();
    assert_eq!(inner.roots, vec![Path::new("/a")]);

    Ok(())
}

// ---------------------- helpers -----------------------

/// Creates a test manager with fake watcher and store states that can be
/// controlled by the test.
fn test_manager(
    watcher_state: WatcherState,
    store_state: StoreState,
) -> (
    FileManagerContext,
    Arc<Mutex<StoreState>>,
    FakeClockHandler,
    Sender<watcher::Update>,
) {
    let watcher_state = Arc::new(Mutex::new(watcher_state));
    let store_state = Arc::new(Mutex::new(store_state));
    let (clock, clock_state) = FakeClockHandler::new();
    let (tx, rx) = mpsc::channel(1);
    let (backup_tx, backup_rx) = mpsc::channel(1);

    let manager = FileManager::create(
        FakeStore::new(store_state.clone()),
        FakeDisk::new(),
        clock,
        Box::new(FakeWatcher::new(watcher_state.clone(), rx)),
        FakeDataManager {
            _tx: backup_tx,
            rx: backup_rx,
        },
    )
    .unwrap();

    (manager, store_state, clock_state, tx)
}

struct FakeDataManager {
    _tx: Sender<FakeSlot>,
    rx: Receiver<FakeSlot>,
}

struct FakeSlot {}

impl DataManager for FakeDataManager {
    type Slot = FakeSlot;

    fn backup_slots(&mut self) -> &mut Receiver<Self::Slot> {
        &mut self.rx
    }

    async fn shutdown(self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn in_flight(&mut self) -> anyhow::Result<Vec<InFlight>> {
        Ok(vec![])
    }
}

impl BackupSlot for FakeSlot {
    async fn enqueue(self, _path: PathBuf) -> anyhow::Result<oneshot::Receiver<BackupResult>> {
        todo!()
    }
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

struct FakeWatcher {
    state: Arc<Mutex<WatcherState>>,
    rx: mpsc::Receiver<watcher::Update>,
}

#[derive(Debug)]
struct FakeStore {
    state: Arc<Mutex<StoreState>>,
}

impl FakeWatcher {
    fn new(state: Arc<Mutex<WatcherState>>, rx: mpsc::Receiver<watcher::Update>) -> FakeWatcher {
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

    fn hash_next(&mut self, now: SystemTime) -> anyhow::Result<Next<()>> {
        Ok(Next::Done(now + Duration::from_secs(10)))
    }

    fn tree_scan_next(&mut self) -> anyhow::Result<Next<Self::Scanner<'_>>> {
        let state = self.state.lock().unwrap();
        if state.dirs_to_scan.is_empty() {
            tracing::info!("scan is done");
            Ok(filestore::Next::Done(state.next_scan))
        } else {
            // Keep returning the same element until we get an update for it.
            let next = state.dirs_to_scan.front().unwrap().clone();
            tracing::info!("returning next directory {:?}", &next);
            Ok(filestore::Next::Next(next, FakeUpdater {}))
        }
    }

    fn hash_update(
        &mut self,
        _file: PathBuf,
        _now: SystemTime,
        _update: HashUpdate,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn metadata_update(
        &mut self,
        _path: PathBuf,
        _now: SystemTime,
        _fsize: FileSize,
        _mtime: ModificationTime,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn backup_next(&mut self, _now: SystemTime) -> anyhow::Result<Next<EncryptionGroup>> {
        Ok(Next::Done(_now + Duration::from_secs(1)))
    }

    fn backup_start(&mut self, _path: PathBuf) -> anyhow::Result<()> {
        todo!()
    }

    fn backup_pending(&mut self) -> anyhow::Result<Vec<(PathBuf, crypto::model::EncryptionGroup)>> {
        Ok(vec![])
    }

    fn backup_done(
        &mut self,
        _path: PathBuf,
        _next: SystemTime,
        _update: HashUpdate,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn get_stats(&mut self) -> anyhow::Result<Stats> {
        todo!()
    }
}

struct FakeUpdater {}
impl filestore::Scanner for FakeUpdater {
    fn update(&mut self, _now: SystemTime, _: &ScanEntry) -> anyhow::Result<()> {
        todo!()
    }

    fn commit(self, _complete: bool) -> anyhow::Result<()> {
        Ok(())
    }

    fn error(self, _error: anyhow::Error) -> anyhow::Result<()> {
        todo!()
    }
}

fn t(s: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(s)
}
