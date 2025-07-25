use super::*;
use crate::{
    filestore::{self, HashUpdate, Next},
    model::{FileSize, ModificationTime},
};
use crypto::model::EncryptionGroup;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, Weak},
    task::{Context, Poll, Waker},
    time::UNIX_EPOCH,
};
use test_log::test;
use tokio::{sync::oneshot, task::LocalSet};

// Have one file dirty, with next set at +5.
// Send a metadata update within that time, with
// an actual change.
// Check that the next is set further out.
// This should not be true if the update is a noop.

#[test(tokio::test)]
async fn wait_for_tree_scan() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let local_ref = &local;
    local
        .run_until(async move {
            let (manager, _store, clock, _tx) =
                test_manager(local_ref, WatcherState::default(), StoreState::default());

            // Not a very deep test: we just confirm that when there is nothing
            // to scan, we wait until the next round.
            let (delay, _waker) = clock.wait("tree_scan").await;
            assert_eq!(delay, Duration::ZERO); // it's UNIX_EPOCH and we scan next then.
            manager.shutdown().await?;

            Ok(())
        })
        .await
}

#[test(tokio::test)]
async fn detect_rescans_needed() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let local_ref = &local;
    local
        .run_until(async move {
            let mut store = StoreState::default();
            // Convince the manager to avoid running a scan right away after
            // the first one.
            store.next_scan = t(10);

            let (manager, store_state, _clock, _tx) =
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
            let (manager, store_state, _clock, _tx) =
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

struct FakeClock {
    inner: Arc<Mutex<InnerFakeClock>>,
}

struct FakeClockHandler {
    inner: Arc<Mutex<InnerFakeClock>>,
}

impl FakeClockHandler {
    fn new() -> (FakeClock, FakeClockHandler) {
        let inner = Arc::new(Mutex::new(InnerFakeClock {
            now: UNIX_EPOCH,
            pending: HashMap::new(),
            expected: HashMap::new(),
        }));
        (
            FakeClock {
                inner: inner.clone(),
            },
            FakeClockHandler { inner },
        )
    }

    #[allow(dead_code)] // TODO: use it!
    fn set_now(&self, now: SystemTime) {
        let mut inner = self.inner.lock().unwrap();
        inner.now = now;
    }

    async fn wait(&self, id: &str) -> (Duration, Waker) {
        let (tx, rx) = oneshot::channel();
        {
            let mut inner = self.inner.lock().unwrap();
            // Either the sleep was already initiated...
            let early_result = get_pending_future(&mut inner, id);
            if let Some((delay, waker)) = early_result {
                tracing::debug!("sleep({}) was pending already", id);
                return (delay, waker);
            }
            // ... or we can put a oneshot with the lock held, so it's
            // already present when it arrives.
            inner.expected.insert(id.to_owned(), tx);
        }
        tracing::debug!("waiting for sleep({})", id);
        rx.await.unwrap();
        // Now we should have received the result when we lock again.
        let mut inner = self.inner.lock().unwrap();
        get_pending_future(&mut inner, id).unwrap()
    }
}

fn get_pending_future(
    inner: &mut MutexGuard<'_, InnerFakeClock>,
    id: &str,
) -> Option<(Duration, Waker)> {
    if let Some(weak) = inner.pending.get_mut(id) {
        let Some(mu) = weak.upgrade() else {
            return None;
        };
        let mut v = mu.lock().unwrap();
        let waker = v.waker.take();
        return Some((v.delay, waker.unwrap()));
    }
    None
}

struct InnerFakeClock {
    now: SystemTime,
    pending: HashMap<String, Weak<Mutex<InnerFakeSleep>>>,
    expected: HashMap<String, oneshot::Sender<()>>,
}

impl InnerFakeClock {
    fn register(&mut self, id: String, sleep: Weak<Mutex<InnerFakeSleep>>) {
        if let Some(expected) = self.expected.remove(&id) {
            expected.send(()).expect("failed to send");
        }
        self.pending.insert(id, sleep);
    }
}

impl Clock for FakeClock {
    fn now(&self) -> SystemTime {
        let inner = self.inner.lock().unwrap();
        inner.now
    }

    fn sleep(&self, delay: Duration, id: &str) -> impl Future<Output = ()> + '_ {
        let inner = InnerFakeSleep {
            delay,
            id: id.to_owned(),
            waker: None,
            parent: Arc::downgrade(&self.inner),
        };
        FakeSleep {
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

struct FakeSleep {
    inner: Arc<Mutex<InnerFakeSleep>>,
}

struct InnerFakeSleep {
    delay: Duration,
    id: String,
    waker: Option<Waker>,
    parent: Weak<Mutex<InnerFakeClock>>,
}

impl Future for FakeSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.inner.lock().unwrap();
        match inner.parent.upgrade() {
            Some(lock) => {
                inner.waker = Some(cx.waker().clone());
                let mut clock = lock.lock().unwrap();
                clock.register(inner.id.clone(), Arc::downgrade(&self.inner));

                Poll::Pending
            }
            // If there is nobody to wake this up, we have no other option than
            // completing right away.
            None => Poll::Ready(()),
        }
    }
}

/// Creates a test manager with fake watcher and store states that can be
/// controlled by the test.
fn test_manager(
    store_local: &LocalSet,
    watcher_state: WatcherState,
    store_state: StoreState,
) -> (
    FileManager,
    Arc<Mutex<StoreState>>,
    FakeClockHandler,
    Sender<watcher::Update>,
) {
    let watcher_state = Arc::new(Mutex::new(watcher_state));
    let store_state = Arc::new(Mutex::new(store_state));
    let (clock, clock_state) = FakeClockHandler::new();
    let (tx, rx) = mpsc::channel(1);

    let manager = FileManager::new(
        store_local,
        FakeStore::new(store_state.clone()),
        FakeDisk::new(),
        clock,
        Box::new(FakeWatcher::new(watcher_state.clone(), rx)),
    );

    (manager, store_state, clock_state, tx)
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

fn t(s: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(s)
}
