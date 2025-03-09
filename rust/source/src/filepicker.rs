//! Filepicker decides which file needs to be checked
//! and backed up over time. It does not interact with
//! the filesystem directly but handles timing.

// TODO:
//  - scan should detect candidates for full hashing and accelerate their hashing.

use crate::{
    filestore::{self, Connection, Filestore, HashNext, HashUpdate, Scanner},
    watcher,
};
use anyhow::{anyhow, Context};
use std::{
    collections::VecDeque,
    ffi::OsString,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::{JoinHandle, LocalSet},
};

/// The async public API.
pub struct Picker {
    tx: Sender<PickerOp>,
    handle: JoinHandle<anyhow::Result<()>>,
}

#[derive(Debug)]
pub enum Next {
    /// Scan the provided directory, returing its content via the provided
    /// channel. This is expected to happen without interleaving other calls
    /// to Picker.next().
    ScanDir(PathBuf, Sender<ScanUpdate>),
    CheckFile(PathBuf),
}

#[derive(Debug)]
pub enum ScanUpdate {
    File(OsString, filestore::FileSize, filestore::ModificationTime),
    Directory(OsString),
    /// Total failure to list the content of the directory.
    Error(std::io::Error),
    /// Completed the scan. Return true if all the entries were
    /// reported.
    Done(bool),
}

#[derive(Debug)]
pub enum Update {
    Hash(ring::digest::Digest),
    Unreadable(std::io::Error),
}

/// Create a production Picker, using the production store, and
/// doing full scans at the specified period. The store is single-threaded
/// and will execute in the context of store_local.
pub fn new(
    store_local: &LocalSet,
    scan_period: Duration,
    db: &Path,
    rand: crypto::SharedRandom,
) -> anyhow::Result<Picker> {
    let store = Connection::new(db, rand)?;

    Ok(Picker::new(
        store_local,
        store,
        watcher::new()?,
        scan_period,
        MAX_QUEUE_SIZE,
    ))
}

impl Picker {
    fn new<S: Filestore + 'static>(
        store_local: &LocalSet,
        store: S,
        watcher: Box<dyn watcher::Watcher + Send>,
        scan_period: Duration,
        max_queue_size: usize,
    ) -> Picker {
        let (tx, rx) = mpsc::channel::<PickerOp>(1);
        let mut runner = PickerRunner {
            store,
            watcher,
            scan_period,
            pending: VecDeque::new(),
            max_queue_size,
        };
        let handle = store_local.spawn_local(async move {
            let result = runner.run(rx).await;
            if let Err(err) = &result {
                tracing::error!("PickerImpl failed: {:?}", err);
            }
            result
        });
        Picker { tx, handle }
    }

    /// Shutdown can be called in parallel with any pending call and will interrupt them,
    /// shutting down the file picker as early as possible.
    #[allow(dead_code)]
    pub async fn shutdown(self) -> anyhow::Result<()> {
        drop(self.tx);
        self.handle.await?
    }

    // set_roots() will update the roots for file scanning. The updated value will be used
    // as soon as possible.
    #[tracing::instrument(skip(self))]
    pub async fn set_roots(&self, roots: &[&Path]) -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel::<anyhow::Result<()>>(1);
        let op = PickerOp::SetRoots(roots.iter().map(|r| r.to_path_buf()).collect(), tx);
        self.tx.send(op).await?;
        rx.recv().await.context("WatcherImpl closed early")??;
        Ok(())
    }

    /// next() returns the next file or directory to be actualized. This call can block for
    /// extended periods of time. Only one outstanding call is accepted, the second will
    /// fail immediately.
    #[tracing::instrument(skip(self))]
    pub async fn next(&self) -> anyhow::Result<Next> {
        let (tx, mut rx) = mpsc::channel(1);
        let op = PickerOp::Next(tx);
        self.tx.send(op).await?;
        rx.recv().await.context("WatcherImpl closed early")?
    }

    /// Updates the content of the directory or file returned by next().
    #[tracing::instrument(skip(self))]
    pub async fn update(&self, next: PathBuf, update: Update) -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        let op = PickerOp::Update(next, update, tx);
        self.tx.send(op).await?;
        rx.recv().await.context("WatcherImpl closed early")?
    }
}

enum PickerOp {
    SetRoots(Vec<PathBuf>, Sender<anyhow::Result<()>>),
    Next(Sender<anyhow::Result<Next>>),
    Update(PathBuf, Update, Sender<anyhow::Result<()>>),
}

struct PickerRunner<S>
where
    S: Filestore,
{
    store: S,
    watcher: Box<dyn watcher::Watcher + Send>,
    scan_period: Duration,
    pending: VecDeque<watcher::Update>,
    max_queue_size: usize,
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
const MAX_QUEUE_SIZE: usize = 1000;
const HASH_DELAY: Duration = Duration::from_secs(3600);

// The agent side of the Picker. Holds the mutable store and watcher, and
// performs the dispatching logic.
impl<S: Filestore> PickerRunner<S> {
    async fn run(&mut self, mut rx: Receiver<PickerOp>) -> anyhow::Result<()> {
        tracing::info!("PickerRunner starting");
        let mut next_tx: Option<Sender<anyhow::Result<Next>>> = None;
        let mut scan_delay: Option<SystemTime> = None;
        loop {
            let now = SystemTime::now();
            if let Some(tx) = next_tx.as_ref() {
                // The client is waiting for the next action to take.
                // We prioritize the file watcher over directory scanning for simplicity for now.
                // Many possible cases to handle (starvation, etc)
                if !self.pending.is_empty() {
                    let next = self.pending.pop_back().unwrap();
                    tracing::debug!("hashing modified file {:?}", &next);
                    match next {
                        watcher::Update::File(file) => {
                            let _ = tx.send(Ok(Next::CheckFile(file))).await;
                            next_tx = None;
                        }
                        watcher::Update::Directory(_) => {}
                    }
                    continue;
                }
                // Second in priority order: files that haven't been hashed in a while.
                match self.store.hash_next(now)? {
                    HashNext::Next(file) => {
                        tracing::debug!("hashing stale file {:?}", &file);
                        let _ = tx.send(Ok(Next::CheckFile(file))).await;
                        next_tx = None;
                        continue;
                    }
                    HashNext::Done(delay) => {
                        tracing::debug!(
                            "no next file to hash, waiting until {:?}",
                            isotime(delay.clone())
                        );
                        scan_delay = Some(if let Some(previous) = scan_delay {
                            std::cmp::min(previous, delay)
                        } else {
                            delay
                        });
                    }
                }
                // Finally, we do a filesystem walk, if it's due.
                match self.store.tree_scan_next() {
                    Err(err) => {
                        let _ = tx.send(Err(err)).await;
                        next_tx = None;
                    }
                    Ok(filestore::ScanNext::Done(delay)) => {
                        tracing::info!(
                            "tree scan done, waiting until {:?}",
                            isotime(delay.clone())
                        );
                        scan_delay = Some(if let Some(previous) = scan_delay {
                            std::cmp::min(previous, delay)
                        } else {
                            delay
                        });
                    }
                    Ok(filestore::ScanNext::Next(dir, updater)) => {
                        tracing::debug!("scanning {:?}", &dir);
                        let (file_tx, mut file_rx) = mpsc::channel(1);
                        let next = Ok(Next::ScanDir(dir, file_tx));
                        // We can now fulfill the current "next()" call.
                        let _ = tx.send(next).await;
                        next_tx = None;
                        // The client is expected to feed us the results of the ScanDir right away,
                        // other operations are paused in the meantime, as this is single-threaded
                        // anyways.
                        // Some operations will kill the updater. This API does not enforce
                        // sequencing via types, so we need to manage those errors dynamically.
                        let mut updater = Some(updater);
                        while let Some(update) = file_rx.recv().await {
                            match update {
                                ScanUpdate::File(name, size, modification) => {
                                    let updater = updater
                                        .as_mut()
                                        .ok_or_else(|| anyhow!("update was already complete"))?;
                                    updater.update(&filestore::ScanUpdate::File(
                                        name,
                                        size,
                                        modification,
                                    ))?;
                                }
                                ScanUpdate::Directory(os_string) => {
                                    let updater = updater
                                        .as_mut()
                                        .ok_or_else(|| anyhow!("update was already complete"))?;
                                    updater.update(&filestore::ScanUpdate::Directory(os_string))?;
                                }
                                ScanUpdate::Error(error) => {
                                    let updater = updater
                                        .take()
                                        .ok_or_else(|| anyhow!("update was already complete"))?;
                                    updater.error(error)?;
                                }
                                ScanUpdate::Done(complete) => {
                                    let updater = updater
                                        .take()
                                        .ok_or_else(|| anyhow!("update was already complete"))?;
                                    updater.commit(complete)?;
                                }
                            }
                        }
                        tracing::debug!("received all next() content");
                    }
                }
            }
            let duration = duration_or_zero(now, &scan_delay);
            tokio::select! {
                // Only active when: (1) there is a pending next() and (2) it can't be
                // fulfilled right away as there is no ongoing scan.
                _ = tokio::time::sleep(duration), if scan_delay.is_some() => {
                    tracing::info!("ready to start a new tree scan");
                    scan_delay = None;
                    if let Err(err) = self.store.tree_scan_start(SystemTime::now() + self.scan_period) {
                        tracing::error!("tree_scan_start() failed: {:?}", err);
                    }
                }
                op = rx.recv() => {
                    match op {
                        None => break,
                        Some(PickerOp::SetRoots(roots, tx)) => {
                            let refs: Vec<&Path> = roots.iter().map(|r| r.as_ref()).collect();
                            let _ = tx.send(self.set_roots(&refs)).await;
                        }
                        Some(PickerOp::Next(tx)) => {
                            if next_tx.is_some() {
                                let _ = tx.send(Err(anyhow!("Only one next() call at a time"))).await;
                            } else {
                                tracing::debug!("ready to return next()");
                                next_tx = Some(tx);
                            }
                        }
                        Some(PickerOp::Update(path, update, tx)) => {
                            let update = match update {
                                Update::Hash(digest) => HashUpdate::Hash(digest),
                                Update::Unreadable(error) => HashUpdate::Unreadable(error),
                            };
                            let result = self.store.hash_update(path, SystemTime::now() + HASH_DELAY, update);
                            let _ = tx.send(result).await;
                        }
                    }
                }
                update = self.watcher.next().recv() => {
                    match update {
                        None => {
                            tracing::error!("watcher died...");
                            break;
                        },
                        Some(update) => {
                            tracing::debug!("Receiver watcher: {:?}", update);
                            self.pending.push_front(update);
                        },
                    }
                    while self.pending.len() > self.max_queue_size {
                        self.pending.pop_back();
                    }
                }
            }
        }
        tracing::info!("PickerRunner shutting down");
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

fn isotime<T>(dt: T) -> anyhow::Result<String>
where
    T: Into<time::OffsetDateTime>,
{
    Ok(dt
        .into()
        .format(&time::format_description::well_known::Iso8601::DEFAULT)
        .context("can't format timestamp")?)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::filestore::{self, HashNext};
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
                let (picker, store_state, _tx) =
                    test_picker(local_ref, WatcherState::default(), StoreState::default(), 2);

                picker.set_roots(&[Path::new("/a")]).await?;
                let sc1 = store_state.lock().unwrap().scan_round;

                picker.set_roots(&[Path::new("/a")]).await?;
                let sc2 = store_state.lock().unwrap().scan_round;

                picker.set_roots(&[Path::new("/b")]).await?;
                let sc3 = store_state.lock().unwrap().scan_round;

                assert_eq!(sc1, sc2); // no new scan
                assert_ne!(sc2, sc3); // new root -> new scan.

                picker.shutdown().await?;
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
                let (picker, store_state, _tx) =
                    test_picker(local_ref, WatcherState::default(), StoreState::default(), 2);

                picker.set_roots(&[Path::new("/a")]).await?;
                picker.shutdown().await?;

                let inner = store_state.lock().unwrap();
                assert_eq!(inner.roots, vec![Path::new("/a")]);

                Ok(())
            })
            .await
    }

    #[test(tokio::test)]
    async fn next_gets_from_store() -> anyhow::Result<()> {
        let local = LocalSet::new();
        let local_ref = &local;
        local
            .run_until(async move {
                let p1 = Path::new("/a").to_path_buf();
                let p2 = Path::new("/a/b").to_path_buf();

                let mut store_state = StoreState::default();
                store_state.dirs_to_scan = VecDeque::from([p1.clone(), p2.clone()]);
                let (picker, _, _tx) =
                    test_picker(local_ref, WatcherState::default(), store_state, 2);
                {
                    let d = picker.next().await?;
                    let Next::ScanDir(r, _) = d else {
                        panic!("unexpected")
                    };
                    assert_eq!(r, p1);
                }
                {
                    let d = picker.next().await?;
                    let Next::ScanDir(_, _) = &d else {
                        panic!("unexpected")
                    };
                }
                picker.shutdown().await?;
                Ok(())
            })
            .await
    }

    #[test(tokio::test)]
    async fn only_one_next_inflight() -> anyhow::Result<()> {
        let local = LocalSet::new();
        let local_ref = &local;
        local
            .run_until(async move {
                let (picker, _, _tx) =
                    test_picker(local_ref, WatcherState::default(), StoreState::default(), 2);

                let n1 = picker.next();
                let n2 = picker.next();

                let first;
                tokio::select! {
                    r = n1 => { first = r; }
                    r = n2 => { first = r; }
                }
                picker.shutdown().await?;

                let Err(err) = first else {
                    panic!("unexpected success")
                };
                assert_eq!(err.to_string(), "Only one next() call at a time");
                Ok(())
            })
            .await
    }

    #[test(tokio::test)]
    async fn ignore_directory_updates() -> anyhow::Result<()> {
        let local = LocalSet::new();
        let local_ref = &local;
        local
            .run_until(async move {
                let (picker, _, tx) =
                    test_picker(local_ref, WatcherState::default(), StoreState::default(), 2);

                let tx_move = tx.clone(); // keep a sender so the receiver stays alive.
                let j: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
                    tx_move
                        .send(watcher::Update::Directory(Path::new("a").into()))
                        .await?;
                    Ok(())
                });

                // As we sent no useful information, next() should not yield a result.
                tokio::select! {
                    _ = picker.next() => panic!("should block"),
                    _ = j => {},
                }

                picker.shutdown().await?;
                Ok(())
            })
            .await
    }

    #[test(tokio::test)]
    async fn file_updates_take_precedence() -> anyhow::Result<()> {
        let local = LocalSet::new();
        let local_ref = &local;
        local
            .run_until(async move {
                let p = Path::new("p").to_path_buf();

                let mut store_state = StoreState::default();
                store_state.dirs_to_scan = VecDeque::from([p.clone()]);
                let (picker, _, tx) =
                    test_picker(local_ref, WatcherState::default(), store_state, 2);

                picker.set_roots(&[Path::new("/a")]).await?;
                let tx_move = tx.clone(); // keep a sender so the receiver stays alive.
                let j: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
                    tx_move
                        .send(watcher::Update::File(Path::new("a").into()))
                        .await?;
                    tx_move
                        .send(watcher::Update::File(Path::new("b").into()))
                        .await?;
                    Ok(())
                });

                // Ensure we publish the watcher data first, before calling next() to avoid
                // a race condition where we could return the content of the scan first.
                j.await??;

                // The files are returned in the order they were provided.
                let Next::CheckFile(next) = picker.next().await? else {
                    panic!("must be a file")
                };
                assert_eq!(next, Path::new("a"));
                let Next::CheckFile(next) = picker.next().await? else {
                    panic!("must be a file")
                };
                assert_eq!(next, Path::new("b"));

                // Finally we return the next scan directory as we exhausted the watcher
                // queue.
                {
                    let d = picker.next().await?;
                    let Next::ScanDir(r, _) = &d else {
                        panic!("unexpected")
                    };
                    assert_eq!(r, &p);
                }
                picker.shutdown().await?;
                Ok(())
            })
            .await
    }

    #[test(tokio::test)]
    async fn cap_queue_size() -> anyhow::Result<()> {
        let local = LocalSet::new();
        let local_ref = &local;
        local
            .run_until(async move {
                let p = Path::new("p").to_path_buf();

                let mut store_state = StoreState::default();
                store_state.dirs_to_scan = VecDeque::from([p.clone()]);
                let (picker, _, tx) = test_picker(
                    local_ref,
                    WatcherState::default(),
                    store_state,
                    1, /* only keep the latest*/
                );

                tx.send(watcher::Update::File(Path::new("a").into()))
                    .await?;
                tx.send(watcher::Update::File(Path::new("b").into()))
                    .await?;
                tx.send(watcher::Update::File(Path::new("c").into()))
                    .await?;

                let mut sequence = vec![];
                loop {
                    match picker.next().await? {
                        Next::ScanDir(path_buf, _) => {
                            sequence.push(path_buf);
                            break;
                        }
                        Next::CheckFile(path_buf) => {
                            sequence.push(path_buf);
                        }
                    }
                }
                // With a channel(1) we still have one item in transit. So the sequence
                // above will necessarily have had "a" removed, but "b" is in a race
                // with the call to next. So we have a few invariants but not a hard sequence:
                let a = Path::new("a").to_path_buf();
                assert!(!sequence.contains(&a), "got: {:?}", &sequence);
                assert_eq!(&sequence[sequence.len() - 1], &p);

                picker.shutdown().await?;
                Ok(())
            })
            .await
    }

    // ---------------------- helpers -----------------------

    /// Creates a test Picker with fake watcher and store states that can be
    /// controlled by the test.
    fn test_picker(
        store_local: &LocalSet,
        watcher_state: WatcherState,
        store_state: StoreState,
        queue_size: usize,
    ) -> (Picker, Arc<Mutex<StoreState>>, Sender<watcher::Update>) {
        let watcher_state = Arc::new(Mutex::new(watcher_state));
        let store_state = Arc::new(Mutex::new(store_state));
        let (tx, rx) = mpsc::channel(1);

        let picker = Picker::new(
            store_local,
            FakeStore::new(store_state.clone()),
            Box::new(FakeWatcher::new(watcher_state.clone(), rx)),
            std::time::Duration::from_secs(10),
            queue_size,
        );

        (picker, store_state, tx)
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
