//! The watcher module handles the filesystem side of watching for file
//! changes.

use notify::Event;
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};
use tokio::sync::mpsc;

/// Public API of a watcher.
pub trait Watcher {
    /// Set the root directories that should be watched.
    fn set_roots(&mut self, roots: &[&Path]) -> anyhow::Result<()>;
    /// Wait for the next filesystem change.
    fn next(&mut self) -> &mut mpsc::Receiver<Update>;
}

#[derive(Debug, PartialEq, Hash, Eq)]
pub enum Update {
    File(PathBuf),
    Directory(PathBuf),
}

/// Create a watcher which uses the production components (notify-rs, etc)
pub fn new() -> anyhow::Result<Box<dyn Watcher + Send>> {
    let (tx, rx) = std::sync::mpsc::channel();
    let watcher = Box::new(notify::recommended_watcher(tx)?);

    Ok(Box::new(WatcherImpl::new(watcher, rx)))
}

struct WatcherImpl {
    roots: HashSet<PathBuf>,
    watcher: Box<dyn notify::Watcher + Send>,
    async_rx: mpsc::Receiver<Update>,
}

impl WatcherImpl {
    pub fn new(
        watcher: Box<dyn notify::Watcher + Send>,
        rx: std::sync::mpsc::Receiver<notify::Result<Event>>,
    ) -> Self {
        let (async_tx, async_rx) = mpsc::channel(1);
        tokio::task::spawn_blocking(move || {
            loop {
                match rx.recv() {
                    Ok(msg) => match msg {
                        Ok(event) => {
                            let update = map_update(&event);
                            tracing::debug!("notify event: {:?} -> {:?}", &event, &update);
                            if let Some(update) = update
                                && let Err(err) = async_tx.blocking_send(update)
                            {
                                tracing::info!(
                                    "failed to send from WatcherImpl's copier: {:?}",
                                    err
                                );
                                break;
                            }
                        }
                        Err(err) => {
                            tracing::info!("notifier failed to return event: {:?}", err);
                        }
                    },
                    Err(err) => {
                        tracing::info!("failed to receive from WatcherImpl's copier: {:?}", err);
                        break;
                    }
                }
            }
        });
        WatcherImpl {
            roots: HashSet::new(),
            watcher,
            async_rx,
        }
    }
}

// Basic mapping of notify Events to a simpler model.
//
// Note in particular that missing files and directories are skipped
// altogether: full scans are expected to pick up the pieces.
fn map_update(event: &Event) -> Option<Update> {
    match event.kind {
        notify::EventKind::Create(_) => map_path(&event.paths),
        notify::EventKind::Modify(_) => map_path(&event.paths),
        _ => {
            tracing::debug!("ignoring event {:?}", &event);
            None
        }
    }
}

fn map_path(path: &[PathBuf]) -> Option<Update> {
    if path.len() != 1 {
        return None;
    }
    match std::fs::metadata(&path[0]) {
        Ok(md) => {
            if md.is_file() {
                Some(Update::File(path[0].clone()))
            } else if md.is_dir() {
                Some(Update::Directory(path[0].clone()))
            } else {
                None
            }
        }
        Err(_) => None,
    }
}

impl Watcher for WatcherImpl {
    fn set_roots(&mut self, roots: &[&Path]) -> anyhow::Result<()> {
        let mut remaining = self.roots.clone();
        for root in roots.iter() {
            remaining.remove(*root);
            if self.roots.contains(*root) {
                continue;
            }
            self.watcher.watch(root, notify::RecursiveMode::Recursive)?;
        }
        for root in remaining {
            self.watcher.unwatch(&root)?;
        }
        Ok(())
    }

    fn next(&mut self) -> &mut mpsc::Receiver<Update> {
        &mut self.async_rx
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::*;
    use async_trait::async_trait;
    use test_log::test;

    #[test(tokio::test)]
    async fn detect_creation() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;

        let events: HashSet<Update> = observe(&temp, |path: PathBuf| async move {
            std::fs::File::create(path.join("f"))?;
            std::fs::create_dir(path.join("d"))?;

            Ok(())
        })
        .await?
        .into_iter()
        .collect();

        assert_eq!(
            events,
            HashSet::from_iter([
                Update::File(temp.path().join("f")),
                Update::Directory(temp.path().join("d")),
            ])
        );
        Ok(())
    }

    #[test(tokio::test)]
    async fn detect_move() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;

        // Create the file before we observe the directory, to only get one event.
        std::fs::File::create(temp.path().join("f"))?;

        let events = observe(&temp, |path: PathBuf| async move {
            std::fs::rename(path.join("f"), path.join("f2"))?;
            Ok(())
        })
        .await?;

        assert_eq!(events, vec![Update::File(temp.path().join("f2"))]);
        Ok(())
    }

    #[async_trait]
    trait AsyncOperation {
        async fn execute(self, path: PathBuf) -> anyhow::Result<()>;
    }

    #[async_trait]
    impl<F, Fut> AsyncOperation for F
    where
        F: Fn(PathBuf) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = anyhow::Result<()>> + Send,
    {
        async fn execute(self, path: PathBuf) -> anyhow::Result<()> {
            self(path).await
        }
    }

    async fn observe<O>(temp: &tempfile::TempDir, operation: O) -> anyhow::Result<Vec<Update>>
    where
        O: AsyncOperation + Send + Sync + 'static,
    {
        let mut w = new()?;
        w.set_roots(&[temp.path()])?;

        let mut j = tokio::spawn(operation.execute(temp.path().to_path_buf()));

        let mut done = false;
        let mut events = vec![];
        loop {
            tokio::select! {
                event = w.next().recv() => {
                    tracing::info!("event {:?}", event);
                    if let Some(event) = event {
                        events.push(event);
                    }
                }
                _ = &mut j, if !done => {
                    tracing::info!("done changing the fs, waiting a bit");
                    done = true;
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(2)), if done => {
                    break;
                }
            }
        }
        Ok(events)
    }
}
