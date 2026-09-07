use crate::{
    disk::{Disk, Result, ScanEntry},
    model::{Chunk, FileMetadata, ModificationTime},
};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};

#[derive(Clone)]
pub struct FakeDisk {
    handles: Arc<Mutex<Vec<ChunkHandle>>>,
}

impl FakeDisk {
    pub fn new() -> FakeDisk {
        FakeDisk {
            handles: Arc::new(Mutex::new(vec![])),
        }
    }

    /// Wait up to `timeout` for a chunk handle for `path` to be registered,
    /// then remove and return it.
    pub async fn get_chunk_handle(
        &self,
        path: &Path,
        timeout: Duration,
    ) -> anyhow::Result<ChunkHandle> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let mut handles = self.handles.lock().unwrap();
            if let Some(idx) = handles.iter().position(|h| h.path == path) {
                return Ok(handles.remove(idx));
            }
            drop(handles);
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("timeout waiting for chunk handle for {:?}", path);
            }
            tokio::task::yield_now().await;
        }
    }
}

/// Test handle for a single active chunk iterator. Feed `Some(item)` to
/// produce the next chunk, or `None` to end iteration.
pub struct ChunkHandle {
    pub path: PathBuf,
    tx: mpsc::Sender<Option<Result<Chunk>>>,
}

impl ChunkHandle {
    pub fn send(&self, item: Option<Result<Chunk>>) {
        let _ = self.tx.send(item);
    }
}

impl Disk for FakeDisk {
    fn scan(&self, _path: &Path) -> Result<impl Iterator<Item = Result<ScanEntry>>> {
        Ok(FakeScanIterator {})
    }

    fn metadata(&self, _path: &Path) -> Result<FileMetadata> {
        Ok(FileMetadata {
            fsize: 0,
            mtime: ModificationTime::UNIX_EPOCH,
        })
    }

    fn chunk(&self, path: &Path, _offset: usize) -> Result<impl Iterator<Item = Result<Chunk>>> {
        let (tx, rx) = mpsc::channel();
        self.handles.lock().unwrap().push(ChunkHandle {
            path: path.to_path_buf(),
            tx,
        });
        Ok(FakeChunkIterator { rx })
    }
}

struct FakeChunkIterator {
    rx: mpsc::Receiver<Option<Result<Chunk>>>,
}

impl Iterator for FakeChunkIterator {
    type Item = Result<Chunk>;

    fn next(&mut self) -> Option<Self::Item> {
        // Blocks until the test sends a value; None signals end of iteration.
        self.rx.recv().ok().flatten()
    }
}

struct FakeScanIterator {}

impl Iterator for FakeScanIterator {
    type Item = Result<ScanEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
