use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use filetime::FileTime;
use std::{
    fs,
    io::{ErrorKind, Read},
    path::{Path, PathBuf},
};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};

/// Events encountered during an AsyncFileOps::walk().
#[derive(Debug)]
pub enum WalkEvent {
    File(ShallowInfo),
    Error(PathBuf, anyhow::Error),
}

/// Shallow information about a file.
#[derive(Debug, Clone)]
pub struct ShallowInfo {
    file: PathBuf,
    len: u64,
}

impl ShallowInfo {
    /// File path.
    pub fn file(&self) -> &Path {
        self.file.as_path()
    }
    // File length.
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn new(file: PathBuf, len: u64) -> Self {
        ShallowInfo { file, len }
    }
}

/// High-level async API to filesystem operations needed by the rest of the app.
#[derive(Debug, Clone)]
pub struct AsyncFileOps {
    tx_walk: Sender<WalkOp>,
    tx_group: Sender<Ops>,
}

impl AsyncFileOps {
    /// Create a new filesystem runner. Only one new runner should be created per app, but the AsyncFileOps can
    /// be cloned and used in several places.
    pub async fn new() -> Self {
        let (tx_group, rx_group) = mpsc::channel::<Ops>(1);
        let (tx_walk, rx_walk) = mpsc::channel::<WalkOp>(1);
        let mut runner = OpsRunner { rx_walk, rx_group };

        tokio::spawn(async move { runner.run().await });

        AsyncFileOps { tx_walk, tx_group }
    }

    /// Streams all the chunks in `file`. Returns an error if not all chunks were returned.
    /// An empty file will return a single empty chunk. Except in that case, chunks are
    /// guaranteed to be of size `chunk_size` unless they are the last chunk of the file.
    pub async fn read_chunks(
        &self,
        file: &Path,
        chunk_size: usize,
        chunks: Sender<Bytes>,
    ) -> Result<usize> {
        let (tx, rx) = oneshot::channel();
        self.tx_group
            .send(Ops::ReadChunks(file.to_owned(), chunk_size, chunks, tx))
            .await
            .context("failed to send command")?;
        rx.await.context("failed to get result")?
    }

    /// Walks all the files in `roots`. Returns an error if not all the files were streamed.
    pub async fn walk(
        &self,
        roots: Vec<PathBuf>,
        updates: Sender<WalkEvent>,
    ) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx_walk
            .send(WalkOp {
                roots,
                updates,
                done: tx,
            })
            .await
            .context("failed to send command")?;
        rx.await.context("failed to get result")?
    }
}

/// All the high-level operations that can be performed in parallel with each other.
#[derive(Debug)]
enum Ops {
    ReadChunks(
        PathBuf,
        usize,
        Sender<Bytes>,
        oneshot::Sender<Result<usize>>,
    ),
}

/// Specific operation to walk the filesystem. This is separate as it can't be done
/// several times in parallel (because it's not useful).
#[derive(Debug)]
struct WalkOp {
    roots: Vec<PathBuf>,
    updates: Sender<WalkEvent>,
    done: oneshot::Sender<Result<()>>,
}

#[derive(Debug)]
struct OpsRunner {
    rx_walk: Receiver<WalkOp>,
    rx_group: Receiver<Ops>,
}

impl OpsRunner {
    async fn run(&mut self) -> Result<()> {
        let seq_walks = async {
            while let Some(WalkOp {
                roots,
                updates,
                done,
            }) = self.rx_walk.recv().await
            {
                OpsRunner::walk(roots, updates, done).await;
            }
        };
        let seq_ops = async {
            loop {
                tokio::select! {
                    op = self.rx_group.recv() => {
                        match op {
                            None => break,

                            Some(Ops::ReadChunks(path, chunk_size, chunk_tx, result_tx)) => {
                                OpsRunner::read_chunks(path, chunk_size, chunk_tx, result_tx).await;
                            }
                        }
                    },
                }
            }
        };
        tokio::join!(seq_walks, seq_ops);
        tracing::info!("OpsRunner shutting down");
        Ok(())
    }

    async fn walk(
        roots: Vec<PathBuf>,
        updates: Sender<WalkEvent>,
        result_tx: oneshot::Sender<Result<()>>,
    ) {
        let result = match tokio::task::spawn_blocking(move || walk(&roots, updates)).await {
            Ok(v) => v,
            Err(e) => Err(e.into()),
        };
        // Ignore dropped oneshot.
        if result_tx.send(result.context("failed walking")).is_err() {}
    }

    async fn read_chunks(
        path: PathBuf,
        chunk_size: usize,
        chunk_tx: Sender<Bytes>,
        result_tx: oneshot::Sender<Result<usize>>,
    ) {
        let result = match tokio::task::spawn_blocking(move || {
            read_chunks(path.as_path(), chunk_size, chunk_tx)
        })
        .await
        {
            Ok(v) => v,
            Err(e) => Err(e.into()),
        };
        // Ignore dropped oneshot.
        if result_tx.send(result.context("failed chunking")).is_err() {}
    }
}

/// Walks `roots` and informs `update` of what is found.
/// Roots can be individual files or directories.
/// Symlinks and unknown file types are ignored at the moment.
///
/// Returns an error if any of the updates could not be delivered.
#[tracing::instrument(skip(update))]
fn walk(roots: &Vec<PathBuf>, update: Sender<WalkEvent>) -> Result<()> {
    let mut roots = roots.clone();

    while let Some(current) = roots.pop() {
        let md = fs::metadata(&current);
        if let Err(err) = md {
            update.blocking_send(WalkEvent::Error(current, anyhow::Error::new(err)))?;
            continue;
        }
        let md = md.unwrap();
        if md.is_symlink() {
            tracing::debug!("not following symlink {:?}", current);
            continue;
        }
        if md.is_file() {
            update.blocking_send(WalkEvent::File(ShallowInfo::new(current, md.len())))?;
            continue;
        }
        if !md.is_dir() {
            tracing::warn!("skipping unknown type {:?}: {:?}", current, md);
            continue;
        }
        // Now handle the directory content.
        match fs::read_dir(&current) {
            Ok(subdirs) => {
                for entry in subdirs {
                    match entry {
                        Ok(entry) => {
                            roots.push(entry.path());
                        }
                        Err(err) => {
                            update.blocking_send(WalkEvent::Error(
                                current.clone(),
                                anyhow::Error::new(err),
                            ))?;
                        }
                    }
                }
            }
            Err(err) => {
                update.blocking_send(WalkEvent::Error(current, anyhow::Error::new(err)))?;
            }
        }
    }
    Ok(())
}

/// Errors returned by read_chunks.
#[derive(thiserror::Error, Debug)]
pub enum ChunkingError {
    #[error("IO error")]
    IOError(#[from] std::io::Error),
    #[error("Premature closing of channel")]
    ChannelClosed,
}

/// Provide the content of a file in constant-sized chunks. The last chunk is the
/// only chunk that can be smaller than `chunk_size`. Empty files don't produce
/// chunks but return successfully right away.
#[tracing::instrument]
pub fn read_chunks(path: &Path, chunk_size: usize, chunks: Sender<Bytes>) -> Result<usize> {
    let mut file = fs::File::open(path)?;
    let md = file.metadata()?;
    // Preserve access time to avoid messing with other tools.
    let atime = FileTime::from_last_access_time(&md);
    let mut total_size = 0;
    let mut done = false;
    while !done {
        let mut data = BytesMut::new();
        data.resize(chunk_size, 0);

        // We try hard to fill the buffer: random interrupts should not lead to
        // different chunks being generated.
        let mut bytes_in_chunk = 0;
        while bytes_in_chunk < chunk_size {
            match file.read(&mut data[bytes_in_chunk..]) {
                // End of file.
                Ok(0) => {
                    done = true;
                    break;
                }
                // Successful read > 0.
                Ok(bytes_read) => {
                    bytes_in_chunk += bytes_read;
                    total_size += bytes_read;
                }
                // This is a retryable error.
                Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                // All other errors are propagated.
                Err(e) => return Err(e.into()),
            }
        }
        if bytes_in_chunk == 0 {
            break;
        }
        data.truncate(bytes_in_chunk);
        debug_assert!(!data.is_empty());

        if chunks.blocking_send(data.freeze()).is_err() {
            return Err(ChunkingError::ChannelClosed.into());
        }
    }
    drop(file);
    filetime::set_file_atime(path, atime)?;

    // A completely empty file still needs to generate a single empty chunk,
    // to ensure there is a trace of it for recovery.
    if total_size == 0 {
        chunks.blocking_send(Bytes::new())?;
    }
    Ok(total_size)
}

#[cfg(test)]
mod tests {
    use filetime::FileTime;
    use tokio::sync::mpsc::{channel, Receiver};

    use super::*;
    use std::path::Path;

    #[test]
    #[cfg(target_family = "unix")]
    fn handle_symlinks() {
        // TODO: test symlinks on linux by manually setting up the test dir.
    }

    #[tokio::test]
    async fn walk_test_dir() -> anyhow::Result<()> {
        let root = testdir();

        let results = do_walk(vec![root.clone()]).await?;

        let expected: Vec<PathBuf> =
            vec![root.join("a").join("file"), root.join("b").join("file2")];
        assert_eq!(results, expected);
        Ok(())
    }

    #[tokio::test]
    async fn roots_can_be_files() -> anyhow::Result<()> {
        let root = testdir().join("a").join("file");

        let results = do_walk(vec![root.clone()]).await?;

        assert_eq!(results, vec![root]);
        Ok(())
    }

    #[tokio::test]
    async fn read_empty_file() -> anyhow::Result<()> {
        let (tx, mut rx) = channel::<Bytes>(1);

        let handler = get_chunks(&mut rx);

        let res = tokio::spawn(async move {
            let ops = AsyncFileOps::new().await;
            ops.read_chunks(&testdir().join("a").join("file"), 16, tx)
                .await
                .unwrap();
        });

        assert_eq!(handler.await, vec![Bytes::new()]);
        res.await.context("async failed")
    }

    #[tokio::test]
    async fn chunk_file() -> anyhow::Result<()> {
        let (tx, mut rx) = channel::<Bytes>(1);
        let file = testdir().join("b").join("file2");

        let got = get_chunks(&mut rx);

        // We need both the fetcher and the chunk generator to run in parallel.
        let res = tokio::spawn(async move {
            let ops = AsyncFileOps::new().await;
            ops.read_chunks(&file, 2, tx).await.unwrap();
        });

        assert_eq!(
            got.await,
            vec![
                Bytes::from_static(b"ab"),
                Bytes::from_static(b"cd"),
                Bytes::from_static(b"e"),
            ]
        );
        res.await.context("async failed")
    }

    #[tokio::test]
    async fn chunking_leaves_atime_unchanged() -> anyhow::Result<()> {
        let tmpdir = tempfile::tempdir()?;
        let path = tmpdir.path().join("file");

        let original = FileTime::from_unix_time(1000000, 0);

        // Touch a file and set its access time to the value we want.
        std::fs::write(&path, "test data")?;
        filetime::set_file_atime(&path, original)?;

        let (tx, mut rx) = channel::<Bytes>(1);

        {
            let handler = get_chunks(&mut rx);
            let path = path.clone();

            tokio::spawn(async move {
                let ops = AsyncFileOps::new().await;
                ops.read_chunks(&path, 2, tx).await.unwrap();
            });
            handler.await;
        }
        // Ensure the access time is left unchanged.
        let atime = FileTime::from_last_access_time(&fs::metadata(&path)?);
        assert_eq!(atime, original);

        Ok(())
    }

    // Helper function that collects all the chunks until |rx| closes, and
    // returns them.
    async fn get_chunks(rx: &mut Receiver<Bytes>) -> Vec<Bytes> {
        let mut chunks = vec![];
        while let Some(chunk) = rx.recv().await {
            chunks.push(chunk);
        }
        chunks
    }

    async fn do_walk(roots: Vec<PathBuf>) -> Result<Vec<PathBuf>> {
        let (tx, mut rx) = mpsc::channel(1);

        let success = tokio::spawn(async move {
            let ops = AsyncFileOps::new().await;
            ops.walk(roots, tx).await
        });

        let mut results = vec![];
        while let Some(update) = rx.recv().await {
            match update {
                WalkEvent::File(file) => {
                    results.push(file.file().to_owned());
                }
                WalkEvent::Error(_, err) => {
                    // We don't expect errors at this stage.
                    return Err(err);
                }
            }
        }
        success.await??;

        // We don't control the walk, so comparison needs to be made against a fixed order.
        results.sort();
        Ok(results)
    }

    fn testdir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("testdir")
    }
}
