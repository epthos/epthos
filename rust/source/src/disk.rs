//! Abstracted disk operations.

use crate::model::{FileHash, FileSize, ModificationTime};
use std::{
    ffi::OsString,
    fs::{self, DirEntry, ReadDir},
    io::{ErrorKind, Read},
    path::{Path, PathBuf},
};
use thiserror::Error;
use tracing::instrument;

/// Errors specific to this module.
#[derive(Error, Debug)]
pub enum DiskError {
    #[error("IO error in {2} for {1:?}: {0}")]
    IO(std::io::Error, PathBuf, String),

    #[error("Unsupported {0}")]
    Unsupported(String),
}

/// Convenience Result type.
pub type Result<T> = std::result::Result<T, DiskError>;

/// High-level disk operations. They map directly to what the rest of the
/// system needs.
pub trait Disk {
    /// Scan a directory and iterate over its contents.
    fn scan(&self, path: &Path) -> Result<ScanIterator>;

    /// Fetch the metadata for a file.
    fn metadata(&self, path: &Path) -> Result<(FileSize, ModificationTime)>;

    /// Snapshot a file (metadata and hash).
    fn snapshot(&self, path: &Path) -> Result<Snapshot>;

    /// Slice a file into a sequence of chunks. The chunks can't be more than
    /// CHUNK_SIZE bytes, but can be much shorter if the file is sparse, or for
    /// the last chunk.
    fn chunk(&self, path: &Path) -> Result<impl Iterator<Item = Result<Chunk>>>;
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub hash: FileHash,
    pub fsize: FileSize,
    pub mtime: ModificationTime,
}

/// One result of scanning a directory.
pub enum ScanEntry {
    File(OsString, FileSize, ModificationTime),
    Directory(OsString),
}

pub struct ScanIterator {
    path: PathBuf,
    inner: ReadDir,
}

impl Iterator for ScanIterator {
    type Item = Result<ScanEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|entry| scan_entry(entry, &self.path))
    }
}

pub struct Chunk {
    data: Vec<u8>,
    offset: usize,
    // TODO: add a ChunkHash
}

fn scan_entry(entry: std::io::Result<DirEntry>, path: &Path) -> Result<ScanEntry> {
    let entry = entry.disk(path, "DirEntry")?;
    // All the following operations are on the specific dir entry.
    let path = &entry.path();
    let file_type = entry.file_type().disk(path, "file_type")?;
    if file_type.is_file() {
        let md = entry.metadata().disk(path, "metadata")?;
        Ok(ScanEntry::File(
            entry.file_name(),
            md.len(),
            md.modified().disk(path, "modified")?,
        ))
    } else if file_type.is_dir() {
        Ok(ScanEntry::Directory(entry.file_name()))
    } else if file_type.is_symlink() {
        // TODO: represent symlinks?
        Err(DiskError::Unsupported("symlink".into()))
    } else {
        tracing::info!("file [{:?}] has unsupported type", &entry);
        Err(DiskError::Unsupported("file type".into()))
    }
}

pub fn new() -> anyhow::Result<RealDisk> {
    Ok(RealDisk {})
}

pub struct RealDisk {}

impl Disk for RealDisk {
    fn scan(&self, path: &Path) -> Result<ScanIterator> {
        let entries = std::fs::read_dir(path).disk(path, "read_dir")?;
        Ok(ScanIterator {
            path: path.to_path_buf(),
            inner: entries,
        })
    }

    fn metadata(&self, path: &Path) -> Result<(FileSize, ModificationTime)> {
        let md = fs::metadata(path).disk(path, "metadata")?;
        Ok((md.len(), md.modified().disk(path, "modified")?))
    }

    fn snapshot(&self, path: &Path) -> Result<Snapshot> {
        hash_file(path)
    }

    fn chunk(&self, _path: &Path) -> Result<impl Iterator<Item = Result<Chunk>>> {
        Ok(ChunkIterator {})
    }
}

struct ChunkIterator {}

impl Iterator for ChunkIterator {
    type Item = Result<Chunk>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

const _CHUNK_SIZE: usize = 2usize.pow(22); // 23 breaks the current gRPC limit.

// TODO: hashing should be sparse-file sensitive so that we spot when the
// structure changes, not only the contents.
#[instrument]
fn hash_file(path: &Path) -> Result<Snapshot> {
    let mut file = std::fs::File::open(path).disk(path, "File::open")?;
    let md = file.metadata().disk(path, "metadata")?;
    let mut hasher = ring::digest::Context::new(&ring::digest::SHA256);
    let mut buffer = vec![0; _CHUNK_SIZE];
    loop {
        match file.read(buffer.as_mut_slice()) {
            Ok(0) => break,
            Ok(bytes) => {
                hasher.update(&buffer[..bytes]);
            }
            Err(err) => {
                if err.kind() == ErrorKind::Interrupted {
                    continue;
                }
                return Err(err).disk(path, "read");
            }
        }
    }
    Ok(Snapshot {
        fsize: md.len(),
        mtime: md.modified().disk(path, "modified")?,
        hash: hasher.finish().into(),
    })
}

// Trait to facilitate error conversions.
trait ToDiskError<T> {
    fn disk(self, path: &Path, op: &str) -> Result<T>;
}

impl<T> ToDiskError<T> for std::io::Result<T> {
    fn disk(self, path: &Path, op: &str) -> Result<T> {
        self.map_err(|err| DiskError::IO(err, path.to_path_buf(), op.to_owned()))
    }
}
