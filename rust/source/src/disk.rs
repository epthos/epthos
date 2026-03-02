//! Abstracted disk operations.
use crate::model::{Chunk, FileHash, FileHashBuilder, FileSize, ModificationTime};
use std::{
    ffi::OsString,
    io::{ErrorKind, Read},
    path::{Path, PathBuf},
};
use thiserror::Error;

mod real;

/// High-level disk operations. They map directly to what the rest of the
/// system needs.
pub trait Disk {
    /// Scan a directory and iterate over its contents.
    fn scan(&self, path: &Path) -> Result<impl Iterator<Item = Result<ScanEntry>>>;

    /// Fetch the metadata for a file.
    fn metadata(&self, path: &Path) -> Result<(FileSize, ModificationTime)>;

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
#[derive(Debug, PartialEq)]
pub enum ScanEntry {
    File(OsString, FileSize, ModificationTime),
    Directory(OsString),
}

/// Snapshot a file, incl metadata and contents, using the Disk abstractions.
pub fn snapshot<D: Disk>(disk: &D, path: &Path) -> Result<Snapshot> {
    let (fsize, mtime) = disk.metadata(path)?;

    let mut file_hash = FileHashBuilder::new();
    for chunk in disk.chunk(path)? {
        file_hash.update(&chunk?);
    }

    Ok(Snapshot {
        hash: file_hash.finish(),
        fsize,
        mtime,
    })
}

/// Create a new instance of the production Disk trait.
pub fn new() -> anyhow::Result<impl Disk> {
    Ok(real::RealDisk {})
}

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
