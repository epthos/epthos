//! Abstracted disk operations.
use crate::model::{Chunk, FileHash, FileHashBuilder, FileMetadata};
use std::{
    ffi::OsString,
    io::{ErrorKind, Read},
    path::{Path, PathBuf},
};
use thiserror::Error;

mod localfs;

/// High-level disk operations. They map directly to what the rest of the
/// system needs.
pub trait Disk {
    /// Scan a directory and iterate over its contents.
    fn scan(&self, path: &Path) -> Result<impl Iterator<Item = Result<ScanEntry>>>;

    /// Fetch the metadata for a file.
    fn metadata(&self, path: &Path) -> Result<FileMetadata>;

    /// Slice a file into a sequence of chunks, starting at |offset|.
    /// The chunks can't be more than CHUNK_SIZE bytes, but can be much shorter if
    /// the file is sparse, or for the last chunk.
    fn chunk(&self, path: &Path, offset: usize) -> Result<impl Iterator<Item = Result<Chunk>>>;
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub hash: FileHash,
    pub md: FileMetadata,
}

/// One result of scanning a directory.
#[derive(Debug, PartialEq)]
pub enum ScanEntry {
    File(OsString, FileMetadata),
    Directory(OsString),
}

/// Snapshot a file, incl metadata and contents, using the Disk abstractions.
pub fn snapshot<D: Disk>(disk: &D, path: &Path) -> Result<Snapshot> {
    let md = disk.metadata(path)?;

    let mut file_hash = FileHashBuilder::new();
    for chunk in disk.chunk(path, 0)? {
        file_hash.update(&chunk?);
    }

    Ok(Snapshot {
        hash: file_hash.finish(),
        md,
    })
}

/// Create a new instance of the production Disk trait.
pub fn new() -> anyhow::Result<impl Disk + Clone + Send> {
    Ok(localfs::Disk {})
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
