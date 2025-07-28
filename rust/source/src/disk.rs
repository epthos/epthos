//! Abstracted disk operations.

use crate::model::{FileHash, FileSize, ModificationTime};
use anyhow::{Context, bail};
use std::{
    ffi::OsString,
    fs::{self, DirEntry, ReadDir},
    io::{ErrorKind, Read},
    path::Path,
};
use tracing::instrument;

pub trait Disk {
    /// Scan a directory and iterate over its contents.
    fn scan(&self, path: &Path) -> anyhow::Result<ScanIterator>;

    /// Fetch the metadata for a file.
    fn metadata(&self, path: &Path) -> anyhow::Result<(FileSize, ModificationTime)>;

    /// Snapshot a file (metadata and hash).
    fn snapshot(&self, path: &Path) -> std::io::Result<Snapshot>;
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
    inner: ReadDir,
}

impl Iterator for ScanIterator {
    type Item = anyhow::Result<ScanEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(scan_entry)
    }
}

fn scan_entry(entry: std::io::Result<DirEntry>) -> anyhow::Result<ScanEntry> {
    let entry = entry.context("invalid entry")?;
    let file_type = entry.file_type().context("file_type()")?;
    if file_type.is_file() {
        let md = entry.metadata().context("metadata")?;
        Ok(ScanEntry::File(entry.file_name(), md.len(), md.modified()?))
    } else if file_type.is_dir() {
        Ok(ScanEntry::Directory(entry.file_name()))
    } else if file_type.is_symlink() {
        // TODO: represent symlinks?
        bail!("symlinks are not supported yet");
    } else {
        tracing::info!("file [{:?}] has unsupported type", &entry);
        bail!("unsupported entry {:?}", entry);
    }
}

pub fn new() -> anyhow::Result<RealDisk> {
    Ok(RealDisk {})
}

pub struct RealDisk {}

impl Disk for RealDisk {
    fn scan(&self, path: &Path) -> anyhow::Result<ScanIterator> {
        let entries = std::fs::read_dir(path)?;
        Ok(ScanIterator { inner: entries })
    }

    fn metadata(&self, path: &Path) -> anyhow::Result<(FileSize, ModificationTime)> {
        let md = fs::metadata(path).context("stat")?;
        Ok((md.len(), md.modified().context("mtime")?))
    }

    fn snapshot(&self, path: &Path) -> std::io::Result<Snapshot> {
        hash_file(path)
    }
}

const _CHUNK_SIZE: usize = 2usize.pow(22); // 23 breaks the current gRPC limit.

// TODO: hashing should be sparse-file sensitive so that we spot when the
// structure changes, not only the contents.
#[instrument]
fn hash_file(path: &Path) -> Result<Snapshot, std::io::Error> {
    let mut file = std::fs::File::open(path)?;
    let md = file.metadata()?;
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
                return Err(err);
            }
        }
    }
    Ok(Snapshot {
        fsize: md.len(),
        mtime: md.modified()?,
        hash: hasher.finish().into(),
    })
}
