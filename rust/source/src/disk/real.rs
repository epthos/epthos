//! Implementation of Disk on top of the actual filesystem.
use super::*;
use crate::model::Chunk;
use platform::{Block, Sparse};
// This is the only place we expect std::fs to be used.
use std::{
    cmp::min,
    fs::{self, DirEntry, File, ReadDir},
};

#[cfg(test)]
mod test;

pub struct RealDisk {}

impl Disk for RealDisk {
    fn scan(&self, path: &Path) -> Result<impl Iterator<Item = Result<ScanEntry>>> {
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

    fn chunk(&self, path: &Path) -> Result<impl Iterator<Item = Result<Chunk>>> {
        let file = File::open(path).disk(path, "File::open")?;
        Ok(ChunkIterator::new(path.to_path_buf(), file, CHUNK_SIZE))
    }
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

struct ChunkIterator<F: Read> {
    path: PathBuf,
    file: F,
    // Absolute position in the file.
    position: usize,
    // Available bytes to read.
    available: usize,
    // _Next_ data block to read.
    block: Block,
    done: bool,
    chunk_size: usize,
}

impl<F: Read> ChunkIterator<F> {
    fn new(path: PathBuf, file: F, chunk_size: usize) -> Self {
        ChunkIterator {
            path,
            file,
            position: 0,
            available: 0,
            block: Block::default(),
            done: false,
            chunk_size,
        }
    }
}

impl<F> Iterator for ChunkIterator<F>
where
    F: Read + Sparse,
{
    type Item = Result<Chunk>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        // Do we have bytes to consume in the current block?
        if self.available == 0 {
            self.block = match self
                .file
                .next_block(std::mem::take(&mut self.block))
                .disk(&self.path, "next_block")
            {
                Ok(pair) => pair,
                Err(err) => return Some(Err(err)),
            };

            self.done = self.block.size == 0;
            self.available = self.block.size;

            if self.block.skipped > 0 {
                let hole_start = self.position;
                self.position += self.block.skipped;
                // Return the hole right away, we'll process the data in the
                // next round.
                return Some(Ok(Chunk::Hole {
                    offset: hole_start,
                    size: self.block.skipped,
                }));
            }

            if self.available == 0 {
                return None;
            }
        }
        // If we reach here, we have bytes left to read.
        let mut hasher = ring::digest::Context::new(&ring::digest::SHA256);
        let mut start = 0;
        let available_bytes = min(self.chunk_size, self.block.size);
        let mut buffer = vec![0; available_bytes];

        while start < available_bytes - 1 {
            match self.file.read(&mut buffer[start..]) {
                Ok(0) => {
                    self.done = true;
                    break;
                }
                Ok(bytes) => {
                    hasher.update(&buffer[start..(start + bytes)]);
                    start += bytes;
                }
                Err(err) => {
                    if err.kind() == ErrorKind::Interrupted {
                        continue;
                    }
                    self.done = true;
                    return Some(Err(err).disk(&self.path, "read"));
                }
            }
        }
        // Handle the case where we fall right on a boundary.
        if start == 0 {
            return None;
        }
        let offset = self.position;
        // Trim the buffer based on how much we actually read.
        buffer.resize(start, 0);
        self.position += buffer.len();
        self.available -= buffer.len();
        Some(Ok(Chunk::Data {
            data: buffer,
            offset,
            hash: hasher.finish().into(),
        }))
    }
}

const CHUNK_SIZE: usize = 2usize.pow(22); // 23 breaks the current gRPC limit.

// Trait to facilitate error conversions.
trait ToDiskError<T> {
    fn disk(self, path: &Path, op: &str) -> Result<T>;
}

impl<T> ToDiskError<T> for std::io::Result<T> {
    fn disk(self, path: &Path, op: &str) -> Result<T> {
        self.map_err(|err| DiskError::IO(err, path.to_path_buf(), op.to_owned()))
    }
}
