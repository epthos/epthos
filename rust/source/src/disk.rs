//! Abstracted disk operations.
use crate::model::{Chunk, FileHash, FileHashBuilder, FileSize, ModificationTime};
use std::{
    ffi::OsString,
    io::{ErrorKind, Read},
    path::{Path, PathBuf},
};
use thiserror::Error;

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

// Implementation of Disk on top of the actual filesystem.
mod real {
    use super::*;
    use crate::model::Chunk;
    use platform::{Block, Sparse};
    // This is the only place we expect std::fs to be used.
    use std::fs::{self, DirEntry, File, ReadDir};

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
        position: usize,
        done: bool,
        chunk_size: usize,
    }

    impl<F: Read> ChunkIterator<F> {
        fn new(path: PathBuf, file: F, chunk_size: usize) -> Self {
            ChunkIterator {
                path,
                file,
                position: 0,
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
            let mut hasher = ring::digest::Context::new(&ring::digest::SHA256);
            let mut buffer = vec![0; self.chunk_size];
            let mut start = 0;
            // Detect whether there is a discontinuity in the file ahead:
            //   - we don't want to read the hole's zeros
            //   - we want to resume right after it.
            let _next = match self
                .file
                .next_block(Block::default())
                .disk(&self.path, "next_block")
            {
                Ok(pair) => pair,
                Err(err) => return Some(Err(err)),
            };

            while start < self.chunk_size - 1 {
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
            Some(Ok(Chunk {
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

    #[cfg(test)]
    mod test {
        use std::cmp::min;

        use super::*;
        use ring::digest::SHA256;
        use test_log::test;
        use tracing::info;

        #[test]
        fn chunk_file_on_boundary() -> Result<()> {
            let mut input = [0u8; 100];
            for idx in 0..input.len() {
                input[idx] = idx as u8;
            }
            let reader = ReadInjector::new(&input[..], |_| Ok(200));
            let mut it = ChunkIterator::new(PathBuf::from("fake"), reader, 50);
            let c1 = it.next().ok_or(DiskError::Unsupported("oops".into()))??;
            assert_eq!(
                c1,
                Chunk {
                    data: input[..50].to_vec(),
                    offset: 0,
                    hash: ring::digest::digest(&SHA256, &input[0..50]).into(),
                }
            );
            let c2 = it.next().ok_or(DiskError::Unsupported("oops".into()))??;
            assert_eq!(
                c2,
                Chunk {
                    data: input[50..].to_vec(),
                    offset: 50,
                    hash: ring::digest::digest(&SHA256, &input[50..]).into(),
                }
            );
            assert!(it.next().is_none());

            Ok(())
        }

        #[test]
        fn chunk_file_with_different_size() -> Result<()> {
            let mut input = [0u8; 100];
            for idx in 0..input.len() {
                input[idx] = idx as u8;
            }
            let reader = ReadInjector::new(&input[..], |_| Ok(200));
            let mut it = ChunkIterator::new(PathBuf::from("fake"), reader, 60);
            let c1 = it.next().ok_or(DiskError::Unsupported("oops".into()))??;
            assert_eq!(
                c1,
                Chunk {
                    data: input[..60].to_vec(),
                    offset: 0,
                    hash: ring::digest::digest(&SHA256, &input[0..60]).into(),
                }
            );
            let c2 = it.next().ok_or(DiskError::Unsupported("oops".into()))??;
            assert_eq!(
                c2,
                Chunk {
                    data: input[60..].to_vec(),
                    offset: 60,
                    hash: ring::digest::digest(&SHA256, &input[60..]).into(),
                }
            );
            assert!(it.next().is_none());

            Ok(())
        }

        #[test]
        fn chunk_file_handling_interrupt() -> Result<()> {
            let mut input = [0u8; 10];
            for idx in 0..input.len() {
                input[idx] = idx as u8;
            }
            let reader = ReadInjector::new(&input[..], |i: u8| match i {
                0 => Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "again",
                )),
                _ => Ok(10),
            });
            let mut it = ChunkIterator::new(PathBuf::from("fake"), reader, 60);
            let c1 = it.next().ok_or(DiskError::Unsupported("oops".into()))??;
            assert_eq!(
                c1,
                Chunk {
                    data: input[..].to_vec(),
                    offset: 0,
                    hash: ring::digest::digest(&SHA256, &input[..]).into(),
                }
            );
            assert!(it.next().is_none());

            Ok(())
        }

        #[test]
        fn chunk_file_handling_small_read() -> Result<()> {
            let mut input = [0u8; 50];
            for idx in 0..input.len() {
                input[idx] = idx as u8;
            }
            let reader = ReadInjector::new(&input[..], |i: u8| match i {
                0 => Ok(10),
                _ => Ok(100),
            });
            let mut it = ChunkIterator::new(PathBuf::from("fake"), reader, 60);
            let c1 = it.next().ok_or(DiskError::Unsupported("oops".into()))??;
            assert_eq!(
                c1,
                Chunk {
                    data: input[..].to_vec(),
                    offset: 0,
                    hash: ring::digest::digest(&SHA256, &input[..]).into(),
                }
            );
            assert!(it.next().is_none());

            Ok(())
        }

        #[test]
        fn chunk_file_handling_fatal_error() -> Result<()> {
            let mut input = [0u8; 50];
            for idx in 0..input.len() {
                input[idx] = idx as u8;
            }
            let reader = ReadInjector::new(&input[..], |i: u8| match i {
                0 => Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "nope",
                )),
                _ => Ok(100),
            });
            let mut it = ChunkIterator::new(PathBuf::from("fake"), reader, 60);
            let c1 = it.next().ok_or(DiskError::Unsupported("oops".into()))?;
            assert!(c1.is_err());
            assert!(it.next().is_none());

            Ok(())
        }

        // Helper Read impl that allows a user-provided function to decide when
        // to inject errors, small reads, etc.
        struct ReadInjector<'a, F: Fn(u8) -> std::io::Result<usize>> {
            data: &'a [u8],
            // Offset in the data.
            offset: usize,
            // Index of the current read() call.
            iteration: u8,
            // Function that decides if a read is successful or not based on the
            // operation's index.
            op: F,
        }

        impl<'a, F> ReadInjector<'a, F>
        where
            F: Fn(u8) -> std::io::Result<usize>,
        {
            fn new(data: &'a [u8], op: F) -> Self {
                ReadInjector {
                    data,
                    offset: 0,
                    iteration: 0,
                    op,
                }
            }
        }
        impl<'a, F> std::io::Read for ReadInjector<'a, F>
        where
            F: Fn(u8) -> std::io::Result<usize>,
        {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                let next = (self.op)(self.iteration);
                self.iteration += 1; // even when returning errors.
                let next = next?;
                let avail = &self.data[self.offset..];
                let start = self.offset;
                let len = min(min(next, buf.len()), avail.len());
                self.offset += len;
                info!("serving start={} len={}", start, len);
                (&mut buf[..len]).copy_from_slice(&self.data[start..(start + len)]);
                Ok(len)
            }
        }

        impl<'a, F> Sparse for ReadInjector<'a, F>
        where
            F: Fn(u8) -> std::io::Result<usize>,
        {
            fn next_block(&mut self, _previous: Block) -> std::io::Result<Block> {
                Ok(Block::default())
            }
        }
    }
}
