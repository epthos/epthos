use crate::{
    disk::{self, Chunk, Disk, DiskError, Result},
    model::{FileSize, ModificationTime},
};
use std::path::Path;

pub struct FakeDisk {}

impl FakeDisk {
    pub fn new() -> FakeDisk {
        FakeDisk {}
    }
}

impl Disk for FakeDisk {
    fn scan(&self, _path: &Path) -> Result<disk::ScanIterator> {
        todo!()
    }

    fn metadata(&self, _path: &Path) -> Result<(FileSize, ModificationTime)> {
        todo!()
    }

    fn snapshot(&self, _path: &Path) -> Result<disk::Snapshot> {
        Err(DiskError::Unsupported("snapshot".into()))
    }

    fn chunk(&self, _path: &Path) -> Result<impl Iterator<Item = Result<disk::Chunk>>> {
        Ok(FakeChunkIterator {})
    }
}

struct FakeChunkIterator {}

impl Iterator for FakeChunkIterator {
    type Item = Result<Chunk>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
