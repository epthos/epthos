use crate::{
    disk::{self, Disk, DiskError, Result, ScanEntry},
    model::{Chunk, FileSize, ModificationTime},
};
use std::path::Path;

pub struct FakeDisk {}

impl FakeDisk {
    pub fn new() -> FakeDisk {
        FakeDisk {}
    }
}

impl Disk for FakeDisk {
    fn scan(&self, _path: &Path) -> Result<impl Iterator<Item = Result<disk::ScanEntry>>> {
        Ok(FakeScanIterator {})
    }

    fn metadata(&self, _path: &Path) -> Result<(FileSize, ModificationTime)> {
        Err(DiskError::Unsupported("not implemented".into()))
    }

    fn chunk(&self, _path: &Path) -> Result<impl Iterator<Item = Result<Chunk>>> {
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

struct FakeScanIterator {}

impl Iterator for FakeScanIterator {
    type Item = Result<ScanEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
