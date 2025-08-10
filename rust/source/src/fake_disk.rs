use crate::{
    disk::{self, Disk},
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
    fn scan(&self, _path: &Path) -> anyhow::Result<disk::ScanIterator> {
        todo!()
    }

    fn metadata(&self, _path: &Path) -> anyhow::Result<(FileSize, ModificationTime)> {
        todo!()
    }

    fn snapshot(&self, _path: &Path) -> std::io::Result<disk::Snapshot> {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "didn't even try",
        ))
    }
}
