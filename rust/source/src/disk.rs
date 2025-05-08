//! Abstracted disk operations.

use crate::{
    filestore,
    model::{FileSize, ModificationTime},
};
use anyhow::Context;
use std::{
    fs,
    io::{ErrorKind, Read},
    path::Path,
};
use tracing::instrument;

pub trait Disk {
    fn metadata(&self, path: &Path) -> anyhow::Result<(FileSize, ModificationTime)> {
        let md = fs::metadata(path).context("stat")?;
        Ok((md.len(), md.modified().context("mtime")?))
    }

    fn snapshot(&self, path: &Path) -> std::io::Result<filestore::Snapshot> {
        hash_file(path)
    }
}

pub fn new() -> anyhow::Result<RealDisk> {
    Ok(RealDisk {})
}

pub struct RealDisk {}

impl Disk for RealDisk {}

const _CHUNK_SIZE: usize = 2usize.pow(22); // 23 breaks the current gRPC limit.

// TODO: hashing should be sparse-file sensitive so that we spot when the
// structure changes, not only the contents.
#[instrument]
fn hash_file(path: &Path) -> Result<filestore::Snapshot, std::io::Error> {
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
    Ok(filestore::Snapshot {
        fsize: md.len(),
        mtime: md.modified()?,
        hash: hasher.finish(),
    })
}
