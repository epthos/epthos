//! Canonical layout of the encrypted data on disk.
//!
//! This format is used to pass data around in a compatible way
//! but does not represent how the sink stores it. It has a simple
//! structure:
//!   - root directory for multiple files
//!     - ${file_id directory for each file
//!       - ${version}.dsc descriptor file of a specific version
//!       - ${block_id}.blk block file of a block used by one of the descriptors

use anyhow::Context;
use crypto::model;
use prost::Message;
use std::{
    io::Write,
    path::{Path, PathBuf},
};

/// The root directory for the encrypted data.
pub struct Root {
    dir: PathBuf,
}

/// Specific file, possibly with multiple versions.
pub struct File {
    dir: PathBuf,
}

pub fn read_descriptor(path: &Path) -> anyhow::Result<(model::Descriptor, Root)> {
    let data = std::fs::read(path)?;
    let pb = layout_proto::Descriptor::decode(&data[..])?;
    let dir = path
        .parent()
        .context("No parent")?
        .parent()
        .context("No parent")?;
    let desc = model::Descriptor {
        protected: pb.protected,
        verified: pb.verified,
    };
    Ok((desc, Root::new(dir.to_owned())))
}

impl Root {
    /// Create a new root directory. This does _not_ create the directory on disk.
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    /// Get a handler for a specific file. This does create the directory on disk if it does not
    /// exist.
    pub fn file(&self, file_id: &model::FileId) -> anyhow::Result<File> {
        let dir = self.dir.join(hex::encode(file_id.as_bytes()));
        if !dir.exists() {
            std::fs::create_dir(&dir).context(format!("Failed to create output dir {:?}", &dir))?;
        }
        Ok(File { dir })
    }
}

impl File {
    /// Write a block to disk.
    pub fn write_block(
        &self,
        block: &model::Block,
        block_id: &model::BlockId,
    ) -> anyhow::Result<()> {
        let pb = layout_proto::Block {
            protected: block.protected.as_slice().to_vec(),
            verified: block.verified.as_slice().to_vec(),
        };
        let mut dst = bytes::BytesMut::new();
        pb.encode(&mut dst)?;

        store(&self.block_path(block_id), &dst)
    }

    /// Write a descriptor to disk.
    pub fn write_descriptor(
        &self,
        descriptor: &model::Descriptor,
        version: u32,
    ) -> anyhow::Result<()> {
        let pb = layout_proto::Descriptor {
            protected: descriptor.protected.as_slice().to_vec(),
            verified: descriptor.verified.as_slice().to_vec(),
        };
        let mut dst = bytes::BytesMut::new();
        pb.encode(&mut dst)?;

        store(&self.descriptor_path(version), &dst)
    }

    pub fn read_block(&self, block_id: &model::BlockId) -> anyhow::Result<model::Block> {
        let data = std::fs::read(self.block_path(block_id))?;
        let pb = layout_proto::Block::decode(&data[..])?;
        Ok(model::Block {
            protected: pb.protected.into(),
            verified: pb.verified,
        })
    }

    fn block_path(&self, block: &model::BlockId) -> PathBuf {
        self.dir
            .join(format!("{}.blk", hex::encode(block.as_bytes())))
    }

    fn descriptor_path(&self, version: u32) -> PathBuf {
        self.dir.join(format!("v{}.dsc", version))
    }
}

fn store(path: &Path, data: &[u8]) -> anyhow::Result<()> {
    let mut out = std::fs::File::create(path)?;
    out.write_all(data)?;
    Ok(())
}
