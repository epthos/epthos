//! Base types representing the data model of the system.
//! They are shared by various modules like filesystem and
//! encryption.

use bytes::Bytes;
use std::sync::Arc;

/// FileId is a unique and random identifier for a file in this Source.
/// Both properties must be strongly enforced for the encryption to be
/// secure.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct FileId([u8; FILE_ID_LEN]);
pub const FILE_ID_LEN: usize = 48 / 8;

/// BlockId is a unique and random identifier for a data block.
/// Both properties must be strongly enforced for the encryption to be
/// secure. Uniquess is in the context of a given FileId.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct BlockId([u8; BLOCK_ID_LEN]);
pub const BLOCK_ID_LEN: usize = 96 / 8;

pub type Version = u32;

/// Descriptor is all the information about a file, split between a
/// verified part and a protected part. The verified part is signed
/// and the protected part is encrypted. It is essential that the sink
/// stores the verified part as bytes (even if it can deserialize them)
/// as serialization is _not_ guaranteed to be deterministic.
#[derive(Debug, PartialEq)]
pub struct Descriptor {
    pub verified: Vec<u8>,
    pub protected: Vec<u8>,
}

/// Block from a file. Contains at least one byte, but is expected to be in the
/// MB range.
#[derive(Clone, Debug, PartialEq)]
pub struct Chunk {
    bytes: Bytes,
    digest: Bytes,
}

impl Chunk {
    pub fn new(bytes: Bytes, digest: &[u8]) -> Self {
        Self {
            bytes,
            digest: Bytes::copy_from_slice(digest),
        }
    }

    pub fn bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    pub fn digest(&self) -> Bytes {
        self.digest.clone()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct VerifiedDescriptor {
    // Files (based on their path) have a unique file_id
    // per source, as RAND(48).
    pub file_id: FileId,

    // Each file goes through monotonic versions as they are
    // modified. The version is a concept defined by the
    // source based on how frequently it notices changes.
    // Note that we don't reset the version when the epoch
    // changes.
    pub version: Version,

    // Very large files require mutiple Descriptor blocks.
    // Each block will contain the file_id and version from
    // above. The metadata field is only provided for block at
    // index 0.
    pub index: u16,
    pub total: u16,

    pub chunks: Vec<BlockId>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProtectedDescriptor {
    pub filename: String,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VerifiedBlock {
    pub file_id: FileId,
    pub block_id: BlockId,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProtectedBlock {
    pub chunk: Bytes,
    pub padding: Vec<u8>,
}

#[derive(Debug, PartialEq)]
pub struct Block {
    pub verified: Vec<u8>,
    pub protected: Arc<Vec<u8>>,
}

impl FileId {
    pub fn as_bytes(&self) -> &[u8; FILE_ID_LEN] {
        &self.0
    }

    pub fn as_mut_bytes(&mut self) -> &mut [u8; FILE_ID_LEN] {
        &mut self.0
    }
}

impl TryFrom<&[u8]> for FileId {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != FILE_ID_LEN {
            anyhow::bail!("Invalid length for FileId");
        }
        let mut file_id = [0u8; FILE_ID_LEN];
        file_id.copy_from_slice(value);
        Ok(FileId(file_id))
    }
}

impl BlockId {
    pub fn as_bytes(&self) -> &[u8; BLOCK_ID_LEN] {
        &self.0
    }

    pub fn as_mut_bytes(&mut self) -> &mut [u8; BLOCK_ID_LEN] {
        &mut self.0
    }
}

impl TryFrom<&[u8]> for BlockId {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != BLOCK_ID_LEN {
            anyhow::bail!("Invalid length for BlockId");
        }
        let mut block_id = [0u8; BLOCK_ID_LEN];
        block_id.copy_from_slice(value);
        Ok(BlockId(block_id))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_file_id_from_correct_slice() {
        let bytes: [u8; FILE_ID_LEN] = [0, 1, 2, 3, 4, 5];
        let file_id = FileId::try_from(bytes.as_ref());
        assert!(file_id.is_ok());
        assert!(file_id.unwrap().as_bytes() == &bytes);
    }

    #[test]
    fn test_file_id_from_incorrect_slice() {
        let bytes: [u8; FILE_ID_LEN - 1] = [0, 1, 2, 3, 4];
        let file_id = FileId::try_from(bytes.as_ref());
        assert!(file_id.is_err());
    }

    #[test]
    fn test_block_id_from_correct_slice() {
        let bytes: [u8; BLOCK_ID_LEN] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let block_id = BlockId::try_from(bytes.as_ref());
        assert!(block_id.is_ok());
        assert!(block_id.unwrap().as_bytes() == &bytes);
    }

    #[test]
    fn test_block_id_from_incorrect_slice() {
        let bytes: [u8; FILE_ID_LEN] = [0, 1, 2, 3, 4, 5];
        let block_id = BlockId::try_from(bytes.as_ref());
        assert!(block_id.is_err());
    }
}
