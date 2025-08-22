use std::time::SystemTime;
use thiserror::Error;

/// File size, read from the filesystem.
pub type FileSize = u64;
/// Modification time, read from the filesystem.
pub type ModificationTime = SystemTime;
/// Hash of a file, read from the filesystem.
#[derive(PartialEq, Clone)]
pub struct FileHash([u8; HASH_SIZE]);

#[derive(Error, Debug)]
pub enum FileHashConversionError {
    #[error("Expected 32 bytes, got {0}")]
    UnexpectedSize(usize),
}

#[derive(Debug, PartialEq, Default)]
pub struct Stats {
    pub total_file_count: i32,
}

const HASH_SIZE: usize = 32;

/// Infaillible conversion from a Digest, as we control which
/// algorithm we want.
impl From<ring::digest::Digest> for FileHash {
    fn from(value: ring::digest::Digest) -> Self {
        value.as_ref().try_into().unwrap()
    }
}

/// Faillible conversion from a sequence of bytes, typically read
/// from storage.
impl TryFrom<&[u8]> for FileHash {
    type Error = FileHashConversionError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let n = value.len();
        if n != HASH_SIZE {
            Err(FileHashConversionError::UnexpectedSize(n))
        } else {
            let h: [u8; HASH_SIZE] = value.try_into().unwrap();
            Ok(FileHash(h))
        }
    }
}

impl AsRef<[u8]> for FileHash {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for FileHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hash:")?;
        for byte in self.0.as_ref() {
            write!(f, "{:02x?}", byte)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::bail;

    use super::{FileHash, FileHashConversionError};

    #[test]
    fn ensure_digest_size_match() {
        let d = ring::digest::digest(&ring::digest::SHA256, b"foo");
        let _: FileHash = d.into();
    }

    #[test]
    fn catch_size_mismatch() -> anyhow::Result<()> {
        let h: Result<FileHash, FileHashConversionError> = (&[1u8, 2][..]).try_into();
        if let Ok(_) = h {
            bail!("unexpected success");
        }
        Ok(())
    }
}
