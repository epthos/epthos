//! Exceptions and other common elements.

#[derive(Debug, thiserror::Error)]
pub enum PlatformError {
    #[error("The directory {0} could not be created: {1}")]
    DirectoryCreationError(std::path::PathBuf, anyhow::Error),
    #[error("The directory {0} has unexpected permission: {1}")]
    DirectoryPermissionError(std::path::PathBuf, anyhow::Error),
    #[error("The path could not be converted: {0}")]
    PathConversionError(anyhow::Error),
    #[error("Unexpected: {0:?}")]
    UnexpectedError(anyhow::Error),
}

impl From<anyhow::Error> for PlatformError {
    fn from(err: anyhow::Error) -> PlatformError {
        PlatformError::UnexpectedError(err)
    }
}

/// A binary representation of a path, for the current platform.
/// This is the format that should be stored. It is not meant for
/// restoring paths across platforms.
#[derive(Debug, PartialEq, Clone, Eq, Hash)]
pub struct LocalPathRepr(Vec<u8>);

impl LocalPathRepr {
    pub fn new(path: Vec<u8>) -> Self {
        LocalPathRepr(path)
    }
}

impl AsRef<[u8]> for LocalPathRepr {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
