use crate::shared::{LocalPathRepr, PlatformError};
use crate::{Block, Sparse};
use anyhow::{Context, anyhow};
use libc::c_int;
use std::ffi::OsStr;
use std::fs::{DirBuilder, File};
use std::os::fd::AsRawFd;
use std::os::unix::{
    ffi::OsStrExt,
    fs::{DirBuilderExt, PermissionsExt},
};
use std::path::{Path, PathBuf};
use thiserror::Error;

impl Sparse for File {
    fn next_block(&mut self, previous: Block) -> std::io::Result<Block> {
        let hole_start = match lseek(self, previous.offset, libc::SEEK_HOLE) {
            Ok(offset) => offset,
            // When we reach past the end of the file, SEEK_HOLE fails with ENXIO.
            // However, we want to return a clean value for that case, for the caller
            // to handle.
            Err(UnsafeOpError::LSeek(_, _, errno::Errno(libc::ENXIO))) => {
                tracing::debug!("No hole at {}", previous.offset);
                return Ok(Block {
                    skipped: 0,
                    size: 0,
                    offset: previous.offset,
                });
            }
            // Other errors are not expected.
            Err(e) => return Err(e.into()),
        };
        let data_start = match lseek(self, previous.offset, libc::SEEK_DATA) {
            Ok(offset) => offset,
            Err(UnsafeOpError::LSeek(_, _, errno::Errno(libc::ENXIO))) => {
                // All files have an implicit hole at the end. We still want to determine
                // if we skipped data, to be able to recreate the file as it was.
                let end_offset = lseek(self, 0, libc::SEEK_END)?;
                tracing::debug!("No data at {}, end at {}", previous.offset, end_offset);
                return Ok(Block {
                    skipped: end_offset - previous.offset,
                    size: 0,
                    offset: previous.offset,
                });
            }
            Err(e) => return Err(e.into()),
        };
        // offset is in a data block, return its size.
        if hole_start >= data_start {
            return Ok(Block {
                skipped: data_start - previous.offset,
                size: hole_start - data_start,
                offset: hole_start,
            });
        }
        // offset is in a hole. We know when the data starts, now we need to find
        // the hole _after_ that one, and move back to the data block.
        let hole_start = match lseek(self, data_start, libc::SEEK_HOLE) {
            Ok(offset) => offset,
            Err(UnsafeOpError::LSeek(_, _, errno::Errno(libc::ENXIO))) => {
                return Ok(Block {
                    skipped: 0,
                    size: 0,
                    offset: previous.offset,
                });
            }
            Err(e) => return Err(e.into()),
        };
        lseek(self, data_start, libc::SEEK_SET)?;

        Ok(Block {
            skipped: data_start - previous.offset,
            size: hole_start - data_start,
            offset: hole_start,
        })
    }
}

// Internal error type to ensure we can pass all the useful information around.
#[derive(Debug, Error)]
pub(crate) enum UnsafeOpError {
    #[error("lseek({0}, {1}) failure: {2}")]
    LSeek(usize, c_int, errno::Errno),
}

impl From<UnsafeOpError> for std::io::Error {
    fn from(value: UnsafeOpError) -> Self {
        std::io::Error::other(value)
    }
}

pub(crate) fn lseek(
    file: &mut File,
    offset: usize,
    seek_type: c_int,
) -> Result<usize, UnsafeOpError> {
    let fd = file.as_raw_fd();
    let result = unsafe { libc::lseek(fd, offset as i64, seek_type) };

    if result < 0 {
        let e = errno::errno();
        Err(UnsafeOpError::LSeek(offset, seek_type, e))
    } else {
        Ok(result as usize)
    }
}

pub fn private_directory(root: &Path) -> Result<(), PlatformError> {
    let result = DirBuilder::new().mode(0o700).create(root);
    match result {
        Ok(_) => {}
        Err(err) => {
            if err.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(PlatformError::DirectoryCreationError(
                    root.to_path_buf(),
                    anyhow!("DirBuilder failed with {}", err),
                ));
            }
            if !root.is_dir() {
                return Err(PlatformError::DirectoryCreationError(
                    root.to_path_buf(),
                    anyhow!("target exists but is not a directory"),
                ));
            }
            let permissions = root
                .metadata()
                .context("failed to get metadata")
                .map_err(|err| PlatformError::DirectoryPermissionError(root.to_path_buf(), err))?
                .permissions();
            let mode = permissions.mode() & 0o777; // Only check the permissions, not the type.
            if mode != 0o700 {
                return Err(PlatformError::DirectoryPermissionError(
                    root.to_path_buf(),
                    anyhow!("unexpected mode {:o}", mode),
                ));
            }
        }
    }
    Ok(())
}

impl<P: AsRef<Path>> From<P> for LocalPathRepr {
    fn from(path: P) -> LocalPathRepr {
        LocalPathRepr::new(path.as_ref().as_os_str().as_bytes().to_vec())
    }
}

impl TryFrom<&LocalPathRepr> for PathBuf {
    type Error = PlatformError;
    fn try_from(path: &LocalPathRepr) -> Result<PathBuf, Self::Error> {
        Ok(PathBuf::from(OsStr::from_bytes(path.as_ref())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn create_config_dir_with_permissions() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let parent = tempfile::TempDir::new()?;
        let path = parent.path().join("config");
        private_directory(&path)?;

        // Directory permission is expected to be rwx------ aka 700.
        let metadata = path.metadata()?;
        let permissions = metadata.permissions();

        // mode is the full permission, which includes bits for the inode
        // type, etc.
        assert_eq!(permissions.mode() & 0o777, 0o700);

        // We can then call the function again just fine.
        private_directory(&path)?;

        Ok(())
    }

    #[test]
    fn reject_invalid_permissions() -> anyhow::Result<()> {
        use std::fs::DirBuilder;
        use std::os::unix::fs::DirBuilderExt;

        let parent = tempfile::TempDir::new()?;
        let path = parent.path().join("config");
        DirBuilder::new().mode(0o770).create(&path)?;

        assert!(matches!(
            private_directory(&path),
            Err(PlatformError::DirectoryPermissionError(_, _))
        ));
        Ok(())
    }
}
