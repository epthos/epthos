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
enum UnsafeOpError {
    #[error("lseek({0}, {1}) failure: {2}")]
    LSeek(usize, c_int, errno::Errno),
    #[cfg(test)]
    #[error("fallocate({0}, {1}) failure: {2}")]
    FAllocate(usize, usize, errno::Errno),
}

impl From<UnsafeOpError> for std::io::Error {
    fn from(value: UnsafeOpError) -> Self {
        std::io::Error::other(value.to_string())
    }
}

fn lseek(file: &mut File, offset: usize, seek_type: c_int) -> Result<usize, UnsafeOpError> {
    let fd = file.as_raw_fd();
    let result = unsafe { libc::lseek(fd, offset as i64, seek_type as i32) };

    if result < 0 {
        let e = errno::errno();
        Err(UnsafeOpError::LSeek(offset, seek_type, e))
    } else {
        Ok(result as usize)
    }
}

#[cfg(test)]
fn fallocate(file: &mut File, mode: c_int, start: usize, size: usize) -> Result<(), UnsafeOpError> {
    use libc::off_t;
    let fd = file.as_raw_fd();
    let result = unsafe { libc::fallocate(fd, mode, start as off_t, size as off_t) };
    if result < 0 {
        let e = errno::errno();
        Err(UnsafeOpError::FAllocate(start, size, e))
    } else {
        Ok(())
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
    use libc::{FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE, SEEK_SET};
    use std::{
        borrow::Cow,
        fs,
        io::{Read, Write},
    };
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

    #[test]
    fn iterate_over_all_sizes_and_shapes() -> anyhow::Result<()> {
        let payload = b"1234567890";
        let only_data = [data(1, payload)];
        let only_hole = [hole(2)];
        let hole_data = [hole(1), data(1, payload)];

        let mut data_block = vec![0_u8; BLOCK_SIZE];
        data_block[..payload.len()].copy_from_slice(&payload[..]);
        let data_hole = [data(1, data_block.as_slice()), hole(1)];

        for scenario in [
            &only_data[..],
            &only_hole[..],
            &hole_data[..],
            &data_hole[..],
        ] {
            // Each scenario is done in its own temp file.
            let parent = tempfile::TempDir::new()?;
            let file = parent.path().join("file");
            write_sections(&file, scenario)?;
            let metadata = fs::metadata(&file)?;
            tracing::info!("{:?} = {}", &file, metadata.len());

            let sections = read_sections(&file)?;
            assert_eq!(scenario, sections.as_slice());
        }

        Ok(())
    }

    // This only matters in the test environment. Will work with
    // smaller actual sizes in the underlying storage too.
    const BLOCK_SIZE: usize = 4096;

    #[derive(Debug, PartialEq)]
    struct Section<'a> {
        tp: SectionType<'a>,
        blocks: u8,
    }
    #[derive(Debug, PartialEq)]
    enum SectionType<'a> {
        Hole,
        Data(Data<'a>),
    }
    #[derive(Debug, PartialEq)]
    struct Data<'a> {
        contents: Cow<'a, [u8]>,
    }

    fn hole(blocks: u8) -> Section<'static> {
        Section {
            tp: SectionType::Hole,
            blocks,
        }
    }
    fn data<'a>(blocks: u8, contents: &'a [u8]) -> Section<'a> {
        Section {
            tp: SectionType::Data(Data {
                contents: Cow::Borrowed(contents),
            }),
            blocks,
        }
    }
    fn owned_data(blocks: u8, contents: Vec<u8>) -> Section<'static> {
        Section {
            tp: SectionType::Data(Data {
                contents: Cow::Owned(contents),
            }),
            blocks,
        }
    }

    fn read_sections(file: &Path) -> anyhow::Result<Vec<Section<'static>>> {
        let mut results = vec![];
        let mut fd = File::open(&file)?;
        let mut next = fd.next_block(Block::default())?;
        loop {
            // next_block moved us to the beginning of a "len"-sized data block.
            if next.skipped > 0 {
                results.push(hole((next.skipped / BLOCK_SIZE) as u8));
            }
            if next.size == 0 {
                break;
            }
            let mut buf = vec![0_u8; next.size];
            fd.read_exact(&mut buf)?;
            let block_count =
                next.size / BLOCK_SIZE + if next.size % BLOCK_SIZE > 0 { 1 } else { 0 };
            results.push(owned_data(block_count as u8, buf));

            next = fd.next_block(next)?;
        }

        Ok(results)
    }
    fn write_sections(file: &Path, sections: &[Section]) -> anyhow::Result<()> {
        let mut fd = File::create(&file)?;
        if sections.is_empty() {
            return Ok(());
        }
        let mut previous = 0;
        let mut offset = 0;

        for section in sections {
            let start = offset;
            let len = (section.blocks as usize) * BLOCK_SIZE;
            previous = offset;
            offset += len;

            match &section.tp {
                SectionType::Hole => {
                    fallocate(
                        &mut fd,
                        FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                        start,
                        len,
                    )?;
                }
                SectionType::Data(Data { contents }) => {
                    lseek(&mut fd, start, SEEK_SET)?;
                    fd.write(contents)?;
                }
            }
        }
        // In order to finish with a hole, we need to resize the file.
        let last = &sections[sections.len() - 1];
        match last.tp {
            SectionType::Hole => {
                fallocate(&mut fd, 0, previous, last.blocks as usize * BLOCK_SIZE)?;
            }
            SectionType::Data(_) => {}
        }

        fd.sync_all()?;
        Ok(())
    }
}
