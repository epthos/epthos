use std::path::PathBuf;

mod shared;

// Import the platform-specific module. They are all expected to expose
// the same public APIs. This avoids the need to pepper the code with
// #[cfg(windows)] everywhere.

#[cfg(windows)]
mod win;
#[cfg(windows)]
use win as platform;

#[cfg(unix)]
mod unix;
#[cfg(unix)]
use unix as platform;

// Expose the public API at the top-level.
pub use platform::private_directory;
pub use shared::LocalPathRepr;
pub use shared::PlatformError;

// Platform-specific implementations are expected to provide the main
// implementation.
impl TryFrom<LocalPathRepr> for PathBuf {
    type Error = PlatformError;
    fn try_from(path: LocalPathRepr) -> Result<PathBuf, Self::Error> {
        (&path).try_into()
    }
}

/// Trait that extends reading to sparse objects.
pub trait Sparse {
    // Moves the current position of the file to the next available data block
    // at or after "offset".
    // Returns the number of bytes part of the current data block, and the offset
    // to pass to the next call to "next_block()".
    fn next_block(&mut self, previous: Block) -> std::io::Result<Block>;
}

/// Details obtained when reading a Sparse object.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Block {
    // Number of bytes skipped as they were part of a hole.
    pub skipped: usize,
    // Number of bytes of data from the current position.
    pub size: usize,
    // Offset to use for the next call to next_block()
    pub offset: usize,
}

#[cfg(test)]
mod tests {
    mod sparse;

    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn create_directory() -> anyhow::Result<()> {
        let td = TempDir::new()?;
        let target = td.path().join("target");

        private_directory(&target)?;
        assert!(target.is_dir());
        // The existing directory is a valid target.
        private_directory(&target)?;
        Ok(())
    }

    #[test]
    fn detect_creation_errors() -> anyhow::Result<()> {
        let td = TempDir::new()?;
        let target = td.path().join("target");
        let fd = std::fs::File::create(&target)?;
        drop(fd);

        let err = private_directory(&target);
        match err {
            Ok(_) => {
                panic!("This was meant to fail")
            }
            Err(PlatformError::DirectoryCreationError(path, _)) => {
                assert_eq!(path, target);
            }
            Err(_) => {
                panic!("Should be a creation error, got {:?}", err)
            }
        }
        Ok(())
    }

    #[test]
    fn directory_must_be_secure() -> anyhow::Result<()> {
        let td = TempDir::new()?;

        let err = private_directory(td.path());
        match err {
            Ok(_) => {
                panic!("This was meant to fail")
            }
            Err(PlatformError::DirectoryPermissionError(path, _)) => {
                assert_eq!(path, td.path());
            }
            Err(_) => {
                panic!("Should be a permission error, got {:?}", err)
            }
        }
        Ok(())
    }

    #[test]
    fn roundtrip_local_representation() -> anyhow::Result<()> {
        // Take a real example of a path (here a temporary path),
        // convert it to a local representation and back.
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path();

        let local: LocalPathRepr = path.into();
        let roundtrip: std::path::PathBuf = local.try_into()?;

        assert_eq!(path, roundtrip.as_path());
        Ok(())
    }

    #[test]
    fn iterate_over_all_sizes_and_shapes() -> anyhow::Result<()> {
        let payload = b"1234567890";
        let only_data = [sparse::data(1, payload)];
        let only_hole = [sparse::hole(2)];
        let hole_data = [sparse::hole(1), sparse::data(1, payload)];

        let mut data_block = vec![0_u8; sparse::BLOCK_SIZE];
        data_block[..payload.len()].copy_from_slice(&payload[..]);
        let data_hole = [sparse::data(1, data_block.as_slice()), sparse::hole(1)];

        for scenario in [
            &only_data[..],
            &only_hole[..],
            &data_hole[..],
            &hole_data[..],
        ] {
            // Each scenario is done in its own temp file.
            let parent = tempfile::TempDir::new()?;
            let file = parent.path().join("file");
            sparse::write_sections(&file, scenario)?;
            let metadata = fs::metadata(&file)?;
            tracing::info!("{:?} = {} for {:?}", &file, metadata.len(), &scenario);

            let sections = sparse::read_sections(&file)?;
            assert_eq!(scenario, sections.as_slice());
        }

        Ok(())
    }
}
