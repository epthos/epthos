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

#[cfg(test)]
mod tests {
    use super::*;
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
}
