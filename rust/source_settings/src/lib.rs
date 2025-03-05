use anyhow::Context;
use settings::{connection, process};
use std::path::{Path, PathBuf};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use thiserror::Error;

/// Loads and validates the settings.
pub fn load() -> anyhow::Result<Settings> {
    load_impl(&settings::get_anchor(None)?)
}

/// All the validated settings.
pub struct Settings {
    broker: broker_client::Settings,
    connection: connection::Settings,
    process: process::Settings,
    backup: Backup,
}

/// Backup-related settings.
pub struct Backup {
    root: Vec<PathBuf>,
    keyfile: PathBuf,
    db: PathBuf,
}

impl Settings {
    pub fn broker(&self) -> &broker_client::Settings {
        &self.broker
    }

    pub fn connection(&self) -> &connection::Settings {
        &self.connection
    }

    pub fn process(&self) -> &process::Settings {
        &self.process
    }

    pub fn backup(&self) -> &Backup {
        &self.backup
    }
}

impl Backup {
    /// Returns the root paths that must be backed up.
    pub fn roots(&self) -> &Vec<PathBuf> {
        &self.root
    }

    pub fn keyfile(&self) -> &Path {
        &self.keyfile
    }

    pub fn db(&self) -> &Path {
        &self.db
    }
}

/// All the customizable options for creating a fresh config.
#[derive(Debug, Default)]
pub struct Builder {
    certificate: String,
    private_key: String,
    root: Vec<PathBuf>,

    set: std::collections::HashSet<Fields>,
}

impl Builder {
    pub fn certificate<T: Into<String>>(mut self, certificate: T) -> Builder {
        self.certificate = certificate.into();
        self.set.insert(Fields::Certificate);
        self
    }

    pub fn private_key<T: Into<String>>(mut self, private_key: T) -> Builder {
        self.private_key = private_key.into();
        self.set.insert(Fields::PrivateKey);
        self
    }

    pub fn root(mut self, root: &[PathBuf]) -> anyhow::Result<Builder> {
        // During initial creation, we canonicalize roots to avoid ambiguities.
        // Note that this is not enforced when reading the config, which leaves
        // an escape hatch for weird cases.
        let mut canonical = vec![];
        for path in root {
            canonical.push(
                std::fs::canonicalize(path)
                    .context(format!("failed to canonicalize {0:?}", path))?,
            );
        }
        self.root = canonical;
        self.set.insert(Fields::Root);
        Ok(self)
    }

    pub fn path(anchor: &settings::Anchor) -> PathBuf {
        settings::path(NAME, anchor)
    }

    pub fn save(self, anchor: &settings::Anchor) -> anyhow::Result<()> {
        if !Fields::iter().all(|variant| self.set.contains(&variant)) {
            return Err(ConfigError::MissingField).context("Can't write Source settings");
        }
        let settings = wire::Settings {
            broker: broker_client::wire::Settings {
                address: constants::BROKER_ADDRESS.into(),
                name: constants::BROKER_NAME.into(),
            },
            connection: connection::wire::Settings {
                peer_root: constants::CA_ROOT.into(),
                certificate: self.certificate,
                private_key: self.private_key,
            },
            process: process::wire::Settings {
                tracing_level: process::wire::TracingLevel::INFO,
            },
            backup: wire::Backup {
                root: self.root,
                db: "db".into(),
                keyfile: "keyfile".into(),
            },
        };
        settings::save(&settings, NAME, anchor)?;
        Ok(())
    }
}

pub fn load_impl(anchor: &settings::Anchor) -> anyhow::Result<Settings> {
    settings::load::<Settings>(NAME, anchor)
}

// Canonical name of the config file.
const NAME: &str = "source";

#[derive(Debug, PartialEq, Eq, Hash, EnumIter)]
enum Fields {
    Certificate,
    PrivateKey,
    Root,
}

#[derive(Error, Debug)]
enum ConfigError {
    #[error("Some fields have not been set")]
    MissingField,
}

mod wire {
    use super::*;
    use serde::{Deserialize, Serialize};

    /// Represents the overall configuration data for a Source.
    #[derive(Debug, Deserialize, Serialize)]
    pub struct Settings {
        pub broker: broker_client::wire::Settings,
        pub connection: connection::wire::Settings,
        pub process: process::wire::Settings,
        pub backup: Backup,
    }

    /// Settings related to data backup.
    #[derive(Debug, Deserialize, Serialize)]
    pub struct Backup {
        pub root: Vec<PathBuf>,
        pub keyfile: settings::ConfigPath,
        pub db: settings::ConfigPath,
    }
}

impl settings::Anchored for Settings {
    type Wire = wire::Settings;

    fn anchor(wire: &Self::Wire, anchor: &settings::Anchor) -> anyhow::Result<Settings> {
        Ok(Settings {
            broker: broker_client::Settings::anchor(&wire.broker, anchor)?,
            connection: connection::Settings::anchor(&wire.connection, anchor)?,
            process: process::Settings::anchor(&wire.process, anchor)?,
            backup: Backup::anchor(&wire.backup, anchor)?,
        })
    }
}

impl settings::Anchored for Backup {
    type Wire = wire::Backup;

    fn anchor(wire: &Self::Wire, anchor: &settings::Anchor) -> anyhow::Result<Self> {
        Ok(Backup {
            root: wire.root.clone(),
            keyfile: wire.keyfile.path(anchor),
            db: wire.db.path(anchor),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn check_all_fields() -> anyhow::Result<()> {
        let tmpdir = TempDir::new()?;
        let cfg = tmpdir.path().join("cfg");
        match Builder::default().save(&settings::get_anchor(Some(cfg))?) {
            Err(_) => {}
            Ok(_) => panic!("unexpectedly passed"),
        }
        Ok(())
    }

    #[test]
    fn validate_roots() -> anyhow::Result<()> {
        let tmpdir = TempDir::new()?;
        match Builder::default().root(&[tmpdir.path().join("missing")]) {
            Ok(_) => panic!("this should have failed"),
            Err(_) => {}
        }
        Ok(())
    }

    #[test]
    fn roundtrip() -> anyhow::Result<()> {
        let tmpdir = TempDir::new()?;
        let cfg = tmpdir.path().join("cfg");
        let anchor = settings::get_anchor(Some(cfg.clone()))?;
        // The builder validates that the roots exist. So we need to create them
        // for the test and canonicalize them, to ensure there is no risk of them
        // not round-tripping.
        let roots: Vec<PathBuf> = vec!["a", "b"]
            .iter()
            .map(|name| {
                let path = tmpdir.path().join(name);
                std::fs::create_dir(&path).unwrap();
                std::fs::canonicalize(path).unwrap()
            })
            .collect();
        Builder::default()
            .root(&roots)?
            .certificate("")
            .private_key("")
            .save(&anchor)?;

        let settings = load_impl(&anchor)?;
        assert_eq!(settings.backup().roots(), &roots);
        assert_eq!(settings.backup().db(), cfg.join("db"));
        assert_eq!(settings.backup().keyfile(), cfg.join("keyfile"));
        // TODO: validate certificates
        Ok(())
    }
}
