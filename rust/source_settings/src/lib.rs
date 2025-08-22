use anyhow::Context;
use settings::{Anchor, client, connection, process, server};
use std::path::{Path, PathBuf};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use thiserror::Error;

/// Loads and validates the settings.
pub fn load() -> anyhow::Result<Settings> {
    load_impl(&anchor()?)
}

/// Resolve the anchor for the Source settings.
pub fn anchor() -> anyhow::Result<Anchor> {
    settings::get_anchor(NAME.to_owned(), None).context("can't get the anchor")
}

/// All the validated settings.
pub struct Settings {
    broker: client::Settings,
    connection: connection::Settings,
    server: server::Settings,
    process: process::Settings,
    backup: Backup,
    filestore: Filestore,
    datastore: Datastore,
}

/// Backup-related settings.
pub struct Backup {
    root: Vec<PathBuf>,
    keyfile: PathBuf,
}

/// Filestore-related settings.
pub struct Filestore {
    db: PathBuf,
}

/// Datastore-related settings.
pub struct Datastore {
    db: PathBuf,
}

impl Settings {
    pub fn broker(&self) -> &client::Settings {
        &self.broker
    }

    pub fn connection(&self) -> &connection::Settings {
        &self.connection
    }

    pub fn server(&self) -> &server::Settings {
        &self.server
    }

    pub fn process(&self) -> &process::Settings {
        &self.process
    }

    pub fn backup(&self) -> &Backup {
        &self.backup
    }

    pub fn filestore(&self) -> &Filestore {
        &self.filestore
    }

    pub fn datastore(&self) -> &Datastore {
        &self.datastore
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
}

impl Filestore {
    pub fn db(&self) -> &Path {
        &self.db
    }
}

impl Datastore {
    pub fn db(&self) -> &Path {
        &self.db
    }
}

/// All the customizable options for creating a fresh config.
#[derive(Debug, Default)]
pub struct Builder {
    certificate: String,
    private_key: String,
    address: String,
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

    pub fn address<T: Into<String>>(mut self, address: T) -> Builder {
        self.address = address.into();
        self.set.insert(Fields::Address);
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
        settings::path(anchor)
    }

    pub fn save(self, anchor: &settings::Anchor) -> anyhow::Result<()> {
        if !Fields::iter().all(|variant| self.set.contains(&variant)) {
            return Err(ConfigError::MissingField).context("Can't write Source settings");
        }
        let settings = wire::Settings {
            broker: client::wire::Settings {
                address: constants::BROKER_ADDRESS.into(),
                name: constants::BROKER_NAME.into(),
            },
            connection: connection::wire::Settings {
                peer_root: constants::CA_ROOT.into(),
                certificate: self.certificate,
                private_key: self.private_key,
            },
            server: server::wire::Settings {
                address: self.address,
            },
            process: process::wire::Settings {
                tracing_level: process::wire::TracingLevel::INFO,
            },
            backup: wire::Backup {
                root: self.root,
                keyfile: "keyfile".into(),
            },
            filestore: wire::Filestore {
                db: "filestore".into(),
            },
            datastore: wire::Datastore {
                db: "datastore".into(),
            },
        };
        settings::save(&settings, anchor)?;
        Ok(())
    }
}

pub fn load_impl(anchor: &settings::Anchor) -> anyhow::Result<Settings> {
    settings::load::<Settings>(anchor)
}

// Canonical name of the config file.
const NAME: &str = "source";

#[derive(Debug, PartialEq, Eq, Hash, EnumIter)]
enum Fields {
    Certificate,
    PrivateKey,
    Address,
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
        pub broker: client::wire::Settings,
        pub connection: connection::wire::Settings,
        pub server: server::wire::Settings,
        pub process: process::wire::Settings,
        pub backup: Backup,
        pub filestore: Filestore,
        pub datastore: Datastore,
    }

    /// Settings related to data backup.
    #[derive(Debug, Deserialize, Serialize)]
    pub struct Backup {
        pub root: Vec<PathBuf>,
        pub keyfile: settings::ConfigPath,
    }

    #[derive(Debug, Deserialize, Serialize)]
    pub struct Filestore {
        pub db: settings::ConfigPath,
    }

    #[derive(Debug, Deserialize, Serialize)]
    pub struct Datastore {
        pub db: settings::ConfigPath,
    }
}

impl settings::Anchored for Settings {
    type Wire = wire::Settings;

    fn anchor(wire: &Self::Wire, anchor: &settings::Anchor) -> anyhow::Result<Settings> {
        Ok(Settings {
            broker: client::Settings::anchor(&wire.broker, anchor)?,
            connection: connection::Settings::anchor(&wire.connection, anchor)?,
            process: process::Settings::anchor(&wire.process, anchor)?,
            server: server::Settings::anchor(&wire.server, anchor)?,
            backup: Backup::anchor(&wire.backup, anchor)?,
            filestore: Filestore::anchor(&wire.filestore, anchor)?,
            datastore: Datastore::anchor(&wire.datastore, anchor)?,
        })
    }
}

impl settings::Anchored for Backup {
    type Wire = wire::Backup;

    fn anchor(wire: &Self::Wire, anchor: &settings::Anchor) -> anyhow::Result<Self> {
        Ok(Backup {
            root: wire.root.clone(),
            keyfile: wire.keyfile.path(anchor),
        })
    }
}

impl settings::Anchored for Filestore {
    type Wire = wire::Filestore;

    fn anchor(wire: &Self::Wire, anchor: &settings::Anchor) -> anyhow::Result<Self> {
        Ok(Filestore {
            db: wire.db.path(anchor),
        })
    }
}

impl settings::Anchored for Datastore {
    type Wire = wire::Datastore;

    fn anchor(wire: &Self::Wire, anchor: &settings::Anchor) -> anyhow::Result<Self> {
        Ok(Datastore {
            db: wire.db.path(anchor),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::net::SocketAddr;
    use tempfile::TempDir;

    #[test]
    fn check_all_fields() -> anyhow::Result<()> {
        let tmpdir = TempDir::new()?;
        let cfg = tmpdir.path().join("cfg");
        match Builder::default().save(&settings::get_anchor("test".to_owned(), Some(cfg))?) {
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
        let anchor = settings::get_anchor("test".to_owned(), Some(cfg.clone()))?;
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
            .address("127.0.0.1:1111")
            .save(&anchor)?;

        let settings = load_impl(&anchor)?;
        assert_eq!(settings.backup().roots(), &roots);
        assert_eq!(settings.backup().keyfile(), cfg.join("keyfile"));
        assert_eq!(settings.filestore().db(), cfg.join("filestore"));
        let addr: SocketAddr = "127.0.0.1:1111".parse()?;
        assert_eq!(settings.server().address(), &addr);
        // TODO: validate certificates
        Ok(())
    }
}
