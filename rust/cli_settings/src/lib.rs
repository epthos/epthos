use std::path::PathBuf;

use anyhow::Context;
use settings::{
    client, connection,
    process::{self, wire::TracingLevel},
};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use thiserror::Error;

/// Loads and validates the settings.
pub fn load() -> anyhow::Result<Settings> {
    load_impl(&settings::get_anchor(None)?)
}

pub struct Settings {
    source: client::Settings,
    connection: connection::Settings,
    process: process::Settings,
}

impl Settings {
    pub fn source(&self) -> &client::Settings {
        &self.source
    }
    pub fn connection(&self) -> &connection::Settings {
        &self.connection
    }
    pub fn process(&self) -> &process::Settings {
        &self.process
    }
}

#[derive(Debug, Default)]
pub struct Builder {
    certificate: String,
    private_key: String,
    name: String,
    address: String,

    set: std::collections::HashSet<Fields>,
}

#[derive(Error, Debug)]
enum ConfigError {
    #[error("Some fields have not been set")]
    MissingField,
}

/// All the customizable options for creating a fresh config.
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

    pub fn name<T: Into<String>>(mut self, name: T) -> Builder {
        self.name = name.into();
        self.set.insert(Fields::Name);
        self
    }

    pub fn address<T: Into<String>>(mut self, address: T) -> Builder {
        self.address = address.into();
        self.set.insert(Fields::Address);
        self
    }

    pub fn path(anchor: &settings::Anchor) -> PathBuf {
        settings::path(NAME, anchor)
    }

    pub fn save(self, anchor: &settings::Anchor) -> anyhow::Result<()> {
        if !Fields::iter().all(|variant| self.set.contains(&variant)) {
            return Err(ConfigError::MissingField).context("Can't write CLI settings");
        }
        let settings = wire::Settings {
            source: client::wire::Settings {
                address: self.address,
                name: self.name,
            },
            connection: connection::wire::Settings {
                peer_root: constants::CA_ROOT.into(),
                certificate: self.certificate,
                private_key: self.private_key,
            },
            process: process::wire::Settings {
                tracing_level: TracingLevel::INFO,
            },
        };
        settings::save(&settings, NAME, anchor)?;
        Ok(())
    }
}

mod wire {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize)]
    pub struct Settings {
        pub source: client::wire::Settings,
        pub connection: connection::wire::Settings,
        pub process: process::wire::Settings,
    }
}

impl settings::Anchored for Settings {
    type Wire = wire::Settings;

    fn anchor(wire: &Self::Wire, anchor: &settings::Anchor) -> anyhow::Result<Settings> {
        Ok(Settings {
            source: client::Settings::anchor(&wire.source, anchor)?,
            connection: connection::Settings::anchor(&wire.connection, anchor)?,
            process: process::Settings::anchor(&wire.process, anchor)?,
        })
    }
}

const NAME: &str = "cli";

fn load_impl(anchor: &settings::Anchor) -> anyhow::Result<Settings> {
    settings::load::<Settings>(NAME, anchor)
}

#[derive(Debug, PartialEq, Eq, Hash, EnumIter)]
enum Fields {
    Certificate,
    PrivateKey,
    Name,
    Address,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn roundtrip() -> anyhow::Result<()> {
        let tmpdir = TempDir::new()?;
        let cfg = tmpdir.path().join("cfg");
        let anchor = settings::get_anchor(Some(cfg.clone()))?;
        Builder::default()
            .certificate("")
            .private_key("")
            .name("local source")
            .address("127.0.0.1:1111")
            .save(&anchor)?;

        let settings = load_impl(&anchor)?;
        assert_eq!(settings.source().address().host(), Some("127.0.0.1"));
        assert_eq!(settings.source().address().port_u16(), Some(1111));
        assert_eq!(settings.source().name(), "local source");
        Ok(())
    }
}
