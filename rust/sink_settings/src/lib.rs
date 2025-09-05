//! Load and manipulate settings for a Sink.
use anyhow::Context;
use settings::{client, connection, process, server};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use thiserror::Error;

/// Loads and validates the settings.
pub fn load() -> anyhow::Result<Settings> {
    load_impl(&settings::get_anchor(None)?)
}

/// All the validated settings.
pub struct Settings {
    broker: client::Settings,
    connection: connection::Settings,
    process: process::Settings,
    server: server::Settings,
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
}

/// All the customizable options for creating a fresh config.
#[derive(Debug, Default)]
pub struct Builder {
    address: String,
    certificate: String,
    private_key: String,

    set: std::collections::HashSet<Fields>,
}

impl Builder {
    pub fn address<T: Into<String>>(mut self, address: T) -> Self {
        self.set.insert(Fields::Address);
        self.address = address.into();
        self
    }

    pub fn certificate<T: Into<String>>(mut self, certificate: T) -> Self {
        self.set.insert(Fields::Certificate);
        self.certificate = certificate.into();
        self
    }

    pub fn private_key<T: Into<String>>(mut self, private_key: T) -> Self {
        self.set.insert(Fields::PrivateKey);
        self.private_key = private_key.into();
        self
    }

    pub fn path(anchor: &settings::Anchor) -> std::path::PathBuf {
        settings::path(NAME, anchor)
    }

    pub fn save(self, anchor: &settings::Anchor) -> anyhow::Result<()> {
        if !Fields::iter().all(|variant| self.set.contains(&variant)) {
            return Err(ConfigError::MissingField).context("Can't write Sink settings");
        }
        let settings = wire::Settings {
            broker: client::wire::Settings {
                name: constants::BROKER_NAME.into(),
                address: constants::BROKER_ADDRESS.into(),
            },
            connection: connection::wire::Settings {
                peer_root: constants::CA_ROOT.into(),
                certificate: self.certificate,
                private_key: self.private_key,
            },
            process: process::wire::Settings {
                tracing_level: process::wire::TracingLevel::INFO,
            },
            server: server::wire::Settings {
                address: self.address,
            },
        };
        settings::save(&settings, NAME, anchor)?;
        Ok(())
    }
}

fn load_impl(anchor: &settings::Anchor) -> anyhow::Result<Settings> {
    settings::load::<Settings>(NAME, anchor)
}

#[derive(Debug, PartialEq, Eq, Hash, EnumIter)]
enum Fields {
    Address,
    Certificate,
    PrivateKey,
}

#[derive(Error, Debug)]
enum ConfigError {
    #[error("Some fields have not been set")]
    MissingField,
}

const NAME: &str = "sink";

mod wire {
    use super::*;
    use serde::{Deserialize, Serialize};

    /// Represents the overall configuration data for a Source.
    #[derive(Debug, Deserialize, Serialize)]
    pub struct Settings {
        pub broker: client::wire::Settings,
        pub connection: connection::wire::Settings,
        pub process: process::wire::Settings,
        pub server: server::wire::Settings,
    }
}

impl settings::Anchored for Settings {
    type Wire = wire::Settings;

    fn anchor(wire: &Self::Wire, anchor: &settings::Anchor) -> anyhow::Result<Self> {
        Ok(Settings {
            broker: client::Settings::anchor(&wire.broker, anchor)?,
            connection: connection::Settings::anchor(&wire.connection, anchor)?,
            process: process::Settings::anchor(&wire.process, anchor)?,
            server: server::Settings::anchor(&wire.server, anchor)?,
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
    fn roundtrip() -> anyhow::Result<()> {
        let tmpdir = TempDir::new()?;
        let cfg = tmpdir.path().join("cfg");
        let anchor = settings::get_anchor(Some(cfg))?;
        Builder::default()
            .address("127.0.0.1:1234")
            .certificate("")
            .private_key("")
            .save(&anchor)?;

        let settings = load_impl(&anchor)?;
        assert_eq!(settings.server().address().port(), 1234);
        assert_eq!(settings.broker().name(), constants::BROKER_NAME);
        assert_eq!(settings.broker().address(), constants::BROKER_ADDRESS);
        // TODO: validate certificates
        Ok(())
    }
}
