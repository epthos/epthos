//! The settings package provides a mechanism to load TOML settings
//! while keeping the file definition stable from the config results.
//! It also offers canned settings for common patterns (connection info,
//! debugging params, etc)
//!
//! For binary-specific needs, it offers a common way of validating
//! configs from the file and various helpers. Those common config
//! fragments are conventionally put in the following structure:
//!
//!    mod my_fragment {
//!       pub struct Settings {}  // The validated settings.
//!
//!       mod wire {
//!         pub struct Settings {}  // What's parsed from TOML.
//!       }
//!    }
//!
//! Such a fragment can then be loaded using:
//!
//!   // Determine the default anchor of the configs (location, etc).
//!   let anchor = settings::settings_context(None)?;
//!   let fragment : my_fragment::Settings = settings::load("service", &anchor)?;
//!
use anyhow::Context;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    env, fs,
    path::{Path, PathBuf},
};

/// Return the path to the specified anchored config.
pub fn path(anchor: &Anchor) -> PathBuf {
    Path::join(anchor.root.as_ref(), format!("{}.toml", &anchor.name))
}

/// Helper to load settings from a standard location.
pub fn load<T: Anchored>(anchor: &Anchor) -> anyhow::Result<T> {
    let file = path(anchor);
    let toml_data = fs::read_to_string(&file).context(format!("Config file is {:?}", &file))?;
    load_from_str(&toml_data, anchor)
}

/// Helper to save the underlying representation of data in the standard location.
pub fn save<T: serde::Serialize>(data: &T, anchor: &Anchor) -> anyhow::Result<()> {
    let file = path(anchor);
    fs::write(&file, save_to_str(data)?)?;

    Ok(())
}

/// Anchor represents the location where configs and ancillary files
/// are stored. A valid Anchor validates permissions and ensures settings
/// are evaluated in the right location.
pub struct Anchor {
    name: String,
    root: PathBuf,
}

/// Trait of a config object which has an underlying serializable
/// representation. The two objects are distinct, as the config object
/// proper has been sanitized and validated when "anchor()" has been
/// called.
pub trait Anchored: Sized {
    type Wire: DeserializeOwned;

    fn anchor(wire: &Self::Wire, anchor: &Anchor) -> anyhow::Result<Self>;
}

/// Settings required for TLS connections.
///
/// "certificate" & "private_key" define the identity of the local node.
/// The certificate chain must include intermediate certs if they exist.
/// "peer_root" is the certificate authority that will be used to validate
/// the remote party.
pub mod connection {
    use super::*;
    use tonic::transport::{Certificate, Identity};

    pub mod wire {
        use serde::{Deserialize, Serialize};

        /// Definition of the stored config for connections.
        #[derive(Debug, Deserialize, Serialize)]
        pub struct Settings {
            pub peer_root: String,
            pub certificate: String,
            pub private_key: String,
        }
    }

    /// Parsed connection info.
    #[derive(Clone, Debug)]
    pub struct Info {
        identity: Identity,
        peer_root: Certificate,
    }

    impl Info {
        pub fn new(identity: Identity, peer_root: Certificate) -> Info {
            Info {
                identity,
                peer_root,
            }
        }

        pub fn identity(&self) -> &Identity {
            &self.identity
        }
        pub fn peer_root(&self) -> &Certificate {
            &self.peer_root
        }
    }

    pub struct Settings {
        info: Info,
    }

    impl Settings {
        pub fn info(&self) -> &Info {
            &self.info
        }
    }

    impl Anchored for Settings {
        type Wire = wire::Settings;

        fn anchor(wire: &Self::Wire, _anchor: &Anchor) -> anyhow::Result<Self> {
            Ok(Settings {
                info: Info {
                    identity: Identity::from_pem(&wire.certificate, &wire.private_key),
                    peer_root: Certificate::from_pem(&wire.peer_root),
                },
            })
        }
    }
}

/// Common settings used by all processes, servers and clients.
pub mod process {
    use super::*;
    use logroller::{Compression, LogRollerBuilder, Rotation, RotationAge, TimeZone};
    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::prelude::*;

    pub mod wire {
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Deserialize, Serialize)]
        #[allow(clippy::upper_case_acronyms)]
        pub enum TracingLevel {
            TRACE,
            DEBUG,
            INFO,
            WARN,
            ERROR,
        }

        #[derive(Debug, Deserialize, Serialize)]
        pub struct Settings {
            pub tracing_level: TracingLevel,
        }
    }

    pub struct Settings {
        level: LevelFilter,
        logfile: Option<PathBuf>,
    }

    impl Settings {
        pub fn trace_level(&self) -> LevelFilter {
            self.level
        }
    }

    impl Anchored for Settings {
        type Wire = wire::Settings;

        fn anchor(wire: &Self::Wire, anchor: &Anchor) -> anyhow::Result<Self> {
            Ok(Settings {
                level: match wire.tracing_level {
                    wire::TracingLevel::TRACE => LevelFilter::TRACE,
                    wire::TracingLevel::DEBUG => LevelFilter::DEBUG,
                    wire::TracingLevel::INFO => LevelFilter::INFO,
                    wire::TracingLevel::WARN => LevelFilter::WARN,
                    wire::TracingLevel::ERROR => LevelFilter::ERROR,
                },
                logfile: Some(
                    anchor
                        .root
                        .join("logs")
                        .join(format!("{}.log", &anchor.name)),
                ),
            })
        }
    }

    /// Initializes the process with the provided settings.
    #[allow(dyn_drop)]
    pub fn init(settings: &Settings) -> anyhow::Result<()> {
        let mut layers = Vec::new();

        if let Some(ref logfile) = settings.logfile {
            let dir = logfile.parent().context("log dir is incomplete")?;
            let file = logfile.file_name().context("log dir is incomplete")?;
            std::fs::create_dir_all(dir).context("failed to create log dir")?;

            let appender = LogRollerBuilder::new(dir, Path::new(file))
                .rotation(Rotation::AgeBased(RotationAge::Daily))
                .max_keep_files(30)
                .time_zone(TimeZone::Local) // Use system local time zone when rotating files
                .compression(Compression::Gzip) // Compress rotated files with Gzip
                .build()?;
            let (writer, guard) = tracing_appender::non_blocking(appender);
            layers.push(
                tracing_subscriber::fmt::layer()
                    .with_writer(writer)
                    .with_ansi(false)
                    .with_thread_ids(true)
                    .with_filter(settings.trace_level())
                    .boxed(),
            );
            std::mem::forget(guard); // Will log till the end of times.
        } else {
            layers.push(
                tracing_subscriber::fmt::layer()
                    .with_thread_ids(true)
                    .with_filter(settings.trace_level())
                    .boxed(),
            );
        }

        if let Ok(flame) = std::env::var("EPTHOS_FLAME") {
            let (flame_layer, guard) = tracing_flame::FlameLayer::with_file(flame)?;
            layers.push(flame_layer.boxed());
            std::mem::forget(guard); // Will measure till the end of times.
        }
        tracing_subscriber::registry().with(layers).init();
        Ok(())
    }

    #[allow(dyn_drop)]
    pub fn debug() -> anyhow::Result<()> {
        init(&Settings {
            level: LevelFilter::TRACE,
            logfile: None,
        })
    }
}

/// Common settings used by all servers.
pub mod server {
    use super::*;
    use std::net::SocketAddr;

    pub mod wire {
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Deserialize, Serialize)]
        pub struct Settings {
            pub address: String,
        }
    }

    pub struct Settings {
        address: SocketAddr,
    }

    impl Settings {
        pub fn address(&self) -> &SocketAddr {
            &self.address
        }
    }

    impl Anchored for Settings {
        type Wire = wire::Settings;

        fn anchor(wire: &Self::Wire, _anchor: &Anchor) -> anyhow::Result<Self> {
            Ok(Settings {
                address: wire.address.parse()?,
            })
        }
    }
}

/// Common settings used by clients.
pub mod client {
    use super::*;
    use std::str::FromStr;
    use tonic::transport::Uri;

    #[derive(Debug, Clone)]
    pub struct Settings {
        name: String, // Name of the server on the certificate.
        address: Uri, // Address of the server.
    }

    pub mod wire {
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Deserialize, Serialize)]
        pub struct Settings {
            pub name: String,
            pub address: String,
        }
    }

    impl Settings {
        pub fn name(&self) -> &str {
            &self.name
        }
        pub fn address(&self) -> &Uri {
            &self.address
        }
    }

    impl Anchored for Settings {
        type Wire = wire::Settings;

        fn anchor(wire: &Self::Wire, _anchor: &Anchor) -> anyhow::Result<Self> {
            Ok(Settings {
                name: wire.name.clone(),
                address: Uri::from_str(&wire.address)?,
            })
        }
    }
}

/// A ConfigPath represents a path _inside_ the config directory, for things
/// like the encryption key, the database, etc.
#[derive(Serialize, Deserialize, Debug)]
#[serde(transparent)] // Deserializes the PathBuf as if it was the field.
pub struct ConfigPath(PathBuf);

impl ConfigPath {
    /// Get the full path to the config file, resolved within the config context.
    pub fn path(&self, anchor: &Anchor) -> PathBuf {
        anchor.root.as_path().join(&self.0)
    }
}

impl From<PathBuf> for ConfigPath {
    fn from(path: PathBuf) -> ConfigPath {
        ConfigPath(path)
    }
}

impl From<&str> for ConfigPath {
    fn from(path: &str) -> ConfigPath {
        ConfigPath(path.into())
    }
}

/// Error returned by |load|.
#[derive(Debug, thiserror::Error)]
pub enum SettingsError {
    #[error("Failed to load the config")]
    Config(#[from] anyhow::Error),
    #[error("Can't find configuration directory")]
    MissingConfigurationDirectory,
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("Failed to get config directory: {0:?}")]
    CreationError(#[from] platform::PlatformError),
    #[error("Directory permission have been changed")]
    UnsafePermissions,
}

impl Anchor {
    fn new(name: String, root: PathBuf) -> Result<Anchor, SettingsError> {
        platform::private_directory(&root)?;
        Ok(Anchor { name, root })
    }
}

fn load_from_str<T: Anchored>(toml_data: &str, anchor: &Anchor) -> anyhow::Result<T> {
    let wire = toml::from_str::<T::Wire>(toml_data)?;
    T::anchor(&wire, anchor)
}

fn save_to_str<T: serde::Serialize>(data: &T) -> anyhow::Result<String> {
    Ok(toml::to_string(data)?)
}

/// Returns a valid anchor for the config.
pub fn get_anchor(name: String, default: Option<PathBuf>) -> Result<Anchor, SettingsError> {
    let root = default
        .or(home_override())
        .or(default_home())
        .ok_or(SettingsError::MissingConfigurationDirectory)?;
    Anchor::new(name, root)
}

fn home_override() -> Option<PathBuf> {
    Some(PathBuf::from(env::var("EPTHOS_HOME").ok()?))
}

#[cfg(windows)]
fn default_home() -> Option<PathBuf> {
    let local_app_data = env::var("LOCALAPPDATA").ok()?;
    let mut path = PathBuf::from(local_app_data);
    path.push("Epthos");

    Some(path)
}

#[cfg(unix)]
fn default_home() -> Option<PathBuf> {
    let local_app_data = env::var("HOME").ok()?;
    let mut path = PathBuf::from(local_app_data);
    path.push(".epthos");

    Some(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_path() -> anyhow::Result<()> {
        // Verify that the "cfg" config in the settings directory
        // is properly anchored when parsed.
        let settings_dir: PathBuf = "dir".into();
        let cfg: PathBuf = "cfg".into();

        // Intentionally bypass directory validation as we only test
        // anchoring of paths.
        let anchor = Anchor {
            name: "test".to_owned(),
            root: settings_dir.clone(),
        };
        let cfg_path: ConfigPath = cfg.clone().into();

        assert_eq!(cfg_path.path(&anchor), settings_dir.join(cfg));
        Ok(())
    }

    #[test]
    fn anchor_stuff() -> anyhow::Result<()> {
        let toml = r#"
address = "127.0.0.1:1234"
"#;
        // Intentionally bypass directory validation as we only test
        // file parsing.
        let anchor = Anchor {
            name: "test".to_owned(),
            root: "path".into(),
        };
        let parsed: server::Settings = load_from_str(toml, &anchor)?;
        assert_eq!(parsed.address().port(), 1234);
        Ok(())
    }
}
