use anyhow::{anyhow, Context};
use hex::FromHex;
use serde::{Deserialize, Serialize};
use std::{io::Write, path::Path};

pub const KEY_LEN: usize = 128 / 8;

/// A key used for encryption and decryption. This is generally
/// a derived key from a root key.
#[derive(Debug, PartialEq)]
pub struct Key([u8; KEY_LEN]);

/// The root key that is durable and from which all other keys are derived.
pub struct Durable {
    key: Key,
    version: u16,
}

impl From<[u8; KEY_LEN]> for Key {
    fn from(key: [u8; KEY_LEN]) -> Key {
        Key(key)
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Durable {
    pub fn new(key: Key, version: u16) -> Durable {
        Durable { key, version }
    }

    /// Load keys from a file.
    pub fn from_file(file_path: &Path) -> anyhow::Result<Self> {
        let toml = std::fs::read_to_string(file_path).context("reading SourceKey")?;
        let ondisk: OnDisk = toml::from_str(&toml).context("deserializing SourceKey")?;
        if ondisk.key.len() != 1 {
            return Err(anyhow!("Invalid SourceKey file"));
        }
        Durable::try_from(ondisk.key[0].clone())
    }

    /// Save keys to a file.
    pub fn to_file(&self, file_path: &Path) -> anyhow::Result<()> {
        let ondisk = OnDisk {
            key: vec![self.into()],
        };

        let toml = toml::to_string(&ondisk).context("serializing SourceKey")?;
        let mut file = std::fs::File::create(file_path).context("opening SourceKey file")?;
        file.write_all(toml.as_bytes())
            .context("writing SourceKey file")?;
        Ok(())
    }

    pub fn key(&self) -> &Key {
        &self.key
    }

    pub fn version(&self) -> u16 {
        self.version
    }
}

// Helper struct to serialize and deserialize the key to disk.
#[derive(Serialize, Deserialize, Debug)]
struct OnDisk {
    key: Vec<String>,
}

impl From<&Durable> for String {
    fn from(key: &Durable) -> String {
        format!("{}::{}", key.version, hex::encode(key.key.as_ref()))
    }
}

impl TryFrom<String> for Durable {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let parts = value.split("::").collect::<Vec<&str>>();
        if parts.len() != 2 {
            return Err(anyhow!("Invalid key format"));
        }
        let version = parts[0].parse::<u16>()?;
        let key = <[u8; KEY_LEN]>::from_hex(parts[1])?;
        Ok(Durable {
            key: key.into(),
            version,
        })
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn test_key_roundtrip() {
        let key = [0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let durable = super::Durable::new(key.into(), 1);
        let string = String::from(&durable);
        assert_eq!(string, "1::000102030405060708090a0b0c0d0e0f");
        let durable2 = super::Durable::try_from(string).unwrap();
        assert_eq!(durable.key(), durable2.key());
        assert_eq!(durable.version(), durable2.version());
    }
}
