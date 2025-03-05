use crate::server::topology::Mapping;
use settings::{connection, process, server};
use std::path::PathBuf;

mod wire {
    use super::*;
    use serde::Deserialize;
    use std::collections::{HashMap, HashSet};

    /// Represents the overall configuration data for a Broker.
    #[derive(Debug, Deserialize)]
    pub struct Settings {
        pub connection: connection::wire::Settings,
        pub process: process::wire::Settings,
        pub server: server::wire::Settings,
        pub mappings: HashMap<String, HashSet<String>>,
    }
}

pub fn load(home: Option<PathBuf>) -> anyhow::Result<Settings> {
    let anchor = settings::get_anchor(home)?;
    settings::load::<Settings>("broker", &anchor)
}

pub struct Settings {
    connection: connection::Settings,
    process: process::Settings,
    server: server::Settings,
    mappings: Mapping,
}

impl Settings {
    pub fn connection(&self) -> &connection::Settings {
        &self.connection
    }

    pub fn server(&self) -> &server::Settings {
        &self.server
    }

    pub fn process(&self) -> &process::Settings {
        &self.process
    }

    pub fn mappings(&self) -> &Mapping {
        &self.mappings
    }
}

impl settings::Anchored for Settings {
    type Wire = wire::Settings;

    fn anchor(wire: &Self::Wire, anchor: &settings::Anchor) -> anyhow::Result<Self> {
        use settings::{connection, process, server};

        let mut mappings = Mapping::default();
        for (src, sinks) in &wire.mappings {
            for sink in sinks {
                mappings.add_pair(src, sink);
            }
        }

        Ok(Settings {
            connection: connection::Settings::anchor(&wire.connection, anchor)?,
            process: process::Settings::anchor(&wire.process, anchor)?,
            server: server::Settings::anchor(&wire.server, anchor)?,
            mappings,
        })
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn parse_mappings() {}
}
