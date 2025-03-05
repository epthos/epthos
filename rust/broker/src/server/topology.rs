use crate::server::SinkInfo;
use lazy_static::lazy_static;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

/// The cache holds information about sinks, and knows how
/// to pair them with the sources that are expected to use
/// them.
#[derive(Debug)]
pub struct Cache {
    sinks: Mutex<HashMap<String, SinkInfo>>,
    mapping: Mapping,
}

impl Cache {
    pub fn new(mapping: Mapping) -> Cache {
        Cache {
            sinks: Mutex::new(HashMap::new()),
            mapping,
        }
    }

    /// Update information about a sink.
    pub fn update_sink(&self, sink: SinkInfo) {
        self.sinks.lock().unwrap().insert(sink.id.clone(), sink);
    }

    /// Get known sinks for the specified source.
    pub fn for_source(&self, source: &str) -> Vec<SinkInfo> {
        let sinks = self.mapping.get_sinks(source);
        let cache = self.sinks.lock().unwrap();

        let mut results = Vec::new();
        for sink in sinks {
            if let Some(sink) = cache.get(sink) {
                results.push(sink.clone());
            }
        }
        results
    }
}

/// The mapping is responsible for associated sources
/// and sinks.
#[derive(Debug, Default, Clone)]
pub struct Mapping {
    source: HashMap<String, HashSet<String>>,
}

lazy_static! {
    static ref EMPTY: HashSet<String> = HashSet::new();
}

impl Mapping {
    /// Declare that a source can connect to a sink.
    pub fn add_pair<S: Into<String>>(&mut self, source: S, sink: S) -> &mut Self {
        let source: String = source.into();
        let sink: String = sink.into();
        if let Some(sinks) = self.source.get_mut(&source) {
            sinks.insert(sink);
        } else {
            let mut sinks = HashSet::new();
            sinks.insert(sink);
            self.source.insert(source, sinks);
        }
        self
    }

    /// Get all the sinks that a source can connect to.
    pub fn get_sinks<'a>(&'a self, source: &str) -> &'a HashSet<String> {
        if let Some(sinks) = self.source.get(source) {
            sinks
        } else {
            &EMPTY
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn mapping() {
        let mut m = Mapping::default();
        m.add_pair("src1", "snk1")
            .add_pair("src1", "snk2")
            .add_pair("src2", "snk3");

        let mut for_src1 = HashSet::new();
        for_src1.insert("snk1".to_string());
        for_src1.insert("snk2".to_string());
        assert_eq!(m.get_sinks("src1"), &for_src1);

        let mut for_src2 = HashSet::new();
        for_src2.insert("snk3".to_string());
        assert_eq!(m.get_sinks("src2"), &for_src2);

        let empty = HashSet::new();
        assert_eq!(m.get_sinks("nope"), &empty);
    }

    #[test]
    fn smoke_test() {
        // Ensure that the right sinks are returned to the
        // appropriate source.
        let mut m = Mapping::default();
        m.add_pair("111.src.epthos.net", "222.snk.epthos.net");

        let cache = Cache::new(m);

        assert_eq!(cache.for_source("111.src.epthos.net"), vec![]);

        let sink = SinkInfo {
            id: "222.snk.epthos.net".to_string(),
            listening_on: vec!["a".to_string()],
        };
        cache.update_sink(sink.clone());

        assert_eq!(cache.for_source("111.src.epthos.net"), vec![sink]);
        assert_eq!(cache.for_source("other"), vec![]);
    }
}
