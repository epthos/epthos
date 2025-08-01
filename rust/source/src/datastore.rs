//! The datastore is responsible for tracking the content of backup
//! data.

use std::path::Path;

use anyhow::Context;

pub struct Datastore {
    conn: rusqlite::Connection,
}

impl Datastore {
    pub fn new(db: &Path) -> anyhow::Result<Datastore> {
        let conn = rusqlite::Connection::open(db).context("Failed to open db")?;
        Ok(Datastore { conn })
    }
}
