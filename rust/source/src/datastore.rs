//! The datastore is responsible for tracking the content of backup
//! data.

use std::path::Path;

use anyhow::Context;
use rusqlite_migration::{M, Migrations};

pub struct Datastore {
    conn: rusqlite::Connection,
}

impl Datastore {
    pub fn new(db: &Path) -> anyhow::Result<Datastore> {
        let mut conn = rusqlite::Connection::open(db).context("Failed to open db")?;
        initialize(&mut conn)?;
        Ok(Datastore { conn })
    }

    #[cfg(test)]
    pub fn new_in_memory() -> anyhow::Result<Datastore> {
        let mut conn =
            rusqlite::Connection::open_in_memory().context("Failed to open in-memory db")?;
        initialize(&mut conn)?;
        Ok(Datastore { conn })
    }

    fn migrations() -> Migrations<'static> {
        Migrations::new(vec![M::up(
            r#"
CREATE TABLE Backup (
    id BLOB PRIMARY KEY
) STRICT, WITHOUT ROWID;               
            "#,
        )])
    }
}

fn initialize(conn: &mut rusqlite::Connection) -> anyhow::Result<()> {
    conn.pragma_update(None, "foreign_keys", "ON")
        .context("Failed to enable foreign keys")?;
    Datastore::migrations()
        .to_latest(conn)
        .context("Failed to migrate Datastore database")?;
    Ok(())
}
