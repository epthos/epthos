//! The datastore is responsible for tracking the content of backup
//! data.

use crate::sql_model::LocalPath;
use anyhow::Context;
use rusqlite_migration::{M, Migrations};
use std::path::Path;

pub struct Datastore {
    conn: rusqlite::Connection,
}

impl Datastore {
    pub fn add(&self, path: LocalPath) -> anyhow::Result<()> {
        self.conn.execute(
            r#"
                INSERT INTO Backup (path) VALUES (:path)
            "#,
            rusqlite::named_params! {":path": path},
        )?;
        Ok(())
    }

    pub fn remove(&self, path: LocalPath) -> anyhow::Result<()> {
        self.conn.execute(
            r#"
                DELETE FROM Backup WHERE path = :path
            "#,
            rusqlite::named_params! {":path": path},
        )?;
        Ok(())
    }

    pub fn list(&self) -> anyhow::Result<Vec<LocalPath>> {
        let mut stmt = self.conn.prepare(r#"SELECT path FROM Backup"#)?;
        let result: rusqlite::Result<Vec<LocalPath>> = stmt
            .query_map((), |row| {
                let path: LocalPath = row.get(0)?;
                Ok(path)
            })
            .context("failed to list files")?
            .collect();
        Ok(result?)
    }

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
    path BLOB PRIMARY KEY
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn smoke() -> anyhow::Result<()> {
        let s = Datastore::new_in_memory()?;
        let p: LocalPath = PathBuf::from("a").into();

        s.add(p.clone())?;

        let current = s.list()?;
        assert_eq!(&current, &[p.clone()]);

        s.remove(p.clone())?;

        let current = s.list()?;
        assert!(current.is_empty());

        Ok(())
    }
}
