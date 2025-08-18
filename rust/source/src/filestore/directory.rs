use crate::sql_model::LocalPath;
use anyhow::Context;
use rusqlite::{OptionalExtension, Transaction, named_params};
use rusqlite_migration::M;
use std::collections::HashSet;

pub const SQL: M<'_> = M::up(
    r#"
CREATE TABLE Directory (
    path     BLOB PRIMARY KEY,     -- in local form, canonicalized
    root     INTEGER NOT NULL,  -- Boolean
    -- When walking the tree, tree_aim marks directories that have
    -- been seen, and tree_gen marks directories that haven been
    -- checked.
    tree_aim INTEGER NOT NULL,
    tree_gen INTEGER NOT NULL,

    -- A directory can be either readable (with completed set)
    -- or unreadable (with access_error set).
    complete     INTEGER, -- Boolean
    access_error TEXT
) STRICT;
"#,
);

// ---- Root-related operations ----

/// Return all the roots.
pub fn get_roots(txn: &Transaction) -> anyhow::Result<HashSet<LocalPath>> {
    let mut roots = HashSet::new();
    let mut stmt = txn.prepare("SELECT path, root FROM Directory WHERE root = TRUE")?;
    for row in stmt.query_map((), |row| {
        let path: LocalPath = row.get(0)?;
        Ok(path)
    })? {
        roots.insert(row?);
    }
    Ok(roots)
}

/// Add a new root. This only happens outside of scans, so the gen and aim are
/// set to zero to trigger a new scan.
pub fn add_root(txn: &Transaction, path: &LocalPath) -> anyhow::Result<()> {
    txn.execute(
        r#"
        INSERT INTO Directory(path, root, tree_aim, tree_gen, complete)
        VALUES (:path, TRUE, 0, 0, 0)
        "#,
        named_params! {":path": path},
    )
    .context("add_root")?;
    Ok(())
}

/// Change the rootness of a directory.
pub fn update_root(txn: &Transaction, path: &LocalPath, is_root: bool) -> anyhow::Result<usize> {
    let changed = txn
        .execute(
            "UPDATE Directory SET root = :is_root WHERE path = :path",
            named_params! {":path": path, ":is_root": is_root},
        )
        .context("update_root")?;
    Ok(changed)
}

/// Update the aim of all the roots to |aim|.
pub fn set_root_aim(txn: &Transaction, aim: i64) -> anyhow::Result<()> {
    txn.execute(
        "UPDATE Directory SET tree_aim = :aim WHERE root = TRUE",
        named_params! {":aim": aim},
    )?;
    Ok(())
}

// ---- Directory-related operations ----

/// Update the aim of a given directory (expected to be a non-root).
/// Returns the number of affected directories, either zero or one.
pub fn set_path_aim(txn: &Transaction, aim: i64, path: &LocalPath) -> anyhow::Result<usize> {
    let count = txn.execute(
        "UPDATE Directory SET tree_aim = :aim WHERE path = :path",
        named_params! {":aim": aim, ":path": path},
    )?;
    Ok(count)
}

pub fn next(txn: &Transaction, aim: i64) -> anyhow::Result<Option<LocalPath>> {
    let result = txn
        .query_row(
            r#"
                SELECT path FROM Directory
                WHERE
                  tree_gen < tree_aim AND tree_aim = :aim
                ORDER BY path LIMIT 1"#,
            named_params! {":aim": aim},
            |row| {
                let path: LocalPath = row.get(0)?;
                Ok(path)
            },
        )
        .optional()
        .context("Failed to find next dir to scan")?;
    Ok(result)
}

// Insert a non-root directory, typically as part of a scan.
pub fn insert(txn: &Transaction, path: &LocalPath, aim: i64) -> anyhow::Result<()> {
    txn.execute(
        r#"
        INSERT INTO Directory(path, root, tree_aim, tree_gen, complete)
        VALUES (:path, :root, :tree_aim, :tree_gen, :complete)
        "#,
        named_params! {":path": path, ":root": false,
        ":tree_aim": aim, ":tree_gen":0, ":complete": false },
    )?;
    Ok(())
}

pub fn update_dir_complete(
    txn: &Transaction,
    path: &LocalPath,
    tree_gen: i64,
    complete: bool,
) -> anyhow::Result<()> {
    update_dir(txn, path, tree_gen, Some(complete), None)
}

pub fn update_dir_error(
    txn: &Transaction,
    path: &LocalPath,
    tree_gen: i64,
    error: String,
) -> anyhow::Result<()> {
    update_dir(txn, path, tree_gen, None, Some(error))
}

fn update_dir(
    txn: &Transaction,
    path: &LocalPath,
    tree_gen: i64,
    complete: Option<bool>,
    access_error: Option<String>,
) -> anyhow::Result<()> {
    txn.execute(
        r#"
          UPDATE Directory SET tree_gen = :tree_gen, complete = :complete, access_error = :access_error
          WHERE path = :path
        "#,
        named_params!{
            ":path": path, ":tree_gen": tree_gen,
            ":complete": complete, ":access_error": access_error,
        },
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filestore::{Connection, Timing};
    use std::{
        path::{Path, PathBuf},
        sync::Arc,
    };

    #[test]
    fn add_and_remove_roots() -> anyhow::Result<()> {
        let mut cnx =
            Connection::new_in_memory(Arc::new(crypto::Random::new()), Timing::default())?;
        let got = |cnx: &mut Connection| -> anyhow::Result<Vec<(PathBuf, bool)>> {
            Ok(pth::dump(cnx.conn())?
                .into_iter()
                .map(|d| (d.path, d.root))
                .collect())
        };

        assert!(get_roots(&cnx.conn().transaction()?)?.is_empty());
        assert!(got(&mut cnx)?.is_empty());

        let p1: LocalPath = Path::new("/a/b").to_owned().into();

        {
            let tx = cnx.conn().transaction()?;
            add_root(&tx, &p1)?;
            assert_eq!(get_roots(&tx)?, HashSet::from([p1.clone()]));
            tx.commit()?;
        }
        {
            let tx = cnx.conn().transaction()?;
            update_root(&tx, &p1, false)?;
            assert!(get_roots(&tx)?.is_empty());
            tx.commit()?;
        }
        {
            let tx = cnx.conn().transaction()?;
            update_root(&tx, &p1, true)?;
            assert_eq!(get_roots(&tx)?, HashSet::from([p1.clone()]));
            tx.commit()?;
        }
        Ok(())
    }

    #[test]
    fn manipulate_aim_and_gen() -> anyhow::Result<()> {
        let mut cnx =
            Connection::new_in_memory(Arc::new(crypto::Random::new()), Timing::default())?;
        let tx = cnx.conn().transaction()?;

        let p1: LocalPath = Path::new("/a/b").to_owned().into();
        let p2: LocalPath = Path::new("/a/b/c").to_owned().into();

        add_root(&tx, &p1)?;
        set_root_aim(&tx, 10)?;
        assert_eq!(next(&tx, 10)?, Some(p1.clone()));
        assert_eq!(next(&tx, 11)?, None);

        assert_eq!(set_path_aim(&tx, 11, &p2)?, 0); // p2 was no created yet.

        insert(&tx, &p2, 10)?;
        assert_eq!(set_path_aim(&tx, 11, &p2)?, 1);

        assert_eq!(next(&tx, 11)?, Some(p2.clone()));
        Ok(())
    }

    #[test]
    fn complete_scan() -> anyhow::Result<()> {
        let mut cnx =
            Connection::new_in_memory(Arc::new(crypto::Random::new()), Timing::default())?;
        let p1: LocalPath = Path::new("/a/b").to_owned().into();

        let got = |cnx: &mut Connection| -> anyhow::Result<Vec<(PathBuf, u64, u64, Option<bool>, Option<String>)>> {
            Ok(pth::dump(cnx.conn())?
                .into_iter()
                .map(|d| (d.path, d.tree_gen, d.tree_aim, d.complete, d.error))
                .collect())
        };

        {
            let tx = cnx.conn().transaction()?;

            add_root(&tx, &p1)?;
            set_root_aim(&tx, 10)?;

            update_dir_complete(&tx, &p1, 10, true)?;
            assert_eq!(next(&tx, 10)?, None);

            tx.commit()?;
        }
        assert_eq!(
            got(&mut cnx)?,
            vec![("/a/b".into(), 10, 10, Some(true), None)]
        );

        {
            let tx = cnx.conn().transaction()?;
            set_root_aim(&tx, 11)?;

            update_dir_complete(&tx, &p1, 11, false)?;
            assert_eq!(next(&tx, 11)?, None);

            tx.commit()?;
        }
        assert_eq!(
            got(&mut cnx)?,
            vec![("/a/b".into(), 11, 11, Some(false), None)]
        );

        {
            let tx = cnx.conn().transaction()?;
            set_root_aim(&tx, 12)?;

            update_dir_error(&tx, &p1, 12, "boom".to_owned())?;
            assert_eq!(next(&tx, 12)?, None);

            tx.commit()?;
        }
        assert_eq!(
            got(&mut cnx)?,
            vec![("/a/b".into(), 12, 12, None, Some("boom".to_owned()))]
        );
        Ok(())
    }
}

#[cfg(test)]
pub mod pth {
    //! public test helpers
    use crate::sql_model::LocalPath;
    use anyhow::Context;
    use std::path::PathBuf;

    #[derive(Debug, PartialEq)]
    pub struct FullDirectory {
        pub path: PathBuf,
        pub root: bool,
        pub tree_gen: u64,
        pub tree_aim: u64,
        pub complete: Option<bool>,
        pub error: Option<String>,
    }

    pub fn dump(conn: &mut rusqlite::Connection) -> anyhow::Result<Vec<FullDirectory>> {
        let mut result = vec![];
        let mut stmt = conn.prepare(
            "SELECT path, root, tree_aim, tree_gen, complete, access_error FROM Directory ORDER BY path",
        )?;
        for row in stmt
            .query_map((), |row| {
                let path: LocalPath = row.get(0)?;
                Ok(FullDirectory {
                    path: path.try_into()?,
                    root: row.get(1)?,
                    tree_aim: row.get(2)?,
                    tree_gen: row.get(3)?,
                    complete: row.get(4)?,
                    error: row.get(5)?,
                })
            })
            .context("Failed to list directories")?
        {
            result.push(row?);
        }
        Ok(result)
    }
}
