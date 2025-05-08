use super::field::LocalPath;
use anyhow::Context;
use rusqlite::{OptionalExtension, Transaction, named_params};
use rusqlite_migration::M;
use std::{collections::HashMap, path::PathBuf};

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

// Internal representation of a directory.
#[derive(Debug, PartialEq)]
pub struct Directory {
    pub path: LocalPath,
    pub root: bool,
    pub error: Option<String>,
}

// ---- Root-related operations ----

pub fn get_roots(txn: &Transaction) -> anyhow::Result<HashMap<PathBuf, Directory>> {
    let mut roots = HashMap::new();
    let mut stmt =
        txn.prepare("SELECT path, root, access_error FROM Directory WHERE root = TRUE")?;
    for row in stmt.query_map((), |row| {
        Ok(Directory {
            path: row.get(0)?,
            root: row.get(1)?,
            error: row.get(2)?,
        })
    })? {
        let row = row?;
        roots.insert(row.path()?, row);
    }
    Ok(roots)
}

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

pub fn update_root(txn: &Transaction, path: &LocalPath, is_root: bool) -> anyhow::Result<usize> {
    let changed = txn
        .execute(
            "UPDATE Directory SET root = :is_root WHERE path = :path",
            named_params! {":path": path, ":is_root": is_root},
        )
        .context("update_root")?;
    Ok(changed)
}

pub fn set_root_aim(txn: &Transaction, aim: i64) -> anyhow::Result<()> {
    txn.execute(
        "UPDATE Directory SET tree_aim = :aim WHERE root = TRUE",
        named_params! {":aim": aim},
    )?;
    Ok(())
}

pub fn set_path_aim(txn: &Transaction, aim: i64, path: &LocalPath) -> anyhow::Result<usize> {
    let count = txn.execute(
        "UPDATE Directory SET tree_aim = :aim WHERE path = :path",
        named_params! {":aim": aim, ":path": path},
    )?;
    Ok(count)
}

// ---- Directory-related operations ----

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

impl Directory {
    pub fn path(&self) -> anyhow::Result<PathBuf> {
        self.path
            .clone()
            .try_into()
            .context("failed to convert to path")
    }
}
