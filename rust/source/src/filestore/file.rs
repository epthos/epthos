use super::field::{
    FileState, LocalPath, StoredEncryptionGroup, TimeInMicroseconds, TimeInSeconds,
};
use crate::model::FileSize;
use anyhow::{Context, anyhow};
use rusqlite::{OptionalExtension, Transaction, named_params, types::FromSqlError};
use rusqlite_migration::M;

pub const SQL: M<'_> = M::up(
    r#"
CREATE TABLE File (
    path   BLOB PRIMARY KEY,

    state INTEGER NOT NULL,  -- One of FileState's values.

    -- Last scan gen this file was seen.
    tree_gen INTEGER NOT NULL,

    -- Next time the file will be hashed, in seconds since epoch.
    next_hash INTEGER NOT NULL,

    -- Filesystem-level fingerprint of the file.
    fsize INTEGER,
    mtime INTEGER,  -- microseconds
    -- Deep fingerprint of the file.
    hash BLOB,
    -- Encryption groups are random identifiers only shared by identical files.
    egroup BLOB,  

    access_error TEXT
) STRICT, WITHOUT ROWID;
"#,
);

#[derive(Debug, PartialEq)]
pub struct File {
    pub tree_gen: i64,
    pub next_hash: TimeInSeconds,
    pub state: State,
}

#[derive(Debug, PartialEq)]
pub enum State {
    New(New),
    Dirty(Dirty),
    Busy(Busy),
    Clean(Clean),
    Unreadable(Unreadable),
}

#[derive(Debug, PartialEq)]
pub struct New {}

#[derive(Debug, PartialEq)]
pub struct Dirty {
    pub fsize: FileSize,
    pub mtime: TimeInMicroseconds,
    pub hash: Vec<u8>,
    pub egroup: StoredEncryptionGroup,
}

#[derive(Debug, PartialEq)]
pub struct Busy {}

#[derive(Debug, PartialEq)]
pub struct Clean {
    pub fsize: FileSize,
    pub mtime: TimeInMicroseconds,
    pub hash: Vec<u8>,
    pub egroup: StoredEncryptionGroup,
}

#[derive(Debug, PartialEq)]
pub struct Unreadable {
    pub access_error: String,
}

/// Insert a new file.
pub fn new(
    txn: &Transaction,
    path: &LocalPath,
    tree_gen: i64,
    next_hash: &TimeInSeconds,
) -> anyhow::Result<()> {
    txn.execute(
        r#"
            INSERT INTO
            File (path, state, tree_gen, next_hash)
            VALUES (
              :path, :state, :tree_gen, :next_hash
            )
        "#,
        rusqlite::named_params! {
                ":path": path,
                ":state": FileState::New,
                ":tree_gen": tree_gen,
                ":next_hash": next_hash,
        },
    )
    .context("File insert")?;
    Ok(())
}

pub fn next(txn: &Transaction, min_gen: i64) -> anyhow::Result<Option<(LocalPath, TimeInSeconds)>> {
    let result = txn
        .query_row(
            r#"SELECT path, next_hash
            FROM File
            WHERE tree_gen >= :min_gen
            ORDER BY next_hash LIMIT 1"#,
            named_params! {
                ":min_gen": &min_gen,
            },
            |row| {
                let path: LocalPath = row.get(0)?;
                let next: TimeInSeconds = row.get(1)?;
                Ok((path, next))
            },
        )
        .optional()
        .context("Failed to find next file to hash")?;
    Ok(result)
}

pub fn set_state(
    txn: &Transaction,
    path: &LocalPath,
    tree_gen: i64,
    next_hash: TimeInSeconds,
    state: &State,
) -> anyhow::Result<usize> {
    let mut fsize = None;
    let mut mtime = None;
    let mut egroup = None;
    let mut hash = None;
    let mut access_error = None;
    let state = match state {
        State::New(_) => FileState::New,
        State::Dirty(state) => {
            fsize = Some(&state.fsize);
            mtime = Some(&state.mtime);
            egroup = Some(&state.egroup);
            hash = Some(&state.hash);
            FileState::Dirty
        }
        State::Busy(_) => FileState::Busy,
        State::Clean(state) => {
            fsize = Some(&state.fsize);
            mtime = Some(&state.mtime);
            egroup = Some(&state.egroup);
            hash = Some(&state.hash);
            FileState::Clean
        }
        State::Unreadable(state) => {
            access_error = Some(&state.access_error);
            FileState::Unavailable
        }
    };
    let count = txn.execute(
        r#"
        UPDATE File SET
          state = :state,
          tree_gen = :tree_gen,
          next_hash = :next_hash,
          fsize = :fsize,
          mtime = :mtime,
          egroup = :egroup,
          hash = :hash,
          access_error = :access_error
        WHERE path = :path
        "#,
        named_params! {
            ":path": path,
            ":tree_gen": &tree_gen,
            ":next_hash": &next_hash,
            ":state": &state,
            ":fsize": &fsize,
            ":mtime": &mtime,
            ":egroup": &egroup,
            ":hash": &hash,
            ":access_error": &access_error,
        },
    )?;
    Ok(count)
}

pub fn get_state(txn: &Transaction, path: &LocalPath) -> anyhow::Result<Option<File>> {
    txn.query_row(
        r#"
            SELECT
              state, tree_gen, next_hash,
              fsize, mtime, hash, egroup, access_error
            FROM File WHERE path = :path
        "#,
        rusqlite::named_params! {":path": path},
        |row| {
            let state: FileState = row.get(0)?;
            let tree_gen: i64 = row.get(1)?;
            let next_hash: TimeInSeconds = row.get(2)?;
            let fsize: Option<FileSize> = row.get(3)?;
            let mtime: Option<TimeInMicroseconds> = row.get(4)?;
            let hash: Option<Vec<u8>> = row.get(5)?;
            let egroup: Option<StoredEncryptionGroup> = row.get(6)?;
            let access_error: Option<String> = row.get(7)?;
            let state = match state {
                FileState::New => State::New(New {}),
                FileState::Dirty => State::Dirty(Dirty {
                    fsize: fsize.ok_or_else(|| {
                        FromSqlError::Other(anyhow!("missing fsize field").into())
                    })?,
                    mtime: mtime.ok_or_else(|| {
                        FromSqlError::Other(anyhow!("missing mtime field").into())
                    })?,
                    hash: hash
                        .ok_or_else(|| FromSqlError::Other(anyhow!("missing hash field").into()))?,
                    egroup: egroup.ok_or_else(|| {
                        FromSqlError::Other(anyhow!("missing egroup field").into())
                    })?,
                }),
                FileState::Busy => todo!(),
                FileState::Clean => todo!(),
                FileState::Unavailable => State::Unreadable(Unreadable {
                    access_error: access_error.ok_or_else(|| {
                        FromSqlError::Other(anyhow!("missing error field").into())
                    })?,
                }),
            };
            Ok(File {
                tree_gen,
                next_hash,
                state,
            })
        },
    )
    .optional()
    .context("get_state")
}

#[cfg(test)]
pub mod pth {
    use super::{File, get_state};
    use crate::filestore::field::LocalPath;

    use std::path::PathBuf;

    pub fn dump(conn: &mut rusqlite::Connection) -> anyhow::Result<Vec<(PathBuf, File)>> {
        let txn = conn.transaction()?;
        let mut stmt = txn.prepare("SELECT path FROM File ORDER BY path")?;
        let mut result = vec![];
        for path in stmt.query_map((), |row| {
            let path: LocalPath = row.get(0)?;
            Ok(path)
        })? {
            let path = path?;
            let state = get_state(&txn, &path)?.unwrap();
            result.push((path.try_into()?, state));
        }
        Ok(result)
    }
}
