use super::{
    Snapshot,
    field::{FileState, StoredEncryptionGroup, StoredFileHash, TimeInMicroseconds, TimeInSeconds},
};
use crate::{model::FileSize, sql_model::LocalPath};
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

    -- Next time the file will be processed, in seconds since epoch.
    -- The field is used by different states for different processing.
    next INTEGER,

    -- Worst case for the next stage of processing of a file, in seconds since epoch.
    -- Also used by different states for different processing.
    threshold INTEGER,

    -- Filesystem-level fingerprint of the file.
    fsize INTEGER,
    mtime INTEGER,  -- microseconds
    -- Deep fingerprint of the file.
    hash BLOB,
    -- Encryption groups are random identifiers only shared by identical files.
    egroup BLOB,  

    access_error TEXT
) STRICT, WITHOUT ROWID;

CREATE INDEX FileHash ON File(hash);
"#,
);

#[derive(Debug, PartialEq)]
pub struct File {
    pub tree_gen: i64,
    pub state: State,
}

#[derive(Debug, PartialEq, Clone)]
pub enum State {
    New(New),
    Dirty(Dirty),
    Busy(Busy),
    Clean(Clean),
    Unreadable(Unreadable),
}

// New file. Transitions to Dirty or Unreadable after hashing.
#[derive(Debug, PartialEq, Clone)]
pub struct New {
    // When the file should be hashed. This is done right away even if
    // the file is active, so that we can infer duplication and encryption
    // group before any additional modification.
    pub next: TimeInSeconds,
}

// Dirty file. Transitions to Busy when being backed up.
#[derive(Debug, PartialEq, Clone)]
pub struct Dirty {
    // Earliest time the file should be backed up.
    pub next: TimeInSeconds,
    // Latest time a changing file can be postponed for backup.
    pub threshold: TimeInSeconds,
    // Current snapshot of the file.
    // TODO: do we want better types here to have the shallow and deep state
    // explicitly spelled out?
    pub fsize: FileSize,
    pub mtime: TimeInMicroseconds,
    pub hash: StoredFileHash,
    // How should the file be encrypted.
    pub egroup: StoredEncryptionGroup,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Busy {
    pub egroup: StoredEncryptionGroup,
}

// Clean file. Transitions to Dirty or Unreadable after hashing or
// scanning.
#[derive(Debug, PartialEq, Clone)]
pub struct Clean {
    // When the file should be hashed.
    pub next: TimeInSeconds,
    // Earliest time the file could be backed up again.
    pub threshold: TimeInSeconds,
    // Snapshot of the file at the time it was last backed up.
    pub fsize: FileSize,
    pub mtime: TimeInMicroseconds,
    pub hash: StoredFileHash,
    // How should the file be encrypted.
    pub egroup: StoredEncryptionGroup,
}

// Unreadable file. Transitions to New after hashing.
#[derive(Debug, PartialEq, Clone)]
pub struct Unreadable {
    // When the file should be hashed / attempted to be read.
    pub next: TimeInSeconds,
    pub access_error: String,
}

/// Insert a new file.
pub fn new(
    txn: &Transaction,
    path: &LocalPath,
    tree_gen: i64,
    next: &TimeInSeconds,
) -> anyhow::Result<()> {
    txn.execute(
        r#"
            INSERT INTO
            File (path, state, tree_gen, next)
            VALUES (
              :path, :state, :tree_gen, :next
            )
        "#,
        rusqlite::named_params! {
                ":path": path,
                ":state": FileState::New,
                ":tree_gen": tree_gen,
                ":next": next,
        },
    )
    .context("File insert")?;
    Ok(())
}

pub fn hash_next(
    txn: &Transaction,
    min_gen: i64,
) -> anyhow::Result<Option<(LocalPath, TimeInSeconds)>> {
    let result = txn
        .query_row(
            r#"SELECT path, next
            FROM File
            WHERE tree_gen >= :min_gen AND state IN (:new, :clean, :unreadable)
            ORDER BY next LIMIT 1"#,
            named_params! {
                ":min_gen": &min_gen,
                ":new": FileState::New,
                ":clean": FileState::Clean,
                ":unreadable": FileState::Unreadable,
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

pub fn backup_next(
    txn: &Transaction,
    min_gen: i64,
) -> anyhow::Result<Option<(LocalPath, TimeInSeconds, StoredEncryptionGroup)>> {
    let result = txn
        .query_row(
            r#"SELECT path, next, egroup
            FROM File
            WHERE tree_gen >= :min_gen AND state = :dirty
            ORDER BY next LIMIT 1"#,
            named_params! {
                ":min_gen": &min_gen,
                ":dirty": FileState::Dirty,
            },
            |row| {
                let path: LocalPath = row.get(0)?;
                let next: TimeInSeconds = row.get(1)?;
                let egroup: StoredEncryptionGroup = row.get(2)?;
                Ok((path, next, egroup))
            },
        )
        .optional()
        .context("Failed to find next file to hash")?;
    Ok(result)
}

pub fn backup_pending(
    txn: &Transaction,
) -> anyhow::Result<Vec<(LocalPath, StoredEncryptionGroup)>> {
    let mut stmt = txn.prepare(
        r#"
            SELECT path, egroup FROM File WHERE state = :busy
        "#,
    )?;
    let pending: rusqlite::Result<Vec<(LocalPath, StoredEncryptionGroup)>> = stmt
        .query_map(named_params! {":busy": FileState::Busy}, |row| {
            let path: LocalPath = row.get(0)?;
            let egroup: StoredEncryptionGroup = row.get(1)?;
            Ok((path, egroup))
        })?
        .collect();
    Ok(pending?)
}

pub fn set_state(
    txn: &Transaction,
    path: &LocalPath,
    tree_gen: i64,
    state: &State,
) -> anyhow::Result<usize> {
    let mut next = None;
    let mut threshold = None;
    let mut fsize = None;
    let mut mtime = None;
    let mut egroup = None;
    let mut hash = None;
    let mut access_error = None;
    let state = match state {
        State::New(state) => {
            next = Some(&state.next);
            FileState::New
        }
        State::Dirty(state) => {
            next = Some(&state.next);
            threshold = Some(&state.threshold);
            fsize = Some(&state.fsize);
            mtime = Some(&state.mtime);
            egroup = Some(&state.egroup);
            hash = Some(&state.hash);
            FileState::Dirty
        }
        State::Busy(busy) => {
            egroup = Some(&busy.egroup);
            FileState::Busy
        }
        State::Clean(state) => {
            next = Some(&state.next);
            threshold = Some(&state.threshold);
            fsize = Some(&state.fsize);
            mtime = Some(&state.mtime);
            egroup = Some(&state.egroup);
            hash = Some(&state.hash);
            FileState::Clean
        }
        State::Unreadable(state) => {
            next = Some(&state.next);
            access_error = Some(&state.access_error);
            FileState::Unreadable
        }
    };
    let count = txn.execute(
        r#"
        UPDATE File SET
          state = :state,
          tree_gen = :tree_gen,
          next = :next,
          threshold = :threshold,
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
            ":next": &next,
            ":threshold": &threshold,
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
              state, tree_gen, next, threshold,
              fsize, mtime, hash, egroup, access_error
            FROM File WHERE path = :path
        "#,
        rusqlite::named_params! {":path": path},
        |row| {
            let state: FileState = row.get(0)?;
            let tree_gen: i64 = row.get(1)?;
            let next: Option<TimeInSeconds> = row.get(2)?;
            let threshold: Option<TimeInSeconds> = row.get(3)?;
            let fsize: Option<FileSize> = row.get(4)?;
            let mtime: Option<TimeInMicroseconds> = row.get(5)?;
            let hash: Option<StoredFileHash> = row.get(6)?;
            let egroup: Option<StoredEncryptionGroup> = row.get(7)?;
            let access_error: Option<String> = row.get(8)?;
            let state = match state {
                FileState::New => State::New(New {
                    next: next
                        .ok_or_else(|| FromSqlError::Other(anyhow!("missing next field").into()))?,
                }),
                FileState::Dirty => State::Dirty(Dirty {
                    next: next
                        .ok_or_else(|| FromSqlError::Other(anyhow!("missing next field").into()))?,
                    threshold: threshold.ok_or_else(|| {
                        FromSqlError::Other(anyhow!("missing threshold field").into())
                    })?,
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
                FileState::Busy => State::Busy(Busy {
                    egroup: egroup.ok_or_else(|| {
                        FromSqlError::Other(anyhow!("missing egroup field").into())
                    })?,
                }),
                FileState::Clean => State::Clean(Clean {
                    next: next
                        .ok_or_else(|| FromSqlError::Other(anyhow!("missing next field").into()))?,
                    threshold: threshold.ok_or_else(|| {
                        FromSqlError::Other(anyhow!("missing threshold field").into())
                    })?,
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
                FileState::Unreadable => State::Unreadable(Unreadable {
                    next: next
                        .ok_or_else(|| FromSqlError::Other(anyhow!("missing next field").into()))?,
                    access_error: access_error.ok_or_else(|| {
                        FromSqlError::Other(anyhow!("missing error field").into())
                    })?,
                }),
            };
            Ok(File { tree_gen, state })
        },
    )
    .optional()
    .context("get_state")
}

pub fn matching_egroup(
    txn: &Transaction,
    snapshot: &Snapshot,
) -> anyhow::Result<Option<StoredEncryptionGroup>> {
    let hash: StoredFileHash = snapshot.hash.clone().into();
    txn.query_row(
        r#"
            SELECT
              egroup
            FROM File WHERE hash = :hash AND fsize = :fsize
            LIMIT 1
        "#,
        rusqlite::named_params! {":fsize": snapshot.fsize, ":hash": hash},
        |row| {
            let egroup: StoredEncryptionGroup = row.get(0)?;
            Ok(egroup)
        },
    )
    .optional()
    .context("matching_egroup")
}

#[cfg(test)]
pub mod pth {
    use super::{File, get_state};
    use crate::sql_model::LocalPath;
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
