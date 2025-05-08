//! Database of file information.
//!
//! This intentionally does _not_ perform any filesystem operations,
//! to ensure testability and isolation.
use crate::model::{FileSize, ModificationTime};
use anyhow::Context;
use field::{LocalPath, TimeInMicroseconds, TimeInSeconds};
use file::{Dirty, New, State, Unreadable};
use rusqlite_migration::Migrations;
use settings::Setting;
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};
use tracing::instrument;

mod directory;
mod field;
mod file;
mod settings;

#[cfg(test)]
mod tests;

// ----------------- API DEFINITION ------------------------

/// Filestore defines how and when filesystem information is updated and retrieved.
pub trait Filestore {
    type Scanner<'a>: Scanner
    where
        Self: 'a;

    // Define the roots of the filesystem under scrutiny.
    fn set_roots(&mut self, roots: &[&Path]) -> anyhow::Result<bool>;

    // Request a full scan of the filesystem and mark when the next one will be due.
    fn tree_scan_start(&mut self, next_scan: SystemTime) -> anyhow::Result<()>;
    // Get the next directory to analyze, if it's due.
    fn tree_scan_next(&mut self) -> anyhow::Result<ScanNext<Self::Scanner<'_>>>;
    // Get the next file to analyze, if any is due.
    fn hash_next(&mut self, now: SystemTime) -> anyhow::Result<HashNext>;
    // Update a file's hash.
    fn hash_update(
        &mut self,
        file: PathBuf,
        next: SystemTime,
        update: HashUpdate,
    ) -> anyhow::Result<()>;

    // Update the fsize and mtime of a file, typically because a watcher reported a change outside
    // the scope of normal scanning. This can add a file that had not been seen before.
    fn metadata_update(
        &mut self,
        path: PathBuf,
        fsize: FileSize,
        mtime: ModificationTime,
    ) -> anyhow::Result<()>;
}

/// Scanner defines how the content of a specific directory is being updated.
///
/// Error handling is done as follows:
///   - Failing to list the content of a directory altogether is reported using error() below.
///     This prevents the children to be looked up until the next scan.
///   - When a scan has started, it's still possible to fail to obtain the content of some
///     entries, including their type. When that happens, the caller is expected to simply
///     skip those entries, and indicate that the listing was partial when calling commit().
pub trait Scanner {
    /// Call update() repeatedly for all the results in the filesystem.
    /// Any skipped entry will be considered as missing in the next pass.
    fn update(&mut self, update: &ScanUpdate) -> anyhow::Result<()>;
    /// Once done, call commit() to indicate that the directory is processed.
    fn commit(self, complete: bool) -> anyhow::Result<()>;
    /// Alternatively, if the directory cannot be read at all, call error().
    fn error(self, error: std::io::Error) -> anyhow::Result<()>;
}

/// What should be scanned next?
#[derive(Debug, PartialEq)]
pub enum ScanNext<U: Scanner> {
    Next(PathBuf, U), // Specific directory to scan next.
    Done(SystemTime), // Scanning complete.
}

/// What is the next update to the directory?
#[derive(Debug)]
pub enum ScanUpdate {
    File(OsString, FileSize, ModificationTime),
    Directory(OsString),
}

/// What should be hashed next?
#[derive(Debug, PartialEq)]
pub enum HashNext {
    Next(PathBuf),
    Done(SystemTime),
}

/// Represents the file hash update.
#[derive(Debug)]
pub enum HashUpdate {
    Hash(Snapshot),
    Unreadable(std::io::Error),
}

#[derive(Debug)]
pub struct Snapshot {
    pub hash: ring::digest::Digest,
    pub fsize: FileSize,
    pub mtime: ModificationTime,
}

// -----------------------------------------------------------------------

/// Default implementation of the Filestore.
pub struct Connection {
    conn: rusqlite::Connection,
    rand: crypto::SharedRandom,
}

/// Default implementation of the Updater.
pub struct UpdaterImpl<'a> {
    tx: rusqlite::Transaction<'a>,
    dir: LocalPath,
    aim: i64,
}

impl std::fmt::Debug for UpdaterImpl<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Updater")
            .field("dir", &self.dir)
            .field("aim", &self.aim)
            .finish_non_exhaustive()
    }
}

const SETTING_TREE_GEN: &str = "tree-gen";
const SETTING_NEXT_RUN: &str = "next-run";

impl Connection {
    /// Open a connection to the specified database, creating it if missing.
    pub fn new<P: AsRef<Path>>(path: P, rand: crypto::SharedRandom) -> anyhow::Result<Connection> {
        let con = rusqlite::Connection::open(path.as_ref()).context("Failed to open db")?;
        initialize(con, rand)
    }

    fn migrations() -> Migrations<'static> {
        Migrations::new(vec![settings::SQL, directory::SQL, file::SQL])
    }

    /// In-memory database, for testing.
    #[cfg(test)]
    pub fn new_in_memory(rand: crypto::SharedRandom) -> anyhow::Result<Connection> {
        let con = rusqlite::Connection::open_in_memory().context("Failed to open in-memory db")?;
        initialize(con, rand)
    }

    #[cfg(test)]
    fn conn(&mut self) -> &mut rusqlite::Connection {
        &mut self.conn
    }
}

const FILE_NEXT_DELAY: Duration = Duration::from_secs(3600);

impl Filestore for Connection {
    type Scanner<'a> = UpdaterImpl<'a>;

    /// Set the roots to the provided list. This supersedes the roots completely, which
    /// can impact which files will be backed up in the future.
    fn set_roots(&mut self, roots: &[&Path]) -> anyhow::Result<bool> {
        let mut changed = false; // tracks if any of the roots changed.
        let mut txn = self.conn.transaction()?;
        let mut old_roots = directory::get_roots(&txn).context("Failed to get current roots")?;
        for root in roots {
            if old_roots.contains_key(*root) {
                // The directory exists and is already a root. Just ensure we don't delete
                // it at the end.
                old_roots.remove(*root);
                continue;
            }
            changed = true;
            // The directory is not a root, but might exist nevertheless.
            let root: LocalPath = (*root).to_owned().into();
            let count = directory::update_root(&mut txn, &root, true)?;
            if count == 0 {
                directory::add_root(&txn, &root)?;
            }
        }
        for old_root in old_roots.into_values() {
            changed = true;
            // We turn the old roots into regular directories. Their cleanup is done as
            // part of a scan, not now.
            directory::update_root(&mut txn, &old_root.path, false)?;
        }
        txn.commit()?;
        Ok(changed)
    }

    /// Configure a new tree scan, and indicate the earliest time the next one should take
    /// place.
    fn tree_scan_start(&mut self, next_scan: SystemTime) -> anyhow::Result<()> {
        let txn = self.conn.transaction()?;
        // Begin by aiming the new scan at the roots.
        let aim = settings::get_int(&txn, SETTING_TREE_GEN)?.unwrap_or(0) + 1;
        settings::set(&txn, SETTING_TREE_GEN, &Setting::N(aim))?;
        settings::set(&txn, SETTING_NEXT_RUN, &Setting::Ts(next_scan))?;
        directory::set_root_aim(&txn, aim)?;
        txn.commit()?;
        Ok(())
    }

    /// When scanning the whole world, this returns the next directory to scan that is not
    /// at the expected generation. Note that the same directory will be returned until
    /// update_dir() is called. In particular, a directory that is now inaccessible must be
    /// flagged as such. When all is done for generation gen, it returns None.
    ///
    /// This is a top-down analysis of the current structure. It will leave orphans at a lower
    /// generation. Why not simply walk the filestructure directly? Driving it here ensures that
    /// we do monotonic progress, even for very large trees that could be done across several
    /// runs.
    #[instrument(skip(self))]
    fn tree_scan_next(&mut self) -> anyhow::Result<ScanNext<UpdaterImpl>> {
        let txn = self.conn.transaction()?;
        let aim = settings::get_int(&txn, SETTING_TREE_GEN)?.unwrap_or(0);
        let next =
            settings::get_timestamp(&txn, SETTING_NEXT_RUN)?.unwrap_or(SystemTime::UNIX_EPOCH);
        let result = directory::next(&txn, aim)?;
        txn.commit()?;

        let candidate = match result {
            Some(dir) => ScanNext::Next(
                dir.clone().try_into()?,
                UpdaterImpl {
                    tx: self.conn.transaction()?,
                    dir,
                    aim,
                },
            ),
            None => ScanNext::Done(next),
        };
        Ok(candidate)
    }

    fn hash_next(&mut self, now: SystemTime) -> anyhow::Result<HashNext> {
        let txn = self.conn.transaction()?;
        let aim = settings::get_int(&txn, SETTING_TREE_GEN)?.unwrap_or(0);
        // We only hash files that were found during a recent scan: deleted files will remain in the
        // database for longer, but there is no point in finding they disappeared over and over again.
        // This does not apply to the watcher: if a file reappears, it'll be hashed right away, just
        // not from this code path.
        let result = file::next(&txn, aim - 1)?;
        txn.commit()?;
        let candidate = match result {
            Some((path, next)) => {
                if *next <= now {
                    HashNext::Next(path.try_into()?)
                } else {
                    HashNext::Done(*next)
                }
            }
            // If there is no file to wait for, wait for some arbitrary time.
            // We just want to avoid spinning, this will become an actual value
            // as soon as the scanner has something to say.
            None => HashNext::Done(now + FILE_NEXT_DELAY),
        };
        Ok(candidate)
    }

    fn hash_update(
        &mut self,
        file: PathBuf,
        next: SystemTime,
        update: HashUpdate,
    ) -> anyhow::Result<()> {
        let file_repr: LocalPath = file.into();
        let tx = self.conn.transaction()?;
        let next: TimeInSeconds = next.into();
        // Hash updates can only happen to files we already know about (rather than being
        // picked up by the file watcher), so failure to update the record is an internal
        // inconsistency.
        let Some(current) = file::get_state(&tx, &file_repr)? else {
            anyhow::bail!("file [{:?}] is expected to be in db", &file_repr);
        };
        match update {
            HashUpdate::Hash(snapshot) => {
                match current.state {
                    // Either NEW or UNREADABLE deserve a shot at a backup once we have
                    // enough information about them, in particular their egroup.
                    State::New(_) | State::Unreadable(_) => {
                        file::set_state(
                            &tx,
                            &file_repr,
                            current.tree_gen,
                            next,
                            &State::Dirty(Dirty {
                                fsize: snapshot.fsize,
                                mtime: snapshot.mtime.into(),
                                hash: snapshot.hash.as_ref().to_owned(),
                                // TODO: we probably want to reuse the same egroup when
                                // going from UNREADABLE back to readable?
                                egroup: self.rand.generate_file_id()?.into(),
                            }),
                        )?;
                    }
                    // The additional information does not cause any change, we already
                    // need to back the file up.
                    State::Dirty(_) | State::Busy(_) => {}
                    // This is the only conditional case: if the information has not changed,
                    // the file is still CLEAN.
                    State::Clean(old) => {
                        if snapshot.hash.as_ref() != old.hash
                            || snapshot.mtime != *old.mtime
                            || snapshot.fsize != old.fsize
                        {
                            file::set_state(
                                &tx,
                                &file_repr,
                                current.tree_gen,
                                next,
                                &State::Dirty(Dirty {
                                    fsize: snapshot.fsize,
                                    mtime: snapshot.mtime.into(),
                                    hash: snapshot.hash.as_ref().to_owned(),
                                    egroup: old.egroup,
                                }),
                            )?;
                        }
                    }
                };
            }
            HashUpdate::Unreadable(error) => {
                file::set_state(
                    &tx,
                    &file_repr,
                    current.tree_gen,
                    next,
                    &State::Unreadable(Unreadable {
                        access_error: format!("{:?}", error),
                    }),
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn metadata_update(
        &mut self,
        path: PathBuf,
        fsize: FileSize,
        mtime: ModificationTime,
    ) -> anyhow::Result<()> {
        let file_repr: LocalPath = path.into();
        let mtime: TimeInMicroseconds = mtime.into();
        let mut tx = self.conn.transaction()?;
        metadata_update(&mut tx, &file_repr, None, fsize, mtime)?;
        Ok(())
    }
}

impl Scanner for UpdaterImpl<'_> {
    fn update(&mut self, update: &ScanUpdate) -> anyhow::Result<()> {
        let dir: PathBuf = self.dir.clone().try_into()?;
        match update {
            ScanUpdate::Directory(subdir) => {
                let path: LocalPath = dir.join(subdir).into();
                let count = directory::set_path_aim(&self.tx, self.aim, &path)?;
                if count == 0 {
                    directory::insert(&self.tx, &path, self.aim)?;
                }
            }
            ScanUpdate::File(file, fsize, mtime) => {
                let path: LocalPath = dir.join(file).into();
                let mtime: TimeInMicroseconds = (*mtime).into();
                metadata_update(&mut self.tx, &path, Some(self.aim), *fsize, mtime)?;
            }
        };
        Ok(())
    }

    fn commit(self, complete: bool) -> anyhow::Result<()> {
        directory::update_dir_complete(&self.tx, &self.dir, self.aim, complete)?;
        self.tx.commit()?;
        Ok(())
    }

    fn error(self, error: std::io::Error) -> anyhow::Result<()> {
        directory::update_dir_error(&self.tx, &self.dir, self.aim, format!("{:?}", error))?;
        self.tx.commit()?;
        Ok(())
    }
}

fn metadata_update(
    tx: &mut rusqlite::Transaction,
    path: &LocalPath,
    tree_gen: Option<i64>,
    fsize: FileSize,
    mtime: TimeInMicroseconds,
) -> anyhow::Result<()> {
    let Some(state) = file::get_state(tx, &path)? else {
        file::new(
            tx,
            &path,
            tree_gen.unwrap_or(0),
            // TODO: order the new files by detection time.
            &SystemTime::UNIX_EPOCH.into(),
        )?;
        return Ok(());
    };
    let tree_gen = tree_gen.unwrap_or(state.tree_gen);
    let mtime: TimeInMicroseconds = (*mtime).into();
    match state.state {
        // We can't leave NEW until we have a hash and egroup. But we should
        // still update the tree_gen to show that the file is still around.
        State::New(_) | State::Dirty(_) | State::Busy(_) => {
            file::set_state(tx, &path, tree_gen, state.next_hash, &state.state)?;
        }
        // We might have enough information to know if the file is dirty again.
        State::Clean(clean) => {
            if clean.mtime != mtime || clean.fsize != fsize {
                file::set_state(
                    tx,
                    &path,
                    tree_gen,
                    state.next_hash,
                    &State::Dirty(Dirty {
                        fsize,
                        mtime,
                        hash: clean.hash,
                        egroup: clean.egroup,
                    }),
                )?;
            }
        }
        // The file may have been readable in the past. For simplicity, treat it as
        // if it was new so we can pick the correct egroup and force a backup.
        State::Unreadable(_) => {
            file::set_state(tx, &path, tree_gen, state.next_hash, &State::New(New {}))?;
        }
    };
    Ok(())
}

fn initialize(
    mut con: rusqlite::Connection,
    rand: crypto::SharedRandom,
) -> anyhow::Result<Connection> {
    con.pragma_update(None, "foreign_keys", "ON")
        .context("Failed to enable foreign keys")?;

    Connection::migrations()
        .to_latest(&mut con)
        .context("Failed to migrate database")?;
    Ok(Connection { conn: con, rand })
}
