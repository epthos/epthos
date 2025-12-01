//! Database of file information.
//!
//! This intentionally does _not_ perform any filesystem operations,
//! to ensure testability and isolation.
use crate::{
    disk::{DiskError, ScanEntry, Snapshot},
    model::{FileSize, ModificationTime, Stats},
    sql_model::LocalPath,
};
use anyhow::{Context, bail};
use crypto::{SharedRandom, model::EncryptionGroup};
use field::{StoredEncryptionGroup, StoredFileHash, TimeInMicroseconds};
use file::{Busy, Clean, Dirty, New, State, Unreadable};
use rand::{Rng, TryRngCore, rngs::OsRng};
use rusqlite::Transaction;
use rusqlite_migration::Migrations;
use settings::Setting;
use std::{
    cmp::{max, min},
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

#[derive(Debug, Clone)]
pub struct Timing {
    // How often we want to scan the filesystem roots. This is reasonably
    // cheap and catches changes that happened when the service was down or
    // if the file watcher misses them.
    scan_period: Duration,
    // How often are files hashed for unexpected changes. This covers changes
    // that don't impact a file's metadata (mtime, size). It's more expensive.
    hash_period: Duration,
    // How frequently can a given file be backed up if it changes frequently.
    min_backup_period: Duration,
    // How long to wait for a file that is actively being modified, so that
    // it's not backed up mid-work. This is a range, as we want to wait a
    // minimum amount of time, but if the file does not seems to stabilize we
    // might still need to take a stab at it. This applies after min_backup_period
    // has passed.
    cool_off_period: (Duration, Duration),
    // Fraction by which the scan and hash periods are spread, to slowly diffuse the
    // work.
    spread: f64,
}

impl Default for Timing {
    fn default() -> Self {
        Timing {
            scan_period: Duration::from_secs(86400),     // Daily
            hash_period: Duration::from_secs(86400 * 7), // Weekly
            // No more than one backup per hour for active files.
            min_backup_period: Duration::from_secs(3600),
            // Watch a modified file for at least 5 minutes and at most 30 to wait for
            // it to stabilize before backing it up.
            cool_off_period: (Duration::from_secs(300), Duration::from_secs(1800)),
            spread: 0.1,
        }
    }
}

impl Timing {
    fn scan_period(&self) -> Duration {
        self.spread(self.scan_period)
    }
    fn hash_period(&self) -> Duration {
        self.spread(self.hash_period)
    }
    /// Spreads a duration around an initial value. Useful for coordinated work, to
    /// avoid synchronous events.
    fn spread(&self, d: Duration) -> Duration {
        if self.spread > 0.0 {
            // OsRng is fine here, we don't do crypto with the result.
            d.mul_f64(1f64 + OsRng.unwrap_err().random_range(-self.spread..self.spread))
        } else {
            d
        }
    }
}

/// Filestore defines how and when filesystem information is updated and retrieved.
pub trait Filestore {
    type Scanner<'a>: Scanner
    where
        Self: 'a;

    /// Define the roots of the filesystem under scrutiny.
    fn set_roots(&mut self, roots: &[&Path]) -> anyhow::Result<bool>;

    /// Request a full scan of the filesystem and mark when the next one will be due.
    fn tree_scan_start(&mut self, next: SystemTime) -> anyhow::Result<()>;
    /// Get the next directory to analyze, if it's due.
    fn tree_scan_next(&mut self) -> anyhow::Result<Next<Self::Scanner<'_>>>;
    /// Get the next file to analyze, if any is due.
    fn hash_next(&mut self, now: SystemTime) -> anyhow::Result<Next<()>>;
    /// Update a file's hash. If the file was not altered, we can check it again after |next|.
    /// If it's altered, the file should be backed up after |soon|.
    fn hash_update(
        &mut self,
        file: PathBuf,
        now: SystemTime,
        update: HashUpdate,
    ) -> anyhow::Result<()>;

    /// Update the fsize and mtime of a file, typically because a watcher reported a change outside
    /// the scope of normal scanning. This can add a file that had not been seen before.
    fn metadata_update(
        &mut self,
        path: PathBuf,
        now: SystemTime,
        fsize: FileSize,
        mtime: ModificationTime,
    ) -> anyhow::Result<()>;

    /// Get the next file to back up, if any is due.
    fn backup_next(&mut self, now: SystemTime) -> anyhow::Result<Next<EncryptionGroup>>;
    /// Record a file as being backed up at this time.
    fn backup_start(&mut self, path: PathBuf) -> anyhow::Result<()>;
    /// Record a file's backup as having completed, with the provided |update| as the
    /// state as of the backup. |next| indicates the next time it should be hashed
    /// again.
    fn backup_done(
        &mut self,
        path: PathBuf,
        now: SystemTime,
        update: HashUpdate,
    ) -> anyhow::Result<()>;
    /// Return all the pending backups. Typically used to ensure the backup engine
    /// has the right state.
    fn backup_pending(&mut self) -> anyhow::Result<Vec<(PathBuf, EncryptionGroup)>>;

    // Extract stats about the files.
    fn get_stats(&mut self) -> anyhow::Result<Stats>;
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
    fn update(&mut self, now: SystemTime, update: &ScanEntry) -> anyhow::Result<()>;
    /// Once done, call commit() to indicate that the directory is processed.
    fn commit(self, complete: bool) -> anyhow::Result<()>;
    /// Alternatively, if the directory cannot be read at all, call error().
    fn error(self, error: anyhow::Error) -> anyhow::Result<()>;
}

/// Next helps with the common pattern of returning elements based on timing.
/// Either an element is avaialble now, or we know when to check next.
#[derive(Debug, PartialEq)]
pub enum Next<Extra> {
    Next(PathBuf, Extra),
    Done(SystemTime),
}

/// Represents the file hash update.
#[derive(Debug)]
pub enum HashUpdate {
    Hash(Snapshot),
    Unreadable(DiskError),
}

impl<Extra> Next<Extra> {
    #[allow(dead_code)] // Used by tests at least.
    pub fn next(self) -> anyhow::Result<(PathBuf, Extra)> {
        match self {
            Next::Done(_) => bail!("no next element"),
            Next::Next(path, extra) => Ok((path, extra)),
        }
    }
}
// -----------------------------------------------------------------------

/// Default implementation of the Filestore.
pub struct Connection {
    conn: rusqlite::Connection,
    rand: crypto::SharedRandom,
    timing: Timing,
}

/// Default implementation of the Updater.
pub struct UpdaterImpl<'a> {
    tx: rusqlite::Transaction<'a>,
    dir: LocalPath,
    aim: i64,
    timing: Timing,
}

impl std::fmt::Debug for UpdaterImpl<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Updater")
            .field("dir", &self.dir)
            .field("aim", &self.aim)
            .finish_non_exhaustive()
    }
}

const SMALLEST_INDEPENDENT_FILE: u64 = 1024;

const SETTING_TREE_GEN: &str = "tree-gen";
const SETTING_NEXT_RUN: &str = "next-run";

impl Connection {
    /// Open a connection to the specified database, creating it if missing.
    pub fn new<P: AsRef<Path>>(
        path: P,
        rand: crypto::SharedRandom,
        timing: Timing,
    ) -> anyhow::Result<Connection> {
        let con = rusqlite::Connection::open(path.as_ref()).context("Failed to open db")?;
        initialize(con, rand, timing)
    }

    fn migrations() -> Migrations<'static> {
        Migrations::new(vec![settings::SQL, directory::SQL, file::SQL])
    }

    /// In-memory database, for testing.
    #[cfg(test)]
    pub fn new_in_memory(rand: crypto::SharedRandom, timing: Timing) -> anyhow::Result<Connection> {
        let con = rusqlite::Connection::open_in_memory().context("Failed to open in-memory db")?;
        initialize(con, rand, timing)
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
        let txn = self.conn.transaction()?;
        let mut old_roots = directory::get_roots(&txn).context("Failed to get current roots")?;
        for root in roots {
            let root: LocalPath = (*root).to_owned().into();
            if old_roots.contains(&root) {
                // The directory exists and is already a root. Just ensure we don't delete
                // it at the end.
                old_roots.remove(&root);
                continue;
            }
            changed = true;
            // The directory is not a root, but might exist nevertheless.
            let count = directory::update_root(&txn, &root, true)?;
            if count == 0 {
                directory::add_root(&txn, &root)?;
            }
        }
        for old_root in old_roots {
            changed = true;
            // We turn the old roots into regular directories. Their cleanup is done as
            // part of a scan, not now.
            directory::update_root(&txn, &old_root, false)?;
        }
        txn.commit()?;
        Ok(changed)
    }

    /// Configure a new tree scan, and indicate the earliest time the next one should take
    /// place.
    fn tree_scan_start(&mut self, now: SystemTime) -> anyhow::Result<()> {
        let next_scan = now + self.timing.scan_period();
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
    fn tree_scan_next(&mut self) -> anyhow::Result<Next<UpdaterImpl<'_>>> {
        let txn = self.conn.transaction()?;
        let aim = settings::get_int(&txn, SETTING_TREE_GEN)?.unwrap_or(0);
        let next =
            settings::get_timestamp(&txn, SETTING_NEXT_RUN)?.unwrap_or(SystemTime::UNIX_EPOCH);
        let result = directory::next(&txn, aim)?;
        txn.commit()?;

        let candidate = match result {
            Some(dir) => Next::Next(
                dir.clone().try_into()?,
                UpdaterImpl {
                    tx: self.conn.transaction()?,
                    dir,
                    aim,
                    timing: self.timing.clone(),
                },
            ),
            None => Next::Done(next),
        };
        Ok(candidate)
    }

    fn hash_next(&mut self, now: SystemTime) -> anyhow::Result<Next<()>> {
        let txn = self.conn.transaction()?;
        let aim = settings::get_int(&txn, SETTING_TREE_GEN)?.unwrap_or(0);
        // We only hash files that were found during a recent scan: deleted files will remain in the
        // database for longer, but there is no point in finding they disappeared over and over again.
        // This does not apply to the watcher: if a file reappears, it'll be hashed right away, just
        // not from this code path.
        let result = file::hash_next(&txn, aim - 1)?;
        txn.commit()?;
        let candidate = match result {
            Some((path, next)) => {
                let next = next.into_inner();
                if next <= now {
                    Next::Next(path.try_into()?, ())
                } else {
                    Next::Done(next)
                }
            }
            // If there is no file to wait for, wait for some arbitrary time.
            // We just want to avoid spinning, this will become an actual value
            // as soon as the scanner has something to say.
            None => Next::Done(now + FILE_NEXT_DELAY),
        };
        Ok(candidate)
    }

    fn hash_update(
        &mut self,
        file: PathBuf,
        now: SystemTime,
        update: HashUpdate,
    ) -> anyhow::Result<()> {
        let file_repr: LocalPath = file.into();
        let tx = self.conn.transaction()?;
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
                        let egroup = pick_egroup(&tx, &snapshot, self.rand.clone())?;
                        file::set_state(
                            &tx,
                            &file_repr,
                            current.tree_gen,
                            &State::Dirty(Dirty {
                                next: (now + self.timing.cool_off_period.0).into(),
                                // In such cases, there is no reason to delay the backup, as no previous one took place.
                                threshold: (now + self.timing.cool_off_period.1).into(),
                                fsize: snapshot.fsize,
                                mtime: snapshot.mtime.into(),
                                hash: snapshot.hash.into(),
                                // TODO: we probably want to reuse the same egroup when
                                // going from UNREADABLE back to readable?
                                egroup,
                            }),
                        )?;
                    }
                    // The additional information does not cause any change, we already
                    // need to back the file up. In principle we should not even request
                    // hashing, so this is worth pointing out.
                    State::Dirty(_) | State::Busy(_) => {
                        tracing::warn!("unexpected hashing of {:?}", &current);
                    }
                    // This is the only conditional case: if the information has not changed,
                    // the file is still CLEAN.
                    State::Clean(mut old) => {
                        let mtime: TimeInMicroseconds = snapshot.mtime.into();
                        let hash: StoredFileHash = snapshot.hash.into();
                        let next_state = if hash != old.hash
                            || mtime != old.mtime
                            || snapshot.fsize != old.fsize
                        {
                            // Clean.threshold represents the earlier we can back up the file.
                            let earliest_backup = max(
                                old.threshold.into_inner(),
                                now + self.timing.cool_off_period.0,
                            );
                            let latest_backup = earliest_backup + self.timing.cool_off_period.1;
                            State::Dirty(Dirty {
                                next: earliest_backup.into(),
                                threshold: latest_backup.into(),
                                fsize: snapshot.fsize,
                                mtime,
                                hash,
                                egroup: old.egroup,
                            })
                        } else {
                            // Get ready to hash the file again in the future.
                            old.next = (now + self.timing.hash_period()).into();
                            State::Clean(old)
                        };
                        file::set_state(&tx, &file_repr, current.tree_gen, &next_state)?;
                    }
                };
            }
            HashUpdate::Unreadable(error) => {
                let hash_next = now + self.timing.hash_period;
                file::set_state(
                    &tx,
                    &file_repr,
                    current.tree_gen,
                    &State::Unreadable(Unreadable {
                        next: hash_next.into(),
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
        next: SystemTime,
        fsize: FileSize,
        mtime: ModificationTime,
    ) -> anyhow::Result<()> {
        let file_repr: LocalPath = path.into();
        let mtime: TimeInMicroseconds = mtime.into();
        let mut tx = self.conn.transaction()?;
        metadata_update(&mut tx, &file_repr, next, None, fsize, mtime, &self.timing)?;
        tx.commit()?;
        Ok(())
    }

    fn backup_next(&mut self, now: SystemTime) -> anyhow::Result<Next<EncryptionGroup>> {
        let txn = self.conn.transaction()?;
        let aim = settings::get_int(&txn, SETTING_TREE_GEN)?.unwrap_or(0);
        let res = match file::backup_next(&txn, aim)? {
            Some((path, next, egroup)) => {
                let next = next.into_inner();
                if next <= now {
                    Next::Next(path.try_into()?, egroup.into_inner())
                } else {
                    Next::Done(next)
                }
            }
            None => Next::Done(now + FILE_NEXT_DELAY),
        };
        Ok(res)
    }

    fn backup_start(&mut self, path: PathBuf) -> anyhow::Result<()> {
        let path: LocalPath = path.into();
        let txn = self.conn.transaction()?;
        let file = file::get_state(&txn, &path)?.context("missing file")?;
        if let State::Dirty(dirty) = file.state {
            file::set_state(
                &txn,
                &path,
                file.tree_gen,
                &State::Busy(Busy {
                    egroup: dirty.egroup,
                }),
            )?;
        } else {
            anyhow::bail!("unexpected state for {:?}", &file);
        }
        txn.commit()?;
        Ok(())
    }

    fn backup_done(
        &mut self,
        path: PathBuf,
        now: SystemTime,
        update: HashUpdate,
    ) -> anyhow::Result<()> {
        let path: LocalPath = path.into();
        let txn = self.conn.transaction()?;
        let file = file::get_state(&txn, &path)?.context("missing file")?;
        if let State::Busy(busy) = file.state {
            let hash_next = now + self.timing.hash_period;
            let new_state = match update {
                HashUpdate::Hash(snapshot) => {
                    let earliest_backup = now + self.timing.min_backup_period;
                    State::Clean(Clean {
                        next: hash_next.into(),
                        threshold: earliest_backup.into(),
                        fsize: snapshot.fsize,
                        mtime: snapshot.mtime.into(),
                        hash: snapshot.hash.into(),
                        egroup: busy.egroup,
                    })
                }
                HashUpdate::Unreadable(error) => State::Unreadable(Unreadable {
                    next: hash_next.into(),
                    access_error: format!("{:?}", error),
                }),
            };
            file::set_state(&txn, &path, file.tree_gen, &new_state)?;
        } else {
            anyhow::bail!("unexpected state for {:?}", &file);
        }
        txn.commit()?;
        Ok(())
    }

    fn backup_pending(&mut self) -> anyhow::Result<Vec<(PathBuf, EncryptionGroup)>> {
        let txn = self.conn.transaction()?;
        file::backup_pending(&txn)?
            .into_iter()
            .map(
                |(path, egroup)| -> anyhow::Result<(PathBuf, EncryptionGroup)> {
                    Ok((path.try_into()?, egroup.into_inner()))
                },
            )
            .collect()
    }

    fn get_stats(&mut self) -> anyhow::Result<Stats> {
        let txn = self.conn.transaction()?;
        file::stats(&txn)
    }
}

impl Scanner for UpdaterImpl<'_> {
    fn update(&mut self, now: SystemTime, update: &ScanEntry) -> anyhow::Result<()> {
        let dir: PathBuf = self.dir.clone().try_into()?;
        match update {
            ScanEntry::Directory(subdir) => {
                let path: LocalPath = dir.join(subdir).into();
                let count = directory::set_path_aim(&self.tx, self.aim, &path)?;
                if count == 0 {
                    directory::insert(&self.tx, &path, self.aim)?;
                }
            }
            ScanEntry::File(file, fsize, mtime) => {
                let path: LocalPath = dir.join(file).into();
                let mtime: TimeInMicroseconds = (*mtime).into();
                metadata_update(
                    &mut self.tx,
                    &path,
                    now,
                    Some(self.aim),
                    *fsize,
                    mtime,
                    &self.timing,
                )?;
            }
        };
        Ok(())
    }

    fn commit(self, complete: bool) -> anyhow::Result<()> {
        directory::update_dir_complete(&self.tx, &self.dir, self.aim, complete)?;
        self.tx.commit()?;
        Ok(())
    }

    fn error(self, error: anyhow::Error) -> anyhow::Result<()> {
        directory::update_dir_error(&self.tx, &self.dir, self.aim, format!("{:?}", error))?;
        self.tx.commit()?;
        Ok(())
    }
}

fn pick_egroup(
    tx: &Transaction,
    snapshot: &Snapshot,
    rand: SharedRandom,
) -> anyhow::Result<StoredEncryptionGroup> {
    if snapshot.fsize < SMALLEST_INDEPENDENT_FILE {
        // Isolate small files. They don't really represent a big savings
        // opportunity, and this avoids weird cases of small files growing
        // which start as identical but diverge quickly, still being in the
        // same egroup.
        return Ok(rand.generate_file_id()?.into());
    }
    let egroup = match file::matching_egroup(tx, snapshot)? {
        Some(egroup) => egroup,
        None => rand.generate_file_id()?.into(),
    };
    Ok(egroup)
}

fn metadata_update(
    tx: &mut rusqlite::Transaction,
    path: &LocalPath,
    now: SystemTime,
    tree_gen: Option<i64>,
    fsize: FileSize,
    mtime: TimeInMicroseconds,
    timing: &Timing,
) -> anyhow::Result<()> {
    let Some(state) = file::get_state(tx, path)? else {
        file::new(tx, path, tree_gen.unwrap_or(0), &now.into())?;
        return Ok(());
    };
    let tree_gen = tree_gen.unwrap_or(state.tree_gen);
    let new = match state.state {
        // We can't leave NEW until we have a hash and egroup. But we should
        // still update the tree_gen to show that the file is still around.
        State::New(_) | State::Busy(_) => state.state,
        State::Dirty(mut current) => {
            if current.mtime != mtime || current.fsize != fsize {
                current.next = min(
                    current.threshold.into_inner(),
                    now + timing.cool_off_period.0,
                )
                .into();
            }
            State::Dirty(current)
        }
        // We might have enough information to know if the file is dirty again.
        State::Clean(clean) => {
            if clean.mtime != mtime || clean.fsize != fsize {
                // TODO: this is the same as hash_update in many ways.
                let earliest_backup =
                    max(clean.threshold.into_inner(), now + timing.cool_off_period.0);
                let latest_backup = earliest_backup + timing.cool_off_period.1;

                State::Dirty(Dirty {
                    next: earliest_backup.into(),
                    threshold: latest_backup.into(),
                    fsize,
                    mtime,
                    hash: clean.hash,
                    egroup: clean.egroup,
                })
            } else {
                // Even if it's clean, we want to keep the gen up-to-date.
                State::Clean(clean)
            }
        }
        // The file may have been readable in the past. For simplicity, treat it as
        // if it was new so we can pick the correct egroup and force a backup.
        State::Unreadable(_) => State::New(New { next: now.into() }),
    };
    file::set_state(tx, path, tree_gen, &new)?;
    Ok(())
}

fn initialize(
    mut con: rusqlite::Connection,
    rand: crypto::SharedRandom,
    timing: Timing,
) -> anyhow::Result<Connection> {
    con.pragma_update(None, "foreign_keys", "ON")
        .context("Failed to enable foreign keys")?;

    Connection::migrations()
        .to_latest(&mut con)
        .context("Failed to migrate Filestore database")?;
    Ok(Connection {
        conn: con,
        rand,
        timing,
    })
}

#[cfg(test)]
mod spread_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn in_range() {
        let mut t = Timing::default();
        t.spread = 0.1;

        let d = t.spread(Duration::from_secs(100));
        assert!(d >= Duration::from_secs((100f64 * 0.9) as u64));
        assert!(d <= Duration::from_secs((100f64 * 1.1) as u64));
    }
}
