//! Database of file information.
//!
//! This intentionally does _not_ perform any filesystem operations,
//! to ensure testability and isolation.
use anyhow::{anyhow, Context};
use crypto::model::FileId;
use platform::LocalPathRepr;
use rusqlite::OptionalExtension;
use rusqlite_migration::{Migrations, M};
use std::{
    collections::HashMap,
    ffi::OsString,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};
use tracing::instrument;

// ----------------- API DEFINITION ------------------------

/// Filestore defines how and when filesystem information is updated and retrieved.
pub trait Filestore {
    type Scanner<'a>: Scanner
    where
        Self: 'a;
    type Hasher<'a>: Hasher
    where
        Self: 'a;

    // Define the roots of the filesystem under scrutiny.
    fn set_roots(&mut self, roots: &[&Path]) -> anyhow::Result<bool>;
    // Request a full scan of the filesystem and mark when the next one will be due.
    fn tree_scan_start(&mut self, next_scan: SystemTime) -> anyhow::Result<()>;
    // Get the next directory to analyze, if it's due.
    fn tree_scan_next(&mut self) -> anyhow::Result<ScanNext<Self::Scanner<'_>>>;
    // Get the next file to analyze, if any is due.
    fn hash_next(&mut self, now: SystemTime) -> anyhow::Result<HashNext<Self::Hasher<'_>>>;
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

/// Hasher defines how the hash of a specific file is being updated.
pub trait Hasher {
    fn update(self, update: HashUpdate, next: SystemTime) -> anyhow::Result<()>;
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
    File(OsString),
    Directory(OsString),
}

/// What should be hashed next?
#[derive(Debug, PartialEq)]
pub enum HashNext<H: Hasher> {
    Next(PathBuf, H),
    Done(SystemTime),
}

/// Represents the file hash update.
#[derive(Debug)]
pub enum HashUpdate {
    File,
    Unreadable(std::io::Error),
}

// -----------------------------------------------------------------------

/// Default implementation of the Filestore.
pub struct Connection {
    conn: rusqlite::Connection,
    rand: crypto::SharedRandom,
}

/// Default implementation of the Updater.
pub struct UpdaterImpl<'a> {
    rand: crypto::SharedRandom,
    tx: rusqlite::Transaction<'a>,
    dir: Directory,
    aim: i64,
}

#[derive(Debug)]
pub struct HasherImpl<'a> {
    tx: rusqlite::Transaction<'a>,
    file: File,
}

impl std::fmt::Debug for UpdaterImpl<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Updater")
            .field("dir", &self.dir)
            .field("aim", &self.aim)
            .finish_non_exhaustive()
    }
}

/// Unique ID of a directory under the roots.
type DirectoryId = u64;

// Internal representation of a directory.
#[derive(Debug, PartialEq)]
struct Directory {
    id: DirectoryId,
    path: PathBuf,
    root: bool,
    error: Option<String>,
}

impl Directory {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, PartialEq)]
struct File {
    id: FileId,
    path: PathBuf,
}

/// A single Setting value.
#[derive(Debug, PartialEq)]
enum Setting {
    N(i64),
    T(String),
    Ts(SystemTime),
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
        Migrations::new(vec![M::up(
            r#"
-- Series of key/value pairs.
CREATE TABLE Settings (
    k  TEXT PRIMARY KEY,
    i  INTEGER,
    s  TEXT,
    ts INTEGER
) STRICT, WITHOUT ROWID;

CREATE TABLE Directory (
    id       INTEGER PRIMARY KEY,
    path     BLOB NOT NULL,     -- in local form, canonicalized
    root     INTEGER NOT NULL,  -- Boolean
    -- When walking the tree, tree_aim marks directories that have
    -- been seen, and tree_gen marks directories that haven been
    -- checked.
    tree_aim INTEGER NOT NULL,
    tree_gen INTEGER NOT NULL,

    complete     INTEGER NOT NULL, -- Boolean
    access_error TEXT
) STRICT;

CREATE UNIQUE INDEX idx_dir_path ON Directory(path);

CREATE TABLE FileId (
    id     BLOB PRIMARY KEY,
    path   BLOB NOT NULL,

    -- Last scan gen this file was seen.
    tree_gen INTEGER NOT NULL,

    -- Next time the file was hashed, in seconds since epoch.
    next_hash INTEGER NOT NULL,

    access_error TEXT
) STRICT, WITHOUT ROWID;

CREATE UNIQUE INDEX idx_fileid_path ON FileId(path);
"#,
        )])
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
    type Hasher<'a> = HasherImpl<'a>;

    /// Set the roots to the provided list. This supersedes the roots completely, which
    /// can impact which files will be backed up in the future.
    fn set_roots(&mut self, roots: &[&Path]) -> anyhow::Result<bool> {
        let mut changed = false; // tracks if any of the roots changed.
        struct R {
            id: DirectoryId,
        }
        let txn = self.conn.transaction()?;
        let mut old_roots = get_roots(&txn).context("Failed to get current roots")?;
        for root in roots {
            if let Some(_) = old_roots.get(*root) {
                // The directory exists and is already a root. Just ensure we don't delete
                // it at the end.
                old_roots.remove(*root);
                continue;
            }
            changed = true;
            // The directory is not a root, but might exist nevertheless.
            let root: LocalPathRepr = root.into();
            let mut stmt = txn.prepare("SELECT id FROM Directory WHERE path = ?1")?;
            let result = stmt
                .query_row((root.as_ref(),), |row| Ok(R { id: row.get(0)? }))
                .optional()?;
            match result {
                Some(R { id }) => {
                    txn.execute("UPDATE Directory SET root = TRUE WHERE id = ?1", (id,))
                        .context("Failed to demote root")?;
                }
                None => {
                    add_root(&txn, &root)?;
                }
            }
        }
        for old_root in old_roots.into_values() {
            changed = true;
            // We turn the old roots into regular directories. Their cleanup is done as
            // part of a scan, not now.
            txn.execute(
                "UPDATE Directory SET root = FALSE WHERE id = ?1",
                (old_root.id,),
            )
            .context("Failed to demote root")?;
        }
        txn.commit()?;
        Ok(changed)
    }

    /// Configure a new tree scan, and indicate the earliest time the next one should take
    /// place.
    fn tree_scan_start(&mut self, next_scan: SystemTime) -> anyhow::Result<()> {
        let txn = self.conn.transaction()?;
        // Begin by aiming the new scan at the roots.
        let aim = get_int_setting(&txn, SETTING_TREE_GEN)?.unwrap_or(0) + 1;
        set_setting(&txn, SETTING_TREE_GEN, &Setting::N(aim))?;
        set_setting(&txn, SETTING_NEXT_RUN, &Setting::Ts(next_scan))?;
        txn.execute(
            "UPDATE Directory SET tree_aim = ?1 WHERE root = TRUE",
            (aim,),
        )?;
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
        let aim = get_int_setting(&txn, SETTING_TREE_GEN)?.unwrap_or(0);
        let next = get_timestamp_setting(&txn, SETTING_NEXT_RUN)?.unwrap_or(SystemTime::UNIX_EPOCH);
        let result = txn
            .query_row(
                "SELECT id, path, root, access_error FROM Directory WHERE tree_gen < tree_aim AND tree_aim = ?1 ORDER BY path LIMIT 1",
                (aim,),
                |row| {
                    Ok(Directory {
                        id: row.get(0)?,
                        path: to_path(row.get(1)?)?,
                        root: row.get(2)?,
                        error: row.get(3)?,
                    })
                },
            )
            .optional()
            .context("Failed to find next dir to scan")?;
        txn.commit()?;

        let candidate = match result {
            Some(dir) => ScanNext::Next(
                dir.path().into(),
                UpdaterImpl {
                    rand: self.rand.clone(),
                    tx: self.conn.transaction()?,
                    dir,
                    aim,
                },
            ),
            None => ScanNext::Done(next),
        };
        Ok(candidate)
    }

    fn hash_next(&mut self, now: SystemTime) -> anyhow::Result<HashNext<HasherImpl>> {
        let txn = self.conn.transaction()?;
        let aim = get_int_setting(&txn, SETTING_TREE_GEN)?.unwrap_or(0);
        // We only hash files that were found during a recent scan: deleted files will remain in the
        // database for longer, but there is no point in finding they disappeared over and over again.
        // This does not apply to the watcher: if a file reappears, it'll be hashed right away, just
        // not from this code path.
        let result = txn
            .query_row(
                "SELECT id, path, next_hash FROM FileId WHERE tree_gen >= ?1 ORDER BY next_hash LIMIT 1",
                (
                    aim - 1,
                ),
                |row| {
                    let next : u64 = row.get(2)?;
                    let next = SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(next)).context("invalid next_hash").map_err(|e: anyhow::Error| rusqlite::Error::ToSqlConversionFailure(e.into()))?;
                    Ok((File {
                        id: to_file_id(row.get(0)?)?,
                        path: to_path(row.get(1)?)?,
                    }, next))
                },
            )
            .optional()
            .context("Failed to find next file to hash")?;
        txn.commit()?;
        let candidate = match result {
            Some((candidate, next)) => {
                if next <= now {
                    HashNext::Next(
                        candidate.path.clone(),
                        HasherImpl {
                            tx: self.conn.transaction()?,
                            file: candidate,
                        },
                    )
                } else {
                    HashNext::Done(next)
                }
            }
            // If there is no file to wait for, wait for some arbitrary time.
            // We just want to avoid spinning, this will become an actual value
            // as soon as the scanner has something to say.
            None => HashNext::Done(now + FILE_NEXT_DELAY),
        };
        Ok(candidate)
    }
}

impl<'a> Hasher for HasherImpl<'a> {
    fn update(self, update: HashUpdate, next: SystemTime) -> anyhow::Result<()> {
        match update {
            HashUpdate::File => {
                self.tx.execute(
                    "UPDATE FileId SET next_hash = ?1, access_error = NULL WHERE id = ?2",
                    (
                        next.duration_since(SystemTime::UNIX_EPOCH)?.as_secs(),
                        self.file.id.as_bytes(),
                    ),
                )?;
            }
            HashUpdate::Unreadable(error) => {
                self.tx.execute(
                    "UPDATE FileId SET next_hash = ?1, access_error = ?3 WHERE id = ?2",
                    (
                        next.duration_since(SystemTime::UNIX_EPOCH)?.as_secs(),
                        self.file.id.as_bytes(),
                        format!("{:?}", error),
                    ),
                )?;
            }
        }
        self.tx.commit()?;
        Ok(())
    }
}

impl<'a> Scanner for UpdaterImpl<'a> {
    fn update(&mut self, update: &ScanUpdate) -> anyhow::Result<()> {
        match update {
            ScanUpdate::Directory(subdir) => {
                let path: LocalPathRepr = self.dir.path.join(subdir).into();
                let count = self.tx.execute(
                    "UPDATE Directory SET tree_aim = ?1 WHERE path = ?2",
                    (self.aim, path.as_ref()),
                )?;
                if count == 0 {
                    self.tx.execute("INSERT INTO Directory(path, root, tree_aim, tree_gen, complete) VALUES (?1, FALSE, ?2, 0, 0)", (path.as_ref(), self.aim))?;
                }
            }
            ScanUpdate::File(file) => {
                let path: LocalPathRepr = self.dir.path.join(file).into();
                let count = self.tx.execute(
                    "UPDATE FileId SET tree_gen = ?1 WHERE path = ?2",
                    (self.aim, path.as_ref()),
                )?;
                if count == 0 {
                    // INSERT can fail if there is a collision on the generated FileId. This
                    // should be incredibly rare, but is also something we can trivially handle
                    // by retrying (until we have a significant chunk of 2^48 files...)
                    for attempt in 1..3 {
                        if let Err(e) = self.tx.execute(
                                "INSERT INTO FileId(id, path, tree_gen, next_hash) VALUES (?1, ?2, ?3, 0)",
                                (self.rand.generate_file_id()?.as_bytes(), path.as_ref(), self.aim),
                            ) {
                                if e.sqlite_error_code().is_none_or(|code| {
                                    code != rusqlite::ErrorCode::ConstraintViolation
                                }) {
                                    Err(e)? // re-thrown unknown problems
                                }
                                tracing::warn!(
                                    "Collision while generating new FileId, attempt {}",
                                    attempt,
                                );
                            } else {
                                break;
                            }
                    }
                }
            }
        };
        Ok(())
    }
    fn commit(self, complete: bool) -> anyhow::Result<()> {
        self.tx.execute(
            "UPDATE Directory SET tree_gen = ?1, complete = ?3, access_error = NULL WHERE id = ?2",
            (self.aim, self.dir.id, complete),
        )?;
        self.tx.commit()?;
        Ok(())
    }

    fn error(self, error: std::io::Error) -> anyhow::Result<()> {
        self.tx.execute(
            "UPDATE Directory SET tree_gen = ?1, complete = 0, access_error = ?3 WHERE id = ?2",
            (self.aim, self.dir.id, format!("{:?}", error)),
        )?;
        self.tx.commit()?;
        Ok(())
    }
}

fn add_root(txn: &rusqlite::Transaction, path: &LocalPathRepr) -> anyhow::Result<DirectoryId> {
    let mut stmt = txn
        .prepare("INSERT INTO Directory(path, root, tree_aim, tree_gen, complete) VALUES (?1, TRUE, 0, 0, 0)")?;
    stmt.execute((path.as_ref(),))
        .context("Failed to insert root")?;
    Ok(txn.last_insert_rowid() as DirectoryId)
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

fn get_roots(txn: &rusqlite::Transaction) -> anyhow::Result<HashMap<PathBuf, Directory>> {
    let mut roots = HashMap::new();
    let mut stmt =
        txn.prepare("SELECT id, path, root, access_error FROM Directory WHERE root = TRUE")?;
    for row in stmt.query_map((), |row| {
        Ok(Directory {
            id: row.get(0)?,
            path: to_path(row.get(1)?)?,
            root: row.get(2)?,
            error: row.get(3)?,
        })
    })? {
        let row = row?;
        roots.insert(row.path.clone(), row);
    }
    Ok(roots)
}

fn set_setting(txn: &rusqlite::Transaction, key: &str, value: &Setting) -> anyhow::Result<()> {
    match value {
        Setting::N(i) => {
            txn.execute(
                "INSERT OR REPLACE INTO Settings (k, i, s, ts) VALUES (?1, ?2, NULL, NULL)",
                (key, i),
            )?;
        }
        Setting::T(t) => {
            txn.execute(
                "INSERT OR REPLACE INTO Settings (k, i, s, ts) VALUES (?1, NULL, ?2, NULL)",
                (key, t),
            )?;
        }
        Setting::Ts(ts) => {
            txn.execute(
                "INSERT OR REPLACE INTO Settings (k, i, s, ts) VALUES (?1, NULL, NULL, ?2)",
                (key, ts.duration_since(SystemTime::UNIX_EPOCH)?.as_secs()),
            )?;
        }
    }
    Ok(())
}

// Like get_settings() but within a transaction.
fn get_setting(txn: &rusqlite::Transaction, key: &str) -> anyhow::Result<Option<Setting>> {
    let result = txn
        .query_row(
            "SELECT i, s, ts FROM Settings WHERE k = ?1",
            (key,),
            |row| {
                let i: Option<i64> = row.get(0)?;
                let s: Option<String> = row.get(1)?;
                let ts: Option<u64> = row.get(2)?;
                if let Some(i) = i {
                    return Ok(Setting::N(i));
                }
                if let Some(s) = s {
                    return Ok(Setting::T(s));
                }
                if let Some(ts) = ts {
                    return Ok(Setting::Ts(SystemTime::UNIX_EPOCH + Duration::new(ts, 0)));
                }
                Err(rusqlite::Error::ToSqlConversionFailure(
                    anyhow!("Setting is neither int nor text").into(),
                ))
            },
        )
        .optional()?;
    Ok(result)
}

fn get_int_setting(txn: &rusqlite::Transaction, key: &str) -> anyhow::Result<Option<i64>> {
    match get_setting(txn, key)? {
        Some(Setting::N(i)) => Ok(Some(i)),
        Some(_) => Err(anyhow!("key does not hold an int")),
        None => Ok(None),
    }
}

fn get_timestamp_setting(
    txn: &rusqlite::Transaction,
    key: &str,
) -> anyhow::Result<Option<SystemTime>> {
    match get_setting(txn, key)? {
        Some(Setting::Ts(ts)) => Ok(Some(ts)),
        Some(_) => Err(anyhow!("key does not hold a timestamp")),
        None => Ok(None),
    }
}

fn to_path(data: Vec<u8>) -> rusqlite::Result<PathBuf> {
    let repr = LocalPathRepr::new(data);
    let path: PathBuf = repr
        .try_into()
        .map_err(|e: platform::PlatformError| rusqlite::Error::ToSqlConversionFailure(e.into()))?;
    Ok(path)
}

fn to_file_id(data: Vec<u8>) -> rusqlite::Result<FileId> {
    // TODO: FileId should accept AsRef<[u8]> or something similar.
    let data: &[u8] = &data;
    let id: FileId = data
        .try_into()
        .map_err(|e: anyhow::Error| rusqlite::Error::ToSqlConversionFailure(e.into()))?;

    Ok(id)
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::bail;
    use crypto::model::FileId;
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
        time::{Duration, SystemTime},
    };
    use test_log::test;

    #[test]
    fn set_roots_is_stable() -> anyhow::Result<()> {
        let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()))?;
        let p1 = Path::new("/a/b");
        let p2 = Path::new("/c/d");
        let c1 = cnx.set_roots(&[p1])?;
        let c2 = cnx.set_roots(&[p1, p2])?;
        assert!(c1);
        assert!(c2);

        let got: Vec<(PathBuf, bool)> = directory_dump(cnx.conn())?
            .into_iter()
            .map(|d| (d.path, d.root))
            .collect();
        assert_eq!(got, vec![(p1.into(), true), (p2.into(), true),]);
        Ok(())
    }

    #[test]
    fn set_roots_removes_old_roots() -> anyhow::Result<()> {
        let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()))?;
        let p1 = Path::new("/a/b");
        let p2 = Path::new("/c/d");
        let c1 = cnx.set_roots(&[p1]).context("Can't set p1")?;
        let c2 = cnx.set_roots(&[p2]).context("Can't set p2")?;
        assert!(c1);
        assert!(c2);

        let got: Vec<(PathBuf, bool)> = directory_dump(cnx.conn())?
            .into_iter()
            .map(|d| (d.path, d.root))
            .collect();
        // not root anymore
        assert_eq!(got, vec![(p1.into(), false), (p2.into(), true),]);

        // We can also revert back
        let c3 = cnx.set_roots(&[p1])?;
        assert!(c3);

        let got: Vec<(PathBuf, bool)> = directory_dump(cnx.conn())?
            .into_iter()
            .map(|d| (d.path, d.root))
            .collect();
        // root again!
        assert_eq!(got, vec![(p1.into(), true), (p2.into(), false),]);

        Ok(())
    }

    #[test]
    fn manipulate_settings() -> anyhow::Result<()> {
        let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()))?;
        let txn = cnx.conn.transaction()?;

        assert_eq!(get_setting(&txn, "a")?, None);

        let now = SystemTime::UNIX_EPOCH + Duration::new(3600, 0);
        set_setting(&txn, "a", &Setting::Ts(now))?;
        assert_eq!(get_setting(&txn, "a")?, Some(Setting::Ts(now)));

        set_setting(&txn, "a", &Setting::N(123))?;
        assert_eq!(get_setting(&txn, "a")?, Some(Setting::N(123)));

        set_setting(&txn, "a", &Setting::T("foo".into()))?;
        assert_eq!(get_setting(&txn, "a")?, Some(Setting::T("foo".into())));

        Ok(())
    }

    #[test]
    fn tree_scan() -> anyhow::Result<()> {
        let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()))?;
        let a = Path::new("a");
        let b = Path::new("b");
        cnx.set_roots(&[a, b])?;

        let ScanNext::Done(next) = cnx.tree_scan_next()? else {
            bail!("unexpected");
        };
        assert_eq!(next, SystemTime::UNIX_EPOCH); // Not initiated yet.

        let next_time = SystemTime::UNIX_EPOCH + Duration::new(86400, 0);
        cnx.tree_scan_start(next_time)?;

        let (dir, mut updater) = updater_or(cnx.tree_scan_next()?)?;
        assert_eq!(dir, a);

        let f1: OsString = "f1".into();
        updater.update(&ScanUpdate::File(f1))?;
        updater.commit(true)?;

        // Now we progress.
        let (dir, mut updater) = updater_or(cnx.tree_scan_next()?)?;
        assert_eq!(dir, b);

        let p3: OsString = "e".into();
        updater.update(&ScanUpdate::Directory(p3))?;
        updater.commit(true)?;

        let (dir, updater) = updater_or(cnx.tree_scan_next()?)?;
        assert_eq!(dir, b.join("e"));

        updater.commit(true)?;

        let ScanNext::Done(next) = cnx.tree_scan_next()? else {
            bail!("unexpected");
        };
        assert_eq!(next, next_time);

        let got: Vec<(PathBuf, u64, u64)> = directory_dump(cnx.conn())?
            .into_iter()
            .map(|d| (d.path, d.aim, d.gen))
            .collect();
        assert_eq!(
            got,
            vec![(a.into(), 1, 1), (b.into(), 1, 1), (b.join("e"), 1, 1)]
        );

        let got: Vec<(PathBuf, u64)> = fileid_dump(cnx.conn())?
            .into_iter()
            .map(|d| (d.path, d.gen))
            .collect();
        assert_eq!(got, vec![(a.join("f1"), 1)]);
        Ok(())
    }

    #[test]
    fn tree_rescan() -> anyhow::Result<()> {
        let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()))?;
        let a = Path::new("a");
        cnx.set_roots(&[a])?;

        let next_time = SystemTime::UNIX_EPOCH + Duration::new(86400, 0);
        cnx.tree_scan_start(next_time)?;

        let (_, mut updater) = updater_or(cnx.tree_scan_next()?)?;
        updater.update(&ScanUpdate::File("f1".into()))?;
        updater.update(&ScanUpdate::File("f2".into()))?;
        updater.update(&ScanUpdate::Directory("d1".into()))?;
        updater.update(&ScanUpdate::Directory("d2".into()))?;
        updater.commit(true)?;

        let (_, updater) = updater_or(cnx.tree_scan_next()?)?;
        updater.commit(true)?;

        let (_, updater) = updater_or(cnx.tree_scan_next()?)?;
        updater.commit(true)?;

        let next_time = SystemTime::UNIX_EPOCH + Duration::new(86400, 0);
        cnx.tree_scan_start(next_time)?;

        let (_, mut updater) = updater_or(cnx.tree_scan_next()?)?;
        updater.update(&ScanUpdate::File("f1".into()))?;
        updater.update(&ScanUpdate::File("f3".into()))?;
        updater.update(&ScanUpdate::Directory("d1".into()))?;
        updater.update(&ScanUpdate::Directory("d3".into()))?;
        updater.commit(true)?;

        let (_, updater) = updater_or(cnx.tree_scan_next()?)?;
        updater.commit(true)?;

        let (_, updater) = updater_or(cnx.tree_scan_next()?)?;
        updater.commit(true)?;

        let got: Vec<(PathBuf, u64, u64)> = directory_dump(cnx.conn())?
            .into_iter()
            .map(|d| (d.path, d.aim, d.gen))
            .collect();
        assert_eq!(
            got,
            vec![
                (a.into(), 2, 2),
                (a.join("d1"), 2, 2),
                (a.join("d2"), 1, 1),
                (a.join("d3"), 2, 2)
            ]
        );

        let got: Vec<(PathBuf, u64)> = fileid_dump(cnx.conn())?
            .into_iter()
            .map(|d| (d.path, d.gen))
            .collect();
        assert_eq!(
            got,
            vec![(a.join("f1"), 2), (a.join("f2"), 1), (a.join("f3"), 2),]
        );

        Ok(())
    }

    #[test]
    fn tree_update_drop_is_noop() -> anyhow::Result<()> {
        let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()))?;
        let p1 = Path::new("/a");
        cnx.set_roots(&[p1])?;

        cnx.tree_scan_start(SystemTime::UNIX_EPOCH)?;
        {
            let (_, mut updater) = updater_or(cnx.tree_scan_next()?)?;
            updater.update(&ScanUpdate::File("b".into()))?;
        }
        let got: Vec<PathBuf> = fileid_dump(cnx.conn())?
            .into_iter()
            .map(|d| d.path)
            .collect();
        // Despite the update(File()) above, the lack of commit means the db was not
        // altered.
        assert!(got.is_empty());

        Ok(())
    }

    #[test]
    fn tree_scan_with_errors() -> anyhow::Result<()> {
        let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()))?;
        let p1 = Path::new("/a/b");
        cnx.set_roots(&[p1])?;

        cnx.tree_scan_start(SystemTime::UNIX_EPOCH)?;
        let (_, updater) = updater_or(cnx.tree_scan_next()?)?;
        // Make it unreachable
        updater.error(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "get out",
        ))?;
        let got: Vec<(PathBuf, Option<String>)> = directory_dump(cnx.conn())?
            .into_iter()
            .map(|d| (d.path, d.error))
            .collect();
        assert_eq!(
            got,
            vec![(
                p1.into(),
                Some("Custom { kind: PermissionDenied, error: \"get out\" }".to_string()),
            )]
        );

        // ...and fix reachability at the next round.
        cnx.tree_scan_start(SystemTime::UNIX_EPOCH)?;
        let (_, updater) = updater_or(cnx.tree_scan_next()?)?;
        updater.commit(true)?;

        let ScanNext::Done(next) = cnx.tree_scan_next()? else {
            panic!("unexpected");
        };
        assert_eq!(next, SystemTime::UNIX_EPOCH);

        let got: Vec<(PathBuf, Option<String>)> = directory_dump(cnx.conn())?
            .into_iter()
            .map(|d| (d.path, d.error))
            .collect();
        assert_eq!(got, vec![(p1.into(), None)]);

        Ok(())
    }

    #[test]
    fn tree_update_handle_collisions() -> anyhow::Result<()> {
        let rand_seq = FakeRandom::new(vec![fileid(1), fileid(1), fileid(2)]);
        let mut cnx = Connection::new_in_memory(Arc::new(rand_seq))?;
        let a = Path::new("a");
        cnx.set_roots(&[a])?;

        cnx.tree_scan_start(SystemTime::UNIX_EPOCH)?;

        let (_, mut updater) = updater_or(cnx.tree_scan_next()?)?;
        updater.update(&ScanUpdate::File("b".into()))?; // will get id 1.
        updater.update(&ScanUpdate::File("c".into()))?; // will collide with 1 then get 2.
        updater.commit(true)?;

        let got: Vec<(PathBuf, FileId)> = fileid_dump(cnx.conn())?
            .into_iter()
            .map(|d| (d.path, d.id))
            .collect();
        assert_eq!(
            got,
            vec![(a.join("b"), fileid(1)), (a.join("c"), fileid(2)),]
        );
        Ok(())
    }

    #[test]
    fn tree_scan_node_type_change() -> anyhow::Result<()> {
        let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()))?;
        let a = Path::new("a");
        cnx.set_roots(&[a])?;

        let b: OsString = "b".into();

        cnx.tree_scan_start(SystemTime::UNIX_EPOCH)?;

        // Make a/b a file first.
        let (_, mut updater) = updater_or(cnx.tree_scan_next()?)?;
        updater.update(&ScanUpdate::File(b.clone()))?;
        updater.commit(true)?;

        cnx.tree_scan_start(SystemTime::UNIX_EPOCH)?;

        // Make a/b a directory next.
        let (_, mut updater) = updater_or(cnx.tree_scan_next()?)?;
        updater.update(&ScanUpdate::Directory(b.clone()))?;
        updater.commit(true)?;

        let (_, updater) = updater_or(cnx.tree_scan_next()?)?;
        updater.commit(true)?;

        let got: Vec<(PathBuf, u64)> = fileid_dump(cnx.conn())?
            .into_iter()
            .map(|d| (d.path, d.gen))
            .collect();
        // "a/b" is currently a directory, so the file gen is stuck at 1.
        assert_eq!(got, vec![(a.join("b"), 1)]);

        cnx.tree_scan_start(SystemTime::UNIX_EPOCH)?;

        let (_, mut updater) = updater_or(cnx.tree_scan_next()?)?;
        updater.update(&ScanUpdate::File(b.clone()))?;
        updater.commit(true)?;

        let got: Vec<(PathBuf, u64)> = fileid_dump(cnx.conn())?
            .into_iter()
            .map(|d| (d.path, d.gen))
            .collect();
        // The file is back on the scan.
        assert_eq!(got, vec![(a.join("b"), 3)]);

        let got: Vec<(PathBuf, u64, u64)> = directory_dump(cnx.conn())?
            .into_iter()
            .map(|d| (d.path, d.gen, d.aim))
            .collect();
        // ... and "a/b" as a directory is now stale.
        assert_eq!(got, vec![(a.into(), 3, 3), (a.join("b"), 2, 2),]);

        Ok(())
    }

    #[test]
    fn hash_next() -> anyhow::Result<()> {
        let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()))?;

        let delta = Duration::from_secs(10);
        let now = SystemTime::UNIX_EPOCH + delta;

        // Initially, no file is known so no hash is requested.
        let HashNext::Done(next) = cnx.hash_next(now)? else {
            panic!("unexpected")
        };
        assert_eq!(next, now + FILE_NEXT_DELAY);

        let a = Path::new("a");
        cnx.set_roots(&[a])?;

        let next_time = SystemTime::UNIX_EPOCH + Duration::new(86400, 0);
        cnx.tree_scan_start(next_time)?;

        let (_, mut updater) = updater_or(cnx.tree_scan_next()?)?;
        updater.update(&ScanUpdate::File("f1".into()))?;
        updater.commit(true)?;

        // From now on, file a/f1 is in the database, but with no hash yet.
        let (file, hasher) = hasher_or(cnx.hash_next(now)?)?;
        assert_eq!(file, a.join("f1"));
        drop(hasher); // Intentionally don't advance.

        let (file, hasher) = hasher_or(cnx.hash_next(now)?)?;
        assert_eq!(file, a.join("f1"));

        hasher.update(HashUpdate::File, now + delta)?;

        // The next hash is scheduled for when the file above should be
        // checked again.
        let HashNext::Done(next) = cnx.hash_next(now)? else {
            panic!("unexpected")
        };
        assert_eq!(next, now + delta);

        Ok(())
    }

    #[test]
    fn migrations_test() {
        assert!(Connection::migrations().validate().is_ok());
    }

    // ===================== HELPERS =======================

    #[derive(Debug, PartialEq)]
    struct FullDirectory {
        id: DirectoryId,
        path: PathBuf,
        root: bool,
        gen: u64,
        aim: u64,
        error: Option<String>,
    }

    #[derive(Debug, PartialEq)]
    struct FullFileId {
        id: FileId,
        path: PathBuf,
        gen: u64,
    }

    fn updater_or<U: Scanner>(next: ScanNext<U>) -> anyhow::Result<(PathBuf, U)> {
        match next {
            ScanNext::Done(_) => bail!("no next directory"),
            ScanNext::Next(dir, updater) => Ok((dir, updater)),
        }
    }

    fn hasher_or<H: Hasher>(next: HashNext<H>) -> anyhow::Result<(PathBuf, H)> {
        match next {
            HashNext::Done(_) => bail!("no next file to hash"),
            HashNext::Next(file, hasher) => Ok((file, hasher)),
        }
    }

    fn directory_dump(conn: &mut rusqlite::Connection) -> anyhow::Result<Vec<FullDirectory>> {
        let mut result = vec![];
        let mut stmt = conn.prepare(
            "SELECT id, path, root, tree_aim, tree_gen, access_error FROM Directory ORDER BY path",
        )?;
        for row in stmt
            .query_map((), |row| {
                Ok(FullDirectory {
                    id: row.get(0)?,
                    path: to_path(row.get(1)?)?,
                    root: row.get(2)?,
                    aim: row.get(3)?,
                    gen: row.get(4)?,
                    error: row.get(5)?,
                })
            })
            .context("Failed to list directories")?
        {
            result.push(row?);
        }
        Ok(result)
    }

    fn fileid_dump(conn: &mut rusqlite::Connection) -> anyhow::Result<Vec<FullFileId>> {
        let mut result = vec![];
        let mut stmt = conn.prepare("SELECT id, path, tree_gen FROM FileId ORDER BY path")?;
        for row in stmt
            .query_map((), |row| {
                let id: Vec<u8> = row.get(0)?;
                Ok(FullFileId {
                    id: to_fileid(&id)?,
                    path: to_path(row.get(1)?)?,
                    gen: row.get(2)?,
                })
            })
            .context("Faild to list FileIds")?
        {
            result.push(row?);
        }
        Ok(result)
    }

    fn to_fileid(data: &[u8]) -> rusqlite::Result<FileId> {
        let file_id: FileId = data
            .try_into()
            .map_err(|e: anyhow::Error| rusqlite::Error::ToSqlConversionFailure(e.into()))?;
        Ok(file_id)
    }

    fn fileid(b: u8) -> FileId {
        let bytes: [u8; crypto::model::FILE_ID_LEN] = [b, 0, 0, 0, 0, 0];
        crypto::model::FileId::try_from(bytes.as_ref()).unwrap()
    }

    struct FakeRandom {
        next: Arc<Mutex<VecDeque<FileId>>>,
    }

    impl FakeRandom {
        fn new(next: Vec<FileId>) -> FakeRandom {
            let ids = next.into_iter().collect();
            FakeRandom {
                next: Arc::new(Mutex::new(ids)),
            }
        }
    }

    impl crypto::RandomApi for FakeRandom {
        fn generate_file_id(&self) -> anyhow::Result<FileId> {
            self.next.lock().unwrap().pop_front().context("no id left")
        }

        fn generate_block_id(&self) -> anyhow::Result<crypto::model::BlockId> {
            todo!()
        }
    }
}
