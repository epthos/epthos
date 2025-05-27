use super::{directory::pth as dh, file::pth as fh, *};
use crate::filestore::file::File;
use anyhow::bail;
use crypto::model::EncryptionGroup;
use ring::digest;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use test_log::test;

#[test]
fn add_and_remove_roots() -> anyhow::Result<()> {
    let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()))?;
    let got = |cnx: &mut Connection| -> anyhow::Result<Vec<(PathBuf, bool)>> {
        Ok(dh::dump(cnx.conn())?
            .into_iter()
            .map(|d| (d.path, d.root))
            .collect())
    };

    let p1 = Path::new("/a/b");
    let p2 = Path::new("/c/d");
    assert!(cnx.set_roots(&[p1])?);
    assert_eq!(got(&mut cnx)?, vec![(p1.into(), true),]);

    assert!(cnx.set_roots(&[p1, p2])?);
    assert_eq!(got(&mut cnx)?, vec![(p1.into(), true), (p2.into(), true),]);

    assert!(cnx.set_roots(&[p1])?);
    assert_eq!(got(&mut cnx)?, vec![(p1.into(), true), (p2.into(), false),]);
    Ok(())
}

#[test]
fn tree_scan() -> anyhow::Result<()> {
    let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()))?;
    let a = Path::new("a");
    let b = Path::new("b");
    cnx.set_roots(&[a, b])?;

    let Next::Done(next) = cnx.tree_scan_next()? else {
        bail!("unexpected");
    };
    assert_eq!(next, SystemTime::UNIX_EPOCH); // Not initiated yet.

    let next_time = SystemTime::UNIX_EPOCH + Duration::from_secs(86400);
    cnx.tree_scan_start(next_time)?;

    let (dir, mut updater) = cnx.tree_scan_next()?.next()?;
    assert_eq!(dir, a);

    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(3210);

    let f1: OsString = "f1".into();
    let f1_modtime = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
    updater.update(&ScanUpdate::File(f1, now, 100, f1_modtime))?;
    updater.commit(true)?;

    // Now we progress.
    let (dir, mut updater) = cnx.tree_scan_next()?.next()?;
    assert_eq!(dir, b);

    let e: OsString = "e".into();
    updater.update(&ScanUpdate::Directory(e))?;
    updater.commit(true)?;

    let (dir, updater) = cnx.tree_scan_next()?.next()?;
    assert_eq!(dir, b.join("e"));

    updater.commit(true)?;

    let Next::Done(next) = cnx.tree_scan_next()? else {
        bail!("unexpected");
    };
    assert_eq!(next, next_time);

    let got: Vec<(PathBuf, u64, u64)> = dh::dump(cnx.conn())?
        .into_iter()
        .map(|d| (d.path, d.tree_aim, d.tree_gen))
        .collect();
    assert_eq!(
        got,
        vec![(a.into(), 1, 1), (b.into(), 1, 1), (b.join("e"), 1, 1)]
    );

    let got = fh::dump(cnx.conn())?;
    assert_eq!(
        got,
        vec![(
            a.join("f1"),
            File {
                tree_gen: 1,
                state: State::New(New { next: now.into() })
            }
        )]
    );
    Ok(())
}

#[test]
fn tree_rescan() -> anyhow::Result<()> {
    let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()))?;
    let a = Path::new("a");
    cnx.set_roots(&[a])?;

    let next_time = SystemTime::UNIX_EPOCH + Duration::new(86400, 0);
    cnx.tree_scan_start(next_time)?;

    let t1 = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);

    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    let f1_mod_1 = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let f2_mod_1 = SystemTime::UNIX_EPOCH + Duration::from_secs(200);
    updater.update(&ScanUpdate::File("f1".into(), t1, 1, f1_mod_1))?;
    updater.update(&ScanUpdate::File("f2".into(), t1, 2, f2_mod_1))?;
    updater.update(&ScanUpdate::Directory("d1".into()))?;
    updater.update(&ScanUpdate::Directory("d2".into()))?;
    updater.commit(true)?;

    let (_, updater) = cnx.tree_scan_next()?.next()?;
    updater.commit(true)?;

    let (_, updater) = cnx.tree_scan_next()?.next()?;
    updater.commit(true)?;

    let next_time = SystemTime::UNIX_EPOCH + Duration::new(86400, 0);
    cnx.tree_scan_start(next_time)?;

    let t2 = SystemTime::UNIX_EPOCH + Duration::from_secs(2000);

    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    let f1_mod_2 = SystemTime::UNIX_EPOCH + Duration::from_secs(300);
    let f3_mod_1 = SystemTime::UNIX_EPOCH + Duration::from_secs(400);
    updater.update(&ScanUpdate::File("f1".into(), t2, 3, f1_mod_2))?;
    updater.update(&ScanUpdate::File("f3".into(), t2, 4, f3_mod_1))?;
    updater.update(&ScanUpdate::Directory("d1".into()))?;
    updater.update(&ScanUpdate::Directory("d3".into()))?;
    updater.commit(true)?;

    let (_, updater) = cnx.tree_scan_next()?.next()?;
    updater.commit(true)?;

    let (_, updater) = cnx.tree_scan_next()?.next()?;
    updater.commit(true)?;

    let got: Vec<(PathBuf, u64, u64)> = dh::dump(cnx.conn())?
        .into_iter()
        .map(|d| (d.path, d.tree_aim, d.tree_gen))
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

    let got = fh::dump(cnx.conn())?;
    assert_eq!(
        got,
        vec![
            (
                a.join("f1"),
                // Even when rescanned, we don't want to delay the
                // hashing of this file.
                File {
                    tree_gen: 2,
                    state: State::New(New { next: t1.into() }),
                }
            ),
            (
                a.join("f2"),
                File {
                    tree_gen: 1,
                    state: State::New(New { next: t1.into() }),
                }
            ),
            (
                a.join("f3"),
                File {
                    tree_gen: 2,
                    state: State::New(New { next: t2.into() }),
                }
            ),
        ]
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
        let (_, mut updater) = cnx.tree_scan_next()?.next()?;
        updater.update(&ScanUpdate::File(
            "b".into(),
            SystemTime::UNIX_EPOCH.into(),
            1,
            SystemTime::UNIX_EPOCH,
        ))?;
    }
    // Despite the update(File()) above, the lack of commit means the db was not
    // altered.
    assert!(fh::dump(cnx.conn())?.is_empty());

    Ok(())
}

#[test]
fn tree_scan_with_errors() -> anyhow::Result<()> {
    let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()))?;
    let p1 = Path::new("/a/b");
    cnx.set_roots(&[p1])?;

    cnx.tree_scan_start(SystemTime::UNIX_EPOCH)?;
    let (_, updater) = cnx.tree_scan_next()?.next()?;
    // Make it unreachable
    updater.error(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "get out",
    ))?;
    let got: Vec<(PathBuf, Option<String>)> = dh::dump(cnx.conn())?
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
    let (_, updater) = cnx.tree_scan_next()?.next()?;
    updater.commit(true)?;

    let Next::Done(next) = cnx.tree_scan_next()? else {
        panic!("unexpected");
    };
    assert_eq!(next, SystemTime::UNIX_EPOCH);

    let got: Vec<(PathBuf, Option<String>)> = dh::dump(cnx.conn())?
        .into_iter()
        .map(|d| (d.path, d.error))
        .collect();
    assert_eq!(got, vec![(p1.into(), None)]);

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
    let t1 = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    let b_mod = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
    updater.update(&ScanUpdate::File(b.clone(), t1, 1, b_mod))?;
    updater.commit(true)?;

    cnx.tree_scan_start(SystemTime::UNIX_EPOCH)?;

    // Make a/b a directory next.
    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    updater.update(&ScanUpdate::Directory(b.clone()))?;
    updater.commit(true)?;

    let (_, updater) = cnx.tree_scan_next()?.next()?;
    updater.commit(true)?;

    // "a/b" is currently a directory, so the file gen is stuck at 1.
    let got = fh::dump(cnx.conn())?;
    assert_eq!(
        got,
        vec![(
            a.join("b"),
            File {
                tree_gen: 1,
                state: State::New(New { next: t1.into() })
            }
        )]
    );

    cnx.tree_scan_start(SystemTime::UNIX_EPOCH)?;

    let t2 = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    updater.update(&ScanUpdate::File(b.clone(), t2, 1, b_mod))?;
    updater.commit(true)?;

    // The file is back on the scan.
    let got = fh::dump(cnx.conn())?;
    assert_eq!(
        got,
        vec![(
            a.join("b"),
            File {
                tree_gen: 3,
                // Still on the initial time for hashing.
                state: State::New(New { next: t1.into() })
            }
        )]
    );

    let got: Vec<(PathBuf, u64, u64)> = dh::dump(cnx.conn())?
        .into_iter()
        .map(|d| (d.path, d.tree_gen, d.tree_aim))
        .collect();
    // ... and "a/b" as a directory is now stale.
    assert_eq!(got, vec![(a.into(), 3, 3), (a.join("b"), 2, 2),]);

    Ok(())
}

#[test]
fn hash_next() -> anyhow::Result<()> {
    let egroup = fileid(1);
    let mut cnx = Connection::new_in_memory(Arc::new(FakeRandom::new(vec![egroup.clone()])))?;

    let delta = Duration::from_secs(10);
    let now = SystemTime::UNIX_EPOCH + delta;

    // Initially, no file is known so no hash is requested.
    let Next::Done(next) = cnx.hash_next(now)? else {
        panic!("unexpected")
    };
    assert_eq!(next, now + FILE_NEXT_DELAY); // Arbitrary delay until we get files.

    let a = Path::new("a");
    cnx.set_roots(&[a])?;

    let next_time = SystemTime::UNIX_EPOCH + Duration::new(86400, 0);
    cnx.tree_scan_start(next_time)?;

    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    updater.update(&ScanUpdate::File(
        "f1".into(),
        now.into(),
        1,
        SystemTime::UNIX_EPOCH,
    ))?;
    updater.commit(true)?;

    // From now on, file a/f1 is in the database, but with no hash yet.
    //
    // First try to get the hash a bit too soon:
    let Next::Done(next) = cnx.hash_next(SystemTime::UNIX_EPOCH)? else {
        panic!("unexpected")
    };
    assert_eq!(next, now);

    let f1 = a.join("f1");
    let (file, _) = cnx.hash_next(now)?.next()?;
    assert_eq!(&file, &f1);

    let (file, _) = cnx.hash_next(now)?.next()?;
    assert_eq!(&file, &f1);

    let fsize = 100;
    let mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(3600);
    let hash = digest::digest(&digest::SHA256, b"boo");
    cnx.hash_update(
        file,
        /*next=*/ now + delta * 2,
        /*soon=*/ now + delta,
        HashUpdate::Hash(Snapshot {
            fsize,
            mtime: mtime.clone(),
            hash: hash.clone(),
        }),
    )?;

    // The file is now dirty, and doesn't need to be hashed. We revert back
    // to arbitrary delay for the next attempt.
    let Next::Done(next) = cnx.hash_next(now)? else {
        panic!("unexpected")
    };
    assert_eq!(next, now + FILE_NEXT_DELAY);

    // Confirm that the file is now dirty as intended.
    let got = fh::dump(cnx.conn())?;
    assert_eq!(
        got,
        vec![(
            f1,
            File {
                tree_gen: 1,
                state: State::Dirty(Dirty {
                    // The file is made ready for backup "soon".
                    next: (now + delta).into(),
                    fsize,
                    mtime: mtime.into(),
                    hash: hash.as_ref().to_owned(),
                    egroup: egroup.into(),
                })
            }
        )]
    );

    Ok(())
}

#[test]
fn small_files_dont_share_egroups() -> anyhow::Result<()> {
    let now = SystemTime::UNIX_EPOCH;

    let eg1 = fileid(1);
    let eg2 = fileid(2);
    let mut cnx =
        Connection::new_in_memory(Arc::new(FakeRandom::new(vec![eg1.clone(), eg2.clone()])))?;
    let a = Path::new("a");
    cnx.set_roots(&[a])?;
    cnx.tree_scan_start(SystemTime::UNIX_EPOCH)?;
    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    updater.update(&ScanUpdate::File(
        "f1".into(),
        now,
        1,
        SystemTime::UNIX_EPOCH,
    ))?;
    updater.update(&ScanUpdate::File(
        "f2".into(),
        now,
        1,
        SystemTime::UNIX_EPOCH,
    ))?;
    updater.commit(true)?;

    let fsize = 100;
    let mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(3600);
    let hash = digest::digest(&digest::SHA256, b"boo");
    cnx.hash_update(
        a.join("f1"),
        SystemTime::UNIX_EPOCH,
        SystemTime::UNIX_EPOCH,
        HashUpdate::Hash(Snapshot {
            fsize,
            mtime: mtime.clone(),
            hash: hash.clone(),
        }),
    )?;
    cnx.hash_update(
        a.join("f2"),
        SystemTime::UNIX_EPOCH,
        SystemTime::UNIX_EPOCH,
        HashUpdate::Hash(Snapshot {
            fsize,
            mtime: mtime.clone(),
            hash: hash.clone(),
        }),
    )?;

    let got = fh::dump(cnx.conn())?;
    assert_eq!(
        got,
        vec![
            (
                a.join("f1"),
                File {
                    tree_gen: 1,
                    state: State::Dirty(Dirty {
                        next: now.into(),
                        fsize,
                        mtime: mtime.into(),
                        hash: hash.as_ref().to_owned(),
                        egroup: eg1.into(),
                    })
                }
            ),
            (
                a.join("f2"),
                File {
                    tree_gen: 1,
                    state: State::Dirty(Dirty {
                        next: now.into(),
                        fsize,
                        mtime: mtime.into(),
                        hash: hash.as_ref().to_owned(),
                        egroup: eg2.into(),
                    })
                }
            )
        ]
    );
    Ok(())
}

#[test]
fn large_identical_files_share_egroups() -> anyhow::Result<()> {
    let now = SystemTime::UNIX_EPOCH;

    let egroup = fileid(1);
    let mut cnx = Connection::new_in_memory(Arc::new(FakeRandom::new(vec![egroup.clone()])))?;
    let a = Path::new("a");
    cnx.set_roots(&[a])?;
    cnx.tree_scan_start(SystemTime::UNIX_EPOCH)?;
    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    updater.update(&ScanUpdate::File(
        "f1".into(),
        now.into(),
        1,
        SystemTime::UNIX_EPOCH,
    ))?;
    updater.update(&ScanUpdate::File(
        "f2".into(),
        now.into(),
        1,
        SystemTime::UNIX_EPOCH,
    ))?;
    updater.commit(true)?;

    let fsize = 2 * SMALLEST_INDEPENDENT_FILE;
    let mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(3600);
    let hash = digest::digest(&digest::SHA256, b"boo");
    cnx.hash_update(
        a.join("f1"),
        SystemTime::UNIX_EPOCH,
        SystemTime::UNIX_EPOCH,
        HashUpdate::Hash(Snapshot {
            fsize,
            mtime: mtime.clone(),
            hash: hash.clone(),
        }),
    )?;
    cnx.hash_update(
        a.join("f2"),
        SystemTime::UNIX_EPOCH,
        SystemTime::UNIX_EPOCH,
        HashUpdate::Hash(Snapshot {
            fsize,
            mtime: mtime.clone(),
            hash: hash.clone(),
        }),
    )?;

    let got = fh::dump(cnx.conn())?;
    assert_eq!(
        got,
        vec![
            (
                a.join("f1"),
                File {
                    tree_gen: 1,
                    state: State::Dirty(Dirty {
                        next: now.into(),
                        fsize,
                        mtime: mtime.into(),
                        hash: hash.as_ref().to_owned(),
                        egroup: egroup.clone().into(),
                    })
                }
            ),
            (
                a.join("f2"),
                File {
                    tree_gen: 1,
                    state: State::Dirty(Dirty {
                        next: now.into(),
                        fsize,
                        mtime: mtime.into(),
                        hash: hash.as_ref().to_owned(),
                        egroup: egroup.into(),
                    })
                }
            )
        ]
    );
    Ok(())
}

#[test]
fn metadata_update_adds_file() -> anyhow::Result<()> {
    let mut cnx = Connection::new_in_memory(Arc::new(FakeRandom::new(vec![])))?;
    let a = Path::new("a");

    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(321);
    // We can drop random metadata for a file, and it'll be added as New right
    // away.
    cnx.metadata_update(a.to_owned(), now, 100, SystemTime::UNIX_EPOCH)?;

    let got = fh::dump(cnx.conn())?;
    assert_eq!(
        got,
        vec![(
            a.to_owned(),
            File {
                tree_gen: 0,
                state: State::New(New { next: now.into() })
            }
        )]
    );
    Ok(())
}

#[test]
fn full_cycle() -> anyhow::Result<()> {
    // Take a file through all its positiev states (not including unreadable).

    let egroup = fileid(1);
    let mut cnx = Connection::new_in_memory(Arc::new(FakeRandom::new(vec![egroup.clone()])))?;

    let root = Path::new("root");
    cnx.set_roots(&[&root])?;

    let now = UNIX_EPOCH;
    let soon = now + Duration::from_secs(100);
    let next = now + Duration::from_secs(1000);

    cnx.tree_scan_start(next)?;

    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    let f1: OsString = "f1".into();
    let f1_modtime = now + Duration::from_secs(231);
    updater.update(&ScanUpdate::File(f1, now, 100, f1_modtime))?;
    updater.commit(true)?;

    let (to_hash, _) = cnx.hash_next(next)?.next()?;
    assert_eq!(&to_hash, Path::new("root/f1"));

    let hash = digest::digest(&digest::SHA256, b"boo");
    let snapshot = Snapshot {
        fsize: 100,
        mtime: f1_modtime,
        hash: hash.clone(),
    };
    cnx.hash_update(to_hash, next, soon, HashUpdate::Hash(snapshot.clone()))?;

    // The file was hashed, and is expected to be ready to be backed up.
    let Next::Next(to_backup, bk_egroup) = cnx.backup_next(soon)? else {
        panic!("unexepected");
    };
    assert_eq!(bk_egroup, egroup);

    cnx.backup_start(to_backup.clone())?;

    assert_eq!(
        cnx.backup_pending()?,
        vec![(Path::new("root/f1").to_owned(), egroup)]
    );

    cnx.backup_done(to_backup, next, HashUpdate::Hash(snapshot.clone()))?;

    Ok(())
}

#[test]
fn hash_update_progession() -> anyhow::Result<()> {
    let egroup = fileid(1);
    let mut cnx = Connection::new_in_memory(Arc::new(FakeRandom::new(vec![egroup.clone()])))?;

    let root = Path::new("root");
    cnx.set_roots(&[&root])?;

    let now = UNIX_EPOCH;
    let soon = now + Duration::from_secs(100);
    let next = now + Duration::from_secs(1000);

    cnx.tree_scan_start(next)?;

    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    let f1: OsString = "f1".into();
    let f1_modtime = now + Duration::from_secs(231);
    updater.update(&ScanUpdate::File(f1, now, 100, f1_modtime))?;
    updater.commit(true)?;

    // File is ready to hash as of now.
    let (path, _) = cnx.hash_next(now)?.next()?;
    // The first hashing will necessarily make it Dirty for backup after |soon|.
    let hash = digest::digest(&digest::SHA256, b"boo");
    let snapshot = Snapshot {
        fsize: 100,
        mtime: f1_modtime,
        hash: hash.clone(),
    };
    cnx.hash_update(path, next, soon, HashUpdate::Hash(snapshot.clone()))?;

    // Go through the motions of backup right now. This gets us a clean file.
    let (path, _) = cnx.backup_next(soon)?.next()?;
    cnx.backup_start(path.clone())?;
    cnx.backup_done(path, next, HashUpdate::Hash(snapshot.clone()))?;

    let now = next;
    let soon = now + Duration::from_secs(100);
    let next = now + Duration::from_secs(1000);

    // If we hash the file and find no difference, we expect to re-hash only at |next|.
    let (path, _) = cnx.hash_next(now)?.next()?;
    cnx.hash_update(path, next, soon, HashUpdate::Hash(snapshot.clone()))?;

    let Next::Done(hash_next) = cnx.hash_next(now)? else {
        bail!("invalid state");
    };
    assert_eq!(hash_next, next);

    let now = next;
    let soon = now + Duration::from_secs(100);
    let next = now + Duration::from_secs(1000);

    let (path, _) = cnx.hash_next(now)?.next()?;
    let hash = digest::digest(&digest::SHA256, b"boo again");
    let snapshot = Snapshot {
        fsize: 100,
        mtime: f1_modtime,
        hash: hash.clone(),
    };
    cnx.hash_update(path, next, soon, HashUpdate::Hash(snapshot.clone()))?;
    cnx.backup_next(soon)?.next()?;

    Ok(())
}

#[test]
fn migrations_test() {
    assert!(Connection::migrations().validate().is_ok());
}

// ===================== HELPERS =======================

fn fileid(b: u8) -> EncryptionGroup {
    let bytes: [u8; crypto::model::ENCRYPTION_GROUP_LEN] = [b, 0, 0, 0, 0, 0];
    crypto::model::EncryptionGroup::try_from(bytes.as_ref()).unwrap()
}

struct FakeRandom {
    next: Arc<Mutex<VecDeque<EncryptionGroup>>>,
}

impl FakeRandom {
    fn new(next: Vec<EncryptionGroup>) -> FakeRandom {
        let ids = next.into_iter().collect();
        FakeRandom {
            next: Arc::new(Mutex::new(ids)),
        }
    }
}

impl crypto::RandomApi for FakeRandom {
    fn generate_file_id(&self) -> anyhow::Result<EncryptionGroup> {
        self.next.lock().unwrap().pop_front().context("no id left")
    }

    fn generate_block_id(&self) -> anyhow::Result<crypto::model::BlockId> {
        todo!()
    }
}
