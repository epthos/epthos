use super::{directory::pth as dh, field::TimeInSeconds, file::pth as fh, *};
use crate::{filestore::file::File, model::FileHash};
use anyhow::bail;
use crypto::model::EncryptionGroup;
use ring::digest;
use std::{
    cmp::max,
    collections::{HashMap, HashSet, VecDeque},
    ffi::OsString,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use test_log::test;

#[test]
fn add_and_remove_roots() -> anyhow::Result<()> {
    let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()), Timing::default())?;
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
fn tree_scan_sequence() -> anyhow::Result<()> {
    // Let's validate that scans are scheduled as intended, and progress
    // correctly.
    let mut timing = Timing::default();
    timing.spread = 0.0; // Deterministic timing.
    let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()), timing.clone())?;
    let a = Path::new("a");
    let b = Path::new("b");
    cnx.set_roots(&[a, b])?;

    let Next::Done(next) = cnx.tree_scan_next()? else {
        bail!("unexpected");
    };
    assert_eq!(next, t(0)); // Not initialized yet: let's scan right away.

    let now = t(1);
    cnx.tree_scan_start(now)?;

    let (dir, mut updater) = cnx.tree_scan_next()?.next()?;
    assert_eq!(dir, a);

    let hash_time = t(3210);
    let f1: OsString = "f1".into();
    let f1_modtime = t(1000);
    updater.update(hash_time, &ScanEntry::File(f1, 100, f1_modtime))?;
    updater.commit(true)?;

    // Move on to the next directory.
    let (dir, mut updater) = cnx.tree_scan_next()?.next()?;
    assert_eq!(dir, b);

    let e: OsString = "e".into();
    updater.update(hash_time, &ScanEntry::Directory(e))?;
    updater.commit(true)?;

    let (dir, updater) = cnx.tree_scan_next()?.next()?;
    assert_eq!(dir, b.join("e"));

    updater.commit(true)?;

    let Next::Done(next) = cnx.tree_scan_next()? else {
        bail!("unexpected");
    };
    assert_eq!(next, now + timing.scan_period);

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
                state: State::New(New {
                    next: hash_time.into()
                })
            }
        )]
    );
    Ok(())
}

#[test]
fn tree_rescan() -> anyhow::Result<()> {
    // Validate how files' next and gen evolve during successive scans.
    let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()), Timing::default())?;
    let a = Path::new("a");
    cnx.set_roots(&[a])?;

    let f1 = a.join("f1");
    let f2 = a.join("f2");
    let d1 = a.join("d1");
    let d2 = a.join("d2");

    let t1 = secs(1000);
    db_setup(
        cnx.conn(),
        vec![HashMap::from([
            (f1, State::New(New { next: t1 })),
            (f2, State::New(New { next: t1 })),
        ])],
        vec![HashSet::from([d1, d2])],
    )?;

    cnx.tree_scan_start(t(86400))?;

    let t2 = t(2000);

    // f2 disappeared, f3 appeared.
    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    updater.update(t2, &ScanEntry::File("f1".into(), 3, t(300)))?;
    updater.update(t2, &ScanEntry::File("f3".into(), 4, t(400)))?;
    updater.update(t2, &ScanEntry::Directory("d1".into()))?;
    updater.update(t2, &ScanEntry::Directory("d3".into()))?;
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
                    state: State::New(New { next: t1 }),
                }
            ),
            (
                a.join("f2"),
                File {
                    tree_gen: 1,
                    state: State::New(New { next: t1 }),
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
    let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()), Timing::default())?;
    let p1 = Path::new("/a");
    cnx.set_roots(&[p1])?;

    cnx.tree_scan_start(t(0))?;
    {
        let (_, mut updater) = cnx.tree_scan_next()?.next()?;
        updater.update(t(100), &ScanEntry::File("b".into(), 1, t(200)))?;
    }
    // Despite the update(File()) above, the lack of commit means the db was not
    // altered.
    assert!(fh::dump(cnx.conn())?.is_empty());

    Ok(())
}

#[test]
fn tree_scan_with_errors() -> anyhow::Result<()> {
    let mut timing = Timing::default();
    timing.spread = 0.0; // Deterministic timing.
    let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()), timing.clone())?;
    let p1 = Path::new("/a/b");
    cnx.set_roots(&[p1])?;

    let mut now = t(1);
    cnx.tree_scan_start(now)?;
    let (_, updater) = cnx.tree_scan_next()?.next()?;
    // Make it unreachable
    updater.error(anyhow::format_err!("boom"))?;
    let mut got: Vec<(PathBuf, Option<String>)> = dh::dump(cnx.conn())?
        .into_iter()
        .map(|d| (d.path, d.error))
        .collect();
    assert_eq!(got.len(), 1);
    let Some(err) = got.remove(0).1 else {
        panic!("unexpected");
    };
    assert!(err.contains("boom"));

    // ...and fix reachability at the next round.
    now = t(2);
    cnx.tree_scan_start(now)?;
    let (_, updater) = cnx.tree_scan_next()?.next()?;
    updater.commit(true)?;

    let Next::Done(next) = cnx.tree_scan_next()? else {
        panic!("unexpected");
    };
    assert_eq!(next, now + timing.scan_period);

    let got: Vec<(PathBuf, Option<String>)> = dh::dump(cnx.conn())?
        .into_iter()
        .map(|d| (d.path, d.error))
        .collect();
    assert_eq!(got, vec![(p1.into(), None)]);

    Ok(())
}

#[test]
fn tree_scan_node_type_change() -> anyhow::Result<()> {
    let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()), Timing::default())?;
    let a = Path::new("a");
    cnx.set_roots(&[a])?;

    let b: OsString = "b".into();

    cnx.tree_scan_start(t(0))?;

    // Make a/b a file first.
    let t1 = t(100);
    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    updater.update(t1, &ScanEntry::File(b.clone(), 1, t(1)))?;
    updater.commit(true)?;

    cnx.tree_scan_start(SystemTime::UNIX_EPOCH)?;

    // Make a/b a directory next.
    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    updater.update(t1, &ScanEntry::Directory(b.clone()))?;
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

    cnx.tree_scan_start(t(0))?;

    let t2 = t(200);
    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    updater.update(t2, &ScanEntry::File(b.clone(), 1, t(1)))?;
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
    let egroup = egroup(1);
    let timing = Timing::default();
    let mut cnx = Connection::new_in_memory(
        Arc::new(FakeRandom::new(vec![egroup.clone()])),
        timing.clone(),
    )?;

    let delta = Duration::from_secs(10);
    let hash_time = t(0) + delta;

    // Initially, no file is known so no hash is requested.
    let Next::Done(next) = cnx.hash_next(hash_time)? else {
        panic!("unexpected")
    };
    assert_eq!(next, hash_time + FILE_NEXT_DELAY); // Arbitrary delay until we get files.

    let a = Path::new("a");
    cnx.set_roots(&[a])?;

    cnx.tree_scan_start(t(86400))?;
    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    updater.update(hash_time, &ScanEntry::File("f1".into(), 1, t(10)))?;
    updater.commit(true)?;

    // From now on, file a/f1 is in the database, but with no hash yet.
    //
    // First try to get the hash a bit too soon:
    let Next::Done(next) = cnx.hash_next(t(0))? else {
        panic!("unexpected")
    };
    assert_eq!(next, hash_time);

    let f1 = a.join("f1");
    let (file, _) = cnx.hash_next(hash_time)?.next()?;
    assert_eq!(&file, &f1);

    // THe file must be hashed before progress is made.
    let (file, _) = cnx.hash_next(hash_time)?.next()?;
    assert_eq!(&file, &f1);

    let fsize = 100;
    let mtime = t(3600);
    let hash: FileHash = digest::digest(&digest::SHA256, b"boo").into();
    cnx.hash_update(
        file,
        hash_time,
        HashUpdate::Hash(Snapshot {
            fsize,
            mtime,
            hash: hash.clone(),
        }),
    )?;

    // The file is now dirty, and doesn't need to be hashed. We revert back
    // to arbitrary delay for the next attempt.
    let Next::Done(next) = cnx.hash_next(hash_time)? else {
        panic!("unexpected")
    };
    assert_eq!(next, hash_time + FILE_NEXT_DELAY);

    // Confirm that the file is now dirty as intended.
    let got = fh::dump(cnx.conn())?;
    assert_eq!(
        got,
        vec![(
            f1,
            File {
                tree_gen: 1,
                state: State::Dirty(Dirty {
                    next: (hash_time + timing.cool_off_period.0).into(),
                    threshold: (hash_time + timing.cool_off_period.1).into(),
                    fsize,
                    mtime: mtime.into(),
                    hash: hash.into(),
                    egroup: egroup.into(),
                })
            }
        )]
    );

    Ok(())
}

#[test]
fn small_files_dont_share_egroups() -> anyhow::Result<()> {
    let hash_time = t(100);

    let timing = Timing::default();
    let eg1 = egroup(1);
    let eg2 = egroup(2);
    let mut cnx = Connection::new_in_memory(
        Arc::new(FakeRandom::new(vec![eg1.clone(), eg2.clone()])),
        timing.clone(),
    )?;
    let a = Path::new("a");
    cnx.set_roots(&[a])?;
    cnx.tree_scan_start(t(0))?;
    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    updater.update(hash_time, &ScanEntry::File("f1".into(), 1, t(0)))?;
    updater.update(hash_time, &ScanEntry::File("f2".into(), 1, t(0)))?;
    updater.commit(true)?;

    let fsize = 100;
    let mtime = t(3600);
    let hash: FileHash = digest::digest(&digest::SHA256, b"boo").into();
    cnx.hash_update(
        a.join("f1"),
        hash_time,
        HashUpdate::Hash(Snapshot {
            fsize,
            mtime,
            hash: hash.clone(),
        }),
    )?;
    cnx.hash_update(
        a.join("f2"),
        hash_time,
        HashUpdate::Hash(Snapshot {
            fsize,
            mtime,
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
                        next: (hash_time + timing.cool_off_period.0).into(),
                        threshold: (hash_time + timing.cool_off_period.1).into(),
                        fsize,
                        mtime: mtime.into(),
                        hash: hash.clone().into(),
                        egroup: eg1.into(),
                    })
                }
            ),
            (
                a.join("f2"),
                File {
                    tree_gen: 1,
                    state: State::Dirty(Dirty {
                        next: (hash_time + timing.cool_off_period.0).into(),
                        threshold: (hash_time + timing.cool_off_period.1).into(),
                        fsize,
                        mtime: mtime.into(),
                        hash: hash.clone().into(),
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
    let egroup = egroup(1);
    let timing = Timing::default();
    let mut cnx = Connection::new_in_memory(
        Arc::new(FakeRandom::new(vec![egroup.clone()])),
        timing.clone(),
    )?;
    let a = Path::new("a");
    cnx.set_roots(&[a])?;
    cnx.tree_scan_start(t(0))?;
    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    updater.update(t(0), &ScanEntry::File("f1".into(), 1, t(0)))?;
    updater.update(t(0), &ScanEntry::File("f2".into(), 1, t(0)))?;
    updater.commit(true)?;

    let hash_time = t(100);
    let fsize = 2 * SMALLEST_INDEPENDENT_FILE;
    let mtime = t(3600);
    let hash: FileHash = digest::digest(&digest::SHA256, b"boo").into();
    cnx.hash_update(
        a.join("f1"),
        hash_time,
        HashUpdate::Hash(Snapshot {
            fsize,
            mtime,
            hash: hash.clone(),
        }),
    )?;
    cnx.hash_update(
        a.join("f2"),
        hash_time,
        HashUpdate::Hash(Snapshot {
            fsize,
            mtime,
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
                        next: (hash_time + timing.cool_off_period.0).into(),
                        threshold: (hash_time + timing.cool_off_period.1).into(),
                        fsize,
                        mtime: mtime.into(),
                        hash: hash.clone().into(),
                        egroup: egroup.clone().into(),
                    })
                }
            ),
            (
                a.join("f2"),
                File {
                    tree_gen: 1,
                    state: State::Dirty(Dirty {
                        next: (hash_time + timing.cool_off_period.0).into(),
                        threshold: (hash_time + timing.cool_off_period.1).into(),
                        fsize,
                        mtime: mtime.into(),
                        hash: hash.clone().into(),
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
    let mut cnx = Connection::new_in_memory(Arc::new(FakeRandom::new(vec![])), Timing::default())?;
    let a = Path::new("a");

    let next = t(321);
    // We can drop random metadata for a file, and it'll be added as New right
    // away.
    cnx.metadata_update(a.to_owned(), next, 100, t(11))?;

    let got = fh::dump(cnx.conn())?;
    assert_eq!(
        got,
        vec![(
            a.to_owned(),
            File {
                tree_gen: 0, // Not scanned yet.
                state: State::New(New { next: next.into() })
            }
        )]
    );
    Ok(())
}

#[test]
fn metadata_always_advance_tree_gen() -> anyhow::Result<()> {
    let mut cnx = Connection::new_in_memory(Arc::new(FakeRandom::new(vec![])), Timing::default())?;

    let root = PathBuf::from("r");
    let file = root.join("new");

    cnx.set_roots(&[&root])?;

    let hash_next = secs(33);
    let last_tree_gen = db_setup(
        cnx.conn(),
        vec![HashMap::from([(
            file.clone(),
            State::New(New { next: hash_next }),
        )])],
        vec![],
    )?;

    let scan_next = t(100);
    cnx.tree_scan_start(t(200))?;
    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    let f1: OsString = "new".into();
    updater.update(scan_next, &ScanEntry::File(f1, 100, t(321)))?;
    updater.commit(true)?;

    let got = fh::dump(cnx.conn())?;
    assert_eq!(
        got,
        vec![(
            file,
            File {
                tree_gen: last_tree_gen + 1,
                state: State::New(New { next: hash_next })
            }
        )]
    );
    Ok(())
}

#[test]
fn full_cycle() -> anyhow::Result<()> {
    // Take a file through all its positive states (not including unreadable).
    let timing = Timing::default();
    let egroup = egroup(1);
    let mut cnx = Connection::new_in_memory(
        Arc::new(FakeRandom::new(vec![egroup.clone()])),
        timing.clone(),
    )?;

    let root = Path::new("root");
    cnx.set_roots(&[&root])?;

    let mut now = t(1);

    cnx.tree_scan_start(now)?;

    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    let f1: OsString = "f1".into();
    let f1_modtime = now + Duration::from_secs(231);
    updater.update(now, &ScanEntry::File(f1, 100, f1_modtime))?;
    updater.commit(true)?;

    let (to_hash, _) = cnx.hash_next(now)?.next()?;
    assert_eq!(&to_hash, Path::new("root/f1"));

    let hash: FileHash = digest::digest(&digest::SHA256, b"boo").into();
    let snapshot = Snapshot {
        fsize: 100,
        mtime: f1_modtime,
        hash: hash.clone(),
    };
    cnx.hash_update(to_hash, now, HashUpdate::Hash(snapshot.clone()))?;

    // The file was hashed, and is expected to be ready to be backed up.
    now += timing.cool_off_period.0;
    let Next::Next(to_backup, bk_egroup) = cnx.backup_next(now)? else {
        panic!("unexepected");
    };
    assert_eq!(bk_egroup, egroup);

    cnx.backup_start(to_backup.clone())?;

    assert_eq!(
        cnx.backup_pending()?,
        vec![(Path::new("root/f1").to_owned(), egroup)]
    );

    cnx.backup_done(to_backup, now, HashUpdate::Hash(snapshot.clone()))?;

    Ok(())
}

#[test]
fn hash_update_progression() -> anyhow::Result<()> {
    let mut timing = Timing::default();
    timing.spread = 0.0; // Make timing deterministic.
    let egroup = egroup(1);
    let mut cnx = Connection::new_in_memory(
        Arc::new(FakeRandom::new(vec![egroup.clone()])),
        timing.clone(),
    )?;

    let root = Path::new("root");
    cnx.set_roots(&[&root])?;

    let mut now = t(1);

    cnx.tree_scan_start(now)?;

    let (_, mut updater) = cnx.tree_scan_next()?.next()?;
    let f1: OsString = "f1".into();
    let f1_modtime = now + Duration::from_secs(231);
    updater.update(now, &ScanEntry::File(f1, 100, f1_modtime))?;
    updater.commit(true)?;

    // File is ready to hash as of now.
    let (path, _) = cnx.hash_next(now)?.next()?;
    // The first hashing will necessarily make it Dirty for backup after |soon|.
    let hash: FileHash = digest::digest(&digest::SHA256, b"boo").into();
    let snapshot = Snapshot {
        fsize: 100,
        mtime: f1_modtime,
        hash: hash.clone(),
    };
    cnx.hash_update(path, now, HashUpdate::Hash(snapshot.clone()))?;

    // Go through the motions of backup right now. This gets us a clean file.
    now += timing.cool_off_period.0;
    let (path, _) = cnx.backup_next(now)?.next()?;
    cnx.backup_start(path.clone())?;
    cnx.backup_done(path, now, HashUpdate::Hash(snapshot.clone()))?;

    // If we hash the file and find no difference, we expect to re-hash only at |next|.
    now += timing.hash_period;
    let (path, _) = cnx.hash_next(now)?.next()?;
    cnx.hash_update(path, now, HashUpdate::Hash(snapshot.clone()))?;

    let Next::Done(hash_next) = cnx.hash_next(now)? else {
        bail!("invalid state");
    };
    assert_eq!(hash_next, now + timing.hash_period);

    now += timing.hash_period;
    let (path, _) = cnx.hash_next(now)?.next()?;
    let hash: FileHash = digest::digest(&digest::SHA256, b"boo again").into();
    let snapshot = Snapshot {
        fsize: 100,
        mtime: f1_modtime,
        hash: hash.clone(),
    };
    cnx.hash_update(path, now, HashUpdate::Hash(snapshot.clone()))?;
    cnx.backup_next(now + timing.cool_off_period.0)?.next()?;

    Ok(())
}

#[test]
fn no_cool_off_with_hash_updates() -> anyhow::Result<()> {
    let timing = Timing::default();
    let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()), timing.clone())?;

    let f = PathBuf::from("f");
    let hash: FileHash = digest::digest(&digest::SHA256, b"boo again").into();
    let initial_state = State::Dirty(Dirty {
        next: secs(1000),
        threshold: secs(2000),
        fsize: 123,
        mtime: usecs(321),
        hash: hash.clone().into(),
        egroup: egroup(1).into(),
    });
    let tree_gen = db_setup(
        cnx.conn(),
        vec![HashMap::from([(f.clone(), initial_state.clone())])],
        vec![],
    )?;

    // Hashing reports a change between next and threshold.
    let snapshot = Snapshot {
        fsize: 456,
        mtime: t(321),
        hash: hash.clone(),
    };
    cnx.hash_update(f.clone(), t(1000), HashUpdate::Hash(snapshot.clone()))?;

    // But hashing is not expected while in state Dirty: no change.
    let got = fh::dump(cnx.conn())?;
    assert_eq!(
        got,
        vec![(
            f,
            File {
                tree_gen: tree_gen,
                state: initial_state,
            }
        )]
    );
    Ok(())
}

#[test]
fn cool_off_with_metadata_update() -> anyhow::Result<()> {
    let mut timing = Timing::default();
    timing.cool_off_period.0 = Duration::from_secs(12);
    let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()), timing.clone())?;

    let f = PathBuf::from("f");
    let hash: FileHash = digest::digest(&digest::SHA256, b"boo again").into();
    let initial_state = Dirty {
        next: secs(1000),
        threshold: secs(2000),
        fsize: 123,
        mtime: usecs(321),
        hash: hash.clone().into(),
        egroup: egroup(1).into(),
    };
    let tree_gen = db_setup(
        cnx.conn(),
        vec![HashMap::from([(
            f.clone(),
            State::Dirty(initial_state.clone()),
        )])],
        vec![],
    )?;

    // Metadata reports a change between next and threshold.
    cnx.metadata_update(f.clone(), t(1000), 123, t(444))?;

    // This causes the backup time to be pushed later than now.
    let mut current_state = initial_state;
    current_state.next = secs(1012); // now + cool_off.0

    let got = fh::dump(cnx.conn())?;
    assert_eq!(
        got,
        vec![(
            f.clone(),
            File {
                tree_gen: tree_gen,
                state: State::Dirty(current_state.clone()),
            }
        )]
    );

    // Metadata reports a change much closer to threshold.
    cnx.metadata_update(f.clone(), t(1999), 123, t(888))?;

    // The new backup time is capped at threshold.
    current_state.next = secs(2000);

    let got = fh::dump(cnx.conn())?;
    assert_eq!(
        got,
        vec![(
            f,
            File {
                tree_gen: tree_gen,
                state: State::Dirty(current_state),
            }
        )]
    );
    Ok(())
}

#[test]
fn compute_stats() -> anyhow::Result<()> {
    let mut cnx = Connection::new_in_memory(Arc::new(crypto::Random::new()), Timing::default())?;
    let a = Path::new("a");
    let f1 = a.join("f1");
    let f2 = a.join("f2");
    let d1 = a.join("d1");

    let t1 = secs(1000);
    // 2 files, 1 directory.
    db_setup(
        cnx.conn(),
        vec![HashMap::from([
            (f1, State::New(New { next: t1 })),
            (f2, State::New(New { next: t1 })),
        ])],
        vec![HashSet::from([d1])],
    )?;

    let stats = cnx.get_stats()?;
    assert_eq!(
        stats,
        Stats {
            total_file_count: 2 // we report files.
        }
    );

    Ok(())
}

#[test]
fn migrations_test() {
    assert!(Connection::migrations().validate().is_ok());
}

// ===================== HELPERS =======================

fn t(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
}

fn secs(secs: u64) -> TimeInSeconds {
    t(secs).into()
}

fn usecs(secs: u64) -> TimeInMicroseconds {
    t(secs).into()
}

/// Sets up the database with provided files and directories.
fn db_setup(
    conn: &mut rusqlite::Connection,
    files: Vec<HashMap<PathBuf, State>>,
    dirs: Vec<HashSet<PathBuf>>,
) -> anyhow::Result<i64> {
    let txn = conn.transaction()?;
    let mut tree_gen = settings::get_int(&txn, "tree-gen")?.unwrap_or(0);
    let largest = max(files.len(), dirs.len());
    for idx in 0..largest {
        tree_gen += 1;
        if idx < files.len() {
            for (path, state) in &files[idx] {
                let path: LocalPath = path.clone().into();
                // set_state() won't insert by design. So we must ensure the file
                // exists first.
                if file::get_state(&txn, &path)?.is_none() {
                    file::new(&txn, &path, tree_gen, &secs(0))?
                }
                file::set_state(&txn, &path, tree_gen, &state)?;
            }
        }
        if idx < dirs.len() {
            for path in &dirs[idx] {
                let path: LocalPath = path.clone().into();
                let count = directory::set_path_aim(&txn, tree_gen, &path)?;
                if count == 0 {
                    directory::insert(&txn, &path, tree_gen)?;
                }
                directory::update_dir_complete(&txn, &path, tree_gen, true)?;
            }
        }
    }
    settings::set(&txn, "tree-gen", &Setting::N(tree_gen))?;
    txn.commit()?;
    Ok(tree_gen)
}

fn egroup(b: u8) -> EncryptionGroup {
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
