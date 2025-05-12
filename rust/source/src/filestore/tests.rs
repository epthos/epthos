use super::{directory::pth as dh, file::pth as fh, *};
use crate::filestore::file::File;
use anyhow::bail;
use crypto::model::EncryptionGroup;
use ring::digest;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
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

    let ScanNext::Done(next) = cnx.tree_scan_next()? else {
        bail!("unexpected");
    };
    assert_eq!(next, SystemTime::UNIX_EPOCH); // Not initiated yet.

    let next_time = SystemTime::UNIX_EPOCH + Duration::from_secs(86400);
    cnx.tree_scan_start(next_time)?;

    let (dir, mut updater) = updater_or(cnx.tree_scan_next()?)?;
    assert_eq!(dir, a);

    let f1: OsString = "f1".into();
    let f1_modtime = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
    updater.update(&ScanUpdate::File(f1, 100, f1_modtime))?;
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
                next_hash: SystemTime::UNIX_EPOCH.into(),
                state: State::New(New {})
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

    let (_, mut updater) = updater_or(cnx.tree_scan_next()?)?;
    let f1_mod_1 = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let f2_mod_1 = SystemTime::UNIX_EPOCH + Duration::from_secs(200);
    updater.update(&ScanUpdate::File("f1".into(), 1, f1_mod_1))?;
    updater.update(&ScanUpdate::File("f2".into(), 2, f2_mod_1))?;
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
    let f1_mod_2 = SystemTime::UNIX_EPOCH + Duration::from_secs(300);
    let f3_mod_1 = SystemTime::UNIX_EPOCH + Duration::from_secs(400);
    updater.update(&ScanUpdate::File("f1".into(), 3, f1_mod_2))?;
    updater.update(&ScanUpdate::File("f3".into(), 4, f3_mod_1))?;
    updater.update(&ScanUpdate::Directory("d1".into()))?;
    updater.update(&ScanUpdate::Directory("d3".into()))?;
    updater.commit(true)?;

    let (_, updater) = updater_or(cnx.tree_scan_next()?)?;
    updater.commit(true)?;

    let (_, updater) = updater_or(cnx.tree_scan_next()?)?;
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
                File {
                    tree_gen: 2,
                    next_hash: SystemTime::UNIX_EPOCH.into(),
                    state: State::New(New {}),
                }
            ),
            (
                a.join("f2"),
                File {
                    tree_gen: 1,
                    next_hash: SystemTime::UNIX_EPOCH.into(),
                    state: State::New(New {}),
                }
            ),
            (
                a.join("f3"),
                File {
                    tree_gen: 2,
                    next_hash: SystemTime::UNIX_EPOCH.into(),
                    state: State::New(New {}),
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
        let (_, mut updater) = updater_or(cnx.tree_scan_next()?)?;
        updater.update(&ScanUpdate::File("b".into(), 1, SystemTime::UNIX_EPOCH))?;
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
    let (_, updater) = updater_or(cnx.tree_scan_next()?)?;
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
    let (_, updater) = updater_or(cnx.tree_scan_next()?)?;
    updater.commit(true)?;

    let ScanNext::Done(next) = cnx.tree_scan_next()? else {
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
    let (_, mut updater) = updater_or(cnx.tree_scan_next()?)?;
    let b_mod = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
    updater.update(&ScanUpdate::File(b.clone(), 1, b_mod))?;
    updater.commit(true)?;

    cnx.tree_scan_start(SystemTime::UNIX_EPOCH)?;

    // Make a/b a directory next.
    let (_, mut updater) = updater_or(cnx.tree_scan_next()?)?;
    updater.update(&ScanUpdate::Directory(b.clone()))?;
    updater.commit(true)?;

    let (_, updater) = updater_or(cnx.tree_scan_next()?)?;
    updater.commit(true)?;

    // "a/b" is currently a directory, so the file gen is stuck at 1.
    let got = fh::dump(cnx.conn())?;
    assert_eq!(
        got,
        vec![(
            a.join("b"),
            File {
                tree_gen: 1,
                next_hash: SystemTime::UNIX_EPOCH.into(),
                state: State::New(New {})
            }
        )]
    );

    cnx.tree_scan_start(SystemTime::UNIX_EPOCH)?;

    let (_, mut updater) = updater_or(cnx.tree_scan_next()?)?;
    updater.update(&ScanUpdate::File(b.clone(), 1, b_mod))?;
    updater.commit(true)?;

    // The file is back on the scan.
    let got = fh::dump(cnx.conn())?;
    assert_eq!(
        got,
        vec![(
            a.join("b"),
            File {
                tree_gen: 3,
                next_hash: SystemTime::UNIX_EPOCH.into(),
                state: State::New(New {})
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
    updater.update(&ScanUpdate::File("f1".into(), 1, SystemTime::UNIX_EPOCH))?;
    updater.commit(true)?;

    // From now on, file a/f1 is in the database, but with no hash yet.
    let file = hasher_or(cnx.hash_next(now)?)?;
    assert_eq!(file, a.join("f1"));

    let file = hasher_or(cnx.hash_next(now)?)?;
    assert_eq!(file, a.join("f1"));

    cnx.hash_update(
        file,
        now + delta,
        HashUpdate::Hash(Snapshot {
            fsize: 100,
            mtime: SystemTime::UNIX_EPOCH + Duration::from_secs(3600),
            hash: digest::digest(&digest::SHA256, b"boo"),
        }),
    )?;

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

fn updater_or<U: Scanner>(next: ScanNext<U>) -> anyhow::Result<(PathBuf, U)> {
    match next {
        ScanNext::Done(_) => bail!("no next directory"),
        ScanNext::Next(dir, updater) => Ok((dir, updater)),
    }
}

fn hasher_or(next: HashNext) -> anyhow::Result<PathBuf> {
    match next {
        HashNext::Done(_) => bail!("no next file to hash"),
        HashNext::Next(file) => Ok(file),
    }
}

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
