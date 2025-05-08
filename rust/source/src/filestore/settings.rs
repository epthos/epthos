use super::field::TimeInSeconds;
use anyhow::anyhow;
use rusqlite::OptionalExtension;
use rusqlite_migration::M;
use std::time::SystemTime;

pub const SQL: M<'_> = M::up(
    r#"
-- Series of key/value pairs.
CREATE TABLE Settings (
    k  TEXT PRIMARY KEY,
    i  INTEGER,
    s  TEXT,
    ts INTEGER  -- timestamp in seconds since EPOCH.
) STRICT, WITHOUT ROWID;
"#,
);

/// A single Setting value.
#[derive(Debug, PartialEq)]
pub enum Setting {
    N(i64),
    T(String),
    Ts(SystemTime),
}

/// Set a setting for a given key.
pub fn set(txn: &rusqlite::Transaction, key: &str, value: &Setting) -> anyhow::Result<()> {
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
            let ts: TimeInSeconds = (*ts).into();
            txn.execute(
                "INSERT OR REPLACE INTO Settings (k, i, s, ts) VALUES (?1, NULL, NULL, ?2)",
                (key, &ts),
            )?;
        }
    }
    Ok(())
}

/// Get a setting for a key.
pub fn get(txn: &rusqlite::Transaction, key: &str) -> anyhow::Result<Option<Setting>> {
    let result = txn
        .query_row(
            "SELECT i, s, ts FROM Settings WHERE k = ?1",
            (key,),
            |row| {
                let i: Option<i64> = row.get(0)?;
                let s: Option<String> = row.get(1)?;
                let ts: Option<TimeInSeconds> = row.get(2)?;
                if let Some(i) = i {
                    return Ok(Setting::N(i));
                }
                if let Some(s) = s {
                    return Ok(Setting::T(s));
                }
                if let Some(ts) = ts {
                    return Ok(Setting::Ts(*ts));
                }
                Err(rusqlite::Error::ToSqlConversionFailure(
                    anyhow!("Setting has no value set").into(),
                ))
            },
        )
        .optional()?;
    Ok(result)
}

pub fn get_int(txn: &rusqlite::Transaction, key: &str) -> anyhow::Result<Option<i64>> {
    match get(txn, key)? {
        Some(Setting::N(i)) => Ok(Some(i)),
        Some(_) => Err(anyhow!("key does not hold an int")),
        None => Ok(None),
    }
}

pub fn get_timestamp(txn: &rusqlite::Transaction, key: &str) -> anyhow::Result<Option<SystemTime>> {
    match get(txn, key)? {
        Some(Setting::Ts(ts)) => Ok(Some(ts)),
        Some(_) => Err(anyhow!("key does not hold a timestamp")),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use rusqlite_migration::Migrations;
    use std::time::Duration;

    fn db() -> anyhow::Result<rusqlite::Connection> {
        let mut db = rusqlite::Connection::open_in_memory().context("open_in_memory")?;
        Migrations::new(vec![SQL])
            .to_latest(&mut db)
            .context("to_latest")?;
        Ok(db)
    }

    #[test]
    fn manipulate_settings() -> anyhow::Result<()> {
        let mut db = db()?;
        let txn = db.transaction()?;

        assert_eq!(get(&txn, "a")?, None);

        let now = SystemTime::UNIX_EPOCH + Duration::new(3600, 0);
        set(&txn, "a", &Setting::Ts(now))?;
        assert_eq!(get(&txn, "a")?, Some(Setting::Ts(now)));

        set(&txn, "a", &Setting::N(123))?;
        assert_eq!(get(&txn, "a")?, Some(Setting::N(123)));

        set(&txn, "a", &Setting::T("foo".into()))?;
        assert_eq!(get(&txn, "a")?, Some(Setting::T("foo".into())));

        Ok(())
    }
}
