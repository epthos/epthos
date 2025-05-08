use anyhow::anyhow;
use crypto::model::EncryptionGroup;
use platform::LocalPathRepr;
use rusqlite::types::{self, FromSql, FromSqlError, ToSql, ValueRef};
use std::{
    ops::Deref,
    path::PathBuf,
    time::{Duration, SystemTime},
};

/// States a file will go through.
///
/// Careful, the enum values are stored permanently. Do not change.
#[derive(Debug, PartialEq, Clone)]
pub enum FileState {
    // A file is new when it does not yet have a filegroup assigned.
    NEW = 1,
    // A file is dirty if some of its properties changed: size, mtime, hash.
    DIRTY = 2,
    // Many files can be dirty but can't be backed up all at once. A busy file
    // is being actively backed up.
    BUSY = 3,
    // Once backed up, a file's size, mtime and hash are refreshed and the file
    // is now clean.
    CLEAN = 4,
    // The file cannot be read or stat'd.
    UNAVAILABLE = 5,
}

// A wrapper around LocalPathRepr that is rusqlite-friendly.
#[derive(Debug, PartialEq)]
pub struct LocalPath(LocalPathRepr);

// A wrapper around a SystemTime that will store it in seconds.
#[derive(Debug, PartialEq)]
pub struct TimeInSeconds(SystemTime);

// A wrapper around a SystemTime that will store it in microseconds.
#[derive(Debug, PartialEq)]
pub struct TimeInMicroseconds(SystemTime);

// A wrapper around an EncryptionGroup
#[derive(Debug, PartialEq)]
pub struct StoredEncryptionGroup(EncryptionGroup);

// ------ Make rusqlite-friendly types -----

impl FromSql for TimeInSeconds {
    fn column_result(value: types::ValueRef<'_>) -> types::FromSqlResult<Self> {
        let types::ValueRef::Integer(seconds) = value else {
            return Err(FromSqlError::InvalidType);
        };
        let time = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(seconds as u64))
            .ok_or_else(|| FromSqlError::Other(anyhow!("can't recreate time").into()))?;
        Ok(TimeInSeconds { 0: time })
    }
}

impl ToSql for TimeInSeconds {
    fn to_sql(&self) -> rusqlite::Result<types::ToSqlOutput<'_>> {
        let duration = self
            .0
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(e.into()))?;

        Ok(types::ToSqlOutput::Owned(types::Value::Integer(
            duration.as_secs() as i64,
        )))
    }
}

impl From<SystemTime> for TimeInSeconds {
    fn from(value: SystemTime) -> Self {
        TimeInSeconds { 0: value }
    }
}

impl Deref for TimeInSeconds {
    type Target = SystemTime;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// -----------------

impl FromSql for TimeInMicroseconds {
    fn column_result(value: types::ValueRef<'_>) -> types::FromSqlResult<Self> {
        let types::ValueRef::Integer(usecs) = value else {
            return Err(FromSqlError::InvalidType);
        };
        let time = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_micros(usecs as u64))
            .ok_or_else(|| FromSqlError::Other(anyhow!("can't recreate time").into()))?;
        Ok(TimeInMicroseconds { 0: time })
    }
}

impl ToSql for TimeInMicroseconds {
    fn to_sql(&self) -> rusqlite::Result<types::ToSqlOutput<'_>> {
        let duration = self
            .0
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(e.into()))?;

        Ok(types::ToSqlOutput::Owned(types::Value::Integer(
            duration.as_micros() as i64,
        )))
    }
}

impl From<SystemTime> for TimeInMicroseconds {
    fn from(value: SystemTime) -> Self {
        TimeInMicroseconds { 0: value }
    }
}

impl Deref for TimeInMicroseconds {
    type Target = SystemTime;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// -----------------

impl FromSql for FileState {
    fn column_result(value: types::ValueRef<'_>) -> types::FromSqlResult<Self> {
        match value {
            types::ValueRef::Null => Err(FromSqlError::InvalidType),
            types::ValueRef::Integer(1) => Ok(FileState::NEW),
            types::ValueRef::Integer(2) => Ok(FileState::DIRTY),
            types::ValueRef::Integer(3) => Ok(FileState::BUSY),
            types::ValueRef::Integer(4) => Ok(FileState::CLEAN),
            types::ValueRef::Integer(5) => Ok(FileState::UNAVAILABLE),
            types::ValueRef::Integer(v) => Err(FromSqlError::OutOfRange(v)),
            types::ValueRef::Real(_) => Err(FromSqlError::InvalidType),
            types::ValueRef::Text(_) => Err(FromSqlError::InvalidType),
            types::ValueRef::Blob(_) => Err(FromSqlError::InvalidType),
        }
    }
}

impl ToSql for FileState {
    fn to_sql(&self) -> rusqlite::Result<types::ToSqlOutput<'_>> {
        Ok(types::ToSqlOutput::Owned(types::Value::Integer(
            (*self).clone() as i64,
        )))
    }
}

// -----------------

impl FromSql for LocalPath {
    fn column_result(value: types::ValueRef<'_>) -> types::FromSqlResult<Self> {
        let types::ValueRef::Blob(blob) = value else {
            return Err(FromSqlError::InvalidType);
        };
        Ok(LocalPath {
            0: LocalPathRepr::new(blob.to_vec()),
        })
    }
}

impl ToSql for LocalPath {
    fn to_sql(&self) -> rusqlite::Result<types::ToSqlOutput<'_>> {
        Ok(types::ToSqlOutput::Borrowed(types::ValueRef::Blob(
            self.0.as_ref(),
        )))
    }
}

impl From<PathBuf> for LocalPath {
    fn from(value: PathBuf) -> Self {
        let local: LocalPathRepr = value.into();
        local.into()
    }
}

impl TryFrom<LocalPath> for PathBuf {
    type Error = FromSqlError;

    fn try_from(value: LocalPath) -> Result<Self, Self::Error> {
        value
            .0
            .try_into()
            .map_err(|e| FromSqlError::Other(Box::new(e)))
    }
}

impl From<LocalPathRepr> for LocalPath {
    fn from(path: LocalPathRepr) -> Self {
        LocalPath { 0: path }
    }
}

impl Deref for LocalPath {
    type Target = LocalPathRepr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// --------------

impl FromSql for StoredEncryptionGroup {
    fn column_result(value: types::ValueRef<'_>) -> types::FromSqlResult<Self> {
        let types::ValueRef::Blob(blob) = value else {
            return Err(FromSqlError::InvalidType);
        };

        let eg: EncryptionGroup = blob
            .try_into()
            .map_err(|e: anyhow::Error| FromSqlError::Other(e.into()))?;
        Ok(StoredEncryptionGroup { 0: eg })
    }
}

impl ToSql for StoredEncryptionGroup {
    fn to_sql(&self) -> rusqlite::Result<types::ToSqlOutput<'_>> {
        Ok(types::ToSqlOutput::Borrowed(ValueRef::Blob(
            self.0.as_bytes(),
        )))
    }
}

impl From<EncryptionGroup> for StoredEncryptionGroup {
    fn from(value: EncryptionGroup) -> Self {
        StoredEncryptionGroup { 0: value }
    }
}

impl Deref for StoredEncryptionGroup {
    type Target = EncryptionGroup;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Context;
    use types::{FromSql, ToSql, ToSqlOutput};

    use super::*;

    #[test]
    fn state_is_reversible() -> anyhow::Result<()> {
        for v in [
            FileState::NEW,
            FileState::DIRTY,
            FileState::BUSY,
            FileState::CLEAN,
            FileState::UNAVAILABLE,
        ] {
            let ToSqlOutput::Owned(repr) = v.to_sql().context("to_sql")? else {
                panic!("unexpected")
            };
            let back = FileState::column_result((&repr).into()).context("parse")?;

            assert_eq!(back, v);
        }
        Ok(())
    }

    #[test]
    fn detect_invalid_state() -> anyhow::Result<()> {
        let invalid = types::ValueRef::Integer(33);
        assert!(FileState::column_result(invalid).is_err());
        Ok(())
    }

    #[test]
    fn path_is_reversible() -> anyhow::Result<()> {
        let path: LocalPath = PathBuf::from("foo/bar").into();
        let ToSqlOutput::Borrowed(repr) = path.to_sql().context("to_sql")? else {
            panic!("unexpected")
        };
        let back = LocalPath::column_result(repr).context("parse")?;
        assert_eq!(path, back);
        Ok(())
    }

    #[test]
    fn time_in_seconds() -> anyhow::Result<()> {
        let moment = SystemTime::UNIX_EPOCH + Duration::from_secs(12);
        let ts: TimeInSeconds = moment.into();

        let ToSqlOutput::Owned(repr) = ts.to_sql().context("to_sql")? else {
            panic!("unexpected");
        };
        let back = TimeInSeconds::column_result((&repr).into()).context("result")?;
        assert_eq!(back, ts);
        // In this case, the actual representation matters as well, as we care about
        // the scale.
        let types::Value::Integer(duration) = repr else {
            panic!("unexpected");
        };
        assert_eq!(duration, 12);
        Ok(())
    }

    #[test]
    fn time_in_microseconds() -> anyhow::Result<()> {
        let moment = SystemTime::UNIX_EPOCH + Duration::from_secs(12);
        let ts: TimeInMicroseconds = moment.into();

        let ToSqlOutput::Owned(repr) = ts.to_sql().context("to_sql")? else {
            panic!("unexpected");
        };
        let back = TimeInMicroseconds::column_result((&repr).into()).context("result")?;
        assert_eq!(back, ts);
        // In this case, the actual representation matters as well, as we care about
        // the scale.
        let types::Value::Integer(duration) = repr else {
            panic!("unexpected");
        };
        assert_eq!(duration, 12000000);
        Ok(())
    }

    #[test]
    fn encryption_group_is_reversible() -> anyhow::Result<()> {
        let eg =
            StoredEncryptionGroup::from(EncryptionGroup::try_from([1, 2, 3, 4, 5, 6].as_slice())?);

        let ToSqlOutput::Borrowed(repr) = eg.to_sql().context("to_sql")? else {
            panic!("unexpected");
        };
        let back = StoredEncryptionGroup::column_result(repr).context("result")?;
        assert_eq!(back, eg);
        Ok(())
    }
}
