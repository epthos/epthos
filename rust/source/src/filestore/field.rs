use anyhow::anyhow;
use crypto::model::EncryptionGroup;
use rusqlite::types::{self, FromSql, FromSqlError, ToSql, ValueRef};
use std::time::{Duration, SystemTime};

use crate::model::{FileHash, HashConversionError};

/// States a file will go through.
///
/// Careful, the enum values are stored permanently. Do not change.
#[derive(Debug, PartialEq, Clone)]
pub enum FileState {
    // A file is new when it does not yet have a filegroup assigned.
    New = 1,
    // A file is dirty if some of its properties changed: size, mtime, hash.
    Dirty = 2,
    // Many files can be dirty but can't be backed up all at once. A busy file
    // is being actively backed up.
    Busy = 3,
    // Once backed up, a file's size, mtime and hash are refreshed and the file
    // is now clean.
    Clean = 4,
    // The file cannot be read or stat'd.
    Unreadable = 5,
}

// A wrapper around a SystemTime that will store it in seconds.
#[derive(PartialEq, Copy, Clone)]
pub struct TimeInSeconds(pub SystemTime);

// A wrapper around a SystemTime that will store it in microseconds.
#[derive(PartialEq, Copy, Clone)]
pub struct TimeInMicroseconds(pub SystemTime);

// A wrapper around an EncryptionGroup
#[derive(PartialEq, Clone)]
pub struct StoredEncryptionGroup(EncryptionGroup);

// A wrapper around a FileHash.
#[derive(Debug, PartialEq, Clone)]
pub struct StoredFileHash(FileHash);

// ------ Make rusqlite-friendly types -----

impl FromSql for TimeInSeconds {
    fn column_result(value: types::ValueRef<'_>) -> types::FromSqlResult<Self> {
        let types::ValueRef::Integer(seconds) = value else {
            return Err(FromSqlError::InvalidType);
        };
        let time = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(seconds as u64))
            .ok_or_else(|| FromSqlError::Other(anyhow!("can't recreate time").into()))?;
        Ok(TimeInSeconds(time))
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
        TimeInSeconds(value)
    }
}

impl TimeInSeconds {
    pub fn into_inner(self) -> SystemTime {
        self.0
    }
}

impl std::fmt::Debug for TimeInSeconds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        isotime(self.0, f)?;
        Ok(())
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
        Ok(TimeInMicroseconds(time))
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
        TimeInMicroseconds(value)
    }
}

impl TimeInMicroseconds {
    #[allow(dead_code)]
    pub fn into_inner(self) -> SystemTime {
        self.0
    }
}

impl std::fmt::Debug for TimeInMicroseconds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        isotime(self.0, f)?;
        Ok(())
    }
}

// -----------------

impl FromSql for FileState {
    fn column_result(value: types::ValueRef<'_>) -> types::FromSqlResult<Self> {
        match value {
            types::ValueRef::Null => Err(FromSqlError::InvalidType),
            types::ValueRef::Integer(1) => Ok(FileState::New),
            types::ValueRef::Integer(2) => Ok(FileState::Dirty),
            types::ValueRef::Integer(3) => Ok(FileState::Busy),
            types::ValueRef::Integer(4) => Ok(FileState::Clean),
            types::ValueRef::Integer(5) => Ok(FileState::Unreadable),
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

impl FromSql for StoredEncryptionGroup {
    fn column_result(value: types::ValueRef<'_>) -> types::FromSqlResult<Self> {
        let types::ValueRef::Blob(blob) = value else {
            return Err(FromSqlError::InvalidType);
        };

        let eg: EncryptionGroup = blob
            .try_into()
            .map_err(|e: anyhow::Error| FromSqlError::Other(e.into()))?;
        Ok(StoredEncryptionGroup(eg))
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
        StoredEncryptionGroup(value)
    }
}

impl StoredEncryptionGroup {
    pub fn into_inner(self) -> EncryptionGroup {
        self.0
    }
}

impl std::fmt::Debug for StoredEncryptionGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x")?;
        for byte in self.0.as_bytes() {
            write!(f, "{:02x?}", byte)?;
        }
        Ok(())
    }
}

// --------------

impl FromSql for StoredFileHash {
    fn column_result(value: ValueRef<'_>) -> types::FromSqlResult<Self> {
        let types::ValueRef::Blob(blob) = value else {
            return Err(FromSqlError::InvalidType);
        };
        let hash: FileHash = blob
            .try_into()
            .map_err(|e: HashConversionError| FromSqlError::Other(e.into()))?;
        Ok(StoredFileHash(hash))
    }
}

impl ToSql for StoredFileHash {
    fn to_sql(&self) -> rusqlite::Result<types::ToSqlOutput<'_>> {
        Ok(types::ToSqlOutput::Borrowed(ValueRef::Blob(
            self.0.as_ref(),
        )))
    }
}

impl From<FileHash> for StoredFileHash {
    fn from(value: FileHash) -> Self {
        StoredFileHash(value)
    }
}

// --------------

fn isotime<T>(dt: T, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
where
    T: Into<time::OffsetDateTime>,
{
    match dt
        .into()
        .format(&time::format_description::well_known::Iso8601::DEFAULT)
    {
        Ok(timestamp) => {
            f.write_str(&timestamp)?;
            Ok(())
        }
        Err(_) => Err(std::fmt::Error {}),
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
            FileState::New,
            FileState::Dirty,
            FileState::Busy,
            FileState::Clean,
            FileState::Unreadable,
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

    #[test]
    fn file_hash_is_reversible() -> anyhow::Result<()> {
        let d = ring::digest::digest(&ring::digest::SHA256, b"foo");
        let h: FileHash = d.into();
        let fh: StoredFileHash = h.into();

        let ToSqlOutput::Borrowed(repr) = fh.to_sql().context("to_sql")? else {
            panic!("unexpected");
        };
        let back = StoredFileHash::column_result(repr).context("result")?;
        assert_eq!(back, fh);
        Ok(())
    }
}
