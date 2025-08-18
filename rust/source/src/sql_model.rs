//! SQL-friendly types.

use platform::LocalPathRepr;
use rusqlite::{
    ToSql,
    types::{self, FromSql, FromSqlError},
};
use std::path::PathBuf;

// A wrapper around LocalPathRepr that is rusqlite-friendly.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct LocalPath(LocalPathRepr);

impl FromSql for LocalPath {
    fn column_result(value: types::ValueRef<'_>) -> types::FromSqlResult<Self> {
        let types::ValueRef::Blob(blob) = value else {
            return Err(FromSqlError::InvalidType);
        };
        Ok(LocalPath(LocalPathRepr::new(blob.to_vec())))
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
        LocalPath(path)
    }
}

impl LocalPath {
    pub fn into_inner(self) -> LocalPathRepr {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use rusqlite::types::ToSqlOutput;

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
}
