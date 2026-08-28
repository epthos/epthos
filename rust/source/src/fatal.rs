//! Common type for errors that require an immediate termination.
//! This includes "clean" shutdown and panics that must be logged.

#[derive(thiserror::Error, Debug)]
pub enum Fatal {
    #[error("Shutting down cleanly")]
    Shutdown,
    #[error("Shutting down on fatal error")]
    Internal(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Fatal>;

pub trait Shutdown<T, E> {
    fn shutdown(self) -> Result<T>;
}

impl<T, E> Shutdown<T, E> for std::result::Result<T, E> {
    fn shutdown(self) -> Result<T> {
        match self {
            Ok(ok) => Ok(ok),
            Err(_) => Err(Fatal::Shutdown),
        }
    }
}

impl<T> Shutdown<T, Fatal> for Option<T> {
    fn shutdown(self) -> Result<T> {
        match self {
            Some(ok) => Ok(ok),
            None => Err(Fatal::Shutdown),
        }
    }
}

/// Unwraps a `fatal::Result<T>`, returning `()` on `Shutdown` and panicking on `Internal`.
#[macro_export]
macro_rules! bail_fatal {
    ($e:expr) => {
        match $e {
            Ok(ok) => ok,
            Err($crate::fatal::Fatal::Shutdown) => return Ok(()),
            Err($crate::fatal::Fatal::Internal(err)) => panic!("{err:?}"),
        }
    };
}
