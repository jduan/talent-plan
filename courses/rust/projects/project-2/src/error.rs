use crate::error::KvsError::{IoError, SerdeError};
use std::io;
use std::io::Error;

/// Errors that can be thrown by this program.
#[derive(Debug)]
pub enum KvsError {
    /// UnexpectedEOF
    UnexpectedEOF,

    /// IO errors
    IoError(io::Error),

    /// Key not found
    KeyNotFound,

    /// Failed to deserialize serde_json data to KvStore
    SerdeError(serde_json::Error),
}

impl From<io::Error> for KvsError {
    fn from(err: Error) -> Self {
        IoError(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> Self {
        SerdeError(err)
    }
}

/// A type alias for Results.
pub type Result<T> = std::result::Result<T, KvsError>;
