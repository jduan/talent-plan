use crate::error::KvsError::IoError;
use std::io;
use std::io::Error;

/// Errors that can be thrown by this program.
#[derive(Debug)]
pub enum KvsError {
    /// IO errors
    IoError(io::Error),

    /// Key not found
    KeyNotFound,
}

impl From<io::Error> for KvsError {
    fn from(err: Error) -> Self {
        IoError(err)
    }
}

/// A type alias for Results.
pub type Result<T> = std::result::Result<T, KvsError>;
