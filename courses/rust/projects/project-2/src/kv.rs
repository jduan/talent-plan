use crate::error::Result;
use std::path::PathBuf;

/// A database that stores key-value pairs.
pub struct KvStore {}

impl KvStore {
    /// Open a directory and return a KvStore object.
    pub fn open(_path: impl Into<PathBuf>) -> Result<KvStore> {
        todo!()
    }

    /// Set a key
    pub fn set(&self, key: String, value: String) -> Result<KvStore> {
        todo!()
    }

    /// Retrieve the value of a key
    pub fn get(&self, key: String) -> Result<Option<String>> {
        todo!()
    }

    /// Remove a key
    pub fn remove(&self, key: String) -> Result<()> {
        todo!()
    }
}
