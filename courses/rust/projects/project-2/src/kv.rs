use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use log::debug;
use serde::{Deserialize, Serialize};

use crate::error::KvsError::{IoError, KeyNotFound};
use crate::error::Result;

#[derive(Debug, Deserialize, Serialize)]
pub struct KvPair {
    key: String,
    // None means the key has been deleted!
    value: Option<String>,
}

#[derive(Debug)]
struct Offset {
    // The offset where "key value" data starts.
    start: u64,
    // Length of the data in bytes.
    len: usize,
}

/// A database that stores key-value pairs.
#[derive(Debug)]
pub struct KvStore {
    data_file: PathBuf,
    // maps keys to their offsets in the file
    offsets: HashMap<String, Offset>,
    // Number of operations. Compaction runs after every 1000 operations.
    operations: u32,
}

impl KvStore {
    /// Open a directory and return a KvStore object.
    /// If the database already exists, we expect to find a "database" file.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut buf = path.into();
        buf.push("database");
        if !buf.exists() {
            return Ok(KvStore {
                data_file: buf,
                offsets: HashMap::new(),
                operations: 0,
            });
        }
        let f = File::open(&buf)?;
        let md = std::fs::metadata(&buf)?;
        let file_size = md.len();
        debug!("file size: {:?}", md.len());
        let mut reader = BufReader::new(f);
        let mut size_buffer: [u8; 4] = [0; 4];
        let mut offset = 0;
        let mut offsets = HashMap::new();

        while offset < file_size {
            match reader.read_exact(&mut size_buffer) {
                Ok(_) => {
                    let data_size = u32::from_le_bytes(size_buffer) as usize;
                    debug!("data_size: {}", data_size);
                    let mut data_buffer: Vec<u8> = vec![0; data_size];
                    reader.read_exact(&mut data_buffer)?;
                    debug!("data: {:?}", data_buffer);
                    let pair: KvPair = serde_json::from_slice(&data_buffer)?;

                    if pair.value.is_some() {
                        offsets.insert(
                            pair.key,
                            Offset {
                                start: offset + 4,
                                len: data_size,
                            },
                        );
                    } else {
                        // the key is deleted
                        offsets.remove(&pair.key);
                    }
                    offset += 4 + data_size as u64;
                }
                Err(err) => return Err(IoError(err)),
            }
        }

        Ok(KvStore {
            data_file: buf,
            offsets,
            operations: 0,
        })
    }

    /// Set a key and append it to the end of the file.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.append(key, Some(value))
    }

    /// Retrieve the value of a key
    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self.offsets.get(&key) {
            Some(offset) => {
                let mut file = File::open(&self.data_file)?;
                file.seek(SeekFrom::Start(offset.start))?;
                let mut data_buffer: Vec<u8> = vec![0; offset.len];
                file.read_exact(&mut data_buffer)?;
                let pair: KvPair = serde_json::from_slice(&data_buffer)?;
                Ok(pair.value)
            }
            None => Ok(None),
        }
    }

    /// Remove a key by adding a tombstone value!
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.offsets.get(&key).is_some() {
            self.append(key, None)
        } else {
            Err(KeyNotFound)
        }
    }

    fn append(&mut self, key: String, value: Option<String>) -> Result<()> {
        let pair = KvPair { key, value };
        let data = serde_json::to_string(&pair)?;
        let bytes = data.into_bytes();
        let size = bytes.len();
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&self.data_file)?;
        let file_size = file.seek(SeekFrom::End(0))?;
        file.write_all(&u32::to_le_bytes(size as u32))?;
        file.write_all(&bytes)?;
        file.flush()?;

        let offset = Offset {
            start: file_size + 4,
            len: size,
        };
        if pair.value.is_some() {
            self.offsets.insert(pair.key, offset);
        } else {
            self.offsets.remove(&pair.key);
        }

        self.operations += 1;
        if self.operations > 10_000 {
            self.compaction()?;
            self.operations = 0;
        }

        Ok(())
    }

    /// Create a new file, write the compacted key-value pairs to it, and move it to override the
    /// existing data file.
    fn compaction(&mut self) -> Result<()> {
        debug!("Running compaction");
        let mut input = File::open(&self.data_file)?;
        let mut output = tempfile::NamedTempFile::new()?;

        for offset in self.offsets.values() {
            input.seek(SeekFrom::Start(offset.start))?;
            let mut data_buffer: Vec<u8> = vec![0; offset.len];
            input.read_exact(&mut data_buffer)?;

            output.write_all(&u32::to_le_bytes(offset.len as u32))?;
            output.write_all(&data_buffer)?;
        }
        output.flush()?;

        std::fs::rename(output, &self.data_file)?;

        Ok(())
    }
}
