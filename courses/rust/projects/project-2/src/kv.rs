use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::KvsError::{IoError, KeyNotFound, UnexpectedEOF};
use crate::error::Result;
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize)]
pub struct KvPair {
    key: String,
    // None means the key has been deleted!
    value: Option<String>,
}

#[derive(Debug)]
struct Offset {
    start: u64,
    len: usize,
}

/// A database that stores key-value pairs.
#[derive(Debug)]
pub struct KvStore {
    data_file: PathBuf,
    // maps keys to their offsets in the file
    offsets: HashMap<String, Offset>,
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
            });
        }
        let f = File::open(&buf)?;
        let mut reader = BufReader::new(f);
        let mut size_buffer: [u8; 4] = [0; 4];
        let mut offset = 0;
        let mut offsets = HashMap::new();
        loop {
            match reader.read(&mut size_buffer) {
                Ok(bytes) => {
                    if bytes == 4 {
                        let data_size = u32::from_le_bytes(size_buffer) as usize;
                        println!("data_size: {}", data_size);
                        let mut data_buffer: Vec<u8> = vec![0; data_size];
                        reader.read_exact(&mut data_buffer)?;
                        println!("data: {:?}", data_buffer);
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
                    } else if bytes == 0 {
                        // no more data
                        break;
                    } else {
                        return Err(UnexpectedEOF);
                    }
                }
                Err(err) => return Err(IoError(err)),
            }
        }

        Ok(KvStore {
            data_file: buf,
            offsets,
        })
    }

    /// Set a key and append it to the end of the file.
    pub fn set(&self, key: String, value: String) -> Result<()> {
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
    pub fn remove(&self, key: String) -> Result<()> {
        if self.offsets.get(&key).is_some() {
            self.append(key, None)
        } else {
            Err(KeyNotFound)
        }
    }

    fn append(&self, key: String, value: Option<String>) -> Result<()> {
        let pair = KvPair { key, value };
        let data = serde_json::to_string(&pair)?;
        let bytes = data.into_bytes();
        let size = bytes.len();
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&self.data_file)?;
        file.write_all(&u32::to_le_bytes(size as u32))?;
        file.write_all(&bytes)?;

        Ok(())
    }
}
