use bytes::BytesMut;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tracing::warn;

use crate::command::Command;
use crate::db::Db;
use crate::error::{Error, Result};
use crate::frame::Frame;

/// Append-Only File persistence engine for Mini Redis.
pub struct Aof {
    file: Mutex<File>,
    #[allow(dead_code)]
    path: PathBuf,
}

impl Aof {
    /// Opens or creates the AOF file at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        Ok(Self {
            file: Mutex::new(file),
            path,
        })
    }

    /// Appends a serialized frame to the AOF log and flushes immediately.
    pub fn append_frame(&self, frame: &Frame) -> Result<()> {
        let mut bytes = BytesMut::new();
        frame.serialize(&mut bytes);

        let mut file = self.file.lock().map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "AOF lock poisoned",
            ))
        })?;

        file.write_all(&bytes)?;
        file.flush()?;
        Ok(())
    }

    /// Replays all commands stored in the AOF file into the provided database.
    pub fn replay(path: impl AsRef<Path>, db: &Db) -> Result<usize> {
        Self::replay_direct(path, db)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_aof_write_and_replay() {
        let aof_path = std::env::temp_dir().join(format!("mini_redis_test_{}.aof", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));

        let aof = Aof::open(&aof_path).unwrap();
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from("greeting")),
            Frame::Bulk(Bytes::from("hello_rust")),
        ]);

        aof.append_frame(&frame).unwrap();

        // Replay into fresh DB
        let db = Db::new();
        let count = Aof::replay_direct(&aof_path, &db).unwrap();
        assert_eq!(count, 1);
        assert_eq!(db.get("greeting"), Some(Bytes::from("hello_rust")));
        let _ = std::fs::remove_file(&aof_path);
    }
}

impl Aof {
    /// Direct sequential frame replay
    pub fn replay_direct(path: impl AsRef<Path>, db: &Db) -> Result<usize> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(0);
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;

        if buffer.is_empty() {
            return Ok(0);
        }

        let mut cursor = Cursor::new(&buffer[..]);
        let mut count = 0;

        while (cursor.position() as usize) < buffer.len() {
            let start = cursor.position() as usize;
            match Frame::parse(&mut cursor) {
                Ok(frame) => match Command::from_frame(frame) {
                    Ok(cmd) => {
                        cmd.apply(db);
                        count += 1;
                    }
                    Err(e) => warn!("Skipping unparseable AOF command: {}", e),
                },
                Err(Error::Incomplete) => break,
                Err(e) => {
                    warn!("AOF parsing stopped at byte {}: {}", start, e);
                    break;
                }
            }
        }

        Ok(count)
    }
}
