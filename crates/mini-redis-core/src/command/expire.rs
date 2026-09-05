use bytes::Bytes;
use std::time::Duration;

use crate::db::Db;
use crate::error::{Error, Result};
use crate::frame::Frame;

#[derive(Debug, Clone)]
pub struct Expire {
    key: String,
    duration: Duration,
}

impl Expire {
    pub fn new(key: impl Into<String>, duration: Duration) -> Self {
        Self {
            key: key.into(),
            duration,
        }
    }

    pub fn parse_frames(args: &[Frame]) -> Result<Expire> {
        if args.len() != 2 {
            return Err(Error::WrongNumberOfArgs("expire".into()));
        }

        let key = match &args[0] {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| Error::Protocol("Invalid UTF-8 key".into()))?,
            Frame::Simple(s) => s.clone(),
            _ => return Err(Error::Protocol("Invalid key format".into())),
        };

        let secs = match &args[1] {
            Frame::Bulk(b) => {
                let s = std::str::from_utf8(b).map_err(|_| Error::NotAnInteger)?;
                s.parse::<u64>().map_err(|_| Error::NotAnInteger)?
            }
            Frame::Simple(s) => s.parse::<u64>().map_err(|_| Error::NotAnInteger)?,
            Frame::Integer(n) => (*n).max(0) as u64,
            _ => return Err(Error::NotAnInteger),
        };

        Ok(Expire {
            key,
            duration: Duration::from_secs(secs),
        })
    }

    pub fn apply(&self, db: &Db) -> Frame {
        let ok = db.expire(&self.key, self.duration);
        Frame::Integer(if ok { 1 } else { 0 })
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::Bulk(Bytes::from("EXPIRE")),
            Frame::Bulk(Bytes::from(self.key.clone())),
            Frame::Bulk(Bytes::from(self.duration.as_secs().to_string())),
        ])
    }
}

#[derive(Debug, Clone)]
pub struct Persist {
    key: String,
}

impl Persist {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }

    pub fn parse_frames(args: &[Frame]) -> Result<Persist> {
        if args.len() != 1 {
            return Err(Error::WrongNumberOfArgs("persist".into()));
        }

        let key = match &args[0] {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| Error::Protocol("Invalid UTF-8 key".into()))?,
            Frame::Simple(s) => s.clone(),
            _ => return Err(Error::Protocol("Invalid key format".into())),
        };

        Ok(Persist { key })
    }

    pub fn apply(&self, db: &Db) -> Frame {
        let ok = db.persist(&self.key);
        Frame::Integer(if ok { 1 } else { 0 })
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::Bulk(Bytes::from("PERSIST")),
            Frame::Bulk(Bytes::from(self.key.clone())),
        ])
    }
}
