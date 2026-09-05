use bytes::Bytes;
use std::time::Duration;

use crate::db::Db;
use crate::error::{Error, Result};
use crate::frame::Frame;

#[derive(Debug, Clone)]
pub struct Set {
    key: String,
    value: Bytes,
    expire: Option<Duration>,
}

impl Set {
    pub fn new(key: impl Into<String>, value: Bytes, expire: Option<Duration>) -> Self {
        Self {
            key: key.into(),
            value,
            expire,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &Bytes {
        &self.value
    }

    pub fn expire(&self) -> Option<Duration> {
        self.expire
    }

    pub fn parse_frames(args: &[Frame]) -> Result<Set> {
        if args.len() < 2 {
            return Err(Error::WrongNumberOfArgs("set".into()));
        }

        let key = match &args[0] {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| Error::Protocol("Invalid UTF-8 key".into()))?,
            Frame::Simple(s) => s.clone(),
            _ => return Err(Error::Protocol("Invalid key format".into())),
        };

        let value = match &args[1] {
            Frame::Bulk(b) => b.clone(),
            Frame::Simple(s) => Bytes::from(s.clone()),
            _ => return Err(Error::Protocol("Invalid value format".into())),
        };

        let mut expire = None;
        let mut idx = 2;

        while idx < args.len() {
            let opt = match &args[idx] {
                Frame::Bulk(b) => String::from_utf8(b.to_vec())
                    .map_err(|_| Error::Protocol("Invalid UTF-8 option".into()))?,
                Frame::Simple(s) => s.clone(),
                _ => return Err(Error::Protocol("Invalid option format".into())),
            };

            match opt.to_uppercase().as_str() {
                "EX" => {
                    idx += 1;
                    if idx >= args.len() {
                        return Err(Error::Protocol("Syntax error in SET EX".into()));
                    }
                    let secs = parse_int(&args[idx])?;
                    expire = Some(Duration::from_secs(secs as u64));
                }
                "PX" => {
                    idx += 1;
                    if idx >= args.len() {
                        return Err(Error::Protocol("Syntax error in SET PX".into()));
                    }
                    let millis = parse_int(&args[idx])?;
                    expire = Some(Duration::from_millis(millis as u64));
                }
                _ => return Err(Error::Protocol(format!("Unsupported SET option: {}", opt))),
            }
            idx += 1;
        }

        Ok(Set { key, value, expire })
    }

    pub fn apply(&self, db: &Db) -> Frame {
        db.set(self.key.clone(), self.value.clone(), self.expire);
        Frame::ok()
    }

    pub fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from(self.key.clone())),
            Frame::Bulk(self.value.clone()),
        ];

        if let Some(exp) = self.expire {
            frames.push(Frame::Bulk(Bytes::from("EX")));
            frames.push(Frame::Bulk(Bytes::from(exp.as_secs().to_string())));
        }

        Frame::Array(frames)
    }
}

fn parse_int(frame: &Frame) -> Result<i64> {
    match frame {
        Frame::Bulk(b) => {
            let s = std::str::from_utf8(b).map_err(|_| Error::NotAnInteger)?;
            s.parse::<i64>().map_err(|_| Error::NotAnInteger)
        }
        Frame::Simple(s) => s.parse::<i64>().map_err(|_| Error::NotAnInteger),
        Frame::Integer(n) => Ok(*n),
        _ => Err(Error::NotAnInteger),
    }
}
