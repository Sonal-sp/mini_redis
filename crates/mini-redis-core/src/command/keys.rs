use bytes::Bytes;
use crate::db::Db;
use crate::error::{Error, Result};
use crate::frame::Frame;

#[derive(Debug, Clone)]
pub struct Keys {
    pattern: Option<String>,
}

impl Keys {
    pub fn parse_frames(args: &[Frame]) -> Result<Keys> {
        match args.len() {
            0 => Ok(Keys { pattern: None }),
            1 => {
                let pattern = match &args[0] {
                    Frame::Bulk(b) => String::from_utf8(b.to_vec())
                        .map_err(|_| Error::Protocol("Invalid UTF-8 pattern".into()))?,
                    Frame::Simple(s) => s.clone(),
                    _ => return Err(Error::Protocol("Invalid pattern format".into())),
                };
                Ok(Keys {
                    pattern: Some(pattern),
                })
            }
            _ => Err(Error::WrongNumberOfArgs("keys".into())),
        }
    }

    pub fn apply(&self, db: &Db) -> Frame {
        let keys = db.keys(self.pattern.as_deref());
        let frames = keys
            .into_iter()
            .map(|k| Frame::Bulk(Bytes::from(k)))
            .collect();
        Frame::Array(frames)
    }
}

#[derive(Debug, Clone)]
pub struct Exists {
    keys: Vec<String>,
}

impl Exists {
    pub fn parse_frames(args: &[Frame]) -> Result<Exists> {
        if args.is_empty() {
            return Err(Error::WrongNumberOfArgs("exists".into()));
        }

        let mut keys = Vec::with_capacity(args.len());
        for arg in args {
            match arg {
                Frame::Bulk(b) => {
                    let k = String::from_utf8(b.to_vec())
                        .map_err(|_| Error::Protocol("Invalid UTF-8 key".into()))?;
                    keys.push(k);
                }
                Frame::Simple(s) => keys.push(s.clone()),
                _ => return Err(Error::Protocol("Invalid key format".into())),
            }
        }

        Ok(Exists { keys })
    }

    pub fn apply(&self, db: &Db) -> Frame {
        let count = db.exists(&self.keys);
        Frame::Integer(count as i64)
    }
}

#[derive(Debug, Clone)]
pub struct Type {
    key: String,
}

impl Type {
    pub fn parse_frames(args: &[Frame]) -> Result<Type> {
        if args.len() != 1 {
            return Err(Error::WrongNumberOfArgs("type".into()));
        }

        let key = match &args[0] {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| Error::Protocol("Invalid UTF-8 key".into()))?,
            Frame::Simple(s) => s.clone(),
            _ => return Err(Error::Protocol("Invalid key format".into())),
        };

        Ok(Type { key })
    }

    pub fn apply(&self, db: &Db) -> Frame {
        let t = db.type_of(&self.key);
        Frame::Simple(t.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct DbSize;

impl DbSize {
    pub fn parse_frames(args: &[Frame]) -> Result<DbSize> {
        if !args.is_empty() {
            return Err(Error::WrongNumberOfArgs("dbsize".into()));
        }
        Ok(DbSize)
    }

    pub fn apply(&self, db: &Db) -> Frame {
        let size = db.dbsize();
        Frame::Integer(size as i64)
    }
}

#[derive(Debug, Clone)]
pub struct FlushDb;

impl FlushDb {
    pub fn parse_frames(_args: &[Frame]) -> Result<FlushDb> {
        Ok(FlushDb)
    }

    pub fn apply(&self, db: &Db) -> Frame {
        db.flushdb();
        Frame::ok()
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::Bulk(Bytes::from("FLUSHDB"))])
    }
}

#[derive(Debug, Clone)]
pub struct Incr {
    key: String,
    delta: i64,
}

impl Incr {
    pub fn parse_incr(args: &[Frame]) -> Result<Incr> {
        if args.len() != 1 {
            return Err(Error::WrongNumberOfArgs("incr".into()));
        }
        let key = match &args[0] {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| Error::Protocol("Invalid UTF-8 key".into()))?,
            Frame::Simple(s) => s.clone(),
            _ => return Err(Error::Protocol("Invalid key format".into())),
        };
        Ok(Incr { key, delta: 1 })
    }

    pub fn parse_decr(args: &[Frame]) -> Result<Incr> {
        if args.len() != 1 {
            return Err(Error::WrongNumberOfArgs("decr".into()));
        }
        let key = match &args[0] {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| Error::Protocol("Invalid UTF-8 key".into()))?,
            Frame::Simple(s) => s.clone(),
            _ => return Err(Error::Protocol("Invalid key format".into())),
        };
        Ok(Incr { key, delta: -1 })
    }

    pub fn parse_by(args: &[Frame], is_incr: bool) -> Result<Incr> {
        if args.len() != 2 {
            let name = if is_incr { "incrby" } else { "decrby" };
            return Err(Error::WrongNumberOfArgs(name.into()));
        }
        let key = match &args[0] {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| Error::Protocol("Invalid UTF-8 key".into()))?,
            Frame::Simple(s) => s.clone(),
            _ => return Err(Error::Protocol("Invalid key format".into())),
        };
        let delta = match &args[1] {
            Frame::Bulk(b) => {
                let s = std::str::from_utf8(b).map_err(|_| Error::NotAnInteger)?;
                s.parse::<i64>().map_err(|_| Error::NotAnInteger)?
            }
            Frame::Simple(s) => s.parse::<i64>().map_err(|_| Error::NotAnInteger)?,
            Frame::Integer(n) => *n,
            _ => return Err(Error::NotAnInteger),
        };
        let delta = if is_incr { delta } else { -delta };
        Ok(Incr { key, delta })
    }

    pub fn apply(&self, db: &Db) -> Frame {
        match db.incr(&self.key, self.delta) {
            Ok(n) => Frame::Integer(n),
            Err(e) => Frame::Error(e.to_string()),
        }
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::Bulk(Bytes::from("INCRBY")),
            Frame::Bulk(Bytes::from(self.key.clone())),
            Frame::Bulk(Bytes::from(self.delta.to_string())),
        ])
    }
}
