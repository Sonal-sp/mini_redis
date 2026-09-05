use bytes::Bytes;
use crate::db::Db;
use crate::error::{Error, Result};
use crate::frame::Frame;

#[derive(Debug, Clone)]
pub struct LPush {
    key: String,
    values: Vec<Bytes>,
    is_left: bool,
}

impl LPush {
    pub fn parse_frames(args: &[Frame], is_left: bool) -> Result<LPush> {
        let name = if is_left { "lpush" } else { "rpush" };
        if args.len() < 2 {
            return Err(Error::WrongNumberOfArgs(name.into()));
        }

        let key = match &args[0] {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| Error::Protocol("Invalid UTF-8 key".into()))?,
            Frame::Simple(s) => s.clone(),
            _ => return Err(Error::Protocol("Invalid key format".into())),
        };

        let mut values = Vec::with_capacity(args.len() - 1);
        for arg in &args[1..] {
            match arg {
                Frame::Bulk(b) => values.push(b.clone()),
                Frame::Simple(s) => values.push(Bytes::from(s.clone())),
                _ => return Err(Error::Protocol("Invalid value format".into())),
            }
        }

        Ok(LPush {
            key,
            values,
            is_left,
        })
    }

    pub fn apply(&self, db: &Db) -> Frame {
        let res = if self.is_left {
            db.lpush(self.key.clone(), self.values.clone())
        } else {
            db.rpush(self.key.clone(), self.values.clone())
        };

        match res {
            Ok(len) => Frame::Integer(len as i64),
            Err(e) => Frame::Error(e.to_string()),
        }
    }

    pub fn to_frame(&self) -> Frame {
        let cmd = if self.is_left { "LPUSH" } else { "RPUSH" };
        let mut frames = vec![
            Frame::Bulk(Bytes::from(cmd)),
            Frame::Bulk(Bytes::from(self.key.clone())),
        ];
        for v in &self.values {
            frames.push(Frame::Bulk(v.clone()));
        }
        Frame::Array(frames)
    }
}

#[derive(Debug, Clone)]
pub struct LPop {
    key: String,
    count: usize,
    is_left: bool,
}

impl LPop {
    pub fn parse_frames(args: &[Frame], is_left: bool) -> Result<LPop> {
        let name = if is_left { "lpop" } else { "rpop" };
        if args.is_empty() || args.len() > 2 {
            return Err(Error::WrongNumberOfArgs(name.into()));
        }

        let key = match &args[0] {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| Error::Protocol("Invalid UTF-8 key".into()))?,
            Frame::Simple(s) => s.clone(),
            _ => return Err(Error::Protocol("Invalid key format".into())),
        };

        let count = if args.len() == 2 {
            match &args[1] {
                Frame::Bulk(b) => {
                    let s = std::str::from_utf8(b).map_err(|_| Error::NotAnInteger)?;
                    s.parse::<usize>().map_err(|_| Error::NotAnInteger)?
                }
                Frame::Simple(s) => s.parse::<usize>().map_err(|_| Error::NotAnInteger)?,
                Frame::Integer(n) => (*n).max(0) as usize,
                _ => return Err(Error::NotAnInteger),
            }
        } else {
            1
        };

        Ok(LPop {
            key,
            count,
            is_left,
        })
    }

    pub fn apply(&self, db: &Db) -> Frame {
        let res = if self.is_left {
            db.lpop(&self.key, self.count)
        } else {
            db.rpop(&self.key, self.count)
        };

        match res {
            Ok(items) => {
                if items.is_empty() {
                    Frame::Null
                } else if self.count == 1 {
                    Frame::Bulk(items.into_iter().next().unwrap())
                } else {
                    Frame::Array(items.into_iter().map(Frame::Bulk).collect())
                }
            }
            Err(e) => Frame::Error(e.to_string()),
        }
    }

    pub fn to_frame(&self) -> Frame {
        let cmd = if self.is_left { "LPOP" } else { "RPOP" };
        Frame::Array(vec![
            Frame::Bulk(Bytes::from(cmd)),
            Frame::Bulk(Bytes::from(self.key.clone())),
            Frame::Bulk(Bytes::from(self.count.to_string())),
        ])
    }
}

#[derive(Debug, Clone)]
pub struct LRange {
    key: String,
    start: i64,
    stop: i64,
}

impl LRange {
    pub fn parse_frames(args: &[Frame]) -> Result<LRange> {
        if args.len() != 3 {
            return Err(Error::WrongNumberOfArgs("lrange".into()));
        }

        let key = match &args[0] {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| Error::Protocol("Invalid UTF-8 key".into()))?,
            Frame::Simple(s) => s.clone(),
            _ => return Err(Error::Protocol("Invalid key format".into())),
        };

        let start = parse_int(&args[1])?;
        let stop = parse_int(&args[2])?;

        Ok(LRange { key, start, stop })
    }

    pub fn apply(&self, db: &Db) -> Frame {
        match db.lrange(&self.key, self.start, self.stop) {
            Ok(items) => Frame::Array(items.into_iter().map(Frame::Bulk).collect()),
            Err(e) => Frame::Error(e.to_string()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LLen {
    key: String,
}

impl LLen {
    pub fn parse_frames(args: &[Frame]) -> Result<LLen> {
        if args.len() != 1 {
            return Err(Error::WrongNumberOfArgs("llen".into()));
        }

        let key = match &args[0] {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| Error::Protocol("Invalid UTF-8 key".into()))?,
            Frame::Simple(s) => s.clone(),
            _ => return Err(Error::Protocol("Invalid key format".into())),
        };

        Ok(LLen { key })
    }

    pub fn apply(&self, db: &Db) -> Frame {
        match db.llen(&self.key) {
            Ok(len) => Frame::Integer(len as i64),
            Err(e) => Frame::Error(e.to_string()),
        }
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
