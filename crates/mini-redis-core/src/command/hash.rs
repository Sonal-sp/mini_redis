use bytes::Bytes;
use crate::db::Db;
use crate::error::{Error, Result};
use crate::frame::Frame;

#[derive(Debug, Clone)]
pub struct HSet {
    key: String,
    field: String,
    val: Bytes,
}

impl HSet {
    pub fn parse_frames(args: &[Frame]) -> Result<HSet> {
        if args.len() != 3 {
            return Err(Error::WrongNumberOfArgs("hset".into()));
        }

        let key = match &args[0] {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| Error::Protocol("Invalid UTF-8 key".into()))?,
            Frame::Simple(s) => s.clone(),
            _ => return Err(Error::Protocol("Invalid key format".into())),
        };

        let field = match &args[1] {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| Error::Protocol("Invalid UTF-8 field".into()))?,
            Frame::Simple(s) => s.clone(),
            _ => return Err(Error::Protocol("Invalid field format".into())),
        };

        let val = match &args[2] {
            Frame::Bulk(b) => b.clone(),
            Frame::Simple(s) => Bytes::from(s.clone()),
            _ => return Err(Error::Protocol("Invalid value format".into())),
        };

        Ok(HSet { key, field, val })
    }

    pub fn apply(&self, db: &Db) -> Frame {
        match db.hset(self.key.clone(), self.field.clone(), self.val.clone()) {
            Ok(is_new) => Frame::Integer(if is_new { 1 } else { 0 }),
            Err(e) => Frame::Error(e.to_string()),
        }
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::Bulk(Bytes::from("HSET")),
            Frame::Bulk(Bytes::from(self.key.clone())),
            Frame::Bulk(Bytes::from(self.field.clone())),
            Frame::Bulk(self.val.clone()),
        ])
    }
}

#[derive(Debug, Clone)]
pub struct HGet {
    key: String,
    field: String,
}

impl HGet {
    pub fn parse_frames(args: &[Frame]) -> Result<HGet> {
        if args.len() != 2 {
            return Err(Error::WrongNumberOfArgs("hget".into()));
        }

        let key = match &args[0] {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| Error::Protocol("Invalid UTF-8 key".into()))?,
            Frame::Simple(s) => s.clone(),
            _ => return Err(Error::Protocol("Invalid key format".into())),
        };

        let field = match &args[1] {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| Error::Protocol("Invalid UTF-8 field".into()))?,
            Frame::Simple(s) => s.clone(),
            _ => return Err(Error::Protocol("Invalid field format".into())),
        };

        Ok(HGet { key, field })
    }

    pub fn apply(&self, db: &Db) -> Frame {
        match db.hget(&self.key, &self.field) {
            Ok(Some(val)) => Frame::Bulk(val),
            Ok(None) => Frame::Null,
            Err(e) => Frame::Error(e.to_string()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HGetAll {
    key: String,
}

impl HGetAll {
    pub fn parse_frames(args: &[Frame]) -> Result<HGetAll> {
        if args.len() != 1 {
            return Err(Error::WrongNumberOfArgs("hgetall".into()));
        }

        let key = match &args[0] {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| Error::Protocol("Invalid UTF-8 key".into()))?,
            Frame::Simple(s) => s.clone(),
            _ => return Err(Error::Protocol("Invalid key format".into())),
        };

        Ok(HGetAll { key })
    }

    pub fn apply(&self, db: &Db) -> Frame {
        match db.hgetall(&self.key) {
            Ok(Some(map)) => {
                let mut frames = Vec::with_capacity(map.len() * 2);
                for (k, v) in map {
                    frames.push(Frame::Bulk(Bytes::from(k)));
                    frames.push(Frame::Bulk(v));
                }
                Frame::Array(frames)
            }
            Ok(None) => Frame::Array(Vec::new()),
            Err(e) => Frame::Error(e.to_string()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HDel {
    key: String,
    fields: Vec<String>,
}

impl HDel {
    pub fn parse_frames(args: &[Frame]) -> Result<HDel> {
        if args.len() < 2 {
            return Err(Error::WrongNumberOfArgs("hdel".into()));
        }

        let key = match &args[0] {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| Error::Protocol("Invalid UTF-8 key".into()))?,
            Frame::Simple(s) => s.clone(),
            _ => return Err(Error::Protocol("Invalid key format".into())),
        };

        let mut fields = Vec::with_capacity(args.len() - 1);
        for arg in &args[1..] {
            match arg {
                Frame::Bulk(b) => {
                    let f = String::from_utf8(b.to_vec())
                        .map_err(|_| Error::Protocol("Invalid UTF-8 field".into()))?;
                    fields.push(f);
                }
                Frame::Simple(s) => fields.push(s.clone()),
                _ => return Err(Error::Protocol("Invalid field format".into())),
            }
        }

        Ok(HDel { key, fields })
    }

    pub fn apply(&self, db: &Db) -> Frame {
        match db.hdel(&self.key, &self.fields) {
            Ok(n) => Frame::Integer(n as i64),
            Err(e) => Frame::Error(e.to_string()),
        }
    }

    pub fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::Bulk(Bytes::from("HDEL")),
            Frame::Bulk(Bytes::from(self.key.clone())),
        ];
        for f in &self.fields {
            frames.push(Frame::Bulk(Bytes::from(f.clone())));
        }
        Frame::Array(frames)
    }
}
