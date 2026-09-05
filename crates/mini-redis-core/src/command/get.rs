use crate::db::Db;
use crate::error::{Error, Result};
use crate::frame::Frame;

#[derive(Debug, Clone)]
pub struct Get {
    key: String,
}

impl Get {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn parse_frames(args: &[Frame]) -> Result<Get> {
        if args.len() != 1 {
            return Err(Error::WrongNumberOfArgs("get".into()));
        }

        match &args[0] {
            Frame::Bulk(b) => {
                let key = String::from_utf8(b.to_vec())
                    .map_err(|_| Error::Protocol("Invalid UTF-8 key".into()))?;
                Ok(Get { key })
            }
            Frame::Simple(s) => Ok(Get { key: s.clone() }),
            _ => Err(Error::Protocol("Invalid key format".into())),
        }
    }

    pub fn apply(&self, db: &Db) -> Frame {
        match db.get(&self.key) {
            Some(value) => Frame::Bulk(value),
            None => Frame::Null,
        }
    }
}
