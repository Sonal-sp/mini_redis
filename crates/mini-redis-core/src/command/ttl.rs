use crate::db::Db;
use crate::error::{Error, Result};
use crate::frame::Frame;

#[derive(Debug, Clone)]
pub struct Ttl {
    key: String,
}

impl Ttl {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }

    pub fn parse_frames(args: &[Frame]) -> Result<Ttl> {
        if args.len() != 1 {
            return Err(Error::WrongNumberOfArgs("ttl".into()));
        }

        let key = match &args[0] {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| Error::Protocol("Invalid UTF-8 key".into()))?,
            Frame::Simple(s) => s.clone(),
            _ => return Err(Error::Protocol("Invalid key format".into())),
        };

        Ok(Ttl { key })
    }

    pub fn apply(&self, db: &Db) -> Frame {
        let ttl = db.ttl(&self.key);
        Frame::Integer(ttl)
    }
}
