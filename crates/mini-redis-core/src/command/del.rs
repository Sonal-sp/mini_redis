use bytes::Bytes;
use crate::db::Db;
use crate::error::{Error, Result};
use crate::frame::Frame;

#[derive(Debug, Clone)]
pub struct Del {
    keys: Vec<String>,
}

impl Del {
    pub fn new(keys: Vec<String>) -> Self {
        Self { keys }
    }

    pub fn keys(&self) -> &[String] {
        &self.keys
    }

    pub fn parse_frames(args: &[Frame]) -> Result<Del> {
        if args.is_empty() {
            return Err(Error::WrongNumberOfArgs("del".into()));
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

        Ok(Del { keys })
    }

    pub fn apply(&self, db: &Db) -> Frame {
        let count = db.del(&self.keys);
        Frame::Integer(count as i64)
    }

    pub fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::Bulk(Bytes::from("DEL"))];
        for k in &self.keys {
            frames.push(Frame::Bulk(Bytes::from(k.clone())));
        }
        Frame::Array(frames)
    }
}
