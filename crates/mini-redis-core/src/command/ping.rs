use bytes::Bytes;
use crate::error::{Error, Result};
use crate::frame::Frame;

#[derive(Debug, Default, Clone)]
pub struct Ping {
    msg: Option<String>,
}

impl Ping {
    pub fn new(msg: Option<String>) -> Self {
        Self { msg }
    }

    pub fn parse_frames(args: &[Frame]) -> Result<Ping> {
        match args.len() {
            0 => Ok(Ping { msg: None }),
            1 => match &args[0] {
                Frame::Bulk(b) => {
                    let s = String::from_utf8(b.to_vec())
                        .map_err(|_| Error::Protocol("Invalid UTF-8 in PING message".into()))?;
                    Ok(Ping { msg: Some(s) })
                }
                Frame::Simple(s) => Ok(Ping { msg: Some(s.clone()) }),
                _ => Err(Error::Protocol("Invalid PING argument".into())),
            },
            _ => Err(Error::WrongNumberOfArgs("ping".into())),
        }
    }

    pub fn apply(&self) -> Frame {
        match &self.msg {
            Some(msg) => Frame::Bulk(Bytes::from(msg.clone())),
            None => Frame::pong(),
        }
    }
}
