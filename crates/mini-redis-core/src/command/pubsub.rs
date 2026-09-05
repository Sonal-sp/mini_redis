use bytes::Bytes;
use crate::db::Db;
use crate::error::{Error, Result};
use crate::frame::Frame;

#[derive(Debug, Clone)]
pub struct Publish {
    channel: String,
    message: Bytes,
}

impl Publish {
    pub fn new(channel: impl Into<String>, message: Bytes) -> Self {
        Self {
            channel: channel.into(),
            message,
        }
    }

    pub fn parse_frames(args: &[Frame]) -> Result<Publish> {
        if args.len() != 2 {
            return Err(Error::WrongNumberOfArgs("publish".into()));
        }

        let channel = match &args[0] {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| Error::Protocol("Invalid UTF-8 channel".into()))?,
            Frame::Simple(s) => s.clone(),
            _ => return Err(Error::Protocol("Invalid channel format".into())),
        };

        let message = match &args[1] {
            Frame::Bulk(b) => b.clone(),
            Frame::Simple(s) => Bytes::from(s.clone()),
            _ => return Err(Error::Protocol("Invalid message format".into())),
        };

        Ok(Publish { channel, message })
    }

    pub fn apply(&self, db: &Db) -> Frame {
        let receivers = db.publish(&self.channel, self.message.clone());
        Frame::Integer(receivers as i64)
    }
}

#[derive(Debug, Clone)]
pub struct Subscribe {
    channels: Vec<String>,
}

impl Subscribe {
    pub fn new(channels: Vec<String>) -> Self {
        Self { channels }
    }

    pub fn channels(&self) -> &[String] {
        &self.channels
    }

    pub fn parse_frames(args: &[Frame]) -> Result<Subscribe> {
        if args.is_empty() {
            return Err(Error::WrongNumberOfArgs("subscribe".into()));
        }

        let mut channels = Vec::with_capacity(args.len());
        for arg in args {
            match arg {
                Frame::Bulk(b) => {
                    let ch = String::from_utf8(b.to_vec())
                        .map_err(|_| Error::Protocol("Invalid UTF-8 channel".into()))?;
                    channels.push(ch);
                }
                Frame::Simple(s) => channels.push(s.clone()),
                _ => return Err(Error::Protocol("Invalid channel format".into())),
            }
        }

        Ok(Subscribe { channels })
    }
}
