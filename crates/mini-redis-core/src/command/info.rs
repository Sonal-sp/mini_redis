use bytes::Bytes;
use crate::db::Db;
use crate::error::Result;
use crate::frame::Frame;

#[derive(Debug, Clone)]
pub struct Info;

impl Info {
    pub fn parse_frames(_args: &[Frame]) -> Result<Info> {
        Ok(Info)
    }

    pub fn apply(&self, db: &Db) -> Frame {
        let info_str = db.info();
        Frame::Bulk(Bytes::from(info_str))
    }
}
