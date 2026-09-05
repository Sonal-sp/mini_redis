pub mod del;
pub mod expire;
pub mod get;
pub mod hash;
pub mod info;
pub mod keys;
pub mod list;
pub mod ping;
pub mod pubsub;
pub mod set;
pub mod ttl;
pub mod unknown;

use bytes::Bytes;
use crate::db::Db;
use crate::error::{Error, Result};
use crate::frame::Frame;

pub use del::Del;
pub use expire::{Expire, Persist};
pub use get::Get;
pub use hash::{HDel, HGet, HGetAll, HSet};
pub use info::Info;
pub use keys::{DbSize, Exists, FlushDb, Incr, Keys, Type};
pub use list::{LLen, LPop, LPush, LRange};
pub use ping::Ping;
pub use pubsub::{Publish, Subscribe};
pub use set::Set;
pub use ttl::Ttl;
pub use unknown::Unknown;

#[derive(Debug, Clone)]
pub enum Command {
    Ping(Ping),
    Get(Get),
    Set(Set),
    Del(Del),
    Expire(Expire),
    Persist(Persist),
    Ttl(Ttl),
    Keys(Keys),
    Exists(Exists),
    Type(Type),
    DbSize(DbSize),
    FlushDb(FlushDb),
    Incr(Incr),
    HSet(HSet),
    HGet(HGet),
    HGetAll(HGetAll),
    HDel(HDel),
    LPush(LPush),
    RPush(LPush),
    LPop(LPop),
    RPop(LPop),
    LRange(LRange),
    LLen(LLen),
    Publish(Publish),
    Subscribe(Subscribe),
    Info(Info),
    Unknown(Unknown),
}

impl Command {
    pub fn from_frame(frame: Frame) -> Result<Command> {
        let frames = match frame {
            Frame::Array(frames) => frames,
            Frame::Bulk(b) => {
                let s = String::from_utf8(b.to_vec())
                    .map_err(|_| Error::Protocol("Invalid UTF-8 command".into()))?;
                let parts: Vec<Frame> = s
                    .split_whitespace()
                    .map(|p| Frame::Bulk(Bytes::from(p.to_string())))
                    .collect();
                parts
            }
            Frame::Simple(s) => {
                let parts: Vec<Frame> = s
                    .split_whitespace()
                    .map(|p| Frame::Bulk(Bytes::from(p.to_string())))
                    .collect();
                parts
            }
            _ => return Err(Error::Protocol("Expected Array or Bulk frame for command".into())),
        };

        if frames.is_empty() {
            return Err(Error::Protocol("Empty command frame".into()));
        }

        let cmd_name = match &frames[0] {
            Frame::Bulk(b) => {
                String::from_utf8(b.to_vec())
                    .map_err(|_| Error::Protocol("Invalid UTF-8 command name".into()))?
                    .to_uppercase()
            }
            Frame::Simple(s) => s.to_uppercase(),
            _ => return Err(Error::Protocol("Command name must be a string".into())),
        };

        let args = &frames[1..];

        let cmd = match cmd_name.as_str() {
            "PING" => Command::Ping(Ping::parse_frames(args)?),
            "ECHO" => Command::Ping(Ping::parse_frames(args)?),
            "GET" => Command::Get(Get::parse_frames(args)?),
            "SET" => Command::Set(Set::parse_frames(args)?),
            "DEL" => Command::Del(Del::parse_frames(args)?),
            "EXPIRE" => Command::Expire(Expire::parse_frames(args)?),
            "PERSIST" => Command::Persist(Persist::parse_frames(args)?),
            "TTL" => Command::Ttl(Ttl::parse_frames(args)?),
            "KEYS" => Command::Keys(Keys::parse_frames(args)?),
            "EXISTS" => Command::Exists(Exists::parse_frames(args)?),
            "TYPE" => Command::Type(Type::parse_frames(args)?),
            "DBSIZE" => Command::DbSize(DbSize::parse_frames(args)?),
            "FLUSHDB" | "FLUSHALL" => Command::FlushDb(FlushDb::parse_frames(args)?),
            "INCR" => Command::Incr(Incr::parse_incr(args)?),
            "DECR" => Command::Incr(Incr::parse_decr(args)?),
            "INCRBY" => Command::Incr(Incr::parse_by(args, true)?),
            "DECRBY" => Command::Incr(Incr::parse_by(args, false)?),
            "HSET" => Command::HSet(HSet::parse_frames(args)?),
            "HGET" => Command::HGet(HGet::parse_frames(args)?),
            "HGETALL" => Command::HGetAll(HGetAll::parse_frames(args)?),
            "HDEL" => Command::HDel(HDel::parse_frames(args)?),
            "LPUSH" => Command::LPush(LPush::parse_frames(args, true)?),
            "RPUSH" => Command::RPush(LPush::parse_frames(args, false)?),
            "LPOP" => Command::LPop(LPop::parse_frames(args, true)?),
            "RPOP" => Command::RPop(LPop::parse_frames(args, false)?),
            "LRANGE" => Command::LRange(LRange::parse_frames(args)?),
            "LLEN" => Command::LLen(LLen::parse_frames(args)?),
            "PUBLISH" => Command::Publish(Publish::parse_frames(args)?),
            "SUBSCRIBE" => Command::Subscribe(Subscribe::parse_frames(args)?),
            "INFO" => Command::Info(Info::parse_frames(args)?),
            other => Command::Unknown(Unknown::new(other)),
        };

        Ok(cmd)
    }

    /// Whether this command mutates state and should be logged to the AOF file.
    pub fn is_write_command(&self) -> bool {
        matches!(
            self,
            Command::Set(_)
                | Command::Del(_)
                | Command::Expire(_)
                | Command::Persist(_)
                | Command::FlushDb(_)
                | Command::Incr(_)
                | Command::HSet(_)
                | Command::HDel(_)
                | Command::LPush(_)
                | Command::RPush(_)
                | Command::LPop(_)
                | Command::RPop(_)
        )
    }

    /// Serializes command into an AOF Frame if it's a write command
    pub fn to_frame(&self) -> Option<Frame> {
        match self {
            Command::Set(s) => Some(s.to_frame()),
            Command::Del(d) => Some(d.to_frame()),
            Command::Expire(e) => Some(e.to_frame()),
            Command::Persist(p) => Some(p.to_frame()),
            Command::FlushDb(f) => Some(f.to_frame()),
            Command::Incr(i) => Some(i.to_frame()),
            Command::HSet(h) => Some(h.to_frame()),
            Command::HDel(h) => Some(h.to_frame()),
            Command::LPush(l) => Some(l.to_frame()),
            Command::RPush(l) => Some(l.to_frame()),
            Command::LPop(l) => Some(l.to_frame()),
            Command::RPop(l) => Some(l.to_frame()),
            _ => None,
        }
    }

    /// Execute the command against the DB and return the response frame
    pub fn apply(&self, db: &Db) -> Frame {
        match self {
            Command::Ping(p) => p.apply(),
            Command::Get(g) => g.apply(db),
            Command::Set(s) => s.apply(db),
            Command::Del(d) => d.apply(db),
            Command::Expire(e) => e.apply(db),
            Command::Persist(p) => p.apply(db),
            Command::Ttl(t) => t.apply(db),
            Command::Keys(k) => k.apply(db),
            Command::Exists(e) => e.apply(db),
            Command::Type(t) => t.apply(db),
            Command::DbSize(s) => s.apply(db),
            Command::FlushDb(f) => f.apply(db),
            Command::Incr(i) => i.apply(db),
            Command::HSet(h) => h.apply(db),
            Command::HGet(h) => h.apply(db),
            Command::HGetAll(h) => h.apply(db),
            Command::HDel(h) => h.apply(db),
            Command::LPush(l) => l.apply(db),
            Command::RPush(r) => r.apply(db),
            Command::LPop(l) => l.apply(db),
            Command::RPop(r) => r.apply(db),
            Command::LRange(l) => l.apply(db),
            Command::LLen(l) => l.apply(db),
            Command::Publish(p) => p.apply(db),
            Command::Subscribe(_) => Frame::Error("SUBSCRIBE handled via connection stream".into()),
            Command::Info(i) => i.apply(db),
            Command::Unknown(u) => u.apply(),
        }
    }
}
