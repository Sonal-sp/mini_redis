use bytes::{Buf, Bytes, BytesMut};
use std::fmt;
use std::io::Cursor;

use crate::error::{Error, Result};

/// A frame in the Redis Serialization Protocol (RESP2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
    NullArray,
}

impl Frame {
    pub fn ok() -> Self {
        Frame::Simple("OK".to_string())
    }

    pub fn pong() -> Self {
        Frame::Simple("PONG".to_string())
    }

    pub fn nil() -> Self {
        Frame::Null
    }

    pub fn from_string(s: impl Into<String>) -> Self {
        Frame::Bulk(Bytes::from(s.into()))
    }

    pub fn from_i64(n: i64) -> Self {
        Frame::Integer(n)
    }

    pub fn from_err(err: impl Into<String>) -> Self {
        Frame::Error(err.into())
    }

    /// Checks if a complete frame is available in the cursor without consuming it permanently.
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<()> {
        if !src.has_remaining() {
            return Err(Error::Incomplete);
        }

        match get_u8(src)? {
            b'+' => {
                let _ = get_line(src)?;
                Ok(())
            }
            b'-' => {
                let _ = get_line(src)?;
                Ok(())
            }
            b':' => {
                let _ = get_decimal(src)?;
                Ok(())
            }
            b'$' => {
                if peek_u8(src)? == b'-' {
                    let line = get_line(src)?;
                    if line != b"-1" {
                        return Err(Error::Protocol("Invalid bulk string length".into()));
                    }
                    Ok(())
                } else {
                    let len: usize = get_decimal(src)?
                        .try_into()
                        .map_err(|_| Error::Protocol("Invalid bulk string length".into()))?;

                    // Check if entire payload + trailing \r\n is in buffer
                    if src.remaining() < len + 2 {
                        return Err(Error::Incomplete);
                    }

                    // Skip the bytes + CRLF
                    src.advance(len + 2);
                    Ok(())
                }
            }
            b'*' => {
                let len = get_decimal(src)?;
                if len == -1 {
                    return Ok(());
                }
                let len: usize = len
                    .try_into()
                    .map_err(|_| Error::Protocol("Invalid array length".into()))?;

                for _ in 0..len {
                    Frame::check(src)?;
                }
                Ok(())
            }
            other => Err(Error::Protocol(format!("Unexpected frame type: '{}'", other as char))),
        }
    }

    /// Parses a frame from the cursor. Assumes `check` succeeded or will return error.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame> {
        if !src.has_remaining() {
            return Err(Error::Incomplete);
        }

        match get_u8(src)? {
            b'+' => {
                let line = get_line(src)?;
                let string = String::from_utf8(line.to_vec())
                    .map_err(|e| Error::Protocol(e.to_string()))?;
                Ok(Frame::Simple(string))
            }
            b'-' => {
                let line = get_line(src)?;
                let string = String::from_utf8(line.to_vec())
                    .map_err(|e| Error::Protocol(e.to_string()))?;
                Ok(Frame::Error(string))
            }
            b':' => {
                let val = get_decimal(src)?;
                Ok(Frame::Integer(val))
            }
            b'$' => {
                if peek_u8(src)? == b'-' {
                    let line = get_line(src)?;
                    if line == b"-1" {
                        Ok(Frame::Null)
                    } else {
                        Err(Error::Protocol("Invalid bulk string length".into()))
                    }
                } else {
                    let len: usize = get_decimal(src)?
                        .try_into()
                        .map_err(|_| Error::Protocol("Invalid bulk string length".into()))?;

                    let n = len;
                    if src.remaining() < n + 2 {
                        return Err(Error::Incomplete);
                    }

                    let data = Bytes::copy_from_slice(&src.chunk()[..n]);
                    src.advance(n);

                    // Skip CRLF
                    if get_u8(src)? != b'\r' || get_u8(src)? != b'\n' {
                        return Err(Error::Protocol("Expected CRLF after bulk string".into()));
                    }

                    Ok(Frame::Bulk(data))
                }
            }
            b'*' => {
                let len = get_decimal(src)?;
                if len == -1 {
                    return Ok(Frame::NullArray);
                }

                let len: usize = len
                    .try_into()
                    .map_err(|_| Error::Protocol("Invalid array length".into()))?;

                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }

                Ok(Frame::Array(out))
            }
            other => Err(Error::Protocol(format!("Unexpected frame type: '{}'", other as char))),
        }
    }

    /// Serializes the frame into RESP2 wire format bytes.
    pub fn serialize(&self, dst: &mut BytesMut) {
        match self {
            Frame::Simple(val) => {
                dst.extend_from_slice(b"+");
                dst.extend_from_slice(val.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            Frame::Error(val) => {
                dst.extend_from_slice(b"-");
                dst.extend_from_slice(val.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            Frame::Integer(val) => {
                dst.extend_from_slice(b":");
                dst.extend_from_slice(val.to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            Frame::Null => {
                dst.extend_from_slice(b"$-1\r\n");
            }
            Frame::Bulk(val) => {
                dst.extend_from_slice(b"$");
                dst.extend_from_slice(val.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                dst.extend_from_slice(val);
                dst.extend_from_slice(b"\r\n");
            }
            Frame::NullArray => {
                dst.extend_from_slice(b"*-1\r\n");
            }
            Frame::Array(val) => {
                dst.extend_from_slice(b"*");
                dst.extend_from_slice(val.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                for entry in val {
                    entry.serialize(dst);
                }
            }
        }
    }

    /// Convert frame to BytesMut
    pub fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        self.serialize(&mut buf);
        buf
    }

    /// Pretty string representation for displaying in CLI or GUI
    pub fn to_display_string(&self) -> String {
        match self {
            Frame::Simple(s) => s.clone(),
            Frame::Error(e) => format!("(error) {}", e),
            Frame::Integer(i) => format!("(integer) {}", i),
            Frame::Bulk(b) => match String::from_utf8(b.to_vec()) {
                Ok(s) => format!("\"{}\"", s),
                Err(_) => format!("{:?}", b),
            },
            Frame::Null => "(nil)".to_string(),
            Frame::NullArray => "(empty list or set)".to_string(),
            Frame::Array(arr) => {
                if arr.is_empty() {
                    return "(empty list or set)".to_string();
                }
                let mut out = String::new();
                for (i, item) in arr.iter().enumerate() {
                    if i > 0 {
                        out.push('\n');
                    }
                    out.push_str(&format!("{}) {}", i + 1, item.to_display_string()));
                }
                out
            }
        }
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_display_string())
    }
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(src.get_u8())
}

fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(src.chunk()[0])
}

fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8]> {
    let start = src.position() as usize;
    let end = src.get_ref().len();

    for i in start..end {
        if src.get_ref()[i] == b'\r' && i + 1 < end && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(Error::Incomplete)
}

fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<i64> {
    let line = get_line(src)?;
    let string = std::str::from_utf8(line).map_err(|e| Error::Protocol(e.to_string()))?;
    string.parse::<i64>().map_err(|_| Error::Protocol("Invalid decimal integer".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_string() {
        let mut buf = BytesMut::new();
        let frame = Frame::Simple("OK".to_string());
        frame.serialize(&mut buf);
        assert_eq!(&buf[..], b"+OK\r\n");

        let mut cursor = Cursor::new(&buf[..]);
        Frame::check(&mut cursor).unwrap();
        cursor.set_position(0);
        let parsed = Frame::parse(&mut cursor).unwrap();
        assert_eq!(parsed, frame);
    }

    #[test]
    fn test_bulk_string() {
        let mut buf = BytesMut::new();
        let frame = Frame::Bulk(Bytes::from("hello world"));
        frame.serialize(&mut buf);
        assert_eq!(&buf[..], b"$11\r\nhello world\r\n");

        let mut cursor = Cursor::new(&buf[..]);
        Frame::check(&mut cursor).unwrap();
        cursor.set_position(0);
        let parsed = Frame::parse(&mut cursor).unwrap();
        assert_eq!(parsed, frame);
    }

    #[test]
    fn test_array() {
        let mut buf = BytesMut::new();
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from("key")),
            Frame::Bulk(Bytes::from("value")),
        ]);
        frame.serialize(&mut buf);

        let mut cursor = Cursor::new(&buf[..]);
        Frame::check(&mut cursor).unwrap();
        cursor.set_position(0);
        let parsed = Frame::parse(&mut cursor).unwrap();
        assert_eq!(parsed, frame);
    }
}
