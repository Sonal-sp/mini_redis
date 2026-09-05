use bytes::{Buf, BytesMut};
use std::io::Cursor;
use tokio_util::codec::{Decoder, Encoder};

use crate::error::Error;
use crate::frame::Frame;

#[derive(Default, Debug)]
pub struct RespCodec;

impl RespCodec {
    pub fn new() -> Self {
        Self
    }
}

impl Decoder for RespCodec {
    type Item = Frame;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let mut cursor = Cursor::new(&src[..]);

        // Check if full frame is present
        match Frame::check(&mut cursor) {
            Ok(()) => {
                // Get the length of bytes consumed by check()
                let len = cursor.position() as usize;

                // Reset cursor to parse the actual frame
                cursor.set_position(0);
                let frame = Frame::parse(&mut cursor)?;

                // Advance internal buffer by consumed length
                src.advance(len);

                Ok(Some(frame))
            }
            Err(Error::Incomplete) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl Encoder<Frame> for RespCodec {
    type Error = Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.serialize(dst);
        Ok(())
    }
}
