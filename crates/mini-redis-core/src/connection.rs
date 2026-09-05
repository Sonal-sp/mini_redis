use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio_util::codec::Decoder;

use crate::codec::RespCodec;
use crate::error::{Error, Result};
use crate::frame::Frame;

/// A buffered connection to send and receive RESP frames over a TCP socket.
#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
    codec: RespCodec,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4096),
            codec: RespCodec::new(),
        }
    }

    /// Read a single `Frame` value from the underlying stream.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            // Attempt to parse a frame from buffered data.
            if let Some(frame) = self.codec.decode(&mut self.buffer)? {
                return Ok(Some(frame));
            }

            // If incomplete, read more data from the socket.
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // The remote closed the connection.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(Error::ConnectionReset);
                }
            }
        }
    }

    /// Write a single `Frame` to the socket and flush immediately.
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        let mut bytes = BytesMut::new();
        frame.serialize(&mut bytes);
        self.stream.write_all(&bytes).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Flush the internal write buffer.
    pub async fn flush(&mut self) -> Result<()> {
        self.stream.flush().await?;
        Ok(())
    }

    /// Get reference to peer address
    pub fn peer_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.stream.get_ref().peer_addr()
    }
}
