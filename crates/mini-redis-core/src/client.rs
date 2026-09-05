use bytes::Bytes;
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::frame::Frame;

/// Asynchronous client driver for communicating with Mini Redis (or standard Redis).
pub struct Client {
    connection: Connection,
}

impl Client {
    /// Connect to a Redis server at the specified address.
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> Result<Client> {
        let socket = TcpStream::connect(addr).await?;
        let connection = Connection::new(socket);
        Ok(Client { connection })
    }

    /// Send a frame and wait for the response frame.
    pub async fn send_frame(&mut self, frame: Frame) -> Result<Frame> {
        self.connection.write_frame(&frame).await?;
        match self.connection.read_frame().await? {
            Some(response) => Ok(response),
            None => Err(Error::ConnectionReset),
        }
    }

    /// Parse a command string (e.g. "SET foo bar EX 60") and execute it on the server.
    pub async fn execute_raw(&mut self, cmd_line: &str) -> Result<Frame> {
        let parts = parse_command_line(cmd_line);
        if parts.is_empty() {
            return Err(Error::Command("Empty command".into()));
        }

        let frames: Vec<Frame> = parts
            .into_iter()
            .map(|s| Frame::Bulk(Bytes::from(s)))
            .collect();

        self.send_frame(Frame::Array(frames)).await
    }

    /// Ping the server.
    pub async fn ping(&mut self, msg: Option<&str>) -> Result<String> {
        let frame = match msg {
            Some(m) => Frame::Array(vec![
                Frame::Bulk(Bytes::from("PING")),
                Frame::Bulk(Bytes::from(m.to_string())),
            ]),
            None => Frame::Array(vec![Frame::Bulk(Bytes::from("PING"))]),
        };

        match self.send_frame(frame).await? {
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(b) => Ok(String::from_utf8_lossy(&b).to_string()),
            Frame::Error(e) => Err(Error::Command(e)),
            _ => Err(Error::Protocol("Unexpected response to PING".into())),
        }
    }

    /// Get value by key.
    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>> {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("GET")),
            Frame::Bulk(Bytes::from(key.to_string())),
        ]);

        match self.send_frame(frame).await? {
            Frame::Bulk(b) => Ok(Some(b)),
            Frame::Null => Ok(None),
            Frame::Error(e) => Err(Error::Command(e)),
            _ => Err(Error::Protocol("Unexpected response to GET".into())),
        }
    }

    /// Set a key-value pair with optional TTL in seconds.
    pub async fn set(&mut self, key: &str, value: Bytes, expire_secs: Option<u64>) -> Result<()> {
        let mut frames = vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from(key.to_string())),
            Frame::Bulk(value),
        ];

        if let Some(secs) = expire_secs {
            frames.push(Frame::Bulk(Bytes::from("EX")));
            frames.push(Frame::Bulk(Bytes::from(secs.to_string())));
        }

        match self.send_frame(Frame::Array(frames)).await? {
            Frame::Simple(s) if s == "OK" => Ok(()),
            Frame::Error(e) => Err(Error::Command(e)),
            _ => Err(Error::Protocol("Unexpected response to SET".into())),
        }
    }

    /// Delete keys.
    pub async fn del(&mut self, keys: &[&str]) -> Result<usize> {
        let mut frames = vec![Frame::Bulk(Bytes::from("DEL"))];
        for k in keys {
            frames.push(Frame::Bulk(Bytes::from(k.to_string())));
        }

        match self.send_frame(Frame::Array(frames)).await? {
            Frame::Integer(n) => Ok(n as usize),
            Frame::Error(e) => Err(Error::Command(e)),
            _ => Err(Error::Protocol("Unexpected response to DEL".into())),
        }
    }

    /// Query keys matching pattern.
    pub async fn keys(&mut self, pattern: &str) -> Result<Vec<String>> {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("KEYS")),
            Frame::Bulk(Bytes::from(pattern.to_string())),
        ]);

        match self.send_frame(frame).await? {
            Frame::Array(arr) => {
                let mut keys = Vec::with_capacity(arr.len());
                for item in arr {
                    if let Frame::Bulk(b) = item {
                        keys.push(String::from_utf8_lossy(&b).to_string());
                    }
                }
                Ok(keys)
            }
            Frame::Error(e) => Err(Error::Command(e)),
            _ => Err(Error::Protocol("Unexpected response to KEYS".into())),
        }
    }

    /// Get TTL for a key.
    pub async fn ttl(&mut self, key: &str) -> Result<i64> {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("TTL")),
            Frame::Bulk(Bytes::from(key.to_string())),
        ]);

        match self.send_frame(frame).await? {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(Error::Command(e)),
            _ => Err(Error::Protocol("Unexpected response to TTL".into())),
        }
    }

    /// Get type of key.
    pub async fn type_of(&mut self, key: &str) -> Result<String> {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("TYPE")),
            Frame::Bulk(Bytes::from(key.to_string())),
        ]);

        match self.send_frame(frame).await? {
            Frame::Simple(s) => Ok(s),
            Frame::Error(e) => Err(Error::Command(e)),
            _ => Err(Error::Protocol("Unexpected response to TYPE".into())),
        }
    }

    /// Publish a message to a channel.
    pub async fn publish(&mut self, channel: &str, message: &str) -> Result<usize> {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("PUBLISH")),
            Frame::Bulk(Bytes::from(channel.to_string())),
            Frame::Bulk(Bytes::from(message.to_string())),
        ]);

        match self.send_frame(frame).await? {
            Frame::Integer(n) => Ok(n as usize),
            Frame::Error(e) => Err(Error::Command(e)),
            _ => Err(Error::Protocol("Unexpected response to PUBLISH".into())),
        }
    }

    /// Get server INFO string.
    pub async fn info(&mut self) -> Result<String> {
        let frame = Frame::Array(vec![Frame::Bulk(Bytes::from("INFO"))]);

        match self.send_frame(frame).await? {
            Frame::Bulk(b) => Ok(String::from_utf8_lossy(&b).to_string()),
            Frame::Error(e) => Err(Error::Command(e)),
            _ => Err(Error::Protocol("Unexpected response to INFO".into())),
        }
    }

    /// Read next raw frame from connection.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        self.connection.read_frame().await
    }
}

/// Helper function to tokenize a command line respecting double quotes.
pub fn parse_command_line(input: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '"' => {
                in_quotes = !in_quotes;
            }
            ' ' | '\t' | '\r' | '\n' if !in_quotes => {
                if !current.is_empty() {
                    args.push(current.clone());
                    current.clear();
                }
            }
            '\\' if in_quotes => {
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            }
            _ => {
                current.push(c);
            }
        }
    }

    if !current.is_empty() {
        args.push(current);
    }

    args
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_command_line() {
        let cmd = "SET \"my key\" \"hello world\" EX 60";
        let parts = parse_command_line(cmd);
        assert_eq!(parts, vec!["SET", "my key", "hello world", "EX", "60"]);
    }
}
