use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Incomplete frame, waiting for more data")]
    Incomplete,

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Command error: {0}")]
    Command(String),

    #[error("Wrong number of arguments for '{0}' command")]
    WrongNumberOfArgs(String),

    #[error("Value is not an integer or out of range")]
    NotAnInteger,

    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Connection closed")]
    ConnectionReset,

    #[error("General error: {0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
