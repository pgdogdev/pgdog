//! Frontend errors.

use thiserror::Error;

use super::connection::Message;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("unsupported startup request: {0}")]
    UnsupportedStartup(i32),

    #[error("unexpected TLS request")]
    UnexpectedTlsRequest,

    #[error("connection is not sending messages")]
    ConnectionDown,

    #[error("{0}")]
    Mpsc(#[from] tokio::sync::mpsc::error::SendError<Message>),
}
