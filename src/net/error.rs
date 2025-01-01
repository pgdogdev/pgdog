//! Frontend errors.

use thiserror::Error;

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

    #[error("unexpected message, expected {0} got {0}")]
    UnexpectedMessage(char, char),

    #[error("unexpected payload")]
    UnexpectedPayload,

    #[error("unsupported authentication: {0}")]
    UnsupportedAuthentication(i32),

    #[error("unexpected ssl request reply: {0}")]
    UnexpectedSslReply(char),
}
