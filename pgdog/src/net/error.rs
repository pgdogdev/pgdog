//! Frontend errors.

use thiserror::Error;
use tokio_rustls::rustls;

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

    #[error("{0}")]
    TlsCertificate(#[from] rustls::pki_types::pem::Error),

    #[error("{0}")]
    Rustls(#[from] rustls::Error),

    #[error("\"{0}\" parameter is missing")]
    MissingParameter(String),

    #[error("incorrect parameter format code: {0}")]
    IncorrectParameterFormatCode(i16),
}
