//! Frontend errors.

use std::array::TryFromSliceError;

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

    #[error("unexpected message, expected {0} got {1}")]
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

    #[error("unknown tuple data identifier: {0}")]
    UnknownTupleDataIdentifier(char),

    #[error("eof")]
    Eof,

    #[error("not text encoding")]
    NotTextEncoding,

    #[error("not utf-8")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("not an integer")]
    NotInteger(#[from] std::num::ParseIntError),

    #[error("not a float")]
    NotFloat(#[from] std::num::ParseFloatError),

    #[error("not a uuid")]
    NotUuid(#[from] uuid::Error),

    #[error("not a timestamptz")]
    NotTimestampTz,

    #[error("wrong size slice")]
    WrongSizeSlice(#[from] TryFromSliceError),

    #[error("wrong size binary ({0}) for type")]
    WrongSizeBinary(usize),

    #[error("only simple protocols supported for rewrites")]
    OnlySimpleForRewrites,
}
