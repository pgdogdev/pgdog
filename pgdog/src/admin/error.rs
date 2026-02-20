//! Admin error.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("syntax error in admin command")]
    Syntax,

    #[error("empty request")]
    Empty,

    #[error("simple protocol supported only")]
    SimpleOnly,

    #[error("{0}")]
    Net(#[from] crate::net::Error),

    #[error("{0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("{0}")]
    Backend(Box<crate::backend::Error>),

    #[error("parse int")]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("{0}")]
    Config(#[from] crate::config::error::Error),

    #[error("{0}")]
    Url(#[from] url::ParseError),

    #[error("connect timeout")]
    Timeout(#[from] tokio::time::error::Elapsed),

    #[error("address is not valid")]
    InvalidAddress,

    #[error("{0}")]
    Replication(Box<crate::backend::replication::logical::Error>),
}

impl From<crate::backend::replication::logical::Error> for Error {
    fn from(err: crate::backend::replication::logical::Error) -> Self {
        Error::Replication(Box::new(err))
    }
}

impl From<crate::backend::Error> for Error {
    fn from(err: crate::backend::Error) -> Self {
        Error::Backend(Box::new(err))
    }
}
