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
}
