//! Admin error.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("syntax error")]
    Syntax,

    #[error("empty request")]
    Empty,

    #[error("simple protocol supported only")]
    SimpleOnly,

    #[error("{0}")]
    Net(#[from] crate::net::Error),
}
