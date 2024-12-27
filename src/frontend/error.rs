//! Frontend errors.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("net: {0}")]
    Net(#[from] crate::net::Error),
}
