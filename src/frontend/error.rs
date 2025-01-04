//! Frontend errors.

use thiserror::Error;

/// Frontend error.
#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("net: {0}")]
    Net(#[from] crate::net::Error),

    #[error("{0}")]
    Backend(#[from] crate::backend::Error),

    #[error("\"{0}\" parameter is missing")]
    Parameter(String),
}
