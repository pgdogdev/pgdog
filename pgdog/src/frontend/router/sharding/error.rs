use std::{array::TryFromSliceError, num::ParseIntError};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Parse(#[from] ParseIntError),

    #[error("{0}")]
    Size(#[from] TryFromSliceError),

    #[error("{0}")]
    Uuid(#[from] uuid::Error),

    #[error("{0}")]
    Net(#[from] crate::net::Error),

    #[error("context incomplete")]
    IncompleteContext,

    #[error("{0}")]
    Utf8(#[from] std::str::Utf8Error),
}
