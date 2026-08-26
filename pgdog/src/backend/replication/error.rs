use std::num::ParseIntError;

use thiserror::Error;

use crate::backend;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Net(#[from] crate::net::Error),

    #[error("parse int")]
    ParseInt(#[from] ParseIntError),

    #[error("{0}")]
    Backend(Box<backend::Error>),
}

impl From<backend::Error> for Error {
    fn from(value: backend::Error) -> Self {
        Self::Backend(Box::new(value))
    }
}
