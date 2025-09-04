use std::num::ParseIntError;

use thiserror::Error;

use crate::backend;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("{0}")]
    Net(#[from] crate::net::Error),

    #[error("out of sync with unknown oid, expected Relation message first")]
    NoRelationMessage,

    #[error("no message to forward")]
    NoMessage,

    #[error("lsn decode error")]
    LsnDecode,

    #[error("parse int")]
    ParseInt(#[from] ParseIntError),

    #[error("{0}")]
    Backend(Box<backend::Error>),

    #[error("protocol error")]
    Protocol,

    #[error("transaction required for copy")]
    CopyNoTransaction,

    #[error("{0}")]
    PgQuery(#[from] pg_query::Error),
}

impl From<backend::Error> for Error {
    fn from(value: backend::Error) -> Self {
        Self::Backend(Box::new(value))
    }
}
