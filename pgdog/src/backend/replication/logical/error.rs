use std::num::ParseIntError;

use thiserror::Error;

use crate::net::ErrorResponse;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Backend(#[from] crate::backend::Error),

    #[error("{0}")]
    Net(#[from] crate::net::Error),

    #[error("transaction not started")]
    TransactionNotStarted,

    #[error("out of sync")]
    OutOfSync,

    #[error("pg_query: {0}")]
    PgQuery(#[from] pg_query::Error),

    #[error("copy error")]
    Copy,

    #[error("{0}")]
    PgError(ErrorResponse),

    #[error("table \"{0}\".\"{1}\" has no replica identity")]
    NoReplicaIdentity(String, String),

    #[error("lsn decode")]
    LsnDecode,

    #[error("parse int")]
    ParseInt(#[from] ParseIntError),
}
