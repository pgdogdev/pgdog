use std::num::ParseIntError;

use thiserror::Error;

use crate::net::ErrorResponse;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("{0}")]
    Backend(#[from] crate::backend::Error),

    #[error("{0}")]
    Pool(#[from] crate::backend::pool::Error),

    #[error("{0}")]
    Net(#[from] crate::net::Error),

    #[error("transaction not started")]
    TransactionNotStarted,

    #[error("out of sync, got {0}")]
    OutOfSync(char),

    #[error("missing data")]
    MissingData,

    #[error("pg_query: {0}")]
    PgQuery(#[from] pg_query::Error),

    #[error("copy error")]
    Copy,

    #[error("{0}")]
    PgError(Box<ErrorResponse>),

    #[error("table \"{0}\".\"{1}\" has no replica identity")]
    NoReplicaIdentity(String, String),

    #[error("lsn decode")]
    LsnDecode,

    #[error("parse int")]
    ParseInt(#[from] ParseIntError),

    #[error("shard has no primary")]
    NoPrimary,

    #[error("{0}")]
    Parser(#[from] crate::frontend::router::parser::Error),

    #[error("not connected")]
    NotConnected,

    #[error("replication timeout")]
    ReplicationTimeout,

    #[error("shard {0} has no replication tables")]
    NoReplicationTables(usize),

    #[error("shard {0} has no replication slot")]
    NoReplicationSlot(usize),

    #[error("parallel connection error")]
    ParallelConnection,

    #[error("no replicas available for table sync")]
    NoReplicas,
}

impl From<ErrorResponse> for Error {
    fn from(value: ErrorResponse) -> Self {
        Self::PgError(Box::new(value))
    }
}
