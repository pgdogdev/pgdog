use std::num::ParseIntError;

use thiserror::Error;

use crate::{backend::replication::publisher::PublicationTable, net::ErrorResponse};

#[derive(Debug, Error)]
pub enum Error {
    #[error("backend: {0}")]
    Backend(#[from] crate::backend::Error),

    #[error("pool: {0}")]
    Pool(#[from] crate::backend::pool::Error),

    #[error("router: {0}")]
    Router(#[from] crate::frontend::router::Error),

    #[error("net: {0}")]
    Net(#[from] crate::net::Error),

    #[error("type: {0}")]
    Type(#[from] pgdog_postgres_types::Error),

    #[error("transaction not started")]
    TransactionNotStarted,

    #[error("out of sync, got {0}")]
    OutOfSync(char),

    #[error("out of sync during commit, got {0}")]
    CommitOutOfSync(char),

    #[error("out of sync during relation prepare, got {0}")]
    RelationOutOfSync(char),

    #[error("missing data")]
    MissingData,

    #[error("pg_query: {0}")]
    PgQuery(#[from] pg_query::Error),

    #[error("copy error")]
    Copy,

    #[error("pg_error: {0}")]
    PgError(Box<ErrorResponse>),

    #[error("table \"{0}\".\"{1}\" has no replica identity")]
    NoReplicaIdentity(String, String),

    #[error("lsn decode")]
    LsnDecode,

    #[error("replication slot \"{0}\" doesn't exist, but it should")]
    MissingReplicationSlot(String),

    #[error("parse int")]
    ParseInt(#[from] ParseIntError),

    #[error("shard has no primary")]
    NoPrimary,

    #[error("parser: {0}")]
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

    #[error("table {0} doesn't have a primary key")]
    NoPrimaryKey(PublicationTable),

    #[error("router returned incorrect command")]
    IncorrectCommand,

    #[error("schema: {0}")]
    SchemaSync(Box<crate::backend::schema::sync::error::Error>),

    #[error("schema isn't loaded")]
    NoSchema,

    #[error("config wasn't updated with new cluster")]
    NoNewCluster,

    #[error("tokio: {0}")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("copy for table {0} been aborted")]
    CopyAborted(PublicationTable),

    #[error("data sync has been aborted")]
    DataSyncAborted,

    #[error("replication has been aborted")]
    ReplicationAborted,

    #[error("waiter has no publisher")]
    NoPublisher,

    #[error("cutover abort timeout")]
    AbortTimeout,

    #[error("task not found")]
    TaskNotFound,

    #[error("task is not a replication task")]
    NotReplication,
}

impl From<ErrorResponse> for Error {
    fn from(value: ErrorResponse) -> Self {
        Self::PgError(Box::new(value))
    }
}

impl From<crate::backend::schema::sync::error::Error> for Error {
    fn from(value: crate::backend::schema::sync::error::Error) -> Self {
        Self::SchemaSync(Box::new(value))
    }
}
