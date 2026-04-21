use std::num::ParseIntError;

use thiserror::Error;

use crate::{
    backend::replication::publisher::PublicationTable,
    net::{CommandComplete, ErrorResponse},
};

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

    #[error("out of sync during row write, got {0}")]
    SendOutOfSync(char),

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

    #[error("binary format mismatch (likely int -> bigint), use text copy instead: {0}")]
    BinaryFormatMismatch(Box<ErrorResponse>),

    #[error("command complete has no rows: {0}")]
    CommandCompleteNoRows(CommandComplete),

    #[error("missing key in replication stream, out of sync")]
    MissingKey,
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

impl Error {
    /// Whether the table copy should be retried after this error.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Net(inner) => inner.is_retryable(),
            Self::Pool(inner) => inner.is_retryable(),
            Self::Backend(inner) => inner.is_retryable(),
            // No connection yet, or primary is down.
            Self::NotConnected | Self::NoPrimary => true,
            // Replication stalled; temporary slot is gone, next attempt starts fresh.
            Self::ReplicationTimeout => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::pool::Error as PE;
    use crate::backend::replication::publisher::PublicationTable;
    use crate::net::Error as NE;

    #[test]
    fn retryable() {
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        assert!(Error::Net(NE::Io(io)).is_retryable());
        assert!(Error::Net(NE::UnexpectedEof).is_retryable());
        assert!(Error::Pool(PE::NoPrimary).is_retryable());
        assert!(Error::Pool(PE::CheckoutTimeout).is_retryable());
        assert!(Error::NotConnected.is_retryable());
        assert!(Error::NoPrimary.is_retryable());
        assert!(Error::ReplicationTimeout.is_retryable());
    }

    #[test]
    fn retryable_via_backend_wrapper() {
        use crate::backend::Error as BE;

        // IO reset wrapped as Backend — the common path for network drops during COPY.
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        assert!(Error::Backend(BE::Io(io)).is_retryable());

        // Read timeout mid-stream.
        assert!(Error::Backend(BE::ReadTimeout).is_retryable());

        // Pool couldn't hand out a connection.
        assert!(Error::Backend(BE::Pool(PE::CheckoutTimeout)).is_retryable());
        assert!(Error::Backend(BE::Pool(PE::NoPrimary)).is_retryable());
        assert!(Error::Backend(BE::Pool(PE::AllReplicasDown)).is_retryable());

        // Connection variants.
        assert!(Error::Backend(BE::NotConnected).is_retryable());
        assert!(Error::Backend(BE::ClusterNotConnected).is_retryable());
    }

    #[test]
    fn not_retryable_via_backend_wrapper() {
        use crate::backend::Error as BE;
        use crate::net::messages::ErrorResponse;

        // Postgres-level error response: permanent, not a network fault.
        let pg_err = ErrorResponse::default();
        assert!(!Error::Backend(BE::ConnectionError(Box::new(pg_err))).is_retryable());

        // Protocol violations are not transient.
        assert!(!Error::Backend(BE::ProtocolOutOfSync).is_retryable());
    }

    #[test]
    fn not_retryable() {
        assert!(!Error::CopyAborted(PublicationTable::default()).is_retryable());
        assert!(!Error::DataSyncAborted.is_retryable());
        assert!(!Error::NoPrimaryKey(PublicationTable::default()).is_retryable());
        assert!(!Error::NoReplicaIdentity("s".into(), "t".into()).is_retryable());
    }
}
