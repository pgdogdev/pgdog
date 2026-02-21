use thiserror::Error;

use crate::net::messages::ErrorResponse;

use super::databases::User;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Tls(#[from] rustls_pki_types::InvalidDnsNameError),

    #[error("net: {0}")]
    Net(#[from] crate::net::Error),

    #[error("type: {0}")]
    Type(#[from] pgdog_postgres_types::Error),

    #[error("unexpected message: {0}")]
    UnexpectedMessage(char),

    #[error("server did not provide key data")]
    NoBackendKeyData,

    #[error("unexpected transaction status: {0}")]
    UnexpectedTransactionStatus(char),

    #[error("{0}")]
    ConnectionError(Box<ErrorResponse>),

    #[error("server connection is not synchronized")]
    NotInSync,

    #[error("server not connected")]
    NotConnected,

    #[error("direct-to-shard not connected")]
    DirectToShardNotConnected,

    #[error("multi-shard not connected")]
    MultiShardNotConnected,

    #[error("multi shard copy not connected")]
    CopyNotConnected,

    #[error("cluster not connected")]
    ClusterNotConnected,

    #[error("{0}")]
    Pool(#[from] crate::backend::pool::Error),

    #[error("{0}")]
    Admin(#[from] crate::admin::Error),

    #[error("no such user/database: {0}")]
    NoDatabase(User),

    #[error("no cluster connected")]
    NoCluster,

    #[error("database \"{0}\" has no schema owner")]
    NoSchemaOwner(String),

    #[error("{0}")]
    ScramAuth(#[from] crate::auth::scram::Error),

    #[error("{0}")]
    Config(#[from] crate::config::error::Error),

    #[error("{0}")]
    PreparedStatementError(Box<ErrorResponse>),

    #[error("prepared statement \"{0}\" is missing")]
    PreparedStatementMissing(String),

    #[error("expected '1', got '{0}")]
    ExpectedParseComplete(char),

    #[error("expected '3', got '{0}'")]
    ExpectedCloseComplete(char),

    #[error("unsupported authentication algorithm")]
    UnsupportedAuth,

    #[error("{0}")]
    Replication(#[from] crate::backend::replication::Error),

    #[error("{0}")]
    ExecutionError(Box<ErrorResponse>),

    #[error("{0}")]
    Auth(#[from] crate::auth::Error),

    #[error("protocol is out of sync")]
    ProtocolOutOfSync,

    #[error("rollback left server in inconsistent state")]
    RollbackFailed,

    #[error("decoder is missing required data to decode row")]
    DecoderRowError,

    #[error("read timeout")]
    ReadTimeout,

    #[error("router error: {0}")]
    Router(String),

    #[error("TLS connection required but server does not support TLS")]
    TlsRequired,

    #[error("{0}")]
    DnsLookupError(#[from] hickory_resolver::ResolveError),

    #[error("could not resolve to any address for hostname {0}")]
    DnsResolutionFailed(String),

    #[error("pub/sub channel disabled")]
    PubSubDisabled,

    #[error("mirror buffer empty")]
    MirrorBufferEmpty,

    #[error("{0}")]
    FrontendError(Box<crate::frontend::Error>),

    #[error("{0}")]
    MultiShard(#[from] crate::backend::pool::connection::multi_shard::Error),

    #[error("2pc commit supported with multi-shard binding only")]
    TwoPcMultiShardOnly,

    #[error("unsupported aggregation {function}: {reason}")]
    UnsupportedAggregation { function: String, reason: String },

    #[error("no foreign key path found from table \"{table}\" to any sharded table")]
    NoForeignKeyPath { table: String },

    #[error("sharding error: {0}")]
    Sharding(String),
}

impl From<crate::frontend::Error> for Error {
    fn from(value: crate::frontend::Error) -> Self {
        Self::FrontendError(Box::new(value))
    }
}

impl Error {
    /// Checkout timeout.
    pub fn no_server(&self) -> bool {
        use crate::backend::pool::Error as PoolError;
        match self {
            // These are recoverable errors.
            Error::Pool(PoolError::CheckoutTimeout) => true,
            Error::Pool(PoolError::AllReplicasDown) => true,
            Error::Pool(PoolError::Banned) => true,
            _ => false,
        }
    }
}
