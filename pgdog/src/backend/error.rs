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

    #[error("multi shard copy not connected")]
    CopyNotConnected,

    #[error("{0}")]
    Pool(#[from] crate::backend::pool::Error),

    #[error("{0}")]
    Admin(#[from] crate::admin::Error),

    #[error("no such user/database: {0}")]
    NoDatabase(User),

    #[error("no cluster connected")]
    NoCluster,

    #[error("scram auth failed")]
    ScramAuth(#[from] crate::auth::scram::Error),

    #[error("config error")]
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

    #[error("decoder is missing required data to decode row")]
    DecoderRowError,

    #[error("read timeout")]
    ReadTimeout,
}

impl Error {
    /// Checkout timeout.
    pub fn no_server(&self) -> bool {
        use crate::backend::pool::Error as PoolError;
        match self {
            // These are recoverable errors.
            Error::Pool(PoolError::CheckoutTimeout) => true,
            Error::Pool(PoolError::AllReplicasDown) => true,
            _ => false,
        }
    }
}
