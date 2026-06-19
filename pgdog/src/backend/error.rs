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

    #[error("unexpected transaction status: {0}")]
    UnexpectedTransactionStatus(char),

    #[error("{0}")]
    ConnectionError(Box<ErrorResponse>),

    #[error("server connection is not synchronized")]
    NotInSync,

    #[error("server not connected")]
    NotConnected,

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

    #[error("RDS IAM token generation failed: {0}")]
    RdsIamToken(String),

    #[error("Azure Workload Identity token generation failed: {0}")]
    AzureWorkloadIdentityToken(String),

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

    #[error("toml: {0}")]
    TomlSer(#[from] toml::ser::Error),

    #[error("cannot ignore response for message type: {0}")]
    UnsupportedHandleIgnore(char),
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

    /// Transient network/pool fault worth retrying.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Io(_) => true,
            Self::Net(inner) => inner.is_retryable(),
            Self::Pool(inner) => inner.is_retryable(),
            // Postgres ErrorResponse wrapped at the backend boundary, e.g. a destination shard
            // returning FATAL 57P01 mid-replication-apply, or a transient connect-time error.
            // Delegates to the single SQLSTATE retry list in ErrorResponse::is_retryable.
            Self::ExecutionError(resp)
            | Self::ConnectionError(resp)
            | Self::PreparedStatementError(resp) => resp.is_retryable(),
            // Connection dropped between operations.
            Self::NotConnected
            | Self::MultiShardNotConnected
            | Self::CopyNotConnected
            | Self::ClusterNotConnected => true,
            // Server stopped responding mid-stream.
            Self::ReadTimeout => true,
            _ => false,
        }
    }

    pub fn is_auth(&self) -> bool {
        match self {
            Self::Auth(_) => true,
            Self::ConnectionError(err) => {
                err.code == "28000"
                    || err.is_bad_password()
                    || err.code == "22023"
                    || err.code == "42704"
            }
            Self::ExecutionError(err) => {
                err.code == "22023"
                    || err.code == "28000"
                    || err.code == "42704"
                    || err.is_bad_password()
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::messages::ErrorResponse;

    #[test]
    fn test_error_is_auth() {
        // Test role-does-not-exist and other auth-related error codes
        let codes = vec!["28000", "28P01", "22023", "42704"];

        for code in codes {
            let response = ErrorResponse {
                code: code.to_string(),
                ..Default::default()
            };

            let conn_err = Error::ConnectionError(Box::new(response.clone()));
            let exec_err = Error::ExecutionError(Box::new(response));

            assert!(
                conn_err.is_auth(),
                "Expected code {} to be treated as auth error in ConnectionError",
                code
            );
            assert!(
                exec_err.is_auth(),
                "Expected code {} to be treated as auth error in ExecutionError",
                code
            );
        }

        // Test non-auth error code
        let non_auth_response = ErrorResponse {
            code: "58000".to_string(),
            ..Default::default()
        };
        let non_auth_conn_err = Error::ConnectionError(Box::new(non_auth_response.clone()));
        let non_auth_exec_err = Error::ExecutionError(Box::new(non_auth_response));

        assert!(!non_auth_conn_err.is_auth());
        assert!(!non_auth_exec_err.is_auth());
    }
}
