//! Frontend errors.

use std::io::ErrorKind;

use thiserror::Error;

use crate::unique_id;

/// Frontend error.
#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("net: {0}")]
    Net(#[from] crate::net::Error),

    #[error("{0}")]
    Backend(#[from] crate::backend::Error),

    #[error("\"{0}\" parameter is missing")]
    Parameter(String),

    #[error("{0}")]
    Router(#[from] super::router::Error),

    #[error("authentication error")]
    Auth,

    #[error("unexpected message: {0}")]
    UnexpectedMessage(char),

    #[error("scram error")]
    Scram(#[from] scram::Error),

    #[error("replication")]
    Replication(#[from] crate::backend::replication::Error),

    #[error("{0}")]
    PreparedStatements(#[from] super::prepared_statements::Error),

    #[error("prepared statement \"{0}\" is missing")]
    MissingPreparedStatement(String),

    #[error("query timeout")]
    Timeout(#[from] tokio::time::error::Elapsed),

    #[error("schema load timeout")]
    SchemaLoad,

    #[error("join error")]
    Join(#[from] tokio::task::JoinError),

    #[error("unique id: {0}")]
    UniqueId(#[from] unique_id::Error),

    #[error("parser: {0}")]
    Parser(#[from] crate::frontend::router::parser::Error),

    #[error("rewrite: {0}")]
    Rewrite(#[from] crate::frontend::router::parser::rewrite::statement::Error),

    #[error("query has no route")]
    NoRoute,

    #[error("multi-tuple insert requires multi-shard binding")]
    MultiShardRequired,

    #[error("sharding key updates are forbidden")]
    ShardingKeyUpdateForbidden,

    // FIXME: layer errors better so we don't have
    // to reach so deep into a module.
    #[error("{0}")]
    Multi(#[from] Box<crate::frontend::client::query_engine::multi_step::error::Error>),
}

impl From<crate::frontend::client::query_engine::multi_step::error::Error> for Error {
    fn from(value: crate::frontend::client::query_engine::multi_step::error::Error) -> Self {
        Self::Multi(Box::new(value))
    }
}

impl Error {
    /// Checkout timeout.
    pub fn checkout_timeout(&self) -> bool {
        use crate::backend::pool::Error as PoolError;
        use crate::backend::Error as BackendError;

        matches!(
            self,
            &Error::Backend(BackendError::Pool(PoolError::CheckoutTimeout))
        )
    }

    pub(crate) fn disconnect(&self) -> bool {
        if let Error::Net(crate::net::Error::Io(err)) = self {
            if err.kind() == ErrorKind::UnexpectedEof {
                return true;
            }
        }

        false
    }
}
