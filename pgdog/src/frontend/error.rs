//! Frontend errors.

use thiserror::Error;

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

    #[error("prepared staatement \"{0}\" is missing")]
    MissingPreparedStatement(String),

    #[error("query timeout")]
    Timeout(#[from] tokio::time::error::Elapsed),
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
}
