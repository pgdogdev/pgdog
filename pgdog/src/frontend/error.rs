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
}

impl Error {
    /// Checkout timeout.
    pub fn checkout_timeout(&self) -> bool {
        use crate::backend::pool::Error as PoolError;
        use crate::backend::Error as BackendError;

        match self {
            &Error::Backend(BackendError::Pool(PoolError::CheckoutTimeout)) => true,
            _ => false,
        }
    }
}
