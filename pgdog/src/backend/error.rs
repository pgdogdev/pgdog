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
    ConnectionError(ErrorResponse),

    #[error("server connection is not synchronized")]
    NotInSync,

    #[error("server not connected")]
    NotConnected,

    #[error("{0}")]
    Pool(#[from] crate::backend::pool::Error),

    #[error("{0}")]
    Admin(#[from] crate::admin::Error),

    #[error("no such user/database: {0}")]
    NoDatabase(User),

    #[error("no cluster connected")]
    NoCluster,
}

impl Error {
    /// Checkout timeout.
    pub fn checkout_timeout(&self) -> bool {
        match self {
            Error::Pool(crate::backend::pool::Error::CheckoutTimeout) => true,
            _ => false,
        }
    }
}
