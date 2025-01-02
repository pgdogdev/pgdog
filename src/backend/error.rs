use thiserror::Error;

use crate::net::messages::ErrorResponse;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Tls(#[from] tokio_native_tls::native_tls::Error),

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
}
